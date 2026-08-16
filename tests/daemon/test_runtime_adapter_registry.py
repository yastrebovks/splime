"""Focused contract and route tests for the browser-safe adapter registry."""

from __future__ import annotations

import asyncio
import copy
import importlib.metadata
from pathlib import Path
from typing import Any

import pytest

import spl.daemon.routes.meta as meta_routes
import spl.daemon.runtime_adapter_registry as registry_module
from spl.daemon.home_lock import DaemonHomeLock, DaemonInstanceIdentity
from spl.daemon.ide_metadata import LOCAL_CAPABILITY_VERSIONS
from spl.daemon.runtime_adapter_registry import (
    DAEMON_CONTROL_ENVIRONMENT_SCOPE,
    RUNTIME_ADAPTER_REGISTRY_CAPABILITY,
    RUNTIME_ADAPTER_REGISTRY_MAX_BYTES,
    RUNTIME_ADAPTER_REGISTRY_SCHEMA,
    RUNTIME_ADAPTER_REGISTRY_UNAVAILABLE_CODE,
    RuntimeAdapterRegistryUnavailable,
    build_runtime_adapter_registry_document,
    validate_runtime_adapter_registry_document,
)
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore

API_TOKEN = "runtimeAdapterRegistryMasterToken123"
INSTANCE_ID = "0123456789abcdef0123456789abcdef"
STARTED_AT = "2026-08-10T10:00:00.123456+00:00"
OBSERVED_AT = "2026-08-10T10:00:00.123Z"


def _identity(*, instance_id: str = INSTANCE_ID) -> DaemonInstanceIdentity:
    return DaemonInstanceIdentity(
        instance_id=instance_id,
        home_hash="f" * 64,
        generation=7,
        previous_generation=6,
        pid=48123,
        started_at=STARTED_AT,
    )


def _get(app: Any, path: str, token: str | None = None) -> tuple[int, Any, str, dict[str, str]]:
    async def request() -> tuple[int, Any, str, dict[str, str]]:
        headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        response = await app.test_client().get(path, headers=headers)
        raw = (await response.get_data()).decode("utf-8")
        return (
            response.status_code,
            await response.get_json(),
            raw,
            {str(key).casefold(): str(value) for key, value in response.headers.items()},
        )

    return asyncio.run(request())


def _close_app(app: Any, store: RegistryStore, lock: DaemonHomeLock | None = None) -> None:
    app.runtime.shutdown()
    store.close()
    if lock is not None:
        lock.release()


def _fake_versions(installed: dict[str, str]):
    def version(package: str) -> str:
        try:
            return installed[package]
        except KeyError:
            raise importlib.metadata.PackageNotFoundError(package) from None

    return version


def test_registry_document_is_closed_bounded_and_scopes_local_dependency_observations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        registry_module.importlib_metadata,
        "version",
        _fake_versions({"pandas": "2.2.3", "openpyxl": "3.1.5"}),
    )
    document = build_runtime_adapter_registry_document(
        _identity(),
        remote_custom_adapter_execution_enabled=False,
        observed_at=OBSERVED_AT,
    )

    validate_runtime_adapter_registry_document(document)
    assert set(document) == {
        "schema",
        "schema_version",
        "capability",
        "instance",
        "observed_at",
        "registry_revision",
        "environment",
        "policy",
        "limits",
        "adapters",
    }
    assert document["schema"] == RUNTIME_ADAPTER_REGISTRY_SCHEMA
    assert document["capability"] == {"id": RUNTIME_ADAPTER_REGISTRY_CAPABILITY, "version": 1}
    assert document["environment"] == {"scope": DAEMON_CONTROL_ENVIRONMENT_SCOPE}
    assert document["policy"] == {"remote_custom_adapter_execution": {"implemented": True, "enabled": False}}
    assert document["limits"] == {
        "max_bindings": 1_024,
        "max_input_artifacts": 1_024,
        "max_input_bytes": 256 * 1024 * 1024,
        "max_input_total_bytes": 512 * 1024 * 1024,
        "max_custom_bundle_bytes": 512 * 1024,
    }
    assert [row["id"] for row in document["adapters"]] == [
        "binary-file",
        "dataframe-csv-semicolon",
        "dataframe-json-split",
        "dataframe-xlsx",
        "json",
        "opaque-file",
        "png-pillow",
        "text-file-utf8",
    ]
    by_id = {row["id"]: row for row in document["adapters"]}
    assert by_id["dataframe-xlsx"]["required_distributions"] == [
        {"name": "pandas", "state": "installed", "version": "2.2.3"},
        {"name": "openpyxl", "state": "installed", "version": "3.1.5"},
    ]
    assert by_id["png-pillow"]["required_distributions"] == [{"name": "Pillow", "state": "missing", "version": None}]
    assert by_id["png-pillow"]["local_availability"] == {
        "scope": DAEMON_CONTROL_ENVIRONMENT_SCOPE,
        "state": "missing_required_distributions",
        "reason": "required_distribution_missing",
    }
    assert all(
        row["remote_availability"]
        == {"state": "requires_target_preflight", "reason": "target_environment_not_observed"}
        for row in document["adapters"]
    )
    assert all(
        row["builtin"] is True and row["custom"] is False and row["custom_code"] is False
        for row in document["adapters"]
    )
    assert len(repr(document).encode("utf-8")) < RUNTIME_ADAPTER_REGISTRY_MAX_BYTES


def test_registry_revision_excludes_time_policy_and_control_environment_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        registry_module.importlib_metadata,
        "version",
        _fake_versions({"pandas": "1.5.0", "openpyxl": "3.0.0", "Pillow": "9.0.0"}),
    )
    first = build_runtime_adapter_registry_document(
        _identity(),
        remote_custom_adapter_execution_enabled=False,
        observed_at=OBSERVED_AT,
    )
    monkeypatch.setattr(
        registry_module.importlib_metadata,
        "version",
        _fake_versions({"pandas": "3.0.0"}),
    )
    second = build_runtime_adapter_registry_document(
        _identity(),
        remote_custom_adapter_execution_enabled=True,
        observed_at="2026-08-10T10:00:01.000Z",
    )

    assert first["registry_revision"] == second["registry_revision"]
    assert first["policy"] != second["policy"]
    assert first["adapters"] != second["adapters"]


def test_registry_validator_rejects_unknown_fields_remote_claims_and_code_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(registry_module.importlib_metadata, "version", _fake_versions({}))
    document = build_runtime_adapter_registry_document(
        _identity(),
        remote_custom_adapter_execution_enabled=False,
        observed_at=OBSERVED_AT,
    )

    extra = copy.deepcopy(document)
    extra["server"] = {"connected": True}
    with pytest.raises(ValueError, match="document fields"):
        validate_runtime_adapter_registry_document(extra)

    remote_claim = copy.deepcopy(document)
    remote_claim["adapters"][0]["remote_availability"]["state"] = "available"
    with pytest.raises(ValueError, match="target preflight"):
        validate_runtime_adapter_registry_document(remote_claim)

    code_metadata = copy.deepcopy(document)
    code_metadata["adapters"][0]["source"] = "def save(path, value): ..."
    with pytest.raises(ValueError, match="fields"):
        validate_runtime_adapter_registry_document(code_metadata)


def test_registry_builder_rejects_forbidden_value_intersection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(registry_module.importlib_metadata, "version", _fake_versions({}))
    sentinel = "SecretRegistryToken1234567890"
    with pytest.raises(RuntimeAdapterRegistryUnavailable, match="forbidden"):
        build_runtime_adapter_registry_document(
            _identity(instance_id=sentinel),
            remote_custom_adapter_execution_enabled=False,
            observed_at=OBSERVED_AT,
            forbidden_values=(sentinel,),
        )


def test_registry_route_reuses_global_auth_denies_callback_scope_and_is_no_store(tmp_path: Path) -> None:
    lock = DaemonHomeLock(tmp_path)
    identity = lock.acquire()
    store = RegistryStore(tmp_path)
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        allow_remote_custom_adapters=True,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )
    try:
        callback_token = app.runtime.callback_capabilities.mint(
            "callback-run",
            [
                {
                    "id": "11111111-1111-4111-8111-111111111111",
                    "remote": {
                        "url": "https://central.invalid",
                        "name": "callback_object",
                        "version": 1,
                    },
                }
            ],
            ttl_seconds=60,
        )
        missing_status, missing_body, _, _ = _get(app, "/meta/runtime-adapters")
        wrong_status, wrong_body, _, _ = _get(app, "/meta/runtime-adapters", "wrong-bearer")
        callback_status, callback_body, _, _ = _get(app, "/meta/runtime-adapters", callback_token)
        status, body, raw, headers = _get(app, "/meta/runtime-adapters", API_TOKEN)
        capabilities_status, capabilities, _, _ = _get(app, "/meta/capabilities", API_TOKEN)

        assert missing_status == wrong_status == callback_status == 401
        assert missing_body == wrong_body == callback_body == {"error": "missing or invalid local daemon API token"}
        assert status == 200
        validate_runtime_adapter_registry_document(body)
        assert body["instance"] == {
            "instance_id": identity.instance_id,
            "generation": identity.generation,
            "started_at": identity.started_at,
        }
        assert body["policy"]["remote_custom_adapter_execution"] == {"implemented": True, "enabled": True}
        assert headers["cache-control"] == "no-store"
        assert len(raw.encode("utf-8")) <= RUNTIME_ADAPTER_REGISTRY_MAX_BYTES
        assert all(
            sentinel not in raw
            for sentinel in (
                API_TOKEN,
                str(identity.pid),
                str(tmp_path),
                str(store.db_path),
                "Traceback",
                "def save",
                "def load",
            )
        )
        assert capabilities_status == 200
        assert capabilities["capabilities"][RUNTIME_ADAPTER_REGISTRY_CAPABILITY] == {
            "state": "supported",
            "version": 1,
            "reason": None,
        }
        assert LOCAL_CAPABILITY_VERSIONS[RUNTIME_ADAPTER_REGISTRY_CAPABILITY] == 1
    finally:
        _close_app(app, store, lock)


def test_registry_route_fails_closed_without_live_identity_and_redacts_internal_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = RegistryStore(tmp_path / "missing")
    app = create_app(store, auto_build_envs=False, api_token=API_TOKEN)
    try:
        status, body, _, headers = _get(app, "/meta/runtime-adapters", API_TOKEN)
        assert status == 503
        assert body == {
            "error": "runtime adapter registry is unavailable",
            "code": RUNTIME_ADAPTER_REGISTRY_UNAVAILABLE_CODE,
        }
        assert headers["cache-control"] == "no-store"
    finally:
        _close_app(app, store)

    home = tmp_path / "internal-error"
    lock = DaemonHomeLock(home)
    identity = lock.acquire()
    error_store = RegistryStore(home)
    error_app = create_app(
        error_store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )

    def fail_builder(*args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        raise OSError("/private/runtime-adapter-registry-path-sentinel")

    monkeypatch.setattr(meta_routes, "build_runtime_adapter_registry_document", fail_builder)
    try:
        status, body, raw, headers = _get(error_app, "/meta/runtime-adapters", API_TOKEN)
        assert status == 503
        assert body["code"] == RUNTIME_ADAPTER_REGISTRY_UNAVAILABLE_CODE
        assert "private/runtime-adapter" not in raw
        assert headers["cache-control"] == "no-store"
    finally:
        _close_app(error_app, error_store, lock)
