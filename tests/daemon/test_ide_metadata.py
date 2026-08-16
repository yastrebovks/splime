"""Focused contract, authentication, and optionality tests for daemon metadata."""

from __future__ import annotations

import asyncio
import copy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.home_lock import DaemonHomeLock, DaemonInstanceIdentity
from spl.daemon.ide_metadata import (
    DAEMON_CAPABILITIES_SCHEMA,
    DAEMON_CAPABILITIES_SCHEMA_VERSION,
    DAEMON_METADATA_MAX_BYTES,
    DAEMON_METADATA_UNAVAILABLE_CODE,
    MAX_SAFE_INTEGER,
    DaemonMetadataUnavailable,
    build_daemon_capabilities_document,
    load_installed_daemon_release,
    validate_daemon_capabilities_document,
)
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore
import spl.daemon.routes.meta as meta_routes

API_TOKEN = "daemonMasterTokenSentinel123456789"
INSTANCE_ID = "0123456789abcdef0123456789abcdef"
STARTED_AT = "2026-07-31T10:00:00.123456+00:00"
OBSERVED_AT = "2026-07-31T10:00:00.123Z"


class _Distribution:
    def __init__(self, version: Any):
        self.version = version


def _identity(
    *,
    instance_id: str = INSTANCE_ID,
    generation: Any = 7,
    started_at: str = STARTED_AT,
    pid: int = 48123,
) -> DaemonInstanceIdentity:
    return DaemonInstanceIdentity(
        instance_id=instance_id,
        home_hash="f" * 64,
        generation=generation,
        previous_generation=6,
        pid=pid,
        started_at=started_at,
    )


def _document(
    *,
    identity: DaemonInstanceIdentity | None = None,
    release_version: str | None = "0.4.6",
    observed_at: str = OBSERVED_AT,
    forbidden_values: tuple[str, ...] = (),
) -> dict[str, Any]:
    return build_daemon_capabilities_document(
        identity or _identity(),
        release_version=release_version,
        observed_at=observed_at,
        forbidden_values=forbidden_values,
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


def test_daemon_capability_document_is_closed_bounded_and_forward_filterable() -> None:
    document = _document()

    validate_daemon_capabilities_document(document)
    assert document["schema"] == DAEMON_CAPABILITIES_SCHEMA
    assert document["schema_version"] == DAEMON_CAPABILITIES_SCHEMA_VERSION
    assert document["release"] == {
        "distribution": "splime",
        "version": "0.4.6",
        "evidence": {"kind": "installed_distribution_metadata"},
    }
    assert document["build"] == {
        "revision": None,
        "evidence": {
            "kind": "unknown",
            "reason": "build_revision_not_embedded",
        },
    }
    assert document["protocol"] == {"minimum": 1, "maximum": 1}
    assert document["instance"] == {
        "instance_id": INSTANCE_ID,
        "generation": 7,
        "started_at": STARTED_AT,
    }
    assert len(repr(document).encode("utf-8")) < DAEMON_METADATA_MAX_BYTES

    future = copy.deepcopy(document)
    future["capabilities"]["future.local.read"] = {
        "state": "unsupported",
        "version": None,
        "reason": "not_implemented",
    }
    validate_daemon_capabilities_document(future)

    extra = copy.deepcopy(document)
    extra["server"] = {"connected": True}
    with pytest.raises(ValueError, match="document fields"):
        validate_daemon_capabilities_document(extra)


@pytest.mark.parametrize("generation", [True, 0, -1, MAX_SAFE_INTEGER + 1])
def test_generation_must_be_positive_and_browser_safe(generation: Any) -> None:
    with pytest.raises(ValueError, match="positive safe integer"):
        _document(identity=_identity(generation=generation))


@pytest.mark.parametrize(
    ("started_at", "observed_at"),
    [
        ("2026-07-31 10:00:00", "2026-07-31T10:00:00.000Z"),
        ("2026-W31-4T10:00:00+00:00", "2026-07-31T10:00:00.000Z"),
        ("2026-07-31T10:00:00+01:00", "2026-07-31T10:00:00.000Z"),
        ("2026-07-31T10:00:01.000000+00:00", "2026-07-31T10:00:00.999Z"),
    ],
)
def test_timestamps_are_bounded_utc_and_not_before_start(
    started_at: str,
    observed_at: str,
) -> None:
    with pytest.raises(ValueError):
        _document(
            identity=_identity(started_at=started_at),
            observed_at=observed_at,
        )


def test_same_millisecond_observation_preserves_exact_microsecond_start() -> None:
    document = _document()
    assert document["instance"]["started_at"] == STARTED_AT
    assert document["observed_at"] == OBSERVED_AT


@pytest.mark.parametrize(
    "version",
    [
        None,
        "",
        "/private/tmp/splime",
        "0.4.6 bearer=secret",
        "v" * 65,
    ],
)
def test_installed_release_version_is_bounded(version: Any) -> None:
    with pytest.raises(DaemonMetadataUnavailable):
        load_installed_daemon_release(_Distribution(version))  # type: ignore[arg-type]


def test_installed_release_and_unknown_build_evidence_are_distinct() -> None:
    version = load_installed_daemon_release(_Distribution("0.4.6"))  # type: ignore[arg-type]
    document = _document(release_version=version)

    assert document["release"]["evidence"]["kind"] == "installed_distribution_metadata"
    assert document["release"]["version"] == "0.4.6"
    assert document["build"]["evidence"]["kind"] == "unknown"
    assert document["build"]["revision"] is None


def test_known_bearer_intersection_fails_before_serialization() -> None:
    with pytest.raises(DaemonMetadataUnavailable, match="forbidden"):
        _document(
            identity=_identity(instance_id="SecretToken1234567890"),
            forbidden_values=("SecretToken1234567890",),
        )
    with pytest.raises(DaemonMetadataUnavailable, match="forbidden"):
        _document(
            release_version="SecretToken1234567890",
            forbidden_values=("SecretToken1234567890",),
        )


def test_metadata_route_reuses_global_bearer_and_denies_callback_scope(tmp_path: Path) -> None:
    lock = DaemonHomeLock(tmp_path)
    identity = lock.acquire()
    store = RegistryStore(tmp_path)
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
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
        missing_status, missing_body, _, _ = _get(app, "/meta/capabilities")
        wrong_status, wrong_body, _, _ = _get(app, "/meta/capabilities", "wrong-bearer")
        callback_status, callback_body, _, _ = _get(app, "/meta/capabilities", callback_token)
        status, body, raw, headers = _get(app, "/meta/capabilities", API_TOKEN)

        assert missing_status == wrong_status == callback_status == 401
        assert missing_body == wrong_body == callback_body == {"error": "missing or invalid local daemon API token"}
        assert status == 200
        assert body["instance"] == {
            "instance_id": identity.instance_id,
            "generation": identity.generation,
            "started_at": identity.started_at,
        }
        validate_daemon_capabilities_document(body)
        assert headers["cache-control"] == "no-store"
        assert len(raw.encode("utf-8")) <= DAEMON_METADATA_MAX_BYTES
    finally:
        _close_app(app, store, lock)


def test_metadata_route_contains_no_secret_path_pid_or_operational_payload(
    tmp_path: Path,
) -> None:
    sensitive_home = tmp_path / "private-home-path-sentinel"
    lock = DaemonHomeLock(sensitive_home)
    identity = lock.acquire()
    store = RegistryStore(sensitive_home)
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )
    try:
        status, body, raw, headers = _get(app, "/meta/capabilities", API_TOKEN)
        serialized_headers = repr(headers)
        forbidden_values = (
            API_TOKEN,
            "endpointTokenSentinel",
            "providerSecretSentinel",
            "reusableCentralTokenSentinel",
            str(identity.pid),
            str(sensitive_home),
            str(store.db_path),
            "file://",
            "Traceback",
        )
        forbidden_keys = {
            "token",
            "api_token",
            "pid",
            "home",
            "db",
            "db_path",
            "path",
            "socket",
            "endpoint",
            "health",
            "diagnostics",
            "counts",
            "objects",
            "runs",
            "server",
            "environment",
            "telemetry",
            "command",
            "args",
            "traceback",
        }

        def keys(value: Any) -> set[str]:
            if isinstance(value, dict):
                return set(value) | {key for item in value.values() for key in keys(item)}
            if isinstance(value, list):
                return {key for item in value for key in keys(item)}
            return set()

        assert status == 200
        assert all(sentinel not in raw for sentinel in forbidden_values)
        assert all(sentinel not in serialized_headers for sentinel in forbidden_values)
        assert keys(body).isdisjoint(forbidden_keys)
    finally:
        _close_app(app, store, lock)


def test_missing_released_or_mismatched_live_identity_fails_closed(tmp_path: Path) -> None:
    store_without_identity = RegistryStore(tmp_path / "missing")
    app_without_identity = create_app(
        store_without_identity,
        auto_build_envs=False,
        api_token=API_TOKEN,
    )
    try:
        status, body, _, headers = _get(app_without_identity, "/meta/capabilities", API_TOKEN)
        assert status == 503
        assert body == {
            "error": "live daemon metadata is unavailable",
            "code": DAEMON_METADATA_UNAVAILABLE_CODE,
        }
        assert headers["cache-control"] == "no-store"
    finally:
        _close_app(app_without_identity, store_without_identity)

    live_home = tmp_path / "released"
    lock = DaemonHomeLock(live_home)
    identity = lock.acquire()
    store = RegistryStore(live_home)
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )
    lock.release()
    try:
        released_status, released_body, _, _ = _get(app, "/meta/capabilities", API_TOKEN)
        assert released_status == 503
        assert released_body["code"] == DAEMON_METADATA_UNAVAILABLE_CODE
    finally:
        _close_app(app, store)

    mismatched_home = tmp_path / "mismatched"
    mismatch_lock = DaemonHomeLock(mismatched_home)
    mismatch_identity = mismatch_lock.acquire()
    mismatch_store = RegistryStore(mismatched_home)
    mismatch_app = create_app(
        mismatch_store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=mismatch_identity,
        daemon_home_lock=mismatch_lock,
    )
    mismatch_app.runtime.daemon_identity = _identity()
    try:
        mismatch_status, mismatch_body, _, _ = _get(
            mismatch_app,
            "/meta/capabilities",
            API_TOKEN,
        )
        assert mismatch_status == 503
        assert mismatch_body["code"] == DAEMON_METADATA_UNAVAILABLE_CODE
    finally:
        _close_app(mismatch_app, mismatch_store, mismatch_lock)


def test_generation_changes_across_existing_home_identity_lifecycle(tmp_path: Path) -> None:
    observed: list[dict[str, Any]] = []
    for _ in range(2):
        lock = DaemonHomeLock(tmp_path)
        identity = lock.acquire()
        store = RegistryStore(tmp_path)
        app = create_app(
            store,
            auto_build_envs=False,
            api_token=API_TOKEN,
            daemon_identity=identity,
            daemon_home_lock=lock,
        )
        try:
            status, body, _, _ = _get(app, "/meta/capabilities", API_TOKEN)
            assert status == 200
            observed.append(body["instance"])
        finally:
            _close_app(app, store, lock)

    assert observed[1]["instance_id"] == observed[0]["instance_id"]
    assert observed[1]["generation"] == observed[0]["generation"] + 1
    assert observed[1]["started_at"] != observed[0]["started_at"]


def test_release_evidence_is_bound_when_app_is_created(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta_routes, "load_installed_daemon_release", lambda: "0.4.6")
    lock = DaemonHomeLock(tmp_path)
    identity = lock.acquire()
    store = RegistryStore(tmp_path)
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )
    monkeypatch.setattr(meta_routes, "load_installed_daemon_release", lambda: "9.9.9")
    try:
        status, body, _, _ = _get(app, "/meta/capabilities", API_TOKEN)
        assert status == 200
        assert body["release"]["version"] == "0.4.6"
    finally:
        _close_app(app, store, lock)


def test_corrupt_release_metadata_cannot_break_existing_app_or_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_loader() -> str:
        raise OSError("/private/release-metadata-path-sentinel")

    monkeypatch.setattr(meta_routes, "load_installed_daemon_release", fail_loader)
    lock = DaemonHomeLock(tmp_path)
    identity = lock.acquire()
    store = RegistryStore(tmp_path)
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )
    try:
        health_status, health, _, _ = _get(app, "/health", API_TOKEN)
        meta_status, meta_body, meta_raw, _ = _get(app, "/meta/capabilities", API_TOKEN)
        assert health_status == 200
        assert health["ok"] is True
        assert meta_status == 503
        assert meta_body["code"] == DAEMON_METADATA_UNAVAILABLE_CODE
        assert "private/release" not in meta_raw
    finally:
        _close_app(app, store, lock)


def test_observed_time_is_fresh_for_each_request(tmp_path: Path) -> None:
    lock = DaemonHomeLock(tmp_path)
    identity = lock.acquire()
    store = RegistryStore(tmp_path)
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )
    try:
        before = datetime.now(UTC)
        status, body, _, _ = _get(app, "/meta/capabilities", API_TOKEN)
        after = datetime.now(UTC)
        observed = datetime.fromisoformat(body["observed_at"].replace("Z", "+00:00"))

        assert status == 200
        assert before.replace(microsecond=(before.microsecond // 1000) * 1000) <= observed <= after
    finally:
        _close_app(app, store, lock)
