from __future__ import annotations

import asyncio
import json
import sys
import time
from http import HTTPStatus
from pathlib import Path
from typing import Any

import pytest

from spl.core.source_preparation import prepare_source, prepared_manifest
from spl.daemon import prepared_validation as prepared_service
from spl.daemon.home_lock import DaemonHomeLock
from spl.daemon.routes import prepared_validation as prepared_route
from spl.daemon.prepared_validation import (
    MAX_REQUEST_BYTES,
    PREPARED_PUBLICATION_CAPABILITY,
    PREPARED_PUBLICATION_CAPABILITY_VERSION,
    PREPARED_VALIDATION_CAPABILITY,
    PREPARED_VALIDATION_CAPABILITY_VERSION,
)
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore

API_TOKEN = "daemonMasterTokenPreparedValidationSentinel123"


def _prepared(*, body: str = "return left + right") -> dict[str, Any]:
    result = prepare_source(
        {
            "schema": "splime.prepare-source-request/v1",
            "schema_version": 1,
            "sources": [
                {
                    "logical_path": "spl_generated/add_values.py",
                    "language": "python",
                    "text": (f"def add_values(left: int, right: int = 1) -> int:\n    {body}\n"),
                }
            ],
            "proposal_ir": {
                "schema": "splime.generated-plan/v1",
                "schema_version": 1,
                "kind": "function",
                "name": "add_values",
                "nodes": [],
                "edges": [],
                "outputs": [],
                "dependencies": [],
                "adapters": [],
            },
            "entrypoints": ["add_values"],
            "environment_request": None,
            "runtime_request": {"mode": "venv"},
            "target": {
                "python_language": "3.13",
                "spl_ir_schema": "splime.object-ir/v1",
                "preparation_protocol": 1,
            },
            "base": None,
        }
    )
    assert result["status"] == "ready", result["diagnostics"]
    return result["prepared"]


def _document(prepared: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": "splime.object-validation-request/v1",
        "schema_version": 1,
        "prepared_manifest": prepared_manifest(prepared),
        "canonical_ir": prepared["canonical_ir"],
        "reference_resolution": {"mode": "none", "max_age_seconds": None},
    }


def _post(
    app: Any,
    body: bytes,
    *,
    token: str | None = API_TOKEN,
    content_type: str = "application/json",
    path: str = "/objects/validate",
) -> tuple[int, Any, str, dict[str, str]]:
    async def request() -> tuple[int, Any, str, dict[str, str]]:
        headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        headers["Content-Type"] = content_type
        response = await app.test_client().post(
            path,
            data=body,
            headers=headers,
        )
        raw = (await response.get_data()).decode("utf-8")
        return (
            response.status_code,
            await response.get_json(),
            raw,
            {str(key).casefold(): str(value) for key, value in response.headers.items()},
        )

    return asyncio.run(request())


def _get(app: Any, path: str) -> tuple[int, Any]:
    async def request() -> tuple[int, Any]:
        response = await app.test_client().get(
            path,
            headers={"Authorization": f"Bearer {API_TOKEN}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(request())


def _close(app: Any, store: RegistryStore, lock: DaemonHomeLock) -> None:
    app.runtime.shutdown()
    store.close()
    lock.release()


def test_route_authenticates_is_no_store_and_advertises_exact_capability(tmp_path: Path) -> None:
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
    prepared = _prepared()
    raw_request = json.dumps(_document(prepared), separators=(",", ":")).encode("utf-8")
    try:
        missing_status, missing_body, _, missing_headers = _post(app, raw_request, token=None)
        wrong_status, wrong_body, _, wrong_headers = _post(app, raw_request, token="wrong")
        status, body, raw, headers = _post(app, raw_request)

        assert missing_status == wrong_status == 401
        assert missing_body == wrong_body == {"error": "missing or invalid local daemon API token"}
        assert missing_headers["cache-control"] == wrong_headers["cache-control"] == "no-store"
        assert missing_headers["pragma"] == wrong_headers["pragma"] == "no-cache"
        assert status == 200
        assert body["status"] == "valid"
        assert body["publish_ready"] is False
        assert body["capability"] == {
            "id": PREPARED_VALIDATION_CAPABILITY,
            "version": PREPARED_VALIDATION_CAPABILITY_VERSION,
        }
        assert headers["cache-control"] == "no-store"
        assert headers["pragma"] == "no-cache"
        assert prepared["source_documents"][0]["normalized_text"] not in raw

        metadata_status, metadata = _get(app, "/meta/capabilities")
        assert metadata_status == 200
        assert metadata["capabilities"][PREPARED_VALIDATION_CAPABILITY] == {
            "state": "supported",
            "version": PREPARED_VALIDATION_CAPABILITY_VERSION,
            "reason": None,
        }
        assert metadata["capabilities"][PREPARED_PUBLICATION_CAPABILITY] == {
            "state": "supported",
            "version": PREPARED_PUBLICATION_CAPABILITY_VERSION,
            "reason": None,
        }
    finally:
        _close(app, store, lock)


def test_prepare_route_is_nonmutating_and_bundle_registers_through_existing_api(
    tmp_path: Path,
) -> None:
    lock = DaemonHomeLock(tmp_path)
    identity = lock.acquire()
    store = RegistryStore(tmp_path)
    store.register_env("default", sys.executable)
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )
    prepared = _prepared()
    request = {
        "schema": "splime.object-preparation-request/v1",
        "schema_version": 1,
        "prepare_request": {
            "schema": "splime.prepare-source-request/v1",
            "schema_version": 1,
            "sources": [
                {
                    "logical_path": prepared["source_documents"][0]["logical_path"],
                    "language": "python",
                    "text": prepared["source_documents"][0]["normalized_text"],
                }
            ],
            "proposal_ir": prepared["normalized_plan"],
            "entrypoints": prepared["entrypoints"],
            "environment_request": None,
            "runtime_request": prepared["runtime_request"],
            "target": prepared["target"],
            "base": None,
        },
    }
    raw_request = json.dumps(request, separators=(",", ":")).encode("utf-8")
    try:
        before = store.list_objects()
        status, body, raw, headers = _post(
            app,
            raw_request,
            path="/objects/prepare",
        )

        assert status == 200
        assert body["status"] == "ready"
        assert body["entrypoint"] == "add_values"
        assert body["prepared_hash"].startswith("sha256:")
        assert body["validation_hash"].startswith("sha256:")
        assert "!DFunction" in body["yaml"]
        assert store.list_objects() == before == {}
        assert headers["cache-control"] == "no-store"
        assert API_TOKEN not in raw

        publish_status, published, _, _ = _post(
            app,
            json.dumps(
                {
                    "name": "published_add_values",
                    "entrypoint": body["entrypoint"],
                    "env": "default",
                    "yaml": body["yaml"],
                    "local_only": True,
                },
                separators=(",", ":"),
            ).encode("utf-8"),
            path="/objects",
        )
        assert publish_status == 201
        assert published["name"] == "published_add_values"
        assert published["kind"] == "function"
        assert set(store.list_objects()) == {"published_add_values"}
    finally:
        _close(app, store, lock)


def test_route_rejects_known_daemon_token_and_never_echoes_input(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
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
    prepared = _prepared(body=f"return {API_TOKEN!r}")
    raw_request = json.dumps(_document(prepared), separators=(",", ":")).encode("utf-8")
    try:
        status, body, raw, _ = _post(app, raw_request)
        assert status == 400
        assert body == {
            "schema": "spl.daemon.prepared-validation-error",
            "schema_version": 1,
            "code": "request_invalid",
            "operation": "validate_prepared_object",
            "retryable": False,
        }
        assert API_TOKEN not in raw
        assert API_TOKEN not in caplog.text
    finally:
        _close(app, store, lock)


@pytest.mark.parametrize(
    ("body", "content_type", "expected_status", "expected_code"),
    [
        (b'{"schema":"x","schema":"y"}', "application/json", 400, "request_invalid"),
        (b'{"value":Infinity}', "application/json", 400, "request_invalid"),
        (b"{}", "text/plain", 400, "request_invalid"),
    ],
)
def test_route_rejects_adversarial_bodies_with_closed_errors(
    tmp_path: Path,
    body: bytes,
    content_type: str,
    expected_status: int,
    expected_code: str,
) -> None:
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
        status, response, _, headers = _post(app, body, content_type=content_type)
        assert status == expected_status
        assert response["code"] == expected_code
        assert set(response) == {"schema", "schema_version", "code", "operation", "retryable"}
        assert headers["cache-control"] == "no-store"
        assert headers["pragma"] == "no-cache"
    finally:
        _close(app, store, lock)


def test_route_fails_closed_while_draining(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    prepared = _prepared()
    raw_request = json.dumps(_document(prepared), separators=(",", ":")).encode("utf-8")
    monkeypatch.setattr(type(app.runtime.lifecycle), "is_draining", property(lambda _self: True))
    try:
        status, body, _, _ = _post(app, raw_request)
        assert status == 409
        assert body["code"] == "daemon_draining"
    finally:
        _close(app, store, lock)


def test_route_enforces_body_and_work_deadlines_with_closed_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    prepared = _prepared()
    raw_request = json.dumps(_document(prepared), separators=(",", ":")).encode("utf-8")

    def delayed_validation(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        time.sleep(0.05)
        return {}

    try:
        oversized_status, oversized_body, _, oversized_headers = _post(
            app,
            b"x" * (MAX_REQUEST_BYTES + 1),
        )
        assert oversized_status == 413
        assert oversized_body["code"] == "body_too_large"
        assert oversized_headers["cache-control"] == "no-store"

        monkeypatch.setattr(prepared_route, "VALIDATION_TIMEOUT_SECONDS", 0.005)
        monkeypatch.setattr(prepared_route, "validate_prepared_request", delayed_validation)
        timeout_status, timeout_body, timeout_raw, timeout_headers = _post(app, raw_request)
        assert timeout_status == 503
        assert timeout_body == prepared_service.prepared_validation_error_document(
            prepared_service.PreparedValidationError(
                "validation_timeout",
                HTTPStatus.SERVICE_UNAVAILABLE,
                retryable=True,
            )
        )
        assert prepared["source_documents"][0]["normalized_text"] not in timeout_raw
        assert timeout_headers["cache-control"] == "no-store"
    finally:
        _close(app, store, lock)
