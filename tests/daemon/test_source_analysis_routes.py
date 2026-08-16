from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.home_lock import DaemonHomeLock
from spl.daemon.routes.source_analysis import (
    MAX_SOURCE_ANALYSIS_REQUEST_BYTES,
    SOURCE_ANALYSIS_CAPABILITY,
    SOURCE_ANALYSIS_CAPABILITY_VERSION,
)
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore

API_TOKEN = "daemonMasterTokenSourceAnalysisSentinel123"


def _request() -> dict[str, Any]:
    return {
        "schema": "splime.selected-code-analysis-request/v1",
        "schema_version": 1,
        "document": {"id": "notebook-1", "revision": "7"},
        "selection": {
            "kind": "cell",
            "sources": [
                {
                    "cell_id": "cell-1",
                    "source": "def add_values(left: int, right: int = 1) -> int:\n    return left + right\n",
                }
            ],
        },
        "function_name": "add_values",
        "supporting_import_sources": [],
        "downstream_sources": [],
    }


def _post(
    app: Any,
    document: dict[str, Any],
    *,
    token: str | None = API_TOKEN,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    return _post_raw(
        app,
        json.dumps(document, separators=(",", ":")).encode("utf-8"),
        token=token,
    )


def _post_raw(
    app: Any,
    body: bytes,
    *,
    token: str | None = API_TOKEN,
    content_type: str = "application/json",
) -> tuple[int, dict[str, Any], dict[str, str]]:
    async def request() -> tuple[int, dict[str, Any], dict[str, str]]:
        headers = {"Content-Type": content_type}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        response = await app.test_client().post(
            "/objects/analyze-source",
            data=body,
            headers=headers,
        )
        return (
            response.status_code,
            await response.get_json(),
            {str(key).casefold(): str(value) for key, value in response.headers.items()},
        )

    return asyncio.run(request())


def _get(app: Any, path: str) -> tuple[int, dict[str, Any]]:
    async def request() -> tuple[int, dict[str, Any]]:
        response = await app.test_client().get(
            path,
            headers={"Authorization": f"Bearer {API_TOKEN}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(request())


def test_source_analysis_route_is_authenticated_additive_and_no_store(
    tmp_path: Path,
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
        missing, missing_body, missing_headers = _post(app, _request(), token=None)
        status, body, headers = _post(app, _request())
        metadata_status, metadata = _get(app, "/meta/capabilities")

        assert missing == 401
        assert missing_body == {"error": "missing or invalid local daemon API token"}
        assert missing_headers["cache-control"] == "no-store"
        assert status == 200
        assert body["status"] == "ready"
        assert body["function"]["name"] == "add_values"
        assert body["function"]["preparation_compatible"] is True
        assert headers["cache-control"] == "no-store"
        assert headers["pragma"] == "no-cache"
        assert metadata_status == 200
        assert metadata["capabilities"][SOURCE_ANALYSIS_CAPABILITY] == {
            "state": "supported",
            "version": SOURCE_ANALYSIS_CAPABILITY_VERSION,
            "reason": None,
        }
        assert store.list_objects() == {}
    finally:
        app.runtime.shutdown()
        store.close()
        lock.release()


def test_source_analysis_route_rejects_token_and_closed_contract(
    tmp_path: Path,
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
        leaked = _request()
        leaked["selection"]["sources"][0]["source"] = f"secret = {API_TOKEN!r}\n"
        leak_status, leak_body, _ = _post(app, leaked)
        malformed = _request()
        malformed["unexpected"] = True
        malformed_status, malformed_body, _ = _post(app, malformed)

        assert leak_status == 400
        assert leak_body["error"]["code"] == "request_invalid"
        assert API_TOKEN not in json.dumps(leak_body)
        assert malformed_status == 400
        assert malformed_body["error"]["code"] == "request_invalid"
        assert store.list_objects() == {}
    finally:
        app.runtime.shutdown()
        store.close()
        lock.release()


def test_source_analysis_route_returns_source_free_secret_diagnostic(tmp_path: Path) -> None:
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
        request = _request()
        request["selection"]["sources"][0]["source"] = (
            'provider_secret = "ordinary-looking-fixture-value"\nresult = source\nresult\n'
        )

        status, body, headers = _post(app, request)

        assert status == 200
        assert body["status"] == "invalid"
        assert body["function"] is None
        assert {item["code"] for item in body["diagnostics"]} == {"source.secret_like_literal"}
        assert "ordinary-looking-fixture-value" not in json.dumps(body)
        assert headers["cache-control"] == "no-store"
        assert store.list_objects() == {}
    finally:
        app.runtime.shutdown()
        store.close()
        lock.release()


@pytest.mark.parametrize(
    ("body", "content_type", "expected_status", "expected_code"),
    [
        (b"{}", "text/plain", 400, "request_invalid"),
        (b'{"schema":1,"schema":2}', "application/json", 400, "request_invalid"),
        (b'{"value":NaN}', "application/json", 400, "request_invalid"),
        (b"[]", "application/json", 400, "request_invalid"),
    ],
)
def test_source_analysis_route_rejects_malformed_closed_documents(
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
        status, response, headers = _post_raw(app, body, content_type=content_type)

        assert status == expected_status
        assert response == {
            "schema": "spl.daemon.source-analysis-error/v1",
            "schema_version": 1,
            "error": {"code": expected_code},
        }
        assert headers["cache-control"] == "no-store"
        assert store.list_objects() == {}
    finally:
        app.runtime.shutdown()
        store.close()
        lock.release()


def test_source_analysis_route_distinguishes_incompatible_and_bounded_requests(tmp_path: Path) -> None:
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
        incompatible = _request()
        incompatible["schema_version"] = 2
        incompatible_status, incompatible_body, _ = _post(app, incompatible)

        source_too_large = _request()
        source_too_large["selection"]["sources"][0]["source"] = "x" * (512 * 1024 + 1)
        source_status, source_body, _ = _post(app, source_too_large)

        body_status, body_response, body_headers = _post_raw(
            app,
            b"{" + b"x" * MAX_SOURCE_ANALYSIS_REQUEST_BYTES + b"}",
        )

        assert incompatible_status == 409
        assert incompatible_body["error"]["code"] == "request_incompatible"
        assert source_status == 400
        assert source_body["error"]["code"] == "request_invalid"
        assert body_status == 413
        assert body_response["error"]["code"] == "body_too_large"
        assert body_headers["cache-control"] == "no-store"
        assert store.list_objects() == {}
    finally:
        app.runtime.shutdown()
        store.close()
        lock.release()
