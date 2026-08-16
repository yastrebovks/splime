from __future__ import annotations

import asyncio
from copy import deepcopy
import json
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.connected_ide import (
    CONNECTED_CREDENTIALS_REVEAL_REQUEST_SCHEMA,
    CONNECTED_REMOTE_RUN_PREFLIGHT_REQUEST_SCHEMA,
    MAX_CONNECTED_BODY_BYTES,
    MAX_REMOTE_RUN_ITEMS,
    SERVER_REMOTE_CAPABILITY_IDS,
)
from spl.daemon.remote_client import ServerClientError
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore


def _save_connected_server_connection(store: RegistryStore) -> dict[str, Any]:
    return store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-central-secret",
        user_token="user-central-secret",
        connection={
            "id": "remote-connection-1",
            "owner_id": "owner-1",
            "subject_type": "machine",
            "subject_id": "machine-1",
            "machine_id": "machine-1",
            "display_name": "lab-machine",
            "status": "connected",
            "capabilities": {},
        },
        heartbeat_interval_seconds=60,
    )


def _valid_version_document() -> dict[str, Any]:
    return {
        "contract": "version_authority/v1",
        "schema_version": 1,
        "component": "server",
        "declared": {
            "state": "present",
            "reason_code": "bundled_release_identity_present",
            "schema_version": 1,
            "release_id": "splime-0.4.6",
            "component": "server",
            "version": "0.4.6",
            "artifact_sha256": None,
            "release_manifest_sha256": None,
            "schema_target": 32,
            "evidence_state": "declared",
            "contracts": {
                "console_server": "console-server/v1",
                "daemon_server_capabilities": list(SERVER_REMOTE_CAPABILITY_IDS),
            },
            "source_repository": "gitlab.com/spl894143/spl-server",
            "source_ref": "v0.4.6",
            "source_binding": "pinned_commit",
            "source_commit": None,
        },
        "deployment": {
            "state": "present",
            "reason_code": "deployment_receipt_present",
            "schema_version": 1,
            "release_id": "splime-0.4.6",
            "component": "server",
            "version": "0.4.6",
            "source_ref": "v0.4.6",
            "source_commit": "a" * 40,
            "artifact_sha256": "b" * 64,
            "release_manifest_sha256": "c" * 64,
            "schema_target": 32,
            "deployed_at": "2026-08-02T10:00:00+00:00",
            "environment_class": "staging",
        },
        "database_schema": {"current": 32, "target": 32},
    }


def _valid_preflight() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "contract": "execution_preflight",
        "checked_at": "2026-08-02T10:01:00+00:00",
        "can_queue": True,
        "point_in_time": True,
        "reservation": False,
        "resolved_request": {
            "version_id": "version-1",
            "target_machine_id": "machine-1",
            "offline_policy": "queue",
        },
        "object": {
            "id": "demo-object",
            "name": "demo-object",
            "owner_id": "owner-1",
            "library_slug": "default",
            "version": 1,
            "version_id": "version-1",
            "entrypoint": "demo-object",
        },
        "authorization": {},
        "execution": {},
        "environment": {},
        "runtimes": {},
        "trust_boundary": {},
        "telemetry": {},
        "compatibility": {},
        "capacity": {},
        "checks": [
            {
                "code": "object_identity",
                "outcome": "pass",
                "reason_code": "exact_object_version_resolved",
                "explanation": "Exact Object version resolved.",
                "evidence_at": "2026-08-02T10:01:00+00:00",
            }
        ],
    }


def _reveal_request(connection_id: str) -> dict[str, Any]:
    return {
        "schema": CONNECTED_CREDENTIALS_REVEAL_REQUEST_SCHEMA,
        "schema_version": 1,
        "connection_id": connection_id,
        "intent": "reveal_reusable_central_credentials",
    }


def _preflight_request(**updates: Any) -> dict[str, Any]:
    request = {
        "object": "demo-object",
        "version_id": "version-1",
        "target_machine_id": "machine-1",
        "object_owner_id": "owner-1",
        "library": "default",
        "offline_policy": "queue",
    }
    request.update(updates)
    return {
        "schema": CONNECTED_REMOTE_RUN_PREFLIGHT_REQUEST_SCHEMA,
        "schema_version": 1,
        "request": request,
    }


async def _request(
    app: Any,
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    authenticated: bool = True,
    raw: bytes | None = None,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    headers = {"Authorization": f"Bearer {app.api_token}"} if authenticated else {}
    client = app.test_client()
    if method == "GET":
        response = await client.get(path, headers=headers)
    elif raw is not None:
        response = await client.post(
            path,
            data=raw,
            headers={**headers, "Content-Type": "application/json"},
        )
    else:
        response = await client.post(path, json=payload, headers=headers)
    body = await response.get_json()
    return (
        response.status_code,
        body,
        {key.casefold(): value for key, value in response.headers.items()},
    )


class ConnectedReadClient:
    def __init__(self) -> None:
        self.list_calls = 0
        self.detail_calls: list[str] = []
        self.event_calls: list[str] = []
        self.version_calls = 0
        self.preflight_calls: list[dict[str, Any]] = []
        self.list_error: ServerClientError | None = None
        self.detail_error: ServerClientError | None = None
        self.event_error: ServerClientError | None = None
        self.version_document = _valid_version_document()
        self.preflight_document = _valid_preflight()

    def list_remote_runs(self) -> list[dict[str, Any]]:
        self.list_calls += 1
        if self.list_error is not None:
            raise self.list_error
        return [{"id": f"run-{index}", "status": "queued"} for index in range(MAX_REMOTE_RUN_ITEMS + 1)]

    def get_remote_run_detail(self, run_id: str) -> dict[str, Any]:
        self.detail_calls.append(run_id)
        if self.detail_error is not None:
            raise self.detail_error
        return {"run": {"id": run_id, "status": "running"}}

    def list_remote_run_events(self, run_id: str) -> list[dict[str, Any]]:
        self.event_calls.append(run_id)
        if self.event_error is not None:
            raise self.event_error
        return [{"id": "event-1", "run_id": run_id}]

    def get_server_version(self) -> dict[str, Any]:
        self.version_calls += 1
        return deepcopy(self.version_document)

    def preflight_remote_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.preflight_calls.append(deepcopy(payload))
        return deepcopy(self.preflight_document)


@pytest.fixture
def connected_app(tmp_path: Path) -> Any:
    store = RegistryStore(tmp_path)
    _save_connected_server_connection(store)
    app = create_app(
        store,
        api_token="daemon-master-token",
        auto_build_envs=False,
    )
    fake = ConnectedReadClient()
    credentials = store.current_server_connection_credentials()
    assert credentials is not None
    app.runtime._require_live_server_channel_credentials = lambda: credentials
    app.runtime._server_client_for_credentials = lambda *args, **kwargs: fake
    app.connected_test_client = fake
    try:
        yield app
    finally:
        app.runtime.shutdown()
        store.close()


def test_reveal_is_explicit_repeatable_authenticated_and_no_store(
    connected_app: Any,
) -> None:
    connection = connected_app.runtime.store.current_server_connection()
    assert connection is not None
    payload = _reveal_request(connection["id"])

    first = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/ide/server/credentials/reveal",
            payload=payload,
        )
    )
    second = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/ide/server/credentials/reveal",
            payload=payload,
        )
    )
    unauthenticated = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/ide/server/credentials/reveal",
            payload=payload,
            authenticated=False,
        )
    )

    assert first[0] == second[0] == 200
    assert (
        first[1]["credentials"]
        == second[1]["credentials"]
        == [
            {"role": "user", "secret": "user-central-secret"},
            {"role": "machine", "secret": "machine-central-secret"},
        ]
    )
    assert first[1]["connection"]["server_url"] == "https://splime.io/api"
    assert first[2]["cache-control"] == second[2]["cache-control"] == "no-store"
    assert unauthenticated[0] == 401
    assert unauthenticated[2]["cache-control"] == "no-store"
    serialized = json.dumps(first[1])
    assert "daemon-master-token" not in serialized
    assert "secret_ref" not in serialized


def test_reveal_wrong_or_disconnected_connection_is_non_enumerating(
    connected_app: Any,
) -> None:
    connection = connected_app.runtime.store.current_server_connection()
    assert connection is not None
    wrong = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/ide/server/credentials/reveal",
            payload=_reveal_request("connection-does-not-exist"),
        )
    )
    connected_app.runtime.store.mark_server_connection_disconnected(connection["id"])
    disconnected = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/ide/server/credentials/reveal",
            payload=_reveal_request(connection["id"]),
        )
    )

    assert wrong[:2] == disconnected[:2]
    assert wrong[0] == 409
    assert wrong[1]["code"] == "credential_reveal_unavailable"
    assert wrong[2]["cache-control"] == disconnected[2]["cache-control"] == "no-store"
    assert "central-secret" not in json.dumps(wrong[1])


def test_reveal_rejects_unknown_secret_field_and_oversized_body(
    connected_app: Any,
) -> None:
    connection = connected_app.runtime.store.current_server_connection()
    assert connection is not None
    injected = _reveal_request(connection["id"])
    injected["provider_secret"] = "provider-sentinel"
    rejected = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/ide/server/credentials/reveal",
            payload=injected,
        )
    )
    oversized = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/ide/server/credentials/reveal",
            raw=b"{" + b"x" * MAX_CONNECTED_BODY_BYTES + b"}",
        )
    )

    assert rejected[0] == 400
    assert rejected[1]["code"] == "request_invalid"
    assert "provider-sentinel" not in json.dumps(rejected[1])
    assert oversized[0] == 413
    assert oversized[1]["code"] == "body_too_large"
    assert oversized[2]["cache-control"] == "no-store"


def test_remote_run_read_routes_are_bounded_authenticated_and_no_store(
    connected_app: Any,
) -> None:
    listed = asyncio.run(_request(connected_app, "GET", "/server/runs"))
    detail = asyncio.run(_request(connected_app, "GET", "/server/runs/run-1/detail"))
    events = asyncio.run(_request(connected_app, "GET", "/server/runs/run-1/events"))
    unauthenticated = asyncio.run(
        _request(
            connected_app,
            "GET",
            "/server/runs",
            authenticated=False,
        )
    )

    assert listed[0] == detail[0] == events[0] == 200
    assert listed[1]["truncated"] is True
    assert listed[1]["completeness"] == "partial"
    assert len(listed[1]["runs"]) == MAX_REMOTE_RUN_ITEMS
    assert detail[1]["detail"]["run"]["id"] == "run-1"
    assert events[1]["events"] == [{"id": "event-1", "run_id": "run-1"}]
    assert all(result[2]["cache-control"] == "no-store" for result in (listed, detail, events))
    assert unauthenticated[0] == 401
    assert unauthenticated[2]["cache-control"] == "no-store"


@pytest.mark.parametrize("status_code", [403, 404])
def test_remote_run_detail_denial_and_absence_are_non_enumerating(
    connected_app: Any,
    status_code: int,
) -> None:
    connected_app.connected_test_client.detail_error = ServerClientError(
        status_code,
        "raw upstream sentinel must not pass",
    )

    response = asyncio.run(_request(connected_app, "GET", "/server/runs/foreign-run/detail"))

    assert response[0] == 404
    assert response[1]["code"] == "remote_run_not_found"
    assert "sentinel" not in json.dumps(response[1])
    assert response[2]["cache-control"] == "no-store"


def test_remote_run_permission_denied_is_not_offline(
    connected_app: Any,
) -> None:
    connected_app.connected_test_client.list_error = ServerClientError(
        403,
        "raw permission prose",
    )

    response = asyncio.run(_request(connected_app, "GET", "/server/runs"))

    assert response[0] == 403
    assert response[1]["code"] == "server_permission_denied"
    assert "offline" not in json.dumps(response[1])
    assert "raw permission prose" not in json.dumps(response[1])


def test_preflight_is_closed_nonmutating_and_capability_bound(
    connected_app: Any,
) -> None:
    before_runs = connected_app.runtime.store.list_runs()
    response = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/server/runs/preflight",
            payload=_preflight_request(),
        )
    )
    after_runs = connected_app.runtime.store.list_runs()

    assert response[0] == 200
    assert response[1]["preflight"]["point_in_time"] is True
    assert response[1]["preflight"]["reservation"] is False
    assert response[1]["server_capabilities"]["capabilities"]["spl.remote_run.preflight.v1"] == {
        "state": "supported",
        "reason": None,
    }
    assert connected_app.connected_test_client.preflight_calls == [
        {
            "object": "demo-object",
            "version_id": "version-1",
            "target_machine_id": "machine-1",
            "object_owner_id": "owner-1",
            "library": "default",
            "offline_policy": "queue",
        }
    ]
    assert before_runs == after_runs == []
    assert response[2]["cache-control"] == "no-store"

    forbidden = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/server/runs/preflight",
            payload=_preflight_request(args=["forbidden"]),
        )
    )
    assert forbidden[0] == 400
    assert forbidden[1]["code"] == "request_invalid"
    assert len(connected_app.connected_test_client.preflight_calls) == 1


def test_preflight_fails_closed_when_server_capability_is_unproven(
    connected_app: Any,
) -> None:
    connected_app.connected_test_client.version_document["deployment"] = {
        "state": "unknown",
        "reason_code": "deployment_receipt_missing",
    }

    response = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/server/runs/preflight",
            payload=_preflight_request(),
        )
    )

    assert response[0] == 409
    assert response[1]["code"] == "server_capability_unproven"
    assert connected_app.connected_test_client.preflight_calls == []


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("version_id", "different-version"),
        ("target_machine_id", "different-machine"),
        ("offline_policy", "fail_fast"),
    ],
)
def test_preflight_rejects_mismatched_resolved_request(
    connected_app: Any,
    field: str,
    wrong_value: str,
) -> None:
    connected_app.connected_test_client.preflight_document["resolved_request"][field] = wrong_value

    response = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/server/runs/preflight",
            payload=_preflight_request(),
        )
    )

    assert response[0] == 502
    assert response[1]["code"] == "connected_protocol_incompatible"


def test_preflight_rejects_unknown_credential_like_response_field(
    connected_app: Any,
) -> None:
    connected_app.connected_test_client.preflight_document["access_token"] = "central-token-sentinel"

    response = asyncio.run(
        _request(
            connected_app,
            "POST",
            "/server/runs/preflight",
            payload=_preflight_request(),
        )
    )

    assert response[0] == 502
    assert response[1]["code"] == "connected_protocol_incompatible"
    assert "central-token-sentinel" not in json.dumps(response[1])


def test_server_capabilities_reject_invalid_environment_class(
    connected_app: Any,
) -> None:
    connected_app.connected_test_client.version_document["deployment"]["environment_class"] = "arbitrary"

    response = asyncio.run(_request(connected_app, "GET", "/server/capabilities"))

    assert response[0] == 502
    assert response[1]["code"] == "connected_protocol_incompatible"
