from __future__ import annotations

from copy import deepcopy
from io import BytesIO
import json
from typing import Any

import pytest

from spl.daemon.connected_ide import (
    CONNECTED_CREDENTIALS_REVEAL_REQUEST_SCHEMA,
    CONNECTED_REMOTE_RUN_PREFLIGHT_REQUEST_SCHEMA,
    MAX_REMOTE_RUN_EVENT_ITEMS,
    MAX_REMOTE_RUN_ITEMS,
    MAX_UPSTREAM_RESPONSE_BYTES,
    SERVER_REMOTE_CAPABILITY_IDS,
    ConnectedIDEContractError,
    build_connected_credentials_reveal_response,
    build_remote_run_events_response,
    build_remote_run_list_response,
    build_remote_run_preflight_response,
    parse_connected_credentials_reveal_request,
    parse_remote_run_preflight_request,
    project_remote_run_server_capabilities,
    validate_connected_credentials_reveal_response,
    validate_remote_run_events_response,
    validate_remote_run_list_response,
    validate_remote_run_preflight_response,
)
from spl.daemon.ide_metadata import LOCAL_CAPABILITY_VERSIONS
from spl.daemon.remote_client import ServerClient, ServerClientError


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


def _preflight_request_bytes(**updates: Any) -> bytes:
    request = {
        "object": "demo-object",
        "version_id": "version-1",
        "target_machine_id": "machine-1",
        "object_owner_id": "owner-1",
        "library": "default",
        "offline_policy": "queue",
    }
    request.update(updates)
    return json.dumps(
        {
            "schema": CONNECTED_REMOTE_RUN_PREFLIGHT_REQUEST_SCHEMA,
            "schema_version": 1,
            "request": request,
        }
    ).encode()


def test_reveal_contract_exposes_only_persisted_binding_and_role_secrets() -> None:
    raw = json.dumps(
        {
            "schema": CONNECTED_CREDENTIALS_REVEAL_REQUEST_SCHEMA,
            "schema_version": 1,
            "connection_id": "connection-1",
            "intent": "reveal_reusable_central_credentials",
        }
    ).encode()
    request = parse_connected_credentials_reveal_request(raw)

    document = build_connected_credentials_reveal_response(
        {
            "id": request.connection_id,
            "remote_connection_id": "remote-connection-1",
            "owner_id": "owner-1",
            "machine_id": "machine-1",
            "server_url": "https://splime.io/api",
            "user_token": "user-secret",
            "token": "machine-secret",
            "token_secret_ref": "must-not-pass",
        },
        observed_at="2026-08-02T10:00:00.000Z",
    )

    validate_connected_credentials_reveal_response(document)
    assert document == {
        "schema": "spl.daemon.connected-credentials-reveal",
        "schema_version": 1,
        "connection": {
            "connection_id": "connection-1",
            "remote_connection_id": "remote-connection-1",
            "owner_id": "owner-1",
            "machine_id": "machine-1",
            "server_url": "https://splime.io/api",
        },
        "credentials": [
            {"role": "user", "secret": "user-secret"},
            {"role": "machine", "secret": "machine-secret"},
        ],
        "observed_at": "2026-08-02T10:00:00.000Z",
    }
    assert "token_secret_ref" not in json.dumps(document)
    assert "credential_id" not in json.dumps(document)
    assert "scopes" not in json.dumps(document)
    assert "expires_at" not in json.dumps(document)


@pytest.mark.parametrize(
    "server_url",
    [
        "http://splime.io/api",
        "https://user:secret@splime.io/api",
        "https://splime.io/api?token=secret",
        "https://splime.io/api#secret",
    ],
)
def test_reveal_contract_rejects_unproven_or_stateful_server_url(
    server_url: str,
) -> None:
    with pytest.raises(ConnectedIDEContractError) as captured:
        build_connected_credentials_reveal_response(
            {
                "id": "connection-1",
                "remote_connection_id": "remote-connection-1",
                "owner_id": "owner-1",
                "machine_id": "machine-1",
                "server_url": server_url,
                "user_token": "user-secret",
                "token": "machine-secret",
            }
        )
    assert captured.value.code == "request_invalid"


def test_reveal_contract_accepts_explicit_loopback_http_and_rejects_daemon_token() -> None:
    document = build_connected_credentials_reveal_response(
        {
            "id": "connection-1",
            "remote_connection_id": "remote-connection-1",
            "owner_id": "owner-1",
            "machine_id": "machine-1",
            "server_url": "http://127.0.0.1:8500/api/",
            "user_token": "user-secret",
            "token": "machine-secret",
        },
        forbidden_values=("daemon-master-token",),
    )
    assert document["connection"]["server_url"] == "http://127.0.0.1:8500/api"

    with pytest.raises(ConnectedIDEContractError) as captured:
        build_connected_credentials_reveal_response(
            {
                "id": "connection-1",
                "remote_connection_id": "remote-connection-1",
                "owner_id": "owner-1",
                "machine_id": "machine-1",
                "server_url": "https://splime.io/api",
                "user_token": "daemon-master-token",
                "token": "machine-secret",
            },
            forbidden_values=("daemon-master-token",),
        )
    assert captured.value.code == "credential_reveal_unavailable"


def test_preflight_parser_is_exact_version_target_and_field_closed() -> None:
    parsed = parse_remote_run_preflight_request(_preflight_request_bytes(version=3, function="score"))
    assert parsed.payload == {
        "object": "demo-object",
        "version_id": "version-1",
        "target_machine_id": "machine-1",
        "object_owner_id": "owner-1",
        "library": "default",
        "version": 3,
        "function": "score",
        "offline_policy": "queue",
    }

    with pytest.raises(ConnectedIDEContractError) as remote_field:
        parse_remote_run_preflight_request(_preflight_request_bytes(access_token="forbidden"))
    assert remote_field.value.code == "request_invalid"

    missing_version = json.loads(_preflight_request_bytes())
    del missing_version["request"]["version_id"]
    with pytest.raises(ConnectedIDEContractError) as missing:
        parse_remote_run_preflight_request(json.dumps(missing_version).encode())
    assert missing.value.code == "request_invalid"


def test_remote_run_wrappers_bound_counts_and_preserve_partial_truth() -> None:
    runs = [{"id": f"run-{index}"} for index in range(MAX_REMOTE_RUN_ITEMS + 1)]
    events = [{"id": f"event-{index}"} for index in range(MAX_REMOTE_RUN_EVENT_ITEMS + 1)]

    run_document = build_remote_run_list_response(
        runs,
        observed_at="2026-08-02T10:00:00.000Z",
    )
    event_document = build_remote_run_events_response(
        events,
        observed_at="2026-08-02T10:00:00.000Z",
    )

    validate_remote_run_list_response(run_document)
    validate_remote_run_events_response(event_document)
    assert run_document["truncated"] is True
    assert run_document["completeness"] == "partial"
    assert len(run_document["runs"]) == MAX_REMOTE_RUN_ITEMS
    assert event_document["truncated"] is True
    assert event_document["completeness"] == "partial"
    assert len(event_document["events"]) == MAX_REMOTE_RUN_EVENT_ITEMS

    contradictory = deepcopy(run_document)
    contradictory["completeness"] = "complete"
    with pytest.raises(ConnectedIDEContractError) as captured:
        validate_remote_run_list_response(contradictory)
    assert captured.value.code == "server_response_invalid"


def test_remote_run_list_validates_only_the_returned_bounded_prefix() -> None:
    runs = [{"id": f"run-{index}"} for index in range(MAX_REMOTE_RUN_ITEMS)]
    runs.extend({"id": f"ignored-{index}", "payload": ["x"] * 10_001} for index in range(2))

    document = build_remote_run_list_response(
        runs,
        observed_at="2026-08-02T10:00:00.000Z",
    )

    assert document["truncated"] is True
    assert document["completeness"] == "partial"
    assert len(document["runs"]) == MAX_REMOTE_RUN_ITEMS
    assert all(str(run["id"]).startswith("run-") for run in document["runs"])


def test_remote_run_list_accepts_browser_bound_production_shaped_rows() -> None:
    runs = [
        {
            "id": f"run-{index}",
            "owner_id": "owner-1",
            "target_owner_id": "owner-1",
            "object_owner_id": "owner-1",
            "requester_type": "user",
            "requester_id": "user-1",
            "access_token_id": "token-1",
            "object_library_id": "library-1",
            "target_machine_id": "machine-1",
            "object_id": "object-1",
            "object_version_id": "version-1",
            "object_name": "demo-object",
            "object_version": 1,
            "entrypoint": "demo:run",
            "env": "python",
            "status": "succeeded",
            "args": [index],
            "kwargs": {"value": index},
            "output": None,
            "timeout_seconds": 30,
            "correlation_id": f"correlation-{index}",
            "parent_run_id": None,
            "context": {"source": "test"},
            "result": {"value": index},
            "result_present": True,
            "result_normalized": {"value": index},
            "error": None,
            "created_at": "2026-08-04T10:00:00.000Z",
            "assigned_at": "2026-08-04T10:00:01.000Z",
            "last_claimed_at": "2026-08-04T10:00:01.000Z",
            "job_lease_expires_at": None,
            "claim_attempts": 1,
            "max_claim_attempts": 3,
            "active_claim_present": False,
            "stale_attempt_rejections": 0,
            "started_at": "2026-08-04T10:00:01.000Z",
            "finished_at": "2026-08-04T10:00:02.000Z",
            "updated_at": "2026-08-04T10:00:02.000Z",
        }
        for index in range(MAX_REMOTE_RUN_ITEMS)
    ]

    document = build_remote_run_list_response(
        runs,
        observed_at="2026-08-04T10:00:03.000Z",
    )

    validate_remote_run_list_response(document)
    assert document["truncated"] is False
    assert document["completeness"] == "complete"
    assert len(document["runs"]) == MAX_REMOTE_RUN_ITEMS


def test_server_capability_projection_requires_exact_deployment_evidence() -> None:
    projected = project_remote_run_server_capabilities(
        _valid_version_document(),
        observed_at="2026-08-02T10:00:00.000Z",
    )
    assert projected["release"] == {
        "state": "verified",
        "release_id": "splime-0.4.6",
        "version": "0.4.6",
        "evidence": "deployment_receipt",
        "reason": None,
    }
    assert all(
        capability == {"state": "supported", "reason": None} for capability in projected["capabilities"].values()
    )

    fabricated = {
        "contract": "version_authority/v1",
        "schema_version": 1,
        "component": "server",
        "declared": {
            "state": "present",
            "release_id": "splime-0.4.6",
            "version": "0.4.6",
            "contracts": {"daemon_server_capabilities": list(SERVER_REMOTE_CAPABILITY_IDS)},
        },
        "deployment": {
            "state": "present",
            "reason_code": "deployment_receipt_present",
            "release_id": "splime-0.4.6",
            "version": "0.4.6",
        },
        "database_schema": {"current": 32, "target": 32},
    }
    with pytest.raises(ConnectedIDEContractError) as minimal:
        project_remote_run_server_capabilities(fabricated)
    assert minimal.value.code == "connected_protocol_incompatible"


def test_server_capability_projection_handles_old_absent_and_malformed_catalogs() -> None:
    old = _valid_version_document()
    del old["declared"]["contracts"]["daemon_server_capabilities"]
    projected = project_remote_run_server_capabilities(old)
    assert all(
        item
        == {
            "state": "unavailable",
            "reason": "server_capability_evidence_absent",
        }
        for item in projected["capabilities"].values()
    )

    malformed = _valid_version_document()
    malformed["declared"]["contracts"]["daemon_server_capabilities"].append("../fixture-secret")
    with pytest.raises(ConnectedIDEContractError) as unsafe:
        project_remote_run_server_capabilities(malformed)
    assert unsafe.value.code == "connected_protocol_incompatible"

    mismatched = _valid_version_document()
    mismatched["deployment"]["release_id"] = "splime-9.9.9"
    with pytest.raises(ConnectedIDEContractError) as mismatch:
        project_remote_run_server_capabilities(mismatched)
    assert mismatch.value.code == "connected_protocol_incompatible"


def test_server_capability_projection_requires_release_receipt_hash_shapes() -> None:
    accepted = project_remote_run_server_capabilities(_valid_version_document())
    assert accepted["release"]["state"] == "verified"

    invalid_commit = _valid_version_document()
    invalid_commit["deployment"]["source_commit"] = "a" * 39
    with pytest.raises(ConnectedIDEContractError) as commit:
        project_remote_run_server_capabilities(invalid_commit)
    assert commit.value.code == "connected_protocol_incompatible"

    invalid_artifact = _valid_version_document()
    invalid_artifact["deployment"]["artifact_sha256"] = "b" * 63
    with pytest.raises(ConnectedIDEContractError) as artifact:
        project_remote_run_server_capabilities(invalid_artifact)
    assert artifact.value.code == "connected_protocol_incompatible"

    invalid_manifest = _valid_version_document()
    invalid_manifest["deployment"]["release_manifest_sha256"] = "c" * 65
    with pytest.raises(ConnectedIDEContractError) as manifest:
        project_remote_run_server_capabilities(invalid_manifest)
    assert manifest.value.code == "connected_protocol_incompatible"


def test_server_capability_projection_rejects_unknown_environment_class() -> None:
    invalid = _valid_version_document()
    invalid["deployment"]["environment_class"] = "arbitrary"

    with pytest.raises(ConnectedIDEContractError) as captured:
        project_remote_run_server_capabilities(invalid)

    assert captured.value.code == "connected_protocol_incompatible"


def test_preflight_response_binds_validated_server_evidence_and_nonmutation() -> None:
    server_capabilities = project_remote_run_server_capabilities(
        _valid_version_document(),
        observed_at="2026-08-02T10:00:00.000Z",
    )
    document = build_remote_run_preflight_response(
        _valid_preflight(),
        server_capabilities,
        parse_remote_run_preflight_request(_preflight_request_bytes()).payload,
        observed_at="2026-08-02T10:01:00.000Z",
    )
    validate_remote_run_preflight_response(document)
    assert document["preflight"]["point_in_time"] is True
    assert document["preflight"]["reservation"] is False

    mutating_claim = deepcopy(document)
    mutating_claim["preflight"]["reservation"] = True
    with pytest.raises(ConnectedIDEContractError) as captured:
        validate_remote_run_preflight_response(mutating_claim)
    assert captured.value.code == "connected_protocol_incompatible"


class RecordingConnectedServerClient(ServerClient):
    def __init__(self) -> None:
        super().__init__(
            "https://splime.io/api",
            "machine-secret",
            user_token="user-secret",
        )
        self.calls: list[tuple[str, str, dict[str, Any] | None, dict[str, Any]]] = []

    def _json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        self.calls.append((method, path, payload, kwargs))
        if path == "/remote-runs":
            return [{"id": "run-1"}]
        if path.endswith("/detail"):
            return {"run": {"id": "run-1"}}
        if path.endswith("/events"):
            return [{"id": "event-1"}]
        if path == "/version":
            return _valid_version_document()
        if path == "/remote-runs/preflight":
            return _valid_preflight()
        raise AssertionError(path)


def test_connected_server_client_methods_use_user_auth_and_bounded_reads() -> None:
    client = RecordingConnectedServerClient()

    client.list_remote_runs()
    client.get_remote_run_detail("run-1")
    client.list_remote_run_events("run-1")
    client.get_server_version()
    client.preflight_remote_run({"object": "demo-object"})

    assert [call[1] for call in client.calls] == [
        "/remote-runs",
        "/remote-runs/run-1/detail",
        "/remote-runs/run-1/events",
        "/version",
        "/remote-runs/preflight",
    ]
    assert all(call[3]["auth"] == "user" for call in client.calls)
    assert all(call[3]["max_response_bytes"] == MAX_UPSTREAM_RESPONSE_BYTES for call in client.calls)
    assert client.calls[-1][3]["post_send_retry_safe"] is True


def test_connected_server_client_rejects_oversized_response_before_decode() -> None:
    with pytest.raises(ServerClientError) as captured:
        ServerClient._read_json_text(
            BytesIO(b"x" * (MAX_UPSTREAM_RESPONSE_BYTES + 1)),
            max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
        )
    assert captured.value.code == "server_response_too_large"


def test_daemon_metadata_advertises_only_approved_connected_facades() -> None:
    assert {
        key: LOCAL_CAPABILITY_VERSIONS[key]
        for key in (
            "connected.credentials.reveal",
            "run.remote.list",
            "run.remote.detail",
            "run.remote.events",
            "run.remote.preflight",
        )
    } == {
        "connected.credentials.reveal": 1,
        "run.remote.list": 1,
        "run.remote.detail": 1,
        "run.remote.events": 1,
        "run.remote.preflight": 1,
    }
    assert "run.remote.create" not in LOCAL_CAPABILITY_VERSIONS
    assert "run.remote.cancel" not in LOCAL_CAPABILITY_VERSIONS
    assert "run.remote.retry" not in LOCAL_CAPABILITY_VERSIONS
