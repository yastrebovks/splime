from __future__ import annotations

from copy import deepcopy
import threading
from typing import Any

import pytest

from spl.daemon.connected_ide import SERVER_REMOTE_CAPABILITY_IDS
from spl.daemon.home_lock import DaemonInstanceIdentity
from spl.daemon.lifecycle import (
    LifecycleController,
    LifecycleError,
    validate_deployed_no_claim_capability,
)
from spl.daemon.remote_client import (
    SYNC_CLAIM_CONTROL_ACK,
    SYNC_FLUSH_WITHOUT_CLAIM_CAPABILITY,
    ServerClient,
    ServerClientError,
)


class _RecordingClient(ServerClient):
    def __init__(self, response: dict[str, Any]) -> None:
        super().__init__("https://splime.invalid/api", "machine-token")
        self.response = response
        self.payloads: list[dict[str, Any]] = []

    def _json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        assert method == "POST"
        assert path == "/sync"
        assert kwargs["allow_transport_retries"] is False
        assert payload is not None
        self.payloads.append(payload)
        return deepcopy(self.response)


def _sync(client: ServerClient, *, claim_jobs: bool | None = None) -> dict[str, Any]:
    return client.sync(
        connection_id="connection-1",
        machine_id="machine-1",
        heartbeat_interval_seconds=30,
        events=[],
        claim_jobs=claim_jobs,
    )


def _legacy_response() -> dict[str, Any]:
    return {
        "connection": {"id": "connection-1"},
        "event_results": [],
        "jobs": [],
    }


def _no_claim_response() -> dict[str, Any]:
    return {
        **_legacy_response(),
        "claim_control": dict(SYNC_CLAIM_CONTROL_ACK),
    }


def _valid_version_document() -> dict[str, Any]:
    capabilities = [
        *sorted(SERVER_REMOTE_CAPABILITY_IDS),
        SYNC_FLUSH_WITHOUT_CLAIM_CAPABILITY,
    ]
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
                "daemon_server_capabilities": capabilities,
            },
            "source_repository": "gitlab.com/spl894143/spl-server",
            "source_ref": "v0.4.6",
            "source_binding": "pinned_commit",
            "source_commit": "a" * 40,
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


def test_omitted_sync_field_preserves_exact_legacy_payload_and_needs_no_ack() -> None:
    client = _RecordingClient(_legacy_response())

    assert _sync(client) == _legacy_response()
    assert "claim_jobs" not in client.payloads[0]


def test_explicit_true_remains_claim_capable_without_no_claim_ack() -> None:
    client = _RecordingClient(_legacy_response())

    _sync(client, claim_jobs=True)
    assert client.payloads[0]["claim_jobs"] is True


def test_explicit_false_requires_exact_ack_and_empty_jobs() -> None:
    client = _RecordingClient(_no_claim_response())

    assert _sync(client, claim_jobs=False)["claim_control"] == SYNC_CLAIM_CONTROL_ACK
    assert client.payloads[0]["claim_jobs"] is False


@pytest.mark.parametrize(
    "response",
    [
        _legacy_response(),
        {**_no_claim_response(), "claim_control": {**SYNC_CLAIM_CONTROL_ACK, "extra": True}},
        {**_no_claim_response(), "claim_control": {**SYNC_CLAIM_CONTROL_ACK, "claim_suppressed": False}},
        {**_no_claim_response(), "jobs": [{"id": "unexpected-job"}]},
        {**_no_claim_response(), "jobs": None},
    ],
)
def test_no_claim_sync_rejects_absent_malformed_contradictory_and_job_responses(
    response: dict[str, Any],
) -> None:
    with pytest.raises(ServerClientError) as captured:
        _sync(_RecordingClient(response), claim_jobs=False)
    assert captured.value.code == "sync_claim_control_invalid"


def test_sync_rejects_non_boolean_claim_control() -> None:
    with pytest.raises(ValueError, match="boolean"):
        _sync(_RecordingClient(_legacy_response()), claim_jobs=0)  # type: ignore[arg-type]


def test_no_claim_capability_requires_exact_verified_deployment_binding() -> None:
    document = _valid_version_document()
    validate_deployed_no_claim_capability(document)

    missing = deepcopy(document)
    missing["declared"]["contracts"]["daemon_server_capabilities"].remove(SYNC_FLUSH_WITHOUT_CLAIM_CAPABILITY)
    with pytest.raises(LifecycleError) as absent:
        validate_deployed_no_claim_capability(missing)
    assert absent.value.code == "server_no_claim_capability_unproven"

    downgraded = deepcopy(document)
    downgraded["deployment"]["source_commit"] = "d" * 40
    with pytest.raises(Exception):
        validate_deployed_no_claim_capability(downgraded)

    malformed = deepcopy(document)
    malformed["future"] = True
    with pytest.raises(Exception):
        validate_deployed_no_claim_capability(malformed)


def test_old_server_failure_can_be_cancelled_then_current_server_can_stop() -> None:
    identity = DaemonInstanceIdentity(
        instance_id="a" * 32,
        home_hash="b" * 64,
        generation=7,
        previous_generation=6,
        pid=123,
        started_at="2026-08-02T10:00:00+00:00",
    )
    exited = threading.Event()
    proof = "supervisor-proof-value"
    controller = LifecycleController(
        identity,
        supervisor_id="supervisor-1",
        supervisor_proof=proof,
        binding_id="binding-1",
        exit_callback=exited.set,
    )

    def drain_request(request_id: str) -> dict[str, Any]:
        return {
            "schema": "spl.daemon.lifecycle-drain-request",
            "schema_version": 1,
            "expected_instance_id": identity.instance_id,
            "expected_generation": identity.generation,
            "expected_admission_revision": controller.admission_revision,
            "request_id": request_id,
            "mode": "when_idle",
            "deadline_ms": 30_000,
        }

    old_document = _valid_version_document()
    old_document["declared"]["contracts"]["daemon_server_capabilities"].remove(SYNC_FLUSH_WITHOUT_CLAIM_CAPABILITY)
    old_receipt = controller.request_drain(
        drain_request("old-server-drain"),
        supervisor_proof=proof,
    )
    with pytest.raises(LifecycleError):
        validate_deployed_no_claim_capability(old_document)
    controller.mark_claim_boundary_failed(outcome_unknown=False)

    failed = controller.status_document()
    assert failed["drain"]["claim_boundary"] == "failed"
    assert failed["drain"]["blockers"]["unknown_work"] == 0
    assert not exited.is_set()

    controller.cancel_drain(
        {
            "schema": "spl.daemon.lifecycle-cancel-request",
            "schema_version": 1,
            "expected_instance_id": identity.instance_id,
            "expected_generation": identity.generation,
            "expected_admission_revision": controller.admission_revision,
            "request_id": "cancel-old-server-drain",
            "drain_id": old_receipt["drain_id"],
        },
        supervisor_proof=proof,
    )

    validate_deployed_no_claim_capability(_valid_version_document())
    controller.request_drain(
        drain_request("current-server-drain"),
        supervisor_proof=proof,
    )
    controller.mark_claim_boundary_proven()
    assert exited.wait(1)
