"""Daemon side of the signature contract (stage 1.3).

KEEP IN SYNC with ``spl-server/tests/test_signature_contract.py``: both files
pin the SAME ``EXPECTED_CORE`` for the same reference object. The daemon and
the server duplicate their signature builders; this contract makes any
semantic drift in the shared core (name/kind/version/env provenance/
inputs/outputs) fail CI on the side that drifted. Envelope keys beyond the
core may differ by design (daemon adds display_name/origin, the server adds
owner/library/execution).

The env fields are version provenance, not an execution decision: the daemon
syncs the author's env name, interpreter path and interpreter version, while
runtime execution resolves a local interpreter by env name/default/daemon.
"""

from __future__ import annotations

import inspect
import sys
from pathlib import Path

from spl.daemon.remote_client import (
    RUN_CLAIM_FENCING_CAPABILITY,
    RUN_CLAIM_FENCING_VERSION,
    RUN_CLAIM_HEADER,
    ServerClient,
)
from spl.daemon.runtime_dependencies import ServerClientProtocol
from spl.daemon.server import DaemonRuntime
from spl.daemon.run_lifecycle import (
    CANONICAL_RUN_STATUSES,
    LOCAL_RUN_STATUSES,
    REMOTE_RUN_STATUSES,
    RUN_TRANSITIONS,
)
from spl.daemon.signature import build_signature
from spl.daemon.store import RegistryStore

REFERENCE_YAML = (
    "- !DFunction\n"
    "  name: demo_obj\n"
    "  inputs: []\n"
    "  outputs:\n"
    "  - name: default\n"
    "    type: int\n"
    "  body: |-\n"
    "    return 1\n"
)
AUTHOR_ENV = "author_env"
AUTHOR_ENV_PYTHON = "/opt/spl-author/python3.13/bin/python"
AUTHOR_ENV_PYTHON_VERSION = "Python 3.13.0"

# Shared with the server test — do not edit one side only.
EXPECTED_CORE = {
    "name": "demo_obj",
    "kind": "function",
    "version": 1,
    "env": AUTHOR_ENV,
    "env_python": AUTHOR_ENV_PYTHON,
    "env_python_version": AUTHOR_ENV_PYTHON_VERSION,
    "inputs": [],
    "outputs": [
        {
            "name": "default",
            "notes": "Function calls return the function value directly.",
            "ports": [{"name": "default", "type": "int"}],
            "read": "result.value",
            "result_accessor": "result.value",
            "selector": None,
            "type": "int",
            "value_path": [],
        }
    ],
}
REQUIRED_SHARED_KEYS = {
    "name",
    "kind",
    "version",
    "version_id",
    "description",
    "env",
    "env_python",
    "env_python_version",
    "inputs",
    "outputs",
    "call",
    "internal_functions",
}

# Shared with the server contract — do not edit one side only. All fields are
# additive: a 0.4.3 producer may omit every one of them.
EXPECTED_044_ADDITIVE_CONTRACT = {
    "owner_fields": ("owner_handle", "owned"),
    "resolution_envelopes": ("resolution", "resolved_from"),
    "resolution_fields": (
        "auto_resolved",
        "requested_library",
        "resolved_owner_id",
        "resolved_owner_handle",
        "resolved_library",
        "resolved_library_id",
    ),
    "whoami_fields": (
        "id",
        "owner_id",
        "handle",
        "display_name",
        "server_url",
        "machine_id",
        "connection_status",
        "live",
    ),
}

# Shared with the server contract. A 0.4.4 daemon omits this field, while a
# 0.4.5 daemon uses it to distinguish explicit publication from reconciliation.
EXPECTED_045_OBJECT_SYNC_ADDITIVE_CONTRACT = {
    "object_sync_fields": ("revive_removed",),
    "legacy_optional": True,
}

# Shared with the server contract. Old jobs omit ``claim_id``; fenced daemons
# advertise version 1 and carry the optional capability only in this header.
EXPECTED_045_RUN_CLAIM_ADDITIVE_CONTRACT = {
    "job_field": "claim_id",
    "request_header": "X-Spl-Claim",
    "capability": ("run_claim_fencing", 1),
    "legacy_optional": True,
}

# Shared with the server contract. A new daemon reports whether JSON null is
# an explicit result; a 0.4.4 daemon omits the additive presence field.
EXPECTED_045_RESULT_PRESENCE_ADDITIVE_CONTRACT = {
    "local_run_field": "result_present",
    "detail_field": "result_present",
    "detail_value_field": "result_value",
    "legacy_unreadable_fields": ("result_unreadable", "result_json"),
    "legacy_optional": True,
}

# Shared with the 0.4.5 server contract. A pre-0.4.5 local-run payload may
# omit every field and is classified as legacy_full rather than metadata.
EXPECTED_045_TELEMETRY_ADDITIVE_CONTRACT = {
    "local_run_fields": (
        "telemetry_level",
        "telemetry",
        "source_result_present",
        "input_mirrored",
        "result_mirrored",
        "streams_mirrored",
        "artifact_bodies_mirrored",
    ),
    "legacy_level": "legacy_full",
    "legacy_optional": True,
}

EXPECTED_RUN_TRANSITIONS = {
    "local": {
        "queued": ("failed", "starting"),
        "starting": ("failed", "preparing_environment", "running"),
        "preparing_environment": ("failed", "running"),
        "running": ("failed", "succeeded"),
        "succeeded": (),
        "failed": (),
    },
    "remote": {
        "queued": ("assigned", "cancelled"),
        "assigned": ("cancelled", "failed", "fetching_object", "preparing", "running", "stale", "succeeded"),
        "fetching_object": ("cancelled", "failed", "preparing", "running", "stale"),
        "preparing": ("cancelled", "failed", "running", "stale"),
        "running": ("cancelled", "failed", "stale", "succeeded"),
        "succeeded": (),
        "failed": (),
        "cancelled": (),
        "stale": (),
    },
    "lease_expired": {
        "assigned": ("failed", "queued"),
        "fetching_object": ("failed", "queued"),
        "preparing": ("failed", "queued"),
        "running": ("failed", "queued"),
    },
}


def test_run_lifecycle_transition_contract() -> None:
    actual = {
        mode: {source: tuple(sorted(targets)) for source, targets in transitions.items()}
        for mode, transitions in RUN_TRANSITIONS.items()
    }

    assert actual == EXPECTED_RUN_TRANSITIONS
    assert LOCAL_RUN_STATUSES == {
        "queued",
        "starting",
        "preparing_environment",
        "running",
        "succeeded",
        "failed",
    }
    assert REMOTE_RUN_STATUSES == {
        "queued",
        "assigned",
        "fetching_object",
        "preparing",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "stale",
    }
    assert CANONICAL_RUN_STATUSES == LOCAL_RUN_STATUSES | REMOTE_RUN_STATUSES


def test_044_additive_contract_is_named_and_legacy_payloads_may_omit_it() -> None:
    legacy_library = {"id": "library-1", "owner_id": "owner-1", "slug": "solo"}
    legacy_run = {"id": "run-1", "status": "queued"}

    assert EXPECTED_044_ADDITIVE_CONTRACT["owner_fields"] == (
        "owner_handle",
        "owned",
    )
    assert EXPECTED_044_ADDITIVE_CONTRACT["resolution_envelopes"] == (
        "resolution",
        "resolved_from",
    )
    assert all(field not in legacy_library for field in EXPECTED_044_ADDITIVE_CONTRACT["owner_fields"])
    assert all(field not in legacy_run for field in EXPECTED_044_ADDITIVE_CONTRACT["resolution_envelopes"])


def test_045_object_sync_revive_intent_is_additive() -> None:
    legacy_payload = {"name": "demo_obj", "yaml": REFERENCE_YAML}

    assert EXPECTED_045_OBJECT_SYNC_ADDITIVE_CONTRACT == {
        "object_sync_fields": ("revive_removed",),
        "legacy_optional": True,
    }
    assert "revive_removed" not in legacy_payload


def test_045_run_claim_contract_is_additive_and_keyword_optional() -> None:
    legacy_job = {"run": {"id": "run-1"}, "object_version": {"id": "version-1"}}

    assert EXPECTED_045_RUN_CLAIM_ADDITIVE_CONTRACT == {
        "job_field": "claim_id",
        "request_header": "X-Spl-Claim",
        "capability": ("run_claim_fencing", 1),
        "legacy_optional": True,
    }
    assert RUN_CLAIM_HEADER == EXPECTED_045_RUN_CLAIM_ADDITIVE_CONTRACT["request_header"]
    assert (
        RUN_CLAIM_FENCING_CAPABILITY,
        RUN_CLAIM_FENCING_VERSION,
    ) == EXPECTED_045_RUN_CLAIM_ADDITIVE_CONTRACT["capability"]
    assert DaemonRuntime._job_claim_id(legacy_job) is None  # noqa: SLF001
    assert DaemonRuntime._job_claim_id({**legacy_job, "claim_id": "claim-1"}) == "claim-1"  # noqa: SLF001
    for method in (ServerClient.sync, ServerClientProtocol.sync):
        parameter = inspect.signature(method).parameters["claim_id"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is None
    for method in (ServerClient.upload_artifact, ServerClientProtocol.upload_artifact):
        parameter = inspect.signature(method).parameters["claim_id"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is None


def test_045_result_presence_contract_is_additive() -> None:
    legacy_run = {"id": "run-1", "result": None}
    current_run = {**legacy_run, "result_present": True}

    assert EXPECTED_045_RESULT_PRESENCE_ADDITIVE_CONTRACT == {
        "local_run_field": "result_present",
        "detail_field": "result_present",
        "detail_value_field": "result_value",
        "legacy_unreadable_fields": ("result_unreadable", "result_json"),
        "legacy_optional": True,
    }
    assert "result_present" not in legacy_run
    assert current_run["result_present"] is True


def test_local_run_telemetry_contract_is_additive() -> None:
    legacy_run = {"id": "run-1", "status": "succeeded"}
    current_run = {
        **legacy_run,
        **dict.fromkeys(EXPECTED_045_TELEMETRY_ADDITIVE_CONTRACT["local_run_fields"]),
    }

    assert EXPECTED_045_TELEMETRY_ADDITIVE_CONTRACT == {
        "local_run_fields": (
            "telemetry_level",
            "telemetry",
            "source_result_present",
            "input_mirrored",
            "result_mirrored",
            "streams_mirrored",
            "artifact_bodies_mirrored",
        ),
        "legacy_level": "legacy_full",
        "legacy_optional": True,
    }
    assert all(field not in legacy_run for field in EXPECTED_045_TELEMETRY_ADDITIVE_CONTRACT["local_run_fields"])
    assert all(field in current_run for field in EXPECTED_045_TELEMETRY_ADDITIVE_CONTRACT["local_run_fields"])


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection: object, *, token: str) -> None:
        pass

    def ensure_server_heartbeat(self, connection: object | None = None) -> None:
        pass

    def status(self, connection_id: str | None = None) -> dict[str, object]:
        return {"connection_id": connection_id, "thread_alive": False, "last_tick_at": None}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


def test_daemon_signature_matches_shared_contract(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env(AUTHOR_ENV, sys.executable)
        version = store.objects.register_object(
            "demo_obj",
            "demo_obj",
            AUTHOR_ENV,
            yaml_text=REFERENCE_YAML,
        )
        with store._lock, store._conn:  # noqa: SLF001 - contract seeds author provenance.
            store._conn.execute(
                "UPDATE object_versions SET env_python = ? WHERE id = ?",
                (AUTHOR_ENV_PYTHON, version["version_id"]),
            )
        store.objects._python_version_cache[  # noqa: SLF001 - cache pin keeps the contract host-independent.
            str(Path(AUTHOR_ENV_PYTHON).expanduser().absolute())
        ] = AUTHOR_ENV_PYTHON_VERSION
        signature = build_signature(store.objects.get_object("demo_obj"))
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
        )
        sync_event = runtime.enqueue_object_sync(version)
    finally:
        store.close()

    core = {key: signature[key] for key in EXPECTED_CORE}
    assert core == EXPECTED_CORE
    assert REQUIRED_SHARED_KEYS <= set(signature)
    assert {
        "env": sync_event["payload"]["env"],
        "env_python": sync_event["payload"]["env_python"],
        "env_python_version": sync_event["payload"]["env_python_version"],
        "revive_removed": sync_event["payload"]["revive_removed"],
    } == {
        "env": AUTHOR_ENV,
        "env_python": AUTHOR_ENV_PYTHON,
        "env_python_version": AUTHOR_ENV_PYTHON_VERSION,
        "revive_removed": True,
    }


def test_remote_run_receipt_preserves_resolution_annotations(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    runtime = None
    try:
        store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-123456",
            user_token="user-token-123456",
            connection={
                "id": "remote-connection-1",
                "owner_id": "user-self",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": "machine-1",
                "display_name": "lab-machine",
                "status": "connected",
                "capabilities": {},
            },
            heartbeat_interval_seconds=60,
        )
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
        )
        credentials = store.current_server_connection_credentials()
        assert credentials is not None
        runtime._mark_server_channel_success(credentials)
        captured_requests = []
        resolution = {
            "auto_resolved": True,
            "requested_library": "default",
            "resolved_owner_id": "user-a",
            "resolved_owner_handle": "alice",
            "resolved_library": "default",
            "resolved_library_id": "library-a",
        }
        expected = {
            "id": "run-1",
            "resolution": resolution,
            "resolved_from": dict(resolution),
        }

        class CapturingServer:
            def create_remote_run(self, payload, *, idempotency_key):
                captured_requests.append((payload, idempotency_key))
                return expected

        runtime._server_client_for_credentials = lambda *args, **kwargs: CapturingServer()
        runtime._kick_server_sync = lambda *args, **kwargs: None
        receipt = runtime.start_remote_run(
            "demo_obj",
            target_machine="machine-1",
            object_owner_id="@alice",
            library="default",
            correlation_id="logical-run-1",
        )
        retry_receipt = runtime.start_remote_run(
            "demo_obj",
            target_machine="machine-1",
            object_owner_id="@alice",
            library="default",
            correlation_id="logical-run-1",
        )

        assert receipt == expected
        assert retry_receipt == expected
        assert set(receipt["resolution"]) == set(EXPECTED_044_ADDITIVE_CONTRACT["resolution_fields"])
        assert set(receipt["resolved_from"]) == set(EXPECTED_044_ADDITIVE_CONTRACT["resolution_fields"])
        assert captured_requests[0][0]["object_owner_id"] == "@alice"
        assert captured_requests[0][0]["correlation_id"] == "logical-run-1"
        assert captured_requests[1][1] == captured_requests[0][1]
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()
