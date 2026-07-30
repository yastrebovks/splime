"""Approved additive daemon contracts for lifecycle, lineage, and build evidence."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

import spl.daemon.server as daemon_server
from spl.daemon.server import (
    EXECUTION_MANIFEST_CAPABILITY,
    EXECUTION_MANIFEST_CAPABILITY_VERSION,
    WORKER_BUILD_CAPABILITY,
    WORKER_BUILD_SCHEMA_VERSION,
    WORKER_OPERATIONS_CAPABILITY,
    WORKER_OPERATIONS_SCHEMA_VERSION,
    DaemonRuntime,
)
from spl.daemon.store import RegistryStore


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection: dict[str, Any], *, token: str) -> None:
        pass

    def ensure_server_heartbeat(self, connection: dict[str, Any] | None = None) -> None:
        pass

    def status(self, connection_id: str | None = None) -> dict[str, Any]:
        return {"connection_id": connection_id, "thread_alive": False}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


def _remote_connection() -> dict[str, Any]:
    return {
        "id": "remote-connection-1",
        "owner_id": "owner-a",
        "subject_type": "machine",
        "subject_id": "machine-1",
        "machine_id": "machine-1",
        "display_name": "machine-1",
        "capabilities": {},
        "status": "connected",
        "heartbeat_interval_seconds": 60,
    }


class _EventResultServer:
    error_code = "library_archived"

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
        request_timeout_seconds: float | None = None,
    ) -> None:
        del base_url, machine_token, user_token, request_timeout_seconds

    def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        return _remote_connection()

    def sync(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "connection": _remote_connection(),
            "event_results": [
                {
                    "event_id": event["id"],
                    "kind": event["kind"],
                    "status": "error",
                    "code": type(self).error_code,
                    "error": f"rejected with {type(self).error_code}",
                }
                for event in kwargs["events"]
            ],
            "jobs": [],
        }


def _connected_runtime(
    tmp_path: Path,
    server_factory: Any = _EventResultServer,
) -> tuple[RegistryStore, DaemonRuntime, dict[str, Any]]:
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    runtime = DaemonRuntime(
        store,
        heartbeat_service=_NoopHeartbeats(),
        server_client_factory=server_factory,
    )
    credentials = store.get_server_connection_credentials(connection["id"])
    runtime._mark_server_channel_success(credentials)  # noqa: SLF001
    snapshot_hash, _ = runtime.build_machine_library_snapshot_manifest()
    store.record_server_connection_library_snapshot(
        connection["id"],
        snapshot_hash=snapshot_hash,
    )
    return store, runtime, connection


@pytest.mark.parametrize("code", ["library_archived", "resource_archived"])
def test_archived_sync_rejection_is_terminal_for_that_event(
    tmp_path: Path,
    code: str,
) -> None:
    _EventResultServer.error_code = code
    store, runtime, connection = _connected_runtime(tmp_path)
    try:
        event = store.enqueue_sync_event(
            "object_version",
            {"owner_id": "owner-a", "library": "archived"},
        )

        response = runtime.sync_once()

        rejected = store.get_sync_event(event["id"])
        assert rejected["status"] == "failed"
        assert rejected["retry"]["will_retry"] is False
        assert response["partial"] is False
        assert response["event_results"][0]["code"] == code
        assert store.get_server_connection_credentials(connection["id"])["status"] == "connected"
    finally:
        runtime.shutdown()
        store.close()


def test_unrecognized_sync_rejection_remains_retryable_for_legacy_behavior(
    tmp_path: Path,
) -> None:
    _EventResultServer.error_code = "temporary_policy_failure"
    store, runtime, _ = _connected_runtime(tmp_path)
    try:
        event = store.enqueue_sync_event(
            "object_version",
            {"owner_id": "owner-a", "library": "risk"},
        )

        runtime.sync_once()

        rejected = store.get_sync_event(event["id"])
        assert rejected["status"] == "failed"
        assert rejected["retry"]["will_retry"] is True
    finally:
        runtime.shutdown()
        store.close()


def _local_terminal_state() -> dict[str, Any]:
    return {
        "id": "local-run-1",
        "status": "succeeded",
        "manifest": {
            "schema_version": 1,
            "run_id": "local-run-1",
            "status": "succeeded",
            "finished_at": "2026-07-29T12:00:00+00:00",
            "pipeline": {
                "object_version_id": "local-version-1",
                "content_hash": "c" * 64,
            },
            "nodes": {
                "node-a": {
                    "id": "node-a",
                    "alias": "producer",
                    "outputs": {
                        "result": {
                            "kind": "artifact",
                            "sha256": "a" * 64,
                            "ref": {
                                "uri": "artifacts/result.bin",
                                "sha256": "a" * 64,
                                "size": 7,
                            },
                        }
                    },
                }
            },
            "edges": [],
        },
    }


def test_manifest_evidence_requires_claim_and_exact_server_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    monkeypatch.setattr(
        store,
        "get_object_version",
        lambda version_id, *, include_yaml=False: {
            "version_id": version_id,
            "remote_version_id": "server-version-1",
        },
    )
    try:
        absent, producers = runtime._claim_bound_manifest_evidence(  # noqa: SLF001
            _local_terminal_state(),
            claim_id=None,
            server_object_version_id="server-version-1",
        )
        mismatch, mismatch_producers = runtime._claim_bound_manifest_evidence(  # noqa: SLF001
            _local_terminal_state(),
            claim_id="claim-1",
            server_object_version_id="server-version-other",
        )
        evidence, producers = runtime._claim_bound_manifest_evidence(  # noqa: SLF001
            _local_terminal_state(),
            claim_id="claim-1",
            server_object_version_id="server-version-1",
        )

        assert absent is None
        assert producers
        assert mismatch is None
        assert mismatch_producers == {}
        assert evidence is not None
        assert set(evidence) == {
            "schema_version",
            "digest_sha256",
            "captured_at",
            "source",
            "summary",
        }
        assert evidence["summary"]["object_version_id"] == "server-version-1"
        producer = runtime._artifact_producer_evidence(  # noqa: SLF001
            {"name": "result.bin", "sha256": "a" * 64, "size": 7},
            manifest_evidence=evidence,
            artifact_producers=producers,
        )
        assert producer == {
            "manifest_digest_sha256": evidence["digest_sha256"],
            "node_id": "node-a",
            "alias": "producer",
            "output_port": "result",
        }
    finally:
        runtime.shutdown()
        store.close()


def test_remote_artifact_carries_only_exact_manifest_producer_evidence(
    tmp_path: Path,
) -> None:
    store, runtime, connection = _connected_runtime(tmp_path)
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    artifact = artifacts_dir / "result.bin"
    artifact.write_bytes(b"payload")
    digest = hashlib.sha256(b"payload").hexdigest()
    manifest_digest = "d" * 64
    try:
        prepared = runtime._prepare_remote_run_artifacts(  # noqa: SLF001
            connection["id"],
            "run-1",
            {"artifacts_dir": str(artifacts_dir)},
            claim_id="claim-1",
            manifest_evidence={"digest_sha256": manifest_digest},
            artifact_producers={
                ("result.bin", digest, 7): {
                    "node_id": "node-a",
                    "alias": "producer",
                    "output_port": "result",
                }
            },
        )

        assert prepared[0]["producer_evidence"] == {
            "manifest_digest_sha256": manifest_digest,
            "node_id": "node-a",
            "alias": "producer",
            "output_port": "result",
        }
        assert prepared[0]["transfer_mode"] == "inline_base64"
        assert "manifest" not in prepared[0]
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.parametrize("claim_id", [None, "claim-1"])
def test_terminal_run_update_includes_manifest_evidence_only_with_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    claim_id: str | None,
) -> None:
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    state = {
        **_local_terminal_state(),
        "result": {"value": 7},
        "result_present": True,
        "artifacts_dir": str(tmp_path),
    }
    updates: list[dict[str, Any]] = []
    artifact_kwargs: list[dict[str, Any]] = []

    def send_update(
        connection_id: str,
        *,
        run_id: str,
        status: str,
        result: Any = None,
        error: str | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        claim_id: str | None = None,
    ) -> bool:
        updates.append(
            {
                "connection_id": connection_id,
                "run_id": run_id,
                "status": status,
                "result": result,
                "error": error,
                "message": message,
                "payload": payload,
                "artifacts": artifacts,
                "claim_id": claim_id,
            }
        )
        return True

    def prepare_artifacts(
        connection_id: str,
        run_id: str,
        run_state: dict[str, Any],
        *,
        claim_id: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        artifact_kwargs.append(
            {
                "connection_id": connection_id,
                "run_id": run_id,
                "run_state": run_state,
                "claim_id": claim_id,
                **kwargs,
            }
        )
        return []

    monkeypatch.setattr(runtime, "_send_server_run_update", send_update)
    monkeypatch.setattr(runtime, "_ensure_server_object_envs", lambda versions: None)
    monkeypatch.setattr(
        runtime,
        "register_object",
        lambda *args, **kwargs: {"version_id": "local-version-1"},
    )
    monkeypatch.setattr(runtime, "start_run", lambda *args, **kwargs: {"id": "local-run-1"})
    monkeypatch.setattr(runtime, "_wait_local_run", lambda *args, **kwargs: state)
    monkeypatch.setattr(store, "get_run", lambda run_id: state)
    monkeypatch.setattr(
        store,
        "get_object_version",
        lambda version_id, *, include_yaml=False: {
            "version_id": version_id,
            "remote_version_id": "server-version-1",
        },
    )
    monkeypatch.setattr(runtime, "_prepare_remote_run_artifacts", prepare_artifacts)
    monkeypatch.setattr(runtime, "_mark_remote_local_terminal_queued", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        store,
        "get_server_connection_credentials",
        lambda connection_id: {"heartbeat_interval_seconds": 60},
    )
    job = {
        "run": {
            "id": "server-run-1",
            "args": [],
            "kwargs": {},
            "timeout_seconds": 30,
        },
        "object_version": {
            "id": "object-1",
            "version_id": "server-version-1",
            "name": "demo",
            "entrypoint": "demo",
            "env": "default",
            "yaml": "- !DFunction\n  name: demo\n  body: return 7\n",
            "owner_id": "owner-a",
            "library_slug": "default",
        },
    }
    if claim_id is not None:
        job["claim_id"] = claim_id
    try:
        runtime._execute_server_job(job, "connection-1")  # noqa: SLF001

        terminal = updates[-1]
        assert terminal["status"] == "succeeded"
        assert terminal["claim_id"] == claim_id
        if claim_id is None:
            assert "manifest_evidence" not in terminal["payload"]
            assert "manifest_evidence" not in artifact_kwargs[0]
        else:
            evidence = terminal["payload"]["manifest_evidence"]
            assert set(evidence) == {
                "schema_version",
                "digest_sha256",
                "captured_at",
                "source",
                "summary",
            }
            assert artifact_kwargs[0]["manifest_evidence"] == evidence
            assert artifact_kwargs[0]["artifact_producers"]
    finally:
        runtime.shutdown()
        store.close()


def test_daemon_overwrites_spoofed_manifest_and_build_capabilities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(daemon_server.importlib_metadata, "version", lambda name: "0.4.6")
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    try:
        capabilities = runtime._authoritative_server_capabilities(  # noqa: SLF001
            {
                EXECUTION_MANIFEST_CAPABILITY: {
                    "full_manifest": True,
                    "schema_version": 999,
                },
                WORKER_BUILD_CAPABILITY: {
                    "package_version": "9.9.9",
                    "artifact_sha256": "spoofed",
                },
            }
        )

        assert capabilities[EXECUTION_MANIFEST_CAPABILITY] == {
            "schema_version": EXECUTION_MANIFEST_CAPABILITY_VERSION,
            "terminal_summary": True,
            "artifact_producer_evidence": True,
            "full_manifest": False,
        }
        assert capabilities[WORKER_BUILD_CAPABILITY] == {
            "schema_version": WORKER_BUILD_SCHEMA_VERSION,
            "package": "splime",
            "package_version": "0.4.6",
            "version_evidence": "installed_distribution_metadata",
            "artifact_sha256": None,
            "source_ref": None,
            "protocols": {
                "run_claim_fencing": 1,
                EXECUTION_MANIFEST_CAPABILITY: EXECUTION_MANIFEST_CAPABILITY_VERSION,
                WORKER_OPERATIONS_CAPABILITY: WORKER_OPERATIONS_SCHEMA_VERSION,
            },
        }
    finally:
        runtime.shutdown()
        store.close()
