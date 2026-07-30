"""Privacy and truth contracts for daemon-authored Worker operations evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.server import (
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


def _runtime(tmp_path: Path) -> tuple[RegistryStore, DaemonRuntime]:
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    return store, runtime


def _record_build(
    store: RegistryStore,
    tmp_path: Path,
    *,
    spec_hash: str,
    status: str,
    runtime_type: str,
    secret: str,
) -> dict[str, Any]:
    record = store.upsert_environment_build(
        spec_hash=spec_hash,
        base_python=str(tmp_path / secret / "python"),
        python_version="Python 3.13.5",
        distributions=[{"package": secret, "version": "1"}],
        runtime_packages=[{"package": f"{secret}-runtime", "version": "1"}],
        spec={"private": secret},
        venv_path=tmp_path / secret / "venv",
        python_path=tmp_path / secret / "venv" / "bin" / "python",
        install_log_path=tmp_path / secret / "install.log",
        status=status,
        runtime_type=runtime_type,
        image_tag=f"{secret}:latest" if runtime_type == "docker" else None,
        base_image=f"{secret}:base" if runtime_type == "docker" else None,
    )
    if status == "failed":
        record = store.update_environment_build(
            spec_hash,
            status=status,
            error=f"{secret}-build-error",
        )
    return record


def test_worker_operations_capability_is_exact_aggregate_and_allowlisted(
    tmp_path: Path,
) -> None:
    store, runtime = _runtime(tmp_path)
    private = "must-never-cross-worker-operations"
    try:
        store.enqueue_sync_event(
            "object_version",
            {
                "object_id": private,
                "token": f"{private}-token",
                "payload": f"{private}-payload",
            },
        )
        retryable_failed = store.enqueue_sync_event(
            "run_update",
            {"run_id": private, "error": f"{private}-payload-error"},
        )
        store.mark_sync_event_failed(
            retryable_failed["id"],
            f"{private}-queue-error",
            retryable=True,
        )
        sent = store.enqueue_sync_event("object_version", {"object_id": f"{private}-sent"})
        store.mark_sync_event_sent(sent["id"])
        _record_build(
            store,
            tmp_path,
            spec_hash="venv-build",
            status="ready",
            runtime_type="venv",
            secret=f"{private}-venv",
        )
        _record_build(
            store,
            tmp_path,
            spec_hash="docker-build",
            status="failed",
            runtime_type="docker",
            secret=f"{private}-docker",
        )

        capabilities = runtime._authoritative_server_capabilities(  # noqa: SLF001
            {
                "custom": "preserved",
                WORKER_OPERATIONS_CAPABILITY: {
                    "spoofed": private,
                    "path": str(tmp_path / private),
                },
            }
        )
        operations = capabilities[WORKER_OPERATIONS_CAPABILITY]

        assert capabilities["custom"] == "preserved"
        assert set(operations) == {
            "schema_version",
            "observed_at",
            "sync",
            "environment_builds",
            "runtimes",
            "diagnostics",
        }
        assert operations["schema_version"] == WORKER_OPERATIONS_SCHEMA_VERSION
        assert operations["sync"] == {
            "evidence": "observed",
            "pending": 1,
            "retryable": 2,
            "by_status": {
                "pending": 1,
                "failed": 1,
                "sent": 1,
            },
            "oldest_pending_at": operations["sync"]["oldest_pending_at"],
        }
        assert operations["sync"]["oldest_pending_at"] is not None
        assert operations["environment_builds"] == {
            "evidence": "observed",
            "total": 2,
            "by_status": {
                "absent": 0,
                "creating": 0,
                "ready": 1,
                "failed": 1,
            },
            "runtime_types": ["docker", "venv"],
            "latest_updated_at": operations["environment_builds"]["latest_updated_at"],
        }
        assert operations["environment_builds"]["latest_updated_at"] is not None
        assert operations["runtimes"] == {
            "implemented_object_modes": ["docker", "venv"],
            "implemented_node_modes": ["docker", "native", "venv-subprocess"],
            "availability": "unverified",
            "reason": "runtime_availability_not_probed",
        }
        assert operations["diagnostics"] == {
            "availability": "local_only",
            "command": "spl-daemon doctor --json",
            "sharing": "explicit_consent_required",
        }

        encoded = json.dumps(operations, sort_keys=True)
        assert private not in encoded
        for prohibited_key in (
            "object_id",
            "token",
            "payload",
            "error",
            "path",
            "distributions",
            "runtime_packages",
            "spec",
            "image_tag",
            "base_image",
        ):
            assert prohibited_key not in encoded
    finally:
        runtime.shutdown()
        store.close()


def test_worker_operations_sync_counts_are_not_limited_to_diagnostic_page(
    tmp_path: Path,
) -> None:
    store, runtime = _runtime(tmp_path)
    try:
        for index in range(205):
            store.enqueue_sync_event("object_version", {"sequence": index})

        sync = runtime._worker_operations_capability()["sync"]  # noqa: SLF001

        assert sync["evidence"] == "observed"
        assert sync["pending"] == 205
        assert sync["retryable"] == 205
        assert sync["by_status"] == {
            "pending": 205,
            "failed": 0,
            "sent": 0,
        }
    finally:
        runtime.shutdown()
        store.close()


def test_worker_operations_invalid_or_missing_local_evidence_stays_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, runtime = _runtime(tmp_path)
    try:
        monkeypatch.setattr(store, "sync_event_status_summary", lambda: {})
        monkeypatch.setattr(
            store,
            "list_environment_builds",
            lambda: [
                {
                    "status": "ready",
                    "runtime_type": "venv",
                    "updated_at": None,
                }
            ],
        )

        operations = runtime._worker_operations_capability()  # noqa: SLF001

        assert operations["sync"] == {
            "evidence": "unknown",
            "pending": None,
            "retryable": None,
            "by_status": {
                "pending": None,
                "failed": None,
                "sent": None,
            },
            "oldest_pending_at": None,
            "reason": "worker_sync_summary_unavailable",
        }
        assert operations["environment_builds"] == {
            "evidence": "unknown",
            "total": None,
            "by_status": {
                "absent": None,
                "creating": None,
                "ready": None,
                "failed": None,
            },
            "runtime_types": None,
            "latest_updated_at": None,
            "reason": "worker_environment_build_summary_unavailable",
        }
    finally:
        runtime.shutdown()
        store.close()
