"""Claim-bound remote execution telemetry producer contracts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore
from spl.daemon.telemetry import TelemetryLevel


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


def _job(claim_id: str | None) -> dict[str, Any]:
    job: dict[str, Any] = {
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
    return job


def _state(status: str, secret: str) -> dict[str, Any]:
    succeeded = status == "succeeded"
    return {
        "id": "local-run-1",
        "object": "demo",
        "status": status,
        "input": {
            "args": [],
            "kwargs": {"api_key": secret},
        },
        "result": {"echo": secret} if succeeded else None,
        "result_present": succeeded,
        "error": None if succeeded else f"ValueError: api_key={secret}",
        "stdout": f"stdout token={secret}",
        "stderr": f"stderr token={secret}",
        "artifacts_dir": "",
        "created_at": "2026-07-31T01:00:00+00:00",
        "started_at": "2026-07-31T01:00:01+00:00",
        "finished_at": "2026-07-31T01:00:02+00:00",
        "manifest": {
            "pipeline": {"content_hash": "pipeline-hash"},
            "nodes": {},
            "edges": [],
        },
    }


def _exercise_remote_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    outcome: str,
    claim_id: str | None = "claim-1",
    telemetry: TelemetryLevel = "diagnostic",
) -> list[dict[str, Any]]:
    secret = "REMOTE_TERMINAL_SECRET_MARKER"
    terminal_state = _state(
        "running" if outcome == "exception" else outcome,
        secret,
    )
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(
        store,
        heartbeat_service=_NoopHeartbeats(),
        telemetry=telemetry,
    )
    updates: list[dict[str, Any]] = []

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
                "payload": payload or {},
                "artifacts": artifacts or [],
                "claim_id": claim_id,
            }
        )
        return True

    def wait_run(
        run_id: str,
        *,
        timeout_seconds: float | None,
        progress_callback: Any = None,
        progress_interval_seconds: float = 60,
    ) -> dict[str, Any]:
        del run_id, timeout_seconds, progress_interval_seconds
        if progress_callback is not None:
            progress_callback()
        if outcome == "exception":
            raise RuntimeError("remote handoff failed")
        return terminal_state

    monkeypatch.setattr(runtime, "_send_server_run_update", send_update)
    monkeypatch.setattr(runtime, "_ensure_server_object_envs", lambda versions: None)
    monkeypatch.setattr(
        runtime,
        "register_object",
        lambda *args, **kwargs: {"version_id": "local-version-1"},
    )
    monkeypatch.setattr(runtime, "start_run", lambda *args, **kwargs: {"id": "local-run-1"})
    monkeypatch.setattr(runtime, "_wait_local_run", wait_run)
    monkeypatch.setattr(store, "get_run", lambda run_id: terminal_state)
    monkeypatch.setattr(
        runtime,
        "_claim_bound_manifest_evidence",
        lambda *args, **kwargs: (None, {}),
    )
    monkeypatch.setattr(
        runtime,
        "_prepare_remote_run_artifacts",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        runtime,
        "_mark_remote_local_terminal_queued",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        store,
        "get_server_connection_credentials",
        lambda connection_id: {"heartbeat_interval_seconds": 60},
    )
    try:
        runtime._execute_server_job(_job(claim_id), "connection-1")  # noqa: SLF001
        return updates
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "exception"])
def test_claimed_terminal_paths_attach_execution_telemetry_only_to_final_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    updates = _exercise_remote_job(
        tmp_path,
        monkeypatch,
        outcome=outcome,
    )

    assert all("execution_telemetry" not in update["payload"] for update in updates[:-1])
    terminal = updates[-1]
    assert terminal["status"] == ("succeeded" if outcome == "succeeded" else "failed")
    if outcome == "succeeded":
        assert terminal["result"] == {
            "echo": "REMOTE_TERMINAL_SECRET_MARKER",
        }
    evidence = terminal["payload"]["execution_telemetry"]
    assert evidence["schema_version"] == 1
    assert evidence["functional_delivery"] == "independent"
    assert evidence["level"] == "diagnostic"
    assert evidence["availability"]["input"] is False
    assert evidence["availability"]["result"] is False
    assert evidence["availability"]["streams"] is False
    assert evidence["availability"]["artifact_bodies"] is False
    assert "REMOTE_TERMINAL_SECRET_MARKER" not in json.dumps(evidence, sort_keys=True)


def test_unclaimed_legacy_job_omits_execution_telemetry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    updates = _exercise_remote_job(
        tmp_path,
        monkeypatch,
        outcome="succeeded",
        claim_id=None,
    )

    assert all("execution_telemetry" not in update["payload"] for update in updates)
