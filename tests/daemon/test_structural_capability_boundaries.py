"""Negative contracts for capabilities deliberately deferred from this slice."""

from __future__ import annotations

from pathlib import Path

from spl import SPLClient
from spl.daemon.remote_client import ServerClient as WorkerServerClient
from spl.daemon.run_lifecycle import REMOTE_RUN_STATUSES
from spl.server_client import SPLServerClient, ServerRemoteRun


def test_central_execution_does_not_expose_streaming_resume_or_approval() -> None:
    assert hasattr(SPLClient, "resume")
    assert hasattr(ServerRemoteRun, "retry")
    assert not hasattr(ServerRemoteRun, "resume")
    assert not hasattr(SPLServerClient, "resume_run")
    assert not hasattr(WorkerServerClient, "stream_logs")
    assert not hasattr(WorkerServerClient, "follow_logs")
    assert not hasattr(WorkerServerClient, "approve_run")
    assert "pending_approval" not in REMOTE_RUN_STATUSES


def test_daemon_docs_keep_deferred_capability_language_explicit() -> None:
    root = Path(__file__).resolve().parents[2]
    content = (root / "docs/source/daemon-security-telemetry.rst").read_text(encoding="utf-8")

    assert "no server-push, SSE, WebSocket, live-log, tail, or streaming transport" in content
    assert "There is no central Resume command" in content
    assert "Execution approval is a separately approved 0.5 roadmap contract" in content
