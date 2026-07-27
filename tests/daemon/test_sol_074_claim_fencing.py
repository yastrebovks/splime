"""Daemon-side regressions for SOL-074 worker-attempt claim fencing."""

from __future__ import annotations

import json
import logging
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.error import HTTPError

import pytest

import spl.daemon.remote_client as remote_client_module
import spl.daemon.server as daemon_server
from spl.daemon.remote_client import (
    RUN_CLAIM_FENCING_CAPABILITY,
    RUN_CLAIM_FENCING_VERSION,
    RUN_CLAIM_PRIVATE_FIELD,
    RUN_CLAIM_HEADER,
    STALE_RUN_CLAIM_ERROR_CODE,
    ServerClient,
    ServerClientError,
)
from spl.daemon.server import DaemonRuntime
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


def _remote_connection(
    *,
    capabilities: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": "remote-connection-1",
        "owner_id": "owner-a",
        "subject_type": "machine",
        "subject_id": "machine-1",
        "machine_id": "machine-1",
        "display_name": "machine-1",
        "capabilities": capabilities or {},
        "status": "connected",
        "heartbeat_interval_seconds": 60,
    }


def _connected_runtime(
    tmp_path: Path,
    server_client_factory: Any,
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
        server_client_factory=server_client_factory,
    )
    credentials = store.get_server_connection_credentials(connection["id"])
    runtime._mark_server_channel_success(credentials)  # noqa: SLF001
    snapshot_hash, _ = runtime.build_machine_library_snapshot_manifest()
    store.record_server_connection_library_snapshot(
        connection["id"],
        snapshot_hash=snapshot_hash,
    )
    return store, runtime, connection


class _JsonResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def __enter__(self) -> _JsonResponse:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def test_remote_client_claim_header_is_out_of_band_and_private_metadata_is_stripped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim_id = "claim-secret-never-on-json-wire"
    captured: dict[str, Any] = {}

    def fake_open(request: Any, *, timeout: float | None = None) -> _JsonResponse:
        captured["headers"] = {name.casefold(): value for name, value in request.header_items()}
        captured["body"] = request.data
        captured["timeout"] = timeout
        return _JsonResponse({"event_results": [], "jobs": []})

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    client = ServerClient("https://splime.io/api", "machine-token")

    client.sync(
        connection_id="connection-1",
        machine_id="machine-1",
        heartbeat_interval_seconds=60,
        capabilities={RUN_CLAIM_FENCING_CAPABILITY: RUN_CLAIM_FENCING_VERSION},
        claim_id=claim_id,
        events=[
            {
                "id": "event-1",
                "kind": "run_update",
                "payload": {
                    "run_id": "run-1",
                    "status": "running",
                    RUN_CLAIM_PRIVATE_FIELD: claim_id,
                },
                RUN_CLAIM_PRIVATE_FIELD: claim_id,
            }
        ],
    )

    headers = captured["headers"]
    body = captured["body"]
    assert headers[RUN_CLAIM_HEADER.casefold()] == claim_id
    assert claim_id.encode() not in body
    assert RUN_CLAIM_PRIVATE_FIELD.encode() not in body
    payload = json.loads(body)
    assert payload["events"] == [
        {
            "id": "event-1",
            "kind": "run_update",
            "payload": {"run_id": "run-1", "status": "running"},
        }
    ]
    assert payload["capabilities"][RUN_CLAIM_FENCING_CAPABILITY] == 1


def test_remote_client_legacy_sync_omits_claim_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_headers: dict[str, str] = {}

    def fake_open(request: Any, *, timeout: float | None = None) -> _JsonResponse:
        del timeout
        captured_headers.update({name.casefold(): value for name, value in request.header_items()})
        return _JsonResponse({"event_results": [], "jobs": []})

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    ServerClient("https://splime.io/api", "machine-token").sync(
        connection_id="connection-1",
        machine_id="machine-1",
        heartbeat_interval_seconds=60,
        events=[],
    )

    assert RUN_CLAIM_HEADER.casefold() not in captured_headers


def test_remote_client_stale_claim_409_has_stable_code_and_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fake_open(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        raise HTTPError(
            "https://splime.io/api/sync",
            409,
            "Conflict",
            {},
            BytesIO(
                json.dumps(
                    {
                        "code": STALE_RUN_CLAIM_ERROR_CODE,
                        "error": "run was reclaimed by a newer attempt",
                    }
                ).encode("utf-8")
            ),
        )

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    client = ServerClient("https://splime.io/api", "machine-token")

    with pytest.raises(ServerClientError) as captured:
        client.sync(
            connection_id="connection-1",
            machine_id="machine-1",
            heartbeat_interval_seconds=60,
            events=[],
            claim_id="stale-claim",
        )

    assert calls == 1
    assert captured.value.status_code == 409
    assert captured.value.code == STALE_RUN_CLAIM_ERROR_CODE
    assert "reclaimed by a newer attempt" in captured.value.message


def test_remote_client_streaming_upload_carries_optional_claim_header(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            del args

        def read(self) -> bytes:
            return b'{"id":"artifact-1"}'

    def fake_open(request: Any, *, timeout: float | None) -> FakeResponse:
        calls.append(
            {
                "headers": {name.casefold(): value for name, value in request.header_items()},
                "request": (request.get_method(), request.full_url),
                "body": b"".join(request.data),
                "timeout": timeout,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    artifact = tmp_path / "result.bin"
    artifact.write_bytes(b"result")
    client = ServerClient("https://splime.io/api", "machine-token")

    client.upload_artifact("run-1", "result.bin", artifact, claim_id="claim-1")
    client.upload_artifact("run-legacy", "result.bin", artifact)

    assert calls[0]["headers"][RUN_CLAIM_HEADER.casefold()] == "claim-1"
    assert RUN_CLAIM_HEADER.casefold() not in calls[1]["headers"]


class _ClaimSyncServer:
    calls: list[dict[str, Any]] = []
    stale_claims: set[str] = set()

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
        request_timeout_seconds: float | None = None,
    ) -> None:
        del base_url, machine_token, user_token, request_timeout_seconds

    @classmethod
    def reset(cls) -> None:
        cls.calls = []
        cls.stale_claims = set()

    def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        return _remote_connection()

    def latest_machine_library_snapshot(self, machine_id: str) -> dict[str, Any]:
        del machine_id
        return {}

    def sync(self, **kwargs: Any) -> dict[str, Any]:
        type(self).calls.append(kwargs)
        claim_id = kwargs.get("claim_id")
        if claim_id in type(self).stale_claims:
            raise ServerClientError(
                409,
                "run was reclaimed by a newer attempt",
                code=STALE_RUN_CLAIM_ERROR_CODE,
            )
        return {
            "connection": _remote_connection(),
            "event_results": [
                {
                    "event_id": event["id"],
                    "kind": event["kind"],
                    "status": "ok",
                }
                for event in kwargs["events"]
            ],
            "jobs": [],
        }


def test_claimed_outbox_batches_are_claim_homogeneous_and_public_views_are_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ClaimSyncServer.reset()
    store, runtime, connection = _connected_runtime(tmp_path, _ClaimSyncServer)
    claim_a = "claim-a-secret"
    claim_b = "claim-b-secret"
    try:
        monkeypatch.setattr(runtime, "_kick_server_sync", lambda *args, **kwargs: None)
        ordinary = store.enqueue_sync_event(
            "local_run_update",
            {"owner_id": "owner-a", "run": {"id": "local-1"}},
        )
        runtime._send_server_run_update(  # noqa: SLF001
            connection["id"],
            run_id="run-a",
            status="running",
            claim_id=claim_a,
        )
        runtime._send_server_run_update(  # noqa: SLF001
            connection["id"],
            run_id="run-a",
            status="succeeded",
            result={"ok": True},
            claim_id=claim_a,
        )
        runtime._send_server_run_update(  # noqa: SLF001
            connection["id"],
            run_id="run-b",
            status="running",
            claim_id=claim_b,
        )

        stored_claimed = [event for event in store.list_pending_sync_events() if event["kind"] == "run_update"]
        assert len(stored_claimed) == 3
        assert stored_claimed[0]["payload"][RUN_CLAIM_PRIVATE_FIELD] == claim_a
        public_events = runtime.sync_visibility.pending_events()
        assert claim_a not in json.dumps(public_events)
        assert claim_b not in json.dumps(public_events)
        assert all(RUN_CLAIM_PRIVATE_FIELD not in event.get("payload", {}) for event in public_events)

        response = runtime.sync_once()

        assert response["partial"] is False
        claim_batches = [call for call in _ClaimSyncServer.calls if call.get("claim_id")]
        assert [call["claim_id"] for call in claim_batches] == [claim_a, claim_b]
        assert [len(call["events"]) for call in claim_batches] == [2, 1]
        for call in _ClaimSyncServer.calls:
            assert call["capabilities"][RUN_CLAIM_FENCING_CAPABILITY] == 1
            wire_json = json.dumps(call["events"])
            assert RUN_CLAIM_PRIVATE_FIELD not in wire_json
            assert claim_a not in wire_json
            assert claim_b not in wire_json
            if call.get("claim_id"):
                run_ids = {event["payload"]["run_id"] for event in call["events"]}
                assert len(run_ids) == 1
        assert store.get_sync_event(ordinary["id"])["status"] == "sent"
        assert store.list_pending_sync_events() == []
    finally:
        runtime.shutdown()
        store.close()


def test_same_claim_on_different_runs_isolated_into_attempt_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ClaimSyncServer.reset()
    store, runtime, connection = _connected_runtime(tmp_path, _ClaimSyncServer)
    claim_id = "shared-claim-from-injected-outbox"
    try:
        monkeypatch.setattr(runtime, "_kick_server_sync", lambda *args, **kwargs: None)
        for run_id in ("run-a", "run-b"):
            runtime._send_server_run_update(  # noqa: SLF001
                connection["id"],
                run_id=run_id,
                status="running",
                claim_id=claim_id,
            )

        runtime.sync_once()

        assert len(_ClaimSyncServer.calls) == 2
        assert [call["claim_id"] for call in _ClaimSyncServer.calls] == [claim_id, claim_id]
        assert [[event["payload"]["run_id"] for event in call["events"]] for call in _ClaimSyncServer.calls] == [
            ["run-a"],
            ["run-b"],
        ]
    finally:
        runtime.shutdown()
        store.close()


def test_stale_sync_marks_attempt_nonretryable_without_breaking_channel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _ClaimSyncServer.reset()
    stale_claim = "claim-secret-must-never-be-logged"
    _ClaimSyncServer.stale_claims = {stale_claim}
    store, runtime, connection = _connected_runtime(tmp_path, _ClaimSyncServer)
    try:
        monkeypatch.setattr(runtime, "_kick_server_sync", lambda *args, **kwargs: None)
        caplog.set_level(logging.WARNING)
        runtime._send_server_run_update(  # noqa: SLF001
            connection["id"],
            run_id="run-stale",
            status="succeeded",
            result={"wrong": "result"},
            claim_id=stale_claim,
        )
        stale_event = store.list_pending_sync_events()[0]
        healthy_event = store.enqueue_sync_event(
            "local_run_update",
            {"owner_id": "owner-a", "run": {"id": "local-healthy"}},
        )

        response = runtime.sync_once()

        rejected = store.get_sync_event(stale_event["id"])
        assert rejected["status"] == "failed"
        assert rejected["retry"]["will_retry"] is False
        assert "newer attempt" in rejected["error"]
        assert store.get_sync_event(healthy_event["id"])["status"] == "sent"
        assert response["partial"] is False
        assert any(result.get("code") == STALE_RUN_CLAIM_ERROR_CODE for result in response["event_results"])
        assert store.get_server_connection(connection["id"])["status"] == "connected"
        assert (
            runtime._server_channel_breaker_status(  # noqa: SLF001
                store.get_server_connection_credentials(connection["id"])
            )["state"]
            == "closed"
        )
        before = len(store.list_pending_sync_events(limit=None))
        assert (
            runtime._send_server_run_update(  # noqa: SLF001
                connection["id"],
                run_id="run-stale",
                status="failed",
                claim_id=stale_claim,
            )
            is False
        )
        assert len(store.list_pending_sync_events(limit=None)) == before
        assert stale_claim not in caplog.text
        assert "run-stale" in caplog.text
    finally:
        runtime.shutdown()
        store.close()


def test_first_stale_batch_suppresses_all_later_queued_attempt_reports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ClaimSyncServer.reset()
    stale_claim = "claim-with-more-than-one-batch"
    _ClaimSyncServer.stale_claims = {stale_claim}
    store, runtime, connection = _connected_runtime(tmp_path, _ClaimSyncServer)
    try:
        monkeypatch.setattr(runtime, "_kick_server_sync", lambda *args, **kwargs: None)
        for update_number in range(51):
            runtime._send_server_run_update(  # noqa: SLF001
                connection["id"],
                run_id="run-stale-many",
                status="running",
                message=f"progress {update_number}",
                claim_id=stale_claim,
            )

        response = runtime.sync_once()

        assert len(_ClaimSyncServer.calls) == 1
        assert len(_ClaimSyncServer.calls[0]["events"]) == 50
        assert response["partial"] is False
        assert store.list_pending_sync_events(limit=None) == []
        assert store.sync_event_status_summary()["by_status"] == {"failed": 51}
    finally:
        runtime.shutdown()
        store.close()


class _ConnectCaptureServer(_ClaimSyncServer):
    connect_capabilities: list[dict[str, Any]] = []

    @classmethod
    def reset(cls) -> None:
        super().reset()
        cls.connect_capabilities = []

    def connect_machine(self, **kwargs: Any) -> dict[str, Any]:
        type(self).connect_capabilities.append(dict(kwargs["capabilities"]))
        return _remote_connection(capabilities=kwargs["capabilities"])


def test_connect_advertises_claim_fencing_and_overrides_spoofed_version(
    tmp_path: Path,
) -> None:
    _ConnectCaptureServer.reset()
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(
        store,
        heartbeat_service=_NoopHeartbeats(),
        server_client_factory=_ConnectCaptureServer,
    )
    try:
        result = runtime.connect_server(
            server_url="https://splime.io/api",
            machine_token="machine-token-secret",
            user_token="user-token-secret",
            machine_id="machine-1",
            display_name="machine-1",
            capabilities={"python": "3.13", RUN_CLAIM_FENCING_CAPABILITY: 0},
            heartbeat_interval_seconds=60,
        )

        assert result["connected"] is True
        assert _ConnectCaptureServer.connect_capabilities == [
            {
                "python": "3.13",
                RUN_CLAIM_FENCING_CAPABILITY: RUN_CLAIM_FENCING_VERSION,
            }
        ]
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.parametrize("claim_id", [None, "claim-job-secret"])
def test_job_claim_reaches_every_update_progress_result_and_artifact_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    claim_id: str | None,
) -> None:
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    updates: list[dict[str, Any]] = []
    artifact_calls: list[dict[str, Any]] = []
    try:

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

        def wait_run(
            run_id: str,
            *,
            timeout_seconds: float | None,
            progress_callback: Any = None,
            progress_interval_seconds: float = 60,
        ) -> dict[str, Any]:
            del timeout_seconds, progress_interval_seconds
            assert run_id == "local-run-1"
            progress_callback()
            return {"id": run_id, "status": "succeeded", "artifacts_dir": str(tmp_path)}

        def prepare_artifacts(
            connection_id: str,
            run_id: str,
            run_state: dict[str, Any],
            *,
            claim_id: str | None = None,
        ) -> list[dict[str, Any]]:
            artifact_calls.append(
                {
                    "connection_id": connection_id,
                    "run_id": run_id,
                    "run_state": run_state,
                    "claim_id": claim_id,
                }
            )
            return [{"name": "result.txt"}]

        monkeypatch.setattr(runtime, "_send_server_run_update", send_update)
        monkeypatch.setattr(runtime, "_ensure_server_object_envs", lambda versions: None)
        monkeypatch.setattr(
            runtime,
            "register_object",
            lambda *args, **kwargs: {"version_id": "local-version-1"},
        )
        monkeypatch.setattr(
            runtime,
            "start_run",
            lambda *args, **kwargs: {"id": "local-run-1"},
        )
        monkeypatch.setattr(runtime, "_wait_local_run", wait_run)
        monkeypatch.setattr(
            store,
            "get_run",
            lambda run_id: {"result": {"value": 7}, "result_present": True},
        )
        monkeypatch.setattr(runtime, "_prepare_remote_run_artifacts", prepare_artifacts)
        monkeypatch.setattr(
            store,
            "get_server_connection_credentials",
            lambda connection_id: {"heartbeat_interval_seconds": 60},
        )
        job = {
            "run": {
                "id": "remote-run-1",
                "args": [],
                "kwargs": {},
                "timeout_seconds": 30,
            },
            "object_version": {
                "id": "object-1",
                "version_id": "version-1",
                "name": "demo",
                "entrypoint": "demo",
                "env": "default",
                "yaml": "- !DFunction\n  name: demo\n  body: return 7\n",
                "owner_id": "owner-a",
                "library_slug": "default",
                "runtime_config": {"mode": "venv"},
            },
        }
        if claim_id is not None:
            job["claim_id"] = claim_id

        runtime._execute_server_job(job, "connection-1")  # noqa: SLF001

        assert [update["status"] for update in updates] == [
            "fetching_object",
            "running",
            "running",
            "succeeded",
        ]
        assert all(update["claim_id"] == claim_id for update in updates)
        assert updates[-1]["result"] == {"value": 7}
        assert updates[-1]["artifacts"] == [{"name": "result.txt"}]
        assert artifact_calls[0]["claim_id"] == claim_id
    finally:
        runtime.shutdown()
        store.close()


class _ArtifactServer:
    uploads: list[dict[str, Any]] = []
    stale = False

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
    ) -> None:
        del base_url, machine_token, user_token

    @classmethod
    def reset(cls) -> None:
        cls.uploads = []
        cls.stale = False

    def upload_artifact(
        self,
        run_id: str,
        name: str,
        path: str | Path,
        *,
        claim_id: str | None = None,
    ) -> dict[str, Any]:
        type(self).uploads.append({"run_id": run_id, "name": name, "claim_id": claim_id})
        if type(self).stale:
            raise ServerClientError(
                409,
                "run was reclaimed by a newer attempt",
                code=STALE_RUN_CLAIM_ERROR_CODE,
            )
        data = Path(path).read_bytes()
        import hashlib

        return {
            "id": "artifact-1",
            "run_id": run_id,
            "name": name,
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }


def test_direct_artifact_upload_carries_claim_and_stale_upload_supersedes_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _ArtifactServer.reset()
    store, runtime, connection = _connected_runtime(tmp_path, _ArtifactServer)
    claim_id = "direct-upload-secret"
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "result.bin").write_bytes(b"large-result")
    monkeypatch.setattr(daemon_server, "DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES", 0)
    try:
        prepared = runtime._prepare_remote_run_artifacts(  # noqa: SLF001
            connection["id"],
            "run-artifact",
            {"artifacts_dir": str(artifacts_dir)},
            claim_id=claim_id,
        )
        assert prepared[0]["transfer_mode"] == "direct_upload"
        assert _ArtifactServer.uploads[-1]["claim_id"] == claim_id

        _ArtifactServer.stale = True
        caplog.set_level(logging.WARNING)
        with pytest.raises(daemon_server._ServerRunSuperseded):  # noqa: SLF001
            runtime._prepare_remote_run_artifacts(  # noqa: SLF001
                connection["id"],
                "run-stale-artifact",
                {"artifacts_dir": str(artifacts_dir)},
                claim_id=claim_id,
            )
        assert runtime._server_attempt_is_superseded(  # noqa: SLF001
            "run-stale-artifact",
            claim_id,
        )
        assert store.get_server_connection(connection["id"])["status"] == "connected"
        assert claim_id not in caplog.text
    finally:
        runtime.shutdown()
        store.close()


def test_invalid_job_claim_is_refused_without_executing_or_logging_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    executed = False
    try:

        def register(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal executed
            del args, kwargs
            executed = True
            return {}

        monkeypatch.setattr(runtime, "register_object", register)
        caplog.set_level(logging.ERROR)
        runtime._execute_server_job(  # noqa: SLF001
            {
                "claim_id": "   ",
                "run": {"id": "run-invalid-claim"},
                "object_version": {"name": "demo"},
            },
            "connection-1",
        )

        assert executed is False
        assert "run-invalid-claim" in caplog.text
        assert "claim_id" not in caplog.text
    finally:
        runtime.shutdown()
        store.close()


def test_job_exception_fallback_failure_keeps_claim_and_superseded_attempt_skips_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    updates: list[tuple[str, str | None]] = []
    claim_id = "claim-fallback-secret"
    job = {
        "claim_id": claim_id,
        "run": {"id": "run-fallback"},
        "object_version": {
            "name": "demo",
            "entrypoint": "demo",
            "env": "default",
            "yaml": "- !DFunction\n  name: demo\n  body: return 1\n",
        },
    }
    try:

        def send_update(
            connection_id: str,
            *,
            run_id: str,
            status: str,
            claim_id: str | None = None,
            **kwargs: Any,
        ) -> bool:
            del connection_id, run_id, kwargs
            if runtime._server_attempt_is_superseded("run-fallback", claim_id):  # noqa: SLF001
                return False
            updates.append((status, claim_id))
            return True

        monkeypatch.setattr(runtime, "_send_server_run_update", send_update)
        monkeypatch.setattr(runtime, "_ensure_server_object_envs", lambda versions: None)
        monkeypatch.setattr(
            runtime,
            "register_object",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("registration failed")),
        )

        runtime._execute_server_job(job, "connection-1")  # noqa: SLF001

        assert updates == [
            ("fetching_object", claim_id),
            ("failed", claim_id),
        ]

        updates.clear()
        runtime._mark_server_attempt_superseded("run-fallback", claim_id)  # noqa: SLF001
        runtime._execute_server_job(job, "connection-1")  # noqa: SLF001
        assert updates == []
    finally:
        runtime.shutdown()
        store.close()
