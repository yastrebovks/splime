from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import spl.daemon.heartbeat_service as heartbeat_service_module
from spl.daemon.heartbeat_service import HeartbeatService
from spl.daemon.remote_client import ServerClientError
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
        return {"connection_id": connection_id, "thread_alive": False, "last_tick_at": None}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


class _BatchServer:
    calls: list[dict[str, Any]] = []
    timeouts: list[float | None] = []

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
        request_timeout_seconds: float | None = None,
    ) -> None:
        del base_url, machine_token, user_token
        type(self).timeouts.append(request_timeout_seconds)

    @classmethod
    def reset(cls) -> None:
        cls.calls = []
        cls.timeouts = []

    def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
        return _remote_connection()

    def latest_machine_library_snapshot(self, machine_id: str) -> dict[str, Any]:
        del machine_id
        return {}

    def sync(self, **kwargs: Any) -> dict[str, Any]:
        type(self).calls.append(kwargs)
        return {
            "connection": _remote_connection(),
            "event_results": [
                {"event_id": event["id"], "kind": event["kind"], "status": "ok", "result": {}}
                for event in kwargs["events"]
            ],
            "jobs": [],
        }


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


def _runtime(tmp_path: Path) -> tuple[RegistryStore, DaemonRuntime, dict[str, Any]]:
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
        server_client_factory=_BatchServer,
    )
    runtime._mark_server_channel_success(store.get_server_connection_credentials(connection["id"]))  # noqa: SLF001
    snapshot_hash, _ = runtime.build_machine_library_snapshot_manifest()
    store.record_server_connection_library_snapshot(connection["id"], snapshot_hash=snapshot_hash)
    return store, runtime, connection


def test_sync_backlog_is_bounded_by_count_bytes_and_timeout(tmp_path: Path) -> None:
    _BatchServer.reset()
    store, runtime, _ = _runtime(tmp_path)
    try:
        for index in range(121):
            store.enqueue_sync_event(
                "local_run_update",
                {
                    "owner_id": "owner-a",
                    "run": {
                        "id": f"run-{index}",
                        "stdout": "x" * (100_000 if index < 8 else 2_000),
                    },
                },
            )

        runtime.sync_once()

        event_batches = [
            [event for event in call["events"] if event["kind"] == "local_run_update"] for call in _BatchServer.calls
        ]
        assert len(event_batches) >= 3
        assert max(map(len, event_batches)) <= 50
        assert max(len(json.dumps(call["events"]).encode("utf-8")) for call in _BatchServer.calls) <= 512 * 1_024
        assert _BatchServer.timeouts and set(_BatchServer.timeouts) == {15.0}
        assert store.list_pending_sync_events() == []
    finally:
        runtime.shutdown()
        store.close()


def test_oversized_sync_event_is_terminal_failed_and_does_not_block_queue(tmp_path: Path) -> None:
    _BatchServer.reset()
    store, runtime, _ = _runtime(tmp_path)
    try:
        poison = store.enqueue_sync_event(
            "local_run_update",
            {"owner_id": "owner-a", "run": {"id": "poison", "stdout": "x" * (600 * 1_024)}},
        )
        healthy = store.enqueue_sync_event(
            "local_run_update",
            {"owner_id": "owner-a", "run": {"id": "healthy", "stdout": "ok"}},
        )

        runtime.sync_once()

        poisoned = store.get_sync_event(poison["id"])
        assert poisoned["status"] == "failed"
        assert poisoned["retry"]["will_retry"] is False
        assert "exceeds" in poisoned["error"]
        assert store.get_sync_event(healthy["id"])["status"] == "sent"
    finally:
        runtime.shutdown()
        store.close()


def test_unserializable_sync_event_is_terminal_failed_and_does_not_block_queue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _BatchServer.reset()
    store, runtime, _ = _runtime(tmp_path)
    try:
        poison = store.enqueue_sync_event(
            "local_run_update",
            {"owner_id": "owner-a", "run": {"id": "poison"}},
        )
        healthy = store.enqueue_sync_event(
            "local_run_update",
            {"owner_id": "owner-a", "run": {"id": "healthy", "stdout": "ok"}},
        )
        original_list = store.list_pending_sync_events

        def corrupted_pending(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            events = original_list(*args, **kwargs)
            for event in events:
                if event["id"] == poison["id"]:
                    event["payload"]["not_json"] = {"set-values"}
            return events

        monkeypatch.setattr(store, "list_pending_sync_events", corrupted_pending)

        runtime.sync_once()

        poisoned = store.get_sync_event(poison["id"])
        assert poisoned["status"] == "failed"
        assert poisoned["retry"]["will_retry"] is False
        assert "not JSON serializable" in poisoned["error"]
        assert store.get_sync_event(healthy["id"])["status"] == "sent"
    finally:
        runtime.shutdown()
        store.close()


def test_sync_event_retryability_migration_is_guarded_and_idempotent(tmp_path: Path) -> None:
    database = sqlite3.connect(tmp_path / "daemon.sqlite3")
    database.execute(
        """
        CREATE TABLE sync_events (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            sent_at TEXT,
            error TEXT
        )
        """
    )
    database.commit()
    database.close()

    for _ in range(2):
        store = RegistryStore(tmp_path)
        try:
            with store._lock:  # noqa: SLF001 - migration contract inspects the physical schema.
                columns = {
                    row["name"]
                    for row in store._conn.execute("PRAGMA table_info(sync_events)").fetchall()  # noqa: SLF001
                }
            assert "retryable" in columns
        finally:
            store.close()


@pytest.mark.parametrize("status_code", [401, 409])
def test_lease_rejection_backs_off_and_heartbeat_loop_recovers(
    tmp_path: Path,
    status_code: int,
) -> None:
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    stop = threading.Event()
    calls = 0

    def sync_once(**kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        del kwargs
        calls += 1
        if calls == 1:
            raise ServerClientError(status_code, "stale lease")
        store.record_server_connection_heartbeat(connection["id"], remote_connection=_remote_connection())
        stop.set()
        return {"connected": True}

    service = HeartbeatService(
        store,
        sync_once,
        initial_backoff_seconds=0.001,
        max_backoff_seconds=0.002,
        watchdog_interval_seconds=0.01,
    )
    try:
        service._server_heartbeat_loop(connection["id"], "machine-token-secret", stop)  # noqa: SLF001

        assert calls == 2
        assert store.get_server_connection(connection["id"])["status"] == "connected"
    finally:
        service.shutdown()
        store.close()


def test_restore_starts_needs_reconnect_heartbeat_without_explicit_connect(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    store.record_server_connection_error(connection["id"], status="needs_reconnect", error="stale lease")
    called = threading.Event()

    def sync_once(**kwargs: Any) -> dict[str, Any]:
        del kwargs
        called.set()
        return {"connected": True}

    service = HeartbeatService(store, sync_once)
    try:
        service.restore_server_heartbeat()
        assert called.wait(timeout=1)
    finally:
        service.shutdown()
        store.close()


def test_restore_credential_failure_is_retried_by_watchdog_without_aborting_startup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = RegistryStore(tmp_path)
    store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    original_current = store.current_server_connection_credentials
    restored = threading.Event()
    reads = 0

    def flaky_current() -> dict[str, Any] | None:
        nonlocal reads
        reads += 1
        if reads == 1:
            raise OSError("secret store temporarily unavailable during restore")
        return original_current()

    def sync_once(**kwargs: Any) -> dict[str, Any]:
        del kwargs
        restored.set()
        return {"connected": True}

    monkeypatch.setattr(store, "current_server_connection_credentials", flaky_current)
    service = HeartbeatService(store, sync_once, watchdog_interval_seconds=0.01)
    try:
        with caplog.at_level(logging.ERROR):
            service.restore_server_heartbeat()

        assert restored.wait(timeout=1)
        assert reads >= 2
        assert "credentials unavailable during restore" in caplog.text
    finally:
        service.shutdown()
        store.close()


@pytest.mark.parametrize("status_code", [401, 409])
def test_runtime_rehandshakes_after_one_rejected_lease_and_whoami_becomes_live(
    tmp_path: Path,
    status_code: int,
) -> None:
    class RecoveringServer(_BatchServer):
        heartbeat_calls = 0
        connect_calls = 0

        def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
            del kwargs
            type(self).heartbeat_calls += 1
            if type(self).heartbeat_calls == 1:
                raise ServerClientError(status_code, "stale lease")
            return _remote_connection()

        def connect_machine(self, **kwargs: Any) -> dict[str, Any]:
            del kwargs
            type(self).connect_calls += 1
            return _remote_connection()

        def list_users(self, *, handle: str | None = None) -> list[dict[str, Any]]:
            del handle
            return [{"id": "owner-a", "handle": "alice", "display_name": "Alice"}]

    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    service: HeartbeatService | None = None
    try:
        store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-secret",
            user_token="user-token-secret",
            connection=_remote_connection(),
            heartbeat_interval_seconds=60,
        )
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=RecoveringServer,
        )
        snapshot_hash, _ = runtime.build_machine_library_snapshot_manifest()
        current = store.current_server_connection()
        assert current is not None
        store.record_server_connection_library_snapshot(current["id"], snapshot_hash=snapshot_hash)
        service = HeartbeatService(
            store,
            runtime.sync_once,
            initial_backoff_seconds=0.001,
            max_backoff_seconds=0.002,
            watchdog_interval_seconds=0.01,
        )
        runtime.heartbeat_service = service
        service.restore_server_heartbeat()

        deadline = time.monotonic() + 2
        while RecoveringServer.connect_calls < 1 and time.monotonic() < deadline:
            time.sleep(0.005)

        assert RecoveringServer.connect_calls == 1
        assert store.current_server_connection_credentials()["status"] == "connected"
        assert runtime.server_whoami()["live"] is True
    finally:
        if service is not None:
            service.shutdown()
        if runtime is not None:
            runtime.heartbeat_service = _NoopHeartbeats()
            runtime.shutdown()
        store.close()


@pytest.mark.parametrize(
    "failure",
    [
        KeyError("connection vanished"),
        sqlite3.OperationalError("database temporarily unavailable"),
        OSError("secret store temporarily unavailable"),
    ],
    ids=["key-error", "sqlite-error", "secret-store-error"],
)
def test_heartbeat_credential_failures_are_recorded_and_loop_survives(
    tmp_path: Path,
    failure: Exception,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    original_get = store.get_server_connection_credentials
    stop = threading.Event()
    reads = 0

    def flaky_get(connection_id: str) -> dict[str, Any]:
        nonlocal reads
        reads += 1
        if reads == 1:
            raise failure
        stop.set()
        return original_get(connection_id)

    monkeypatch.setattr(store, "get_server_connection_credentials", flaky_get)
    service = HeartbeatService(store, lambda **_: {"connected": True}, initial_backoff_seconds=0.001)
    try:
        service._server_heartbeat_loop(connection["id"], "machine-token-secret", stop)  # noqa: SLF001

        updated = store.get_server_connection(connection["id"])
        assert reads == 2
        assert updated["status"] == "heartbeat_failed"
        assert updated["error"] is not None
        assert type(failure).__name__ in updated["error"]
    finally:
        service.shutdown()
        store.close()


def test_null_heartbeat_interval_uses_default_and_does_not_kill_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    original_get = store.get_server_connection_credentials
    stop = threading.Event()
    calls = 0

    def null_interval(connection_id: str) -> dict[str, Any]:
        credentials = original_get(connection_id)
        credentials["heartbeat_interval_seconds"] = None
        return credentials

    def sync_once(**kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        del kwargs
        calls += 1
        stop.set()
        return {"connected": True}

    monkeypatch.setattr(store, "get_server_connection_credentials", null_interval)
    service = HeartbeatService(store, sync_once)
    try:
        service._server_heartbeat_loop(connection["id"], "machine-token-secret", stop)  # noqa: SLF001
        assert calls == 1
        assert store.get_server_connection(connection["id"])["status"] == "connected"
    finally:
        service.shutdown()
        store.close()


def test_error_recorder_failure_is_logged_then_later_cause_is_persisted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    original_record = store.record_server_connection_error
    record_calls = 0
    sync_calls = 0
    stop = threading.Event()

    def flaky_record(connection_id: str, *, status: str, error: str) -> dict[str, Any]:
        nonlocal record_calls
        record_calls += 1
        if record_calls == 1:
            raise sqlite3.OperationalError("cannot write heartbeat cause")
        return original_record(connection_id, status=status, error=error)

    def failing_sync(**kwargs: Any) -> dict[str, Any]:
        nonlocal sync_calls
        del kwargs
        sync_calls += 1
        if sync_calls == 2:
            stop.set()
        raise ValueError(f"heartbeat failure {sync_calls}")

    monkeypatch.setattr(store, "record_server_connection_error", flaky_record)
    service = HeartbeatService(store, failing_sync, initial_backoff_seconds=0.001)
    try:
        with caplog.at_level(logging.ERROR):
            service._server_heartbeat_loop(connection["id"], "machine-token-secret", stop)  # noqa: SLF001

        updated = store.get_server_connection(connection["id"])
        assert sync_calls == 2
        assert "heartbeat failure 2" in updated["error"]
        assert "could not be persisted" in caplog.text
    finally:
        service.shutdown()
        store.close()


def test_active_non_connected_connection_status_always_has_a_recorded_error(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    try:
        connection = store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-secret",
            user_token="user-token-secret",
            connection=_remote_connection(),
            heartbeat_interval_seconds=60,
        )
        for status in ("heartbeat_failed", "connect_failed", "needs_reconnect"):
            store.record_server_connection_error(
                connection["id"],
                status=status,
                error=f"recorded cause for {status}",
            )
            current = store.current_server_connection()
            assert current is not None
            assert current["status"] != "connected"
            assert current["error"] is not None
    finally:
        store.close()


def test_legacy_stale_identity_recovery_fills_a_missing_error(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    with store._lock, store._conn:  # noqa: SLF001 - create a pre-hotfix legacy row.
        store._conn.execute(  # noqa: SLF001
            "UPDATE server_connections SET status = 'stale', error = NULL WHERE id = ?",
            (connection["id"],),
        )
    store.close()

    reopened = RegistryStore(tmp_path)
    try:
        recovered = reopened.current_server_connection()
        assert recovered is not None
        assert recovered["status"] == "needs_reconnect"
        assert recovered["error"] is not None
        assert "legacy stale lease" in recovered["error"]
    finally:
        reopened.close()


def test_watchdog_restarts_dead_heartbeat_without_creating_duplicates(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    first_tick = threading.Event()
    recovered_tick = threading.Event()
    calls = 0

    def sync_once(**kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        del kwargs
        calls += 1
        first_tick.set()
        if calls >= 2:
            recovered_tick.set()
        return {"connected": True}

    service = HeartbeatService(store, sync_once, watchdog_interval_seconds=0.01)
    try:
        service.restore_server_heartbeat()
        assert first_tick.wait(timeout=1)
        with service._lock:  # noqa: SLF001 - artificial thread death is the regression setup.
            old_thread = service._threads[connection["id"]]  # noqa: SLF001
            service._stops[connection["id"]].set()  # noqa: SLF001
        old_thread.join(timeout=1)
        assert not old_thread.is_alive()

        assert recovered_tick.wait(timeout=1)
        service.ensure_server_heartbeat()
        service.ensure_server_heartbeat()
        status = service.status(connection["id"])
        assert status["thread_alive"] is True
        assert status["last_tick_at"] is not None
        live_threads = [
            thread
            for thread in threading.enumerate()
            if thread.name == f"spl-server-heartbeat-{connection['id']}" and thread.is_alive()
        ]
        assert len(live_threads) == 1
    finally:
        service.shutdown()
        store.close()


def test_watchdog_does_not_restart_a_live_thread_during_capped_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(heartbeat_service_module, "HEARTBEAT_MIN_STALE_SECONDS", 0.05)
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=0.01,
    )
    failed_once = threading.Event()
    calls = 0

    def sync_once(**kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        del kwargs
        calls += 1
        failed_once.set()
        raise ServerClientError(503, "server remains unavailable")

    service = HeartbeatService(
        store,
        sync_once,
        initial_backoff_seconds=0.2,
        max_backoff_seconds=0.2,
        watchdog_interval_seconds=0.005,
    )
    try:
        service.restore_server_heartbeat()
        assert failed_once.wait(timeout=1)
        with service._lock:  # noqa: SLF001 - pin the supervised thread identity.
            original_thread = service._threads[connection["id"]]  # noqa: SLF001

        time.sleep(0.12)

        with service._lock:  # noqa: SLF001 - assert the watchdog did not reset backoff.
            current_thread = service._threads[connection["id"]]  # noqa: SLF001
        assert current_thread is original_thread
        assert current_thread.is_alive()
        assert calls == 1
    finally:
        service.shutdown()
        store.close()


def test_queue_status_and_prune_protect_non_telemetry_events(tmp_path: Path) -> None:
    store, runtime, _ = _runtime(tmp_path)
    try:
        protected = store.enqueue_sync_event("object_version", {"owner_id": "owner-a", "name": "important"})
        telemetry = store.enqueue_sync_event("local_run_update", {"owner_id": "owner-a", "run": {"id": "r1"}})

        status = runtime.sync_status()
        assert status["by_status"]["pending"] == 2
        assert status["oldest_event"] is not None
        assert status["heartbeat"]["thread_alive"] is False

        result = runtime.prune_sync_events(status="pending", older_than_days=0)
        assert [row["id"] for row in result["pruned"]] == [telemetry["id"]]
        assert [row["id"] for row in result["protected"]] == [protected["id"]]
        assert store.get_sync_event(protected["id"])["status"] == "pending"

        explicit = runtime.prune_sync_events(
            status="pending",
            older_than_days=0,
            include_protected=True,
        )
        assert [row["id"] for row in explicit["pruned"]] == [protected["id"]]
    finally:
        runtime.shutdown()
        store.close()


def test_user_visible_connection_state_never_calls_dead_channel_connected(tmp_path: Path) -> None:
    store, runtime, connection = _runtime(tmp_path)
    try:
        runtime._mark_server_channel_failure(store.get_server_connection_credentials(connection["id"]))  # noqa: SLF001

        state = runtime.server_connection_state()
        assert state["identity_present"] is True
        assert state["live"] is False
        assert state["connected"] is False
        assert state["offline"] is True
        assert state["code"] == "central_server_unreachable"
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.parametrize("interval", [None, "garbage", 0, -1])
def test_server_liveness_window_uses_default_for_invalid_interval(interval: Any) -> None:
    assert (
        DaemonRuntime._server_channel_window_seconds(  # noqa: SLF001
            {"heartbeat_interval_seconds": interval}
        )
        == 120.0
    )


def test_long_drain_renews_lease_between_batches(tmp_path: Path) -> None:
    class TtlServer(_BatchServer):
        logical_time = 0.0
        lease_expires_at = -1.0
        heartbeat_calls = 0

        def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
            del kwargs
            type(self).heartbeat_calls += 1
            type(self).lease_expires_at = type(self).logical_time + 1.0
            return _remote_connection()

        def sync(self, **kwargs: Any) -> dict[str, Any]:
            if type(self).logical_time >= type(self).lease_expires_at:
                raise ServerClientError(409, "connection is not active: stale")
            response = super().sync(**kwargs)
            type(self).logical_time += 0.6
            return response

    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    try:
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
            server_client_factory=TtlServer,
        )
        runtime._mark_server_channel_success(store.get_server_connection_credentials(connection["id"]))  # noqa: SLF001
        snapshot_hash, _ = runtime.build_machine_library_snapshot_manifest()
        store.record_server_connection_library_snapshot(connection["id"], snapshot_hash=snapshot_hash)
        for index in range(121):
            store.enqueue_sync_event("local_run_update", {"owner_id": "owner-a", "run": {"id": str(index)}})

        response = runtime.sync_once()

        assert response["partial"] is False
        assert response["batches"] == 3
        assert TtlServer.heartbeat_calls >= response["batches"]
        assert store.list_pending_sync_events() == []
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_later_batch_failure_keeps_acknowledged_prefix_and_returns_partial_success(tmp_path: Path) -> None:
    class FailSecondBatch(_BatchServer):
        sync_calls = 0

        def sync(self, **kwargs: Any) -> dict[str, Any]:
            type(self).sync_calls += 1
            if type(self).sync_calls == 2:
                raise ServerClientError(502, "temporary batch failure")
            return super().sync(**kwargs)

    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    try:
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
            server_client_factory=FailSecondBatch,
        )
        runtime._mark_server_channel_success(store.get_server_connection_credentials(connection["id"]))  # noqa: SLF001
        snapshot_hash, _ = runtime.build_machine_library_snapshot_manifest()
        store.record_server_connection_library_snapshot(connection["id"], snapshot_hash=snapshot_hash)
        event_ids = [
            store.enqueue_sync_event(
                "local_run_update",
                {"owner_id": "owner-a", "run": {"id": str(index)}},
            )["id"]
            for index in range(75)
        ]

        response = runtime.sync_once()

        assert response["partial"] is True
        assert response["error"] == "temporary batch failure"
        assert response["partial_error"] == {
            "message": "temporary batch failure",
            "status_code": 502,
        }
        assert store.get_server_connection(connection["id"])["status"] == "heartbeat_failed"
        assert store.get_server_connection(connection["id"])["error"] == "temporary batch failure"
        assert all(store.get_sync_event(event_id)["status"] == "sent" for event_id in event_ids[:50])
        assert all(store.get_sync_event(event_id)["status"] == "pending" for event_id in event_ids[50:])
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()
