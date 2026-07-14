from __future__ import annotations

import hashlib
import threading
from pathlib import Path
from typing import Any, Iterator

import pytest

from spl.daemon.heartbeat_service import HeartbeatService
from spl.daemon.repositories.server_connection import SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
from spl.daemon.remote_client import ServerClientError
from spl.daemon.server import DaemonRuntime
from spl.daemon.server_connection import (
    SERVER_OFFLINE_MESSAGE,
    ServerConnectionManager,
    ServerOfflineError,
)
from spl.daemon.store import RegistryStore


class OfflineClient:
    def connect_machine(self, **kwargs: Any) -> dict[str, Any]:
        raise ServerClientError(503, "server offline")


class ConnectedClient:
    def __init__(self):
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.connect_kwargs: list[dict[str, Any]] = []

    def connect_machine(self, **kwargs: Any) -> dict[str, Any]:
        self.connect_calls += 1
        self.connect_kwargs.append(kwargs)
        machine_id = kwargs.get("machine_id") or "machine-1"
        connection = _remote_connection(machine_id=machine_id)
        if kwargs.get("display_name"):
            connection["display_name"] = kwargs["display_name"]
        return connection

    def disconnect_machine(self) -> dict[str, Any]:
        self.disconnect_calls += 1
        return {
            **_remote_connection(machine_id="machine-1"),
            "status": "disconnected",
        }


class ClientFactory:
    def __init__(self, client: Any):
        self.client = client
        self.calls: list[tuple[str, str, str | None]] = []

    def __call__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
    ) -> Any:
        self.calls.append((base_url, machine_token, user_token))
        return self.client


class FakeServerConnections:
    def __init__(self):
        self.connected = False
        self.disconnected_with: dict[str, Any] | None = None

    def server_client(
        self,
        server_url: str,
        token: str,
        *,
        user_token: str | None,
    ) -> Any:
        raise NotImplementedError

    def server_client_for_credentials(self, credentials: dict[str, Any]) -> Any:
        raise NotImplementedError

    def connect_server(self, **kwargs: Any) -> dict[str, Any]:
        self.connected = True
        return {
            "connected": True,
            "connection": {"id": "local-connection-1"},
            "remote_connection": {"id": "remote-connection-1"},
        }

    def disconnect_server(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.disconnected_with = credentials
        return {
            "connected": False,
            "connection": {"id": credentials["id"] if credentials else "missing"},
            "remote_connection": {"id": "remote-connection-1"},
        }

    def matching_server_connection(self, **kwargs: Any) -> dict[str, Any] | None:
        return None

    def restore_pending_server_connection(
        self,
        credentials: dict[str, Any],
    ) -> dict[str, Any]:
        return credentials

    def require_connected_server_credentials(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if credentials is None:
            raise KeyError("active server connection is not found")
        return credentials

    def remote_connection_snapshot(
        self,
        connection: dict[str, Any],
    ) -> dict[str, Any]:
        return {"id": connection["remote_connection_id"]}


class FakeHeartbeatService:
    def __init__(self):
        self.restored = False
        self.started: list[tuple[str, str]] = []
        self.stopped: list[str] = []
        self.shutdown_called = False

    def restore_server_heartbeat(self) -> None:
        self.restored = True

    def start_server_heartbeat(
        self,
        connection: dict[str, Any],
        *,
        token: str,
    ) -> None:
        self.started.append((connection["id"], token))

    def ensure_server_heartbeat(self, connection: dict[str, Any] | None = None) -> None:
        pass

    def status(self, connection_id: str | None = None) -> dict[str, Any]:
        return {"connection_id": connection_id, "thread_alive": False, "last_tick_at": None}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        self.stopped.append(connection_id)

    def shutdown(self) -> None:
        self.shutdown_called = True


@pytest.fixture
def store(tmp_path: Path) -> Iterator[RegistryStore]:
    registry = RegistryStore(tmp_path)
    try:
        yield registry
    finally:
        registry.close()


def _remote_connection(*, machine_id: str) -> dict[str, Any]:
    return {
        "id": "remote-connection-1",
        "owner_id": "admin1",
        "subject_type": "machine",
        "subject_id": machine_id,
        "machine_id": machine_id,
        "display_name": machine_id,
        "capabilities": {"python": "3.13"},
        "status": "connected",
        "last_seen_at": "2026-01-01T00:00:00+00:00",
        "expires_at": "2026-01-01T00:01:00+00:00",
    }


def test_server_connection_manager_persists_pending_connection_when_offline(
    store: RegistryStore,
) -> None:
    machine_token = "machine-token-secret"
    manager = ServerConnectionManager(store, ClientFactory(OfflineClient()))

    result = manager.connect_server(
        server_url="https://splime.io/api",
        machine_token=machine_token,
        user_token="user-token-secret",
        machine_id=None,
        display_name=None,
        capabilities={},
        heartbeat_interval_seconds=60,
    )

    connection = result["connection"]
    expected_machine_id = f"machine-{hashlib.sha256(machine_token.encode('utf-8')).hexdigest()[:12]}"
    assert result == {
        "connected": False,
        "offline": True,
        "connection": connection,
        "remote_connection": None,
        "error": SERVER_OFFLINE_MESSAGE,
        "detail": "server offline",
    }
    assert connection["status"] == "enroll_failed"
    assert connection["remote_connection_id"] is None
    assert connection["machine_id"] == expected_machine_id
    assert store.current_server_connection_credentials() is None
    pending_credentials = store.get_server_connection_credentials(connection["id"])
    with pytest.raises(ServerOfflineError, match="central SPL daemon server is offline"):
        manager.require_connected_server_credentials(pending_credentials)


def test_server_connection_manager_reuses_ownerless_pending_attempt(
    store: RegistryStore,
) -> None:
    manager = ServerConnectionManager(store, ClientFactory(OfflineClient()))

    first = manager.connect_server(
        server_url="https://splime.io/api",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-1",
        display_name=None,
        capabilities={},
        heartbeat_interval_seconds=60,
    )
    second = manager.connect_server(
        server_url="https://splime.io/api",
        machine_token="next-machine-token-secret",
        user_token="next-user-token-secret",
        machine_id="machine-1",
        display_name=None,
        capabilities={},
        heartbeat_interval_seconds=60,
    )

    assert second["connection"]["id"] == first["connection"]["id"]
    assert second["connection"]["status"] == "enroll_failed"
    assert len(store.list_server_connections()) == 1
    credentials = store.get_server_connection_credentials(first["connection"]["id"])
    assert credentials["token"] == "next-machine-token-secret"
    assert credentials["user_token"] == "next-user-token-secret"


def test_server_connection_manager_completes_reused_pending_attempt(
    store: RegistryStore,
) -> None:
    pending = ServerConnectionManager(store, ClientFactory(OfflineClient())).connect_server(
        server_url="https://splime.io/api",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-1",
        display_name=None,
        capabilities={},
        heartbeat_interval_seconds=60,
    )["connection"]
    client = ConnectedClient()
    manager = ServerConnectionManager(store, ClientFactory(client))

    result = manager.connect_server(
        server_url="https://splime.io/api",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-1",
        display_name=None,
        capabilities={},
        heartbeat_interval_seconds=60,
    )

    assert result["connected"] is True
    assert result["connection"]["id"] == pending["id"]
    assert result["connection"]["owner_id"] == "admin1"
    assert result["connection"]["status"] == "connected"
    assert len(store.list_server_connections()) == 1
    assert client.connect_calls == 1


def test_server_connection_manager_reuses_matching_connection(
    store: RegistryStore,
) -> None:
    client = ConnectedClient()
    manager = ServerConnectionManager(store, ClientFactory(client))

    first = manager.connect_server(
        server_url="https://splime.io/api/",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-1",
        display_name=None,
        capabilities={},
        heartbeat_interval_seconds=60,
    )
    second = manager.connect_server(
        server_url="https://splime.io/api",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-1",
        display_name=None,
        capabilities={},
        heartbeat_interval_seconds=60,
    )

    assert first["connected"] is True
    assert second["reused"] is True
    assert second["remote_connection"]["id"] == "remote-connection-1"
    assert second["remote_connection"]["machine_id"] == "machine-1"
    assert client.connect_calls == 1


def test_save_server_connection_requires_confirmed_remote_identity_before_replace(
    store: RegistryStore,
) -> None:
    first = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(machine_id="machine-1"),
        heartbeat_interval_seconds=60,
    )
    incomplete_remote_connection = dict(_remote_connection(machine_id="machine-2"))
    incomplete_remote_connection.pop("id")

    with pytest.raises(ValueError, match="remote_connection_id"):
        store.save_server_connection(
            server_url="https://splime.io/api",
            token="next-machine-token-secret",
            user_token="next-user-token-secret",
            connection=incomplete_remote_connection,
            heartbeat_interval_seconds=60,
        )

    current = store.current_server_connection()
    credentials = store.get_server_connection_credentials(first["id"])
    assert current is not None
    assert current["id"] == first["id"]
    assert current["status"] == "connected"
    assert credentials["token"] == "machine-token-secret"
    assert credentials["user_token"] == "user-token-secret"
    assert len(store.list_server_connections()) == 1


def test_server_connection_summary_reports_replaced_identity_without_secrets(
    store: RegistryStore,
) -> None:
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(machine_id="machine-1"),
        heartbeat_interval_seconds=60,
    )

    with store._lock, store._conn:  # noqa: SLF001 - seed a post-K-01 victim row.
        store._conn.execute(
            """
            UPDATE server_connections
            SET status = 'replaced',
                token_secret_ref = NULL,
                user_token_secret_ref = NULL
            WHERE id = ?
            """,
            (connection["id"],),
        )

    summary = store.server_connection_summary()
    assert summary["identity_degraded"] is True
    assert summary["offline_replaced_identity_rows"] == 1


def test_server_connection_manager_refreshes_reused_technical_display_name(
    store: RegistryStore,
) -> None:
    client = ConnectedClient()
    manager = ServerConnectionManager(store, ClientFactory(client))

    first = manager.connect_server(
        server_url="https://splime.io/api/",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-86c8b6063d0bef7b",
        display_name=None,
        capabilities={},
        heartbeat_interval_seconds=60,
    )
    second = manager.connect_server(
        server_url="https://splime.io/api",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-86c8b6063d0bef7b",
        display_name="Pair3",
        capabilities={},
        heartbeat_interval_seconds=60,
    )
    third = manager.connect_server(
        server_url="https://splime.io/api",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-86c8b6063d0bef7b",
        display_name="Pair3",
        capabilities={},
        heartbeat_interval_seconds=60,
    )

    assert first["connection"]["display_name"] == "machine-86c8b6063d0bef7b"
    assert second["reused"] is True
    assert second["refreshed"] is True
    assert second["connection"]["display_name"] == "Pair3"
    assert second["remote_connection"]["display_name"] == "Pair3"
    assert third["reused"] is True
    assert "refreshed" not in third
    assert client.connect_calls == 2
    assert client.connect_kwargs[1]["display_name"] == "Pair3"


def test_server_connection_manager_disconnects_pending_connection(
    store: RegistryStore,
) -> None:
    client = ConnectedClient()
    manager = ServerConnectionManager(store, ClientFactory(client))
    pending = store.save_pending_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-1",
    )

    result = manager.disconnect_server(store.get_server_connection_credentials(pending["id"]))

    assert result["connected"] is False
    assert result["offline"] is True
    assert result["connection"]["id"] == pending["id"]
    assert result["connection"]["status"] == "disconnected"
    assert result["remote_connection"] is None
    assert client.disconnect_calls == 0


@pytest.mark.parametrize("status_code", [401, 404])
def test_heartbeat_service_records_lease_rejection_keeps_identity_and_secrets(
    store: RegistryStore,
    status_code: int,
) -> None:
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(machine_id="machine-1"),
        heartbeat_interval_seconds=60,
    )
    stop_event = threading.Event()

    def sync_once(**kwargs: Any) -> dict[str, Any]:
        stop_event.set()
        raise ServerClientError(status_code, "stale lease")

    service = HeartbeatService(store, sync_once)

    service._server_heartbeat_loop(
        connection["id"],
        "machine-token-secret",
        stop_event,
    )

    updated = store.get_server_connection(connection["id"])
    assert updated["status"] == SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
    assert updated["error"] == f"lease rejected by server ({status_code}): stale lease"

    credentials = store.current_server_connection_credentials()
    assert credentials["id"] == connection["id"]
    assert credentials["owner_id"] == "admin1"
    assert credentials["token"] == "machine-token-secret"
    assert credentials["user_token"] == "user-token-secret"


def test_heartbeat_restore_retries_needs_reconnect_without_explicit_reconnect(
    store: RegistryStore,
) -> None:
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(machine_id="machine-1"),
        heartbeat_interval_seconds=60,
    )
    store.record_server_connection_error(
        connection["id"],
        status=SERVER_CONNECTION_STATUS_NEEDS_RECONNECT,
        error="lease rejected by server (404): stale lease",
    )
    called = threading.Event()

    def sync_once(**kwargs: Any) -> dict[str, Any]:
        called.set()
        return {"connected": True}

    service = HeartbeatService(store, sync_once)
    service.restore_server_heartbeat()

    assert called.wait(timeout=1)
    assert store.current_server_connection_credentials()["id"] == connection["id"]
    service.shutdown()


def test_record_server_connection_error_normalizes_legacy_stale_status(
    store: RegistryStore,
) -> None:
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(machine_id="machine-1"),
        heartbeat_interval_seconds=60,
    )

    store.record_server_connection_error(
        connection["id"],
        status="stale",
        error="legacy stale lease",
    )

    assert store.current_server_connection_credentials()["status"] == SERVER_CONNECTION_STATUS_NEEDS_RECONNECT


def test_connect_server_reconnects_reused_needs_reconnect_identity(
    store: RegistryStore,
) -> None:
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(machine_id="machine-1"),
        heartbeat_interval_seconds=60,
    )
    store.record_server_connection_error(
        connection["id"],
        status=SERVER_CONNECTION_STATUS_NEEDS_RECONNECT,
        error="lease rejected by server (404): stale lease",
    )
    client = ConnectedClient()
    manager = ServerConnectionManager(store, ClientFactory(client))

    result = manager.connect_server(
        server_url="https://splime.io/api",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-1",
        display_name=None,
        capabilities={"python": "3.13"},
        heartbeat_interval_seconds=60,
    )

    assert result["connected"] is True
    assert result["reused"] is True
    assert result["refreshed"] is True
    assert client.connect_calls == 1
    assert store.current_server_connection_credentials()["status"] == "connected"


def test_daemon_runtime_reconnect_restores_live_sync_channel(
    store: RegistryStore,
) -> None:
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(machine_id="machine-1"),
        heartbeat_interval_seconds=60,
    )
    store.record_server_connection_error(
        connection["id"],
        status=SERVER_CONNECTION_STATUS_NEEDS_RECONNECT,
        error="lease rejected by server (404): stale lease",
    )
    client = ConnectedClient()
    runtime = DaemonRuntime(
        store,
        heartbeat_service=FakeHeartbeatService(),
        server_client_factory=ClientFactory(client),
    )

    result = runtime.connect_server(
        server_url="https://splime.io/api",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-1",
        display_name=None,
        capabilities={"python": "3.13"},
        heartbeat_interval_seconds=60,
    )

    credentials = store.current_server_connection_credentials()
    assert result["connected"] is True
    assert result["reconcile"]["owner_id"] == "admin1"
    assert runtime._server_channel_is_live(credentials) is True  # noqa: SLF001
    runtime.shutdown()


def test_heartbeat_service_records_transient_failure(
    store: RegistryStore,
) -> None:
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(machine_id="machine-1"),
        heartbeat_interval_seconds=60,
    )
    stop_event = threading.Event()

    def sync_once(**kwargs: Any) -> dict[str, Any]:
        stop_event.set()
        raise RuntimeError("temporary failure")

    service = HeartbeatService(store, sync_once)

    service._server_heartbeat_loop(
        connection["id"],
        "machine-token-secret",
        stop_event,
    )

    updated = store.get_server_connection(connection["id"])
    assert updated["status"] == "heartbeat_failed"
    assert updated["error"] == "RuntimeError('temporary failure')"


def test_daemon_runtime_delegates_connection_and_heartbeat(
    store: RegistryStore,
) -> None:
    server_connections = FakeServerConnections()
    heartbeats = FakeHeartbeatService()
    existing = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(machine_id="machine-1"),
        heartbeat_interval_seconds=60,
    )
    runtime = DaemonRuntime(
        store,
        server_connections=server_connections,
        heartbeat_service=heartbeats,
    )

    connect_result = runtime.connect_server(
        server_url="https://splime.io/api",
        machine_token="machine-token-secret",
        user_token="user-token-secret",
        machine_id="machine-1",
        display_name=None,
        capabilities={},
        heartbeat_interval_seconds=60,
    )
    disconnect_result = runtime.disconnect_server()
    runtime.shutdown()

    assert heartbeats.restored is True
    assert connect_result["connected"] is True
    assert heartbeats.started == [("local-connection-1", "machine-token-secret")]
    assert heartbeats.stopped == [existing["id"]]
    assert server_connections.disconnected_with is not None
    assert server_connections.disconnected_with["id"] == existing["id"]
    assert disconnect_result["connected"] is False
    assert heartbeats.shutdown_called is True
    assert not hasattr(runtime, "_server_heartbeat_threads")
