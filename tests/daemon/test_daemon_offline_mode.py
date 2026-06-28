from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

import spl.daemon.server as daemon_server
from spl.daemon.remote_client import ServerClientError
from spl.daemon.server import DaemonRuntime, ServerOfflineError
from spl.daemon.store import REDACTED_SECRET_VALUE, RegistryStore, utc_now


class OfflineServerClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def connect_machine(self, **kwargs):
        raise ServerClientError(502, "server offline")


class ConnectedServerClient:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def connect_machine(self, **kwargs):
        machine_id = kwargs.get("machine_id") or "machine-x"
        return {
            "id": "remote-connection-1",
            "owner_id": "admin1",
            "subject_type": "machine",
            "subject_id": machine_id,
            "machine_id": machine_id,
            "display_name": kwargs.get("display_name") or machine_id,
            "capabilities": kwargs.get("capabilities") or {},
            "status": "connected",
        }


def test_connect_server_persists_pending_connection_when_server_is_offline(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", OfflineServerClient)
    try:
        runtime = DaemonRuntime(store)
        machine_token = "spl-demo-machine-x-token"
        result = runtime.connect_server(
            server_url="https://splime.io/api",
            machine_token=machine_token,
            user_token="spl-demo-user-token",
            machine_id=None,
            display_name=None,
            capabilities={},
            heartbeat_interval_seconds=60,
        )

        connection = result["connection"]

        assert result["connected"] is False
        assert result["offline"] is True
        assert connection["status"] == "connect_failed"
        assert connection["remote_connection_id"] is None
        expected_machine_id = (
            f"machine-{hashlib.sha256(machine_token.encode('utf-8')).hexdigest()[:12]}"
        )
        assert connection["machine_id"] == expected_machine_id
        assert connection["machine_id"] != "machine-x"
        assert store.current_server_connection_credentials() is not None

        sync = runtime.sync_once(connection_id=connection["id"])

        assert sync["connected"] is False
        assert sync["offline"] is True
        try:
            runtime._require_connected_server_credentials()
        except ServerOfflineError:
            pass
        else:
            raise AssertionError("server-backed operations must fail while offline")
        runtime.disconnect_server()
    finally:
        store.close()


def test_connect_server_persists_on_legacy_schema_with_secret_refs(
    tmp_path,
    monkeypatch,
) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "daemon.sqlite3"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            """
            CREATE TABLE server_connections (
                id TEXT PRIMARY KEY,
                server_url TEXT NOT NULL,
                token TEXT NOT NULL,
                token_hint TEXT NOT NULL,
                user_token TEXT,
                user_token_hint TEXT,
                machine_id TEXT NOT NULL,
                display_name TEXT,
                capabilities_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", ConnectedServerClient)
    monkeypatch.setattr(
        DaemonRuntime,
        "start_server_heartbeat",
        lambda self, connection, token: None,
    )
    try:
        runtime = DaemonRuntime(store)
        result = runtime.connect_server(
            server_url="https://splime.io/api",
            machine_token="machine-token-secret",
            user_token="user-token-secret",
            machine_id="machine-x",
            display_name=None,
            capabilities={},
            heartbeat_interval_seconds=60,
        )

        connection_record = result["connection"]
        row = store._conn.execute(  # noqa: SLF001 - regression inspects storage.
            """
            SELECT token, user_token, token_secret_ref, user_token_secret_ref
            FROM server_connections
            WHERE id = ?
            """,
            (connection_record["id"],),
        ).fetchone()

        assert result["connected"] is True
        assert connection_record["remote_connection_id"] == "remote-connection-1"
        assert row["token"] == REDACTED_SECRET_VALUE
        assert row["user_token"] == REDACTED_SECRET_VALUE
        assert row["token_secret_ref"].startswith("file:")
        assert row["user_token_secret_ref"].startswith("file:")
    finally:
        store.close()


def test_server_connection_tokens_are_stored_outside_sqlite(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        first = store.save_pending_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-secret",
            user_token="user-token-secret",
            machine_id="machine-1",
        )

        row = store._conn.execute(  # noqa: SLF001 - regression inspects storage.
            """
            SELECT token_redacted, user_token_redacted,
                   token_secret_ref, user_token_secret_ref
            FROM server_connections
            """
        ).fetchone()

        assert row["token_redacted"] == REDACTED_SECRET_VALUE
        assert row["user_token_redacted"] == REDACTED_SECRET_VALUE
        assert row["token_secret_ref"].startswith("file:")
        assert row["user_token_secret_ref"].startswith("file:")

        credentials = store.current_server_connection_credentials()
        assert credentials["token"] == "machine-token-secret"
        assert credentials["user_token"] == "user-token-secret"

        store.save_pending_server_connection(
            server_url="https://splime.io/api",
            token="next-machine-token-secret",
            user_token="next-user-token-secret",
            machine_id="machine-2",
        )
        secret_values = json.loads(
            (tmp_path / "daemon-secrets.json").read_text(encoding="utf-8")
        ).values()
        assert "machine-token-secret" not in secret_values
        assert "user-token-secret" not in secret_values
        assert "next-machine-token-secret" in secret_values
        assert "next-user-token-secret" in secret_values
        old_row = store._conn.execute(  # noqa: SLF001 - regression inspects storage.
            """
            SELECT status, token_secret_ref, user_token_secret_ref
            FROM server_connections
            WHERE id = ?
            """,
            (first["id"],),
        ).fetchone()
        assert old_row["status"] == "replaced"
        assert old_row["token_secret_ref"] is None
        assert old_row["user_token_secret_ref"] is None

        current = store.current_server_connection_credentials()
        store.mark_server_connection_disconnected(current["id"])
        secret_values = json.loads(
            (tmp_path / "daemon-secrets.json").read_text(encoding="utf-8")
        ).values()
        assert "next-machine-token-secret" not in secret_values
        assert "next-user-token-secret" not in secret_values
    finally:
        store.close()


def test_legacy_server_connection_tokens_are_migrated_from_sqlite(tmp_path) -> None:
    now = utc_now()
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "daemon.sqlite3"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            """
            CREATE TABLE server_connections (
                id TEXT PRIMARY KEY,
                server_url TEXT NOT NULL,
                token TEXT NOT NULL,
                token_hint TEXT NOT NULL,
                user_token TEXT,
                user_token_hint TEXT,
                machine_id TEXT NOT NULL,
                display_name TEXT,
                capabilities_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO server_connections(
                id, server_url, token, token_hint, user_token, user_token_hint,
                machine_id, display_name, capabilities_json, status,
                created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy1",
                "https://splime.io/api",
                "legacy-machine-secret",
                "...secret",
                "legacy-user-secret",
                "...secret",
                "machine-1",
                "machine-1",
                "{}",
                "connected",
                now,
                now,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    store = RegistryStore(tmp_path)
    try:
        row = store._conn.execute(  # noqa: SLF001 - regression inspects storage.
            """
            SELECT token, user_token, token_redacted, user_token_redacted,
                   token_secret_ref, user_token_secret_ref
            FROM server_connections
            WHERE id = 'legacy1'
            """
        ).fetchone()

        assert row["token"] == REDACTED_SECRET_VALUE
        assert row["user_token"] == REDACTED_SECRET_VALUE
        assert row["token_redacted"] == REDACTED_SECRET_VALUE
        assert row["user_token_redacted"] == REDACTED_SECRET_VALUE
        assert row["token_secret_ref"].startswith("file:")
        assert row["user_token_secret_ref"].startswith("file:")

        credentials = store.current_server_connection_credentials()
        assert credentials["token"] == "legacy-machine-secret"
        assert credentials["user_token"] == "legacy-user-secret"
    finally:
        store.close()


def test_legacy_server_connection_schema_accepts_new_secret_backed_inserts(
    tmp_path,
) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "daemon.sqlite3"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            """
            CREATE TABLE server_connections (
                id TEXT PRIMARY KEY,
                server_url TEXT NOT NULL,
                token TEXT NOT NULL,
                token_hint TEXT NOT NULL,
                user_token TEXT,
                user_token_hint TEXT,
                machine_id TEXT NOT NULL,
                display_name TEXT,
                capabilities_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()

    store = RegistryStore(tmp_path)
    try:
        connected = store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-secret",
            user_token="user-token-secret",
            connection={
                "id": "remote-connection-1",
                "owner_id": "admin1",
                "subject_type": "machine",
                "subject_id": "machine-x",
                "machine_id": "machine-x",
                "display_name": "machine-x",
                "capabilities": {"python": "3.13"},
                "status": "connected",
            },
        )

        row = store._conn.execute(  # noqa: SLF001 - regression inspects storage.
            """
            SELECT token, user_token, token_secret_ref, user_token_secret_ref
            FROM server_connections
            WHERE id = ?
            """,
            (connected["id"],),
        ).fetchone()

        assert row["token"] == REDACTED_SECRET_VALUE
        assert row["user_token"] == REDACTED_SECRET_VALUE
        assert row["token_secret_ref"].startswith("file:")
        assert row["user_token_secret_ref"].startswith("file:")
        credentials = store.current_server_connection_credentials()
        assert credentials["token"] == "machine-token-secret"
        assert credentials["user_token"] == "user-token-secret"

        pending = store.save_pending_server_connection(
            server_url="https://splime.io/api",
            token="next-machine-token-secret",
            user_token="next-user-token-secret",
            machine_id="machine-y",
        )
        pending_row = store._conn.execute(  # noqa: SLF001
            """
            SELECT token, user_token, token_secret_ref, user_token_secret_ref
            FROM server_connections
            WHERE id = ?
            """,
            (pending["id"],),
        ).fetchone()

        assert pending_row["token"] == REDACTED_SECRET_VALUE
        assert pending_row["user_token"] == REDACTED_SECRET_VALUE
        assert pending_row["token_secret_ref"].startswith("file:")
        assert pending_row["user_token_secret_ref"].startswith("file:")
    finally:
        store.close()


def test_remote_node_without_server_connection_explains_requirement(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store)

        with pytest.raises(KeyError, match="remote Function/Pipeline"):
            runtime.run_remote_node({"name": "remote_func"}, kwargs={})
    finally:
        store.close()


def test_remote_node_without_target_machine_explains_missing_selection(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store)
        monkeypatch.setattr(
            runtime,
            "resolve_remote_signature",
            lambda ref, force=False: {
                "id": "remote-object-1",
                "kind": "function",
                "outputs": [],
                "remote_ref": {},
            },
        )

        with pytest.raises(RuntimeError, match="no target machine was selected"):
            runtime.run_remote_node(
                {"url": "https://splime.io/api", "name": "remote_func"},
                kwargs={},
            )
    finally:
        store.close()


def test_remote_node_failure_message_includes_context(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store)
        monkeypatch.setattr(
            runtime,
            "resolve_remote_signature",
            lambda ref, force=False: {
                "id": "remote-object-1",
                "kind": "function",
                "outputs": [],
                "target_machine": "machine-1",
                "remote_ref": {},
            },
        )
        monkeypatch.setattr(
            runtime,
            "start_remote_run",
            lambda *args, **kwargs: {"id": "run-1"},
        )
        monkeypatch.setattr(
            runtime,
            "_wait_server_run",
            lambda *args, **kwargs: {"status": "failed", "error": "boom"},
        )

        with pytest.raises(RuntimeError) as exc_info:
            runtime.run_remote_node(
                {"url": "https://splime.io/api", "name": "remote_func"},
                kwargs={},
            )

        message = str(exc_info.value)
        assert "remote function node 'remote_func' failed" in message
        assert "machine 'machine-1'" in message
        assert "run run-1 ended as 'failed'" in message
        assert "boom" in message
    finally:
        store.close()


def test_connect_server_does_not_derive_machine_id_from_display_name(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", OfflineServerClient)
    try:
        runtime = DaemonRuntime(store)
        machine_id = runtime._offline_machine_id(
            machine_token="spl-real-machine-token",
            machine_id=None,
            display_name="lab-box",
        )

        assert machine_id.startswith("machine-")
        assert machine_id != "lab-box"
    finally:
        store.close()
