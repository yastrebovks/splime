"""ServerConnectionRepository aggregate storage."""

from __future__ import annotations

import sqlite3
from typing import Any
from uuid import uuid4

from spl.daemon.storage_base import (
    REDACTED_SECRET_VALUE,
    RepositoryBase,
    iso_after_now,
    json_dumps,
    json_loads,
    normalize_heartbeat_interval,
    utc_now,
    validate_name,
)


class ServerConnectionRepository(RepositoryBase):
    """Persist and query serverconnection aggregate records."""

    def _secret_key(self, connection_id: str, name: str) -> str:
        return f"server-connections/{connection_id}/{name}"

    def _store_server_connection_secrets(
        self,
        connection_id: str,
        *,
        token: str,
        user_token: str | None,
    ) -> tuple[str, str | None]:
        token_ref = self.secret_store.put(
            self._secret_key(connection_id, "machine-token"),
            token,
        )
        user_token_ref = None
        if user_token:
            try:
                user_token_ref = self.secret_store.put(
                    self._secret_key(connection_id, "user-token"),
                    user_token,
                )
            except Exception:
                self.secret_store.delete(token_ref)
                raise
        return token_ref, user_token_ref

    def _delete_server_connection_secrets(
        self,
        token_ref: str | None,
        user_token_ref: str | None,
    ) -> None:
        self.secret_store.delete(token_ref)
        self.secret_store.delete(user_token_ref)

    def _delete_server_connection_secret_rows(
        self,
        rows: list[sqlite3.Row],
    ) -> None:
        for row in rows:
            self._delete_server_connection_secrets(
                row["token_secret_ref"],
                row["user_token_secret_ref"],
            )

    def _replace_active_server_connections_locked(
        self,
        now: str,
    ) -> list[sqlite3.Row]:
        rows = self._conn.execute(
            """
            SELECT token_secret_ref, user_token_secret_ref
            FROM server_connections
            WHERE status IN ('connected', 'heartbeat_failed', 'connect_failed')
            """
        ).fetchall()
        self._conn.execute(
            """
            UPDATE server_connections
            SET status = 'replaced',
                disconnected_at = :now,
                updated_at = :now,
                token_secret_ref = NULL,
                user_token_secret_ref = NULL
            WHERE status IN ('connected', 'heartbeat_failed', 'connect_failed')
            """,
            {"now": now},
        )
        return rows

    def _migrate_server_connection_secrets_locked(self) -> None:
        columns = self._table_columns_locked("server_connections")
        select_columns = [
            "id",
            "token_secret_ref",
            "user_token_secret_ref",
            "token_redacted",
            "user_token_redacted",
        ]
        if "token" in columns:
            select_columns.append("token")
        if "user_token" in columns:
            select_columns.append("user_token")
        rows = self._conn.execute(
            f"SELECT {', '.join(select_columns)} FROM server_connections"
        ).fetchall()
        for row in rows:
            token_ref = row["token_secret_ref"]
            user_token_ref = row["user_token_secret_ref"]
            token_value = self._row_value(row, "token")
            user_token_value = self._row_value(row, "user_token")
            if not token_ref and token_value and token_value != REDACTED_SECRET_VALUE:
                token_ref = self.secret_store.put(
                    self._secret_key(row["id"], "machine-token"),
                    token_value,
                )
            if (
                not user_token_ref
                and user_token_value
                and user_token_value != REDACTED_SECRET_VALUE
            ):
                user_token_ref = self.secret_store.put(
                    self._secret_key(row["id"], "user-token"),
                    user_token_value,
                )
            if token_ref != row["token_secret_ref"] or user_token_ref != row["user_token_secret_ref"]:
                assignments = [
                    "token_redacted = ?",
                    "user_token_redacted = ?",
                    "token_secret_ref = ?",
                    "user_token_secret_ref = ?",
                ]
                values: list[Any] = [
                    REDACTED_SECRET_VALUE,
                    REDACTED_SECRET_VALUE if user_token_ref else None,
                    token_ref,
                    user_token_ref,
                ]
                if "token" in columns:
                    assignments.append("token = ?")
                    values.append(REDACTED_SECRET_VALUE)
                if "user_token" in columns:
                    assignments.append("user_token = ?")
                    values.append(REDACTED_SECRET_VALUE if user_token_ref else None)
                values.append(row["id"])
                self._conn.execute(
                    f"""
                    UPDATE server_connections
                    SET {', '.join(assignments)}
                    WHERE id = ?
                    """,
                    values,
                )

    def _insert_server_connection_locked(self, values: dict[str, Any]) -> None:
        columns = [
            "id",
            "server_url",
            "token_hint",
            "user_token_hint",
            "token_secret_ref",
            "user_token_secret_ref",
            "token_redacted",
            "user_token_redacted",
            "remote_connection_id",
            "owner_id",
            "subject_type",
            "subject_id",
            "machine_id",
            "display_name",
            "capabilities_json",
            "status",
            "heartbeat_interval_seconds",
            "last_heartbeat_at",
            "next_heartbeat_at",
            "lease_expires_at",
            "last_library_snapshot_hash",
            "last_library_snapshot_at",
            "created_at",
            "connected_at",
            "disconnected_at",
            "updated_at",
            "error",
        ]
        params = dict(values)
        table_columns = self._table_columns_locked("server_connections")
        if "token" in table_columns:
            columns.insert(2, "token")
            params["token"] = REDACTED_SECRET_VALUE
        if "user_token" in table_columns:
            columns.insert(columns.index("user_token_hint"), "user_token")
            params["user_token"] = (
                REDACTED_SECRET_VALUE if params.get("user_token_secret_ref") else None
            )
        placeholders = [f":{column}" for column in columns]
        self._conn.execute(
            f"""
            INSERT INTO server_connections(
                {', '.join(columns)}
            )
            VALUES(
                {', '.join(placeholders)}
            )
            """,
            params,
        )

    def save_server_connection(
        self,
        *,
        server_url: str,
        token: str,
        user_token: str,
        connection: dict[str, Any],
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Persist a successful central-server connection in the local DB.

        Tokens are stored through the daemon secret store so a future long-lived
        connector can heartbeat or poll jobs without asking user code to pass
        the token again.  API responses expose only ``token_hint``.
        """

        machine_id = validate_name(connection["machine_id"])
        interval = normalize_heartbeat_interval(
            heartbeat_interval_seconds
            if heartbeat_interval_seconds is not None
            else connection.get("heartbeat_interval_seconds")
        )
        connection_id = uuid4().hex
        now = utc_now()
        token_hint = f"...{token[-6:]}"
        user_token_hint = f"...{user_token[-6:]}"
        token_ref, user_token_ref = self._store_server_connection_secrets(
            connection_id,
            token=token,
            user_token=user_token,
        )
        try:
            with self._lock, self._conn:
                replaced_secret_rows = self._replace_active_server_connections_locked(now)
                self._insert_server_connection_locked(
                    {
                        "id": connection_id,
                        "server_url": server_url.rstrip("/"),
                        "token_hint": token_hint,
                        "user_token_hint": user_token_hint,
                        "token_secret_ref": token_ref,
                        "user_token_secret_ref": user_token_ref,
                        "token_redacted": REDACTED_SECRET_VALUE,
                        "user_token_redacted": REDACTED_SECRET_VALUE,
                        "remote_connection_id": connection.get("id"),
                        "owner_id": connection.get("owner_id"),
                        "subject_type": connection.get("subject_type"),
                        "subject_id": connection.get("subject_id"),
                        "machine_id": machine_id,
                        "display_name": connection.get("display_name"),
                        "capabilities_json": json_dumps(connection.get("capabilities") or {}),
                        "status": connection.get("status") or "connected",
                        "heartbeat_interval_seconds": interval,
                        "last_heartbeat_at": connection.get("last_seen_at") or now,
                        "next_heartbeat_at": iso_after_now(interval),
                        "lease_expires_at": connection.get("expires_at"),
                        "last_library_snapshot_hash": None,
                        "last_library_snapshot_at": None,
                        "created_at": now,
                        "connected_at": connection.get("connected_at") or now,
                        "disconnected_at": connection.get("disconnected_at"),
                        "updated_at": now,
                        "error": None,
                    },
                )
        except Exception:
            self._delete_server_connection_secrets(token_ref, user_token_ref)
            raise
        self._delete_server_connection_secret_rows(replaced_secret_rows)
        return self.get_server_connection(connection_id)

    def save_pending_server_connection(
        self,
        *,
        server_url: str,
        token: str,
        user_token: str,
        machine_id: str,
        display_name: str | None = None,
        capabilities: dict[str, Any] | None = None,
        heartbeat_interval_seconds: float | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Persist an offline server connection attempt for later reconnect."""

        machine_id = validate_name(machine_id)
        interval = normalize_heartbeat_interval(heartbeat_interval_seconds)
        connection_id = uuid4().hex
        now = utc_now()
        token_hint = f"...{token[-6:]}"
        user_token_hint = f"...{user_token[-6:]}"
        token_ref, user_token_ref = self._store_server_connection_secrets(
            connection_id,
            token=token,
            user_token=user_token,
        )
        try:
            with self._lock, self._conn:
                replaced_secret_rows = self._replace_active_server_connections_locked(now)
                self._insert_server_connection_locked(
                    {
                        "id": connection_id,
                        "server_url": server_url.rstrip("/"),
                        "token_hint": token_hint,
                        "user_token_hint": user_token_hint,
                        "token_secret_ref": token_ref,
                        "user_token_secret_ref": user_token_ref,
                        "token_redacted": REDACTED_SECRET_VALUE,
                        "user_token_redacted": REDACTED_SECRET_VALUE,
                        "remote_connection_id": None,
                        "owner_id": None,
                        "subject_type": "machine",
                        "subject_id": machine_id,
                        "machine_id": machine_id,
                        "display_name": display_name or machine_id,
                        "capabilities_json": json_dumps(capabilities or {}),
                        "status": "connect_failed",
                        "heartbeat_interval_seconds": interval,
                        "last_heartbeat_at": None,
                        "next_heartbeat_at": iso_after_now(interval),
                        "lease_expires_at": None,
                        "last_library_snapshot_hash": None,
                        "last_library_snapshot_at": None,
                        "created_at": now,
                        "connected_at": None,
                        "disconnected_at": None,
                        "updated_at": now,
                        "error": error,
                    },
                )
        except Exception:
            self._delete_server_connection_secrets(token_ref, user_token_ref)
            raise
        self._delete_server_connection_secret_rows(replaced_secret_rows)
        return self.get_server_connection(connection_id)

    def complete_server_connection(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any],
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Turn a pending local connection row into a live server lease."""

        connection_id = validate_name(connection_id)
        machine_id = validate_name(remote_connection["machine_id"])
        interval = normalize_heartbeat_interval(
            heartbeat_interval_seconds
            if heartbeat_interval_seconds is not None
            else remote_connection.get("heartbeat_interval_seconds")
        )
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET remote_connection_id = ?,
                    owner_id = ?,
                    subject_type = ?,
                    subject_id = ?,
                    machine_id = ?,
                    display_name = ?,
                    capabilities_json = ?,
                    status = ?,
                    heartbeat_interval_seconds = ?,
                    last_heartbeat_at = ?,
                    next_heartbeat_at = ?,
                    lease_expires_at = ?,
                    connected_at = COALESCE(connected_at, ?),
                    disconnected_at = NULL,
                    updated_at = ?,
                    error = NULL
                WHERE id = ?
                """,
                (
                    remote_connection.get("id"),
                    remote_connection.get("owner_id"),
                    remote_connection.get("subject_type"),
                    remote_connection.get("subject_id"),
                    machine_id,
                    remote_connection.get("display_name"),
                    json_dumps(remote_connection.get("capabilities") or {}),
                    remote_connection.get("status") or "connected",
                    interval,
                    remote_connection.get("last_seen_at") or now,
                    iso_after_now(interval),
                    remote_connection.get("expires_at"),
                    remote_connection.get("connected_at") or now,
                    now,
                    connection_id,
                ),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self.get_server_connection(connection_id)

    def get_server_connection(self, connection_id: str) -> dict[str, Any]:
        """Return one stored central-server connection by local id."""

        connection_id = validate_name(connection_id)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM server_connections WHERE id = ?",
                (connection_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self._server_connection_row(row)

    def get_server_connection_credentials(self, connection_id: str) -> dict[str, Any]:
        """Return one stored connection including the token for daemon internals."""

        connection_id = validate_name(connection_id)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM server_connections WHERE id = ?",
                (connection_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self._server_connection_secret_row(row)

    def current_server_connection(self) -> dict[str, Any] | None:
        """Return the newest active central-server connection, if any."""

        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM server_connections
                WHERE status IN ('connected', 'heartbeat_failed', 'connect_failed')
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return self._server_connection_row(row)

    def current_server_connection_credentials(self) -> dict[str, Any] | None:
        """Return the newest active connection including token for daemon internals."""

        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM server_connections
                WHERE status IN ('connected', 'heartbeat_failed', 'connect_failed')
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return self._server_connection_secret_row(row)

    def list_server_connections(self) -> list[dict[str, Any]]:
        """Return stored central-server connection attempts, newest first."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM server_connections ORDER BY updated_at DESC"
            ).fetchall()
        return [self._server_connection_row(row) for row in rows]

    def record_server_connection_heartbeat(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any],
    ) -> dict[str, Any]:
        """Persist a successful connection heartbeat."""

        connection_id = validate_name(connection_id)
        interval = normalize_heartbeat_interval(
            remote_connection.get("heartbeat_interval_seconds")
        )
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET status = ?,
                    heartbeat_interval_seconds = ?,
                    last_heartbeat_at = ?,
                    next_heartbeat_at = ?,
                    lease_expires_at = ?,
                    updated_at = ?,
                    error = NULL
                WHERE id = ?
                """,
                (
                    remote_connection.get("status") or "connected",
                    interval,
                    remote_connection.get("last_seen_at") or now,
                    iso_after_now(interval),
                    remote_connection.get("expires_at"),
                    now,
                    connection_id,
                ),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self.get_server_connection(connection_id)

    def record_server_connection_library_snapshot(
        self,
        connection_id: str,
        *,
        snapshot_hash: str,
    ) -> dict[str, Any]:
        """Remember the last full library snapshot acknowledged by the server."""

        connection_id = validate_name(connection_id)
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET last_library_snapshot_hash = ?,
                    last_library_snapshot_at = ?,
                    updated_at = ?,
                    error = NULL
                WHERE id = ?
                """,
                (snapshot_hash, now, now, connection_id),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self.get_server_connection(connection_id)

    def record_server_connection_error(
        self,
        connection_id: str,
        *,
        status: str,
        error: str,
    ) -> dict[str, Any]:
        """Persist a heartbeat/connectivity error for diagnostics."""

        connection_id = validate_name(connection_id)
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET status = ?,
                    updated_at = ?,
                    error = ?
                WHERE id = ?
                """,
                (status, now, error, connection_id),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self.get_server_connection(connection_id)

    def mark_server_connection_disconnected(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Mark a local server connection as gracefully disconnected."""

        connection_id = validate_name(connection_id)
        now = utc_now()
        with self._lock, self._conn:
            secret_rows = self._conn.execute(
                """
                SELECT token_secret_ref, user_token_secret_ref
                FROM server_connections
                WHERE id = ?
                """,
                (connection_id,),
            ).fetchall()
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET status = 'disconnected',
                    disconnected_at = ?,
                    lease_expires_at = ?,
                    updated_at = ?,
                    error = NULL,
                    token_secret_ref = NULL,
                    user_token_secret_ref = NULL
                WHERE id = ?
                """,
                (
                    (remote_connection or {}).get("disconnected_at") or now,
                    (remote_connection or {}).get("expires_at"),
                    now,
                    connection_id,
                ),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        self._delete_server_connection_secret_rows(secret_rows)
        return self.get_server_connection(connection_id)

    def _server_connection_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "server_url": row["server_url"],
            "token_hint": row["token_hint"],
            "user_token_hint": row["user_token_hint"],
            "remote_connection_id": self._row_value(row, "remote_connection_id"),
            "owner_id": self._row_value(row, "owner_id"),
            "subject_type": self._row_value(row, "subject_type"),
            "subject_id": self._row_value(row, "subject_id"),
            "machine_id": row["machine_id"],
            "display_name": row["display_name"],
            "capabilities": json_loads(row["capabilities_json"], {}),
            "status": row["status"],
            "heartbeat_interval_seconds": row["heartbeat_interval_seconds"],
            "last_heartbeat_at": row["last_heartbeat_at"],
            "next_heartbeat_at": row["next_heartbeat_at"],
            "lease_expires_at": row["lease_expires_at"],
            "last_library_snapshot_hash": row["last_library_snapshot_hash"],
            "last_library_snapshot_at": row["last_library_snapshot_at"],
            "created_at": row["created_at"],
            "connected_at": self._row_value(row, "connected_at"),
            "disconnected_at": self._row_value(row, "disconnected_at"),
            "updated_at": row["updated_at"],
            "error": self._row_value(row, "error"),
        }

    def _server_connection_secret_row(self, row: sqlite3.Row) -> dict[str, Any]:
        record = self._server_connection_row(row)
        record["token"] = (
            self.secret_store.get(row["token_secret_ref"])
            if row["token_secret_ref"]
            else self._row_value(row, "token")
        )
        record["user_token"] = (
            self.secret_store.get(row["user_token_secret_ref"])
            if row["user_token_secret_ref"]
            else self._row_value(row, "user_token")
        )
        return record
