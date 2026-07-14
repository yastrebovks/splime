"""ServerConnectionRepository aggregate storage."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from spl.daemon.storage_base import (
    REDACTED_SECRET_VALUE,
    RepositoryBase,
    iso_after_now,
    json_dumps,
    json_loads,
    normalize_heartbeat_interval,
    read_json,
    utc_now,
    validate_name,
    write_json,
)

SERVER_CONNECTION_STATUS_NEEDS_RECONNECT = "needs_reconnect"
ACTIVE_SERVER_CONNECTION_STATUSES = (
    "connected",
    "heartbeat_failed",
    "connect_failed",
    SERVER_CONNECTION_STATUS_NEEDS_RECONNECT,
)
OFFLINE_SERVER_CONNECTION_STATUSES = (
    "heartbeat_failed",
    "connect_failed",
    SERVER_CONNECTION_STATUS_NEEDS_RECONNECT,
)
_MACHINE_IDENTITY_FILE = "server-machine-identity.json"


class ServerConnectionRepository(RepositoryBase):
    """Persist and query serverconnection aggregate records."""

    def _active_status_placeholders(self) -> str:
        return ", ".join("?" for _ in ACTIVE_SERVER_CONNECTION_STATUSES)

    def _machine_identity_path(self) -> Path:
        return cast(Path, self.home) / _MACHINE_IDENTITY_FILE

    def _read_local_machine_id(self) -> str | None:
        payload = read_json(self._machine_identity_path(), {})
        if not isinstance(payload, dict):
            return None
        machine_id = payload.get("machine_id")
        if not machine_id:
            return None
        try:
            return validate_name(str(machine_id))
        except ValueError:
            return None

    def _write_local_machine_id(self, machine_id: str) -> None:
        write_json(self._machine_identity_path(), {"machine_id": validate_name(machine_id)})

    def _current_server_connection_row_locked(self) -> sqlite3.Row | None:
        """Return the identity row for this daemon's machine, then newest row.

        The daemon's local machine identity is persisted in the daemon home
        after a successful owner-bearing enrollment or heartbeat, and older
        homes can backfill it from a single owned identity row. Connectivity is
        separate: ``needs_reconnect`` rows still identify the enrolled owner
        locally, but server-backed operations must reconnect before sync. If
        the machine identity is present, copied rows for other machines do not
        win solely because their ``updated_at`` is newer. Older daemon homes
        without a backfillable sidecar keep the historical newest-row behavior,
        but ownerless rows are never identity candidates.
        """

        placeholders = self._active_status_placeholders()
        local_machine_id = self._read_local_machine_id()
        if local_machine_id is not None:
            row = self._conn.execute(
                f"""
                SELECT * FROM server_connections
                WHERE status IN ({placeholders})
                  AND owner_id IS NOT NULL
                  AND machine_id = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (*ACTIVE_SERVER_CONNECTION_STATUSES, local_machine_id),
            ).fetchone()
            if row is not None:
                return cast(sqlite3.Row, row)
        row = self._conn.execute(
            f"""
            SELECT * FROM server_connections
            WHERE status IN ({placeholders})
              AND owner_id IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            ACTIVE_SERVER_CONNECTION_STATUSES,
        ).fetchone()
        return cast(sqlite3.Row | None, row)

    def backfill_local_machine_identity(self) -> dict[str, Any]:
        """Backfill the sidecar when exactly one owned identity machine exists."""

        if self._read_local_machine_id() is not None:
            return {"backfilled": False, "reason": "already_present"}
        placeholders = self._active_status_placeholders()
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT DISTINCT machine_id
                FROM server_connections
                WHERE status IN ({placeholders})
                  AND owner_id IS NOT NULL
                  AND machine_id IS NOT NULL
                """,
                ACTIVE_SERVER_CONNECTION_STATUSES,
            ).fetchall()
        machine_ids = sorted({str(row["machine_id"]) for row in rows if row["machine_id"]})
        if len(machine_ids) != 1:
            return {
                "backfilled": False,
                "reason": "ambiguous" if machine_ids else "missing_owned_active",
                "machine_ids": machine_ids,
            }
        self._write_local_machine_id(machine_ids[0])
        return {"backfilled": True, "machine_id": machine_ids[0]}

    def recover_lease_rejected_identity_rows(self) -> dict[str, Any]:
        """Recover the freshest legacy stale lease row as an offline identity.

        Older daemons wrote heartbeat lease rejections (401/403/404/409) as
        ``stale``. That made otherwise valid stored credentials disappear from
        identity selection. The secret-bearing latest such row is safe to keep
        as identity but not connectivity, so it becomes ``needs_reconnect``.
        """

        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT id
                FROM server_connections
                WHERE status = 'stale'
                  AND owner_id IS NOT NULL
                  AND remote_connection_id IS NOT NULL
                  AND token_secret_ref IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return {"recovered": False, "reason": "no_legacy_stale_identity"}
            self._conn.execute(
                """
                UPDATE server_connections
                SET status = ?,
                    error = COALESCE(NULLIF(error, ''), ?)
                WHERE id = ?
                """,
                (
                    SERVER_CONNECTION_STATUS_NEEDS_RECONNECT,
                    "legacy stale lease requires reconnect",
                    row["id"],
                ),
            )
        return {"recovered": True, "id": row["id"]}

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
        *,
        confirmed_owner_id: str | None,
        confirmed_remote_connection_id: str | None,
    ) -> list[sqlite3.Row]:
        if not confirmed_owner_id or not confirmed_remote_connection_id:
            raise ValueError("active server connections can be replaced only after confirmed server enrollment")
        placeholders = self._active_status_placeholders()
        rows = self._conn.execute(
            f"""
            SELECT token_secret_ref, user_token_secret_ref
            FROM server_connections
            WHERE status IN ({placeholders})
            """,
            ACTIVE_SERVER_CONNECTION_STATUSES,
        ).fetchall()
        self._conn.execute(
            f"""
            UPDATE server_connections
            SET status = 'replaced',
                disconnected_at = ?,
                updated_at = ?,
                token_secret_ref = NULL,
                user_token_secret_ref = NULL
            WHERE status IN ({placeholders})
            """,
            (now, now, *ACTIVE_SERVER_CONNECTION_STATUSES),
        )
        return cast(list[sqlite3.Row], rows)

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
        rows = self._conn.execute(f"SELECT {', '.join(select_columns)} FROM server_connections").fetchall()
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
            if not user_token_ref and user_token_value and user_token_value != REDACTED_SECRET_VALUE:
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
                    SET {", ".join(assignments)}
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
            params["user_token"] = REDACTED_SECRET_VALUE if params.get("user_token_secret_ref") else None
        placeholders = [f":{column}" for column in columns]
        self._conn.execute(
            f"""
            INSERT INTO server_connections(
                {", ".join(columns)}
            )
            VALUES(
                {", ".join(placeholders)}
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

        if not connection.get("owner_id"):
            raise ValueError("server connection requires owner_id")
        remote_connection_id = connection.get("id")
        if not remote_connection_id:
            raise ValueError("server connection requires remote_connection_id")
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
                replaced_secret_rows = self._replace_active_server_connections_locked(
                    now,
                    confirmed_owner_id=str(connection["owner_id"]),
                    confirmed_remote_connection_id=str(remote_connection_id),
                )
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
                        "remote_connection_id": str(remote_connection_id),
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
        self._write_local_machine_id(machine_id)
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
        now = utc_now()
        token_hint = f"...{token[-6:]}"
        user_token_hint = f"...{user_token[-6:]}"
        server_url = server_url.rstrip("/")
        with self._lock:
            existing = self._conn.execute(
                """
                SELECT id, token_secret_ref, user_token_secret_ref
                FROM server_connections
                WHERE server_url = ?
                  AND machine_id = ?
                  AND owner_id IS NULL
                  AND remote_connection_id IS NULL
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (server_url, machine_id),
            ).fetchone()
        connection_id = existing["id"] if existing is not None else uuid4().hex
        token_ref, user_token_ref = self._store_server_connection_secrets(
            connection_id,
            token=token,
            user_token=user_token,
        )
        replaced_secret_rows: list[sqlite3.Row] = []
        try:
            with self._lock, self._conn:
                if existing is not None:
                    replaced_secret_rows = [existing]
                    self._conn.execute(
                        """
                        UPDATE server_connections
                        SET token_hint = ?,
                            user_token_hint = ?,
                            token_secret_ref = ?,
                            user_token_secret_ref = ?,
                            token_redacted = ?,
                            user_token_redacted = ?,
                            subject_type = 'machine',
                            subject_id = ?,
                            display_name = ?,
                            capabilities_json = ?,
                            status = 'enroll_failed',
                            heartbeat_interval_seconds = ?,
                            last_heartbeat_at = NULL,
                            next_heartbeat_at = ?,
                            lease_expires_at = NULL,
                            connected_at = NULL,
                            disconnected_at = NULL,
                            updated_at = ?,
                            error = ?
                        WHERE id = ?
                        """,
                        (
                            token_hint,
                            user_token_hint,
                            token_ref,
                            user_token_ref,
                            REDACTED_SECRET_VALUE,
                            REDACTED_SECRET_VALUE,
                            machine_id,
                            display_name or machine_id,
                            json_dumps(capabilities or {}),
                            interval,
                            iso_after_now(interval),
                            now,
                            error,
                            connection_id,
                        ),
                    )
                else:
                    self._insert_server_connection_locked(
                        {
                            "id": connection_id,
                            "server_url": server_url,
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
                            "status": "enroll_failed",
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
        for row in replaced_secret_rows:
            if row["token_secret_ref"] != token_ref:
                self.secret_store.delete(row["token_secret_ref"])
            if row["user_token_secret_ref"] != user_token_ref:
                self.secret_store.delete(row["user_token_secret_ref"])
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
        if not remote_connection.get("owner_id"):
            raise ValueError("server connection requires owner_id")
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
        self._write_local_machine_id(machine_id)
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

    def find_pending_server_connection(
        self,
        *,
        server_url: str,
        machine_id: str,
    ) -> dict[str, Any] | None:
        """Return the reusable ownerless enrollment attempt row, if present."""

        machine_id = validate_name(machine_id)
        with self._lock:
            row = self._conn.execute(
                """
                SELECT *
                FROM server_connections
                WHERE server_url = ?
                  AND machine_id = ?
                  AND owner_id IS NULL
                  AND remote_connection_id IS NULL
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (server_url.rstrip("/"), machine_id),
            ).fetchone()
        if row is None:
            return None
        return self._server_connection_row(row)

    def current_server_connection(self) -> dict[str, Any] | None:
        """Return the preferred active central-server connection, if any.

        Rows for the daemon home's stored machine id are preferred first; within
        that machine, the newest ``updated_at`` wins.  If the home has no stored
        machine id yet, this falls back to the historical newest-active row.
        """

        with self._lock:
            row = self._current_server_connection_row_locked()
        if row is None:
            return None
        return self._server_connection_row(row)

    def current_server_connection_credentials(self) -> dict[str, Any] | None:
        """Return preferred active connection credentials for daemon internals.

        The picker matches :meth:`current_server_connection`: prefer the daemon
        home's stored machine id, then newest ``updated_at``.  This keeps
        identity selection deterministic when a copied database contains active
        rows from more than one machine.
        """

        with self._lock:
            row = self._current_server_connection_row_locked()
        if row is None:
            return None
        return self._server_connection_secret_row(row)

    def list_server_connections(self) -> list[dict[str, Any]]:
        """Return stored central-server connection attempts, newest first."""

        with self._lock:
            rows = self._conn.execute("SELECT * FROM server_connections ORDER BY updated_at DESC").fetchall()
        return [self._server_connection_row(row) for row in rows]

    def server_connection_summary(self, *, older_than_days: int | None = 30) -> dict[str, Any]:
        """Return identity and hygiene counts for diagnostics."""

        cutoff = self._stale_cutoff(older_than_days)
        with self._lock:
            current_row = self._current_server_connection_row_locked()
            rows = self._conn.execute("SELECT * FROM server_connections ORDER BY updated_at DESC").fetchall()
        owners = sorted({str(row["owner_id"]) for row in rows if row["owner_id"]})
        stale_count = sum(1 for row in rows if self._stale_reasons(row, cutoff=cutoff))
        identity_degraded = current_row is None and bool(rows)
        offline_replaced_identity_rows = 0
        if current_row is None:
            offline_replaced_identity_rows = sum(
                1
                for row in rows
                if row["status"] == "replaced"
                and row["owner_id"]
                and row["remote_connection_id"]
                and not row["token_secret_ref"]
            )
        return {
            "current_id": current_row["id"] if current_row is not None else None,
            "current_machine_id": current_row["machine_id"]
            if current_row is not None
            else self._read_local_machine_id(),
            "stored_count": len(rows),
            "stored_credential_rows": len(rows),
            "stale_count": stale_count,
            "owners": owners,
            "owner_count": len(owners),
            "identity_degraded": identity_degraded,
            "offline_replaced_identity_rows": offline_replaced_identity_rows,
        }

    def prune_server_connections(
        self,
        *,
        older_than_days: int | None = 30,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Delete stale stored server connections while keeping the current row."""

        cutoff = self._stale_cutoff(older_than_days)
        with self._lock, self._conn:
            current_row = self._current_server_connection_row_locked()
            current_id = current_row["id"] if current_row is not None else None
            rows = self._conn.execute("SELECT * FROM server_connections ORDER BY updated_at DESC").fetchall()
            stale_rows: list[sqlite3.Row] = []
            stale_records: list[dict[str, Any]] = []
            kept_current: list[dict[str, Any]] = []
            for row in rows:
                reasons = self._stale_reasons(row, cutoff=cutoff)
                if not reasons:
                    continue
                record = self._server_connection_row(row)
                record["stale_reasons"] = reasons
                if row["id"] == current_id:
                    kept_current.append(record)
                    continue
                stale_records.append(record)
                stale_rows.append(row)

            if not dry_run and stale_rows:
                self._conn.executemany(
                    "DELETE FROM server_connections WHERE id = ?",
                    [(row["id"],) for row in stale_rows],
                )
        if not dry_run and stale_rows:
            self._delete_server_connection_secret_rows(stale_rows)
        return {
            "dry_run": dry_run,
            "older_than_days": older_than_days,
            "stale": [*stale_records, *kept_current] if dry_run else kept_current,
            "pruned": [] if dry_run else stale_records,
            "kept_current": kept_current,
            "count": 0 if dry_run else len(stale_records),
        }

    def _stale_cutoff(self, older_than_days: int | None) -> datetime | None:
        if older_than_days is None:
            return None
        if older_than_days < 0:
            raise ValueError("older_than_days must be non-negative")
        return datetime.now(UTC) - timedelta(days=older_than_days)

    def _stale_reasons(self, row: sqlite3.Row, *, cutoff: datetime | None) -> list[str]:
        reasons = []
        if row["status"] not in ACTIVE_SERVER_CONNECTION_STATUSES:
            reasons.append("inactive_status")
        if not row["owner_id"]:
            reasons.append("missing_owner")
        if cutoff is not None and self._row_is_older_than(row, cutoff):
            reasons.append("older_than_days")
        return reasons

    def _row_is_older_than(self, row: sqlite3.Row, cutoff: datetime) -> bool:
        value = row["updated_at"]
        if not value:
            return False
        try:
            updated_at = datetime.fromisoformat(str(value))
        except ValueError:
            return False
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=UTC)
        return updated_at < cutoff

    def record_server_connection_heartbeat(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any],
    ) -> dict[str, Any]:
        """Persist a successful connection heartbeat."""

        connection_id = validate_name(connection_id)
        interval = normalize_heartbeat_interval(remote_connection.get("heartbeat_interval_seconds"))
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
        record = self.get_server_connection(connection_id)
        if record.get("owner_id"):
            self._write_local_machine_id(str(record["machine_id"]))
        return record

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
        if status == "stale":
            status = SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET status = CASE
                        WHEN owner_id IS NULL THEN 'enroll_failed'
                        ELSE ?
                    END,
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
            self.secret_store.get(row["token_secret_ref"]) if row["token_secret_ref"] else self._row_value(row, "token")
        )
        record["user_token"] = (
            self.secret_store.get(row["user_token_secret_ref"])
            if row["user_token_secret_ref"]
            else self._row_value(row, "user_token")
        )
        return record
