"""SyncEventRepository aggregate storage."""

from __future__ import annotations

import sqlite3
from typing import Any
from uuid import uuid4

from spl.daemon.storage_base import (
    RepositoryBase,
    json_dumps,
    json_loads,
    utc_now,
    validate_name,
)


class SyncEventRepository(RepositoryBase):
    """Persist and query syncevent aggregate records."""

    def enqueue_sync_event(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Persist one outbound sync event for the central server."""

        kind = validate_name(kind)
        event_id = uuid4().hex
        now = utc_now()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO sync_events(
                    id, kind, payload_json, status, attempts, created_at, updated_at
                )
                VALUES(?, ?, ?, 'pending', 0, ?, ?)
                """,
                (event_id, kind, json_dumps(payload), now, now),
            )
        return self.get_sync_event(event_id)

    def get_sync_event(self, event_id: str) -> dict[str, Any]:
        event_id = validate_name(event_id)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM sync_events WHERE id = ?",
                (event_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"sync event is not found: {event_id}")
        return self._sync_event_row(row)

    def list_pending_sync_events(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM sync_events
                WHERE status IN ('pending', 'failed')
                ORDER BY created_at
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._sync_event_row(row) for row in rows]

    def mark_sync_event_sent(self, event_id: str) -> dict[str, Any]:
        event_id = validate_name(event_id)
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE sync_events
                SET status = 'sent', sent_at = ?, updated_at = ?, error = NULL
                WHERE id = ?
                """,
                (now, now, event_id),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"sync event is not found: {event_id}")
        return self.get_sync_event(event_id)

    def mark_sync_event_failed(self, event_id: str, error: str) -> dict[str, Any]:
        event_id = validate_name(event_id)
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE sync_events
                SET status = 'failed',
                    attempts = attempts + 1,
                    updated_at = ?,
                    error = ?
                WHERE id = ?
                """,
                (now, error, event_id),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"sync event is not found: {event_id}")
        return self.get_sync_event(event_id)

    def _sync_event_row(self, row: sqlite3.Row) -> dict[str, Any]:
        status = row["status"]
        attempts = int(row["attempts"] or 0)
        will_retry = status in {"pending", "failed"}
        return {
            "id": row["id"],
            "kind": row["kind"],
            "payload": json_loads(row["payload_json"], {}),
            "status": status,
            "attempts": attempts,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "sent_at": row["sent_at"],
            "error": row["error"],
            "retry": {
                "will_retry": will_retry,
                "next_attempt": attempts + 1 if will_retry else None,
                "last_error": row["error"],
            },
        }
