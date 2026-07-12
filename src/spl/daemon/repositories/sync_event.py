"""SyncEventRepository aggregate storage."""

from __future__ import annotations

import sqlite3
from typing import Any
from uuid import uuid4

from spl.daemon.storage_base import (
    DEFAULT_OBJECT_OWNER_ID,
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

    def list_pending_sync_events(self, limit: int | None = 100) -> list[dict[str, Any]]:
        limit_clause = "" if limit is None else "LIMIT ?"
        args: tuple[Any, ...] = () if limit is None else (limit,)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM sync_events
                WHERE status IN ('pending', 'failed')
                ORDER BY created_at
                {limit_clause}
                """,
                args,
            ).fetchall()
        return [self._sync_event_row(row) for row in rows]

    def pending_sync_event_identity_summary(
        self,
        current_owner_id: str | None = None,
    ) -> dict[str, Any]:
        """Return owner-routing counts for retryable sync events.

        Events from the pre-enrollment namespace are adoptable by the next
        flush.  Events owned by a different real owner stay pending until the
        daemon reconnects under that identity.
        """

        current_owner_id = str(current_owner_id) if current_owner_id else None
        by_owner: dict[str, int] = {}
        pre_enrollment = 0
        held = 0
        held_owners: set[str] = set()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT payload_json
                FROM sync_events
                WHERE status IN ('pending', 'failed')
                """
            ).fetchall()
        for row in rows:
            payload = json_loads(row["payload_json"], {})
            owner_id = self._payload_owner_id(payload)
            if owner_id is None or owner_id == DEFAULT_OBJECT_OWNER_ID:
                pre_enrollment += 1
                continue
            by_owner[owner_id] = by_owner.get(owner_id, 0) + 1
            if current_owner_id is not None and owner_id != current_owner_id:
                held += 1
                held_owners.add(owner_id)
        return {
            "by_owner": by_owner,
            "pre_enrollment": pre_enrollment,
            "held_for_other_identities": held,
            "held_owner_ids": sorted(held_owners),
        }

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

    def _payload_owner_id(self, payload: dict[str, Any]) -> str | None:
        owner_id = payload.get("owner_id")
        if owner_id is None and isinstance(payload.get("run"), dict):
            owner_id = payload["run"].get("owner_id")
        if owner_id is None:
            return None
        owner_text = str(owner_id)
        return owner_text or None
