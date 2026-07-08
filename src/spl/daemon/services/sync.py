"""Sync visibility helpers for daemon diagnostics and runtime responses."""

from __future__ import annotations

from typing import Any

from spl.daemon.store import RegistryStore


class SyncVisibilityService:
    """Build stable summaries for pending outbound sync events."""

    def __init__(self, store: RegistryStore):
        self.store = store

    def summary(
        self,
        events: list[dict[str, Any]] | None = None,
        *,
        limit: int = 200,
    ) -> dict[str, Any]:
        events = events if events is not None else self.store.list_pending_sync_events(limit=limit)
        by_status: dict[str, int] = {}
        by_kind: dict[str, int] = {}
        max_attempts = 0
        last_error = None
        oldest_pending_at = None
        for event in events:
            status = str(event.get("status") or "unknown")
            kind = str(event.get("kind") or "unknown")
            attempts = int(event.get("attempts") or 0)
            by_status[status] = by_status.get(status, 0) + 1
            by_kind[kind] = by_kind.get(kind, 0) + 1
            max_attempts = max(max_attempts, attempts)
            if event.get("error"):
                last_error = event["error"]
            created_at = event.get("created_at")
            if created_at and (oldest_pending_at is None or created_at < oldest_pending_at):
                oldest_pending_at = created_at
        retryable = [event for event in events if event.get("status") in {"pending", "failed"}]
        return {
            "pending": len(events),
            "retryable": len(retryable),
            "by_status": by_status,
            "by_kind": by_kind,
            "max_attempts": max_attempts,
            "last_error": last_error,
            "oldest_pending_at": oldest_pending_at,
            "next_action": ("will_retry_on_next_sync" if retryable else "idle"),
        }

    def decorate_event(self, event: dict[str, Any]) -> dict[str, Any]:
        status = event.get("status")
        attempts = int(event.get("attempts") or 0)
        return {
            **event,
            "retry": {
                "will_retry": status in {"pending", "failed"},
                "next_attempt": attempts + 1 if status in {"pending", "failed"} else None,
                "last_error": event.get("error"),
            },
        }

    def pending_events(self, *, limit: int = 200) -> list[dict[str, Any]]:
        return [self.decorate_event(event) for event in self.store.list_pending_sync_events(limit=limit)]
