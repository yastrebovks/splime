"""LibraryRepository aggregate storage."""

from __future__ import annotations

import hashlib
import importlib.metadata
import sqlite3
import subprocess
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from spl.daemon.runtime_config import normalize_runtime_config
from spl.daemon.storage_base import (
    REDACTED_SECRET_VALUE,
    RepositoryBase,
    iso_after_now,
    json_dumps,
    json_loads,
    normalize_heartbeat_interval,
    read_json,
    split_object_function_ref,
    utc_now,
    validate_name,
    write_json,
)



class LibraryRepository(RepositoryBase):
    """Persist and query library aggregate records."""

    def remote_signature_key_for(self, ref: dict[str, Any]) -> str:
        """Return a stable cache key for one remote object reference."""

        normalized = self._normalize_remote_signature_ref(ref)
        return hashlib.sha256(json_dumps(normalized).encode("utf-8")).hexdigest()

    def get_remote_signature(self, ref: dict[str, Any]) -> dict[str, Any] | None:
        """Return a cached remote signature row, if present."""

        key = self.remote_signature_key_for(ref)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM remote_signatures WHERE id = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return self._remote_signature_row(row)

    def list_remote_signatures(self) -> list[dict[str, Any]]:
        """Return cached remote signatures, newest updates first."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM remote_signatures ORDER BY updated_at DESC"
            ).fetchall()
        return [self._remote_signature_row(row) for row in rows]

    def save_remote_signature(
        self,
        ref: dict[str, Any],
        signature: dict[str, Any],
        *,
        status: str = "resolved",
        error: str | None = None,
    ) -> dict[str, Any]:
        """Persist a remote object signature resolved from the server."""

        normalized = self._normalize_remote_signature_ref(ref)
        key = self.remote_signature_key_for(normalized)
        now = utc_now()
        with self._lock, self._conn:
            existing = self._conn.execute(
                "SELECT created_at FROM remote_signatures WHERE id = ?",
                (key,),
            ).fetchone()
            created_at = existing["created_at"] if existing is not None else now
            self._conn.execute(
                """
                INSERT INTO remote_signatures(
                    id, server_url, owner_id, library, object_name, version, version_id,
                    signature_json, status, error, fetched_at, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    signature_json = excluded.signature_json,
                    status = excluded.status,
                    error = excluded.error,
                    fetched_at = excluded.fetched_at,
                    updated_at = excluded.updated_at
                """,
                (
                    key,
                    normalized["server_url"],
                    normalized.get("owner_id"),
                    normalized.get("library"),
                    normalized["object_name"],
                    normalized.get("version"),
                    normalized.get("version_id"),
                    json_dumps(signature),
                    status,
                    error,
                    now if status == "resolved" else None,
                    created_at,
                    now,
                ),
            )
        record = self.get_remote_signature(normalized)
        if record is None:
            raise KeyError(f"remote signature is not found: {key}")
        return record

    def mark_remote_signature_unavailable(
        self,
        ref: dict[str, Any],
        error: str,
    ) -> dict[str, Any]:
        """Persist an unavailable remote signature state for diagnostics."""

        cached = self.get_remote_signature(ref)
        signature = cached["signature"] if cached is not None else {}
        return self.save_remote_signature(
            ref,
            signature,
            status="unavailable",
            error=error,
        )

    def _remote_signature_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "server_url": row["server_url"],
            "owner_id": row["owner_id"],
            "library": row["library"],
            "object_name": row["object_name"],
            "version": row["version"],
            "version_id": row["version_id"],
            "signature": json_loads(row["signature_json"], {}),
            "status": row["status"],
            "error": row["error"],
            "fetched_at": row["fetched_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _normalize_remote_signature_ref(self, ref: dict[str, Any]) -> dict[str, Any]:
        server_url = str(ref.get("server_url") or ref.get("url") or "").rstrip("/")
        object_name = str(
            ref.get("object_name")
            or ref.get("object")
            or ref.get("name")
            or ""
        )
        raw_function = ref.get("function") or ref.get("entrypoint")
        if not server_url:
            raise ValueError("remote signature ref requires url/server_url")
        if not object_name:
            raise ValueError("remote signature ref requires name/object_name")
        object_name, function = split_object_function_ref(object_name, raw_function)

        version = ref.get("version")
        version_id = ref.get("version_id")
        owner_id = ref.get("owner_id") or ref.get("owner")
        library = ref.get("library") or ref.get("library_slug")
        normalized_owner = None if owner_id is None or owner_id == "" else str(owner_id)
        normalized_library = None if library is None or library == "" else str(library)
        normalized_version = None if version is None or version == "" else str(version)
        normalized_version_id = (
            None if version_id is None or version_id == "" else str(version_id)
        )
        return {
            "server_url": server_url,
            "owner_id": normalized_owner,
            "library": normalized_library,
            "object_name": object_name,
            "function": function,
            "version": normalized_version,
            "version_id": normalized_version_id,
        }
