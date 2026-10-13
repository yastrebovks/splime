"""Durable, bounded catalogue transfer independent of ordinary sync events.

The registry remains authoritative. SQLite retains only the current transfer
and content cache for each identity; accepted hashes can be reused on the server.
No network operation or JSON/YAML construction holds the SQLite mutex.
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
from typing import Any, Callable
from uuid import uuid4

from spl.core import json_contract
from spl.daemon.sync_diagnostics import SyncPayloadError, sync_failure

CATALOGUE_CAPABILITY = "catalogue_manifest_v1"
CATALOGUE_REQUEST_MAX_BYTES = 128 * 1024
CATALOGUE_PART_BYTES = 48 * 1024
CATALOGUE_OBJECT_MAX_BYTES = 16 * 1024 * 1024
CATALOGUE_TOTAL_MAX_BYTES = 64 * 1024 * 1024
CATALOGUE_MAX_OBJECTS = 1024
CATALOGUE_MAX_REQUESTS_PER_TICK = 16
CATALOGUE_TICK_SECONDS = 5.0


def wire_bytes(value: Any) -> bytes:
    return json_contract.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")


def _invalid_receipt() -> Exception:
    # remote_client imports the wire-size constants above. Import its typed
    # response error only once a live exchange is being validated.
    from spl.daemon.remote_client import ServerResponseError

    return ServerResponseError(code="catalogue_response_invalid")


def _validate_committed_receipt(receipt: Any, state: dict[str, Any]) -> None:
    if (
        not isinstance(receipt, dict)
        or receipt.get("state") != "committed"
        or receipt.get("snapshot_id") != state["snapshot_id"]
        or receipt.get("snapshot_hash") != state["snapshot_hash"]
        or not isinstance(receipt.get("id"), str)
        or not receipt["id"]
        or len(receipt["id"]) > 128
        or type(receipt.get("item_count")) is not int
        or receipt["item_count"] != len(state["objects"])
        or type(receipt.get("unchanged")) is not bool
    ):
        raise _invalid_receipt()


def _validate_pending_receipt(receipt: Any, state: dict[str, Any]) -> dict[str, list[int]]:
    if (
        not isinstance(receipt, dict)
        or receipt.get("state") != "pending"
        or receipt.get("snapshot_id") != state["snapshot_id"]
        or type(receipt.get("max_part_bytes")) is not int
        or receipt["max_part_bytes"] != CATALOGUE_PART_BYTES
    ):
        raise _invalid_receipt()
    known = {obj["sha256"]: obj["part_count"] for obj in state["objects"]}
    missing = receipt.get("missing")
    if (
        not isinstance(missing, list)
        or len(missing) > len(known)
        or any(not isinstance(digest, str) or digest not in known for digest in missing)
        or len(set(missing)) != len(missing)
    ):
        raise _invalid_receipt()
    missing_parts = receipt.get("missing_parts")
    if not isinstance(missing_parts, dict) or set(missing_parts) != set(missing):
        raise _invalid_receipt()
    # Validate every requested index before uploading any content. The server
    # reports the complete missing set on each begin, including after restart.
    for digest, parts in missing_parts.items():
        if (
            not isinstance(parts, list)
            or not 0 < len(parts) <= known[digest]
            or any(type(index) is not int or not 0 <= index < known[digest] for index in parts)
            or len(set(parts)) != len(parts)
        ):
            raise _invalid_receipt()
    return missing_parts


def _validate_content_receipt(receipt: Any, snapshot_id: str, digest: str, index: int) -> None:
    if (
        not isinstance(receipt, dict)
        or receipt.get("snapshot_id") != snapshot_id
        or receipt.get("sha256") != digest
        or type(receipt.get("part_index")) is not int
        or receipt["part_index"] != index
        or type(receipt.get("complete")) is not bool
    ):
        raise _invalid_receipt()


class CatalogueSyncService:
    def __init__(self, store: Any) -> None:
        self.store = store
        with store._lock, store._conn:
            store._conn.executescript("""
                CREATE TABLE IF NOT EXISTS catalogue_sync_state (
                    scope TEXT PRIMARY KEY, state_json TEXT NOT NULL, updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS catalogue_sync_content (
                    scope TEXT NOT NULL, metadata_hash TEXT NOT NULL,
                    sha256 TEXT NOT NULL, content BLOB NOT NULL,
                    PRIMARY KEY(scope, metadata_hash)
                );
                CREATE TABLE IF NOT EXISTS catalogue_sync_registrations (
                    version_id TEXT PRIMARY KEY REFERENCES object_versions(id) ON DELETE CASCADE,
                    registration_id TEXT NOT NULL, options_json TEXT NOT NULL
                );
            """)

    def defer_registration(self, version_id: str, options: dict[str, Any]) -> str:
        """Retain one-shot publication intent while the registry retains YAML."""
        registration_id = uuid4().hex
        with self.store._lock, self.store._conn:
            self.store._conn.execute(
                "DELETE FROM catalogue_sync_registrations WHERE version_id IN "
                "(SELECT id FROM object_versions WHERE object_id = "
                "(SELECT object_id FROM object_versions WHERE id = ?))",
                (version_id,),
            )
            self.store._conn.execute(
                "INSERT INTO catalogue_sync_registrations VALUES (?, ?, ?)",
                (version_id, registration_id, json.dumps(options)),
            )
        credentials = self.store.current_server_connection_credentials()
        if credentials is not None:
            scope = self.scope(credentials)
            state = self._read(scope)
            state["state"] = "pending"
            state.pop("diagnostic", None)
            self._write(scope, state)
        return registration_id

    def registration_intents(self) -> dict[str, dict[str, Any]]:
        with self.store._lock:
            rows = self.store._conn.execute("SELECT * FROM catalogue_sync_registrations").fetchall()
        return {
            row["version_id"]: {**json.loads(row["options_json"]), "registration_id": row["registration_id"]}
            for row in rows
        }

    @staticmethod
    def scope(credentials: dict[str, Any]) -> str:
        return hashlib.sha256(
            wire_bytes(
                [
                    credentials.get("server_url"),
                    credentials.get("owner_id"),
                    credentials.get("machine_id"),
                ]
            )
        ).hexdigest()

    def _read(self, scope: str) -> dict[str, Any]:
        with self.store._lock:
            row = self.store._conn.execute(
                "SELECT state_json FROM catalogue_sync_state WHERE scope = ?",
                (scope,),
            ).fetchone()
        return json.loads(row["state_json"]) if row else {}

    def _write(self, scope: str, state: dict[str, Any]) -> None:
        encoded = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
        with self.store._lock, self.store._conn:
            self.store._conn.execute(
                "INSERT OR REPLACE INTO catalogue_sync_state VALUES (?, ?, ?)",
                (scope, encoded, time.time()),
            )
            # Keep sequence fences (small rows) across owner/token switches;
            # bound source content cache to the one currently active context.
            self.store._conn.execute("DELETE FROM catalogue_sync_content WHERE scope != ?", (scope,))
            self.store._conn.execute(
                "DELETE FROM catalogue_sync_state WHERE scope != ? AND (updated_at < ? OR scope IN "
                "(SELECT scope FROM catalogue_sync_state ORDER BY updated_at DESC LIMIT -1 OFFSET 32))",
                (scope, time.time() - 7 * 24 * 60 * 60),
            )

    def status(self, credentials: dict[str, Any] | None) -> dict[str, Any]:
        if credentials is None:
            return {"state": "disconnected"}
        state = self._read(self.scope(credentials))
        return {
            key: state[key]
            for key in (
                "state",
                "snapshot_id",
                "snapshot_hash",
                "diagnostic",
                "uploaded_parts",
                "reused_objects",
            )
            if key in state
        } or {"state": "pending"}

    def record_failure(
        self, credentials: dict[str, Any], exc: BaseException, snapshot_hash: str | None = None
    ) -> dict[str, Any]:
        scope = self.scope(credentials)
        state = self._read(scope)
        diagnostic = sync_failure(exc, operation="catalogue_sync", identifier=state.get("snapshot_id") or snapshot_hash)
        state.update(state="degraded", diagnostic=diagnostic)
        state["connection_id"] = str(credentials.get("remote_connection_id") or "")
        if snapshot_hash is not None:
            if state.get("snapshot_hash") != snapshot_hash:
                state.pop("objects", None)
            state["snapshot_hash"] = snapshot_hash
        self._write(scope, state)
        return diagnostic

    def record_committed(self, credentials: dict[str, Any], snapshot_hash: str) -> None:
        scope = self.scope(credentials)
        state = self._read(scope)
        if state.get("snapshot_hash") == snapshot_hash:
            # An acknowledgement consumes only the exact intents in this
            # transfer, never a concurrent registration of the same version.
            with self.store._lock, self.store._conn:
                self.store._conn.executemany(
                    "DELETE FROM catalogue_sync_registrations WHERE version_id = ? AND registration_id = ?",
                    state.pop("registration_intents", []),
                )
        state.update(state="current", snapshot_hash=snapshot_hash)
        state.pop("diagnostic", None)
        self._write(scope, state)

    def synchronize(
        self,
        *,
        credentials: dict[str, Any],
        server: Any,
        snapshot_hash: str,
        manifest_items: list[dict[str, Any]],
        renew_lease: Callable[[], None],
    ) -> dict[str, Any]:
        scope = self.scope(credentials)
        state = self._read(scope)
        context = str(credentials["remote_connection_id"])
        diagnostic = state.get("diagnostic") or {}
        if (
            state.get("snapshot_hash") == snapshot_hash
            and state.get("connection_id") == context
            and state.get("state") == "degraded"
            and diagnostic.get("retryable") is False
            and diagnostic.get("category") != "unsupported_capability"
        ):
            return self.status(credentials)
        if (
            state.get("snapshot_hash") != snapshot_hash
            or state.get("connection_id") != context
            or "objects" not in state
        ):
            state = self._prepare(scope, state, context, snapshot_hash, manifest_items, renew_lease)
        diagnostic = state.get("diagnostic") or {}
        if state.get("state") == "degraded" and diagnostic.get("retryable") is False:
            return self.status(credentials)
        common = {
            "machine_id": credentials["machine_id"],
            "connection_id": context,
            "snapshot_id": state["snapshot_id"],
        }
        started = time.monotonic()
        try:
            begin = server.catalogue_sync_begin(
                {
                    **common,
                    "sequence": state["sequence"],
                    "snapshot_hash": snapshot_hash,
                    "objects": state["objects"],
                }
            )
            if isinstance(begin, dict) and begin.get("state") == "committed":
                _validate_committed_receipt(begin, state)
                self.record_committed(credentials, snapshot_hash)
                return self.status(credentials)
            missing_parts = _validate_pending_receipt(begin, state)
            state["reused_objects"] = len(state["objects"]) - len(missing_parts)
            requests = 1
            offsets = state.setdefault("next_parts", {})
            for obj in state["objects"]:
                digest = obj["sha256"]
                if digest not in missing_parts:
                    continue
                with self.store._lock:
                    row = self.store._conn.execute(
                        "SELECT content FROM catalogue_sync_content WHERE scope = ? AND sha256 = ?",
                        (scope, digest),
                    ).fetchone()
                if row is None:
                    # A different active identity may have evicted this cache.
                    state.pop("objects", None)
                    state["state"] = "pending"
                    self._write(scope, state)
                    return self.status(credentials)
                content = bytes(row["content"])
                for index in missing_parts[digest]:
                    if (
                        requests >= CATALOGUE_MAX_REQUESTS_PER_TICK
                        or time.monotonic() - started >= CATALOGUE_TICK_SECONDS
                    ):
                        state["state"] = "pending"
                        self._write(scope, state)
                        return self.status(credentials)
                    renew_lease()
                    content_receipt = server.catalogue_sync_content(
                        {
                            **common,
                            "sha256": digest,
                            "part_index": index,
                            "data": base64.b64encode(
                                content[index * CATALOGUE_PART_BYTES : (index + 1) * CATALOGUE_PART_BYTES]
                            ).decode("ascii"),
                        }
                    )
                    _validate_content_receipt(content_receipt, common["snapshot_id"], digest, index)
                    requests += 1
                    offsets[digest] = index + 1
                    state["uploaded_parts"] = state.get("uploaded_parts", 0) + 1
                    self._write(scope, state)
            if requests >= CATALOGUE_MAX_REQUESTS_PER_TICK or time.monotonic() - started >= CATALOGUE_TICK_SECONDS:
                state["state"] = "pending"
                self._write(scope, state)
                return self.status(credentials)
            renew_lease()
            self._write(scope, state)
            receipt = server.catalogue_sync_commit(common)
            _validate_committed_receipt(receipt, state)
            self.record_committed(credentials, snapshot_hash)
        except Exception as exc:
            # Retry only a recoverable response, retaining the exact transfer.
            diagnostic = sync_failure(exc, operation="catalogue_sync", identifier=state["snapshot_id"])
            payload = getattr(exc, "payload", None) or {}
            if diagnostic["code"] == "catalogue_incomplete":
                diagnostic["retryable"] = True
                state["next_parts"] = {}
            if diagnostic["code"] == "catalogue_stale_snapshot":
                latest = payload.get("latest_sequence")
                if type(latest) is int and latest >= state["sequence"]:
                    state["sequence"] = latest
                    state.pop("objects", None)
                    diagnostic["retryable"] = True
            state.update(state="degraded", diagnostic=diagnostic)
            self._write(scope, state)
        return self.status(credentials)

    def _prepare(
        self,
        scope: str,
        previous: dict[str, Any],
        context: str,
        snapshot_hash: str,
        items: list[dict[str, Any]],
        renew_lease: Callable[[], None],
    ) -> dict[str, Any]:
        if len(items) > CATALOGUE_MAX_OBJECTS:
            raise SyncPayloadError(
                "catalogue_object_count_exceeded", operation="catalogue_sync", identifier=snapshot_hash
            )
        objects = []
        metadata_hashes = [hashlib.sha256(wire_bytes(item)).hexdigest() for item in items]
        keep = set(metadata_hashes)
        with self.store._lock, self.store._conn:
            rows = self.store._conn.execute(
                "SELECT metadata_hash FROM catalogue_sync_content WHERE scope = ?", (scope,)
            ).fetchall()
            self.store._conn.executemany(
                "DELETE FROM catalogue_sync_content WHERE scope = ? AND metadata_hash = ?",
                [(scope, row["metadata_hash"]) for row in rows if row["metadata_hash"] not in keep],
            )
        total = 0
        for item, metadata_hash in zip(items, metadata_hashes, strict=True):
            renew_lease()
            with self.store._lock:
                cached = self.store._conn.execute(
                    "SELECT sha256, length(content) AS byte_count FROM catalogue_sync_content WHERE scope = ? AND metadata_hash = ?",
                    (scope, metadata_hash),
                ).fetchone()
            if cached is None:
                version = self.store.get_object_version(item["local_version_id"], include_yaml=True)
                content = wire_bytes({**item, "yaml": version["yaml"]})
                size = len(content)
                if size > CATALOGUE_OBJECT_MAX_BYTES:
                    raise SyncPayloadError(
                        "catalogue_object_too_large",
                        operation="catalogue_sync",
                        identifier=item["local_version_id"],
                        actual_bytes=size,
                        limit_bytes=CATALOGUE_OBJECT_MAX_BYTES,
                    )
                digest = hashlib.sha256(content).hexdigest()
            else:
                digest, size = cached["sha256"], cached["byte_count"]
            total += size
            if total > CATALOGUE_TOTAL_MAX_BYTES:
                raise SyncPayloadError(
                    "catalogue_snapshot_too_large",
                    operation="catalogue_sync",
                    identifier=snapshot_hash,
                    actual_bytes=total,
                    limit_bytes=CATALOGUE_TOTAL_MAX_BYTES,
                )
            if cached is None:
                with self.store._lock, self.store._conn:
                    self.store._conn.execute(
                        "INSERT OR REPLACE INTO catalogue_sync_content VALUES (?, ?, ?, ?)",
                        (scope, metadata_hash, digest, content),
                    )
            objects.append(
                {
                    "sha256": digest,
                    "byte_count": size,
                    "part_count": (size + CATALOGUE_PART_BYTES - 1) // CATALOGUE_PART_BYTES,
                }
            )
        state = {
            "snapshot_id": uuid4().hex,
            "snapshot_hash": snapshot_hash,
            "sequence": int(previous.get("sequence", 0)) + 1,
            "connection_id": context,
            "objects": objects,
            "registration_intents": [
                [item["local_version_id"], item["registration_id"]] for item in items if item.get("registration_id")
            ],
            "state": "pending",
            "uploaded_parts": 0,
            "next_parts": {},
        }
        self._write(scope, state)
        with self.store._lock, self.store._conn:
            rows = self.store._conn.execute(
                "SELECT metadata_hash FROM catalogue_sync_content WHERE scope = ?", (scope,)
            ).fetchall()
            keep = set(metadata_hashes)
            self.store._conn.executemany(
                "DELETE FROM catalogue_sync_content WHERE scope = ? AND metadata_hash = ?",
                [(scope, row["metadata_hash"]) for row in rows if row["metadata_hash"] not in keep],
            )
        return state
