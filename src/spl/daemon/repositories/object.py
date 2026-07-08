"""ObjectRepository aggregate storage."""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from pathlib import Path
from typing import Any, Callable, cast
from uuid import uuid4

from spl.daemon.canonical import canonical_object_definition, canonicalize
from spl.daemon.runtime_config import normalize_runtime_config
from spl.daemon.storage_base import (
    DEFAULT_OBJECT_LIBRARY,
    DEFAULT_OBJECT_OWNER_ID,
    RepositoryBase,
    json_dumps,
    json_loads,
    read_json,
    utc_now,
    validate_name,
)

LOGGER = logging.getLogger(__name__)


class ObjectRepository(RepositoryBase):
    """Persist and query object aggregate records."""

    def register_object(
        self,
        name: str,
        entrypoint: str,
        env: str,
        *,
        yaml_text: str | None = None,
        yaml_path: str | None = None,
        workdir: str | None = None,
        runtime_config: dict[str, Any] | None = None,
        description: str | None = None,
        version_label: str | None = None,
        object_id: str | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        origin: str = "local",
        remote_owner_id: str | None = None,
        remote_object_id: str | None = None,
        remote_version_id: str | None = None,
        source_object_name: str | None = None,
        remote_name: str | None = None,
        remote_signature_resolver: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Register a new immutable version of a function or pipeline."""

        name = validate_name(name)
        origin = validate_name(origin)
        if object_id is not None:
            object_id = validate_name(object_id)
        if remote_object_id is not None:
            remote_object_id = validate_name(remote_object_id)
        if remote_version_id is not None:
            remote_version_id = validate_name(remote_version_id)
        if owner_id is None and origin == "server" and remote_owner_id is not None:
            owner_id = remote_owner_id
        owner_id = self._caller_owner_id(owner_id)
        library = self._object_library(library)
        source_object_name = source_object_name if source_object_name is not None else remote_name
        if source_object_name is not None:
            source_object_name = validate_name(str(source_object_name))
        entrypoint = validate_name(entrypoint)
        env_record = self.get_env(env)
        normalized_runtime_config = normalize_runtime_config(runtime_config)

        if (yaml_text is None) == (yaml_path is None):
            raise ValueError("provide exactly one of yaml_text or yaml_path")
        if yaml_text is None:
            source_path = Path(str(yaml_path)).expanduser().absolute()
            if not source_path.exists():
                raise ValueError(f"SPL YAML file is not found: {source_path}")
            yaml_text = source_path.read_text(encoding="utf-8")

        try:
            from spl.daemon.metadata import extract_metadata

            metadata = extract_metadata(
                yaml_text,
                entrypoint,
                remote_signature_resolver=remote_signature_resolver,
            )
        except ModuleNotFoundError as exc:
            if exc.name == "yaml":
                raise ValueError("PyYAML is required to register SPL/YAML objects") from exc
            raise
        except KeyError as exc:
            raise ValueError(str(exc)) from exc
        self._validate_runtime_config_for_metadata(
            normalized_runtime_config,
            metadata,
        )
        if metadata["kind"] == "pipeline":
            from spl.core.adapter_compat import warn_yaml_adapter_compatibility

            warn_yaml_adapter_compatibility(yaml_text, entrypoint)

        resolved_workdir = None
        if workdir is not None:
            resolved_workdir = str(Path(workdir).expanduser().absolute())

        now = utc_now()
        resolved_object_id = object_id or uuid4().hex
        version_id = uuid4().hex
        object_description = description or ""
        yaml_sha256 = hashlib.sha256(yaml_text.encode("utf-8")).hexdigest()
        canonical_definition = canonical_object_definition(
            yaml_text=yaml_text,
            entrypoint=entrypoint,
            env=env,
            env_python_version=self._cached_python_version(env_record["python"]),
            metadata=metadata,
            runtime_config=normalized_runtime_config,
        )
        content_hash = hashlib.sha256(canonicalize(canonical_definition)).hexdigest()
        object_kind = validate_name(str(metadata["kind"]))

        with self._lock, self._conn:
            if remote_version_id is not None:
                existing_remote = self._conn.execute(
                    """
                    SELECT id
                    FROM object_versions
                    WHERE remote_version_id = ?
                    """,
                    (remote_version_id,),
                ).fetchone()
                if existing_remote is not None:
                    version_id = existing_remote["id"]
                    return self.get_object_version(
                        version_id,
                        include_yaml=False,
                    )

            if object_id is None:
                object_row = None
                if remote_object_id is not None:
                    object_row = self._conn.execute(
                        """
                        SELECT id, owner_id, library, name, kind, created_at
                        FROM objects
                        WHERE remote_object_id = ?
                        """,
                        (remote_object_id,),
                    ).fetchone()
                if object_row is None:
                    object_row = self._conn.execute(
                        """
                        SELECT id, owner_id, library, name, kind, created_at
                        FROM objects
                        WHERE owner_id = ? AND library = ? AND name = ?
                        """,
                        (owner_id, library, name),
                    ).fetchone()
            else:
                object_row = self._conn.execute(
                    """
                    SELECT id, owner_id, library, name, kind, created_at
                    FROM objects
                    WHERE id = ?
                    """,
                    (object_id,),
                ).fetchone()
                if object_row is None:
                    raise KeyError(f"object is not registered: {object_id}")
                if object_row["owner_id"] != owner_id or object_row["library"] != library or object_row["name"] != name:
                    raise ValueError(
                        "object_id points to object "
                        f"{object_row['owner_id']}/{object_row['library']}/"
                        f"{object_row['name']!r}, not "
                        f"{owner_id}/{library}/{name!r}"
                    )
            if object_row is not None and object_row["kind"] is not None and object_row["kind"] != object_kind:
                raise ValueError(
                    f"object kind is stable and cannot change from {object_row['kind']!r} to {object_kind!r}"
                )

            self._validate_object_decomposition_metadata(metadata)

            if object_row is None:
                self._conn.execute(
                    """
                    INSERT INTO objects(
                        id, owner_id, library, name, kind, origin,
                        remote_owner_id, remote_object_id, source_object_name,
                        description, current_version_id,
                        created_at, updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                    """,
                    (
                        resolved_object_id,
                        owner_id,
                        library,
                        name,
                        object_kind,
                        origin,
                        remote_owner_id,
                        remote_object_id,
                        source_object_name,
                        object_description,
                        now,
                        now,
                    ),
                )
                next_version = 1
            else:
                resolved_object_id = object_row["id"]
                row = self._conn.execute(
                    """
                    SELECT COALESCE(MAX(version), 0) + 1 AS next_version
                    FROM object_versions
                    WHERE object_id = ?
                    """,
                    (resolved_object_id,),
                ).fetchone()
                next_version = int(row["next_version"])

            existing_content = self._conn.execute(
                """
                SELECT id, remote_version_id
                FROM object_versions
                WHERE object_id = ? AND content_hash = ?
                """,
                (resolved_object_id, content_hash),
            ).fetchone()
            if existing_content is not None:
                version_id = existing_content["id"]
                existing_remote_version_id = cast(str | None, existing_content["remote_version_id"])
                if (
                    existing_remote_version_id is not None
                    and remote_version_id is not None
                    and existing_remote_version_id != remote_version_id
                ):
                    self._warn_remote_version_id_collision(
                        object_id=resolved_object_id,
                        content_hash=content_hash,
                        existing_remote_version_id=existing_remote_version_id,
                        incoming_remote_version_id=remote_version_id,
                    )
                self._conn.execute(
                    """
                    UPDATE object_versions
                    SET remote_owner_id = COALESCE(?, remote_owner_id),
                        remote_object_id = COALESCE(?, remote_object_id),
                        remote_version_id = CASE
                            WHEN remote_version_id IS NULL AND ? IS NOT NULL THEN ?
                            ELSE remote_version_id
                        END
                    WHERE id = ?
                    """,
                    (
                        remote_owner_id,
                        remote_object_id,
                        remote_version_id,
                        remote_version_id,
                        version_id,
                    ),
                )
                self._conn.execute(
                    """
                    UPDATE objects
                    SET description = ?,
                        kind = COALESCE(kind, ?),
                        current_version_id = ?,
                        -- Local authorship is sticky: republishing your own
                        -- object never silently demotes it to a mirror, while
                        -- a mirror the caller republishes becomes local.
                        origin = CASE WHEN origin = 'local' THEN origin ELSE ? END,
                        remote_owner_id = COALESCE(?, remote_owner_id),
                        remote_object_id = COALESCE(?, remote_object_id),
                        source_object_name = COALESCE(?, source_object_name),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        object_description,
                        object_kind,
                        version_id,
                        origin,
                        remote_owner_id,
                        remote_object_id,
                        source_object_name,
                        now,
                        resolved_object_id,
                    ),
                )
                return self.get_object_version(version_id, include_yaml=False)

            self._conn.execute(
                """
                INSERT INTO object_versions(
                    id, object_id, version, version_label, description,
                    entrypoint, env, env_python, kind, yaml_text, yaml_sha256,
                    content_hash, metadata_json, inputs_json, outputs_json,
                    pipeline_nodes_json, distributions_json, runtime_config_json, workdir,
                    remote_owner_id, remote_object_id, remote_version_id,
                    created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    resolved_object_id,
                    next_version,
                    version_label,
                    object_description,
                    entrypoint,
                    env,
                    env_record["python"],
                    metadata["kind"],
                    yaml_text,
                    yaml_sha256,
                    content_hash,
                    json_dumps(metadata),
                    json_dumps(metadata["inputs"]),
                    json_dumps(metadata["outputs"]),
                    json_dumps(metadata["pipeline_nodes"]),
                    json_dumps(metadata["distributions"]),
                    json_dumps(normalized_runtime_config),
                    resolved_workdir,
                    remote_owner_id,
                    remote_object_id,
                    remote_version_id,
                    now,
                ),
            )
            self._store_object_decomposition_locked(
                object_id=resolved_object_id,
                object_version_id=version_id,
                metadata=metadata,
                created_at=now,
            )
            self._conn.execute(
                """
                UPDATE objects
                SET description = ?,
                    kind = COALESCE(kind, ?),
                    current_version_id = ?,
                    -- Local authorship is sticky (see the dedup path above).
                    origin = CASE WHEN origin = 'local' THEN origin ELSE ? END,
                    remote_owner_id = COALESCE(?, remote_owner_id),
                    remote_object_id = COALESCE(?, remote_object_id),
                    source_object_name = COALESCE(?, source_object_name),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    object_description,
                    object_kind,
                    version_id,
                    origin,
                    remote_owner_id,
                    remote_object_id,
                    source_object_name,
                    now,
                    resolved_object_id,
                ),
            )

        # A YAML cache keeps older clients that display ``yaml_path`` useful.
        # The database remains the source of truth: workers materialize YAML from
        # SQLite into each run directory before executing.
        yaml_cache_path = self._object_yaml_cache_path(
            object_row["owner_id"] if object_row is not None else owner_id,
            object_row["library"] if object_row is not None else library,
            object_row["name"] if object_row is not None else name,
            next_version,
        )
        yaml_cache_path.parent.mkdir(parents=True, exist_ok=True)
        yaml_cache_path.write_text(yaml_text, encoding="utf-8")

        return self.get_object_version(version_id, include_yaml=False)

    @staticmethod
    def _warn_remote_version_id_collision(
        *,
        object_id: str,
        content_hash: str,
        existing_remote_version_id: str,
        incoming_remote_version_id: str,
    ) -> None:
        payload = {
            "event": "remote_version_id_collision",
            "object_id": object_id,
            "content_hash": content_hash,
            "existing_remote_version_id": existing_remote_version_id,
            "incoming_remote_version_id": incoming_remote_version_id,
        }
        LOGGER.warning(
            "remote_version_id collision in object version dedup: "
            "object_id=%s content_hash=%s existing_remote_version_id=%s incoming_remote_version_id=%s",
            object_id,
            content_hash,
            existing_remote_version_id,
            incoming_remote_version_id,
            extra={
                "spl_event": "remote_version_id_collision",
                "remote_version_id_collision": payload,
            },
        )

    def list_objects(self) -> dict[str, Any]:
        """Return current object versions keyed by registry name."""

        with self._lock:
            rows = self._conn.execute(self._object_select_sql()).fetchall()
            conflicts = self._object_conflicts_by_canonical_locked()
        records = [self._object_row_to_record(row, include_yaml=False) for row in rows]
        for record in records:
            record["conflicts"] = conflicts.get(record["canonical_name"], [])
        result: dict[str, Any] = {}
        for record in records:
            key = record["name"]
            if key in result:
                existing = result.pop(key)
                result[existing["canonical_name"]] = existing
                result[record["canonical_name"]] = record
                continue
            result[key] = record
        return result

    def search_objects(self, query: str) -> list[dict[str, Any]]:
        """Search current objects by name, description, and indexed metadata."""

        needle = query.strip().casefold()
        records = list(self.list_objects().values())
        if not needle:
            return records

        result = []
        for record in records:
            haystack = json_dumps(
                {
                    "name": record["name"],
                    "description": record["description"],
                    "entrypoint": record["entrypoint"],
                    "kind": record["kind"],
                    "inputs": record["inputs"],
                    "outputs": record["outputs"],
                    "pipeline_nodes": record["pipeline_nodes"],
                    "internal_objects": record["internal_objects"],
                }
            ).casefold()
            if needle in haystack:
                result.append(record)
        return result

    def list_object_identities(
        self,
        *,
        owner_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return object rows without joining versions."""

        args: tuple[Any, ...] = ()
        where = ""
        if owner_id is not None:
            where = "WHERE owner_id = ?"
            args = (validate_name(str(owner_id)),)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT id, owner_id, library, name, kind, origin,
                       remote_owner_id, remote_object_id, source_object_name,
                       current_version_id, created_at, updated_at
                FROM objects
                {where}
                ORDER BY owner_id, library, name
                """,
                args,
            ).fetchall()
        return [
            {
                "id": row["id"],
                "owner_id": row["owner_id"],
                "library": row["library"],
                "name": row["name"],
                "canonical_name": f"{row['owner_id']}/{row['library']}/{row['name']}",
                "kind": row["kind"],
                "origin": row["origin"],
                "remote_owner_id": row["remote_owner_id"],
                "remote_object_id": row["remote_object_id"],
                "source_object_name": row["source_object_name"],
                "current_version_id": row["current_version_id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def rekey_local_placeholder_objects(
        self,
        owner_id: str,
    ) -> dict[str, Any]:
        """Move local-placeholder objects into the connected owner's namespace."""

        owner_id = validate_name(str(owner_id))
        if owner_id == DEFAULT_OBJECT_OWNER_ID:
            return {"owner_id": owner_id, "rekeyed": [], "merged": [], "conflicts": []}
        report: dict[str, Any] = {
            "owner_id": owner_id,
            "rekeyed": [],
            "merged": [],
            "conflicts": [],
        }
        with self._lock, self._conn:
            rows = self._conn.execute(
                """
                SELECT *
                FROM objects
                WHERE owner_id = ?
                ORDER BY library, name, id
                """,
                (DEFAULT_OBJECT_OWNER_ID,),
            ).fetchall()
            for row in rows:
                existing = self._conn.execute(
                    """
                    SELECT *
                    FROM objects
                    WHERE owner_id = ? AND library = ? AND name = ?
                    """,
                    (owner_id, row["library"], row["name"]),
                ).fetchone()
                if existing is not None and existing["id"] != row["id"]:
                    if existing["kind"] is not None and row["kind"] is not None and existing["kind"] != row["kind"]:
                        conflict = {
                            "canonical_name": f"{owner_id}/{row['library']}/{row['name']}",
                            "reason": "kind_mismatch_on_rekey",
                            "local_object_id": row["id"],
                            "existing_object_id": existing["id"],
                            "local_kind": row["kind"],
                            "existing_kind": existing["kind"],
                        }
                        self._enqueue_sync_event_once_locked(
                            "object_conflict",
                            conflict,
                            dedupe_key=conflict["canonical_name"] + ":kind_mismatch",
                        )
                        report["conflicts"].append(conflict)
                        continue
                    merge = self._merge_object_rows_locked(row["id"], existing["id"])
                    report["merged"].append(
                        {
                            "target_id": row["id"],
                            "source_id": existing["id"],
                            "owner_id": owner_id,
                            "library": row["library"],
                            "name": row["name"],
                            **merge,
                        }
                    )
                self._conn.execute(
                    """
                    UPDATE objects
                    SET owner_id = ?,
                        remote_owner_id = COALESCE(remote_owner_id, ?),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (owner_id, owner_id, utc_now(), row["id"]),
                )
                report["rekeyed"].append(
                    {
                        "id": row["id"],
                        "owner_id": owner_id,
                        "library": row["library"],
                        "name": row["name"],
                    }
                )
        return report

    def link_object_remote_identity(
        self,
        *,
        owner_id: str,
        library: str,
        name: str,
        remote_owner_id: str | None = None,
        remote_object_id: str | None = None,
        source_object_name: str | None = None,
    ) -> dict[str, Any] | None:
        owner_id = validate_name(str(owner_id))
        library = validate_name(str(library))
        name = validate_name(str(name))
        if remote_owner_id is not None:
            remote_owner_id = validate_name(str(remote_owner_id))
        if remote_object_id is not None:
            remote_object_id = validate_name(str(remote_object_id))
        if source_object_name is not None:
            source_object_name = validate_name(str(source_object_name))
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT id
                FROM objects
                WHERE owner_id = ? AND library = ? AND name = ?
                """,
                (owner_id, library, name),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                """
                UPDATE objects
                SET remote_owner_id = COALESCE(?, remote_owner_id),
                    remote_object_id = COALESCE(?, remote_object_id),
                    source_object_name = COALESCE(?, source_object_name),
                    updated_at = ?
                WHERE id = ?
                """,
                (remote_owner_id, remote_object_id, source_object_name, utc_now(), row["id"]),
            )
        return self.get_object(name, owner_id=owner_id, library=library)

    def enqueue_object_version_sync_once(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        source_version_id = str(payload.get("source_version_id") or "")
        dedupe_key = f"object_version:{source_version_id}" if source_version_id else None
        with self._lock, self._conn:
            return self._enqueue_sync_event_once_locked(
                "object_version",
                payload,
                dedupe_key=dedupe_key,
            )

    def record_object_conflict_once(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        canonical_name = str(payload.get("canonical_name") or "")
        reason = str(payload.get("reason") or "conflict")
        local_hash = str(payload.get("local_content_hash") or "")
        remote_hash = str(payload.get("remote_content_hash") or "")
        dedupe_key = f"{canonical_name}:{reason}:{local_hash}:{remote_hash}"
        with self._lock, self._conn:
            return self._enqueue_sync_event_once_locked(
                "object_conflict",
                payload,
                dedupe_key=dedupe_key,
            )

    def _enqueue_sync_event_once_locked(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        dedupe_key: str | None,
    ) -> dict[str, Any]:
        kind = validate_name(kind)
        if dedupe_key:
            existing_rows = self._conn.execute(
                """
                SELECT *
                FROM sync_events
                WHERE kind = ? AND status IN ('pending', 'failed')
                ORDER BY created_at
                """,
                (kind,),
            ).fetchall()
            for row in existing_rows:
                existing_payload = json_loads(row["payload_json"], {})
                if existing_payload.get("dedupe_key") == dedupe_key:
                    return self._sync_event_row_from_row(row)
        event_id = uuid4().hex
        now = utc_now()
        event_payload = dict(payload)
        if dedupe_key:
            event_payload["dedupe_key"] = dedupe_key
        self._conn.execute(
            """
            INSERT INTO sync_events(
                id, kind, payload_json, status, attempts, created_at, updated_at
            )
            VALUES(?, ?, ?, 'pending', 0, ?, ?)
            """,
            (event_id, kind, json_dumps(event_payload), now, now),
        )
        row = self._conn.execute(
            "SELECT * FROM sync_events WHERE id = ?",
            (event_id,),
        ).fetchone()
        return self._sync_event_row_from_row(row)

    def _sync_event_row_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
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

    def _object_conflicts_by_canonical_locked(self) -> dict[str, list[dict[str, Any]]]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM sync_events
            WHERE kind = 'object_conflict'
              AND status IN ('pending', 'failed')
            ORDER BY created_at
            """
        ).fetchall()
        conflicts: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            payload = json_loads(row["payload_json"], {})
            canonical_name = payload.get("canonical_name")
            if not canonical_name:
                owner_id = payload.get("owner_id")
                library = payload.get("library")
                name = payload.get("name")
                if owner_id and library and name:
                    canonical_name = f"{owner_id}/{library}/{name}"
            if not canonical_name:
                continue
            conflicts.setdefault(str(canonical_name), []).append(
                {
                    "id": row["id"],
                    "status": row["status"],
                    "error": row["error"],
                    **payload,
                }
            )
        return conflicts

    def get_object_decomposition(self, object_version_id: str) -> dict[str, Any]:
        """Return normalized Function/Node/Link rows for one object version."""

        object_version_id = validate_name(object_version_id)
        with self._lock:
            return self._object_decomposition_locked(object_version_id)

    def _store_object_decomposition_locked(
        self,
        *,
        object_id: str,
        object_version_id: str,
        metadata: dict[str, Any],
        created_at: str,
    ) -> None:
        self._conn.execute(
            "DELETE FROM object_functions WHERE object_version_id = ?",
            (object_version_id,),
        )
        self._conn.execute(
            "DELETE FROM object_pipeline_nodes WHERE object_version_id = ?",
            (object_version_id,),
        )
        self._conn.execute(
            "DELETE FROM object_pipeline_links WHERE object_version_id = ?",
            (object_version_id,),
        )
        for item in self._function_decomposition_items(metadata):
            self._conn.execute(
                """
                INSERT INTO object_functions(
                    id, object_id, object_version_id, role, node_id, name,
                    inputs_json, outputs_json, metadata_json, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid4().hex,
                    object_id,
                    object_version_id,
                    item["role"],
                    item.get("node_id"),
                    item["name"],
                    json_dumps(item.get("inputs") or []),
                    json_dumps(item.get("outputs") or []),
                    json_dumps(item.get("metadata") or {}),
                    created_at,
                ),
            )
        for node in metadata.get("pipeline_nodes") or []:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("id") or node.get("node_id") or "")
            if not node_id:
                continue
            self._conn.execute(
                """
                INSERT INTO object_pipeline_nodes(
                    id, object_id, object_version_id, node_id, node_kind, name,
                    function_name, remote_json, inputs_json, outputs_json,
                    metadata_json, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid4().hex,
                    object_id,
                    object_version_id,
                    node_id,
                    str(node.get("kind") or "unknown"),
                    str(node.get("name") or node.get("function") or node_id),
                    node.get("function"),
                    json_dumps(node.get("remote") or {}),
                    json_dumps(node.get("inputs") or []),
                    json_dumps(node.get("outputs") or []),
                    json_dumps(node),
                    created_at,
                ),
            )
        for link in metadata.get("links") or []:
            if not isinstance(link, dict):
                continue
            target = link.get("from") or {}
            source = link.get("to") or {}
            target_node_id = str(target.get("node_id") or "")
            target_port = str(target.get("port") or "")
            if not target_node_id or not target_port:
                continue
            self._conn.execute(
                """
                INSERT INTO object_pipeline_links(
                    id, object_id, object_version_id, target_node_id,
                    target_port, source_kind, source_node_id, source_port,
                    scalar_json, link_json, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid4().hex,
                    object_id,
                    object_version_id,
                    target_node_id,
                    target_port,
                    str(source.get("kind") or "unknown"),
                    source.get("node_id"),
                    source.get("port"),
                    json_dumps(source.get("value")) if "value" in source else None,
                    json_dumps(link),
                    created_at,
                ),
            )

    def _validate_object_decomposition_metadata(self, metadata: dict[str, Any]) -> None:
        kind = str(metadata.get("kind") or "")
        nodes = metadata.get("pipeline_nodes") or []
        links = metadata.get("links") or []

        if kind not in {"function", "pipeline"}:
            raise ValueError("object decomposition kind must be 'function' or 'pipeline'")
        if not isinstance(nodes, list):
            raise ValueError("pipeline_nodes must be a list")
        if not isinstance(links, list):
            raise ValueError("links must be a list")

        if kind == "function":
            if nodes:
                raise ValueError("function decomposition cannot contain pipeline nodes")
            if links:
                raise ValueError("function decomposition cannot contain pipeline links")
            return

        if not nodes:
            raise ValueError("pipeline decomposition requires at least one node")

        node_by_id: dict[str, dict[str, Any]] = {}
        for index, node in enumerate(nodes, start=1):
            if not isinstance(node, dict):
                raise ValueError(f"pipeline node #{index} must be an object")
            node_id = str(node.get("id") or node.get("node_id") or "")
            if not node_id:
                raise ValueError(f"pipeline node #{index} is missing id")
            if node_id in node_by_id:
                raise ValueError(f"pipeline node id is duplicated: {node_id}")
            node_kind = str(node.get("kind") or "")
            if node_kind not in {"function", "remote"}:
                raise ValueError(f"pipeline node kind must be 'function' or 'remote': {node_id}")
            if node_kind == "function" and not str(node.get("function") or node.get("name") or ""):
                raise ValueError(f"pipeline function node is missing function: {node_id}")
            if node_kind == "remote" and not (isinstance(node.get("remote"), dict) or node.get("name")):
                raise ValueError(f"pipeline remote node is missing remote ref: {node_id}")
            node_by_id[node_id] = node

        for index, link in enumerate(links, start=1):
            if not isinstance(link, dict):
                raise ValueError(f"pipeline link #{index} must be an object")
            target = link.get("from")
            source = link.get("to")
            if not isinstance(target, dict):
                raise ValueError(f"pipeline link #{index} target must be an object")
            if not isinstance(source, dict):
                raise ValueError(f"pipeline link #{index} source must be an object")

            target_node_id = str(target.get("node_id") or "")
            target_port = str(target.get("port") or "")
            if not target_node_id:
                raise ValueError("pipeline link target node is not defined")
            if target_node_id not in node_by_id:
                raise ValueError(f"pipeline link target node is not defined: {target_node_id}")
            if not target_port:
                raise ValueError(f"pipeline link target port is not defined: {target_node_id}")

            source_kind = str(source.get("kind") or "")
            if source_kind == "node_output":
                source_node_id = str(source.get("node_id") or "")
                source_port = str(source.get("port") or "")
                if not source_node_id:
                    raise ValueError("pipeline link source node is not defined")
                if source_node_id not in node_by_id:
                    raise ValueError(f"pipeline link source node is not defined: {source_node_id}")
                if not source_port:
                    raise ValueError(f"pipeline link source port is not defined: {source_node_id}")
            elif source_kind == "scalar":
                if "value" not in source:
                    raise ValueError("pipeline scalar link source is missing value")
            elif not source_kind:
                raise ValueError("pipeline link source kind is not defined")

    def _validate_runtime_config_for_metadata(
        self,
        runtime_config: dict[str, Any],
        metadata: dict[str, Any],
    ) -> None:
        if runtime_config.get("mode") != "docker":
            return
        has_remote_nodes = any(node.get("kind") == "remote" for node in metadata.get("pipeline_nodes") or [])
        if has_remote_nodes and runtime_config.get("network") == "none":
            raise ValueError(
                "docker runtime network='none' is incompatible with remote "
                "pipeline nodes; use network='auto' or network='enabled'"
            )

    def _function_decomposition_items(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        if metadata.get("kind") == "function":
            return [
                {
                    "role": "top_level",
                    "node_id": None,
                    "name": str(metadata.get("entrypoint") or "function"),
                    "inputs": metadata.get("inputs") or [],
                    "outputs": metadata.get("outputs") or [],
                    "metadata": metadata,
                }
            ]
        items = []
        for node in metadata.get("pipeline_nodes") or []:
            if isinstance(node, dict) and node.get("kind") == "function":
                items.append(
                    {
                        "role": "pipeline_component",
                        "node_id": node.get("id") or node.get("node_id"),
                        "name": str(node.get("function") or node.get("name") or "function"),
                        "inputs": node.get("inputs") or [],
                        "outputs": node.get("outputs") or [],
                        "metadata": node,
                    }
                )
        return items

    def _object_decomposition_locked(self, object_version_id: str) -> dict[str, Any]:
        function_rows = self._conn.execute(
            """
            SELECT *
            FROM object_functions
            WHERE object_version_id = ?
            ORDER BY role, node_id, name
            """,
            (object_version_id,),
        ).fetchall()
        node_rows = self._conn.execute(
            """
            SELECT *
            FROM object_pipeline_nodes
            WHERE object_version_id = ?
            ORDER BY node_id
            """,
            (object_version_id,),
        ).fetchall()
        link_rows = self._conn.execute(
            """
            SELECT *
            FROM object_pipeline_links
            WHERE object_version_id = ?
            ORDER BY target_node_id, target_port
            """,
            (object_version_id,),
        ).fetchall()
        return {
            "functions": [self._object_function_row(row) for row in function_rows],
            "nodes": [self._object_pipeline_node_row(row) for row in node_rows],
            "links": [self._object_pipeline_link_row(row) for row in link_rows],
        }

    def _backfill_object_decomposition_locked(self) -> None:
        rows = self._conn.execute(
            """
            SELECT ov.id, ov.object_id, ov.metadata_json, ov.created_at
            FROM object_versions ov
            LEFT JOIN object_functions f ON f.object_version_id = ov.id
            LEFT JOIN object_pipeline_nodes n ON n.object_version_id = ov.id
            WHERE f.id IS NULL AND n.id IS NULL
            """
        ).fetchall()
        for row in rows:
            self._store_object_decomposition_locked(
                object_id=row["object_id"],
                object_version_id=row["id"],
                metadata=json_loads(row["metadata_json"], {}),
                created_at=row["created_at"],
            )

    def _object_function_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "object_id": row["object_id"],
            "object_version_id": row["object_version_id"],
            "kind": "function",
            "role": row["role"],
            "node_id": row["node_id"],
            "name": row["name"],
            "inputs": json_loads(row["inputs_json"], []),
            "outputs": json_loads(row["outputs_json"], []),
            "metadata": json_loads(row["metadata_json"], {}),
            "created_at": row["created_at"],
        }

    def _object_pipeline_node_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "object_id": row["object_id"],
            "object_version_id": row["object_version_id"],
            "node_id": row["node_id"],
            "kind": row["node_kind"],
            "name": row["name"],
            "function": row["function_name"],
            "remote": json_loads(row["remote_json"], {}),
            "inputs": json_loads(row["inputs_json"], []),
            "outputs": json_loads(row["outputs_json"], []),
            "metadata": json_loads(row["metadata_json"], {}),
            "created_at": row["created_at"],
        }

    def _object_pipeline_link_row(self, row: sqlite3.Row) -> dict[str, Any]:
        source: dict[str, Any] = {"kind": row["source_kind"]}
        if row["source_node_id"] is not None:
            source["node_id"] = row["source_node_id"]
        if row["source_port"] is not None:
            source["port"] = row["source_port"]
        if row["scalar_json"] is not None:
            source["value"] = json_loads(row["scalar_json"], None)
        return {
            "id": row["id"],
            "object_id": row["object_id"],
            "object_version_id": row["object_version_id"],
            "target_node_id": row["target_node_id"],
            "target_port": row["target_port"],
            "source_kind": row["source_kind"],
            "source_node_id": row["source_node_id"],
            "source_port": row["source_port"],
            "source": source,
            "target": {
                "node_id": row["target_node_id"],
                "port": row["target_port"],
            },
            "raw": json_loads(row["link_json"], {}),
            "created_at": row["created_at"],
        }

    def get_object(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Return one object by local name or id."""

        with self._lock:
            rows = self._resolve_object_rows_locked(
                name_or_id,
                version=version,
                owner_id=owner_id,
                library=library,
            )
            same_name_libraries: list[str] = []
            if not rows and library is None:
                same_name_libraries = sorted(
                    {
                        str(item["library"])
                        for item in self._object_identity_rows_for_clause_locked(
                            "owner_id = ? AND name = ?",
                            (self._caller_owner_id(owner_id), name_or_id),
                        )
                    }
                )
        if not rows:
            suffix = f" version {version}" if version is not None else ""
            if len(same_name_libraries) > 1:
                libraries = ", ".join(same_name_libraries)
                raise KeyError(
                    f"object is not registered: {name_or_id}{suffix} "
                    f"(you have {name_or_id!r} in several libraries: {libraries}; "
                    "pass library=... to choose one)"
                )
            raise KeyError(f"object is not registered: {name_or_id}{suffix}")
        if len(rows) > 1:
            names = ", ".join(sorted(self._canonical_row_name(item) for item in rows))
            raise ValueError(f"object display name is ambiguous locally: {name_or_id}; use one of: {names}")
        [row] = rows
        return self._object_row_to_record(row, include_yaml=include_yaml)

    def get_object_version(
        self,
        version_id: str,
        *,
        include_yaml: bool = True,
    ) -> dict[str, Any]:
        """Return one immutable object version by internal version id."""

        version_id = validate_name(version_id)
        with self._lock:
            row = self._conn.execute(
                f"{self._object_select_sql(current_only=False)} WHERE ov.id = ?",
                (version_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"object version is not found: {version_id}")
        return self._object_row_to_record(row, include_yaml=include_yaml)

    def get_object_by_remote_version(
        self,
        remote_version_id: str,
        *,
        include_yaml: bool = False,
    ) -> dict[str, Any] | None:
        """Return a locally cached server object version, if present."""

        remote_version_id = validate_name(remote_version_id)
        with self._lock:
            row = self._conn.execute(
                f"""
                {self._object_select_sql(current_only=False)}
                WHERE ov.remote_version_id = ?
                """,
                (remote_version_id,),
            ).fetchone()
        if row is None:
            return None
        return self._object_row_to_record(row, include_yaml=include_yaml)

    def list_object_versions_by_remote_object(
        self,
        remote_object_id: str,
    ) -> list[dict[str, Any]]:
        """Return locally cached versions for one server object id."""

        remote_object_id = validate_name(remote_object_id)
        with self._lock:
            rows = self._conn.execute(
                f"""
                {self._object_select_sql(current_only=False)}
                WHERE o.remote_object_id = ? OR ov.remote_object_id = ?
                ORDER BY ov.version DESC
                """,
                (remote_object_id, remote_object_id),
            ).fetchall()
        return [self._object_row_to_record(row, include_yaml=False) for row in rows]

    def list_object_versions(
        self,
        name_or_id: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return all versions of one object, newest first."""

        with self._lock:
            current_rows = self._resolve_object_rows_locked(
                name_or_id,
                owner_id=owner_id,
                library=library,
            )
        if not current_rows:
            raise KeyError(f"object is not registered: {name_or_id}")
        object_ids = {row["object_id"] for row in current_rows}
        if len(object_ids) != 1:
            names = ", ".join(sorted({self._canonical_row_name(row) for row in current_rows}))
            raise ValueError(f"object display name is ambiguous locally: {name_or_id}; use one of: {names}")
        [object_id] = object_ids
        with self._lock:
            rows = self._conn.execute(
                f"""
                {self._object_select_sql(current_only=False)}
                WHERE o.id = ?
                ORDER BY ov.version DESC
                """,
                (object_id,),
            ).fetchall()
        return [self._object_row_to_record(row, include_yaml=False) for row in rows]

    def forget_object(
        self,
        name_or_id: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove one local object and all dependent local rows."""

        name_or_id = validate_name(str(name_or_id))
        with self._lock, self._conn:
            rows = self._resolve_object_identity_rows_locked(
                name_or_id,
                owner_id=owner_id,
                library=library,
            )
            if not rows:
                raise KeyError(f"object is not registered: {name_or_id}")
            if len(rows) > 1:
                names = ", ".join(sorted(self._canonical_object_identity_name(row) for row in rows))
                raise ValueError(f"object display name is ambiguous locally: {name_or_id}; use one of: {names}")
            [row] = rows
            return self._delete_object_identity_locked(row)

    def forget_object_version(
        self,
        name_or_id: str,
        version_ref: str | int,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove one local object version and its dependent local rows."""

        name_or_id = validate_name(str(name_or_id))
        with self._lock, self._conn:
            rows = self._resolve_object_identity_rows_locked(
                name_or_id,
                owner_id=owner_id,
                library=library,
            )
            if not rows:
                raise KeyError(f"object is not registered: {name_or_id}")
            if len(rows) > 1:
                names = ", ".join(sorted(self._canonical_object_identity_name(row) for row in rows))
                raise ValueError(f"object display name is ambiguous locally: {name_or_id}; use one of: {names}")
            [object_row] = rows
            version_row = self._object_version_row_for_ref_locked(
                object_row["id"],
                version_ref,
            )
            if version_row is None:
                raise KeyError(f"object version is not found: {name_or_id} version {version_ref}")

            object_info = self._object_identity_info(object_row)
            deleted = self._delete_single_object_version_locked(
                object_row["id"],
                version_row["id"],
            )
            remaining = self._conn.execute(
                """
                SELECT id
                FROM object_versions
                WHERE object_id = ?
                ORDER BY version DESC, created_at DESC, id DESC
                LIMIT 1
                """,
                (object_row["id"],),
            ).fetchone()
            object_deleted = remaining is None
            if object_deleted:
                deleted["sync_events"] += self._delete_object_sync_events_locked(
                    object_info,
                    {version_row["id"]},
                )
                object_delete = self._conn.execute(
                    "DELETE FROM objects WHERE id = ?",
                    (object_row["id"],),
                )
                deleted["objects"] = object_delete.rowcount
                current_version_id = None
            else:
                current_version_id = remaining["id"]
                if object_row["current_version_id"] == version_row["id"]:
                    self._conn.execute(
                        """
                        UPDATE objects
                        SET current_version_id = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (current_version_id, utc_now(), object_row["id"]),
                    )

            return {
                "forgotten": True,
                "object_deleted": object_deleted,
                "object": object_info,
                "version": {
                    "id": version_row["id"],
                    "version": version_row["version"],
                    "content_hash": version_row["content_hash"],
                },
                "current_version_id": current_version_id,
                "deleted": deleted,
            }

    def prune_stale_mirrors(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove locally cached server-origin mirror rows."""

        clauses = ["origin = 'server'"]
        args: list[Any] = []
        if owner_id is not None:
            clauses.append("owner_id = ?")
            args.append(validate_name(str(owner_id)))
        if library is not None:
            clauses.append("library = ?")
            args.append(self._object_library(library))
        with self._lock, self._conn:
            rows = self._conn.execute(
                f"""
                SELECT *
                FROM objects
                WHERE {" AND ".join(clauses)}
                ORDER BY owner_id, library, name, id
                """,
                tuple(args),
            ).fetchall()
            pruned = [self._delete_object_identity_locked(row) for row in rows]
        return {"pruned": pruned, "count": len(pruned)}

    def _resolve_object_rows_locked(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[sqlite3.Row]:
        id_rows = self._object_rows_for_clause_locked(
            "o.id = ?",
            (name_or_id,),
            version=version,
        )
        if id_rows:
            return id_rows

        explicit_scope = owner_id is not None or library is not None
        caller_owner_id = self._caller_owner_id(owner_id)

        if explicit_scope:
            if library is not None:
                return self._object_rows_for_clause_locked(
                    "o.owner_id = ? AND o.library = ? AND o.name = ?",
                    (caller_owner_id, self._object_library(library), name_or_id),
                    version=version,
                )
            return self._object_rows_for_clause_locked(
                "o.owner_id = ? AND o.name = ?",
                (caller_owner_id, name_or_id),
                version=version,
            )

        caller_library = self._object_library(None)
        own_library_rows = self._object_rows_for_clause_locked(
            "o.owner_id = ? AND o.library = ? AND o.name = ?",
            (caller_owner_id, caller_library, name_or_id),
            version=version,
        )
        if own_library_rows:
            return own_library_rows

        own_rows = self._object_rows_for_clause_locked(
            "o.owner_id = ? AND o.name = ?",
            (caller_owner_id, name_or_id),
            version=version,
        )
        if len(own_rows) == 1:
            return own_rows
        return []

    def _resolve_object_identity_rows_locked(
        self,
        name_or_id: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[sqlite3.Row]:
        id_rows = self._object_identity_rows_for_clause_locked(
            "id = ?",
            (name_or_id,),
        )
        if id_rows:
            return id_rows

        explicit_scope = owner_id is not None or library is not None
        caller_owner_id = self._caller_owner_id(owner_id)

        if explicit_scope:
            if library is not None:
                return self._object_identity_rows_for_clause_locked(
                    "owner_id = ? AND library = ? AND name = ?",
                    (caller_owner_id, self._object_library(library), name_or_id),
                )
            return self._object_identity_rows_for_clause_locked(
                "owner_id = ? AND name = ?",
                (caller_owner_id, name_or_id),
            )

        caller_library = self._object_library(None)
        own_library_rows = self._object_identity_rows_for_clause_locked(
            "owner_id = ? AND library = ? AND name = ?",
            (caller_owner_id, caller_library, name_or_id),
        )
        if own_library_rows:
            return own_library_rows

        own_rows = self._object_identity_rows_for_clause_locked(
            "owner_id = ? AND name = ?",
            (caller_owner_id, name_or_id),
        )
        if len(own_rows) == 1:
            return own_rows
        return []

    def _object_identity_rows_for_clause_locked(
        self,
        clause: str,
        args: tuple[Any, ...],
    ) -> list[sqlite3.Row]:
        return cast(
            list[sqlite3.Row],
            self._conn.execute(
                f"""
            SELECT *
            FROM objects
            WHERE {clause}
            ORDER BY owner_id, library, name, id
            """,
                args,
            ).fetchall(),
        )

    def _object_version_row_for_ref_locked(
        self,
        object_id: str,
        version_ref: str | int,
    ) -> sqlite3.Row | None:
        if isinstance(version_ref, int) or str(version_ref).isdigit():
            return cast(
                sqlite3.Row | None,
                self._conn.execute(
                    """
                SELECT id, version, content_hash
                FROM object_versions
                WHERE object_id = ? AND version = ?
                """,
                    (object_id, int(version_ref)),
                ).fetchone(),
            )
        version_id = validate_name(str(version_ref))
        return cast(
            sqlite3.Row | None,
            self._conn.execute(
                """
            SELECT id, version, content_hash
            FROM object_versions
            WHERE object_id = ? AND id = ?
            """,
                (object_id, version_id),
            ).fetchone(),
        )

    def _delete_object_identity_locked(self, row: sqlite3.Row) -> dict[str, Any]:
        object_info = self._object_identity_info(row)
        version_rows = self._conn.execute(
            """
            SELECT id, version, content_hash
            FROM object_versions
            WHERE object_id = ?
            ORDER BY version, id
            """,
            (row["id"],),
        ).fetchall()
        version_ids = {version["id"] for version in version_rows}
        deleted = self._delete_object_dependents_locked(
            row["id"],
            version_ids,
        )
        deleted["sync_events"] = self._delete_object_sync_events_locked(
            object_info,
            version_ids,
        )
        cursor = self._conn.execute("DELETE FROM objects WHERE id = ?", (row["id"],))
        deleted["objects"] = cursor.rowcount
        return {
            "forgotten": True,
            "object_deleted": True,
            "object": object_info,
            "versions": [
                {
                    "id": version["id"],
                    "version": version["version"],
                    "content_hash": version["content_hash"],
                }
                for version in version_rows
            ],
            "deleted": deleted,
        }

    def _delete_single_object_version_locked(
        self,
        object_id: str,
        version_id: str,
    ) -> dict[str, int]:
        deleted = self._delete_object_dependents_locked(
            object_id,
            {version_id},
            whole_object=False,
        )
        cursor = self._conn.execute(
            "DELETE FROM object_versions WHERE id = ?",
            (version_id,),
        )
        deleted["versions"] = cursor.rowcount
        deleted["sync_events"] = self._delete_object_sync_events_locked(
            None,
            {version_id},
        )
        return deleted

    def _delete_object_dependents_locked(
        self,
        object_id: str,
        version_ids: set[str],
        *,
        whole_object: bool = True,
    ) -> dict[str, int]:
        deleted: dict[str, int] = {
            "runs": 0,
            "functions": 0,
            "pipeline_nodes": 0,
            "pipeline_links": 0,
            "versions": 0,
            "sync_events": 0,
        }
        if version_ids:
            placeholders = ", ".join("?" for _ in version_ids)
            version_args = tuple(sorted(version_ids))
            run_clause = (
                f"object_id = ? OR object_version_id IN ({placeholders})"
                if whole_object
                else f"object_version_id IN ({placeholders})"
            )
            run_args = (object_id, *version_args) if whole_object else version_args
            run_cursor = self._conn.execute(
                f"DELETE FROM runs WHERE {run_clause}",
                run_args,
            )
            deleted["runs"] = run_cursor.rowcount
            for table, key in (
                ("object_functions", "functions"),
                ("object_pipeline_nodes", "pipeline_nodes"),
                ("object_pipeline_links", "pipeline_links"),
            ):
                row_clause = (
                    f"object_id = ? OR object_version_id IN ({placeholders})"
                    if whole_object
                    else f"object_version_id IN ({placeholders})"
                )
                row_args = (object_id, *version_args) if whole_object else version_args
                cursor = self._conn.execute(
                    f"""
                    DELETE FROM {validate_name(table)}
                    WHERE {row_clause}
                    """,
                    row_args,
                )
                deleted[key] = cursor.rowcount
            if whole_object:
                cursor = self._conn.execute(
                    "DELETE FROM object_versions WHERE object_id = ?",
                    (object_id,),
                )
                deleted["versions"] = cursor.rowcount
        elif whole_object:
            run_cursor = self._conn.execute(
                "DELETE FROM runs WHERE object_id = ?",
                (object_id,),
            )
            deleted["runs"] = run_cursor.rowcount
            for table, key in (
                ("object_functions", "functions"),
                ("object_pipeline_nodes", "pipeline_nodes"),
                ("object_pipeline_links", "pipeline_links"),
            ):
                cursor = self._conn.execute(
                    f"DELETE FROM {validate_name(table)} WHERE object_id = ?",
                    (object_id,),
                )
                deleted[key] = cursor.rowcount
        return deleted

    def _delete_object_sync_events_locked(
        self,
        object_info: dict[str, Any] | None,
        version_ids: set[str],
    ) -> int:
        object_id = object_info["id"] if object_info is not None else None
        canonical_name = object_info["canonical_name"] if object_info is not None else None
        event_rows = self._conn.execute(
            """
            SELECT id, kind, payload_json
            FROM sync_events
            WHERE status IN ('pending', 'failed')
            """
        ).fetchall()
        delete_ids: list[str] = []
        for event in event_rows:
            payload = json_loads(event["payload_json"], {})
            if self._sync_event_matches_object_or_versions(
                event["kind"],
                payload,
                object_id=object_id,
                canonical_name=canonical_name,
                version_ids=version_ids,
            ):
                delete_ids.append(event["id"])
        for event_id in delete_ids:
            self._conn.execute("DELETE FROM sync_events WHERE id = ?", (event_id,))
        return len(delete_ids)

    def _sync_event_matches_object_or_versions(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        object_id: str | None,
        canonical_name: str | None,
        version_ids: set[str],
    ) -> bool:
        if kind == "object_version":
            return (object_id is not None and payload.get("source_object_id") == object_id) or payload.get(
                "source_version_id"
            ) in version_ids
        if kind == "object_conflict":
            return (object_id is not None and payload.get("local_object_id") == object_id) or (
                canonical_name is not None and payload.get("canonical_name") == canonical_name
            )
        if kind == "local_run_update":
            run_payload = payload.get("run") or {}
            return (object_id is not None and run_payload.get("object_id") == object_id) or run_payload.get(
                "object_version_id"
            ) in version_ids
        return False

    def _object_identity_info(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "owner_id": row["owner_id"],
            "library": row["library"],
            "name": row["name"],
            "canonical_name": self._canonical_object_identity_name(row),
            "kind": row["kind"],
            "origin": row["origin"],
            "remote_owner_id": row["remote_owner_id"],
            "remote_object_id": row["remote_object_id"],
            "source_object_name": row["source_object_name"],
        }

    def _canonical_object_identity_name(self, row: sqlite3.Row) -> str:
        return f"{row['owner_id']}/{row['library']}/{row['name']}"

    def _object_rows_for_clause_locked(
        self,
        clause: str,
        args: tuple[Any, ...],
        *,
        version: int | None,
    ) -> list[sqlite3.Row]:
        sql = self._object_select_sql(current_only=version is None)
        version_filter = "" if version is None else " AND ov.version = ?"
        version_args: tuple[Any, ...] = () if version is None else (int(version),)
        return cast(
            list[sqlite3.Row],
            self._conn.execute(
                f"""
            {sql}
            WHERE {clause}{version_filter}
            """,
                (*args, *version_args),
            ).fetchall(),
        )

    def _caller_owner_id(self, owner_id: str | None = None) -> str:
        if owner_id is not None:
            return validate_name(str(owner_id))
        credentials = self.current_server_connection_credentials()
        if credentials is not None and credentials.get("remote_connection_id") and credentials.get("owner_id"):
            return validate_name(str(credentials["owner_id"]))
        return DEFAULT_OBJECT_OWNER_ID

    def _object_library(self, library: str | None = None) -> str:
        return validate_name(str(library or DEFAULT_OBJECT_LIBRARY))

    def _canonical_row_name(self, row: sqlite3.Row) -> str:
        return f"{row['object_owner_id']}/{row['object_library']}/{row['object_name']}"

    def _object_select_sql(self, *, current_only: bool = True) -> str:
        join_condition = "ov.id = o.current_version_id" if current_only else "ov.object_id = o.id"
        return f"""
            SELECT
                o.id AS object_id,
                o.owner_id AS object_owner_id,
                o.library AS object_library,
                o.name AS object_name,
                o.kind AS object_kind,
                o.origin AS object_origin,
                o.remote_owner_id AS object_remote_owner_id,
                o.remote_object_id AS object_remote_object_id,
                o.source_object_name AS object_source_object_name,
                o.description AS object_description,
                o.current_version_id AS current_version_id,
                o.created_at AS object_created_at,
                o.updated_at AS object_updated_at,
                ov.id AS version_id,
                ov.version AS version,
                ov.version_label AS version_label,
                ov.description AS version_description,
                ov.entrypoint AS entrypoint,
                ov.env AS env,
                ov.env_python AS env_python,
                COALESCE(o.kind, ov.kind) AS kind,
                ov.kind AS version_kind,
                ov.yaml_text AS yaml_text,
                ov.yaml_sha256 AS yaml_sha256,
                ov.content_hash AS content_hash,
                ov.metadata_json AS metadata_json,
                ov.inputs_json AS inputs_json,
                ov.outputs_json AS outputs_json,
                ov.pipeline_nodes_json AS pipeline_nodes_json,
                ov.distributions_json AS distributions_json,
                ov.runtime_config_json AS runtime_config_json,
                ov.workdir AS workdir,
                ov.remote_owner_id AS remote_owner_id,
                ov.remote_object_id AS remote_object_id,
                ov.remote_version_id AS remote_version_id,
                ov.created_at AS version_created_at
            FROM objects o
            JOIN object_versions ov ON {join_condition}
        """

    def _object_row_to_record(
        self,
        row: sqlite3.Row,
        *,
        include_yaml: bool,
    ) -> dict[str, Any]:
        metadata = json_loads(row["metadata_json"], {})
        decomposition = self.get_object_decomposition(row["version_id"])
        source_object_name = row["object_source_object_name"]
        source_owner_id = row["object_remote_owner_id"] or row["remote_owner_id"]
        source_object_id = row["object_remote_object_id"] or row["remote_object_id"]
        source_version_id = row["remote_version_id"]
        runtime_config = normalize_runtime_config(json_loads(row["runtime_config_json"], {"mode": "venv"}))
        distributions = json_loads(row["distributions_json"], [])
        env_python_version = self._cached_python_version(row["env_python"])
        environment_spec_hash = None
        if row["object_origin"] != "server":
            environment_spec_hash = self.environment_spec_hash_for(
                row["env_python"],
                distributions,
                python_version=env_python_version,
            )
        record = {
            "id": row["object_id"],
            "owner_id": row["object_owner_id"],
            "library": row["object_library"],
            "canonical_name": self._canonical_row_name(row),
            "name": row["object_name"],
            "local_registry_name": row["object_name"],
            "display_name": source_object_name or row["object_name"],
            "origin": row["object_origin"],
            "object_remote_owner_id": row["object_remote_owner_id"],
            "object_remote_object_id": row["object_remote_object_id"],
            "object_remote_name": source_object_name,
            "object_source_object_name": source_object_name,
            "remote_name": source_object_name,
            "source_owner_id": source_owner_id,
            "source_object_id": source_object_id,
            "source_object_name": source_object_name,
            "source_version_id": source_version_id,
            "remote_display_name": source_object_name,
            "remote_identity": {
                "origin": row["object_origin"],
                "local_registry_name": row["object_name"],
                "owner_id": row["object_owner_id"],
                "library": row["object_library"],
                "source_owner_id": source_owner_id,
                "source_object_id": source_object_id,
                "source_object_name": source_object_name,
                "source_version_id": source_version_id,
                "remote_display_name": source_object_name,
                "storage_remote_name": source_object_name,
            },
            "compatibility": {
                "remote_name": {
                    "status": "deprecated_alias",
                    "replacement": "source_object_name",
                    "storage_field": "objects.source_object_name",
                }
            },
            "description": row["version_description"] or row["object_description"] or "",
            "current_version_id": row["current_version_id"],
            "version_id": row["version_id"],
            "version": row["version"],
            "version_label": row["version_label"],
            "entrypoint": row["entrypoint"],
            "env": row["env"],
            "env_python": row["env_python"],
            "env_python_version": env_python_version,
            "authored_python_version": env_python_version,
            "object_kind": row["object_kind"] or row["kind"],
            "version_kind": row["version_kind"],
            "kind": row["kind"],
            "type": row["kind"],
            "yaml_path": str(
                self._object_yaml_cache_path(
                    row["object_owner_id"],
                    row["object_library"],
                    row["object_name"],
                    row["version"],
                )
            ),
            "yaml_sha256": row["yaml_sha256"],
            "content_hash": row["content_hash"],
            "workdir": row["workdir"],
            "runtime_config": runtime_config,
            "runtime_mode": runtime_config["mode"],
            "remote_owner_id": row["remote_owner_id"],
            "remote_object_id": row["remote_object_id"],
            "remote_version_id": row["remote_version_id"],
            "inputs": json_loads(row["inputs_json"], []),
            "outputs": json_loads(row["outputs_json"], []),
            "functions": decomposition["functions"],
            "pipeline_nodes": decomposition["nodes"] or json_loads(row["pipeline_nodes_json"], []),
            "pipeline_links": decomposition["links"],
            "links": decomposition["links"],
            "decomposition": decomposition,
            "internal_objects": metadata.get("internal_objects", []),
            "distributions": distributions,
            "environment_spec_hash": environment_spec_hash,
            "metadata": metadata,
            "created_at": row["object_created_at"],
            "updated_at": row["object_updated_at"],
            "version_created_at": row["version_created_at"],
        }
        if include_yaml:
            record["yaml"] = row["yaml_text"]
        return record

    def _object_yaml_cache_path(
        self,
        owner_id: str,
        library: str,
        name: str,
        version: int,
    ) -> Path:
        """Return the display-cache YAML path for one object version.

        The path is namespaced by ``owner/library`` so two same-named objects
        from different owners or libraries can never overwrite each other's
        cache file.  The database stays the source of truth; this file only
        keeps ``yaml_path`` displays useful.
        """

        return cast(
            Path,
            (
                self.objects_dir
                / validate_name(owner_id)
                / validate_name(library)
                / validate_name(name)
                / "versions"
                / f"{version}.yaml"
            ),
        )

    def _backfill_object_kinds_locked(self) -> None:
        self._conn.execute(
            """
            UPDATE objects
            SET kind = (
                SELECT ov.kind
                FROM object_versions ov
                WHERE ov.id = objects.current_version_id
            )
            WHERE kind IS NULL
              AND current_version_id IS NOT NULL
            """
        )

    def _migrate_legacy_registry(self) -> None:
        """Import the old JSON registry once when the SQLite registry is empty."""

        if not self.registry_path.exists():
            return

        env_count = self._conn.execute("SELECT COUNT(*) FROM envs").fetchone()[0]
        object_count = self._conn.execute("SELECT COUNT(*) FROM objects").fetchone()[0]
        if env_count or object_count:
            return

        registry = read_json(self.registry_path, {"envs": {}, "objects": {}})
        now = utc_now()
        for name, env in registry.get("envs", {}).items():
            self._conn.execute(
                """
                INSERT OR IGNORE INTO envs(name, python, created_at, updated_at)
                VALUES(?, ?, ?, ?)
                """,
                (
                    validate_name(name),
                    env["python"],
                    env.get("updated_at") or now,
                    env.get("updated_at") or now,
                ),
            )

        # Object migration is best-effort.  Bad legacy records should not prevent
        # a daemon with an empty SQLite database from starting.
        for name, record in registry.get("objects", {}).items():
            yaml_path = Path(record["yaml_path"])
            if not yaml_path.exists():
                continue
            try:
                self.register_object(
                    name,
                    record["entrypoint"],
                    record["env"],
                    yaml_text=yaml_path.read_text(encoding="utf-8"),
                    workdir=record.get("workdir"),
                    description=record.get("description") or "",
                )
            except Exception:
                continue
