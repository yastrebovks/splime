"""ObjectRepository aggregate storage."""

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
        origin: str = "local",
        remote_owner_id: str | None = None,
        remote_object_id: str | None = None,
        remote_version_id: str | None = None,
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
                raise ValueError(
                    "PyYAML is required to register SPL/YAML objects"
                ) from exc
            raise
        except KeyError as exc:
            raise ValueError(str(exc)) from exc
        self._validate_runtime_config_for_metadata(
            normalized_runtime_config,
            metadata,
        )

        resolved_workdir = None
        if workdir is not None:
            resolved_workdir = str(Path(workdir).expanduser().absolute())

        now = utc_now()
        resolved_object_id = object_id or uuid4().hex
        version_id = uuid4().hex
        object_description = description or ""
        yaml_sha256 = hashlib.sha256(yaml_text.encode("utf-8")).hexdigest()
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
                    return self.get_object_version(
                        existing_remote["id"],
                        include_yaml=False,
                    )

            if object_id is None and remote_object_id is not None:
                object_row = self._conn.execute(
                    """
                    SELECT id, name, kind, created_at
                    FROM objects
                    WHERE remote_object_id = ?
                    """,
                    (remote_object_id,),
                ).fetchone()
            elif object_id is None:
                object_row = self._conn.execute(
                    "SELECT id, name, kind, created_at FROM objects WHERE name = ?",
                    (name,),
                ).fetchone()
            else:
                object_row = self._conn.execute(
                    "SELECT id, name, kind, created_at FROM objects WHERE id = ?",
                    (object_id,),
                ).fetchone()
                if object_row is None:
                    raise KeyError(f"object is not registered: {object_id}")
                if object_row["name"] != name:
                    raise ValueError(
                        "object_id points to object "
                        f"{object_row['name']!r}, not {name!r}"
                    )
            if (
                object_row is not None
                and object_row["kind"] is not None
                and object_row["kind"] != object_kind
            ):
                raise ValueError(
                    "object kind is stable and cannot change from "
                    f"{object_row['kind']!r} to {object_kind!r}"
                )

            self._validate_object_decomposition_metadata(metadata)

            if object_row is None:
                self._conn.execute(
                    """
                    INSERT INTO objects(
                        id, name, kind, origin, remote_owner_id, remote_object_id,
                        remote_name, description, current_version_id,
                        created_at, updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                    """,
                    (
                        resolved_object_id,
                        name,
                        object_kind,
                        origin,
                        remote_owner_id,
                        remote_object_id,
                        remote_name,
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

            self._conn.execute(
                """
                INSERT INTO object_versions(
                    id, object_id, version, version_label, description,
                    entrypoint, env, env_python, kind, yaml_text, yaml_sha256,
                    metadata_json, inputs_json, outputs_json,
                    pipeline_nodes_json, distributions_json, runtime_config_json, workdir,
                    remote_owner_id, remote_object_id, remote_version_id,
                    created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    origin = ?,
                    remote_owner_id = COALESCE(?, remote_owner_id),
                    remote_object_id = COALESCE(?, remote_object_id),
                    remote_name = COALESCE(?, remote_name),
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
                    remote_name,
                    now,
                    resolved_object_id,
                ),
            )

        # A YAML cache keeps older clients that display ``yaml_path`` useful.
        # The database remains the source of truth: workers materialize YAML from
        # SQLite into each run directory before executing.
        yaml_cache_path = self._object_yaml_cache_path(name, next_version)
        yaml_cache_path.parent.mkdir(parents=True, exist_ok=True)
        yaml_cache_path.write_text(yaml_text, encoding="utf-8")

        return self.get_object_version(version_id, include_yaml=False)

    def list_objects(self) -> dict[str, Any]:
        """Return current object versions keyed by registry name."""

        with self._lock:
            rows = self._conn.execute(self._object_select_sql()).fetchall()
        records = [
            self._object_row_to_record(row, include_yaml=False)
            for row in rows
        ]
        return {record["name"]: record for record in records}

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
            raise ValueError(
                "object decomposition kind must be 'function' or 'pipeline'"
            )
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
                raise ValueError(
                    "pipeline node kind must be 'function' or 'remote': "
                    f"{node_id}"
                )
            if node_kind == "function" and not str(
                node.get("function") or node.get("name") or ""
            ):
                raise ValueError(f"pipeline function node is missing function: {node_id}")
            if node_kind == "remote" and not (
                isinstance(node.get("remote"), dict) or node.get("name")
            ):
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
                raise ValueError(
                    "pipeline link target node is not defined: "
                    f"{target_node_id}"
                )
            if not target_port:
                raise ValueError(
                    "pipeline link target port is not defined: "
                    f"{target_node_id}"
                )

            source_kind = str(source.get("kind") or "")
            if source_kind == "node_output":
                source_node_id = str(source.get("node_id") or "")
                source_port = str(source.get("port") or "")
                if not source_node_id:
                    raise ValueError("pipeline link source node is not defined")
                if source_node_id not in node_by_id:
                    raise ValueError(
                        "pipeline link source node is not defined: "
                        f"{source_node_id}"
                    )
                if not source_port:
                    raise ValueError(
                        "pipeline link source port is not defined: "
                        f"{source_node_id}"
                    )
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
        has_remote_nodes = any(
            node.get("kind") == "remote"
            for node in metadata.get("pipeline_nodes") or []
        )
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
    ) -> dict[str, Any]:
        """Return one object by local name, id, or unique server display name."""

        if version is None:
            with self._lock:
                row = self._conn.execute(
                    f"""
                    {self._object_select_sql()}
                    WHERE o.name = ? OR o.id = ? OR o.remote_name = ?
                    """,
                    (name_or_id, name_or_id, name_or_id),
                ).fetchall()
        else:
            with self._lock:
                row = self._conn.execute(
                    f"""
                    {self._object_select_sql(current_only=False)}
                    WHERE (o.name = ? OR o.id = ? OR o.remote_name = ?)
                      AND ov.version = ?
                    """,
                    (name_or_id, name_or_id, name_or_id, int(version)),
                ).fetchall()
        if not row:
            suffix = f" version {version}" if version is not None else ""
            raise KeyError(f"object is not registered: {name_or_id}{suffix}")
        if len(row) > 1:
            names = ", ".join(sorted(item["object_name"] for item in row))
            raise ValueError(
                "object display name is ambiguous locally: "
                f"{name_or_id}; use one of: {names}"
            )
        [row] = row
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
        return [
            self._object_row_to_record(row, include_yaml=False)
            for row in rows
        ]

    def list_object_versions(self, name_or_id: str) -> list[dict[str, Any]]:
        """Return all versions of one object, newest first."""

        with self._lock:
            rows = self._conn.execute(
                f"""
                {self._object_select_sql(current_only=False)}
                WHERE o.name = ? OR o.id = ? OR o.remote_name = ?
                ORDER BY ov.version DESC
                """,
                (name_or_id, name_or_id, name_or_id),
            ).fetchall()
        if not rows:
            raise KeyError(f"object is not registered: {name_or_id}")
        object_ids = {row["object_id"] for row in rows}
        if len(object_ids) > 1:
            names = ", ".join(sorted({row["object_name"] for row in rows}))
            raise ValueError(
                "object display name is ambiguous locally: "
                f"{name_or_id}; use one of: {names}"
            )
        return [
            self._object_row_to_record(row, include_yaml=False)
            for row in rows
        ]

    def _object_select_sql(self, *, current_only: bool = True) -> str:
        join_condition = (
            "ov.id = o.current_version_id"
            if current_only
            else "ov.object_id = o.id"
        )
        return f"""
            SELECT
                o.id AS object_id,
                o.name AS object_name,
                o.kind AS object_kind,
                o.origin AS object_origin,
                o.remote_owner_id AS object_remote_owner_id,
                o.remote_object_id AS object_remote_object_id,
                o.remote_name AS object_remote_name,
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
        source_owner_id = row["object_remote_owner_id"] or row["remote_owner_id"]
        source_object_id = row["object_remote_object_id"] or row["remote_object_id"]
        source_object_name = row["object_remote_name"]
        source_version_id = row["remote_version_id"]
        runtime_config = normalize_runtime_config(
            json_loads(row["runtime_config_json"], {"mode": "venv"})
        )
        record = {
            "id": row["object_id"],
            "name": row["object_name"],
            "local_registry_name": row["object_name"],
            "display_name": row["object_remote_name"] or row["object_name"],
            "origin": row["object_origin"],
            "object_remote_owner_id": row["object_remote_owner_id"],
            "object_remote_object_id": row["object_remote_object_id"],
            "object_remote_name": row["object_remote_name"],
            "remote_name": row["object_remote_name"],
            "source_owner_id": source_owner_id,
            "source_object_id": source_object_id,
            "source_object_name": source_object_name,
            "source_version_id": source_version_id,
            "remote_display_name": row["object_remote_name"],
            "remote_identity": {
                "origin": row["object_origin"],
                "local_registry_name": row["object_name"],
                "source_owner_id": source_owner_id,
                "source_object_id": source_object_id,
                "source_object_name": source_object_name,
                "source_version_id": source_version_id,
                "remote_display_name": row["object_remote_name"],
                "storage_remote_name": row["object_remote_name"],
            },
            "compatibility": {
                "remote_name": {
                    "status": "deprecated_alias",
                    "replacement": "source_object_name",
                    "storage_field": "objects.remote_name",
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
            "object_kind": row["object_kind"] or row["kind"],
            "version_kind": row["version_kind"],
            "kind": row["kind"],
            "type": row["kind"],
            "yaml_path": str(
                self._object_yaml_cache_path(row["object_name"], row["version"])
            ),
            "yaml_sha256": row["yaml_sha256"],
            "workdir": row["workdir"],
            "runtime_config": runtime_config,
            "runtime_mode": runtime_config["mode"],
            "remote_owner_id": row["remote_owner_id"],
            "remote_object_id": row["remote_object_id"],
            "remote_version_id": row["remote_version_id"],
            "inputs": json_loads(row["inputs_json"], []),
            "outputs": json_loads(row["outputs_json"], []),
            "functions": decomposition["functions"],
            "pipeline_nodes": decomposition["nodes"]
            or json_loads(row["pipeline_nodes_json"], []),
            "pipeline_links": decomposition["links"],
            "links": decomposition["links"],
            "decomposition": decomposition,
            "internal_objects": metadata.get("internal_objects", []),
            "distributions": json_loads(row["distributions_json"], []),
            "environment_spec_hash": self.environment_spec_hash_for(
                row["env_python"],
                json_loads(row["distributions_json"], []),
                python_version=self._cached_python_version(row["env_python"]),
            ),
            "metadata": metadata,
            "created_at": row["object_created_at"],
            "updated_at": row["object_updated_at"],
            "version_created_at": row["version_created_at"],
        }
        if include_yaml:
            record["yaml"] = row["yaml_text"]
        return record

    def _object_yaml_cache_path(self, name: str, version: int) -> Path:
        return self.objects_dir / validate_name(name) / "versions" / f"{version}.yaml"

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
