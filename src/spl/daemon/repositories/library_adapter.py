"""Persistence for the separately versioned Library Adapter aggregate."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping
from typing import Any
from uuid import uuid4

from spl.core.library_adapters import normalize_library_adapter_ref
from spl.daemon.storage_base import (
    DEFAULT_OBJECT_LIBRARY,
    DEFAULT_OBJECT_OWNER_ID,
    RepositoryBase,
    json_dumps,
    json_loads,
    utc_now,
    validate_name,
)

MAX_LIBRARY_ADAPTER_CATALOG_LIMIT = 100
MAX_LIBRARY_ADAPTER_CATALOG_ENTRY_BYTES = 192 * 1024
MAX_LIBRARY_ADAPTER_PAGE_BYTES = 384 * 1024
MAX_LIBRARY_ADAPTER_QUERY_CHARS = 128


class LibraryAdapterRepository(RepositoryBase):
    """Store immutable Adapter versions outside the Object tables."""

    def publish_library_adapter(
        self,
        prepared: Mapping[str, Any],
        *,
        owner_id: str | None = None,
        library: str | None = None,
        publisher_id: str | None = None,
        origin: str = "local",
        adapter_id: str | None = None,
        remote_owner_id: str | None = None,
        remote_adapter_id: str | None = None,
        remote_version_id: str | None = None,
    ) -> dict[str, Any]:
        owner = self._library_adapter_owner(owner_id)
        library_name = validate_name(str(library or DEFAULT_OBJECT_LIBRARY))
        name = validate_name(str(prepared["name"]))
        publisher = validate_name(str(publisher_id or owner))
        origin = validate_name(origin)
        requested_adapter_id = None if adapter_id is None else validate_name(adapter_id)
        remote_owner_id = None if remote_owner_id is None else validate_name(remote_owner_id)
        remote_adapter_id = None if remote_adapter_id is None else validate_name(remote_adapter_id)
        remote_version_id = None if remote_version_id is None else validate_name(remote_version_id)
        now = utc_now()
        new_adapter_id = requested_adapter_id or uuid4().hex
        new_version_id = uuid4().hex
        description = str(prepared.get("description") or "")

        with self._lock, self._conn:
            row = None
            if remote_adapter_id is not None:
                row = self._conn.execute(
                    "SELECT * FROM library_adapters WHERE remote_adapter_id = ?",
                    (remote_adapter_id,),
                ).fetchone()
            if row is None:
                row = self._conn.execute(
                    """
                    SELECT * FROM library_adapters
                    WHERE owner_id = ? AND library = ? AND name = ?
                    """,
                    (owner, library_name, name),
                ).fetchone()
            if requested_adapter_id is not None and row is not None and row["id"] != requested_adapter_id:
                raise ValueError("adapter_id does not match the canonical owner/Library/name identity")
            if requested_adapter_id is not None and row is None:
                by_id = self._conn.execute(
                    "SELECT * FROM library_adapters WHERE id = ?",
                    (requested_adapter_id,),
                ).fetchone()
                if by_id is not None:
                    raise ValueError("adapter_id belongs to a different owner/Library/name identity")

            if row is None:
                self._conn.execute(
                    """
                    INSERT INTO library_adapters(
                        id, owner_id, library, name, description, origin,
                        remote_owner_id, remote_adapter_id, current_version_id,
                        created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                    """,
                    (
                        new_adapter_id,
                        owner,
                        library_name,
                        name,
                        description,
                        origin,
                        remote_owner_id,
                        remote_adapter_id,
                        now,
                        now,
                    ),
                )
                resolved_adapter_id = new_adapter_id
                next_version = 1
            else:
                resolved_adapter_id = str(row["id"])
                next_row = self._conn.execute(
                    """
                    SELECT COALESCE(MAX(version), 0) + 1 AS next_version
                    FROM library_adapter_versions WHERE adapter_id = ?
                    """,
                    (resolved_adapter_id,),
                ).fetchone()
                next_version = int(next_row["next_version"])

            if remote_version_id is not None:
                remote_existing = self._conn.execute(
                    "SELECT id FROM library_adapter_versions WHERE remote_version_id = ?",
                    (remote_version_id,),
                ).fetchone()
                if remote_existing is not None:
                    record = self.get_library_adapter_version(
                        str(remote_existing["id"]),
                        include_source=False,
                    )
                    return {**record, "created": False, "deduplicated": True}

            existing = self._conn.execute(
                """
                SELECT id FROM library_adapter_versions
                WHERE adapter_id = ? AND content_hash = ?
                """,
                (resolved_adapter_id, prepared["content_hash"]),
            ).fetchone()
            if existing is not None:
                version_id = str(existing["id"])
                self._conn.execute(
                    """
                    UPDATE library_adapter_versions
                    SET remote_owner_id = COALESCE(remote_owner_id, ?),
                        remote_adapter_id = COALESCE(remote_adapter_id, ?),
                        remote_version_id = COALESCE(remote_version_id, ?)
                    WHERE id = ?
                    """,
                    (remote_owner_id, remote_adapter_id, remote_version_id, version_id),
                )
                self._conn.execute(
                    """
                    UPDATE library_adapters
                    SET origin = CASE WHEN origin = 'local' THEN origin ELSE ? END,
                        remote_owner_id = COALESCE(remote_owner_id, ?),
                        remote_adapter_id = COALESCE(remote_adapter_id, ?),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        origin,
                        remote_owner_id,
                        remote_adapter_id,
                        now,
                        resolved_adapter_id,
                    ),
                )
                record = self.get_library_adapter_version(version_id, include_source=False)
                return {**record, "created": False, "deduplicated": True}

            directions = list(prepared["directions"])
            availability = {
                "local": {"state": "available", "reason": None},
                "remote": {
                    "state": (
                        "requires_target_preflight"
                        if prepared["policy"]["remote_custom_code"] == "allow"
                        else "unavailable"
                    ),
                    "reason": (
                        "target_dependency_and_policy_unverified"
                        if prepared["policy"]["remote_custom_code"] == "allow"
                        else "remote_custom_code_denied"
                    ),
                },
            }
            compatibility = {
                "directions": directions,
                "cross_version": (
                    "declared_format_tag" if prepared.get("format_tag") is not None else "exact_version_only"
                ),
            }
            self._conn.execute(
                """
                INSERT INTO library_adapter_versions(
                    id, adapter_id, version, description, content_hash,
                    signature_hash, semantic_type, semantic_category,
                    save_source, load_source, save_symbol, load_symbol,
                    dependencies_json, format_tag, media_type,
                    preferred_extension, policy_json,
                    publication_environment_json, signature_json,
                    availability_json, compatibility_json, publisher_id,
                    remote_owner_id, remote_adapter_id, remote_version_id,
                    created_at
                ) VALUES(
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    new_version_id,
                    resolved_adapter_id,
                    next_version,
                    description,
                    prepared["content_hash"],
                    prepared["signature_hash"],
                    prepared["semantic_type"],
                    prepared.get("semantic_category"),
                    prepared.get("save_source"),
                    prepared.get("load_source"),
                    prepared["symbols"]["save"],
                    prepared["symbols"]["load"],
                    json_dumps(prepared["dependencies"]),
                    prepared.get("format_tag"),
                    prepared.get("media_type"),
                    prepared.get("preferred_extension"),
                    json_dumps(prepared["policy"]),
                    json_dumps(prepared["publication_environment"]),
                    json_dumps(prepared["signature"]),
                    json_dumps(availability),
                    json_dumps(compatibility),
                    publisher,
                    remote_owner_id,
                    remote_adapter_id,
                    remote_version_id,
                    now,
                ),
            )
            self._conn.execute(
                """
                UPDATE library_adapters
                SET description = ?, current_version_id = ?,
                    origin = CASE WHEN origin = 'local' THEN origin ELSE ? END,
                    remote_owner_id = COALESCE(remote_owner_id, ?),
                    remote_adapter_id = COALESCE(remote_adapter_id, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    description,
                    new_version_id,
                    origin,
                    remote_owner_id,
                    remote_adapter_id,
                    now,
                    resolved_adapter_id,
                ),
            )
            record = self.get_library_adapter_version(new_version_id, include_source=False)
            return {**record, "created": True, "deduplicated": False}

    def list_library_adapters(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        query: str | None = None,
        direction: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_LIBRARY_ADAPTER_CATALOG_LIMIT:
            raise ValueError(f"limit must be between 1 and {MAX_LIBRARY_ADAPTER_CATALOG_LIMIT}")
        if direction is not None and direction not in {"input", "output"}:
            raise ValueError("direction must be 'input' or 'output'")
        if query is not None and (not isinstance(query, str) or len(query) > MAX_LIBRARY_ADAPTER_QUERY_CHARS):
            raise ValueError("query is too long")
        clauses = ["a.current_version_id = v.id"]
        args: list[Any] = []
        if owner_id is not None:
            clauses.append("a.owner_id = ?")
            args.append(validate_name(owner_id))
        if library is not None:
            clauses.append("a.library = ?")
            args.append(validate_name(library))
        if query:
            clauses.append("(a.name LIKE ? ESCAPE '\\' OR a.description LIKE ? ESCAPE '\\')")
            escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            args.extend((f"%{escaped}%", f"%{escaped}%"))
        if direction == "input":
            clauses.append("v.load_source IS NOT NULL")
        elif direction == "output":
            clauses.append("v.save_source IS NOT NULL")
        if cursor is not None:
            cursor_key = self._decode_catalog_cursor(cursor)
            clauses.append("(a.owner_id, a.library, a.name, -v.version, v.id) > (?, ?, ?, ?, ?)")
            args.extend(
                (
                    cursor_key[0],
                    cursor_key[1],
                    cursor_key[2],
                    -int(cursor_key[3]),
                    cursor_key[4],
                )
            )
        rows = self._conn.execute(
            f"""
            {self._select_sql()}
            WHERE {" AND ".join(clauses)}
            ORDER BY a.owner_id, a.library, a.name, v.version DESC, v.id
            LIMIT ?
            """,
            (*args, limit + 1),
        ).fetchall()
        records = [self._row_to_record(row, include_source=False) for row in rows]
        selected, next_cursor, truncated = self._bounded_page(
            records,
            limit=limit,
            cursor_encoder=self._encode_catalog_cursor,
            schema="spl.library-adapter-catalog",
            include_catalog_envelope=True,
        )
        return {
            "schema": "spl.library-adapter-catalog",
            "schema_version": 1,
            "items": selected,
            "next_cursor": next_cursor,
            "truncated": truncated,
        }

    def get_library_adapter(
        self,
        name_or_id: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        clauses = ["a.current_version_id = v.id"]
        args: list[Any] = []
        if owner_id is None and library is None:
            clauses.append("a.id = ?")
            args.append(validate_name(name_or_id))
        else:
            clauses.extend(["a.owner_id = ?", "a.library = ?", "a.name = ?"])
            args.extend(
                (
                    self._library_adapter_owner(owner_id),
                    validate_name(str(library or DEFAULT_OBJECT_LIBRARY)),
                    validate_name(name_or_id),
                )
            )
        row = self._conn.execute(
            f"{self._select_sql()} WHERE {' AND '.join(clauses)}",
            tuple(args),
        ).fetchone()
        if row is None:
            raise KeyError("library adapter is not available")
        return self._row_to_record(row, include_source=False)

    def list_library_adapter_versions(
        self,
        name_or_id: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_LIBRARY_ADAPTER_CATALOG_LIMIT:
            raise ValueError(f"limit must be between 1 and {MAX_LIBRARY_ADAPTER_CATALOG_LIMIT}")
        current = self.get_library_adapter(
            name_or_id,
            owner_id=owner_id,
            library=library,
        )
        cursor_clause = ""
        args: list[Any] = [current["adapter_id"]]
        if cursor is not None:
            cursor_key = self._decode_version_cursor(cursor)
            cursor_clause = "AND (-v.version, v.id) > (?, ?)"
            args.extend((-int(cursor_key[0]), cursor_key[1]))
        args.append(limit + 1)
        rows = self._conn.execute(
            f"""
            {self._select_sql()}
            WHERE a.id = ?
            {cursor_clause}
            ORDER BY v.version DESC, v.id
            LIMIT ?
            """,
            tuple(args),
        ).fetchall()
        records = [self._row_to_record(row, include_source=False) for row in rows]
        selected, next_cursor, truncated = self._bounded_page(
            records,
            limit=limit,
            cursor_encoder=self._encode_version_cursor,
            schema="spl.library-adapter-versions",
            include_catalog_envelope=False,
        )
        return {
            "items": selected,
            "truncated": truncated,
            "next_cursor": next_cursor,
        }

    def get_library_adapter_version(
        self,
        version_ref: str | int,
        *,
        adapter_id: str | None = None,
        include_source: bool = False,
    ) -> dict[str, Any]:
        if isinstance(version_ref, bool):
            raise ValueError("adapter version reference is invalid")
        if isinstance(version_ref, int):
            if adapter_id is None:
                raise ValueError("adapter_id is required with a numeric version")
            clause = "a.id = ? AND v.version = ?"
            args: tuple[Any, ...] = (validate_name(adapter_id), version_ref)
        else:
            clause = "v.id = ?"
            args = (validate_name(str(version_ref)),)
            if adapter_id is not None:
                clause += " AND a.id = ?"
                args += (validate_name(adapter_id),)
        row = self._conn.execute(
            f"{self._select_sql()} WHERE {clause}",
            args,
        ).fetchone()
        if row is None:
            raise KeyError("library adapter version is not available")
        return self._row_to_record(row, include_source=include_source)

    def resolve_library_adapter_ref(
        self,
        value: Mapping[str, Any],
        *,
        include_source: bool = False,
    ) -> dict[str, Any]:
        ref = normalize_library_adapter_ref(value)
        try:
            record = self.get_library_adapter_version(
                ref["adapter_version_id"],
                adapter_id=ref["adapter_id"],
                include_source=include_source,
            )
        except (KeyError, ValueError):
            raise KeyError("exact library adapter reference is not available") from None
        observed = {key: record[key] for key in ref}
        if observed != ref:
            raise KeyError("exact library adapter reference is not available")
        return record

    def library_adapter_remote_link(
        self,
        value: Mapping[str, Any],
    ) -> dict[str, str]:
        """Return a proven central identity linked to one exact local version."""

        ref = normalize_library_adapter_ref(value)
        row = self._conn.execute(
            f"{self._select_sql()} WHERE a.id = ? AND v.id = ?",
            (ref["adapter_id"], ref["adapter_version_id"]),
        ).fetchone()
        if row is None:
            raise KeyError("exact local library adapter reference is not available")
        record = self._row_to_record(row, include_source=False)
        if {key: record[key] for key in ref} != ref:
            raise KeyError("exact local library adapter reference is not available")
        remote_owner = row["remote_owner_id"] or row["adapter_remote_owner_id"]
        remote_adapter = row["remote_adapter_id"] or row["adapter_remote_adapter_id"]
        remote_version = row["remote_version_id"]
        if not all(
            isinstance(item, str) and item
            for item in (
                remote_owner,
                remote_adapter,
                remote_version,
            )
        ):
            raise KeyError("exact local library adapter version has not been synced")
        return {
            "owner": validate_name(str(remote_owner)),
            "library": ref["library"],
            "adapter_id": validate_name(str(remote_adapter)),
            "adapter_version_id": validate_name(str(remote_version)),
        }

    def _library_adapter_owner(self, owner_id: str | None) -> str:
        if owner_id is not None:
            return validate_name(str(owner_id))
        credentials = self.current_server_connection_credentials()
        if credentials is not None and credentials.get("owner_id"):
            return validate_name(str(credentials["owner_id"]))
        return DEFAULT_OBJECT_OWNER_ID

    @staticmethod
    def _select_sql() -> str:
        return """
            SELECT
                a.id AS adapter_id,
                a.owner_id AS adapter_owner,
                a.library AS adapter_library,
                a.name AS adapter_name,
                a.origin AS adapter_origin,
                a.current_version_id,
                a.remote_owner_id AS adapter_remote_owner_id,
                a.remote_adapter_id AS adapter_remote_adapter_id,
                v.id AS adapter_version_id,
                v.version,
                v.description,
                v.content_hash,
                v.signature_hash,
                v.semantic_type,
                v.semantic_category,
                v.save_source,
                v.load_source,
                v.save_symbol,
                v.load_symbol,
                v.dependencies_json,
                v.format_tag,
                v.media_type,
                v.preferred_extension,
                v.policy_json,
                v.publication_environment_json,
                v.signature_json,
                v.availability_json,
                v.compatibility_json,
                v.publisher_id,
                v.remote_owner_id,
                v.remote_adapter_id,
                v.remote_version_id,
                v.created_at
            FROM library_adapters AS a
            JOIN library_adapter_versions AS v ON v.adapter_id = a.id
        """

    @staticmethod
    def _row_to_record(row: Any, *, include_source: bool) -> dict[str, Any]:
        policy = json_loads(row["policy_json"], {})
        record = {
            "owner": row["adapter_owner"],
            "library": row["adapter_library"],
            "name": row["adapter_name"],
            "version": int(row["version"]),
            "adapter_id": row["adapter_id"],
            "adapter_version_id": row["adapter_version_id"],
            "content_hash": row["content_hash"],
            "signature_hash": row["signature_hash"],
            "description": row["description"],
            "dependencies": json_loads(row["dependencies_json"], []),
            "semantic_type": row["semantic_type"],
            "semantic_category": row["semantic_category"],
            "directions": [
                direction
                for direction, source in (
                    ("input", row["load_source"]),
                    ("output", row["save_source"]),
                )
                if source is not None
            ],
            "format_tag": row["format_tag"],
            "media_type": row["media_type"],
            "preferred_extension": row["preferred_extension"],
            "effective_policy": {
                "local_custom": policy.get("local_custom_code", "deny"),
                "remote_custom": policy.get("remote_custom_code", "deny"),
            },
            "created_at": row["created_at"],
            "publisher": row["publisher_id"],
            "availability": json_loads(row["availability_json"], {}),
            "compatibility": json_loads(row["compatibility_json"], {}),
        }
        if include_source:
            record.update(
                {
                    "save_source": row["save_source"],
                    "load_source": row["load_source"],
                    "symbols": {
                        "save": row["save_symbol"],
                        "load": row["load_symbol"],
                    },
                    "policy": policy,
                    "publication_environment": json_loads(
                        row["publication_environment_json"],
                        {},
                    ),
                    "signature": json_loads(row["signature_json"], {}),
                }
            )
        return record

    @classmethod
    def _bounded_page(
        cls,
        records: list[dict[str, Any]],
        *,
        limit: int,
        cursor_encoder: Callable[[Mapping[str, Any]], str],
        schema: str,
        include_catalog_envelope: bool,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """Select a deterministic page whose browser JSON stays within 384 KiB."""

        selected: list[dict[str, Any]] = []
        for record in records:
            projected_bytes = len(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            )
            if projected_bytes > MAX_LIBRARY_ADAPTER_CATALOG_ENTRY_BYTES:
                raise ValueError("Library Adapter catalog entry exceeds its safe projection bound")
            if len(selected) >= limit:
                break
            candidate = [*selected, record]
            candidate_cursor = cursor_encoder(record)
            candidate_body: dict[str, Any] = {
                "schema": schema,
                "schema_version": 1,
                "items": candidate,
                "next_cursor": candidate_cursor,
                "truncated": True,
            }
            if include_catalog_envelope:
                # The route adds a fixed-width SHA-256 revision and, when the
                # daemon owns its home lock, this bounded instance identity.
                candidate_body.update(
                    {
                        "revision": "f" * 64,
                        "instance": {
                            "instance_id": "i" * 64,
                            "generation": 9_223_372_036_854_775_807,
                        },
                    }
                )
            serialized_bytes = len(
                json.dumps(
                    candidate_body,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                    separators=None,
                    allow_nan=False,
                ).encode("utf-8")
            )
            if serialized_bytes > MAX_LIBRARY_ADAPTER_PAGE_BYTES:
                if not selected:
                    raise ValueError("Library Adapter catalog entry exceeds the page response bound")
                break
            selected.append(record)

        truncated = len(selected) < len(records)
        next_cursor = cursor_encoder(selected[-1]) if truncated and selected else None
        return selected, next_cursor, truncated

    @staticmethod
    def _catalog_key(record: Mapping[str, Any]) -> list[Any]:
        return [
            record["owner"],
            record["library"],
            record["name"],
            int(record["version"]),
            record["adapter_version_id"],
        ]

    @classmethod
    def _encode_catalog_cursor(cls, record: Mapping[str, Any]) -> str:
        body = json.dumps(
            cls._catalog_key(record),
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        return base64.urlsafe_b64encode(body).decode("ascii").rstrip("=")

    @staticmethod
    def _decode_catalog_cursor(cursor: str) -> list[Any]:
        if not isinstance(cursor, str) or not cursor or len(cursor) > 1_024:
            raise ValueError("catalog cursor is invalid")
        try:
            padding = "=" * (-len(cursor) % 4)
            value = json.loads(base64.b64decode(cursor + padding, altchars=b"-_", validate=True))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            raise ValueError("catalog cursor is invalid") from None
        if not isinstance(value, list) or len(value) != 5 or any(not isinstance(item, str | int) for item in value):
            raise ValueError("catalog cursor is invalid")
        return value

    @staticmethod
    def _encode_version_cursor(record: Mapping[str, Any]) -> str:
        body = json.dumps(
            [int(record["version"]), record["adapter_version_id"]],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        return base64.urlsafe_b64encode(body).decode("ascii").rstrip("=")

    @staticmethod
    def _decode_version_cursor(cursor: str) -> list[Any]:
        if not isinstance(cursor, str) or not cursor or len(cursor) > 512:
            raise ValueError("version cursor is invalid")
        try:
            padding = "=" * (-len(cursor) % 4)
            value = json.loads(base64.b64decode(cursor + padding, altchars=b"-_", validate=True))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            raise ValueError("version cursor is invalid") from None
        if (
            not isinstance(value, list)
            or len(value) != 2
            or isinstance(value[0], bool)
            or not isinstance(value[0], int)
            or value[0] < 1
            or not isinstance(value[1], str)
        ):
            raise ValueError("version cursor is invalid")
        return value


__all__ = [
    "LibraryAdapterRepository",
    "MAX_LIBRARY_ADAPTER_CATALOG_ENTRY_BYTES",
    "MAX_LIBRARY_ADAPTER_CATALOG_LIMIT",
    "MAX_LIBRARY_ADAPTER_PAGE_BYTES",
    "MAX_LIBRARY_ADAPTER_QUERY_CHARS",
]
