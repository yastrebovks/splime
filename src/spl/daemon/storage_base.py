"""Persistent SQLite registry used by the local SPL daemon.

The first daemon version kept JSON metadata next to copied YAML files.  That was
useful while the runtime shape was still moving, but it could not provide a real
object history.  The registry is now SQLite-backed: object versions, metadata,
run state, inputs, logs, and JSON results live in one local database.  Per-run
files are still used as the worker protocol and artifact storage, but they are
derived from the database, not the other way around.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import re
import shutil
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Any, Callable
from uuid import uuid4

from spl.daemon.secret_store import SecretStore


NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
FUNCTION_REF_SEPARATOR = "::"
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
REDACTED_SECRET_VALUE = "__spl_daemon_secret__"
DEFAULT_OBJECT_OWNER_ID = "local"
DEFAULT_OBJECT_LIBRARY = "default"
OBJECT_IDENTITY_MIGRATION_ID = "20260702_object_identity_v1"
OBJECT_IDENTITY_HEAL_MIGRATION_ID = "20260702_object_identity_heal_v1"
OBJECT_IDENTITY_SCHEMA_VERSION = 1


def utc_now() -> str:
    """Return a stable timestamp format for registry and run records."""

    return datetime.now(UTC).isoformat()


def iso_after_now(seconds: float) -> str:
    """Return a UTC timestamp ``seconds`` from now."""

    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()


def normalize_heartbeat_interval(value: Any) -> float:
    """Normalize optional heartbeat interval values used by server connections."""

    if value is None:
        return DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    interval = float(value)
    if interval <= 0:
        raise ValueError("heartbeat_interval_seconds must be positive")
    return interval


def default_home() -> Path:
    """Return the default daemon data directory."""

    import os

    return Path(os.environ.get("SPL_DAEMON_HOME", Path.home() / ".spl-daemon"))


def validate_name(name: str) -> str:
    """Validate a registry-safe name and return it unchanged."""

    if not NAME_PATTERN.fullmatch(name):
        raise ValueError(
            "name must contain only letters, digits, underscore, dash, and dot"
        )
    return name


def split_object_function_ref(
    object_name: str,
    function: str | None = None,
) -> tuple[str, str | None]:
    """Split ``object::function`` while preserving ordinary object names."""

    object_name = str(object_name)
    if FUNCTION_REF_SEPARATOR in object_name:
        parent, inline_function = object_name.split(FUNCTION_REF_SEPARATOR, 1)
        if not parent or not inline_function:
            raise ValueError("function reference must look like object::function")
        if function is not None and str(function) != inline_function:
            raise ValueError(
                "function was provided twice with different values: "
                f"{inline_function!r} and {function!r}"
            )
        object_name = parent
        function = inline_function
    object_name = validate_name(object_name)
    if function is not None:
        function = validate_name(str(function))
    return object_name, function


def read_json(path: Path, default: Any) -> Any:
    """Read JSON from ``path`` or return ``default`` when the file is absent."""

    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    """Write JSON atomically enough for local daemon files."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def json_dumps(value: Any) -> str:
    """Serialize stable JSON for SQLite text columns."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def json_loads(value: str | None, default: Any) -> Any:
    """Decode a nullable SQLite JSON text value."""

    if value is None:
        return default
    return json.loads(value)


class StorageBase:
    """Shared SQLite storage state, paths, locks, and schema bootstrap."""

    def __init__(self, home: Path | None = None):
        self.home = (home or default_home()).absolute()
        self.db_path = self.home / "daemon.sqlite3"
        self.registry_path = self.home / "registry.json"
        self.objects_dir = self.home / "objects"
        self.runs_dir = self.home / "runs"
        self.environment_builds_dir = self.home / "environment-builds"
        self.secret_store = SecretStore(self.home)
        self._lock = RLock()
        self._python_version_cache: dict[str, str] = {}
        # SQLite cannot create the database file when the parent directory is
        # absent.  Create the daemon home before opening the connection so a new
        # --home path works on the first run.
        self.home.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

    def register_repositories(self, *repositories: object) -> None:
        """Register aggregate repositories for private cross-aggregate helpers."""

        self._repositories = repositories

    def __getattr__(self, name: str) -> Any:
        for repository in getattr(self, "_repositories", ()):
            for cls in type(repository).__mro__:
                if name in cls.__dict__:
                    return getattr(repository, name)
        raise AttributeError(name)

    def bootstrap(
        self,
        *,
        migrate_legacy_registry: Callable[[], None],
        migrate_server_connection_secrets_locked: Callable[[], None],
        backfill_object_kinds_locked: Callable[[], None],
        backfill_object_decomposition_locked: Callable[[], None],
    ) -> None:
        """Create the database schema and local run directories."""

        self.home.mkdir(parents=True, exist_ok=True)
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.environment_builds_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.execute("PRAGMA busy_timeout = 5000")
            self.migrate_object_identity_schema(dry_run=False)
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS envs (
                    name TEXT PRIMARY KEY,
                    python TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS objects (
                    id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    library TEXT NOT NULL DEFAULT 'default',
                    name TEXT NOT NULL,
                    kind TEXT,
                    origin TEXT NOT NULL DEFAULT 'local',
                    remote_owner_id TEXT,
                    remote_object_id TEXT,
                    source_object_name TEXT,
                    remote_name TEXT GENERATED ALWAYS AS (source_object_name) VIRTUAL,
                    description TEXT NOT NULL DEFAULT '',
                    current_version_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(owner_id, library, name)
                );

                CREATE TABLE IF NOT EXISTS object_versions (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    version_label TEXT,
                    description TEXT NOT NULL DEFAULT '',
                    entrypoint TEXT NOT NULL,
                    env TEXT NOT NULL,
                    env_python TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    yaml_text TEXT NOT NULL,
                    yaml_sha256 TEXT NOT NULL,
                    content_hash TEXT,
                    metadata_json TEXT NOT NULL,
                    inputs_json TEXT NOT NULL,
                    outputs_json TEXT NOT NULL,
                    pipeline_nodes_json TEXT NOT NULL,
                    distributions_json TEXT NOT NULL,
                    runtime_config_json TEXT NOT NULL DEFAULT '{"mode":"venv"}',
                    workdir TEXT,
                    remote_owner_id TEXT,
                    remote_object_id TEXT,
                    remote_version_id TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(env) REFERENCES envs(name),
                    UNIQUE(object_id, version),
                    UNIQUE(object_id, content_hash)
                );

                CREATE TABLE IF NOT EXISTS object_functions (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    object_version_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    node_id TEXT,
                    name TEXT NOT NULL,
                    inputs_json TEXT NOT NULL,
                    outputs_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(object_version_id) REFERENCES object_versions(id)
                );

                CREATE TABLE IF NOT EXISTS object_pipeline_nodes (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    object_version_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    node_kind TEXT NOT NULL,
                    name TEXT NOT NULL,
                    function_name TEXT,
                    remote_json TEXT NOT NULL,
                    inputs_json TEXT NOT NULL,
                    outputs_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(object_version_id) REFERENCES object_versions(id),
                    UNIQUE(object_version_id, node_id)
                );

                CREATE TABLE IF NOT EXISTS object_pipeline_links (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    object_version_id TEXT NOT NULL,
                    target_node_id TEXT NOT NULL,
                    target_port TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_node_id TEXT,
                    source_port TEXT,
                    scalar_json TEXT,
                    link_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(object_version_id) REFERENCES object_versions(id)
                );

                CREATE TABLE IF NOT EXISTS environment_builds (
                    spec_hash TEXT PRIMARY KEY,
                    base_python TEXT NOT NULL,
                    python_version TEXT NOT NULL,
                    distributions_json TEXT NOT NULL,
                    runtime_packages_json TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    venv_path TEXT NOT NULL,
                    python_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    error TEXT,
                    install_log_path TEXT,
                    runtime_type TEXT NOT NULL DEFAULT 'venv',
                    image_tag TEXT,
                    base_image TEXT
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    object_version_id TEXT NOT NULL,
                    object_name TEXT NOT NULL,
                    object_version INTEGER NOT NULL,
                    entrypoint TEXT NOT NULL,
                    env TEXT NOT NULL,
                    env_python TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    run_dir TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    result_path TEXT NOT NULL,
                    result_json TEXT,
                    artifacts_dir TEXT NOT NULL,
                    env_build_hash TEXT,
                    runtime_config_json TEXT NOT NULL DEFAULT '{"mode":"venv"}',
                    runtime_build_hash TEXT,
                    resolved_runtime TEXT,
                    runtime_backend TEXT,
                    image_tag TEXT,
                    container_id TEXT,
                    resolved_python TEXT,
                    error TEXT,
                    returncode INTEGER,
                    command_json TEXT,
                    stdout_path TEXT,
                    stderr_path TEXT,
                    stdout_text TEXT,
                    stderr_text TEXT,
                    FOREIGN KEY(object_id) REFERENCES objects(id),
                    FOREIGN KEY(object_version_id) REFERENCES object_versions(id)
                );

                CREATE TABLE IF NOT EXISTS server_connections (
                    id TEXT PRIMARY KEY,
                    server_url TEXT NOT NULL,
                    token_hint TEXT NOT NULL,
                    user_token_hint TEXT,
                    token_secret_ref TEXT,
                    user_token_secret_ref TEXT,
                    token_redacted TEXT NOT NULL,
                    user_token_redacted TEXT,
                    remote_connection_id TEXT,
                    owner_id TEXT,
                    subject_type TEXT,
                    subject_id TEXT,
                    machine_id TEXT NOT NULL,
                    display_name TEXT,
                    capabilities_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    heartbeat_interval_seconds REAL NOT NULL DEFAULT 60,
                    last_heartbeat_at TEXT,
                    next_heartbeat_at TEXT,
                    lease_expires_at TEXT,
                    last_library_snapshot_hash TEXT,
                    last_library_snapshot_at TEXT,
                    created_at TEXT NOT NULL,
                    connected_at TEXT,
                    disconnected_at TEXT,
                    updated_at TEXT NOT NULL,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS sync_events (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    sent_at TEXT,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS remote_signatures (
                    id TEXT PRIMARY KEY,
                    server_url TEXT NOT NULL,
                    owner_id TEXT,
                    library TEXT,
                    object_name TEXT NOT NULL,
                    version TEXT,
                    version_id TEXT,
                    signature_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    fetched_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_objects_name
                    ON objects(name);
                CREATE INDEX IF NOT EXISTS idx_object_versions_object
                    ON object_versions(object_id, version);
                CREATE INDEX IF NOT EXISTS idx_object_functions_version
                    ON object_functions(object_version_id, role, node_id);
                CREATE INDEX IF NOT EXISTS idx_object_pipeline_nodes_version
                    ON object_pipeline_nodes(object_version_id, node_id);
                CREATE INDEX IF NOT EXISTS idx_object_pipeline_links_version
                    ON object_pipeline_links(object_version_id, target_node_id, target_port);
                CREATE INDEX IF NOT EXISTS idx_runs_created
                    ON runs(created_at);
                CREATE INDEX IF NOT EXISTS idx_environment_builds_status
                    ON environment_builds(status);
                CREATE INDEX IF NOT EXISTS idx_server_connections_status
                    ON server_connections(status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_remote_signatures_ref
                    ON remote_signatures(server_url, owner_id, library, object_name, version, version_id);
                """
            )
            self._ensure_column("runs", "env_build_hash", "TEXT")
            self._ensure_column(
                "runs",
                "runtime_config_json",
                "TEXT NOT NULL DEFAULT '{\"mode\":\"venv\"}'",
            )
            self._ensure_column("runs", "runtime_build_hash", "TEXT")
            self._ensure_column("runs", "resolved_runtime", "TEXT")
            self._ensure_column("runs", "runtime_backend", "TEXT")
            self._ensure_column("runs", "image_tag", "TEXT")
            self._ensure_column("runs", "container_id", "TEXT")
            self._ensure_column("runs", "resolved_python", "TEXT")
            self._ensure_column("server_connections", "user_token_hint", "TEXT")
            self._ensure_column("server_connections", "token_secret_ref", "TEXT")
            self._ensure_column("server_connections", "user_token_secret_ref", "TEXT")
            self._ensure_column(
                "server_connections",
                "token_redacted",
                f"TEXT NOT NULL DEFAULT '{REDACTED_SECRET_VALUE}'",
            )
            self._ensure_column("server_connections", "user_token_redacted", "TEXT")
            self._ensure_column(
                "server_connections",
                "heartbeat_interval_seconds",
                "REAL NOT NULL DEFAULT 60",
            )
            self._ensure_column("server_connections", "last_heartbeat_at", "TEXT")
            self._ensure_column("server_connections", "next_heartbeat_at", "TEXT")
            self._ensure_column("server_connections", "lease_expires_at", "TEXT")
            self._ensure_column("server_connections", "remote_connection_id", "TEXT")
            self._ensure_column("server_connections", "owner_id", "TEXT")
            self._ensure_column("server_connections", "subject_type", "TEXT")
            self._ensure_column("server_connections", "subject_id", "TEXT")
            self._ensure_column("server_connections", "last_library_snapshot_hash", "TEXT")
            self._ensure_column("server_connections", "last_library_snapshot_at", "TEXT")
            self._ensure_column("server_connections", "connected_at", "TEXT")
            self._ensure_column("server_connections", "disconnected_at", "TEXT")
            self._ensure_column("server_connections", "error", "TEXT")
            self._ensure_column("objects", "kind", "TEXT")
            self._ensure_column(
                "objects",
                "owner_id",
                f"TEXT NOT NULL DEFAULT '{DEFAULT_OBJECT_OWNER_ID}'",
            )
            self._ensure_column(
                "objects",
                "library",
                f"TEXT NOT NULL DEFAULT '{DEFAULT_OBJECT_LIBRARY}'",
            )
            self._ensure_column("objects", "origin", "TEXT NOT NULL DEFAULT 'local'")
            self._ensure_column("objects", "remote_owner_id", "TEXT")
            self._ensure_column("objects", "remote_object_id", "TEXT")
            self._ensure_column("objects", "source_object_name", "TEXT")
            self._ensure_column("object_versions", "remote_owner_id", "TEXT")
            self._ensure_column("object_versions", "remote_object_id", "TEXT")
            self._ensure_column("object_versions", "remote_version_id", "TEXT")
            self._ensure_column("object_versions", "content_hash", "TEXT")
            self._ensure_column(
                "object_versions",
                "runtime_config_json",
                "TEXT NOT NULL DEFAULT '{\"mode\":\"venv\"}'",
            )
            self._ensure_column(
                "environment_builds",
                "runtime_type",
                "TEXT NOT NULL DEFAULT 'venv'",
            )
            self._ensure_column("environment_builds", "image_tag", "TEXT")
            self._ensure_column("environment_builds", "base_image", "TEXT")
            self._ensure_column("sync_events", "attempts", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("remote_signatures", "library", "TEXT")
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_identity
                ON objects(owner_id, library, name)
                """
            )
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_object_versions_content_hash
                ON object_versions(object_id, content_hash)
                WHERE content_hash IS NOT NULL
                """
            )
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_remote_object
                ON objects(remote_object_id)
                WHERE remote_object_id IS NOT NULL
                """
            )
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_object_versions_remote_version
                ON object_versions(remote_version_id)
                WHERE remote_version_id IS NOT NULL
                """
            )
            migrate_legacy_registry()
            migrate_server_connection_secrets_locked()
            backfill_object_kinds_locked()
            backfill_object_decomposition_locked()
            self._conn.execute(
                """
                INSERT OR IGNORE INTO schema_migrations(id, applied_at)
                VALUES(?, ?)
                """,
                (OBJECT_IDENTITY_MIGRATION_ID, utc_now()),
            )
            self._conn.execute(
                f"PRAGMA user_version = {OBJECT_IDENTITY_SCHEMA_VERSION}"
            )
            self._conn.commit()

    def close(self) -> None:
        """Close the SQLite connection held by this store."""

        self._conn.close()

    def __enter__(self) -> "StorageBase":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def read_json(self, path: Path, default: Any) -> Any:
        """Read JSON from ``path`` or return ``default`` when absent."""

        return read_json(path, default)

    def write_json(self, path: Path, value: Any) -> None:
        """Write JSON atomically enough for local daemon files."""

        write_json(path, value)

    def _table_columns_locked(self, table: str) -> set[str]:
        rows = self._conn.execute(f"PRAGMA table_xinfo({validate_name(table)})").fetchall()
        return {row["name"] for row in rows}

    def _row_value(self, row: sqlite3.Row, name: str) -> Any:
        return row[name] if name in row.keys() else None

    def _ensure_column(self, table: str, column: str, declaration: str) -> None:
        existing = {
            row["name"]
            for row in self._conn.execute(f"PRAGMA table_xinfo({table})").fetchall()
        }
        if column not in existing:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    def migrate_object_identity_schema(self, *, dry_run: bool = False) -> dict[str, Any]:
        """Apply the object identity schema migration.

        The migration rebuilds the two affected tables because SQLite cannot drop
        the old ``UNIQUE(name)`` constraint in place.
        """

        with self._lock:
            plan = self._object_identity_migration_plan_locked()
            migration_id = (
                OBJECT_IDENTITY_MIGRATION_ID
                if plan["schema_needed"]
                else OBJECT_IDENTITY_HEAL_MIGRATION_ID
            )
            backup_path = self._object_identity_backup_path(migration_id)
            report = {
                "id": OBJECT_IDENTITY_MIGRATION_ID,
                "heal_id": OBJECT_IDENTITY_HEAL_MIGRATION_ID,
                "schema_version": OBJECT_IDENTITY_SCHEMA_VERSION,
                "needed": plan["needed"],
                "dry_run": dry_run,
                "actions": plan["actions"],
                "healing": plan["healing"],
                "backup_path": str(backup_path),
            }
            if not plan["needed"] or dry_run:
                return report

            self._conn.commit()
            if self.db_path.exists() and not backup_path.exists():
                shutil.copy2(self.db_path, backup_path)

            foreign_keys_enabled = bool(
                self._conn.execute("PRAGMA foreign_keys").fetchone()[0]
            )
            self._conn.execute("PRAGMA foreign_keys = OFF")
            self._conn.execute("BEGIN")
            try:
                if plan["objects_exists"]:
                    self._rebuild_objects_for_identity_locked()
                if plan["object_versions_exists"]:
                    self._rebuild_object_versions_for_identity_locked()
                healing = self._heal_object_identity_collisions_locked()
                self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        id TEXT PRIMARY KEY,
                        applied_at TEXT NOT NULL
                    )
                    """
                )
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO schema_migrations(id, applied_at)
                    VALUES(?, ?)
                    """,
                    (OBJECT_IDENTITY_MIGRATION_ID, utc_now()),
                )
                if healing["merges"] or healing["conflicts"]:
                    self._conn.execute(
                        """
                        INSERT OR IGNORE INTO schema_migrations(id, applied_at)
                        VALUES(?, ?)
                        """,
                        (OBJECT_IDENTITY_HEAL_MIGRATION_ID, utc_now()),
                    )
                self._conn.execute(
                    f"PRAGMA user_version = {OBJECT_IDENTITY_SCHEMA_VERSION}"
                )
                violations = self._conn.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    details = "; ".join(str(tuple(row)) for row in violations)
                    raise RuntimeError(
                        "object identity migration produced foreign-key violations: "
                        f"{details}"
                    )
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()
            finally:
                if foreign_keys_enabled:
                    self._conn.execute("PRAGMA foreign_keys = ON")

            report["backup_created"] = str(backup_path)
            report["healing"] = healing
            return report

    def _object_identity_backup_path(
        self,
        migration_id: str = OBJECT_IDENTITY_MIGRATION_ID,
    ) -> Path:
        return self.db_path.with_name(
            f"{self.db_path.name}.before-{migration_id}.bak"
        )

    def _object_identity_migration_plan_locked(self) -> dict[str, Any]:
        tables = {
            row["name"]
            for row in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        actions: list[str] = []
        objects_exists = "objects" in tables
        object_versions_exists = "object_versions" in tables

        if objects_exists:
            object_columns = self._column_metadata_locked("objects")
            object_unique_name = self._has_unique_index_locked("objects", ("name",))
            object_identity_unique = self._has_unique_index_locked(
                "objects",
                ("owner_id", "library", "name"),
            )
            remote_name_generated = (
                "remote_name" in object_columns
                and int(object_columns["remote_name"]["hidden"]) != 0
            )
            if (
                "owner_id" not in object_columns
                or "library" not in object_columns
                or "source_object_name" not in object_columns
                or object_unique_name
                or not object_identity_unique
                or not remote_name_generated
            ):
                actions.append("rebuild objects with canonical identity")

        if object_versions_exists:
            version_columns = self._column_metadata_locked("object_versions")
            if (
                "content_hash" not in version_columns
                or not self._has_unique_index_locked(
                    "object_versions",
                    ("object_id", "content_hash"),
                )
            ):
                actions.append("rebuild object_versions with content_hash identity")

        schema_needed = bool(actions)
        healing = self._object_identity_heal_plan_locked() if not schema_needed else {
            "needed": False,
            "merges": [],
            "conflicts": [],
        }
        if healing["needed"]:
            actions.append("heal synthetic server object identity collisions")

        return {
            "needed": bool(actions),
            "schema_needed": schema_needed,
            "actions": actions,
            "objects_exists": objects_exists,
            "object_versions_exists": object_versions_exists,
            "healing": healing,
        }

    def _object_identity_heal_plan_locked(self) -> dict[str, Any]:
        tables = {
            row["name"]
            for row in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if "objects" not in tables or "object_versions" not in tables:
            return {"needed": False, "merges": [], "conflicts": []}
        object_columns = self._table_columns_locked("objects")
        required = {"owner_id", "library", "name", "origin", "source_object_name"}
        if not required <= object_columns:
            return {"needed": False, "merges": [], "conflicts": []}

        rows = self._conn.execute(
            """
            SELECT
                local.id AS target_id,
                mirror.id AS source_id,
                local.owner_id AS owner_id,
                local.library AS library,
                local.name AS name,
                local.kind AS target_kind,
                mirror.kind AS source_kind,
                mirror.name AS synthetic_name,
                mirror.remote_object_id AS remote_object_id
            FROM objects mirror
            JOIN objects local
              ON local.owner_id = mirror.owner_id
             AND local.library = mirror.library
             AND local.name = mirror.source_object_name
             AND local.id <> mirror.id
            WHERE mirror.name LIKE 'server.%'
              AND COALESCE(mirror.origin, 'server') = 'server'
              AND local.name NOT LIKE 'server.%'
            ORDER BY local.owner_id, local.library, local.name, mirror.name
            """
        ).fetchall()
        merges = []
        conflicts = []
        for row in rows:
            item = {
                "owner_id": row["owner_id"],
                "library": row["library"],
                "name": row["name"],
                "target_id": row["target_id"],
                "source_id": row["source_id"],
                "synthetic_name": row["synthetic_name"],
                "remote_object_id": row["remote_object_id"],
            }
            if (
                row["target_kind"] is not None
                and row["source_kind"] is not None
                and row["target_kind"] != row["source_kind"]
            ):
                conflicts.append(
                    {
                        **item,
                        "reason": "kind_mismatch",
                        "target_kind": row["target_kind"],
                        "source_kind": row["source_kind"],
                    }
                )
            else:
                merges.append(item)
        return {
            "needed": bool(merges or conflicts),
            "merges": merges,
            "conflicts": conflicts,
        }

    def _heal_object_identity_collisions_locked(self) -> dict[str, Any]:
        plan = self._object_identity_heal_plan_locked()
        result = {"merges": [], "conflicts": list(plan["conflicts"])}
        for item in plan["merges"]:
            self._merge_object_rows_locked(item["target_id"], item["source_id"])
            result["merges"].append(item)
        return result

    def _merge_object_rows_locked(
        self,
        target_id: str,
        source_id: str,
    ) -> dict[str, Any]:
        if target_id == source_id:
            return {"target_id": target_id, "source_id": source_id, "merged": False}

        target = self._conn.execute(
            "SELECT * FROM objects WHERE id = ?",
            (target_id,),
        ).fetchone()
        source = self._conn.execute(
            "SELECT * FROM objects WHERE id = ?",
            (source_id,),
        ).fetchone()
        if target is None:
            raise KeyError(f"target object is not found: {target_id}")
        if source is None:
            raise KeyError(f"source object is not found: {source_id}")
        if (
            target["kind"] is not None
            and source["kind"] is not None
            and target["kind"] != source["kind"]
        ):
            raise ValueError(
                "object kind is stable and cannot merge "
                f"{source['kind']!r} into {target['kind']!r}"
            )

        target_versions = self._conn.execute(
            """
            SELECT id, version, content_hash
            FROM object_versions
            WHERE object_id = ?
            ORDER BY version, id
            """,
            (target_id,),
        ).fetchall()
        target_by_hash = {
            row["content_hash"]: row
            for row in target_versions
            if row["content_hash"] is not None
        }
        max_version = max((int(row["version"]) for row in target_versions), default=0)
        next_version = max_version + 1
        version_map: dict[str, str] = {}
        moved_versions = 0
        deduped_versions = 0

        source_versions = self._conn.execute(
            """
            SELECT *
            FROM object_versions
            WHERE object_id = ?
            ORDER BY version, id
            """,
            (source_id,),
        ).fetchall()
        for version in source_versions:
            content_hash = version["content_hash"]
            duplicate = target_by_hash.get(content_hash) if content_hash else None
            if duplicate is not None:
                keep_id = duplicate["id"]
                version_map[version["id"]] = keep_id
                self._conn.execute(
                    """
                    UPDATE object_versions
                    SET remote_owner_id = COALESCE(remote_owner_id, ?),
                        remote_object_id = COALESCE(remote_object_id, ?),
                        remote_version_id = COALESCE(remote_version_id, ?)
                    WHERE id = ?
                    """,
                    (
                        version["remote_owner_id"],
                        version["remote_object_id"],
                        version["remote_version_id"],
                        keep_id,
                    ),
                )
                self._repoint_object_version_references_locked(version["id"], keep_id)
                self._delete_object_version_decomposition_locked(version["id"])
                self._conn.execute(
                    "DELETE FROM object_versions WHERE id = ?",
                    (version["id"],),
                )
                deduped_versions += 1
                continue

            version_map[version["id"]] = version["id"]
            self._conn.execute(
                """
                UPDATE object_versions
                SET object_id = ?, version = ?
                WHERE id = ?
                """,
                (target_id, next_version, version["id"]),
            )
            self._conn.execute(
                "UPDATE object_functions SET object_id = ? WHERE object_version_id = ?",
                (target_id, version["id"]),
            )
            self._conn.execute(
                """
                UPDATE object_pipeline_nodes
                SET object_id = ?
                WHERE object_version_id = ?
                """,
                (target_id, version["id"]),
            )
            self._conn.execute(
                """
                UPDATE object_pipeline_links
                SET object_id = ?
                WHERE object_version_id = ?
                """,
                (target_id, version["id"]),
            )
            if content_hash:
                target_by_hash[content_hash] = {
                    "id": version["id"],
                    "version": next_version,
                    "content_hash": content_hash,
                }
            next_version += 1
            moved_versions += 1

        self._conn.execute(
            "UPDATE runs SET object_id = ? WHERE object_id = ?",
            (target_id, source_id),
        )
        self._conn.execute("DELETE FROM objects WHERE id = ?", (source_id,))

        origin = target["origin"]
        if target["origin"] == "local" or source["origin"] == "local":
            origin = "local"
        elif not origin:
            origin = source["origin"]
        description = target["description"] or source["description"] or ""
        current_version = self._conn.execute(
            """
            SELECT id
            FROM object_versions
            WHERE object_id = ?
            ORDER BY version DESC, created_at DESC, id DESC
            LIMIT 1
            """,
            (target_id,),
        ).fetchone()
        now = utc_now()
        self._conn.execute(
            """
            UPDATE objects
            SET kind = COALESCE(kind, ?),
                origin = ?,
                remote_owner_id = COALESCE(remote_owner_id, ?),
                remote_object_id = COALESCE(remote_object_id, ?),
                source_object_name = COALESCE(source_object_name, ?),
                description = ?,
                current_version_id = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                source["kind"],
                origin,
                source["remote_owner_id"],
                source["remote_object_id"],
                source["source_object_name"],
                description,
                current_version["id"] if current_version is not None else None,
                now,
                target_id,
            ),
        )
        return {
            "target_id": target_id,
            "source_id": source_id,
            "merged": True,
            "moved_versions": moved_versions,
            "deduped_versions": deduped_versions,
            "version_map": version_map,
        }

    def _repoint_object_version_references_locked(
        self,
        old_version_id: str,
        keep_version_id: str,
    ) -> None:
        for table, column in (
            ("objects", "current_version_id"),
            ("runs", "object_version_id"),
        ):
            if table not in {
                row["name"]
                for row in self._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }:
                continue
            if column not in self._table_columns_locked(table):
                continue
            self._conn.execute(
                f"""
                UPDATE {validate_name(table)}
                SET {validate_name(column)} = ?
                WHERE {validate_name(column)} = ?
                """,
                (keep_version_id, old_version_id),
            )

    def _delete_object_version_decomposition_locked(
        self,
        object_version_id: str,
    ) -> None:
        for table in (
            "object_functions",
            "object_pipeline_nodes",
            "object_pipeline_links",
        ):
            self._conn.execute(
                f"""
                DELETE FROM {validate_name(table)}
                WHERE object_version_id = ?
                """,
                (object_version_id,),
            )

    def _column_metadata_locked(self, table: str) -> dict[str, sqlite3.Row]:
        return {
            row["name"]: row
            for row in self._conn.execute(
                f"PRAGMA table_xinfo({validate_name(table)})"
            ).fetchall()
        }

    def _has_unique_index_locked(self, table: str, columns: tuple[str, ...]) -> bool:
        for index in self._conn.execute(
            f"PRAGMA index_list({validate_name(table)})"
        ).fetchall():
            if not index["unique"]:
                continue
            indexed_columns = tuple(
                row["name"]
                for row in self._conn.execute(
                    f"PRAGMA index_info({validate_name(index['name'])})"
                ).fetchall()
            )
            if indexed_columns == columns:
                return True
        return False

    def _rebuild_objects_for_identity_locked(self) -> None:
        columns = self._column_metadata_locked("objects")
        source_name_expr = self._coalesce_existing_columns(
            columns,
            ("source_object_name", "remote_name"),
            fallback="NULL",
        )
        self._conn.execute("DROP TABLE IF EXISTS objects__object_identity_v1")
        self._conn.execute(
            """
            CREATE TABLE objects__object_identity_v1 (
                id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                library TEXT NOT NULL DEFAULT 'default',
                name TEXT NOT NULL,
                kind TEXT,
                origin TEXT NOT NULL DEFAULT 'local',
                remote_owner_id TEXT,
                remote_object_id TEXT,
                source_object_name TEXT,
                remote_name TEXT GENERATED ALWAYS AS (source_object_name) VIRTUAL,
                description TEXT NOT NULL DEFAULT '',
                current_version_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(owner_id, library, name)
            )
            """
        )
        self._conn.execute(
            f"""
            INSERT INTO objects__object_identity_v1(
                id, owner_id, library, name, kind, origin, remote_owner_id,
                remote_object_id, source_object_name, description,
                current_version_id, created_at, updated_at
            )
            SELECT
                {self._existing_column(columns, "id")},
                COALESCE({self._existing_column(columns, "owner_id", "NULL")},
                    '{DEFAULT_OBJECT_OWNER_ID}'),
                COALESCE({self._existing_column(columns, "library", "NULL")},
                    '{DEFAULT_OBJECT_LIBRARY}'),
                {self._existing_column(columns, "name")},
                {self._existing_column(columns, "kind", "NULL")},
                COALESCE({self._existing_column(columns, "origin", "NULL")}, 'local'),
                {self._existing_column(columns, "remote_owner_id", "NULL")},
                {self._existing_column(columns, "remote_object_id", "NULL")},
                {source_name_expr},
                COALESCE({self._existing_column(columns, "description", "NULL")}, ''),
                {self._existing_column(columns, "current_version_id", "NULL")},
                COALESCE({self._existing_column(columns, "created_at", "NULL")},
                    '{utc_now()}'),
                COALESCE({self._existing_column(columns, "updated_at", "NULL")},
                    '{utc_now()}')
            FROM objects
            """
        )
        self._conn.execute("DROP TABLE objects")
        self._conn.execute(
            "ALTER TABLE objects__object_identity_v1 RENAME TO objects"
        )

    def _rebuild_object_versions_for_identity_locked(self) -> None:
        columns = self._column_metadata_locked("object_versions")
        content_hash_expr = self._coalesce_existing_columns(
            columns,
            ("content_hash", "yaml_sha256"),
            fallback="NULL",
        )
        self._prepare_object_version_identity_duplicates_locked(
            columns,
            content_hash_expr,
        )
        self._conn.execute(
            "DROP TABLE IF EXISTS object_versions__object_identity_v1"
        )
        self._conn.execute(
            """
            CREATE TABLE object_versions__object_identity_v1 (
                id TEXT PRIMARY KEY,
                object_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                version_label TEXT,
                description TEXT NOT NULL DEFAULT '',
                entrypoint TEXT NOT NULL,
                env TEXT NOT NULL,
                env_python TEXT NOT NULL,
                kind TEXT NOT NULL,
                yaml_text TEXT NOT NULL,
                yaml_sha256 TEXT NOT NULL,
                content_hash TEXT,
                metadata_json TEXT NOT NULL,
                inputs_json TEXT NOT NULL,
                outputs_json TEXT NOT NULL,
                pipeline_nodes_json TEXT NOT NULL,
                distributions_json TEXT NOT NULL,
                runtime_config_json TEXT NOT NULL DEFAULT '{"mode":"venv"}',
                workdir TEXT,
                remote_owner_id TEXT,
                remote_object_id TEXT,
                remote_version_id TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(object_id) REFERENCES objects(id),
                FOREIGN KEY(env) REFERENCES envs(name),
                UNIQUE(object_id, version),
                UNIQUE(object_id, content_hash)
            )
            """
        )
        self._conn.execute(
            f"""
            INSERT INTO object_versions__object_identity_v1(
                id, object_id, version, version_label, description, entrypoint,
                env, env_python, kind, yaml_text, yaml_sha256, content_hash,
                metadata_json, inputs_json, outputs_json, pipeline_nodes_json,
                distributions_json, runtime_config_json, workdir, remote_owner_id,
                remote_object_id, remote_version_id, created_at
            )
            SELECT
                {self._existing_column(columns, "id")},
                {self._existing_column(columns, "object_id")},
                {self._existing_column(columns, "version")},
                {self._existing_column(columns, "version_label", "NULL")},
                COALESCE({self._existing_column(columns, "description", "NULL")}, ''),
                {self._existing_column(columns, "entrypoint")},
                {self._existing_column(columns, "env")},
                {self._existing_column(columns, "env_python")},
                {self._existing_column(columns, "kind")},
                {self._existing_column(columns, "yaml_text")},
                {self._existing_column(columns, "yaml_sha256")},
                {content_hash_expr},
                {self._existing_column(columns, "metadata_json")},
                {self._existing_column(columns, "inputs_json")},
                {self._existing_column(columns, "outputs_json")},
                {self._existing_column(columns, "pipeline_nodes_json")},
                {self._existing_column(columns, "distributions_json")},
                COALESCE(
                    {self._existing_column(columns, "runtime_config_json", "NULL")},
                    '{{"mode":"venv"}}'
                ),
                {self._existing_column(columns, "workdir", "NULL")},
                {self._existing_column(columns, "remote_owner_id", "NULL")},
                {self._existing_column(columns, "remote_object_id", "NULL")},
                {self._existing_column(columns, "remote_version_id", "NULL")},
                COALESCE({self._existing_column(columns, "created_at", "NULL")},
                    '{utc_now()}')
            FROM object_versions
            WHERE id NOT IN (
                SELECT old_id FROM object_version_identity_duplicates
            )
            """
        )
        self._conn.execute("DROP TABLE object_versions")
        self._conn.execute(
            """
            ALTER TABLE object_versions__object_identity_v1
            RENAME TO object_versions
            """
        )
        self._conn.execute("DROP TABLE object_version_identity_duplicates")

    def _prepare_object_version_identity_duplicates_locked(
        self,
        columns: dict[str, sqlite3.Row],
        content_hash_expr: str,
    ) -> None:
        self._conn.execute("DROP TABLE IF EXISTS object_version_identity_duplicates")
        self._conn.execute(
            """
            CREATE TEMP TABLE object_version_identity_duplicates (
                old_id TEXT PRIMARY KEY,
                keep_id TEXT NOT NULL
            )
            """
        )
        if content_hash_expr == "NULL":
            return

        object_id_expr = self._existing_column(columns, "object_id")
        version_expr = self._existing_column(columns, "version")
        id_expr = self._existing_column(columns, "id")
        self._conn.execute(
            f"""
            INSERT INTO object_version_identity_duplicates(old_id, keep_id)
            WITH ranked AS (
                SELECT
                    {id_expr} AS old_id,
                    FIRST_VALUE({id_expr}) OVER (
                        PARTITION BY {object_id_expr}, {content_hash_expr}
                        ORDER BY {version_expr}, {id_expr}
                    ) AS keep_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY {object_id_expr}, {content_hash_expr}
                        ORDER BY {version_expr}, {id_expr}
                    ) AS row_number
                FROM object_versions
                WHERE {content_hash_expr} IS NOT NULL
            )
            SELECT old_id, keep_id
            FROM ranked
            WHERE row_number > 1
            """
        )
        self._repoint_duplicate_object_version_references_locked()

    def _repoint_duplicate_object_version_references_locked(self) -> None:
        tables = {
            row["name"]
            for row in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if "objects" in tables:
            self._repoint_duplicate_object_version_reference_locked(
                "objects",
                "current_version_id",
            )
        if "runs" in tables:
            self._repoint_duplicate_object_version_reference_locked(
                "runs",
                "object_version_id",
            )
        for table in (
            "object_functions",
            "object_pipeline_nodes",
            "object_pipeline_links",
        ):
            if table not in tables:
                continue
            self._conn.execute(
                f"""
                DELETE FROM {validate_name(table)}
                WHERE object_version_id IN (
                    SELECT old_id FROM object_version_identity_duplicates
                )
                """
            )

    def _repoint_duplicate_object_version_reference_locked(
        self,
        table: str,
        column: str,
    ) -> None:
        columns = self._table_columns_locked(table)
        if column not in columns:
            return
        table = validate_name(table)
        column = validate_name(column)
        self._conn.execute(
            f"""
            UPDATE {table}
            SET {column} = (
                SELECT keep_id
                FROM object_version_identity_duplicates
                WHERE old_id = {table}.{column}
            )
            WHERE {column} IN (
                SELECT old_id FROM object_version_identity_duplicates
            )
            """
        )

    def _existing_column(
        self,
        columns: dict[str, sqlite3.Row],
        name: str,
        fallback: str | None = None,
    ) -> str:
        if name in columns:
            return validate_name(name)
        if fallback is not None:
            return fallback
        raise RuntimeError(f"object identity migration requires column: {name}")

    def _coalesce_existing_columns(
        self,
        columns: dict[str, sqlite3.Row],
        names: tuple[str, ...],
        *,
        fallback: str,
    ) -> str:
        existing = [validate_name(name) for name in names if name in columns]
        if not existing:
            return fallback
        if len(existing) == 1:
            return existing[0]
        return f"COALESCE({', '.join(existing)})"


class RepositoryBase:
    """Base class for aggregate repositories backed by shared storage."""

    def __init__(self, storage: StorageBase):
        self.storage = storage

    def __getattr__(self, name: str) -> Any:
        return getattr(self.storage, name)
