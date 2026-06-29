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
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS envs (
                    name TEXT PRIMARY KEY,
                    python TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS objects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    kind TEXT,
                    origin TEXT NOT NULL DEFAULT 'local',
                    remote_owner_id TEXT,
                    remote_object_id TEXT,
                    remote_name TEXT,
                    description TEXT NOT NULL DEFAULT '',
                    current_version_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
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
                    UNIQUE(object_id, version)
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
            self._ensure_column("objects", "origin", "TEXT NOT NULL DEFAULT 'local'")
            self._ensure_column("objects", "remote_owner_id", "TEXT")
            self._ensure_column("objects", "remote_object_id", "TEXT")
            self._ensure_column("objects", "remote_name", "TEXT")
            self._ensure_column("object_versions", "remote_owner_id", "TEXT")
            self._ensure_column("object_versions", "remote_object_id", "TEXT")
            self._ensure_column("object_versions", "remote_version_id", "TEXT")
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
        rows = self._conn.execute(f"PRAGMA table_info({validate_name(table)})").fetchall()
        return {row["name"] for row in rows}

    def _row_value(self, row: sqlite3.Row, name: str) -> Any:
        return row[name] if name in row.keys() else None

    def _ensure_column(self, table: str, column: str, declaration: str) -> None:
        existing = {
            row["name"]
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in existing:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")


class RepositoryBase:
    """Base class for aggregate repositories backed by shared storage."""

    def __init__(self, storage: StorageBase):
        self.storage = storage

    def __getattr__(self, name: str) -> Any:
        return getattr(self.storage, name)
