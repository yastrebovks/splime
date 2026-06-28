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

from spl.daemon.runtime_config import normalize_runtime_config
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


class RegistryStore:
    """SQLite-backed registry for daemon state.

    The HTTP server may handle several requests at once.  SQLite serializes
    writes, and the in-process lock keeps compound operations such as "create
    object version and mark it current" easy to reason about.
    """

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
        self.bootstrap()

    def bootstrap(self) -> None:
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
            self._migrate_legacy_registry()
            self._migrate_server_connection_secrets_locked()
            self._backfill_object_kinds_locked()
            self._backfill_object_decomposition_locked()
            self._conn.commit()

    def close(self) -> None:
        """Close the SQLite connection held by this store."""

        self._conn.close()

    def __enter__(self) -> "RegistryStore":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _secret_key(self, connection_id: str, name: str) -> str:
        return f"server-connections/{connection_id}/{name}"

    def _table_columns_locked(self, table: str) -> set[str]:
        rows = self._conn.execute(f"PRAGMA table_info({validate_name(table)})").fetchall()
        return {row["name"] for row in rows}

    def _row_value(self, row: sqlite3.Row, name: str) -> Any:
        return row[name] if name in row.keys() else None

    def _store_server_connection_secrets(
        self,
        connection_id: str,
        *,
        token: str,
        user_token: str | None,
    ) -> tuple[str, str | None]:
        token_ref = self.secret_store.put(
            self._secret_key(connection_id, "machine-token"),
            token,
        )
        user_token_ref = None
        if user_token:
            try:
                user_token_ref = self.secret_store.put(
                    self._secret_key(connection_id, "user-token"),
                    user_token,
                )
            except Exception:
                self.secret_store.delete(token_ref)
                raise
        return token_ref, user_token_ref

    def _delete_server_connection_secrets(
        self,
        token_ref: str | None,
        user_token_ref: str | None,
    ) -> None:
        self.secret_store.delete(token_ref)
        self.secret_store.delete(user_token_ref)

    def _delete_server_connection_secret_rows(
        self,
        rows: list[sqlite3.Row],
    ) -> None:
        for row in rows:
            self._delete_server_connection_secrets(
                row["token_secret_ref"],
                row["user_token_secret_ref"],
            )

    def _replace_active_server_connections_locked(
        self,
        now: str,
    ) -> list[sqlite3.Row]:
        rows = self._conn.execute(
            """
            SELECT token_secret_ref, user_token_secret_ref
            FROM server_connections
            WHERE status IN ('connected', 'heartbeat_failed', 'connect_failed')
            """
        ).fetchall()
        self._conn.execute(
            """
            UPDATE server_connections
            SET status = 'replaced',
                disconnected_at = :now,
                updated_at = :now,
                token_secret_ref = NULL,
                user_token_secret_ref = NULL
            WHERE status IN ('connected', 'heartbeat_failed', 'connect_failed')
            """,
            {"now": now},
        )
        return rows

    def _migrate_server_connection_secrets_locked(self) -> None:
        columns = self._table_columns_locked("server_connections")
        select_columns = [
            "id",
            "token_secret_ref",
            "user_token_secret_ref",
            "token_redacted",
            "user_token_redacted",
        ]
        if "token" in columns:
            select_columns.append("token")
        if "user_token" in columns:
            select_columns.append("user_token")
        rows = self._conn.execute(
            f"SELECT {', '.join(select_columns)} FROM server_connections"
        ).fetchall()
        for row in rows:
            token_ref = row["token_secret_ref"]
            user_token_ref = row["user_token_secret_ref"]
            token_value = self._row_value(row, "token")
            user_token_value = self._row_value(row, "user_token")
            if not token_ref and token_value and token_value != REDACTED_SECRET_VALUE:
                token_ref = self.secret_store.put(
                    self._secret_key(row["id"], "machine-token"),
                    token_value,
                )
            if (
                not user_token_ref
                and user_token_value
                and user_token_value != REDACTED_SECRET_VALUE
            ):
                user_token_ref = self.secret_store.put(
                    self._secret_key(row["id"], "user-token"),
                    user_token_value,
                )
            if token_ref != row["token_secret_ref"] or user_token_ref != row["user_token_secret_ref"]:
                assignments = [
                    "token_redacted = ?",
                    "user_token_redacted = ?",
                    "token_secret_ref = ?",
                    "user_token_secret_ref = ?",
                ]
                values: list[Any] = [
                    REDACTED_SECRET_VALUE,
                    REDACTED_SECRET_VALUE if user_token_ref else None,
                    token_ref,
                    user_token_ref,
                ]
                if "token" in columns:
                    assignments.append("token = ?")
                    values.append(REDACTED_SECRET_VALUE)
                if "user_token" in columns:
                    assignments.append("user_token = ?")
                    values.append(REDACTED_SECRET_VALUE if user_token_ref else None)
                values.append(row["id"])
                self._conn.execute(
                    f"""
                    UPDATE server_connections
                    SET {', '.join(assignments)}
                    WHERE id = ?
                    """,
                    values,
                )

    def _insert_server_connection_locked(self, values: dict[str, Any]) -> None:
        columns = [
            "id",
            "server_url",
            "token_hint",
            "user_token_hint",
            "token_secret_ref",
            "user_token_secret_ref",
            "token_redacted",
            "user_token_redacted",
            "remote_connection_id",
            "owner_id",
            "subject_type",
            "subject_id",
            "machine_id",
            "display_name",
            "capabilities_json",
            "status",
            "heartbeat_interval_seconds",
            "last_heartbeat_at",
            "next_heartbeat_at",
            "lease_expires_at",
            "last_library_snapshot_hash",
            "last_library_snapshot_at",
            "created_at",
            "connected_at",
            "disconnected_at",
            "updated_at",
            "error",
        ]
        params = dict(values)
        table_columns = self._table_columns_locked("server_connections")
        if "token" in table_columns:
            columns.insert(2, "token")
            params["token"] = REDACTED_SECRET_VALUE
        if "user_token" in table_columns:
            columns.insert(columns.index("user_token_hint"), "user_token")
            params["user_token"] = (
                REDACTED_SECRET_VALUE if params.get("user_token_secret_ref") else None
            )
        placeholders = [f":{column}" for column in columns]
        self._conn.execute(
            f"""
            INSERT INTO server_connections(
                {', '.join(columns)}
            )
            VALUES(
                {', '.join(placeholders)}
            )
            """,
            params,
        )

    def register_env(self, name: str, python: str) -> dict[str, Any]:
        """Register or update a named Python interpreter."""

        name = validate_name(name)
        python_path = Path(python).expanduser().absolute()
        if not python_path.exists():
            raise ValueError(f"python executable is not found: {python_path}")

        now = utc_now()
        with self._lock, self._conn:
            existing = self._conn.execute(
                "SELECT created_at FROM envs WHERE name = ?",
                (name,),
            ).fetchone()
            created_at = existing["created_at"] if existing is not None else now
            self._conn.execute(
                """
                INSERT INTO envs(name, python, created_at, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    python = excluded.python,
                    updated_at = excluded.updated_at
                """,
                (name, str(python_path), created_at, now),
            )
        return self.get_env(name)

    def list_envs(self) -> dict[str, Any]:
        """Return registered environments keyed by name."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM envs ORDER BY name"
            ).fetchall()
        return {row["name"]: dict(row) for row in rows}

    def get_env(self, name: str) -> dict[str, Any]:
        """Return one registered environment or raise a clear error."""

        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM envs WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            raise KeyError(f"environment is not registered: {name}")
        return dict(row)

    def save_server_connection(
        self,
        *,
        server_url: str,
        token: str,
        user_token: str,
        connection: dict[str, Any],
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Persist a successful central-server connection in the local DB.

        Tokens are stored through the daemon secret store so a future long-lived
        connector can heartbeat or poll jobs without asking user code to pass
        the token again.  API responses expose only ``token_hint``.
        """

        machine_id = validate_name(connection["machine_id"])
        interval = normalize_heartbeat_interval(
            heartbeat_interval_seconds
            if heartbeat_interval_seconds is not None
            else connection.get("heartbeat_interval_seconds")
        )
        connection_id = uuid4().hex
        now = utc_now()
        token_hint = f"...{token[-6:]}"
        user_token_hint = f"...{user_token[-6:]}"
        token_ref, user_token_ref = self._store_server_connection_secrets(
            connection_id,
            token=token,
            user_token=user_token,
        )
        try:
            with self._lock, self._conn:
                replaced_secret_rows = self._replace_active_server_connections_locked(now)
                self._insert_server_connection_locked(
                    {
                        "id": connection_id,
                        "server_url": server_url.rstrip("/"),
                        "token_hint": token_hint,
                        "user_token_hint": user_token_hint,
                        "token_secret_ref": token_ref,
                        "user_token_secret_ref": user_token_ref,
                        "token_redacted": REDACTED_SECRET_VALUE,
                        "user_token_redacted": REDACTED_SECRET_VALUE,
                        "remote_connection_id": connection.get("id"),
                        "owner_id": connection.get("owner_id"),
                        "subject_type": connection.get("subject_type"),
                        "subject_id": connection.get("subject_id"),
                        "machine_id": machine_id,
                        "display_name": connection.get("display_name"),
                        "capabilities_json": json_dumps(connection.get("capabilities") or {}),
                        "status": connection.get("status") or "connected",
                        "heartbeat_interval_seconds": interval,
                        "last_heartbeat_at": connection.get("last_seen_at") or now,
                        "next_heartbeat_at": iso_after_now(interval),
                        "lease_expires_at": connection.get("expires_at"),
                        "last_library_snapshot_hash": None,
                        "last_library_snapshot_at": None,
                        "created_at": now,
                        "connected_at": connection.get("connected_at") or now,
                        "disconnected_at": connection.get("disconnected_at"),
                        "updated_at": now,
                        "error": None,
                    },
                )
        except Exception:
            self._delete_server_connection_secrets(token_ref, user_token_ref)
            raise
        self._delete_server_connection_secret_rows(replaced_secret_rows)
        return self.get_server_connection(connection_id)

    def save_pending_server_connection(
        self,
        *,
        server_url: str,
        token: str,
        user_token: str,
        machine_id: str,
        display_name: str | None = None,
        capabilities: dict[str, Any] | None = None,
        heartbeat_interval_seconds: float | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Persist an offline server connection attempt for later reconnect."""

        machine_id = validate_name(machine_id)
        interval = normalize_heartbeat_interval(heartbeat_interval_seconds)
        connection_id = uuid4().hex
        now = utc_now()
        token_hint = f"...{token[-6:]}"
        user_token_hint = f"...{user_token[-6:]}"
        token_ref, user_token_ref = self._store_server_connection_secrets(
            connection_id,
            token=token,
            user_token=user_token,
        )
        try:
            with self._lock, self._conn:
                replaced_secret_rows = self._replace_active_server_connections_locked(now)
                self._insert_server_connection_locked(
                    {
                        "id": connection_id,
                        "server_url": server_url.rstrip("/"),
                        "token_hint": token_hint,
                        "user_token_hint": user_token_hint,
                        "token_secret_ref": token_ref,
                        "user_token_secret_ref": user_token_ref,
                        "token_redacted": REDACTED_SECRET_VALUE,
                        "user_token_redacted": REDACTED_SECRET_VALUE,
                        "remote_connection_id": None,
                        "owner_id": None,
                        "subject_type": "machine",
                        "subject_id": machine_id,
                        "machine_id": machine_id,
                        "display_name": display_name or machine_id,
                        "capabilities_json": json_dumps(capabilities or {}),
                        "status": "connect_failed",
                        "heartbeat_interval_seconds": interval,
                        "last_heartbeat_at": None,
                        "next_heartbeat_at": iso_after_now(interval),
                        "lease_expires_at": None,
                        "last_library_snapshot_hash": None,
                        "last_library_snapshot_at": None,
                        "created_at": now,
                        "connected_at": None,
                        "disconnected_at": None,
                        "updated_at": now,
                        "error": error,
                    },
                )
        except Exception:
            self._delete_server_connection_secrets(token_ref, user_token_ref)
            raise
        self._delete_server_connection_secret_rows(replaced_secret_rows)
        return self.get_server_connection(connection_id)

    def complete_server_connection(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any],
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Turn a pending local connection row into a live server lease."""

        connection_id = validate_name(connection_id)
        machine_id = validate_name(remote_connection["machine_id"])
        interval = normalize_heartbeat_interval(
            heartbeat_interval_seconds
            if heartbeat_interval_seconds is not None
            else remote_connection.get("heartbeat_interval_seconds")
        )
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET remote_connection_id = ?,
                    owner_id = ?,
                    subject_type = ?,
                    subject_id = ?,
                    machine_id = ?,
                    display_name = ?,
                    capabilities_json = ?,
                    status = ?,
                    heartbeat_interval_seconds = ?,
                    last_heartbeat_at = ?,
                    next_heartbeat_at = ?,
                    lease_expires_at = ?,
                    connected_at = COALESCE(connected_at, ?),
                    disconnected_at = NULL,
                    updated_at = ?,
                    error = NULL
                WHERE id = ?
                """,
                (
                    remote_connection.get("id"),
                    remote_connection.get("owner_id"),
                    remote_connection.get("subject_type"),
                    remote_connection.get("subject_id"),
                    machine_id,
                    remote_connection.get("display_name"),
                    json_dumps(remote_connection.get("capabilities") or {}),
                    remote_connection.get("status") or "connected",
                    interval,
                    remote_connection.get("last_seen_at") or now,
                    iso_after_now(interval),
                    remote_connection.get("expires_at"),
                    remote_connection.get("connected_at") or now,
                    now,
                    connection_id,
                ),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self.get_server_connection(connection_id)

    def get_server_connection(self, connection_id: str) -> dict[str, Any]:
        """Return one stored central-server connection by local id."""

        connection_id = validate_name(connection_id)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM server_connections WHERE id = ?",
                (connection_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self._server_connection_row(row)

    def get_server_connection_credentials(self, connection_id: str) -> dict[str, Any]:
        """Return one stored connection including the token for daemon internals."""

        connection_id = validate_name(connection_id)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM server_connections WHERE id = ?",
                (connection_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self._server_connection_secret_row(row)

    def current_server_connection(self) -> dict[str, Any] | None:
        """Return the newest active central-server connection, if any."""

        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM server_connections
                WHERE status IN ('connected', 'heartbeat_failed', 'connect_failed')
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return self._server_connection_row(row)

    def current_server_connection_credentials(self) -> dict[str, Any] | None:
        """Return the newest active connection including token for daemon internals."""

        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM server_connections
                WHERE status IN ('connected', 'heartbeat_failed', 'connect_failed')
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return self._server_connection_secret_row(row)

    def list_server_connections(self) -> list[dict[str, Any]]:
        """Return stored central-server connection attempts, newest first."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM server_connections ORDER BY updated_at DESC"
            ).fetchall()
        return [self._server_connection_row(row) for row in rows]

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

    def record_server_connection_heartbeat(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any],
    ) -> dict[str, Any]:
        """Persist a successful connection heartbeat."""

        connection_id = validate_name(connection_id)
        interval = normalize_heartbeat_interval(
            remote_connection.get("heartbeat_interval_seconds")
        )
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET status = ?,
                    heartbeat_interval_seconds = ?,
                    last_heartbeat_at = ?,
                    next_heartbeat_at = ?,
                    lease_expires_at = ?,
                    updated_at = ?,
                    error = NULL
                WHERE id = ?
                """,
                (
                    remote_connection.get("status") or "connected",
                    interval,
                    remote_connection.get("last_seen_at") or now,
                    iso_after_now(interval),
                    remote_connection.get("expires_at"),
                    now,
                    connection_id,
                ),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self.get_server_connection(connection_id)

    def record_server_connection_library_snapshot(
        self,
        connection_id: str,
        *,
        snapshot_hash: str,
    ) -> dict[str, Any]:
        """Remember the last full library snapshot acknowledged by the server."""

        connection_id = validate_name(connection_id)
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET last_library_snapshot_hash = ?,
                    last_library_snapshot_at = ?,
                    updated_at = ?,
                    error = NULL
                WHERE id = ?
                """,
                (snapshot_hash, now, now, connection_id),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self.get_server_connection(connection_id)

    def record_server_connection_error(
        self,
        connection_id: str,
        *,
        status: str,
        error: str,
    ) -> dict[str, Any]:
        """Persist a heartbeat/connectivity error for diagnostics."""

        connection_id = validate_name(connection_id)
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET status = ?,
                    updated_at = ?,
                    error = ?
                WHERE id = ?
                """,
                (status, now, error, connection_id),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        return self.get_server_connection(connection_id)

    def mark_server_connection_disconnected(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Mark a local server connection as gracefully disconnected."""

        connection_id = validate_name(connection_id)
        now = utc_now()
        with self._lock, self._conn:
            secret_rows = self._conn.execute(
                """
                SELECT token_secret_ref, user_token_secret_ref
                FROM server_connections
                WHERE id = ?
                """,
                (connection_id,),
            ).fetchall()
            cursor = self._conn.execute(
                """
                UPDATE server_connections
                SET status = 'disconnected',
                    disconnected_at = ?,
                    lease_expires_at = ?,
                    updated_at = ?,
                    error = NULL,
                    token_secret_ref = NULL,
                    user_token_secret_ref = NULL
                WHERE id = ?
                """,
                (
                    (remote_connection or {}).get("disconnected_at") or now,
                    (remote_connection or {}).get("expires_at"),
                    now,
                    connection_id,
                ),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"server connection is not found: {connection_id}")
        self._delete_server_connection_secret_rows(secret_rows)
        return self.get_server_connection(connection_id)

    def get_environment_build(self, spec_hash: str) -> dict[str, Any] | None:
        """Return one cached environment build record by hash."""

        spec_hash = validate_name(spec_hash)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM environment_builds WHERE spec_hash = ?",
                (spec_hash,),
            ).fetchone()
        if row is None:
            return None
        return self._environment_build_row_to_record(row)

    def list_environment_builds(self) -> list[dict[str, Any]]:
        """Return known environment builds, newest updates first."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM environment_builds ORDER BY updated_at DESC"
            ).fetchall()
        return [self._environment_build_row_to_record(row) for row in rows]

    def upsert_environment_build(
        self,
        *,
        spec_hash: str,
        base_python: str,
        python_version: str,
        distributions: list[dict[str, Any]],
        runtime_packages: list[dict[str, Any]],
        spec: dict[str, Any],
        venv_path: Path,
        python_path: Path,
        install_log_path: Path,
        status: str,
        runtime_type: str = "venv",
        image_tag: str | None = None,
        base_image: str | None = None,
    ) -> dict[str, Any]:
        """Create or update an environment build record."""

        spec_hash = validate_name(spec_hash)
        now = utc_now()
        with self._lock, self._conn:
            existing = self._conn.execute(
                "SELECT created_at FROM environment_builds WHERE spec_hash = ?",
                (spec_hash,),
            ).fetchone()
            created_at = existing["created_at"] if existing is not None else now
            self._conn.execute(
                """
                INSERT INTO environment_builds(
                    spec_hash, base_python, python_version, distributions_json,
                    runtime_packages_json, spec_json, venv_path, python_path,
                    status, created_at, updated_at, install_log_path,
                    runtime_type, image_tag, base_image
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(spec_hash) DO UPDATE SET
                    base_python = excluded.base_python,
                    python_version = excluded.python_version,
                    distributions_json = excluded.distributions_json,
                    runtime_packages_json = excluded.runtime_packages_json,
                    spec_json = excluded.spec_json,
                    venv_path = excluded.venv_path,
                    python_path = excluded.python_path,
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    install_log_path = excluded.install_log_path,
                    runtime_type = excluded.runtime_type,
                    image_tag = excluded.image_tag,
                    base_image = excluded.base_image
                """,
                (
                    spec_hash,
                    base_python,
                    python_version,
                    json_dumps(distributions),
                    json_dumps(runtime_packages),
                    json_dumps(spec),
                    str(venv_path),
                    str(python_path),
                    status,
                    created_at,
                    now,
                    str(install_log_path),
                    runtime_type,
                    image_tag,
                    base_image,
                ),
            )
        record = self.get_environment_build(spec_hash)
        if record is None:
            raise KeyError(f"environment build is not found: {spec_hash}")
        return record

    def update_environment_build(
        self,
        spec_hash: str,
        *,
        status: str,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Update the lifecycle status for an environment build."""

        spec_hash = validate_name(spec_hash)
        now = utc_now()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE environment_builds
                SET status = ?,
                    updated_at = ?,
                    started_at = CASE
                        WHEN ? IS NOT NULL THEN ?
                        ELSE started_at
                    END,
                    finished_at = ?,
                    error = ?
                WHERE spec_hash = ?
                """,
                (
                    status,
                    now,
                    started_at,
                    started_at,
                    finished_at,
                    error,
                    spec_hash,
                ),
            )
        if cursor.rowcount == 0:
            raise KeyError(f"environment build is not found: {spec_hash}")
        record = self.get_environment_build(spec_hash)
        if record is None:
            raise KeyError(f"environment build is not found: {spec_hash}")
        return record

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

    def create_run(
        self,
        object_name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        object_version_id: str | None = None,
        function: str | None = None,
    ) -> dict[str, Any]:
        """Create a run for an exact object version and persist initial state."""

        object_name, function = split_object_function_ref(object_name, function)
        if object_version_id is not None:
            object_record = self.get_object_version(object_version_id, include_yaml=False)
        else:
            object_record = self.get_object(
                object_name,
                version=version,
                include_yaml=False,
            )
        entrypoint = self._run_entrypoint_for(object_record, function)
        runtime_config = normalize_runtime_config(object_record.get("runtime_config"))

        run_id = uuid4().hex
        run_dir = self.runs_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=False)

        input_payload = {
            "args": args or [],
            "kwargs": kwargs or {},
            "output": output,
            "timeout_seconds": timeout_seconds,
        }
        if function is not None:
            input_payload["function"] = function
        write_json(run_dir / "input.json", input_payload)

        now = utc_now()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO runs(
                    id, object_id, object_version_id, object_name, object_version,
                    entrypoint, env, env_python, status, created_at, run_dir,
                    input_json, result_path, artifacts_dir, env_build_hash,
                    runtime_config_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    object_record["id"],
                    object_record["version_id"],
                    object_record["name"],
                    object_record["version"],
                    entrypoint,
                    object_record["env"],
                    object_record["env_python"],
                    "queued",
                    now,
                    str(run_dir),
                    json_dumps(input_payload),
                    str(run_dir / "result.json"),
                    str(run_dir / "artifacts"),
                    object_record.get("environment_spec_hash"),
                    json_dumps(runtime_config),
                ),
            )

        state = self.get_run(run_id)
        self._write_run_state_file(state)
        return state

    def _run_entrypoint_for(
        self,
        object_record: dict[str, Any],
        function: str | None,
    ) -> str:
        if function is None:
            return object_record["entrypoint"]

        function = validate_name(function)
        for item in object_record.get("functions") or []:
            if item.get("kind") == "function" and item.get("name") == function:
                return function
        for item in object_record.get("internal_objects") or []:
            if item.get("kind") == "function" and item.get("name") == function:
                return function

        available = sorted(
            {
                str(item.get("name"))
                for item in [
                    *(object_record.get("functions") or []),
                    *(object_record.get("internal_objects") or []),
                ]
                if item.get("kind") == "function" and item.get("name")
            }
        )
        raise ValueError(
            f"function is not found in object {object_record['name']}: "
            f"{function}; available: {', '.join(available) or '<none>'}"
        )

    def update_run(self, run_id: str, **changes: Any) -> dict[str, Any]:
        """Merge changes into a run row and return the new state."""

        run_id = validate_name(run_id)
        column_values: dict[str, Any] = {}
        for key, value in changes.items():
            column, stored_value = self._run_change_to_column(key, value)
            column_values[column] = stored_value

        if column_values:
            assignments = ", ".join(f"{column} = ?" for column in column_values)
            values = [*column_values.values(), run_id]
            with self._lock, self._conn:
                cursor = self._conn.execute(
                    f"UPDATE runs SET {assignments} WHERE id = ?",
                    values,
                )
            if cursor.rowcount == 0:
                raise KeyError(f"run is not found: {run_id}")

        state = self.get_run(run_id)
        self._write_run_state_file(state)
        return state

    def get_run(self, run_id: str) -> dict[str, Any]:
        """Read a run state by id."""

        run_id = validate_name(run_id)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"run is not found: {run_id}")
        return self._run_row_to_state(row)

    def list_runs(self) -> list[dict[str, Any]]:
        """Return all known runs, newest first by creation time."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY created_at DESC"
            ).fetchall()
        return [self._run_row_to_state(row) for row in rows]

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

    def _run_row_to_state(self, row: sqlite3.Row) -> dict[str, Any]:
        command = json_loads(row["command_json"], None)
        result = json_loads(row["result_json"], None)
        state = {
            "id": row["id"],
            "object": row["object_name"],
            "object_id": row["object_id"],
            "object_version_id": row["object_version_id"],
            "object_version": row["object_version"],
            "entrypoint": row["entrypoint"],
            "env": row["env"],
            "env_python": row["env_python"],
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "run_dir": row["run_dir"],
            "input": json_loads(row["input_json"], {}),
            "result_path": row["result_path"],
            "result": result,
            "artifacts_dir": row["artifacts_dir"],
            "env_build_hash": row["env_build_hash"],
            "runtime_config": normalize_runtime_config(
                json_loads(row["runtime_config_json"], {"mode": "venv"})
            ),
            "runtime_build_hash": row["runtime_build_hash"],
            "resolved_runtime": row["resolved_runtime"],
            "runtime_backend": row["runtime_backend"],
            "image_tag": row["image_tag"],
            "container_id": row["container_id"],
            "resolved_python": row["resolved_python"],
            "error": row["error"],
            "returncode": row["returncode"],
            "command": command,
            "stdout_path": row["stdout_path"],
            "stderr_path": row["stderr_path"],
            "stdout": row["stdout_text"],
            "stderr": row["stderr_text"],
        }
        return state

    def _server_connection_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "server_url": row["server_url"],
            "token_hint": row["token_hint"],
            "user_token_hint": row["user_token_hint"],
            "remote_connection_id": self._row_value(row, "remote_connection_id"),
            "owner_id": self._row_value(row, "owner_id"),
            "subject_type": self._row_value(row, "subject_type"),
            "subject_id": self._row_value(row, "subject_id"),
            "machine_id": row["machine_id"],
            "display_name": row["display_name"],
            "capabilities": json_loads(row["capabilities_json"], {}),
            "status": row["status"],
            "heartbeat_interval_seconds": row["heartbeat_interval_seconds"],
            "last_heartbeat_at": row["last_heartbeat_at"],
            "next_heartbeat_at": row["next_heartbeat_at"],
            "lease_expires_at": row["lease_expires_at"],
            "last_library_snapshot_hash": row["last_library_snapshot_hash"],
            "last_library_snapshot_at": row["last_library_snapshot_at"],
            "created_at": row["created_at"],
            "connected_at": self._row_value(row, "connected_at"),
            "disconnected_at": self._row_value(row, "disconnected_at"),
            "updated_at": row["updated_at"],
            "error": self._row_value(row, "error"),
        }

    def _server_connection_secret_row(self, row: sqlite3.Row) -> dict[str, Any]:
        record = self._server_connection_row(row)
        record["token"] = (
            self.secret_store.get(row["token_secret_ref"])
            if row["token_secret_ref"]
            else self._row_value(row, "token")
        )
        record["user_token"] = (
            self.secret_store.get(row["user_token_secret_ref"])
            if row["user_token_secret_ref"]
            else self._row_value(row, "user_token")
        )
        return record

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

    def _object_yaml_cache_path(self, name: str, version: int) -> Path:
        return self.objects_dir / validate_name(name) / "versions" / f"{version}.yaml"

    def _run_change_to_column(self, key: str, value: Any) -> tuple[str, Any]:
        aliases = {
            "command": "command_json",
            "input": "input_json",
            "result": "result_json",
            "runtime_config": "runtime_config_json",
        }
        json_columns = {
            "command_json",
            "input_json",
            "result_json",
            "runtime_config_json",
        }
        valid_columns = {
            "status",
            "started_at",
            "finished_at",
            "run_dir",
            "result_path",
            "artifacts_dir",
            "env_build_hash",
            "runtime_config_json",
            "runtime_build_hash",
            "resolved_runtime",
            "runtime_backend",
            "image_tag",
            "container_id",
            "resolved_python",
            "error",
            "returncode",
            "command_json",
            "input_json",
            "result_json",
            "stdout_path",
            "stderr_path",
            "stdout_text",
            "stderr_text",
        }
        column = aliases.get(key, key)
        if column not in valid_columns:
            raise ValueError(f"unknown run state field: {key}")
        if key in aliases:
            return column, json_dumps(value)
        if column in json_columns and not isinstance(value, str):
            return column, json_dumps(value)
        return column, value

    def _write_run_state_file(self, state: dict[str, Any]) -> None:
        """Write a diagnostic state snapshot next to worker files."""

        run_dir = state.get("run_dir")
        if run_dir:
            write_json(Path(run_dir) / "state.json", state)

    def environment_spec_hash_for(
        self,
        base_python: str,
        distributions: list[dict[str, Any]],
        *,
        python_version: str | None = None,
        runtime_packages: list[dict[str, Any]] | None = None,
    ) -> str:
        """Return a stable hash for an interpreter and dependency list."""

        normalized = sorted(
            [
                {
                    "package": str(item["package"]).casefold(),
                    "version": (
                        None
                        if item.get("version") is None
                        else str(item["version"])
                    ),
                }
                for item in distributions
            ],
            key=lambda item: (item["package"], item["version"] or ""),
        )
        runtime = runtime_packages
        if runtime is None:
            runtime = self.environment_runtime_packages_for(normalized)
        runtime_normalized = sorted(
            [
                {
                    "package": str(item["package"]).casefold(),
                    "version": (
                        None
                        if item.get("version") is None
                        else str(item["version"])
                    ),
                }
                for item in runtime
            ],
            key=lambda item: (item["package"], item["version"] or ""),
        )
        spec = {
            "base_python": str(Path(base_python).expanduser().absolute()),
            "python_version": python_version or "unknown",
            "distributions": normalized,
            "runtime_packages": runtime_normalized,
        }
        return hashlib.sha256(json_dumps(spec).encode("utf-8")).hexdigest()

    def environment_runtime_packages_for(
        self,
        distributions: list[dict[str, Any]],
    ) -> list[dict[str, str | None]]:
        """Return daemon runtime packages needed inside worker venvs."""

        packages = {str(item["package"]).casefold() for item in distributions}
        if "pyyaml" in packages:
            return []
        try:
            version = importlib.metadata.version("PyYAML")
        except importlib.metadata.PackageNotFoundError:
            version = None
        return [{"package": "pyyaml", "version": version}]

    def _cached_python_version(self, python: str) -> str:
        path = str(Path(python).expanduser().absolute())
        if path in self._python_version_cache:
            return self._python_version_cache[path]
        try:
            completed = subprocess.run(
                [path, "--version"],
                text=True,
                capture_output=True,
                timeout=5,
                check=False,
            )
            version = (completed.stdout or completed.stderr).strip() or "unknown"
        except Exception:
            version = "unknown"
        self._python_version_cache[path] = version
        return version

    def _environment_build_row_to_record(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "spec_hash": row["spec_hash"],
            "base_python": row["base_python"],
            "python_version": row["python_version"],
            "distributions": json_loads(row["distributions_json"], []),
            "runtime_packages": json_loads(row["runtime_packages_json"], []),
            "spec": json_loads(row["spec_json"], {}),
            "venv_path": row["venv_path"],
            "python_path": row["python_path"],
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "error": row["error"],
            "install_log_path": row["install_log_path"],
            "runtime_type": row["runtime_type"],
            "image_tag": row["image_tag"],
            "base_image": row["base_image"],
        }

    def _ensure_column(self, table: str, column: str, declaration: str) -> None:
        existing = {
            row["name"]
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in existing:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

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
