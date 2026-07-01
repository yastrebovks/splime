"""EnvRepository aggregate storage."""

from __future__ import annotations

import hashlib
import importlib.metadata
import sqlite3
import subprocess
import sys
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



class EnvRepository(RepositoryBase):
    """Persist and query env aggregate records."""

    def register_env(self, name: str, python: str | None = None) -> dict[str, Any]:
        """Register or update a named Python interpreter."""

        python = python or sys.executable
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
