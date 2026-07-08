"""Cached virtual environment builder for daemon runs.

Object versions describe their package requirements through SPL distribution
metadata.  The daemon turns that metadata into a stable environment hash and
stores the venv under the daemon home directory, so environments are independent
from the project that exported the object.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from threading import RLock
from typing import Any, Protocol

from spl.daemon.environment_base import (
    ABSENT,
    CREATING,
    READY,
    BaseEnvironmentManager,
    _ExternalBuildReady,
)

# Compatibility re-exports: ``spl.daemon.server`` and older callers import
# these names from ``spl.daemon.environment``.  The redundant ``X as X`` alias
# marks each one as an explicit re-export, so "unused import" autofixes
# (ruff F401) can never strip them again.
from spl.daemon.environment_base import (
    DEFAULT_BUILD_TIMEOUT_SECONDS as DEFAULT_BUILD_TIMEOUT_SECONDS,
    DEFAULT_STALE_LOCK_SECONDS as DEFAULT_STALE_LOCK_SECONDS,
    FAILED as FAILED,
    EnvironmentBuildError as EnvironmentBuildError,
)
from spl.daemon.interpreter_visibility import INTERPRETER_RESOLUTION_KEY
from spl.daemon.store import RegistryStore, utc_now

__all__ = [
    "ABSENT",
    "CREATING",
    "DEFAULT_BUILD_TIMEOUT_SECONDS",
    "DEFAULT_STALE_LOCK_SECONDS",
    "EnvironmentBuildError",
    "EnvironmentManager",
    "FAILED",
    "READY",
]


class _EnvironmentBuilderProtocol(Protocol):
    """Command strategy for constructing one cached Python environment."""

    name: str

    def create_command(self, spec: dict[str, Any]) -> list[str]:
        """Return the command that creates the empty environment."""
        ...

    def install_command(
        self,
        spec: dict[str, Any],
        requirements: list[str],
    ) -> list[str]:
        """Return the command that installs requirements into the environment."""
        ...


class _PipEnvironmentBuilder:
    """Build a venv with the stdlib venv module and pip."""

    name = "pip"

    def create_command(self, spec: dict[str, Any]) -> list[str]:
        return [str(spec["base_python"]), "-m", "venv", str(spec["venv_path"])]

    def install_command(
        self,
        spec: dict[str, Any],
        requirements: list[str],
    ) -> list[str]:
        return [
            str(spec["python_path"]),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            *requirements,
        ]


class _UvEnvironmentBuilder:
    """Build a relocatable venv with uv and install requirements with uv pip."""

    name = "uv"

    def __init__(self, executable: str):
        self.executable = executable

    def create_command(self, spec: dict[str, Any]) -> list[str]:
        return [
            self.executable,
            "venv",
            "--relocatable",
            "--python",
            str(spec["base_python"]),
            str(spec["venv_path"]),
        ]

    def install_command(
        self,
        spec: dict[str, Any],
        requirements: list[str],
    ) -> list[str]:
        return [
            self.executable,
            "pip",
            "install",
            "--strict",
            "--python",
            str(spec["python_path"]),
            *requirements,
        ]


class EnvironmentManager(BaseEnvironmentManager):
    """Build and reuse venvs keyed by dependency specification hash."""

    def __init__(
        self,
        store: RegistryStore,
        *,
        build_timeout_seconds: float = DEFAULT_BUILD_TIMEOUT_SECONDS,
        stale_lock_seconds: float = DEFAULT_STALE_LOCK_SECONDS,
    ) -> None:
        super().__init__(
            store,
            build_timeout_seconds=build_timeout_seconds,
            stale_lock_seconds=stale_lock_seconds,
        )
        uv_executable = shutil.which("uv")
        self._builders: dict[str, _EnvironmentBuilderProtocol] = {
            "pip": _PipEnvironmentBuilder(),
            "uv": _UvEnvironmentBuilder(uv_executable or "uv"),
        }
        self._default_builder = self._builders["uv" if uv_executable else "pip"]
        self._python_version_cache: dict[tuple[str, int | None], str] = {}
        self._python_version_cache_lock = RLock()

    def build_spec(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Build a deterministic venv specification from an object version."""

        interpreter_resolution = self._resolve_interpreter(object_record)
        base_python = str(interpreter_resolution["resolved_python"])
        distributions = self._normalize_distributions(object_record["distributions"])
        runtime_packages = self._runtime_packages(distributions)
        python_version = str(interpreter_resolution["resolved_python_version"])
        builder = self._default_builder.name
        # Builder is part of the spec hash: uv relocatable venvs and stdlib
        # venv+pip builds have different layouts and must not share a cache.
        spec_hash = self.store.environment_spec_hash_for(
            base_python,
            distributions,
            python_version=python_version,
            runtime_packages=runtime_packages,
            builder=builder,
        )
        env_dir = self.store.environment_builds_dir / spec_hash
        venv_path = env_dir / "venv"
        python_path = self._venv_python_path(venv_path)
        return {
            "spec_hash": spec_hash,
            "base_python": base_python,
            "python_version": python_version,
            "builder": builder,
            "distributions": distributions,
            "runtime_packages": runtime_packages,
            "spec": {
                "base_python": base_python,
                "python_version": python_version,
                "builder": builder,
                "distributions": distributions,
                "runtime_packages": runtime_packages,
                "build_timeout_seconds": self.build_timeout_seconds,
                INTERPRETER_RESOLUTION_KEY: interpreter_resolution,
            },
            "venv_path": venv_path,
            "python_path": python_path,
            "install_log_path": env_dir / "install.log",
            "lock_path": env_dir / "build.lock",
            "force_rebuild": False,
        }

    def _resolve_base_python(self, object_record: dict[str, Any]) -> str:
        """Resolve the local interpreter used to build a venv for an object."""

        return str(self._resolve_interpreter(object_record)["resolved_python"])

    def _resolve_interpreter(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Resolve local interpreter metadata without building an environment."""

        stored_python = Path(str(object_record["env_python"])).expanduser().absolute()
        authored_python = str(stored_python)
        authored_python_version = str(
            object_record.get("env_python_version")
            or object_record.get("authored_python_version")
            or self._python_version(authored_python)
        )
        if object_record.get("origin") != "server":
            resolved_python = authored_python
            resolved_python_version = authored_python_version
            reason = "authored_python"
            reason_detail = None
        else:
            env_name = str(object_record.get("env") or "default")
            env_python = self._registered_python_if_available(env_name)
            if env_python is not None:
                resolved_python = env_python
                reason = "local_env"
                reason_detail = env_name
            else:
                default_python = self._registered_python_if_available("default")
                if default_python is not None:
                    resolved_python = default_python
                    reason = "default_env"
                    reason_detail = "default"
                else:
                    resolved_python = str(Path(sys.executable).expanduser().absolute())
                    reason = "daemon_python"
                    reason_detail = None
            resolved_python_version = self._python_version(resolved_python)

        payload: dict[str, Any] = {
            "authored_python": authored_python,
            "authored_python_version": authored_python_version,
            "resolved_python": resolved_python,
            "resolved_python_version": resolved_python_version,
            "reason": reason,
            "substituted": (
                object_record.get("origin") == "server"
                and (authored_python != resolved_python or authored_python_version != resolved_python_version)
            ),
        }
        if reason_detail is not None:
            payload["reason_detail"] = reason_detail
        return payload

    def _registered_python_if_available(self, name: str) -> str | None:
        try:
            env = self.store.get_env(name)
        except KeyError:
            return None
        python = env.get("python")
        if not python:
            return None
        python_path = Path(str(python)).expanduser().absolute()
        if not python_path.exists():
            return None
        return str(python_path)

    def _build_environment(self, spec: dict[str, Any]) -> None:
        env_dir = Path(spec["venv_path"]).parent
        venv_path = Path(spec["venv_path"])
        install_log_path = Path(spec["install_log_path"])
        builder = self._builder_for_spec(spec)
        self._assert_daemon_environment_path(venv_path)
        env_dir.mkdir(parents=True, exist_ok=True)

        try:
            with self._build_lock(spec):
                self.store.update_environment_build(
                    spec["spec_hash"],
                    status=CREATING,
                    started_at=utc_now(),
                    finished_at=None,
                    error=None,
                )

                if venv_path.exists():
                    shutil.rmtree(venv_path)

                requirements = self._requirements(spec)
                with install_log_path.open("w", encoding="utf-8") as log:
                    log.write(f"Builder: {builder.name}\n")
                    log.write(f"Creating venv: {venv_path}\n")
                    log.write(f"Build timeout: {self.build_timeout_seconds} seconds\n")
                    self._run_logged(builder.create_command(spec), log)

                    if requirements:
                        log.write("\nInstalling requirements:\n")
                        for requirement in requirements:
                            log.write(f"  {requirement}\n")
                        self._run_logged(builder.install_command(spec, requirements), log)
        except _ExternalBuildReady:
            return

    def _builder_for_spec(self, spec: dict[str, Any]) -> _EnvironmentBuilderProtocol:
        builder_name = str(spec.get("builder") or (spec.get("spec") or {}).get("builder") or "pip")
        try:
            return self._builders[builder_name]
        except KeyError as exc:
            raise EnvironmentBuildError(f"unknown environment builder: {builder_name}") from exc

    def _spec_from_record(self, record: dict[str, Any]) -> dict[str, Any]:
        spec = dict(record["spec"])
        builder = str(record.get("builder") or spec.get("builder") or "pip")
        env_dir = Path(record["venv_path"]).parent
        return {
            "spec_hash": record["spec_hash"],
            "base_python": record["base_python"],
            "python_version": record["python_version"],
            "builder": builder,
            "distributions": record["distributions"],
            "runtime_packages": record["runtime_packages"],
            "spec": spec,
            "venv_path": Path(record["venv_path"]),
            "python_path": Path(record["python_path"]),
            "install_log_path": Path(record["install_log_path"]),
            "lock_path": env_dir / "build.lock",
            "force_rebuild": False,
        }

    def _is_ready(self, record: dict[str, Any]) -> bool:
        return record["status"] == READY and Path(record["python_path"]).exists()

    def _validate_rebuild_record(
        self,
        record: dict[str, Any] | None,
        spec_hash: str,
    ) -> dict[str, Any]:
        if record is None:
            raise KeyError(f"environment build is not found: {spec_hash}")
        return record

    def _upsert_creating_record(self, spec: dict[str, Any]) -> dict[str, Any]:
        return self.store.upsert_environment_build(
            spec_hash=spec["spec_hash"],
            base_python=spec["base_python"],
            python_version=spec["python_version"],
            distributions=spec["distributions"],
            runtime_packages=spec["runtime_packages"],
            spec=spec["spec"],
            venv_path=spec["venv_path"],
            python_path=spec["python_path"],
            install_log_path=spec["install_log_path"],
            status=CREATING,
            builder=spec["builder"],
        )

    def _build_thread_name(self, spec_hash: str) -> str:
        return f"spl-env-{spec_hash[:12]}"

    def _absent_record(self, spec: dict[str, Any]) -> dict[str, Any]:
        return {
            "spec_hash": spec["spec_hash"],
            "status": ABSENT,
            "builder": spec["builder"],
            "venv_path": str(spec["venv_path"]),
            "python_path": str(spec["python_path"]),
            "install_log_path": str(spec["install_log_path"]),
        }

    def _ready_record_is_missing(
        self,
        record: dict[str, Any],
        spec: dict[str, Any],
    ) -> bool:
        return not Path(record["python_path"]).exists()

    def _missing_ready_error(self) -> str:
        return "cached environment is missing from disk"

    def _build_failed_message(self, record: dict[str, Any]) -> str:
        return f"environment build failed: {record.get('error') or record['spec_hash']}"

    def _rebuild_failed_message(self, record: dict[str, Any]) -> str:
        return f"environment rebuild failed: {record.get('error') or record['spec_hash']}"

    def _build_lock_timeout_message(self, lock_path: Path) -> str:
        return f"timed out waiting for environment build lock: {lock_path}"

    def _command_timeout_message(self, command: list[str]) -> str:
        return f"command timed out after {self.build_timeout_seconds} seconds: {command[0]}"

    def _command_failed_message(self, command: list[str], returncode: int) -> str:
        return f"command failed with exit code {returncode}: {command[0]}"

    def _path_refusal_message(self, target: Path) -> str:
        return f"refusing to modify venv outside daemon home: {target}"

    def _python_version(self, python: str) -> str:
        python_path = Path(python).expanduser().absolute()
        cache_key = (str(python_path), self._python_mtime_ns(python_path))
        with self._python_version_cache_lock:
            cached = self._python_version_cache.get(cache_key)
            if cached is not None:
                return cached
            version = self._read_python_version(str(python_path))
            self._python_version_cache = {
                key: value for key, value in self._python_version_cache.items() if key[0] != cache_key[0]
            }
            self._python_version_cache[cache_key] = version
            return version

    def _python_mtime_ns(self, python: Path) -> int | None:
        try:
            return python.stat().st_mtime_ns
        except OSError:
            return None

    def _read_python_version(self, python: str) -> str:
        try:
            completed = subprocess.run(
                [python, "--version"],
                text=True,
                capture_output=True,
                timeout=15,
                check=False,
            )
        except Exception:
            return "unknown"
        version = (completed.stdout or completed.stderr).strip()
        return version or "unknown"

    def _venv_python_path(self, venv_path: Path) -> Path:
        if os.name == "nt":
            return venv_path / "Scripts" / "python.exe"
        return venv_path / "bin" / "python"
