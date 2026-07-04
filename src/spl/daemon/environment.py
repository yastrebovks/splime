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
from typing import Any

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
from spl.daemon.store import utc_now


class EnvironmentManager(BaseEnvironmentManager):
    """Build and reuse venvs keyed by dependency specification hash."""

    def build_spec(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Build a deterministic venv specification from an object version."""

        base_python = self._base_python_for_object(object_record)
        distributions = self._normalize_distributions(object_record["distributions"])
        runtime_packages = self._runtime_packages(distributions)
        python_version = self._python_version(base_python)
        spec_hash = self.store.environment_spec_hash_for(
            base_python,
            distributions,
            python_version=python_version,
            runtime_packages=runtime_packages,
        )
        env_dir = self.store.environment_builds_dir / spec_hash
        venv_path = env_dir / "venv"
        python_path = self._venv_python_path(venv_path)
        return {
            "spec_hash": spec_hash,
            "base_python": base_python,
            "python_version": python_version,
            "distributions": distributions,
            "runtime_packages": runtime_packages,
            "spec": {
                "base_python": base_python,
                "python_version": python_version,
                "distributions": distributions,
                "runtime_packages": runtime_packages,
                "build_timeout_seconds": self.build_timeout_seconds,
            },
            "venv_path": venv_path,
            "python_path": python_path,
            "install_log_path": env_dir / "install.log",
            "lock_path": env_dir / "build.lock",
            "force_rebuild": False,
        }

    def _base_python_for_object(self, object_record: dict[str, Any]) -> str:
        stored_python = Path(str(object_record["env_python"])).expanduser().absolute()
        if stored_python.exists():
            return str(stored_python)

        env_name = object_record.get("env")
        if env_name:
            try:
                current_env = self.store.get_env(str(env_name))
            except KeyError:
                current_env = None
            if current_env is not None:
                env_python = Path(str(current_env.get("python"))).expanduser().absolute()
                if env_python.exists():
                    return str(env_python)

        if object_record.get("origin") == "server":
            try:
                default_env = self.store.get_env("default")
            except KeyError:
                default_env = None
            if default_env is not None:
                default_python = Path(
                    str(default_env.get("python"))
                ).expanduser().absolute()
                if default_python.exists():
                    return str(default_python)
            executable = Path(sys.executable).expanduser().absolute()
            if executable.exists():
                return str(executable)

        return str(stored_python)

    def _build_environment(self, spec: dict[str, Any]) -> None:
        env_dir = Path(spec["venv_path"]).parent
        venv_path = Path(spec["venv_path"])
        install_log_path = Path(spec["install_log_path"])
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
                    log.write(f"Creating venv: {venv_path}\n")
                    log.write(
                        f"Build timeout: {self.build_timeout_seconds} seconds\n"
                    )
                    self._run_logged(
                        [spec["base_python"], "-m", "venv", str(venv_path)],
                        log,
                    )

                    if requirements:
                        log.write("\nInstalling requirements:\n")
                        for requirement in requirements:
                            log.write(f"  {requirement}\n")
                        self._run_logged(
                            [
                                str(spec["python_path"]),
                                "-m",
                                "pip",
                                "install",
                                "--disable-pip-version-check",
                                *requirements,
                            ],
                            log,
                        )
        except _ExternalBuildReady:
            return

    def _spec_from_record(self, record: dict[str, Any]) -> dict[str, Any]:
        spec = dict(record["spec"])
        env_dir = Path(record["venv_path"]).parent
        return {
            "spec_hash": record["spec_hash"],
            "base_python": record["base_python"],
            "python_version": record["python_version"],
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
        )

    def _build_thread_name(self, spec_hash: str) -> str:
        return f"spl-env-{spec_hash[:12]}"

    def _absent_record(self, spec: dict[str, Any]) -> dict[str, Any]:
        return {
            "spec_hash": spec["spec_hash"],
            "status": ABSENT,
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
        return (
            "environment build failed: "
            f"{record.get('error') or record['spec_hash']}"
        )

    def _rebuild_failed_message(self, record: dict[str, Any]) -> str:
        return (
            "environment rebuild failed: "
            f"{record.get('error') or record['spec_hash']}"
        )

    def _build_lock_timeout_message(self, lock_path: Path) -> str:
        return f"timed out waiting for environment build lock: {lock_path}"

    def _command_timeout_message(self, command: list[str]) -> str:
        return (
            "command timed out after "
            f"{self.build_timeout_seconds} seconds: {command[0]}"
        )

    def _command_failed_message(self, command: list[str], returncode: int) -> str:
        return f"command failed with exit code {returncode}: {command[0]}"

    def _path_refusal_message(self, target: Path) -> str:
        return f"refusing to modify venv outside daemon home: {target}"

    def _python_version(self, python: str) -> str:
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
