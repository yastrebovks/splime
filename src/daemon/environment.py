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
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from spl.daemon.store import RegistryStore, utc_now


ABSENT = "absent"
CREATING = "creating"
READY = "ready"
FAILED = "failed"
DEFAULT_BUILD_TIMEOUT_SECONDS = float(
    os.environ.get("SPL_DAEMON_ENV_BUILD_TIMEOUT_SECONDS", "900")
)
DEFAULT_STALE_LOCK_SECONDS = float(
    os.environ.get("SPL_DAEMON_ENV_STALE_LOCK_SECONDS", "1800")
)


class EnvironmentBuildError(RuntimeError):
    """Raised when an environment cannot be prepared for a run."""


class _ExternalBuildReady(RuntimeError):
    """Raised internally when another daemon finished the same build."""


class EnvironmentManager:
    """Build and reuse venvs keyed by dependency specification hash."""

    def __init__(
        self,
        store: RegistryStore,
        *,
        build_timeout_seconds: float = DEFAULT_BUILD_TIMEOUT_SECONDS,
        stale_lock_seconds: float = DEFAULT_STALE_LOCK_SECONDS,
    ):
        self.store = store
        self.build_timeout_seconds = float(build_timeout_seconds)
        self.stale_lock_seconds = float(stale_lock_seconds)
        if self.build_timeout_seconds <= 0:
            raise ValueError("build_timeout_seconds must be positive")
        if self.stale_lock_seconds <= 0:
            raise ValueError("stale_lock_seconds must be positive")
        self._lock = threading.RLock()
        self._conditions: dict[str, threading.Condition] = {}
        self._active_builds: set[str] = set()

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Return the cached build status for an object version."""

        spec = self.build_spec(object_record)
        record = self.store.get_environment_build(spec["spec_hash"])
        if record is None:
            return {
                "spec_hash": spec["spec_hash"],
                "status": ABSENT,
                "venv_path": str(spec["venv_path"]),
                "python_path": str(spec["python_path"]),
                "install_log_path": str(spec["install_log_path"]),
            }
        return record

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        """Start a build when needed and optionally wait until it is ready."""

        spec = self.build_spec(object_record)
        condition = self._condition_for(spec["spec_hash"])
        with condition:
            record = self._record_or_absent(spec)
            if self._is_ready(record):
                return record

            if record["status"] == FAILED:
                if not retry_failed:
                    raise EnvironmentBuildError(
                        "environment build failed: "
                        f"{record.get('error') or record['spec_hash']}"
                    )
                self._start_build_thread(spec, condition)

            if (
                record["status"] == CREATING
                and spec["spec_hash"] not in self._active_builds
                and not self._is_stale_creating(record, spec)
            ):
                pass
            elif spec["spec_hash"] not in self._active_builds:
                self._start_build_thread(spec, condition)

            if not wait:
                return self.store.get_environment_build(spec["spec_hash"]) or record

            while True:
                condition.wait(timeout=5.0)
                record = self._record_or_absent(spec)
                if self._is_ready(record):
                    return record
                if record["status"] == FAILED:
                    raise EnvironmentBuildError(
                        "environment build failed: "
                        f"{record.get('error') or record['spec_hash']}"
                    )
                if (
                    record["status"] == CREATING
                    and spec["spec_hash"] not in self._active_builds
                    and self._is_stale_creating(record, spec)
                ):
                    self._start_build_thread(spec, condition)

    def build_spec(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Build a deterministic venv specification from an object version."""

        base_python = str(Path(object_record["env_python"]).expanduser().absolute())
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

    def rebuild(
        self,
        spec_hash: str,
        *,
        wait: bool,
    ) -> dict[str, Any]:
        """Force a cached environment to be rebuilt from its stored spec."""

        record = self.store.get_environment_build(spec_hash)
        if record is None:
            raise KeyError(f"environment build is not found: {spec_hash}")
        spec = self._spec_from_record(record)
        spec["force_rebuild"] = True
        condition = self._condition_for(spec["spec_hash"])
        with condition:
            if spec["spec_hash"] not in self._active_builds:
                self._start_build_thread(spec, condition)
            if not wait:
                return self.store.get_environment_build(spec_hash) or record

            while True:
                condition.wait(timeout=5.0)
                current = self._record_or_absent(spec)
                if self._is_ready(current):
                    return current
                if current["status"] == FAILED:
                    raise EnvironmentBuildError(
                        "environment rebuild failed: "
                        f"{current.get('error') or current['spec_hash']}"
                    )

    def _start_build_thread(
        self,
        spec: dict[str, Any],
        condition: threading.Condition,
    ) -> None:
        spec_hash = spec["spec_hash"]
        self._active_builds.add(spec_hash)
        self.store.upsert_environment_build(
            spec_hash=spec_hash,
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
        thread = threading.Thread(
            target=self._build_thread_main,
            args=(spec, condition),
            name=f"spl-env-{spec_hash[:12]}",
            daemon=True,
        )
        thread.start()

    def _build_thread_main(
        self,
        spec: dict[str, Any],
        condition: threading.Condition,
    ) -> None:
        spec_hash = spec["spec_hash"]
        try:
            self._build_environment(spec)
            self.store.update_environment_build(
                spec_hash,
                status=READY,
                finished_at=utc_now(),
                error=None,
            )
        except Exception as exc:  # noqa: BLE001 - build errors are persisted.
            self.store.update_environment_build(
                spec_hash,
                status=FAILED,
                finished_at=utc_now(),
                error=repr(exc),
            )
        finally:
            with condition:
                self._active_builds.discard(spec_hash)
                condition.notify_all()

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

    def _record_or_absent(self, spec: dict[str, Any]) -> dict[str, Any]:
        record = self.store.get_environment_build(spec["spec_hash"])
        if record is None:
            return {
                "spec_hash": spec["spec_hash"],
                "status": ABSENT,
                "venv_path": str(spec["venv_path"]),
                "python_path": str(spec["python_path"]),
                "install_log_path": str(spec["install_log_path"]),
            }
        if record["status"] == READY and not Path(record["python_path"]).exists():
            return self.store.update_environment_build(
                spec["spec_hash"],
                status=ABSENT,
                finished_at=None,
                error="cached environment is missing from disk",
            )
        return record

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

    def _is_stale_creating(
        self,
        record: dict[str, Any],
        spec: dict[str, Any],
    ) -> bool:
        started_at = self._parse_timestamp(record.get("started_at"))
        if started_at is not None:
            age = (datetime.now(UTC) - started_at).total_seconds()
            if age > self.stale_lock_seconds:
                return True

        lock_path = Path(spec["lock_path"])
        if lock_path.exists():
            age = time.time() - lock_path.stat().st_mtime
            return age > self.stale_lock_seconds
        return started_at is None

    def _parse_timestamp(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _condition_for(self, spec_hash: str) -> threading.Condition:
        with self._lock:
            if spec_hash not in self._conditions:
                self._conditions[spec_hash] = threading.Condition(self._lock)
            return self._conditions[spec_hash]

    def _normalize_distributions(
        self,
        distributions: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        return sorted(
            [
                {
                    "package": str(item["package"]).casefold(),
                    "version": str(item["version"]),
                }
                for item in distributions
            ],
            key=lambda item: (item["package"], item["version"]),
        )

    def _runtime_packages(
        self,
        distributions: list[dict[str, str]],
    ) -> list[dict[str, str | None]]:
        return self.store.environment_runtime_packages_for(distributions)

    def _requirements(self, spec: dict[str, Any]) -> list[str]:
        requirements = []
        for item in [*spec["runtime_packages"], *spec["distributions"]]:
            if item.get("version") is None:
                requirements.append(item["package"])
            else:
                requirements.append(f"{item['package']}=={item['version']}")
        return requirements

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

    @contextmanager
    def _build_lock(self, spec: dict[str, Any]) -> Iterator[None]:
        lock_path = Path(spec["lock_path"])
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.build_timeout_seconds
        fd: int | None = None
        while fd is None:
            if not spec.get("force_rebuild", False):
                current = self.store.get_environment_build(spec["spec_hash"])
                if current is not None and self._is_ready(current):
                    raise _ExternalBuildReady()
            try:
                fd = os.open(
                    str(lock_path),
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
                os.write(
                    fd,
                    (
                        f"pid={os.getpid()}\n"
                        f"created_at={datetime.now(UTC).isoformat()}\n"
                    ).encode("utf-8"),
                )
                break
            except FileExistsError:
                if self._lock_file_is_stale(lock_path):
                    try:
                        lock_path.unlink()
                    except FileNotFoundError:
                        pass
                    continue

                current = self.store.get_environment_build(spec["spec_hash"])
                if current is not None and self._is_ready(current):
                    raise _ExternalBuildReady()
                if time.monotonic() >= deadline:
                    raise EnvironmentBuildError(
                        "timed out waiting for environment build lock: "
                        f"{lock_path}"
                    )
                time.sleep(1.0)

        try:
            yield
        finally:
            if fd is not None:
                os.close(fd)
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass

    def _lock_file_is_stale(self, lock_path: Path) -> bool:
        try:
            age = time.time() - lock_path.stat().st_mtime
        except FileNotFoundError:
            return False
        return age > self.stale_lock_seconds

    def _run_logged(self, command: list[str], log: Any) -> None:
        log.write("\n$ " + " ".join(command) + "\n")
        log.flush()
        try:
            completed = subprocess.run(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=self.build_timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise EnvironmentBuildError(
                "command timed out after "
                f"{self.build_timeout_seconds} seconds: {command[0]}"
            ) from exc
        if completed.returncode != 0:
            raise EnvironmentBuildError(
                f"command failed with exit code {completed.returncode}: {command[0]}"
            )

    def _assert_daemon_environment_path(self, venv_path: Path) -> None:
        root = self.store.environment_builds_dir.resolve()
        target = venv_path.resolve()
        if root != target and root not in target.parents:
            raise EnvironmentBuildError(
                f"refusing to modify venv outside daemon home: {target}"
            )
