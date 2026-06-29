"""Shared contracts and build lifecycle for daemon environments."""

from __future__ import annotations

import os
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator, Protocol

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


class EnvironmentManagerProtocol(Protocol):
    """Public environment-manager surface used by daemon call sites."""

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        """Start a build when needed and optionally wait until it is ready."""
        ...

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Return the cached build status for an object version."""
        ...

    def build_spec(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Build a deterministic environment specification."""
        ...

    def rebuild(self, spec_hash: str, *, wait: bool) -> dict[str, Any]:
        """Force a cached environment to be rebuilt from its stored spec."""
        ...


class BaseEnvironmentManager(ABC):
    """Shared build lifecycle for cached daemon environments."""

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
            return self._absent_record(spec)
        return self._status_record(record, spec)

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
                    raise EnvironmentBuildError(self._build_failed_message(record))
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
                    raise EnvironmentBuildError(self._build_failed_message(record))
                if (
                    record["status"] == CREATING
                    and spec["spec_hash"] not in self._active_builds
                    and self._is_stale_creating(record, spec)
                ):
                    self._start_build_thread(spec, condition)

    @abstractmethod
    def build_spec(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Build a deterministic environment specification."""
        ...

    def rebuild(
        self,
        spec_hash: str,
        *,
        wait: bool,
    ) -> dict[str, Any]:
        """Force a cached environment to be rebuilt from its stored spec."""

        record = self._validate_rebuild_record(
            self.store.get_environment_build(spec_hash),
            spec_hash,
        )
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
                    raise EnvironmentBuildError(self._rebuild_failed_message(current))

    def _status_record(
        self,
        record: dict[str, Any],
        spec: dict[str, Any],
    ) -> dict[str, Any]:
        return record

    def _start_build_thread(
        self,
        spec: dict[str, Any],
        condition: threading.Condition,
    ) -> None:
        spec_hash = spec["spec_hash"]
        self._active_builds.add(spec_hash)
        self._upsert_creating_record(spec)
        thread = threading.Thread(
            target=self._build_thread_main,
            args=(spec, condition),
            name=self._build_thread_name(spec_hash),
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
            self._run_build(spec)
            self.store.update_environment_build(
                spec_hash,
                status=READY,
                finished_at=utc_now(),
                error=None,
            )
        except _ExternalBuildReady:
            return
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

    def _run_build(self, spec: dict[str, Any]) -> None:
        self._build_environment(spec)

    @abstractmethod
    def _build_environment(self, spec: dict[str, Any]) -> None:
        """Build the concrete environment described by ``spec``."""
        ...

    def _record_or_absent(self, spec: dict[str, Any]) -> dict[str, Any]:
        record = self.store.get_environment_build(spec["spec_hash"])
        if record is None:
            return self._absent_record(spec)
        return self._verified_record(record, spec)

    def _verified_record(
        self,
        record: dict[str, Any],
        spec: dict[str, Any],
    ) -> dict[str, Any]:
        if record["status"] == READY and self._ready_record_is_missing(record, spec):
            return self.store.update_environment_build(
                spec["spec_hash"],
                status=ABSENT,
                finished_at=None,
                error=self._missing_ready_error(),
            )
        return record

    @abstractmethod
    def _spec_from_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Rebuild an environment spec from a stored build record."""
        ...

    @abstractmethod
    def _is_ready(self, record: dict[str, Any]) -> bool:
        """Return whether a stored build record is ready for use."""
        ...

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
                        self._build_lock_timeout_message(lock_path)
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
                self._command_timeout_message(command)
            ) from exc
        if completed.returncode != 0:
            raise EnvironmentBuildError(
                self._command_failed_message(command, completed.returncode)
            )

    def _assert_daemon_environment_path(self, path: Path) -> None:
        root = self.store.environment_builds_dir.resolve()
        target = path.resolve()
        if root != target and root not in target.parents:
            raise EnvironmentBuildError(self._path_refusal_message(target))

    @abstractmethod
    def _validate_rebuild_record(
        self,
        record: dict[str, Any] | None,
        spec_hash: str,
    ) -> dict[str, Any]:
        """Validate and return the build record used for rebuilds."""
        ...

    @abstractmethod
    def _upsert_creating_record(self, spec: dict[str, Any]) -> dict[str, Any]:
        """Persist a creating build record for ``spec``."""
        ...

    @abstractmethod
    def _build_thread_name(self, spec_hash: str) -> str:
        """Return the thread name used for one build."""
        ...

    @abstractmethod
    def _absent_record(self, spec: dict[str, Any]) -> dict[str, Any]:
        """Return the absent status record for ``spec``."""
        ...

    @abstractmethod
    def _ready_record_is_missing(
        self,
        record: dict[str, Any],
        spec: dict[str, Any],
    ) -> bool:
        """Return whether a ready record no longer has its backing resource."""
        ...

    @abstractmethod
    def _missing_ready_error(self) -> str:
        """Return the error persisted when a ready resource disappears."""
        ...

    @abstractmethod
    def _build_failed_message(self, record: dict[str, Any]) -> str:
        """Return the user-facing build failure message."""
        ...

    @abstractmethod
    def _rebuild_failed_message(self, record: dict[str, Any]) -> str:
        """Return the user-facing rebuild failure message."""
        ...

    @abstractmethod
    def _build_lock_timeout_message(self, lock_path: Path) -> str:
        """Return the timeout message for a busy build lock."""
        ...

    @abstractmethod
    def _command_timeout_message(self, command: list[str]) -> str:
        """Return the timeout message for a logged command."""
        ...

    @abstractmethod
    def _command_failed_message(self, command: list[str], returncode: int) -> str:
        """Return the failure message for a logged command."""
        ...

    @abstractmethod
    def _path_refusal_message(self, target: Path) -> str:
        """Return the safety failure message for an invalid environment path."""
        ...
