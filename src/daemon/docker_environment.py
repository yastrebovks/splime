"""Cached Docker image builder for daemon runs."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from spl.daemon.environment import (
    ABSENT,
    CREATING,
    DEFAULT_BUILD_TIMEOUT_SECONDS,
    DEFAULT_STALE_LOCK_SECONDS,
    FAILED,
    READY,
    EnvironmentBuildError,
    _ExternalBuildReady,
)
from spl.daemon.runtime_config import normalize_runtime_config
from spl.daemon.store import RegistryStore, json_dumps, utc_now


class DockerEnvironmentManager:
    """Build and reuse Docker images keyed by runtime dependency spec."""

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
        spec = self.build_spec(object_record)
        return self._record_or_absent(spec)

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        spec = self.build_spec(object_record)
        condition = self._condition_for(spec["spec_hash"])
        with condition:
            record = self._record_or_absent(spec)
            if self._is_ready(record):
                return record

            if record["status"] == FAILED:
                if not retry_failed:
                    raise EnvironmentBuildError(
                        "docker environment build failed: "
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
                        "docker environment build failed: "
                        f"{record.get('error') or record['spec_hash']}"
                    )
                if (
                    record["status"] == CREATING
                    and spec["spec_hash"] not in self._active_builds
                    and self._is_stale_creating(record, spec)
                ):
                    self._start_build_thread(spec, condition)

    def rebuild(
        self,
        spec_hash: str,
        *,
        wait: bool,
    ) -> dict[str, Any]:
        record = self.store.get_environment_build(spec_hash)
        if record is None:
            raise KeyError(f"Docker image build is not found: {spec_hash}")
        if record.get("runtime_type") != "docker":
            raise ValueError(f"environment build is not a Docker image: {spec_hash}")
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
                        "docker image rebuild failed: "
                        f"{current.get('error') or current['spec_hash']}"
                    )

    def prune_images(self, spec_hash: str | None = None) -> list[dict[str, Any]]:
        self._assert_docker_available()
        records = [
            record
            for record in self.store.list_environment_builds()
            if record.get("runtime_type") == "docker"
            and (spec_hash is None or record["spec_hash"] == spec_hash)
        ]
        pruned = []
        for record in records:
            image_tag = record.get("image_tag")
            if image_tag:
                subprocess.run(
                    ["docker", "image", "rm", "-f", str(image_tag)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    timeout=60,
                    check=False,
                )
            pruned.append(
                self.store.update_environment_build(
                    record["spec_hash"],
                    status=ABSENT,
                    finished_at=None,
                    error="Docker image was pruned by user request",
                )
            )
        return pruned

    def build_spec(self, object_record: dict[str, Any]) -> dict[str, Any]:
        runtime_config = normalize_runtime_config(object_record.get("runtime_config"))
        if runtime_config["mode"] != "docker":
            raise ValueError("object runtime is not docker")
        distributions = self._normalize_distributions(object_record["distributions"])
        runtime_packages = self.store.environment_runtime_packages_for(distributions)
        python_version = runtime_config["python"]
        base_image = runtime_config["base_image"]
        spec_payload = {
            "runtime_type": "docker",
            "python_version": python_version,
            "base_image": base_image,
            "distributions": distributions,
            "runtime_packages": runtime_packages,
            "apt_packages": runtime_config.get("apt_packages") or [],
            "pull": bool(runtime_config.get("pull", False)),
        }
        spec_hash = hashlib.sha256(json_dumps(spec_payload).encode("utf-8")).hexdigest()
        env_dir = self.store.environment_builds_dir / spec_hash
        image_tag = f"splime-runtime:{spec_hash[:24]}"
        return {
            "spec_hash": spec_hash,
            "base_python": base_image,
            "python_version": python_version,
            "base_image": base_image,
            "image_tag": image_tag,
            "distributions": distributions,
            "runtime_packages": runtime_packages,
            "spec": spec_payload,
            "env_dir": env_dir,
            "context_dir": env_dir / "context",
            "venv_path": env_dir / "image",
            "python_path": image_tag,
            "dockerfile_path": env_dir / "context" / "Dockerfile",
            "requirements_path": env_dir / "context" / "requirements.txt",
            "dockerignore_path": env_dir / "context" / ".dockerignore",
            "install_log_path": env_dir / "install.log",
            "lock_path": env_dir / "build.lock",
            "force_rebuild": False,
        }

    def _spec_from_record(self, record: dict[str, Any]) -> dict[str, Any]:
        spec = dict(record["spec"])
        env_dir = self.store.environment_builds_dir / record["spec_hash"]
        image_tag = record.get("image_tag") or f"splime-runtime:{record['spec_hash'][:24]}"
        base_image = record.get("base_image") or record["base_python"]
        return {
            "spec_hash": record["spec_hash"],
            "base_python": base_image,
            "python_version": record["python_version"],
            "base_image": base_image,
            "image_tag": image_tag,
            "distributions": record["distributions"],
            "runtime_packages": record["runtime_packages"],
            "spec": spec,
            "env_dir": env_dir,
            "context_dir": env_dir / "context",
            "venv_path": env_dir / "image",
            "python_path": image_tag,
            "dockerfile_path": env_dir / "context" / "Dockerfile",
            "requirements_path": env_dir / "context" / "requirements.txt",
            "dockerignore_path": env_dir / "context" / ".dockerignore",
            "install_log_path": env_dir / "install.log",
            "lock_path": env_dir / "build.lock",
            "force_rebuild": False,
        }

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
            python_path=Path(spec["python_path"]),
            install_log_path=spec["install_log_path"],
            status=CREATING,
            runtime_type="docker",
            image_tag=spec["image_tag"],
            base_image=spec["base_image"],
        )
        thread = threading.Thread(
            target=self._build_thread_main,
            args=(spec, condition),
            name=f"spl-docker-env-{spec_hash[:12]}",
            daemon=True,
        )
        thread.start()

    def _build_thread_main(
        self,
        spec: dict[str, Any],
        condition: threading.Condition,
    ) -> None:
        try:
            with self._build_lock(spec):
                self.store.update_environment_build(
                    spec["spec_hash"],
                    status=CREATING,
                    started_at=utc_now(),
                    finished_at=None,
                    error=None,
                )
                self._build_environment(spec)
            self.store.update_environment_build(
                spec["spec_hash"],
                status=READY,
                finished_at=utc_now(),
                error=None,
            )
        except _ExternalBuildReady:
            return
        except Exception as exc:  # noqa: BLE001 - build errors are persisted.
            self.store.update_environment_build(
                spec["spec_hash"],
                status=FAILED,
                finished_at=utc_now(),
                error=repr(exc),
            )
        finally:
            with condition:
                self._active_builds.discard(spec["spec_hash"])
                condition.notify_all()

    def _build_environment(self, spec: dict[str, Any]) -> None:
        self._assert_docker_available()
        env_dir = Path(spec["env_dir"])
        self._assert_daemon_environment_path(env_dir)
        env_dir.mkdir(parents=True, exist_ok=True)
        Path(spec["context_dir"]).mkdir(parents=True, exist_ok=True)
        dockerfile = self._dockerfile(spec)
        requirements = self._requirements(spec)
        spec["dockerfile_path"].write_text(dockerfile, encoding="utf-8")
        spec["requirements_path"].write_text("\n".join(requirements), encoding="utf-8")
        spec["dockerignore_path"].write_text("*\n!Dockerfile\n!requirements.txt\n", encoding="utf-8")

        log_path = spec["install_log_path"]
        with log_path.open("w", encoding="utf-8") as log:
            log.write(f"Base image: {spec['base_image']}\n")
            log.write(f"Image tag: {spec['image_tag']}\n")
            log.write(f"Build timeout: {self.build_timeout_seconds} seconds\n")
            if requirements:
                log.write("\nInstalling requirements:\n")
                for requirement in requirements:
                    log.write(f"  {requirement}\n")
            command = [
                "docker",
                "build",
                f"--pull={'true' if spec['spec'].get('pull') else 'false'}",
                "-t",
                spec["image_tag"],
                str(spec["context_dir"]),
            ]
            self._run_logged(command, log)

    def _dockerfile(self, spec: dict[str, Any]) -> str:
        requirements = self._requirements(spec)
        lines = [
            f"FROM {spec['base_image']}",
            "ENV PYTHONDONTWRITEBYTECODE=1",
            "ENV PYTHONUNBUFFERED=1",
            "ENV PIP_DISABLE_PIP_VERSION_CHECK=1",
            "WORKDIR /work",
        ]
        apt_packages = spec["spec"].get("apt_packages") or []
        if apt_packages:
            packages = " ".join(apt_packages)
            lines.extend(
                [
                    "RUN apt-get update "
                    f"&& apt-get install -y --no-install-recommends {packages} "
                    "&& rm -rf /var/lib/apt/lists/*",
                ]
            )
        if requirements:
            lines.extend(
                [
                    "COPY requirements.txt /tmp/splime-requirements.txt",
                    "RUN python -m pip install --no-cache-dir -r /tmp/splime-requirements.txt",
                ]
            )
        lines.append('CMD ["python", "--version"]')
        return "\n".join(lines) + "\n"

    def _requirements(self, spec: dict[str, Any]) -> list[str]:
        requirements = []
        for item in [*spec["runtime_packages"], *spec["distributions"]]:
            if item.get("version") is None:
                requirements.append(item["package"])
            else:
                requirements.append(f"{item['package']}=={item['version']}")
        return requirements

    def _record_or_absent(self, spec: dict[str, Any]) -> dict[str, Any]:
        record = self.store.get_environment_build(spec["spec_hash"])
        if record is None:
            return self._absent_record(spec)
        if record["status"] == READY and not self._image_exists(spec["image_tag"]):
            return self.store.update_environment_build(
                spec["spec_hash"],
                status=ABSENT,
                finished_at=None,
                error="cached Docker image is missing from local Docker daemon",
            )
        return record

    def _absent_record(self, spec: dict[str, Any]) -> dict[str, Any]:
        return {
            "spec_hash": spec["spec_hash"],
            "status": ABSENT,
            "runtime_type": "docker",
            "base_image": spec["base_image"],
            "image_tag": spec["image_tag"],
            "python_path": spec["image_tag"],
            "install_log_path": str(spec["install_log_path"]),
        }

    def _is_ready(self, record: dict[str, Any]) -> bool:
        image_tag = record.get("image_tag")
        return (
            record["status"] == READY
            and bool(image_tag)
            and self._image_exists(str(image_tag))
        )

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
                "docker build timed out after "
                f"{self.build_timeout_seconds} seconds"
            ) from exc
        if completed.returncode != 0:
            raise EnvironmentBuildError(
                f"docker build failed with exit code {completed.returncode}"
            )

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
                        "timed out waiting for Docker environment build lock: "
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

    def _image_exists(self, image_tag: str) -> bool:
        if shutil.which("docker") is None:
            return False
        try:
            completed = subprocess.run(
                ["docker", "image", "inspect", image_tag],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=15,
                check=False,
            )
        except Exception:
            return False
        return completed.returncode == 0

    def _assert_docker_available(self) -> None:
        if shutil.which("docker") is None:
            raise EnvironmentBuildError("docker executable is not available on PATH")
        try:
            completed = subprocess.run(
                ["docker", "info"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                timeout=15,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise EnvironmentBuildError(
                "Docker daemon did not respond to `docker info` within 15 seconds"
            ) from exc
        if completed.returncode != 0:
            detail = (completed.stderr or "").strip()
            message = "Docker daemon is not reachable"
            if detail:
                message = f"{message}: {detail}"
            raise EnvironmentBuildError(message)

    def _assert_daemon_environment_path(self, path: Path) -> None:
        root = self.store.environment_builds_dir.resolve()
        target = path.resolve()
        if root != target and root not in target.parents:
            raise EnvironmentBuildError(
                f"refusing to modify environment outside daemon home: {target}"
            )
