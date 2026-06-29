"""Cached Docker image builder for daemon runs."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from pathlib import Path
from typing import Any

from spl.daemon.environment_base import (
    ABSENT,
    CREATING,
    DEFAULT_BUILD_TIMEOUT_SECONDS,
    DEFAULT_STALE_LOCK_SECONDS,
    FAILED,
    READY,
    BaseEnvironmentManager,
    EnvironmentBuildError,
)
from spl.daemon.runtime_config import normalize_runtime_config
from spl.daemon.store import json_dumps, utc_now


class DockerEnvironmentManager(BaseEnvironmentManager):
    """Build and reuse Docker images keyed by runtime dependency spec."""

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
        runtime_packages = self._runtime_packages(distributions)
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

    def _status_record(
        self,
        record: dict[str, Any],
        spec: dict[str, Any],
    ) -> dict[str, Any]:
        return self._verified_record(record, spec)

    def _spec_from_record(self, record: dict[str, Any]) -> dict[str, Any]:
        spec = dict(record["spec"])
        env_dir = self.store.environment_builds_dir / record["spec_hash"]
        image_tag = record.get("image_tag") or (
            f"splime-runtime:{record['spec_hash'][:24]}"
        )
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

    def _run_build(self, spec: dict[str, Any]) -> None:
        with self._build_lock(spec):
            self.store.update_environment_build(
                spec["spec_hash"],
                status=CREATING,
                started_at=utc_now(),
                finished_at=None,
                error=None,
            )
            self._build_environment(spec)

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
        spec["dockerignore_path"].write_text(
            "*\n!Dockerfile\n!requirements.txt\n",
            encoding="utf-8",
        )

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

    def _is_ready(self, record: dict[str, Any]) -> bool:
        image_tag = record.get("image_tag")
        return (
            record["status"] == READY
            and bool(image_tag)
            and self._image_exists(str(image_tag))
        )

    def _validate_rebuild_record(
        self,
        record: dict[str, Any] | None,
        spec_hash: str,
    ) -> dict[str, Any]:
        if record is None:
            raise KeyError(f"Docker image build is not found: {spec_hash}")
        if record.get("runtime_type") != "docker":
            raise ValueError(f"environment build is not a Docker image: {spec_hash}")
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
            python_path=Path(spec["python_path"]),
            install_log_path=spec["install_log_path"],
            status=CREATING,
            runtime_type="docker",
            image_tag=spec["image_tag"],
            base_image=spec["base_image"],
        )

    def _build_thread_name(self, spec_hash: str) -> str:
        return f"spl-docker-env-{spec_hash[:12]}"

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

    def _ready_record_is_missing(
        self,
        record: dict[str, Any],
        spec: dict[str, Any],
    ) -> bool:
        return not self._image_exists(spec["image_tag"])

    def _missing_ready_error(self) -> str:
        return "cached Docker image is missing from local Docker daemon"

    def _build_failed_message(self, record: dict[str, Any]) -> str:
        return (
            "docker environment build failed: "
            f"{record.get('error') or record['spec_hash']}"
        )

    def _rebuild_failed_message(self, record: dict[str, Any]) -> str:
        return (
            "docker image rebuild failed: "
            f"{record.get('error') or record['spec_hash']}"
        )

    def _build_lock_timeout_message(self, lock_path: Path) -> str:
        return f"timed out waiting for Docker environment build lock: {lock_path}"

    def _command_timeout_message(self, command: list[str]) -> str:
        return f"docker build timed out after {self.build_timeout_seconds} seconds"

    def _command_failed_message(self, command: list[str], returncode: int) -> str:
        return f"docker build failed with exit code {returncode}"

    def _path_refusal_message(self, target: Path) -> str:
        return f"refusing to modify environment outside daemon home: {target}"

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
