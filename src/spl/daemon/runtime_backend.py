"""Pluggable runtime backends for daemon worker execution."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Callable

from spl.daemon.runtime_dependencies import (
    DockerEnvironmentBuilderProtocol,
    DockerPoolRunnerProtocol,
    EnvironmentManagerProtocol,
    RuntimeBackendProtocol,
)
from spl.daemon.interpreter_visibility import environment_record_interpreter_substitution
from spl.daemon.spl_free_generator import (
    LEGACY_WORKER_RUNTIME,
    REASON_DOCKER_RUNTIME,
    SPL_FREE_WORKER_RUNTIME,
    WorkerRuntimePlan,
    prepare_worker_runtime,
    write_worker_runtime_marker,
)
from spl.daemon.store import validate_name


@dataclass(frozen=True)
class RunContext:
    """Resolved filesystem and object state for one worker process."""

    object_record: dict[str, Any]
    run_id: str
    run_dir: Path
    workdir: Path
    input_path: Path
    object_yaml_path: Path
    result_path: Path
    artifacts_dir: Path
    env_spec_path: Path
    remote_signatures_path: Path
    stdout_path: Path
    stderr_path: Path
    worker_path: Path
    spl_free_runner_path: Path
    generated_modules_dir: Path
    worker_runtime_marker_path: Path
    entrypoint: str
    daemon_base_url: str


class VenvBackend:
    """Runtime backend that executes workers in cached virtual environments."""

    mode = "venv"

    def __init__(self, environment_manager: EnvironmentManagerProtocol):
        self.environment_manager = environment_manager
        self._environment_record: dict[str, Any] | None = None

    def __enter__(self) -> VenvBackend:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        return self.environment_manager.status_for_object(object_record)

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        self._environment_record = self.environment_manager.ensure_ready(
            object_record,
            wait=wait,
            retry_failed=retry_failed,
        )
        return self._environment_record

    def build_command(self, ctx: RunContext) -> list[str]:
        environment_record = self._ready_record()
        worker_plan = prepare_worker_runtime(
            object_record=ctx.object_record,
            object_yaml_path=ctx.object_yaml_path,
            entrypoint=ctx.entrypoint,
            run_dir=ctx.run_dir,
            generated_modules_dir=ctx.generated_modules_dir,
            runner_source_path=ctx.spl_free_runner_path,
            marker_path=ctx.worker_runtime_marker_path,
        )
        if worker_plan.runtime == SPL_FREE_WORKER_RUNTIME:
            if worker_plan.runner_path is None or worker_plan.module_path is None or worker_plan.module_name is None:
                raise RuntimeError("SPL-free worker plan is incomplete")
            return [
                environment_record["python_path"],
                str(worker_plan.runner_path),
                *spl_free_runner_args(
                    module_path=str(worker_plan.module_path),
                    module_name=worker_plan.module_name,
                    entrypoint=ctx.entrypoint,
                    input_path=str(ctx.input_path),
                    result_path=str(ctx.result_path),
                    artifacts_dir=str(ctx.artifacts_dir),
                    env_spec_path=str(ctx.env_spec_path),
                ),
            ]
        return [
            environment_record["python_path"],
            str(ctx.worker_path),
            *worker_args(
                object_yaml_path=str(ctx.object_yaml_path),
                entrypoint=ctx.entrypoint,
                input_path=str(ctx.input_path),
                result_path=str(ctx.result_path),
                artifacts_dir=str(ctx.artifacts_dir),
                env_spec_path=str(ctx.env_spec_path),
                remote_signatures_path=str(ctx.remote_signatures_path),
                daemon_url=ctx.daemon_base_url,
            ),
        ]

    def run_state_fields(self) -> dict[str, Any]:
        environment_record = self._ready_record()
        fields = {
            "resolved_runtime": environment_record["python_path"],
            "runtime_backend": self.mode,
            "image_tag": None,
            "container_id": None,
            "resolved_python": environment_record["python_path"],
        }
        substitution = environment_record_interpreter_substitution(environment_record)
        if substitution is not None:
            fields["interpreter_substitution"] = substitution
        return fields

    def after_prepare(self, object_record: dict[str, Any]) -> None:
        return None

    def after_run(self, ctx: RunContext) -> dict[str, Any]:
        return {}

    def process_result(
        self,
        ctx: RunContext,
        result_payload: dict[str, Any],
    ) -> bool:
        return False

    def _ready_record(self) -> dict[str, Any]:
        if self._environment_record is None:
            raise RuntimeError("runtime backend is not ready")
        return self._environment_record


class DockerBackend:
    """Runtime backend that executes workers in Docker containers."""

    mode = "docker"

    def __init__(
        self,
        environment_manager: DockerEnvironmentBuilderProtocol,
        docker_pool: DockerPoolRunnerProtocol,
    ):
        self.environment_manager = environment_manager
        self.docker_pool = docker_pool
        self._environment_record: dict[str, Any] | None = None
        self._pool_record: dict[str, Any] | None = None
        self._pool_context: Any = None
        self._container_name: str | None = None
        self._cleanup_container = False
        self._container_id: str | None = None

    def __enter__(self) -> DockerBackend:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._pool_context is not None:
            self._pool_context.__exit__(exc_type, exc, traceback)
            self._pool_context = None
        if self._container_name is not None and self._cleanup_container:
            self.docker_pool.remove_container(self._container_name)

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        return self.environment_manager.status_for_object(object_record)

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        if wait:
            self._assert_docker_available_for_run()
        self._environment_record = self.environment_manager.ensure_ready(
            object_record,
            wait=wait,
            retry_failed=retry_failed,
        )
        return self._environment_record

    def build_command(self, ctx: RunContext) -> list[str]:
        environment_record = self._ready_record()
        runtime_config = ctx.object_record.get("runtime_config") or {"mode": "venv"}
        write_worker_runtime_marker(
            WorkerRuntimePlan(
                runtime=LEGACY_WORKER_RUNTIME,
                reason=REASON_DOCKER_RUNTIME,
                marker_path=ctx.worker_runtime_marker_path,
            )
        )
        if self.docker_pool.can_use(ctx.run_dir, ctx.workdir):
            self._pool_record = self.docker_pool.ensure_container(
                object_record=ctx.object_record,
                image_tag=environment_record["image_tag"],
                runtime_config=runtime_config,
            )
            self._container_name = self._pool_record["name"]
            self._container_id = self._pool_record.get("container_id")
            self._pool_context = self.docker_pool.use_container(self._pool_record)
            self._pool_context.__enter__()
            return self.docker_pool.exec_worker_command(
                object_record=ctx.object_record,
                entrypoint=ctx.entrypoint,
                run_id=ctx.run_id,
                container_name=self._container_name,
                runtime_config=runtime_config,
            )

        self._container_name = self._docker_container_name(ctx.run_id)
        self._cleanup_container = True
        return self.docker_pool.worker_command(
            object_record=ctx.object_record,
            entrypoint=ctx.entrypoint,
            run_id=ctx.run_id,
            run_dir=ctx.run_dir,
            workdir=ctx.workdir,
            image_tag=environment_record["image_tag"],
            container_name=self._container_name,
            runtime_config=runtime_config,
        )

    def run_state_fields(self) -> dict[str, Any]:
        environment_record = self._ready_record()
        fields = {
            "resolved_runtime": environment_record["image_tag"],
            "runtime_backend": self.mode,
            "image_tag": environment_record["image_tag"],
            "container_id": self._container_id,
            "resolved_python": None,
        }
        substitution = environment_record_interpreter_substitution(environment_record)
        if substitution is not None:
            fields["interpreter_substitution"] = substitution
        return fields

    def after_prepare(self, object_record: dict[str, Any]) -> None:
        if self.docker_pool.should_prewarm:
            self.docker_pool.prewarm_object(object_record)

    def after_run(self, ctx: RunContext) -> dict[str, Any]:
        if not self._cleanup_container:
            return {}
        return {"container_id": self._read_docker_container_id(ctx.run_dir)}

    def process_result(
        self,
        ctx: RunContext,
        result_payload: dict[str, Any],
    ) -> bool:
        artifacts = result_payload.get("artifacts")
        if isinstance(artifacts, dict):
            result_payload["artifacts"] = {name: str(ctx.artifacts_dir / name) for name in artifacts}
        return True

    def _ready_record(self) -> dict[str, Any]:
        if self._environment_record is None:
            raise RuntimeError("runtime backend is not ready")
        return self._environment_record

    def _assert_docker_available_for_run(self) -> None:
        if shutil.which("docker") is None:
            raise RuntimeError("Docker runtime is selected, but the docker executable is not available on PATH")
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
            raise RuntimeError(
                "Docker runtime is selected, but `docker info` did not respond within 15 seconds"
            ) from exc
        if completed.returncode != 0:
            detail = (completed.stderr or "").strip()
            message = "Docker runtime is selected, but the Docker daemon is not reachable"
            if detail:
                message = f"{message}: {detail}"
            raise RuntimeError(message)

    def _docker_container_name(self, run_id: str) -> str:
        return f"splime-run-{validate_name(run_id)[:32]}"

    def _read_docker_container_id(self, run_dir: Path) -> str | None:
        cid_path = run_dir / "container.cid"
        try:
            value = cid_path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return value or None


@dataclass(frozen=True)
class RuntimeBackendServices:
    """Dependencies used by runtime backend factories."""

    environment_manager: EnvironmentManagerProtocol
    docker_environment_manager: DockerEnvironmentBuilderProtocol
    docker_pool: DockerPoolRunnerProtocol


BackendFactory = Callable[[RuntimeBackendServices], RuntimeBackendProtocol]


def _venv_backend(services: RuntimeBackendServices) -> RuntimeBackendProtocol:
    return VenvBackend(services.environment_manager)


def _docker_backend(services: RuntimeBackendServices) -> RuntimeBackendProtocol:
    return DockerBackend(services.docker_environment_manager, services.docker_pool)


RUNTIME_BACKENDS: dict[str, BackendFactory] = {
    "venv": _venv_backend,
    "docker": _docker_backend,
}


class RuntimeBackendRegistry:
    """Create runtime backends from object runtime configuration."""

    def __init__(
        self,
        services: RuntimeBackendServices,
        backends: dict[str, BackendFactory] | None = None,
    ):
        self.services = services
        self.backends = backends or RUNTIME_BACKENDS

    def backend_for(self, object_record: dict[str, Any]) -> RuntimeBackendProtocol:
        mode = runtime_mode(object_record)
        try:
            factory = self.backends[mode]
        except KeyError as exc:
            raise ValueError(f"unsupported runtime mode: {mode}") from exc
        return factory(self.services)


def runtime_mode(object_record: dict[str, Any]) -> str:
    runtime_config = object_record.get("runtime_config") or {}
    return str(runtime_config.get("mode") or object_record.get("runtime_mode") or "venv")


def worker_args(
    *,
    object_yaml_path: str,
    entrypoint: str,
    input_path: str,
    result_path: str,
    artifacts_dir: str,
    env_spec_path: str,
    remote_signatures_path: str,
    daemon_url: str,
) -> list[str]:
    return [
        "--object-yaml",
        object_yaml_path,
        "--entrypoint",
        entrypoint,
        "--input",
        input_path,
        "--result",
        result_path,
        "--artifacts-dir",
        artifacts_dir,
        "--env-spec",
        env_spec_path,
        "--remote-signatures",
        remote_signatures_path,
        "--daemon-url",
        daemon_url,
    ]


def spl_free_runner_args(
    *,
    module_path: str,
    module_name: str,
    entrypoint: str,
    input_path: str,
    result_path: str,
    artifacts_dir: str,
    env_spec_path: str,
) -> list[str]:
    return [
        "--module",
        module_path,
        "--module-name",
        module_name,
        "--entrypoint",
        entrypoint,
        "--input",
        input_path,
        "--result",
        result_path,
        "--artifacts-dir",
        artifacts_dir,
        "--env-spec",
        env_spec_path,
    ]
