from __future__ import annotations

import json
from pathlib import Path
from types import TracebackType
from typing import Any

from spl.daemon.interpreter_visibility import INTERPRETER_RESOLUTION_KEY
from spl.daemon.runtime_backend import (
    DockerBackend,
    RunContext,
    RuntimeBackendRegistry,
    RuntimeBackendServices,
    VenvBackend,
)
from spl.daemon.spl_free_generator import (
    LEGACY_WORKER_RUNTIME,
    REASON_DOCKER_RUNTIME,
    REASON_PIPELINE,
    REASON_SUPPORTED_FUNCTION,
    SPL_FREE_WORKER_RUNTIME,
)
from spl.daemon.store import RegistryStore


FUNCTION_YAML = """\
- !DFunction
  name: artifact_func
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 7
"""


class FakeEnvironmentManager:
    def __init__(self, record: dict[str, Any]):
        self.record = record

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        return {"status": "ready", **self.record}

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        return self.record

    def build_spec(self, object_record: dict[str, Any]) -> dict[str, Any]:
        return self.record

    def rebuild(self, spec_hash: str, *, wait: bool) -> dict[str, Any]:
        return self.record


class FakeDockerPool:
    should_prewarm = False

    def __init__(self, *, can_use: bool):
        self.can_use_value = can_use
        self.removed: list[str] = []
        self.use_context = FakeUseContext()

    def can_use(self, run_dir: Path, workdir: Path) -> bool:
        return self.can_use_value

    def ensure_container(
        self,
        *,
        object_record: dict[str, Any],
        image_tag: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "name": "splime-pool-test",
            "container_id": "warm-container-id",
        }

    def exec_worker_command(
        self,
        *,
        object_record: dict[str, Any],
        entrypoint: str,
        run_id: str,
        container_name: str,
        runtime_config: dict[str, Any],
    ) -> list[str]:
        return ["docker", "exec", container_name, entrypoint, run_id]

    def worker_command(
        self,
        *,
        object_record: dict[str, Any],
        entrypoint: str,
        run_id: str,
        run_dir: Path,
        workdir: Path,
        image_tag: str,
        container_name: str,
        runtime_config: dict[str, Any],
    ) -> list[str]:
        return ["docker", "run", "--name", container_name, image_tag]

    def use_container(self, record: dict[str, Any]) -> FakeUseContext:
        return self.use_context

    def remove_container(self, name: str) -> None:
        self.removed.append(name)

    def prewarm_object(self, object_record: dict[str, Any]) -> None:
        pass

    def cleanup_stale_containers(self) -> None:
        pass

    def shutdown(self) -> None:
        pass


class FakeUseContext:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    def __enter__(self) -> None:
        self.entered = True

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.exited = True


def _ctx(tmp_path: Path, *, run_id: str = "run-123") -> RunContext:
    return RunContext(
        object_record={"runtime_config": {"mode": "docker"}},
        run_id=run_id,
        run_dir=tmp_path,
        workdir=tmp_path,
        input_path=tmp_path / "input.json",
        object_yaml_path=tmp_path / "object.yaml",
        result_path=tmp_path / "result.json",
        artifacts_dir=tmp_path / "artifacts",
        env_spec_path=tmp_path / "env-spec.json",
        remote_signatures_path=tmp_path / "remote-signatures.json",
        stdout_path=tmp_path / "stdout.txt",
        stderr_path=tmp_path / "stderr.txt",
        worker_path=tmp_path / "worker.py",
        spl_free_runner_path=tmp_path / "spl_free_runner_source.py",
        generated_modules_dir=tmp_path / "generated-modules",
        worker_runtime_marker_path=tmp_path / "worker-runtime.json",
        entrypoint="artifact_func",
        daemon_base_url="http://127.0.0.1:8765",
    )


def test_runtime_backend_registry_selects_default_and_configured_modes() -> None:
    services = RuntimeBackendServices(
        environment_manager=FakeEnvironmentManager({"spec_hash": "venv", "python_path": "/venv/bin/python"}),
        docker_environment_manager=FakeEnvironmentManager({"spec_hash": "docker", "image_tag": "splime-runtime:demo"}),
        docker_pool=FakeDockerPool(can_use=False),
    )
    registry = RuntimeBackendRegistry(services)

    assert isinstance(registry.backend_for({}), VenvBackend)
    assert isinstance(
        registry.backend_for({"runtime_config": {"mode": "docker"}}),
        DockerBackend,
    )
    assert isinstance(registry.backend_for({"runtime_mode": "docker"}), DockerBackend)


def test_venv_backend_builds_worker_command(tmp_path: Path) -> None:
    backend = VenvBackend(FakeEnvironmentManager({"spec_hash": "venv-hash", "python_path": "/venv/bin/python"}))
    ctx = _ctx(tmp_path)
    ctx.object_yaml_path.write_text(FUNCTION_YAML, encoding="utf-8")

    environment_record = backend.ensure_ready(ctx.object_record)
    command = backend.build_command(ctx)

    assert environment_record["spec_hash"] == "venv-hash"
    assert command[:2] == ["/venv/bin/python", str(ctx.worker_path)]
    assert "--object-yaml" in command
    assert str(ctx.object_yaml_path) in command
    assert ctx.worker_runtime_marker_path.read_text(encoding="utf-8")
    assert backend.run_state_fields() == {
        "resolved_runtime": "/venv/bin/python",
        "runtime_backend": "venv",
        "image_tag": None,
        "container_id": None,
        "resolved_python": "/venv/bin/python",
    }


def test_venv_backend_builds_spl_free_function_command(tmp_path: Path) -> None:
    runner_source = tmp_path / "spl_free_runner_source.py"
    runner_source.write_text("# runner\n", encoding="utf-8")
    backend = VenvBackend(FakeEnvironmentManager({"spec_hash": "venv-hash", "python_path": "/venv/bin/python"}))
    ctx = _ctx(tmp_path)
    ctx.object_record.update(
        {
            "kind": "function",
            "content_hash": "a" * 64,
        }
    )
    ctx.object_yaml_path.write_text(FUNCTION_YAML, encoding="utf-8")

    backend.ensure_ready(ctx.object_record)
    command = backend.build_command(ctx)

    assert command[:2] == ["/venv/bin/python", str(tmp_path / "spl_free_runner.py")]
    assert "--module" in command
    assert str(ctx.generated_modules_dir / "v1" / ("a" * 64) / "object_module.py") in command
    assert "--object-yaml" not in command
    marker = ctx.worker_runtime_marker_path.read_text(encoding="utf-8")
    assert SPL_FREE_WORKER_RUNTIME in marker
    assert REASON_SUPPORTED_FUNCTION in marker


def test_venv_backend_marks_pipeline_as_legacy(tmp_path: Path) -> None:
    backend = VenvBackend(FakeEnvironmentManager({"spec_hash": "venv-hash", "python_path": "/venv/bin/python"}))
    ctx = _ctx(tmp_path)
    ctx.object_yaml_path.write_text(FUNCTION_YAML, encoding="utf-8")

    backend.ensure_ready(ctx.object_record)
    command = backend.build_command(ctx)

    assert command[:2] == ["/venv/bin/python", str(ctx.worker_path)]
    marker = ctx.worker_runtime_marker_path.read_text(encoding="utf-8")
    assert LEGACY_WORKER_RUNTIME in marker
    assert REASON_PIPELINE in marker


def test_venv_backend_exposes_interpreter_substitution(tmp_path: Path) -> None:
    backend = VenvBackend(
        FakeEnvironmentManager(
            {
                "spec_hash": "venv-hash",
                "python_path": "/venv/bin/python",
                "spec": {
                    INTERPRETER_RESOLUTION_KEY: {
                        "authored_python": "/author/bin/python",
                        "authored_python_version": "Python 3.11.8",
                        "resolved_python": "/venv/bin/python",
                        "resolved_python_version": "Python 3.13.0",
                        "reason": "local_env",
                        "reason_detail": "spl_core",
                        "substituted": True,
                    }
                },
            }
        )
    )
    ctx = _ctx(tmp_path)

    backend.ensure_ready(ctx.object_record)

    assert backend.run_state_fields()["interpreter_substitution"] == {
        "authored_python": "/author/bin/python",
        "authored_python_version": "Python 3.11.8",
        "resolved_python": "/venv/bin/python",
        "resolved_python_version": "Python 3.13.0",
        "reason": "local_env",
        "reason_detail": "spl_core",
        "minor_mismatch": True,
    }


def test_docker_backend_builds_pool_exec_command(tmp_path: Path) -> None:
    pool = FakeDockerPool(can_use=True)
    backend = DockerBackend(
        FakeEnvironmentManager({"spec_hash": "docker-hash", "image_tag": "splime-runtime:demo"}),
        pool,
    )
    ctx = _ctx(tmp_path)

    with backend:
        backend.ensure_ready(ctx.object_record, wait=False)
        command = backend.build_command(ctx)
        fields = backend.run_state_fields()
        assert pool.use_context.entered is True

    assert command == [
        "docker",
        "exec",
        "splime-pool-test",
        "artifact_func",
        "run-123",
    ]
    assert fields["container_id"] == "warm-container-id"
    assert fields["runtime_backend"] == "docker"
    assert pool.use_context.exited is True
    assert pool.removed == []


def test_docker_backend_writes_legacy_worker_runtime_marker(tmp_path: Path) -> None:
    pool = FakeDockerPool(can_use=True)
    backend = DockerBackend(
        FakeEnvironmentManager({"spec_hash": "docker-hash", "image_tag": "splime-runtime:demo"}),
        pool,
    )
    ctx = _ctx(tmp_path)

    with backend:
        backend.ensure_ready(ctx.object_record, wait=False)
        backend.build_command(ctx)

    marker = json.loads(ctx.worker_runtime_marker_path.read_text(encoding="utf-8"))
    assert marker == {
        "worker_runtime": LEGACY_WORKER_RUNTIME,
        "worker_runtime_reason": REASON_DOCKER_RUNTIME,
    }
    store = RegistryStore(tmp_path / "store")
    try:
        assert store.runs._worker_runtime_marker({"run_dir": str(ctx.run_dir)}) == marker
    finally:
        store.close()


def test_docker_backend_builds_one_shot_command_and_rewrites_artifacts(
    tmp_path: Path,
) -> None:
    pool = FakeDockerPool(can_use=False)
    backend = DockerBackend(
        FakeEnvironmentManager({"spec_hash": "docker-hash", "image_tag": "splime-runtime:demo"}),
        pool,
    )
    ctx = _ctx(tmp_path, run_id="abc123")
    (ctx.run_dir / "container.cid").write_text("container-id\n", encoding="utf-8")
    payload = {"artifacts": {"artifact.txt": "/work/artifacts/artifact.txt"}}

    with backend:
        backend.ensure_ready(ctx.object_record, wait=False)
        command = backend.build_command(ctx)
        after_run = backend.after_run(ctx)
        changed = backend.process_result(ctx, payload)

    assert command == [
        "docker",
        "run",
        "--name",
        "splime-run-abc123",
        "splime-runtime:demo",
    ]
    assert after_run == {"container_id": "container-id"}
    assert changed is True
    assert payload["artifacts"] == {"artifact.txt": str(ctx.artifacts_dir / "artifact.txt")}
    assert pool.removed == ["splime-run-abc123"]
