from __future__ import annotations

import base64
import hashlib
import logging
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

import spl.daemon.server as daemon_server
from spl import lift
from spl.core.ir.utils import spl_export_to_file
from spl.daemon.environment import EnvironmentBuildError
from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore, utc_now
from spl.daemon.worker import ARTIFACT_REF_KEY, run_pipeline


ARTIFACT_FUNCTION_YAML = """\
- !DFunction
  name: artifact_func
  inputs: []
  outputs:
  - name: default
    type: dict
  body: |-
    from pathlib import Path
    Path("artifact.txt").write_text("daemon artifact", encoding="utf-8")
    return {
        "__spl_result__": {"answer": 7},
        "__spl_artifacts__": {"artifact.txt": "artifact.txt"},
    }
"""


REMOTE_FUNCTION_YAML = """\
- !DFunction
  name: demo_obj
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 1
"""


def _worker_final_png() -> bytes:
    return b"\x89PNG\r\n\x1a\nsplime-final-output"


def _worker_save_bytes(path: str, obj: bytes) -> None:
    with open(path, "wb") as f:
        f.write(obj)


def _worker_load_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _worker_explicit_artifact_payload() -> dict[str, Any]:
    Path("nested.txt").write_text("nested daemon artifact", encoding="utf-8")
    return {
        "__spl_result__": {"ok": True},
        "__spl_artifacts__": {"nested.txt": "nested.txt"},
    }


def _worker_slow_marker(marker_path: str) -> str:
    import time
    from pathlib import Path

    time.sleep(1.2)
    Path(marker_path).write_text("finished", encoding="utf-8")
    return "finished"


def _worker_final_unadapted_bytes() -> bytes:
    return b"missing adapter"


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection, *, token: str) -> None:
        pass

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


PIPELINE_WITH_INTERNAL_FUNCTION_YAML = """\
- !DPipeline
  name: demo_pipeline
  nodes:
  - !DNodeFunction
    uuid: 11111111-1111-4111-8111-111111111111
    func: inner_add
  links: []
  aliases:
  - - total
    - 11111111-1111-4111-8111-111111111111
---
- !DFunction
  name: inner_add
  inputs:
  - name: a
    type: int
    default: null
  - name: b
    type: int
    default: null
  outputs:
  - name: default
    type: int
  body: |-
    return a + b
"""


def _mark_object_environment_ready(
    runtime: DaemonRuntime,
    object_record: dict[str, Any],
) -> dict[str, Any]:
    spec = runtime.environment_manager.build_spec(object_record)
    ready = runtime.store.upsert_environment_build(
        spec_hash=spec["spec_hash"],
        base_python=spec["base_python"],
        python_version=spec["python_version"],
        distributions=spec["distributions"],
        runtime_packages=spec["runtime_packages"],
        spec=spec["spec"],
        venv_path=Path(sys.executable).parent,
        python_path=Path(sys.executable),
        install_log_path=Path(spec["install_log_path"]),
        status="ready",
    )
    return runtime.store.update_environment_build(
        ready["spec_hash"],
        status="ready",
        started_at=utc_now(),
        finished_at=utc_now(),
    )


def _wait_for_run(
    store: RegistryStore,
    run_id: str,
    *,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        state = store.get_run(run_id)
        if state["status"] in {"succeeded", "failed"}:
            return state
        time.sleep(0.1)
    raise TimeoutError(f"run did not finish: {run_id}")


def _final_png_pipeline():
    return (
        lift(_worker_final_png)
        .alias("thumbnail")
        .render("thumbnail_pipeline")
        .add_adapter(bytes, "png", save=_worker_save_bytes, load=_worker_load_bytes)
    )


def test_pipeline_final_adapter_output_is_daemon_artifact(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store)
        pipeline = _final_png_pipeline()
        yaml_path = tmp_path / "thumbnail_pipeline.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        record = runtime.register_object(
            "thumbnail_pipeline",
            "thumbnail_pipeline",
            "default",
            yaml_text=yaml_path.read_text(encoding="utf-8"),
        )
        build = _mark_object_environment_ready(runtime, record)

        started = runtime.start_run(
            "thumbnail_pipeline",
            output="thumbnail",
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["env_build_hash"] == build["spec_hash"]
        artifact_path = Path(final["artifacts_dir"]) / "thumbnail.png"
        assert artifact_path.read_bytes() == _worker_final_png()
        assert final["result"]["artifacts"] == {"thumbnail.png": str(artifact_path)}
        assert final["result"]["result"] == {
            "default": {
                ARTIFACT_REF_KEY: True,
                "format": "png",
                "key": "builtins.bytes@png",
                "name": "thumbnail.png",
                "sha256": hashlib.sha256(_worker_final_png()).hexdigest(),
                "size": len(_worker_final_png()),
            }
        }
    finally:
        store.close()


def test_pipeline_output_normalizer_extracts_nested_explicit_artifacts(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    pipeline = lift(_worker_explicit_artifact_payload).alias("payload").render("payload_pipeline")

    result, artifacts = run_pipeline(
        pipeline,
        {},
        "payload",
        daemon_url="http://127.0.0.1:8765",
        timeout_seconds=None,
        artifacts_dir=tmp_path / "artifacts",
    )

    assert result == {"default": {"ok": True}}
    assert artifacts == {"nested.txt": str(tmp_path / "artifacts" / "nested.txt")}
    assert (tmp_path / "artifacts" / "nested.txt").read_text(encoding="utf-8") == "nested daemon artifact"


def test_pipeline_output_normalizer_reports_missing_adapter_path(tmp_path) -> None:
    pipeline = lift(_worker_final_unadapted_bytes).alias("thumbnail").render("thumbnail_pipeline")

    with pytest.raises(TypeError) as exc_info:
        run_pipeline(
            pipeline,
            {},
            "thumbnail",
            daemon_url="http://127.0.0.1:8765",
            timeout_seconds=None,
            artifacts_dir=tmp_path / "artifacts",
        )

    assert str(exc_info.value) == ("result.thumbnail.default bytes is not JSON serializable; add_adapter(bytes, ...)")


def test_pipeline_node_timeout_runtime_config_reaches_daemon_worker(tmp_path) -> None:
    marker_path = tmp_path / "marker.txt"
    pipeline = (
        lift(_worker_slow_marker)
        .alias("slow")
        .render("slow_worker_pipeline")
        .with_node_runtime("slow", "venv-subprocess")
    )

    with pytest.raises(RuntimeError, match=r"node runtime `venv-subprocess` timed out after 0.8s"):
        run_pipeline(
            pipeline,
            {"marker_path": str(marker_path)},
            "slow",
            daemon_url="http://127.0.0.1:8765",
            timeout_seconds=None,
            artifacts_dir=tmp_path / "artifacts",
            runtime_config={"node_timeout_seconds": 0.8},
        )

    time.sleep(0.7)
    assert not marker_path.exists()


def test_publish_run_environment_and_artifact_flow(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store)

        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
        )
        build = _mark_object_environment_ready(runtime, record)

        started = runtime.start_run(
            "artifact_func",
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["env_build_hash"] == build["spec_hash"]
        assert final["result"]["result"] == {"answer": 7}

        artifact_path = Path(final["artifacts_dir"]) / "artifact.txt"
        assert artifact_path.read_text(encoding="utf-8") == "daemon artifact"

        encoded = runtime._encode_local_artifacts(final)
        assert encoded == [
            {
                "name": "artifact.txt",
                "data_base64": base64.b64encode(b"daemon artifact").decode("ascii"),
            }
        ]
        text_artifacts = runtime._local_run_text_artifacts(final)
        assert any(
            item["name"] == "artifact.artifact.txt" and item["content_text"] == "daemon artifact"
            for item in text_artifacts
        )
    finally:
        store.close()


def test_docker_runtime_config_is_persisted_and_command_is_constructed(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)

        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13"},
        )

        assert record["runtime_config"]["mode"] == "docker"
        assert record["runtime_config"]["python"] == "3.13"
        assert record["runtime_config"]["base_image"] == "python:3.13-slim-trixie"
        assert record["runtime_config"]["network"] == "auto"
        assert record["runtime_config"]["limits"]["pids_limit"] == 256
        assert record["runtime_config"]["tmpfs"] == "/tmp:rw,nosuid,size=512m"

        build_spec = runtime.docker_environment_manager.build_spec(record)
        assert build_spec["base_image"] == "python:3.13-slim-trixie"
        assert build_spec["image_tag"].startswith("splime-runtime:")

        run_dir = tmp_path / "runs" / "docker-run"
        run_dir.mkdir(parents=True)
        workdir = tmp_path / "work"
        workdir.mkdir()
        daemon_src = tmp_path / "daemon-src"
        worker = daemon_src / "spl" / "daemon" / "worker.py"
        worker.parent.mkdir(parents=True)
        worker.write_text("# worker", encoding="utf-8")
        framework_src = tmp_path / "framework-src"
        framework_src.mkdir()
        monkeypatch.setattr(
            runtime.docker_pool,
            "source_roots",
            lambda: [("daemon", daemon_src), ("framework", framework_src)],
        )

        command = runtime.docker_pool.worker_command(
            object_record=record,
            entrypoint=record["entrypoint"],
            run_id="abc123",
            run_dir=run_dir,
            workdir=workdir,
            image_tag=build_spec["image_tag"],
            container_name="splime-run-abc123",
            runtime_config=record["runtime_config"],
        )

        assert command[:5] == ["docker", "run", "--rm", "--name", "splime-run-abc123"]
        assert "--network" in command
        assert command[command.index("--network") + 1] == "none"
        assert "--read-only" in command
        assert "--tmpfs" in command
        assert command[command.index("--tmpfs") + 1] == "/tmp:rw,nosuid,size=512m"
        assert "--cap-drop" in command
        assert command[command.index("--cap-drop") + 1] == "ALL"
        assert "--security-opt" in command
        assert "no-new-privileges" in command
        assert "--pids-limit" in command
        assert command[command.index("--pids-limit") + 1] == "256"
        assert build_spec["image_tag"] in command
        assert "/opt/splime/src0/spl/daemon/worker.py" in command
        assert f"{run_dir.resolve()}:/work" in command
        assert f"{workdir.resolve()}:/workspace" in command
        assert "PYTHONPATH=/opt/splime/src0:/opt/splime/src1" in command
        assert "HOME=/tmp" in command
        assert "XDG_CACHE_HOME=/tmp/.cache" in command
        assert "MPLCONFIGDIR=/tmp/.cache/matplotlib" in command
    finally:
        store.close()


def test_docker_runtime_rejects_python_before_313(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)

        try:
            runtime.register_object(
                "artifact_func",
                "artifact_func",
                "default",
                yaml_text=ARTIFACT_FUNCTION_YAML,
                runtime_config={"mode": "docker", "python": "3.12"},
            )
        except ValueError as exc:
            assert "Python >= 3.13" in str(exc)
        else:
            raise AssertionError("expected Python version validation to fail")
    finally:
        store.close()


def test_docker_runtime_rejects_network_none_with_remote_nodes(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        try:
            store._validate_runtime_config_for_metadata(
                {"mode": "docker", "network": "none"},
                {"pipeline_nodes": [{"kind": "remote"}]},
            )
        except ValueError as exc:
            assert "network='none' is incompatible with remote" in str(exc)
        else:
            raise AssertionError("expected network validation to fail")
    finally:
        store.close()


def test_docker_network_uses_bridge_host_gateway_for_remote_nodes(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False)
        monkeypatch.setattr("spl.daemon.docker_pool.platform.system", lambda: "Linux")

        args, daemon_url = runtime.docker_pool.network_args(
            {"pipeline_nodes": [{"kind": "remote"}]},
            {"mode": "docker", "network": "auto"},
        )

        assert args == ["--add-host", "host.docker.internal:host-gateway"]
        assert "host.docker.internal" in daemon_url
    finally:
        store.close()


def test_docker_pool_exec_command_uses_runs_mount(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=1)

        command = runtime.docker_pool.exec_worker_command(
            object_record={"pipeline_nodes": []},
            entrypoint="artifact_func",
            run_id="abc123",
            container_name="splime-pool-test",
            runtime_config={"mode": "docker", "network": "auto"},
        )

        assert command[:4] == ["docker", "exec", "-w", "/runs/abc123"]
        assert "splime-pool-test" in command
        assert "/runs/abc123/object.yaml" in command
        assert "/runs/abc123/result.json" in command
    finally:
        store.close()


def test_docker_pool_key_includes_effective_network(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=1)
        monkeypatch.setattr("spl.daemon.docker_pool.platform.system", lambda: "Linux")
        config = {"mode": "docker", "network": "auto"}

        local_key = runtime.docker_pool.pool_key(
            "splime-runtime:demo",
            config,
            {"pipeline_nodes": []},
        )
        remote_key = runtime.docker_pool.pool_key(
            "splime-runtime:demo",
            config,
            {"pipeline_nodes": [{"kind": "remote"}]},
        )

        assert local_key != remote_key
    finally:
        store.close()


def test_docker_pool_records_exec_lock(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=1)
        monkeypatch.setattr(
            runtime.docker_pool,
            "start_container",
            lambda **kwargs: {
                "key": kwargs["key"],
                "name": "splime-pool-test",
                "image_tag": kwargs["image_tag"],
            },
        )
        monkeypatch.setattr(runtime.docker_pool, "container_running", lambda name: True)

        record = runtime.docker_pool.ensure_container(
            object_record={"pipeline_nodes": []},
            image_tag="splime-runtime:demo",
            runtime_config={"mode": "docker", "network": "auto"},
        )

        assert "exec_lock" in record
    finally:
        store.close()


def test_docker_pool_idle_eviction_skips_in_use_containers(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    removed: list[str] = []
    try:
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            docker_pool_size=2,
            docker_idle_timeout_seconds=1,
        )
        monkeypatch.setattr(runtime.docker_pool, "remove_container", lambda name: removed.append(name))
        runtime.docker_pool._containers = {
            "busy": {
                "name": "splime-pool-busy",
                "last_used": 1.0,
                "in_use": True,
            },
            "idle": {
                "name": "splime-pool-idle",
                "last_used": 1.0,
                "in_use": False,
            },
        }

        runtime.docker_pool.evict_idle_locked(now=10.0)

        assert removed == ["splime-pool-idle"]
        assert "busy" in runtime.docker_pool._containers
        assert "idle" not in runtime.docker_pool._containers
    finally:
        store.close()


def test_docker_pool_lru_eviction_skips_in_use_containers(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    removed: list[str] = []
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=1)
        monkeypatch.setattr(runtime.docker_pool, "remove_container", lambda name: removed.append(name))
        runtime.docker_pool._containers = {
            "busy": {
                "name": "splime-pool-busy",
                "last_used": 1.0,
                "in_use": True,
            },
            "idle": {
                "name": "splime-pool-idle",
                "last_used": 2.0,
                "in_use": False,
            },
        }

        runtime.docker_pool.evict_excess_locked(reserve=1)

        assert removed == ["splime-pool-idle"]
        assert "busy" in runtime.docker_pool._containers
        assert "idle" not in runtime.docker_pool._containers
    finally:
        store.close()


def test_dockerfile_includes_apt_packages(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={
                "mode": "docker",
                "python": "3.13",
                "apt_packages": ["libgomp1"],
            },
        )
        spec = runtime.docker_environment_manager.build_spec(record)
        dockerfile = runtime.docker_environment_manager._dockerfile(spec)

        assert "apt-get install -y --no-install-recommends libgomp1" in dockerfile
    finally:
        store.close()


def test_docker_ready_record_becomes_absent_when_image_is_missing(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13"},
        )
        manager = runtime.docker_environment_manager
        spec = manager.build_spec(record)
        store.upsert_environment_build(
            spec_hash=spec["spec_hash"],
            base_python=spec["base_python"],
            python_version=spec["python_version"],
            distributions=spec["distributions"],
            runtime_packages=spec["runtime_packages"],
            spec=spec["spec"],
            venv_path=spec["venv_path"],
            python_path=Path(spec["python_path"]),
            install_log_path=spec["install_log_path"],
            status="ready",
            runtime_type="docker",
            image_tag=spec["image_tag"],
            base_image=spec["base_image"],
        )
        monkeypatch.setattr(manager, "_image_exists", lambda image_tag: False)

        status = manager.status_for_object(record)

        assert status["status"] == "absent"
        assert status["error"] == "cached Docker image is missing from local Docker daemon"
    finally:
        store.close()


def test_docker_pull_config_updates_build_command(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    commands: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13", "pull": True},
        )
        manager = runtime.docker_environment_manager
        spec = manager.build_spec(record)
        monkeypatch.setattr("spl.daemon.docker_environment.shutil.which", lambda _: "/usr/bin/docker")
        monkeypatch.setattr("spl.daemon.docker_environment.subprocess.run", fake_run)

        manager._build_environment(spec)

        assert commands[0] == ["docker", "info"]
        assert commands[1][0:2] == ["docker", "build"]
        assert "--pull=true" in commands[1]
    finally:
        store.close()


def test_docker_run_reports_clear_error_when_executable_is_missing(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False)
        monkeypatch.setattr("spl.daemon.runtime_backend.shutil.which", lambda _: None)

        try:
            runtime.runtime_backends.backend_for({"runtime_config": {"mode": "docker"}}).ensure_ready({})
        except RuntimeError as exc:
            assert "docker executable is not available" in str(exc)
        else:
            raise AssertionError("expected missing docker executable to fail")
    finally:
        store.close()


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        completed = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        )
    except Exception:
        return False
    return completed.returncode == 0


@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_docker_runtime_end_to_end_runs_function_in_a_container(tmp_path) -> None:
    """End-to-end: build a Docker image and run an object inside a container.

    Requires a working local Docker (skipped otherwise) and network access to
    pull the base image + pyyaml on first run.  Exercises the full one-shot
    launch path: image build, bind mounts, hardening, the worker protocol, the
    artifact flow, and host-side artifact path rewriting.
    """

    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False)
    try:
        store.register_env("default", sys.executable)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13"},
        )

        build = runtime.docker_environment_manager.ensure_ready(record, wait=True)
        assert build["status"] == "ready", build.get("error")
        assert build["image_tag"].startswith("splime-runtime:")

        started = runtime.start_run(
            "artifact_func",
            source="local",
            report_local_run=False,
            timeout_seconds=600,
        )
        final = _wait_for_run(store, started["id"], timeout_seconds=600)

        assert final["status"] == "succeeded", final.get("error")
        assert final["runtime_backend"] == "docker"
        assert final["image_tag"] == build["image_tag"]
        assert final["result"]["result"] == {"answer": 7}

        artifact_path = Path(final["artifacts_dir"]) / "artifact.txt"
        assert artifact_path.read_text(encoding="utf-8") == "daemon artifact"
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_docker_runtime_reuses_warm_pool_container(tmp_path) -> None:
    """A pooled run uses `docker exec` into a reusable warm container."""

    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=2)
    try:
        store.register_env("default", sys.executable)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13"},
        )
        runtime.docker_environment_manager.ensure_ready(record, wait=True)

        results = []
        for _ in range(2):
            started = runtime.start_run(
                "artifact_func",
                source="local",
                report_local_run=False,
                timeout_seconds=600,
            )
            results.append(_wait_for_run(store, started["id"], timeout_seconds=600))

        assert [r["status"] for r in results] == ["succeeded", "succeeded"]
        # Exactly one warm container should be backing both runs.
        assert len(runtime.docker_pool) == 1
        for run in results:
            assert run["result"]["result"] == {"answer": 7}
    finally:
        runtime.shutdown()
        store.close()


def test_prepare_remote_run_artifacts_splits_inline_and_direct_uploads(
    tmp_path,
    monkeypatch,
) -> None:
    class UploadingServerClient:
        uploads: list[dict[str, Any]] = []

        def __init__(
            self,
            base_url: str,
            machine_token: str,
            *,
            user_token: str | None = None,
        ):
            self.base_url = base_url
            self.machine_token = machine_token
            self.user_token = user_token

        def upload_artifact(
            self,
            run_id: str,
            name: str,
            path: str | Path,
        ) -> dict[str, Any]:
            payload = Path(path).read_bytes()
            record = {
                "id": f"artifact-{name}",
                "run_id": run_id,
                "name": name,
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
            self.uploads.append(record)
            return record

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES", 8)
    try:
        runtime = DaemonRuntime(store, server_client_factory=UploadingServerClient)
        connection = store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-123456",
            user_token="user-token-123456",
            connection={
                "id": "remote-connection-1",
                "owner_id": "owner-1",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": "machine-1",
                "display_name": "lab-machine",
                "status": "connected",
                "capabilities": {},
            },
            heartbeat_interval_seconds=60,
        )
        artifacts_dir = tmp_path / "artifacts"
        artifacts_dir.mkdir()
        small = artifacts_dir / "small.txt"
        large = artifacts_dir / "large.bin"
        small.write_bytes(b"tiny")
        large.write_bytes(b"larger-than-inline")

        prepared = runtime._prepare_remote_run_artifacts(
            connection["id"],
            "remote-run-1",
            {"artifacts_dir": str(artifacts_dir)},
        )

        assert [item["name"] for item in prepared] == ["large.bin", "small.txt"]
        large_item, small_item = prepared
        assert large_item["transfer_mode"] == "direct_upload"
        assert large_item["uploaded"] is True
        assert "data_base64" not in large_item
        assert small_item["transfer_mode"] == "inline_base64"
        assert base64.b64decode(small_item["data_base64"]) == b"tiny"
        assert UploadingServerClient.uploads == [
            {
                "id": "artifact-large.bin",
                "run_id": "remote-run-1",
                "name": "large.bin",
                "size": len(b"larger-than-inline"),
                "sha256": hashlib.sha256(b"larger-than-inline").hexdigest(),
            }
        ]
    finally:
        store.close()


def test_prepare_remote_run_artifacts_rejects_mismatched_direct_upload(
    tmp_path,
    monkeypatch,
) -> None:
    class MismatchedServerClient:
        def __init__(
            self,
            base_url: str,
            machine_token: str,
            *,
            user_token: str | None = None,
        ):
            pass

        def upload_artifact(
            self,
            run_id: str,
            name: str,
            path: str | Path,
        ) -> dict[str, Any]:
            return {
                "id": f"artifact-{name}",
                "run_id": run_id,
                "name": name,
                "size": Path(path).stat().st_size,
                "sha256": "0" * 64,
            }

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", MismatchedServerClient)
    monkeypatch.setattr(daemon_server, "DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES", 0)
    try:
        runtime = DaemonRuntime(store)
        connection = store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-123456",
            user_token="user-token-123456",
            connection={
                "id": "remote-connection-1",
                "owner_id": "owner-1",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": "machine-1",
                "display_name": "lab-machine",
                "status": "connected",
                "capabilities": {},
            },
            heartbeat_interval_seconds=60,
        )
        artifacts_dir = tmp_path / "artifacts"
        artifacts_dir.mkdir()
        (artifacts_dir / "model.bin").write_bytes(b"weights")

        try:
            runtime._prepare_remote_run_artifacts(
                connection["id"],
                "remote-run-1",
                {"artifacts_dir": str(artifacts_dir)},
            )
        except RuntimeError as exc:
            assert "checksum mismatch" in str(exc)
        else:
            raise AssertionError("mismatched upload should fail before run_update")
    finally:
        store.close()


def test_pipeline_internal_function_runs_in_parent_environment(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store)

        record = runtime.register_object(
            "demo_pipeline",
            "demo_pipeline",
            "default",
            yaml_text=PIPELINE_WITH_INTERNAL_FUNCTION_YAML,
        )
        build = _mark_object_environment_ready(runtime, record)

        started = runtime.start_run(
            "demo_pipeline",
            kwargs={"a": 2, "b": 5},
            function="inner_add",
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["entrypoint"] == "inner_add"
        assert final["input"]["function"] == "inner_add"
        assert final["env_build_hash"] == build["spec_hash"]
        assert final["result"]["result"] == 7
    finally:
        store.close()


def test_remote_import_mirrors_server_versions_with_source_identity(
    tmp_path,
    monkeypatch,
) -> None:
    class ImportServerClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_object(self, name_or_id, *, version=None, include_yaml=False):
            assert name_or_id == "demo_obj"
            assert include_yaml is False
            return {
                "id": "remote-object-1",
                "owner_id": "owner-1",
                "name": "demo_obj",
                "version": 2,
                "version_id": "remote-version-2",
                "entrypoint": "demo_obj",
                "env": "default",
            }

        def list_object_versions(self, name_or_id, *, include_yaml=False):
            assert name_or_id == "demo_obj"
            assert include_yaml is True
            return [
                {
                    "id": "remote-object-1",
                    "owner_id": "owner-1",
                    "name": "demo_obj",
                    "version": 1,
                    "version_id": "remote-version-1",
                    "entrypoint": "demo_obj",
                    "env": "default",
                    "description": "first",
                    "version_label": "v1",
                    "yaml": REMOTE_FUNCTION_YAML,
                },
                {
                    "id": "remote-object-1",
                    "owner_id": "owner-1",
                    "name": "demo_obj",
                    "version": 2,
                    "version_id": "remote-version-2",
                    "entrypoint": "demo_obj",
                    "env": "default",
                    "description": "second",
                    "version_label": "v2",
                    "yaml": REMOTE_FUNCTION_YAML,
                },
            ]

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", ImportServerClient)
    try:
        store.register_env("default", sys.executable)
        store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-123456",
            user_token="user-token-123456",
            connection={
                "id": "remote-connection-1",
                "owner_id": "owner-1",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": "machine-1",
                "display_name": "lab-machine",
                "status": "connected",
                "capabilities": {},
            },
            heartbeat_interval_seconds=60,
        )
        runtime = DaemonRuntime(store)

        imported = runtime.import_server_object("demo_obj")

        assert imported["refreshed"] is True
        assert imported["current_version"]["name"] == "demo_obj"
        assert imported["current_version"]["version"] == 1
        assert len(imported["versions"]) == 2

        current = imported["current_version"]
        assert current["local_registry_name"] == "demo_obj"
        assert current["owner_id"] == "owner-1"
        assert current["library"] == "default"
        assert current["source_owner_id"] == "owner-1"
        assert current["source_object_id"] == "remote-object-1"
        assert current["source_object_name"] == "demo_obj"
        assert current["remote_identity"]["source_version_id"] == "remote-version-1"
        assert current["remote_name"] == "demo_obj"
    finally:
        store.close()


def test_remote_node_can_target_pipeline_internal_function(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
        calls: dict[str, Any] = {}

        def resolve_remote_signature(ref: dict[str, Any]) -> dict[str, Any]:
            calls["resolve_ref"] = ref
            return {
                "id": "remote-object-1",
                "version_id": "remote-version-1",
                "kind": "function",
                "function": "inner_add",
                "outputs": [{"name": "default", "selector": None}],
                "remote_ref": {"owner_id": "owner-1", "library": "default"},
                "execution": {"default_machine_id": "machine-1"},
            }

        def start_remote_run(object_name: str, **kwargs: Any) -> dict[str, Any]:
            calls["start"] = {"object_name": object_name, **kwargs}
            return {"id": "remote-run-1"}

        def wait_server_run(run_id: str, **kwargs: Any) -> dict[str, Any]:
            calls["wait"] = {"run_id": run_id, **kwargs}
            return {"status": "succeeded", "result": {"result": 12}}

        monkeypatch.setattr(runtime, "resolve_remote_signature", resolve_remote_signature)
        monkeypatch.setattr(runtime, "start_remote_run", start_remote_run)
        monkeypatch.setattr(runtime, "_wait_server_run", wait_server_run)

        result = runtime.run_remote_node(
            {
                "url": "https://splime.io/api",
                "name": "demo_pipeline::inner_add",
                "version": "latest",
            },
            kwargs={"a": 5, "b": 7},
        )

        assert result["value"] == 12
        assert result["run_id"] == "remote-run-1"
        assert result["status"] == "succeeded"
        assert result["run"] == {"status": "succeeded", "result": {"result": 12}}
        assert result["payload"] == {"result": 12}
        assert result["artifacts"] == {}
        assert calls["resolve_ref"]["object_name"] == "demo_pipeline"
        assert calls["resolve_ref"]["function"] == "inner_add"
        assert calls["start"]["object_name"] == "remote-object-1"
        assert calls["start"]["function"] == "inner_add"
        assert calls["start"]["kwargs"] == {"a": 5, "b": 7}
    finally:
        store.close()


def test_remote_import_auto_registers_missing_server_env(
    tmp_path,
    monkeypatch,
) -> None:
    class ImportServerClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_object(self, name_or_id, *, version=None, include_yaml=False):
            assert name_or_id == "demo_obj"
            assert include_yaml is False
            return {
                "id": "remote-object-1",
                "owner_id": "owner-1",
                "name": "demo_obj",
                "version": 1,
                "version_id": "remote-version-1",
                "entrypoint": "demo_obj",
                "env": "spl_core",
            }

        def list_object_versions(self, name_or_id, *, include_yaml=False):
            assert name_or_id == "demo_obj"
            assert include_yaml is True
            return [
                {
                    "id": "remote-object-1",
                    "owner_id": "owner-1",
                    "name": "demo_obj",
                    "version": 1,
                    "version_id": "remote-version-1",
                    "entrypoint": "demo_obj",
                    "env": "spl_core",
                    "description": "first",
                    "version_label": "v1",
                    "yaml": REMOTE_FUNCTION_YAML,
                },
            ]

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", ImportServerClient)
    try:
        default_env = store.register_env("default", sys.executable)
        store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-123456",
            user_token="user-token-123456",
            connection={
                "id": "remote-connection-1",
                "owner_id": "owner-1",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": "machine-1",
                "display_name": "lab-machine",
                "status": "connected",
                "capabilities": {},
            },
            heartbeat_interval_seconds=60,
        )
        runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())

        imported = runtime.import_server_object("demo_obj")

        assert imported["refreshed"] is True
        assert imported["current_version"]["env"] == "spl_core"
        assert store.get_env("spl_core")["python"] == default_env["python"]
    finally:
        store.close()


def _existing_python(path: Path) -> str:
    path.write_text("", encoding="utf-8")
    return str(path)


def test_server_origin_resolver_uses_local_env_by_name_before_provenance(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        local_python = store.register_env("spl_core", _existing_python(tmp_path / "local-python"))["python"]
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = {
            "origin": "server",
            "env": "spl_core",
            "env_python": sys.executable,
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] == local_python
    finally:
        store.close()


def test_server_origin_resolver_falls_back_to_default_env_when_named_env_is_missing(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        default_python = store.register_env("default", _existing_python(tmp_path / "default-python"))["python"]
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = {
            "origin": "server",
            "env": "spl_core",
            "env_python": str(tmp_path / "author-python"),
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] == default_python
    finally:
        store.close()


def test_server_origin_resolver_falls_back_to_daemon_python_without_local_envs(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = {
            "origin": "server",
            "env": "spl_core",
            "env_python": str(tmp_path / "author-python"),
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] == str(Path(sys.executable).expanduser().absolute())
    finally:
        store.close()


def test_local_origin_resolver_uses_stored_python_when_live(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        local_python = _existing_python(tmp_path / "local-python")
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = {
            "origin": "local",
            "env": "spl_core",
            "env_python": local_python,
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] == str(Path(local_python).expanduser().absolute())
    finally:
        store.close()


def test_local_origin_dead_python_fails_without_default_substitution(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        default_env = store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)
        missing_python = str(tmp_path / "missing-python")
        record = {
            "origin": "local",
            "env": "default",
            "env_python": missing_python,
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] != default_env["python"]
        assert spec["base_python"] == str(Path(missing_python).expanduser().absolute())
        with pytest.raises(EnvironmentBuildError):
            runtime.environment_manager.ensure_ready(record, wait=True)
    finally:
        store.close()


def test_server_mirror_reregister_keeps_env_python_provenance_and_resolves_locally(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        default_env = store.register_env("default", sys.executable)
        store.register_env("default1", sys.executable)
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
        )
        first = runtime.register_object(
            "demo_obj",
            "demo_obj",
            "default1",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-1",
            library="default",
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )
        author_python = str(tmp_path / "author-python")
        with store._lock, store._conn:  # noqa: SLF001 - regression seeds server provenance.
            store._conn.execute(
                "UPDATE envs SET python = ? WHERE name = ?",
                (str(tmp_path / "missing-local-python"), "default1"),
            )
            store._conn.execute(
                "UPDATE object_versions SET env_python = ? WHERE id = ?",
                (author_python, first["version_id"]),
            )
        runtime._ensure_server_object_envs([{"env": "default1"}])

        mirrored = runtime.register_object(
            "demo_obj",
            "demo_obj",
            "default1",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-1",
            library="default",
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )
        spec = runtime.environment_manager.build_spec(mirrored)

        assert mirrored["version_id"] == first["version_id"]
        assert mirrored["env_python"] == author_python
        assert spec["base_python"] == default_env["python"]
    finally:
        store.close()


def test_server_origin_interpreter_substitution_is_logged_and_reported(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        local_env = store.register_env("spl_core", sys.executable)
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
        )
        record = runtime.register_object(
            "demo_obj",
            "demo_obj",
            "spl_core",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-1",
            library="default",
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )
        author_python = str(tmp_path / "author-python")
        with store._lock, store._conn:  # noqa: SLF001 - regression seeds server provenance.
            store._conn.execute(
                "UPDATE object_versions SET env_python = ? WHERE id = ?",
                (author_python, record["version_id"]),
            )
        record = store.get_object_version(record["version_id"])
        _mark_object_environment_ready(runtime, record)

        caplog.set_level(logging.INFO, logger="spl.daemon.server")
        started = runtime.start_run(
            "demo_obj",
            source="local",
            object_version_id=record["version_id"],
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["interpreter_substitution"] is not None
        assert final["interpreter_substitution"]["authored_python"] == author_python
        assert final["interpreter_substitution"]["resolved_python"] == local_env["python"]
        assert final["interpreter_substitution"]["reason"] == "local_env"
        records = [
            log_record
            for log_record in caplog.records
            if getattr(log_record, "spl_event", None) == "interpreter_substitution"
        ]
        assert len(records) == 1
        payload = records[0].interpreter_substitution
        assert payload["object"] == "demo_obj"
        assert payload["version_id"] == record["version_id"]
        assert payload["authored_python"] == author_python
        assert payload["resolved_python"] == local_env["python"]
    finally:
        store.close()
