from __future__ import annotations

import ast
import inspect
import json
import re
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any, Mapping, cast

import pytest
import yaml

from spl.core import node_runtime as m_node_runtime
from spl import Deployment, lift
from spl.core import manifest as m_manifest
from spl.core.entities.function import DFunction, METADATA_DUNDER_NAME
from spl.core.entities.node import DEFAULT_PORT, InputPort, OutputPort
from spl.core.entities.node_function import NodeFunction
from spl.core.entities.pipeline import DPipeline, Pipeline
from spl.core.ir.parse import ir_parse
from spl.core.ir.utils import SPLSafeLoader
from spl.daemon.canonical import _canonical_spl_documents
from spl.daemon.spl_free_generator import filter_spl_runtime_scaffolding


class _StaticNodeEnvironmentProvider:
    def __init__(self, metadata: dict[str, Any] | None = None):
        self.metadata = metadata or {}

    def prepare(
        self,
        spec: Mapping[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> m_node_runtime.PreparedNodeEnvironment:
        del spec, wait, retry_failed
        return m_node_runtime.PreparedNodeEnvironment(name="test-env", python_path=None, metadata=dict(self.metadata))


def _seed_text() -> str:
    return "seed"


def _upper_text(value: str) -> str:
    return value.upper()


def _double_value(value: int) -> int:
    return value * 2


def _spl_import_probe() -> bool:
    import sys

    return "spl" in sys.modules


def _exit_process() -> int:
    import os

    os._exit(2)


def _describe_value(value: Any) -> str:
    return type(value).__name__


def _source_only_value() -> str:
    return "source"


def _slow_marker(marker_path: str) -> str:
    import time
    from pathlib import Path

    print("started", flush=True)
    time.sleep(1.2)
    Path(marker_path).write_text("finished", encoding="utf-8")
    return "finished"


def _save_text(path: str, value: str) -> None:
    Path(path).write_text(value, encoding="utf-8")


def _load_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _runtime_pipeline(*, tag_consumer: bool = True) -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(_seed_text).alias("producer")
    pipeline = lift_any(_upper_text).bind(value=producer.as_format("txt")).alias("consumer").render("node_runtime")
    pipeline = pipeline.add_adapter(str, "txt", save=_save_text, load=_load_text)
    return pipeline.with_node_runtime("consumer", "venv-subprocess") if tag_consumer else pipeline


def _docker_runtime_context(
    tmp_path: Path,
    *,
    runtime_config: dict[str, Any] | None = None,
    provider: m_node_runtime.NodeEnvironmentProvider | None = None,
) -> m_node_runtime.NodeRuntimeContext:
    node = NodeFunction(_double_value)
    [input_port] = node.inputs
    return m_node_runtime.NodeRuntimeContext(
        node=node,
        node_label="double",
        inputs={input_port: 21},
        output_port=node.get_output_port(DEFAULT_PORT),
        callback=lambda _node, _inputs: {"default": 42},
        work_dir=tmp_path / "runs" / "run-123" / "node-runtimes" / str(node.uuid),
        environment_provider=provider or _StaticNodeEnvironmentProvider(),
        runtime_config=runtime_config or {},
        environment_spec=[],
    )


def _read_manifest(run: Any) -> dict[str, Any]:
    assert run.manifest_path is not None
    return cast(dict[str, Any], json.loads(run.manifest_path.read_text(encoding="utf-8")))


def _node_by_alias(manifest: dict[str, Any], alias: str) -> dict[str, Any]:
    return cast(dict[str, Any], next(node for node in manifest["nodes"].values() if node["alias"] == alias))


def test_pipeline_runtime_tags_are_additive_yaml_and_canonical() -> None:
    old_yaml = textwrap.dedent("""
    - !DPipeline
      name: old_pipeline
      nodes: []
      links: []
      aliases: []
      adapters: []
    """)

    loaded = cast(list[Any], yaml.load(old_yaml, Loader=SPLSafeLoader))[0]
    assert isinstance(loaded, DPipeline)
    assert loaded.tags == {}
    assert _canonical_spl_documents(old_yaml)[0]["root"] == {
        "tag": "DPipeline",
        "name": "old_pipeline",
        "nodes": [],
        "links": [],
        "aliases": [],
        "adapters": [],
    }

    pipeline = _runtime_pipeline()
    root = cast(DPipeline, ir_parse(pipeline, name="pipeline").mk_root())
    dumped = yaml.dump(root, sort_keys=False)
    loaded_with_tags = cast(DPipeline, yaml.load(dumped, Loader=SPLSafeLoader))

    assert root.tags
    assert loaded_with_tags.tags == root.tags
    assert "runtime: venv-subprocess" in dumped


def test_node_runtime_tag_runs_in_subprocess_and_records_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_runtime_pipeline()).run(keep=True)

    with run:
        assert run.value("consumer") == "SEED"

    manifest = _read_manifest(run)
    producer = _node_by_alias(manifest, "producer")
    consumer = _node_by_alias(manifest, "consumer")

    assert producer["runtime"]["name"] == "native"
    assert producer["runtime"]["source"] == "default"
    assert consumer["runtime"]["name"] == "venv-subprocess"
    assert consumer["runtime"]["source"] == "node-tag"
    assert consumer["runtime"]["resolved"]["python"] == sys.executable
    assert manifest["edges"][0]["artifact"]["kind"] == "artifact"
    assert manifest["edges"][0]["artifact"]["tag"] == "txt"
    assert consumer["outputs"][DEFAULT_PORT]["value"] == "SEED"
    assert m_manifest.manifest_summary(manifest)["node_runtimes"] == [
        {
            "node_id": consumer["id"],
            "alias": "consumer",
            "name": "venv-subprocess",
            "source": "node-tag",
            "config_hash": consumer["runtime"]["config_hash"],
            "resolved": {"python": sys.executable},
        },
        {
            "node_id": producer["id"],
            "alias": "producer",
            "name": "native",
            "source": "default",
            "config_hash": None,
            "resolved": {"python": sys.executable},
        },
    ]


def test_node_runtime_run_override_wins_and_unknown_alias_fails_early(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    pipeline = _runtime_pipeline(tag_consumer=False)

    with pytest.raises(ValueError, match="unknown alias `missing`"):
        Deployment(pipeline).run(runtimes={"missing": "venv-subprocess"})

    run = Deployment(pipeline).run(keep=True, runtimes={"consumer": "venv-subprocess"})
    with run:
        assert run.value("consumer") == "SEED"

    consumer = _node_by_alias(_read_manifest(run), "consumer")
    assert consumer["runtime"]["name"] == "venv-subprocess"
    assert consumer["runtime"]["source"] == "run-override"


def test_object_runtime_config_node_runtime_is_between_default_and_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_runtime_pipeline(tag_consumer=False), runtime_config={"node_runtime": "venv-subprocess"}).run(
        keep=True
    )

    with run:
        assert run.value("consumer") == "SEED"

    consumer = _node_by_alias(_read_manifest(run), "consumer")
    assert consumer["runtime"]["name"] == "venv-subprocess"
    assert consumer["runtime"]["source"] == "object-runtime-config"


def test_venv_subprocess_runner_does_not_import_spl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(_spl_import_probe)
        .alias("probe")
        .render("spl_import_probe")
        .with_node_runtime("probe", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("probe") is False


def test_venv_subprocess_generated_module_fast_path_text_is_unchanged() -> None:
    node = NodeFunction(_upper_text)
    generated = m_node_runtime._generated_node_module_text(node, "consumer")

    module = ast.parse(textwrap.dedent(inspect.getsource(_upper_text)))
    expected = ast.unparse(filter_spl_runtime_scaffolding(module)) + "\n"

    assert generated == expected


def test_venv_subprocess_uses_ir_fallback_when_getsource_reads_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setattr(
        _double_value,
        METADATA_DUNDER_NAME,
        DFunction(
            name="_double_value",
            body="return value * 2",
            inputs=[InputPort(name="value", typ_="int", default=None)],
            outputs=[OutputPort(name=DEFAULT_PORT, typ_="int")],
        ),
        raising=False,
    )
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(_double_value).alias("double").render("yaml_source").with_node_runtime("double", "venv-subprocess")
    )
    monkeypatch.setattr(m_node_runtime.inspect, "getsource", lambda _: "- !DPipeline\n")
    run = Deployment(pipeline).run(keep=True, value=21)

    with run:
        assert run.value("double") == 42


def test_venv_subprocess_reports_unrecoverable_source_before_work_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(_source_only_value)
        .alias("source_only")
        .render("source_only")
        .with_node_runtime("source_only", "venv-subprocess")
    )

    def _raise_getsource(_: Any) -> str:
        raise OSError("source unavailable")

    def _raise_ir_parse(*_: Any, **__: Any) -> Any:
        raise ValueError("IR unavailable")

    monkeypatch.setattr(m_node_runtime.inspect, "getsource", _raise_getsource)
    monkeypatch.setattr(m_node_runtime, "ir_parse", _raise_ir_parse)
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(RuntimeError) as exc_info:
        run.value("source_only")

    message = str(exc_info.value)
    assert "source_only" in message
    assert "_source_only_value" in message
    assert "source is not recoverable" in message
    assert "source-visible or IR-parsable function node" in message

    manifest = _read_manifest(run)
    node = _node_by_alias(manifest, "source_only")
    assert manifest["status"] == "failed"
    assert node["status"] == "failed"
    assert run.run_dir is not None
    assert not (run.run_dir / "node-runtimes" / node["id"]).exists()


def test_venv_subprocess_failure_records_failed_node_and_does_not_hang(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(_exit_process).alias("exit").render("exit_pipeline").with_node_runtime("exit", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(RuntimeError, match="return code 2"):
        run.value("exit")

    manifest = _read_manifest(run)
    node = _node_by_alias(manifest, "exit")
    assert manifest["status"] == "failed"
    assert node["status"] == "failed"
    assert node["runtime"]["name"] == "venv-subprocess"
    assert run.run_dir is not None
    runtime_dir = run.run_dir / "node-runtimes" / node["id"]
    assert (runtime_dir / "stdout.txt").exists()
    assert (runtime_dir / "stderr.txt").exists()


def test_venv_subprocess_rejects_non_json_inputs_before_work_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(_describe_value)
        .alias("consumer")
        .render("non_json_input")
        .with_node_runtime("consumer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True, value={1, 2})

    with pytest.raises(RuntimeError) as exc_info:
        run.value("consumer")

    message = str(exc_info.value)
    assert "consumer" in message
    assert "value" in message
    assert "builtins.set" in message
    assert "native runtime" in message
    assert "Converter Nodes For Adapter Tags" in message
    assert "artifact-file input transport (0.4.x)" in message

    manifest = _read_manifest(run)
    node = _node_by_alias(manifest, "consumer")
    assert manifest["status"] == "failed"
    assert node["status"] == "failed"
    assert run.run_dir is not None
    assert not (run.run_dir / "node-runtimes" / node["id"]).exists()


def test_venv_subprocess_node_timeout_records_failure_and_stops_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    marker_path = tmp_path / "marker.txt"
    lift_any = cast(Any, lift)
    pipeline = lift_any(_slow_marker).alias("slow").render("slow_timeout").with_node_runtime("slow", "venv-subprocess")
    run = Deployment(pipeline, runtime_config={"node_timeout_seconds": 0.8}).run(
        keep=True, marker_path=str(marker_path)
    )

    with pytest.raises(RuntimeError, match=r"node runtime `venv-subprocess` timed out after 0.8s for `_slow_marker`"):
        run.value("slow")

    manifest = _read_manifest(run)
    node = _node_by_alias(manifest, "slow")
    assert manifest["status"] == "failed"
    assert node["status"] == "failed"
    assert run.run_dir is not None
    runtime_dir = run.run_dir / "node-runtimes" / node["id"]
    assert (runtime_dir / "stdout.txt").read_text(encoding="utf-8") == "started\n"
    assert (runtime_dir / "stderr.txt").read_text(encoding="utf-8") == ""
    time.sleep(0.7)
    assert not marker_path.exists()


def test_docker_node_runtime_builds_spl_free_docker_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = m_node_runtime.DockerNodeRuntime()
    context = _docker_runtime_context(
        tmp_path,
        runtime_config={"docker": {"image": "python:3.13-slim", "network": "none"}},
    )
    environment = runtime.prepare(context)
    commands: list[list[str]] = []

    def fake_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        (context.work_dir / "result.json").write_text('{"result": 42}', encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="node stdout\n", stderr="")

    monkeypatch.setattr(m_node_runtime.subprocess, "run", fake_run)

    assert runtime.execute(context, environment) == {DEFAULT_PORT: 42}
    assert runtime.execute(context, environment) == {DEFAULT_PORT: 42}

    first_command, second_command = commands
    first_name = first_command[first_command.index("--name") + 1]
    second_name = second_command[second_command.index("--name") + 1]
    for container_name in (first_name, second_name):
        assert re.fullmatch(r"spl-node-run-123-[0-9a-f]{8}-[0-9a-f]{6}", container_name)
        assert len(container_name) <= 63
    assert first_name != second_name

    command = first_command
    assert command[:5] == ["docker", "run", "--rm", "--name", command[4]]
    assert command[command.index("--cidfile") + 1] == str(context.work_dir / "container.cid")
    assert command[command.index("-v") + 1] == "{}:/spl-node".format(context.work_dir.resolve())
    assert command[command.index("-w") + 1] == "/spl-node"
    assert command[command.index("--network") + 1] == "none"
    assert "--read-only" in command
    assert command[command.index("--pids-limit") + 1] == "256"
    assert "PYTHONPATH" not in " ".join(command)
    image_index = command.index("python:3.13-slim")
    assert command[image_index + 1 : image_index + 3] == ["python", "/spl-node/spl_free_runner.py"]
    assert command[command.index("--module") + 1] == "/spl-node/node_module.py"
    assert command[command.index("--input") + 1] == "/spl-node/input.json"
    assert command[command.index("--result") + 1] == "/spl-node/result.json"
    assert command[command.index("--artifacts-dir") + 1] == "/spl-node/artifacts"
    assert command[command.index("--env-spec") + 1] == "/spl-node/env-spec.json"
    assert (context.work_dir / "stdout.txt").read_text(encoding="utf-8") == "node stdout\n"
    assert (context.work_dir / "stderr.txt").read_text(encoding="utf-8") == ""


def test_docker_node_runtime_timeout_kills_container(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = m_node_runtime.DockerNodeRuntime()
    context = _docker_runtime_context(
        tmp_path,
        runtime_config={
            "docker": {"image": "python:3.13-slim", "network": "none"},
            "node_timeout_seconds": 0.25,
        },
    )
    environment = runtime.prepare(context)
    commands: list[list[str]] = []

    def fake_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command[:2] == ["docker", "kill"]:
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        raise subprocess.TimeoutExpired(command, 0.25, output="started\n", stderr="")

    monkeypatch.setattr(m_node_runtime.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match=r"node runtime `docker` timed out after 0.25s for node `double`"):
        runtime.execute(context, environment)

    run_command, kill_command = commands
    assert kill_command == ["docker", "kill", run_command[run_command.index("--name") + 1]]
    assert (context.work_dir / "stdout.txt").read_text(encoding="utf-8") == "started\n"
    assert (context.work_dir / "stderr.txt").read_text(encoding="utf-8") == ""


def test_docker_node_runtime_rejects_nested_object_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_OBJECT_RUNTIME_BACKEND", "docker")
    runtime = m_node_runtime.DockerNodeRuntime()
    context = _docker_runtime_context(
        tmp_path,
        runtime_config={"docker": {"image": "python:3.13-slim"}},
    )

    with pytest.raises(
        RuntimeError,
        match="nested docker runtimes are not supported; keep the object runtime on venv or drop the node tag",
    ):
        runtime.prepare(context)


def test_docker_node_runtime_requires_image_tag_without_daemon(tmp_path: Path) -> None:
    runtime = m_node_runtime.DockerNodeRuntime()
    context = _docker_runtime_context(tmp_path)

    with pytest.raises(RuntimeError) as exc_info:
        runtime.prepare(context)

    message = str(exc_info.value)
    assert 'runtime_config["docker"]["image"]' in message
    assert "run the object through the daemon" in message


def test_docker_runtime_manifest_uses_image_tag() -> None:
    environment = m_node_runtime.PreparedNodeEnvironment(
        name="docker-image",
        python_path=None,
        metadata={"image_tag": "python:3.13-slim", "spec_hash": "docker-hash"},
    )
    record = m_node_runtime.runtime_manifest_record(
        m_node_runtime.NodeRuntimeResolution(
            m_node_runtime.DOCKER_NODE_RUNTIME,
            m_node_runtime.NodeRuntimeResolutionSource.NODE_TAG,
        ),
        environment,
    )

    assert record["name"] == "docker"
    assert record["config_hash"] == "docker-hash"
    assert record["resolved"] == {"image_tag": "python:3.13-slim"}


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    completed = subprocess.run(
        ["docker", "info"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=15,
        check=False,
    )
    return completed.returncode == 0


def _ensure_docker_image(image: str) -> None:
    inspected = subprocess.run(
        ["docker", "image", "inspect", image],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=15,
        check=False,
    )
    if inspected.returncode == 0:
        return
    pulled = subprocess.run(
        ["docker", "pull", image],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        timeout=180,
        check=False,
    )
    if pulled.returncode != 0:
        pytest.skip("Docker image is not available for e2e: {}".format((pulled.stderr or "").strip() or image))


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_docker_node_runtime_explicit_image_local_deployment_e2e(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    image = "python:3.13-slim"
    _ensure_docker_image(image)
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    pipeline = _runtime_pipeline(tag_consumer=False).with_node_runtime("consumer", "docker")
    run = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}},
    ).run(keep=True)

    with run:
        assert run.value("consumer") == "SEED"

    consumer = _node_by_alias(_read_manifest(run), "consumer")
    assert consumer["runtime"]["name"] == "docker"
    assert consumer["runtime"]["source"] == "node-tag"
    assert consumer["runtime"]["resolved"] == {"image_tag": image}


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_docker_node_runtime_timeout_removes_container(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    image = "python:3.13-slim"
    _ensure_docker_image(image)
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    marker_path = tmp_path / "marker.txt"
    lift_any = cast(Any, lift)
    pipeline = lift_any(_slow_marker).alias("slow").render("slow_timeout").with_node_runtime("slow", "docker")
    run = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}, "node_timeout_seconds": 0.5},
    ).run(keep=True, marker_path=str(marker_path))
    node = pipeline.aliases["slow"]
    container_name_prefix = "spl-node-{}-{}-".format(
        m_node_runtime._docker_name_token(run.run_id)[:32],
        str(node.uuid).replace("-", "")[:8],
    )

    with pytest.raises(RuntimeError, match=r"node runtime `docker` timed out after 0.5s for node `slow`"):
        run.value("slow")

    matching_names: list[str] = []
    for _ in range(25):
        completed = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=15,
            check=False,
        )
        matching_names = [name for name in completed.stdout.splitlines() if name.startswith(container_name_prefix)]
        if not matching_names:
            break
        time.sleep(0.2)
    else:
        raise AssertionError("timed-out Docker node container was not removed: {}".format(", ".join(matching_names)))
    time.sleep(0.8)
    assert not marker_path.exists()


@pytest.mark.parametrize("timeout_value", ["1", 0, -1, True])
def test_node_timeout_runtime_config_is_validated_early(timeout_value: Any) -> None:
    deployment = Deployment(_runtime_pipeline(), runtime_config={"node_timeout_seconds": timeout_value})

    with pytest.raises(ValueError, match=r"node_timeout_seconds"):
        deployment.run()
