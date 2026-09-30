from __future__ import annotations

import _thread
import ast
import inspect
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import threading
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
from spl.daemon.runtime_config import normalize_runtime_config, runtime_config_for_run
from spl.daemon.spl_free_generator import filter_spl_runtime_scaffolding
from spl.daemon import spl_free_runner as m_spl_free_runner


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


def _noisy_system_exit() -> int:
    import sys

    print("stdout-marker-" + "o" * 100_000, flush=True)
    print("stderr-marker-" + "e" * 100_000, file=sys.stderr, flush=True)
    raise SystemExit("secret user SystemExit detail")


def _describe_value(value: Any) -> str:
    return type(value).__name__


def _accept_bytearray(value: bytearray) -> bytearray:
    return value


def _accept_memoryview(value: memoryview) -> memoryview:
    return value


def _accept_type(value: type) -> type:
    return value


def _accept_range(value: range) -> range:
    return value


def _accept_base_exception(value: BaseException) -> BaseException:
    return value


def _source_only_value() -> str:
    return "source"


def _slow_marker(marker_path: str) -> str:
    import time
    from pathlib import Path

    print("started", flush=True)
    time.sleep(1.2)
    Path(marker_path).write_text("finished", encoding="utf-8")
    return "finished"


def _very_slow_marker(marker_path: str) -> str:
    import time
    from pathlib import Path

    print("started", flush=True)
    time.sleep(10)
    Path(marker_path).write_text("finished", encoding="utf-8")
    return "finished"


def _replace_runtime_diagnostic(stream: str, kind: str) -> int:
    import os
    from pathlib import Path

    diagnostic = Path("{}.txt".format(stream))
    canary = Path("diagnostic-canary.bin")
    diagnostic.unlink()
    if kind == "symlink":
        diagnostic.symlink_to(canary.name)
    elif kind == "hardlink":
        os.link(canary, diagnostic)
    elif kind == "fifo":
        os.mkfifo(diagnostic)
    elif kind == "regular":
        diagnostic.write_bytes(canary.read_bytes())
    else:
        raise ValueError("unsupported diagnostic replacement")
    return 7


def save_text(path: str, value: str) -> None:
    from pathlib import Path

    Path(path).write_text(value, encoding="utf-8")


def load_text(path: str) -> str:
    from pathlib import Path

    return Path(path).read_text(encoding="utf-8")


def _runtime_pipeline(*, tag_consumer: bool = True) -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(_seed_text).alias("producer")
    pipeline = lift_any(_upper_text).bind(value=producer.as_format("txt")).alias("consumer").render("node_runtime")
    pipeline = pipeline.add_adapter(str, "txt", save=save_text, load=load_text)
    return cast(Pipeline, pipeline.with_node_runtime("consumer", "venv-subprocess") if tag_consumer else pipeline)


def _docker_runtime_context(
    tmp_path: Path,
    *,
    runtime_config: dict[str, Any] | None = None,
    provider: m_node_runtime.NodeEnvironmentProvider | None = None,
) -> m_node_runtime.NodeRuntimeContext:
    node = NodeFunction(cast(Any, _double_value))
    [input_port] = node.inputs
    return m_node_runtime.NodeRuntimeContext(
        node=node,
        node_label="double",
        inputs={input_port: 21},  # type: ignore[dict-item]  # Direct runtime-boundary fixture.
        output_port=node.get_output_port(DEFAULT_PORT),
        callback=lambda _node, _inputs: {"default": 42},
        work_dir=tmp_path / "runs" / "run-123" / "node-runtimes" / str(node.uuid),
        environment_provider=provider or _StaticNodeEnvironmentProvider(),
        runtime_config=runtime_config or {},
        environment_spec=[],
    )


def _clear_daemon_ownership_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "SPL_DAEMON_INSTANCE_ID",
        "SPL_DAEMON_HOME_HASH",
        "SPL_DAEMON_GENERATION",
        "SPL_DAEMON_RUN_ID",
    ):
        monkeypatch.delenv(name, raising=False)


def _docker_inspect_payload(name: str, labels: Mapping[str, str] | None = None) -> str:
    return json.dumps([{"Name": "/{}".format(name), "Config": {"Labels": dict(labels or {})}}])


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


def test_string_runtime_override_applies_to_every_function_node(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_runtime_pipeline(tag_consumer=False)).run(
        keep=True,
        runtimes="venv-subprocess",
    )

    with run:
        assert run.value("consumer") == "SEED"

    manifest = _read_manifest(run)
    producer = _node_by_alias(manifest, "producer")
    consumer = _node_by_alias(manifest, "consumer")
    assert producer["runtime"]["name"] == "venv-subprocess"
    assert producer["runtime"]["source"] == "run-override"
    assert consumer["runtime"]["name"] == "venv-subprocess"
    assert consumer["runtime"]["source"] == "run-override"


def test_pipeline_node_docker_override_uses_non_docker_conductor() -> None:
    object_runtime = {
        "mode": "docker",
        "base_image": "python:3.13-slim-trixie",
        "docker": {"network": "none"},
    }

    effective = runtime_config_for_run(
        "pipeline",
        object_runtime,
        {"heavy_step": "docker"},
    )

    assert effective == {
        "mode": "venv",
        "docker": {"network": "none"},
    }


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


def test_spl_free_runner_blocks_accidentally_installed_spl_package(tmp_path: Path) -> None:
    module_path = tmp_path / "node.py"
    module_path.write_text(
        "def run():\n"
        "    try:\n"
        "        import spl\n"
        "    except ModuleNotFoundError:\n"
        "        return False\n"
        "    return True\n",
        encoding="utf-8",
    )
    input_path = tmp_path / "input.json"
    input_path.write_text('{"args":[],"kwargs":{}}', encoding="utf-8")
    result_path = tmp_path / "result.json"

    m_spl_free_runner.execute(
        module_path=module_path,
        module_name="spl_import_attempt",
        entrypoint="run",
        input_path=input_path,
        result_path=result_path,
        artifacts_dir=tmp_path / "artifacts",
    )

    assert json.loads(result_path.read_text(encoding="utf-8"))["result"] is False


def test_venv_subprocess_generated_module_fast_path_text_is_unchanged() -> None:
    node = NodeFunction(cast(Any, _upper_text))
    generated = m_node_runtime._generated_node_module_text(node, "consumer")

    module = ast.parse(textwrap.dedent(inspect.getsource(_upper_text)))
    expected = ast.unparse(filter_spl_runtime_scaffolding(module)) + "\n"

    assert generated == expected


@pytest.mark.parametrize(
    ("function", "expected_annotation"),
    [
        (_accept_bytearray, bytearray),
        (_accept_memoryview, memoryview),
        (_accept_type, type),
        (_accept_range, range),
        (_accept_base_exception, BaseException),
    ],
)
def test_generated_module_builtin_annotation_text_is_unchanged(
    function: Any,
    expected_annotation: type[Any],
) -> None:
    del expected_annotation
    generated = m_node_runtime._generated_node_module_text(NodeFunction(function), function.__name__)
    module = ast.parse(textwrap.dedent(inspect.getsource(function)))
    expected = ast.unparse(filter_spl_runtime_scaffolding(module)) + "\n"

    assert generated == expected
    assert not generated.startswith("from __future__ import annotations")


@pytest.mark.parametrize(
    ("function", "expected_annotation"),
    [
        (_accept_bytearray, bytearray),
        (_accept_memoryview, memoryview),
        (_accept_type, type),
        (_accept_range, range),
        (_accept_base_exception, BaseException),
    ],
)
def test_generated_module_builtin_annotations_remain_runtime_objects(
    function: Any,
    expected_annotation: type[Any],
) -> None:
    generated = m_node_runtime._generated_node_module_text(NodeFunction(function), function.__name__)
    namespace: dict[str, Any] = {}
    exec(compile(generated, "<generated-node>", "exec", dont_inherit=True), namespace)

    generated_function = namespace[function.__name__]
    assert generated_function.__annotations__["value"] is expected_annotation


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


def test_venv_subprocess_bounds_diagnostics_and_closes_user_system_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(_noisy_system_exit)
        .alias("noisy")
        .render("noisy_system_exit")
        .with_node_runtime("noisy", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(RuntimeError) as exc_info:
        run.value("noisy")

    message = str(exc_info.value)
    assert "node_execution: function_failed" in message
    assert "SystemExit" not in message
    assert "secret user" not in message
    assert "Traceback" not in message
    manifest = _read_manifest(run)
    node = _node_by_alias(manifest, "noisy")
    assert run.run_dir is not None
    runtime_dir = run.run_dir / "node-runtimes" / node["id"]
    stdout = runtime_dir / "stdout.txt"
    stderr = runtime_dir / "stderr.txt"
    assert stdout.stat().st_size <= m_node_runtime._MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES
    assert stderr.stat().st_size <= m_node_runtime._MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES
    assert "stdout-marker" not in stdout.read_text(encoding="utf-8")
    assert "secret user" not in stderr.read_text(encoding="utf-8")


@pytest.mark.parametrize("body_kind", ["ascii", "multibyte-boundary", "invalid", "mixed"])
def test_runtime_diagnostic_retention_enforces_exact_utf8_byte_limit(
    tmp_path: Path,
    body_kind: str,
) -> None:
    limit = m_node_runtime._MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES
    if body_kind == "ascii":
        body = b"discarded-prefix" + b"a" * (limit + 17)
    elif body_kind == "multibyte-boundary":
        body = b"discarded-prefix" + "€".encode() * (limit // 3 + 17)
    elif body_kind == "invalid":
        body = b"\xff" * (limit + 17)
    elif body_kind == "mixed":
        body = b"discarded-prefix" * 5000 + "νέο".encode() + b"\xff\xfe" + b"newest-tail"
    else:
        raise AssertionError(body_kind)
    path = tmp_path / "diagnostics" / "stdout.txt"
    path.parent.mkdir()
    handle, identity = m_node_runtime._open_runtime_diagnostic(path)
    with handle:
        m_node_runtime._write_all(handle.fileno(), body)
        retained = m_node_runtime._retain_bounded_runtime_diagnostic(
            path,
            fallback=b"unused fallback",
            trusted_handle=handle,
            expected_identity=identity,
        )

    retained_body = path.read_bytes()
    assert retained_body == retained.encode("utf-8")
    assert len(retained_body) <= limit
    assert retained_body.decode("utf-8") == retained
    if body_kind == "ascii":
        assert retained_body == b"a" * limit
    elif body_kind == "multibyte-boundary":
        assert retained.endswith("€" * 17)
    elif body_kind == "invalid":
        assert retained and set(retained) == {"�"}
    else:
        assert retained.endswith("newest-tail")
        assert "νέο" in retained
        assert "�" in retained


def test_runtime_diagnostic_fallback_enforces_exact_utf8_byte_limit(tmp_path: Path) -> None:
    limit = m_node_runtime._MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES
    path = tmp_path / "stdout.txt"
    handle, identity = m_node_runtime._open_runtime_diagnostic(path)
    with handle:
        retained = m_node_runtime._retain_bounded_runtime_diagnostic(
            path,
            fallback=b"\xff" * (limit + 17),
            trusted_handle=handle,
            expected_identity=identity,
        )

    retained_body = path.read_bytes()
    assert retained_body == retained.encode("utf-8")
    assert len(retained_body) <= limit
    assert retained_body.decode("utf-8") == retained
    assert retained and set(retained) == {"�"}


@pytest.mark.parametrize("replacement_kind", ["symlink", "hardlink", "fifo", "regular", "directory"])
def test_runtime_diagnostic_retention_rejects_node_controlled_path_replacement(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    replacement_kind: str,
) -> None:
    path = tmp_path / "runtime" / "stdout.txt"
    path.parent.mkdir()
    canary = tmp_path / "external-canary.bin"
    canary_body = b"external-diagnostic-secret"
    canary.write_bytes(canary_body)
    handle, identity = m_node_runtime._open_runtime_diagnostic(path)
    with handle:
        m_node_runtime._write_all(handle.fileno(), b"trusted diagnostic")
        path.unlink()
        if replacement_kind == "symlink":
            path.symlink_to(canary)
        elif replacement_kind == "hardlink":
            os.link(canary, path)
        elif replacement_kind == "fifo":
            os.mkfifo(path)
        elif replacement_kind == "regular":
            path.write_bytes(canary_body)
        elif replacement_kind == "directory":
            path.mkdir()
        else:
            raise AssertionError(replacement_kind)
        retained = m_node_runtime._retain_bounded_runtime_diagnostic(
            path,
            fallback=b"safe-fallback-\xff",
            trusted_handle=handle,
            expected_identity=identity,
        )

    assert retained == "safe-fallback-�"
    if replacement_kind == "directory":
        assert path.is_dir()
    else:
        retained_body = path.read_bytes()
        assert retained_body == retained.encode("utf-8")
        assert path.lstat().st_nlink == 1
        assert canary_body.decode() not in retained_body.decode()
    assert canary.read_bytes() == canary_body
    assert canary_body.decode() not in retained
    assert canary_body.decode() not in caplog.text
    assert str(canary) not in caplog.text


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
@pytest.mark.parametrize("replacement_kind", ["symlink", "hardlink", "fifo", "regular"])
def test_docker_runtime_failure_does_not_follow_replaced_diagnostic_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    stream: str,
    replacement_kind: str,
) -> None:
    runtime = m_node_runtime.DockerNodeRuntime()
    context = _docker_runtime_context(
        tmp_path,
        runtime_config={"docker": {"image": "python:3.13-slim", "network": "none"}},
    )
    environment = runtime.prepare(context)
    canary = tmp_path / "external-runtime-canary.bin"
    canary_body = b"external-runtime-diagnostic-secret"
    canary.write_bytes(canary_body)

    def replaced_diagnostic_run(
        command: list[str],
        **_: Any,
    ) -> subprocess.CompletedProcess[str]:
        diagnostic = context.work_dir / "{}.txt".format(stream)
        diagnostic.unlink()
        if replacement_kind == "symlink":
            diagnostic.symlink_to(canary)
        elif replacement_kind == "hardlink":
            os.link(canary, diagnostic)
        elif replacement_kind == "fifo":
            os.mkfifo(diagnostic)
        elif replacement_kind == "regular":
            diagnostic.write_bytes(canary_body)
        else:
            raise AssertionError(replacement_kind)
        return subprocess.CompletedProcess(
            command,
            9,
            stdout="safe stdout fallback",
            stderr="safe stderr fallback",
        )

    monkeypatch.setattr(m_node_runtime, "run_process_tree", replaced_diagnostic_run)

    with pytest.raises(RuntimeError, match=r"return code 9") as exc_info:
        runtime.execute(context, environment)

    message = str(exc_info.value)
    assert canary_body.decode() not in message
    assert str(canary) not in message
    assert canary_body.decode() not in caplog.text
    assert str(canary) not in caplog.text
    assert canary.read_bytes() == canary_body
    for name in ("stdout.txt", "stderr.txt"):
        diagnostic_body = (context.work_dir / name).read_bytes()
        assert canary_body not in diagnostic_body
        assert len(diagnostic_body) <= m_node_runtime._MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES


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
    assert ".as_format(...)" in message
    assert "wait for artifact-file input transport" not in message

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

    monkeypatch.setattr(m_node_runtime, "run_process_tree", fake_run)

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
    assert "--cidfile" not in command
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


def test_daemon_worker_docker_node_name_and_labels_include_instance_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_DAEMON_INSTANCE_ID", "daemon-01")
    monkeypatch.setenv("SPL_DAEMON_HOME_HASH", "home-hash")
    monkeypatch.setenv("SPL_DAEMON_GENERATION", "7")
    monkeypatch.setenv("SPL_DAEMON_RUN_ID", "run-123")
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
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(m_node_runtime, "run_process_tree", fake_run)

    assert runtime.execute(context, environment) == {DEFAULT_PORT: 42}

    [command] = commands
    container_name = command[command.index("--name") + 1]
    assert container_name.startswith("spl-node-daemon-0-run-123-")
    labels = [command[index + 1] for index, item in enumerate(command) if item == "--label"]
    assert "com.splime.instance=daemon-01" in labels
    assert "com.splime.home=home-hash" in labels
    assert "com.splime.generation=7" in labels
    assert "com.splime.run=run-123" in labels
    assert "com.splime.kind=node" in labels


def test_docker_node_runtime_timeout_kills_container(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_daemon_ownership_environment(monkeypatch)
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

    def fake_worker_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        (context.work_dir / "container.cid").write_text("immutable-container-id\n", encoding="utf-8")
        raise subprocess.TimeoutExpired(command, 0.25, output="started\n", stderr="")

    def fake_docker_cleanup(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command[:2] == ["docker", "inspect"]:
            container_name = commands[0][commands[0].index("--name") + 1]
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=_docker_inspect_payload(container_name),
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(m_node_runtime, "run_process_tree", fake_worker_run)
    monkeypatch.setattr(m_node_runtime.subprocess, "run", fake_docker_cleanup)

    with pytest.raises(RuntimeError, match=r"node runtime `docker` timed out after 0.25s for node `double`"):
        runtime.execute(context, environment)

    run_command, inspect_command, kill_command, remove_command = commands
    container_name = run_command[run_command.index("--name") + 1]
    assert container_name.startswith("spl-node-run-123-")
    assert inspect_command == ["docker", "inspect", container_name]
    assert kill_command == ["docker", "kill", container_name]
    assert remove_command == ["docker", "rm", "-f", container_name]
    assert all("immutable-container-id" not in command for command in commands[1:])
    assert (context.work_dir / "stdout.txt").read_text(encoding="utf-8") == "started\n"
    assert (context.work_dir / "stderr.txt").read_text(encoding="utf-8") == ""


def test_docker_node_runtime_cancellation_removes_owned_container(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_daemon_ownership_environment(monkeypatch)
    runtime = m_node_runtime.DockerNodeRuntime()
    context = _docker_runtime_context(
        tmp_path,
        runtime_config={"docker": {"image": "python:3.13-slim", "network": "none"}},
    )
    environment = runtime.prepare(context)
    commands: list[list[str]] = []

    def cancelled_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        (context.work_dir / "container.cid").write_text("owned-container-id\n", encoding="utf-8")
        raise KeyboardInterrupt

    def fake_docker_cleanup(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command[:2] == ["docker", "inspect"]:
            container_name = commands[0][commands[0].index("--name") + 1]
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=_docker_inspect_payload(container_name),
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(m_node_runtime, "run_process_tree", cancelled_run)
    monkeypatch.setattr(m_node_runtime.subprocess, "run", fake_docker_cleanup)

    with pytest.raises(KeyboardInterrupt):
        runtime.execute(context, environment)

    run_command = commands[0]
    container_name = run_command[run_command.index("--name") + 1]
    assert commands[1:] == [
        ["docker", "inspect", container_name],
        ["docker", "kill", container_name],
        ["docker", "rm", "-f", container_name],
    ]
    assert all("owned-container-id" not in command for command in commands[1:])


@pytest.mark.parametrize(
    "cidfile_kind",
    ["different", "symlink", "hardlink", "malformed", "empty", "oversized", "missing"],
)
def test_docker_cleanup_never_derives_authority_from_node_writable_cidfile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cidfile_kind: str,
) -> None:
    _clear_daemon_ownership_environment(monkeypatch)
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
    canary = tmp_path / "external-cidfile-canary"
    canary_body = b"external-cidfile-canary-body"
    canary.write_bytes(canary_body)

    def timed_out_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        cidfile = context.work_dir / "container.cid"
        if cidfile_kind == "different":
            cidfile.write_text("unrelated-container\n", encoding="utf-8")
        elif cidfile_kind == "symlink":
            cidfile.symlink_to(canary)
        elif cidfile_kind == "hardlink":
            os.link(canary, cidfile)
        elif cidfile_kind == "malformed":
            cidfile.write_text("not a valid Docker identifier !\n", encoding="utf-8")
        elif cidfile_kind == "empty":
            cidfile.write_bytes(b"")
        elif cidfile_kind == "oversized":
            cidfile.write_bytes(b"x" * (1024 * 1024))
        elif cidfile_kind != "missing":
            raise AssertionError(cidfile_kind)
        raise subprocess.TimeoutExpired(command, 0.25)

    def exact_cleanup(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command[:2] == ["docker", "inspect"]:
            container_name = commands[0][commands[0].index("--name") + 1]
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=_docker_inspect_payload(container_name),
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(m_node_runtime, "run_process_tree", timed_out_run)
    monkeypatch.setattr(m_node_runtime.subprocess, "run", exact_cleanup)

    with pytest.raises(RuntimeError, match=r"node runtime `docker` timed out"):
        runtime.execute(context, environment)

    run_command = commands[0]
    container_name = run_command[run_command.index("--name") + 1]
    assert commands[1:] == [
        ["docker", "inspect", container_name],
        ["docker", "kill", container_name],
        ["docker", "rm", "-f", container_name],
    ]
    assert all("unrelated-container" not in command for command in commands[1:])
    assert canary.read_bytes() == canary_body


def test_docker_cleanup_requires_exact_name_even_when_all_ownership_labels_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_DAEMON_INSTANCE_ID", "daemon-01")
    monkeypatch.setenv("SPL_DAEMON_HOME_HASH", "home-hash")
    monkeypatch.setenv("SPL_DAEMON_GENERATION", "7")
    monkeypatch.setenv("SPL_DAEMON_RUN_ID", "run-123")
    from spl.daemon.docker_pool import worker_container_labels_from_env

    expected_name = "spl-node-daemon-0-run-123-12345678-abcdef"
    commands: list[list[str]] = []

    def mismatched_inspect(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=_docker_inspect_payload(
                "unrelated-container",
                worker_container_labels_from_env(kind="node"),
            ),
            stderr="",
        )

    monkeypatch.setattr(m_node_runtime.subprocess, "run", mismatched_inspect)

    m_node_runtime._kill_docker_container(expected_name)

    assert commands == [["docker", "inspect", expected_name]]


def test_docker_cleanup_ignores_a_different_matching_label_cidfile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_DAEMON_INSTANCE_ID", "daemon-01")
    monkeypatch.setenv("SPL_DAEMON_HOME_HASH", "home-hash")
    monkeypatch.setenv("SPL_DAEMON_GENERATION", "7")
    monkeypatch.setenv("SPL_DAEMON_RUN_ID", "run-123")
    from spl.daemon.docker_pool import worker_container_labels_from_env

    labels = worker_container_labels_from_env(kind="node")
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

    def timed_out_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        (context.work_dir / "container.cid").write_text("other-node-from-the-same-run\n", encoding="utf-8")
        raise subprocess.TimeoutExpired(command, 0.25)

    def exact_cleanup(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        if command[:2] == ["docker", "inspect"]:
            container_name = commands[0][commands[0].index("--name") + 1]
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=_docker_inspect_payload(container_name, labels),
                stderr="",
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(m_node_runtime, "run_process_tree", timed_out_run)
    monkeypatch.setattr(m_node_runtime.subprocess, "run", exact_cleanup)

    with pytest.raises(RuntimeError, match=r"node runtime `docker` timed out"):
        runtime.execute(context, environment)

    run_command = commands[0]
    container_name = run_command[run_command.index("--name") + 1]
    assert commands[1:] == [
        ["docker", "inspect", container_name],
        ["docker", "kill", container_name],
        ["docker", "rm", "-f", container_name],
    ]
    assert all("other-node-from-the-same-run" not in command for command in commands[1:])


def test_docker_cleanup_refuses_an_unrelated_target_without_inspecting_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_inspect(*_: Any, **__: Any) -> Any:
        pytest.fail("an unrelated cleanup target must not be inspected")

    monkeypatch.setattr(m_node_runtime.subprocess, "run", forbidden_inspect)

    assert (
        m_node_runtime._docker_cleanup_target_is_owned(
            "unrelated-container",
            expected_name="spl-node-owned-container",
        )
        is False
    )


def test_docker_cleanup_treats_an_already_removed_exact_container_as_harmless(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []
    expected_name = "spl-node-owned-container"

    def missing_inspect(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="Error: No such container")

    monkeypatch.setattr(m_node_runtime.subprocess, "run", missing_inspect)

    m_node_runtime._kill_docker_container(expected_name)

    assert commands == [["docker", "inspect", expected_name]]


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


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_docker_node_runtime_cancellation_removes_exact_container_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = "python:3.13-slim"
    _ensure_docker_image(image)
    _clear_daemon_ownership_environment(monkeypatch)
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    marker_path = tmp_path / "cancelled-marker.txt"
    lift_any = cast(Any, lift)
    pipeline = lift_any(_very_slow_marker).alias("slow").render("slow_cancellation").with_node_runtime("slow", "docker")
    run = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}},
    ).run(keep=True, marker_path=str(marker_path))
    node = pipeline.aliases["slow"]
    container_name_prefix = "spl-node-{}-{}-".format(
        m_node_runtime._docker_name_token(run.run_id)[:32],
        str(node.uuid).replace("-", "")[:8],
    )
    stop = threading.Event()
    container_seen = threading.Event()

    def interrupt_when_container_starts() -> None:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and not stop.wait(0.05):
            completed = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=15,
                check=False,
            )
            if any(name.startswith(container_name_prefix) for name in completed.stdout.splitlines()):
                container_seen.set()
                _thread.interrupt_main()
                return

    interrupter = threading.Thread(target=interrupt_when_container_starts, daemon=True)
    interrupter.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            run.value("slow")
    finally:
        stop.set()
        interrupter.join(timeout=5)

    assert container_seen.is_set()
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
        stop.wait(0.2)
    assert matching_names == []
    stop.wait(0.4)
    assert not marker_path.exists()


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
@pytest.mark.parametrize("stream", ["stdout", "stderr"])
@pytest.mark.parametrize("replacement_kind", ["symlink", "hardlink", "fifo", "regular"])
def test_docker_node_runtime_rejects_live_diagnostic_path_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    stream: str,
    replacement_kind: str,
) -> None:
    image = "python:3.13-slim"
    _ensure_docker_image(image)
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    canary_body = b"live-external-diagnostic-secret"
    invocations: list[m_node_runtime._SplFreeInvocation] = []
    original_prepare = m_node_runtime._prepare_spl_free_invocation

    def prepare_with_canary(
        context: m_node_runtime.NodeRuntimeContext,
        *,
        runtime_name: str,
    ) -> m_node_runtime._SplFreeInvocation:
        invocation = original_prepare(context, runtime_name=runtime_name)
        (invocation.work_dir / "diagnostic-canary.bin").write_bytes(canary_body)
        invocations.append(invocation)
        return invocation

    monkeypatch.setattr(m_node_runtime, "_prepare_spl_free_invocation", prepare_with_canary)
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(_replace_runtime_diagnostic)
        .alias("tamper")
        .render("diagnostic_replacement")
        .with_node_runtime("tamper", "docker")
    )
    run = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}},
    ).run(keep=True, stream=stream, kind=replacement_kind)

    with run:
        assert run.value("tamper") == 7

    [invocation] = invocations
    assert (invocation.work_dir / "diagnostic-canary.bin").read_bytes() == canary_body
    retained_stdout = invocation.stdout_path.read_bytes()
    retained_stderr = invocation.stderr_path.read_bytes()
    assert canary_body not in retained_stdout
    assert canary_body not in retained_stderr
    assert len(retained_stdout) <= m_node_runtime._MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES
    assert len(retained_stderr) <= m_node_runtime._MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES
    manifest_text = json.dumps(_read_manifest(run), sort_keys=True)
    assert canary_body.decode() not in manifest_text
    assert canary_body.decode() not in caplog.text


@pytest.mark.parametrize(
    "timeout_value",
    ["1", 0, -1, True, float("nan"), float("inf"), float("-inf")],
    ids=["string", "zero", "negative", "boolean", "nan", "positive-infinity", "negative-infinity"],
)
def test_node_timeout_runtime_config_is_validated_early(timeout_value: Any) -> None:
    deployment = Deployment(_runtime_pipeline(), runtime_config={"node_timeout_seconds": timeout_value})

    with pytest.raises(ValueError, match=r"node_timeout_seconds"):
        deployment.run()


@pytest.mark.parametrize(
    ("timeout_value", "expected"),
    [(None, None), (1, 1.0), (1.25, 1.25)],
)
def test_node_timeout_runtime_config_preserves_valid_values(
    timeout_value: float | None,
    expected: float | None,
) -> None:
    assert m_node_runtime.node_timeout_seconds({"node_timeout_seconds": timeout_value}) == expected


@pytest.mark.parametrize(
    "timeout_value",
    ["1", 0, -1, True, float("nan"), float("inf"), float("-inf")],
    ids=["string", "zero", "negative", "boolean", "nan", "positive-infinity", "negative-infinity"],
)
def test_daemon_runtime_config_rejects_invalid_node_timeout(timeout_value: Any) -> None:
    with pytest.raises(ValueError, match=r'runtime_config\["node_timeout_seconds"\]'):
        normalize_runtime_config({"node_timeout_seconds": timeout_value})


@pytest.mark.parametrize(
    "timeout_value",
    [float("nan"), float("inf"), float("-inf")],
    ids=["nan", "positive-infinity", "negative-infinity"],
)
def test_docker_node_timeout_is_rejected_before_container_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    timeout_value: float,
) -> None:
    runtime = m_node_runtime.DockerNodeRuntime()
    context = _docker_runtime_context(
        tmp_path,
        runtime_config={
            "docker": {"image": "python:3.13-slim", "network": "none"},
            "node_timeout_seconds": timeout_value,
        },
    )
    environment = runtime.prepare(context)
    process_called = False

    def forbidden_run_process_tree(*args: Any, **kwargs: Any) -> Any:
        nonlocal process_called
        del args, kwargs
        process_called = True
        pytest.fail("docker run must not be called for a non-finite timeout")

    monkeypatch.setattr(m_node_runtime, "run_process_tree", forbidden_run_process_tree)

    with pytest.raises(ValueError, match=r"node_timeout_seconds"):
        runtime.execute(context, environment)

    assert process_called is False
