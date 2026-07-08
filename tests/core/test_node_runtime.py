from __future__ import annotations

import ast
import inspect
import json
import sys
import textwrap
import time
from pathlib import Path
from typing import Any, cast

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
        },
        {
            "node_id": producer["id"],
            "alias": "producer",
            "name": "native",
            "source": "default",
            "config_hash": None,
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


@pytest.mark.parametrize("timeout_value", ["1", 0, -1, True])
def test_node_timeout_runtime_config_is_validated_early(timeout_value: Any) -> None:
    deployment = Deployment(_runtime_pipeline(), runtime_config={"node_timeout_seconds": timeout_value})

    with pytest.raises(ValueError, match=r"node_timeout_seconds"):
        deployment.run()
