from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Annotated, Any, Mapping, cast

import numpy as np  # type: ignore[import-not-found, unused-ignore]  # Optional acceptance dependency.
import pytest

from spl import Deployment, lift
from spl.core import node_runtime as m_node_runtime
from spl.core import runtime_port_adapters as m_runtime_port_adapters
from spl.core.entities.adapter import Adapter, make_key
from spl.core.entities.artifact import ArtifactRef
from spl.core.entities.distribution import DDistribution
from spl.core.entities.node import DEFAULT_PORT
from spl.core.entities.node_function import NodeFunction
from spl.daemon import spl_free_runner


_producer_calls = 0


def produce_integer_set() -> set[int]:
    global _producer_calls
    _producer_calls += 1
    return {3, 1, 2}


def consume_integer_set(value):  # type: ignore[no-untyped-def]  # Deliberately untyped transport case.
    import os

    return {
        "type": type(value).__name__,
        "values": sorted(value),
        "loaded_before_call": os.environ.get("SPL_PHASE1_LOAD_OCCURRED"),
    }


def consume_annotated_integer_set(value: Annotated[set[int], "meta"]) -> list[int]:
    return sorted(value)


def consume_typed_integer_set(value: set[int]) -> list[int]:
    return sorted(value)


def consume_two_long_ports(  # type: ignore[no-untyped-def]  # Deliberately untyped transport case.
    shared_prefix_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnop_alpha,
    shared_prefix_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnop_beta,
):
    return [
        sorted(
            shared_prefix_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnop_alpha
        ),
        sorted(
            shared_prefix_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnop_beta
        ),
    ]


def save_integer_set(path: str, value: set[int]) -> None:
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE1_SAVE_MARKER")
    if marker:
        Path(marker).write_text("save\n", encoding="utf-8")
    Path(path).write_text(",".join(str(item) for item in sorted(value)), encoding="utf-8")


def load_integer_set(path: str) -> set[int]:
    import os
    from pathlib import Path

    conductor_pid = os.environ.get("SPL_PHASE1_CONDUCTOR_PID")
    if conductor_pid == str(os.getpid()):
        raise RuntimeError("load ran in conductor")
    os.environ["SPL_PHASE1_LOAD_OCCURRED"] = "yes"
    return {int(item) for item in Path(path).read_text(encoding="utf-8").split(",")}


def load_integer_set_failure(path: str) -> set[int]:
    del path
    raise RuntimeError("secret adapter detail")


def load_integer_set_slow(path: str) -> set[int]:
    import threading
    from pathlib import Path

    threading.Event().wait()
    return {int(item) for item in Path(path).read_text(encoding="utf-8").split(",")}


def load_integer_set_system_exit(path: str) -> set[int]:
    del path
    raise SystemExit("secret adapter SystemExit detail")


def load_integer_set_with_certifi(path: str) -> set[int]:
    import certifi
    from pathlib import Path

    if not Path(certifi.where()).is_file():
        raise RuntimeError("certifi CA bundle is unavailable")
    return {int(item) for item in Path(path).read_text(encoding="utf-8").split(",")}


def produce_ndarray() -> Any:
    import numpy as np

    return np.asarray([[1, 2], [3, 4]], dtype=np.int64)


def consume_ndarray(value):  # type: ignore[no-untyped-def]  # Deliberately untyped transport case.
    return {"type": type(value).__name__, "shape": list(value.shape), "values": value.tolist()}


def save_ndarray(path: str, value: Any) -> None:
    import numpy as np

    with open(path, "wb") as handle:
        np.save(handle, value, allow_pickle=False)


def load_ndarray(path: str):  # type: ignore[no-untyped-def]  # Source bundle must not depend on typing.Any.
    import numpy as np

    with open(path, "rb") as handle:
        return np.load(handle, allow_pickle=False)


def inline_identity(value):  # type: ignore[no-untyped-def]  # Exact legacy untyped JSON case.
    return value


def _set_pipeline(*, load: Any = load_integer_set, runtime: str = "venv-subprocess") -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_integer_set).alias("producer")
    pipeline = (
        lift_any(consume_integer_set)
        .bind(value=producer.as_format("integer-set"))
        .alias("consumer")
        .render("isolated_integer_set")
        .add_adapter(set, "integer-set", save=save_integer_set, load=load)
    )
    return pipeline.with_node_runtime("consumer", runtime)


def _numpy_pipeline(*, runtime: str = "venv-subprocess") -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_ndarray).alias("producer")
    numpy_distribution = DDistribution(package="numpy", version=np.__version__)
    pipeline = (
        lift_any(consume_ndarray)
        .bind(value=producer.as_format("npy"))
        .alias("consumer")
        .render("isolated_numpy")
        .add_adapter(
            np.ndarray,
            "npy",
            save=save_ndarray,
            load=load_ndarray,
            distributions=(numpy_distribution,),
        )
    )
    return pipeline.with_node_runtime("consumer", runtime)


def _annotated_pipeline(*, runtime: str | None) -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_integer_set).alias("producer")
    pipeline = (
        lift_any(consume_annotated_integer_set)
        .bind(value=producer.as_format("integer-set"))
        .alias("consumer")
        .render("annotated_integer_set")
        .add_adapter(set, "integer-set", save=save_integer_set, load=load_integer_set)
    )
    return pipeline if runtime is None else pipeline.with_node_runtime("consumer", runtime)


def _node_by_alias(manifest: Mapping[str, Any], alias: str) -> Mapping[str, Any]:
    return next(node for node in manifest["nodes"].values() if node["alias"] == alias)


def test_custom_non_json_artifact_loads_only_inside_venv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    marker = tmp_path / "save-marker.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE1_SAVE_MARKER", str(marker))
    monkeypatch.setenv("SPL_PHASE1_CONDUCTOR_PID", str(os.getpid()))
    run = Deployment(_set_pipeline()).run(keep=True)

    with run:
        assert run.value("consumer") == {
            "type": "set",
            "values": [1, 2, 3],
            "loaded_before_call": "yes",
        }

    assert marker.read_text(encoding="utf-8") == "save\n"
    assert run.run_dir is not None
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    consumer = _node_by_alias(manifest, "consumer")
    runtime_dir = run.run_dir / "node-runtimes" / consumer["id"]
    payload = json.loads((runtime_dir / "input.json").read_text(encoding="utf-8"))
    assert set(payload) == {"schema_version", "args", "kwargs", "runtime_port_adapters"}
    assert payload["schema_version"] == 2
    assert payload["args"] == []
    assert payload["kwargs"] == {}
    assert str(run.run_dir) not in json.dumps(payload)
    assert (runtime_dir / "runtime-inputs" / "custom-adapters.py").exists()
    bundle_text = (runtime_dir / "runtime-inputs" / "custom-adapters.py").read_text(encoding="utf-8")
    assert "def load_integer_set" in bundle_text
    assert "def save_integer_set" not in bundle_text
    [edge] = manifest["edges"]
    assert edge["artifact"]["kind"] == "artifact"
    assert edge["adapter"]["save"]["identity"]["save"].endswith("save_integer_set")
    assert edge["adapter"]["load"]["identity"]["load"].endswith("load_integer_set")


def test_numpy_artifact_dependency_is_merged_and_loaded_inside_venv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_numpy_pipeline()).run(keep=True)

    with run:
        assert run.value("consumer") == {
            "type": "ndarray",
            "shape": [2, 2],
            "values": [[1, 2], [3, 4]],
        }

    assert run.run_dir is not None
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    consumer = _node_by_alias(manifest, "consumer")
    runtime_dir = run.run_dir / "node-runtimes" / consumer["id"]
    env_spec = json.loads((runtime_dir / "env-spec.json").read_text(encoding="utf-8"))
    assert env_spec == [{"package": "numpy", "version": np.__version__}]


def test_retained_artifact_feeds_isolated_consumer_without_rerunning_producer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    global _producer_calls
    _producer_calls = 0
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE1_CONDUCTOR_PID", str(os.getpid()))
    parent = Deployment(_set_pipeline()).run(keep=True)
    with parent:
        assert parent.value("consumer")["values"] == [1, 2, 3]
    assert _producer_calls == 1

    child = parent.resume(from_="consumer", keep=True)
    with child:
        assert child.value("consumer")["values"] == [1, 2, 3]

    assert _producer_calls == 1
    manifest = cast(dict[str, Any], child.manifest_snapshot)
    assert _node_by_alias(manifest, "producer")["status"] == "frozen"
    assert _node_by_alias(manifest, "consumer")["status"] == "succeeded"


def test_adapter_load_failure_is_closed_and_does_not_leak_exception_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_set_pipeline(load=load_integer_set_failure)).run(keep=True)

    with pytest.raises(RuntimeError) as exc_info:
        run.value("consumer")

    message = str(exc_info.value)
    assert "node_adapter_load" in message
    assert "secret adapter detail" not in message


def test_adapter_load_system_exit_is_closed_and_does_not_leak_exception_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_set_pipeline(load=load_integer_set_system_exit)).run(keep=True)

    with pytest.raises(RuntimeError) as exc_info:
        run.value("consumer")

    message = str(exc_info.value)
    assert "node_adapter_load" in message
    assert "adapter_load_failed" in message
    assert "SystemExit" not in message
    assert "secret adapter" not in message
    assert "Traceback" not in message


def test_custom_adapter_separate_exact_dependency_executes_in_target_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_integer_set).alias("producer")
    version = importlib_metadata.version("certifi")
    pipeline = (
        lift_any(consume_integer_set)
        .bind(value=producer.as_format("certifi-set"))
        .alias("consumer")
        .render("certifi_adapter_dependency")
        .add_adapter(
            set,
            "certifi-set",
            save=save_integer_set,
            load=load_integer_set_with_certifi,
            distributions=(DDistribution(package="certifi", version=version),),
        )
        .with_node_runtime("consumer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("consumer")["values"] == [1, 2, 3]

    assert run.run_dir is not None
    consumer = _node_by_alias(cast(dict[str, Any], run.manifest_snapshot), "consumer")
    env_spec = json.loads(
        (run.run_dir / "node-runtimes" / consumer["id"] / "env-spec.json").read_text(encoding="utf-8")
    )
    assert env_spec == [{"package": "certifi", "version": version}]
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    assert "secret adapter detail" not in str(_node_by_alias(manifest, "consumer")["error"])


def test_timeout_during_adapter_load_terminates_subprocess(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(
        _set_pipeline(load=load_integer_set_slow),
        runtime_config={"node_timeout_seconds": 0.4},
    ).run(keep=True)

    with pytest.raises(RuntimeError, match=r"timed out after 0.4s"):
        run.value("consumer")


def test_custom_load_missing_declared_dependency_fails_before_node_workdir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_ndarray).alias("producer")
    pipeline = (
        lift_any(consume_ndarray)
        .bind(value=producer.as_format("npy"))
        .alias("consumer")
        .render("missing_numpy_dependency")
        .add_adapter(np.ndarray, "npy", save=save_ndarray, load=load_ndarray)
        .with_node_runtime("consumer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(RuntimeError, match=r"environment_preflight.*numpy.*supported adapter"):
        run.value("consumer")

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    consumer = _node_by_alias(manifest, "consumer")
    assert run.run_dir is not None
    assert not (run.run_dir / "node-runtimes" / consumer["id"]).exists()


def test_keep_false_removes_isolated_input_staging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_set_pipeline()).run(keep=False)
    with run:
        assert run.value("consumer")["values"] == [1, 2, 3]
        artifacts_dir = run._artifacts_dir
        assert artifacts_dir is not None
        assert (artifacts_dir / "node-runtimes").exists()

    assert not artifacts_dir.exists()


def test_annotated_custom_artifact_input_uses_advisory_wire_type_in_venv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_annotated_pipeline(runtime="venv-subprocess")).run(keep=True)
    with run:
        assert run.value("consumer") == [1, 2, 3]

    assert run.run_dir is not None
    consumer = _node_by_alias(cast(dict[str, Any], run.manifest_snapshot), "consumer")
    payload = json.loads((run.run_dir / "node-runtimes" / consumer["id"] / "input.json").read_text(encoding="utf-8"))
    [binding] = payload["runtime_port_adapters"]["bindings"]
    assert binding["port"] == "value"
    assert binding["semantic_type"] is None


def test_annotated_custom_artifact_input_docker_protocol_planning(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_text("1,2,3", encoding="utf-8")
    adapter = Adapter(
        key=make_key(set, "integer-set"),
        save=save_integer_set,
        load=load_integer_set,
        py_type=set,
        format="integer-set",
    )
    node = NodeFunction(cast(Any, consume_annotated_integer_set))
    [port] = node.inputs
    context = m_node_runtime.NodeRuntimeContext(
        node=node,
        node_label="consumer",
        inputs={
            port: m_node_runtime.ArtifactInput(
                ref=ArtifactRef(
                    key=adapter.key,
                    uri=str(source),
                    sha256=m_node_runtime.hashlib.sha256(source.read_bytes()).hexdigest(),
                    size=source.stat().st_size,
                    tag=adapter.tag,
                ),
                load_adapter=adapter,
                resolution_source="edge",
            )
        },
        output_port=node.get_output_port(DEFAULT_PORT),
        callback=lambda _node, _inputs: {DEFAULT_PORT: None},
        work_dir=tmp_path / "docker-annotated",
        environment_provider=m_node_runtime.CurrentPythonEnvironmentProvider(),
        runtime_config={},
        environment_spec=[],
    )
    prepared = m_node_runtime.prepare_isolated_node_inputs(context, runtime_name="docker")
    assert prepared.isolated_inputs is not None
    [binding] = prepared.isolated_inputs.document["bindings"]
    assert binding["semantic_type"] is None


def test_annotated_custom_artifact_input_native_behavior_is_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_annotated_pipeline(runtime=None)).run(keep=True)
    with run:
        assert run.value("consumer") == [1, 2, 3]


def test_ordinary_typed_custom_artifact_input_is_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_integer_set).alias("producer")
    pipeline = (
        lift_any(consume_typed_integer_set)
        .bind(value=producer.as_format("integer-set"))
        .alias("consumer")
        .render("typed_integer_set")
        .add_adapter(set, "integer-set", save=save_integer_set, load=load_integer_set)
        .with_node_runtime("consumer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)
    with run:
        assert run.value("consumer") == [1, 2, 3]


def test_two_long_ports_stage_same_artifact_under_unique_names(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_integer_set).alias("producer")
    names = [port.name for port in NodeFunction(cast(Any, consume_two_long_ports)).inputs]
    pipeline = (
        lift_any(consume_two_long_ports)
        .bind(**{name: producer.as_format("integer-set") for name in names})
        .alias("consumer")
        .render("long_port_identity")
        .add_adapter(set, "integer-set", save=save_integer_set, load=load_integer_set)
        .with_node_runtime("consumer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)
    with run:
        assert run.value("consumer") == [[1, 2, 3], [1, 2, 3]]
    assert run.run_dir is not None
    consumer = _node_by_alias(cast(dict[str, Any], run.manifest_snapshot), "consumer")
    payload = json.loads((run.run_dir / "node-runtimes" / consumer["id"] / "input.json").read_text(encoding="utf-8"))
    staged_names = [item["staged_name"] for item in payload["runtime_port_adapters"]["inputs"]]
    assert len(staged_names) == len(set(staged_names)) == 2


def _prepared_runner_case(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    source = tmp_path / "source.bin"
    source.write_text("1,2,3", encoding="utf-8")
    adapter = Adapter(
        key=make_key(set, "integer-set"),
        save=save_integer_set,
        load=load_integer_set,
        py_type=set,
        format="integer-set",
    )
    node = NodeFunction(cast(Any, consume_integer_set))
    [port] = node.inputs
    ref = ArtifactRef(
        key=adapter.key,
        uri=str(source),
        sha256=m_node_runtime.hashlib.sha256(source.read_bytes()).hexdigest(),
        size=source.stat().st_size,
        tag=adapter.tag,
    )
    context = m_node_runtime.NodeRuntimeContext(
        node=node,
        node_label="consumer",
        inputs={
            port: m_node_runtime.ArtifactInput(
                ref=ref,
                load_adapter=adapter,
                resolution_source="edge",
            )
        },
        output_port=node.get_output_port(DEFAULT_PORT),
        callback=lambda _node, _inputs: {DEFAULT_PORT: None},
        work_dir=tmp_path / "node",
        environment_provider=m_node_runtime.CurrentPythonEnvironmentProvider(),
        runtime_config={},
        environment_spec=[],
    )
    prepared = m_node_runtime.prepare_isolated_node_inputs(context, runtime_name="venv-subprocess")
    invocation = m_node_runtime._prepare_spl_free_invocation(prepared, runtime_name="venv-subprocess")
    payload = json.loads(invocation.input_path.read_text(encoding="utf-8"))
    return invocation.input_path, payload


@pytest.mark.parametrize(
    "mutation",
    ["wrong-keyword", "wrong-external", "positional", "default", "inline-collision", "duplicate-binding"],
)
def test_isolated_binding_locations_are_closed_before_adapter_loading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    input_path, payload = _prepared_runner_case(tmp_path)
    binding = payload["runtime_port_adapters"]["bindings"][0]
    if mutation == "wrong-keyword":
        binding["argument"] = {"kind": "keyword", "name": "other", "index": None}
    elif mutation == "wrong-external":
        binding["external_name"] = "other"
    elif mutation == "positional":
        binding["argument"] = {"kind": "positional", "name": None, "index": 0}
    elif mutation == "default":
        binding["argument"] = {"kind": "default", "name": "value", "index": None}
    elif mutation == "inline-collision":
        payload["kwargs"]["value"] = "inline-secret"
    else:
        payload["runtime_port_adapters"]["bindings"].append(json.loads(json.dumps(binding)))

    inline_names = payload["kwargs"].keys()
    with pytest.raises(m_runtime_port_adapters.RuntimePortAdapterContractError) as control_error:
        m_runtime_port_adapters.normalize_isolated_node_inputs(
            payload["runtime_port_adapters"], inline_keyword_names=inline_names
        )
    assert control_error.value.stage == "node_adapter_input_validation"

    def forbidden_bundle_load(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("custom adapter bundle was loaded")

    monkeypatch.setattr(spl_free_runner, "_load_verified_custom_module", forbidden_bundle_load)
    with pytest.raises(spl_free_runner.NodeStageError) as runner_error:
        spl_free_runner.prepare_invocation_inputs(payload, input_path=input_path)
    assert runner_error.value.stage == "node_adapter_input_validation"


@pytest.mark.parametrize(
    "body",
    [b'{"args":', b'{"args":[],"kwargs":{"secret-remediation":}}', b"\xff\xfe\xfd"],
)
def test_runner_execute_closes_malformed_input_documents(tmp_path: Path, body: bytes) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_bytes(body)
    with pytest.raises(spl_free_runner.NodeStageError) as exc_info:
        spl_free_runner.execute(
            module_path=tmp_path / "must-not-load.py",
            module_name="must_not_load",
            entrypoint="run",
            input_path=input_path,
            result_path=tmp_path / "result.json",
            artifacts_dir=tmp_path / "artifacts",
        )
    assert exc_info.value.stage == "node_adapter_input_validation"
    assert "secret-remediation" not in str(exc_info.value)


@pytest.mark.parametrize("body", [b'{"args":', b'{"secret-remediation":', b"\xff\xfe\xfd"])
def test_runner_subprocess_closes_malformed_input_without_traceback(tmp_path: Path, body: bytes) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_bytes(body)
    marker = tmp_path / "executed"
    module_path = tmp_path / "node.py"
    module_path.write_text(
        "from pathlib import Path\nPath({!r}).write_text('loaded')\ndef run(): return 1\n".format(str(marker)),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(spl_free_runner.__file__).resolve()),
            "--module",
            str(module_path),
            "--module-name",
            "malformed_input_node",
            "--entrypoint",
            "run",
            "--input",
            str(input_path),
            "--result",
            str(tmp_path / "result.json"),
            "--artifacts-dir",
            str(tmp_path / "artifacts"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode != 0
    assert "node_adapter_input_validation" in completed.stderr
    assert "Traceback" not in completed.stderr
    assert "secret-remediation" not in completed.stderr
    assert str(input_path) not in completed.stderr
    assert not marker.exists()


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown-field",
        "unknown-record-field",
        "unknown-version",
        "unknown-transport",
        "unknown-adapter-kind",
        "unknown-adapter-id",
        "unsafe-port",
        "empty-path",
        "traversal",
        "absolute-path",
        "forward-separator",
        "backslash-separator",
        "unsafe-unicode",
        "normalization-trick",
        "duplicate-input",
        "malformed-checksum",
        "bad-checksum",
        "bad-size",
        "bad-key",
        "bad-tag",
        "bad-accepted-tags",
        "bad-legacy-guard",
    ],
)
def test_runner_rejects_closed_protocol_and_identity_tampering(tmp_path: Path, mutation: str) -> None:
    input_path, payload = _prepared_runner_case(tmp_path)
    document = payload["runtime_port_adapters"]
    binding = document["bindings"][0]
    item = document["inputs"][0]
    if mutation == "unknown-field":
        payload["extra"] = True
    elif mutation == "unknown-record-field":
        item["extra"] = True
    elif mutation == "unknown-version":
        payload["schema_version"] = 99
    elif mutation == "unknown-transport":
        binding["transport"] = "host-path"
    elif mutation == "unknown-adapter-kind":
        binding["adapter"]["kind"] = "plugin"
    elif mutation == "unknown-adapter-id":
        binding["adapter"]["kind"] = "builtin"
        binding["adapter"]["id"] = "unknown"
    elif mutation == "unsafe-port":
        binding["port"] = "../value"
    elif mutation == "empty-path":
        item["staged_name"] = ""
    elif mutation == "traversal":
        item["staged_name"] = "../secret"
    elif mutation == "absolute-path":
        item["staged_name"] = "/tmp/secret"
    elif mutation == "forward-separator":
        item["staged_name"] = "nested/secret"
    elif mutation == "backslash-separator":
        item["staged_name"] = r"nested\secret"
    elif mutation == "unsafe-unicode":
        item["staged_name"] = "nested∕secret"
    elif mutation == "normalization-trick":
        item["staged_name"] = "e\u0301.bin"
    elif mutation == "duplicate-input":
        document["inputs"].append(json.loads(json.dumps(item)))
    elif mutation == "malformed-checksum":
        item["sha256"] = "not-a-sha256"
    elif mutation == "bad-checksum":
        item["sha256"] = "0" * 64
    elif mutation == "bad-size":
        item["size"] += 1
    elif mutation == "bad-key":
        binding["artifact_key"] = "other.Type@integer-set"
    elif mutation == "bad-tag":
        binding["adapter"]["format_tag"] = "other"
    elif mutation == "bad-accepted-tags":
        binding["adapter"]["accepted_tags"] = ["other"]
    elif mutation == "bad-legacy-guard":
        binding["legacy_key_guard"] = True
        binding["artifact_key"] = "other.Type@integer-set"
    input_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(spl_free_runner.NodeStageError) as exc_info:
        spl_free_runner.prepare_invocation_inputs(payload, input_path=input_path)

    assert exc_info.value.stage == "node_adapter_input_validation"


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "directory", "fifo", "socket"])
def test_runner_rejects_non_regular_staged_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    input_path, payload = _prepared_runner_case(tmp_path)
    item = payload["runtime_port_adapters"]["inputs"][0]
    staged = input_path.parent / "runtime-inputs" / item["staged_name"]
    staged.unlink()
    if kind == "symlink":
        staged.symlink_to(tmp_path / "source.bin")
    elif kind == "hardlink":
        outside = tmp_path / "hardlink-source.bin"
        outside.write_bytes(b"1,2,3")
        os.link(outside, staged)
    elif kind == "directory":
        staged.mkdir()
    elif kind == "fifo":
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO is unavailable")
        os.mkfifo(staged)
    else:
        socket_stat = os.stat_result((stat.S_IFSOCK | 0o600, 0, 0, 1, 0, 0, 0, 0, 0, 0))
        real_lstat = Path.lstat
        monkeypatch.setattr(Path, "lstat", lambda path: socket_stat if path == staged else real_lstat(path))

    with pytest.raises(spl_free_runner.NodeStageError) as exc_info:
        spl_free_runner.prepare_invocation_inputs(payload, input_path=input_path)

    assert exc_info.value.stage == "node_adapter_input_validation"


def test_runner_rejects_device_runtime_artifact() -> None:
    device = Path("/dev/null")
    if not device.exists():
        pytest.skip("the platform has no regular device fixture")
    with pytest.raises(RuntimeError, match="not a regular file"):
        spl_free_runner._read_verified_runtime_file(
            device,
            expected_size=0,
            expected_sha256=m_node_runtime.hashlib.sha256(b"").hexdigest(),
        )


def test_runner_rejects_aggregate_input_size_before_file_or_adapter_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_path, payload = _prepared_runner_case(tmp_path)
    document = payload["runtime_port_adapters"]
    item = document["inputs"][0]
    binding = document["bindings"][0]
    second_item = json.loads(json.dumps(item))
    second_item.update(name="second.bin", staged_name="second.bin", port="second")
    second_binding = json.loads(json.dumps(binding))
    second_binding.update(port="second", external_name="second", input_name="second.bin")
    second_binding["argument"] = {"kind": "keyword", "name": "second", "index": None}
    document["inputs"].append(second_item)
    document["bindings"].append(second_binding)
    monkeypatch.setattr(spl_free_runner, "MAX_RUNTIME_INPUT_TOTAL_BYTES", item["size"])

    def forbidden_load(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("file or adapter access occurred")

    monkeypatch.setattr(spl_free_runner, "_load_verified_custom_module", forbidden_load)
    with pytest.raises(spl_free_runner.NodeStageError) as exc_info:
        spl_free_runner.prepare_invocation_inputs(payload, input_path=input_path)
    assert exc_info.value.stage == "node_adapter_input_validation"
    assert "aggregate" in str(exc_info.value)


def test_runner_rejects_raw_duplicate_input_json_before_module_load(tmp_path: Path) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text('{"args":[],"args":[],"kwargs":{}}', encoding="utf-8")
    with pytest.raises(spl_free_runner.NodeStageError) as exc_info:
        spl_free_runner.execute(
            module_path=tmp_path / "must-not-load.py",
            module_name="duplicate_input_node",
            entrypoint="run",
            input_path=input_path,
            result_path=tmp_path / "result.json",
            artifacts_dir=tmp_path / "artifacts",
        )
    assert exc_info.value.stage == "node_adapter_input_validation"
    assert "duplicate" not in str(exc_info.value).casefold()


@pytest.mark.parametrize("reader", ["conductor", "runner"])
@pytest.mark.parametrize("replacement", ["before-open", "during-read", "after-read"])
def test_artifact_identity_replacement_races_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
    replacement: str,
) -> None:
    path = tmp_path / "artifact.bin"
    path.write_bytes(b"original")
    real_open = os.open
    real_read = os.read
    real_fstat = os.fstat
    replaced = False
    fstat_calls = 0

    def replace_path() -> None:
        nonlocal replaced
        if replaced:
            return
        replaced = True
        path.rename(tmp_path / "artifact-original.bin")
        path.write_bytes(b"attacker")

    def raced_open(name: Any, flags: int, mode: int = 0o777) -> int:
        if replacement == "before-open" and Path(name) == path:
            replace_path()
        return real_open(name, flags, mode)

    def raced_read(descriptor: int, size: int) -> bytes:
        chunk = real_read(descriptor, size)
        if replacement == "during-read" and chunk:
            replace_path()
        return chunk

    def raced_fstat(descriptor: int) -> os.stat_result:
        nonlocal fstat_calls
        value = real_fstat(descriptor)
        fstat_calls += 1
        if replacement == "after-read" and fstat_calls == 2:
            replace_path()
        return value

    monkeypatch.setattr(os, "open", raced_open)
    monkeypatch.setattr(os, "read", raced_read)
    monkeypatch.setattr(os, "fstat", raced_fstat)
    if reader == "conductor":
        with pytest.raises(RuntimeError, match="node_adapter_input_validation"):
            m_node_runtime._read_bounded_regular_file(path)
    else:
        with pytest.raises(RuntimeError):
            spl_free_runner._read_verified_runtime_file(
                path,
                expected_size=len(b"original"),
                expected_sha256=m_node_runtime.hashlib.sha256(b"original").hexdigest(),
            )


def test_all_inline_invocation_bytes_and_absence_of_input_artifacts(tmp_path: Path) -> None:
    node = NodeFunction(cast(Any, inline_identity))
    [port] = node.inputs
    context = m_node_runtime.NodeRuntimeContext(
        node=node,
        node_label="inline",
        inputs={port: m_node_runtime.InlineInput({"value": [1, 2]})},
        output_port=node.get_output_port(DEFAULT_PORT),
        callback=lambda _node, _inputs: {DEFAULT_PORT: None},
        work_dir=tmp_path / "inline",
        environment_provider=m_node_runtime.CurrentPythonEnvironmentProvider(),
        runtime_config={},
        environment_spec=[],
    )

    assert m_node_runtime._subprocess_input_json(context) == '{"args":[],"kwargs":{"value":{"value": [1, 2]}}}'
    assert m_node_runtime.prepare_isolated_node_inputs(context, runtime_name="venv-subprocess") is context
    assert not context.work_dir.exists()
    invocation = m_node_runtime._prepare_spl_free_invocation(context, runtime_name="venv-subprocess")
    assert invocation.input_path.read_text(encoding="utf-8") == ('{"args":[],"kwargs":{"value":{"value": [1, 2]}}}')
    assert not (context.work_dir / "runtime-inputs").exists()
    assert not invocation.artifacts_dir.exists()


def test_docker_mixed_input_uses_safe_staging_and_preserves_hardening(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_path, payload = _prepared_runner_case(tmp_path)
    node_dir = input_path.parent
    node = NodeFunction(cast(Any, consume_integer_set))
    [port] = node.inputs
    item = payload["runtime_port_adapters"]["inputs"][0]
    adapter = Adapter(
        key=make_key(set, "integer-set"),
        save=save_integer_set,
        load=load_integer_set,
        py_type=set,
        format="integer-set",
    )
    source = tmp_path / "source.bin"
    context = m_node_runtime.NodeRuntimeContext(
        node=node,
        node_label="consumer",
        inputs={
            port: m_node_runtime.ArtifactInput(
                ref=ArtifactRef(
                    key=adapter.key,
                    uri=str(source),
                    sha256=item["sha256"],
                    size=item["size"],
                    tag=adapter.tag,
                ),
                load_adapter=adapter,
                resolution_source="edge",
            )
        },
        output_port=node.get_output_port(DEFAULT_PORT),
        callback=lambda _node, _inputs: {DEFAULT_PORT: None},
        work_dir=tmp_path / "docker-node",
        environment_provider=m_node_runtime.CurrentPythonEnvironmentProvider(),
        runtime_config={"docker": {"image": "python:3.13-slim", "network": "none"}},
        environment_spec=[],
    )
    prepared = m_node_runtime.prepare_isolated_node_inputs(context, runtime_name="docker")
    commands: list[list[str]] = []

    def fake_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        (prepared.work_dir / "result.json").write_text('{"result": 3, "artifacts": {}}', encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(m_node_runtime, "run_process_tree", fake_run)
    runtime = m_node_runtime.DockerNodeRuntime()
    environment = runtime.prepare(prepared)
    assert runtime.execute(prepared, environment) == {DEFAULT_PORT: 3}
    [command] = commands
    assert command[command.index("--network") + 1] == "none"
    assert "--read-only" in command
    assert command[command.index("-v") + 1] == f"{prepared.work_dir.resolve()}:/spl-node"
    docker_payload = json.loads((prepared.work_dir / "input.json").read_text(encoding="utf-8"))
    assert str(tmp_path) not in json.dumps(docker_payload)
    assert (prepared.work_dir / "runtime-inputs" / item["staged_name"]).is_file()
    assert node_dir != prepared.work_dir


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    completed = subprocess.run(
        ["docker", "info"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=15,
    )
    return completed.returncode == 0


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_live_docker_custom_artifact_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    global _producer_calls
    _producer_calls = 0
    image = "python:3.13-slim"
    inspected = subprocess.run(
        ["docker", "image", "inspect", image],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=15,
    )
    if inspected.returncode != 0:
        pytest.skip(f"Docker image is unavailable locally: {image}")
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    pipeline = _set_pipeline(runtime="docker")
    run = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}},
    ).run(keep=True)

    with run:
        assert run.value("consumer")["values"] == [1, 2, 3]
    assert _producer_calls == 1

    resumed = run.resume(from_="consumer", keep=True)
    with resumed:
        assert resumed.value("consumer")["values"] == [1, 2, 3]

    assert _producer_calls == 1
    manifest = cast(dict[str, Any], resumed.manifest_snapshot)
    assert _node_by_alias(manifest, "producer")["status"] == "frozen"
    assert _node_by_alias(manifest, "consumer")["status"] == "succeeded"


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_live_docker_ndarray_artifact_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    image = "splime-phase1-numpy-test:py313"
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM python:3.13-slim\nRUN python -m pip install --no-cache-dir numpy=={}\n".format(np.__version__),
        encoding="utf-8",
    )
    built = subprocess.run(
        ["docker", "build", "--tag", image, str(tmp_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
        timeout=300,
    )
    assert built.returncode == 0, built.stdout[-4000:]
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(
        _numpy_pipeline(runtime="docker"),
        runtime_config={"docker": {"image": image, "network": "none"}},
    ).run(keep=True)

    with run:
        assert run.value("consumer") == {
            "type": "ndarray",
            "shape": [2, 2],
            "values": [[1, 2], [3, 4]],
        }
