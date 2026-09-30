from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
import numpy as np  # type: ignore[import-not-found, unused-ignore]  # Optional acceptance dependency.

from spl import Deployment, lift
from spl.core import _common as m_common
from spl.core import manifest as m_manifest
from spl.core import node_runtime as m_node_runtime
from spl.core import resume as m_resume
from spl.core import runtime_port_adapters as m_runtime_port_adapters
from spl.core.entities.adapter import Adapter, SplitLoadAdapter, SplitSaveAdapter, make_key
from spl.core.entities.node import DEFAULT_PORT
from spl.core.entities.node_function import NodeFunction
from spl.daemon import spl_free_runner as m_spl_free_runner


def produce_output_set() -> set[int]:
    return {7, 2, 5}


def produce_output_set_counted() -> set[int]:
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE3_PRODUCER_CALLS")
    if marker is not None:
        with Path(marker).open("a", encoding="utf-8") as handle:
            handle.write("producer\n")
    return {7, 2, 5}


def produce_untyped_output_set():  # type: ignore[no-untyped-def]  # Deliberately untyped producer.
    return {7, 2, 5}


def produce_untyped_output_set_counted():  # type: ignore[no-untyped-def]  # Deliberately untyped producer.
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE3_PRODUCER_CALLS")
    if marker is not None:
        with Path(marker).open("a", encoding="utf-8") as handle:
            handle.write("producer\n")
    return {7, 2, 5}


def produce_seeded_output_set(seed: int) -> set[int]:
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE3_PRODUCER_CALLS")
    if marker is not None:
        with Path(marker).open("a", encoding="utf-8") as handle:
            handle.write("producer\n")
    return {seed, seed + 1}


def produce_untyped_output_set_with_side_effect():  # type: ignore[no-untyped-def]  # Deliberately untyped producer.
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE2_UNFORMATTED_SIDE_EFFECT")
    if marker is not None:
        Path(marker).write_text("set producer executed", encoding="utf-8")
    return {7, 2, 5}


def produce_inline_json() -> dict[str, Any]:
    return {"kind": "inline", "values": [1, 2, 3]}


def produce_untyped_inline_json():  # type: ignore[no-untyped-def]  # Deliberately untyped JSON fast path.
    return {"kind": "inline", "values": [1, 2, 3]}


def consume_typed_inline_json(value: dict[str, Any]) -> list[int]:
    return list(value["values"])


def save_output_dict_text(path: str, value: dict[str, Any]) -> None:
    import json
    from pathlib import Path

    Path(path).write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def load_output_dict_text(path: str) -> dict[str, Any]:
    import json
    from pathlib import Path

    return cast(dict[str, Any], json.loads(Path(path).read_text(encoding="utf-8")))


def consume_output_set(value: set[int]) -> list[int]:
    return sorted(value)


def consume_output_set_alt(value: set[int]) -> list[int]:
    return sorted(value, reverse=True)


def consume_output_set_maybe_fail(value: set[int], should_fail: bool) -> list[int]:
    if should_fail:
        raise RuntimeError("intentional recovery child failure")
    return sorted(value)


def consume_phase3_diamond_left(value: set[int]) -> int:
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE3_DIAMOND_CALLS")
    if marker is not None:
        with Path(marker).open("a", encoding="utf-8") as handle:
            handle.write("left\n")
    return sum(value)


def consume_phase3_diamond_right(value: set[int]) -> int:
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE3_DIAMOND_CALLS")
    if marker is not None:
        with Path(marker).open("a", encoding="utf-8") as handle:
            handle.write("right\n")
    return max(value)


def join_phase3_diamond(left: int, right: int) -> int:
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE3_DIAMOND_CALLS")
    if marker is not None:
        with Path(marker).open("a", encoding="utf-8") as handle:
            handle.write("join\n")
    return left + right


def consume_output_set_isolated(value: set[int]) -> dict[str, Any]:
    import os

    return {
        "values": sorted(value),
        "load_pid": os.environ.get("SPL_PHASE2_LOAD_PID"),
    }


def transform_output_set(value: set[int]) -> set[int]:
    return {item + 1 for item in value}


def save_output_set(path: str, value: set[int]) -> None:
    from pathlib import Path

    Path(path).write_text(",".join(str(item) for item in sorted(value)), encoding="utf-8")


def save_output_set_alt(path: str, value: set[int]) -> None:
    from pathlib import Path

    Path(path).write_text(",".join(str(item) for item in sorted(value)), encoding="utf-8")


def save_output_set_counted(path: str, value: set[int]) -> None:
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE3_SAVE_CALLS")
    if marker is not None:
        with Path(marker).open("a", encoding="utf-8") as handle:
            handle.write("save\n")
    Path(path).write_text(",".join(str(item) for item in sorted(value)), encoding="utf-8")


def save_output_set_override(path: str, value: set[int]) -> None:
    from pathlib import Path

    Path(path).write_text(
        "override:" + ",".join(str(item) for item in sorted(value)),
        encoding="utf-8",
    )


def save_output_set_failure(path: str, value: set[int]) -> None:
    del path, value
    raise RuntimeError("secret output save detail")


def save_output_set_system_exit(path: str, value: set[int]) -> None:
    del path, value
    raise SystemExit("secret output SystemExit detail")


def save_output_set_missing(path: str, value: set[int]) -> None:
    del value
    from pathlib import Path

    Path(path).unlink()


def save_output_set_symlink(path: str, value: set[int]) -> None:
    del value
    from pathlib import Path

    target = Path(path)
    target.unlink()
    target.symlink_to(target.parent / "outside.bin")


def save_output_set_extra_file(path: str, value: set[int]) -> None:
    from pathlib import Path

    target = Path(path)
    target.write_text(",".join(str(item) for item in sorted(value)), encoding="utf-8")
    (target.parent / "extra.bin").write_bytes(b"extra")


def save_output_set_slow(path: str, value: set[int]) -> None:
    import threading
    from pathlib import Path

    threading.Event().wait()
    Path(path).write_text(",".join(str(item) for item in sorted(value)), encoding="utf-8")


def load_output_set(path: str) -> set[int]:
    from pathlib import Path

    return {int(item) for item in Path(path).read_text(encoding="utf-8").split(",")}


def load_output_set_counted(path: str) -> set[int]:
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE3_LOAD_CALLS")
    if marker is not None:
        with Path(marker).open("a", encoding="utf-8") as handle:
            handle.write("{}\n".format(os.getpid()))
    return {int(item) for item in Path(path).read_text(encoding="utf-8").split(",")}


def load_output_set_override(path: str) -> set[int]:
    from pathlib import Path

    body = Path(path).read_text(encoding="utf-8")
    if not body.startswith("override:"):
        raise ValueError("run override bytes were not used")
    return {int(item) for item in body.removeprefix("override:").split(",")}


def load_output_set_isolated_only(path: str) -> set[int]:
    import os
    from pathlib import Path

    if os.environ.get("SPL_PHASE2_CONDUCTOR_PID") == str(os.getpid()):
        raise RuntimeError("load ran in conductor")
    os.environ["SPL_PHASE2_LOAD_PID"] = str(os.getpid())
    return {int(item) for item in Path(path).read_text(encoding="utf-8").split(",")}


def load_output_list(path: str) -> list[int]:
    from pathlib import Path

    return [int(item) for item in Path(path).read_text(encoding="utf-8").split(",")]


def consume_output_list(value: list[int]) -> list[int]:
    return list(reversed(value))


def consume_output_list_counted(value: list[int]) -> list[int]:
    import os
    from pathlib import Path

    marker = os.environ.get("SPL_PHASE3_DIAMOND_CALLS")
    if marker is not None:
        with Path(marker).open("a", encoding="utf-8") as handle:
            handle.write("distinct\n")
    return list(reversed(value))


def produce_output_ndarray() -> Any:
    import numpy as np

    return np.asarray([[11, 12], [13, 14]], dtype=np.int64)


def produce_any_output_ndarray_with_side_effect() -> Any:
    import os
    from pathlib import Path

    import numpy as np

    marker = os.environ.get("SPL_PHASE2_UNFORMATTED_SIDE_EFFECT")
    if marker is not None:
        Path(marker).write_text("ndarray producer executed", encoding="utf-8")
    return np.asarray([[11, 12], [13, 14]], dtype=np.int64)


def consume_output_ndarray(value):  # type: ignore[no-untyped-def]  # Deliberately untyped transport case.
    return {"type": type(value).__name__, "shape": list(value.shape), "values": value.tolist()}


def consume_typed_output_ndarray(value: np.ndarray) -> dict[str, Any]:
    return {"type": type(value).__name__, "shape": list(value.shape), "values": value.tolist()}


def save_output_ndarray(path: str, value: Any) -> None:
    import numpy as np

    with open(path, "wb") as handle:
        np.save(handle, value, allow_pickle=False)


def load_output_ndarray(path: str):  # type: ignore[no-untyped-def]  # Source bundle must not depend on typing.Any.
    import numpy as np

    with open(path, "rb") as handle:
        return np.load(handle, allow_pickle=False)


def _output_pipeline(
    *,
    consumer_runtime: str | None,
    load: Any = load_output_set,
    producer_runtime: str = "venv-subprocess",
) -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    pipeline = (
        lift_any(consume_output_set if consumer_runtime is None else consume_output_set_isolated)
        .bind(value=producer.as_format("phase2-set"))
        .alias("consumer")
        .render("isolated_artifact_output")
        .add_adapter(set, "phase2-set", save=save_output_set, load=load)
        .with_node_runtime("producer", producer_runtime)
    )
    if consumer_runtime is not None:
        pipeline = pipeline.with_node_runtime("consumer", consumer_runtime)
    return pipeline


def _phase3_resume_validation_pipeline() -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set_counted).alias("producer")
    first = lift_any(consume_phase3_diamond_left).bind(value=producer.as_format("phase3-a")).alias("first")
    second = lift_any(consume_phase3_diamond_right).bind(value=producer.as_format("phase3-b")).alias("second")
    return (
        (first.render("phase3_resume_validation") | second.render("phase3_resume_validation"))
        .add_adapter(
            set,
            "phase3-a",
            save=save_output_set_counted,
            load=load_output_set_counted,
        )
        .add_adapter(
            set,
            "phase3-b",
            save=save_output_set_counted,
            load=load_output_set_counted,
        )
        .with_node_runtime(
            "producer",
            "venv-subprocess",
        )
        .with_node_runtime(
            "second",
            "venv-subprocess",
        )
    )


def _phase3_mixed_save_source_pipeline(*, formatted_consumer_runtime: str = "venv-subprocess") -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set_counted).alias("producer")
    pipeline_consumer = lift_any(consume_phase3_diamond_left).bind(value=producer).alias("pipeline-consumer")
    edge_consumer = (
        lift_any(consume_phase3_diamond_right).bind(value=producer.as_format("phase3-shared")).alias("edge-consumer")
    )
    return (
        (pipeline_consumer.render("phase3_mixed_save_source") | edge_consumer.render("phase3_mixed_save_source"))
        .add_adapter(
            set,
            "phase3-shared",
            save=save_output_set_counted,
            load=load_output_set_counted,
        )
        .with_node_runtime("producer", "venv-subprocess")
        .with_node_runtime("edge-consumer", formatted_consumer_runtime)
    )


def _phase3_multi_variant_mixed_save_source_pipeline() -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_untyped_output_set_counted).alias("producer")
    pipeline_consumer = lift_any(consume_phase3_diamond_left).bind(value=producer).alias("pipeline-consumer")
    edge_consumer = (
        lift_any(consume_phase3_diamond_right).bind(value=producer.as_format("phase3-shared")).alias("edge-consumer")
    )
    distinct_consumer = (
        lift_any(consume_output_list_counted).bind(value=producer.as_format("phase3-list")).alias("distinct-consumer")
    )
    return (
        (
            pipeline_consumer.render("phase3_mixed_multi_variant")
            | edge_consumer.render("phase3_mixed_multi_variant")
            | distinct_consumer.render("phase3_mixed_multi_variant")
        )
        .add_adapter(set, "phase3-shared", save=save_output_set_counted, load=load_output_set_counted)
        .add_adapter(list, "phase3-list", save=save_output_set_counted, load=load_output_list)
        .with_node_runtime("producer", "venv-subprocess")
        .with_node_runtime("edge-consumer", "venv-subprocess")
    )


def _phase3_run_override_adapter() -> Adapter:
    return Adapter(
        key=make_key(set, "phase3-run-override"),
        save=save_output_set_counted,
        load=load_output_set_counted,
        py_type=set,
        format="phase3-run-override",
    )


class _Phase3SplitRuntimeOverride:
    def __init__(self) -> None:
        self.key = make_key(set, "phase3-split-override")
        self.tag = "phase3-split-override"
        self.accepted_tags = frozenset((self.tag,))
        self.save = save_output_set_counted
        self.load = load_output_set_counted
        self.legacy_key_guard = False
        self.distributions: tuple[Any, ...] = ()


def _phase3_producer_and_edges(manifest: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    producer = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer["id"]]
    return producer, edges


def _phase3_native_recovery_pipeline() -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set_counted).alias("producer")
    return (
        lift_any(consume_phase3_diamond_left)
        .bind(value=producer.as_format("phase3-shared"))
        .alias("consumer")
        .render("phase3_native_recovery")
        .add_adapter(set, "phase3-shared", save=save_output_set_counted, load=load_output_set_counted)
    )


def _corrupt_execution_plan(retained: Any, tamper: str) -> None:
    assert retained.manifest_path is not None
    assert retained.run_dir is not None
    manifest = cast(dict[str, Any], json.loads(retained.manifest_path.read_text(encoding="utf-8")))
    sidecar_path = retained.run_dir / m_manifest.RUN_EXECUTION_PLAN_FILENAME

    def replace_document(document: dict[str, Any]) -> None:
        sidecar_path.write_text(json.dumps(document), encoding="utf-8")
        manifest["execution_plan"] = m_manifest.run_execution_plan_record(document)

    if tamper == "invalid-manifest-digest":
        manifest["execution_plan"]["evidence"]["sha256"] = "0" * 64
    elif tamper == "missing-manifest-record":
        manifest.pop("execution_plan")
    elif tamper == "missing-sidecar":
        sidecar_path.unlink()
    elif tamper == "sidecar-identity-mismatch":
        document = cast(dict[str, Any], json.loads(sidecar_path.read_text(encoding="utf-8")))
        document["run_id"] = "different-retained-run"
        replace_document(document)
    elif tamper == "malformed-sidecar-json":
        sidecar_path.write_text("{not-json", encoding="utf-8")
    elif tamper == "duplicate-sidecar-json-key":
        sidecar_path.write_text('{"version":1,"version":1}', encoding="utf-8")
    elif tamper == "oversized-sidecar":
        sidecar_path.write_bytes(b"x" * (m_resume._MAX_EXECUTION_PLAN_BYTES + 1))
    elif tamper == "sidecar-symlink":
        target = retained.run_dir.parent / "external-execution-plan.json"
        target.write_text("{}", encoding="utf-8")
        sidecar_path.unlink()
        sidecar_path.symlink_to(target)
    elif tamper == "sidecar-hard-link":
        os.link(sidecar_path, retained.run_dir / "execution-plan-hard-link.json")
    elif tamper == "sidecar-directory":
        sidecar_path.unlink()
        sidecar_path.mkdir()
    elif tamper == "sidecar-fifo":
        sidecar_path.unlink()
        os.mkfifo(sidecar_path)
    elif tamper == "inline-sidecar-mismatch":
        manifest["execution_plan"]["adapter_overrides"][0]["identity"]["key"] = "mismatched-inline-key"
    elif tamper == "duplicate-override-entry":
        document = cast(dict[str, Any], json.loads(sidecar_path.read_text(encoding="utf-8")))
        document["adapter_overrides"].append(deepcopy(document["adapter_overrides"][0]))
        replace_document(document)
    elif tamper in {"unknown-node", "unknown-output-port"}:
        document = cast(dict[str, Any], json.loads(sidecar_path.read_text(encoding="utf-8")))
        field = "node_id" if tamper == "unknown-node" else "port"
        document["adapter_overrides"][0][field] = "unknown"
        replace_document(document)
    elif tamper == "unsupported-version":
        document = cast(dict[str, Any], json.loads(sidecar_path.read_text(encoding="utf-8")))
        document["version"] = 999
        replace_document(document)
    else:
        raise AssertionError("unhandled execution-plan corruption")
    retained.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")


def _rewrite_variant_sources(
    producer: dict[str, Any],
    edges: list[dict[str, Any]],
    variant_ids: set[str],
) -> None:
    output_variants = producer["outputs"][DEFAULT_PORT]["variants"]
    adapter_record = producer["adapters"][DEFAULT_PORT]
    for variant in output_variants:
        if variant["variant_id"] in variant_ids:
            variant["save"]["source"] = "run-override"
    for variant in adapter_record["variants"]:
        if variant["variant_id"] in variant_ids:
            variant["save"]["source"] = "run-override"
    primary = min(output_variants, key=lambda item: item["variant_id"])
    if primary["variant_id"] in variant_ids:
        adapter_record["source"] = "run-override"
    for edge in edges:
        if edge["artifact_variant"] in variant_ids:
            edge["adapter"]["save"]["source"] = "run-override"


def _ndarray_output_pipeline(*, producer_runtime: str, consumer_runtime: str | None) -> Any:
    from spl.core.entities.distribution import DDistribution

    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_ndarray).alias("producer")
    pipeline = (
        lift_any(consume_output_ndarray)
        .bind(value=producer.as_format("npy"))
        .alias("consumer")
        .render("isolated_ndarray_output")
        .add_adapter(
            np.ndarray,
            "npy",
            save=save_output_ndarray,
            load=load_output_ndarray,
            distributions=(DDistribution(package="numpy", version=np.__version__),),
        )
        .with_node_runtime("producer", producer_runtime)
    )
    if consumer_runtime is not None:
        pipeline = pipeline.with_node_runtime("consumer", consumer_runtime)
    return pipeline


def _override_output_pipeline(*, producer_runtime: str | None) -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    pipeline = (
        lift_any(consume_output_set)
        .bind(value=producer.as_format("phase2-set"))
        .alias("consumer")
        .render("isolated_output_override_precedence")
        .add_adapter(
            set,
            "phase2-set",
            save=save_output_set,
            load=load_output_set,
        )
    )
    if producer_runtime is not None:
        pipeline = pipeline.with_node_runtime("producer", producer_runtime)
    return pipeline


def _output_override_adapter() -> Adapter:
    return Adapter(
        key=make_key(set, "phase2-set-override"),
        save=save_output_set_override,
        load=load_output_set_override,
        py_type=set,
        format="phase2-set-override",
    )


def _unformatted_untyped_output_pipeline(kind: str) -> Any:
    lift_any = cast(Any, lift)
    if kind == "set":
        producer = lift_any(produce_untyped_output_set_with_side_effect).alias("producer")
        return (
            lift_any(consume_output_set)
            .bind(value=producer)
            .alias("consumer")
            .render("unformatted_untyped_set_output")
            .add_adapter(set, "phase2-set", save=save_output_set, load=load_output_set)
            .with_node_runtime("producer", "venv-subprocess")
        )
    if kind == "ndarray":
        from spl.core.entities.distribution import DDistribution

        producer = lift_any(produce_any_output_ndarray_with_side_effect).alias("producer")
        return (
            lift_any(consume_typed_output_ndarray)
            .bind(value=producer)
            .alias("consumer")
            .render("unformatted_any_ndarray_output")
            .add_adapter(
                np.ndarray,
                "npy",
                save=save_output_ndarray,
                load=load_output_ndarray,
                distributions=(DDistribution(package="numpy", version=np.__version__),),
            )
            .with_node_runtime("producer", "venv-subprocess")
        )
    raise AssertionError(kind)


def test_venv_artifact_producer_loads_for_native_consumer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_output_pipeline(consumer_runtime=None)).run(keep=True)

    with run:
        assert run.value("consumer") == [2, 5, 7]

    assert run.run_dir is not None
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    output = producer["outputs"]["default"]
    assert output["kind"] == "artifact"
    assert not Path(output["ref"]["uri"]).is_absolute()
    runtime_dir = run.run_dir / "node-runtimes" / producer["id"]
    payload = json.loads((runtime_dir / "input.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == 3
    [binding] = payload["runtime_port_adapters"]["bindings"]
    assert binding["direction"] == "output"
    assert binding["adapter"]["load_symbol"] is None
    bundle = (runtime_dir / "runtime-inputs" / "custom-adapters.py").read_text(encoding="utf-8")
    assert "def save_output_set" in bundle
    assert "def load_output_set" not in bundle


def test_venv_artifact_producer_feeds_venv_without_conductor_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE2_CONDUCTOR_PID", str(os.getpid()))
    run = Deployment(
        _output_pipeline(
            consumer_runtime="venv-subprocess",
            load=load_output_set_isolated_only,
        )
    ).run(keep=True)

    with run:
        result = run.value("consumer")

    assert result["values"] == [2, 5, 7]
    assert result["load_pid"] != str(os.getpid())


def test_mixed_venv_node_receives_and_emits_one_artifact_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    middle = lift_any(transform_output_set).bind(value=producer.as_format("phase2-set")).alias("middle")
    pipeline = (
        lift_any(consume_output_set)
        .bind(value=middle.as_format("phase2-set"))
        .alias("consumer")
        .render("mixed_isolated_artifact_output")
        .add_adapter(set, "phase2-set", save=save_output_set, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
        .with_node_runtime("middle", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("consumer") == [3, 6, 8]

    assert run.run_dir is not None
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    middle_record = next(item for item in manifest["nodes"].values() if item["alias"] == "middle")
    runtime_dir = run.run_dir / "node-runtimes" / middle_record["id"]
    payload = json.loads((runtime_dir / "input.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == 3
    assert [binding["direction"] for binding in payload["runtime_port_adapters"]["bindings"]] == [
        "input",
        "output",
    ]
    bundle = (runtime_dir / "runtime-inputs" / "custom-adapters.py").read_text(encoding="utf-8")
    assert "def load_output_set" in bundle
    assert "def save_output_set" in bundle


@pytest.mark.parametrize("consumer_runtime", [None, "venv-subprocess"])
def test_venv_ndarray_artifact_output_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    consumer_runtime: str | None,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(
        _ndarray_output_pipeline(
            producer_runtime="venv-subprocess",
            consumer_runtime=consumer_runtime,
        )
    ).run(keep=True)

    with run:
        assert run.value("consumer") == {
            "type": "ndarray",
            "shape": [2, 2],
            "values": [[11, 12], [13, 14]],
        }


@pytest.mark.parametrize("producer_runtime", [None, "venv-subprocess"])
def test_run_override_replaces_explicit_edge_format_with_native_isolated_parity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    producer_runtime: str | None,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    override = _output_override_adapter()
    run = Deployment(_override_output_pipeline(producer_runtime=producer_runtime)).run(
        keep=True,
        adapters={("producer", DEFAULT_PORT): override},
    )

    with run:
        assert run.value("consumer") == [2, 5, 7]

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    [edge] = manifest["edges"]
    assert edge["adapter"]["save"]["source"] == "run-override"
    assert edge["adapter"]["load"]["source"] == "run-override"
    assert edge["adapter"]["save"]["identity"]["key"] == override.key
    assert edge["adapter"]["load"]["identity"]["key"] == override.key
    producer = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    assert producer["outputs"][DEFAULT_PORT]["ref"]["key"] == override.key
    if producer_runtime == "venv-subprocess":
        assert run.run_dir is not None
        payload = json.loads(
            (run.run_dir / "node-runtimes" / producer["id"] / "input.json").read_text(encoding="utf-8")
        )
        [binding] = payload["runtime_port_adapters"]["bindings"]
        assert binding["resolution_source"] == "run_override"
        assert binding["adapter"]["key"] == override.key


@pytest.mark.parametrize("first_alias", ["producer", "consumer"])
def test_untyped_ndarray_uses_graph_format_independent_of_access_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    first_alias: str,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(
        _ndarray_output_pipeline(
            producer_runtime="venv-subprocess",
            consumer_runtime=None,
        )
    ).run(keep=True)
    expected = {
        "type": "ndarray",
        "shape": [2, 2],
        "values": [[11, 12], [13, 14]],
    }

    with run:
        if first_alias == "producer":
            assert run.value("producer").tolist() == [[11, 12], [13, 14]]
        assert run.value("consumer") == expected

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    assert producer["outputs"][DEFAULT_PORT]["ref"]["key"] == make_key(np.ndarray, "npy")


def test_untyped_custom_output_uses_explicit_graph_format_when_producer_is_requested_first(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_untyped_output_set).alias("producer")
    pipeline = (
        lift_any(consume_output_set)
        .bind(value=producer.as_format("phase2-set"))
        .alias("consumer")
        .render("producer_first_custom_output")
        .add_adapter(set, "phase2-set", save=save_output_set, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("producer") == {2, 5, 7}
        assert run.value("consumer") == [2, 5, 7]

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    assert producer_record["outputs"][DEFAULT_PORT]["ref"]["key"] == make_key(set, "phase2-set")


def test_terminal_venv_artifact_output_preserves_python_value_ux(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(produce_output_set)
        .alias("producer")
        .render("terminal_isolated_artifact")
        .add_adapter(set, "phase2-set", save=save_output_set, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("producer") == {2, 5, 7}


def test_legacy_all_inline_result_protocol_and_runtime_tree_are_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(produce_inline_json)
        .alias("producer")
        .render("legacy_inline_output")
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)
    expected = {"kind": "inline", "values": [1, 2, 3]}

    with run:
        assert run.value("producer") == expected

    assert run.run_dir is not None
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    runtime_dir = run.run_dir / "node-runtimes" / producer["id"]
    assert (runtime_dir / "input.json").read_bytes() == b'{"args":[],"kwargs":{}}'
    expected_result_protocol = {
        "artifacts": {},
        "result": expected,
    }
    assert (runtime_dir / "result.json").read_bytes() == json.dumps(
        expected_result_protocol,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")
    assert not (runtime_dir / "runtime-inputs").exists()
    assert not (runtime_dir / "runtime-output-tmp").exists()
    assert not (runtime_dir / "runtime-outputs").exists()
    assert not (runtime_dir / "artifacts").exists()
    assert not (run.run_dir / "artifacts").exists()


def test_untyped_output_with_only_json_graph_requirement_keeps_legacy_fast_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_untyped_inline_json).alias("producer")
    pipeline = (
        lift_any(consume_typed_inline_json)
        .bind(value=producer)
        .alias("consumer")
        .render("unformatted_untyped_json_output")
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("producer") == {"kind": "inline", "values": [1, 2, 3]}
        assert run.value("consumer") == [1, 2, 3]

    assert run.run_dir is not None
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    runtime_dir = run.run_dir / "node-runtimes" / producer_record["id"]
    assert not (runtime_dir / "runtime-output-tmp").exists()
    assert not (runtime_dir / "runtime-outputs").exists()
    assert not (run.run_dir / "artifacts").exists()


def test_phase3_mixed_json_and_custom_requirements_materialize_exact_variants(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_inline_json).alias("producer")
    json_consumer = lift_any(consume_typed_inline_json).bind(value=producer).alias("json-consumer")
    custom_consumer = (
        lift_any(consume_typed_inline_json).bind(value=producer.as_format("phase3-dict-text")).alias("custom-consumer")
    )
    pipeline = (
        (json_consumer.render("phase3_mixed_json") | custom_consumer.render("phase3_mixed_json"))
        .add_adapter(
            dict,
            "phase3-dict-text",
            save=save_output_dict_text,
            load=load_output_dict_text,
        )
        .with_node_runtime("producer", "venv-subprocess")
        .with_node_runtime("json-consumer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("producer") == {"kind": "inline", "values": [1, 2, 3]}
        assert run.value("json-consumer") == [1, 2, 3]
        assert run.value("custom-consumer") == [1, 2, 3]

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    variants = producer_record["outputs"][DEFAULT_PORT]["variants"]
    assert {item["artifact"]["ref"]["key"] for item in variants} == {
        "spl.core.json@json",
        make_key(dict, "phase3-dict-text"),
    }


def test_retained_isolated_output_is_relative_and_resumable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    parent = Deployment(_output_pipeline(consumer_runtime=None)).run(keep=True)
    with parent:
        assert parent.value("consumer") == [2, 5, 7]

    child = parent.resume(from_="consumer", keep=True)
    with child:
        assert child.value("consumer") == [2, 5, 7]

    parent_manifest = cast(dict[str, Any], parent.manifest_snapshot)
    parent_producer = next(item for item in parent_manifest["nodes"].values() if item["alias"] == "producer")
    assert not Path(parent_producer["outputs"][DEFAULT_PORT]["ref"]["uri"]).is_absolute()
    manifest = cast(dict[str, Any], child.manifest_snapshot)
    producer = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    assert producer["status"] == "frozen"
    child_uri = producer["outputs"][DEFAULT_PORT]["ref"]["uri"]
    assert not Path(child_uri).is_absolute()
    assert child.run_dir is not None
    assert (child.run_dir / child_uri).is_file()


@pytest.mark.parametrize("artifact_uri", ["relative", "absolute"])
def test_phase3_resume_accepts_unambiguous_legacy_single_artifact_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_uri: str,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    pipeline = _output_pipeline(consumer_runtime=None)
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("consumer") == [2, 5, 7]

    assert parent.manifest_path is not None
    assert parent.run_dir is not None
    manifest = cast(
        dict[str, Any],
        json.loads(parent.manifest_path.read_text(encoding="utf-8")),
    )
    producer = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    output = producer["outputs"][DEFAULT_PORT]
    output.pop("variants", None)
    if artifact_uri == "absolute":
        output["ref"]["uri"] = str(parent.run_dir / output["ref"]["uri"])
    producer["adapters"][DEFAULT_PORT].pop("variants", None)
    for edge in manifest["edges"]:
        edge.pop("artifact_variant", None)
        edge["artifact"]["ref"]["uri"] = output["ref"]["uri"]
    manifest.pop("execution_plan")
    (parent.run_dir / "execution-plan.json").unlink()
    parent.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    child = Deployment(pipeline).resume(parent.run_id, from_="consumer", keep=True)
    with child:
        assert child.value("consumer") == [2, 5, 7]

    child_manifest = cast(dict[str, Any], child.manifest_snapshot)
    frozen = next(item for item in child_manifest["nodes"].values() if item["alias"] == "producer")
    assert frozen["status"] == "frozen"


@pytest.mark.parametrize("generation", ["parent", "resume-of-resume"])
@pytest.mark.parametrize(
    ("tamper", "expected_error"),
    [
        ("absolute-node-variant-and-edge", "current artifact uri must be relative"),
        ("missing-edge-variant-with-substituted-artifact", "missing artifact_variant"),
        ("edge-artifact-mismatch", "artifact does not exactly match"),
        ("edge-save-mismatch", "save provenance does not exactly match"),
        ("top-level-artifact-mismatch", "top-level compatibility artifact does not match"),
        ("top-level-save-mismatch", "top-level compatibility save provenance does not match"),
        ("marker-stripped-contained-edge", "marker-free artifact evidence is ambiguous"),
        ("marker-stripped-absolute-edge", "marker-free artifact evidence is ambiguous"),
    ],
)
def test_phase3_resume_rejects_tampered_current_variant_manifest_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    generation: str,
    tamper: str,
    expected_error: str,
) -> None:
    runs_home = tmp_path / "runs"
    producer_marker = tmp_path / "producer-calls.txt"
    consumer_marker = tmp_path / "consumer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(consumer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    pipeline = _phase3_resume_validation_pipeline()
    retained = Deployment(pipeline).run(keep=True)
    with retained:
        assert retained.value("first") == 14
        assert retained.value("second") == 7

    if generation == "resume-of-resume":
        retained = Deployment(pipeline).resume(
            retained.run_id,
            from_=["first", "second"],
            keep=True,
        )
        with retained:
            assert retained.value("second") == 7
            assert retained.value("first") == 14

    assert retained.manifest_path is not None
    assert retained.run_dir is not None
    manifest = cast(
        dict[str, Any],
        json.loads(retained.manifest_path.read_text(encoding="utf-8")),
    )
    producer = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    output = producer["outputs"][DEFAULT_PORT]
    variants = output["variants"]
    primary = min(variants, key=lambda item: item["variant_id"])
    alternate = next(item for item in variants if item["variant_id"] != primary["variant_id"])
    producer_edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer["id"]]
    alternate_edge = next(edge for edge in producer_edges if edge["artifact_variant"] == alternate["variant_id"])

    def replacement_artifact(template: dict[str, Any], path: Path, payload: bytes) -> dict[str, Any]:
        path.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        replacement = deepcopy(template)
        replacement["sha256"] = digest
        replacement["ref"].update(
            uri=(
                str(path)
                if path.is_absolute() and retained.run_dir not in path.parents
                else path.relative_to(cast(Path, retained.run_dir)).as_posix()
            ),
            sha256=digest,
            size=len(payload),
        )
        return replacement

    external_path: Path | None = None
    if tamper == "absolute-node-variant-and-edge":
        external_path = tmp_path / "external-substitute.bin"
        replacement = replacement_artifact(
            alternate["artifact"],
            external_path,
            b"41,43",
        )
        alternate["artifact"] = replacement
        alternate_edge["artifact"] = deepcopy(replacement)
    elif tamper == "missing-edge-variant-with-substituted-artifact":
        replacement = replacement_artifact(
            alternate["artifact"],
            retained.run_dir / "artifacts" / "contained-substitute.bin",
            b"41,43",
        )
        alternate_edge.pop("artifact_variant")
        alternate_edge["artifact"] = replacement
    elif tamper == "edge-artifact-mismatch":
        alternate_edge["artifact"] = deepcopy(primary["artifact"])
    elif tamper == "edge-save-mismatch":
        alternate_edge["adapter"]["save"] = deepcopy(primary["save"])
    elif tamper == "top-level-artifact-mismatch":
        output.update(deepcopy(alternate["artifact"]))
    elif tamper == "top-level-save-mismatch":
        producer["adapters"][DEFAULT_PORT].update(deepcopy(alternate["save"]))
    elif tamper in {"marker-stripped-contained-edge", "marker-stripped-absolute-edge"}:
        output.pop("variants")
        producer["adapters"][DEFAULT_PORT].pop("variants")
        for edge in producer_edges:
            edge.pop("artifact_variant")
        if tamper == "marker-stripped-absolute-edge":
            external_path = tmp_path / "external-marker-stripped.bin"
            replacement_path = external_path
        else:
            replacement_path = retained.run_dir / "artifacts" / "contained-marker-stripped.bin"
        alternate_edge["artifact"] = replacement_artifact(
            alternate["artifact"],
            replacement_path,
            b"47,53",
        )
    else:
        raise AssertionError("unhandled tamper case")

    retained.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    producer_calls_before = producer_marker.read_bytes()
    save_calls_before = save_marker.read_bytes()
    load_calls_before = load_marker.read_bytes()
    consumer_marker.unlink(missing_ok=True)
    runs_before = {path.name for path in runs_home.iterdir()}
    runtime_dirs_before = {path.relative_to(runs_home) for path in runs_home.glob("*/node-runtimes/*") if path.is_dir()}

    with pytest.raises(RuntimeError) as exc_info:
        Deployment(pipeline).resume(
            retained.run_id,
            from_=["first", "second"],
            keep=True,
        )

    message = str(exc_info.value)
    assert expected_error in message
    assert "from_='producer'" in message
    assert "Traceback" not in message
    assert "save_output_set" not in message
    if external_path is not None:
        assert str(external_path) not in message
    assert producer_marker.read_bytes() == producer_calls_before
    assert save_marker.read_bytes() == save_calls_before
    assert load_marker.read_bytes() == load_calls_before
    assert not consumer_marker.exists()
    assert {path.name for path in runs_home.iterdir()} == runs_before
    assert {
        path.relative_to(runs_home) for path in runs_home.glob("*/node-runtimes/*") if path.is_dir()
    } == runtime_dirs_before


def test_keep_false_removes_promoted_output_and_node_runtime_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(produce_output_set)
        .alias("producer")
        .render("transient_isolated_artifact")
        .add_adapter(set, "phase2-set", save=save_output_set, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=False)
    with run:
        assert run.value("producer") == {2, 5, 7}
        artifacts_dir = run._artifacts_dir
        assert artifacts_dir is not None
        assert any(artifacts_dir.glob("artifact-*"))
        assert (artifacts_dir / "node-runtimes").exists()

    assert not artifacts_dir.exists()


@pytest.mark.parametrize(
    ("keep", "failing", "retained"),
    [
        (False, False, False),
        (True, False, True),
        ("on_failure", False, False),
        (False, True, False),
        (True, True, True),
        ("on_failure", True, True),
    ],
)
def test_phase3_multi_variant_cleanup_obeys_keep_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    keep: Any,
    failing: bool,
    retained: bool,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set).bind(value=producer.as_format("phase3-a")).alias("first")
    second = lift_any(consume_output_set_alt).bind(value=producer.as_format("phase3-b")).alias("second")
    pipeline = (
        (first.render("phase3_keep_policy") | second.render("phase3_keep_policy"))
        .add_adapter(set, "phase3-a", save=save_output_set, load=load_output_set)
        .add_adapter(
            set,
            "phase3-b",
            save=save_output_set_failure if failing else save_output_set_alt,
            load=load_output_set,
        )
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=keep)

    with run:
        if failing:
            with pytest.raises(RuntimeError, match="node_adapter_save"):
                run.value("first")
        else:
            assert run.value("first") == [2, 5, 7]
            assert run.value("second") == [7, 5, 2]
        artifacts_dir = run._artifacts_dir
        assert artifacts_dir is not None

    assert artifacts_dir.exists() is retained
    if retained and failing:
        assert list(artifacts_dir.glob("artifact-*")) == []
    elif retained:
        assert len(list(artifacts_dir.glob("artifact-*"))) == 2


def test_untyped_isolated_non_json_output_uses_unique_graph_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_untyped_output_set).alias("producer")
    pipeline = (
        lift_any(consume_output_set)
        .bind(value=producer)
        .alias("consumer")
        .render("untyped_isolated_artifact")
        .add_adapter(set, "phase2-set", save=save_output_set, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("consumer") == [2, 5, 7]

    assert run.run_dir is not None
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    assert (run.run_dir / "node-runtimes" / producer_record["id"]).exists()


@pytest.mark.parametrize("kind", ["set", "ndarray"])
def test_unique_unformatted_non_json_requirement_is_access_order_independent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    results: list[Any] = []
    for access_order in ("producer", "consumer"):
        case_dir = tmp_path / "{}-{}".format(kind, access_order)
        marker = case_dir / "user-code-side-effect.txt"
        monkeypatch.setenv("SPL_RUNS_HOME", str(case_dir / "runs"))
        monkeypatch.setenv("SPL_PHASE2_UNFORMATTED_SIDE_EFFECT", str(marker))
        run = Deployment(_unformatted_untyped_output_pipeline(kind)).run(keep=True)

        with run:
            run.value(access_order)
            consumer_result = run.value("consumer")
            results.append(consumer_result)
        if kind == "set":
            assert consumer_result == [2, 5, 7]
        else:
            assert consumer_result == {
                "type": "ndarray",
                "shape": [2, 2],
                "values": [[11, 12], [13, 14]],
            }
        assert marker.exists()
        assert run.run_dir is not None
        assert (run.run_dir / "node-runtimes").exists()

    assert type(results[0]) is type(results[1])


@pytest.mark.parametrize("access_order", ["producer", "consumer"])
def test_ambiguous_unformatted_output_fails_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    access_order: str,
) -> None:
    marker = tmp_path / "producer-side-effect.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE2_UNFORMATTED_SIDE_EFFECT", str(marker))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_untyped_output_set_with_side_effect).alias("producer")
    pipeline = (
        lift_any(consume_output_set)
        .bind(value=producer)
        .alias("consumer")
        .render("ambiguous_unformatted_output")
        .add_adapter(set, "phase2-set", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase2-set-alt", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(ValueError) as exc_info:
        run.value(access_order)

    message = str(exc_info.value)
    assert message.startswith("adapter_resolution:")
    assert "producer" in message and DEFAULT_PORT in message
    assert make_key(set, "phase2-set") in message
    assert make_key(set, "phase2-set-alt") in message
    assert "`.as_format()`" in message
    assert "run-level adapter override" in message
    assert not marker.exists()
    assert run.run_dir is not None
    assert not (run.run_dir / "node-runtimes").exists()


def test_explicit_format_resolves_otherwise_ambiguous_untyped_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_untyped_output_set).alias("producer")
    pipeline = (
        lift_any(consume_output_set)
        .bind(value=producer.as_format("phase2-set-alt"))
        .alias("consumer")
        .render("explicit_ambiguous_output")
        .add_adapter(set, "phase2-set", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase2-set-alt", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )

    run = Deployment(pipeline).run(keep=True)
    with run:
        assert run.value("producer") == {2, 5, 7}
        assert run.value("consumer") == [2, 5, 7]

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    [edge] = manifest["edges"]
    assert edge["artifact"]["ref"]["key"] == make_key(set, "phase2-set-alt")


def test_run_override_resolves_untyped_terminal_isolated_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(produce_untyped_output_set)
        .alias("producer")
        .render("untyped_override_artifact")
        .with_node_runtime("producer", "venv-subprocess")
    )
    adapter = Adapter(
        key=make_key(set, "phase2-set"),
        save=save_output_set,
        load=load_output_set,
        py_type=set,
        format="phase2-set",
    )
    run = Deployment(pipeline).run(
        keep=True,
        adapters={("producer", DEFAULT_PORT): adapter},
    )

    with run:
        assert run.value("producer") == {2, 5, 7}


def test_phase3_materializes_multiple_graph_formats_before_consumers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set).bind(value=producer.as_format("phase2-set")).alias("first")
    second = lift_any(consume_output_set_alt).bind(value=producer.as_format("phase2-set-alt")).alias("second")
    pipeline = (
        (first.render("phase2_one_format") | second.render("phase2_one_format"))
        .add_adapter(set, "phase2-set", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase2-set-alt", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("producer") == {2, 5, 7}
        assert run.value("first") == [2, 5, 7]
        assert run.value("second") == [7, 5, 2]

    assert run.run_dir is not None
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    variants = producer_record["outputs"][DEFAULT_PORT]["variants"]
    assert {item["artifact"]["ref"]["key"] for item in variants} == {
        make_key(set, "phase2-set"),
        make_key(set, "phase2-set-alt"),
    }
    assert (run.run_dir / "node-runtimes" / producer_record["id"]).exists()


def test_phase3_same_format_isolated_consumers_share_one_save_and_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "save-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(marker))
    monkeypatch.setenv("SPL_PHASE2_CONDUCTOR_PID", str(os.getpid()))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set_isolated).bind(value=producer.as_format("phase3-set")).alias("first")
    second = lift_any(consume_output_set_isolated).bind(value=producer.as_format("phase3-set")).alias("second")
    pipeline = (
        (first.render("phase3_shared_output") | second.render("phase3_shared_output"))
        .add_adapter(
            set,
            "phase3-set",
            save=save_output_set_counted,
            load=load_output_set_isolated_only,
        )
        .with_node_runtime("producer", "venv-subprocess")
        .with_node_runtime("first", "venv-subprocess")
        .with_node_runtime("second", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("second")["values"] == [2, 5, 7]
        assert run.value("first")["values"] == [2, 5, 7]

    assert marker.read_text(encoding="utf-8").splitlines() == ["save"]
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    output = producer_record["outputs"][DEFAULT_PORT]
    assert "variants" not in output
    producer_edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer_record["id"]]
    assert len(producer_edges) == 2
    assert len({edge["artifact"]["ref"]["uri"] for edge in producer_edges}) == 1
    assert len({edge["artifact_variant"] for edge in producer_edges}) == 1
    assert len(list((cast(Path, run.run_dir) / "artifacts").glob("artifact-*"))) == 1


def test_phase3_mixed_pipeline_and_edge_sources_share_one_variant_in_both_access_orders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = _phase3_mixed_save_source_pipeline()
    observed: list[dict[str, Any]] = []
    for index, order in enumerate(
        (
            ("pipeline-consumer", "edge-consumer"),
            ("edge-consumer", "pipeline-consumer"),
        )
    ):
        runs_home = tmp_path / "runs-{}".format(index)
        save_marker = tmp_path / "save-{}.txt".format(index)
        monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
        monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
        run = Deployment(pipeline).run(keep=True)

        with run:
            results = {alias: run.value(alias) for alias in order}

        assert results == {"pipeline-consumer": 14, "edge-consumer": 7}
        assert save_marker.read_text(encoding="utf-8").splitlines() == ["save"]
        assert run.run_dir is not None
        manifest = cast(dict[str, Any], run.manifest_snapshot)
        nodes = {item["id"]: item for item in manifest["nodes"].values()}
        producer_record = next(item for item in nodes.values() if item["alias"] == "producer")
        output = producer_record["outputs"][DEFAULT_PORT]
        producer_edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer_record["id"]]
        edge_facts = sorted(
            (
                nodes[edge["target"]["node_id"]]["alias"],
                edge["artifact_variant"],
                edge["artifact"],
                edge["adapter"]["save"],
            )
            for edge in producer_edges
        )
        assert {item[1] for item in edge_facts} == {producer_edges[0]["artifact_variant"]}
        assert len({json.dumps(item[2], sort_keys=True) for item in edge_facts}) == 1
        assert {item[0]: item[3]["source"] for item in edge_facts} == {
            "edge-consumer": "edge",
            "pipeline-consumer": "pipeline",
        }
        assert producer_record["adapters"][DEFAULT_PORT]["source"] == "edge"
        assert "variants" not in output
        assert len(list((run.run_dir / "artifacts").glob("artifact-*"))) == 1
        normalized_output = deepcopy(output)
        normalized_output["ref"].pop("uri")
        normalized_edges = deepcopy(edge_facts)
        for _, _, artifact, _ in normalized_edges:
            artifact["ref"].pop("uri")
        observed.append(
            {
                "results": results,
                "output": normalized_output,
                "edges": normalized_edges,
                "fingerprint": producer_record["fingerprint"],
            }
        )

    assert observed[0] == observed[1]


def test_phase3_mixed_save_sources_resume_and_resume_of_resume_preserve_exact_edge_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    consumer_marker = tmp_path / "consumer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(consumer_marker))
    pipeline = _phase3_mixed_save_source_pipeline()
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("pipeline-consumer") == 14
        assert parent.value("edge-consumer") == 7

    child = parent.resume(from_=["pipeline-consumer", "edge-consumer"], keep=True)
    with child:
        assert child.value("edge-consumer") == 7
        assert child.value("pipeline-consumer") == 14

    grandchild = Deployment(pipeline).resume(
        child.run_id,
        from_=["pipeline-consumer", "edge-consumer"],
        keep=True,
    )
    with grandchild:
        assert grandchild.value("pipeline-consumer") == 14
        assert grandchild.value("edge-consumer") == 7

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer"]
    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save"]
    assert consumer_marker.read_text(encoding="utf-8").splitlines().count("left") == 3
    assert consumer_marker.read_text(encoding="utf-8").splitlines().count("right") == 3
    load_pids = load_marker.read_text(encoding="utf-8").splitlines()
    assert load_pids.count(str(os.getpid())) == 3
    assert len(load_pids) == 6

    parent_manifest = cast(dict[str, Any], parent.manifest_snapshot)
    parent_nodes = {item["id"]: item for item in parent_manifest["nodes"].values()}
    parent_producer = next(item for item in parent_nodes.values() if item["alias"] == "producer")
    retained_sha256 = parent_producer["outputs"][DEFAULT_PORT]["ref"]["sha256"]
    retained_variant_id = next(
        edge["artifact_variant"]
        for edge in parent_manifest["edges"]
        if edge["source"]["node_id"] == parent_producer["id"]
    )
    for run in (child, grandchild):
        assert run.run_dir is not None
        manifest = cast(dict[str, Any], run.manifest_snapshot)
        nodes = {item["id"]: item for item in manifest["nodes"].values()}
        producer_record = next(item for item in nodes.values() if item["alias"] == "producer")
        assert producer_record["status"] == "frozen"
        output_ref = producer_record["outputs"][DEFAULT_PORT]["ref"]
        assert output_ref["sha256"] == retained_sha256
        assert not Path(output_ref["uri"]).is_absolute()
        assert (run.run_dir / output_ref["uri"]).is_file()
        edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer_record["id"]]
        assert {edge["artifact_variant"] for edge in edges} == {retained_variant_id}
        assert {nodes[edge["target"]["node_id"]]["alias"]: edge["adapter"]["save"]["source"] for edge in edges} == {
            "edge-consumer": "edge",
            "pipeline-consumer": "pipeline",
        }
        assert all(not Path(edge["artifact"]["ref"]["uri"]).is_absolute() for edge in edges)
        assert all((run.run_dir / edge["artifact"]["ref"]["uri"]).is_file() for edge in edges)


def test_phase3_multi_variant_shared_identity_keeps_edge_sources_across_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_untyped_output_set_counted).alias("producer")
    pipeline_consumer = lift_any(consume_phase3_diamond_left).bind(value=producer).alias("pipeline-consumer")
    edge_consumer = (
        lift_any(consume_phase3_diamond_right).bind(value=producer.as_format("phase3-shared")).alias("edge-consumer")
    )
    distinct_consumer = (
        lift_any(consume_output_list).bind(value=producer.as_format("phase3-list")).alias("distinct-consumer")
    )
    pipeline = (
        (
            pipeline_consumer.render("phase3_mixed_multi_variant")
            | edge_consumer.render("phase3_mixed_multi_variant")
            | distinct_consumer.render("phase3_mixed_multi_variant")
        )
        .add_adapter(set, "phase3-shared", save=save_output_set_counted, load=load_output_set)
        .add_adapter(list, "phase3-list", save=save_output_set_counted, load=load_output_list)
        .with_node_runtime("producer", "venv-subprocess")
    )
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("distinct-consumer") == [7, 5, 2]
        assert parent.value("edge-consumer") == 7
        assert parent.value("pipeline-consumer") == 14

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer"]
    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save", "save"]
    child = parent.resume(
        from_=["pipeline-consumer", "edge-consumer", "distinct-consumer"],
        keep=True,
    )
    with child:
        assert child.value("pipeline-consumer") == 14
        assert child.value("edge-consumer") == 7
        assert child.value("distinct-consumer") == [7, 5, 2]

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer"]
    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save", "save"]
    for run in (parent, child):
        manifest = cast(dict[str, Any], run.manifest_snapshot)
        nodes = {item["id"]: item for item in manifest["nodes"].values()}
        producer_record = next(item for item in nodes.values() if item["alias"] == "producer")
        variants = producer_record["outputs"][DEFAULT_PORT]["variants"]
        assert len(variants) == 2
        variant_by_key = {item["artifact"]["ref"]["key"]: item["variant_id"] for item in variants}
        edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer_record["id"]]
        by_alias = {nodes[edge["target"]["node_id"]]["alias"]: edge for edge in edges}
        assert by_alias["pipeline-consumer"]["artifact_variant"] == variant_by_key[make_key(set, "phase3-shared")]
        assert by_alias["edge-consumer"]["artifact_variant"] == variant_by_key[make_key(set, "phase3-shared")]
        assert by_alias["distinct-consumer"]["artifact_variant"] == variant_by_key[make_key(list, "phase3-list")]
        assert {alias: edge["adapter"]["save"]["source"] for alias, edge in by_alias.items()} == {
            "distinct-consumer": "edge",
            "edge-consumer": "edge",
            "pipeline-consumer": "pipeline",
        }


def test_phase3_resume_rejects_tampered_edge_source_before_any_child_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    consumer_marker = tmp_path / "consumer-calls.txt"
    runs_home = tmp_path / "runs"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(consumer_marker))
    pipeline = _phase3_mixed_save_source_pipeline()
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("pipeline-consumer") == 14
        assert parent.value("edge-consumer") == 7

    assert parent.manifest_path is not None
    manifest = json.loads(parent.manifest_path.read_text(encoding="utf-8"))
    nodes = {item["id"]: item for item in manifest["nodes"].values()}
    pipeline_consumer_id = next(item["id"] for item in nodes.values() if item["alias"] == "pipeline-consumer")
    tampered_edge = next(edge for edge in manifest["edges"] if edge["target"]["node_id"] == pipeline_consumer_id)
    tampered_edge["adapter"]["save"]["source"] = "edge"
    parent.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    before_dirs = {path.name for path in runs_home.iterdir()}
    before_side_effects = {
        path: path.read_text(encoding="utf-8") for path in (producer_marker, save_marker, load_marker, consumer_marker)
    }
    calls = {"run": 0, "copy": 0, "decode": 0}

    def unexpected_run(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["run"] += 1
        raise AssertionError("child Run must not be created")

    def unexpected_copy(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["copy"] += 1
        raise AssertionError("artifact copy must not run")

    def unexpected_decode(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["decode"] += 1
        raise AssertionError("adapter decode must not run")

    monkeypatch.setattr(m_common, "Run", unexpected_run)
    monkeypatch.setattr(m_resume, "copy_artifact_ref_for_run", unexpected_copy)
    monkeypatch.setattr(m_common, "decode", unexpected_decode)
    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        Deployment(pipeline).resume(
            parent.run_id,
            from_=["pipeline-consumer", "edge-consumer"],
            keep=True,
        )

    message = str(exc_info.value)
    assert "save resolution source is inconsistent with its graph edge" in message
    assert "from_='producer'" in message
    assert str(tmp_path) not in message
    assert "Traceback" not in message
    assert "save_output_set_counted" not in message
    assert "load_output_set_counted" not in message
    assert calls == {"run": 0, "copy": 0, "decode": 0}
    assert {path.name for path in runs_home.iterdir()} == before_dirs
    assert all(path.read_text(encoding="utf-8") == value for path, value in before_side_effects.items())


@pytest.mark.parametrize(
    ("generation", "tamper", "rewrite_fingerprint"),
    [
        ("parent", "node", False),
        ("parent", "edge", False),
        ("parent", "coordinated", False),
        ("parent", "coordinated", True),
        ("resume-of-resume", "coordinated", False),
        ("resume-of-resume", "coordinated", True),
    ],
)
def test_phase3_resume_rejects_source_provenance_tampering_without_override_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    generation: str,
    tamper: str,
    rewrite_fingerprint: bool,
) -> None:
    runs_home = tmp_path / "runs"
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    consumer_marker = tmp_path / "consumer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(consumer_marker))
    pipeline = _phase3_mixed_save_source_pipeline()
    retained = Deployment(pipeline).run(keep=True)
    with retained:
        assert retained.value("pipeline-consumer") == 14
        assert retained.value("edge-consumer") == 7
    if generation == "resume-of-resume":
        retained = retained.resume(
            from_=["pipeline-consumer", "edge-consumer"],
            keep=True,
        )
        with retained:
            assert retained.value("edge-consumer") == 7
            assert retained.value("pipeline-consumer") == 14

    assert retained.manifest_path is not None
    manifest = cast(dict[str, Any], json.loads(retained.manifest_path.read_text(encoding="utf-8")))
    producer, producer_edges = _phase3_producer_and_edges(manifest)
    if tamper in {"node", "coordinated"}:
        producer["adapters"][DEFAULT_PORT]["source"] = "run-override"
    if tamper == "edge":
        producer_edges[0]["adapter"]["save"]["source"] = "run-override"
    elif tamper == "coordinated":
        for edge in producer_edges:
            edge["adapter"]["save"]["source"] = "run-override"
    if rewrite_fingerprint:
        producer["fingerprint"]["sha256"] = "0" * 64
    retained.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    side_effects_before = {
        path: path.read_bytes() for path in (producer_marker, save_marker, load_marker, consumer_marker)
    }
    run_dirs_before = {path.name for path in runs_home.iterdir()}
    runtime_dirs_before = {path.relative_to(runs_home) for path in runs_home.glob("*/node-runtimes/*")}
    calls = {"run": 0, "copy": 0, "decode": 0}

    def unexpected_run(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["run"] += 1
        raise AssertionError("child Run must not be created")

    def unexpected_copy(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["copy"] += 1
        raise AssertionError("artifact copy must not run")

    def unexpected_decode(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["decode"] += 1
        raise AssertionError("adapter decode must not run")

    monkeypatch.setattr(m_common, "Run", unexpected_run)
    monkeypatch.setattr(m_resume, "copy_artifact_ref_for_run", unexpected_copy)
    monkeypatch.setattr(m_common, "decode", unexpected_decode)
    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        Deployment(pipeline).resume(
            retained.run_id,
            from_=["pipeline-consumer", "edge-consumer"],
            keep=True,
        )

    message = str(exc_info.value)
    assert "source is inconsistent" in message
    assert "from_='producer'" in message
    assert "run-override" not in message
    assert str(tmp_path) not in message
    assert "Traceback" not in message
    assert "save_output_set_counted" not in message
    assert "load_output_set_counted" not in message
    assert calls == {"run": 0, "copy": 0, "decode": 0}
    assert {path.name for path in runs_home.iterdir()} == run_dirs_before
    assert {path.relative_to(runs_home) for path in runs_home.glob("*/node-runtimes/*")} == runtime_dirs_before
    assert all(path.read_bytes() == value for path, value in side_effects_before.items())


def test_phase3_full_recalculation_recovers_from_coordinated_source_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    pipeline = _phase3_mixed_save_source_pipeline()
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("pipeline-consumer") == 14
        assert parent.value("edge-consumer") == 7

    assert parent.manifest_path is not None
    manifest = cast(dict[str, Any], json.loads(parent.manifest_path.read_text(encoding="utf-8")))
    producer, producer_edges = _phase3_producer_and_edges(manifest)
    producer["adapters"][DEFAULT_PORT]["source"] = "run-override"
    for edge in producer_edges:
        edge["adapter"]["save"]["source"] = "run-override"
    parent.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    child = Deployment(pipeline).resume(parent.run_id, from_="producer", keep=True)
    with child:
        assert child.value("edge-consumer") == 7
        assert child.value("pipeline-consumer") == 14

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer", "producer"]
    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save", "save"]
    child_manifest = cast(dict[str, Any], child.manifest_snapshot)
    child_producer, child_edges = _phase3_producer_and_edges(child_manifest)
    assert child_producer["adapters"][DEFAULT_PORT]["source"] == "edge"
    assert {edge["adapter"]["save"]["source"] for edge in child_edges} == {"edge", "pipeline"}
    assert child_manifest["execution_plan"]["adapter_overrides"] == []


@pytest.mark.parametrize("tamper_scope", ["one", "all"])
def test_phase3_resume_rejects_multi_variant_coordinated_source_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper_scope: str,
) -> None:
    runs_home = tmp_path / "runs"
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    consumer_marker = tmp_path / "consumer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(consumer_marker))
    pipeline = _phase3_multi_variant_mixed_save_source_pipeline()
    retained = Deployment(pipeline).run(keep=True)
    with retained:
        assert retained.value("distinct-consumer") == [7, 5, 2]
        assert retained.value("pipeline-consumer") == 14
        assert retained.value("edge-consumer") == 7

    assert retained.manifest_path is not None
    manifest = cast(dict[str, Any], json.loads(retained.manifest_path.read_text(encoding="utf-8")))
    producer, producer_edges = _phase3_producer_and_edges(manifest)
    variants = producer["outputs"][DEFAULT_PORT]["variants"]
    shared_variant = next(
        item for item in variants if item["save"]["identity"]["key"] == make_key(set, "phase3-shared")
    )
    tampered_variant_ids = (
        {shared_variant["variant_id"]} if tamper_scope == "one" else {item["variant_id"] for item in variants}
    )
    _rewrite_variant_sources(producer, producer_edges, tampered_variant_ids)
    retained.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    side_effects_before = {
        path: path.read_bytes() for path in (producer_marker, save_marker, load_marker, consumer_marker)
    }
    run_dirs_before = {path.name for path in runs_home.iterdir()}
    calls = {"run": 0, "copy": 0, "decode": 0}

    def unexpected_run(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["run"] += 1
        raise AssertionError("child Run must not be created")

    def unexpected_copy(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["copy"] += 1
        raise AssertionError("artifact copy must not run")

    def unexpected_decode(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["decode"] += 1
        raise AssertionError("adapter decode must not run")

    monkeypatch.setattr(m_common, "Run", unexpected_run)
    monkeypatch.setattr(m_resume, "copy_artifact_ref_for_run", unexpected_copy)
    monkeypatch.setattr(m_common, "decode", unexpected_decode)
    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        Deployment(pipeline).resume(
            retained.run_id,
            from_=["pipeline-consumer", "edge-consumer", "distinct-consumer"],
            keep=True,
        )

    message = str(exc_info.value)
    assert "source is inconsistent" in message
    assert "from_='producer'" in message
    assert "run-override" not in message
    assert str(tmp_path) not in message
    assert "Traceback" not in message
    assert calls == {"run": 0, "copy": 0, "decode": 0}
    assert {path.name for path in runs_home.iterdir()} == run_dirs_before
    assert all(path.read_bytes() == value for path, value in side_effects_before.items())


@pytest.mark.parametrize("redundant", [False, True], ids=["different-identity", "redundant-identity"])
def test_phase3_genuine_run_override_evidence_survives_parent_and_disk_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    redundant: bool,
) -> None:
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    consumer_marker = tmp_path / "consumer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(consumer_marker))
    pipeline = _phase3_mixed_save_source_pipeline()
    override = (
        cast(Adapter, pipeline.adapters[make_key(set, "phase3-shared")])
        if redundant
        else _phase3_run_override_adapter()
    )
    override_map = {("producer", DEFAULT_PORT): override}
    parent = Deployment(pipeline).run(keep=True, adapters=override_map)
    with parent:
        assert parent.value("pipeline-consumer") == 14
        assert parent.value("edge-consumer") == 7

    child = parent.resume(
        from_=["pipeline-consumer", "edge-consumer"],
        adapters=override_map,
        keep=True,
    )
    with child:
        assert child.value("edge-consumer") == 7
        assert child.value("pipeline-consumer") == 14

    grandchild = Deployment(pipeline).resume(
        child.run_id,
        from_=["pipeline-consumer", "edge-consumer"],
        adapters=override_map,
        keep=True,
    )
    with grandchild:
        assert grandchild.value("pipeline-consumer") == 14
        assert grandchild.value("edge-consumer") == 7

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer"]
    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save"]
    assert len(load_marker.read_text(encoding="utf-8").splitlines()) == 6
    assert consumer_marker.read_text(encoding="utf-8").splitlines().count("left") == 3
    assert consumer_marker.read_text(encoding="utf-8").splitlines().count("right") == 3
    for retained in (parent, child, grandchild):
        assert retained.run_dir is not None
        manifest = cast(dict[str, Any], retained.manifest_snapshot)
        assert manifest["schema_version"] == 1
        producer, producer_edges = _phase3_producer_and_edges(manifest)
        assert producer["status"] in {"succeeded", "frozen"}
        assert producer["adapters"][DEFAULT_PORT]["source"] == "run-override"
        assert "variants" not in producer["outputs"][DEFAULT_PORT]
        assert {edge["adapter"]["save"]["source"] for edge in producer_edges} == {"run-override"}
        assert {edge["adapter"]["load"]["source"] for edge in producer_edges} == {"run-override"}
        assert len({edge["artifact_variant"] for edge in producer_edges}) == 1
        assert len({edge["artifact"]["ref"]["sha256"] for edge in producer_edges}) == 1
        assert len(list((retained.run_dir / "artifacts").glob("artifact-*"))) == 1
        [override_evidence] = manifest["execution_plan"]["adapter_overrides"]
        assert override_evidence == {
            "node_id": producer["id"],
            "port": DEFAULT_PORT,
            "identity": producer["adapters"][DEFAULT_PORT]["identity"],
        }
        execution_plan = json.loads((retained.run_dir / "execution-plan.json").read_text(encoding="utf-8"))
        assert execution_plan["adapter_overrides"] == [override_evidence]


@pytest.mark.parametrize(
    "tamper",
    [
        "manifest-record-removed",
        "manifest-identity-changed",
        "manifest-override-duplicated",
        "manifest-digest-changed",
        "sidecar-identity-changed",
    ],
)
def test_phase3_resume_rejects_changed_or_inconsistent_execution_plan_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    runs_home = tmp_path / "runs"
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    consumer_marker = tmp_path / "consumer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(consumer_marker))
    pipeline = _phase3_mixed_save_source_pipeline()
    override = _phase3_run_override_adapter()
    retained = Deployment(pipeline).run(
        keep=True,
        adapters={("producer", DEFAULT_PORT): override},
    )
    with retained:
        assert retained.value("pipeline-consumer") == 14
        assert retained.value("edge-consumer") == 7

    assert retained.manifest_path is not None
    assert retained.run_dir is not None
    manifest = cast(dict[str, Any], json.loads(retained.manifest_path.read_text(encoding="utf-8")))
    sidecar_path = retained.run_dir / "execution-plan.json"
    if tamper == "manifest-record-removed":
        manifest.pop("execution_plan")
    elif tamper == "manifest-identity-changed":
        manifest["execution_plan"]["adapter_overrides"][0]["identity"]["key"] = "secret-evidence"
    elif tamper == "manifest-override-duplicated":
        manifest["execution_plan"]["adapter_overrides"].append(
            deepcopy(manifest["execution_plan"]["adapter_overrides"][0])
        )
    elif tamper == "manifest-digest-changed":
        manifest["execution_plan"]["evidence"]["sha256"] = "0" * 64
    elif tamper == "sidecar-identity-changed":
        sidecar = cast(dict[str, Any], json.loads(sidecar_path.read_text(encoding="utf-8")))
        sidecar["adapter_overrides"][0]["identity"]["key"] = "secret-evidence"
        sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
    else:
        raise AssertionError("unhandled execution-plan tamper")
    retained.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    side_effects_before = {
        path: path.read_bytes() for path in (producer_marker, save_marker, load_marker, consumer_marker)
    }
    run_dirs_before = {path.name for path in runs_home.iterdir()}
    calls = {"run": 0, "copy": 0, "decode": 0}

    def unexpected_run(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["run"] += 1
        raise AssertionError("child Run must not be created")

    def unexpected_copy(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["copy"] += 1
        raise AssertionError("artifact copy must not run")

    def unexpected_decode(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        calls["decode"] += 1
        raise AssertionError("adapter decode must not run")

    monkeypatch.setattr(m_common, "Run", unexpected_run)
    monkeypatch.setattr(m_resume, "copy_artifact_ref_for_run", unexpected_copy)
    monkeypatch.setattr(m_common, "decode", unexpected_decode)
    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        Deployment(pipeline).resume(
            retained.run_id,
            from_=["pipeline-consumer", "edge-consumer"],
            keep=True,
        )

    message = str(exc_info.value)
    assert "execution-plan evidence is invalid" in message
    assert "from_='producer'" in message
    assert "secret-evidence" not in message
    assert "run-override" not in message
    assert str(tmp_path) not in message
    assert "Traceback" not in message
    assert calls == {"run": 0, "copy": 0, "decode": 0}
    assert {path.name for path in runs_home.iterdir()} == run_dirs_before
    assert all(path.read_bytes() == value for path, value in side_effects_before.items())


@pytest.mark.parametrize(
    "tamper",
    [
        "invalid-manifest-digest",
        "missing-manifest-record",
        "missing-sidecar",
        "sidecar-identity-mismatch",
        "malformed-sidecar-json",
        "duplicate-sidecar-json-key",
        "oversized-sidecar",
        "sidecar-symlink",
        "sidecar-hard-link",
        "sidecar-directory",
        "sidecar-fifo",
        "inline-sidecar-mismatch",
        "duplicate-override-entry",
        "unknown-node",
        "unknown-output-port",
        "unsupported-version",
    ],
)
def test_phase3_full_recalculation_never_accesses_invalid_historical_execution_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    runs_home = tmp_path / "runs"
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    consumer_marker = tmp_path / "consumer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(consumer_marker))
    pipeline = _phase3_mixed_save_source_pipeline()
    parent = Deployment(pipeline).run(
        keep=True,
        adapters={("producer", DEFAULT_PORT): _phase3_run_override_adapter()},
    )
    with parent:
        assert parent.value("pipeline-consumer") == 14
        assert parent.value("edge-consumer") == 7
    parent_manifest = cast(dict[str, Any], parent.manifest_snapshot)
    parent_producer, _ = _phase3_producer_and_edges(parent_manifest)
    _corrupt_execution_plan(parent, tamper)

    load_calls = 0
    copy_calls = 0
    decode_paths: list[Path] = []
    original_load_plan = m_resume.load_run_execution_plan
    original_decode = m_common.decode

    def unexpected_load_plan(**kwargs: Any) -> Any:
        del kwargs
        nonlocal load_calls
        load_calls += 1
        raise AssertionError("historical execution-plan evidence must not be accessed")

    def unexpected_copy(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        nonlocal copy_calls
        copy_calls += 1
        raise AssertionError("parent artifacts must not be copied")

    def decode_spy(ref: Any, adapter: Any) -> Any:
        decode_paths.append(Path(ref.uri))
        return original_decode(ref, adapter)

    monkeypatch.setattr(m_resume, "load_run_execution_plan", unexpected_load_plan)
    monkeypatch.setattr(m_resume, "copy_artifact_ref_for_run", unexpected_copy)
    monkeypatch.setattr(m_common, "decode", decode_spy)
    child = Deployment(pipeline).resume(parent.run_id, from_="producer", keep=True)
    with child:
        assert child.value("edge-consumer") == 7
        assert child.value("pipeline-consumer") == 14

    assert load_calls == 0
    assert copy_calls == 0
    assert parent.run_dir is not None
    assert decode_paths
    assert all(not path.is_relative_to(parent.run_dir) for path in decode_paths)
    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer", "producer"]
    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save", "save"]
    assert len(load_marker.read_text(encoding="utf-8").splitlines()) == 4
    assert consumer_marker.read_text(encoding="utf-8").splitlines().count("left") == 2
    assert consumer_marker.read_text(encoding="utf-8").splitlines().count("right") == 2

    assert child.run_dir is not None
    child_manifest = cast(dict[str, Any], child.manifest_snapshot)
    assert child_manifest["parent_run_id"] == parent.run_id
    assert all(record["status"] == "succeeded" for record in child_manifest["nodes"].values())
    assert child_manifest["execution_plan"]["adapter_overrides"] == []
    child_document = json.loads((child.run_dir / m_manifest.RUN_EXECUTION_PLAN_FILENAME).read_text(encoding="utf-8"))
    assert child_document["adapter_overrides"] == []
    assert not list(child.run_dir.glob(".execution-plan*.tmp"))
    child_producer, _ = _phase3_producer_and_edges(child_manifest)
    assert child_producer["fingerprint"] != parent_producer["fingerprint"]
    monkeypatch.setattr(m_resume, "load_run_execution_plan", original_load_plan)
    assert (
        original_load_plan(
            pipeline=pipeline,
            parent_manifest=child_manifest,
            parent_run_dir=child.run_dir,
        )
        == {}
    )


@pytest.mark.parametrize("recovery_api", ["run", "deployment"])
@pytest.mark.parametrize(
    "override_kind",
    ["none", "different-identity", "redundant-identity", "split-halves"],
)
def test_phase3_fresh_recovery_uses_only_current_overrides_and_is_resumable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recovery_api: str,
    override_kind: str,
) -> None:
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    pipeline = _phase3_mixed_save_source_pipeline()
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("pipeline-consumer") == 14
        assert parent.value("edge-consumer") == 7
    _corrupt_execution_plan(parent, "malformed-sidecar-json")

    if override_kind == "none":
        override_map = None
        expected_identity = None
    else:
        if override_kind == "different-identity":
            override = _phase3_run_override_adapter()
        elif override_kind == "redundant-identity":
            override = cast(Adapter, pipeline.adapters[make_key(set, "phase3-shared")])
        else:
            override = _Phase3SplitRuntimeOverride()
        override_map = {("producer", DEFAULT_PORT): override}
        expected_identity = override.key
    if recovery_api == "run":
        child = parent.resume(
            from_="producer",
            adapters=override_map,
            keep=True,
        )
    else:
        child = Deployment(pipeline).resume(
            parent.run_id,
            from_="producer",
            adapters=override_map,
            keep=True,
        )
    with child:
        assert child.value("pipeline-consumer") == 14
        assert child.value("edge-consumer") == 7

    child_manifest = cast(dict[str, Any], child.manifest_snapshot)
    child_producer, child_edges = _phase3_producer_and_edges(child_manifest)
    evidence = child_manifest["execution_plan"]["adapter_overrides"]
    if expected_identity is None:
        assert evidence == []
        assert {edge["adapter"]["save"]["source"] for edge in child_edges} == {"edge", "pipeline"}
    else:
        [entry] = evidence
        assert entry["identity"]["key"] == expected_identity
        assert child_producer["adapters"][DEFAULT_PORT]["source"] == "run-override"
        assert {edge["adapter"]["save"]["source"] for edge in child_edges} == {"run-override"}

    grandchild = Deployment(pipeline).resume(
        child.run_id,
        from_=["pipeline-consumer", "edge-consumer"],
        adapters=override_map,
        keep=True,
    )
    with grandchild:
        assert grandchild.value("edge-consumer") == 7
        assert grandchild.value("pipeline-consumer") == 14

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer", "producer"]
    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save", "save"]
    grandchild_manifest = cast(dict[str, Any], grandchild.manifest_snapshot)
    grandchild_producer, _ = _phase3_producer_and_edges(grandchild_manifest)
    assert grandchild_producer["status"] == "frozen"
    assert grandchild_manifest["parent_run_id"] == child.run_id


def test_phase3_full_recalculation_applies_current_kwargs_and_runtime_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer_marker = tmp_path / "producer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_seeded_output_set).alias("producer")
    pipeline = (
        lift_any(consume_output_set)
        .bind(value=producer.as_format("phase3-shared"))
        .alias("consumer")
        .render("phase3_current_recovery_overrides")
        .add_adapter(set, "phase3-shared", save=save_output_set_counted, load=load_output_set_counted)
    )
    parent = Deployment(pipeline).run(seed=2, keep=True)
    with parent:
        assert parent.value("consumer") == [2, 3]
    parent_manifest = cast(dict[str, Any], parent.manifest_snapshot)
    parent_producer, _ = _phase3_producer_and_edges(parent_manifest)
    _corrupt_execution_plan(parent, "invalid-manifest-digest")

    child = Deployment(pipeline).resume(
        parent.run_id,
        from_="producer",
        kwargs={"seed": 11},
        runtimes={"producer": "venv-subprocess"},
        keep=True,
    )
    with child:
        assert child.value("consumer") == [11, 12]

    child_manifest = cast(dict[str, Any], child.manifest_snapshot)
    child_producer, _ = _phase3_producer_and_edges(child_manifest)
    assert child_manifest["execution_plan"]["adapter_overrides"] == []
    assert child_producer["runtime"]["name"] == "venv-subprocess"
    assert child_producer["runtime"]["source"] == "run-override"
    assert child_producer["inputs"]["seed"]["value"] == 11
    assert child_producer["fingerprint"] != parent_producer["fingerprint"]
    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer", "producer"]


@pytest.mark.parametrize("graph", ["simple", "diamond"])
def test_phase3_invalid_evidence_guidance_recalculates_every_remaining_graph_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    graph: str,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    if graph == "simple":
        pipeline = _phase3_native_recovery_pipeline()
        consumers = ["consumer"]
        expected = {"consumer": 14}
    else:
        pipeline = _phase3_resume_validation_pipeline()
        consumers = ["first", "second"]
        expected = {"first": 14, "second": 7}
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert {alias: parent.value(alias) for alias in consumers} == expected
    _corrupt_execution_plan(parent, "invalid-manifest-digest")

    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        Deployment(pipeline).resume(parent.run_id, from_=consumers, keep=True)
    message = str(exc_info.value)
    assert "recalculate with from_='producer'" in message
    assert str(tmp_path) not in message
    assert "Traceback" not in message

    child = Deployment(pipeline).resume(parent.run_id, from_="producer", keep=True)
    with child:
        assert {alias: child.value(alias) for alias in consumers} == expected
    manifest = cast(dict[str, Any], child.manifest_snapshot)
    assert manifest["parent_run_id"] == parent.run_id
    assert all(record["status"] == "succeeded" for record in manifest["nodes"].values())


def test_phase3_invalid_evidence_guidance_lists_disconnected_roots_deterministically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer_b = lift_any(produce_output_set).alias("producer-b")
    consumer_b = lift_any(consume_phase3_diamond_right).bind(value=producer_b).alias("consumer-b")
    producer_a = lift_any(produce_output_set).alias("producer-a")
    consumer_a = lift_any(consume_phase3_diamond_left).bind(value=producer_a).alias("consumer-a")
    pipeline = (
        (consumer_b.render("phase3_disconnected_guidance") | consumer_a.render("phase3_disconnected_guidance"))
        .add_adapter(set, "phase3-shared", save=save_output_set, load=load_output_set)
        .with_node_runtime("producer-b", "venv-subprocess")
        .with_node_runtime("producer-a", "venv-subprocess")
    )
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("consumer-a") == 14
        assert parent.value("consumer-b") == 7
    _corrupt_execution_plan(parent, "invalid-manifest-digest")
    exact_selection = ["producer-a", "producer-b"]

    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        Deployment(pipeline).resume(
            parent.run_id,
            from_=["consumer-b", "consumer-a"],
            keep=True,
        )
    assert "recalculate with from_=['producer-a', 'producer-b']" in str(exc_info.value)

    child = Deployment(pipeline).resume(parent.run_id, from_=exact_selection, keep=True)
    with child:
        assert child.value("consumer-a") == 14
        assert child.value("consumer-b") == 7
    manifest = cast(dict[str, Any], child.manifest_snapshot)
    assert all(record["status"] == "succeeded" for record in manifest["nodes"].values())


def test_phase3_resume_of_resume_invalid_evidence_guidance_is_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    pipeline = _phase3_mixed_save_source_pipeline()
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("pipeline-consumer") == 14
        assert parent.value("edge-consumer") == 7
    retained_child = parent.resume(
        from_=["pipeline-consumer", "edge-consumer"],
        keep=True,
    )
    with retained_child:
        assert retained_child.value("pipeline-consumer") == 14
        assert retained_child.value("edge-consumer") == 7
    _corrupt_execution_plan(retained_child, "invalid-manifest-digest")

    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        Deployment(pipeline).resume(
            retained_child.run_id,
            from_=["pipeline-consumer", "edge-consumer"],
            keep=True,
        )
    assert "recalculate with from_='producer'" in str(exc_info.value)

    grandchild = Deployment(pipeline).resume(retained_child.run_id, from_="producer", keep=True)
    with grandchild:
        assert grandchild.value("pipeline-consumer") == 14
        assert grandchild.value("edge-consumer") == 7
    manifest = cast(dict[str, Any], grandchild.manifest_snapshot)
    assert manifest["parent_run_id"] == retained_child.run_id
    assert all(record["status"] == "succeeded" for record in manifest["nodes"].values())


@pytest.mark.parametrize(
    "tamper",
    [
        "missing-artifact",
        "sha-mismatch",
        "size-mismatch",
        "unsafe-uri",
        "artifact-symlink",
        "wrong-variant",
    ],
)
def test_phase3_artifact_corruption_blocks_frozen_reuse_but_not_full_recalculation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    runs_home = tmp_path / "runs"
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    consumer_marker = tmp_path / "consumer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(consumer_marker))
    pipeline = _phase3_mixed_save_source_pipeline()
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("pipeline-consumer") == 14
        assert parent.value("edge-consumer") == 7

    assert parent.manifest_path is not None
    assert parent.run_dir is not None
    manifest = cast(dict[str, Any], json.loads(parent.manifest_path.read_text(encoding="utf-8")))
    producer, producer_edges = _phase3_producer_and_edges(manifest)
    output = producer["outputs"][DEFAULT_PORT]
    artifact_path = parent.run_dir / output["ref"]["uri"]
    if tamper == "missing-artifact":
        artifact_path.unlink()
    elif tamper == "sha-mismatch":
        artifact_path.write_bytes(b"9,9,9")
    elif tamper == "size-mismatch":
        artifact_path.write_bytes(b"9")
    elif tamper == "unsafe-uri":
        output["ref"]["uri"] = "../unsafe-artifact"
    elif tamper == "artifact-symlink":
        external = tmp_path / "external-artifact"
        external.write_bytes(artifact_path.read_bytes())
        artifact_path.unlink()
        artifact_path.symlink_to(external)
    elif tamper == "wrong-variant":
        producer_edges[0]["artifact_variant"] = "0" * 64
    else:
        raise AssertionError("unhandled artifact corruption")
    parent.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    side_effects_before = {
        path: path.read_bytes() for path in (producer_marker, save_marker, load_marker, consumer_marker)
    }
    run_dirs_before = {path.name for path in runs_home.iterdir()}
    run_calls = 0
    copy_calls = 0
    original_run = m_common.Run

    def unexpected_run(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        nonlocal run_calls
        run_calls += 1
        raise AssertionError("child Run must not be created for invalid frozen state")

    def unexpected_copy(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        nonlocal copy_calls
        copy_calls += 1
        raise AssertionError("parent artifacts must not be copied")

    monkeypatch.setattr(m_common, "Run", unexpected_run)
    monkeypatch.setattr(m_resume, "copy_artifact_ref_for_run", unexpected_copy)
    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        Deployment(pipeline).resume(
            parent.run_id,
            from_=["pipeline-consumer", "edge-consumer"],
            keep=True,
        )
    message = str(exc_info.value)
    assert "from_='producer'" in message
    assert str(tmp_path) not in message
    assert "Traceback" not in message
    assert run_calls == 0
    assert copy_calls == 0
    assert {path.name for path in runs_home.iterdir()} == run_dirs_before
    assert all(path.read_bytes() == value for path, value in side_effects_before.items())

    monkeypatch.setattr(m_common, "Run", original_run)
    original_decode = m_common.decode
    decode_paths: list[Path] = []

    def decode_spy(ref: Any, adapter: Any) -> Any:
        decode_paths.append(Path(ref.uri))
        return original_decode(ref, adapter)

    monkeypatch.setattr(m_common, "decode", decode_spy)
    child = Deployment(pipeline).resume(parent.run_id, from_="producer", keep=True)
    with child:
        assert child.value("edge-consumer") == 7
        assert child.value("pipeline-consumer") == 14

    assert copy_calls == 0
    assert decode_paths
    assert all(not path.is_relative_to(parent.run_dir) for path in decode_paths)
    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer", "producer"]
    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save", "save"]
    child_manifest = cast(dict[str, Any], child.manifest_snapshot)
    child_producer, _ = _phase3_producer_and_edges(child_manifest)
    child_uri = child_producer["outputs"][DEFAULT_PORT]["ref"]["uri"]
    assert not Path(child_uri).is_absolute()
    assert child.run_dir is not None
    assert (child.run_dir / child_uri).is_file()


@pytest.mark.parametrize("child_outcome", ["success", "failure"])
def test_phase3_full_recalculation_obeys_on_failure_retention_without_stale_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    child_outcome: str,
) -> None:
    runs_home = tmp_path / "runs"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set_counted).alias("producer")
    pipeline = (
        lift_any(consume_output_set_maybe_fail)
        .bind(value=producer.as_format("phase3-shared"))
        .alias("consumer")
        .render("phase3_recovery_retention")
        .add_adapter(set, "phase3-shared", save=save_output_set_counted, load=load_output_set_counted)
        .with_node_runtime("producer", "venv-subprocess")
    )
    parent = Deployment(pipeline).run(should_fail=False, keep=True)
    with parent:
        assert parent.value("consumer") == [2, 5, 7]
    _corrupt_execution_plan(parent, "invalid-manifest-digest")

    should_fail = child_outcome == "failure"
    child = Deployment(pipeline).resume(
        parent.run_id,
        from_="producer",
        kwargs={"should_fail": should_fail},
        keep="on_failure",
    )
    if should_fail:
        with pytest.raises(RuntimeError, match="intentional recovery child failure"):
            with child:
                child.value("consumer")
    else:
        with child:
            assert child.value("consumer") == [2, 5, 7]

    snapshot = cast(dict[str, Any], child.manifest_snapshot)
    assert snapshot["parent_run_id"] == parent.run_id
    assert snapshot["execution_plan"]["adapter_overrides"] == []
    assert child.run_dir is not None
    if should_fail:
        assert snapshot["status"] == "failed"
        assert child.run_dir.is_dir()
        assert child.manifest_path is not None
        document = json.loads((child.run_dir / m_manifest.RUN_EXECUTION_PLAN_FILENAME).read_text(encoding="utf-8"))
        assert document["adapter_overrides"] == []
        assert (
            m_resume.load_run_execution_plan(
                pipeline=pipeline,
                parent_manifest=snapshot,
                parent_run_dir=child.run_dir,
            )
            == {}
        )
        producer_record, _ = _phase3_producer_and_edges(snapshot)
        assert not Path(producer_record["outputs"][DEFAULT_PORT]["ref"]["uri"]).is_absolute()
    else:
        assert snapshot["status"] == "succeeded"
        assert not child.run_dir.exists()
        assert child.manifest_path is None
        assert {path.name for path in runs_home.iterdir()} == {parent.run_id}
    assert not list(runs_home.rglob(".execution-plan*.tmp"))


def test_phase3_native_and_isolated_consumers_share_artifact_without_extra_conductor_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    save_marker = tmp_path / "save-calls.txt"
    load_marker = tmp_path / "load-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    monkeypatch.setenv("SPL_PHASE3_LOAD_CALLS", str(load_marker))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    native = lift_any(consume_output_set).bind(value=producer.as_format("phase3-set")).alias("native")
    isolated = lift_any(consume_output_set_isolated).bind(value=producer.as_format("phase3-set")).alias("isolated")
    pipeline = (
        (native.render("phase3_mixed_shared") | isolated.render("phase3_mixed_shared"))
        .add_adapter(set, "phase3-set", save=save_output_set_counted, load=load_output_set_counted)
        .with_node_runtime("producer", "venv-subprocess")
        .with_node_runtime("isolated", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("isolated")["values"] == [2, 5, 7]
        assert run.value("native") == [2, 5, 7]

    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save"]
    load_pids = load_marker.read_text(encoding="utf-8").splitlines()
    assert load_pids.count(str(os.getpid())) == 1
    assert len(load_pids) == 2


def test_phase3_different_formats_save_once_each_and_map_exact_edges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = tmp_path / "save-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(marker))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set).bind(value=producer.as_format("phase3-a")).alias("first")
    second = lift_any(consume_output_set_alt).bind(value=producer.as_format("phase3-b")).alias("second")
    pipeline = (
        (first.render("phase3_distinct_outputs") | second.render("phase3_distinct_outputs"))
        .add_adapter(set, "phase3-a", save=save_output_set_counted, load=load_output_set)
        .add_adapter(set, "phase3-b", save=save_output_set_counted, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("first") == [2, 5, 7]
        assert run.value("second") == [7, 5, 2]

    assert marker.read_text(encoding="utf-8").splitlines() == ["save", "save"]
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    variants = producer_record["outputs"][DEFAULT_PORT]["variants"]
    variant_refs = {item["variant_id"]: item["artifact"]["ref"] for item in variants}
    producer_edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer_record["id"]]
    assert len(producer_edges) == 2
    for edge in producer_edges:
        assert edge["artifact"]["ref"] == variant_refs[edge["artifact_variant"]]
        assert edge["adapter"]["save"]["identity"]["key"] == edge["artifact"]["ref"]["key"]
    assert run.run_dir is not None
    runtime_dir = run.run_dir / "node-runtimes" / producer_record["id"]
    invocation = json.loads((runtime_dir / "input.json").read_text(encoding="utf-8"))
    assert invocation["schema_version"] == m_runtime_port_adapters.ISOLATED_NODE_MULTI_OUTPUT_TRANSPORT_SCHEMA_VERSION
    output_bindings = [
        item for item in invocation["runtime_port_adapters"]["bindings"] if item["direction"] == "output"
    ]
    assert len(output_bindings) == 2
    assert len({item["variant_id"] for item in output_bindings}) == 2
    malformed = deepcopy(invocation["runtime_port_adapters"])
    next(item for item in malformed["bindings"] if item["direction"] == "output")["unknown"] = True
    with pytest.raises(
        m_runtime_port_adapters.RuntimePortAdapterContractError,
        match=r"^node_adapter_output_validation:",
    ):
        m_runtime_port_adapters.normalize_isolated_node_multi_output_transport(malformed)
    result_envelope = json.loads((runtime_dir / "result.json").read_text(encoding="utf-8"))
    assert set(result_envelope) == {"result", "artifacts", "runtime_port_adapter_outputs"}
    assert len(result_envelope["runtime_port_adapter_outputs"]) == 2


def test_phase3_multi_variant_split_save_and_load_halves_are_independent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set).bind(value=producer.as_format("phase3-a")).alias("first")
    second = lift_any(consume_output_set_isolated).bind(value=producer.as_format("phase3-b")).alias("second")
    pipeline = (
        (first.render("phase3_split_halves") | second.render("phase3_split_halves"))
        .with_node_runtime("producer", "venv-subprocess")
        .with_node_runtime("second", "venv-subprocess")
    )
    save_a = SplitSaveAdapter(
        key=make_key(set, "phase3-a"),
        save=save_output_set,
        tag="phase3-a",
    )
    save_b = SplitSaveAdapter(
        key=make_key(set, "phase3-b"),
        save=save_output_set_alt,
        tag="phase3-b",
    )
    load_a = SplitLoadAdapter(
        key=make_key(set, "phase3-a"),
        load=load_output_set,
        accepted_tags_value=("phase3-a",),
    )
    load_b = SplitLoadAdapter(
        key=make_key(set, "phase3-b"),
        load=load_output_set,
        accepted_tags_value=("phase3-b",),
    )
    pipeline = replace(
        pipeline,
        save_adapters={save_a.key: save_a, save_b.key: save_b},
        load_adapters={load_a.key: load_a, load_b.key: load_b},
    )._validate_consistency()
    run = Deployment(pipeline).run(keep=True)

    with run:
        assert run.value("first") == [2, 5, 7]
        assert run.value("second")["values"] == [2, 5, 7]

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    producer_edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer_record["id"]]
    assert len(producer_edges) == 2
    assert all(edge["adapter"]["save"]["identity"]["load"] is None for edge in producer_edges)
    assert all(edge["adapter"]["load"]["identity"]["save"] is None for edge in producer_edges)


def test_phase3_consumer_access_order_preserves_variant_and_manifest_facts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set).bind(value=producer.as_format("phase3-a")).alias("first")
    second = lift_any(consume_output_set_alt).bind(value=producer.as_format("phase3-b")).alias("second")
    pipeline = (
        (first.render("phase3_order") | second.render("phase3_order"))
        .add_adapter(set, "phase3-a", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase3-b", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    observed: list[dict[str, Any]] = []
    for index, order in enumerate((("first", "second"), ("second", "first"))):
        monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs-{}".format(index)))
        run = Deployment(pipeline).run(keep=True)
        with run:
            results = {alias: run.value(alias) for alias in order}
        manifest = cast(dict[str, Any], run.manifest_snapshot)
        nodes = {item["id"]: item for item in manifest["nodes"].values()}
        producer_record = next(item for item in nodes.values() if item["alias"] == "producer")
        output = producer_record["outputs"][DEFAULT_PORT]
        observed.append(
            {
                "results": results,
                "variants": sorted(
                    (item["artifact"]["ref"]["key"], item["artifact"]["ref"]["sha256"]) for item in output["variants"]
                ),
                "edges": sorted(
                    (
                        nodes[edge["target"]["node_id"]]["alias"],
                        edge["artifact"]["ref"]["key"],
                        edge["artifact"]["ref"]["sha256"],
                        edge["adapter"]["save"]["source"],
                        edge["adapter"]["load"]["source"],
                    )
                    for edge in manifest["edges"]
                    if edge["source"]["node_id"] == producer_record["id"]
                ),
                "fingerprint": producer_record["fingerprint"],
            }
        )

    assert observed[0] == observed[1]


def test_phase3_multi_variant_resume_and_resume_of_resume_keep_producer_frozen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer_marker = tmp_path / "producer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set_counted).alias("producer")
    native = lift_any(consume_output_set).bind(value=producer.as_format("phase3-a")).alias("native")
    isolated = lift_any(consume_output_set_isolated).bind(value=producer.as_format("phase3-b")).alias("isolated")
    pipeline = (
        (native.render("phase3_resume") | isolated.render("phase3_resume"))
        .add_adapter(set, "phase3-a", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase3-b", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
        .with_node_runtime("isolated", "venv-subprocess")
    )
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("native") == [2, 5, 7]
        assert parent.value("isolated")["values"] == [2, 5, 7]

    child = parent.resume(from_=["native", "isolated"], keep=True)
    with child:
        assert child.value("isolated")["values"] == [2, 5, 7]
        assert child.value("native") == [2, 5, 7]

    grandchild = child.resume(from_=["native", "isolated"], keep=True)
    with grandchild:
        assert grandchild.value("native") == [2, 5, 7]
        assert grandchild.value("isolated")["values"] == [2, 5, 7]

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer"]
    for run in (child, grandchild):
        manifest = cast(dict[str, Any], run.manifest_snapshot)
        producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
        assert producer_record["status"] == "frozen"
        variants = producer_record["outputs"][DEFAULT_PORT]["variants"]
        assert all(not Path(item["artifact"]["ref"]["uri"]).is_absolute() for item in variants)
        assert run.run_dir is not None
        assert all((run.run_dir / item["artifact"]["ref"]["uri"]).is_file() for item in variants)
        producer_edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer_record["id"]]
        assert {edge["artifact"]["ref"]["key"] for edge in producer_edges} == {
            make_key(set, "phase3-a"),
            make_key(set, "phase3-b"),
        }


def test_phase3_resume_mixed_frozen_and_recalculated_diamond(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer_marker = tmp_path / "producer-calls.txt"
    diamond_marker = tmp_path / "diamond-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_DIAMOND_CALLS", str(diamond_marker))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set_counted).alias("producer")
    left = lift_any(consume_phase3_diamond_left).bind(value=producer.as_format("phase3-a")).alias("left")
    right = lift_any(consume_phase3_diamond_right).bind(value=producer.as_format("phase3-b")).alias("right")
    pipeline = (
        lift_any(join_phase3_diamond)
        .bind(left=left, right=right)
        .alias("join")
        .render("phase3_resume_diamond")
        .add_adapter(set, "phase3-a", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase3-b", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("join") == 21

    child = parent.resume(from_="left", keep=True)
    with child:
        assert child.value("join") == 21

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer"]
    diamond_calls = diamond_marker.read_text(encoding="utf-8").splitlines()
    assert diamond_calls.count("left") == 2
    assert diamond_calls.count("right") == 1
    assert diamond_calls.count("join") == 2

    manifest = cast(dict[str, Any], child.manifest_snapshot)
    nodes = {item["alias"]: item for item in manifest["nodes"].values()}
    assert nodes["producer"]["status"] == "frozen"
    assert nodes["right"]["status"] == "frozen"
    assert nodes["left"]["status"] == "succeeded"
    assert nodes["join"]["status"] == "succeeded"
    producer_edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == nodes["producer"]["id"]]
    assert {edge["artifact"]["ref"]["key"] for edge in producer_edges} == {make_key(set, "phase3-a")}
    assert {item["artifact"]["ref"]["key"] for item in nodes["producer"]["outputs"][DEFAULT_PORT]["variants"]} == {
        make_key(set, "phase3-a"),
        make_key(set, "phase3-b"),
    }


def test_phase3_resume_rejects_missing_exact_variant_with_recalculation_guidance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set).bind(value=producer.as_format("phase3-a")).alias("first")
    second = lift_any(consume_output_set_alt).bind(value=producer.as_format("phase3-b")).alias("second")
    pipeline = (
        (first.render("phase3_missing_variant") | second.render("phase3_missing_variant"))
        .add_adapter(set, "phase3-a", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase3-b", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        assert parent.value("first") == [2, 5, 7]
        assert parent.value("second") == [7, 5, 2]

    assert parent._manifest_writer is not None
    manifest = parent._manifest_writer.data
    original_manifest = deepcopy(manifest)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    output = producer_record["outputs"][DEFAULT_PORT]
    output["variants"] = [
        item for item in output["variants"] if item["artifact"]["ref"]["key"] != make_key(set, "phase3-b")
    ]
    second_id = next(item["id"] for item in manifest["nodes"].values() if item["alias"] == "second")
    manifest["edges"] = [edge for edge in manifest["edges"] if edge["target"]["node_id"] != second_id]

    with pytest.raises(RuntimeError, match=r"from_='producer'"):
        parent.resume(from_="second", keep=True)

    parent._manifest_writer.data = original_manifest
    restored = parent._manifest_writer.data
    second_id = next(item["id"] for item in restored["nodes"].values() if item["alias"] == "second")
    producer_record = next(item for item in restored["nodes"].values() if item["alias"] == "producer")
    wrong_variant_id = next(
        item["variant_id"]
        for item in producer_record["outputs"][DEFAULT_PORT]["variants"]
        if item["artifact"]["ref"]["key"] == make_key(set, "phase3-a")
    )
    second_edge = next(edge for edge in restored["edges"] if edge["target"]["node_id"] == second_id)
    second_edge["artifact_variant"] = wrong_variant_id
    with pytest.raises(RuntimeError, match=r"referenced node variant.*from_='producer'"):
        parent.resume(from_="second", keep=True)


def test_phase3_resume_rejects_corrupted_retained_variant_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set).bind(value=producer.as_format("phase3-a")).alias("first")
    second = lift_any(consume_output_set_alt).bind(value=producer.as_format("phase3-b")).alias("second")
    pipeline = (
        (first.render("phase3_corrupt_variant") | second.render("phase3_corrupt_variant"))
        .add_adapter(set, "phase3-a", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase3-b", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    parent = Deployment(pipeline).run(keep=True)
    with parent:
        parent.value("first")
        parent.value("second")

    manifest = cast(dict[str, Any], parent.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    variant = producer_record["outputs"][DEFAULT_PORT]["variants"][1]
    assert parent.run_dir is not None
    artifact_path = parent.run_dir / variant["artifact"]["ref"]["uri"]
    artifact_path.write_bytes(b"corrupted")

    with pytest.raises(RuntimeError, match=r"sha256 mismatch.*from_='producer'"):
        parent.resume(from_="second", keep=True)


def test_custom_output_save_dependency_mismatch_fails_before_node_workdir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_ndarray).alias("producer")
    pipeline = (
        lift_any(consume_output_ndarray)
        .bind(value=producer.as_format("npy"))
        .alias("consumer")
        .render("missing_output_dependency")
        .add_adapter(np.ndarray, "npy", save=save_output_ndarray, load=load_output_ndarray)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(RuntimeError, match=r"environment_preflight.*numpy.*supported adapter"):
        run.value("consumer")

    assert run.run_dir is not None
    producer_record = next(
        item for item in cast(dict[str, Any], run.manifest_snapshot)["nodes"].values() if item["alias"] == "producer"
    )
    assert not (run.run_dir / "node-runtimes" / producer_record["id"]).exists()


def test_isolated_output_save_failure_is_staged_and_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(produce_output_set)
        .alias("producer")
        .render("failed_isolated_artifact")
        .add_adapter(set, "phase2-set", save=save_output_set_failure, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(RuntimeError) as exc_info:
        run.value("producer")

    assert "node_adapter_save" in str(exc_info.value)
    assert "secret output save detail" not in str(exc_info.value)


def test_isolated_output_save_system_exit_is_staged_and_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(produce_output_set)
        .alias("producer")
        .render("isolated_output_system_exit")
        .add_adapter(set, "phase2-set", save=save_output_set_system_exit, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(RuntimeError) as exc_info:
        run.value("producer")

    message = str(exc_info.value)
    assert "node_adapter_save" in message
    assert "adapter_save_failed" in message
    assert "SystemExit" not in message
    assert "secret output" not in message
    assert "Traceback" not in message


@pytest.mark.parametrize(
    "save",
    [save_output_set_missing, save_output_set_symlink, save_output_set_extra_file],
)
def test_isolated_output_rejects_missing_or_non_regular_save_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    save: Any,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(produce_output_set)
        .alias("producer")
        .render("invalid_isolated_artifact")
        .add_adapter(set, "phase2-set", save=save, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(RuntimeError, match="node_adapter_output_validation"):
        run.value("producer")


def test_timeout_during_isolated_output_save_terminates_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = (
        lift_any(produce_output_set)
        .alias("producer")
        .render("slow_isolated_artifact")
        .add_adapter(set, "phase2-set", save=save_output_set_slow, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline, runtime_config={"node_timeout_seconds": 0.4}).run(keep=True)

    with pytest.raises(RuntimeError, match=r"timed out after 0.4s"):
        run.value("producer")

    assert run.run_dir is not None
    producer = next(
        item for item in cast(dict[str, Any], run.manifest_snapshot)["nodes"].values() if item["alias"] == "producer"
    )
    runtime_dir = run.run_dir / "node-runtimes" / producer["id"]
    assert not (runtime_dir / "runtime-output-tmp").exists()


def test_docker_artifact_output_command_uses_closed_plan_and_promotes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = Adapter(
        key=make_key(set, "phase2-set"),
        save=save_output_set,
        load=load_output_set,
        py_type=set,
        format="phase2-set",
    )
    node = NodeFunction(cast(Any, produce_output_set))
    context = m_node_runtime.NodeRuntimeContext(
        node=node,
        node_label="producer",
        inputs={},
        output_port=node.get_output_port(DEFAULT_PORT),
        callback=lambda _node, _inputs: {DEFAULT_PORT: {2, 5, 7}},
        work_dir=tmp_path / "docker-producer",
        environment_provider=m_node_runtime.CurrentPythonEnvironmentProvider(),
        runtime_config={"docker": {"image": "python:3.13-slim", "network": "none"}},
        environment_spec=[],
        output_plan=m_node_runtime.ArtifactOutputPlan(
            save_adapter=adapter,
            resolution_source="edge",
            artifacts_dir=tmp_path / "run-artifacts",
        ),
    )
    prepared = m_node_runtime.prepare_isolated_node_inputs(context, runtime_name="docker")
    assert prepared.isolated_inputs is not None
    [binding] = prepared.isolated_inputs.document["bindings"]
    adapter_descriptor = binding["adapter"]
    commands: list[list[str]] = []
    environments: list[dict[str, str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        environments.append(kwargs["env"])
        output_dir = prepared.work_dir / "runtime-outputs"
        output_dir.mkdir(mode=0o700)
        body = b"2,5,7"
        (output_dir / "output-test.bin").write_bytes(body)
        result = {
            "result": None,
            "artifacts": {},
            "runtime_port_adapter_output": {
                "schema_version": 1,
                "transport": "artifact",
                "port": DEFAULT_PORT,
                "relative_path": "runtime-outputs/output-test.bin",
                "size": len(body),
                "sha256": m_node_runtime.hashlib.sha256(body).hexdigest(),
                "adapter_id": adapter_descriptor["id"],
                "adapter_key": adapter_descriptor["key"],
                "format_tag": adapter_descriptor["format_tag"],
            },
        }
        (prepared.work_dir / "result.json").write_text(json.dumps(result), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(m_node_runtime, "run_process_tree", fake_run)
    runtime = m_node_runtime.DockerNodeRuntime()
    result = runtime.execute(prepared, runtime.prepare(prepared))

    assert isinstance(result[DEFAULT_PORT], m_node_runtime.ArtifactOutput)
    assert Path(result[DEFAULT_PORT].ref.uri).read_bytes() == b"2,5,7"
    [command] = commands
    assert command[command.index("--network") + 1] == "none"
    assert "--read-only" in command
    assert [environment for environment in environments if "PYTHONPATH" in environment] == []
    payload = json.loads((prepared.work_dir / "input.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == 3
    assert str(tmp_path) not in json.dumps(payload)


def _promotion_case(
    tmp_path: Path,
) -> tuple[
    m_node_runtime.NodeRuntimeContext,
    m_node_runtime._SplFreeInvocation,
    dict[str, Any],
    Path,
]:
    adapter = Adapter(
        key=make_key(set, "phase2-set"),
        save=save_output_set,
        load=load_output_set,
        py_type=set,
        format="phase2-set",
    )
    node = NodeFunction(cast(Any, produce_output_set))
    work_dir = tmp_path / "node-runtime"
    context = m_node_runtime.NodeRuntimeContext(
        node=node,
        node_label="producer",
        inputs={},
        output_port=node.get_output_port(DEFAULT_PORT),
        callback=lambda _node, _inputs: {DEFAULT_PORT: {2, 5, 7}},
        work_dir=work_dir,
        environment_provider=m_node_runtime.CurrentPythonEnvironmentProvider(),
        runtime_config={},
        environment_spec=[],
        output_plan=m_node_runtime.ArtifactOutputPlan(
            save_adapter=adapter,
            resolution_source="edge",
            artifacts_dir=tmp_path / "run-artifacts",
        ),
    )
    prepared = m_node_runtime.prepare_isolated_node_inputs(context, runtime_name="venv-subprocess")
    assert prepared.isolated_inputs is not None
    [binding] = prepared.isolated_inputs.document["bindings"]
    adapter_descriptor = binding["adapter"]
    output_dir = work_dir / "runtime-outputs"
    output_dir.mkdir(mode=0o700, parents=True)
    output_path = output_dir / "output-test.bin"
    body = b"2,5,7"
    output_path.write_bytes(body)
    digest = m_node_runtime.hashlib.sha256(body).hexdigest()
    payload = {
        "result": None,
        "artifacts": {},
        "runtime_port_adapter_output": {
            "schema_version": 1,
            "transport": "artifact",
            "port": DEFAULT_PORT,
            "relative_path": "runtime-outputs/output-test.bin",
            "size": len(body),
            "sha256": digest,
            "adapter_id": adapter_descriptor["id"],
            "adapter_key": adapter_descriptor["key"],
            "format_tag": adapter_descriptor["format_tag"],
        },
    }
    invocation = m_node_runtime._SplFreeInvocation(
        work_dir=work_dir,
        artifacts_dir=work_dir / "artifacts",
        input_path=work_dir / "input.json",
        result_path=work_dir / "result.json",
        env_spec_path=work_dir / "env-spec.json",
        stdout_path=work_dir / "stdout.txt",
        stderr_path=work_dir / "stderr.txt",
        runner_path=work_dir / "spl_free_runner.py",
        module_path=work_dir / "node_module.py",
        module_name="_test_node",
    )
    return prepared, invocation, payload, output_path


def _malformed_output_transport(document: dict[str, Any], case: str) -> dict[str, Any]:
    tampered = deepcopy(document)
    [binding] = [item for item in tampered["bindings"] if item["direction"] == "output"]
    descriptor = binding["adapter"]
    if case == "unknown-binding-key":
        binding["/private/host/SECRET_OUTPUT_ADAPTER_SOURCE"] = True
    elif case == "invalid-field-type":
        binding["external_name"] = 7
    elif case == "unsafe-port":
        binding["port"] = "/private/host/output"
    elif case == "unsafe-tag":
        descriptor["format_tag"] = "/private/host/tag"
    elif case == "malformed-presentation":
        descriptor["presentation"] = {
            "media_type": "/private/host/SECRET_OUTPUT_ADAPTER_SOURCE",
            "preferred_extension": None,
        }
    elif case == "invalid-distribution":
        descriptor["distributions"] = [{"package": "/private/host/SECRET_OUTPUT_ADAPTER_SOURCE", "version": "1"}]
    elif case == "duplicate-distribution":
        descriptor["distributions"] = [
            {"package": "Example_Package", "version": "1"},
            {"package": "example-package", "version": "1"},
        ]
    else:
        raise AssertionError(case)
    return tampered


@pytest.mark.parametrize(
    "case",
    [
        "unknown-binding-key",
        "invalid-field-type",
        "unsafe-port",
        "unsafe-tag",
        "malformed-presentation",
        "invalid-distribution",
        "duplicate-distribution",
    ],
)
def test_schema_v3_output_binding_failures_are_staged_and_redacted(tmp_path: Path, case: str) -> None:
    context, _, _, _ = _promotion_case(tmp_path)
    assert context.isolated_inputs is not None

    for validator in (
        m_runtime_port_adapters.normalize_isolated_node_transport,
        m_spl_free_runner._normalize_isolated_transport_document,
    ):
        with pytest.raises(
            (
                m_runtime_port_adapters.RuntimePortAdapterContractError,
                m_spl_free_runner.NodeStageError,
            )
        ) as exc_info:
            validator(_malformed_output_transport(context.isolated_inputs.document, case))

        message = str(exc_info.value)
        assert message.startswith("node_adapter_output_validation:")
        assert "SECRET_OUTPUT_ADAPTER_SOURCE" not in message
        assert "/private/host" not in message
        assert "Traceback" not in message


@pytest.mark.parametrize("duplicate_location", ["envelope", "descriptor"])
def test_raw_duplicate_output_result_keys_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    duplicate_location: str,
) -> None:
    context, invocation, payload, _ = _promotion_case(tmp_path)
    descriptor_text = json.dumps(
        payload["runtime_port_adapter_output"],
        separators=(",", ":"),
    )
    if duplicate_location == "envelope":
        result_text = (
            '{"result":null,"result":null,"artifacts":{},"runtime_port_adapter_output":' + descriptor_text + "}"
        )
    else:
        size_field = '"size":{}'.format(payload["runtime_port_adapter_output"]["size"])
        descriptor_text = descriptor_text.replace(size_field, size_field + "," + size_field, 1)
        result_text = '{"result":null,"artifacts":{},"runtime_port_adapter_output":' + descriptor_text + "}"

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        invocation.result_path.write_text(result_text, encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(m_node_runtime, "run_process_tree", fake_run)
    with pytest.raises(RuntimeError, match="node_adapter_output_validation") as exc_info:
        m_node_runtime._run_spl_free_invocation(
            context,
            invocation,
            ["fake-runtime"],
            runtime_name="venv-subprocess",
            failure_target="producer",
            timeout_cleanup=None,
        )

    assert "duplicate field" in str(exc_info.value)
    assert str(tmp_path) not in str(exc_info.value)
    assert "Traceback" not in str(exc_info.value)


def test_conductor_promotes_and_reverifies_closed_artifact_result(tmp_path: Path) -> None:
    context, invocation, payload, _ = _promotion_case(tmp_path)

    output = m_node_runtime._promote_isolated_node_output(
        context,
        invocation,
        payload,
        runtime_name="venv-subprocess",
        failure_target="producer",
    )

    assert output.ref.key == make_key(set, "phase2-set")
    assert Path(output.ref.uri).read_bytes() == b"2,5,7"
    assert Path(output.ref.uri).stat().st_mode & 0o077 == 0


@pytest.mark.parametrize(
    ("case", "field", "value"),
    [
        ("version", "schema_version", 999),
        ("transport", "transport", "inline_json"),
        ("traversal", "relative_path", "runtime-outputs/../node_module.py"),
        ("absolute", "relative_path", "/tmp/output.bin"),
        ("forward-separator", "relative_path", "runtime-outputs/nested/output.bin"),
        ("backslash-separator", "relative_path", r"runtime-outputs\output-test.bin"),
        ("unsafe-unicode", "relative_path", "runtime-outputs/output-∕.bin"),
        ("staged-input", "relative_path", "runtime-inputs/input.bin"),
        ("bundle", "relative_path", "runtime-inputs/custom-adapters.py"),
        ("runner", "relative_path", "spl_free_runner.py"),
        ("module", "relative_path", "node_module.py"),
        ("result", "relative_path", "result.json"),
        ("arbitrary", "relative_path", "arbitrary.bin"),
        ("size", "size", 999),
        ("checksum", "sha256", "0" * 64),
        ("port", "port", "undeclared"),
        ("adapter-id", "adapter_id", "custom:" + "0" * 64),
        ("adapter-key", "adapter_key", "builtins.set@wrong"),
        ("tag", "format_tag", "wrong"),
    ],
)
def test_conductor_rejects_artifact_descriptor_tampering(
    tmp_path: Path,
    case: str,
    field: str,
    value: Any,
) -> None:
    del case
    context, invocation, payload, _ = _promotion_case(tmp_path)
    tampered = deepcopy(payload)
    tampered["runtime_port_adapter_output"][field] = value

    with pytest.raises(RuntimeError, match="node_adapter_output_validation"):
        m_node_runtime._promote_isolated_node_output(
            context,
            invocation,
            tampered,
            runtime_name="venv-subprocess",
            failure_target="producer",
        )


@pytest.mark.parametrize(
    "case",
    ["top-extra", "descriptor-extra", "descriptor-missing", "inline-result", "undeclared-artifact"],
)
def test_conductor_rejects_unknown_or_duplicate_result_fields(tmp_path: Path, case: str) -> None:
    context, invocation, payload, _ = _promotion_case(tmp_path)
    tampered = deepcopy(payload)
    if case == "top-extra":
        tampered["extra"] = True
    elif case == "descriptor-extra":
        tampered["runtime_port_adapter_output"]["extra"] = True
    elif case == "descriptor-missing":
        del tampered["runtime_port_adapter_output"]["sha256"]
    elif case == "inline-result":
        tampered["result"] = {"unexpected": True}
    else:
        tampered["artifacts"] = {"extra": "runtime-outputs/output-test.bin"}

    with pytest.raises(RuntimeError, match="node_adapter_output_validation"):
        m_node_runtime._promote_isolated_node_output(
            context,
            invocation,
            tampered,
            runtime_name="venv-subprocess",
            failure_target="producer",
        )


@pytest.mark.parametrize("kind", ["missing", "symlink", "hardlink", "directory", "fifo", "socket"])
def test_conductor_rejects_missing_or_non_regular_artifact_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    context, invocation, payload, output_path = _promotion_case(tmp_path)
    output_path.unlink()
    if kind == "symlink":
        outside = tmp_path / "outside.bin"
        outside.write_bytes(b"2,5,7")
        output_path.symlink_to(outside)
    elif kind == "hardlink":
        outside = tmp_path / "outside.bin"
        outside.write_bytes(b"2,5,7")
        os.link(outside, output_path)
    elif kind == "directory":
        output_path.mkdir()
    elif kind == "fifo":
        if not hasattr(os, "mkfifo"):
            pytest.skip("FIFO creation is unavailable")
        os.mkfifo(output_path)
    elif kind == "socket":
        socket_stat = os.stat_result((stat.S_IFSOCK | 0o600, 0, 0, 1, 0, 0, 0, 0, 0, 0))
        real_lstat = Path.lstat
        monkeypatch.setattr(Path, "lstat", lambda path: socket_stat if path == output_path else real_lstat(path))

    with pytest.raises(RuntimeError, match="node_adapter_output_validation"):
        m_node_runtime._promote_isolated_node_output(
            context,
            invocation,
            payload,
            runtime_name="venv-subprocess",
            failure_target="producer",
        )


def test_conductor_and_runner_reject_device_output() -> None:
    device = Path("/dev/null")
    if not device.exists():
        pytest.skip("the platform has no device fixture")
    with pytest.raises(RuntimeError):
        m_spl_free_runner._read_bounded_runtime_output(device)
    with pytest.raises(RuntimeError, match="node_adapter_input_validation"):
        m_node_runtime._read_bounded_regular_file(device)


def test_conductor_rejects_multi_output_aggregate_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setattr(m_runtime_port_adapters, "MAX_RUNTIME_OUTPUT_TOTAL_BYTES", 8)
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set).bind(value=producer.as_format("phase3-a")).alias("first")
    second = lift_any(consume_output_set_alt).bind(value=producer.as_format("phase3-b")).alias("second")
    pipeline = (
        (first.render("phase3_output_aggregate_limit") | second.render("phase3_output_aggregate_limit"))
        .add_adapter(set, "phase3-a", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase3-b", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
    )
    run = Deployment(pipeline).run(keep=True)

    with pytest.raises(RuntimeError, match=r"node_adapter_output_validation.*aggregate"):
        run.value("first")


def test_conductor_rejects_oversized_artifact_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, invocation, payload, _ = _promotion_case(tmp_path)
    monkeypatch.setattr(m_node_runtime.m_runtime_port_adapters, "MAX_RUNTIME_OUTPUT_BYTES", 4)

    with pytest.raises(RuntimeError, match="node_adapter_output_validation"):
        m_node_runtime._promote_isolated_node_output(
            context,
            invocation,
            payload,
            runtime_name="venv-subprocess",
            failure_target="producer",
        )


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
@pytest.mark.parametrize(
    ("producer_runtime", "consumer_runtime"),
    [("docker", None), ("venv-subprocess", "docker")],
    ids=["docker-producer", "docker-consumer"],
)
def test_live_docker_full_recalculation_skips_invalid_parent_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    producer_runtime: str,
    consumer_runtime: str | None,
) -> None:
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
    pipeline = _output_pipeline(
        consumer_runtime=consumer_runtime,
        producer_runtime=producer_runtime,
    )
    deployment = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}},
    )
    parent = deployment.run(keep=True)
    with parent:
        expected = [2, 5, 7] if consumer_runtime is None else {"values": [2, 5, 7], "load_pid": None}
        assert parent.value("consumer") == expected
    _corrupt_execution_plan(parent, "invalid-manifest-digest")

    load_calls = 0

    def unexpected_load_plan(**kwargs: Any) -> Any:
        del kwargs
        nonlocal load_calls
        load_calls += 1
        raise AssertionError("full recovery must not access parent evidence")

    monkeypatch.setattr(m_resume, "load_run_execution_plan", unexpected_load_plan)
    child = deployment.resume(parent.run_id, from_="producer", keep=True)
    with child:
        assert child.value("consumer") == expected

    assert load_calls == 0
    manifest = cast(dict[str, Any], child.manifest_snapshot)
    assert all(record["status"] == "succeeded" for record in manifest["nodes"].values())
    runtimes = {record["alias"]: record["runtime"]["name"] for record in manifest["nodes"].values()}
    assert runtimes["producer"] == producer_runtime
    assert runtimes["consumer"] == (consumer_runtime or "native")
    assert manifest["execution_plan"]["adapter_overrides"] == []


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
@pytest.mark.parametrize(
    ("producer_runtime", "consumer_runtime"),
    [
        ("docker", None),
        ("docker", "venv-subprocess"),
        ("venv-subprocess", "docker"),
    ],
)
def test_live_docker_custom_artifact_output_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    producer_runtime: str,
    consumer_runtime: str | None,
) -> None:
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
    run = Deployment(
        _output_pipeline(
            producer_runtime=producer_runtime,
            consumer_runtime=consumer_runtime,
        ),
        runtime_config={"docker": {"image": image, "network": "none"}},
    ).run(keep=True)

    with run:
        result = run.value("consumer")

    if consumer_runtime is None:
        assert result == [2, 5, 7]
    else:
        assert result["values"] == [2, 5, 7]


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_live_docker_phase3_producer_materializes_multiple_formats(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set).alias("producer")
    first = lift_any(consume_output_set).bind(value=producer.as_format("phase3-a")).alias("first")
    second = lift_any(consume_output_set_isolated).bind(value=producer.as_format("phase3-b")).alias("second")
    pipeline = (
        (first.render("phase3_docker_fanout") | second.render("phase3_docker_fanout"))
        .add_adapter(set, "phase3-a", save=save_output_set, load=load_output_set)
        .add_adapter(set, "phase3-b", save=save_output_set_alt, load=load_output_set)
        .with_node_runtime("producer", "docker")
        .with_node_runtime("second", "venv-subprocess")
    )
    run = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}},
    ).run(keep=True)

    with run:
        assert run.value("second")["values"] == [2, 5, 7]
        assert run.value("first") == [2, 5, 7]

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    assert producer_record["runtime"]["name"] == "docker"
    assert len(producer_record["outputs"][DEFAULT_PORT]["variants"]) == 2


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_live_docker_mixed_pipeline_and_edge_sources_share_one_variant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    save_marker = tmp_path / "save-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    pipeline = _phase3_mixed_save_source_pipeline(formatted_consumer_runtime="docker")
    run = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}},
    ).run(keep=True)

    with run:
        assert run.value("edge-consumer") == 7
        assert run.value("pipeline-consumer") == 14

    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save"]
    manifest = cast(dict[str, Any], run.manifest_snapshot)
    nodes = {item["id"]: item for item in manifest["nodes"].values()}
    producer_record = next(item for item in nodes.values() if item["alias"] == "producer")
    edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer_record["id"]]
    assert len({edge["artifact_variant"] for edge in edges}) == 1
    assert len({edge["artifact"]["ref"]["sha256"] for edge in edges}) == 1
    assert {nodes[edge["target"]["node_id"]]["alias"]: edge["adapter"]["save"]["source"] for edge in edges} == {
        "edge-consumer": "edge",
        "pipeline-consumer": "pipeline",
    }


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_live_docker_genuine_run_override_evidence_survives_frozen_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    producer_marker = tmp_path / "producer-calls.txt"
    save_marker = tmp_path / "save-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    pipeline = _phase3_mixed_save_source_pipeline(formatted_consumer_runtime="docker")
    override = _phase3_run_override_adapter()
    override_map = {("producer", DEFAULT_PORT): override}
    deployment = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}},
    )
    parent = deployment.run(keep=True, adapters=override_map)
    with parent:
        assert parent.value("pipeline-consumer") == 14
        assert parent.value("edge-consumer") == 7

    child = parent.resume(
        from_=["pipeline-consumer", "edge-consumer"],
        adapters=override_map,
        keep=True,
    )
    with child:
        assert child.value("edge-consumer") == 7
        assert child.value("pipeline-consumer") == 14

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer"]
    assert save_marker.read_text(encoding="utf-8").splitlines() == ["save"]
    for retained in (parent, child):
        manifest = cast(dict[str, Any], retained.manifest_snapshot)
        producer, producer_edges = _phase3_producer_and_edges(manifest)
        assert producer["adapters"][DEFAULT_PORT]["source"] == "run-override"
        assert {edge["adapter"]["save"]["source"] for edge in producer_edges} == {"run-override"}
        assert len({edge["artifact"]["ref"]["sha256"] for edge in producer_edges}) == 1
        assert len(manifest["execution_plan"]["adapter_overrides"]) == 1


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_live_docker_coordinated_source_tamper_is_rejected_before_child_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    runs_home = tmp_path / "runs"
    save_marker = tmp_path / "save-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    monkeypatch.setenv("SPL_PHASE3_SAVE_CALLS", str(save_marker))
    pipeline = _phase3_mixed_save_source_pipeline(formatted_consumer_runtime="docker")
    deployment = Deployment(
        pipeline,
        runtime_config={"docker": {"image": image, "network": "none"}},
    )
    parent = deployment.run(keep=True)
    with parent:
        assert parent.value("edge-consumer") == 7
        assert parent.value("pipeline-consumer") == 14

    assert parent.manifest_path is not None
    manifest = cast(dict[str, Any], json.loads(parent.manifest_path.read_text(encoding="utf-8")))
    producer, producer_edges = _phase3_producer_and_edges(manifest)
    producer["adapters"][DEFAULT_PORT]["source"] = "run-override"
    for edge in producer_edges:
        edge["adapter"]["save"]["source"] = "run-override"
    parent.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    run_dirs_before = {path.name for path in runs_home.iterdir()}
    save_calls_before = save_marker.read_bytes()

    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        deployment.resume(
            parent.run_id,
            from_=["pipeline-consumer", "edge-consumer"],
            keep=True,
        )

    message = str(exc_info.value)
    assert "source is inconsistent" in message
    assert "from_='producer'" in message
    assert "run-override" not in message
    assert str(tmp_path) not in message
    assert "Traceback" not in message
    assert {path.name for path in runs_home.iterdir()} == run_dirs_before
    assert save_marker.read_bytes() == save_calls_before


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_live_docker_phase3_frozen_shared_variant_feeds_all_runtime_kinds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    producer_marker = tmp_path / "producer-calls.txt"
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setenv("SPL_PHASE3_PRODUCER_CALLS", str(producer_marker))
    lift_any = cast(Any, lift)
    producer = lift_any(produce_output_set_counted).alias("producer")
    native = lift_any(consume_output_set).bind(value=producer.as_format("phase3-set")).alias("native")
    venv = lift_any(consume_output_set_isolated).bind(value=producer.as_format("phase3-set")).alias("venv")
    docker = lift_any(consume_output_set_isolated).bind(value=producer.as_format("phase3-set")).alias("docker")
    pipeline = (
        (
            native.render("phase3_docker_resume")
            | venv.render("phase3_docker_resume")
            | docker.render("phase3_docker_resume")
        )
        .add_adapter(set, "phase3-set", save=save_output_set, load=load_output_set)
        .with_node_runtime("producer", "venv-subprocess")
        .with_node_runtime("venv", "venv-subprocess")
        .with_node_runtime("docker", "docker")
    )
    deployment = Deployment(pipeline, runtime_config={"docker": {"image": image, "network": "none"}})
    parent = deployment.run(keep=True)
    with parent:
        assert parent.value("native") == [2, 5, 7]
        assert parent.value("venv")["values"] == [2, 5, 7]
        assert parent.value("docker")["values"] == [2, 5, 7]

    child = parent.resume(from_=["native", "venv", "docker"], keep=True)
    with child:
        assert child.value("docker")["values"] == [2, 5, 7]
        assert child.value("venv")["values"] == [2, 5, 7]
        assert child.value("native") == [2, 5, 7]

    assert producer_marker.read_text(encoding="utf-8").splitlines() == ["producer"]
    manifest = cast(dict[str, Any], child.manifest_snapshot)
    producer_record = next(item for item in manifest["nodes"].values() if item["alias"] == "producer")
    assert producer_record["status"] == "frozen"
    producer_edges = [edge for edge in manifest["edges"] if edge["source"]["node_id"] == producer_record["id"]]
    assert len({edge["artifact"]["ref"]["sha256"] for edge in producer_edges}) == 1


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_live_docker_run_override_replaces_explicit_graph_format(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    override = _output_override_adapter()
    run = Deployment(
        _override_output_pipeline(producer_runtime="docker"),
        runtime_config={"docker": {"image": image, "network": "none"}},
    ).run(
        keep=True,
        adapters={("producer", DEFAULT_PORT): override},
    )

    with run:
        assert run.value("consumer") == [2, 5, 7]

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    [edge] = manifest["edges"]
    assert edge["adapter"]["save"]["source"] == "run-override"
    assert edge["adapter"]["load"]["source"] == "run-override"
    assert edge["adapter"]["save"]["identity"]["key"] == override.key
    assert edge["adapter"]["load"]["identity"]["key"] == override.key


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_live_docker_ndarray_artifact_output_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = "splime-phase2-numpy-test:py313"
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
    expected = {
        "type": "ndarray",
        "shape": [2, 2],
        "values": [[11, 12], [13, 14]],
    }
    for index, (producer_runtime, consumer_runtime) in enumerate(
        [
            ("docker", None),
            ("docker", "venv-subprocess"),
            ("venv-subprocess", "docker"),
        ]
    ):
        monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / f"runs-{index}"))
        run = Deployment(
            _ndarray_output_pipeline(
                producer_runtime=producer_runtime,
                consumer_runtime=consumer_runtime,
            ),
            runtime_config={"docker": {"image": image, "network": "none"}},
        ).run(keep=True)
        with run:
            assert run.value("consumer") == expected
