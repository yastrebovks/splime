import ast
import gc
import hashlib
import importlib
import json
import re
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from spl import Deployment, lift
from spl.core._common import Run, decode
from spl.core.entities.adapter import (
    BUILTIN_JSON_ADAPTER,
    Adapter,
    DAdapter,
    DLoadAdapter,
    DSaveAdapter,
    adapter_identity,
    make_key,
)
from spl.core.entities.artifact import ArtifactRef
from spl.core.entities.distribution import DDistribution
from spl.core.entities.node import DEFAULT_PORT, DFormattedOutputRef, FormattedOutputRef
from spl.core.entities.pipeline import AdapterResolutionSource, DPipeline, Pipeline
from spl.core.ir.parse import get_top_level_deps
from spl.core.ir.unparse import ir_unparse
from spl.core.ir.utils import SPLSafeLoader, spl_export_to_file, spl_import_from_file


def _adapter_save(path: str, obj: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(obj)


def _adapter_load(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


def _adapter_load_should_not_run(path: str) -> str:
    raise AssertionError("load must not run before tag compatibility is checked")


def _adapter_save_upper(path: str, obj: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(obj.upper())


@dataclass(frozen=True)
class _TagOnlyLoadAdapter:
    key: str
    load: Any
    accepted_tags: frozenset[str]
    legacy_key_guard: bool = False
    distributions: tuple[DDistribution, ...] = ()


@dataclass(frozen=True)
class RuntimeBox:
    value: str
    decoded: bool = False


def _runtime_make_box() -> RuntimeBox:
    return RuntimeBox("hello-runtime")


def _runtime_consume_box(box: RuntimeBox) -> tuple[str, bool]:
    return (box.value, box.decoded)


def _runtime_save_box(path: str, obj: RuntimeBox) -> None:
    with open(path, "wb") as f:
        f.write(obj.value.encode("utf-8"))


def _runtime_load_box(path: str) -> RuntimeBox:
    with open(path, "rb") as f:
        return RuntimeBox(f.read().decode("utf-8"), decoded=True)


def _runtime_example_box() -> RuntimeBox:
    return RuntimeBox("hello-runtime")


def _runtime_load_box_broken(path: str) -> RuntimeBox:
    del path
    raise ValueError("broken runtime box load")


def _runtime_save_box_alt(path: str, obj: RuntimeBox) -> None:
    with open(path, "wb") as f:
        f.write(obj.value.upper().encode("utf-8"))


def _unused_run_callback(**kwargs: Any) -> dict[str, Any]:
    del kwargs
    return {}


def _runtime_box_pipeline() -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(_runtime_make_box)
    pipeline = lift_any(_runtime_consume_box).bind(box=producer).alias("consumer").render("runtime_pipeline")
    return cast(Pipeline, pipeline.add_adapter(RuntimeBox, "bytes", save=_runtime_save_box, load=_runtime_load_box))


def _runtime_box_override_pipeline() -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(_runtime_make_box).alias("producer")
    pipeline = lift_any(_runtime_consume_box).bind(box=producer).alias("consumer").render("runtime_override_pipeline")
    return cast(
        Pipeline, pipeline.add_adapter(RuntimeBox, "bytes", save=_runtime_save_box, load=_runtime_load_box_broken)
    )


def _format_make_value() -> str:
    return "hello-format"


def _format_consume_value(value: str) -> str:
    return value


def _json_scalar_add(left: int, right: int) -> int:
    return left + right


def _format_save_csv(path: str, obj: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(obj)


def _format_load_csv(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        value = f.read()
    return "csv:{}".format(value)


def _format_save_bytes(path: str, obj: str) -> None:
    with open(path, "wb") as f:
        f.write(obj.encode("utf-8"))


def _format_load_bytes(path: str) -> str:
    with open(path, "rb") as f:
        value = f.read().decode("utf-8")
    return "bytes:{}".format(value)


def _format_pipeline(include_bytes: bool = True) -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(_format_make_value)
    csv_pipeline = lift_any(_format_consume_value).bind(value=producer.as_format("csv")).alias("csv").render()
    bytes_pipeline = lift_any(_format_consume_value).bind(value=producer.as_format("bytes")).alias("bytes").render()
    pipeline = csv_pipeline | bytes_pipeline
    pipeline = Pipeline(
        name="format_pipeline",
        nodes=pipeline.nodes,
        links=pipeline.links,
        aliases=pipeline.aliases,
        adapters=pipeline.adapters,
    )
    pipeline = pipeline.add_adapter(str, "csv", save=_format_save_csv, load=_format_load_csv)
    if not include_bytes:
        return pipeline
    return pipeline.add_adapter(str, "bytes", save=_format_save_bytes, load=_format_load_bytes)


def _plain_string_pipeline() -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(_format_make_value)
    return cast(
        Pipeline, lift_any(_format_consume_value).bind(value=producer).alias("consumer").render("plain_string_pipeline")
    )


def _run_without_context(pipeline: Pipeline) -> tuple[dict[str, Any], Path, list[Any]]:
    deployment = cast(Any, Deployment)
    consumer = cast(Any, pipeline).get_node_by_alias("consumer")
    run = deployment(pipeline).run()
    result = run[consumer]
    artifacts_dir = cast(Path, run._artifacts_dir)
    refs = list(run._artifact_refs.values())

    assert artifacts_dir.exists()
    return result, artifacts_dir, refs


def _load_e2e_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    module_name = "adapter_e2e_module"
    module_path = tmp_path / "{}.py".format(module_name)

    module_path.write_text(
        textwrap.dedent("""
        from dataclasses import dataclass


        @dataclass(frozen=True)
        class E2EBox:
            value: str
            history: tuple[str, ...] = ()


        def make_box():
            return E2EBox('seed', ('made',))


        def normalize_box(box):
            return E2EBox(box.value.upper(), (*box.history, 'normalized'))


        def summarize_box(box):
            return '|'.join((*box.history, box.value))


        def save_box(path, obj):
            with open(path, 'w', encoding='utf-8') as f:
                f.write(obj.value)
                f.write('\\n')
                f.write('\\n'.join(obj.history))


        def load_box(path):
            with open(path, encoding='utf-8') as f:
                lines = f.read().splitlines()
            value, *history = lines
            return E2EBox(value, (*history, 'loaded'))
        """),
        encoding="utf-8",
    )

    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop(module_name, None)
    importlib.invalidate_caches()
    return importlib.import_module(module_name)


def _e2e_pipeline(module: Any) -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(module.make_box)
    normalizer = lift_any(module.normalize_box).bind(box=producer)
    pipeline = lift_any(module.summarize_box).bind(box=normalizer).alias("summary").render("adapter_e2e_pipeline")
    return cast(Pipeline, pipeline.add_adapter(module.E2EBox, "txt", save=module.save_box, load=module.load_box))


def _reconstruct_adapter(root: DAdapter, dependencies: list[Any]) -> Adapter:
    body: list[ast.stmt] = []
    for dependency in dependencies:
        body.extend(ir_unparse(dependency, source=Path("adapter.yaml")))
    body.extend(ir_unparse(root, source=Path("adapter.yaml")))

    module = ast.fix_missing_locations(ast.Module(body=body, type_ignores=[]))
    namespace: dict[str, Any] = {
        "Adapter": Adapter,
        "DDistribution": DDistribution,
    }

    exec(compile(module, "adapter.yaml", mode="exec"), namespace)  # noqa: S102

    return cast(Adapter, namespace["_adapter"])


def test_adapter_yaml_ir_round_trip_reconstructs_save_load(tmp_path: Path) -> None:
    adapter = Adapter(key=make_key(str, "txt"), save=_adapter_save, load=_adapter_load, py_type=str, format="txt")
    [(root, dependencies)] = [
        (root, dependencies) for root, dependencies in get_top_level_deps(2, [adapter]) if isinstance(root, DAdapter)
    ]
    dumped = yaml.dump([root, *dependencies], sort_keys=False)
    [loaded_root, *loaded_dependencies] = yaml.load(dumped, Loader=SPLSafeLoader)

    reconstructed = _reconstruct_adapter(cast(DAdapter, loaded_root), cast(list[Any], loaded_dependencies))
    path = tmp_path / "value.txt"

    reconstructed.save(str(path), "hello adapter")

    assert loaded_root == root
    assert reconstructed.key == adapter.key
    assert reconstructed.format == adapter.format
    assert reconstructed.py_type is None
    assert reconstructed.save.__name__ == _adapter_save.__name__
    assert reconstructed.load.__name__ == _adapter_load.__name__
    assert reconstructed.load(str(path)) == "hello adapter"


def test_adapter_exposes_legacy_save_and_load_halves() -> None:
    adapter = Adapter(key=make_key(str, "txt"), save=_adapter_save, load=_adapter_load, py_type=str, format="txt")

    assert adapter.tag == "txt"
    assert adapter.accepted_tags == frozenset({"txt"})
    assert adapter.legacy_key_guard is True


def test_adapter_half_yaml_tags_are_additive() -> None:
    save = DSaveAdapter(key=make_key(str, "json"), tag="json", save="_adapter_save")
    load = DLoadAdapter(key=make_key(str, "json"), accepted_tags=("json", "ndjson"), load="_adapter_load")
    dumped = yaml.dump([save, load], sort_keys=False)
    loaded = yaml.load(dumped, Loader=SPLSafeLoader)

    assert loaded == [save, load]


def test_make_key_is_stable() -> None:
    assert make_key(str, "txt") == "builtins.str@txt"
    assert make_key(str, "txt") == make_key(str, "txt")


def test_pipeline_add_adapter_returns_new_pipeline() -> None:
    pipeline = Pipeline()
    updated = pipeline.add_adapter(str, "txt", save=_adapter_save, load=_adapter_load)
    key = make_key(str, "txt")
    adapter = updated.resolve_adapter(py_type=str, format="txt")

    assert pipeline.adapters == {}
    assert updated is not pipeline
    assert adapter is updated.adapters[key]
    assert updated.resolve_adapter(key=key) is adapter
    assert updated.resolve_adapter(py_type=bytes, format="txt") is None
    assert Pipeline().resolve_adapter(py_type=dict) is None


def test_adapter_resolution_uses_port_default_json_for_json_native_values() -> None:
    resolution = Pipeline().resolve_adapter_binding(py_type=dict)

    assert resolution is not None
    assert resolution.adapter is BUILTIN_JSON_ADAPTER
    assert resolution.adapter.tag == "json"
    assert resolution.source == AdapterResolutionSource.PORT_DEFAULT


def test_builtin_json_adapter_can_round_trip_file(tmp_path: Path) -> None:
    path = tmp_path / "value.json"

    BUILTIN_JSON_ADAPTER.save(str(path), {"items": [1, 2], "ok": True})

    assert BUILTIN_JSON_ADAPTER.load(str(path)) == {"items": [1, 2], "ok": True}
    assert BUILTIN_JSON_ADAPTER.accepted_tags == frozenset({"json"})
    assert BUILTIN_JSON_ADAPTER.legacy_key_guard is False


def test_adapter_resolution_reports_pipeline_source_for_registered_adapter() -> None:
    pipeline = _runtime_box_pipeline()
    registered = pipeline.resolve_adapter(py_type=RuntimeBox)
    resolution = pipeline.resolve_adapter_binding(py_type=RuntimeBox)

    assert registered is not None
    assert resolution is not None
    assert resolution.adapter is registered
    assert resolution.source == AdapterResolutionSource.PIPELINE


def test_adapter_resolution_reports_edge_source_for_format_override() -> None:
    pipeline = _format_pipeline()
    resolution = pipeline.resolve_adapter_binding(py_type=str, format="csv")

    assert resolution is not None
    assert resolution.adapter is pipeline.resolve_adapter(py_type=str, format="csv")
    assert resolution.source == AdapterResolutionSource.EDGE


def test_adapter_resolution_run_override_extension_point_wins_last() -> None:
    pipeline = _runtime_box_pipeline()
    override = Adapter(
        key=make_key(RuntimeBox, "override"),
        save=_runtime_save_box_alt,
        load=_runtime_load_box,
        py_type=RuntimeBox,
        format="override",
    )
    resolution = pipeline.resolve_adapter_binding(py_type=RuntimeBox, run_override=override)

    assert resolution is not None
    assert resolution.adapter is override
    assert resolution.source == AdapterResolutionSource.RUN_OVERRIDE


def test_pipeline_or_merges_adapters_and_rejects_conflicts() -> None:
    left = Pipeline().add_adapter(str, "txt", save=_adapter_save, load=_adapter_load)
    right = Pipeline().add_adapter(bytes, "bin", save=_adapter_save, load=_adapter_load)
    merged = left | right

    assert set(merged.adapters) == {make_key(str, "txt"), make_key(bytes, "bin")}

    conflicting = Pipeline().add_adapter(str, "txt", save=_adapter_save_upper, load=_adapter_load)
    with pytest.raises(ValueError, match="pipeline adapter conflict: `builtins.str@txt`"):
        left | conflicting


def test_adapter_requires_save_and_load_functions() -> None:
    key = make_key(str, "txt")

    with pytest.raises(TypeError, match="adapter save must be a function"):
        Adapter(key=key, save=cast(Any, None), load=_adapter_load, py_type=str, format="txt")

    with pytest.raises(TypeError, match="adapter load must be a function"):
        Adapter(key=key, save=_adapter_save, load=cast(Any, "load"), py_type=str, format="txt")


def test_pipeline_merge_rejects_duplicate_adapter_key() -> None:
    left = Pipeline().add_adapter(RuntimeBox, "bytes", save=_runtime_save_box, load=_runtime_load_box)
    right = Pipeline().add_adapter(RuntimeBox, "bytes", save=_runtime_save_box_alt, load=_runtime_load_box)
    key = make_key(RuntimeBox, "bytes")

    with pytest.raises(ValueError, match=re.escape("pipeline adapter conflict: `{}`".format(key))):
        left | right


def test_pipeline_yaml_round_trip_preserves_callable_adapters(tmp_path: Path) -> None:
    pipeline = Pipeline(name="adapter_pipeline").add_adapter(str, "txt", save=_adapter_save, load=_adapter_load)
    [(root, dependencies)] = [
        (root, dependencies) for root, dependencies in get_top_level_deps(2, [pipeline]) if isinstance(root, DPipeline)
    ]
    dumped = yaml.dump_all([[root, *dependencies]], sort_keys=False)
    [(loaded_root, *loaded_dependencies)] = yaml.load_all(dumped, Loader=SPLSafeLoader)
    path = tmp_path / "pipeline.yaml"
    namespace: dict[str, Any] = {}

    path.write_text(dumped, encoding="utf-8")
    spl_import_from_file(path, namespace)

    imported = cast(Pipeline, namespace["adapter_pipeline"])
    adapter = imported.resolve_adapter(py_type=str, format="txt")
    value_path = tmp_path / "value.txt"

    assert loaded_root == root
    assert loaded_dependencies == dependencies
    assert set(imported.adapters) == {make_key(str, "txt")}
    assert adapter is not None

    adapter.save(str(value_path), "hello pipeline")

    assert adapter.load(str(value_path)) == "hello pipeline"


def test_run_encodes_and_decodes_adapter_edges(tmp_path: Path) -> None:
    deployment = cast(Any, Deployment)
    pipeline = _runtime_box_pipeline()
    consumer = cast(Any, pipeline).get_node_by_alias("consumer")
    run = deployment(pipeline).run()

    with run:
        result = run[consumer]
        artifacts_dir = cast(Path, run._artifacts_dir)
        refs = list(run._artifact_refs.values())

    expected_sha256 = hashlib.sha256(b"hello-runtime").hexdigest()

    assert result == {"default": ("hello-runtime", True)}
    assert len(refs) == 1
    assert refs[0].key == make_key(RuntimeBox, "bytes")
    assert refs[0].tag == "bytes"
    assert refs[0].sha256 == expected_sha256
    assert refs[0].size == len(b"hello-runtime")
    assert not artifacts_dir.exists()


def test_run_adapter_override_replaces_edge_adapter_and_records_source() -> None:
    deployment = cast(Any, Deployment)
    pipeline = _runtime_box_override_pipeline()
    consumer = cast(Any, pipeline).get_node_by_alias("consumer")
    producer = cast(Any, pipeline).get_node_by_alias("producer")
    override = Adapter(
        key=make_key(RuntimeBox, "bytes"),
        save=_runtime_save_box,
        load=_runtime_load_box,
        py_type=RuntimeBox,
        format="bytes",
    )
    identity = adapter_identity(override)

    with pytest.raises(ValueError, match="broken runtime box load"):
        deployment(pipeline).run(output="consumer")

    run = deployment(pipeline).run(adapters={("producer", DEFAULT_PORT): override})
    with run:
        assert run[consumer] == {"default": ("hello-runtime", True)}
        assert run._adapter_resolutions[(producer, DEFAULT_PORT)].source == AdapterResolutionSource.RUN_OVERRIDE

    assert identity["key"] == make_key(RuntimeBox, "bytes")
    assert identity["tag"] == "bytes"
    assert identity["accepted_tags"] == ["bytes"]
    json.dumps(identity, sort_keys=True)


def test_adapter_example_does_not_change_hash_or_identity() -> None:
    without_example = Adapter(
        key=make_key(RuntimeBox, "bytes"),
        save=_runtime_save_box,
        load=_runtime_load_box,
        py_type=RuntimeBox,
        format="bytes",
    )
    with_example = Adapter(
        key=make_key(RuntimeBox, "bytes"),
        save=_runtime_save_box,
        load=_runtime_load_box,
        py_type=RuntimeBox,
        format="bytes",
        example=_runtime_example_box,
    )

    assert with_example == without_example
    assert hash(with_example) == hash(without_example)
    assert adapter_identity(with_example) == adapter_identity(without_example)


def test_run_adapter_override_validates_alias_and_port_before_execution() -> None:
    deployment = cast(Any, Deployment)
    pipeline = _runtime_box_override_pipeline()
    override = Adapter(
        key=make_key(RuntimeBox, "bytes"),
        save=_runtime_save_box,
        load=_runtime_load_box,
        py_type=RuntimeBox,
        format="bytes",
    )

    with pytest.raises(ValueError, match="unknown alias `missing`"):
        deployment(pipeline).run(adapters={("missing", DEFAULT_PORT): override})

    with pytest.raises(ValueError, match="unknown output port `missing` for alias `producer`"):
        deployment(pipeline).run(adapters={("producer", "missing"): override})


def test_run_adapter_override_does_not_leak_between_runs() -> None:
    deployment = cast(Any, Deployment)
    pipeline = _runtime_box_override_pipeline()
    override = Adapter(
        key=make_key(RuntimeBox, "bytes"),
        save=_runtime_save_box,
        load=_runtime_load_box,
        py_type=RuntimeBox,
        format="bytes",
    )

    assert deployment(pipeline).run(output="consumer", adapters={("producer", DEFAULT_PORT): override}) == (
        "hello-runtime",
        True,
    )
    with pytest.raises(ValueError, match="broken runtime box load"):
        deployment(pipeline).run(output="consumer")


def test_run_without_context_cleans_artifacts_with_finalizer() -> None:
    result, artifacts_dir, refs = _run_without_context(_runtime_box_pipeline())
    gc.collect()

    assert result == {"default": ("hello-runtime", True)}
    assert len(refs) == 1
    assert not artifacts_dir.exists()


def test_run_without_adapters_does_not_create_artifacts_dir() -> None:
    deployment = cast(Any, Deployment)
    pipeline = _plain_string_pipeline()
    consumer = cast(Any, pipeline).get_node_by_alias("consumer")
    run = deployment(pipeline).run()

    assert run._artifacts_dir is None
    assert run[consumer] == {"default": "hello-format"}
    assert run._artifact_refs == {}
    assert run._artifacts_dir is None


def test_run_with_json_native_scalars_does_not_create_artifacts_dir() -> None:
    lift_any = cast(Any, lift)
    deployment = cast(Any, Deployment)
    pipeline = lift_any(_json_scalar_add).bind(left=2, right=5).alias("sum").render("json_scalar_pipeline")
    node = cast(Any, pipeline).get_node_by_alias("sum")
    run = deployment(pipeline).run()

    assert run[node] == {"default": 7}
    assert run._artifact_refs == {}
    assert run._artifacts_dir is None


@pytest.mark.parametrize(
    "value",
    [
        7,
        {"left": 1, "right": [True, "ok"]},
        list(range(256)),
    ],
)
def test_json_native_shortcut_matches_folded_builtin_json_adapter_path(value: Any) -> None:
    pipeline = Pipeline()
    shortcut_run = Run(_unused_run_callback, pipeline, keep=False)
    folded_run = Run(_unused_run_callback, pipeline, keep=False)

    assert shortcut_run._round_trip_artifact(value) == value
    assert folded_run._round_trip_resolved(value, None, None, None) == value
    assert shortcut_run._artifact_refs == folded_run._artifact_refs == {}
    assert shortcut_run._adapter_resolutions == folded_run._adapter_resolutions == {}
    assert shortcut_run._artifacts_dir is folded_run._artifacts_dir is None


def test_explicit_json_edge_uses_builtin_adapter_without_artifact_files() -> None:
    lift_any = cast(Any, lift)
    deployment = cast(Any, Deployment)
    producer = lift_any(_format_make_value)
    pipeline = lift_any(_format_consume_value).bind(value=producer.as_format("json")).alias("consumer").render()
    consumer = cast(Any, pipeline).get_node_by_alias("consumer")
    run = deployment(pipeline).run()

    assert run[consumer] == {"default": "hello-format"}
    assert run._artifact_refs == {}
    assert run._artifacts_dir is None


def test_builtin_adapter_requires_explicit_artifact_edge() -> None:
    deployment = cast(Any, Deployment)
    pipeline = _plain_string_pipeline().add_adapter(str, "csv", save=_format_save_csv, load=_format_load_csv)
    consumer = cast(Any, pipeline).get_node_by_alias("consumer")
    run = deployment(pipeline).run()

    assert run[consumer] == {"default": "hello-format"}
    assert run._artifact_refs == {}
    assert run._artifacts_dir is None


def test_e2e_artifact_adapter_pipeline_round_trips_through_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deployment = cast(Any, Deployment)
    module = _load_e2e_module(tmp_path, monkeypatch)
    pipeline = _e2e_pipeline(module)
    summary = cast(Any, pipeline).get_node_by_alias("summary")
    yaml_path = tmp_path / "adapter-e2e.yaml"
    namespace: dict[str, Any] = {}

    with deployment(pipeline).run() as run:
        in_memory_result = run[summary]
        in_memory_artifacts_dir = cast(Path, run._artifacts_dir)
        in_memory_refs = list(run._artifact_refs.values())

    spl_export_to_file(yaml_path, [pipeline])
    spl_import_from_file(yaml_path, namespace)

    imported = cast(Pipeline, namespace["adapter_e2e_pipeline"])
    imported_summary = cast(Any, imported).get_node_by_alias("summary")

    with deployment(imported).run() as run:
        imported_result = run[imported_summary]
        imported_artifacts_dir = cast(Path, run._artifacts_dir)
        imported_refs = list(run._artifact_refs.values())

    assert in_memory_result == {"default": "made|loaded|normalized|loaded|SEED"}
    assert imported_result == in_memory_result
    assert set(imported.adapters) == set(pipeline.adapters)
    assert len(in_memory_refs) == 2
    assert len(imported_refs) == 2
    assert not in_memory_artifacts_dir.exists()
    assert not imported_artifacts_dir.exists()


def test_pipeline_builder_format_override_selects_edge_adapter() -> None:
    deployment = cast(Any, Deployment)
    pipeline = _format_pipeline()
    csv_node = cast(Any, pipeline).get_node_by_alias("csv")
    bytes_node = cast(Any, pipeline).get_node_by_alias("bytes")
    run = deployment(pipeline).run()

    with run:
        csv_result = run[csv_node]
        bytes_result = run[bytes_node]
        refs = list(run._artifact_refs.values())

    assert sorted([value.format for _, value in pipeline.links if isinstance(value, FormattedOutputRef)]) == [
        "bytes",
        "csv",
    ]
    assert csv_result == {"default": "csv:hello-format"}
    assert bytes_result == {"default": "bytes:hello-format"}
    assert sorted([ref.key for ref in refs]) == [make_key(str, "bytes"), make_key(str, "csv")]
    assert sorted([ref.tag for ref in refs]) == ["bytes", "csv"]


def test_decode_rejects_unaccepted_artifact_tag_before_loading(tmp_path: Path) -> None:
    path = tmp_path / "value.csv"
    path.write_text("a,b\n", encoding="utf-8")
    ref = ArtifactRef(
        key=make_key(str, "csv"),
        uri=str(path),
        sha256=hashlib.sha256(b"a,b\n").hexdigest(),
        size=path.stat().st_size,
        tag="csv",
    )
    adapter = _TagOnlyLoadAdapter(
        key=make_key(str, "tsv"),
        load=_adapter_load_should_not_run,
        accepted_tags=frozenset({"tsv"}),
    )

    with pytest.raises(ValueError, match=r"artifact tag `csv`.*_adapter_load_should_not_run.*accepted tags: tsv"):
        decode(ref, adapter)


def test_legacy_decode_still_requires_adapter_key_match(tmp_path: Path) -> None:
    path = tmp_path / "value.txt"
    path.write_text("hello", encoding="utf-8")
    adapter = Adapter(key=make_key(str, "txt"), save=_adapter_save, load=_adapter_load, py_type=str, format="txt")
    ref = ArtifactRef(
        key=make_key(bytes, "txt"),
        uri=str(path),
        sha256=hashlib.sha256(b"hello").hexdigest(),
        size=path.stat().st_size,
        tag="txt",
    )

    with pytest.raises(ValueError, match="artifact ref key does not match adapter"):
        decode(ref, adapter)


def test_pipeline_builder_format_override_requires_matching_adapter() -> None:
    deployment = cast(Any, Deployment)
    pipeline = _format_pipeline(include_bytes=False)
    bytes_node = cast(Any, pipeline).get_node_by_alias("bytes")
    run = deployment(pipeline).run()

    with pytest.raises(ValueError, match="adapter is not found.*bytes"):
        run[bytes_node]

    assert run._artifacts_dir is None


def test_artifact_edge_with_no_resolvable_adapter_raises_precise_error() -> None:
    lift_any = cast(Any, lift)
    deployment = cast(Any, Deployment)
    producer = lift_any(_runtime_make_box)
    pipeline = (
        lift_any(_runtime_consume_box)
        .bind(box=producer.as_format("bytes"))
        .alias("consumer")
        .render("missing_adapter_pipeline")
    )
    consumer = cast(Any, pipeline).get_node_by_alias("consumer")
    run = deployment(pipeline).run()

    with pytest.raises(ValueError, match=r"pipeline adapter is not found .*format `bytes`"):
        run[consumer]

    assert run._artifacts_dir is None


def test_pipeline_builder_format_override_survives_yaml_round_trip(tmp_path: Path) -> None:
    deployment = cast(Any, Deployment)
    pipeline = _format_pipeline()
    [(root, dependencies)] = [
        (root, dependencies) for root, dependencies in get_top_level_deps(2, [pipeline]) if isinstance(root, DPipeline)
    ]
    dumped = yaml.dump_all([[root, *dependencies]], sort_keys=False)
    [(loaded_root, *loaded_dependencies)] = yaml.load_all(dumped, Loader=SPLSafeLoader)
    path = tmp_path / "format-pipeline.yaml"
    namespace: dict[str, Any] = {}

    path.write_text(dumped, encoding="utf-8")
    spl_import_from_file(path, namespace)

    imported = cast(Pipeline, namespace["format_pipeline"])
    csv_node = cast(Any, imported).get_node_by_alias("csv")
    bytes_node = cast(Any, imported).get_node_by_alias("bytes")
    run = deployment(imported).run()

    with run:
        csv_result = run[csv_node]
        bytes_result = run[bytes_node]

    assert loaded_root == root
    assert loaded_dependencies == dependencies
    assert sorted(
        [
            link_to.format
            for _, link_to in cast(DPipeline, loaded_root).links
            if isinstance(link_to, DFormattedOutputRef)
        ]
    ) == ["bytes", "csv"]
    assert sorted([value.format for _, value in imported.links if isinstance(value, FormattedOutputRef)]) == [
        "bytes",
        "csv",
    ]
    assert csv_result == {"default": "csv:hello-format"}
    assert bytes_result == {"default": "bytes:hello-format"}
