from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from spl import Deployment, lift
from spl.core.adapter_compat import AdapterCompatibilityWarning
from spl.core.entities.adapter import DAdapter, DLoadAdapter, DSaveAdapter, SplitLoadAdapter, SplitSaveAdapter
from spl.core.entities.control import DSPLImport, DSPLSelfImport
from spl.core.entities.module import DImport, DImportFrom
from spl.core.entities.node import (
    DFormattedOutputRef,
    DNodeInputRef,
    DNodeOutputRef,
    InputPort,
    OutputPort,
)
from spl.core.entities.function import DFunction
from spl.core.entities.node_function import DNodeFunction
from spl.core.entities.node_remote import DNodeRemote
from spl.core.entities.pipeline import DPipeline, Pipeline
from spl.core.entities.scalar import DScalar, Scalar
from spl.core.ir.utils import spl_export_to_file, spl_import_from_file
from spl.daemon.metadata import extract_metadata
from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore


PRODUCER_ID = "11111111-1111-4111-8111-111111111111"
CONSUMER_ID = "22222222-2222-4222-8222-222222222222"
SECOND_CONSUMER_ID = "33333333-3333-4333-8333-333333333333"


def _function(
    name: str,
    *,
    inputs: list[InputPort] | None = None,
    outputs: list[OutputPort] | None = None,
) -> DFunction:
    return DFunction(
        name=name,
        inputs=inputs or [],
        outputs=outputs if outputs is not None else [OutputPort(name="default", typ_="str")],
        body="return 'ok'",
    )


def _yaml(root: DFunction | DPipeline, *functions: DFunction) -> str:
    return yaml.dump_all([[root, *functions]], sort_keys=False)


def _missing_function_yaml() -> str:
    return _yaml(
        DPipeline(
            name="broken_pipeline",
            nodes=[DNodeFunction(uuid=PRODUCER_ID, func="missing")],
            links=[],
            aliases=[["producer", PRODUCER_ID]],
        )
    )


def _unknown_target_port_yaml() -> str:
    producer = _function("produce")
    consumer = _function(
        "consume",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    return _yaml(
        DPipeline(
            name="broken_pipeline",
            nodes=[
                DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
                DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
            ],
            links=[
                [
                    DNodeInputRef(uuid=CONSUMER_ID, port="missing"),
                    DNodeOutputRef(uuid=PRODUCER_ID, port="default"),
                ]
            ],
            aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        ),
        producer,
        consumer,
    )


def _unknown_source_port_yaml() -> str:
    producer = _function("produce")
    consumer = _function(
        "consume",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    return _yaml(
        DPipeline(
            name="broken_pipeline",
            nodes=[
                DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
                DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
            ],
            links=[
                [
                    DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                    DNodeOutputRef(uuid=PRODUCER_ID, port="missing"),
                ]
            ],
            aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        ),
        producer,
        consumer,
    )


def _duplicate_target_yaml() -> str:
    consumer = _function(
        "consume",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    return _yaml(
        DPipeline(
            name="broken_pipeline",
            nodes=[DNodeFunction(uuid=CONSUMER_ID, func=consumer.name)],
            links=[
                [DNodeInputRef(uuid=CONSUMER_ID, port="value"), DScalar(value="first")],
                [DNodeInputRef(uuid=CONSUMER_ID, port="value"), DScalar(value="second")],
            ],
            aliases=[["consumer", CONSUMER_ID]],
        ),
        consumer,
    )


def _wrong_link_kind_yaml() -> str:
    producer = _function("produce")
    return _yaml(
        DPipeline(
            name="broken_pipeline",
            nodes=[DNodeFunction(uuid=PRODUCER_ID, func=producer.name)],
            links=[[DNodeOutputRef(uuid=PRODUCER_ID, port="default"), DScalar(value="wrong-kind")]],
            aliases=[["producer", PRODUCER_ID]],
        ),
        producer,
    )


def _unknown_runtime_yaml() -> str:
    producer = _function("produce")
    return _yaml(
        DPipeline(
            name="broken_pipeline",
            nodes=[DNodeFunction(uuid=PRODUCER_ID, func=producer.name)],
            links=[],
            aliases=[["producer", PRODUCER_ID]],
            tags={PRODUCER_ID: {"runtime": "missing-runtime"}},
        ),
        producer,
    )


def _unknown_adapter_format_yaml() -> str:
    producer = _function("produce")
    consumer = _function(
        "consume",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    return _yaml(
        DPipeline(
            name="broken_pipeline",
            nodes=[
                DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
                DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
            ],
            links=[
                [
                    DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                    DFormattedOutputRef(uuid=PRODUCER_ID, port="default", format="missing-format"),
                ]
            ],
            aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        ),
        producer,
        consumer,
    )


def _duplicate_alias_yaml() -> str:
    producer = _function("produce")
    consumer = _function("consume")
    return _yaml(
        DPipeline(
            name="broken_pipeline",
            nodes=[
                DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
                DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
            ],
            links=[],
            aliases=[["duplicate", PRODUCER_ID], ["duplicate", CONSUMER_ID]],
        ),
        producer,
        consumer,
    )


def _duplicate_function_yaml() -> str:
    first = _function("produce")
    second = DFunction(
        name="produce",
        inputs=[],
        outputs=[OutputPort(name="other", typ_="str")],
        body="return 'different'",
    )
    return _yaml(
        DPipeline(
            name="broken_pipeline",
            nodes=[DNodeFunction(uuid=PRODUCER_ID, func="produce")],
            links=[],
            aliases=[["producer", PRODUCER_ID]],
        ),
        first,
        second,
    )


def _invalid_function_body_yaml() -> str:
    return _yaml(
        DFunction(
            name="broken_pipeline",
            inputs=[],
            outputs=[OutputPort(name="default", typ_="str")],
            body="return (",
        )
    )


def _invalid_input_identifier_yaml() -> str:
    return _yaml(
        DFunction(
            name="broken_pipeline",
            inputs=[InputPort(name="not-an-identifier", typ_="str", default=None)],
            outputs=[OutputPort(name="default", typ_="str")],
            body="return 'ok'",
        )
    )


def _scalar_pipeline_yaml(value: object) -> str:
    consumer = _function(
        "consume_literal",
        inputs=[InputPort(name="value", typ_=None, default=None)],
    )
    pipeline = DPipeline(
        name="literal_pipeline",
        nodes=[DNodeFunction(uuid=CONSUMER_ID, func=consumer.name)],
        links=[[DNodeInputRef(uuid=CONSUMER_ID, port="value"), DScalar(value=value)]],
        aliases=[["consumer", CONSUMER_ID]],
    )
    return _yaml(pipeline, consumer)


def _split_adapter_pipeline_yaml(
    halves: str = "both",
    *,
    cross_key: bool = False,
    untyped_target: bool = False,
    extra_load: bool = False,
) -> str:
    save_key = "builtins.str@txt"
    load_key = "builtins.dict@txt" if cross_key else save_key
    producer = DFunction(
        name="produce_text",
        inputs=[],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return 'split adapter'",
    )
    consumer = DFunction(
        name="consume_text",
        inputs=[
            InputPort(
                name="value",
                typ_=None if untyped_target else ("dict" if cross_key else "str"),
                default=None,
            )
        ],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return value['text'] + ' imported'" if cross_key else "return value + ' imported'",
    )
    save = DFunction(
        name="save_text",
        inputs=[
            InputPort(name="path", typ_="str", default=None),
            InputPort(name="value", typ_="str", default=None),
        ],
        outputs=[],
        body="from pathlib import Path\nPath(path).write_text(value, encoding='utf-8')",
    )
    load = DFunction(
        name="load_text",
        inputs=[InputPort(name="path", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="dict" if cross_key else "str")],
        body=(
            "from pathlib import Path\nreturn {'text': Path(path).read_text(encoding='utf-8')}"
            if cross_key
            else "from pathlib import Path\nreturn Path(path).read_text(encoding='utf-8')"
        ),
    )
    alternate_load = DFunction(
        name="load_text_alternate",
        inputs=[InputPort(name="path", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="from pathlib import Path\nreturn Path(path).read_text(encoding='utf-8')",
    )
    adapters: list[object] = []
    if halves in {"both", "save"}:
        adapters.append(DSaveAdapter(key=save_key, tag="txt", save=save.name))
    if halves in {"both", "load"}:
        adapters.append(DLoadAdapter(key=load_key, accepted_tags=("txt",), load=load.name))
    if extra_load:
        adapters.append(
            DLoadAdapter(
                key="builtins.str@txt",
                accepted_tags=("txt",),
                load=alternate_load.name,
            )
        )
    pipeline = DPipeline(
        name="split_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
            DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DFormattedOutputRef(uuid=PRODUCER_ID, port="default", format="txt"),
            ]
        ],
        aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        adapters=adapters,
    )
    dependencies = [producer, consumer, save, load]
    if extra_load:
        dependencies.append(alternate_load)
    return _yaml(pipeline, *dependencies)


@pytest.mark.parametrize(
    ("yaml_factory", "message_parts"),
    [
        (_missing_function_yaml, ("producer", "missing function", "missing")),
        (_unknown_target_port_yaml, ("consumer", "missing", "input port")),
        (_unknown_source_port_yaml, ("producer", "missing", "output port")),
        (_duplicate_target_yaml, ("consumer", "value", "linked more than once")),
        (_wrong_link_kind_yaml, ("target", "DNodeInputRef")),
        (_unknown_runtime_yaml, ("producer", "missing-runtime", "runtime")),
        (_unknown_adapter_format_yaml, ("missing-format", "adapter")),
        (_duplicate_alias_yaml, ("duplicate", "alias")),
        (_duplicate_function_yaml, ("produce", "defined more than once")),
        (_invalid_function_body_yaml, ("broken_pipeline", "invalid serialized Python syntax")),
        (_invalid_input_identifier_yaml, ("not-an-identifier", "valid Python identifier")),
    ],
    ids=[
        "missing-function",
        "unknown-target-port",
        "unknown-source-port",
        "duplicate-target",
        "wrong-link-kind",
        "unknown-runtime",
        "unknown-adapter-format",
        "duplicate-alias",
        "duplicate-function",
        "invalid-function-body",
        "invalid-input-identifier",
    ],
)
def test_registration_rejects_invalid_ir_without_persisting_rows(
    tmp_path: Path,
    yaml_factory: Callable[[], str],
    message_parts: tuple[str, ...],
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        with pytest.raises(ValueError) as raised:
            store.register_object(
                "broken_pipeline",
                "broken_pipeline",
                "default",
                yaml_text=yaml_factory(),
            )

        message = str(raised.value)
        for part in message_parts:
            assert part in message
        assert store.list_objects() == {}
        for table in ("object_versions", "object_functions", "object_pipeline_nodes", "object_pipeline_links"):
            assert store._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    finally:
        store.close()


def test_valid_function_and_pipeline_metadata_remain_compatible(tmp_path: Path) -> None:
    function = _function(
        "consume",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    function_yaml = _yaml(function)
    function_metadata = extract_metadata(function_yaml, "consume")
    assert function_metadata["kind"] == "function"
    assert function_metadata["inputs"] == [{"name": "value", "type": "str", "default": None, "required": True}]

    pipeline_yaml = _unknown_target_port_yaml().replace("port: missing", "port: value")
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "valid_pipeline",
            "broken_pipeline",
            "default",
            yaml_text=pipeline_yaml,
        )
        stored = store.get_object_version(record["version_id"], include_yaml=True)
        assert stored["yaml"] == pipeline_yaml
        assert stored["metadata"]["kind"] == "pipeline"
        assert {node["function"] for node in stored["pipeline_nodes"]} == {"produce", "consume"}
    finally:
        store.close()


def test_nested_scalar_literal_registers_and_imports_without_code_evaluation(tmp_path: Path) -> None:
    value = {
        "numbers": [1, 2, 3],
        "nested": {"enabled": True, "missing": None},
        "empty": [],
    }
    yaml_text = _scalar_pipeline_yaml(value)
    yaml_path = tmp_path / "literal.yaml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "literal_pipeline",
            "literal_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        assert record["kind"] == "pipeline"

        namespace: dict[str, object] = {}
        spl_import_from_file(yaml_path, namespace)
        imported = cast(Pipeline, namespace["literal_pipeline"])
        scalar_values = [source.value for _, source in imported.links if isinstance(source, Scalar)]
        assert scalar_values == [value]
    finally:
        store.close()


@pytest.mark.parametrize("cross_key", [False, True], ids=["same-key", "cross-key"])
def test_split_adapter_halves_register_import_round_trip_and_execute(tmp_path: Path, cross_key: bool) -> None:
    yaml_text = _split_adapter_pipeline_yaml(cross_key=cross_key)
    yaml_path = tmp_path / "split.yaml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "split_pipeline",
            "split_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        stored = store.get_object_version(record["version_id"], include_yaml=True)
        assert stored["yaml"] == yaml_text

        namespace: dict[str, object] = {}
        spl_import_from_file(yaml_path, namespace)
        imported = cast(Pipeline, namespace["split_pipeline"])
        save_adapter = imported.resolve_save_adapter(py_type=str, format="txt")
        load_type = dict if cross_key else str
        load_adapter = imported.resolve_load_adapter(py_type=load_type, format="txt")
        assert isinstance(save_adapter, SplitSaveAdapter)
        assert isinstance(load_adapter, SplitLoadAdapter)
        assert save_adapter.key == "builtins.str@txt"
        assert load_adapter.key == ("builtins.dict@txt" if cross_key else "builtins.str@txt")
        assert save_adapter.tag == "txt"
        assert load_adapter.accepted_tags == frozenset({"txt"})
        assert load_adapter.legacy_key_guard is False

        deployment = cast(Any, Deployment)
        consumer = imported.get_node_by_alias("consumer")
        run = deployment(imported).run(keep=False)
        with run:
            assert run[consumer] == {"default": "split adapter imported"}

        round_trip_path = tmp_path / "split-round-trip.yaml"
        spl_export_to_file(round_trip_path, [imported])
        round_trip_yaml = round_trip_path.read_text(encoding="utf-8")
        assert "!DSaveAdapter" in round_trip_yaml
        assert "!DLoadAdapter" in round_trip_yaml
        assert "!DAdapter\n" not in round_trip_yaml

        round_trip_namespace: dict[str, object] = {}
        spl_import_from_file(round_trip_path, round_trip_namespace)
        round_trip = cast(Pipeline, round_trip_namespace["split_pipeline"])
        round_trip_consumer = round_trip.get_node_by_alias("consumer")
        with deployment(round_trip).run(keep=False) as round_trip_run:
            assert round_trip_run[round_trip_consumer] == {"default": "split adapter imported"}
    finally:
        store.close()


def test_untyped_target_uses_sole_split_load_half_by_format(tmp_path: Path) -> None:
    yaml_text = _split_adapter_pipeline_yaml(cross_key=True, untyped_target=True)
    yaml_path = tmp_path / "untyped-split.yaml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "split_pipeline",
            "split_pipeline",
            "default",
            yaml_text=yaml_text,
        )

        namespace: dict[str, object] = {}
        spl_import_from_file(yaml_path, namespace)
        imported = cast(Pipeline, namespace["split_pipeline"])
        deployment = cast(Any, Deployment)
        with deployment(imported).run(keep=False) as run:
            assert run.value("consumer") == "split adapter imported"
    finally:
        store.close()


def test_untyped_target_rejects_ambiguous_split_load_halves_before_persistence(tmp_path: Path) -> None:
    yaml_text = _split_adapter_pipeline_yaml(
        cross_key=True,
        untyped_target=True,
        extra_load=True,
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError) as raised:
            store.register_object(
                "split_pipeline",
                "split_pipeline",
                "default",
                yaml_text=yaml_text,
            )
        message = str(raised.value)
        assert "ambiguous adapter resolution" in message
        assert "load adapters" in message
        assert "builtins.dict@txt" in message
        assert "builtins.str@txt" in message
        assert "declare a concrete port type" in message
        assert store.list_objects() == {}
        assert store._conn.execute("SELECT COUNT(*) FROM object_versions").fetchone()[0] == 0
    finally:
        store.close()


def test_any_target_preserves_source_selected_legacy_full_adapter(tmp_path: Path) -> None:
    producer = DFunction(
        name="produce_any_text",
        inputs=[],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return 'legacy any'",
    )
    consumer = DFunction(
        name="consume_any_text",
        inputs=[InputPort(name="value", typ_="Any", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return type(value).__name__ + ':' + value",
    )
    save = DFunction(
        name="save_any_text",
        inputs=[InputPort(name="path", typ_="str", default=None), InputPort(name="value", typ_="str", default=None)],
        outputs=[],
        body="from pathlib import Path\nPath(path).write_text(value, encoding='utf-8')",
    )
    load = DFunction(
        name="load_any_text",
        inputs=[InputPort(name="path", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="from pathlib import Path\nreturn Path(path).read_text(encoding='utf-8')",
    )
    pipeline = DPipeline(
        name="legacy_any_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
            DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DFormattedOutputRef(uuid=PRODUCER_ID, port="default", format="txt"),
            ]
        ],
        aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        adapters=[DAdapter(key="builtins.str@txt", save=save.name, load=load.name)],
    )
    yaml_text = yaml.dump_all(
        [[pipeline, DImportFrom(module="typing", target="Any"), producer, consumer, save, load]],
        sort_keys=False,
    )
    yaml_path = tmp_path / "legacy-any.yaml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "legacy_any_pipeline",
            "legacy_any_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        namespace: dict[str, object] = {}
        spl_import_from_file(yaml_path, namespace)
        imported = cast(Pipeline, namespace["legacy_any_pipeline"])
        deployment = cast(Any, Deployment)
        with deployment(imported).run(keep=False) as run:
            assert run.value("consumer") == "str:legacy any"
    finally:
        store.close()


@pytest.mark.parametrize(
    ("target_hint", "load_key", "load_body", "consumer_body"),
    [
        (
            "dict[str, Any]",
            "builtins.dict@txt",
            "from pathlib import Path\nreturn {'text': Path(path).read_text(encoding='utf-8')}",
            "return value['text'] + ' generic'",
        ),
        (
            "list[str]",
            "builtins.list@txt",
            "from pathlib import Path\nreturn [Path(path).read_text(encoding='utf-8')]",
            "return value[0] + ' generic'",
        ),
    ],
    ids=["dict", "list"],
)
def test_generic_target_hint_selects_outer_split_load_type(
    tmp_path: Path,
    target_hint: str,
    load_key: str,
    load_body: str,
    consumer_body: str,
) -> None:
    producer = DFunction(
        name="produce_generic_text",
        inputs=[],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return 'outer'",
    )
    consumer = DFunction(
        name="consume_generic_value",
        inputs=[InputPort(name="value", typ_=target_hint, default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body=consumer_body,
    )
    save = DFunction(
        name="save_generic_text",
        inputs=[InputPort(name="path", typ_="str", default=None), InputPort(name="value", typ_="str", default=None)],
        outputs=[],
        body="from pathlib import Path\nPath(path).write_text(value, encoding='utf-8')",
    )
    load = DFunction(
        name="load_generic_value",
        inputs=[InputPort(name="path", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_=target_hint)],
        body=load_body,
    )
    pipeline = DPipeline(
        name="generic_target_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
            DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DFormattedOutputRef(uuid=PRODUCER_ID, port="default", format="txt"),
            ]
        ],
        aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        adapters=[
            DSaveAdapter(key="builtins.str@txt", tag="txt", save=save.name),
            DLoadAdapter(key=load_key, accepted_tags=("txt",), load=load.name),
        ],
    )
    yaml_text = yaml.dump_all(
        [[pipeline, DImportFrom(module="typing", target="Any"), producer, consumer, save, load]],
        sort_keys=False,
    )
    yaml_path = tmp_path / "generic-target.yaml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "generic_target_pipeline",
            "generic_target_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        namespace: dict[str, object] = {}
        spl_import_from_file(yaml_path, namespace)
        imported = cast(Pipeline, namespace["generic_target_pipeline"])
        deployment = cast(Any, Deployment)
        with deployment(imported).run(keep=False) as run:
            assert run.value("consumer") == "outer generic"
    finally:
        store.close()


def test_cross_key_tag_mismatch_warns_at_registration_and_rejects_before_load(
    tmp_path: Path,
) -> None:
    producer = DFunction(
        name="produce_mismatched_text",
        inputs=[],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return 'mismatch'",
    )
    consumer = DFunction(
        name="consume_mismatched_value",
        inputs=[InputPort(name="value", typ_="dict", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return value['text']",
    )
    save = DFunction(
        name="save_mismatched_text",
        inputs=[InputPort(name="path", typ_="str", default=None), InputPort(name="value", typ_="str", default=None)],
        outputs=[],
        body="from pathlib import Path\nPath(path).write_text(value, encoding='utf-8')",
    )
    load = DFunction(
        name="load_must_not_run",
        inputs=[InputPort(name="path", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="dict")],
        body="raise AssertionError('load must not run')",
    )
    pipeline = DPipeline(
        name="mismatched_tag_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
            DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DFormattedOutputRef(uuid=PRODUCER_ID, port="default", format="txt"),
            ]
        ],
        aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        adapters=[
            DSaveAdapter(key="builtins.str@txt", tag="txt", save=save.name),
            DLoadAdapter(key="builtins.dict@txt", accepted_tags=("other",), load=load.name),
        ],
    )
    yaml_text = _yaml(pipeline, producer, consumer, save, load)
    yaml_path = tmp_path / "mismatched-tag.yaml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        with pytest.warns(AdapterCompatibilityWarning, match="adapter tag mismatch"):
            store.register_object(
                "mismatched_tag_pipeline",
                "mismatched_tag_pipeline",
                "default",
                yaml_text=yaml_text,
            )

        namespace: dict[str, object] = {}
        spl_import_from_file(yaml_path, namespace)
        imported = cast(Pipeline, namespace["mismatched_tag_pipeline"])
        deployment = cast(Any, Deployment)
        with pytest.raises(ValueError, match=r"artifact tag `txt`.*accepted tags: other"):
            with deployment(imported).run(keep=False) as run:
                run.value("consumer")
    finally:
        store.close()


def test_split_adapter_fanout_materializes_once_and_resolves_each_load_half(tmp_path: Path) -> None:
    producer = DFunction(
        name="fanout_producer",
        inputs=[],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return 'fanout'",
    )
    text_consumer = DFunction(
        name="consume_as_text",
        inputs=[InputPort(name="value", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return 'text:' + value",
    )
    dict_consumer = DFunction(
        name="consume_as_dict",
        inputs=[InputPort(name="value", typ_="dict", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return 'dict:' + value['text']",
    )
    save = DFunction(
        name="fanout_save",
        inputs=[InputPort(name="path", typ_="str", default=None), InputPort(name="value", typ_="str", default=None)],
        outputs=[],
        body="from pathlib import Path\nPath(path).write_text(value, encoding='utf-8')",
    )
    load_text = DFunction(
        name="fanout_load_text",
        inputs=[InputPort(name="path", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="from pathlib import Path\nreturn Path(path).read_text(encoding='utf-8')",
    )
    load_dict = DFunction(
        name="fanout_load_dict",
        inputs=[InputPort(name="path", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="dict")],
        body="from pathlib import Path\nreturn {'text': Path(path).read_text(encoding='utf-8')}",
    )
    pipeline = DPipeline(
        name="fanout_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
            DNodeFunction(uuid=CONSUMER_ID, func=text_consumer.name),
            DNodeFunction(uuid=SECOND_CONSUMER_ID, func=dict_consumer.name),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DFormattedOutputRef(uuid=PRODUCER_ID, port="default", format="txt"),
            ],
            [
                DNodeInputRef(uuid=SECOND_CONSUMER_ID, port="value"),
                DFormattedOutputRef(uuid=PRODUCER_ID, port="default", format="txt"),
            ],
        ],
        aliases=[["producer", PRODUCER_ID], ["text", CONSUMER_ID], ["mapping", SECOND_CONSUMER_ID]],
        adapters=[
            DSaveAdapter(key="builtins.str@txt", tag="txt", save=save.name),
            DLoadAdapter(key="builtins.str@txt", accepted_tags=("txt",), load=load_text.name),
            DLoadAdapter(key="builtins.dict@txt", accepted_tags=("txt",), load=load_dict.name),
        ],
    )
    yaml_path = tmp_path / "fanout.yaml"
    yaml_path.write_text(
        _yaml(pipeline, producer, text_consumer, dict_consumer, save, load_text, load_dict),
        encoding="utf-8",
    )
    namespace: dict[str, object] = {}
    spl_import_from_file(yaml_path, namespace)
    imported = cast(Pipeline, namespace["fanout_pipeline"])
    deployment = cast(Any, Deployment)
    run = deployment(imported).run(keep=False)
    with run:
        assert run[imported.get_node_by_alias("text")] == {"default": "text:fanout"}
        assert run[imported.get_node_by_alias("mapping")] == {"default": "dict:fanout"}
        assert len(run._artifact_refs) == 1

    manifest = cast(dict[str, Any], run.manifest_snapshot)
    load_keys = {
        edge["adapter"]["load"]["identity"]["key"] for edge in manifest["edges"] if edge["adapter"] is not None
    }
    assert load_keys == {"builtins.str@txt", "builtins.dict@txt"}


@pytest.mark.parametrize("untyped_target", [False, True], ids=["typed", "untyped"])
def test_cross_key_split_adapter_loads_frozen_artifact_on_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    untyped_target: bool,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    yaml_path = tmp_path / "cross-key-resume.yaml"
    yaml_path.write_text(
        _split_adapter_pipeline_yaml(cross_key=True, untyped_target=untyped_target),
        encoding="utf-8",
    )
    namespace: dict[str, object] = {}
    spl_import_from_file(yaml_path, namespace)
    pipeline = cast(Pipeline, namespace["split_pipeline"])
    deployment = cast(Any, Deployment)
    run = deployment(pipeline).run(keep=True)
    with run:
        assert run.value("consumer") == "split adapter imported"
    parent_manifest = cast(dict[str, Any], run.manifest_snapshot)
    parent_output = parent_manifest["nodes"][PRODUCER_ID]["outputs"]["default"]

    resumed = run.resume(from_="consumer", keep=True)
    with resumed:
        assert resumed.value("consumer") == "split adapter imported"
    child_manifest = cast(dict[str, Any], resumed.manifest_snapshot)
    child_output = child_manifest["nodes"][PRODUCER_ID]["outputs"]["default"]
    edge_adapter = child_manifest["edges"][0]["adapter"]

    assert child_manifest["nodes"][PRODUCER_ID]["status"] == "frozen"
    assert child_output["ref"]["sha256"] == parent_output["ref"]["sha256"]
    assert edge_adapter["save"]["identity"]["key"] == "builtins.str@txt"
    assert edge_adapter["load"]["identity"]["key"] == "builtins.dict@txt"


@pytest.mark.parametrize(("halves", "missing"), [("save", "load"), ("load", "save")])
def test_incomplete_split_adapter_is_rejected_before_persistence(
    tmp_path: Path,
    halves: str,
    missing: str,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match=rf"missing: {missing} adapter"):
            store.register_object(
                "split_pipeline",
                "split_pipeline",
                "default",
                yaml_text=_split_adapter_pipeline_yaml(halves),
            )
        assert store.list_objects() == {}
        assert store._conn.execute("SELECT COUNT(*) FROM object_versions").fetchone()[0] == 0
    finally:
        store.close()


def test_qualified_adapter_type_name_requires_exact_static_match(tmp_path: Path) -> None:
    yaml_text = _split_adapter_pipeline_yaml().replace(
        "builtins.str@txt",
        "evil.builtins.str@txt",
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match=r"unknown adapter format `txt`.*missing: save adapter"):
            store.register_object(
                "split_pipeline",
                "split_pipeline",
                "default",
                yaml_text=yaml_text,
            )
        assert store.list_objects() == {}
        assert store._conn.execute("SELECT COUNT(*) FROM object_versions").fetchone()[0] == 0
    finally:
        store.close()


@pytest.mark.parametrize("role", ["save", "load"])
def test_unused_single_split_half_registers_and_imports(tmp_path: Path, role: str) -> None:
    function = _function("adapter_callable")
    adapter: DSaveAdapter | DLoadAdapter
    if role == "save":
        adapter = DSaveAdapter(key="builtins.str@txt", tag="txt", save=function.name)
    else:
        adapter = DLoadAdapter(key="builtins.str@txt", accepted_tags=("txt",), load=function.name)
    pipeline = DPipeline(
        name="unused_half_pipeline",
        nodes=[DNodeFunction(uuid=PRODUCER_ID, func=function.name)],
        links=[],
        aliases=[["producer", PRODUCER_ID]],
        adapters=[adapter],
    )
    yaml_text = _yaml(pipeline, function)
    yaml_path = tmp_path / "unused-half.yaml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "unused_half_pipeline",
            "unused_half_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        namespace: dict[str, object] = {}
        spl_import_from_file(yaml_path, namespace)
        imported = cast(Pipeline, namespace["unused_half_pipeline"])
        assert set(imported.save_adapters) == ({"builtins.str@txt"} if role == "save" else set())
        assert set(imported.load_adapters) == ({"builtins.str@txt"} if role == "load" else set())
    finally:
        store.close()


def test_unsupported_yaml_safe_scalar_is_rejected_without_rows(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match=r"invalid scalar literal .*links\[0\]\[1\].*type `date`"):
            store.register_object(
                "literal_pipeline",
                "literal_pipeline",
                "default",
                yaml_text=_scalar_pipeline_yaml(date(2026, 7, 15)),
            )
        assert store.list_objects() == {}
        assert store._conn.execute("SELECT COUNT(*) FROM object_versions").fetchone()[0] == 0
    finally:
        store.close()


@pytest.mark.parametrize("outputs", [[], None], ids=["empty-list", "null"])
def test_zero_output_function_registers_and_imports(
    tmp_path: Path,
    outputs: list[OutputPort] | None,
) -> None:
    function = DFunction(
        name="notify",
        inputs=[],
        outputs=outputs,
        body="return None",
    )
    yaml_text = _yaml(function)
    yaml_path = tmp_path / "notify.yaml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "notify",
            "notify",
            "default",
            yaml_text=yaml_text,
        )
        assert record["outputs"] == []

        namespace: dict[str, object] = {}
        spl_import_from_file(yaml_path, namespace)
        imported = cast(Callable[[], object], namespace["notify"])
        assert imported() is None
    finally:
        store.close()


def test_registration_rejects_unknown_object_node_runtime_without_rows(tmp_path: Path) -> None:
    function = _function("produce")
    pipeline = DPipeline(
        name="runtime_pipeline",
        nodes=[DNodeFunction(uuid=PRODUCER_ID, func="produce")],
        links=[],
        aliases=[["producer", PRODUCER_ID]],
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match="unknown node runtime `missing-runtime`"):
            store.register_object(
                "runtime_pipeline",
                "runtime_pipeline",
                "default",
                yaml_text=_yaml(pipeline, function),
                runtime_config={"mode": "venv", "node_runtime": "missing-runtime"},
            )
        assert store.list_objects() == {}
    finally:
        store.close()


def test_import_backed_node_remains_registration_compatible(tmp_path: Path) -> None:
    pipeline = lift(yaml.safe_load).render("external_pipeline")
    yaml_path = tmp_path / "external.yaml"
    spl_export_to_file(yaml_path, [pipeline])
    yaml_text = yaml_path.read_text(encoding="utf-8")
    assert "!DImportFrom" in yaml_text
    assert "func: safe_load" in yaml_text

    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "external_pipeline",
            "external_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        assert record["kind"] == "pipeline"
        assert record["pipeline_nodes"][0]["function"] == "safe_load"
        assert record["pipeline_nodes"][0]["inputs"] == []
    finally:
        store.close()


def test_directory_style_spl_import_is_rejected_as_non_materializable(tmp_path: Path) -> None:
    pipeline = DPipeline(
        name="external_pipeline",
        nodes=[DNodeFunction(uuid=PRODUCER_ID, func="external")],
        links=[],
        aliases=[["external", PRODUCER_ID]],
    )
    yaml_text = yaml.dump_all(
        [[pipeline, DSPLImport(path="./external.yaml", name="external")]],
        sort_keys=False,
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match="cannot be registered as one daemon object"):
            store.register_object(
                "external_pipeline",
                "external_pipeline",
                "default",
                yaml_text=yaml_text,
            )
        assert store.list_objects() == {}
    finally:
        store.close()


def test_unsupported_extra_document_root_is_rejected(tmp_path: Path) -> None:
    yaml_text = yaml.dump_all(
        [[_function("produce")], [DScalar(value="not executable")]],
        sort_keys=False,
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match="document root must be a DFunction or DPipeline"):
            store.register_object(
                "produce",
                "produce",
                "default",
                yaml_text=yaml_text,
            )
        assert store.list_objects() == {}
    finally:
        store.close()


def _extra_pipeline_with_missing_function() -> tuple[DPipeline, list[DFunction]]:
    return (
        DPipeline(
            name="extra_pipeline",
            nodes=[DNodeFunction(uuid=PRODUCER_ID, func="missing")],
            links=[],
            aliases=[["extra", PRODUCER_ID]],
        ),
        [],
    )


def _extra_pipeline_with_bad_link() -> tuple[DPipeline, list[DFunction]]:
    consumer = _function(
        "extra_consume",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    return (
        DPipeline(
            name="extra_pipeline",
            nodes=[DNodeFunction(uuid=CONSUMER_ID, func=consumer.name)],
            links=[[DNodeInputRef(uuid=CONSUMER_ID, port="missing"), DScalar(value="x")]],
            aliases=[["extra", CONSUMER_ID]],
        ),
        [consumer],
    )


def _extra_pipeline_with_cycle() -> tuple[DPipeline, list[DFunction]]:
    left = _function(
        "extra_left",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    right = _function(
        "extra_right",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    return (
        DPipeline(
            name="extra_pipeline",
            nodes=[
                DNodeFunction(uuid=PRODUCER_ID, func=left.name),
                DNodeFunction(uuid=CONSUMER_ID, func=right.name),
            ],
            links=[
                [
                    DNodeInputRef(uuid=PRODUCER_ID, port="value"),
                    DNodeOutputRef(uuid=CONSUMER_ID, port="default"),
                ],
                [
                    DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                    DNodeOutputRef(uuid=PRODUCER_ID, port="default"),
                ],
            ],
            aliases=[["left", PRODUCER_ID], ["right", CONSUMER_ID]],
        ),
        [left, right],
    )


@pytest.mark.parametrize(
    ("extra_factory", "message"),
    [
        (_extra_pipeline_with_missing_function, "missing function `missing`"),
        (_extra_pipeline_with_bad_link, "has no input port `missing`"),
        (_extra_pipeline_with_cycle, "pipeline contains cycle: left → right → left"),
    ],
    ids=["missing-function", "bad-link", "cycle"],
)
def test_unselected_pipeline_document_is_semantically_validated(
    tmp_path: Path,
    extra_factory: Callable[[], tuple[DPipeline, list[DFunction]]],
    message: str,
) -> None:
    extra, dependencies = extra_factory()
    yaml_text = yaml.dump_all(
        [[_function("selected")], [extra, *dependencies]],
        sort_keys=False,
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match=message):
            store.register_object(
                "selected",
                "selected",
                "default",
                yaml_text=yaml_text,
            )
        assert store.list_objects() == {}
    finally:
        store.close()


def test_unselected_remote_pipelines_are_resolved_with_root_scoped_uuid_keys(tmp_path: Path) -> None:
    shared_uuid = "44444444-4444-4444-8444-444444444444"
    first = DPipeline(
        name="extra_remote_one",
        nodes=[
            DNodeRemote(
                uuid=shared_uuid,
                url="https://example.test",
                name="remote_one",
                version="1",
            )
        ],
        links=[],
        aliases=[["one", shared_uuid]],
    )
    second = DPipeline(
        name="extra_remote_two",
        nodes=[
            DNodeRemote(
                uuid=shared_uuid,
                url="https://example.test",
                name="remote_two",
                version="1",
            )
        ],
        links=[],
        aliases=[["two", shared_uuid]],
    )
    yaml_text = yaml.dump_all(
        [[_function("selected")], [first], [second]],
        sort_keys=False,
    )
    calls: list[str] = []

    def resolve(ref: dict[str, object]) -> dict[str, object]:
        calls.append(str(ref["name"]))
        return {
            "id": f"remote-{ref['name']}",
            "version_id": f"version-{ref['name']}",
            "inputs": [],
            "outputs": [],
        }

    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "selected",
            "selected",
            "default",
            yaml_text=yaml_text,
            remote_signature_resolver=resolve,
        )
        assert record["kind"] == "function"
        assert calls == ["remote_one", "remote_two"]
    finally:
        store.close()


def test_adapter_callable_must_not_be_a_module_binding(tmp_path: Path) -> None:
    function = _function("produce")
    pipeline = DPipeline(
        name="adapter_pipeline",
        nodes=[DNodeFunction(uuid=PRODUCER_ID, func="produce")],
        links=[],
        aliases=[["producer", PRODUCER_ID]],
        adapters=[DAdapter(key="builtins.str@txt", save="os", load="produce")],
    )
    yaml_text = yaml.dump_all([[pipeline, function, DImport(module="os")]], sort_keys=False)
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match="missing save function `os`"):
            store.register_object(
                "adapter_pipeline",
                "adapter_pipeline",
                "default",
                yaml_text=yaml_text,
            )
        assert store.list_objects() == {}
    finally:
        store.close()


def test_identical_import_rebinding_remains_compatible_but_conflict_is_rejected(tmp_path: Path) -> None:
    function = _function("produce")
    compatible_yaml = yaml.dump_all(
        [[function, DImport(module="numpy", alias="np"), DImport(module="numpy", alias="np")]],
        sort_keys=False,
    )
    conflicting_yaml = yaml.dump_all(
        [[function, DImport(module="numpy", alias="np"), DImport(module="pandas", alias="np")]],
        sort_keys=False,
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "compatible_import",
            "produce",
            "default",
            yaml_text=compatible_yaml,
        )
        assert record["kind"] == "function"

        with pytest.raises(ValueError, match="symbol `np` has conflicting bindings"):
            store.register_object(
                "conflicting_import",
                "produce",
                "default",
                yaml_text=conflicting_yaml,
            )
        assert "conflicting_import" not in store.list_objects()
    finally:
        store.close()


def test_full_adapter_conflicts_with_earlier_split_half(tmp_path: Path) -> None:
    function = _function("produce")
    pipeline = DPipeline(
        name="adapter_pipeline",
        nodes=[DNodeFunction(uuid=PRODUCER_ID, func="produce")],
        links=[],
        aliases=[["producer", PRODUCER_ID]],
        adapters=[
            DSaveAdapter(
                key="builtins.str@txt",
                tag="txt",
                save="produce",
            ),
            DAdapter(
                key="builtins.str@txt",
                save="produce",
                load="produce",
            ),
        ],
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match="conflicts with split adapter"):
            store.register_object(
                "adapter_pipeline",
                "adapter_pipeline",
                "default",
                yaml_text=_yaml(pipeline, function),
            )
        assert store.list_objects() == {}
    finally:
        store.close()


def test_opaque_node_still_requires_known_formatted_edge_adapter(tmp_path: Path) -> None:
    consumer = _function(
        "consume",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    pipeline = DPipeline(
        name="opaque_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func="safe_load"),
            DNodeFunction(uuid=CONSUMER_ID, func="consume"),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DFormattedOutputRef(
                    uuid=PRODUCER_ID,
                    port="opaque",
                    format="missing-format",
                ),
            ]
        ],
        aliases=[["opaque", PRODUCER_ID], ["consumer", CONSUMER_ID]],
    )
    yaml_text = yaml.dump_all(
        [[pipeline, consumer, DImportFrom(module="yaml", target="safe_load")]],
        sort_keys=False,
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match="unknown adapter format `missing-format`"):
            store.register_object(
                "opaque_pipeline",
                "opaque_pipeline",
                "default",
                yaml_text=yaml_text,
            )
        assert store.list_objects() == {}
    finally:
        store.close()


def test_multiple_legacy_pairs_defer_opaque_source_and_target_resolution(tmp_path: Path) -> None:
    pipeline = DPipeline(
        name="opaque_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func="dumps"),
            DNodeFunction(uuid=CONSUMER_ID, func="loads"),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="opaque"),
                DFormattedOutputRef(
                    uuid=PRODUCER_ID,
                    port="opaque",
                    format="blob",
                ),
            ]
        ],
        aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        adapters=[
            DAdapter(key="builtins.str@blob", save="dumps", load="loads"),
            DAdapter(key="builtins.int@blob", save="dumps", load="loads"),
        ],
    )
    yaml_text = yaml.dump_all(
        [
            [
                pipeline,
                DImportFrom(module="json", target="dumps"),
                DImportFrom(module="json", target="loads"),
            ]
        ],
        sort_keys=False,
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "opaque_pipeline",
            "opaque_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        assert record["metadata"]["links"][0]["to"]["format"] == "blob"
    finally:
        store.close()


def test_multiple_save_candidates_for_opaque_source_defer_to_runtime_type(tmp_path: Path) -> None:
    consumer = _function(
        "consume_opaque_value",
        inputs=[InputPort(name="value", typ_="str", default=None)],
    )
    pipeline = DPipeline(
        name="opaque_source_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func="safe_load"),
            DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DFormattedOutputRef(uuid=PRODUCER_ID, port="opaque", format="blob"),
            ]
        ],
        aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        adapters=[
            DSaveAdapter(key="builtins.str@blob", tag="blob", save="dumps"),
            DSaveAdapter(key="builtins.int@blob", tag="blob", save="dumps"),
            DLoadAdapter(key="builtins.str@blob", accepted_tags=("other",), load="loads"),
        ],
    )
    yaml_text = yaml.dump_all(
        [
            [
                pipeline,
                consumer,
                DImportFrom(module="yaml", target="safe_load"),
                DImportFrom(module="json", target="dumps"),
                DImportFrom(module="json", target="loads"),
            ]
        ],
        sort_keys=False,
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.warns(AdapterCompatibilityWarning, match="adapter tag mismatch"):
            record = store.register_object(
                "opaque_source_pipeline",
                "opaque_source_pipeline",
                "default",
                yaml_text=yaml_text,
            )
        assert record["metadata"]["links"][0]["to"]["format"] == "blob"
    finally:
        store.close()


def test_unqualified_custom_source_type_defers_multiple_module_candidates_to_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "sol082_alpha_payload"
    (tmp_path / f"{module_name}.py").write_text(
        "class Payload:\n    def __init__(self, value: str):\n        self.value = value\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    producer = DFunction(
        name="produce_custom_payload",
        inputs=[],
        outputs=[OutputPort(name="default", typ_="Payload")],
        body="return Payload('source-deferred')",
    )
    consumer = DFunction(
        name="consume_custom_payload_text",
        inputs=[InputPort(name="value", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return value",
    )
    save = DFunction(
        name="save_custom_payload",
        inputs=[
            InputPort(name="path", typ_="str", default=None),
            InputPort(name="value", typ_="Payload", default=None),
        ],
        outputs=[],
        body="from pathlib import Path\nPath(path).write_text(value.value, encoding='utf-8')",
    )
    load = DFunction(
        name="load_custom_payload_text",
        inputs=[InputPort(name="path", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="from pathlib import Path\nreturn Path(path).read_text(encoding='utf-8')",
    )
    pipeline = DPipeline(
        name="custom_source_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
            DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DFormattedOutputRef(uuid=PRODUCER_ID, port="default", format="txt"),
            ]
        ],
        aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        adapters=[
            DSaveAdapter(key=f"{module_name}.Payload@txt", tag="txt", save=save.name),
            DSaveAdapter(key="sol082_beta_payload.Payload@txt", tag="txt", save=save.name),
            DLoadAdapter(key="builtins.str@txt", accepted_tags=("txt",), load=load.name),
        ],
    )
    yaml_text = yaml.dump_all(
        [
            [
                pipeline,
                DImportFrom(module=module_name, target="Payload"),
                producer,
                consumer,
                save,
                load,
            ]
        ],
        sort_keys=False,
    )
    yaml_path = tmp_path / "custom-source.yaml"
    yaml_path.write_text(yaml_text, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon")
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "custom_source_pipeline",
            "custom_source_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        namespace: dict[str, object] = {}
        spl_import_from_file(yaml_path, namespace)
        imported = cast(Pipeline, namespace["custom_source_pipeline"])
        deployment = cast(Any, Deployment)
        with deployment(imported).run(keep=False) as run:
            assert run.value("consumer") == "source-deferred"
    finally:
        store.close()


def test_unqualified_custom_target_type_rejects_multiple_module_candidates(tmp_path: Path) -> None:
    producer = _function("produce_target_ambiguity")
    consumer = DFunction(
        name="consume_target_ambiguity",
        inputs=[InputPort(name="value", typ_="Payload", default=None)],
        outputs=[OutputPort(name="default", typ_="str")],
        body="return 'unused'",
    )
    save = DFunction(
        name="save_target_ambiguity",
        inputs=[InputPort(name="path", typ_="str", default=None), InputPort(name="value", typ_="str", default=None)],
        outputs=[],
        body="from pathlib import Path\nPath(path).write_text(value, encoding='utf-8')",
    )
    load = DFunction(
        name="load_target_ambiguity",
        inputs=[InputPort(name="path", typ_="str", default=None)],
        outputs=[OutputPort(name="default", typ_="Payload")],
        body="return None",
    )
    pipeline = DPipeline(
        name="custom_target_pipeline",
        nodes=[
            DNodeFunction(uuid=PRODUCER_ID, func=producer.name),
            DNodeFunction(uuid=CONSUMER_ID, func=consumer.name),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DFormattedOutputRef(uuid=PRODUCER_ID, port="default", format="txt"),
            ]
        ],
        aliases=[["producer", PRODUCER_ID], ["consumer", CONSUMER_ID]],
        adapters=[
            DSaveAdapter(key="builtins.str@txt", tag="txt", save=save.name),
            DLoadAdapter(
                key="sol082_alpha_payload.Payload@txt",
                accepted_tags=("txt",),
                load=load.name,
            ),
            DLoadAdapter(
                key="sol082_beta_payload.Payload@txt",
                accepted_tags=("txt",),
                load=load.name,
            ),
        ],
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError) as raised:
            store.register_object(
                "custom_target_pipeline",
                "custom_target_pipeline",
                "default",
                yaml_text=_yaml(pipeline, producer, consumer, save, load),
            )
        message = str(raised.value)
        assert "ambiguous adapter resolution" in message
        assert "load adapters for `Payload`" in message
        assert "sol082_alpha_payload.Payload@txt" in message
        assert "sol082_beta_payload.Payload@txt" in message
        assert store.list_objects() == {}
    finally:
        store.close()


def test_missing_adapter_callable_and_self_import_are_rejected(tmp_path: Path) -> None:
    function = _function("produce")
    pipeline = DPipeline(
        name="adapter_pipeline",
        nodes=[DNodeFunction(uuid=PRODUCER_ID, func="produce")],
        links=[],
        aliases=[["producer", PRODUCER_ID]],
        adapters=[
            DAdapter(
                key="builtins.str@txt",
                save="missing_save",
                load="produce",
            )
        ],
    )
    cases = [
        (_yaml(pipeline, function), "missing save function `missing_save`"),
        (
            yaml.dump_all([[pipeline, function, DSPLSelfImport(name="missing_load")]], sort_keys=False),
            "missing object symbol",
        ),
    ]
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        for yaml_text, expected in cases:
            with pytest.raises(ValueError, match=expected):
                store.register_object(
                    "adapter_pipeline",
                    "adapter_pipeline",
                    "default",
                    yaml_text=yaml_text,
                )
        assert store.list_objects() == {}
    finally:
        store.close()


def test_mid_registration_failure_rolls_back_every_aggregate_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        def fail_decomposition(**_: object) -> None:
            raise RuntimeError("injected decomposition failure")

        monkeypatch.setattr(store.objects, "_store_object_decomposition_locked", fail_decomposition)
        with pytest.raises(RuntimeError, match="injected decomposition failure"):
            store.register_object(
                "atomic_pipeline",
                "broken_pipeline",
                "default",
                yaml_text=_unknown_target_port_yaml().replace("port: missing", "port: value"),
            )

        assert store.list_objects() == {}
        for table in (
            "objects",
            "object_versions",
            "object_functions",
            "object_pipeline_nodes",
            "object_pipeline_links",
        ):
            assert store._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    finally:
        store.close()


def test_display_cache_failure_is_honest_success_after_atomic_database_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        def fail_cache(_: Path, __: str) -> None:
            raise RuntimeError("injected cache failure")

        monkeypatch.setattr(store.objects, "_write_object_yaml_cache", fail_cache)
        record = store.register_object(
            "cache_function",
            "produce",
            "default",
            yaml_text=_yaml(_function("produce")),
        )

        assert store.get_object_version(record["version_id"], include_yaml=True)["yaml"]
        assert "object YAML compatibility cache was not written" in caplog.text
    finally:
        store.close()


def test_display_cache_never_follows_symlinked_parent(tmp_path: Path) -> None:
    daemon_home = tmp_path / "daemon"
    outside = tmp_path / "outside"
    outside.mkdir()
    store = RegistryStore(daemon_home)
    try:
        store.register_env("default", sys.executable)
        (store.objects_dir / "local").symlink_to(outside, target_is_directory=True)

        record = store.register_object(
            "symlink_function",
            "produce",
            "default",
            yaml_text=_yaml(_function("produce")),
        )

        assert record["name"] == "symlink_function"
        assert list(outside.rglob("*")) == []
    finally:
        store.close()


def _remote_pipeline_yaml() -> str:
    remote_id = "33333333-3333-4333-8333-333333333333"
    return _yaml(
        DPipeline(
            name="remote_pipeline",
            nodes=[
                DNodeRemote(
                    uuid=remote_id,
                    url="https://example.test",
                    name="remote_fn",
                    version="1",
                )
            ],
            links=[],
            aliases=[["remote", remote_id]],
        )
    )


@pytest.mark.parametrize(
    ("signature", "message"),
    [
        ({"inputs": []}, "signature is missing `outputs`"),
        ({"inputs": [{"type": "str"}], "outputs": []}, "input #0 must declare a non-empty name"),
        ({"inputs": [], "outputs": [{"type": "str"}]}, "output #0 must declare a non-empty name"),
        (
            {
                "inputs": [],
                "outputs": [
                    {
                        "name": "result",
                        "ports": [
                            {"name": "duplicate", "type": "str"},
                            {"name": "duplicate", "type": "str"},
                        ],
                    }
                ],
            },
            "output port `duplicate` more than once",
        ),
        (
            {"inputs": [], "outputs": [{"name": "result", "ports": []}]},
            "ports must be a non-empty list",
        ),
        (
            {
                "inputs": [],
                "outputs": [
                    {
                        "name": "first",
                        "selector": "first",
                        "ports": [{"name": "left", "type": "str"}],
                    },
                    {
                        "name": "second",
                        "selector": "second",
                        "ports": [{"name": "right", "type": "str"}],
                    },
                ],
            },
            "multiple selectable outputs .*`first`, `second`",
        ),
    ],
    ids=[
        "missing-outputs",
        "missing-input-name",
        "missing-output-name",
        "duplicate-output-port",
        "empty-output-port-list",
        "multiple-selectors",
    ],
)
def test_malformed_remote_signature_is_rejected_without_rows(
    tmp_path: Path,
    signature: dict[str, object],
    message: str,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        with pytest.raises(ValueError, match=message):
            store.register_object(
                "remote_pipeline",
                "remote_pipeline",
                "default",
                yaml_text=_remote_pipeline_yaml(),
                remote_signature_resolver=lambda _: signature,
            )
        assert store.list_objects() == {}
    finally:
        store.close()


def test_runtime_registration_stages_remote_cache_until_atomic_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SignatureServer:
        def __init__(self) -> None:
            self.signature: dict[str, object] = {}

        def object_signature(
            self,
            object_name: str,
            *,
            version: int | None = None,
            owner_id: str | None = None,
            library: str | None = None,
            function: str | None = None,
        ) -> dict[str, object]:
            del object_name, version, owner_id, library, function
            return dict(self.signature)

    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    try:
        store.register_env("default", sys.executable)
        cache_ref = {
            "server_url": "https://example.test",
            "owner_id": "owner-1",
            "object_name": "remote_fn",
            "version": "1",
        }
        store.save_remote_signature(
            cache_ref,
            {"inputs": [], "outputs": []},
            status="unavailable",
            error="baseline failure",
        )
        baseline_cache = store.list_remote_signatures()

        server = SignatureServer()
        runtime = DaemonRuntime(store, auto_build_envs=False)
        monkeypatch.setattr(
            runtime,
            "_credentials_for_remote_ref",
            lambda _ref: {"server_url": "https://example.test", "owner_id": "owner-1"},
        )
        monkeypatch.setattr(
            runtime,
            "_server_client_for_credentials",
            lambda _credentials, **_kwargs: server,
        )

        server.signature = {"owner_id": "owner-1", "inputs": []}
        with pytest.raises(ValueError, match="signature is missing `outputs`"):
            runtime.register_object(
                "remote_pipeline",
                "remote_pipeline",
                "default",
                yaml_text=_remote_pipeline_yaml(),
            )
        assert store.list_remote_signatures() == baseline_cache
        assert store.list_objects() == {}

        server.signature = {
            "id": "remote-object",
            "version_id": "remote-version",
            "owner_id": "owner-1",
            "inputs": [],
            "outputs": [],
        }
        original_store_decomposition = store.objects._store_object_decomposition_locked

        def fail_decomposition(**_: object) -> None:
            raise RuntimeError("injected post-cache failure")

        monkeypatch.setattr(store.objects, "_store_object_decomposition_locked", fail_decomposition)
        with pytest.raises(RuntimeError, match="injected post-cache failure"):
            runtime.register_object(
                "remote_pipeline",
                "remote_pipeline",
                "default",
                yaml_text=_remote_pipeline_yaml(),
            )
        assert store.list_remote_signatures() == baseline_cache
        assert store.list_objects() == {}
        for table in ("object_versions", "object_pipeline_nodes", "object_pipeline_links"):
            assert store._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0

        monkeypatch.setattr(
            store.objects,
            "_store_object_decomposition_locked",
            original_store_decomposition,
        )
        record = runtime.register_object(
            "remote_pipeline",
            "remote_pipeline",
            "default",
            yaml_text=_remote_pipeline_yaml(),
        )
        assert record["kind"] == "pipeline"
        cache = store.list_remote_signatures()
        assert len(cache) == 1
        assert cache[0]["status"] == "resolved"
        assert cache[0]["signature"]["outputs"] == []
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_explicit_zero_output_remote_signature_stays_zero_output(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "remote_pipeline",
            "remote_pipeline",
            "default",
            yaml_text=_remote_pipeline_yaml(),
            remote_signature_resolver=lambda _: {
                "id": "remote-object",
                "version_id": "remote-version",
                "inputs": [],
                "outputs": [],
            },
        )
        assert record["pipeline_nodes"][0]["outputs"] == []
    finally:
        store.close()


def test_remote_signature_transport_diagnostics_do_not_change_identity(tmp_path: Path) -> None:
    yaml_text = _remote_pipeline_yaml()
    base_signature = {
        "id": "remote-object",
        "version_id": "remote-version",
        "inputs": [],
        "outputs": [{"name": "default", "type": "str"}],
    }
    signatures = iter(
        [
            {**base_signature, "resolution": {"source": "server"}, "resolved_from": {"name": "alias"}},
            {
                **base_signature,
                "cache_status": "stale",
                "cache_error": "network error with transient details",
                "resolution": {"source": "cache"},
                "resolved_from": {"name": "other-alias"},
            },
        ]
    )
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        first = store.register_object(
            "remote_pipeline",
            "remote_pipeline",
            "default",
            yaml_text=yaml_text,
            remote_signature_resolver=lambda _: next(signatures),
        )
        second = store.register_object(
            "remote_pipeline",
            "remote_pipeline",
            "default",
            yaml_text=yaml_text,
            remote_signature_resolver=lambda _: next(signatures),
        )

        assert second["version_id"] == first["version_id"]
        assert len(store.list_object_versions("remote_pipeline")) == 1
    finally:
        store.close()


def test_semantic_uuid_reference_spellings_resolve_without_rewriting_yaml(tmp_path: Path) -> None:
    lower_producer = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    upper_producer = lower_producer.upper()
    producer = _function("produce")
    consumer = _function("consume", inputs=[InputPort(name="value", typ_="str", default=None)])
    pipeline = DPipeline(
        name="case_pipeline",
        nodes=[
            DNodeFunction(uuid=upper_producer, func="produce"),
            DNodeFunction(uuid=CONSUMER_ID, func="consume"),
        ],
        links=[
            [
                DNodeInputRef(uuid=CONSUMER_ID, port="value"),
                DNodeOutputRef(uuid=lower_producer, port="default"),
            ]
        ],
        aliases=[["producer", lower_producer], ["consumer", CONSUMER_ID]],
        tags={lower_producer: {"runtime": "native"}},
    )
    yaml_text = _yaml(pipeline, producer, consumer)
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "case_pipeline",
            "case_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        assert record["yaml_sha256"]
        assert record["pipeline_nodes"][0]["outputs"] == [{"name": "default", "type": "str"}]
        stored = store.get_object_version(record["version_id"], include_yaml=True)
        assert stored["yaml"] == yaml_text
        metadata = stored["metadata"]
        node_ids = {node["id"] for node in metadata["pipeline_nodes"]}
        assert upper_producer in node_ids
        assert {alias["node_id"] for alias in metadata["aliases"]} <= node_ids
        assert {output["node_id"] for output in metadata["outputs"]} <= node_ids
        for link in metadata["links"]:
            assert link["from"]["node_id"] in node_ids
            if link["to"]["kind"] == "node_output":
                assert link["to"]["node_id"] in node_ids
        decomposition = store.get_object_decomposition(record["version_id"])
        decomposition_node_ids = {node["node_id"] for node in decomposition["nodes"]}
        assert {link["target_node_id"] for link in decomposition["links"]} <= decomposition_node_ids
        assert {link["source_node_id"] for link in decomposition["links"]} <= decomposition_node_ids
    finally:
        store.close()
