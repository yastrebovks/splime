from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
import yaml

from spl.core.entities.function import DFunction
from spl.core.entities.node import (
    DNodeInputRef,
    DNodeOutputRef,
    InputPort,
    Node,
    NodeInputRef,
    NodeOutputRef,
    OutputPort,
)
from spl.core.entities.node_function import DNodeFunction, NodeFunction
from spl.core.entities.pipeline import DPipeline, Pipeline, validate_pipeline_ir
from spl.core.ir.parse import ir_parse
from spl.core.ir.unparse import ir_unparse
from spl.core.ir.utils import spl_import_from_file


def _identity(value: int) -> int:
    return value


def _increment(value: int) -> int:
    return value + 1


def _uuid(index: int) -> UUID:
    return UUID("00000000-0000-0000-0000-{:012d}".format(index))


def _node(index: int, *input_names: str) -> Node:
    names = input_names or ("in",)
    return Node(
        uuid=_uuid(index),
        inputs=[InputPort(name, "int", None) for name in names],
        outputs=[OutputPort("out", "int")],
    )


def _link(source: Node, target: Node, target_port: str = "in") -> tuple[NodeInputRef, NodeOutputRef]:
    return (
        NodeInputRef(target, target.get_input_port(target_port)),
        NodeOutputRef(source, source.get_output_port("out")),
    )


def _duplicate_ir() -> DPipeline:
    node_id = str(_uuid(50))
    return DPipeline(
        name="duplicate_pipeline",
        nodes=[
            DNodeFunction(uuid=node_id, func="identity"),
            DNodeFunction(uuid=node_id, func="increment"),
        ],
        links=[],
        aliases=[["left", node_id], ["right", node_id]],
    )


def _function_ir(name: str, body: str) -> DFunction:
    return DFunction(
        name=name,
        body=body,
        inputs=[InputPort("value", "int", None)],
        outputs=[OutputPort("default", "int")],
    )


def test_pipeline_construction_rejects_duplicate_uuid_with_aliases_and_locations() -> None:
    shared_uuid = _uuid(50)
    left = NodeFunction(_identity, uuid=shared_uuid)
    right = NodeFunction(_increment, uuid=shared_uuid)

    with pytest.raises(ValueError) as raised:
        Pipeline(nodes={right, left}, aliases={"right": right, "left": left})

    message = str(raised.value)
    assert "duplicate node uuid `{}`".format(shared_uuid) in message
    assert "aliases: `left`, `right`" in message
    assert message.count(Path(__file__).name) == 2


def test_many_aliases_for_the_same_node_object_remain_valid() -> None:
    node = _node(1)

    pipeline = Pipeline(nodes={node}, aliases={"first": node, "second": node})

    assert pipeline.aliases == {"first": node, "second": node}


def test_duplicate_diagnostic_resolves_equal_but_distinct_alias_references() -> None:
    shared_uuid = _uuid(50)
    left = NodeFunction(_identity, uuid=shared_uuid)
    right = NodeFunction(_increment, uuid=shared_uuid)
    left_alias_ref = NodeFunction(_identity, uuid=shared_uuid)
    right_alias_ref = NodeFunction(_increment, uuid=shared_uuid)

    with pytest.raises(ValueError) as raised:
        Pipeline(
            nodes={left, right},
            aliases={"left": left_alias_ref, "right": right_alias_ref},
        )

    assert "aliases: `left`, `right`" in str(raised.value)


def test_pipeline_construction_compares_semantically_canonical_uuid_values() -> None:
    upper = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
    lower = upper.lower()
    left = NodeFunction(_identity, uuid=cast(UUID, upper))
    right = NodeFunction(_increment, uuid=cast(UUID, lower))

    with pytest.raises(ValueError) as raised:
        Pipeline(nodes={left, right}, aliases={"left": left, "right": right})

    assert "duplicate node uuid `aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa`" in str(raised.value)
    assert "aliases: `left`, `right`" in str(raised.value)


def test_ir_parse_revalidates_mutated_pipeline_before_serialization() -> None:
    shared_uuid = _uuid(50)
    left = NodeFunction(_identity, uuid=shared_uuid)
    right = NodeFunction(_increment, uuid=shared_uuid)
    pipeline = Pipeline(nodes={left}, aliases={"left": left})
    pipeline.nodes.add(right)
    pipeline.aliases["right"] = right

    with pytest.raises(ValueError, match="duplicate node uuid") as raised:
        ir_parse(pipeline)

    assert "aliases: `left`, `right`" in str(raised.value)


def test_duplicate_uuid_is_rejected_before_ir_unparse_and_yaml_import(tmp_path: Path) -> None:
    pipeline = _duplicate_ir()
    source = tmp_path / "duplicate.yaml"
    source.write_text(
        yaml.dump_all(
            [[pipeline, _function_ir("identity", "return value"), _function_ir("increment", "return value + 1")]],
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as direct_error:
        validate_pipeline_ir(pipeline, source=source)
    with pytest.raises(ValueError) as unparse_error:
        list(ir_unparse(pipeline, source=source))
    with pytest.raises(ValueError) as import_error:
        spl_import_from_file(source, {})

    expected = "aliases: `left`, `right`; locations: `{0}:pipeline.nodes[0]`, `{0}:pipeline.nodes[1]`".format(source)
    assert expected in str(direct_error.value)
    assert str(unparse_error.value) == str(direct_error.value)
    assert str(import_error.value) == str(direct_error.value)


def test_ir_duplicate_check_ignores_malformed_alias_records_for_later_validation() -> None:
    pipeline = _duplicate_ir()
    pipeline.aliases.append(["malformed", "too", "long"])

    with pytest.raises(ValueError, match="duplicate node uuid") as raised:
        validate_pipeline_ir(pipeline)

    assert "aliases: `left`, `right`" in str(raised.value)


def test_ir_duplicate_diagnostic_lists_every_alias_for_the_uuid() -> None:
    pipeline = _duplicate_ir()
    pipeline.aliases.append(["middle", pipeline.nodes[0].uuid])

    with pytest.raises(ValueError) as raised:
        validate_pipeline_ir(pipeline)

    assert "aliases: `left`, `middle`, `right`" in str(raised.value)


def test_semantically_equal_uuid_spellings_are_rejected_before_yaml_import(tmp_path: Path) -> None:
    upper = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
    lower = upper.lower()
    pipeline = DPipeline(
        name="semantic_duplicate",
        nodes=[
            DNodeFunction(uuid=upper, func="identity"),
            DNodeFunction(uuid=lower, func="increment"),
        ],
        links=[],
        aliases=[["left", upper], ["right", lower]],
    )
    source = tmp_path / "semantic-duplicate.yaml"
    source.write_text(
        yaml.dump_all(
            [[pipeline, _function_ir("identity", "return value"), _function_ir("increment", "return value + 1")]],
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as raised:
        spl_import_from_file(source, {})

    message = str(raised.value)
    assert "duplicate node uuid `aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa`" in message
    assert "aliases: `left`, `right`" in message


def test_pipeline_construction_rejects_self_loop_with_readable_path() -> None:
    node = _node(1)

    with pytest.raises(ValueError, match="^pipeline contains cycle: self → self$"):
        Pipeline(nodes={node}, links={_link(node, node)}, aliases={"self": node})


def test_pipeline_construction_rejects_two_node_cycle_with_readable_path() -> None:
    left = _node(1)
    right = _node(2)

    with pytest.raises(ValueError, match="^pipeline contains cycle: left → right → left$"):
        Pipeline(
            nodes={right, left},
            links={_link(right, left), _link(left, right)},
            aliases={"right": right, "left": left},
        )


def test_pipeline_construction_reports_nested_cycle_without_acyclic_prefix() -> None:
    entry = _node(1)
    bridge = _node(2)
    left = _node(3, "entry", "loop")
    right = _node(4)

    with pytest.raises(ValueError, match="^pipeline contains cycle: left → right → left$"):
        Pipeline(
            nodes={right, left, bridge, entry},
            links={
                _link(entry, bridge),
                _link(bridge, left, "entry"),
                _link(left, right),
                _link(right, left, "loop"),
            },
            aliases={"entry": entry, "bridge": bridge, "left": left, "right": right},
        )


def test_external_node_reusing_internal_uuid_reports_unknown_node_not_false_cycle() -> None:
    left = _node(1)
    right = _node(2)
    external_right = Node(
        uuid=right.uuid,
        inputs=[InputPort("different", "int", None)],
        outputs=[OutputPort("out", "int")],
    )
    pipeline = Pipeline(
        nodes={left, right},
        links={
            _link(left, right),
            (
                NodeInputRef(left, left.get_input_port("in")),
                NodeOutputRef(external_right, external_right.get_output_port("out")),
            ),
        },
        aliases={"left": left, "right": right},
    )

    with pytest.raises(ValueError, match="link source node is not in pipeline"):
        pipeline._validate_consistency()


def test_cycle_validation_uses_the_same_equality_membership_as_endpoint_checks() -> None:
    left = _node(1)
    right = _node(2)
    equal_left = _node(1)
    equal_right = _node(2)
    assert equal_left == left and equal_left is not left
    assert equal_right == right and equal_right is not right

    with pytest.raises(ValueError, match="^pipeline contains cycle: left → right → left$"):
        Pipeline(
            nodes={left, right},
            links={_link(equal_left, equal_right), _link(equal_right, equal_left)},
            aliases={"left": left, "right": right},
        )


def test_ir_cycle_selection_is_deterministic_for_multiple_cycles(tmp_path: Path) -> None:
    ids = [str(_uuid(index)) for index in range(1, 5)]
    nodes = [DNodeFunction(uuid=node_id, func="identity") for node_id in ids]
    links = [
        [DNodeInputRef(uuid=ids[1], port="value"), DNodeOutputRef(uuid=ids[0], port="default")],
        [DNodeInputRef(uuid=ids[0], port="value"), DNodeOutputRef(uuid=ids[1], port="default")],
        [DNodeInputRef(uuid=ids[3], port="value"), DNodeOutputRef(uuid=ids[2], port="default")],
        [DNodeInputRef(uuid=ids[2], port="value"), DNodeOutputRef(uuid=ids[3], port="default")],
    ]
    aliases = [[alias, node_id] for alias, node_id in zip(("a", "b", "c", "d"), ids, strict=True)]
    first = DPipeline(name="multi", nodes=nodes, links=links, aliases=aliases)
    second = DPipeline(name="multi", nodes=list(reversed(nodes)), links=list(reversed(links)), aliases=aliases)

    messages = []
    for pipeline in (first, second):
        with pytest.raises(ValueError) as raised:
            validate_pipeline_ir(pipeline, source=tmp_path / "multi.yaml")
        messages.append(str(raised.value))

    assert messages == ["pipeline contains cycle: a → b → a"] * 2


def test_ir_dag_validation_is_iterative_for_deep_graphs() -> None:
    node_count = 2_500
    node_ids = ["00000000-0000-0000-0000-{:012d}".format(index) for index in range(node_count)]
    pipeline = DPipeline(
        name="deep_dag",
        nodes=[DNodeFunction(uuid=node_id, func="identity") for node_id in reversed(node_ids)],
        links=[
            [DNodeInputRef(uuid=target, port="value"), DNodeOutputRef(uuid=source, port="default")]
            for source, target in zip(node_ids[:-1], node_ids[1:], strict=True)
        ],
        aliases=[],
    )

    assert validate_pipeline_ir(pipeline) is pipeline
