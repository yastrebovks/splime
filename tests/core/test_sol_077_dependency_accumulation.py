import json
import os
import random
import subprocess
import sys
import textwrap
from collections.abc import Iterator
from typing import Any
from uuid import UUID

import pytest

from spl.core._common import Run, _accumulate_pipeline_dependencies
from spl.core.entities.node import InputPort, Node, NodeInputRef
from spl.core.entities.pipeline import Pipeline
from spl.core.entities.scalar import Scalar


def _unused_callback(node: Node, kwargs: dict[InputPort, Any]) -> dict[str, Any]:
    del node, kwargs
    return {}


def _dependency_items(run: Run) -> Iterator[tuple[NodeInputRef, Any]]:
    for node, port_values in run._deps.items():
        for port, value in port_values.items():
            yield NodeInputRef(node, port), value


def _force_equal_hashes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Node, "__hash__", lambda _node: 7)
    monkeypatch.setattr(InputPort, "__hash__", lambda _port: 11)


def test_equal_hash_nodes_preserve_all_twenty_five_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    _force_equal_hashes(monkeypatch)
    ports = [InputPort(name="p{}".format(index), typ_="int", default=None) for index in range(5)]
    nodes = [Node(inputs=ports, outputs=[], uuid=UUID(int=index + 1)) for index in range(5)]
    value = Scalar(1)
    interleaved_links = [(NodeInputRef(node, ports[port_index]), value) for port_index in range(5) for node in nodes]
    pipeline = Pipeline(nodes=set(nodes), links=set(interleaved_links))._validate_consistency()

    run = Run(_unused_callback, pipeline, keep=False)

    assert len(pipeline.links) == 25
    assert list(run._deps) == nodes
    assert [list(port_values) for port_values in run._deps.values()] == [ports] * 5
    assert set(_dependency_items(run)) == pipeline.links


def test_generated_collision_heavy_pipelines_preserve_every_declared_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _force_equal_hashes(monkeypatch)
    for seed in range(40):
        random_source = random.Random(seed)
        node_count = random_source.randint(1, 9)
        nodes: list[Node] = []
        links: list[tuple[NodeInputRef, Scalar]] = []
        for node_index in range(node_count):
            ports = [
                InputPort(name="input_{:02d}".format(port_index), typ_="int", default=None)
                for port_index in range(random_source.randint(1, 7))
            ]
            node = Node(inputs=ports, outputs=[], uuid=UUID(int=(seed + 1) * 100 + node_index))
            nodes.append(node)
            for port in random_source.sample(ports, k=random_source.randint(1, len(ports))):
                links.append((NodeInputRef(node, port), Scalar((seed, node_index, port.name))))
        random_source.shuffle(links)
        pipeline = Pipeline(nodes=set(nodes), links=set(links))._validate_consistency()

        run = Run(_unused_callback, pipeline, keep=False)

        actual = list(_dependency_items(run))
        assert len(actual) == len(pipeline.links)
        assert set(actual) == pipeline.links
        assert list(run._deps) == sorted(run._deps, key=lambda node: str(node.uuid))
        for port_values in run._deps.values():
            assert list(port_values) == sorted(port_values, key=lambda port: port.name)


def test_dependency_order_uses_target_uuid_then_input_port_name() -> None:
    low_uuid = Node(
        inputs=[InputPort("zeta", "int", None), InputPort("alpha", "int", None)],
        outputs=[],
        uuid=UUID(int=1),
    )
    high_uuid = Node(
        inputs=[InputPort("middle", "int", None), InputPort("beta", "int", None)],
        outputs=[],
        uuid=UUID(int=2),
    )
    links = {
        (NodeInputRef(high_uuid, high_uuid.inputs[0]), Scalar(1)),
        (NodeInputRef(low_uuid, low_uuid.inputs[0]), Scalar(2)),
        (NodeInputRef(high_uuid, high_uuid.inputs[1]), Scalar(3)),
        (NodeInputRef(low_uuid, low_uuid.inputs[1]), Scalar(4)),
    }

    run = Run(_unused_callback, Pipeline(nodes={high_uuid, low_uuid}, links=links), keep=False)

    assert list(run._deps) == [low_uuid, high_uuid]
    assert [[port.name for port in values] for values in run._deps.values()] == [
        ["alpha", "zeta"],
        ["beta", "middle"],
    ]


def test_dependency_order_has_stable_tiebreakers_for_duplicate_port_names() -> None:
    no_type = InputPort("value", None, None)
    integer = InputPort("value", "int", None)
    integer_default = InputPort("value", "int", "0")
    string = InputPort("value", "str", None)
    node = Node(inputs=[string, integer_default, no_type, integer], outputs=[], uuid=UUID(int=1))
    links = {(NodeInputRef(node, port), Scalar(port.typ_)) for port in (string, integer_default, no_type, integer)}

    run = Run(_unused_callback, Pipeline(nodes={node}, links=links), keep=False)

    assert list(run._deps[node]) == [no_type, integer, integer_default, string]


def test_duplicate_port_name_order_is_identical_across_hash_seeds() -> None:
    script = textwrap.dedent(
        """
        import json
        from uuid import UUID

        from spl.core._common import Run
        from spl.core.entities.node import InputPort, Node, NodeInputRef
        from spl.core.entities.pipeline import Pipeline
        from spl.core.entities.scalar import Scalar

        integer = InputPort("value", "int", None)
        string = InputPort("value", "str", None)
        node = Node(inputs=[string, integer], outputs=[], uuid=UUID(int=1))
        pipeline = Pipeline(
            nodes={node},
            links={
                (NodeInputRef(node, integer), Scalar("integer")),
                (NodeInputRef(node, string), Scalar("string")),
            },
        )
        run = Run(lambda _node, _kwargs: {}, pipeline, keep=False)
        print(json.dumps([(port.typ_, value.value) for port, value in run._deps[node].items()]))
        """
    )
    results = []
    for seed in ("0", "1", "4", "random"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        output = subprocess.check_output([sys.executable, "-c", script], env=env, text=True)
        results.append(json.loads(output))

    assert results == [[["int", "integer"], ["str", "string"]]] * 4


def test_dependency_accumulation_rejects_duplicate_input_when_validation_is_bypassed() -> None:
    port = InputPort("value", "int", None)
    node = Node(inputs=[port], outputs=[], uuid=UUID(int=1))
    node_input_ref = NodeInputRef(node, port)
    pipeline = Pipeline(nodes={node})
    object.__setattr__(
        pipeline,
        "links",
        {(node_input_ref, Scalar("first")), (node_input_ref, Scalar("second"))},
    )

    with pytest.raises(ValueError, match=r"pipeline input `.*:value` is linked more than once"):
        _accumulate_pipeline_dependencies(pipeline)
