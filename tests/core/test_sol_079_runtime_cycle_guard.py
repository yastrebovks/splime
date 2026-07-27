from typing import Any
from uuid import UUID

import pytest

from spl.core._common import Run
from spl.core.entities.node import InputPort, Node, NodeOutputRef, OutputPort
from spl.core.entities.pipeline import Pipeline


def _unused_callback(node: Node, kwargs: dict[InputPort, Any]) -> dict[str, Any]:
    del node, kwargs
    return {"default": 1}


def _node(number: int) -> Node:
    return Node(
        inputs=[InputPort("value", "int", None)],
        outputs=[OutputPort("default", "int")],
        uuid=UUID(int=number),
    )


def test_runtime_cycle_guard_rejects_validation_bypass_with_readable_path() -> None:
    left = _node(1)
    right = _node(2)
    pipeline = Pipeline(nodes={left, right}, aliases={"left": left, "right": right})
    run = Run(_unused_callback, pipeline, keep=False)

    # Simulate a malformed graph entering after construction through an internal
    # compatibility seam. Construction-time validation remains the primary gate.
    run._deps = {
        left: {left.inputs[0]: NodeOutputRef(right, right.outputs[0])},
        right: {right.inputs[0]: NodeOutputRef(left, left.outputs[0])},
    }

    with pytest.raises(RuntimeError) as exc_info:
        run[left]

    assert type(exc_info.value) is RuntimeError
    assert str(exc_info.value) == "splime pipeline execution cycle detected: left → right → left"
    assert run._visiting_nodes == []
    assert run._visiting_node_set == set()
