from __future__ import annotations

from spl import lift
from spl.core.entities.function import serialize_function


def test_serialize_function_accepts_nested_definition() -> None:
    # Regression: ``inspect.getsource`` keeps the original indentation, so a
    # function defined inside a block (a notebook ``if RUN_*:`` guard, another
    # function body) failed with ``IndentationError: unexpected indent``
    # before the ``textwrap.dedent`` fix.
    if True:
        def nested_probe(x: int, factor: float = 1.5) -> float:
            return x * factor

    payload = serialize_function(nested_probe)
    assert payload.name == 'nested_probe'
    assert [port.name for port in payload.inputs] == ['x', 'factor']
    assert payload.inputs[0].typ_ == 'int'
    assert payload.inputs[1].default == '1.5'
    assert 'return x * factor' in payload.body


def test_lift_accepts_nested_definition() -> None:
    # Same regression through the public entry point used by notebooks.
    if True:
        def nested_double(x: int) -> int:
            return 2 * x

    node = lift(nested_double)   # raised IndentationError before the fix
    assert node is not None
