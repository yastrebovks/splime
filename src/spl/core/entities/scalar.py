import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, cast

import yaml

from spl.core.ir.common import DBase
from spl.core.ir.parse import _branch, ir_parse
from spl.core.ir.unparse import ir_unparse


class Scalar:
    value: Any

    def __init__(self, value: Any) -> None:
        self.value = value

    # def __repr__(self):
    #     return repr(self.value)


@dataclass(frozen=True)
class DScalar(DBase):
    value: Any


yaml.add_representer(DScalar, lambda dumper, data: dumper.represent_mapping("!DScalar", data.__dict__))

yaml.add_constructor(
    "!DScalar", lambda loader, node: DScalar(**cast(dict[str, Any], loader.construct_mapping(cast(Any, node))))
)


@ir_parse.register(lambda x: isinstance(x, Scalar))
def _ir_parse__scalar(x: Scalar, name: str | None = None) -> _branch:
    return _branch(x, lambda: DScalar(x.value), lambda frame_offset: [])


def scalar_value_expression(value: Any, *, _active: set[int] | None = None) -> ast.expr:
    """Build an executable AST literal without evaluating user-controlled text."""

    if value is None or value is Ellipsis or type(value) in {bool, int, float, complex, str, bytes}:
        return ast.Constant(value=value)

    if not isinstance(value, list | tuple | dict | set):
        raise TypeError(
            "DScalar value of type `{}` is not a supported SPL literal; use None, booleans, "
            "numbers, strings, bytes, lists, tuples, dictionaries, or sets".format(type(value).__name__)
        )

    active = _active if _active is not None else set()
    identity = id(value)
    if identity in active:
        raise TypeError("DScalar value contains a cyclic container and cannot be imported")
    active.add(identity)
    try:
        if isinstance(value, list):
            return ast.List(
                elts=[scalar_value_expression(item, _active=active) for item in value],
                ctx=ast.Load(),
            )
        if isinstance(value, tuple):
            return ast.Tuple(
                elts=[scalar_value_expression(item, _active=active) for item in value],
                ctx=ast.Load(),
            )
        if isinstance(value, set):
            return ast.Set(elts=[scalar_value_expression(item, _active=active) for item in value])
        return ast.Dict(
            keys=[scalar_value_expression(item, _active=active) for item in value],
            values=[scalar_value_expression(item, _active=active) for item in value.values()],
        )
    finally:
        active.remove(identity)


@ir_unparse.register(lambda x: isinstance(x, DScalar))
def _ir_unparse__scalar(x: DScalar, source: Path) -> Generator[ast.stmt]:
    yield ast.Assign(
        targets=[ast.Name(id="_link_to", ctx=ast.Store())],
        value=ast.Call(
            func=ast.Name(id="Scalar", ctx=ast.Load()),
            keywords=[ast.keyword(arg="value", value=scalar_value_expression(x.value))],
        ),
    )
