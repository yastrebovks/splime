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


@ir_unparse.register(lambda x: isinstance(x, DScalar))
def _ir_unparse__scalar(x: DScalar, source: Path) -> Generator[ast.stmt]:
    yield ast.Assign(
        targets=[ast.Name(id="_link_to", ctx=ast.Store())],
        value=ast.Call(
            func=ast.Name(id="Scalar", ctx=ast.Load()),
            keywords=[ast.keyword(arg="value", value=ast.Constant(value=x.value))],
        ),
    )
