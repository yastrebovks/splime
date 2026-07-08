import ast
from dataclasses import dataclass
from pathlib import Path
from types import FunctionType
from typing import Any, Generator, cast
from uuid import UUID

import yaml

from spl.core.entities.function import get_function_metadata
from spl.core.entities.node import Node
from spl.core.ir.common import DBase
from spl.core.ir.parse import _branch, ir_parse
from spl.core.ir.unparse import ir_unparse


@dataclass(frozen=True)
class NodeFunction(Node):
    func: FunctionType

    def __init__(self, func: FunctionType, uuid: UUID | None = None) -> None:
        md = get_function_metadata(func)
        super().__init__(inputs=md.inputs, outputs=cast(Any, md.outputs), uuid=uuid)
        object.__setattr__(self, "func", func)

    def __repr__(self) -> str:
        return "<{}>".format(self.func.__name__)

    def __hash__(self) -> int:
        return hash(self.uuid)


@dataclass(frozen=True)
class DNodeFunction(DBase):
    uuid: str
    func: str


yaml.add_representer(DNodeFunction, lambda dumper, data: dumper.represent_mapping("!DNodeFunction", data.__dict__))

yaml.add_constructor(
    "!DNodeFunction",
    lambda loader, node: DNodeFunction(**cast(dict[str, Any], loader.construct_mapping(cast(Any, node)))),
)


@ir_parse.register(lambda x: isinstance(x, NodeFunction))
def _ir_parse__node_function(x: NodeFunction, name: str | None = None) -> _branch:

    return _branch(
        x,
        lambda: DNodeFunction(uuid=str(x.uuid), func=get_function_metadata(x.func).name),
        lambda frame_offset: [ir_parse(x.func, name=name)],
    )


@ir_unparse.register(lambda x: isinstance(x, DNodeFunction))
def _ir_unparse__node_function(x: DNodeFunction, source: Path) -> Generator[ast.stmt]:
    yield ast.Assign(
        targets=[ast.Name(id="_node", ctx=ast.Store())],
        value=ast.Call(
            func=ast.Name(id="NodeFunction", ctx=ast.Load()),
            keywords=[
                ast.keyword(
                    arg="uuid",
                    value=ast.Call(func=ast.Name(id="UUID", ctx=ast.Load()), args=[ast.Constant(value=x.uuid)]),
                ),
                ast.keyword(arg="func", value=ast.Name(id=x.func, ctx=ast.Load())),
            ],
        ),
    )
