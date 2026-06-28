import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Generator
from uuid import UUID, uuid4

import yaml

from spl.core.ir.common import DBase
from spl.core.ir.parse import _branch, ir_parse
from spl.core.ir.unparse import ir_unparse

DEFAULT_PORT = 'default'


@dataclass(frozen = True)
class Port:
    name: str
    typ_: str | None


@dataclass(frozen = True)
class InputPort(Port):
    default: str | None


@dataclass(frozen = True)
class OutputPort(Port):
    pass


@dataclass(frozen = True)
class Node:
    uuid: UUID
    inputs: list[InputPort]
    outputs: list[OutputPort]

    def __init__(self, inputs, outputs, uuid: UUID | None = None):
        uuid = uuid or uuid4()
        super().__init__()
        object.__setattr__(self, 'uuid', uuid)
        object.__setattr__(self, 'inputs', inputs)
        object.__setattr__(self, 'outputs', outputs)

    def __hash__(self):
        return hash(self.uuid)

    def get_input_port(self, port_name):
        return next(iter(filter(lambda p: p.name == port_name, self.inputs)))

    def get_output_port(self, port_name = DEFAULT_PORT):
        return next(iter(filter(lambda p: p.name == port_name, self.outputs)))


@dataclass(frozen = True)
class NodeInputRef:
    node: Node
    port: InputPort

    def __repr__(self):
        return self.node.__repr__() + ':' + self.port.name

@dataclass(frozen = True)
class DNodeInputRef(DBase):
    uuid: str
    port: str

yaml.add_representer(
    DNodeInputRef,
    lambda dumper, data: dumper.represent_mapping('!DNodeInputRef', data.__dict__))

yaml.add_constructor(
    '!DNodeInputRef',
    lambda loader, node: DNodeInputRef(**loader.construct_mapping(node)))

@ir_parse.register(
    lambda x: isinstance(x, NodeInputRef))
def _ir_parse__node_input_ref(
        x: NodeInputRef,
        name: str | None = None):
    return _branch(
        x,
        lambda: DNodeInputRef(
            uuid = str(x.node.uuid),
            port = str(x.port.name)),
        lambda frame_offset: [])


@ir_unparse.register(
    lambda x: isinstance(x, DNodeInputRef))
def _ir_unparse__node_input_ref(x: DNodeInputRef, source: Path) -> Generator[ast.stmt]:
    yield ast.Assign(
        targets = [ast.Name(id = '_link_from', ctx = ast.Store())],
        value = ast.Call(
            func = ast.Name(id = 'NodeInputRef', ctx = ast.Load()),
            keywords = [
                ast.keyword(
                    arg = 'node',
                    value = ast.Subscript(
                        value = ast.Name(id = '_nodes', ctx = ast.Load()),
                        slice = ast.Call(
                            func = ast.Name(id = 'UUID', ctx = ast.Load()),
                            args = [ast.Constant(value = x.uuid)]),
                        ctx = ast.Load())),
                ast.keyword(
                    arg = 'port',
                    value = ast.Call(
                        func = ast.Attribute(
                            value = ast.Subscript(
                                value = ast.Name(id = '_nodes', ctx = ast.Load()),
                                slice = ast.Call(
                                    func = ast.Name(id = 'UUID', ctx = ast.Load()),
                                    args = [ast.Constant(value = x.uuid)]),
                                ctx = ast.Load()),
                            attr = 'get_input_port',
                            ctx = ast.Load()),
                        args = [ast.Constant(value = x.port)]))]))


@dataclass(frozen = True)
class NodeOutputRef:
    node: Node
    port: OutputPort

    def __repr__(self):
        return self.node.__repr__() + ':' + self.port.name

@dataclass(frozen = True)
class DNodeOutputRef(DBase):
    uuid: str
    port: str

yaml.add_representer(
    DNodeOutputRef,
    lambda dumper, data: dumper.represent_mapping('!DNodeOutputRef', data.__dict__))

yaml.add_constructor(
    '!DNodeOutputRef',
    lambda loader, node: DNodeOutputRef(**loader.construct_mapping(node)))

@ir_parse.register(
    lambda x: isinstance(x, NodeOutputRef))
def _ir_parse__node_output_ref(
        x: NodeOutputRef,
        name: str | None = None):
    return _branch(
        x,
        lambda: DNodeOutputRef(
            uuid = str(x.node.uuid),
            port = str(x.port.name)),
        lambda frame_offset: [])


@ir_unparse.register(
    lambda x: isinstance(x, DNodeOutputRef))
def _ir_unparse__node_output_ref(x: DNodeOutputRef, source: Path) -> Generator[ast.stmt]:
    yield ast.Assign(
        targets = [ast.Name(id = '_link_to', ctx = ast.Store())],
        value = ast.Call(
            func = ast.Name(id = 'NodeOutputRef', ctx = ast.Load()),
            keywords = [
                ast.keyword(
                    arg = 'node',
                    value = ast.Subscript(
                        value = ast.Name(id = '_nodes', ctx = ast.Load()),
                        slice = ast.Call(
                            func = ast.Name(id = 'UUID', ctx = ast.Load()),
                            args = [ast.Constant(value = x.uuid)]),
                        ctx = ast.Load())),
                ast.keyword(
                    arg = 'port',
                    value = ast.Call(
                        func = ast.Attribute(
                            value = ast.Subscript(
                                value = ast.Name(id = '_nodes', ctx = ast.Load()),
                                slice = ast.Call(
                                    func = ast.Name(id = 'UUID', ctx = ast.Load()),
                                    args = [ast.Constant(value = x.uuid)]),
                                ctx = ast.Load()),
                            attr = 'get_output_port',
                            ctx = ast.Load()),
                        args = [ast.Constant(value = x.port)]))]))
