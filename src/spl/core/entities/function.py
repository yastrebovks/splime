import ast
import builtins
import dis
import inspect
import sys
import typing
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from types import CodeType, FunctionType
from typing import Annotated, Generator

import yaml

import spl.core.entities.node as m_node
from spl.core.entities.control import DSPLImport
from spl.core.entities.node import DEFAULT_PORT, InputPort, OutputPort
from spl.core.ir.common import DBase
from spl.core.ir.parse import _attach, _branch, ir_parse
from spl.core.ir.unparse import ir_unparse

LOCATION_DUNDER_NAME = '__spl_location__'
METADATA_DUNDER_NAME = '__spl_metadata__'


@dataclass
class _body:  # noqa: N801
    value: str

yaml.add_representer(
    _body,
    lambda dumper, data: dumper.represent_scalar('tag:yaml.org,2002:str', data.value, style = '|'))


@dataclass(frozen = True)
class DFunction(DBase):
    name: str
    body: str
    inputs: list[InputPort]
    outputs: list[OutputPort] | None

    def __hash__(self):
        return hash((self.name, self.body, tuple(self.inputs), tuple(self.outputs)))

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self.name))


yaml.add_representer(
    DFunction,
    lambda dumper, data: dumper.represent_mapping('!DFunction', {
        'name': data.name,
        'inputs': list(map(
            lambda x: {
                'name': x.name,
                'type': x.typ_,
                'default': x.default}, data.inputs)),
        'outputs': None if data.outputs is None else list(map(
            lambda x: {
                'name': x.name,
                'type': x.typ_}, data.outputs)),
        'body': _body(data.body)}))


yaml.add_constructor(
    '!DFunction',
    lambda loader, node: (lambda data: DFunction(
        name = data['name'],
        body = data['body'],
        inputs = [
            InputPort(
                name = x['name'],
                typ_ = x['type'],
                default = x.get('default'))
            for x in data['inputs']],
        outputs = [
            OutputPort(
                name = x['name'],
                typ_ = x['type'])
            for x in data['outputs']]))
    (loader.construct_mapping(node, deep = True)))


def get_dependency_names_from_bytecode(f: FunctionType | CodeType):
    for x in dis.Bytecode(f):
        match x.opname:
            case 'LOAD_GLOBAL':
                yield x.argval
            case 'LOAD_NAME':
                yield x.argval
            case 'LOAD_CONST':
                if isinstance(x.argval, CodeType):
                    yield from get_dependency_names_from_bytecode(x.argval)

def get_dependencies_from_bytecode(
        frame_offset,
        f: FunctionType | CodeType):
    g = sys._getframe(1 + frame_offset).f_globals

    names = sorted(filter(
        lambda x: x not in vars(builtins),
        set(get_dependency_names_from_bytecode(f))))

    if missing_names := (set(names) - set(g.keys())):
        raise ValueError('missing names: {}'.format(', '.join(sorted(missing_names))))

    for name in names:
        yield ir_parse(g[name], name)


def get_dependencies_from_ast(frame_offset, tree: ast.FunctionDef):
    for value in map(ast.unparse, filter(None, [x.annotation for x in tree.args.args])):
        yield from get_dependencies_from_bytecode(
            frame_offset + 1,
            value)
    for value in map(ast.unparse, tree.args.defaults):
        yield from get_dependencies_from_bytecode(
            frame_offset + 1,
            value)


def serialize_function_output(func: FunctionType, tree: ast.FunctionDef):
    name = DEFAULT_PORT

    return_type = func.__annotations__.get('return')
    if (return_type is not None) and (typing.get_origin(return_type) == Annotated):
        (_, name, *_) = typing.get_args(return_type)

    return [
        OutputPort(
            name = name,
            typ_ = ast.unparse(tree.returns) if (tree.returns is not None) else None)]


def serialize_function(func: FunctionType, tree: ast.FunctionDef | None = None):
    if tree is None:
        [tree] = ast.parse(inspect.getsource(func)).body

    args = tree.args

    return DFunction(
        name = tree.name,
        body = ast.unparse(tree.body),
        inputs = [
            InputPort(
                name = a.arg,
                typ_ = ast.unparse(a.annotation) if a.annotation is not None else None,
                default = ast.unparse(d) if d is not None else None)
            for a, d in zip(
                    args.args,
                    [*[None] * (len(args.args) - len(args.defaults)), *args.defaults],
                    strict = True)],
        outputs = serialize_function_output(func, tree))


@ir_parse.register(
    lambda x: (isinstance(x, FunctionType) and (getattr(x, '__module__', None) == '__main__')))
def _ir_parse__function(
        x: FunctionType,
        name: str | None = None):

    if hasattr(x, LOCATION_DUNDER_NAME):
        # We imported this function using SPL, using it's metadata.
        return _attach(chain([DSPLImport(*getattr(x, LOCATION_DUNDER_NAME))]))

    [tree] = ast.parse(inspect.getsource(x)).body
    return _branch(
        x,
        lambda: serialize_function(x, tree),
        lambda frame_offset: chain(
            get_dependencies_from_bytecode(frame_offset, x),
            get_dependencies_from_ast(frame_offset, tree)))


def get_function_metadata(func: FunctionType):
    if hasattr(func, METADATA_DUNDER_NAME):
        # We imported this function using SPL, using it's metadata.
        return getattr(func, METADATA_DUNDER_NAME)
    return serialize_function(func)


@ir_unparse.register(lambda x: isinstance(x, DFunction))
def _ir_unparse__function(x: DFunction, source: Path) -> Generator[ast.stmt]:
    yield ast.FunctionDef(
        name = x.name,
        body = ast.parse(x.body).body,
        # TODO: add support for multiple outputs
        returns = ast.parse(x.outputs[0].typ_, mode = 'eval').body if x.outputs[0].typ_ is not None else None,
        args = ast.arguments(
            args = [
                ast.arg(
                    arg = port.name,
                    annotation = ast.parse(port.typ_, mode = 'eval').body if port.typ_ is not None else None)
                for port in x.inputs],
            defaults = [
                ast.parse(port.default, mode = 'eval').body
                for port in x.inputs
                if port.default is not None]))

    # Importing helpers
    yield ast.ImportFrom(
        module = m_node.__name__,
        names = [
            ast.alias(name = 'InputPort'),
            ast.alias(name = 'OutputPort')])

    yield ast.ImportFrom(
        module = __name__,
        names = [
            ast.alias(name = 'DFunction')])

    # Marking function as spl-imported
    yield ast.Expr(value = ast.Call(
        func = ast.Name(id = 'setattr', ctx = ast.Load()),
        args = [
            ast.Name(id = x.name, ctx = ast.Load()),
            ast.Constant(value = LOCATION_DUNDER_NAME),
            ast.Tuple(elts = [
                ast.Constant(value = str(source.absolute())),
                ast.Constant(value = x.name)])]))

    # Adding metadata
    yield ast.Expr(value = ast.Call(
        func = ast.Name(id = 'setattr', ctx = ast.Load()),
        args = [
            ast.Name(id = x.name, ctx = ast.Load()),
            ast.Constant(value = METADATA_DUNDER_NAME),
            ast.Call(
                func = ast.Name(id = 'DFunction', ctx = ast.Load()),
                keywords = [
                    ast.keyword(arg = 'name', value = ast.Constant(value = x.name)),
                    ast.keyword(arg = 'body', value = ast.Constant(value = x.body)),
                    ast.keyword(arg = 'inputs', value = ast.List(
                        elts = [
                            ast.Call(
                                func = ast.Name(id = 'InputPort', ctx = ast.Load()),
                                keywords = [
                                    ast.keyword(arg = 'name', value = ast.Constant(value = port.name)),
                                    ast.keyword(arg = 'typ_', value = ast.Constant(value = port.typ_)),
                                    ast.keyword(
                                        arg = 'default',
                                        value = ast.Constant(value = port.default))])
                            for port in x.inputs],
                        ctx = ast.Load())),

                    ast.keyword(arg = 'outputs', value = ast.List(
                        elts = [
                            ast.Call(
                                func = ast.Name(id = 'OutputPort', ctx = ast.Load()),
                                keywords = [
                                    ast.keyword(arg = 'name', value = ast.Constant(value = port.name)),
                                    ast.keyword(arg = 'typ_', value = ast.Constant(value = port.typ_))])
                            for port in x.outputs],
                        ctx = ast.Load()))])]))
