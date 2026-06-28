import ast
from dataclasses import dataclass, field, replace
from itertools import chain
from operator import itemgetter
from pathlib import Path
from typing import Any, Generator

import yaml

import spl.core.entities.node as m_node
import spl.core.entities.node_function as m_node_function
import spl.core.entities.scalar as m_scalar
from spl.core.entities.node import Node, NodeInputRef, NodeOutputRef
from spl.core.ir.common import DBase
from spl.core.ir.parse import _branch, ir_parse
from spl.core.ir.unparse import ir_unparse


@dataclass(frozen = True)
class Pipeline:
    name: str | None = None
    nodes: set[Node] = field(default_factory = set)
    links: set[tuple[NodeInputRef, Any]] = field(default_factory = set)
    aliases: dict[str, Node] = field(default_factory = dict)

    def __hash__(self):
        return hash((
            tuple(sorted(map(hash, self.nodes))),
            tuple(sorted(map(hash, self.links)))))

    def __or__(self, other):
        nodes = set.union(self.nodes, other.nodes)
        links = set.union(self.links, other.links)
        aliases = self._merge_aliases(other)
        return Pipeline(
            nodes = nodes,
            links = links,
            aliases = aliases)._validate_consistency()

    def add_link(self, node_input_ref, value):
        if (node := node_input_ref.node) not in self.nodes:
            raise ValueError('pipeline does not contain input node ({})'.format(node))
        if node_input_ref.port not in node.inputs:
            raise ValueError('pipeline input ref does not belong to node ({})'.format(node_input_ref))

        if isinstance(value, NodeOutputRef):
            if (node := value.node) not in self.nodes:
                raise ValueError('pipeline does not contain output node ({})'.format(node))
            if value.port not in node.outputs:
                raise ValueError('pipeline output ref does not belong to node ({})'.format(value))

        for existing_ref, existing_value in self.links:
            if existing_ref == node_input_ref and existing_value != value:
                raise ValueError(
                    'pipeline input `{}` is already linked'.format(node_input_ref))

        return Pipeline(
            nodes = self.nodes,
            links = {*self.links, (node_input_ref, value)},
            aliases = self.aliases)._validate_consistency()


    def add_alias(self, node, name):
        if not isinstance(name, str) or not name:
            raise ValueError('pipeline alias name must be a non-empty string')
        if node not in self.nodes:
            raise ValueError('pipeline alias points to unknown node ({})'.format(node))
        if name in self.aliases and self.aliases[name] != node:
            raise ValueError('pipeline alias `{}` already points to another node'.format(name))
        return replace(self, aliases = {**self.aliases, name: node})._validate_consistency()


    def get_free_inputs(self) -> list[NodeInputRef]:
        return ({
            NodeInputRef(node, port)
            for node in self.nodes
            for port in node.inputs} - set(map(itemgetter(0), self.links)))

    def get_unbound_inputs(self) -> list[NodeInputRef]:
        return ({
            NodeInputRef(node, port)
            for node in self.nodes
            for port in node.inputs
            if port.default is None} - set(map(itemgetter(0), self.links)))

    def get_outputs(self) -> list[NodeOutputRef]:
        return ({
            NodeOutputRef(node, port)
            for node in self.nodes
            for port in node.outputs})

    def get_node_by_alias(self, name):
        return self.aliases[name]

    def _merge_aliases(self, other):
        aliases = dict(self.aliases)
        for name, node in other.aliases.items():
            if name in aliases and aliases[name] != node:
                raise ValueError('pipeline alias conflict: `{}`'.format(name))
            aliases[name] = node
        return aliases

    def _validate_consistency(self):
        linked_inputs = set()
        for node_input_ref, value in self.links:
            if node_input_ref.node not in self.nodes:
                raise ValueError(
                    'pipeline link target node is not in pipeline ({})'.format(
                        node_input_ref.node))
            if node_input_ref.port not in node_input_ref.node.inputs:
                raise ValueError(
                    'pipeline link target port is not on node ({})'.format(
                        node_input_ref))
            if node_input_ref in linked_inputs:
                raise ValueError('pipeline input `{}` is linked more than once'.format(node_input_ref))
            linked_inputs.add(node_input_ref)

            if isinstance(value, NodeOutputRef):
                if value.node not in self.nodes:
                    raise ValueError(
                        'pipeline link source node is not in pipeline ({})'.format(
                            value.node))
                if value.port not in value.node.outputs:
                    raise ValueError(
                        'pipeline link source port is not on node ({})'.format(
                            value))

        for name, node in self.aliases.items():
            if node not in self.nodes:
                raise ValueError('pipeline alias `{}` points to unknown node'.format(name))
        return self


@dataclass(frozen = True)
class DPipeline(DBase):
    name: str
    nodes: list[Any]
    links: list[Any]
    aliases: list[list[str]]

    def __hash__(self):
        return hash((
            tuple(sorted(map(hash, self.nodes))),
            tuple(sorted(map(hash, chain.from_iterable(self.links))))))

yaml.add_representer(
    DPipeline,
    lambda dumper, data: dumper.represent_mapping('!DPipeline', data.__dict__))

yaml.add_constructor(
    '!DPipeline',
    lambda loader, node: DPipeline(**loader.construct_mapping(node)))


@ir_parse.register(
    lambda x: isinstance(x, Pipeline))
def _ir_parse__pipeline(
        x: Pipeline,
        name: str | None = None):

    def mk_root():
        return DPipeline(
            name = x.name,
            nodes = [ir_parse(n, name = name).mk_root() for n in x.nodes],
            links = [
                [ir_parse(l_from).mk_root(), ir_parse(l_to).mk_root()]
                for (l_from, l_to) in x.links],
            aliases = [[k, str(v.uuid)] for k, v in x.aliases.items()])

    def mk_dependencies(frame_offset):
        return chain.from_iterable([
            ir_parse(n, name = name).mk_dependencies(frame_offset) for n in x.nodes])

    return _branch(
        x,
        mk_root,
        mk_dependencies)


@ir_unparse.register(
    lambda x: isinstance(x, DPipeline))
def _ir_unparse__pipeline(x: DPipeline, source: Path) -> Generator[ast.stmt]:


    # Importing helpers
    # TODO: move to corresponding modules
    yield ast.ImportFrom(
        module = 'uuid',
        names = [
            ast.alias(name = 'UUID')])

    yield ast.ImportFrom(
        module = m_node.__name__,
        names = [
            ast.alias(name = 'NodeInputRef'),
            ast.alias(name = 'NodeOutputRef')])

    yield ast.ImportFrom(
        module = m_scalar.__name__,
        names = [
            ast.alias(name = 'Scalar')])

    yield ast.ImportFrom(
        module = m_node_function.__name__,
        names = [
            ast.alias(name = 'NodeFunction')])

    yield ast.ImportFrom(
        module = __name__,
        names = [
            ast.alias(name = 'Pipeline')])

    # _nodes = {}
    yield ast.Assign(
        targets = [ast.Name(id = '_nodes', ctx = ast.Store())],
        value = ast.Dict())

    for n in x.nodes:
        # _node = ...
        yield from ir_unparse(n, source = source)

        # _nodes[_node.uuid] = _node
        yield ast.Assign(
            targets = [
                ast.Subscript(
                    value = ast.Name(id = '_nodes', ctx = ast.Load()),
                    slice = ast.Attribute(
                        value = ast.Name(id = '_node', ctx = ast.Load()),
                        attr = 'uuid',
                        ctx = ast.Load()),
                    ctx = ast.Store())],
            value = ast.Name(id = '_node', ctx = ast.Load()))

    # _links = []
    yield ast.Assign(
        targets = [ast.Name(id = '_links', ctx = ast.Store())],
        value = ast.List())

    for (link_from, link_to) in x.links:
        # _link_from = ...
        yield from ir_unparse(link_from, source = source)

        # _link_to = ...
        yield from ir_unparse(link_to, source = source)

        # _links.append((_link_from, _link_to))
        yield ast.Expr(
          value = ast.Call(
              func = ast.Attribute(
                  value = ast.Name(id = '_links', ctx = ast.Load()),
                  attr = 'append',
                  ctx = ast.Load()),
              args = [
                  ast.Tuple(elts = [
                      ast.Name(id = '_link_from', ctx = ast.Load()),
                      ast.Name(id = '_link_to', ctx = ast.Load())],
                    ctx = ast.Load())]))

    # pipeline = Pipeline(...)
    yield ast.Assign(
        targets = [ast.Name(id = x.name, ctx = ast.Store())],
        value = ast.Call(
            func = ast.Name(id = 'Pipeline', ctx = ast.Load()),
            keywords = [
                ast.keyword(
                    arg = 'name',
                    value = ast.Constant(value = x.name)),

                ast.keyword(
                    arg = 'nodes',
                    value = ast.Set(elts = [
                        ast.Starred(value = ast.Call(func = ast.Attribute(
                            value = ast.Name(id = '_nodes', ctx = ast.Load()),
                            attr = 'values',
                            ctx = ast.Load())))])),

                ast.keyword(
                    arg = 'links',
                    value = ast.Set(elts = [
                        ast.Starred(value = ast.Name(id = '_links', ctx = ast.Load()))])),

                ast.keyword(
                    arg = 'aliases',
                    value = ast.Dict(
                        keys = [
                            ast.Constant(value = k)
                            for [k, _] in x.aliases],
                        values = [
                            ast.Subscript(
                                value = ast.Name(id = '_nodes', ctx = ast.Load()),
                                slice = ast.Call(
                                    func = ast.Name(id = 'UUID', ctx = ast.Load()),
                                    args = [ast.Constant(value = v)]),
                                ctx = ast.Load())
                            for [_, v] in x.aliases]))]))
