from dataclasses import dataclass, replace
from functools import reduce
from itertools import groupby
from operator import itemgetter
from types import FunctionType

from spl.core.entities.node import DEFAULT_PORT, Node, NodeInputRef, NodeOutputRef
from spl.core.entities.node_function import NodeFunction
from spl.core.entities.node_remote import NodeRemote
from spl.core.entities.pipeline import Pipeline
from spl.core.entities.scalar import Scalar


@dataclass(frozen = True)
class PipelineBuilder:
    pipeline: Pipeline
    root: Node

    @staticmethod
    def lift(x):

        match x:
            case PipelineBuilder():
                return x

            case FunctionType():
                root = NodeFunction(x)
                return PipelineBuilder(
                    Pipeline(nodes = {root}, links = set()),
                    root)

            case NodeFunction():
                root = x
                return PipelineBuilder(
                    Pipeline(nodes = {root}, links = set()),
                    root)

            case NodeRemote():
                root = x
                return PipelineBuilder(
                    Pipeline(nodes = {root}, links = set()),
                    root)

            case _:
                raise ValueError(x)

    def get_input_node_refs(self, port_name: str, is_free: bool):
        node_refs = [
            NodeInputRef(node, port)
            for node in self.pipeline.nodes
            for port in node.inputs
            if port.name == port_name]

        if is_free:
            bound_refs = set(map(itemgetter(0), self.pipeline.links))
            node_refs = [x for x in node_refs if x not in bound_refs]

        return node_refs


    def bind(self, **kwargs):
        return self._bind(kwargs, is_strict = True, is_free = False)

    def bind_all(self, **kwargs):
        return self._bind(kwargs, is_strict = False, is_free = True)

    def _bind(self, kwargs, is_strict: bool, is_free: bool):
        pipeline = self.pipeline
        for port_name, v in kwargs.items():
            match self.get_input_node_refs(port_name, is_free):
                case []:
                    raise ValueError('node(s) for port `{}` is not found'.format(port_name))

                case [ref]:
                    pipeline = self._update_pipeline(pipeline, ref, v)

                case refs:
                    if is_strict:
                        raise ValueError('ambigious node for port `{}`'.format(port_name))

                    pipeline = reduce(
                        lambda acc, ref: self._update_pipeline(acc, ref, v),
                        refs,
                        pipeline)
        return PipelineBuilder(pipeline = pipeline, root = self.root)

    def alias(self, name):
        return replace(self, pipeline = self.pipeline.add_alias(self.root, name))

    @staticmethod
    def _update_pipeline(pipeline, ref, v):
        match v:
            case PipelineBuilder():
                return (pipeline | v.pipeline).add_link(
                    ref,
                    NodeOutputRef(v.root, v.root.get_output_port(DEFAULT_PORT)))
            case _:
                return pipeline.add_link(
                    ref,
                    Scalar(v))

    def render(self, name: str | None = None):
        return replace(self.pipeline, name = name)


lift = PipelineBuilder.lift


class Deployment:
    def __init__(self, client=None, pipeline=None):
        if pipeline is None:
            pipeline = client
            client = None
        self._client = client
        self._pipeline = pipeline

    def setup(self):
        pass

    def teardown(self):
        pass

    def run(self, **kwargs):
        return Run(self._callback, self._pipeline, **kwargs)

    def _callback(self, node, kwargs):
        final_kwargs = {port.name: v for port, v in kwargs.items()}
        output_port = self._single_output_port(node)
        match node:
            case NodeFunction():
                return {output_port.name: node.func(**final_kwargs)}

            case NodeRemote():
                if self._client is None:
                    raise RuntimeError('remote node execution requires a client')
                return {output_port.name: self._client.run_node(node, final_kwargs)}

            case _:
                raise ValueError(node)

    @staticmethod
    def _single_output_port(node):
        outputs = node.outputs or []
        if len(outputs) != 1:
            raise RuntimeError(
                'node {} has {} outputs; local Deployment currently supports '
                'exactly one output and requires an explicit daemon/server '
                'output selector for multi-output pipelines'.format(
                    node,
                    len(outputs)))
        return outputs[0]


class Run:
    def __init__(self, callback, pipeline, **kwargs):
        self._callback = callback
        self._kwargs = kwargs
        self._deps = {
            k: dict(map(itemgetter(slice(1, None)), vs))
            for k, vs in groupby(
                sorted(
                    [(x.node, x.port, y) for (x, y) in pipeline.links],
                    key = lambda x: hash(x[0])),
                itemgetter(0))}
        self._results = dict()

    def _get_input(self, x):
        match x:
            case Scalar():
                return x.value

            case NodeOutputRef():
                return (self._get_result(x.node))[x.port.name]

            case _: raise ValueError(x)

    def _get_result(self, node):
        if node not in self._results:
            kwargs = {
                port: self._kwargs[port.name]
                for port in node.inputs
                if port.name in self._kwargs}

            if node in self._deps:
                kwargs = kwargs | {
                    port: self._get_input(v)
                    for port, v in self._deps[node].items()}

            self._results[node] = self._callback(node, kwargs)
        return self._results[node]

    def __getitem__(self, node):
        return self._get_result(node)
