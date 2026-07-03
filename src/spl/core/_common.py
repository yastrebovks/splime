import os
import shutil
import tempfile
import weakref
from collections.abc import Callable
from dataclasses import dataclass, replace
from functools import reduce
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from types import FunctionType
from typing import Any, overload

from spl.core.entities.adapter import Adapter
from spl.core.entities.artifact import ArtifactRef, compute_sha256
from spl.core.entities.node import (
    DEFAULT_PORT,
    FormattedOutputRef,
    Node,
    NodeInputRef,
    NodeOutputRef,
)
from spl.core.entities.node_function import NodeFunction
from spl.core.entities.node_remote import NodeRemote
from spl.core.entities.pipeline import Pipeline
from spl.core.entities.scalar import Scalar

_JSON_NATIVE_TYPES = {str, int, float, bool, dict, list}


@dataclass(frozen = True)
class PipelineBuilder:
    pipeline: Pipeline
    root: Node
    format: str | None = None

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
        return PipelineBuilder(
            pipeline = pipeline,
            root = self.root,
            format = self.format)

    def alias(self, name):
        return replace(self, pipeline = self.pipeline.add_alias(self.root, name))

    def as_format(self, format: str) -> 'PipelineBuilder':
        """Return a builder whose output edge uses an artifact format."""

        if not isinstance(format, str):
            raise TypeError('pipeline builder format must be a string')
        if not format:
            raise ValueError('pipeline builder format must be a non-empty string')
        return replace(self, format = format)

    @staticmethod
    def _update_pipeline(pipeline, ref, v):
        match v:
            case PipelineBuilder():
                output_ref = NodeOutputRef(
                    v.root,
                    v.root.get_output_port(DEFAULT_PORT))
                link_value = (
                    output_ref
                    if v.format is None
                    else FormattedOutputRef(output_ref, v.format))
                return (pipeline | v.pipeline).add_link(
                    ref,
                    link_value)
            case _:
                return pipeline.add_link(
                    ref,
                    Scalar(v))

    def render(self, name: str | None = None):
        return replace(self.pipeline, name = name)


lift = PipelineBuilder.lift


def encode(value: Any, adapter: Adapter, artifacts_dir: Path) -> ArtifactRef:
    """Materialize a value with an adapter and return its artifact reference."""

    fd, artifact_path_value = tempfile.mkstemp(
        prefix = 'artifact-',
        dir = artifacts_dir)
    os.close(fd)
    artifact_path = Path(artifact_path_value)

    try:
        adapter.save(str(artifact_path), value)
    except BaseException:
        artifact_path.unlink(missing_ok = True)
        raise

    size = artifact_path.stat().st_size
    sha256 = compute_sha256(artifact_path)
    return ArtifactRef(
        key = adapter.key,
        uri = str(artifact_path),
        sha256 = sha256,
        size = size)


def decode(ref: ArtifactRef, adapter: Adapter) -> Any:
    """Load an artifact reference with an adapter after validating its digest."""

    if ref.key != adapter.key:
        raise ValueError('artifact ref key does not match adapter')

    artifact_path = Path(ref.uri)
    if artifact_path.stat().st_size != ref.size:
        raise ValueError('artifact ref size does not match file')
    if compute_sha256(artifact_path) != ref.sha256:
        raise ValueError('artifact ref sha256 does not match file')
    return adapter.load(str(artifact_path))


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

    @overload
    def run(self, *, output: None = None, **kwargs: Any) -> 'Run': ...

    @overload
    def run(self, *, output: str, **kwargs: Any) -> Any: ...

    def run(self, *, output: str | None = None, **kwargs: Any) -> Any:
        run = Run(self._callback, self._pipeline, **kwargs)
        if output is None:
            return run
        with run:
            return run.value(output)

    def _callback(self, node, kwargs):
        final_kwargs = {port.name: v for port, v in kwargs.items()}
        output_port = self._single_output_port(node)
        match node:
            case NodeFunction():
                return {output_port.name: node.func(**final_kwargs)}

            case NodeRemote():
                if self._client is None:
                    raise RuntimeError('remote node execution requires a client')
                # The private entry point keeps this canonical pipeline path
                # silent; the public ``run_node`` carries a DeprecationWarning.
                run_node = getattr(self._client, '_run_node_value', None) or self._client.run_node
                return {output_port.name: run_node(node, final_kwargs)}

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
    def __init__(
            self,
            callback: Callable[..., dict[str, Any]],
            pipeline: Pipeline,
            **kwargs: Any) -> None:
        self._callback = callback
        self._pipeline = pipeline
        self._kwargs = kwargs
        self._deps: dict[Node, dict[Any, Any]] = {
            k: dict(map(itemgetter(slice(1, None)), vs))
            for k, vs in groupby(
                sorted(
                    [(x.node, x.port, y) for (x, y) in pipeline.links],
                    key = lambda x: hash(x[0])),
                itemgetter(0))}
        self._results: dict[Node, dict[str, Any]] = dict()
        self._artifact_refs: dict[tuple[Node, str, str], ArtifactRef] = dict()
        self._artifacts_dir: Path | None = None
        self._artifacts_finalizer: Any = None
        self._closed = False

    def __enter__(self) -> 'Run':
        self._ensure_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            if self._artifacts_finalizer is not None:
                self._artifacts_finalizer()
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError('run is closed')

    def _get_artifacts_dir(self) -> Path:
        self._ensure_open()
        if self._artifacts_dir is None:
            artifacts_dir = Path(tempfile.mkdtemp(prefix = 'spl-run-'))
            self._artifacts_dir = artifacts_dir
            self._artifacts_finalizer = weakref.finalize(
                self,
                shutil.rmtree,
                artifacts_dir,
                ignore_errors = True)
        return self._artifacts_dir

    def _round_trip_artifact(
            self,
            value: Any,
            source_ref: NodeOutputRef | None = None,
            adapter_format: str | None = None) -> Any:
        if adapter_format is None and type(value) in _JSON_NATIVE_TYPES:
            return value

        adapter = self._pipeline.resolve_adapter(
            py_type = type(value),
            format = adapter_format)
        if adapter is None:
            if adapter_format is not None:
                raise ValueError(
                    'pipeline adapter is not found for python type ({}) '
                    'and format `{}`'.format(type(value), adapter_format))
            return value

        self._ensure_open()
        if source_ref is None:
            ref = encode(value, adapter, self._get_artifacts_dir())
        else:
            cache_key = (source_ref.node, source_ref.port.name, adapter.key)
            if cache_key not in self._artifact_refs:
                self._artifact_refs[cache_key] = encode(
                    value,
                    adapter,
                    self._get_artifacts_dir())
            ref = self._artifact_refs[cache_key]
        return decode(ref, adapter)

    def _get_input(self, x: Any) -> Any:
        match x:
            case Scalar():
                return self._round_trip_artifact(x.value)

            case NodeOutputRef():
                return self._round_trip_artifact(
                    (self._get_result(x.node))[x.port.name],
                    x)

            case FormattedOutputRef():
                return self._round_trip_artifact(
                    (self._get_result(x.out_ref.node))[x.out_ref.port.name],
                    source_ref = x.out_ref,
                    adapter_format = x.format)

            case _: raise ValueError(x)

    def _get_result(self, node: Node) -> dict[str, Any]:
        if node not in self._results:
            self._ensure_open()
            kwargs = {
                port: self._round_trip_artifact(self._kwargs[port.name])
                for port in node.inputs
                if port.name in self._kwargs}

            if node in self._deps:
                kwargs = kwargs | {
                    port: self._get_input(v)
                    for port, v in self._deps[node].items()}

            self._results[node] = self._callback(node, kwargs)
        return self._results[node]

    def __getitem__(self, node: Node) -> dict[str, Any]:
        try:
            return self._get_result(node)
        except BaseException:
            self.close()
            raise

    def value(self, alias: str | None = None, port: str = DEFAULT_PORT) -> Any:
        """Return one output value directly, without ``[node][port]`` indexing."""

        return self[self._resolve_alias_node(alias)][port]

    def _resolve_alias_node(self, alias: str | None) -> Node:
        if alias is not None:
            return self._pipeline.aliases[alias]
        if len(self._pipeline.nodes) == 1:
            return next(iter(self._pipeline.nodes))
        raise ValueError("Run.value() requires alias=... for multi-node pipelines")
