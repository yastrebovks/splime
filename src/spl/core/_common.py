import inspect
import hashlib
import json
import logging
import os
import shutil
import tempfile
import typing
import warnings
import weakref
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, replace
from functools import reduce
from operator import itemgetter
from pathlib import Path
from types import FunctionType, UnionType
from typing import Any, cast, overload

from spl.core import manifest as m_manifest
from spl.core import node_runtime as m_node_runtime
from spl.core import resume as m_resume
from spl.core._graph import node_input_ref_sort_key
from spl.core.entities.adapter import (
    BUILTIN_JSON_ADAPTER,
    JSON_NATIVE_TYPES,
    Adapter,
    BuiltInJsonAdapter,
    LoadAdapter,
    RuntimeAdapter,
    SaveAdapter,
    load_adapter_identity,
    save_adapter_identity,
)
from spl.core.entities.artifact import ArtifactRef, compute_sha256
from spl.core.entities.node import (
    DEFAULT_PORT,
    FormattedOutputRef,
    InputPort,
    Node,
    NodeInputRef,
    NodeOutputRef,
    OutputPort,
)
from spl.core.entities.node_function import NodeFunction
from spl.core.entities.node_remote import NodeRemote
from spl.core.entities.pipeline import (
    AdapterResolution,
    AdapterResolutionSource,
    LoadAdapterResolution,
    Pipeline,
    SaveAdapterResolution,
)
from spl.core.entities.scalar import Scalar
from spl.core.fingerprint import canonical_json_bytes, node_fingerprint
from spl.core.json_contract import validate_json_value

_JSON_NATIVE_TYPES = JSON_NATIVE_TYPES
LOGGER = logging.getLogger(__name__)
RunAdapterOverrideKey = tuple[str, str]
RunAdapterOverrides = Mapping[RunAdapterOverrideKey, RuntimeAdapter]
_NormalizedRunAdapterOverrides = dict[tuple[Node, str], RuntimeAdapter]
RunRuntimeOverrides = m_node_runtime.RunRuntimeOverrides
_OutputMaterializationRequest = tuple[NodeOutputRef, str | None, RuntimeAdapter | None, Node, InputPort]

_BUILTIN_PORT_TYPES: dict[str, type[Any]] = {
    "str": str,
    "builtins.str": str,
    "int": int,
    "builtins.int": int,
    "float": float,
    "builtins.float": float,
    "bool": bool,
    "builtins.bool": bool,
    "None": type(None),
    "NoneType": type(None),
    "builtins.NoneType": type(None),
    "dict": dict,
    "builtins.dict": dict,
    "list": list,
    "builtins.list": list,
    "Dict": dict,
    "typing.Dict": dict,
    "List": list,
    "typing.List": list,
    "set": set,
    "builtins.set": set,
    "Set": set,
    "typing.Set": set,
    "tuple": tuple,
    "builtins.tuple": tuple,
    "Tuple": tuple,
    "typing.Tuple": tuple,
}


class _SourceOutputCommitError(RuntimeError):
    """Failure while saving or recording one producing-node output."""


@dataclass(frozen=True)
class _ResolvedNodeRuntimePlan:
    resolution: m_node_runtime.NodeRuntimeResolution
    backend: m_node_runtime.NodeRuntimeBackend
    fast_native: bool

    @property
    def isolated(self) -> bool:
        return self.resolution.name != m_node_runtime.NATIVE_NODE_RUNTIME


def _runtime_input_type_hint(node: Node, port: InputPort) -> type[Any] | str | None:
    if isinstance(node, NodeFunction):
        try:
            annotation = typing.get_type_hints(node.func, include_extras=True).get(port.name)
        except (NameError, TypeError):
            annotation = node.func.__annotations__.get(port.name)
        while typing.get_origin(annotation) is typing.Annotated:
            annotation = typing.get_args(annotation)[0]
        if annotation is Any:
            annotation = None
        origin = typing.get_origin(annotation)
        if origin in {typing.Union, UnionType}:
            concrete = [item for item in typing.get_args(annotation) if item is not type(None)]
            annotation = concrete[0] if len(concrete) == 1 else None
            if annotation is Any:
                annotation = None
            origin = typing.get_origin(annotation)
        if isinstance(origin, type):
            return origin
        if isinstance(annotation, type):
            return annotation
    if port.typ_ is not None:
        type_name = _normalize_port_type_name(port.typ_)
        if type_name is not None:
            return _BUILTIN_PORT_TYPES.get(type_name, type_name)
    return None


def _runtime_output_type_hint(node: Node, port: OutputPort) -> type[Any] | str | None:
    if isinstance(node, NodeFunction):
        try:
            annotation = typing.get_type_hints(node.func, include_extras=True).get("return")
        except (NameError, TypeError):
            annotation = node.func.__annotations__.get("return")
        while typing.get_origin(annotation) is typing.Annotated:
            annotation = typing.get_args(annotation)[0]
        if annotation is Any:
            annotation = None
        origin = typing.get_origin(annotation)
        if origin in {typing.Union, UnionType}:
            concrete = [item for item in typing.get_args(annotation) if item is not type(None)]
            annotation = concrete[0] if len(concrete) == 1 else None
            if annotation is Any:
                annotation = None
            origin = typing.get_origin(annotation)
        if isinstance(origin, type):
            return origin
        if isinstance(annotation, type):
            return annotation
    if port.typ_ is not None:
        type_name = _normalize_port_type_name(port.typ_)
        if type_name is not None:
            return _BUILTIN_PORT_TYPES.get(type_name, type_name)
    return None


def _normalize_port_type_name(type_name: str) -> str | None:
    normalized = type_name.strip()
    if not normalized or normalized in {"Any", "typing.Any"} or "|" in normalized:
        return None
    outer = normalized.partition("[")[0].strip()
    if outer in {"Annotated", "typing.Annotated", "Optional", "typing.Optional", "Union", "typing.Union"}:
        return None
    concrete = _BUILTIN_PORT_TYPES.get(outer)
    if concrete is not None:
        return "{}.{}".format(concrete.__module__, concrete.__qualname__)
    return outer


def _accumulate_pipeline_dependencies(pipeline: Pipeline) -> dict[Node, dict[InputPort, Any]]:
    deps: defaultdict[Node, dict[InputPort, Any]] = defaultdict(dict)
    linked_inputs: set[NodeInputRef] = set()
    for node_input_ref, value in sorted(pipeline.links, key=lambda link: node_input_ref_sort_key(link[0])):
        if node_input_ref in linked_inputs:
            raise ValueError("pipeline input `{}` is linked more than once".format(node_input_ref))
        linked_inputs.add(node_input_ref)
        deps[node_input_ref.node][node_input_ref.port] = value
    return dict(deps)


def _normalize_run_adapter_override_key(key: Any) -> RunAdapterOverrideKey:
    if not isinstance(key, tuple) or len(key) != 2:
        raise TypeError("run adapter override key must be a `(alias, port)` tuple")
    alias, port_name = key
    if not isinstance(alias, str) or not alias:
        raise ValueError("run adapter override alias must be a non-empty string")
    if not isinstance(port_name, str) or not port_name:
        raise ValueError("run adapter override port must be a non-empty string")
    return alias, port_name


def _validate_runtime_adapter(adapter: RuntimeAdapter) -> None:
    for attr in ("key", "tag", "accepted_tags", "save", "load", "legacy_key_guard", "distributions"):
        if not hasattr(adapter, attr):
            raise TypeError("run adapter override value must implement RuntimeAdapter; missing `{}`".format(attr))
    if not isinstance(adapter.key, str) or not adapter.key:
        raise ValueError("run adapter override key must be a non-empty string")
    if not isinstance(adapter.tag, str) or not adapter.tag:
        raise ValueError("run adapter override tag must be a non-empty string")
    if not isinstance(adapter.accepted_tags, frozenset) or any(
        not isinstance(tag, str) or not tag for tag in adapter.accepted_tags
    ):
        raise TypeError("run adapter override accepted_tags must be a frozenset of non-empty strings")
    if not callable(adapter.save):
        raise TypeError("run adapter override save must be callable")
    if not callable(adapter.load):
        raise TypeError("run adapter override load must be callable")
    if not isinstance(adapter.legacy_key_guard, bool):
        raise TypeError("run adapter override legacy_key_guard must be a bool")
    if not isinstance(adapter.distributions, tuple):
        raise TypeError("run adapter override distributions must be a tuple")


def _validate_run_adapter_overrides(
    pipeline: Pipeline, adapters: RunAdapterOverrides | None
) -> _NormalizedRunAdapterOverrides:
    if adapters is None:
        return {}
    if not isinstance(adapters, Mapping):
        raise TypeError("run adapter overrides must be a mapping")

    normalized: _NormalizedRunAdapterOverrides = {}
    for raw_key, adapter in adapters.items():
        alias, port_name = _normalize_run_adapter_override_key(raw_key)
        if alias not in pipeline.aliases:
            raise ValueError("run adapter override references unknown alias `{}`".format(alias))
        node = pipeline.aliases[alias]
        known_ports = {port.name for port in node.outputs or []}
        if port_name not in known_ports:
            raise ValueError(
                "run adapter override references unknown output port `{}` for alias `{}`".format(port_name, alias)
            )
        _validate_runtime_adapter(adapter)
        normalized[(node, port_name)] = adapter
    return normalized


def _kwargs_from_manifest(pipeline: Pipeline, manifest: Mapping[str, Any]) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    linked_inputs = {ref for ref, _ in pipeline.links}
    nodes = manifest.get("nodes")
    if not isinstance(nodes, Mapping):
        return kwargs
    for node in pipeline.nodes:
        record = nodes.get(str(node.uuid))
        if not isinstance(record, Mapping):
            continue
        inputs = record.get("inputs")
        if not isinstance(inputs, Mapping):
            continue
        for port in node.inputs:
            if any(ref.node == node and ref.port == port for ref in linked_inputs):
                continue
            input_record = inputs.get(port.name)
            if isinstance(input_record, Mapping) and input_record.get("kind") == "json":
                kwargs.setdefault(port.name, input_record.get("value"))
    return kwargs


def _warn_reserved_run_input_names(pipeline: Pipeline, reserved_names: frozenset[str]) -> None:
    conflicts = sorted({ref.port.name for ref in pipeline.get_free_inputs()} & reserved_names)
    if not conflicts:
        return
    formatted_conflicts = ", ".join("`{}`".format(name) for name in conflicts)
    warnings.warn(
        "pipeline has free input port(s) with reserved run()/resume() parameter name(s): {}; "
        "bind these inputs via `lift(...).bind(<name>=...)` before `render()`, reserved run()/resume() "
        "parameter names cannot be passed as inputs.".format(formatted_conflicts),
        UserWarning,
        stacklevel=4,
    )


@dataclass(frozen=True)
class PipelineBuilder:
    pipeline: Pipeline
    root: Node
    format: str | None = None

    @staticmethod
    def lift(x: Any) -> "PipelineBuilder":

        match x:
            case PipelineBuilder():
                return x

            case FunctionType():
                root: Node = NodeFunction(x)
                return PipelineBuilder(Pipeline(nodes={root}, links=set()), root)

            case NodeFunction():
                root = x
                return PipelineBuilder(Pipeline(nodes={root}, links=set()), root)

            case NodeRemote():
                root = x
                return PipelineBuilder(Pipeline(nodes={root}, links=set()), root)

            case _:
                raise ValueError(x)

    def get_input_node_refs(self, port_name: str, is_free: bool) -> list[NodeInputRef]:
        node_refs = [
            NodeInputRef(node, port) for node in self.pipeline.nodes for port in node.inputs if port.name == port_name
        ]

        if is_free:
            bound_refs = set(map(itemgetter(0), self.pipeline.links))
            node_refs = [x for x in node_refs if x not in bound_refs]

        return node_refs

    def bind(self, **kwargs: Any) -> "PipelineBuilder":
        return self._bind(kwargs, is_strict=True, is_free=False)

    def bind_all(self, **kwargs: Any) -> "PipelineBuilder":
        return self._bind(kwargs, is_strict=False, is_free=True)

    def _bind(self, kwargs: dict[str, Any], is_strict: bool, is_free: bool) -> "PipelineBuilder":
        pipeline = self.pipeline
        for port_name, v in kwargs.items():
            match self.get_input_node_refs(port_name, is_free):
                case []:
                    raise ValueError("node(s) for port `{}` is not found".format(port_name))

                case [ref]:
                    pipeline = self._update_pipeline(pipeline, ref, v)

                case refs:
                    if is_strict:
                        raise ValueError("ambigious node for port `{}`".format(port_name))

                    pipeline = reduce(lambda acc, ref: self._update_pipeline(acc, ref, v), refs, pipeline)
        return PipelineBuilder(pipeline=pipeline, root=self.root, format=self.format)

    def alias(self, name: str) -> "PipelineBuilder":
        return replace(self, pipeline=self.pipeline.add_alias(self.root, name))

    def as_format(self, format: str) -> "PipelineBuilder":
        """Return a builder whose output edge uses an artifact format."""

        if not isinstance(format, str):
            raise TypeError("pipeline builder format must be a string")
        if not format:
            raise ValueError("pipeline builder format must be a non-empty string")
        return replace(self, format=format)

    @staticmethod
    def _update_pipeline(pipeline: Pipeline, ref: NodeInputRef, v: Any) -> Pipeline:
        match v:
            case PipelineBuilder():
                output_ref = NodeOutputRef(v.root, v.root.get_output_port(DEFAULT_PORT))
                link_value = output_ref if v.format is None else FormattedOutputRef(output_ref, v.format)
                return (pipeline | v.pipeline).add_link(ref, link_value)
            case _:
                return pipeline.add_link(ref, Scalar(v))

    def render(self, name: str | None = None) -> Pipeline:
        return replace(self.pipeline, name=name)


lift = PipelineBuilder.lift


def encode(value: Any, adapter: SaveAdapter, artifacts_dir: Path) -> ArtifactRef:
    """Materialize a value with an adapter and return its artifact reference."""

    fd, artifact_path_value = tempfile.mkstemp(prefix="artifact-", dir=artifacts_dir)
    os.close(fd)
    artifact_path = Path(artifact_path_value)

    try:
        adapter.save(str(artifact_path), value)
    except BaseException:
        artifact_path.unlink(missing_ok=True)
        raise

    size = artifact_path.stat().st_size
    sha256 = compute_sha256(artifact_path)
    return ArtifactRef(key=adapter.key, uri=str(artifact_path), sha256=sha256, size=size, tag=adapter.tag)


def _callable_name(func: Callable[..., Any]) -> str:
    return str(getattr(func, "__qualname__", getattr(func, "__name__", type(func).__name__)))


def decode(ref: ArtifactRef, adapter: LoadAdapter) -> Any:
    """Load an artifact reference with an adapter after validating its digest."""

    ref_tag = cast(str, ref.tag)
    if ref_tag not in adapter.accepted_tags:
        accepted_tags = ", ".join(sorted(adapter.accepted_tags))
        raise ValueError(
            "artifact tag `{}` from `{}` is not accepted by load adapter `{}` (accepted tags: {})".format(
                ref_tag, ref.key, _callable_name(adapter.load), accepted_tags
            )
        )

    if adapter.legacy_key_guard and ref.key != adapter.key:
        raise ValueError("artifact ref key does not match adapter")

    artifact_path = Path(ref.uri)
    if artifact_path.stat().st_size != ref.size:
        raise ValueError("artifact ref size does not match file")
    if compute_sha256(artifact_path) != ref.sha256:
        raise ValueError("artifact ref sha256 does not match file")
    return adapter.load(str(artifact_path))


RESERVED_RUN_KWARGS = frozenset({"output", "adapters", "runtimes", "keep"})
RESERVED_RESUME_KWARGS = RESERVED_RUN_KWARGS | frozenset({"kwargs", "from_"})


class Deployment:
    def __init__(
        self,
        client: Any = None,
        pipeline: Pipeline | None = None,
        *,
        runtime_config: Mapping[str, Any] | None = None,
        node_environment_provider: m_node_runtime.NodeEnvironmentProvider | None = None,
        runtime_env_spec: Sequence[Mapping[str, Any]] | None = None,
        _runtime_adapter_fingerprint_sha256: str | None = None,
    ) -> None:
        """Create a deployment.

        ``runtime_config["node_timeout_seconds"]`` optionally bounds non-native
        per-node subprocess runtimes; native nodes still run without a timeout.
        """

        if pipeline is None:
            pipeline = client
            client = None
        self._client = client
        self._pipeline = pipeline
        self._runtime_config = dict(runtime_config or {})
        self._node_environment_provider = node_environment_provider
        self._runtime_env_spec = list(runtime_env_spec or [])
        self._runtime_adapter_fingerprint_sha256 = _runtime_adapter_fingerprint_sha256

    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass

    @overload
    def run(
        self,
        *,
        output: None = None,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
        **kwargs: Any,
    ) -> "Run": ...

    @overload
    def run(
        self,
        *,
        output: str,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
        **kwargs: Any,
    ) -> Any: ...

    def run(
        self,
        *,
        output: str | None = None,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
        **kwargs: Any,
    ) -> Any:
        """Run the pipeline locally.

        ``adapters`` maps ``(output_alias, output_port)`` to a runtime adapter
        override for that output edge.  Daemon-backed ``SPLClient`` runs do not
        serialize Python adapter callables in 0.4.0; use local ``Deployment``
        when applying run-level adapter overrides. ``keep`` controls retained
        local run state: ``False`` keeps old tempdir cleanup, ``True`` retains
        successful and failed runs, and ``"on_failure"`` retains failures.
        ``runtimes`` maps node aliases to runtime names and overrides
        pipeline/runtime-config selection for this run only. Reserved input
        names for direct ``run(...)`` kwargs are ``output``, ``adapters``,
        ``runtimes`` and ``keep``; bind ports with these names before
        ``render()``.
        """

        run = Run(
            self._callback,
            self._pipeline,
            adapters=adapters,
            runtimes=runtimes,
            keep=keep,
            runtime_config=self._runtime_config,
            node_environment_provider=self._node_environment_provider,
            runtime_env_spec=self._runtime_env_spec,
            runtime_adapter_fingerprint_sha256=self._runtime_adapter_fingerprint_sha256,
            **kwargs,
        )
        if output is None:
            return run
        with run:
            return run.value(output)

    @overload
    def resume(
        self,
        run_id: str,
        *,
        from_: m_resume.NodeSelection,
        output: None = None,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        kwargs: Mapping[str, Any] | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
    ) -> "Run": ...

    @overload
    def resume(
        self,
        run_id: str,
        *,
        from_: m_resume.NodeSelection,
        output: str,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        kwargs: Mapping[str, Any] | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
    ) -> Any: ...

    def resume(
        self,
        run_id: str,
        *,
        from_: m_resume.NodeSelection,
        output: str | None = None,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        kwargs: Mapping[str, Any] | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
    ) -> Any:
        """Resume a retained local run by id.

        ``from_`` is the only control surface for recalculation: selected nodes
        plus their descendants run again, while all other nodes are frozen from
        the retained manifest. ``kwargs`` and ``adapters`` are per-resume
        overrides applied only to the new run. For a frozen producer, a pair
        adapter override ignores its save half and uses only its load half.
        """

        if self._pipeline is None:
            raise RuntimeError("resume requires a local pipeline")
        if m_manifest.normalize_keep(keep) is False:
            raise ValueError(
                "keep=False is incompatible with resume because the child run would discard the state needed "
                "for another resume; use keep=True (keep='on_failure' retains the child only if it fails)"
            )
        try:
            parent_run_dir, parent_manifest = m_resume.load_retained_manifest(run_id)
        except FileNotFoundError as exc:
            raise RuntimeError(
                "cannot resume run `{}` because its retained files are unavailable; transient keep=False and "
                "successful keep='on_failure' runs are intentionally removed, so launch the parent with keep=True".format(
                    run_id
                )
            ) from exc
        if parent_manifest.get("status") not in {"failed", "succeeded"}:
            raise RuntimeError(
                "resume requires a terminal retained run; current status is `{}`".format(parent_manifest.get("status"))
            )
        base_kwargs = _kwargs_from_manifest(self._pipeline, parent_manifest)
        if kwargs is not None:
            base_kwargs.update(kwargs)
        plan = m_resume.plan_resume(
            pipeline=self._pipeline,
            parent_manifest=parent_manifest,
            parent_run_dir=parent_run_dir,
            from_=from_,
            kwargs=kwargs,
        )
        run = Run(
            self._callback,
            self._pipeline,
            adapters=adapters,
            runtimes=runtimes,
            keep=keep,
            parent_run_id=str(parent_manifest["run_id"]),
            resume_plan=plan,
            runtime_config=self._runtime_config,
            node_environment_provider=self._node_environment_provider,
            runtime_env_spec=self._runtime_env_spec,
            runtime_adapter_fingerprint_sha256=self._runtime_adapter_fingerprint_sha256,
            **base_kwargs,
        )
        if output is None:
            return run
        with run:
            return run.value(output)

    def _callback(self, node: Node, kwargs: dict[InputPort, Any]) -> dict[str, Any]:
        final_kwargs = {port.name: v for port, v in kwargs.items()}
        output_port = self._single_output_port(node)
        match node:
            case NodeFunction():
                return {output_port.name: node.func(**final_kwargs)}

            case NodeRemote():
                if self._client is None:
                    raise RuntimeError("remote node execution requires a client")
                # The private entry point keeps this canonical pipeline path
                # silent; the public ``run_node`` carries a DeprecationWarning.
                run_node = getattr(self._client, "_run_node_value", None) or self._client.run_node
                return {output_port.name: run_node(node, final_kwargs)}

            case _:
                raise ValueError(node)

    @staticmethod
    def _single_output_port(node: Node) -> OutputPort:
        outputs = node.outputs or []
        if len(outputs) != 1:
            raise RuntimeError(
                "node {} has {} outputs; local Deployment currently supports "
                "exactly one output and requires an explicit daemon/server "
                "output selector for multi-output pipelines".format(node, len(outputs))
            )
        return outputs[0]


class Run:
    def __init__(
        self,
        callback: Callable[..., dict[str, Any]],
        pipeline: Pipeline,
        *,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
        run_id: str | None = None,
        parent_run_id: str | None = None,
        resume_plan: m_resume.ResumePlan | None = None,
        runtime_config: Mapping[str, Any] | None = None,
        node_environment_provider: m_node_runtime.NodeEnvironmentProvider | None = None,
        runtime_env_spec: Sequence[Mapping[str, Any]] | None = None,
        runtime_adapter_fingerprint_sha256: str | None = None,
        _input_values: Mapping[str, Any] | None = None,
        _native_values: bool = False,
        **kwargs: Any,
    ) -> None:
        self._runs_home: Path | None = None
        self._callback = callback
        self._pipeline = pipeline
        if _input_values is not None and kwargs:
            raise TypeError("_input_values cannot be combined with expanded keyword inputs")
        self._kwargs = dict(_input_values) if _input_values is not None else kwargs
        self._native_values = _native_values
        self._keep = m_manifest.normalize_keep(keep)
        self._run_id = run_id or m_manifest.new_run_id()
        self._parent_run_id = parent_run_id
        self._resume_plan = resume_plan
        if resume_plan is not None and parent_run_id is None:
            self._parent_run_id = str(resume_plan.parent_manifest["run_id"])
        reserved_names = RESERVED_RESUME_KWARGS if resume_plan is not None else RESERVED_RUN_KWARGS
        if _input_values is None:
            _warn_reserved_run_input_names(pipeline, reserved_names)
        self._adapter_overrides = _validate_run_adapter_overrides(pipeline, adapters)
        self._save_override_evidence = self._effective_save_override_evidence(resume_plan)
        self._execution_plan_document = m_manifest.run_execution_plan_document(
            self._run_id,
            self._save_override_evidence,
        )
        self._runtime_overrides = m_node_runtime.validate_run_runtime_overrides(pipeline, runtimes)
        self._runtime_config = m_node_runtime.validate_node_runtime_config(runtime_config)
        self._node_environment_provider = node_environment_provider or m_node_runtime.CurrentPythonEnvironmentProvider()
        self._runtime_env_spec = list(runtime_env_spec or [])
        self._runtime_adapter_fingerprint_sha256 = runtime_adapter_fingerprint_sha256
        self._node_runtime_registry = m_node_runtime.NodeRuntimeRegistry()
        self._node_runtime_plans: dict[Node, _ResolvedNodeRuntimePlan] = {}
        self._has_runtime_selection = bool(
            self._runtime_overrides
            or self._runtime_config.get("node_runtime") is not None
            or any(m_node_runtime.RUNTIME_TAG_NAME in node_tags for node_tags in pipeline.tags.values())
        )
        self._deps = _accumulate_pipeline_dependencies(pipeline)
        self._results: dict[Node, dict[str, Any]] = dict()
        self._public_output_values: dict[tuple[Node, str], Any] = {}
        self._visiting_nodes: list[Node] = []
        self._visiting_node_set: set[Node] = set()
        self._artifact_refs: dict[tuple[Node, str, str], ArtifactRef] = dict()
        self._adapter_resolutions: dict[tuple[Node, str], SaveAdapterResolution | AdapterResolution] = dict()
        self._variant_save_resolutions: dict[tuple[Node, str, str], SaveAdapterResolution | AdapterResolution] = dict()
        self._edge_save_adapter_resolutions: dict[
            tuple[Node, str, Node, str], SaveAdapterResolution | AdapterResolution
        ] = dict()
        self._edge_variant_ids: dict[tuple[Node, str, Node, str], str] = dict()
        self._load_adapter_resolutions: dict[tuple[Node, str, Node, str], LoadAdapterResolution] = dict()
        self._frozen_save_adapter_records: dict[tuple[Node, str, Node, str], dict[str, Any]] = dict()
        self._node_inputs: dict[Node, dict[str, Any]] = dict()
        self._node_adapters: dict[Node, dict[str, Any]] = dict()
        self._node_runtimes: dict[Node, dict[str, Any]] = dict()
        self._artifacts_dir: Path | None = None
        self._run_dir: Path | None = None
        self._artifacts_finalizer: Any = None
        self._run_dir_finalizer: Any = None
        self._manifest_writer: m_manifest.RunManifestWriter | None = None
        self._terminal_status: str | None = None
        self._closed = False

    @property
    def run_id(self) -> str:
        """Return the local run id."""

        return self._run_id

    @property
    def run_dir(self) -> Path | None:
        """Return the retained run directory once it has been created."""

        return self._run_dir

    @property
    def manifest_path(self) -> Path | None:
        """Return the manifest path once it has been created."""

        if self._manifest_writer is None:
            return None
        return self._manifest_writer.path

    @property
    def manifest_snapshot(self) -> dict[str, Any] | None:
        """Return an in-memory manifest copy, including deferred transient runs."""

        if self._manifest_writer is None:
            return None
        return deepcopy(self._manifest_writer.data)

    @overload
    def resume(
        self,
        *,
        from_: m_resume.NodeSelection,
        output: None = None,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        kwargs: Mapping[str, Any] | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
    ) -> "Run": ...

    @overload
    def resume(
        self,
        *,
        from_: m_resume.NodeSelection,
        output: str,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        kwargs: Mapping[str, Any] | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
    ) -> Any: ...

    def resume(
        self,
        *,
        from_: m_resume.NodeSelection,
        output: str | None = None,
        adapters: RunAdapterOverrides | None = None,
        runtimes: RunRuntimeOverrides | None = None,
        kwargs: Mapping[str, Any] | None = None,
        keep: m_manifest.KeepPolicy = "on_failure",
    ) -> Any:
        """Resume this retained run from a recalculation set.

        The new run gets a fresh ``run_id`` and records this run's id as
        ``parent_run_id``. ``from_`` selects recalculated nodes; descendants are
        inferred and all other nodes are frozen from this run's manifest. For a
        frozen producer, a pair adapter override ignores its save half and uses
        only its load half.
        """

        if m_manifest.normalize_keep(keep) is False:
            raise ValueError(
                "keep=False is incompatible with resume because the child run would discard the state needed "
                "for another resume; use keep=True (keep='on_failure' retains the child only if it fails)"
            )
        parent_run_dir, parent_manifest = self._parent_manifest()
        merged_kwargs = dict(self._kwargs)
        if kwargs is not None:
            merged_kwargs.update(kwargs)
        plan = m_resume.plan_resume(
            pipeline=self._pipeline,
            parent_manifest=parent_manifest,
            parent_run_dir=parent_run_dir,
            from_=from_,
            kwargs=kwargs,
        )
        run = Run(
            self._callback,
            self._pipeline,
            adapters=adapters,
            runtimes=runtimes,
            keep=keep,
            parent_run_id=str(parent_manifest["run_id"]),
            resume_plan=plan,
            runtime_config=self._runtime_config,
            node_environment_provider=self._node_environment_provider,
            runtime_env_spec=self._runtime_env_spec,
            runtime_adapter_fingerprint_sha256=self._runtime_adapter_fingerprint_sha256,
            **merged_kwargs,
        )
        if output is None:
            return run
        with run:
            return run.value(output)

    def __enter__(self) -> "Run":
        self._ensure_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            self.close()
        except BaseException as close_exc:
            if exc is not None:
                raise close_exc from exc
            raise

    def close(self) -> None:
        if self._closed:
            return
        discovered_error: RuntimeError | None = None
        if self._terminal_status is None and self._manifest_writer is None:
            self._ensure_manifest_writer()
        if self._manifest_writer is not None and self._terminal_status is None:
            discovered_error = self._finish_unfinished_manifest()
        if self._artifacts_finalizer is not None:
            self._artifacts_finalizer()
        if self._run_dir_finalizer is not None and not self._should_retain_terminal():
            self._run_dir_finalizer()
        self._closed = True
        if discovered_error is not None:
            raise discovered_error

    def _finish_unfinished_manifest(self) -> RuntimeError | None:
        writer = self._manifest_writer
        if writer is None:
            return None
        nodes = writer.data.get("nodes")
        node_records = nodes if isinstance(nodes, Mapping) else {}
        if self._all_node_records_are_complete(node_records):
            self._finish_manifest(status="succeeded")
            return None
        recorded_error = self._recorded_node_failure(node_records)
        if recorded_error is not None:
            LOGGER.error(
                "run `%s` reached a failed node but its terminal run status was missing; recording failure: %s",
                self._run_id,
                recorded_error,
            )
            for node in self._pipeline.nodes:
                record = node_records.get(self._node_id(node))
                if self._node_record_is_complete(record) or (
                    isinstance(record, Mapping) and record.get("status") in {"failed", "upstream-failed"}
                ):
                    continue
                self._write_node_manifest(
                    node,
                    status="failed",
                    outputs={},
                    error="run finalization followed another node failure: {}".format(recorded_error),
                    compute_fingerprint=False,
                )
            self._finish_manifest(status="failed", error=recorded_error)
            return RuntimeError(
                "run `{}` had a failed node but no terminal run status; the recorded failure was preserved: {}".format(
                    self._run_id,
                    recorded_error,
                )
            )

        error = self._incomplete_close_error(node_records)
        LOGGER.error("%s", error)
        for node in self._pipeline.nodes:
            node_id = self._node_id(node)
            record = node_records.get(node_id)
            if self._node_record_is_complete(record):
                continue
            self._write_node_manifest(
                node,
                status="failed",
                outputs={},
                error=error,
                compute_fingerprint=False,
            )
        self._finish_manifest(status="failed", error=error)
        return RuntimeError(error)

    def _all_node_records_are_complete(self, node_records: Mapping[Any, Any]) -> bool:
        expected_node_ids = {self._node_id(node) for node in self._pipeline.nodes}
        return set(node_records) == expected_node_ids and all(
            self._node_record_is_complete(node_records[node_id]) for node_id in expected_node_ids
        )

    def _node_record_is_complete(self, record: Any) -> bool:
        if not isinstance(record, Mapping) or record.get("status") not in {"succeeded", "frozen"}:
            return False
        node_id = record.get("id")
        node = next((item for item in self._pipeline.nodes if self._node_id(item) == node_id), None)
        if node is None:
            return False
        outputs = record.get("outputs")
        return isinstance(outputs, Mapping) and set(outputs) == {port.name for port in node.outputs}

    def _recorded_node_failure(self, node_records: Mapping[Any, Any]) -> str | None:
        for node_id, record in sorted(node_records.items(), key=lambda item: str(item[0])):
            if not isinstance(record, Mapping) or record.get("status") not in {"failed", "upstream-failed"}:
                continue
            error = record.get("error")
            if isinstance(error, str) and error:
                return error
            return "node `{}` ended with status `{}` without an error detail".format(
                record.get("alias") or node_id,
                record.get("status"),
            )
        return None

    def _incomplete_close_error(self, node_records: Mapping[Any, Any]) -> str:
        incomplete = []
        expected_nodes = {self._node_id(node): node for node in self._pipeline.nodes}
        for node_id, node in sorted(expected_nodes.items()):
            record = node_records.get(node_id)
            if self._node_record_is_complete(record):
                continue
            status = record.get("status") if isinstance(record, Mapping) else "missing"
            alias = record.get("alias") if isinstance(record, Mapping) else self._node_alias(node)
            incomplete.append("{} ({})".format(alias or node_id, status))
        for node_id, record in sorted(node_records.items(), key=lambda item: str(item[0])):
            if node_id in expected_nodes:
                continue
            status = record.get("status") if isinstance(record, Mapping) else "invalid"
            alias = record.get("alias") if isinstance(record, Mapping) else None
            incomplete.append("{} ({}, unknown node)".format(alias or node_id, status))
        detail = ", ".join(incomplete) if incomplete else "no terminal node records"
        return (
            "run `{}` closed before node finalization completed; non-terminal or inconsistent nodes: {}. "
            "The run is recorded as failed; re-run it and inspect this manifest if the problem repeats."
        ).format(self._run_id, detail)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("run is closed")

    def _get_artifacts_dir(self) -> Path:
        self._ensure_open()
        if self._artifacts_dir is None:
            if self._keep is False:
                artifacts_dir = Path(tempfile.mkdtemp(prefix="spl-run-"))
                self._artifacts_finalizer = weakref.finalize(self, shutil.rmtree, artifacts_dir, ignore_errors=True)
            else:
                artifacts_dir = self._ensure_run_dir() / "artifacts"
                artifacts_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._artifacts_dir = artifacts_dir
        return self._artifacts_dir

    def _ensure_run_dir(self) -> Path:
        if self._run_dir is None:
            run_dir = m_manifest.create_run_dir(self._run_id, self._runs_home)
            self._run_dir = run_dir
            if self._keep == "on_failure":
                self._run_dir_finalizer = weakref.finalize(self, shutil.rmtree, run_dir, ignore_errors=True)
        return self._run_dir

    def _ensure_manifest_writer(self) -> m_manifest.RunManifestWriter | None:
        if self._manifest_writer is None:
            if self._keep is False or self._keep == "on_failure":
                self._manifest_writer = m_manifest.RunManifestWriter.create_deferred(
                    run_id=self._run_id,
                    keep=self._keep,
                    pipeline_name=self._pipeline.name,
                    parent_run_id=self._parent_run_id,
                )
            else:
                self._manifest_writer = m_manifest.RunManifestWriter.create(
                    run_dir=self._ensure_run_dir(),
                    run_id=self._run_id,
                    keep=self._keep,
                    pipeline_name=self._pipeline.name,
                    parent_run_id=self._parent_run_id,
                )
            self._manifest_writer.data["execution_plan"] = m_manifest.run_execution_plan_record(
                self._execution_plan_document
            )
            if self._manifest_writer.path is not None:
                m_manifest.write_run_execution_plan(
                    self._manifest_writer.path.parent,
                    self._execution_plan_document,
                )
            self._manifest_writer.write()
            if self._resume_plan is not None:
                for node in sorted(self._resume_plan.frozen_nodes, key=lambda item: str(item.uuid)):
                    self._ensure_frozen_node_manifest(node)
        return self._manifest_writer

    def _finish_manifest(self, *, status: str, error: str | None = None) -> None:
        writer = self._ensure_manifest_writer()
        if writer is None:
            return
        if self._terminal_status is not None:
            return
        should_retain = m_manifest.should_retain_terminal(self._keep, status)
        if should_retain:
            run_dir = self._ensure_run_dir()
            self._localize_resume_manifest_artifacts(run_dir)
            m_manifest.write_run_execution_plan(run_dir, self._execution_plan_document)
            writer.materialize(run_dir)
        writer.finish(status=status, error=error)
        self._terminal_status = status
        if should_retain and self._run_dir_finalizer is not None:
            self._run_dir_finalizer.detach()

    def _localize_resume_manifest_artifacts(self, run_dir: Path) -> None:
        writer = self._manifest_writer
        if writer is None or self._resume_plan is None:
            return
        nodes = writer.data.get("nodes")
        if isinstance(nodes, Mapping):
            localized_nodes: dict[str, Any] = {}
            for node_id, raw_node in nodes.items():
                if not isinstance(raw_node, Mapping):
                    localized_nodes[str(node_id)] = raw_node
                    continue
                node_record = dict(raw_node)
                for section in ("inputs", "outputs"):
                    values = node_record.get(section)
                    if not isinstance(values, Mapping):
                        continue
                    node_record[section] = {
                        str(port): (
                            m_resume.localize_output_record(cast(Mapping[str, Any], value), run_dir)
                            if isinstance(value, Mapping)
                            else value
                        )
                        for port, value in values.items()
                    }
                localized_nodes[str(node_id)] = node_record
            writer.data["nodes"] = localized_nodes
        edges = writer.data.get("edges")
        if isinstance(edges, list):
            localized_edges: list[Any] = []
            for raw_edge in edges:
                if not isinstance(raw_edge, Mapping):
                    localized_edges.append(raw_edge)
                    continue
                edge = dict(raw_edge)
                artifact = edge.get("artifact")
                if isinstance(artifact, Mapping):
                    edge["artifact"] = m_resume.localize_output_record(
                        cast(Mapping[str, Any], artifact),
                        run_dir,
                    )
                localized_edges.append(edge)
            writer.data["edges"] = localized_edges
        writer.write()

    def _should_retain_terminal(self) -> bool:
        return self._terminal_status is not None and m_manifest.should_retain_terminal(
            self._keep, self._terminal_status
        )

    def _round_trip_artifact(
        self, value: Any, source_ref: NodeOutputRef | None = None, adapter_format: str | None = None
    ) -> Any:
        if adapter_format is None and type(value) in _JSON_NATIVE_TYPES:
            # ADR 002 keeps this pre-resolution shortcut: implicit JSON-native values and the resolved built-in
            # json adapter both return the original object without files, but avoiding resolver work keeps this
            # hot path fast.
            if not self._native_values:
                validate_json_value(value)
            return value

        return self._round_trip_resolved(value, source_ref, adapter_format, run_override=None)

    def _round_trip_artifact_override(
        self, value: Any, source_ref: NodeOutputRef, adapter_format: str | None, run_override: RuntimeAdapter
    ) -> Any:
        return self._round_trip_resolved(value, source_ref, adapter_format, run_override)

    def _round_trip_resolved(
        self,
        value: Any,
        source_ref: NodeOutputRef | None,
        adapter_format: str | None,
        run_override: RuntimeAdapter | None,
    ) -> Any:
        resolution = self._pipeline.resolve_adapter_binding(
            py_type=type(value), format=adapter_format, run_override=run_override
        )
        if resolution is None:
            if adapter_format is not None:
                raise ValueError(
                    "pipeline adapter is not found for python type ({}) and format `{}`".format(
                        type(value), adapter_format
                    )
                )
            return value

        adapter = resolution.adapter
        if source_ref is not None:
            self._adapter_resolutions[(source_ref.node, source_ref.port.name)] = resolution
        if adapter is BUILTIN_JSON_ADAPTER and type(value) in _JSON_NATIVE_TYPES:
            validate_json_value(value)
            return value

        self._ensure_open()
        if source_ref is None:
            ref = encode(value, adapter, self._get_artifacts_dir())
        else:
            try:
                ref = self._materialize_source_output(value, source_ref, resolution)
            except _SourceOutputCommitError as exc:
                self._finalize_lazy_source_output_failure(source_ref, exc)
                raise
        return decode(ref, adapter)

    def _resolve_edge_adapter_bindings(
        self,
        value: Any,
        source_ref: NodeOutputRef,
        target_node: Node,
        target_port: InputPort,
        adapter_format: str | None,
        run_override: RuntimeAdapter | None,
    ) -> tuple[SaveAdapterResolution | None, LoadAdapterResolution | None]:
        save = self._pipeline.resolve_save_adapter_binding(
            py_type=type(value), format=adapter_format, run_override=run_override
        )
        target_type = _runtime_input_type_hint(target_node, target_port)
        load: LoadAdapterResolution | None
        if run_override is not None:
            load = LoadAdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE)
        elif isinstance(target_type, type):
            load = self._pipeline.resolve_load_adapter_binding(
                py_type=target_type,
                format=adapter_format,
            )
        elif isinstance(target_type, str):
            load = self._pipeline.resolve_load_adapter_binding_by_type_name(
                type_name=target_type,
                format=adapter_format,
            )
        elif save is not None and isinstance(save.adapter, Adapter | BuiltInJsonAdapter):
            load = LoadAdapterResolution(save.adapter, save.source)
        elif adapter_format is not None:
            load = self._pipeline.resolve_load_adapter_binding_by_format(format=adapter_format)
        elif save is not None:
            load_adapter = self._pipeline.resolve_load_adapter(key=save.adapter.key)
            load = (
                None if load_adapter is None else LoadAdapterResolution(load_adapter, AdapterResolutionSource.PIPELINE)
            )
        else:
            load = None

        if adapter_format is None and save is None and load is None:
            return None, None
        if save is None:
            raise ValueError(
                "pipeline adapter is not found for save python type ({}) and format `{}`".format(
                    type(value), adapter_format or "<default>"
                )
            )
        if target_type is None and load is None:
            label = self._node_alias(target_node) or self._node_name(target_node)
            raise ValueError(
                "pipeline load adapter cannot be resolved for {}.{} because its Python input type is unknown".format(
                    label, target_port.name
                )
            )
        if load is None:
            raise ValueError(
                "pipeline adapter is not found for load python type ({}) and format `{}`".format(
                    target_type, adapter_format or "<default>"
                )
            )

        edge_key = (source_ref.node, source_ref.port.name, target_node, target_port.name)
        variant_id = self._save_adapter_variant_id(save.adapter)
        self._adapter_resolutions[(source_ref.node, source_ref.port.name)] = save
        self._variant_save_resolutions[(source_ref.node, source_ref.port.name, variant_id)] = save
        self._edge_save_adapter_resolutions[edge_key] = save
        self._edge_variant_ids[edge_key] = variant_id
        self._load_adapter_resolutions[edge_key] = load
        return save, load

    @staticmethod
    def _save_adapter_variant_id(adapter: SaveAdapter) -> str:
        return hashlib.sha256(canonical_json_bytes(save_adapter_identity(adapter))).hexdigest()

    def _resolve_static_save_adapter(
        self,
        type_hint: type[Any] | str | None,
        *,
        adapter_format: str | None,
        run_override: RuntimeAdapter | None,
    ) -> SaveAdapterResolution | None:
        if run_override is not None:
            return SaveAdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE)
        registered: dict[str, SaveAdapter] = {
            **self._pipeline.adapters,
            **self._pipeline.save_adapters,
        }
        if isinstance(type_hint, type):
            try:
                return self._pipeline.resolve_save_adapter_binding(
                    py_type=type_hint,
                    format=adapter_format,
                )
            except ValueError:
                prefix = "{}.{}@".format(type_hint.__module__, type_hint.__qualname__)
                candidate_keys = [
                    key
                    for key in sorted(registered)
                    if key.startswith(prefix) and (adapter_format is None or key.rpartition("@")[2] == adapter_format)
                ]
                raise ValueError(
                    "pipeline save adapter lookup is ambiguous for Python output type `{}`; candidates: {}".format(
                        "{}.{}".format(type_hint.__module__, type_hint.__qualname__),
                        ", ".join("`{}`".format(key) for key in candidate_keys),
                    )
                ) from None
        if isinstance(type_hint, str):
            canonical = type_hint if "." in type_hint else "builtins.{}".format(type_hint)
            candidates = [
                (key, adapter)
                for key, adapter in sorted(registered.items())
                if (
                    key.rpartition("@")[0] == canonical
                    or ("." not in type_hint and key.rpartition("@")[0].endswith(".{}".format(type_hint)))
                )
                and (adapter_format is None or key.rpartition("@")[2] == adapter_format)
            ]
            if len(candidates) > 1:
                raise ValueError(
                    "pipeline save adapter lookup is ambiguous for Python output type hint `{}`; candidates: {}".format(
                        type_hint,
                        ", ".join("`{}`".format(key) for key, _ in candidates),
                    )
                )
            if candidates:
                source = (
                    AdapterResolutionSource.EDGE if adapter_format is not None else AdapterResolutionSource.PIPELINE
                )
                return SaveAdapterResolution(candidates[0][1], source)
        if type_hint is None and adapter_format is not None:
            candidates = [
                (key, adapter)
                for key, adapter in sorted(registered.items())
                if key.rpartition("@")[2] == adapter_format
            ]
            if len(candidates) > 1:
                raise ValueError(
                    "pipeline save adapter lookup is ambiguous for explicit format `{}`; candidates: {}".format(
                        adapter_format,
                        ", ".join("`{}`".format(key) for key, _ in candidates),
                    )
                )
            if candidates:
                return SaveAdapterResolution(candidates[0][1], AdapterResolutionSource.EDGE)
        return None

    def _isolated_output_plan(
        self,
        node: Node,
        output_port: OutputPort,
        output_request: _OutputMaterializationRequest | None,
    ) -> m_node_runtime.ArtifactOutputPlan | None:
        source_ref = NodeOutputRef(node, output_port)
        run_override = self._adapter_override_for(source_ref)
        label = self._node_alias(node) or self._node_name(node)
        type_hint = _runtime_output_type_hint(node, output_port)
        requests = self._outgoing_output_requests(source_ref)
        if output_request is not None:
            request_key = (
                output_request[3],
                output_request[4].name,
                output_request[1],
            )
            if all((target, port.name, adapter_format) != request_key for target, port, adapter_format in requests):
                requests.append((output_request[3], output_request[4], output_request[1]))
        variants: dict[str, SaveAdapterResolution] = {}
        json_edges: list[tuple[tuple[Node, str, Node, str], SaveAdapterResolution]] = []
        for target_node, target_port, adapter_format in requests:
            try:
                save, load = self._resolve_static_edge_requirement(
                    source_ref,
                    target_node,
                    target_port,
                    type_hint=type_hint,
                    adapter_format=adapter_format,
                    run_override=run_override,
                )
            except ValueError as exc:
                raise ValueError(
                    "adapter_resolution: isolated node `{}` output port `{}` cannot resolve graph requirement "
                    "for {}.{}: {}. Select an exact adapter with `.as_format()` or a run-level adapter "
                    "override.".format(
                        label,
                        output_port.name,
                        self._node_alias(target_node) or self._node_name(target_node),
                        target_port.name,
                        exc,
                    )
                ) from None
            if save is None or load is None:
                continue
            edge_key = (node, output_port.name, target_node, target_port.name)
            self._edge_save_adapter_resolutions[edge_key] = save
            self._load_adapter_resolutions[edge_key] = load
            if save.adapter is BUILTIN_JSON_ADAPTER and load.adapter is BUILTIN_JSON_ADAPTER:
                json_edges.append((edge_key, save))
                continue
            variant_id = self._save_adapter_variant_id(save.adapter)
            self._edge_variant_ids[edge_key] = variant_id
            self._variant_save_resolutions[(node, output_port.name, variant_id)] = save
            existing = variants.get(variant_id)
            if existing is None or self._adapter_source_rank(save.source) > self._adapter_source_rank(existing.source):
                variants[variant_id] = save

        if not requests:
            try:
                terminal_resolution = self._resolve_static_save_adapter(
                    type_hint,
                    adapter_format=None,
                    run_override=run_override,
                )
            except ValueError as exc:
                raise ValueError(
                    "adapter_resolution: isolated node `{}` output port `{}` is ambiguous: {}. "
                    "Select an exact adapter with `.as_format()` or a run-level adapter override.".format(
                        label, output_port.name, exc
                    )
                ) from None
            if terminal_resolution is not None and terminal_resolution.adapter is not BUILTIN_JSON_ADAPTER:
                variant_id = self._save_adapter_variant_id(terminal_resolution.adapter)
                variants[variant_id] = terminal_resolution
                self._variant_save_resolutions[(node, output_port.name, variant_id)] = terminal_resolution
        if variants and json_edges:
            for edge_key, resolution in json_edges:
                variant_id = self._save_adapter_variant_id(resolution.adapter)
                self._edge_variant_ids[edge_key] = variant_id
                variants.setdefault(variant_id, resolution)
        if not variants:
            return None
        for variant_id, resolution in variants.items():
            self._variant_save_resolutions[(node, output_port.name, variant_id)] = resolution
        ordered = sorted(variants.items())
        primary_id, primary = ordered[0]
        additional = tuple(
            m_node_runtime.ArtifactOutputVariantPlan(
                variant_id=variant_id,
                save_adapter=resolution.adapter,
                resolution_source=str(resolution.source),
            )
            for variant_id, resolution in ordered[1:]
        )
        self._adapter_resolutions[(node, output_port.name)] = primary
        return m_node_runtime.ArtifactOutputPlan(
            save_adapter=primary.adapter,
            resolution_source=str(primary.source),
            artifacts_dir=self._get_artifacts_dir(),
            variant_id=primary_id,
            additional_variants=additional,
        )

    @staticmethod
    def _adapter_source_rank(source: AdapterResolutionSource) -> int:
        return {
            AdapterResolutionSource.PORT_DEFAULT: 0,
            AdapterResolutionSource.PIPELINE: 1,
            AdapterResolutionSource.EDGE: 2,
            AdapterResolutionSource.RUN_OVERRIDE: 3,
        }[source]

    def _outgoing_output_requests(
        self,
        source_ref: NodeOutputRef,
    ) -> list[tuple[Node, InputPort, str | None]]:
        requests: list[tuple[Node, InputPort, str | None]] = []
        for target_ref, value in sorted(self._pipeline.links, key=lambda item: node_input_ref_sort_key(item[0])):
            if isinstance(value, FormattedOutputRef) and value.out_ref == source_ref:
                requests.append((target_ref.node, target_ref.port, value.format))
            elif isinstance(value, NodeOutputRef) and value == source_ref:
                requests.append((target_ref.node, target_ref.port, None))
        return requests

    def _resolve_static_edge_requirement(
        self,
        source_ref: NodeOutputRef,
        target_node: Node,
        target_port: InputPort,
        *,
        type_hint: type[Any] | str | None,
        adapter_format: str | None,
        run_override: RuntimeAdapter | None,
    ) -> tuple[SaveAdapterResolution | None, LoadAdapterResolution | None]:
        del source_ref
        if run_override is not None:
            return (
                SaveAdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE),
                LoadAdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE),
            )
        target_type = _runtime_input_type_hint(target_node, target_port)
        if type_hint is None and adapter_format is None:
            pairs = self._untyped_adapter_pairs(target_type)
            if len(pairs) > 1:
                raise ValueError(
                    "untyped/Any output has multiple valid save adapters: {}".format(
                        ", ".join(
                            "`{}` (format `{}`)".format(save_adapter.key, save_adapter.key.rpartition("@")[2])
                            for save_adapter, _ in pairs
                        )
                    )
                )
            if pairs:
                save_adapter, load_adapter = pairs[0]
                return (
                    SaveAdapterResolution(save_adapter, AdapterResolutionSource.PIPELINE),
                    LoadAdapterResolution(load_adapter, AdapterResolutionSource.PIPELINE),
                )
        save = self._resolve_static_save_adapter(
            type_hint,
            adapter_format=adapter_format,
            run_override=None,
        )
        load = self._resolve_static_load_adapter(target_type, adapter_format=adapter_format)
        if save is None and type_hint is None and load is not None and load.adapter is not BUILTIN_JSON_ADAPTER:
            candidates = self._untyped_save_candidates(load.adapter, adapter_format=adapter_format)
            if len(candidates) > 1:
                raise ValueError(
                    "untyped/Any output has multiple valid save adapters: {}".format(
                        ", ".join(
                            "`{}` (format `{}`)".format(adapter.key, adapter.key.rpartition("@")[2])
                            for adapter in candidates
                        )
                    )
                )
            if candidates:
                source = (
                    AdapterResolutionSource.EDGE if adapter_format is not None else AdapterResolutionSource.PIPELINE
                )
                save = SaveAdapterResolution(candidates[0], source)
        if save is None:
            if type_hint is None and adapter_format is None and (load is None or load.adapter is BUILTIN_JSON_ADAPTER):
                return None, load
            raise ValueError(
                "no unambiguous save adapter for static Python type `{}` and format `{}`".format(
                    type_hint if type_hint is not None else "<unknown>",
                    adapter_format or "<default>",
                )
            )
        if load is None:
            if isinstance(save.adapter, Adapter | BuiltInJsonAdapter):
                load = LoadAdapterResolution(save.adapter, save.source)
            elif adapter_format is not None:
                load = self._pipeline.resolve_load_adapter_binding_by_format(format=adapter_format)
            else:
                resolved_load_adapter = self._pipeline.resolve_load_adapter(key=save.adapter.key)
                if resolved_load_adapter is not None:
                    load = LoadAdapterResolution(resolved_load_adapter, AdapterResolutionSource.PIPELINE)
        if load is None:
            raise ValueError("no load adapter is registered for the downstream input")
        if save.adapter.tag not in load.adapter.accepted_tags:
            raise ValueError(
                "save adapter tag `{}` is not accepted by load adapter `{}`".format(
                    save.adapter.tag,
                    load.adapter.key,
                )
            )
        return save, load

    def _untyped_adapter_pairs(
        self,
        target_type: type[Any] | str | None,
    ) -> list[tuple[SaveAdapter, LoadAdapter]]:
        if target_type is None:
            return []
        if isinstance(target_type, type):
            target_name = "{}.{}".format(target_type.__module__, target_type.__qualname__)
        else:
            target_name = target_type if "." in target_type else "builtins.{}".format(target_type)
        loads: dict[str, LoadAdapter] = {**self._pipeline.adapters, **self._pipeline.load_adapters}
        pairs: dict[tuple[str, str], tuple[SaveAdapter, LoadAdapter]] = {}
        for load_key, load_adapter in sorted(loads.items()):
            load_type = load_key.rpartition("@")[0]
            if load_type != target_name:
                continue
            for save_adapter in self._untyped_save_candidates(load_adapter, adapter_format=None):
                save_id = self._save_adapter_variant_id(save_adapter)
                load_id = hashlib.sha256(canonical_json_bytes(load_adapter_identity(load_adapter))).hexdigest()
                pairs.setdefault((save_id, load_id), (save_adapter, load_adapter))
        return [pairs[key] for key in sorted(pairs)]

    def _resolve_static_load_adapter(
        self,
        type_hint: type[Any] | str | None,
        *,
        adapter_format: str | None,
    ) -> LoadAdapterResolution | None:
        if isinstance(type_hint, type):
            return self._pipeline.resolve_load_adapter_binding(py_type=type_hint, format=adapter_format)
        if isinstance(type_hint, str):
            return self._pipeline.resolve_load_adapter_binding_by_type_name(
                type_name=type_hint,
                format=adapter_format,
            )
        if adapter_format is not None:
            return self._pipeline.resolve_load_adapter_binding_by_format(format=adapter_format)
        return None

    def _untyped_save_candidates(
        self,
        load_adapter: LoadAdapter,
        *,
        adapter_format: str | None,
    ) -> list[SaveAdapter]:
        registered: dict[str, SaveAdapter] = {**self._pipeline.adapters, **self._pipeline.save_adapters}
        candidates: dict[str, SaveAdapter] = {}
        for key, adapter in sorted(registered.items()):
            _, _, key_format = key.rpartition("@")
            if adapter_format is not None and key_format != adapter_format:
                continue
            if adapter.tag not in load_adapter.accepted_tags:
                continue
            if load_adapter.legacy_key_guard and adapter.key != load_adapter.key:
                continue
            candidates.setdefault(self._save_adapter_variant_id(adapter), adapter)
        return [candidates[key] for key in sorted(candidates)]

    def _round_trip_edge(
        self,
        value: Any,
        source_ref: NodeOutputRef,
        target_node: Node,
        target_port: InputPort,
        adapter_format: str | None,
        run_override: RuntimeAdapter | None,
    ) -> Any:
        if isinstance(value, m_node_runtime.ArtifactOutput):
            promoted_variant, _, promoted_load = self._resolve_promoted_output_edge(
                value,
                source_ref,
                target_node,
                target_port,
                adapter_format,
                run_override,
            )
            return decode(promoted_variant.ref, promoted_load.adapter)
        save, load = self._resolve_edge_adapter_bindings(
            value,
            source_ref,
            target_node,
            target_port,
            adapter_format,
            run_override,
        )
        if save is None or load is None:
            return value
        if (
            save.adapter is BUILTIN_JSON_ADAPTER
            and load.adapter is BUILTIN_JSON_ADAPTER
            and type(value) in _JSON_NATIVE_TYPES
        ):
            if not self._native_values:
                validate_json_value(value)
            return value

        self._ensure_open()
        try:
            ref = self._materialize_source_output(value, source_ref, save)
        except _SourceOutputCommitError as exc:
            self._finalize_lazy_source_output_failure(source_ref, exc)
            raise
        return decode(ref, load.adapter)

    def _transport_edge_input(
        self,
        value: Any,
        source_ref: NodeOutputRef,
        target_node: Node,
        target_port: InputPort,
        adapter_format: str | None,
        run_override: RuntimeAdapter | None,
    ) -> m_node_runtime.NodeInputTransport:
        if isinstance(value, m_node_runtime.ArtifactOutput):
            promoted_variant, _, promoted_load = self._resolve_promoted_output_edge(
                value,
                source_ref,
                target_node,
                target_port,
                adapter_format,
                run_override,
            )
            return m_node_runtime.ArtifactInput(
                ref=promoted_variant.ref,
                load_adapter=promoted_load.adapter,
                resolution_source=str(promoted_load.source),
            )
        save, load = self._resolve_edge_adapter_bindings(
            value,
            source_ref,
            target_node,
            target_port,
            adapter_format,
            run_override,
        )
        if save is None or load is None:
            return m_node_runtime.InlineInput(value)
        if (
            save.adapter is BUILTIN_JSON_ADAPTER
            and load.adapter is BUILTIN_JSON_ADAPTER
            and type(value) in _JSON_NATIVE_TYPES
        ):
            validate_json_value(value)
            return m_node_runtime.InlineInput(value)
        self._ensure_open()
        try:
            ref = self._materialize_source_output(value, source_ref, save)
        except _SourceOutputCommitError as exc:
            self._finalize_lazy_source_output_failure(source_ref, exc)
            raise
        return m_node_runtime.ArtifactInput(
            ref=ref,
            load_adapter=load.adapter,
            resolution_source=str(load.source),
        )

    def _resolve_promoted_output_edge(
        self,
        value: m_node_runtime.ArtifactOutput,
        source_ref: NodeOutputRef,
        target_node: Node,
        target_port: InputPort,
        adapter_format: str | None,
        run_override: RuntimeAdapter | None,
    ) -> tuple[m_node_runtime.ArtifactOutputVariant, SaveAdapterResolution, LoadAdapterResolution]:
        edge_key = (source_ref.node, source_ref.port.name, target_node, target_port.name)
        expected_variant_id = self._edge_variant_ids.get(edge_key)
        variants = {variant.variant_id: variant for variant in value.variants()}
        if expected_variant_id is None:
            expected_save, expected_load = self._resolve_static_edge_requirement(
                source_ref,
                target_node,
                target_port,
                type_hint=_runtime_output_type_hint(source_ref.node, source_ref.port),
                adapter_format=adapter_format,
                run_override=run_override,
            )
            if expected_save is None or expected_load is None:
                raise ValueError("isolated artifact output has no resolved adapter requirement for this edge")
            expected_variant_id = self._save_adapter_variant_id(expected_save.adapter)
            self._edge_save_adapter_resolutions[edge_key] = expected_save
            self._load_adapter_resolutions[edge_key] = expected_load
            self._edge_variant_ids[edge_key] = expected_variant_id
        variant = variants.get(expected_variant_id)
        if variant is None:
            raise ValueError(
                "isolated output port `{}` does not contain the exact artifact variant required by this edge".format(
                    source_ref.port.name
                )
            )
        actual_format = variant.save_adapter.key.rpartition("@")[2]
        if run_override is not None and run_override.key != variant.save_adapter.key:
            raise ValueError(
                "isolated output port `{}` was not materialized with its selected run-level adapter".format(
                    source_ref.port.name
                )
            )
        if run_override is None and adapter_format is not None and adapter_format != actual_format:
            raise ValueError(
                "isolated output port `{}` selected the wrong materialized variant `{}`".format(
                    source_ref.port.name,
                    actual_format,
                )
            )
        save = cast(
            SaveAdapterResolution,
            self._edge_save_adapter_resolutions.get(edge_key),
        )
        if save is None:
            try:
                save_source = AdapterResolutionSource(variant.resolution_source)
            except ValueError:
                save_source = AdapterResolutionSource.PIPELINE
            save = SaveAdapterResolution(variant.save_adapter, save_source)
        target_type = _runtime_input_type_hint(target_node, target_port)
        load = self._load_adapter_resolutions.get(edge_key)
        if load is not None:
            pass
        elif run_override is not None:
            load = LoadAdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE)
        elif isinstance(target_type, type):
            load = self._pipeline.resolve_load_adapter_binding(
                py_type=target_type,
                format=adapter_format,
            )
        elif isinstance(target_type, str):
            load = self._pipeline.resolve_load_adapter_binding_by_type_name(
                type_name=target_type,
                format=adapter_format,
            )
        elif isinstance(variant.save_adapter, Adapter | BuiltInJsonAdapter):
            load = LoadAdapterResolution(variant.save_adapter, save.source)
        elif adapter_format is not None:
            load = self._pipeline.resolve_load_adapter_binding_by_format(format=adapter_format)
        else:
            load_adapter = self._pipeline.resolve_load_adapter(key=variant.ref.key)
            load = (
                None if load_adapter is None else LoadAdapterResolution(load_adapter, AdapterResolutionSource.PIPELINE)
            )
        if load is None:
            label = self._node_alias(target_node) or self._node_name(target_node)
            raise ValueError(
                "pipeline load adapter cannot be resolved for {}.{} from isolated artifact `{}`".format(
                    label,
                    target_port.name,
                    variant.ref.key,
                )
            )
        if cast(str, variant.ref.tag) not in load.adapter.accepted_tags:
            raise ValueError(
                "pipeline load adapter for {}.{} does not accept isolated artifact tag `{}`".format(
                    self._node_alias(target_node) or self._node_name(target_node),
                    target_port.name,
                    variant.ref.tag,
                )
            )
        self._adapter_resolutions.setdefault((source_ref.node, source_ref.port.name), save)
        self._variant_save_resolutions.setdefault((source_ref.node, source_ref.port.name, expected_variant_id), save)
        self._edge_save_adapter_resolutions[edge_key] = save
        self._load_adapter_resolutions[edge_key] = load
        return variant, save, load

    def _materialize_source_output(
        self,
        value: Any,
        source_ref: NodeOutputRef,
        resolution: SaveAdapterResolution | AdapterResolution,
    ) -> ArtifactRef:
        adapter = resolution.adapter
        variant_id = self._save_adapter_variant_id(adapter)
        cache_key = (source_ref.node, source_ref.port.name, variant_id)
        self._variant_save_resolutions[cache_key] = resolution
        if cache_key not in self._artifact_refs:
            try:
                self._artifact_refs[cache_key] = encode(value, adapter, self._get_artifacts_dir())
            except Exception as exc:
                raise self._source_output_commit_error(
                    source_ref,
                    stage="adapter save/materialization",
                    exc=exc,
                ) from exc
        ref = self._artifact_refs[cache_key]
        try:
            self._record_materialized_output(source_ref, ref, resolution)
        except Exception as exc:
            raise self._source_output_commit_error(
                source_ref,
                stage="output manifest persistence",
                exc=exc,
            ) from exc
        return ref

    def _source_output_commit_error(
        self,
        source_ref: NodeOutputRef,
        *,
        stage: str,
        exc: Exception,
    ) -> _SourceOutputCommitError:
        label = self._node_alias(source_ref.node) or self._node_name(source_ref.node)
        return _SourceOutputCommitError(
            "output commit failed for node `{}` port `{}` during {}: {}".format(
                label,
                source_ref.port.name,
                stage,
                repr(exc),
            )
        )

    def _finalize_lazy_source_output_failure(
        self,
        source_ref: NodeOutputRef,
        exc: _SourceOutputCommitError,
    ) -> None:
        error = repr(exc)
        try:
            self._write_node_manifest(
                source_ref.node,
                status="failed",
                outputs={},
                error=error,
                compute_fingerprint=False,
            )
            self._finish_manifest(status="failed", error=error)
        except BaseException as finalization_exc:
            raise finalization_exc from exc

    def _adapter_override_for(self, source_ref: NodeOutputRef | None) -> RuntimeAdapter | None:
        if source_ref is None:
            return None
        return self._adapter_overrides.get((source_ref.node, source_ref.port.name))

    def _effective_save_override_evidence(
        self,
        resume_plan: m_resume.ResumePlan | None,
    ) -> dict[tuple[str, str], Mapping[str, Any]]:
        evidence: dict[tuple[str, str], Mapping[str, Any]] = {}
        if resume_plan is not None:
            frozen_ids = {str(node.uuid) for node in resume_plan.frozen_nodes}
            evidence.update(
                {
                    key: dict(identity)
                    for key, identity in resume_plan.historical_adapter_overrides.items()
                    if key[0] in frozen_ids
                }
            )
        for (node, port), adapter in self._adapter_overrides.items():
            if resume_plan is not None and node not in resume_plan.recalculated_nodes:
                continue
            evidence[(str(node.uuid), port)] = save_adapter_identity(adapter)
        return evidence

    def _parent_manifest(self) -> tuple[Path, dict[str, Any]]:
        if (
            self._manifest_writer is not None
            and self._manifest_writer.path is not None
            and self._manifest_writer.path.exists()
        ):
            status = self._manifest_writer.data.get("status")
            if status not in {"failed", "succeeded"}:
                raise RuntimeError("resume requires a terminal retained run; current status is `{}`".format(status))
            return self._manifest_writer.path.parent, dict(self._manifest_writer.data)
        if self._run_dir is not None and self._run_dir.exists():
            manifest_path = self._run_dir / m_manifest.RUN_MANIFEST_FILENAME
            if manifest_path.exists():
                data = cast(dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8")))
                if data.get("status") not in {"failed", "succeeded"}:
                    raise RuntimeError(
                        "resume requires a terminal retained run; current status is `{}`".format(data.get("status"))
                    )
                return self._run_dir, data
        try:
            run_dir, data = m_resume.load_retained_manifest(self._run_id)
        except FileNotFoundError as exc:
            raise RuntimeError(
                "cannot resume run `{}` because no retained manifest exists; run with keep=True or keep='on_failure' "
                "for failed runs".format(self._run_id)
            ) from exc
        if data.get("status") not in {"failed", "succeeded"}:
            raise RuntimeError(
                "resume requires a terminal retained run; current status is `{}`".format(data.get("status"))
            )
        return run_dir, data

    def _is_frozen_node(self, node: Node) -> bool:
        return self._resume_plan is not None and node not in self._resume_plan.recalculated_nodes

    def _record_materialized_output(
        self,
        source_ref: NodeOutputRef,
        ref: ArtifactRef,
        resolution: SaveAdapterResolution | AdapterResolution,
    ) -> None:
        if self._manifest_writer is None:
            return
        variant_id = self._save_adapter_variant_id(resolution.adapter)
        self._artifact_refs[(source_ref.node, source_ref.port.name, variant_id)] = ref
        self._variant_save_resolutions[(source_ref.node, source_ref.port.name, variant_id)] = resolution
        output_record, adapter_record = self._artifact_output_manifest_records(
            source_ref.node,
            source_ref.port.name,
        )
        self._set_node_output(source_ref.node, source_ref.port.name, output_record)
        self._set_node_adapter(source_ref.node, source_ref.port.name, adapter_record)
        self._write_node_manifest(source_ref.node, status=self._node_status(source_ref.node))

    def _artifact_output_manifest_records(
        self,
        node: Node,
        port_name: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        variants: list[dict[str, Any]] = []
        for ref_node, ref_port, variant_id in sorted(self._artifact_refs, key=lambda item: item[2]):
            if ref_node != node or ref_port != port_name:
                continue
            ref = self._artifact_refs[(ref_node, ref_port, variant_id)]
            resolution = self._variant_save_resolutions.get((ref_node, ref_port, variant_id))
            if resolution is None:
                continue
            save_record = m_manifest.adapter_record(
                save_adapter_identity(resolution.adapter),
                str(resolution.source),
            )
            variants.append(
                {
                    "variant_id": variant_id,
                    "artifact": m_manifest.artifact_record(ref, run_dir=self._run_dir),
                    "save": save_record,
                }
            )
        if not variants:
            raise RuntimeError("artifact output manifest has no materialized variants")
        primary = variants[0]
        output_record = dict(primary["artifact"])
        adapter_record = dict(primary["save"])
        if len(variants) > 1:
            output_record["variants"] = variants
            adapter_record["variants"] = [{"variant_id": item["variant_id"], "save": item["save"]} for item in variants]
        return output_record, adapter_record

    def _ensure_frozen_node_manifest(self, node: Node) -> None:
        writer = self._manifest_writer
        if writer is None or self._resume_plan is None:
            return
        node_id = self._node_id(node)
        if node_id in writer.data["nodes"]:
            return
        parent_record = m_resume.manifest_node_record(self._resume_plan.parent_manifest, node)
        if parent_record is None:
            raise m_resume.ResumeValidationError(
                "{}: missing node record; recalculate with from_=...".format(self._node_alias(node) or node_id)
            )
        writer.set_node(
            m_resume.frozen_node_record(
                parent_record,
                parent_run_dir=self._resume_plan.parent_run_dir,
                run_dir=self._run_dir,
            )
        )

    def _restore_frozen_result(self, node: Node) -> dict[str, Any]:
        self._ensure_frozen_node_manifest(node)
        if self._resume_plan is None:
            raise RuntimeError("frozen result requested outside resume")
        result: dict[str, Any] = {}
        for port in node.outputs:
            record = m_resume.manifest_output_record(self._resume_plan.parent_manifest, node, port.name)
            result[port.name] = self._value_from_frozen_record(record, NodeOutputRef(node, port), None)
        return result

    def _get_frozen_edge_input(
        self,
        source_ref: NodeOutputRef,
        adapter_format: str | None,
        target_node: Node,
        target_port: InputPort,
        *,
        isolated: bool,
    ) -> m_node_runtime.NodeInputTransport:
        self._ensure_frozen_node_manifest(source_ref.node)
        if self._resume_plan is None:
            raise RuntimeError("frozen edge input requested outside resume")
        record = m_resume.manifest_output_record(
            self._resume_plan.parent_manifest, source_ref.node, source_ref.port.name
        )
        if record.get("kind") == "json":
            value = record.get("value")
            if isolated:
                return self._transport_edge_input(
                    value,
                    source_ref,
                    target_node,
                    target_port,
                    adapter_format,
                    self._adapter_override_for(source_ref),
                )
            return m_node_runtime.InlineInput(
                self._round_trip_edge(
                    value,
                    source_ref,
                    target_node,
                    target_port,
                    adapter_format,
                    self._adapter_override_for(source_ref),
                )
            )
        if record.get("kind") != "artifact":
            label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
            raise m_resume.ResumeValidationError(
                "{}:{} cannot be frozen from output kind `{}`; recalculate with from_='{}'".format(
                    label, source_ref.port.name, record.get("kind"), label
                )
            )

        edge_key = (source_ref.node, source_ref.port.name, target_node, target_port.name)
        parent_edge = m_resume.manifest_edge_record(
            self._resume_plan.parent_manifest,
            source_node=source_ref.node,
            source_port=source_ref.port.name,
            target_node=target_node,
            target_port=target_port.name,
        )
        selected_record: Mapping[str, Any] = record
        parent_save_record: dict[str, Any]
        variant_id: str | None = None
        used_legacy_fallback = False
        node_variants = m_resume.manifest_output_variants(record)
        has_current_variant_edge = parent_edge is not None and "artifact_variant" in parent_edge
        if node_variants or has_current_variant_edge:
            selected = m_resume.manifest_exact_variant_for_edge(
                self._resume_plan.parent_manifest,
                pipeline=self._pipeline,
                historical_adapter_overrides=self._resume_plan.historical_adapter_overrides,
                parent_run_dir=self._resume_plan.parent_run_dir,
                source_node=source_ref.node,
                source_port=source_ref.port.name,
                target_node=target_node,
                target_port=target_port.name,
                output_record=record,
            )
            selected_record = cast(Mapping[str, Any], selected["artifact"])
            parent_save_record = dict(cast(Mapping[str, Any], selected["save"]))
            variant_id = str(selected["variant_id"])
        else:
            used_legacy_fallback = True
            legacy = self._resume_plan.legacy_artifacts.get((str(source_ref.node.uuid), source_ref.port.name))
            if legacy is None:
                source_label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
                raise m_resume.ResumeValidationError(
                    "marker-free frozen artifact was not classified as an unambiguous legacy artifact/save "
                    "pair; recalculate with from_='{}'".format(source_label)
                )
            selected_record = legacy.artifact
            parent_save_record = dict(legacy.save)
        validation_errors = m_resume.validate_artifact_record(
            self._node_alias(source_ref.node) or self._node_id(source_ref.node),
            source_ref.port.name,
            selected_record,
            self._resume_plan.parent_run_dir,
            allow_absolute=used_legacy_fallback,
        )
        if validation_errors:
            raise m_resume.ResumeValidationError(validation_errors[0])
        ref = m_resume.artifact_ref_from_record(
            selected_record,
            self._resume_plan.parent_run_dir,
            allow_absolute=used_legacy_fallback,
        )
        parent_save_identity = parent_save_record.get("identity")
        parent_save_key = parent_save_identity.get("key") if isinstance(parent_save_identity, Mapping) else None
        if parent_save_key != ref.key:
            source_label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
            raise m_resume.ResumeValidationError(
                "cannot restore frozen artifact `{}` because its ref key `{}` does not match the parent save "
                "adapter provenance key `{}`; recalculate with from_='{}'".format(
                    ref.uri,
                    ref.key,
                    parent_save_key or "<missing>",
                    source_label,
                )
            )
        parent_save_tag = parent_save_identity.get("tag") if isinstance(parent_save_identity, Mapping) else None
        if parent_save_tag != ref.tag:
            source_label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
            raise m_resume.ResumeValidationError(
                "cannot restore frozen artifact because its tag does not match the retained save provenance; "
                "recalculate with from_='{}'".format(source_label)
            )
        computed_variant_id = (
            hashlib.sha256(canonical_json_bytes(parent_save_identity)).hexdigest()
            if isinstance(parent_save_identity, Mapping)
            else None
        )
        if variant_id is not None and variant_id != computed_variant_id:
            source_label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
            raise m_resume.ResumeValidationError(
                "cannot restore frozen artifact because its variant identity does not match save provenance; "
                "recalculate with from_='{}'".format(source_label)
            )
        if variant_id is None:
            variant_id = computed_variant_id
        if used_legacy_fallback and adapter_format is not None and parent_save_key.rpartition("@")[2] != adapter_format:
            source_label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
            raise m_resume.ResumeValidationError(
                "the old retained manifest has no exact `{}` artifact variant for `{}.{}`; "
                "recalculate with from_='{}'".format(
                    adapter_format,
                    source_label,
                    source_ref.port.name,
                    source_label,
                )
            )
        if self._run_dir is not None:
            ref = m_resume.copy_artifact_ref_for_run(ref, self._run_dir)
        run_override = self._adapter_override_for(source_ref)
        target_type = _runtime_input_type_hint(target_node, target_port)
        load: LoadAdapterResolution | None
        if run_override is not None:
            load = LoadAdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE)
        elif isinstance(target_type, type):
            load = self._pipeline.resolve_load_adapter_binding(
                py_type=target_type,
                format=adapter_format,
            )
        elif isinstance(target_type, str):
            load = self._pipeline.resolve_load_adapter_binding_by_type_name(
                type_name=target_type,
                format=adapter_format,
            )
        elif ref.key == BUILTIN_JSON_ADAPTER.key:
            source = (
                AdapterResolutionSource.EDGE if adapter_format is not None else AdapterResolutionSource.PORT_DEFAULT
            )
            load = LoadAdapterResolution(BUILTIN_JSON_ADAPTER, source)
        elif ref.key in self._pipeline.adapters:
            source = AdapterResolutionSource.EDGE if adapter_format is not None else AdapterResolutionSource.PIPELINE
            load = LoadAdapterResolution(self._pipeline.adapters[ref.key], source)
        elif adapter_format is not None:
            load = self._pipeline.resolve_load_adapter_binding_by_format(format=adapter_format)
        else:
            load_adapter = self._pipeline.resolve_load_adapter(key=ref.key)
            load = (
                None if load_adapter is None else LoadAdapterResolution(load_adapter, AdapterResolutionSource.PIPELINE)
            )
        if load is None:
            raise m_resume.ResumeValidationError(
                "cannot restore frozen artifact `{}` because no load adapter is registered for {} and format `{}`".format(
                    ref.uri, target_type, adapter_format or "<default>"
                )
            )
        if cast(str, ref.tag) not in load.adapter.accepted_tags or (
            load.adapter.legacy_key_guard and ref.key != load.adapter.key
        ):
            source_label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
            raise m_resume.ResumeValidationError(
                "the retained artifact variant is incompatible with the exact load adapter for this edge; "
                "recalculate with from_='{}'".format(source_label)
            )
        self._frozen_save_adapter_records[edge_key] = parent_save_record
        self._load_adapter_resolutions[edge_key] = load
        if variant_id is not None:
            self._edge_variant_ids[edge_key] = variant_id
            self._artifact_refs[(source_ref.node, source_ref.port.name, variant_id)] = ref
        if isolated:
            return m_node_runtime.ArtifactInput(
                ref=ref,
                load_adapter=load.adapter,
                resolution_source=str(load.source),
            )
        return m_node_runtime.InlineInput(decode(ref, load.adapter))

    def _value_from_frozen_record(
        self, record: Mapping[str, Any], source_ref: NodeOutputRef, adapter_format: str | None
    ) -> Any:
        kind = record.get("kind")
        if kind == "json":
            return record.get("value")
        if kind != "artifact":
            label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
            raise m_resume.ResumeValidationError(
                "{}:{} cannot be frozen from output kind `{}`; recalculate with from_='{}'".format(
                    label, source_ref.port.name, kind, label
                )
            )
        if self._resume_plan is None:
            raise RuntimeError("artifact restore requested outside resume")
        ref = m_resume.artifact_ref_from_record(record, self._resume_plan.parent_run_dir)
        resolution = self._resolution_for_frozen_artifact(ref, source_ref, adapter_format)
        return decode(ref, resolution.adapter)

    def _resolution_for_frozen_artifact(
        self, ref: ArtifactRef, source_ref: NodeOutputRef, adapter_format: str | None
    ) -> LoadAdapterResolution:
        run_override = self._adapter_override_for(source_ref)
        if run_override is not None:
            return LoadAdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE)
        if ref.key == BUILTIN_JSON_ADAPTER.key:
            return LoadAdapterResolution(BUILTIN_JSON_ADAPTER, AdapterResolutionSource.PORT_DEFAULT)
        adapter = self._pipeline.resolve_load_adapter(key=ref.key)
        if adapter is None:
            label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
            raise m_resume.ResumeValidationError(
                "{}:{} cannot restore artifact `{}` because adapter `{}` is not registered; "
                "recalculate with from_='{}'".format(label, source_ref.port.name, ref.uri, ref.key, label)
            )
        source = AdapterResolutionSource.EDGE if adapter_format is not None else AdapterResolutionSource.PIPELINE
        return LoadAdapterResolution(adapter, source)

    def _get_input(
        self,
        x: Any,
        target_node: Node,
        target_port: InputPort,
        target_runtime: _ResolvedNodeRuntimePlan,
    ) -> m_node_runtime.NodeInputTransport:
        match x:
            case Scalar():
                return m_node_runtime.InlineInput(self._round_trip_artifact(x.value))

            case NodeOutputRef():
                if self._is_frozen_node(x.node):
                    return self._get_frozen_edge_input(
                        x,
                        None,
                        target_node,
                        target_port,
                        isolated=target_runtime.isolated,
                    )
                run_override = self._adapter_override_for(x)
                value = (
                    self._get_result(
                        x.node,
                        output_request=(x, None, run_override, target_node, target_port),
                    )
                )[x.port.name]
                if target_runtime.isolated:
                    return self._transport_edge_input(value, x, target_node, target_port, None, run_override)
                return m_node_runtime.InlineInput(
                    self._round_trip_edge(value, x, target_node, target_port, None, run_override)
                )

            case FormattedOutputRef():
                if self._is_frozen_node(x.out_ref.node):
                    return self._get_frozen_edge_input(
                        x.out_ref,
                        x.format,
                        target_node,
                        target_port,
                        isolated=target_runtime.isolated,
                    )
                run_override = self._adapter_override_for(x.out_ref)
                value = (
                    self._get_result(
                        x.out_ref.node,
                        output_request=(x.out_ref, x.format, run_override, target_node, target_port),
                    )
                )[x.out_ref.port.name]
                if target_runtime.isolated:
                    return self._transport_edge_input(
                        value,
                        x.out_ref,
                        target_node,
                        target_port,
                        x.format,
                        run_override,
                    )
                return m_node_runtime.InlineInput(
                    self._round_trip_edge(
                        value,
                        x.out_ref,
                        target_node,
                        target_port,
                        x.format,
                        run_override,
                    )
                )

            case _:
                raise ValueError(x)

    def _get_result(
        self,
        node: Node,
        *,
        output_request: _OutputMaterializationRequest | None = None,
    ) -> dict[str, Any]:
        if node in self._results:
            if output_request is not None:
                try:
                    self._materialize_requested_output_artifact(node, self._results[node], output_request)
                except _SourceOutputCommitError as exc:
                    self._finalize_lazy_source_output_failure(output_request[0], exc)
                    raise
            return self._results[node]
        if node in self._visiting_node_set:
            cycle_start = self._visiting_nodes.index(node)
            cycle = [*self._visiting_nodes[cycle_start:], node]
            labels = [self._node_alias(item) or self._node_id(item) for item in cycle]
            raise RuntimeError("splime pipeline execution cycle detected: {}".format(" → ".join(labels)))

        self._visiting_nodes.append(node)
        self._visiting_node_set.add(node)
        try:
            return self._get_uncached_result(node, output_request=output_request)
        finally:
            completed = self._visiting_nodes.pop()
            self._visiting_node_set.remove(completed)

    def _get_uncached_result(
        self,
        node: Node,
        *,
        output_request: _OutputMaterializationRequest | None,
    ) -> dict[str, Any]:
        self._ensure_open()
        self._ensure_manifest_writer()
        if self._is_frozen_node(node):
            self._results[node] = self._restore_frozen_result(node)
            return self._results[node]
        runtime_plan = self._resolve_node_runtime_plan(node)
        kwargs: dict[InputPort, m_node_runtime.NodeInputTransport] = {}
        input_records: dict[str, Any] = {}
        input_value_ref: Any = None
        try:
            for port in node.inputs:
                input_value_ref = None
                if port.name not in self._kwargs:
                    continue
                value = self._round_trip_artifact(self._kwargs[port.name])
                kwargs[port] = m_node_runtime.InlineInput(value)
                if self._manifest_writer is not None:
                    input_records[port.name] = self._value_record(value)

            if node in self._deps:
                for port, value_ref in self._deps[node].items():
                    input_value_ref = value_ref
                    value = self._get_input(value_ref, node, port, runtime_plan)
                    kwargs[port] = value
                    if self._manifest_writer is not None:
                        input_records[port.name] = self._record_link_input(node, port, value_ref, value)
        except BaseException as exc:
            error = self._upstream_failure_error(input_value_ref, exc)
            status = "upstream-failed" if error is not None else "failed"
            try:
                self._write_node_manifest(
                    node,
                    status=status,
                    inputs=input_records,
                    outputs={},
                    error=error or repr(exc),
                    compute_fingerprint=False,
                )
                self._finish_manifest(status="failed", error=error or repr(exc))
            except BaseException as finalization_exc:
                raise finalization_exc from exc
            raise

        try:
            self._node_inputs[node] = input_records
            result = self._execute_node_with_runtime(
                node,
                kwargs,
                input_records,
                runtime_plan,
                output_request=output_request,
            )
            output_records = self._output_records(node, result, output_request=output_request)
            self._write_node_manifest(
                node,
                status="succeeded",
                inputs=input_records,
                outputs=output_records,
            )
        except BaseException as exc:
            error = repr(exc)
            try:
                self._write_node_manifest(
                    node,
                    status="failed",
                    inputs=input_records,
                    outputs={},
                    error=error,
                    compute_fingerprint=False,
                )
                self._finish_manifest(status="failed", error=error)
            except BaseException as finalization_exc:
                raise finalization_exc from exc
            raise

        self._results[node] = result
        return self._results[node]

    def _upstream_failure_error(self, value_ref: Any, exc: BaseException) -> str | None:
        source_ref = self._source_ref(value_ref)
        if source_ref is None:
            return None
        source_status = self._node_status(source_ref.node)
        if source_status not in {"failed", "upstream-failed"}:
            return None
        label = self._node_alias(source_ref.node) or self._node_name(source_ref.node)
        return "upstream node `{}` failed: {}".format(label, repr(exc))

    def _execute_node_with_runtime(
        self,
        node: Node,
        kwargs: dict[InputPort, m_node_runtime.NodeInputTransport],
        input_records: Mapping[str, Any],
        runtime_plan: _ResolvedNodeRuntimePlan,
        *,
        output_request: _OutputMaterializationRequest | None,
    ) -> dict[str, Any]:
        if runtime_plan.fast_native:
            return self._callback(
                node,
                {port: self._inline_input_value(value) for port, value in kwargs.items()},
            )
        resolution = runtime_plan.resolution
        backend = runtime_plan.backend
        output_port = self._single_output_port(node)
        output_plan = self._isolated_output_plan(node, output_port, output_request) if runtime_plan.isolated else None
        context = m_node_runtime.NodeRuntimeContext(
            node=node,
            node_label=self._node_alias(node) or self._node_name(node),
            inputs=kwargs,
            output_port=output_port,
            callback=self._callback,
            work_dir=self._node_runtime_work_dir(node, resolution.name),
            environment_provider=self._node_environment_provider,
            runtime_config=self._runtime_config,
            environment_spec=self._runtime_env_spec,
            output_plan=output_plan,
        )
        if runtime_plan.isolated:
            context = m_node_runtime.prepare_isolated_node_inputs(context, runtime_name=resolution.name)
        environment = backend.prepare(context)
        self._node_runtimes[node] = m_node_runtime.runtime_manifest_record(resolution, environment)
        self._write_node_manifest(node, status="running", inputs=input_records)
        return backend.execute(context, environment)

    def _resolve_node_runtime_plan(self, node: Node) -> _ResolvedNodeRuntimePlan:
        cached = self._node_runtime_plans.get(node)
        if cached is not None:
            return cached
        fast_native = self._can_use_native_fast_path(node)
        if fast_native:
            resolution = m_node_runtime.NodeRuntimeResolution(
                m_node_runtime.NATIVE_NODE_RUNTIME,
                m_node_runtime.NodeRuntimeResolutionSource.DEFAULT,
            )
        else:
            resolution = m_node_runtime.resolve_node_runtime(
                self._pipeline,
                node,
                runtime_config=self._runtime_config,
                run_override=self._runtime_overrides.get(node),
            )
        plan = _ResolvedNodeRuntimePlan(
            resolution=resolution,
            backend=self._node_runtime_registry.backend_for(resolution.name),
            fast_native=fast_native,
        )
        self._node_runtime_plans[node] = plan
        return plan

    @staticmethod
    def _inline_input_value(value: m_node_runtime.NodeInputTransport) -> Any:
        if isinstance(value, m_node_runtime.InlineInput):
            return value.value
        raise RuntimeError("native node received an isolated artifact input")

    def _can_use_native_fast_path(self, node: Node) -> bool:
        del node
        return not self._has_runtime_selection and (self._manifest_writer is None or self._manifest_writer.path is None)

    def _node_runtime_work_dir(self, node: Node, runtime_name: str) -> Path:
        if runtime_name == m_node_runtime.NATIVE_NODE_RUNTIME:
            return Path(tempfile.gettempdir())
        if self._keep is False:
            base_dir = self._get_artifacts_dir() / "node-runtimes"
        else:
            base_dir = self._ensure_run_dir() / "node-runtimes"
        return base_dir / str(node.uuid)

    def _single_output_port(self, node: Node) -> OutputPort:
        outputs = node.outputs or []
        if len(outputs) != 1:
            raise RuntimeError(
                "node {} has {} outputs; per-node runtime execution currently supports exactly one output".format(
                    node, len(outputs)
                )
            )
        return outputs[0]

    def __getitem__(self, node: Node) -> dict[str, Any]:
        try:
            result = self._get_result(node)
            if not any(isinstance(value, m_node_runtime.ArtifactOutput) for value in result.values()):
                return result
            return {port_name: self._public_output_value(node, port_name, value) for port_name, value in result.items()}
        except BaseException as exc:
            try:
                self.close()
            except BaseException as close_exc:
                raise close_exc from exc
            raise

    def _public_output_value(self, node: Node, port_name: str, value: Any) -> Any:
        if not isinstance(value, m_node_runtime.ArtifactOutput):
            return value
        cache_key = (node, port_name)
        if cache_key in self._public_output_values:
            return self._public_output_values[cache_key]
        load_adapter = self._pipeline.resolve_load_adapter(key=value.ref.key)
        if load_adapter is None and all(
            hasattr(value.save_adapter, attribute) for attribute in ("load", "accepted_tags", "legacy_key_guard")
        ):
            load_adapter = cast(LoadAdapter, value.save_adapter)
        if load_adapter is None:
            raise ValueError(
                "terminal isolated artifact output `{}` cannot be converted back to a Python value because "
                "no matching load adapter is registered".format(value.ref.key)
            )
        loaded = decode(value.ref, load_adapter)
        self._public_output_values[cache_key] = loaded
        return loaded

    def value(self, alias: str | None = None, port: str = DEFAULT_PORT) -> Any:
        """Return one output value directly, without ``[node][port]`` indexing."""

        return self[self._resolve_alias_node(alias)][port]

    def _resolve_alias_node(self, alias: str | None) -> Node:
        if alias is not None:
            return self._pipeline.aliases[alias]
        if len(self._pipeline.nodes) == 1:
            return next(iter(self._pipeline.nodes))
        raise ValueError("Run.value() requires alias=... for multi-node pipelines")

    def _record_link_input(
        self, target_node: Node, target_port: InputPort, value_ref: Any, value: Any
    ) -> dict[str, Any]:
        source_ref = self._source_ref(value_ref)
        if source_ref is None:
            return self._value_record(value)

        edge_key = (source_ref.node, source_ref.port.name, target_node, target_port.name)
        record = self._edge_value_record(source_ref, target_node, target_port, value)
        save_resolution = self._edge_save_adapter_resolutions.get(edge_key)
        load_resolution = self._load_adapter_resolutions.get(edge_key)
        save_record = self._frozen_save_adapter_records.get(edge_key)
        load_record = None
        if save_record is None and save_resolution is not None:
            save_record = m_manifest.adapter_record(
                save_adapter_identity(save_resolution.adapter), str(save_resolution.source)
            )
        if save_record is not None and not self._is_frozen_node(source_ref.node):
            if self._artifact_ref_for_output(source_ref.node, source_ref.port.name) is None:
                self._set_node_adapter(source_ref.node, source_ref.port.name, save_record)
            else:
                output_record, node_adapter_record = self._artifact_output_manifest_records(
                    source_ref.node,
                    source_ref.port.name,
                )
                self._set_node_output(source_ref.node, source_ref.port.name, output_record)
                self._set_node_adapter(source_ref.node, source_ref.port.name, node_adapter_record)
        if load_resolution is not None:
            load_record = m_manifest.adapter_record(
                load_adapter_identity(load_resolution.adapter), str(load_resolution.source)
            )
            self._set_node_adapter(target_node, target_port.name, load_record)

        writer = self._manifest_writer
        if writer is not None:
            writer.add_edge(
                m_manifest.edge_record(
                    source_node_id=self._node_id(source_ref.node),
                    source_port=source_ref.port.name,
                    target_node_id=self._node_id(target_node),
                    target_port=target_port.name,
                    artifact=record,
                    adapter=(
                        None
                        if save_record is None or load_record is None
                        else m_manifest.edge_adapter_record(save_record, load_record)
                    ),
                    variant_id=self._edge_variant_ids.get(edge_key),
                )
            )
            self._write_node_manifest(source_ref.node, status=self._node_status(source_ref.node))
        return record

    def _source_ref(self, value_ref: Any) -> NodeOutputRef | None:
        if isinstance(value_ref, FormattedOutputRef):
            return value_ref.out_ref
        if isinstance(value_ref, NodeOutputRef):
            return value_ref
        return None

    def _edge_value_record(
        self,
        source_ref: NodeOutputRef,
        target_node: Node,
        target_port: InputPort,
        value: Any,
    ) -> dict[str, Any]:
        if isinstance(value, m_node_runtime.ArtifactInput):
            record = m_manifest.artifact_record(value.ref, run_dir=self._run_dir)
            if not self._is_frozen_node(source_ref.node):
                self._set_node_output(source_ref.node, source_ref.port.name, record)
            return record
        edge_key = (source_ref.node, source_ref.port.name, target_node, target_port.name)
        variant_id = self._edge_variant_ids.get(edge_key)
        ref = (
            self._artifact_refs.get((source_ref.node, source_ref.port.name, variant_id))
            if variant_id is not None
            else self._artifact_ref_for_output(source_ref.node, source_ref.port.name)
        )
        if ref is not None:
            record = m_manifest.artifact_record(ref, run_dir=self._run_dir)
            if not self._is_frozen_node(source_ref.node):
                self._set_node_output(source_ref.node, source_ref.port.name, record)
            return record
        if self._is_frozen_node(source_ref.node) and self._resume_plan is not None:
            parent_record = m_resume.manifest_output_record(
                self._resume_plan.parent_manifest, source_ref.node, source_ref.port.name
            )
            return m_resume.rebase_output_record(parent_record, self._resume_plan.parent_run_dir, self._run_dir)
        return self._value_record(value)

    def _value_record(self, value: Any) -> dict[str, Any]:
        if isinstance(value, m_node_runtime.ArtifactOutput):
            return m_manifest.artifact_record(value.ref, run_dir=self._run_dir)
        if isinstance(value, m_node_runtime.ArtifactInput):
            return m_manifest.artifact_record(value.ref, run_dir=self._run_dir)
        if isinstance(value, m_node_runtime.InlineInput):
            value = value.value
        if self._native_values and type(value) not in {type(None), bool, int, float, str}:
            # JSON cannot reconstruct identity, aliasing, cycles or native
            # types. Do not serialize a native container as resume evidence.
            return m_manifest.unfreezable_record("native value was not materialized as an artifact")
        if type(value) in _JSON_NATIVE_TYPES:
            try:
                return m_manifest.json_record(value)
            except ValueError:
                if not self._native_values:
                    raise
        return m_manifest.unfreezable_record("value was not materialized as an artifact")

    def _set_node_adapter(self, node: Node, port_name: str, record: dict[str, Any]) -> None:
        self._node_adapters.setdefault(node, {})[port_name] = record
        writer = self._manifest_writer
        if writer is not None and self._node_id(node) in writer.data["nodes"]:
            writer.set_node_adapter(self._node_id(node), port_name, record)
            self._write_node_manifest(node, status=self._node_status(node))

    def _set_node_output(self, node: Node, port_name: str, record: dict[str, Any]) -> None:
        writer = self._manifest_writer
        if writer is not None and self._node_id(node) in writer.data["nodes"]:
            writer.set_node_output(self._node_id(node), port_name, record)

    def _output_records(
        self,
        node: Node,
        result: dict[str, Any],
        *,
        output_request: _OutputMaterializationRequest | None = None,
    ) -> dict[str, Any]:
        expected_ports = {port.name for port in node.outputs}
        actual_ports = set(result)
        if actual_ports != expected_ports:
            missing = sorted(expected_ports - actual_ports)
            unexpected = sorted(actual_ports - expected_ports)
            details = []
            if missing:
                details.append("missing port(s): {}".format(", ".join(missing)))
            if unexpected:
                details.append("unexpected port(s): {}".format(", ".join(unexpected)))
            label = self._node_alias(node) or self._node_name(node)
            raise ValueError(
                "invalid output mapping from node `{}`: {}; return exactly the declared output ports".format(
                    label, "; ".join(details)
                )
            )
        for port_name, value in result.items():
            if isinstance(value, m_node_runtime.ArtifactOutput):
                self._register_promoted_output(node, port_name, value)
        if output_request is not None:
            self._materialize_requested_output_artifact(node, result, output_request)
        outputs = {}
        for port_name, value in result.items():
            ref = self._artifact_ref_for_output(node, port_name)
            if ref is not None:
                outputs[port_name], adapter_record = self._artifact_output_manifest_records(node, port_name)
                self._node_adapters.setdefault(node, {})[port_name] = adapter_record
                continue
            if not self._native_values and type(value) in _JSON_NATIVE_TYPES:
                try:
                    validate_json_value(value)
                except ValueError as exc:
                    if self._native_values:
                        outputs[port_name] = self._value_record(value)
                        continue
                    label = self._node_alias(node) or self._node_name(node)
                    raise ValueError(
                        "invalid JSON output from node `{}` port `{}`: {}. Return a valid JSON value "
                        "or materialize this output with an adapter.".format(label, port_name, exc)
                    ) from exc
            outputs[port_name] = self._value_record(value)
        return outputs

    def _materialize_requested_output_artifact(
        self,
        node: Node,
        result: Mapping[str, Any],
        request: _OutputMaterializationRequest,
    ) -> None:
        source_ref, adapter_format, run_override, target_node, target_port = request
        if source_ref.node != node:
            raise RuntimeError("output materialization request does not belong to the producing node")
        value = result[source_ref.port.name]
        if isinstance(value, m_node_runtime.ArtifactOutput):
            self._resolve_promoted_output_edge(
                value,
                source_ref,
                target_node,
                target_port,
                adapter_format,
                run_override,
            )
            return
        save, load = self._resolve_edge_adapter_bindings(
            value,
            source_ref,
            target_node,
            target_port,
            adapter_format,
            run_override,
        )
        if save is None or load is None:
            return
        if (
            save.adapter is BUILTIN_JSON_ADAPTER
            and load.adapter is BUILTIN_JSON_ADAPTER
            and type(value) in _JSON_NATIVE_TYPES
        ):
            if not self._native_values or adapter_format is not None or run_override is not None:
                validate_json_value(value)
            return
        self._ensure_open()
        self._materialize_source_output(value, source_ref, save)

    def _register_promoted_output(
        self,
        node: Node,
        port_name: str,
        value: m_node_runtime.ArtifactOutput,
    ) -> None:
        primary_resolution: SaveAdapterResolution | None = None
        source_ref = NodeOutputRef(node, node.get_output_port(port_name))
        for variant in value.variants():
            cache_key = (node, port_name, variant.variant_id)
            existing = self._artifact_refs.get(cache_key)
            if existing is not None and existing != variant.ref:
                raise RuntimeError("isolated output artifact identity changed after promotion")
            self._artifact_refs[cache_key] = variant.ref
            cached_resolution = self._variant_save_resolutions.get(cache_key)
            if isinstance(cached_resolution, SaveAdapterResolution):
                resolution = cached_resolution
            else:
                try:
                    source = AdapterResolutionSource(variant.resolution_source)
                except ValueError:
                    source = AdapterResolutionSource.PIPELINE
                resolution = SaveAdapterResolution(variant.save_adapter, source)
                self._variant_save_resolutions[cache_key] = resolution
            if primary_resolution is None:
                primary_resolution = resolution
            self._record_materialized_output(source_ref, variant.ref, resolution)
        if primary_resolution is not None:
            self._adapter_resolutions[(node, port_name)] = primary_resolution

    def _artifact_ref_for_output(self, node: Node, port_name: str) -> ArtifactRef | None:
        refs = [
            (variant_id, ref)
            for ref_node, ref_port, variant_id in self._artifact_refs
            if ref_node == node and ref_port == port_name
            for ref in (self._artifact_refs[(ref_node, ref_port, variant_id)],)
        ]
        if not refs:
            return None
        return sorted(refs, key=lambda item: item[0])[0][1]

    def _write_node_manifest(
        self,
        node: Node,
        *,
        status: str,
        inputs: Mapping[str, Any] | None = None,
        outputs: Mapping[str, Any] | None = None,
        runtime: Mapping[str, Any] | None = None,
        error: str | None = None,
        compute_fingerprint: bool = True,
    ) -> None:
        writer = self._manifest_writer
        if writer is None:
            return
        node_id = self._node_id(node)
        existing = writer.data["nodes"].get(node_id, {})
        merged_inputs = dict(inputs if inputs is not None else existing.get("inputs", {}))
        merged_outputs = dict(outputs if outputs is not None else existing.get("outputs", {}))
        adapters = dict(self._node_adapters.get(node, existing.get("adapters", {})))
        runtime_record = dict(
            runtime if runtime is not None else self._node_runtimes.get(node, existing.get("runtime", {}))
        )
        fingerprint_sha256: str | None
        if compute_fingerprint:
            fingerprint_sha256 = self._node_fingerprint(node, merged_inputs, adapters)
        else:
            existing_fingerprint = existing.get("fingerprint")
            existing_sha256 = existing_fingerprint.get("sha256") if isinstance(existing_fingerprint, Mapping) else None
            fingerprint_sha256 = existing_sha256 if isinstance(existing_sha256, str) else None
        writer.set_node(
            m_manifest.node_record(
                node_id=node_id,
                alias=self._node_alias(node),
                kind=self._node_kind(node),
                name=self._node_name(node),
                status=status,
                fingerprint_sha256=fingerprint_sha256,
                inputs=merged_inputs,
                outputs=merged_outputs,
                adapters=adapters,
                runtime=runtime_record,
                error=error,
            )
        )

    def _node_fingerprint(self, node: Node, inputs: Mapping[str, Any], adapters: Mapping[str, Any]) -> str:
        artifact_inputs = {
            port: str(record["sha256"]) for port, record in inputs.items() if record.get("kind") == "artifact"
        }
        inline_inputs = {port: record["value"] for port, record in inputs.items() if record.get("kind") == "json"}
        adapter_identities = {
            port: dict(record)
            for port, record in adapters.items()
            if isinstance(record, Mapping) and isinstance(record.get("identity"), Mapping)
        }
        execution_context_sha256 = self._runtime_adapter_fingerprint_sha256
        node_override_evidence = [
            {"port": port, "identity": dict(identity)}
            for (node_id, port), identity in sorted(self._save_override_evidence.items())
            if node_id == str(node.uuid)
        ]
        if node_override_evidence:
            execution_context_sha256 = hashlib.sha256(
                canonical_json_bytes(
                    {
                        "runtime_adapter_fingerprint_sha256": execution_context_sha256,
                        "historical_save_adapter_overrides": node_override_evidence,
                    }
                )
            ).hexdigest()
        return node_fingerprint(
            node_content=self._node_content(node),
            node_version=self._node_version(node),
            input_ports=[port.name for port in node.inputs],
            output_ports=[port.name for port in node.outputs],
            adapter_identities=adapter_identities,
            artifact_inputs=artifact_inputs,
            inline_inputs=inline_inputs,
            execution_context_sha256=execution_context_sha256,
        )

    def _node_content(self, node: Node) -> bytes:
        if isinstance(node, NodeFunction):
            try:
                source = inspect.getsource(node.func)
            except OSError:
                source = None
            payload = {
                "kind": "function",
                "module": node.func.__module__,
                "qualname": node.func.__qualname__,
                "source": source,
                "inputs": [self._port_payload(port) for port in node.inputs],
                "outputs": [self._port_payload(port) for port in node.outputs],
            }
            return canonical_json_bytes(payload)
        if isinstance(node, NodeRemote):
            payload = {
                "kind": "remote",
                "url": node.url,
                "name": node.name,
                "version": node.version,
                "owner_id": node.owner_id,
                "library": node.library,
                "target_machine": node.target_machine,
                "inputs": [self._port_payload(port) for port in node.inputs],
                "outputs": [self._port_payload(port) for port in node.outputs],
            }
            return canonical_json_bytes(payload)
        return canonical_json_bytes({"kind": type(node).__name__, "uuid": str(node.uuid)})

    def _node_version(self, node: Node) -> str | None:
        if isinstance(node, NodeRemote):
            return node.version
        return None

    def _port_payload(self, port: InputPort | OutputPort) -> dict[str, Any]:
        payload = {"name": port.name, "type": port.typ_}
        if isinstance(port, InputPort):
            payload["default"] = port.default
        return payload

    def _node_id(self, node: Node) -> str:
        return str(node.uuid)

    def _node_alias(self, node: Node) -> str | None:
        aliases = sorted(alias for alias, alias_node in self._pipeline.aliases.items() if alias_node == node)
        return aliases[0] if aliases else None

    def _node_kind(self, node: Node) -> str:
        if isinstance(node, NodeFunction):
            return "function"
        if isinstance(node, NodeRemote):
            return "remote"
        return type(node).__name__

    def _node_name(self, node: Node) -> str:
        if isinstance(node, NodeFunction):
            return node.func.__name__
        if isinstance(node, NodeRemote):
            return node.name
        return str(node)

    def _node_status(self, node: Node) -> str:
        writer = self._manifest_writer
        if writer is None:
            return "pending"
        record = writer.data["nodes"].get(self._node_id(node))
        if isinstance(record, Mapping) and isinstance(record.get("status"), str):
            return cast(str, record["status"])
        return "pending"
