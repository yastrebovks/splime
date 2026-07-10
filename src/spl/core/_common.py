import inspect
import json
import os
import shutil
import tempfile
import warnings
import weakref
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from functools import reduce
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from types import FunctionType
from typing import Any, cast, overload

from spl.core import manifest as m_manifest
from spl.core import node_runtime as m_node_runtime
from spl.core import resume as m_resume
from spl.core.entities.adapter import (
    BUILTIN_JSON_ADAPTER,
    JSON_NATIVE_TYPES,
    LoadAdapter,
    RuntimeAdapter,
    SaveAdapter,
    adapter_identity,
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
from spl.core.entities.pipeline import AdapterResolution, AdapterResolutionSource, Pipeline
from spl.core.entities.scalar import Scalar
from spl.core.fingerprint import canonical_json_bytes, node_fingerprint

_JSON_NATIVE_TYPES = JSON_NATIVE_TYPES
RunAdapterOverrideKey = tuple[str, str]
RunAdapterOverrides = Mapping[RunAdapterOverrideKey, RuntimeAdapter]
_NormalizedRunAdapterOverrides = dict[tuple[Node, str], RuntimeAdapter]
RunRuntimeOverrides = m_node_runtime.RunRuntimeOverrides


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
        parent_run_dir, parent_manifest = m_resume.load_retained_manifest(run_id)
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
        **kwargs: Any,
    ) -> None:
        self._callback = callback
        self._pipeline = pipeline
        self._kwargs = kwargs
        self._keep = m_manifest.normalize_keep(keep)
        self._run_id = run_id or m_manifest.new_run_id()
        self._parent_run_id = parent_run_id
        self._resume_plan = resume_plan
        if resume_plan is not None and parent_run_id is None:
            self._parent_run_id = str(resume_plan.parent_manifest["run_id"])
        reserved_names = RESERVED_RESUME_KWARGS if resume_plan is not None else RESERVED_RUN_KWARGS
        _warn_reserved_run_input_names(pipeline, reserved_names)
        self._adapter_overrides = _validate_run_adapter_overrides(pipeline, adapters)
        self._runtime_overrides = m_node_runtime.validate_run_runtime_overrides(pipeline, runtimes)
        self._runtime_config = m_node_runtime.validate_node_runtime_config(runtime_config)
        self._node_environment_provider = node_environment_provider or m_node_runtime.CurrentPythonEnvironmentProvider()
        self._runtime_env_spec = list(runtime_env_spec or [])
        self._node_runtime_registry = m_node_runtime.NodeRuntimeRegistry()
        self._has_runtime_selection = bool(
            self._runtime_overrides
            or self._runtime_config.get("node_runtime") is not None
            or any(m_node_runtime.RUNTIME_TAG_NAME in node_tags for node_tags in pipeline.tags.values())
        )
        self._deps: dict[Node, dict[Any, Any]] = {
            k: dict(map(itemgetter(slice(1, None)), vs))
            for k, vs in groupby(
                sorted([(x.node, x.port, y) for (x, y) in pipeline.links], key=lambda x: hash(x[0])), itemgetter(0)
            )
        }
        self._results: dict[Node, dict[str, Any]] = dict()
        self._artifact_refs: dict[tuple[Node, str, str], ArtifactRef] = dict()
        self._adapter_resolutions: dict[tuple[Node, str], AdapterResolution] = dict()
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
        self.close()

    def close(self) -> None:
        if not self._closed:
            if self._manifest_writer is not None and self._terminal_status is None:
                self._finish_manifest(status="succeeded")
            if self._artifacts_finalizer is not None:
                self._artifacts_finalizer()
            if self._run_dir_finalizer is not None and not self._should_retain_terminal():
                self._run_dir_finalizer()
            self._closed = True

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
            run_dir = m_manifest.create_run_dir(self._run_id)
            self._run_dir = run_dir
            if self._keep == "on_failure":
                self._run_dir_finalizer = weakref.finalize(self, shutil.rmtree, run_dir, ignore_errors=True)
        return self._run_dir

    def _ensure_manifest_writer(self) -> m_manifest.RunManifestWriter | None:
        if self._keep is False:
            return None
        if self._manifest_writer is None:
            if self._keep == "on_failure":
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
        self._terminal_status = status
        if self._should_retain_terminal():
            writer.materialize(self._ensure_run_dir())
        writer.finish(status=status, error=error)
        if self._should_retain_terminal() and self._run_dir_finalizer is not None:
            self._run_dir_finalizer.detach()

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
            return value

        self._ensure_open()
        if source_ref is None:
            ref = encode(value, adapter, self._get_artifacts_dir())
        else:
            cache_key = (source_ref.node, source_ref.port.name, adapter.key)
            if cache_key not in self._artifact_refs:
                self._artifact_refs[cache_key] = encode(value, adapter, self._get_artifacts_dir())
            ref = self._artifact_refs[cache_key]
            self._record_materialized_output(source_ref, ref, resolution)
        return decode(ref, adapter)

    def _adapter_override_for(self, source_ref: NodeOutputRef | None) -> RuntimeAdapter | None:
        if source_ref is None:
            return None
        return self._adapter_overrides.get((source_ref.node, source_ref.port.name))

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
        self, source_ref: NodeOutputRef, ref: ArtifactRef, resolution: AdapterResolution
    ) -> None:
        if self._manifest_writer is None:
            return
        output_record = m_manifest.artifact_record(ref, run_dir=self._run_dir)
        self._set_node_output(source_ref.node, source_ref.port.name, output_record)
        adapter_record = m_manifest.adapter_record(adapter_identity(resolution.adapter), str(resolution.source))
        self._set_node_adapter(source_ref.node, source_ref.port.name, adapter_record)
        self._write_node_manifest(source_ref.node, status=self._node_status(source_ref.node))

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

    def _get_frozen_edge_input(self, source_ref: NodeOutputRef, adapter_format: str | None) -> Any:
        self._ensure_frozen_node_manifest(source_ref.node)
        if self._resume_plan is None:
            raise RuntimeError("frozen edge input requested outside resume")
        record = m_resume.manifest_output_record(
            self._resume_plan.parent_manifest, source_ref.node, source_ref.port.name
        )
        return self._value_from_frozen_record(record, source_ref, adapter_format)

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
        self._adapter_resolutions[(source_ref.node, source_ref.port.name)] = resolution
        return decode(ref, resolution.adapter)

    def _resolution_for_frozen_artifact(
        self, ref: ArtifactRef, source_ref: NodeOutputRef, adapter_format: str | None
    ) -> AdapterResolution:
        run_override = self._adapter_override_for(source_ref)
        if run_override is not None:
            return AdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE)
        if ref.key == BUILTIN_JSON_ADAPTER.key:
            return AdapterResolution(BUILTIN_JSON_ADAPTER, AdapterResolutionSource.PORT_DEFAULT)
        adapter = self._pipeline.resolve_adapter(key=ref.key)
        if adapter is None:
            label = self._node_alias(source_ref.node) or self._node_id(source_ref.node)
            raise m_resume.ResumeValidationError(
                "{}:{} cannot restore artifact `{}` because adapter `{}` is not registered; "
                "recalculate with from_='{}'".format(label, source_ref.port.name, ref.uri, ref.key, label)
            )
        source = AdapterResolutionSource.EDGE if adapter_format is not None else AdapterResolutionSource.PIPELINE
        return AdapterResolution(adapter, source)

    def _get_input(self, x: Any) -> Any:
        match x:
            case Scalar():
                return self._round_trip_artifact(x.value)

            case NodeOutputRef():
                if self._is_frozen_node(x.node):
                    return self._get_frozen_edge_input(x, None)
                value = (self._get_result(x.node))[x.port.name]
                if (run_override := self._adapter_override_for(x)) is not None:
                    return self._round_trip_artifact_override(value, x, None, run_override)
                return self._round_trip_artifact(value, x)

            case FormattedOutputRef():
                if self._is_frozen_node(x.out_ref.node):
                    return self._get_frozen_edge_input(x.out_ref, x.format)
                value = (self._get_result(x.out_ref.node))[x.out_ref.port.name]
                if (run_override := self._adapter_override_for(x.out_ref)) is not None:
                    return self._round_trip_artifact_override(value, x.out_ref, x.format, run_override)
                return self._round_trip_artifact(value, source_ref=x.out_ref, adapter_format=x.format)

            case _:
                raise ValueError(x)

    def _get_result(self, node: Node) -> dict[str, Any]:
        if node not in self._results:
            self._ensure_open()
            self._ensure_manifest_writer()
            if self._is_frozen_node(node):
                self._results[node] = self._restore_frozen_result(node)
                return self._results[node]
            kwargs: dict[InputPort, Any] = {}
            input_records: dict[str, Any] = {}
            input_value_ref: Any = None
            try:
                for port in node.inputs:
                    input_value_ref = None
                    if port.name not in self._kwargs:
                        continue
                    value = self._round_trip_artifact(self._kwargs[port.name])
                    kwargs[port] = value
                    if self._manifest_writer is not None:
                        input_records[port.name] = self._value_record(value)

                if node in self._deps:
                    for port, value_ref in self._deps[node].items():
                        input_value_ref = value_ref
                        value = self._get_input(value_ref)
                        kwargs[port] = value
                        if self._manifest_writer is not None:
                            input_records[port.name] = self._record_link_input(node, port, value_ref, value)
            except BaseException as exc:
                error = self._upstream_failure_error(input_value_ref, exc)
                status = "upstream-failed" if error is not None else "failed"
                self._write_node_manifest(node, status=status, inputs=input_records, error=error or repr(exc))
                self._finish_manifest(status="failed", error=error or repr(exc))
                raise

            try:
                self._node_inputs[node] = input_records
                result = self._execute_node_with_runtime(node, kwargs, input_records)
            except BaseException as exc:
                error = repr(exc)
                self._write_node_manifest(node, status="failed", inputs=input_records, error=error)
                self._finish_manifest(status="failed", error=error)
                raise

            self._results[node] = result
            self._write_node_manifest(
                node,
                status="succeeded",
                inputs=input_records,
                outputs=self._output_records(node, result),
            )
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
        kwargs: dict[InputPort, Any],
        input_records: Mapping[str, Any],
    ) -> dict[str, Any]:
        if self._can_use_native_fast_path(node):
            return self._callback(node, kwargs)
        resolution = m_node_runtime.resolve_node_runtime(
            self._pipeline,
            node,
            runtime_config=self._runtime_config,
            run_override=self._runtime_overrides.get(node),
        )
        backend = self._node_runtime_registry.backend_for(resolution.name)
        context = m_node_runtime.NodeRuntimeContext(
            node=node,
            node_label=self._node_alias(node) or self._node_name(node),
            inputs=kwargs,
            output_port=self._single_output_port(node),
            callback=self._callback,
            work_dir=self._node_runtime_work_dir(node, resolution.name),
            environment_provider=self._node_environment_provider,
            runtime_config=self._runtime_config,
            environment_spec=self._runtime_env_spec,
        )
        environment = backend.prepare(context)
        self._node_runtimes[node] = m_node_runtime.runtime_manifest_record(resolution, environment)
        self._write_node_manifest(node, status="running", inputs=input_records)
        return backend.execute(context, environment)

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

    def _record_link_input(
        self, target_node: Node, target_port: InputPort, value_ref: Any, value: Any
    ) -> dict[str, Any]:
        source_ref = self._source_ref(value_ref)
        if source_ref is None:
            return self._value_record(value)

        adapter_format = value_ref.format if isinstance(value_ref, FormattedOutputRef) else None
        run_override = self._adapter_override_for(source_ref)
        resolution = self._adapter_resolutions.get((source_ref.node, source_ref.port.name))
        if resolution is None:
            resolution = self._pipeline.resolve_adapter_binding(
                py_type=type(value), format=adapter_format, run_override=run_override
            )
            if resolution is not None:
                self._adapter_resolutions[(source_ref.node, source_ref.port.name)] = resolution

        record = self._edge_value_record(source_ref, value)
        adapter_record = None
        if resolution is not None:
            adapter_record = m_manifest.adapter_record(adapter_identity(resolution.adapter), str(resolution.source))
            self._set_node_adapter(source_ref.node, source_ref.port.name, adapter_record)
            self._set_node_adapter(target_node, target_port.name, adapter_record)

        writer = self._manifest_writer
        if writer is not None:
            writer.add_edge(
                m_manifest.edge_record(
                    source_node_id=self._node_id(source_ref.node),
                    source_port=source_ref.port.name,
                    target_node_id=self._node_id(target_node),
                    target_port=target_port.name,
                    artifact=record,
                    adapter=None if adapter_record is None else m_manifest.edge_adapter_record(adapter_record),
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

    def _edge_value_record(self, source_ref: NodeOutputRef, value: Any) -> dict[str, Any]:
        if self._is_frozen_node(source_ref.node) and self._resume_plan is not None:
            parent_record = m_resume.manifest_output_record(
                self._resume_plan.parent_manifest, source_ref.node, source_ref.port.name
            )
            record = m_resume.rebase_output_record(parent_record, self._resume_plan.parent_run_dir, self._run_dir)
            self._set_node_output(source_ref.node, source_ref.port.name, record)
            return record
        ref = self._artifact_ref_for_output(source_ref.node, source_ref.port.name)
        if ref is not None:
            record = m_manifest.artifact_record(ref, run_dir=self._run_dir)
            self._set_node_output(source_ref.node, source_ref.port.name, record)
            return record
        return self._value_record(value)

    def _value_record(self, value: Any) -> dict[str, Any]:
        if type(value) in _JSON_NATIVE_TYPES:
            return m_manifest.json_record(value)
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

    def _output_records(self, node: Node, result: dict[str, Any]) -> dict[str, Any]:
        outputs = {}
        for port_name, value in result.items():
            ref = self._artifact_ref_for_output(node, port_name)
            outputs[port_name] = (
                m_manifest.artifact_record(ref, run_dir=self._run_dir) if ref is not None else self._value_record(value)
            )
        return outputs

    def _artifact_ref_for_output(self, node: Node, port_name: str) -> ArtifactRef | None:
        refs = [
            (adapter_key, ref)
            for ref_node, ref_port, adapter_key in self._artifact_refs
            if ref_node == node and ref_port == port_name
            for ref in (self._artifact_refs[(ref_node, ref_port, adapter_key)],)
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
        writer.set_node(
            m_manifest.node_record(
                node_id=node_id,
                alias=self._node_alias(node),
                kind=self._node_kind(node),
                name=self._node_name(node),
                status=status,
                fingerprint_sha256=self._node_fingerprint(node, merged_inputs, adapters),
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
            port: record["identity"] for port, record in adapters.items() if isinstance(record.get("identity"), Mapping)
        }
        return node_fingerprint(
            node_content=self._node_content(node),
            node_version=self._node_version(node),
            input_ports=[port.name for port in node.inputs],
            output_ports=[port.name for port in node.outputs],
            adapter_identities=adapter_identities,
            artifact_inputs=artifact_inputs,
            inline_inputs=inline_inputs,
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
