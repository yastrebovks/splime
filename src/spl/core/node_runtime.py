"""Per-node runtime resolution and execution backends."""

from __future__ import annotations

import ast
import builtins
import hashlib
import inspect
import json
import logging
import os
import shutil
import stat
import subprocess
import sys
import textwrap
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, TextIO, cast
from uuid import uuid4

from spl._process import run_process_tree
from spl._timeout import TimeoutDomain, validate_timeout_seconds
from spl.adapters import builtin_id_for_adapter
from spl.core import runtime_port_adapters as m_runtime_port_adapters
from spl.core.entities.adapter import LoadAdapter, SaveAdapter, save_adapter_identity
from spl.core.entities.artifact import ArtifactRef
from spl.core.entities.function import DFunction
from spl.core.entities.node import InputPort, Node, OutputPort
from spl.core.entities.node_function import NodeFunction
from spl.core.entities.pipeline import Pipeline
from spl.core.fingerprint import canonical_json_bytes
from spl.core.ir.parse import _branch, ir_parse
from spl.core.json_contract import dumps as json_dumps

NATIVE_NODE_RUNTIME = "native"
VENV_SUBPROCESS_NODE_RUNTIME = "venv-subprocess"
DOCKER_NODE_RUNTIME = "docker"
RUNTIME_TAG_NAME = "runtime"
REMOTE_RUN_RUNTIME_OVERRIDES_CAPABILITY = "spl.remote_run.runtime_overrides.v1"
NODE_TIMEOUT_SECONDS_KEY = "node_timeout_seconds"
_SPL_NODE_CONTAINER_WORKDIR = "/spl-node"
_SPL_OBJECT_RUNTIME_BACKEND_ENV = "SPL_OBJECT_RUNTIME_BACKEND"
_SPL_OBJECT_DOCKER_WORKER_ENV = "SPL_OBJECT_DOCKER_WORKER"
_MAX_ISOLATED_RESULT_BYTES = 1024 * 1024
_MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES = 64 * 1024
_DOCKER_NODE_IMAGE_REMEDIATION = (
    'per-node docker runtime requires runtime_config["docker"]["image"] when running without the SPL daemon; '
    "run the object through the daemon so it can prepare the object image, or set "
    'runtime_config["docker"]["image"] to a Docker image that is already available to Docker.'
)

LOGGER = logging.getLogger(__name__)

RunRuntimeOverrides = str | Mapping[str, str]
NormalizedRunRuntimeOverrides = dict[Node, str]


def explicit_docker_image_spec_hash(image_tag: str) -> str:
    """Return the stable config hash for an explicit per-node Docker image."""

    return hashlib.sha256(
        canonical_json_bytes({"node_runtime": DOCKER_NODE_RUNTIME, "image_tag": image_tag})
    ).hexdigest()


class NodeRuntimeResolutionSource(StrEnum):
    """Source level that selected a node runtime."""

    DEFAULT = "default"
    OBJECT_RUNTIME_CONFIG = "object-runtime-config"
    NODE_TAG = "node-tag"
    RUN_OVERRIDE = "run-override"


@dataclass(frozen=True)
class NodeRuntimeResolution:
    """Resolved runtime name and the source level that selected it."""

    name: str
    source: NodeRuntimeResolutionSource


@dataclass(frozen=True)
class PreparedNodeEnvironment:
    """Resolved execution environment for one node runtime."""

    name: str
    python_path: Path | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class InlineInput:
    """Private in-process input value; isolated backends require strict JSON."""

    value: Any


@dataclass(frozen=True)
class ArtifactInput:
    """Private verified-artifact intent for one isolated consumer input."""

    ref: ArtifactRef
    load_adapter: LoadAdapter
    resolution_source: str


@dataclass(frozen=True)
class ArtifactOutputVariantPlan:
    """One exact save identity in an isolated output fan-out plan."""

    variant_id: str
    save_adapter: SaveAdapter
    resolution_source: str


@dataclass(frozen=True)
class ArtifactOutputPlan:
    """Private pre-execution save intent for one isolated output port."""

    save_adapter: SaveAdapter
    resolution_source: str
    artifacts_dir: Path
    variant_id: str | None = None
    additional_variants: tuple[ArtifactOutputVariantPlan, ...] = ()

    def variants(self) -> tuple[ArtifactOutputVariantPlan, ...]:
        """Return every planned save identity in deterministic order."""

        primary_id = (
            self.variant_id
            or hashlib.sha256(canonical_json_bytes(save_adapter_identity(self.save_adapter))).hexdigest()
        )
        primary = ArtifactOutputVariantPlan(primary_id, self.save_adapter, self.resolution_source)
        return (primary, *self.additional_variants)


@dataclass(frozen=True)
class ArtifactOutputVariant:
    """One promoted artifact variant produced by an isolated node."""

    variant_id: str
    ref: ArtifactRef
    save_adapter: SaveAdapter
    resolution_source: str


@dataclass(frozen=True)
class ArtifactOutput:
    """Private promoted isolated result; public Run access loads it lazily."""

    ref: ArtifactRef
    save_adapter: SaveAdapter
    resolution_source: str
    variant_id: str | None = None
    additional_variants: tuple[ArtifactOutputVariant, ...] = ()

    def variants(self) -> tuple[ArtifactOutputVariant, ...]:
        """Return every promoted artifact variant in deterministic order."""

        primary_id = (
            self.variant_id
            or hashlib.sha256(canonical_json_bytes(save_adapter_identity(self.save_adapter))).hexdigest()
        )
        primary = ArtifactOutputVariant(primary_id, self.ref, self.save_adapter, self.resolution_source)
        return (primary, *self.additional_variants)


NodeInputTransport = InlineInput | ArtifactInput


@dataclass(frozen=True)
class _PreparedIsolatedInputs:
    input_json: str
    document: dict[str, Any]
    artifacts: dict[str, ArtifactRef]
    custom_bundle_body: bytes | None


class NodeEnvironmentProvider(Protocol):
    """Prepare or locate the Python environment used by a node runtime."""

    def prepare(
        self,
        spec: Mapping[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> PreparedNodeEnvironment:
        """Return the environment for ``spec``."""
        ...


@dataclass(frozen=True)
class NodeRuntimeContext:
    """Execution inputs shared by node runtime backends."""

    node: Node
    node_label: str
    inputs: dict[InputPort, NodeInputTransport]
    output_port: OutputPort
    callback: Callable[[Node, dict[InputPort, Any]], dict[str, Any]]
    work_dir: Path
    environment_provider: NodeEnvironmentProvider
    runtime_config: Mapping[str, Any]
    environment_spec: Sequence[Mapping[str, Any]]
    output_plan: ArtifactOutputPlan | None = None
    isolated_inputs: _PreparedIsolatedInputs | None = None


class NodeRuntimeBackend(Protocol):
    """Minimal execution contract for interchangeable node runtimes."""

    name: str

    def prepare(self, context: NodeRuntimeContext) -> PreparedNodeEnvironment:
        """Prepare the runtime environment."""
        ...

    def execute(self, context: NodeRuntimeContext, environment: PreparedNodeEnvironment) -> dict[str, Any]:
        """Execute one node and return output values by port name."""
        ...


class CurrentPythonEnvironmentProvider:
    """Environment provider that resolves to the current interpreter."""

    def prepare(
        self,
        spec: Mapping[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> PreparedNodeEnvironment:
        del wait, retry_failed
        spec_hash = hashlib.sha256(canonical_json_bytes(spec)).hexdigest()
        return PreparedNodeEnvironment(
            name="current-python",
            python_path=Path(sys.executable),
            metadata={
                "spec_hash": spec_hash,
                "spec": dict(spec),
            },
        )


class NativeNodeRuntime:
    """Execute a node in the current conductor process."""

    name = NATIVE_NODE_RUNTIME

    def prepare(self, context: NodeRuntimeContext) -> PreparedNodeEnvironment:
        del context
        return PreparedNodeEnvironment(name="current-process", python_path=Path(sys.executable), metadata={})

    def execute(self, context: NodeRuntimeContext, environment: PreparedNodeEnvironment) -> dict[str, Any]:
        del environment
        inputs = {
            port: value.value if isinstance(value, InlineInput) else value for port, value in context.inputs.items()
        }
        return context.callback(context.node, inputs)


class VenvSubprocessNodeRuntime:
    """Execute one JSON-native function node through the SPL-free subprocess runner.

    ``runtime_config["node_timeout_seconds"]`` optionally bounds subprocess
    execution time; native nodes still run without a per-node timeout.
    """

    name = VENV_SUBPROCESS_NODE_RUNTIME

    def prepare(self, context: NodeRuntimeContext) -> PreparedNodeEnvironment:
        prepare_for_node = getattr(context.environment_provider, "prepare_for_node", None)
        if callable(prepare_for_node):
            return cast(
                PreparedNodeEnvironment,
                prepare_for_node(
                    _runtime_spec(self.name, context),
                    node_label=context.node_label,
                ),
            )
        return context.environment_provider.prepare(_runtime_spec(self.name, context))

    def execute(self, context: NodeRuntimeContext, environment: PreparedNodeEnvironment) -> dict[str, Any]:
        if not isinstance(context.node, NodeFunction):
            raise RuntimeError("venv-subprocess runtime supports function nodes only")
        if environment.python_path is None:
            raise RuntimeError("venv-subprocess runtime requires a Python executable")

        invocation = _prepare_spl_free_invocation(context, runtime_name=self.name)
        command = [
            str(environment.python_path),
            str(invocation.runner_path),
            *_spl_free_runner_args(
                module_path=str(invocation.module_path),
                module_name=invocation.module_name,
                entrypoint=context.node.func.__name__,
                input_path=str(invocation.input_path),
                result_path=str(invocation.result_path),
                artifacts_dir=str(invocation.artifacts_dir),
                env_spec_path=str(invocation.env_spec_path),
            ),
        ]
        return _run_spl_free_invocation(
            context,
            invocation,
            command,
            runtime_name=self.name,
            failure_target="`{}`".format(context.node.func.__name__),
            timeout_cleanup=None,
        )


class DockerNodeRuntime:
    """Execute one JSON-native function node through the SPL-free Docker runner."""

    name = DOCKER_NODE_RUNTIME

    def prepare(self, context: NodeRuntimeContext) -> PreparedNodeEnvironment:
        _raise_if_nested_object_docker()
        explicit_image = _explicit_docker_image(context.runtime_config)
        if explicit_image is not None:
            return PreparedNodeEnvironment(
                name="docker-image",
                python_path=None,
                metadata={
                    "image_tag": explicit_image,
                    "spec_hash": explicit_docker_image_spec_hash(explicit_image),
                    "source": "runtime_config.docker.image",
                },
            )

        environment = context.environment_provider.prepare(_runtime_spec(self.name, context))
        image_tag = environment.metadata.get("image_tag")
        if not isinstance(image_tag, str) or not image_tag:
            raise RuntimeError(_DOCKER_NODE_IMAGE_REMEDIATION)
        return environment

    def execute(self, context: NodeRuntimeContext, environment: PreparedNodeEnvironment) -> dict[str, Any]:
        if not isinstance(context.node, NodeFunction):
            raise RuntimeError("docker runtime supports function nodes only")
        image_tag = environment.metadata.get("image_tag")
        if not isinstance(image_tag, str) or not image_tag:
            raise RuntimeError(_DOCKER_NODE_IMAGE_REMEDIATION)

        invocation = _prepare_spl_free_invocation(context, runtime_name=self.name)
        container_name = _docker_container_name(context)
        docker_options = _node_docker_runtime_options(context.runtime_config)
        network_args = _docker_network_args(docker_options)
        command = [
            "docker",
            "run",
            "--rm",
            "--name",
            container_name,
            *_docker_label_args(),
            "-v",
            "{}:{}".format(invocation.work_dir.resolve(), _SPL_NODE_CONTAINER_WORKDIR),
            "-w",
            _SPL_NODE_CONTAINER_WORKDIR,
            *network_args,
            *_docker_hardening_args(docker_options),
            *_docker_user_args(),
            *_docker_env_args(docker_options),
            image_tag,
            "python",
            "{}/spl_free_runner.py".format(_SPL_NODE_CONTAINER_WORKDIR),
            *_spl_free_runner_args(
                module_path="{}/node_module.py".format(_SPL_NODE_CONTAINER_WORKDIR),
                module_name=invocation.module_name,
                entrypoint=context.node.func.__name__,
                input_path="{}/input.json".format(_SPL_NODE_CONTAINER_WORKDIR),
                result_path="{}/result.json".format(_SPL_NODE_CONTAINER_WORKDIR),
                artifacts_dir="{}/artifacts".format(_SPL_NODE_CONTAINER_WORKDIR),
                env_spec_path="{}/env-spec.json".format(_SPL_NODE_CONTAINER_WORKDIR),
            ),
        ]
        return _run_spl_free_invocation(
            context,
            invocation,
            command,
            runtime_name=self.name,
            failure_target="node `{}`".format(context.node_label),
            timeout_cleanup=lambda: _kill_docker_container(container_name),
        )


NodeRuntimeFactory = Callable[[], NodeRuntimeBackend]


NODE_RUNTIME_BACKENDS: dict[str, NodeRuntimeFactory] = {
    NATIVE_NODE_RUNTIME: NativeNodeRuntime,
    VENV_SUBPROCESS_NODE_RUNTIME: VenvSubprocessNodeRuntime,
    DOCKER_NODE_RUNTIME: DockerNodeRuntime,
}


class NodeRuntimeRegistry:
    """Create node runtime backends from explicit runtime names."""

    def __init__(self, backends: Mapping[str, NodeRuntimeFactory] | None = None):
        self.backends = dict(backends or NODE_RUNTIME_BACKENDS)

    def backend_for(self, runtime_name: str) -> NodeRuntimeBackend:
        try:
            factory = self.backends[runtime_name]
        except KeyError as exc:
            raise ValueError("unsupported node runtime: {}".format(runtime_name)) from exc
        return factory()


def resolve_node_runtime(
    pipeline: Pipeline,
    node: Node,
    *,
    runtime_config: Mapping[str, Any] | None = None,
    run_override: str | None = None,
    default_runtime: str = NATIVE_NODE_RUNTIME,
) -> NodeRuntimeResolution:
    """Resolve the runtime for ``node`` and report the selected source level."""

    if node not in pipeline.nodes:
        raise ValueError("node runtime resolution received a node outside the pipeline")
    resolution = NodeRuntimeResolution(_validate_runtime_name(default_runtime), NodeRuntimeResolutionSource.DEFAULT)
    config = runtime_config or {}
    configured = config.get("node_runtime")
    if configured is not None:
        resolution = NodeRuntimeResolution(
            _validate_runtime_name(configured),
            NodeRuntimeResolutionSource.OBJECT_RUNTIME_CONFIG,
        )
    node_tags = pipeline.tags.get(str(node.uuid), {})
    tagged = node_tags.get(RUNTIME_TAG_NAME)
    if tagged is not None:
        resolution = NodeRuntimeResolution(_validate_runtime_name(tagged), NodeRuntimeResolutionSource.NODE_TAG)
    if run_override is not None:
        resolution = NodeRuntimeResolution(
            _validate_runtime_name(run_override), NodeRuntimeResolutionSource.RUN_OVERRIDE
        )
    return resolution


def validate_run_runtime_overrides(
    pipeline: Pipeline,
    runtimes: RunRuntimeOverrides | None,
) -> NormalizedRunRuntimeOverrides:
    """Validate run-level runtime overrides and normalize them to nodes.

    A mapping keeps the established per-alias behavior.  A string is the
    whole-Pipeline shorthand and applies to every local Function node; remote
    references keep their own execution authority.
    """

    if runtimes is None:
        return {}
    if isinstance(runtimes, str):
        runtime_name = _validate_runtime_name(runtimes)
        return {node: runtime_name for node in pipeline.nodes if isinstance(node, NodeFunction)}
    if not isinstance(runtimes, Mapping):
        raise TypeError("run runtime overrides must be a runtime name or mapping")
    normalized: NormalizedRunRuntimeOverrides = {}
    for alias, runtime_name in runtimes.items():
        if not isinstance(alias, str) or not alias:
            raise ValueError("run runtime override alias must be a non-empty string")
        if alias not in pipeline.aliases:
            raise ValueError("run runtime override references unknown alias `{}`".format(alias))
        normalized[pipeline.aliases[alias]] = _validate_runtime_name(runtime_name)
    return normalized


def validate_node_runtime_config(runtime_config: Mapping[str, Any] | None) -> dict[str, Any]:
    """Validate run-level node runtime configuration and return a copy."""

    config = dict(runtime_config or {})
    node_timeout_seconds(config)
    return config


def node_timeout_seconds(runtime_config: Mapping[str, Any]) -> float | None:
    """Return the configured non-native node runtime timeout in seconds."""

    return validate_timeout_seconds(
        runtime_config.get(NODE_TIMEOUT_SECONDS_KEY),
        name='runtime_config["{}"]'.format(NODE_TIMEOUT_SECONDS_KEY),
        domain=TimeoutDomain.POSITIVE,
        allow_none=True,
    )


def prepare_isolated_node_inputs(context: NodeRuntimeContext, *, runtime_name: str) -> NodeRuntimeContext:
    """Close one mixed input/output plan before environment preparation or execution."""

    artifact_items = [
        (port, value)
        for port, value in sorted(context.inputs.items(), key=lambda item: item[0].name)
        if isinstance(value, ArtifactInput)
    ]
    if not artifact_items and context.output_plan is None:
        return context

    custom_adapters: list[LoadAdapter] = []
    seen_custom: set[int] = set()
    for _, value in artifact_items:
        assert isinstance(value, ArtifactInput)
        if _builtin_id_for_load_adapter(value.load_adapter) is None and id(value.load_adapter) not in seen_custom:
            custom_adapters.append(value.load_adapter)
            seen_custom.add(id(value.load_adapter))
    output_variants = () if context.output_plan is None else context.output_plan.variants()
    custom_save_adapters: list[SaveAdapter] = []
    seen_custom_saves: set[int] = set()
    for variant in output_variants:
        adapter = variant.save_adapter
        if _builtin_id_for_save_adapter(adapter) is None and id(adapter) not in seen_custom_saves:
            custom_save_adapters.append(adapter)
            seen_custom_saves.add(id(adapter))
    custom_bundle: dict[str, Any] | None = None
    custom_load_symbols: dict[int, str] = {}
    custom_save_symbols: dict[int, str] = {}
    try:
        if context.output_plan is None:
            if custom_adapters:
                custom_bundle, custom_load_symbols = m_runtime_port_adapters.build_custom_load_bundle(custom_adapters)
        elif custom_adapters or custom_save_adapters:
            custom_bundle, custom_load_symbols, custom_save_symbols = (
                m_runtime_port_adapters.build_custom_isolated_bundle(
                    custom_adapters,
                    custom_save_adapters,
                )
            )
        bindings: list[dict[str, Any]] = []
        inputs: list[dict[str, Any]] = []
        artifacts: dict[str, ArtifactRef] = {}
        inline_kwargs: dict[str, Any] = {}
        for port, transport in sorted(context.inputs.items(), key=lambda item: item[0].name):
            if isinstance(transport, InlineInput):
                inline_kwargs[port.name] = transport.value
                continue
            if not isinstance(transport, ArtifactInput):
                inline_kwargs[port.name] = transport
                continue
            ref = transport.ref
            ref_tag = cast(str, ref.tag)
            if ref_tag not in transport.load_adapter.accepted_tags:
                raise m_runtime_port_adapters.RuntimePortAdapterContractError(
                    "input_tag",
                    f"artifact tag {ref_tag!r} is not accepted",
                    stage="node_adapter_input_validation",
                )
            if transport.load_adapter.legacy_key_guard and ref.key != transport.load_adapter.key:
                raise m_runtime_port_adapters.RuntimePortAdapterContractError(
                    "input_key",
                    "artifact key does not match the guarded load adapter",
                    stage="node_adapter_input_validation",
                )
            adapter_id = _builtin_id_for_load_adapter(transport.load_adapter)
            if adapter_id is None:
                if custom_bundle is None or id(transport.load_adapter) not in custom_load_symbols:
                    raise m_runtime_port_adapters.RuntimePortAdapterContractError(
                        "custom_bundle",
                        "custom load adapter source metadata is missing",
                        stage="environment_preflight",
                    )
                descriptor = m_runtime_port_adapters.load_adapter_descriptor(
                    transport.load_adapter,
                    adapter_id=None,
                    artifact_tag=ref_tag,
                    custom_bundle_sha256=str(custom_bundle["sha256"]),
                    load_symbol=custom_load_symbols[id(transport.load_adapter)],
                )
            else:
                descriptor = m_runtime_port_adapters.load_adapter_descriptor(
                    transport.load_adapter,
                    adapter_id=adapter_id,
                    artifact_tag=ref_tag,
                )
            name = _isolated_input_name(port.name, ref.sha256)
            if name in artifacts:
                raise m_runtime_port_adapters.RuntimePortAdapterContractError(
                    "input_duplicate",
                    "isolated input artifact name is duplicated",
                    stage="node_adapter_input_validation",
                )
            artifacts[name] = ref
            semantic_type = _isolated_wire_semantic_type(port.typ_)
            inputs.append(
                {
                    "name": name,
                    "port": port.name,
                    "size": ref.size,
                    "sha256": ref.sha256.casefold(),
                    "format_tag": ref_tag,
                    "semantic_type": semantic_type,
                    "adapter_id": descriptor["id"],
                    "media_type": descriptor["presentation"]["media_type"],
                    "content_base64": None,
                    "staged_name": name,
                }
            )
            bindings.append(
                {
                    "direction": "input",
                    "port": port.name,
                    "external_name": port.name,
                    "semantic_type": semantic_type,
                    "adapter": descriptor,
                    "resolution_source": _wire_adapter_resolution_source(transport.resolution_source),
                    "transport": "artifact",
                    "argument": {"kind": "keyword", "name": port.name, "index": None},
                    "input_name": name,
                    "result_path": [],
                    "artifact_key": ref.key,
                    "legacy_key_guard": transport.load_adapter.legacy_key_guard,
                }
            )
        for output_variant in output_variants:
            output_adapter = output_variant.save_adapter
            adapter_id = _builtin_id_for_save_adapter(output_adapter)
            if adapter_id is None:
                if custom_bundle is None or id(output_adapter) not in custom_save_symbols:
                    raise m_runtime_port_adapters.RuntimePortAdapterContractError(
                        "custom_bundle",
                        "custom save adapter source metadata is missing",
                        stage="environment_preflight",
                    )
                output_descriptor = m_runtime_port_adapters.save_adapter_descriptor(
                    output_adapter,
                    adapter_id=None,
                    custom_bundle_sha256=str(custom_bundle["sha256"]),
                    save_symbol=custom_save_symbols[id(output_adapter)],
                )
            else:
                output_descriptor = m_runtime_port_adapters.save_adapter_descriptor(
                    output_adapter,
                    adapter_id=adapter_id,
                )
            output_port = context.output_port
            output_binding: dict[str, Any] = {
                "direction": "output",
                "port": output_port.name,
                "external_name": output_port.name,
                "semantic_type": _isolated_wire_semantic_type(output_port.typ_),
                "adapter": output_descriptor,
                "resolution_source": _wire_adapter_resolution_source(output_variant.resolution_source),
                "transport": "artifact",
                "argument": None,
                "input_name": None,
                "result_path": [],
                "artifact_key": output_adapter.key,
                "legacy_key_guard": False,
            }
            if len(output_variants) > 1:
                output_binding["variant_id"] = output_variant.variant_id
            bindings.append(output_binding)
        wire_bundle = None
        bundle_body = None
        if custom_bundle is not None:
            bundle_body = cast(bytes, custom_bundle["source_bytes"])
            wire_bundle = {
                "name": custom_bundle["name"],
                "size": custom_bundle["size"],
                "sha256": custom_bundle["sha256"],
                "functions": custom_bundle["functions"],
                "content_base64": None,
                "staged_name": custom_bundle["name"],
            }
        if context.output_plan is None:
            schema_version = m_runtime_port_adapters.ISOLATED_NODE_INPUTS_SCHEMA_VERSION
        elif len(output_variants) == 1:
            schema_version = m_runtime_port_adapters.ISOLATED_NODE_TRANSPORT_SCHEMA_VERSION
        else:
            schema_version = m_runtime_port_adapters.ISOLATED_NODE_MULTI_OUTPUT_TRANSPORT_SCHEMA_VERSION
        raw_document = {
            "schema_version": schema_version,
            "bindings": bindings,
            "inputs": inputs,
            "custom_bundle": wire_bundle,
        }
        if context.output_plan is None:
            document = m_runtime_port_adapters.normalize_isolated_node_inputs(
                raw_document,
                inline_keyword_names=inline_kwargs,
            )
            m_runtime_port_adapters.validate_isolated_node_input_bundle_dependencies(
                bundle_body or b"",
                document,
            )
            merged_distributions = m_runtime_port_adapters.merge_isolated_node_input_distributions(
                context.environment_spec,
                document,
            )
        elif schema_version == m_runtime_port_adapters.ISOLATED_NODE_TRANSPORT_SCHEMA_VERSION:
            document = m_runtime_port_adapters.normalize_isolated_node_transport(
                raw_document,
                inline_keyword_names=inline_kwargs,
            )
            m_runtime_port_adapters.validate_isolated_node_transport_bundle_dependencies(
                bundle_body or b"",
                document,
            )
            merged_distributions = m_runtime_port_adapters.merge_isolated_node_transport_distributions(
                context.environment_spec,
                document,
            )
        else:
            document = m_runtime_port_adapters.normalize_isolated_node_multi_output_transport(
                raw_document,
                inline_keyword_names=inline_kwargs,
            )
            m_runtime_port_adapters.validate_isolated_node_multi_output_transport_bundle_dependencies(
                bundle_body or b"",
                document,
            )
            merged_distributions = m_runtime_port_adapters.merge_isolated_node_multi_output_transport_distributions(
                context.environment_spec,
                document,
            )
        payload = {
            "schema_version": schema_version,
            "args": [],
            "kwargs": inline_kwargs,
            "runtime_port_adapters": document,
        }
        input_json = json_dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except m_runtime_port_adapters.RuntimePortAdapterContractError as exc:
        ports = ", ".join(
            [*(port.name for port, _ in artifact_items), *([context.output_port.name] if context.output_plan else [])]
        )
        raise RuntimeError(
            "{}: node `{}` artifact port(s) {} cannot be prepared for {}: {}. "
            "Choose a supported adapter with `.as_format()` or a run-level adapter override.".format(
                exc.stage,
                context.node_label,
                ports,
                runtime_name,
                exc.safe_message,
            )
        ) from None
    return replace(
        context,
        environment_spec=merged_distributions,
        isolated_inputs=_PreparedIsolatedInputs(
            input_json=input_json,
            document=document,
            artifacts=artifacts,
            custom_bundle_body=bundle_body,
        ),
    )


def _isolated_input_name(port_name: str, sha256: str) -> str:
    token = "".join(character if character.isalnum() or character in "._-" else "-" for character in port_name)
    token = token.strip(".-") or "input"
    port_digest = hashlib.sha256(port_name.encode("utf-8")).hexdigest()[:16]
    return "input-{}-{}-{}.bin".format(token[:72], port_digest, sha256.casefold()[:16])


def _isolated_wire_semantic_type(value: Any) -> str | None:
    """Return only the restricted advisory spelling accepted by the local wire schema."""

    if not isinstance(value, str) or not value:
        return None
    return m_runtime_port_adapters.isolated_wire_semantic_type(value)


def _builtin_id_for_load_adapter(adapter: LoadAdapter) -> str | None:
    try:
        return builtin_id_for_adapter(cast(Any, adapter))
    except AttributeError:
        return None


def _builtin_id_for_save_adapter(adapter: SaveAdapter) -> str | None:
    try:
        return builtin_id_for_adapter(cast(Any, adapter))
    except AttributeError:
        return None


def _wire_adapter_resolution_source(value: str) -> str:
    if value == "run-override":
        return "run_override"
    if value == "port-default":
        return "system_default"
    return "preset"


@dataclass(frozen=True)
class _SplFreeInvocation:
    work_dir: Path
    artifacts_dir: Path
    input_path: Path
    result_path: Path
    env_spec_path: Path
    stdout_path: Path
    stderr_path: Path
    runner_path: Path
    module_path: Path
    module_name: str


def _prepare_spl_free_invocation(context: NodeRuntimeContext, *, runtime_name: str) -> _SplFreeInvocation:
    if not isinstance(context.node, NodeFunction):
        raise RuntimeError("{} runtime supports function nodes only".format(runtime_name))

    input_json = (
        context.isolated_inputs.input_json
        if context.isolated_inputs is not None
        else _subprocess_input_json(context, runtime_name=runtime_name)
    )
    module_text = _generated_node_module_text(context.node, context.node_label, runtime_name=runtime_name)
    work_dir = context.work_dir
    artifacts_dir = work_dir / "artifacts"
    input_path = work_dir / "input.json"
    result_path = work_dir / "result.json"
    env_spec_path = work_dir / "env-spec.json"
    stdout_path = work_dir / "stdout.txt"
    stderr_path = work_dir / "stderr.txt"
    runner_path = work_dir / "spl_free_runner.py"
    module_path = work_dir / "node_module.py"
    module_name = "_spl_node_{}".format(str(context.node.uuid).replace("-", "_"))

    work_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if context.isolated_inputs is not None:
        _stage_isolated_node_inputs(work_dir, context.isolated_inputs)
    _write_generated_node_module(module_path, module_text)
    _copy_spl_free_runner(runner_path)
    input_path.write_text(input_json, encoding="utf-8")
    _write_json(env_spec_path, list(context.environment_spec))
    return _SplFreeInvocation(
        work_dir=work_dir,
        artifacts_dir=artifacts_dir,
        input_path=input_path,
        result_path=result_path,
        env_spec_path=env_spec_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        runner_path=runner_path,
        module_path=module_path,
        module_name=module_name,
    )


def _stage_isolated_node_inputs(work_dir: Path, prepared: _PreparedIsolatedInputs) -> None:
    target_dir = work_dir / "runtime-inputs"
    target_dir.mkdir(mode=0o700, exist_ok=False)
    try:
        target_dir.chmod(0o700)
    except OSError:
        pass
    try:
        for name, ref in prepared.artifacts.items():
            body = _read_verified_artifact_ref(ref)
            _write_private_runtime_file(target_dir / name, body)
            _read_verified_runtime_file(
                target_dir / name,
                expected_size=ref.size,
                expected_sha256=ref.sha256.casefold(),
            )
        if prepared.custom_bundle_body is not None:
            bundle = prepared.document["custom_bundle"]
            if not isinstance(bundle, Mapping):
                raise RuntimeError("node_adapter_input_validation: custom bundle metadata is missing")
            bundle_target = target_dir / str(bundle["staged_name"])
            _write_private_runtime_file(bundle_target, prepared.custom_bundle_body)
            _read_verified_runtime_file(
                bundle_target,
                expected_size=int(bundle["size"]),
                expected_sha256=str(bundle["sha256"]),
            )
    except BaseException:
        shutil.rmtree(target_dir, ignore_errors=True)
        raise


def _read_verified_artifact_ref(ref: ArtifactRef) -> bytes:
    if ref.size > m_runtime_port_adapters.MAX_RUNTIME_INPUT_BYTES:
        raise RuntimeError("node_adapter_input_validation: artifact input exceeds the per-file size limit")
    try:
        body = _read_bounded_regular_file(Path(ref.uri))
    except (OSError, RuntimeError):
        raise RuntimeError("node_adapter_input_validation: artifact input is not a stable regular file") from None
    if len(body) != ref.size or hashlib.sha256(body).hexdigest() != ref.sha256.casefold():
        raise RuntimeError("node_adapter_input_validation: artifact input size or checksum does not match")
    return body


def _read_verified_runtime_file(path: Path, *, expected_size: int, expected_sha256: str) -> bytes:
    body = _read_bounded_regular_file(path)
    if len(body) != expected_size or hashlib.sha256(body).hexdigest() != expected_sha256:
        raise RuntimeError("node_adapter_input_validation: staged artifact input failed integrity verification")
    return body


def _read_bounded_regular_file(path: Path) -> bytes:
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or _is_reparse_point(before):
        raise RuntimeError("node_adapter_input_validation: artifact input is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or _is_reparse_point(opened)
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise RuntimeError("node_adapter_input_validation: artifact input changed before it was opened")
        chunks: list[bytes] = []
        remaining = m_runtime_port_adapters.MAX_RUNTIME_INPUT_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    current = path.lstat()
    if (
        len(body) > m_runtime_port_adapters.MAX_RUNTIME_INPUT_BYTES
        or not stat.S_ISREG(after.st_mode)
        or not stat.S_ISREG(current.st_mode)
        or after.st_nlink != 1
        or current.st_nlink != 1
        or _is_reparse_point(after)
        or _is_reparse_point(current)
        or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
        or (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino)
        or after.st_size != len(body)
        or current.st_size != len(body)
    ):
        raise RuntimeError("node_adapter_input_validation: artifact input changed while it was read")
    return body


def _write_private_runtime_file(path: Path, body: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o400)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _is_reparse_point(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0))
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _run_spl_free_invocation(
    context: NodeRuntimeContext,
    invocation: _SplFreeInvocation,
    command: list[str],
    *,
    runtime_name: str,
    failure_target: str,
    timeout_cleanup: Callable[[], None] | None,
) -> dict[str, Any]:
    timeout_seconds = node_timeout_seconds(context.runtime_config)
    stdout_handle, stdout_identity = _open_runtime_diagnostic(invocation.stdout_path)
    try:
        stderr_handle, stderr_identity = _open_runtime_diagnostic(invocation.stderr_path)
    except BaseException:
        stdout_handle.close()
        raise
    with stdout_handle, stderr_handle:
        try:
            completed = run_process_tree(
                command,
                cwd=invocation.work_dir,
                env=_subprocess_env_without_project_pythonpath(),
                timeout=timeout_seconds,
                stdout_target=stdout_handle,
                stderr_target=stderr_handle,
            )
        except subprocess.TimeoutExpired as exc:
            if timeout_cleanup is not None:
                timeout_cleanup()
            _retain_bounded_runtime_diagnostic(
                invocation.stdout_path,
                fallback=exc.stdout,
                trusted_handle=stdout_handle,
                expected_identity=stdout_identity,
            )
            _retain_bounded_runtime_diagnostic(
                invocation.stderr_path,
                fallback=exc.stderr,
                trusted_handle=stderr_handle,
                expected_identity=stderr_identity,
            )
            _remove_incomplete_runtime_outputs(context, invocation)
            raise RuntimeError(
                "node runtime `{}` timed out after {}s for {}".format(
                    runtime_name,
                    _format_timeout_seconds(timeout_seconds),
                    failure_target,
                )
            ) from exc
        except BaseException:
            if timeout_cleanup is not None:
                timeout_cleanup()
            _retain_bounded_runtime_diagnostic(
                invocation.stdout_path,
                fallback=None,
                trusted_handle=stdout_handle,
                expected_identity=stdout_identity,
            )
            _retain_bounded_runtime_diagnostic(
                invocation.stderr_path,
                fallback=None,
                trusted_handle=stderr_handle,
                expected_identity=stderr_identity,
            )
            _remove_incomplete_runtime_outputs(context, invocation)
            raise

        stdout_text = _retain_bounded_runtime_diagnostic(
            invocation.stdout_path,
            fallback=completed.stdout,
            trusted_handle=stdout_handle,
            expected_identity=stdout_identity,
        )
        stderr_text = _retain_bounded_runtime_diagnostic(
            invocation.stderr_path,
            fallback=completed.stderr,
            trusted_handle=stderr_handle,
            expected_identity=stderr_identity,
        )
    if completed.returncode != 0:
        detail = _output_tail(stderr_text.strip() or stdout_text.strip() or "no subprocess output")
        raise RuntimeError(
            "node runtime `{}` failed for {} with return code {}: {}".format(
                runtime_name,
                failure_target,
                completed.returncode,
                detail,
            )
        )
    if not invocation.result_path.exists():
        stage = "node_adapter_output_validation: " if context.output_plan is not None else ""
        raise RuntimeError(
            "{}node runtime `{}` finished for {} without writing result.json".format(
                stage,
                runtime_name,
                failure_target,
            )
        )
    try:
        if context.output_plan is None:
            result_text = invocation.result_path.read_text(encoding="utf-8")
        else:
            result_body = _read_bounded_regular_file(invocation.result_path)
            if len(result_body) > _MAX_ISOLATED_RESULT_BYTES:
                raise RuntimeError("artifact result descriptor exceeds its size limit")
            result_text = result_body.decode("utf-8")
        payload = (
            json.loads(result_text)
            if context.output_plan is None
            else json.loads(result_text, object_pairs_hook=_closed_json_object)
        )
    except (UnicodeDecodeError, ValueError, OSError, RuntimeError) as exc:
        stage = "node_adapter_output_validation: " if context.output_plan is not None else ""
        raise RuntimeError(
            "{}node runtime `{}` wrote invalid result.json for {}: {}".format(
                stage,
                runtime_name,
                failure_target,
                exc,
            )
        ) from exc
    if context.output_plan is not None:
        output = _promote_isolated_node_output(
            context,
            invocation,
            payload,
            runtime_name=runtime_name,
            failure_target=failure_target,
        )
        return {context.output_port.name: output}
    return {context.output_port.name: payload.get("result")}


def _remove_incomplete_runtime_outputs(context: NodeRuntimeContext, invocation: _SplFreeInvocation) -> None:
    if context.output_plan is None:
        return
    for temporary_output in invocation.work_dir.glob("runtime-output-tmp*"):
        shutil.rmtree(temporary_output, ignore_errors=True)
    shutil.rmtree(invocation.work_dir / "runtime-outputs", ignore_errors=True)


def _closed_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("artifact result descriptor contains a duplicate field")
        value[key] = item
    return value


def _promote_isolated_node_output(
    context: NodeRuntimeContext,
    invocation: _SplFreeInvocation,
    payload: Any,
    *,
    runtime_name: str,
    failure_target: str,
) -> ArtifactOutput:
    """Fail closed while promoting one worker artifact into Run-owned storage."""

    if context.output_plan is not None and len(context.output_plan.variants()) > 1:
        return _promote_isolated_node_multi_output(
            context,
            invocation,
            payload,
            runtime_name=runtime_name,
            failure_target=failure_target,
        )

    prefix = "node_adapter_output_validation: node runtime `{}` produced an invalid artifact output for {}: ".format(
        runtime_name,
        failure_target,
    )
    if not isinstance(payload, Mapping) or set(payload) != {
        "result",
        "artifacts",
        "runtime_port_adapter_output",
    }:
        raise RuntimeError(prefix + "result document does not match the closed artifact schema")
    if payload["result"] is not None or payload["artifacts"] != {}:
        raise RuntimeError(prefix + "artifact result cannot include inline or undeclared legacy outputs")
    try:
        descriptor = m_runtime_port_adapters.normalize_isolated_node_output_result(
            payload["runtime_port_adapter_output"]
        )
    except m_runtime_port_adapters.RuntimePortAdapterContractError as exc:
        raise RuntimeError(prefix + exc.safe_message) from None
    prepared = context.isolated_inputs
    if prepared is None:
        raise RuntimeError(prefix + "pre-execution output plan is missing")
    output_bindings = [binding for binding in prepared.document["bindings"] if binding["direction"] == "output"]
    if len(output_bindings) != 1:
        raise RuntimeError(prefix + "pre-execution output plan is not singular")
    output_plan = context.output_plan
    if output_plan is None:
        raise RuntimeError(prefix + "pre-execution output plan is missing")
    binding = output_bindings[0]
    adapter = binding["adapter"]
    expected_identity = (
        binding["port"],
        adapter["id"],
        adapter["key"],
        adapter["format_tag"],
    )
    observed_identity = (
        descriptor["port"],
        descriptor["adapter_id"],
        descriptor["adapter_key"],
        descriptor["format_tag"],
    )
    if observed_identity != expected_identity:
        raise RuntimeError(prefix + "result descriptor does not match the exact planned adapter identity")
    output_dir = invocation.work_dir / "runtime-outputs"
    try:
        output_dir_identity = output_dir.lstat()
        if not stat.S_ISDIR(output_dir_identity.st_mode) or _is_reparse_point(output_dir_identity):
            raise RuntimeError
        source_path = invocation.work_dir / descriptor["relative_path"]
        if source_path.parent != output_dir:
            raise RuntimeError
        source_identity = source_path.lstat()
        if not stat.S_ISREG(source_identity.st_mode) or source_identity.st_nlink != 1:
            raise RuntimeError
        body = _read_bounded_regular_file(source_path)
        current_source = source_path.lstat()
        if current_source.st_nlink != 1 or (current_source.st_dev, current_source.st_ino) != (
            source_identity.st_dev,
            source_identity.st_ino,
        ):
            raise RuntimeError
        current_output_dir = output_dir.lstat()
        if (
            not stat.S_ISDIR(current_output_dir.st_mode)
            or _is_reparse_point(current_output_dir)
            or (current_output_dir.st_dev, current_output_dir.st_ino)
            != (output_dir_identity.st_dev, output_dir_identity.st_ino)
        ):
            raise RuntimeError
    except (OSError, RuntimeError):
        raise RuntimeError(
            prefix + "declared output is not one stable regular file in its dedicated namespace"
        ) from None
    if len(body) != descriptor["size"] or hashlib.sha256(body).hexdigest() != descriptor["sha256"]:
        raise RuntimeError(prefix + "declared output size or checksum does not match its file")
    artifacts_dir = output_plan.artifacts_dir
    artifacts_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = artifacts_dir / "artifact-{}".format(uuid4().hex)
    temporary = artifacts_dir / ".{}.tmp".format(target.name)
    try:
        _write_private_runtime_file(temporary, body)
        os.replace(temporary, target)
        try:
            target.chmod(0o600)
        except OSError:
            pass
        promoted = _read_bounded_regular_file(target)
    except (OSError, RuntimeError):
        temporary.unlink(missing_ok=True)
        target.unlink(missing_ok=True)
        raise RuntimeError(prefix + "artifact could not be atomically promoted into Run-owned storage") from None
    if promoted != body:
        target.unlink(missing_ok=True)
        raise RuntimeError(prefix + "promoted artifact failed final integrity verification")
    ref = ArtifactRef(
        key=descriptor["adapter_key"],
        uri=str(target),
        sha256=descriptor["sha256"],
        size=descriptor["size"],
        tag=descriptor["format_tag"],
    )
    return ArtifactOutput(
        ref=ref,
        save_adapter=output_plan.save_adapter,
        resolution_source=output_plan.resolution_source,
        variant_id=output_plan.variant_id,
    )


def _promote_isolated_node_multi_output(
    context: NodeRuntimeContext,
    invocation: _SplFreeInvocation,
    payload: Any,
    *,
    runtime_name: str,
    failure_target: str,
) -> ArtifactOutput:
    """Fail closed while promoting every exact schema-v4 output variant."""

    prefix = "node_adapter_output_validation: node runtime `{}` produced invalid artifact outputs for {}: ".format(
        runtime_name,
        failure_target,
    )
    if not isinstance(payload, Mapping) or set(payload) != {
        "result",
        "artifacts",
        "runtime_port_adapter_outputs",
    }:
        raise RuntimeError(prefix + "result document does not match the closed multi-output artifact schema")
    raw_descriptors = payload["runtime_port_adapter_outputs"]
    if payload["result"] is not None or payload["artifacts"] != {} or not isinstance(raw_descriptors, list):
        raise RuntimeError(prefix + "artifact result cannot include inline or undeclared legacy outputs")
    output_plan = context.output_plan
    prepared = context.isolated_inputs
    if output_plan is None or prepared is None:
        raise RuntimeError(prefix + "pre-execution output plan is missing")
    plan_variants = output_plan.variants()
    if len(raw_descriptors) != len(plan_variants):
        raise RuntimeError(prefix + "result descriptor count does not match the planned variants")
    try:
        descriptors = [
            m_runtime_port_adapters.normalize_isolated_node_multi_output_result(item) for item in raw_descriptors
        ]
    except m_runtime_port_adapters.RuntimePortAdapterContractError as exc:
        raise RuntimeError(prefix + exc.safe_message) from None
    if sum(int(item["size"]) for item in descriptors) > m_runtime_port_adapters.MAX_RUNTIME_OUTPUT_TOTAL_BYTES:
        raise RuntimeError(prefix + "result descriptors exceed the aggregate artifact output size limit")
    descriptor_ids = [str(item["variant_id"]) for item in descriptors]
    if len(set(descriptor_ids)) != len(descriptor_ids):
        raise RuntimeError(prefix + "result descriptor variant identity is duplicated")
    bindings = {
        str(binding["variant_id"]): binding
        for binding in prepared.document["bindings"]
        if binding["direction"] == "output"
    }
    planned = {variant.variant_id: variant for variant in plan_variants}
    if set(bindings) != set(planned) or set(descriptor_ids) != set(planned):
        raise RuntimeError(prefix + "result variants do not match the pre-execution plan")
    descriptor_by_id = {str(item["variant_id"]): item for item in descriptors}
    for variant_id, binding in bindings.items():
        descriptor = descriptor_by_id[variant_id]
        adapter = binding["adapter"]
        expected = (
            binding["port"],
            variant_id,
            adapter["id"],
            adapter["key"],
            adapter["format_tag"],
        )
        observed = (
            descriptor["port"],
            descriptor["variant_id"],
            descriptor["adapter_id"],
            descriptor["adapter_key"],
            descriptor["format_tag"],
        )
        if observed != expected:
            raise RuntimeError(prefix + "result descriptor does not match its exact planned adapter identity")

    output_dir = invocation.work_dir / "runtime-outputs"
    bodies: dict[str, bytes] = {}
    try:
        output_dir_identity = output_dir.lstat()
        if not stat.S_ISDIR(output_dir_identity.st_mode) or _is_reparse_point(output_dir_identity):
            raise RuntimeError
        expected_paths = {invocation.work_dir / item["relative_path"] for item in descriptors}
        if (
            len(expected_paths) != len(descriptors)
            or any(path.parent != output_dir for path in expected_paths)
            or set(output_dir.iterdir()) != expected_paths
        ):
            raise RuntimeError
        for descriptor in descriptors:
            variant_id = str(descriptor["variant_id"])
            source_path = invocation.work_dir / descriptor["relative_path"]
            source_identity = source_path.lstat()
            if not stat.S_ISREG(source_identity.st_mode) or source_identity.st_nlink != 1:
                raise RuntimeError
            body = _read_bounded_regular_file(source_path)
            current_source = source_path.lstat()
            if (
                current_source.st_nlink != 1
                or (current_source.st_dev, current_source.st_ino) != (source_identity.st_dev, source_identity.st_ino)
                or len(body) != descriptor["size"]
                or hashlib.sha256(body).hexdigest() != descriptor["sha256"]
            ):
                raise RuntimeError
            bodies[variant_id] = body
        current_output_dir = output_dir.lstat()
        if (
            not stat.S_ISDIR(current_output_dir.st_mode)
            or _is_reparse_point(current_output_dir)
            or (current_output_dir.st_dev, current_output_dir.st_ino)
            != (output_dir_identity.st_dev, output_dir_identity.st_ino)
        ):
            raise RuntimeError
    except (OSError, RuntimeError):
        raise RuntimeError(
            prefix + "declared outputs are not exact stable regular files in their dedicated namespace"
        ) from None

    artifacts_dir = output_plan.artifacts_dir
    artifacts_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    promoted_paths: list[Path] = []
    temporary_paths: list[Path] = []
    promoted_variants: list[ArtifactOutputVariant] = []
    try:
        for variant in plan_variants:
            descriptor = descriptor_by_id[variant.variant_id]
            body = bodies[variant.variant_id]
            target = artifacts_dir / "artifact-{}".format(uuid4().hex)
            temporary = artifacts_dir / ".{}.tmp".format(target.name)
            temporary_paths.append(temporary)
            _write_private_runtime_file(temporary, body)
            os.replace(temporary, target)
            temporary_paths.remove(temporary)
            promoted_paths.append(target)
            try:
                target.chmod(0o600)
            except OSError:
                pass
            if _read_bounded_regular_file(target) != body:
                raise RuntimeError
            promoted_variants.append(
                ArtifactOutputVariant(
                    variant_id=variant.variant_id,
                    ref=ArtifactRef(
                        key=str(descriptor["adapter_key"]),
                        uri=str(target),
                        sha256=str(descriptor["sha256"]),
                        size=int(descriptor["size"]),
                        tag=cast(str, descriptor["format_tag"]),
                    ),
                    save_adapter=variant.save_adapter,
                    resolution_source=variant.resolution_source,
                )
            )
    except (OSError, RuntimeError):
        for path in temporary_paths:
            path.unlink(missing_ok=True)
        for path in promoted_paths:
            path.unlink(missing_ok=True)
        raise RuntimeError(prefix + "artifacts could not be atomically promoted into Run-owned storage") from None
    primary, *additional = promoted_variants
    return ArtifactOutput(
        ref=primary.ref,
        save_adapter=primary.save_adapter,
        resolution_source=primary.resolution_source,
        variant_id=primary.variant_id,
        additional_variants=tuple(additional),
    )


def runtime_manifest_record(
    resolution: NodeRuntimeResolution,
    environment: PreparedNodeEnvironment,
) -> dict[str, Any]:
    """Return a manifest record for a resolved node runtime."""

    resolved: dict[str, Any] = {}
    if environment.python_path is not None:
        resolved["python"] = str(environment.python_path)
    metadata = dict(environment.metadata)
    image_tag = metadata.get("image_tag")
    if isinstance(image_tag, str) and image_tag:
        resolved["image_tag"] = image_tag
    return {
        "name": resolution.name,
        "source": str(resolution.source),
        "config_hash": metadata.get("spec_hash"),
        "resolved": resolved,
    }


def _validate_runtime_name(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("node runtime name must be a non-empty string")
    return value


def _runtime_spec(runtime_name: str, context: NodeRuntimeContext) -> dict[str, Any]:
    return {
        "node_runtime": runtime_name,
        "runtime_config": dict(context.runtime_config),
        "distributions": list(context.environment_spec),
    }


def _explicit_docker_image(runtime_config: Mapping[str, Any]) -> str | None:
    docker_config = runtime_config.get("docker")
    if docker_config is None:
        return None
    if not isinstance(docker_config, Mapping):
        raise ValueError('runtime_config["docker"] must be a mapping')
    image = docker_config.get("image")
    if image is None:
        return None
    image_tag = str(image)
    if not image_tag:
        raise ValueError('runtime_config["docker"]["image"] must be a non-empty string')
    return image_tag


def _node_docker_runtime_options(runtime_config: Mapping[str, Any]) -> dict[str, Any]:
    from spl.daemon.runtime_config import normalize_docker_runtime_options

    docker_config = runtime_config.get("docker")
    if docker_config is None:
        return normalize_docker_runtime_options({})
    if not isinstance(docker_config, Mapping):
        raise ValueError('runtime_config["docker"] must be a mapping')
    return normalize_docker_runtime_options(docker_config)


def _raise_if_nested_object_docker() -> None:
    if os.environ.get(_SPL_OBJECT_RUNTIME_BACKEND_ENV) == DOCKER_NODE_RUNTIME or (
        os.environ.get(_SPL_OBJECT_DOCKER_WORKER_ENV) == "1"
    ):
        raise RuntimeError(
            "nested docker runtimes are not supported; keep the object runtime on venv or drop the node tag"
        )


def _docker_network_args(runtime_config: dict[str, Any]) -> list[str]:
    from spl.daemon.docker_pool import docker_node_network_args

    return docker_node_network_args(runtime_config)


def _docker_label_args() -> list[str]:
    from spl.daemon.docker_pool import worker_container_label_args_from_env

    return worker_container_label_args_from_env(kind="node")


def _docker_hardening_args(runtime_config: dict[str, Any]) -> list[str]:
    from spl.daemon.docker_pool import docker_hardening_args

    return docker_hardening_args(runtime_config)


def _docker_env_args(runtime_config: dict[str, Any]) -> list[str]:
    from spl.daemon.docker_pool import docker_env_args

    return docker_env_args(runtime_config)


def _docker_user_args() -> list[str]:
    from spl.daemon.docker_pool import docker_user_args

    return docker_user_args()


def _docker_container_name(context: NodeRuntimeContext) -> str:
    parent = context.work_dir.parent
    run_part = parent.parent.name if parent.name == "node-runtimes" else parent.name
    run_token = _docker_name_token(run_part)[:32]
    uuid_token = str(context.node.uuid).replace("-", "")[:8]
    random_token = uuid4().hex[:6]
    instance_value = os.environ.get("SPL_DAEMON_INSTANCE_ID")
    if instance_value:
        instance_token = _docker_name_token(instance_value)[:8]
        suffix = "{}-{}".format(uuid_token, random_token)
        prefix = "spl-node-{}-".format(instance_token)
        run_token = run_token[: max(1, 63 - len(prefix) - len(suffix) - 1)]
        container_name = "{}{}-{}".format(prefix, run_token, suffix)
    else:
        container_name = "spl-node-{}-{}-{}".format(run_token, uuid_token, random_token)
    if len(container_name) > 63:
        raise RuntimeError("docker node container name exceeds Docker's 63-character limit")
    return container_name


def _docker_name_token(value: str) -> str:
    token = "".join(char if char.isalnum() or char in "_.-" else "-" for char in value).strip("._-")
    if not token:
        return "run"
    if not token[0].isalnum():
        return "run-{}".format(token)
    return token


def _kill_docker_container(container_name: str) -> None:
    ownership = _docker_cleanup_target_is_owned(container_name, expected_name=container_name)
    if ownership is None:
        return
    if not ownership:
        LOGGER.warning(
            "refusing to remove timed-out Docker node container `%s`: invocation identity differs",
            container_name,
        )
        return
    kill_error: str | None = None
    try:
        completed = subprocess.run(
            ["docker", "kill", container_name],
            text=True,
            capture_output=True,
            timeout=15,
            check=False,
        )
    except Exception as exc:
        kill_error = str(exc)
    else:
        detail = (completed.stderr or completed.stdout or "").strip()
        if completed.returncode != 0 and "no such container" not in detail.casefold():
            kill_error = detail or str(completed.returncode)
    if kill_error is not None:
        LOGGER.warning("docker kill `%s` failed: %s", container_name, kill_error)

    try:
        removed = subprocess.run(
            ["docker", "rm", "-f", container_name],
            text=True,
            capture_output=True,
            timeout=15,
            check=False,
        )
    except Exception as exc:
        LOGGER.warning("failed to remove timed-out Docker node container `%s`: %s", container_name, exc)
        return
    detail = (removed.stderr or removed.stdout or "").strip()
    if removed.returncode != 0 and "no such container" not in detail.casefold():
        LOGGER.warning("docker rm `%s` failed: %s", container_name, detail or removed.returncode)


def _docker_cleanup_target_is_owned(
    container_target: str,
    *,
    expected_name: str | None = None,
) -> bool | None:
    from spl.daemon.docker_pool import worker_container_labels_from_env

    if expected_name is None or container_target != expected_name:
        return False
    try:
        expected = worker_container_labels_from_env(kind="node")
    except (RuntimeError, ValueError):
        LOGGER.warning("refusing Docker node cleanup because daemon ownership labels are invalid")
        return False
    try:
        inspected = subprocess.run(
            ["docker", "inspect", container_target],
            text=True,
            capture_output=True,
            timeout=15,
            check=False,
        )
    except Exception as exc:
        LOGGER.warning("failed to inspect timed-out Docker node container `%s`: %s", container_target, exc)
        return False
    if inspected.returncode != 0:
        detail = (inspected.stderr or inspected.stdout or "").strip()
        if "no such" in detail.casefold():
            return None
        LOGGER.warning("docker inspect `%s` failed: %s", container_target, detail or inspected.returncode)
        return False
    try:
        records = json.loads(inspected.stdout or "[]")
    except json.JSONDecodeError:
        return False
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], Mapping):
        return False
    record = records[0]
    actual_name = record.get("Name")
    config = record.get("Config")
    if actual_name != "/{}".format(expected_name) or not isinstance(config, Mapping):
        return False
    labels = config.get("Labels")
    if labels is None:
        labels = {}
    return isinstance(labels, Mapping) and all(labels.get(key) == value for key, value in expected.items())


def _spl_free_runner_args(
    *,
    module_path: str,
    module_name: str,
    entrypoint: str,
    input_path: str,
    result_path: str,
    artifacts_dir: str,
    env_spec_path: str,
) -> list[str]:
    return [
        "--module",
        module_path,
        "--module-name",
        module_name,
        "--entrypoint",
        entrypoint,
        "--input",
        input_path,
        "--result",
        result_path,
        "--artifacts-dir",
        artifacts_dir,
        "--env-spec",
        env_spec_path,
    ]


def _subprocess_input_json(
    context: NodeRuntimeContext,
    *,
    runtime_name: str = VENV_SUBPROCESS_NODE_RUNTIME,
) -> str:
    kwargs = []
    for port, transport in sorted(context.inputs.items(), key=lambda item: item[0].name):
        if isinstance(transport, ArtifactInput):
            raise RuntimeError(
                "node `{}` port `{}` has an unprepared artifact input for {}".format(
                    context.node_label,
                    port.name,
                    runtime_name,
                )
            )
        value = transport.value if isinstance(transport, InlineInput) else transport
        try:
            encoded_value = json_dumps(value, separators=None)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "node `{}` port `{}` cannot run with {} because input value type `{}` is not JSON-serializable; "
                "select a registered artifact adapter with `.as_format(...)` (or a run-level adapter override), "
                "execute this node with the native runtime, or insert a converter node "
                "(cookbook: Converter Nodes For Adapter Tags).".format(
                    context.node_label,
                    port.name,
                    runtime_name,
                    _type_name(value),
                )
            ) from exc
        kwargs.append("{}:{}".format(json_dumps(port.name, sort_keys=False, separators=None), encoded_value))
    return '{{"args":[],"kwargs":{{{}}}}}'.format(",".join(kwargs))


def _type_name(value: Any) -> str:
    typ = type(value)
    return "{}.{}".format(typ.__module__, typ.__qualname__)


def _open_runtime_diagnostic(path: Path) -> tuple[TextIO, tuple[int, int]]:
    """Create one private diagnostic and keep its trusted descriptor open."""

    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    identity: tuple[int, int] | None = None
    try:
        path.unlink(missing_ok=True)
        descriptor = os.open(path, flags, 0o600)
        opened = os.fstat(descriptor)
        identity = (opened.st_dev, opened.st_ino)
        if not _runtime_diagnostic_stat_is_private_regular(opened):
            raise RuntimeError
        current = path.lstat()
        if (
            not _runtime_diagnostic_stat_is_private_regular(current)
            or (
                current.st_dev,
                current.st_ino,
            )
            != identity
        ):
            raise RuntimeError
        return os.fdopen(descriptor, "w+", encoding="utf-8"), identity
    except (OSError, RuntimeError):
        if descriptor >= 0:
            os.close(descriptor)
        if identity is not None:
            try:
                current = path.lstat()
                if (current.st_dev, current.st_ino) == identity:
                    path.unlink()
            except OSError:
                pass
        raise RuntimeError("node runtime diagnostic file could not be prepared") from None


def _runtime_diagnostic_stat_is_private_regular(value: os.stat_result) -> bool:
    return stat.S_ISREG(value.st_mode) and value.st_nlink == 1 and not _is_reparse_point(value)


def _retain_bounded_runtime_diagnostic(
    path: Path,
    *,
    fallback: str | bytes | None,
    trusted_handle: TextIO,
    expected_identity: tuple[int, int],
) -> str:
    """Retain a byte-bounded UTF-8 tail through the pre-execution descriptor."""

    fallback_body = _bounded_runtime_diagnostic_bytes(fallback)
    try:
        trusted_handle.flush()
        descriptor = trusted_handle.fileno()
        before = os.fstat(descriptor)
        current = path.lstat()
        if not _runtime_diagnostic_identity_is_valid(before, current, expected_identity):
            raise RuntimeError
        retained_size = min(before.st_size, _MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES)
        os.lseek(descriptor, before.st_size - retained_size, os.SEEK_SET)
        chunks: list[bytes] = []
        remaining = retained_size
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        current_after = path.lstat()
        if (
            remaining != 0
            or before.st_size != after.st_size
            or not _runtime_diagnostic_identity_is_valid(after, current_after, expected_identity)
        ):
            raise RuntimeError
        body = _bounded_runtime_diagnostic_bytes(b"".join(chunks))
        if not body:
            body = fallback_body
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.ftruncate(descriptor, 0)
        _write_all(descriptor, body)
        os.fsync(descriptor)
        final = os.fstat(descriptor)
        current_final = path.lstat()
        if final.st_size != len(body) or not _runtime_diagnostic_identity_is_valid(
            final, current_final, expected_identity
        ):
            raise RuntimeError
    except (OSError, RuntimeError, ValueError):
        body = fallback_body
        _replace_untrusted_runtime_diagnostic(path, body)
    return body.decode("utf-8")


def _runtime_diagnostic_identity_is_valid(
    opened: os.stat_result,
    current: os.stat_result,
    expected_identity: tuple[int, int],
) -> bool:
    return (
        _runtime_diagnostic_stat_is_private_regular(opened)
        and _runtime_diagnostic_stat_is_private_regular(current)
        and (opened.st_dev, opened.st_ino) == expected_identity
        and (current.st_dev, current.st_ino) == expected_identity
    )


def _bounded_runtime_diagnostic_bytes(value: str | bytes | None) -> bytes:
    if value is None:
        raw = b""
    elif isinstance(value, bytes):
        raw = value[-_MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES:]
    else:
        raw = value[-_MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES:].encode("utf-8", errors="replace")
    encoded = (
        raw[-_MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES:]
        .decode(
            "utf-8",
            errors="replace",
        )
        .encode("utf-8")
    )
    if len(encoded) > _MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES:
        encoded = encoded[-_MAX_RETAINED_RUNTIME_DIAGNOSTIC_BYTES:]
        encoded = encoded.decode("utf-8", errors="ignore").encode("utf-8")
    return encoded


def _write_all(descriptor: int, body: bytes) -> None:
    offset = 0
    while offset < len(body):
        offset += os.write(descriptor, body[offset:])


def _replace_untrusted_runtime_diagnostic(path: Path, body: bytes) -> None:
    """Unlink an untrusted entry without following it, then create a private replacement."""

    try:
        path.unlink(missing_ok=True)
    except OSError:
        return
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    identity: tuple[int, int] | None = None
    try:
        descriptor = os.open(path, flags, 0o600)
        opened = os.fstat(descriptor)
        if not _runtime_diagnostic_stat_is_private_regular(opened):
            raise RuntimeError
        identity = (opened.st_dev, opened.st_ino)
        _write_all(descriptor, body)
        os.fsync(descriptor)
        current = path.lstat()
        if not _runtime_diagnostic_identity_is_valid(os.fstat(descriptor), current, identity) or current.st_size != len(
            body
        ):
            raise RuntimeError
    except (OSError, RuntimeError):
        if identity is not None:
            try:
                current = path.lstat()
                if (current.st_dev, current.st_ino) == identity:
                    path.unlink()
            except OSError:
                pass
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _format_timeout_seconds(timeout_seconds: float | None) -> str:
    if timeout_seconds is None:
        return "unknown"
    return "{:g}".format(timeout_seconds)


def _output_tail(value: str, *, limit: int = 4000) -> str:
    if len(value) <= limit:
        return value
    return value[-limit:]


class _SourceRecoveryError(Exception):
    pass


def _generated_node_module_text(
    node: NodeFunction,
    node_label: str,
    *,
    runtime_name: str = VENV_SUBPROCESS_NODE_RUNTIME,
) -> str:
    from spl.daemon.spl_free_generator import filter_spl_runtime_scaffolding, unsupported_stage1_reason

    module, failures = _recover_node_module(node)
    if module is None:
        details = "; ".join(failures) if failures else "no source candidate was available"
        raise RuntimeError(
            "node `{}` function `{}` source is not recoverable; {} requires a source-visible or IR-parsable "
            "function node ({})".format(node_label, node.func.__name__, runtime_name, details)
        )

    reason = unsupported_stage1_reason(module)
    if reason is not None:
        raise RuntimeError(
            "{} runtime cannot execute node `{}` function `{}` via spl-free runner: {}".format(
                runtime_name, node_label, node.func.__name__, reason
            )
        )
    filtered = filter_spl_runtime_scaffolding(module)
    prefix = "from __future__ import annotations\n\n" if _module_requires_postponed_annotations(filtered) else ""
    return prefix + ast.unparse(filtered) + "\n"


def _module_requires_postponed_annotations(module: ast.Module) -> bool:
    runtime_builtin_names = vars(builtins).keys()
    annotations: list[ast.expr] = []
    for function in (item for item in module.body if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef)):
        annotations.extend(arg.annotation for arg in function.args.args if arg.annotation is not None)
        annotations.extend(arg.annotation for arg in function.args.kwonlyargs if arg.annotation is not None)
        if function.args.vararg is not None and function.args.vararg.annotation is not None:
            annotations.append(function.args.vararg.annotation)
        if function.args.kwarg is not None and function.args.kwarg.annotation is not None:
            annotations.append(function.args.kwarg.annotation)
        if function.returns is not None:
            annotations.append(function.returns)
    return any(
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        or isinstance(node, ast.Name)
        and node.id not in runtime_builtin_names
        for annotation in annotations
        for node in ast.walk(annotation)
    )


def _recover_node_module(node: NodeFunction) -> tuple[ast.Module | None, list[str]]:
    failures: list[str] = []
    try:
        return _module_from_source_text(inspect.getsource(node.func), node.func.__name__), failures
    except (OSError, SyntaxError, TypeError, _SourceRecoveryError) as exc:
        failures.append("inspect.getsource: {}".format(exc))

    try:
        return _module_from_dfunction(_dfunction_from_ir(node), node.func.__name__), failures
    except (KeyError, SyntaxError, TypeError, ValueError, _SourceRecoveryError) as exc:
        failures.append("ir_parse: {}".format(exc))
    return None, failures


def _module_from_source_text(source: str, func_name: str) -> ast.Module:
    module = ast.parse(textwrap.dedent(source))
    _validate_top_level_function(module, func_name)
    return module


def _dfunction_from_ir(node: NodeFunction) -> DFunction:
    parsed = ir_parse(node.func)
    if isinstance(parsed, _branch):
        root = parsed.mk_root()
        if isinstance(root, DFunction):
            return root
    if isinstance(parsed, DFunction):
        return parsed
    raise ValueError("IR did not produce DFunction for `{}`".format(node.func.__name__))


def _module_from_dfunction(dfunction: DFunction, func_name: str) -> ast.Module:
    outputs = dfunction.outputs or []
    returns = None
    if outputs and outputs[0].typ_ is not None:
        returns = ast.parse(outputs[0].typ_, mode="eval").body
    function_def = ast.FunctionDef(
        name=dfunction.name,
        args=ast.arguments(
            posonlyargs=[],
            args=[ast.arg(arg=port.name, annotation=_annotation_expr(port.typ_)) for port in dfunction.inputs],
            vararg=None,
            kwonlyargs=[],
            kw_defaults=[],
            kwarg=None,
            defaults=[
                ast.parse(port.default, mode="eval").body for port in dfunction.inputs if port.default is not None
            ],
        ),
        body=ast.parse(textwrap.dedent(dfunction.body)).body,
        decorator_list=[],
        returns=returns,
    )
    module = ast.fix_missing_locations(ast.Module(body=[function_def], type_ignores=[]))
    _validate_top_level_function(module, func_name)
    return module


def _annotation_expr(value: str | None) -> ast.expr | None:
    if value is None:
        return None
    return ast.parse(value, mode="eval").body


def _validate_top_level_function(module: ast.Module, func_name: str) -> None:
    if any(isinstance(stmt, ast.FunctionDef) and stmt.name == func_name for stmt in module.body):
        return
    raise _SourceRecoveryError("top-level function `{}` is not present".format(func_name))


def _write_generated_node_module(module_path: Path, module_text: str) -> None:
    module_path.write_text(module_text, encoding="utf-8")


def _copy_spl_free_runner(runner_path: Path) -> None:
    import spl.daemon.spl_free_runner as spl_free_runner

    source = Path(str(spl_free_runner.__file__))
    shutil.copy2(source, runner_path)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json_dumps(value, indent=2, separators=None), encoding="utf-8")


def _subprocess_env_without_project_pythonpath() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    return env
