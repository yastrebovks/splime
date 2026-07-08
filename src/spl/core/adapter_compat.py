"""Static adapter compatibility checks and local adapter probes."""

from __future__ import annotations

import json
import tempfile
import typing
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from spl.core.entities.adapter import (
    BUILTIN_JSON_ADAPTER,
    JSON_ADAPTER_FORMAT,
    JSON_NATIVE_TYPES,
    DAdapter,
    DLoadAdapter,
    DSaveAdapter,
    RuntimeAdapter,
    adapter_identity,
)
from spl.core.entities.function import DFunction
from spl.core.entities.node import (
    DFormattedOutputRef,
    DNodeInputRef,
    DNodeOutputRef,
    FormattedOutputRef,
    InputPort,
    Node,
    NodeOutputRef,
    OutputPort,
)
from spl.core.entities.node_function import DNodeFunction, NodeFunction
from spl.core.entities.pipeline import AdapterResolution, Pipeline
from spl.core.ir.utils import SPLSafeLoader


class AdapterCompatibilityWarning(UserWarning):
    """Warning emitted when an edge save tag is not accepted by its load half."""


@dataclass(frozen=True)
class AdapterCompatibilityIssue:
    """A static save-tag/load-tags mismatch on one pipeline edge."""

    edge: str
    save_tag: str
    accepted_tags: tuple[str, ...]
    save_adapter: str
    load_adapter: str

    @property
    def detail(self) -> str:
        """Return a human-readable explanation for reports and warnings."""

        return (
            "adapter tag mismatch on edge {}: save tag `{}` from `{}` is not accepted "
            "by load adapter `{}` (accepted tags: {})"
        ).format(
            self.edge,
            self.save_tag,
            self.save_adapter,
            self.load_adapter,
            ", ".join(self.accepted_tags) or "<none>",
        )

    @property
    def hint(self) -> str:
        """Return the standard repair hint for this mismatch."""

        return (
            "use `.as_format()`, a run-level adapter override, or an explicit converter node "
            "(cookbook: Converter Nodes For Adapter Tags)"
        )

    @property
    def warning_message(self) -> str:
        """Return the complete warning message."""

        return "{}; hint: {}".format(self.detail, self.hint)


@dataclass(frozen=True)
class AdapterProbeFailure:
    """One failed adapter example probe."""

    adapter: str
    reason: str


@dataclass(frozen=True)
class AdapterProbeReport:
    """Summary of local save/load adapter probes."""

    probed: int
    skipped: int
    failures: tuple[AdapterProbeFailure, ...] = ()


@dataclass(frozen=True)
class _RuntimeEdgeBinding:
    edge: str
    save: AdapterResolution
    load: AdapterResolution


@dataclass(frozen=True)
class _StaticHalf:
    key: str
    tag: str | None
    accepted_tags: tuple[str, ...] | None


_BUILTIN_TYPES: dict[str, type[Any]] = {
    "str": str,
    "builtins.str": str,
    "int": int,
    "builtins.int": int,
    "float": float,
    "builtins.float": float,
    "bool": bool,
    "builtins.bool": bool,
    "dict": dict,
    "builtins.dict": dict,
    "list": list,
    "builtins.list": list,
}
_AdapterCompatibilityIssueKey = tuple[str, str, tuple[str, ...], str, str]
_WARNED_PIPELINE_ISSUES: set[_AdapterCompatibilityIssueKey] = set()


def find_pipeline_adapter_compatibility_issues(pipeline: Pipeline) -> tuple[AdapterCompatibilityIssue, ...]:
    """Return static adapter tag mismatches for a runtime pipeline."""

    issues = []
    for binding in _iter_runtime_edge_bindings(pipeline):
        save_tag = binding.save.adapter.tag
        accepted_tags = tuple(sorted(binding.load.adapter.accepted_tags))
        if save_tag not in binding.load.adapter.accepted_tags:
            issues.append(
                AdapterCompatibilityIssue(
                    edge=binding.edge,
                    save_tag=save_tag,
                    accepted_tags=accepted_tags,
                    save_adapter=binding.save.adapter.key,
                    load_adapter=binding.load.adapter.key,
                )
            )
    return tuple(issues)


def warn_pipeline_adapter_compatibility(pipeline: Pipeline) -> None:
    """Warn once per unique static adapter mismatch in this Python process.

    The deduplication key is the issue content: edge label, save tag, accepted
    load tags, save adapter, and load adapter. Discovery APIs such as
    ``find_pipeline_adapter_compatibility_issues`` and doctor checks do not use
    this warning set and always return the full current issue list.
    """

    for issue in find_pipeline_adapter_compatibility_issues(pipeline):
        key = _adapter_compatibility_issue_key(issue)
        if key in _WARNED_PIPELINE_ISSUES:
            continue
        _WARNED_PIPELINE_ISSUES.add(key)
        warnings.warn(issue.warning_message, AdapterCompatibilityWarning, stacklevel=3)


def _adapter_compatibility_issue_key(issue: AdapterCompatibilityIssue) -> _AdapterCompatibilityIssueKey:
    return (issue.edge, issue.save_tag, issue.accepted_tags, issue.save_adapter, issue.load_adapter)


def _reset_adapter_compatibility_warnings() -> None:
    """Clear process-local adapter compatibility warning deduplication state."""

    _WARNED_PIPELINE_ISSUES.clear()


def find_yaml_adapter_compatibility_issues(yaml_text: str, entrypoint: str) -> tuple[AdapterCompatibilityIssue, ...]:
    """Return static adapter tag mismatches for a serialized pipeline."""

    documents = _load_documents(yaml_text)
    functions = {
        item.name: item
        for root, dependencies in documents
        for item in (root, *dependencies)
        if isinstance(item, DFunction)
    }
    pipeline = _find_dpipeline(documents, entrypoint)
    if pipeline is None:
        return ()
    return _find_dpipeline_adapter_compatibility_issues(pipeline, functions)


def warn_yaml_adapter_compatibility(yaml_text: str, entrypoint: str) -> None:
    """Warn about serialized pipeline adapter mismatches during registration."""

    for issue in find_yaml_adapter_compatibility_issues(yaml_text, entrypoint):
        warnings.warn(issue.warning_message, AdapterCompatibilityWarning, stacklevel=3)


def probe_pipeline_adapters(pipeline: Pipeline) -> AdapterProbeReport:
    """Run local save/load probes for adapters with an ``example`` callable."""

    probed = 0
    skipped = 0
    failures: list[AdapterProbeFailure] = []
    seen: set[str] = set()
    for adapter in _iter_runtime_edge_adapters(pipeline):
        identity = json.dumps(adapter_identity(adapter), sort_keys=True)
        if identity in seen:
            continue
        seen.add(identity)
        example = getattr(adapter, "example", None)
        if example is None:
            skipped += 1
            continue
        if not callable(example):
            failures.append(AdapterProbeFailure(adapter=adapter.key, reason="example is not callable"))
            continue
        probed += 1
        try:
            _probe_adapter(adapter, example)
        except Exception as exc:
            failures.append(AdapterProbeFailure(adapter=adapter.key, reason=str(exc)))
    return AdapterProbeReport(probed=probed, skipped=skipped, failures=tuple(failures))


def _probe_adapter(adapter: RuntimeAdapter, example: Any) -> None:
    value = example()
    with tempfile.TemporaryDirectory(prefix="spl-adapter-probe-") as tmp_dir:
        path = Path(tmp_dir) / "artifact"
        adapter.save(str(path), value)
        loaded = adapter.load(str(path))
    if loaded != value:
        raise ValueError("round-trip value changed")


def _iter_runtime_edge_adapters(pipeline: Pipeline) -> Iterable[RuntimeAdapter]:
    for binding in _iter_runtime_edge_bindings(pipeline):
        yield binding.save.adapter
        yield binding.load.adapter


def _iter_runtime_edge_bindings(pipeline: Pipeline) -> Iterable[_RuntimeEdgeBinding]:
    alias_by_node = _alias_by_node(pipeline)
    for target_ref, raw_source in sorted(pipeline.links, key=lambda item: _runtime_edge_sort_key(item[0], item[1])):
        source_ref, adapter_format = _runtime_source_ref_and_format(raw_source)
        if source_ref is None:
            continue
        source_type = _runtime_port_type(source_ref.node, source_ref.port, is_output=True)
        target_type = _runtime_port_type(target_ref.node, target_ref.port, is_output=False)
        if source_type is None or target_type is None:
            continue
        save = pipeline.resolve_adapter_binding(py_type=source_type, format=adapter_format)
        load = pipeline.resolve_adapter_binding(py_type=target_type, format=adapter_format)
        if save is None or load is None:
            continue
        yield _RuntimeEdgeBinding(
            edge=_runtime_edge_label(alias_by_node, source_ref, target_ref),
            save=save,
            load=load,
        )


def _runtime_source_ref_and_format(raw_source: Any) -> tuple[NodeOutputRef | None, str | None]:
    if isinstance(raw_source, FormattedOutputRef):
        return raw_source.out_ref, raw_source.format
    if isinstance(raw_source, NodeOutputRef):
        return raw_source, None
    return None, None


def _runtime_port_type(node: Node, port: InputPort | OutputPort, *, is_output: bool) -> type[Any] | None:
    if isinstance(node, NodeFunction):
        annotation = node.func.__annotations__.get("return" if is_output else port.name)
        annotation = _unwrap_annotated(annotation)
        if isinstance(annotation, type):
            return annotation
    if port.typ_ is not None:
        return _BUILTIN_TYPES.get(port.typ_)
    return None


def _unwrap_annotated(annotation: Any) -> Any:
    if typing.get_origin(annotation) is typing.Annotated:
        return typing.get_args(annotation)[0]
    return annotation


def _alias_by_node(pipeline: Pipeline) -> dict[Node, str]:
    result: dict[Node, str] = {}
    for alias, node in sorted(pipeline.aliases.items()):
        result.setdefault(node, alias)
    return result


def _runtime_edge_label(alias_by_node: dict[Node, str], source_ref: NodeOutputRef, target_ref: Any) -> str:
    source = alias_by_node.get(source_ref.node, str(source_ref.node.uuid))
    target = alias_by_node.get(target_ref.node, str(target_ref.node.uuid))
    return "{}.{} -> {}.{}".format(source, source_ref.port.name, target, target_ref.port.name)


def _runtime_edge_sort_key(target_ref: Any, raw_source: Any) -> tuple[str, str]:
    source_ref, _ = _runtime_source_ref_and_format(raw_source)
    source = "" if source_ref is None else str(source_ref.node.uuid)
    return source, "{}.{}".format(target_ref.node.uuid, target_ref.port.name)


def _load_documents(yaml_text: str) -> list[tuple[Any, list[Any]]]:
    documents = []
    for document in yaml.load_all(yaml_text, Loader=SPLSafeLoader):
        if isinstance(document, list) and document:
            root, *dependencies = document
            documents.append((root, dependencies))
    return documents


def _find_dpipeline(documents: list[tuple[Any, list[Any]]], entrypoint: str) -> Any | None:
    for root, _ in documents:
        if getattr(root, "name", None) == entrypoint:
            return root
    return None


def _find_dpipeline_adapter_compatibility_issues(
    pipeline: Any, functions: dict[str, DFunction]
) -> tuple[AdapterCompatibilityIssue, ...]:
    node_functions = {
        node.uuid: functions[node.func]
        for node in pipeline.nodes
        if isinstance(node, DNodeFunction) and node.func in functions
    }
    save_halves, load_halves = _static_adapter_halves(pipeline.adapters)
    aliases = {node_uuid: alias for alias, node_uuid in pipeline.aliases}
    issues = []
    for target_ref, raw_source in pipeline.links:
        if not isinstance(target_ref, DNodeInputRef):
            continue
        source_ref, adapter_format = _static_source_ref_and_format(raw_source)
        if source_ref is None:
            continue
        source_type = _static_port_type(node_functions, source_ref.uuid, source_ref.port, is_output=True)
        target_type = _static_port_type(node_functions, target_ref.uuid, target_ref.port, is_output=False)
        if source_type is None or target_type is None:
            continue
        save = _resolve_static_save(save_halves, source_type, adapter_format)
        load = _resolve_static_load(load_halves, target_type, adapter_format)
        if save is None or load is None or save.tag is None or load.accepted_tags is None:
            continue
        if save.tag not in load.accepted_tags:
            issues.append(
                AdapterCompatibilityIssue(
                    edge=_static_edge_label(aliases, source_ref, target_ref),
                    save_tag=save.tag,
                    accepted_tags=tuple(sorted(load.accepted_tags)),
                    save_adapter=save.key,
                    load_adapter=load.key,
                )
            )
    return tuple(issues)


def _static_adapter_halves(adapters: list[Any]) -> tuple[list[_StaticHalf], list[_StaticHalf]]:
    save_halves: list[_StaticHalf] = []
    load_halves: list[_StaticHalf] = []
    for adapter in adapters:
        if isinstance(adapter, DAdapter):
            adapter_format = _format_from_key(adapter.key)
            half = _StaticHalf(
                key=adapter.key,
                tag=adapter_format,
                accepted_tags=(adapter_format,),
            )
            save_halves.append(half)
            load_halves.append(half)
        elif isinstance(adapter, DSaveAdapter):
            save_halves.append(_StaticHalf(key=adapter.key, tag=adapter.tag, accepted_tags=None))
        elif isinstance(adapter, DLoadAdapter):
            load_halves.append(_StaticHalf(key=adapter.key, tag=None, accepted_tags=adapter.accepted_tags))
    return save_halves, load_halves


def _static_source_ref_and_format(raw_source: Any) -> tuple[DNodeOutputRef | None, str | None]:
    if isinstance(raw_source, DFormattedOutputRef):
        return DNodeOutputRef(uuid=raw_source.uuid, port=raw_source.port), raw_source.format
    if isinstance(raw_source, DNodeOutputRef):
        return raw_source, None
    return None, None


def _static_port_type(
    functions: dict[str, DFunction], node_uuid: str, port_name: str, *, is_output: bool
) -> str | None:
    function = functions.get(node_uuid)
    if function is None:
        return None
    ports = (function.outputs or []) if is_output else function.inputs
    for port in ports:
        if port.name == port_name:
            return port.typ_
    return None


def _resolve_static_save(halves: list[_StaticHalf], type_name: str, adapter_format: str | None) -> _StaticHalf | None:
    if _is_json_native_type_name(type_name) and adapter_format in {None, JSON_ADAPTER_FORMAT}:
        return _StaticHalf(
            key=BUILTIN_JSON_ADAPTER.key,
            tag=BUILTIN_JSON_ADAPTER.tag,
            accepted_tags=None,
        )
    candidates = _matching_static_halves(halves, type_name, adapter_format)
    return candidates[0] if len(candidates) == 1 else None


def _resolve_static_load(halves: list[_StaticHalf], type_name: str, adapter_format: str | None) -> _StaticHalf | None:
    if _is_json_native_type_name(type_name) and adapter_format in {None, JSON_ADAPTER_FORMAT}:
        return _StaticHalf(
            key=BUILTIN_JSON_ADAPTER.key,
            tag=None,
            accepted_tags=tuple(sorted(BUILTIN_JSON_ADAPTER.accepted_tags)),
        )
    candidates = _matching_static_halves(halves, type_name, adapter_format)
    return candidates[0] if len(candidates) == 1 else None


def _matching_static_halves(halves: list[_StaticHalf], type_name: str, adapter_format: str | None) -> list[_StaticHalf]:
    return [
        half
        for half in halves
        if _key_matches_type(half.key, type_name)
        and (adapter_format is None or _format_from_key(half.key) == adapter_format)
    ]


def _key_matches_type(key: str, type_name: str) -> bool:
    key_type, _, _ = key.rpartition("@")
    return key_type == type_name or key_type.endswith(".{}".format(type_name))


def _format_from_key(key: str) -> str:
    _, _, adapter_format = key.rpartition("@")
    return adapter_format


def _is_json_native_type_name(type_name: str | None) -> bool:
    if type_name is None:
        return False
    return _BUILTIN_TYPES.get(type_name) in JSON_NATIVE_TYPES


def _static_edge_label(aliases: dict[str, str], source_ref: DNodeOutputRef, target_ref: DNodeInputRef) -> str:
    source = aliases.get(source_ref.uuid, source_ref.uuid)
    target = aliases.get(target_ref.uuid, target_ref.uuid)
    return "{}.{} -> {}.{}".format(source, source_ref.port, target, target_ref.port)
