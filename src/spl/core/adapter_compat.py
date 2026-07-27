"""Static adapter compatibility checks and local adapter probes."""

from __future__ import annotations

import tempfile
import typing
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from types import UnionType
from typing import Any

import yaml

from spl.core.entities.adapter import (
    BUILTIN_JSON_ADAPTER,
    JSON_ADAPTER_FORMAT,
    JSON_NATIVE_TYPES,
    Adapter,
    BuiltInJsonAdapter,
    DAdapter,
    DLoadAdapter,
    DSaveAdapter,
    RuntimeAdapter,
    SaveAdapter,
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
from spl.core.entities.pipeline import (
    AdapterResolutionSource,
    LoadAdapterResolution,
    Pipeline,
    SaveAdapterResolution,
)
from spl.core.ir.utils import SPLSafeLoader
from spl.core.json_contract import dumps as json_dumps


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
class StaticAdapterResolution:
    """Adapter keys statically selected for one serialized formatted edge."""

    save_adapter: str | None
    load_adapter: str | None
    save_candidates: tuple[str, ...] = ()
    load_candidates: tuple[str, ...] = ()
    save_deferred: bool = False
    load_deferred: bool = False

    @property
    def save_ambiguous(self) -> bool:
        """Return whether more than one save half could serve the edge."""

        return not self.save_deferred and self.save_adapter is None and len(self.save_candidates) > 1

    @property
    def load_ambiguous(self) -> bool:
        """Return whether more than one load half could serve the edge."""

        return not self.load_deferred and self.load_adapter is None and len(self.load_candidates) > 1


@dataclass(frozen=True)
class _RuntimeEdgeBinding:
    edge: str
    save: SaveAdapterResolution
    load: LoadAdapterResolution


@dataclass(frozen=True)
class _StaticHalf:
    key: str
    tag: str | None
    accepted_tags: tuple[str, ...] | None
    legacy_pair: bool = False


_BUILTIN_TYPES: dict[str, type[Any]] = {
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


def resolve_yaml_edge_adapters(
    adapters: list[Any],
    *,
    source_type: str | None,
    target_type: str | None,
    adapter_format: str,
) -> StaticAdapterResolution:
    """Resolve both adapter halves for an explicitly formatted serialized edge."""

    save_halves, load_halves = _static_adapter_halves(adapters)
    normalized_source_type = _normalize_static_type_name(source_type)
    normalized_target_type = _normalize_static_type_name(target_type)
    save_candidates = _static_save_candidates(save_halves, normalized_source_type, adapter_format)
    save = _sole_static_half(save_candidates)
    save_deferred = (
        save is None and bool(save_candidates) and (normalized_source_type is None or "." not in normalized_source_type)
    )

    paired_load = _legacy_paired_load(save, load_halves)
    load_deferred = False
    if normalized_target_type is None and paired_load is not None:
        # A legacy DAdapter is one logical save/load pair. Preserve its
        # historical source-selected behavior for untyped/Any consumers.
        load_candidates = [paired_load]
    elif normalized_target_type is None and save_deferred:
        candidate_pairs = [_legacy_paired_load(candidate, load_halves) for candidate in save_candidates]
        if all(pair is not None for pair in candidate_pairs):
            # Runtime selects the save half from type(value). If every possible
            # save is a full legacy pair, that same selection also determines
            # the load half without a target annotation.
            load_candidates = [typing.cast(_StaticHalf, pair) for pair in candidate_pairs]
            load_deferred = True
        else:
            load_candidates = _static_load_candidates(load_halves, normalized_target_type, adapter_format)
    else:
        load_candidates = _static_load_candidates(load_halves, normalized_target_type, adapter_format)
    load = _sole_static_half(load_candidates)
    if load_deferred:
        load = None
    return StaticAdapterResolution(
        save_adapter=None if save is None else save.key,
        load_adapter=None if load is None else load.key,
        save_candidates=_static_candidate_keys(save_candidates),
        load_candidates=_static_candidate_keys(load_candidates),
        save_deferred=save_deferred,
        load_deferred=load_deferred,
    )


def _static_save_candidates(halves: list[_StaticHalf], type_name: str | None, adapter_format: str) -> list[_StaticHalf]:
    candidates = (
        _matching_static_halves(halves, type_name, adapter_format)
        if type_name is not None
        else _static_halves_by_format(halves, adapter_format)
    )
    if candidates or adapter_format != JSON_ADAPTER_FORMAT:
        return candidates
    return [
        _StaticHalf(
            key=BUILTIN_JSON_ADAPTER.key,
            tag=BUILTIN_JSON_ADAPTER.tag,
            accepted_tags=None,
            legacy_pair=True,
        )
    ]


def _static_load_candidates(halves: list[_StaticHalf], type_name: str | None, adapter_format: str) -> list[_StaticHalf]:
    candidates = (
        _matching_static_halves(halves, type_name, adapter_format)
        if type_name is not None
        else _static_halves_by_format(halves, adapter_format)
    )
    if candidates or adapter_format != JSON_ADAPTER_FORMAT:
        return candidates
    return [
        _StaticHalf(
            key=BUILTIN_JSON_ADAPTER.key,
            tag=None,
            accepted_tags=tuple(sorted(BUILTIN_JSON_ADAPTER.accepted_tags)),
            legacy_pair=True,
        )
    ]


def _static_halves_by_format(halves: list[_StaticHalf], adapter_format: str) -> list[_StaticHalf]:
    return [half for half in halves if _format_from_key(half.key) == adapter_format]


def _sole_static_half(candidates: list[_StaticHalf]) -> _StaticHalf | None:
    unique = {candidate.key: candidate for candidate in candidates}
    return next(iter(unique.values())) if len(unique) == 1 else None


def _static_candidate_keys(candidates: list[_StaticHalf]) -> tuple[str, ...]:
    return tuple(sorted({candidate.key for candidate in candidates}))


def _legacy_paired_load(save: _StaticHalf | None, load_halves: list[_StaticHalf]) -> _StaticHalf | None:
    if save is None or not save.legacy_pair:
        return None
    if save.key == BUILTIN_JSON_ADAPTER.key:
        return _StaticHalf(
            key=BUILTIN_JSON_ADAPTER.key,
            tag=None,
            accepted_tags=tuple(sorted(BUILTIN_JSON_ADAPTER.accepted_tags)),
            legacy_pair=True,
        )
    return next((half for half in load_halves if half.legacy_pair and half.key == save.key), None)


def probe_pipeline_adapters(pipeline: Pipeline) -> AdapterProbeReport:
    """Run local save/load probes for adapters with an ``example`` callable."""

    probed = 0
    skipped = 0
    failures: list[AdapterProbeFailure] = []
    seen: set[str] = set()
    for adapter in _iter_runtime_edge_adapters(pipeline):
        identity = json_dumps(adapter_identity(adapter), ensure_ascii=True, separators=None)
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
        for adapter in (binding.save.adapter, binding.load.adapter):
            if isinstance(adapter, Adapter | BuiltInJsonAdapter):
                yield adapter


def _iter_runtime_edge_bindings(pipeline: Pipeline) -> Iterable[_RuntimeEdgeBinding]:
    alias_by_node = _alias_by_node(pipeline)
    for target_ref, raw_source in sorted(pipeline.links, key=lambda item: _runtime_edge_sort_key(item[0], item[1])):
        source_ref, adapter_format = _runtime_source_ref_and_format(raw_source)
        if source_ref is None:
            continue
        source_type = _runtime_port_type(source_ref.node, source_ref.port, is_output=True)
        target_type = _runtime_port_type(target_ref.node, target_ref.port, is_output=False)
        if isinstance(source_type, type):
            save = pipeline.resolve_save_adapter_binding(py_type=source_type, format=adapter_format)
        else:
            save = _representative_runtime_save_by_format(pipeline, adapter_format)
        if save is None:
            continue
        if isinstance(target_type, type):
            load = pipeline.resolve_load_adapter_binding(py_type=target_type, format=adapter_format)
        elif isinstance(target_type, str):
            load = pipeline.resolve_load_adapter_binding_by_type_name(
                type_name=target_type,
                format=adapter_format,
            )
        elif isinstance(save.adapter, Adapter | BuiltInJsonAdapter):
            load = LoadAdapterResolution(save.adapter, save.source)
        elif adapter_format is not None:
            try:
                load = pipeline.resolve_load_adapter_binding_by_format(format=adapter_format)
            except ValueError:
                load = None
        else:
            load = None
        if save is None or load is None:
            continue
        yield _RuntimeEdgeBinding(
            edge=_runtime_edge_label(alias_by_node, source_ref, target_ref),
            save=save,
            load=load,
        )


def _representative_runtime_save_by_format(
    pipeline: Pipeline, adapter_format: str | None
) -> SaveAdapterResolution | None:
    if adapter_format is None:
        return None
    registered: dict[str, SaveAdapter] = {**pipeline.adapters, **pipeline.save_adapters}
    candidates: list[SaveAdapter] = [
        adapter for key, adapter in sorted(registered.items()) if _format_from_key(key) == adapter_format
    ]
    if not candidates and adapter_format == JSON_ADAPTER_FORMAT:
        candidates = [BUILTIN_JSON_ADAPTER]
    if not candidates:
        return None
    return SaveAdapterResolution(candidates[0], AdapterResolutionSource.EDGE)


def _runtime_source_ref_and_format(raw_source: Any) -> tuple[NodeOutputRef | None, str | None]:
    if isinstance(raw_source, FormattedOutputRef):
        return raw_source.out_ref, raw_source.format
    if isinstance(raw_source, NodeOutputRef):
        return raw_source, None
    return None, None


def _runtime_port_type(node: Node, port: InputPort | OutputPort, *, is_output: bool) -> type[Any] | str | None:
    if isinstance(node, NodeFunction):
        try:
            annotation = typing.get_type_hints(node.func, include_extras=True).get("return" if is_output else port.name)
        except (NameError, TypeError):
            annotation = node.func.__annotations__.get("return" if is_output else port.name)
        normalized = _normalize_runtime_annotation(annotation)
        if normalized is not None:
            return normalized
    if port.typ_ is not None:
        type_name = _normalize_static_type_name(port.typ_)
        if type_name is not None:
            return _BUILTIN_TYPES.get(type_name, type_name)
    return None


def _normalize_runtime_annotation(annotation: Any) -> type[Any] | str | None:
    if annotation is None:
        return None
    if isinstance(annotation, str):
        type_name = _normalize_static_type_name(annotation)
        if type_name is None:
            return None
        return _BUILTIN_TYPES.get(type_name, type_name)
    while typing.get_origin(annotation) is typing.Annotated:
        annotation = typing.get_args(annotation)[0]
    if annotation is Any:
        return None
    origin = typing.get_origin(annotation)
    if origin in {typing.Union, UnionType}:
        concrete = [item for item in typing.get_args(annotation) if item is not type(None)]
        if len(concrete) != 1:
            return None
        return _normalize_runtime_annotation(concrete[0])
    if isinstance(origin, type):
        return origin
    if isinstance(annotation, type):
        return annotation
    return None


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
        if adapter_format is not None:
            resolution = resolve_yaml_edge_adapters(
                pipeline.adapters,
                source_type=source_type,
                target_type=target_type,
                adapter_format=adapter_format,
            )
            save = _static_half_by_key(save_halves, resolution.save_adapter, role="save")
            load = _static_half_by_key(load_halves, resolution.load_adapter, role="load")
            if save is None and resolution.save_deferred and resolution.save_candidates:
                save = _static_half_by_key(save_halves, resolution.save_candidates[0], role="save")
        elif source_type is not None and target_type is not None:
            save = _resolve_static_save(save_halves, source_type, adapter_format)
            load = _resolve_static_load(load_halves, target_type, adapter_format)
        else:
            continue
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


def _static_half_by_key(halves: list[_StaticHalf], key: str | None, *, role: str) -> _StaticHalf | None:
    if key is None:
        return None
    match = next((half for half in halves if half.key == key), None)
    if match is not None:
        return match
    if key != BUILTIN_JSON_ADAPTER.key:
        return None
    return _StaticHalf(
        key=key,
        tag=BUILTIN_JSON_ADAPTER.tag if role == "save" else None,
        accepted_tags=(None if role == "save" else tuple(sorted(BUILTIN_JSON_ADAPTER.accepted_tags))),
        legacy_pair=True,
    )


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
                legacy_pair=True,
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
    normalized_type = _normalize_static_type_name(type_name)
    if normalized_type is None:
        return None
    candidates = _matching_static_halves(halves, normalized_type, adapter_format)
    if len(candidates) == 1:
        return candidates[0]
    if candidates:
        return None
    if _is_json_native_type_name(normalized_type) and adapter_format in {None, JSON_ADAPTER_FORMAT}:
        return _StaticHalf(
            key=BUILTIN_JSON_ADAPTER.key,
            tag=BUILTIN_JSON_ADAPTER.tag,
            accepted_tags=None,
            legacy_pair=True,
        )
    return None


def _resolve_static_load(halves: list[_StaticHalf], type_name: str, adapter_format: str | None) -> _StaticHalf | None:
    normalized_type = _normalize_static_type_name(type_name)
    if normalized_type is None:
        return None
    candidates = _matching_static_halves(halves, normalized_type, adapter_format)
    if len(candidates) == 1:
        return candidates[0]
    if candidates:
        return None
    if _is_json_native_type_name(normalized_type) and adapter_format in {None, JSON_ADAPTER_FORMAT}:
        return _StaticHalf(
            key=BUILTIN_JSON_ADAPTER.key,
            tag=None,
            accepted_tags=tuple(sorted(BUILTIN_JSON_ADAPTER.accepted_tags)),
            legacy_pair=True,
        )
    return None


def _matching_static_halves(halves: list[_StaticHalf], type_name: str, adapter_format: str | None) -> list[_StaticHalf]:
    return [
        half
        for half in halves
        if _key_matches_type(half.key, type_name)
        and (adapter_format is None or _format_from_key(half.key) == adapter_format)
    ]


def _key_matches_type(key: str, type_name: str) -> bool:
    key_type, _, _ = key.rpartition("@")
    return key_type == type_name or ("." not in type_name and key_type.endswith(".{}".format(type_name)))


def _format_from_key(key: str) -> str:
    _, _, adapter_format = key.rpartition("@")
    return adapter_format


def _normalize_static_type_name(type_name: str | None) -> str | None:
    """Return a concrete outer type name, or None for unavailable/union types."""

    if type_name is None:
        return None
    normalized = type_name.strip()
    if not normalized or normalized in {"Any", "typing.Any"}:
        return None
    if "|" in normalized:
        return None
    outer = normalized.partition("[")[0].strip()
    if outer in {"Annotated", "typing.Annotated", "Optional", "typing.Optional", "Union", "typing.Union"}:
        return None
    concrete = _BUILTIN_TYPES.get(outer)
    if concrete is not None:
        return "{}.{}".format(concrete.__module__, concrete.__qualname__)
    return outer


def _is_json_native_type_name(type_name: str | None) -> bool:
    if type_name is None:
        return False
    return _BUILTIN_TYPES.get(type_name) in JSON_NATIVE_TYPES


def _static_edge_label(aliases: dict[str, str], source_ref: DNodeOutputRef, target_ref: DNodeInputRef) -> str:
    source = aliases.get(source_ref.uuid, source_ref.uuid)
    target = aliases.get(target_ref.uuid, target_ref.uuid)
    return "{}.{} -> {}.{}".format(source, source_ref.port, target, target_ref.port)
