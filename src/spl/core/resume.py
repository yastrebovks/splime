"""Resume planning and frozen-output validation helpers."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias, cast
from uuid import uuid4

from spl.core import manifest as m_manifest
from spl.core.entities.adapter import BUILTIN_JSON_ADAPTER
from spl.core.entities.artifact import ArtifactRef, compute_sha256
from spl.core.entities.node import FormattedOutputRef, Node, NodeOutputRef
from spl.core.entities.pipeline import AdapterResolutionSource, Pipeline
from spl.core.fingerprint import canonical_json_bytes, inline_value_sha256

NodeSelector: TypeAlias = Node | str
NodeSelection: TypeAlias = NodeSelector | Iterable[NodeSelector]
_MAX_EXECUTION_PLAN_BYTES = 1024 * 1024


class ResumeValidationError(RuntimeError):
    """Raised when a retained run cannot safely be resumed."""


@dataclass(frozen=True)
class LegacyArtifactClassification:
    """One validated, unambiguous legacy artifact/save pair."""

    artifact: Mapping[str, Any]
    save: Mapping[str, Any]


@dataclass(frozen=True)
class ResumePlan:
    """Resolved resume plan for one parent manifest and pipeline."""

    parent_manifest: Mapping[str, Any]
    parent_run_dir: Path
    recalculated_nodes: frozenset[Node]
    frozen_nodes: frozenset[Node]
    legacy_artifacts: Mapping[tuple[str, str], LegacyArtifactClassification]
    historical_adapter_overrides: Mapping[tuple[str, str], Mapping[str, Any]]


def load_retained_manifest(run_id: str, runs_home: Path | None = None) -> tuple[Path, dict[str, Any]]:
    """Load a retained run manifest by id or run directory path."""

    candidate = Path(run_id).expanduser()
    run_dir = candidate if candidate.is_dir() else (runs_home or m_manifest.default_runs_home()) / run_id
    manifest_path = run_dir / m_manifest.RUN_MANIFEST_FILENAME
    if not manifest_path.exists():
        raise FileNotFoundError("retained run manifest not found: {}".format(manifest_path))
    return run_dir, cast(dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8")))


def plan_resume(
    *,
    pipeline: Pipeline,
    parent_manifest: Mapping[str, Any],
    parent_run_dir: Path,
    from_: NodeSelection,
    kwargs: Mapping[str, Any] | None = None,
) -> ResumePlan:
    """Build and validate a resume plan from recalculation nodes plus overrides."""

    selected_nodes = resolve_selected_nodes(pipeline, from_)
    kwarg_nodes = kwarg_affected_nodes(pipeline, kwargs or {})
    recalculated = close_over_descendants(pipeline, selected_nodes | kwarg_nodes)
    frozen = set(pipeline.nodes) - recalculated
    historical_adapter_overrides: dict[tuple[str, str], Mapping[str, Any]] = {}
    if frozen:
        try:
            historical_adapter_overrides = load_run_execution_plan(
                pipeline=pipeline,
                parent_manifest=parent_manifest,
                parent_run_dir=parent_run_dir,
            )
        except ResumeValidationError:
            guidance = _execution_plan_recovery_guidance(pipeline, frozen)
            raise ResumeValidationError(
                "cannot resume because retained execution-plan evidence is invalid; recalculate with {}".format(
                    guidance
                )
            ) from None
    legacy_artifacts = validate_frozen_outputs(
        pipeline=pipeline,
        parent_manifest=parent_manifest,
        parent_run_dir=parent_run_dir,
        frozen_nodes=frozen,
        recalculated_nodes=recalculated,
        historical_adapter_overrides=historical_adapter_overrides,
    )
    return ResumePlan(
        parent_manifest=parent_manifest,
        parent_run_dir=parent_run_dir,
        recalculated_nodes=frozenset(recalculated),
        frozen_nodes=frozenset(frozen),
        legacy_artifacts=legacy_artifacts,
        historical_adapter_overrides=historical_adapter_overrides,
    )


def load_run_execution_plan(
    *,
    pipeline: Pipeline,
    parent_manifest: Mapping[str, Any],
    parent_run_dir: Path,
) -> dict[tuple[str, str], Mapping[str, Any]]:
    """Load independently persisted save-override evidence for a retained Run."""

    manifest_record = parent_manifest.get("execution_plan")
    evidence_path = parent_run_dir / m_manifest.RUN_EXECUTION_PLAN_FILENAME
    try:
        evidence_exists = evidence_path.exists() or evidence_path.is_symlink()
    except OSError:
        evidence_exists = True
    if manifest_record is None and not evidence_exists:
        return {}
    if not isinstance(manifest_record, Mapping) or not evidence_exists:
        raise ResumeValidationError("retained execution-plan evidence is incomplete")
    if set(manifest_record) != {"version", "adapter_overrides", "evidence"}:
        raise ResumeValidationError("retained execution-plan manifest record is malformed")
    if manifest_record.get("version") != m_manifest.RUN_EXECUTION_PLAN_VERSION:
        raise ResumeValidationError("retained execution-plan version is unsupported")
    evidence = manifest_record.get("evidence")
    if not isinstance(evidence, Mapping) or set(evidence) != {"uri", "sha256"}:
        raise ResumeValidationError("retained execution-plan descriptor is malformed")
    digest = evidence.get("sha256")
    if (
        evidence.get("uri") != m_manifest.RUN_EXECUTION_PLAN_FILENAME
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ResumeValidationError("retained execution-plan descriptor is malformed")

    document = _read_run_execution_plan(evidence_path)
    if set(document) != {"version", "run_id", "adapter_overrides"}:
        raise ResumeValidationError("retained execution-plan document is malformed")
    if document.get("version") != m_manifest.RUN_EXECUTION_PLAN_VERSION:
        raise ResumeValidationError("retained execution-plan document version is unsupported")
    run_id = document.get("run_id")
    if (
        not isinstance(run_id, str)
        or run_id != parent_manifest.get("run_id")
        or hashlib.sha256(canonical_json_bytes(document)).hexdigest() != digest
    ):
        raise ResumeValidationError("retained execution-plan evidence identity is inconsistent")
    raw_overrides = document.get("adapter_overrides")
    if manifest_record.get("adapter_overrides") != raw_overrides or not isinstance(raw_overrides, list):
        raise ResumeValidationError("retained execution-plan overrides are inconsistent")

    known_ports = {(str(node.uuid), port.name) for node in pipeline.nodes for port in node.outputs}
    overrides: dict[tuple[str, str], Mapping[str, Any]] = {}
    for item in raw_overrides:
        if not isinstance(item, Mapping) or set(item) != {"node_id", "port", "identity"}:
            raise ResumeValidationError("retained execution-plan override is malformed")
        node_id = item.get("node_id")
        port = item.get("port")
        identity = item.get("identity")
        if (
            not isinstance(node_id, str)
            or not isinstance(port, str)
            or not isinstance(identity, Mapping)
            or (node_id, port) not in known_ports
            or (node_id, port) in overrides
        ):
            raise ResumeValidationError("retained execution-plan override is invalid or duplicated")
        overrides[(node_id, port)] = dict(identity)
    return overrides


def _read_run_execution_plan(path: Path) -> dict[str, Any]:
    try:
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or _is_reparse_point(before)
            or before.st_size > _MAX_EXECUTION_PLAN_BYTES
        ):
            raise OSError
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            ):
                raise OSError
            body = os.read(descriptor, _MAX_EXECUTION_PLAN_BYTES + 1)
            if len(body) > _MAX_EXECUTION_PLAN_BYTES or os.read(descriptor, 1):
                raise OSError
        finally:
            os.close(descriptor)
        after = path.lstat()
        if (
            (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
            or after.st_size != len(body)
            or after.st_nlink != 1
            or _is_reparse_point(after)
        ):
            raise OSError
        value = json.loads(body.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_pairs)
    except (OSError, UnicodeError, ValueError, TypeError):
        raise ResumeValidationError("retained execution-plan sidecar is malformed") from None
    if not isinstance(value, Mapping):
        raise ResumeValidationError("retained execution-plan sidecar is malformed")
    return dict(value)


def _reject_duplicate_json_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON object key")
        value[key] = item
    return value


def _execution_plan_recovery_guidance(
    pipeline: Pipeline,
    frozen_nodes: Iterable[Node],
) -> str:
    frozen = set(frozen_nodes)
    nodes_with_frozen_graph_inputs = {
        target_ref.node
        for target_ref, value in pipeline.links
        for source_ref in (_as_source_ref(value),)
        if target_ref.node in frozen and source_ref is not None and source_ref.node in frozen
    }
    roots = frozen - nodes_with_frozen_graph_inputs
    if not roots:
        roots = frozen

    def selector(node: Node) -> str:
        aliases = sorted(alias for alias, candidate in pipeline.aliases.items() if candidate == node)
        return aliases[0] if aliases else str(node.uuid)

    selections = sorted(selector(node) for node in roots)
    if len(selections) == 1:
        return "from_={!r}".format(selections[0])
    return "from_=[{}]".format(", ".join(repr(item) for item in selections))


def close_over_descendants(pipeline: Pipeline, nodes: Iterable[Node]) -> set[Node]:
    """Return ``nodes`` plus every DAG descendant."""

    adjacency = _adjacency(pipeline)
    closed = set(nodes)
    queue = list(closed)
    while queue:
        node = queue.pop(0)
        for child in adjacency.get(node, set()):
            if child not in closed:
                closed.add(child)
                queue.append(child)
    return closed


def kwarg_affected_nodes(pipeline: Pipeline, kwargs: Mapping[str, Any]) -> set[Node]:
    """Return nodes whose free inputs are explicitly changed by kwargs."""

    if not kwargs:
        return set()
    linked_inputs = {ref for ref, _ in pipeline.links}
    free_by_name: dict[str, set[Node]] = {}
    for node in pipeline.nodes:
        for port in node.inputs:
            if any(ref.node == node and ref.port == port for ref in linked_inputs):
                continue
            free_by_name.setdefault(port.name, set()).add(node)

    affected: set[Node] = set()
    unknown = []
    for name in kwargs:
        nodes = free_by_name.get(name)
        if not nodes:
            unknown.append(name)
            continue
        affected.update(nodes)
    if unknown:
        raise ValueError(
            "resume kwargs override unknown or linked input(s): {}; pass from_=... for node recalculation "
            "or override a free input name".format(", ".join(sorted(unknown)))
        )
    return affected


def resolve_selected_nodes(pipeline: Pipeline, selection: NodeSelection) -> set[Node]:
    """Resolve aliases, UUID strings, or Node values into pipeline nodes."""

    items = _selection_items(selection)
    nodes = {_resolve_node(pipeline, item) for item in items}
    return nodes


def validate_frozen_outputs(
    *,
    pipeline: Pipeline,
    parent_manifest: Mapping[str, Any],
    parent_run_dir: Path,
    frozen_nodes: Iterable[Node],
    recalculated_nodes: Iterable[Node],
    historical_adapter_overrides: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
) -> dict[tuple[str, str], LegacyArtifactClassification]:
    """Validate that frozen node outputs still match their manifest digests."""

    mismatches: list[str] = []
    frozen = set(frozen_nodes)
    recalculated = set(recalculated_nodes)
    historical_adapter_overrides = historical_adapter_overrides or {}
    legacy_artifacts: dict[tuple[str, str], LegacyArtifactClassification] = {}
    current_outputs: dict[tuple[str, str], tuple[str, Mapping[str, Any], Mapping[str, Any] | None]] = {}
    for node in sorted(frozen, key=lambda item: str(item.uuid)):
        node_record = manifest_node_record(parent_manifest, node)
        label = node_label(node_record, node)
        if node_record is None:
            mismatches.append("{}: missing node record; recalculate with from_='{}'".format(label, label))
            continue
        if node_record.get("status") not in {"succeeded", "frozen"}:
            mismatches.append(
                "{}: node status is `{}`; recalculate with from_='{}'".format(label, node_record.get("status"), label)
            )
            continue
        outputs = node_record.get("outputs")
        if not isinstance(outputs, Mapping):
            mismatches.append("{}: missing outputs; recalculate with from_='{}'".format(label, label))
            continue
        adapters = node_record.get("adapters")
        for port in node.outputs:
            record = outputs.get(port.name)
            adapter_record = (
                cast(Mapping[str, Any], adapters[port.name])
                if isinstance(adapters, Mapping) and isinstance(adapters.get(port.name), Mapping)
                else None
            )
            source_edges = _manifest_edges_from(parent_manifest, str(node.uuid), port.name)
            output_has_variants = isinstance(record, Mapping) and "variants" in record
            adapter_has_variants = adapter_record is not None and "variants" in adapter_record
            edge_has_variant = any("artifact_variant" in edge for edge in source_edges)
            current_evidence = output_has_variants or adapter_has_variants or edge_has_variant
            if isinstance(record, Mapping) and record.get("kind") == "artifact" and not current_evidence:
                classification, output_errors = _classify_legacy_artifact(
                    label,
                    port.name,
                    record,
                    adapter_record=adapter_record,
                    source_edges=source_edges,
                    parent_run_dir=parent_run_dir,
                )
                if classification is not None:
                    legacy_artifacts[(str(node.uuid), port.name)] = classification
            else:
                output_errors = _validate_output_record(
                    label,
                    port.name,
                    record,
                    parent_run_dir,
                    adapter_record=adapter_record,
                    current_variant_evidence=current_evidence,
                )
            mismatches.extend(output_errors)
            if isinstance(record, Mapping) and record.get("kind") == "artifact" and current_evidence:
                current_outputs[(str(node.uuid), port.name)] = (label, record, adapter_record)

    validated_edges: set[tuple[str, str, str, str]] = set()
    for (source_id, source_port), (label, output, adapter_record) in sorted(current_outputs.items()):
        source_edges = _manifest_edges_from(parent_manifest, source_id, source_port)
        for edge in source_edges:
            target = edge.get("target")
            target_id = target.get("node_id") if isinstance(target, Mapping) else None
            target_port = target.get("port") if isinstance(target, Mapping) else None
            edge_key = (source_id, source_port, str(target_id), str(target_port))
            validated_edges.add(edge_key)
            mismatches.extend(
                _validate_current_variant_edge(
                    pipeline,
                    historical_adapter_overrides,
                    label,
                    source_port,
                    edge,
                    output,
                    adapter_record,
                    parent_run_dir,
                )
            )
        mismatches.extend(
            _validate_current_variant_node_sources(
                pipeline,
                historical_adapter_overrides,
                label,
                source_id,
                source_port,
                source_edges,
                output,
                adapter_record,
            )
        )
    for target_ref, value in pipeline.links:
        source_ref = _as_source_ref(value)
        if source_ref is None or source_ref.node not in frozen or target_ref.node not in recalculated:
            continue
        source_id = str(source_ref.node.uuid)
        output_key = (source_id, source_ref.port.name)
        current = current_outputs.get(output_key)
        if current is None:
            continue
        edge_key = (source_id, source_ref.port.name, str(target_ref.node.uuid), target_ref.port.name)
        matching = _manifest_edges_between(
            parent_manifest,
            source_id=source_id,
            source_port=source_ref.port.name,
            target_id=str(target_ref.node.uuid),
            target_port=target_ref.port.name,
        )
        if len(matching) != 1:
            label = current[0]
            mismatches.append(
                "{}:{} current variant edge to {}.{} is missing or duplicated; recalculate with from_='{}'".format(
                    label,
                    source_ref.port.name,
                    node_label(manifest_node_record(parent_manifest, target_ref.node), target_ref.node),
                    target_ref.port.name,
                    label,
                )
            )
        elif edge_key not in validated_edges:
            mismatches.extend(
                _validate_current_variant_edge(
                    pipeline,
                    historical_adapter_overrides,
                    current[0],
                    source_ref.port.name,
                    matching[0],
                    current[1],
                    current[2],
                    parent_run_dir,
                )
            )
    if mismatches:
        raise ResumeValidationError(
            "cannot resume because frozen outputs are invalid:\n- {}\nHint: include these nodes in from_=... "
            "to recalculate them.".format("\n- ".join(mismatches))
        )
    return legacy_artifacts


def manifest_node_record(parent_manifest: Mapping[str, Any], node: Node) -> dict[str, Any] | None:
    """Return a parent manifest node record by current pipeline node UUID."""

    nodes = parent_manifest.get("nodes")
    if not isinstance(nodes, Mapping):
        return None
    record = nodes.get(str(node.uuid))
    if not isinstance(record, Mapping):
        return None
    return dict(record)


def manifest_output_record(parent_manifest: Mapping[str, Any], node: Node, port_name: str) -> dict[str, Any]:
    """Return one frozen output record or raise a readable resume error."""

    node_record = manifest_node_record(parent_manifest, node)
    label = node_label(node_record, node)
    if node_record is None:
        raise ResumeValidationError("{}: missing node record; recalculate with from_='{}'".format(label, label))
    outputs = node_record.get("outputs")
    if not isinstance(outputs, Mapping) or port_name not in outputs:
        raise ResumeValidationError(
            "{}:{} is missing in the retained manifest; recalculate with from_='{}'".format(label, port_name, label)
        )
    record = outputs[port_name]
    if not isinstance(record, Mapping):
        raise ResumeValidationError("{}:{} has an invalid output record".format(label, port_name))
    return dict(record)


def manifest_frozen_save_adapter_record(
    parent_manifest: Mapping[str, Any],
    *,
    source_node: Node,
    source_port: str,
    target_node: Node,
    target_port: str,
) -> dict[str, Any]:
    """Return the save provenance for bytes reused by one frozen edge.

    Current manifests store independent ``save`` and ``load`` blocks. Older
    manifests stored one adapter block directly on the edge. A failed parent
    may not have reached edge recording after it materialized the source, so
    the source node's output-adapter block is the final compatibility fallback.
    """

    edge = manifest_edge_record(
        parent_manifest,
        source_node=source_node,
        source_port=source_port,
        target_node=target_node,
        target_port=target_port,
    )
    if edge is not None:
        record = _save_record_from_edge_adapter(edge.get("adapter"))
        if record is not None:
            return record

    node_record = manifest_node_record(parent_manifest, source_node)
    adapters = None if node_record is None else node_record.get("adapters")
    if isinstance(adapters, Mapping):
        record = _normalize_manifest_adapter_record(adapters.get(source_port))
        if record is not None:
            return record

    source_label = node_label(node_record, source_node)
    raise ResumeValidationError(
        "cannot preserve frozen artifact provenance for `{}.{}` -> `{}.{}` because the parent manifest "
        "has no usable save adapter record; recalculate the producer with from_='{}' or retain a manifest "
        "that records its output adapter".format(
            source_label,
            source_port,
            node_label(manifest_node_record(parent_manifest, target_node), target_node),
            target_port,
            source_label,
        )
    )


def manifest_edge_record(
    parent_manifest: Mapping[str, Any],
    *,
    source_node: Node,
    source_port: str,
    target_node: Node,
    target_port: str,
) -> dict[str, Any] | None:
    """Return the exact parent edge record for one current graph edge."""

    edges = parent_manifest.get("edges")
    if not isinstance(edges, list):
        return None
    source_id = str(source_node.uuid)
    target_id = str(target_node.uuid)
    for edge in edges:
        if not isinstance(edge, Mapping):
            continue
        source = edge.get("source")
        target = edge.get("target")
        if not isinstance(source, Mapping) or not isinstance(target, Mapping):
            continue
        if (
            str(source.get("node_id")) == source_id
            and source.get("port") == source_port
            and str(target.get("node_id")) == target_id
            and target.get("port") == target_port
        ):
            return dict(edge)
    return None


def _manifest_edges_from(
    parent_manifest: Mapping[str, Any],
    source_id: str,
    source_port: str,
) -> list[dict[str, Any]]:
    edges = parent_manifest.get("edges")
    if not isinstance(edges, list):
        return []
    matches: list[dict[str, Any]] = []
    for raw_edge in edges:
        if not isinstance(raw_edge, Mapping):
            continue
        source = raw_edge.get("source")
        if not isinstance(source, Mapping):
            continue
        if str(source.get("node_id")) == source_id and source.get("port") == source_port:
            matches.append(dict(raw_edge))
    return matches


def _manifest_edges_between(
    parent_manifest: Mapping[str, Any],
    *,
    source_id: str,
    source_port: str,
    target_id: str,
    target_port: str,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for edge in _manifest_edges_from(parent_manifest, source_id, source_port):
        target = edge.get("target")
        if (
            isinstance(target, Mapping)
            and str(target.get("node_id")) == target_id
            and target.get("port") == target_port
        ):
            matches.append(edge)
    return matches


def _validate_current_variant_edge(
    pipeline: Pipeline,
    historical_adapter_overrides: Mapping[tuple[str, str], Mapping[str, Any]],
    label: str,
    port: str,
    edge: Mapping[str, Any],
    output_record: Mapping[str, Any],
    adapter_record: Mapping[str, Any] | None,
    parent_run_dir: Path,
) -> list[str]:
    prefix = "{}:{} current edge variant".format(label, port)
    remediation = "recalculate with from_='{}'".format(label)
    try:
        variants = manifest_output_variants(output_record)
    except ResumeValidationError as exc:
        return ["{} cannot be validated because {}; {}".format(prefix, exc, remediation)]
    variant_id = edge.get("artifact_variant")
    if not isinstance(variant_id, str):
        return ["{} is missing artifact_variant; {}".format(prefix, remediation)]
    if variants:
        matches = [item for item in variants if item["variant_id"] == variant_id]
        if len(matches) != 1:
            return ["{} does not identify exactly one node variant; {}".format(prefix, remediation)]
        expected_artifact = cast(Mapping[str, Any], matches[0]["artifact"])
        expected_save = cast(Mapping[str, Any], matches[0]["save"])
    else:
        if adapter_record is None:
            return ["{} has no node save provenance; {}".format(prefix, remediation)]
        expected_artifact = {key: value for key, value in output_record.items() if key != "variants"}
        expected_save = {key: value for key, value in adapter_record.items() if key != "variants"}
        identity = expected_save.get("identity")
        expected_variant_id = (
            hashlib.sha256(canonical_json_bytes(identity)).hexdigest() if isinstance(identity, Mapping) else None
        )
        if variant_id != expected_variant_id:
            return ["{} does not match node save provenance; {}".format(prefix, remediation)]
    edge_artifact = edge.get("artifact")
    edge_adapter = edge.get("adapter")
    edge_save = edge_adapter.get("save") if isinstance(edge_adapter, Mapping) else None
    errors: list[str] = []
    if edge_artifact != expected_artifact:
        errors.append("{} artifact does not exactly match its referenced node variant; {}".format(prefix, remediation))
    if not isinstance(edge_save, Mapping) or {key: value for key, value in edge_save.items() if key != "source"} != {
        key: value for key, value in expected_save.items() if key != "source"
    }:
        errors.append(
            "{} save provenance does not exactly match its referenced node variant; {}".format(prefix, remediation)
        )
    else:
        expected_source = _expected_edge_save_source(
            pipeline,
            historical_adapter_overrides,
            edge,
            expected_save,
        )
        if expected_source is None:
            errors.append("{} does not identify exactly one current graph edge; {}".format(prefix, remediation))
        elif edge_save.get("source") != expected_source:
            errors.append(
                "{} save resolution source is inconsistent with its graph edge; {}".format(prefix, remediation)
            )
    if isinstance(edge_artifact, Mapping):
        errors.extend(
            validate_artifact_record(
                label,
                "{} edge".format(port),
                edge_artifact,
                parent_run_dir,
                allow_absolute=False,
            )
        )
    else:
        errors.append("{} artifact record is malformed; {}".format(prefix, remediation))
    return errors


def _expected_edge_save_source(
    pipeline: Pipeline,
    historical_adapter_overrides: Mapping[tuple[str, str], Mapping[str, Any]],
    edge: Mapping[str, Any],
    node_variant_save: Mapping[str, Any],
) -> str | None:
    source = edge.get("source")
    target = edge.get("target")
    if not isinstance(source, Mapping) or not isinstance(target, Mapping):
        return None
    matches: list[Any] = []
    for target_ref, value in pipeline.links:
        source_ref = _as_source_ref(value)
        if source_ref is None:
            continue
        if (
            str(source_ref.node.uuid) == str(source.get("node_id"))
            and source_ref.port.name == source.get("port")
            and str(target_ref.node.uuid) == str(target.get("node_id"))
            and target_ref.port.name == target.get("port")
        ):
            matches.append(value)
    if len(matches) != 1:
        return None

    override_identity = historical_adapter_overrides.get((str(source.get("node_id")), str(source.get("port"))))
    if override_identity is not None:
        if node_variant_save.get("identity") != override_identity:
            return None
        return AdapterResolutionSource.RUN_OVERRIDE.value
    if isinstance(matches[0], FormattedOutputRef):
        return AdapterResolutionSource.EDGE.value
    identity = node_variant_save.get("identity")
    if isinstance(identity, Mapping) and identity.get("key") == BUILTIN_JSON_ADAPTER.key:
        return AdapterResolutionSource.PORT_DEFAULT.value
    return AdapterResolutionSource.PIPELINE.value


def _validate_current_variant_node_sources(
    pipeline: Pipeline,
    historical_adapter_overrides: Mapping[tuple[str, str], Mapping[str, Any]],
    label: str,
    source_id: str,
    source_port: str,
    source_edges: Iterable[Mapping[str, Any]],
    output_record: Mapping[str, Any],
    adapter_record: Mapping[str, Any] | None,
) -> list[str]:
    source_edges = list(source_edges)
    graph_edge_keys = {
        (str(target_ref.node.uuid), target_ref.port.name)
        for target_ref, value in pipeline.links
        for source_ref in (_as_source_ref(value),)
        if source_ref is not None and str(source_ref.node.uuid) == source_id and source_ref.port.name == source_port
    }
    manifest_edge_keys = [
        (str(target.get("node_id")), str(target.get("port")))
        for edge in source_edges
        for target in (edge.get("target"),)
        if isinstance(target, Mapping)
    ]
    if len(manifest_edge_keys) != len(graph_edge_keys) or set(manifest_edge_keys) != graph_edge_keys:
        return []

    try:
        variants = manifest_output_variants(output_record)
    except ResumeValidationError:
        return []
    if variants:
        saves = {item["variant_id"]: cast(Mapping[str, Any], item["save"]) for item in variants}
    elif adapter_record is not None:
        identity = adapter_record.get("identity")
        variant_id = (
            hashlib.sha256(canonical_json_bytes(identity)).hexdigest() if isinstance(identity, Mapping) else None
        )
        saves = {} if variant_id is None else {variant_id: adapter_record}
    else:
        saves = {}

    sources: dict[str, list[str]] = {}
    for edge in source_edges:
        variant_id = edge.get("artifact_variant")
        save = saves.get(variant_id) if isinstance(variant_id, str) else None
        if save is None:
            continue
        expected_source = _expected_edge_save_source(
            pipeline,
            historical_adapter_overrides,
            edge,
            save,
        )
        if expected_source is not None and isinstance(variant_id, str):
            sources.setdefault(variant_id, []).append(expected_source)

    source_rank = {item.value: rank for rank, item in enumerate(AdapterResolutionSource)}
    remediation = "recalculate with from_='{}'".format(label)
    errors: list[str] = []
    for variant_id, save in saves.items():
        edge_sources = sources.get(variant_id)
        if not edge_sources:
            continue
        canonical_source = max(edge_sources, key=source_rank.__getitem__)
        if save.get("source") != canonical_source:
            errors.append(
                "{}:{} node variant save resolution source is inconsistent with its graph edges; {}".format(
                    label,
                    source_port,
                    remediation,
                )
            )
    return errors


def manifest_exact_variant_for_edge(
    parent_manifest: Mapping[str, Any],
    *,
    pipeline: Pipeline,
    historical_adapter_overrides: Mapping[tuple[str, str], Mapping[str, Any]],
    parent_run_dir: Path,
    source_node: Node,
    source_port: str,
    target_node: Node,
    target_port: str,
    output_record: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a current edge's exact validated node variant."""

    node_record = manifest_node_record(parent_manifest, source_node)
    label = node_label(node_record, source_node)
    adapters = None if node_record is None else node_record.get("adapters")
    adapter_record = (
        cast(Mapping[str, Any], adapters[source_port])
        if isinstance(adapters, Mapping) and isinstance(adapters.get(source_port), Mapping)
        else None
    )
    matching_edges = _manifest_edges_between(
        parent_manifest,
        source_id=str(source_node.uuid),
        source_port=source_port,
        target_id=str(target_node.uuid),
        target_port=target_port,
    )
    if len(matching_edges) != 1:
        raise ResumeValidationError(
            "current variant edge for `{}.{}` -> `{}.{}` is missing or duplicated; recalculate with from_='{}'".format(
                label,
                source_port,
                node_label(manifest_node_record(parent_manifest, target_node), target_node),
                target_port,
                label,
            )
        )
    edge = matching_edges[0]
    errors = _validate_current_variant_edge(
        pipeline,
        historical_adapter_overrides,
        label,
        source_port,
        edge,
        output_record,
        adapter_record,
        parent_run_dir,
    )
    if errors:
        raise ResumeValidationError(errors[0])
    edge_adapter = edge.get("adapter")
    edge_save = edge_adapter.get("save") if isinstance(edge_adapter, Mapping) else None
    if not isinstance(edge_save, Mapping):
        raise ResumeValidationError(
            "current edge has no retained save provenance; recalculate with from_='{}'".format(label)
        )
    variant_id = cast(str, edge["artifact_variant"])
    variants = manifest_output_variants(output_record)
    if not variants:
        if adapter_record is None:
            raise ResumeValidationError(
                "current edge has no retained node save provenance; recalculate with from_='{}'".format(label)
            )
        return {
            "variant_id": variant_id,
            "artifact": {key: value for key, value in output_record.items() if key != "variants"},
            "save": dict(edge_save),
        }
    matches = [item for item in variants if item["variant_id"] == variant_id]
    if len(matches) != 1:
        raise ResumeValidationError(
            "current edge does not identify exactly one retained variant; recalculate with from_='{}'".format(label)
        )
    selected = dict(matches[0])
    selected["save"] = dict(edge_save)
    return selected


def manifest_output_variants(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return additive output variants, or a compatible empty list."""

    raw = record.get("variants")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ResumeValidationError("artifact output variants are malformed")
    variants: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping) or set(item) != {"variant_id", "artifact", "save"}:
            raise ResumeValidationError("artifact output variant is malformed")
        variant_id = item.get("variant_id")
        artifact = item.get("artifact")
        save = item.get("save")
        if (
            not isinstance(variant_id, str)
            or len(variant_id) != 64
            or any(character not in "0123456789abcdef" for character in variant_id)
            or not isinstance(artifact, Mapping)
            or _normalize_manifest_adapter_record(save) is None
        ):
            raise ResumeValidationError("artifact output variant identity is malformed")
        variants.append(
            {"variant_id": variant_id, "artifact": dict(artifact), "save": dict(cast(Mapping[str, Any], save))}
        )
    ids = [item["variant_id"] for item in variants]
    if len(set(ids)) != len(ids):
        raise ResumeValidationError("artifact output variant identity is duplicated")
    return variants


def _save_record_from_edge_adapter(adapter: Any) -> dict[str, Any] | None:
    if not isinstance(adapter, Mapping):
        return None
    save = adapter.get("save")
    if isinstance(save, Mapping):
        return _normalize_manifest_adapter_record(save)
    # v1 compatibility: the edge adapter was one adapter_record used for both
    # halves, before role-separated save/load blocks were additive.
    return _normalize_manifest_adapter_record(adapter)


def _normalize_manifest_adapter_record(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    identity = value.get("identity")
    source = value.get("source")
    if isinstance(identity, Mapping) and isinstance(source, str) and source:
        record = dict(value)
        record["identity"] = dict(identity)
        return record

    # Also tolerate the oldest direct-identity edge shape when it carries its
    # resolution source beside the identity fields.
    if isinstance(value.get("key"), str) and isinstance(source, str) and source:
        identity_value = {key: item for key, item in value.items() if key != "source"}
        return m_manifest.adapter_record(identity_value, source)
    return None


def _classify_legacy_artifact(
    label: str,
    port: str,
    output_record: Mapping[str, Any],
    *,
    adapter_record: Mapping[str, Any] | None,
    source_edges: Iterable[Mapping[str, Any]],
    parent_run_dir: Path,
) -> tuple[LegacyArtifactClassification | None, list[str]]:
    """Prove that marker-free evidence represents one legacy artifact pair."""

    remediation = "recalculate with from_='{}'".format(label)
    errors = validate_artifact_record(
        label,
        port,
        output_record,
        parent_run_dir,
        allow_absolute=True,
    )
    normalized_save = _normalize_manifest_adapter_record(adapter_record)
    if normalized_save is None:
        errors.append(
            "{}:{} marker-free artifact output has no valid top-level save provenance; {}".format(
                label,
                port,
                remediation,
            )
        )
    else:
        try:
            ref = artifact_ref_from_record(output_record, parent_run_dir, allow_absolute=True)
        except ResumeValidationError:
            ref = None
        identity = normalized_save.get("identity")
        identity_mapping = cast(Mapping[str, Any], identity) if isinstance(identity, Mapping) else {}
        if ref is None or identity_mapping.get("key") != ref.key or identity_mapping.get("tag") != ref.tag:
            errors.append(
                "{}:{} marker-free top-level artifact/save provenance does not match; {}".format(
                    label,
                    port,
                    remediation,
                )
            )

    top_artifact = dict(output_record)
    for edge in source_edges:
        edge_artifact = edge.get("artifact")
        edge_save = _save_record_from_edge_adapter(edge.get("adapter"))
        if (
            not isinstance(edge_artifact, Mapping)
            or edge_artifact.get("kind") != "artifact"
            or normalized_save is None
            or dict(edge_artifact) != top_artifact
            or edge_save != normalized_save
        ):
            errors.append(
                "{}:{} marker-free artifact evidence is ambiguous because an outgoing edge does not exactly "
                "match the top-level artifact/save pair; {}".format(label, port, remediation)
            )

    if errors or normalized_save is None:
        return None, errors
    return (
        LegacyArtifactClassification(
            artifact=top_artifact,
            save=normalized_save,
        ),
        [],
    )


def artifact_ref_from_record(
    record: Mapping[str, Any],
    parent_run_dir: Path,
    *,
    allow_absolute: bool = True,
) -> ArtifactRef:
    """Build an ArtifactRef from a manifest artifact record."""

    ref = record.get("ref")
    if not isinstance(ref, Mapping):
        raise ResumeValidationError("artifact output record is missing `ref`")
    uri = ref.get("uri")
    if not isinstance(uri, str) or not uri:
        raise ResumeValidationError("artifact output record has an invalid uri")
    path = Path(uri)
    if path.is_absolute():
        if not allow_absolute:
            raise ResumeValidationError("current artifact uri must be relative to the retained Run")
    else:
        parent = parent_run_dir.resolve()
        if any(part in {"", ".", ".."} for part in path.parts):
            raise ResumeValidationError("artifact output record has an unsafe relative uri")
        candidate = parent / path
        current = parent
        for part in path.parts:
            current /= part
            if current.is_symlink():
                raise ResumeValidationError("artifact output record traverses a symbolic link")
        resolved = candidate.resolve()
        try:
            resolved.relative_to(parent)
        except ValueError:
            raise ResumeValidationError("artifact output record escapes its retained run directory") from None
        path = candidate
    key = ref.get("key")
    sha256 = ref.get("sha256")
    size = ref.get("size")
    tag = ref.get("tag")
    if not isinstance(key, str) or not isinstance(sha256, str) or type(size) is not int:
        raise ResumeValidationError("artifact output reference identity is malformed")
    if tag is not None and not isinstance(tag, str):
        raise ResumeValidationError("artifact output reference tag is malformed")
    try:
        return ArtifactRef(
            key=key,
            uri=str(path),
            sha256=sha256,
            size=size,
            tag=tag,
        )
    except (TypeError, ValueError) as exc:
        raise ResumeValidationError("artifact output reference identity is malformed") from exc


def copy_artifact_ref_for_run(ref: ArtifactRef, run_dir: Path) -> ArtifactRef:
    """Copy a verified retained artifact into a child Run without decoding it."""

    source = Path(ref.uri)
    run_path = run_dir.absolute()
    run_root = run_path.resolve()
    try:
        relative_source = source.resolve().relative_to(run_root)
    except ValueError:
        pass
    else:
        return ArtifactRef(
            key=ref.key,
            uri=str(run_path / relative_source),
            sha256=ref.sha256,
            size=ref.size,
            tag=ref.tag,
        )

    artifacts_dir = run_path / "artifacts"
    artifacts_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    artifacts_dir.chmod(0o700)
    directory_identity = artifacts_dir.lstat()
    if not stat.S_ISDIR(directory_identity.st_mode) or _is_reparse_point(directory_identity):
        raise ResumeValidationError("child artifact directory is not a stable private directory")
    identity_digest = hashlib.sha256(
        canonical_json_bytes({"key": ref.key, "tag": ref.tag, "sha256": ref.sha256, "size": ref.size})
    ).hexdigest()
    target = artifacts_dir / "artifact-frozen-{}".format(identity_digest)
    localized = ArtifactRef(
        key=ref.key,
        uri=str(target),
        sha256=ref.sha256,
        size=ref.size,
        tag=ref.tag,
    )
    if target.exists():
        _verify_copied_artifact(localized)
        return localized

    temporary = artifacts_dir / ".{}.{}.tmp".format(target.name, uuid4().hex)
    before = source.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or _is_reparse_point(before):
        raise ResumeValidationError("retained artifact is not a stable private regular file")
    source_flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
    source_flags |= getattr(os, "O_NOFOLLOW", 0)
    target_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    target_flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    source_fd = os.open(source, source_flags)
    target_fd: int | None = None
    try:
        opened = os.fstat(source_fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or _is_reparse_point(opened)
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise ResumeValidationError("retained artifact changed before it was opened")
        target_fd = os.open(temporary, target_flags, 0o600)
        digest = hashlib.sha256()
        copied = 0
        while True:
            chunk = os.read(source_fd, 1024 * 1024)
            if not chunk:
                break
            copied += len(chunk)
            if copied > ref.size:
                raise ResumeValidationError("retained artifact grew while it was copied")
            digest.update(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(target_fd, view)
                if written <= 0:
                    raise OSError("artifact copy made no progress")
                view = view[written:]
        os.fsync(target_fd)
        after = os.fstat(source_fd)
        copied_identity = os.fstat(target_fd)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        os.close(source_fd)
        if target_fd is not None:
            os.close(target_fd)
    current = source.lstat()
    current_directory = artifacts_dir.lstat()
    if (
        copied != ref.size
        or digest.hexdigest() != ref.sha256
        or copied_identity.st_size != copied
        or not stat.S_ISREG(copied_identity.st_mode)
        or copied_identity.st_nlink != 1
        or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
        or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino)
        or current.st_nlink != 1
        or current.st_size != copied
        or _is_reparse_point(current)
        or (current_directory.st_dev, current_directory.st_ino)
        != (directory_identity.st_dev, directory_identity.st_ino)
    ):
        temporary.unlink(missing_ok=True)
        raise ResumeValidationError("retained artifact changed while it was copied")
    os.replace(temporary, target)
    target.chmod(0o600)
    _verify_copied_artifact(localized)
    return localized


def _verify_copied_artifact(ref: ArtifactRef) -> None:
    path = Path(ref.uri)
    try:
        identity = path.lstat()
    except OSError as exc:
        raise ResumeValidationError("copied frozen artifact is missing") from exc
    if (
        not stat.S_ISREG(identity.st_mode)
        or identity.st_nlink != 1
        or identity.st_size != ref.size
        or _is_reparse_point(identity)
        or compute_sha256(path) != ref.sha256
    ):
        raise ResumeValidationError("copied frozen artifact failed integrity validation")


def _is_reparse_point(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0))
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def rebase_output_record(record: Mapping[str, Any], parent_run_dir: Path, run_dir: Path | None) -> dict[str, Any]:
    """Return an output record whose artifact URI is meaningful in a child manifest."""

    if record.get("kind") != "artifact":
        return dict(record)
    ref = artifact_ref_from_record(record, parent_run_dir)
    if run_dir is not None:
        ref = copy_artifact_ref_for_run(ref, run_dir)
    rebased = m_manifest.artifact_record(ref, run_dir=run_dir)
    variants = manifest_output_variants(record)
    if variants:
        rebased["variants"] = [
            {
                "variant_id": item["variant_id"],
                "artifact": m_manifest.artifact_record(
                    artifact_ref_from_record(cast(Mapping[str, Any], item["artifact"]), parent_run_dir),
                    run_dir=run_dir,
                ),
                "save": dict(cast(Mapping[str, Any], item["save"])),
            }
            for item in variants
        ]
    return rebased


def localize_output_record(record: Mapping[str, Any], run_dir: Path) -> dict[str, Any]:
    """Localize absolute resume artifact references before retaining a child manifest."""

    if record.get("kind") != "artifact":
        return dict(record)
    ref_value = record.get("ref")
    uri = ref_value.get("uri") if isinstance(ref_value, Mapping) else None
    localized = dict(record)
    if isinstance(uri, str) and Path(uri).is_absolute():
        ref = copy_artifact_ref_for_run(artifact_ref_from_record(record, run_dir), run_dir)
        localized.update(m_manifest.artifact_record(ref, run_dir=run_dir))
    variants = manifest_output_variants(record)
    if variants:
        localized["variants"] = [
            {
                "variant_id": item["variant_id"],
                "artifact": localize_output_record(
                    cast(Mapping[str, Any], item["artifact"]),
                    run_dir,
                ),
                "save": dict(cast(Mapping[str, Any], item["save"])),
            }
            for item in variants
        ]
    return localized


def frozen_node_record(
    parent_record: Mapping[str, Any], *, parent_run_dir: Path, run_dir: Path | None
) -> dict[str, Any]:
    """Copy a parent manifest node record into a child manifest as frozen."""

    record = dict(parent_record)
    record["status"] = "frozen"
    record["error"] = None
    outputs = record.get("outputs")
    if isinstance(outputs, Mapping):
        record["outputs"] = {
            str(port): rebase_output_record(cast(Mapping[str, Any], output), parent_run_dir, run_dir)
            for port, output in outputs.items()
            if isinstance(output, Mapping)
        }
    return record


def node_label(record: Mapping[str, Any] | None, node: Node) -> str:
    """Return a human-readable node selector for resume errors."""

    if record is not None:
        alias = record.get("alias")
        if isinstance(alias, str) and alias:
            return alias
        name = record.get("name")
        if isinstance(name, str) and name:
            return name
    return str(node.uuid)


def _validate_output_record(
    label: str,
    port: str,
    record: Any,
    parent_run_dir: Path,
    *,
    adapter_record: Mapping[str, Any] | None,
    current_variant_evidence: bool,
) -> list[str]:
    if not isinstance(record, Mapping):
        return ["{}:{} is missing; recalculate with from_='{}'".format(label, port, label)]
    kind = record.get("kind")
    if kind == "json":
        expected = record.get("sha256")
        actual = inline_value_sha256(record.get("value"))
        if expected != actual:
            return [
                "{}:{} JSON sha256 mismatch: expected {}, actual {}; recalculate with from_='{}'".format(
                    label, port, expected, actual, label
                )
            ]
        return []
    if kind == "artifact":
        errors = validate_artifact_record(
            label,
            port,
            record,
            parent_run_dir,
            allow_absolute=not current_variant_evidence,
        )
        try:
            variants = manifest_output_variants(record)
        except ResumeValidationError as exc:
            return ["{}:{} {}; recalculate with from_='{}'".format(label, port, exc, label)]
        if (
            current_variant_evidence
            and not variants
            and ("variants" in record or adapter_record is not None and "variants" in adapter_record)
        ):
            errors.append(
                "{}:{} current variant evidence is incomplete; recalculate with from_='{}'".format(label, port, label)
            )
        if variants and len(variants) < 2:
            errors.append(
                "{}:{} current multi-variant output must contain at least two variants; recalculate with "
                "from_='{}'".format(label, port, label)
            )
        seen_save_identities: set[str] = set()
        for item in variants:
            save = cast(Mapping[str, Any], item["save"])
            identity = save.get("identity")
            identity_mapping = cast(Mapping[str, Any], identity) if isinstance(identity, Mapping) else {}
            identity_key = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            if identity_key in seen_save_identities:
                errors.append(
                    "{}:{} has duplicate save identities; recalculate with from_='{}'".format(label, port, label)
                )
            seen_save_identities.add(identity_key)
            artifact = cast(Mapping[str, Any], item["artifact"])
            try:
                ref = artifact_ref_from_record(
                    artifact,
                    parent_run_dir,
                    allow_absolute=False,
                )
            except ResumeValidationError:
                ref = None
            expected_variant_id = (
                hashlib.sha256(canonical_json_bytes(identity)).hexdigest() if isinstance(identity, Mapping) else None
            )
            if (
                ref is None
                or expected_variant_id != item["variant_id"]
                or identity_mapping.get("key") != ref.key
                or identity_mapping.get("tag") != ref.tag
            ):
                errors.append(
                    "{}:{} variant identity/provenance mismatch; recalculate with from_='{}'".format(label, port, label)
                )
            errors.extend(
                validate_artifact_record(
                    label,
                    "{}[{}]".format(port, item["variant_id"]),
                    artifact,
                    parent_run_dir,
                    allow_absolute=False,
                )
            )
        if variants:
            primary = min(variants, key=lambda item: item["variant_id"])
            compatibility_artifact = {key: value for key, value in record.items() if key != "variants"}
            if compatibility_artifact != primary["artifact"]:
                errors.append(
                    "{}:{} top-level compatibility artifact does not match the deterministic primary variant; "
                    "recalculate with from_='{}'".format(label, port, label)
                )
            expected_adapter_variants = [
                {"variant_id": item["variant_id"], "save": item["save"]}
                for item in sorted(variants, key=lambda item: item["variant_id"])
            ]
            if adapter_record is None:
                errors.append(
                    "{}:{} current multi-variant output is missing top-level save provenance; recalculate with "
                    "from_='{}'".format(label, port, label)
                )
            else:
                compatibility_save = {key: value for key, value in adapter_record.items() if key != "variants"}
                if compatibility_save != primary["save"]:
                    errors.append(
                        "{}:{} top-level compatibility save provenance does not match the deterministic primary "
                        "variant; recalculate with from_='{}'".format(label, port, label)
                    )
                if adapter_record.get("variants") != expected_adapter_variants:
                    errors.append(
                        "{}:{} node adapter variants do not exactly match output variants; recalculate with "
                        "from_='{}'".format(label, port, label)
                    )
        return errors
    if kind == "unfreezable":
        return [
            "{}:{} is unfreezable ({}); recalculate with from_='{}'".format(label, port, record.get("reason"), label)
        ]
    return ["{}:{} has unsupported output kind `{}`; recalculate with from_='{}'".format(label, port, kind, label)]


def validate_artifact_record(
    label: str,
    port: str,
    record: Mapping[str, Any],
    parent_run_dir: Path,
    *,
    allow_absolute: bool = True,
) -> list[str]:
    try:
        ref = artifact_ref_from_record(
            record,
            parent_run_dir,
            allow_absolute=allow_absolute,
        )
    except ResumeValidationError as exc:
        return ["{}:{} {}; recalculate with from_='{}'".format(label, port, exc, label)]
    if record.get("tag") != ref.tag or record.get("sha256") != ref.sha256:
        return [
            "{}:{} artifact record identity differs from its ref; recalculate with from_='{}'".format(
                label, port, label
            )
        ]
    path = Path(ref.uri)
    try:
        path_stat = path.lstat()
    except OSError:
        path_stat = None
    if path_stat is None or not stat.S_ISREG(path_stat.st_mode) or path_stat.st_nlink != 1:
        return [
            "{}:{} artifact is missing or not a private regular file; expected sha256 {}; "
            "recalculate with from_='{}'".format(label, port, ref.sha256, label)
        ]
    try:
        path = Path(ref.uri)
        actual_size = path.stat().st_size
        actual_sha256 = compute_sha256(path)
    except OSError:
        return ["{}:{} artifact cannot be read; recalculate with from_='{}'".format(label, port, label)]
    errors = []
    if actual_size != ref.size:
        errors.append(
            "{}:{} artifact size mismatch: expected {}, actual {}; recalculate with from_='{}'".format(
                label, port, ref.size, actual_size, label
            )
        )
    if actual_sha256 != ref.sha256:
        errors.append(
            "{}:{} artifact sha256 mismatch: expected {}, actual {}; recalculate with from_='{}'".format(
                label, port, ref.sha256, actual_sha256, label
            )
        )
    return errors


def _adjacency(pipeline: Pipeline) -> dict[Node, set[Node]]:
    adjacency: dict[Node, set[Node]] = {node: set() for node in pipeline.nodes}
    for target_ref, value in pipeline.links:
        source_ref = _as_source_ref(value)
        if source_ref is not None:
            adjacency.setdefault(source_ref.node, set()).add(target_ref.node)
    return adjacency


def _as_source_ref(value: Any) -> NodeOutputRef | None:
    if isinstance(value, FormattedOutputRef):
        return value.out_ref
    if isinstance(value, NodeOutputRef):
        return value
    return None


def _selection_items(selection: NodeSelection) -> list[NodeSelector]:
    if isinstance(selection, Node | str):
        return [selection]
    return list(selection)


def _resolve_node(pipeline: Pipeline, selector: NodeSelector) -> Node:
    if isinstance(selector, Node):
        if selector not in pipeline.nodes:
            raise ValueError("resume from_ node is not in the pipeline: {}".format(selector))
        return selector
    if selector in pipeline.aliases:
        return pipeline.aliases[selector]
    for node in pipeline.nodes:
        if str(node.uuid) == selector:
            return node
    raise ValueError("resume from_ references unknown node or alias `{}`".format(selector))
