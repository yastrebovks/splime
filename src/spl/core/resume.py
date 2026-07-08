"""Resume planning and frozen-output validation helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias, cast

from spl.core import manifest as m_manifest
from spl.core.entities.artifact import ArtifactRef, compute_sha256
from spl.core.entities.node import FormattedOutputRef, Node, NodeOutputRef
from spl.core.entities.pipeline import Pipeline
from spl.core.fingerprint import inline_value_sha256

NodeSelector: TypeAlias = Node | str
NodeSelection: TypeAlias = NodeSelector | Iterable[NodeSelector]


class ResumeValidationError(RuntimeError):
    """Raised when a retained run cannot safely be resumed."""


@dataclass(frozen=True)
class ResumePlan:
    """Resolved resume plan for one parent manifest and pipeline."""

    parent_manifest: Mapping[str, Any]
    parent_run_dir: Path
    recalculated_nodes: frozenset[Node]
    frozen_nodes: frozenset[Node]


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
    validate_frozen_outputs(parent_manifest=parent_manifest, parent_run_dir=parent_run_dir, frozen_nodes=frozen)
    return ResumePlan(
        parent_manifest=parent_manifest,
        parent_run_dir=parent_run_dir,
        recalculated_nodes=frozenset(recalculated),
        frozen_nodes=frozenset(frozen),
    )


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
    parent_manifest: Mapping[str, Any],
    parent_run_dir: Path,
    frozen_nodes: Iterable[Node],
) -> None:
    """Validate that frozen node outputs still match their manifest digests."""

    mismatches: list[str] = []
    for node in sorted(frozen_nodes, key=lambda item: str(item.uuid)):
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
        for port in node.outputs:
            record = outputs.get(port.name)
            mismatches.extend(_validate_output_record(label, port.name, record, parent_run_dir))
    if mismatches:
        raise ResumeValidationError(
            "cannot resume because frozen outputs are invalid:\n- {}\nHint: include these nodes in from_=... "
            "to recalculate them.".format("\n- ".join(mismatches))
        )


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


def artifact_ref_from_record(record: Mapping[str, Any], parent_run_dir: Path) -> ArtifactRef:
    """Build an ArtifactRef from a manifest artifact record."""

    ref = record.get("ref")
    if not isinstance(ref, Mapping):
        raise ResumeValidationError("artifact output record is missing `ref`")
    uri = ref.get("uri")
    if not isinstance(uri, str) or not uri:
        raise ResumeValidationError("artifact output record has an invalid uri")
    path = Path(uri)
    if not path.is_absolute():
        path = parent_run_dir / path
    return ArtifactRef(
        key=str(ref["key"]),
        uri=str(path),
        sha256=str(ref["sha256"]),
        size=int(ref["size"]),
        tag=cast(str | None, ref.get("tag")),
    )


def rebase_output_record(record: Mapping[str, Any], parent_run_dir: Path, run_dir: Path | None) -> dict[str, Any]:
    """Return an output record whose artifact URI is meaningful in a child manifest."""

    if record.get("kind") != "artifact":
        return dict(record)
    return m_manifest.artifact_record(artifact_ref_from_record(record, parent_run_dir), run_dir=run_dir)


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


def _validate_output_record(label: str, port: str, record: Any, parent_run_dir: Path) -> list[str]:
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
        ref = artifact_ref_from_record(record, parent_run_dir)
        path = Path(ref.uri)
        if not path.exists():
            return [
                "{}:{} artifact is missing at {}; expected sha256 {}; recalculate with from_='{}'".format(
                    label, port, path, ref.sha256, label
                )
            ]
        actual_size = path.stat().st_size
        actual_sha256 = compute_sha256(path)
        errors = []
        if actual_size != ref.size:
            errors.append(
                "{}:{} artifact size mismatch at {}: expected {}, actual {}; recalculate with from_='{}'".format(
                    label, port, path, ref.size, actual_size, label
                )
            )
        if actual_sha256 != ref.sha256:
            errors.append(
                "{}:{} artifact sha256 mismatch at {}: expected {}, actual {}; recalculate with from_='{}'".format(
                    label, port, path, ref.sha256, actual_sha256, label
                )
            )
        return errors
    if kind == "unfreezable":
        return [
            "{}:{} is unfreezable ({}); recalculate with from_='{}'".format(label, port, record.get("reason"), label)
        ]
    return ["{}:{} has unsupported output kind `{}`; recalculate with from_='{}'".format(label, port, kind, label)]


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
