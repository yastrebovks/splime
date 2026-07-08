"""Run manifest format and persistence helpers."""

from __future__ import annotations

import json
import os
import shutil
import sys
import uuid
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TypeAlias, cast

from spl.core.entities.artifact import ArtifactRef
from spl.core.fingerprint import FINGERPRINT_FORMAT_VERSION, inline_value_sha256

RUN_MANIFEST_SCHEMA_VERSION = 1
RUN_MANIFEST_FILENAME = "manifest.json"
DEFAULT_ON_FAILURE_TTL_SECONDS = 7 * 24 * 60 * 60
TERMINAL_RUN_STATUSES = frozenset({"succeeded", "failed", "cancelled", "interrupted", "stale"})
ACTIVE_RUN_STATUSES = frozenset({"queued", "starting", "preparing_environment", "running", "fetching_object"})
SENSITIVE_INLINE_KEY_FRAGMENTS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "credential",
        "authorization",
        "api_key",
        "apikey",
        "access_key",
        "private_key",
    }
)

KeepPolicy: TypeAlias = bool | Literal["on_failure"]


def utc_now() -> str:
    """Return a stable UTC timestamp for run manifests."""

    return datetime.now(UTC).isoformat()


def utc_after(seconds: float) -> str:
    """Return a stable UTC timestamp ``seconds`` in the future."""

    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()


def new_run_id() -> str:
    """Return a filesystem-safe local run id."""

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return "{}-{}".format(stamp, uuid.uuid4().hex[:12])


def normalize_keep(value: Any) -> KeepPolicy:
    """Normalize public keep values."""

    if type(value) is bool:
        return value
    if value == "on_failure":
        return "on_failure"
    raise ValueError("keep must be False, True, or 'on_failure'")


def keep_to_storage(value: KeepPolicy) -> str:
    """Return a stable database representation for a keep value."""

    if value is True:
        return "true"
    if value is False:
        return "false"
    return value


def keep_from_storage(value: str | None) -> KeepPolicy:
    """Load a keep value from a database text column."""

    if value is None:
        return "on_failure"
    if value == "true":
        return True
    if value == "false":
        return False
    return normalize_keep(value)


def keep_json_value(value: KeepPolicy) -> bool | str:
    """Return the JSON-facing representation for a keep value."""

    return value


def should_retain_terminal(keep: KeepPolicy, status: str) -> bool:
    """Return whether a terminal run state must stay on disk."""

    if keep is True:
        return True
    if keep is False:
        return False
    return status != "succeeded"


def default_runs_home() -> Path:
    """Return the default local retained-runs directory."""

    return Path(os.environ.get("SPL_RUNS_HOME", Path.home() / ".splime" / "runs")).expanduser()


def parse_utc_timestamp(value: Any) -> datetime | None:
    """Parse a manifest timestamp into an aware UTC datetime."""

    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def create_run_dir(run_id: str, runs_home: Path | None = None) -> Path:
    """Create and return an owner-only run directory."""

    home = (runs_home or default_runs_home()).absolute()
    home.mkdir(mode=0o700, parents=True, exist_ok=True)
    _chmod_dir_owner_only(home)
    run_dir = home / run_id
    run_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    _chmod_dir_owner_only(run_dir)
    return run_dir


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Write JSON by replacing the destination with a complete temp file."""

    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _chmod_dir_owner_only(path.parent)
    tmp_path = path.with_name(".{}.{}.tmp".format(path.name, uuid.uuid4().hex))
    try:
        _write_text_owner_only(tmp_path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        tmp_path.replace(path)
        _chmod_file_owner_only(path)
    finally:
        tmp_path.unlink(missing_ok=True)


def read_manifest(path: Path) -> dict[str, Any]:
    """Read a run manifest JSON file."""

    return dict(json.loads(path.read_text(encoding="utf-8")))


def run_dir_size(path: Path) -> int:
    """Return the total size in bytes under a run directory."""

    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except OSError:
            continue
    return total


def manifest_summary(manifest: Mapping[str, Any], *, run_dir: Path | None = None) -> dict[str, Any]:
    """Return list-facing metadata for a retained run manifest."""

    retention_value = manifest.get("retention")
    retention: Mapping[str, Any] = retention_value if isinstance(retention_value, Mapping) else {}
    return {
        "id": manifest.get("run_id"),
        "status": manifest.get("status"),
        "keep": manifest.get("keep"),
        "has_manifest": True,
        "parent_run_id": manifest.get("parent_run_id"),
        "created_at": manifest.get("created_at"),
        "started_at": manifest.get("started_at"),
        "finished_at": manifest.get("finished_at"),
        "retention": dict(retention),
        "expires_at": retention.get("expires_at"),
        "run_dir": None if run_dir is None else str(run_dir),
        "disk_size_bytes": None if run_dir is None else run_dir_size(run_dir),
        "node_runtimes": node_runtime_summary(manifest),
        "edge_adapters": edge_adapter_summary(manifest),
    }


def edge_adapter_summary(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return compact edge adapter and tag records from a manifest."""

    edges = manifest.get("edges")
    nodes = manifest.get("nodes")
    node_labels = _node_labels(nodes if isinstance(nodes, Mapping) else {})
    if not isinstance(edges, list):
        return []
    rows = []
    for edge in edges:
        if not isinstance(edge, Mapping):
            continue
        source = edge.get("source")
        target = edge.get("target")
        artifact = edge.get("artifact")
        adapter = edge.get("adapter")
        if not isinstance(source, Mapping) or not isinstance(target, Mapping):
            continue
        artifact_record = artifact if isinstance(artifact, Mapping) else {}
        save_adapter: Mapping[str, Any] = {}
        load_adapter: Mapping[str, Any] = {}
        if isinstance(adapter, Mapping):
            save_value = adapter.get("save")
            load_value = adapter.get("load")
            save_adapter = save_value if isinstance(save_value, Mapping) else {}
            load_adapter = load_value if isinstance(load_value, Mapping) else {}
        source_node_id = str(source.get("node_id") or "")
        target_node_id = str(target.get("node_id") or "")
        source_port = str(source.get("port") or "")
        target_port = str(target.get("port") or "")
        rows.append(
            {
                "source": _endpoint_label(node_labels, source_node_id, source_port),
                "target": _endpoint_label(node_labels, target_node_id, target_port),
                "source_node_id": source_node_id,
                "source_port": source_port,
                "target_node_id": target_node_id,
                "target_port": target_port,
                "tag": artifact_record.get("tag"),
                "save": _adapter_name(save_adapter),
                "load": _adapter_name(load_adapter),
                "source_level": save_adapter.get("source") or load_adapter.get("source"),
            }
        )
    return sorted(rows, key=lambda item: (item["source"], item["target"]))


def tag_stats_from_manifests(manifests: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate edge artifact tag counts from retained run manifests."""

    tag_edge_counts: dict[str, int] = {}
    tag_run_counts: dict[str, int] = {}
    pair_edge_counts: dict[tuple[str, tuple[str, ...]], int] = {}
    pair_run_counts: dict[tuple[str, tuple[str, ...]], int] = {}
    runs_scanned = 0
    edges_scanned = 0

    for manifest in manifests:
        runs_scanned += 1
        run_tags: set[str] = set()
        run_pairs: set[tuple[str, tuple[str, ...]]] = set()
        for edge in _iter_manifest_edges(manifest):
            tag = _edge_artifact_tag(edge)
            if tag is not None:
                edges_scanned += 1
                tag_edge_counts[tag] = tag_edge_counts.get(tag, 0) + 1
                run_tags.add(tag)
            pair = _edge_tag_pair(edge)
            if pair is not None:
                pair_edge_counts[pair] = pair_edge_counts.get(pair, 0) + 1
                run_pairs.add(pair)
        for tag in run_tags:
            tag_run_counts[tag] = tag_run_counts.get(tag, 0) + 1
        for pair in run_pairs:
            pair_run_counts[pair] = pair_run_counts.get(pair, 0) + 1

    return {
        "runs_scanned": runs_scanned,
        "edges_scanned": edges_scanned,
        "tags": [
            {
                "tag": tag,
                "edge_count": edge_count,
                "run_count": tag_run_counts.get(tag, 0),
            }
            for tag, edge_count in sorted(tag_edge_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        "pairs": [
            {
                "save_tag": save_tag,
                "load_tags": list(load_tags),
                "edge_count": edge_count,
                "run_count": pair_run_counts.get((save_tag, load_tags), 0),
            }
            for (save_tag, load_tags), edge_count in sorted(
                pair_edge_counts.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1]),
            )
        ],
    }


def local_tag_stats(runs_home: Path | None = None) -> dict[str, Any]:
    """Aggregate edge artifact tag counts from local retained run manifests."""

    return tag_stats_from_manifests(_iter_local_run_manifests(runs_home))


def _iter_manifest_edges(manifest: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    edges = manifest.get("edges")
    if not isinstance(edges, list):
        return ()
    return (edge for edge in edges if isinstance(edge, Mapping))


def _edge_artifact_tag(edge: Mapping[str, Any]) -> str | None:
    artifact = edge.get("artifact")
    if isinstance(artifact, Mapping):
        tag = _non_empty_string(artifact.get("tag"))
        if tag is not None:
            return tag
    save = _edge_adapter_half(edge, "save")
    if save is not None:
        return _non_empty_string(save.get("tag"))
    return None


def _edge_tag_pair(edge: Mapping[str, Any]) -> tuple[str, tuple[str, ...]] | None:
    artifact_tag = _edge_artifact_tag(edge)
    save = _edge_adapter_half(edge, "save")
    load = _edge_adapter_half(edge, "load")
    save_tag = _non_empty_string(save.get("tag")) if save is not None else artifact_tag
    load_tags = _edge_load_tags(load, fallback=artifact_tag)
    if save_tag is None or not load_tags:
        return None
    return save_tag, load_tags


def _edge_adapter_half(edge: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    adapter = edge.get("adapter")
    if not isinstance(adapter, Mapping):
        return None
    half = adapter.get(name)
    return half if isinstance(half, Mapping) else None


def _edge_load_tags(load: Mapping[str, Any] | None, *, fallback: str | None) -> tuple[str, ...]:
    if load is None:
        return () if fallback is None else (fallback,)
    accepted_tags = load.get("accepted_tags")
    if isinstance(accepted_tags, list | tuple):
        tags = tuple(sorted({tag for item in accepted_tags if (tag := _non_empty_string(item)) is not None}))
        if tags:
            return tags
    tag = _non_empty_string(load.get("tag"))
    if tag is not None:
        return (tag,)
    return () if fallback is None else (fallback,)


def _non_empty_string(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def node_runtime_summary(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return compact per-node runtime records from a manifest."""

    nodes = manifest.get("nodes")
    if not isinstance(nodes, Mapping):
        return []
    rows = []
    for node_id, raw_record in nodes.items():
        if not isinstance(raw_record, Mapping):
            continue
        runtime = raw_record.get("runtime")
        if not isinstance(runtime, Mapping):
            continue
        rows.append(
            {
                "node_id": str(node_id),
                "alias": raw_record.get("alias"),
                "name": runtime.get("name"),
                "source": runtime.get("source"),
                "config_hash": runtime.get("config_hash"),
            }
        )
    return sorted(rows, key=lambda item: (str(item.get("alias") or ""), item["node_id"]))


def _node_labels(nodes: Mapping[Any, Any]) -> dict[str, str]:
    labels = {}
    for node_id, raw_record in nodes.items():
        node_id_text = str(node_id)
        if not isinstance(raw_record, Mapping):
            labels[node_id_text] = node_id_text
            continue
        labels[node_id_text] = str(raw_record.get("alias") or raw_record.get("name") or node_id_text)
    return labels


def _endpoint_label(node_labels: Mapping[str, str], node_id: str, port: str) -> str:
    label = node_labels.get(node_id) or node_id
    return "{}.{}".format(label, port)


def _adapter_name(adapter: Mapping[str, Any]) -> str | None:
    identity = adapter.get("identity")
    if not isinstance(identity, Mapping):
        return None
    save = identity.get("save")
    load = identity.get("load")
    key = identity.get("key")
    if save and load and save != load:
        return "{} / {}".format(save, load)
    return str(save or load or key) if save or load or key else None


def sanitize_manifest_inline(
    manifest: Mapping[str, Any],
    *,
    include_values: bool = False,
    preview_limit: int = 120,
) -> dict[str, Any]:
    """Return a manifest copy with inline JSON values summarized by default."""

    value = deepcopy(dict(manifest))
    if include_values:
        return value
    return cast(dict[str, Any], _sanitize_inline_value(value, preview_limit=preview_limit))


def sanitize_run_state(
    state: Mapping[str, Any],
    *,
    include_values: bool = False,
    preview_limit: int = 120,
) -> dict[str, Any]:
    """Return an HTTP/CLI-safe run state copy."""

    value = deepcopy(dict(state))
    manifest = value.get("manifest")
    if isinstance(manifest, Mapping):
        value["manifest"] = sanitize_manifest_inline(
            manifest,
            include_values=include_values,
            preview_limit=preview_limit,
        )
    if not include_values:
        input_payload = value.get("input")
        if isinstance(input_payload, Mapping):
            value["input"] = _sanitize_plain_sensitive_value(input_payload, preview_limit=preview_limit)
    return value


def list_local_runs(runs_home: Path | None = None) -> list[dict[str, Any]]:
    """List local retained Deployment runs under the configured runs home."""

    home = (runs_home or default_runs_home()).expanduser()
    if not home.exists():
        return []
    rows = []
    for run_dir in sorted((item for item in home.iterdir() if item.is_dir()), key=lambda item: item.name):
        manifest_path = run_dir / RUN_MANIFEST_FILENAME
        if manifest_path.exists():
            try:
                summary = manifest_summary(read_manifest(manifest_path), run_dir=run_dir)
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                summary = _legacy_run_summary(run_dir)
        else:
            summary = _legacy_run_summary(run_dir)
        rows.append(summary)
    return sorted(rows, key=lambda item: str(item.get("created_at") or item.get("id") or ""), reverse=True)


def show_local_run(
    run_id: str,
    *,
    runs_home: Path | None = None,
    include_inline_values: bool = False,
) -> dict[str, Any]:
    """Return one local retained run manifest with safe inline-value defaults."""

    run_dir = _local_run_dir(run_id, runs_home)
    manifest_path = run_dir / RUN_MANIFEST_FILENAME
    if not manifest_path.exists():
        raise FileNotFoundError("retained run manifest not found: {}".format(manifest_path))
    manifest = read_manifest(manifest_path)
    return {
        **manifest_summary(manifest, run_dir=run_dir),
        "manifest": sanitize_manifest_inline(manifest, include_values=include_inline_values),
    }


def _iter_local_run_manifests(runs_home: Path | None = None) -> Iterable[Mapping[str, Any]]:
    home = (runs_home or default_runs_home()).expanduser()
    if not home.exists():
        return
    for run_dir in sorted((item for item in home.iterdir() if item.is_dir()), key=lambda item: item.name):
        manifest_path = run_dir / RUN_MANIFEST_FILENAME
        if not manifest_path.exists():
            continue
        try:
            yield read_manifest(manifest_path)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            continue


def prune_local_runs(
    *,
    run_id: str | None = None,
    statuses: Iterable[str] | None = None,
    older_than_seconds: float | None = None,
    dry_run: bool = False,
    runs_home: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Prune local retained Deployment run directories."""

    home = (runs_home or default_runs_home()).expanduser()
    if not home.exists():
        return {"dry_run": dry_run, "count": 0, "pruned": [], "skipped_active": [], "candidates": []}
    status_filter = set(statuses or [])
    checked_at = now or datetime.now(UTC)
    candidates: list[dict[str, Any]] = []
    skipped_active: list[dict[str, Any]] = []
    for summary in list_local_runs(home):
        item_id = str(summary.get("id") or "")
        if run_id is not None and item_id != run_id:
            continue
        status = str(summary.get("status") or "")
        if status in ACTIVE_RUN_STATUSES:
            skipped_active.append(summary)
            continue
        if _summary_matches_prune(summary, status_filter, older_than_seconds, checked_at, explicit=run_id is not None):
            candidates.append(summary)
    if not dry_run:
        for item in candidates:
            run_dir = item.get("run_dir")
            if isinstance(run_dir, str):
                shutil.rmtree(run_dir, ignore_errors=True)
    return {
        "dry_run": dry_run,
        "count": len(candidates),
        "pruned": candidates,
        "skipped_active": skipped_active,
        "candidates": candidates if dry_run else [],
    }


def build_initial_manifest(
    *,
    run_id: str,
    keep: KeepPolicy,
    pipeline_name: str | None,
    parent_run_id: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Build the top-level manifest structure."""

    now = created_at or utc_now()
    return {
        "schema_version": RUN_MANIFEST_SCHEMA_VERSION,
        "run_id": run_id,
        "parent_run_id": parent_run_id,
        "status": "running",
        "keep": keep_json_value(keep),
        "created_at": now,
        "started_at": now,
        "finished_at": None,
        "pipeline": {
            "name": pipeline_name,
            "entrypoint": pipeline_name,
            "object_version_id": None,
            "content_hash": None,
        },
        "inputs": {},
        "nodes": {},
        "edges": [],
        "retention": retention_record(keep, "running"),
    }


def retention_record(keep: KeepPolicy, status: str) -> dict[str, Any]:
    """Return the retention block for a manifest state."""

    if keep is True:
        return {"class": "keep", "expires_at": None}
    if keep is False:
        return {"class": "transient", "expires_at": None}
    if status == "succeeded":
        return {"class": "on_failure", "expires_at": None}
    if status in {"failed", "cancelled", "interrupted"}:
        return {"class": "on_failure", "expires_at": utc_after(DEFAULT_ON_FAILURE_TTL_SECONDS)}
    return {"class": "on_failure", "expires_at": None}


def node_record(
    *,
    node_id: str,
    alias: str | None,
    kind: str,
    name: str,
    status: str,
    fingerprint_sha256: str | None,
    runtime: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Any] | None = None,
    outputs: Mapping[str, Any] | None = None,
    adapters: Mapping[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    """Build a manifest node record."""

    return {
        "id": node_id,
        "alias": alias,
        "kind": kind,
        "name": name,
        "status": status,
        "fingerprint": {
            "version": FINGERPRINT_FORMAT_VERSION,
            "sha256": fingerprint_sha256,
        },
        "runtime": dict(runtime or native_runtime_record()),
        "inputs": dict(inputs or {}),
        "outputs": dict(outputs or {}),
        "adapters": dict(adapters or {}),
        "error": error,
    }


def native_runtime_record() -> dict[str, Any]:
    """Return the local native runtime record for manifest v1."""

    return {
        "name": "native",
        "source": "default",
        "config_hash": None,
        "resolved": {"python": sys.executable},
    }


def adapter_record(identity: Mapping[str, Any], source: str) -> dict[str, Any]:
    """Return the manifest representation of a resolved adapter."""

    return {
        "identity": dict(identity),
        "tag": identity.get("tag"),
        "accepted_tags": list(identity.get("accepted_tags") or []),
        "source": source,
    }


def edge_record(
    *,
    source_node_id: str,
    source_port: str,
    target_node_id: str,
    target_port: str,
    artifact: Mapping[str, Any],
    adapter: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build a manifest edge record."""

    return {
        "source": {"node_id": source_node_id, "port": source_port},
        "target": {"node_id": target_node_id, "port": target_port},
        "artifact": dict(artifact),
        "adapter": None if adapter is None else dict(adapter),
    }


def edge_adapter_record(adapter: Mapping[str, Any]) -> dict[str, Any]:
    """Return save/load adapter blocks for a runtime adapter."""

    return {"save": dict(adapter), "load": dict(adapter)}


def artifact_ref_record(ref: ArtifactRef, *, run_dir: Path | None = None) -> dict[str, Any]:
    """Return a manifest artifact reference."""

    return {
        "key": ref.key,
        "tag": ref.tag,
        "uri": _manifest_uri(Path(ref.uri), run_dir),
        "sha256": ref.sha256,
        "size": ref.size,
    }


def artifact_record(ref: ArtifactRef, *, run_dir: Path | None = None) -> dict[str, Any]:
    """Return an artifact value record."""

    return {
        "kind": "artifact",
        "tag": ref.tag,
        "sha256": ref.sha256,
        "ref": artifact_ref_record(ref, run_dir=run_dir),
    }


def json_record(value: Any) -> dict[str, Any]:
    """Return an inline JSON-native value record."""

    return {
        "kind": "json",
        "tag": "json",
        "value": value,
        "sha256": inline_value_sha256(value),
    }


def unfreezable_record(reason: str) -> dict[str, Any]:
    """Return an output record that cannot be used for resume freezing."""

    return {"kind": "unfreezable", "reason": reason}


@dataclass
class RunManifestWriter:
    """Incrementally maintain and atomically persist one run manifest."""

    path: Path | None
    data: dict[str, Any]

    @classmethod
    def create(
        cls,
        *,
        run_dir: Path,
        run_id: str,
        keep: KeepPolicy,
        pipeline_name: str | None,
        parent_run_id: str | None = None,
    ) -> "RunManifestWriter":
        """Create a writer and write the initial manifest."""

        writer = cls(
            path=run_dir / RUN_MANIFEST_FILENAME,
            data=build_initial_manifest(
                run_id=run_id,
                keep=keep,
                pipeline_name=pipeline_name,
                parent_run_id=parent_run_id,
            ),
        )
        writer.write()
        return writer

    @classmethod
    def create_deferred(
        cls,
        *,
        run_id: str,
        keep: KeepPolicy,
        pipeline_name: str | None,
        parent_run_id: str | None = None,
    ) -> "RunManifestWriter":
        """Create an in-memory writer that can be materialized later."""

        return cls(
            path=None,
            data=build_initial_manifest(
                run_id=run_id,
                keep=keep,
                pipeline_name=pipeline_name,
                parent_run_id=parent_run_id,
            ),
        )

    def materialize(self, run_dir: Path) -> None:
        """Attach this writer to a run directory and persist current data."""

        manifest_path = run_dir / RUN_MANIFEST_FILENAME
        if self.path is None:
            self.path = manifest_path
        elif self.path != manifest_path:
            raise ValueError("run manifest writer is already materialized at {}".format(self.path))
        self.write()

    def write(self) -> None:
        """Persist the current manifest atomically."""

        if self.path is None:
            return
        atomic_write_json(self.path, self.data)

    def set_node(self, record: Mapping[str, Any]) -> None:
        """Insert or replace one node record and persist the manifest."""

        self.data["nodes"][str(record["id"])] = dict(record)
        self.write()

    def update_node(self, node_id: str, **changes: Any) -> None:
        """Merge changes into one node record and persist the manifest."""

        node = self.data["nodes"][node_id]
        node.update(changes)
        self.write()

    def set_node_output(self, node_id: str, port: str, output: Mapping[str, Any]) -> None:
        """Set one node output record and persist the manifest."""

        self.data["nodes"][node_id]["outputs"][port] = dict(output)
        self.write()

    def set_node_adapter(self, node_id: str, port: str, adapter: Mapping[str, Any]) -> None:
        """Set one node adapter record and persist the manifest."""

        self.data["nodes"][node_id]["adapters"][port] = dict(adapter)
        self.write()

    def add_edge(self, edge: Mapping[str, Any]) -> None:
        """Add or replace an edge record and persist the manifest."""

        key = (
            edge["source"]["node_id"],
            edge["source"]["port"],
            edge["target"]["node_id"],
            edge["target"]["port"],
        )
        edges = [
            item
            for item in self.data["edges"]
            if (
                item["source"]["node_id"],
                item["source"]["port"],
                item["target"]["node_id"],
                item["target"]["port"],
            )
            != key
        ]
        edges.append(dict(edge))
        self.data["edges"] = sorted(edges, key=lambda item: json.dumps(item, sort_keys=True))
        self.write()

    def finish(self, *, status: str, error: str | None = None) -> None:
        """Write the terminal manifest status."""

        self.data["status"] = status
        self.data["finished_at"] = utc_now()
        self.data["retention"] = retention_record(normalize_keep(self.data["keep"]), status)
        if error is not None:
            self.data["error"] = error
        self.write()


def _manifest_uri(path: Path, run_dir: Path | None) -> str:
    if run_dir is None:
        return str(path)
    try:
        return str(path.relative_to(run_dir))
    except ValueError:
        return str(path)


def _write_text_owner_only(path: Path, text: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)


def _chmod_dir_owner_only(path: Path) -> None:
    try:
        path.chmod(0o700)
    except OSError:
        pass


def _chmod_file_owner_only(path: Path) -> None:
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _sanitize_inline_value(value: Any, *, preview_limit: int, key_path: tuple[str, ...] = ()) -> Any:
    if isinstance(value, dict):
        if value.get("kind") == "json" and "value" in value:
            raw = value["value"]
            text = json.dumps(raw, ensure_ascii=False, sort_keys=True)
            result = {
                key: _sanitize_inline_value(item, preview_limit=preview_limit, key_path=(*key_path, str(key)))
                for key, item in value.items()
            }
            result.pop("value", None)
            if _inline_value_looks_sensitive(raw, key_path):
                result["value_preview"] = "<omitted>"
                result["value_preview_omitted"] = True
            else:
                result["value_preview"] = text[:preview_limit] + ("..." if len(text) > preview_limit else "")
            result["value_size_bytes"] = len(text.encode("utf-8"))
            result["value_omitted"] = True
            return result
        return {
            key: _sanitize_inline_value(item, preview_limit=preview_limit, key_path=(*key_path, str(key)))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_inline_value(item, preview_limit=preview_limit, key_path=key_path) for item in value]
    return value


def _inline_value_looks_sensitive(value: Any, key_path: tuple[str, ...]) -> bool:
    if any(_is_sensitive_key(key) for key in key_path):
        return True
    if isinstance(value, Mapping):
        return any(
            _is_sensitive_key(str(key)) or _inline_value_looks_sensitive(item, ()) for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_inline_value_looks_sensitive(item, ()) for item in value)
    return False


def _is_sensitive_key(key: str) -> bool:
    normalized = key.casefold().replace("-", "_").replace(".", "_")
    return any(fragment in normalized for fragment in SENSITIVE_INLINE_KEY_FRAGMENTS)


def _sanitize_plain_sensitive_value(
    value: Any,
    *,
    preview_limit: int,
    key_path: tuple[str, ...] = (),
) -> Any:
    if _inline_value_looks_sensitive(value, key_path):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        if any(_is_sensitive_key(key) for key in key_path):
            return {
                "value_preview": "<omitted>",
                "value_preview_omitted": True,
                "value_size_bytes": len(text.encode("utf-8")),
                "value_omitted": True,
                "sha256": inline_value_sha256(value),
            }
    if isinstance(value, Mapping):
        return {
            str(key): _sanitize_plain_sensitive_value(item, preview_limit=preview_limit, key_path=(*key_path, str(key)))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_plain_sensitive_value(item, preview_limit=preview_limit, key_path=key_path) for item in value]
    return value


def _local_run_dir(run_id: str, runs_home: Path | None) -> Path:
    candidate = Path(run_id).expanduser()
    if candidate.is_dir():
        return candidate
    return (runs_home or default_runs_home()).expanduser() / run_id


def _legacy_run_summary(run_dir: Path) -> dict[str, Any]:
    try:
        modified = datetime.fromtimestamp(run_dir.stat().st_mtime, UTC).isoformat()
    except OSError:
        modified = None
    return {
        "id": run_dir.name,
        "status": "legacy",
        "keep": None,
        "has_manifest": False,
        "parent_run_id": None,
        "created_at": modified,
        "started_at": None,
        "finished_at": None,
        "retention": {"class": "legacy", "expires_at": None},
        "expires_at": None,
        "run_dir": str(run_dir),
        "disk_size_bytes": run_dir_size(run_dir),
    }


def _summary_matches_prune(
    summary: Mapping[str, Any],
    status_filter: set[str],
    older_than_seconds: float | None,
    now: datetime,
    *,
    explicit: bool,
) -> bool:
    if explicit:
        return True
    status = str(summary.get("status") or "")
    if status_filter and status not in status_filter:
        return False
    if older_than_seconds is not None:
        timestamp = parse_utc_timestamp(summary.get("finished_at")) or parse_utc_timestamp(summary.get("created_at"))
        if timestamp is None:
            return False
        return (now - timestamp).total_seconds() >= older_than_seconds
    if status_filter:
        return True
    expires_at = parse_utc_timestamp(summary.get("expires_at"))
    if expires_at is not None:
        return now >= expires_at
    if not bool(summary.get("has_manifest")):
        timestamp = parse_utc_timestamp(summary.get("created_at"))
        return timestamp is not None and (now - timestamp).total_seconds() >= DEFAULT_ON_FAILURE_TTL_SECONDS
    return False
