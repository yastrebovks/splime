import ast
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from enum import StrEnum
from itertools import chain
from operator import itemgetter
from pathlib import Path
from typing import Any, Generator, TypeVar, cast

import yaml

import spl.core.entities.adapter as m_adapter
import spl.core.entities.artifact as m_artifact
import spl.core.entities.distribution as m_distribution
import spl.core.entities.node as m_node
import spl.core.entities.node_function as m_node_function
import spl.core.entities.scalar as m_scalar
from spl.core._graph import canonical_uuid_key, node_sort_key
from spl.core.entities.adapter import (
    BUILTIN_JSON_ADAPTER,
    JSON_ADAPTER_FORMAT,
    JSON_NATIVE_TYPES,
    Adapter,
    LoadAdapter,
    RuntimeAdapter,
    SaveAdapter,
    SplitLoadAdapter,
    SplitSaveAdapter,
    make_key,
)
from spl.core.entities.node import (
    FormattedOutputRef,
    Node,
    NodeInputRef,
    NodeOutputRef,
)
from spl.core.ir.common import DBase
from spl.core.ir.parse import _branch, ir_parse
from spl.core.ir.unparse import ir_unparse


def _as_node_output_ref(value: Any) -> NodeOutputRef | None:
    if isinstance(value, FormattedOutputRef):
        return value.out_ref
    if isinstance(value, NodeOutputRef):
        return value
    return None


def _try_canonical_uuid(value: Any) -> str | None:
    try:
        return canonical_uuid_key(value)
    except (TypeError, ValueError):
        return None


def _counting_sort_graph_edges(edges: list[tuple[int, int]], component: int, node_count: int) -> list[tuple[int, int]]:
    counts = [0] * node_count
    for edge in edges:
        counts[edge[component]] += 1
    offsets: list[int] = []
    offset = 0
    for count in counts:
        offsets.append(offset)
        offset += count

    ordered = [(0, 0)] * len(edges)
    for edge in edges:
        bucket = edge[component]
        ordered[offsets[bucket]] = edge
        offsets[bucket] += 1
    return ordered


def _find_graph_cycle(node_ids: list[str], edges: list[tuple[str, str]]) -> list[str] | None:
    """Return one canonical cycle using a linear pass after node ordering."""

    # Canonicalizing V node identities costs O(V log V). Dense ranks then let
    # counting sort and the graph traversal remain O(V + E), independent of
    # the hash-derived iteration order of Pipeline.links.
    ordered_node_ids = sorted(set(node_ids))
    rank_by_node_id = {node_id: rank for rank, node_id in enumerate(ordered_node_ids)}
    ranked_edges = [
        (rank_by_node_id[source_id], rank_by_node_id[target_id])
        for source_id, target_id in edges
        if source_id in rank_by_node_id and target_id in rank_by_node_id
    ]
    if ranked_edges:
        ranked_edges = _counting_sort_graph_edges(ranked_edges, 1, len(ordered_node_ids))
        ranked_edges = _counting_sort_graph_edges(ranked_edges, 0, len(ordered_node_ids))
    adjacency: list[list[int]] = [[] for _ in ordered_node_ids]
    for source_rank, target_rank in ranked_edges:
        adjacency[source_rank].append(target_rank)

    state = [0] * len(ordered_node_ids)
    active_path: list[int] = []
    active_index: dict[int, int] = {}
    for start_rank in range(len(ordered_node_ids)):
        if state[start_rank] != 0:
            continue
        state[start_rank] = 1
        active_index[start_rank] = len(active_path)
        active_path.append(start_rank)
        frames: list[tuple[int, int]] = [(start_rank, 0)]
        while frames:
            current_rank, target_index = frames[-1]
            targets = adjacency[current_rank]
            if target_index >= len(targets):
                frames.pop()
                active_path.pop()
                active_index.pop(current_rank)
                state[current_rank] = 2
                continue

            target_rank = targets[target_index]
            frames[-1] = (current_rank, target_index + 1)
            target_state = state[target_rank]
            if target_state == 0:
                state[target_rank] = 1
                active_index[target_rank] = len(active_path)
                active_path.append(target_rank)
                frames.append((target_rank, 0))
            elif target_state == 1:
                cycle_ranks = [*active_path[active_index[target_rank] :], target_rank]
                return [ordered_node_ids[rank] for rank in cycle_ranks]
    return None


class AdapterResolutionSource(StrEnum):
    """Source level that selected an adapter for an edge."""

    PORT_DEFAULT = "port-default"
    PIPELINE = "pipeline"
    EDGE = "edge"
    RUN_OVERRIDE = "run-override"


@dataclass(frozen=True)
class AdapterResolution:
    """Resolved adapter and the source level that selected it."""

    adapter: RuntimeAdapter
    source: AdapterResolutionSource


@dataclass(frozen=True)
class SaveAdapterResolution:
    """Resolved save half and the source level that selected it."""

    adapter: SaveAdapter
    source: AdapterResolutionSource


@dataclass(frozen=True)
class LoadAdapterResolution:
    """Resolved load half and the source level that selected it."""

    adapter: LoadAdapter
    source: AdapterResolutionSource


_HalfT = TypeVar("_HalfT")


@dataclass(frozen=True)
class Pipeline:
    name: str | None = None
    nodes: set[Node] = field(default_factory=set)
    links: set[tuple[NodeInputRef, Any]] = field(default_factory=set)
    aliases: dict[str, Node] = field(default_factory=dict)
    adapters: dict[str, Adapter] = field(default_factory=dict)
    tags: dict[str, dict[str, str]] = field(default_factory=dict)
    save_adapters: dict[str, SplitSaveAdapter] = field(default_factory=dict)
    load_adapters: dict[str, SplitLoadAdapter] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._validate_graph_semantics()

    def __hash__(self) -> int:
        return hash(
            (
                tuple(sorted(map(hash, self.nodes))),
                tuple(sorted(map(hash, self.links))),
                tuple(sorted([(key, hash(adapter)) for key, adapter in self.adapters.items()])),
                tuple(sorted((node_id, tuple(sorted(tags.items()))) for node_id, tags in self.tags.items())),
                tuple(sorted((key, hash(adapter)) for key, adapter in self.save_adapters.items())),
                tuple(sorted((key, hash(adapter)) for key, adapter in self.load_adapters.items())),
            )
        )

    def __or__(self, other: "Pipeline") -> "Pipeline":
        nodes = set.union(self.nodes, other.nodes)
        links = set.union(self.links, other.links)
        aliases = self._merge_aliases(other)
        adapters = self._merge_adapters(other)
        save_adapters = self._merge_half_adapters(self.save_adapters, other.save_adapters)
        load_adapters = self._merge_half_adapters(self.load_adapters, other.load_adapters)
        tags = self._merge_tags(other)
        return Pipeline(
            nodes=nodes,
            links=links,
            aliases=aliases,
            adapters=adapters,
            tags=tags,
            save_adapters=save_adapters,
            load_adapters=load_adapters,
        )._validate_consistency()

    def add_link(self, node_input_ref: NodeInputRef, value: Any) -> "Pipeline":
        if (node := node_input_ref.node) not in self.nodes:
            raise ValueError("pipeline does not contain input node ({})".format(node))
        if node_input_ref.port not in node.inputs:
            raise ValueError("pipeline input ref does not belong to node ({})".format(node_input_ref))

        if (output_ref := _as_node_output_ref(value)) is not None:
            if (node := output_ref.node) not in self.nodes:
                raise ValueError("pipeline does not contain output node ({})".format(node))
            if output_ref.port not in node.outputs:
                raise ValueError("pipeline output ref does not belong to node ({})".format(output_ref))

        for existing_ref, existing_value in self.links:
            if existing_ref == node_input_ref and existing_value != value:
                raise ValueError("pipeline input `{}` is already linked".format(node_input_ref))

        return Pipeline(
            nodes=self.nodes,
            links={*self.links, (node_input_ref, value)},
            aliases=self.aliases,
            adapters=self.adapters,
            tags=self.tags,
            save_adapters=self.save_adapters,
            load_adapters=self.load_adapters,
        )._validate_consistency()

    def add_alias(self, node: Node, name: str) -> "Pipeline":
        if not isinstance(name, str) or not name:
            raise ValueError("pipeline alias name must be a non-empty string")
        if node not in self.nodes:
            raise ValueError("pipeline alias points to unknown node ({})".format(node))
        if name in self.aliases and self.aliases[name] != node:
            raise ValueError("pipeline alias `{}` already points to another node".format(name))
        return replace(self, aliases={**self.aliases, name: node})._validate_consistency()

    def with_node_tag(self, node: Node | str, name: str, value: str) -> "Pipeline":
        """Return a pipeline with one additive tag assigned to a node."""

        resolved = self._resolve_tagged_node(node)
        if not isinstance(name, str) or not name:
            raise ValueError("pipeline node tag name must be a non-empty string")
        if not isinstance(value, str) or not value:
            raise ValueError("pipeline node tag value must be a non-empty string")
        node_id = str(resolved.uuid)
        node_tags = {**self.tags.get(node_id, {}), name: value}
        return replace(self, tags={**self.tags, node_id: node_tags})._validate_consistency()

    def with_node_runtime(self, node: Node | str, runtime: str) -> "Pipeline":
        """Return a pipeline with the ``runtime`` tag assigned to a node."""

        return self.with_node_tag(node, "runtime", runtime)

    def add_adapter(
        self,
        py_type: type[Any],
        format: str,
        *,
        save: Callable[..., Any],
        load: Callable[..., Any],
        distributions: tuple[Any, ...] = (),
    ) -> "Pipeline":
        key = make_key(py_type, format)
        adapter = Adapter(key=key, save=save, load=load, py_type=py_type, format=format, distributions=distributions)
        if key in self.save_adapters or key in self.load_adapters:
            raise ValueError("pipeline adapter conflict: `{}`".format(key))
        if key in self.adapters and self.adapters[key] != adapter:
            raise ValueError("pipeline adapter conflict: `{}`".format(key))
        return replace(self, adapters={**self.adapters, key: adapter})._validate_consistency()

    @staticmethod
    def _resolve_registered_half(
        registered: Mapping[str, _HalfT],
        *,
        py_type: type[Any] | None = None,
        format: str | None = None,
        key: str | None = None,
    ) -> _HalfT | None:
        if key is not None and (py_type is not None or format is not None):
            raise ValueError("pipeline adapter lookup accepts key or python type and format")
        if key is None:
            if py_type is None:
                raise ValueError("pipeline adapter lookup requires key or python type")
            if format is not None:
                key = make_key(py_type, format)
            else:
                prefix = "{}.{}@".format(py_type.__module__, py_type.__qualname__)
                adapters = [
                    adapter for adapter_key, adapter in sorted(registered.items()) if adapter_key.startswith(prefix)
                ]
                if len(adapters) > 1:
                    raise ValueError("pipeline adapter lookup is ambiguous for python type ({})".format(py_type))
                return adapters[0] if adapters else None
        if not isinstance(key, str):
            raise TypeError("pipeline adapter key must be a string")
        if not key:
            raise ValueError("pipeline adapter key must be a non-empty string")
        return registered.get(key)

    def _resolve_registered_adapter(
        self, *, py_type: type[Any] | None = None, format: str | None = None, key: str | None = None
    ) -> Adapter | None:
        return self._resolve_registered_half(self.adapters, py_type=py_type, format=format, key=key)

    def resolve_save_adapter(
        self, *, py_type: type[Any] | None = None, format: str | None = None, key: str | None = None
    ) -> SaveAdapter | None:
        """Resolve one registered save half without requiring a matching load key."""

        registered: dict[str, SaveAdapter] = {**self.adapters, **self.save_adapters}
        return self._resolve_registered_half(registered, py_type=py_type, format=format, key=key)

    def resolve_load_adapter(
        self, *, py_type: type[Any] | None = None, format: str | None = None, key: str | None = None
    ) -> LoadAdapter | None:
        """Resolve one registered load half without requiring a matching save key."""

        registered: dict[str, LoadAdapter] = {**self.adapters, **self.load_adapters}
        return self._resolve_registered_half(registered, py_type=py_type, format=format, key=key)

    def resolve_load_adapter_by_type_name(self, *, type_name: str, format: str | None = None) -> LoadAdapter | None:
        """Resolve a load half from a serialized Python type hint."""

        if not isinstance(type_name, str) or not type_name:
            raise ValueError("adapter resolution type name must be a non-empty string")
        if format is not None and (not isinstance(format, str) or not format):
            raise ValueError("adapter resolution format must be a non-empty string")
        registered: dict[str, LoadAdapter] = {**self.adapters, **self.load_adapters}
        canonical = type_name if "." in type_name else "builtins.{}".format(type_name)
        candidates = [
            adapter
            for key, adapter in sorted(registered.items())
            if key.rpartition("@")[0] == canonical and (format is None or key.rpartition("@")[2] == format)
        ]
        if not candidates and "." not in type_name:
            candidates = [
                adapter
                for key, adapter in sorted(registered.items())
                if key.rpartition("@")[0].endswith(".{}".format(type_name))
                and (format is None or key.rpartition("@")[2] == format)
            ]
        if len(candidates) > 1:
            raise ValueError("pipeline load adapter lookup is ambiguous for Python type hint `{}`".format(type_name))
        return candidates[0] if candidates else None

    def resolve_load_adapter_by_format(self, *, format: str) -> LoadAdapter | None:
        """Resolve the sole load half for a format when a target type is unavailable."""

        if not isinstance(format, str) or not format:
            raise ValueError("adapter resolution format must be a non-empty string")
        registered: dict[str, LoadAdapter] = {**self.adapters, **self.load_adapters}
        candidates = [(key, adapter) for key, adapter in sorted(registered.items()) if key.rpartition("@")[2] == format]
        if len(candidates) > 1:
            raise ValueError(
                "pipeline load adapter lookup is ambiguous for format `{}`; candidates: {}".format(
                    format, ", ".join("`{}`".format(key) for key, _ in candidates)
                )
            )
        return candidates[0][1] if candidates else None

    def resolve_adapter(
        self, *, py_type: type[Any] | None = None, format: str | None = None, key: str | None = None
    ) -> Adapter | None:
        """Return a registered pipeline adapter using the legacy lookup contract."""

        return self._resolve_registered_adapter(py_type=py_type, format=format, key=key)

    def resolve_adapter_binding(
        self,
        *,
        py_type: type[Any],
        format: str | None = None,
        run_override: RuntimeAdapter | None = None,
    ) -> AdapterResolution | None:
        """Resolve the logical adapter for an edge and report its source level."""

        if not isinstance(py_type, type):
            raise TypeError("adapter resolution python type must be a type")
        if format is not None and (not isinstance(format, str) or not format):
            raise ValueError("adapter resolution format must be a non-empty string")

        resolution: AdapterResolution | None = None
        if py_type in JSON_NATIVE_TYPES:
            resolution = AdapterResolution(BUILTIN_JSON_ADAPTER, AdapterResolutionSource.PORT_DEFAULT)

        if format is None:
            if py_type not in JSON_NATIVE_TYPES:
                adapter = self._resolve_registered_adapter(py_type=py_type)
                if adapter is not None:
                    resolution = AdapterResolution(adapter, AdapterResolutionSource.PIPELINE)
        else:
            adapter = self._resolve_registered_adapter(py_type=py_type, format=format)
            if adapter is not None:
                resolution = AdapterResolution(adapter, AdapterResolutionSource.EDGE)
            elif format == JSON_ADAPTER_FORMAT and py_type in JSON_NATIVE_TYPES:
                resolution = AdapterResolution(BUILTIN_JSON_ADAPTER, AdapterResolutionSource.EDGE)
            else:
                resolution = None

        if run_override is not None:
            resolution = AdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE)
        return resolution

    def resolve_save_adapter_binding(
        self,
        *,
        py_type: type[Any],
        format: str | None = None,
        run_override: RuntimeAdapter | None = None,
    ) -> SaveAdapterResolution | None:
        """Resolve the save half for a producing value."""

        self._validate_resolution_request(py_type, format)
        resolution: SaveAdapterResolution | None = None
        if py_type in JSON_NATIVE_TYPES:
            resolution = SaveAdapterResolution(BUILTIN_JSON_ADAPTER, AdapterResolutionSource.PORT_DEFAULT)
        if format is None:
            if py_type not in JSON_NATIVE_TYPES:
                adapter = self.resolve_save_adapter(py_type=py_type)
                if adapter is not None:
                    resolution = SaveAdapterResolution(adapter, AdapterResolutionSource.PIPELINE)
        else:
            adapter = self.resolve_save_adapter(py_type=py_type, format=format)
            if adapter is not None:
                resolution = SaveAdapterResolution(adapter, AdapterResolutionSource.EDGE)
            elif format == JSON_ADAPTER_FORMAT and py_type in JSON_NATIVE_TYPES:
                resolution = SaveAdapterResolution(BUILTIN_JSON_ADAPTER, AdapterResolutionSource.EDGE)
            else:
                resolution = None
        if run_override is not None:
            resolution = SaveAdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE)
        return resolution

    def resolve_load_adapter_binding(
        self,
        *,
        py_type: type[Any],
        format: str | None = None,
        run_override: RuntimeAdapter | None = None,
    ) -> LoadAdapterResolution | None:
        """Resolve the load half for a consuming port."""

        self._validate_resolution_request(py_type, format)
        resolution: LoadAdapterResolution | None = None
        if py_type in JSON_NATIVE_TYPES:
            resolution = LoadAdapterResolution(BUILTIN_JSON_ADAPTER, AdapterResolutionSource.PORT_DEFAULT)
        if format is None:
            if py_type not in JSON_NATIVE_TYPES:
                adapter = self.resolve_load_adapter(py_type=py_type)
                if adapter is not None:
                    resolution = LoadAdapterResolution(adapter, AdapterResolutionSource.PIPELINE)
        else:
            adapter = self.resolve_load_adapter(py_type=py_type, format=format)
            if adapter is not None:
                resolution = LoadAdapterResolution(adapter, AdapterResolutionSource.EDGE)
            elif format == JSON_ADAPTER_FORMAT and py_type in JSON_NATIVE_TYPES:
                resolution = LoadAdapterResolution(BUILTIN_JSON_ADAPTER, AdapterResolutionSource.EDGE)
            else:
                resolution = None
        if run_override is not None:
            resolution = LoadAdapterResolution(run_override, AdapterResolutionSource.RUN_OVERRIDE)
        return resolution

    def resolve_load_adapter_binding_by_type_name(
        self, *, type_name: str, format: str | None = None
    ) -> LoadAdapterResolution | None:
        """Resolve a load half from a serialized port type name."""

        adapter = self.resolve_load_adapter_by_type_name(type_name=type_name, format=format)
        if adapter is None:
            return None
        source = AdapterResolutionSource.EDGE if format is not None else AdapterResolutionSource.PIPELINE
        return LoadAdapterResolution(adapter, source)

    def resolve_load_adapter_binding_by_format(self, *, format: str) -> LoadAdapterResolution | None:
        """Resolve the sole edge load half when its target type is unavailable."""

        adapter = self.resolve_load_adapter_by_format(format=format)
        if adapter is None:
            return None
        return LoadAdapterResolution(adapter, AdapterResolutionSource.EDGE)

    @staticmethod
    def _validate_resolution_request(py_type: type[Any], format: str | None) -> None:
        if not isinstance(py_type, type):
            raise TypeError("adapter resolution python type must be a type")
        if format is not None and (not isinstance(format, str) or not format):
            raise ValueError("adapter resolution format must be a non-empty string")

    def get_free_inputs(self) -> list[NodeInputRef]:
        return list(
            {NodeInputRef(node, port) for node in self.nodes for port in node.inputs}
            - set(map(itemgetter(0), self.links))
        )

    def get_unbound_inputs(self) -> list[NodeInputRef]:
        return list(
            {NodeInputRef(node, port) for node in self.nodes for port in node.inputs if port.default is None}
            - set(map(itemgetter(0), self.links))
        )

    def get_outputs(self) -> list[NodeOutputRef]:
        return list({NodeOutputRef(node, port) for node in self.nodes for port in node.outputs})

    def get_node_by_alias(self, name: str) -> Node:
        return self.aliases[name]

    def _merge_aliases(self, other: "Pipeline") -> dict[str, Node]:
        aliases = dict(self.aliases)
        for name, node in other.aliases.items():
            if name in aliases and aliases[name] != node:
                raise ValueError("pipeline alias conflict: `{}`".format(name))
            aliases[name] = node
        return aliases

    def _merge_adapters(self, other: "Pipeline") -> dict[str, Adapter]:
        adapters = dict(self.adapters)
        for key, adapter in other.adapters.items():
            if key in adapters and adapters[key] != adapter:
                raise ValueError("pipeline adapter conflict: `{}`".format(key))
            adapters[key] = adapter
        return adapters

    @staticmethod
    def _merge_half_adapters(left: Mapping[str, _HalfT], right: Mapping[str, _HalfT]) -> dict[str, _HalfT]:
        adapters = dict(left)
        for key, adapter in right.items():
            if key in adapters and adapters[key] != adapter:
                raise ValueError("pipeline adapter conflict: `{}`".format(key))
            adapters[key] = adapter
        return adapters

    def _merge_tags(self, other: "Pipeline") -> dict[str, dict[str, str]]:
        tags = {node_id: dict(node_tags) for node_id, node_tags in self.tags.items()}
        for node_id, node_tags in other.tags.items():
            merged = tags.setdefault(node_id, {})
            for name, value in node_tags.items():
                if name in merged and merged[name] != value:
                    raise ValueError("pipeline node tag conflict: `{}` for node `{}`".format(name, node_id))
                merged[name] = value
        return tags

    def _resolve_tagged_node(self, node: Node | str) -> Node:
        if isinstance(node, Node):
            if node not in self.nodes:
                raise ValueError("pipeline node tag points to unknown node ({})".format(node))
            return node
        if isinstance(node, str):
            if node not in self.aliases:
                raise ValueError("pipeline node tag references unknown alias `{}`".format(node))
            return self.aliases[node]
        raise TypeError("pipeline node tag target must be a Node or alias string")

    def _validate_consistency(self) -> "Pipeline":
        self._validate_node_uuid_uniqueness()
        linked_inputs = set()
        for node_input_ref, value in self.links:
            if node_input_ref.node not in self.nodes:
                raise ValueError("pipeline link target node is not in pipeline ({})".format(node_input_ref.node))
            if node_input_ref.port not in node_input_ref.node.inputs:
                raise ValueError("pipeline link target port is not on node ({})".format(node_input_ref))
            if node_input_ref in linked_inputs:
                raise ValueError("pipeline input `{}` is linked more than once".format(node_input_ref))
            linked_inputs.add(node_input_ref)

            if (output_ref := _as_node_output_ref(value)) is not None:
                if output_ref.node not in self.nodes:
                    raise ValueError("pipeline link source node is not in pipeline ({})".format(output_ref.node))
                if output_ref.port not in output_ref.node.outputs:
                    raise ValueError("pipeline link source port is not on node ({})".format(output_ref))

        self._validate_acyclicity()

        for name, node in self.aliases.items():
            if node not in self.nodes:
                raise ValueError("pipeline alias `{}` points to unknown node".format(name))
        for key, full_adapter in self.adapters.items():
            if not isinstance(key, str) or not key:
                raise ValueError("pipeline adapter key must be a non-empty string")
            if not isinstance(full_adapter, Adapter):
                raise TypeError("pipeline adapter `{}` must be Adapter".format(key))
            if key != full_adapter.key:
                raise ValueError("pipeline adapter key mismatch: `{}`".format(key))
        for key, save_adapter in self.save_adapters.items():
            if not isinstance(key, str) or not key:
                raise ValueError("pipeline save adapter key must be a non-empty string")
            if not isinstance(save_adapter, SplitSaveAdapter):
                raise TypeError("pipeline save adapter `{}` must be SplitSaveAdapter".format(key))
            if key != save_adapter.key:
                raise ValueError("pipeline save adapter key mismatch: `{}`".format(key))
            if key in self.adapters:
                raise ValueError("pipeline save adapter `{}` conflicts with full adapter".format(key))
        for key, load_adapter in self.load_adapters.items():
            if not isinstance(key, str) or not key:
                raise ValueError("pipeline load adapter key must be a non-empty string")
            if not isinstance(load_adapter, SplitLoadAdapter):
                raise TypeError("pipeline load adapter `{}` must be SplitLoadAdapter".format(key))
            if key != load_adapter.key:
                raise ValueError("pipeline load adapter key mismatch: `{}`".format(key))
            if key in self.adapters:
                raise ValueError("pipeline load adapter `{}` conflicts with full adapter".format(key))
        node_ids = {str(node.uuid) for node in self.nodes}
        for node_id, node_tags in self.tags.items():
            if not isinstance(node_id, str) or not node_id:
                raise ValueError("pipeline node tag id must be a non-empty string")
            if node_id not in node_ids:
                raise ValueError("pipeline node tags reference unknown node `{}`".format(node_id))
            if not isinstance(node_tags, Mapping):
                raise TypeError("pipeline node tags for `{}` must be a mapping".format(node_id))
            for name, value in node_tags.items():
                if not isinstance(name, str) or not name:
                    raise ValueError("pipeline node tag name must be a non-empty string")
                if not isinstance(value, str) or not value:
                    raise ValueError("pipeline node tag value must be a non-empty string")
        from spl.core.adapter_compat import warn_pipeline_adapter_compatibility

        warn_pipeline_adapter_compatibility(self)
        return self

    def _validate_graph_semantics(self) -> None:
        self._validate_node_uuid_uniqueness()
        self._validate_acyclicity()

    def _validate_node_uuid_uniqueness(self) -> None:
        node_ids = [node.uuid for node in self.nodes]
        nodes_by_uuid: dict[Any, list[Node]] = {}
        for node, node_id in zip(self.nodes, node_ids, strict=True):
            canonical_node_id = _try_canonical_uuid(node_id)
            if canonical_node_id is None:
                raise ValueError("pipeline node uuid `{}` is not a valid UUID".format(node_id))
            nodes_by_uuid.setdefault(canonical_node_id, []).append(node)

        for node_id in sorted(nodes_by_uuid, key=str):
            duplicates = nodes_by_uuid[node_id]
            if len(duplicates) < 2:
                continue
            ordered = sorted(duplicates, key=self._duplicate_node_sort_key)
            aliases = [self._node_alias_label(node) for node in ordered]
            locations = [self._node_source_location(node) for node in ordered]
            message = "pipeline has duplicate node uuid `{}` (aliases: {}".format(
                node_id,
                ", ".join("`{}`".format(alias) for alias in aliases),
            )
            if any(location is not None for location in locations):
                message = "{}; locations: {})".format(
                    message,
                    ", ".join("`{}`".format(location or "<unknown>") for location in locations),
                )
            else:
                message = "{})".format(message)
            raise ValueError(message)

    def _duplicate_node_sort_key(self, node: Node) -> tuple[str, str, str]:
        return (
            self._node_alias_label(node),
            self._node_source_location(node) or "",
            repr(node),
        )

    def _node_alias_label(self, node: Node) -> str:
        aliases = sorted(alias for alias, target in self.aliases.items() if target is node)
        if not aliases:
            aliases = sorted(alias for alias, target in self.aliases.items() if target == node)
        return "|".join(aliases) if aliases else "<none>"

    @staticmethod
    def _node_source_location(node: Node) -> str | None:
        if not isinstance(node, m_node_function.NodeFunction):
            return None
        location = getattr(node.func, "__spl_location__", None)
        if isinstance(location, tuple) and len(location) == 2:
            return "{}::{}".format(location[0], location[1])
        code = getattr(node.func, "__code__", None)
        if code is None:
            return None
        return "{}:{}".format(code.co_filename, code.co_firstlineno)

    def _validate_acyclicity(self) -> None:
        ordered_nodes = sorted(self.nodes, key=node_sort_key)
        edges = []
        for node_input_ref, value in self.links:
            output_ref = _as_node_output_ref(value)
            if output_ref is None:
                continue
            if output_ref.node not in self.nodes or node_input_ref.node not in self.nodes:
                continue
            source_id = canonical_uuid_key(output_ref.node.uuid)
            target_id = canonical_uuid_key(node_input_ref.node.uuid)
            edges.append((source_id, target_id))

        cycle = _find_graph_cycle([canonical_uuid_key(node.uuid) for node in ordered_nodes], edges)
        if cycle is None:
            return
        alias_by_node_id: dict[str, str] = {}
        for alias, node in sorted(self.aliases.items()):
            if node in self.nodes:
                alias_by_node_id.setdefault(canonical_uuid_key(node.uuid), alias)
        labels = [alias_by_node_id.get(node_id, node_id) for node_id in cycle]
        raise ValueError("pipeline contains cycle: {}".format(" → ".join(labels)))


@dataclass(frozen=True)
class DPipeline(DBase):
    name: str
    nodes: list[Any]
    links: list[Any]
    aliases: list[list[str]]
    adapters: list[Any] = field(default_factory=list)
    tags: dict[str, dict[str, str]] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(
            (
                tuple(sorted(map(hash, self.nodes))),
                tuple(sorted(map(hash, chain.from_iterable(self.links)))),
                tuple(sorted(map(hash, self.adapters))),
                tuple(sorted((node_id, tuple(sorted(tags.items()))) for node_id, tags in self.tags.items())),
            )
        )


def _valid_ir_alias_pairs(aliases: list[list[str]]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for value in aliases:
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            continue
        alias, node_id = value
        canonical_node_id = _try_canonical_uuid(node_id)
        if isinstance(alias, str) and canonical_node_id is not None:
            pairs.append((alias, canonical_node_id))
    return pairs


def validate_pipeline_ir(pipeline: DPipeline, source: Path | None = None) -> DPipeline:
    """Validate graph semantics before an IR consumer keys nodes by UUID."""

    source_label = str(source) if source is not None else "<pipeline-ir>"
    nodes_by_uuid: dict[str, list[int]] = {}
    for index, node in enumerate(pipeline.nodes):
        node_id = getattr(node, "uuid", None)
        if not isinstance(node_id, str) or not node_id:
            raise ValueError(
                "pipeline node uuid must be a non-empty string (location: `{}`)".format(
                    "{}:pipeline.nodes[{}]".format(source_label, index)
                )
            )
        canonical_node_id = _try_canonical_uuid(node_id)
        if canonical_node_id is None:
            raise ValueError(
                "pipeline node uuid `{}` is not a valid UUID (location: `{}`)".format(
                    node_id,
                    "{}:pipeline.nodes[{}]".format(source_label, index),
                )
            )
        nodes_by_uuid.setdefault(canonical_node_id, []).append(index)

    for node_id in sorted(nodes_by_uuid):
        indexes = nodes_by_uuid[node_id]
        if len(indexes) < 2:
            continue
        matching_aliases = sorted(
            alias for alias, alias_node_id in _valid_ir_alias_pairs(pipeline.aliases) if alias_node_id == node_id
        )
        alias_labels = matching_aliases or ["<none>"]
        locations = ["{}:pipeline.nodes[{}]".format(source_label, index) for index in indexes]
        raise ValueError(
            "pipeline has duplicate node uuid `{}` (aliases: {}; locations: {})".format(
                node_id,
                ", ".join("`{}`".format(alias) for alias in alias_labels),
                ", ".join("`{}`".format(location) for location in locations),
            )
        )

    known_node_ids = set(nodes_by_uuid)
    edges: list[tuple[str, str]] = []
    for link in pipeline.links:
        if not isinstance(link, (list, tuple)) or len(link) != 2:
            continue
        node_input_ref, value = link
        target_id = _try_canonical_uuid(getattr(node_input_ref, "uuid", None))
        if isinstance(value, (m_node.DNodeOutputRef, m_node.DFormattedOutputRef)):
            source_id = _try_canonical_uuid(value.uuid)
            if source_id in known_node_ids and target_id in known_node_ids:
                edges.append((source_id, target_id))

    cycle = _find_graph_cycle(sorted(known_node_ids), edges)
    if cycle is not None:
        alias_by_node_id: dict[str, str] = {}
        for alias, node_id in sorted(_valid_ir_alias_pairs(pipeline.aliases)):
            alias_by_node_id.setdefault(node_id, alias)
        labels = [alias_by_node_id.get(node_id, node_id) for node_id in cycle]
        raise ValueError("pipeline contains cycle: {}".format(" → ".join(labels)))
    return pipeline


def _represent_dpipeline(dumper: yaml.Dumper, data: DPipeline) -> yaml.Node:
    payload: dict[str, Any] = {
        "name": data.name,
        "nodes": data.nodes,
        "links": data.links,
        "aliases": data.aliases,
        "adapters": data.adapters,
    }
    if data.tags:
        payload["tags"] = data.tags
    return dumper.represent_mapping("!DPipeline", payload)


yaml.add_representer(DPipeline, _represent_dpipeline)

yaml.add_constructor(
    "!DPipeline", lambda loader, node: DPipeline(**cast(dict[str, Any], loader.construct_mapping(cast(Any, node))))
)


@ir_parse.register(lambda x: isinstance(x, Pipeline))
def _ir_parse__pipeline(x: Pipeline, name: str | None = None) -> _branch:
    x._validate_graph_semantics()

    serialized_adapters = [
        *[adapter for _, adapter in sorted(x.adapters.items())],
        *[adapter for _, adapter in sorted(x.save_adapters.items())],
        *[adapter for _, adapter in sorted(x.load_adapters.items())],
    ]

    def mk_root() -> DPipeline:
        return DPipeline(
            name=cast(str, x.name),
            nodes=[ir_parse(n, name=name).mk_root() for n in x.nodes],
            links=[[ir_parse(l_from).mk_root(), ir_parse(l_to).mk_root()] for (l_from, l_to) in x.links],
            aliases=[[k, str(v.uuid)] for k, v in x.aliases.items()],
            adapters=[ir_parse(adapter).mk_root() for adapter in serialized_adapters],
            tags={node_id: dict(tags) for node_id, tags in sorted(x.tags.items())},
        )

    def mk_dependencies(frame_offset: int) -> Any:
        return chain.from_iterable(
            [
                *[ir_parse(n, name=name).mk_dependencies(frame_offset) for n in x.nodes],
                *[ir_parse(adapter).mk_dependencies(frame_offset) for adapter in serialized_adapters],
            ]
        )

    return _branch(x, mk_root, mk_dependencies)


def _adapter_map_assignment(map_name: str, adapter_name: str) -> ast.Assign:
    return ast.Assign(
        targets=[
            ast.Subscript(
                value=ast.Name(id=map_name, ctx=ast.Load()),
                slice=ast.Attribute(value=ast.Name(id=adapter_name, ctx=ast.Load()), attr="key", ctx=ast.Load()),
                ctx=ast.Store(),
            )
        ],
        value=ast.Name(id=adapter_name, ctx=ast.Load()),
    )


@ir_unparse.register(lambda x: isinstance(x, DPipeline))
def _ir_unparse__pipeline(x: DPipeline, source: Path) -> Generator[ast.stmt]:
    validate_pipeline_ir(x, source=source)

    # Importing helpers
    # TODO: move to corresponding modules
    yield ast.ImportFrom(module="uuid", names=[ast.alias(name="UUID")], level=0)

    yield ast.ImportFrom(
        module=m_node.__name__,
        names=[ast.alias(name="FormattedOutputRef"), ast.alias(name="NodeInputRef"), ast.alias(name="NodeOutputRef")],
        level=0,
    )

    yield ast.ImportFrom(module=m_scalar.__name__, names=[ast.alias(name="Scalar")], level=0)

    yield ast.ImportFrom(module=m_artifact.__name__, names=[ast.alias(name="ArtifactRef")], level=0)

    yield ast.ImportFrom(
        module=m_adapter.__name__,
        names=[
            ast.alias(name="Adapter"),
            ast.alias(name="SplitLoadAdapter"),
            ast.alias(name="SplitSaveAdapter"),
        ],
        level=0,
    )

    yield ast.ImportFrom(module=m_distribution.__name__, names=[ast.alias(name="DDistribution")], level=0)

    yield ast.ImportFrom(module=m_node_function.__name__, names=[ast.alias(name="NodeFunction")], level=0)

    yield ast.ImportFrom(module=__name__, names=[ast.alias(name="Pipeline")], level=0)

    # _nodes = {}
    yield ast.Assign(targets=[ast.Name(id="_nodes", ctx=ast.Store())], value=ast.Dict())

    for n in x.nodes:
        # _node = ...
        yield from ir_unparse(n, source=source)

        # _nodes[_node.uuid] = _node
        yield ast.Assign(
            targets=[
                ast.Subscript(
                    value=ast.Name(id="_nodes", ctx=ast.Load()),
                    slice=ast.Attribute(value=ast.Name(id="_node", ctx=ast.Load()), attr="uuid", ctx=ast.Load()),
                    ctx=ast.Store(),
                )
            ],
            value=ast.Name(id="_node", ctx=ast.Load()),
        )

    # _links = []
    yield ast.Assign(targets=[ast.Name(id="_links", ctx=ast.Store())], value=ast.List())

    for link_from, link_to in x.links:
        # _link_from = ...
        yield from ir_unparse(link_from, source=source)

        # _link_to = ...
        yield from ir_unparse(link_to, source=source)

        # _links.append((_link_from, _link_to))
        yield ast.Expr(
            value=ast.Call(
                func=ast.Attribute(value=ast.Name(id="_links", ctx=ast.Load()), attr="append", ctx=ast.Load()),
                args=[
                    ast.Tuple(
                        elts=[ast.Name(id="_link_from", ctx=ast.Load()), ast.Name(id="_link_to", ctx=ast.Load())],
                        ctx=ast.Load(),
                    )
                ],
            )
        )

    # _adapters = {}
    yield ast.Assign(targets=[ast.Name(id="_adapters", ctx=ast.Store())], value=ast.Dict())
    yield ast.Assign(targets=[ast.Name(id="_save_adapters", ctx=ast.Store())], value=ast.Dict())
    yield ast.Assign(targets=[ast.Name(id="_load_adapters", ctx=ast.Store())], value=ast.Dict())

    save_keys: set[str] = set()
    load_keys: set[str] = set()
    for adapter in x.adapters:
        if isinstance(adapter, m_adapter.DAdapter):
            if adapter.key in save_keys:
                raise ValueError("pipeline save adapter `{}` is defined more than once".format(adapter.key))
            if adapter.key in load_keys:
                raise ValueError("pipeline load adapter `{}` is defined more than once".format(adapter.key))
            save_keys.add(adapter.key)
            load_keys.add(adapter.key)
        elif isinstance(adapter, m_adapter.DSaveAdapter):
            if adapter.key in save_keys:
                raise ValueError("pipeline save adapter `{}` is defined more than once".format(adapter.key))
            save_keys.add(adapter.key)
        elif isinstance(adapter, m_adapter.DLoadAdapter):
            if adapter.key in load_keys:
                raise ValueError("pipeline load adapter `{}` is defined more than once".format(adapter.key))
            load_keys.add(adapter.key)
        else:
            raise ValueError("pipeline contains unsupported adapter `{}`".format(type(adapter).__name__))

    for adapter in x.adapters:
        if isinstance(adapter, m_adapter.DAdapter):
            yield from ir_unparse(adapter, source=source)
            yield _adapter_map_assignment("_adapters", "_adapter")
        elif isinstance(adapter, m_adapter.DSaveAdapter):
            yield from ir_unparse(adapter, source=source)
            yield _adapter_map_assignment("_save_adapters", "_save_adapter")
        else:
            yield from ir_unparse(adapter, source=source)
            yield _adapter_map_assignment("_load_adapters", "_load_adapter")

    # pipeline = Pipeline(...)
    keywords = [
        ast.keyword(arg="name", value=ast.Constant(value=x.name)),
        ast.keyword(
            arg="nodes",
            value=ast.Set(
                elts=[
                    ast.Starred(
                        value=ast.Call(
                            func=ast.Attribute(
                                value=ast.Name(id="_nodes", ctx=ast.Load()), attr="values", ctx=ast.Load()
                            )
                        )
                    )
                ]
            ),
        ),
        ast.keyword(arg="links", value=ast.Set(elts=[ast.Starred(value=ast.Name(id="_links", ctx=ast.Load()))])),
        ast.keyword(
            arg="aliases",
            value=ast.Dict(
                keys=[ast.Constant(value=k) for [k, _] in x.aliases],
                values=[
                    ast.Subscript(
                        value=ast.Name(id="_nodes", ctx=ast.Load()),
                        slice=ast.Call(func=ast.Name(id="UUID", ctx=ast.Load()), args=[ast.Constant(value=v)]),
                        ctx=ast.Load(),
                    )
                    for [_, v] in x.aliases
                ],
            ),
        ),
        ast.keyword(arg="adapters", value=ast.Name(id="_adapters", ctx=ast.Load())),
        ast.keyword(arg="save_adapters", value=ast.Name(id="_save_adapters", ctx=ast.Load())),
        ast.keyword(arg="load_adapters", value=ast.Name(id="_load_adapters", ctx=ast.Load())),
    ]
    if x.tags:
        keywords.append(ast.keyword(arg="tags", value=_literal_ast(x.tags)))

    yield ast.Assign(
        targets=[ast.Name(id=x.name, ctx=ast.Store())],
        value=ast.Call(
            func=ast.Name(id="Pipeline", ctx=ast.Load()),
            keywords=keywords,
        ),
    )


def _literal_ast(value: Any) -> ast.expr:
    if isinstance(value, dict):
        return ast.Dict(
            keys=[_literal_ast(key) for key in value],
            values=[_literal_ast(item) for item in value.values()],
        )
    if isinstance(value, list):
        return ast.List(elts=[_literal_ast(item) for item in value], ctx=ast.Load())
    if isinstance(value, tuple):
        return ast.Tuple(elts=[_literal_ast(item) for item in value], ctx=ast.Load())
    return ast.Constant(value=value)
