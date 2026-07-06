"""Notebook HTML widget for visualizing SPL pipelines.

The Console owns the primary pipeline graph implementation.  This module keeps
an intentionally duplicated, dependency-free renderer for Jupyter notebooks so
``spl-framework`` users can inspect live or published pipelines without running
the frontend app.
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

NODE_WIDTH = 292
PORT_ROW_HEIGHT = 28
PORT_Y_OFFSET = 76
LAYER_GAP = 166
ROW_GAP = 72
STAGE_PADDING = 72
MIN_SCALE = 0.52
MAX_SCALE = 1.8


@dataclass(frozen=True, repr=False)
class PipelineGraphWidget:
    """Rich notebook display object for one pipeline graph."""

    decomposition: dict[str, Any]
    object_info: dict[str, Any] = field(default_factory=dict)
    height: int = 560
    theme: str = "dark"
    dom_id: str = field(default_factory=lambda: f"spl-pipeline-{uuid4().hex}")

    def __repr__(self) -> str:
        """Return a compact text fallback instead of dumping graph JSON."""

        model = create_pipeline_graph_model(self.decomposition, self.object_info)
        stats = model["stats"]
        return (
            "PipelineGraphWidget("
            f"title={model['title']!r}, "
            f"nodes={len(model['nodes'])}, "
            f"links={len(model['links'])}, "
            f"ports={stats['portCount']}, "
            f"height={self.height}, "
            f"theme={self.theme!r})"
        )

    def _repr_html_(self) -> str:
        """Return the HTML representation used by Jupyter rich display."""

        return render_pipeline_graph_html(
            self.decomposition,
            self.object_info,
            height=self.height,
            theme=self.theme,
            dom_id=self.dom_id,
        )

    @property
    def html(self) -> str:
        """Return the widget HTML for callers that want to embed it manually."""

        return self._repr_html_()

    def display(self) -> "PipelineGraphWidget":
        """Display the widget immediately when IPython is available."""

        try:
            from IPython.display import display
        except Exception:
            return self
        display(self)
        return self


def pipeline_to_decomposition(pipeline: Any) -> dict[str, Any]:
    """Convert a live ``spl.core`` Pipeline into the daemon decomposition shape."""

    from spl.core.entities.node import NodeInputRef, NodeOutputRef
    from spl.core.entities.node_function import NodeFunction
    from spl.core.entities.node_remote import NodeRemote
    from spl.core.entities.scalar import Scalar

    nodes = []
    functions = []

    for node in sorted(pipeline.nodes, key=lambda item: str(item.uuid)):
        node_id = str(node.uuid)
        inputs = [_input_port_to_dict(port) for port in getattr(node, "inputs", [])]
        outputs = [_output_port_to_dict(port) for port in getattr(node, "outputs", [])]
        if isinstance(node, NodeFunction):
            function_name = getattr(node.func, "__name__", node_id)
            payload = {
                "node_id": node_id,
                "id": node_id,
                "kind": "function",
                "function": function_name,
                "name": function_name,
                "inputs": inputs,
                "outputs": outputs,
            }
            functions.append(
                {
                    "kind": "function",
                    "role": "pipeline_component",
                    "node_id": node_id,
                    "name": function_name,
                    "inputs": inputs,
                    "outputs": outputs,
                }
            )
        elif isinstance(node, NodeRemote):
            remote = {
                "url": node.url,
                "name": node.name,
                "version": node.version,
            }
            for attr in ("owner_id", "library", "target_machine"):
                value = getattr(node, attr, None)
                if value is not None:
                    remote[attr] = value
            payload = {
                "node_id": node_id,
                "id": node_id,
                "kind": "remote",
                "name": node.name,
                "remote": remote,
                "inputs": inputs,
                "outputs": outputs,
            }
        else:
            payload = {
                "node_id": node_id,
                "id": node_id,
                "kind": type(node).__name__,
                "name": repr(node),
                "inputs": inputs,
                "outputs": outputs,
            }
        nodes.append(payload)

    links = []
    for node_input_ref, value in sorted(pipeline.links, key=_link_sort_key):
        if not isinstance(node_input_ref, NodeInputRef):
            continue
        target = {
            "node_id": str(node_input_ref.node.uuid),
            "port": node_input_ref.port.name,
        }
        if isinstance(value, NodeOutputRef):
            source: dict[str, Any] = {
                "kind": "node_output",
                "node_id": str(value.node.uuid),
                "port": value.port.name,
            }
        elif isinstance(value, Scalar):
            source = {
                "kind": "scalar",
                "value": _json_safe_value(value.value),
            }
        else:
            source = {
                "kind": type(value).__name__,
                "value": repr(value),
            }
        links.append(
            {
                "target_node_id": target["node_id"],
                "target_port": target["port"],
                "source_kind": source["kind"],
                "source_node_id": source.get("node_id"),
                "source_port": source.get("port"),
                "scalar_json": source.get("value"),
                "source": source,
                "target": target,
                "raw": {
                    "from": target,
                    "to": source,
                },
            }
        )

    return {
        "functions": functions,
        "nodes": nodes,
        "links": links,
    }


def render_pipeline_graph_html(
    decomposition: dict[str, Any],
    object_info: dict[str, Any] | None = None,
    *,
    height: int = 560,
    theme: str = "dark",
    dom_id: str | None = None,
) -> str:
    """Render a standalone pipeline graph widget as HTML."""

    object_info = object_info or {}
    dom_id = dom_id or f"spl-pipeline-{uuid4().hex}"
    model = create_pipeline_graph_model(decomposition, object_info)
    layout_pipeline_graph(model)
    selected_node_id = _first_selectable_node_id(model)
    model["domId"] = dom_id
    model["selectedNodeId"] = selected_node_id
    model_json = _json_for_script(model)
    theme_attr = html.escape(theme or "dark", quote=True)
    title = _escape(model["title"])
    stats = model["stats"]
    stats_label = (
        f"{stats['functionNodes']} functions / "
        f"{len(model['links'])} links / {stats['portCount']} ports"
    )

    return f"""
<div id="{html.escape(dom_id, quote=True)}" class="pipeline-graph-shell spl-pipeline-widget" data-spl-pipeline-widget data-theme="{theme_attr}">
  <style>{_widget_css(dom_id, height)}</style>
  <script type="application/json" data-spl-pipeline-model>{model_json}</script>
  <div class="pipeline-graph-topbar">
    <div class="pipeline-graph-title">
      <span class="panel-label">Node graph</span>
      <strong>{title}</strong>
      <small>{_escape(stats_label)}</small>
    </div>
    <div class="pipeline-graph-tools" role="toolbar" aria-label="Pipeline graph controls">
      {_graph_tool("zoom-out", "Zoom out", "zoomOut")}
      {_graph_tool("target", "Fit view", "fit")}
      {_graph_tool("zoom-in", "Zoom in", "zoomIn")}
      {_graph_tool("reset", "Reset view", "reset")}
      {_graph_tool("fullscreen", "Enter fullscreen", "fullscreen")}
    </div>
  </div>
  <div class="pipeline-graph-workbench">
    <aside class="pipeline-graph-sidebar pipeline-graph-outliner">
      <div class="pipeline-sidebar-head">
        <span class="panel-label">Outliner</span>
        <strong>{len(model["nodes"])} nodes</strong>
      </div>
      <div class="pipeline-outliner-list">
        {_render_outliner(model, selected_node_id)}
      </div>
    </aside>
    <div class="pipeline-graph-viewport" data-pipeline-viewport tabindex="0" aria-label="Pipeline graph canvas">
      <div class="pipeline-graph-stage" data-pipeline-stage style="width:{model["stageWidth"]}px;height:{model["stageHeight"]}px">
        {_render_edge_svg(model)}
        {"".join(_render_node(node, selected_node_id) for node in model["nodes"])}
      </div>
    </div>
    <aside class="pipeline-graph-sidebar pipeline-graph-inspector" data-pipeline-inspector>
      {_render_inspector(model, selected_node_id)}
    </aside>
  </div>
  <script>{_widget_js(dom_id)}</script>
</div>
"""


def create_pipeline_graph_model(
    decomposition: dict[str, Any],
    object_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create the graph model consumed by the notebook renderer."""

    object_info = object_info or {}
    links = _ensure_list(decomposition.get("links"))
    raw_nodes = _ensure_list(decomposition.get("nodes"))
    functions = _ensure_list(decomposition.get("functions"))
    used_node_ids: set[str] = set()
    used_link_ids: set[str] = set()
    nodes_by_raw_id: dict[str, dict[str, Any]] = {}
    nodes_by_function: dict[str, dict[str, Any]] = {}

    nodes = []
    for index, raw_node in enumerate(raw_nodes):
        raw_id = _node_id_for(raw_node, index)
        function_name = _function_name_for(raw_node)
        function_row = _matching_function(functions, raw_id, function_name)
        node_id = _unique_id(_slug_for(raw_id, f"node-{index + 1}"), used_node_ids)
        inputs = _merge_ports(
            raw_node.get("inputs") or function_row.get("inputs"),
            _link_ports_for_node(links, raw_id, "target"),
            "input",
        )
        outputs = _merge_ports(
            raw_node.get("outputs") or function_row.get("outputs"),
            _link_ports_for_node(links, raw_id, "source"),
            "output",
        )
        kind = str(raw_node.get("kind") or raw_node.get("type") or "function")
        label = function_name
        model_node = {
            "id": node_id,
            "rawId": raw_id,
            "kind": "function",
            "nodeKind": kind,
            "label": label,
            "subtitle": raw_id,
            "version": raw_node.get("version") or raw_node.get("version_id") or function_row.get("version") or "",
            "inputs": inputs,
            "outputs": outputs,
            "width": NODE_WIDTH,
            "height": _node_height(inputs, outputs),
        }
        nodes_by_raw_id[raw_id] = model_node
        nodes_by_function[label] = model_node
        nodes.append(model_node)

    for index, item in enumerate(functions):
        function_name = str(item.get("name") or item.get("function") or item.get("function_name") or f"function_{index + 1}")
        if function_name in nodes_by_function:
            continue
        raw_id = str(item.get("node_id") or item.get("id") or function_name)
        node_id = _unique_id(_slug_for(raw_id, f"function-{index + 1}"), used_node_ids)
        inputs = _merge_ports(item.get("inputs"), [], "input")
        outputs = _merge_ports(item.get("outputs"), [], "output")
        model_node = {
            "id": node_id,
            "rawId": raw_id,
            "kind": "function",
            "nodeKind": str(item.get("kind") or "function"),
            "label": function_name,
            "subtitle": raw_id,
            "version": item.get("version") or item.get("version_id") or "",
            "inputs": inputs,
            "outputs": outputs,
            "width": NODE_WIDTH,
            "height": _node_height(inputs, outputs),
        }
        nodes_by_raw_id[raw_id] = model_node
        nodes_by_function[function_name] = model_node
        nodes.append(model_node)

    external_nodes: dict[str, dict[str, Any]] = {}
    model_links = []
    for index, link in enumerate(links):
        target_raw_id = _target_node_id(link)
        target_port_name = _target_port(link)
        target_node = nodes_by_raw_id.get(target_raw_id)
        if target_node is None:
            target_node = _create_missing_node(target_raw_id, nodes, used_node_ids, nodes_by_raw_id)
        source_node, source_port_name, source_label = _resolve_source_node(
            link,
            index,
            external_nodes=external_nodes,
            nodes=nodes,
            nodes_by_raw_id=nodes_by_raw_id,
            used_node_ids=used_node_ids,
        )
        _ensure_port(source_node, "output", source_port_name)
        _ensure_port(target_node, "input", target_port_name)
        model_links.append(
            {
                "id": _unique_id(f"link-{index + 1}-{source_node['id']}-{target_node['id']}", used_link_ids),
                "sourceNodeId": source_node["id"],
                "sourceRawNodeId": source_node["rawId"],
                "sourcePort": source_port_name,
                "sourceLabel": _readable_port_label(source_node, source_port_name, source_label),
                "targetNodeId": target_node["id"],
                "targetRawNodeId": target_node["rawId"],
                "targetPort": target_port_name,
                "targetLabel": _readable_port_label(target_node, target_port_name, _target_label_for(link)),
                "label": _link_label(source_port_name, target_port_name),
            }
        )

    for node in nodes:
        node["inputs"] = _merge_ports(node.get("inputs"), [], "input")
        node["outputs"] = _merge_ports(node.get("outputs"), [], "output")
        node["height"] = _node_height(node["inputs"], node["outputs"])

    object_name = object_info.get("name") or object_info.get("id") or "pipeline"
    return {
        "id": object_info.get("id") or object_name,
        "title": object_info.get("displayName") or object_info.get("display_name") or object_name or "Pipeline",
        "objectName": object_name,
        "nodes": nodes,
        "links": model_links,
        "stats": {
            "functionNodes": len([node for node in nodes if node.get("kind") != "external"]),
            "externalNodes": len([node for node in nodes if node.get("kind") == "external"]),
            "portCount": sum(len(_visible_ports(node.get("inputs"))) + len(_visible_ports(node.get("outputs"))) for node in nodes),
        },
    }


def layout_pipeline_graph(model: dict[str, Any]) -> None:
    """Assign a stable layered layout and edge paths to a graph model."""

    nodes = model["nodes"]
    node_by_id = {node["id"]: node for node in nodes}
    order = {node["id"]: index for index, node in enumerate(nodes)}
    layers = {node["id"]: 0 for node in nodes}
    for _ in range(max(1, len(nodes))):
        changed = False
        for link in model["links"]:
            source_id = link["sourceNodeId"]
            target_id = link["targetNodeId"]
            next_layer = layers.get(source_id, 0) + 1
            if layers.get(target_id, 0) < next_layer:
                layers[target_id] = next_layer
                changed = True
        if not changed:
            break

    grouped: dict[int, list[dict[str, Any]]] = {}
    for node in nodes:
        grouped.setdefault(layers.get(node["id"], 0), []).append(node)

    max_right = STAGE_PADDING + NODE_WIDTH
    max_bottom = STAGE_PADDING + 160
    for layer, layer_nodes in sorted(grouped.items()):
        y = STAGE_PADDING
        for node in sorted(layer_nodes, key=lambda item: order[item["id"]]):
            node["x"] = STAGE_PADDING + layer * (NODE_WIDTH + LAYER_GAP)
            node["y"] = y
            y += node["height"] + ROW_GAP
            max_right = max(max_right, node["x"] + node["width"] + STAGE_PADDING)
            max_bottom = max(max_bottom, node["y"] + node["height"] + STAGE_PADDING)

    model["stageWidth"] = int(max_right)
    model["stageHeight"] = int(max_bottom)

    for link in model["links"]:
        source = node_by_id.get(link["sourceNodeId"])
        target = node_by_id.get(link["targetNodeId"])
        if source is None or target is None:
            link["path"] = ""
            continue
        source_index = _port_index(source.get("outputs"), link["sourcePort"])
        target_index = _port_index(target.get("inputs"), link["targetPort"])
        start_x = source["x"] + source["width"]
        start_y = source["y"] + PORT_Y_OFFSET + source_index * PORT_ROW_HEIGHT
        end_x = target["x"]
        end_y = target["y"] + PORT_Y_OFFSET + target_index * PORT_ROW_HEIGHT
        mid_x = max(start_x + 62, round((start_x + end_x) / 2))
        link["path"] = f"M {start_x} {start_y} L {mid_x} {start_y} L {mid_x} {end_y} L {end_x} {end_y}"


def _input_port_to_dict(port: Any) -> dict[str, Any]:
    return {
        "name": str(port.name),
        "type": port.typ_,
        "default": port.default,
        "required": port.default is None,
    }


def _output_port_to_dict(port: Any) -> dict[str, Any]:
    return {
        "name": str(port.name),
        "type": port.typ_,
    }


def _link_sort_key(item: tuple[Any, Any]) -> tuple[str, str, str]:
    node_input_ref, value = item
    source = getattr(getattr(value, "node", None), "uuid", "")
    return (
        str(getattr(getattr(node_input_ref, "node", None), "uuid", "")),
        str(getattr(getattr(node_input_ref, "port", None), "name", "")),
        str(source) or repr(value),
    )


def _json_safe_value(value: Any) -> Any:
    try:
        json.dumps(value)
    except TypeError:
        return repr(value)
    return value


def _ensure_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _node_id_for(node: dict[str, Any], index: int) -> str:
    return str(node.get("node_id") or node.get("id") or node.get("name") or f"node-{index + 1}")


def _function_name_for(node: dict[str, Any]) -> str:
    remote = node.get("remote") if isinstance(node.get("remote"), dict) else {}
    return str(
        node.get("function")
        or node.get("function_name")
        or remote.get("name")
        or node.get("object_name")
        or node.get("name")
        or node.get("node_id")
        or "function"
    )


def _matching_function(functions: list[Any], raw_id: str, function_name: str) -> dict[str, Any]:
    for item in functions:
        if not isinstance(item, dict):
            continue
        if (item.get("node_id") or item.get("id")) == raw_id:
            return item
        if (item.get("name") or item.get("function") or item.get("function_name")) == function_name:
            return item
    return {}


def _merge_ports(declared: Any, linked: list[dict[str, Any]], direction: str) -> list[dict[str, Any]]:
    ports = _port_items(declared, direction)
    names = {port["name"] for port in ports}
    for item in linked:
        name = str(item.get("name") or item.get("port") or "default")
        if name not in names:
            ports.append({"name": name, "detail": item.get("detail") or ""})
            names.add(name)
    return ports


def _port_items(value: Any, direction: str) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        iterable = [
            {"name": name, **(detail if isinstance(detail, dict) else {"type": detail})}
            for name, detail in value.items()
        ]
    elif isinstance(value, list):
        iterable = [item for item in value if isinstance(item, dict)]
    else:
        iterable = []

    ports = []
    for index, item in enumerate(iterable):
        name = str(item.get("name") or item.get("port") or ("input" if direction == "input" else "default"))
        detail = item.get("detail")
        if detail is None:
            detail = item.get("type")
        if direction == "input" and item.get("default") is not None:
            default = item["default"]
            detail = f"{detail}, default={default}" if detail else f"default={default}"
        ports.append(
            {
                "name": name,
                "detail": "" if detail is None else str(detail),
                "hidden": bool(item.get("hidden")),
                "order": index,
            }
        )
    return ports


def _link_ports_for_node(links: list[Any], raw_id: str, role: str) -> list[dict[str, Any]]:
    result = []
    for link in links:
        if not isinstance(link, dict):
            continue
        if role == "target" and _target_node_id(link) == raw_id:
            result.append({"name": _target_port(link)})
        if role == "source" and _source_node_id(link) == raw_id:
            result.append({"name": _source_port(link)})
    return result


def _target_node_id(link: dict[str, Any]) -> str:
    target = link.get("target") if isinstance(link.get("target"), dict) else {}
    raw = link.get("raw") if isinstance(link.get("raw"), dict) else {}
    raw_from = raw.get("from") if isinstance(raw.get("from"), dict) else {}
    return str(link.get("target_node_id") or target.get("node_id") or raw_from.get("node_id") or "")


def _target_port(link: dict[str, Any]) -> str:
    target = link.get("target") if isinstance(link.get("target"), dict) else {}
    raw = link.get("raw") if isinstance(link.get("raw"), dict) else {}
    raw_from = raw.get("from") if isinstance(raw.get("from"), dict) else {}
    return str(link.get("target_port") or target.get("port") or raw_from.get("port") or "default")


def _source_node_id(link: dict[str, Any]) -> str:
    source = link.get("source") if isinstance(link.get("source"), dict) else {}
    raw = link.get("raw") if isinstance(link.get("raw"), dict) else {}
    raw_to = raw.get("to") if isinstance(raw.get("to"), dict) else {}
    return str(link.get("source_node_id") or source.get("node_id") or raw_to.get("node_id") or "")


def _source_port(link: dict[str, Any]) -> str:
    source = link.get("source") if isinstance(link.get("source"), dict) else {}
    raw = link.get("raw") if isinstance(link.get("raw"), dict) else {}
    raw_to = raw.get("to") if isinstance(raw.get("to"), dict) else {}
    return str(link.get("source_port") or source.get("port") or raw_to.get("port") or "value")


def _source_kind(link: dict[str, Any]) -> str:
    source = link.get("source") if isinstance(link.get("source"), dict) else {}
    raw = link.get("raw") if isinstance(link.get("raw"), dict) else {}
    raw_to = raw.get("to") if isinstance(raw.get("to"), dict) else {}
    return str(link.get("source_kind") or source.get("kind") or raw_to.get("kind") or "source")


def _resolve_source_node(
    link: dict[str, Any],
    index: int,
    *,
    external_nodes: dict[str, dict[str, Any]],
    nodes: list[dict[str, Any]],
    nodes_by_raw_id: dict[str, dict[str, Any]],
    used_node_ids: set[str],
) -> tuple[dict[str, Any], str, str]:
    raw_id = _source_node_id(link)
    if raw_id and raw_id in nodes_by_raw_id:
        return nodes_by_raw_id[raw_id], _source_port(link), _source_label_for(link)

    kind = _source_kind(link)
    value = _source_value(link)
    label = _format_scalar_value(value) if kind in {"scalar", "literal"} else kind
    external_key = f"{kind}:{label}:{index}"
    if external_key not in external_nodes:
        node_id = _unique_id(_slug_for(external_key, f"source-{index + 1}"), used_node_ids)
        node = {
            "id": node_id,
            "rawId": external_key,
            "kind": "external",
            "nodeKind": kind,
            "label": label,
            "subtitle": "bound input",
            "version": "",
            "inputs": [],
            "outputs": [{"name": _source_port(link), "detail": "literal"}],
            "width": 220,
            "height": 128,
        }
        external_nodes[external_key] = node
        nodes.append(node)
        nodes_by_raw_id[external_key] = node
    return external_nodes[external_key], _source_port(link), label


def _source_value(link: dict[str, Any]) -> Any:
    source = link.get("source") if isinstance(link.get("source"), dict) else {}
    raw = link.get("raw") if isinstance(link.get("raw"), dict) else {}
    raw_to = raw.get("to") if isinstance(raw.get("to"), dict) else {}
    if "scalar_json" in link:
        return link.get("scalar_json")
    if "value" in source:
        return source.get("value")
    return raw_to.get("value")


def _create_missing_node(
    raw_id: str,
    nodes: list[dict[str, Any]],
    used_node_ids: set[str],
    nodes_by_raw_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    label = raw_id or "missing node"
    node = {
        "id": _unique_id(_slug_for(label, "missing-node"), used_node_ids),
        "rawId": label,
        "kind": "external",
        "nodeKind": "missing",
        "label": label,
        "subtitle": "missing node",
        "version": "",
        "inputs": [],
        "outputs": [],
        "width": 220,
        "height": 128,
    }
    nodes.append(node)
    nodes_by_raw_id[raw_id] = node
    return node


def _ensure_port(node: dict[str, Any], direction: str, name: str) -> None:
    key = "inputs" if direction == "input" else "outputs"
    ports = node.setdefault(key, [])
    if not any(port.get("name") == name for port in ports):
        ports.append({"name": name, "detail": ""})


def _readable_port_label(node: dict[str, Any], port_name: str, fallback: str) -> str:
    label = node.get("label") or fallback or node.get("rawId") or "node"
    if node.get("kind") == "external":
        return str(label)
    return f"{label}.{port_name}"


def _source_label_for(link: dict[str, Any]) -> str:
    return _source_port(link)


def _target_label_for(link: dict[str, Any]) -> str:
    return _target_port(link)


def _link_label(source_port: str, target_port: str) -> str:
    if source_port == target_port:
        return target_port
    return f"{source_port} -> {target_port}"


def _node_height(inputs: list[dict[str, Any]], outputs: list[dict[str, Any]]) -> int:
    rows = max(len(_visible_ports(inputs)), len(_visible_ports(outputs)), 1)
    return max(148, 104 + rows * PORT_ROW_HEIGHT)


def _visible_ports(ports: Any) -> list[dict[str, Any]]:
    return [port for port in _ensure_list(ports) if not port.get("hidden")]


def _port_index(ports: Any, name: str) -> int:
    visible = _visible_ports(ports)
    for index, port in enumerate(visible):
        if port.get("name") == name:
            return index
    return 0


def _slug_for(value: str, fallback: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(value).strip()).strip("-").lower()
    return slug or fallback


def _unique_id(base: str, used: set[str]) -> str:
    candidate = base
    index = 2
    while candidate in used:
        candidate = f"{base}-{index}"
        index += 1
    used.add(candidate)
    return candidate


def _first_selectable_node_id(model: dict[str, Any]) -> str:
    for node in model["nodes"]:
        if node.get("kind") != "external":
            return str(node["id"])
    return str(model["nodes"][0]["id"]) if model["nodes"] else ""


def _format_scalar_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return repr(value)


def _escape(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _json_for_script(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def _render_outliner(model: dict[str, Any], selected_node_id: str) -> str:
    if not model["nodes"]:
        return '<div class="pipeline-empty-slot">No nodes</div>'
    return "".join(_render_outliner_node(node, selected_node_id) for node in model["nodes"])


def _render_outliner_node(node: dict[str, Any], selected_node_id: str) -> str:
    selected = " selected" if node["id"] == selected_node_id else ""
    subtitle = node.get("nodeKind") if node.get("kind") == "external" else node.get("subtitle") or node.get("nodeKind")
    return f"""
<button class="pipeline-outliner-node{selected}" type="button" data-pipeline-select-node="{_escape(node["id"])}">
  <span>{_escape(node.get("label"))}</span>
  <small>{_escape(subtitle)}</small>
</button>
"""


def _render_edge_svg(model: dict[str, Any]) -> str:
    arrow_id = f"{_slug_for(str(model.get('domId') or model['id']), 'graph')}-pipeline-arrow"
    return f"""
<svg class="pipeline-graph-edges" viewBox="0 0 {model["stageWidth"]} {model["stageHeight"]}" aria-hidden="true">
  <defs>
    <marker id="{_escape(arrow_id)}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z"></path>
    </marker>
  </defs>
  {"".join(_render_edge(model, link, index, arrow_id) for index, link in enumerate(model["links"]))}
</svg>
"""


def _render_edge(model: dict[str, Any], link: dict[str, Any], index: int, arrow_id: str) -> str:
    path_base = _slug_for(str(model.get("domId") or model["id"]), "graph")
    path_id = f"pipeline-edge-path-{path_base}-{index}"
    label = link.get("label") or ""
    return f"""
<g class="pipeline-graph-edge" data-pipeline-edge-id="{_escape(link["id"])}">
  <path id="{_escape(path_id)}" d="{_escape(link.get("path") or "")}" marker-end="url(#{_escape(arrow_id)})"></path>
  {f'<text dy="-7"><textPath href="#{_escape(path_id)}" startOffset="50%">{_escape(label)}</textPath></text>' if label else ""}
</g>
"""


def _render_node(node: dict[str, Any], selected_node_id: str) -> str:
    selected = " selected" if node["id"] == selected_node_id else ""
    external = " external" if node.get("kind") == "external" else ""
    ports = _render_port_column(node.get("inputs"), "input") + _render_port_column(node.get("outputs"), "output")
    dots = _render_port_dots(node.get("inputs"), "input", 0) + _render_port_dots(node.get("outputs"), "output", node["width"])
    return f"""
<div class="pipeline-graph-node{external}{selected}" data-pipeline-node-id="{_escape(node["id"])}" tabindex="0" role="button" aria-label="{_escape(node.get("label"))}" style="left:{node["x"]}px;top:{node["y"]}px;width:{node["width"]}px;height:{node["height"]}px">
  <div class="pipeline-graph-node-head">
    <span>{_escape(node.get("subtitle") or node.get("nodeKind"))}</span>
    <strong>{_escape(node.get("label"))}</strong>
  </div>
  <div class="pipeline-graph-node-ports">{ports}</div>
  {dots}
</div>
"""


def _render_port_column(ports: Any, direction: str) -> str:
    visible = _visible_ports(ports)
    if visible:
        body = "".join(
            f"""
<span class="pipeline-port-row">
  <span>{_escape(port.get("name"))}</span>
  {f'<small>{_escape(port.get("detail"))}</small>' if port.get("detail") else ""}
</span>
"""
            for port in visible
        )
    else:
        body = f'<span class="pipeline-port-row muted"><span>{"no inputs" if direction == "input" else "no outputs"}</span></span>'
    return f'<div class="pipeline-port-column {direction}">{body}</div>'


def _render_port_dots(ports: Any, direction: str, left: int) -> str:
    dot_left = left - 5
    return "".join(
        f'<span class="pipeline-port-dot {direction}" style="left:{dot_left}px;top:{PORT_Y_OFFSET + index * PORT_ROW_HEIGHT - 5}px"></span>'
        for index, _ in enumerate(_visible_ports(ports))
    )


def _render_inspector(model: dict[str, Any], node_id: str) -> str:
    node = next((item for item in model["nodes"] if item["id"] == node_id), None)
    if node is None and model["nodes"]:
        node = model["nodes"][0]
    if node is None:
        return '<div class="pipeline-empty-slot">No node selected</div>'
    related = [
        link
        for link in model["links"]
        if link["sourceNodeId"] == node["id"] or link["targetNodeId"] == node["id"]
    ]
    return f"""
<div class="pipeline-sidebar-head">
  <span class="panel-label">Inspector</span>
  <strong>{_escape(node.get("label"))}</strong>
</div>
<div class="pipeline-inspector-body">
  <div class="pipeline-inspector-meta">
    <span>Node <strong>{_escape(node.get("subtitle") or node.get("rawId"))}</strong></span>
    <span>Type <strong>{_escape(node.get("nodeKind") or node.get("kind"))}</strong></span>
    {f'<span>Version <strong>{_escape(node.get("version"))}</strong></span>' if node.get("version") else ""}
  </div>
  <div class="pipeline-inspector-section">
    <span class="panel-label">Inputs</span>
    <div class="pipeline-chip-list">{"".join(_render_port_chip(port) for port in _visible_ports(node.get("inputs"))) or '<div class="pipeline-empty-slot">None</div>'}</div>
  </div>
  <div class="pipeline-inspector-section">
    <span class="panel-label">Outputs</span>
    <div class="pipeline-chip-list">{"".join(_render_port_chip(port) for port in _visible_ports(node.get("outputs"))) or '<div class="pipeline-empty-slot">None</div>'}</div>
  </div>
  <div class="pipeline-inspector-section">
    <span class="panel-label">Links</span>
    <div class="pipeline-link-mini-list">{"".join(_render_mini_link(link, node["id"]) for link in related) or '<div class="pipeline-empty-slot">No links</div>'}</div>
  </div>
</div>
"""


def _render_port_chip(port: dict[str, Any]) -> str:
    detail = f'<small>{_escape(port.get("detail"))}</small>' if port.get("detail") else ""
    return f'<span class="pipeline-port-chip">{_escape(port.get("name"))}{detail}</span>'


def _render_mini_link(link: dict[str, Any], node_id: str) -> str:
    outgoing = link["sourceNodeId"] == node_id
    return f"""
<span class="pipeline-mini-link {'outgoing' if outgoing else 'incoming'}">
  <small>{'out' if outgoing else 'in'}</small>
  <strong>{_escape(link.get("sourceLabel"))} -> {_escape(link.get("targetLabel"))}</strong>
</span>
"""


def _graph_tool(icon_name: str, label: str, control: str) -> str:
    return f"""
<button class="pipeline-tool-button" type="button" data-pipeline-control="{_escape(control)}" title="{_escape(label)}" aria-label="{_escape(label)}">
  {_icon(icon_name)}
</button>
"""


def _icon(name: str) -> str:
    paths = {
        "zoom-out": '<path d="M5 11a6 6 0 1 0 12 0 6 6 0 0 0-12 0Z"></path><path d="M8.5 11h5"></path><path d="m16 16 4 4"></path>',
        "zoom-in": '<path d="M5 11a6 6 0 1 0 12 0 6 6 0 0 0-12 0Z"></path><path d="M8.5 11h5"></path><path d="M11 8.5v5"></path><path d="m16 16 4 4"></path>',
        "target": '<circle cx="12" cy="12" r="7"></circle><circle cx="12" cy="12" r="2"></circle><path d="M12 2v3"></path><path d="M12 19v3"></path><path d="M2 12h3"></path><path d="M19 12h3"></path>',
        "reset": '<path d="M3 12a9 9 0 1 0 3-6.7"></path><path d="M3 4v6h6"></path>',
        "fullscreen": '<path d="M8 3H3v5"></path><path d="M16 3h5v5"></path><path d="M21 16v5h-5"></path><path d="M8 21H3v-5"></path>',
    }
    return f'<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">{paths[name]}</svg>'


def _widget_css(dom_id: str, height: int) -> str:
    selector = f"#{dom_id}"
    return f"""
{selector}.pipeline-graph-shell {{
  --spl-bg: #09111d;
  --spl-panel: #0f1b2b;
  --spl-panel-2: #142337;
  --spl-node: #16263a;
  --spl-node-border: rgba(148, 163, 184, 0.3);
  --spl-node-active: #38bdf8;
  --spl-text: #e5edf7;
  --spl-muted: #8ea1b8;
  --spl-edge: #67e8f9;
  --spl-input: #fbbf24;
  --spl-output: #22c55e;
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  width: 100%;
  min-height: {int(height)}px;
  max-height: none;
  overflow: hidden;
  border: 1px solid rgba(148, 163, 184, 0.22);
  border-radius: 8px;
  background: var(--spl-bg);
  color: var(--spl-text);
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}}
{selector} *, {selector} *::before, {selector} *::after {{ box-sizing: border-box; }}
{selector} .panel-label {{
  color: var(--spl-muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0;
  text-transform: uppercase;
}}
{selector} .pipeline-graph-topbar {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 14px 16px;
  border-bottom: 1px solid rgba(148, 163, 184, 0.18);
  background: rgba(15, 27, 43, 0.96);
}}
{selector} .pipeline-graph-title {{
  display: grid;
  gap: 3px;
  min-width: 0;
}}
{selector} .pipeline-graph-title strong,
{selector} .pipeline-graph-title small {{
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
{selector} .pipeline-graph-title strong {{ font-size: 15px; }}
{selector} .pipeline-graph-title small {{ color: var(--spl-muted); font-size: 12px; }}
{selector} .pipeline-graph-tools {{
  display: flex;
  align-items: center;
  gap: 7px;
  flex: 0 0 auto;
}}
{selector} .pipeline-tool-button {{
  display: inline-grid;
  place-items: center;
  width: 34px;
  height: 34px;
  border: 1px solid rgba(148, 163, 184, 0.24);
  border-radius: 7px;
  color: var(--spl-text);
  background: rgba(15, 23, 42, 0.76);
  cursor: pointer;
}}
{selector} .pipeline-tool-button:hover,
{selector} .pipeline-tool-button:focus-visible,
{selector} .pipeline-tool-button.active {{
  border-color: rgba(56, 189, 248, 0.78);
  color: #e0f7ff;
  outline: none;
}}
{selector} .pipeline-tool-button svg {{ width: 17px; height: 17px; }}
{selector} .pipeline-graph-workbench {{
  display: grid;
  grid-template-columns: minmax(150px, 210px) minmax(280px, 1fr) minmax(170px, 240px);
  min-height: calc({int(height)}px - 64px);
}}
{selector}.pipeline-graph-is-fullscreen,
{selector}:fullscreen {{
  position: fixed;
  inset: 12px;
  z-index: 999999;
  width: auto;
  height: auto;
  min-height: 0;
}}
{selector}.pipeline-graph-is-fullscreen .pipeline-graph-workbench,
{selector}:fullscreen .pipeline-graph-workbench {{
  min-height: calc(100vh - 88px);
}}
{selector} .pipeline-graph-sidebar {{
  min-width: 0;
  border-right: 1px solid rgba(148, 163, 184, 0.16);
  background: rgba(15, 27, 43, 0.8);
}}
{selector} .pipeline-graph-inspector {{
  border-right: 0;
  border-left: 1px solid rgba(148, 163, 184, 0.16);
}}
{selector} .pipeline-sidebar-head {{
  display: grid;
  gap: 4px;
  padding: 14px 14px 10px;
}}
{selector} .pipeline-sidebar-head strong {{
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 13px;
}}
{selector} .pipeline-outliner-list {{
  display: grid;
  gap: 7px;
  max-height: calc({int(height)}px - 132px);
  overflow: auto;
  padding: 0 10px 14px;
}}
{selector} .pipeline-outliner-node {{
  display: grid;
  gap: 3px;
  width: 100%;
  padding: 9px 10px;
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: 7px;
  background: rgba(15, 23, 42, 0.82);
  color: var(--spl-text);
  text-align: left;
  cursor: pointer;
}}
{selector} .pipeline-outliner-node:hover,
{selector} .pipeline-outliner-node:focus-visible,
{selector} .pipeline-outliner-node.selected {{
  border-color: rgba(56, 189, 248, 0.76);
  outline: none;
}}
{selector} .pipeline-outliner-node span,
{selector} .pipeline-outliner-node small {{
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
{selector} .pipeline-outliner-node span {{ font-size: 12px; font-weight: 700; }}
{selector} .pipeline-outliner-node small {{ color: var(--spl-muted); font-size: 11px; }}
{selector} .pipeline-graph-viewport {{
  position: relative;
  min-height: calc({int(height)}px - 64px);
  overflow: hidden;
  background:
    linear-gradient(rgba(148, 163, 184, 0.05) 1px, transparent 1px),
    linear-gradient(90deg, rgba(148, 163, 184, 0.05) 1px, transparent 1px),
    #0a1321;
  background-size: 28px 28px;
  cursor: grab;
}}
{selector} .pipeline-graph-viewport.panning {{ cursor: grabbing; }}
{selector} .pipeline-graph-stage {{
  position: absolute;
  left: 0;
  top: 0;
  transform-origin: 0 0;
}}
{selector} .pipeline-graph-edges {{
  position: absolute;
  inset: 0;
  overflow: visible;
  pointer-events: none;
}}
{selector} .pipeline-graph-edge path {{
  fill: none;
  stroke: var(--spl-edge);
  stroke-width: 2.2;
  stroke-linecap: round;
  stroke-linejoin: round;
  opacity: 0.78;
}}
{selector} .pipeline-graph-edge text {{
  fill: #bfefff;
  font-size: 11px;
  paint-order: stroke;
  stroke: #07111f;
  stroke-width: 4px;
  text-anchor: middle;
}}
{selector} marker path {{ fill: var(--spl-edge); }}
{selector} .pipeline-graph-node {{
  position: absolute;
  display: grid;
  grid-template-rows: auto 1fr;
  border: 1px solid var(--spl-node-border);
  border-radius: 8px;
  background: linear-gradient(180deg, rgba(30, 49, 73, 0.96), var(--spl-node));
  box-shadow: 0 18px 44px rgba(0, 0, 0, 0.26);
  overflow: visible;
}}
{selector} .pipeline-graph-node:hover,
{selector} .pipeline-graph-node:focus-visible,
{selector} .pipeline-graph-node.selected {{
  border-color: rgba(56, 189, 248, 0.9);
  outline: none;
}}
{selector} .pipeline-graph-node.external {{
  background: linear-gradient(180deg, rgba(55, 43, 20, 0.96), rgba(32, 27, 17, 0.98));
}}
{selector} .pipeline-graph-node-head {{
  display: grid;
  gap: 3px;
  padding: 12px 14px 10px;
  border-bottom: 1px solid rgba(148, 163, 184, 0.16);
}}
{selector} .pipeline-graph-node-head span,
{selector} .pipeline-graph-node-head strong {{
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
{selector} .pipeline-graph-node-head span {{
  color: var(--spl-muted);
  font-size: 10px;
}}
{selector} .pipeline-graph-node-head strong {{ font-size: 13px; }}
{selector} .pipeline-graph-node-ports {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  padding: 12px 14px 14px;
}}
{selector} .pipeline-port-column {{
  display: grid;
  align-content: start;
  gap: 6px;
  min-width: 0;
}}
{selector} .pipeline-port-column.output {{ text-align: right; }}
{selector} .pipeline-port-row {{
  display: grid;
  gap: 2px;
  min-height: 22px;
}}
{selector} .pipeline-port-row span,
{selector} .pipeline-port-row small {{
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
{selector} .pipeline-port-row span {{ font-size: 11px; font-weight: 700; }}
{selector} .pipeline-port-row small {{ color: var(--spl-muted); font-size: 10px; }}
{selector} .pipeline-port-row.muted span {{ color: var(--spl-muted); font-weight: 600; }}
{selector} .pipeline-port-dot {{
  position: absolute;
  width: 10px;
  height: 10px;
  border: 2px solid #07111f;
  border-radius: 999px;
  background: var(--spl-output);
}}
{selector} .pipeline-port-dot.input {{ background: var(--spl-input); }}
{selector} .pipeline-graph-node.external .pipeline-port-dot {{ background: #fbbf24; }}
{selector} .pipeline-inspector-body,
{selector} .pipeline-inspector-section {{
  display: grid;
  gap: 10px;
}}
{selector} .pipeline-inspector-body {{ padding: 0 14px 14px; }}
{selector} .pipeline-inspector-meta {{
  display: grid;
  gap: 7px;
  color: var(--spl-muted);
  font-size: 11px;
}}
{selector} .pipeline-inspector-meta span {{
  display: flex;
  justify-content: space-between;
  gap: 8px;
  min-width: 0;
}}
{selector} .pipeline-inspector-meta strong {{
  color: var(--spl-text);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
{selector} .pipeline-chip-list,
{selector} .pipeline-link-mini-list {{
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
}}
{selector} .pipeline-port-chip,
{selector} .pipeline-mini-link,
{selector} .pipeline-empty-slot {{
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: 7px;
  background: rgba(15, 23, 42, 0.72);
  color: var(--spl-text);
  font-size: 11px;
}}
{selector} .pipeline-port-chip {{
  display: inline-grid;
  gap: 2px;
  padding: 6px 8px;
}}
{selector} .pipeline-port-chip small {{
  color: var(--spl-muted);
  font-size: 10px;
}}
{selector} .pipeline-mini-link {{
  display: grid;
  gap: 3px;
  max-width: 100%;
  padding: 7px 8px;
}}
{selector} .pipeline-mini-link small {{ color: var(--spl-muted); }}
{selector} .pipeline-mini-link strong {{
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
{selector} .pipeline-empty-slot {{
  padding: 9px 10px;
  color: var(--spl-muted);
}}
@media (max-width: 860px) {{
  {selector} .pipeline-graph-workbench {{
    grid-template-columns: 1fr;
  }}
  {selector} .pipeline-graph-sidebar {{
    display: none;
  }}
  {selector} .pipeline-graph-viewport {{
    min-height: {max(360, int(height) - 68)}px;
  }}
}}
"""


def _widget_js(dom_id: str) -> str:
    safe_dom_id = json.dumps(dom_id)
    return f"""
(function () {{
  const root = document.getElementById({safe_dom_id});
  if (!root || root.dataset.splPipelineReady === "true") return;
  root.dataset.splPipelineReady = "true";
  const modelScript = root.querySelector("[data-spl-pipeline-model]");
  const model = JSON.parse(modelScript.textContent || "{{}}");
  const viewport = root.querySelector("[data-pipeline-viewport]");
  const stage = root.querySelector("[data-pipeline-stage]");
  const inspector = root.querySelector("[data-pipeline-inspector]");
  const state = {{
    scale: 1,
    panX: 28,
    panY: 28,
    dragging: false,
    dragStart: null,
    selectedNodeId: model.selectedNodeId || ""
  }};

  function escapeHtml(value) {{
    return String(value == null ? "" : value).replace(/[&<>"']/g, function (char) {{
      return ({{ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }})[char];
    }});
  }}

  function clamp(value, min, max) {{
    return Math.max(min, Math.min(max, value));
  }}

  function visiblePorts(ports) {{
    return Array.isArray(ports) ? ports.filter(function (port) {{ return !port.hidden; }}) : [];
  }}

  function applyTransform() {{
    if (!stage) return;
    stage.style.transform = "translate(" + Math.round(state.panX) + "px, " + Math.round(state.panY) + "px) scale(" + state.scale.toFixed(3) + ")";
  }}

  function fitGraph() {{
    if (!viewport || !stage) return;
    const width = viewport.clientWidth || 720;
    const height = viewport.clientHeight || 420;
    const stageWidth = model.stageWidth || 640;
    const stageHeight = model.stageHeight || 360;
    const minReadableScale = width < 640 ? {MIN_SCALE} : 0.72;
    state.scale = clamp(Math.min(width / stageWidth, height / stageHeight) * 0.92, minReadableScale, 1);
    state.panX = Math.round((width - stageWidth * state.scale) / 2);
    state.panY = Math.round((height - stageHeight * state.scale) / 2);
    applyTransform();
  }}

  function zoomGraph(nextScale, originX, originY) {{
    const previous = state.scale;
    const scale = clamp(nextScale, {MIN_SCALE}, {MAX_SCALE});
    if (scale === previous) return;
    if (originX != null && originY != null) {{
      const worldX = (originX - state.panX) / previous;
      const worldY = (originY - state.panY) / previous;
      state.panX = originX - worldX * scale;
      state.panY = originY - worldY * scale;
    }}
    state.scale = scale;
    applyTransform();
  }}

  function renderPortChip(port) {{
    return '<span class="pipeline-port-chip">' + escapeHtml(port.name) +
      (port.detail ? '<small>' + escapeHtml(port.detail) + '</small>' : '') +
      '</span>';
  }}

  function renderMiniLink(link, nodeId) {{
    const outgoing = link.sourceNodeId === nodeId;
    return '<span class="pipeline-mini-link ' + (outgoing ? "outgoing" : "incoming") + '">' +
      '<small>' + (outgoing ? "out" : "in") + '</small>' +
      '<strong>' + escapeHtml(link.sourceLabel) + ' -&gt; ' + escapeHtml(link.targetLabel) + '</strong>' +
      '</span>';
  }}

  function renderInspector(nodeId) {{
    if (!inspector) return;
    const node = model.nodes.find(function (item) {{ return item.id === nodeId; }}) || model.nodes[0];
    if (!node) {{
      inspector.innerHTML = '<div class="pipeline-empty-slot">No node selected</div>';
      return;
    }}
    const links = model.links.filter(function (link) {{
      return link.sourceNodeId === node.id || link.targetNodeId === node.id;
    }});
    inspector.innerHTML =
      '<div class="pipeline-sidebar-head">' +
        '<span class="panel-label">Inspector</span>' +
        '<strong>' + escapeHtml(node.label) + '</strong>' +
      '</div>' +
      '<div class="pipeline-inspector-body">' +
        '<div class="pipeline-inspector-meta">' +
          '<span>Node <strong>' + escapeHtml(node.subtitle || node.rawId) + '</strong></span>' +
          '<span>Type <strong>' + escapeHtml(node.nodeKind || node.kind) + '</strong></span>' +
          (node.version ? '<span>Version <strong>' + escapeHtml(node.version) + '</strong></span>' : '') +
        '</div>' +
        '<div class="pipeline-inspector-section"><span class="panel-label">Inputs</span>' +
          '<div class="pipeline-chip-list">' + (visiblePorts(node.inputs).map(renderPortChip).join("") || '<div class="pipeline-empty-slot">None</div>') + '</div>' +
        '</div>' +
        '<div class="pipeline-inspector-section"><span class="panel-label">Outputs</span>' +
          '<div class="pipeline-chip-list">' + (visiblePorts(node.outputs).map(renderPortChip).join("") || '<div class="pipeline-empty-slot">None</div>') + '</div>' +
        '</div>' +
        '<div class="pipeline-inspector-section"><span class="panel-label">Links</span>' +
          '<div class="pipeline-link-mini-list">' + (links.map(function (link) {{ return renderMiniLink(link, node.id); }}).join("") || '<div class="pipeline-empty-slot">No links</div>') + '</div>' +
        '</div>' +
      '</div>';
  }}

  function selectNode(nodeId) {{
    if (!nodeId || !model.nodes.some(function (node) {{ return node.id === nodeId; }})) return;
    state.selectedNodeId = nodeId;
    root.querySelectorAll("[data-pipeline-node-id]").forEach(function (node) {{
      node.classList.toggle("selected", node.dataset.pipelineNodeId === nodeId);
    }});
    root.querySelectorAll("[data-pipeline-select-node]").forEach(function (node) {{
      node.classList.toggle("selected", node.dataset.pipelineSelectNode === nodeId);
    }});
    renderInspector(nodeId);
  }}

  root.addEventListener("click", function (event) {{
    const target = event.target;
    if (!(target instanceof Element)) return;
    const control = target.closest("[data-pipeline-control]");
    if (control) {{
      const action = control.dataset.pipelineControl;
      if (action === "fit") fitGraph();
      if (action === "reset") {{ state.scale = 1; state.panX = 28; state.panY = 28; applyTransform(); }}
      if (action === "zoomIn") zoomGraph(state.scale + 0.14);
      if (action === "zoomOut") zoomGraph(state.scale - 0.14);
      if (action === "fullscreen") {{
        root.classList.toggle("pipeline-graph-is-fullscreen");
        setTimeout(fitGraph, 40);
      }}
      return;
    }}
    const outlinerNode = target.closest("[data-pipeline-select-node]");
    if (outlinerNode) {{
      selectNode(outlinerNode.dataset.pipelineSelectNode);
      return;
    }}
    const graphNode = target.closest("[data-pipeline-node-id]");
    if (graphNode) selectNode(graphNode.dataset.pipelineNodeId);
  }});

  root.addEventListener("keydown", function (event) {{
    const target = event.target;
    if (!(target instanceof Element)) return;
    const graphNode = target.closest("[data-pipeline-node-id]");
    if (graphNode && (event.key === "Enter" || event.key === " ")) {{
      event.preventDefault();
      selectNode(graphNode.dataset.pipelineNodeId);
    }}
    if (event.key === "Escape" && root.classList.contains("pipeline-graph-is-fullscreen")) {{
      root.classList.remove("pipeline-graph-is-fullscreen");
      setTimeout(fitGraph, 40);
    }}
  }});

  if (viewport) {{
    viewport.addEventListener("wheel", function (event) {{
      event.preventDefault();
      const delta = event.deltaY > 0 ? -0.08 : 0.08;
      const rect = viewport.getBoundingClientRect();
      zoomGraph(state.scale + delta, event.clientX - rect.left, event.clientY - rect.top);
    }}, {{ passive: false }});

    viewport.addEventListener("pointerdown", function (event) {{
      if (event.button !== 0 || event.target.closest && event.target.closest("[data-pipeline-node-id],button,a")) return;
      viewport.setPointerCapture && viewport.setPointerCapture(event.pointerId);
      state.dragging = true;
      state.dragStart = {{ x: event.clientX, y: event.clientY, panX: state.panX, panY: state.panY }};
      viewport.classList.add("panning");
    }});
    viewport.addEventListener("pointermove", function (event) {{
      if (!state.dragging) return;
      state.panX = state.dragStart.panX + event.clientX - state.dragStart.x;
      state.panY = state.dragStart.panY + event.clientY - state.dragStart.y;
      applyTransform();
    }});
    viewport.addEventListener("pointerup", function (event) {{
      if (!state.dragging) return;
      viewport.releasePointerCapture && viewport.releasePointerCapture(event.pointerId);
      state.dragging = false;
      viewport.classList.remove("panning");
    }});
  }}

  applyTransform();
  setTimeout(fitGraph, 30);
  if (typeof ResizeObserver !== "undefined" && viewport) {{
    new ResizeObserver(function () {{ fitGraph(); }}).observe(viewport);
  }}
}})();
"""
