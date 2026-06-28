"""Human-sized object views for the local daemon API."""

from __future__ import annotations

from typing import Any


def summarize_object(record: dict[str, Any]) -> dict[str, Any]:
    """Return a compact registry row suitable for object lists."""

    signature = build_signature(record)
    return {
        "name": signature["name"],
        "display_name": signature["display_name"],
        "origin": signature["origin"],
        "kind": signature["kind"],
        "version": signature["version"],
        "version_id": signature["version_id"],
        "description": signature["description"],
        "inputs": [
            {
                "name": item["name"],
                "type": item["type"],
                "required": item["required"],
                "default": item["default"],
            }
            for item in signature["inputs"]
        ],
        "outputs": [
            {
                "name": item["name"],
                "selector": item["selector"],
                "type": item["type"],
                "read": item["read"],
            }
            for item in signature["outputs"]
        ],
    }


def build_signature(
    record: dict[str, Any],
    *,
    function: str | None = None,
) -> dict[str, Any]:
    """Build a concise call/read signature from stored daemon metadata."""

    if function is not None:
        return _build_internal_function_signature(record, function)

    kind = record.get("kind") or record.get("type") or "unknown"
    inputs = _normalize_inputs(record.get("inputs") or [])
    outputs = _normalize_outputs(kind, record.get("outputs") or [])
    display_name = record.get("display_name") or record["name"]
    return {
        "name": record["name"],
        "display_name": display_name,
        "origin": record.get("origin", "local"),
        "id": record["id"],
        "version": record["version"],
        "version_id": record["version_id"],
        "kind": kind,
        "description": record.get("description") or "",
        "inputs": inputs,
        "outputs": outputs,
        "pipeline_nodes": record.get("pipeline_nodes") or [],
        "remote_nodes": [
            node
            for node in record.get("pipeline_nodes") or []
            if node.get("kind") == "remote"
        ],
        "internal_objects": record.get("internal_objects") or [],
        "internal_functions": _internal_functions(record),
        "call": _call_help(display_name, kind, inputs, outputs),
    }


def _build_internal_function_signature(
    record: dict[str, Any],
    function: str,
) -> dict[str, Any]:
    item = _find_internal_function(record, function)
    parent_name = record["name"]
    parent_display_name = record.get("display_name") or parent_name
    display_name = f"{parent_display_name}::{item['name']}"
    canonical_name = f"{parent_name}::{item['name']}"
    inputs = _normalize_inputs(item.get("inputs") or [])
    outputs = _normalize_outputs("function", item.get("outputs") or [])
    return {
        "name": canonical_name,
        "display_name": display_name,
        "origin": record.get("origin", "local"),
        "id": record["id"],
        "version": record["version"],
        "version_id": record["version_id"],
        "kind": "function",
        "description": record.get("description") or "",
        "inputs": inputs,
        "outputs": outputs,
        "pipeline_nodes": [],
        "remote_nodes": [],
        "internal_objects": [],
        "internal_functions": [],
        "function": item["name"],
        "entrypoint": item["name"],
        "parent_object": {
            "name": parent_name,
            "display_name": parent_display_name,
            "kind": record.get("kind") or record.get("type") or "unknown",
            "version": record["version"],
            "version_id": record["version_id"],
        },
        "call": _call_help(
            parent_display_name,
            "function",
            inputs,
            outputs,
            function=item["name"],
        ),
    }


def _internal_functions(record: dict[str, Any]) -> list[dict[str, Any]]:
    functions = []
    seen: set[str] = set()
    for item in record.get("functions") or []:
        if item.get("kind") != "function":
            continue
        name = str(item.get("name") or "")
        if not name or name in seen:
            continue
        seen.add(name)
        functions.append(
            {
                "name": name,
                "role": item.get("role"),
                "node_id": item.get("node_id"),
                "inputs": item.get("inputs") or [],
                "outputs": item.get("outputs") or [],
            }
        )
    for item in record.get("internal_objects") or []:
        if item.get("kind") != "function":
            continue
        name = str(item.get("name") or "")
        if not name or name in seen:
            continue
        seen.add(name)
        functions.append(
            {
                "name": name,
                "role": "pipeline_component",
                "node_id": item.get("node_id"),
                "inputs": item.get("inputs") or [],
                "outputs": item.get("outputs") or [],
            }
        )
    return sorted(functions, key=lambda item: item["name"])


def _find_internal_function(
    record: dict[str, Any],
    function: str,
) -> dict[str, Any]:
    function = str(function)
    for item in _internal_functions(record):
        if item["name"] == function:
            return item
    available = ", ".join(item["name"] for item in _internal_functions(record))
    raise KeyError(
        f"function is not found in object {record['name']}: "
        f"{function}; available: {available or '<none>'}"
    )


def _normalize_inputs(inputs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    for item in inputs:
        name = str(item.get("external_name") or item.get("name"))
        if name not in by_name:
            by_name[name] = {
                "name": name,
                "type": item.get("type"),
                "required": bool(item.get("required", item.get("default") is None)),
                "default": item.get("default"),
                "ui": _input_ui(item),
                "sources": [],
            }
        by_name[name]["sources"].append(
            {
                "node_id": item.get("node_id"),
                "function": item.get("function"),
                "port": item.get("name"),
            }
        )
        by_name[name]["required"] = by_name[name]["required"] or bool(
            item.get("required", item.get("default") is None)
        )
    return [by_name[name] for name in sorted(by_name)]


def _normalize_outputs(kind: str, outputs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if kind == "pipeline":
        return _pipeline_outputs(outputs)
    return _function_outputs(outputs)


def _function_outputs(outputs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not outputs:
        outputs = [{"name": "default", "type": None}]
    result = []
    for item in outputs:
        name = str(item.get("name") or "default")
        result.append(
            {
                "name": name,
                "selector": None,
                "type": item.get("type"),
                "ports": [{"name": name, "type": item.get("type")}],
                "read": "result.value",
                "result_accessor": "result.value",
                "value_path": [],
                "notes": "Function calls return the function value directly.",
            }
        )
    return result


def _pipeline_outputs(outputs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not outputs:
        return []

    result = []
    for item in outputs:
        if "ports" in item:
            alias = str(item["name"])
            ports = _ports(item.get("ports") or [{"name": "default", "type": None}])
            default_port = _default_port_name(ports)
            result.append(
                {
                    "name": alias,
                    "selector": alias,
                    "type": _single_port_type(ports),
                    "ports": ports,
                    "read": f'result.value["{default_port}"]',
                    "result_accessor": f'result.value["{default_port}"]',
                    "value_path": [default_port],
                    "read_without_output_selector": (
                        f'result.value["{alias}"]["{default_port}"]'
                    ),
                    "notes": (
                        f'Pass output="{alias}". SPL pipeline nodes return a '
                        "port map; for the common single-output case read the "
                        f'"{default_port}" port.'
                    ),
                }
            )
            continue

        port_name = str(item.get("name") or "default")
        result.append(
            {
                "name": port_name,
                "selector": None,
                "type": item.get("type"),
                "ports": [{"name": port_name, "type": item.get("type")}],
                "read": f'result.value["{port_name}"]',
                "result_accessor": f'result.value["{port_name}"]',
                "value_path": [port_name],
                "node_id": item.get("node_id"),
                "function": item.get("function"),
                "notes": "Pipeline output is returned as a node port map.",
            }
        )
    return result


def _ports(raw_ports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "name": str(item.get("name") or "default"),
            "type": item.get("type"),
        }
        for item in raw_ports
    ]


def _default_port_name(ports: list[dict[str, Any]]) -> str:
    for item in ports:
        if item["name"] == "default":
            return "default"
    return ports[0]["name"] if ports else "default"


def _single_port_type(ports: list[dict[str, Any]]) -> Any:
    if len(ports) == 1:
        return ports[0].get("type")
    return None


def _input_ui(item: dict[str, Any]) -> dict[str, Any]:
    type_text = str(item.get("type") or "").lower()
    if type_text in {"int", "integer"}:
        widget = "number"
        input_type = "number"
    elif type_text in {"float", "double", "decimal", "number"}:
        widget = "number"
        input_type = "number"
    elif type_text in {"bool", "boolean"}:
        widget = "checkbox"
        input_type = "checkbox"
    elif type_text in {"dict", "list", "tuple", "set", "json", "object", "array"}:
        widget = "json"
        input_type = "text"
    else:
        widget = "text"
        input_type = "text"
    return {
        "widget": widget,
        "input_type": input_type,
        "placeholder": item.get("default") if item.get("default") is not None else item.get("type") or "value",
    }


def _call_help(
    name: str,
    kind: str,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    *,
    function: str | None = None,
) -> dict[str, Any]:
    kwargs_template = {
        item["name"]: f"<{item['type'] or 'value'}>"
        for item in inputs
    }
    output_values = [
        item["selector"]
        for item in outputs
        if item.get("selector") is not None
    ]
    first_output = outputs[0] if outputs else None
    output_arg = (
        f', output="{first_output["selector"]}"'
        if first_output and first_output.get("selector")
        else ""
    )
    function_arg = f', function="{function}"' if function is not None else ""
    return {
        "kwargs": kwargs_template,
        "output_values": output_values,
        "schema": {
            "kwargs": [
                {
                    "name": item["name"],
                    "type": item["type"],
                    "required": item["required"],
                    "default": item["default"],
                    "ui": item.get("ui") or {},
                }
                for item in inputs
            ],
            "outputs": output_values,
            "raw_json_template": kwargs_template,
        },
        "example": (
            f'result = client.call("{name}", '
            f'kwargs={kwargs_template}{output_arg}{function_arg})'
        ),
        "read": first_output.get("read") if first_output else "result.value",
        "result_shape": (
            "pipeline_node_port_map"
            if kind == "pipeline"
            else "function_value"
        ),
    }
