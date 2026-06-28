"""Metadata extraction for SPL objects stored by the daemon.

The daemon should be able to answer registry questions without importing and
executing user code.  SPL/YAML already contains enough IR data for that: function
ports, pipeline nodes, aliases, and captured Python distribution versions.  This
module turns that YAML into plain JSON-compatible dictionaries for the SQLite
registry.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import yaml

from spl.core.entities.distribution import DDistribution
from spl.core.entities.function import DFunction
from spl.core.entities.module import DImport, DImportFrom
from spl.core.entities.node import DNodeInputRef, DNodeOutputRef, InputPort, OutputPort
from spl.core.entities.node_function import DNodeFunction
from spl.core.entities.node_remote import DNodeRemote
from spl.core.entities.pipeline import DPipeline
from spl.core.entities.scalar import DScalar
from spl.core.ir.utils import SPLSafeLoader


RemoteSignatureResolver = Callable[[dict[str, Any]], dict[str, Any]]


def extract_metadata(
    yaml_text: str,
    entrypoint: str,
    *,
    remote_signature_resolver: RemoteSignatureResolver | None = None,
) -> dict[str, Any]:
    """Return daemon registry metadata for one SPL/YAML entrypoint.

    The result deliberately contains only JSON-compatible values so it can be
    stored in SQLite and returned directly through the HTTP API.
    """

    documents = _load_documents(yaml_text)
    functions = _collect_functions(documents)
    distributions = _collect_distributions(documents)
    imports = _collect_imports(documents)
    root = _find_entrypoint(documents, entrypoint)

    if isinstance(root, DFunction):
        inputs = [_input_port_to_dict(port) for port in root.inputs]
        outputs = [_output_port_to_dict(port) for port in (root.outputs or [])]
        return {
            "entrypoint": entrypoint,
            "kind": "function",
            "inputs": inputs,
            "outputs": outputs,
            "pipeline_nodes": [],
            "internal_objects": [
                {
                    "kind": "function",
                    "name": root.name,
                    "inputs": inputs,
                    "outputs": outputs,
                }
            ],
            "distributions": distributions,
            "imports": imports,
        }

    if isinstance(root, DPipeline):
        pipeline_metadata = _pipeline_metadata(
            root,
            functions,
            remote_signature_resolver=remote_signature_resolver,
        )
        return {
            "entrypoint": entrypoint,
            "kind": "pipeline",
            "inputs": pipeline_metadata["inputs"],
            "outputs": pipeline_metadata["outputs"],
            "pipeline_nodes": pipeline_metadata["nodes"],
            "internal_objects": pipeline_metadata["internal_objects"],
            "aliases": pipeline_metadata["aliases"],
            "links": pipeline_metadata["links"],
            "distributions": distributions,
            "imports": imports,
        }

    raise TypeError(
        f"entrypoint must be a DFunction or DPipeline, got {type(root).__name__}"
    )


def _load_documents(yaml_text: str) -> list[tuple[Any, list[Any]]]:
    raw_documents = list(yaml.load_all(yaml_text, SPLSafeLoader))
    documents: list[tuple[Any, list[Any]]] = []
    for index, document in enumerate(raw_documents, start=1):
        if not isinstance(document, list) or len(document) == 0:
            raise ValueError(f"SPL YAML document #{index} must be a non-empty list")
        root, *dependencies = document
        documents.append((root, dependencies))
    if not documents:
        raise ValueError("SPL YAML does not contain any documents")
    return documents


def _find_entrypoint(documents: list[tuple[Any, list[Any]]], entrypoint: str) -> Any:
    for root, _ in documents:
        if getattr(root, "name", None) == entrypoint:
            return root
    available = sorted(
        str(name)
        for root, _ in documents
        if (name := getattr(root, "name", None)) is not None
    )
    raise KeyError(
        "entrypoint is not found in SPL YAML: "
        f"{entrypoint}; available: {', '.join(available) or '<none>'}"
    )


def _collect_functions(documents: list[tuple[Any, list[Any]]]) -> dict[str, DFunction]:
    functions: dict[str, DFunction] = {}
    for root, dependencies in documents:
        for item in [root, *dependencies]:
            if isinstance(item, DFunction):
                functions[item.name] = item
    return functions


def _collect_distributions(documents: list[tuple[Any, list[Any]]]) -> list[dict[str, str]]:
    unique = {
        (item.package, item.version)
        for _, dependencies in documents
        for item in dependencies
        if isinstance(item, DDistribution)
    }
    return [
        {"package": package, "version": version}
        for package, version in sorted(unique)
    ]


def _collect_imports(documents: list[tuple[Any, list[Any]]]) -> list[dict[str, Any]]:
    imports: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for _, dependencies in documents:
        for item in dependencies:
            if isinstance(item, DImport):
                key = ("import", item.module, item.alias)
                payload = {
                    "kind": "import",
                    "module": item.module,
                    "alias": item.alias,
                }
            elif isinstance(item, DImportFrom):
                key = ("from", item.module, item.target, item.alias)
                payload = {
                    "kind": "from",
                    "module": item.module,
                    "target": item.target,
                    "alias": item.alias,
                }
            else:
                continue
            if key not in seen:
                seen.add(key)
                imports.append(payload)
    return imports


def _pipeline_metadata(
    pipeline: DPipeline,
    functions: dict[str, DFunction],
    *,
    remote_signature_resolver: RemoteSignatureResolver | None,
) -> dict[str, Any]:
    node_infos = {
        node.uuid: _pipeline_node_to_dict(
            node,
            functions,
            remote_signature_resolver=remote_signature_resolver,
        )
        for node in sorted(pipeline.nodes, key=lambda node: node.uuid)
    }
    nodes = [node_infos[node.uuid] for node in sorted(pipeline.nodes, key=lambda node: node.uuid)]
    node_by_uuid = {
        node.uuid: node_infos[node.uuid]
        for node in pipeline.nodes
    }

    bound_inputs = {
        (link_from.uuid, link_from.port)
        for link_from, _ in pipeline.links
        if isinstance(link_from, DNodeInputRef)
    }

    free_inputs: list[dict[str, Any]] = []
    for node in sorted(pipeline.nodes, key=lambda item: item.uuid):
        node_info = node_infos[node.uuid]
        for port in node_info["inputs"]:
            port_name = port["name"]
            if (node.uuid, port_name) not in bound_inputs:
                payload = {
                    **port,
                    "node_id": node.uuid,
                    "external_name": port_name,
                }
                if node_info["kind"] == "function":
                    payload["function"] = node_info.get("function")
                else:
                    payload["remote"] = node_info.get("remote")
                free_inputs.append(
                    payload
                )

    aliases = [
        {"name": name, "node_id": node_uuid}
        for name, node_uuid in sorted(pipeline.aliases, key=lambda item: item[0])
    ]

    outputs = _pipeline_outputs(pipeline, node_by_uuid)
    internal_objects = []
    for node_info in nodes:
        if node_info["kind"] == "function":
            internal_objects.append(
                {
                    "kind": "function",
                    "name": node_info["function"],
                    "inputs": node_info["inputs"],
                    "outputs": node_info["outputs"],
                }
            )
        else:
            internal_objects.append(
                {
                    "kind": "remote",
                    "name": node_info["name"],
                    "remote": node_info["remote"],
                    "inputs": node_info["inputs"],
                    "outputs": node_info["outputs"],
                }
            )

    return {
        "nodes": nodes,
        "inputs": free_inputs,
        "outputs": outputs,
        "internal_objects": internal_objects,
        "aliases": aliases,
        "links": [_pipeline_link_to_dict(link) for link in pipeline.links],
    }


def _pipeline_outputs(
    pipeline: DPipeline,
    node_by_uuid: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if pipeline.aliases:
        outputs: list[dict[str, Any]] = []
        for alias, node_uuid in sorted(pipeline.aliases, key=lambda item: item[0]):
            node_info = node_by_uuid.get(node_uuid)
            ports = (node_info or {}).get("outputs") or []
            payload = {
                "name": alias,
                "node_id": node_uuid,
                "ports": ports,
            }
            if node_info and node_info["kind"] == "function":
                payload["function"] = node_info.get("function")
            elif node_info:
                payload["remote"] = node_info.get("remote")
            outputs.append(
                payload
            )
        return outputs

    outputs = []
    for node_uuid, node_info in sorted(node_by_uuid.items(), key=lambda item: item[0]):
        for port in node_info.get("outputs") or []:
            payload = {
                **port,
                "node_id": node_uuid,
            }
            if node_info["kind"] == "function":
                payload["function"] = node_info.get("function")
            else:
                payload["remote"] = node_info.get("remote")
            outputs.append(payload)
    return outputs


def _pipeline_node_to_dict(
    node: DNodeFunction | DNodeRemote,
    functions: dict[str, DFunction],
    *,
    remote_signature_resolver: RemoteSignatureResolver | None,
) -> dict[str, Any]:
    if isinstance(node, DNodeFunction):
        function = functions.get(node.func)
        return {
            "id": node.uuid,
            "kind": "function",
            "function": node.func,
            "name": node.func,
            "inputs": [
                _input_port_to_dict(port)
                for port in (function.inputs if function is not None else [])
            ],
            "outputs": [
                _output_port_to_dict(port)
                for port in ((function.outputs or []) if function is not None else [])
            ],
        }
    if isinstance(node, DNodeRemote):
        return _remote_node_to_dict(node, remote_signature_resolver)
    raise TypeError(f"unsupported pipeline node: {type(node).__name__}")


def _remote_node_to_dict(
    node: DNodeRemote,
    remote_signature_resolver: RemoteSignatureResolver | None,
) -> dict[str, Any]:
    ref = {
        "url": node.url,
        "name": node.name,
        "version": node.version,
    }
    for attr in ("owner_id", "library", "target_machine"):
        value = getattr(node, attr, None)
        if value is not None:
            ref[attr] = value
    if remote_signature_resolver is None:
        raise ValueError(
            "pipeline contains DNodeRemote but no remote signature resolver "
            f"is configured: {node.url}/{node.name}:{node.version}"
        )

    signature = remote_signature_resolver(ref)
    inputs = [_signature_input_to_port(item) for item in signature.get("inputs") or []]
    outputs = _signature_outputs_to_ports(signature.get("outputs") or [])
    if not outputs:
        outputs = [OutputPort(name="default", typ_=None)]
    return {
        "id": node.uuid,
        "kind": "remote",
        "name": node.name,
        "remote": {
            "url": node.url,
            "name": node.name,
            "version": node.version,
            "version_id": signature.get("version_id"),
            "object_id": signature.get("id"),
            "owner_id": signature.get("owner_id"),
            "library": (signature.get("remote_ref") or {}).get("library")
            or (signature.get("library") or {}).get("slug"),
            "target_machine": ref.get("target_machine")
            or signature.get("target_machine")
            or (signature.get("execution") or {}).get("default_machine_id"),
            "kind": signature.get("kind"),
            "signature": signature,
        },
        "inputs": [_input_port_to_dict(port) for port in inputs],
        "outputs": [_output_port_to_dict(port) for port in outputs],
    }


def _signature_input_to_port(item: dict[str, Any]) -> InputPort:
    return InputPort(
        name=str(item.get("name") or "default"),
        typ_=item.get("type"),
        default=item.get("default"),
    )


def _signature_outputs_to_ports(outputs: list[dict[str, Any]]) -> list[OutputPort]:
    ports: list[OutputPort] = []
    seen: set[str] = set()
    for item in outputs:
        raw_ports = item.get("ports")
        if raw_ports:
            candidates = raw_ports
        else:
            candidates = [{"name": item.get("name") or "default", "type": item.get("type")}]
        for port in candidates:
            name = str(port.get("name") or "default")
            if name in seen:
                continue
            seen.add(name)
            ports.append(OutputPort(name=name, typ_=port.get("type")))
    return ports


def _pipeline_link_to_dict(link: Any) -> dict[str, Any]:
    link_from, link_to = link
    return {
        "from": _node_ref_to_dict(link_from),
        "to": _link_value_to_dict(link_to),
    }


def _node_ref_to_dict(value: DNodeInputRef | DNodeOutputRef) -> dict[str, str]:
    return {"node_id": value.uuid, "port": value.port}


def _link_value_to_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, DNodeOutputRef):
        return {"kind": "node_output", **_node_ref_to_dict(value)}
    if isinstance(value, DScalar):
        return {"kind": "scalar", "value": value.value}
    return {"kind": type(value).__name__, "value": repr(value)}


def _input_port_to_dict(port: InputPort) -> dict[str, Any]:
    return {
        "name": port.name,
        "type": port.typ_,
        "default": port.default,
        "required": port.default is None,
    }


def _output_port_to_dict(port: OutputPort) -> dict[str, Any]:
    return {
        "name": port.name,
        "type": port.typ_,
    }
