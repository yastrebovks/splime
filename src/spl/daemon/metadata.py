"""Metadata extraction for SPL objects stored by the daemon.

The daemon should be able to answer registry questions without importing and
executing user code.  SPL/YAML already contains enough IR data for that: function
ports, pipeline nodes, aliases, and captured Python distribution versions.  This
module turns that YAML into plain JSON-compatible dictionaries for the SQLite
registry.
"""

from __future__ import annotations

import ast
import keyword
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from spl.core._graph import canonical_uuid_key
from spl.core.adapter_compat import resolve_yaml_edge_adapters
from spl.core.entities.adapter import DAdapter, DLoadAdapter, DSaveAdapter
from spl.core.entities.artifact import DArtifactRef
from spl.core.entities.control import DSPLImport, DSPLSelfImport
from spl.core.entities.distribution import DDistribution
from spl.core.entities.function import DFunction
from spl.core.entities.local_function import DLocalAlias
from spl.core.entities.module import DImport, DImportFrom
from spl.core.entities.node import (
    DFormattedOutputRef,
    DNodeInputRef,
    DNodeOutputRef,
    InputPort,
    OutputPort,
)
from spl.core.entities.node_function import DNodeFunction
from spl.core.entities.node_remote import DNodeRemote
from spl.core.entities.pipeline import DPipeline, validate_pipeline_ir
from spl.core.entities.scalar import DScalar, scalar_value_expression
from spl.core.ir.utils import SPLSafeLoader
from spl.core.node_runtime import NODE_RUNTIME_BACKENDS, RUNTIME_TAG_NAME

RemoteSignatureResolver = Callable[[dict[str, Any]], dict[str, Any]]
ObjectDocument = tuple[Any, tuple[Any, ...]]
RemoteSignatureKey = tuple[str, str]


@dataclass(frozen=True)
class ObjectIR:
    """Parsed object IR plus resolved remote interfaces and registration context."""

    documents: tuple[ObjectDocument, ...]
    entrypoint: str
    root: DFunction | DPipeline
    remote_signatures: Mapping[RemoteSignatureKey, Mapping[str, Any]]
    runtime_config: Mapping[str, Any]
    source: Path


@dataclass(frozen=True)
class _NodeInterface:
    node_id: str
    alias: str
    reference: str
    inputs: Mapping[str, InputPort]
    outputs: Mapping[str, OutputPort]
    resolved: bool


def extract_metadata(
    yaml_text: str,
    entrypoint: str,
    *,
    remote_signature_resolver: RemoteSignatureResolver | None = None,
    runtime_config: Mapping[str, Any] | None = None,
    source: Path | None = None,
) -> dict[str, Any]:
    """Return daemon registry metadata for one SPL/YAML entrypoint.

    The result deliberately contains only JSON-compatible values so it can be
    stored in SQLite and returned directly through the HTTP API.
    """

    ir = load_object_ir(
        yaml_text,
        entrypoint,
        remote_signature_resolver=remote_signature_resolver,
        runtime_config=runtime_config,
        source=source,
    )
    return extract_object_ir_metadata(ir)


def load_object_ir(
    yaml_text: str,
    entrypoint: str,
    *,
    remote_signature_resolver: RemoteSignatureResolver | None = None,
    runtime_config: Mapping[str, Any] | None = None,
    source: Path | None = None,
) -> ObjectIR:
    """Parse one registration payload and resolve remote interfaces without persistence."""

    documents = _load_documents(yaml_text)
    root = _find_entrypoint(documents, entrypoint)
    source_path = source or Path("<daemon-metadata>")
    preliminary = ObjectIR(
        documents=tuple((document_root, tuple(dependencies)) for document_root, dependencies in documents),
        entrypoint=entrypoint,
        root=root,
        remote_signatures={},
        runtime_config=dict(runtime_config or {}),
        source=source_path,
    )
    _validate_object_ir(preliminary, require_remote_signatures=False)
    remote_signatures: dict[RemoteSignatureKey, Mapping[str, Any]] = {}
    pipelines = sorted(
        (document_root for document_root, _ in documents if isinstance(document_root, DPipeline)),
        key=lambda pipeline: pipeline.name,
    )
    for pipeline in pipelines:
        for node in sorted(pipeline.nodes, key=lambda item: canonical_uuid_key(item.uuid)):
            if not isinstance(node, DNodeRemote):
                continue
            if remote_signature_resolver is None:
                raise ValueError(
                    f"pipeline `{pipeline.name}` contains DNodeRemote but no remote signature resolver "
                    f"is configured: {node.url}/{node.name}:{node.version}"
                )
            signature = remote_signature_resolver(_remote_ref(node))
            if not isinstance(signature, dict):
                raise ValueError(
                    "remote signature resolver must return an object for node "
                    f"`{node.name}` ({node.uuid}), got {type(signature).__name__}"
                )
            remote_signatures[_remote_signature_key(pipeline, node)] = _stable_remote_signature(signature)
    if not isinstance(root, DFunction | DPipeline):
        raise TypeError(f"entrypoint must be a DFunction or DPipeline, got {type(root).__name__}")
    return ObjectIR(
        documents=tuple((document_root, tuple(dependencies)) for document_root, dependencies in documents),
        entrypoint=entrypoint,
        root=root,
        remote_signatures=remote_signatures,
        runtime_config=dict(runtime_config or {}),
        source=source_path,
    )


def validate_object_ir(ir: ObjectIR) -> None:
    """Validate complete object IR semantics without reading or mutating repository state."""

    _validate_object_ir(ir, require_remote_signatures=True)


def _validate_object_ir(ir: ObjectIR, *, require_remote_signatures: bool) -> None:
    _validate_document_roots(ir)
    _validate_unique_root_symbols(ir)
    functions = _validate_function_definitions(ir)
    _validate_executable_symbols(ir)
    _validate_declared_symbol_references(ir)
    _validate_scalar_values(ir)
    _validate_object_runtime(ir)
    callable_symbols = _declared_callable_symbols(ir)
    for pipeline, _ in ir.documents:
        if not isinstance(pipeline, DPipeline):
            continue
        validate_pipeline_ir(pipeline, source=ir.source)
        aliases = _validate_pipeline_aliases(ir, pipeline)
        interfaces = _validate_pipeline_nodes(
            ir,
            pipeline,
            functions,
            callable_symbols,
            aliases,
            require_remote_signatures=require_remote_signatures,
        )
        _validate_pipeline_tags(ir, pipeline, interfaces)
        _validate_pipeline_adapters(ir, pipeline)
        _validate_pipeline_links(ir, pipeline, interfaces)


def extract_object_ir_metadata(ir: ObjectIR) -> dict[str, Any]:
    """Return registry metadata for object IR that passed semantic validation."""

    validate_object_ir(ir)
    documents = [(root, list(dependencies)) for root, dependencies in ir.documents]
    functions = _collect_functions(documents)
    distributions = _collect_distributions(documents)
    imports = _collect_imports(documents)
    root = ir.root

    if isinstance(root, DFunction):
        inputs = [_input_port_to_dict(port) for port in root.inputs]
        outputs = [_output_port_to_dict(port) for port in (root.outputs or [])]
        return {
            "entrypoint": ir.entrypoint,
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
            callable_symbols=_declared_callable_symbols(ir),
            remote_signatures=ir.remote_signatures,
            source=ir.source,
        )
        return {
            "entrypoint": ir.entrypoint,
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

    raise AssertionError("validated object IR has an unsupported entrypoint")


def _iter_object_items(ir: ObjectIR) -> Iterator[tuple[Any, str]]:
    source = str(ir.source)
    for document_index, (root, dependencies) in enumerate(ir.documents):
        yield root, f"{source}:documents[{document_index}].root"
        for dependency_index, dependency in enumerate(dependencies):
            yield dependency, f"{source}:documents[{document_index}].dependencies[{dependency_index}]"


def _validate_scalar_values(ir: ObjectIR) -> None:
    def validate(value: Any, *, location: str) -> None:
        if not isinstance(value, DScalar):
            return
        try:
            scalar_value_expression(value.value)
        except TypeError as exc:
            raise ValueError(f"invalid scalar literal (location: `{location}`): {exc}") from exc

    for item, location in _iter_object_items(ir):
        validate(item, location=location)
        if not isinstance(item, DPipeline):
            continue
        for link_index, link in enumerate(item.links):
            for endpoint_index, endpoint in enumerate(link):
                validate(
                    endpoint,
                    location=f"{location}.links[{link_index}][{endpoint_index}]",
                )


def _stable_remote_signature(signature: Mapping[str, Any]) -> dict[str, Any]:
    """Remove transport diagnostics that must not affect object version identity."""

    return {
        key: value
        for key, value in signature.items()
        if key not in {"cache_status", "cache_error", "resolved_from", "resolution"}
    }


def _remote_signature_key(pipeline: DPipeline, node: DNodeRemote) -> RemoteSignatureKey:
    return pipeline.name, canonical_uuid_key(node.uuid)


def _validate_identifier(value: Any, *, description: str, location: str) -> None:
    if not isinstance(value, str) or not value.isidentifier() or keyword.iskeyword(value):
        raise ValueError(f"{description} `{value}` is not a valid Python identifier (location: `{location}`)")


def _validate_document_roots(ir: ObjectIR) -> None:
    for document_index, (root, _) in enumerate(ir.documents):
        location = f"{ir.source}:documents[{document_index}].root"
        if not isinstance(root, DFunction | DPipeline):
            raise ValueError(
                "SPL YAML document root must be a DFunction or DPipeline, got `{}` "
                "(location: `{}`); register a self-contained executable object bundle".format(
                    type(root).__name__,
                    location,
                )
            )
        _validate_identifier(root.name, description="object root name", location=location)


def _validate_unique_root_symbols(ir: ObjectIR) -> None:
    locations_by_name: dict[str, list[str]] = {}
    for document_index, (root, _) in enumerate(ir.documents):
        name = getattr(root, "name", None)
        if not isinstance(name, str) or not name:
            continue
        locations_by_name.setdefault(name, []).append(f"{ir.source}:documents[{document_index}].root")
    for name in sorted(locations_by_name):
        locations = locations_by_name[name]
        if len(locations) > 1:
            raise ValueError(
                "object root symbol `{}` is defined more than once (locations: {})".format(
                    name,
                    ", ".join("`{}`".format(location) for location in locations),
                )
            )


def _validate_function_definitions(ir: ObjectIR) -> dict[str, DFunction]:
    definitions: dict[str, tuple[DFunction, str]] = {}
    for item, location in _iter_object_items(ir):
        if not isinstance(item, DFunction):
            continue
        if not isinstance(item.name, str) or not item.name:
            raise ValueError(f"object function name must be a non-empty string (location: `{location}`)")
        existing = definitions.get(item.name)
        if existing is not None:
            raise ValueError(
                "object function symbol `{}` is defined more than once (locations: `{}`, `{}`)".format(
                    item.name,
                    existing[1],
                    location,
                )
            )
        _validate_declared_ports(item.inputs, kind="input", owner=f"function `{item.name}`", location=location)
        _validate_declared_ports(item.outputs or [], kind="output", owner=f"function `{item.name}`", location=location)
        _validate_function_syntax(item, location=location)
        definitions[item.name] = (item, location)
    return {name: definition for name, (definition, _) in definitions.items()}


def _validate_declared_symbol_references(ir: ObjectIR) -> None:
    root_symbols = {item.name for item, _ in _iter_object_items(ir) if isinstance(item, DFunction | DPipeline)}
    declared = _declared_symbols(ir)
    for item, location in _iter_object_items(ir):
        if isinstance(item, DSPLImport):
            raise ValueError(
                "external SPL import `{}` from path `{}` cannot be registered as one daemon object "
                "(location: `{}`); export a self-contained YAML file with `spl_export_to_file` "
                "or inline the referenced object".format(item.name, item.path, location)
            )
        if isinstance(item, DSPLSelfImport) and item.name not in root_symbols:
            raise ValueError(
                f"SPL self-import references missing object symbol `{item.name}` (location: `{location}`); "
                "include that object in the same YAML bundle"
            )
        if isinstance(item, DLocalAlias) and item.target not in declared:
            raise ValueError(
                f"local alias `{item.alias}` references missing symbol `{item.target}` (location: `{location}`)"
            )


def _validate_executable_symbols(ir: ObjectIR) -> None:
    bindings: dict[str, tuple[tuple[Any, ...], str, bool]] = {}
    for document_index, (root, dependencies) in enumerate(ir.documents):
        items = [(root, f"{ir.source}:documents[{document_index}].root")]
        items.extend(
            (dependency, f"{ir.source}:documents[{document_index}].dependencies[{dependency_index}]")
            for dependency_index, dependency in enumerate(dependencies)
        )
        for item, location in items:
            symbol: str | None = None
            semantic_key: tuple[Any, ...] = ()
            allow_identical_rebinding = False
            if isinstance(item, DFunction | DPipeline):
                symbol = item.name
                semantic_key = (type(item).__name__, item.name)
            elif isinstance(item, DLocalAlias):
                symbol = item.alias
                semantic_key = ("local_alias", item.alias, item.target)
                allow_identical_rebinding = True
            elif isinstance(item, DImport):
                symbol = item.alias or item.module.split(".")[0]
                semantic_key = ("import", item.module, item.alias)
                allow_identical_rebinding = True
            elif isinstance(item, DImportFrom):
                symbol = item.alias or item.target
                semantic_key = ("import_from", item.module, item.target, item.alias)
                allow_identical_rebinding = True
            if symbol is None:
                continue
            existing = bindings.get(symbol)
            if existing is not None:
                existing_key, existing_location, existing_allows_identical = existing
                if allow_identical_rebinding and existing_allows_identical and semantic_key == existing_key:
                    continue
                raise ValueError(
                    "object executable symbol `{}` has conflicting bindings in the YAML bundle "
                    "(locations: `{}`, `{}`)".format(symbol, existing_location, location)
                )
            bindings[symbol] = (semantic_key, location, allow_identical_rebinding)


def _validate_declared_ports(
    ports: list[InputPort] | list[OutputPort],
    *,
    kind: str,
    owner: str,
    location: str,
) -> None:
    seen: set[str] = set()
    for index, port in enumerate(ports):
        if not isinstance(port, InputPort if kind == "input" else OutputPort):
            raise ValueError(f"{owner} {kind} port #{index} has the wrong IR kind (location: `{location}`)")
        if not isinstance(port.name, str) or not port.name:
            raise ValueError(f"{owner} {kind} port name must be non-empty (location: `{location}`)")
        if kind == "input":
            _validate_identifier(
                port.name,
                description=f"{owner} input port name",
                location=location,
            )
        if port.name in seen:
            raise ValueError(f"{owner} declares {kind} port `{port.name}` more than once (location: `{location}`)")
        seen.add(port.name)


def _validate_function_syntax(function: DFunction, *, location: str) -> None:
    """Prove serialized function syntax is importable without executing it."""

    _validate_identifier(function.name, description="function name", location=location)
    if not isinstance(function.body, str) or not function.body.strip():
        raise ValueError(f"function `{function.name}` body must be non-empty (location: `{location}`)")
    outputs = function.outputs or []
    seen_default = False
    defaults: list[ast.expr] = []
    arguments: list[ast.arg] = []
    try:
        for port in function.inputs:
            annotation = ast.parse(port.typ_, mode="eval").body if port.typ_ is not None else None
            arguments.append(ast.arg(arg=port.name, annotation=annotation))
            if port.default is None:
                if seen_default:
                    raise ValueError(f"function `{function.name}` has a required input after a defaulted input")
            else:
                seen_default = True
                defaults.append(ast.parse(port.default, mode="eval").body)
        for output in outputs:
            if output.typ_ is not None:
                ast.parse(output.typ_, mode="eval")
        body = ast.parse(function.body, mode="exec").body
        function_node = ast.FunctionDef(
            name=function.name,
            args=ast.arguments(
                posonlyargs=[],
                args=arguments,
                vararg=None,
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=None,
                defaults=defaults,
            ),
            body=body,
            decorator_list=[],
            returns=(ast.parse(outputs[0].typ_, mode="eval").body if outputs and outputs[0].typ_ is not None else None),
        )
        module = ast.fix_missing_locations(ast.Module(body=[function_node], type_ignores=[]))
        compile(module, location, "exec")
    except (SyntaxError, TypeError, ValueError) as exc:
        raise ValueError(
            f"function `{function.name}` contains invalid serialized Python syntax (location: `{location}`): {exc}"
        ) from exc


def _validate_object_runtime(ir: ObjectIR) -> None:
    runtime_name = ir.runtime_config.get("node_runtime")
    if runtime_name is None:
        return
    if not isinstance(runtime_name, str) or runtime_name not in NODE_RUNTIME_BACKENDS:
        available = ", ".join(sorted(NODE_RUNTIME_BACKENDS))
        raise ValueError(
            "object runtime_config references unknown node runtime `{}`; use one of: {}".format(
                runtime_name,
                available,
            )
        )


def _canonical_reference(value: Any, *, description: str, location: str) -> str:
    try:
        return canonical_uuid_key(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{description} `{value}` is not a valid UUID (location: `{location}`)") from exc


def _validate_pipeline_aliases(ir: ObjectIR, pipeline: DPipeline) -> dict[str, list[str]]:
    if not isinstance(pipeline.aliases, list):
        raise ValueError(f"pipeline aliases must be a list (location: `{ir.source}:pipeline.aliases`)")
    known_node_ids = {
        canonical_uuid_key(node.uuid) for node in pipeline.nodes if isinstance(node, DNodeFunction | DNodeRemote)
    }
    aliases_by_node: dict[str, list[str]] = {}
    seen_aliases: dict[str, str] = {}
    for index, value in enumerate(pipeline.aliases):
        location = f"{ir.source}:pipeline.aliases[{index}]"
        if not isinstance(value, list | tuple) or len(value) != 2:
            raise ValueError(f"pipeline alias #{index} must be an [alias, node_uuid] pair (location: `{location}`)")
        alias, raw_node_id = value
        if not isinstance(alias, str) or not alias:
            raise ValueError(f"pipeline alias name must be a non-empty string (location: `{location}`)")
        if alias in seen_aliases:
            raise ValueError(
                "pipeline alias `{}` is defined more than once (locations: `{}`, `{}`)".format(
                    alias,
                    seen_aliases[alias],
                    location,
                )
            )
        node_id = _canonical_reference(raw_node_id, description="pipeline alias node uuid", location=location)
        if node_id not in known_node_ids:
            raise ValueError(
                f"pipeline alias `{alias}` references unknown node `{raw_node_id}` (location: `{location}`)"
            )
        seen_aliases[alias] = location
        aliases_by_node.setdefault(node_id, []).append(alias)
    return aliases_by_node


def _node_alias(aliases: Mapping[str, list[str]], node_id: str) -> str:
    values = aliases.get(node_id) or []
    return "|".join(sorted(values)) if values else "<none>"


def _input_ports_by_name(ports: list[InputPort], *, owner: str, location: str) -> dict[str, InputPort]:
    _validate_declared_ports(ports, kind="input", owner=owner, location=location)
    return {port.name: port for port in ports}


def _output_ports_by_name(ports: list[OutputPort], *, owner: str, location: str) -> dict[str, OutputPort]:
    _validate_declared_ports(ports, kind="output", owner=owner, location=location)
    return {port.name: port for port in ports}


def _remote_signature_ports(
    signature: Mapping[str, Any],
    *,
    node_name: str,
    location: str,
) -> tuple[list[InputPort], list[OutputPort]]:
    if "inputs" not in signature:
        raise ValueError(f"remote node `{node_name}` signature is missing `inputs` (location: `{location}`)")
    if "outputs" not in signature:
        raise ValueError(f"remote node `{node_name}` signature is missing `outputs` (location: `{location}`)")
    raw_inputs = signature["inputs"]
    raw_outputs = signature["outputs"]
    if not isinstance(raw_inputs, list) or not all(isinstance(item, dict) for item in raw_inputs):
        raise ValueError(
            f"remote node `{node_name}` signature inputs must be a list of objects (location: `{location}`)"
        )
    if not isinstance(raw_outputs, list) or not all(isinstance(item, dict) for item in raw_outputs):
        raise ValueError(
            f"remote node `{node_name}` signature outputs must be a list of objects (location: `{location}`)"
        )
    selectors = [item.get("selector") for item in raw_outputs if item.get("selector") is not None]
    if any(not isinstance(selector, str) or not selector for selector in selectors):
        raise ValueError(
            f"remote node `{node_name}` signature output selectors must be non-empty strings (location: `{location}`)"
        )
    if len(selectors) > 1:
        raise ValueError(
            "remote node `{}` signature exposes multiple selectable outputs ({}); choose a remote "
            "object/function with one output selector before registering (location: `{}`)".format(
                node_name,
                ", ".join("`{}`".format(selector) for selector in selectors),
                location,
            )
        )
    inputs: list[InputPort] = []
    for index, item in enumerate(raw_inputs):
        name = item.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(
                f"remote node `{node_name}` signature input #{index} must declare a non-empty name "
                f"(location: `{location}`)"
            )
        inputs.append(InputPort(name=name, typ_=item.get("type"), default=item.get("default")))

    outputs: list[OutputPort] = []
    seen_output_groups: set[str] = set()
    for output_index, item in enumerate(raw_outputs):
        output_name = item.get("name")
        if not isinstance(output_name, str) or not output_name:
            raise ValueError(
                f"remote node `{node_name}` signature output #{output_index} must declare a non-empty name "
                f"(location: `{location}`)"
            )
        if output_name in seen_output_groups:
            raise ValueError(
                f"remote node `{node_name}` signature declares output group `{output_name}` more than once "
                f"(location: `{location}`)"
            )
        seen_output_groups.add(output_name)
        raw_ports = item.get("ports")
        if raw_ports is not None:
            if (
                not isinstance(raw_ports, list)
                or not raw_ports
                or not all(isinstance(port, dict) for port in raw_ports)
            ):
                raise ValueError(
                    f"remote node `{node_name}` signature output #{output_index} ports must be a non-empty "
                    "list of objects "
                    f"(location: `{location}`)"
                )
            candidates = raw_ports
        else:
            candidates = [item]
        for port_index, port in enumerate(candidates):
            port_name = port.get("name")
            if not isinstance(port_name, str) or not port_name:
                raise ValueError(
                    f"remote node `{node_name}` signature output #{output_index} port #{port_index} "
                    f"must declare a non-empty name (location: `{location}`)"
                )
            outputs.append(OutputPort(name=port_name, typ_=port.get("type")))

    _validate_declared_ports(
        inputs,
        kind="input",
        owner=f"remote node `{node_name}` signature",
        location=location,
    )
    _validate_declared_ports(
        outputs,
        kind="output",
        owner=f"remote node `{node_name}` signature",
        location=location,
    )
    return inputs, outputs


def _validate_pipeline_nodes(
    ir: ObjectIR,
    pipeline: DPipeline,
    functions: Mapping[str, DFunction],
    callable_symbols: set[str],
    aliases: Mapping[str, list[str]],
    *,
    require_remote_signatures: bool,
) -> dict[str, _NodeInterface]:
    interfaces: dict[str, _NodeInterface] = {}
    for index, node in enumerate(pipeline.nodes):
        location = f"{ir.source}:pipeline.nodes[{index}]"
        if not isinstance(node, DNodeFunction | DNodeRemote):
            raise ValueError(
                f"pipeline node #{index} has unsupported IR kind `{type(node).__name__}` (location: `{location}`)"
            )
        node_id = canonical_uuid_key(node.uuid)
        alias = _node_alias(aliases, node_id)
        if isinstance(node, DNodeFunction):
            _validate_identifier(
                node.func,
                description=f"pipeline node `{alias}` function reference",
                location=location,
            )
            function = functions.get(node.func)
            if function is None:
                if node.func not in callable_symbols:
                    raise ValueError(
                        "pipeline node `{}` ({}) references missing function `{}` (location: `{}`); "
                        "include or import that function in the SPL YAML before registering".format(
                            alias,
                            node.uuid,
                            node.func,
                            location,
                        )
                    )
                inputs, outputs = [], []
                resolved = False
            else:
                inputs = list(function.inputs)
                outputs = list(function.outputs or [])
                resolved = True
            reference = node.func
        else:
            signature = ir.remote_signatures.get(_remote_signature_key(pipeline, node))
            if signature is None:
                if require_remote_signatures:
                    raise ValueError(
                        f"pipeline remote node `{alias}` ({node.uuid}) has no resolved signature "
                        f"(location: `{location}`)"
                    )
                inputs, outputs = [], []
            else:
                inputs, outputs = _remote_signature_ports(signature, node_name=node.name, location=location)
            reference = f"{node.url}/{node.name}:{node.version}"
            resolved = signature is not None
        input_ports = _input_ports_by_name(
            inputs,
            owner=f"pipeline node `{alias}` ({reference})",
            location=location,
        )
        output_ports = _output_ports_by_name(
            outputs,
            owner=f"pipeline node `{alias}` ({reference})",
            location=location,
        )
        interfaces[node_id] = _NodeInterface(
            node_id=str(node.uuid),
            alias=alias,
            reference=reference,
            inputs=input_ports,
            outputs=output_ports,
            resolved=resolved,
        )
    return interfaces


def _validate_pipeline_tags(
    ir: ObjectIR,
    pipeline: DPipeline,
    interfaces: Mapping[str, _NodeInterface],
) -> None:
    if not isinstance(pipeline.tags, dict):
        raise ValueError(f"pipeline tags must be an object (location: `{ir.source}:pipeline.tags`)")
    for raw_node_id, node_tags in pipeline.tags.items():
        location = f"{ir.source}:pipeline.tags[{raw_node_id!r}]"
        node_id = _canonical_reference(raw_node_id, description="pipeline tag node uuid", location=location)
        interface = interfaces.get(node_id)
        if interface is None:
            raise ValueError(f"pipeline tags reference unknown node `{raw_node_id}` (location: `{location}`)")
        if not isinstance(node_tags, Mapping):
            raise ValueError(f"pipeline tags for node `{interface.alias}` must be an object (location: `{location}`)")
        for name, value in node_tags.items():
            if not isinstance(name, str) or not name or not isinstance(value, str) or not value:
                raise ValueError(
                    f"pipeline tag names and values for node `{interface.alias}` must be non-empty strings "
                    f"(location: `{location}`)"
                )
        runtime_name = node_tags.get(RUNTIME_TAG_NAME)
        if runtime_name is not None and runtime_name not in NODE_RUNTIME_BACKENDS:
            available = ", ".join(sorted(NODE_RUNTIME_BACKENDS))
            raise ValueError(
                "pipeline node `{}` ({}) references unknown runtime `{}`; use one of: {} (location: `{}`)".format(
                    interface.alias,
                    interface.node_id,
                    runtime_name,
                    available,
                    location,
                )
            )


def _declared_symbols(ir: ObjectIR) -> set[str]:
    symbols: set[str] = set()
    for item, _ in _iter_object_items(ir):
        if isinstance(item, (DFunction, DPipeline, DSPLSelfImport, DSPLImport)):
            symbols.add(item.name)
        elif isinstance(item, DLocalAlias):
            symbols.add(item.alias)
        elif isinstance(item, DImport):
            symbols.add(item.alias or item.module.split(".")[0])
        elif isinstance(item, DImportFrom):
            symbols.add(item.alias or item.target)
    return symbols


def _declared_callable_symbols(ir: ObjectIR) -> set[str]:
    function_symbols = {item.name for item, _ in _iter_object_items(ir) if isinstance(item, DFunction)}
    symbols = set(function_symbols)
    symbols.update(
        item.name
        for item, _ in _iter_object_items(ir)
        if isinstance(item, DSPLSelfImport) and item.name in function_symbols
    )
    symbols.update(item.alias or item.target for item, _ in _iter_object_items(ir) if isinstance(item, DImportFrom))
    aliases = [item for item, _ in _iter_object_items(ir) if isinstance(item, DLocalAlias)]
    changed = True
    while changed:
        changed = False
        for alias in aliases:
            if alias.target in symbols and alias.alias not in symbols:
                symbols.add(alias.alias)
                changed = True
    return symbols


def _validate_pipeline_adapters(ir: ObjectIR, pipeline: DPipeline) -> None:
    if not isinstance(pipeline.adapters, list):
        raise ValueError(f"pipeline adapters must be a list (location: `{ir.source}:pipeline.adapters`)")
    symbols = _declared_callable_symbols(ir)
    seen: dict[tuple[str, str], str] = {}
    full_keys: dict[str, str] = {}
    split_keys: dict[str, str] = {}
    for index, adapter in enumerate(pipeline.adapters):
        location = f"{ir.source}:pipeline.adapters[{index}]"
        if not isinstance(adapter, DAdapter | DSaveAdapter | DLoadAdapter):
            raise ValueError(
                f"pipeline adapter #{index} has unsupported IR kind `{type(adapter).__name__}` (location: `{location}`)"
            )
        kind = type(adapter).__name__
        duplicate_key = (kind, adapter.key)
        if duplicate_key in seen:
            raise ValueError(
                "pipeline adapter `{}` of kind `{}` is defined more than once (locations: `{}`, `{}`)".format(
                    adapter.key,
                    kind,
                    seen[duplicate_key],
                    location,
                )
            )
        references: tuple[tuple[str, str], ...]
        if isinstance(adapter, DAdapter):
            if adapter.key in full_keys:
                raise ValueError(
                    f"pipeline adapter `{adapter.key}` conflicts with another full adapter "
                    f"(locations: `{full_keys[adapter.key]}`, `{location}`)"
                )
            if adapter.key in split_keys:
                raise ValueError(
                    f"pipeline full adapter `{adapter.key}` conflicts with split adapter at "
                    f"`{split_keys[adapter.key]}` (location: `{location}`)"
                )
            full_keys[adapter.key] = location
            references = (("save", adapter.save), ("load", adapter.load))
        elif isinstance(adapter, DSaveAdapter):
            if adapter.key in full_keys:
                raise ValueError(
                    f"pipeline save adapter `{adapter.key}` conflicts with full adapter at `{full_keys[adapter.key]}`"
                )
            split_keys.setdefault(adapter.key, location)
            references = (("save", adapter.save),)
        else:
            if adapter.key in full_keys:
                raise ValueError(
                    f"pipeline load adapter `{adapter.key}` conflicts with full adapter at `{full_keys[adapter.key]}`"
                )
            split_keys.setdefault(adapter.key, location)
            references = (("load", adapter.load),)
        seen[duplicate_key] = location
        for role, reference in references:
            if reference not in symbols:
                raise ValueError(
                    f"pipeline adapter `{adapter.key}` references missing {role} function `{reference}` "
                    f"(location: `{location}`); include or import that function before registering"
                )


def _validate_pipeline_links(
    ir: ObjectIR,
    pipeline: DPipeline,
    interfaces: Mapping[str, _NodeInterface],
) -> None:
    if not isinstance(pipeline.links, list):
        raise ValueError(f"pipeline links must be a list (location: `{ir.source}:pipeline.links`)")
    linked_inputs: dict[tuple[str, str], str] = {}
    for index, link in enumerate(pipeline.links):
        location = f"{ir.source}:pipeline.links[{index}]"
        if not isinstance(link, list | tuple) or len(link) != 2:
            raise ValueError(f"pipeline link #{index} must contain target and source values (location: `{location}`)")
        target, source = link
        if not isinstance(target, DNodeInputRef):
            raise ValueError(
                f"pipeline link target must be DNodeInputRef, got `{type(target).__name__}` (location: `{location}`)"
            )
        target_id = _canonical_reference(target.uuid, description="pipeline link target node uuid", location=location)
        target_node = interfaces.get(target_id)
        if target_node is None:
            raise ValueError(f"pipeline link target references unknown node `{target.uuid}` (location: `{location}`)")
        if not isinstance(target.port, str) or not target.port:
            raise ValueError(f"pipeline link target port must be a non-empty string (location: `{location}`)")
        if target_node.resolved and target.port not in target_node.inputs:
            raise ValueError(
                f"pipeline node `{target_node.alias}` ({target_node.reference}) has no input port `{target.port}` "
                f"(location: `{location}`)"
            )
        input_key = (target_id, target.port)
        if input_key in linked_inputs:
            raise ValueError(
                f"pipeline input `{target_node.alias}.{target.port}` is linked more than once "
                f"(locations: `{linked_inputs[input_key]}`, `{location}`)"
            )
        linked_inputs[input_key] = location

        if isinstance(source, DNodeOutputRef | DFormattedOutputRef):
            source_id = _canonical_reference(
                source.uuid, description="pipeline link source node uuid", location=location
            )
            source_node = interfaces.get(source_id)
            if source_node is None:
                raise ValueError(
                    f"pipeline link source references unknown node `{source.uuid}` (location: `{location}`)"
                )
            if not isinstance(source.port, str) or not source.port:
                raise ValueError(f"pipeline link source port must be a non-empty string (location: `{location}`)")
            if source_node.resolved and source.port not in source_node.outputs:
                raise ValueError(
                    f"pipeline node `{source_node.alias}` ({source_node.reference}) has no output port `{source.port}` "
                    f"(location: `{location}`)"
                )
            if isinstance(source, DFormattedOutputRef):
                if not isinstance(source.format, str) or not source.format:
                    raise ValueError(
                        f"pipeline formatted edge must name a non-empty adapter format (location: `{location}`)"
                    )
                source_type = (
                    source_node.outputs[source.port].typ_
                    if source_node.resolved and source.port in source_node.outputs
                    else None
                )
                target_type = (
                    target_node.inputs[target.port].typ_
                    if target_node.resolved and target.port in target_node.inputs
                    else None
                )
                resolution = resolve_yaml_edge_adapters(
                    pipeline.adapters,
                    source_type=source_type if isinstance(source_type, str) and source_type else None,
                    target_type=target_type if isinstance(target_type, str) and target_type else None,
                    adapter_format=source.format,
                )
                ambiguous = []
                if resolution.save_ambiguous:
                    ambiguous.append(
                        "save adapters for `{}` (candidates: {})".format(
                            source_type or "<runtime type>",
                            ", ".join(f"`{key}`" for key in resolution.save_candidates),
                        )
                    )
                if resolution.load_ambiguous:
                    ambiguous.append(
                        "load adapters for `{}` (candidates: {})".format(
                            target_type or "<runtime type>",
                            ", ".join(f"`{key}`" for key in resolution.load_candidates),
                        )
                    )
                if ambiguous:
                    raise ValueError(
                        "pipeline edge `{}.{} -> {}.{}` has ambiguous adapter resolution for format `{}`: {}; "
                        "declare a concrete port type or leave exactly one matching half for the unknown role "
                        "(location: `{}`)".format(
                            source_node.alias,
                            source.port,
                            target_node.alias,
                            target.port,
                            source.format,
                            "; ".join(ambiguous),
                            location,
                        )
                    )
                missing = []
                if resolution.save_adapter is None and not resolution.save_deferred:
                    missing.append(f"save adapter for `{source_type or '<runtime type>'}`")
                if resolution.load_adapter is None and not resolution.load_deferred:
                    missing.append(f"load adapter for `{target_type or '<runtime type>'}`")
                if missing:
                    raise ValueError(
                        "pipeline edge `{}.{} -> {}.{}` references unknown adapter format `{}` (missing: {}; "
                        "location: `{}`); register matching adapter halves or choose a known format".format(
                            source_node.alias,
                            source.port,
                            target_node.alias,
                            target.port,
                            source.format,
                            ", ".join(missing),
                            location,
                        )
                    )
        elif isinstance(source, DNodeInputRef):
            raise ValueError(
                f"pipeline link source must be a node output or literal, got DNodeInputRef (location: `{location}`)"
            )
        elif not isinstance(source, DScalar | DArtifactRef):
            raise ValueError(
                f"pipeline link source has unsupported IR kind `{type(source).__name__}` (location: `{location}`)"
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
    available = sorted(str(name) for root, _ in documents if (name := getattr(root, "name", None)) is not None)
    raise KeyError(f"entrypoint is not found in SPL YAML: {entrypoint}; available: {', '.join(available) or '<none>'}")


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
    return [{"package": package, "version": version} for package, version in sorted(unique)]


def _collect_imports(documents: list[tuple[Any, list[Any]]]) -> list[dict[str, Any]]:
    imports: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for _, dependencies in documents:
        for item in dependencies:
            key: tuple[Any, ...]
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
    callable_symbols: set[str],
    remote_signatures: Mapping[RemoteSignatureKey, Mapping[str, Any]],
    source: Path | None = None,
) -> dict[str, Any]:
    validate_pipeline_ir(pipeline, source=source or Path("<daemon-metadata>"))
    node_infos = {
        canonical_uuid_key(node.uuid): _pipeline_node_to_dict(
            node,
            functions,
            callable_symbols=callable_symbols,
            remote_signature=(
                remote_signatures.get(_remote_signature_key(pipeline, node)) if isinstance(node, DNodeRemote) else None
            ),
        )
        for node in sorted(pipeline.nodes, key=lambda node: node.uuid)
    }
    nodes = [node_infos[canonical_uuid_key(node.uuid)] for node in sorted(pipeline.nodes, key=lambda node: node.uuid)]
    node_by_uuid = {canonical_uuid_key(node.uuid): node_infos[canonical_uuid_key(node.uuid)] for node in pipeline.nodes}
    declared_node_ids = {
        canonical_node_id: str(node_info["id"]) for canonical_node_id, node_info in node_by_uuid.items()
    }

    bound_inputs = {
        (canonical_uuid_key(link_from.uuid), link_from.port)
        for link_from, _ in pipeline.links
        if isinstance(link_from, DNodeInputRef)
    }

    free_inputs: list[dict[str, Any]] = []
    for node in sorted(pipeline.nodes, key=lambda item: item.uuid):
        canonical_node_id = canonical_uuid_key(node.uuid)
        node_info = node_infos[canonical_node_id]
        for port in node_info["inputs"]:
            port_name = port["name"]
            if (canonical_node_id, port_name) not in bound_inputs:
                payload = {
                    **port,
                    "node_id": node.uuid,
                    "external_name": port_name,
                }
                if node_info["kind"] == "function":
                    payload["function"] = node_info.get("function")
                else:
                    payload["remote"] = node_info.get("remote")
                free_inputs.append(payload)

    aliases = [
        {
            "name": name,
            "node_id": declared_node_ids[canonical_uuid_key(node_uuid)],
        }
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
        "links": [_pipeline_link_to_dict(link, declared_node_ids) for link in pipeline.links],
    }


def _pipeline_outputs(
    pipeline: DPipeline,
    node_by_uuid: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if pipeline.aliases:
        outputs: list[dict[str, Any]] = []
        for alias, node_uuid in sorted(pipeline.aliases, key=lambda item: item[0]):
            node_info = node_by_uuid.get(canonical_uuid_key(node_uuid))
            ports = (node_info or {}).get("outputs") or []
            payload = {
                "name": alias,
                "node_id": node_info["id"] if node_info is not None else node_uuid,
                "ports": ports,
            }
            if node_info and node_info["kind"] == "function":
                payload["function"] = node_info.get("function")
            elif node_info:
                payload["remote"] = node_info.get("remote")
            outputs.append(payload)
        return outputs

    outputs = []
    for _, node_info in sorted(node_by_uuid.items(), key=lambda item: item[0]):
        node_uuid = node_info["id"]
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
    callable_symbols: set[str],
    remote_signature: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if isinstance(node, DNodeFunction):
        function = functions.get(node.func)
        if function is None and node.func not in callable_symbols:
            raise ValueError(f"function `{node.func}` is missing after object IR validation")
        return {
            "id": node.uuid,
            "kind": "function",
            "function": node.func,
            "name": node.func,
            "inputs": [_input_port_to_dict(port) for port in (function.inputs if function is not None else [])],
            "outputs": [
                _output_port_to_dict(port) for port in ((function.outputs or []) if function is not None else [])
            ],
        }
    if isinstance(node, DNodeRemote):
        if remote_signature is None:
            raise ValueError(f"remote signature is missing after validation for node `{node.name}` ({node.uuid})")
        return _remote_node_to_dict(node, remote_signature)
    raise TypeError(f"unsupported pipeline node: {type(node).__name__}")


def _remote_ref(node: DNodeRemote) -> dict[str, Any]:
    ref: dict[str, Any] = {
        "url": node.url,
        "name": node.name,
        "version": node.version,
    }
    for attr in ("owner_id", "library", "target_machine"):
        value = getattr(node, attr, None)
        if value is not None:
            ref[attr] = value
    return ref


def _remote_node_to_dict(
    node: DNodeRemote,
    signature: Mapping[str, Any],
) -> dict[str, Any]:
    inputs, outputs = _remote_signature_ports(
        signature,
        node_name=node.name,
        location=f"remote node `{node.name}` ({node.uuid})",
    )
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
            "target_machine": getattr(node, "target_machine", None)
            or signature.get("target_machine")
            or (signature.get("execution") or {}).get("default_machine_id"),
            "kind": signature.get("kind"),
            "signature": signature,
        },
        "inputs": [_input_port_to_dict(port) for port in inputs],
        "outputs": [_output_port_to_dict(port) for port in outputs],
    }


def _pipeline_link_to_dict(
    link: Any,
    declared_node_ids: Mapping[str, str],
) -> dict[str, Any]:
    link_from, link_to = link
    return {
        "from": _node_ref_to_dict(link_from, declared_node_ids),
        "to": _link_value_to_dict(link_to, declared_node_ids),
    }


def _node_ref_to_dict(
    value: DNodeInputRef | DNodeOutputRef | DFormattedOutputRef,
    declared_node_ids: Mapping[str, str],
) -> dict[str, str]:
    return {
        "node_id": declared_node_ids[canonical_uuid_key(value.uuid)],
        "port": value.port,
    }


def _link_value_to_dict(value: Any, declared_node_ids: Mapping[str, str]) -> dict[str, Any]:
    if isinstance(value, DNodeOutputRef):
        return {"kind": "node_output", **_node_ref_to_dict(value, declared_node_ids)}
    if isinstance(value, DFormattedOutputRef):
        return {
            "kind": "node_output",
            **_node_ref_to_dict(value, declared_node_ids),
            "format": value.format,
        }
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
