"""Static, shared producer/consumer admission for user-managed public execution.

Only authenticated captured dependencies are inventoried. This is neither a
resolver nor a transitive lock builder. YAML is read as data, never executed.
"""

from __future__ import annotations

import ast
import json
import posixpath
import sys
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any

from spl.core._yaml import yaml
from spl.public_dependencies import normalize_record

CURRENT_PROCESS_PROFILE = "current-process-v1"
CURRENT_PROCESS_MANIFEST_SCHEMA = "spl.public_object_manifest.v2"
CURRENT_PROCESS_CONTRACT = {
    "profile": CURRENT_PROCESS_PROFILE,
    "dependencies": "user-managed-provenance-v1",
    "runtime": "native",
    "python": "3.13",
    "implementation": "cpython",
    "framework": "spl.public_current_process.v1",
    "minimum_framework_version": "0.4.10",
}
COMPILED_EXECUTION = {
    "kind": "spl-compiled-python-v1",
    "member": "object.py",
    "compiler": "spl.core.ir.utils.spl_compile_to_source",
}
_TAGS = {
    "DSPLSelfImport",
    "DSPLImport",
    "DDistribution",
    "DFunction",
    "DImport",
    "DImportFrom",
    "DFormattedOutputRef",
    "DNodeInputRef",
    "DNodeOutputRef",
    "DNodeFunction",
    "DNodeRemote",
    "DPipeline",
    "DScalar",
    "DArtifactRef",
    "DAdapter",
    "DSaveAdapter",
    "DLoadAdapter",
}


class ContractError(ValueError):
    pass


def _has_entrypoint_suspension(statements: list[ast.stmt]) -> bool:
    """Inspect the current scope, leaving nested function bodies independent."""
    pending: list[ast.AST] = list(statements)
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.Yield, ast.YieldFrom, ast.Await, ast.AsyncFor, ast.AsyncWith)):
            return True
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            # Defaults and decorators execute in the enclosing scope.
            pending.extend(node.args.defaults)
            pending.extend(item for item in node.args.kw_defaults if item is not None)
            pending.extend(getattr(node, "decorator_list", ()))
        elif isinstance(node, ast.ClassDef):
            pending.extend(node.bases)
            pending.extend(node.decorator_list)
            pending.extend(keyword.value for keyword in node.keywords)
        else:
            pending.extend(ast.iter_child_nodes(node))
    return False


class _DataLoader(yaml.SafeLoader):  # type: ignore[misc]
    pass


def _mapping(loader: Any, node: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if not isinstance(key, str) or key in result or key == "__spl_tag__":
            raise ContractError("duplicate or invalid metadata key")
        result[key] = loader.construct_object(value_node, deep=True)
    return result


def _tagged(loader: Any, tag: str, node: Any) -> dict[str, Any]:
    if tag not in _TAGS:
        raise ContractError(f"unsupported SPL metadata tag: {tag}")
    return {**_mapping(loader, node), "__spl_tag__": tag}


_DataLoader.add_constructor("tag:yaml.org,2002:map", _mapping)
_DataLoader.add_multi_constructor("!", _tagged)


def validate_native_runtime_config(
    value: Any, path: str = "root.runtime_config", *, depth: int = 0, _budget: list[int] | None = None
) -> None:
    """Validate fields in an actual runtime-config document.

    This must not be applied to arbitrary user metadata: ordinary dictionaries
    are allowed to contain keys such as ``mode`` and ``runtime``.
    """
    budget = _budget if _budget is not None else [100_000]
    budget[0] -= 1
    if depth > 100 or budget[0] < 0:
        raise ContractError(f"{path}: metadata is cyclic or exceeds the inspection bound")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key in {"node_runtime", "runtime"} and isinstance(item, str) and item != "native":
                raise ContractError(f"{path}.{key}: explicit {item!r} execution boundary is unsupported")
            if key in {"node_timeout_seconds", "timeout_seconds"} and item is not None:
                raise ContractError(f"{path}.{key}: hard timeouts are unsupported in the caller's process")
            if key == "mode" and item not in {"venv", "native", None}:
                raise ContractError(f"{path}.mode: unsupported runtime {item!r}")
            validate_native_runtime_config(item, f"{path}.{key}", depth=depth + 1, _budget=budget)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            validate_native_runtime_config(item, f"{path}[{i}]", depth=depth + 1, _budget=budget)


def validate_native_metadata(
    value: Any, path: str = "root", *, depth: int = 0, _budget: list[int] | None = None
) -> None:
    """Validate tagged SPL IR without assigning semantics to literal user data."""
    budget = _budget if _budget is not None else [100_000]
    budget[0] -= 1
    if depth > 100 or budget[0] < 0:
        raise ContractError(f"{path}: metadata is cyclic or exceeds the inspection bound")
    if isinstance(value, Mapping):
        tag = value.get("__spl_tag__")
        if tag == "DScalar":
            return
        if tag == "DNodeRemote":
            raise ContractError(f"{path}: NodeRemote is unsupported in current-process-v1")
        if tag == "DPipeline":
            tags = value.get("tags", {})
            if not isinstance(tags, Mapping):
                raise ContractError(f"{path}.tags: pipeline tags are malformed")
            for node, node_tags in tags.items():
                if not isinstance(node_tags, Mapping):
                    raise ContractError(f"{path}.tags.{node}: node tags are malformed")
                runtime = node_tags.get("runtime")
                if runtime not in {None, "native"}:
                    raise ContractError(
                        f"{path}.tags.{node}.runtime: explicit {runtime!r} execution boundary is unsupported"
                    )
        for key, item in value.items():
            validate_native_metadata(item, f"{path}.{key}", depth=depth + 1, _budget=budget)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            validate_native_metadata(item, f"{path}[{i}]", depth=depth + 1, _budget=budget)


def dependency_graph(members: Mapping[str, bytes]) -> list[dict[str, Any]]:
    """Inventory every captured IR/descriptor/adapter dependency and its source.

    Copied component members are covered as well as the root. References must
    remain in the authenticated member inventory, before the producer compiler
    is allowed to read them. The consumer recomputes this graph before loading.
    """
    grouped: dict[tuple[str, str], dict[str, set[str]]] = {}
    documents_by_member: dict[str, list[Any]] = {}
    imports_by_member: dict[str, set[str]] = {}
    external_imports: list[tuple[str, str]] = []
    module_claims: dict[str, set[tuple[str, str]]] = {}
    budget = [100_000]

    def add(item: Any, source: str) -> None:
        if not isinstance(item, Mapping):
            raise ContractError(f"{source}: dependency record is malformed")
        try:
            identity = normalize_record(item.get("package"), item.get("version"))
        except ValueError as exc:
            raise ContractError(f"{source}: {exc}") from exc
        group = grouped.setdefault(identity, {"sources": set(), "modules": set()})
        group["sources"].add(source)
        raw_modules = item.get("modules", [])
        if not isinstance(raw_modules, (list, tuple)) or any(
            not isinstance(module, str) or not module.partition(".")[0].isidentifier() for module in raw_modules
        ):
            raise ContractError(f"{source}: distribution import-root metadata is malformed")
        for module in raw_modules:
            root = module.partition(".")[0]
            group["modules"].add(root)
            module_claims.setdefault(root, set()).add(identity)

    def record_import(module: Any, source: str) -> None:
        if not isinstance(module, str) or not module:
            raise ContractError(f"{source}: import metadata is malformed")
        root = module.partition(".")[0]
        standard_modules = set(sys.stdlib_module_names) | set(sys.builtin_module_names)
        if root not in standard_modules | {"spl", "_spl_components"}:
            external_imports.append((root, source))

    def record_function_imports(item: Mapping[str, Any], source: str) -> None:
        body = item.get("body")
        if not isinstance(body, str):
            raise ContractError(f"{source}: function body is malformed")
        try:
            tree = ast.parse(body)
        except SyntaxError as exc:
            raise ContractError(f"{source}: function body is invalid") from exc
        if _has_entrypoint_suspension(tree.body):
            raise ContractError(f"{source}: async and generator entrypoints are unsupported")
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    record_import(alias.name, source)
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                record_import(node.module, source)

    def walk(value: Any, path: str, member: str, depth: int = 0) -> None:
        budget[0] -= 1
        if depth > 100 or budget[0] < 0:
            raise ContractError(f"{path}: metadata is cyclic or exceeds the inspection bound")
        if isinstance(value, Mapping):
            tag = value.get("__spl_tag__")
            if tag == "DScalar":
                return  # Literal fields named dependencies are not package records.
            if tag == "DDistribution":
                add(value, path)
            if tag in {"DImport", "DImportFrom"}:
                record_import(value.get("module"), path)
            if tag == "DFunction":
                record_function_imports(value, path)
            if tag == "DSPLImport":
                raw = value.get("path")
                if not isinstance(raw, str) or "\\" in raw or PurePosixPath(raw).is_absolute():
                    raise ContractError(f"{path}: unsafe SPL component path")
                target = posixpath.normpath(posixpath.join(posixpath.dirname(member), raw))
                if target not in members or target.startswith("../"):
                    raise ContractError(f"{path}: SPL component is not included in authenticated members")
                imports_by_member.setdefault(member, set()).add(target)
            for key, item in value.items():
                walk(item, f"{path}.{key}", member, depth + 1)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}[{index}]", member, depth + 1)

    def json_mapping(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result or key == "__spl_tag__":
                raise ContractError("duplicate or reserved JSON metadata key")
            result[key] = value
        return result

    for name, body in sorted(members.items()):
        try:
            if name.endswith(".yaml"):
                documents = list(yaml.load_all(body.decode("utf-8"), Loader=_DataLoader))
                documents_by_member[name] = documents
                for index, doc in enumerate(documents):
                    if not isinstance(doc, list) or not doc:
                        raise ContractError(f"{name}: missing object dependency document")
                    root = doc[0]
                    label = root.get("name", str(index)) if isinstance(root, Mapping) else str(index)
                    source = f"{name}:{label}"
                    validate_native_metadata(doc, source)
                    walk(doc, source, name)
            elif name.endswith(("object.json", "adapter.json")):
                doc = json.loads(body, object_pairs_hook=json_mapping)
                if not isinstance(doc, Mapping):
                    raise ContractError(f"{name}: descriptor is malformed")
                required = "distributions" if name.endswith("object.json") else "dependencies"
                if required not in doc:
                    raise ContractError(f"{name}: dependency inventory is missing")
                records = doc[required]
                if not isinstance(records, list):
                    raise ContractError(f"{name}.{required}: dependency inventory must be a list")
                for index, record in enumerate(records):
                    add(record, f"{name}.{required}[{index}]")
                validate_native_runtime_config(doc.get("runtime_config", {}), f"{name}.runtime_config")
            elif name.endswith("/manifest.json"):
                envelope = json.loads(body, object_pairs_hook=json_mapping)
                manifest = envelope.get("manifest") if isinstance(envelope, Mapping) else None
                if not isinstance(manifest, Mapping) or not isinstance(manifest.get("dependencies"), list):
                    raise ContractError(f"{name}: component dependency metadata is missing")
                identity = manifest.get("identity", {})
                label = identity.get("name", name) if isinstance(identity, Mapping) else name
                for index, record in enumerate(manifest["dependencies"]):
                    add(record, f"{name}:{label}.dependencies[{index}]")
        except (UnicodeError, yaml.YAMLError, TypeError, RecursionError) as exc:
            raise ContractError(f"{name}: malformed dependency metadata: {exc}") from exc
    if "object.yaml" not in members or "object.json" not in members:
        raise ContractError("root object source/descriptor dependency metadata is missing")
    # The existing producer compiler inlines DSPLImport definitions. Do not
    # silently overwrite same-named definitions from two included components.
    visited: set[str] = set()
    symbols: dict[str, str] = {}
    queue = ["object.yaml"]
    while queue:
        member = queue.pop()
        if member in visited:
            continue
        visited.add(member)
        for doc in documents_by_member.get(member, []):
            root = doc[0]
            if isinstance(root, Mapping) and isinstance(root.get("name"), str):
                name = root["name"]
                if name in symbols:
                    raise ContractError(f"duplicate inlined definition {name!r}: {symbols[name]} and {member}")
                symbols[name] = member
        queue.extend(sorted(imports_by_member.get(member, set())))
    for root, source in external_imports:
        owners = module_claims.get(root, set())
        if not owners:
            raise ContractError(f"{source}: imported module {root!r} has no captured distribution mapping")
        packages = {package for package, _ in owners}
        if len(packages) != 1:
            package_list = ", ".join(sorted(packages))
            raise ContractError(f"{source}: imported module {root!r} has ambiguous distribution owners: {package_list}")
    return [
        {
            "package": package,
            "version": version,
            "modules": sorted(details["modules"]),
            "sources": sorted(details["sources"]),
        }
        for (package, version), details in sorted(grouped.items())
    ]


def validate_compiled_members(members: Mapping[str, bytes], entrypoint: str, kind: str) -> None:
    """Check compiled member syntax and the supported bundled import surface."""
    if not isinstance(entrypoint, str) or not entrypoint.isidentifier():
        raise ContractError("entrypoint must be a Python identifier")
    roots = [
        doc[0]
        for doc in yaml.load_all(members["object.yaml"], Loader=_DataLoader)
        if isinstance(doc, list) and doc and isinstance(doc[0], Mapping) and doc[0].get("name") == entrypoint
    ]
    if len(roots) != 1 or roots[0].get("__spl_tag__") != {"function": "DFunction", "pipeline": "DPipeline"}.get(kind):
        raise ContractError("entrypoint kind disagrees with captured object metadata")
    try:
        tree = ast.parse(members["object.py"], filename="object.py")
    except (KeyError, SyntaxError) as exc:
        raise ContractError("producer-compiled execution member is unavailable or invalid") from exc
    entry_functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == entrypoint
    ]
    if any(isinstance(node, ast.AsyncFunctionDef) for node in entry_functions) or any(
        _has_entrypoint_suspension(node.body) for node in entry_functions
    ):
        raise ContractError("async and generator entrypoints are unsupported")
    if not entry_functions and not any(
        isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == entrypoint for t in node.targets)
        for node in tree.body
    ):
        raise ContractError("compiled member does not define the declared entrypoint")
    for name, body in members.items():
        if name.endswith(".py"):
            if name != "object.py" and not name.startswith("_spl_components/"):
                raise ContractError(f"unsupported bundled Python module: {name}")
            try:
                ast.parse(body, filename=name)
            except SyntaxError as exc:
                raise ContractError(f"invalid bundled Python module: {name}") from exc
