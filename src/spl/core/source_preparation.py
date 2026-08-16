"""Deterministic, non-executing generated-source preparation.

This module is intentionally client-neutral.  It parses an explicitly supplied
generated Python bundle and a closed structured plan without importing,
compiling, evaluating, or executing either input.  The output is an immutable
JSON-compatible ``PreparedObject`` value whose source, plan, IR, source-map,
dependency, and prepared identities use separate hash domains.

The supported v1 grammar is deliberately small:

* source documents contain only a module docstring, absolute imports, and
  undecorated synchronous function definitions;
* functions use positional-or-keyword parameters, JSON-literal defaults, and
  one optional return annotation; generated helpers may use nested functions,
  lambdas, and absolute local imports, while nested classes remain excluded;
* a function Object names one parsed function directly;
* a Pipeline Object declares function Nodes and typed edges in the structured
  plan.  The source is never executed to discover a fluent builder graph.

Generated source is untrusted data.  A successful result proves only static
representability under this exact compiler/schema/target version.  It does not
write files, reserve or publish an Object, build an environment, or execute a
Run.
"""

from __future__ import annotations

import ast
import hashlib
import json
import keyword
import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Literal, cast

from spl.core import json_contract as m_json_contract

PREPARE_SOURCE_REQUEST_SCHEMA = "splime.prepare-source-request/v1"
PREPARE_SOURCE_RESULT_SCHEMA = "splime.prepare-source-result/v1"
PREPARED_OBJECT_SCHEMA = "splime.prepared-object/v1"
GENERATED_PLAN_SCHEMA = "splime.generated-plan/v1"
SEMANTIC_PORTS_SCHEMA = "splime.semantic-ports/v1"
CANONICAL_IR_SCHEMA = "splime.object-ir/v1"
SOURCE_MAP_SCHEMA = "splime.source-map/v1"

SCHEMA_VERSION = 1
PREPARATION_PROTOCOL = 1
COMPILER_NAME = "splime-source-preparer"
COMPILER_VERSION = "1.0.0"
CANONICALIZATION_VERSION = 1
PYTHON_LANGUAGE = "3.13"

SOURCE_HASH_DOMAIN = "splime.source/v1"
PLAN_HASH_DOMAIN = "splime.plan/v1"
IR_HASH_DOMAIN = "splime.ir/v1"
SOURCE_MAP_HASH_DOMAIN = "splime.source-map/v1"
DEPENDENCIES_HASH_DOMAIN = "splime.dependencies/v1"
PREPARED_HASH_DOMAIN = "splime.prepared/v1"

MAX_SOURCE_DOCUMENTS = 16
MAX_SOURCE_DOCUMENT_BYTES = 512 * 1024
MAX_TOTAL_SOURCE_BYTES = 2 * 1024 * 1024
MAX_AST_NODES = 100_000
MAX_AST_DEPTH = 64
MAX_FUNCTIONS = 256
MAX_PLAN_NODES = 512
MAX_PLAN_EDGES = 4_096
MAX_PLAN_INPUTS = 1_024
MAX_PLAN_OUTPUTS = 1_024
MAX_DEPENDENCIES = 256
MAX_IMPORTS = 1_024
MAX_ADAPTERS = 256
MAX_DIAGNOSTICS = 128
MAX_JSON_DEPTH = 32
MAX_JSON_NODES = 200_000
MAX_STRING_CHARS = 1_048_576
MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991
MAX_IDENTIFIER_CHARS = 128
MAX_LOGICAL_PATH_CHARS = 512

_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,63}$")
_PLAN_ID_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")
_PYTHON_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LOGICAL_PATH_PATTERN = re.compile(
    r"^(?!/)(?![.]{1,2}(?:/|$))(?!.*\/[.]{1,2}(?:/|$))"
    r"[A-Za-z0-9._-]+(?:\/[A-Za-z0-9._-]+)*$"
)
_IDENTITY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_PACKAGE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_MODULE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
_ADAPTER_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*(?:\.[A-Za-z_][A-Za-z0-9_.]*)?@[A-Za-z0-9._+-]{1,64}$")
_SECRET_NAME_PATTERN = re.compile(
    r"(?:^|_)(?:access_?token|api_?key|authorization|credential|password|private_?key|provider_?key|secret|token)(?:$|_)",
    re.IGNORECASE,
)
_SECRET_LITERAL_PATTERN = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~+/-]{12,}|\bsk-[A-Za-z0-9_-]{12,})"
)
_ABSOLUTE_PATH_PATTERN = re.compile(r"^(?:/[^/\s]|[A-Za-z]:[\\/]|\\\\[^\\])")

_REQUEST_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "sources",
        "proposal_ir",
        "entrypoints",
        "environment_request",
        "runtime_request",
        "target",
        "base",
    }
)
_PLAN_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "kind",
        "name",
        "nodes",
        "edges",
        "outputs",
        "dependencies",
        "adapters",
    }
)
_PLAN_KEYS_WITH_SEMANTIC_PORTS = _PLAN_KEYS | {"semantic_ports"}
_SEMANTIC_TYPE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\[\], |]*$")
_PREPARED_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "compiler",
        "target",
        "entrypoints",
        "source_documents",
        "source_hash",
        "normalized_plan",
        "plan_hash",
        "canonical_ir",
        "ir_hash",
        "serialized_ir",
        "generated_files",
        "source_map",
        "source_map_hash",
        "signature",
        "dependencies",
        "dependencies_hash",
        "environment_request",
        "runtime_request",
        "base",
        "diagnostics",
        "prepared_hash",
    }
)
_MANIFEST_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "prepared_hash",
        "source_hash",
        "plan_hash",
        "ir_hash",
        "source_map_hash",
        "dependencies_hash",
        "generated_files",
        "entrypoints",
        "compiler",
        "target",
        "environment_request",
        "runtime_request",
        "base",
    }
)

_FORBIDDEN_DYNAMIC_CALLS = frozenset(
    {
        "__import__",
        "compile",
        "delattr",
        "eval",
        "exec",
        "getattr",
        "globals",
        "locals",
        "setattr",
        "vars",
    }
)
_BUILTIN_NAMES = frozenset(
    {
        "ArithmeticError",
        "AssertionError",
        "AttributeError",
        "BaseException",
        "Exception",
        "False",
        "IndexError",
        "KeyError",
        "LookupError",
        "None",
        "NotImplemented",
        "RuntimeError",
        "StopIteration",
        "True",
        "TypeError",
        "ValueError",
        "ZeroDivisionError",
        "abs",
        "all",
        "any",
        "bool",
        "bytearray",
        "bytes",
        "callable",
        "dict",
        "enumerate",
        "filter",
        "float",
        "format",
        "frozenset",
        "hash",
        "int",
        "isinstance",
        "issubclass",
        "iter",
        "len",
        "list",
        "map",
        "max",
        "min",
        "next",
        "object",
        "open",
        "ord",
        "pow",
        "print",
        "range",
        "repr",
        "reversed",
        "round",
        "set",
        "slice",
        "sorted",
        "str",
        "sum",
        "super",
        "tuple",
        "type",
        "zip",
    }
)
_JSON_TYPE_NAMES = frozenset(
    {
        "Any",
        "None",
        "NoneType",
        "bool",
        "builtins.bool",
        "dict",
        "builtins.dict",
        "float",
        "builtins.float",
        "int",
        "builtins.int",
        "list",
        "builtins.list",
        "str",
        "builtins.str",
    }
)
_STDLIB_MODULES = frozenset(
    {
        "base64",
        "collections",
        "csv",
        "dataclasses",
        "datetime",
        "decimal",
        "enum",
        "functools",
        "hashlib",
        "io",
        "itertools",
        "json",
        "math",
        "operator",
        "pathlib",
        "pickle",
        "random",
        "re",
        "statistics",
        "string",
        "struct",
        "time",
        "typing",
        "uuid",
        "zlib",
    }
)

DiagnosticSeverity = Literal["error", "warning", "info"]
CancelCheck = Callable[[], bool]


class PrepareSourceContractError(ValueError):
    """Malformed closed request or prepared document."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class PreparationCancelled(RuntimeError):
    """The caller cancelled preparation before a result was produced."""


class _StaticFunctionSubsetVisitor(ast.NodeVisitor):
    """Validate compile-time function constraints without producing bytecode."""

    def __init__(self, root: ast.FunctionDef) -> None:
        self._root = root
        self._loop_depth = 0

    def _fail(self, code: str) -> None:
        raise PrepareSourceContractError(code)

    def _validate_arguments(self, arguments: ast.arguments) -> None:
        names = [item.arg for item in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)]
        if arguments.vararg is not None:
            names.append(arguments.vararg.arg)
        if arguments.kwarg is not None:
            names.append(arguments.kwarg.arg)
        if len(names) != len(set(names)):
            self._fail("source.duplicate_parameter")

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._validate_arguments(node.args)
        previous_loop_depth = self._loop_depth
        self._loop_depth = 0
        self.visit(node.args)
        for decorator in node.decorator_list:
            self.visit(decorator)
        if node.returns is not None:
            self.visit(node.returns)
        for statement in node.body:
            self.visit(statement)
        self._loop_depth = previous_loop_depth

    def visit_AsyncFunctionDef(self, _node: ast.AsyncFunctionDef) -> None:
        self._fail("source.async_function_unsupported")

    def visit_ClassDef(self, _node: ast.ClassDef) -> None:
        self._fail("source.nested_class_unsupported")

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self._validate_arguments(node.args)
        self.visit(node.args)
        self.visit(node.body)

    def visit_Global(self, _node: ast.Global) -> None:
        self._fail("source.dynamic_scope_unsupported")

    def visit_Nonlocal(self, _node: ast.Nonlocal) -> None:
        self._fail("source.dynamic_scope_unsupported")

    def visit_Yield(self, _node: ast.Yield) -> None:
        self._fail("source.dynamic_scope_unsupported")

    def visit_YieldFrom(self, _node: ast.YieldFrom) -> None:
        self._fail("source.dynamic_scope_unsupported")

    def visit_Await(self, _node: ast.Await) -> None:
        self._fail("source.dynamic_scope_unsupported")

    def visit_AsyncFor(self, _node: ast.AsyncFor) -> None:
        self._fail("source.dynamic_scope_unsupported")

    def visit_AsyncWith(self, _node: ast.AsyncWith) -> None:
        self._fail("source.dynamic_scope_unsupported")

    def visit_comprehension(self, node: ast.comprehension) -> None:
        if node.is_async:
            self._fail("source.async_comprehension_unsupported")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == "__future__":
            self._fail("source.future_import_unsupported")
        if node.level != 0 or node.module is None or any(alias.name == "*" for alias in node.names):
            self._fail("source.local_import_unsupported")

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Name) and node.func.id in _FORBIDDEN_DYNAMIC_CALLS:
            self._fail("source.dynamic_call_unsupported")
        self.generic_visit(node)

    def visit_Break(self, _node: ast.Break) -> None:
        if self._loop_depth == 0:
            self._fail("source.break_outside_loop")

    def visit_Continue(self, _node: ast.Continue) -> None:
        if self._loop_depth == 0:
            self._fail("source.continue_outside_loop")

    def visit_For(self, node: ast.For) -> None:
        self.visit(node.target)
        self.visit(node.iter)
        previous_loop_depth = self._loop_depth
        self._loop_depth += 1
        for statement in node.body:
            self.visit(statement)
        self._loop_depth = previous_loop_depth
        for statement in node.orelse:
            self.visit(statement)

    def visit_While(self, node: ast.While) -> None:
        self.visit(node.test)
        previous_loop_depth = self._loop_depth
        self._loop_depth += 1
        for statement in node.body:
            self.visit(statement)
        self._loop_depth = previous_loop_depth
        for statement in node.orelse:
            self.visit(statement)


def validate_static_function_ast(function: ast.FunctionDef) -> None:
    """Validate the generated v1 function subset without ``compile()``."""

    _StaticFunctionSubsetVisitor(function).visit(function)


@dataclass(frozen=True)
class _FunctionRecord:
    name: str
    logical_path: str
    span: dict[str, int]
    inputs: list[dict[str, Any]]
    outputs: list[dict[str, Any]]
    body: str
    unresolved_names: tuple[str, ...]
    import_modules: tuple[str, ...]


def prepare_source(
    request: Mapping[str, Any],
    *,
    cancelled: CancelCheck | None = None,
) -> dict[str, Any]:
    """Prepare an explicit generated-source request without side effects.

    Malformed protocol input raises :class:`PrepareSourceContractError`.
    Syntactically valid but unsupported generated source returns
    ``status="invalid"`` with stable diagnostics and no partial
    ``PreparedObject``.
    """

    cancel = cancelled or (lambda: False)
    _check_cancel(cancel)
    normalized_request = _validate_and_normalize_request(request)
    diagnostics: list[dict[str, Any]] = []
    functions, import_modules, imports, source_map_symbols = _parse_sources(
        normalized_request["sources"], diagnostics, cancel
    )
    if diagnostics:
        return _invalid_result(diagnostics)

    _check_cancel(cancel)
    normalized_plan, canonical_ir, signature, graph_map = _compile_plan(
        normalized_request,
        functions,
        import_modules,
        imports,
        diagnostics,
        cancel,
    )
    if diagnostics:
        return _invalid_result(diagnostics)
    for adapter in normalized_plan["adapters"]:
        if adapter["format"] == "pickle":
            _add_diagnostic(
                diagnostics,
                "plan.adapter_pickle_trusted_only",
                "warning",
                None,
                subject=adapter["key"],
            )
    diagnostics = _ordered_diagnostics(diagnostics)

    source_documents = [
        {
            "logical_path": source["logical_path"],
            "normalized_text": source["text"],
            "content_hash": _content_hash(source["text"].encode("utf-8")),
        }
        for source in normalized_request["sources"]
    ]
    source_hash = _source_bundle_hash(source_documents)
    plan_hash = _domain_json_hash(PLAN_HASH_DOMAIN, normalized_plan)
    ir_hash = _domain_json_hash(IR_HASH_DOMAIN, canonical_ir)
    source_map = {
        "schema": SOURCE_MAP_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "symbols": source_map_symbols,
        "nodes": graph_map["nodes"],
        "edges": graph_map["edges"],
    }
    source_map_hash = _domain_json_hash(SOURCE_MAP_HASH_DOMAIN, source_map)
    dependencies = cast(list[dict[str, Any]], normalized_plan["dependencies"])
    dependencies_hash = _domain_json_hash(DEPENDENCIES_HASH_DOMAIN, dependencies)

    # JSON is a YAML 1.2 subset.  Using the canonical JSON spelling gives the
    # compatibility document stable bytes without making a YAML emitter part
    # of the IR hash contract.  It is not a legacy registration payload.
    serialized_ir_text = _canonical_json_text(canonical_ir) + "\n"
    serialized_ir = {
        "media_type": "application/vnd.splime.yaml",
        "text": serialized_ir_text,
        "content_hash": _content_hash(serialized_ir_text.encode("utf-8")),
    }
    generated_files = [
        {
            "logical_path": item["logical_path"],
            "media_type": "text/x-python",
            "text": item["normalized_text"],
            "content_hash": item["content_hash"],
        }
        for item in source_documents
    ]
    ir_path = _generated_ir_path(
        source_documents[0]["logical_path"],
        normalized_request["entrypoints"][0],
    )
    if ir_path in {item["logical_path"] for item in generated_files}:
        raise PrepareSourceContractError("prepare.generated_path_collision")
    generated_files.append(
        {
            "logical_path": ir_path,
            "media_type": serialized_ir["media_type"],
            "text": serialized_ir_text,
            "content_hash": serialized_ir["content_hash"],
        }
    )
    generated_files.sort(key=lambda item: item["logical_path"])

    prepared: dict[str, Any] = {
        "schema": PREPARED_OBJECT_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "compiler": _compiler_identity(),
        "target": dict(normalized_request["target"]),
        "entrypoints": list(normalized_request["entrypoints"]),
        "source_documents": source_documents,
        "source_hash": source_hash,
        "normalized_plan": normalized_plan,
        "plan_hash": plan_hash,
        "canonical_ir": canonical_ir,
        "ir_hash": ir_hash,
        "serialized_ir": serialized_ir,
        "generated_files": generated_files,
        "source_map": source_map,
        "source_map_hash": source_map_hash,
        "signature": signature,
        "dependencies": dependencies,
        "dependencies_hash": dependencies_hash,
        "environment_request": normalized_request["environment_request"],
        "runtime_request": normalized_request["runtime_request"],
        "base": normalized_request["base"],
        "diagnostics": diagnostics,
        "prepared_hash": "",
    }
    prepared["prepared_hash"] = _prepared_hash(prepared_manifest(prepared, include_prepared_hash=False))
    validate_prepared_object(prepared)
    return {
        "schema": PREPARE_SOURCE_RESULT_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "status": "ready",
        "diagnostics": diagnostics,
        "prepared": prepared,
    }


def prepared_manifest(
    prepared: Mapping[str, Any],
    *,
    include_prepared_hash: bool = True,
) -> dict[str, Any]:
    """Return the source-free manifest accepted by daemon validation."""

    _require_exact_keys(prepared, _PREPARED_KEYS, "prepared")
    manifest: dict[str, Any] = {
        "schema": "splime.prepared-manifest/v1",
        "schema_version": SCHEMA_VERSION,
        "prepared_hash": prepared["prepared_hash"] if include_prepared_hash else "",
        "source_hash": prepared["source_hash"],
        "plan_hash": prepared["plan_hash"],
        "ir_hash": prepared["ir_hash"],
        "source_map_hash": prepared["source_map_hash"],
        "dependencies_hash": prepared["dependencies_hash"],
        "generated_files": [
            {
                "logical_path": item["logical_path"],
                "content_hash": item["content_hash"],
            }
            for item in prepared["generated_files"]
        ],
        "entrypoints": list(prepared["entrypoints"]),
        "compiler": dict(prepared["compiler"]),
        "target": dict(prepared["target"]),
        "environment_request": prepared["environment_request"],
        "runtime_request": prepared["runtime_request"],
        "base": prepared["base"],
    }
    return manifest


def validate_prepared_object(prepared: Mapping[str, Any]) -> None:
    """Revalidate a complete PreparedObject and every deterministic hash."""

    _require_exact_keys(prepared, _PREPARED_KEYS, "prepared")
    if prepared["schema"] != PREPARED_OBJECT_SCHEMA or not _is_v1(prepared["schema_version"]):
        raise PrepareSourceContractError("prepared.schema")
    _validate_compiler(prepared["compiler"])
    _validate_target(prepared["target"])
    entrypoints = _validate_entrypoints(prepared["entrypoints"])
    if entrypoints != prepared["entrypoints"]:
        raise PrepareSourceContractError("prepared.entrypoints_order")
    source_documents = _require_list(prepared["source_documents"], "prepared.source_documents")
    if not source_documents or len(source_documents) > MAX_SOURCE_DOCUMENTS:
        raise PrepareSourceContractError("prepared.source_documents")
    normalized_sources: list[dict[str, str]] = []
    for index, raw in enumerate(source_documents):
        document = _require_mapping(raw, f"prepared.source_documents[{index}]")
        _require_exact_keys(document, frozenset({"logical_path", "normalized_text", "content_hash"}), "source")
        path = _validate_logical_path(document["logical_path"])
        text = _normalize_source_text(document["normalized_text"])
        if text != document["normalized_text"] or document["content_hash"] != _content_hash(text.encode("utf-8")):
            raise PrepareSourceContractError("prepared.source_document_hash")
        normalized_sources.append(
            {"logical_path": path, "normalized_text": text, "content_hash": document["content_hash"]}
        )
    if normalized_sources != sorted(normalized_sources, key=lambda item: item["logical_path"]):
        raise PrepareSourceContractError("prepared.source_document_order")
    if prepared["source_hash"] != _source_bundle_hash(normalized_sources):
        raise PrepareSourceContractError("prepared.source_hash")

    _validate_json_value(prepared["normalized_plan"], "prepared.normalized_plan")
    if _validate_plan(prepared["normalized_plan"]) != prepared["normalized_plan"]:
        raise PrepareSourceContractError("prepared.normalized_plan")
    _validate_json_value(prepared["canonical_ir"], "prepared.canonical_ir")
    _validate_canonical_ir(prepared["canonical_ir"])
    if prepared["plan_hash"] != _domain_json_hash(PLAN_HASH_DOMAIN, prepared["normalized_plan"]):
        raise PrepareSourceContractError("prepared.plan_hash")
    if prepared["ir_hash"] != _domain_json_hash(IR_HASH_DOMAIN, prepared["canonical_ir"]):
        raise PrepareSourceContractError("prepared.ir_hash")

    serialized_ir = _require_mapping(prepared["serialized_ir"], "prepared.serialized_ir")
    _require_exact_keys(serialized_ir, frozenset({"media_type", "text", "content_hash"}), "serialized_ir")
    expected_ir_text = _canonical_json_text(prepared["canonical_ir"]) + "\n"
    if (
        serialized_ir["media_type"] != "application/vnd.splime.yaml"
        or serialized_ir["text"] != expected_ir_text
        or serialized_ir["content_hash"] != _content_hash(expected_ir_text.encode("utf-8"))
    ):
        raise PrepareSourceContractError("prepared.serialized_ir")

    generated_files = _require_list(prepared["generated_files"], "prepared.generated_files")
    if len(generated_files) != len(source_documents) + 1:
        raise PrepareSourceContractError("prepared.generated_files")
    normalized_generated_files: list[dict[str, str]] = []
    for index, raw in enumerate(generated_files):
        item = _require_mapping(raw, f"prepared.generated_files[{index}]")
        _require_exact_keys(item, frozenset({"logical_path", "media_type", "text", "content_hash"}), "generated_file")
        path = _validate_logical_path(item["logical_path"])
        text = _require_string(item["text"], "generated_file.text", maximum=MAX_TOTAL_SOURCE_BYTES)
        if item["content_hash"] != _content_hash(text.encode("utf-8")):
            raise PrepareSourceContractError("prepared.generated_file_hash")
        normalized_generated_files.append(
            {
                "logical_path": path,
                "media_type": _require_string(item["media_type"], "generated_file.media_type", maximum=128),
                "text": text,
                "content_hash": item["content_hash"],
            }
        )
    generated_paths = [item["logical_path"] for item in normalized_generated_files]
    if generated_paths != sorted(generated_paths) or len(generated_paths) != len(set(generated_paths)):
        raise PrepareSourceContractError("prepared.generated_file_order")
    expected_generated_files = sorted(
        [
            {
                "logical_path": item["logical_path"],
                "media_type": "text/x-python",
                "text": item["normalized_text"],
                "content_hash": item["content_hash"],
            }
            for item in normalized_sources
        ]
        + [
            {
                "logical_path": _generated_ir_path(normalized_sources[0]["logical_path"], entrypoints[0]),
                "media_type": serialized_ir["media_type"],
                "text": serialized_ir["text"],
                "content_hash": serialized_ir["content_hash"],
            }
        ],
        key=lambda item: item["logical_path"],
    )
    if normalized_generated_files != expected_generated_files:
        raise PrepareSourceContractError("prepared.generated_file_binding")

    _validate_json_value(prepared["source_map"], "prepared.source_map")
    _validate_source_map(prepared["source_map"])
    if prepared["source_map_hash"] != _domain_json_hash(SOURCE_MAP_HASH_DOMAIN, prepared["source_map"]):
        raise PrepareSourceContractError("prepared.source_map_hash")
    dependencies = _validate_dependencies(prepared["dependencies"])
    if dependencies != prepared["dependencies"]:
        raise PrepareSourceContractError("prepared.dependencies_order")
    if prepared["dependencies_hash"] != _domain_json_hash(DEPENDENCIES_HASH_DOMAIN, dependencies):
        raise PrepareSourceContractError("prepared.dependencies_hash")
    _validate_environment(prepared["environment_request"])
    _validate_runtime(prepared["runtime_request"])
    _validate_base(prepared["base"])
    _validate_signature(prepared["signature"])
    diagnostics = _require_list(prepared["diagnostics"], "prepared.diagnostics")
    for diagnostic in diagnostics:
        _validate_diagnostic(diagnostic)
    expected_warnings: list[dict[str, Any]] = []
    for adapter in prepared["normalized_plan"]["adapters"]:
        if adapter["format"] == "pickle":
            _add_diagnostic(
                expected_warnings,
                "plan.adapter_pickle_trusted_only",
                "warning",
                None,
                subject=adapter["key"],
            )
    if diagnostics != _ordered_diagnostics(expected_warnings):
        raise PrepareSourceContractError("prepared.diagnostics_binding")

    binding_diagnostics: list[dict[str, Any]] = []
    functions, import_modules, imports, symbols = _parse_sources(
        [
            {
                "logical_path": item["logical_path"],
                "language": "python",
                "text": item["normalized_text"],
            }
            for item in normalized_sources
        ],
        binding_diagnostics,
        lambda: False,
    )
    normalized_plan, expected_ir, expected_signature, graph_map = _compile_plan(
        {
            "proposal_ir": prepared["normalized_plan"],
            "entrypoints": entrypoints,
            "environment_request": prepared["environment_request"],
            "runtime_request": prepared["runtime_request"],
            "base": prepared["base"],
        },
        functions,
        import_modules,
        imports,
        binding_diagnostics,
        lambda: False,
    )
    expected_source_map = {
        "schema": SOURCE_MAP_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "symbols": symbols,
        "nodes": graph_map["nodes"],
        "edges": graph_map["edges"],
    }
    if binding_diagnostics or (
        normalized_plan != prepared["normalized_plan"]
        or expected_ir != prepared["canonical_ir"]
        or expected_signature != prepared["signature"]
        or expected_source_map != prepared["source_map"]
    ):
        raise PrepareSourceContractError("prepared.source_plan_ir_binding")
    _require_hash(prepared["prepared_hash"], "prepared.prepared_hash")
    expected_manifest = prepared_manifest(prepared, include_prepared_hash=False)
    if prepared["prepared_hash"] != _prepared_hash(expected_manifest):
        raise PrepareSourceContractError("prepared.prepared_hash")


def validate_prepared_manifest_and_ir(
    manifest: Mapping[str, Any],
    canonical_ir: Mapping[str, Any],
) -> None:
    """Revalidate the source-free daemon validation payload."""

    _require_exact_keys(manifest, _MANIFEST_KEYS, "prepared_manifest")
    if manifest["schema"] != "splime.prepared-manifest/v1" or not _is_v1(manifest["schema_version"]):
        raise PrepareSourceContractError("prepared_manifest.schema")
    for field in (
        "prepared_hash",
        "source_hash",
        "plan_hash",
        "ir_hash",
        "source_map_hash",
        "dependencies_hash",
    ):
        _require_hash(manifest[field], f"prepared_manifest.{field}")
    _validate_compiler(manifest["compiler"])
    _validate_target(manifest["target"])
    if _validate_entrypoints(manifest["entrypoints"]) != manifest["entrypoints"]:
        raise PrepareSourceContractError("prepared_manifest.entrypoints")
    _validate_environment(manifest["environment_request"])
    _validate_runtime(manifest["runtime_request"])
    _validate_base(manifest["base"])
    generated_files = _require_list(manifest["generated_files"], "prepared_manifest.generated_files")
    if not generated_files or len(generated_files) > MAX_SOURCE_DOCUMENTS + 1:
        raise PrepareSourceContractError("prepared_manifest.generated_files")
    previous = ""
    for raw in generated_files:
        item = _require_mapping(raw, "prepared_manifest.generated_file")
        _require_exact_keys(item, frozenset({"logical_path", "content_hash"}), "prepared_manifest.generated_file")
        path = _validate_logical_path(item["logical_path"])
        _require_hash(item["content_hash"], "prepared_manifest.generated_file.content_hash")
        if path <= previous:
            raise PrepareSourceContractError("prepared_manifest.generated_files_order")
        previous = path
    _validate_canonical_ir(canonical_ir)
    if manifest["ir_hash"] != _domain_json_hash(IR_HASH_DOMAIN, canonical_ir):
        raise PrepareSourceContractError("prepared_manifest.ir_hash")
    dependencies = _validate_dependencies(canonical_ir["dependencies"])
    if dependencies != canonical_ir["dependencies"]:
        raise PrepareSourceContractError("prepared_manifest.dependencies_order")
    if manifest["dependencies_hash"] != _domain_json_hash(DEPENDENCIES_HASH_DOMAIN, dependencies):
        raise PrepareSourceContractError("prepared_manifest.dependencies_hash")
    if canonical_ir["entrypoints"] != manifest["entrypoints"]:
        raise PrepareSourceContractError("prepared_manifest.entrypoint_binding")
    for field in ("environment_request", "runtime_request", "base"):
        if canonical_ir[field] != manifest[field]:
            raise PrepareSourceContractError(f"prepared_manifest.{field}_binding")
    hash_manifest = dict(manifest)
    hash_manifest["prepared_hash"] = ""
    if manifest["prepared_hash"] != _prepared_hash(hash_manifest):
        raise PrepareSourceContractError("prepared_manifest.prepared_hash")


def canonical_json_bytes(value: Any) -> bytes:
    """Canonical UTF-8 JSON for hash and byte-parity fixtures."""

    _validate_json_value(value, "canonical_json")
    return _canonical_json_text(value).encode("utf-8")


def domain_hash(domain: str, value: Any) -> str:
    """Public helper for the documented v1 domain-separated JSON hashes."""

    return _domain_json_hash(domain, value)


def _validate_and_normalize_request(request: Mapping[str, Any]) -> dict[str, Any]:
    document = _require_mapping(request, "request")
    _require_exact_keys(document, _REQUEST_KEYS, "request")
    if document["schema"] != PREPARE_SOURCE_REQUEST_SCHEMA or not _is_v1(document["schema_version"]):
        raise PrepareSourceContractError("prepare.request_schema")
    sources = _require_list(document["sources"], "request.sources")
    if not 1 <= len(sources) <= MAX_SOURCE_DOCUMENTS:
        raise PrepareSourceContractError("prepare.source_count")
    normalized_sources: list[dict[str, str]] = []
    total_bytes = 0
    for index, raw in enumerate(sources):
        source = _require_mapping(raw, f"request.sources[{index}]")
        _require_exact_keys(source, frozenset({"logical_path", "language", "text"}), "source")
        if source["language"] != "python":
            raise PrepareSourceContractError("prepare.source_language")
        path = _validate_logical_path(source["logical_path"])
        if not path.endswith(".py"):
            raise PrepareSourceContractError("prepare.source_extension")
        text = _normalize_source_text(source["text"])
        encoded = text.encode("utf-8")
        if not encoded or len(encoded) > MAX_SOURCE_DOCUMENT_BYTES:
            raise PrepareSourceContractError("prepare.source_size")
        total_bytes += len(encoded)
        normalized_sources.append({"logical_path": path, "language": "python", "text": text})
    if total_bytes > MAX_TOTAL_SOURCE_BYTES:
        raise PrepareSourceContractError("prepare.source_total_size")
    normalized_sources.sort(key=lambda item: item["logical_path"])
    paths = [item["logical_path"] for item in normalized_sources]
    if len(paths) != len(set(paths)):
        raise PrepareSourceContractError("prepare.source_path_duplicate")

    plan = _validate_plan(document["proposal_ir"])
    entrypoints = _validate_entrypoints(document["entrypoints"])
    if entrypoints != sorted(entrypoints):
        entrypoints.sort()
    environment = _validate_environment(document["environment_request"])
    runtime = _validate_runtime(document["runtime_request"])
    target = _validate_target(document["target"])
    base = _validate_base(document["base"])
    return {
        "schema": PREPARE_SOURCE_REQUEST_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "sources": normalized_sources,
        "proposal_ir": plan,
        "entrypoints": entrypoints,
        "environment_request": environment,
        "runtime_request": runtime,
        "target": target,
        "base": base,
    }


def _validate_plan(raw: Any) -> dict[str, Any]:
    plan = _require_mapping(raw, "request.proposal_ir")
    if set(plan) not in {_PLAN_KEYS, _PLAN_KEYS_WITH_SEMANTIC_PORTS}:
        raise PrepareSourceContractError("proposal_ir.keys")
    if plan["schema"] != GENERATED_PLAN_SCHEMA or not _is_v1(plan["schema_version"]):
        raise PrepareSourceContractError("prepare.plan_schema")
    kind = plan["kind"]
    if kind not in {"function", "pipeline"}:
        raise PrepareSourceContractError("prepare.plan_kind")
    name = _validate_python_identifier(plan["name"], "prepare.plan_name")
    nodes = _require_list(plan["nodes"], "proposal_ir.nodes")
    edges = _require_list(plan["edges"], "proposal_ir.edges")
    outputs = _require_list(plan["outputs"], "proposal_ir.outputs")
    if len(nodes) > MAX_PLAN_NODES or len(edges) > MAX_PLAN_EDGES or len(outputs) > MAX_PLAN_OUTPUTS:
        raise PrepareSourceContractError("prepare.plan_size")
    if kind == "function" and (nodes or edges or outputs or plan["adapters"]):
        raise PrepareSourceContractError("prepare.function_plan_shape")
    if kind == "pipeline" and not nodes:
        raise PrepareSourceContractError("prepare.pipeline_nodes")
    semantic_ports = None
    if "semantic_ports" in plan:
        if kind != "function":
            raise PrepareSourceContractError("prepare.semantic_ports_kind")
        semantic_ports = _validate_semantic_ports(plan["semantic_ports"])
    dependencies = _validate_dependencies(plan["dependencies"])
    adapters = _validate_adapters(plan["adapters"])
    normalized_nodes: list[dict[str, Any]] = []
    for raw_node in nodes:
        node = _require_mapping(raw_node, "proposal_ir.node")
        _require_exact_keys(node, frozenset({"id", "function", "runtime"}), "proposal_ir.node")
        node_id = _require_string(node["id"], "proposal_ir.node.id", maximum=MAX_IDENTIFIER_CHARS)
        if not _PLAN_ID_PATTERN.fullmatch(node_id):
            raise PrepareSourceContractError("prepare.node_id")
        function = _validate_python_identifier(node["function"], "prepare.node_function")
        runtime = node["runtime"]
        if runtime not in {None, "native", "venv-subprocess", "docker"}:
            raise PrepareSourceContractError("prepare.node_runtime")
        normalized_nodes.append({"id": node_id, "function": function, "runtime": runtime})
    normalized_nodes.sort(key=lambda item: item["id"])
    if len({node["id"] for node in normalized_nodes}) != len(normalized_nodes):
        raise PrepareSourceContractError("prepare.node_duplicate")

    normalized_edges = [_validate_plan_edge(item) for item in edges]
    normalized_edges.sort(key=_edge_sort_key)
    if len({_canonical_json_text(edge) for edge in normalized_edges}) != len(normalized_edges):
        raise PrepareSourceContractError("prepare.edge_duplicate")
    normalized_outputs = [_validate_plan_output(item) for item in outputs]
    normalized_outputs.sort(key=lambda item: item["name"])
    if len({item["name"] for item in normalized_outputs}) != len(normalized_outputs):
        raise PrepareSourceContractError("prepare.output_duplicate")
    normalized = {
        "schema": GENERATED_PLAN_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "kind": kind,
        "name": name,
        "nodes": normalized_nodes,
        "edges": normalized_edges,
        "outputs": normalized_outputs,
        "dependencies": dependencies,
        "adapters": adapters,
    }
    if semantic_ports is not None:
        normalized["semantic_ports"] = semantic_ports
    return normalized


def _validate_semantic_ports(raw: Any) -> dict[str, Any]:
    contract = _require_mapping(raw, "proposal_ir.semantic_ports")
    _require_exact_keys(
        contract,
        frozenset({"schema", "schema_version", "inputs", "outputs"}),
        "proposal_ir.semantic_ports",
    )
    if contract["schema"] != SEMANTIC_PORTS_SCHEMA or not _is_v1(contract["schema_version"]):
        raise PrepareSourceContractError("prepare.semantic_ports_schema")

    def normalize_ports(value: Any, direction: str) -> list[dict[str, str]]:
        ports = _require_list(value, f"proposal_ir.semantic_ports.{direction}")
        if len(ports) > MAX_PLAN_INPUTS:
            raise PrepareSourceContractError("prepare.semantic_ports_size")
        normalized: list[dict[str, str]] = []
        for item in ports:
            port = _require_mapping(item, f"proposal_ir.semantic_ports.{direction}.port")
            _require_exact_keys(port, frozenset({"name", "type"}), "semantic_port")
            name = _validate_python_identifier(port["name"], "prepare.semantic_port_name")
            semantic_type = _require_string(port["type"], "semantic_port.type", maximum=256)
            if not _SEMANTIC_TYPE_PATTERN.fullmatch(semantic_type):
                raise PrepareSourceContractError("prepare.semantic_port_type")
            normalized.append({"name": name, "type": semantic_type})
        normalized.sort(key=lambda item: item["name"])
        if len({item["name"] for item in normalized}) != len(normalized):
            raise PrepareSourceContractError("prepare.semantic_port_duplicate")
        return normalized

    inputs = normalize_ports(contract["inputs"], "inputs")
    outputs = normalize_ports(contract["outputs"], "outputs")
    if not inputs and not outputs:
        raise PrepareSourceContractError("prepare.semantic_ports_empty")
    return {
        "schema": SEMANTIC_PORTS_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "inputs": inputs,
        "outputs": outputs,
    }


def _validate_plan_edge(raw: Any) -> dict[str, Any]:
    edge = _require_mapping(raw, "proposal_ir.edge")
    _require_exact_keys(edge, frozenset({"from", "to", "adapter"}), "proposal_ir.edge")
    source = _require_mapping(edge["from"], "proposal_ir.edge.from")
    kind = source.get("kind")
    if kind == "node":
        _require_exact_keys(source, frozenset({"kind", "node_id", "port"}), "edge.from.node")
        normalized_source: dict[str, Any] = {
            "kind": "node",
            "node_id": _validate_plan_id(source["node_id"], "prepare.edge_source_node"),
            "port": _validate_python_identifier(source["port"], "prepare.edge_source_port"),
        }
    elif kind == "input":
        _require_exact_keys(source, frozenset({"kind", "name"}), "edge.from.input")
        normalized_source = {
            "kind": "input",
            "name": _validate_python_identifier(source["name"], "prepare.edge_input"),
        }
    elif kind == "literal":
        _require_exact_keys(source, frozenset({"kind", "value"}), "edge.from.literal")
        _validate_json_value(source["value"], "proposal_ir.edge.from.value")
        normalized_source = {"kind": "literal", "value": source["value"]}
    else:
        raise PrepareSourceContractError("prepare.edge_source_kind")
    target = _require_mapping(edge["to"], "proposal_ir.edge.to")
    _require_exact_keys(target, frozenset({"node_id", "port"}), "edge.to")
    adapter = edge["adapter"]
    if adapter is not None and (not isinstance(adapter, str) or not _ADAPTER_KEY_PATTERN.fullmatch(adapter)):
        raise PrepareSourceContractError("prepare.edge_adapter")
    return {
        "from": normalized_source,
        "to": {
            "node_id": _validate_plan_id(target["node_id"], "prepare.edge_target_node"),
            "port": _validate_python_identifier(target["port"], "prepare.edge_target_port"),
        },
        "adapter": adapter,
    }


def _validate_plan_output(raw: Any) -> dict[str, str]:
    output = _require_mapping(raw, "proposal_ir.output")
    _require_exact_keys(output, frozenset({"name", "node_id", "port"}), "proposal_ir.output")
    return {
        "name": _validate_python_identifier(output["name"], "prepare.output_name"),
        "node_id": _validate_plan_id(output["node_id"], "prepare.output_node"),
        "port": _validate_python_identifier(output["port"], "prepare.output_port"),
    }


def _validate_dependencies(raw: Any) -> list[dict[str, Any]]:
    values = _require_list(raw, "dependencies")
    if len(values) > MAX_DEPENDENCIES:
        raise PrepareSourceContractError("prepare.dependencies_size")
    result: list[dict[str, Any]] = []
    for item in values:
        dependency = _require_mapping(item, "dependency")
        _require_exact_keys(dependency, frozenset({"package", "version", "modules"}), "dependency")
        package = _require_string(dependency["package"], "dependency.package", maximum=128)
        version = _require_string(dependency["version"], "dependency.version", maximum=64)
        if not _PACKAGE_PATTERN.fullmatch(package) or not _VERSION_PATTERN.fullmatch(version):
            raise PrepareSourceContractError("prepare.dependency_identity")
        modules = _require_list(dependency["modules"], "dependency.modules")
        if not modules or len(modules) > 64:
            raise PrepareSourceContractError("prepare.dependency_modules")
        normalized_modules = sorted(_require_string(module, "dependency.module", maximum=128) for module in modules)
        if len(set(normalized_modules)) != len(normalized_modules) or any(
            not _MODULE_PATTERN.fullmatch(module) for module in normalized_modules
        ):
            raise PrepareSourceContractError("prepare.dependency_modules")
        result.append({"package": package, "version": version, "modules": normalized_modules})
    result.sort(key=lambda item: (item["package"].casefold(), item["version"], item["modules"]))
    if len({item["package"].casefold() for item in result}) != len(result):
        raise PrepareSourceContractError("prepare.dependency_duplicate")
    module_owners: dict[str, str] = {}
    for dependency in result:
        for module in dependency["modules"]:
            root = module.partition(".")[0]
            previous = module_owners.setdefault(root, dependency["package"])
            if previous != dependency["package"]:
                raise PrepareSourceContractError("prepare.dependency_module_ambiguous")
    return result


def _validate_adapters(raw: Any) -> list[dict[str, str]]:
    values = _require_list(raw, "adapters")
    if len(values) > MAX_ADAPTERS:
        raise PrepareSourceContractError("prepare.adapters_size")
    result: list[dict[str, str]] = []
    for item in values:
        adapter = _require_mapping(item, "adapter")
        _require_exact_keys(
            adapter,
            frozenset({"key", "python_type", "format", "save_symbol", "load_symbol"}),
            "adapter",
        )
        key = _require_string(adapter["key"], "adapter.key", maximum=256)
        py_type = _require_string(adapter["python_type"], "adapter.python_type", maximum=256)
        format_name = _require_string(adapter["format"], "adapter.format", maximum=64)
        if not _ADAPTER_KEY_PATTERN.fullmatch(key) or key != f"{py_type}@{format_name}":
            raise PrepareSourceContractError("prepare.adapter_key")
        result.append(
            {
                "key": key,
                "python_type": py_type,
                "format": format_name,
                "save_symbol": _validate_python_identifier(adapter["save_symbol"], "prepare.adapter_save"),
                "load_symbol": _validate_python_identifier(adapter["load_symbol"], "prepare.adapter_load"),
            }
        )
    result.sort(key=lambda item: item["key"])
    if len({item["key"] for item in result}) != len(result):
        raise PrepareSourceContractError("prepare.adapter_duplicate")
    return result


def _validate_imports(raw: Any) -> list[dict[str, Any]]:
    values = _require_list(raw, "imports")
    if len(values) > MAX_IMPORTS:
        raise PrepareSourceContractError("canonical_ir.imports_size")
    result: list[dict[str, Any]] = []
    bindings: dict[str, tuple[str, str, str | None]] = {}
    for raw_item in values:
        item = _require_mapping(raw_item, "import")
        kind = item.get("kind")
        if kind == "module":
            _require_exact_keys(item, frozenset({"kind", "module", "alias"}), "import.module")
            module = _require_string(item["module"], "import.module", maximum=128)
            if not _MODULE_PATTERN.fullmatch(module) or module == "__future__":
                raise PrepareSourceContractError("canonical_ir.import_module")
            alias = item["alias"]
            if alias is not None:
                alias = _validate_python_identifier(alias, "canonical_ir.import_alias")
            binding = alias or module.partition(".")[0]
            normalized = {"kind": "module", "module": module, "alias": alias}
            semantic = ("module", module, alias)
        elif kind == "from":
            _require_exact_keys(item, frozenset({"kind", "module", "target", "alias"}), "import.from")
            module = _require_string(item["module"], "import.module", maximum=128)
            if not _MODULE_PATTERN.fullmatch(module) or module == "__future__":
                raise PrepareSourceContractError("canonical_ir.import_module")
            target = _validate_python_identifier(item["target"], "canonical_ir.import_target")
            alias = item["alias"]
            if alias is not None:
                alias = _validate_python_identifier(alias, "canonical_ir.import_alias")
            binding = alias or target
            normalized = {"kind": "from", "module": module, "target": target, "alias": alias}
            semantic = ("from", module, f"{target}:{alias or ''}")
        else:
            raise PrepareSourceContractError("canonical_ir.import_kind")
        previous = bindings.setdefault(binding, semantic)
        if previous != semantic:
            raise PrepareSourceContractError("canonical_ir.import_binding_conflict")
        if normalized not in result:
            result.append(normalized)
    result.sort(
        key=lambda item: (
            item["kind"],
            item["module"],
            item.get("target") or "",
            item["alias"] or "",
        )
    )
    return result


def _parse_sources(
    sources: Sequence[Mapping[str, str]],
    diagnostics: list[dict[str, Any]],
    cancel: CancelCheck,
) -> tuple[dict[str, _FunctionRecord], set[str], list[dict[str, Any]], list[dict[str, Any]]]:
    functions: dict[str, _FunctionRecord] = {}
    imported_modules: set[str] = set()
    import_records: list[dict[str, Any]] = []
    import_bindings: dict[str, tuple[str, str, str | None]] = {}
    symbols: list[dict[str, Any]] = []
    parsed: list[tuple[Mapping[str, str], ast.Module, set[str]]] = []
    for source in sources:
        _check_cancel(cancel)
        path = source["logical_path"]
        text = source["text"]
        try:
            tree = ast.parse(text, filename=path, mode="exec", type_comments=True, feature_version=(3, 13))
        except (SyntaxError, ValueError, MemoryError, RecursionError):
            _add_diagnostic(diagnostics, "source.syntax_invalid", "error", path)
            continue
        try:
            _validate_ast_budget(tree, cancel)
        except PrepareSourceContractError as error:
            _add_diagnostic(diagnostics, error.code, "error", path)
            continue
        _scan_forbidden_literals(tree, path, text, diagnostics)

        bound_imports: set[str] = set()
        for statement in tree.body:
            if isinstance(statement, ast.Import):
                for alias in statement.names:
                    imported_modules.add(alias.name.partition(".")[0])
                    binding = alias.asname or alias.name.partition(".")[0]
                    bound_imports.add(binding)
                    semantic = ("module", alias.name, alias.asname)
                    previous = import_bindings.setdefault(binding, semantic)
                    if previous != semantic:
                        _add_diagnostic(
                            diagnostics,
                            "source.import_binding_conflict",
                            "error",
                            path,
                            statement,
                            binding,
                            source_text=text,
                        )
                    import_record = {"kind": "module", "module": alias.name, "alias": alias.asname}
                    if import_record not in import_records:
                        import_records.append(import_record)
            elif isinstance(statement, ast.ImportFrom):
                if statement.level != 0 or statement.module is None:
                    continue
                if statement.module == "__future__":
                    _add_diagnostic(
                        diagnostics,
                        "source.future_import_unsupported",
                        "error",
                        path,
                        statement,
                        source_text=text,
                    )
                    continue
                imported_modules.add(statement.module.partition(".")[0])
                for alias in statement.names:
                    if alias.name == "*":
                        _add_diagnostic(
                            diagnostics,
                            "source.star_import_unsupported",
                            "error",
                            path,
                            statement,
                            source_text=text,
                        )
                        continue
                    binding = alias.asname or alias.name
                    bound_imports.add(binding)
                    semantic = ("from", statement.module, f"{alias.name}:{alias.asname or ''}")
                    previous = import_bindings.setdefault(binding, semantic)
                    if previous != semantic:
                        _add_diagnostic(
                            diagnostics,
                            "source.import_binding_conflict",
                            "error",
                            path,
                            statement,
                            binding,
                            source_text=text,
                        )
                    import_record = {
                        "kind": "from",
                        "module": statement.module,
                        "target": alias.name,
                        "alias": alias.asname,
                    }
                    if import_record not in import_records:
                        import_records.append(import_record)
        parsed.append((source, tree, bound_imports))

    for source, tree, bound_imports in parsed:
        _check_cancel(cancel)
        path = source["logical_path"]
        text = source["text"]
        for index, statement in enumerate(tree.body):
            _check_cancel(cancel)
            if index == 0 and _is_docstring(statement):
                continue
            if isinstance(statement, ast.Import):
                continue
            if isinstance(statement, ast.ImportFrom):
                if statement.level != 0 or statement.module is None:
                    _add_diagnostic(
                        diagnostics,
                        "source.relative_import_unsupported",
                        "error",
                        path,
                        statement,
                        source_text=text,
                    )
                    continue
                continue
            if isinstance(statement, ast.AsyncFunctionDef):
                _add_diagnostic(
                    diagnostics,
                    "source.async_function_unsupported",
                    "error",
                    path,
                    statement,
                    source_text=text,
                )
                continue
            if not isinstance(statement, ast.FunctionDef):
                _add_diagnostic(
                    diagnostics,
                    "source.top_level_statement_unsupported",
                    "error",
                    path,
                    statement,
                    source_text=text,
                )
                continue
            function_record = _parse_function(statement, path, text, bound_imports, diagnostics)
            if function_record is None:
                continue
            if function_record.name in functions:
                _add_diagnostic(
                    diagnostics,
                    "source.duplicate_function",
                    "error",
                    path,
                    statement,
                    function_record.name,
                    source_text=text,
                )
                continue
            functions[function_record.name] = function_record
            imported_modules.update(function_record.import_modules)
            symbols.append(
                {
                    "symbol_id": f"function:{function_record.name}",
                    "kind": "function",
                    "logical_path": path,
                    "span": function_record.span,
                }
            )
    if len(functions) > MAX_FUNCTIONS:
        _add_diagnostic(diagnostics, "source.function_limit", "error", None)
    symbols.sort(key=lambda item: (item["logical_path"], item["span"]["start_line"], item["symbol_id"]))
    return functions, imported_modules, _validate_imports(import_records), symbols


def _parse_function(
    node: ast.FunctionDef,
    path: str,
    source_text: str,
    imported_bindings: set[str],
    diagnostics: list[dict[str, Any]],
) -> _FunctionRecord | None:
    valid = True
    if node.decorator_list:
        _add_diagnostic(
            diagnostics,
            "source.decorator_unsupported",
            "error",
            path,
            node,
            node.name,
            source_text=source_text,
        )
        valid = False
    args = node.args
    if args.posonlyargs or args.kwonlyargs or args.vararg is not None or args.kwarg is not None:
        _add_diagnostic(
            diagnostics,
            "source.parameter_kind_unsupported",
            "error",
            path,
            node,
            node.name,
            source_text=source_text,
        )
        valid = False
    try:
        validate_static_function_ast(node)
    except PrepareSourceContractError as error:
        _add_diagnostic(
            diagnostics,
            error.code,
            "error",
            path,
            node,
            node.name,
            source_text=source_text,
        )
        valid = False
    inputs: list[dict[str, Any]] = []
    defaults = [None] * (len(args.args) - len(args.defaults)) + list(args.defaults)
    for argument, default in zip(args.args, defaults, strict=True):
        annotation = _annotation_text(argument.annotation, diagnostics, path, node.name, source_text)
        default_value: Any = None
        has_default = default is not None
        if default is not None:
            try:
                default_value = _json_literal(default)
            except PrepareSourceContractError:
                _add_diagnostic(
                    diagnostics,
                    "source.default_unsupported",
                    "error",
                    path,
                    default,
                    node.name,
                    source_text=source_text,
                )
                valid = False
        inputs.append(
            {
                "name": argument.arg,
                "type": annotation,
                "required": not has_default,
                "default": default_value if has_default else None,
                "has_default": has_default,
            }
        )
    return_type = _annotation_text(node.returns, diagnostics, path, node.name, source_text)
    outputs = [] if return_type in {"None", "NoneType"} else [{"name": "default", "type": return_type}]
    if not any(isinstance(item, ast.Return) for item in ast.walk(node)) and outputs:
        _add_diagnostic(
            diagnostics,
            "source.return_missing",
            "error",
            path,
            node,
            node.name,
            source_text=source_text,
        )
        valid = False

    local_names = {item.arg for item in ast.walk(node) if isinstance(item, ast.arg)}
    local_names.update(
        item.id for item in ast.walk(node) if isinstance(item, ast.Name) and isinstance(item.ctx, (ast.Store, ast.Del))
    )
    local_names.update(
        item.name
        for item in ast.walk(node)
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and item is not node
    )
    local_names.update(
        item.name for item in ast.walk(node) if isinstance(item, ast.ExceptHandler) and isinstance(item.name, str)
    )
    local_import_modules: set[str] = set()
    for item in ast.walk(node):
        if isinstance(item, ast.Import):
            for alias in item.names:
                local_import_modules.add(alias.name.partition(".")[0])
                local_names.add(alias.asname or alias.name.partition(".")[0])
        elif isinstance(item, ast.ImportFrom):
            if item.level != 0 or item.module is None or any(alias.name == "*" for alias in item.names):
                continue
            local_import_modules.add(item.module.partition(".")[0])
            local_names.update(alias.asname or alias.name for alias in item.names)
    loaded_names = {item.id for item in ast.walk(node) if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Load)}
    unresolved = sorted(loaded_names - local_names - imported_bindings - _BUILTIN_NAMES)
    # Other module-level function symbols are resolved after every document is parsed.
    body = "\n".join(ast.unparse(statement) for statement in node.body)
    record = _FunctionRecord(
        name=node.name,
        logical_path=path,
        span=_span(node, source_text),
        inputs=inputs,
        outputs=outputs,
        body=body,
        unresolved_names=tuple(unresolved),
        import_modules=tuple(sorted(local_import_modules)),
    )
    return record if valid else None


def _compile_plan(
    request: Mapping[str, Any],
    functions: Mapping[str, _FunctionRecord],
    import_modules: set[str],
    imports: list[dict[str, Any]],
    diagnostics: list[dict[str, Any]],
    cancel: CancelCheck,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, list[dict[str, Any]]]]:
    plan = cast(dict[str, Any], request["proposal_ir"])
    function_names = set(functions)
    function_names_by_path: dict[str, set[str]] = {}
    for function in functions.values():
        function_names_by_path.setdefault(function.logical_path, set()).add(function.name)
    for function in functions.values():
        unresolved = sorted(set(function.unresolved_names) - function_names_by_path[function.logical_path])
        if unresolved:
            _add_diagnostic(
                diagnostics,
                "source.unresolved_symbol",
                "error",
                function.logical_path,
                subject=f"{function.name}:{','.join(unresolved)}",
            )
    dependency_modules = {
        module.partition(".")[0] for dependency in plan["dependencies"] for module in dependency["modules"]
    }
    uncovered_imports = sorted(import_modules - dependency_modules - _STDLIB_MODULES - {"__future__", "spl"})
    if uncovered_imports:
        _add_diagnostic(
            diagnostics,
            "plan.import_dependency_missing",
            "error",
            None,
            subject=",".join(uncovered_imports),
        )
    adapter_symbols = {
        symbol for adapter in plan["adapters"] for symbol in (adapter["save_symbol"], adapter["load_symbol"])
    }
    missing_adapter_symbols = sorted(adapter_symbols - function_names)
    if missing_adapter_symbols:
        _add_diagnostic(
            diagnostics,
            "plan.adapter_symbol_missing",
            "error",
            None,
            subject=",".join(missing_adapter_symbols),
        )
    if diagnostics:
        return plan, {}, {}, {"nodes": [], "edges": []}

    entrypoints = cast(list[str], request["entrypoints"])
    if entrypoints != [plan["name"]]:
        _add_diagnostic(diagnostics, "plan.entrypoint_mismatch", "error", None, subject=plan["name"])
        return plan, {}, {}, {"nodes": [], "edges": []}

    effective_functions = _apply_semantic_port_contract(plan, functions, diagnostics)
    if diagnostics:
        return plan, {}, {}, {"nodes": [], "edges": []}

    function_ir = [
        {
            "name": function.name,
            "source": {"logical_path": function.logical_path, "span": function.span},
            "inputs": function.inputs,
            "outputs": function.outputs,
            "body": function.body,
        }
        for function in sorted(effective_functions.values(), key=lambda item: item.name)
    ]
    normalized_plan = json.loads(_canonical_json_text(plan))
    if plan["kind"] == "function":
        root = effective_functions.get(plan["name"])
        if root is None:
            _add_diagnostic(diagnostics, "plan.function_missing", "error", None, subject=plan["name"])
            return normalized_plan, {}, {}, {"nodes": [], "edges": []}
        canonical_ir = {
            "schema": CANONICAL_IR_SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "kind": "function",
            "name": plan["name"],
            "entrypoints": entrypoints,
            "functions": function_ir,
            "imports": imports,
            "pipeline": None,
            "dependencies": plan["dependencies"],
            "adapters": [],
            "environment_request": request["environment_request"],
            "runtime_request": request["runtime_request"],
            "base": request["base"],
        }
        signature = {
            "kind": "function",
            "name": root.name,
            "inputs": root.inputs,
            "outputs": root.outputs,
        }
        return normalized_plan, canonical_ir, signature, {"nodes": [], "edges": []}

    nodes_by_id: dict[str, dict[str, Any]] = {}
    for node in plan["nodes"]:
        _check_cancel(cancel)
        node_function = effective_functions.get(node["function"])
        if node_function is None:
            _add_diagnostic(diagnostics, "plan.node_function_missing", "error", None, subject=node["id"])
            continue
        nodes_by_id[node["id"]] = {
            "logical_id": node["id"],
            "node_id": _deterministic_uuid(
                "splime.node/v1\x00"
                + _canonical_json_text(
                    {
                        "pipeline": plan["name"],
                        "logical_id": node["id"],
                        "function": node["function"],
                        "runtime": node["runtime"] or "native",
                    }
                )
            ),
            "function": node["function"],
            "runtime": node["runtime"] or "native",
            "inputs": node_function.inputs,
            "outputs": node_function.outputs,
        }
    if diagnostics:
        return normalized_plan, {}, {}, {"nodes": [], "edges": []}

    adapters = {adapter["key"]: adapter for adapter in plan["adapters"]}
    bindings: dict[tuple[str, str], dict[str, Any]] = {}
    canonical_edges: list[dict[str, Any]] = []
    graph_edges: list[tuple[str, str]] = []
    external_inputs: dict[str, dict[str, Any]] = {}
    for edge_index, edge in enumerate(plan["edges"]):
        _check_cancel(cancel)
        target = edge["to"]
        target_node = nodes_by_id.get(target["node_id"])
        if target_node is None:
            _add_diagnostic(diagnostics, "plan.edge_target_missing", "error", None, subject=str(edge_index))
            continue
        target_port = _port_by_name(target_node["inputs"], target["port"])
        if target_port is None:
            _add_diagnostic(diagnostics, "plan.edge_target_port_missing", "error", None, subject=str(edge_index))
            continue
        binding_key = (target["node_id"], target["port"])
        if binding_key in bindings:
            _add_diagnostic(diagnostics, "plan.edge_target_duplicate", "error", None, subject=str(edge_index))
            continue
        source = edge["from"]
        source_type: str | None = None
        canonical_source: dict[str, Any]
        if source["kind"] == "node":
            source_node = nodes_by_id.get(source["node_id"])
            if source_node is None:
                _add_diagnostic(diagnostics, "plan.edge_source_missing", "error", None, subject=str(edge_index))
                continue
            source_port = _port_by_name(source_node["outputs"], source["port"])
            if source_port is None:
                _add_diagnostic(diagnostics, "plan.edge_source_port_missing", "error", None, subject=str(edge_index))
                continue
            source_type = source_port["type"]
            canonical_source = {
                "kind": "node",
                "node_id": source_node["node_id"],
                "port": source["port"],
            }
            graph_edges.append((source["node_id"], target["node_id"]))
        elif source["kind"] == "input":
            source_type = target_port["type"]
            current = external_inputs.get(source["name"])
            if current is not None and current["type"] != source_type:
                _add_diagnostic(diagnostics, "plan.input_type_conflict", "error", None, subject=source["name"])
                continue
            external_inputs[source["name"]] = {
                "name": source["name"],
                "type": source_type,
                "required": target_port["required"],
                "default": target_port["default"],
                "has_default": target_port["has_default"],
            }
            canonical_source = {"kind": "input", "name": source["name"]}
        else:
            source_type = _literal_type(source["value"])
            canonical_source = {"kind": "literal", "value": source["value"]}

        adapter = edge["adapter"]
        if adapter is not None and adapter not in adapters:
            _add_diagnostic(diagnostics, "plan.edge_adapter_missing", "error", None, subject=str(edge_index))
            continue
        if (
            adapter is None
            and source["kind"] == "node"
            and not _edge_without_adapter_safe(source_type, target_port["type"])
        ):
            _add_diagnostic(diagnostics, "plan.edge_adapter_required", "error", None, subject=str(edge_index))
            continue
        adapter_source_types = {source_type}
        if source_type in {"bool", "bytes", "dict", "float", "int", "list", "str"}:
            adapter_source_types.add(f"builtins.{source_type}")
        if adapter is not None and adapters[adapter]["python_type"] not in adapter_source_types:
            _add_diagnostic(diagnostics, "plan.edge_adapter_type_mismatch", "error", None, subject=str(edge_index))
            continue
        canonical_edge = {
            "edge_id": f"edge-{edge_index:04d}",
            "from": canonical_source,
            "to": {"node_id": target_node["node_id"], "port": target["port"]},
            "adapter": adapter,
        }
        bindings[binding_key] = canonical_edge
        canonical_edges.append(canonical_edge)

    for logical_id, node in nodes_by_id.items():
        for port in node["inputs"]:
            if (logical_id, port["name"]) not in bindings and port["required"]:
                _add_diagnostic(
                    diagnostics,
                    "plan.required_input_unbound",
                    "error",
                    None,
                    subject=f"{logical_id}.{port['name']}",
                )
    cycle = _find_cycle(nodes_by_id, graph_edges)
    if cycle:
        _add_diagnostic(diagnostics, "plan.cycle", "error", None, subject="->".join(cycle))

    canonical_outputs: list[dict[str, Any]] = []
    for output in plan["outputs"]:
        node = nodes_by_id.get(output["node_id"])
        port = None if node is None else _port_by_name(node["outputs"], output["port"])
        if node is None or port is None:
            _add_diagnostic(diagnostics, "plan.output_source_missing", "error", None, subject=output["name"])
            continue
        canonical_outputs.append(
            {
                "name": output["name"],
                "node_id": node["node_id"],
                "port": output["port"],
                "type": port["type"],
            }
        )
    if not canonical_outputs:
        _add_diagnostic(diagnostics, "plan.pipeline_output_missing", "error", None)
    if diagnostics:
        return normalized_plan, {}, {}, {"nodes": [], "edges": []}

    canonical_nodes = [nodes_by_id[key] for key in sorted(nodes_by_id)]
    canonical_edges.sort(key=lambda item: item["edge_id"])
    canonical_outputs.sort(key=lambda item: item["name"])
    canonical_inputs = sorted(external_inputs.values(), key=lambda item: item["name"])
    pipeline = {
        "nodes": canonical_nodes,
        "edges": canonical_edges,
        "inputs": canonical_inputs,
        "outputs": canonical_outputs,
    }
    canonical_ir = {
        "schema": CANONICAL_IR_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "kind": "pipeline",
        "name": plan["name"],
        "entrypoints": entrypoints,
        "functions": function_ir,
        "imports": imports,
        "pipeline": pipeline,
        "dependencies": plan["dependencies"],
        "adapters": plan["adapters"],
        "environment_request": request["environment_request"],
        "runtime_request": request["runtime_request"],
        "base": request["base"],
    }
    signature = {
        "kind": "pipeline",
        "name": plan["name"],
        "inputs": canonical_inputs,
        "outputs": canonical_outputs,
    }
    graph_map = {
        "nodes": [
            {
                "logical_id": node["logical_id"],
                "node_id": node["node_id"],
                "function_symbol": f"function:{node['function']}",
            }
            for node in canonical_nodes
        ],
        "edges": [
            {
                "edge_id": edge["edge_id"],
                "from_node_id": edge["from"].get("node_id") if edge["from"]["kind"] == "node" else None,
                "to_node_id": edge["to"]["node_id"],
            }
            for edge in canonical_edges
        ],
    }
    return normalized_plan, canonical_ir, signature, graph_map


def _apply_semantic_port_contract(
    plan: Mapping[str, Any],
    functions: Mapping[str, _FunctionRecord],
    diagnostics: list[dict[str, Any]],
) -> dict[str, _FunctionRecord]:
    """Overlay reviewed semantic types without rewriting reviewed source."""

    contract = plan.get("semantic_ports")
    if not isinstance(contract, Mapping):
        return dict(functions)
    root = functions.get(str(plan["name"]))
    if root is None:
        return dict(functions)

    def overlay(
        existing_ports: list[dict[str, Any]],
        declarations: Any,
        direction: str,
    ) -> list[dict[str, Any]]:
        result = [dict(port) for port in existing_ports]
        by_name = {port["name"]: port for port in result}
        for declaration in cast(list[dict[str, str]], declarations):
            port = by_name.get(declaration["name"])
            if port is None:
                _add_diagnostic(
                    diagnostics,
                    f"plan.semantic_{direction}_missing",
                    "error",
                    None,
                    subject=declaration["name"],
                )
                continue
            if port["type"] is not None and port["type"] != declaration["type"]:
                _add_diagnostic(
                    diagnostics,
                    "plan.semantic_type_conflict",
                    "error",
                    root.logical_path,
                    subject=f"{direction}:{declaration['name']}",
                )
                continue
            port["type"] = declaration["type"]
        return result

    effective_root = replace(
        root,
        inputs=overlay(root.inputs, contract["inputs"], "input"),
        outputs=overlay(root.outputs, contract["outputs"], "output"),
    )
    return {**functions, root.name: effective_root}


def _validate_canonical_ir(raw: Any) -> None:
    ir = _require_mapping(raw, "canonical_ir")
    expected = frozenset(
        {
            "schema",
            "schema_version",
            "kind",
            "name",
            "entrypoints",
            "functions",
            "imports",
            "pipeline",
            "dependencies",
            "adapters",
            "environment_request",
            "runtime_request",
            "base",
        }
    )
    _require_exact_keys(ir, expected, "canonical_ir")
    if ir["schema"] != CANONICAL_IR_SCHEMA or not _is_v1(ir["schema_version"]):
        raise PrepareSourceContractError("canonical_ir.schema")
    if ir["kind"] not in {"function", "pipeline"}:
        raise PrepareSourceContractError("canonical_ir.kind")
    _validate_python_identifier(ir["name"], "canonical_ir.name")
    if _validate_entrypoints(ir["entrypoints"]) != ir["entrypoints"] or ir["entrypoints"] != [ir["name"]]:
        raise PrepareSourceContractError("canonical_ir.entrypoints")
    functions = _require_list(ir["functions"], "canonical_ir.functions")
    if not functions or len(functions) > MAX_FUNCTIONS:
        raise PrepareSourceContractError("canonical_ir.functions")
    functions_by_name: dict[str, Mapping[str, Any]] = {}
    for raw_function in functions:
        function = _require_mapping(raw_function, "canonical_ir.function")
        _require_exact_keys(
            function, frozenset({"name", "source", "inputs", "outputs", "body"}), "canonical_ir.function"
        )
        name = _validate_python_identifier(function["name"], "canonical_ir.function.name")
        if name in functions_by_name:
            raise PrepareSourceContractError("canonical_ir.function_duplicate")
        functions_by_name[name] = function
        source = _require_mapping(function["source"], "canonical_ir.function.source")
        _require_exact_keys(source, frozenset({"logical_path", "span"}), "canonical_ir.function.source")
        _validate_logical_path(source["logical_path"])
        _validate_span(source["span"])
        _validate_ports(function["inputs"], inputs=True)
        _validate_ports(function["outputs"], inputs=False)
        _require_string(function["body"], "canonical_ir.function.body", maximum=MAX_SOURCE_DOCUMENT_BYTES)
    if [item["name"] for item in functions] != sorted(item["name"] for item in functions):
        raise PrepareSourceContractError("canonical_ir.function_order")
    normalized_imports = _validate_imports(ir["imports"])
    if normalized_imports != ir["imports"]:
        raise PrepareSourceContractError("canonical_ir.imports_order")
    if _validate_dependencies(ir["dependencies"]) != ir["dependencies"]:
        raise PrepareSourceContractError("canonical_ir.dependencies_order")
    normalized_adapters = _validate_adapters(ir["adapters"])
    if normalized_adapters != ir["adapters"]:
        raise PrepareSourceContractError("canonical_ir.adapters_order")
    _validate_environment(ir["environment_request"])
    _validate_runtime(ir["runtime_request"])
    _validate_base(ir["base"])
    if ir["kind"] == "function":
        if ir["pipeline"] is not None or ir["adapters"]:
            raise PrepareSourceContractError("canonical_ir.function_shape")
        if ir["name"] not in functions_by_name:
            raise PrepareSourceContractError("canonical_ir.function_entrypoint")
        return
    pipeline = _require_mapping(ir["pipeline"], "canonical_ir.pipeline")
    _require_exact_keys(pipeline, frozenset({"nodes", "edges", "inputs", "outputs"}), "canonical_ir.pipeline")
    nodes = _require_list(pipeline["nodes"], "canonical_ir.pipeline.nodes")
    edges = _require_list(pipeline["edges"], "canonical_ir.pipeline.edges")
    if not nodes or len(nodes) > MAX_PLAN_NODES or len(edges) > MAX_PLAN_EDGES:
        raise PrepareSourceContractError("canonical_ir.pipeline_size")
    nodes_by_id: dict[str, Mapping[str, Any]] = {}
    logical_ids: set[str] = set()
    for raw_node in nodes:
        node = _require_mapping(raw_node, "canonical_ir.node")
        _require_exact_keys(
            node,
            frozenset({"logical_id", "node_id", "function", "runtime", "inputs", "outputs"}),
            "canonical_ir.node",
        )
        logical_id = _validate_plan_id(node["logical_id"], "canonical_ir.node.logical_id")
        node_id = _validate_uuid(node["node_id"], "canonical_ir.node.node_id")
        if node_id in nodes_by_id or logical_id in logical_ids:
            raise PrepareSourceContractError("canonical_ir.node_duplicate")
        bound_function = functions_by_name.get(node["function"])
        if bound_function is None or node["runtime"] not in {"native", "venv-subprocess", "docker"}:
            raise PrepareSourceContractError("canonical_ir.node_binding")
        _validate_ports(node["inputs"], inputs=True)
        _validate_ports(node["outputs"], inputs=False)
        if node["inputs"] != bound_function["inputs"] or node["outputs"] != bound_function["outputs"]:
            raise PrepareSourceContractError("canonical_ir.node_signature_binding")
        nodes_by_id[node_id] = node
        logical_ids.add(logical_id)
    if [item["logical_id"] for item in nodes] != sorted(item["logical_id"] for item in nodes):
        raise PrepareSourceContractError("canonical_ir.node_order")
    adapters_by_key = {item["key"]: item for item in normalized_adapters}
    bindings: set[tuple[str, str]] = set()
    graph_edges: list[tuple[str, str]] = []
    external_inputs: dict[str, dict[str, Any]] = {}
    expected_edge_ids = [f"edge-{index:04d}" for index in range(len(edges))]
    for edge_index, raw_edge in enumerate(edges):
        edge = _require_mapping(raw_edge, "canonical_ir.edge")
        _require_exact_keys(edge, frozenset({"edge_id", "from", "to", "adapter"}), "canonical_ir.edge")
        if edge["edge_id"] != expected_edge_ids[edge_index]:
            raise PrepareSourceContractError("canonical_ir.edge_order")
        target = _require_mapping(edge["to"], "canonical_ir.edge.to")
        _require_exact_keys(target, frozenset({"node_id", "port"}), "canonical_ir.edge.to")
        target_node = nodes_by_id.get(target["node_id"])
        if target_node is None:
            raise PrepareSourceContractError("canonical_ir.edge_target")
        target_port_name = _validate_python_identifier(target["port"], "canonical_ir.edge.port")
        target_port = _port_by_name(target_node["inputs"], target_port_name)
        if target_port is None:
            raise PrepareSourceContractError("canonical_ir.edge_target_port")
        binding = (target["node_id"], target_port_name)
        if binding in bindings:
            raise PrepareSourceContractError("canonical_ir.edge_target_duplicate")
        bindings.add(binding)
        source = _require_mapping(edge["from"], "canonical_ir.edge.from")
        source_type: str | None
        if source.get("kind") == "node":
            _require_exact_keys(source, frozenset({"kind", "node_id", "port"}), "canonical_ir.edge.from.node")
            source_node = nodes_by_id.get(source["node_id"])
            if source_node is None:
                raise PrepareSourceContractError("canonical_ir.edge_source")
            source_port = _port_by_name(source_node["outputs"], source["port"])
            if source_port is None:
                raise PrepareSourceContractError("canonical_ir.edge_source_port")
            source_type = cast(str | None, source_port["type"])
            graph_edges.append((source["node_id"], target["node_id"]))
        elif source.get("kind") == "input":
            _require_exact_keys(source, frozenset({"kind", "name"}), "canonical_ir.edge.from.input")
            input_name = _validate_python_identifier(source["name"], "canonical_ir.edge.input")
            source_type = cast(str | None, target_port["type"])
            candidate_input = {
                "name": input_name,
                "type": source_type,
                "required": target_port["required"],
                "default": target_port["default"],
                "has_default": target_port["has_default"],
            }
            previous = external_inputs.get(input_name)
            if previous is not None and previous != candidate_input:
                raise PrepareSourceContractError("canonical_ir.input_conflict")
            external_inputs[input_name] = candidate_input
        elif source.get("kind") == "literal":
            _require_exact_keys(source, frozenset({"kind", "value"}), "canonical_ir.edge.from.literal")
            _validate_json_value(source["value"], "canonical_ir.edge.literal")
            source_type = _literal_type(source["value"])
        else:
            raise PrepareSourceContractError("canonical_ir.edge_source_kind")
        adapter_key = edge["adapter"]
        if adapter_key is not None and adapter_key not in adapters_by_key:
            raise PrepareSourceContractError("canonical_ir.edge_adapter")
        if adapter_key is not None:
            expected_types = {source_type}
            if source_type in {"bool", "bytes", "dict", "float", "int", "list", "str"}:
                expected_types.add(f"builtins.{source_type}")
            if adapters_by_key[adapter_key]["python_type"] not in expected_types:
                raise PrepareSourceContractError("canonical_ir.edge_adapter_type")
        elif source["kind"] == "node" and not _edge_without_adapter_safe(
            source_type, cast(str | None, target_port["type"])
        ):
            raise PrepareSourceContractError("canonical_ir.edge_adapter_required")
    for node_id, node in nodes_by_id.items():
        for port in node["inputs"]:
            if port["required"] and (node_id, port["name"]) not in bindings:
                raise PrepareSourceContractError("canonical_ir.required_input_unbound")
    if _find_cycle(nodes_by_id, graph_edges) is not None:
        raise PrepareSourceContractError("canonical_ir.cycle")
    _validate_ports(pipeline["inputs"], inputs=True)
    expected_inputs = sorted(external_inputs.values(), key=lambda item: item["name"])
    if pipeline["inputs"] != expected_inputs:
        raise PrepareSourceContractError("canonical_ir.pipeline_inputs")
    _validate_pipeline_outputs(pipeline["outputs"], set(nodes_by_id))
    for output in pipeline["outputs"]:
        node = nodes_by_id[output["node_id"]]
        port = _port_by_name(node["outputs"], output["port"])
        if port is None or output["type"] != port["type"]:
            raise PrepareSourceContractError("canonical_ir.pipeline_output_binding")


def _validate_source_map(raw: Any) -> None:
    source_map = _require_mapping(raw, "source_map")
    _require_exact_keys(source_map, frozenset({"schema", "schema_version", "symbols", "nodes", "edges"}), "source_map")
    if source_map["schema"] != SOURCE_MAP_SCHEMA or not _is_v1(source_map["schema_version"]):
        raise PrepareSourceContractError("source_map.schema")
    symbols = _require_list(source_map["symbols"], "source_map.symbols")
    for raw_symbol in symbols:
        symbol = _require_mapping(raw_symbol, "source_map.symbol")
        _require_exact_keys(symbol, frozenset({"symbol_id", "kind", "logical_path", "span"}), "source_map.symbol")
        if symbol["kind"] != "function" or not str(symbol["symbol_id"]).startswith("function:"):
            raise PrepareSourceContractError("source_map.symbol")
        _validate_logical_path(symbol["logical_path"])
        _validate_span(symbol["span"])
    nodes = _require_list(source_map["nodes"], "source_map.nodes")
    for raw_node in nodes:
        node = _require_mapping(raw_node, "source_map.node")
        _require_exact_keys(node, frozenset({"logical_id", "node_id", "function_symbol"}), "source_map.node")
        _validate_plan_id(node["logical_id"], "source_map.node.logical_id")
        _validate_uuid(node["node_id"], "source_map.node.node_id")
    edges = _require_list(source_map["edges"], "source_map.edges")
    for raw_edge in edges:
        edge = _require_mapping(raw_edge, "source_map.edge")
        _require_exact_keys(edge, frozenset({"edge_id", "from_node_id", "to_node_id"}), "source_map.edge")
        if edge["from_node_id"] is not None:
            _validate_uuid(edge["from_node_id"], "source_map.edge.from_node_id")
        _validate_uuid(edge["to_node_id"], "source_map.edge.to_node_id")


def _validate_signature(raw: Any) -> None:
    signature = _require_mapping(raw, "signature")
    _require_exact_keys(signature, frozenset({"kind", "name", "inputs", "outputs"}), "signature")
    if signature["kind"] not in {"function", "pipeline"}:
        raise PrepareSourceContractError("signature.kind")
    _validate_python_identifier(signature["name"], "signature.name")
    _validate_ports(signature["inputs"], inputs=True)
    if signature["kind"] == "pipeline":
        _validate_pipeline_outputs(signature["outputs"], None)
    else:
        _validate_ports(signature["outputs"], inputs=False)


def _validate_ports(raw: Any, *, inputs: bool) -> None:
    ports = _require_list(raw, "ports")
    if len(ports) > MAX_PLAN_INPUTS:
        raise PrepareSourceContractError("ports.size")
    seen: set[str] = set()
    expected = (
        frozenset({"name", "type", "required", "default", "has_default"}) if inputs else frozenset({"name", "type"})
    )
    for raw_port in ports:
        port = _require_mapping(raw_port, "port")
        _require_exact_keys(port, expected, "port")
        name = _validate_python_identifier(port["name"], "port.name")
        if name in seen:
            raise PrepareSourceContractError("port.duplicate")
        seen.add(name)
        if port["type"] is not None:
            _require_string(port["type"], "port.type", maximum=256)
        if inputs:
            if type(port["required"]) is not bool or type(port["has_default"]) is not bool:
                raise PrepareSourceContractError("port.default_shape")
            if port["required"] == port["has_default"]:
                raise PrepareSourceContractError("port.default_shape")
            if port["has_default"]:
                _validate_json_value(port["default"], "port.default")
            elif port["default"] is not None:
                raise PrepareSourceContractError("port.default_shape")


def _validate_pipeline_outputs(raw: Any, node_ids: set[str] | None) -> None:
    outputs = _require_list(raw, "pipeline.outputs")
    if not outputs or len(outputs) > MAX_PLAN_OUTPUTS:
        raise PrepareSourceContractError("pipeline.outputs")
    seen: set[str] = set()
    for raw_output in outputs:
        output = _require_mapping(raw_output, "pipeline.output")
        _require_exact_keys(output, frozenset({"name", "node_id", "port", "type"}), "pipeline.output")
        name = _validate_python_identifier(output["name"], "pipeline.output.name")
        if name in seen:
            raise PrepareSourceContractError("pipeline.output_duplicate")
        seen.add(name)
        node_id = _validate_uuid(output["node_id"], "pipeline.output.node_id")
        if node_ids is not None and node_id not in node_ids:
            raise PrepareSourceContractError("pipeline.output_node")
        _validate_python_identifier(output["port"], "pipeline.output.port")
        if output["type"] is not None:
            _require_string(output["type"], "pipeline.output.type", maximum=256)


def _validate_entrypoints(raw: Any) -> list[str]:
    values = _require_list(raw, "entrypoints")
    if not values or len(values) > 16:
        raise PrepareSourceContractError("prepare.entrypoints")
    result = [_validate_python_identifier(value, "prepare.entrypoint") for value in values]
    if len(result) != len(set(result)):
        raise PrepareSourceContractError("prepare.entrypoint_duplicate")
    return sorted(result)


def _validate_environment(raw: Any) -> dict[str, str] | None:
    if raw is None:
        return None
    value = _require_mapping(raw, "environment_request")
    _require_exact_keys(value, frozenset({"ref"}), "environment_request")
    ref = _require_string(value["ref"], "environment_request.ref", maximum=128)
    if not _PLAN_ID_PATTERN.fullmatch(ref):
        raise PrepareSourceContractError("prepare.environment_ref")
    return {"ref": ref}


def _validate_runtime(raw: Any) -> dict[str, str] | None:
    if raw is None:
        return None
    value = _require_mapping(raw, "runtime_request")
    _require_exact_keys(value, frozenset({"mode"}), "runtime_request")
    if value["mode"] not in {"venv", "docker"}:
        raise PrepareSourceContractError("prepare.runtime_mode")
    return {"mode": value["mode"]}


def _validate_base(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    value = _require_mapping(raw, "base")
    _require_exact_keys(
        value,
        frozenset(
            {
                "kind",
                "owner_id",
                "library_id",
                "object_id",
                "version_id",
                "version",
                "content_hash",
            }
        ),
        "base",
    )
    if value["kind"] not in {"object", "pipeline"}:
        raise PrepareSourceContractError("prepare.base_kind")
    owner_id = _require_string(value["owner_id"], "base.owner_id", maximum=160)
    library_id = _require_string(value["library_id"], "base.library_id", maximum=160)
    object_id = _require_string(value["object_id"], "base.object_id", maximum=160)
    version_id = _require_string(value["version_id"], "base.version_id", maximum=160)
    if any(not _IDENTITY_PATTERN.fullmatch(identity) for identity in (owner_id, library_id, object_id, version_id)):
        raise PrepareSourceContractError("prepare.base_identity")
    version = value["version"]
    if type(version) is not int or not 1 <= version <= MAX_SAFE_JSON_INTEGER:
        raise PrepareSourceContractError("prepare.base_version")
    _require_hash(value["content_hash"], "base.content_hash")
    return {
        "kind": value["kind"],
        "owner_id": owner_id,
        "library_id": library_id,
        "object_id": object_id,
        "version_id": version_id,
        "version": version,
        "content_hash": value["content_hash"],
    }


def _validate_target(raw: Any) -> dict[str, Any]:
    target = _require_mapping(raw, "target")
    _require_exact_keys(target, frozenset({"python_language", "spl_ir_schema", "preparation_protocol"}), "target")
    if (
        target["python_language"] != PYTHON_LANGUAGE
        or target["spl_ir_schema"] != CANONICAL_IR_SCHEMA
        or type(target["preparation_protocol"]) is not int
        or target["preparation_protocol"] != PREPARATION_PROTOCOL
    ):
        raise PrepareSourceContractError("prepare.target_incompatible")
    return {
        "python_language": PYTHON_LANGUAGE,
        "spl_ir_schema": CANONICAL_IR_SCHEMA,
        "preparation_protocol": PREPARATION_PROTOCOL,
    }


def _validate_compiler(raw: Any) -> dict[str, Any]:
    compiler = _require_mapping(raw, "compiler")
    _require_exact_keys(compiler, frozenset({"name", "version", "canonicalization_version"}), "compiler")
    if compiler != _compiler_identity():
        raise PrepareSourceContractError("prepared.compiler_incompatible")
    return dict(compiler)


def _compiler_identity() -> dict[str, Any]:
    return {
        "name": COMPILER_NAME,
        "version": COMPILER_VERSION,
        "canonicalization_version": CANONICALIZATION_VERSION,
    }


def _validate_ast_budget(tree: ast.AST, cancel: CancelCheck) -> None:
    count = 0
    maximum_depth = 0
    stack: list[tuple[ast.AST, int]] = [(tree, 1)]
    while stack:
        _check_cancel(cancel)
        node, depth = stack.pop()
        count += 1
        maximum_depth = max(maximum_depth, depth)
        if count > MAX_AST_NODES:
            raise PrepareSourceContractError("source.ast_node_limit")
        if maximum_depth > MAX_AST_DEPTH:
            raise PrepareSourceContractError("source.ast_depth_limit")
        stack.extend((child, depth + 1) for child in ast.iter_child_nodes(node))


def _scan_forbidden_literals(
    tree: ast.AST,
    path: str,
    source_text: str,
    diagnostics: list[dict[str, Any]],
) -> None:
    """Reject obvious embedded credentials and host paths without echoing them."""

    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if _SECRET_LITERAL_PATTERN.search(node.value):
                _add_diagnostic(
                    diagnostics,
                    "source.secret_like_literal",
                    "error",
                    path,
                    node,
                    source_text=source_text,
                )
            if _ABSOLUTE_PATH_PATTERN.match(node.value):
                _add_diagnostic(
                    diagnostics,
                    "source.host_path_literal",
                    "error",
                    path,
                    node,
                    source_text=source_text,
                )
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            assigned = node.value
            if not isinstance(assigned, ast.Constant) or not isinstance(assigned.value, str) or not assigned.value:
                continue
            if any(isinstance(target, ast.Name) and _SECRET_NAME_PATTERN.search(target.id) for target in targets):
                _add_diagnostic(
                    diagnostics,
                    "source.secret_like_literal",
                    "error",
                    path,
                    node,
                    source_text=source_text,
                )


def _annotation_text(
    annotation: ast.expr | None,
    diagnostics: list[dict[str, Any]],
    path: str,
    subject: str,
    source_text: str,
) -> str | None:
    if annotation is None:
        return None
    allowed = (
        ast.Attribute,
        ast.BinOp,
        ast.BitOr,
        ast.Constant,
        ast.Load,
        ast.Name,
        ast.Slice,
        ast.Subscript,
        ast.Tuple,
    )
    if any(not isinstance(item, allowed) for item in ast.walk(annotation)):
        _add_diagnostic(
            diagnostics,
            "source.annotation_unsupported",
            "error",
            path,
            annotation,
            subject,
            source_text=source_text,
        )
        return None
    return ast.unparse(annotation)


def _json_literal(node: ast.expr) -> Any:
    if isinstance(node, ast.Constant):
        value = node.value
        if value is None or type(value) in {bool, int, float, str}:
            if isinstance(value, float) and not math.isfinite(value):
                raise PrepareSourceContractError("source.default_unsupported")
            # JSON has one number domain and loses the Python distinction
            # between ``2`` and ``2.0`` after a TypeScript round trip.  The v1
            # generated subset rejects that ambiguous default rather than
            # silently changing its Python runtime type.
            if isinstance(value, float) and value.is_integer():
                raise PrepareSourceContractError("source.default_unsupported")
            if type(value) is int and abs(value) > MAX_SAFE_JSON_INTEGER:
                raise PrepareSourceContractError("source.default_unsupported")
            return value
        raise PrepareSourceContractError("source.default_unsupported")
    if (
        isinstance(node, ast.UnaryOp)
        and isinstance(node.op, (ast.UAdd, ast.USub))
        and isinstance(node.operand, ast.Constant)
    ):
        value = node.operand.value
        if type(value) not in {int, float}:
            raise PrepareSourceContractError("source.default_unsupported")
        number = cast(int | float, value)
        signed_number = number if isinstance(node.op, ast.UAdd) else -number
        if isinstance(signed_number, float) and not math.isfinite(signed_number):
            raise PrepareSourceContractError("source.default_unsupported")
        if isinstance(signed_number, float) and signed_number.is_integer():
            raise PrepareSourceContractError("source.default_unsupported")
        if type(signed_number) is int and abs(signed_number) > MAX_SAFE_JSON_INTEGER:
            raise PrepareSourceContractError("source.default_unsupported")
        return signed_number
    if isinstance(node, ast.List):
        return [_json_literal(item) for item in node.elts]
    if isinstance(node, ast.Dict):
        mapping_result: dict[str, Any] = {}
        for raw_key, item_value in zip(node.keys, node.values, strict=True):
            if (
                not isinstance(raw_key, ast.Constant)
                or not isinstance(raw_key.value, str)
                or raw_key.value in mapping_result
            ):
                raise PrepareSourceContractError("source.default_unsupported")
            mapping_result[raw_key.value] = _json_literal(item_value)
        return mapping_result
    raise PrepareSourceContractError("source.default_unsupported")


def _validate_json_value(value: Any, name: str) -> None:
    try:
        m_json_contract.validate_json_value(value, path=f"$.{name}")
    except (RecursionError, ValueError) as exc:
        raise PrepareSourceContractError("prepare.json_invalid") from exc
    nodes = 0
    strings = 0
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise PrepareSourceContractError("prepare.json_budget")
        if isinstance(item, str):
            strings += len(item)
            if len(item) > MAX_STRING_CHARS or strings > MAX_TOTAL_SOURCE_BYTES:
                raise PrepareSourceContractError("prepare.json_budget")
        elif type(item) is int:
            if abs(item) > MAX_SAFE_JSON_INTEGER:
                raise PrepareSourceContractError("prepare.json_number_range")
        elif isinstance(item, float):
            if not math.isfinite(item) or (item.is_integer() and abs(item) > MAX_SAFE_JSON_INTEGER):
                raise PrepareSourceContractError("prepare.json_number_range")
        elif isinstance(item, dict):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)


def _normalize_source_text(raw: Any) -> str:
    text = _require_string(raw, "source.text", maximum=MAX_SOURCE_DOCUMENT_BYTES)
    if text.startswith("\ufeff"):
        text = text[1:]
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise PrepareSourceContractError("prepare.source_unicode") from exc
    return text


def _validate_logical_path(raw: Any) -> str:
    path = _require_string(raw, "logical_path", maximum=MAX_LOGICAL_PATH_CHARS)
    # v1 intentionally uses the portable ASCII path subset frozen in the
    # Python/TypeScript schemas. Source text itself remains canonical UTF-8;
    # host- or locale-dependent path normalization never participates.
    if not _LOGICAL_PATH_PATTERN.fullmatch(path):
        raise PrepareSourceContractError("prepare.logical_path")
    return path


def _validate_python_identifier(raw: Any, code: str) -> str:
    value = _require_string(raw, code, maximum=MAX_IDENTIFIER_CHARS)
    # Python accepts a broader Unicode identifier grammar, but v1 freezes an
    # ASCII subset that JSON Schema and TypeScript can validate identically.
    if not _PYTHON_IDENTIFIER_PATTERN.fullmatch(value) or keyword.iskeyword(value):
        raise PrepareSourceContractError(code)
    return value


def _validate_plan_id(raw: Any, code: str) -> str:
    value = _require_string(raw, code, maximum=MAX_IDENTIFIER_CHARS)
    if not _PLAN_ID_PATTERN.fullmatch(value):
        raise PrepareSourceContractError(code)
    return value


def _validate_uuid(raw: Any, code: str) -> str:
    value = _require_string(raw, code, maximum=36)
    if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", value):
        raise PrepareSourceContractError(code)
    return value


def _validate_span(raw: Any) -> dict[str, int]:
    span = _require_mapping(raw, "span")
    _require_exact_keys(span, frozenset({"start_line", "start_column", "end_line", "end_column"}), "span")
    for key in ("start_line", "start_column", "end_line", "end_column"):
        value = span[key]
        if type(value) is not int or not 0 <= value <= MAX_SAFE_JSON_INTEGER:
            raise PrepareSourceContractError("span.invalid")
    if (span["end_line"], span["end_column"]) < (span["start_line"], span["start_column"]):
        raise PrepareSourceContractError("span.order")
    return cast(dict[str, int], span)


def _validate_diagnostic(raw: Any) -> None:
    diagnostic = _require_mapping(raw, "diagnostic")
    _require_exact_keys(
        diagnostic, frozenset({"code", "severity", "logical_path", "span", "subject", "message"}), "diagnostic"
    )
    if not re.fullmatch(r"[a-z][a-z0-9_.]{1,127}", str(diagnostic["code"])):
        raise PrepareSourceContractError("diagnostic.code")
    if diagnostic["severity"] not in {"error", "warning", "info"}:
        raise PrepareSourceContractError("diagnostic.severity")
    if diagnostic["logical_path"] is not None:
        _validate_logical_path(diagnostic["logical_path"])
    if diagnostic["span"] is not None:
        _validate_span(diagnostic["span"])
    if diagnostic["subject"] is not None:
        _require_string(diagnostic["subject"], "diagnostic.subject", maximum=256)
    _require_string(diagnostic["message"], "diagnostic.message", maximum=256)


def _invalid_result(diagnostics: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = _ordered_diagnostics(diagnostics)
    return {
        "schema": PREPARE_SOURCE_RESULT_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "status": "invalid",
        "diagnostics": ordered,
        "prepared": None,
    }


def _ordered_diagnostics(diagnostics: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in sorted(
            diagnostics[:MAX_DIAGNOSTICS],
            key=lambda item: (
                {"error": 0, "warning": 1, "info": 2}[item["severity"]],
                item["code"],
                item["logical_path"] or "",
                (item["span"] or {}).get("start_line", -1),
                item["subject"] or "",
            ),
        )
    ]


_DIAGNOSTIC_MESSAGES: dict[str, str] = {
    "plan.adapter_pickle_trusted_only": "Pickle adapters are trusted-only and require explicit later review.",
    "plan.adapter_symbol_missing": "An adapter references a function that is not present in the generated bundle.",
    "plan.cycle": "The proposed Pipeline graph contains a cycle.",
    "plan.edge_adapter_missing": "An edge references an undeclared adapter.",
    "plan.edge_adapter_required": "A non-JSON edge requires an explicit adapter.",
    "plan.edge_adapter_type_mismatch": "The selected adapter does not match the source port type.",
    "plan.edge_source_missing": "An edge references a missing source Node.",
    "plan.edge_source_port_missing": "An edge references a missing source port.",
    "plan.edge_target_duplicate": "A Node input has more than one binding.",
    "plan.edge_target_missing": "An edge references a missing target Node.",
    "plan.edge_target_port_missing": "An edge references a missing target port.",
    "plan.entrypoint_mismatch": "The generated plan and requested entrypoint do not match.",
    "plan.function_missing": "The requested Function entrypoint is not present in generated source.",
    "plan.import_dependency_missing": "A generated import has no exact declared dependency.",
    "plan.input_type_conflict": "One Pipeline input is bound to incompatible port types.",
    "plan.node_function_missing": "A proposed Node references a missing generated function.",
    "plan.output_source_missing": "A Pipeline output references a missing Node or port.",
    "plan.pipeline_output_missing": "A Pipeline requires at least one explicit output.",
    "plan.required_input_unbound": "A required Node input is not bound by an edge or Pipeline input.",
    "source.annotation_unsupported": "An annotation uses syntax outside the static generated subset.",
    "source.ast_depth_limit": "The generated source exceeds the AST depth limit.",
    "source.ast_node_limit": "The generated source exceeds the AST node limit.",
    "source.async_function_unsupported": "Async functions are outside the static generated subset.",
    "source.async_comprehension_unsupported": "Async comprehensions are outside the static generated subset.",
    "source.break_outside_loop": "A break statement appears outside an enclosing loop body.",
    "source.continue_outside_loop": "A continue statement appears outside an enclosing loop body.",
    "source.decorator_unsupported": "Decorators are outside the static generated subset.",
    "source.default_unsupported": "A default value is not a finite JSON literal.",
    "source.duplicate_function": "A generated function symbol is defined more than once.",
    "source.duplicate_parameter": "A generated function declares one parameter name more than once.",
    "source.dynamic_call_unsupported": "A dynamic evaluation or namespace call is not supported.",
    "source.dynamic_scope_unsupported": "Dynamic scope, yield, or await syntax is not supported.",
    "source.function_limit": "The generated bundle exceeds the function limit.",
    "source.future_import_unsupported": "Future imports are outside the fixed generated-source grammar.",
    "source.import_binding_conflict": "Generated imports bind one name to conflicting symbols.",
    "source.host_path_literal": "Generated source contains an absolute host path literal.",
    "source.local_import_unsupported": "Relative and star imports inside generated functions are not supported.",
    "source.nested_class_unsupported": "Nested classes are outside the static generated subset.",
    "source.parameter_kind_unsupported": "Only positional-or-keyword parameters are supported.",
    "source.relative_import_unsupported": "Relative imports are not supported in generated source.",
    "source.return_missing": "A value-producing generated function has no return statement.",
    "source.secret_like_literal": "Generated source contains a secret-like literal and cannot be prepared.",
    "source.star_import_unsupported": "Star imports are outside the closed static generated subset.",
    "source.syntax_invalid": "The generated Python source is not valid under the selected grammar.",
    "source.top_level_statement_unsupported": "Only imports and function definitions are allowed at module level.",
    "source.unresolved_symbol": "A generated function references an unresolved symbol.",
}


def _add_diagnostic(
    diagnostics: list[dict[str, Any]],
    code: str,
    severity: DiagnosticSeverity,
    logical_path: str | None,
    node: ast.AST | None = None,
    subject: str | None = None,
    *,
    source_text: str | None = None,
) -> None:
    if len(diagnostics) >= MAX_DIAGNOSTICS:
        return
    diagnostics.append(
        {
            "code": code,
            "severity": severity,
            "logical_path": logical_path,
            "span": _span(node, source_text) if node is not None else None,
            "subject": subject,
            "message": _DIAGNOSTIC_MESSAGES.get(code, "The generated source does not satisfy the static contract."),
        }
    )


def _span(node: ast.AST, source_text: str | None = None) -> dict[str, int]:
    start_line = max(0, int(getattr(node, "lineno", 1)) - 1)
    start_column = _unicode_column(source_text, start_line, max(0, int(getattr(node, "col_offset", 0))))
    end_line = max(start_line, int(getattr(node, "end_lineno", start_line + 1)) - 1)
    end_column = _unicode_column(
        source_text,
        end_line,
        max(0, int(getattr(node, "end_col_offset", start_column))),
    )
    return {
        "start_line": start_line,
        "start_column": start_column,
        "end_line": end_line,
        "end_column": end_column,
    }


def _unicode_column(source_text: str | None, line_index: int, byte_column: int) -> int:
    """Convert CPython AST UTF-8 byte columns to Unicode code-point columns."""

    if source_text is None:
        return byte_column
    lines = source_text.splitlines()
    if line_index >= len(lines):
        return byte_column
    encoded = lines[line_index].encode("utf-8")
    if byte_column > len(encoded):
        return len(lines[line_index])
    try:
        return len(encoded[:byte_column].decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise PrepareSourceContractError("source.span_unicode") from exc


def _is_docstring(statement: ast.stmt) -> bool:
    return (
        isinstance(statement, ast.Expr)
        and isinstance(statement.value, ast.Constant)
        and isinstance(statement.value.value, str)
    )


def _port_by_name(ports: Sequence[Mapping[str, Any]], name: str) -> Mapping[str, Any] | None:
    return next((port for port in ports if port["name"] == name), None)


def _literal_type(value: Any) -> str:
    if value is None:
        return "None"
    return type(value).__name__


def _edge_without_adapter_safe(source_type: str | None, target_type: str | None) -> bool:
    if source_type is None or target_type is None:
        return True
    return source_type == target_type and source_type in _JSON_TYPE_NAMES


def _find_cycle(nodes: Mapping[str, Any], edges: Iterable[tuple[str, str]]) -> list[str] | None:
    adjacency: dict[str, list[str]] = {node_id: [] for node_id in nodes}
    for source, target in sorted(edges):
        adjacency[source].append(target)
    state: dict[str, int] = {node_id: 0 for node_id in nodes}
    path: list[str] = []

    def visit(node_id: str) -> list[str] | None:
        state[node_id] = 1
        path.append(node_id)
        for target in adjacency[node_id]:
            if state[target] == 0:
                found = visit(target)
                if found is not None:
                    return found
            elif state[target] == 1:
                return [*path[path.index(target) :], target]
        path.pop()
        state[node_id] = 2
        return None

    for node_id in sorted(nodes):
        if state[node_id] == 0:
            found = visit(node_id)
            if found is not None:
                return found
    return None


def _edge_sort_key(edge: Mapping[str, Any]) -> tuple[str, ...]:
    source = edge["from"]
    if source["kind"] == "node":
        source_key = ("node", source["node_id"], source["port"])
    elif source["kind"] == "input":
        source_key = ("input", source["name"], "")
    else:
        source_key = ("literal", _canonical_json_text(source["value"]), "")
    return (*source_key, edge["to"]["node_id"], edge["to"]["port"], edge["adapter"] or "")


def _deterministic_uuid(seed: str) -> str:
    raw = bytearray(hashlib.sha256(seed.encode("utf-8")).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x80
    raw[8] = (raw[8] & 0x3F) | 0x80
    value = raw.hex()
    return f"{value[:8]}-{value[8:12]}-{value[12:16]}-{value[16:20]}-{value[20:]}"


def _generated_ir_path(source_path: str, entrypoint: str) -> str:
    prefix, separator, _ = source_path.rpartition("/")
    name = f"{entrypoint}.spl.yaml"
    return f"{prefix}/{name}" if separator else name


def _source_bundle_hash(documents: Sequence[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    digest.update(SOURCE_HASH_DOMAIN.encode("ascii"))
    digest.update(b"\x00")
    for document in documents:
        path = str(document["logical_path"]).encode("utf-8")
        text = str(document["normalized_text"]).encode("utf-8")
        digest.update(len(path).to_bytes(8, "big"))
        digest.update(path)
        digest.update(len(text).to_bytes(8, "big"))
        digest.update(text)
    return f"sha256:{digest.hexdigest()}"


def _prepared_hash(manifest_without_hash: Mapping[str, Any]) -> str:
    return _domain_json_hash(PREPARED_HASH_DOMAIN, manifest_without_hash)


def _domain_json_hash(domain: str, value: Any) -> str:
    digest = hashlib.sha256(domain.encode("ascii") + b"\x00" + canonical_json_bytes(value)).hexdigest()
    return f"sha256:{digest}"


def _content_hash(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _canonical_json_text(value: Any) -> str:
    if value is None:
        return "null"
    if type(value) is bool:
        return "true" if value else "false"
    if type(value) is int:
        return str(value)
    if isinstance(value, float):
        return _canonical_float_text(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    if isinstance(value, list):
        return "[{}]".format(",".join(_canonical_json_text(item) for item in value))
    if isinstance(value, dict):
        return "{{{}}}".format(
            ",".join(
                "{}:{}".format(
                    json.dumps(key, ensure_ascii=False, allow_nan=False),
                    _canonical_json_text(value[key]),
                )
                for key in sorted(value)
            )
        )
    raise PrepareSourceContractError("prepare.json_invalid")


def _canonical_float_text(value: float) -> str:
    """Return the ECMAScript/JCS shortest spelling for one safe finite float."""

    if not math.isfinite(value):
        raise PrepareSourceContractError("prepare.json_number_range")
    if value == 0:
        return "0"
    if value.is_integer():
        if abs(value) > MAX_SAFE_JSON_INTEGER:
            raise PrepareSourceContractError("prepare.json_number_range")
        return str(int(value))
    spelling = repr(value).lower()
    if "e" not in spelling:
        return spelling
    mantissa, exponent_text = spelling.split("e", 1)
    exponent = int(exponent_text)
    negative = mantissa.startswith("-")
    unsigned_mantissa = mantissa[1:] if negative else mantissa
    digits = unsigned_mantissa.replace(".", "")
    decimal_position = 1 + exponent
    if -6 <= exponent < 21:
        if decimal_position <= 0:
            fixed = "0." + ("0" * -decimal_position) + digits
        elif decimal_position >= len(digits):
            fixed = digits + ("0" * (decimal_position - len(digits)))
        else:
            fixed = digits[:decimal_position] + "." + digits[decimal_position:]
        return ("-" if negative else "") + fixed
    exponent_suffix = f"+{exponent}" if exponent >= 0 else str(exponent)
    return ("-" if negative else "") + unsigned_mantissa + "e" + exponent_suffix


def _require_hash(raw: Any, name: str) -> str:
    value = _require_string(raw, name, maximum=71)
    if not _HASH_PATTERN.fullmatch(value):
        raise PrepareSourceContractError("prepare.hash")
    return value


def _is_v1(raw: Any) -> bool:
    return type(raw) is int and raw == SCHEMA_VERSION


def _require_mapping(raw: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise PrepareSourceContractError(f"{name}.object")
    return raw


def _require_list(raw: Any, name: str) -> list[Any]:
    if not isinstance(raw, list):
        raise PrepareSourceContractError(f"{name}.array")
    return raw


def _require_string(raw: Any, name: str, *, maximum: int) -> str:
    if not isinstance(raw, str) or not raw or len(raw) > maximum:
        raise PrepareSourceContractError(f"{name}.string")
    try:
        raw.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise PrepareSourceContractError(f"{name}.unicode") from exc
    return raw


def _require_exact_keys(value: Mapping[str, Any], expected: frozenset[str], name: str) -> None:
    if set(value) != expected:
        raise PrepareSourceContractError(f"{name}.fields")


def _check_cancel(cancelled: CancelCheck) -> None:
    if cancelled():
        raise PreparationCancelled("preparation cancelled")


__all__ = [
    "CANONICALIZATION_VERSION",
    "CANONICAL_IR_SCHEMA",
    "COMPILER_NAME",
    "COMPILER_VERSION",
    "DEPENDENCIES_HASH_DOMAIN",
    "GENERATED_PLAN_SCHEMA",
    "IR_HASH_DOMAIN",
    "PLAN_HASH_DOMAIN",
    "PREPARED_HASH_DOMAIN",
    "PREPARED_OBJECT_SCHEMA",
    "PREPARATION_PROTOCOL",
    "PREPARE_SOURCE_REQUEST_SCHEMA",
    "PREPARE_SOURCE_RESULT_SCHEMA",
    "PYTHON_LANGUAGE",
    "PrepareSourceContractError",
    "PreparationCancelled",
    "SOURCE_HASH_DOMAIN",
    "SEMANTIC_PORTS_SCHEMA",
    "SOURCE_MAP_HASH_DOMAIN",
    "SOURCE_MAP_SCHEMA",
    "canonical_json_bytes",
    "domain_hash",
    "prepare_source",
    "prepared_manifest",
    "validate_prepared_manifest_and_ir",
    "validate_prepared_object",
    "validate_static_function_ast",
]
