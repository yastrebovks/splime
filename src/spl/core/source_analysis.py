"""Deterministic notebook-source analysis and active-kernel evidence.

The functions in this module are client-neutral framework contracts.  They do
not execute notebook source and they do not import modules named by that
source.  ``analyze_selected_code`` turns an explicitly bound selection into a
single statically representable Function draft.  ``inspect_active_kernel_environment``
resolves the draft's import roots from distribution metadata in the *calling*
Python interpreter, which is the active kernel when invoked by a kernel probe.

Both APIs accept and return closed JSON-compatible documents.  Source analysis
is deliberately conservative: an unsupported construct yields a stable
diagnostic instead of a plausible-but-unfaithful draft.
"""

from __future__ import annotations

import ast
import builtins
from copy import deepcopy
import hashlib
import json
import keyword
import platform
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any, Literal, cast

from spl.core.source_preparation import PrepareSourceContractError, validate_static_function_ast

SELECTED_CODE_ANALYSIS_REQUEST_SCHEMA = "splime.selected-code-analysis-request/v1"
SELECTED_CODE_ANALYSIS_RESULT_SCHEMA = "splime.selected-code-analysis-result/v1"
ACTIVE_KERNEL_ENVIRONMENT_REQUEST_SCHEMA = "splime.active-kernel-environment-request/v1"
ACTIVE_KERNEL_ENVIRONMENT_SCHEMA = "splime.active-kernel-environment/v1"
SOURCE_ANALYSIS_SCHEMA_VERSION = 1

MAX_SOURCE_RECORDS = 64
MAX_SOURCE_BYTES = 512 * 1024
MAX_TOTAL_SOURCE_BYTES = 2 * 1024 * 1024
MAX_CONTEXT_BYTES = 2 * 1024 * 1024
MAX_IMPORT_ROOTS = 256
MAX_IDENTIFIER_CHARS = 128
MAX_ENVIRONMENT_DISTRIBUTIONS = 4096
MAX_DISTRIBUTIONS_PER_IMPORT_ROOT = 16
MAX_TOTAL_DISTRIBUTION_CANDIDATES = 512
MAX_DISTRIBUTION_NAME_CHARS = 160
MAX_DISTRIBUTION_VERSION_CHARS = 160
MAX_DIRECT_URL_BYTES = 16 * 1024
MAX_EXECUTABLE_CHARS = 4096

_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SECRET_NAME_PATTERN = re.compile(
    r"(?:^|_)(?:access_?token|api_?key|authorization|credential|password|private_?key|provider_?key|secret|token)(?:$|_)",
    re.IGNORECASE,
)
_SECRET_LITERAL_PATTERN = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~+/-]{12,}|\bsk-[A-Za-z0-9_-]{12,})"
)
_PYTHON_CELL_MAGICS = frozenset({"capture", "time", "timeit"})
_PYTHON_LINE_MAGICS = frozenset({"capture", "matplotlib", "time", "timeit"})
_KNOWN_IMPORT_DISTRIBUTIONS = {"sklearn": "scikit-learn"}
_DYNAMIC_CALL_NAMES = frozenset({"__import__", "compile", "eval", "exec", "globals", "locals", "vars"})
_SIDE_EFFECT_CALL_NAMES = frozenset({"breakpoint", "exit", "quit"})
_SIDE_EFFECT_CALL_PATHS = frozenset(
    {
        "os.chdir",
        "os.kill",
        "os.remove",
        "os.rename",
        "os.replace",
        "os.system",
        "os.unlink",
        "pathlib.Path.rmdir",
        "pathlib.Path.unlink",
        "shutil.rmtree",
        "socket.create_connection",
        "subprocess.call",
        "subprocess.check_call",
        "subprocess.check_output",
        "subprocess.Popen",
        "subprocess.run",
        "urllib.request.urlopen",
    }
)

_ANALYSIS_REQUEST_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "document",
        "selection",
        "function_name",
        "supporting_import_sources",
        "downstream_sources",
    }
)
_DOCUMENT_KEYS = frozenset({"id", "revision"})
_SELECTION_KEYS = frozenset({"kind", "sources"})
_SOURCE_KEYS = frozenset({"cell_id", "source"})
_ENVIRONMENT_REQUEST_KEYS = frozenset({"schema", "schema_version", "analysis_binding", "kernel", "import_roots"})
_ANALYSIS_BINDING_KEYS = frozenset(
    {
        "analysis_hash",
        "selected_source_hash",
        "generated_source_hash",
        "document_id",
        "document_revision",
    }
)
_KERNEL_KEYS = frozenset({"id", "name"})

AnalysisStatus = Literal["ready", "invalid"]
DiagnosticSeverity = Literal["error", "warning", "info"]


class SourceAnalysisContractError(ValueError):
    """A malformed closed source-analysis or environment request."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class _EnvironmentEvidenceError(RuntimeError):
    """A bounded metadata inspection could not produce complete evidence."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def analyze_selected_code(request: Mapping[str, Any]) -> dict[str, Any]:
    """Analyze an explicitly bound selection without executing it.

    ``downstream_sources`` are optional, explicitly supplied static context.
    They are parsed only to prove which produced name leaves the selection;
    their text is never copied into the Function draft.  Earlier bounded
    context is supplied as ``supporting_import_sources``.  Used imports are
    copied, while exact sibling Functions and safe static bindings may be
    folded into a preserved Function dependency closure.  No context is
    executed and unresolved runtime values become explicit inputs.
    """

    normalized = _validate_analysis_request(request)
    diagnostics: list[dict[str, Any]] = []
    normalized_sources: list[dict[str, str]] = []
    for index, source in enumerate(normalized["selection"]["sources"]):
        text = _normalize_jupyter_source(source["source"], index, diagnostics)
        normalized_sources.append({"cell_id": source["cell_id"], "source": text})

    selected_source_hash = _domain_hash(
        "splime.selected-source/v1",
        [{"cell_id": item["cell_id"], "source": item["source"]} for item in normalized["selection"]["sources"]],
    )
    binding: dict[str, Any] = {
        "document_id": normalized["document"]["id"],
        "document_revision": normalized["document"]["revision"],
        "selection_kind": normalized["selection"]["kind"],
        "selected_cell_ids": [item["cell_id"] for item in normalized["selection"]["sources"]],
        "analysis_input_hash": _domain_hash("splime.selected-code-analysis-input/v1", normalized),
        "selected_source_hash": selected_source_hash,
        "generated_source_hash": None,
    }
    if diagnostics:
        return _analysis_result("invalid", binding, None, diagnostics)

    selected_text = _join_sources(normalized_sources)
    try:
        selected_tree = ast.parse(selected_text, filename="<selected-notebook-code>", mode="exec")
    except SyntaxError as error:
        diagnostics.append(
            _diagnostic(
                "analysis.syntax_error",
                "error",
                line=error.lineno,
                column=error.offset,
            )
        )
        return _analysis_result("invalid", binding, None, diagnostics)

    _inspect_forbidden_constructs(selected_tree, diagnostics)
    if diagnostics:
        return _analysis_result("invalid", binding, None, diagnostics)

    module_imports = [
        statement for statement in selected_tree.body if isinstance(statement, (ast.Import, ast.ImportFrom))
    ]
    functions = [statement for statement in selected_tree.body if isinstance(statement, ast.FunctionDef)]
    non_prelude = [
        statement
        for index, statement in enumerate(selected_tree.body)
        if not isinstance(statement, (ast.Import, ast.ImportFrom)) and not (index == 0 and _is_docstring(statement))
    ]

    if len(functions) == 1 and non_prelude == functions:
        function = functions[0]
        generated = _preserve_function(
            selected_text,
            function,
            module_imports,
            normalized["supporting_import_sources"],
            diagnostics,
        )
        transformation = cast(str, generated.get("transformation")) if generated is not None else None
    elif any(isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) for statement in non_prelude):
        diagnostics.append(_diagnostic("analysis.mixed_definition_unsupported", "error"))
        generated = None
        transformation = None
    else:
        supporting_imports = _parse_supporting_imports(
            normalized["supporting_import_sources"],
            selected_tree,
            diagnostics,
        )
        generated = _wrap_imperative_selection(
            selected_tree,
            normalized["function_name"],
            supporting_imports,
            normalized["downstream_sources"],
            diagnostics,
        )
        transformation = "wrapped_imperative"

    if generated is None or diagnostics:
        return _analysis_result("invalid", binding, None, diagnostics)

    source = cast(str, generated["source"])
    generated_source_hash = _domain_hash("splime.generated-function-source/v1", source)
    binding["generated_source_hash"] = generated_source_hash
    function_document = {
        "name": generated["name"],
        "source": source,
        "inputs": generated["inputs"],
        "outputs": generated["outputs"],
        "imports": generated["imports"],
        "import_roots": generated["import_roots"],
        "transformation": transformation,
        "preparation_compatible": True,
    }
    return _analysis_result("ready", binding, function_document, [])


def inspect_active_kernel_environment(request: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve import roots from metadata in the calling Python interpreter.

    This function intentionally uses only ``sys`` and ``importlib.metadata``
    evidence plus bounded exact-root path existence checks for unpackaged
    modules.  It never imports an analyzed module and performs no network I/O.
    """

    normalized = _validate_environment_request(request)
    observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    executable = sys.executable
    python_version = platform.python_version()
    binding = dict(normalized["analysis_binding"])
    kernel = {
        "id": normalized["kernel"]["id"],
        "name": normalized["kernel"]["name"],
        "python_version": python_version,
        "executable": executable,
    }

    if not executable or len(executable) > MAX_EXECUTABLE_CHARS or not Path(executable).is_file():
        return _environment_result(
            status="unverified",
            binding=binding,
            kernel=kernel,
            observed_at=observed_at,
            inventory_hash=None,
            dependencies=[],
            excluded_imports=[],
            diagnostics=[_diagnostic("environment.interpreter_unverified", "error")],
        )

    try:
        package_mapping = _bounded_package_mapping(
            metadata.packages_distributions(),
            normalized["import_roots"],
        )
        inventory = _distribution_inventory()
    except _EnvironmentEvidenceError as error:
        return _environment_result(
            status="unverified",
            binding=binding,
            kernel=kernel,
            observed_at=observed_at,
            inventory_hash=None,
            dependencies=[],
            excluded_imports=[],
            diagnostics=[_diagnostic(error.code, "error")],
        )
    except (OSError, ValueError, metadata.PackageNotFoundError):
        return _environment_result(
            status="unverified",
            binding=binding,
            kernel=kernel,
            observed_at=observed_at,
            inventory_hash=None,
            dependencies=[],
            excluded_imports=[],
            diagnostics=[_diagnostic("environment.metadata_unavailable", "error")],
        )

    inventory_hash = _domain_hash(
        "splime.active-environment-inventory/v1",
        {"python_version": python_version, "distributions": inventory},
    )
    stdlib = set(getattr(sys, "stdlib_module_names", frozenset())) | set(sys.builtin_module_names)
    dependencies: list[dict[str, Any]] = []
    excluded: list[dict[str, str]] = []
    diagnostics: list[dict[str, Any]] = []
    try:
        for root in normalized["import_roots"]:
            if root in stdlib:
                excluded.append({"import_root": root, "reason": "standard_library"})
                continue
            if root == "spl":
                excluded.append({"import_root": root, "reason": "framework_runtime"})
                continue
            record, record_diagnostics = _resolve_import_root(root, package_mapping)
            dependencies.append(record)
            diagnostics.extend(record_diagnostics)
    except _EnvironmentEvidenceError as error:
        return _environment_result(
            status="unverified",
            binding=binding,
            kernel=kernel,
            observed_at=observed_at,
            inventory_hash=inventory_hash,
            dependencies=[],
            excluded_imports=[],
            diagnostics=[_diagnostic(error.code, "error")],
        )

    dependencies.sort(key=lambda item: item["import_root"])
    excluded.sort(key=lambda item: item["import_root"])
    status: Literal["verified", "unverified"] = (
        "verified" if not any(item["severity"] == "error" for item in diagnostics) else "unverified"
    )
    return _environment_result(
        status=status,
        binding=binding,
        kernel=kernel,
        observed_at=observed_at,
        inventory_hash=inventory_hash,
        dependencies=dependencies,
        excluded_imports=excluded,
        diagnostics=diagnostics,
    )


def _preserve_function(
    selected_text: str,
    function: ast.FunctionDef,
    module_imports: Sequence[ast.Import | ast.ImportFrom],
    supporting_sources: Sequence[Mapping[str, str]],
    diagnostics: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if function.decorator_list:
        diagnostics.append(_diagnostic("analysis.decorator_unsupported", "error", line=function.lineno))
    if function.args.posonlyargs or function.args.kwonlyargs or function.args.vararg or function.args.kwarg:
        diagnostics.append(_diagnostic("analysis.parameter_kind_unsupported", "error", line=function.lineno))
    if any(
        isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)) and item is not function
        for item in _walk(function)
    ):
        diagnostics.append(_diagnostic("analysis.closure_unsupported", "error", line=function.lineno))
    all_supporting_imports = _all_supporting_imports(supporting_sources)
    imported_bindings = _import_bindings([*all_supporting_imports, *module_imports])
    hidden = _function_hidden_globals(function, imported_bindings)
    generated_function = function
    transformation = "preserved_function"
    if hidden and not diagnostics:
        generated_function = _synthesize_static_dependencies(
            function,
            supporting_sources,
            imported_bindings,
            diagnostics,
        )

    generated_tree = ast.Module(body=[generated_function], type_ignores=[])
    supporting_imports = _parse_supporting_imports(supporting_sources, generated_tree, diagnostics)
    imports = [*supporting_imports, *module_imports]
    try:
        validate_static_function_ast(generated_function)
    except PrepareSourceContractError as error:
        diagnostics.append(_diagnostic(error.code, "error", line=function.lineno))
    _validate_json_defaults(generated_function, diagnostics)
    returns = _outer_value_returns(generated_function)
    if not returns:
        diagnostics.append(_diagnostic("analysis.output_unproven", "error", line=function.lineno))
    if diagnostics:
        return None

    if generated_function is function:
        function_segment = ast.get_source_segment(selected_text, function)
        if function_segment is None:
            diagnostics.append(_diagnostic("analysis.source_span_unavailable", "error"))
            return None
    else:
        function_segment = ast.unparse(ast.fix_missing_locations(generated_function))
    import_text = _deduplicated_import_text(imports)
    source = "\n\n".join([*import_text, function_segment.rstrip()]) + "\n"
    try:
        parsed = ast.parse(source, filename="<generated-function>", mode="exec")
        generated_function = next(item for item in parsed.body if isinstance(item, ast.FunctionDef))
        validate_static_function_ast(generated_function)
    except (SyntaxError, PrepareSourceContractError, StopIteration):
        diagnostics.append(_diagnostic("analysis.generated_source_incompatible", "error"))
        return None
    default_count = len(generated_function.args.defaults)
    required_count = len(generated_function.args.args) - default_count
    inputs = [
        {
            "name": argument.arg,
            "annotation": ast.unparse(argument.annotation) if argument.annotation else None,
            "required": index < required_count,
            "has_default": index >= required_count,
        }
        for index, argument in enumerate(generated_function.args.args)
    ]
    output_annotation = ast.unparse(generated_function.returns) if generated_function.returns else None
    import_records = _import_records(parsed)
    return {
        "name": function.name,
        "source": source,
        "inputs": inputs,
        "outputs": [{"name": "default", "annotation": output_annotation}],
        "imports": import_records,
        "import_roots": sorted({item["module"].partition(".")[0] for item in import_records}),
        "transformation": transformation,
    }


def _synthesize_static_dependencies(
    function: ast.FunctionDef,
    supporting_sources: Sequence[Mapping[str, str]],
    imported_bindings: set[str],
    diagnostics: list[dict[str, Any]],
) -> ast.FunctionDef:
    """Close over deterministic source context without inspecting the kernel.

    The classic live Function serializer follows values through
    ``function.__globals__``.  A browser-originated draft has no trustworthy
    live function object, but it does carry bounded earlier notebook source.
    This routine mirrors the useful part of that traversal for facts which are
    exact in source: sibling top-level Functions, JSON literals, and imported
    constructor calls whose arguments are JSON literals.  Anything else is an
    explicit Function input rather than an invented captured value.
    """

    bindings = _supporting_static_bindings(supporting_sources)
    generated = deepcopy(function)
    static_values: dict[str, ast.stmt] = {}
    helpers: dict[str, ast.FunctionDef] = {}
    external_inputs: list[str] = []
    resolving: set[str] = set()

    def add_external(name: str) -> None:
        existing = {argument.arg for argument in generated.args.args}
        if name not in existing and name not in external_inputs:
            external_inputs.append(name)

    def resolve(name: str) -> None:
        if name in imported_bindings or name in dir(builtins) or name == function.name:
            return
        if name in static_values or name in helpers or name in external_inputs:
            return
        statement = bindings.get(name)
        if statement is None:
            add_external(name)
            return
        if name in resolving:
            diagnostics.append(_diagnostic("analysis.dependency_cycle_unsupported", "error", subject=name))
            return
        resolving.add(name)
        try:
            if isinstance(statement, ast.FunctionDef):
                _validate_supporting_function(statement, diagnostics)
                if diagnostics:
                    return
                for dependency in _function_hidden_globals(statement, imported_bindings):
                    if dependency != statement.name:
                        resolve(dependency)
                helpers[name] = deepcopy(statement)
                return
            if _is_safe_static_binding(statement, imported_bindings):
                _inspect_forbidden_constructs(statement, diagnostics)
                if not diagnostics:
                    static_values[name] = cast(ast.stmt, deepcopy(statement))
                return
            add_external(name)
        finally:
            resolving.discard(name)

    for hidden in _function_hidden_globals(function, imported_bindings):
        resolve(hidden)
    if diagnostics:
        return generated

    _insert_required_function_arguments(generated, external_inputs)
    prefix: list[ast.stmt] = [*static_values.values(), *helpers.values()]
    if prefix:
        insert_at = 1 if generated.body and _is_docstring(generated.body[0]) else 0
        generated.body[insert_at:insert_at] = prefix
    ast.fix_missing_locations(generated)
    return generated


def _validate_supporting_function(
    function: ast.FunctionDef,
    diagnostics: list[dict[str, Any]],
) -> None:
    if function.decorator_list:
        diagnostics.append(_diagnostic("analysis.decorator_unsupported", "error", line=function.lineno))
    if function.args.posonlyargs or function.args.kwonlyargs or function.args.vararg or function.args.kwarg:
        diagnostics.append(_diagnostic("analysis.parameter_kind_unsupported", "error", line=function.lineno))
    if any(
        isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)) and item is not function
        for item in _walk(function)
    ):
        diagnostics.append(_diagnostic("analysis.closure_unsupported", "error", line=function.lineno))
    _inspect_forbidden_constructs(function, diagnostics)
    _validate_json_defaults(function, diagnostics)
    try:
        validate_static_function_ast(function)
    except PrepareSourceContractError as error:
        diagnostics.append(_diagnostic(error.code, "error", line=function.lineno))


def _insert_required_function_arguments(function: ast.FunctionDef, names: Sequence[str]) -> None:
    if not names:
        return
    default_count = len(function.args.defaults)
    required_count = len(function.args.args) - default_count
    additions = [ast.arg(arg=name, annotation=None) for name in names]
    function.args.args[required_count:required_count] = additions


def _supporting_static_bindings(
    sources: Sequence[Mapping[str, str]],
) -> dict[str, ast.FunctionDef | ast.Assign | ast.AnnAssign]:
    bindings: dict[str, ast.FunctionDef | ast.Assign | ast.AnnAssign] = {}
    for tree in _supporting_trees(sources):
        for statement in tree.body:
            if isinstance(statement, ast.FunctionDef):
                bindings[statement.name] = statement
            elif (
                isinstance(statement, ast.Assign)
                and len(statement.targets) == 1
                and isinstance(statement.targets[0], ast.Name)
            ):
                bindings[statement.targets[0].id] = statement
            elif isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
                bindings[statement.target.id] = statement
    return bindings


def _all_supporting_imports(
    sources: Sequence[Mapping[str, str]],
) -> list[ast.Import | ast.ImportFrom]:
    return [
        statement
        for tree in _supporting_trees(sources)
        for statement in tree.body
        if isinstance(statement, (ast.Import, ast.ImportFrom))
        and not (
            isinstance(statement, ast.ImportFrom)
            and (
                statement.level != 0 or statement.module is None or any(alias.name == "*" for alias in statement.names)
            )
        )
    ]


def _supporting_trees(sources: Sequence[Mapping[str, str]]) -> list[ast.Module]:
    trees: list[ast.Module] = []
    for source_index, source in enumerate(sources):
        local_diagnostics: list[dict[str, Any]] = []
        normalized = _normalize_jupyter_source(source["source"], source_index, local_diagnostics)
        if local_diagnostics:
            continue
        try:
            trees.append(ast.parse(normalized, filename="<supporting-source>", mode="exec"))
        except SyntaxError:
            continue
    return trees


def _is_safe_static_binding(statement: ast.stmt, imported_bindings: set[str]) -> bool:
    value: ast.expr | None
    if isinstance(statement, ast.Assign):
        value = statement.value
    elif isinstance(statement, ast.AnnAssign):
        value = statement.value
    else:
        return False
    if value is None:
        return False
    if _is_json_literal_ast(value):
        return True
    if not isinstance(value, ast.Call) or any(keyword.arg is None for keyword in value.keywords):
        return False
    path = _call_path(value.func)
    if path is None:
        return False
    root = path.partition(".")[0]
    return (
        root in imported_bindings
        and all(_is_json_literal_ast(argument) for argument in value.args)
        and all(_is_json_literal_ast(keyword.value) for keyword in value.keywords)
    )


def _outer_value_returns(function: ast.FunctionDef) -> list[ast.Return]:
    returns: list[ast.Return] = []

    class _ReturnVisitor(ast.NodeVisitor):
        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            if node is function:
                for statement in node.body:
                    self.visit(statement)

        def visit_AsyncFunctionDef(self, _node: ast.AsyncFunctionDef) -> None:
            return

        def visit_Lambda(self, _node: ast.Lambda) -> None:
            return

        def visit_Return(self, node: ast.Return) -> None:
            if node.value is not None:
                returns.append(node)

    _ReturnVisitor().visit(function)
    return returns


def _wrap_imperative_selection(
    tree: ast.Module,
    function_name: str,
    supporting_imports: Sequence[ast.Import | ast.ImportFrom],
    downstream_sources: Sequence[Mapping[str, str]],
    diagnostics: list[dict[str, Any]],
) -> dict[str, Any] | None:
    body = [item for index, item in enumerate(tree.body) if not (index == 0 and _is_docstring(item))]
    if not body:
        diagnostics.append(_diagnostic("analysis.selection_empty", "error"))
        return None
    imported_bindings = _import_bindings(
        [*supporting_imports, *[x for x in body if isinstance(x, (ast.Import, ast.ImportFrom))]]
    )
    inputs = _imperative_inputs(body, imported_bindings)
    assigned = _assigned_names(body)
    output = _select_output(body, assigned, downstream_sources, diagnostics)
    if diagnostics or output is None:
        return None

    body_copy = [ast.fix_missing_locations(ast.parse(ast.unparse(item), mode="exec").body[0]) for item in body]
    if (
        isinstance(body_copy[-1], ast.Expr)
        and isinstance(body_copy[-1].value, ast.Name)
        and body_copy[-1].value.id == output
    ):
        body_copy[-1] = ast.Return(value=ast.Name(id=output, ctx=ast.Load()))
    else:
        body_copy.append(ast.Return(value=ast.Name(id=output, ctx=ast.Load())))
    arguments = ast.arguments(
        posonlyargs=[],
        args=[ast.arg(arg=name, annotation=None) for name in inputs],
        vararg=None,
        kwonlyargs=[],
        kw_defaults=[],
        kwarg=None,
        defaults=[],
    )
    function = ast.FunctionDef(
        name=function_name,
        args=arguments,
        body=body_copy,
        decorator_list=[],
        returns=None,
        type_comment=None,
    )
    module = ast.Module(body=[*supporting_imports, function], type_ignores=[])
    ast.fix_missing_locations(module)
    try:
        validate_static_function_ast(function)
        source = ast.unparse(module).rstrip() + "\n"
        parsed = ast.parse(source, filename="<generated-function>", mode="exec")
        generated_function = next(item for item in parsed.body if isinstance(item, ast.FunctionDef))
        validate_static_function_ast(generated_function)
    except (PrepareSourceContractError, SyntaxError, StopIteration) as error:
        code = error.code if isinstance(error, PrepareSourceContractError) else "analysis.generated_source_incompatible"
        diagnostics.append(_diagnostic(code, "error"))
        return None
    import_records = _import_records(parsed)
    return {
        "name": function_name,
        "source": source,
        "inputs": [
            {
                "name": name,
                "annotation": None,
                "required": True,
                "has_default": False,
            }
            for name in inputs
        ],
        "outputs": [{"name": "default", "annotation": None}],
        "imports": import_records,
        "import_roots": sorted({item["module"].partition(".")[0] for item in import_records}),
    }


def _validate_analysis_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = _mapping(request, "analysis.request")
    _exact_keys(value, _ANALYSIS_REQUEST_KEYS, "analysis.request")
    if value["schema"] != SELECTED_CODE_ANALYSIS_REQUEST_SCHEMA or value["schema_version"] != 1:
        raise SourceAnalysisContractError("analysis.schema")
    document = _mapping(value["document"], "analysis.document")
    _exact_keys(document, _DOCUMENT_KEYS, "analysis.document")
    document_id = _string(document["id"], "analysis.document.id", 256)
    revision = _string(document["revision"], "analysis.document.revision", 256)
    selection = _mapping(value["selection"], "analysis.selection")
    _exact_keys(selection, _SELECTION_KEYS, "analysis.selection")
    kind = selection["kind"]
    if kind not in {"fragment", "cell", "cells"}:
        raise SourceAnalysisContractError("analysis.selection.kind")
    sources = _source_records(selection["sources"], "analysis.selection.sources", MAX_TOTAL_SOURCE_BYTES)
    if kind in {"fragment", "cell"} and len(sources) != 1:
        raise SourceAnalysisContractError("analysis.selection.cardinality")
    function_name = _string(value["function_name"], "analysis.function_name", MAX_IDENTIFIER_CHARS)
    if not _IDENTIFIER_PATTERN.fullmatch(function_name) or keyword.iskeyword(function_name):
        raise SourceAnalysisContractError("analysis.function_name")
    supporting = _source_records(
        value["supporting_import_sources"],
        "analysis.supporting_import_sources",
        MAX_SOURCE_BYTES,
        allow_empty=True,
    )
    downstream = _source_records(
        value["downstream_sources"],
        "analysis.downstream_sources",
        MAX_CONTEXT_BYTES,
        allow_empty=True,
    )
    return {
        "schema": SELECTED_CODE_ANALYSIS_REQUEST_SCHEMA,
        "schema_version": 1,
        "document": {"id": document_id, "revision": revision},
        "selection": {"kind": kind, "sources": sources},
        "function_name": function_name,
        "supporting_import_sources": supporting,
        "downstream_sources": downstream,
    }


def _validate_environment_request(request: Mapping[str, Any]) -> dict[str, Any]:
    value = _mapping(request, "environment.request")
    _exact_keys(value, _ENVIRONMENT_REQUEST_KEYS, "environment.request")
    if value["schema"] != ACTIVE_KERNEL_ENVIRONMENT_REQUEST_SCHEMA or value["schema_version"] != 1:
        raise SourceAnalysisContractError("environment.schema")
    binding = _mapping(value["analysis_binding"], "environment.analysis_binding")
    _exact_keys(binding, _ANALYSIS_BINDING_KEYS, "environment.analysis_binding")
    normalized_binding: dict[str, str] = {}
    for key in ("analysis_hash", "selected_source_hash", "generated_source_hash"):
        item = _string(binding[key], f"environment.analysis_binding.{key}", 80)
        if not _HASH_PATTERN.fullmatch(item):
            raise SourceAnalysisContractError(f"environment.analysis_binding.{key}")
        normalized_binding[key] = item
    normalized_binding["document_id"] = _string(binding["document_id"], "environment.analysis_binding.document_id", 256)
    normalized_binding["document_revision"] = _string(
        binding["document_revision"], "environment.analysis_binding.document_revision", 256
    )
    kernel = _mapping(value["kernel"], "environment.kernel")
    _exact_keys(kernel, _KERNEL_KEYS, "environment.kernel")
    normalized_kernel = {
        "id": _string(kernel["id"], "environment.kernel.id", 256),
        "name": _string(kernel["name"], "environment.kernel.name", 256),
    }
    roots = value["import_roots"]
    if not isinstance(roots, Sequence) or isinstance(roots, (str, bytes)) or len(roots) > MAX_IMPORT_ROOTS:
        raise SourceAnalysisContractError("environment.import_roots")
    normalized_roots: list[str] = []
    for index, raw in enumerate(roots):
        root = _string(raw, f"environment.import_roots.{index}", MAX_IDENTIFIER_CHARS)
        if not _IDENTIFIER_PATTERN.fullmatch(root) or keyword.iskeyword(root):
            raise SourceAnalysisContractError("environment.import_root")
        if root not in normalized_roots:
            normalized_roots.append(root)
    return {
        "schema": ACTIVE_KERNEL_ENVIRONMENT_REQUEST_SCHEMA,
        "schema_version": 1,
        "analysis_binding": normalized_binding,
        "kernel": normalized_kernel,
        "import_roots": normalized_roots,
    }


def _source_records(raw: Any, path: str, byte_limit: int, *, allow_empty: bool = False) -> list[dict[str, str]]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise SourceAnalysisContractError(path)
    if (not allow_empty and not raw) or len(raw) > MAX_SOURCE_RECORDS:
        raise SourceAnalysisContractError(path)
    result: list[dict[str, str]] = []
    total = 0
    seen: set[str] = set()
    for index, item in enumerate(raw):
        record = _mapping(item, f"{path}.{index}")
        _exact_keys(record, _SOURCE_KEYS, f"{path}.{index}")
        cell_id = _string(record["cell_id"], f"{path}.{index}.cell_id", 256)
        if cell_id in seen:
            raise SourceAnalysisContractError(f"{path}.duplicate_cell_id")
        seen.add(cell_id)
        source = record["source"]
        if not isinstance(source, str) or not source or "\x00" in source:
            raise SourceAnalysisContractError(f"{path}.{index}.source")
        size = len(source.encode("utf-8"))
        if size > MAX_SOURCE_BYTES:
            raise SourceAnalysisContractError(f"{path}.{index}.source_limit")
        total += size
        if total > byte_limit:
            raise SourceAnalysisContractError(f"{path}.total_limit")
        result.append({"cell_id": cell_id, "source": source})
    return result


def _normalize_jupyter_source(source: str, source_index: int, diagnostics: list[dict[str, Any]]) -> str:
    lines = source.splitlines(keepends=True)
    if not lines:
        return source
    first_body = lines[0].rstrip("\r\n")
    first = first_body.lstrip()
    if first.startswith("%%"):
        command = first[2:].split(maxsplit=1)[0] if first[2:].strip() else ""
        if command not in _PYTHON_CELL_MAGICS:
            diagnostics.append(
                _diagnostic("analysis.unsafe_magic", "error", source_index=source_index, line=1, subject=command)
            )
            return source
        lines[0] = lines[0][len(first_body) :]
    normalized: list[str] = []
    for line_number, line in enumerate(lines, 1):
        body = line.rstrip("\r\n")
        ending = line[len(body) :]
        stripped = body.lstrip()
        indentation = body[: len(body) - len(stripped)]
        if stripped.startswith("!") or stripped.startswith("?"):
            diagnostics.append(
                _diagnostic("analysis.unsafe_magic", "error", source_index=source_index, line=line_number)
            )
            normalized.append(line)
            continue
        if stripped.startswith("%") and not stripped.startswith("%%"):
            command_body = stripped[1:]
            command, _, argument = command_body.partition(" ")
            if command not in _PYTHON_LINE_MAGICS:
                diagnostics.append(
                    _diagnostic(
                        "analysis.unsafe_magic",
                        "error",
                        source_index=source_index,
                        line=line_number,
                        subject=command,
                    )
                )
            elif command in {"time", "timeit"} and argument.strip():
                line = f"{indentation}{argument}{ending}"
            else:
                line = f"{indentation}pass{ending}"
        normalized.append(line)
    return "".join(normalized)


def _parse_supporting_imports(
    sources: Sequence[Mapping[str, str]],
    selected_tree: ast.Module,
    diagnostics: list[dict[str, Any]],
) -> list[ast.Import | ast.ImportFrom]:
    """Extract only imports whose bound names are used by the selection.

    Browser clients may provide bounded earlier notebook code cells as static
    context.  This helper projects only imports used by the supplied tree;
    static Functions and values are handled separately by the dependency
    closure.  Partially invalid context is ignored, so it cannot invent a safe
    import binding.
    """

    selected_loads = {
        item.id for item in _walk(selected_tree) if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Load)
    }
    imports: list[ast.Import | ast.ImportFrom] = []
    for source_index, source in enumerate(sources):
        local_diagnostics: list[dict[str, Any]] = []
        normalized = _normalize_jupyter_source(source["source"], source_index, local_diagnostics)
        if local_diagnostics:
            continue
        try:
            tree = ast.parse(normalized, filename="<supporting-import-source>", mode="exec")
        except SyntaxError:
            continue
        for statement in tree.body:
            if isinstance(statement, ast.Import):
                selected_aliases = [
                    alias
                    for alias in statement.names
                    if (alias.asname or alias.name.partition(".")[0]) in selected_loads
                ]
                if selected_aliases:
                    imports.append(ast.Import(names=selected_aliases))
                continue
            if not isinstance(statement, ast.ImportFrom):
                continue
            if statement.level != 0 or statement.module is None:
                continue
            selected_aliases = [
                alias
                for alias in statement.names
                if alias.name != "*" and (alias.asname or alias.name) in selected_loads
            ]
            if selected_aliases:
                imports.append(
                    ast.ImportFrom(
                        module=statement.module,
                        names=selected_aliases,
                        level=0,
                    )
                )
    bindings: dict[str, tuple[str, str, str | None]] = {}
    for item in imports:
        for binding, semantic in _import_binding_semantics(item):
            previous = bindings.setdefault(binding, semantic)
            if previous != semantic:
                diagnostics.append(_diagnostic("analysis.import_binding_conflict", "error", subject=binding))
    return imports


def _inspect_forbidden_constructs(node: ast.AST, diagnostics: list[dict[str, Any]]) -> None:
    codes: set[tuple[str, int | None]] = set()
    for item in _walk(node):
        code: str | None = None
        if isinstance(item, ast.Constant) and isinstance(item.value, str):
            if _SECRET_LITERAL_PATTERN.search(item.value):
                code = "source.secret_like_literal"
        elif isinstance(item, (ast.Assign, ast.AnnAssign)):
            targets = item.targets if isinstance(item, ast.Assign) else [item.target]
            assigned = item.value
            if (
                isinstance(assigned, ast.Constant)
                and isinstance(assigned.value, str)
                and assigned.value
                and any(isinstance(target, ast.Name) and _SECRET_NAME_PATTERN.search(target.id) for target in targets)
            ):
                code = "source.secret_like_literal"
        if code is not None:
            key = (code, getattr(item, "lineno", None))
            if key not in codes:
                codes.add(key)
                diagnostics.append(_diagnostic(code, "error", line=key[1]))
            continue
        if isinstance(item, (ast.Global, ast.Nonlocal, ast.Yield, ast.YieldFrom, ast.Await)):
            code = "analysis.dynamic_scope_unsupported"
        elif isinstance(item, (ast.AsyncFunctionDef, ast.AsyncFor, ast.AsyncWith)):
            code = "analysis.async_unsupported"
        elif isinstance(item, ast.ImportFrom) and (
            item.level != 0 or item.module is None or any(alias.name == "*" for alias in item.names)
        ):
            code = "analysis.static_import_unsupported"
        elif isinstance(item, ast.Call):
            path = _call_path(item.func)
            if path in _DYNAMIC_CALL_NAMES or path in {"importlib.import_module", "get_ipython"}:
                code = "analysis.dynamic_import_or_execution"
            elif path in _SIDE_EFFECT_CALL_NAMES or path in _SIDE_EFFECT_CALL_PATHS:
                code = "analysis.side_effect_unsupported"
        if code is not None:
            key = (code, getattr(item, "lineno", None))
            if key not in codes:
                codes.add(key)
                diagnostics.append(_diagnostic(code, "error", line=key[1]))


def _function_hidden_globals(function: ast.FunctionDef, imported_bindings: set[str]) -> list[str]:
    parameters = {item.arg for item in function.args.args}
    assigned = _assigned_names(function.body)
    local_imports = _import_bindings(
        [item for item in _walk(function) if isinstance(item, (ast.Import, ast.ImportFrom))]
    )
    loaded = {
        item.id
        for statement in function.body
        for item in _walk(statement)
        if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Load)
    }
    allowed = parameters | assigned | imported_bindings | local_imports | set(dir(builtins)) | {function.name}
    return sorted(loaded - allowed)


def _import_binding_semantics(
    statement: ast.Import | ast.ImportFrom,
) -> list[tuple[str, tuple[str, str, str | None]]]:
    if isinstance(statement, ast.Import):
        return [
            (
                alias.asname or alias.name.partition(".")[0],
                ("module", alias.name, alias.asname),
            )
            for alias in statement.names
        ]
    if statement.level != 0 or statement.module is None:
        return []
    return [
        (
            alias.asname or alias.name,
            (statement.module, alias.name, alias.asname),
        )
        for alias in statement.names
        if alias.name != "*"
    ]


def _validate_json_defaults(function: ast.FunctionDef, diagnostics: list[dict[str, Any]]) -> None:
    for default in function.args.defaults:
        if not _is_json_literal_ast(default):
            diagnostics.append(
                _diagnostic("analysis.default_unsupported", "error", line=getattr(default, "lineno", None))
            )


def _is_json_literal_ast(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant):
        return node.value is None or type(node.value) in {bool, int, float, str}
    if isinstance(node, (ast.List, ast.Tuple)):
        return all(_is_json_literal_ast(item) for item in node.elts)
    if isinstance(node, ast.Dict):
        return all(
            key is not None
            and isinstance(key, ast.Constant)
            and isinstance(key.value, str)
            and _is_json_literal_ast(value)
            for key, value in zip(node.keys, node.values, strict=True)
        )
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        return isinstance(node.operand, ast.Constant) and type(node.operand.value) in {int, float}
    return False


def _imperative_inputs(body: Sequence[ast.stmt], imported_bindings: set[str]) -> list[str]:
    assigned_positions: dict[str, tuple[int, int]] = {}
    load_positions: dict[str, tuple[int, int]] = {}
    self_updated: set[str] = set()
    for statement in body:
        if isinstance(statement, (ast.Assign, ast.AnnAssign)):
            targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
            value = statement.value
            target_names = {item.id for target in targets for item in _walk(target) if isinstance(item, ast.Name)}
            value_loads = (
                {item.id for item in _walk(value) if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Load)}
                if value is not None
                else set()
            )
            self_updated.update(target_names & value_loads)
        elif isinstance(statement, ast.AugAssign):
            self_updated.update(item.id for item in _walk(statement.target) if isinstance(item, ast.Name))
        for item in _walk(statement):
            if not isinstance(item, ast.Name):
                continue
            position = (getattr(item, "lineno", 0), getattr(item, "col_offset", 0))
            if isinstance(item.ctx, ast.Load):
                load_positions[item.id] = min(load_positions.get(item.id, position), position)
            elif isinstance(item.ctx, (ast.Store, ast.Del)):
                assigned_positions[item.id] = min(assigned_positions.get(item.id, position), position)
    result = [
        name
        for name, position in load_positions.items()
        if name not in imported_bindings
        and name not in dir(builtins)
        and (name not in assigned_positions or position <= assigned_positions[name] or name in self_updated)
    ]
    return sorted(result, key=lambda name: (load_positions[name], name))


def _assigned_names(statements: Sequence[ast.stmt]) -> set[str]:
    return {
        item.id
        for statement in statements
        for item in _walk(statement)
        if isinstance(item, ast.Name) and isinstance(item.ctx, (ast.Store, ast.Del))
    }


def _select_output(
    body: Sequence[ast.stmt],
    assigned: set[str],
    downstream_sources: Sequence[Mapping[str, str]],
    diagnostics: list[dict[str, Any]],
) -> str | None:
    downstream_loads: set[str] = set()
    for source_index, source in enumerate(downstream_sources):
        local_diagnostics: list[dict[str, Any]] = []
        normalized = _normalize_jupyter_source(source["source"], source_index, local_diagnostics)
        if local_diagnostics:
            continue
        try:
            context_tree = ast.parse(normalized, filename="<downstream-notebook-context>", mode="exec")
        except SyntaxError:
            continue
        downstream_loads.update(
            item.id for item in _walk(context_tree) if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Load)
        )
    candidates = sorted(assigned & downstream_loads)
    if not candidates and body and isinstance(body[-1], ast.Expr) and isinstance(body[-1].value, ast.Name):
        if body[-1].value.id in assigned:
            candidates = [body[-1].value.id]
    if not candidates:
        diagnostics.append(_diagnostic("analysis.output_unproven", "error"))
        return None
    if len(candidates) > 1:
        diagnostics.append(_diagnostic("analysis.multiple_outputs_unsupported", "error", subject=",".join(candidates)))
        return None
    return candidates[0]


def _import_records(tree: ast.AST) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in _walk(tree):
        if isinstance(item, ast.Import):
            for alias in item.names:
                record = {"kind": "module", "module": alias.name, "name": None, "alias": alias.asname}
                key = json.dumps(record, sort_keys=True)
                if key not in seen:
                    seen.add(key)
                    records.append(record)
        elif isinstance(item, ast.ImportFrom) and item.level == 0 and item.module is not None:
            for alias in item.names:
                record = {
                    "kind": "from",
                    "module": item.module,
                    "name": alias.name,
                    "alias": alias.asname,
                }
                key = json.dumps(record, sort_keys=True)
                if key not in seen:
                    seen.add(key)
                    records.append(record)
    records.sort(key=lambda item: (item["module"], item["name"] or "", item["alias"] or ""))
    return records


def _import_bindings(imports: Sequence[ast.Import | ast.ImportFrom]) -> set[str]:
    result: set[str] = set()
    for item in imports:
        if isinstance(item, ast.Import):
            result.update(alias.asname or alias.name.partition(".")[0] for alias in item.names)
        else:
            result.update(alias.asname or alias.name for alias in item.names if alias.name != "*")
    return result


def _deduplicated_import_text(imports: Sequence[ast.Import | ast.ImportFrom]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in imports:
        text = ast.unparse(item)
        if text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _resolve_import_root(
    root: str, package_mapping: Mapping[str, Sequence[str]]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    names = sorted({_distribution_display_name(name) for name in package_mapping.get(root, [])})
    if not names and root in _KNOWN_IMPORT_DISTRIBUTIONS:
        known = _KNOWN_IMPORT_DISTRIBUTIONS[root]
        try:
            metadata.version(known)
        except metadata.PackageNotFoundError:
            pass
        else:
            names = [_distribution_display_name(known)]
    if len(names) > 1:
        candidates = [_distribution_record(name) for name in names]
        return (
            {
                "import_root": root,
                "classification": "ambiguous",
                "distribution": None,
                "candidates": candidates,
            },
            [_diagnostic("dependency.ambiguous_distribution", "error", subject=root)],
        )
    if len(names) == 1:
        distribution = _distribution_record(names[0])
        classification = distribution["origin"]
        if classification == "unresolved":
            return (
                {
                    "import_root": root,
                    "classification": "unresolved",
                    "distribution": None,
                    "candidates": [],
                },
                [_diagnostic("dependency.distribution_metadata_unavailable", "error", subject=root)],
            )
        return (
            {
                "import_root": root,
                "classification": classification,
                "distribution": distribution,
                "candidates": [],
            },
            [],
        )
    if _unpackaged_root_exists(root):
        return (
            {
                "import_root": root,
                "classification": "unpackaged_local",
                "distribution": None,
                "candidates": [],
            },
            [_diagnostic("dependency.unpackaged_local", "error", subject=root)],
        )
    return (
        {"import_root": root, "classification": "unresolved", "distribution": None, "candidates": []},
        [_diagnostic("dependency.unresolved", "error", subject=root)],
    )


def _distribution_record(name: str) -> dict[str, str]:
    try:
        distribution = metadata.distribution(name)
        canonical_name = _bounded_metadata_text(
            distribution.metadata.get("Name") or name,
            maximum=MAX_DISTRIBUTION_NAME_CHARS,
        )
        version = _bounded_metadata_text(
            distribution.version,
            maximum=MAX_DISTRIBUTION_VERSION_CHARS,
        )
        origin = "installed"
        direct_url_text = distribution.read_text("direct_url.json")
        if direct_url_text:
            if not isinstance(direct_url_text, str) or len(direct_url_text.encode("utf-8")) > MAX_DIRECT_URL_BYTES:
                raise _EnvironmentEvidenceError("environment.metadata_bounds_exceeded")
            try:
                direct_url = json.loads(direct_url_text)
            except (TypeError, ValueError):
                direct_url = None
            if isinstance(direct_url, Mapping) and str(direct_url.get("url", "")).startswith("file:"):
                directory_info = direct_url.get("dir_info")
                if isinstance(directory_info, Mapping) and directory_info.get("editable") is True:
                    origin = "editable"
                else:
                    origin = "packaged_local"
        return {"name": canonical_name, "version": version, "origin": origin}
    except (metadata.PackageNotFoundError, OSError, ValueError):
        return {"name": name, "version": "unknown", "origin": "unresolved"}


def _distribution_display_name(name: str) -> str:
    try:
        distribution = metadata.distribution(name)
        return _bounded_metadata_text(
            distribution.metadata.get("Name") or name,
            maximum=MAX_DISTRIBUTION_NAME_CHARS,
        )
    except (metadata.PackageNotFoundError, OSError, ValueError):
        return _bounded_metadata_text(name, maximum=MAX_DISTRIBUTION_NAME_CHARS)


def _distribution_inventory() -> list[dict[str, str]]:
    result: dict[str, str] = {}
    for index, distribution in enumerate(metadata.distributions()):
        if index >= MAX_ENVIRONMENT_DISTRIBUTIONS:
            raise _EnvironmentEvidenceError("environment.metadata_bounds_exceeded")
        name = distribution.metadata.get("Name")
        if not name:
            continue
        bounded_name = _bounded_metadata_text(name, maximum=MAX_DISTRIBUTION_NAME_CHARS)
        bounded_version = _bounded_metadata_text(
            distribution.version,
            maximum=MAX_DISTRIBUTION_VERSION_CHARS,
        )
        result[_canonical_distribution_name(bounded_name)] = bounded_version
    return [{"name": name, "version": version} for name, version in sorted(result.items())]


def _bounded_package_mapping(
    raw: Mapping[str, Sequence[str]],
    requested_roots: Sequence[str],
) -> dict[str, list[str]]:
    if not isinstance(raw, Mapping):
        raise _EnvironmentEvidenceError("environment.metadata_invalid")
    result: dict[str, list[str]] = {}
    total_candidates = 0
    for root in requested_roots:
        candidates = raw.get(root, ())
        if isinstance(candidates, (str, bytes)) or not isinstance(candidates, Sequence):
            raise _EnvironmentEvidenceError("environment.metadata_invalid")
        if len(candidates) > MAX_DISTRIBUTIONS_PER_IMPORT_ROOT:
            raise _EnvironmentEvidenceError("environment.metadata_bounds_exceeded")
        normalized: list[str] = []
        for candidate in candidates:
            bounded = _bounded_metadata_text(candidate, maximum=MAX_DISTRIBUTION_NAME_CHARS)
            if bounded not in normalized:
                normalized.append(bounded)
        total_candidates += len(normalized)
        if total_candidates > MAX_TOTAL_DISTRIBUTION_CANDIDATES:
            raise _EnvironmentEvidenceError("environment.metadata_bounds_exceeded")
        result[root] = normalized
    return result


def _bounded_metadata_text(raw: Any, *, maximum: int) -> str:
    if (
        not isinstance(raw, str)
        or not raw
        or len(raw) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in raw)
    ):
        raise _EnvironmentEvidenceError("environment.metadata_invalid")
    return raw


def _unpackaged_root_exists(root: str) -> bool:
    for item in sys.path[:256]:
        try:
            base = Path(item or ".")
            if (base / f"{root}.py").is_file() or (base / root / "__init__.py").is_file() or (base / root).is_dir():
                return True
        except OSError:
            continue
    return False


def _analysis_result(
    status: AnalysisStatus,
    binding: Mapping[str, Any],
    function: Mapping[str, Any] | None,
    diagnostics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema": SELECTED_CODE_ANALYSIS_RESULT_SCHEMA,
        "schema_version": 1,
        "status": status,
        "binding": dict(binding),
        "function": dict(function) if function is not None else None,
        "diagnostics": list(diagnostics),
        "analysis_hash": "",
    }
    document["analysis_hash"] = _domain_hash(
        "splime.selected-code-analysis/v1", {key: value for key, value in document.items() if key != "analysis_hash"}
    )
    return document


def _environment_result(
    *,
    status: Literal["verified", "unverified"],
    binding: Mapping[str, Any],
    kernel: Mapping[str, Any],
    observed_at: str,
    inventory_hash: str | None,
    dependencies: Sequence[Mapping[str, Any]],
    excluded_imports: Sequence[Mapping[str, Any]],
    diagnostics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema": ACTIVE_KERNEL_ENVIRONMENT_SCHEMA,
        "schema_version": 1,
        "status": status,
        "binding": dict(binding),
        "kernel": dict(kernel),
        "observed_at": observed_at,
        "inventory_hash": inventory_hash,
        "dependencies": list(dependencies),
        "excluded_imports": list(excluded_imports),
        "diagnostics": list(diagnostics),
        "evidence_hash": "",
    }
    document["evidence_hash"] = _domain_hash(
        "splime.active-kernel-environment/v1", {key: value for key, value in document.items() if key != "evidence_hash"}
    )
    return document


def _diagnostic(
    code: str,
    severity: DiagnosticSeverity,
    *,
    source_index: int | None = None,
    line: int | None = None,
    column: int | None = None,
    subject: str | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "source_index": source_index,
        "line": line,
        "column": column,
        "subject": subject,
    }


def _call_path(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _call_path(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return None


def _walk(node: ast.AST) -> Sequence[ast.AST]:
    """Return a deterministic AST walk without ``ast.walk``'s lazy import."""

    result: list[ast.AST] = []
    stack = [node]
    while stack:
        current = stack.pop()
        result.append(current)
        children = list(ast.iter_child_nodes(current))
        stack.extend(reversed(children))
    return result


def _is_docstring(node: ast.stmt) -> bool:
    return isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)


def _join_sources(sources: Sequence[Mapping[str, str]]) -> str:
    return "\n\n".join(item["source"].rstrip("\n") for item in sources) + "\n"


def _canonical_distribution_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _domain_hash(domain: str, value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
        "utf-8"
    )
    digest = hashlib.sha256(domain.encode("utf-8") + b"\0" + encoded).hexdigest()
    return f"sha256:{digest}"


def _mapping(raw: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping) or any(not isinstance(key, str) for key in raw):
        raise SourceAnalysisContractError(code)
    return cast(Mapping[str, Any], raw)


def _exact_keys(value: Mapping[str, Any], expected: frozenset[str], code: str) -> None:
    if set(value) != expected:
        raise SourceAnalysisContractError(f"{code}.fields")


def _string(raw: Any, code: str, maximum: int) -> str:
    if not isinstance(raw, str) or not raw or len(raw) > maximum or "\x00" in raw:
        raise SourceAnalysisContractError(code)
    return raw


__all__ = [
    "ACTIVE_KERNEL_ENVIRONMENT_REQUEST_SCHEMA",
    "ACTIVE_KERNEL_ENVIRONMENT_SCHEMA",
    "SELECTED_CODE_ANALYSIS_REQUEST_SCHEMA",
    "SELECTED_CODE_ANALYSIS_RESULT_SCHEMA",
    "SOURCE_ANALYSIS_SCHEMA_VERSION",
    "SourceAnalysisContractError",
    "analyze_selected_code",
    "inspect_active_kernel_environment",
]
