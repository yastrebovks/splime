"""Static safety and compatibility contract for the public 0.4.5 cookbook."""

from __future__ import annotations

import ast
import json
import os
import re
from pathlib import Path
from typing import Any

import pytest


DEFAULT_NOTEBOOK_PATH = Path(__file__).resolve().parents[3] / "Notebooks" / "splime-cookbook.ipynb"
NOTEBOOK_PATH = Path(os.environ.get("SPL_RELEASE_COOKBOOK_PATH", DEFAULT_NOTEBOOK_PATH))
EXPECTED_MARKER = "splime==0.4.5"
EXPECTED_VERIFICATION_DATE = "2026-07-27"
pytestmark = pytest.mark.skipif(
    not NOTEBOOK_PATH.is_file(),
    reason="canonical cookbook is available only in the SPL release workspace",
)

DANGEROUS_DEFAULTS = {
    "RUN_CROSS_OWNER",
    "RUN_DESTRUCTIVE_ACTIONS",
    "RUN_DOCKER_EXAMPLES",
    "RUN_ENVIRONMENT_PROBE",
    "RUN_EXTERNAL_NETWORK",
    "RUN_LIVE_SERVER",
    "RUN_LOCAL_CLEANUP",
    "RUN_REMOTE_EXECUTION",
    "RUN_SHARED_LIBRARY_MUTATIONS",
}


def _notebook() -> dict[str, Any]:
    with NOTEBOOK_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def _source(cell: Any) -> str:
    source = cell.get("source", "")
    return "".join(source) if isinstance(source, list) else str(source)


def _code_cells(notebook: Any) -> list[Any]:
    return [cell for cell in notebook["cells"] if cell.get("cell_type") == "code"]


def _code_source(notebook: Any) -> str:
    return "\n\n".join(_source(cell) for cell in _code_cells(notebook))


def _trees(notebook: Any) -> list[ast.Module]:
    return [ast.parse(_source(cell)) for cell in _code_cells(notebook)]


def _top_level_assignments(notebook: Any) -> dict[str, list[ast.expr]]:
    assignments: dict[str, list[ast.expr]] = {}
    for tree in _trees(notebook):
        for statement in tree.body:
            if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
                continue
            targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
            value = statement.value
            if value is None:
                continue
            for target in targets:
                if isinstance(target, ast.Name):
                    assignments.setdefault(target.id, []).append(value)
    return assignments


def _attribute_name(call: ast.Call) -> str | None:
    return call.func.attr if isinstance(call.func, ast.Attribute) else None


def _receiver_name(call: ast.Call) -> str | None:
    if not isinstance(call.func, ast.Attribute) or not isinstance(call.func.value, ast.Name):
        return None
    return call.func.value.id


def _attribute_chain(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _attribute_chain(node.value)
        return f"{parent}.{node.attr}" if parent else None
    return None


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    return next((item.value for item in call.keywords if item.arg == name), None)


def _guard_names(node: ast.AST, parent: dict[ast.AST, ast.AST]) -> set[str]:
    guards: set[str] = set()
    ancestor = parent.get(node)
    while ancestor is not None:
        if isinstance(ancestor, ast.If):
            guards.update(item.id for item in ast.walk(ancestor.test) if isinstance(item, ast.Name))
        ancestor = parent.get(ancestor)
    return guards


def _expression_is_namespaced(
    expression: ast.expr,
    assignments: dict[str, list[ast.expr]],
    *,
    seen: frozenset[str] = frozenset(),
) -> bool:
    if isinstance(expression, ast.Name):
        if expression.id == "NOTEBOOK_NAMESPACE":
            return True
        if expression.id in seen:
            return False
        values = assignments.get(expression.id, [])
        return bool(values) and all(
            _expression_is_namespaced(value, assignments, seen=seen | {expression.id}) for value in values
        )
    if isinstance(expression, ast.JoinedStr):
        return any(
            isinstance(value, ast.FormattedValue) and _expression_is_namespaced(value.value, assignments, seen=seen)
            for value in expression.values
        )
    if isinstance(expression, ast.BinOp):
        return _expression_is_namespaced(
            expression.left,
            assignments,
            seen=seen,
        ) or _expression_is_namespaced(expression.right, assignments, seen=seen)
    return False


def test_cookbook_is_valid_current_nbformat() -> None:
    notebook = _notebook()

    assert notebook["nbformat"] == 4
    assert notebook["nbformat_minor"] >= 5
    assert isinstance(notebook["metadata"], dict)
    assert isinstance(notebook["cells"], list) and notebook["cells"]
    assert all(cell.get("cell_type") in {"code", "markdown", "raw"} for cell in notebook["cells"])
    assert all(isinstance(cell.get("metadata"), dict) for cell in notebook["cells"])
    cell_ids = [cell.get("id") for cell in notebook["cells"]]
    assert all(isinstance(cell_id, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", cell_id) for cell_id in cell_ids)
    assert len(cell_ids) == len(set(cell_ids))
    for cell in notebook["cells"]:
        assert isinstance(cell.get("source"), (str, list))
        if cell.get("cell_type") == "code":
            assert isinstance(cell.get("outputs"), list)
            assert cell.get("execution_count") is None or isinstance(cell.get("execution_count"), int)


def test_cookbook_has_the_045_marker_and_intentional_date() -> None:
    notebook = _notebook()
    first_cell = _source(notebook["cells"][0])
    markdown = "\n".join(_source(cell) for cell in notebook["cells"] if cell.get("cell_type") == "markdown")

    assert f"Public notebook marker: {EXPECTED_MARKER}; updated {EXPECTED_VERIFICATION_DATE}." in first_cell
    assert "splime==0.3.0" not in markdown
    assert "splime==0.4.4" not in markdown
    assert "offline-first" in markdown.lower()


def test_cookbook_preserves_the_current_top_level_api_examples() -> None:
    notebook = _notebook()
    imported: set[str] = set()
    called_attributes: set[str] = set()
    for tree in _trees(notebook):
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "spl":
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.Call):
                attribute = _attribute_name(node)
                if attribute is not None:
                    called_attributes.add(attribute)

    assert {"Deployment", "DDistribution", "NodeRemote", "SPLClient", "lift"} <= imported
    assert {
        "call",
        "collect",
        "describe",
        "forget",
        "forget_version",
        "objects",
        "publish",
        "signature",
        "submit",
    } <= called_attributes


def test_all_network_mutation_and_cleanup_controls_default_false() -> None:
    assignments = _top_level_assignments(_notebook())

    assert DANGEROUS_DEFAULTS <= assignments.keys()
    for name in DANGEROUS_DEFAULTS:
        assert assignments[name]
        assert all(isinstance(value, ast.Constant) and value.value is False for value in assignments[name]), name


def test_placeholder_credentials_never_construct_a_connected_client(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import spl

    notebook = _notebook()
    credential_cells = [
        cell for cell in _code_cells(notebook) if "user_token=" in _source(cell) and "machine_token=" in _source(cell)
    ]
    assert len(credential_cells) == 1

    class GuardClient:
        constructor_calls = 0

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args
            del kwargs
            type(self).constructor_calls += 1

        def current_server_connection(self, *, probe: bool = True) -> dict[str, bool]:
            del probe
            return {"connected": False}

    class ExistingLocalClient:
        def current_server_connection(self, *, probe: bool = True) -> dict[str, bool]:
            assert probe is False
            return {"connected": False}

    monkeypatch.setattr(spl, "SPLClient", GuardClient)
    local_client = ExistingLocalClient()
    namespace: dict[str, Any] = {
        "RUN_EXTERNAL_NETWORK": False,
        "RUN_LIVE_SERVER": False,
        "client": local_client,
    }
    exec(compile(_source(credential_cells[0]), "<cookbook-credentials-cell>", "exec"), namespace)

    output = capsys.readouterr().out.lower()
    assert GuardClient.constructor_calls == 0
    assert namespace["client"] is local_client
    assert "skip" in output


def test_resource_names_share_one_unique_notebook_namespace() -> None:
    notebook = _notebook()
    assignments = _top_level_assignments(notebook)

    assert "NOTEBOOK_NAMESPACE" in assignments
    assert "CREATED_OBJECT_NAMES" in assignments
    assert "CREATED_LIBRARY_NAMES" in assignments
    assert "CREATED_RUN_IDS" in assignments
    namespace_values = assignments["NOTEBOOK_NAMESPACE"]
    assert len(namespace_values) == 1
    assert any(
        isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "uuid4"
        for node in ast.walk(namespace_values[0])
    )

    checked = 0
    tracked_resources = 0
    tracked_runs = 0
    name_methods = {"call", "describe", "forget_version", "render", "signature", "submit"}
    for tree in _trees(notebook):
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            method = _attribute_name(node)
            receiver = _receiver_name(node)
            if method == "add" and receiver in {"CREATED_LIBRARY_NAMES", "CREATED_OBJECT_NAMES"}:
                assert node.args and _expression_is_namespaced(node.args[0], assignments)
                tracked_resources += 1
                continue
            if method == "add" and receiver == "CREATED_RUN_IDS":
                assert node.args
                assert any(
                    isinstance(item, ast.Attribute)
                    and item.attr in {"id", "run"}
                    or isinstance(item, ast.Subscript)
                    and isinstance(item.slice, ast.Constant)
                    and item.slice.value == "id"
                    for item in ast.walk(node.args[0])
                )
                tracked_runs += 1
                continue
            expression: ast.expr | None = None
            if method == "publish":
                expression = _keyword(node, "name")
            elif method in name_methods and node.args:
                expression = node.args[0]
                library = _keyword(node, "library")
                if library is not None:
                    assert _expression_is_namespaced(library, assignments)
            elif method in {"create", "grant"} and node.args:
                expression = node.args[0]
            elif _attribute_chain(node.func) == "NodeRemote.locate":
                expression = _keyword(node, "pipeline")
            if expression is None:
                continue
            checked += 1
            assert _expression_is_namespaced(expression, assignments), ast.dump(expression, include_attributes=False)

    assert checked >= 15
    assert tracked_resources >= 6
    assert tracked_runs >= 6


def test_cleanup_is_opt_in_and_only_targets_notebook_created_names() -> None:
    notebook = _notebook()
    parent: dict[ast.AST, ast.AST] = {}
    forget_calls: list[ast.Call] = []
    for tree in _trees(notebook):
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parent[child] = node
            if isinstance(node, ast.Call) and _attribute_name(node) in {"forget", "forget_version"}:
                forget_calls.append(node)

    assert forget_calls
    for call in forget_calls:
        assert call.args and not isinstance(call.args[0], ast.Constant)
        guards = _guard_names(call, parent)
        assert {"RUN_DESTRUCTIVE_ACTIONS", "RUN_LOCAL_CLEANUP"} <= guards

    cleanup_loops = [
        node
        for tree in _trees(notebook)
        for node in ast.walk(tree)
        if isinstance(node, ast.For)
        and isinstance(node.target, ast.Name)
        and node.target.id == "object_name"
        and isinstance(node.iter, ast.Call)
        and isinstance(node.iter.func, ast.Name)
        and node.iter.func.id == "sorted"
        and node.iter.args
        and isinstance(node.iter.args[0], ast.Name)
        and node.iter.args[0].id == "CREATED_OBJECT_NAMES"
    ]
    assert len(cleanup_loops) == 1


def test_part_one_connection_reads_never_probe_the_server() -> None:
    notebook = _notebook()
    part_two = next(
        index
        for index, cell in enumerate(notebook["cells"])
        if cell.get("cell_type") == "markdown" and "# Part 2" in _source(cell)
    )
    calls = []
    for cell in notebook["cells"][:part_two]:
        if cell.get("cell_type") != "code":
            continue
        calls.extend(
            node
            for node in ast.walk(ast.parse(_source(cell)))
            if isinstance(node, ast.Call) and _attribute_name(node) == "current_server_connection"
        )

    assert len(calls) == 1
    probe = _keyword(calls[0], "probe")
    assert isinstance(probe, ast.Constant) and probe.value is False


def test_live_external_and_mutating_calls_are_guarded() -> None:
    notebook = _notebook()
    checks = {
        "connected-client": False,
        "cross-owner": False,
        "distribution-build": False,
        "docker-build": False,
        "remote-run": False,
        "shared-library": False,
    }
    for tree in _trees(notebook):
        parent: dict[ast.AST, ast.AST] = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parent[child] = node
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            method = _attribute_name(node)
            call_chain = _attribute_chain(node.func)
            guards = _guard_names(node, parent)
            keyword_names = {item.arg for item in node.keywords}
            if (
                isinstance(node.func, ast.Name)
                and node.func.id == "SPLClient"
                and {
                    "machine_token",
                    "user_token",
                }
                <= keyword_names
            ):
                assert {"RUN_EXTERNAL_NETWORK", "RUN_LIVE_SERVER"} <= guards
                checks["connected-client"] = True
            elif method == "call" and "target_machine" in keyword_names:
                assert {"RUN_LIVE_SERVER", "RUN_REMOTE_EXECUTION"} <= guards
                checks["remote-run"] = True
            elif call_chain in {"client.library.create", "client.library.grant"}:
                assert {"RUN_LIVE_SERVER", "RUN_SHARED_LIBRARY_MUTATIONS"} <= guards
                checks["shared-library"] = True
            elif method == "publish" and _keyword(node, "library") is not None:
                assert {"RUN_LIVE_SERVER", "RUN_SHARED_LIBRARY_MUTATIONS"} <= guards
                checks["shared-library"] = True
            elif method == "objects" and _keyword(node, "owner") is not None:
                assert {"RUN_CROSS_OWNER", "RUN_LIVE_SERVER"} <= guards
                checks["cross-owner"] = True
            elif method == "publish" and isinstance(_keyword(node, "name"), ast.Name):
                name = _keyword(node, "name")
                assert isinstance(name, ast.Name)
                if name.id == "MATRIX_PIPELINE_OBJECT":
                    assert "RUN_EXTERNAL_NETWORK" in guards
                    checks["distribution-build"] = True
                elif name.id == "CLEAN_ENV_OBJECT":
                    assert "RUN_ENVIRONMENT_PROBE" in guards
            elif method == "environment_builds":
                assert {"RUN_DOCKER_EXAMPLES", "RUN_ENVIRONMENT_PROBE"} <= guards
                checks["docker-build"] = True
            elif call_chain == "NodeRemote.locate":
                assert {"RUN_LIVE_SERVER", "RUN_REMOTE_EXECUTION"} <= guards
            elif (
                method == "run"
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Call)
                and isinstance(node.func.value.func, ast.Name)
                and node.func.value.func.id == "Deployment"
                and any(isinstance(item, ast.Name) and item.id == "p_mixed" for item in ast.walk(node))
            ):
                assert {"RUN_LIVE_SERVER", "RUN_REMOTE_EXECUTION"} <= guards
            elif method == "call" and node.args and isinstance(node.args[0], ast.Name):
                if node.args[0].id == "CLEAN_ENV_OBJECT":
                    assert "RUN_ENVIRONMENT_PROBE" in guards
            elif method == "with_node_runtime":
                assert "RUN_DOCKER_EXAMPLES" in guards
            elif method == "current_server_connection" and _keyword(node, "probe") is None:
                assert {"RUN_EXTERNAL_NETWORK", "RUN_LIVE_SERVER"} <= guards

    assert all(checks.values()), checks


def test_owner_grantee_and_machine_defaults_are_non_live_placeholders() -> None:
    assignments = _top_level_assignments(_notebook())

    target_machine = assignments["TARGET_MACHINE"][0]
    owner = assignments["OWNER_HANDLE"][0]
    grantee = assignments["SHARED_LIBRARY_GRANTEE"][0]
    assert isinstance(target_machine, ast.Constant) and target_machine.value is None
    assert isinstance(owner, ast.Constant) and isinstance(owner.value, str) and owner.value.endswith("-placeholder")
    assert (
        isinstance(grantee, ast.Constant) and isinstance(grantee.value, str) and grantee.value.endswith("-placeholder")
    )

    source = _code_source(_notebook())
    assert "admin1" not in source
    assert "analyst1" not in source
    assert "production" not in source.lower()


def test_large_finite_numbers_are_demonstrated_without_javascript_coercion() -> None:
    source = _code_source(_notebook())
    compact = "".join(source.split())

    assert "2**53+1" in compact
    assert "1e20" in compact
    assert "LARGE_FINITE" in source
    assert "Number(" not in source
    assert "parseFloat(" not in source
    tree = ast.parse(source)
    large_asserts = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assert)
        and any(isinstance(item, ast.Name) and item.id == "large_finite_result" for item in ast.walk(node.test))
    ]
    assert len(large_asserts) == 3
    asserted_fields = {
        item.slice.value
        for node in large_asserts
        for item in ast.walk(node.test)
        if isinstance(item, ast.Subscript)
        and isinstance(item.slice, ast.Constant)
        and isinstance(item.slice.value, str)
    }
    assert asserted_fields == {"finite_float", "negative", "positive"}
    assert any(
        isinstance(node, ast.Call)
        and _attribute_name(node) == "call"
        and node.args
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "LARGE_FINITE_OBJECT"
        for node in ast.walk(tree)
    )


def test_checked_in_public_notebook_has_no_saved_outputs() -> None:
    for cell in _code_cells(_notebook()):
        assert cell.get("execution_count") is None
        assert not cell.get("outputs")
