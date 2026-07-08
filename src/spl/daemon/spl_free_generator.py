"""Generate SPL-free Python modules for functional daemon runs."""

from __future__ import annotations

import ast
import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from spl.core.ir.unparse import mk_top_level_ast
from spl.core.ir.utils import mk_top_level_deps_closure
from spl.daemon.storage_base import read_json, write_json

GENERATED_MODULE_CACHE_VERSION = "v1"
WorkerRuntime = Literal["spl-free-runner", "legacy-spl-worker"]
SPL_FREE_WORKER_RUNTIME: WorkerRuntime = "spl-free-runner"
LEGACY_WORKER_RUNTIME: WorkerRuntime = "legacy-spl-worker"
REASON_SUPPORTED_FUNCTION = "supported_function_node"
REASON_PIPELINE = "pipeline_node"
REASON_ASYNC_FUNCTION = "async_function"
REASON_DECORATED_FUNCTION = "decorated_function"
REASON_IMPORTS_SPL = "imports_spl"
REASON_DOCKER_RUNTIME = "docker_runtime"


@dataclass(frozen=True)
class WorkerRuntimePlan:
    """Prepared worker command inputs for one venv run."""

    runtime: WorkerRuntime
    reason: str
    marker_path: Path
    runner_path: Path | None = None
    module_path: Path | None = None
    module_name: str | None = None

    def marker(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "worker_runtime": self.runtime,
            "worker_runtime_reason": self.reason,
        }
        if self.module_path is not None:
            payload["generated_module"] = str(self.module_path)
        if self.module_name is not None:
            payload["generated_module_name"] = self.module_name
        return payload


def prepare_worker_runtime(
    *,
    object_record: dict[str, Any],
    object_yaml_path: Path,
    entrypoint: str,
    run_dir: Path,
    generated_modules_dir: Path,
    runner_source_path: Path,
    marker_path: Path,
) -> WorkerRuntimePlan:
    """Prepare the SPL-free path when stage-1 supports this object."""

    if object_record.get("kind") != "function":
        return _legacy_plan(marker_path, REASON_PIPELINE)

    module = build_flat_module_ast(object_yaml_path)
    unsupported_reason = unsupported_stage1_reason(module)
    if unsupported_reason is not None:
        return _legacy_plan(marker_path, unsupported_reason)

    content_hash = str(object_record.get("content_hash") or object_record.get("yaml_sha256") or "")
    if not content_hash:
        content_hash = hashlib.sha256(object_yaml_path.read_bytes()).hexdigest()
    module_dir = generated_modules_dir / GENERATED_MODULE_CACHE_VERSION / content_hash
    module_path = module_dir / "object_module.py"
    module_name = f"_spl_generated_{content_hash[:32]}"
    module_dir.mkdir(parents=True, exist_ok=True)
    if not module_path.exists():
        _write_text_atomically_if_absent(module_path, ast.unparse(module) + "\n")

    runner_path = run_dir / "spl_free_runner.py"
    shutil.copy2(runner_source_path, runner_path)

    plan = WorkerRuntimePlan(
        runtime=SPL_FREE_WORKER_RUNTIME,
        reason=REASON_SUPPORTED_FUNCTION,
        marker_path=marker_path,
        runner_path=runner_path,
        module_path=module_path,
        module_name=module_name,
    )
    write_worker_runtime_marker(plan)
    return plan


def build_flat_module_ast(object_yaml_path: Path) -> ast.Module:
    """Build a flat, SPL-free module AST from an SPL object YAML file."""

    body: list[ast.stmt] = []
    for source_path, top_level in mk_top_level_deps_closure([object_yaml_path])[::-1]:
        body.extend(mk_top_level_ast(top_level, source_path).body)
    module = ast.fix_missing_locations(ast.Module(body=body, type_ignores=[]))
    return filter_spl_runtime_scaffolding(module)


def filter_spl_runtime_scaffolding(module: ast.Module) -> ast.Module:
    """Remove generated SPL metadata scaffolding while preserving user code."""

    filtered = _SplRuntimeScaffoldingFilter().visit(module)
    return cast(ast.Module, ast.fix_missing_locations(filtered))


def unsupported_stage1_reason(module: ast.AST) -> str | None:
    """Return the legacy reason for AST shapes unsupported by stage 1."""

    for node in ast.walk(module):
        if isinstance(node, ast.AsyncFunctionDef):
            return REASON_ASYNC_FUNCTION
        if isinstance(node, ast.FunctionDef) and node.decorator_list:
            return REASON_DECORATED_FUNCTION
        if _references_spl_runtime(node):
            return REASON_IMPORTS_SPL
    return None


def write_worker_runtime_marker(plan: WorkerRuntimePlan) -> None:
    """Persist the worker runtime marker as run-dir diagnostics."""

    write_json(plan.marker_path, plan.marker())


def read_worker_runtime_marker(path: Path) -> dict[str, Any] | None:
    """Read a worker runtime marker if one has been written."""

    marker = read_json(path, None)
    if not isinstance(marker, dict):
        return None
    return cast(dict[str, Any], marker)


def _legacy_plan(marker_path: Path, reason: str) -> WorkerRuntimePlan:
    plan = WorkerRuntimePlan(
        runtime=LEGACY_WORKER_RUNTIME,
        reason=reason,
        marker_path=marker_path,
    )
    write_worker_runtime_marker(plan)
    return plan


def _write_text_atomically_if_absent(path: Path, text: str) -> None:
    if path.exists():
        return

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write(text)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        if path.exists():
            return
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


class _SplRuntimeScaffoldingFilter(ast.NodeTransformer):
    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:
        if node.name != "_":
            return node
        node.body = [
            stmt for stmt in node.body if not _is_spl_scaffolding_import(stmt) and not _is_spl_metadata_setattr(stmt)
        ]
        return node


def _is_spl_scaffolding_import(stmt: ast.stmt) -> bool:
    if not isinstance(stmt, ast.ImportFrom):
        return False
    if stmt.level != 0:
        return False
    if any(alias.asname is not None for alias in stmt.names):
        return False
    imported_names = {alias.name for alias in stmt.names}
    return (stmt.module == "spl.core.entities.node" and imported_names == {"InputPort", "OutputPort"}) or (
        stmt.module == "spl.core.entities.function" and imported_names == {"DFunction"}
    )


def _is_spl_metadata_setattr(stmt: ast.stmt) -> bool:
    if not isinstance(stmt, ast.Expr):
        return False
    call = stmt.value
    if not isinstance(call, ast.Call):
        return False
    if not isinstance(call.func, ast.Name) or call.func.id != "setattr":
        return False
    if len(call.args) != 3 or call.keywords:
        return False
    target_arg = call.args[0]
    if not isinstance(target_arg, ast.Name):
        return False
    name_arg = call.args[1]
    if not isinstance(name_arg, ast.Constant):
        return False
    if name_arg.value == "__spl_location__":
        location_arg = call.args[2]
        return (
            isinstance(location_arg, ast.Tuple)
            and len(location_arg.elts) == 2
            and all(isinstance(elt, ast.Constant) and isinstance(elt.value, str) for elt in location_arg.elts)
        )
    if name_arg.value == "__spl_metadata__":
        metadata_arg = call.args[2]
        return (
            isinstance(metadata_arg, ast.Call)
            and isinstance(metadata_arg.func, ast.Name)
            and metadata_arg.func.id == "DFunction"
        )
    return False


def _references_spl_runtime(node: ast.AST) -> bool:
    if isinstance(node, ast.Import):
        return any(alias.name == "spl" or alias.name.startswith("spl.") for alias in node.names)
    if isinstance(node, ast.ImportFrom):
        return node.module == "spl" or (node.module is not None and node.module.startswith("spl."))
    return isinstance(node, ast.Name) and node.id == "spl"
