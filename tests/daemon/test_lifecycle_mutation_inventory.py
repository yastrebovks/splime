from __future__ import annotations

import ast
from pathlib import Path


ROUTES_ROOT = Path(__file__).resolve().parents[2] / "src" / "spl" / "daemon" / "routes"
SERVER_SOURCE = ROUTES_ROOT.parent / "server.py"
ENVIRONMENT_SOURCE = ROUTES_ROOT.parent / "environment_base.py"
DOCKER_POOL_SOURCE = ROUTES_ROOT.parent / "docker_pool.py"

TRACKED_MUTATION_ROUTES: dict[tuple[str, str], frozenset[str]] = {
    ("adapter_runs.py", "finalize_adapter_run"): frozenset({"broker.finalize"}),
    ("envs.py", "register_env"): frozenset({"runtime.register_env"}),
    ("envs.py", "rebuild_environment"): frozenset({"manager.rebuild"}),
    ("envs.py", "prune_docker_images"): frozenset({"runtime.run_registry_mutation"}),
    ("guarded_runs.py", "admit_guarded_local_run"): frozenset({"runtime.start_guarded_local_run"}),
    ("library_adapters.py", "publish_library_adapter"): frozenset({"runtime.run_registry_mutation"}),
    ("libraries.py", "create_server_library"): frozenset({"runtime.run_registry_mutation"}),
    ("libraries.py", "update_server_library"): frozenset({"runtime.run_registry_mutation"}),
    ("libraries.py", "grant_server_library"): frozenset({"runtime.run_registry_mutation"}),
    ("libraries.py", "revoke_server_library_grant"): frozenset({"runtime.run_registry_mutation"}),
    ("libraries.py", "add_server_library_reference"): frozenset({"runtime.run_registry_mutation"}),
    ("libraries.py", "copy_server_library_object"): frozenset({"runtime.run_registry_mutation"}),
    ("libraries.py", "remove_server_library_entry"): frozenset({"runtime.run_registry_mutation"}),
    ("objects.py", "pull_server_object"): frozenset({"runtime.pull_server_object"}),
    ("objects.py", "prune_stale_mirrors"): frozenset({"runtime.run_registry_mutation"}),
    ("objects.py", "forget_object"): frozenset({"runtime.run_registry_mutation"}),
    ("objects.py", "forget_object_version"): frozenset({"runtime.run_registry_mutation"}),
    ("objects.py", "register_object"): frozenset(
        {
            "runtime.register_object",
            "runtime.enqueue_object_sync",
            "runtime.prepare_object_environment",
        }
    ),
    ("remote.py", "resolve_remote_signature"): frozenset({"runtime.resolve_remote_signature"}),
    ("remote.py", "run_remote_node"): frozenset({"runtime.run_worker_callback"}),
    ("runs.py", "prune_runs"): frozenset({"runtime.run_registry_mutation"}),
    ("runs.py", "start_run"): frozenset({"runtime.start_run", "runtime.start_remote_run"}),
    ("runs.py", "resume_run"): frozenset({"runtime.resume_run"}),
    ("runs.py", "delete_run"): frozenset({"runtime.run_registry_mutation"}),
    ("runs.py", "acknowledge_delivery"): frozenset({"runtime.acknowledge_run_delivery"}),
    ("server_connections.py", "prune_server_connections"): frozenset({"runtime.run_registry_mutation"}),
    ("server_connections.py", "prune_sync_events"): frozenset({"runtime.prune_sync_events"}),
    ("server_connections.py", "connect_server"): frozenset({"runtime.connect_server"}),
    ("server_connections.py", "disconnect_server"): frozenset({"runtime.disconnect_server"}),
}

NON_ROOT_MUTATION_ROUTES = frozenset(
    {
        ("adapter_runs.py", "cancel_adapter_run"),
        ("adapter_runs.py", "create_adapter_run_admission"),
        ("adapter_runs.py", "upload_adapter_run_input"),
        ("libraries.py", "delete_server_library"),
        ("lifecycle.py", "lifecycle_drain"),
        ("lifecycle.py", "lifecycle_cancel"),
        ("remote.py", "resolve_remote_decomposition"),
        ("runs.py", "connected_remote_run_preflight"),
        ("library_adapters.py", "update_public_adapter_profile"),
        ("objects.py", "update_public_object_profile"),
        ("server_connections.py", "reveal_server_connection_credentials"),
    }
)

NON_MUTATING_POST_ROUTES = frozenset(
    {
        ("adapter_runs.py", "adapter_run_preflight"),
        ("ai_assistant.py", "create_ai_assistant"),
        ("ai_preview.py", "create_ai_preview"),
        ("library_adapters.py", "preflight_library_adapter"),
        ("objects.py", "preflight_public_object"),
        ("prepared_validation.py", "prepare_publication_object"),
        ("prepared_validation.py", "validate_prepared_object"),
        ("source_analysis.py", "analyze_source"),
    }
)

RUNTIME_ADMISSION_CALLS: dict[str, frozenset[str]] = {
    "prune_sync_events": frozenset({"self.run_registry_mutation"}),
    "connect_server": frozenset({"self.run_registry_mutation"}),
    "disconnect_server": frozenset({"self.run_registry_mutation"}),
    "enqueue_object_sync": frozenset({"self.run_registry_mutation"}),
    "register_object": frozenset({"self.lifecycle.reserve_current_or_root"}),
    "resolve_remote_signature": frozenset({"self.run_registry_mutation"}),
    "start_remote_run": frozenset({"self.run_registry_mutation"}),
    "pull_server_object": frozenset({"self.run_registry_mutation"}),
    "register_env": frozenset({"self.lifecycle.reserve_current_or_root"}),
    "sync_once": frozenset({"self.lifecycle.begin_sync"}),
    "start_run": frozenset({"self.lifecycle.reserve_current_or_root"}),
    "start_guarded_local_run": frozenset({"self.lifecycle.reserve_current_or_root"}),
    "resume_run": frozenset({"self.lifecycle.reserve_current_or_root"}),
    "renew_run_delivery": frozenset({"self.run_registry_mutation"}),
    "acknowledge_run_delivery": frozenset({"self.run_registry_mutation"}),
    "sweep_run_retention": frozenset({"self.run_registry_mutation"}),
    "run_worker_callback": frozenset(
        {
            "self._reserve_run_descendant",
            "self.lifecycle.reserve_root",
        }
    ),
    "_probe_server_channel": frozenset({"self.lifecycle.reserve_current_or_root"}),
}


def _attribute_name(value: ast.AST) -> str | None:
    if isinstance(value, ast.Name):
        return value.id
    if isinstance(value, ast.Attribute):
        parent = _attribute_name(value.value)
        return f"{parent}.{value.attr}" if parent is not None else value.attr
    return None


def _calls(node: ast.AST) -> set[str]:
    return {
        name
        for candidate in ast.walk(node)
        if isinstance(candidate, ast.Call)
        if (name := _attribute_name(candidate.func)) is not None
    }


def _references(node: ast.AST) -> set[str]:
    return {
        name
        for candidate in ast.walk(node)
        if isinstance(candidate, ast.Attribute)
        if (name := _attribute_name(candidate)) is not None
    }


def _nested_functions(source: Path) -> dict[str, ast.AsyncFunctionDef | ast.FunctionDef]:
    tree = ast.parse(source.read_text(encoding="utf-8"))
    return {
        candidate.name: candidate
        for candidate in ast.walk(tree)
        if isinstance(candidate, (ast.AsyncFunctionDef, ast.FunctionDef))
    }


def test_every_mutating_daemon_route_has_an_explicit_lifecycle_disposition() -> None:
    discovered: set[tuple[str, str]] = set()
    route_functions: dict[tuple[str, str], ast.AsyncFunctionDef | ast.FunctionDef] = {}
    for source in sorted(ROUTES_ROOT.glob("*.py")):
        for name, function in _nested_functions(source).items():
            for decorator in function.decorator_list:
                if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                    continue
                if decorator.func.attr not in {"post", "put", "delete"}:
                    continue
                key = (source.name, name)
                discovered.add(key)
                route_functions[key] = function

    assert discovered == (set(TRACKED_MUTATION_ROUTES) | set(NON_ROOT_MUTATION_ROUTES) | set(NON_MUTATING_POST_ROUTES))
    for key, required_calls in TRACKED_MUTATION_ROUTES.items():
        assert required_calls <= _references(route_functions[key]), key

    forbidden_mutation_entrypoints = {call for calls in TRACKED_MUTATION_ROUTES.values() for call in calls}
    for key in NON_MUTATING_POST_ROUTES:
        assert not (forbidden_mutation_entrypoints & _references(route_functions[key])), key


def test_every_mutating_runtime_entrypoint_reaches_the_shared_authority() -> None:
    functions = _nested_functions(SERVER_SOURCE)
    for name, required_calls in RUNTIME_ADMISSION_CALLS.items():
        assert required_calls <= _calls(functions[name]), name

    environment_functions = _nested_functions(ENVIRONMENT_SOURCE)
    assert "self._lifecycle.reserve_current_or_root" in _calls(environment_functions["_start_build_thread"])

    docker_pool_functions = _nested_functions(DOCKER_POOL_SOURCE)
    assert "self._lifecycle.reserve_current_or_root" in _calls(docker_pool_functions["prewarm_object"])


def test_side_effecting_get_routes_use_admitted_runtime_entrypoints() -> None:
    artifact_functions = _nested_functions(ROUTES_ROOT / "artifacts.py")
    run_functions = _nested_functions(ROUTES_ROOT / "runs.py")
    for name in ("list_artifacts", "get_artifact"):
        assert "runtime.renew_run_delivery" in _calls(artifact_functions[name])
    assert "runtime.renew_run_delivery" in _calls(run_functions["get_result"])
