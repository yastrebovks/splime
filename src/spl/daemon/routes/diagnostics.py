"""Health and diagnostics routes for the local daemon."""

from __future__ import annotations

from typing import Any, Callable

from spl.daemon.interpreter_visibility import (
    interpreter_substitution_from_resolution,
    python_minor_mismatch,
)
from spl.daemon.repositories.server_connection import OFFLINE_SERVER_CONNECTION_STATUSES
from spl.daemon.routes._helpers import RouteErrorDecorator, RouteRegistrar
from spl.daemon.services.sync import SyncVisibilityService
from spl.daemon.store import utc_now


def register_diagnostics_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    json_response: Callable[[Any], Any],
    route_errors: RouteErrorDecorator,
) -> None:
    sync_visibility = getattr(
        runtime,
        "sync_visibility",
        SyncVisibilityService(runtime.store),
    )

    @app.get("/health")
    @route_errors
    async def health() -> Any:
        envs = runtime.store.list_envs()
        objects = runtime.store.list_objects()
        runs = runtime.store.list_runs()
        environment_builds = runtime.store.list_environment_builds()
        remote_signatures = runtime.store.list_remote_signatures()
        interpreter_substitutions = _interpreter_substitution_summary(runtime, objects)
        build_statuses: dict[str, int] = {}
        for build in environment_builds:
            status = str(build["status"])
            build_statuses[status] = build_statuses.get(status, 0) + 1

        connection = runtime.store.current_server_connection()
        connection_summary = runtime.store.server_connection_summary()
        sync_summary = sync_visibility.summary()
        return json_response(
            {
                "ok": True,
                "db": {
                    "path": str(runtime.store.db_path),
                    "exists": runtime.store.db_path.exists(),
                },
                "counts": {
                    "envs": len(envs),
                    "objects": len(objects),
                    "runs": len(runs),
                    "pending_sync_events": sync_summary["pending"],
                    "environment_builds": len(environment_builds),
                    "remote_signatures": len(remote_signatures),
                },
                "server": {
                    "connected": (
                        connection is not None
                        and connection["status"] == "connected"
                        and bool(connection.get("remote_connection_id"))
                    ),
                    "offline": (connection is not None and connection["status"] in OFFLINE_SERVER_CONNECTION_STATUSES),
                    "connection": connection,
                    "connection_summary": connection_summary,
                },
                "sync": sync_summary,
                "interpreter_substitutions": interpreter_substitutions,
                "environment_builds": {
                    "by_status": build_statuses,
                    "auto_build_envs": runtime.auto_build_envs,
                    "build_timeout_seconds": (getattr(runtime.environment_manager, "build_timeout_seconds", None)),
                    "stale_lock_seconds": (getattr(runtime.environment_manager, "stale_lock_seconds", None)),
                },
            }
        )

    @app.get("/diagnostics")
    @route_errors
    async def diagnostics() -> Any:
        connection = runtime.store.current_server_connection()
        pending_sync_events = sync_visibility.pending_events(limit=200)
        environment_builds = runtime.store.list_environment_builds()
        runs = runtime.store.list_runs()
        objects = runtime.store.list_objects()
        envs = runtime.store.list_envs()
        return json_response(
            {
                "ok": True,
                "generated_at": utc_now(),
                "home": str(runtime.store.home),
                "db_path": str(runtime.store.db_path),
                "server": {
                    "connection": connection,
                    "pending_sync_events": len(pending_sync_events),
                    "last_error": connection.get("error") if connection else None,
                },
                "sync": sync_visibility.summary(pending_sync_events),
                "counts": {
                    "envs": len(envs),
                    "objects": len(objects),
                    "runs": len(runs),
                    "environment_builds": len(environment_builds),
                    "pending_sync_events": len(pending_sync_events),
                },
                "envs": envs,
                "environment_builds": environment_builds,
                "pending_sync_events": pending_sync_events,
                "recent_runs": runs[:25],
                "objects": list(objects.values())[:100],
            }
        )


def _interpreter_substitution_summary(
    runtime: Any,
    objects: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for record in objects.values():
        if record.get("origin") != "server":
            continue
        runtime_config = record.get("runtime_config") or {"mode": "venv"}
        if runtime_config.get("mode") != "venv":
            continue
        try:
            spec = runtime.environment_manager.build_spec(record)
        except Exception:
            continue
        spec_payload = spec.get("spec")
        if not isinstance(spec_payload, dict):
            continue
        resolution = spec_payload.get("interpreter_resolution")
        if not isinstance(resolution, dict):
            continue
        substitution = interpreter_substitution_from_resolution(resolution)
        if substitution is None:
            continue
        item = {
            "object": record.get("name"),
            "display_name": record.get("display_name"),
            "version": record.get("version"),
            "version_id": record.get("version_id"),
            **substitution,
        }
        item["minor_mismatch"] = python_minor_mismatch(
            item.get("authored_python_version"),
            item.get("resolved_python_version"),
        )
        items.append(item)
    minor_mismatches = sum(1 for item in items if item.get("minor_mismatch"))
    return {
        "items": items,
        "count": len(items),
        "minor_mismatches": minor_mismatches,
    }
