"""Health and diagnostics routes for the local daemon."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from spl.daemon.services.sync import SyncVisibilityService
from spl.daemon.store import utc_now


def register_diagnostics_routes(
    app: Any,
    *,
    runtime: Any,
    json_response: Callable[[Any], Any],
    route_errors: Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]],
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
        build_statuses: dict[str, int] = {}
        for build in environment_builds:
            status = str(build["status"])
            build_statuses[status] = build_statuses.get(status, 0) + 1

        connection = runtime.store.current_server_connection()
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
                    "offline": (
                        connection is not None
                        and connection["status"] in {"connect_failed", "heartbeat_failed"}
                    ),
                    "connection": connection,
                },
                "sync": sync_summary,
                "environment_builds": {
                    "by_status": build_statuses,
                    "auto_build_envs": runtime.auto_build_envs,
                    "build_timeout_seconds": (
                        getattr(runtime.environment_manager, "build_timeout_seconds", None)
                    ),
                    "stale_lock_seconds": (
                        getattr(runtime.environment_manager, "stale_lock_seconds", None)
                    ),
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
