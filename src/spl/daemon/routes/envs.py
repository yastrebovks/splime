"""Environment and environment-build routes for the local daemon."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from spl.daemon.routes._helpers import RouteContext
from spl.daemon.store import validate_name


def register_env_routes(
    app: Any,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    route_errors = context.route_errors
    json_response = context.json_response

    @app.get("/envs")
    @route_errors
    async def list_envs() -> Any:
        return json_response(runtime.store.list_envs())

    @app.post("/envs")
    @route_errors
    async def register_env() -> Any:
        body = await context.read_json_body()
        return json_response(
            runtime.store.register_env(body["name"], body["python"]),
            HTTPStatus.CREATED,
        )

    @app.get("/environment-builds")
    @route_errors
    async def list_environment_builds() -> Any:
        return json_response(runtime.store.list_environment_builds())

    @app.get("/environment-builds/<spec_hash>")
    @route_errors
    async def get_environment_build(spec_hash: str) -> Any:
        record = runtime.store.get_environment_build(validate_name(spec_hash))
        if record is None:
            return json_response(
                {"error": f"environment build is not found: {spec_hash}"},
                HTTPStatus.NOT_FOUND,
            )
        return json_response(record)

    @app.post("/environment-builds/<spec_hash>/rebuild")
    @route_errors
    async def rebuild_environment(spec_hash: str) -> Any:
        body = await context.read_json_body()
        wait = bool(body.get("wait", False))
        resolved_spec_hash = validate_name(spec_hash)
        record = runtime.store.get_environment_build(resolved_spec_hash)
        if record is None:
            return json_response(
                {"error": f"environment build is not found: {spec_hash}"},
                HTTPStatus.NOT_FOUND,
            )
        manager = (
            runtime.docker_environment_manager
            if record.get("runtime_type") == "docker"
            else runtime.environment_manager
        )
        return json_response(
            manager.rebuild(resolved_spec_hash, wait=wait),
            HTTPStatus.ACCEPTED,
        )

    @app.post("/docker-images/prune")
    @route_errors
    async def prune_docker_images() -> Any:
        body = await context.read_json_body()
        spec_hash = body.get("spec_hash")
        return json_response(
            runtime.docker_environment_manager.prune_images(
                validate_name(spec_hash) if spec_hash else None
            )
        )
