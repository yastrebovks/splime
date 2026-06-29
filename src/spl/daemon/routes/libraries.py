"""Server library routes for the local daemon."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from spl.daemon.routes._helpers import RouteContext
from spl.daemon.store import validate_name


def register_library_routes(
    app: Any,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    del runtime
    route_errors = context.route_errors
    json_response = context.json_response

    @app.get("/server/libraries")
    @route_errors
    async def list_server_libraries() -> Any:
        _, server = context.connected_server_client()
        return json_response(
            server.list_libraries(
                include_accessible=context.query_bool(
                    "include_accessible",
                    default=True,
                ),
            )
        )

    @app.post("/server/libraries")
    @route_errors
    async def create_server_library() -> Any:
        _, server = context.connected_server_client()
        return json_response(
            server.create_library(await context.read_json_body()),
            HTTPStatus.CREATED,
        )

    @app.get("/server/libraries/<library_ref>")
    @route_errors
    async def get_server_library(library_ref: str) -> Any:
        _, server = context.connected_server_client()
        return json_response(server.get_library(validate_name(library_ref)))

    @app.put("/server/libraries/<library_ref>")
    @route_errors
    async def update_server_library(library_ref: str) -> Any:
        _, server = context.connected_server_client()
        return json_response(
            server.update_library(
                validate_name(library_ref),
                await context.read_json_body(),
            )
        )

    @app.delete("/server/libraries/<library_ref>")
    @route_errors
    async def delete_server_library(library_ref: str) -> Any:
        _, server = context.connected_server_client()
        return json_response(server.delete_library(validate_name(library_ref)))

    @app.get("/server/libraries/<library_ref>/grants")
    @route_errors
    async def list_server_library_grants(library_ref: str) -> Any:
        _, server = context.connected_server_client()
        return json_response(server.list_library_grants(validate_name(library_ref)))

    @app.post("/server/libraries/<library_ref>/grants")
    @route_errors
    async def grant_server_library(library_ref: str) -> Any:
        _, server = context.connected_server_client()
        return json_response(
            server.grant_library(
                validate_name(library_ref),
                await context.read_json_body(),
            ),
            HTTPStatus.CREATED,
        )

    @app.post("/server/libraries/<library_ref>/grants/<grantee>/revoke")
    @route_errors
    async def revoke_server_library_grant(library_ref: str, grantee: str) -> Any:
        _, server = context.connected_server_client()
        return json_response(
            server.revoke_library_grant(
                validate_name(library_ref),
                validate_name(grantee),
            )
        )

    @app.post("/server/libraries/<library_ref>/references")
    @route_errors
    async def add_server_library_reference(library_ref: str) -> Any:
        _, server = context.connected_server_client()
        return json_response(
            server.add_library_reference(
                validate_name(library_ref),
                await context.read_json_body(),
            ),
            HTTPStatus.CREATED,
        )

    @app.post("/server/libraries/<library_ref>/copies")
    @route_errors
    async def copy_server_library_object(library_ref: str) -> Any:
        _, server = context.connected_server_client()
        return json_response(
            server.copy_object_into_library(
                validate_name(library_ref),
                await context.read_json_body(),
            ),
            HTTPStatus.CREATED,
        )

    @app.delete("/server/libraries/<library_ref>/entries/<name>")
    @route_errors
    async def remove_server_library_entry(library_ref: str, name: str) -> Any:
        _, server = context.connected_server_client()
        return json_response(
            server.remove_library_entry(
                validate_name(library_ref),
                validate_name(name),
            )
        )
