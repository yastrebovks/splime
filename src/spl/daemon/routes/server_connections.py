"""Server connection routes for the local daemon."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from spl.daemon.remote_client import DEFAULT_SERVER_URL
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar


def register_server_connection_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    route_errors = context.route_errors
    json_response = context.json_response

    @app.get("/server/connection")
    @route_errors
    async def current_server_connection() -> Any:
        connection = runtime.store.current_server_connection()
        return json_response(
            {
                "connected": (
                    connection is not None
                    and connection["status"] == "connected"
                    and bool(connection.get("remote_connection_id"))
                ),
                "offline": (connection is not None and connection["status"] in {"connect_failed", "heartbeat_failed"}),
                "connection": connection,
            }
        )

    @app.get("/server/connections")
    @route_errors
    async def list_server_connections() -> Any:
        return json_response(runtime.store.list_server_connections())

    @app.get("/server/machines")
    @route_errors
    async def list_server_machines() -> Any:
        credentials, server = context.connected_server_client()
        machines = server.list_machines()
        current_machine_id = credentials["machine_id"]
        for machine in machines:
            machine["is_current"] = machine["id"] == current_machine_id
        return json_response(
            {
                "current_machine_id": current_machine_id,
                "machines": machines,
            }
        )

    @app.post("/server/connect")
    @route_errors
    async def connect_server() -> Any:
        body = await context.read_json_body()
        machine_token = body.get("machine_token")
        user_token = body.get("user_token")
        if not machine_token or not user_token:
            raise ValueError("machine_token and user_token are required")

        server_url = body.get("server_url") or DEFAULT_SERVER_URL
        return json_response(
            runtime.connect_server(
                server_url=server_url,
                machine_token=machine_token,
                user_token=user_token,
                machine_id=body.get("machine_id"),
                display_name=body.get("display_name"),
                capabilities=body.get("capabilities") or {},
                heartbeat_interval_seconds=body.get("heartbeat_interval_seconds"),
            ),
            HTTPStatus.CREATED,
        )

    @app.post("/server/disconnect")
    @route_errors
    async def disconnect_server() -> Any:
        return json_response(runtime.disconnect_server())
