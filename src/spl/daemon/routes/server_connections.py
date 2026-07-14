"""Server connection routes for the local daemon."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from spl.daemon.remote_client import DEFAULT_SERVER_URL, ServerClientError
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
        return json_response(
            await context.run_blocking(
                runtime.server_connection_state,
                probe=context.query_bool("probe", default=True),
            )
        )

    @app.get("/server/connections")
    @route_errors
    async def list_server_connections() -> Any:
        return json_response(runtime.store.list_server_connections())

    @app.get("/server/users")
    @route_errors
    async def list_server_users() -> Any:
        _, server = await context.connected_server_client_async()
        return json_response(
            await context.run_blocking(
                server.list_users,
                handle=context.first_query_value("handle"),
            )
        )

    @app.get("/server/whoami")
    @route_errors
    async def server_whoami() -> Any:
        return json_response(await context.run_blocking(runtime.server_whoami))

    @app.post("/server/connections/prune")
    @route_errors
    async def prune_server_connections() -> Any:
        older_than_days = context.optional_int_query("older_than_days")
        return json_response(
            runtime.store.prune_server_connections(
                older_than_days=30 if older_than_days is None else older_than_days,
                dry_run=context.strict_query_bool("dry_run"),
            )
        )

    @app.get("/server/sync/status")
    @route_errors
    async def sync_status() -> Any:
        return json_response(runtime.sync_status())

    @app.post("/server/sync/prune")
    @route_errors
    async def prune_sync_events() -> Any:
        status = context.first_query_value("status")
        if not status:
            raise ValueError("status is required")
        older_than_days = context.optional_int_query("older_than_days")
        limit = context.optional_int_query("limit")
        return json_response(
            runtime.prune_sync_events(
                status=status,
                older_than_days=0 if older_than_days is None else older_than_days,
                include_protected=context.strict_query_bool("include_protected"),
                limit=1_000 if limit is None else limit,
            )
        )

    @app.get("/server/machines")
    @route_errors
    async def list_server_machines() -> Any:
        credentials, server = await context.connected_server_client_async()
        return json_response(await context.run_blocking(_server_machines_payload, credentials, server))

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
            await context.run_blocking(
                runtime.connect_server,
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
        return json_response(await context.run_blocking(runtime.disconnect_server))


def _server_machines_payload(credentials: dict[str, Any], server: Any) -> dict[str, Any]:
    machines = server.list_machines()
    _apply_machine_token_display_names(machines, credentials, server)
    current_machine_id = credentials["machine_id"]
    for machine in machines:
        machine["is_current"] = machine["id"] == current_machine_id
    return {
        "current_machine_id": current_machine_id,
        "machines": machines,
    }


def _apply_machine_token_display_names(
    machines: list[dict[str, Any]],
    credentials: dict[str, Any],
    server: Any,
) -> None:
    aliases = _machine_token_aliases(server)
    current_machine_id = credentials.get("machine_id")
    current_display_name = credentials.get("display_name")
    for machine in machines:
        machine_id = str(machine.get("id") or "")
        display_name = machine.get("display_name")
        if not _is_technical_machine_label(display_name, machine_id):
            continue
        alias = aliases.get(machine_id)
        if alias is None and machine_id == current_machine_id:
            alias = _display_name_from_machine_token(current_display_name, machine_id)
        if alias:
            machine["stored_display_name"] = display_name
            machine["display_name"] = alias


def _machine_token_aliases(server: Any) -> dict[str, str]:
    try:
        tokens = server.list_tokens()
    except (AttributeError, ServerClientError):
        return {}
    aliases: dict[str, str] = {}
    for token in tokens:
        if token.get("subject_type") != "machine":
            continue
        if token.get("status") != "active":
            continue
        machine_id = str(token.get("subject_id") or "")
        alias = _display_name_from_machine_token(token.get("name"), machine_id)
        if alias and machine_id not in aliases:
            aliases[machine_id] = alias
    return aliases


def _display_name_from_machine_token(value: Any, machine_id: str) -> str | None:
    display_name = str(value or "").strip()
    for prefix in ("Machine credential for ",):
        if display_name.casefold().startswith(prefix.casefold()):
            display_name = display_name[len(prefix) :].strip()
    for suffix in (" machine credential", " machine token"):
        if display_name.casefold().endswith(suffix.casefold()):
            display_name = display_name[: -len(suffix)].strip()
    if display_name and not _is_technical_machine_label(display_name, machine_id):
        return display_name
    return None


def _is_technical_machine_label(value: Any, machine_id: str) -> bool:
    label = str(value or "").strip()
    if not label:
        return True
    folded = label.casefold()
    if folded == str(machine_id).casefold():
        return True
    if folded == f"mach_{machine_id}".casefold():
        return True
    if folded.startswith(("mach_", "mach-")):
        return True
    if not folded.startswith("machine"):
        return False
    suffix = folded.removeprefix("machine")
    if suffix.startswith(("_", "-")):
        suffix = suffix[1:]
    return len(suffix) >= 8 and all(char in "0123456789abcdef" for char in suffix)
