"""Object registry routes for the local daemon."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from spl.daemon.remote_client import ServerClientError
from spl.daemon.routes._helpers import RouteContext
from spl.daemon.signature import build_signature, summarize_object
from spl.daemon.store import validate_name


def register_object_routes(
    app: Any,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    route_errors = context.route_errors
    json_response = context.json_response

    @app.get("/server/objects")
    @route_errors
    async def list_server_objects() -> Any:
        _, server = context.connected_server_client()
        view = (context.first_query_value("view") or "").lower()
        compact = (context.first_query_value("compact") or "").lower() in {
            "1",
            "true",
            "yes",
        }
        return json_response(
            server.list_objects(
                owner_id=context.first_query_value("owner", "owner_id"),
                library=context.first_query_value("library"),
                compact=view == "summary" or compact,
            )
        )

    @app.get("/objects")
    @route_errors
    async def list_objects() -> Any:
        query = context.first_query_value("q", "query")
        view = (context.first_query_value("view") or "").lower()
        compact = (context.first_query_value("compact") or "").lower() in {
            "1",
            "true",
            "yes",
        }
        if query is None:
            records = runtime.store.list_objects()
            if view == "summary" or compact:
                return json_response(
                    {
                        name: summarize_object(record)
                        for name, record in records.items()
                    }
                )
            return json_response(records)

        search_records = runtime.store.search_objects(query)
        if view == "summary" or compact:
            return json_response(
                [summarize_object(record) for record in search_records]
            )
        return json_response(search_records)

    @app.get("/objects/search")
    @route_errors
    async def search_objects() -> Any:
        return json_response(
            runtime.store.search_objects(context.first_query_value("q", "query") or "")
        )

    @app.get("/objects/<name_or_id>")
    @route_errors
    async def get_object(name_or_id: str) -> Any:
        include_yaml = (context.first_query_value("include_yaml") or "").lower() in {
            "1",
            "true",
            "yes",
        }
        return json_response(
            context.object_from_local_or_server(
                validate_name(name_or_id),
                include_yaml=include_yaml,
            )
        )

    @app.get("/objects/<name_or_id>/signature")
    @route_errors
    async def object_signature(name_or_id: str) -> Any:
        object_name, function = context.object_function_ref(name_or_id)
        record = context.object_from_local_or_server(
            object_name,
            include_yaml=False,
        )
        return json_response(build_signature(record, function=function))

    @app.get("/objects/<name_or_id>/decomposition")
    @route_errors
    async def object_decomposition(name_or_id: str) -> Any:
        record = context.object_from_local_or_server(
            validate_name(name_or_id),
            include_yaml=False,
        )
        return json_response(record["decomposition"])

    @app.get("/objects/<name_or_id>/inputs")
    @route_errors
    async def object_inputs(name_or_id: str) -> Any:
        object_name, function = context.object_function_ref(name_or_id)
        record = context.object_from_local_or_server(
            object_name,
            include_yaml=False,
        )
        return json_response(build_signature(record, function=function)["inputs"])

    @app.get("/objects/<name_or_id>/outputs")
    @route_errors
    async def object_outputs(name_or_id: str) -> Any:
        object_name, function = context.object_function_ref(name_or_id)
        record = context.object_from_local_or_server(
            object_name,
            include_yaml=False,
        )
        return json_response(build_signature(record, function=function)["outputs"])

    @app.get("/objects/<name_or_id>/versions")
    @route_errors
    async def list_object_versions(name_or_id: str) -> Any:
        refresh = runtime.refresh_server_object_if_available(validate_name(name_or_id))
        if refresh and refresh.get("current_version"):
            name_or_id = refresh["current_version"]["name"]
        return json_response(
            runtime.store.list_object_versions(validate_name(name_or_id))
        )

    @app.post("/objects")
    @route_errors
    async def register_object() -> Any:
        body = await context.read_json_body()
        create_library = str(
            body.get("create_library", body.get("create"))
        ).strip().lower() in {"1", "true", "yes", "on"}
        record = runtime.register_object(
            body["name"],
            body["entrypoint"],
            body["env"],
            yaml_text=body.get("yaml"),
            yaml_path=body.get("yaml_path"),
            workdir=body.get("workdir"),
            description=body.get("description"),
            version_label=body.get("version_label"),
            object_id=body.get("object_id"),
            runtime_config=body.get("runtime_config"),
        )
        if not body.get("local_only", False):
            record["sync_event"] = runtime.enqueue_object_sync(
                record,
                library=body.get("library") or body.get("library_slug"),
                create_library=create_library,
                library_display_name=(
                    body.get("library_display_name")
                    or body.get("library_name")
                ),
            )
            try:
                record["sync"] = runtime.sync_once()
            except ServerClientError as exc:
                record["sync_error"] = exc.message
        record["environment_build"] = runtime.prepare_object_environment(record)
        return json_response(record, HTTPStatus.CREATED)
