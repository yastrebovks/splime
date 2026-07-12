"""Object registry routes for the local daemon."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from spl.daemon.routes._helpers import RouteContext, RouteRegistrar
from spl.daemon.signature import build_signature, summarize_object
from spl.daemon.store import validate_name


def register_object_routes(
    app: RouteRegistrar,
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
            await context.run_blocking(
                server.list_objects,
                owner_id=context.first_query_value("owner", "owner_id"),
                library=context.first_query_value("library"),
                compact=view == "summary" or compact,
            )
        )

    @app.post("/server-objects/pull")
    @route_errors
    async def pull_server_object() -> Any:
        body = await context.read_json_body()
        name = body.get("name") or body.get("object_name")
        if name is None or str(name) == "":
            raise ValueError("name is required")
        raw_version = body.get("version")
        version = None if raw_version is None or raw_version == "" else int(raw_version)
        return json_response(
            await context.run_blocking(
                runtime.pull_server_object,
                validate_name(str(name)),
                version=version,
                owner_id=body.get("owner_id") or body.get("owner"),
                library=body.get("library"),
                all_versions=str(body.get("all_versions", "")).strip().lower() in {"1", "true", "yes", "on"},
                dry_run=str(body.get("dry_run", "")).strip().lower() in {"1", "true", "yes", "on"},
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
                return json_response({name: summarize_object(record) for name, record in records.items()})
            return json_response(records)

        search_records = runtime.store.search_objects(query)
        if view == "summary" or compact:
            return json_response([summarize_object(record) for record in search_records])
        return json_response(search_records)

    @app.get("/objects/search")
    @route_errors
    async def search_objects() -> Any:
        return json_response(runtime.store.search_objects(context.first_query_value("q", "query") or ""))

    @app.post("/objects/prune-stale-mirrors")
    @route_errors
    async def prune_stale_mirrors() -> Any:
        return json_response(
            runtime.store.prune_stale_mirrors(
                owner_id=context.first_query_value("owner", "owner_id"),
                library=context.first_query_value("library"),
            )
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
            await context.object_from_local_or_server_async(
                validate_name(name_or_id),
                include_yaml=include_yaml,
            )
        )

    @app.delete("/objects/<name_or_id>")
    @route_errors
    async def forget_object(name_or_id: str) -> Any:
        version = context.first_query_value("version")
        owner_id = context.first_query_value("owner", "owner_id")
        library = context.first_query_value("library")
        if version is not None and version != "":
            return json_response(
                runtime.store.forget_object_version(
                    validate_name(name_or_id),
                    version,
                    owner_id=owner_id,
                    library=library,
                )
            )
        return json_response(
            runtime.store.forget_object(
                validate_name(name_or_id),
                owner_id=owner_id,
                library=library,
            )
        )

    @app.get("/objects/<name_or_id>/signature")
    @route_errors
    async def object_signature(name_or_id: str) -> Any:
        object_name, function = context.object_function_ref(name_or_id)
        record = await context.object_from_local_or_server_async(
            object_name,
            include_yaml=False,
        )
        return json_response(build_signature(record, function=function))

    @app.get("/objects/<name_or_id>/decomposition")
    @route_errors
    async def object_decomposition(name_or_id: str) -> Any:
        record = await context.object_from_local_or_server_async(
            validate_name(name_or_id),
            include_yaml=False,
        )
        return json_response(record["decomposition"])

    @app.get("/objects/<name_or_id>/inputs")
    @route_errors
    async def object_inputs(name_or_id: str) -> Any:
        object_name, function = context.object_function_ref(name_or_id)
        record = await context.object_from_local_or_server_async(
            object_name,
            include_yaml=False,
        )
        return json_response(build_signature(record, function=function)["inputs"])

    @app.get("/objects/<name_or_id>/outputs")
    @route_errors
    async def object_outputs(name_or_id: str) -> Any:
        object_name, function = context.object_function_ref(name_or_id)
        record = await context.object_from_local_or_server_async(
            object_name,
            include_yaml=False,
        )
        return json_response(build_signature(record, function=function)["outputs"])

    @app.get("/objects/<name_or_id>/versions")
    @route_errors
    async def list_object_versions(name_or_id: str) -> Any:
        owner_id = context.first_query_value("owner", "owner_id")
        library = context.first_query_value("library")
        refresh = await context.run_blocking(
            runtime.refresh_server_object_if_available,
            validate_name(name_or_id),
            owner_id=owner_id,
            library=library,
        )
        if refresh and refresh.get("current_version"):
            name_or_id = refresh["current_version"]["name"]
        return json_response(
            runtime.store.list_object_versions(
                validate_name(name_or_id),
                owner_id=owner_id,
                library=library,
            )
        )

    @app.delete("/objects/<name_or_id>/versions/<version_ref>")
    @route_errors
    async def forget_object_version(name_or_id: str, version_ref: str) -> Any:
        return json_response(
            runtime.store.forget_object_version(
                validate_name(name_or_id),
                version_ref,
                owner_id=context.first_query_value("owner", "owner_id"),
                library=context.first_query_value("library"),
            )
        )

    @app.post("/objects")
    @route_errors
    async def register_object() -> Any:
        body = await context.read_json_body()
        create_library = str(body.get("create_library", body.get("create"))).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
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
            owner_id=body.get("owner_id") or body.get("object_owner_id"),
            library=body.get("library") or body.get("library_slug"),
            runtime_config=body.get("runtime_config"),
        )
        if not body.get("local_only", False):
            record["sync_event"] = runtime.enqueue_object_sync(
                record,
                library=body.get("library") or body.get("library_slug"),
                create_library=create_library,
                library_display_name=(body.get("library_display_name") or body.get("library_name")),
            )
            record["sync"] = runtime._local_sync_status()
            runtime._kick_server_sync()
        record["environment_build"] = runtime.prepare_object_environment(record)
        return json_response(record, HTTPStatus.CREATED)
