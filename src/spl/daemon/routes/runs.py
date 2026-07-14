"""Run lifecycle routes for the local daemon."""

from __future__ import annotations

import json
from http import HTTPStatus
from pathlib import Path
from typing import Any

from spl.core import manifest as m_manifest
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar
from spl.daemon.run_progress import environment_progress, run_observability_progress
from spl.daemon.server_connection import SERVER_PROXY_TIMEOUT_SECONDS
from spl.daemon.store import validate_name


def register_run_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    route_errors = context.route_errors
    json_response = context.json_response

    @app.get("/runs")
    @route_errors
    async def list_runs() -> Any:
        return json_response(runtime.store.list_runs())

    @app.get("/runs/tag-stats")
    @route_errors
    async def tag_stats() -> Any:
        return json_response(runtime.store.run_tag_stats())

    @app.post("/runs/prune")
    @route_errors
    async def prune_runs() -> Any:
        body = await context.read_json_body()
        statuses = body.get("statuses")
        if statuses is not None and not isinstance(statuses, list):
            raise ValueError("statuses must be a list")
        if statuses == []:
            raise ValueError("statuses must not be empty; provide statuses or omit the field")
        return json_response(
            runtime.store.prune_runs(
                run_id=body.get("run_id"),
                statuses=statuses,
                older_than_seconds=body.get("older_than_seconds"),
                dry_run=context.strict_body_bool(body, "dry_run"),
            )
        )

    @app.post("/runs")
    @route_errors
    async def start_run() -> Any:
        body = await context.read_json_body()
        runtimes = body.get("runtimes")
        if runtimes is not None and not isinstance(runtimes, dict):
            raise ValueError("runtimes must be a mapping")
        remote = context.strict_body_bool(body, "remote")
        if body.get("target_machine") or remote:
            return json_response(
                await context.run_blocking(
                    runtime.start_remote_run,
                    body["object"],
                    target_machine=body.get("target_machine"),
                    object_owner_id=body.get("object_owner_id"),
                    library=body.get("library"),
                    args=body.get("args"),
                    kwargs=body.get("kwargs"),
                    output=body.get("output"),
                    timeout_seconds=body.get("timeout_seconds"),
                    version=body.get("version"),
                    object_version_id=body.get("version_id"),
                    function=body.get("function"),
                    correlation_id=body.get("correlation_id"),
                    parent_run_id=body.get("parent_run_id"),
                    context=body.get("context") or {},
                    offline_policy=body.get("offline_policy"),
                ),
                HTTPStatus.ACCEPTED,
            )
        return json_response(
            runtime.start_run(
                body["object"],
                args=body.get("args"),
                kwargs=body.get("kwargs"),
                output=body.get("output"),
                timeout_seconds=body.get("timeout_seconds"),
                version=body.get("version"),
                object_version_id=body.get("version_id"),
                function=body.get("function"),
                object_owner_id=body.get("object_owner_id"),
                library=body.get("library"),
                source=body.get("source", "auto"),
                runtimes=runtimes,
                keep=body.get("keep", "on_failure"),
            ),
            HTTPStatus.ACCEPTED,
        )

    @app.get("/remote-runs/<run_id>")
    @route_errors
    async def get_remote_run(run_id: str) -> Any:
        credentials = await context.run_blocking(runtime._require_live_server_channel_credentials)
        return json_response(
            await context.run_blocking(
                runtime._server_client_for_credentials(
                    credentials,
                    request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
                ).get_remote_run,
                validate_name(run_id),
            )
        )

    @app.get("/runs/<run_id>")
    @route_errors
    async def get_run(run_id: str) -> Any:
        if context.first_query_value("view") == "show":
            return json_response(
                runtime.store.show_run(
                    validate_name(run_id),
                    include_inline_values=context.query_bool("full_inline", default=False),
                )
            )
        state = runtime.store.get_run(validate_name(run_id))
        progress = environment_progress(runtime.store, state)
        if progress is not None:
            state = {**state, "environment": progress}
        observability = run_observability_progress(state)
        if observability is not None:
            state = {**state, "run_progress": observability}
        return json_response(m_manifest.sanitize_run_state(state))

    @app.post("/runs/<run_id>/resume")
    @route_errors
    async def resume_run(run_id: str) -> Any:
        body = await context.read_json_body()
        from_selection = body.get("from", body.get("from_"))
        if from_selection is None:
            raise ValueError("resume requires `from`")
        kwargs = body.get("kwargs")
        if kwargs is not None and not isinstance(kwargs, dict):
            raise ValueError("kwargs must be a mapping")
        adapters = body.get("adapters")
        if adapters is not None and not isinstance(adapters, dict):
            raise ValueError("adapters must be a mapping")
        runtimes = body.get("runtimes")
        if runtimes is not None and not isinstance(runtimes, dict):
            raise ValueError("runtimes must be a mapping")
        return json_response(
            runtime.resume_run(
                validate_name(run_id),
                from_=from_selection,
                kwargs=kwargs,
                output=body.get("output"),
                timeout_seconds=body.get("timeout_seconds"),
                adapters=adapters,
                runtimes=runtimes,
                keep=body.get("keep", "on_failure"),
            ),
            HTTPStatus.ACCEPTED,
        )

    @app.delete("/runs/<run_id>")
    @route_errors
    async def delete_run(run_id: str) -> Any:
        return json_response(
            runtime.store.delete_run(
                validate_name(run_id),
                dry_run=context.strict_query_bool("dry_run"),
            )
        )

    @app.get("/runs/<run_id>/result")
    @route_errors
    async def get_result(run_id: str) -> Any:
        state = runtime.store.get_run(validate_name(run_id))
        if state.get("result") is not None:
            return json_response(state["result"])

        result_path = Path(state["result_path"])
        if not result_path.exists():
            return json_response(
                {"error": "result is not available", "status": state["status"]},
                HTTPStatus.CONFLICT,
            )
        return json_response(json.loads(result_path.read_text(encoding="utf-8")))
