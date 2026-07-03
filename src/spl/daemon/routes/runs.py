"""Run lifecycle routes for the local daemon."""

from __future__ import annotations

import json
from http import HTTPStatus
from pathlib import Path
from typing import Any

from spl.daemon.routes._helpers import RouteContext
from spl.daemon.run_progress import environment_progress
from spl.daemon.store import validate_name


def register_run_routes(
    app: Any,
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

    @app.post("/runs")
    @route_errors
    async def start_run() -> Any:
        body = await context.read_json_body()
        if body.get("target_machine") or body.get("remote"):
            return json_response(
                runtime.start_remote_run(
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
                source=body.get("source", "auto"),
            ),
            HTTPStatus.ACCEPTED,
        )

    @app.get("/remote-runs/<run_id>")
    @route_errors
    async def get_remote_run(run_id: str) -> Any:
        credentials = runtime._require_connected_server_credentials()
        return json_response(
            runtime._server_client_for_credentials(credentials).get_remote_run(
                validate_name(run_id)
            )
        )

    @app.get("/runs/<run_id>")
    @route_errors
    async def get_run(run_id: str) -> Any:
        state = runtime.store.get_run(validate_name(run_id))
        progress = environment_progress(runtime.store, state)
        if progress is not None:
            state = {**state, "environment": progress}
        return json_response(state)

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
