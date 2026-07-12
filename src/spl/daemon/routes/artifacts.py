"""Run artifact routes for the local daemon."""

from __future__ import annotations

from http import HTTPStatus
from pathlib import Path
from typing import Any

from spl.daemon.routes._helpers import RouteContext, RouteRegistrar
from spl.daemon.server_connection import SERVER_PROXY_TIMEOUT_SECONDS
from spl.daemon.store import validate_name


def register_artifact_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    route_errors = context.route_errors
    json_response = context.json_response

    @app.get("/remote-runs/<run_id>/artifacts")
    @route_errors
    async def list_remote_artifacts(run_id: str) -> Any:
        credentials = runtime._require_live_server_channel_credentials()
        artifacts = await context.run_blocking(
            runtime._server_client_for_credentials(
                credentials,
                request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
            ).list_artifacts,
            validate_name(run_id),
        )
        return json_response([artifact["name"] for artifact in artifacts])

    @app.get("/remote-runs/<run_id>/artifacts/<artifact_name>")
    @route_errors
    async def get_remote_artifact(run_id: str, artifact_name: str) -> Any:
        credentials = runtime._require_live_server_channel_credentials()
        data = await context.run_blocking(
            runtime._server_client_for_credentials(
                credentials,
                request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
            ).artifact_bytes,
            validate_name(run_id),
            validate_name(artifact_name),
        )
        return context.response_cls(
            data,
            status=int(HTTPStatus.OK),
            content_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{artifact_name}"'},
        )

    @app.get("/runs/<run_id>/artifacts")
    @route_errors
    async def list_artifacts(run_id: str) -> Any:
        state = runtime.store.get_run(validate_name(run_id))
        artifacts_dir = Path(state["artifacts_dir"])
        if not artifacts_dir.exists():
            return json_response([])
        return json_response(sorted(path.name for path in artifacts_dir.iterdir()))

    @app.get("/runs/<run_id>/artifacts/<artifact_name>")
    @route_errors
    async def get_artifact(run_id: str, artifact_name: str) -> Any:
        state = runtime.store.get_run(validate_name(run_id))
        artifact_path = Path(state["artifacts_dir"]) / validate_name(artifact_name)
        if not artifact_path.exists() or not artifact_path.is_file():
            return json_response(
                {"error": "artifact is not found"},
                HTTPStatus.NOT_FOUND,
            )

        return context.response_cls(
            artifact_path.read_bytes(),
            status=int(HTTPStatus.OK),
            content_type="application/octet-stream",
            headers={"Content-Disposition": (f'attachment; filename="{artifact_path.name}"')},
        )
