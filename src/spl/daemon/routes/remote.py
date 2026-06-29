"""Remote signature, decomposition, and node-run routes."""

from __future__ import annotations

from typing import Any

from spl.daemon.routes._helpers import RouteContext


def register_remote_routes(
    app: Any,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    route_errors = context.route_errors
    json_response = context.json_response

    @app.get("/remote-signatures")
    @route_errors
    async def list_remote_signatures() -> Any:
        return json_response(runtime.store.list_remote_signatures())

    @app.post("/remote-signatures/resolve")
    @route_errors
    async def resolve_remote_signature() -> Any:
        body = await context.read_json_body()
        ref = body.get("ref") or body
        force = bool(body.get("force", False))
        signature = runtime.resolve_remote_signature(ref, force=force)
        normalized = runtime._normalize_remote_ref(ref)
        return json_response(
            {
                "signature": signature,
                "cache": runtime.store.get_remote_signature(normalized),
            }
        )

    @app.post("/remote-decompositions/resolve")
    @route_errors
    async def resolve_remote_decomposition() -> Any:
        body = await context.read_json_body()
        ref = body.get("ref") or body
        return json_response(runtime.resolve_remote_decomposition(ref))

    @app.post("/remote-nodes/run")
    @route_errors
    async def run_remote_node() -> Any:
        body = await context.read_json_body()
        return json_response(
            runtime.run_remote_node(
                body["node"],
                kwargs=body.get("kwargs") or {},
                timeout_seconds=body.get("timeout_seconds"),
            )
        )
