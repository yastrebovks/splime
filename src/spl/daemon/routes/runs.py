"""Run lifecycle routes for the local daemon."""

from __future__ import annotations

import json
from http import HTTPStatus
from pathlib import Path
from typing import Any

from spl.core import manifest as m_manifest
from spl.daemon.connected_ide import (
    CONNECTED_REMOTE_RUN_PREFLIGHT_ROUTE,
    CONNECTED_REMOTE_RUNS_ROUTE,
    CONNECTED_SERVER_CAPABILITIES_ROUTE,
    MAX_CONNECTED_BODY_BYTES,
    ConnectedIDEContractError,
    build_remote_run_detail_response,
    build_remote_run_events_response,
    build_remote_run_list_response,
    build_remote_run_preflight_response,
    connected_error_document,
    parse_remote_run_preflight_request,
    project_remote_run_server_capabilities,
)
from spl.daemon.remote_client import ServerClientError
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar
from spl.daemon.run_progress import environment_progress, run_observability_progress
from spl.daemon.server_connection import (
    SERVER_REMOTE_RUN_PROXY_TIMEOUT_SECONDS,
    HandleRequiresServerConnectionError,
    ServerOfflineError,
)
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
        dry_run = context.strict_body_bool(body, "dry_run")
        prune_kwargs = {
            "run_id": body.get("run_id"),
            "statuses": statuses,
            "older_than_seconds": body.get("older_than_seconds"),
            "dry_run": dry_run,
        }
        if dry_run:
            result = runtime.store.prune_runs(**prune_kwargs)
        else:
            result = runtime.run_registry_mutation(
                runtime.store.prune_runs,
                **prune_kwargs,
            )
        return json_response(result)

    @app.post("/runs")
    @route_errors
    async def start_run() -> Any:
        body = await context.read_json_body()
        runtimes = body.get("runtimes")
        if runtimes is not None and not isinstance(runtimes, (str, dict)):
            raise ValueError("runtimes must be a runtime name or mapping")
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
                    runtime_port_adapters=body.get("runtime_port_adapters"),
                    runtime_library_adapter_refs=body.get("runtime_library_adapter_refs"),
                    runtime_adapter_semantic_advisories=body.get("runtime_adapter_semantic_advisories"),
                    adapter_policy=body.get("adapter_policy"),
                    runtimes=runtimes,
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
                keep=body.get("keep", True),
                runtime_port_adapters=body.get("runtime_port_adapters"),
                runtime_library_adapter_refs=body.get("runtime_library_adapter_refs"),
                runtime_adapter_semantic_advisories=body.get("runtime_adapter_semantic_advisories"),
                adapter_policy=body.get("adapter_policy"),
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
                    request_timeout_seconds=SERVER_REMOTE_RUN_PROXY_TIMEOUT_SECONDS,
                ).get_remote_run,
                validate_name(run_id),
            )
        )

    @app.get(CONNECTED_SERVER_CAPABILITIES_ROUTE)
    async def connected_server_capabilities() -> Any:
        operation = "connected_server_capabilities"
        try:
            _, server = await context.connected_server_client_async()
            raw_version = await context.run_blocking(server.get_server_version)
            document = project_remote_run_server_capabilities(raw_version)
        except Exception as exc:
            return _connected_exception_response(
                context,
                runtime,
                exc,
                operation=operation,
            )
        return _connected_success_response(context, document)

    @app.get(CONNECTED_REMOTE_RUNS_ROUTE)
    async def connected_remote_runs() -> Any:
        operation = "connected_remote_run_list"
        try:
            _, server = await context.connected_server_client_async()
            runs = await context.run_blocking(server.list_remote_runs)
            document = build_remote_run_list_response(runs)
        except Exception as exc:
            return _connected_exception_response(
                context,
                runtime,
                exc,
                operation=operation,
            )
        return _connected_success_response(context, document)

    @app.get(f"{CONNECTED_REMOTE_RUNS_ROUTE}/<run_id>/detail")
    async def connected_remote_run_detail(run_id: str) -> Any:
        operation = "connected_remote_run_detail"
        try:
            run_id = validate_name(run_id)
            _, server = await context.connected_server_client_async()
            detail = await context.run_blocking(
                server.get_remote_run_detail,
                run_id,
            )
            document = build_remote_run_detail_response(detail)
        except Exception as exc:
            return _connected_exception_response(
                context,
                runtime,
                exc,
                operation=operation,
                non_enumerating=True,
            )
        return _connected_success_response(context, document)

    @app.get(f"{CONNECTED_REMOTE_RUNS_ROUTE}/<run_id>/events")
    async def connected_remote_run_events(run_id: str) -> Any:
        operation = "connected_remote_run_events"
        try:
            run_id = validate_name(run_id)
            _, server = await context.connected_server_client_async()
            events = await context.run_blocking(
                server.list_remote_run_events,
                run_id,
            )
            document = build_remote_run_events_response(events)
        except Exception as exc:
            return _connected_exception_response(
                context,
                runtime,
                exc,
                operation=operation,
                non_enumerating=True,
            )
        return _connected_success_response(context, document)

    @app.post(CONNECTED_REMOTE_RUN_PREFLIGHT_ROUTE)
    async def connected_remote_run_preflight() -> Any:
        operation = "connected_remote_run_preflight"
        try:
            raw = await _read_connected_body(context)
            preflight_request = parse_remote_run_preflight_request(raw)
            _, server = await context.connected_server_client_async()
            raw_version = await context.run_blocking(server.get_server_version)
            server_capabilities = project_remote_run_server_capabilities(raw_version)
            capability = server_capabilities["capabilities"]["spl.remote_run.preflight.v1"]
            if capability["state"] != "supported":
                raise ConnectedIDEContractError(
                    "server_capability_unproven",
                    HTTPStatus.CONFLICT,
                )
            preflight = await context.run_blocking(
                server.preflight_remote_run,
                preflight_request.payload,
            )
            document = build_remote_run_preflight_response(
                preflight,
                server_capabilities,
                preflight_request.payload,
            )
        except Exception as exc:
            return _connected_exception_response(
                context,
                runtime,
                exc,
                operation=operation,
                preflight=True,
            )
        return _connected_success_response(context, document)

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
        if runtimes is not None and not isinstance(runtimes, (str, dict)):
            raise ValueError("runtimes must be a runtime name or mapping")
        return json_response(
            runtime.resume_run(
                validate_name(run_id),
                from_=from_selection,
                kwargs=kwargs,
                output=body.get("output"),
                timeout_seconds=body.get("timeout_seconds"),
                adapters=adapters,
                runtimes=runtimes,
                keep=body.get("keep", True),
            ),
            HTTPStatus.ACCEPTED,
        )

    @app.delete("/runs/<run_id>")
    @route_errors
    async def delete_run(run_id: str) -> Any:
        dry_run = context.strict_query_bool("dry_run")
        run_id = validate_name(run_id)
        if dry_run:
            result = runtime.store.delete_run(run_id, dry_run=True)
        else:
            result = runtime.run_registry_mutation(
                runtime.store.delete_run,
                run_id,
                dry_run=False,
            )
        return json_response(result)

    @app.get("/runs/<run_id>/result")
    @route_errors
    async def get_result(run_id: str) -> Any:
        run_id = validate_name(run_id)
        runtime.renew_run_delivery(run_id)
        state = runtime.store.get_run(run_id)
        if state.get("result") is not None:
            return json_response(state["result"])

        raw_result_path = state.get("result_path")
        result_path = Path(str(raw_result_path)) if raw_result_path else None
        if result_path is None or not result_path.is_file():
            return json_response(
                {"error": "result is not available", "status": state["status"]},
                HTTPStatus.CONFLICT,
            )
        return json_response(json.loads(result_path.read_text(encoding="utf-8")))

    @app.post("/runs/<run_id>/delivery-ack")
    @route_errors
    async def acknowledge_delivery(run_id: str) -> Any:
        return json_response(runtime.acknowledge_run_delivery(validate_name(run_id)))


async def _read_connected_body(context: RouteContext) -> bytes:
    if context.request.mimetype != "application/json":
        raise ConnectedIDEContractError("request_invalid", HTTPStatus.BAD_REQUEST)
    content_length = context.request.content_length
    if content_length is not None and content_length > MAX_CONNECTED_BODY_BYTES:
        raise ConnectedIDEContractError(
            "body_too_large",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
        )
    try:
        raw = await context.request.get_data(cache=False)
    except Exception as exc:
        too_large = getattr(exc, "code", None) == int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        raise ConnectedIDEContractError(
            "body_too_large" if too_large else "request_invalid",
            (HTTPStatus.REQUEST_ENTITY_TOO_LARGE if too_large else HTTPStatus.BAD_REQUEST),
        ) from exc
    if not isinstance(raw, bytes):
        raise ConnectedIDEContractError("request_invalid", HTTPStatus.BAD_REQUEST)
    return raw


def _connected_success_response(context: RouteContext, document: Any) -> Any:
    response = context.json_response(document)
    response.headers["Cache-Control"] = "no-store"
    return response


def _connected_exception_response(
    context: RouteContext,
    runtime: Any,
    exc: Exception,
    *,
    operation: str,
    non_enumerating: bool = False,
    preflight: bool = False,
) -> Any:
    error = _connected_error_for_exception(
        runtime,
        exc,
        non_enumerating=non_enumerating,
        preflight=preflight,
    )
    response = context.json_response(
        connected_error_document(
            error.code,
            operation,
            retryable=error.retryable,
        ),
        error.status,
    )
    response.headers["Cache-Control"] = "no-store"
    return response


def _connected_error_for_exception(
    runtime: Any,
    exc: Exception,
    *,
    non_enumerating: bool,
    preflight: bool,
) -> ConnectedIDEContractError:
    if isinstance(exc, ConnectedIDEContractError):
        return exc
    if isinstance(exc, (HandleRequiresServerConnectionError, KeyError)):
        return ConnectedIDEContractError(
            "server_not_connected",
            HTTPStatus.CONFLICT,
        )
    if isinstance(exc, ServerOfflineError):
        return ConnectedIDEContractError(
            "server_offline",
            HTTPStatus.SERVICE_UNAVAILABLE,
            retryable=True,
        )
    if isinstance(exc, ServerClientError):
        if non_enumerating and exc.status_code in {403, 404}:
            return ConnectedIDEContractError(
                "remote_run_not_found",
                HTTPStatus.NOT_FOUND,
            )
        if exc.code in {"server_response_invalid", "server_response_too_large"}:
            return ConnectedIDEContractError(
                "connected_protocol_incompatible",
                HTTPStatus.BAD_GATEWAY,
            )
        if runtime._is_server_connectivity_error(exc):
            runtime._mark_current_server_channel_failure(error=exc)
            return ConnectedIDEContractError(
                "server_offline",
                HTTPStatus.SERVICE_UNAVAILABLE,
                retryable=True,
            )
        if exc.status_code == 401 and exc.code in {
            "central_credential_revoked",
            "central_credential_expired",
        }:
            return ConnectedIDEContractError(
                exc.code,
                HTTPStatus.FAILED_DEPENDENCY,
            )
        if exc.status_code == 401:
            return ConnectedIDEContractError(
                "server_authentication_failed",
                HTTPStatus.FAILED_DEPENDENCY,
            )
        if exc.status_code == 403:
            return ConnectedIDEContractError(
                ("preflight_permission_denied" if preflight else "server_permission_denied"),
                HTTPStatus.FORBIDDEN,
            )
        if preflight and exc.code in {
            "preflight_target_unavailable",
            "server_object_copy_missing",
        }:
            return ConnectedIDEContractError(exc.code, HTTPStatus.NOT_FOUND)
        if preflight and exc.status_code == 409:
            return ConnectedIDEContractError(
                "remote_preflight_conflict",
                HTTPStatus.CONFLICT,
            )
        return ConnectedIDEContractError(
            "server_response_invalid",
            HTTPStatus.BAD_GATEWAY,
        )
    if isinstance(exc, ValueError):
        return ConnectedIDEContractError("request_invalid", HTTPStatus.BAD_REQUEST)
    return ConnectedIDEContractError(
        "server_response_invalid",
        HTTPStatus.BAD_GATEWAY,
    )
