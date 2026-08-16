"""Fixed central AI Preview facade for the local integrated daemon."""

from __future__ import annotations

import asyncio
from http import HTTPStatus
from typing import Any, cast
from uuid import uuid4

from spl.daemon.ai_preview import (
    AI_PREVIEW_CAPABILITIES_ROUTE,
    AI_PREVIEW_CAPABILITY_TIMEOUT_SECONDS,
    AI_PREVIEW_ROUTE,
    AI_PREVIEW_SERVER_TIMEOUT_SECONDS,
    MAX_AI_PREVIEW_REQUEST_BYTES,
    AIPreviewContractError,
    CENTRAL_AI_PREVIEW_ERROR_SEMANTICS,
    ai_preview_error_document,
    parse_ai_preview_request,
    validate_ai_preview_capabilities,
    validate_ai_preview_request,
    validate_ai_preview_result,
)
from spl.daemon.remote_client import ServerClientError
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar
from spl.daemon.server_connection import (
    HandleRequiresServerConnectionError,
    ServerOfflineError,
)

_SAFE_MESSAGES = {
    "ai_preview_disabled": "AI Preview is disabled on the central server.",
    "provider_unavailable": "The configured AI provider is unavailable.",
    "central_credential_rejected": "The saved central credential was rejected.",
    "permission_denied": "The connected identity is not authorized to use AI Preview.",
    "rate_limited": "AI Preview is temporarily rate limited.",
    "invalid_request": "The AI Preview request did not match the closed contract.",
    "provider_timeout": "The AI provider did not complete within the bounded time.",
    "provider_refused": "The AI provider declined to produce this proposal.",
    "provider_failed": "The AI provider request did not complete safely.",
    "provider_result_invalid": "The AI provider result did not match the closed proposal contract.",
    "server_not_connected": "Connect this daemon to the central server before using AI Preview.",
    "server_offline": "The connected central server is unavailable.",
    "ai_preview_protocol_invalid": "The central AI Preview response did not match the closed contract.",
}


def register_ai_preview_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    """Register the two authenticated, additive client-neutral operations."""

    route_app = cast(Any, app)
    _install_ai_preview_request_limit(route_app)

    if not getattr(route_app, "_spl_ai_preview_no_store", False):

        async def ai_preview_no_store(response: Any) -> Any:
            if str(context.request.path) in {
                AI_PREVIEW_CAPABILITIES_ROUTE,
                AI_PREVIEW_ROUTE,
            }:
                response.headers["Cache-Control"] = "no-store"
            return response

        route_app.after_request(ai_preview_no_store)
        route_app._spl_ai_preview_no_store = True

    @app.get(AI_PREVIEW_CAPABILITIES_ROUTE)
    async def ai_preview_capabilities() -> Any:
        try:
            _, server = await context.connected_server_client_async(
                request_timeout_seconds=AI_PREVIEW_CAPABILITY_TIMEOUT_SECONDS,
            )
            async with asyncio.timeout(AI_PREVIEW_CAPABILITY_TIMEOUT_SECONDS):
                document = await context.run_blocking(server.get_ai_preview_capabilities)
            if server.ai_preview_request_contains_configured_credential(document):
                raise AIPreviewContractError("ai_preview_protocol_invalid")
            validate_ai_preview_capabilities(document)
        except Exception as exc:
            return _error_response(
                context,
                runtime,
                exc,
                post_attempted=False,
                capability_read=True,
            )
        return _success_response(context, document)

    @app.post(AI_PREVIEW_ROUTE)
    async def create_ai_preview() -> Any:
        post_attempted = False
        try:
            request = parse_ai_preview_request(await _read_body(context))
            _, server = await context.connected_server_client_async(
                request_timeout_seconds=AI_PREVIEW_CAPABILITY_TIMEOUT_SECONDS,
            )
            if server.ai_preview_request_contains_configured_credential(
                request.document,
            ):
                raise AIPreviewContractError("invalid_request")
            async with asyncio.timeout(AI_PREVIEW_CAPABILITY_TIMEOUT_SECONDS):
                capabilities = await context.run_blocking(
                    server.get_ai_preview_capabilities,
                )
            if server.ai_preview_request_contains_configured_credential(capabilities):
                raise AIPreviewContractError("ai_preview_protocol_invalid")
            validate_ai_preview_capabilities(capabilities)
            # Bind consent to the server's current stable policy immediately
            # before the one non-replayed provider request.
            validate_ai_preview_request(
                request.document,
                current_capabilities=capabilities,
            )
            _, server = await context.connected_server_client_async(
                request_timeout_seconds=AI_PREVIEW_SERVER_TIMEOUT_SECONDS,
            )
            post_attempted = True
            async with asyncio.timeout(AI_PREVIEW_SERVER_TIMEOUT_SECONDS):
                document = await context.run_blocking(
                    server.create_ai_preview,
                    request.document,
                )
            if server.ai_preview_request_contains_configured_credential(document):
                raise AIPreviewContractError("ai_preview_protocol_invalid")
            validate_ai_preview_result(
                document,
                request=request.document,
                current_capabilities=capabilities,
            )
        except Exception as exc:
            return _error_response(
                context,
                runtime,
                exc,
                post_attempted=post_attempted,
                capability_read=False,
            )
        return _success_response(context, document)


async def _read_body(context: RouteContext) -> bytes:
    if context.request.mimetype != "application/json":
        raise AIPreviewContractError("invalid_request")
    content_length = context.request.content_length
    if content_length is not None and content_length > MAX_AI_PREVIEW_REQUEST_BYTES:
        raise AIPreviewContractError(
            "body_too_large",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
        )
    try:
        raw = await context.request.get_data(cache=False)
    except Exception as exc:
        too_large = getattr(exc, "code", None) == int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        raise AIPreviewContractError(
            "body_too_large" if too_large else "invalid_request",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE if too_large else HTTPStatus.BAD_REQUEST,
        ) from exc
    if not isinstance(raw, bytes):
        raise AIPreviewContractError("invalid_request")
    return raw


def _success_response(context: RouteContext, document: Any) -> Any:
    response = context.json_response(document)
    response.headers["Cache-Control"] = "no-store"
    return response


def _error_response(
    context: RouteContext,
    runtime: Any,
    exc: Exception,
    *,
    post_attempted: bool,
    capability_read: bool,
) -> Any:
    error = _map_error(
        runtime,
        exc,
        post_attempted=post_attempted,
        capability_read=capability_read,
    )
    code, status, retryable, outcome = error
    correlation_id = f"ai-{uuid4().hex}"
    response = context.json_response(
        ai_preview_error_document(
            code,
            safe_message=_SAFE_MESSAGES[code],
            retryable=retryable,
            outcome=outcome,
            correlation_id=correlation_id,
        ),
        status,
    )
    response.headers["Cache-Control"] = "no-store"
    return response


def _map_error(
    runtime: Any,
    exc: Exception,
    *,
    post_attempted: bool,
    capability_read: bool,
) -> tuple[str, HTTPStatus, bool, str]:
    if isinstance(exc, AIPreviewContractError):
        if exc.code in {"invalid_request", "body_too_large"}:
            return "invalid_request", exc.status, False, "not_started"
        if exc.code in {"ai_preview_disabled", "provider_unavailable"}:
            return exc.code, HTTPStatus.SERVICE_UNAVAILABLE, False, "not_started"
        return (
            "provider_unavailable" if capability_read else "provider_result_invalid",
            HTTPStatus.BAD_GATEWAY,
            False,
            "not_completed" if post_attempted else "not_started",
        )
    if isinstance(exc, (HandleRequiresServerConnectionError, KeyError)):
        return "server_not_connected", HTTPStatus.CONFLICT, False, "not_started"
    if isinstance(exc, TimeoutError):
        if post_attempted:
            return "provider_timeout", HTTPStatus.GATEWAY_TIMEOUT, False, "unknown"
        return "server_offline", HTTPStatus.SERVICE_UNAVAILABLE, True, "not_started"
    if isinstance(exc, ServerOfflineError):
        return "server_offline", HTTPStatus.SERVICE_UNAVAILABLE, True, "not_started"
    if isinstance(exc, ServerClientError):
        if exc.code == "server_response_timeout":
            if post_attempted:
                return (
                    "provider_timeout",
                    HTTPStatus.GATEWAY_TIMEOUT,
                    False,
                    "unknown",
                )
            return (
                "server_offline",
                HTTPStatus.SERVICE_UNAVAILABLE,
                True,
                "not_started",
            )
        if exc.code in {
            "ai_preview_disabled",
            "provider_unavailable",
            "central_credential_rejected",
            "permission_denied",
            "rate_limited",
            "invalid_request",
            "provider_timeout",
            "provider_refused",
            "provider_failed",
            "provider_result_invalid",
        }:
            return _central_error_semantics(exc.code, exc.status_code)
        if exc.code == "ai_preview_protocol_invalid":
            return (
                "ai_preview_protocol_invalid",
                HTTPStatus.BAD_GATEWAY,
                False,
                "unknown" if post_attempted else "not_started",
            )
        if runtime._is_server_connectivity_error(exc):
            runtime._mark_current_server_channel_failure(error=exc)
            return (
                "server_offline",
                HTTPStatus.SERVICE_UNAVAILABLE,
                False if post_attempted else True,
                "unknown" if post_attempted else "not_started",
            )
        if exc.status_code == 401:
            return (
                "central_credential_rejected",
                HTTPStatus.FAILED_DEPENDENCY,
                False,
                "not_started",
            )
        if exc.status_code == 403:
            return "permission_denied", HTTPStatus.FORBIDDEN, False, "not_started"
        if exc.status_code == 429:
            return "rate_limited", HTTPStatus.TOO_MANY_REQUESTS, True, "not_started"
        if exc.status_code == 400:
            return "invalid_request", HTTPStatus.BAD_REQUEST, False, "not_started"
        return (
            "provider_unavailable" if capability_read else "provider_result_invalid",
            HTTPStatus.BAD_GATEWAY,
            False,
            "not_completed" if post_attempted else "not_started",
        )
    return (
        "provider_unavailable" if capability_read else "provider_result_invalid",
        HTTPStatus.BAD_GATEWAY,
        False,
        "not_completed" if post_attempted else "not_started",
    )


def _central_error_semantics(
    code: str,
    status_code: int,
) -> tuple[str, HTTPStatus, bool, str]:
    statuses: dict[str, HTTPStatus] = {
        "ai_preview_disabled": HTTPStatus.SERVICE_UNAVAILABLE,
        "provider_unavailable": HTTPStatus.SERVICE_UNAVAILABLE,
        "central_credential_rejected": HTTPStatus.FAILED_DEPENDENCY,
        "permission_denied": HTTPStatus.FORBIDDEN,
        "rate_limited": HTTPStatus.TOO_MANY_REQUESTS,
        "invalid_request": HTTPStatus.BAD_REQUEST,
        "provider_timeout": HTTPStatus.GATEWAY_TIMEOUT,
        "provider_refused": HTTPStatus.UNPROCESSABLE_ENTITY,
        "provider_failed": HTTPStatus.BAD_GATEWAY,
        "provider_result_invalid": HTTPStatus.BAD_GATEWAY,
    }
    status = statuses[code]
    retryable, outcome = CENTRAL_AI_PREVIEW_ERROR_SEMANTICS[code]
    try:
        observed_status = HTTPStatus(status_code)
    except ValueError:
        observed_status = status
    # Preserve safe 4xx/5xx central status without ever upgrading it to 2xx/3xx.
    if 400 <= int(observed_status) <= 599:
        status = observed_status
    return code, status, retryable, outcome


def _install_ai_preview_request_limit(app: Any) -> None:
    base_request_class = app.request_class
    if getattr(base_request_class, "_spl_ai_preview_limit", False):
        return

    class AIPreviewRequest(base_request_class):  # type: ignore[misc, valid-type]
        _spl_ai_preview_limit = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            method = args[0] if args else kwargs.get("method")
            path = args[2] if len(args) > 2 else kwargs.get("path")
            if method == "POST" and path == AI_PREVIEW_ROUTE:
                configured_limit = kwargs.get("max_content_length")
                kwargs["max_content_length"] = (
                    MAX_AI_PREVIEW_REQUEST_BYTES
                    if configured_limit is None
                    else min(int(configured_limit), MAX_AI_PREVIEW_REQUEST_BYTES)
                )
            super().__init__(*args, **kwargs)

    app.request_class = AIPreviewRequest


__all__ = ["register_ai_preview_routes"]
