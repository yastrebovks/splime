"""Provider-neutral paid AI helper facade for the integrated daemon."""

from __future__ import annotations

import asyncio
from http import HTTPStatus
from typing import Any, cast
from uuid import uuid4

from spl.daemon.ai_assistant import (
    AI_ASSISTANT_CAPABILITIES_ROUTE,
    AI_ASSISTANT_ROUTE,
    CAPABILITY_TIMEOUT_SECONDS,
    MAX_REQUEST_BYTES,
    SERVER_TIMEOUT_SECONDS,
    AIAssistantContractError,
    error_document,
    parse_request,
    validate_capabilities,
    validate_request,
    validate_result,
)
from spl.daemon.remote_client import ServerClientError
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar
from spl.daemon.server_connection import (
    HandleRequiresServerConnectionError,
    ServerOfflineError,
)


_SAFE_MESSAGES = {
    "ai_assistant_disabled": "AI tools are disabled on the central server.",
    "provider_unavailable": "The configured AI provider is unavailable.",
    "central_credential_rejected": "The saved central credential was rejected.",
    "permission_denied": "The connected identity is not authorized to use AI tools.",
    "plan_limit_reached": "The monthly AI request allowance has been used.",
    "subscription_inactive": "The current subscription does not allow managed AI requests.",
    "rate_limited": "AI tools are temporarily rate limited.",
    "invalid_request": "The AI tool request is invalid, stale, or already used.",
    "provider_timeout": "The AI provider did not complete within the bounded time.",
    "provider_refused": "The AI provider declined this request.",
    "provider_failed": "The AI provider request did not complete safely.",
    "provider_result_invalid": "The AI provider result did not match the closed contract.",
    "server_not_connected": "Connect this daemon to spl-server before using AI tools.",
    "server_offline": "The connected spl-server is unavailable.",
    "ai_assistant_protocol_invalid": "The central AI response did not match the closed contract.",
}


def register_ai_assistant_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    """Register additive helper capability and execution routes."""

    route_app = cast(Any, app)
    _install_request_limit(route_app)

    if not getattr(route_app, "_spl_ai_assistant_no_store", False):

        async def no_store(response: Any) -> Any:
            if str(context.request.path) in {
                AI_ASSISTANT_CAPABILITIES_ROUTE,
                AI_ASSISTANT_ROUTE,
            }:
                response.headers["Cache-Control"] = "no-store"
                response.headers["Pragma"] = "no-cache"
                response.headers["Expires"] = "0"
            return response

        route_app.after_request(no_store)
        route_app._spl_ai_assistant_no_store = True

    @app.get(AI_ASSISTANT_CAPABILITIES_ROUTE)
    async def ai_assistant_capabilities() -> Any:
        try:
            _, server = await context.connected_server_client_async(
                request_timeout_seconds=CAPABILITY_TIMEOUT_SECONDS,
            )
            async with asyncio.timeout(CAPABILITY_TIMEOUT_SECONDS):
                document = await context.run_blocking(
                    server.get_ai_assistant_capabilities,
                )
            if server.ai_preview_request_contains_configured_credential(document):
                raise AIAssistantContractError("ai_assistant_protocol_invalid")
            validate_capabilities(document)
        except Exception as exc:
            return _error_response(
                context,
                runtime,
                exc,
                post_attempted=False,
                capability_read=True,
            )
        return _success_response(context, document)

    @app.post(AI_ASSISTANT_ROUTE)
    async def create_ai_assistant() -> Any:
        post_attempted = False
        try:
            request = parse_request(await _read_body(context))
            _, server = await context.connected_server_client_async(
                request_timeout_seconds=CAPABILITY_TIMEOUT_SECONDS,
            )
            if server.ai_preview_request_contains_configured_credential(
                request.document,
            ):
                raise AIAssistantContractError("invalid_request")
            async with asyncio.timeout(CAPABILITY_TIMEOUT_SECONDS):
                capabilities = await context.run_blocking(
                    server.get_ai_assistant_capabilities,
                )
            if server.ai_preview_request_contains_configured_credential(capabilities):
                raise AIAssistantContractError("ai_assistant_protocol_invalid")
            validate_capabilities(capabilities)
            if not capabilities["available"]:
                raise AIAssistantContractError(
                    "ai_assistant_disabled" if capabilities["reason"] == "feature_disabled" else "provider_unavailable",
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            if capabilities["billing"]["allow"] is not True:
                code = (
                    "subscription_inactive"
                    if capabilities["billing"]["recovery_action"] == "activate_subscription"
                    else "plan_limit_reached"
                )
                raise AIAssistantContractError(code, HTTPStatus.PAYMENT_REQUIRED)
            validate_request(
                request.document,
                current_capabilities=capabilities,
            )
            _, server = await context.connected_server_client_async(
                request_timeout_seconds=SERVER_TIMEOUT_SECONDS,
            )
            post_attempted = True
            async with asyncio.timeout(SERVER_TIMEOUT_SECONDS):
                document = await context.run_blocking(
                    server.create_ai_assistant,
                    request.document,
                )
            if server.ai_preview_request_contains_configured_credential(document):
                raise AIAssistantContractError("ai_assistant_protocol_invalid")
            validate_result(
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
        raise AIAssistantContractError("invalid_request")
    content_length = context.request.content_length
    if content_length is not None and content_length > MAX_REQUEST_BYTES:
        raise AIAssistantContractError(
            "invalid_request",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
        )
    try:
        raw = await context.request.get_data(cache=False)
    except Exception as exc:
        too_large = getattr(exc, "code", None) == int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        raise AIAssistantContractError(
            "invalid_request",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE if too_large else HTTPStatus.BAD_REQUEST,
        ) from exc
    if not isinstance(raw, bytes):
        raise AIAssistantContractError("invalid_request")
    return raw


def _success_response(context: RouteContext, document: Any) -> Any:
    response = context.json_response(document)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def _error_response(
    context: RouteContext,
    runtime: Any,
    exc: Exception,
    *,
    post_attempted: bool,
    capability_read: bool,
) -> Any:
    code, status, retryable, outcome = _map_error(
        runtime,
        exc,
        post_attempted=post_attempted,
        capability_read=capability_read,
    )
    response = context.json_response(
        error_document(
            code,
            safe_message=_SAFE_MESSAGES[code],
            retryable=retryable,
            outcome=outcome,
            correlation_id=f"assistant-{uuid4().hex}",
        ),
        status,
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def _map_error(
    runtime: Any,
    exc: Exception,
    *,
    post_attempted: bool,
    capability_read: bool,
) -> tuple[str, HTTPStatus, bool, str]:
    if isinstance(exc, AIAssistantContractError):
        if exc.code in _SAFE_MESSAGES:
            return exc.code, exc.status, exc.retryable, exc.outcome
        return (
            "provider_unavailable" if capability_read else "provider_result_invalid",
            HTTPStatus.BAD_GATEWAY,
            False,
            "unknown" if post_attempted else "not_started",
        )
    if isinstance(exc, (HandleRequiresServerConnectionError, KeyError)):
        return "server_not_connected", HTTPStatus.CONFLICT, False, "not_started"
    if isinstance(exc, TimeoutError):
        return (
            (
                "provider_timeout",
                HTTPStatus.GATEWAY_TIMEOUT,
                False,
                "unknown",
            )
            if post_attempted
            else (
                "server_offline",
                HTTPStatus.SERVICE_UNAVAILABLE,
                True,
                "not_started",
            )
        )
    if isinstance(exc, ServerOfflineError):
        return "server_offline", HTTPStatus.SERVICE_UNAVAILABLE, True, "not_started"
    if isinstance(exc, ServerClientError):
        if exc.code == "server_response_timeout":
            return (
                (
                    "provider_timeout",
                    HTTPStatus.GATEWAY_TIMEOUT,
                    False,
                    "unknown",
                )
                if post_attempted
                else (
                    "server_offline",
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    True,
                    "not_started",
                )
            )
        if exc.code in _SAFE_MESSAGES:
            return _central_error_semantics(
                exc.code,
                exc.status_code,
                post_attempted=post_attempted,
            )
        if runtime._is_server_connectivity_error(exc):
            runtime._mark_current_server_channel_failure(error=exc)
            return (
                "server_offline",
                HTTPStatus.SERVICE_UNAVAILABLE,
                not post_attempted,
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
        if exc.status_code == 402:
            return "plan_limit_reached", HTTPStatus.PAYMENT_REQUIRED, False, "not_started"
        if exc.status_code == 429:
            return "rate_limited", HTTPStatus.TOO_MANY_REQUESTS, True, "not_started"
        if exc.status_code == 400:
            return "invalid_request", HTTPStatus.BAD_REQUEST, False, "not_started"
    return (
        "provider_unavailable" if capability_read else "provider_result_invalid",
        HTTPStatus.BAD_GATEWAY,
        False,
        "unknown" if post_attempted else "not_started",
    )


def _central_error_semantics(
    code: str,
    status_code: int,
    *,
    post_attempted: bool,
) -> tuple[str, HTTPStatus, bool, str]:
    statuses = {
        "ai_assistant_disabled": HTTPStatus.SERVICE_UNAVAILABLE,
        "provider_unavailable": HTTPStatus.SERVICE_UNAVAILABLE,
        "central_credential_rejected": HTTPStatus.FAILED_DEPENDENCY,
        "permission_denied": HTTPStatus.FORBIDDEN,
        "plan_limit_reached": HTTPStatus.PAYMENT_REQUIRED,
        "subscription_inactive": HTTPStatus.PAYMENT_REQUIRED,
        "rate_limited": HTTPStatus.TOO_MANY_REQUESTS,
        "invalid_request": HTTPStatus.BAD_REQUEST,
        "provider_timeout": HTTPStatus.GATEWAY_TIMEOUT,
        "provider_refused": HTTPStatus.UNPROCESSABLE_ENTITY,
        "provider_failed": HTTPStatus.BAD_GATEWAY,
        "provider_result_invalid": HTTPStatus.BAD_GATEWAY,
        "ai_assistant_protocol_invalid": HTTPStatus.BAD_GATEWAY,
    }
    retryable = code == "rate_limited"
    outcome = {
        "provider_timeout": "unknown",
        "provider_failed": "unknown" if post_attempted else "not_started",
        "provider_refused": "not_completed",
        "provider_result_invalid": "not_completed",
        "ai_assistant_protocol_invalid": "unknown" if post_attempted else "not_started",
    }.get(code, "not_started")
    try:
        observed = HTTPStatus(status_code)
    except ValueError:
        observed = statuses[code]
    status = observed if 400 <= int(observed) <= 599 else statuses[code]
    return code, status, retryable, outcome


def _install_request_limit(app: Any) -> None:
    base_request_class = app.request_class
    if getattr(base_request_class, "_spl_ai_assistant_limit", False):
        return

    class AIAssistantRequestLimit(base_request_class):  # type: ignore[misc, valid-type]
        _spl_ai_assistant_limit = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            method = args[0] if args else kwargs.get("method")
            path = args[2] if len(args) > 2 else kwargs.get("path")
            if method == "POST" and path == AI_ASSISTANT_ROUTE:
                configured = kwargs.get("max_content_length")
                kwargs["max_content_length"] = (
                    MAX_REQUEST_BYTES if configured is None else min(int(configured), MAX_REQUEST_BYTES)
                )
            super().__init__(*args, **kwargs)

    app.request_class = AIAssistantRequestLimit


__all__ = ["register_ai_assistant_routes"]
