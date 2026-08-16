"""Authenticated, non-mutating Object preparation and validation routes."""

from __future__ import annotations

import asyncio
from http import HTTPStatus
from typing import Any, cast

from spl.daemon.prepared_validation import (
    MAX_PREPARATION_REQUEST_BYTES,
    MAX_REQUEST_BYTES,
    PREPARED_PUBLICATION_ROUTE,
    PREPARED_VALIDATION_ROUTE,
    VALIDATION_TIMEOUT_SECONDS,
    PreparedValidationError,
    parse_prepared_publication_request,
    parse_prepared_validation_request,
    prepare_validated_publication,
    prepared_publication_contains_value,
    prepared_validation_contains_value,
    prepared_validation_error_document,
    validate_prepared_request,
)
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar


def install_prepared_validation_request_limit(app: Any) -> None:
    """Bound the validation body while Quart receives chunked requests."""

    base_request_class = app.request_class
    if getattr(base_request_class, "_spl_prepared_validation_limit", False):
        return

    class PreparedValidationRequestClass(base_request_class):  # type: ignore[misc, valid-type]
        _spl_prepared_validation_limit = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            method = args[0] if args else kwargs.get("method")
            path = args[2] if len(args) > 2 else kwargs.get("path")
            if method == "POST" and path in {
                PREPARED_VALIDATION_ROUTE,
                PREPARED_PUBLICATION_ROUTE,
            }:
                configured_limit = kwargs.get("max_content_length")
                route_limit = MAX_PREPARATION_REQUEST_BYTES if path == PREPARED_PUBLICATION_ROUTE else MAX_REQUEST_BYTES
                kwargs["max_content_length"] = (
                    route_limit if configured_limit is None else min(int(configured_limit), route_limit)
                )
            super().__init__(*args, **kwargs)

    app.request_class = PreparedValidationRequestClass


def register_prepared_validation_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    """Register client-neutral v1 preparation/validation without a mutation lease."""

    route_app = cast(Any, app)

    async def prepared_validation_no_store(response: Any) -> Any:
        if context.request.path in {
            PREPARED_VALIDATION_ROUTE,
            PREPARED_PUBLICATION_ROUTE,
        }:
            return _no_store(response)
        return response

    route_app.after_request(prepared_validation_no_store)

    @app.post(PREPARED_VALIDATION_ROUTE)
    async def validate_prepared_object() -> Any:
        if context.request.mimetype != "application/json":
            return _error_response(
                context,
                PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST),
            )
        content_length = context.request.content_length
        if content_length is not None and content_length > MAX_REQUEST_BYTES:
            return _error_response(
                context,
                PreparedValidationError(
                    "body_too_large",
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                ),
            )
        try:
            raw = await context.request.get_data(cache=False)
        except Exception as exc:
            too_large = getattr(exc, "code", None) == int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            return _error_response(
                context,
                PreparedValidationError(
                    "body_too_large" if too_large else "request_invalid",
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE if too_large else HTTPStatus.BAD_REQUEST,
                ),
            )
        if not isinstance(raw, bytes):
            return _error_response(
                context,
                PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST),
            )

        try:
            request = parse_prepared_validation_request(raw)
            if prepared_validation_contains_value(request, context.local_api_token):
                raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
            if runtime.lifecycle.is_draining:
                raise PreparedValidationError("daemon_draining", HTTPStatus.CONFLICT)
            result = await asyncio.wait_for(
                context.run_blocking(
                    validate_prepared_request,
                    runtime.store,
                    request,
                ),
                timeout=VALIDATION_TIMEOUT_SECONDS,
            )
        except PreparedValidationError as error:
            return _error_response(context, error)
        except TimeoutError:
            return _error_response(
                context,
                PreparedValidationError(
                    "validation_timeout",
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=True,
                ),
            )
        except Exception:
            return _error_response(
                context,
                PreparedValidationError(
                    "validation_unavailable",
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=True,
                ),
            )
        return _no_store(context.json_response(result))

    @app.post(PREPARED_PUBLICATION_ROUTE)
    async def prepare_publication_object() -> Any:
        if context.request.mimetype != "application/json":
            return _error_response(
                context,
                PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST),
                operation="prepare_publication_object",
            )
        content_length = context.request.content_length
        if content_length is not None and content_length > MAX_PREPARATION_REQUEST_BYTES:
            return _error_response(
                context,
                PreparedValidationError(
                    "body_too_large",
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                ),
                operation="prepare_publication_object",
            )
        try:
            raw = await context.request.get_data(cache=False)
        except Exception as exc:
            too_large = getattr(exc, "code", None) == int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            return _error_response(
                context,
                PreparedValidationError(
                    "body_too_large" if too_large else "request_invalid",
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE if too_large else HTTPStatus.BAD_REQUEST,
                ),
                operation="prepare_publication_object",
            )
        if not isinstance(raw, bytes):
            return _error_response(
                context,
                PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST),
                operation="prepare_publication_object",
            )
        try:
            request = parse_prepared_publication_request(raw)
            if prepared_publication_contains_value(
                request,
                context.local_api_token,
            ):
                raise PreparedValidationError(
                    "request_invalid",
                    HTTPStatus.BAD_REQUEST,
                )
            if runtime.lifecycle.is_draining:
                raise PreparedValidationError(
                    "daemon_draining",
                    HTTPStatus.CONFLICT,
                )
            result = await asyncio.wait_for(
                context.run_blocking(
                    prepare_validated_publication,
                    runtime.store,
                    request,
                ),
                timeout=VALIDATION_TIMEOUT_SECONDS,
            )
        except PreparedValidationError as error:
            return _error_response(
                context,
                error,
                operation="prepare_publication_object",
            )
        except TimeoutError:
            return _error_response(
                context,
                PreparedValidationError(
                    "preparation_timeout",
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=True,
                ),
                operation="prepare_publication_object",
            )
        except Exception:
            return _error_response(
                context,
                PreparedValidationError(
                    "preparation_unavailable",
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=True,
                ),
                operation="prepare_publication_object",
            )
        return _no_store(context.json_response(result))


def _error_response(
    context: RouteContext,
    error: PreparedValidationError,
    *,
    operation: str = "validate_prepared_object",
) -> Any:
    return _no_store(
        context.json_response(
            prepared_validation_error_document(error, operation=operation),
            error.status,
        )
    )


def _no_store(response: Any) -> Any:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


__all__ = [
    "install_prepared_validation_request_limit",
    "register_prepared_validation_routes",
]
