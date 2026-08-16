"""Authenticated additive daemon lifecycle routes."""

from __future__ import annotations

import json
from http import HTTPStatus
from typing import Any, cast

from spl.daemon.lifecycle import LifecycleError
from spl.daemon.lifecycle_startup import STARTUP_PROOF_HEADER, StartupBindingError
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar

LIFECYCLE_STATUS_ROUTE = "/lifecycle/status"
LIFECYCLE_DRAIN_ROUTE = "/lifecycle/drain"
LIFECYCLE_CANCEL_ROUTE = "/lifecycle/cancel"
LIFECYCLE_STARTUP_BINDING_ROUTE = "/lifecycle/startup-binding"
LIFECYCLE_REQUEST_MAX_BYTES = 8 * 1024
_LIFECYCLE_MUTATION_ROUTES = frozenset({LIFECYCLE_DRAIN_ROUTE, LIFECYCLE_CANCEL_ROUTE})
_LIFECYCLE_ROUTES = frozenset(
    {
        LIFECYCLE_STATUS_ROUTE,
        LIFECYCLE_DRAIN_ROUTE,
        LIFECYCLE_CANCEL_ROUTE,
        LIFECYCLE_STARTUP_BINDING_ROUTE,
    }
)


def install_lifecycle_request_limit(app: Any) -> None:
    """Bound lifecycle mutation bodies, including chunked requests."""

    base_request_class = app.request_class
    if getattr(base_request_class, "_spl_lifecycle_limit", False):
        return

    class LifecycleRequest(base_request_class):  # type: ignore[misc, valid-type]
        _spl_lifecycle_limit = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            method = args[0] if args else kwargs.get("method")
            path = args[2] if len(args) > 2 else kwargs.get("path")
            if method == "POST" and path in _LIFECYCLE_MUTATION_ROUTES:
                configured_limit = kwargs.get("max_content_length")
                kwargs["max_content_length"] = (
                    LIFECYCLE_REQUEST_MAX_BYTES
                    if configured_limit is None
                    else min(int(configured_limit), LIFECYCLE_REQUEST_MAX_BYTES)
                )
            super().__init__(*args, **kwargs)

    app.request_class = LifecycleRequest


def register_lifecycle_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    route_app = cast(Any, app)

    async def lifecycle_route_no_store(response: Any) -> Any:
        if context.request.path in _LIFECYCLE_ROUTES:
            return _no_store(response)
        return response

    route_app.after_request(lifecycle_route_no_store)

    @app.get(LIFECYCLE_STATUS_ROUTE)
    async def lifecycle_status() -> Any:
        try:
            document = runtime.lifecycle_status()
        except LifecycleError as error:
            return _lifecycle_error(context, runtime, error)
        return _no_store(context.json_response(document))

    @app.get(LIFECYCLE_STARTUP_BINDING_ROUTE)
    async def lifecycle_startup_binding() -> Any:
        try:
            document = runtime.consume_lifecycle_startup_receipt(
                context.request.headers.get(STARTUP_PROOF_HEADER),
            )
        except StartupBindingError:
            error = LifecycleError(
                "startup_binding_unavailable",
                HTTPStatus.CONFLICT,
                operation="startup_binding",
            )
            return _lifecycle_error(context, runtime, error)
        return _no_store(context.json_response(document))

    @app.post(LIFECYCLE_DRAIN_ROUTE)
    async def lifecycle_drain() -> Any:
        try:
            document = await _closed_json_body(context)
            receipt = await context.run_blocking(
                runtime.request_lifecycle_drain,
                document,
                supervisor_proof=context.request.headers.get(STARTUP_PROOF_HEADER),
            )
        except LifecycleError as error:
            return _lifecycle_error(context, runtime, error)
        return _no_store(context.json_response(receipt, HTTPStatus.ACCEPTED))

    @app.post(LIFECYCLE_CANCEL_ROUTE)
    async def lifecycle_cancel() -> Any:
        try:
            document = await _closed_json_body(context)
            receipt = await context.run_blocking(
                runtime.cancel_lifecycle_drain,
                document,
                supervisor_proof=context.request.headers.get(STARTUP_PROOF_HEADER),
            )
        except LifecycleError as error:
            return _lifecycle_error(context, runtime, error)
        return _no_store(context.json_response(receipt))


async def _closed_json_body(context: RouteContext) -> dict[str, Any]:
    if context.request.mimetype != "application/json":
        raise LifecycleError(
            "request_invalid",
            HTTPStatus.BAD_REQUEST,
            operation="request",
        )
    content_length = context.request.content_length
    if content_length is not None and content_length > LIFECYCLE_REQUEST_MAX_BYTES:
        raise LifecycleError(
            "body_too_large",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            operation="request",
        )
    try:
        raw = await context.request.get_data(cache=False)
    except Exception as exc:
        too_large = getattr(exc, "code", None) == int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        raise LifecycleError(
            "body_too_large" if too_large else "request_invalid",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE if too_large else HTTPStatus.BAD_REQUEST,
            operation="request",
        ) from exc
    if not isinstance(raw, bytes):
        raise LifecycleError(
            "request_invalid",
            HTTPStatus.BAD_REQUEST,
            operation="request",
        )
    if not raw or len(raw) > LIFECYCLE_REQUEST_MAX_BYTES:
        raise LifecycleError(
            "body_too_large" if len(raw) > LIFECYCLE_REQUEST_MAX_BYTES else "request_invalid",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE if len(raw) > LIFECYCLE_REQUEST_MAX_BYTES else HTTPStatus.BAD_REQUEST,
            operation="request",
        )
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_closed_json_object,
            parse_constant=lambda _value: _reject_json_constant(),
        )
    except LifecycleError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise LifecycleError(
            "request_invalid",
            HTTPStatus.BAD_REQUEST,
            operation="request",
        ) from exc
    if not isinstance(value, dict):
        raise LifecycleError(
            "request_invalid",
            HTTPStatus.BAD_REQUEST,
            operation="request",
        )
    return cast(dict[str, Any], value)


def _closed_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise LifecycleError(
                "request_invalid",
                HTTPStatus.BAD_REQUEST,
                operation="request",
            )
        document[key] = value
    return document


def _reject_json_constant() -> None:
    raise LifecycleError(
        "request_invalid",
        HTTPStatus.BAD_REQUEST,
        operation="request",
    )


def _lifecycle_error(
    context: RouteContext,
    runtime: Any,
    error: LifecycleError,
) -> Any:
    return _no_store(
        context.json_response(
            runtime.lifecycle.error_document(error),
            error.status,
        )
    )


def _no_store(response: Any) -> Any:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


__all__ = [
    "LIFECYCLE_CANCEL_ROUTE",
    "LIFECYCLE_DRAIN_ROUTE",
    "LIFECYCLE_STARTUP_BINDING_ROUTE",
    "LIFECYCLE_STATUS_ROUTE",
    "install_lifecycle_request_limit",
    "register_lifecycle_routes",
]
