"""Authenticated route for additive guarded local-Run admission."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, cast

from spl.daemon.guarded_local_run import (
    GUARDED_LOCAL_RUN_ROUTE,
    MAX_BODY_BYTES,
    GuardedLocalRunError,
    guarded_admission_contains_value,
    guarded_local_run_error_document,
    parse_guarded_local_run_request,
)
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar


def install_guarded_run_request_limit(app: Any) -> None:
    """Apply the frozen body cap only while receiving the guarded POST body."""

    base_request_class = app.request_class
    if getattr(base_request_class, "_spl_guarded_run_limit", False):
        return

    class GuardedRunRequest(base_request_class):  # type: ignore[misc, valid-type]
        _spl_guarded_run_limit = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            method = args[0] if args else kwargs.get("method")
            path = args[2] if len(args) > 2 else kwargs.get("path")
            if method == "POST" and path == GUARDED_LOCAL_RUN_ROUTE:
                configured_limit = kwargs.get("max_content_length")
                kwargs["max_content_length"] = (
                    MAX_BODY_BYTES if configured_limit is None else min(int(configured_limit), MAX_BODY_BYTES)
                )
            super().__init__(*args, **kwargs)

    app.request_class = GuardedRunRequest


def register_guarded_run_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    """Register the closed route without changing the legacy Run parser."""

    route_app = cast(Any, app)

    async def guarded_route_no_store(response: Any) -> Any:
        if context.request.method == "POST" and context.request.path == GUARDED_LOCAL_RUN_ROUTE:
            response.headers["Cache-Control"] = "no-store"
        return response

    route_app.after_request(guarded_route_no_store)

    @app.post(GUARDED_LOCAL_RUN_ROUTE)
    async def admit_guarded_local_run() -> Any:
        if context.request.mimetype != "application/json":
            error = GuardedLocalRunError("request_invalid", HTTPStatus.BAD_REQUEST)
            return _error_response(context, error)
        content_length = context.request.content_length
        if content_length is not None and content_length > MAX_BODY_BYTES:
            error = GuardedLocalRunError(
                "body_too_large",
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
            return _error_response(context, error)

        try:
            raw = await context.request.get_data(cache=False)
        except Exception as exc:
            too_large = getattr(exc, "code", None) == int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            error = GuardedLocalRunError(
                "body_too_large" if too_large else "request_invalid",
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE if too_large else HTTPStatus.BAD_REQUEST,
            )
            return _error_response(context, error)
        if not isinstance(raw, bytes):
            error = GuardedLocalRunError("request_invalid", HTTPStatus.BAD_REQUEST)
            return _error_response(context, error)

        try:
            admission = parse_guarded_local_run_request(raw)
            if guarded_admission_contains_value(
                admission,
                context.local_api_token,
            ):
                raise GuardedLocalRunError(
                    "request_invalid",
                    HTTPStatus.BAD_REQUEST,
                )
            receipt = await context.run_blocking(
                runtime.start_guarded_local_run,
                admission,
            )
        except GuardedLocalRunError as error:
            return _error_response(context, error)
        except Exception:
            return _error_response(
                context,
                GuardedLocalRunError(
                    "admission_failed",
                    HTTPStatus.SERVICE_UNAVAILABLE,
                ),
            )

        response = context.json_response(receipt, HTTPStatus.ACCEPTED)
        response.headers["Cache-Control"] = "no-store"
        return response


def _error_response(context: RouteContext, error: GuardedLocalRunError) -> Any:
    response = context.json_response(
        guarded_local_run_error_document(error),
        error.status,
    )
    response.headers["Cache-Control"] = "no-store"
    return response


__all__ = ["install_guarded_run_request_limit", "register_guarded_run_routes"]
