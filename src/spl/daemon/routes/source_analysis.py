"""Authenticated, non-mutating deterministic source-analysis route.

The route is deliberately client-neutral: it projects the public framework
analysis contract without knowing about Jupyter or ``splime-jupyter``.  The
submitted source is bounded, never logged, never executed, and is not retained
by the daemon.
"""

from __future__ import annotations

import asyncio
import json
from http import HTTPStatus
from typing import Any, cast

from spl.core.source_analysis import SourceAnalysisContractError, analyze_selected_code
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar

SOURCE_ANALYSIS_ROUTE = "/objects/analyze-source"
SOURCE_ANALYSIS_CAPABILITY = "object.source.analyze"
SOURCE_ANALYSIS_CAPABILITY_VERSION = 1
MAX_SOURCE_ANALYSIS_REQUEST_BYTES = 5 * 1024 * 1024
SOURCE_ANALYSIS_TIMEOUT_SECONDS = 5.0


def install_source_analysis_request_limit(app: Any) -> None:
    """Bound chunked request bodies before Quart materializes them."""

    base_request_class = app.request_class
    if getattr(base_request_class, "_spl_source_analysis_limit", False):
        return

    class SourceAnalysisRequestClass(base_request_class):  # type: ignore[misc, valid-type]
        _spl_source_analysis_limit = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            method = args[0] if args else kwargs.get("method")
            path = args[2] if len(args) > 2 else kwargs.get("path")
            if method == "POST" and path == SOURCE_ANALYSIS_ROUTE:
                configured = kwargs.get("max_content_length")
                kwargs["max_content_length"] = (
                    MAX_SOURCE_ANALYSIS_REQUEST_BYTES
                    if configured is None
                    else min(int(configured), MAX_SOURCE_ANALYSIS_REQUEST_BYTES)
                )
            super().__init__(*args, **kwargs)

    app.request_class = SourceAnalysisRequestClass


def register_source_analysis_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    """Register the v1 pure framework-analysis projection."""

    del runtime
    route_app = cast(Any, app)

    async def no_store(response: Any) -> Any:
        if context.request.path == SOURCE_ANALYSIS_ROUTE:
            return _no_store(response)
        return response

    route_app.after_request(no_store)

    @app.post(SOURCE_ANALYSIS_ROUTE)
    async def analyze_source() -> Any:
        if context.request.mimetype != "application/json":
            return _error(context, "request_invalid", HTTPStatus.BAD_REQUEST)
        content_length = context.request.content_length
        if content_length is not None and content_length > MAX_SOURCE_ANALYSIS_REQUEST_BYTES:
            return _error(context, "body_too_large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        try:
            raw = await context.request.get_data(cache=False)
        except Exception as exc:
            too_large = getattr(exc, "code", None) == int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            return _error(
                context,
                "body_too_large" if too_large else "request_invalid",
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE if too_large else HTTPStatus.BAD_REQUEST,
            )
        if (
            not isinstance(raw, bytes)
            or not raw
            or len(raw) > MAX_SOURCE_ANALYSIS_REQUEST_BYTES
            or context.local_api_token.encode("utf-8") in raw
        ):
            return _error(context, "request_invalid", HTTPStatus.BAD_REQUEST)
        try:
            document = json.loads(
                raw.decode("utf-8", errors="strict"),
                object_pairs_hook=_closed_object,
                parse_constant=_reject_constant,
            )
            if not isinstance(document, dict):
                raise ValueError("request must be an object")
            result = await asyncio.wait_for(
                context.run_blocking(analyze_selected_code, document),
                timeout=SOURCE_ANALYSIS_TIMEOUT_SECONDS,
            )
        except SourceAnalysisContractError as exc:
            status = HTTPStatus.CONFLICT if exc.code == "analysis.schema" else HTTPStatus.BAD_REQUEST
            return _error(context, "request_incompatible" if status == 409 else "request_invalid", status)
        except TimeoutError:
            return _error(
                context,
                "analysis_timeout",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
            return _error(context, "request_invalid", HTTPStatus.BAD_REQUEST)
        except Exception:
            return _error(
                context,
                "analysis_unavailable",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        return _no_store(context.json_response(result))


def _closed_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> Any:
    raise ValueError("non-finite JSON value")


def _error(context: RouteContext, code: str, status: HTTPStatus) -> Any:
    return _no_store(
        context.json_response(
            {
                "schema": "spl.daemon.source-analysis-error/v1",
                "schema_version": 1,
                "error": {"code": code},
            },
            status,
        )
    )


def _no_store(response: Any) -> Any:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


__all__ = [
    "MAX_SOURCE_ANALYSIS_REQUEST_BYTES",
    "SOURCE_ANALYSIS_CAPABILITY",
    "SOURCE_ANALYSIS_CAPABILITY_VERSION",
    "SOURCE_ANALYSIS_ROUTE",
    "install_source_analysis_request_limit",
    "register_source_analysis_routes",
]
