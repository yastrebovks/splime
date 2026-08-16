"""Authenticated routes for the guarded browser adapter Run broker."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

from spl.daemon.browser_adapter_run import (
    ADAPTER_RUN_ADMISSIONS_ROUTE,
    ADAPTER_RUN_PREFLIGHT_ROUTE,
    MAX_JSON_BODY_BYTES,
    AdapterRunError,
    adapter_run_error_document,
    parse_adapter_run_request,
    parse_finalize_request,
)
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar


def install_adapter_run_request_limit(app: Any) -> None:
    """Apply route-specific body limits without changing legacy endpoints."""

    base_request_class = app.request_class
    if getattr(base_request_class, "_spl_adapter_run_limit", False):
        return

    class AdapterRunRequest(base_request_class):  # type: ignore[misc, valid-type]
        _spl_adapter_run_limit = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            method = args[0] if args else kwargs.get("method")
            path = args[2] if len(args) > 2 else kwargs.get("path")
            limit = None
            if method == "POST" and path in {ADAPTER_RUN_PREFLIGHT_ROUTE, ADAPTER_RUN_ADMISSIONS_ROUTE}:
                limit = MAX_JSON_BODY_BYTES
            elif (
                method == "POST"
                and isinstance(path, str)
                and path.endswith("/finalize")
                and path.startswith(ADAPTER_RUN_ADMISSIONS_ROUTE + "/")
            ):
                limit = 4096
            elif (
                method == "PUT"
                and isinstance(path, str)
                and path.startswith(ADAPTER_RUN_ADMISSIONS_ROUTE + "/")
                and "/inputs/" in path
            ):
                # The parser separately enforces the exact declaration.  The
                # transport cap prevents an unbounded body before it can do so.
                from spl.core.runtime_port_adapters import MAX_RUNTIME_INPUT_BYTES

                limit = MAX_RUNTIME_INPUT_BYTES
            if limit is not None:
                configured = kwargs.get("max_content_length")
                kwargs["max_content_length"] = limit if configured is None else min(int(configured), limit)
            super().__init__(*args, **kwargs)

    app.request_class = AdapterRunRequest


def register_adapter_run_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    route_app = cast(Any, app)
    broker = runtime.browser_adapter_runs

    async def no_store(response: Any) -> Any:
        path = context.request.path
        if (
            path in {ADAPTER_RUN_PREFLIGHT_ROUTE, ADAPTER_RUN_ADMISSIONS_ROUTE}
            or path.startswith(ADAPTER_RUN_ADMISSIONS_ROUTE + "/")
            or (path.startswith("/runs/") and ("/adapter-result" in path or "/adapter-artifacts/" in path))
        ):
            response.headers["Cache-Control"] = "no-store"
        return response

    route_app.after_request(no_store)

    @app.post(ADAPTER_RUN_PREFLIGHT_ROUTE)
    async def adapter_run_preflight() -> Any:
        try:
            document = parse_adapter_run_request(await _json_bytes(context, stage="preflight"), create=False)
            _reject_token(document, context.local_api_token, stage="preflight")
            result = await context.run_blocking(broker.preflight, document)
            return _json(context, result, HTTPStatus.OK)
        except AdapterRunError as error:
            return _error(context, error)
        except Exception:
            return _error(
                context,
                AdapterRunError("target_unavailable", "preflight", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True),
            )

    @app.post(ADAPTER_RUN_ADMISSIONS_ROUTE)
    async def create_adapter_run_admission() -> Any:
        try:
            document = parse_adapter_run_request(await _json_bytes(context, stage="admission"), create=True)
            _reject_token(document, context.local_api_token, stage="admission")
            result, created = await context.run_blocking(broker.create, document)
            return _json(context, result, HTTPStatus.CREATED if created else HTTPStatus.OK)
        except AdapterRunError as error:
            return _error(context, error)
        except Exception:
            return _error(
                context,
                AdapterRunError("finalize_failed", "admission", HTTPStatus.SERVICE_UNAVAILABLE),
            )

    @app.get(ADAPTER_RUN_ADMISSIONS_ROUTE + "/<request_id>")
    async def reconcile_adapter_run_admission(request_id: str) -> Any:
        try:
            return _json(context, await context.run_blocking(broker.reconcile, request_id), HTTPStatus.OK)
        except AdapterRunError as error:
            return _error(context, error)
        except Exception:
            return _error(
                context,
                AdapterRunError("outcome_unknown", "reconcile", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True),
            )

    @app.put(ADAPTER_RUN_ADMISSIONS_ROUTE + "/<request_id>/inputs/<name>")
    async def upload_adapter_run_input(request_id: str, name: str) -> Any:
        try:
            content_length = context.request.content_length
            digest = context.request.headers.get("X-Spl-Content-SHA256")
            media_type = context.request.headers.get("Content-Type")

            async def chunks() -> Any:
                async for chunk in context.request.body:
                    yield bytes(chunk)

            result = await broker.upload(
                request_id,
                name,
                chunks(),
                content_length=content_length,
                digest_header=digest,
                media_type=media_type,
            )
            return _json(context, result, HTTPStatus.OK)
        except AdapterRunError as error:
            return _error(context, error)
        except Exception:
            return _error(context, AdapterRunError("request_invalid", "upload", HTTPStatus.BAD_REQUEST))

    @app.post(ADAPTER_RUN_ADMISSIONS_ROUTE + "/<request_id>/finalize")
    async def finalize_adapter_run(request_id: str) -> Any:
        try:
            parse_finalize_request(await _json_bytes(context, stage="finalize", maximum=4096), request_id=request_id)
            result, created = await context.run_blocking(broker.finalize, request_id)
            return _json(context, result, HTTPStatus.ACCEPTED if created else HTTPStatus.OK)
        except AdapterRunError as error:
            return _error(context, error)
        except Exception:
            return _error(context, AdapterRunError("finalize_failed", "finalize", HTTPStatus.SERVICE_UNAVAILABLE))

    @app.delete(ADAPTER_RUN_ADMISSIONS_ROUTE + "/<request_id>")
    async def cancel_adapter_run(request_id: str) -> Any:
        try:
            return _json(context, await context.run_blocking(broker.cancel, request_id), HTTPStatus.OK)
        except AdapterRunError as error:
            return _error(context, error)
        except Exception:
            return _error(
                context,
                AdapterRunError("outcome_unknown", "reconcile", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True),
            )

    @app.get("/runs/<run_id>/adapter-result")
    async def adapter_run_result(run_id: str) -> Any:
        try:
            return _json(context, await context.run_blocking(broker.project_result, run_id), HTTPStatus.OK)
        except AdapterRunError as error:
            return _error(context, error)
        except Exception:
            return _error(
                context,
                AdapterRunError("result_unavailable", "result", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True),
            )

    @app.get("/runs/<run_id>/adapter-artifacts/<handle>")
    async def adapter_run_artifact(run_id: str, handle: str) -> Any:
        try:
            data, media_type, logical_name = await context.run_blocking(broker.artifact_bytes, run_id, handle)
            response = context.response_cls(
                data,
                status=int(HTTPStatus.OK),
                content_type=media_type,
                headers={
                    "Content-Disposition": f"attachment; filename*=UTF-8''{quote(logical_name, safe='')}",
                    "Content-Length": str(len(data)),
                    "Cache-Control": "no-store",
                    "X-Content-Type-Options": "nosniff",
                },
            )
            return response
        except AdapterRunError as error:
            return _error(context, error)
        except Exception:
            return _error(
                context,
                AdapterRunError("result_unavailable", "download", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True),
            )


async def _json_bytes(context: RouteContext, *, stage: str, maximum: int = MAX_JSON_BODY_BYTES) -> bytes:
    if context.request.mimetype != "application/json":
        raise AdapterRunError("request_invalid", stage, HTTPStatus.BAD_REQUEST)
    length = context.request.content_length
    if length is not None and length > maximum:
        raise AdapterRunError("body_too_large", stage, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    try:
        raw = await context.request.get_data(cache=False)
    except Exception as exc:
        too_large = getattr(exc, "code", None) == int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        raise AdapterRunError(
            "body_too_large" if too_large else "request_invalid",
            stage,
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE if too_large else HTTPStatus.BAD_REQUEST,
        ) from exc
    if not isinstance(raw, bytes) or len(raw) > maximum:
        raise AdapterRunError("body_too_large", stage, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    return raw


def _reject_token(document: Any, token: str, *, stage: str) -> None:
    if token and token in repr(document):
        raise AdapterRunError("request_invalid", stage, HTTPStatus.BAD_REQUEST)


def _json(context: RouteContext, document: Any, status: HTTPStatus) -> Any:
    response = context.json_response(document, status)
    response.headers["Cache-Control"] = "no-store"
    return response


def _error(context: RouteContext, error: AdapterRunError) -> Any:
    return _json(context, adapter_run_error_document(error), error.status)


__all__ = ["install_adapter_run_request_limit", "register_adapter_run_routes"]
