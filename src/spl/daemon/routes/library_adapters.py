"""Authenticated bounded APIs for separately versioned Library Adapters."""

from __future__ import annotations

import asyncio
import json
from http import HTTPStatus
from typing import Any, cast

from spl.core.library_adapters import (
    LIBRARY_ADAPTER_CATALOG_CAPABILITY,
    LIBRARY_ADAPTER_SCHEMA_VERSION,
    MAX_ADAPTER_SOURCE_BYTES,
    LibraryAdapterContractError,
    domain_hash,
)
from spl.daemon.library_adapters import (
    commit_library_adapter_publication,
    prepare_library_adapter_publication,
)
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar
from spl.daemon.storage_base import validate_name

LIBRARY_ADAPTER_CATALOG_ROUTE = "/library-adapters"
LIBRARY_ADAPTER_PREFLIGHT_ROUTE = "/library-adapters/preflight"
MAX_LIBRARY_ADAPTER_REQUEST_BYTES = (2 * MAX_ADAPTER_SOURCE_BYTES) + (256 * 1024)
LIBRARY_ADAPTER_VALIDATION_TIMEOUT_SECONDS = 5.0


def install_library_adapter_request_limit(app: Any) -> None:
    """Bound publication bodies while Quart receives chunked requests."""

    base_request_class = app.request_class
    if getattr(base_request_class, "_spl_library_adapter_limit", False):
        return

    class LibraryAdapterRequestClass(base_request_class):  # type: ignore[misc, valid-type]
        _spl_library_adapter_limit = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            method = args[0] if args else kwargs.get("method")
            path = args[2] if len(args) > 2 else kwargs.get("path")
            if method == "POST" and path in {
                LIBRARY_ADAPTER_CATALOG_ROUTE,
                LIBRARY_ADAPTER_PREFLIGHT_ROUTE,
            }:
                configured = kwargs.get("max_content_length")
                kwargs["max_content_length"] = (
                    MAX_LIBRARY_ADAPTER_REQUEST_BYTES
                    if configured is None
                    else min(int(configured), MAX_LIBRARY_ADAPTER_REQUEST_BYTES)
                )
            super().__init__(*args, **kwargs)

    app.request_class = LibraryAdapterRequestClass


def register_library_adapter_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    route_app = cast(Any, app)

    async def no_store(response: Any) -> Any:
        if context.request.path.startswith(LIBRARY_ADAPTER_CATALOG_ROUTE):
            return _no_store(response)
        return response

    route_app.after_request(no_store)

    @app.get(LIBRARY_ADAPTER_CATALOG_ROUTE)
    async def list_library_adapters() -> Any:
        try:
            limit = context.optional_int_query("limit") or 50
            owner_id = await _resolved_owner(runtime, context)
            library = context.first_query_value("library")
            query = context.first_query_value("q", "query")
            direction = context.first_query_value("direction")
            cursor = context.first_query_value("cursor")
            target_machine_id = context.first_query_value("target_machine_id")
            if target_machine_id is not None:
                target_machine_id = validate_name(target_machine_id)
            server = await context.run_blocking(
                _connected_library_adapter_server,
                runtime,
            )
            if server is None:
                result = await context.run_blocking(
                    runtime.store.list_library_adapters,
                    owner_id=owner_id,
                    library=library,
                    query=query,
                    direction=direction,
                    limit=limit,
                    cursor=cursor,
                )
            else:
                execution_target, central_machine_id = _central_target_context(
                    runtime,
                    target_machine_id,
                )
                result = await context.run_blocking(
                    server.list_library_adapters,
                    owner=owner_id,
                    library=library,
                    query=query,
                    direction=direction,
                    limit=limit,
                    cursor=cursor,
                    target_machine_id=central_machine_id,
                    execution_target=execution_target,
                )
            if (
                not isinstance(result, dict)
                or result.get("schema") != "spl.library-adapter-catalog"
                or result.get("schema_version") != LIBRARY_ADAPTER_SCHEMA_VERSION
                or not isinstance(result.get("items"), list)
                or type(result.get("truncated")) is not bool
                or (result.get("next_cursor") is not None and not isinstance(result.get("next_cursor"), str))
            ):
                raise ValueError("Library Adapter catalog response is invalid")
            result = {
                "schema": "spl.library-adapter-catalog",
                "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
                "items": [
                    _browser_adapter_record(
                        item,
                        selected_target=("remote" if target_machine_id is not None else "local"),
                    )
                    for item in result["items"]
                ],
                "next_cursor": result["next_cursor"],
                "truncated": result["truncated"],
            }
            result["revision"] = domain_hash(
                "spl.library-adapter.catalog/v1",
                {
                    "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
                    "items": result["items"],
                    "next_cursor": result["next_cursor"],
                },
            )
            identity = runtime.daemon_identity
            result["instance"] = (
                None
                if identity is None
                else {
                    "instance_id": identity.instance_id,
                    "generation": identity.generation,
                }
            )
        except (ValueError, LibraryAdapterContractError) as exc:
            return _contract_error(context, exc, HTTPStatus.BAD_REQUEST)
        except Exception:
            return _unavailable(context)
        return _no_store(context.json_response(result))

    @app.get("/library-adapters/<adapter_id>")
    async def get_library_adapter(adapter_id: str) -> Any:
        try:
            result = await _library_adapter_detail(
                runtime,
                context,
                adapter_id=validate_name(adapter_id),
            )
        except KeyError:
            return _error(context, "adapter_not_found", HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            return _contract_error(context, exc, HTTPStatus.BAD_REQUEST)
        except Exception:
            return _unavailable(context)
        return _no_store(context.json_response(result))

    @app.get("/library-adapters/<adapter_id>/versions")
    async def list_library_adapter_versions(adapter_id: str) -> Any:
        try:
            limit = context.optional_int_query("limit") or 100
            result = await _library_adapter_versions(
                runtime,
                context,
                adapter_id=validate_name(adapter_id),
                limit=limit,
                cursor=context.first_query_value("cursor"),
            )
        except KeyError:
            return _error(context, "adapter_not_found", HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            return _contract_error(context, exc, HTTPStatus.BAD_REQUEST)
        except Exception:
            return _unavailable(context)
        return _no_store(
            context.json_response(
                {
                    "schema": "spl.library-adapter-versions",
                    "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
                    "items": result["items"],
                    "next_cursor": result["next_cursor"],
                    "truncated": result["truncated"],
                }
            )
        )

    @app.get("/library-adapters/<adapter_id>/versions/<version_id>")
    async def get_library_adapter_version(adapter_id: str, version_id: str) -> Any:
        try:
            result = await _library_adapter_exact_version(
                runtime,
                context,
                adapter_id=validate_name(adapter_id),
                version_id=validate_name(version_id),
                include_source=False,
            )
        except KeyError:
            return _error(context, "adapter_version_not_found", HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            return _contract_error(context, exc, HTTPStatus.BAD_REQUEST)
        except Exception:
            return _unavailable(context)
        return _no_store(context.json_response(result))

    @app.get("/library-adapters/<adapter_id>/versions/<version_id>/source")
    async def get_library_adapter_source(adapter_id: str, version_id: str) -> Any:
        try:
            result = await _library_adapter_exact_version(
                runtime,
                context,
                adapter_id=validate_name(adapter_id),
                version_id=validate_name(version_id),
                include_source=True,
            )
        except KeyError:
            return _error(context, "adapter_version_not_found", HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            return _contract_error(context, exc, HTTPStatus.BAD_REQUEST)
        except Exception:
            return _unavailable(context)
        return _no_store(
            context.json_response(
                {
                    "schema": "spl.library-adapter-source",
                    "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
                    "ref": {
                        key: result[key]
                        for key in (
                            "owner",
                            "library",
                            "name",
                            "version",
                            "adapter_id",
                            "adapter_version_id",
                            "content_hash",
                            "signature_hash",
                        )
                    },
                    "save_source": result["save_source"],
                    "load_source": result["load_source"],
                    "symbols": result["symbols"],
                    "dependencies": result["dependencies"],
                    "policy": result["policy"],
                }
            )
        )

    @app.post(LIBRARY_ADAPTER_PREFLIGHT_ROUTE)
    async def preflight_library_adapter() -> Any:
        document = await _publication_body(context)
        if not isinstance(document, dict):
            return document
        try:
            owner_id = await _resolved_owner(runtime, context)
            result = await asyncio.wait_for(
                context.run_blocking(
                    prepare_library_adapter_publication,
                    runtime.store,
                    document,
                    owner_id=owner_id,
                    library=context.first_query_value("library"),
                ),
                timeout=LIBRARY_ADAPTER_VALIDATION_TIMEOUT_SECONDS,
            )
        except LibraryAdapterContractError as exc:
            return _contract_error(context, exc, HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            return _contract_error(context, exc, HTTPStatus.BAD_REQUEST)
        except TimeoutError:
            return _error(context, "validation_timeout", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True)
        except Exception:
            return _unavailable(context)
        return _no_store(context.json_response(result))

    @app.post(LIBRARY_ADAPTER_CATALOG_ROUTE)
    async def publish_library_adapter() -> Any:
        document = await _publication_body(context)
        if not isinstance(document, dict):
            return document
        try:
            local_only = context.strict_query_bool("local_only", default=False)
            owner_id = await _resolved_owner(runtime, context)
            result = await asyncio.wait_for(
                context.run_blocking(
                    runtime.run_registry_mutation,
                    commit_library_adapter_publication,
                    runtime.store,
                    document,
                    owner_id=owner_id,
                    library=context.first_query_value("library"),
                ),
                timeout=LIBRARY_ADAPTER_VALIDATION_TIMEOUT_SECONDS,
            )
        except LibraryAdapterContractError as exc:
            return _contract_error(context, exc, HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            return _contract_error(context, exc, HTTPStatus.BAD_REQUEST)
        except TimeoutError:
            return _error(
                context,
                "publication_outcome_unknown",
                HTTPStatus.SERVICE_UNAVAILABLE,
                retryable=False,
            )
        except Exception:
            return _unavailable(context)
        enqueue = getattr(runtime, "enqueue_library_adapter_sync", None)
        if callable(enqueue) and not local_only:
            try:
                result["sync"] = await context.run_blocking(enqueue, result["ref"])
            except Exception:
                result["sync"] = {"state": "deferred", "reason": "sync_unavailable"}
        status = HTTPStatus.CREATED if result["created"] else HTTPStatus.OK
        return _no_store(context.json_response(result, status))


def _requested_target_machine_id(context: RouteContext) -> str | None:
    value = context.first_query_value("target_machine_id")
    return None if value is None else validate_name(value)


def _central_target_context(
    runtime: Any,
    requested_machine_id: str | None,
) -> tuple[str, str]:
    if requested_machine_id is not None:
        return "remote", validate_name(requested_machine_id)
    credentials = runtime.store.current_server_connection_credentials()
    if credentials is None or not credentials.get("machine_id"):
        raise RuntimeError("connected machine identity is unavailable")
    return "local", validate_name(str(credentials["machine_id"]))


def _browser_adapter_record(
    value: Any,
    *,
    selected_target: str,
) -> dict[str, Any]:
    """Translate central selected-target evidence to the browser DTO."""

    if not isinstance(value, dict):
        raise ValueError("Library Adapter record is invalid")
    result = dict(value)
    availability = result.get("availability")
    if isinstance(availability, dict) and set(availability) == {"local", "remote"}:
        return result
    if selected_target not in {"local", "remote"} or not isinstance(
        availability,
        dict,
    ):
        raise ValueError("Library Adapter availability is invalid")
    state = availability.get("state")
    reason = availability.get("reason_code")
    if state not in {"available", "unavailable", "unknown"}:
        raise ValueError("Library Adapter availability state is invalid")
    safe_reason = None
    if state != "available":
        try:
            safe_reason = validate_name(str(reason))
        except ValueError:
            safe_reason = "target_unavailable"
    effective = result.get("effective_policy")
    if not isinstance(effective, dict):
        raise ValueError("Library Adapter effective policy is invalid")
    local: dict[str, str | None]
    remote: dict[str, str | None]
    if selected_target == "local":
        local = {
            "state": "available" if state == "available" else "unavailable",
            "reason": None if state == "available" else safe_reason,
        }
        remote = (
            {
                "state": "requires_target_preflight",
                "reason": "remote_target_not_selected",
            }
            if effective.get("remote_custom") == "allow"
            else {
                "state": "unavailable",
                "reason": "remote_custom_code_denied",
            }
        )
    else:
        remote = {
            "state": (
                "available"
                if state == "available"
                else "requires_target_preflight"
                if state == "unknown"
                else "unavailable"
            ),
            "reason": None if state == "available" else safe_reason,
        }
        local = {
            "state": "unavailable",
            "reason": (
                "local_custom_code_denied" if effective.get("local_custom") != "allow" else "local_target_not_selected"
            ),
        }
    result["availability"] = {"local": local, "remote": remote}
    return result


def _connected_library_adapter_server(runtime: Any) -> Any | None:
    credentials = runtime.store.current_server_connection_credentials()
    if credentials is None or not credentials.get("remote_connection_id"):
        return None
    if not runtime._server_channel_is_live(  # noqa: SLF001 - daemon-owned route/runtime boundary
        credentials,
        supervise_heartbeat=False,
    ):
        return None
    server = runtime._server_client_for_credentials(credentials)  # noqa: SLF001
    version = server.get_server_version()
    declared = version.get("declared")
    contracts = declared.get("contracts") if isinstance(declared, dict) else None
    capabilities = contracts.get("daemon_server_capabilities") if isinstance(contracts, dict) else None
    if not isinstance(capabilities, list) or LIBRARY_ADAPTER_CATALOG_CAPABILITY not in capabilities:
        return None
    return server


async def _exact_namespace(runtime: Any, context: RouteContext) -> tuple[str | None, str | None]:
    owner = await _resolved_owner(runtime, context)
    library = context.first_query_value("library")
    if (owner is None) != (library is None):
        raise LibraryAdapterContractError(
            "adapter_namespace_invalid",
            "owner and library must be supplied together for a shared Adapter read",
        )
    return owner, None if library is None else validate_name(library)


async def _library_adapter_detail(
    runtime: Any,
    context: RouteContext,
    *,
    adapter_id: str,
) -> dict[str, Any]:
    owner, library = await _exact_namespace(runtime, context)
    requested_machine_id = _requested_target_machine_id(context)
    try:
        local = await context.run_blocking(
            runtime.store.get_library_adapter,
            adapter_id,
        )
        if owner is not None and (local.get("owner") != owner or local.get("library") != library):
            raise KeyError("local Adapter namespace does not match")
        return cast(dict[str, Any], local)
    except KeyError:
        if owner is None or library is None:
            raise
    server = await context.run_blocking(_connected_library_adapter_server, runtime)
    if server is None:
        raise KeyError("shared Adapter is unavailable")
    execution_target, central_machine_id = _central_target_context(
        runtime,
        requested_machine_id,
    )
    result = await context.run_blocking(
        server.get_library_adapter,
        owner,
        library,
        adapter_id,
        target_machine_id=central_machine_id,
        execution_target=execution_target,
    )
    return _browser_adapter_record(
        result,
        selected_target="remote" if requested_machine_id is not None else "local",
    )


async def _library_adapter_versions(
    runtime: Any,
    context: RouteContext,
    *,
    adapter_id: str,
    limit: int,
    cursor: str | None,
) -> dict[str, Any]:
    owner, library = await _exact_namespace(runtime, context)
    requested_machine_id = _requested_target_machine_id(context)
    try:
        local_current = await context.run_blocking(
            runtime.store.get_library_adapter,
            adapter_id,
        )
        if owner is not None and (local_current.get("owner") != owner or local_current.get("library") != library):
            raise KeyError("local Adapter namespace does not match")
        return cast(
            dict[str, Any],
            await context.run_blocking(
                runtime.store.list_library_adapter_versions,
                adapter_id,
                limit=limit,
                cursor=cursor,
            ),
        )
    except KeyError:
        if owner is None or library is None:
            raise
    if cursor is not None:
        raise ValueError("shared Adapter version pagination cursor is unsupported")
    server = await context.run_blocking(_connected_library_adapter_server, runtime)
    if server is None:
        raise KeyError("shared Adapter is unavailable")
    execution_target, central_machine_id = _central_target_context(
        runtime,
        requested_machine_id,
    )
    items = await context.run_blocking(
        server.list_library_adapter_versions,
        owner,
        library,
        adapter_id,
        limit=limit,
        target_machine_id=central_machine_id,
        execution_target=execution_target,
    )
    if not isinstance(items, list):
        raise ValueError("shared Adapter version response is invalid")
    return {
        "items": [
            _browser_adapter_record(
                item,
                selected_target=("remote" if requested_machine_id is not None else "local"),
            )
            for item in items
        ],
        "truncated": False,
        "next_cursor": None,
    }


async def _library_adapter_exact_version(
    runtime: Any,
    context: RouteContext,
    *,
    adapter_id: str,
    version_id: str,
    include_source: bool,
) -> dict[str, Any]:
    owner, library = await _exact_namespace(runtime, context)
    requested_machine_id = _requested_target_machine_id(context)
    try:
        local = await context.run_blocking(
            runtime.store.get_library_adapter_version,
            version_id,
            adapter_id=adapter_id,
            include_source=include_source,
        )
        if owner is not None and (local.get("owner") != owner or local.get("library") != library):
            raise KeyError("local Adapter namespace does not match")
        return cast(dict[str, Any], local)
    except KeyError:
        if owner is None or library is None:
            raise
    server = await context.run_blocking(_connected_library_adapter_server, runtime)
    if server is None:
        raise KeyError("shared Adapter version is unavailable")
    execution_target, central_machine_id = _central_target_context(
        runtime,
        requested_machine_id,
    )
    result = await context.run_blocking(
        server.get_library_adapter_version,
        owner,
        library,
        adapter_id,
        version_id,
        include_source=include_source,
        target_machine_id=central_machine_id,
        execution_target=execution_target,
    )
    return _browser_adapter_record(
        result,
        selected_target="remote" if requested_machine_id is not None else "local",
    )


async def _publication_body(context: RouteContext) -> dict[str, Any] | Any:
    if context.request.mimetype != "application/json":
        return _error(context, "request_invalid", HTTPStatus.BAD_REQUEST)
    content_length = context.request.content_length
    if content_length is not None and content_length > MAX_LIBRARY_ADAPTER_REQUEST_BYTES:
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
        or len(raw) > MAX_LIBRARY_ADAPTER_REQUEST_BYTES
        or context.local_api_token.encode("utf-8") in raw
    ):
        return _error(context, "request_invalid", HTTPStatus.BAD_REQUEST)
    try:
        document = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_closed_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
        return _error(context, "request_invalid", HTTPStatus.BAD_REQUEST)
    return document if isinstance(document, dict) else _error(context, "request_invalid", HTTPStatus.BAD_REQUEST)


async def _resolved_owner(runtime: Any, context: RouteContext) -> str | None:
    owner = context.first_query_value("owner", "owner_id")
    if owner is None:
        return None
    try:
        return str(await context.run_blocking(runtime.resolve_user_ref, owner))
    except Exception:
        raise LibraryAdapterContractError(
            "owner_unavailable",
            "the requested owner could not be resolved for this operation",
        ) from None


def _closed_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> Any:
    raise ValueError("non-finite JSON value")


def _contract_error(context: RouteContext, exc: Exception, status: HTTPStatus) -> Any:
    code = getattr(exc, "code", "request_invalid")
    message = str(exc)
    return _error(context, str(code), status, message=message)


def _unavailable(context: RouteContext) -> Any:
    return _error(
        context,
        "library_adapter_unavailable",
        HTTPStatus.SERVICE_UNAVAILABLE,
        retryable=True,
    )


def _error(
    context: RouteContext,
    code: str,
    status: HTTPStatus,
    *,
    message: str | None = None,
    retryable: bool = False,
) -> Any:
    error: dict[str, Any] = {"code": code, "retryable": retryable}
    if message:
        error["message"] = message
    return _no_store(
        context.json_response(
            {
                "schema": "spl.library-adapter-error",
                "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
                "error": error,
            },
            status,
        )
    )


def _no_store(response: Any) -> Any:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


__all__ = [
    "LIBRARY_ADAPTER_CATALOG_ROUTE",
    "LIBRARY_ADAPTER_PREFLIGHT_ROUTE",
    "MAX_LIBRARY_ADAPTER_REQUEST_BYTES",
    "install_library_adapter_request_limit",
    "register_library_adapter_routes",
]
