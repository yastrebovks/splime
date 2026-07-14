"""Shared route helper surface for the local daemon HTTP API."""

from __future__ import annotations

import json
import secrets
import asyncio
from dataclasses import dataclass
from functools import wraps
from http import HTTPStatus
from typing import Any, Awaitable, Callable, Protocol, TypeVar, cast

from spl.daemon.remote_client import ServerClientError
from spl.daemon.runtime_dependencies import ServerClientProtocol
from spl.daemon.server_connection import (
    SERVER_PROXY_TIMEOUT_SECONDS,
    SERVER_UNREACHABLE_CODE,
    HandleRequiresServerConnectionError,
    ServerOfflineError,
)
from spl.daemon.store import split_object_function_ref

RouteHandler = TypeVar("RouteHandler", bound=Callable[..., Awaitable[Any]])


class RouteErrorDecorator(Protocol):
    def __call__(self, handler: RouteHandler) -> RouteHandler: ...


class RouteRegistrar(Protocol):
    def get(self, path: str) -> Callable[[RouteHandler], RouteHandler]: ...

    def post(self, path: str) -> Callable[[RouteHandler], RouteHandler]: ...

    def put(self, path: str) -> Callable[[RouteHandler], RouteHandler]: ...

    def delete(self, path: str) -> Callable[[RouteHandler], RouteHandler]: ...


@dataclass(frozen=True)
class RouteContext:
    """Shared request parsing, auth, and response helpers for route modules."""

    runtime: Any
    response_cls: Any
    request: Any
    local_api_token: str

    def json_response(
        self,
        value: Any,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> Any:
        body = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
        return self.response_cls(
            body,
            status=int(status),
            content_type="application/json; charset=utf-8",
        )

    async def run_blocking(self, func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(func, *args, **kwargs)

    def route_errors(
        self,
        handler: RouteHandler,
    ) -> RouteHandler:
        @wraps(handler)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await handler(*args, **kwargs)
            except HandleRequiresServerConnectionError as exc:
                return self.json_response(
                    {
                        "error": str(exc),
                        "code": exc.code,
                        "owner": exc.owner,
                    },
                    HTTPStatus.NOT_FOUND,
                )
            except KeyError as exc:
                return self.json_response({"error": str(exc)}, HTTPStatus.NOT_FOUND)
            except ValueError as exc:
                return self.json_response({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            except ServerOfflineError as exc:
                body = {"error": str(exc), "offline": True}
                code = getattr(exc, "code", None)
                if code:
                    body["code"] = code
                detail = getattr(exc, "detail", None)
                if detail:
                    body["detail"] = detail
                return self.json_response(
                    body,
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except ServerClientError as exc:
                body = {
                    "error": exc.message,
                    "upstream_status": exc.status_code,
                }
                if self.runtime._is_server_connectivity_error(exc):
                    self.runtime._mark_current_server_channel_failure(error=exc)
                    body["offline"] = True
                    body["code"] = SERVER_UNREACHABLE_CODE
                try:
                    status = HTTPStatus(exc.status_code)
                except ValueError:
                    status = HTTPStatus.BAD_GATEWAY
                if int(status) >= 500:
                    status = HTTPStatus.BAD_GATEWAY
                return self.json_response(body, status)
            except RuntimeError as exc:
                return self.json_response({"error": str(exc)}, HTTPStatus.CONFLICT)
            except Exception as exc:
                return self.json_response(
                    {"error": repr(exc)},
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )

        return cast(RouteHandler, wrapper)

    async def require_local_api_auth(self) -> Any:
        auth_header = self.request.headers.get("Authorization") or ""
        scheme, _, token = auth_header.partition(" ")
        if scheme.casefold() == "bearer" and token and secrets.compare_digest(token, self.local_api_token):
            return None
        return self.json_response(
            {"error": "missing or invalid local daemon API token"},
            HTTPStatus.UNAUTHORIZED,
        )

    async def read_json_body(self) -> dict[str, Any]:
        body = await self.request.get_json(silent=True)
        return body if isinstance(body, dict) else {}

    def first_query_value(self, *names: str) -> str | None:
        for name in names:
            value = self.request.args.get(name)
            if value is not None:
                return str(value)
        return None

    def optional_int_query(self, name: str) -> int | None:
        value = self.first_query_value(name)
        if value is None or value == "":
            return None
        return int(value)

    def query_bool(
        self,
        name: str,
        *,
        default: bool = False,
        accept_on: bool = True,
    ) -> bool:
        """Parse a compatibility boolean for read-only query options only.

        ``1``, ``true``, and ``yes`` are true; routes that historically did so
        may also accept ``on``. Unknown values retain the historical false-like
        behavior. Mutation and destructive routes must use
        :meth:`strict_query_bool` instead.
        """

        value = self.first_query_value(name)
        if value is None:
            return default
        normalized = value.lower()
        return normalized in {"1", "true", "yes"} or (accept_on and normalized == "on")

    def strict_query_bool(self, name: str, *, default: bool = False) -> bool:
        """Accept only case-insensitive ``1``, ``true``, ``0``, or ``false``."""

        value = self.first_query_value(name)
        if value is None:
            return default
        normalized = value.casefold()
        if normalized in {"1", "true"}:
            return True
        if normalized in {"0", "false"}:
            return False
        raise ValueError(f"query parameter {name!r} must be one of: 0, 1, false, true; received {value!r}")

    @staticmethod
    def strict_body_bool(
        body: dict[str, Any],
        name: str,
        *,
        default: bool = False,
    ) -> bool:
        """Accept only a real JSON boolean; use ``default`` when absent."""

        if name not in body:
            return default
        value = body[name]
        if isinstance(value, bool):
            return value
        raise ValueError(f"JSON field {name!r} must be a boolean; received {value!r}")

    def connected_server_client(self) -> tuple[dict[str, Any], ServerClientProtocol]:
        credentials = self.runtime._require_live_server_channel_credentials()
        return credentials, self.runtime._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
        )

    async def connected_server_client_async(self) -> tuple[dict[str, Any], ServerClientProtocol]:
        """Acquire a live server client without blocking the daemon event loop."""

        return cast(
            tuple[dict[str, Any], ServerClientProtocol],
            await self.run_blocking(self.connected_server_client),
        )

    def object_function_ref(self, name_or_id: str) -> tuple[str, str | None]:
        return split_object_function_ref(
            name_or_id,
            self.first_query_value("function", "entrypoint"),
        )

    async def object_from_local_or_server_async(
        self,
        name_or_id: str,
        *,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        version = self.optional_int_query("version")
        owner_id = self.first_query_value("owner", "owner_id")
        library = self.first_query_value("library")
        refresh = await self.run_blocking(
            self.runtime.refresh_server_object_if_available,
            name_or_id,
            version=version,
            owner_id=owner_id,
            library=library,
        )
        if refresh and refresh.get("current_version"):
            record = cast(
                dict[str, Any],
                self.runtime.store.get_object_version(
                    refresh["current_version"]["version_id"],
                    include_yaml=include_yaml,
                ),
            )
            resolved_from = refresh["current_version"].get("resolved_from")
            if isinstance(resolved_from, dict):
                record = {**record, "resolved_from": dict(resolved_from)}
            return record
        owner_id = self.runtime.resolve_user_ref(owner_id) if owner_id is not None else None
        return cast(
            dict[str, Any],
            self.runtime.store.get_object(
                name_or_id,
                version=version,
                owner_id=owner_id,
                library=library,
                include_yaml=include_yaml,
            ),
        )
