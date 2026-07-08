"""Shared route helper surface for the local daemon HTTP API."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from functools import wraps
from http import HTTPStatus
from typing import Any, Awaitable, Callable, Protocol, TypeVar, cast

from spl.daemon.remote_client import ServerClientError
from spl.daemon.runtime_dependencies import ServerClientProtocol
from spl.daemon.server_connection import ServerOfflineError
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

    def route_errors(
        self,
        handler: RouteHandler,
    ) -> RouteHandler:
        @wraps(handler)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await handler(*args, **kwargs)
            except KeyError as exc:
                return self.json_response({"error": str(exc)}, HTTPStatus.NOT_FOUND)
            except ValueError as exc:
                return self.json_response({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            except ServerOfflineError as exc:
                return self.json_response(
                    {"error": str(exc), "offline": True},
                    HTTPStatus.SERVICE_UNAVAILABLE,
                )
            except ServerClientError as exc:
                try:
                    status = HTTPStatus(exc.status_code)
                except ValueError:
                    status = HTTPStatus.BAD_GATEWAY
                if int(status) >= 500:
                    status = HTTPStatus.BAD_GATEWAY
                return self.json_response(
                    {
                        "error": exc.message,
                        "upstream_status": exc.status_code,
                    },
                    status,
                )
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

    def query_bool(self, name: str, *, default: bool = False) -> bool:
        value = self.first_query_value(name)
        if value is None:
            return default
        return value.lower() in {"1", "true", "yes", "on"}

    def connected_server_client(self) -> tuple[dict[str, Any], ServerClientProtocol]:
        credentials = self.runtime._require_connected_server_credentials()
        return credentials, self.runtime._server_client_for_credentials(credentials)

    def object_function_ref(self, name_or_id: str) -> tuple[str, str | None]:
        return split_object_function_ref(
            name_or_id,
            self.first_query_value("function", "entrypoint"),
        )

    def object_from_local_or_server(
        self,
        name_or_id: str,
        *,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        version = self.optional_int_query("version")
        owner_id = self.first_query_value("owner", "owner_id")
        library = self.first_query_value("library")
        refresh = self.runtime.refresh_server_object_if_available(
            name_or_id,
            version=version,
            owner_id=owner_id,
            library=library,
        )
        if refresh and refresh.get("current_version"):
            return cast(
                dict[str, Any],
                self.runtime.store.get_object_version(
                    refresh["current_version"]["version_id"],
                    include_yaml=include_yaml,
                ),
            )
        return cast(
            dict[str, Any],
            self.runtime.store.get_object(
                name_or_id,
                version=version,
                include_yaml=include_yaml,
                owner_id=owner_id,
                library=library,
            ),
        )
