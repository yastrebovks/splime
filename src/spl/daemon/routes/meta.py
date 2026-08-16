"""Authenticated, additive metadata routes for trusted local clients."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from spl.daemon.ide_metadata import (
    DAEMON_METADATA_UNAVAILABLE_CODE,
    DaemonMetadataUnavailable,
    build_daemon_capabilities_document,
    load_installed_daemon_release,
)
from spl.daemon.routes._helpers import RouteContext, RouteRegistrar
from spl.daemon.runtime_adapter_registry import (
    RUNTIME_ADAPTER_REGISTRY_UNAVAILABLE_CODE,
    RuntimeAdapterRegistryUnavailable,
    build_runtime_adapter_registry_document,
)


def register_meta_routes(
    app: RouteRegistrar,
    *,
    runtime: Any,
    context: RouteContext,
) -> None:
    try:
        release_version = load_installed_daemon_release()
    except Exception:
        release_version = None

    @app.get("/meta/capabilities")
    async def daemon_capabilities() -> Any:
        try:
            home_lock = runtime.daemon_home_lock
            identity = runtime.daemon_identity
            if home_lock is None or not home_lock.is_acquired or home_lock.identity is not identity:
                identity = None
            document = build_daemon_capabilities_document(
                identity,
                release_version=release_version,
                forbidden_values=(context.local_api_token,),
            )
        except DaemonMetadataUnavailable:
            response = context.json_response(
                {
                    "error": "live daemon metadata is unavailable",
                    "code": DAEMON_METADATA_UNAVAILABLE_CODE,
                },
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        except Exception:
            response = context.json_response(
                {
                    "error": "live daemon metadata is unavailable",
                    "code": DAEMON_METADATA_UNAVAILABLE_CODE,
                },
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        else:
            response = context.json_response(document)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/meta/runtime-adapters")
    async def runtime_adapter_registry() -> Any:
        try:
            home_lock = runtime.daemon_home_lock
            identity = runtime.daemon_identity
            if home_lock is None or not home_lock.is_acquired or home_lock.identity is not identity:
                identity = None
            document = build_runtime_adapter_registry_document(
                identity,
                remote_custom_adapter_execution_enabled=runtime.allow_remote_custom_adapters,
                forbidden_values=(context.local_api_token,),
            )
        except RuntimeAdapterRegistryUnavailable:
            response = context.json_response(
                {
                    "error": "runtime adapter registry is unavailable",
                    "code": RUNTIME_ADAPTER_REGISTRY_UNAVAILABLE_CODE,
                },
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        except Exception:
            response = context.json_response(
                {
                    "error": "runtime adapter registry is unavailable",
                    "code": RUNTIME_ADAPTER_REGISTRY_UNAVAILABLE_CODE,
                },
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        else:
            response = context.json_response(document)
        response.headers["Cache-Control"] = "no-store"
        return response
