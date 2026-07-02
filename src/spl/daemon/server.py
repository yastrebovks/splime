"""Quart server for the local SPL daemon.

The daemon remains local-first: by default it binds to ``127.0.0.1`` and exposes
an API that can run arbitrary registered SPL objects in the selected Python
environment.  Do not bind it to an untrusted network.

Endpoints:

    GET  /health
    GET  /envs
    POST /envs
    GET  /objects
    POST /objects
    GET  /objects/<name-or-id>
    GET  /objects/<name-or-id>/versions
    GET  /objects/search?q=<text>
    GET  /environment-builds
    GET  /environment-builds/<spec-hash>
    POST /environment-builds/<spec-hash>/rebuild
    GET  /remote-signatures
    POST /remote-signatures/resolve
    POST /remote-decompositions/resolve
    POST /remote-nodes/run
    GET  /server/connection
    GET  /server/libraries
    POST /server/libraries
    GET  /server/libraries/<ref>
    PUT  /server/libraries/<ref>
    DELETE /server/libraries/<ref>
    GET  /server/libraries/<ref>/grants
    POST /server/libraries/<ref>/grants
    POST /server/libraries/<ref>/grants/<grantee>/revoke
    POST /server/libraries/<ref>/references
    POST /server/libraries/<ref>/copies
    DELETE /server/libraries/<ref>/entries/<name>
    POST /server/connect
    POST /server/disconnect
    GET  /runs
    POST /runs
    GET  /runs/<id>
    GET  /runs/<id>/result
    GET  /runs/<id>/artifacts
    GET  /runs/<id>/artifacts/<name>
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

from spl.daemon.docker_environment import DockerEnvironmentManager
from spl.daemon.docker_pool import DockerPool
from spl.daemon.environment import EnvironmentBuildError
from spl.daemon.environment import EnvironmentManager as VenvEnvironmentManager
from spl.daemon.environment_base import EnvironmentManagerProtocol
from spl.daemon.heartbeat_service import HeartbeatService
from spl.daemon.remote_client import (
    ServerClient,
    ServerClientError,
)
from spl.daemon.routes._helpers import RouteContext
from spl.daemon.routes.artifacts import register_artifact_routes
from spl.daemon.routes.diagnostics import register_diagnostics_routes
from spl.daemon.routes.envs import register_env_routes
from spl.daemon.routes.libraries import register_library_routes
from spl.daemon.routes.objects import register_object_routes
from spl.daemon.routes.remote import register_remote_routes
from spl.daemon.routes.runs import register_run_routes
from spl.daemon.routes.server_connections import register_server_connection_routes
from spl.daemon.runtime_backend import (
    RunContext,
    RuntimeBackendRegistry,
    RuntimeBackendServices,
)
from spl.daemon.runtime_dependencies import (
    DockerEnvironmentManagerProtocol,
    DockerPoolRunnerProtocol,
    HeartbeatsProtocol,
    ServerClientFactoryProtocol,
    ServerClientProtocol,
    ServerConnectionsProtocol,
    SyncVisibilityProtocol,
)
from spl.daemon.server_connection import (
    SERVER_OFFLINE_MESSAGE,
    ServerConnectionManager,
    ServerOfflineError,
)
from spl.daemon.services.sync import SyncVisibilityService
from spl.daemon.store import (
    DEFAULT_OBJECT_LIBRARY,
    DEFAULT_OBJECT_OWNER_ID,
    RegistryStore,
    split_object_function_ref,
    utc_now,
    validate_name,
    write_json,
)
from spl.daemon_client import (
    clear_daemon_endpoint,
    daemon_url,
    generate_daemon_api_token,
    write_daemon_endpoint,
)

LOCAL_RUN_TEXT_ARTIFACT_MAX_BYTES = 256 * 1024
LOCAL_RUN_TEXT_ARTIFACT_EXTENSIONS = {
    ".csv",
    ".htm",
    ".html",
    ".json",
    ".log",
    ".md",
    ".txt",
    ".tsv",
    ".yaml",
    ".yml",
}
DEFAULT_PORT_SCAN_LIMIT = 100
DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES = int(
    os.environ.get("SPL_DAEMON_INLINE_REMOTE_ARTIFACT_MAX_BYTES", str(5 * 1024 * 1024))
)
DEFAULT_INLINE_REMOTE_ARTIFACT_TOTAL_MAX_BYTES = int(
    os.environ.get(
        "SPL_DAEMON_INLINE_REMOTE_ARTIFACT_TOTAL_MAX_BYTES",
        str(20 * 1024 * 1024),
    )
)


EnvironmentManagerFactory = Callable[..., EnvironmentManagerProtocol]
DockerEnvironmentManagerFactory = Callable[..., DockerEnvironmentManagerProtocol]
DockerPoolFactory = Callable[..., DockerPoolRunnerProtocol]
RuntimeBackendRegistryFactory = Callable[
    [RuntimeBackendServices],
    RuntimeBackendRegistry,
]
SyncVisibilityFactory = Callable[[RegistryStore], SyncVisibilityProtocol]
ServerConnectionManagerFactory = Callable[
    [RegistryStore, ServerClientFactoryProtocol],
    ServerConnectionsProtocol,
]
HeartbeatServiceFactory = Callable[
    [RegistryStore, Callable[..., dict[str, Any]]],
    HeartbeatsProtocol,
]


def _default_environment_manager_factory(
    store: RegistryStore,
    **kwargs: Any,
) -> EnvironmentManagerProtocol:
    return VenvEnvironmentManager(store, **kwargs)


def _default_docker_environment_manager_factory(
    store: RegistryStore,
    **kwargs: Any,
) -> DockerEnvironmentManagerProtocol:
    return DockerEnvironmentManager(store, **kwargs)


def _default_docker_pool_factory(
    store: RegistryStore,
    docker_environment_manager: DockerEnvironmentManagerProtocol,
    *,
    daemon_base_url: str,
    pool_size: int,
    idle_timeout_seconds: float,
    prewarm: bool,
) -> DockerPoolRunnerProtocol:
    return DockerPool(
        store,
        docker_environment_manager,
        daemon_base_url=daemon_base_url,
        pool_size=pool_size,
        idle_timeout_seconds=idle_timeout_seconds,
        prewarm=prewarm,
    )


def _default_runtime_backend_registry_factory(
    services: RuntimeBackendServices,
) -> RuntimeBackendRegistry:
    return RuntimeBackendRegistry(services)


def _default_sync_visibility_factory(store: RegistryStore) -> SyncVisibilityProtocol:
    return SyncVisibilityService(store)


def _default_server_client_factory(
    base_url: str,
    machine_token: str,
    *,
    user_token: str | None = None,
) -> ServerClientProtocol:
    return ServerClient(base_url, machine_token, user_token=user_token)


def _default_server_connection_manager_factory(
    store: RegistryStore,
    server_client_factory: ServerClientFactoryProtocol,
) -> ServerConnectionManager:
    return ServerConnectionManager(store, server_client_factory)


def _default_heartbeat_service_factory(
    store: RegistryStore,
    sync_once: Callable[..., dict[str, Any]],
) -> HeartbeatService:
    return HeartbeatService(store, sync_once)


class DaemonRuntime:
    """Coordinates registry operations and worker subprocesses."""

    def __init__(
        self,
        store: RegistryStore,
        *,
        auto_build_envs: bool = True,
        env_build_timeout_seconds: float | None = None,
        env_stale_lock_seconds: float | None = None,
        daemon_base_url: str = "http://127.0.0.1:8765",
        docker_pool_size: int = 0,
        docker_idle_timeout_seconds: float = 300.0,
        docker_prewarm: bool = False,
        environment_manager: EnvironmentManagerProtocol | None = None,
        docker_environment_manager: DockerEnvironmentManagerProtocol | None = None,
        docker_pool: DockerPoolRunnerProtocol | None = None,
        runtime_backends: RuntimeBackendRegistry | None = None,
        sync_visibility: SyncVisibilityProtocol | None = None,
        server_connections: ServerConnectionsProtocol | None = None,
        heartbeat_service: HeartbeatsProtocol | None = None,
        server_client_factory: ServerClientFactoryProtocol = (
            _default_server_client_factory
        ),
        environment_manager_factory: EnvironmentManagerFactory = (
            _default_environment_manager_factory
        ),
        docker_environment_manager_factory: DockerEnvironmentManagerFactory = (
            _default_docker_environment_manager_factory
        ),
        docker_pool_factory: DockerPoolFactory = _default_docker_pool_factory,
        runtime_backend_registry_factory: RuntimeBackendRegistryFactory = (
            _default_runtime_backend_registry_factory
        ),
        sync_visibility_factory: SyncVisibilityFactory = (
            _default_sync_visibility_factory
        ),
        server_connection_manager_factory: ServerConnectionManagerFactory = (
            _default_server_connection_manager_factory
        ),
        heartbeat_service_factory: HeartbeatServiceFactory = (
            _default_heartbeat_service_factory
        ),
    ):
        self.store = store
        self.auto_build_envs = auto_build_envs
        self.daemon_base_url = daemon_base_url.rstrip("/")
        self.server_client_factory = server_client_factory
        manager_kwargs = {}
        if env_build_timeout_seconds is not None:
            manager_kwargs["build_timeout_seconds"] = env_build_timeout_seconds
        if env_stale_lock_seconds is not None:
            manager_kwargs["stale_lock_seconds"] = env_stale_lock_seconds
        self.environment_manager = environment_manager or environment_manager_factory(
            store,
            **manager_kwargs,
        )
        self.docker_environment_manager = (
            docker_environment_manager
            or docker_environment_manager_factory(store, **manager_kwargs)
        )
        self.docker_pool = docker_pool or docker_pool_factory(
            store,
            self.docker_environment_manager,
            daemon_base_url=self.daemon_base_url,
            pool_size=docker_pool_size,
            idle_timeout_seconds=docker_idle_timeout_seconds,
            prewarm=docker_prewarm,
        )
        backend_services = RuntimeBackendServices(
            environment_manager=self.environment_manager,
            docker_environment_manager=self.docker_environment_manager,
            docker_pool=self.docker_pool,
        )
        self.runtime_backends = runtime_backends or runtime_backend_registry_factory(
            backend_services
        )
        self.sync_visibility = sync_visibility or sync_visibility_factory(store)
        self._server_sync_lock = threading.Lock()
        self.server_connections = (
            server_connections
            or server_connection_manager_factory(store, server_client_factory)
        )
        self.heartbeat_service = heartbeat_service or heartbeat_service_factory(
            store,
            self.sync_once,
        )
        self.docker_pool.cleanup_stale_containers()
        self.restore_server_heartbeat()

    def _server_client(
        self,
        server_url: str,
        token: str,
        *,
        user_token: str | None,
    ) -> ServerClientProtocol:
        return self.server_connections.server_client(
            server_url,
            token,
            user_token=user_token,
        )

    def _server_client_for_credentials(
        self,
        credentials: dict[str, Any],
    ) -> ServerClientProtocol:
        return self.server_connections.server_client_for_credentials(credentials)

    def connect_server(
        self,
        *,
        server_url: str,
        machine_token: str,
        user_token: str,
        machine_id: str | None,
        display_name: str | None,
        capabilities: dict[str, Any],
        heartbeat_interval_seconds: float | None,
    ) -> dict[str, Any]:
        """Connect to the central daemon server and start lease heartbeats."""

        result = self.server_connections.connect_server(
            server_url=server_url,
            machine_token=machine_token,
            user_token=user_token,
            machine_id=machine_id,
            display_name=display_name,
            capabilities=capabilities,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )
        if not result.get("reused") and result.get("connection") is not None:
            self.start_server_heartbeat(result["connection"], token=machine_token)
        connection = result.get("connection") or {}
        if connection.get("owner_id") and connection.get("remote_connection_id"):
            credentials = self.store.get_server_connection_credentials(connection["id"])
            result["reconcile"] = self.reconcile_connected_objects(credentials)
        return result

    def _matching_server_connection(
        self,
        *,
        server_url: str,
        machine_token: str,
        user_token: str,
        machine_id: str | None,
    ) -> dict[str, Any] | None:
        """Return the active connection when the requested credentials match.

        Repeated ``SPLClient(machine_token=..., user_token=...)`` calls are a
        normal notebook workflow.  They should reuse the local daemon's existing
        lease instead of asking the server to connect the same token again.
        """

        return self.server_connections.matching_server_connection(
            server_url=server_url,
            machine_token=machine_token,
            user_token=user_token,
            machine_id=machine_id,
        )

    @staticmethod
    def _is_server_connectivity_error(exc: ServerClientError) -> bool:
        return ServerConnectionManager._is_server_connectivity_error(exc)

    @staticmethod
    def _offline_machine_id(
        machine_token: str,
        *,
        machine_id: str | None,
        display_name: str | None,
    ) -> str:
        return ServerConnectionManager._offline_machine_id(
            machine_token,
            machine_id=machine_id,
            display_name=display_name,
        )

    def _restore_pending_server_connection(
        self,
        credentials: dict[str, Any],
    ) -> dict[str, Any]:
        return self.server_connections.restore_pending_server_connection(credentials)

    def _require_connected_server_credentials(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.server_connections.require_connected_server_credentials(
            credentials
        )

    def _remote_connection_snapshot(self, connection: dict[str, Any]) -> dict[str, Any]:
        """Build a server-like connection payload from the local cached row."""

        return self.server_connections.remote_connection_snapshot(connection)

    def disconnect_server(self) -> dict[str, Any]:
        """Gracefully disconnect the current central-server lease."""

        credentials = self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError("active server connection is not found")

        self.stop_server_heartbeat(credentials["id"])
        return self.server_connections.disconnect_server(credentials)

    def restore_server_heartbeat(self) -> None:
        """Resume heartbeat loop for a persisted active connection, if present."""

        self.heartbeat_service.restore_server_heartbeat()

    def start_server_heartbeat(
        self,
        connection: dict[str, Any],
        *,
        token: str,
    ) -> None:
        """Start one background heartbeat loop and stop older loops."""

        self.heartbeat_service.start_server_heartbeat(connection, token=token)

    def stop_server_heartbeat(self, connection_id: str) -> None:
        """Stop the heartbeat loop for one local connection."""

        self.heartbeat_service.stop_server_heartbeat(connection_id)

    def enqueue_object_sync(
        self,
        record: dict[str, Any],
        *,
        library: str | None = None,
        create_library: bool = False,
        library_display_name: str | None = None,
    ) -> dict[str, Any]:
        """Queue a freshly registered local object version for server sync."""

        version = self.store.get_object(
            record["id"],
            version=record["version"],
            include_yaml=True,
        )
        payload = self._object_sync_payload_for_version(version)
        if library:
            payload["library"] = validate_name(library)
        if create_library:
            payload["create_library"] = True
        if library_display_name:
            payload["library_display_name"] = str(library_display_name)
        return self.store.enqueue_sync_event("object_version", payload)

    def _object_sync_payload_for_version(
        self,
        version: dict[str, Any],
    ) -> dict[str, Any]:
        payload = {
            "name": version["name"],
            "entrypoint": version["entrypoint"],
            "env": version["env"],
            "kind": version.get("kind") or version.get("type") or "unknown",
            "description": version.get("description") or "",
            "version_label": version.get("version_label"),
            "yaml": version["yaml"],
            "metadata": version.get("metadata") or {},
            "distributions": version.get("distributions") or [],
            "runtime_config": version.get("runtime_config") or {"mode": "venv"},
            "source_object_id": version["id"],
            "source_version_id": version["version_id"],
        }
        if version.get("owner_id"):
            payload["owner_id"] = version["owner_id"]
        if version.get("library"):
            payload["library"] = version["library"]
        if version.get("content_hash"):
            payload["content_hash"] = version["content_hash"]
        return payload

    def reconcile_connected_objects(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Link local objects to the connected owner's server namespace."""

        credentials = credentials or self.store.current_server_connection_credentials()
        if (
            credentials is None
            or not credentials.get("remote_connection_id")
            or not credentials.get("owner_id")
        ):
            return {"skipped": True, "reason": "not_connected"}

        owner_id = validate_name(str(credentials["owner_id"]))
        report: dict[str, Any] = {
            "owner_id": owner_id,
            "rekey": self.store.rekey_local_placeholder_objects(owner_id),
            "objects": [],
            "conflicts": [],
            "pending_sync_events": [],
        }
        server = self._server_client_for_credentials(credentials)
        for identity in self.store.list_object_identities(owner_id=owner_id):
            if str(identity["name"]).startswith("server."):
                continue
            object_report = self._reconcile_connected_object(
                server,
                identity,
                owner_id=owner_id,
            )
            report["objects"].append(object_report)
            report["conflicts"].extend(object_report.get("conflicts") or [])
            report["pending_sync_events"].extend(
                object_report.get("pending_sync_events") or []
            )
        return report

    def _reconcile_connected_object(
        self,
        server: ServerClientProtocol,
        identity: dict[str, Any],
        *,
        owner_id: str,
    ) -> dict[str, Any]:
        library = validate_name(str(identity.get("library") or DEFAULT_OBJECT_LIBRARY))
        name = validate_name(str(identity["name"]))
        canonical_name = f"{owner_id}/{library}/{name}"
        report: dict[str, Any] = {
            "canonical_name": canonical_name,
            "status": "checked",
            "conflicts": [],
            "imported_versions": [],
            "pending_sync_events": [],
        }
        try:
            remote_current = server.get_object(
                name,
                include_yaml=False,
                owner_id=owner_id,
                library=library,
            )
        except ServerClientError as exc:
            if exc.status_code == 404:
                report["status"] = "local_only"
                report["pending_sync_events"] = self._enqueue_local_only_object_syncs(
                    owner_id=owner_id,
                    library=library,
                    name=name,
                )
                return report
            raise

        remote_object_id = remote_current.get("id")
        self.store.link_object_remote_identity(
            owner_id=owner_id,
            library=library,
            name=name,
            remote_owner_id=remote_current.get("owner_id") or owner_id,
            remote_object_id=remote_object_id,
            source_object_name=remote_current.get("name") or name,
        )
        remote_versions = server.list_object_versions(
            name,
            include_yaml=True,
            owner_id=owner_id,
            library=library,
        )
        if not remote_versions and remote_current.get("yaml"):
            remote_versions = [remote_current]
        self._ensure_server_object_envs(remote_versions)

        local_versions = self.store.list_object_versions(
            name,
            owner_id=owner_id,
            library=library,
        )
        local_by_hash = {
            item["content_hash"]: item
            for item in local_versions
            if item.get("content_hash")
        }
        local_by_version = {int(item["version"]): item for item in local_versions}
        remote_hashes = {
            item.get("content_hash")
            for item in remote_versions
            if item.get("content_hash")
        }
        remote_max_version = max(
            (int(item.get("version") or 0) for item in remote_versions),
            default=0,
        )
        conflict_local_version_ids: set[str] = set()

        for remote_version in sorted(
            remote_versions,
            key=lambda item: int(item.get("version") or 0),
        ):
            remote_hash = remote_version.get("content_hash")
            remote_number = int(remote_version.get("version") or 0)
            local_at_version = local_by_version.get(remote_number)
            if (
                remote_hash
                and local_at_version is not None
                and local_at_version.get("content_hash") != remote_hash
                and remote_hash not in local_by_hash
            ):
                conflict = self._record_object_reconcile_conflict(
                    owner_id=owner_id,
                    library=library,
                    name=name,
                    local_version=local_at_version,
                    remote_version=remote_version,
                    remote_object_id=remote_object_id,
                )
                report["conflicts"].append(conflict)
                conflict_local_version_ids.add(local_at_version["version_id"])
                continue
            if remote_hash and remote_hash in local_by_hash:
                self.store.link_object_remote_identity(
                    owner_id=owner_id,
                    library=library,
                    name=name,
                    remote_owner_id=remote_version.get("owner_id") or owner_id,
                    remote_object_id=remote_version.get("id") or remote_object_id,
                    source_object_name=remote_version.get("name") or name,
                )
            imported = self._import_reconcile_remote_version(
                remote_version,
                owner_id=owner_id,
                library=library,
                remote_object_id=remote_object_id,
            )
            report["imported_versions"].append(imported["version_id"])

        for local_version in self.store.list_object_versions(
            name,
            owner_id=owner_id,
            library=library,
        ):
            if local_version.get("remote_version_id"):
                continue
            if local_version.get("version_id") in conflict_local_version_ids:
                continue
            if local_version.get("content_hash") in remote_hashes:
                continue
            if int(local_version["version"]) <= remote_max_version and remote_hashes:
                continue
            event = self._enqueue_object_version_sync_once(local_version)
            report["pending_sync_events"].append(event)
        report["status"] = "linked"
        return report

    def _import_reconcile_remote_version(
        self,
        remote_version: dict[str, Any],
        *,
        owner_id: str,
        library: str,
        remote_object_id: str | None,
    ) -> dict[str, Any]:
        yaml_text = remote_version.get("yaml")
        if not yaml_text:
            existing = self.store.get_object_by_remote_version(
                remote_version["version_id"],
                include_yaml=False,
            )
            if existing is not None:
                return existing
            raise RuntimeError(
                "server did not return YAML for object version "
                f"{remote_version.get('version_id')}"
            )
        return self.register_object(
            remote_version["name"],
            remote_version["entrypoint"],
            remote_version.get("env") or "default",
            yaml_text=yaml_text,
            owner_id=owner_id,
            library=library,
            description=remote_version.get("description") or "",
            version_label=remote_version.get("version_label"),
            origin="server",
            remote_owner_id=remote_version.get("owner_id") or owner_id,
            remote_object_id=remote_version.get("id") or remote_object_id,
            remote_version_id=remote_version.get("version_id"),
            source_object_name=remote_version["name"],
            runtime_config=remote_version.get("runtime_config"),
        )

    def _enqueue_local_only_object_syncs(
        self,
        *,
        owner_id: str,
        library: str,
        name: str,
    ) -> list[dict[str, Any]]:
        events = []
        for version in self.store.list_object_versions(
            name,
            owner_id=owner_id,
            library=library,
        ):
            if version.get("remote_version_id"):
                continue
            events.append(self._enqueue_object_version_sync_once(version))
        return events

    def _enqueue_object_version_sync_once(
        self,
        version: dict[str, Any],
    ) -> dict[str, Any]:
        full_version = self.store.get_object_version(
            version["version_id"],
            include_yaml=True,
        )
        return self.store.enqueue_object_version_sync_once(
            self._object_sync_payload_for_version(full_version)
        )

    def _record_object_reconcile_conflict(
        self,
        *,
        owner_id: str,
        library: str,
        name: str,
        local_version: dict[str, Any],
        remote_version: dict[str, Any],
        remote_object_id: str | None,
    ) -> dict[str, Any]:
        payload = {
            "canonical_name": f"{owner_id}/{library}/{name}",
            "owner_id": owner_id,
            "library": library,
            "name": name,
            "reason": "divergent_content",
            "local_object_id": local_version["id"],
            "local_version_id": local_version["version_id"],
            "local_version": local_version["version"],
            "local_content_hash": local_version.get("content_hash"),
            "remote_object_id": remote_object_id,
            "remote_version_id": remote_version.get("version_id"),
            "remote_version": remote_version.get("version"),
            "remote_content_hash": remote_version.get("content_hash"),
        }
        return self.store.record_object_conflict_once(payload)

    def build_machine_library_snapshot_manifest(self) -> tuple[str, list[dict[str, Any]]]:
        """Build a lightweight, stable manifest for the current local library."""

        items = []
        for record in self.store.list_objects().values():
            items.append(
                {
                    "library_slug": record.get("library") or DEFAULT_OBJECT_LIBRARY,
                    "name": record["name"],
                    "display_name": record.get("display_name") or record["name"],
                    "description": record.get("description") or "",
                    "local_object_id": record["id"],
                    "local_version_id": record["version_id"],
                    "version": record["version"],
                    "version_label": record.get("version_label"),
                    "entrypoint": record["entrypoint"],
                    "env": record.get("env"),
                    "kind": record.get("kind") or record.get("type") or "unknown",
                    "origin": record.get("origin") or "local",
                    "yaml_sha256": record["yaml_sha256"],
                    "content_hash": record.get("content_hash") or record["yaml_sha256"],
                    "metadata": record.get("metadata") or {},
                    "distributions": record.get("distributions") or [],
                    "runtime_config": record.get("runtime_config") or {"mode": "venv"},
                    "remote_owner_id": record.get("remote_owner_id")
                    or record.get("object_remote_owner_id"),
                    "remote_object_id": record.get("remote_object_id")
                    or record.get("object_remote_object_id"),
                    "remote_version_id": record.get("remote_version_id"),
                }
            )
        items.sort(
            key=lambda item: (
                item["library_slug"],
                item["name"],
                item.get("local_version_id") or "",
            )
        )
        manifest = {"format_version": 1, "items": items}
        snapshot_hash = hashlib.sha256(
            json.dumps(
                manifest,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return snapshot_hash, items

    def build_machine_library_snapshot_event(
        self,
        *,
        snapshot_hash: str,
        manifest_items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build a self-contained snapshot only after the manifest changed."""

        items = []
        for item in manifest_items:
            version = self.store.get_object_version(
                item["local_version_id"],
                include_yaml=True,
            )
            items.append({**item, "yaml": version["yaml"]})
        return {
            "id": uuid4().hex,
            "kind": "machine_library_snapshot",
            "payload": {
                "format_version": 1,
                "generated_at": utc_now(),
                "snapshot_hash": snapshot_hash,
                "items": items,
            },
        }

    def register_object(
        self,
        name: str,
        entrypoint: str,
        env: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Register an object and resolve remote-node signatures if needed."""

        kwargs = dict(kwargs)
        self._adopt_server_object_identity_for_publish(name, kwargs)
        return self.store.register_object(
            name,
            entrypoint,
            env,
            remote_signature_resolver=self.resolve_remote_signature,
            **kwargs,
        )

    def _adopt_server_object_identity_for_publish(
        self,
        name: str,
        kwargs: dict[str, Any],
    ) -> None:
        """Attach server identity before the first local row for a canonical key."""

        if kwargs.get("object_id") is not None or kwargs.get("remote_object_id"):
            return
        if kwargs.get("origin", "local") != "local":
            return

        credentials = self.store.current_server_connection_credentials()
        if (
            credentials is None
            or not credentials.get("remote_connection_id")
            or not credentials.get("owner_id")
            or credentials.get("status") != "connected"
        ):
            return

        owner_id = validate_name(str(kwargs.get("owner_id") or credentials["owner_id"]))
        library = validate_name(str(kwargs.get("library") or DEFAULT_OBJECT_LIBRARY))
        object_name = validate_name(name)

        try:
            self.store.get_object(
                object_name,
                owner_id=owner_id,
                library=library,
                include_yaml=False,
            )
            return
        except KeyError:
            pass

        server = self._server_client_for_credentials(credentials)
        try:
            remote_current = server.get_object(
                object_name,
                include_yaml=False,
                owner_id=owner_id,
                library=library,
            )
        except ServerClientError as exc:
            if exc.status_code == 404:
                return
            raise

        kwargs["owner_id"] = owner_id
        kwargs["library"] = library
        kwargs.setdefault("remote_owner_id", remote_current.get("owner_id") or owner_id)
        kwargs.setdefault("remote_object_id", remote_current.get("id"))
        kwargs.setdefault("source_object_name", remote_current.get("name") or object_name)

    def resolve_remote_signature(
        self,
        ref: dict[str, Any],
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        """Resolve a NodeRemote reference through the central server.

        The current framework stores only ``url/name/version`` in DNodeRemote.
        The daemon treats that as a server object reference and caches the
        resolved call signature locally, so metadata extraction and worker
        import can proceed without repeatedly asking the server.
        """

        normalized = self._normalize_remote_ref(ref)
        cached = self.store.get_remote_signature(normalized)
        if cached is not None and cached["status"] == "resolved" and not force:
            return cached["signature"]

        try:
            credentials = self._credentials_for_remote_ref(normalized)
            server = self._server_client_for_credentials(credentials)
            signature = server.object_signature(
                normalized["object_name"],
                version=self._remote_ref_version(normalized),
                owner_id=normalized.get("owner_id"),
                library=normalized.get("library"),
                function=normalized.get("function"),
            )
            signature["remote"] = {
                "url": normalized["server_url"],
                "name": normalized["object_name"],
                "function": normalized.get("function"),
                "requested_version": normalized.get("version"),
                "version_id": signature.get("version_id"),
                "owner_id": signature.get("owner_id"),
                "library": normalized.get("library")
                or (signature.get("library") or {}).get("slug"),
            }
            self.store.save_remote_signature(normalized, signature)
            return signature
        except Exception as exc:
            self.store.mark_remote_signature_unavailable(normalized, repr(exc))
            if cached is not None and cached.get("signature"):
                signature = dict(cached["signature"])
                signature["cache_status"] = "stale"
                signature["cache_error"] = repr(exc)
                return signature
            raise

    def resolve_remote_decomposition(self, ref: dict[str, Any]) -> dict[str, Any]:
        """Resolve a remote object graph through the connected central server."""

        normalized = self._normalize_remote_ref(ref)
        credentials = self._credentials_for_remote_ref(normalized)
        server = self._server_client_for_credentials(credentials)
        record = server.get_object(
            normalized["object_name"],
            version=self._remote_ref_version(normalized),
            include_yaml=False,
            owner_id=normalized.get("owner_id"),
            library=normalized.get("library"),
        )
        return {
            "decomposition": record.get("decomposition") or {},
            "object": record,
            "remote": {
                "url": normalized["server_url"],
                "name": normalized["object_name"],
                "function": normalized.get("function"),
                "requested_version": normalized.get("version"),
                "owner_id": normalized.get("owner_id") or record.get("owner_id"),
                "library": normalized.get("library")
                or (record.get("library") or {}).get("slug"),
                "version_id": record.get("version_id"),
                "object_id": record.get("id"),
            },
        }

    def _normalize_remote_ref(self, ref: dict[str, Any]) -> dict[str, Any]:
        raw_url = str(ref.get("server_url") or ref.get("url") or "").rstrip("/")
        url, path_owner, path_library = self._split_remote_url(raw_url)
        object_name = str(
            ref.get("object_name")
            or ref.get("object")
            or ref.get("name")
            or ""
        )
        raw_function = ref.get("function") or ref.get("entrypoint")
        if not object_name:
            raise ValueError("remote node requires object name")
        object_name, function = split_object_function_ref(object_name, raw_function)
        if not url:
            credentials = self.store.current_server_connection_credentials()
            if credentials is None:
                raise KeyError(
                    "active server connection is not found; remote Function/Pipeline "
                    "nodes require the daemon to be connected before resolution"
                )
            url = credentials["server_url"]
        return {
            "server_url": url,
            "owner_id": ref.get("owner_id") or ref.get("owner") or path_owner,
            "library": ref.get("library") or ref.get("library_slug") or path_library,
            "object_name": object_name,
            "function": function,
            "version": ref.get("version"),
            "version_id": ref.get("version_id"),
            "target_machine": ref.get("target_machine")
            or ref.get("target_machine_id"),
        }

    def _split_remote_url(self, raw_url: str) -> tuple[str, str | None, str | None]:
        """Extract optional owner/library from NodeRemote.url.

        The current framework gives NodeRemote only ``url/name/version``.  To
        keep it usable for shared libraries without changing framework code, the
        daemon accepts both a plain server URL and a scoped URL:
        ``https://splime.io/api/owners/alice/libraries/math``.
        """

        if not raw_url:
            return "", None, None
        parsed = urlparse(raw_url)
        parts = [part for part in parsed.path.split("/") if part]
        owner_id = None
        library = None
        if len(parts) >= 4 and parts[-4] == "owners" and parts[-2] == "libraries":
            owner_id = parts[-3]
            library = parts[-1]
            parts = parts[:-4]
        elif len(parts) >= 2 and parts[-2] == "libraries":
            library = parts[-1]
            parts = parts[:-2]
        base_path = "/" + "/".join(parts) if parts else ""
        base_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                base_path.rstrip("/"),
                "",
                "",
                "",
            )
        ).rstrip("/")
        return base_url, owner_id, library

    def _credentials_for_remote_ref(self, ref: dict[str, Any]) -> dict[str, Any]:
        credentials = self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError(
                "active server connection is not found; remote Function/Pipeline "
                "node requires daemon server credentials before it can resolve "
                "or run"
            )
        credentials = self._require_connected_server_credentials(credentials)
        if credentials["server_url"].rstrip("/") != ref["server_url"].rstrip("/"):
            raise KeyError(
                "remote node points to a different server than the active "
                f"daemon connection: {ref['server_url']}"
            )
        return credentials

    def _remote_ref_version(self, ref: dict[str, Any]) -> int | None:
        version = ref.get("version")
        if version is None:
            return None
        version_text = str(version)
        if version_text in {"", "latest", "current", "TODO"}:
            return None
        try:
            return int(version_text)
        except (TypeError, ValueError):
            return None

    def start_remote_run(
        self,
        object_name: str,
        *,
        target_machine: str | None = None,
        object_owner_id: str | None = None,
        library: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        object_version_id: str | None = None,
        function: str | None = None,
        correlation_id: str | None = None,
        parent_run_id: str | None = None,
        context: dict[str, Any] | None = None,
        offline_policy: str | None = None,
    ) -> dict[str, Any]:
        """Create a server-side run through the next sync handshake."""

        object_name, function = split_object_function_ref(object_name, function)
        credentials = self._require_connected_server_credentials()
        resolved_offline_policy = (
            offline_policy
            or (
                "fail_fast"
                if target_machine and target_machine != credentials["machine_id"]
                else "queue"
            )
        )
        if resolved_offline_policy not in {"queue", "wait", "fail_fast"}:
            raise ValueError("offline_policy must be 'queue', 'wait', or 'fail_fast'")

        payload: dict[str, Any] = {
            "object": object_name,
            "version": version,
            "version_id": object_version_id,
            "args": args or [],
            "kwargs": kwargs or {},
            "output": output,
            "timeout_seconds": timeout_seconds,
            "offline_policy": resolved_offline_policy,
        }
        if function is not None:
            payload["function"] = function
        if target_machine:
            payload["target_machine_id"] = target_machine
        if object_owner_id:
            payload["object_owner_id"] = object_owner_id
        if library:
            payload["library"] = library
        if correlation_id:
            payload["correlation_id"] = correlation_id
        if parent_run_id:
            payload["parent_run_id"] = parent_run_id
        if context:
            payload["context"] = context

        event_id = uuid4().hex
        event = {
            "id": event_id,
            "kind": "remote_run_request",
            "payload": payload,
        }
        if (
            resolved_offline_policy == "fail_fast"
            and target_machine
            and target_machine != credentials["machine_id"]
        ):
            self._raise_if_target_machine_offline(credentials, target_machine)
        response = self.sync_once(extra_events=[event])
        for result in response.get("event_results", []):
            if result.get("event_id") == event_id:
                if result.get("status") != "ok":
                    raise RuntimeError(result.get("error") or "remote run request failed")
                return result["result"]
        raise RuntimeError("server did not acknowledge remote run request")

    def _raise_if_target_machine_offline(
        self,
        credentials: dict[str, Any],
        target_machine: str,
    ) -> None:
        """Fail before queueing when the caller expects an immediate remote result."""

        server = self._server_client_for_credentials(credentials)
        machines = server.list_machines()
        machine = next((item for item in machines if item.get("id") == target_machine), None)
        if machine is None:
            return
        if machine.get("status") == "online":
            return
        raise RuntimeError(
            "target machine "
            f"{target_machine!r} is {machine.get('status') or 'offline'}; "
            "the run was not queued. Use "
            "client.submit(..., offline_policy='queue') to register the task "
            "and poll it later."
        )

    def import_server_object(
        self,
        object_name: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Pull missing server object versions into the local registry.

        The first import of a server object mirrors the full version history.
        Later refreshes are cheap: the daemon checks the server's current
        version id first and downloads YAML only when that version is absent
        locally.
        """

        credentials = self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError(
                "object is not registered locally and active server connection "
                f"is not found: {object_name}"
            )
        credentials = self._require_connected_server_credentials(credentials)

        server = self._server_client_for_credentials(credentials)

        server_scope: dict[str, Any] = {}
        if owner_id is not None:
            server_scope["owner_id"] = owner_id
        if library is not None:
            server_scope["library"] = library

        remote_current = server.get_object(
            object_name,
            version=version,
            include_yaml=False,
            **server_scope,
        )
        remote_object_id = remote_current["id"]
        remote_version_id = remote_current["version_id"]
        existing_current = self.store.get_object_by_remote_version(
            remote_version_id,
            include_yaml=False,
        )
        if existing_current is not None:
            return {
                "source": "server",
                "name": object_name,
                "remote_object_id": remote_object_id,
                "current_version": existing_current,
                "versions": [existing_current],
                "refreshed": False,
            }

        if self.store.list_object_versions_by_remote_object(remote_object_id):
            remote_versions = [
                server.get_object(
                    object_name,
                    version=version,
                    include_yaml=True,
                    **server_scope,
                )
            ]
        else:
            remote_versions = server.list_object_versions(
                object_name,
                include_yaml=True,
                **server_scope,
            )
        if not remote_versions:
            raise KeyError(f"object is not registered on server: {object_name}")

        self._ensure_server_object_envs(remote_versions)

        imported = []
        for remote_version in sorted(remote_versions, key=lambda item: int(item["version"])):
            existing_version = self.store.get_object_by_remote_version(
                remote_version["version_id"],
                include_yaml=False,
            )
            if existing_version is not None:
                imported.append(existing_version)
                continue

            yaml_text = remote_version.get("yaml")
            if not yaml_text:
                raise RuntimeError(
                    "server did not return YAML for object version "
                    f"{remote_version.get('version_id')}"
                )
            imported.append(
                self.register_object(
                    remote_version["name"],
                    remote_version["entrypoint"],
                    remote_version.get("env") or "default",
                    yaml_text=yaml_text,
                    owner_id=self._server_version_owner_id(remote_version),
                    library=self._server_version_library(remote_version),
                    description=remote_version.get("description") or "",
                    version_label=remote_version.get("version_label"),
                    origin="server",
                    remote_owner_id=remote_version.get("owner_id"),
                    remote_object_id=remote_version.get("id"),
                    remote_version_id=remote_version.get("version_id"),
                    source_object_name=remote_version["name"],
                    runtime_config=remote_version.get("runtime_config"),
                )
            )

        current_version = self.store.get_object_by_remote_version(
            remote_version_id,
            include_yaml=False,
        )
        if current_version is None:
            raise KeyError(f"server object version was not imported: {remote_version_id}")

        return {
            "source": "server",
            "name": object_name,
            "remote_object_id": remote_object_id,
            "current_version": current_version,
            "versions": imported,
            "refreshed": True,
        }

    def _server_version_owner_id(self, version: dict[str, Any]) -> str:
        owner_id = version.get("owner_id") or version.get("owner")
        return validate_name(str(owner_id or DEFAULT_OBJECT_OWNER_ID))

    def _server_version_library(self, version: dict[str, Any]) -> str:
        library = version.get("library_slug") or version.get("library")
        if isinstance(library, dict):
            library = library.get("slug") or library.get("name")
        return validate_name(str(library or DEFAULT_OBJECT_LIBRARY))

    def _ensure_server_object_envs(self, remote_versions: list[dict[str, Any]]) -> None:
        """Map server env names to local Python executables when first seen."""

        for env in sorted({item.get("env") or "default" for item in remote_versions}):
            if not self._has_local_env(env):
                self._register_auto_server_env(env)

    def _register_auto_server_env(self, name: str) -> dict[str, Any]:
        try:
            base_python = self.store.get_env("default")["python"]
        except KeyError:
            base_python = sys.executable
        return self.store.register_env(name, base_python)

    def refresh_server_object_if_available(
        self,
        object_name: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any] | None:
        """Best-effort server refresh before a local auto-sourced run.

        ``source="auto"`` must keep local objects runnable when the central
        server is offline or does not have the object.  Operational problems
        that mean "cannot check for updates" are therefore soft failures here;
        semantic problems, such as a missing local environment for a real server
        object, still surface to the caller.
        """

        try:
            return self.import_server_object(
                object_name,
                version=version,
                owner_id=owner_id,
                library=library,
            )
        except ServerOfflineError:
            return None
        except KeyError as exc:
            message = str(exc)
            if (
                "active server connection is not found" in message
                or "object is not registered on server" in message
            ):
                return None
            raise
        except ServerClientError as exc:
            if exc.status_code in {404, 502, 503, 504}:
                return None
            raise

    def _has_local_env(self, name: str) -> bool:
        try:
            self.store.get_env(name)
        except KeyError:
            return False
        return True

    def sync_once(
        self,
        *,
        connection_id: str | None = None,
        extra_events: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Exchange pending local events and jobs with the central server."""

        credentials = (
            self.store.get_server_connection_credentials(connection_id)
            if connection_id is not None
            else self.store.current_server_connection_credentials()
        )
        if credentials is None:
            return {
                "connected": False,
                "event_results": [],
                "jobs": [],
                "sync": self.sync_visibility.summary(),
            }

        if not credentials.get("remote_connection_id"):
            try:
                credentials = self._restore_pending_server_connection(credentials)
            except ServerClientError as exc:
                if not self._is_server_connectivity_error(exc):
                    raise
                self.store.record_server_connection_error(
                    credentials["id"],
                    status="connect_failed",
                    error=exc.message,
                )
                return {
                    "connected": False,
                    "offline": True,
                    "event_results": [],
                    "jobs": [],
                    "error": SERVER_OFFLINE_MESSAGE,
                    "detail": exc.message,
                    "sync": self.sync_visibility.summary(),
                }

        snapshot_hash, manifest_items = self.build_machine_library_snapshot_manifest()
        pending = self.store.list_pending_sync_events()
        sync_before = self.sync_visibility.summary(pending)
        snapshot_event = None

        with self._server_sync_lock:
            server = self._server_client_for_credentials(credentials)
            if snapshot_hash != credentials.get("last_library_snapshot_hash"):
                remote_snapshot_hash = self._server_latest_library_snapshot_hash(
                    server,
                    credentials["machine_id"],
                )
                if remote_snapshot_hash == snapshot_hash:
                    self.store.record_server_connection_library_snapshot(
                        credentials["id"],
                        snapshot_hash=snapshot_hash,
                    )
                else:
                    snapshot_event = self.build_machine_library_snapshot_event(
                        snapshot_hash=snapshot_hash,
                        manifest_items=manifest_items,
                    )

            events = []
            if snapshot_event is not None:
                events.append(snapshot_event)
            events.extend([
                {"id": item["id"], "kind": item["kind"], "payload": item["payload"]}
                for item in pending
            ])
            events.extend(extra_events or [])

            response = server.sync(
                connection_id=credentials["remote_connection_id"],
                machine_id=credentials["machine_id"],
                heartbeat_interval_seconds=float(
                    credentials["heartbeat_interval_seconds"]
                ),
                events=events,
            )

        connection = response.get("connection")
        if connection:
            self.store.record_server_connection_heartbeat(
                credentials["id"],
                remote_connection=connection,
            )

        snapshot_event_id = snapshot_event["id"] if snapshot_event is not None else None
        pending_ids = {item["id"] for item in pending}
        for result in response.get("event_results", []):
            event_id = result.get("event_id")
            if event_id == snapshot_event_id:
                if result.get("status") == "ok":
                    self.store.record_server_connection_library_snapshot(
                        credentials["id"],
                        snapshot_hash=snapshot_hash,
                    )
                continue
            if event_id not in pending_ids:
                continue
            if result.get("status") == "ok":
                self.store.mark_sync_event_sent(event_id)
            else:
                self.store.mark_sync_event_failed(
                    event_id,
                    result.get("error") or "sync event failed",
                )

        for job in response.get("jobs", []):
            self.accept_server_job(job, credentials["id"])
        response["sync"] = {
            "before": sync_before,
            "after": self.sync_visibility.summary(),
        }
        return response

    def _server_latest_library_snapshot_hash(
        self,
        server: ServerClientProtocol,
        machine_id: str,
    ) -> str | None:
        """Return the server's latest snapshot hash when it can be checked cheaply."""

        try:
            snapshot = server.latest_machine_library_snapshot(machine_id)
        except ServerClientError:
            return None
        return snapshot.get("snapshot_hash")

    def accept_server_job(self, job: dict[str, Any], connection_id: str) -> None:
        """Run one server job in a background thread."""

        thread = threading.Thread(
            target=self._execute_server_job,
            args=(job, connection_id),
            name=f"spl-server-job-{job['run']['id']}",
            daemon=True,
        )
        thread.start()

    def _execute_server_job(self, job: dict[str, Any], connection_id: str) -> None:
        """Execute one server-assigned job locally and sync the result back."""

        run = job["run"]
        version = job["object_version"]
        run_id = run["id"]
        local_name = version["name"]

        try:
            self._send_server_run_update(
                connection_id,
                run_id=run_id,
                status="fetching_object",
                message="registering object bundle in local daemon",
            )
            object_record = self.register_object(
                local_name,
                version["entrypoint"],
                version["env"] or "default",
                yaml_text=version["yaml"],
                owner_id=self._server_version_owner_id(version),
                library=self._server_version_library(version),
                description=version.get("description") or version["name"],
                version_label=version.get("version_label"),
                origin="server",
                remote_owner_id=version.get("owner_id"),
                remote_object_id=version.get("id"),
                remote_version_id=version.get("version_id"),
                source_object_name=version["name"],
                runtime_config=version.get("runtime_config"),
            )
            self._send_server_run_update(connection_id, run_id=run_id, status="running")
            function = (
                run.get("entrypoint")
                if run.get("entrypoint") and run.get("entrypoint") != version["entrypoint"]
                else None
            )
            local_run = self.start_run(
                local_name,
                args=run.get("args"),
                kwargs=run.get("kwargs"),
                output=run.get("output"),
                timeout_seconds=run.get("timeout_seconds"),
                object_version_id=object_record["version_id"],
                function=function,
                source="local",
                report_local_run=False,
            )
            final_state = self._wait_local_run(
                local_run["id"],
                timeout_seconds=run.get("timeout_seconds"),
            )
            if final_state["status"] != "succeeded":
                self._send_server_run_update(
                    connection_id,
                    run_id=run_id,
                    status="failed",
                    error=final_state.get("error") or "local run failed",
                    payload={"local_run": final_state},
                )
                return

            result = self.store.get_run(final_state["id"]).get("result")
            artifacts = self._prepare_remote_run_artifacts(
                connection_id,
                run_id,
                final_state,
            )
            self._send_server_run_update(
                connection_id,
                run_id=run_id,
                status="succeeded",
                result=result,
                payload={"local_run": final_state},
                artifacts=artifacts,
            )
        except Exception as exc:
            self._send_server_run_update(
                connection_id,
                run_id=run_id,
                status="failed",
                error=repr(exc),
            )

    def _send_server_run_update(
        self,
        connection_id: str,
        *,
        run_id: str,
        status: str,
        result: Any = None,
        error: str | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
    ) -> None:
        event: dict[str, Any] = {
            "id": uuid4().hex,
            "kind": "run_update",
            "payload": {
                "run_id": run_id,
                "status": status,
                "result": result,
                "error": error,
                "message": message,
                "payload": payload or {},
                "artifacts": artifacts or [],
            },
        }
        self.store.enqueue_sync_event(event["kind"], event["payload"])
        self.sync_once(connection_id=connection_id)

    def _wait_local_run(
        self,
        run_id: str,
        *,
        timeout_seconds: float | None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        while True:
            state = self.store.get_run(run_id)
            if state["status"] in {"succeeded", "failed"}:
                return state
            if timeout_seconds is not None and time.monotonic() - started > timeout_seconds:
                raise TimeoutError(f"local run did not finish within {timeout_seconds} seconds")
            time.sleep(0.25)

    def _wait_server_run(
        self,
        run_id: str,
        *,
        timeout_seconds: float | None,
    ) -> dict[str, Any]:
        credentials = self._require_connected_server_credentials()
        server = self._server_client_for_credentials(credentials)
        started = time.monotonic()
        while True:
            state = server.get_remote_run(run_id)
            if state["status"] in {"succeeded", "failed", "cancelled", "stale"}:
                return state
            if timeout_seconds is not None and time.monotonic() - started > timeout_seconds:
                raise TimeoutError(
                    f"remote node run did not finish within {timeout_seconds} seconds"
                )
            time.sleep(0.5)

    def run_remote_node(
        self,
        node: dict[str, Any],
        *,
        kwargs: dict[str, Any],
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Execute a NodeRemote through the central server and return its result."""

        ref = self._normalize_remote_ref(node)
        signature = self.resolve_remote_signature(ref)
        target_machine = (
            ref.get("target_machine")
            or signature.get("target_machine")
            or (signature.get("execution") or {}).get("default_machine_id")
        )
        remote_ref = signature.get("remote_ref") or {}
        object_owner_id = ref.get("owner_id") or remote_ref.get("owner_id")
        library = ref.get("library") or remote_ref.get("library")

        output = self._remote_node_output_selector(signature)
        if not target_machine:
            raise RuntimeError(
                "remote node cannot run because no target machine was selected; "
                "set target_machine on the remote reference or configure "
                "execution.default_machine_id for the server object "
                f"{ref['object_name']!r}"
            )
        remote_run = self.start_remote_run(
            signature.get("id") or ref["object_name"],
            target_machine=target_machine,
            object_owner_id=object_owner_id,
            library=library,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            object_version_id=signature.get("version_id"),
            function=ref.get("function") or signature.get("function"),
            context={
                "remote_ref": remote_ref,
                "node": {
                    "url": ref.get("server_url"),
                    "name": ref.get("object_name"),
                    "function": ref.get("function"),
                    "version": ref.get("version"),
                },
            },
        )
        final_state = self._wait_server_run(
            remote_run["id"],
            timeout_seconds=timeout_seconds,
        )
        if final_state["status"] != "succeeded":
            status = final_state.get("status") or "unknown"
            error = final_state.get("error") or "remote run returned no error message"
            object_kind = signature.get("kind") or "object"
            raise RuntimeError(
                f"remote {object_kind} node {ref['object_name']!r} failed on "
                f"machine {target_machine!r}: run {remote_run['id']} ended as "
                f"{status!r} ({error})"
            )
        payload = final_state.get("result") or {}
        value = self._remote_node_result_value(
            payload,
            signature,
            output=output,
        )
        artifacts = payload.get("artifacts") if isinstance(payload, dict) else {}
        return {
            "value": value,
            "run_id": remote_run["id"],
            "status": final_state.get("status"),
            "run": final_state,
            "payload": payload if isinstance(payload, dict) else {"result": value},
            "artifacts": artifacts or {},
        }

    def _remote_node_output_selector(self, signature: dict[str, Any]) -> str | None:
        selectors = [
            item.get("selector")
            for item in signature.get("outputs") or []
            if item.get("selector") is not None
        ]
        selectors = [str(item) for item in selectors]
        if len(selectors) > 1:
            raise RuntimeError(
                "remote Function/Pipeline node has multiple selectable outputs; "
                "the current NodeRemote shape cannot choose one explicitly"
            )
        return selectors[0] if selectors else None

    def _remote_node_result_value(
        self,
        payload: Any,
        signature: dict[str, Any],
        *,
        output: str | None,
    ) -> Any:
        value = payload.get("result") if isinstance(payload, dict) else payload
        if (signature.get("kind") or "unknown") != "pipeline":
            return value
        if not isinstance(value, dict):
            return value

        selected = None
        for item in signature.get("outputs") or []:
            if output is not None and item.get("selector") == output:
                selected = item
                break
        if selected is None and len(signature.get("outputs") or []) == 1:
            selected = (signature.get("outputs") or [None])[0]
        if selected is None:
            return value

        value_path = selected.get("value_path") or []
        if value_path and len(value_path) == 1 and value_path[0] in value:
            return value[value_path[0]]
        return value

    def _encode_local_artifacts(self, run_state: dict[str, Any]) -> list[dict[str, Any]]:
        artifacts_dir = Path(run_state["artifacts_dir"])
        if not artifacts_dir.exists():
            return []
        encoded = []
        for path in sorted(artifacts_dir.iterdir()):
            if path.is_file():
                encoded.append(
                    {
                        "name": path.name,
                        "data_base64": base64.b64encode(path.read_bytes()).decode("ascii"),
                    }
                )
        return encoded

    def _prepare_remote_run_artifacts(
        self,
        connection_id: str,
        run_id: str,
        run_state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        artifacts_dir = Path(run_state["artifacts_dir"])
        if not artifacts_dir.exists():
            return []

        credentials = self.store.get_server_connection_credentials(connection_id)
        if credentials is None:
            raise ServerOfflineError(SERVER_OFFLINE_MESSAGE)
        server = self._server_client_for_credentials(credentials)

        prepared: list[dict[str, Any]] = []
        inline_bytes = 0
        for path in sorted(artifacts_dir.iterdir()):
            if not path.is_file():
                continue
            metadata = self._artifact_file_metadata(path)
            inline_allowed = (
                metadata["size"] <= DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES
                and inline_bytes + metadata["size"]
                <= DEFAULT_INLINE_REMOTE_ARTIFACT_TOTAL_MAX_BYTES
            )
            if inline_allowed:
                inline_bytes += metadata["size"]
                prepared.append(
                    {
                        **metadata,
                        "transfer_mode": "inline_base64",
                        "data_base64": base64.b64encode(path.read_bytes()).decode("ascii"),
                    }
                )
                continue

            uploaded = server.upload_artifact(run_id, path.name, path)
            self._assert_uploaded_artifact_matches(metadata, uploaded)
            prepared.append(
                {
                    **metadata,
                    "transfer_mode": "direct_upload",
                    "uploaded": True,
                    "server_artifact_id": uploaded.get("id"),
                }
            )
        return prepared

    def _artifact_file_metadata(self, path: Path) -> dict[str, Any]:
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
        return {
            "name": validate_name(path.name),
            "size": size,
            "sha256": digest.hexdigest(),
        }

    @staticmethod
    def _assert_uploaded_artifact_matches(
        expected: dict[str, Any],
        uploaded: dict[str, Any],
    ) -> None:
        if uploaded.get("name") != expected["name"]:
            raise RuntimeError(
                "uploaded artifact name mismatch: "
                f"expected {expected['name']}, got {uploaded.get('name')}"
            )
        if int(uploaded.get("size", -1)) != int(expected["size"]):
            raise RuntimeError(
                "uploaded artifact size mismatch: "
                f"{expected['name']} expected {expected['size']}, got {uploaded.get('size')}"
            )
        if str(uploaded.get("sha256", "")).casefold() != str(expected["sha256"]).casefold():
            raise RuntimeError(
                "uploaded artifact checksum mismatch: "
                f"{expected['name']} expected {expected['sha256']}, got {uploaded.get('sha256')}"
            )

    def prepare_object_environment(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Start a cached runtime build for an object version when configured."""

        backend = self.runtime_backends.backend_for(object_record)
        if not self.auto_build_envs:
            return backend.status_for_object(object_record)
        if object_record.get("origin") == "server":
            status = backend.status_for_object(object_record)
            status["auto_build_skipped"] = "server_imported_object"
            return status
        try:
            status = backend.ensure_ready(object_record, wait=False)
            backend.after_prepare(object_record)
            return status
        except EnvironmentBuildError:
            return backend.status_for_object(object_record)

    def start_run(
        self,
        object_name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        object_version_id: str | None = None,
        function: str | None = None,
        source: str = "auto",
        report_local_run: bool = True,
    ) -> dict[str, Any]:
        """Create a run and execute it in a background worker thread."""

        if source not in {"auto", "local"}:
            raise ValueError("source must be 'auto' or 'local'")

        object_name, function = split_object_function_ref(object_name, function)
        resolved_version_id = object_version_id
        if source == "auto" and object_version_id is None:
            refresh = self.refresh_server_object_if_available(
                object_name,
                version=version,
            )
            if refresh and refresh.get("current_version"):
                resolved_version_id = refresh["current_version"]["version_id"]

        try:
            state = self.store.create_run(
                object_name,
                args=args,
                kwargs=kwargs,
                output=output,
                timeout_seconds=timeout_seconds,
                version=version,
                object_version_id=resolved_version_id,
                function=function,
            )
        except KeyError as exc:
            can_import = (
                source == "auto"
                and resolved_version_id is None
                and "object is not registered" in str(exc)
            )
            if not can_import:
                raise
            # No local fallback exists, so this second import attempt is strict:
            # if the server is unavailable, report that instead of hiding it
            # behind a local "object is not registered" error.
            imported = self.import_server_object(object_name, version=version)
            resolved_version_id = imported["current_version"]["version_id"]
            state = self.store.create_run(
                object_name,
                args=args,
                kwargs=kwargs,
                output=output,
                timeout_seconds=timeout_seconds,
                version=version,
                object_version_id=resolved_version_id,
                function=function,
            )
        thread = threading.Thread(
            target=self._execute_run,
            args=(state["id"], report_local_run),
            name=f"spl-run-{state['id']}",
            daemon=True,
        )
        thread.start()
        return self._update_local_run(
            state["id"],
            report_local_run=report_local_run,
            status="starting",
        )

    def _execute_run(self, run_id: str, report_local_run: bool = True) -> None:
        """Launch the worker process and persist the final run state."""

        state = self.store.get_run(run_id)
        object_record = self.store.get_object_version(state["object_version_id"])
        run_dir = Path(state["run_dir"])
        result_path = Path(state["result_path"])
        artifacts_dir = Path(state["artifacts_dir"])
        input_path = run_dir / "input.json"
        object_yaml_path = run_dir / "object.yaml"
        env_spec_path = run_dir / "env-spec.json"
        remote_signatures_path = run_dir / "remote-signatures.json"
        stdout_path = run_dir / "stdout.txt"
        stderr_path = run_dir / "stderr.txt"
        worker_path = Path(__file__).with_name("worker.py")

        object_yaml_path.write_text(object_record["yaml"], encoding="utf-8")
        write_json(env_spec_path, object_record["distributions"])
        write_json(
            remote_signatures_path,
            {
                "nodes": [
                    node
                    for node in object_record.get("pipeline_nodes") or []
                    if node.get("kind") == "remote"
                ]
            },
        )

        self._update_local_run(
            run_id,
            report_local_run=report_local_run,
            status="preparing_environment",
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
        )

        env = os.environ.copy()
        env["PYTHONPATH"] = self._worker_pythonpath(env)

        timeout = self._read_timeout(input_path)
        workdir = Path(object_record.get("workdir") or str(run_dir))
        workdir.mkdir(parents=True, exist_ok=True)
        ctx = RunContext(
            object_record=object_record,
            run_id=run_id,
            run_dir=run_dir,
            workdir=workdir,
            input_path=input_path,
            object_yaml_path=object_yaml_path,
            result_path=result_path,
            artifacts_dir=artifacts_dir,
            env_spec_path=env_spec_path,
            remote_signatures_path=remote_signatures_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            worker_path=worker_path,
            entrypoint=state["entrypoint"],
            daemon_base_url=self.daemon_base_url,
        )

        try:
            backend = self.runtime_backends.backend_for(object_record)
            with backend:
                environment_record = backend.ensure_ready(object_record)
                command = backend.build_command(ctx)
                self._update_local_run(
                    run_id,
                    report_local_run=report_local_run,
                    status="running",
                    started_at=utc_now(),
                    command=command,
                    env_build_hash=environment_record["spec_hash"],
                    runtime_build_hash=environment_record["spec_hash"],
                    **backend.run_state_fields(),
                )
                completed = subprocess.run(
                    command,
                    cwd=workdir,
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=timeout,
                    check=False,
                )
                stdout_path.write_text(completed.stdout, encoding="utf-8")
                stderr_path.write_text(completed.stderr, encoding="utf-8")
                after_run = backend.after_run(ctx)
                if after_run:
                    self._update_local_run(
                        run_id,
                        report_local_run=report_local_run,
                        **after_run,
                    )

                if completed.returncode == 0 and result_path.exists():
                    result_payload = json.loads(result_path.read_text(encoding="utf-8"))
                    if backend.process_result(ctx, result_payload):
                        write_json(result_path, result_payload)
                    self._update_local_run(
                        run_id,
                        report_local_run=report_local_run,
                        status="succeeded",
                        finished_at=utc_now(),
                        returncode=completed.returncode,
                        result=result_payload,
                        stdout_text=completed.stdout,
                        stderr_text=completed.stderr,
                    )
                else:
                    error = completed.stderr.strip() or completed.stdout.strip()
                    if completed.returncode == 0:
                        error = "worker finished without writing result.json"
                    self._update_local_run(
                        run_id,
                        report_local_run=report_local_run,
                        status="failed",
                        finished_at=utc_now(),
                        returncode=completed.returncode,
                        error=error,
                        stdout_text=completed.stdout,
                        stderr_text=completed.stderr,
                    )
        except subprocess.TimeoutExpired as exc:
            stdout = self._subprocess_text(exc.stdout)
            stderr = self._subprocess_text(exc.stderr)
            stdout_path.write_text(stdout, encoding="utf-8")
            stderr_path.write_text(stderr, encoding="utf-8")
            self._update_local_run(
                run_id,
                report_local_run=report_local_run,
                status="failed",
                finished_at=utc_now(),
                error=f"run timed out after {timeout} seconds",
                stdout_text=stdout,
                stderr_text=stderr,
            )
        except Exception as exc:
            self._update_local_run(
                run_id,
                report_local_run=report_local_run,
                status="failed",
                finished_at=utc_now(),
                error=repr(exc),
            )

    def _update_local_run(
        self,
        run_id: str,
        *,
        report_local_run: bool,
        **changes: Any,
    ) -> dict[str, Any]:
        state = self.store.update_run(run_id, **changes)
        if report_local_run:
            self.enqueue_local_run_update(state)
        return state

    def enqueue_local_run_update(self, state: dict[str, Any]) -> None:
        """Queue a local-only run status for central-server observability."""

        if self.store.current_server_connection_credentials() is None:
            return
        self.store.enqueue_sync_event(
            "local_run_update",
            {"run": self._local_run_sync_payload(state)},
        )
        self._kick_server_sync()

    def _kick_server_sync(self) -> None:
        credentials = self.store.current_server_connection_credentials()
        if credentials is None:
            return
        thread = threading.Thread(
            target=self._sync_once_safely,
            args=(credentials["id"],),
            name=f"spl-server-sync-kick-{credentials['id']}",
            daemon=True,
        )
        thread.start()

    def _sync_once_safely(self, connection_id: str) -> None:
        try:
            self.sync_once(connection_id=connection_id)
        except Exception as exc:
            self.store.record_server_connection_error(
                connection_id,
                status="heartbeat_failed",
                error=repr(exc),
            )

    def _local_run_sync_payload(self, state: dict[str, Any]) -> dict[str, Any]:
        object_label = self._local_run_object_label(state)
        return {
            "id": state["id"],
            "object_name": object_label["display_name"],
            "object_display_name": object_label["display_name"],
            "local_object_name": object_label["local_name"],
            "object_id": state.get("object_id"),
            "object_version_id": state.get("object_version_id"),
            "object_version": state.get("object_version"),
            "remote_object_id": object_label.get("remote_object_id"),
            "remote_version_id": object_label.get("remote_version_id"),
            "entrypoint": state.get("entrypoint"),
            "env": state.get("env"),
            "runtime_backend": state.get("runtime_backend"),
            "runtime_build_hash": state.get("runtime_build_hash"),
            "resolved_runtime": state.get("resolved_runtime"),
            "image_tag": state.get("image_tag"),
            "container_id": state.get("container_id"),
            "status": state.get("status"),
            "input": state.get("input") or {},
            "result": state.get("result"),
            "error": state.get("error"),
            "started_at": state.get("started_at"),
            "finished_at": state.get("finished_at"),
            "created_at": state.get("created_at"),
            "artifacts": self._local_run_text_artifacts(state),
        }

    def _local_run_object_label(self, state: dict[str, Any]) -> dict[str, Any]:
        local_name = state.get("object") or "local_object"
        label = {
            "display_name": local_name,
            "local_name": local_name,
            "remote_object_id": None,
            "remote_version_id": None,
        }
        version_id = state.get("object_version_id")
        if not version_id:
            return label
        try:
            record = self.store.get_object_version(version_id, include_yaml=False)
        except KeyError:
            return label
        display_name = (
            record.get("display_name")
            or record.get("object_remote_name")
            or record.get("name")
            or local_name
        )
        label["display_name"] = display_name
        label["remote_object_id"] = (
            record.get("remote_object_id")
            or record.get("object_remote_object_id")
        )
        label["remote_version_id"] = record.get("remote_version_id")
        return label

    def _local_run_text_artifacts(self, state: dict[str, Any]) -> list[dict[str, Any]]:
        artifacts: list[dict[str, Any]] = []
        if state.get("result") is not None:
            artifacts.append(
                self._text_artifact_payload(
                    "result.json",
                    json.dumps(
                        state["result"],
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    ),
                    kind="result",
                    content_type="application/json",
                )
            )
        if state.get("stdout"):
            artifacts.append(
                self._text_artifact_payload(
                    "stdout.txt",
                    state["stdout"],
                    kind="stdout",
                    content_type="text/plain; charset=utf-8",
                )
            )
        if state.get("stderr"):
            artifacts.append(
                self._text_artifact_payload(
                    "stderr.txt",
                    state["stderr"],
                    kind="stderr",
                    content_type="text/plain; charset=utf-8",
                )
            )

        artifacts_dir = Path(state["artifacts_dir"])
        if artifacts_dir.exists():
            for path in sorted(artifacts_dir.iterdir()):
                if not path.is_file() or path.suffix.lower() not in LOCAL_RUN_TEXT_ARTIFACT_EXTENSIONS:
                    continue
                payload = self._file_text_artifact_payload(path)
                if payload is not None:
                    artifacts.append(payload)
        return artifacts

    def _file_text_artifact_payload(self, path: Path) -> dict[str, Any] | None:
        try:
            data = path.read_bytes()
        except OSError:
            return None
        content_type = self._artifact_content_type(path)
        truncated = len(data) > LOCAL_RUN_TEXT_ARTIFACT_MAX_BYTES
        text = data[:LOCAL_RUN_TEXT_ARTIFACT_MAX_BYTES].decode(
            "utf-8",
            errors="replace",
        )
        try:
            return self._text_artifact_payload(
                f"artifact.{path.name}",
                text,
                kind="artifact",
                content_type=content_type,
                size=len(data),
                truncated=truncated,
            )
        except ValueError:
            return None

    def _text_artifact_payload(
        self,
        name: str,
        content_text: str,
        *,
        kind: str,
        content_type: str,
        size: int | None = None,
        truncated: bool = False,
    ) -> dict[str, Any]:
        encoded = content_text.encode("utf-8")
        payload: dict[str, Any] = {
            "name": validate_name(name),
            "kind": kind,
            "content_type": content_type,
            "size": size if size is not None else len(encoded),
            "content_text": content_text,
            "truncated": truncated,
        }
        if content_type == "application/json" and not truncated:
            try:
                payload["content_json"] = json.loads(content_text)
            except json.JSONDecodeError:
                pass
        return payload

    def _artifact_content_type(self, path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix == ".json":
            return "application/json"
        if suffix in {".htm", ".html"}:
            return "text/html; charset=utf-8"
        if suffix == ".csv":
            return "text/csv; charset=utf-8"
        if suffix == ".tsv":
            return "text/tab-separated-values; charset=utf-8"
        if suffix in {".yaml", ".yml"}:
            return "application/yaml"
        return "text/plain; charset=utf-8"

    def _worker_pythonpath(self, env: dict[str, str]) -> str:
        """Make this checkout's ``src`` directory visible to the worker."""

        src_dir = Path(__file__).parents[2]
        current = env.get("PYTHONPATH")
        if current:
            return os.pathsep.join([str(src_dir), current])
        return str(src_dir)

    def shutdown(self) -> None:
        self.heartbeat_service.shutdown()
        self.docker_pool.shutdown()

    def _read_timeout(self, input_path: Path) -> float | None:
        """Read an optional run timeout from the stored input payload."""

        payload = json.loads(input_path.read_text(encoding="utf-8"))
        timeout = payload.get("timeout_seconds")
        if timeout is None:
            return None
        return float(timeout)

    def _subprocess_text(self, value: str | bytes | None) -> str:
        """Normalize TimeoutExpired stdout/stderr values."""

        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value


def _load_quart() -> tuple[Any, Any, Any]:
    """Import Quart lazily so non-server daemon commands stay dependency-light."""

    try:
        from quart import Quart, Response, request
    except ModuleNotFoundError as exc:
        if exc.name == "quart":
            raise RuntimeError(
                "Quart is required to run the SPL daemon server. "
                "Install it in the daemon environment, for example: pip install quart"
            ) from exc
        raise
    return Quart, Response, request


def create_app(
    store: RegistryStore,
    *,
    auto_build_envs: bool = True,
    env_build_timeout_seconds: float | None = None,
    env_stale_lock_seconds: float | None = None,
    daemon_base_url: str = "http://127.0.0.1:8765",
    docker_pool_size: int = 0,
    docker_idle_timeout_seconds: float = 300.0,
    docker_prewarm: bool = False,
    api_token: str | None = None,
    environment_manager: EnvironmentManagerProtocol | None = None,
    docker_environment_manager: DockerEnvironmentManagerProtocol | None = None,
    docker_pool: DockerPoolRunnerProtocol | None = None,
    runtime_backends: RuntimeBackendRegistry | None = None,
    sync_visibility: SyncVisibilityProtocol | None = None,
    server_client_factory: ServerClientFactoryProtocol = _default_server_client_factory,
) -> Any:
    """Create a Quart application bound to one registry store."""

    Quart, Response, request = _load_quart()
    app = Quart(__name__)
    local_api_token = api_token or generate_daemon_api_token()
    app.api_token = local_api_token
    runtime = DaemonRuntime(
        store,
        auto_build_envs=auto_build_envs,
        env_build_timeout_seconds=env_build_timeout_seconds,
        env_stale_lock_seconds=env_stale_lock_seconds,
        daemon_base_url=daemon_base_url,
        docker_pool_size=docker_pool_size,
        docker_idle_timeout_seconds=docker_idle_timeout_seconds,
        docker_prewarm=docker_prewarm,
        environment_manager=environment_manager,
        docker_environment_manager=docker_environment_manager,
        docker_pool=docker_pool,
        runtime_backends=runtime_backends,
        sync_visibility=sync_visibility,
        server_client_factory=server_client_factory,
    )
    app.runtime = runtime

    context = RouteContext(
        runtime=runtime,
        response_cls=Response,
        request=request,
        local_api_token=local_api_token,
    )
    app.before_request(context.require_local_api_auth)

    register_diagnostics_routes(
        app,
        runtime=runtime,
        json_response=context.json_response,
        route_errors=context.route_errors,
    )
    register_server_connection_routes(app, runtime=runtime, context=context)
    register_library_routes(app, runtime=runtime, context=context)
    register_object_routes(app, runtime=runtime, context=context)
    register_env_routes(app, runtime=runtime, context=context)
    register_remote_routes(app, runtime=runtime, context=context)
    register_run_routes(app, runtime=runtime, context=context)
    register_artifact_routes(app, runtime=runtime, context=context)

    return app


def make_server(
    host: str,
    port: int,
    store: RegistryStore,
    **runtime_kwargs: Any,
) -> Any:
    """Backward-compatible factory name; returns a Quart app."""

    _ = (host, port)
    return create_app(store, **runtime_kwargs)


def _port_is_available(host: str, port: int) -> bool:
    """Return whether a local TCP server can bind to ``host:port``."""

    try:
        with socket.create_server((host, port), backlog=1):
            return True
    except OSError:
        return False


def select_daemon_port(
    host: str,
    preferred_port: int,
    *,
    auto_port: bool = True,
    scan_limit: int = DEFAULT_PORT_SCAN_LIMIT,
) -> int:
    """Select an available port, optionally scanning upward from the preference."""

    if preferred_port < 1 or preferred_port > 65535:
        raise ValueError("daemon port must be between 1 and 65535")
    if scan_limit < 1:
        raise ValueError("port_scan_limit must be positive")

    attempts = scan_limit if auto_port else 1
    last_port = min(65535, preferred_port + attempts - 1)
    for candidate in range(preferred_port, last_port + 1):
        if _port_is_available(host, candidate):
            return candidate

    if auto_port:
        raise OSError(
            f"no free daemon port found on {host} from {preferred_port} to {last_port}"
        )
    raise OSError(f"daemon port {preferred_port} is already busy on {host}")


def _client_host_for_bind_host(host: str) -> str:
    """Return a loopback host that local clients can use for wildcard binds."""

    if host in {"", "0.0.0.0"}:
        return "127.0.0.1"
    if host == "::":
        return "::1"
    return host


def serve(
    host: str = "127.0.0.1",
    port: int = 8765,
    home: Path | None = None,
    *,
    auto_port: bool = True,
    port_scan_limit: int = DEFAULT_PORT_SCAN_LIMIT,
    auto_build_envs: bool = True,
    env_build_timeout_seconds: float | None = None,
    env_stale_lock_seconds: float | None = None,
    docker_pool_size: int = 0,
    docker_idle_timeout_seconds: float = 300.0,
    docker_prewarm: bool = False,
    environment_manager: EnvironmentManagerProtocol | None = None,
    docker_environment_manager: DockerEnvironmentManagerProtocol | None = None,
    docker_pool: DockerPoolRunnerProtocol | None = None,
    runtime_backends: RuntimeBackendRegistry | None = None,
    sync_visibility: SyncVisibilityProtocol | None = None,
    server_client_factory: ServerClientFactoryProtocol = _default_server_client_factory,
) -> None:
    """Run the local daemon until interrupted."""

    store = RegistryStore(home)
    base_url: str | None = None
    app: Any | None = None
    try:
        selected_port = select_daemon_port(
            host,
            port,
            auto_port=auto_port,
            scan_limit=port_scan_limit,
        )
        client_host = _client_host_for_bind_host(host)
        api_token = generate_daemon_api_token()
        endpoint = write_daemon_endpoint(
            store.home,
            bind_host=host,
            host=client_host,
            port=selected_port,
            api_token=api_token,
            updated_at=utc_now(),
        )
        base_url = str(endpoint["base_url"])
        app = create_app(
            store,
            auto_build_envs=auto_build_envs,
            env_build_timeout_seconds=env_build_timeout_seconds,
            env_stale_lock_seconds=env_stale_lock_seconds,
            daemon_base_url=base_url,
            docker_pool_size=docker_pool_size,
            docker_idle_timeout_seconds=docker_idle_timeout_seconds,
            docker_prewarm=docker_prewarm,
            api_token=api_token,
            environment_manager=environment_manager,
            docker_environment_manager=docker_environment_manager,
            docker_pool=docker_pool,
            runtime_backends=runtime_backends,
            sync_visibility=sync_visibility,
            server_client_factory=server_client_factory,
        )
        if selected_port != port:
            print(f"SPL daemon port {port} is busy; using {selected_port} instead")
        print(f"SPL daemon listening on {daemon_url(host, selected_port)}")
        print(f"SPL daemon client endpoint: {base_url}")
        print(f"SPL daemon home: {store.home}")
        app.run(host=host, port=selected_port)
    except KeyboardInterrupt:
        print("\nSPL daemon stopped")
    finally:
        if base_url is not None:
            clear_daemon_endpoint(store.home, base_url=base_url)
        if app is not None:
            try:
                app.runtime.shutdown()  # type: ignore[attr-defined]
            except Exception:
                pass
        store.close()
