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

import json
import hashlib
import os
import platform
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
import base64
from functools import wraps
from http import HTTPStatus
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

from spl.daemon_client import (
    clear_daemon_endpoint,
    daemon_url,
    generate_daemon_api_token,
    write_daemon_endpoint,
)
from spl.daemon.docker_environment import DockerEnvironmentManager
from spl.daemon.environment import EnvironmentBuildError, EnvironmentManager
from spl.daemon.remote_client import (
    DEFAULT_SERVER_URL,
    ServerClient,
    ServerClientError,
)
from spl.daemon.routes.diagnostics import register_diagnostics_routes
from spl.daemon.services.sync import SyncVisibilityService
from spl.daemon.signature import build_signature, summarize_object
from spl.daemon.store import (
    RegistryStore,
    split_object_function_ref,
    utc_now,
    validate_name,
    write_json,
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


class ServerOfflineError(RuntimeError):
    """Raised when a server-backed operation is requested while offline."""


SERVER_OFFLINE_MESSAGE = (
    "central SPL daemon server is offline or unreachable. Local registry, "
    "local runs, and pending sync events remain available; server-backed "
    "operations require connectivity and should be retried after the daemon "
    "reconnects."
)


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
    ):
        self.store = store
        self.auto_build_envs = auto_build_envs
        self.daemon_base_url = daemon_base_url.rstrip("/")
        self.docker_pool_size = max(0, int(docker_pool_size))
        self.docker_idle_timeout_seconds = float(docker_idle_timeout_seconds)
        self.docker_prewarm = bool(docker_prewarm)
        self._docker_pool_lock = threading.RLock()
        self._docker_pool: dict[str, dict[str, Any]] = {}
        manager_kwargs = {}
        if env_build_timeout_seconds is not None:
            manager_kwargs["build_timeout_seconds"] = env_build_timeout_seconds
        if env_stale_lock_seconds is not None:
            manager_kwargs["stale_lock_seconds"] = env_stale_lock_seconds
        self.environment_manager = EnvironmentManager(store, **manager_kwargs)
        self.docker_environment_manager = DockerEnvironmentManager(store, **manager_kwargs)
        self.sync_visibility = SyncVisibilityService(store)
        self._server_heartbeat_lock = threading.Lock()
        self._server_heartbeat_stops: dict[str, threading.Event] = {}
        self._server_heartbeat_threads: dict[str, threading.Thread] = {}
        self._server_sync_lock = threading.Lock()
        self._cleanup_stale_docker_pool_containers()
        self.restore_server_heartbeat()

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

        existing = self._matching_server_connection(
            server_url=server_url,
            machine_token=machine_token,
            user_token=user_token,
            machine_id=machine_id,
        )
        if (
            existing is not None
            and existing.get("remote_connection_id")
        ):
            local_connection = self.store.get_server_connection(existing["id"])
            return {
                "connected": local_connection["status"] == "connected",
                "offline": local_connection["status"] == "heartbeat_failed",
                "reused": True,
                "connection": local_connection,
                "remote_connection": self._remote_connection_snapshot(local_connection),
            }

        server = ServerClient(server_url, machine_token, user_token=user_token)
        try:
            remote_connection = server.connect_machine(
                machine_id=machine_id,
                display_name=display_name,
                capabilities=capabilities,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
            )
        except ServerClientError as exc:
            if not self._is_server_connectivity_error(exc):
                raise
            pending_machine_id = self._offline_machine_id(
                machine_token,
                machine_id=machine_id,
                display_name=display_name,
            )
            local_connection = self.store.save_pending_server_connection(
                server_url=server_url,
                token=machine_token,
                user_token=user_token,
                machine_id=pending_machine_id,
                display_name=display_name or pending_machine_id,
                capabilities=capabilities,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
                error=exc.message,
            )
            self.start_server_heartbeat(local_connection, token=machine_token)
            return {
                "connected": False,
                "offline": True,
                "connection": local_connection,
                "remote_connection": None,
                "error": SERVER_OFFLINE_MESSAGE,
                "detail": exc.message,
            }

        if existing is not None:
            local_connection = self.store.complete_server_connection(
                existing["id"],
                remote_connection=remote_connection,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
            )
        else:
            local_connection = self.store.save_server_connection(
                server_url=server_url,
                token=machine_token,
                user_token=user_token,
                connection=remote_connection,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
            )
        self.start_server_heartbeat(local_connection, token=machine_token)
        return {
            "connected": True,
            "connection": local_connection,
            "remote_connection": remote_connection,
        }

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

        credentials = self.store.current_server_connection_credentials()
        if credentials is None:
            return None
        if credentials["server_url"].rstrip("/") != server_url.rstrip("/"):
            return None
        if credentials["token"] != machine_token:
            return None
        if credentials["user_token"] != user_token:
            return None
        if machine_id is not None and credentials["machine_id"] != machine_id:
            return None
        return credentials

    @staticmethod
    def _is_server_connectivity_error(exc: ServerClientError) -> bool:
        return exc.status_code in {502, 503, 504}

    @staticmethod
    def _offline_machine_id(
        machine_token: str,
        *,
        machine_id: str | None,
        display_name: str | None,
    ) -> str:
        if machine_id:
            return validate_name(machine_id)
        digest = hashlib.sha256(machine_token.encode("utf-8")).hexdigest()[:12]
        return f"machine-{digest}"

    def _restore_pending_server_connection(
        self,
        credentials: dict[str, Any],
    ) -> dict[str, Any]:
        if credentials.get("remote_connection_id"):
            return credentials
        server = ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        )
        remote_connection = server.connect_machine(
            machine_id=credentials["machine_id"],
            display_name=credentials.get("display_name"),
            capabilities=credentials.get("capabilities") or {},
            heartbeat_interval_seconds=float(credentials["heartbeat_interval_seconds"]),
        )
        return self.store.complete_server_connection(
            credentials["id"],
            remote_connection=remote_connection,
            heartbeat_interval_seconds=float(credentials["heartbeat_interval_seconds"]),
        )

    def _require_connected_server_credentials(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        credentials = credentials or self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError("active server connection is not found")
        if not credentials.get("remote_connection_id"):
            raise ServerOfflineError(SERVER_OFFLINE_MESSAGE)
        return credentials

    def _remote_connection_snapshot(self, connection: dict[str, Any]) -> dict[str, Any]:
        """Build a server-like connection payload from the local cached row."""

        return {
            "id": connection["remote_connection_id"],
            "owner_id": connection["owner_id"],
            "subject_type": connection["subject_type"],
            "subject_id": connection["subject_id"],
            "machine_id": connection["machine_id"],
            "display_name": connection["display_name"],
            "capabilities": connection.get("capabilities") or {},
            "status": connection["status"],
            "heartbeat_interval_seconds": connection["heartbeat_interval_seconds"],
            "last_seen_at": connection["last_heartbeat_at"],
            "expires_at": connection["lease_expires_at"],
        }

    def disconnect_server(self) -> dict[str, Any]:
        """Gracefully disconnect the current central-server lease."""

        credentials = self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError("active server connection is not found")

        self.stop_server_heartbeat(credentials["id"])
        if not credentials.get("remote_connection_id"):
            local_connection = self.store.mark_server_connection_disconnected(
                credentials["id"],
                remote_connection=None,
            )
            return {
                "connected": False,
                "offline": True,
                "connection": local_connection,
                "remote_connection": None,
            }
        remote_connection = ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        ).disconnect_machine()
        local_connection = self.store.mark_server_connection_disconnected(
            credentials["id"],
            remote_connection=remote_connection,
        )
        return {
            "connected": False,
            "connection": local_connection,
            "remote_connection": remote_connection,
        }

    def restore_server_heartbeat(self) -> None:
        """Resume heartbeat loop for a persisted active connection, if present."""

        credentials = self.store.current_server_connection_credentials()
        if credentials is not None:
            self.start_server_heartbeat(credentials, token=credentials["token"])

    def start_server_heartbeat(
        self,
        connection: dict[str, Any],
        *,
        token: str,
    ) -> None:
        """Start one background heartbeat loop and stop older loops."""

        stop_event = threading.Event()
        with self._server_heartbeat_lock:
            for event in self._server_heartbeat_stops.values():
                event.set()
            self._server_heartbeat_stops.clear()
            self._server_heartbeat_threads.clear()
            self._server_heartbeat_stops[connection["id"]] = stop_event

        thread = threading.Thread(
            target=self._server_heartbeat_loop,
            args=(connection["id"], token, stop_event),
            name=f"spl-server-heartbeat-{connection['id']}",
            daemon=True,
        )
        with self._server_heartbeat_lock:
            self._server_heartbeat_threads[connection["id"]] = thread
        thread.start()

    def stop_server_heartbeat(self, connection_id: str) -> None:
        """Stop the heartbeat loop for one local connection."""

        with self._server_heartbeat_lock:
            event = self._server_heartbeat_stops.pop(connection_id, None)
            self._server_heartbeat_threads.pop(connection_id, None)
        if event is not None:
            event.set()

    def _server_heartbeat_loop(
        self,
        connection_id: str,
        token: str,
        stop_event: threading.Event,
    ) -> None:
        """Keep the central-server connection lease alive while daemon runs."""

        first_tick = True
        while not stop_event.is_set():
            credentials = self.store.get_server_connection_credentials(connection_id)
            interval = float(credentials["heartbeat_interval_seconds"])
            if not first_tick and stop_event.wait(interval):
                return
            first_tick = False

            try:
                self.sync_once(connection_id=connection_id)
            except ServerClientError as exc:
                status = (
                    "stale"
                    if exc.status_code in {401, 403, 404, 409}
                    else "heartbeat_failed"
                )
                self.store.record_server_connection_error(
                    connection_id,
                    status=status,
                    error=exc.message,
                )
                if status == "stale":
                    return
            except Exception as exc:  # noqa: BLE001 - heartbeat must not kill daemon.
                self.store.record_server_connection_error(
                    connection_id,
                    status="heartbeat_failed",
                    error=repr(exc),
                )

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
            record["name"],
            version=record["version"],
            include_yaml=True,
        )
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
        if library:
            payload["library"] = validate_name(library)
        if create_library:
            payload["create_library"] = True
        if library_display_name:
            payload["library_display_name"] = str(library_display_name)
        return self.store.enqueue_sync_event("object_version", payload)

    def build_machine_library_snapshot_manifest(self) -> tuple[str, list[dict[str, Any]]]:
        """Build a lightweight, stable manifest for the current local library."""

        items = []
        for record in self.store.list_objects().values():
            items.append(
                {
                    "library_slug": "default",
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

        return self.store.register_object(
            name,
            entrypoint,
            env,
            remote_signature_resolver=self.resolve_remote_signature,
            **kwargs,
        )

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
            server = ServerClient(
                credentials["server_url"],
                credentials["token"],
                user_token=credentials["user_token"],
            )
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
        server = ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        )
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

        server = ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        )
        machines = server.list_machines()
        machine = next((item for item in machines if item.get("id") == target_machine), None)
        if machine is None:
            return
        if machine.get("status") == "online":
            return
        raise RuntimeError(
            "target machine "
            f"{target_machine!r} is {machine.get('status') or 'offline'}; "
            "the run was not queued. Use client.queue(...) or "
            "client.start(..., offline_policy='queue') to register the task "
            "and poll it later."
        )

    def import_server_object(
        self,
        object_name: str,
        *,
        version: int | None = None,
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

        server = ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        )

        remote_current = server.get_object(
            object_name,
            version=version,
            include_yaml=False,
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
                )
            ]
        else:
            remote_versions = server.list_object_versions(
                object_name,
                include_yaml=True,
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
                    self._server_object_local_name(remote_version["id"]),
                    remote_version["entrypoint"],
                    remote_version.get("env") or "default",
                    yaml_text=yaml_text,
                    description=remote_version.get("description") or "",
                    version_label=remote_version.get("version_label"),
                    origin="server",
                    remote_owner_id=remote_version.get("owner_id"),
                    remote_object_id=remote_version.get("id"),
                    remote_version_id=remote_version.get("version_id"),
                    remote_name=remote_version["name"],
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

    def _server_object_local_name(self, remote_object_id: str) -> str:
        """Return the local registry name for a mirrored server object."""

        return f"server.{validate_name(remote_object_id)}"

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
    ) -> dict[str, Any] | None:
        """Best-effort server refresh before a local auto-sourced run.

        ``source="auto"`` must keep local objects runnable when the central
        server is offline or does not have the object.  Operational problems
        that mean "cannot check for updates" are therefore soft failures here;
        semantic problems, such as a missing local environment for a real server
        object, still surface to the caller.
        """

        try:
            return self.import_server_object(object_name, version=version)
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
            server = ServerClient(
                credentials["server_url"],
                credentials["token"],
                user_token=credentials["user_token"],
            )
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
        server: ServerClient,
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
        local_name = self._server_object_local_name(version["id"])

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
                description=version.get("description") or version["name"],
                version_label=version.get("version_label"),
                origin="server",
                remote_owner_id=version.get("owner_id"),
                remote_object_id=version.get("id"),
                remote_version_id=version.get("version_id"),
                remote_name=version["name"],
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
        except Exception as exc:  # noqa: BLE001 - report job failure to server.
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
        event = {
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
        server = ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        )
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
        server = ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        )

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

        manager = self._environment_manager_for(object_record)
        if not self.auto_build_envs:
            return manager.status_for_object(object_record)
        if object_record.get("origin") == "server":
            status = manager.status_for_object(object_record)
            status["auto_build_skipped"] = "server_imported_object"
            return status
        try:
            status = manager.ensure_ready(object_record, wait=False)
            if (
                object_record.get("runtime_mode") == "docker"
                or (object_record.get("runtime_config") or {}).get("mode") == "docker"
            ) and self.docker_prewarm and self.docker_pool_size > 0:
                self._prewarm_docker_object(object_record)
            return status
        except EnvironmentBuildError:
            return manager.status_for_object(object_record)

    def _environment_manager_for(self, object_record: dict[str, Any]) -> Any:
        runtime_config = object_record.get("runtime_config") or {"mode": "venv"}
        if runtime_config.get("mode") == "docker":
            return self.docker_environment_manager
        return self.environment_manager

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

        command = [
            object_record["env_python"],
            str(worker_path),
            "--object-yaml",
            str(object_yaml_path),
            "--entrypoint",
            state["entrypoint"],
            "--input",
            str(input_path),
            "--result",
            str(result_path),
            "--artifacts-dir",
            str(artifacts_dir),
            "--env-spec",
            str(env_spec_path),
            "--remote-signatures",
            str(remote_signatures_path),
            "--daemon-url",
            self.daemon_base_url,
        ]

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
        workdir = object_record.get("workdir") or str(run_dir)
        Path(workdir).mkdir(parents=True, exist_ok=True)
        runtime_config = object_record.get("runtime_config") or {"mode": "venv"}
        docker_runtime = runtime_config.get("mode") == "docker"
        container_name: str | None = None
        container_id: str | None = None
        cleanup_container = False
        pool_record: dict[str, Any] | None = None

        try:
            if docker_runtime:
                self._assert_docker_available_for_run()
                environment_record = self.docker_environment_manager.ensure_ready(
                    object_record,
                    wait=True,
                )
                if self._can_use_docker_pool(run_dir, Path(workdir)):
                    pool_record = self._ensure_docker_pool_container(
                        object_record=object_record,
                        image_tag=environment_record["image_tag"],
                        runtime_config=runtime_config,
                    )
                    container_name = pool_record["name"]
                    container_id = pool_record.get("container_id")
                    command = self._docker_exec_worker_command(
                        object_record=object_record,
                        entrypoint=state["entrypoint"],
                        run_id=run_id,
                        container_name=container_name,
                        runtime_config=runtime_config,
                    )
                else:
                    container_name = self._docker_container_name(run_id)
                    cleanup_container = True
                    command = self._docker_worker_command(
                        object_record=object_record,
                        entrypoint=state["entrypoint"],
                        run_id=run_id,
                        run_dir=run_dir,
                        workdir=Path(workdir),
                        image_tag=environment_record["image_tag"],
                        container_name=container_name,
                        runtime_config=runtime_config,
                    )
                resolved_runtime = environment_record["image_tag"]
                resolved_python = None
                runtime_backend = "docker"
                image_tag = environment_record["image_tag"]
            else:
                environment_record = self.environment_manager.ensure_ready(
                    object_record,
                    wait=True,
                )
                resolved_python = environment_record["python_path"]
                command = [resolved_python, *command[1:]]
                resolved_runtime = resolved_python
                runtime_backend = "venv"
                image_tag = None

            self._update_local_run(
                run_id,
                report_local_run=report_local_run,
                status="running",
                started_at=utc_now(),
                command=command,
                env_build_hash=environment_record["spec_hash"],
                runtime_build_hash=environment_record["spec_hash"],
                resolved_runtime=resolved_runtime,
                runtime_backend=runtime_backend,
                image_tag=image_tag,
                container_id=container_id,
                resolved_python=resolved_python,
            )

            if pool_record is not None:
                exec_lock = pool_record["exec_lock"]
                with exec_lock:
                    pool_record["in_use"] = True
                    try:
                        completed = subprocess.run(
                            command,
                            cwd=workdir,
                            env=env,
                            text=True,
                            capture_output=True,
                            timeout=timeout,
                            check=False,
                        )
                    finally:
                        pool_record["in_use"] = False
                        pool_record["last_used"] = time.monotonic()
            else:
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
            if docker_runtime and cleanup_container:
                self._update_local_run(
                    run_id,
                    report_local_run=report_local_run,
                    container_id=self._read_docker_container_id(run_dir),
                )

            if completed.returncode == 0 and result_path.exists():
                result_payload = json.loads(result_path.read_text(encoding="utf-8"))
                if docker_runtime:
                    self._rewrite_docker_artifact_paths(result_payload, artifacts_dir)
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
            if container_name is not None and cleanup_container:
                self._remove_docker_container(container_name)
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
        except Exception as exc:  # noqa: BLE001 - daemon must record worker errors.
            self._update_local_run(
                run_id,
                report_local_run=report_local_run,
                status="failed",
                finished_at=utc_now(),
                error=repr(exc),
            )
        finally:
            if container_name is not None and cleanup_container:
                self._remove_docker_container(container_name)

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
        except Exception as exc:  # noqa: BLE001 - keep pending events for heartbeat retry.
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

    def _docker_worker_command(
        self,
        *,
        object_record: dict[str, Any],
        entrypoint: str,
        run_id: str,
        run_dir: Path,
        workdir: Path,
        image_tag: str,
        container_name: str,
        runtime_config: dict[str, Any],
    ) -> list[str]:
        """Build a Docker CLI command that runs the normal worker protocol."""

        source_roots = self._docker_source_roots()
        daemon_source = source_roots[0][1]
        container_run_dir = "/work"
        container_workdir = container_run_dir
        mounts = [
            "-v",
            f"{run_dir.resolve()}:{container_run_dir}",
        ]
        if workdir.resolve() != run_dir.resolve():
            container_workdir = "/workspace"
            mounts.extend(["-v", f"{workdir.resolve()}:{container_workdir}"])

        pythonpath_entries = []
        for index, (_, source_root) in enumerate(source_roots):
            container_path = f"/opt/splime/src{index}"
            mounts.extend(["-v", f"{source_root}:{container_path}:ro"])
            pythonpath_entries.append(container_path)

        network_args, daemon_url = self._docker_network_args(
            object_record,
            runtime_config,
        )
        command = [
            "docker",
            "run",
            "--rm",
            "--name",
            container_name,
            "--cidfile",
            str(run_dir.resolve() / "container.cid"),
            *network_args,
            *self._docker_hardening_args(runtime_config),
            *self._docker_user_args(),
            *mounts,
            "-w",
            container_workdir,
            "-e",
            f"PYTHONPATH={':'.join(pythonpath_entries)}",
            *self._docker_env_args(runtime_config),
            image_tag,
            "python",
            f"/opt/splime/src0/spl/daemon/worker.py",
            "--object-yaml",
            f"{container_run_dir}/object.yaml",
            "--entrypoint",
            entrypoint,
            "--input",
            f"{container_run_dir}/input.json",
            "--result",
            f"{container_run_dir}/result.json",
            "--artifacts-dir",
            f"{container_run_dir}/artifacts",
            "--env-spec",
            f"{container_run_dir}/env-spec.json",
            "--remote-signatures",
            f"{container_run_dir}/remote-signatures.json",
            "--daemon-url",
            daemon_url,
        ]
        if not (daemon_source / "spl" / "daemon" / "worker.py").exists():
            raise RuntimeError(f"Docker worker source is not found: {daemon_source}")
        return command

    def _docker_exec_worker_command(
        self,
        *,
        object_record: dict[str, Any],
        entrypoint: str,
        run_id: str,
        container_name: str,
        runtime_config: dict[str, Any],
    ) -> list[str]:
        run_path = f"/runs/{validate_name(run_id)}"
        _, daemon_url = self._docker_network_args(object_record, runtime_config)
        return [
            "docker",
            "exec",
            "-w",
            run_path,
            container_name,
            "python",
            "/opt/splime/src0/spl/daemon/worker.py",
            "--object-yaml",
            f"{run_path}/object.yaml",
            "--entrypoint",
            entrypoint,
            "--input",
            f"{run_path}/input.json",
            "--result",
            f"{run_path}/result.json",
            "--artifacts-dir",
            f"{run_path}/artifacts",
            "--env-spec",
            f"{run_path}/env-spec.json",
            "--remote-signatures",
            f"{run_path}/remote-signatures.json",
            "--daemon-url",
            daemon_url,
        ]

    def _can_use_docker_pool(self, run_dir: Path, workdir: Path) -> bool:
        return self.docker_pool_size > 0 and run_dir.resolve() == workdir.resolve()

    def _prewarm_docker_object(self, object_record: dict[str, Any]) -> None:
        def prewarm() -> None:
            try:
                environment_record = self.docker_environment_manager.ensure_ready(
                    object_record,
                    wait=True,
                )
                self._ensure_docker_pool_container(
                    object_record=object_record,
                    image_tag=environment_record["image_tag"],
                    runtime_config=object_record.get("runtime_config") or {"mode": "venv"},
                )
            except Exception:
                return

        thread = threading.Thread(
            target=prewarm,
            name=f"spl-docker-prewarm-{object_record['version_id']}",
            daemon=True,
        )
        thread.start()

    def _ensure_docker_pool_container(
        self,
        *,
        object_record: dict[str, Any],
        image_tag: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        key = self._docker_pool_key(image_tag, runtime_config, object_record)
        now = time.monotonic()
        with self._docker_pool_lock:
            self._evict_idle_docker_pool_locked(now)
            existing = self._docker_pool.get(key)
            if existing is not None and self._docker_container_running(existing["name"]):
                existing["last_used"] = now
                return existing
            if existing is not None:
                self._remove_docker_container(existing["name"])
                self._docker_pool.pop(key, None)

            self._evict_excess_docker_pool_locked(reserve=1)

        record = self._start_docker_pool_container(
            key=key,
            object_record=object_record,
            image_tag=image_tag,
            runtime_config=runtime_config,
        )
        record["last_used"] = time.monotonic()
        record["exec_lock"] = threading.Lock()
        with self._docker_pool_lock:
            existing = self._docker_pool.get(key)
            if existing is not None and self._docker_container_running(existing["name"]):
                self._remove_docker_container(record["name"])
                existing["last_used"] = time.monotonic()
                return existing
            if existing is not None:
                self._remove_docker_container(existing["name"])
            self._evict_excess_docker_pool_locked(reserve=1)
            self._docker_pool[key] = record
            return record

    def _start_docker_pool_container(
        self,
        *,
        key: str,
        object_record: dict[str, Any],
        image_tag: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        source_roots = self._docker_source_roots()
        daemon_source = source_roots[0][1]
        if not (daemon_source / "spl" / "daemon" / "worker.py").exists():
            raise RuntimeError(f"Docker worker source is not found: {daemon_source}")

        name = f"splime-pool-{key[:24]}"
        self._remove_docker_container(name)
        pool_dir = self.store.home / "docker-pool"
        pool_dir.mkdir(parents=True, exist_ok=True)
        cidfile = pool_dir / f"{name}.cid"
        try:
            cidfile.unlink()
        except FileNotFoundError:
            pass

        mounts = ["-v", f"{self.store.runs_dir.resolve()}:/runs"]
        pythonpath_entries = []
        for index, (_, source_root) in enumerate(source_roots):
            container_path = f"/opt/splime/src{index}"
            mounts.extend(["-v", f"{source_root}:{container_path}:ro"])
            pythonpath_entries.append(container_path)

        network_args, _ = self._docker_network_args(object_record, runtime_config)
        command = [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--cidfile",
            str(cidfile),
            *network_args,
            *self._docker_hardening_args(runtime_config),
            *self._docker_user_args(),
            *mounts,
            "-w",
            "/runs",
            "-e",
            f"PYTHONPATH={':'.join(pythonpath_entries)}",
            *self._docker_env_args(runtime_config),
            image_tag,
            "python",
            "-c",
            "import time; time.sleep(10**9)",
        ]
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "failed to start warm Docker runtime container: "
                + (completed.stderr.strip() or completed.stdout.strip())
            )
        container_id = None
        try:
            container_id = cidfile.read_text(encoding="utf-8").strip() or None
        except OSError:
            pass
        return {
            "key": key,
            "name": name,
            "container_id": container_id,
            "image_tag": image_tag,
            "started_at": utc_now(),
            "in_use": False,
        }

    def _docker_pool_key(
        self,
        image_tag: str,
        runtime_config: dict[str, Any],
        object_record: dict[str, Any],
    ) -> str:
        payload = json.dumps(
            {
                "image_tag": image_tag,
                "runtime_config": runtime_config,
                "network_args": self._docker_network_args(object_record, runtime_config)[0],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _evict_idle_docker_pool_locked(self, now: float) -> None:
        if self.docker_idle_timeout_seconds <= 0:
            return
        for key, record in list(self._docker_pool.items()):
            if record.get("in_use"):
                continue
            if now - float(record.get("last_used") or now) > self.docker_idle_timeout_seconds:
                self._remove_docker_container(record["name"])
                self._docker_pool.pop(key, None)

    def _evict_excess_docker_pool_locked(self, *, reserve: int = 0) -> None:
        while len(self._docker_pool) + reserve > self.docker_pool_size and self._docker_pool:
            candidates = {
                key: record
                for key, record in self._docker_pool.items()
                if not record.get("in_use")
            }
            if not candidates:
                return
            key, record = min(
                candidates.items(),
                key=lambda item: float(item[1].get("last_used") or 0.0),
            )
            self._remove_docker_container(record["name"])
            self._docker_pool.pop(key, None)

    def _docker_container_running(self, name: str) -> bool:
        completed = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", name],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=15,
            check=False,
        )
        return completed.returncode == 0 and completed.stdout.strip() == "true"

    def _cleanup_stale_docker_pool_containers(self) -> None:
        if shutil.which("docker") is None:
            return
        completed = subprocess.run(
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                "name=^/splime-pool-",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=15,
            check=False,
        )
        if completed.returncode != 0:
            return
        container_ids = [
            item.strip()
            for item in completed.stdout.splitlines()
            if item.strip()
        ]
        if not container_ids:
            return
        subprocess.run(
            ["docker", "rm", "-f", *container_ids],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=30,
            check=False,
        )

    def shutdown(self) -> None:
        with self._server_heartbeat_lock:
            stop_events = list(self._server_heartbeat_stops.values())
            threads = list(self._server_heartbeat_threads.values())
            self._server_heartbeat_stops.clear()
            self._server_heartbeat_threads.clear()
        for event in stop_events:
            event.set()
        current_thread = threading.current_thread()
        for thread in threads:
            if thread is not current_thread:
                thread.join(timeout=2)

        with self._docker_pool_lock:
            for record in list(self._docker_pool.values()):
                self._remove_docker_container(record["name"])
            self._docker_pool.clear()

    def _docker_hardening_args(self, runtime_config: dict[str, Any]) -> list[str]:
        args: list[str] = []
        if runtime_config.get("init", True):
            args.append("--init")
        cap_drop = runtime_config.get("cap_drop")
        if cap_drop:
            args.extend(["--cap-drop", str(cap_drop)])
        if runtime_config.get("no_new_privileges", True):
            args.extend(["--security-opt", "no-new-privileges"])
        limits = runtime_config.get("limits") or {}
        if limits.get("memory"):
            args.extend(["--memory", str(limits["memory"])])
        if limits.get("cpus"):
            args.extend(["--cpus", str(limits["cpus"])])
        if limits.get("pids_limit"):
            args.extend(["--pids-limit", str(limits["pids_limit"])])
        if runtime_config.get("read_only", True):
            args.append("--read-only")
        tmpfs = runtime_config.get("tmpfs")
        if tmpfs:
            args.extend(["--tmpfs", str(tmpfs)])
        return args

    def _docker_env_args(self, runtime_config: dict[str, Any]) -> list[str]:
        env_values = {
            "HOME": "/tmp",
            "XDG_CACHE_HOME": "/tmp/.cache",
            "MPLCONFIGDIR": "/tmp/.cache/matplotlib",
            **(runtime_config.get("env") or {}),
        }
        args: list[str] = []
        for key, value in sorted(env_values.items()):
            args.extend(["-e", f"{key}={value}"])
        return args

    def _assert_docker_available_for_run(self) -> None:
        if shutil.which("docker") is None:
            raise RuntimeError(
                "Docker runtime is selected, but the docker executable is not "
                "available on PATH"
            )
        try:
            completed = subprocess.run(
                ["docker", "info"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                timeout=15,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                "Docker runtime is selected, but `docker info` did not respond "
                "within 15 seconds"
            ) from exc
        if completed.returncode != 0:
            detail = (completed.stderr or "").strip()
            message = "Docker runtime is selected, but the Docker daemon is not reachable"
            if detail:
                message = f"{message}: {detail}"
            raise RuntimeError(message)

    def _docker_source_roots(self) -> list[tuple[str, Path]]:
        roots = [("daemon", Path(__file__).parents[2].resolve())]
        try:
            import spl.core as spl_core

            core_path = Path(str(spl_core.__file__)).parents[2].resolve()
            if core_path not in [path for _, path in roots]:
                roots.append(("framework", core_path))
        except Exception:  # noqa: BLE001 - build/run will surface import errors.
            pass
        return roots

    def _docker_network_args(
        self,
        object_record: dict[str, Any],
        runtime_config: dict[str, Any],
    ) -> tuple[list[str], str]:
        mode = runtime_config.get("network", "auto")
        has_remote_nodes = any(
            node.get("kind") == "remote"
            for node in object_record.get("pipeline_nodes") or []
        )
        if mode == "none" and has_remote_nodes:
            raise RuntimeError(
                "docker runtime network='none' cannot run pipelines with remote nodes"
            )
        if mode == "none" or (mode == "auto" and not has_remote_nodes):
            return ["--network", "none"], self.daemon_base_url
        if platform.system().lower() == "linux":
            return ["--add-host", "host.docker.internal:host-gateway"], (
                self._docker_host_daemon_url()
            )
        return [], self._docker_host_daemon_url()

    def _docker_host_daemon_url(self) -> str:
        parsed = urlparse(self.daemon_base_url)
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            return self.daemon_base_url
        host = "host.docker.internal"
        netloc = host
        if parsed.port is not None:
            netloc = f"{host}:{parsed.port}"
        return urlunparse(
            (
                parsed.scheme,
                netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment,
            )
        )

    def _docker_user_args(self) -> list[str]:
        if os.name == "nt" or not hasattr(os, "getuid") or not hasattr(os, "getgid"):
            return []
        return ["--user", f"{os.getuid()}:{os.getgid()}"]

    def _docker_container_name(self, run_id: str) -> str:
        return f"splime-run-{validate_name(run_id)[:32]}"

    def _remove_docker_container(self, name: str) -> None:
        subprocess.run(
            ["docker", "rm", "-f", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    def _read_docker_container_id(self, run_dir: Path) -> str | None:
        cid_path = run_dir / "container.cid"
        try:
            value = cid_path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return value or None

    def _rewrite_docker_artifact_paths(
        self,
        result_payload: dict[str, Any],
        artifacts_dir: Path,
    ) -> None:
        artifacts = result_payload.get("artifacts")
        if not isinstance(artifacts, dict):
            return
        result_payload["artifacts"] = {
            name: str(artifacts_dir / name)
            for name in artifacts
        }

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
    )
    app.runtime = runtime

    def json_response(
        value: Any,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> Any:
        body = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
        return Response(
            body,
            status=int(status),
            content_type="application/json; charset=utf-8",
        )

    def route_errors(
        handler: Callable[..., Awaitable[Any]],
    ) -> Callable[..., Awaitable[Any]]:
        @wraps(handler)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await handler(*args, **kwargs)
            except KeyError as exc:
                return json_response({"error": str(exc)}, HTTPStatus.NOT_FOUND)
            except ValueError as exc:
                return json_response({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            except ServerOfflineError as exc:
                return json_response(
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
                return json_response(
                    {
                        "error": exc.message,
                        "upstream_status": exc.status_code,
                    },
                    status,
                )
            except RuntimeError as exc:
                return json_response({"error": str(exc)}, HTTPStatus.CONFLICT)
            except Exception as exc:  # noqa: BLE001 - HTTP boundary must not crash.
                return json_response(
                    {"error": repr(exc)},
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )

        return wrapper

    @app.before_request
    async def require_local_api_auth() -> Any:
        auth_header = request.headers.get("Authorization") or ""
        scheme, _, token = auth_header.partition(" ")
        if (
            scheme.casefold() == "bearer"
            and token
            and secrets.compare_digest(token, local_api_token)
        ):
            return None
        return json_response(
            {"error": "missing or invalid local daemon API token"},
            HTTPStatus.UNAUTHORIZED,
        )

    async def read_json_body() -> dict[str, Any]:
        body = await request.get_json(silent=True)
        return body or {}

    def first_query_value(*names: str) -> str | None:
        for name in names:
            value = request.args.get(name)
            if value is not None:
                return value
        return None

    def optional_int_query(name: str) -> int | None:
        value = first_query_value(name)
        if value is None or value == "":
            return None
        return int(value)

    def query_bool(name: str, *, default: bool = False) -> bool:
        value = first_query_value(name)
        if value is None:
            return default
        return value.lower() in {"1", "true", "yes", "on"}

    def connected_server_client() -> tuple[dict[str, Any], ServerClient]:
        credentials = runtime._require_connected_server_credentials()
        return credentials, ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        )

    def object_function_ref(name_or_id: str) -> tuple[str, str | None]:
        return split_object_function_ref(
            name_or_id,
            first_query_value("function", "entrypoint"),
        )

    def object_from_local_or_server(
        name_or_id: str,
        *,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        version = optional_int_query("version")
        refresh = runtime.refresh_server_object_if_available(
            name_or_id,
            version=version,
        )
        if refresh and refresh.get("current_version"):
            return runtime.store.get_object_version(
                refresh["current_version"]["version_id"],
                include_yaml=include_yaml,
            )
        return runtime.store.get_object(
            name_or_id,
            version=version,
            include_yaml=include_yaml,
        )

    register_diagnostics_routes(
        app,
        runtime=runtime,
        json_response=json_response,
        route_errors=route_errors,
    )

    @app.get("/server/connection")
    @route_errors
    async def current_server_connection() -> Any:
        connection = runtime.store.current_server_connection()
        return json_response(
            {
                "connected": (
                    connection is not None
                    and connection["status"] == "connected"
                    and bool(connection.get("remote_connection_id"))
                ),
                "offline": (
                    connection is not None
                    and connection["status"] in {"connect_failed", "heartbeat_failed"}
                ),
                "connection": connection,
            }
        )

    @app.get("/server/connections")
    @route_errors
    async def list_server_connections() -> Any:
        return json_response(runtime.store.list_server_connections())

    @app.get("/server/machines")
    @route_errors
    async def list_server_machines() -> Any:
        credentials, server = connected_server_client()
        machines = server.list_machines()
        current_machine_id = credentials["machine_id"]
        for machine in machines:
            machine["is_current"] = machine["id"] == current_machine_id
        return json_response(
            {
                "current_machine_id": current_machine_id,
                "machines": machines,
            }
        )

    @app.get("/server/objects")
    @route_errors
    async def list_server_objects() -> Any:
        _, server = connected_server_client()
        view = (first_query_value("view") or "").lower()
        compact = (first_query_value("compact") or "").lower() in {
            "1",
            "true",
            "yes",
        }
        return json_response(
            server.list_objects(
                owner_id=first_query_value("owner", "owner_id"),
                library=first_query_value("library"),
                compact=view == "summary" or compact,
            )
        )

    @app.get("/server/libraries")
    @route_errors
    async def list_server_libraries() -> Any:
        _, server = connected_server_client()
        return json_response(
            server.list_libraries(
                include_accessible=query_bool("include_accessible", default=True),
            )
        )

    @app.post("/server/libraries")
    @route_errors
    async def create_server_library() -> Any:
        _, server = connected_server_client()
        return json_response(
            server.create_library(await read_json_body()),
            HTTPStatus.CREATED,
        )

    @app.get("/server/libraries/<library_ref>")
    @route_errors
    async def get_server_library(library_ref: str) -> Any:
        _, server = connected_server_client()
        return json_response(server.get_library(validate_name(library_ref)))

    @app.put("/server/libraries/<library_ref>")
    @route_errors
    async def update_server_library(library_ref: str) -> Any:
        _, server = connected_server_client()
        return json_response(
            server.update_library(
                validate_name(library_ref),
                await read_json_body(),
            )
        )

    @app.delete("/server/libraries/<library_ref>")
    @route_errors
    async def delete_server_library(library_ref: str) -> Any:
        _, server = connected_server_client()
        return json_response(server.delete_library(validate_name(library_ref)))

    @app.get("/server/libraries/<library_ref>/grants")
    @route_errors
    async def list_server_library_grants(library_ref: str) -> Any:
        _, server = connected_server_client()
        return json_response(server.list_library_grants(validate_name(library_ref)))

    @app.post("/server/libraries/<library_ref>/grants")
    @route_errors
    async def grant_server_library(library_ref: str) -> Any:
        _, server = connected_server_client()
        return json_response(
            server.grant_library(
                validate_name(library_ref),
                await read_json_body(),
            ),
            HTTPStatus.CREATED,
        )

    @app.post("/server/libraries/<library_ref>/grants/<grantee>/revoke")
    @route_errors
    async def revoke_server_library_grant(library_ref: str, grantee: str) -> Any:
        _, server = connected_server_client()
        return json_response(
            server.revoke_library_grant(
                validate_name(library_ref),
                validate_name(grantee),
            )
        )

    @app.post("/server/libraries/<library_ref>/references")
    @route_errors
    async def add_server_library_reference(library_ref: str) -> Any:
        _, server = connected_server_client()
        return json_response(
            server.add_library_reference(
                validate_name(library_ref),
                await read_json_body(),
            ),
            HTTPStatus.CREATED,
        )

    @app.post("/server/libraries/<library_ref>/copies")
    @route_errors
    async def copy_server_library_object(library_ref: str) -> Any:
        _, server = connected_server_client()
        return json_response(
            server.copy_object_into_library(
                validate_name(library_ref),
                await read_json_body(),
            ),
            HTTPStatus.CREATED,
        )

    @app.delete("/server/libraries/<library_ref>/entries/<name>")
    @route_errors
    async def remove_server_library_entry(library_ref: str, name: str) -> Any:
        _, server = connected_server_client()
        return json_response(
            server.remove_library_entry(
                validate_name(library_ref),
                validate_name(name),
            )
        )

    @app.post("/server/connect")
    @route_errors
    async def connect_server() -> Any:
        body = await read_json_body()
        machine_token = body.get("machine_token")
        user_token = body.get("user_token")
        if not machine_token or not user_token:
            raise ValueError("machine_token and user_token are required")

        server_url = body.get("server_url") or DEFAULT_SERVER_URL
        return json_response(
            runtime.connect_server(
                server_url=server_url,
                machine_token=machine_token,
                user_token=user_token,
                machine_id=body.get("machine_id"),
                display_name=body.get("display_name"),
                capabilities=body.get("capabilities") or {},
                heartbeat_interval_seconds=body.get("heartbeat_interval_seconds"),
            ),
            HTTPStatus.CREATED,
        )

    @app.post("/server/disconnect")
    @route_errors
    async def disconnect_server() -> Any:
        return json_response(runtime.disconnect_server())

    @app.get("/envs")
    @route_errors
    async def list_envs() -> Any:
        return json_response(runtime.store.list_envs())

    @app.post("/envs")
    @route_errors
    async def register_env() -> Any:
        body = await read_json_body()
        return json_response(
            runtime.store.register_env(body["name"], body["python"]),
            HTTPStatus.CREATED,
        )

    @app.get("/objects")
    @route_errors
    async def list_objects() -> Any:
        query = first_query_value("q", "query")
        view = (first_query_value("view") or "").lower()
        compact = (first_query_value("compact") or "").lower() in {
            "1",
            "true",
            "yes",
        }
        if query is None:
            records = runtime.store.list_objects()
            if view == "summary" or compact:
                return json_response(
                    {
                        name: summarize_object(record)
                        for name, record in records.items()
                    }
                )
            return json_response(records)

        records = runtime.store.search_objects(query)
        if view == "summary" or compact:
            return json_response([summarize_object(record) for record in records])
        return json_response(records)

    @app.get("/objects/search")
    @route_errors
    async def search_objects() -> Any:
        return json_response(
            runtime.store.search_objects(first_query_value("q", "query") or "")
        )

    @app.get("/objects/<name_or_id>")
    @route_errors
    async def get_object(name_or_id: str) -> Any:
        include_yaml = (first_query_value("include_yaml") or "").lower() in {
            "1",
            "true",
            "yes",
        }
        return json_response(
            object_from_local_or_server(
                validate_name(name_or_id),
                include_yaml=include_yaml,
            )
        )

    @app.get("/objects/<name_or_id>/signature")
    @route_errors
    async def object_signature(name_or_id: str) -> Any:
        object_name, function = object_function_ref(name_or_id)
        record = object_from_local_or_server(
            object_name,
            include_yaml=False,
        )
        return json_response(build_signature(record, function=function))

    @app.get("/objects/<name_or_id>/decomposition")
    @route_errors
    async def object_decomposition(name_or_id: str) -> Any:
        record = object_from_local_or_server(
            validate_name(name_or_id),
            include_yaml=False,
        )
        return json_response(record["decomposition"])

    @app.get("/objects/<name_or_id>/inputs")
    @route_errors
    async def object_inputs(name_or_id: str) -> Any:
        object_name, function = object_function_ref(name_or_id)
        record = object_from_local_or_server(
            object_name,
            include_yaml=False,
        )
        return json_response(build_signature(record, function=function)["inputs"])

    @app.get("/objects/<name_or_id>/outputs")
    @route_errors
    async def object_outputs(name_or_id: str) -> Any:
        object_name, function = object_function_ref(name_or_id)
        record = object_from_local_or_server(
            object_name,
            include_yaml=False,
        )
        return json_response(build_signature(record, function=function)["outputs"])

    @app.get("/objects/<name_or_id>/versions")
    @route_errors
    async def list_object_versions(name_or_id: str) -> Any:
        refresh = runtime.refresh_server_object_if_available(validate_name(name_or_id))
        if refresh and refresh.get("current_version"):
            name_or_id = refresh["current_version"]["name"]
        return json_response(
            runtime.store.list_object_versions(validate_name(name_or_id))
        )

    @app.post("/objects")
    @route_errors
    async def register_object() -> Any:
        body = await read_json_body()
        create_library = str(
            body.get("create_library", body.get("create"))
        ).strip().lower() in {"1", "true", "yes", "on"}
        record = runtime.register_object(
            body["name"],
            body["entrypoint"],
            body["env"],
            yaml_text=body.get("yaml"),
            yaml_path=body.get("yaml_path"),
            workdir=body.get("workdir"),
            description=body.get("description"),
            version_label=body.get("version_label"),
            object_id=body.get("object_id"),
            runtime_config=body.get("runtime_config"),
        )
        if not body.get("local_only", False):
            record["sync_event"] = runtime.enqueue_object_sync(
                record,
                library=body.get("library") or body.get("library_slug"),
                create_library=create_library,
                library_display_name=(
                    body.get("library_display_name")
                    or body.get("library_name")
                ),
            )
            try:
                record["sync"] = runtime.sync_once()
            except ServerClientError as exc:
                record["sync_error"] = exc.message
        record["environment_build"] = runtime.prepare_object_environment(record)
        return json_response(record, HTTPStatus.CREATED)

    @app.get("/environment-builds")
    @route_errors
    async def list_environment_builds() -> Any:
        return json_response(runtime.store.list_environment_builds())

    @app.get("/environment-builds/<spec_hash>")
    @route_errors
    async def get_environment_build(spec_hash: str) -> Any:
        record = runtime.store.get_environment_build(validate_name(spec_hash))
        if record is None:
            return json_response(
                {"error": f"environment build is not found: {spec_hash}"},
                HTTPStatus.NOT_FOUND,
            )
        return json_response(record)

    @app.post("/environment-builds/<spec_hash>/rebuild")
    @route_errors
    async def rebuild_environment(spec_hash: str) -> Any:
        body = await read_json_body()
        wait = bool(body.get("wait", False))
        resolved_spec_hash = validate_name(spec_hash)
        record = runtime.store.get_environment_build(resolved_spec_hash)
        if record is None:
            return json_response(
                {"error": f"environment build is not found: {spec_hash}"},
                HTTPStatus.NOT_FOUND,
            )
        manager = (
            runtime.docker_environment_manager
            if record.get("runtime_type") == "docker"
            else runtime.environment_manager
        )
        return json_response(
            manager.rebuild(resolved_spec_hash, wait=wait),
            HTTPStatus.ACCEPTED,
        )

    @app.post("/docker-images/prune")
    @route_errors
    async def prune_docker_images() -> Any:
        body = await read_json_body()
        spec_hash = body.get("spec_hash")
        return json_response(
            runtime.docker_environment_manager.prune_images(
                validate_name(spec_hash) if spec_hash else None
            )
        )

    @app.get("/remote-signatures")
    @route_errors
    async def list_remote_signatures() -> Any:
        return json_response(runtime.store.list_remote_signatures())

    @app.post("/remote-signatures/resolve")
    @route_errors
    async def resolve_remote_signature() -> Any:
        body = await read_json_body()
        ref = body.get("ref") or body
        force = bool(body.get("force", False))
        signature = runtime.resolve_remote_signature(ref, force=force)
        normalized = runtime._normalize_remote_ref(ref)
        return json_response(
            {
                "signature": signature,
                "cache": runtime.store.get_remote_signature(normalized),
            }
        )

    @app.post("/remote-decompositions/resolve")
    @route_errors
    async def resolve_remote_decomposition() -> Any:
        body = await read_json_body()
        ref = body.get("ref") or body
        return json_response(runtime.resolve_remote_decomposition(ref))

    @app.post("/remote-nodes/run")
    @route_errors
    async def run_remote_node() -> Any:
        body = await read_json_body()
        return json_response(
            runtime.run_remote_node(
                body["node"],
                kwargs=body.get("kwargs") or {},
                timeout_seconds=body.get("timeout_seconds"),
            )
        )

    @app.get("/runs")
    @route_errors
    async def list_runs() -> Any:
        return json_response(runtime.store.list_runs())

    @app.post("/runs")
    @route_errors
    async def start_run() -> Any:
        body = await read_json_body()
        if body.get("target_machine") or body.get("remote"):
            return json_response(
                runtime.start_remote_run(
                    body["object"],
                    target_machine=body.get("target_machine"),
                    object_owner_id=body.get("object_owner_id"),
                    library=body.get("library"),
                    args=body.get("args"),
                    kwargs=body.get("kwargs"),
                    output=body.get("output"),
                    timeout_seconds=body.get("timeout_seconds"),
                    version=body.get("version"),
                    object_version_id=body.get("version_id"),
                    function=body.get("function"),
                    correlation_id=body.get("correlation_id"),
                    parent_run_id=body.get("parent_run_id"),
                    context=body.get("context") or {},
                    offline_policy=body.get("offline_policy"),
                ),
                HTTPStatus.ACCEPTED,
            )
        return json_response(
            runtime.start_run(
                body["object"],
                args=body.get("args"),
                kwargs=body.get("kwargs"),
                output=body.get("output"),
                timeout_seconds=body.get("timeout_seconds"),
                version=body.get("version"),
                object_version_id=body.get("version_id"),
                function=body.get("function"),
                source=body.get("source", "auto"),
            ),
            HTTPStatus.ACCEPTED,
        )

    @app.get("/remote-runs/<run_id>")
    @route_errors
    async def get_remote_run(run_id: str) -> Any:
        credentials = runtime._require_connected_server_credentials()
        return json_response(
            ServerClient(
                credentials["server_url"],
                credentials["token"],
                user_token=credentials["user_token"],
            ).get_remote_run(validate_name(run_id))
        )

    @app.get("/remote-runs/<run_id>/artifacts")
    @route_errors
    async def list_remote_artifacts(run_id: str) -> Any:
        credentials = runtime._require_connected_server_credentials()
        artifacts = ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        ).list_artifacts(validate_name(run_id))
        return json_response([artifact["name"] for artifact in artifacts])

    @app.get("/remote-runs/<run_id>/artifacts/<artifact_name>")
    @route_errors
    async def get_remote_artifact(run_id: str, artifact_name: str) -> Any:
        credentials = runtime._require_connected_server_credentials()
        data = ServerClient(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
        ).artifact_bytes(validate_name(run_id), validate_name(artifact_name))
        return Response(
            data,
            status=int(HTTPStatus.OK),
            content_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{artifact_name}"'
            },
        )

    @app.get("/runs/<run_id>")
    @route_errors
    async def get_run(run_id: str) -> Any:
        return json_response(runtime.store.get_run(validate_name(run_id)))

    @app.get("/runs/<run_id>/result")
    @route_errors
    async def get_result(run_id: str) -> Any:
        state = runtime.store.get_run(validate_name(run_id))
        if state.get("result") is not None:
            return json_response(state["result"])

        result_path = Path(state["result_path"])
        if not result_path.exists():
            return json_response(
                {"error": "result is not available", "status": state["status"]},
                HTTPStatus.CONFLICT,
            )
        return json_response(json.loads(result_path.read_text(encoding="utf-8")))

    @app.get("/runs/<run_id>/artifacts")
    @route_errors
    async def list_artifacts(run_id: str) -> Any:
        state = runtime.store.get_run(validate_name(run_id))
        artifacts_dir = Path(state["artifacts_dir"])
        if not artifacts_dir.exists():
            return json_response([])
        return json_response(sorted(path.name for path in artifacts_dir.iterdir()))

    @app.get("/runs/<run_id>/artifacts/<artifact_name>")
    @route_errors
    async def get_artifact(run_id: str, artifact_name: str) -> Any:
        state = runtime.store.get_run(validate_name(run_id))
        artifact_path = Path(state["artifacts_dir"]) / validate_name(artifact_name)
        if not artifact_path.exists() or not artifact_path.is_file():
            return json_response(
                {"error": "artifact is not found"},
                HTTPStatus.NOT_FOUND,
            )

        return Response(
            artifact_path.read_bytes(),
            status=int(HTTPStatus.OK),
            content_type="application/octet-stream",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="{artifact_path.name}"'
                )
            },
        )

    return app


def make_server(host: str, port: int, store: RegistryStore) -> Any:
    """Backward-compatible factory name; returns a Quart app."""

    _ = (host, port)
    return create_app(store)


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
