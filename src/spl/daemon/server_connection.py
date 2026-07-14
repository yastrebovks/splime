"""Server connection lifecycle management for the local daemon."""

from __future__ import annotations

import hashlib
from typing import Any

from spl.daemon.repositories.server_connection import SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
from spl.daemon.remote_client import ServerClientError
from spl.daemon.runtime_dependencies import (
    ServerClientFactoryProtocol,
    ServerClientProtocol,
)
from spl.daemon.store import RegistryStore, validate_name


class ServerOfflineError(RuntimeError):
    """Raised when a server-backed operation is requested while offline."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "central_server_offline",
        detail: str | None = None,
    ):
        self.code = code
        self.detail = detail
        super().__init__(message)


class HandleRequiresServerConnectionError(KeyError):
    """Raised when a handle reaches local storage without a live server."""

    code = "handle_requires_server_connection"

    def __init__(self, owner: str):
        self.owner = owner
        self.message = (
            f"cannot resolve owner {owner!r}: handles resolve on the server; "
            "connect first with client.connect_server(...) or pass the canonical owner id"
        )
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


SERVER_OFFLINE_MESSAGE = (
    "central SPL daemon server is offline or unreachable. Local registry, "
    "local runs, and pending sync events remain available; server-backed "
    "operations require connectivity and should be retried after the daemon "
    "reconnects."
)
SERVER_UNREACHABLE_CODE = "central_server_unreachable"
SERVER_PROXY_TIMEOUT_SECONDS = 1.0


class ServerConnectionManager:
    """Manage central-server connection records and remote lease calls."""

    def __init__(
        self,
        store: RegistryStore,
        server_client_factory: ServerClientFactoryProtocol,
    ):
        self.store = store
        self.server_client_factory = server_client_factory

    def server_client(
        self,
        server_url: str,
        token: str,
        *,
        user_token: str | None,
        request_timeout_seconds: float | None = None,
    ) -> ServerClientProtocol:
        if request_timeout_seconds is None:
            return self.server_client_factory(server_url, token, user_token=user_token)
        try:
            return self.server_client_factory(
                server_url,
                token,
                user_token=user_token,
                request_timeout_seconds=request_timeout_seconds,
            )
        except TypeError as exc:
            if "request_timeout_seconds" not in str(exc):
                raise
            return self.server_client_factory(server_url, token, user_token=user_token)

    def server_client_for_credentials(
        self,
        credentials: dict[str, Any],
        *,
        request_timeout_seconds: float | None = None,
    ) -> ServerClientProtocol:
        return self.server_client(
            credentials["server_url"],
            credentials["token"],
            user_token=credentials["user_token"],
            request_timeout_seconds=request_timeout_seconds,
        )

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
        """Connect to the central daemon server."""

        existing = self.matching_server_connection(
            server_url=server_url,
            machine_token=machine_token,
            user_token=user_token,
            machine_id=machine_id,
        )
        if existing is not None and existing.get("remote_connection_id"):
            local_connection = self.store.get_server_connection(existing["id"])
            display_name_refresh = bool(
                display_name
                and display_name != local_connection.get("display_name")
                and self._is_technical_machine_label(
                    local_connection.get("display_name"),
                    local_connection["machine_id"],
                )
            )
            if local_connection.get("status") != "connected" or display_name_refresh:
                server = self.server_client(
                    server_url,
                    machine_token,
                    user_token=user_token,
                )
                try:
                    remote_connection = server.connect_machine(
                        machine_id=machine_id or local_connection["machine_id"],
                        display_name=display_name,
                        capabilities=capabilities,
                        heartbeat_interval_seconds=heartbeat_interval_seconds,
                    )
                except ServerClientError as exc:
                    if not self._is_server_connectivity_error(exc):
                        raise
                    local_connection = self.store.record_server_connection_error(
                        existing["id"],
                        status="connect_failed",
                        error=exc.message,
                    )
                    return {
                        "connected": False,
                        "offline": True,
                        "reused": True,
                        "connection": local_connection,
                        "remote_connection": None,
                        "error": SERVER_OFFLINE_MESSAGE,
                        "detail": exc.message,
                    }
                local_connection = self.store.complete_server_connection(
                    existing["id"],
                    remote_connection=remote_connection,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                )
                return {
                    "connected": True,
                    "reused": True,
                    "refreshed": True,
                    "connection": local_connection,
                    "remote_connection": remote_connection,
                }
            return {
                "connected": local_connection["status"] == "connected",
                "offline": local_connection["status"]
                in {"heartbeat_failed", "connect_failed", SERVER_CONNECTION_STATUS_NEEDS_RECONNECT},
                "reused": True,
                "connection": local_connection,
                "remote_connection": self.remote_connection_snapshot(local_connection),
            }

        server = self.server_client(
            server_url,
            machine_token,
            user_token=user_token,
        )
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
            pending = self.store.find_pending_server_connection(
                server_url=server_url,
                machine_id=remote_connection["machine_id"],
            )
            if pending is not None:
                local_connection = self.store.complete_server_connection(
                    pending["id"],
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
        return {
            "connected": True,
            "connection": local_connection,
            "remote_connection": remote_connection,
        }

    def disconnect_server(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Gracefully disconnect the current central-server lease."""

        credentials = credentials or self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError("active server connection is not found")

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
        remote_connection = self.server_client_for_credentials(credentials).disconnect_machine()
        local_connection = self.store.mark_server_connection_disconnected(
            credentials["id"],
            remote_connection=remote_connection,
        )
        return {
            "connected": False,
            "connection": local_connection,
            "remote_connection": remote_connection,
        }

    def matching_server_connection(
        self,
        *,
        server_url: str,
        machine_token: str,
        user_token: str,
        machine_id: str | None,
    ) -> dict[str, Any] | None:
        """Return the active connection when the requested credentials match."""

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

    @staticmethod
    def _is_technical_machine_label(value: Any, machine_id: str) -> bool:
        label = str(value or "").strip()
        if not label:
            return True
        folded = label.casefold()
        if folded == str(machine_id).casefold():
            return True
        if folded == f"mach_{machine_id}".casefold():
            return True
        if folded.startswith(("mach_", "mach-")):
            return True
        if not folded.startswith("machine"):
            return False
        suffix = folded.removeprefix("machine")
        if suffix.startswith(("_", "-")):
            suffix = suffix[1:]
        return len(suffix) >= 8 and all(char in "0123456789abcdef" for char in suffix)

    def restore_pending_server_connection(
        self,
        credentials: dict[str, Any],
    ) -> dict[str, Any]:
        if credentials.get("remote_connection_id"):
            return credentials
        server = self.server_client_for_credentials(credentials)
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

    def require_connected_server_credentials(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        credentials = credentials or self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError("active server connection is not found")
        if not credentials.get("remote_connection_id"):
            raise ServerOfflineError(SERVER_OFFLINE_MESSAGE)
        if credentials.get("status") == SERVER_CONNECTION_STATUS_NEEDS_RECONNECT:
            raise ServerOfflineError(
                "central SPL daemon server lease was rejected; reconnect with client.connect_server(...) to restore sync.",
            )
        return credentials

    def remote_connection_snapshot(self, connection: dict[str, Any]) -> dict[str, Any]:
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
