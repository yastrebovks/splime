"""Protocols for DaemonRuntime collaborators."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from spl.daemon.environment_base import EnvironmentManagerProtocol

if TYPE_CHECKING:
    from spl.daemon.runtime_backend import RunContext

__all__ = [
    "DockerEnvironmentBuilderProtocol",
    "DockerEnvironmentManagerProtocol",
    "DockerPoolRunnerProtocol",
    "EnvironmentManagerProtocol",
    "HeartbeatsProtocol",
    "RuntimeBackendProtocol",
    "ServerClientFactoryProtocol",
    "ServerClientProtocol",
    "ServerConnectionsProtocol",
    "SyncVisibilityProtocol",
]


class DockerEnvironmentManagerProtocol(EnvironmentManagerProtocol, Protocol):
    """Docker environment manager surface used by the daemon runtime."""

    def prune_images(self, spec_hash: str | None = None) -> list[dict[str, Any]]:
        """Remove cached Docker images and mark their build records absent."""
        ...


class DockerEnvironmentBuilderProtocol(Protocol):
    """Docker environment build surface needed by Docker runtime components."""

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Return the cached Docker build status for an object version."""
        ...

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        """Prepare a Docker image and return its build record."""
        ...


class DockerPoolRunnerProtocol(Protocol):
    """Docker pool surface needed by the Docker runtime backend."""

    @property
    def should_prewarm(self) -> bool:
        """Return whether objects should be prewarmed after prepare."""
        ...

    def can_use(self, run_dir: Path, workdir: Path) -> bool:
        """Return whether a run can use a warm pooled container."""
        ...

    def ensure_container(
        self,
        *,
        object_record: dict[str, Any],
        image_tag: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Return a warm pooled container record."""
        ...

    def exec_worker_command(
        self,
        *,
        object_record: dict[str, Any],
        entrypoint: str,
        run_id: str,
        container_name: str,
        runtime_config: dict[str, Any],
    ) -> list[str]:
        """Build the command for executing inside a pooled container."""
        ...

    def worker_command(
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
        """Build the command for a one-shot Docker container."""
        ...

    def use_container(self, record: dict[str, Any]) -> Any:
        """Return a context manager marking a pooled container in use."""
        ...

    def remove_container(self, name: str) -> None:
        """Remove a Docker container by name."""
        ...

    def prewarm_object(self, object_record: dict[str, Any]) -> None:
        """Start asynchronous prewarming for an object."""
        ...

    def cleanup_stale_containers(self) -> None:
        """Remove stale warm containers left by previous daemon processes."""
        ...

    def shutdown(self) -> None:
        """Stop all warm containers owned by this pool."""
        ...


class RuntimeBackendProtocol(Protocol):
    """Worker runtime backend contract used by the daemon executor."""

    def __enter__(self) -> RuntimeBackendProtocol:
        """Enter backend-specific run lifecycle."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        """Leave backend-specific run lifecycle."""
        ...

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Return the cached build status for an object version."""
        ...

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        """Prepare the runtime environment and return its build record."""
        ...

    def build_command(self, ctx: RunContext) -> list[str]:
        """Return the command used to execute the worker."""
        ...

    def run_state_fields(self) -> dict[str, Any]:
        """Return backend-specific persisted run fields."""
        ...

    def after_prepare(self, object_record: dict[str, Any]) -> None:
        """Run optional post-prepare work such as prewarming."""
        ...

    def after_run(self, ctx: RunContext) -> dict[str, Any]:
        """Return persisted run fields collected after subprocess completion."""
        ...

    def process_result(
        self,
        ctx: RunContext,
        result_payload: dict[str, Any],
    ) -> bool:
        """Mutate a successful worker result and return whether it changed."""
        ...


class SyncVisibilityProtocol(Protocol):
    """Diagnostic sync visibility surface used by the daemon runtime."""

    def summary(
        self,
        events: list[dict[str, Any]] | None = None,
        *,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Return a stable summary of pending sync work."""
        ...

    def pending_events(self, *, limit: int = 200) -> list[dict[str, Any]]:
        """Return decorated pending sync events."""
        ...


class ServerClientProtocol(Protocol):
    """Central server client surface used by daemon runtime and routes."""

    def connect_machine(
        self,
        *,
        machine_id: str | None = None,
        display_name: str | None = None,
        capabilities: dict[str, Any] | None = None,
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Connect this machine to the central server."""
        ...

    def heartbeat_connection(
        self,
        *,
        connection_id: str,
        machine_id: str,
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Send one server heartbeat."""
        ...

    def disconnect_machine(self) -> dict[str, Any]:
        """Disconnect this machine from the central server."""
        ...

    def list_machines(self) -> list[dict[str, Any]]:
        """Return machines visible to the connected identity."""
        ...

    def list_tokens(self) -> list[dict[str, Any]]:
        """Return tokens visible to the connected user identity."""
        ...

    def list_libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        """Return visible server libraries."""
        ...

    def create_library(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Create a server library."""
        ...

    def get_library(self, library_ref: str) -> dict[str, Any]:
        """Return a server library."""
        ...

    def update_library(self, library_ref: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Update a server library."""
        ...

    def delete_library(self, library_ref: str) -> dict[str, Any]:
        """Delete a server library when upstream support is added."""
        ...

    def list_library_grants(self, library_ref: str) -> list[dict[str, Any]]:
        """Return library grants."""
        ...

    def grant_library(self, library_ref: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Grant library access."""
        ...

    def revoke_library_grant(self, library_ref: str, grantee: str) -> dict[str, Any]:
        """Revoke library access."""
        ...

    def add_library_reference(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Add an object reference to a library."""
        ...

    def copy_object_into_library(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Copy an object into a library."""
        ...

    def remove_library_entry(self, library_ref: str, name: str) -> dict[str, Any]:
        """Remove an object from a library."""
        ...

    def list_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        """Return server objects."""
        ...

    def latest_machine_library_snapshot(
        self,
        machine_id: str,
        *,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        """Return the latest machine library snapshot."""
        ...

    def get_object(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Return a server object."""
        ...

    def object_signature(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> dict[str, Any]:
        """Return a server object signature."""
        ...

    def list_object_versions(
        self,
        name_or_id: str,
        *,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return server object versions."""
        ...

    def sync(
        self,
        *,
        connection_id: str,
        machine_id: str,
        heartbeat_interval_seconds: float,
        events: list[dict[str, Any]],
        capabilities: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Send one sync request."""
        ...

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        """Return a remote run."""
        ...

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        """Return remote run artifacts."""
        ...

    def upload_artifact(self, run_id: str, name: str, path: str | Path) -> dict[str, Any]:
        """Upload a remote run artifact."""
        ...

    def artifact_bytes(self, run_id: str, name: str) -> bytes:
        """Return remote artifact bytes."""
        ...


class ServerClientFactoryProtocol(Protocol):
    """Factory for central server clients."""

    def __call__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
    ) -> ServerClientProtocol:
        """Return a central server client."""
        ...


class ServerConnectionsProtocol(Protocol):
    """Server connection lifecycle surface used by daemon runtime."""

    def server_client(
        self,
        server_url: str,
        token: str,
        *,
        user_token: str | None,
    ) -> ServerClientProtocol:
        """Return a central server client for explicit credentials."""
        ...

    def server_client_for_credentials(
        self,
        credentials: dict[str, Any],
    ) -> ServerClientProtocol:
        """Return a central server client for stored credentials."""
        ...

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
        ...

    def disconnect_server(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Disconnect from the central daemon server."""
        ...

    def matching_server_connection(
        self,
        *,
        server_url: str,
        machine_token: str,
        user_token: str,
        machine_id: str | None,
    ) -> dict[str, Any] | None:
        """Return matching stored server credentials, if any."""
        ...

    def restore_pending_server_connection(
        self,
        credentials: dict[str, Any],
    ) -> dict[str, Any]:
        """Reconnect a pending offline server connection."""
        ...

    def require_connected_server_credentials(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return connected server credentials or raise."""
        ...

    def remote_connection_snapshot(
        self,
        connection: dict[str, Any],
    ) -> dict[str, Any]:
        """Return a server-shaped snapshot for a stored connection."""
        ...


class HeartbeatsProtocol(Protocol):
    """Server heartbeat lifecycle surface used by daemon runtime."""

    def restore_server_heartbeat(self) -> None:
        """Restore heartbeat for the current connection."""
        ...

    def start_server_heartbeat(
        self,
        connection: dict[str, Any],
        *,
        token: str,
    ) -> None:
        """Start heartbeat for one connection."""
        ...

    def stop_server_heartbeat(self, connection_id: str) -> None:
        """Stop heartbeat for one connection."""
        ...

    def shutdown(self) -> None:
        """Stop all heartbeat activity."""
        ...
