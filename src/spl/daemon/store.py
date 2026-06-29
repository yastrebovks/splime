"""Persistent SQLite registry facade used by the local SPL daemon."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from spl.daemon.repositories import (
    EnvRepository,
    LibraryRepository,
    ObjectRepository,
    RunRepository,
    ServerConnectionRepository,
    SyncEventRepository,
)
from spl.daemon.storage_base import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    FUNCTION_REF_SEPARATOR,
    NAME_PATTERN,
    REDACTED_SECRET_VALUE,
    StorageBase,
    iso_after_now,
    json_dumps,
    json_loads,
    normalize_heartbeat_interval,
    read_json,
    split_object_function_ref,
    utc_now,
    validate_name,
    write_json,
)


class RegistryStore:
    """Compatibility facade over focused daemon registry repositories."""

    def __init__(self, home: Path | None = None):
        self._storage = StorageBase(home)
        self.envs = EnvRepository(self._storage)
        self.server_connections = ServerConnectionRepository(self._storage)
        self.sync_events = SyncEventRepository(self._storage)
        self.libraries = LibraryRepository(self._storage)
        self.objects = ObjectRepository(self._storage)
        self.runs = RunRepository(self._storage)
        self._storage.register_repositories(
            self.envs,
            self.server_connections,
            self.sync_events,
            self.libraries,
            self.objects,
            self.runs,
        )
        self.bootstrap()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._storage, name)

    def bootstrap(self) -> None:
        """Create the database schema and local run directories."""

        self._storage.bootstrap(
            migrate_legacy_registry=self.objects._migrate_legacy_registry,
            migrate_server_connection_secrets_locked=(
                self.server_connections._migrate_server_connection_secrets_locked
            ),
            backfill_object_kinds_locked=self.objects._backfill_object_kinds_locked,
            backfill_object_decomposition_locked=(
                self.objects._backfill_object_decomposition_locked
            ),
        )

    def close(self) -> None:
        """Close the SQLite connection held by this store."""

        self._storage.close()

    def __enter__(self) -> "RegistryStore":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def register_env(self, name: str, python: str) -> dict[str, Any]:
        return self.envs.register_env(name, python)

    def list_envs(self) -> dict[str, Any]:
        return self.envs.list_envs()

    def get_env(self, name: str) -> dict[str, Any]:
        return self.envs.get_env(name)

    def get_environment_build(self, spec_hash: str) -> dict[str, Any] | None:
        return self.envs.get_environment_build(spec_hash)

    def list_environment_builds(self) -> list[dict[str, Any]]:
        return self.envs.list_environment_builds()

    def upsert_environment_build(
        self,
        *,
        spec_hash: str,
        base_python: str,
        python_version: str,
        distributions: list[dict[str, Any]],
        runtime_packages: list[dict[str, Any]],
        spec: dict[str, Any],
        venv_path: Path,
        python_path: Path,
        install_log_path: Path,
        status: str,
        runtime_type: str = "venv",
        image_tag: str | None = None,
        base_image: str | None = None,
    ) -> dict[str, Any]:
        return self.envs.upsert_environment_build(
            spec_hash=spec_hash,
            base_python=base_python,
            python_version=python_version,
            distributions=distributions,
            runtime_packages=runtime_packages,
            spec=spec,
            venv_path=venv_path,
            python_path=python_path,
            install_log_path=install_log_path,
            status=status,
            runtime_type=runtime_type,
            image_tag=image_tag,
            base_image=base_image,
        )

    def update_environment_build(
        self,
        spec_hash: str,
        *,
        status: str,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        return self.envs.update_environment_build(
            spec_hash,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            error=error,
        )

    def environment_spec_hash_for(
        self,
        base_python: str,
        distributions: list[dict[str, Any]],
        *,
        python_version: str | None = None,
        runtime_packages: list[dict[str, Any]] | None = None,
    ) -> str:
        return self.envs.environment_spec_hash_for(
            base_python,
            distributions,
            python_version=python_version,
            runtime_packages=runtime_packages,
        )

    def environment_runtime_packages_for(
        self,
        distributions: list[dict[str, Any]],
    ) -> list[dict[str, str | None]]:
        return self.envs.environment_runtime_packages_for(distributions)

    def save_server_connection(
        self,
        *,
        server_url: str,
        token: str,
        user_token: str,
        connection: dict[str, Any],
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        return self.server_connections.save_server_connection(
            server_url=server_url,
            token=token,
            user_token=user_token,
            connection=connection,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )

    def save_pending_server_connection(
        self,
        *,
        server_url: str,
        token: str,
        user_token: str,
        machine_id: str,
        display_name: str | None = None,
        capabilities: dict[str, Any] | None = None,
        heartbeat_interval_seconds: float | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        return self.server_connections.save_pending_server_connection(
            server_url=server_url,
            token=token,
            user_token=user_token,
            machine_id=machine_id,
            display_name=display_name,
            capabilities=capabilities,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            error=error,
        )

    def complete_server_connection(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any],
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        return self.server_connections.complete_server_connection(
            connection_id,
            remote_connection=remote_connection,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )

    def get_server_connection(self, connection_id: str) -> dict[str, Any]:
        return self.server_connections.get_server_connection(connection_id)

    def get_server_connection_credentials(self, connection_id: str) -> dict[str, Any]:
        return self.server_connections.get_server_connection_credentials(connection_id)

    def current_server_connection(self) -> dict[str, Any] | None:
        return self.server_connections.current_server_connection()

    def current_server_connection_credentials(self) -> dict[str, Any] | None:
        return self.server_connections.current_server_connection_credentials()

    def list_server_connections(self) -> list[dict[str, Any]]:
        return self.server_connections.list_server_connections()

    def record_server_connection_heartbeat(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any],
    ) -> dict[str, Any]:
        return self.server_connections.record_server_connection_heartbeat(connection_id, remote_connection=remote_connection)

    def record_server_connection_library_snapshot(
        self,
        connection_id: str,
        *,
        snapshot_hash: str,
    ) -> dict[str, Any]:
        return self.server_connections.record_server_connection_library_snapshot(connection_id, snapshot_hash=snapshot_hash)

    def record_server_connection_error(
        self,
        connection_id: str,
        *,
        status: str,
        error: str,
    ) -> dict[str, Any]:
        return self.server_connections.record_server_connection_error(
            connection_id,
            status=status,
            error=error,
        )

    def mark_server_connection_disconnected(
        self,
        connection_id: str,
        *,
        remote_connection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.server_connections.mark_server_connection_disconnected(connection_id, remote_connection=remote_connection)

    def enqueue_sync_event(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self.sync_events.enqueue_sync_event(kind, payload)

    def get_sync_event(self, event_id: str) -> dict[str, Any]:
        return self.sync_events.get_sync_event(event_id)

    def list_pending_sync_events(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.sync_events.list_pending_sync_events(limit)

    def mark_sync_event_sent(self, event_id: str) -> dict[str, Any]:
        return self.sync_events.mark_sync_event_sent(event_id)

    def mark_sync_event_failed(self, event_id: str, error: str) -> dict[str, Any]:
        return self.sync_events.mark_sync_event_failed(event_id, error)

    def remote_signature_key_for(self, ref: dict[str, Any]) -> str:
        return self.libraries.remote_signature_key_for(ref)

    def get_remote_signature(self, ref: dict[str, Any]) -> dict[str, Any] | None:
        return self.libraries.get_remote_signature(ref)

    def list_remote_signatures(self) -> list[dict[str, Any]]:
        return self.libraries.list_remote_signatures()

    def save_remote_signature(
        self,
        ref: dict[str, Any],
        signature: dict[str, Any],
        *,
        status: str = "resolved",
        error: str | None = None,
    ) -> dict[str, Any]:
        return self.libraries.save_remote_signature(
            ref,
            signature,
            status=status,
            error=error,
        )

    def mark_remote_signature_unavailable(
        self,
        ref: dict[str, Any],
        error: str,
    ) -> dict[str, Any]:
        return self.libraries.mark_remote_signature_unavailable(ref, error)

    def register_object(
        self,
        name: str,
        entrypoint: str,
        env: str,
        *,
        yaml_text: str | None = None,
        yaml_path: str | None = None,
        workdir: str | None = None,
        runtime_config: dict[str, Any] | None = None,
        description: str | None = None,
        version_label: str | None = None,
        object_id: str | None = None,
        origin: str = "local",
        remote_owner_id: str | None = None,
        remote_object_id: str | None = None,
        remote_version_id: str | None = None,
        remote_name: str | None = None,
        remote_signature_resolver: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return self.objects.register_object(
            name,
            entrypoint,
            env,
            yaml_text=yaml_text,
            yaml_path=yaml_path,
            workdir=workdir,
            runtime_config=runtime_config,
            description=description,
            version_label=version_label,
            object_id=object_id,
            origin=origin,
            remote_owner_id=remote_owner_id,
            remote_object_id=remote_object_id,
            remote_version_id=remote_version_id,
            remote_name=remote_name,
            remote_signature_resolver=remote_signature_resolver,
        )

    def list_objects(self) -> dict[str, Any]:
        return self.objects.list_objects()

    def search_objects(self, query: str) -> list[dict[str, Any]]:
        return self.objects.search_objects(query)

    def get_object_decomposition(self, object_version_id: str) -> dict[str, Any]:
        return self.objects.get_object_decomposition(object_version_id)

    def get_object(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        return self.objects.get_object(
            name_or_id,
            version=version,
            include_yaml=include_yaml,
        )

    def get_object_version(
        self,
        version_id: str,
        *,
        include_yaml: bool = True,
    ) -> dict[str, Any]:
        return self.objects.get_object_version(version_id, include_yaml=include_yaml)

    def get_object_by_remote_version(
        self,
        remote_version_id: str,
        *,
        include_yaml: bool = False,
    ) -> dict[str, Any] | None:
        return self.objects.get_object_by_remote_version(remote_version_id, include_yaml=include_yaml)

    def list_object_versions_by_remote_object(
        self,
        remote_object_id: str,
    ) -> list[dict[str, Any]]:
        return self.objects.list_object_versions_by_remote_object(remote_object_id)

    def list_object_versions(self, name_or_id: str) -> list[dict[str, Any]]:
        return self.objects.list_object_versions(name_or_id)

    def create_run(
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
    ) -> dict[str, Any]:
        return self.runs.create_run(
            object_name,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            version=version,
            object_version_id=object_version_id,
            function=function,
        )

    def update_run(self, run_id: str, **changes: Any) -> dict[str, Any]:
        return self.runs.update_run(run_id, **changes)

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self.runs.get_run(run_id)

    def list_runs(self) -> list[dict[str, Any]]:
        return self.runs.list_runs()
