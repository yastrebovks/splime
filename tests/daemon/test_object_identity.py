from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import shutil
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

import spl.daemon.metadata as metadata_module
from spl.daemon.repositories.server_connection import SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
from spl.daemon.remote_client import ServerClientError
from spl.daemon.server import DaemonRuntime
from spl.daemon.services.sync import SyncVisibilityService
from spl.daemon.storage_base import DEFAULT_OBJECT_LIBRARY, DEFAULT_OBJECT_OWNER_ID
from spl.daemon.storage_base import StorageBase
from spl.daemon.store import RegistryStore


FUNCTION_YAML = """\
- !DFunction
  name: demo_obj
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 1
"""


FUNCTION_YAML_V2 = """\
- !DFunction
  name: demo_obj
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 2
"""


PIPELINE_YAML = """\
- !DPipeline
  name: demo_obj
  nodes: []
  links: []
  aliases: []
"""


def _adapter_pipeline_yaml(
    *,
    save_name: str = "save_text",
    adapter_distribution_version: str = "1.0",
    reverse_dependencies: bool = False,
) -> str:
    dependencies = [
        """\
- !DFunction
  name: step
  inputs: []
  outputs:
  - name: default
    type: str
  body: |-
    return "ok"
""",
        f"""\
- !DFunction
  name: {save_name}
  inputs: []
  outputs: []
  body: |-
    pass
""",
        """\
- !DFunction
  name: load_text
  inputs: []
  outputs: []
  body: |-
    pass
""",
    ]
    if reverse_dependencies:
        dependencies = list(reversed(dependencies))
    return (
        """\
- !DPipeline
  name: demo_pipeline
  nodes:
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000001
    func: step
  links: []
  aliases:
  - [out, 00000000-0000-0000-0000-000000000001]
  adapters:
  - !DAdapter
    key: builtins.str@txt
    save: """
        + save_name
        + """
    load: load_text
    distributions:
    - !DDistribution
      package: demo-adapter
      version: """
        + adapter_distribution_version
        + "\n"
        + "".join(dependencies)
    )


class _NoopDockerPool:
    def cleanup_stale_containers(self) -> None:
        pass

    def shutdown(self) -> None:
        pass


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection, *, token: str) -> None:
        pass

    def ensure_server_heartbeat(self, connection=None) -> None:
        pass

    def status(self, connection_id: str | None = None) -> dict[str, object]:
        return {"connection_id": connection_id, "thread_alive": False, "last_tick_at": None}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


def _unique_columns(conn: sqlite3.Connection, table: str) -> set[tuple[str, ...]]:
    uniques = set()
    for index in conn.execute(f"PRAGMA index_list({table})").fetchall():
        if not index[2]:
            continue
        columns = tuple(row[2] for row in conn.execute(f"PRAGMA index_info({index[1]})").fetchall())
        uniques.add(columns)
    return uniques


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _test_credential(label: str) -> str:
    return hashlib.sha256(f"spl-test-credential:{label}".encode("utf-8")).hexdigest()


def _save_connected_owner_credentials(store: RegistryStore, *, owner_id: str = "owner-1") -> dict[str, Any]:
    return store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-123456",
        user_token="user-token-123456",
        connection={
            "id": "remote-connection-1",
            "owner_id": owner_id,
            "subject_type": "machine",
            "subject_id": "machine-1",
            "machine_id": "machine-1",
            "display_name": "lab-machine",
            "status": "connected",
            "capabilities": {},
        },
        heartbeat_interval_seconds=60,
    )


def _drop_remote_connection_lease(store: RegistryStore, connection_id: str) -> None:
    with store._lock, store._conn:  # noqa: SLF001 - regression seeds post-restart offline state.
        store._conn.execute(
            """
            UPDATE server_connections
            SET remote_connection_id = NULL,
                status = 'connect_failed',
                error = 'offline after restart'
            WHERE id = ?
            """,
            (connection_id,),
        )


def _insert_server_connection_row(
    store: RegistryStore,
    *,
    connection_id: str,
    machine_id: str,
    owner_id: str | None,
    status: str = "connected",
    updated_at: str = "2999-01-01T00:00:00+00:00",
) -> None:
    with store._lock, store._conn:  # noqa: SLF001 - regression seed for multi-identity rows.
        store._conn.execute(
            """
            INSERT INTO server_connections(
                id, server_url, token_hint, user_token_hint,
                token_secret_ref, user_token_secret_ref,
                token_redacted, user_token_redacted,
                remote_connection_id, owner_id, subject_type, subject_id,
                machine_id, display_name, capabilities_json, status,
                heartbeat_interval_seconds, last_heartbeat_at, next_heartbeat_at,
                lease_expires_at, last_library_snapshot_hash,
                last_library_snapshot_at, created_at, connected_at,
                disconnected_at, updated_at, error
            )
            VALUES(?, ?, ?, ?, NULL, NULL, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, NULL, ?, NULL)
            """,
            (
                connection_id,
                "https://splime.io/api",
                "...foreign",
                "...foreign",
                "<redacted>",
                f"remote-{connection_id}",
                owner_id,
                "machine",
                machine_id,
                machine_id,
                machine_id,
                "{}",
                status,
                60.0,
                updated_at,
                updated_at,
                updated_at,
                updated_at if status == "connected" else None,
                updated_at,
            ),
        )


def _assert_identity_invariants(home: Path) -> None:
    with sqlite3.connect(home / "daemon.sqlite3") as conn:
        duplicate_objects = conn.execute(
            """
            SELECT owner_id, library, name, COUNT(*)
            FROM objects
            GROUP BY owner_id, library, name
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        assert duplicate_objects == []

        duplicate_versions = conn.execute(
            """
            SELECT object_id, content_hash, COUNT(*)
            FROM object_versions
            WHERE content_hash IS NOT NULL
            GROUP BY object_id, content_hash
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        assert duplicate_versions == []

        current_pointers = conn.execute(
            """
            SELECT o.id, o.current_version_id, ov.object_id
            FROM objects o
            LEFT JOIN object_versions ov ON ov.id = o.current_version_id
            """
        ).fetchall()
        for object_id, current_version_id, current_object_id in current_pointers:
            assert current_version_id is None or current_object_id == object_id

        synthetic_names = [
            name
            for (name,) in conn.execute("SELECT name FROM objects").fetchall()
            if re.fullmatch(r"server\.[0-9a-fA-F]{8,}", name)
        ]
        assert synthetic_names == []


def test_caller_owner_id_uses_stored_owner_without_remote_connection_id(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        connection = _save_connected_owner_credentials(store, owner_id="owner-1")
        _drop_remote_connection_lease(store, connection["id"])

        assert store.objects._caller_owner_id() == "owner-1"  # noqa: SLF001
        assert store.objects._caller_owner_id("explicit-owner") == "explicit-owner"  # noqa: SLF001
    finally:
        store.close()


def test_caller_owner_id_defaults_without_enrollment_credentials(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        assert store.objects._caller_owner_id() == DEFAULT_OBJECT_OWNER_ID  # noqa: SLF001
    finally:
        store.close()


def test_needs_reconnect_identity_still_resolves_bare_names(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        connection = _save_connected_owner_credentials(store, owner_id="owner-a")
        store.register_object(
            "clean_amount",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            library="default",
        )
        store.record_server_connection_error(
            connection["id"],
            status=SERVER_CONNECTION_STATUS_NEEDS_RECONNECT,
            error="lease rejected by server (404): stale lease",
        )

        assert store.current_server_connection_credentials()["owner_id"] == "owner-a"
        assert store.objects._caller_owner_id() == "owner-a"  # noqa: SLF001
        assert store.get_object("clean_amount")["owner_id"] == "owner-a"
    finally:
        store.close()


def test_bare_lookup_names_cross_owner_local_matches(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        _save_connected_owner_credentials(store, owner_id="owner-b")
        store.register_object(
            "clean_amount",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="owner-a",
            library="default",
        )
        store.register_object(
            "clean_amount",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
            owner_id="owner-c",
            library="risk",
        )

        with pytest.raises(KeyError) as exc_info:
            store.get_object("clean_amount")

        message = str(exc_info.value)
        assert "registered locally under other owners" in message
        assert "owner 'owner-a' (library 'default')" in message
        assert "owner 'owner-c' (library 'risk')" in message
        assert "pass owner=/library=" in message
        assert "reconnect under that identity" in message

        scoped = store.get_object("clean_amount", owner_id="owner-a", library="default")
        assert scoped["canonical_name"] == "owner-a/default/clean_amount"
    finally:
        store.close()


def test_publish_cross_owner_fork_warns_with_existing_owner_version(tmp_path, caplog) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "shared_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="owner-a",
            library="default",
        )
        store.register_object(
            "shared_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
            owner_id="owner-a",
            library="default",
        )
        store.register_object(
            "shared_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML.replace("return 1", "return 3"),
            owner_id="owner-a",
            library="default",
        )
        _save_connected_owner_credentials(store, owner_id="owner-b")

        with caplog.at_level(logging.WARNING, logger="spl.daemon.repositories.object"):
            fork = store.register_object(
                "shared_obj",
                "demo_obj",
                "default",
                yaml_text=FUNCTION_YAML_V2,
                library="default",
            )

        warning = fork["warning"]
        assert warning == fork["warnings"][0]["message"]
        assert fork["warnings"][0]["type"] == "cross_owner_publish_fork"
        assert "'shared_obj' also exists locally under owner 'owner-a' (their v3)" in warning
        assert "Publishing as 'owner-b' creates a SEPARATE object" in warning
        assert "versions do not continue owner-a's chain" in warning
        assert "bare-name lookups resolve per current identity" in warning
        assert any(getattr(record, "spl_event", None) == "cross_owner_publish_fork" for record in caplog.records)
    finally:
        store.close()


def test_publish_cross_owner_fork_warning_is_not_emitted_for_single_owner_republish(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        first = store.register_object(
            "solo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="owner-a",
            library="default",
        )
        second = store.register_object(
            "solo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
            owner_id="owner-a",
            library="default",
        )

        assert "warning" not in first
        assert "warnings" not in first
        assert "warning" not in second
        assert "warnings" not in second
    finally:
        store.close()


def test_local_publish_after_same_name_mirror_warns_about_separate_owner_chain(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        mirror = store.register_object(
            "shared_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="owner-a",
            library="default",
            origin="server",
            remote_owner_id="owner-a",
            remote_object_id="remote-object-a",
            remote_version_id="remote-version-a-1",
        )
        # I-01 decision: mirror imports themselves are quiet, but a later local
        # publish under another owner gets the same fork warning as hand-authored
        # local objects because it starts an independent version chain.
        published = store.register_object(
            "shared_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
            owner_id="owner-b",
            library="default",
        )

        assert "warning" not in mirror
        assert "'shared_obj' also exists locally under owner 'owner-a' (their v1)" in published["warning"]
        assert "Publishing as 'owner-b' creates a SEPARATE object" in published["warning"]
    finally:
        store.close()


def test_bare_forget_names_cross_owner_match_and_scoped_forget_works(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "clean_amount",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="owner-a",
            library="default",
        )
        _save_connected_owner_credentials(store, owner_id="owner-b")

        with pytest.raises(KeyError) as exc_info:
            store.forget_object("clean_amount")

        message = str(exc_info.value)
        assert "registered locally under owner 'owner-a'" in message
        assert "library 'default'" in message
        assert "pass owner=/library=" in message
        assert store.get_object("clean_amount", owner_id="owner-a", library="default")["owner_id"] == "owner-a"

        receipt = store.forget_object("clean_amount", owner_id="owner-a", library="default")

        assert receipt["object"]["canonical_name"] == "owner-a/default/clean_amount"
        with pytest.raises(KeyError):
            store.get_object("clean_amount", owner_id="owner-a", library="default")
    finally:
        store.close()


def test_bare_forget_version_names_cross_owner_match_and_scoped_forget_version_works(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "versioned_amount",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="owner-a",
            library="default",
        )
        store.register_object(
            "versioned_amount",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
            owner_id="owner-a",
            library="default",
        )
        _save_connected_owner_credentials(store, owner_id="owner-b")

        with pytest.raises(KeyError) as exc_info:
            store.forget_object_version("versioned_amount", 1)

        message = str(exc_info.value)
        assert "registered locally under owner 'owner-a'" in message
        assert "library 'default'" in message
        assert (
            store.get_object("versioned_amount", version=1, owner_id="owner-a", library="default")["owner_id"]
            == "owner-a"
        )

        receipt = store.forget_object_version("versioned_amount", 1, owner_id="owner-a", library="default")

        assert receipt["object"]["canonical_name"] == "owner-a/default/versioned_amount"
        assert receipt["object_deleted"] is False
        assert store.get_object("versioned_amount", owner_id="owner-a", library="default")["version"] == 2
    finally:
        store.close()


def test_current_connection_prefers_local_machine_identity_over_newer_foreign_row(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        current = _save_connected_owner_credentials(store, owner_id="owner-a")
        _insert_server_connection_row(
            store,
            connection_id="foreign-connection",
            machine_id="machine-2",
            owner_id="owner-b",
            updated_at="2999-01-01T00:00:00+00:00",
        )

        assert store.current_server_connection()["id"] == current["id"]
        credentials = store.current_server_connection_credentials()
        assert credentials["id"] == current["id"]
        assert credentials["owner_id"] == "owner-a"
        assert credentials["token"] == "machine-token-123456"
    finally:
        store.close()


def test_current_connection_ignores_newer_active_missing_owner_row(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        current = _save_connected_owner_credentials(store, owner_id="owner-a")
        _insert_server_connection_row(
            store,
            connection_id="ownerless-connection",
            machine_id="machine-1",
            owner_id=None,
            status="connect_failed",
            updated_at="2999-01-01T00:00:00+00:00",
        )

        assert store.current_server_connection()["id"] == current["id"]
        credentials = store.current_server_connection_credentials()
        assert credentials["id"] == current["id"]
        assert credentials["owner_id"] == "owner-a"
    finally:
        store.close()


def test_caller_owner_id_warns_when_identity_degrades_with_only_ownerless_rows(
    tmp_path,
    caplog,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        _insert_server_connection_row(
            store,
            connection_id="ownerless-connection",
            machine_id="machine-1",
            owner_id=None,
            status="connect_failed",
            updated_at="2999-01-01T00:00:00+00:00",
        )

        assert store.current_server_connection() is None
        assert store.current_server_connection_credentials() is None
        with caplog.at_level(logging.WARNING, logger="spl.daemon.repositories.object"):
            assert store.objects._caller_owner_id() == DEFAULT_OBJECT_OWNER_ID  # noqa: SLF001

        assert "identity degraded to 'local'" in caplog.text
        assert "1 stored credential rows exist" in caplog.text
        summary = store.server_connection_summary()
        assert summary["identity_degraded"] is True
        assert summary["stored_credential_rows"] == 1
    finally:
        store.close()


def test_store_backfills_machine_identity_sidecar_from_single_owned_active_row(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        current = _save_connected_owner_credentials(store, owner_id="owner-a")
        sidecar_path = tmp_path / "server-machine-identity.json"
        sidecar_path.unlink()
        assert not sidecar_path.exists()
    finally:
        store.close()

    reopened = RegistryStore(tmp_path)
    try:
        assert json.loads(sidecar_path.read_text(encoding="utf-8")) == {"machine_id": "machine-1"}
        assert reopened.current_server_connection()["id"] == current["id"]
    finally:
        reopened.close()


def test_store_recovers_legacy_stale_lease_identity_across_restarts(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        current = _save_connected_owner_credentials(store, owner_id="owner-a")
        with store._lock, store._conn:  # noqa: SLF001 - seed a legacy K-01v1 victim row.
            store._conn.execute(
                """
                UPDATE server_connections
                SET status = 'stale',
                    error = 'lease rejected by server (404): stale lease'
                WHERE id = ?
                """,
                (current["id"],),
            )
    finally:
        store.close()

    first_reopen = RegistryStore(tmp_path)
    try:
        credentials = first_reopen.current_server_connection_credentials()
        assert credentials["id"] == current["id"]
        assert credentials["owner_id"] == "owner-a"
        assert credentials["status"] == SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
        assert credentials["token"] == "machine-token-123456"
    finally:
        first_reopen.close()

    second_reopen = RegistryStore(tmp_path)
    try:
        connections = second_reopen.list_server_connections()
        assert len(connections) == 1
        assert connections[0]["id"] == current["id"]
        assert connections[0]["status"] == SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
    finally:
        second_reopen.close()


def test_prune_server_connections_prunes_missing_owner_rows_even_if_newer(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        current = _save_connected_owner_credentials(store, owner_id="owner-a")
        with store._lock, store._conn:  # noqa: SLF001 - make the current row stale but protected.
            store._conn.execute(
                "UPDATE server_connections SET owner_id = NULL WHERE id = ?",
                (current["id"],),
            )
        _insert_server_connection_row(
            store,
            connection_id="old-disconnected",
            machine_id="machine-old",
            owner_id="owner-old",
            status="disconnected",
            updated_at="2000-01-01T00:00:00+00:00",
        )

        dry_run = store.prune_server_connections(older_than_days=None, dry_run=True)

        assert dry_run["dry_run"] is True
        assert [row["id"] for row in dry_run["stale"]] == [current["id"], "old-disconnected"]
        assert store.get_server_connection("old-disconnected")["id"] == "old-disconnected"

        receipt = store.prune_server_connections(older_than_days=None)

        assert receipt["count"] == 2
        assert [row["id"] for row in receipt["pruned"]] == [current["id"], "old-disconnected"]
        assert receipt["kept_current"] == []
        with pytest.raises(KeyError):
            store.get_server_connection(current["id"])
        with pytest.raises(KeyError):
            store.get_server_connection("old-disconnected")
    finally:
        store.close()


def test_forget_scoped_object_does_not_delete_pending_sync_events_for_other_owner(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "shared_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="owner-a",
            library="default",
        )
        store.register_object(
            "shared_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
            owner_id="owner-b",
            library="default",
        )
        foreign_event = store.enqueue_sync_event(
            "object_conflict",
            {
                # I-02 regression seed: a legacy/confused payload can share the
                # scoped canonical name while still belonging to another owner.
                "canonical_name": "owner-b/default/shared_obj",
                "owner_id": "owner-a",
                "library": "default",
                "name": "shared_obj",
            },
        )

        result = store.forget_object("shared_obj", owner_id="owner-b", library="default")

        assert result["object"]["canonical_name"] == "owner-b/default/shared_obj"
        assert store.get_sync_event(foreign_event["id"])["status"] == "pending"
    finally:
        store.close()


def _create_old_identity_db(home: Path) -> tuple[Path, str]:
    db_path = home / "daemon.sqlite3"
    python_path = home / "python"
    python_path.touch()
    yaml_sha256 = hashlib.sha256(FUNCTION_YAML.encode("utf-8")).hexdigest()
    metadata = {
        "entrypoint": "demo_obj",
        "kind": "function",
        "inputs": [],
        "outputs": [{"name": "default", "type": "int"}],
        "pipeline_nodes": [],
        "links": [],
        "internal_objects": [],
        "distributions": [],
    }
    created_at = "2026-01-01T00:00:00+00:00"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE envs (
                name TEXT PRIMARY KEY,
                python TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE objects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                kind TEXT,
                origin TEXT NOT NULL DEFAULT 'local',
                remote_owner_id TEXT,
                remote_object_id TEXT,
                remote_name TEXT,
                description TEXT NOT NULL DEFAULT '',
                current_version_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE object_versions (
                id TEXT PRIMARY KEY,
                object_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                version_label TEXT,
                description TEXT NOT NULL DEFAULT '',
                entrypoint TEXT NOT NULL,
                env TEXT NOT NULL,
                env_python TEXT NOT NULL,
                kind TEXT NOT NULL,
                yaml_text TEXT NOT NULL,
                yaml_sha256 TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                inputs_json TEXT NOT NULL,
                outputs_json TEXT NOT NULL,
                pipeline_nodes_json TEXT NOT NULL,
                distributions_json TEXT NOT NULL,
                runtime_config_json TEXT NOT NULL DEFAULT '{"mode":"venv"}',
                workdir TEXT,
                remote_owner_id TEXT,
                remote_object_id TEXT,
                remote_version_id TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(object_id) REFERENCES objects(id),
                FOREIGN KEY(env) REFERENCES envs(name),
                UNIQUE(object_id, version)
            );
            """
        )
        conn.execute(
            """
            INSERT INTO envs(name, python, created_at, updated_at)
            VALUES('default', ?, ?, ?)
            """,
            (str(python_path), created_at, created_at),
        )
        conn.execute(
            """
            INSERT INTO objects(
                id, name, kind, origin, remote_owner_id, remote_object_id,
                remote_name, description, current_version_id, created_at, updated_at
            )
            VALUES(
                'object1', 'demo_obj', 'function', 'server', 'admin1',
                'remote-object-1', 'demo_obj', 'old demo', 'version1', ?, ?
            )
            """,
            (created_at, created_at),
        )
        conn.execute(
            """
            INSERT INTO object_versions(
                id, object_id, version, version_label, description, entrypoint,
                env, env_python, kind, yaml_text, yaml_sha256, metadata_json,
                inputs_json, outputs_json, pipeline_nodes_json, distributions_json,
                runtime_config_json, workdir, remote_owner_id, remote_object_id,
                remote_version_id, created_at
            )
            VALUES(
                'version1', 'object1', 1, 'v1', 'old demo', 'demo_obj',
                'default', ?, 'function', ?, ?, ?, '[]', ?, '[]', '[]',
                '{"mode":"venv"}', NULL, 'admin1', 'remote-object-1',
                'remote-version-1', ?
            )
            """,
            (
                str(python_path),
                FUNCTION_YAML,
                yaml_sha256,
                json.dumps(metadata, sort_keys=True),
                json.dumps(metadata["outputs"], sort_keys=True),
                created_at,
            ),
        )
    return db_path, yaml_sha256


def test_fresh_schema_uses_canonical_object_and_content_identity(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.close()

    with sqlite3.connect(tmp_path / "daemon.sqlite3") as conn:
        object_columns = {row[1]: row for row in conn.execute("PRAGMA table_xinfo(objects)").fetchall()}
        version_columns = {row[1] for row in conn.execute("PRAGMA table_xinfo(object_versions)").fetchall()}

        assert {"owner_id", "library", "source_object_name"} <= set(object_columns)
        assert object_columns["remote_name"][6] != 0
        assert "content_hash" in version_columns
        assert ("owner_id", "library", "name") in _unique_columns(conn, "objects")
        assert ("name",) not in _unique_columns(conn, "objects")
        assert ("object_id", "content_hash") in _unique_columns(
            conn,
            "object_versions",
        )


def test_object_identity_migration_dry_run_leaves_old_db_unchanged(tmp_path) -> None:
    db_path, _ = _create_old_identity_db(tmp_path)
    before = db_path.read_bytes()

    storage = StorageBase(tmp_path)
    try:
        report = storage.migrate_object_identity_schema(dry_run=True)
    finally:
        storage.close()

    assert report["needed"] is True
    assert report["dry_run"] is True
    assert db_path.read_bytes() == before
    assert list(tmp_path.glob("daemon.sqlite3.before-*.bak")) == []


def test_object_identity_migration_is_idempotent_and_restorable(tmp_path) -> None:
    db_path, yaml_sha256 = _create_old_identity_db(tmp_path)
    old_bytes = db_path.read_bytes()

    store = RegistryStore(tmp_path)
    try:
        record = store.get_object("demo_obj")

        assert record["owner_id"] == DEFAULT_OBJECT_OWNER_ID
        assert record["library"] == DEFAULT_OBJECT_LIBRARY
        assert record["source_object_name"] == "demo_obj"
        assert record["remote_name"] == "demo_obj"
        assert record["content_hash"] == yaml_sha256
    finally:
        store.close()

    backups = list(tmp_path.glob("daemon.sqlite3.before-*.bak"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == old_bytes

    store = RegistryStore(tmp_path)
    store.close()
    assert list(tmp_path.glob("daemon.sqlite3.before-*.bak")) == backups

    restored_path = tmp_path / "restored.sqlite3"
    shutil.copy2(backups[0], restored_path)
    with sqlite3.connect(restored_path) as conn:
        old_columns = {row[1] for row in conn.execute("PRAGMA table_info(objects)").fetchall()}
        assert "owner_id" not in old_columns
        assert "remote_name" in old_columns


def test_object_identity_migration_dedupes_existing_content_versions(tmp_path) -> None:
    db_path, yaml_sha256 = _create_old_identity_db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        created_at = "2026-01-01T00:00:01+00:00"
        row = conn.execute(
            """
            SELECT env_python, yaml_text, yaml_sha256, metadata_json, inputs_json,
                   outputs_json, pipeline_nodes_json, distributions_json,
                   runtime_config_json
            FROM object_versions
            WHERE id = 'version1'
            """
        ).fetchone()
        conn.execute(
            """
            INSERT INTO object_versions(
                id, object_id, version, version_label, description, entrypoint,
                env, env_python, kind, yaml_text, yaml_sha256, metadata_json,
                inputs_json, outputs_json, pipeline_nodes_json, distributions_json,
                runtime_config_json, workdir, remote_owner_id, remote_object_id,
                remote_version_id, created_at
            )
            VALUES(
                'version2', 'object1', 2, 'v2', 'same content', 'demo_obj',
                'default', ?, 'function', ?, ?, ?, ?, ?, ?, ?, ?, NULL,
                'admin1', 'remote-object-1', 'remote-version-2', ?
            )
            """,
            (*row, created_at),
        )
        conn.execute("UPDATE objects SET current_version_id = 'version2' WHERE id = 'object1'")

    store = RegistryStore(tmp_path)
    try:
        versions = store.list_object_versions("demo_obj")
        current = store.get_object("demo_obj")

        assert len(versions) == 1
        assert versions[0]["version_id"] == "version1"
        assert versions[0]["content_hash"] == yaml_sha256
        assert current["version_id"] == "version1"
    finally:
        store.close()


def test_metadata_yaml_rejects_python_object_tags_before_execution(tmp_path) -> None:
    marker = tmp_path / "metadata-loader-executed"
    payload = "__import__('pathlib').Path({!r}).write_text('pwned')".format(str(marker))
    yaml_text = f"!!python/object/apply:builtins.eval\n- |\n  {payload}\n"

    with pytest.raises(yaml.constructor.ConstructorError, match="python/object/apply"):
        metadata_module.extract_metadata(yaml_text, "demo_obj")

    assert not marker.exists()


def test_object_kind_is_stable_for_local_versions(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        first = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )

        assert first["kind"] == "function"
        assert first["object_kind"] == "function"
        assert first["version_kind"] == "function"

        with pytest.raises(ValueError, match="object kind is stable"):
            store.register_object(
                "demo_obj",
                "demo_obj",
                "default",
                yaml_text=PIPELINE_YAML,
            )
    finally:
        store.close()


def test_identical_content_reuses_existing_object_version(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        first = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )
        second = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )

        assert second["version_id"] == first["version_id"]
        assert second["version"] == 1
        assert second["content_hash"] == first["content_hash"]
        assert len(second["content_hash"]) == 64
        assert len(store.list_object_versions("demo_obj")) == 1
    finally:
        store.close()


@pytest.mark.parametrize("publish_count", [1, 2, 3, 5, 8])
def test_identical_republish_property_keeps_one_version(
    tmp_path,
    publish_count: int,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        records = [
            store.register_object(
                "demo_obj",
                "demo_obj",
                "default",
                yaml_text=FUNCTION_YAML,
            )
            for _ in range(publish_count)
        ]

        assert {record["version_id"] for record in records} == {records[0]["version_id"]}
        assert len(store.list_object_versions("demo_obj")) == 1
    finally:
        store.close()


def test_adapter_only_change_bumps_object_version(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        first = store.register_object(
            "demo_pipeline",
            "demo_pipeline",
            "default",
            yaml_text=_adapter_pipeline_yaml(save_name="save_text"),
        )
        second = store.register_object(
            "demo_pipeline",
            "demo_pipeline",
            "default",
            yaml_text=_adapter_pipeline_yaml(save_name="save_text_v2"),
        )

        assert second["version_id"] != first["version_id"]
        assert second["version"] == 2
        assert second["content_hash"] != first["content_hash"]
        assert len(store.list_object_versions("demo_pipeline")) == 2
    finally:
        store.close()


def test_adapter_distribution_change_bumps_object_version(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        first = store.register_object(
            "demo_pipeline",
            "demo_pipeline",
            "default",
            yaml_text=_adapter_pipeline_yaml(adapter_distribution_version="1.0"),
        )
        second = store.register_object(
            "demo_pipeline",
            "demo_pipeline",
            "default",
            yaml_text=_adapter_pipeline_yaml(adapter_distribution_version="2.0"),
        )

        assert second["version_id"] != first["version_id"]
        assert second["version"] == 2
        assert second["content_hash"] != first["content_hash"]
        assert len(store.list_object_versions("demo_pipeline")) == 2
    finally:
        store.close()


def test_identical_definitions_from_different_call_sites_hash_equal(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        first = store.register_object(
            "demo_pipeline_a",
            "demo_pipeline",
            "default",
            yaml_text=_adapter_pipeline_yaml(reverse_dependencies=False),
        )
        second = store.register_object(
            "demo_pipeline_b",
            "demo_pipeline",
            "default",
            yaml_text=_adapter_pipeline_yaml(reverse_dependencies=True),
        )

        assert second["content_hash"] == first["content_hash"]
    finally:
        store.close()


def test_server_mirror_exposes_source_name_aliases(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        record = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="admin1",
            library="default",
            origin="server",
            remote_owner_id="admin1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )

        assert record["name"] == "demo_obj"
        assert record["owner_id"] == "admin1"
        assert record["library"] == "default"
        assert record["local_registry_name"] == "demo_obj"
        assert record["display_name"] == "demo_obj"
        assert record["remote_display_name"] == "demo_obj"
        assert record["remote_name"] == "demo_obj"
        assert record["source_owner_id"] == "admin1"
        assert record["source_object_id"] == "remote-object-1"
        assert record["source_object_name"] == "demo_obj"
        assert record["source_version_id"] == "remote-version-1"
        assert record["remote_identity"]["local_registry_name"] == "demo_obj"
        assert record["remote_identity"]["source_object_name"] == "demo_obj"
        assert record["remote_identity"]["storage_remote_name"] == "demo_obj"
        assert record["compatibility"]["remote_name"]["replacement"] == ("source_object_name")

        resolved = store.get_object(
            "demo_obj",
            owner_id="admin1",
            library="default",
        )
        assert resolved["name"] == "demo_obj"
        assert resolved["display_name"] == "demo_obj"
        # H-01: a bare miss with a visible cross-owner local mirror now explains
        # the owner/library to pass instead of returning the old empty 404.
        with pytest.raises(KeyError, match="registered locally under owner 'admin1'"):
            store.get_object("demo_obj")
    finally:
        store.close()


def test_dedup_keeps_remote_version_link_stable_on_ping_pong_sync(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        first = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="admin1",
            library="default",
            origin="server",
            remote_owner_id="admin1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )

        caplog.set_level(logging.WARNING, logger="spl.daemon.repositories.object")
        second = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="admin1",
            library="default",
            origin="server",
            remote_owner_id="admin1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-2",
            source_object_name="demo_obj",
        )

        current = store.get_object_version(first["version_id"], include_yaml=False)
        assert second["version_id"] == first["version_id"]
        assert current["content_hash"] == first["content_hash"]
        assert current["remote_version_id"] == "remote-version-1"
        assert store.get_object_by_remote_version("remote-version-2") is None

        warnings = [
            record for record in caplog.records if getattr(record, "spl_event", None) == "remote_version_id_collision"
        ]
        assert len(warnings) == 1
        warning_message = warnings[0].getMessage()
        assert "remote-version-1" in warning_message
        assert "remote-version-2" in warning_message
        assert getattr(warnings[0], "remote_version_id_collision") == {
            "event": "remote_version_id_collision",
            "object_id": first["id"],
            "content_hash": first["content_hash"],
            "existing_remote_version_id": "remote-version-1",
            "incoming_remote_version_id": "remote-version-2",
        }
    finally:
        store.close()


def test_dedup_fills_missing_remote_version_link(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        first = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="admin1",
            library="default",
            origin="server",
            remote_owner_id="admin1",
            remote_object_id="remote-object-1",
            source_object_name="demo_obj",
        )
        second = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="admin1",
            library="default",
            origin="server",
            remote_owner_id="admin1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )

        current = store.get_object_version(first["version_id"], include_yaml=False)
        assert second["version_id"] == first["version_id"]
        assert current["remote_version_id"] == "remote-version-1"
    finally:
        store.close()


def test_explicit_scope_reports_canonical_ambiguous_matches(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="admin1",
            library="research",
            origin="server",
            remote_owner_id="admin1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )
        store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="admin1",
            library="analytics",
            origin="server",
            remote_owner_id="admin1",
            remote_object_id="remote-object-2",
            remote_version_id="remote-version-2",
            source_object_name="demo_obj",
        )

        with pytest.raises(ValueError, match="ambiguous locally") as exc_info:
            store.get_object("demo_obj", owner_id="admin1")
        message = str(exc_info.value)
        assert "admin1/analytics/demo_obj" in message
        assert "admin1/research/demo_obj" in message
        assert "server." not in message
    finally:
        store.close()


@pytest.mark.parametrize(
    ("object_kind", "entrypoint", "first_yaml", "second_yaml"),
    [
        ("function", "demo_obj", FUNCTION_YAML, FUNCTION_YAML_V2),
        (
            "pipeline",
            "demo_pipeline",
            _adapter_pipeline_yaml(save_name="save_text"),
            _adapter_pipeline_yaml(save_name="save_text_v2"),
        ),
    ],
)
def test_bare_resolution_prefers_callers_object_over_same_name_server_mirror(
    tmp_path,
    object_kind: str,
    entrypoint: str,
    first_yaml: str,
    second_yaml: str,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        local = store.register_object(
            "order_pipeline",
            entrypoint,
            "default",
            yaml_text=first_yaml,
        )
        mirror = store.register_object(
            "order_pipeline",
            entrypoint,
            "default",
            yaml_text=first_yaml,
            owner_id="owner-1",
            library="default",
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id=f"remote-{object_kind}-object",
            remote_version_id=f"remote-{object_kind}-version-1",
            source_object_name="order_pipeline",
        )

        resolved = store.get_object("order_pipeline")
        assert resolved["id"] == local["id"]
        assert resolved["owner_id"] == DEFAULT_OBJECT_OWNER_ID
        assert resolved["kind"] == object_kind

        bumped = store.register_object(
            "order_pipeline",
            entrypoint,
            "default",
            yaml_text=second_yaml,
        )

        assert bumped["id"] == local["id"]
        assert bumped["version"] == 2
        assert bumped["owner_id"] == DEFAULT_OBJECT_OWNER_ID

        local_versions = store.list_object_versions("order_pipeline")
        mirror_versions = store.list_object_versions(
            "order_pipeline",
            owner_id="owner-1",
            library="default",
        )
        assert [item["id"] for item in local_versions] == [local["id"], local["id"]]
        assert {item["version"] for item in local_versions} == {1, 2}
        assert len(mirror_versions) == 1
        assert mirror_versions[0]["id"] == mirror["id"]
    finally:
        store.close()


def test_connected_publish_adopts_existing_server_object_identity(tmp_path) -> None:
    class ExistingObjectServerClient:
        requests: list[tuple[str, str | None, str | None]] = []

        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_object(
            self,
            name_or_id,
            *,
            version=None,
            include_yaml=False,
            owner_id=None,
            library=None,
        ):
            assert version is None
            assert include_yaml is False
            self.requests.append((name_or_id, owner_id, library))
            return {
                "id": "remote-object-1",
                "owner_id": owner_id,
                "name": name_or_id,
                "version": 7,
                "version_id": "remote-version-7",
                "entrypoint": "demo_obj",
                "env": "default",
            }

    store = RegistryStore(tmp_path)
    runtime = None
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            docker_pool=_NoopDockerPool(),
            server_client_factory=ExistingObjectServerClient,
        )
        connection = store.save_server_connection(
            server_url="https://splime.io/api",
            token=_test_credential("machine"),
            user_token=_test_credential("user"),
            connection={
                "id": "remote-connection-1",
                "owner_id": "owner-1",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": "machine-1",
                "display_name": "lab-machine",
                "status": "connected",
                "capabilities": {},
            },
            heartbeat_interval_seconds=60,
        )
        runtime._mark_server_channel_success(store.get_server_connection_credentials(connection["id"]))

        first = runtime.register_object(
            "order_pipeline",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )
        second = runtime.register_object(
            "order_pipeline",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
        )

        assert ExistingObjectServerClient.requests == [("order_pipeline", "owner-1", "default")]
        assert first["owner_id"] == "owner-1"
        assert first["object_remote_object_id"] == "remote-object-1"
        assert first["source_object_id"] == "remote-object-1"
        assert second["id"] == first["id"]
        assert second["version"] == 2
        assert store.get_object("order_pipeline")["id"] == first["id"]
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_connect_reconcile_links_existing_server_object_without_fork(tmp_path) -> None:
    class ExistingObjectServerClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def connect_machine(self, **kwargs):
            return {
                "id": "remote-connection-1",
                "owner_id": "owner-1",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": kwargs.get("machine_id") or "machine-1",
                "display_name": "lab-machine",
                "status": "connected",
                "capabilities": {},
            }

        def get_object(
            self,
            name_or_id,
            *,
            version=None,
            include_yaml=False,
            owner_id=None,
            library=None,
        ):
            assert name_or_id == "order_pipeline"
            assert owner_id == "owner-1"
            assert library == "default"
            return {
                "id": "remote-object-1",
                "owner_id": "owner-1",
                "name": "order_pipeline",
                "version": 1,
                "version_id": "remote-version-1",
                "entrypoint": "demo_obj",
                "env": "default",
                "yaml": FUNCTION_YAML if include_yaml else None,
            }

        def list_object_versions(
            self,
            name_or_id,
            *,
            include_yaml=False,
            owner_id=None,
            library=None,
        ):
            # Stage 1.4: reconcile lists version metadata WITHOUT bodies;
            # YAML is fetched lazily per missing version.
            assert include_yaml is False
            return [
                {
                    "id": "remote-object-1",
                    "owner_id": "owner-1",
                    "name": "order_pipeline",
                    "version": 1,
                    "version_id": "remote-version-1",
                    "entrypoint": "demo_obj",
                    "env": "default",
                    "description": "server copy",
                    "yaml": FUNCTION_YAML,
                }
            ]

    store = RegistryStore(tmp_path)
    runtime = None
    try:
        store.register_env("default", sys.executable)
        local = store.register_object(
            "order_pipeline",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            docker_pool=_NoopDockerPool(),
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=ExistingObjectServerClient,
        )

        result = runtime.connect_server(
            server_url="https://splime.io/api",
            machine_token=_test_credential("machine"),
            user_token=_test_credential("user"),
            machine_id="machine-1",
            display_name=None,
            capabilities={},
            heartbeat_interval_seconds=60,
        )

        assert result["connected"] is True
        identities = store.list_object_identities(owner_id="owner-1")
        assert len(identities) == 1
        assert identities[0]["id"] == local["id"]
        assert identities[0]["remote_object_id"] == "remote-object-1"
        versions = store.list_object_versions(
            "order_pipeline",
            owner_id="owner-1",
            library="default",
        )
        assert len(versions) == 1
        assert versions[0]["remote_version_id"] == "remote-version-1"
        assert store.list_pending_sync_events() == []
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_connect_reconcile_records_conflict_for_divergent_same_version(tmp_path) -> None:
    class DivergentObjectServerClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def connect_machine(self, **kwargs):
            return {
                "id": "remote-connection-1",
                "owner_id": "owner-1",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": kwargs.get("machine_id") or "machine-1",
                "display_name": "lab-machine",
                "status": "connected",
                "capabilities": {},
            }

        def get_object(self, name_or_id, **kwargs):
            return {
                "id": "remote-object-1",
                "owner_id": "owner-1",
                "name": "order_pipeline",
                "version": 1,
                "version_id": "remote-version-1",
                "entrypoint": "demo_obj",
                "env": "default",
            }

        def list_object_versions(self, name_or_id, **kwargs):
            return [
                {
                    "id": "remote-object-1",
                    "owner_id": "owner-1",
                    "name": "order_pipeline",
                    "version": 1,
                    "version_id": "remote-version-1",
                    "entrypoint": "demo_obj",
                    "env": "default",
                    "content_hash": "remotecontenthash",
                    "yaml": FUNCTION_YAML,
                }
            ]

    store = RegistryStore(tmp_path)
    runtime = None
    try:
        store.register_env("default", sys.executable)
        local = store.register_object(
            "order_pipeline",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
        )
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            docker_pool=_NoopDockerPool(),
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=DivergentObjectServerClient,
        )

        result = runtime.connect_server(
            server_url="https://splime.io/api",
            machine_token=_test_credential("machine"),
            user_token=_test_credential("user"),
            machine_id="machine-1",
            display_name=None,
            capabilities={},
            heartbeat_interval_seconds=60,
        )

        assert result["reconcile"]["conflicts"]
        identities = store.list_object_identities(owner_id="owner-1")
        assert len(identities) == 1
        assert identities[0]["id"] == local["id"]
        versions = store.list_object_versions(
            "order_pipeline",
            owner_id="owner-1",
            library="default",
        )
        assert len(versions) == 1
        assert versions[0]["remote_version_id"] is None
        conflicts = [event for event in store.list_pending_sync_events() if event["kind"] == "object_conflict"]
        assert len(conflicts) == 1
        assert conflicts[0]["payload"]["canonical_name"] == ("owner-1/default/order_pipeline")
        objects = store.list_objects()
        assert objects["order_pipeline"]["conflicts"]
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_healing_migration_merges_corrupt_synthetic_server_row(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        local = store.register_object(
            "order_pipeline",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )
        mirror = store.register_object(
            "server.remote-object-1",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
            owner_id=DEFAULT_OBJECT_OWNER_ID,
            library=DEFAULT_OBJECT_LIBRARY,
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-2",
            source_object_name="order_pipeline",
        )
    finally:
        store.close()

    db_path = tmp_path / "daemon.sqlite3"
    before = db_path.read_bytes()
    storage = StorageBase(tmp_path)
    try:
        dry_run = storage.migrate_object_identity_schema(dry_run=True)
        assert dry_run["needed"] is True
        assert dry_run["dry_run"] is True
        assert dry_run["healing"]["merges"]
        assert db_path.read_bytes() == before

        report = storage.migrate_object_identity_schema(dry_run=False)
        assert report["healing"]["merges"]
    finally:
        storage.close()

    backups = list(tmp_path.glob("daemon.sqlite3.before-*.bak"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == before

    store = RegistryStore(tmp_path)
    try:
        identities = store.list_object_identities()
        assert len(identities) == 1
        assert identities[0]["id"] == local["id"]
        assert identities[0]["name"] == "order_pipeline"
        assert identities[0]["remote_object_id"] == "remote-object-1"
        versions = store.list_object_versions("order_pipeline")
        assert len(versions) == 2
        assert {item["version_id"] for item in versions} == {
            local["version_id"],
            mirror["version_id"],
        }
        assert store.get_object("order_pipeline")["version"] == 2
    finally:
        store.close()

    storage = StorageBase(tmp_path)
    try:
        rerun = storage.migrate_object_identity_schema(dry_run=False)
        assert rerun["needed"] is False
        assert list(tmp_path.glob("daemon.sqlite3.before-*.bak")) == backups
    finally:
        storage.close()

    restored_path = tmp_path / "restored.sqlite3"
    shutil.copy2(backups[0], restored_path)
    with sqlite3.connect(restored_path) as conn:
        rows = conn.execute("SELECT name FROM objects ORDER BY name").fetchall()
        assert [row[0] for row in rows] == [
            "order_pipeline",
            "server.remote-object-1",
        ]


@pytest.mark.parametrize(
    ("name", "entrypoint", "first_yaml", "second_yaml", "expected_kind"),
    [
        ("cleanup_function", "demo_obj", FUNCTION_YAML, FUNCTION_YAML_V2, "function"),
        (
            "cleanup_pipeline",
            "demo_pipeline",
            _adapter_pipeline_yaml(save_name="save_text"),
            _adapter_pipeline_yaml(save_name="save_text_v2"),
            "pipeline",
        ),
    ],
)
def test_forget_object_works_offline_for_functions_and_pipelines(
    tmp_path,
    name: str,
    entrypoint: str,
    first_yaml: str,
    second_yaml: str,
    expected_kind: str,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        first = store.register_object(
            name,
            entrypoint,
            "default",
            yaml_text=first_yaml,
        )
        second = store.register_object(
            name,
            entrypoint,
            "default",
            yaml_text=second_yaml,
        )
        run = store.create_run(name)

        result = store.forget_object(name)

        assert result["forgotten"] is True
        assert result["object_deleted"] is True
        assert result["object"]["id"] == first["id"] == second["id"]
        assert result["object"]["kind"] == expected_kind
        assert {item["id"] for item in result["versions"]} == {
            first["version_id"],
            second["version_id"],
        }
        assert result["deleted"]["versions"] == 2
        assert result["deleted"]["runs"] == 1
        with pytest.raises(KeyError, match="object is not registered"):
            store.get_object(name)
        with pytest.raises(KeyError, match="run is not found"):
            store.get_run(run["id"])

        with sqlite3.connect(tmp_path / "daemon.sqlite3") as conn:
            assert _table_count(conn, "objects") == 0
            assert _table_count(conn, "object_versions") == 0
            assert _table_count(conn, "runs") == 0
            assert _table_count(conn, "object_functions") == 0
            assert _table_count(conn, "object_pipeline_nodes") == 0
            assert _table_count(conn, "object_pipeline_links") == 0
        _assert_identity_invariants(tmp_path)
    finally:
        store.close()


@pytest.mark.parametrize(
    ("name", "entrypoint", "first_yaml", "second_yaml"),
    [
        ("versioned_function", "demo_obj", FUNCTION_YAML, FUNCTION_YAML_V2),
        (
            "versioned_pipeline",
            "demo_pipeline",
            _adapter_pipeline_yaml(save_name="save_text"),
            _adapter_pipeline_yaml(save_name="save_text_v2"),
        ),
    ],
)
def test_forget_single_version_updates_current_or_deletes_empty_object(
    tmp_path,
    name: str,
    entrypoint: str,
    first_yaml: str,
    second_yaml: str,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        first = store.register_object(
            name,
            entrypoint,
            "default",
            yaml_text=first_yaml,
        )
        second = store.register_object(
            name,
            entrypoint,
            "default",
            yaml_text=second_yaml,
        )
        store.create_run(name, version=2)

        result = store.forget_object_version(name, 2)

        assert result["forgotten"] is True
        assert result["object_deleted"] is False
        assert result["version"]["id"] == second["version_id"]
        assert result["current_version_id"] == first["version_id"]
        assert result["deleted"]["runs"] == 1
        assert store.get_object(name)["version_id"] == first["version_id"]
        assert [item["version"] for item in store.list_object_versions(name)] == [1]

        result = store.forget_object_version(name, first["version_id"])

        assert result["object_deleted"] is True
        with pytest.raises(KeyError, match="object is not registered"):
            store.get_object(name)
        _assert_identity_invariants(tmp_path)
    finally:
        store.close()


def test_forget_object_failure_rolls_back_database_bytes(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "rollback_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )
        store.register_object(
            "rollback_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML_V2,
        )
        store.create_run("rollback_obj")

        db_path = tmp_path / "daemon.sqlite3"
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TRIGGER fail_object_version_delete
                BEFORE DELETE ON object_versions
                BEGIN
                    SELECT RAISE(ABORT, 'forced rollback');
                END
                """
            )
        before = db_path.read_bytes()

        with pytest.raises(sqlite3.IntegrityError, match="forced rollback"):
            store.forget_object("rollback_obj")

        assert db_path.read_bytes() == before
        assert len(store.list_object_versions("rollback_obj")) == 2
        assert len(store.list_runs()) == 1
    finally:
        store.close()


def test_prune_stale_mirrors_removes_server_origin_rows_only(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        local = store.register_object(
            "local_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )
        mirror = store.register_object(
            "mirrored_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            owner_id="owner-1",
            library="default",
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="mirrored_obj",
        )

        result = store.prune_stale_mirrors(owner_id="owner-1", library="default")

        assert result["count"] == 1
        assert result["pruned"][0]["object"]["id"] == mirror["id"]
        assert store.get_object("local_obj")["id"] == local["id"]
        with pytest.raises(KeyError, match="object is not registered"):
            store.get_object(
                "mirrored_obj",
                owner_id="owner-1",
                library="default",
            )
        _assert_identity_invariants(tmp_path)
    finally:
        store.close()


def test_random_publish_republish_connect_forget_preserves_identity_invariants(
    tmp_path,
) -> None:
    class LocalOnlyServerClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def connect_machine(self, **kwargs):
            return {
                "id": "remote-connection-1",
                "owner_id": "owner-1",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": kwargs.get("machine_id") or "machine-1",
                "display_name": "lab-machine",
                "status": "connected",
                "capabilities": {},
            }

        def get_object(self, name_or_id, **kwargs):
            raise ServerClientError(404, "object is not found")

    rng = random.Random(8675309)
    store = RegistryStore(tmp_path)
    runtime = None
    definitions = {
        "property_function": [
            ("demo_obj", FUNCTION_YAML),
            ("demo_obj", FUNCTION_YAML_V2),
        ],
        "property_pipeline": [
            ("demo_pipeline", _adapter_pipeline_yaml(save_name="save_text")),
            ("demo_pipeline", _adapter_pipeline_yaml(save_name="save_text_v2")),
        ],
    }
    try:
        store.register_env("default", sys.executable)

        for name, variants in definitions.items():
            entrypoint, yaml_text = variants[0]
            first = store.register_object(
                name,
                entrypoint,
                "default",
                yaml_text=yaml_text,
            )
            second = store.register_object(
                name,
                entrypoint,
                "default",
                yaml_text=yaml_text,
            )
            assert second["version_id"] == first["version_id"]
            _assert_identity_invariants(tmp_path)

        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            docker_pool=_NoopDockerPool(),
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=LocalOnlyServerClient,
        )
        operations = [
            "publish_function_same",
            "publish_function_changed",
            "publish_pipeline_same",
            "publish_pipeline_changed",
            "connect",
            "forget",
            "forget_version",
        ]
        for _ in range(36):
            operation = rng.choice(operations)
            if operation == "connect":
                if store.current_server_connection() is None:
                    runtime.connect_server(
                        server_url="https://splime.io/api",
                        machine_token=_test_credential("machine"),
                        user_token=_test_credential("user"),
                        machine_id="machine-1",
                        display_name=None,
                        capabilities={},
                        heartbeat_interval_seconds=60,
                    )
            elif operation.startswith("publish_function"):
                variant = 1 if operation.endswith("changed") else 0
                entrypoint, yaml_text = definitions["property_function"][variant]
                runtime.register_object(
                    "property_function",
                    entrypoint,
                    "default",
                    yaml_text=yaml_text,
                )
            elif operation.startswith("publish_pipeline"):
                variant = 1 if operation.endswith("changed") else 0
                entrypoint, yaml_text = definitions["property_pipeline"][variant]
                runtime.register_object(
                    "property_pipeline",
                    entrypoint,
                    "default",
                    yaml_text=yaml_text,
                )
            elif operation == "forget":
                name = rng.choice(list(definitions))
                try:
                    store.forget_object(name)
                except KeyError:
                    pass
            else:
                name = rng.choice(list(definitions))
                try:
                    versions = store.list_object_versions(name)
                except KeyError:
                    versions = []
                if versions:
                    selected = rng.choice(versions)
                    store.forget_object_version(name, selected["version_id"])

            _assert_identity_invariants(tmp_path)

        for name, variants in definitions.items():
            entrypoint, yaml_text = variants[0]
            first = runtime.register_object(
                name,
                entrypoint,
                "default",
                yaml_text=yaml_text,
            )
            second = runtime.register_object(
                name,
                entrypoint,
                "default",
                yaml_text=yaml_text,
            )
            assert second["version_id"] == first["version_id"]
            _assert_identity_invariants(tmp_path)
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_object_decomposition_persists_functions_nodes_and_links(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        function_record = store.register_object(
            "demo_function",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )
        function_decomposition = store.get_object_decomposition(function_record["version_id"])

        assert [item["name"] for item in function_decomposition["functions"]] == ["demo_obj"]
        assert function_decomposition["functions"][0]["role"] == "top_level"
        assert function_decomposition["nodes"] == []
        assert function_decomposition["links"] == []

        bundle_path = Path(__file__).resolve().parent / "demo" / "_bundle.yaml"
        pipeline_record = store.register_object(
            "test_pipeline",
            "test_pipeline",
            "default",
            yaml_text=bundle_path.read_text(encoding="utf-8"),
        )
        decomposition = store.get_object_decomposition(pipeline_record["version_id"])

        assert pipeline_record["kind"] == "pipeline"
        assert len(decomposition["functions"]) >= 3
        assert len(decomposition["nodes"]) == len(decomposition["functions"])
        assert len(decomposition["links"]) >= 1
        assert {node["kind"] for node in decomposition["nodes"]} == {"function"}
        assert pipeline_record["decomposition"]["links"] == decomposition["links"]
    finally:
        store.close()


def test_sync_visibility_exposes_retry_state(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        event = store.enqueue_sync_event(
            "object_version",
            {"name": "demo_obj", "version": 1},
        )
        failed = store.mark_sync_event_failed(event["id"], "server rejected event")

        assert failed["retry"]["will_retry"] is True
        assert failed["retry"]["next_attempt"] == 2
        assert failed["retry"]["last_error"] == "server rejected event"

        service = SyncVisibilityService(store)
        pending_events = service.pending_events()
        summary = service.summary(pending_events)

        assert len(pending_events) == 1
        assert pending_events[0]["retry"]["will_retry"] is True
        assert summary["pending"] == 1
        assert summary["retryable"] == 1
        assert summary["by_status"] == {"failed": 1}
        assert summary["by_kind"] == {"object_version": 1}
        assert summary["last_error"] == "server rejected event"
        assert summary["next_action"] == "will_retry_on_next_sync"
    finally:
        store.close()


def test_pipeline_decomposition_validation_rejects_bad_links(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        def invalid_metadata(*args, **kwargs):
            return {
                "entrypoint": "bad_pipeline",
                "kind": "pipeline",
                "inputs": [],
                "outputs": [],
                "pipeline_nodes": [
                    {
                        "id": "node_a",
                        "kind": "function",
                        "function": "demo_obj",
                        "inputs": [],
                        "outputs": [],
                    }
                ],
                "internal_objects": [],
                "links": [
                    {
                        "from": {"node_id": "missing_node", "port": "x"},
                        "to": {"kind": "scalar", "value": 1},
                    }
                ],
                "distributions": [],
            }

        monkeypatch.setattr(metadata_module, "extract_metadata", invalid_metadata)

        with pytest.raises(ValueError, match="pipeline link target node"):
            store.register_object(
                "bad_pipeline",
                "bad_pipeline",
                "default",
                yaml_text=PIPELINE_YAML,
            )
    finally:
        store.close()
