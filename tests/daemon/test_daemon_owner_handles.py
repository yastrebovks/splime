from __future__ import annotations

import asyncio
from typing import Any

import pytest

from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore
from spl.daemon_client import Client


FUNCTION_YAML = """\
- !DFunction
  name: score
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 1
"""


def _save_connection(store: RegistryStore, *, owner_id: str = "user-self") -> None:
    store.save_server_connection(
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


def _mark_live(runtime: Any) -> None:
    runtime.heartbeat_service.shutdown()
    credentials = runtime.store.current_server_connection_credentials()
    assert credentials is not None
    runtime.store.record_server_connection_heartbeat(
        credentials["id"],
        remote_connection={
            "id": credentials["remote_connection_id"],
            "owner_id": credentials["owner_id"],
            "subject_type": credentials["subject_type"],
            "subject_id": credentials["subject_id"],
            "machine_id": credentials["machine_id"],
            "display_name": credentials["display_name"],
            "capabilities": credentials.get("capabilities") or {},
            "status": "connected",
            "heartbeat_interval_seconds": credentials["heartbeat_interval_seconds"],
        },
    )
    runtime._mark_server_channel_success(runtime.store.get_server_connection_credentials(credentials["id"]))


def _request(app: Any, method: str, path: str) -> tuple[int, Any]:
    async def request() -> tuple[int, Any]:
        client = app.test_client()
        response = await getattr(client, method)(
            path,
            headers={"Authorization": f"Bearer {app.api_token}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(request())


def _assert_no_handle_in_owner_columns(store: RegistryStore) -> None:
    tables = store._conn.execute(  # noqa: SLF001 - release guard scans persisted identity fields.
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    violations = []
    for table_row in tables:
        table = str(table_row["name"])
        columns = store._conn.execute(f'PRAGMA table_info("{table}")').fetchall()  # noqa: S608
        for column_row in columns:
            column = str(column_row["name"])
            if "owner" not in column:
                continue
            rows = store._conn.execute(  # noqa: SLF001, S608 - identifiers come from SQLite metadata.
                f'SELECT "{column}" AS value FROM "{table}" WHERE CAST("{column}" AS TEXT) LIKE \'@%\''
            ).fetchall()
            violations.extend((table, column, row["value"]) for row in rows)
    assert violations == []


class _OwnerServer:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def list_users(self, *, handle: str | None = None) -> list[dict[str, Any]]:
        self.calls.append(("list_users", handle))
        return [
            {
                "id": "user-a" if handle else "user-self",
                "handle": "alice" if handle else "self",
                "display_name": "Alice" if handle else "Self User",
                "status": "active",
            }
        ]

    def list_owner_libraries(self, owner_id: str) -> list[dict[str, Any]]:
        self.calls.append(("list_owner_libraries", owner_id))
        return [{"owner_id": "user-a", "slug": "default"}]

    def get_library(self, library_ref: str, *, owner: str | None = None) -> dict[str, Any]:
        self.calls.append(("get_library", library_ref, owner))
        return {"owner_id": "user-a", "slug": library_ref}

    def list_library_grants(
        self,
        library_ref: str,
        *,
        owner: str | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(("list_library_grants", library_ref, owner))
        return [{"owner_id": "user-a", "library": library_ref}]

    def get_object(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(("get_object", name_or_id, version, include_yaml, owner_id, library))
        return {
            "id": "object-a",
            "owner_id": "user-a",
            "library": "default",
            "name": "score",
            "version": 1,
            "version_id": "version-a-1",
            "entrypoint": "score",
            "env": "default",
            "yaml": FUNCTION_YAML,
        }

    def object_signature(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(("object_signature", name_or_id, version, owner_id, library, function))
        return {
            "id": "object-a",
            "owner_id": "user-a",
            "library": {"slug": "default"},
            "name": "score",
            "version_id": "version-a-1",
            "inputs": [],
            "outputs": [],
        }


def test_daemon_client_owner_methods_keep_legacy_paths_when_omitted(monkeypatch) -> None:
    client = Client("http://127.0.0.1:8765", api_token="local-token")
    calls = []

    def fake_json_request(method, path, payload=None, **kwargs):
        calls.append((method, path, payload))
        return [] if path.startswith("/server/users") or "libraries" in path else {}

    monkeypatch.setattr(client, "_json_request", fake_json_request)

    client.server_users()
    client.server_users(handle="@alice")
    client.server_whoami()
    client.server_libraries(include_accessible=False)
    client.server_libraries(owner="@alice", include_accessible=False)
    client.get_server_library("default")
    client.get_server_library("default", owner="@alice")
    client.server_library_grants("default")
    client.server_library_grants("default", owner="@alice")

    assert calls == [
        ("GET", "/server/users", None),
        ("GET", "/server/users?handle=%40alice", None),
        ("GET", "/server/whoami", None),
        ("GET", "/server/libraries?include_accessible=0", None),
        (
            "GET",
            "/server/libraries?owner=%40alice&include_accessible=0",
            None,
        ),
        ("GET", "/server/libraries/default", None),
        ("GET", "/server/libraries/default?owner=%40alice", None),
        ("GET", "/server/libraries/default/grants", None),
        ("GET", "/server/libraries/default/grants?owner=%40alice", None),
    ]


def test_whoami_uses_live_directory_then_offline_cached_identity(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    server = _OwnerServer()
    _save_connection(store)
    app = create_app(store, server_client_factory=lambda *args, **kwargs: server)
    try:
        _mark_live(app.runtime)
        status, body = _request(app, "get", "/server/whoami")
        assert status == 200
        assert body == {
            "id": "user-self",
            "owner_id": "user-self",
            "handle": "self",
            "display_name": "Self User",
            "server_url": "https://splime.io/api",
            "machine_id": "machine-1",
            "connection_status": "connected",
            "live": True,
        }

        credentials = store.current_server_connection_credentials()
        assert credentials is not None
        app.runtime._mark_server_channel_failure(credentials)
        status, body = _request(app, "get", "/server/whoami")
        assert status == 200
        assert body == {
            "id": "user-self",
            "owner_id": "user-self",
            "handle": None,
            "display_name": "user-self",
            "server_url": "https://splime.io/api",
            "machine_id": "machine-1",
            "connection_status": "connected",
            "live": False,
        }
        assert server.calls == [("list_users", None)]
    finally:
        app.runtime.shutdown()
        store.close()


def test_whoami_without_identity_connection_has_connect_remediation(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = create_app(store)
    try:
        status, body = _request(app, "get", "/server/whoami")
        assert status == 404
        assert "client.connect_server(...)" in body["error"]
    finally:
        app.runtime.shutdown()
        store.close()


def test_owner_library_passthrough_keeps_handle_unresolved(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    server = _OwnerServer()
    _save_connection(store)
    app = create_app(store, server_client_factory=lambda *args, **kwargs: server)
    try:
        _mark_live(app.runtime)
        assert _request(app, "get", "/server/users?handle=%40alice")[0] == 200
        assert _request(app, "get", "/server/libraries?owner=%40alice")[0] == 200
        assert _request(app, "get", "/server/libraries/default?owner=%40alice")[0] == 200
        assert (
            _request(
                app,
                "get",
                "/server/libraries/default/grants?owner=%40alice",
            )[0]
            == 200
        )
        assert server.calls == [
            ("list_users", "@alice"),
            ("list_owner_libraries", "@alice"),
            ("get_library", "default", "@alice"),
            ("list_library_grants", "default", "@alice"),
        ]
    finally:
        app.runtime.shutdown()
        store.close()


def test_local_forget_handle_fails_offline_then_resolves_live(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    server = _OwnerServer()
    store.register_env("default")
    store.register_object(
        "score",
        "score",
        "default",
        yaml_text=FUNCTION_YAML,
        owner_id="user-a",
        library="default",
    )
    _save_connection(store)
    app = create_app(store, server_client_factory=lambda *args, **kwargs: server)
    try:
        status, body = _request(
            app,
            "delete",
            "/objects/score?owner=%40alice&library=default",
        )
        assert status == 404
        assert body == {
            "error": (
                "cannot resolve owner '@alice': handles resolve on the server; "
                "connect first with client.connect_server(...) or pass the canonical owner id"
            ),
            "code": "handle_requires_server_connection",
            "owner": "@alice",
        }
        assert store.get_object("score", owner_id="user-a", library="default")["name"] == "score"

        _mark_live(app.runtime)
        status, body = _request(
            app,
            "delete",
            "/objects/score?owner=%40alice&library=default",
        )
        assert status == 200
        assert body["deleted"]["objects"] == 1
        assert server.calls == [("list_users", "@alice")]
        with pytest.raises(KeyError):
            store.get_object("score", owner_id="user-a", library="default")
        _assert_no_handle_in_owner_columns(store)
    finally:
        app.runtime.shutdown()
        store.close()


def test_pull_and_signature_forward_handle_but_persist_canonical_owner(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    server = _OwnerServer()
    _save_connection(store)
    app = create_app(store, server_client_factory=lambda *args, **kwargs: server)
    try:
        _mark_live(app.runtime)
        receipt = app.runtime.pull_server_object(
            "score",
            owner_id="@alice",
            library="default",
        )
        assert receipt["failed"] == []
        assert store.get_object("score", owner_id="user-a", library="default")["owner_id"] == "user-a"

        signature = app.runtime.resolve_remote_signature(
            {
                "server_url": "https://splime.io/api",
                "owner_id": "@alice",
                "library": "default",
                "object_name": "score",
            },
            force=True,
        )
        assert signature["remote"]["owner_id"] == "user-a"
        cache = app.runtime.remote_signature_cache_record(
            {
                "server_url": "https://splime.io/api",
                "owner_id": "user-a",
                "library": "default",
                "object_name": "score",
            }
        )
        assert cache is not None
        assert cache["owner_id"] == "user-a"
        assert (
            "get_object",
            "score",
            None,
            False,
            "@alice",
            "default",
        ) in server.calls
        assert (
            "object_signature",
            "score",
            None,
            "@alice",
            "default",
            None,
        ) in server.calls
        _assert_no_handle_in_owner_columns(store)
    finally:
        app.runtime.shutdown()
        store.close()


def test_cross_owner_mirror_hint_lists_canonical_candidates(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default")
        for owner_id in ("user-a", "user-b"):
            store.register_object(
                "score",
                "score",
                "default",
                yaml_text=FUNCTION_YAML,
                owner_id=owner_id,
                library="default",
                origin="server",
                remote_owner_id=owner_id,
                remote_object_id=f"object-{owner_id}",
                remote_version_id=f"version-{owner_id}",
            )
        with pytest.raises(KeyError) as exc_info:
            store.get_object("score")
        message = str(exc_info.value)
        assert "user-a/default/score" in message
        assert "user-b/default/score" in message
    finally:
        store.close()
