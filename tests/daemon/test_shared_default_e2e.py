from __future__ import annotations

import asyncio
import secrets
import sys
from pathlib import Path
from typing import Any

import pytest

from spl._client import SPLClient
from spl.daemon.server import create_app
from spl.daemon.storage_base import DEFAULT_OBJECT_LIBRARY
from spl.daemon.store import RegistryStore
from spl.daemon_client import ClientError

from tests.daemon.test_daemon_endpoint import (
    _reserved_port_with_free_next,
    _serve_app_in_thread,
)


A_ID = "a-example.com"
B_ID = "b-example.com"
C_ID = "c-example.com"
A_HANDLE = "aa"
B_HANDLE = "bb"
C_HANDLE = "cc"


def _function_yaml(name: str, value: int) -> str:
    return (
        "- !DFunction\n"
        f"  name: {name}\n"
        "  inputs: []\n"
        "  outputs:\n"
        "  - name: default\n"
        "    type: int\n"
        "  body: |-\n"
        f"    return {value}\n"
    )


class _SharedDefaultServer:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.remote_runs: dict[str, dict[str, Any]] = {}
        self.users = [
            {"id": A_ID, "handle": A_HANDLE, "display_name": "User A", "status": "active"},
            {"id": B_ID, "handle": B_HANDLE, "display_name": "User B", "status": "active"},
        ]
        self.objects = [
            self._record(A_ID, "shared_fn", 11),
            self._record(A_ID, "common_fn", 12),
            self._record(A_ID, "a_only_fn", 13),
            self._record(B_ID, "common_fn", 21),
        ]

    @property
    def resolution(self) -> dict[str, Any]:
        return {
            "auto_resolved": True,
            "requested_library": DEFAULT_OBJECT_LIBRARY,
            "resolved_owner_id": A_ID,
            "resolved_owner_handle": A_HANDLE,
            "resolved_library": DEFAULT_OBJECT_LIBRARY,
            "resolved_library_id": f"library-{A_ID}",
        }

    @staticmethod
    def _connection() -> dict[str, Any]:
        return {
            "id": "shared-default-connection",
            "owner_id": B_ID,
            "subject_type": "machine",
            "subject_id": "b-example-com-machine",
            "machine_id": "b-example-com-machine",
            "display_name": "B acceptance machine",
            "status": "connected",
            "capabilities": {},
            "heartbeat_interval_seconds": 60,
        }

    @staticmethod
    def _record(owner_id: str, name: str, value: int) -> dict[str, Any]:
        return {
            "id": f"object-{owner_id}-{name}",
            "owner_id": owner_id,
            "owner_handle": A_HANDLE if owner_id == A_ID else B_HANDLE,
            "library": DEFAULT_OBJECT_LIBRARY,
            "name": name,
            "version": 1,
            "version_id": f"version-{owner_id}-{name}-1",
            "entrypoint": name,
            "env": DEFAULT_OBJECT_LIBRARY,
            "yaml": _function_yaml(name, value),
            "kind": "function",
        }

    @staticmethod
    def _canonical_owner(owner_ref: str | None) -> str | None:
        return {
            f"@{A_HANDLE}": A_ID,
            f"@{B_HANDLE}": B_ID,
            A_ID: A_ID,
            B_ID: B_ID,
        }.get(owner_ref, owner_ref)

    def list_users(self, *, handle: str | None = None) -> list[dict[str, Any]]:
        self.calls.append(("list_users", handle))
        if handle is None:
            return [dict(row) for row in self.users]
        normalized = handle.removeprefix("@").casefold()
        return [dict(row) for row in self.users if row["handle"] == normalized]

    def list_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        self.calls.append(("list_objects", owner_id, library, compact))
        canonical = self._canonical_owner(owner_id)
        records = [
            row
            for row in self.objects
            if (canonical is None or row["owner_id"] == canonical) and (library is None or row["library"] == library)
        ]
        if compact:
            return [
                {
                    "id": row["id"],
                    "owner_id": row["owner_id"],
                    "owner_handle": row["owner_handle"],
                    "library": row["library"],
                    "name": row["name"],
                    "kind": row["kind"],
                    "version": row["version"],
                }
                for row in records
            ]
        return [dict(row) for row in records]

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
        canonical = self._canonical_owner(owner_id)
        candidates = [
            row
            for row in self.objects
            if row["name"] == name_or_id
            and (canonical is None or row["owner_id"] == canonical)
            and (library is None or row["library"] == library)
        ]
        if len(candidates) != 1:
            raise KeyError(f"server object is not uniquely addressable: {name_or_id}")
        record = dict(candidates[0])
        if not include_yaml:
            record.pop("yaml", None)
        if owner_id is None and library is not None and record["owner_id"] != B_ID:
            record["resolved_from"] = dict(self.resolution)
        return record

    def list_object_versions(
        self,
        name_or_id: str,
        *,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            (
                "list_object_versions",
                name_or_id,
                include_yaml,
                owner_id,
                library,
            )
        )
        record = self.get_object(
            name_or_id,
            include_yaml=include_yaml,
            owner_id=owner_id,
            library=library,
        )
        record.pop("resolved_from", None)
        return [record]

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
        record = self.get_object(
            name_or_id,
            version=version,
            include_yaml=False,
            owner_id=owner_id,
            library=library,
        )
        return {
            **record,
            "library": {"slug": record["library"]},
            "inputs": [],
            "outputs": [],
        }

    def list_owner_libraries(self, owner_id: str) -> list[dict[str, Any]]:
        self.calls.append(("list_owner_libraries", owner_id))
        canonical = self._canonical_owner(owner_id)
        return [
            {
                "id": f"library-{canonical}",
                "owner_id": canonical,
                "owner_handle": A_HANDLE if canonical == A_ID else B_HANDLE,
                "slug": DEFAULT_OBJECT_LIBRARY,
                "owned": canonical == B_ID,
            }
        ]

    def get_library(
        self,
        library_ref: str,
        *,
        owner: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(("get_library", library_ref, owner))
        canonical = self._canonical_owner(owner) or B_ID
        return {
            "id": f"library-{canonical}",
            "owner_id": canonical,
            "owner_handle": A_HANDLE if canonical == A_ID else B_HANDLE,
            "slug": library_ref,
            "owned": canonical == B_ID,
        }

    def heartbeat_connection(
        self,
        *,
        connection_id: str,
        machine_id: str,
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "heartbeat_connection",
                connection_id,
                machine_id,
                heartbeat_interval_seconds,
            )
        )
        return self._connection()

    def latest_machine_library_snapshot(
        self,
        machine_id: str,
        *,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        self.calls.append(("latest_machine_library_snapshot", machine_id, include_yaml))
        return {"snapshot_hash": None}

    def sync(
        self,
        *,
        connection_id: str,
        machine_id: str,
        heartbeat_interval_seconds: float,
        events: list[dict[str, Any]],
        capabilities: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            (
                "sync",
                connection_id,
                machine_id,
                heartbeat_interval_seconds,
                [event["kind"] for event in events],
                capabilities,
            )
        )
        event_results = []
        for event in events:
            if event["kind"] == "remote_run_request":
                payload = dict(event["payload"])
                assert "object_owner_id" not in payload
                run_id = f"run-{payload['object']}"
                queued = {
                    "id": run_id,
                    "status": "queued",
                    "object_name": payload["object"],
                    "resolution": dict(self.resolution),
                }
                self.remote_runs[run_id] = {
                    "id": run_id,
                    "status": "succeeded",
                    "object_name": payload["object"],
                    "result": {
                        "result": {"value": 44},
                        "artifacts": {},
                    },
                }
                result: dict[str, Any] = queued
            else:
                result = {}
            event_results.append(
                {
                    "event_id": event["id"],
                    "kind": event["kind"],
                    "status": "ok",
                    "result": result,
                }
            )
        return {
            "connection": self._connection(),
            "event_results": event_results,
            "jobs": [],
        }

    def create_remote_run(
        self,
        payload: dict[str, Any],
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.calls.append(("create_remote_run", payload, idempotency_key))
        assert "object_owner_id" not in payload
        run_id = f"run-{payload['object']}"
        queued = {
            "id": run_id,
            "status": "queued",
            "object_name": payload["object"],
            "resolution": dict(self.resolution),
        }
        self.remote_runs[run_id] = {
            "id": run_id,
            "status": "succeeded",
            "object_name": payload["object"],
            "result": {
                "result": {"value": 44},
                "artifacts": {},
            },
        }
        return queued

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        self.calls.append(("get_remote_run", run_id))
        return dict(self.remote_runs[run_id])

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        self.calls.append(("list_artifacts", run_id))
        return []


def _save_connection(store: RegistryStore) -> None:
    store.save_server_connection(
        server_url="https://splime.io/api",
        token=secrets.token_urlsafe(24),
        user_token=secrets.token_urlsafe(24),
        connection={
            "id": "shared-default-connection",
            "owner_id": B_ID,
            "subject_type": "machine",
            "subject_id": "b-example-com-machine",
            "machine_id": "b-example-com-machine",
            "display_name": "B acceptance machine",
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
    runtime._mark_server_channel_success(  # noqa: SLF001 - explicit live-channel fixture.
        runtime.store.get_server_connection_credentials(credentials["id"])
    )


def _mark_offline(runtime: Any) -> None:
    credentials = runtime.store.current_server_connection_credentials()
    assert credentials is not None
    runtime._mark_server_channel_failure(credentials)  # noqa: SLF001 - offline replay fixture.


def _request(app: Any, method: str, path: str) -> tuple[int, Any]:
    async def request() -> tuple[int, Any]:
        response = await getattr(app.test_client(), method)(
            path,
            headers={"Authorization": f"Bearer {app.api_token}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(request())


def _build_daemon(tmp_path: Path) -> tuple[RegistryStore, Any, _SharedDefaultServer]:
    store = RegistryStore(tmp_path)
    store.register_env(DEFAULT_OBJECT_LIBRARY, sys.executable)
    server = _SharedDefaultServer()
    _save_connection(store)
    app = create_app(store, server_client_factory=lambda *args, **kwargs: server)
    _mark_live(app.runtime)
    return store, app, server


def _assert_no_handle_in_owner_columns(store: RegistryStore) -> None:
    violations: list[tuple[str, str, Any]] = []
    tables = store._conn.execute(  # noqa: SLF001 - release identity guard.
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    for table_row in tables:
        table = str(table_row["name"])
        columns = store._conn.execute(f'PRAGMA table_info("{table}")').fetchall()  # noqa: SLF001, S608
        for column_row in columns:
            column = str(column_row["name"])
            if "owner" not in column:
                continue
            rows = store._conn.execute(  # noqa: SLF001, S608
                f'SELECT "{column}" AS value FROM "{table}" WHERE CAST("{column}" AS TEXT) LIKE \'@%\''
            ).fetchall()
            violations.extend((table, column, row["value"]) for row in rows)
    assert violations == [], "daemon SQLite must contain canonical owner ids only"


def test_daemon_machine_channel_scoped_signature_and_call_preserve_d1_resolution(
    tmp_path,
) -> None:
    store, app, server = _build_daemon(tmp_path)
    stop_server = None
    server_thread = None
    server_errors: list[BaseException] = []
    reserved, port = _reserved_port_with_free_next()
    reserved.close()
    try:
        stop_server, server_thread, server_errors = _serve_app_in_thread(app, port)
        client = SPLClient(daemon_port=port, api_token=app.api_token)

        signature = client.signature(
            "shared_fn",
            library=DEFAULT_OBJECT_LIBRARY,
        )
        result = client.call(
            "shared_fn",
            library=DEFAULT_OBJECT_LIBRARY,
            progress=False,
        )

        assert signature["resolved_from"] == server.resolution
        assert result.run["resolution"] == server.resolution
        assert result.value == {"value": 44}
        assert result.mode == "server"
        assert not server_errors
    finally:
        if stop_server is not None:
            stop_server.set()
        if server_thread is not None:
            server_thread.join(timeout=5.0)
        app.runtime.shutdown()
        store.close()


def test_daemon_server_catalog_lists_both_shared_default_owners(tmp_path) -> None:
    store, app, server = _build_daemon(tmp_path)
    try:
        status, body = _request(
            app,
            "get",
            f"/server/objects?library={DEFAULT_OBJECT_LIBRARY}&view=summary",
        )

        assert status == 200, "daemon server catalog proxy must stay available"
        assert {(row["owner_id"], row["name"]) for row in body} == {
            (A_ID, "shared_fn"),
            (A_ID, "common_fn"),
            (A_ID, "a_only_fn"),
            (B_ID, "common_fn"),
        }
        assert server.calls[-1] == (
            "list_objects",
            None,
            DEFAULT_OBJECT_LIBRARY,
            True,
        )
    finally:
        app.runtime.shutdown()
        store.close()


def test_daemon_pull_by_handle_persists_canonical_owner(tmp_path) -> None:
    store, app, server = _build_daemon(tmp_path)
    try:
        receipt = app.runtime.pull_server_object(
            "shared_fn",
            owner_id=f"@{A_HANDLE}",
            library=DEFAULT_OBJECT_LIBRARY,
        )
        record = store.get_object(
            "shared_fn",
            owner_id=A_ID,
            library=DEFAULT_OBJECT_LIBRARY,
        )

        assert receipt["failed"] == [], "handle-qualified pull must succeed"
        assert record["owner_id"] == A_ID
        assert record["remote_owner_id"] == A_ID
        assert (
            "get_object",
            "shared_fn",
            None,
            False,
            f"@{A_HANDLE}",
            DEFAULT_OBJECT_LIBRARY,
        ) in server.calls
        _assert_no_handle_in_owner_columns(store)
    finally:
        app.runtime.shutdown()
        store.close()


def test_daemon_offline_signature_and_describe_use_the_canonical_mirror(
    tmp_path,
) -> None:
    store, app, _ = _build_daemon(tmp_path)
    stop_server = None
    server_thread = None
    server_errors: list[BaseException] = []
    reserved, port = _reserved_port_with_free_next()
    reserved.close()
    try:
        app.runtime.pull_server_object(
            "shared_fn",
            owner_id=f"@{A_HANDLE}",
            library=DEFAULT_OBJECT_LIBRARY,
        )
        _mark_offline(app.runtime)
        stop_server, server_thread, server_errors = _serve_app_in_thread(app, port)
        client = SPLClient(daemon_port=port, api_token=app.api_token)

        signature = client.signature(
            "shared_fn",
            owner=A_ID,
            library=DEFAULT_OBJECT_LIBRARY,
        )
        description = client.describe(
            "shared_fn",
            owner=A_ID,
            library=DEFAULT_OBJECT_LIBRARY,
        )

        assert signature["name"] == "shared_fn", "offline signature must use mirror"
        assert "shared_fn v1 (function)" in description
        assert "return 11" not in description
        assert not server_errors
    finally:
        if stop_server is not None:
            stop_server.set()
        if server_thread is not None:
            server_thread.join(timeout=5.0)
        app.runtime.shutdown()
        store.close()


def test_daemon_bare_common_fn_hint_lists_both_canonical_mirrors(tmp_path) -> None:
    store, app, _ = _build_daemon(tmp_path)
    try:
        for owner_ref in (f"@{A_HANDLE}", f"@{B_HANDLE}"):
            app.runtime.pull_server_object(
                "common_fn",
                owner_id=owner_ref,
                library=DEFAULT_OBJECT_LIBRARY,
            )
        _mark_offline(app.runtime)

        status, body = _request(app, "get", "/objects/common_fn")

        assert status == 404, "bare duplicate mirror lookup must fail loudly"
        assert f"{A_ID}/{DEFAULT_OBJECT_LIBRARY}/common_fn" in body["error"]
        assert f"{B_ID}/{DEFAULT_OBJECT_LIBRARY}/common_fn" in body["error"]
        assert body["error"].index(A_ID) < body["error"].index(B_ID)
    finally:
        app.runtime.shutdown()
        store.close()


def test_daemon_offline_forget_handle_returns_rfc_error(tmp_path) -> None:
    store, app, _ = _build_daemon(tmp_path)
    try:
        app.runtime.pull_server_object(
            "shared_fn",
            owner_id=f"@{A_HANDLE}",
            library=DEFAULT_OBJECT_LIBRARY,
        )
        _mark_offline(app.runtime)

        status, body = _request(
            app,
            "delete",
            f"/objects/shared_fn?owner=%40{A_HANDLE}&library={DEFAULT_OBJECT_LIBRARY}",
        )

        assert status == 404, "offline handle forget must fail before local mutation"
        assert body == {
            "error": (
                f"cannot resolve owner '@{A_HANDLE}': handles resolve on the server; "
                "connect first with client.connect_server(...) or pass the canonical owner id"
            ),
            "code": "handle_requires_server_connection",
            "owner": f"@{A_HANDLE}",
        }
        assert (
            store.get_object(
                "shared_fn",
                owner_id=A_ID,
                library=DEFAULT_OBJECT_LIBRARY,
            )["name"]
            == "shared_fn"
        )
    finally:
        app.runtime.shutdown()
        store.close()


def test_daemon_whoami_round_trips_online_then_cached_offline(tmp_path) -> None:
    store, app, server = _build_daemon(tmp_path)
    try:
        online_status, online = _request(app, "get", "/server/whoami")
        _mark_offline(app.runtime)
        offline_status, offline = _request(app, "get", "/server/whoami")

        assert online_status == offline_status == 200
        assert online == {
            "id": B_ID,
            "owner_id": B_ID,
            "handle": B_HANDLE,
            "display_name": "User B",
            "server_url": "https://splime.io/api",
            "machine_id": "b-example-com-machine",
            "connection_status": "connected",
            "live": True,
        }
        assert offline == {
            **online,
            "handle": None,
            "display_name": B_ID,
            "live": False,
        }
        assert [call for call in server.calls if call[0] == "list_users"] == [("list_users", None)]
    finally:
        app.runtime.shutdown()
        store.close()


class _SharedDefaultDaemon:
    def __init__(self, *, solo: bool = False) -> None:
        self.solo = solo
        self.run_calls: list[dict[str, Any]] = []
        self.signature_calls: list[dict[str, Any]] = []
        self.object_calls: list[dict[str, Any]] = []
        self.library_calls: list[tuple[Any, ...]] = []
        self.resolution = {
            "auto_resolved": True,
            "requested_library": DEFAULT_OBJECT_LIBRARY,
            "resolved_owner_id": A_ID,
            "resolved_owner_handle": A_HANDLE,
            "resolved_library": DEFAULT_OBJECT_LIBRARY,
            "resolved_library_id": f"library-{A_ID}",
        }

    def server_connection(self) -> dict[str, Any]:
        return {
            "connected": True,
            "connection": {"status": "connected", "owner_id": B_ID},
        }

    def run(self, object_name: str, **kwargs: Any) -> dict[str, Any]:
        self.run_calls.append({"object_name": object_name, **kwargs})
        if self.solo and object_name != "common_fn":
            raise ClientError(f"404: object is not found: {object_name}", status_code=404)
        if object_name == "a_only_fn":
            raise ClientError(
                "404: object is not found for execute: a_only_fn",
                status_code=404,
            )
        if object_name == "ambiguous_fn":
            raise ClientError(
                "409: object 'ambiguous_fn' is ambiguous in library "
                f"'{DEFAULT_OBJECT_LIBRARY}'; candidates: "
                f"@{A_HANDLE}/{DEFAULT_OBJECT_LIBRARY}, "
                f"@{C_HANDLE}/{DEFAULT_OBJECT_LIBRARY}",
                status_code=409,
            )
        state: dict[str, Any] = {
            "id": f"run-{object_name}",
            "status": "queued",
            "object_name": object_name,
        }
        if object_name == "shared_fn" and not self.solo:
            state["resolution"] = dict(self.resolution)
        return state

    def wait_remote_run(
        self,
        run_id: str,
        *,
        poll_interval: float,
        timeout_seconds: float | None,
        on_state: Any | None = None,
    ) -> dict[str, Any]:
        del poll_interval, timeout_seconds
        state = {
            "id": run_id,
            "status": "succeeded",
            "result": {"result": {"value": 44}, "artifacts": {}},
        }
        if on_state is not None:
            on_state(state)
        return state

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        return {
            "id": run_id,
            "status": "succeeded",
            "result": {"result": {"value": 44}, "artifacts": {}},
        }

    def signature(self, name: str, **kwargs: Any) -> dict[str, Any]:
        self.signature_calls.append({"name": name, **kwargs})
        if self.solo and name != "common_fn":
            raise ClientError(f"404: object is not found: {name}", status_code=404)
        payload: dict[str, Any] = {
            "name": name,
            "display_name": name,
            "version": 1,
            "kind": "function",
            "description": "",
            "inputs": [],
            "outputs": [],
            "call": {"example": f'result = client.call("{name}")', "read": "result.value"},
        }
        if name == "a_only_fn" and not self.solo:
            payload["resolved_from"] = dict(self.resolution)
        return payload

    def server_libraries(
        self,
        *,
        owner: str | None = None,
        include_accessible: bool = True,
    ) -> list[dict[str, Any]]:
        self.library_calls.append(("server_libraries", owner, include_accessible))
        if self.solo:
            return [{"slug": DEFAULT_OBJECT_LIBRARY, "owner_id": B_ID}]
        return [
            {
                "slug": DEFAULT_OBJECT_LIBRARY,
                "owner_id": A_ID,
                "owner_handle": A_HANDLE,
                "owned": False,
            },
            {
                "slug": DEFAULT_OBJECT_LIBRARY,
                "owner_id": B_ID,
                "owner_handle": B_HANDLE,
                "owned": True,
            },
            {
                "slug": DEFAULT_OBJECT_LIBRARY,
                "owner_id": C_ID,
                "owner_handle": C_HANDLE,
                "owned": False,
            },
        ]

    def server_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        self.object_calls.append({"owner_id": owner_id, "library": library, "compact": compact})
        return [
            {
                "name": "common_fn",
                "owner_id": B_ID,
                "library": library or DEFAULT_OBJECT_LIBRARY,
            }
        ]

    def get_server_library(
        self,
        library_ref: str,
        *,
        owner: str | None = None,
    ) -> dict[str, Any]:
        self.library_calls.append(("get_server_library", library_ref, owner))
        if self.solo and owner is not None:
            raise ClientError(
                f"404: library is not found: {owner}/{library_ref}",
                status_code=404,
            )
        if self.solo:
            return {"slug": library_ref, "owner_id": B_ID}
        return {
            "slug": library_ref,
            "owner_id": A_ID,
            "owner_handle": A_HANDLE,
            "owned": False,
        }


def _sdk(daemon: _SharedDefaultDaemon) -> SPLClient:
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon
    return client


def test_sdk_call_shared_fn_auto_resolves_and_receipt_renders_owner() -> None:
    daemon = _SharedDefaultDaemon()
    result = _sdk(daemon).call(
        "shared_fn",
        library=DEFAULT_OBJECT_LIBRARY,
        progress=False,
    )

    assert result.value == {"value": 44}, "SDK call must preserve successful result"
    assert result.run["resolution"] == daemon.resolution
    assert f"@{A_HANDLE}/{DEFAULT_OBJECT_LIBRARY}" in repr(result.run)
    assert daemon.run_calls[-1]["object_owner_id"] is None


def test_sdk_call_common_fn_uses_own_namespace_without_annotation() -> None:
    daemon = _SharedDefaultDaemon()
    result = _sdk(daemon).call(
        "common_fn",
        library=DEFAULT_OBJECT_LIBRARY,
        progress=False,
    )

    assert result.value == {"value": 44}
    assert "resolution" not in result.run
    assert "resolved" not in repr(result.run)


def test_sdk_read_only_a_only_fn_signature_resolves_but_call_is_excluded() -> None:
    daemon = _SharedDefaultDaemon()
    client = _sdk(daemon)

    signature = client.signature("a_only_fn", library=DEFAULT_OBJECT_LIBRARY)
    with pytest.raises(ClientError, match="not found for execute"):
        client.call(
            "a_only_fn",
            library=DEFAULT_OBJECT_LIBRARY,
            progress=False,
        )

    assert signature["resolved_from"] == daemon.resolution
    assert f"owner @{A_HANDLE}" in repr(signature)


def test_sdk_two_foreign_candidates_surface_sorted_handle_scopes() -> None:
    client = _sdk(_SharedDefaultDaemon())

    with pytest.raises(ClientError) as exc_info:
        client.call(
            "ambiguous_fn",
            library=DEFAULT_OBJECT_LIBRARY,
            progress=False,
        )

    message = str(exc_info.value)
    expected = [
        f"@{A_HANDLE}/{DEFAULT_OBJECT_LIBRARY}",
        f"@{C_HANDLE}/{DEFAULT_OBJECT_LIBRARY}",
    ]
    assert all(candidate in message for candidate in expected)
    assert message.index(expected[0]) < message.index(expected[1])


def test_sdk_server_objects_same_slug_requires_explicit_owner() -> None:
    daemon = _SharedDefaultDaemon()
    client = _sdk(daemon)

    with pytest.raises(ClientError) as exc_info:
        client.objects(
            scope="server",
            library=DEFAULT_OBJECT_LIBRARY,
            compact=True,
        )

    message = str(exc_info.value)
    assert f"@{A_HANDLE}/{DEFAULT_OBJECT_LIBRARY}" in message
    assert f"@{B_HANDLE}/{DEFAULT_OBJECT_LIBRARY}" in message
    assert f"@{C_HANDLE}/{DEFAULT_OBJECT_LIBRARY}" in message
    assert daemon.object_calls == []


def test_sdk_library_get_handle_returns_foreign_owner_metadata() -> None:
    daemon = _SharedDefaultDaemon()
    library = _sdk(daemon).library.get(
        DEFAULT_OBJECT_LIBRARY,
        owner=f"@{A_HANDLE}",
    )

    assert library["owner_id"] == A_ID
    assert library["owner_handle"] == A_HANDLE
    assert library["owned"] is False
    assert daemon.library_calls == [("get_server_library", DEFAULT_OBJECT_LIBRARY, f"@{A_HANDLE}")]


def test_sdk_solo_user_043_payloads_and_errors_remain_unannotated() -> None:
    daemon = _SharedDefaultDaemon(solo=True)
    client = _sdk(daemon)

    result = client.call(
        "common_fn",
        library=DEFAULT_OBJECT_LIBRARY,
        progress=False,
    )
    objects = client.objects(
        scope="server",
        library=DEFAULT_OBJECT_LIBRARY,
        compact=True,
    )
    library = client.library.get(DEFAULT_OBJECT_LIBRARY)
    failures: list[str] = []
    for operation in (
        lambda: client.call(
            "shared_fn",
            library=DEFAULT_OBJECT_LIBRARY,
            progress=False,
        ),
        lambda: client.call(
            "a_only_fn",
            library=DEFAULT_OBJECT_LIBRARY,
            progress=False,
        ),
        lambda: client.call(
            "ambiguous_fn",
            library=DEFAULT_OBJECT_LIBRARY,
            progress=False,
        ),
        lambda: client.signature("a_only_fn", library=DEFAULT_OBJECT_LIBRARY),
        lambda: client.library.get(
            DEFAULT_OBJECT_LIBRARY,
            owner=f"@{A_HANDLE}",
        ),
    ):
        with pytest.raises(ClientError) as exc_info:
            operation()
        failures.append(str(exc_info.value))

    assert result.run.raw == {
        "id": "run-common_fn",
        "status": "succeeded",
        "result": {"result": {"value": 44}, "artifacts": {}},
    }
    assert objects.raw == [
        {
            "name": "common_fn",
            "owner_id": B_ID,
            "library": DEFAULT_OBJECT_LIBRARY,
        }
    ]
    assert library.raw == {"slug": DEFAULT_OBJECT_LIBRARY, "owner_id": B_ID}
    assert failures == [
        "404: object is not found: shared_fn",
        "404: object is not found: a_only_fn",
        "404: object is not found: ambiguous_fn",
        "404: object is not found: a_only_fn",
        f"404: library is not found: @{A_HANDLE}/{DEFAULT_OBJECT_LIBRARY}",
    ]
    assert "resolution" not in result.run
    assert "owner_handle" not in objects[0]
    assert "owned" not in library
