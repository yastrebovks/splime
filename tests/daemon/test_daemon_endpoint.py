import socket
import asyncio
import sys
import stat

import pytest

import spl.client as spl_client_module
import spl.daemon.repositories.env as env_repository
from spl.client import SPLClient
import spl.daemon.server as daemon_server
from spl.daemon.client import Client as CompatibilityClient
from spl.daemon_client import (
    DEFAULT_URL,
    Client,
    clear_daemon_endpoint,
    read_daemon_endpoint,
    write_daemon_endpoint,
)
from spl.daemon.server import create_app, select_daemon_port
from spl.daemon.store import RegistryStore


REMOTE_FUNCTION_YAML = """\
- !DFunction
  name: demo_obj
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 1
"""


PIPELINE_WITH_INTERNAL_FUNCTION_YAML = """\
- !DPipeline
  name: demo_pipeline
  nodes:
  - !DNodeFunction
    uuid: 11111111-1111-4111-8111-111111111111
    func: inner_add
  links: []
  aliases:
  - - total
    - 11111111-1111-4111-8111-111111111111
---
- !DFunction
  name: inner_add
  inputs:
  - name: a
    type: int
    default: null
  - name: b
    type: int
    default: null
  outputs:
  - name: default
    type: int
  body: |-
    return a + b
"""


def _json_from_app(app, path: str):
    async def _request():
        client = app.test_client()
        response = await client.get(
            path,
            headers={"Authorization": f"Bearer {app.api_token}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(_request())


def _post_json_from_app(app, path: str, payload: dict):
    async def _request():
        client = app.test_client()
        response = await client.post(
            path,
            json=payload,
            headers={"Authorization": f"Bearer {app.api_token}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(_request())


def _put_json_from_app(app, path: str, payload: dict):
    async def _request():
        client = app.test_client()
        response = await client.put(
            path,
            json=payload,
            headers={"Authorization": f"Bearer {app.api_token}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(_request())


def _delete_json_from_app(app, path: str):
    async def _request():
        client = app.test_client()
        response = await client.delete(
            path,
            headers={"Authorization": f"Bearer {app.api_token}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(_request())


def _json_from_app_without_auth(app, path: str):
    async def _request():
        client = app.test_client()
        response = await client.get(path)
        return response.status_code, await response.get_json()

    return asyncio.run(_request())


def _shutdown_app(app) -> None:
    if app is not None:
        app.runtime.shutdown()


def _save_connected_server_connection(store: RegistryStore) -> dict:
    return store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-123456",
        user_token="user-token-123456",
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


def _reserved_port_with_free_next() -> tuple[socket.socket, int]:
    for port in range(20000, 45000):
        try:
            reserved = socket.create_server(("127.0.0.1", port), backlog=1)
        except OSError:
            continue
        try:
            with socket.create_server(("127.0.0.1", port + 1), backlog=1):
                return reserved, port
        except OSError:
            reserved.close()
    raise RuntimeError("could not find a reserved port with a free next port")


def test_clients_accept_explicit_daemon_port() -> None:
    assert Client(daemon_port=9876).base_url == "http://127.0.0.1:9876"
    assert SPLClient(daemon_port=9877)._daemon.base_url == "http://127.0.0.1:9877"
    assert SPLClient(daemon_port=9877, api_token="token")._daemon.api_token == "token"
    assert CompatibilityClient(daemon_port=9878).base_url == "http://127.0.0.1:9878"


def test_clients_use_saved_daemon_endpoint(tmp_path) -> None:
    api_token = "local-api-token"
    endpoint = write_daemon_endpoint(
        tmp_path,
        bind_host="127.0.0.1",
        host="127.0.0.1",
        port=8766,
        api_token=api_token,
        updated_at="2026-06-05T00:00:00+00:00",
    )

    assert read_daemon_endpoint(tmp_path) == endpoint
    endpoint_mode = stat.S_IMODE((tmp_path / "daemon-endpoint.json").stat().st_mode)
    assert endpoint_mode == 0o600
    assert Client(daemon_home=tmp_path).base_url == "http://127.0.0.1:8766"
    assert Client(daemon_home=tmp_path).api_token == api_token
    assert SPLClient(daemon_home=tmp_path)._daemon.base_url == "http://127.0.0.1:8766"
    assert SPLClient(daemon_home=tmp_path)._daemon.api_token == api_token

    clear_daemon_endpoint(tmp_path, base_url="http://127.0.0.1:8765")
    assert read_daemon_endpoint(tmp_path) == endpoint

    clear_daemon_endpoint(tmp_path, base_url=endpoint["base_url"])
    assert read_daemon_endpoint(tmp_path) is None
    assert Client(daemon_home=tmp_path).base_url == DEFAULT_URL


def test_base_url_and_daemon_port_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="either base_url or daemon_port"):
        Client("http://127.0.0.1:8765", daemon_port=8766)


def test_spl_client_register_env_forwards_omitted_python(monkeypatch) -> None:
    calls = []

    def fake_register_env(self, name, python=None):
        calls.append((name, python))
        return {"name": name, "python": python}

    monkeypatch.setattr(spl_client_module.Client, "register_env", fake_register_env)

    result = SPLClient().register_env("default")

    assert result == {"name": "default", "python": None}
    assert calls == [("default", None)]


def test_daemon_client_register_env_omits_python_payload_when_omitted(
    monkeypatch,
    tmp_path,
) -> None:
    calls = []

    def fake_json_request(self, method, path, payload=None):
        calls.append((method, path, payload))
        return {"ok": True}

    monkeypatch.setattr(Client, "_json_request", fake_json_request)

    client = Client()
    assert client.register_env("default") == {"ok": True}
    explicit_python = str(tmp_path / "client-python")
    assert client.register_env("custom", explicit_python) == {"ok": True}

    assert calls == [
        ("POST", "/envs", {"name": "default"}),
        ("POST", "/envs", {"name": "custom", "python": explicit_python}),
    ]


def test_register_env_route_defaults_to_daemon_python(tmp_path, monkeypatch) -> None:
    daemon_python = tmp_path / "daemon-python"
    daemon_python.touch()
    client_python = tmp_path / "client-python"
    client_python.touch()
    monkeypatch.setattr(env_repository.sys, "executable", str(daemon_python))

    store = RegistryStore(tmp_path)
    app = None
    try:
        app = create_app(store)
        status, body = _post_json_from_app(app, "/envs", {"name": "default"})

        assert status == 201
        assert body["name"] == "default"
        assert body["python"] == str(daemon_python.absolute())
        assert body["python"] != str(client_python.absolute())
    finally:
        _shutdown_app(app)
        store.close()


def test_register_env_route_preserves_explicit_valid_python(tmp_path) -> None:
    python = tmp_path / "explicit-python"
    python.touch()

    store = RegistryStore(tmp_path)
    app = None
    try:
        app = create_app(store)
        status, body = _post_json_from_app(
            app,
            "/envs",
            {"name": "explicit", "python": str(python)},
        )

        assert status == 201
        assert body["name"] == "explicit"
        assert body["python"] == str(python.absolute())
    finally:
        _shutdown_app(app)
        store.close()


def test_register_env_route_rejects_explicit_missing_python(tmp_path) -> None:
    missing_python = tmp_path / "missing-python"

    store = RegistryStore(tmp_path)
    app = None
    try:
        app = create_app(store)
        status, body = _post_json_from_app(
            app,
            "/envs",
            {"name": "missing", "python": str(missing_python)},
        )

        assert status == 400
        assert body == {
            "error": f"python executable is not found: {missing_python.absolute()}"
        }
    finally:
        _shutdown_app(app)
        store.close()


def test_select_daemon_port_scans_when_preferred_port_is_busy() -> None:
    reserved, preferred_port = _reserved_port_with_free_next()
    with reserved:
        assert (
            select_daemon_port(
                "127.0.0.1",
                preferred_port,
                auto_port=True,
                scan_limit=2,
            )
            == preferred_port + 1
        )


def test_select_daemon_port_can_require_the_exact_port() -> None:
    reserved, preferred_port = _reserved_port_with_free_next()
    with reserved:
        with pytest.raises(OSError, match="already busy"):
            select_daemon_port(
                "127.0.0.1",
                preferred_port,
                auto_port=False,
            )


def test_health_and_diagnostics_include_sync_retry_visibility(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        event = store.enqueue_sync_event(
            "object_version",
            {"name": "demo_obj", "version": 1},
        )
        store.mark_sync_event_failed(event["id"], "temporary server error")

        app = create_app(store)
        unauth_status, unauth_body = _json_from_app_without_auth(app, "/health")
        health_status, health = _json_from_app(app, "/health")
        diagnostics_status, diagnostics = _json_from_app(app, "/diagnostics")

        assert unauth_status == 401
        assert unauth_body == {"error": "missing or invalid local daemon API token"}
        assert health_status == 200
        assert health["counts"]["pending_sync_events"] == 1
        assert health["sync"]["retryable"] == 1
        assert health["sync"]["last_error"] == "temporary server error"

        assert diagnostics_status == 200
        assert diagnostics["sync"]["retryable"] == 1
        assert diagnostics["pending_sync_events"][0]["retry"]["will_retry"] is True
        assert diagnostics["pending_sync_events"][0]["retry"]["next_attempt"] == 2
    finally:
        _shutdown_app(app)
        store.close()


def test_signature_can_describe_pipeline_internal_function(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "demo_pipeline",
            "demo_pipeline",
            "default",
            yaml_text=PIPELINE_WITH_INTERNAL_FUNCTION_YAML,
        )

        app = create_app(store)
        status, body = _json_from_app(
            app,
            "/objects/demo_pipeline/signature?function=inner_add",
        )
        inline_status, inline_body = _json_from_app(
            app,
            "/objects/demo_pipeline::inner_add/signature",
        )

        assert status == 200
        assert body["kind"] == "function"
        assert body["function"] == "inner_add"
        assert body["parent_object"]["name"] == "demo_pipeline"
        assert body["call"]["example"].startswith(
            'result = client.call("demo_pipeline", '
        )
        assert 'function="inner_add"' in body["call"]["example"]
        assert [item["name"] for item in body["inputs"]] == ["a", "b"]
        assert inline_status == 200
        assert inline_body["call"]["example"] == body["call"]["example"]
    finally:
        _shutdown_app(app)
        store.close()


def test_signature_imports_server_object_by_display_name(tmp_path, monkeypatch) -> None:
    class ImportServerClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def get_object(self, name_or_id, *, version=None, include_yaml=False):
            assert name_or_id == "demo_obj"
            assert include_yaml is False
            return {
                "id": "remote-object-1",
                "owner_id": "owner-1",
                "name": "demo_obj",
                "version": 1,
                "version_id": "remote-version-1",
                "entrypoint": "demo_obj",
                "env": "default",
            }

        def list_object_versions(self, name_or_id, *, include_yaml=False):
            assert name_or_id == "demo_obj"
            assert include_yaml is True
            return [
                {
                    "id": "remote-object-1",
                    "owner_id": "owner-1",
                    "name": "demo_obj",
                    "version": 1,
                    "version_id": "remote-version-1",
                    "entrypoint": "demo_obj",
                    "env": "default",
                    "description": "remote demo",
                    "version_label": "v1",
                    "yaml": REMOTE_FUNCTION_YAML,
                },
            ]

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", ImportServerClient)
    app = None
    try:
        store.register_env("default", sys.executable)
        store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-123456",
            user_token="user-token-123456",
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

        app = create_app(store)
        status, body = _json_from_app(app, "/objects/demo_obj/signature")

        assert status == 200
        assert body["name"] == "demo_obj"
        assert body["display_name"] == "demo_obj"
        assert 'client.call("demo_obj"' in body["call"]["example"]
    finally:
        _shutdown_app(app)
        store.close()


def test_server_objects_are_listed_through_daemon_proxy(tmp_path, monkeypatch) -> None:
    class CatalogServerClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def list_objects(self, *, owner_id=None, library=None, compact=False):
            return [
                {
                    "name": "demo_obj",
                    "library": library or "default",
                    "owner_id": owner_id or "owner-1",
                    "compact": compact,
                }
            ]

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", CatalogServerClient)
    app = None
    try:
        store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-123456",
            user_token="user-token-123456",
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

        app = create_app(store)
        status, body = _json_from_app(
            app,
            "/server/objects?library=default&view=summary",
        )

        assert status == 200
        assert body == [
            {
                "name": "demo_obj",
                "library": "default",
                "owner_id": "owner-1",
                "compact": True,
            }
        ]
    finally:
        _shutdown_app(app)
        store.close()


def test_server_libraries_are_managed_through_daemon_proxy(
    tmp_path,
    monkeypatch,
) -> None:
    class LibraryServerClient:
        calls = []

        def __init__(self, base_url, machine_token, *, user_token=None) -> None:
            assert base_url == "https://splime.io/api"
            assert machine_token == "machine-token-123456"
            assert user_token == "user-token-123456"

        def list_libraries(self, *, include_accessible=True):
            self.calls.append(("list_libraries", include_accessible))
            return [{"slug": "risk", "accessible": include_accessible}]

        def create_library(self, payload):
            self.calls.append(("create_library", payload))
            return {"slug": payload["slug"], "created": True}

        def get_library(self, library_ref):
            self.calls.append(("get_library", library_ref))
            return {"slug": library_ref}

        def update_library(self, library_ref, payload):
            self.calls.append(("update_library", library_ref, payload))
            return {"slug": library_ref, **payload}

        def delete_library(self, library_ref):
            self.calls.append(("delete_library", library_ref))
            return {"slug": library_ref, "deleted": True}

        def list_library_grants(self, library_ref):
            self.calls.append(("list_library_grants", library_ref))
            return [{"library": library_ref, "grantee_id": "admin2"}]

        def grant_library(self, library_ref, payload):
            self.calls.append(("grant_library", library_ref, payload))
            return {"library": library_ref, **payload}

        def revoke_library_grant(self, library_ref, grantee):
            self.calls.append(("revoke_library_grant", library_ref, grantee))
            return {"library": library_ref, "grantee": grantee, "revoked": True}

        def add_library_reference(self, library_ref, payload):
            self.calls.append(("add_library_reference", library_ref, payload))
            return {"library": library_ref, "reference": payload}

        def copy_object_into_library(self, library_ref, payload):
            self.calls.append(("copy_object_into_library", library_ref, payload))
            return {"library": library_ref, "copy": payload}

        def remove_library_entry(self, library_ref, name):
            self.calls.append(("remove_library_entry", library_ref, name))
            return {"library": library_ref, "name": name, "removed": True}

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", LibraryServerClient)
    app = None
    try:
        app = create_app(store)
        _save_connected_server_connection(store)

        status, body = _json_from_app(app, "/server/libraries?include_accessible=0")
        assert status == 200
        assert body == [{"slug": "risk", "accessible": False}]

        status, body = _post_json_from_app(
            app,
            "/server/libraries",
            {"slug": "risk", "display_name": "Risk"},
        )
        assert status == 201
        assert body == {"slug": "risk", "created": True}

        status, body = _json_from_app(app, "/server/libraries/risk")
        assert status == 200
        assert body == {"slug": "risk"}

        status, body = _put_json_from_app(
            app,
            "/server/libraries/risk",
            {"description": "Updated"},
        )
        assert status == 200
        assert body == {"slug": "risk", "description": "Updated"}

        status, body = _json_from_app(app, "/server/libraries/risk/grants")
        assert status == 200
        assert body == [{"library": "risk", "grantee_id": "admin2"}]

        grant_payload = {"grantee_id": "admin2", "scopes": ["metadata:read"]}
        status, body = _post_json_from_app(
            app,
            "/server/libraries/risk/grants",
            grant_payload,
        )
        assert status == 201
        assert body == {"library": "risk", **grant_payload}

        status, body = _post_json_from_app(
            app,
            "/server/libraries/risk/grants/admin2/revoke",
            {},
        )
        assert status == 200
        assert body == {"library": "risk", "grantee": "admin2", "revoked": True}

        reference_payload = {
            "from_owner": "admin2",
            "from_library": "default",
            "name": "source",
        }
        status, body = _post_json_from_app(
            app,
            "/server/libraries/risk/references",
            reference_payload,
        )
        assert status == 201
        assert body == {"library": "risk", "reference": reference_payload}

        copy_payload = {"from_library": "default", "name": "source"}
        status, body = _post_json_from_app(
            app,
            "/server/libraries/risk/copies",
            copy_payload,
        )
        assert status == 201
        assert body == {"library": "risk", "copy": copy_payload}

        status, body = _delete_json_from_app(
            app,
            "/server/libraries/risk/entries/source",
        )
        assert status == 200
        assert body == {"library": "risk", "name": "source", "removed": True}

        status, body = _delete_json_from_app(app, "/server/libraries/risk")
        assert status == 200
        assert body == {"slug": "risk", "deleted": True}

        assert LibraryServerClient.calls == [
            ("list_libraries", False),
            ("create_library", {"slug": "risk", "display_name": "Risk"}),
            ("get_library", "risk"),
            ("update_library", "risk", {"description": "Updated"}),
            ("list_library_grants", "risk"),
            ("grant_library", "risk", grant_payload),
            ("revoke_library_grant", "risk", "admin2"),
            ("add_library_reference", "risk", reference_payload),
            ("copy_object_into_library", "risk", copy_payload),
            ("remove_library_entry", "risk", "source"),
            ("delete_library", "risk"),
        ]
    finally:
        _shutdown_app(app)
        store.close()


def test_server_library_routes_report_missing_connection(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        app = create_app(store)
        status, body = _json_from_app(app, "/server/libraries")

        assert status == 404
        assert "active server connection is not found" in body["error"]
    finally:
        _shutdown_app(app)
        store.close()


def test_object_registration_sync_event_preserves_target_library(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        app = create_app(store, auto_build_envs=False)
        status, body = _post_json_from_app(
            app,
            "/objects",
            {
                "name": "demo_obj",
                "entrypoint": "demo_obj",
                "env": "default",
                "yaml": REMOTE_FUNCTION_YAML,
                "library": "risk",
            },
        )

        assert status == 201
        assert body["sync_event"]["payload"]["library"] == "risk"
        assert body["sync"]["connected"] is False
        pending = store.list_pending_sync_events()
        assert len(pending) == 1
        assert pending[0]["kind"] == "object_version"
        assert pending[0]["payload"]["library"] == "risk"
    finally:
        _shutdown_app(app)
        store.close()


def test_object_registration_sync_event_preserves_library_create_request(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        app = create_app(store, auto_build_envs=False)
        status, body = _post_json_from_app(
            app,
            "/objects",
            {
                "name": "demo_obj",
                "entrypoint": "demo_obj",
                "env": "default",
                "yaml": REMOTE_FUNCTION_YAML,
                "library": "research",
                "library_display_name": "Research",
                "create_library": True,
            },
        )

        assert status == 201
        assert body["sync_event"]["payload"]["library"] == "research"
        assert body["sync_event"]["payload"]["create_library"] is True
        assert body["sync_event"]["payload"]["library_display_name"] == "Research"
        pending = store.list_pending_sync_events()
        assert len(pending) == 1
        assert pending[0]["kind"] == "object_version"
        assert pending[0]["payload"]["library"] == "research"
        assert pending[0]["payload"]["create_library"] is True
        assert pending[0]["payload"]["library_display_name"] == "Research"
    finally:
        _shutdown_app(app)
        store.close()


def test_delete_object_route_forgets_local_object_without_server_connection(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        first = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=REMOTE_FUNCTION_YAML,
        )
        second = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=REMOTE_FUNCTION_YAML.replace("return 1", "return 2"),
        )

        app = create_app(store, auto_build_envs=False)
        assert store.current_server_connection() is None

        status, body = _delete_json_from_app(app, "/objects/demo_obj?version=1")
        assert status == 200
        assert body["object_deleted"] is False
        assert body["version"]["id"] == first["version_id"]
        assert store.get_object("demo_obj")["version_id"] == second["version_id"]

        status, body = _delete_json_from_app(app, "/objects/demo_obj")
        assert status == 200
        assert body["object_deleted"] is True
        assert body["object"]["canonical_name"] == "local/default/demo_obj"
        assert body["deleted"]["versions"] == 1

        status, body = _json_from_app(app, "/objects/demo_obj")
        assert status == 404
        assert "object is not registered" in body["error"]
    finally:
        _shutdown_app(app)
        store.close()


def test_remote_decomposition_resolves_through_daemon_proxy(tmp_path, monkeypatch) -> None:
    class DecompositionServerClient:
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
            assert name_or_id == "demo_traktorist_pipeline"
            assert version is None
            assert include_yaml is False
            assert owner_id == "alice"
            assert library == "tractors"
            return {
                "id": "remote-object-1",
                "owner_id": "alice",
                "name": "demo_traktorist_pipeline",
                "version": 3,
                "version_id": "remote-version-3",
                "library": {"slug": "tractors"},
                "decomposition": {
                    "functions": [],
                    "nodes": [{"node_id": "node-1", "kind": "remote"}],
                    "links": [],
                },
            }

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", DecompositionServerClient)
    app = None
    try:
        store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-123456",
            user_token="user-token-123456",
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

        app = create_app(store)
        status, body = _post_json_from_app(
            app,
            "/remote-decompositions/resolve",
            {
                "ref": {
                    "url": "https://splime.io/api",
                    "name": "demo_traktorist_pipeline",
                    "version": "latest",
                    "owner_id": "alice",
                    "library": "tractors",
                }
            },
        )

        assert status == 200
        assert body["decomposition"]["nodes"] == [
            {"node_id": "node-1", "kind": "remote"}
        ]
        assert body["remote"] == {
            "url": "https://splime.io/api",
            "name": "demo_traktorist_pipeline",
            "function": None,
            "requested_version": "latest",
            "owner_id": "alice",
            "library": "tractors",
            "version_id": "remote-version-3",
            "object_id": "remote-object-1",
        }
    finally:
        _shutdown_app(app)
        store.close()
