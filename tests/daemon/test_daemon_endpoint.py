import asyncio
import json
import logging
import socket
import sys
import stat
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import spl._client as spl_client_module
import spl.daemon_client as daemon_client
import spl.daemon.repositories.env as env_repository
from spl import SPLClient
import spl.daemon.server as daemon_server
from spl.daemon.client import Client as CompatibilityClient
from spl.daemon_client import (
    DEFAULT_URL,
    Client,
    clear_daemon_endpoint,
    read_daemon_endpoint,
    write_daemon_endpoint,
)
from spl.core.entities.adapter import JSON_ADAPTER_KEY
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


def _remote_function_yaml(name: str, value: int) -> str:
    return """\
- !DFunction
  name: {name}
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return {value}
""".format(name=name, value=value)


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


SECRET_PIPELINE_YAML = """\
- !DPipeline
  name: secret_pipeline
  nodes:
  - !DNodeFunction
    uuid: 33333333-3333-4333-8333-333333333333
    func: accept_secret
  links: []
  aliases:
  - - login
    - 33333333-3333-4333-8333-333333333333
---
- !DFunction
  name: accept_secret
  inputs:
  - name: password
    type: str
    default: null
  outputs:
  - name: default
    type: str
  body: |-
    return "ok"
"""


VENV_SUBPROCESS_PIPELINE_YAML = """\
- !DPipeline
  name: daemon_venv_pipeline
  nodes:
  - !DNodeFunction
    uuid: 44444444-4444-4444-8444-444444444444
    func: daemon_seed
  - !DNodeFunction
    uuid: 55555555-5555-4555-8555-555555555555
    func: daemon_consumer
  links:
  - - !DNodeInputRef
      uuid: 55555555-5555-4555-8555-555555555555
      port: value
    - !DNodeOutputRef
      uuid: 44444444-4444-4444-8444-444444444444
      port: default
  aliases:
  - - seed
    - 44444444-4444-4444-8444-444444444444
  - - consumer
    - 55555555-5555-4555-8555-555555555555
  adapters: []
  tags:
    55555555-5555-4555-8555-555555555555:
      runtime: venv-subprocess
---
- !DFunction
  name: daemon_seed
  inputs: []
  outputs:
  - name: default
    type: str
  body: |-
    return "seed"
---
- !DFunction
  name: daemon_consumer
  inputs:
  - name: value
    type: str
    default: null
  outputs:
  - name: default
    type: str
  body: |-
    return value.upper()
"""


RESUMABLE_PIPELINE_YAML = """\
- !DPipeline
  name: resumable_pipeline
  nodes:
  - !DNodeFunction
    uuid: 11111111-1111-4111-8111-111111111111
    func: produce_text
  - !DNodeFunction
    uuid: 22222222-2222-4222-8222-222222222222
    func: consume_text
  links:
  - - !DNodeInputRef
      uuid: 22222222-2222-4222-8222-222222222222
      port: value
    - !DFormattedOutputRef
      uuid: 11111111-1111-4111-8111-111111111111
      port: default
      format: txt
  aliases:
  - - producer
    - 11111111-1111-4111-8111-111111111111
  - - consumer
    - 22222222-2222-4222-8222-222222222222
  adapters:
  - !DAdapter
    key: builtins.str@txt
    save: save_text
    load: bad_load_text
    distributions: []
- !DSPLSelfImport
  name: produce_text
- !DSPLSelfImport
  name: consume_text
- !DSPLSelfImport
  name: save_text
- !DSPLSelfImport
  name: bad_load_text
- !DSPLSelfImport
  name: good_load_text
---
- !DFunction
  name: produce_text
  inputs:
  - name: counter_path
    type: str
    default: null
  outputs:
  - name: default
    type: str
  body: |-
    from pathlib import Path
    path = Path(counter_path)
    count = int(path.read_text()) if path.exists() else 0
    path.write_text(str(count + 1))
    return "seed"
---
- !DFunction
  name: consume_text
  inputs:
  - name: value
    type: str
    default: null
  outputs:
  - name: default
    type: str
  body: |-
    return "consumed:" + value
---
- !DFunction
  name: save_text
  inputs:
  - name: path
    type: null
    default: null
  - name: value
    type: null
    default: null
  outputs:
  - name: default
    type: null
  body: |-
    with open(path, "w", encoding="utf-8") as f:
        f.write(value)
---
- !DFunction
  name: bad_load_text
  inputs:
  - name: path
    type: null
    default: null
  outputs:
  - name: default
    type: null
  body: |-
    raise RuntimeError("bad load for " + open(path, encoding="utf-8").read())
---
- !DFunction
  name: good_load_text
  inputs:
  - name: path
    type: null
    default: null
  outputs:
  - name: default
    type: null
  body: |-
    return open(path, encoding="utf-8").read()
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


def _wait_store_run(store: RegistryStore, run_id: str, *, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = store.get_run(run_id)
        if state["status"] in {"succeeded", "failed", "cancelled", "stale"}:
            return state
        time.sleep(0.05)
    raise TimeoutError(f"run did not finish: {run_id}")


def _manifest_node_by_alias(manifest: dict, alias: str) -> dict:
    return next(node for node in manifest["nodes"].values() if node["alias"] == alias)


def _worker_manifest_dir(run_dir: str) -> Path:
    manifests = sorted((Path(run_dir) / "pipeline-state").glob("*/manifest.json"))
    assert manifests
    return manifests[0].parent


def _json_from_app_without_auth(app, path: str):
    async def _request():
        client = app.test_client()
        response = await client.get(path)
        return response.status_code, await response.get_json()

    return asyncio.run(_request())


def _shutdown_app(app) -> None:
    if app is not None:
        app.runtime.shutdown()


def _save_connected_server_connection(
    store: RegistryStore,
    *,
    server_url: str = "https://splime.io/api",
) -> dict:
    return store.save_server_connection(
        server_url=server_url,
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


def _mark_current_server_channel_live(runtime) -> None:
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


def _closed_local_server_url() -> str:
    reserved = socket.socket()
    reserved.bind(("127.0.0.1", 0))
    port = reserved.getsockname()[1]
    reserved.close()
    return f"http://127.0.0.1:{port}"


def _serve_app_in_thread(app, port: int) -> tuple[threading.Event, threading.Thread, list[BaseException]]:
    stop_event = threading.Event()
    errors: list[BaseException] = []

    def _run() -> None:
        from hypercorn.asyncio import serve as hypercorn_serve
        from hypercorn.config import Config

        async def _shutdown_trigger() -> None:
            while not stop_event.is_set():
                await asyncio.sleep(0.05)

        async def _serve() -> None:
            config = Config()
            config.bind = [f"127.0.0.1:{port}"]
            config.use_reloader = False
            config.accesslog = None
            config.errorlog = None
            await hypercorn_serve(app, config, shutdown_trigger=_shutdown_trigger)

        try:
            asyncio.run(_serve())
        except BaseException as exc:  # pragma: no cover - re-raised by caller.
            errors.append(exc)

    thread = threading.Thread(target=_run, name=f"spl-test-daemon-{port}", daemon=True)
    thread.start()

    client = Client(f"http://127.0.0.1:{port}", api_token=app.api_token)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if errors:
            raise RuntimeError("test daemon failed to start") from errors[0]
        try:
            client.health()
            return stop_event, thread, errors
        except Exception:
            time.sleep(0.05)

    stop_event.set()
    thread.join(timeout=2.0)
    raise TimeoutError(f"test daemon did not start on port {port}")


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


def test_run_management_cli_commands(monkeypatch, capsys) -> None:
    import spl.daemon.cli as cli_module

    calls = []

    class FakeClient:
        def __init__(self, url=None) -> None:
            calls.append(("init", url))

        def list_runs(self):
            calls.append(("list_runs",))
            return [{"id": "run-1", "has_manifest": True}]

        def run_tag_stats(self):
            calls.append(("run_tag_stats",))
            return {"runs_scanned": 0, "edges_scanned": 0, "tags": [], "pairs": []}

        def show_run(self, run_id, *, full_inline=False):
            calls.append(("show_run", run_id, full_inline))
            return {"id": run_id, "full_inline": full_inline}

        def prune_runs(self, *, run_id=None, statuses=None, older_than_seconds=None, dry_run=False):
            calls.append(("prune_runs", run_id, statuses, older_than_seconds, dry_run))
            return {"count": 1, "dry_run": dry_run}

    monkeypatch.setattr(cli_module, "Client", FakeClient)

    assert cli_module.main(["run-list"]) == 0
    assert cli_module.main(["run-list", "--tag-stats"]) == 0
    assert cli_module.main(["run-show", "run-1", "--full-inline"]) == 0
    assert cli_module.main(["run-prune", "run-1", "--status", "failed", "--dry-run"]) == 0

    captured = capsys.readouterr()
    assert '"has_manifest": true' in captured.out
    assert '"tags": []' in captured.out
    assert calls == [
        ("init", None),
        ("list_runs",),
        ("init", None),
        ("run_tag_stats",),
        ("init", None),
        ("show_run", "run-1", True),
        ("init", None),
        ("prune_runs", "run-1", ["failed"], None, True),
    ]


def test_connection_hygiene_cli_commands(monkeypatch, capsys) -> None:
    import spl.daemon.cli as cli_module

    calls = []

    class FakeClient:
        def __init__(self, url=None) -> None:
            calls.append(("init", url))

        def server_connections(self):
            calls.append(("server_connections",))
            return [{"id": "connection-1"}]

        def prune_server_connections(self, *, older_than_days=30, dry_run=False):
            calls.append(("prune_server_connections", older_than_days, dry_run))
            return {"count": 0, "dry_run": dry_run}

    monkeypatch.setattr(cli_module, "Client", FakeClient)

    assert cli_module.main(["connections-list"]) == 0
    assert cli_module.main(["connections-prune", "--older-than-days", "7", "--dry-run"]) == 0

    captured = capsys.readouterr()
    assert '"connection-1"' in captured.out
    assert '"dry_run": true' in captured.out
    assert calls == [
        ("init", None),
        ("server_connections",),
        ("init", None),
        ("prune_server_connections", 7, True),
    ]


def test_pull_cli_command_uses_daemon_pull_endpoint(monkeypatch, capsys) -> None:
    import spl.daemon.cli as cli_module

    calls = []

    class FakeClient:
        def __init__(self, url=None) -> None:
            calls.append(("init", url))

        def pull_server_object(
            self,
            name,
            *,
            owner_id=None,
            library=None,
            version=None,
            all_versions=False,
        ):
            calls.append(("pull_server_object", name, owner_id, library, version, all_versions))
            return {
                "pulled": ["owner-1/risk/demo_obj@v3"],
                "skipped": [],
                "failed": [],
                "ambiguous_names": [],
            }

    monkeypatch.setattr(cli_module, "Client", FakeClient)

    assert (
        cli_module.main(
            [
                "pull",
                "demo_obj",
                "--owner",
                "owner-1",
                "--library",
                "risk",
                "--version",
                "3",
                "--all-versions",
            ]
        )
        == 0
    )

    assert '"pulled": [' in capsys.readouterr().out
    assert calls == [
        ("init", None),
        ("pull_server_object", "demo_obj", "owner-1", "risk", 3, True),
    ]


def test_pull_all_cli_command_uses_daemon_pull_all_batch(monkeypatch, capsys) -> None:
    import spl.daemon.cli as cli_module

    calls = []

    class FakeClient:
        def __init__(self, url=None) -> None:
            calls.append(("init", url))

        def pull_all_server_objects(
            self,
            *,
            owner_id=None,
            library=None,
            all_versions=False,
            dry_run=False,
        ):
            calls.append(("pull_all_server_objects", owner_id, library, all_versions, dry_run))
            return {
                "objects_seen": 2,
                "pulled": ["owner-1/risk/demo_obj@v3"],
                "skipped": [],
                "failed": [],
                "ambiguous_names": [],
            }

    monkeypatch.setattr(cli_module, "Client", FakeClient)

    assert (
        cli_module.main(
            [
                "pull",
                "--all",
                "--owner",
                "owner-1",
                "--library",
                "risk",
                "--all-versions",
                "--dry-run",
            ]
        )
        == 0
    )

    assert '"objects_seen": 2' in capsys.readouterr().out
    assert calls == [
        ("init", None),
        ("pull_all_server_objects", "owner-1", "risk", True, True),
    ]


def test_run_show_cli_hides_sensitive_inline_values_without_flag(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    import spl.daemon.cli as cli_module
    from spl.core import manifest as m_manifest

    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run_id = "sensitive-run"
    run_dir = m_manifest.create_run_dir(run_id)
    payload = m_manifest.build_initial_manifest(run_id=run_id, keep=True, pipeline_name="sensitive_pipeline")
    payload["nodes"] = {
        "node-1": {
            "id": "node-1",
            "alias": "login",
            "status": "succeeded",
            "inputs": {
                "password": {
                    "kind": "json",
                    "tag": "json",
                    "value": "hunter2",
                    "sha256": "0" * 64,
                }
            },
            "outputs": {},
        }
    }
    m_manifest.atomic_write_json(run_dir / m_manifest.RUN_MANIFEST_FILENAME, payload)

    assert cli_module.main(["run-show", run_id, "--local"]) == 0
    output = capsys.readouterr().out

    assert "hunter2" not in output
    assert '"value_preview": "<omitted>"' in output
    assert '"value_size_bytes"' in output
    assert '"sha256": "' in output

    assert cli_module.main(["run-show", run_id, "--local", "--full-inline"]) == 0
    full_output = capsys.readouterr().out
    assert "hunter2" in full_output


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
        assert body == {"error": f"python executable is not found: {missing_python.absolute()}"}
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


def test_blocking_remote_node_http_call_is_not_capped_by_default_timeout(
    tmp_path,
    monkeypatch,
) -> None:
    reserved, port = _reserved_port_with_free_next()
    reserved.close()
    store = RegistryStore(tmp_path)
    app = None
    server_thread: threading.Thread | None = None
    stop_server: threading.Event | None = None
    server_errors: list[BaseException] = []
    try:
        app = create_app(store, api_token="test-token")

        def slow_remote_node(
            node: dict[str, object],
            *,
            kwargs: dict[str, object],
            timeout_seconds: float | None = None,
        ) -> dict[str, object]:
            _ = node
            time.sleep(0.15)
            return {
                "value": int(kwargs["value"]) + 1,
                "timeout_seconds": timeout_seconds,
            }

        monkeypatch.setattr(app.runtime, "run_remote_node", slow_remote_node)
        stop_server, server_thread, server_errors = _serve_app_in_thread(app, port)
        monkeypatch.setattr(daemon_client, "DEFAULT_HTTP_TIMEOUT_SECONDS", 0.01)

        result = Client(f"http://127.0.0.1:{port}", api_token=app.api_token).run_remote_node(
            {"name": "slow_remote"},
            kwargs={"value": 6},
        )

        assert result == {"value": 7, "timeout_seconds": None}
    finally:
        if stop_server is not None:
            stop_server.set()
        if server_thread is not None:
            server_thread.join(timeout=5.0)
        _shutdown_app(app)
        store.close()

    if server_thread is not None and server_thread.is_alive():
        raise AssertionError("test daemon thread did not stop")
    if server_errors:
        raise AssertionError("test daemon failed") from server_errors[0]


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


def test_health_and_connections_include_sync_events_held_for_other_identities(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        current = _save_connected_server_connection(store)
        store.enqueue_sync_event(
            "object_version",
            {"name": "foreign_obj", "owner_id": "owner-2"},
        )

        app = create_app(store)
        health_status, health = _json_from_app(app, "/health")
        connections_status, connections = _json_from_app(app, "/server/connections")

        assert health_status == 200
        assert health["server"]["connection_summary"]["held_sync_events"] == 1
        assert health["server"]["connection_summary"]["held_sync_event_owner_ids"] == ["owner-2"]

        assert connections_status == 200
        current_row = next(row for row in connections if row["id"] == current["id"])
        assert current_row["held_sync_events"] == 1
        assert current_row["pending_sync_events"] == 0
    finally:
        _shutdown_app(app)
        store.close()


def test_health_reports_server_origin_interpreter_substitutions(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        env = store.register_env("spl_core", sys.executable)
        record = store.register_object(
            "demo_obj",
            "demo_obj",
            "spl_core",
            yaml_text=REMOTE_FUNCTION_YAML,
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )
        author_python = str(tmp_path / "author-python")
        with store._lock, store._conn:  # noqa: SLF001 - regression seeds server provenance.
            store._conn.execute(
                "UPDATE object_versions SET env_python = ? WHERE id = ?",
                (author_python, record["version_id"]),
            )

        app = create_app(store)
        status, health = _json_from_app(app, "/health")

        assert status == 200
        substitutions = health["interpreter_substitutions"]
        assert substitutions["count"] == 1
        assert substitutions["items"][0]["object"] == "demo_obj"
        assert substitutions["items"][0]["authored_python"] == author_python
        assert substitutions["items"][0]["resolved_python"] == env["python"]
        assert substitutions["items"][0]["reason"] == "local_env"
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
        assert body["call"]["example"].startswith('result = client.call("demo_pipeline", ')
        assert 'function="inner_add"' in body["call"]["example"]
        assert [item["name"] for item in body["inputs"]] == ["a", "b"]
        assert inline_status == 200
        assert inline_body["call"]["example"] == body["call"]["example"]
    finally:
        _shutdown_app(app)
        store.close()


def test_signature_prefers_local_object_over_same_name_server_mirror(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        local = store.register_object(
            "order_pipeline",
            "demo_pipeline",
            "default",
            yaml_text=PIPELINE_WITH_INTERNAL_FUNCTION_YAML,
        )
        store.register_object(
            "order_pipeline",
            "demo_pipeline",
            "default",
            yaml_text=PIPELINE_WITH_INTERNAL_FUNCTION_YAML.replace(
                "return a + b",
                "return a - b",
            ),
            owner_id="owner-1",
            library="default",
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id="remote-pipeline-object",
            remote_version_id="remote-pipeline-version-1",
            source_object_name="order_pipeline",
        )

        app = create_app(store)
        status, body = _json_from_app(app, "/objects/order_pipeline/signature")

        assert status == 200
        assert body["id"] == local["id"]
        assert body["origin"] == "local"
        assert body["name"] == "order_pipeline"
        assert body["kind"] == "pipeline"
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
        _mark_current_server_channel_live(app.runtime)
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
        _mark_current_server_channel_live(app.runtime)
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


def test_pull_server_object_route_returns_pinned_receipt(tmp_path, monkeypatch) -> None:
    class PullServerClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def list_objects(self, *, owner_id=None, library=None, compact=False):
            assert owner_id is None
            assert library is None
            assert compact is True
            return [
                {
                    "name": "demo_obj",
                    "library": "default",
                    "owner_id": "owner-1",
                    "current_version": {"version": 1},
                }
            ]

        def get_object(self, name_or_id, *, version=None, include_yaml=False, owner_id=None, library=None):
            assert name_or_id == "demo_obj"
            assert owner_id == "owner-1"
            assert library == "default"
            if include_yaml:
                assert version == 1
            return {
                "id": "remote-object-1",
                "owner_id": "owner-1",
                "library": "default",
                "name": "demo_obj",
                "version": version or 1,
                "version_id": "remote-version-1",
                "entrypoint": "demo_obj",
                "env": "default",
                "description": "remote demo",
                "version_label": "v1",
                **({"yaml": REMOTE_FUNCTION_YAML} if include_yaml else {}),
            }

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", PullServerClient)
    app = None
    try:
        store.register_env("default", sys.executable)
        _save_connected_server_connection(store)
        app = create_app(store)
        _mark_current_server_channel_live(app.runtime)

        status, body = _post_json_from_app(
            app,
            "/server-objects/pull",
            {"name": "demo_obj"},
        )

        assert status == 200
        assert body == {
            "pulled": ["owner-1/default/demo_obj@v1"],
            "skipped": [],
            "failed": [],
            "ambiguous_names": [],
        }
    finally:
        _shutdown_app(app)
        store.close()


def test_pull_server_object_route_dry_run_does_not_write_local_db(tmp_path, monkeypatch) -> None:
    class DryRunServerClient:
        def __init__(self, *args, **kwargs) -> None:
            self.get_object_calls: list[dict[str, Any]] = []

        def list_objects(self, *, owner_id=None, library=None, compact=False):
            return [
                {
                    "name": "demo_obj",
                    "library": "default",
                    "owner_id": "owner-1",
                    "current_version": {"version": 1},
                }
            ]

        def get_object(self, name_or_id, *, version=None, include_yaml=False, owner_id=None, library=None):
            self.get_object_calls.append(
                {
                    "name_or_id": name_or_id,
                    "version": version,
                    "include_yaml": include_yaml,
                    "owner_id": owner_id,
                    "library": library,
                }
            )
            assert include_yaml is False
            return {
                "id": "remote-object-1",
                "owner_id": "owner-1",
                "library": "default",
                "name": "demo_obj",
                "version": 1,
                "version_id": "remote-version-1",
                "entrypoint": "demo_obj",
                "env": "default",
            }

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", DryRunServerClient)
    app = None
    try:
        _save_connected_server_connection(store)
        app = create_app(store)
        _mark_current_server_channel_live(app.runtime)
        before = store.list_object_identities()

        status, body = _post_json_from_app(
            app,
            "/server-objects/pull",
            {"name": "demo_obj", "dry_run": True},
        )

        assert status == 200
        assert body == {
            "pulled": ["owner-1/default/demo_obj@v1"],
            "skipped": [],
            "failed": [],
            "ambiguous_names": [],
        }
        assert store.list_object_identities() == before == []
    finally:
        _shutdown_app(app)
        store.close()


def _remote_catalog_record(
    name: str,
    *,
    version: int,
    value: int,
    owner_id: str = "owner-1",
    library: str = "default",
    object_id: str | None = None,
    version_id: str | None = None,
) -> dict[str, Any]:
    resolved_object_id = object_id or f"remote-object-{name}"
    return {
        "id": resolved_object_id,
        "owner_id": owner_id,
        "library": library,
        "name": name,
        "version": version,
        "version_id": version_id or f"{resolved_object_id}-version-{version}",
        "entrypoint": name,
        "env": "default",
        "description": f"{name} v{version}",
        "version_label": f"v{version}",
        "yaml": _remote_function_yaml(name, value),
        "current_version": {"version": version},
    }


class _BatchServerClient:
    versions: list[dict[str, Any]] = []
    fail_on_get_names: set[str] = set()
    list_objects_calls: list[dict[str, Any]] = []
    get_object_calls: list[dict[str, Any]] = []
    list_object_versions_calls: list[dict[str, Any]] = []

    def __init__(self, *args, **kwargs) -> None:
        pass

    @classmethod
    def reset(cls, versions: list[dict[str, Any]], *, fail_on_get_names: set[str] | None = None) -> None:
        cls.versions = versions
        cls.fail_on_get_names = fail_on_get_names or set()
        cls.list_objects_calls = []
        cls.get_object_calls = []
        cls.list_object_versions_calls = []

    @staticmethod
    def _public_record(record: dict[str, Any], *, include_yaml: bool) -> dict[str, Any]:
        payload = dict(record)
        if not include_yaml:
            payload.pop("yaml", None)
        return payload

    @classmethod
    def _matches(
        cls,
        record: dict[str, Any],
        name_or_id: str,
        *,
        owner_id: str | None,
        library: str | None,
    ) -> bool:
        if name_or_id not in {record.get("name"), record.get("id")}:
            return False
        if owner_id is not None and record.get("owner_id") != owner_id:
            return False
        if library is not None and record.get("library") != library:
            return False
        return True

    @classmethod
    def _matching_versions(
        cls,
        name_or_id: str,
        *,
        owner_id: str | None,
        library: str | None,
    ) -> list[dict[str, Any]]:
        return [
            record for record in cls.versions if cls._matches(record, name_or_id, owner_id=owner_id, library=library)
        ]

    @classmethod
    def _latest_catalog_records(cls) -> list[dict[str, Any]]:
        latest: dict[tuple[str, str, str], dict[str, Any]] = {}
        for record in cls.versions:
            key = (
                str(record.get("owner_id") or ""),
                str(record.get("library") or ""),
                str(record.get("name") or ""),
            )
            if key not in latest or int(record.get("version") or 0) > int(latest[key].get("version") or 0):
                latest[key] = record
        return list(latest.values())

    def list_objects(self, *, owner_id=None, library=None, compact=False):
        type(self).list_objects_calls.append({"owner_id": owner_id, "library": library, "compact": compact})
        return [
            self._public_record(record, include_yaml=False)
            for record in type(self)._latest_catalog_records()
            if (owner_id is None or record.get("owner_id") == owner_id)
            and (library is None or record.get("library") == library)
        ]

    def get_object(self, name_or_id, *, version=None, include_yaml=False, owner_id=None, library=None):
        type(self).get_object_calls.append(
            {
                "name_or_id": name_or_id,
                "version": version,
                "include_yaml": include_yaml,
                "owner_id": owner_id,
                "library": library,
            }
        )
        if name_or_id in type(self).fail_on_get_names:
            raise KeyError(f"server object is not registered: {name_or_id}")
        matches = type(self)._matching_versions(name_or_id, owner_id=owner_id, library=library)
        if version is not None:
            matches = [record for record in matches if int(record.get("version") or 0) == int(version)]
        if not matches:
            raise KeyError(f"server object is not registered: {name_or_id}")
        return self._public_record(
            max(matches, key=lambda item: int(item.get("version") or 0)), include_yaml=include_yaml
        )

    def list_object_versions(self, name_or_id, *, include_yaml=False, owner_id=None, library=None):
        type(self).list_object_versions_calls.append(
            {
                "name_or_id": name_or_id,
                "include_yaml": include_yaml,
                "owner_id": owner_id,
                "library": library,
            }
        )
        return [
            self._public_record(record, include_yaml=include_yaml)
            for record in sorted(
                type(self)._matching_versions(name_or_id, owner_id=owner_id, library=library),
                key=lambda item: int(item.get("version") or 0),
                reverse=True,
            )
        ]


def test_pull_all_client_mirrors_filtered_library_and_repeat_skips(tmp_path, monkeypatch) -> None:
    _BatchServerClient.reset(
        [
            _remote_catalog_record("risk_a", version=1, value=1, library="risk"),
            _remote_catalog_record("risk_b", version=2, value=2, library="risk"),
            _remote_catalog_record("ops_a", version=1, value=3, library="ops"),
        ]
    )
    monkeypatch.setattr(daemon_server, "ServerClient", _BatchServerClient)
    store = RegistryStore(tmp_path)
    app = None
    stop_server: threading.Event | None = None
    server_thread: threading.Thread | None = None
    reserved, port = _reserved_port_with_free_next()
    reserved.close()
    try:
        store.register_env("default", sys.executable)
        _save_connected_server_connection(store)
        app = create_app(store)
        _mark_current_server_channel_live(app.runtime)
        stop_server, server_thread, server_errors = _serve_app_in_thread(app, port)
        client = Client(f"http://127.0.0.1:{port}", api_token=app.api_token)

        receipt = client.pull_all_server_objects(library="risk", progress=False)
        repeat = client.pull_all_server_objects(library="risk", progress=False)

        assert receipt["objects_seen"] == 2
        assert sorted(receipt["pulled"]) == ["owner-1/risk/risk_a@v1", "owner-1/risk/risk_b@v2"]
        assert receipt["skipped"] == []
        assert receipt["failed"] == []
        assert repeat["pulled"] == []
        assert sorted(repeat["skipped"]) == ["owner-1/risk/risk_a@v1", "owner-1/risk/risk_b@v2"]
        assert store.get_object("risk_a", owner_id="owner-1", library="risk")["origin"] == "server"
        assert store.get_object("risk_b", owner_id="owner-1", library="risk")["origin"] == "server"
        with pytest.raises(KeyError):
            store.get_object("ops_a", owner_id="owner-1", library="ops")
        assert _BatchServerClient.list_objects_calls == [
            {"owner_id": None, "library": "risk", "compact": True},
            {"owner_id": None, "library": "risk", "compact": True},
        ]
        assert not server_errors
    finally:
        if stop_server is not None:
            stop_server.set()
        if server_thread is not None:
            server_thread.join(timeout=5.0)
        _shutdown_app(app)
        store.close()


def test_pull_all_client_dry_run_keeps_local_catalog_unchanged(tmp_path, monkeypatch) -> None:
    _BatchServerClient.reset([_remote_catalog_record("dry_demo", version=5, value=5, library="risk")])
    monkeypatch.setattr(daemon_server, "ServerClient", _BatchServerClient)
    store = RegistryStore(tmp_path)
    app = None
    stop_server: threading.Event | None = None
    server_thread: threading.Thread | None = None
    reserved, port = _reserved_port_with_free_next()
    reserved.close()
    try:
        _save_connected_server_connection(store)
        app = create_app(store)
        _mark_current_server_channel_live(app.runtime)
        stop_server, server_thread, server_errors = _serve_app_in_thread(app, port)
        client = Client(f"http://127.0.0.1:{port}", api_token=app.api_token)
        before = store.list_object_identities()

        receipt = client.pull_all_server_objects(library="risk", dry_run=True, progress=False)

        assert receipt == {
            "objects_seen": 1,
            "pulled": ["owner-1/risk/dry_demo@v5"],
            "skipped": [],
            "failed": [],
            "ambiguous_names": [],
        }
        assert store.list_object_identities() == before == []
        assert all(not call["include_yaml"] for call in _BatchServerClient.get_object_calls)
        assert not server_errors
    finally:
        if stop_server is not None:
            stop_server.set()
        if server_thread is not None:
            server_thread.join(timeout=5.0)
        _shutdown_app(app)
        store.close()


def test_pull_all_client_keeps_partial_imports_when_one_object_fails(tmp_path, monkeypatch) -> None:
    _BatchServerClient.reset(
        [
            _remote_catalog_record("ok_a", version=1, value=1, library="risk"),
            _remote_catalog_record("broken", version=1, value=2, library="risk"),
            _remote_catalog_record("ok_b", version=1, value=3, library="risk"),
        ],
        fail_on_get_names={"broken"},
    )
    monkeypatch.setattr(daemon_server, "ServerClient", _BatchServerClient)
    store = RegistryStore(tmp_path)
    app = None
    stop_server: threading.Event | None = None
    server_thread: threading.Thread | None = None
    reserved, port = _reserved_port_with_free_next()
    reserved.close()
    try:
        store.register_env("default", sys.executable)
        _save_connected_server_connection(store)
        app = create_app(store)
        _mark_current_server_channel_live(app.runtime)
        stop_server, server_thread, server_errors = _serve_app_in_thread(app, port)
        client = Client(f"http://127.0.0.1:{port}", api_token=app.api_token)

        receipt = client.pull_all_server_objects(library="risk", progress=False)

        assert receipt["objects_seen"] == 3
        assert sorted(receipt["pulled"]) == ["owner-1/risk/ok_a@v1", "owner-1/risk/ok_b@v1"]
        assert receipt["skipped"] == []
        assert len(receipt["failed"]) == 1
        assert receipt["failed"][0]["ref"] == "owner-1/risk/broken@v1"
        assert "server object is not registered: broken" in receipt["failed"][0]["reason"]
        assert store.get_object("ok_a", owner_id="owner-1", library="risk")["origin"] == "server"
        assert store.get_object("ok_b", owner_id="owner-1", library="risk")["origin"] == "server"
        with pytest.raises(KeyError):
            store.get_object("broken", owner_id="owner-1", library="risk")
        assert not server_errors
    finally:
        if stop_server is not None:
            stop_server.set()
        if server_thread is not None:
            server_thread.join(timeout=5.0)
        _shutdown_app(app)
        store.close()


def test_pulled_mirror_survives_offline_restart_for_bare_lookup_and_call(tmp_path, monkeypatch) -> None:
    _BatchServerClient.reset([_remote_catalog_record("clean_amount", version=1, value=42)])
    monkeypatch.setattr(daemon_server, "ServerClient", _BatchServerClient)
    store = RegistryStore(tmp_path)
    app = None
    stop_server: threading.Event | None = None
    server_thread: threading.Thread | None = None
    reserved, port = _reserved_port_with_free_next()
    reserved.close()
    try:
        store.register_env("default", sys.executable)
        connection = _save_connected_server_connection(store)
        app = create_app(store, auto_build_envs=False)
        _mark_current_server_channel_live(app.runtime)

        receipt = app.runtime.pull_server_object("clean_amount")
        assert receipt["pulled"] == ["owner-1/default/clean_amount@v1"]
        assert store.get_object("clean_amount", owner_id="owner-1", library="default")["origin"] == "server"
        assert store.list_pending_sync_events() == []

        _drop_remote_connection_lease(store, connection["id"])
        spam = store.save_pending_server_connection(
            server_url="https://splime.io/api",
            token="spam-machine-token",
            user_token="spam-user-token",
            machine_id="machine-spam",
        )
        with store._lock, store._conn:  # noqa: SLF001 - legacy H-02 ownerless ACTIVE spam.
            store._conn.execute(
                """
                UPDATE server_connections
                SET status = 'connect_failed',
                    updated_at = '2999-01-01T00:00:00+00:00',
                    error = 'legacy ownerless connect spam'
                WHERE id = ?
                """,
                (spam["id"],),
            )
        _shutdown_app(app)
        app = None
        store.close()

        store = RegistryStore(tmp_path)
        credentials = store.current_server_connection_credentials()
        assert credentials is not None
        assert credentials["id"] == connection["id"]
        assert credentials["owner_id"] == "owner-1"
        assert credentials["remote_connection_id"] is None
        assert credentials["status"] == "connect_failed"

        app = create_app(store, auto_build_envs=False)
        stop_server, server_thread, server_errors = _serve_app_in_thread(app, port)
        client = SPLClient(base_url=f"http://127.0.0.1:{port}", api_token=app.api_token)

        listed = client.objects(scope="local")
        signature = client.signature("clean_amount")
        scoped_signature = client.signature("clean_amount", owner="owner-1", library="default")
        description = client.describe("clean_amount")
        result = client.call("clean_amount", progress=False)

        assert listed["clean_amount"]["owner_id"] == "owner-1"
        assert store.get_object("clean_amount")["canonical_name"] == "owner-1/default/clean_amount"
        assert signature["name"] == "clean_amount"
        assert scoped_signature["name"] == "clean_amount"
        assert "clean_amount" in description
        assert result.mode == "local"
        assert result.output == 42
        assert not server_errors
    finally:
        if stop_server is not None:
            stop_server.set()
        if server_thread is not None:
            server_thread.join(timeout=5.0)
        _shutdown_app(app)
        store.close()


def test_local_scoped_run_resolves_cross_owner_object_offline(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "clean_amount",
            "demo_obj",
            "default",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-a",
            library="default",
        )
        app = create_app(store, auto_build_envs=False)

        status, missing = _json_from_app(app, "/objects/clean_amount/signature")
        assert status == 404
        assert "owner 'owner-a'" in missing["error"]
        assert "library 'default'" in missing["error"]

        status, scoped_signature = _json_from_app(
            app,
            "/objects/clean_amount/signature?owner_id=owner-a&library=default",
        )
        assert status == 200
        assert scoped_signature["name"] == "clean_amount"

        status, started = _post_json_from_app(
            app,
            "/runs",
            {
                "object": "clean_amount",
                "object_owner_id": "owner-a",
                "library": "default",
                "source": "local",
                "remote": False,
                "keep": True,
            },
        )
        assert status == 202
        final = _wait_store_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["result"]["result"] == 1
    finally:
        _shutdown_app(app)
        store.close()


def test_server_machines_use_machine_token_aliases(tmp_path, monkeypatch) -> None:
    class MachineServerClient:
        def __init__(self, base_url, machine_token, *, user_token=None, request_timeout_seconds=None) -> None:
            assert base_url == "https://splime.io/api"
            assert machine_token == "machine-token-123456"
            assert user_token == "user-token-123456"

        def list_machines(self):
            return [
                {
                    "id": "machine-86c8b6063d0bef7b",
                    "display_name": "machine-86c8b6063d0bef7b",
                    "status": "offline",
                },
                {
                    "id": "machine-f82b6486f6595e39",
                    "display_name": "machine-f82b6486f6595e39",
                    "status": "online",
                },
                {
                    "id": "machine-custom",
                    "display_name": "Already Custom",
                    "status": "online",
                },
            ]

        def list_tokens(self):
            return [
                {
                    "subject_type": "machine",
                    "subject_id": "machine-86c8b6063d0bef7b",
                    "name": "Machine credential for Pair3",
                    "status": "active",
                },
                {
                    "subject_type": "machine",
                    "subject_id": "machine-f82b6486f6595e39",
                    "name": "Machine credential for MBP16_N+1",
                    "status": "active",
                },
            ]

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", MachineServerClient)
    app = None
    try:
        _save_connected_server_connection(store)
        app = create_app(store)
        _mark_current_server_channel_live(app.runtime)

        status, body = _json_from_app(app, "/server/machines")

        machines = {machine["id"]: machine for machine in body["machines"]}
        assert status == 200
        assert machines["machine-86c8b6063d0bef7b"]["display_name"] == "Pair3"
        assert machines["machine-86c8b6063d0bef7b"]["stored_display_name"] == "machine-86c8b6063d0bef7b"
        assert machines["machine-f82b6486f6595e39"]["display_name"] == "MBP16_N+1"
        assert machines["machine-custom"]["display_name"] == "Already Custom"
    finally:
        _shutdown_app(app)
        store.close()


def test_server_libraries_are_managed_through_daemon_proxy(
    tmp_path,
    monkeypatch,
) -> None:
    class LibraryServerClient:
        calls = []

        def __init__(self, base_url, machine_token, *, user_token=None, request_timeout_seconds=None) -> None:
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
        _save_connected_server_connection(store)
        app = create_app(store)
        _mark_current_server_channel_live(app.runtime)

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
        assert status == 501
        assert "not supported" in body["error"]

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


def test_local_mutation_routes_do_not_wait_for_unreachable_server(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        _save_connected_server_connection(store, server_url=_closed_local_server_url())
        app = create_app(store, auto_build_envs=False)

        started = time.monotonic()
        env_status, env_body = _post_json_from_app(
            app,
            "/envs",
            {"name": "default", "python": sys.executable},
        )
        env_elapsed = time.monotonic() - started

        started = time.monotonic()
        publish_status, publish_body = _post_json_from_app(
            app,
            "/objects",
            {
                "name": "demo_obj",
                "entrypoint": "demo_obj",
                "env": "default",
                "yaml": REMOTE_FUNCTION_YAML,
            },
        )
        publish_elapsed = time.monotonic() - started
        stored_origin = store.get_object("demo_obj", owner_id="owner-1", library="default")["origin"]

        started = time.monotonic()
        forget_status, forget_body = _delete_json_from_app(app, "/objects/demo_obj")
        forget_elapsed = time.monotonic() - started

        assert env_status == 201
        assert env_body["name"] == "default"
        assert publish_status == 201
        assert publish_body["owner_id"] == "owner-1"
        assert publish_body["sync"]["connected"] is False
        assert publish_body["sync"]["offline"] is True
        assert publish_body["sync"]["code"] == "central_server_unreachable"
        assert publish_body["sync_event"]["status"] == "pending"
        assert stored_origin == "local"
        assert forget_status == 200
        assert forget_body["object_deleted"] is True
        assert env_elapsed < 2.0
        assert publish_elapsed < 2.0
        assert forget_elapsed < 2.0
    finally:
        _shutdown_app(app)
        store.close()


def test_server_proxy_fails_fast_when_stored_connection_is_not_live(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        _save_connected_server_connection(store, server_url=_closed_local_server_url())
        app = create_app(store, auto_build_envs=False)

        started = time.monotonic()
        status, body = _json_from_app(app, "/server/objects")
        elapsed = time.monotonic() - started

        assert status == 503
        assert "central SPL daemon server is offline or unreachable" in body["error"]
        assert body["offline"] is True
        assert body["code"] == "central_server_unreachable"
        assert elapsed < 2.0
    finally:
        _shutdown_app(app)
        store.close()


def test_blocking_heartbeat_does_not_block_local_object_registration(tmp_path, monkeypatch) -> None:
    class BlockingSyncServerClient:
        started = threading.Event()
        release = threading.Event()

        def __init__(self, *args, **kwargs) -> None:
            pass

        def latest_machine_library_snapshot(self, machine_id):
            return {}

        def sync(self, **kwargs):
            type(self).started.set()
            type(self).release.wait(timeout=5)
            raise RuntimeError("heartbeat is still offline")

    monkeypatch.setattr(daemon_server, "ServerClient", BlockingSyncServerClient)
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        _save_connected_server_connection(store)
        app = create_app(store, auto_build_envs=False)
        assert BlockingSyncServerClient.started.wait(timeout=2)

        started = time.monotonic()
        status, body = _post_json_from_app(
            app,
            "/objects",
            {
                "name": "demo_obj",
                "entrypoint": "demo_obj",
                "env": "default",
                "yaml": REMOTE_FUNCTION_YAML,
            },
        )
        elapsed = time.monotonic() - started

        assert status == 201
        assert body["owner_id"] == "owner-1"
        assert elapsed < 2.0
    finally:
        BlockingSyncServerClient.release.set()
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


def test_run_management_routes_list_show_prune_and_delete(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        store.register_object("demo_obj", "demo_obj", "default", yaml_text=REMOTE_FUNCTION_YAML)
        run = store.create_run("demo_obj", keep=True)
        manifest = dict(run["manifest"])
        manifest["nodes"] = {
            "node-1": {
                "id": "node-1",
                "alias": "producer",
                "status": "succeeded",
                "runtime": {
                    "name": "venv-subprocess",
                    "source": "node-tag",
                    "config_hash": "abc123",
                    "resolved": {"python": "/venv/bin/python"},
                },
                "outputs": {
                    "default": {
                        "kind": "json",
                        "tag": "json",
                        "value": {"secret": "route-default-preview"},
                        "sha256": "0" * 64,
                    }
                },
            }
        }
        adapter = {
            "identity": {
                "key": JSON_ADAPTER_KEY,
                "tag": "json",
                "accepted_tags": ["json"],
                "save": "spl.core.entities.adapter._json_save",
                "load": "spl.core.entities.adapter._json_load",
                "distributions": [],
            },
            "tag": "json",
            "accepted_tags": ["json"],
            "source": "port-default",
        }
        manifest["nodes"]["node-2"] = {
            "id": "node-2",
            "alias": "consumer",
            "status": "succeeded",
            "runtime": {
                "name": "native",
                "source": "default",
                "config_hash": None,
                "resolved": {"python": sys.executable},
            },
            "inputs": {},
            "outputs": {},
        }
        manifest["edges"] = [
            {
                "source": {"node_id": "node-1", "port": "default"},
                "target": {"node_id": "node-2", "port": "value"},
                "artifact": {"kind": "json", "tag": "json", "sha256": "0" * 64},
                "adapter": {"save": adapter, "load": adapter},
            }
        ]
        store.update_run(run["id"], status="failed", manifest=manifest)
        app = create_app(store)

        status, listed = _json_from_app(app, "/runs")
        assert status == 200
        assert listed[0]["has_manifest"] is True
        assert listed[0]["disk_size_bytes"] > 0
        assert listed[0]["node_runtimes"] == [
            {
                "node_id": "node-2",
                "alias": "consumer",
                "name": "native",
                "source": "default",
                "config_hash": None,
                "resolved": {"python": sys.executable},
            },
            {
                "node_id": "node-1",
                "alias": "producer",
                "name": "venv-subprocess",
                "source": "node-tag",
                "config_hash": "abc123",
                "resolved": {"python": "/venv/bin/python"},
            },
        ]
        assert listed[0]["edge_adapters"] == [
            {
                "source": "producer.default",
                "target": "consumer.value",
                "source_node_id": "node-1",
                "source_port": "default",
                "target_node_id": "node-2",
                "target_port": "value",
                "tag": "json",
                "save": "json",
                "load": "json",
                "source_level": "port-default",
            }
        ]

        status, observed = _json_from_app(app, f"/runs/{run['id']}")
        assert status == 200
        assert "route-default-preview" not in json.dumps(observed, sort_keys=True)
        assert observed["run_progress"]["node_runtimes"] == listed[0]["node_runtimes"]
        assert observed["run_progress"]["edge_adapters"] == listed[0]["edge_adapters"]

        status, shown = _json_from_app(app, f"/runs/{run['id']}?view=show")
        assert status == 200
        assert shown["edge_adapters"] == listed[0]["edge_adapters"]
        output = shown["manifest"]["nodes"]["node-1"]["outputs"]["default"]
        assert output["value_omitted"] is True
        assert "value" not in output
        assert output["value_preview"] == "<omitted>"
        assert "route-default-preview" not in json.dumps(shown, sort_keys=True)

        status, shown_full = _json_from_app(app, f"/runs/{run['id']}?view=show&full_inline=1")
        assert status == 200
        assert shown_full["manifest"]["nodes"]["node-1"]["outputs"]["default"]["value"] == {
            "secret": "route-default-preview"
        }

        status, tag_stats = _json_from_app(app, "/runs/tag-stats")
        assert status == 200
        assert tag_stats == {
            "runs_scanned": 1,
            "edges_scanned": 1,
            "tags": [{"tag": "json", "edge_count": 1, "run_count": 1}],
            "pairs": [{"save_tag": "json", "load_tags": ["json"], "edge_count": 1, "run_count": 1}],
        }

        status, preview = _post_json_from_app(app, "/runs/prune", {"statuses": ["failed"], "dry_run": True})
        assert status == 200
        assert preview["candidates"][0]["id"] == run["id"]
        assert store.get_run(run["id"])["status"] == "failed"

        status, deleted = _delete_json_from_app(app, f"/runs/{run['id']}")
        assert status == 200
        assert deleted["pruned"][0]["id"] == run["id"]
        assert not Path(run["run_dir"]).exists()
    finally:
        _shutdown_app(app)
        store.close()


def test_daemon_kept_state_does_not_log_sensitive_inline_values(
    tmp_path,
    caplog,
) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        caplog.set_level(logging.INFO, logger="spl.daemon.server")
        store.register_env("default", sys.executable)
        store.register_object("secret_pipeline", "secret_pipeline", "default", yaml_text=SECRET_PIPELINE_YAML)
        app = create_app(store, auto_build_envs=False)

        run = app.runtime.start_run(
            "secret_pipeline",
            kwargs={"password": "hunter2"},
            output="login",
            keep=True,
            source="local",
        )
        state = _wait_store_run(store, run["id"])

        assert state["status"] == "succeeded"
        assert "hunter2" not in caplog.text
        assert "hunter2" not in str(state.get("stdout") or "")
        assert "hunter2" not in str(state.get("stderr") or "")

        status, observed = _json_from_app(app, f"/runs/{run['id']}")
        assert status == 200
        rendered = json.dumps(observed, sort_keys=True)
        assert "hunter2" not in rendered
        assert '"value_preview": "<omitted>"' in rendered

        status, shown = _json_from_app(app, f"/runs/{run['id']}?view=show")
        assert status == 200
        assert "hunter2" not in json.dumps(shown, sort_keys=True)

        status, shown_full = _json_from_app(app, f"/runs/{run['id']}?view=show&full_inline=1")
        assert status == 200
        assert "hunter2" in json.dumps(shown_full, sort_keys=True)
    finally:
        _shutdown_app(app)
        store.close()


def test_daemon_pipeline_venv_subprocess_uses_ir_source_for_yaml_functions(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "daemon_venv_pipeline",
            "daemon_venv_pipeline",
            "default",
            yaml_text=VENV_SUBPROCESS_PIPELINE_YAML,
        )
        app = create_app(store, auto_build_envs=False)

        status, started = _post_json_from_app(
            app,
            "/runs",
            {
                "object": "daemon_venv_pipeline",
                "output": "consumer",
                "source": "local",
                "keep": True,
            },
        )
        assert status == 202
        final = _wait_store_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["result"]["result"] == {"default": "SEED"}
        consumer = _manifest_node_by_alias(final["manifest"], "consumer")
        assert consumer["runtime"]["name"] == "venv-subprocess"
        assert consumer["runtime"]["source"] == "node-tag"

        status, observed = _json_from_app(app, f"/runs/{started['id']}")
        assert status == 200
        assert {
            "alias": "consumer",
            "node_id": "55555555-5555-4555-8555-555555555555",
            "name": "venv-subprocess",
            "source": "node-tag",
            "config_hash": consumer["runtime"]["config_hash"],
            "resolved": consumer["runtime"]["resolved"],
        } in observed["run_progress"]["node_runtimes"]
    finally:
        _shutdown_app(app)
        store.close()


def test_daemon_pipeline_resume_via_http_reuses_core_semantics(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        store.register_object("resumable_pipeline", "resumable_pipeline", "default", yaml_text=RESUMABLE_PIPELINE_YAML)
        app = create_app(store, auto_build_envs=False)
        counter_path = tmp_path / "producer-count.txt"

        initial = app.runtime.start_run(
            "resumable_pipeline",
            kwargs={"counter_path": str(counter_path)},
            output="consumer",
            keep=True,
            source="local",
        )
        failed = _wait_store_run(store, initial["id"])

        assert failed["status"] == "failed"
        assert counter_path.read_text(encoding="utf-8") == "1"
        assert _manifest_node_by_alias(failed["manifest"], "producer")["status"] == "succeeded"
        assert _manifest_node_by_alias(failed["manifest"], "consumer")["status"] == "failed"

        override = {
            "producer.default": {
                "key": "builtins.str@txt",
                "save": "save_text",
                "load": "good_load_text",
                "distributions": [],
            }
        }
        status, resumed = _post_json_from_app(
            app,
            f"/runs/{initial['id']}/resume",
            {"from": "consumer", "adapters": override},
        )
        assert status == 202
        assert resumed["parent_run_id"] == initial["id"]
        child = _wait_store_run(store, resumed["id"])

        assert child["status"] == "succeeded"
        assert child["result"]["result"] == {"default": "consumed:seed"}
        assert counter_path.read_text(encoding="utf-8") == "1"
        assert child["manifest"]["parent_run_id"] == initial["id"]
        assert _manifest_node_by_alias(child["manifest"], "producer")["status"] == "frozen"
        assert _manifest_node_by_alias(child["manifest"], "consumer")["status"] == "succeeded"

        status, observed = _json_from_app(app, f"/runs/{child['id']}")
        assert status == 200
        assert {"alias": "producer", "node_id": "11111111-1111-4111-8111-111111111111", "status": "frozen"} in observed[
            "run_progress"
        ]["nodes"]
        assert {
            "alias": "consumer",
            "node_id": "22222222-2222-4222-8222-222222222222",
            "status": "succeeded",
        } in observed["run_progress"]["nodes"]

        status, _ = _post_json_from_app(app, "/runs/missing-run/resume", {"from": "consumer"})
        assert status == 404

        corrupt_parent = app.runtime.start_run(
            "resumable_pipeline",
            kwargs={"counter_path": str(tmp_path / "corrupt-count.txt")},
            output="consumer",
            keep=True,
            source="local",
        )
        corrupt_failed = _wait_store_run(store, corrupt_parent["id"])
        manifest_dir = _worker_manifest_dir(corrupt_failed["run_dir"])
        producer = _manifest_node_by_alias(corrupt_failed["manifest"], "producer")
        artifact_path = manifest_dir / producer["outputs"]["default"]["ref"]["uri"]
        artifact_path.write_text("broken", encoding="utf-8")

        status, body = _post_json_from_app(
            app,
            f"/runs/{corrupt_parent['id']}/resume",
            {"from": "consumer", "adapters": override},
        )
        assert status == 409
        assert "sha256 mismatch" in body["error"]

        status, first = _post_json_from_app(
            app, f"/runs/{initial['id']}/resume", {"from": "consumer", "adapters": override}
        )
        assert status == 202
        status, second = _post_json_from_app(
            app,
            f"/runs/{initial['id']}/resume",
            {"from": "consumer", "adapters": override},
        )
        assert status == 202
        assert first["id"] != second["id"]
        _wait_store_run(store, first["id"])
        _wait_store_run(store, second["id"])
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
        _mark_current_server_channel_live(app.runtime)
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
        assert body["decomposition"]["nodes"] == [{"node_id": "node-1", "kind": "remote"}]
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
