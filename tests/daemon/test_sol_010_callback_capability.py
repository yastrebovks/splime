from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import time
from types import SimpleNamespace
from typing import Any

import pytest

from spl.daemon.callback_capability import (
    CALLBACK_CAPABILITY_ENV,
    CALLBACK_CAPABILITY_PREFIX,
    CALLBACK_CAPABILITY_TIMEOUT_GRACE_SECONDS,
    DEFAULT_CALLBACK_CAPABILITY_TTL_SECONDS,
    CallbackCapabilityAuthority,
    callback_capability_ttl_seconds,
)
from spl.daemon import worker
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore
from tests.daemon.test_daemon_endpoint import _serve_app_in_thread


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

ENV_FUNCTION_YAML = f"""\
- !DFunction
  name: read_callback_env
  inputs: []
  outputs:
  - name: default
    type: str
  body: |-
    import os
    return os.environ.get({CALLBACK_CAPABILITY_ENV!r})
"""

MASTER_ENV_FUNCTION_YAML = """\
- !DFunction
  name: read_master_env
  inputs: []
  outputs:
  - name: default
    type: str
  body: |-
    import os
    return os.environ.get("SPL_DAEMON_API_TOKEN")
"""

REMOTE_PIPELINE_YAML = """\
- !DPipeline
  name: remote_pipeline
  nodes:
  - !DNodeRemote
    uuid: 11111111-1111-4111-8111-111111111111
    url: https://splime.io/api
    name: remote_obj
    version: latest
  links: []
  aliases:
  - - remote
    - 11111111-1111-4111-8111-111111111111
"""

REMOTE_NODE = {
    "id": "11111111-1111-4111-8111-111111111111",
    "kind": "remote",
    "name": "remote_obj",
    "remote": {
        "url": "https://splime.io/api",
        "name": "remote_obj",
        "version": "latest",
        "owner_id": "owner-1",
        "library": "default",
        "target_machine": "machine-1",
    },
}


class _ReadyEnvironment:
    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        return self.ensure_ready(object_record)

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        _ = object_record, wait, retry_failed
        return {
            "spec_hash": "ready-environment",
            "python_path": sys.executable,
        }


def _free_local_port() -> int:
    reserved = socket.socket()
    try:
        reserved.bind(("127.0.0.1", 0))
        return int(reserved.getsockname()[1])
    finally:
        reserved.close()


def _wait_for_terminal(store: RegistryStore, run_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        state = store.get_run(run_id)
        if state["status"] in {"succeeded", "failed"}:
            return state
        time.sleep(0.05)
    raise TimeoutError(f"run did not reach terminal status: {run_id}")


def _running_run(store: RegistryStore) -> dict[str, Any]:
    store.register_env("default", sys.executable)
    store.register_object("demo_obj", "demo_obj", "default", yaml_text=FUNCTION_YAML)
    run = store.create_run("demo_obj", report_local_run=False, keep=True)
    store.update_run(run["id"], status="starting")
    return store.update_run(run["id"], status="running")


def _request_node() -> dict[str, Any]:
    remote = REMOTE_NODE["remote"]
    return {
        "uuid": REMOTE_NODE["id"],
        "url": remote["url"],
        "name": remote["name"],
        "version": remote["version"],
        "owner_id": remote["owner_id"],
        "library": remote["library"],
        "target_machine": remote["target_machine"],
    }


def test_callback_authority_is_hash_only_route_scoped_and_node_bound() -> None:
    statuses = {"run-1": "running"}
    authority = CallbackCapabilityAuthority(
        statuses.get,
        token_factory=lambda _: "deterministic-secret-material",
    )
    token = authority.mint("run-1", [REMOTE_NODE], ttl_seconds=60.0)

    assert token == CALLBACK_CAPABILITY_PREFIX + "deterministic-secret-material"
    assert token not in repr(authority.__dict__)
    assert authority.authenticate(token, method="GET", path="/remote-nodes/run") is None
    assert authority.authenticate(token, method="POST", path="/objects") is None

    principal = authority.authenticate(token, method="POST", path="/remote-nodes/run")

    assert principal is not None
    assert principal.run_id == "run-1"
    assert authority.authorizes_node(principal, _request_node()) is True


def test_callback_authority_prefers_graph_uuid_over_repository_row_id() -> None:
    authority = CallbackCapabilityAuthority(lambda _: "running")
    enriched_node = {
        **REMOTE_NODE,
        "id": "repository-row-id",
        "node_id": REMOTE_NODE["id"],
    }
    token = authority.mint("run-1", [enriched_node], ttl_seconds=60.0)
    principal = authority.authenticate(token, method="POST", path="/remote-nodes/run")

    assert principal is not None
    assert authority.authorizes_node(principal, _request_node()) is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("uuid", "22222222-2222-4222-8222-222222222222"),
        ("url", "https://attacker.invalid/api"),
        ("name", "different_obj"),
        ("version", 2),
        ("owner_id", "owner-2"),
        ("library", "risk"),
        ("target_machine", "machine-2"),
    ],
)
def test_callback_authority_rejects_each_tampered_node_identity(field: str, value: Any) -> None:
    authority = CallbackCapabilityAuthority(lambda _: "running")
    token = authority.mint("run-1", [REMOTE_NODE], ttl_seconds=60.0)
    principal = authority.authenticate(token, method="POST", path="/remote-nodes/run")
    assert principal is not None
    node = _request_node()
    node[field] = value

    assert authority.authorizes_node(principal, node) is False


@pytest.mark.parametrize("tampered_version", [True, 1.0])
def test_callback_version_binding_does_not_conflate_equal_python_number_types(
    tampered_version: Any,
) -> None:
    numeric_node = {
        **REMOTE_NODE,
        "remote": {**REMOTE_NODE["remote"], "version": 1},
    }
    authority = CallbackCapabilityAuthority(lambda _: "running")
    token = authority.mint("run-1", [numeric_node], ttl_seconds=60.0)
    principal = authority.authenticate(token, method="POST", path="/remote-nodes/run")
    assert principal is not None
    node = _request_node()
    node["version"] = 1
    assert authority.authorizes_node(principal, node) is True
    node["version"] = tampered_version

    assert authority.authorizes_node(principal, node) is False


def test_callback_optional_identity_fields_do_not_coerce_request_types() -> None:
    string_owner_node = {
        **REMOTE_NODE,
        "remote": {**REMOTE_NODE["remote"], "owner_id": "1"},
    }
    authority = CallbackCapabilityAuthority(lambda _: "running")
    token = authority.mint("run-1", [string_owner_node], ttl_seconds=60.0)
    principal = authority.authenticate(token, method="POST", path="/remote-nodes/run")
    assert principal is not None
    node = _request_node()
    node["owner_id"] = 1

    assert authority.authorizes_node(principal, node) is False


@pytest.mark.parametrize(
    "field",
    [
        "id",
        "node_id",
        "server_url",
        "object_name",
        "object",
        "function",
        "entrypoint",
        "owner",
        "library_slug",
        "version_id",
        "target_machine_id",
    ],
)
def test_callback_authority_rejects_identity_alias_smuggling(field: str) -> None:
    authority = CallbackCapabilityAuthority(lambda _: "running")
    token = authority.mint("run-1", [REMOTE_NODE], ttl_seconds=60.0)
    principal = authority.authenticate(token, method="POST", path="/remote-nodes/run")
    assert principal is not None
    node = _request_node()
    node[field] = "attacker-controlled"

    assert authority.authorizes_node(principal, node) is False


def test_callback_authority_expires_and_revokes_when_run_is_not_running() -> None:
    now = [100.0]
    statuses = {"run-1": "running"}
    authority = CallbackCapabilityAuthority(
        statuses.get,
        clock=lambda: now[0],
    )
    expired = authority.mint("run-1", [REMOTE_NODE], ttl_seconds=5.0)
    now[0] = 105.0
    assert authority.authenticate(expired, method="POST", path="/remote-nodes/run") is None

    active = authority.mint("run-1", [REMOTE_NODE], ttl_seconds=5.0)
    statuses["run-1"] = "succeeded"
    assert authority.authenticate(active, method="POST", path="/remote-nodes/run") is None
    assert authority._records == {}


def test_callback_capability_does_not_borrow_another_active_run() -> None:
    statuses = {"run-x": "running", "run-y": "running"}
    authority = CallbackCapabilityAuthority(statuses.get)
    token = authority.mint("run-x", [REMOTE_NODE], ttl_seconds=60.0)

    statuses["run-x"] = "failed"

    assert statuses["run-y"] == "running"
    assert authority.authenticate(token, method="POST", path="/remote-nodes/run") is None


def test_callback_capability_ttl_is_finite_and_tracks_explicit_timeout() -> None:
    assert callback_capability_ttl_seconds(None) == DEFAULT_CALLBACK_CAPABILITY_TTL_SECONDS
    assert callback_capability_ttl_seconds(0) == CALLBACK_CAPABILITY_TIMEOUT_GRACE_SECONDS
    assert callback_capability_ttl_seconds(12.5) == 12.5 + CALLBACK_CAPABILITY_TIMEOUT_GRACE_SECONDS
    for invalid_timeout in (-1.0, True, "12", float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match="finite non-negative"):
            callback_capability_ttl_seconds(invalid_timeout)  # type: ignore[arg-type]


def test_worker_consumes_callback_environment_before_importing_user_code(tmp_path: Any, monkeypatch: Any) -> None:
    secret = CALLBACK_CAPABILITY_PREFIX + "worker-only-secret"
    object_yaml = tmp_path / "object.yaml"
    input_path = tmp_path / "input.json"
    result_path = tmp_path / "result.json"
    artifacts_dir = tmp_path / "artifacts"
    object_yaml.write_text(ENV_FUNCTION_YAML, encoding="utf-8")
    input_path.write_text(json.dumps({"args": [], "kwargs": {}}), encoding="utf-8")
    monkeypatch.setenv(CALLBACK_CAPABILITY_ENV, secret)

    result = worker.execute(
        object_yaml=object_yaml,
        entrypoint="read_callback_env",
        input_path=input_path,
        result_path=result_path,
        artifacts_dir=artifacts_dir,
    )

    assert result["result"] is None
    assert CALLBACK_CAPABILITY_ENV not in os.environ
    assert all(secret not in path.read_text(encoding="utf-8") for path in tmp_path.rglob("*") if path.is_file())


def test_worker_remote_node_refuses_to_fall_back_to_master_auth() -> None:
    client = worker.RemoteNodeClient("http://127.0.0.1:8765")

    with pytest.raises(RuntimeError, match="callback capability is missing"):
        client.run_node(
            SimpleNamespace(uuid=REMOTE_NODE["id"], url=None, name="remote_obj", version="latest"),
            {},
        )


def test_daemon_scrubs_inherited_master_token_from_user_worker_environment(
    tmp_path: Any,
    monkeypatch: Any,
) -> None:
    store = RegistryStore(tmp_path)
    app = None
    master_marker = "inherited-master-token-marker"
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "read_master_env",
            "read_master_env",
            "default",
            yaml_text=MASTER_ENV_FUNCTION_YAML,
        )
        monkeypatch.setenv("SPL_DAEMON_API_TOKEN", master_marker)
        app = create_app(
            store,
            auto_build_envs=False,
            environment_manager=_ReadyEnvironment(),
        )

        started = app.runtime.start_run(
            "read_master_env",
            source="local",
            report_local_run=False,
            keep=True,
        )
        final = _wait_for_terminal(store, started["id"])

        assert final["status"] == "succeeded", final.get("error")
        assert final["result"] == {"result": None, "artifacts": {}}
        assert master_marker not in repr(final)
        assert all(
            master_marker not in path.read_text(encoding="utf-8", errors="replace")
            for path in tmp_path.rglob("*")
            if path.is_file()
        )
    finally:
        if app is not None:
            app.runtime.shutdown()
        store.close()


def test_callback_route_is_minimal_and_master_route_stays_compatible(tmp_path: Any, monkeypatch: Any) -> None:
    store = RegistryStore(tmp_path)
    app = None
    calls: list[dict[str, Any]] = []
    try:
        run = _running_run(store)
        app = create_app(store, auto_build_envs=False, api_token="master-token")
        token = app.runtime.callback_capabilities.mint(run["id"], [REMOTE_NODE], ttl_seconds=60.0)

        def fake_run_remote_node(
            node: dict[str, Any],
            *,
            kwargs: dict[str, Any],
            timeout_seconds: float | None = None,
        ) -> dict[str, Any]:
            calls.append({"node": node, "kwargs": kwargs, "timeout_seconds": timeout_seconds})
            return {"value": 7, "run_id": "central-run", "run": {"secret": "server-details"}}

        monkeypatch.setattr(app.runtime, "run_remote_node", fake_run_remote_node)

        async def request() -> tuple[int, Any, int, Any]:
            client = app.test_client()
            payload = {"node": _request_node(), "kwargs": {"x": 1}}
            callback_response = await client.post(
                "/remote-nodes/run",
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
            )
            master_response = await client.post(
                "/remote-nodes/run",
                json=payload,
                headers={"Authorization": "Bearer master-token"},
            )
            return (
                callback_response.status_code,
                await callback_response.get_json(),
                master_response.status_code,
                await master_response.get_json(),
            )

        callback_status, callback_body, master_status, master_body = asyncio.run(request())

        assert callback_status == 200
        assert callback_body == {"value": 7}
        assert master_status == 200
        assert master_body == {"value": 7, "run_id": "central-run", "run": {"secret": "server-details"}}
        assert len(calls) == 2
    finally:
        if app is not None:
            app.runtime.shutdown()
        store.close()


def test_callback_route_rejects_other_routes_and_tampering_before_central_io(
    tmp_path: Any,
    monkeypatch: Any,
) -> None:
    store = RegistryStore(tmp_path)
    app = None
    calls: list[object] = []
    try:
        run = _running_run(store)
        app = create_app(store, auto_build_envs=False)
        token = app.runtime.callback_capabilities.mint(run["id"], [REMOTE_NODE], ttl_seconds=60.0)
        monkeypatch.setattr(app.runtime, "run_remote_node", lambda *args, **kwargs: calls.append((args, kwargs)))

        async def request() -> tuple[list[int], list[tuple[int, Any]]]:
            client = app.test_client()
            route_statuses = []
            for path in ("/health", "/objects", "/runs", "/libraries"):
                response = await client.get(path, headers={"Authorization": f"Bearer {token}"})
                route_statuses.append(response.status_code)
            tampered_responses = []
            for field, value in (
                ("owner_id", "owner-2"),
                ("server_url", "https://attacker.invalid/api"),
            ):
                tampered = _request_node()
                tampered[field] = value
                response = await client.post(
                    "/remote-nodes/run",
                    json={"node": tampered, "kwargs": {}},
                    headers={"Authorization": f"Bearer {token}"},
                )
                tampered_responses.append((response.status_code, await response.get_json()))
            return route_statuses, tampered_responses

        statuses, tampered_responses = asyncio.run(request())

        assert statuses == [401, 401, 401, 401]
        assert [status for status, _ in tampered_responses] == [403, 403]
        assert all("does not authorize" in body["error"] for _, body in tampered_responses)
        assert calls == []
    finally:
        if app is not None:
            app.runtime.shutdown()
        store.close()


def test_real_worker_node_remote_succeeds_through_guard_without_capability_leaks(
    tmp_path: Any,
    monkeypatch: Any,
    caplog: Any,
) -> None:
    store = RegistryStore(tmp_path)
    app = None
    stop_server = None
    server_thread = None
    server_errors: list[BaseException] = []
    token_marker = CALLBACK_CAPABILITY_PREFIX + "real-worker-capability-marker"
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "remote_pipeline",
            "remote_pipeline",
            "default",
            yaml_text=REMOTE_PIPELINE_YAML,
            remote_signature_resolver=lambda _: {
                "id": "remote-object",
                "version_id": "remote-version",
                "kind": "function",
                "inputs": [],
                "outputs": [{"name": "default", "type": "int"}],
            },
        )
        port = _free_local_port()
        app = create_app(
            store,
            auto_build_envs=False,
            api_token="master-token",
            daemon_base_url=f"http://127.0.0.1:{port}",
            environment_manager=_ReadyEnvironment(),
        )
        monkeypatch.setattr(
            app.runtime.callback_capabilities,
            "_token_factory",
            lambda _: "real-worker-capability-marker",
        )
        callback_calls: list[dict[str, Any]] = []

        def fake_run_remote_node(
            node: dict[str, Any],
            *,
            kwargs: dict[str, Any],
            timeout_seconds: float | None = None,
        ) -> dict[str, Any]:
            callback_calls.append({"node": node, "kwargs": kwargs, "timeout_seconds": timeout_seconds})
            return {"value": 7, "run_id": "central-run", "run": {"private": "not-for-worker"}}

        monkeypatch.setattr(app.runtime, "run_remote_node", fake_run_remote_node)
        stop_server, server_thread, server_errors = _serve_app_in_thread(app, port)

        started = app.runtime.start_run(
            "remote_pipeline",
            output="remote",
            source="local",
            report_local_run=False,
            keep=True,
        )
        final = _wait_for_terminal(store, started["id"])

        assert final["status"] == "succeeded", final.get("error")
        assert final["result"] == {"result": {"default": 7}, "artifacts": {}}
        assert len(callback_calls) == 1
        assert callback_calls[0]["node"]["uuid"] == REMOTE_NODE["id"]
        assert app.runtime.callback_capabilities._records == {}
        assert token_marker not in repr(final)
        assert token_marker not in caplog.text
        assert all(
            token_marker not in path.read_text(encoding="utf-8", errors="replace")
            for path in tmp_path.rglob("*")
            if path.is_file()
        )
        assert record["pipeline_nodes"][0]["node_id"] == REMOTE_NODE["id"]
    finally:
        if stop_server is not None:
            stop_server.set()
        if server_thread is not None:
            server_thread.join(timeout=5.0)
        if app is not None:
            app.runtime.shutdown()
        store.close()

    if server_thread is not None and server_thread.is_alive():
        raise AssertionError("test daemon thread did not stop")
    if server_errors:
        raise AssertionError("test daemon failed") from server_errors[0]


def test_terminal_hook_and_shutdown_revoke_callback_capabilities(tmp_path: Any) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        run = _running_run(store)
        app = create_app(store, auto_build_envs=False)
        token = app.runtime.callback_capabilities.mint(run["id"], [REMOTE_NODE], ttl_seconds=60.0)
        assert (
            app.runtime.callback_capabilities.authenticate(token, method="POST", path="/remote-nodes/run") is not None
        )

        app.runtime._update_local_run_terminal(
            run["id"],
            report_local_run=False,
            status="succeeded",
            result={"result": 1, "artifacts": {}},
        )

        assert app.runtime.callback_capabilities._records == {}
        second = app.runtime.callback_capabilities.mint(
            run["id"],
            [REMOTE_NODE],
            ttl_seconds=60.0,
        )
        assert second
        app.runtime.shutdown()
        assert app.runtime.callback_capabilities._records == {}
    finally:
        if app is not None:
            app.runtime.shutdown()
        store.close()
