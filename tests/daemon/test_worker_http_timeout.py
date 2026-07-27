from __future__ import annotations

import json
from io import BytesIO
from types import SimpleNamespace
from typing import Any
from urllib.error import HTTPError

import pytest

import spl.daemon_client as daemon_client
from spl.daemon import worker


class FakeResponse:
    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return b'{"value": 7}'


def test_remote_node_client_without_user_timeout_does_not_cap_blocking_read(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        allow_docker_callback_http: bool,
    ) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["allow_docker_callback_http"] = allow_docker_callback_http
        return FakeResponse()

    monkeypatch.setattr(worker, "urlopen_verified", fake_urlopen_verified)

    value = worker.RemoteNodeClient(
        "http://127.0.0.1:8765",
        callback_capability="callback-token",
    ).run_node(
        SimpleNamespace(
            uuid="node-1",
            url=None,
            name="demo",
            version=1,
        ),
        {"x": 1},
    )

    assert value == 7
    assert calls["timeout"] is None
    assert calls["allow_docker_callback_http"] is False
    assert calls["request"].full_url == "http://127.0.0.1:8765/remote-nodes/run"
    assert calls["request"].get_header("Authorization") == "Bearer callback-token"


def test_remote_node_client_passes_explicit_urlopen_timeout(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        allow_docker_callback_http: bool,
    ) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["allow_docker_callback_http"] = allow_docker_callback_http
        return FakeResponse()

    monkeypatch.setattr(worker, "urlopen_verified", fake_urlopen_verified)

    value = worker.RemoteNodeClient(
        "https://host.docker.internal",
        callback_capability="callback-token",
        timeout_seconds=12.5,
    ).run_node(
        SimpleNamespace(
            uuid="node-1",
            url=None,
            name="demo",
            version=1,
        ),
        {"x": 1},
    )

    payload = json.loads(calls["request"].data.decode("utf-8"))
    assert value == 7
    assert calls["timeout"] == 12.5
    assert calls["allow_docker_callback_http"] is False
    assert payload["timeout_seconds"] == 12.5


def test_remote_node_client_enables_plaintext_only_for_docker_callback_bridge(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        allow_docker_callback_http: bool,
    ) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["allow_docker_callback_http"] = allow_docker_callback_http
        return FakeResponse()

    monkeypatch.setattr(worker, "urlopen_verified", fake_urlopen_verified)

    worker.RemoteNodeClient(
        "http://host.docker.internal",
        callback_capability="callback-token",
    ).run_node(
        SimpleNamespace(uuid="node-1", url=None, name="demo", version=1),
        {},
    )

    assert calls["request"].full_url == "http://host.docker.internal/remote-nodes/run"
    assert calls["allow_docker_callback_http"] is True


def test_remote_node_client_surfaces_actionable_redirect_error(monkeypatch: Any) -> None:
    def refuse_redirect(request: Any, **kwargs: Any) -> None:
        del kwargs
        raise HTTPError(
            request.full_url,
            302,
            "HTTP 302 redirect refused: configure the worker with the final HTTPS endpoint instead",
            {},
            BytesIO(),
        )

    monkeypatch.setattr(worker, "urlopen_verified", refuse_redirect)

    client = worker.RemoteNodeClient(
        "http://127.0.0.1:8765",
        callback_capability="callback-token",
    )
    with pytest.raises(RuntimeError, match="final HTTPS endpoint"):
        client.run_node(
            SimpleNamespace(uuid="node-1", url=None, name="demo", version=1),
            {},
        )


def test_daemon_client_control_json_request_uses_default_timeout(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        contains_credentials: bool,
    ) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["contains_credentials"] = contains_credentials
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen_verified", fake_urlopen_verified)

    value = daemon_client.Client("http://127.0.0.1:8765")._json_request("GET", "/health")

    assert value == {"value": 7}
    assert calls["timeout"] == daemon_client.DEFAULT_HTTP_TIMEOUT_SECONDS
    assert calls["contains_credentials"] is False


def test_daemon_client_marks_server_connect_body_as_credentialed(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        contains_credentials: bool,
    ) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["contains_credentials"] = contains_credentials
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen_verified", fake_urlopen_verified)

    daemon_client.Client("http://127.0.0.1:8765").connect_server(
        machine_token="fake-machine-token",
        user_token="fake-user-token",
    )

    assert calls["contains_credentials"] is True
    assert calls["timeout"] == daemon_client.DEFAULT_HTTP_TIMEOUT_SECONDS


def test_daemon_client_surfaces_actionable_redirect_error(monkeypatch: Any) -> None:
    def refuse_redirect(request: Any, **kwargs: Any) -> None:
        del kwargs
        raise HTTPError(
            request.full_url,
            301,
            "HTTP 301 redirect refused: configure the client with the final HTTPS endpoint instead",
            {},
            BytesIO(),
        )

    monkeypatch.setattr(daemon_client, "urlopen_verified", refuse_redirect)

    client = daemon_client.Client("http://127.0.0.1:8765", api_token="fake-local-token")
    with pytest.raises(daemon_client.ClientError, match="final HTTPS endpoint"):
        client.health()


def test_daemon_client_remote_node_run_without_user_timeout_is_uncapped(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        contains_credentials: bool,
    ) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["contains_credentials"] = contains_credentials
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen_verified", fake_urlopen_verified)

    daemon_client.Client("http://127.0.0.1:8765").run_remote_node(
        {"name": "demo", "version": 1},
        kwargs={"x": 1},
    )

    payload = json.loads(calls["request"].data.decode("utf-8"))
    assert calls["request"].full_url == "http://127.0.0.1:8765/remote-nodes/run"
    assert calls["timeout"] is None
    assert payload["timeout_seconds"] is None


def test_daemon_client_remote_node_run_uses_user_timeout(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        contains_credentials: bool,
    ) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["contains_credentials"] = contains_credentials
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen_verified", fake_urlopen_verified)

    daemon_client.Client("http://127.0.0.1:8765").run_remote_node(
        {"name": "demo", "version": 1},
        kwargs={"x": 1},
        timeout_seconds=12.5,
    )

    payload = json.loads(calls["request"].data.decode("utf-8"))
    assert calls["timeout"] == 12.5
    assert payload["timeout_seconds"] == 12.5


def test_daemon_client_waiting_run_without_user_timeout_is_uncapped(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        contains_credentials: bool,
    ) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["contains_credentials"] = contains_credentials
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen_verified", fake_urlopen_verified)

    daemon_client.Client("http://127.0.0.1:8765").run("demo", wait=True)

    payload = json.loads(calls["request"].data.decode("utf-8"))
    assert calls["request"].full_url == "http://127.0.0.1:8765/runs"
    assert calls["timeout"] is None
    assert payload["wait"] is True


def test_daemon_client_waiting_resume_without_user_timeout_is_uncapped(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    class ResumeResponse:
        def __enter__(self) -> "ResumeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"id": "child-run", "status": "queued"}'

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        contains_credentials: bool,
    ) -> ResumeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["contains_credentials"] = contains_credentials
        return ResumeResponse()

    def fake_wait_run(
        self: daemon_client.Client,
        run_id: str,
        *,
        poll_interval: float = 0.5,
        timeout_seconds: float | None = None,
        on_state: daemon_client.RunStateCallback | None = None,
    ) -> dict[str, Any]:
        calls["wait"] = (run_id, poll_interval, timeout_seconds, on_state)
        return {"id": run_id, "status": "succeeded"}

    monkeypatch.setattr(daemon_client, "urlopen_verified", fake_urlopen_verified)
    monkeypatch.setattr(daemon_client.Client, "wait_run", fake_wait_run)

    result = daemon_client.Client("http://127.0.0.1:8765").resume_run(
        "parent-run",
        from_="consumer",
        wait=True,
    )

    payload = json.loads(calls["request"].data.decode("utf-8"))
    assert result == {"id": "child-run", "status": "succeeded"}
    assert calls["request"].full_url == "http://127.0.0.1:8765/runs/parent-run/resume"
    assert calls["timeout"] is None
    assert payload == {"from": "consumer"}
    assert calls["wait"] == ("child-run", 0.5, None, None)


def test_daemon_client_waiting_rebuild_without_user_timeout_is_uncapped(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen_verified(
        request: Any,
        *,
        timeout: float | None,
        contains_credentials: bool,
    ) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["contains_credentials"] = contains_credentials
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen_verified", fake_urlopen_verified)

    daemon_client.Client("http://127.0.0.1:8765").rebuild_environment_build("abc123", wait=True)

    payload = json.loads(calls["request"].data.decode("utf-8"))
    assert calls["request"].full_url == "http://127.0.0.1:8765/environment-builds/abc123/rebuild"
    assert calls["timeout"] is None
    assert payload["wait"] is True
