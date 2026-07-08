from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

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

    def fake_urlopen(request: Any, *, timeout: float | None) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(worker, "urlopen", fake_urlopen)

    value = worker.RemoteNodeClient("http://127.0.0.1:8765").run_node(
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
    assert calls["request"].full_url == "http://127.0.0.1:8765/remote-nodes/run"


def test_remote_node_client_passes_explicit_urlopen_timeout(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen(request: Any, *, timeout: float | None) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(worker, "urlopen", fake_urlopen)

    value = worker.RemoteNodeClient(
        "http://127.0.0.1:8765",
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
    assert payload["timeout_seconds"] == 12.5


def test_daemon_client_control_json_request_uses_default_timeout(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen(request: Any, *, timeout: float | None) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen", fake_urlopen)

    value = daemon_client.Client("http://127.0.0.1:8765")._json_request("GET", "/health")

    assert value == {"value": 7}
    assert calls["timeout"] == daemon_client.DEFAULT_HTTP_TIMEOUT_SECONDS


def test_daemon_client_remote_node_run_without_user_timeout_is_uncapped(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen(request: Any, *, timeout: float | None) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen", fake_urlopen)

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

    def fake_urlopen(request: Any, *, timeout: float | None) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen", fake_urlopen)

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

    def fake_urlopen(request: Any, *, timeout: float | None) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen", fake_urlopen)

    daemon_client.Client("http://127.0.0.1:8765").run("demo", wait=True)

    payload = json.loads(calls["request"].data.decode("utf-8"))
    assert calls["request"].full_url == "http://127.0.0.1:8765/runs"
    assert calls["timeout"] is None
    assert payload["wait"] is True


def test_daemon_client_waiting_rebuild_without_user_timeout_is_uncapped(monkeypatch: Any) -> None:
    calls: dict[str, Any] = {}

    def fake_urlopen(request: Any, *, timeout: float | None) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(daemon_client, "urlopen", fake_urlopen)

    daemon_client.Client("http://127.0.0.1:8765").rebuild_environment_build("abc123", wait=True)

    payload = json.loads(calls["request"].data.decode("utf-8"))
    assert calls["request"].full_url == "http://127.0.0.1:8765/environment-builds/abc123/rebuild"
    assert calls["timeout"] is None
    assert payload["wait"] is True
