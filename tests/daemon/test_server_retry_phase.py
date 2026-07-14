from __future__ import annotations

import json
import socket
from typing import Any
from urllib.error import URLError

import pytest

import spl.daemon.remote_client as remote_client_module
from spl._http import ConnectionPhaseError
from spl.daemon.remote_client import ServerClient, ServerClientError
from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(
        self,
        connection: dict[str, Any],
        *,
        token: str,
    ) -> None:
        del connection, token

    def ensure_server_heartbeat(
        self,
        connection: dict[str, Any] | None = None,
    ) -> None:
        del connection

    def status(self, connection_id: str | None = None) -> dict[str, Any]:
        return {
            "connection_id": connection_id,
            "thread_alive": False,
            "last_tick_at": None,
        }

    def stop_server_heartbeat(self, connection_id: str) -> None:
        del connection_id

    def shutdown(self) -> None:
        pass


class _JsonResponse:
    def __init__(self, payload: Any):
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> _JsonResponse:
        return self

    def __exit__(self, *args: Any) -> None:
        del args

    def read(self) -> bytes:
        return self._payload


def test_tls_handshake_timeout_retries_post_and_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    def fake_open(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls == 1:
            raise URLError(TimeoutError("_ssl.c:1015: The handshake operation timed out"))
        return _JsonResponse({"connected": True})

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    monkeypatch.setattr(remote_client_module.time, "sleep", sleeps.append)

    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )
    result = client.create_library({"slug": "pilot"})

    assert result == {"connected": True}
    assert calls == 2
    assert sleeps == [0.5]


def test_connection_phase_dns_failures_use_bounded_backoff_for_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    def fake_open(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls < 3:
            raise URLError(socket.gaierror("temporary name resolution failure"))
        return _JsonResponse({"connected": True})

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    monkeypatch.setattr(remote_client_module.time, "sleep", sleeps.append)

    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )
    result = client.create_library({"slug": "pilot"})

    assert result == {"connected": True}
    assert calls == 3
    assert sleeps == [0.5, 1.5]


def test_ambiguous_post_send_timeout_does_not_retry_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    def fake_open(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        raise URLError(TimeoutError("timed out while reading response"))

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    monkeypatch.setattr(remote_client_module.time, "sleep", sleeps.append)

    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )
    with pytest.raises(ServerClientError, match="not reachable"):
        client.create_library({"slug": "pilot"})

    assert calls == 1
    assert sleeps == []


def test_sync_lock_owned_calls_are_single_attempt_even_for_handshake_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    def fake_open(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        raise URLError(TimeoutError("_ssl.c:1015: The handshake operation timed out"))

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    monkeypatch.setattr(remote_client_module.time, "sleep", sleeps.append)
    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )
    operations = [
        lambda: client.heartbeat_connection(
            connection_id="connection-1",
            machine_id="machine-1",
        ),
        lambda: client.latest_machine_library_snapshot("machine-1"),
        lambda: client.sync(
            connection_id="connection-1",
            machine_id="machine-1",
            heartbeat_interval_seconds=60,
            events=[],
        ),
    ]

    for operation in operations:
        before = calls
        with pytest.raises(ServerClientError, match="not reachable"):
            operation()
        assert calls == before + 1

    assert sleeps == []


def test_connection_reset_retries_only_with_explicit_pre_send_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    def ambiguous_reset(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        raise URLError(ConnectionResetError("response reset"))

    monkeypatch.setattr(remote_client_module, "urlopen_verified", ambiguous_reset)
    monkeypatch.setattr(remote_client_module.time, "sleep", sleeps.append)
    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )

    with pytest.raises(ServerClientError, match="response reset"):
        client.create_library({"slug": "pilot"})
    assert calls == 1
    assert sleeps == []

    calls = 0

    def pre_send_reset(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls == 1:
            raise URLError(ConnectionPhaseError(ConnectionResetError("reset during handshake")))
        return _JsonResponse({"id": "library-1"})

    monkeypatch.setattr(remote_client_module, "urlopen_verified", pre_send_reset)

    assert client.create_library({"slug": "pilot"}) == {"id": "library-1"}
    assert calls == 2
    assert sleeps == [0.5]


def test_ambiguous_post_send_timeout_still_retries_get_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    def fake_open(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls == 1:
            raise URLError(TimeoutError("timed out while reading response"))
        return _JsonResponse([{"id": "machine-1"}])

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    monkeypatch.setattr(remote_client_module.time, "sleep", sleeps.append)

    client = ServerClient("https://splime.io/api", "machine-token")
    assert client.list_machines() == [{"id": "machine-1"}]
    assert calls == 2
    assert sleeps == [0.5]


def test_keyed_remote_run_retries_dropped_response_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []
    request_headers: list[dict[str, str]] = []

    def fake_open(request: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del kwargs
        calls += 1
        request_headers.append(dict(request.header_items()))
        if calls == 1:
            raise URLError(TimeoutError("response dropped after request was sent"))
        return _JsonResponse({"id": "run-1", "status": "queued"})

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    monkeypatch.setattr(remote_client_module.time, "sleep", sleeps.append)

    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )
    result = client.create_remote_run(
        {"object": "demo", "kwargs": {"value": 3}},
        idempotency_key="remote-run-key-1",
    )

    assert result == {"id": "run-1", "status": "queued"}
    assert calls == 2
    assert sleeps == [0.5]
    assert [headers["Idempotency-key"] for headers in request_headers] == [
        "remote-run-key-1",
        "remote-run-key-1",
    ]
    assert all(
        headers["Authorization"] == "Bearer machine-token" and headers["X-spl-user-token"] == "user-token"
        for headers in request_headers
    )


def test_node_remote_execution_recovers_first_tls_handshake_without_opening_breaker(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    requests: list[Any] = []

    def fake_open(request: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del kwargs
        calls += 1
        requests.append(request)
        if calls == 1:
            raise URLError(TimeoutError("_ssl.c:1015: The handshake operation timed out"))
        return _JsonResponse({"id": "remote-run-1", "status": "queued"})

    store = RegistryStore(tmp_path)
    runtime = None
    try:
        connection = store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-secret",
            user_token="user-token-secret",
            connection={
                "id": "remote-connection-1",
                "owner_id": "owner-a",
                "subject_type": "machine",
                "subject_id": "machine-1",
                "machine_id": "machine-1",
                "display_name": "machine-1",
                "capabilities": {},
                "status": "connected",
                "heartbeat_interval_seconds": 60,
            },
            heartbeat_interval_seconds=60,
        )
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
        )
        credentials = store.get_server_connection_credentials(connection["id"])
        runtime._mark_server_channel_success(credentials)  # noqa: SLF001
        monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
        monkeypatch.setattr(remote_client_module.time, "sleep", lambda delay: None)
        monkeypatch.setattr(runtime, "_kick_server_sync", lambda *args: None)
        monkeypatch.setattr(
            runtime,
            "resolve_remote_signature",
            lambda ref: {
                "id": "remote-object-1",
                "version_id": "remote-version-1",
                "kind": "function",
                "outputs": [{"name": "default", "selector": None}],
                "remote_ref": {"owner_id": "owner-a", "library": "default"},
                "execution": {"default_machine_id": "machine-1"},
            },
        )
        monkeypatch.setattr(
            runtime,
            "_wait_server_run",
            lambda run_id, **kwargs: {
                "id": run_id,
                "status": "succeeded",
                "result": {"result": 12},
            },
        )

        result = runtime.run_remote_node(
            {
                "url": "https://splime.io/api",
                "name": "demo_function",
                "version": "latest",
            },
            kwargs={"value": 3},
        )

        assert result["value"] == 12
        assert calls == 2
        assert all(request.method == "POST" for request in requests)
        assert all(request.full_url.endswith("/remote-runs") for request in requests)
        assert requests[0].get_header("Idempotency-key") == requests[1].get_header("Idempotency-key")
        breaker = runtime.server_connection_state()["breaker"]
        assert breaker["state"] == "closed"
        assert breaker["consecutive_failures"] == 0
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()
