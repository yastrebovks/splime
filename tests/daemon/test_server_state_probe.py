from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import spl.daemon.server as server_module
from spl import SPLClient
from spl.daemon.remote_client import ServerClientError
from spl.daemon.server import DaemonRuntime, create_app
from spl.daemon.store import RegistryStore
from spl.daemon_client import Client as DaemonClient


class _HeartbeatSpy:
    def __init__(self) -> None:
        self.ensure_calls = 0

    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection: dict[str, Any], *, token: str) -> None:
        del connection, token

    def ensure_server_heartbeat(self, connection: dict[str, Any] | None = None) -> None:
        del connection
        self.ensure_calls += 1

    def status(self, connection_id: str | None = None) -> dict[str, Any]:
        return {"connection_id": connection_id, "thread_alive": False, "last_tick_at": None}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        del connection_id

    def shutdown(self) -> None:
        pass


class _StateProbeServer:
    current_calls = 0
    user_calls = 0
    probe_started: threading.Event | None = None
    probe_release: threading.Event | None = None
    probe_error: ServerClientError | None = None
    request_timeouts: list[float | None] = []

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
        request_timeout_seconds: float | None = None,
    ) -> None:
        del base_url, machine_token, user_token
        type(self).request_timeouts.append(request_timeout_seconds)

    @classmethod
    def reset(cls) -> None:
        cls.current_calls = 0
        cls.user_calls = 0
        cls.probe_started = None
        cls.probe_release = None
        cls.probe_error = None
        cls.request_timeouts = []

    def current_connection(self) -> dict[str, Any]:
        type(self).current_calls += 1
        if type(self).probe_started is not None:
            type(self).probe_started.set()
        if type(self).probe_release is not None:
            assert type(self).probe_release.wait(timeout=2.0)
        if type(self).probe_error is not None:
            raise type(self).probe_error
        return _remote_connection()

    def list_users(self, *, handle: str | None = None) -> list[dict[str, Any]]:
        del handle
        type(self).user_calls += 1
        return [
            {
                "id": "owner-a",
                "handle": "owner_a",
                "display_name": "Owner A",
                "status": "active",
            }
        ]


def _remote_connection() -> dict[str, Any]:
    return {
        "id": "remote-connection-1",
        "owner_id": "owner-a",
        "subject_type": "machine",
        "subject_id": "machine-1",
        "machine_id": "machine-1",
        "display_name": "machine-1",
        "capabilities": {},
        "status": "connected",
        "heartbeat_interval_seconds": 60,
    }


def _save_connection(store: RegistryStore) -> dict[str, Any]:
    return store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )


def _runtime(tmp_path: Path) -> tuple[RegistryStore, DaemonRuntime, dict[str, Any], _HeartbeatSpy]:
    _StateProbeServer.reset()
    store = RegistryStore(tmp_path)
    connection = _save_connection(store)
    heartbeats = _HeartbeatSpy()
    runtime = DaemonRuntime(
        store,
        heartbeat_service=heartbeats,
        server_client_factory=_StateProbeServer,
    )
    credentials = store.get_server_connection_credentials(connection["id"])
    runtime._mark_server_channel_success(credentials)  # noqa: SLF001 - circuit-breaker fixture.
    return store, runtime, credentials, heartbeats


def _open_breaker(runtime: DaemonRuntime, credentials: dict[str, Any]) -> None:
    runtime._mark_server_channel_failure(  # noqa: SLF001 - circuit-breaker fixture.
        credentials,
        error=ServerClientError(503, "transient failure one"),
    )
    runtime._mark_server_channel_failure(  # noqa: SLF001 - circuit-breaker fixture.
        credentials,
        error=ServerClientError(503, "transient failure two"),
    )


def _request(app: Any, path: str) -> tuple[int, Any]:
    async def request() -> tuple[int, Any]:
        response = await app.test_client().get(
            path,
            headers={"Authorization": f"Bearer {app.api_token}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(request())


class _RuntimeDaemon:
    def __init__(self, runtime: DaemonRuntime) -> None:
        self.runtime = runtime

    def server_connection(self, *, probe: bool = True) -> dict[str, Any]:
        return self.runtime.server_connection_state(probe=probe)

    def server_whoami(self) -> dict[str, Any]:
        return self.runtime.server_whoami()


def test_sdk_state_and_whoami_recover_open_channel_on_first_call(tmp_path: Path) -> None:
    store, runtime, credentials, _ = _runtime(tmp_path)
    client = SPLClient(daemon_port=8765)
    client._daemon = _RuntimeDaemon(runtime)  # type: ignore[assignment]  # noqa: SLF001
    try:
        _open_breaker(runtime, credentials)

        connection = client.current_server_connection()
        identity = client.whoami()

        assert connection["connected"] is True
        assert identity["live"] is True
        assert _StateProbeServer.current_calls == 1
        assert connection["breaker"]["last_probe_ok"] is True
        assert connection["breaker"]["last_probe_at"]
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.parametrize("breaker_state", ["closed", "open", "half_open"])
def test_connection_state_and_whoami_never_disagree(
    tmp_path: Path,
    breaker_state: str,
) -> None:
    store, runtime, credentials, _ = _runtime(tmp_path)
    _StateProbeServer.probe_release = threading.Event()
    try:
        if breaker_state == "closed":
            state = runtime.server_connection_state(probe=True)
            identity = runtime.server_whoami()
        elif breaker_state == "open":
            _StateProbeServer.probe_release.set()
            _open_breaker(runtime, credentials)
            state = runtime.server_connection_state(probe=True)
            identity = runtime.server_whoami()
        else:
            _open_breaker(runtime, credentials)
            _StateProbeServer.probe_started = threading.Event()
            state_box: list[dict[str, Any]] = []
            identity_box: list[dict[str, Any]] = []
            state_thread = threading.Thread(
                target=lambda: state_box.append(runtime.server_connection_state(probe=True))
            )
            identity_thread = threading.Thread(target=lambda: identity_box.append(runtime.server_whoami()))
            state_thread.start()
            assert _StateProbeServer.probe_started.wait(timeout=1.0)
            identity_thread.start()
            _StateProbeServer.probe_release.set()
            state_thread.join(timeout=2.0)
            identity_thread.join(timeout=2.0)
            assert not state_thread.is_alive()
            assert not identity_thread.is_alive()
            state = state_box[0]
            identity = identity_box[0]

        assert state["connected"] is identity["live"]
        assert state["connected"] is True
    finally:
        _StateProbeServer.probe_release.set()
        runtime.shutdown()
        store.close()


def test_closed_state_is_network_free_and_concurrent_open_state_uses_one_probe(
    tmp_path: Path,
) -> None:
    store, runtime, credentials, _ = _runtime(tmp_path)
    _StateProbeServer.probe_started = threading.Event()
    _StateProbeServer.probe_release = threading.Event()
    states: list[dict[str, Any]] = []
    try:
        assert runtime.server_connection_state(probe=True)["connected"] is True
        assert _StateProbeServer.current_calls == 0

        _open_breaker(runtime, credentials)
        threads = [
            threading.Thread(target=lambda: states.append(runtime.server_connection_state(probe=True)))
            for _ in range(8)
        ]
        threads[0].start()
        assert _StateProbeServer.probe_started.wait(timeout=1.0)
        for thread in threads[1:]:
            thread.start()
        _StateProbeServer.probe_release.set()
        for thread in threads:
            thread.join(timeout=2.0)

        assert all(not thread.is_alive() for thread in threads)
        assert len(states) == 8
        assert all(state["connected"] is True for state in states)
        assert _StateProbeServer.current_calls == 1
    finally:
        _StateProbeServer.probe_release.set()
        runtime.shutdown()
        store.close()


def test_probe_follower_wait_is_bounded_by_probe_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, runtime, credentials, _ = _runtime(tmp_path)
    _StateProbeServer.probe_started = threading.Event()
    _StateProbeServer.probe_release = threading.Event()
    monkeypatch.setattr(server_module, "SERVER_CHANNEL_PROBE_TIMEOUT_SECONDS", 0.1)
    leader = threading.Thread(target=lambda: runtime.server_connection_state(probe=True))
    try:
        _open_breaker(runtime, credentials)
        leader.start()
        assert _StateProbeServer.probe_started.wait(timeout=1.0)

        started_at = time.monotonic()
        state = runtime.server_connection_state(probe=True)
        elapsed = time.monotonic() - started_at

        assert state["connected"] is False
        assert elapsed < 0.25
    finally:
        _StateProbeServer.probe_release.set()
        leader.join(timeout=2.0)
        runtime.shutdown()
        store.close()


def test_failed_state_probe_reports_cause_and_visible_outcome(tmp_path: Path) -> None:
    store, runtime, credentials, _ = _runtime(tmp_path)
    try:
        _open_breaker(runtime, credentials)
        _StateProbeServer.probe_error = ServerClientError(503, "pilot server is restarting")

        state = runtime.server_connection_state(probe=True)

        assert state["connected"] is False
        assert state["detail"] == "pilot server is restarting"
        assert state["breaker"]["state"] == "open"
        assert state["breaker"]["last_probe_ok"] is False
        assert state["breaker"]["last_probe_at"]
        assert state["breaker"]["last_probe_result"]["detail"] == "pilot server is restarting"
    finally:
        runtime.shutdown()
        store.close()


def test_connection_route_defaults_to_probe_but_health_diagnostics_and_cold_route_do_not(
    tmp_path: Path,
) -> None:
    _StateProbeServer.reset()
    store = RegistryStore(tmp_path)
    app = create_app(store, auto_build_envs=False, server_client_factory=_StateProbeServer)
    app.runtime.heartbeat_service.shutdown()
    heartbeats = _HeartbeatSpy()
    app.runtime.heartbeat_service = heartbeats
    connection = _save_connection(store)
    credentials = store.get_server_connection_credentials(connection["id"])
    app.runtime._mark_server_channel_success(credentials)  # noqa: SLF001 - circuit-breaker fixture.
    try:
        _open_breaker(app.runtime, credentials)

        health_status, health = _request(app, "/health")
        diagnostics_status, diagnostics = _request(app, "/diagnostics")
        cold_status, cold = _request(app, "/server/connection?probe=0")

        assert health_status == diagnostics_status == cold_status == 200
        assert health["server"]["connected"] is False
        assert diagnostics["server"]["connected"] is False
        assert cold["connected"] is False
        assert _StateProbeServer.current_calls == 0
        assert heartbeats.ensure_calls == 0

        live_status, live = _request(app, "/server/connection")
        assert live_status == 200
        assert live["connected"] is True
        assert live["breaker"]["last_probe_ok"] is True
        assert _StateProbeServer.current_calls == 1
    finally:
        app.runtime.shutdown()
        store.close()


def test_daemon_and_sdk_expose_explicit_cold_connection_read(monkeypatch: pytest.MonkeyPatch) -> None:
    daemon = DaemonClient("http://127.0.0.1:8765", api_token="local-token")
    requests: list[tuple[str, str]] = []

    def json_request(
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        del payload, timeout
        requests.append((method, path))
        return {"connected": True, "connection": {"status": "connected"}}

    monkeypatch.setattr(daemon, "_json_request", json_request)
    assert daemon.server_connection()["connected"] is True
    assert daemon.server_connection(probe=False)["connected"] is True
    assert requests == [
        ("GET", "/server/connection"),
        ("GET", "/server/connection?probe=0"),
    ]

    class _RecordingDaemon:
        def __init__(self) -> None:
            self.probes: list[bool] = []

        def server_connection(self, *, probe: bool = True) -> dict[str, Any]:
            self.probes.append(probe)
            return {"connected": True, "connection": {"status": "connected"}}

    sdk = SPLClient(daemon_port=8765)
    recording = _RecordingDaemon()
    sdk._daemon = recording  # type: ignore[assignment]  # noqa: SLF001
    assert sdk.current_server_connection()["connected"] is True
    assert sdk.current_server_connection(probe=False)["connected"] is True
    assert recording.probes == [True, False]
