from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path
from typing import Any, Callable
from urllib.error import URLError

import pytest

import spl.daemon.remote_client as remote_client_module
from spl.daemon.remote_client import ServerClient, ServerClientError
from spl.daemon.routes._helpers import RouteContext
from spl.daemon.server import DaemonRuntime
from spl.daemon.server_connection import ServerOfflineError
from spl.daemon.store import RegistryStore


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection: dict[str, Any], *, token: str) -> None:
        pass

    def ensure_server_heartbeat(self, connection: dict[str, Any] | None = None) -> None:
        pass

    def status(self, connection_id: str | None = None) -> dict[str, Any]:
        return {"connection_id": connection_id, "thread_alive": False, "last_tick_at": None}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


class _ProbeServer:
    current_calls = 0
    machine_calls = 0
    probe_started: threading.Event | None = None
    probe_release: threading.Event | None = None
    probe_error: ServerClientError | None = None

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
        request_timeout_seconds: float | None = None,
    ) -> None:
        del base_url, machine_token, user_token
        assert request_timeout_seconds is None or request_timeout_seconds <= 5.0

    @classmethod
    def reset(cls) -> None:
        cls.current_calls = 0
        cls.machine_calls = 0
        cls.probe_started = None
        cls.probe_release = None
        cls.probe_error = None

    def current_connection(self) -> dict[str, Any]:
        type(self).current_calls += 1
        if type(self).probe_started is not None:
            type(self).probe_started.set()
        if type(self).probe_release is not None:
            assert type(self).probe_release.wait(timeout=2.0)
        if type(self).probe_error is not None:
            raise type(self).probe_error
        return _remote_connection()

    def list_machines(self) -> list[dict[str, Any]]:
        type(self).machine_calls += 1
        return [{"id": "machine-1"}]


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


def _runtime(tmp_path: Path) -> tuple[RegistryStore, DaemonRuntime, dict[str, Any]]:
    _ProbeServer.reset()
    store = RegistryStore(tmp_path)
    connection = store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    runtime = DaemonRuntime(
        store,
        heartbeat_service=_NoopHeartbeats(),
        server_client_factory=_ProbeServer,
    )
    credentials = store.get_server_connection_credentials(connection["id"])
    runtime._mark_server_channel_success(credentials)  # noqa: SLF001
    return store, runtime, credentials


def _open_breaker(runtime: DaemonRuntime, credentials: dict[str, Any]) -> None:
    runtime._mark_server_channel_failure(  # noqa: SLF001
        credentials,
        error=ServerClientError(503, "transient failure one"),
    )
    runtime._mark_server_channel_failure(  # noqa: SLF001
        credentials,
        error=ServerClientError(503, "transient failure two"),
    )


def test_machines_request_probes_open_channel_and_succeeds_on_next_call(tmp_path: Path) -> None:
    store, runtime, credentials = _runtime(tmp_path)
    try:
        _open_breaker(runtime, credentials)

        recovered = runtime._require_live_server_channel_credentials(credentials)  # noqa: SLF001
        server = runtime._server_client_for_credentials(recovered)  # noqa: SLF001

        assert server.list_machines() == [{"id": "machine-1"}]
        assert _ProbeServer.current_calls == 1
        assert _ProbeServer.machine_calls == 1
        assert runtime.server_connection_state()["breaker"]["state"] == "closed"
    finally:
        runtime.shutdown()
        store.close()


def test_breaker_opens_after_two_consecutive_failures_and_success_resets_it(tmp_path: Path) -> None:
    store, runtime, credentials = _runtime(tmp_path)
    try:
        runtime._mark_server_channel_failure(  # noqa: SLF001
            credentials,
            error=ServerClientError(503, "transient failure"),
        )
        state = runtime.server_connection_state()["breaker"]
        assert state["state"] == "closed"
        assert state["consecutive_failures"] == 1

        server = runtime._server_client_for_credentials(credentials)  # noqa: SLF001
        assert server.list_machines() == [{"id": "machine-1"}]
        assert runtime.server_connection_state()["breaker"]["consecutive_failures"] == 0

        _open_breaker(runtime, credentials)
        state = runtime.server_connection_state()["breaker"]
        assert state["state"] == "open"
        assert state["consecutive_failures"] == 2
    finally:
        runtime.shutdown()
        store.close()


def test_concurrent_open_channel_requests_share_one_probe_and_do_not_block_local_store(
    tmp_path: Path,
) -> None:
    store, runtime, credentials = _runtime(tmp_path)
    _ProbeServer.probe_started = threading.Event()
    _ProbeServer.probe_release = threading.Event()
    results: list[dict[str, Any]] = []
    errors: list[BaseException] = []

    def require_live() -> None:
        try:
            results.append(runtime._require_live_server_channel_credentials(credentials))  # noqa: SLF001
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    try:
        _open_breaker(runtime, credentials)
        threads = [threading.Thread(target=require_live) for _ in range(8)]
        threads[0].start()
        assert _ProbeServer.probe_started.wait(timeout=1.0)
        for thread in threads[1:]:
            thread.start()

        started_at = time.monotonic()
        assert store.list_objects() == {}
        assert time.monotonic() - started_at < 0.25
        assert runtime.sync_status()["breaker"]["state"] == "half_open"

        _ProbeServer.probe_release.set()
        for thread in threads:
            thread.join(timeout=2.0)

        assert errors == []
        assert len(results) == 8
        assert _ProbeServer.current_calls == 1
    finally:
        _ProbeServer.probe_release.set()
        runtime.shutdown()
        store.close()


@pytest.mark.asyncio
async def test_route_probe_does_not_block_local_event_loop_operation(tmp_path: Path) -> None:
    store, runtime, credentials = _runtime(tmp_path)
    _ProbeServer.probe_started = threading.Event()
    _ProbeServer.probe_release = threading.Event()
    context = RouteContext(
        runtime=runtime,
        response_cls=object(),
        request=object(),
        local_api_token="local-token",
    )
    try:
        _open_breaker(runtime, credentials)
        server_task = asyncio.create_task(context.connected_server_client_async())
        assert await asyncio.to_thread(_ProbeServer.probe_started.wait, 1.0)

        async def local_operation() -> dict[str, dict[str, Any]]:
            await asyncio.sleep(0)
            return store.list_objects()

        assert await asyncio.wait_for(local_operation(), timeout=0.25) == {}
        _ProbeServer.probe_release.set()
        await asyncio.wait_for(server_task, timeout=2.0)
    finally:
        _ProbeServer.probe_release.set()
        runtime.shutdown()
        store.close()


def test_failed_probe_keeps_channel_open_and_exposes_actionable_detail(tmp_path: Path) -> None:
    store, runtime, credentials = _runtime(tmp_path)
    try:
        _open_breaker(runtime, credentials)
        _ProbeServer.probe_error = ServerClientError(503, "pilot server is restarting")

        with pytest.raises(ServerOfflineError, match="offline or unreachable") as caught:
            runtime._require_live_server_channel_credentials(credentials)  # noqa: SLF001

        assert caught.value.detail == "pilot server is restarting"
        breaker = runtime.sync_status()["breaker"]
        assert breaker["state"] == "open"
        assert breaker["last_probe_result"]["ok"] is False
        assert breaker["last_probe_result"]["detail"] == "pilot server is restarting"
    finally:
        runtime.shutdown()
        store.close()


def test_lease_rejection_opens_breaker_immediately(tmp_path: Path) -> None:
    store, runtime, credentials = _runtime(tmp_path)
    try:
        key = runtime._server_channel_key(credentials)  # noqa: SLF001
        with runtime._server_channel_lock:  # noqa: SLF001
            runtime._server_channel_success_at[key] = 0.0  # noqa: SLF001
        _ProbeServer.probe_error = ServerClientError(409, "connection is not active: stale")

        with pytest.raises(ServerOfflineError):
            runtime._require_live_server_channel_credentials(credentials)  # noqa: SLF001

        breaker = runtime.server_connection_state()["breaker"]
        assert breaker["state"] == "open"
        assert breaker["consecutive_failures"] == 1
        assert store.current_server_connection()["status"] == "needs_reconnect"
    finally:
        runtime.shutdown()
        store.close()


class _JsonResponse:
    def __init__(self, payload: Any):
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> _JsonResponse:
        return self

    def __exit__(self, *args: Any) -> None:
        del args

    def read(self) -> bytes:
        return self._payload


def test_transient_get_is_retried_once_after_short_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    sleeps: list[float] = []

    def fake_open(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls == 1:
            raise URLError("server restart")
        return _JsonResponse([{"id": "machine-1"}])

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    monkeypatch.setattr(remote_client_module.time, "sleep", sleeps.append)

    client = ServerClient("https://splime.io/api", "machine-token", user_token="user-token")
    assert client.list_machines() == [{"id": "machine-1"}]
    assert calls == 2
    assert sleeps == [0.5]


def test_run_publish_and_other_mutations_are_never_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    sleeps: list[float] = []

    def fake_open(*args: Any, **kwargs: Any) -> _JsonResponse:
        nonlocal calls
        del args, kwargs
        calls += 1
        raise URLError("server restart")

    monkeypatch.setattr(remote_client_module, "urlopen_verified", fake_open)
    monkeypatch.setattr(remote_client_module.time, "sleep", sleeps.append)

    client = ServerClient("https://splime.io/api", "machine-token", user_token="user-token")
    operations: list[Callable[[], Any]] = [
        lambda: client.sync(
            connection_id="connection-1",
            machine_id="machine-1",
            heartbeat_interval_seconds=60,
            events=[],
        ),
        lambda: client.create_library({"slug": "pilot"}),
        lambda: client.update_library("pilot", {"description": "updated"}),
        lambda: client.remove_library_entry("pilot", "published-object"),
    ]
    for operation in operations:
        before = calls
        with pytest.raises(ServerClientError, match="not reachable"):
            operation()
        assert calls == before + 1
    assert sleeps == []
