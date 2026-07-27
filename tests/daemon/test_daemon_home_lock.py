from __future__ import annotations

import json
import os
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator

import pytest

import spl.daemon.server as daemon_server
import spl.daemon.home_lock as home_lock_module
from spl.daemon.home_lock import (
    DAEMON_HOME_IDENTITY_FILENAME,
    DAEMON_HOME_LOCK_FILENAME,
    DaemonHomeLock,
    DaemonHomeLockedError,
)
from spl.daemon_client import Client, read_daemon_endpoint, write_daemon_endpoint


def _subprocess_env(home: Path) -> dict[str, str]:
    env = dict(os.environ)
    source_root = Path(__file__).parents[2] / "src"
    current_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = os.pathsep.join(
        [str(source_root), current_pythonpath] if current_pythonpath else [str(source_root)]
    )
    env["SPL_DAEMON_HOME"] = str(home)
    env["SPL_RUNS_HOME"] = str(home / "deployment-runs")
    env["SPL_DAEMON_SECRET_BACKEND"] = "file"
    return env


def _free_port() -> int:
    with socket.create_server(("127.0.0.1", 0)) as server:
        return int(server.getsockname()[1])


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.terminate()
    else:
        process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _wait_for_daemon(home: Path, process: subprocess.Popen[str]) -> dict[str, Any]:
    deadline = time.monotonic() + 15.0
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"test daemon exited during startup with code {process.returncode}")
        endpoint = read_daemon_endpoint(home)
        if endpoint is not None:
            try:
                health = Client(daemon_home=home).health()
                if health.get("ok") is True:
                    return endpoint
            except BaseException as exc:  # The endpoint is published immediately before bind.
                last_error = exc
        time.sleep(0.05)
    raise TimeoutError("test daemon did not become healthy") from last_error


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


class _HealthHandler(BaseHTTPRequestHandler):
    database_path = ""
    expected_token = ""

    def do_GET(self) -> None:
        if self.path != "/health" or self.headers.get("Authorization") != f"Bearer {self.expected_token}":
            self.send_response(401)
            self.end_headers()
            return
        encoded = json.dumps({"ok": True, "db": {"path": self.database_path}}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:
        del format, args


def _health_server(database_path: Path, token: str) -> Iterator[tuple[ThreadingHTTPServer, int]]:
    handler = type(
        "BoundHealthHandler",
        (_HealthHandler,),
        {"database_path": str(database_path), "expected_token": token},
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_home_lock_is_owner_only_non_inheritable_and_identity_is_stable(tmp_path: Path) -> None:
    lock = DaemonHomeLock(tmp_path)
    first = lock.acquire()
    assert lock._fd is not None
    assert os.get_inheritable(lock._fd) is False
    assert first.generation == 1
    assert first.previous_generation is None
    assert first.stale_takeover is False

    lock_payload = _read_json(tmp_path / DAEMON_HOME_LOCK_FILENAME)
    identity_payload = _read_json(tmp_path / DAEMON_HOME_IDENTITY_FILENAME)
    assert lock_payload["state"] == "running"
    assert lock_payload["instance_id"] == first.instance_id
    assert identity_payload["instance_id"] == first.instance_id
    assert "api_token" not in lock_payload
    if os.name != "nt":
        assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o700
        assert stat.S_IMODE((tmp_path / DAEMON_HOME_LOCK_FILENAME).stat().st_mode) == 0o600
        assert stat.S_IMODE((tmp_path / DAEMON_HOME_IDENTITY_FILENAME).stat().st_mode) == 0o600

    lock.release()
    assert _read_json(tmp_path / DAEMON_HOME_LOCK_FILENAME)["state"] == "stopped"

    restarted = DaemonHomeLock(tmp_path)
    second = restarted.acquire()
    try:
        assert second.instance_id == first.instance_id
        assert second.home_hash == first.home_hash
        assert second.previous_generation == 1
        assert second.generation == 2
        assert second.stale_takeover is False
    finally:
        restarted.release()


def test_generation_advances_past_newer_matching_lock_metadata(tmp_path: Path) -> None:
    first_lock = DaemonHomeLock(tmp_path)
    first = first_lock.acquire()
    first_lock.release()

    second_lock = DaemonHomeLock(tmp_path)
    second = second_lock.acquire()
    second_lock.release()
    assert second.generation == 2

    # Model an identity-file rollback after daemon.lock generation 2 was
    # already fsynced. Reusing generation 2 would collide with stale labels.
    identity_path = tmp_path / DAEMON_HOME_IDENTITY_FILENAME
    identity_payload = _read_json(identity_path)
    identity_payload["generation"] = 1
    identity_path.write_text(json.dumps(identity_payload), encoding="utf-8")

    recovered_lock = DaemonHomeLock(tmp_path)
    recovered = recovered_lock.acquire()
    try:
        assert recovered.instance_id == first.instance_id
        assert recovered.previous_generation == 2
        assert recovered.generation == 3
    finally:
        recovered_lock.release()


def test_copied_lock_metadata_cannot_seed_identity_for_another_home(tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    copied_instance = "copiedlockidentity0000000000000001"
    (tmp_path / DAEMON_HOME_LOCK_FILENAME).write_text(
        json.dumps(
            {
                "generation": 41,
                "home_hash": "0" * 64,
                "instance_id": copied_instance,
                "pid": 1,
                "state": "stopped",
            }
        ),
        encoding="utf-8",
    )

    lock = DaemonHomeLock(tmp_path)
    identity = lock.acquire()
    try:
        assert identity.instance_id != copied_instance
        assert identity.previous_generation is None
        assert identity.generation == 1
    finally:
        lock.release()


def test_held_lock_without_endpoint_is_not_stolen_or_advanced(tmp_path: Path) -> None:
    script = """
import json
import sys
from pathlib import Path
from spl.daemon.home_lock import DaemonHomeLock

lock = DaemonHomeLock(Path(sys.argv[1]))
identity = lock.acquire()
print(json.dumps({"instance_id": identity.instance_id, "generation": identity.generation}), flush=True)
sys.stdin.readline()
lock.release()
"""
    holder = subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path)],
        env=_subprocess_env(tmp_path),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        ready = json.loads(holder.stdout.readline())
        before = (tmp_path / DAEMON_HOME_IDENTITY_FILENAME).read_bytes()
        with pytest.raises(DaemonHomeLockedError, match="may still be starting or may be unresponsive"):
            DaemonHomeLock(tmp_path).acquire()
        assert (tmp_path / DAEMON_HOME_IDENTITY_FILENAME).read_bytes() == before
        assert _read_json(tmp_path / DAEMON_HOME_IDENTITY_FILENAME)["generation"] == ready["generation"]
        assert not (tmp_path / "daemon.sqlite3").exists()
        assert read_daemon_endpoint(tmp_path) is None
    finally:
        if holder.stdin is not None:
            holder.stdin.write("\n")
            holder.stdin.flush()
        holder.wait(timeout=5)
    assert holder.returncode == 0, holder.stderr.read() if holder.stderr is not None else ""


def test_crashed_owner_is_taken_over_with_same_identity_and_new_generation(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script = """
import json
import os
import sys
from pathlib import Path
from spl.daemon.home_lock import DaemonHomeLock

lock = DaemonHomeLock(Path(sys.argv[1]))
identity = lock.acquire()
print(json.dumps({"instance_id": identity.instance_id, "generation": identity.generation}), flush=True)
os._exit(0)
"""
    crashed = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        env=_subprocess_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    previous = json.loads(crashed.stdout)

    with caplog.at_level("WARNING"):
        takeover = DaemonHomeLock(tmp_path)
        current = takeover.acquire()
    try:
        assert current.instance_id == previous["instance_id"]
        assert current.previous_generation == previous["generation"]
        assert current.generation == previous["generation"] + 1
        assert current.stale_takeover is True
        assert "taking over stale daemon home state" in caplog.text
    finally:
        takeover.release()


def test_live_legacy_endpoint_is_bound_to_exact_home_before_store_open(tmp_path: Path) -> None:
    token = "legacy-local-token"
    server_context = _health_server(tmp_path / "daemon.sqlite3", token)
    server, port = next(server_context)
    try:
        write_daemon_endpoint(
            tmp_path,
            bind_host="127.0.0.1",
            host="127.0.0.1",
            port=port,
            api_token=token,
        )
        endpoint_before = (tmp_path / "daemon-endpoint.json").read_bytes()

        with pytest.raises(DaemonHomeLockedError, match="a daemon is already running for this home"):
            DaemonHomeLock(tmp_path).acquire()

        assert (tmp_path / "daemon-endpoint.json").read_bytes() == endpoint_before
        assert not (tmp_path / DAEMON_HOME_IDENTITY_FILENAME).exists()
        assert not (tmp_path / "daemon.sqlite3").exists()
    finally:
        try:
            next(server_context)
        except StopIteration:
            pass


def test_live_endpoint_for_another_home_does_not_claim_this_home(tmp_path: Path) -> None:
    token = "other-home-token"
    other_home = tmp_path / "other"
    home = tmp_path / "requested"
    server_context = _health_server(other_home / "daemon.sqlite3", token)
    server, port = next(server_context)
    try:
        write_daemon_endpoint(
            home,
            bind_host="127.0.0.1",
            host="127.0.0.1",
            port=port,
            api_token=token,
        )
        lock = DaemonHomeLock(home)
        identity = lock.acquire()
        try:
            assert identity.generation == 1
            assert identity.stale_takeover is True
        finally:
            lock.release()
    finally:
        try:
            next(server_context)
        except StopIteration:
            pass


def test_serve_acquires_before_store_and_releases_after_constructor_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_state: list[str] = []

    def fail_store(home: Path) -> None:
        observed_state.append(str(_read_json(home / DAEMON_HOME_LOCK_FILENAME)["state"]))
        raise RuntimeError("synthetic store failure")

    monkeypatch.setattr(daemon_server, "RegistryStore", fail_store)
    with pytest.raises(RuntimeError, match="synthetic store failure"):
        daemon_server.serve(home=tmp_path)

    assert observed_state == ["running"]
    assert read_daemon_endpoint(tmp_path) is None
    replacement = DaemonHomeLock(tmp_path)
    identity = replacement.acquire()
    try:
        assert identity.generation == 2
    finally:
        replacement.release()


def test_serve_holds_lock_until_runtime_shutdown_and_store_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    def assert_still_locked(stage: str) -> None:
        with pytest.raises(DaemonHomeLockedError):
            DaemonHomeLock(tmp_path).acquire()
        events.append(stage)

    class FakeStore:
        def __init__(self, home: Path):
            self.home = home

        def close(self) -> None:
            assert_still_locked("store.close")

    class FakeRuntime:
        def shutdown(self) -> None:
            assert_still_locked("runtime.shutdown")

    class FakeApp:
        runtime = FakeRuntime()

        def run(self, *, host: str, port: int) -> None:
            del host, port
            events.append("app.run")

    def fake_create_app(store: FakeStore, **kwargs: Any) -> FakeApp:
        assert kwargs["daemon_identity"].generation == 1
        events.append("create_app")
        return FakeApp()

    monkeypatch.setattr(home_lock_module, "DAEMON_HOME_LOCK_RETRIES", 1)
    monkeypatch.setattr(daemon_server, "RegistryStore", FakeStore)
    monkeypatch.setattr(daemon_server, "create_app", fake_create_app)
    monkeypatch.setattr(daemon_server, "select_daemon_port", lambda *args, **kwargs: 18765)

    daemon_server.serve(home=tmp_path)

    assert events == ["create_app", "app.run", "runtime.shutdown", "store.close"]
    replacement = DaemonHomeLock(tmp_path)
    replacement.acquire()
    replacement.release()


def test_second_real_daemon_exits_without_touching_store_or_endpoint(tmp_path: Path) -> None:
    port = _free_port()
    command = [
        sys.executable,
        "-m",
        "spl.daemon",
        "serve",
        "--home",
        str(tmp_path),
        "--port",
        str(port),
        "--no-auto-port",
        "--no-auto-build-envs",
    ]
    first = subprocess.Popen(
        command,
        env=_subprocess_env(tmp_path),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        endpoint = _wait_for_daemon(tmp_path, first)
        endpoint_before = (tmp_path / "daemon-endpoint.json").read_bytes()
        database_before = (tmp_path / "daemon.sqlite3").read_bytes()

        second = subprocess.run(
            command,
            env=_subprocess_env(tmp_path),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        assert second.returncode == 1
        assert "a daemon is already running for this home" in second.stderr
        assert str(endpoint["base_url"]) in second.stderr
        assert f"pid {first.pid}" in second.stderr
        assert "different --home" in second.stderr
        assert (tmp_path / "daemon-endpoint.json").read_bytes() == endpoint_before
        assert (tmp_path / "daemon.sqlite3").read_bytes() == database_before
        assert Client(daemon_home=tmp_path).health()["ok"] is True
    finally:
        _stop_process(first)
