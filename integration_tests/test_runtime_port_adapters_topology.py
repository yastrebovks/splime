"""Explicit cross-repository runtime-adapter acceptance topology.

This module intentionally lives outside the default ``tests`` testpath.  Run
it only in a disposable environment containing both current source trees; it
is integration evidence, not a runtime dependency from ``spl`` to
``daemon-server``.
"""

from __future__ import annotations

import asyncio
import base64
import socket
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from daemon_server.app import create_app as create_central_app
from daemon_server.auth import ALL_SCOPES, MACHINE_TOKEN_SCOPES, generate_token
from daemon_server.store import ServerStore
from spl import SPLClient
from spl._client import RemoteRun
from spl.adapters import (
    PNG_PILLOW,
    TEXT_FILE_UTF8,
    Adapter,
    ArtifactHandle,
    FileInput,
)
from spl.daemon.remote_client import ServerClient, ServerClientError
from spl.daemon.home_lock import DaemonHomeLock
from spl.daemon.server import DaemonRuntime, create_app as create_integrated_app
from spl.daemon.store import RegistryStore, utc_now
from spl.daemon_client import _safe_write_download


RUNTIME_CAPABILITY = {
    "spl.remote_run.runtime_port_adapters.v1": {
        "schema_version": 1,
        "transport": True,
        "custom_adapter_execution": {"implemented": True, "enabled": True},
    },
    "run_claim_fencing": 1,
}


@pytest.fixture(autouse=True)
def _deny_external_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Permit only the loopback HTTP topology exercised by this module."""

    original_connect = socket.socket.connect

    def guarded_connect(sock: socket.socket, address: object) -> Any:
        if isinstance(address, tuple) and str(address[0]) in {"127.0.0.1", "::1", "localhost"}:
            return original_connect(sock, address)
        raise AssertionError(f"external network access attempted: {address!r}")

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)


def save_reverse_topology(path: str, value: str) -> None:
    from pathlib import Path as LocalPath

    LocalPath(path).write_text(value[::-1], encoding="utf-8")


def load_reverse_topology(path: str) -> str:
    from pathlib import Path as LocalPath

    return LocalPath(path).read_text(encoding="utf-8")[::-1]


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection: object, *, token: str) -> None:
        del connection, token

    def ensure_server_heartbeat(self, connection: object | None = None) -> None:
        del connection

    def status(self, connection_id: str | None = None) -> dict[str, object]:
        return {"connection_id": connection_id}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        del connection_id

    def shutdown(self) -> None:
        pass


class _LoopbackCentralServer:
    """Serve the real central Quart app on an isolated loopback port."""

    def __init__(self, app: Any) -> None:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = int(reservation.getsockname()[1])
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._app = app
        self._stop = threading.Event()
        self.errors: list[BaseException] = []
        self._thread = threading.Thread(target=self._run, name="central-loopback-http", daemon=True)
        self._thread.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.errors:
                raise RuntimeError("central loopback server failed") from self.errors[0]
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.05)
        self.close()
        raise TimeoutError("central loopback server did not start")

    def _run(self) -> None:
        from hypercorn.asyncio import serve as hypercorn_serve
        from hypercorn.config import Config

        async def shutdown_trigger() -> None:
            while not self._stop.is_set():
                await asyncio.sleep(0.05)

        async def serve() -> None:
            config = Config()
            config.bind = [f"127.0.0.1:{self.port}"]
            config.use_reloader = False
            config.accesslog = None
            config.errorlog = None
            await hypercorn_serve(self._app, config, shutdown_trigger=shutdown_trigger)

        try:
            asyncio.run(serve())
        except BaseException as exc:  # pragma: no cover - surfaced in close/startup.
            self.errors.append(exc)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise RuntimeError("central loopback server did not stop")
        if self.errors:
            raise RuntimeError("central loopback server failed") from self.errors[0]


class _DroppingServerClient(ServerClient):
    """Production HTTP client with deterministic post-success response loss."""

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str,
        drop_after_success: set[str] | None = None,
        read_as_user: bool = False,
    ) -> None:
        super().__init__(base_url, machine_token, user_token=user_token, request_timeout_seconds=10)
        self._drop_after_success = set(drop_after_success or ())
        self._read_as_user = read_as_user
        self._user_reader = ServerClient(base_url, user_token, request_timeout_seconds=10)
        self.dropped: list[str] = []

    def _drop(self, operation: str) -> None:
        if operation in self._drop_after_success:
            self._drop_after_success.remove(operation)
            self.dropped.append(operation)
            raise ServerClientError(503, f"simulated dropped {operation} response")

    def create_remote_run_admission(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = super().create_remote_run_admission(payload)
        self._drop("create")
        return result

    def upload_remote_run_admission_input(
        self,
        request_id: str,
        name: str,
        body: bytes,
        *,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        result = super().upload_remote_run_admission_input(
            request_id,
            name,
            body,
            size=size,
            sha256=sha256,
        )
        self._drop("input")
        return result

    def upload_remote_run_admission_custom_bundle(
        self,
        request_id: str,
        body: bytes,
        *,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        result = super().upload_remote_run_admission_custom_bundle(
            request_id,
            body,
            size=size,
            sha256=sha256,
        )
        self._drop("bundle")
        return result

    def finalize_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        result = super().finalize_remote_run_admission(request_id)
        self._drop("finalize")
        return result

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        if not self._read_as_user:
            return super().get_remote_run(run_id)
        value = self._json_request("GET", f"/remote-runs/{run_id}", auth="user")
        if not isinstance(value, dict):
            raise RuntimeError("central Run response is not an object")
        return value

    def wait_remote_run(self, run_id: str, **options: Any) -> dict[str, Any]:
        del options
        state = self.get_remote_run(run_id)
        if state["status"] not in {"succeeded", "failed", "cancelled", "stale"}:
            raise RuntimeError(f"central Run {run_id} is not terminal")
        return state

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        if not self._read_as_user:
            return super().list_artifacts(run_id)
        value = self._json_request("GET", f"/remote-runs/{run_id}/artifacts", auth="user")
        if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
            raise RuntimeError("central artifact response is not a list of objects")
        return value

    def artifact_bytes(self, run_id: str, name: str) -> bytes:
        if not self._read_as_user:
            return super().artifact_bytes(run_id, name)
        return self._user_reader.artifact_bytes(run_id, name)

    def list_remote_artifacts(self, run_id: str) -> list[str]:
        return sorted(str(item["name"]) for item in self.list_artifacts(run_id))

    def download_remote_artifact(self, run_id: str, name: str, target: Path) -> Path:
        return _safe_write_download(target / name, self.artifact_bytes(run_id, name))


def _register_token(
    store: ServerStore,
    *,
    raw_token: str,
    subject_type: str,
    subject_id: str,
    scopes: list[str],
) -> None:
    store.create_token(
        owner_id="admin1",
        subject_type=subject_type,
        subject_id=subject_id,
        name=f"Topology {subject_id}",
        scopes=scopes,
        raw_token=raw_token,
    )


def _central_setup(store: ServerStore) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    store.create_user(user_id="admin1", display_name="Topology Admin")
    with store._transaction():
        store._conn.execute(
            """
            INSERT INTO admin_grants(
                user_id, status, granted_by_user_id, granted_at,
                revoked_by_user_id, revoked_at, reason
            )
            VALUES('admin1', 'active', NULL, ?, NULL, NULL, 'topology fixture')
            """,
            (datetime.now(UTC).isoformat(),),
        )
    tokens = {"user": generate_token(), "machine-x": generate_token(), "machine-y": generate_token()}
    _register_token(
        store,
        raw_token=tokens["user"],
        subject_type="user",
        subject_id="admin1",
        scopes=sorted(ALL_SCOPES),
    )
    connections: dict[str, dict[str, Any]] = {}
    for machine_id in ("machine-x", "machine-y"):
        store.register_machine(
            owner_id="admin1",
            machine_id=machine_id,
            display_name=machine_id,
            capabilities=RUNTIME_CAPABILITY,
        )
        _register_token(
            store,
            raw_token=tokens[machine_id],
            subject_type="machine",
            subject_id=machine_id,
            scopes=MACHINE_TOKEN_SCOPES,
        )
        connections[machine_id] = store.connect_machine(
            owner_id="admin1",
            subject_type="machine",
            subject_id=machine_id,
            token=tokens[machine_id],
            machine_id=machine_id,
            display_name=machine_id,
            capabilities=RUNTIME_CAPABILITY,
            heartbeat_interval_seconds=30,
        )
    return tokens, connections


def _echo_signature() -> dict[str, Any]:
    return {
        "kind": "function",
        "input_order": ["value"],
        "inputs": [
            {
                "name": "value",
                "type": "str",
                "required": True,
                "default": None,
                "sources": [{"node_id": None, "function": "echo", "port": "value"}],
            }
        ],
        "outputs": [
            {
                "name": "default",
                "type": "str",
                "selector": None,
                "ports": [{"name": "default", "type": "str"}],
            }
        ],
        "aliases": [],
        "distributions": [],
    }


def _image_signature(pillow_version: str) -> dict[str, Any]:
    return {
        "kind": "function",
        "input_order": ["image"],
        "inputs": [
            {
                "name": "image",
                "type": "Image.Image",
                "required": True,
                "default": None,
                "sources": [{"node_id": None, "function": "describe_image", "port": "image"}],
            }
        ],
        "outputs": [
            {
                "name": "default",
                "type": "str",
                "selector": None,
                "ports": [{"name": "default", "type": "str"}],
            }
        ],
        "aliases": [],
        "distributions": [{"package": "Pillow", "version": pillow_version}],
    }


def _remote_payload(name: str, version_id: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    return {
        "object": name,
        "version_id": version_id,
        "target_machine_id": "machine-x",
        "offline_policy": "queue",
        "args": [],
        "kwargs": kwargs,
        "output": None,
    }


def _mark_environment_ready(runtime: DaemonRuntime, record: dict[str, Any]) -> None:
    spec = runtime.environment_manager.build_spec(record)
    ready = runtime.store.upsert_environment_build(
        spec_hash=spec["spec_hash"],
        base_python=spec["base_python"],
        python_version=spec["python_version"],
        distributions=spec["distributions"],
        runtime_packages=spec["runtime_packages"],
        spec=spec["spec"],
        venv_path=Path(sys.executable).parent,
        python_path=Path(sys.executable),
        install_log_path=Path(spec["install_log_path"]),
        status="ready",
    )
    runtime.store.update_environment_build(
        ready["spec_hash"],
        status="ready",
        started_at=utc_now(),
        finished_at=utc_now(),
    )


def _expire_active_claim(store: ServerStore, run_id: str) -> None:
    expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    with store._lock, store._conn:
        store._conn.execute(
            "UPDATE remote_runs SET job_lease_expires_at = ? WHERE id = ?",
            (expired, run_id),
        )
    assert store.get_remote_run(run_id)["status"] == "queued"


def test_real_central_app_two_daemon_runtime_adapter_topology(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image_module = pytest.importorskip("PIL.Image")
    pillow_version = pytest.importorskip("importlib.metadata").version("Pillow")
    central = ServerStore(tmp_path / "central")
    requester_store = RegistryStore(tmp_path / "requester")
    target_store = RegistryStore(tmp_path / "target")
    requester: DaemonRuntime | None = None
    target: DaemonRuntime | None = None
    central_http: _LoopbackCentralServer | None = None
    requester_http: _LoopbackCentralServer | None = None
    requester_transport: _DroppingServerClient | None = None
    target_transport: _DroppingServerClient | None = None
    requester_lock: DaemonHomeLock | None = None
    try:
        tokens, connections = _central_setup(central)
        central_http = _LoopbackCentralServer(create_central_app(central))
        requester_transport = _DroppingServerClient(
            central_http.base_url,
            tokens["machine-y"],
            user_token=tokens["user"],
            drop_after_success={"create", "input", "bundle", "finalize"},
            read_as_user=True,
        )
        target_transport = _DroppingServerClient(
            central_http.base_url,
            tokens["machine-x"],
            user_token=tokens["user"],
        )

        requester_store.register_env("default", sys.executable)
        requester_lock = DaemonHomeLock(tmp_path / "requester")
        requester_identity = requester_lock.acquire()
        requester_connection = requester_store.save_server_connection(
            server_url=central_http.base_url,
            token=tokens["machine-y"],
            user_token=tokens["user"],
            connection=connections["machine-y"],
            heartbeat_interval_seconds=30,
        )
        requester_app = create_integrated_app(
            requester_store,
            auto_build_envs=False,
            server_client_factory=lambda *args, **kwargs: requester_transport,
            daemon_identity=requester_identity,
            daemon_home_lock=requester_lock,
        )
        requester = requester_app.runtime
        requester.heartbeat_service.shutdown()
        requester_store.record_server_connection_heartbeat(
            requester_connection["id"],
            remote_connection=connections["machine-y"],
        )
        requester_credentials = requester_store.get_server_connection_credentials(requester_connection["id"])
        requester._mark_server_channel_success(requester_credentials)  # noqa: SLF001
        requester_snapshot, _ = requester.build_machine_library_snapshot_manifest()
        requester_store.record_server_connection_library_snapshot(
            requester_connection["id"],
            snapshot_hash=requester_snapshot,
        )
        requester_http = _LoopbackCentralServer(requester_app)
        sdk = SPLClient(requester_http.base_url, api_token=requester_app.api_token)
        target_store.register_env("default", sys.executable)
        local_connection = target_store.save_server_connection(
            server_url=central_http.base_url,
            token=tokens["machine-x"],
            user_token=tokens["user"],
            connection=connections["machine-x"],
            heartbeat_interval_seconds=30,
        )
        target = DaemonRuntime(
            target_store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=lambda *args, **kwargs: target_transport,
            allow_remote_custom_adapters=True,
        )
        target_store.record_server_connection_heartbeat(
            local_connection["id"],
            remote_connection=connections["machine-x"],
        )
        credentials = target_store.get_server_connection_credentials(local_connection["id"])
        target._mark_server_channel_success(credentials)  # noqa: SLF001
        snapshot_hash, _ = target.build_machine_library_snapshot_manifest()
        target_store.record_server_connection_library_snapshot(
            local_connection["id"],
            snapshot_hash=snapshot_hash,
        )

        original_register = target.register_object

        def register_ready(*args: Any, **kwargs: Any) -> dict[str, Any]:
            record = original_register(*args, **kwargs)
            _mark_environment_ready(target, record)
            return record

        monkeypatch.setattr(target, "register_object", register_ready)
        monkeypatch.setattr(
            target, "_kick_server_sync", lambda connection_id: target.sync_once(connection_id=connection_id)
        )
        monkeypatch.setattr("spl.daemon.server.DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES", 0)
        monkeypatch.setattr("spl.daemon.server.DEFAULT_INLINE_REMOTE_ARTIFACT_TOTAL_MAX_BYTES", 0)

        echo_yaml = """\
- !DFunction
  name: echo
  inputs:
  - name: value
    type: str
  outputs:
  - name: default
    type: str
  body: |-
    return value.upper()
"""
        published_echo = central.publish_object(
            owner_id="admin1",
            name="echo",
            entrypoint="echo",
            yaml_text=echo_yaml,
            kind="function",
            library="default",
            content_hash="topology-echo-v1",
            metadata=_echo_signature(),
        )
        reverse = Adapter(
            key="builtins.str@spl.test.remote-reverse.v1",
            save=save_reverse_topology,
            load=load_reverse_topology,
            py_type=str,
            format="spl.test.remote-reverse.v1",
        )
        requester.import_server_object("echo", owner_id="admin1", library="default")
        custom_run = sdk.submit(
            "echo",
            kwargs={"value": "payload"},
            adapters={"inputs": {"value": reverse}, "outputs": {"default": reverse}},
            adapter_policy={"custom_remote": "allow"},
            target_machine="machine-x",
            owner="admin1",
            library="default",
            keep=True,
        )
        run_id = custom_run.id
        assert requester_transport.dropped[0] == "create"
        assert requester_transport.dropped[-1] == "finalize"
        assert set(requester_transport.dropped) == {"create", "input", "bundle", "finalize"}
        assert central._conn.execute("SELECT COUNT(*) FROM remote_run_admissions").fetchone()[0] == 1
        assert central._conn.execute("SELECT COUNT(*) FROM remote_runs WHERE id = ?", (run_id,)).fetchone()[0] == 1
        assert (
            central._conn.execute(
                "SELECT COUNT(*) FROM remote_run_input_artifacts WHERE admission_id = (SELECT id FROM remote_run_admissions WHERE run_id = ?)",
                (run_id,),
            ).fetchone()[0]
            == 2
        )

        claimed_jobs: list[dict[str, Any]] = []

        def capture_job(job: dict[str, Any], connection_id: str, **kwargs: Any) -> None:
            del connection_id, kwargs
            claimed_jobs.append(job)

        monkeypatch.setattr(target, "accept_server_job", capture_job)
        target.sync_once(connection_id=local_connection["id"])
        assert len(claimed_jobs) == 1
        stale_job = claimed_jobs.pop()
        assert stale_job["run"]["id"] == run_id
        assert stale_job["object_version"]["version_id"] == published_echo["version_id"]
        replacement_job: dict[str, Any] | None = None
        original_fence = target._send_claim_bound_runtime_progress_now  # noqa: SLF001
        start_calls: list[str] = []
        original_start = target.start_run

        def start_spy(*args: Any, **kwargs: Any) -> dict[str, Any]:
            start_calls.append(str(args[0]))
            return original_start(*args, **kwargs)

        def supersede_at_execution_fence(*args: Any, **kwargs: Any) -> None:
            nonlocal replacement_job
            if kwargs.get("status") == "running" and replacement_job is None:
                _expire_active_claim(central, run_id)
                target.sync_once(connection_id=local_connection["id"])
                assert len(claimed_jobs) == 1
                replacement_job = claimed_jobs.pop()
            return original_fence(*args, **kwargs)

        monkeypatch.setattr(target, "start_run", start_spy)
        monkeypatch.setattr(target, "_send_claim_bound_runtime_progress_now", supersede_at_execution_fence)
        target._execute_server_job(stale_job, local_connection["id"])  # noqa: SLF001
        assert replacement_job is not None
        assert start_calls == []
        assert target_store.list_runs() == []

        monkeypatch.setattr(target, "_send_claim_bound_runtime_progress_now", original_fence)
        target._execute_server_job(replacement_job, local_connection["id"])  # noqa: SLF001
        assert start_calls == ["echo"]
        terminal = custom_run.refresh()
        assert terminal["status"] == "succeeded"
        assert terminal["runtime_port_adapters"]["terminal"]["custom_execution"] == {
            "requested": True,
            "allowed": True,
            "used": True,
        }

        same_process = custom_run.collect(artifacts_dir=tmp_path / "custom-result", progress=False)
        assert same_process.output == "PAYLOAD"
        retained = next(iter(same_process.downloaded_artifacts.values()))
        assert retained.read_bytes() == b"DAOLYAP"

        restarted_sdk = SPLClient(requester_http.base_url, api_token=requester_app.api_token)
        recovered = RemoteRun(restarted_sdk, terminal, server_side=True).collect(
            artifacts_dir=tmp_path / "custom-restarted",
            progress=False,
        )
        assert isinstance(recovered.output, ArtifactHandle)
        assert recovered.output.path is not None
        assert recovered.output.path.read_bytes() == b"DAOLYAP"
        assert "explicitly trusted" in str(recovered.output.recovery)

        image_yaml = """\
- !DFunction
  name: describe_image
  inputs:
  - name: image
    type: Image.Image
  outputs:
  - name: default
    type: str
  body: |-
    return f"Image size: {image.size[0]} x {image.size[1]}; mode: {image.mode}"
- !DImport
  module: PIL.Image
  alias: Image
"""
        image_signature = _image_signature(pillow_version)
        published_image = central.publish_object(
            owner_id="admin1",
            name="describe_image",
            entrypoint="describe_image",
            yaml_text=image_yaml,
            kind="function",
            library="default",
            content_hash="topology-describe-image-v1",
            metadata=image_signature,
            distributions=image_signature["distributions"],
        )
        requester.import_server_object("describe_image", owner_id="admin1", library="default")
        image = image_module.new("RGB", (7, 5), (2, 4, 6))
        image_path = tmp_path / "caller-private-photo.png"
        image.save(image_path, format="PNG")
        image_versions: list[str] = []
        image_worker_errors: list[BaseException] = []

        def execute_next_image_job() -> None:
            try:
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    target.sync_once(connection_id=local_connection["id"])
                    if claimed_jobs:
                        image_job = claimed_jobs.pop()
                        image_versions.append(str(image_job["object_version"]["version_id"]))
                        target._execute_server_job(image_job, local_connection["id"])  # noqa: SLF001
                        return
                    time.sleep(0.05)
                raise TimeoutError("target did not claim the public call image job")
            except BaseException as exc:  # pragma: no cover - asserted in the test thread.
                image_worker_errors.append(exc)

        image_worker = threading.Thread(
            target=execute_next_image_job,
            name="topology-public-call-target",
            daemon=True,
        )
        image_worker.start()
        image_result = sdk.call(
            "describe_image",
            kwargs={"image": image},
            adapters={"inputs": {"image": PNG_PILLOW}, "outputs": {"default": TEXT_FILE_UTF8}},
            adapter_policy=None,
            target_machine="machine-x",
            owner="admin1",
            library="default",
            keep=True,
            artifacts_dir=tmp_path / "topology-image-memory-result",
            timeout_seconds=20,
            progress=False,
        )
        image_worker.join(timeout=25)
        assert not image_worker.is_alive()
        assert image_worker_errors == []
        assert image_result.mode == "server"
        assert image_result.output == "Image size: 7 x 5; mode: RGB"
        assert next(iter(image_result.downloaded_artifacts.values())).read_bytes() == (b"Image size: 7 x 5; mode: RGB")
        assert str(image_path) not in repr(image_result.run)

        def reject_caller_pillow(*args: Any, **kwargs: Any) -> None:
            del args, kwargs
            raise AssertionError("FileInput must bypass caller-side Pillow encode/decode")

        monkeypatch.setattr("spl.adapters._png_pillow_save", reject_caller_pillow)
        monkeypatch.setattr("spl.adapters._png_pillow_load", reject_caller_pillow)
        image_run = sdk.submit(
            "describe_image",
            kwargs={"image": FileInput(image_path, media_type="image/png")},
            adapters={"inputs": {"image": PNG_PILLOW}, "outputs": {"default": TEXT_FILE_UTF8}},
            adapter_policy=None,
            target_machine="machine-x",
            owner="admin1",
            library="default",
            keep=True,
        )
        target.sync_once(connection_id=local_connection["id"])
        assert len(claimed_jobs) == 1
        image_job = claimed_jobs.pop()
        assert image_job["run"]["id"] == image_run.id
        target._execute_server_job(image_job, local_connection["id"])  # noqa: SLF001
        image_terminal = image_run.refresh()
        image_result = image_run.collect(
            artifacts_dir=tmp_path / "topology-image-file-result",
            progress=False,
        )
        assert image_result.output == "Image size: 7 x 5; mode: RGB"
        assert next(iter(image_result.downloaded_artifacts.values())).read_bytes() == (b"Image size: 7 x 5; mode: RGB")
        image_versions.append(str(image_job["object_version"]["version_id"]))
        assert str(image_path) not in repr(image_terminal)
        assert image_versions == [published_image["version_id"], published_image["version_id"]]

        safe_projection = repr(requester_transport.get_remote_run(run_id))
        assert str(tmp_path) not in safe_projection
        assert "def save_reverse_topology" not in safe_projection
        assert base64.b64encode(b"DAOLYAP").decode("ascii") not in safe_projection
    finally:
        if target is not None:
            target.shutdown()
        if requester is not None:
            requester.shutdown()
        if requester_http is not None:
            requester_http.close()
        if central_http is not None:
            central_http.close()
        if requester_lock is not None:
            requester_lock.release()
        target_store.close()
        requester_store.close()
        central.close()
