from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import shutil
import subprocess
import sys
import stat
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import spl.daemon.server as daemon_server
from spl import Deployment, lift
from spl.core import node_runtime as m_node_runtime
from spl.core.entities.node import DEFAULT_PORT
from spl.core.entities.node_function import NodeFunction
from spl.core.ir.utils import spl_export_to_file
from spl.daemon.environment import EnvironmentBuildError
from spl.daemon.heartbeat_service import HeartbeatService
from spl.daemon.remote_client import ServerClientError
from spl.daemon.server import DaemonRuntime, create_app
from spl.daemon.storage_base import json_dumps
from spl.daemon.store import DEFAULT_OBJECT_OWNER_ID, RegistryStore, utc_now
from spl.daemon import worker as worker_module
from spl.daemon.worker import ARTIFACT_REF_KEY, WorkerNodeEnvironmentProvider, run_pipeline


ARTIFACT_FUNCTION_YAML = """\
- !DFunction
  name: artifact_func
  inputs: []
  outputs:
  - name: default
    type: dict
  body: |-
    from pathlib import Path
    Path("artifact.txt").write_text("daemon artifact", encoding="utf-8")
    return {
        "__spl_result__": {"answer": 7},
        "__spl_artifacts__": {"artifact.txt": "artifact.txt"},
    }
"""


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


def _worker_final_png() -> bytes:
    return b"\x89PNG\r\n\x1a\nsplime-final-output"


def _worker_save_bytes(path: str, obj: bytes) -> None:
    with open(path, "wb") as f:
        f.write(obj)


def _worker_load_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _worker_explicit_artifact_payload() -> dict[str, Any]:
    Path("nested.txt").write_text("nested daemon artifact", encoding="utf-8")
    return {
        "__spl_result__": {"ok": True},
        "__spl_artifacts__": {"nested.txt": "nested.txt"},
    }


def _worker_slow_marker(marker_path: str) -> str:
    import time
    from pathlib import Path

    time.sleep(1.2)
    Path(marker_path).write_text("finished", encoding="utf-8")
    return "finished"


def _worker_final_unadapted_bytes() -> bytes:
    return b"missing adapter"


def _worker_save_text(path: str, value: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(value)


def _worker_load_text(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def _worker_node_docker_seed() -> str:
    return "seed"


def _worker_node_docker_consumer(value: str) -> str:
    return f"docker:{value}"


def _worker_object_docker_seed(seed: str = "seed") -> str:
    return seed


def _worker_object_docker_consumer(value: str) -> str:
    return f"consumed:{value}"


def _worker_object_docker_resume_seed(seed: str = "seed") -> str:
    return seed


def _worker_object_docker_resume_consumer(value: str, should_fail: bool = True) -> str:
    if should_fail:
        raise RuntimeError("intentional object docker resume failure")
    return f"resumed:{value}"


def _node_docker_pipeline(name: str = "node_docker_pipeline", *, tag_consumer: bool = True):
    producer = lift(_worker_node_docker_seed).alias("producer")
    pipeline = lift(_worker_node_docker_consumer).bind(value=producer).alias("consumer").render(name)
    return pipeline.with_node_runtime("consumer", "docker") if tag_consumer else pipeline


def _object_docker_manifest_pipeline(name: str = "object_docker_manifest_pipeline"):
    producer = lift(_worker_object_docker_seed).alias("producer")
    pipeline = lift(_worker_object_docker_consumer).bind(value=producer.as_format("txt")).alias("consumer").render(name)
    return pipeline.add_adapter(str, "txt", save=_worker_save_text, load=_worker_load_text)


def _object_docker_resume_pipeline(name: str = "object_docker_resume_pipeline"):
    producer = lift(_worker_object_docker_resume_seed).alias("producer")
    return lift(_worker_object_docker_resume_consumer).bind(value=producer).alias("consumer").render(name)


def _local_node_docker_environment(
    tmp_path: Path,
    runtime_config: dict[str, Any],
) -> m_node_runtime.PreparedNodeEnvironment:
    node = NodeFunction(_worker_node_docker_consumer)
    [input_port] = node.inputs
    context = m_node_runtime.NodeRuntimeContext(
        node=node,
        node_label="consumer",
        inputs={input_port: "seed"},
        output_port=node.get_output_port(DEFAULT_PORT),
        callback=lambda _node, _inputs: {DEFAULT_PORT: "docker:seed"},
        work_dir=tmp_path / "local-node-docker",
        environment_provider=m_node_runtime.CurrentPythonEnvironmentProvider(),
        runtime_config=runtime_config,
        environment_spec=[],
    )
    return m_node_runtime.DockerNodeRuntime().prepare(context)


def _manifest_node_by_alias(manifest: dict[str, Any], alias: str) -> dict[str, Any]:
    return next(node for node in manifest["nodes"].values() if node["alias"] == alias)


def _worker_manifest_paths(run_dir: str | Path) -> list[Path]:
    return sorted((Path(run_dir) / "pipeline-state").glob("*/manifest.json"))


def _worker_manifest_dir(run_dir: str | Path) -> Path:
    manifests = _worker_manifest_paths(run_dir)
    assert manifests
    return manifests[0].parent


def _docker_container_names_for_run_ids(run_ids: list[str]) -> list[str]:
    completed = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("docker ps failed: {}".format((completed.stderr or "").strip()))
    names = set(completed.stdout.splitlines())
    return [f"splime-run-{run_id[:32]}" for run_id in run_ids if f"splime-run-{run_id[:32]}" in names]


def _manifest_key_paths(value: Any, prefix: tuple[str, ...] = ()) -> set[tuple[str, ...]]:
    paths: set[tuple[str, ...]] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            key_path = (*prefix, str(key))
            paths.add(key_path)
            paths.update(_manifest_key_paths(item, key_path))
    elif isinstance(value, list):
        list_path = (*prefix, "[]")
        paths.add(list_path)
        for item in value:
            paths.update(_manifest_key_paths(item, list_path))
    return paths


def _assert_owner_only(path: Path) -> None:
    mode = stat.S_IMODE(path.stat().st_mode)
    assert mode & 0o077 == 0, f"{path} has non-owner permissions {mode:o}"


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection, *, token: str) -> None:
        pass

    def ensure_server_heartbeat(self, connection=None) -> None:
        pass

    def status(self, connection_id: str | None = None) -> dict[str, object]:
        return {"connection_id": connection_id, "thread_alive": False, "last_tick_at": None}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


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


def _mark_object_environment_ready(
    runtime: DaemonRuntime,
    object_record: dict[str, Any],
) -> dict[str, Any]:
    spec = runtime.environment_manager.build_spec(object_record)
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
    return runtime.store.update_environment_build(
        ready["spec_hash"],
        status="ready",
        started_at=utc_now(),
        finished_at=utc_now(),
    )


def _wait_for_run(
    store: RegistryStore,
    run_id: str,
    *,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        state = store.get_run(run_id)
        if state["status"] in {"succeeded", "failed"}:
            return state
        time.sleep(0.1)
    raise TimeoutError(f"run did not finish: {run_id}")


def _json_from_app(app: Any, path: str) -> tuple[int, Any]:
    async def _request() -> tuple[int, Any]:
        client = app.test_client()
        response = await client.get(
            path,
            headers={"Authorization": f"Bearer {app.api_token}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(_request())


def _post_json_from_app(app: Any, path: str, payload: dict[str, Any]) -> tuple[int, Any]:
    async def _request() -> tuple[int, Any]:
        client = app.test_client()
        response = await client.post(
            path,
            json=payload,
            headers={"Authorization": f"Bearer {app.api_token}"},
        )
        return response.status_code, await response.get_json()

    return asyncio.run(_request())


def _shutdown_app(app: Any) -> None:
    if app is not None:
        app.runtime.shutdown()


def _save_connected_server_connection(store: RegistryStore) -> dict[str, Any]:
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


def _save_connected_owner(
    store: RegistryStore,
    *,
    owner_id: str,
    connection_id: str,
    heartbeat_interval_seconds: float = 60,
) -> dict[str, Any]:
    return store.save_server_connection(
        server_url="https://splime.io/api",
        token=f"machine-token-{owner_id}",
        user_token=f"user-token-{owner_id}",
        connection={
            "id": f"remote-{connection_id}",
            "owner_id": owner_id,
            "subject_type": "machine",
            "subject_id": "machine-1",
            "machine_id": "machine-1",
            "display_name": "lab-machine",
            "status": "connected",
            "capabilities": {},
        },
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )


def _mark_current_server_channel_live(runtime: DaemonRuntime) -> None:
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


def _heartbeat_ok(**kwargs: Any) -> dict[str, Any]:
    return {
        "status": "connected",
        "heartbeat_interval_seconds": kwargs.get("heartbeat_interval_seconds") or 60,
    }


class _CapturingSyncServerClient:
    calls: list[dict[str, Any]] = []

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
        request_timeout_seconds: float | None = None,
    ) -> None:
        self.base_url = base_url
        self.machine_token = machine_token
        self.user_token = user_token

    @classmethod
    def reset(cls) -> None:
        cls.calls = []

    def latest_machine_library_snapshot(self, machine_id: str) -> dict[str, Any] | None:
        _ = machine_id
        return {}

    def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
        return _heartbeat_ok(**kwargs)

    def sync(
        self,
        *,
        connection_id: str,
        machine_id: str,
        heartbeat_interval_seconds: float,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "connection_id": connection_id,
                "machine_id": machine_id,
                "heartbeat_interval_seconds": heartbeat_interval_seconds,
                "events": events,
            }
        )
        return {
            "event_results": [
                {
                    "event_id": event["id"],
                    "status": "ok",
                }
                for event in events
            ],
            "jobs": [],
        }


class _OwnerSignatureServerClient:
    calls: list[dict[str, Any]] = []

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
        request_timeout_seconds: float | None = None,
    ) -> None:
        self.base_url = base_url
        self.machine_token = machine_token
        self.user_token = user_token

    @classmethod
    def reset(cls) -> None:
        cls.calls = []

    def object_signature(
        self,
        object_name: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> dict[str, Any]:
        if owner_id is None:
            raise AssertionError("remote signature server calls must be owner-concrete")
        self.calls.append(
            {
                "object_name": object_name,
                "version": version,
                "owner_id": owner_id,
                "library": library,
                "function": function,
            }
        )
        return {
            "id": f"remote-object-{owner_id}",
            "version_id": f"remote-version-{owner_id}-1",
            "owner_id": owner_id,
            "kind": "function",
            "inputs": [{"name": f"{owner_id}_amount", "type": "int"}],
            "outputs": [{"name": "default", "type": "int"}],
        }


def test_sync_flush_holds_events_for_other_identities_until_matching_owner(tmp_path, caplog) -> None:
    store = RegistryStore(tmp_path)
    runtime = None
    _CapturingSyncServerClient.reset()
    try:
        event_a = store.enqueue_sync_event("object_version", {"name": "a_only", "owner_id": "owner-a"})
        event_b = store.enqueue_sync_event("object_version", {"name": "b_only", "owner_id": "owner-b"})
        _save_connected_owner(store, owner_id="owner-b", connection_id="connection-b")
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=_CapturingSyncServerClient,
        )
        _mark_current_server_channel_live(runtime)

        with caplog.at_level(logging.INFO, logger=daemon_server.LOGGER.name):
            runtime.sync_once()

        first_events = [
            event for event in _CapturingSyncServerClient.calls[-1]["events"] if event["kind"] == "object_version"
        ]
        assert [event["id"] for event in first_events] == [event_b["id"]]
        assert store.get_sync_event(event_a["id"])["status"] == "pending"
        assert store.get_sync_event(event_b["id"])["status"] == "sent"
        assert "held for another identity" in caplog.text

        summary = store.server_connection_summary()
        assert summary["held_sync_events"] == 1
        current_connection = store.current_server_connection()
        connections = store.list_server_connections()
        current_row = next(row for row in connections if row["id"] == current_connection["id"])
        assert current_row["held_sync_events"] == 1

        _save_connected_owner(store, owner_id="owner-a", connection_id="connection-a")
        _mark_current_server_channel_live(runtime)
        runtime.sync_once()

        second_events = [
            event for event in _CapturingSyncServerClient.calls[-1]["events"] if event["kind"] == "object_version"
        ]
        assert [event["id"] for event in second_events] == [event_a["id"]]
        assert store.get_sync_event(event_a["id"])["status"] == "sent"
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_sync_flush_adopts_legacy_pre_enrollment_events_under_current_owner(tmp_path, caplog) -> None:
    store = RegistryStore(tmp_path)
    runtime = None
    _CapturingSyncServerClient.reset()
    try:
        ownerless = store.enqueue_sync_event("object_version", {"name": "legacy_ownerless"})
        placeholder = store.enqueue_sync_event(
            "object_version",
            {"name": "local_placeholder", "owner_id": DEFAULT_OBJECT_OWNER_ID},
        )
        _save_connected_owner(store, owner_id="owner-a", connection_id="connection-a")
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=_CapturingSyncServerClient,
        )
        _mark_current_server_channel_live(runtime)

        with caplog.at_level(logging.INFO, logger=daemon_server.LOGGER.name):
            runtime.sync_once()

        sent = [event for event in _CapturingSyncServerClient.calls[-1]["events"] if event["kind"] == "object_version"]
        assert [event["id"] for event in sent] == [ownerless["id"], placeholder["id"]]
        assert [event["payload"]["owner_id"] for event in sent] == ["owner-a", "owner-a"]
        assert store.get_sync_event(ownerless["id"])["status"] == "sent"
        assert store.get_sync_event(placeholder["id"])["status"] == "sent"
        assert "adopted 2 pre-enrollment events as owner-a" in caplog.text
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_sync_lock_prevents_two_threads_from_sending_the_same_event(tmp_path) -> None:
    class ObservedLock:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._attempts_lock = threading.Lock()
            self._attempts = 0
            self.second_attempt = threading.Event()

        def __enter__(self) -> None:
            with self._attempts_lock:
                self._attempts += 1
                if self._attempts == 2:
                    self.second_attempt.set()
            self._lock.acquire()

        def __exit__(self, *_args: object) -> None:
            self._lock.release()

    class BlockingSyncServerClient:
        calls: list[list[dict[str, Any]]] = []
        first_call_started = threading.Event()
        release_first_call = threading.Event()
        calls_lock = threading.Lock()

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def latest_machine_library_snapshot(
            self,
            machine_id: str,
        ) -> dict[str, Any]:
            _ = machine_id
            return {}

        def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
            return _heartbeat_ok(**kwargs)

        def sync(self, **kwargs: Any) -> dict[str, Any]:
            events = kwargs["events"]
            with type(self).calls_lock:
                type(self).calls.append(events)
                call_number = len(type(self).calls)
            if call_number == 1:
                type(self).first_call_started.set()
                if not type(self).release_first_call.wait(timeout=5):
                    raise TimeoutError("test did not release the first sync call")
            return {
                "event_results": [
                    {
                        "event_id": event["id"],
                        "kind": event["kind"],
                        "status": "ok",
                        "result": {},
                    }
                    for event in events
                ],
                "jobs": [],
            }

    store = RegistryStore(tmp_path)
    runtime = None
    errors: list[BaseException] = []
    try:
        connection = _save_connected_owner(
            store,
            owner_id="owner-a",
            connection_id="connection-a",
        )
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=BlockingSyncServerClient,
        )
        _mark_current_server_channel_live(runtime)
        snapshot_hash, _ = runtime.build_machine_library_snapshot_manifest()
        store.record_server_connection_library_snapshot(
            connection["id"],
            snapshot_hash=snapshot_hash,
        )
        event = store.enqueue_sync_event(
            "object_version",
            {"name": "single_send", "owner_id": "owner-a"},
        )
        observed_lock = ObservedLock()
        runtime._server_sync_lock = observed_lock

        def run_sync() -> None:
            try:
                runtime.sync_once()
            except BaseException as exc:  # pragma: no cover - assertion below.
                errors.append(exc)

        first = threading.Thread(target=run_sync, name="sync-race-first")
        second = threading.Thread(target=run_sync, name="sync-race-second")
        first.start()
        assert BlockingSyncServerClient.first_call_started.wait(timeout=5)
        second.start()
        assert observed_lock.second_attempt.wait(timeout=5)
        BlockingSyncServerClient.release_first_call.set()
        first.join(timeout=5)
        second.join(timeout=5)

        assert not first.is_alive()
        assert not second.is_alive()
        assert errors == []
        sent_ids = [item["id"] for events in BlockingSyncServerClient.calls for item in events]
        assert sent_ids.count(event["id"]) == 1
        assert store.get_sync_event(event["id"])["status"] == "sent"
    finally:
        BlockingSyncServerClient.release_first_call.set()
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_machine_library_snapshot_event_id_is_stable_for_same_hash(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    try:
        first = runtime.build_machine_library_snapshot_event(
            snapshot_hash="snapshot-hash-a",
            manifest_items=[],
        )
        replay = runtime.build_machine_library_snapshot_event(
            snapshot_hash="snapshot-hash-a",
            manifest_items=[],
        )
        changed = runtime.build_machine_library_snapshot_event(
            snapshot_hash="snapshot-hash-b",
            manifest_items=[],
        )

        assert replay["id"] == first["id"]
        assert changed["id"] != first["id"]
    finally:
        runtime.shutdown()
        store.close()


def test_successful_lease_resets_breaker_between_sync_payload_failures(tmp_path) -> None:
    class FailingSyncServerClient:
        calls = 0

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def latest_machine_library_snapshot(self, machine_id: str) -> dict[str, Any]:
            return {}

        def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
            return _heartbeat_ok(**kwargs)

        def sync(self, **kwargs: Any) -> dict[str, Any]:
            type(self).calls += 1
            raise ServerClientError(502, "closed port")

    store = RegistryStore(tmp_path)
    runtime = None
    try:
        _save_connected_owner(store, owner_id="owner-a", connection_id="connection-a")
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=FailingSyncServerClient,
        )
        _mark_current_server_channel_live(runtime)

        with pytest.raises(ServerClientError):
            runtime.sync_once()
        with pytest.raises(ServerClientError):
            runtime.sync_once()

        assert FailingSyncServerClient.calls == 2
        assert runtime.server_connection_state()["breaker"] == {
            "state": "closed",
            "consecutive_failures": 1,
            "last_probe_result": None,
        }
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_offline_heartbeat_preserves_identity_row_and_secrets_across_restarts(tmp_path) -> None:
    class UnreachableHeartbeatServerClient:
        sync_calls = 0

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def latest_machine_library_snapshot(self, machine_id: str) -> dict[str, Any]:
            return {}

        def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
            return _heartbeat_ok(**kwargs)

        def sync(self, **kwargs: Any) -> dict[str, Any]:
            type(self).sync_calls += 1
            raise ServerClientError(502, "closed port")

    store = RegistryStore(tmp_path)
    runtime = None
    try:
        original = _save_connected_owner(
            store,
            owner_id="ky-monetech.mx",
            connection_id="monetech",
            heartbeat_interval_seconds=0.01,
        )
        original_credentials = store.get_server_connection_credentials(original["id"])

        for _ in range(3):
            store.close()
            store = RegistryStore(tmp_path)
            runtime = DaemonRuntime(
                store,
                heartbeat_service=_NoopHeartbeats(),
                server_client_factory=UnreachableHeartbeatServerClient,
            )
            credentials = store.current_server_connection_credentials()
            assert credentials is not None
            stop_event = threading.Event()
            tick_count = 0

            def heartbeat_sync_once(**kwargs: Any) -> dict[str, Any]:
                nonlocal tick_count
                tick_count += 1
                if tick_count >= 2:
                    stop_event.set()
                return runtime.sync_once(**kwargs)

            HeartbeatService(
                store,
                heartbeat_sync_once,
                initial_backoff_seconds=0.001,
                max_backoff_seconds=0.002,
            )._server_heartbeat_loop(
                credentials["id"],
                credentials["token"],
                stop_event,
            )
            runtime.shutdown()
            runtime = None

            current = store.current_server_connection()
            credentials = store.current_server_connection_credentials()
            machine_identity = json.loads((tmp_path / "server-machine-identity.json").read_text(encoding="utf-8"))

            assert tick_count == 2
            assert current is not None
            assert current["id"] == original["id"]
            assert current["owner_id"] == "ky-monetech.mx"
            assert current["status"] == "heartbeat_failed"
            assert credentials is not None
            assert credentials["id"] == original["id"]
            assert credentials["owner_id"] == "ky-monetech.mx"
            assert credentials["token"] == original_credentials["token"]
            assert credentials["user_token"] == original_credentials["user_token"]
            assert len(store.list_server_connections()) == 1
            assert machine_identity == {"machine_id": "machine-1"}
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_run_update_sync_kick_does_not_block_local_run_path(tmp_path) -> None:
    class BlockingRunUpdateServerClient:
        started = threading.Event()
        release = threading.Event()

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def latest_machine_library_snapshot(self, machine_id: str) -> dict[str, Any]:
            return {}

        def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
            return _heartbeat_ok(**kwargs)

        def sync(self, **kwargs: Any) -> dict[str, Any]:
            type(self).started.set()
            type(self).release.wait(timeout=5)
            raise ServerClientError(502, "closed port")

    store = RegistryStore(tmp_path)
    runtime = None
    try:
        connection = _save_connected_owner(store, owner_id="owner-a", connection_id="connection-a")
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=BlockingRunUpdateServerClient,
        )
        _mark_current_server_channel_live(runtime)

        started = time.monotonic()
        runtime._send_server_run_update(connection["id"], run_id="run-1", status="succeeded")
        elapsed = time.monotonic() - started

        pending = store.list_pending_sync_events()
        assert elapsed < 2.0
        assert BlockingRunUpdateServerClient.started.wait(timeout=2)
        assert len(pending) == 1
        assert pending[0]["kind"] == "run_update"
        assert pending[0]["payload"]["owner_id"] == "owner-a"
    finally:
        BlockingRunUpdateServerClient.release.set()
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_remote_signature_cache_misses_after_identity_switch_between_same_bare_name(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime = None
    _OwnerSignatureServerClient.reset()
    try:
        _save_connected_owner(store, owner_id="owner-a", connection_id="connection-a")
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=_OwnerSignatureServerClient,
        )
        _mark_current_server_channel_live(runtime)

        owner_a_signature = runtime.resolve_remote_signature({"url": "https://splime.io/api", "name": "clean_amount"})

        assert owner_a_signature["inputs"] == [{"name": "owner-a_amount", "type": "int"}]
        assert [call["owner_id"] for call in _OwnerSignatureServerClient.calls] == ["owner-a"]

        _save_connected_owner(store, owner_id="owner-b", connection_id="connection-b")
        _mark_current_server_channel_live(runtime)

        owner_b_signature = runtime.resolve_remote_signature({"url": "https://splime.io/api", "name": "clean_amount"})

        assert owner_b_signature["inputs"] == [{"name": "owner-b_amount", "type": "int"}]
        assert [call["owner_id"] for call in _OwnerSignatureServerClient.calls] == ["owner-a", "owner-b"]
        cached_by_owner = {row["owner_id"]: row for row in store.list_remote_signatures()}
        assert cached_by_owner["owner-a"]["signature"]["inputs"] == [{"name": "owner-a_amount", "type": "int"}]
        assert cached_by_owner["owner-b"]["signature"]["inputs"] == [{"name": "owner-b_amount", "type": "int"}]

        _save_connected_owner(store, owner_id="owner-a", connection_id="connection-a-latest")
        _mark_current_server_channel_live(runtime)
        call_count = len(_OwnerSignatureServerClient.calls)

        owner_a_cached = runtime.resolve_remote_signature({"url": "https://splime.io/api", "name": "clean_amount"})

        assert owner_a_cached["inputs"] == [{"name": "owner-a_amount", "type": "int"}]
        assert len(_OwnerSignatureServerClient.calls) == call_count
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_remote_signature_cache_ignores_legacy_ownerless_rows(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime = None
    _OwnerSignatureServerClient.reset()
    try:
        legacy_ref = {
            "server_url": "https://splime.io/api",
            "owner_id": None,
            "library": None,
            "object_name": "clean_amount",
            "function": None,
            "version": None,
            "version_id": None,
        }
        legacy_key = hashlib.sha256(json_dumps(legacy_ref).encode("utf-8")).hexdigest()
        now = utc_now()
        with store._lock, store._conn:  # noqa: SLF001 - seed a pre-I-03 ownerless cache row.
            store._conn.execute(
                """
                INSERT INTO remote_signatures(
                    id, server_url, owner_id, library, object_name, version, version_id,
                    signature_json, status, error, fetched_at, created_at, updated_at
                )
                VALUES(?, ?, NULL, NULL, ?, NULL, NULL, ?, 'resolved', NULL, ?, ?, ?)
                """,
                (
                    legacy_key,
                    legacy_ref["server_url"],
                    legacy_ref["object_name"],
                    json_dumps({"inputs": [{"name": "legacy_amount", "type": "int"}], "outputs": []}),
                    now,
                    now,
                    now,
                ),
            )
        _save_connected_owner(store, owner_id="owner-b", connection_id="connection-b")
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=_OwnerSignatureServerClient,
        )
        _mark_current_server_channel_live(runtime)

        signature = runtime.resolve_remote_signature({"url": "https://splime.io/api", "name": "clean_amount"})

        assert signature["inputs"] == [{"name": "owner-b_amount", "type": "int"}]
        assert [call["owner_id"] for call in _OwnerSignatureServerClient.calls] == ["owner-b"]
        rows = store.list_remote_signatures()
        assert any(row["owner_id"] is None for row in rows)
        assert store.get_remote_signature(
            {
                "server_url": "https://splime.io/api",
                "owner_id": "owner-b",
                "object_name": "clean_amount",
            }
        )["signature"]["inputs"] == [{"name": "owner-b_amount", "type": "int"}]
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_local_run_update_payload_includes_object_owner_id(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime = None
    try:
        store.register_env("default", sys.executable)
        record = store.register_object(
            "owned_runner",
            "demo_obj",
            "default",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-a",
            library="default",
        )
        run = store.create_run(
            "owned_runner",
            object_version_id=record["version_id"],
        )
        runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())

        payload = runtime._local_run_sync_payload(run)  # noqa: SLF001 - I-02 payload identity regression.

        assert payload["owner_id"] == "owner-a"
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_fast_worker_cannot_be_overwritten_by_starting(tmp_path, monkeypatch) -> None:
    """The parent must persist ``starting`` before a worker can complete."""

    store = RegistryStore(tmp_path)
    runtime = None
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
        )
        runtime.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=REMOTE_FUNCTION_YAML,
        )
        observed_statuses: list[str] = []

        def complete_synchronously(run_id: str, report_local_run: bool) -> None:
            observed_statuses.append(store.get_run(run_id)["status"])
            runtime._update_local_run(  # noqa: SLF001 - deterministic fast-worker race.
                run_id,
                report_local_run=report_local_run,
                status="preparing_environment",
            )
            runtime._update_local_run(  # noqa: SLF001 - deterministic fast-worker race.
                run_id,
                report_local_run=report_local_run,
                status="running",
                started_at=utc_now(),
            )
            runtime._update_local_run_terminal(  # noqa: SLF001 - deterministic fast-worker race.
                run_id,
                report_local_run=report_local_run,
                status="succeeded",
                result={"value": 1},
                finished_at=utc_now(),
            )

        monkeypatch.setattr(runtime, "_start_run_thread", complete_synchronously)

        returned = runtime.start_run(
            "demo_obj",
            source="local",
            report_local_run=False,
        )
        stored = store.get_run(returned["id"])

        assert observed_statuses == ["starting"]
        assert returned["status"] == "succeeded"
        assert stored["status"] == "succeeded"
        assert stored["result"] == {"value": 1}
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


class _FakeDockerEnvironmentManager:
    def __init__(
        self,
        *,
        image_tag: str = "splime-runtime:node-test",
        spec_hash: str = "node-docker-spec",
        error: BaseException | None = None,
    ) -> None:
        self.image_tag = image_tag
        self.spec_hash = spec_hash
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        return {"status": "absent", "spec_hash": self.spec_hash}

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "object_record": object_record,
                "wait": wait,
                "retry_failed": retry_failed,
            }
        )
        if self.error is not None:
            raise self.error
        return {
            "status": "ready",
            "spec_hash": self.spec_hash,
            "image_tag": self.image_tag,
        }

    def prune_images(self, spec_hash: str | None = None) -> list[dict[str, Any]]:
        return []


def _daemon_resume_parent_with_worker_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    runtime_config: dict[str, Any],
) -> tuple[RegistryStore, DaemonRuntime, dict[str, Any], Path]:
    store = RegistryStore(tmp_path / "daemon-store")
    store.register_env("default", sys.executable)
    runtime = DaemonRuntime(store, auto_build_envs=False)
    pipeline = _object_docker_resume_pipeline("object_docker_resume_staging")
    yaml_path = tmp_path / "object_docker_resume_staging.yaml"
    spl_export_to_file(yaml_path, [pipeline])
    yaml_text = yaml_path.read_text(encoding="utf-8")
    record = runtime.register_object(
        "object_docker_resume_staging",
        "object_docker_resume_staging",
        "default",
        yaml_text=yaml_text,
        runtime_config=runtime_config,
    )

    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "local-runs"))
    retained_parent = Deployment(pipeline).run(keep=True, seed="seed", should_fail=True)
    with pytest.raises(RuntimeError, match="intentional object docker resume failure"):
        with retained_parent:
            retained_parent.value("consumer")
    assert retained_parent.run_dir is not None

    parent = store.create_run(
        "object_docker_resume_staging",
        kwargs={"seed": "seed", "should_fail": True},
        output="consumer",
        object_version_id=record["version_id"],
        keep=True,
    )
    parent_run_dir = Path(parent["run_dir"])
    (parent_run_dir / "object.yaml").write_text(yaml_text, encoding="utf-8")
    parent_manifest_dir = parent_run_dir / "pipeline-state" / retained_parent.run_id
    shutil.copytree(retained_parent.run_dir, parent_manifest_dir)
    parent_manifest = json.loads((parent_manifest_dir / "manifest.json").read_text(encoding="utf-8"))
    parent = store.update_run(
        parent["id"],
        status="failed",
        finished_at=utc_now(),
        manifest=parent_manifest,
    )
    return store, runtime, parent, parent_manifest_dir


def _final_png_pipeline():
    return (
        lift(_worker_final_png)
        .alias("thumbnail")
        .render("thumbnail_pipeline")
        .add_adapter(bytes, "png", save=_worker_save_bytes, load=_worker_load_bytes)
    )


def test_pipeline_final_adapter_output_is_daemon_artifact(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store)
        pipeline = _final_png_pipeline()
        yaml_path = tmp_path / "thumbnail_pipeline.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        record = runtime.register_object(
            "thumbnail_pipeline",
            "thumbnail_pipeline",
            "default",
            yaml_text=yaml_path.read_text(encoding="utf-8"),
        )
        build = _mark_object_environment_ready(runtime, record)

        started = runtime.start_run(
            "thumbnail_pipeline",
            output="thumbnail",
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["env_build_hash"] == build["spec_hash"]
        artifact_path = Path(final["artifacts_dir"]) / "thumbnail.png"
        assert artifact_path.read_bytes() == _worker_final_png()
        assert final["result"]["artifacts"] == {"thumbnail.png": str(artifact_path)}
        assert final["result"]["result"] == {
            "default": {
                ARTIFACT_REF_KEY: True,
                "format": "png",
                "key": "builtins.bytes@png",
                "name": "thumbnail.png",
                "sha256": hashlib.sha256(_worker_final_png()).hexdigest(),
                "size": len(_worker_final_png()),
            }
        }
    finally:
        store.close()


def test_pipeline_output_normalizer_extracts_nested_explicit_artifacts(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    pipeline = lift(_worker_explicit_artifact_payload).alias("payload").render("payload_pipeline")

    result, artifacts = run_pipeline(
        pipeline,
        {},
        "payload",
        daemon_url="http://127.0.0.1:8765",
        timeout_seconds=None,
        artifacts_dir=tmp_path / "artifacts",
    )

    assert result == {"default": {"ok": True}}
    assert artifacts == {"nested.txt": str(tmp_path / "artifacts" / "nested.txt")}
    assert (tmp_path / "artifacts" / "nested.txt").read_text(encoding="utf-8") == "nested daemon artifact"


def test_pipeline_output_normalizer_reports_missing_adapter_path(tmp_path) -> None:
    pipeline = lift(_worker_final_unadapted_bytes).alias("thumbnail").render("thumbnail_pipeline")

    with pytest.raises(TypeError) as exc_info:
        run_pipeline(
            pipeline,
            {},
            "thumbnail",
            daemon_url="http://127.0.0.1:8765",
            timeout_seconds=None,
            artifacts_dir=tmp_path / "artifacts",
        )

    assert str(exc_info.value) == ("result.thumbnail.default bytes is not JSON serializable; add_adapter(bytes, ...)")


def test_pipeline_node_timeout_runtime_config_reaches_daemon_worker(tmp_path) -> None:
    marker_path = tmp_path / "marker.txt"
    pipeline = (
        lift(_worker_slow_marker)
        .alias("slow")
        .render("slow_worker_pipeline")
        .with_node_runtime("slow", "venv-subprocess")
    )

    with pytest.raises(RuntimeError, match=r"node runtime `venv-subprocess` timed out after 0.8s"):
        run_pipeline(
            pipeline,
            {"marker_path": str(marker_path)},
            "slow",
            daemon_url="http://127.0.0.1:8765",
            timeout_seconds=None,
            artifacts_dir=tmp_path / "artifacts",
            runtime_config={"node_timeout_seconds": 0.8},
        )

    time.sleep(0.7)
    assert not marker_path.exists()


def test_node_docker_start_run_prepares_image_from_pipeline_tag(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    docker_manager = _FakeDockerEnvironmentManager(image_tag="splime-runtime:from-tag")
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_environment_manager=docker_manager)
        monkeypatch.setattr(runtime, "_execute_run", lambda run_id, report_local_run=True: None)
        pipeline = _node_docker_pipeline()
        yaml_path = tmp_path / "node_docker_pipeline.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        runtime.register_object(
            "node_docker_pipeline",
            "node_docker_pipeline",
            "default",
            yaml_text=yaml_path.read_text(encoding="utf-8"),
        )

        started = runtime.start_run(
            "node_docker_pipeline",
            output="consumer",
            source="local",
            report_local_run=False,
        )

        state = store.get_run(started["id"])
        assert started["status"] == "starting"
        assert len(docker_manager.calls) == 1
        assert docker_manager.calls[0]["wait"] is True
        assert docker_manager.calls[0]["object_record"]["runtime_config"] == {"mode": "docker"}
        assert state["input"]["node_runtime_environments"]["docker"] == {
            "image_tag": "splime-runtime:from-tag",
            "spec_hash": "node-docker-spec",
            "source": "object-env-spec",
        }
    finally:
        store.close()


def test_node_docker_start_run_without_docker_selection_is_zero_cost(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    docker_manager = _FakeDockerEnvironmentManager()
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_environment_manager=docker_manager)
        monkeypatch.setattr(runtime, "_execute_run", lambda run_id, report_local_run=True: None)
        pipeline = _node_docker_pipeline("node_native_pipeline", tag_consumer=False)
        yaml_path = tmp_path / "node_native_pipeline.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        runtime.register_object(
            "node_native_pipeline",
            "node_native_pipeline",
            "default",
            yaml_text=yaml_path.read_text(encoding="utf-8"),
        )

        started = runtime.start_run(
            "node_native_pipeline",
            output="consumer",
            source="local",
            report_local_run=False,
        )

        state = store.get_run(started["id"])
        assert started["status"] == "starting"
        assert docker_manager.calls == []
        assert "node_runtime_environments" not in state["input"]
    finally:
        store.close()


def test_node_docker_explicit_image_bypasses_daemon_build(tmp_path, monkeypatch) -> None:
    runtime_config = {"docker": {"image": "python:3.13-slim", "network": "none"}}
    local_environment = _local_node_docker_environment(tmp_path, runtime_config)
    store = RegistryStore(tmp_path)
    docker_manager = _FakeDockerEnvironmentManager()
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_environment_manager=docker_manager)
        monkeypatch.setattr(runtime, "_execute_run", lambda run_id, report_local_run=True: None)
        pipeline = _node_docker_pipeline("node_docker_explicit")
        yaml_path = tmp_path / "node_docker_explicit.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        runtime.register_object(
            "node_docker_explicit",
            "node_docker_explicit",
            "default",
            yaml_text=yaml_path.read_text(encoding="utf-8"),
            runtime_config=runtime_config,
        )

        started = runtime.start_run(
            "node_docker_explicit",
            output="consumer",
            source="local",
            report_local_run=False,
        )

        state = store.get_run(started["id"])
        server_environment = state["input"]["node_runtime_environments"]["docker"]
        expected_hash = m_node_runtime.explicit_docker_image_spec_hash("python:3.13-slim")
        resolution = m_node_runtime.NodeRuntimeResolution(
            m_node_runtime.DOCKER_NODE_RUNTIME,
            m_node_runtime.NodeRuntimeResolutionSource.NODE_TAG,
        )
        local_record = m_node_runtime.runtime_manifest_record(resolution, local_environment)
        server_record = m_node_runtime.runtime_manifest_record(
            resolution,
            m_node_runtime.PreparedNodeEnvironment(
                name="docker-image",
                python_path=None,
                metadata=server_environment,
            ),
        )
        assert started["status"] == "starting"
        assert docker_manager.calls == []
        assert server_environment == {
            "image_tag": "python:3.13-slim",
            "spec_hash": expected_hash,
            "source": "runtime_config.docker.image",
        }
        assert server_environment["spec_hash"] == local_environment.metadata["spec_hash"]
        assert server_record["config_hash"] == local_record["config_hash"] == expected_hash
    finally:
        store.close()


def test_node_docker_run_override_prepares_image(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    docker_manager = _FakeDockerEnvironmentManager(image_tag="splime-runtime:from-override")
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_environment_manager=docker_manager)
        monkeypatch.setattr(runtime, "_execute_run", lambda run_id, report_local_run=True: None)
        pipeline = _node_docker_pipeline("node_override_pipeline", tag_consumer=False)
        yaml_path = tmp_path / "node_override_pipeline.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        runtime.register_object(
            "node_override_pipeline",
            "node_override_pipeline",
            "default",
            yaml_text=yaml_path.read_text(encoding="utf-8"),
        )

        started = runtime.start_run(
            "node_override_pipeline",
            output="consumer",
            source="local",
            report_local_run=False,
            runtimes={"consumer": "docker"},
        )

        state = store.get_run(started["id"])
        assert len(docker_manager.calls) == 1
        assert state["input"]["node_runtime_environments"]["docker"]["image_tag"] == "splime-runtime:from-override"
    finally:
        store.close()


def test_node_docker_preensure_failure_marks_run_failed_before_worker(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    docker_manager = _FakeDockerEnvironmentManager(error=RuntimeError("docker build exploded"))
    worker_calls: list[str] = []
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_environment_manager=docker_manager)
        monkeypatch.setattr(runtime, "_execute_run", lambda run_id, report_local_run=True: worker_calls.append(run_id))
        pipeline = _node_docker_pipeline("node_docker_failure")
        yaml_path = tmp_path / "node_docker_failure.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        runtime.register_object(
            "node_docker_failure",
            "node_docker_failure",
            "default",
            yaml_text=yaml_path.read_text(encoding="utf-8"),
        )

        started = runtime.start_run(
            "node_docker_failure",
            output="consumer",
            source="local",
            report_local_run=False,
        )

        assert started["status"] == "failed"
        assert "docker build exploded" in started["error"]
        assert worker_calls == []
    finally:
        store.close()


def test_worker_node_environment_provider_returns_daemon_node_docker_image_tag() -> None:
    provider = WorkerNodeEnvironmentProvider(
        {
            "docker": {
                "image_tag": "splime-runtime:from-worker-input",
                "spec_hash": "node-docker-spec",
                "source": "object-env-spec",
            }
        }
    )

    environment = provider.prepare({"node_runtime": m_node_runtime.DOCKER_NODE_RUNTIME})

    assert environment.python_path is None
    assert environment.metadata == {
        "image_tag": "splime-runtime:from-worker-input",
        "spec_hash": "node-docker-spec",
        "source": "object-env-spec",
    }


def test_node_docker_resume_override_prepares_image(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    docker_manager = _FakeDockerEnvironmentManager(image_tag="splime-runtime:from-resume")
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_environment_manager=docker_manager)
        pipeline = _node_docker_pipeline("node_resume_pipeline", tag_consumer=False)
        yaml_path = tmp_path / "node_resume_pipeline.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        record = runtime.register_object(
            "node_resume_pipeline",
            "node_resume_pipeline",
            "default",
            yaml_text=yaml_path.read_text(encoding="utf-8"),
        )
        _mark_object_environment_ready(runtime, record)
        parent = runtime.start_run(
            "node_resume_pipeline",
            output="consumer",
            source="local",
            report_local_run=False,
            keep=True,
        )
        parent_final = _wait_for_run(store, parent["id"])
        assert parent_final["status"] == "succeeded"
        assert docker_manager.calls == []

        observed_statuses: list[str] = []
        monkeypatch.setattr(
            runtime,
            "_start_run_thread",
            lambda run_id, report_local_run: observed_statuses.append(store.get_run(run_id)["status"]),
        )
        resumed = runtime.resume_run(
            parent["id"],
            from_="consumer",
            output="consumer",
            report_local_run=False,
            runtimes={"consumer": "docker"},
        )

        state = store.get_run(resumed["id"])
        assert observed_statuses == ["starting"]
        assert resumed["status"] == "starting"
        assert resumed["id"] != parent_final["id"]
        assert resumed["parent_run_id"] == parent_final["id"]
        assert store.get_run(parent_final["id"])["status"] == "succeeded"
        assert len(docker_manager.calls) == 1
        assert state["input"]["node_runtime_environments"]["docker"]["image_tag"] == "splime-runtime:from-resume"
    finally:
        store.close()


def test_object_docker_resume_stages_parent_manifest_dir(tmp_path, monkeypatch) -> None:
    store, runtime, parent, parent_manifest_dir = _daemon_resume_parent_with_worker_manifest(
        tmp_path,
        monkeypatch,
        runtime_config={"mode": "docker", "python": "3.13"},
    )
    try:
        monkeypatch.setattr(runtime, "_execute_run", lambda run_id, report_local_run=True: None)

        resumed = runtime.resume_run(
            parent["id"],
            from_="consumer",
            output="consumer",
            kwargs={"should_fail": False},
            report_local_run=False,
            keep=True,
        )

        state = store.get_run(resumed["id"])
        run_dir = Path(state["run_dir"])
        staged_dir = run_dir / "resume-parent"
        assert staged_dir.is_dir()
        assert (staged_dir / "manifest.json").read_text(encoding="utf-8") == (
            parent_manifest_dir / "manifest.json"
        ).read_text(encoding="utf-8")
        assert state["input"]["resume"]["parent_run_dir"] == "resume-parent"
        input_payload = json.loads((run_dir / "input.json").read_text(encoding="utf-8"))
        assert input_payload["resume"]["parent_run_dir"] == "resume-parent"
    finally:
        runtime.shutdown()
        store.close()


def test_object_venv_resume_does_not_stage_parent_manifest_dir(tmp_path, monkeypatch) -> None:
    store, runtime, parent, parent_manifest_dir = _daemon_resume_parent_with_worker_manifest(
        tmp_path,
        monkeypatch,
        runtime_config={"mode": "venv"},
    )
    try:
        monkeypatch.setattr(runtime, "_execute_run", lambda run_id, report_local_run=True: None)

        resumed = runtime.resume_run(
            parent["id"],
            from_="consumer",
            output="consumer",
            kwargs={"should_fail": False},
            report_local_run=False,
            keep=True,
        )

        state = store.get_run(resumed["id"])
        run_dir = Path(state["run_dir"])
        parent_run_dir = state["input"]["resume"]["parent_run_dir"]
        assert parent_run_dir == str(parent_manifest_dir)
        assert Path(parent_run_dir).is_absolute()
        assert not (run_dir / "resume-parent").exists()
    finally:
        runtime.shutdown()
        store.close()


def test_object_docker_resume_parent_staging_is_idempotent(tmp_path, monkeypatch) -> None:
    store, runtime, parent, parent_manifest_dir = _daemon_resume_parent_with_worker_manifest(
        tmp_path,
        monkeypatch,
        runtime_config={"mode": "docker", "python": "3.13"},
    )
    try:
        monkeypatch.setattr(runtime, "_execute_run", lambda run_id, report_local_run=True: None)
        resumed = runtime.resume_run(
            parent["id"],
            from_="consumer",
            output="consumer",
            kwargs={"should_fail": False},
            report_local_run=False,
            keep=True,
        )
        state = store.get_run(resumed["id"])
        object_record = store.get_object_version(state["object_version_id"])
        staged_dir = Path(state["run_dir"]) / "resume-parent"
        stale_path = staged_dir / "stale.txt"
        stale_path.write_text("old copy", encoding="utf-8")

        restaged = runtime._stage_object_docker_resume_parent(
            state,
            object_record=object_record,
            parent_manifest_dir=parent_manifest_dir,
            report_local_run=False,
        )

        assert restaged["input"]["resume"]["parent_run_dir"] == "resume-parent"
        assert not stale_path.exists()
        assert (staged_dir / "manifest.json").exists()
    finally:
        runtime.shutdown()
        store.close()


def test_worker_resume_parent_dir_resolves_relative_path_from_run_dir(tmp_path, monkeypatch) -> None:
    unrelated_cwd = tmp_path / "cwd with spaces"
    unrelated_cwd.mkdir()
    monkeypatch.chdir(unrelated_cwd)
    artifacts_dir = tmp_path / "worker-run" / "artifacts"
    artifacts_dir.mkdir(parents=True)

    resolved = worker_module._resume_parent_run_dir({"parent_run_dir": "resume-parent"}, artifacts_dir=artifacts_dir)

    assert Path(resolved) == artifacts_dir.parent / "resume-parent"
    absolute_parent = str(tmp_path / "absolute-parent")
    assert (
        worker_module._resume_parent_run_dir({"parent_run_dir": absolute_parent}, artifacts_dir=artifacts_dir)
        == absolute_parent
    )


def test_publish_run_environment_and_artifact_flow(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store)

        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
        )
        build = _mark_object_environment_ready(runtime, record)

        started = runtime.start_run(
            "artifact_func",
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["env_build_hash"] == build["spec_hash"]
        assert final["result"]["result"] == {"answer": 7}

        artifact_path = Path(final["artifacts_dir"]) / "artifact.txt"
        assert artifact_path.read_text(encoding="utf-8") == "daemon artifact"

        encoded = runtime._encode_local_artifacts(final)
        assert encoded == [
            {
                "name": "artifact.txt",
                "data_base64": base64.b64encode(b"daemon artifact").decode("ascii"),
            }
        ]
        text_artifacts = runtime._local_run_text_artifacts(final)
        assert any(
            item["name"] == "artifact.artifact.txt" and item["content_text"] == "daemon artifact"
            for item in text_artifacts
        )
    finally:
        store.close()


def test_runtime_shutdown_joins_run_threads_before_store_close(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    release_run = threading.Event()
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
        )
        _mark_object_environment_ready(runtime, record)

        run_started = threading.Event()
        run_finished = threading.Event()
        shutdown_finished = threading.Event()
        active_runtime = runtime

        def slow_execute_run(run_id: str, report_local_run: bool = True) -> None:
            active_runtime._update_local_run(
                run_id,
                report_local_run=report_local_run,
                status="running",
                started_at=utc_now(),
            )
            run_started.set()
            assert release_run.wait(2)
            active_runtime._update_local_run(
                run_id,
                report_local_run=report_local_run,
                status="succeeded",
                finished_at=utc_now(),
                result={"result": "finished"},
            )
            run_finished.set()

        monkeypatch.setattr(runtime, "_execute_run", slow_execute_run)
        started = runtime.start_run(
            "artifact_func",
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        assert run_started.wait(2)

        shutdown_thread = threading.Thread(
            target=lambda: (runtime.shutdown(), shutdown_finished.set()),
            name="shutdown-join-test",
        )
        shutdown_thread.start()
        assert not shutdown_finished.wait(0.1)

        release_run.set()
        assert shutdown_finished.wait(2)
        shutdown_thread.join(2)
        assert run_finished.is_set()
        assert not any(thread.name.startswith("spl-run-") for thread in threading.enumerate())

        assert store.get_run(started["id"])["status"] == "succeeded"
        store.close()
        store.close()
    finally:
        release_run.set()
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_terminal_run_update_skips_store_closed_during_shutdown(tmp_path, caplog) -> None:
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False)
    try:
        store.close()
        caplog.set_level(logging.WARNING, logger="spl.daemon.server")

        updated = runtime._update_local_run_terminal(
            "closed-store-run",
            report_local_run=False,
            status="failed",
        )

        assert updated is None
        assert "run state write skipped: store closed during shutdown" in caplog.text
    finally:
        runtime.shutdown()
        store.close()


def test_docker_runtime_config_is_persisted_and_command_is_constructed(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)

        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13"},
        )

        assert record["runtime_config"]["mode"] == "docker"
        assert record["runtime_config"]["python"] == "3.13"
        assert record["runtime_config"]["base_image"] == "python:3.13-slim-trixie"
        assert record["runtime_config"]["network"] == "auto"
        assert record["runtime_config"]["limits"]["pids_limit"] == 256
        assert record["runtime_config"]["tmpfs"] == "/tmp:rw,nosuid,size=512m"

        build_spec = runtime.docker_environment_manager.build_spec(record)
        assert build_spec["base_image"] == "python:3.13-slim-trixie"
        assert build_spec["image_tag"].startswith("splime-runtime:")

        run_dir = tmp_path / "runs" / "docker-run"
        run_dir.mkdir(parents=True)
        workdir = tmp_path / "work"
        workdir.mkdir()
        daemon_src = tmp_path / "daemon-src"
        worker = daemon_src / "spl" / "daemon" / "worker.py"
        worker.parent.mkdir(parents=True)
        worker.write_text("# worker", encoding="utf-8")
        framework_src = tmp_path / "framework-src"
        framework_src.mkdir()
        monkeypatch.setattr(
            runtime.docker_pool,
            "source_roots",
            lambda: [("daemon", daemon_src), ("framework", framework_src)],
        )

        command = runtime.docker_pool.worker_command(
            object_record=record,
            entrypoint=record["entrypoint"],
            run_id="abc123",
            run_dir=run_dir,
            workdir=workdir,
            image_tag=build_spec["image_tag"],
            container_name="splime-run-abc123",
            runtime_config=record["runtime_config"],
        )

        assert command[:5] == ["docker", "run", "--rm", "--name", "splime-run-abc123"]
        assert "--network" in command
        assert command[command.index("--network") + 1] == "none"
        assert "--read-only" in command
        assert "--tmpfs" in command
        assert command[command.index("--tmpfs") + 1] == "/tmp:rw,nosuid,size=512m"
        assert "--cap-drop" in command
        assert command[command.index("--cap-drop") + 1] == "ALL"
        assert "--security-opt" in command
        assert "no-new-privileges" in command
        assert "--pids-limit" in command
        assert command[command.index("--pids-limit") + 1] == "256"
        assert build_spec["image_tag"] in command
        assert "/opt/splime/src0/spl/daemon/worker.py" in command
        assert f"{run_dir.resolve()}:/work" in command
        assert f"{workdir.resolve()}:/workspace" in command
        assert "PYTHONPATH=/opt/splime/src0:/opt/splime/src1" in command
        assert "SPL_OBJECT_RUNTIME_BACKEND=docker" in command
        assert "SPL_OBJECT_DOCKER_WORKER=1" in command
        assert "HOME=/tmp" in command
        assert "XDG_CACHE_HOME=/tmp/.cache" in command
        assert "MPLCONFIGDIR=/tmp/.cache/matplotlib" in command
    finally:
        store.close()


def test_docker_runtime_rejects_python_before_313(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)

        try:
            runtime.register_object(
                "artifact_func",
                "artifact_func",
                "default",
                yaml_text=ARTIFACT_FUNCTION_YAML,
                runtime_config={"mode": "docker", "python": "3.12"},
            )
        except ValueError as exc:
            assert "Python >= 3.13" in str(exc)
        else:
            raise AssertionError("expected Python version validation to fail")
    finally:
        store.close()


def test_docker_runtime_rejects_network_none_with_remote_nodes(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        try:
            store._validate_runtime_config_for_metadata(
                {"mode": "docker", "network": "none"},
                {"pipeline_nodes": [{"kind": "remote"}]},
            )
        except ValueError as exc:
            assert "network='none' is incompatible with remote" in str(exc)
        else:
            raise AssertionError("expected network validation to fail")
    finally:
        store.close()


def test_docker_network_uses_bridge_host_gateway_for_remote_nodes(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False)
        monkeypatch.setattr("spl.daemon.docker_pool.platform.system", lambda: "Linux")

        args, daemon_url = runtime.docker_pool.network_args(
            {"pipeline_nodes": [{"kind": "remote"}]},
            {"mode": "docker", "network": "auto"},
        )

        assert args == ["--add-host", "host.docker.internal:host-gateway"]
        assert "host.docker.internal" in daemon_url
    finally:
        store.close()


def test_docker_pool_exec_command_uses_runs_mount(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=1)

        command = runtime.docker_pool.exec_worker_command(
            object_record={"pipeline_nodes": []},
            entrypoint="artifact_func",
            run_id="abc123",
            container_name="splime-pool-test",
            runtime_config={"mode": "docker", "network": "auto"},
        )

        assert command[:2] == ["docker", "exec"]
        assert command[command.index("-w") + 1] == "/runs/abc123"
        assert "SPL_OBJECT_RUNTIME_BACKEND=docker" in command
        assert "SPL_OBJECT_DOCKER_WORKER=1" in command
        assert "splime-pool-test" in command
        assert "/runs/abc123/object.yaml" in command
        assert "/runs/abc123/result.json" in command
    finally:
        store.close()


def test_docker_pool_key_includes_effective_network(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=1)
        monkeypatch.setattr("spl.daemon.docker_pool.platform.system", lambda: "Linux")
        config = {"mode": "docker", "network": "auto"}

        local_key = runtime.docker_pool.pool_key(
            "splime-runtime:demo",
            config,
            {"pipeline_nodes": []},
        )
        remote_key = runtime.docker_pool.pool_key(
            "splime-runtime:demo",
            config,
            {"pipeline_nodes": [{"kind": "remote"}]},
        )

        assert local_key != remote_key
    finally:
        store.close()


def test_docker_pool_records_exec_lock(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=1)
        monkeypatch.setattr(
            runtime.docker_pool,
            "start_container",
            lambda **kwargs: {
                "key": kwargs["key"],
                "name": "splime-pool-test",
                "image_tag": kwargs["image_tag"],
            },
        )
        monkeypatch.setattr(runtime.docker_pool, "container_running", lambda name: True)

        record = runtime.docker_pool.ensure_container(
            object_record={"pipeline_nodes": []},
            image_tag="splime-runtime:demo",
            runtime_config={"mode": "docker", "network": "auto"},
        )

        assert "exec_lock" in record
    finally:
        store.close()


def test_docker_pool_idle_eviction_skips_in_use_containers(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    removed: list[str] = []
    try:
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            docker_pool_size=2,
            docker_idle_timeout_seconds=1,
        )
        monkeypatch.setattr(runtime.docker_pool, "remove_container", lambda name: removed.append(name))
        runtime.docker_pool._containers = {
            "busy": {
                "name": "splime-pool-busy",
                "last_used": 1.0,
                "in_use": True,
            },
            "idle": {
                "name": "splime-pool-idle",
                "last_used": 1.0,
                "in_use": False,
            },
        }

        runtime.docker_pool.evict_idle_locked(now=10.0)

        assert removed == ["splime-pool-idle"]
        assert "busy" in runtime.docker_pool._containers
        assert "idle" not in runtime.docker_pool._containers
    finally:
        store.close()


def test_docker_pool_lru_eviction_skips_in_use_containers(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    removed: list[str] = []
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=1)
        monkeypatch.setattr(runtime.docker_pool, "remove_container", lambda name: removed.append(name))
        runtime.docker_pool._containers = {
            "busy": {
                "name": "splime-pool-busy",
                "last_used": 1.0,
                "in_use": True,
            },
            "idle": {
                "name": "splime-pool-idle",
                "last_used": 2.0,
                "in_use": False,
            },
        }

        runtime.docker_pool.evict_excess_locked(reserve=1)

        assert removed == ["splime-pool-idle"]
        assert "busy" in runtime.docker_pool._containers
        assert "idle" not in runtime.docker_pool._containers
    finally:
        store.close()


def test_dockerfile_includes_apt_packages(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={
                "mode": "docker",
                "python": "3.13",
                "apt_packages": ["libgomp1"],
            },
        )
        spec = runtime.docker_environment_manager.build_spec(record)
        dockerfile = runtime.docker_environment_manager._dockerfile(spec)

        assert "apt-get install -y --no-install-recommends libgomp1" in dockerfile
    finally:
        store.close()


def test_docker_ready_record_becomes_absent_when_image_is_missing(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13"},
        )
        manager = runtime.docker_environment_manager
        spec = manager.build_spec(record)
        store.upsert_environment_build(
            spec_hash=spec["spec_hash"],
            base_python=spec["base_python"],
            python_version=spec["python_version"],
            distributions=spec["distributions"],
            runtime_packages=spec["runtime_packages"],
            spec=spec["spec"],
            venv_path=spec["venv_path"],
            python_path=Path(spec["python_path"]),
            install_log_path=spec["install_log_path"],
            status="ready",
            runtime_type="docker",
            image_tag=spec["image_tag"],
            base_image=spec["base_image"],
        )
        monkeypatch.setattr(manager, "_image_exists", lambda image_tag: False)

        status = manager.status_for_object(record)

        assert status["status"] == "absent"
        assert status["error"] == "cached Docker image is missing from local Docker daemon"
    finally:
        store.close()


def test_docker_pull_config_updates_build_command(tmp_path, monkeypatch) -> None:
    store = RegistryStore(tmp_path)
    commands: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13", "pull": True},
        )
        manager = runtime.docker_environment_manager
        spec = manager.build_spec(record)
        monkeypatch.setattr("spl.daemon.docker_environment.shutil.which", lambda _: "/usr/bin/docker")
        monkeypatch.setattr("spl.daemon.docker_environment.subprocess.run", fake_run)

        manager._build_environment(spec)

        assert commands[0] == ["docker", "info"]
        assert commands[1][0:2] == ["docker", "build"]
        assert "--pull=true" in commands[1]
    finally:
        store.close()


def test_docker_run_reports_clear_error_when_executable_is_missing(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False)
        monkeypatch.setattr("spl.daemon.runtime_backend.shutil.which", lambda _: None)

        try:
            runtime.runtime_backends.backend_for({"runtime_config": {"mode": "docker"}}).ensure_ready({})
        except RuntimeError as exc:
            assert "docker executable is not available" in str(exc)
        else:
            raise AssertionError("expected missing docker executable to fail")
    finally:
        store.close()


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        completed = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        )
    except Exception:
        return False
    return completed.returncode == 0


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_docker_runtime_end_to_end_runs_function_in_a_container(tmp_path) -> None:
    """End-to-end: build a Docker image and run an object inside a container.

    Requires a working local Docker (skipped otherwise) and network access to
    pull the base image + pyyaml on first run.  Exercises the full one-shot
    launch path: image build, bind mounts, hardening, the worker protocol, the
    artifact flow, and host-side artifact path rewriting.
    """

    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False)
    try:
        store.register_env("default", sys.executable)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13"},
        )

        build = runtime.docker_environment_manager.ensure_ready(record, wait=True)
        assert build["status"] == "ready", build.get("error")
        assert build["image_tag"].startswith("splime-runtime:")

        started = runtime.start_run(
            "artifact_func",
            source="local",
            report_local_run=False,
            timeout_seconds=600,
        )
        final = _wait_for_run(store, started["id"], timeout_seconds=600)

        assert final["status"] == "succeeded", final.get("error")
        assert final["runtime_backend"] == "docker"
        assert final["image_tag"] == build["image_tag"]
        assert final["result"]["result"] == {"answer": 7}

        artifact_path = Path(final["artifacts_dir"]) / "artifact.txt"
        assert artifact_path.read_text(encoding="utf-8") == "daemon artifact"
        _assert_owner_only(Path(final["run_dir"]))
        _assert_owner_only(Path(final["result_path"]))
        _assert_owner_only(Path(final["artifacts_dir"]))
        _assert_owner_only(artifact_path)
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_object_docker_pipeline_manifest_show_prune_permissions_and_schema(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    try:
        store.register_env("default", sys.executable)
        app = create_app(store, auto_build_envs=False)
        pipeline = _object_docker_manifest_pipeline()
        yaml_path = tmp_path / "object_docker_manifest_pipeline.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        yaml_text = yaml_path.read_text(encoding="utf-8")
        venv_record = app.runtime.register_object(
            "object_docker_manifest_venv",
            "object_docker_manifest_pipeline",
            "default",
            yaml_text=yaml_text,
        )
        docker_record = app.runtime.register_object(
            "object_docker_manifest_docker",
            "object_docker_manifest_pipeline",
            "default",
            yaml_text=yaml_text,
            runtime_config={"mode": "docker", "python": "3.13"},
        )
        _mark_object_environment_ready(app.runtime, venv_record)
        build = app.runtime.docker_environment_manager.ensure_ready(docker_record, wait=True)
        assert build["status"] == "ready", build.get("error")

        status, venv_started = _post_json_from_app(
            app,
            "/runs",
            {
                "object": "object_docker_manifest_venv",
                "output": "consumer",
                "source": "local",
                "keep": True,
                "kwargs": {"seed": "seed"},
            },
        )
        assert status == 202
        venv_final = _wait_for_run(store, venv_started["id"], timeout_seconds=60)
        assert venv_final["status"] == "succeeded", venv_final.get("error")

        docker_runs = []
        for seed in ("seed", "again"):
            status, started = _post_json_from_app(
                app,
                "/runs",
                {
                    "object": "object_docker_manifest_docker",
                    "output": "consumer",
                    "source": "local",
                    "keep": True,
                    "timeout_seconds": 600,
                    "kwargs": {"seed": seed},
                },
            )
            assert status == 202
            final = _wait_for_run(store, started["id"], timeout_seconds=600)
            assert final["status"] == "succeeded", final.get("error")
            assert final["runtime_backend"] == "docker"
            assert final["result"]["result"] == {"default": f"consumed:{seed}"}
            docker_runs.append(final)

        first, second = docker_runs
        assert first["run_dir"] != second["run_dir"]
        assert len(_worker_manifest_paths(first["run_dir"])) == 1
        assert len(_worker_manifest_paths(second["run_dir"])) == 1
        manifest_dir = _worker_manifest_dir(first["run_dir"])
        manifest_path = manifest_dir / "manifest.json"
        worker_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert worker_manifest["status"] == "succeeded"
        assert _manifest_key_paths(venv_final["manifest"]) == _manifest_key_paths(first["manifest"])

        consumer = _manifest_node_by_alias(first["manifest"], "consumer")
        producer = _manifest_node_by_alias(first["manifest"], "producer")
        assert consumer["status"] == "succeeded"
        assert producer["status"] == "succeeded"
        [edge] = first["manifest"]["edges"]
        assert edge["adapter"]["save"]["tag"] == "txt"
        artifact_uri = edge["artifact"]["ref"]["uri"]
        assert not Path(artifact_uri).is_absolute()
        edge_artifact_path = manifest_dir / artifact_uri
        assert edge_artifact_path.read_text(encoding="utf-8") == "seed"

        status, observed = _json_from_app(app, f"/runs/{first['id']}")
        assert status == 200
        assert {"alias": "producer", "node_id": producer["id"], "status": "succeeded"} in observed["run_progress"][
            "nodes"
        ]
        assert {"alias": "consumer", "node_id": consumer["id"], "status": "succeeded"} in observed["run_progress"][
            "nodes"
        ]
        assert observed["run_progress"]["edge_adapters"]
        assert observed["run_progress"]["node_runtimes"]

        status, shown = _json_from_app(app, f"/runs/{first['id']}?view=show")
        assert status == 200
        assert shown["edge_adapters"]
        assert shown["node_runtimes"]
        assert _manifest_node_by_alias(shown["manifest"], "consumer")["status"] == "succeeded"

        _assert_owner_only(Path(first["run_dir"]))
        _assert_owner_only(Path(first["result_path"]))
        if Path(first["artifacts_dir"]).exists():
            _assert_owner_only(Path(first["artifacts_dir"]))
        _assert_owner_only(Path(first["run_dir"]) / "input.json")
        _assert_owner_only(manifest_dir)
        _assert_owner_only(manifest_path)
        _assert_owner_only(edge_artifact_path)

        status, preview = _post_json_from_app(app, "/runs/prune", {"run_id": first["id"], "dry_run": True})
        assert status == 200
        assert preview["count"] == 1
        assert preview["pruned"][0]["disk_size_bytes"] > 0
        status, pruned = _post_json_from_app(app, "/runs/prune", {"run_id": first["id"]})
        assert status == 200
        assert pruned["count"] == 1
        assert not Path(first["run_dir"]).exists()
        with pytest.raises(KeyError):
            store.get_run(first["id"])
        assert Path(second["run_dir"]).exists()
    finally:
        _shutdown_app(app)
        store.close()


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_object_docker_pipeline_resume_via_http(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    app = None
    run_ids: list[str] = []
    try:
        store.register_env("default", sys.executable)
        app = create_app(store, auto_build_envs=False)
        pipeline = _object_docker_resume_pipeline()
        yaml_path = tmp_path / "object_docker_resume_pipeline.yaml"
        spl_export_to_file(yaml_path, [pipeline])
        record = app.runtime.register_object(
            "object_docker_resume_pipeline",
            "object_docker_resume_pipeline",
            "default",
            yaml_text=yaml_path.read_text(encoding="utf-8"),
            runtime_config={"mode": "docker", "python": "3.13", "base_image": "python:3.13-slim"},
        )
        build = app.runtime.docker_environment_manager.ensure_ready(record, wait=True)
        assert build["status"] == "ready", build.get("error")

        status, started = _post_json_from_app(
            app,
            "/runs",
            {
                "object": "object_docker_resume_pipeline",
                "output": "consumer",
                "source": "local",
                "keep": True,
                "timeout_seconds": 600,
                "kwargs": {"seed": "seed", "should_fail": True},
            },
        )
        assert status == 202
        failed = _wait_for_run(store, started["id"], timeout_seconds=600)
        assert failed["status"] == "failed"
        assert _manifest_node_by_alias(failed["manifest"], "producer")["status"] == "succeeded"
        assert _manifest_node_by_alias(failed["manifest"], "consumer")["status"] == "failed"

        status, resumed = _post_json_from_app(
            app,
            f"/runs/{failed['id']}/resume",
            {
                "from": "consumer",
                "output": "consumer",
                "timeout_seconds": 600,
                "kwargs": {"should_fail": False},
                "keep": True,
            },
        )
        assert status == 202
        child = _wait_for_run(store, resumed["id"], timeout_seconds=600)

        assert child["status"] == "succeeded", child.get("error")
        assert child["runtime_backend"] == "docker"
        assert child["parent_run_id"] == failed["id"]
        assert child["manifest"]["parent_run_id"] == failed["id"]
        assert child["result"]["result"] == {"default": "resumed:seed"}
        run_ids = [failed["id"], child["id"]]
        parent_producer = _manifest_node_by_alias(failed["manifest"], "producer")
        child_producer = _manifest_node_by_alias(child["manifest"], "producer")
        assert _manifest_node_by_alias(child["manifest"], "producer")["status"] == "frozen"
        assert child_producer["outputs"] == parent_producer["outputs"]
        assert _manifest_node_by_alias(child["manifest"], "consumer")["status"] == "succeeded"
    finally:
        _shutdown_app(app)
        store.close()
    assert _docker_container_names_for_run_ids(run_ids) == []


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_daemon_node_docker_runtime_end_to_end_and_resume_override(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False)
    try:
        store.register_env("default", sys.executable)

        tagged_pipeline = _node_docker_pipeline("daemon_node_docker_tagged")
        tagged_yaml_path = tmp_path / "daemon_node_docker_tagged.yaml"
        spl_export_to_file(tagged_yaml_path, [tagged_pipeline])
        tagged_record = runtime.register_object(
            "daemon_node_docker_tagged",
            "daemon_node_docker_tagged",
            "default",
            yaml_text=tagged_yaml_path.read_text(encoding="utf-8"),
        )
        _mark_object_environment_ready(runtime, tagged_record)

        started = runtime.start_run(
            "daemon_node_docker_tagged",
            output="consumer",
            source="local",
            report_local_run=False,
            timeout_seconds=600,
            keep=True,
        )
        final = _wait_for_run(store, started["id"], timeout_seconds=600)

        assert final["status"] == "succeeded", final.get("error")
        assert final["result"]["result"] == {"default": "docker:seed"}
        consumer = _manifest_node_by_alias(final["manifest"], "consumer")
        assert consumer["runtime"]["name"] == "docker"
        assert consumer["runtime"]["source"] == "node-tag"
        image_tag = consumer["runtime"]["resolved"]["image_tag"]
        assert image_tag.startswith("splime-runtime:")
        assert "python" not in consumer["runtime"]["resolved"]
        shown = store.show_run(final["id"])
        assert _manifest_node_by_alias(shown["manifest"], "consumer")["runtime"]["resolved"]["image_tag"] == image_tag
        assert any(
            runtime.get("alias") == "consumer" and runtime.get("resolved") == {"image_tag": image_tag}
            for runtime in shown["node_runtimes"]
        )

        resume_pipeline = _node_docker_pipeline("daemon_node_docker_resume", tag_consumer=False)
        resume_yaml_path = tmp_path / "daemon_node_docker_resume.yaml"
        spl_export_to_file(resume_yaml_path, [resume_pipeline])
        resume_record = runtime.register_object(
            "daemon_node_docker_resume",
            "daemon_node_docker_resume",
            "default",
            yaml_text=resume_yaml_path.read_text(encoding="utf-8"),
        )
        _mark_object_environment_ready(runtime, resume_record)

        parent = runtime.start_run(
            "daemon_node_docker_resume",
            output="consumer",
            source="local",
            report_local_run=False,
            timeout_seconds=600,
            keep=True,
        )
        parent_final = _wait_for_run(store, parent["id"], timeout_seconds=600)
        assert parent_final["status"] == "succeeded", parent_final.get("error")

        resumed = runtime.resume_run(
            parent["id"],
            from_="consumer",
            output="consumer",
            report_local_run=False,
            timeout_seconds=600,
            runtimes={"consumer": "docker"},
            keep=True,
        )
        child = _wait_for_run(store, resumed["id"], timeout_seconds=600)

        assert child["status"] == "succeeded", child.get("error")
        assert child["result"]["result"] == {"default": "docker:seed"}
        resumed_consumer = _manifest_node_by_alias(child["manifest"], "consumer")
        assert resumed_consumer["runtime"]["name"] == "docker"
        assert resumed_consumer["runtime"]["source"] == "run-override"
        assert resumed_consumer["runtime"]["resolved"]["image_tag"].startswith("splime-runtime:")
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.docker
@pytest.mark.skipif(not _docker_available(), reason="Docker is not available")
def test_docker_runtime_reuses_warm_pool_container(tmp_path) -> None:
    """A pooled run uses `docker exec` into a reusable warm container."""

    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False, docker_pool_size=2)
    try:
        store.register_env("default", sys.executable)
        record = runtime.register_object(
            "artifact_func",
            "artifact_func",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13"},
        )
        runtime.docker_environment_manager.ensure_ready(record, wait=True)

        results = []
        for _ in range(2):
            started = runtime.start_run(
                "artifact_func",
                source="local",
                report_local_run=False,
                timeout_seconds=600,
            )
            results.append(_wait_for_run(store, started["id"], timeout_seconds=600))

        assert [r["status"] for r in results] == ["succeeded", "succeeded"]
        # Exactly one warm container should be backing both runs.
        assert len(runtime.docker_pool) == 1
        for run in results:
            assert run["result"]["result"] == {"answer": 7}
    finally:
        runtime.shutdown()
        store.close()


def test_prepare_remote_run_artifacts_splits_inline_and_direct_uploads(
    tmp_path,
    monkeypatch,
) -> None:
    class UploadingServerClient:
        uploads: list[dict[str, Any]] = []

        def __init__(
            self,
            base_url: str,
            machine_token: str,
            *,
            user_token: str | None = None,
        ):
            self.base_url = base_url
            self.machine_token = machine_token
            self.user_token = user_token

        def upload_artifact(
            self,
            run_id: str,
            name: str,
            path: str | Path,
        ) -> dict[str, Any]:
            payload = Path(path).read_bytes()
            record = {
                "id": f"artifact-{name}",
                "run_id": run_id,
                "name": name,
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
            self.uploads.append(record)
            return record

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES", 8)
    try:
        runtime = DaemonRuntime(store, server_client_factory=UploadingServerClient)
        connection = store.save_server_connection(
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
        artifacts_dir = tmp_path / "artifacts"
        artifacts_dir.mkdir()
        small = artifacts_dir / "small.txt"
        large = artifacts_dir / "large.bin"
        small.write_bytes(b"tiny")
        large.write_bytes(b"larger-than-inline")

        prepared = runtime._prepare_remote_run_artifacts(
            connection["id"],
            "remote-run-1",
            {"artifacts_dir": str(artifacts_dir)},
        )

        assert [item["name"] for item in prepared] == ["large.bin", "small.txt"]
        large_item, small_item = prepared
        assert large_item["transfer_mode"] == "direct_upload"
        assert large_item["uploaded"] is True
        assert "data_base64" not in large_item
        assert small_item["transfer_mode"] == "inline_base64"
        assert base64.b64decode(small_item["data_base64"]) == b"tiny"
        assert UploadingServerClient.uploads == [
            {
                "id": "artifact-large.bin",
                "run_id": "remote-run-1",
                "name": "large.bin",
                "size": len(b"larger-than-inline"),
                "sha256": hashlib.sha256(b"larger-than-inline").hexdigest(),
            }
        ]
    finally:
        store.close()


def test_prepare_remote_run_artifacts_rejects_mismatched_direct_upload(
    tmp_path,
    monkeypatch,
) -> None:
    class MismatchedServerClient:
        def __init__(
            self,
            base_url: str,
            machine_token: str,
            *,
            user_token: str | None = None,
        ):
            pass

        def upload_artifact(
            self,
            run_id: str,
            name: str,
            path: str | Path,
        ) -> dict[str, Any]:
            return {
                "id": f"artifact-{name}",
                "run_id": run_id,
                "name": name,
                "size": Path(path).stat().st_size,
                "sha256": "0" * 64,
            }

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", MismatchedServerClient)
    monkeypatch.setattr(daemon_server, "DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES", 0)
    try:
        runtime = DaemonRuntime(store)
        connection = store.save_server_connection(
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
        artifacts_dir = tmp_path / "artifacts"
        artifacts_dir.mkdir()
        (artifacts_dir / "model.bin").write_bytes(b"weights")

        try:
            runtime._prepare_remote_run_artifacts(
                connection["id"],
                "remote-run-1",
                {"artifacts_dir": str(artifacts_dir)},
            )
        except RuntimeError as exc:
            assert "checksum mismatch" in str(exc)
        else:
            raise AssertionError("mismatched upload should fail before run_update")
    finally:
        store.close()


def test_pipeline_internal_function_runs_in_parent_environment(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store)

        record = runtime.register_object(
            "demo_pipeline",
            "demo_pipeline",
            "default",
            yaml_text=PIPELINE_WITH_INTERNAL_FUNCTION_YAML,
        )
        build = _mark_object_environment_ready(runtime, record)

        started = runtime.start_run(
            "demo_pipeline",
            kwargs={"a": 2, "b": 5},
            function="inner_add",
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["entrypoint"] == "inner_add"
        assert final["input"]["function"] == "inner_add"
        assert final["env_build_hash"] == build["spec_hash"]
        assert final["result"]["result"] == 7
    finally:
        store.close()


def test_remote_import_mirrors_server_versions_with_source_identity(
    tmp_path,
    monkeypatch,
) -> None:
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
                "version": 2,
                "version_id": "remote-version-2",
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
                    "description": "first",
                    "version_label": "v1",
                    "yaml": REMOTE_FUNCTION_YAML,
                },
                {
                    "id": "remote-object-1",
                    "owner_id": "owner-1",
                    "name": "demo_obj",
                    "version": 2,
                    "version_id": "remote-version-2",
                    "entrypoint": "demo_obj",
                    "env": "default",
                    "description": "second",
                    "version_label": "v2",
                    "yaml": REMOTE_FUNCTION_YAML,
                },
            ]

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", ImportServerClient)
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
        runtime = DaemonRuntime(store)
        _mark_current_server_channel_live(runtime)

        imported = runtime.import_server_object("demo_obj")

        assert imported["refreshed"] is True
        assert imported["current_version"]["name"] == "demo_obj"
        assert imported["current_version"]["version"] == 1
        assert len(imported["versions"]) == 2

        current = imported["current_version"]
        assert current["local_registry_name"] == "demo_obj"
        assert current["owner_id"] == "owner-1"
        assert current["library"] == "default"
        assert current["source_owner_id"] == "owner-1"
        assert current["source_object_id"] == "remote-object-1"
        assert current["source_object_name"] == "demo_obj"
        assert current["remote_identity"]["source_version_id"] == "remote-version-1"
        assert current["remote_name"] == "demo_obj"
    finally:
        store.close()


def test_remote_node_can_target_pipeline_internal_function(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
        calls: dict[str, Any] = {}

        def resolve_remote_signature(ref: dict[str, Any]) -> dict[str, Any]:
            calls["resolve_ref"] = ref
            return {
                "id": "remote-object-1",
                "version_id": "remote-version-1",
                "kind": "function",
                "function": "inner_add",
                "outputs": [{"name": "default", "selector": None}],
                "remote_ref": {"owner_id": "owner-1", "library": "default"},
                "execution": {"default_machine_id": "machine-1"},
            }

        def start_remote_run(object_name: str, **kwargs: Any) -> dict[str, Any]:
            calls["start"] = {"object_name": object_name, **kwargs}
            return {"id": "remote-run-1"}

        def wait_server_run(run_id: str, **kwargs: Any) -> dict[str, Any]:
            calls["wait"] = {"run_id": run_id, **kwargs}
            return {"status": "succeeded", "result": {"result": 12}}

        monkeypatch.setattr(runtime, "resolve_remote_signature", resolve_remote_signature)
        monkeypatch.setattr(runtime, "start_remote_run", start_remote_run)
        monkeypatch.setattr(runtime, "_wait_server_run", wait_server_run)

        result = runtime.run_remote_node(
            {
                "url": "https://splime.io/api",
                "name": "demo_pipeline::inner_add",
                "version": "latest",
            },
            kwargs={"a": 5, "b": 7},
        )

        assert result["value"] == 12
        assert result["run_id"] == "remote-run-1"
        assert result["status"] == "succeeded"
        assert result["run"] == {"status": "succeeded", "result": {"result": 12}}
        assert result["payload"] == {"result": 12}
        assert result["artifacts"] == {}
        assert calls["resolve_ref"]["object_name"] == "demo_pipeline"
        assert calls["resolve_ref"]["function"] == "inner_add"
        assert calls["start"]["object_name"] == "remote-object-1"
        assert calls["start"]["function"] == "inner_add"
        assert calls["start"]["kwargs"] == {"a": 5, "b": 7}
    finally:
        store.close()


def test_remote_import_auto_registers_missing_server_env(
    tmp_path,
    monkeypatch,
) -> None:
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
                "env": "spl_core",
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
                    "env": "spl_core",
                    "description": "first",
                    "version_label": "v1",
                    "yaml": REMOTE_FUNCTION_YAML,
                },
            ]

    store = RegistryStore(tmp_path)
    monkeypatch.setattr(daemon_server, "ServerClient", ImportServerClient)
    try:
        default_env = store.register_env("default", sys.executable)
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
        runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
        _mark_current_server_channel_live(runtime)

        imported = runtime.import_server_object("demo_obj")

        assert imported["refreshed"] is True
        assert imported["current_version"]["env"] == "spl_core"
        assert store.get_env("spl_core")["python"] == default_env["python"]
    finally:
        store.close()


def _remote_version(
    name: str,
    *,
    version: int,
    value: int,
    owner_id: str = "owner-1",
    library: str = "default",
    object_id: str = "remote-object-1",
    version_id: str | None = None,
    content_hash: str | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "id": object_id,
        "owner_id": owner_id,
        "library": library,
        "name": name,
        "version": version,
        "version_id": version_id or f"{object_id}-version-{version}",
        "entrypoint": name,
        "env": "default",
        "description": f"{name} v{version}",
        "version_label": f"v{version}",
        "yaml": _remote_function_yaml(name, value),
    }
    if content_hash is not None:
        record["content_hash"] = content_hash
    return record


class _PullServerClient:
    def __init__(
        self,
        versions: list[dict[str, Any]],
        *,
        catalog: list[dict[str, Any]] | None = None,
    ) -> None:
        self.versions = versions
        self.catalog = catalog
        self.list_objects_calls: list[dict[str, Any]] = []
        self.get_object_calls: list[dict[str, Any]] = []
        self.list_object_versions_calls: list[dict[str, Any]] = []

    @staticmethod
    def _public_record(record: dict[str, Any], *, include_yaml: bool) -> dict[str, Any]:
        payload = dict(record)
        if not include_yaml:
            payload.pop("yaml", None)
        return payload

    @staticmethod
    def _matches(
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

    def _matching_versions(
        self,
        name_or_id: str,
        *,
        owner_id: str | None,
        library: str | None,
    ) -> list[dict[str, Any]]:
        return [
            record for record in self.versions if self._matches(record, name_or_id, owner_id=owner_id, library=library)
        ]

    def _latest_catalog_records(self) -> list[dict[str, Any]]:
        latest: dict[tuple[str, str, str], dict[str, Any]] = {}
        for record in self.versions:
            key = (
                str(record.get("owner_id") or ""),
                str(record.get("library") or ""),
                str(record.get("name") or ""),
            )
            if key not in latest or int(record.get("version") or 0) > int(latest[key].get("version") or 0):
                latest[key] = record
        return list(latest.values())

    def list_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        self.list_objects_calls.append({"owner_id": owner_id, "library": library, "compact": compact})
        records = self.catalog if self.catalog is not None else self._latest_catalog_records()
        return [
            self._public_record(record, include_yaml=False)
            for record in records
            if (owner_id is None or record.get("owner_id") == owner_id)
            and (library is None or record.get("library") == library)
        ]

    def get_object(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        self.get_object_calls.append(
            {
                "name_or_id": name_or_id,
                "version": version,
                "include_yaml": include_yaml,
                "owner_id": owner_id,
                "library": library,
            }
        )
        matches = self._matching_versions(name_or_id, owner_id=owner_id, library=library)
        if version is not None:
            matches = [record for record in matches if int(record.get("version") or 0) == int(version)]
        if not matches:
            raise KeyError(f"server object is not registered: {name_or_id}")
        record = max(matches, key=lambda item: int(item.get("version") or 0))
        return self._public_record(record, include_yaml=include_yaml)

    def list_object_versions(
        self,
        name_or_id: str,
        *,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        self.list_object_versions_calls.append(
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
                self._matching_versions(name_or_id, owner_id=owner_id, library=library),
                key=lambda item: int(item.get("version") or 0),
                reverse=True,
            )
        ]


def test_pull_server_object_by_bare_name_imports_latest_skips_repeat_and_runs_locally(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    try:
        store.register_env("default", sys.executable)
        _save_connected_server_connection(store)
        server = _PullServerClient(
            [
                _remote_version(
                    "remote_calc",
                    version=2,
                    value=7,
                    object_id="remote-object-calc",
                    version_id="remote-version-calc-2",
                )
            ]
        )
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=lambda *args, **kwargs: server,
        )
        _mark_current_server_channel_live(runtime)

        receipt = runtime.pull_server_object("remote_calc")
        repeat = runtime.pull_server_object("remote_calc")
        record = store.get_object("remote_calc", owner_id="owner-1", library="default")
        build = _mark_object_environment_ready(runtime, record)
        started = runtime.start_run(
            "remote_calc",
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])

        assert receipt == {
            "pulled": ["owner-1/default/remote_calc@v2"],
            "skipped": [],
            "failed": [],
            "ambiguous_names": [],
        }
        assert repeat["pulled"] == []
        assert repeat["skipped"] == ["owner-1/default/remote_calc@v2"]
        assert repeat["failed"] == []
        assert repeat["ambiguous_names"] == []
        assert server.list_objects_calls == [
            {"owner_id": None, "library": None, "compact": True},
            {"owner_id": None, "library": None, "compact": True},
        ]
        assert record["origin"] == "server"
        assert record["source_owner_id"] == "owner-1"
        assert record["source_object_id"] == "remote-object-calc"
        assert record["remote_identity"]["source_version_id"] == "remote-version-calc-2"
        assert final["status"] == "succeeded"
        assert final["env_build_hash"] == build["spec_hash"]
        assert final["result"]["result"] == 7
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_pull_server_object_scoped_all_versions_reports_new_local_ambiguity(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    try:
        store.register_env("default", sys.executable)
        server = _PullServerClient(
            [
                _remote_version(
                    "demo_obj",
                    version=1,
                    value=1,
                    library="risk",
                    object_id="remote-object-risk",
                    version_id="remote-version-risk-1",
                ),
                _remote_version(
                    "demo_obj",
                    version=2,
                    value=2,
                    library="risk",
                    object_id="remote-object-risk",
                    version_id="remote-version-risk-2",
                ),
            ]
        )
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=lambda *args, **kwargs: server,
        )
        runtime.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-1",
            library="default",
        )
        _save_connected_server_connection(store)
        _mark_current_server_channel_live(runtime)

        receipt = runtime.pull_server_object(
            "demo_obj",
            owner_id="owner-1",
            library="risk",
            all_versions=True,
        )
        versions = store.list_object_versions("demo_obj", owner_id="owner-1", library="risk")

        assert receipt == {
            "pulled": ["owner-1/risk/demo_obj@v1", "owner-1/risk/demo_obj@v2"],
            "skipped": [],
            "failed": [],
            "ambiguous_names": ["demo_obj"],
        }
        assert server.list_objects_calls == []
        assert server.list_object_versions_calls == [
            {
                "name_or_id": "demo_obj",
                "include_yaml": False,
                "owner_id": "owner-1",
                "library": "risk",
            }
        ]
        assert [item["version"] for item in versions] == [2, 1]
        assert {item["origin"] for item in versions} == {"server"}
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_pull_server_object_ambiguous_bare_name_lists_server_candidates(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    try:
        _save_connected_server_connection(store)
        server = _PullServerClient(
            [],
            catalog=[
                _remote_version("order_pipeline", version=10, value=10, library="default"),
                _remote_version("order_pipeline", version=1, value=1, library="risk"),
            ],
        )
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=lambda *args, **kwargs: server,
        )
        _mark_current_server_channel_live(runtime)

        with pytest.raises(ValueError) as exc_info:
            runtime.pull_server_object("order_pipeline")

        message = str(exc_info.value)
        assert "'order_pipeline' is not registered locally" in message
        assert "default (owner owner-1, v10)" in message
        assert "risk (owner owner-1, v1)" in message
        assert "client.pull('order_pipeline', library='...')" in message
        assert server.get_object_calls == []
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_pull_server_object_requires_server_connection(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    try:
        runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())

        with pytest.raises(KeyError) as exc_info:
            runtime.pull_server_object("demo_obj")

        message = str(exc_info.value)
        assert "pull requires a server connection" in message
        assert "no server connection" in message
        assert "connect_server" in message
        assert "client.pull" in message
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def test_pull_server_object_does_not_overwrite_local_origin_on_matching_content(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    try:
        store.register_env("default", sys.executable)
        server_holder: dict[str, _PullServerClient] = {}
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=lambda *args, **kwargs: server_holder["server"],
        )
        local = runtime.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-1",
            library="default",
        )
        _save_connected_server_connection(store)
        _mark_current_server_channel_live(runtime)
        server_holder["server"] = _PullServerClient(
            [
                _remote_version(
                    "demo_obj",
                    version=3,
                    value=1,
                    object_id="remote-object-local-content",
                    version_id="remote-version-local-content-3",
                    content_hash=local["content_hash"],
                )
            ]
        )

        receipt = runtime.pull_server_object("demo_obj", owner_id="owner-1", library="default")
        current = store.get_object("demo_obj", owner_id="owner-1", library="default")

        assert receipt == {
            "pulled": [],
            "skipped": ["owner-1/default/demo_obj@v3"],
            "failed": [],
            "ambiguous_names": [],
        }
        assert current["origin"] == "local"
        assert current["remote_identity"]["source_version_id"] == "remote-version-local-content-3"
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


def _existing_python(path: Path) -> str:
    path.write_text("", encoding="utf-8")
    return str(path)


def test_server_origin_resolver_uses_local_env_by_name_before_provenance(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        local_python = store.register_env("spl_core", _existing_python(tmp_path / "local-python"))["python"]
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = {
            "origin": "server",
            "env": "spl_core",
            "env_python": sys.executable,
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] == local_python
    finally:
        store.close()


def test_server_origin_resolver_falls_back_to_default_env_when_named_env_is_missing(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        default_python = store.register_env("default", _existing_python(tmp_path / "default-python"))["python"]
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = {
            "origin": "server",
            "env": "spl_core",
            "env_python": str(tmp_path / "author-python"),
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] == default_python
    finally:
        store.close()


def test_server_origin_resolver_falls_back_to_daemon_python_without_local_envs(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = {
            "origin": "server",
            "env": "spl_core",
            "env_python": str(tmp_path / "author-python"),
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] == str(Path(sys.executable).expanduser().absolute())
    finally:
        store.close()


def test_local_origin_resolver_uses_stored_python_when_live(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        local_python = _existing_python(tmp_path / "local-python")
        runtime = DaemonRuntime(store, auto_build_envs=False)
        record = {
            "origin": "local",
            "env": "spl_core",
            "env_python": local_python,
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] == str(Path(local_python).expanduser().absolute())
    finally:
        store.close()


def test_local_origin_dead_python_fails_without_default_substitution(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        default_env = store.register_env("default", sys.executable)
        runtime = DaemonRuntime(store, auto_build_envs=False)
        missing_python = str(tmp_path / "missing-python")
        record = {
            "origin": "local",
            "env": "default",
            "env_python": missing_python,
            "distributions": [],
        }

        spec = runtime.environment_manager.build_spec(record)

        assert spec["base_python"] != default_env["python"]
        assert spec["base_python"] == str(Path(missing_python).expanduser().absolute())
        with pytest.raises(EnvironmentBuildError):
            runtime.environment_manager.ensure_ready(record, wait=True)
    finally:
        store.close()


def test_server_mirror_reregister_keeps_env_python_provenance_and_resolves_locally(
    tmp_path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        default_env = store.register_env("default", sys.executable)
        store.register_env("default1", sys.executable)
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
        )
        first = runtime.register_object(
            "demo_obj",
            "demo_obj",
            "default1",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-1",
            library="default",
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )
        author_python = str(tmp_path / "author-python")
        with store._lock, store._conn:  # noqa: SLF001 - regression seeds server provenance.
            store._conn.execute(
                "UPDATE envs SET python = ? WHERE name = ?",
                (str(tmp_path / "missing-local-python"), "default1"),
            )
            store._conn.execute(
                "UPDATE object_versions SET env_python = ? WHERE id = ?",
                (author_python, first["version_id"]),
            )
        runtime._ensure_server_object_envs([{"env": "default1"}])

        mirrored = runtime.register_object(
            "demo_obj",
            "demo_obj",
            "default1",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-1",
            library="default",
            origin="server",
            remote_owner_id="owner-1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            source_object_name="demo_obj",
        )
        spec = runtime.environment_manager.build_spec(mirrored)

        assert mirrored["version_id"] == first["version_id"]
        assert mirrored["env_python"] == author_python
        assert spec["base_python"] == default_env["python"]
    finally:
        store.close()


def test_server_origin_interpreter_substitution_is_logged_and_reported(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        local_env = store.register_env("spl_core", sys.executable)
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
        )
        record = runtime.register_object(
            "demo_obj",
            "demo_obj",
            "spl_core",
            yaml_text=REMOTE_FUNCTION_YAML,
            owner_id="owner-1",
            library="default",
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
        record = store.get_object_version(record["version_id"])
        _mark_object_environment_ready(runtime, record)

        caplog.set_level(logging.INFO, logger="spl.daemon.server")
        started = runtime.start_run(
            "demo_obj",
            source="local",
            object_version_id=record["version_id"],
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])

        assert final["status"] == "succeeded"
        assert final["interpreter_substitution"] is not None
        assert final["interpreter_substitution"]["authored_python"] == author_python
        assert final["interpreter_substitution"]["resolved_python"] == local_env["python"]
        assert final["interpreter_substitution"]["reason"] == "local_env"
        records = [
            log_record
            for log_record in caplog.records
            if getattr(log_record, "spl_event", None) == "interpreter_substitution"
        ]
        assert len(records) == 1
        payload = records[0].interpreter_substitution
        assert payload["object"] == "demo_obj"
        assert payload["version_id"] == record["version_id"]
        assert payload["authored_python"] == author_python
        assert payload["resolved_python"] == local_env["python"]
    finally:
        store.close()
