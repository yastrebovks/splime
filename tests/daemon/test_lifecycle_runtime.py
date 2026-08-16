from __future__ import annotations

from pathlib import Path
import sys
import threading
import time
from typing import Any, Iterator

import pytest

from spl.daemon.guarded_local_run import (
    GuardedInput,
    GuardedLocalRunAdmission,
    GuardedLocalRunError,
    GuardedObjectReference,
)
from spl.daemon.home_lock import DaemonHomeLock
from spl.daemon.lifecycle import LifecycleAdmissionClosed
from spl.daemon.lifecycle_startup import FileIdentity, StartupBinding
from spl.daemon.remote_client import ServerClientError
from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore


FUNCTION_YAML = """\
- !DFunction
  name: fit
  inputs:
  - name: max_depth
    type: int
  outputs:
  - name: default
    type: int
  body: |-
    return max_depth
"""
SUPERVISOR_PROOF = "supervisor-proof-value"


@pytest.fixture
def runtime(tmp_path: Path) -> Iterator[DaemonRuntime]:
    lock = DaemonHomeLock(tmp_path / "daemon-home")
    identity = lock.acquire()
    store = RegistryStore(lock.home)
    store.register_env("default", sys.executable)
    binding = StartupBinding(
        supervisor_id="supervisor-1",
        spawn_nonce="spawn-nonce-value",
        supervisor_proof=SUPERVISOR_PROOF,
        executable=FileIdentity("sha256:" + "a" * 64, 1, 2, 501, 0o755, "sha256:" + "b" * 64),
        home=FileIdentity("sha256:" + "c" * 64, 3, 4, 501, 0o700, None),
        release_version="0.4.6",
    )
    daemon = DaemonRuntime(
        store,
        auto_build_envs=False,
        daemon_identity=identity,
        daemon_home_lock=lock,
        startup_binding=binding,
        startup_supervisor_proof=SUPERVISOR_PROOF,
    )
    try:
        yield daemon
    finally:
        daemon.shutdown()
        store.close()
        lock.release()


def _drain_request(runtime: DaemonRuntime) -> dict[str, Any]:
    identity = runtime.daemon_identity
    assert identity is not None
    return {
        "schema": "spl.daemon.lifecycle-drain-request",
        "schema_version": 1,
        "expected_instance_id": identity.instance_id,
        "expected_generation": identity.generation,
        "expected_admission_revision": runtime.lifecycle.admission_revision,
        "request_id": "request-1",
        "mode": "when_idle",
        "deadline_ms": 30_000,
    }


def test_legacy_run_admission_is_retained_and_new_root_mutations_close_during_drain(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = runtime.register_object(
        "fit_xgboost",
        "fit",
        "default",
        yaml_text=FUNCTION_YAML,
    )
    monkeypatch.setattr(runtime, "_start_run_thread", lambda _run_id, _report: None)
    state = runtime.start_run(
        "fit_xgboost",
        kwargs={"max_depth": 4},
        source="local",
        object_version_id=record["version_id"],
    )

    receipt = runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()
    assert receipt["blockers"]["local_run"] == 1
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["local_run"] == 1

    with pytest.raises(LifecycleAdmissionClosed):
        runtime.start_run("fit_xgboost", kwargs={"max_depth": 5}, source="local")
    with pytest.raises(LifecycleAdmissionClosed):
        runtime.register_env("new-env", sys.executable)
    with pytest.raises(LifecycleAdmissionClosed):
        runtime.register_object("other", "fit", "default", yaml_text=FUNCTION_YAML)

    runtime._release_lifecycle_run_lease(str(state["id"]))


def test_environment_build_is_visible_until_background_build_finishes(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = runtime.register_object(
        "fit_xgboost",
        "fit",
        "default",
        yaml_text=FUNCTION_YAML,
    )
    started = threading.Event()
    release = threading.Event()

    def wait_for_release(_spec: dict[str, Any]) -> None:
        started.set()
        assert release.wait(2)

    monkeypatch.setattr(runtime.environment_manager, "_build_environment", wait_for_release)
    runtime.environment_manager.ensure_ready(record, wait=False)
    assert started.wait(1)
    status = runtime.lifecycle.status_document()
    assert status["admission"]["new_root_work"] == "open"

    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["environment_build"] == 1
    release.set()
    for _ in range(100):
        if runtime.lifecycle.status_document()["drain"]["blockers"]["environment_build"] == 0:
            break
        time.sleep(0.01)
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["environment_build"] == 0


def test_terminal_commit_is_a_named_descendant_blocker(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = runtime.lifecycle.reserve_root("local_run", queued=False)
    runtime._bind_lifecycle_run_lease("run-1", root)
    entered = threading.Event()
    release = threading.Event()

    def terminal(_run_id: str, *, report_local_run: bool, **_changes: Any) -> None:
        del report_local_run
        entered.set()
        assert release.wait(2)

    monkeypatch.setattr(runtime, "_update_local_run_terminal_admitted", terminal)
    thread = threading.Thread(
        target=runtime._update_local_run_terminal,
        kwargs={"run_id": "run-1", "report_local_run": True, "status": "succeeded"},
    )
    thread.start()
    assert entered.wait(1)
    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["terminal_commit"] == 1
    release.set()
    thread.join(timeout=2)
    runtime._release_lifecycle_run_lease("run-1")


def test_guarded_run_is_retained_as_its_exact_blocker_until_terminal_work(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = runtime.register_object(
        "fit_xgboost",
        "fit",
        "default",
        yaml_text=FUNCTION_YAML,
    )
    identity = runtime.daemon_identity
    assert identity is not None
    admission = GuardedLocalRunAdmission(
        request_id="guarded-request-1",
        daemon_instance_id=identity.instance_id,
        daemon_generation=identity.generation,
        object=GuardedObjectReference(
            owner_id=str(record["owner_id"]),
            library=str(record["library"]),
            object_name=str(record["name"]),
            object_id=str(record["id"]),
            expected_current_version_id=str(record["current_version_id"]),
            selected_version_id=str(record["version_id"]),
            selected_version=int(record["version"]),
            function="fit",
            origin=str(record["origin"]),
        ),
        content_hash=f"sha256:{record['content_hash']}",
        inputs=(GuardedInput(name="max_depth", value=4),),
        output_selector=None,
        timeout_ms=30_000,
        retention="keep",
        target="local",
        source="local",
    )
    monkeypatch.setattr(runtime, "_continue_guarded_local_run", lambda _state: None)

    receipt = runtime.start_guarded_local_run(admission)
    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["guarded_local_run"] == 1

    with pytest.raises(GuardedLocalRunError) as blocked:
        runtime.start_guarded_local_run(admission)
    assert blocked.value.code == "daemon_draining"
    runtime._release_lifecycle_run_lease(str(receipt["run_id"]))


def test_preboundary_remote_claim_path_remains_a_blocker_after_drain(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_lease, claim_jobs = runtime.lifecycle.begin_sync()
    assert claim_jobs is True
    sync_lease.activate()
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    errors: list[BaseException] = []

    def execute(_job: dict[str, Any], _connection_id: str) -> None:
        entered.set()
        assert release.wait(2)
        try:
            runtime.register_object(
                "remote-after-boundary",
                "fit",
                "default",
                yaml_text=FUNCTION_YAML,
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    monkeypatch.setattr(runtime, "_execute_server_job", execute)
    runtime.accept_server_job(
        {"run": {"id": "remote-run-1"}},
        "connection-1",
        parent_sync_lease=sync_lease,
    )
    assert entered.wait(1)

    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_proven()
    sync_lease.complete(seal_lineage=True)
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["remote_claim"] == 1

    release.set()
    assert finished.wait(2)
    for _ in range(100):
        if runtime.lifecycle.status_document()["drain"]["blockers"]["remote_claim"] == 0:
            break
        time.sleep(0.01)
    assert errors == []
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["remote_claim"] == 0


def test_unexpected_remote_claim_escape_becomes_unknown_without_exception_leakage(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    lease = runtime.lifecycle.reserve_root("remote_claim", queued=True)
    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()

    def fail(_job: dict[str, Any], _connection_id: str) -> None:
        raise RuntimeError("forbidden-remote-job-secret")

    monkeypatch.setattr(runtime, "_execute_server_job", fail)
    runtime._execute_server_job_with_lifecycle({}, "connection-1", lease)

    blockers = runtime.lifecycle.status_document()["drain"]["blockers"]
    assert blockers["remote_claim"] == 0
    assert blockers["unknown_work"] == 1
    assert "forbidden-remote-job-secret" not in caplog.text


def test_unexpected_local_run_escape_becomes_unknown_without_exception_leakage(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    lease = runtime.lifecycle.reserve_root("local_run", queued=False)
    runtime._bind_lifecycle_run_lease("run-1", lease)
    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()

    def fail(_run_id: str, _report_local_run: bool) -> None:
        raise RuntimeError("forbidden-local-run-secret")

    monkeypatch.setattr(runtime, "_execute_run", fail)
    runtime._execute_run_with_lifecycle("run-1", True)

    blockers = runtime.lifecycle.status_document()["drain"]["blockers"]
    assert blockers["local_run"] == 0
    assert blockers["unknown_work"] == 1
    assert "forbidden-local-run-secret" not in caplog.text


def test_worker_callback_is_bound_to_the_exact_parent_run_lineage(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = runtime.lifecycle.reserve_root("local_run", queued=False)
    root.activate()
    runtime._bind_lifecycle_run_lease("parent-run-1", root)
    entered = threading.Event()
    release = threading.Event()

    def callback(
        _node: dict[str, Any],
        *,
        kwargs: dict[str, Any],
        timeout_seconds: float | None,
    ) -> dict[str, Any]:
        del kwargs, timeout_seconds
        entered.set()
        assert release.wait(2)
        return {"ok": True}

    monkeypatch.setattr(runtime, "run_remote_node", callback)
    thread = threading.Thread(
        target=runtime.run_worker_callback,
        args=("parent-run-1", {}),
        kwargs={"kwargs": {}, "timeout_seconds": None},
    )
    thread.start()
    assert entered.wait(1)
    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["callback"] == 1

    release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    runtime._release_lifecycle_run_lease("parent-run-1")


def test_descendant_registration_is_retained_under_an_admitted_lineage(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = runtime.lifecycle.reserve_root("remote_claim", queued=False)
    root.activate()
    entered = threading.Event()
    release = threading.Event()

    def register(_name: str, _entrypoint: str, _env: str, **_kwargs: Any) -> dict[str, Any]:
        entered.set()
        assert release.wait(2)
        return {"id": "object-1"}

    monkeypatch.setattr(runtime, "_register_object_admitted", register)

    def register_in_lineage() -> None:
        root.attach_current_thread()
        try:
            runtime.register_object("remote-object", "fit", "default")
        finally:
            root.detach_current_thread()

    thread = threading.Thread(target=register_in_lineage)
    thread.start()
    assert entered.wait(1)
    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["registry_mutation"] == 1

    release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    root.complete(seal_lineage=True)


def test_every_registry_runtime_entrypoint_fails_before_mutation_after_drain(
    runtime: DaemonRuntime,
) -> None:
    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()

    operations = (
        lambda: runtime.run_registry_mutation(lambda: None),
        lambda: runtime.prune_sync_events(status="sent", older_than_days=0),
        lambda: runtime.connect_server(
            server_url="https://splime.invalid/api",
            machine_token="machine-token",
            user_token="user-token",
            machine_id=None,
            display_name=None,
            capabilities={},
            heartbeat_interval_seconds=None,
        ),
        runtime.disconnect_server,
        lambda: runtime.enqueue_object_sync({}),
        lambda: runtime.resolve_remote_signature({}),
        lambda: runtime.start_remote_run("remote-object"),
        lambda: runtime.pull_server_object("remote-object"),
        lambda: runtime.renew_run_delivery("missing-run"),
        lambda: runtime.acknowledge_run_delivery("missing-run"),
        runtime.sweep_run_retention,
    )
    for operation in operations:
        with pytest.raises(LifecycleAdmissionClosed):
            operation()


def test_registry_mutation_race_blocks_stop_commit_and_rejects_late_work(
    runtime: DaemonRuntime,
) -> None:
    exited = threading.Event()
    entered = threading.Event()
    release = threading.Event()
    runtime.lifecycle.bind_exit_callback(exited.set)

    def mutation() -> None:
        runtime.run_registry_mutation(
            lambda: (entered.set(), release.wait(2)),
        )

    thread = threading.Thread(target=mutation)
    thread.start()
    assert entered.wait(1)
    runtime.lifecycle.request_drain(
        _drain_request(runtime),
        supervisor_proof=SUPERVISOR_PROOF,
    )
    runtime.lifecycle.mark_claim_boundary_not_required()
    assert runtime.lifecycle.status_document()["drain"]["blockers"]["registry_mutation"] == 1
    assert not exited.is_set()
    with pytest.raises(LifecycleAdmissionClosed):
        runtime.run_registry_mutation(lambda: None)

    release.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert exited.wait(1)


def test_docker_prewarm_thread_and_container_creation_block_drain_until_cleanup(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = runtime.docker_pool
    runtime.auto_build_envs = True
    pool.enabled = True  # type: ignore[attr-defined]
    pool.pool_size = 1  # type: ignore[attr-defined]
    pool.prewarm = True  # type: ignore[attr-defined]
    started = threading.Event()
    release = threading.Event()
    exited = threading.Event()
    released: list[dict[str, Any]] = []
    runtime.lifecycle.bind_exit_callback(exited.set)

    def ensure_ready(
        _object_record: dict[str, Any],
        *,
        wait: bool,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        del retry_failed
        return {
            "status": "ready",
            "spec_hash": "docker-spec",
            "image_tag": "splime-runtime:prewarm",
            "wait": wait,
        }

    def ensure_container(**_kwargs: Any) -> dict[str, Any]:
        started.set()
        assert release.wait(2)
        return {"name": "owned-prewarm-container"}

    monkeypatch.setattr(runtime.docker_environment_manager, "ensure_ready", ensure_ready)
    monkeypatch.setattr(pool, "ensure_container", ensure_container)
    monkeypatch.setattr(pool, "release_container", released.append)
    object_record = {
        "version_id": "prewarm-version-1",
        "distributions": [],
        "runtime_config": {"mode": "docker"},
    }

    try:
        status = runtime.prepare_object_environment(object_record)
        assert status["image_tag"] == "splime-runtime:prewarm"
        assert started.wait(1)

        runtime.lifecycle.request_drain(
            _drain_request(runtime),
            supervisor_proof=SUPERVISOR_PROOF,
        )
        runtime.lifecycle.mark_claim_boundary_not_required()
        blockers = runtime.lifecycle.status_document()["drain"]["blockers"]
        assert blockers["environment_build"] == 1
        assert not exited.is_set()

        with pytest.raises(LifecycleAdmissionClosed):
            runtime.prepare_object_environment(object_record)

        release.set()
        for _ in range(100):
            if runtime.lifecycle.status_document()["drain"]["blockers"]["environment_build"] == 0:
                break
            time.sleep(0.01)
        assert released == [{"name": "owned-prewarm-container"}]
        assert exited.wait(1)
    finally:
        release.set()


def test_side_effecting_channel_probe_and_error_persistence_are_drain_admitted(
    runtime: DaemonRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote_connection = {
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
    connection = runtime.store.save_server_connection(
        server_url="https://splime.invalid/api",
        token="machine-token-secret",
        user_token="user-token-secret",
        connection=remote_connection,
        heartbeat_interval_seconds=60,
    )
    credentials = runtime.store.get_server_connection_credentials(connection["id"])
    runtime._mark_server_channel_failure(  # noqa: SLF001 - exact breaker fixture.
        credentials,
        error=ServerClientError(503, "open probe fixture"),
        immediate=True,
    )
    started = threading.Event()
    release = threading.Event()
    exited = threading.Event()
    results: list[dict[str, Any]] = []
    errors: list[BaseException] = []

    class BlockingRejectedProbe:
        calls = 0

        def current_connection(self) -> dict[str, Any]:
            type(self).calls += 1
            started.set()
            assert release.wait(2)
            raise ServerClientError(401, "stored channel rejected")

    probe = BlockingRejectedProbe()
    monkeypatch.setattr(
        runtime,
        "_server_client_for_credentials",
        lambda _credentials, *, request_timeout_seconds=None: probe,
    )
    runtime.lifecycle.bind_exit_callback(exited.set)

    def run_probe() -> None:
        try:
            results.append(runtime.server_connection_state(probe=True))
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_probe)
    try:
        thread.start()
        assert started.wait(1)
        runtime.lifecycle.request_drain(
            _drain_request(runtime),
            supervisor_proof=SUPERVISOR_PROOF,
        )
        runtime.lifecycle.mark_claim_boundary_not_required()
        blockers = runtime.lifecycle.status_document()["drain"]["blockers"]
        assert blockers["registry_mutation"] == 1
        assert not exited.is_set()

        with pytest.raises(LifecycleAdmissionClosed):
            runtime.server_connection_state(probe=True)
        assert probe.calls == 1

        release.set()
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert errors == []
        assert results[0]["connected"] is False
        assert runtime.store.get_server_connection(connection["id"])["status"] == "needs_reconnect"
        assert runtime.lifecycle.status_document()["drain"]["blockers"]["registry_mutation"] == 0
        assert exited.wait(1)
    finally:
        release.set()
        thread.join(timeout=2)
