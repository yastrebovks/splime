from __future__ import annotations

import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pytest

from spl.daemon.docker_pool import (
    CONTAINER_GENERATION_LABEL,
    DAEMON_GENERATION_ENV,
    DAEMON_HOME_HASH_ENV,
    DAEMON_INSTANCE_ID_ENV,
    DAEMON_RUN_ID_ENV,
    GENERATION_LABEL,
    HOME_LABEL,
    INSTANCE_LABEL,
    KIND_LABEL,
    MANAGED_LABEL,
    RUN_LABEL,
    DockerQueryError,
    DockerPool,
    docker_node_network_args,
    worker_container_labels_from_env,
)
from spl.daemon.store import RegistryStore


class FakeDockerEnvironmentManager:
    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool,
    ) -> dict[str, Any]:
        return {
            "spec_hash": "demo",
            "image_tag": "splime-runtime:demo",
        }


@dataclass(frozen=True)
class FakeIdentity:
    instance_id: str = "instance-a"
    home_hash: str = "a" * 64
    generation: int = 7


@dataclass
class FakeCleanupAuthority:
    identity: FakeIdentity
    is_acquired: bool = True


class RecordingDockerPool(DockerPool):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.removed: list[str] = []
        self.killed: list[str] = []

    def remove_container(self, name: str) -> None:
        self.removed.append(name)

    def container_running(self, name: str) -> bool:
        return True

    def _kill_and_remove_container(self, container_id: str) -> None:
        self.killed.append(container_id)
        self.remove_container(container_id)


@pytest.fixture
def store(tmp_path: Path) -> Iterator[RegistryStore]:
    registry = RegistryStore(tmp_path)
    try:
        yield registry
    finally:
        registry.close()


def _pool(
    store: RegistryStore,
    *,
    pool_size: int = 1,
    idle_timeout_seconds: float = 300.0,
    enabled: bool = True,
    identity: FakeIdentity | None = None,
) -> RecordingDockerPool:
    selected_identity = identity or FakeIdentity()
    return RecordingDockerPool(
        store,
        FakeDockerEnvironmentManager(),
        daemon_base_url="http://127.0.0.1:8765",
        enabled=enabled,
        identity=selected_identity,
        startup_cleanup_authority=FakeCleanupAuthority(selected_identity),
        pool_size=pool_size,
        idle_timeout_seconds=idle_timeout_seconds,
    )


@pytest.mark.parametrize(
    "idle_timeout_seconds",
    [True, "300", float("nan"), float("inf"), float("-inf")],
    ids=["boolean", "string", "nan", "positive-infinity", "negative-infinity"],
)
def test_pool_rejects_invalid_idle_timeout(
    store: RegistryStore,
    idle_timeout_seconds: Any,
) -> None:
    with pytest.raises(ValueError, match="docker_idle_timeout_seconds"):
        _pool(store, idle_timeout_seconds=idle_timeout_seconds)


@pytest.mark.parametrize("idle_timeout_seconds", [-1, 0, 0.25, 300])
def test_pool_preserves_every_finite_idle_timeout(
    store: RegistryStore,
    idle_timeout_seconds: float,
) -> None:
    pool = _pool(store, idle_timeout_seconds=idle_timeout_seconds)

    assert pool.idle_timeout_seconds == float(idle_timeout_seconds)


def test_pool_key_includes_effective_network(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    monkeypatch.setattr("spl.daemon.docker_pool.platform.system", lambda: "Linux")
    config = {"mode": "docker", "network": "auto"}

    local_key = pool.pool_key(
        "splime-runtime:demo",
        config,
        {"pipeline_nodes": []},
    )
    remote_key = pool.pool_key(
        "splime-runtime:demo",
        config,
        {"pipeline_nodes": [{"kind": "remote"}]},
    )

    assert local_key != remote_key


def test_node_docker_network_args_do_not_add_daemon_host_mapping() -> None:
    assert docker_node_network_args({"network": "none"}) == ["--network", "none"]
    assert docker_node_network_args({"network": "auto"}) == ["--network", "none"]
    assert docker_node_network_args({"network": "enabled"}) == []


def test_idle_eviction_skips_in_use_containers(store: RegistryStore) -> None:
    pool = _pool(store, pool_size=2, idle_timeout_seconds=1)
    pool._containers = {
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

    pool.evict_idle_locked(now=10.0)

    assert pool.removed == ["splime-pool-idle"]
    assert "busy" in pool._containers
    assert "idle" not in pool._containers


def test_excess_eviction_skips_in_use_containers(store: RegistryStore) -> None:
    pool = _pool(store, pool_size=1)
    pool._containers = {
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

    pool.evict_excess_locked(reserve=1)

    assert pool.removed == ["splime-pool-idle"]
    assert "busy" in pool._containers
    assert "idle" not in pool._containers


def test_pool_is_default_off_and_explicit_enablement_warns(
    store: RegistryStore,
    caplog: pytest.LogCaptureFixture,
) -> None:
    disabled = DockerPool(
        store,
        FakeDockerEnvironmentManager(),
        daemon_base_url="http://127.0.0.1:8765",
        pool_size=2,
    )
    assert disabled.can_use(store.runs_dir / "run", store.runs_dir / "run") is False

    with caplog.at_level("WARNING"):
        enabled = DockerPool(
            store,
            FakeDockerEnvironmentManager(),
            daemon_base_url="http://127.0.0.1:8765",
            enabled=True,
            identity=FakeIdentity(),
            pool_size=2,
        )

    assert enabled.can_use(store.runs_dir / "run", store.runs_dir / "run") is True
    assert (
        "pooled containers share the runs directory with every other run on this daemon; "
        "enable only for single-tenant, mutually-trusting workloads"
    ) in caplog.text


def test_enabled_pool_requires_managed_instance_identity(store: RegistryStore) -> None:
    with pytest.raises(ValueError, match="requires a daemon instance identity"):
        DockerPool(
            store,
            FakeDockerEnvironmentManager(),
            daemon_base_url="http://127.0.0.1:8765",
            enabled=True,
            pool_size=1,
        )


def test_startup_cleanup_requires_a_live_guard_for_the_exact_identity(store: RegistryStore) -> None:
    identity = FakeIdentity()
    with pytest.raises(ValueError, match="acquired daemon home lock"):
        DockerPool(
            store,
            FakeDockerEnvironmentManager(),
            daemon_base_url="http://127.0.0.1:8765",
            identity=identity,
            startup_cleanup_authority=FakeCleanupAuthority(
                FakeIdentity(instance_id="other-instance", home_hash=identity.home_hash)
            ),
        )


def test_released_startup_cleanup_guard_disables_new_and_legacy_cleanup(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    authority = pool.startup_cleanup_authority
    assert isinstance(authority, FakeCleanupAuthority)
    authority.is_acquired = False
    queried = False

    def unexpected_query(labels: dict[str, str]) -> list[str]:
        nonlocal queried
        queried = True
        return []

    monkeypatch.setattr("spl.daemon.docker_pool.shutil.which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr(pool, "_container_ids_with_labels", unexpected_query)

    pool.cleanup_stale_containers()
    pool._cleanup_legacy_containers()

    assert queried is False


def test_pool_rejects_enabled_zero_size_and_disabled_prewarm(store: RegistryStore) -> None:
    with pytest.raises(ValueError, match="requires a positive docker_pool_size"):
        DockerPool(
            store,
            FakeDockerEnvironmentManager(),
            daemon_base_url="http://127.0.0.1:8765",
            enabled=True,
            identity=FakeIdentity(),
        )
    with pytest.raises(ValueError, match="prewarm requires docker_pool_enabled"):
        DockerPool(
            store,
            FakeDockerEnvironmentManager(),
            daemon_base_url="http://127.0.0.1:8765",
            prewarm=True,
        )


def test_one_shot_command_mounts_only_active_run_and_carries_owner_labels(
    store: RegistryStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    run_dir = tmp_path / "run-a"
    run_dir.mkdir()
    unrelated_workdir = tmp_path / "other-run"
    unrelated_workdir.mkdir()
    daemon_source = tmp_path / "daemon-source"
    worker = daemon_source / "spl" / "daemon" / "worker.py"
    worker.parent.mkdir(parents=True)
    worker.write_text("# worker\n", encoding="utf-8")
    monkeypatch.setattr(pool, "source_roots", lambda: [("daemon", daemon_source)])
    name = pool.run_container_name("run-a", fallback="splime-run-run-a")

    command = pool.worker_command(
        object_record={"pipeline_nodes": []},
        entrypoint="demo",
        run_id="run-a",
        run_dir=run_dir,
        workdir=unrelated_workdir,
        image_tag="splime-runtime:demo",
        container_name=name,
        runtime_config={"mode": "docker", "network": "none"},
    )

    assert command[command.index("--name") + 1] == name
    assert name.startswith("splime-run-instancea-d7-")
    assert f"{run_dir.resolve()}:/work" in command
    assert not any(str(unrelated_workdir.resolve()) in item for item in command)
    assert "/workspace" not in command
    assert command[command.index("-w") + 1] == "/work"
    assert f"{MANAGED_LABEL}=true" in command
    assert f"{INSTANCE_LABEL}=instance-a" in command
    assert f"{HOME_LABEL}={'a' * 64}" in command
    assert f"{GENERATION_LABEL}=7" in command
    assert f"{KIND_LABEL}=run" in command
    assert f"{RUN_LABEL}=run-a" in command
    assert f"{DAEMON_RUN_ID_ENV}=run-a" in command


def test_worker_container_labels_round_trip_from_daemon_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(DAEMON_INSTANCE_ID_ENV, "instance-a")
    monkeypatch.setenv(DAEMON_HOME_HASH_ENV, "b" * 64)
    monkeypatch.setenv(DAEMON_GENERATION_ENV, "4")
    monkeypatch.setenv(DAEMON_RUN_ID_ENV, "run-4")

    assert worker_container_labels_from_env(kind="node") == {
        MANAGED_LABEL: "true",
        INSTANCE_LABEL: "instance-a",
        HOME_LABEL: "b" * 64,
        GENERATION_LABEL: "4",
        KIND_LABEL: "node",
        RUN_LABEL: "run-4",
    }


def _fake_start_container(**kwargs: Any) -> dict[str, Any]:
    generation = int(kwargs["container_generation"])
    return {
        "key": kwargs["key"],
        "name": f"pool-{generation}",
        "container_id": f"container-{generation}",
        "image_tag": kwargs["image_tag"],
    }


def _lease(pool: DockerPool, *, image_tag: str = "splime-runtime:demo") -> dict[str, Any]:
    return pool.ensure_container(
        object_record={"pipeline_nodes": []},
        image_tag=image_tag,
        runtime_config={"mode": "docker", "network": "none"},
    )


def test_ensure_container_returns_an_atomic_lease_and_use_only_releases(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    monkeypatch.setattr(pool, "start_container", _fake_start_container)

    lease = _lease(pool)

    assert lease["in_use"] is True
    assert lease["lease_id"]
    assert lease["container_generation"] == 1
    assert pool._containers[lease["key"]]["in_use"] is True
    pool.evict_idle_locked(now=10**9)
    assert pool.removed == []

    with pool.use_container(lease):
        assert pool._containers[lease["key"]]["in_use"] is True

    assert pool._containers[lease["key"]]["in_use"] is False


def test_stale_release_cannot_release_recreated_container_generation(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    monkeypatch.setattr(pool, "start_container", _fake_start_container)
    monkeypatch.setattr(pool, "_quarantine_owned_target", lambda target, *, expected: True)
    first = _lease(pool)

    pool.quarantine_container(first)
    second = _lease(pool)
    pool.release_container(first)

    assert second["container_generation"] == 2
    assert pool._containers[second["key"]]["in_use"] is True
    assert pool._containers[second["key"]]["lease_id"] == second["lease_id"]
    with pytest.raises(RuntimeError, match="stale"):
        with pool.use_container(first):
            pass
    pool.release_container(second)


def test_failed_physical_quarantine_never_restores_retired_pool_lease(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    monkeypatch.setattr(pool, "start_container", _fake_start_container)
    monkeypatch.setattr(pool, "_quarantine_owned_target", lambda target, *, expected: False)
    lease = _lease(pool)

    assert pool.quarantine_container(lease) is False
    assert len(pool) == 0
    pool.release_container(lease)
    assert len(pool) == 0


def test_exec_fence_rejects_a_stale_lease_during_later_reuse(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    monkeypatch.setattr(pool, "start_container", _fake_start_container)
    first = _lease(pool)
    pool.release_container(first)
    second = _lease(pool)
    kwargs = {
        "object_record": {"pipeline_nodes": []},
        "entrypoint": "demo",
        "run_id": "run-a",
        "container_name": second["name"],
        "runtime_config": {"mode": "docker", "network": "none"},
    }

    with pytest.raises(RuntimeError, match="stale"):
        pool.exec_worker_command(**kwargs, lease=first)
    command = pool.exec_worker_command(**kwargs, lease=second)

    assert command[:3] == ["docker", "exec", "-e"]
    assert "container-1" in command
    pool.release_container(second)


def test_concurrent_same_key_starts_once_and_never_exposes_false_idle(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    start_entered = threading.Event()
    allow_start = threading.Event()
    first_acquired = threading.Event()
    allow_first_release = threading.Event()
    second_acquired = threading.Event()
    leases: list[dict[str, Any]] = []
    start_count = 0
    count_lock = threading.Lock()

    def controlled_start(**kwargs: Any) -> dict[str, Any]:
        nonlocal start_count
        with count_lock:
            start_count += 1
        start_entered.set()
        assert allow_start.wait(5)
        return _fake_start_container(**kwargs)

    monkeypatch.setattr(pool, "start_container", controlled_start)

    def first_user() -> None:
        lease = _lease(pool)
        leases.append(lease)
        first_acquired.set()
        assert allow_first_release.wait(5)
        pool.release_container(lease)

    def second_user() -> None:
        lease = _lease(pool)
        leases.append(lease)
        second_acquired.set()
        pool.release_container(lease)

    first_thread = threading.Thread(target=first_user)
    second_thread = threading.Thread(target=second_user)
    first_thread.start()
    assert start_entered.wait(5)
    second_thread.start()
    allow_start.set()
    assert first_acquired.wait(5)
    assert second_acquired.is_set() is False
    pool.evict_excess_locked(reserve=1)
    assert pool.removed == []
    allow_first_release.set()
    first_thread.join(5)
    second_thread.join(5)

    assert first_thread.is_alive() is False
    assert second_thread.is_alive() is False
    assert start_count == 1
    assert len(leases) == 2
    assert leases[0]["container_id"] == leases[1]["container_id"]
    assert leases[0]["lease_id"] != leases[1]["lease_id"]


def test_many_requesters_and_looping_evictor_never_remove_a_live_lease(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requester_count = 12
    rounds = 8
    pool = _pool(store, pool_size=3, idle_timeout_seconds=0.001)
    monkeypatch.setattr(pool, "start_container", _fake_start_container)
    start = threading.Barrier(requester_count + 1)
    stop_evictor = threading.Event()
    active_targets: set[str] = set()
    active_lock = threading.Lock()
    removed_while_active: list[str] = []
    errors: list[BaseException] = []
    completed_leases: list[tuple[str, str]] = []
    original_remove = pool.remove_container

    def checked_remove(target: str) -> None:
        with active_lock:
            if target in active_targets:
                removed_while_active.append(target)
        original_remove(target)

    monkeypatch.setattr(pool, "remove_container", checked_remove)

    def requester(index: int) -> None:
        try:
            start.wait(5)
            for iteration in range(rounds):
                lease = _lease(pool, image_tag=f"image:{(index + iteration) % 4}")
                target = str(lease["container_id"])
                with pool._condition:
                    assert pool._matching_lease_locked(lease) is not None
                    with active_lock:
                        assert target not in active_targets
                        active_targets.add(target)
                time.sleep(0.001)
                with pool._condition:
                    with active_lock:
                        active_targets.remove(target)
                    pool.release_container(lease)
                completed_leases.append((target, str(lease["lease_id"])))
        except BaseException as exc:
            errors.append(exc)

    def evictor() -> None:
        try:
            start.wait(5)
            while not stop_evictor.is_set():
                with pool._condition:
                    pool.evict_idle_locked(time.monotonic() + 1.0)
                    pool.evict_excess_locked()
                    pool._condition.notify_all()
                time.sleep(0)
        except BaseException as exc:
            errors.append(exc)

    requester_threads = [threading.Thread(target=requester, args=(index,)) for index in range(requester_count)]
    evictor_thread = threading.Thread(target=evictor)
    evictor_thread.start()
    for thread in requester_threads:
        thread.start()
    for thread in requester_threads:
        thread.join(15)
    stop_evictor.set()
    evictor_thread.join(5)

    assert all(thread.is_alive() is False for thread in requester_threads)
    assert evictor_thread.is_alive() is False
    assert errors == []
    assert removed_while_active == []
    assert active_targets == set()
    assert len(completed_leases) == requester_count * rounds
    assert all(target and lease_id for target, lease_id in completed_leases)


def test_distinct_keys_wait_when_pool_capacity_is_fully_leased(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store, pool_size=1)
    monkeypatch.setattr(pool, "start_container", _fake_start_container)
    first = _lease(pool, image_tag="image:first")
    second_attempting = threading.Event()
    second_acquired = threading.Event()
    second_lease: list[dict[str, Any]] = []

    def acquire_second() -> None:
        second_attempting.set()
        second_lease.append(_lease(pool, image_tag="image:second"))
        second_acquired.set()

    thread = threading.Thread(target=acquire_second)
    thread.start()
    assert second_attempting.wait(5)
    assert second_acquired.is_set() is False
    pool.release_container(first)
    thread.join(5)

    assert thread.is_alive() is False
    assert second_acquired.is_set() is True
    assert len(pool._containers) == 1
    pool.release_container(second_lease[0])


def test_shutdown_waits_for_start_and_removes_late_container(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    start_entered = threading.Event()
    allow_start = threading.Event()
    errors: list[BaseException] = []

    def controlled_start(**kwargs: Any) -> dict[str, Any]:
        start_entered.set()
        assert allow_start.wait(5)
        return _fake_start_container(**kwargs)

    monkeypatch.setattr(pool, "start_container", controlled_start)

    def acquire() -> None:
        try:
            _lease(pool)
        except BaseException as exc:
            errors.append(exc)

    acquire_thread = threading.Thread(target=acquire)
    acquire_thread.start()
    assert start_entered.wait(5)
    shutdown_thread = threading.Thread(target=pool.shutdown)
    shutdown_thread.start()
    with pool._condition:
        while not pool._closed:
            pool._condition.wait(5)
    assert shutdown_thread.is_alive() is True
    allow_start.set()
    acquire_thread.join(5)
    shutdown_thread.join(5)

    assert acquire_thread.is_alive() is False
    assert shutdown_thread.is_alive() is False
    assert len(errors) == 1
    assert "shut down while a container was starting" in str(errors[0])
    assert pool.removed == ["container-1"]
    assert pool._containers == {}


def test_stale_cleanup_is_label_scoped_and_skips_current_generation(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    requested: list[dict[str, str]] = []
    labels = {
        "old": {
            MANAGED_LABEL: "true",
            INSTANCE_LABEL: "instance-a",
            HOME_LABEL: "a" * 64,
            GENERATION_LABEL: "6",
        },
        "current": {
            MANAGED_LABEL: "true",
            INSTANCE_LABEL: "instance-a",
            HOME_LABEL: "a" * 64,
            GENERATION_LABEL: "7",
        },
        "foreign": {
            MANAGED_LABEL: "true",
            INSTANCE_LABEL: "instance-b",
            HOME_LABEL: "b" * 64,
            GENERATION_LABEL: "1",
        },
    }

    def ids_for(expected: dict[str, str]) -> list[str]:
        requested.append(dict(expected))
        return ["old", "current", "foreign"]

    monkeypatch.setattr("spl.daemon.docker_pool.shutil.which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr(pool, "_container_ids_with_labels", ids_for)
    monkeypatch.setattr(pool, "_container_labels", lambda container_id: labels[container_id])
    monkeypatch.setattr(pool, "_cleanup_legacy_containers", lambda: None)

    pool.cleanup_stale_containers()

    assert requested == [
        {
            MANAGED_LABEL: "true",
            INSTANCE_LABEL: "instance-a",
            HOME_LABEL: "a" * 64,
        }
    ]
    assert pool.killed == ["old"]
    assert pool.removed == ["old"]


def test_quarantine_run_containers_verifies_every_owner_label(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    expected = {
        MANAGED_LABEL: "true",
        INSTANCE_LABEL: "instance-a",
        HOME_LABEL: "a" * 64,
        GENERATION_LABEL: "7",
        RUN_LABEL: "run-a",
        KIND_LABEL: "node",
    }
    monkeypatch.setattr("spl.daemon.docker_pool.shutil.which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr(pool, "_container_ids_with_labels", lambda labels: ["owned", "foreign"])
    monkeypatch.setattr(
        pool,
        "_container_labels",
        lambda container_id: expected if container_id == "owned" else {**expected, INSTANCE_LABEL: "instance-b"},
    )

    def inspect_outcome(container_id: str) -> tuple[str, dict[str, Any] | None]:
        if container_id in pool.removed:
            return "absent", None
        labels = expected if container_id == "owned" else {**expected, INSTANCE_LABEL: "instance-b"}
        return "present", {"Id": container_id, "Config": {"Labels": labels}}

    monkeypatch.setattr(pool, "_container_inspect_outcome", inspect_outcome)

    assert pool.quarantine_run_containers("run-a") is False

    assert pool.killed == ["owned"]
    assert pool.removed == ["owned"]


def test_owned_container_query_failure_is_not_reported_as_an_empty_result(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)

    def timeout(command: list[str], **kwargs: Any) -> Any:
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr("spl.daemon.docker_pool.shutil.which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr("spl.daemon.docker_pool.subprocess.run", timeout)

    with pytest.raises(DockerQueryError, match="ownership query failed"):
        pool.quarantine_run_containers("run-a")


def test_remove_owned_container_resists_name_reuse_by_another_instance(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    foreign = {
        "Id": "foreign-id",
        "Config": {
            "Labels": {
                **pool.ownership_labels(kind="run", run_id="run-a"),
                INSTANCE_LABEL: "instance-b",
            }
        },
    }
    owned = {
        "Id": "owned-id",
        "Config": {"Labels": pool.ownership_labels(kind="run", run_id="run-a")},
    }
    inspections = iter([("present", foreign), ("present", owned), ("absent", None)])
    monkeypatch.setattr(pool, "_container_inspect_outcome", lambda _: next(inspections))

    assert pool.remove_owned_container("reused-name", kind="run", run_id="run-a") is False
    assert pool.removed == []
    assert pool.remove_owned_container("owned-name", kind="run", run_id="run-a") is True
    assert pool.killed == ["owned-id"]
    assert pool.removed == ["owned-id"]


def test_legacy_cleanup_requires_exact_home_cid_name_and_runs_mount(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    pool = _pool(store)
    pool_dir = store.home / "docker-pool"
    pool_dir.mkdir(parents=True, exist_ok=True)
    owned_name = f"splime-pool-{'a' * 24}"
    foreign_name = f"splime-pool-{'b' * 24}"
    owned_cidfile = pool_dir / f"{owned_name}.cid"
    foreign_cidfile = pool_dir / f"{foreign_name}.cid"
    invalid_name_cidfile = pool_dir / "splime-pool-not-canonical.cid"
    absent_cidfile = pool_dir / f"splime-pool-{'c' * 24}.cid"
    invalid_id_cidfile = pool_dir / f"splime-pool-{'d' * 24}.cid"
    owned_id = "1" * 64
    foreign_id = "2" * 64
    invalid_name_id = "3" * 64
    absent_id = "4" * 64
    owned_cidfile.write_text(f"{owned_id}\n", encoding="utf-8")
    foreign_cidfile.write_text(f"{foreign_id}\n", encoding="utf-8")
    invalid_name_cidfile.write_text(f"{invalid_name_id}\n", encoding="utf-8")
    absent_cidfile.write_text(f"{absent_id}\n", encoding="utf-8")
    invalid_id_cidfile.write_text("not-a-container-id\n", encoding="utf-8")
    expected_mount = {
        "Source": str(store.runs_dir.resolve()),
        "Destination": "/runs",
        "RW": True,
    }
    inspections = {
        owned_id: {
            "Id": owned_id,
            "Name": f"/{owned_name}",
            "Config": {"Labels": {}},
            "Mounts": [expected_mount],
        },
        foreign_id: {
            "Id": foreign_id,
            "Name": f"/{foreign_name}",
            "Config": {"Labels": {}},
            "Mounts": [{**expected_mount, "Source": str(store.home / "other-runs")}],
        },
    }

    def inspect_outcome(container_id: str) -> tuple[str, dict[str, Any] | None]:
        if container_id in pool.removed:
            return "absent", None
        if container_id == absent_id:
            return "absent", None
        return "present", inspections[container_id]

    monkeypatch.setattr(pool, "_container_inspect_outcome", inspect_outcome)

    pool._cleanup_legacy_containers()

    assert pool.killed == [owned_id]
    assert pool.removed == [owned_id]
    for unsafe_id in (foreign_id, invalid_name_id, absent_id, "not-a-container-id"):
        assert repr(unsafe_id) in caplog.text
        assert f"docker inspect -- {unsafe_id}" in caplog.text
        assert f"docker rm -f -- {unsafe_id}" in caplog.text


def test_pool_start_command_has_instance_and_container_generation_labels(
    store: RegistryStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    daemon_source = tmp_path / "daemon-source"
    worker = daemon_source / "spl" / "daemon" / "worker.py"
    worker.parent.mkdir(parents=True)
    worker.write_text("# worker\n", encoding="utf-8")
    monkeypatch.setattr(pool, "source_roots", lambda: [("daemon", daemon_source)])
    commands: list[list[str]] = []

    class Completed:
        returncode = 0
        stdout = "immutable-id\n"
        stderr = ""

    def run(command: list[str], **kwargs: Any) -> Completed:
        commands.append(command)
        cidfile = Path(command[command.index("--cidfile") + 1])
        cidfile.write_text("immutable-id\n", encoding="utf-8")
        return Completed()

    monkeypatch.setattr("spl.daemon.docker_pool.subprocess.run", run)

    record = pool.start_container(
        key="f" * 64,
        object_record={"pipeline_nodes": []},
        image_tag="splime-runtime:demo",
        runtime_config={"mode": "docker", "network": "none"},
        container_generation=3,
    )

    command = commands[0]
    assert command[command.index("--name") + 1] == "splime-pool-instancea-ffffffffffffffff-d7-g3"
    assert f"{MANAGED_LABEL}=true" in command
    assert f"{INSTANCE_LABEL}=instance-a" in command
    assert f"{GENERATION_LABEL}=7" in command
    assert f"{CONTAINER_GENERATION_LABEL}=3" in command
    assert f"{KIND_LABEL}=pool" in command
    assert record["container_id"] == "immutable-id"


def test_kill_failure_still_attempts_bounded_force_remove(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = DockerPool(
        store,
        FakeDockerEnvironmentManager(),
        daemon_base_url="http://127.0.0.1:8765",
    )
    commands: list[tuple[list[str], float | None]] = []

    class Completed:
        returncode = 0

    def run(command: list[str], **kwargs: Any) -> Completed:
        commands.append((command, kwargs.get("timeout")))
        if command[1] == "kill":
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return Completed()

    monkeypatch.setattr("spl.daemon.docker_pool.subprocess.run", run)

    pool._kill_and_remove_container("immutable-id")

    assert commands == [
        (["docker", "kill", "immutable-id"], 15),
        (["docker", "rm", "-f", "immutable-id"], 30),
    ]


def test_start_timeout_quarantines_only_verified_new_container(
    store: RegistryStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(store)
    daemon_source = tmp_path / "daemon-source"
    worker = daemon_source / "spl" / "daemon" / "worker.py"
    worker.parent.mkdir(parents=True)
    worker.write_text("# worker\n", encoding="utf-8")
    monkeypatch.setattr(pool, "source_roots", lambda: [("daemon", daemon_source)])

    def timeout(command: list[str], **kwargs: Any) -> Any:
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    expected_labels = pool.ownership_labels(kind="pool", container_generation=1)
    monkeypatch.setattr("spl.daemon.docker_pool.subprocess.run", timeout)
    monkeypatch.setattr(
        pool,
        "_container_inspect",
        lambda _: {"Id": "started-id", "Config": {"Labels": expected_labels}},
    )

    with pytest.raises(RuntimeError, match="start exceeded 30 seconds") as captured:
        pool.start_container(
            key="f" * 64,
            object_record={"pipeline_nodes": []},
            image_tag="splime-runtime:demo",
            runtime_config={"mode": "docker", "network": "none"},
            container_generation=1,
        )

    assert isinstance(captured.value.__cause__, subprocess.TimeoutExpired)
    assert pool.killed == ["started-id"]
    assert pool.removed == ["started-id"]
