from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest

from spl.daemon.docker_pool import DockerPool, docker_node_network_args
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


class RecordingDockerPool(DockerPool):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.removed: list[str] = []

    def remove_container(self, name: str) -> None:
        self.removed.append(name)

    def container_running(self, name: str) -> bool:
        return True


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
) -> RecordingDockerPool:
    return RecordingDockerPool(
        store,
        FakeDockerEnvironmentManager(),
        daemon_base_url="http://127.0.0.1:8765",
        pool_size=pool_size,
        idle_timeout_seconds=idle_timeout_seconds,
    )


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
