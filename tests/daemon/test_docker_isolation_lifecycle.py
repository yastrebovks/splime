from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import pytest

from spl._process import run_process_tree
from spl.daemon.docker_pool import (
    GENERATION_LABEL,
    HOME_LABEL,
    INSTANCE_LABEL,
    KIND_LABEL,
    MANAGED_LABEL,
    DockerPool,
)
from spl.daemon.home_lock import DaemonHomeLock, DaemonInstanceIdentity
from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore, utc_now

DOCKER_TEST_IMAGE = "python:3.13-slim-trixie"
ALPINE_TEST_IMAGE = "alpine:3.20"

DOCKER_TIMEOUT_FUNCTION_YAML = """\
- !DFunction
  name: docker_timeout
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    import signal
    import time
    from pathlib import Path
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(1.0)
    Path("timed-out-process-survived.txt").write_text("survived", encoding="utf-8")
    time.sleep(10.0)
    return 1
"""


class _UnusedEnvironmentManager:
    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        del object_record, wait, retry_failed
        raise AssertionError("the lifecycle test supplies an already-local image")


@pytest.fixture
def store(tmp_path: Path) -> Iterator[RegistryStore]:
    registry = RegistryStore(tmp_path / "daemon-home")
    try:
        yield registry
    finally:
        registry.close()


def _docker_available(*images: str) -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        info = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        )
        if info.returncode != 0:
            return False
        return all(
            subprocess.run(
                ["docker", "image", "inspect", image],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
                check=False,
            ).returncode
            == 0
            for image in images
        )
    except (OSError, subprocess.TimeoutExpired):
        return False


def _identity(home: Path, *, instance_id: str, generation: int) -> DaemonInstanceIdentity:
    return DaemonInstanceIdentity(
        instance_id=instance_id,
        home_hash=hashlib.sha256(str(home.resolve()).encode("utf-8")).hexdigest(),
        generation=generation,
        previous_generation=generation - 1 if generation > 1 else None,
        pid=1,
        started_at=utc_now(),
    )


def _runtime_config() -> dict[str, Any]:
    return {
        "cap_drop": "ALL",
        "env": {},
        "init": True,
        "limits": {"pids_limit": 64},
        "network": "none",
        "no_new_privileges": True,
        "read_only": True,
        "tmpfs": "/tmp:rw,nosuid,size=64m",
    }


def _container_exists(container_id: str) -> bool:
    return (
        subprocess.run(
            ["docker", "inspect", container_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        ).returncode
        == 0
    )


def _remove_exact_containers(container_ids: list[str]) -> None:
    for container_id in container_ids:
        try:
            subprocess.run(
                ["docker", "rm", "-f", container_id],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass


def _owned_container_ids(identity: DaemonInstanceIdentity) -> list[str]:
    completed = subprocess.run(
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            f"label={INSTANCE_LABEL}={identity.instance_id}",
            "--filter",
            f"label={HOME_LABEL}={identity.home_hash}",
            "--filter",
            f"label={GENERATION_LABEL}={identity.generation}",
        ],
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return [item for item in completed.stdout.splitlines() if item]


@pytest.mark.docker
@pytest.mark.skipif(
    not _docker_available(DOCKER_TEST_IMAGE),
    reason=f"Docker or the already-local {DOCKER_TEST_IMAGE} image is unavailable",
)
def test_one_shot_container_cannot_read_another_run(store: RegistryStore) -> None:
    """The real one-shot command exposes its own run and no other run mount."""

    token = uuid4().hex
    identity = _identity(store.home, instance_id=f"audit{token}", generation=1)
    pool = DockerPool(
        store,
        _UnusedEnvironmentManager(),
        daemon_base_url="http://127.0.0.1:8765",
        identity=identity,
    )
    run_a = store.runs_dir / "run-a"
    run_b = store.runs_dir / "run-b"
    run_a.mkdir(parents=True)
    run_b.mkdir(parents=True)
    (run_a / "own.txt").write_text("owned-by-a", encoding="utf-8")
    other_secret = run_b / "secret.txt"
    other_secret.write_text("owned-by-b", encoding="utf-8")

    container_name = pool.run_container_name("run-a", fallback="unused-fallback")
    command = pool.worker_command(
        object_record={"pipeline_nodes": []},
        entrypoint="unused",
        run_id="run-a",
        run_dir=run_a,
        # An adversarial configured workdir must not become a second bind.
        workdir=store.runs_dir,
        image_tag=DOCKER_TEST_IMAGE,
        container_name=container_name,
        runtime_config=_runtime_config(),
    )
    assert f"{run_a.resolve()}:/work" in command
    assert not any(str(store.runs_dir.resolve()) == item.split(":", 1)[0] for item in command if ":" in item)
    assert "/workspace" not in command

    image_index = command.index(DOCKER_TEST_IMAGE)
    probe = """
import pathlib
import sys

assert pathlib.Path('/work/own.txt').read_text(encoding='utf-8') == 'owned-by-a'
for forbidden in (sys.argv[1], '/runs/run-b/secret.txt', '/workspace/secret.txt'):
    if pathlib.Path(forbidden).exists():
        raise SystemExit('cross-run path was visible: ' + forbidden)
print('isolated')
"""
    try:
        completed = subprocess.run(
            [*command[: image_index + 1], "python", "-c", probe, str(other_secret.resolve())],
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        assert completed.stdout.strip() == "isolated"
        assert other_secret.read_text(encoding="utf-8") == "owned-by-b"
    finally:
        target = container_name
        try:
            target = (run_a / "container.cid").read_text(encoding="utf-8").strip() or target
        except OSError:
            pass
        try:
            pool.quarantine_owned_container(target, kind="run", run_id="run-a")
            pool.quarantine_run_containers("run-a")
        finally:
            _remove_exact_containers(_owned_container_ids(identity))


@pytest.mark.docker
@pytest.mark.skipif(
    not _docker_available(ALPINE_TEST_IMAGE),
    reason=f"Docker or the already-local {ALPINE_TEST_IMAGE} image is unavailable",
)
def test_live_cleanup_is_instance_scoped_and_skips_current_generation(tmp_path: Path) -> None:
    """Two real instance labels remain independent during stale cleanup."""

    created: list[str] = []
    lock_a = DaemonHomeLock(tmp_path / "home-a")
    lock_b = DaemonHomeLock(tmp_path / "home-b")
    identity_a = lock_a.acquire()
    identity_b = lock_b.acquire()
    store_a = RegistryStore(lock_a.home)
    store_b = RegistryStore(lock_b.home)
    token = uuid4().hex
    pool_a = DockerPool(
        store_a,
        _UnusedEnvironmentManager(),
        daemon_base_url="http://127.0.0.1:8765",
        identity=identity_a,
        startup_cleanup_authority=lock_a,
    )
    pool_b = DockerPool(
        store_b,
        _UnusedEnvironmentManager(),
        daemon_base_url="http://127.0.0.1:8765",
        identity=identity_b,
        startup_cleanup_authority=lock_b,
    )

    def start(identity: DaemonInstanceIdentity, *, generation: int, suffix: str) -> str:
        labels = {
            MANAGED_LABEL: "true",
            INSTANCE_LABEL: identity.instance_id,
            HOME_LABEL: identity.home_hash,
            GENERATION_LABEL: str(generation),
            KIND_LABEL: "pool",
        }
        command = ["docker", "run", "-d", "--name", f"splime-audit-{token[:12]}-{suffix}"]
        for key, value in sorted(labels.items()):
            command.extend(["--label", f"{key}={value}"])
        command.extend([ALPINE_TEST_IMAGE, "sleep", "60"])
        completed = subprocess.run(command, text=True, capture_output=True, timeout=30, check=True)
        container_id = completed.stdout.strip()
        created.append(container_id)
        return container_id

    try:
        stale_a = start(identity_a, generation=identity_a.generation - 1, suffix="stale-a")
        current_a = start(identity_a, generation=identity_a.generation, suffix="current-a")
        stale_b = start(identity_b, generation=identity_b.generation - 1, suffix="stale-b")

        pool_a.cleanup_stale_containers()
        assert not _container_exists(stale_a)
        assert _container_exists(current_a)
        assert _container_exists(stale_b)

        pool_b.cleanup_stale_containers()
        assert not _container_exists(stale_b)
        assert _container_exists(current_a)
    finally:
        _remove_exact_containers(created)
        store_a.close()
        store_b.close()
        lock_a.release()
        lock_b.release()


@pytest.mark.docker
@pytest.mark.skipif(
    not _docker_available(DOCKER_TEST_IMAGE),
    reason=f"Docker or the already-local {DOCKER_TEST_IMAGE} image is unavailable",
)
def test_live_pooled_timeout_quarantines_and_never_reuses_container(store: RegistryStore) -> None:
    """A container-side process that ignores SIGTERM dies with its lease."""

    token = uuid4().hex
    identity = _identity(store.home, instance_id=f"timeout{token}", generation=1)
    pool = DockerPool(
        store,
        _UnusedEnvironmentManager(),
        daemon_base_url="http://127.0.0.1:8765",
        enabled=True,
        identity=identity,
        pool_size=1,
    )
    marker = store.runs_dir / "timed-out-process-survived.txt"
    config = _runtime_config()
    object_record: dict[str, Any] = {"pipeline_nodes": []}
    first: dict[str, Any] | None = None
    second: dict[str, Any] | None = None
    try:
        first = pool.ensure_container(
            object_record=object_record,
            image_tag=DOCKER_TEST_IMAGE,
            runtime_config=config,
        )
        first_id = str(first["container_id"])
        code = """
import signal
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(1.0)
Path('/runs/timed-out-process-survived.txt').write_text('survived', encoding='utf-8')
time.sleep(10.0)
"""
        with pytest.raises(subprocess.TimeoutExpired):
            run_process_tree(
                ["docker", "exec", first_id, "python", "-c", code],
                timeout=0.25,
                termination_grace_seconds=0.1,
            )
        pool.quarantine_container(first)
        first = None
        assert len(pool) == 0
        assert not _container_exists(first_id)
        time.sleep(1.1)
        assert not marker.exists()

        second = pool.ensure_container(
            object_record=object_record,
            image_tag=DOCKER_TEST_IMAGE,
            runtime_config=config,
        )
        assert second["container_id"] != first_id
        assert second["container_generation"] == 2
        pool.release_container(second)
        second = None
    finally:
        if first is not None:
            pool.quarantine_container(first)
        if second is not None:
            pool.quarantine_container(second)
        pool.shutdown()


@pytest.mark.docker
@pytest.mark.skipif(
    not _docker_available(DOCKER_TEST_IMAGE),
    reason=f"Docker or the already-local {DOCKER_TEST_IMAGE} image is unavailable",
)
def test_live_daemon_docker_timeout_is_terminal_and_destroys_pool_container(store: RegistryStore) -> None:
    """The real daemon/backend timeout seam quarantines before terminal state."""

    token = uuid4().hex
    identity = _identity(store.home, instance_id=f"daemon{token}", generation=1)
    runtime = DaemonRuntime(
        store,
        auto_build_envs=False,
        docker_pool_enabled=True,
        docker_pool_size=1,
        daemon_identity=identity,
    )
    observed_container_id: str | None = None
    final: dict[str, Any] | None = None
    try:
        store.register_env("default", sys.executable)
        record = runtime.register_object(
            "docker_timeout",
            "docker_timeout",
            "default",
            yaml_text=DOCKER_TIMEOUT_FUNCTION_YAML,
            runtime_config={"mode": "docker", "python": "3.13"},
        )
        build = runtime.docker_environment_manager.ensure_ready(record, wait=True)
        assert build["status"] == "ready", build.get("error")

        started = runtime.start_run(
            "docker_timeout",
            source="local",
            report_local_run=True,
            timeout_seconds=0.4,
            keep=True,
        )
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            state = store.get_run(started["id"])
            container_id = state.get("container_id")
            if isinstance(container_id, str) and container_id:
                observed_container_id = container_id
            if state["status"] in {"succeeded", "failed"}:
                final = state
                break
            time.sleep(0.01)
        assert final is not None, "Docker run did not reach a terminal state"
        assert final["status"] == "failed"
        assert final["error"] == "run timed out after 0.4 seconds"
        assert observed_container_id is not None
        assert not _container_exists(observed_container_id)
        assert len(runtime.docker_pool) == 0
        time.sleep(1.0)
        assert not (Path(final["run_dir"]) / "timed-out-process-survived.txt").exists()

        # Local-run reporting uses the same terminal state and claim-fenced
        # sync path as other failures; no later worker write can change it.
        assert store.get_run(started["id"])["status"] == "failed"
    finally:
        runtime.shutdown()
        _remove_exact_containers(_owned_container_ids(identity))
