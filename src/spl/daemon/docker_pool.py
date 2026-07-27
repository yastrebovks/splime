"""Thread-safe warm Docker container pool for daemon runs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import shlex
import shutil
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Protocol
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

from spl._timeout import TimeoutDomain, validate_timeout_seconds
from spl.core import json_contract as m_json_contract
from spl.daemon.callback_capability import CALLBACK_CAPABILITY_ENV
from spl.daemon.runtime_dependencies import DockerEnvironmentBuilderProtocol
from spl.daemon.store import RegistryStore, utc_now, validate_name

OBJECT_DOCKER_RUNTIME_ENV = "SPL_OBJECT_RUNTIME_BACKEND"
OBJECT_DOCKER_RUNTIME_VALUE = "docker"
OBJECT_DOCKER_WORKER_ENV = "SPL_OBJECT_DOCKER_WORKER"
OBJECT_DOCKER_WORKER_VALUE = "1"
DAEMON_INSTANCE_ID_ENV = "SPL_DAEMON_INSTANCE_ID"
DAEMON_HOME_HASH_ENV = "SPL_DAEMON_HOME_HASH"
DAEMON_GENERATION_ENV = "SPL_DAEMON_GENERATION"
DAEMON_RUN_ID_ENV = "SPL_DAEMON_RUN_ID"

MANAGED_LABEL = "com.splime.managed"
INSTANCE_LABEL = "com.splime.instance"
HOME_LABEL = "com.splime.home"
GENERATION_LABEL = "com.splime.generation"
CONTAINER_GENERATION_LABEL = "com.splime.container-generation"
KIND_LABEL = "com.splime.kind"
RUN_LABEL = "com.splime.run"
DOCKER_POOL_TRUST_WARNING = (
    "pooled containers share the runs directory with every other run on this daemon; "
    "enable only for single-tenant, mutually-trusting workloads."
)

LOGGER = logging.getLogger(__name__)


class DockerInstanceIdentityProtocol(Protocol):
    """Nonsecret daemon identity fields used to own Docker containers."""

    @property
    def instance_id(self) -> str:
        """Return the stable per-home instance id."""
        ...

    @property
    def home_hash(self) -> str:
        """Return the hash of the canonical daemon home."""
        ...

    @property
    def generation(self) -> int:
        """Return the current daemon-start generation."""
        ...


class DockerCleanupAuthorityProtocol(Protocol):
    """Live daemon-home lock authority required for startup cleanup."""

    @property
    def is_acquired(self) -> bool:
        """Return whether the authority still holds the daemon-home lock."""
        ...

    @property
    def identity(self) -> DockerInstanceIdentityProtocol | None:
        """Return the exact identity protected by the live lock."""
        ...


class DockerQueryError(RuntimeError):
    """Raised when Docker ownership enumeration cannot be completed safely."""


@dataclass(frozen=True)
class _EphemeralDockerIdentity:
    """Process-local ownership used when no locked home identity is available."""

    instance_id: str
    home_hash: str
    generation: int = 0


def docker_hardening_args(runtime_config: dict[str, Any]) -> list[str]:
    """Return Docker hardening and resource-limit CLI arguments."""

    args: list[str] = []
    if runtime_config.get("init", True):
        args.append("--init")
    cap_drop = runtime_config.get("cap_drop")
    if cap_drop:
        args.extend(["--cap-drop", str(cap_drop)])
    if runtime_config.get("no_new_privileges", True):
        args.extend(["--security-opt", "no-new-privileges"])
    limits = runtime_config.get("limits") or {}
    if limits.get("memory"):
        args.extend(["--memory", str(limits["memory"])])
    if limits.get("cpus"):
        args.extend(["--cpus", str(limits["cpus"])])
    if limits.get("pids_limit"):
        args.extend(["--pids-limit", str(limits["pids_limit"])])
    if runtime_config.get("read_only", True):
        args.append("--read-only")
    tmpfs = runtime_config.get("tmpfs")
    if tmpfs:
        args.extend(["--tmpfs", str(tmpfs)])
    return args


def docker_env_args(runtime_config: dict[str, Any]) -> list[str]:
    """Return deterministic Docker environment CLI arguments."""

    env_values = {
        "HOME": "/tmp",
        "XDG_CACHE_HOME": "/tmp/.cache",
        "MPLCONFIGDIR": "/tmp/.cache/matplotlib",
        **(runtime_config.get("env") or {}),
    }
    args: list[str] = []
    for key, value in sorted(env_values.items()):
        args.extend(["-e", f"{key}={value}"])
    return args


def docker_user_args() -> list[str]:
    """Return host user mapping arguments when supported by the platform."""

    if os.name == "nt" or not hasattr(os, "getuid") or not hasattr(os, "getgid"):
        return []
    return ["--user", f"{os.getuid()}:{os.getgid()}"]


def docker_network_args(
    object_record: dict[str, Any],
    runtime_config: dict[str, Any],
    *,
    daemon_base_url: str,
) -> tuple[list[str], str]:
    """Return Docker network arguments and the daemon URL visible from Docker."""

    mode = runtime_config.get("network", "auto")
    has_remote_nodes = any(node.get("kind") == "remote" for node in object_record.get("pipeline_nodes") or [])
    if mode == "none" and has_remote_nodes:
        raise RuntimeError("docker runtime network='none' cannot run pipelines with remote nodes")
    if mode == "none" or (mode == "auto" and not has_remote_nodes):
        return ["--network", "none"], daemon_base_url
    daemon_url = docker_host_daemon_url(daemon_base_url)
    if platform.system().lower() == "linux":
        return ["--add-host", "host.docker.internal:host-gateway"], daemon_url
    return [], daemon_url


def docker_node_network_args(runtime_config: dict[str, Any]) -> list[str]:
    """Return network CLI arguments for per-node Docker containers.

    Node containers never call back into the SPL daemon, so ``auto`` keeps them
    isolated and ``enabled`` does not add host daemon reachability helpers.
    """

    mode = runtime_config.get("network", "auto")
    if mode in {"none", "auto"}:
        return ["--network", "none"]
    return []


def docker_host_daemon_url(daemon_base_url: str) -> str:
    """Return a daemon URL reachable from a Docker container."""

    parsed = urlparse(daemon_base_url)
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        return daemon_base_url
    host = "host.docker.internal"
    netloc = host
    if parsed.port is not None:
        netloc = f"{host}:{parsed.port}"
    return urlunparse(
        (
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )


def object_docker_worker_env_args() -> list[str]:
    """Return env markers used to reject nested per-node Docker."""

    return [
        "-e",
        f"{OBJECT_DOCKER_RUNTIME_ENV}={OBJECT_DOCKER_RUNTIME_VALUE}",
        "-e",
        f"{OBJECT_DOCKER_WORKER_ENV}={OBJECT_DOCKER_WORKER_VALUE}",
    ]


def callback_capability_env_args(object_record: dict[str, Any]) -> list[str]:
    """Propagate the scoped callback environment without exposing its value."""

    has_remote_nodes = any(node.get("kind") == "remote" for node in object_record.get("pipeline_nodes") or [])
    return ["-e", CALLBACK_CAPABILITY_ENV] if has_remote_nodes else []


def worker_container_labels_from_env(*, kind: str) -> dict[str, str]:
    """Return daemon ownership labels available to a nested worker runtime."""

    instance_id = os.environ.get(DAEMON_INSTANCE_ID_ENV)
    home_hash = os.environ.get(DAEMON_HOME_HASH_ENV)
    generation = os.environ.get(DAEMON_GENERATION_ENV)
    if instance_id is None or home_hash is None or generation is None:
        return {}
    if not generation.isdecimal():
        raise RuntimeError(f"{DAEMON_GENERATION_ENV} must be a non-negative integer")
    labels = {
        MANAGED_LABEL: "true",
        INSTANCE_LABEL: instance_id,
        HOME_LABEL: home_hash,
        GENERATION_LABEL: generation,
        KIND_LABEL: validate_name(kind),
    }
    run_id = os.environ.get(DAEMON_RUN_ID_ENV)
    if run_id is not None:
        labels[RUN_LABEL] = validate_name(run_id)
    return labels


def worker_container_label_args_from_env(*, kind: str) -> list[str]:
    """Return deterministic Docker label arguments for a worker container."""

    args: list[str] = []
    for key, value in sorted(worker_container_labels_from_env(kind=kind).items()):
        args.extend(["--label", f"{key}={value}"])
    return args


class DockerPool:
    """Own warm Docker runtime containers and pool-specific Docker commands."""

    def __init__(
        self,
        store: RegistryStore,
        environment_manager: DockerEnvironmentBuilderProtocol,
        *,
        daemon_base_url: str,
        enabled: bool = False,
        identity: DockerInstanceIdentityProtocol | None = None,
        startup_cleanup_authority: DockerCleanupAuthorityProtocol | None = None,
        pool_size: int = 0,
        idle_timeout_seconds: float = 300.0,
        prewarm: bool = False,
    ):
        self.store = store
        self.environment_manager = environment_manager
        self.daemon_base_url = daemon_base_url.rstrip("/")
        self.enabled = bool(enabled)
        stable_identity = identity
        self.identity: DockerInstanceIdentityProtocol = identity or _EphemeralDockerIdentity(
            instance_id=uuid4().hex,
            home_hash=hashlib.sha256(str(store.home.resolve()).encode("utf-8")).hexdigest(),
        )
        self.startup_cleanup_authority = startup_cleanup_authority
        self.pool_size = max(0, int(pool_size))
        normalized_idle_timeout = validate_timeout_seconds(
            idle_timeout_seconds,
            name="docker_idle_timeout_seconds",
            domain=TimeoutDomain.FINITE,
            allow_none=False,
        )
        assert normalized_idle_timeout is not None
        self.idle_timeout_seconds = normalized_idle_timeout
        self.prewarm = bool(prewarm)
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._containers: dict[str, dict[str, Any]] = {}
        self._starting: set[str] = set()
        self._next_container_generation = 1
        self._closed = False
        if self.enabled and stable_identity is None:
            raise ValueError(
                "docker pooling requires a daemon instance identity; start the daemon with its home lock enabled"
            )
        if self.startup_cleanup_authority is not None and stable_identity is None:
            raise ValueError("Docker startup cleanup requires the stable identity held by the daemon home lock")
        if self.startup_cleanup_authority is not None and not self._has_startup_cleanup_authority():
            raise ValueError(
                "Docker startup cleanup authority must be an acquired daemon home lock protecting the exact identity"
            )
        if self.enabled and self.pool_size <= 0:
            raise ValueError("docker_pool_enabled requires a positive docker_pool_size")
        if self.prewarm and not self.enabled:
            raise ValueError(
                "docker prewarm requires docker_pool_enabled because per-run containers cannot be prewarmed"
            )
        if self.enabled:
            LOGGER.warning(
                DOCKER_POOL_TRUST_WARNING,
                extra={"spl_event": "docker_pool_unsafe_compatibility_enabled"},
            )

    def __len__(self) -> int:
        with self._lock:
            return len(self._containers)

    @property
    def should_prewarm(self) -> bool:
        return self.enabled and self.prewarm and self.pool_size > 0

    def worker_identity_env(self, run_id: str) -> dict[str, str]:
        """Return nonsecret identity fields propagated to one run worker."""

        return {
            DAEMON_INSTANCE_ID_ENV: str(self.identity.instance_id),
            DAEMON_HOME_HASH_ENV: str(self.identity.home_hash),
            DAEMON_GENERATION_ENV: str(self.identity.generation),
            DAEMON_RUN_ID_ENV: validate_name(run_id),
        }

    def ownership_labels(
        self,
        *,
        kind: str,
        run_id: str | None = None,
        container_generation: int | None = None,
    ) -> dict[str, str]:
        """Return Docker labels proving this daemon owns a container."""

        labels = {
            MANAGED_LABEL: "true",
            INSTANCE_LABEL: str(self.identity.instance_id),
            HOME_LABEL: str(self.identity.home_hash),
            GENERATION_LABEL: str(self.identity.generation),
            KIND_LABEL: validate_name(kind),
        }
        if run_id is not None:
            labels[RUN_LABEL] = validate_name(run_id)
        if container_generation is not None:
            labels[CONTAINER_GENERATION_LABEL] = str(container_generation)
        return labels

    def ownership_label_args(
        self,
        *,
        kind: str,
        run_id: str | None = None,
        container_generation: int | None = None,
    ) -> list[str]:
        """Return deterministic ``docker run --label`` ownership arguments."""

        args: list[str] = []
        for key, value in sorted(
            self.ownership_labels(
                kind=kind,
                run_id=run_id,
                container_generation=container_generation,
            ).items()
        ):
            args.extend(["--label", f"{key}={value}"])
        return args

    def worker_command(
        self,
        *,
        object_record: dict[str, Any],
        entrypoint: str,
        run_id: str,
        run_dir: Path,
        workdir: Path,
        image_tag: str,
        container_name: str,
        runtime_config: dict[str, Any],
    ) -> list[str]:
        """Build a Docker CLI command that runs the normal worker protocol."""

        source_roots = self.source_roots()
        daemon_source = source_roots[0][1]
        container_run_dir = "/work"
        _ = workdir
        mounts = [
            "-v",
            f"{run_dir.resolve()}:{container_run_dir}",
        ]

        pythonpath_entries = []
        for index, (_, source_root) in enumerate(source_roots):
            container_path = f"/opt/splime/src{index}"
            mounts.extend(["-v", f"{source_root}:{container_path}:ro"])
            pythonpath_entries.append(container_path)

        network_args, daemon_url = self.network_args(
            object_record,
            runtime_config,
        )
        command = [
            "docker",
            "run",
            "--rm",
            "--name",
            container_name,
            "--cidfile",
            str(run_dir.resolve() / "container.cid"),
            *self.ownership_label_args(kind="run", run_id=run_id),
            *network_args,
            *self.hardening_args(runtime_config),
            *self.user_args(),
            *mounts,
            "-w",
            container_run_dir,
            "-e",
            f"PYTHONPATH={':'.join(pythonpath_entries)}",
            *object_docker_worker_env_args(),
            *self.env_args(runtime_config),
            *self._explicit_env_args(self.worker_identity_env(run_id)),
            *callback_capability_env_args(object_record),
            image_tag,
            "python",
            "/opt/splime/src0/spl/daemon/worker.py",
            "--object-yaml",
            f"{container_run_dir}/object.yaml",
            "--entrypoint",
            entrypoint,
            "--input",
            f"{container_run_dir}/input.json",
            "--result",
            f"{container_run_dir}/result.json",
            "--artifacts-dir",
            f"{container_run_dir}/artifacts",
            "--env-spec",
            f"{container_run_dir}/env-spec.json",
            "--remote-signatures",
            f"{container_run_dir}/remote-signatures.json",
            "--daemon-url",
            daemon_url,
        ]
        if not (daemon_source / "spl" / "daemon" / "worker.py").exists():
            raise RuntimeError(f"Docker worker source is not found: {daemon_source}")
        return command

    def exec_worker_command(
        self,
        *,
        object_record: dict[str, Any],
        entrypoint: str,
        run_id: str,
        container_name: str,
        runtime_config: dict[str, Any],
        lease: dict[str, Any] | None = None,
    ) -> list[str]:
        run_path = f"/runs/{validate_name(run_id)}"
        _, daemon_url = self.network_args(object_record, runtime_config)
        target = self._leased_container_target(container_name, lease=lease)
        return [
            "docker",
            "exec",
            *object_docker_worker_env_args(),
            *self._explicit_env_args(self.worker_identity_env(run_id)),
            *callback_capability_env_args(object_record),
            "-w",
            run_path,
            target,
            "python",
            "/opt/splime/src0/spl/daemon/worker.py",
            "--object-yaml",
            f"{run_path}/object.yaml",
            "--entrypoint",
            entrypoint,
            "--input",
            f"{run_path}/input.json",
            "--result",
            f"{run_path}/result.json",
            "--artifacts-dir",
            f"{run_path}/artifacts",
            "--env-spec",
            f"{run_path}/env-spec.json",
            "--remote-signatures",
            f"{run_path}/remote-signatures.json",
            "--daemon-url",
            daemon_url,
        ]

    def can_use(self, run_dir: Path, workdir: Path) -> bool:
        return self.enabled and self.pool_size > 0 and run_dir.resolve() == workdir.resolve()

    def prewarm_object(self, object_record: dict[str, Any]) -> None:
        def prewarm() -> None:
            try:
                environment_record = self.environment_manager.ensure_ready(
                    object_record,
                    wait=True,
                )
                record = self.ensure_container(
                    object_record=object_record,
                    image_tag=environment_record["image_tag"],
                    runtime_config=object_record.get("runtime_config") or {"mode": "venv"},
                )
                self.release_container(record)
            except Exception:
                return

        thread = threading.Thread(
            target=prewarm,
            name=f"spl-docker-prewarm-{object_record['version_id']}",
            daemon=True,
        )
        thread.start()

    def ensure_container(
        self,
        *,
        object_record: dict[str, Any],
        image_tag: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Return an already-leased warm container record.

        Selection and reservation are one operation under the pool condition.
        The caller must release the returned lease exactly once.
        """

        if not self.enabled:
            raise RuntimeError(
                "docker pooling is disabled; enable docker_pool_enabled only for single-tenant, mutually-trusting runs"
            )
        if self.pool_size <= 0:
            raise RuntimeError("docker pooling is enabled but docker_pool_size is zero; configure a positive pool size")
        key = self.pool_key(image_tag, runtime_config, object_record)
        container_generation: int | None = None
        while container_generation is None:
            with self._condition:
                if self._closed:
                    raise RuntimeError("docker pool is shut down and cannot lease a container")
                now = time.monotonic()
                self.evict_idle_locked(now)
                existing = self._containers.get(key)
                if existing is not None and existing.get("in_use"):
                    self._condition.wait()
                    continue
                if existing is not None and self.container_running(self._container_target(existing)):
                    return self._lease_record_locked(existing, now=now)
                if existing is not None:
                    self._containers.pop(key, None)
                    self.remove_container(self._container_target(existing))
                if key in self._starting:
                    self._condition.wait()
                    continue
                self.evict_excess_locked(reserve=len(self._starting) + 1)
                if len(self._containers) + len(self._starting) >= self.pool_size:
                    self._condition.wait()
                    continue
                self._starting.add(key)
                container_generation = self._next_container_generation
                self._next_container_generation += 1

        try:
            record = self.start_container(
                key=key,
                object_record=object_record,
                image_tag=image_tag,
                runtime_config=runtime_config,
                container_generation=container_generation,
            )
            record["container_generation"] = container_generation
            record["last_used"] = time.monotonic()
            record["in_use"] = False
            record["lease_id"] = None
        except BaseException:
            with self._condition:
                self._starting.discard(key)
                self._condition.notify_all()
            raise

        with self._condition:
            self._starting.discard(key)
            if self._closed:
                self.remove_container(self._container_target(record))
                self._condition.notify_all()
                raise RuntimeError("docker pool shut down while a container was starting; the container was removed")
            existing = self._containers.get(key)
            if existing is not None:
                self.remove_container(self._container_target(record))
                if existing.get("in_use") or not self.container_running(self._container_target(existing)):
                    self._condition.notify_all()
                    raise RuntimeError("docker pool generation changed while a container was starting; retry the run")
                leased = self._lease_record_locked(existing, now=time.monotonic())
                self._condition.notify_all()
                return leased
            self.evict_excess_locked(reserve=1)
            self._containers[key] = record
            leased = self._lease_record_locked(record, now=time.monotonic())
            self._condition.notify_all()
            return leased

    @contextmanager
    def use_container(self, record: dict[str, Any]) -> Iterator[None]:
        """Validate an existing lease and release it on context exit."""

        self._validate_lease(record)
        try:
            yield
        finally:
            self.release_container(record)

    def release_container(self, record: dict[str, Any]) -> None:
        """Release ``record`` without modifying a newer container generation."""

        with self._condition:
            current = self._matching_lease_locked(record)
            if current is None:
                return
            current["in_use"] = False
            current["lease_id"] = None
            current["last_used"] = time.monotonic()
            self._condition.notify_all()

    def quarantine_container(self, record: dict[str, Any]) -> bool:
        """Retire and destroy the exact container represented by ``record``.

        The lease is retired before Docker is touched and is never restored,
        even when Docker cannot confirm physical removal.
        """

        target: str | None = None
        container_generation: int | None = None
        with self._condition:
            current = self._matching_lease_locked(record)
            if current is None:
                return False
            self._containers.pop(str(current["key"]), None)
            target = self._container_target(current)
            generation_value = current.get("container_generation")
            if isinstance(generation_value, int):
                container_generation = generation_value
            self._condition.notify_all()
        removed = self._quarantine_owned_target(
            target,
            expected=self.ownership_labels(
                kind="pool",
                container_generation=container_generation,
            ),
        )
        if not removed:
            LOGGER.error(
                "retired Docker pool lease %s but could not verify physical container removal",
                target,
                extra={"spl_event": "docker_pool_quarantine_unverified", "container_id": target},
            )
        return removed

    def start_container(
        self,
        *,
        key: str,
        object_record: dict[str, Any],
        image_tag: str,
        runtime_config: dict[str, Any],
        container_generation: int,
    ) -> dict[str, Any]:
        source_roots = self.source_roots()
        daemon_source = source_roots[0][1]
        if not (daemon_source / "spl" / "daemon" / "worker.py").exists():
            raise RuntimeError(f"Docker worker source is not found: {daemon_source}")

        name = self.pool_container_name(key, container_generation)
        pool_dir = self.store.home / "docker-pool"
        pool_dir.mkdir(parents=True, exist_ok=True)
        cidfile = pool_dir / f"{name}.cid"
        try:
            cidfile.unlink()
        except FileNotFoundError:
            pass

        mounts = ["-v", f"{self.store.runs_dir.resolve()}:/runs"]
        pythonpath_entries = []
        for index, (_, source_root) in enumerate(source_roots):
            container_path = f"/opt/splime/src{index}"
            mounts.extend(["-v", f"{source_root}:{container_path}:ro"])
            pythonpath_entries.append(container_path)

        network_args, _ = self.network_args(object_record, runtime_config)
        command = [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--cidfile",
            str(cidfile),
            *self.ownership_label_args(
                kind="pool",
                container_generation=container_generation,
            ),
            *network_args,
            *self.hardening_args(runtime_config),
            *self.user_args(),
            *mounts,
            "-w",
            "/runs",
            "-e",
            f"PYTHONPATH={':'.join(pythonpath_entries)}",
            *object_docker_worker_env_args(),
            *self.env_args(runtime_config),
            image_tag,
            "python",
            "-c",
            "import time; time.sleep(10**9)",
        ]
        try:
            completed = subprocess.run(
                command,
                text=True,
                capture_output=True,
                timeout=30,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            self._quarantine_started_pool_container(
                name=name,
                cidfile=cidfile,
                container_generation=container_generation,
            )
            raise RuntimeError(
                "warm Docker runtime container start exceeded 30 seconds; "
                "the partially-started container was quarantined"
            ) from exc
        except OSError as exc:
            self._quarantine_started_pool_container(
                name=name,
                cidfile=cidfile,
                container_generation=container_generation,
            )
            raise RuntimeError(f"could not invoke Docker to start a warm runtime container: {exc}") from exc
        if completed.returncode != 0:
            self._quarantine_started_pool_container(
                name=name,
                cidfile=cidfile,
                container_generation=container_generation,
            )
            raise RuntimeError(
                "failed to start warm Docker runtime container: "
                + (completed.stderr.strip() or completed.stdout.strip())
            )
        container_id = None
        try:
            container_id = cidfile.read_text(encoding="utf-8").strip() or None
        except OSError:
            pass
        if container_id is None:
            container_id = completed.stdout.strip() or None
        if container_id is None:
            self._quarantine_started_pool_container(
                name=name,
                cidfile=cidfile,
                container_generation=container_generation,
            )
            raise RuntimeError("warm Docker runtime started without reporting its immutable container id")
        return {
            "key": key,
            "name": name,
            "container_id": container_id,
            "image_tag": image_tag,
            "started_at": utc_now(),
            "in_use": False,
            "container_generation": container_generation,
        }

    def pool_key(
        self,
        image_tag: str,
        runtime_config: dict[str, Any],
        object_record: dict[str, Any],
    ) -> str:
        payload = m_json_contract.dumps(
            {
                "image_tag": image_tag,
                "runtime_config": runtime_config,
                "network_args": self.network_args(object_record, runtime_config)[0],
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=None,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def evict_idle_locked(self, now: float) -> None:
        if self.idle_timeout_seconds <= 0:
            return
        for key, record in list(self._containers.items()):
            if record.get("in_use"):
                continue
            if now - float(record.get("last_used") or now) > self.idle_timeout_seconds:
                self._containers.pop(key, None)
                self.remove_container(self._container_target(record))

    def evict_excess_locked(self, *, reserve: int = 0) -> None:
        while len(self._containers) + reserve > self.pool_size and self._containers:
            candidates = {key: record for key, record in self._containers.items() if not record.get("in_use")}
            if not candidates:
                return
            key, record = min(
                candidates.items(),
                key=lambda item: float(item[1].get("last_used") or 0.0),
            )
            self._containers.pop(key, None)
            self.remove_container(self._container_target(record))

    def container_running(self, name: str) -> bool:
        try:
            completed = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", name],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return False
        return completed.returncode == 0 and completed.stdout.strip() == "true"

    def cleanup_stale_containers(self) -> None:
        """Remove only verified containers from prior generations of this home."""

        if not self._has_startup_cleanup_authority() or shutil.which("docker") is None:
            return
        expected = {
            MANAGED_LABEL: "true",
            INSTANCE_LABEL: str(self.identity.instance_id),
            HOME_LABEL: str(self.identity.home_hash),
        }
        try:
            container_ids = self._container_ids_with_labels(expected)
        except DockerQueryError as exc:
            LOGGER.error(
                "could not enumerate stale Docker containers for verified startup cleanup: %s",
                exc,
                extra={"spl_event": "docker_startup_cleanup_query_failed"},
            )
            return
        for container_id in container_ids:
            labels = self._container_labels(container_id)
            if labels is None or any(labels.get(key) != value for key, value in expected.items()):
                continue
            generation = labels.get(GENERATION_LABEL)
            if generation == str(self.identity.generation):
                continue
            if generation is None or not generation.isdecimal():
                LOGGER.warning(
                    "leaving managed Docker container %s because its generation label is missing or invalid",
                    container_id,
                )
                continue
            self._kill_and_remove_container(container_id)
        self._cleanup_legacy_containers()

    def quarantine_run_containers(self, run_id: str) -> bool:
        """Kill exact owned one-shot and per-node containers for ``run_id``.

        A Docker query failure raises :class:`DockerQueryError`; it is never
        treated as an empty, successfully-cleaned ownership set.
        """

        if shutil.which("docker") is None:
            raise DockerQueryError("docker executable is unavailable during owned-run cleanup")
        expected = {
            MANAGED_LABEL: "true",
            INSTANCE_LABEL: str(self.identity.instance_id),
            HOME_LABEL: str(self.identity.home_hash),
            GENERATION_LABEL: str(self.identity.generation),
            RUN_LABEL: validate_name(run_id),
        }
        all_absent = True
        for container_id in self._container_ids_with_labels(expected):
            labels = self._container_labels(container_id)
            if labels is None or any(labels.get(key) != value for key, value in expected.items()):
                all_absent = False
                continue
            kind = labels.get(KIND_LABEL)
            if not isinstance(kind, str) or not kind:
                all_absent = False
                continue
            if not self._quarantine_owned_target(
                container_id,
                expected=self.ownership_labels(kind=kind, run_id=run_id),
            ):
                all_absent = False
        return all_absent

    def quarantine_owned_container(
        self,
        target: str,
        *,
        kind: str,
        run_id: str | None = None,
    ) -> bool:
        """Kill ``target`` and return true only after verified absence.

        The immutable id or name is first inspected and must carry the full
        current daemon-generation ownership label set.  A missing target is
        successful only when Docker explicitly reports that it is absent.
        """

        return self._quarantine_owned_target(
            target,
            expected=self.ownership_labels(kind=kind, run_id=run_id),
        )

    def remove_owned_container(
        self,
        target: str,
        *,
        kind: str,
        run_id: str | None = None,
    ) -> bool:
        """Remove ``target`` only after verifying this generation's labels."""

        return self.quarantine_owned_container(target, kind=kind, run_id=run_id)

    def shutdown(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()
            while self._starting:
                self._condition.wait()
            for record in list(self._containers.values()):
                self._kill_and_remove_container(self._container_target(record))
            self._containers.clear()
            self._condition.notify_all()

    def run_container_name(self, run_id: str, *, fallback: str) -> str:
        """Return an instance-scoped name for one run container."""

        del fallback
        return f"splime-run-{self._instance_name_token()}-d{self.identity.generation}-{validate_name(run_id)[:32]}"

    def pool_container_name(self, key: str, container_generation: int) -> str:
        """Return an instance-scoped, generation-fenced pool container name."""

        return (
            f"splime-pool-{self._instance_name_token()}-{key[:16]}-d{self.identity.generation}-g{container_generation}"
        )

    def _instance_name_token(self) -> str:
        token = "".join(char.lower() for char in str(self.identity.instance_id) if char.isalnum())
        if not token:
            raise ValueError("daemon instance id must contain at least one alphanumeric character")
        return token[:16]

    @staticmethod
    def _explicit_env_args(values: dict[str, str]) -> list[str]:
        args: list[str] = []
        for key, value in sorted(values.items()):
            args.extend(["-e", f"{key}={value}"])
        return args

    def _lease_record_locked(self, record: dict[str, Any], *, now: float) -> dict[str, Any]:
        if record.get("in_use"):
            raise RuntimeError("docker pool attempted to lease a container that is already in use")
        record["in_use"] = True
        record["lease_id"] = uuid4().hex
        record["last_used"] = now
        return dict(record)

    def _matching_lease_locked(self, lease: dict[str, Any]) -> dict[str, Any] | None:
        key = str(lease.get("key") or "")
        current = self._containers.get(key)
        if current is None or not current.get("in_use"):
            return None
        exact_fields = ("container_id", "container_generation", "lease_id")
        if any(current.get(field) != lease.get(field) for field in exact_fields):
            return None
        return current

    def _validate_lease(self, lease: dict[str, Any]) -> None:
        with self._condition:
            if self._matching_lease_locked(lease) is None:
                raise RuntimeError("Docker container lease is stale; the container was released, removed, or recreated")

    def _leased_container_target(
        self,
        container_name: str,
        *,
        lease: dict[str, Any] | None,
    ) -> str:
        if not self.enabled:
            return container_name
        if lease is None:
            raise RuntimeError("Docker container lease is required for generation-fenced pooled execution")
        with self._condition:
            record = self._matching_lease_locked(lease)
            if record is not None and record.get("name") == container_name:
                return self._container_target(record)
        raise RuntimeError(
            "Docker container lease is stale; refusing to execute against an unleased or recreated container"
        )

    @staticmethod
    def _container_target(record: dict[str, Any]) -> str:
        return str(record.get("container_id") or record["name"])

    def _has_startup_cleanup_authority(self) -> bool:
        authority = self.startup_cleanup_authority
        if authority is None:
            return False
        try:
            authority_identity = authority.identity
            is_acquired = authority.is_acquired
        except (AttributeError, RuntimeError):
            return False
        return (
            is_acquired
            and authority_identity is self.identity
            and str(authority_identity.home_hash) == str(self.identity.home_hash)
        )

    def _container_ids_with_labels(self, labels: dict[str, str]) -> list[str]:
        command = ["docker", "ps", "-aq"]
        for key, value in sorted(labels.items()):
            command.extend(["--filter", f"label={key}={value}"])
        try:
            completed = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise DockerQueryError(f"Docker ownership query failed: {exc}") from exc
        if completed.returncode != 0:
            raise DockerQueryError(f"Docker ownership query exited with status {completed.returncode}")
        return [item.strip() for item in completed.stdout.splitlines() if item.strip()]

    def _container_inspect(self, container_id: str) -> dict[str, Any] | None:
        outcome, inspected = self._container_inspect_outcome(container_id)
        return inspected if outcome == "present" else None

    def _container_inspect_outcome(self, container_id: str) -> tuple[str, dict[str, Any] | None]:
        """Return ``present``, verified ``absent``, or ``error`` for a target."""

        try:
            completed = subprocess.run(
                ["docker", "inspect", container_id],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            LOGGER.warning("could not inspect Docker container %s: %s", container_id, exc)
            return "error", None
        if completed.returncode != 0:
            detail = (completed.stderr or "").strip()
            lowered = detail.lower()
            if "no such object" in lowered or "no such container" in lowered:
                return "absent", None
            LOGGER.warning(
                "could not verify Docker container %s state (status=%s): %s",
                container_id,
                completed.returncode,
                detail or "no Docker error detail",
            )
            return "error", None
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError:
            LOGGER.warning("Docker inspect returned invalid JSON for container %s", container_id)
            return "error", None
        if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
            LOGGER.warning("Docker inspect returned an invalid payload for container %s", container_id)
            return "error", None
        return "present", payload[0]

    def _quarantine_owned_target(self, target: str, *, expected: dict[str, str]) -> bool:
        outcome, inspected = self._container_inspect_outcome(target)
        if outcome == "absent":
            return True
        if outcome != "present" or inspected is None:
            return False
        container_id = inspected.get("Id")
        config = inspected.get("Config")
        labels = config.get("Labels") if isinstance(config, dict) else None
        if not isinstance(container_id, str) or not container_id or not isinstance(labels, dict):
            LOGGER.warning("refusing to quarantine Docker target %s with incomplete inspect identity", target)
            return False
        if any(labels.get(key) != value for key, value in expected.items()):
            LOGGER.warning("refusing to quarantine Docker target %s because ownership labels do not match", target)
            return False
        self._kill_and_remove_container(container_id)
        final_outcome, _ = self._container_inspect_outcome(container_id)
        if final_outcome == "absent":
            return True
        LOGGER.error(
            "Docker container %s cleanup completed without verified absence (state=%s)",
            container_id,
            final_outcome,
            extra={"spl_event": "docker_container_removal_unverified", "container_id": container_id},
        )
        return False

    def _container_labels(self, container_id: str) -> dict[str, str] | None:
        inspected = self._container_inspect(container_id)
        if inspected is None:
            return None
        config = inspected.get("Config")
        if not isinstance(config, dict):
            return None
        labels = config.get("Labels")
        if labels is None:
            return {}
        if not isinstance(labels, dict) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()
        ):
            return None
        return labels

    def _cleanup_legacy_containers(self) -> None:
        if not self._has_startup_cleanup_authority():
            return
        pool_dir = self.store.home / "docker-pool"
        try:
            cidfiles = sorted(pool_dir.glob("splime-pool-*.cid"))
        except OSError:
            return
        expected_runs_dir = str(self.store.runs_dir.resolve())
        for cidfile in cidfiles:
            try:
                container_id = cidfile.read_text(encoding="utf-8").strip()
            except OSError:
                continue
            if not container_id:
                continue
            if not self._legacy_pool_name(cidfile.stem):
                self._warn_unattributed_legacy_candidate(container_id, cidfile, "cidfile name is not canonical")
                continue
            if not self._legacy_container_id(container_id):
                self._warn_unattributed_legacy_candidate(container_id, cidfile, "container id is not canonical")
                continue
            outcome, inspected = self._container_inspect_outcome(container_id)
            if outcome == "absent":
                self._warn_unattributed_legacy_candidate(
                    container_id,
                    cidfile,
                    "the exact Docker id is already absent; review and remove the stale cidfile manually",
                )
                continue
            if outcome != "present" or inspected is None:
                self._warn_unattributed_legacy_candidate(container_id, cidfile, "exact Docker inspect failed")
                continue
            if not self._legacy_container_matches(
                inspected,
                expected_container_id=container_id,
                cidfile=cidfile,
                expected_runs_dir=expected_runs_dir,
            ):
                self._warn_unattributed_legacy_candidate(
                    container_id,
                    cidfile,
                    "name, labels, immutable id, or /runs mount did not match",
                )
                continue
            LOGGER.warning("removing verified legacy Docker pool container %s for this daemon home", container_id)
            self._kill_and_remove_container(container_id)
            final_outcome, _ = self._container_inspect_outcome(container_id)
            if final_outcome != "absent":
                self._warn_unattributed_legacy_candidate(
                    container_id,
                    cidfile,
                    f"automatic removal did not verify absence (state={final_outcome})",
                )

    @staticmethod
    def _legacy_pool_name(name: str) -> bool:
        prefix = "splime-pool-"
        token = name.removeprefix(prefix)
        return name.startswith(prefix) and len(token) == 24 and all(char in "0123456789abcdef" for char in token)

    @staticmethod
    def _legacy_container_id(container_id: str) -> bool:
        return len(container_id) == 64 and all(char in "0123456789abcdef" for char in container_id)

    @staticmethod
    def _warn_unattributed_legacy_candidate(container_id: str, cidfile: Path, reason: str) -> None:
        quoted_id = shlex.quote(container_id)
        LOGGER.warning(
            "leaving nonempty legacy Docker CID candidate %r from %s: %s. "
            "Inspect the exact id with `docker inspect -- %s` and, only after confirming ownership, "
            "remove that exact id with `docker rm -f -- %s`.",
            container_id,
            cidfile,
            reason,
            quoted_id,
            quoted_id,
            extra={"spl_event": "docker_legacy_cleanup_manual_review", "cidfile": str(cidfile)},
        )

    @staticmethod
    def _legacy_container_matches(
        inspected: dict[str, Any],
        *,
        expected_container_id: str,
        cidfile: Path,
        expected_runs_dir: str,
    ) -> bool:
        if inspected.get("Id") != expected_container_id:
            return False
        name = str(inspected.get("Name") or "").removeprefix("/")
        if name != cidfile.stem or not name.startswith("splime-pool-"):
            return False
        config = inspected.get("Config")
        if not isinstance(config, dict):
            return False
        labels = config.get("Labels")
        if isinstance(labels, dict) and labels.get(MANAGED_LABEL) is not None:
            return False
        mounts = inspected.get("Mounts")
        if not isinstance(mounts, list):
            return False
        return any(
            isinstance(mount, dict)
            and mount.get("Source") == expected_runs_dir
            and mount.get("Destination") == "/runs"
            and mount.get("RW") is True
            for mount in mounts
        )

    def _kill_and_remove_container(self, container_id: str) -> None:
        try:
            subprocess.run(
                ["docker", "kill", container_id],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            LOGGER.warning("failed to kill owned Docker container %s: %s", container_id, exc)
        finally:
            self.remove_container(container_id)

    def _quarantine_started_pool_container(
        self,
        *,
        name: str,
        cidfile: Path,
        container_generation: int,
    ) -> None:
        candidates: list[str] = []
        try:
            container_id = cidfile.read_text(encoding="utf-8").strip()
        except OSError:
            container_id = ""
        if container_id:
            candidates.append(container_id)
        candidates.append(name)
        expected = self.ownership_labels(kind="pool", container_generation=container_generation)
        for target in candidates:
            inspected = self._container_inspect(target)
            if inspected is None:
                continue
            immutable_id = inspected.get("Id")
            config = inspected.get("Config")
            labels = config.get("Labels") if isinstance(config, dict) else None
            if not isinstance(immutable_id, str) or not isinstance(labels, dict):
                continue
            if any(labels.get(key) != value for key, value in expected.items()):
                continue
            self._kill_and_remove_container(immutable_id)
            return
        LOGGER.warning("could not verify ownership of Docker pool container %s after a failed start", name)

    def hardening_args(self, runtime_config: dict[str, Any]) -> list[str]:
        return docker_hardening_args(runtime_config)

    def env_args(self, runtime_config: dict[str, Any]) -> list[str]:
        return docker_env_args(runtime_config)

    def source_roots(self) -> list[tuple[str, Path]]:
        roots = [("daemon", Path(__file__).parents[2].resolve())]
        try:
            import spl.core as spl_core

            core_path = Path(str(spl_core.__file__)).parents[2].resolve()
            if core_path not in [path for _, path in roots]:
                roots.append(("framework", core_path))
        except Exception:
            pass
        return roots

    def network_args(
        self,
        object_record: dict[str, Any],
        runtime_config: dict[str, Any],
    ) -> tuple[list[str], str]:
        return docker_network_args(object_record, runtime_config, daemon_base_url=self.daemon_base_url)

    def host_daemon_url(self) -> str:
        return docker_host_daemon_url(self.daemon_base_url)

    def user_args(self) -> list[str]:
        return docker_user_args()

    def remove_container(self, name: str) -> None:
        try:
            subprocess.run(
                ["docker", "rm", "-f", name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            LOGGER.warning("failed to remove owned Docker container %s: %s", name, exc)
