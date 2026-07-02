"""Thread-safe warm Docker container pool for daemon runs."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse, urlunparse

from spl.daemon.runtime_dependencies import DockerEnvironmentBuilderProtocol
from spl.daemon.store import RegistryStore, utc_now, validate_name


class DockerPool:
    """Own warm Docker runtime containers and pool-specific Docker commands."""

    def __init__(
        self,
        store: RegistryStore,
        environment_manager: DockerEnvironmentBuilderProtocol,
        *,
        daemon_base_url: str,
        pool_size: int = 0,
        idle_timeout_seconds: float = 300.0,
        prewarm: bool = False,
    ):
        self.store = store
        self.environment_manager = environment_manager
        self.daemon_base_url = daemon_base_url.rstrip("/")
        self.pool_size = max(0, int(pool_size))
        self.idle_timeout_seconds = float(idle_timeout_seconds)
        self.prewarm = bool(prewarm)
        self._lock = threading.RLock()
        self._containers: dict[str, dict[str, Any]] = {}

    def __len__(self) -> int:
        with self._lock:
            return len(self._containers)

    @property
    def should_prewarm(self) -> bool:
        return self.prewarm and self.pool_size > 0

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
        container_workdir = container_run_dir
        mounts = [
            "-v",
            f"{run_dir.resolve()}:{container_run_dir}",
        ]
        if workdir.resolve() != run_dir.resolve():
            container_workdir = "/workspace"
            mounts.extend(["-v", f"{workdir.resolve()}:{container_workdir}"])

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
            *network_args,
            *self.hardening_args(runtime_config),
            *self.user_args(),
            *mounts,
            "-w",
            container_workdir,
            "-e",
            f"PYTHONPATH={':'.join(pythonpath_entries)}",
            *self.env_args(runtime_config),
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
    ) -> list[str]:
        run_path = f"/runs/{validate_name(run_id)}"
        _, daemon_url = self.network_args(object_record, runtime_config)
        return [
            "docker",
            "exec",
            "-w",
            run_path,
            container_name,
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
        return self.pool_size > 0 and run_dir.resolve() == workdir.resolve()

    def prewarm_object(self, object_record: dict[str, Any]) -> None:
        def prewarm() -> None:
            try:
                environment_record = self.environment_manager.ensure_ready(
                    object_record,
                    wait=True,
                )
                self.ensure_container(
                    object_record=object_record,
                    image_tag=environment_record["image_tag"],
                    runtime_config=object_record.get("runtime_config") or {"mode": "venv"},
                )
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
        key = self.pool_key(image_tag, runtime_config, object_record)
        now = time.monotonic()
        with self._lock:
            self.evict_idle_locked(now)
            existing = self._containers.get(key)
            if existing is not None and self.container_running(existing["name"]):
                existing["last_used"] = now
                return existing
            if existing is not None:
                self.remove_container(existing["name"])
                self._containers.pop(key, None)

            self.evict_excess_locked(reserve=1)

        record = self.start_container(
            key=key,
            object_record=object_record,
            image_tag=image_tag,
            runtime_config=runtime_config,
        )
        record["last_used"] = time.monotonic()
        record["exec_lock"] = threading.Lock()
        with self._lock:
            existing = self._containers.get(key)
            if existing is not None and self.container_running(existing["name"]):
                self.remove_container(record["name"])
                existing["last_used"] = time.monotonic()
                return existing
            if existing is not None:
                self.remove_container(existing["name"])
            self.evict_excess_locked(reserve=1)
            self._containers[key] = record
            return record

    @contextmanager
    def use_container(self, record: dict[str, Any]) -> Iterator[None]:
        exec_lock = record["exec_lock"]
        with exec_lock:
            record["in_use"] = True
            try:
                yield
            finally:
                record["in_use"] = False
                record["last_used"] = time.monotonic()

    def start_container(
        self,
        *,
        key: str,
        object_record: dict[str, Any],
        image_tag: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        source_roots = self.source_roots()
        daemon_source = source_roots[0][1]
        if not (daemon_source / "spl" / "daemon" / "worker.py").exists():
            raise RuntimeError(f"Docker worker source is not found: {daemon_source}")

        name = f"splime-pool-{key[:24]}"
        self.remove_container(name)
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
            *network_args,
            *self.hardening_args(runtime_config),
            *self.user_args(),
            *mounts,
            "-w",
            "/runs",
            "-e",
            f"PYTHONPATH={':'.join(pythonpath_entries)}",
            *self.env_args(runtime_config),
            image_tag,
            "python",
            "-c",
            "import time; time.sleep(10**9)",
        ]
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "failed to start warm Docker runtime container: "
                + (completed.stderr.strip() or completed.stdout.strip())
            )
        container_id = None
        try:
            container_id = cidfile.read_text(encoding="utf-8").strip() or None
        except OSError:
            pass
        return {
            "key": key,
            "name": name,
            "container_id": container_id,
            "image_tag": image_tag,
            "started_at": utc_now(),
            "in_use": False,
        }

    def pool_key(
        self,
        image_tag: str,
        runtime_config: dict[str, Any],
        object_record: dict[str, Any],
    ) -> str:
        payload = json.dumps(
            {
                "image_tag": image_tag,
                "runtime_config": runtime_config,
                "network_args": self.network_args(object_record, runtime_config)[0],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def evict_idle_locked(self, now: float) -> None:
        if self.idle_timeout_seconds <= 0:
            return
        for key, record in list(self._containers.items()):
            if record.get("in_use"):
                continue
            if now - float(record.get("last_used") or now) > self.idle_timeout_seconds:
                self.remove_container(record["name"])
                self._containers.pop(key, None)

    def evict_excess_locked(self, *, reserve: int = 0) -> None:
        while len(self._containers) + reserve > self.pool_size and self._containers:
            candidates = {
                key: record
                for key, record in self._containers.items()
                if not record.get("in_use")
            }
            if not candidates:
                return
            key, record = min(
                candidates.items(),
                key=lambda item: float(item[1].get("last_used") or 0.0),
            )
            self.remove_container(record["name"])
            self._containers.pop(key, None)

    def container_running(self, name: str) -> bool:
        completed = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", name],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=15,
            check=False,
        )
        return completed.returncode == 0 and completed.stdout.strip() == "true"

    def cleanup_stale_containers(self) -> None:
        if shutil.which("docker") is None:
            return
        completed = subprocess.run(
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                "name=^/splime-pool-",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=15,
            check=False,
        )
        if completed.returncode != 0:
            return
        container_ids = [
            item.strip()
            for item in completed.stdout.splitlines()
            if item.strip()
        ]
        if not container_ids:
            return
        subprocess.run(
            ["docker", "rm", "-f", *container_ids],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=30,
            check=False,
        )

    def shutdown(self) -> None:
        with self._lock:
            for record in list(self._containers.values()):
                self.remove_container(record["name"])
            self._containers.clear()

    def hardening_args(self, runtime_config: dict[str, Any]) -> list[str]:
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

    def env_args(self, runtime_config: dict[str, Any]) -> list[str]:
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
        mode = runtime_config.get("network", "auto")
        has_remote_nodes = any(
            node.get("kind") == "remote"
            for node in object_record.get("pipeline_nodes") or []
        )
        if mode == "none" and has_remote_nodes:
            raise RuntimeError(
                "docker runtime network='none' cannot run pipelines with remote nodes"
            )
        if mode == "none" or (mode == "auto" and not has_remote_nodes):
            return ["--network", "none"], self.daemon_base_url
        if platform.system().lower() == "linux":
            return ["--add-host", "host.docker.internal:host-gateway"], (
                self.host_daemon_url()
            )
        return [], self.host_daemon_url()

    def host_daemon_url(self) -> str:
        parsed = urlparse(self.daemon_base_url)
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            return self.daemon_base_url
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

    def user_args(self) -> list[str]:
        if os.name == "nt" or not hasattr(os, "getuid") or not hasattr(os, "getgid"):
            return []
        return ["--user", f"{os.getuid()}:{os.getgid()}"]

    def remove_container(self, name: str) -> None:
        subprocess.run(
            ["docker", "rm", "-f", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
