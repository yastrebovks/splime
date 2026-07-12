"""Setup diagnostics behind the ``spl-daemon doctor`` CLI command.

``doctor`` answers the cold-start question "why does my splime setup not
work?" without requiring the user to know daemon internals.  It inspects the
local interpreter, the daemon home directory, the daemon HTTP endpoint, the
central-server connection, cached environment builds, and Docker availability,
and prints one human-readable line per check.

Every check is isolated: a failing probe becomes a ``fail``/``warn`` result,
never an exception, so the command always produces a full report.  Checks that
cannot run without a reachable daemon are reported as ``skip``.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from spl.core.entities.pipeline import Pipeline
from spl.core.ir.utils import spl_import_from_file
from spl.core.adapter_compat import find_pipeline_adapter_compatibility_issues, probe_pipeline_adapters
from spl.daemon.docker_pool import (
    OBJECT_DOCKER_RUNTIME_ENV,
    OBJECT_DOCKER_RUNTIME_VALUE,
    OBJECT_DOCKER_WORKER_ENV,
    OBJECT_DOCKER_WORKER_VALUE,
)
from spl.daemon.interpreter_visibility import python_minor_mismatch
from spl.daemon.repositories.server_connection import SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
from spl.daemon_client import default_daemon_home

OK = "ok"
WARN = "warn"
FAIL = "fail"
SKIP = "skip"

_GIB = 1024**3
DISK_FAIL_BYTES = 1 * _GIB
DISK_WARN_BYTES = 5 * _GIB
DOCKER_INFO_TIMEOUT_SECONDS = 10.0
SERVE_HINT = "start the daemon with `python -m spl.daemon serve`"


class DaemonHealthClient(Protocol):
    """The daemon-client capability doctor needs."""

    base_url: str

    def health(self) -> dict[str, Any]:
        """Return the daemon /health payload."""
        ...


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one diagnostic check."""

    name: str
    status: str
    detail: str
    hint: str | None = None


@dataclass(frozen=True)
class _DockerProbeResult:
    """Local Docker CLI/daemon probe shared by object and per-node checks."""

    docker_path: str | None
    available: bool
    detail: str
    hint: str | None = None


@dataclass(frozen=True)
class DoctorReport:
    """All check results plus shell-friendly aggregates."""

    checks: list[CheckResult] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        """Return 1 when any check failed, otherwise 0."""

        return 1 if any(check.status == FAIL for check in self.checks) else 0

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-friendly representation for ``doctor --json``."""

        return {
            "ok": self.exit_code == 0,
            "checks": [
                {
                    "name": check.name,
                    "status": check.status,
                    "detail": check.detail,
                    "hint": check.hint,
                }
                for check in self.checks
            ],
        }

    def render(self) -> str:
        """Return the human-readable multi-line report."""

        lines = []
        for check in self.checks:
            lines.append(f"{check.status:>4}  {check.name:<22} {check.detail}")
            if check.hint:
                lines.append(f"{'':>4}  {'':<22} hint: {check.hint}")
        counts = {status: 0 for status in (OK, WARN, FAIL, SKIP)}
        for check in self.checks:
            counts[check.status] = counts.get(check.status, 0) + 1
        lines.append(f"\n{counts[OK]} ok, {counts[WARN]} warnings, {counts[FAIL]} failures, {counts[SKIP]} skipped")
        return "\n".join(lines)


def run_doctor(
    client: DaemonHealthClient,
    *,
    home: str | Path | None = None,
    pipeline: Pipeline | None = None,
    probe_pipeline: bool = False,
) -> DoctorReport:
    """Run all diagnostic checks and return the collected report."""

    home_path = Path(home) if home is not None else default_daemon_home()
    checks = [
        check_python(),
        check_venv_tooling(),
        check_uv_builder(),
        check_daemon_home(home_path),
        check_disk_space(home_path),
    ]
    daemon_check, health = check_daemon(client)
    checks.append(daemon_check)
    checks.append(check_server_connection(health))
    checks.append(check_server_identity(health))
    checks.append(check_environment_builds(health))
    checks.append(check_interpreter_substitutions(health))
    if pipeline is not None:
        checks.append(check_pipeline_adapter_tags(pipeline))
        if probe_pipeline:
            checks.append(check_pipeline_adapter_probe(pipeline))
    checks.append(check_docker())
    checks.append(check_node_docker(daemon_available=daemon_check.status == OK))
    return DoctorReport(checks=checks)


def load_pipeline_from_yaml_file(path: str | Path) -> Pipeline:
    """Load exactly one runtime ``Pipeline`` from a local SPL YAML file."""

    source = Path(path).expanduser().absolute()
    namespace: dict[str, Any] = {}
    spl_import_from_file(source, namespace)
    pipelines = [value for value in namespace.values() if isinstance(value, Pipeline)]
    if len(pipelines) != 1:
        raise ValueError("doctor --pipeline expects exactly one Pipeline in {}".format(source))
    return pipelines[0]


def check_python() -> CheckResult:
    """Report the interpreter doctor itself is running under."""

    version = ".".join(str(part) for part in sys.version_info[:3])
    return CheckResult(
        name="python",
        status=OK,
        detail=f"Python {version} ({sys.executable})",
    )


def check_venv_tooling() -> CheckResult:
    """Verify the interpreter can create virtual environments."""

    missing = []
    for module in ("venv", "ensurepip"):
        try:
            spec = importlib.util.find_spec(module)
        except Exception:
            spec = None
        if spec is None:
            missing.append(module)
    if missing:
        return CheckResult(
            name="venv tooling",
            status=FAIL,
            detail=f"missing stdlib modules: {', '.join(missing)}",
            hint=(
                "install the venv package for this interpreter "
                "(for example `apt install python3-venv` on Debian/Ubuntu)"
            ),
        )
    return CheckResult(
        name="venv tooling",
        status=OK,
        detail="venv and ensurepip are available",
    )


def check_uv_builder() -> CheckResult:
    """Report whether fast uv-based environment builds are available."""

    uv_path = shutil.which("uv")
    if uv_path is not None:
        return CheckResult(
            name="uv builder",
            status=OK,
            detail=f"available ({uv_path})",
        )
    return CheckResult(
        name="uv builder",
        status=WARN,
        detail="uv not found; environment builds will use the slower pip fallback",
        hint="install uv on PATH to enable fast relocatable environment builds",
    )


def check_daemon_home(home_path: Path) -> CheckResult:
    """Verify the daemon home directory exists and is writable."""

    if not home_path.exists():
        return CheckResult(
            name="daemon home",
            status=WARN,
            detail=f"{home_path} does not exist yet",
            hint=f"it is created on first start; {SERVE_HINT}",
        )
    if not home_path.is_dir():
        return CheckResult(
            name="daemon home",
            status=FAIL,
            detail=f"{home_path} exists but is not a directory",
        )
    if not os.access(home_path, os.W_OK):
        return CheckResult(
            name="daemon home",
            status=FAIL,
            detail=f"{home_path} is not writable by this user",
        )
    return CheckResult(name="daemon home", status=OK, detail=str(home_path))


def check_disk_space(home_path: Path) -> CheckResult:
    """Verify there is room for cached environments under the daemon home."""

    probe = home_path
    while not probe.exists():
        parent = probe.parent
        if parent == probe:
            break
        probe = parent
    try:
        free_bytes = shutil.disk_usage(probe).free
    except OSError as exc:
        return CheckResult(
            name="disk space",
            status=WARN,
            detail=f"could not read disk usage for {probe}: {exc}",
        )
    detail = f"{free_bytes / _GIB:.1f} GiB free at {probe}"
    if free_bytes < DISK_FAIL_BYTES:
        return CheckResult(
            name="disk space",
            status=FAIL,
            detail=detail,
            hint="environment builds need free disk space; clean up before running",
        )
    if free_bytes < DISK_WARN_BYTES:
        return CheckResult(name="disk space", status=WARN, detail=detail)
    return CheckResult(name="disk space", status=OK, detail=detail)


def check_daemon(
    client: DaemonHealthClient,
) -> tuple[CheckResult, dict[str, Any] | None]:
    """Probe the daemon /health endpoint; return the payload for later checks."""

    try:
        health = client.health()
    except Exception as exc:
        return (
            CheckResult(
                name="daemon",
                status=FAIL,
                detail=str(exc),
                hint=SERVE_HINT,
            ),
            None,
        )
    counts = health.get("counts") or {}
    detail = (
        f"reachable at {client.base_url}; "
        f"{counts.get('objects', 0)} objects, {counts.get('runs', 0)} runs, "
        f"{counts.get('environment_builds', 0)} cached environments"
    )
    return CheckResult(name="daemon", status=OK, detail=detail), health


def check_server_connection(health: dict[str, Any] | None) -> CheckResult:
    """Classify the central-server connection from the /health payload."""

    name = "server connection"
    if health is None:
        return CheckResult(
            name=name,
            status=SKIP,
            detail="skipped: daemon is not reachable",
        )
    server = health.get("server") or {}
    connection = server.get("connection") or {}
    server_url = connection.get("server_url") or "central server"
    if server.get("connected"):
        return CheckResult(
            name=name,
            status=OK,
            detail=f"connected to {server_url}",
        )
    if connection.get("status") == SERVER_CONNECTION_STATUS_NEEDS_RECONNECT:
        error = str(connection.get("error") or "")
        code = _lease_rejection_code(error) or "unknown"
        return CheckResult(
            name=name,
            status=WARN,
            detail=f"lease rejected by server ({code}) - identity kept; run connect_server to restore sync",
            hint="reconnect with `client.connect_server(...)` or `spl-daemon server-connect`",
        )
    if server.get("offline"):
        error = connection.get("error") or connection.get("status") or "unknown error"
        return CheckResult(
            name=name,
            status=FAIL,
            detail=f"connection to {server_url} is failing: {error}",
            hint="check tokens and network, then re-run `server-connect`",
        )
    return CheckResult(
        name=name,
        status=OK,
        detail="no server connection configured (local-only mode)",
    )


def _lease_rejection_code(error: str) -> str | None:
    prefix = "lease rejected by server ("
    if not error.startswith(prefix):
        return None
    suffix = error[len(prefix) :]
    code, _, _ = suffix.partition(")")
    return code or None


def check_server_identity(health: dict[str, Any] | None) -> CheckResult:
    """Report the locally selected server owner/machine identity."""

    name = "server identity"
    if health is None:
        return CheckResult(
            name=name,
            status=SKIP,
            detail="skipped: daemon is not reachable",
        )
    server = health.get("server") or {}
    connection = server.get("connection") or {}
    summary = server.get("connection_summary") or {}
    owner_id = connection.get("owner_id") or "not enrolled"
    machine_id = connection.get("machine_id") or summary.get("current_machine_id") or "unknown machine"
    stored_count = _as_int(summary.get("stored_count"))
    stale_count = _as_int(summary.get("stale_count"))
    held_sync_events = _as_int(summary.get("held_sync_events"))
    offline_replaced_identity_rows = _as_int(summary.get("offline_replaced_identity_rows"))
    owners = [str(owner) for owner in summary.get("owners") or [] if owner]
    if summary.get("identity_degraded"):
        if offline_replaced_identity_rows:
            return CheckResult(
                name=name,
                status=WARN,
                detail=(
                    "identity row replaced while offline — reconnect to restore; "
                    f"{offline_replaced_identity_rows} stored identity rows lost secrets"
                ),
                hint=(
                    "reconnect with `client.connect_server(...)`, then inspect "
                    "`connections-list` and prune stale rows with `connections-prune --dry-run`"
                ),
            )
        return CheckResult(
            name=name,
            status=WARN,
            detail=(
                f"identity degraded to 'local'; {stored_count} stored credential rows exist; "
                "run spl-daemon doctor / connections-prune"
            ),
            hint="inspect `connections-list` and prune stale rows with `connections-prune --dry-run`",
        )
    detail = (
        f"enrolled as {owner_id} on {machine_id}; "
        f"{stored_count} stored connections ({stale_count} stale); "
        f"{held_sync_events} sync events held for other identities"
    )
    if len(set(owners)) > 1:
        return CheckResult(
            name=name,
            status=WARN,
            detail=detail,
            hint="multiple owners stored locally: {}; inspect `connections-list` and prune stale rows with `connections-prune --dry-run`".format(
                ", ".join(sorted(set(owners)))
            ),
        )
    return CheckResult(name=name, status=OK, detail=detail)


def check_environment_builds(health: dict[str, Any] | None) -> CheckResult:
    """Report failed cached environment builds from the /health payload."""

    name = "environment builds"
    if health is None:
        return CheckResult(
            name=name,
            status=SKIP,
            detail="skipped: daemon is not reachable",
        )
    by_status = (health.get("environment_builds") or {}).get("by_status") or {}
    failed = _as_int(by_status.get("failed"))
    total = sum(_as_int(count) for count in by_status.values())
    if failed:
        return CheckResult(
            name=name,
            status=WARN,
            detail=f"{failed} of {total} cached builds failed",
            hint=("inspect with `env-build-list`, then retry with `env-build-rebuild <spec_hash>`"),
        )
    if total == 0:
        return CheckResult(
            name=name,
            status=OK,
            detail="no cached builds yet (they appear after the first run)",
        )
    return CheckResult(name=name, status=OK, detail=f"{total} cached builds healthy")


def check_interpreter_substitutions(health: dict[str, Any] | None) -> CheckResult:
    """Warn when server-origin objects resolve to a different Python minor."""

    name = "interpreter versions"
    if health is None:
        return CheckResult(
            name=name,
            status=SKIP,
            detail="skipped: daemon is not reachable",
        )
    substitutions = health.get("interpreter_substitutions") or {}
    raw_items = substitutions.get("items") if isinstance(substitutions, dict) else []
    items = raw_items if isinstance(raw_items, list) else []
    mismatches = [
        item
        for item in items
        if isinstance(item, dict)
        and (
            item.get("minor_mismatch")
            or python_minor_mismatch(
                item.get("authored_python_version"),
                item.get("resolved_python_version"),
            )
        )
    ]
    if not mismatches:
        return CheckResult(
            name=name,
            status=OK,
            detail="no server-origin Python minor substitutions detected",
        )

    examples = ", ".join(
        str(item.get("display_name") or item.get("object") or item.get("version_id") or "server object")
        for item in mismatches[:3]
    )
    return CheckResult(
        name=name,
        status=WARN,
        detail=(
            f"{len(mismatches)} server-origin object version(s) resolve to a different Python minor"
            + (f": {examples}" if examples else "")
        ),
        hint="register a matching local env or republish after validating behavior on this Python version",
    )


def check_pipeline_adapter_tags(pipeline: Pipeline) -> CheckResult:
    """Statically compare save artifact tags with load accepted tags."""

    issues = find_pipeline_adapter_compatibility_issues(pipeline)
    if not issues:
        return CheckResult(
            name="adapter tags",
            status=OK,
            detail="pipeline adapter tags are statically compatible",
        )
    first = issues[0]
    return CheckResult(
        name="adapter tags",
        status=WARN,
        detail=first.detail + ("; {} total mismatches".format(len(issues)) if len(issues) > 1 else ""),
        hint=first.hint,
    )


def check_pipeline_adapter_probe(pipeline: Pipeline) -> CheckResult:
    """Run local save/load probes for pipeline adapters that declare examples."""

    report = probe_pipeline_adapters(pipeline)
    if report.failures:
        first = report.failures[0]
        return CheckResult(
            name="adapter probe",
            status=WARN,
            detail="{} of {} adapter probe(s) failed; first failure: {}: {}".format(
                len(report.failures), report.probed, first.adapter, first.reason
            ),
            hint="fix the adapter example/save/load round-trip before relying on resume repair",
        )
    if report.probed == 0:
        return CheckResult(
            name="adapter probe",
            status=SKIP,
            detail="no adapter examples declared for this pipeline",
        )
    return CheckResult(
        name="adapter probe",
        status=OK,
        detail="{} adapter example probe(s) passed".format(report.probed),
    )


def _as_int(value: Any) -> int:
    """Coerce a /health counter to ``int``; malformed payloads count as 0.

    The counters travel over HTTP from the daemon, so a version-skewed or
    hand-rolled daemon must degrade to "0" rather than crash the report
    (the module contract is "never an exception").
    """

    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def check_docker() -> CheckResult:
    """Probe Docker only as far as the optional Docker runtime needs it."""

    name = "docker"
    probe = _probe_docker_daemon()
    if probe.docker_path is None:
        return CheckResult(
            name=name,
            status=OK,
            detail="not installed (needed only for objects with the Docker runtime)",
        )
    if not probe.available:
        return CheckResult(
            name=name,
            status=WARN,
            detail=f"installed, but {probe.detail}",
            hint=probe.hint or "start Docker Desktop (or dockerd) before running Docker objects",
        )
    return CheckResult(name=name, status=OK, detail=probe.detail)


def check_node_docker(
    *,
    daemon_available: bool = False,
    explicit_image: str | None = None,
    inside_object_docker_worker: bool | None = None,
) -> CheckResult:
    """Report whether the per-node Docker runtime can run in this context."""

    name = "per-node docker"
    if inside_object_docker_worker is None:
        inside_object_docker_worker = _inside_object_docker_worker()
    if inside_object_docker_worker:
        return CheckResult(
            name=name,
            status=WARN,
            detail="unavailable: nested Docker runtimes are not supported inside an object Docker worker",
            hint="keep the object runtime on venv or drop the node tag",
        )

    image = explicit_image.strip() if explicit_image else None
    probe = _probe_docker_daemon()
    if probe.docker_path is None:
        return CheckResult(
            name=name,
            status=WARN,
            detail="unavailable: docker CLI is not on PATH",
            hint="install Docker Desktop and ensure the `docker` command is on PATH",
        )
    if not probe.available:
        return CheckResult(
            name=name,
            status=WARN,
            detail=f"unavailable: {probe.detail}",
            hint=probe.hint,
        )
    if not daemon_available and image is None:
        return CheckResult(
            name=name,
            status=WARN,
            detail="unavailable: no daemon-provided image_tag and no runtime_config.docker.image is set",
            hint="set runtime_config.docker.image or run via the daemon",
        )

    source = f"explicit image {image}" if image is not None else "daemon-provided image_tag"
    return CheckResult(
        name=name,
        status=OK,
        detail=f"available: docker CLI and daemon are ready; per-node image source is {source}",
    )


def _inside_object_docker_worker() -> bool:
    """Return True inside the object-level Docker worker container."""

    return (
        os.environ.get(OBJECT_DOCKER_RUNTIME_ENV) == OBJECT_DOCKER_RUNTIME_VALUE
        or os.environ.get(OBJECT_DOCKER_WORKER_ENV) == OBJECT_DOCKER_WORKER_VALUE
    )


def _probe_docker_daemon() -> _DockerProbeResult:
    """Probe local Docker without raising."""

    docker_path = shutil.which("docker")
    if docker_path is None:
        return _DockerProbeResult(
            docker_path=None,
            available=False,
            detail="docker CLI is not on PATH",
            hint="install Docker Desktop and ensure the `docker` command is on PATH",
        )
    try:
        completed = subprocess.run(
            [docker_path, "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=DOCKER_INFO_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return _DockerProbeResult(
            docker_path=docker_path,
            available=False,
            detail=f"`docker info` timed out after {DOCKER_INFO_TIMEOUT_SECONDS:g}s: {exc}",
            hint="start Docker Desktop (or dockerd) and wait until `docker info` succeeds",
        )
    except OSError as exc:
        return _DockerProbeResult(
            docker_path=docker_path,
            available=False,
            detail=f"`docker info` failed to start: {exc}",
            hint="start Docker Desktop (or dockerd) and wait until `docker info` succeeds",
        )
    if completed.returncode != 0:
        detail = (completed.stderr or "").strip().splitlines()
        return _DockerProbeResult(
            docker_path=docker_path,
            available=False,
            detail=("the Docker daemon is not reachable" + (f": {detail[0]}" if detail else "")),
            hint="start Docker Desktop (or dockerd) and wait until `docker info` succeeds",
        )
    return _DockerProbeResult(
        docker_path=docker_path,
        available=True,
        detail=f"available ({docker_path})",
    )
