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
        lines.append(
            f"\n{counts[OK]} ok, {counts[WARN]} warnings, "
            f"{counts[FAIL]} failures, {counts[SKIP]} skipped"
        )
        return "\n".join(lines)


def run_doctor(
    client: DaemonHealthClient,
    *,
    home: str | Path | None = None,
) -> DoctorReport:
    """Run all diagnostic checks and return the collected report."""

    home_path = Path(home) if home is not None else default_daemon_home()
    checks = [
        check_python(),
        check_venv_tooling(),
        check_daemon_home(home_path),
        check_disk_space(home_path),
    ]
    daemon_check, health = check_daemon(client)
    checks.append(daemon_check)
    checks.append(check_server_connection(health))
    checks.append(check_environment_builds(health))
    checks.append(check_docker())
    return DoctorReport(checks=checks)


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
            hint=(
                "inspect with `env-build-list`, then retry with "
                "`env-build-rebuild <spec_hash>`"
            ),
        )
    if total == 0:
        return CheckResult(
            name=name,
            status=OK,
            detail="no cached builds yet (they appear after the first run)",
        )
    return CheckResult(name=name, status=OK, detail=f"{total} cached builds healthy")


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
    docker_path = shutil.which("docker")
    if docker_path is None:
        return CheckResult(
            name=name,
            status=OK,
            detail="not installed (needed only for objects with the Docker runtime)",
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
    except (OSError, subprocess.TimeoutExpired) as exc:
        return CheckResult(
            name=name,
            status=WARN,
            detail=f"`docker info` did not answer: {exc}",
            hint="Docker runtime objects will fail until the Docker daemon responds",
        )
    if completed.returncode != 0:
        detail = (completed.stderr or "").strip().splitlines()
        return CheckResult(
            name=name,
            status=WARN,
            detail=(
                "installed, but the Docker daemon is not reachable"
                + (f": {detail[0]}" if detail else "")
            ),
            hint="start Docker Desktop (or dockerd) before running Docker objects",
        )
    return CheckResult(name=name, status=OK, detail=f"available ({docker_path})")
