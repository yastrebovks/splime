"""Bounded subprocess execution with POSIX process-tree termination."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import IO

from spl._timeout import TimeoutDomain, validate_timeout_seconds

DEFAULT_TERMINATION_GRACE_SECONDS = 0.5
WINDOWS_TIMEOUT_UNSUPPORTED = (
    "finite subprocess timeouts require Windows Job Object support; "
    "timed worker execution is currently supported only on POSIX"
)


def run_process_tree(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float | None = None,
    termination_grace_seconds: float = DEFAULT_TERMINATION_GRACE_SECONDS,
    stdout_target: int | IO[str] | None = subprocess.PIPE,
    stderr_target: int | IO[str] | None = subprocess.PIPE,
) -> subprocess.CompletedProcess[str]:
    """Run ``command`` and terminate its complete POSIX process group on timeout.

    A finite timeout fails closed on non-POSIX systems. Windows process groups
    do not contain arbitrary descendants; equivalent support requires assigning
    the child to a Job Object at creation time.
    """

    validate_timeout_seconds(
        timeout,
        name="process timeout",
        domain=TimeoutDomain.FINITE,
        allow_none=True,
    )
    validate_timeout_seconds(
        termination_grace_seconds,
        name="termination_grace_seconds",
        domain=TimeoutDomain.NON_NEGATIVE,
        allow_none=False,
    )
    process_groups_supported = _posix_process_groups_supported()
    if timeout is not None and not process_groups_supported:
        raise RuntimeError(WINDOWS_TIMEOUT_UNSUPPORTED)

    process = subprocess.Popen(
        list(command),
        cwd=cwd,
        env=env,
        text=True,
        stdout=stdout_target,
        stderr=stderr_target,
        start_new_session=process_groups_supported,
    )
    try:
        stdout_text, stderr_text = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        stdout_text, stderr_text = _terminate_timed_out_process(
            process,
            grace_seconds=termination_grace_seconds,
        )
        assert timeout is not None
        raise subprocess.TimeoutExpired(
            command,
            timeout,
            output=stdout_text,
            stderr=stderr_text,
        ) from exc

    return subprocess.CompletedProcess(
        list(command),
        process.returncode,
        stdout=stdout_text,
        stderr=stderr_text,
    )


def _terminate_timed_out_process(
    process: subprocess.Popen[str],
    *,
    grace_seconds: float,
) -> tuple[str, str]:
    """Terminate, force-kill, and reap one timed-out POSIX process group."""

    _signal_process_group(process.pid, signal.SIGTERM)
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if not _process_group_exists(process.pid):
            break
        time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
    _signal_process_group(process.pid, signal.SIGKILL)
    stdout, stderr = process.communicate()
    return stdout or "", stderr or ""


def _posix_process_groups_supported() -> bool:
    return os.name == "posix"


def _process_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _signal_process_group(process_group_id: int, sig: signal.Signals) -> None:
    try:
        os.killpg(process_group_id, sig)
    except ProcessLookupError:
        return
    except PermissionError:
        # macOS can report EPERM for a process group containing only a zombie
        # leader between termination and the final wait. The leader is still
        # reaped by ``communicate`` below; a live same-user group is killable.
        return
