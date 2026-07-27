from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from spl import _process as m_process


CHILD_CODE = """
import os
import signal
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(sys.argv[1]).write_text(str(os.getpid()), encoding="utf-8")
time.sleep(1.0)
Path(sys.argv[2]).write_text("survived", encoding="utf-8")
time.sleep(10.0)
"""

PARENT_CODE = """
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, "-c", sys.argv[1], sys.argv[3], sys.argv[4]])
Path(sys.argv[2]).write_text(str(os.getpid()), encoding="utf-8")
deadline = time.monotonic() + 2.0
while not Path(sys.argv[3]).exists() and time.monotonic() < deadline:
    time.sleep(0.01)
print("tree-ready", flush=True)
time.sleep(10.0)
"""


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_pid_absent(pid: int, *, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(0.02)
    return not _pid_exists(pid)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group contract")
def test_timeout_kills_sigterm_ignoring_parent_and_descendant(tmp_path: Path) -> None:
    parent_pid_path = tmp_path / "parent.pid"
    child_pid_path = tmp_path / "child.pid"
    survived_marker = tmp_path / "survived.txt"
    parent_pid: int | None = None
    child_pid: int | None = None

    try:
        with pytest.raises(subprocess.TimeoutExpired) as exc_info:
            m_process.run_process_tree(
                [
                    sys.executable,
                    "-c",
                    PARENT_CODE,
                    CHILD_CODE,
                    str(parent_pid_path),
                    str(child_pid_path),
                    str(survived_marker),
                ],
                timeout=0.4,
                termination_grace_seconds=0.1,
            )

        assert exc_info.value.output == "tree-ready\n"
        parent_pid = int(parent_pid_path.read_text(encoding="utf-8"))
        child_pid = int(child_pid_path.read_text(encoding="utf-8"))
        assert _wait_pid_absent(parent_pid)
        assert _wait_pid_absent(child_pid)
        time.sleep(0.7)
        assert not survived_marker.exists()
    finally:
        for pid in (parent_pid, child_pid):
            if pid is None or not _pid_exists(pid):
                continue
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_finite_timeout_fails_closed_without_posix_process_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(m_process, "_posix_process_groups_supported", lambda: False)

    with pytest.raises(RuntimeError, match="Windows Job Object support"):
        m_process.run_process_tree([sys.executable, "--version"], timeout=1.0)


@pytest.mark.parametrize(
    "timeout",
    [float("nan"), float("inf"), float("-inf")],
    ids=["nan", "positive-infinity", "negative-infinity"],
)
def test_non_finite_timeout_is_rejected_before_popen(
    monkeypatch: pytest.MonkeyPatch,
    timeout: float,
) -> None:
    popen_called = False

    def forbidden_popen(*args: Any, **kwargs: Any) -> Any:
        nonlocal popen_called
        del args, kwargs
        popen_called = True
        pytest.fail("Popen must not be called for a non-finite timeout")

    monkeypatch.setattr(m_process.subprocess, "Popen", forbidden_popen)

    with pytest.raises(ValueError, match="process timeout must be a finite number or None"):
        m_process.run_process_tree([sys.executable, "--version"], timeout=timeout)

    assert popen_called is False


class _ImmediateProcess:
    returncode = 0

    def __init__(self) -> None:
        self.timeout: float | None = None

    def communicate(self, *, timeout: float | None = None) -> tuple[str, str]:
        self.timeout = timeout
        return "", ""


@pytest.mark.parametrize("timeout", [None, 1, 1.25, 0, -1])
def test_process_timeout_preserves_every_finite_deadline(
    monkeypatch: pytest.MonkeyPatch,
    timeout: float | None,
) -> None:
    process = _ImmediateProcess()

    def fake_popen(*args: Any, **kwargs: Any) -> _ImmediateProcess:
        del args, kwargs
        return process

    monkeypatch.setattr(m_process, "_posix_process_groups_supported", lambda: True)
    monkeypatch.setattr(m_process.subprocess, "Popen", fake_popen)

    completed = m_process.run_process_tree([sys.executable, "--version"], timeout=timeout)

    assert completed.returncode == 0
    assert process.timeout == timeout


@pytest.mark.parametrize("timeout", ["1", True, object()], ids=["string", "boolean", "object"])
def test_process_timeout_rejects_non_numeric_values_before_popen(
    monkeypatch: pytest.MonkeyPatch,
    timeout: object,
) -> None:
    def forbidden_popen(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        pytest.fail("Popen must not be called for an invalid timeout type")

    monkeypatch.setattr(m_process.subprocess, "Popen", forbidden_popen)

    with pytest.raises(ValueError, match="process timeout must be a finite number or None"):
        m_process.run_process_tree(
            [sys.executable, "--version"],
            timeout=timeout,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "grace_seconds",
    [-1, True, "0.5", object(), float("nan"), float("inf"), float("-inf")],
    ids=["negative", "boolean", "string", "object", "nan", "positive-infinity", "negative-infinity"],
)
def test_invalid_termination_grace_is_rejected_before_popen(
    monkeypatch: pytest.MonkeyPatch,
    grace_seconds: object,
) -> None:
    def forbidden_popen(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        pytest.fail("Popen must not be called for an invalid termination grace")

    monkeypatch.setattr(m_process.subprocess, "Popen", forbidden_popen)

    with pytest.raises(ValueError, match="termination_grace_seconds"):
        m_process.run_process_tree(
            [sys.executable, "--version"],
            termination_grace_seconds=grace_seconds,  # type: ignore[arg-type]
        )
