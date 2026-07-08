"""Derived environment-build progress for runs that are still preparing.

The daemon stores run rows and environment-build rows separately.  While a run
sits in ``preparing_environment`` the interesting facts (build status, how long
it has been running, the last install-log line) live in the environment-build
record.  This module joins the two into one small read-only payload that the
run routes can attach to a polled run state, so clients can show progress
instead of staying silent for minutes on a first run.

The payload is best effort by design: any missing hash, record, or log file
means "no extra information", never an error, because progress reporting must
not be able to break run polling.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from spl.daemon.interpreter_visibility import environment_record_interpreter_substitution

PREPARING_ENVIRONMENT_STATUS = "preparing_environment"
LOG_TAIL_MAX_BYTES = 8192


class EnvironmentBuildReader(Protocol):
    """The single store capability this module needs."""

    def get_environment_build(self, spec_hash: str) -> dict[str, Any] | None:
        """Return one environment-build record or ``None``."""
        ...


def environment_progress(
    store: EnvironmentBuildReader,
    run_state: dict[str, Any],
) -> dict[str, Any] | None:
    """Return build progress for a preparing run, or ``None`` when unknown.

    ``None`` means the run either does not need the extra payload (it is not
    preparing an environment) or the daemon cannot map it to a build record
    yet.  Callers should simply omit the field in that case.
    """

    if run_state.get("status") != PREPARING_ENVIRONMENT_STATUS:
        return None
    spec_hash = run_state.get("env_build_hash")
    if not spec_hash:
        return None
    try:
        record = store.get_environment_build(str(spec_hash))
    except Exception:
        # Progress is advisory; a store error here must not fail `GET /runs`.
        return None
    if record is None:
        return None

    progress: dict[str, Any] = {
        # ``.get`` with fallbacks keeps the "never break run polling" promise
        # even for store implementations that return a sparse record.
        "spec_hash": record.get("spec_hash") or str(spec_hash),
        "status": record.get("status") or "unknown",
        "runtime_type": record.get("runtime_type") or "venv",
        "started_at": record.get("started_at"),
        "elapsed_seconds": _elapsed_seconds(record.get("started_at")),
        "error": record.get("error"),
    }
    substitution = environment_record_interpreter_substitution(record)
    if substitution is not None:
        progress["interpreter_substitution"] = substitution
    log_path = record.get("install_log_path")
    if log_path:
        progress["log_path"] = str(log_path)
        log_tail = _last_log_line(Path(log_path))
        if log_tail is not None:
            progress["log_tail"] = log_tail
    return progress


def _elapsed_seconds(started_at: Any) -> float | None:
    """Return non-negative seconds since ``started_at``, or ``None``."""

    if not started_at:
        return None
    try:
        parsed = datetime.fromisoformat(str(started_at))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    elapsed = (datetime.now(UTC) - parsed.astimezone(UTC)).total_seconds()
    return max(elapsed, 0.0)


def _last_log_line(log_path: Path) -> str | None:
    """Return the last non-empty line of a build log, reading only its tail."""

    try:
        with log_path.open("rb") as log:
            log.seek(0, os.SEEK_END)
            size = log.tell()
            log.seek(max(size - LOG_TAIL_MAX_BYTES, 0))
            tail = log.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        stripped = line.strip()
        if stripped:
            return stripped
    return None
