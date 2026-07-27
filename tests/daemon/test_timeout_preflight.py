from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator

import pytest

from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore


@pytest.fixture
def store(tmp_path: Path) -> Iterator[RegistryStore]:
    registry = RegistryStore(tmp_path)
    try:
        yield registry
    finally:
        registry.close()


@pytest.mark.parametrize(
    "timeout_value",
    [-1, True, "1", float("nan"), float("inf"), float("-inf")],
    ids=["negative", "boolean", "string", "nan", "positive-infinity", "negative-infinity"],
)
def test_run_repository_rejects_invalid_timeout_before_run_creation(
    store: RegistryStore,
    timeout_value: Any,
) -> None:
    with pytest.raises(ValueError, match="timeout_seconds"):
        store.create_run("missing-object", timeout_seconds=timeout_value)

    assert list(store.runs_dir.iterdir()) == []


@pytest.mark.parametrize("timeout_value", [None, 0, 1, 1.25])
def test_run_repository_preserves_valid_timeout_domain(
    store: RegistryStore,
    timeout_value: float | None,
) -> None:
    with pytest.raises(KeyError, match="object is not registered"):
        store.create_run("missing-object", timeout_seconds=timeout_value)

    assert list(store.runs_dir.iterdir()) == []


@pytest.mark.parametrize(
    "timeout_value",
    [-1, True, "1", {"unexpected": 1}, float("nan"), float("inf"), float("-inf")],
    ids=["negative", "boolean", "string", "mapping", "nan", "positive-infinity", "negative-infinity"],
)
def test_stored_worker_timeout_is_rejected_before_backend_selection(
    tmp_path: Path,
    timeout_value: Any,
) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps({"timeout_seconds": timeout_value}), encoding="utf-8")
    runtime = object.__new__(DaemonRuntime)

    with pytest.raises(ValueError, match="timeout_seconds"):
        runtime._read_timeout(input_path)


@pytest.mark.parametrize(
    ("timeout_value", "expected"),
    [(None, None), (0, 0.0), (1, 1.0), (1.25, 1.25)],
)
def test_stored_worker_timeout_preserves_valid_domain(
    tmp_path: Path,
    timeout_value: float | None,
    expected: float | None,
) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps({"timeout_seconds": timeout_value}), encoding="utf-8")
    runtime = object.__new__(DaemonRuntime)

    assert runtime._read_timeout(input_path) == expected


@pytest.mark.parametrize(
    "timeout_value",
    [float("nan"), float("inf"), float("-inf")],
    ids=["nan", "positive-infinity", "negative-infinity"],
)
@pytest.mark.parametrize("boundary", ["start-run", "resume-run", "start-remote-run", "run-remote-node"])
def test_runtime_boundaries_reject_non_finite_timeout_before_external_work(
    timeout_value: float,
    boundary: str,
) -> None:
    runtime = object.__new__(DaemonRuntime)

    with pytest.raises(ValueError, match="timeout_seconds"):
        if boundary == "start-run":
            runtime.start_run("missing-object", source="local", timeout_seconds=timeout_value)
        elif boundary == "resume-run":
            runtime.resume_run("missing-run", from_="node", timeout_seconds=timeout_value)
        elif boundary == "start-remote-run":
            runtime.start_remote_run("missing-object", timeout_seconds=timeout_value)
        else:
            runtime.run_remote_node({"name": "missing-object"}, kwargs={}, timeout_seconds=timeout_value)


def test_invalid_stored_timeout_finalizes_run_before_backend_selection(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "run-1"
    run_dir.mkdir(parents=True)
    (run_dir / "input.json").write_text('{"timeout_seconds": NaN}', encoding="utf-8")
    state = {
        "id": "run-1",
        "object_version_id": "version-1",
        "run_dir": str(run_dir),
        "result_path": str(run_dir / "result.json"),
        "artifacts_dir": str(run_dir / "artifacts"),
        "entrypoint": "pipeline",
        "parent_run_id": None,
    }
    object_record = {
        "yaml": "[]\n",
        "distributions": [],
        "pipeline_nodes": [],
        "workdir": str(run_dir),
    }
    runtime = object.__new__(DaemonRuntime)
    runtime.store = SimpleNamespace(
        home=tmp_path,
        get_run=lambda _: state,
        get_object_version=lambda _: object_record,
    )
    runtime.daemon_base_url = "http://127.0.0.1:8765"
    updates: list[dict[str, Any]] = []
    terminal_updates: list[dict[str, Any]] = []

    def record_update(*_: Any, **changes: Any) -> None:
        updates.append(changes)

    def record_terminal_update(*_: Any, **changes: Any) -> None:
        terminal_updates.append(changes)

    runtime._update_local_run = record_update  # type: ignore[method-assign]
    runtime._update_local_run_terminal = record_terminal_update  # type: ignore[method-assign]

    runtime._execute_run("run-1", report_local_run=False)

    assert updates[-1]["status"] == "preparing_environment"
    assert len(terminal_updates) == 1
    terminal = terminal_updates[0]
    assert terminal["report_local_run"] is False
    assert terminal["status"] == "failed"
    assert terminal["finished_at"]
    assert terminal["error"] == "ValueError('timeout_seconds must be a finite non-negative number or None')"
