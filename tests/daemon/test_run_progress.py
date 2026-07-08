"""Environment-build progress for polled runs (Stage 2.2 / TODO #34).

While a run sits in ``preparing_environment``, ``GET /runs/<id>`` should
describe the environment build (status, elapsed time, last install-log line)
so a first run does not look frozen for minutes.  The payload is best effort:
anything unknown means the field is simply absent, never an error.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.run_progress import (
    _elapsed_seconds,
    _last_log_line,
    environment_progress,
)
from spl.daemon.interpreter_visibility import INTERPRETER_RESOLUTION_KEY
from spl.daemon.store import RegistryStore

from .test_object_identity import FUNCTION_YAML


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[RegistryStore]:
    registry = RegistryStore(tmp_path)
    try:
        yield registry
    finally:
        registry.close()


def _upsert_build(
    store: RegistryStore,
    tmp_path: Path,
    *,
    spec_hash: str = "hash-1",
    status: str = "creating",
    log_lines: list[str] | None = None,
    spec: dict[str, Any] | None = None,
) -> dict[str, Any]:
    env_dir = tmp_path / "env-builds" / spec_hash
    env_dir.mkdir(parents=True, exist_ok=True)
    install_log_path = env_dir / "install.log"
    if log_lines is not None:
        install_log_path.write_text("\n".join(log_lines), encoding="utf-8")
    record = store.upsert_environment_build(
        spec_hash=spec_hash,
        base_python=sys.executable,
        python_version="Python 3.13",
        distributions=[],
        runtime_packages=[],
        spec=spec or {},
        venv_path=env_dir / "venv",
        python_path=env_dir / "venv" / "bin" / "python",
        install_log_path=install_log_path,
        status=status,
    )
    if status == "creating":
        record = store.update_environment_build(
            spec_hash,
            status=status,
            started_at=datetime.now(UTC).isoformat(),
        )
    return record


class TestEnvironmentProgress:
    def test_none_for_non_preparing_status(self, store: RegistryStore) -> None:
        state = {"status": "running", "env_build_hash": "hash-1"}
        assert environment_progress(store, state) is None

    def test_none_without_build_hash(self, store: RegistryStore) -> None:
        state = {"status": "preparing_environment", "env_build_hash": None}
        assert environment_progress(store, state) is None

    def test_none_for_unknown_build_record(self, store: RegistryStore) -> None:
        state = {"status": "preparing_environment", "env_build_hash": "missing"}
        assert environment_progress(store, state) is None

    def test_none_when_store_raises(self) -> None:
        class ExplodingStore:
            def get_environment_build(self, spec_hash: str) -> None:
                raise RuntimeError("boom")

        state = {"status": "preparing_environment", "env_build_hash": "hash-1"}
        assert environment_progress(ExplodingStore(), state) is None

    def test_creating_build_payload(
        self,
        store: RegistryStore,
        tmp_path: Path,
    ) -> None:
        _upsert_build(
            store,
            tmp_path,
            log_lines=["Creating venv", "", "Collecting numpy==2.1.0", "  "],
        )
        state = {"status": "preparing_environment", "env_build_hash": "hash-1"}

        progress = environment_progress(store, state)

        assert progress is not None
        assert progress["status"] == "creating"
        assert progress["spec_hash"] == "hash-1"
        assert progress["runtime_type"] == "venv"
        assert progress["error"] is None
        assert progress["log_tail"] == "Collecting numpy==2.1.0"
        assert progress["log_path"].endswith("install.log")
        assert progress["elapsed_seconds"] is not None
        assert progress["elapsed_seconds"] >= 0.0

    def test_missing_log_file_omits_tail_only(
        self,
        store: RegistryStore,
        tmp_path: Path,
    ) -> None:
        _upsert_build(store, tmp_path, log_lines=None)
        state = {"status": "preparing_environment", "env_build_hash": "hash-1"}

        progress = environment_progress(store, state)

        assert progress is not None
        assert "log_tail" not in progress
        assert progress["log_path"].endswith("install.log")

    def test_preparing_payload_exposes_interpreter_substitution(
        self,
        store: RegistryStore,
        tmp_path: Path,
    ) -> None:
        _upsert_build(
            store,
            tmp_path,
            spec={
                INTERPRETER_RESOLUTION_KEY: {
                    "authored_python": "/author/bin/python",
                    "authored_python_version": "Python 3.11.9",
                    "resolved_python": "/local/bin/python",
                    "resolved_python_version": "Python 3.13.0",
                    "reason": "local_env",
                    "reason_detail": "spl_core",
                    "substituted": True,
                }
            },
        )
        state = {"status": "preparing_environment", "env_build_hash": "hash-1"}

        progress = environment_progress(store, state)

        assert progress is not None
        assert progress["interpreter_substitution"] == {
            "authored_python": "/author/bin/python",
            "authored_python_version": "Python 3.11.9",
            "resolved_python": "/local/bin/python",
            "resolved_python_version": "Python 3.13.0",
            "reason": "local_env",
            "reason_detail": "spl_core",
            "minor_mismatch": True,
        }


class TestHelpers:
    def test_elapsed_handles_naive_and_garbage_timestamps(self) -> None:
        naive = (datetime.now(UTC) - timedelta(seconds=30)).replace(tzinfo=None)
        elapsed = _elapsed_seconds(naive.isoformat())
        assert elapsed is not None
        assert 25.0 <= elapsed <= 120.0

        assert _elapsed_seconds("not-a-date") is None
        assert _elapsed_seconds(None) is None
        assert _elapsed_seconds("") is None

    def test_elapsed_is_clamped_to_zero_for_future_start(self) -> None:
        future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
        assert _elapsed_seconds(future) == 0.0

    def test_last_log_line_reads_only_the_tail(self, tmp_path: Path) -> None:
        log_path = tmp_path / "install.log"
        filler = "\n".join(f"line {index}" for index in range(5000))
        log_path.write_text(filler + "\nfinal line\n\n", encoding="utf-8")

        assert _last_log_line(log_path) == "final line"

    def test_last_log_line_for_missing_or_empty_file(self, tmp_path: Path) -> None:
        assert _last_log_line(tmp_path / "absent.log") is None

        empty = tmp_path / "empty.log"
        empty.write_text("", encoding="utf-8")
        assert _last_log_line(empty) is None


class TestRunEndpoint:
    def test_get_run_attaches_environment_while_preparing(
        self,
        tmp_path: Path,
    ) -> None:
        from spl.daemon.server import create_app

        from .test_daemon_endpoint import _json_from_app, _shutdown_app

        store = RegistryStore(tmp_path)
        app = None
        try:
            store.register_env("default", sys.executable)
            store.register_object(
                "progress_probe",
                "demo_obj",
                "default",
                yaml_text=FUNCTION_YAML,
            )
            app = create_app(store, auto_build_envs=False)
            run_state = store.create_run("progress_probe")
            spec_hash = run_state["env_build_hash"]
            assert spec_hash, "create_run should stamp the environment spec hash"
            _upsert_build(
                store,
                tmp_path,
                spec_hash=spec_hash,
                log_lines=["Creating venv", "Collecting pyyaml"],
            )
            store.update_run(run_state["id"], status="preparing_environment")

            status, body = _json_from_app(app, f"/runs/{run_state['id']}")

            assert status == 200
            assert body["environment"]["status"] == "creating"
            assert body["environment"]["spec_hash"] == spec_hash
            assert body["environment"]["log_tail"] == "Collecting pyyaml"

            store.update_run(run_state["id"], status="running")
            status, body = _json_from_app(app, f"/runs/{run_state['id']}")

            assert status == 200
            assert "environment" not in body
        finally:
            _shutdown_app(app)
            store.close()
