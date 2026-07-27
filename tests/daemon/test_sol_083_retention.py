from __future__ import annotations

import asyncio
import json
import socket
import sqlite3
import sys
import threading
import time
import warnings
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from spl import SPLClient, lift
from spl._client import RemoteRun
from spl.core.entities.pipeline import Pipeline
from spl.core.ir.utils import spl_export_to_file
import spl.daemon.server as daemon_server_module
from spl.daemon import worker as worker_module
import spl.daemon.repositories.run as run_repository_module
from spl.daemon.remote_client import ServerClientError
from spl.daemon.server import DaemonRuntime, create_app
from spl.daemon.storage_base import (
    RUN_RETENTION_DELIVERY_MIGRATION_ID,
    RUN_RETENTION_MIGRATION_ID,
    SYNC_EVENT_TELEMETRY_SCHEMA_VERSION,
)
from spl.daemon.store import RegistryStore
from spl.daemon.worker_runtime_marker import WORKER_MANIFEST_HANDOFF_FILE
from spl.daemon_client import ClientError


FUNCTION_YAML = """\
- !DFunction
  name: demo_obj
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 1
"""

ARTIFACT_FUNCTION_YAML = """\
- !DFunction
  name: artifact_obj
  inputs: []
  outputs:
  - name: default
    type: dict
  body: |-
    from pathlib import Path
    Path("artifact.txt").write_text("durable artifact", encoding="utf-8")
    return {
        "__spl_result__": {"ok": True},
        "__spl_artifacts__": {"artifact.txt": "artifact.txt"},
    }
"""


def _pipeline_value(should_fail: bool = False) -> str:
    if should_fail:
        raise RuntimeError("retention pipeline failed")
    return "retention-value"


def _seed_store(home: Path) -> RegistryStore:
    store = RegistryStore(home)
    store.register_env("default", sys.executable)
    store.register_object("demo_obj", "demo_obj", "default", yaml_text=FUNCTION_YAML)
    return store


def _succeed(store: RegistryStore, run_id: str) -> dict[str, Any]:
    store.update_run(run_id, status="starting")
    store.update_run(run_id, status="preparing_environment")
    store.update_run(run_id, status="running")
    return store.update_run(run_id, status="succeeded", finished_at="2026-07-15T10:00:00+00:00")


def _fail(store: RegistryStore, run_id: str) -> dict[str, Any]:
    return store.update_run(
        run_id,
        status="failed",
        finished_at="2026-07-15T10:00:00+00:00",
        error="boom",
    )


def _wait_terminal(store: RegistryStore, run_id: str, timeout_seconds: float = 10.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        state = store.get_run(run_id)
        if state["status"] in {"succeeded", "failed"} and (
            not state.get("retention_enforced") or state.get("retention_terminal_queued")
        ):
            return state
        time.sleep(0.02)
    raise AssertionError("run did not reach a terminal state: {}".format(run_id))


def _export_retention_pipeline(path: Path) -> str:
    pipeline = lift(_pipeline_value).alias("result").render("retention_pipeline")
    spl_export_to_file(path, [pipeline])
    return path.read_text(encoding="utf-8")


def test_daemon_omitted_keep_preserves_observed_retain_all_default(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("demo_obj")

        assert run["keep"] is True
        assert run["input"]["keep"] is True
        assert run["retention_enforced"] is True
    finally:
        store.close()


def test_worker_forwards_exact_keep_and_returns_deferred_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = Pipeline()
    seen: dict[str, Any] = {}

    monkeypatch.setattr(
        worker_module,
        "load_entrypoint_with_namespace",
        lambda *_args, **_kwargs: (pipeline, {}),
    )

    def fake_run_pipeline(*_args: Any, keep: Any, **_kwargs: Any) -> tuple[Any, dict[str, str], dict[str, Any]]:
        seen["keep"] = keep
        return None, {}, {"run_id": "worker-run", "status": "succeeded", "keep": keep}

    monkeypatch.setattr(worker_module, "run_pipeline", fake_run_pipeline)
    input_path = tmp_path / "input.json"
    input_path.write_text('{"keep":false}', encoding="utf-8")
    result_path = tmp_path / "result.json"

    result = worker_module.execute(
        object_yaml=tmp_path / "object.yaml",
        entrypoint="pipeline",
        input_path=input_path,
        result_path=result_path,
        artifacts_dir=tmp_path / "artifacts",
    )

    assert seen["keep"] is False
    assert result["manifest"]["keep"] is False
    assert result_path.exists()


def test_real_worker_hands_off_success_and_failure_manifests_for_keep_false(tmp_path: Path) -> None:
    object_yaml = tmp_path / "pipeline.yaml"
    _export_retention_pipeline(object_yaml)

    success_dir = tmp_path / "success"
    success_dir.mkdir()
    success_input = success_dir / "input.json"
    success_input.write_text(
        '{"kwargs":{"should_fail":false},"output":"result","keep":false}',
        encoding="utf-8",
    )
    success = worker_module.execute(
        object_yaml=object_yaml,
        entrypoint="retention_pipeline",
        input_path=success_input,
        result_path=success_dir / "result.json",
        artifacts_dir=success_dir / "artifacts",
    )

    assert success["manifest"]["status"] == "succeeded"
    assert success["manifest"]["keep"] is False
    assert len(success["manifest"]["nodes"]) == 1
    assert json.loads((success_dir / "result.json").read_text(encoding="utf-8"))["manifest"] == success["manifest"]

    failure_dir = tmp_path / "failure"
    failure_dir.mkdir()
    failure_input = failure_dir / "input.json"
    failure_input.write_text(
        '{"kwargs":{"should_fail":true},"output":"result","keep":false}',
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="retention pipeline failed"):
        worker_module.execute(
            object_yaml=object_yaml,
            entrypoint="retention_pipeline",
            input_path=failure_input,
            result_path=failure_dir / "result.json",
            artifacts_dir=failure_dir / "artifacts",
        )

    handoff = json.loads((failure_dir / WORKER_MANIFEST_HANDOFF_FILE).read_text(encoding="utf-8"))
    assert handoff["status"] == "failed"
    assert handoff["keep"] is False
    assert next(iter(handoff["nodes"].values()))["status"] == "failed"


def test_real_daemon_worker_enforces_all_four_retention_outcomes(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    runtime = None
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "retention_pipeline",
            "retention_pipeline",
            "default",
            yaml_text=_export_retention_pipeline(tmp_path / "pipeline.yaml"),
        )
        runtime = DaemonRuntime(store, auto_build_envs=False)

        cases = [
            (False, False, "succeeded", True),
            (False, True, "failed", True),
            ("on_failure", False, "succeeded", True),
            ("on_failure", True, "failed", False),
            (True, False, "succeeded", False),
        ]
        for keep, should_fail, expected_status, removed in cases:
            started = runtime.start_run(
                "retention_pipeline",
                kwargs={"should_fail": should_fail},
                output="result",
                source="local",
                keep=keep,
            )
            final = _wait_terminal(store, started["id"])
            assert final["status"] == expected_status
            if removed:
                assert Path(started["run_dir"]).exists()
                assert final["retention_delivery_required"] is True
                assert final["retention_delivery_acked"] is False
                assert final["retention_delivery_expires_at"] is not None
                acknowledged = runtime.acknowledge_run_delivery(started["id"])
                assert acknowledged["retention"]["removed"] is True
                final = store.get_run(started["id"])
                assert not Path(started["run_dir"]).exists()
                assert final["manifest"]["summary"]["node_count"] == 1
                assert final["input"] == {}
            elif keep == "on_failure":
                assert Path(started["run_dir"]).exists()
                assert final["manifest"]["retention"]["expires_at"] is not None
                assert final["retention_delivery_expires_at"] is None
            else:
                assert Path(started["run_dir"]).exists()
                assert final["manifest"]["retention"]["expires_at"] is None
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()


@pytest.mark.parametrize(
    ("keep", "status", "removed", "retention_class", "has_expiry"),
    [
        pytest.param(False, "succeeded", True, "transient", False, id="false-success"),
        pytest.param("on_failure", "succeeded", True, "on_failure", False, id="on-failure-success"),
        pytest.param("on_failure", "failed", False, "on_failure", True, id="on-failure-failed"),
        pytest.param(True, "succeeded", False, "keep", False, id="true-success"),
    ],
)
def test_terminal_retention_policy(
    tmp_path: Path,
    keep: bool | str,
    status: str,
    removed: bool,
    retention_class: str,
    has_expiry: bool,
) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("demo_obj", keep=keep)
        final = _succeed(store, run["id"]) if status == "succeeded" else _fail(store, run["id"])
        store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)

        result = store.runs.enforce_run_retention(run["id"], blocked_by_sync=False)
        if removed:
            assert result["reason"] == "consumer-delivery-pending"
            store.runs.acknowledge_run_delivery(run["id"])
            result = store.runs.enforce_run_retention(run["id"], blocked_by_sync=False)
        current = store.get_run(run["id"])

        assert result["removed"] is removed
        assert Path(run["run_dir"]).exists() is (not removed)
        assert current["status"] == status
        assert current["manifest"]["retention"]["class"] == retention_class
        assert (final["manifest"]["retention"]["expires_at"] is not None) is has_expiry
        if removed:
            assert current["input"] == {}
            assert current["result_present"] is False
            assert current["stdout"] is None
            assert current["stderr"] is None
            assert current["run_dir"] == ""
            assert "nodes" not in current["manifest"]
            assert current["manifest"]["summary"]["node_count"] == 0
    finally:
        store.close()


def test_pending_terminal_sync_blocks_cleanup_and_sent_payload_is_scrubbed(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("demo_obj", keep=False, report_local_run=True)
        final = _succeed(store, run["id"])
        event = store.enqueue_sync_event(
            "local_run_update",
            {"run": {**final, "status": "succeeded", "secret": "must-disappear"}},
        )
        store.runs.mark_retention_terminal_queued(run["id"], sync_required=True)
        store.runs.acknowledge_run_delivery(run["id"])

        blocked = store.enforce_run_retention(run["id"])
        manual_preview = store.prune_runs(run_id=run["id"], dry_run=True)
        delete_preview = store.delete_run(run["id"], dry_run=True)

        assert blocked["removed"] is False
        assert blocked["reason"] == "unsent-sync"
        assert manual_preview["count"] == 0
        assert [item["id"] for item in manual_preview["skipped_pending_sync"]] == [run["id"]]
        assert delete_preview == manual_preview
        with pytest.raises(RuntimeError, match="pending sync"):
            store.delete_run(run["id"])
        assert Path(run["run_dir"]).exists()

        store.mark_sync_event_sent(event["id"])
        cleaned = store.enforce_run_retention(run["id"])
        sent = store.get_sync_event(event["id"])

        assert cleaned["removed"] is True
        assert sent["payload"] == {"run": {"id": run["id"], "status": "succeeded"}}
        assert "secret" not in str(sent)
    finally:
        store.close()


def test_startup_sweep_finishes_crash_window_but_never_deletes_active_or_legacy_rows(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    active = store.create_run("demo_obj", keep=False)
    crash = store.create_run("demo_obj", keep="on_failure")
    _succeed(store, crash["id"])
    store.runs.mark_retention_terminal_queued(crash["id"], sync_required=False)
    store.runs.acknowledge_run_delivery(crash["id"])
    legacy = store.create_run("demo_obj", keep="on_failure")
    _succeed(store, legacy["id"])
    with store._storage._lock, store._storage._conn:  # noqa: SLF001 - migration compatibility fixture.
        store._storage._conn.execute(  # noqa: SLF001
            "UPDATE runs SET retention_enforced = 0 WHERE id = ?",
            (legacy["id"],),
        )
    store.close()

    restarted = RegistryStore(tmp_path)
    runtime = DaemonRuntime(restarted, auto_build_envs=False)
    try:
        assert not Path(crash["run_dir"]).exists()
        assert restarted.get_run(crash["id"])["status"] == "succeeded"
        assert Path(active["run_dir"]).exists()
        assert Path(legacy["run_dir"]).exists()
    finally:
        runtime.shutdown()
        restarted.close()


def test_startup_recovers_terminal_before_queue_marker_and_nonretryable_sync_blocks(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    unmarked = store.create_run("demo_obj", keep=False, report_local_run=True)
    _succeed(store, unmarked["id"])
    store.runs.acknowledge_run_delivery(unmarked["id"])
    blocked = store.create_run("demo_obj", keep=False, report_local_run=True)
    blocked_final = _succeed(store, blocked["id"])
    event = store.enqueue_sync_event("local_run_update", {"run": blocked_final})
    store.mark_sync_event_failed(event["id"], "permanent rejection", retryable=False)
    store.runs.acknowledge_run_delivery(blocked["id"])
    store.close()

    restarted = RegistryStore(tmp_path)
    runtime = DaemonRuntime(restarted, auto_build_envs=False)
    try:
        assert not Path(unmarked["run_dir"]).exists()
        assert Path(blocked["run_dir"]).exists()
        assert restarted.get_run(blocked["id"])["retention_terminal_queued"] is True
        assert restarted.sync_events.run_sync_state(blocked["id"])["unsent"] is True
    finally:
        runtime.shutdown()
        restarted.close()


def test_retention_rmtree_failure_is_retryable_and_double_enforcement_is_serialized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("demo_obj", keep=False)
        _succeed(store, run["id"])
        store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)
        store.runs.acknowledge_run_delivery(run["id"])
        original_rmtree = run_repository_module.shutil.rmtree

        def fail_rmtree(_path: Path) -> None:
            raise OSError("injected cleanup crash")

        monkeypatch.setattr(run_repository_module.shutil, "rmtree", fail_rmtree)
        with pytest.raises(OSError, match="injected cleanup crash"):
            store.enforce_run_retention(run["id"])
        assert store.get_run(run["id"])["run_dir"] == run["run_dir"]
        assert Path(run["run_dir"]).exists()

        monkeypatch.setattr(run_repository_module.shutil, "rmtree", original_rmtree)
        second = store.create_run("demo_obj", keep=False)
        _succeed(store, second["id"])
        store.runs.mark_retention_terminal_queued(second["id"], sync_required=False)
        store.runs.acknowledge_run_delivery(second["id"])
        errors: list[BaseException] = []

        def enforce() -> None:
            try:
                store.enforce_run_retention(second["id"])
            except BaseException as exc:  # pragma: no cover - assertion reports the concrete race.
                errors.append(exc)

        threads = [threading.Thread(target=enforce), threading.Thread(target=enforce)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(2)

        assert errors == []
        assert not Path(second["run_dir"]).exists()
        assert store.get_run(second["id"])["run_dir"] == ""
    finally:
        store.close()


@pytest.mark.parametrize(
    ("historical_shape", "pre_retention_columns"),
    (
        ("current-minus-retention", ()),
        (
            "c1d7c72-pre-keep",
            ("interpreter_substitution_json", "keep", "manifest_json"),
        ),
    ),
)
def test_run_retention_migration_grandfathers_rows_and_is_idempotent(
    tmp_path: Path,
    historical_shape: str,
    pre_retention_columns: tuple[str, ...],
) -> None:
    home = tmp_path / historical_shape
    database = home / "daemon.sqlite3"
    retention_columns = (
        "retention_enforced",
        "retention_report_mode",
        "retention_sync_required",
        "retention_terminal_queued",
        "retention_delivery_required",
        "retention_delivery_acked",
        "retention_delivery_expires_at",
        "retention_effective_status",
        "retention_outcome_reason",
    )
    if historical_shape == "c1d7c72-pre-keep":
        home.mkdir(parents=True)
        fixture = Path(__file__).parent / "fixtures" / "run_retention_c1d7c72.sql"
        with sqlite3.connect(database) as conn:
            conn.executescript(fixture.read_text(encoding="utf-8"))
    else:
        store = _seed_store(home)
        legacy = store.create_run("demo_obj", keep="on_failure", kwargs={"secret": "preserve-me"})
        _fail(store, legacy["id"])
        store.close()

    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        if historical_shape == "current-minus-retention":
            for column in retention_columns:
                conn.execute("ALTER TABLE runs DROP COLUMN {}".format(column))
            conn.execute("DELETE FROM schema_migrations WHERE id = ?", (RUN_RETENTION_MIGRATION_ID,))
            conn.execute(
                "DELETE FROM schema_migrations WHERE id = ?",
                (RUN_RETENTION_DELIVERY_MIGRATION_ID,),
            )
            conn.execute("PRAGMA user_version = 1")
        historical_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
        historical_row = dict(conn.execute("SELECT * FROM runs").fetchone())
        legacy_id = str(historical_row["id"])
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE id = ?",
                (RUN_RETENTION_MIGRATION_ID,),
            ).fetchone()[0]
            == 0
        )

    upgraded = RegistryStore(home)
    try:
        current = upgraded.get_run(legacy_id)
        assert current["keep"] == "on_failure"
        assert current["input"]["kwargs"] == {"secret": "preserve-me"}
        assert current["status"] == "failed"
        assert current["id"] == historical_row["id"]
        assert current["object_id"] == historical_row["object_id"]
        assert current["object_version_id"] == historical_row["object_version_id"]
        assert current["retention_enforced"] is False
        with sqlite3.connect(database) as conn:
            conn.row_factory = sqlite3.Row
            upgraded_row = dict(conn.execute("SELECT * FROM runs WHERE id = ?", (legacy_id,)).fetchone())
            assert set(upgraded_row) - historical_columns == set(retention_columns) | set(pre_retention_columns)
            assert {column: upgraded_row[column] for column in historical_columns} == historical_row
            assert upgraded_row["keep"] == "on_failure"
            if "manifest_json" in pre_retention_columns:
                assert upgraded_row["manifest_json"] is None
            if "interpreter_substitution_json" in pre_retention_columns:
                assert upgraded_row["interpreter_substitution_json"] is None
            migration_row = conn.execute(
                "SELECT applied_at FROM schema_migrations WHERE id = ?",
                (RUN_RETENTION_MIGRATION_ID,),
            ).fetchone()
            assert migration_row is not None
            applied_at = migration_row["applied_at"]
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE id = ?",
                    (RUN_RETENTION_MIGRATION_ID,),
                ).fetchone()[0]
                == 1
            )
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE id = ?",
                    (RUN_RETENTION_DELIVERY_MIGRATION_ID,),
                ).fetchone()[0]
                == 1
            )
            assert conn.execute("PRAGMA user_version").fetchone()[0] == SYNC_EVENT_TELEMETRY_SCHEMA_VERSION
            assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
            assert [row[0] for row in conn.execute("PRAGMA integrity_check").fetchall()] == ["ok"]
    finally:
        upgraded.close()

    second = RegistryStore(home)
    try:
        second_state = second.get_run(legacy_id)
        assert second_state["retention_enforced"] is False
        assert second_state["id"] == historical_row["id"]
        assert second_state["status"] == historical_row["status"]
        with sqlite3.connect(database) as conn:
            conn.row_factory = sqlite3.Row
            second_row = dict(conn.execute("SELECT * FROM runs WHERE id = ?", (legacy_id,)).fetchone())
            assert second_row == upgraded_row
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE id = ?",
                    (RUN_RETENTION_MIGRATION_ID,),
                ).fetchone()[0]
                == 1
            )
            assert (
                conn.execute(
                    "SELECT applied_at FROM schema_migrations WHERE id = ?",
                    (RUN_RETENTION_MIGRATION_ID,),
                ).fetchone()[0]
                == applied_at
            )
            assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
            assert [row[0] for row in conn.execute("PRAGMA integrity_check").fetchall()] == ["ok"]
    finally:
        second.close()


def test_recorded_retention_v1_adds_delivery_v2_without_data_loss(tmp_path: Path) -> None:
    """Upgrade the exact intermediate 0.4.5 schema without rewriting run rows."""

    store = _seed_store(tmp_path)
    run = store.create_run("demo_obj", keep="on_failure", kwargs={"secret": "preserve-me"})
    _fail(store, run["id"])
    store.close()
    database = tmp_path / "daemon.sqlite3"
    delivery_columns = (
        "retention_delivery_required",
        "retention_delivery_acked",
        "retention_delivery_expires_at",
        "retention_effective_status",
        "retention_outcome_reason",
    )

    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        original = dict(conn.execute("SELECT * FROM runs WHERE id = ?", (run["id"],)).fetchone())
        preserved = {key: value for key, value in original.items() if key not in delivery_columns}
        v1_applied_at = conn.execute(
            "SELECT applied_at FROM schema_migrations WHERE id = ?",
            (RUN_RETENTION_MIGRATION_ID,),
        ).fetchone()[0]
        for column in delivery_columns:
            conn.execute(f"ALTER TABLE runs DROP COLUMN {column}")
        conn.execute(
            "DELETE FROM schema_migrations WHERE id = ?",
            (RUN_RETENTION_DELIVERY_MIGRATION_ID,),
        )
        conn.execute("PRAGMA user_version = 2")

    upgraded = RegistryStore(tmp_path)
    upgraded.close()

    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        repaired = dict(conn.execute("SELECT * FROM runs WHERE id = ?", (run["id"],)).fetchone())
        assert {key: repaired[key] for key in preserved} == preserved
        assert repaired["retention_delivery_required"] == 0
        assert repaired["retention_delivery_acked"] == 0
        assert repaired["retention_delivery_expires_at"] is None
        assert repaired["retention_effective_status"] is None
        assert repaired["retention_outcome_reason"] is None
        assert (
            conn.execute(
                "SELECT applied_at FROM schema_migrations WHERE id = ?",
                (RUN_RETENTION_MIGRATION_ID,),
            ).fetchone()[0]
            == v1_applied_at
        )
        v2_applied_at = conn.execute(
            "SELECT applied_at FROM schema_migrations WHERE id = ?",
            (RUN_RETENTION_DELIVERY_MIGRATION_ID,),
        ).fetchone()[0]
        assert v2_applied_at
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SYNC_EVENT_TELEMETRY_SCHEMA_VERSION
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert [row[0] for row in conn.execute("PRAGMA integrity_check").fetchall()] == ["ok"]

    reopened = RegistryStore(tmp_path)
    reopened.close()
    with sqlite3.connect(database) as conn:
        assert (
            conn.execute(
                "SELECT applied_at FROM schema_migrations WHERE id = ?",
                (RUN_RETENTION_DELIVERY_MIGRATION_ID,),
            ).fetchone()[0]
            == v2_applied_at
        )


def test_effective_outcome_and_manifest_retention_update_are_atomic(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("demo_obj", keep="on_failure", report_local_run=False)
        _succeed(store, run["id"])
        with pytest.raises(ValueError, match="unknown effective retention status"):
            store.runs.mark_retention_terminal_queued(
                run["id"],
                sync_required=True,
                effective_status="",
            )
        with store._storage._lock, store._storage._conn:  # noqa: SLF001 - injected transaction failure.
            store._storage._conn.execute(  # noqa: SLF001
                """
                CREATE TRIGGER fail_retention_manifest_update
                BEFORE UPDATE OF manifest_json ON runs
                BEGIN
                    SELECT RAISE(ABORT, 'injected manifest failure');
                END
                """
            )

        with pytest.raises(sqlite3.IntegrityError, match="injected manifest failure"):
            store.runs.mark_retention_terminal_queued(
                run["id"],
                sync_required=True,
                effective_status="failed",
                outcome_reason="server-handoff-failed",
            )

        current = store.get_run(run["id"])
        assert current["retention_terminal_queued"] is False
        assert current["retention_effective_status"] is None
        assert current["retention_outcome_reason"] is None
        assert current["manifest"]["retention"]["expires_at"] is None
    finally:
        store.close()


def test_ordinary_terminal_queue_marker_keeps_manifest_shape_stable(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("demo_obj", keep=True, report_local_run=False)
        final = _succeed(store, run["id"])
        terminal_retention = dict(final["manifest"]["retention"])

        marked = store.runs.mark_retention_terminal_queued(
            run["id"],
            sync_required=False,
        )

        assert marked["manifest"]["retention"] == terminal_retention
        assert marked["retention_effective_status"] is None
        assert marked["retention_outcome_reason"] is None
    finally:
        store.close()


def test_terminal_queue_marker_retry_preserves_effective_outcome_override(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("demo_obj", keep="on_failure", report_local_run=False)
        _succeed(store, run["id"])
        overridden = store.runs.mark_retention_terminal_queued(
            run["id"],
            sync_required=True,
            effective_status="failed",
            outcome_reason="server-handoff-failed",
        )

        retried = store.runs.mark_retention_terminal_queued(
            run["id"],
            sync_required=True,
        )

        assert retried["retention_effective_status"] == "failed"
        assert retried["retention_outcome_reason"] == "server-handoff-failed"
        assert retried["manifest"]["retention"] == overridden["manifest"]["retention"]
    finally:
        store.close()


def test_http_omitted_keep_uses_compatible_true_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = RegistryStore(tmp_path)
    app = create_app(store, auto_build_envs=False)
    seen: dict[str, Any] = {}

    def fake_start_run(_object: str, **kwargs: Any) -> dict[str, Any]:
        seen["keep"] = kwargs["keep"]
        return {"id": "run-default", "status": "queued", "keep": kwargs["keep"]}

    monkeypatch.setattr(app.runtime, "start_run", fake_start_run)

    async def post() -> tuple[int, Any]:
        response = await app.test_client().post(
            "/runs",
            json={"object": "demo_obj", "source": "local"},
            headers={"Authorization": "Bearer {}".format(app.api_token)},
        )
        return response.status_code, await response.get_json()

    try:
        status, payload = asyncio.run(post())
        assert status == 202
        assert payload["keep"] is True
        assert seen["keep"] is True
    finally:
        app.runtime.shutdown()
        store.close()


def test_result_and_artifact_reads_renew_without_acknowledging_delivery(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    run = store.create_run("demo_obj", keep=False)
    store.update_run(run["id"], status="starting")
    store.update_run(run["id"], status="preparing_environment")
    store.update_run(run["id"], status="running")
    store.update_run(
        run["id"],
        status="succeeded",
        finished_at="2026-07-15T10:00:00+00:00",
        result={"result": 1},
    )
    store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)
    app = create_app(store, auto_build_envs=False)

    async def read_data() -> tuple[int, int]:
        headers = {"Authorization": "Bearer {}".format(app.api_token)}
        result_response = await app.test_client().get(
            f"/runs/{run['id']}/result",
            headers=headers,
        )
        with store._storage._lock, store._storage._conn:  # noqa: SLF001 - lease-renewal fixture.
            store._storage._conn.execute(  # noqa: SLF001
                "UPDATE runs SET retention_delivery_expires_at = ? WHERE id = ?",
                ((datetime.now(UTC) + timedelta(seconds=5)).isoformat(), run["id"]),
            )
        artifact_response = await app.test_client().get(
            f"/runs/{run['id']}/artifacts",
            headers=headers,
        )
        return result_response.status_code, artifact_response.status_code

    try:
        assert asyncio.run(read_data()) == (200, 200)
        current = store.get_run(run["id"])
        renewed = datetime.fromisoformat(str(current["retention_delivery_expires_at"]))
        assert renewed > datetime.now(UTC) + timedelta(minutes=14)
        assert current["retention_delivery_acked"] is False
        assert Path(run["run_dir"]).exists()
    finally:
        app.runtime.shutdown()
        store.close()


def test_retention_rejects_outside_and_symlink_run_directories(tmp_path: Path) -> None:
    store = _seed_store(tmp_path / "store")
    try:
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "secret.txt").write_text("keep", encoding="utf-8")
        run = store.create_run("demo_obj", keep=False)
        _succeed(store, run["id"])
        store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)
        store.runs.acknowledge_run_delivery(run["id"])
        with store._storage._lock, store._storage._conn:  # noqa: SLF001 - adversarial corrupted-row fixture.
            store._storage._conn.execute(  # noqa: SLF001
                "UPDATE runs SET run_dir = ? WHERE id = ?",
                (str(outside), run["id"]),
            )

        with pytest.raises(RuntimeError, match="outside the daemon runs directory"):
            store.enforce_run_retention(run["id"])
        assert (outside / "secret.txt").exists()

        valid_path = Path(run["run_dir"])
        valid_path.rename(valid_path.with_name(valid_path.name + "-real"))
        valid_path.symlink_to(valid_path.with_name(valid_path.name + "-real"), target_is_directory=True)
        with store._storage._lock, store._storage._conn:  # noqa: SLF001
            store._storage._conn.execute(  # noqa: SLF001
                "UPDATE runs SET run_dir = ? WHERE id = ?",
                (str(valid_path), run["id"]),
            )
        with pytest.raises(RuntimeError, match="symbolic link"):
            store.enforce_run_retention(run["id"])
    finally:
        store.close()


@pytest.mark.parametrize("keep", [False, "on_failure"])
def test_splclient_call_delivers_result_and_artifact_before_transient_ack(
    tmp_path: Path,
    keep: bool | str,
) -> None:
    from tests.daemon.test_daemon_endpoint import _serve_app_in_thread

    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = int(reservation.getsockname()[1])

    store = RegistryStore(tmp_path)
    app = None
    stop_server = None
    server_thread = None
    server_errors: list[BaseException] = []
    try:
        store.register_env("default", sys.executable)
        store.register_object(
            "artifact_obj",
            "artifact_obj",
            "default",
            yaml_text=ARTIFACT_FUNCTION_YAML,
        )
        app = create_app(
            store,
            auto_build_envs=False,
            daemon_base_url=f"http://127.0.0.1:{port}",
        )
        stop_server, server_thread, server_errors = _serve_app_in_thread(app, port)
        client = SPLClient(base_url=f"http://127.0.0.1:{port}", api_token=app.api_token)
        downloads = tmp_path / f"downloads-{keep}"

        result = client.call(
            "artifact_obj",
            keep=keep,
            artifacts_dir=downloads,
            progress=False,
        )

        assert result.value == {"ok": True}
        assert result.downloaded_artifacts["artifact.txt"].read_text(encoding="utf-8") == "durable artifact"
        run_id = str(result.run["id"])
        current = store.get_run(run_id)
        assert current["retention_delivery_acked"] is True
        assert current["run_dir"] == ""
        assert current["result_present"] is False
        assert current["manifest"]["retention"]["directory_removed"] is True
        assert not server_errors
    finally:
        if stop_server is not None:
            stop_server.set()
        if server_thread is not None:
            server_thread.join(timeout=5.0)
        if app is not None:
            app.runtime.shutdown()
        store.close()


class _CollectDaemonStub:
    def __init__(self, *, ack_error: BaseException | None = None, download_error: BaseException | None = None):
        self.ack_error = ack_error
        self.download_error = download_error
        self.ack_count = 0

    def wait_run(self, _run_id: str, **_kwargs: Any) -> dict[str, Any]:
        return {"id": "a" * 32, "status": "succeeded", "keep": False}

    def result(self, _run_id: str) -> dict[str, Any]:
        return {"result": 42}

    def list_artifacts(self, _run_id: str) -> list[str]:
        return ["artifact.txt"]

    def download_artifact(self, _run_id: str, _name: str, _target: Path) -> Path:
        if self.download_error is not None:
            raise self.download_error
        return _target / "artifact.txt"

    def acknowledge_run_delivery(self, _run_id: str) -> dict[str, Any]:
        self.ack_count += 1
        if self.ack_error is not None:
            raise self.ack_error
        return {"acknowledged": True}


class _CollectClientStub:
    def __init__(self, daemon: _CollectDaemonStub):
        self._daemon = daemon


def test_failed_artifact_download_does_not_acknowledge_delivery(tmp_path: Path) -> None:
    daemon = _CollectDaemonStub(download_error=RuntimeError("download interrupted"))
    run = RemoteRun(
        _CollectClientStub(daemon),  # type: ignore[arg-type]
        {"id": "a" * 32, "status": "queued", "keep": False},
    )

    with pytest.raises(RuntimeError, match="download interrupted"):
        run.collect(artifacts_dir=tmp_path, progress=False)

    assert daemon.ack_count == 0


@pytest.mark.parametrize("status_code", [404, 405])
def test_new_client_treats_missing_delivery_ack_endpoint_as_legacy_success(
    status_code: int,
) -> None:
    daemon = _CollectDaemonStub(
        ack_error=ClientError("legacy daemon", status_code=status_code),
    )
    run = RemoteRun(
        _CollectClientStub(daemon),  # type: ignore[arg-type]
        {"id": "a" * 32, "status": "queued", "keep": False},
    )

    with warnings.catch_warnings(record=True) as caught:
        result = run.collect(progress=False)

    assert result.payload == {"result": 42}
    assert daemon.ack_count == 1
    assert caught == []


def test_unacked_delivery_expiry_uses_one_bounded_scheduler_thread(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False)
    scheduler = runtime._retention_scheduler  # noqa: SLF001 - bounded lifecycle assertion.
    runs: list[dict[str, Any]] = []
    try:
        deadline = (datetime.now(UTC) + timedelta(seconds=1.5)).isoformat()
        for _ in range(24):
            run = store.create_run("demo_obj", keep=False)
            _succeed(store, run["id"])
            store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)
            with store._storage._lock, store._storage._conn:  # noqa: SLF001 - accelerated lease fixture.
                store._storage._conn.execute(  # noqa: SLF001
                    "UPDATE runs SET retention_delivery_expires_at = ? WHERE id = ?",
                    (deadline, run["id"]),
                )
            runtime._schedule_run_retention(store.get_run(run["id"]))  # noqa: SLF001
            runs.append(run)

        scheduler_threads = [thread for thread in threading.enumerate() if thread.name == "spl-run-retention-scheduler"]
        assert len(scheduler_threads) == 1
        assert len(runtime._retention_deadlines) == len(runs)  # noqa: SLF001

        deadline_monotonic = time.monotonic() + 5.0
        while time.monotonic() < deadline_monotonic and any(Path(run["run_dir"]).exists() for run in runs):
            time.sleep(0.02)
        assert all(not Path(run["run_dir"]).exists() for run in runs)
    finally:
        runtime.shutdown()
        store.close()
        assert not scheduler.is_alive()


def test_restart_preserves_unacked_lease_then_cleans_after_expiry(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    run = store.create_run("demo_obj", keep=False)
    _succeed(store, run["id"])
    store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)
    store.close()

    before_expiry = RegistryStore(tmp_path)
    runtime = DaemonRuntime(before_expiry, auto_build_envs=False)
    try:
        assert Path(run["run_dir"]).exists()
        assert before_expiry.get_run(run["id"])["retention_delivery_acked"] is False
    finally:
        runtime.shutdown()
        before_expiry.close()

    with sqlite3.connect(tmp_path / "daemon.sqlite3") as conn:
        conn.execute(
            "UPDATE runs SET retention_delivery_expires_at = ? WHERE id = ?",
            ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(), run["id"]),
        )

    after_expiry = RegistryStore(tmp_path)
    restarted = DaemonRuntime(after_expiry, auto_build_envs=False)
    try:
        assert not Path(run["run_dir"]).exists()
        assert after_expiry.get_run(run["id"])["run_dir"] == ""
    finally:
        restarted.shutdown()
        after_expiry.close()


def test_duplicate_ack_and_restart_preserve_compact_manifest_summary(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    run = store.create_run("demo_obj", keep=False)
    manifest = dict(run["manifest"])
    manifest["nodes"] = {"node-1": {"status": "succeeded"}}
    manifest["edges"] = [{"source": "node-1", "target": "output"}]
    store.update_run(run["id"], manifest=manifest)
    _succeed(store, run["id"])
    store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)
    first = store.acknowledge_run_delivery(run["id"])
    compact_before = json.dumps(store.get_run(run["id"])["manifest"], sort_keys=True)

    second = store.acknowledge_run_delivery(run["id"])
    compact_after_second_ack = json.dumps(store.get_run(run["id"])["manifest"], sort_keys=True)

    assert first["retention"]["removed"] is True
    assert second["retention"]["reason"] == "already-removed"
    assert compact_after_second_ack == compact_before
    assert store.get_run(run["id"])["manifest"]["summary"] == {
        "edge_count": 1,
        "node_count": 1,
        "node_status_counts": {"succeeded": 1},
    }
    store.close()

    restarted_store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(restarted_store, auto_build_envs=False)
    try:
        assert json.dumps(restarted_store.get_run(run["id"])["manifest"], sort_keys=True) == compact_before
    finally:
        runtime.shutdown()
        restarted_store.close()


def test_internal_remote_job_cleanup_does_not_wait_for_local_delivery_ack(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("demo_obj", keep=False, report_local_run=False)
        _succeed(store, run["id"])
        store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)

        result = store.enforce_run_retention(run["id"])
        current = store.get_run(run["id"])

        assert result["removed"] is True
        assert current["retention_delivery_required"] is False
        assert current["retention_delivery_acked"] is True
        assert current["run_dir"] == ""
    finally:
        store.close()


def test_failed_direct_artifact_handoff_uses_failed_effective_retention_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _seed_store(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False)
    local_run = store.create_run(
        "demo_obj",
        keep="on_failure",
        report_local_run=False,
    )
    store.update_run(local_run["id"], status="starting")
    store.update_run(local_run["id"], status="preparing_environment")
    store.update_run(local_run["id"], status="running")
    final = store.update_run(
        local_run["id"],
        status="succeeded",
        finished_at="2026-07-15T10:00:00+00:00",
        result={"result": 7},
    )
    artifact_path = Path(final["artifacts_dir"]) / "large.bin"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(b"unshipped-artifact" * 1024)
    terminal_event_ids: list[str] = []
    upload_attempts: list[str] = []

    class FailingArtifactServer:
        def upload_artifact(self, _run_id: str, name: str, _path: Path) -> dict[str, Any]:
            upload_attempts.append(name)
            raise ServerClientError(503, "artifact service unavailable")

    def send_update(
        _connection_id: str,
        *,
        run_id: str,
        status: str,
        payload: dict[str, Any] | None = None,
        **_kwargs: Any,
    ) -> bool:
        event = store.enqueue_sync_event(
            "run_update",
            {"run_id": run_id, "status": status, "payload": payload or {}},
        )
        if status in {"succeeded", "failed"}:
            terminal_event_ids.append(event["id"])
        return True

    monkeypatch.setattr(daemon_server_module, "DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES", 1)
    monkeypatch.setattr(runtime, "_send_server_run_update", send_update)
    monkeypatch.setattr(runtime, "_ensure_server_object_envs", lambda _versions: None)
    monkeypatch.setattr(
        runtime,
        "register_object",
        lambda *_args, **_kwargs: {"version_id": "local-version-1"},
    )
    monkeypatch.setattr(runtime, "start_run", lambda *_args, **_kwargs: {"id": local_run["id"]})
    monkeypatch.setattr(runtime, "_wait_local_run", lambda *_args, **_kwargs: final)
    monkeypatch.setattr(
        store,
        "get_server_connection_credentials",
        lambda _connection_id: {"heartbeat_interval_seconds": 60},
    )
    monkeypatch.setattr(
        runtime,
        "_server_client_for_credentials",
        lambda _credentials: FailingArtifactServer(),
    )
    job = {
        "run": {"id": "remote-run-1", "args": [], "kwargs": {}},
        "object_version": {
            "id": "object-1",
            "version_id": "version-1",
            "name": "demo_obj",
            "entrypoint": "demo_obj",
            "env": "default",
            "yaml": FUNCTION_YAML,
        },
    }

    runtime._execute_server_job(job, "connection-1")  # noqa: SLF001

    try:
        current = store.get_run(local_run["id"])
        retained_expiry = current["manifest"]["retention"]["expires_at"]
        assert upload_attempts == ["large.bin"]
        assert terminal_event_ids
        assert current["status"] == "succeeded"
        assert current["retention_effective_status"] == "failed"
        assert current["retention_outcome_reason"] == "server-handoff-failed"
        assert current["manifest"]["status"] == "succeeded"
        assert current["manifest"]["retention"]["effective_status"] == "failed"
        assert retained_expiry is not None
        assert artifact_path.read_bytes().startswith(b"unshipped-artifact")

        for event in store.list_pending_sync_events(limit=None):
            store.mark_sync_event_sent(event["id"])
        outcome = store.enforce_run_retention(local_run["id"])
        assert outcome["reason"] == "policy-retain"
        assert artifact_path.is_file()
    finally:
        runtime.shutdown()
        store.close()

    restarted_store = RegistryStore(tmp_path)
    restarted = DaemonRuntime(restarted_store, auto_build_envs=False)
    try:
        recovered = restarted_store.get_run(local_run["id"])
        assert recovered["status"] == "succeeded"
        assert recovered["retention_effective_status"] == "failed"
        assert recovered["manifest"]["retention"]["expires_at"] == retained_expiry
        assert artifact_path.is_file()
    finally:
        restarted.shutdown()
        restarted_store.close()


def test_scheduler_retries_transient_rmtree_failure_without_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _seed_store(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False)
    scheduler = runtime._retention_scheduler  # noqa: SLF001
    run = store.create_run("demo_obj", keep=False)
    _succeed(store, run["id"])
    store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)
    original_rmtree = run_repository_module.shutil.rmtree
    calls = 0

    def fail_once(path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("transient cleanup failure")
        original_rmtree(path)

    monkeypatch.setattr(run_repository_module.shutil, "rmtree", fail_once)
    monkeypatch.setattr(daemon_server_module, "RUN_RETENTION_CLEANUP_RETRY_SECONDS", 0.05)
    with store._storage._lock, store._storage._conn:  # noqa: SLF001 - accelerated lease fixture.
        store._storage._conn.execute(  # noqa: SLF001
            "UPDATE runs SET retention_delivery_expires_at = ? WHERE id = ?",
            ((datetime.now(UTC) + timedelta(milliseconds=50)).isoformat(), run["id"]),
        )
    runtime._schedule_run_retention(store.get_run(run["id"]))  # noqa: SLF001

    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and Path(run["run_dir"]).exists():
            time.sleep(0.02)
        assert calls >= 2
        assert not Path(run["run_dir"]).exists()
        assert store.get_run(run["id"])["run_dir"] == ""
    finally:
        runtime.shutdown()
        store.close()
        assert not scheduler.is_alive()


def test_startup_sweep_retries_transient_rmtree_failure_without_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _seed_store(tmp_path)
    run = store.create_run("demo_obj", keep=False)
    _succeed(store, run["id"])
    store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)
    store.runs.acknowledge_run_delivery(run["id"])
    store.close()

    original_rmtree = run_repository_module.shutil.rmtree
    calls = 0

    def fail_once(path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("transient startup cleanup failure")
        original_rmtree(path)

    monkeypatch.setattr(run_repository_module.shutil, "rmtree", fail_once)
    monkeypatch.setattr(daemon_server_module, "RUN_RETENTION_CLEANUP_RETRY_SECONDS", 0.05)
    restarted_store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(restarted_store, auto_build_envs=False)
    scheduler = runtime._retention_scheduler  # noqa: SLF001
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and Path(run["run_dir"]).exists():
            time.sleep(0.02)
        assert calls >= 2
        assert not Path(run["run_dir"]).exists()
        assert restarted_store.get_run(run["id"])["run_dir"] == ""
    finally:
        runtime.shutdown()
        restarted_store.close()
        assert not scheduler.is_alive()


def test_retention_rejects_symbolic_link_in_trusted_runs_root(tmp_path: Path) -> None:
    home = tmp_path / "daemon-home"
    store = _seed_store(home)
    run = store.create_run("demo_obj", keep=False)
    _succeed(store, run["id"])
    store.runs.mark_retention_terminal_queued(run["id"], sync_required=False)
    store.runs.acknowledge_run_delivery(run["id"])
    real_home = tmp_path / "daemon-home-real"
    home.rename(real_home)
    home.symlink_to(real_home, target_is_directory=True)
    try:
        with pytest.raises(RuntimeError, match="symbolic-link daemon root"):
            store.enforce_run_retention(run["id"])
        assert (real_home / "runs" / run["id"]).is_dir()
    finally:
        store.close()


def test_daemon_resume_rejects_transient_child_policy_before_lookup(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    runtime = DaemonRuntime(store, auto_build_envs=False)
    try:
        with pytest.raises(ValueError, match="keep=False is incompatible with resume"):
            runtime.resume_run("missing", from_="producer", keep=False)
    finally:
        runtime.shutdown()
        store.close()
