from __future__ import annotations

import json
import logging
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from spl.core.redaction import redact_text
from spl.daemon import artifact_access
from spl.daemon.cli import build_parser
from spl.daemon.storage_base import (
    SYNC_EVENT_TELEMETRY_MIGRATION_ID,
    SYNC_EVENT_TELEMETRY_SCHEMA_VERSION,
)
from spl.daemon.server import (
    LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES,
    LOCAL_RUN_TEXT_ARTIFACT_MAX_COUNT,
    DaemonRuntime,
)
from spl.daemon.store import RegistryStore
from spl.daemon.telemetry import (
    SYNC_EVENT_MAX_BYTES,
    SYNC_EVENT_PAYLOAD_BUDGET,
    TelemetryPolicy,
)

FUNCTION_YAML = """\
- !DFunction
  name: telemetry_obj
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 1
"""


def _state(secret: str) -> dict[str, Any]:
    return {
        "id": "telemetry-run",
        "object": "telemetry_obj",
        "status": "failed",
        "input": {
            "args": [secret],
            "kwargs": {"api_key": secret, "explicit_private": secret},
        },
        "result": {"echo": secret},
        "result_present": True,
        "error": "ValueError: {}".format(secret),
        "stdout": "stdout {}".format(secret),
        "stderr": "stderr {}".format(secret),
        "created_at": "2026-07-15T10:00:00+00:00",
        "started_at": "2026-07-15T10:00:01+00:00",
        "finished_at": "2026-07-15T10:00:02+00:00",
        "artifacts_dir": "",
        "manifest": {
            "pipeline": {"content_hash": "pipeline-hash"},
            "nodes": {
                "node-1": {
                    "id": "node-1",
                    "alias": "step",
                    "name": "work",
                    "kind": "function",
                    "status": "failed",
                    "fingerprint": {"sha256": "node-hash"},
                }
            },
            "edges": [{"source": "node-1", "target": "output"}],
        },
    }


def _label() -> dict[str, Any]:
    return {
        "display_name": "telemetry_obj",
        "local_name": "telemetry_obj",
        "owner_id": "owner-1",
        "remote_object_id": None,
        "remote_version_id": None,
    }


def _artifacts(secret: str) -> list[dict[str, Any]]:
    return [
        {
            "name": "stdout.txt",
            "kind": "stdout",
            "content_type": "text/plain; charset=utf-8",
            "content_text": secret,
        },
        {
            "name": "evidence.txt",
            "kind": "artifact",
            "content_type": "text/plain; charset=utf-8",
            "content_text": secret,
        },
    ]


class _TailSliceBomb(str):
    """A string that proves bounded readers do not inspect its oversized tail."""

    forbidden_slice_start: int

    def __new__(cls, value: str, *, forbidden_slice_start: int) -> _TailSliceBomb:
        instance = super().__new__(cls, value)
        instance.forbidden_slice_start = forbidden_slice_start
        return instance

    def __getitem__(self, key: int | slice) -> str:
        if isinstance(key, slice) and (key.start or 0) >= self.forbidden_slice_start:
            raise AssertionError("oversized string tail was traversed")
        return super().__getitem__(key)


def test_metadata_default_has_an_exact_raw_value_denylist() -> None:
    secret = "SOL012_SECRET_MARKER"
    payload = TelemetryPolicy().build_local_run_payload(
        _state(secret),
        _label(),
        full_artifacts=_artifacts(secret),
    )
    wire = json.dumps(payload, sort_keys=True)

    assert secret not in wire
    assert payload["telemetry_level"] == "metadata"
    assert payload["source_result_present"] is True
    assert payload["input_mirrored"] is False
    assert payload["result_mirrored"] is False
    assert payload["streams_mirrored"] is False
    assert payload["artifact_bodies_mirrored"] is False
    assert not {"input", "result", "result_present", "stdout", "stderr", "artifacts"} & payload.keys()
    assert payload["telemetry"]["summary"] == {
        "argument_count": 1,
        "keyword_argument_count": 2,
        "node_count": 1,
        "node_detail_count": 1,
        "edge_count": 1,
        "artifact_count": 2,
        "artifact_count_truncated": False,
        "stdout_bytes": len("stdout {}".format(secret).encode()),
        "stderr_bytes": len("stderr {}".format(secret).encode()),
        "duration_ms": 1000,
        "source_result_present": True,
    }


@pytest.mark.parametrize(
    "text",
    [
        "Authorization: Bearer header-secret",
        "aws=AKIAABCDEFGHIJKLMNOP",
        "password=hunter2",
        "api_key=service-secret",
        "token=service-secret",
        '{"api_key":"json-secret"}',
        "postgresql://service:database-secret@db.internal/app",
        "-----BEGIN PRIVATE KEY-----\nprivate-material\n-----END PRIVATE KEY-----",
    ],
)
def test_shared_text_redactor_covers_common_credential_shapes(text: str) -> None:
    redacted = redact_text(text)

    assert redacted != text
    assert any(marker in redacted for marker in ("[REDACTED]", "Bearer [REDACTED]"))


@pytest.mark.parametrize("level", ["diagnostic", "full"])
def test_opt_in_levels_redact_common_and_explicit_secret_values(level: str) -> None:
    secret = "marked-value-without-a-secret-shape"
    payload = TelemetryPolicy(
        level,  # type: ignore[arg-type]
        ("/input/kwargs/explicit_private",),
    ).build_local_run_payload(
        _state(secret),
        _label(),
        full_artifacts=_artifacts(secret),
    )
    wire = json.dumps(payload, sort_keys=True)

    assert secret not in wire
    assert "[REDACTED]" in wire
    assert payload["telemetry_level"] == level
    assert payload["streams_mirrored"] is True
    if level == "diagnostic":
        assert "input" not in payload
        assert "result" not in payload
        assert payload["artifact_bodies_mirrored"] is False
        assert {artifact["kind"] for artifact in payload["artifacts"]} <= {
            "stdout",
            "stderr",
        }
    else:
        assert payload["input_mirrored"] is True
        assert payload["result_mirrored"] is True
        assert payload["artifact_bodies_mirrored"] is True


@pytest.mark.parametrize("level", ["diagnostic", "full"])
def test_opt_in_levels_redact_values_repeated_from_common_secret_keys(
    level: str,
) -> None:
    secret = "COMMON_KEY_SECRET_MARKER"
    state = _state("ordinary-value")
    state["input"]["kwargs"] = {"api_key": secret}
    state["error"] = f"ValueError: api_key={secret}"
    state["stdout"] = f"token={secret}"

    payload = TelemetryPolicy(level).build_local_run_payload(  # type: ignore[arg-type]
        state,
        _label(),
        full_artifacts=_artifacts(secret),
    )
    wire = json.dumps(payload, sort_keys=True)

    assert secret not in wire
    assert "[REDACTED]" in wire


def test_full_telemetry_structurally_redacts_json_artifact_only_secrets() -> None:
    secret = "ARTIFACT_ONLY_SECRET_MARKER"
    content = {"api_key": secret, "nested": {"token": secret}}
    artifact = {
        "name": "artifact.secrets.json",
        "kind": "artifact",
        "content_type": "application/json",
        "content_text": json.dumps(content),
        "content_json": content,
    }

    payload = TelemetryPolicy("full").build_local_run_payload(
        _state("ordinary-value"),
        _label(),
        full_artifacts=[artifact],
    )
    wire = json.dumps(payload, sort_keys=True)

    assert secret not in wire
    assert wire.count("[REDACTED]") >= 2


def test_full_telemetry_omits_components_before_crossing_event_budget() -> None:
    state = _state("ordinary-value")
    state["result"] = {"large": "r" * SYNC_EVENT_MAX_BYTES}
    artifacts = _artifacts("a" * SYNC_EVENT_MAX_BYTES)

    payload = TelemetryPolicy("full").build_local_run_payload(
        state,
        _label(),
        full_artifacts=artifacts,
    )

    assert len(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()) <= SYNC_EVENT_PAYLOAD_BUDGET
    assert payload["telemetry"].get("omissions")
    for artifact in payload.get("artifacts", []):
        assert "content_text" in artifact or "content_json" in artifact
    assert payload["streams_mirrored"] is any(
        artifact.get("kind") in {"stdout", "stderr"} for artifact in payload.get("artifacts", [])
    )
    assert payload["artifact_bodies_mirrored"] is any(
        artifact.get("kind") == "artifact" for artifact in payload.get("artifacts", [])
    )


def test_real_daemon_default_payload_does_not_mirror_secret_fixture(tmp_path: Path) -> None:
    secret = "SOL012_ACTUAL_DAEMON_SECRET"
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "evidence.txt").write_text(secret, encoding="utf-8")
    state = _state(secret)
    state["artifacts_dir"] = str(artifacts_dir)
    store = RegistryStore(tmp_path / "store")
    runtime = DaemonRuntime(store, auto_build_envs=False)
    try:
        payload = runtime._local_run_sync_payload(state)  # noqa: SLF001 - privacy seam fixture.
        wire = json.dumps(payload, sort_keys=True)

        assert secret not in wire
        assert payload["telemetry_level"] == "metadata"
        assert payload["telemetry"]["summary"]["artifact_count"] == 4
        assert "artifacts" not in payload
        event = store.enqueue_sync_event("local_run_update", {"run": payload})
        with store._storage._lock:  # noqa: SLF001 - persisted/wire privacy seam.
            stored_json = str(
                store._storage._conn.execute(  # noqa: SLF001
                    "SELECT payload_json FROM sync_events WHERE id = ?",
                    (event["id"],),
                ).fetchone()[0]
            )
        wire_json = json.dumps(
            {"id": event["id"], "kind": event["kind"], "payload": event["payload"]},
            sort_keys=True,
        )
        assert secret not in stored_json
        assert secret not in wire_json
    finally:
        runtime.shutdown()
        store.close()


def test_metadata_artifact_count_marks_an_unavailable_directory_as_incomplete(
    tmp_path: Path,
) -> None:
    store = RegistryStore(tmp_path / "store")
    runtime = DaemonRuntime(store, auto_build_envs=False)
    try:
        state = _state("ordinary-value")
        state["artifacts_dir"] = str(tmp_path / "missing-artifacts")

        payload = runtime._local_run_sync_payload(state)  # noqa: SLF001

        assert payload["telemetry"]["summary"]["artifact_count"] == 3
        assert payload["telemetry"]["summary"]["artifact_count_truncated"] is True
        assert "artifact_directory_unavailable" in payload["telemetry"]["omissions"]
    finally:
        runtime.shutdown()
        store.close()


def test_full_artifact_collection_rejects_file_and_directory_symlinks(
    tmp_path: Path,
) -> None:
    secret = "SOL012_HOST_FILE_SECRET"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "host-secret.txt").write_text(secret, encoding="utf-8")
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    try:
        (artifacts_dir / "linked.txt").symlink_to(outside / "host-secret.txt")
        linked_directory = tmp_path / "linked-artifacts"
        linked_directory.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink fixture is unavailable: {exc}")

    store = RegistryStore(tmp_path / "store")
    runtime = DaemonRuntime(store, auto_build_envs=False, telemetry="full")
    try:
        state = _state("ordinary-value")
        state["artifacts_dir"] = str(artifacts_dir)
        file_payload = runtime._local_run_sync_payload(state)  # noqa: SLF001
        state["artifacts_dir"] = str(linked_directory)
        directory_payload = runtime._local_run_sync_payload(state)  # noqa: SLF001

        assert secret not in json.dumps(file_payload, sort_keys=True)
        assert "unsafe_artifact_entry" in file_payload["telemetry"]["omissions"]
        assert secret not in json.dumps(directory_payload, sort_keys=True)
        assert "artifact_directory_unavailable" in directory_payload["telemetry"]["omissions"]
    finally:
        runtime.shutdown()
        store.close()


def test_full_artifact_collection_is_bounded_before_payload_materialization(
    tmp_path: Path,
) -> None:
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    huge = artifacts_dir / "huge.txt"
    with huge.open("wb") as stream:
        stream.seek(LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES * 2 - 1)
        stream.write(b"x")

    store = RegistryStore(tmp_path / "store")
    runtime = DaemonRuntime(store, auto_build_envs=False, telemetry="full")
    try:
        state = _state("ordinary-value")
        state.update(
            {
                "artifacts_dir": str(artifacts_dir),
                "result": None,
                "result_present": False,
                "stdout": "",
                "stderr": "",
            }
        )
        artifacts, omissions = runtime._collect_local_run_text_artifacts(state)  # noqa: SLF001

        assert len(artifacts) == 1
        assert artifacts[0]["size"] == LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES * 2
        assert len(artifacts[0]["content_text"].encode("utf-8")) <= (LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES)
        assert artifacts[0]["truncated"] is True
        assert "artifact_content_limit" in omissions

        huge.unlink()
        invalid_utf8 = artifacts_dir / "invalid.txt"
        invalid_utf8.write_bytes(b"\xff" * (LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES + 1))
        artifacts, omissions = runtime._collect_local_run_text_artifacts(state)  # noqa: SLF001
        assert sum(len(item["content_text"].encode("utf-8")) for item in artifacts) <= (
            LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES
        )
        assert "artifact_content_limit" in omissions

        invalid_utf8.unlink()
        for index in range(LOCAL_RUN_TEXT_ARTIFACT_MAX_COUNT + 5):
            (artifacts_dir / f"entry-{index:03}.txt").write_text("x", encoding="utf-8")
        artifacts, omissions = runtime._collect_local_run_text_artifacts(state)  # noqa: SLF001

        assert len(artifacts) == LOCAL_RUN_TEXT_ARTIFACT_MAX_COUNT
        assert "artifact_collection_limit" in omissions

        state.update(
            {
                "artifacts_dir": "",
                "result": {"large": "x" * (LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES * 2)},
                "result_present": True,
            }
        )
        artifacts, omissions = runtime._collect_local_run_text_artifacts(state)  # noqa: SLF001
        assert not any(artifact["kind"] == "result" for artifact in artifacts)
        assert "result_artifact_limit" in omissions
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.parametrize(
    ("source", "artifact_name"),
    (
        ("stdout", "stdout.txt"),
        ("stderr", "stderr.txt"),
        ("result_json", "result.json"),
    ),
)
def test_synthesized_text_artifacts_stop_after_the_collection_cap(
    tmp_path: Path,
    source: str,
    artifact_name: str,
) -> None:
    cap = LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES
    oversized = _TailSliceBomb(
        "x" * (cap * 4),
        forbidden_slice_start=cap,
    )
    state = _state("ordinary-value")
    state.update(
        {
            "artifacts_dir": "",
            "result": None,
            "result_present": False,
            "stdout": "",
            "stderr": "",
        }
    )
    if source == "result_json":
        state.update(
            {
                "result_present": True,
                "result_unreadable": True,
                "result_json": oversized,
            }
        )
    else:
        state[source] = oversized

    store = RegistryStore(tmp_path / "store")
    runtime = DaemonRuntime(store, auto_build_envs=False, telemetry="full")
    try:
        artifacts, omissions = runtime._collect_local_run_text_artifacts(state)  # noqa: SLF001

        assert len(artifacts) == 1
        assert artifacts[0]["name"] == artifact_name
        assert artifacts[0]["content_text"] == "x" * cap
        assert artifacts[0]["size"] == cap + 1
        assert artifacts[0]["size"] < len(oversized)
        assert artifacts[0]["truncated"] is True
        assert "artifact_content_limit" in omissions
    finally:
        runtime.shutdown()
        store.close()


def test_synthesized_text_artifact_preserves_exact_in_cap_utf8_size(tmp_path: Path) -> None:
    text = "plain \N{SNOWMAN} text"
    state = _state("ordinary-value")
    state.update(
        {
            "artifacts_dir": "",
            "result": None,
            "result_present": False,
            "stdout": text,
            "stderr": "",
        }
    )

    store = RegistryStore(tmp_path / "store")
    runtime = DaemonRuntime(store, auto_build_envs=False, telemetry="full")
    try:
        artifacts, omissions = runtime._collect_local_run_text_artifacts(state)  # noqa: SLF001

        assert len(artifacts) == 1
        assert artifacts[0]["content_text"] == text
        assert artifacts[0]["size"] == len(text.encode("utf-8"))
        assert artifacts[0]["truncated"] is False
        assert "artifact_content_limit" not in omissions
    finally:
        runtime.shutdown()
        store.close()


def test_synthesized_text_artifact_sanitizes_lone_surrogate(tmp_path: Path) -> None:
    state = _state("ordinary-value")
    state.update(
        {
            "artifacts_dir": "",
            "result": None,
            "result_present": False,
            "stdout": "before\ud800after",
            "stderr": "",
        }
    )

    store = RegistryStore(tmp_path / "store")
    runtime = DaemonRuntime(store, auto_build_envs=False, telemetry="full")
    try:
        artifacts, omissions = runtime._collect_local_run_text_artifacts(state)  # noqa: SLF001
        sanitized = "before\ufffdafter"

        assert len(artifacts) == 1
        assert artifacts[0]["content_text"] == sanitized
        assert artifacts[0]["size"] == len(sanitized.encode("utf-8"))
        assert artifacts[0]["truncated"] is False
        assert "artifact_content_limit" not in omissions
    finally:
        runtime.shutdown()
        store.close()


def test_artifact_directory_scan_cap_is_reported_as_a_lower_bound(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    for index in range(artifact_access.LOCAL_ARTIFACT_SCAN_MAX_ENTRIES + 1):
        (artifacts_dir / f"unsupported-{index:04}.bin").touch()

    store = RegistryStore(tmp_path / "store")
    runtime = DaemonRuntime(store, auto_build_envs=False, telemetry="full")
    try:
        state = _state("ordinary-value")
        state.update(
            {
                "artifacts_dir": str(artifacts_dir),
                "result": None,
                "result_present": False,
                "stdout": "",
                "stderr": "",
            }
        )

        payload = runtime._local_run_sync_payload(state)  # noqa: SLF001

        assert payload["telemetry"]["summary"]["artifact_count"] == (artifact_access.LOCAL_ARTIFACT_SCAN_MAX_ENTRIES)
        assert payload["telemetry"]["summary"]["artifact_count_truncated"] is True
        assert "artifact_scan_limit" in payload["telemetry"]["omissions"]
        assert not payload.get("artifacts")
    finally:
        runtime.shutdown()
        store.close()


def test_artifact_collection_portable_fallback_accepts_regular_files_and_rejects_links(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(artifact_access, "directory_fd_supported", lambda: False)
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "ordinary.txt").write_text("ordinary-body", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_secret = "PORTABLE_FALLBACK_OUTSIDE_SECRET"
    (outside / "secret.txt").write_text(outside_secret, encoding="utf-8")
    linked_root = tmp_path / "linked-root"
    try:
        (artifacts_dir / "linked.txt").symlink_to(outside / "secret.txt")
        linked_root.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink fixture is unavailable: {exc}")

    store = RegistryStore(tmp_path / "store")
    runtime = DaemonRuntime(store, auto_build_envs=False, telemetry="full")
    try:
        state = _state("ordinary-value")
        state.update(
            {
                "artifacts_dir": str(artifacts_dir),
                "result": None,
                "result_present": False,
                "stdout": "",
                "stderr": "",
            }
        )
        payload = runtime._local_run_sync_payload(state)  # noqa: SLF001
        wire = json.dumps(payload, sort_keys=True)

        assert "ordinary-body" in wire
        assert outside_secret not in wire
        assert "unsafe_artifact_entry" in payload["telemetry"]["omissions"]
        assert payload["telemetry"]["summary"]["artifact_count"] == 1
        assert payload["telemetry"]["summary"]["artifact_count_truncated"] is False

        state["artifacts_dir"] = str(linked_root)
        linked_payload = runtime._local_run_sync_payload(state)  # noqa: SLF001

        assert outside_secret not in json.dumps(linked_payload, sort_keys=True)
        assert "artifact_directory_unavailable" in linked_payload["telemetry"]["omissions"]
    finally:
        runtime.shutdown()
        store.close()


def test_artifact_directory_fails_closed_when_advertised_secure_open_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    monkeypatch.setattr(artifact_access, "directory_fd_supported", lambda: True)

    def fail_open(*args: Any, **kwargs: Any) -> int:
        raise OSError("simulated descriptor exhaustion")

    monkeypatch.setattr(artifact_access.os, "open", fail_open)

    assert artifact_access.ArtifactDirectory.open(artifacts_dir) is None


def test_startup_rewrites_legacy_raw_queue_before_heartbeat(tmp_path: Path) -> None:
    secret = "SOL012_LEGACY_QUEUE_SECRET"
    store = _seed_store(tmp_path)
    run = store.create_run(
        "telemetry_obj",
        args=[secret],
        kwargs={"api_key": secret},
    )
    event = store.enqueue_sync_event(
        "local_run_update",
        {
            "run": {
                **run,
                "input": {"kwargs": {"api_key": secret}},
                "result": secret,
                "error": secret,
                "artifacts": [{"content_text": secret}],
            }
        },
    )
    original_expiry = event["payload_expires_at"]

    runtime = DaemonRuntime(store, auto_build_envs=False)
    try:
        normalized = store.get_sync_event(event["id"])
        wire = json.dumps(normalized["payload"], sort_keys=True)

        assert secret not in wire
        assert normalized["payload"]["run"]["telemetry_level"] == "metadata"
        assert normalized["status"] == "pending"
        assert normalized["payload_expires_at"] == original_expiry
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.parametrize(
    ("level", "log_level"),
    [("metadata", logging.INFO), ("diagnostic", logging.WARNING), ("full", logging.WARNING)],
)
def test_telemetry_level_is_logged_at_startup(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    level: str,
    log_level: int,
) -> None:
    caplog.set_level(logging.INFO)
    store = RegistryStore(tmp_path / level)
    runtime = DaemonRuntime(
        store,
        auto_build_envs=False,
        telemetry=level,  # type: ignore[arg-type]
    )
    try:
        record = next(
            item
            for item in caplog.records
            if getattr(item, "spl_event", None) == "daemon_telemetry_policy"
            and "level={}".format(level) in item.getMessage()
        )
        assert record.levelno == log_level
        assert runtime.telemetry_status()["level"] == level
    finally:
        runtime.shutdown()
        store.close()


def test_server_job_local_projection_is_minimal_delivery_proof() -> None:
    assert DaemonRuntime._local_run_delivery_proof(  # noqa: SLF001
        {"id": "run-1", "status": "succeeded", "input": {"secret": "never-copy"}}
    ) == {"id": "run-1", "status": "succeeded"}


def test_cli_and_environment_select_telemetry_without_secret_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_DAEMON_TELEMETRY", "diagnostic")
    monkeypatch.setenv(
        "SPL_DAEMON_TELEMETRY_SENSITIVE_FIELDS",
        '["/input/kwargs/customer_secret"]',
    )

    environment = build_parser().parse_args(["serve"])
    explicit = build_parser().parse_args(
        [
            "serve",
            "--telemetry",
            "full",
            "--telemetry-sensitive-field",
            "/result/private",
        ]
    )

    assert environment.telemetry == "diagnostic"
    assert environment.telemetry_sensitive_field == ["/input/kwargs/customer_secret"]
    assert explicit.telemetry == "full"
    assert explicit.telemetry_sensitive_field == [
        "/input/kwargs/customer_secret",
        "/result/private",
    ]


def _seed_store(home: Path) -> RegistryStore:
    store = RegistryStore(home)
    store.register_env("default", sys.executable)
    store.register_object(
        "telemetry_obj",
        "telemetry_obj",
        "default",
        yaml_text=FUNCTION_YAML,
    )
    return store


def test_sync_payload_has_ttl_and_cascades_with_run_deletion(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("telemetry_obj")
        event = store.enqueue_sync_event(
            "local_run_update",
            {"run": {"id": run["id"], "status": "queued"}},
        )
        assert event["local_run_id"] == run["id"]
        assert event["payload_expires_at"] is not None

        store.mark_sync_event_sent(event["id"])
        store.update_run(run["id"], status="starting")
        store.update_run(run["id"], status="preparing_environment")
        store.update_run(run["id"], status="running")
        store.update_run(run["id"], status="failed", error="fixture")
        assert store.prune_runs(run_id=run["id"])["count"] == 1

        with pytest.raises(KeyError, match="sync event is not found"):
            store.get_sync_event(event["id"])
    finally:
        store.close()


def test_expired_local_telemetry_is_removed_before_sync(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    try:
        run = store.create_run("telemetry_obj")
        event = store.enqueue_sync_event(
            "local_run_update",
            {"run": {"id": run["id"], "status": "queued"}},
        )
        expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        with store._storage._lock, store._storage._conn:  # noqa: SLF001 - TTL boundary fixture.
            store._storage._conn.execute(  # noqa: SLF001
                "UPDATE sync_events SET payload_expires_at = ? WHERE id = ?",
                (expired, event["id"]),
            )

        assert store.list_pending_sync_events(limit=None) == []
        with pytest.raises(KeyError, match="sync event is not found"):
            store.get_sync_event(event["id"])
    finally:
        store.close()


def test_event_size_is_rejected_before_sqlite_persistence(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    try:
        with pytest.raises(ValueError, match="sync event exceeds"):
            store.enqueue_sync_event("local_run_update", {"raw": "x" * SYNC_EVENT_MAX_BYTES})
        with store._storage._lock:  # noqa: SLF001 - queue admission fixture.
            count = store._storage._conn.execute("SELECT COUNT(*) FROM sync_events").fetchone()[0]  # noqa: SLF001
        assert count == 0
    finally:
        store.close()


def test_existing_sync_schema_migrates_twice_and_backfills_run_link(tmp_path: Path) -> None:
    store = _seed_store(tmp_path)
    run = store.create_run("telemetry_obj")
    event = store.enqueue_sync_event(
        "local_run_update",
        {"run": {"id": run["id"], "status": "queued"}},
    )
    store.close()

    database = sqlite3.connect(tmp_path / "daemon.sqlite3")
    database.execute("PRAGMA foreign_keys = OFF")
    database.executescript(
        """
        ALTER TABLE sync_events RENAME TO sync_events_with_telemetry;
        CREATE TABLE sync_events (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            retryable INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            sent_at TEXT,
            error TEXT
        );
        INSERT INTO sync_events(
            id, kind, payload_json, status, attempts, retryable,
            created_at, updated_at, sent_at, error
        )
        SELECT id, kind, payload_json, status, attempts, retryable,
               created_at, updated_at, sent_at, error
        FROM sync_events_with_telemetry;
        DROP TABLE sync_events_with_telemetry;
        """
    )
    database.execute(
        "DELETE FROM schema_migrations WHERE id = ?",
        (SYNC_EVENT_TELEMETRY_MIGRATION_ID,),
    )
    database.execute("PRAGMA user_version = 2")
    database.commit()
    database.close()

    for _ in range(2):
        upgraded = RegistryStore(tmp_path)
        try:
            migrated = upgraded.get_sync_event(event["id"])
            assert migrated["local_run_id"] == run["id"]
            assert migrated["payload_expires_at"] is not None
            with upgraded._storage._lock:  # noqa: SLF001 - migration invariant fixture.
                assert (  # noqa: SLF001
                    upgraded._storage._conn.execute("PRAGMA user_version").fetchone()[0]
                    == SYNC_EVENT_TELEMETRY_SCHEMA_VERSION
                )
                assert upgraded._storage._conn.execute("PRAGMA foreign_key_check").fetchall() == []  # noqa: SLF001
                assert upgraded._storage._conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"  # noqa: SLF001
        finally:
            upgraded.close()
