from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

from spl import Deployment, lift
from spl.core import manifest as m_manifest
from spl.core._common import Run
from spl.core.entities.node import DEFAULT_PORT
from spl.core.entities.pipeline import Pipeline


@dataclass(frozen=True)
class ManifestBox:
    value: str


def _make_box() -> ManifestBox:
    return ManifestBox("seed")


def _summarize_box(box: ManifestBox) -> str:
    return "box:{}".format(box.value)


def _fail_box(box: ManifestBox) -> str:
    raise RuntimeError("cannot consume {}".format(box.value))


def _json_sum(left: int, right: int) -> int:
    return left + right


def _json_seed(seed: int) -> int:
    return seed


def _fail_json(value: int) -> int:
    raise RuntimeError("cannot consume {}".format(value))


def _save_box(path: str, obj: ManifestBox) -> None:
    Path(path).write_text(obj.value, encoding="utf-8")


def _load_box(path: str) -> ManifestBox:
    return ManifestBox(Path(path).read_text(encoding="utf-8"))


def _box_pipeline(consumer: Any = _summarize_box) -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(_make_box).alias("producer")
    pipeline = lift_any(consumer).bind(box=producer).alias("consumer").render("manifest_pipeline")
    return pipeline.add_adapter(ManifestBox, "box", save=_save_box, load=_load_box)


def _read_manifest(run: Run) -> dict[str, Any]:
    assert run.manifest_path is not None
    return cast(dict[str, Any], json.loads(run.manifest_path.read_text(encoding="utf-8")))


def _node_by_alias(manifest: dict[str, Any], alias: str) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        next(node for node in manifest["nodes"].values() if node["alias"] == alias),
    )


def _mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


def _tag_stats_edge(save_tag: str, load_tags: list[str], *, artifact_tag: str | None = None) -> dict[str, Any]:
    return {
        "source": {"node_id": "source", "port": "default"},
        "target": {"node_id": "target", "port": "value"},
        "artifact": {"kind": "artifact", "tag": artifact_tag or save_tag, "sha256": "0" * 64},
        "adapter": {
            "save": {"tag": save_tag, "accepted_tags": [save_tag], "source": "pipeline"},
            "load": {"tag": load_tags[0], "accepted_tags": load_tags, "source": "pipeline"},
        },
    }


def test_manifest_format_represents_multiple_output_ports() -> None:
    record = m_manifest.node_record(
        node_id="node-1",
        alias="pair",
        kind="function",
        name="pair",
        status="succeeded",
        fingerprint_sha256="a" * 64,
        outputs={
            "left": m_manifest.json_record(1),
            "right": m_manifest.json_record({"ok": True}),
        },
    )

    assert set(record["outputs"]) == {"left", "right"}
    assert record["outputs"]["left"]["kind"] == "json"
    assert record["fingerprint"]["version"] == 1


def test_failed_default_run_retains_manifest_and_completed_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_box_pipeline(_fail_box)).run()

    with pytest.raises(RuntimeError, match="cannot consume seed"):
        run.value("consumer")

    assert run.run_dir is not None
    assert run.run_dir.exists()
    manifest = _read_manifest(run)
    producer = _node_by_alias(manifest, "producer")
    consumer = _node_by_alias(manifest, "consumer")

    assert manifest["status"] == "failed"
    assert manifest["keep"] == "on_failure"
    assert producer["status"] == "succeeded"
    assert producer["outputs"][DEFAULT_PORT]["kind"] == "artifact"
    assert producer["outputs"][DEFAULT_PORT]["ref"]["uri"].startswith("artifacts/")
    assert producer["fingerprint"]["sha256"]
    assert consumer["status"] == "failed"
    assert "cannot consume seed" in consumer["error"]
    assert manifest["edges"][0]["adapter"]["load"]["source"] == "pipeline"
    assert (run.run_dir / producer["outputs"][DEFAULT_PORT]["ref"]["uri"]).exists()
    if os.name == "posix":
        assert _mode(run.run_dir) == 0o700
        assert run.manifest_path is not None
        assert _mode(run.manifest_path) == 0o600
        assert _mode(run.run_dir / "artifacts") == 0o700
        assert _mode(run.run_dir / producer["outputs"][DEFAULT_PORT]["ref"]["uri"]) == 0o600
    edge_summary = m_manifest.manifest_summary(manifest)["edge_adapters"]
    assert len(edge_summary) == 1
    assert edge_summary[0]["source"] == "producer.default"
    assert edge_summary[0]["target"] == "consumer.box"
    assert edge_summary[0]["tag"] == "box"
    assert edge_summary[0]["source_level"] == "pipeline"
    assert "_save_box" in edge_summary[0]["save"]
    assert "_load_box" in edge_summary[0]["load"]


def test_failed_default_json_run_materializes_deferred_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    source = lift_any(_json_seed).alias("source")
    pipeline = lift_any(_fail_json).bind(value=source).alias("consumer").render("deferred_failure")
    run = Deployment(pipeline).run(seed=7)

    with pytest.raises(RuntimeError, match="cannot consume 7"):
        run.value("consumer")

    assert run.run_dir is not None
    assert run.run_dir.exists()
    manifest = _read_manifest(run)
    source_record = _node_by_alias(manifest, "source")
    consumer = _node_by_alias(manifest, "consumer")

    assert manifest["status"] == "failed"
    assert source_record["status"] == "succeeded"
    assert source_record["outputs"][DEFAULT_PORT]["kind"] == "json"
    assert source_record["outputs"][DEFAULT_PORT]["value"] == 7
    assert consumer["status"] == "failed"
    assert consumer["inputs"]["value"]["kind"] == "json"
    assert consumer["inputs"]["value"]["value"] == 7
    assert "cannot consume 7" in consumer["error"]
    edge_summary = m_manifest.manifest_summary(manifest)["edge_adapters"]
    assert edge_summary[0]["tag"] == "json"
    assert edge_summary[0]["save"] == "json"
    assert edge_summary[0]["load"] == "json"


def test_local_tag_stats_reads_fixture_manifests_and_empty_catalog(tmp_path: Path) -> None:
    empty_home = tmp_path / "empty"
    assert m_manifest.local_tag_stats(empty_home) == {
        "runs_scanned": 0,
        "edges_scanned": 0,
        "tags": [],
        "pairs": [],
    }

    runs_home = tmp_path / "runs"
    run_a = runs_home / "run-a"
    run_b = runs_home / "run-b"
    m_manifest.atomic_write_json(
        run_a / m_manifest.RUN_MANIFEST_FILENAME,
        {
            "run_id": "run-a",
            "edges": [
                _tag_stats_edge("json", ["json"]),
                _tag_stats_edge("csv", ["tsv"]),
            ],
        },
    )
    m_manifest.atomic_write_json(
        run_b / m_manifest.RUN_MANIFEST_FILENAME,
        {
            "run_id": "run-b",
            "edges": [_tag_stats_edge("json", ["json"])],
        },
    )

    assert m_manifest.local_tag_stats(runs_home) == {
        "runs_scanned": 2,
        "edges_scanned": 3,
        "tags": [
            {"tag": "json", "edge_count": 2, "run_count": 2},
            {"tag": "csv", "edge_count": 1, "run_count": 1},
        ],
        "pairs": [
            {"save_tag": "json", "load_tags": ["json"], "edge_count": 2, "run_count": 2},
            {"save_tag": "csv", "load_tags": ["tsv"], "edge_count": 1, "run_count": 1},
        ],
    }


def test_successful_default_run_removes_retained_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runs_home = tmp_path / "runs"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    lift_any = cast(Any, lift)
    run = Deployment(lift_any(_json_sum).alias("sum").render("deferred_success")).run(left=2, right=5)

    with run:
        assert run.value("sum") == 7

    assert run.run_dir is None
    assert run.manifest_path is None
    assert not runs_home.exists()


def test_successful_default_artifact_run_removes_retained_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_box_pipeline()).run()

    with run:
        assert run.value("consumer") == "box:seed"

    assert run.run_dir is not None
    assert not run.run_dir.exists()


def test_keep_true_preserves_successful_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_box_pipeline()).run(keep=True)

    with run:
        assert run.value("consumer") == "box:seed"

    assert run.run_dir is not None
    assert run.run_dir.exists()
    if os.name == "posix":
        assert _mode(run.run_dir) == 0o700
        assert run.manifest_path is not None
        assert _mode(run.manifest_path) == 0o600
    manifest = _read_manifest(run)
    consumer = _node_by_alias(manifest, "consumer")

    assert manifest["status"] == "succeeded"
    assert manifest["keep"] is True
    assert consumer["outputs"][DEFAULT_PORT]["kind"] == "json"


def test_json_native_outputs_are_inline_in_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def add(left: int, right: int) -> int:
        return left + right

    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    pipeline = lift_any(add).alias("sum").render("json_manifest")
    run = Deployment(pipeline).run(keep=True, left=2, right=5)

    with run:
        assert run.value("sum") == 7

    manifest = _read_manifest(run)
    node = _node_by_alias(manifest, "sum")

    assert run._artifacts_dir is None
    assert node["inputs"]["left"]["value"] == 2
    assert node["outputs"][DEFAULT_PORT]["kind"] == "json"
    assert node["outputs"][DEFAULT_PORT]["value"] == 7
    assert len(node["outputs"][DEFAULT_PORT]["sha256"]) == 64


def test_sanitized_manifest_omits_sensitive_inline_previews() -> None:
    manifest = {
        "nodes": {
            "node-1": {
                "inputs": {
                    "password": {
                        "kind": "json",
                        "tag": "json",
                        "value": "hunter2",
                        "sha256": "a" * 64,
                    },
                    "payload": {
                        "kind": "json",
                        "tag": "json",
                        "value": {"secret": "route-default-preview"},
                        "sha256": "b" * 64,
                    },
                    "ordinary": {
                        "kind": "json",
                        "tag": "json",
                        "value": "visible-preview",
                        "sha256": "c" * 64,
                    },
                }
            }
        }
    }

    sanitized = m_manifest.sanitize_manifest_inline(manifest)
    rendered = json.dumps(sanitized, sort_keys=True)
    password = sanitized["nodes"]["node-1"]["inputs"]["password"]
    payload = sanitized["nodes"]["node-1"]["inputs"]["payload"]
    ordinary = sanitized["nodes"]["node-1"]["inputs"]["ordinary"]

    assert "hunter2" not in rendered
    assert "route-default-preview" not in rendered
    assert password["value_preview"] == "<omitted>"
    assert password["value_preview_omitted"] is True
    assert password["sha256"] == "a" * 64
    assert password["value_size_bytes"] > 0
    assert payload["value_preview"] == "<omitted>"
    assert ordinary["value_preview"] == '"visible-preview"'


def test_repeated_run_id_does_not_overwrite_existing_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    monkeypatch.setattr(m_manifest, "new_run_id", lambda: "fixed-run-id")
    pipeline = _box_pipeline()

    first = Deployment(pipeline).run(keep=True)
    with first:
        assert first.value("consumer") == "box:seed"

    second = Deployment(pipeline).run(keep=True)
    with pytest.raises(FileExistsError):
        second.value("consumer")

    assert first.run_dir is not None
    assert first.run_dir.exists()
    assert _read_manifest(first)["status"] == "succeeded"


def test_manifest_survives_killed_subprocess_mid_run(tmp_path: Path) -> None:
    runs_home = tmp_path / "runs"
    script = textwrap.dedent(
        """
        import time

        from spl import Deployment, lift


        def slow():
            time.sleep(30)
            return 1


        run = Deployment(lift(slow).alias("slow").render("kill_manifest")).run(keep=True)
        print(run.run_id, flush=True)
        run.value("slow")
        """
    )
    env = os.environ.copy()
    src_path = str(Path(__file__).resolve().parents[2] / "src")
    env["PYTHONPATH"] = src_path if not env.get("PYTHONPATH") else os.pathsep.join([src_path, env["PYTHONPATH"]])
    env["SPL_RUNS_HOME"] = str(runs_home)
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        assert proc.stdout is not None
        run_id = proc.stdout.readline().strip()
        manifest_path = runs_home / run_id / m_manifest.RUN_MANIFEST_FILENAME
        deadline = time.monotonic() + 10
        payload: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            if manifest_path.exists():
                payload = cast(dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8")))
                if payload["nodes"]:
                    break
            time.sleep(0.05)
        assert payload is not None
        proc.kill()
        proc.wait(timeout=5)

        payload = cast(dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8")))
        assert payload["schema_version"] == m_manifest.RUN_MANIFEST_SCHEMA_VERSION
        assert payload["status"] == "running"
        assert _node_by_alias(payload, "slow")["status"] == "running"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_local_run_list_show_and_prune(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_box_pipeline()).run(keep=True)
    with run:
        assert run.value("consumer") == "box:seed"

    listed = m_manifest.list_local_runs()
    shown = m_manifest.show_local_run(run.run_id)
    shown_full = m_manifest.show_local_run(run.run_id, include_inline_values=True)

    assert listed[0]["id"] == run.run_id
    assert listed[0]["has_manifest"] is True
    assert listed[0]["disk_size_bytes"] > 0
    output = shown["manifest"]["nodes"][_node_by_alias(shown["manifest"], "consumer")["id"]]["outputs"][DEFAULT_PORT]
    assert output["kind"] == "json"
    assert output["value_omitted"] is True
    assert "value" not in output
    full_output = shown_full["manifest"]["nodes"][_node_by_alias(shown_full["manifest"], "consumer")["id"]]["outputs"][
        DEFAULT_PORT
    ]
    assert full_output["value"] == "box:seed"

    preview = m_manifest.prune_local_runs(statuses=["succeeded"], dry_run=True)
    assert preview["candidates"][0]["id"] == run.run_id
    assert run.run_dir is not None and run.run_dir.exists()

    result = m_manifest.prune_local_runs(statuses=["succeeded"])
    assert result["pruned"][0]["id"] == run.run_id
    assert run.run_dir is not None and not run.run_dir.exists()
