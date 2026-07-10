from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
from pathlib import Path
from typing import Any, cast

import pytest

from spl import Deployment, lift
from spl.core import resume as m_resume
from spl.core._common import Run
from spl.core.entities.adapter import Adapter, make_key
from spl.core.entities.node import DEFAULT_PORT
from spl.core.entities.pipeline import Pipeline


@dataclass(frozen=True)
class ResumeBox:
    value: str


_calls: dict[str, int] = {}


def _reset_calls(*names: str) -> None:
    _calls.clear()
    for name in names:
        _calls[name] = 0


def _count(name: str) -> None:
    _calls[name] = _calls.get(name, 0) + 1


def _make_resume_box() -> ResumeBox:
    _count("producer")
    return ResumeBox("seed")


def _consume_resume_box(box: ResumeBox) -> str:
    _count("consumer")
    return "box:{}".format(box.value)


def _save_resume_box(path: str, obj: ResumeBox) -> None:
    Path(path).write_text(obj.value, encoding="utf-8")


def _mutating_save_resume_box(path: str, obj: ResumeBox) -> None:
    Path(path).write_text("changed:{}".format(obj.value), encoding="utf-8")


def _bad_load_resume_box(path: str) -> ResumeBox:
    raise RuntimeError("bad load for {}".format(Path(path).read_text(encoding="utf-8")))


def _good_load_resume_box(path: str) -> ResumeBox:
    return ResumeBox(Path(path).read_text(encoding="utf-8"))


def _resume_box_pipeline() -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(_make_resume_box).alias("producer")
    pipeline = lift_any(_consume_resume_box).bind(box=producer).alias("consumer").render("resume_box")
    return pipeline.add_adapter(ResumeBox, "box", save=_save_resume_box, load=_bad_load_resume_box)


def _good_box_adapter() -> Adapter:
    return Adapter(
        key=make_key(ResumeBox, "box"),
        save=_save_resume_box,
        load=_good_load_resume_box,
        py_type=ResumeBox,
        format="box",
    )


def _good_box_adapter_with_mutating_save() -> Adapter:
    return Adapter(
        key=make_key(ResumeBox, "box"),
        save=_mutating_save_resume_box,
        load=_good_load_resume_box,
        py_type=ResumeBox,
        format="box",
    )


def _source(seed: int) -> int:
    _count("source")
    return seed


def _left(value: int) -> int:
    _count("left")
    return value + 10


def _right(value: int) -> int:
    _count("right")
    return value + 100


def _join(left: int, right: int) -> int:
    _count("join")
    return left + right


def _side() -> str:
    _count("side")
    return "side"


def _parse_seed(seed: str) -> int:
    _count("parse")
    if seed == "bad":
        raise RuntimeError("parse boom")
    return int(seed)


def _total(value: int) -> int:
    _count("total")
    return value + 10


def _diamond_pipeline() -> Pipeline:
    lift_any = cast(Any, lift)
    source = lift_any(_source).alias("source")
    left = lift_any(_left).bind(value=source).alias("left")
    right = lift_any(_right).bind(value=source).alias("right")
    join = lift_any(_join).bind(left=left, right=right).alias("join")
    side = lift_any(_side).alias("side")
    return dataclass_replace(join.pipeline | side.pipeline, name="resume_diamond")


def _parse_total_pipeline() -> Pipeline:
    lift_any = cast(Any, lift)
    parse = lift_any(_parse_seed).alias("parse")
    return lift_any(_total).bind(value=parse).alias("total").render("resume_upstream_failed")


def _read_manifest(run: Run) -> dict[str, Any]:
    assert run.manifest_path is not None
    return cast(dict[str, Any], json.loads(run.manifest_path.read_text(encoding="utf-8")))


def _node_by_alias(manifest: dict[str, Any], alias: str) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        next(node for node in manifest["nodes"].values() if node["alias"] == alias),
    )


def _artifact_path(run: Run, output_record: dict[str, Any]) -> Path:
    uri = output_record["ref"]["uri"]
    path = Path(uri)
    if path.is_absolute():
        return path
    assert run.run_dir is not None
    return run.run_dir / path


def test_resume_with_load_adapter_override_does_not_recalculate_upstream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _reset_calls("producer", "consumer")
    pipeline = _resume_box_pipeline()
    run = Deployment(pipeline).run()

    with pytest.raises(RuntimeError, match="bad load for seed"):
        run.value("consumer")

    assert _calls == {"producer": 1, "consumer": 0}

    resumed = run.resume(
        from_="consumer",
        adapters={("producer", DEFAULT_PORT): _good_box_adapter()},
        keep=True,
    )
    with resumed:
        assert resumed.value("consumer") == "box:seed"

    assert _calls == {"producer": 1, "consumer": 1}
    manifest = _read_manifest(resumed)
    assert manifest["parent_run_id"] == run.run_id
    assert _node_by_alias(manifest, "producer")["status"] == "frozen"
    assert _node_by_alias(manifest, "consumer")["status"] == "succeeded"
    assert manifest["edges"][0]["adapter"]["load"]["source"] == "run-override"


def test_resume_pair_override_ignores_save_half_for_frozen_producer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _reset_calls("producer", "consumer")
    pipeline = _resume_box_pipeline()
    run = Deployment(pipeline).run()

    with pytest.raises(RuntimeError, match="bad load for seed"):
        run.value("consumer")

    parent_manifest = _read_manifest(run)
    parent_output = _node_by_alias(parent_manifest, "producer")["outputs"][DEFAULT_PORT]
    parent_path = _artifact_path(run, parent_output)
    parent_bytes = parent_path.read_bytes()

    resumed = run.resume(
        from_="consumer",
        adapters={("producer", DEFAULT_PORT): _good_box_adapter_with_mutating_save()},
        keep=True,
    )
    with resumed:
        assert resumed.value("consumer") == "box:seed"

    child_manifest = _read_manifest(resumed)
    child_output = _node_by_alias(child_manifest, "producer")["outputs"][DEFAULT_PORT]
    child_path = _artifact_path(resumed, child_output)

    assert _calls == {"producer": 1, "consumer": 1}
    assert _node_by_alias(child_manifest, "producer")["status"] == "frozen"
    assert parent_bytes == b"seed"
    assert child_path.read_bytes() == parent_bytes
    assert child_output["ref"]["sha256"] == parent_output["ref"]["sha256"]
    assert child_output["ref"]["size"] == parent_output["ref"]["size"]
    assert child_output["ref"]["sha256"] == hashlib.sha256(parent_bytes).hexdigest()
    assert child_manifest["edges"][0]["adapter"]["save"]["identity"]["save"].endswith("_mutating_save_resume_box")
    assert child_manifest["edges"][0]["adapter"]["load"]["identity"]["load"].endswith("_good_load_resume_box")


def test_resume_kwargs_recalculates_consumers_and_descendants_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _reset_calls("source", "left", "right", "join", "side")
    pipeline = _diamond_pipeline()
    run = Deployment(pipeline).run(keep=True, seed=1)
    with run:
        assert run.value("join") == 112
        assert run.value("side") == "side"

    resumed = run.resume(from_=[], kwargs={"seed": 2}, keep=True)
    with resumed:
        assert resumed.value("join") == 114

    assert _calls == {"source": 2, "left": 2, "right": 2, "join": 2, "side": 1}
    manifest = _read_manifest(resumed)
    assert _node_by_alias(manifest, "side")["status"] == "frozen"
    assert _node_by_alias(manifest, "source")["status"] == "succeeded"


def test_resume_from_middle_recalculates_descendants_and_freezes_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _reset_calls("source", "left", "right", "join", "side")
    pipeline = _diamond_pipeline()
    run = Deployment(pipeline).run(keep=True, seed=3)
    with run:
        assert run.value("join") == 116
        assert run.value("side") == "side"

    resumed = run.resume(from_="left", keep=True)
    with resumed:
        assert resumed.value("join") == 116

    assert _calls == {"source": 1, "left": 2, "right": 1, "join": 2, "side": 1}
    manifest = _read_manifest(resumed)
    assert _node_by_alias(manifest, "source")["status"] == "frozen"
    assert _node_by_alias(manifest, "right")["status"] == "frozen"
    assert _node_by_alias(manifest, "left")["status"] == "succeeded"
    assert _node_by_alias(manifest, "join")["status"] == "succeeded"


def test_failed_dependency_records_upstream_failed_and_resume_recalculates_descendant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _reset_calls("parse", "total")
    pipeline = _parse_total_pipeline()
    run = Deployment(pipeline).run(seed="bad")

    with pytest.raises(RuntimeError, match="parse boom"):
        run.value("total")

    manifest = _read_manifest(run)
    parse = _node_by_alias(manifest, "parse")
    total = _node_by_alias(manifest, "total")
    assert manifest["status"] == "failed"
    assert parse["status"] == "failed"
    assert total["status"] == "upstream-failed"
    assert "upstream node `parse` failed" in total["error"]
    assert "parse boom" in total["error"]
    assert _calls == {"parse": 1, "total": 0}

    resumed = run.resume(from_="parse", kwargs={"seed": "4"}, keep=True)
    with resumed:
        assert resumed.value("total") == 14

    resumed_manifest = _read_manifest(resumed)
    assert resumed_manifest["parent_run_id"] == run.run_id
    assert _node_by_alias(resumed_manifest, "parse")["status"] == "succeeded"
    assert _node_by_alias(resumed_manifest, "total")["status"] == "succeeded"
    assert _calls == {"parse": 2, "total": 1}


def test_resume_refuses_to_freeze_upstream_failed_node(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _reset_calls("parse", "total")
    run = Deployment(_parse_total_pipeline()).run(seed="bad")

    with pytest.raises(RuntimeError, match="parse boom"):
        run.value("total")

    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        run.resume(from_=[], keep=True)

    message = str(exc_info.value)
    assert "total: node status is `upstream-failed`" in message
    assert "from_='total'" in message


def test_resume_reports_corrupted_frozen_artifact_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _reset_calls("producer", "consumer")
    pipeline = _resume_box_pipeline()
    run = Deployment(pipeline).run()
    with pytest.raises(RuntimeError, match="bad load for seed"):
        run.value("consumer")

    manifest = _read_manifest(run)
    producer = _node_by_alias(manifest, "producer")
    artifact_path = cast(Path, run.run_dir) / producer["outputs"][DEFAULT_PORT]["ref"]["uri"]
    artifact_path.write_text("broken", encoding="utf-8")

    with pytest.raises(m_resume.ResumeValidationError) as exc_info:
        run.resume(
            from_="consumer",
            adapters={("producer", DEFAULT_PORT): _good_box_adapter()},
            keep=True,
        )

    message = str(exc_info.value)
    assert "producer:default artifact sha256 mismatch" in message
    assert "expected" in message
    assert "actual" in message
    assert "from_='producer'" in message
    assert _calls == {"producer": 1, "consumer": 0}


def test_resume_from_resume_preserves_lineage_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _reset_calls("source")
    lift_any = cast(Any, lift)
    pipeline = lift_any(_source).alias("source").render("resume_lineage")
    first = Deployment(pipeline).run(keep=True, seed=1)
    with first:
        assert first.value("source") == 1

    second = first.resume(from_="source", kwargs={"seed": 2}, keep=True)
    with second:
        assert second.value("source") == 2

    third = second.resume(from_="source", kwargs={"seed": 3}, keep=True)
    with third:
        assert third.value("source") == 3

    assert _read_manifest(second)["parent_run_id"] == first.run_id
    assert _read_manifest(third)["parent_run_id"] == second.run_id
    assert _calls == {"source": 3}


def test_deployment_resume_loads_manifest_by_run_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _reset_calls("source")
    lift_any = cast(Any, lift)
    pipeline = lift_any(_source).alias("source").render("resume_by_id")
    run = Deployment(pipeline).run(keep=True, seed=5)
    with run:
        assert run.value("source") == 5

    resumed = Deployment(pipeline).resume(run.run_id, from_="source", kwargs={"seed": 6}, keep=True)
    with resumed:
        assert resumed.value("source") == 6

    assert _read_manifest(resumed)["parent_run_id"] == run.run_id
    assert _calls == {"source": 2}


def test_descendant_closure_handles_diamond_disconnected_and_multiple_roots() -> None:
    pipeline = _diamond_pipeline()
    aliases = pipeline.aliases

    assert m_resume.close_over_descendants(pipeline, {aliases["left"]}) == {aliases["left"], aliases["join"]}
    assert m_resume.close_over_descendants(pipeline, {aliases["source"]}) == {
        aliases["source"],
        aliases["left"],
        aliases["right"],
        aliases["join"],
    }
    assert m_resume.close_over_descendants(pipeline, {aliases["left"], aliases["side"]}) == {
        aliases["left"],
        aliases["join"],
        aliases["side"],
    }
