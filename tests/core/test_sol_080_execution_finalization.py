from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

from spl import Deployment, lift
from spl.core import manifest as m_manifest
from spl.core._common import Run


def _finite_output() -> float:
    return 1.5


def _nan_output() -> float:
    return float("nan")


def _nested_nan_output() -> dict[str, Any]:
    return {"outer": [{"score": float("nan")}]}


def _large_finite_output() -> dict[str, int | float]:
    return {
        "large_integer": 123456789012345678901234567890,
        "integral_float": 1e20,
    }


def _failing_output() -> int:
    raise RuntimeError("execution boom")


def _increment(value: float) -> float:
    return value + 1


class _AdapterBox:
    def __init__(self, value: str) -> None:
        self.value = value


def _box_output() -> _AdapterBox:
    return _AdapterBox("payload")


def _consume_box(value: _AdapterBox) -> str:
    return value.value


def _failing_box_save(path: str, value: _AdapterBox) -> None:
    del path, value
    raise RuntimeError("adapter save exploded")


def _successful_box_save(path: str, value: _AdapterBox) -> None:
    Path(path).write_text(value.value, encoding="utf-8")


def _successful_box_load(path: str) -> _AdapterBox:
    return _AdapterBox(Path(path).read_text(encoding="utf-8"))


def _failing_box_load(path: str) -> _AdapterBox:
    del path
    raise RuntimeError("adapter load exploded")


def _pipeline(function: Callable[[], Any], alias: str = "producer") -> Any:
    return cast(Any, lift)(function).alias(alias).render("sol080_finalization")


def _box_pipeline(*, fail_save: bool) -> Any:
    lift_any = cast(Any, lift)
    producer = lift_any(_box_output).alias("producer")
    pipeline = lift_any(_consume_box).bind(value=producer).alias("consumer").render("adapter_finalization")
    return pipeline.add_adapter(
        _AdapterBox,
        "box",
        save=_failing_box_save if fail_save else _successful_box_save,
        load=_successful_box_load if fail_save else _failing_box_load,
    )


def _manifest(run: Run) -> dict[str, Any]:
    assert run.manifest_path is not None
    return cast(dict[str, Any], json.loads(run.manifest_path.read_text(encoding="utf-8")))


def _node_by_alias(manifest: dict[str, Any], alias: str) -> dict[str, Any]:
    return cast(dict[str, Any], next(node for node in manifest["nodes"].values() if node["alias"] == alias))


def test_nan_output_fails_the_producing_node_and_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_pipeline(_nan_output, "nan-producer")).run(keep=True)

    with pytest.raises(
        ValueError,
        match=r"node `nan-producer` port `default`.*\$.*non-finite",
    ):
        run.value("nan-producer")

    manifest = _manifest(run)
    node = _node_by_alias(manifest, "nan-producer")
    assert manifest["status"] == "failed"
    assert node["status"] == "failed"
    assert node["outputs"] == {}
    assert "non-finite" in node["error"]


def test_nested_nan_error_names_node_port_and_json_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = Deployment(_pipeline(_nested_nan_output, "nested-producer")).run(keep=True)

    with pytest.raises(ValueError) as exc_info:
        run.value("nested-producer")

    message = str(exc_info.value)
    assert "node `nested-producer`" in message
    assert "port `default`" in message
    assert '$["outer"][0]["score"]' in message
    assert "non-finite" in message
    manifest = _manifest(run)
    assert manifest["status"] == "failed"
    assert _node_by_alias(manifest, "nested-producer")["status"] == "failed"


def test_large_finite_result_reaches_successful_output_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = cast(Run, Deployment(_pipeline(_large_finite_output, "large-finite-producer")).run(keep=True))

    expected = _large_finite_output()
    with run:
        assert run.value("large-finite-producer") == expected

    manifest = _manifest(run)
    node = _node_by_alias(manifest, "large-finite-producer")
    assert manifest["status"] == "succeeded"
    assert node["status"] == "succeeded"
    assert node["outputs"]["default"]["kind"] == "json"
    assert node["outputs"]["default"]["value"] == expected


def test_output_shape_normalization_failure_is_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    pipeline = _pipeline(_finite_output)
    run = Run(lambda node, kwargs: {}, pipeline, keep=True)

    with pytest.raises(ValueError, match=r"node `producer`.*missing port\(s\): default"):
        run.value("producer")

    manifest = _manifest(run)
    assert manifest["status"] == "failed"
    assert _node_by_alias(manifest, "producer")["status"] == "failed"


def test_real_adapter_save_failure_is_attributed_to_the_producing_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = cast(Run, Deployment(_box_pipeline(fail_save=True)).run(keep=True))

    with pytest.raises(RuntimeError, match="adapter save exploded") as exc_info:
        run.value("consumer")

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert str(exc_info.value.__cause__) == "adapter save exploded"

    manifest = _manifest(run)
    producer = _node_by_alias(manifest, "producer")
    consumer = _node_by_alias(manifest, "consumer")
    assert manifest["status"] == "failed"
    assert producer["status"] == "failed"
    assert producer["outputs"] == {}
    assert "producer" in producer["error"]
    assert "default" in producer["error"]
    assert "adapter save/materialization" in producer["error"]
    assert "adapter save exploded" in producer["error"]
    assert consumer["status"] == "upstream-failed"
    assert "upstream node `producer` failed" in consumer["error"]


def test_real_adapter_load_failure_remains_attributed_to_the_consuming_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = cast(Run, Deployment(_box_pipeline(fail_save=False)).run(keep=True))

    with pytest.raises(RuntimeError, match="adapter load exploded") as exc_info:
        run.value("consumer")

    assert str(exc_info.value) == "adapter load exploded"

    manifest = _manifest(run)
    producer = _node_by_alias(manifest, "producer")
    consumer = _node_by_alias(manifest, "consumer")
    assert manifest["status"] == "failed"
    assert producer["status"] == "succeeded"
    assert producer["outputs"]["default"]["kind"] == "artifact"
    assert consumer["status"] == "failed"
    assert "adapter load exploded" in consumer["error"]


@pytest.mark.parametrize("failure_point", ["normalization", "fingerprint", "manifest-write"])
def test_output_commit_failure_has_one_failed_terminal_state(
    failure_point: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = cast(Run, Deployment(_pipeline(_finite_output)).run(keep=True))
    detail = "injected {} failure".format(failure_point)

    if failure_point == "normalization":

        def fail_output_records(self: Run, node: Any, result: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
            del self, node, result, kwargs
            raise RuntimeError(detail)

        monkeypatch.setattr(Run, "_output_records", fail_output_records)
    elif failure_point == "fingerprint":
        original_fingerprint = Run._node_fingerprint
        calls = 0

        def fail_success_fingerprint(self: Run, node: Any, inputs: Any, adapters: Any) -> str:
            nonlocal calls
            calls += 1
            if calls >= 2:
                raise RuntimeError(detail)
            return original_fingerprint(self, node, inputs, adapters)

        monkeypatch.setattr(Run, "_node_fingerprint", fail_success_fingerprint)
    else:
        original_write = Run._write_node_manifest
        failed = False

        def fail_success_write(self: Run, node: Any, **kwargs: Any) -> None:
            nonlocal failed
            if kwargs.get("status") == "succeeded" and not failed:
                failed = True
                raise RuntimeError(detail)
            original_write(self, node, **kwargs)

        monkeypatch.setattr(Run, "_write_node_manifest", fail_success_write)

    with pytest.raises(RuntimeError, match=detail):
        run.value("producer")

    manifest = _manifest(run)
    node = _node_by_alias(manifest, "producer")
    assert manifest["status"] == "failed"
    assert node["status"] == "failed"
    assert detail in manifest["error"]
    assert detail in node["error"]


def test_close_marks_an_abandoned_running_node_failed_and_logs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    pipeline = _pipeline(_finite_output)
    run = cast(Run, Deployment(pipeline).run(keep=True))
    node = pipeline.aliases["producer"]
    run._ensure_manifest_writer()
    run._write_node_manifest(node, status="running")

    with caplog.at_level(logging.ERROR, logger="spl.core._common"):
        with pytest.raises(RuntimeError, match="closed before node finalization completed"):
            run.close()

    manifest = _manifest(run)
    assert manifest["status"] == "failed"
    assert _node_by_alias(manifest, "producer")["status"] == "failed"
    assert "closed before node finalization completed" in manifest["error"]
    assert "closed before node finalization completed" in caplog.text
    run.close()


def test_close_materializes_a_never_started_retained_run_as_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = cast(Run, Deployment(_pipeline(_finite_output)).run(keep=True))

    with pytest.raises(RuntimeError, match=r"producer \(missing\)"):
        run.close()

    manifest = _manifest(run)
    assert manifest["status"] == "failed"
    producer = _node_by_alias(manifest, "producer")
    assert producer["status"] == "failed"
    assert producer["fingerprint"]["sha256"] is None


def test_close_requires_every_pipeline_node_to_be_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(_finite_output).alias("producer")
    pipeline = lift_any(_increment).bind(value=producer).alias("consumer").render("complete_pipeline")
    run = cast(Run, Deployment(pipeline).run(keep=True))

    with pytest.raises(RuntimeError, match="closed before node finalization completed"):
        with run:
            assert run.value("producer") == 1.5

    manifest = _manifest(run)
    assert manifest["status"] == "failed"
    assert _node_by_alias(manifest, "producer")["status"] == "succeeded"
    consumer = _node_by_alias(manifest, "consumer")
    assert consumer["status"] == "failed"
    assert consumer["outputs"] == {}


def test_close_validates_partial_transient_runs_without_materializing_a_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    lift_any = cast(Any, lift)
    producer = lift_any(_finite_output).alias("producer")
    pipeline = lift_any(_increment).bind(value=producer).alias("consumer").render("complete_transient_pipeline")
    run = cast(Run, Deployment(pipeline).run(keep=False))

    assert run.value("producer") == 1.5
    with pytest.raises(RuntimeError, match=r"consumer \(missing\)"):
        run.close()

    assert run.manifest_path is None
    run.close()


def test_close_preserves_a_recorded_node_failure_when_run_finalization_was_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    pipeline = _pipeline(_finite_output)
    run = cast(Run, Deployment(pipeline).run(keep=True))
    node = pipeline.aliases["producer"]
    run._ensure_manifest_writer()
    run._write_node_manifest(node, status="failed", error="real finalization failure")

    with caplog.at_level(logging.ERROR, logger="spl.core._common"):
        with pytest.raises(RuntimeError, match="failed node but no terminal run status"):
            run.close()

    manifest = _manifest(run)
    assert manifest["status"] == "failed"
    assert manifest["error"] == "real finalization failure"
    assert _node_by_alias(manifest, "producer")["error"] == "real finalization failure"
    assert "terminal run status was missing" in caplog.text


def test_exception_inside_close_is_explicitly_chained_from_execution_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = cast(Run, Deployment(_pipeline(_failing_output)).run(keep=True))

    def fail_close() -> None:
        raise RuntimeError("close failure")

    monkeypatch.setattr(run, "close", fail_close)
    with pytest.raises(RuntimeError, match="close failure") as exc_info:
        run.value("producer")

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert str(exc_info.value.__cause__) == "execution boom"


def test_terminal_writer_failure_is_retried_and_keeps_the_complete_exception_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    run = cast(Run, Deployment(_pipeline(_failing_output)).run(keep=True))
    original_finish = m_manifest.RunManifestWriter.finish
    calls = 0

    def flaky_finish(
        self: m_manifest.RunManifestWriter,
        *,
        status: str,
        error: str | None = None,
    ) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("terminal write boom")
        original_finish(self, status=status, error=error)

    monkeypatch.setattr(m_manifest.RunManifestWriter, "finish", flaky_finish)

    with pytest.raises(RuntimeError, match="failed node but no terminal run status") as exc_info:
        run.value("producer")

    terminal_write_error = exc_info.value.__cause__
    assert isinstance(terminal_write_error, OSError)
    assert str(terminal_write_error) == "terminal write boom"
    assert isinstance(terminal_write_error.__cause__, RuntimeError)
    assert str(terminal_write_error.__cause__) == "execution boom"
    manifest = _manifest(run)
    assert manifest["status"] == "failed"
    assert "execution boom" in manifest["error"]
    assert "execution boom" in _node_by_alias(manifest, "producer")["error"]
