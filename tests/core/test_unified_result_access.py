from __future__ import annotations

from typing import TYPE_CHECKING, cast

from spl.client import RemoteResult, SPLClient
from spl.core.common import Deployment, lift
from spl.core.entities.function import METADATA_DUNDER_NAME, DFunction
from spl.core.entities.node import DEFAULT_PORT, InputPort, OutputPort

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from spl.core.entities.pipeline import Pipeline


def _assert_equal(actual: Any, expected: Any) -> None:
    if actual != expected:
        raise AssertionError(f"{actual!r} != {expected!r}")


def _assert_is(actual: object, expected: object) -> None:
    if actual is not expected:
        raise AssertionError(f"{actual!r} is not {expected!r}")


def _assert_contains(needle: str, haystack: str) -> None:
    if needle not in haystack:
        raise AssertionError(f"{needle!r} not found in {haystack!r}")


def _attach_metadata(
    func: Callable[..., Any],
    *,
    inputs: list[InputPort] | None = None,
    outputs: list[OutputPort] | None = None,
) -> None:
    setattr(
        func,
        METADATA_DUNDER_NAME,
        DFunction(
            name=func.__name__,
            body="return None",
            inputs=inputs or [],
            outputs=outputs or [OutputPort(DEFAULT_PORT, "Any")],
        ),
    )


class _CallDaemon:
    def __init__(self, result: dict[str, Any]) -> None:
        self._result = result

    def run(self, object_name: str, **kwargs: Any) -> dict[str, Any]:
        return {"id": object_name, "status": "queued"}

    def wait_run(
        self,
        run_id: str,
        *,
        poll_interval: float,
        timeout_seconds: float | None,
    ) -> dict[str, Any]:
        return {"id": run_id, "status": "succeeded"}

    def result(self, run_id: str) -> dict[str, Any]:
        return {"result": self._result, "artifacts": {}}


def test_client_call_output_matches_default_value_path() -> None:
    client = SPLClient(daemon_port=8765)
    client._daemon = cast("Any", _CallDaemon({DEFAULT_PORT: "plain value"}))

    result = client.call("bare_function", kwargs={})

    _assert_equal(result.value[DEFAULT_PORT], "plain value")
    _assert_equal(result.output, result.value[DEFAULT_PORT])


def test_remote_result_output_unwraps_single_non_default_result() -> None:
    result = RemoteResult(
        run={"id": "run-1", "status": "succeeded"},
        payload={"result": {"score": 0.91}, "artifacts": {}},
    )

    _assert_equal(result.value, {"score": 0.91})
    _assert_equal(result.output, 0.91)
    _assert_contains("0.91", repr(result))
    _assert_contains("<table", result._repr_html_())


def test_remote_result_output_preserves_multi_output_result() -> None:
    payload = {"left": 1, "right": 2}
    result = RemoteResult(
        run={"id": "run-1", "status": "succeeded"},
        payload={"result": payload, "artifacts": {}},
    )

    _assert_is(result.output, payload)


def test_deployment_run_output_matches_named_alias_default_port() -> None:
    def add_one(x: int) -> int:
        return x + 1

    def double(y: int) -> int:
        return y * 2

    _attach_metadata(
        add_one,
        inputs=[InputPort("x", "int", None)],
        outputs=[OutputPort(DEFAULT_PORT, "int")],
    )
    _attach_metadata(
        double,
        inputs=[InputPort("y", "int", None)],
        outputs=[OutputPort(DEFAULT_PORT, "int")],
    )
    lift_any = cast("Any", lift)
    first = lift_any(add_one).bind(x=20)
    pipeline = cast("Pipeline", lift_any(double).bind(y=first).alias("answer").render("pipe"))
    node = pipeline.aliases["answer"]
    deployment = cast("Any", Deployment)

    old_value = deployment(None, pipeline).run()[node][DEFAULT_PORT]

    _assert_equal(deployment(None, pipeline).run(output="answer"), old_value)
    with deployment(None, pipeline).run() as run:
        _assert_equal(run.value("answer"), old_value)


def test_run_value_defaults_to_bare_single_node_output() -> None:
    def identity() -> str:
        return "bare"

    _attach_metadata(identity, outputs=[OutputPort(DEFAULT_PORT, "str")])
    lift_any = cast("Any", lift)
    pipeline = cast("Pipeline", lift_any(identity).render("identity"))
    node = next(iter(pipeline.nodes))
    deployment = cast("Any", Deployment)

    with deployment(None, pipeline).run() as run:
        old_value = run[node][DEFAULT_PORT]

    with deployment(None, pipeline).run() as run:
        _assert_equal(run.value(), old_value)
