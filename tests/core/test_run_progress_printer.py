"""Client-side run progress feedback (Stage 2.2 / TODO #34).

`RunProgressPrinter` must stay silent for fast runs, speak up during
environment builds and long waits, throttle repeats, and never raise.
The wait loops must expose an ``on_state`` hook, and the SDK ``progress``
option must map onto it.
"""

from __future__ import annotations

import io
from types import SimpleNamespace
from typing import Any

import pytest

from spl._client import RemoteRun, _progress_callback
from spl.daemon_client import Client, RunProgressPrinter, _format_duration


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_printer(clock: FakeClock) -> tuple[RunProgressPrinter, io.StringIO]:
    stream = io.StringIO()
    printer = RunProgressPrinter(stream=stream, interval_seconds=5.0, clock=clock)
    return printer, stream


def _building_state(**environment: Any) -> dict[str, Any]:
    payload = {
        "status": "preparing_environment",
        "environment": {
            "status": "creating",
            "runtime_type": "venv",
            "elapsed_seconds": 3.0,
            **environment,
        },
    }
    return payload


class TestRunProgressPrinter:
    def test_silent_for_fast_run(self) -> None:
        clock = FakeClock()
        printer, stream = make_printer(clock)

        for status in ("queued", "starting", "preparing_environment", "running"):
            printer({"status": status})
            clock.advance(0.3)
        printer({"status": "succeeded"})

        assert stream.getvalue() == ""

    def test_announces_build_immediately_and_throttles(self) -> None:
        clock = FakeClock()
        printer, stream = make_printer(clock)

        printer(_building_state(log_tail="Creating venv"))
        clock.advance(1.0)
        printer(_building_state(log_tail="Collecting numpy==2.1.0"))

        lines = stream.getvalue().splitlines()
        assert len(lines) == 1
        assert "building the venv environment" in lines[0]
        assert "Creating venv" in lines[0]

    def test_repeats_after_interval_with_fresh_log_tail(self) -> None:
        clock = FakeClock()
        printer, stream = make_printer(clock)

        printer(_building_state(log_tail="Creating venv"))
        clock.advance(6.0)
        printer(_building_state(log_tail="Collecting numpy==2.1.0"))

        lines = stream.getvalue().splitlines()
        assert len(lines) == 2
        assert "Collecting numpy==2.1.0" in lines[1]

    def test_announces_ready_once_when_run_starts(self) -> None:
        clock = FakeClock()
        printer, stream = make_printer(clock)

        printer(_building_state())
        clock.advance(1.0)
        printer({"status": "running"})
        printer({"status": "running"})
        printer({"status": "succeeded"})

        lines = stream.getvalue().splitlines()
        assert len(lines) == 2
        assert "environment is ready; running" in lines[1]

    def test_no_ready_line_without_announced_build(self) -> None:
        clock = FakeClock()
        printer, stream = make_printer(clock)

        printer({"status": "preparing_environment"})
        printer({"status": "running"})
        printer({"status": "succeeded"})

        assert stream.getvalue() == ""

    def test_slow_waiting_phase_reports_after_interval(self) -> None:
        clock = FakeClock()
        printer, stream = make_printer(clock)

        printer({"status": "queued"})
        clock.advance(4.0)
        printer({"status": "queued"})
        assert stream.getvalue() == ""

        clock.advance(2.0)
        printer({"status": "queued"})
        lines = stream.getvalue().splitlines()
        assert len(lines) == 1
        assert "run is still queued after 6s" in lines[0]

    def test_uses_local_phase_timer_without_daemon_elapsed(self) -> None:
        clock = FakeClock()
        printer, stream = make_printer(clock)

        printer(_building_state(elapsed_seconds=None))
        assert "(0s" in stream.getvalue()

    def test_never_raises_for_hostile_state_or_stream(self) -> None:
        class ExplodingStream(io.StringIO):
            def write(self, value: str) -> int:
                raise OSError("broken pipe")

        clock = FakeClock()
        printer = RunProgressPrinter(
            stream=ExplodingStream(),
            interval_seconds=5.0,
            clock=clock,
        )

        printer(_building_state())
        printer({"status": None, "environment": "garbage"})
        printer({})

    def test_rejects_non_positive_interval(self) -> None:
        with pytest.raises(ValueError):
            RunProgressPrinter(interval_seconds=0.0)

    def test_format_duration(self) -> None:
        assert _format_duration(-3.0) == "0s"
        assert _format_duration(45.0) == "45s"
        assert _format_duration(75.0) == "1m 15s"
        assert _format_duration(3700.0) == "1h 01m"


class SequenceClient(Client):
    """A Client whose run states come from a canned sequence (no network)."""

    def __init__(self, states: list[dict[str, Any]]):
        self.base_url = "http://test.invalid"
        self.api_token = None
        self._states = iter(states)

    def get_run(self, run_id: str) -> dict[str, Any]:
        return next(self._states)

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        return next(self._states)


class TestWaitLoopCallback:
    def test_wait_run_invokes_callback_for_every_state(self) -> None:
        states = [
            {"status": "preparing_environment"},
            {"status": "running"},
            {"status": "succeeded"},
        ]
        seen: list[str] = []

        final = SequenceClient(states).wait_run(
            "run-1",
            poll_interval=0.0,
            on_state=lambda state: seen.append(state["status"]),
        )

        assert final["status"] == "succeeded"
        assert seen == ["preparing_environment", "running", "succeeded"]

    def test_wait_remote_run_invokes_callback(self) -> None:
        states = [{"status": "queued"}, {"status": "succeeded"}]
        seen: list[str] = []

        final = SequenceClient(states).wait_remote_run(
            "run-1",
            poll_interval=0.0,
            on_state=lambda state: seen.append(state["status"]),
        )

        assert final["status"] == "succeeded"
        assert seen == ["queued", "succeeded"]

    def test_callback_exception_aborts_wait(self) -> None:
        def explode(state: dict[str, Any]) -> None:
            raise RuntimeError("observer failed")

        with pytest.raises(RuntimeError, match="observer failed"):
            SequenceClient([{"status": "running"}]).wait_run(
                "run-1",
                poll_interval=0.0,
                on_state=explode,
            )


class RecordingDaemon:
    def __init__(self, final_state: dict[str, Any]):
        self.final_state = final_state
        self.wait_run_kwargs: dict[str, Any] | None = None

    def wait_run(self, run_id: str, **kwargs: Any) -> dict[str, Any]:
        self.wait_run_kwargs = kwargs
        return self.final_state


class TestProgressOption:
    def test_progress_callback_mapping(self) -> None:
        assert _progress_callback(False) is None

        printer = _progress_callback(True)
        assert isinstance(printer, RunProgressPrinter)

        def observer(state: dict[str, Any]) -> None:
            pass

        assert _progress_callback(observer) is observer

    def test_remote_run_wait_passes_progress_to_daemon(self) -> None:
        daemon = RecordingDaemon({"status": "succeeded", "id": "run-1"})
        client = SimpleNamespace(_daemon=daemon)
        run = RemoteRun(client, {"id": "run-1", "status": "queued"})

        run.wait(progress=False)
        assert daemon.wait_run_kwargs is not None
        assert daemon.wait_run_kwargs["on_state"] is None

        def observer(state: dict[str, Any]) -> None:
            pass

        run.wait(progress=observer)
        assert daemon.wait_run_kwargs["on_state"] is observer

        run.wait()
        assert isinstance(daemon.wait_run_kwargs["on_state"], RunProgressPrinter)
