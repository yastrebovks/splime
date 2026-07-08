from __future__ import annotations

import argparse
import gc
import json
import platform
import random
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from spl.core._common import Run
from spl.core.entities.pipeline import Pipeline

SEED = 4017
DEFAULT_ITERATIONS = 300_000
DEFAULT_SAMPLES = 9


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    value: Any


def _unused_run_callback(**kwargs: Any) -> dict[str, Any]:
    del kwargs
    return {}


def _cases() -> list[BenchmarkCase]:
    rng = random.Random(SEED)
    medium_dict = {
        "row-{}".format(index): {
            "score": rng.randint(0, 10_000),
            "enabled": bool(index % 2),
            "labels": ["label-{}".format(index % 7), "label-{}".format(index % 11)],
        }
        for index in range(64)
    }
    large_list = [rng.randint(0, 1_000_000) for _ in range(16_384)]
    return [
        BenchmarkCase("scalar-int", 42),
        BenchmarkCase("scalar-string", "json-native-shortcut"),
        BenchmarkCase("medium-dict", medium_dict),
        BenchmarkCase("large-list", large_list),
    ]


def _current_shortcut(run: Run, value: Any) -> Any:
    return run._round_trip_artifact(value)


def _folded_json_adapter(run: Run, value: Any) -> Any:
    return run._round_trip_resolved(value, None, None, None)


def _sample_ns_per_op(func: Callable[[Run, Any], Any], value: Any, iterations: int) -> float:
    run = Run(_unused_run_callback, Pipeline(), keep=False)
    started = time.perf_counter_ns()
    for _ in range(iterations):
        func(run, value)
    elapsed = time.perf_counter_ns() - started
    if run._artifacts_dir is not None or run._artifact_refs or run._adapter_resolutions:
        raise AssertionError("JSON-native benchmark path materialized artifacts or recorded adapters")
    return elapsed / iterations


def _measure(func: Callable[[Run, Any], Any], value: Any, *, iterations: int, samples: int) -> dict[str, Any]:
    timings = [_sample_ns_per_op(func, value, iterations) for _ in range(samples)]
    return {
        "median_ns_per_op": statistics.median(timings),
        "min_ns_per_op": min(timings),
        "max_ns_per_op": max(timings),
        "samples_ns_per_op": timings,
    }


def run_benchmark(*, iterations: int, samples: int) -> dict[str, Any]:
    """Return current-vs-folded timings for JSON-native round trips."""

    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        cases = []
        for case in _cases():
            current = _measure(_current_shortcut, case.value, iterations=iterations, samples=samples)
            folded = _measure(_folded_json_adapter, case.value, iterations=iterations, samples=samples)
            cases.append(
                {
                    "case": case.name,
                    "current": current,
                    "folded": folded,
                    "folded_over_current": folded["median_ns_per_op"] / current["median_ns_per_op"],
                }
            )
    finally:
        if gc_was_enabled:
            gc.enable()

    return {
        "benchmark": "json-native-shortcircuit",
        "seed": SEED,
        "iterations_per_sample": iterations,
        "samples_per_variant": samples,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cases": cases,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark JSON-native Run._round_trip_artifact paths.")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES)
    args = parser.parse_args()
    if args.iterations <= 0:
        raise SystemExit("--iterations must be positive")
    if args.samples <= 0:
        raise SystemExit("--samples must be positive")
    print(json.dumps(run_benchmark(iterations=args.iterations, samples=args.samples), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
