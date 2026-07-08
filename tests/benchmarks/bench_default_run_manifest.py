from __future__ import annotations

import argparse
import gc
import json
import os
import platform
import random
import shutil
import statistics
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from spl import Deployment, lift
from spl.core import manifest as m_manifest
from spl.core.entities.pipeline import Pipeline

SEED = 4017
DEFAULT_ITERATIONS = 120
DEFAULT_SAMPLES = 7


@dataclass(frozen=True)
class BenchPayload:
    value: int


def _single_sum(left: int, right: int) -> int:
    return left + right


def _seed_value(value: int) -> int:
    return value


def _triple(value: int) -> int:
    return value * 3


def _add_offset(value: int, offset: int) -> int:
    return value + offset


def _make_payload(seed: int) -> BenchPayload:
    return BenchPayload(seed)


def _payload_to_int(payload: BenchPayload) -> int:
    return payload.value * 2


def _save_payload(path: str, payload: BenchPayload) -> None:
    Path(path).write_text(str(payload.value), encoding="utf-8")


def _load_payload(path: str) -> BenchPayload:
    return BenchPayload(int(Path(path).read_text(encoding="utf-8")))


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    pipeline: Pipeline
    output_alias: str
    kwargs: dict[str, Any]


def _cases() -> list[BenchmarkCase]:
    rng = random.Random(SEED)
    lift_any = cast(Any, lift)

    single = lift_any(_single_sum).alias("sum").render("bench_single_json")

    seed_node = lift_any(_seed_value).alias("seed")
    triple_node = lift_any(_triple).bind(value=seed_node).alias("triple")
    dag_json = lift_any(_add_offset).bind(value=triple_node).alias("result").render("bench_dag_json")

    payload_node = lift_any(_make_payload).alias("payload")
    payload_value_node = (
        lift_any(_payload_to_int).bind(payload=payload_node.as_format("bench-payload")).alias("payload_value")
    )
    dag_artifact = (
        lift_any(_add_offset)
        .bind(value=payload_value_node)
        .alias("result")
        .render("bench_dag_artifact")
        .add_adapter(BenchPayload, "bench-payload", save=_save_payload, load=_load_payload)
    )

    return [
        BenchmarkCase(
            name="single-json",
            pipeline=single,
            output_alias="sum",
            kwargs={"left": rng.randint(1, 100), "right": rng.randint(1, 100)},
        ),
        BenchmarkCase(
            name="dag-json",
            pipeline=dag_json,
            output_alias="result",
            kwargs={"value": rng.randint(1, 100), "offset": rng.randint(1, 100)},
        ),
        BenchmarkCase(
            name="dag-artifact",
            pipeline=dag_artifact,
            output_alias="result",
            kwargs={"seed": rng.randint(1, 100), "offset": rng.randint(1, 100)},
        ),
    ]


def _run_once(case: BenchmarkCase, keep: m_manifest.KeepPolicy) -> Any:
    return Deployment(case.pipeline).run(output=case.output_alias, keep=keep, **case.kwargs)


def _expected(case: BenchmarkCase) -> int:
    if case.name == "single-json":
        return int(case.kwargs["left"]) + int(case.kwargs["right"])
    if case.name == "dag-json":
        return int(case.kwargs["value"]) * 3 + int(case.kwargs["offset"])
    if case.name == "dag-artifact":
        return int(case.kwargs["seed"]) * 2 + int(case.kwargs["offset"])
    raise AssertionError("unknown benchmark case: {}".format(case.name))


def _sample_us_per_run(
    case: BenchmarkCase,
    keep: m_manifest.KeepPolicy,
    iterations: int,
    runs_home: Path,
) -> float:
    if runs_home.exists():
        shutil.rmtree(runs_home)
    os.environ["SPL_RUNS_HOME"] = str(runs_home)
    expected = _expected(case)
    started = time.perf_counter_ns()
    for _ in range(iterations):
        result = _run_once(case, keep)
        if result != expected:
            raise AssertionError("unexpected benchmark result for {}: {}".format(case.name, result))
    elapsed = time.perf_counter_ns() - started
    return elapsed / iterations / 1_000


def _measure_case_mode(
    case: BenchmarkCase,
    keep: m_manifest.KeepPolicy,
    *,
    iterations: int,
    samples: int,
    root: Path,
) -> dict[str, Any]:
    timings = []
    for sample_index in range(samples):
        sample_home = root / "{}-{}-{}".format(case.name, _keep_name(keep), sample_index)
        timings.append(_sample_us_per_run(case, keep, iterations, sample_home))
        shutil.rmtree(sample_home, ignore_errors=True)
    return {
        "median_us_per_run": statistics.median(timings),
        "min_us_per_run": min(timings),
        "max_us_per_run": max(timings),
        "samples_us_per_run": timings,
    }


def _keep_name(keep: m_manifest.KeepPolicy) -> str:
    if keep is False:
        return "keep_false"
    if keep is True:
        return "keep_true"
    return "on_failure"


def run_benchmark(*, iterations: int, samples: int) -> dict[str, Any]:
    """Return full Deployment.run timings for keep modes."""

    old_runs_home = os.environ.get("SPL_RUNS_HOME")
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        cases = []
        with tempfile.TemporaryDirectory(prefix="spl-run-manifest-bench-") as tmp:
            root = Path(tmp)
            for case in _cases():
                mode_rows = {}
                for keep in (False, "on_failure", True):
                    mode_rows[_keep_name(keep)] = _measure_case_mode(
                        case,
                        keep,
                        iterations=iterations,
                        samples=samples,
                        root=root,
                    )
                baseline = mode_rows["keep_false"]["median_us_per_run"]
                for row in mode_rows.values():
                    row["ratio_to_keep_false"] = row["median_us_per_run"] / baseline
                    row["delta_us_vs_keep_false"] = row["median_us_per_run"] - baseline
                cases.append({"case": case.name, "modes": mode_rows})
    finally:
        if old_runs_home is None:
            os.environ.pop("SPL_RUNS_HOME", None)
        else:
            os.environ["SPL_RUNS_HOME"] = old_runs_home
        if gc_was_enabled:
            gc.enable()

    return {
        "benchmark": "default-run-manifest",
        "seed": SEED,
        "iterations_per_sample": iterations,
        "samples_per_mode": samples,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "decision_threshold": {
            "case": "dag-json",
            "on_failure_delta_us": 2_000,
            "on_failure_ratio": 1.5,
        },
        "cases": cases,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Deployment.run keep-mode manifest overhead.")
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
