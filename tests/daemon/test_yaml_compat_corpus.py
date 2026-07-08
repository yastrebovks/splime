from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.runtime_backend import RuntimeBackendRegistry, VenvBackend
from spl.daemon.server import DaemonRuntime
from spl.daemon.spl_free_generator import LEGACY_WORKER_RUNTIME, SPL_FREE_WORKER_RUNTIME
from spl.daemon.store import RegistryStore

CORPUS_ROOT = Path(__file__).resolve().parents[1] / "compat" / "corpus" / "v02x"

RUN_CASES = {
    "functional_node.yaml": {
        "name": "compat_constant",
        "entrypoint": "compat_constant",
        "output": None,
        "expected_result": 41,
        "worker_runtime": SPL_FREE_WORKER_RUNTIME,
    },
    "scalar_pipeline.yaml": {
        "name": "scalar_pipeline",
        "entrypoint": "scalar_pipeline",
        "output": "sum",
        "expected_result": {"default": 7},
        "worker_runtime": LEGACY_WORKER_RUNTIME,
    },
    "adapter_alias_pipeline.yaml": {
        "name": "adapter_pipeline",
        "entrypoint": "adapter_pipeline",
        "output": "result",
        "expected_result": {"default": "loaded:hello|consumed"},
        "worker_runtime": LEGACY_WORKER_RUNTIME,
    },
    "multinode_dag.yaml": {
        "name": "multinode_dag",
        "entrypoint": "multinode_dag",
        "output": "total",
        "expected_result": {"default": 12},
        "worker_runtime": LEGACY_WORKER_RUNTIME,
    },
}


class _ReadyEnvironmentManager:
    def __init__(self, python_path: Path):
        self.record = {
            "spec_hash": "yaml-compat-corpus-env",
            "python_path": str(python_path),
            "spec": {},
        }

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        return self.record

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        return self.record


class _SingleVenvRegistry(RuntimeBackendRegistry):
    def __init__(self, python_path: Path):
        self.python_path = python_path

    def backend_for(self, object_record: dict[str, Any]) -> VenvBackend:
        return VenvBackend(_ReadyEnvironmentManager(self.python_path))


def _wait_for_run(store: RegistryStore, run_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        state = store.get_run(run_id)
        if state["status"] in {"succeeded", "failed"}:
            return state
        time.sleep(0.05)
    raise TimeoutError("run did not finish: {}".format(run_id))


@pytest.mark.parametrize("filename", sorted(RUN_CASES))
def test_v02x_corpus_registers_and_runs_through_daemon(tmp_path: Path, filename: str) -> None:
    case = RUN_CASES[filename]
    store = RegistryStore(tmp_path)
    runtime = DaemonRuntime(
        store,
        auto_build_envs=False,
        runtime_backends=_SingleVenvRegistry(Path(sys.executable)),
    )
    try:
        store.register_env("default", sys.executable)
        runtime.register_object(
            str(case["name"]),
            str(case["entrypoint"]),
            "default",
            yaml_text=(CORPUS_ROOT / filename).read_text(encoding="utf-8"),
        )
        started = runtime.start_run(
            str(case["name"]),
            output=case["output"],
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])
    finally:
        runtime.shutdown()
        store.close()

    assert final["status"] == "succeeded", final.get("error")
    assert final["result"]["result"] == case["expected_result"]
    assert final["worker_runtime"] == case["worker_runtime"]
