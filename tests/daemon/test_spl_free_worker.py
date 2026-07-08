from __future__ import annotations

import ast
import logging
import os
import shlex
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.runtime_backend import RuntimeBackendRegistry, VenvBackend
from spl.daemon.server import DaemonRuntime
from spl.daemon.spl_free_generator import (
    LEGACY_WORKER_RUNTIME,
    REASON_ASYNC_FUNCTION,
    REASON_DECORATED_FUNCTION,
    REASON_IMPORTS_SPL,
    REASON_SUPPORTED_FUNCTION,
    SPL_FREE_WORKER_RUNTIME,
    build_flat_module_ast,
    filter_spl_runtime_scaffolding,
    prepare_worker_runtime,
    unsupported_stage1_reason,
)
from spl.daemon.spl_free_runner import (
    ARTIFACTS_KEY,
    collect_artifacts,
    validate_name as runner_validate_name,
)
from spl.daemon.storage_base import validate_name as daemon_validate_name
from spl.daemon.store import RegistryStore


FUNCTION_NO_SPL_YAML = """\
- !DFunction
  name: probe
  inputs: []
  outputs:
  - name: default
    type: dict
  body: |-
    import importlib.util
    import os
    return {
        "pythonpath": os.environ.get("PYTHONPATH"),
        "spl_found": importlib.util.find_spec("spl") is not None,
    }
"""


ASYNC_HELPER_YAML = """\
- !DFunction
  name: async_probe
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    import asyncio
    async def value():
        return 11
    return asyncio.run(value())
"""


DECORATED_HELPER_YAML = """\
- !DFunction
  name: decorated_probe
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    def passthrough(func):
        return func
    @passthrough
    def value():
        return 13
    return value()
"""


IMPORT_SPL_YAML = """\
- !DFunction
  name: imports_spl_probe
  inputs: []
  outputs:
  - name: default
    type: str
  body: |-
    import spl
    return spl.__name__
"""


USER_SPL_CORE_IMPORT_YAML = """\
- !DFunction
  name: user_spl_core_import_probe
  inputs: []
  outputs:
  - name: default
    type: str
  body: |-
    def value():
        from spl.core.entities.node import InputPort
        return InputPort.__name__
    return value()
"""


TYPE_HINTS_YAML = """\
- !DFunction
  name: type_hints_probe
  inputs: []
  outputs:
  - name: default
    type: str
  body: |-
    import typing
    global Box
    class Box:
        child: "Box"
    return typing.get_type_hints(Box)["child"].__name__
"""


class FakeEnvironmentManager:
    def __init__(self, python_path: str):
        self.record = {
            "status": "ready",
            "spec_hash": "fake-venv",
            "python_path": python_path,
        }

    def status_for_object(self, object_record: dict[str, Any]) -> dict[str, Any]:
        return self.record

    def ensure_ready(
        self,
        object_record: dict[str, Any],
        *,
        wait: bool,
        retry_failed: bool = False,
    ) -> dict[str, Any]:
        return self.record

    def build_spec(self, object_record: dict[str, Any]) -> dict[str, Any]:
        return self.record

    def rebuild(self, spec_hash: str, *, wait: bool) -> dict[str, Any]:
        return self.record


class SingleVenvRegistry(RuntimeBackendRegistry):
    def __init__(self, python_path: str):
        self.python_path = python_path

    def backend_for(self, object_record: dict[str, Any]) -> VenvBackend:
        return VenvBackend(FakeEnvironmentManager(self.python_path))


def _wait_for_run(store: RegistryStore, run_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        state = store.get_run(run_id)
        if state["status"] in {"succeeded", "failed"}:
            return state
        time.sleep(0.05)
    raise TimeoutError(f"run did not finish: {run_id}")


def _python_without_site_packages(tmp_path: Path) -> Path:
    wrapper = tmp_path / "python-no-site"
    wrapper.write_text(
        f'#!/bin/sh\nexec {shlex.quote(sys.executable)} -S "$@"\n',
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return wrapper


def _run_object(
    tmp_path: Path,
    yaml_text: str,
    *,
    name: str,
    entrypoint: str,
    isolated_python: bool = False,
    caplog: Any | None = None,
) -> dict[str, Any]:
    store = RegistryStore(tmp_path)
    python_path = _python_without_site_packages(tmp_path) if isolated_python else Path(sys.executable)
    runtime = DaemonRuntime(
        store,
        auto_build_envs=False,
        runtime_backends=SingleVenvRegistry(str(python_path)),
    )
    try:
        store.register_env("default", sys.executable)
        runtime.register_object(name, entrypoint, "default", yaml_text=yaml_text)
        if caplog is not None:
            caplog.set_level(logging.INFO, logger="spl.daemon.server")
        started = runtime.start_run(
            name,
            source="local",
            report_local_run=False,
            timeout_seconds=30,
        )
        final = _wait_for_run(store, started["id"])
        return final
    finally:
        runtime.shutdown()
        store.close()


def test_spl_free_runner_module_has_no_spl_imports() -> None:
    runner_path = Path(__file__).resolve().parents[2] / "src" / "spl" / "daemon" / "spl_free_runner.py"
    tree = ast.parse(runner_path.read_text(encoding="utf-8"))

    spl_imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            spl_imports.extend(
                alias.name for alias in node.names if alias.name == "spl" or alias.name.startswith("spl.")
            )
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            if node.module == "spl" or node.module.startswith("spl."):
                spl_imports.append(node.module)

    assert spl_imports == []


def test_validate_name_rejects_dot_only_names_and_allows_dotted_names() -> None:
    for validate in (daemon_validate_name, runner_validate_name):
        for name in (".", "..", "..."):
            with pytest.raises(ValueError, match="letters, digits, underscore, dash, and dot"):
                validate(name)
        assert validate("v1.2") == "v1.2"
        assert validate("a.b") == "a.b"


def test_collect_artifacts_rejects_parent_directory_name(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    source = source_dir / "payload.txt"
    source.write_text("payload\n", encoding="utf-8")

    with pytest.raises(ValueError, match="letters, digits, underscore, dash, and dot"):
        collect_artifacts({ARTIFACTS_KEY: {"..": source}}, tmp_path / "artifacts")

    assert not (tmp_path / "payload.txt").exists()


def test_ast_filter_removes_multiline_spl_import_but_keeps_literals_and_nested_functions() -> None:
    module = ast.parse(
        """
def _():
    from spl.core.entities.node import (
        InputPort,
        OutputPort,
    )
    from spl.core.entities.function import DFunction
    def probe():
        return 1
    setattr(probe, "__spl_location__", ("source.py", "probe"))
    setattr(probe, "__spl_metadata__", DFunction(name="probe", body="return 1", inputs=[], outputs=[]))
    literal = "from spl.core import not_code"
    def nested():
        return "spl.core stays in literals"
    setattr(nested, "__spl_metadata__", None)
    return nested, literal
"""
    )

    filtered = filter_spl_runtime_scaffolding(module)
    text = ast.unparse(filtered)
    spl_imports = [
        node
        for node in ast.walk(filtered)
        if isinstance(node, ast.ImportFrom) and node.module is not None and node.module.startswith("spl.core")
    ]

    assert spl_imports == []
    assert "spl.core stays in literals" in text
    assert "from spl.core import not_code" in text
    assert "def nested" in text
    assert "setattr(nested, '__spl_metadata__', None)" in text
    assert "__spl_location__" not in text
    assert "DFunction" not in text


def test_build_flat_module_filters_spl_scaffolding_from_yaml(tmp_path: Path) -> None:
    yaml_path = tmp_path / "object.yaml"
    yaml_path.write_text(FUNCTION_NO_SPL_YAML, encoding="utf-8")

    module = build_flat_module_ast(yaml_path)
    text = ast.unparse(module)

    assert "from spl.core" not in text
    assert "def probe" in text
    assert unsupported_stage1_reason(module) is None


def test_prepare_worker_runtime_preserves_existing_cache_and_writes_valid_module(tmp_path: Path) -> None:
    object_yaml_path = tmp_path / "object.yaml"
    object_yaml_path.write_text(FUNCTION_NO_SPL_YAML, encoding="utf-8")
    runner_source_path = tmp_path / "runner-source.py"
    runner_source_path.write_text("# runner\n", encoding="utf-8")
    generated_modules_dir = tmp_path / "generated-modules"

    existing_hash = "b" * 64
    existing_module = generated_modules_dir / "v1" / existing_hash / "object_module.py"
    existing_module.parent.mkdir(parents=True)
    existing_module.write_text("EXISTING = True\n", encoding="utf-8")
    os.utime(existing_module, ns=(1_000_000_000, 1_000_000_000))
    existing_mtime = existing_module.stat().st_mtime_ns
    existing_run_dir = tmp_path / "existing-run"
    existing_run_dir.mkdir()

    prepare_worker_runtime(
        object_record={"kind": "function", "content_hash": existing_hash},
        object_yaml_path=object_yaml_path,
        entrypoint="probe",
        run_dir=existing_run_dir,
        generated_modules_dir=generated_modules_dir,
        runner_source_path=runner_source_path,
        marker_path=existing_run_dir / "worker-runtime.json",
    )

    assert existing_module.read_text(encoding="utf-8") == "EXISTING = True\n"
    assert existing_module.stat().st_mtime_ns == existing_mtime

    new_hash = "c" * 64
    new_module = generated_modules_dir / "v1" / new_hash / "object_module.py"
    new_run_dir = tmp_path / "new-run"
    new_run_dir.mkdir()

    prepare_worker_runtime(
        object_record={"kind": "function", "content_hash": new_hash},
        object_yaml_path=object_yaml_path,
        entrypoint="probe",
        run_dir=new_run_dir,
        generated_modules_dir=generated_modules_dir,
        runner_source_path=runner_source_path,
        marker_path=new_run_dir / "worker-runtime.json",
    )

    assert new_module.exists()
    ast.parse(new_module.read_text(encoding="utf-8"))


def test_unsupported_detector_finds_async_decorated_and_spl_imports(tmp_path: Path) -> None:
    async_yaml = tmp_path / "async.yaml"
    async_yaml.write_text(ASYNC_HELPER_YAML, encoding="utf-8")
    decorated_yaml = tmp_path / "decorated.yaml"
    decorated_yaml.write_text(DECORATED_HELPER_YAML, encoding="utf-8")
    import_spl_yaml = tmp_path / "import-spl.yaml"
    import_spl_yaml.write_text(IMPORT_SPL_YAML, encoding="utf-8")
    user_spl_core_import_yaml = tmp_path / "user-spl-core-import.yaml"
    user_spl_core_import_yaml.write_text(USER_SPL_CORE_IMPORT_YAML, encoding="utf-8")

    assert unsupported_stage1_reason(build_flat_module_ast(async_yaml)) == REASON_ASYNC_FUNCTION
    assert unsupported_stage1_reason(build_flat_module_ast(decorated_yaml)) == REASON_DECORATED_FUNCTION
    assert unsupported_stage1_reason(build_flat_module_ast(import_spl_yaml)) == REASON_IMPORTS_SPL
    assert unsupported_stage1_reason(build_flat_module_ast(user_spl_core_import_yaml)) == REASON_IMPORTS_SPL


def test_functional_node_runs_without_spl_installed_or_pythonpath(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.delenv("PYTHONPATH", raising=False)

    final = _run_object(
        tmp_path,
        FUNCTION_NO_SPL_YAML,
        name="probe",
        entrypoint="probe",
        isolated_python=True,
    )

    assert final["status"] == "succeeded", final.get("error")
    assert final["worker_runtime"] == SPL_FREE_WORKER_RUNTIME
    assert final["worker_runtime_reason"] == REASON_SUPPORTED_FUNCTION
    assert "spl_free_runner.py" in final["command"][1]
    assert final["result"]["result"] == {
        "pythonpath": None,
        "spl_found": False,
    }


def test_spl_free_runner_registers_generated_module_for_type_hints(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.delenv("PYTHONPATH", raising=False)

    final = _run_object(
        tmp_path,
        TYPE_HINTS_YAML,
        name="type_hints_probe",
        entrypoint="type_hints_probe",
        isolated_python=True,
    )

    assert final["status"] == "succeeded", final.get("error")
    assert final["worker_runtime"] == SPL_FREE_WORKER_RUNTIME
    assert final["worker_runtime_reason"] == REASON_SUPPORTED_FUNCTION
    assert "spl_free_runner.py" in final["command"][1]
    assert final["result"]["result"] == "Box"


def test_async_function_uses_legacy_worker_with_marker(tmp_path: Path, caplog: Any) -> None:
    final = _run_object(
        tmp_path,
        ASYNC_HELPER_YAML,
        name="async_probe",
        entrypoint="async_probe",
        caplog=caplog,
    )

    assert final["status"] == "succeeded", final.get("error")
    assert final["result"]["result"] == 11
    assert final["worker_runtime"] == LEGACY_WORKER_RUNTIME
    assert final["worker_runtime_reason"] == REASON_ASYNC_FUNCTION
    assert "worker.py" in final["command"][1]
    assert any(
        getattr(record, "spl_event", None) == "worker_runtime"
        and record.worker_runtime["worker_runtime"] == LEGACY_WORKER_RUNTIME
        and record.worker_runtime["worker_runtime_reason"] == REASON_ASYNC_FUNCTION
        for record in caplog.records
    )


def test_decorated_function_uses_legacy_worker_with_marker(tmp_path: Path, caplog: Any) -> None:
    final = _run_object(
        tmp_path,
        DECORATED_HELPER_YAML,
        name="decorated_probe",
        entrypoint="decorated_probe",
        caplog=caplog,
    )

    assert final["status"] == "succeeded", final.get("error")
    assert final["result"]["result"] == 13
    assert final["worker_runtime"] == LEGACY_WORKER_RUNTIME
    assert final["worker_runtime_reason"] == REASON_DECORATED_FUNCTION
    assert "worker.py" in final["command"][1]
    assert any(
        getattr(record, "spl_event", None) == "worker_runtime"
        and record.worker_runtime["worker_runtime"] == LEGACY_WORKER_RUNTIME
        and record.worker_runtime["worker_runtime_reason"] == REASON_DECORATED_FUNCTION
        for record in caplog.records
    )


def test_function_with_import_spl_uses_legacy_worker_with_marker(tmp_path: Path, caplog: Any) -> None:
    final = _run_object(
        tmp_path,
        IMPORT_SPL_YAML,
        name="imports_spl_probe",
        entrypoint="imports_spl_probe",
        caplog=caplog,
    )

    assert final["status"] == "succeeded", final.get("error")
    assert final["result"]["result"] == "spl"
    assert final["worker_runtime"] == LEGACY_WORKER_RUNTIME
    assert final["worker_runtime_reason"] == REASON_IMPORTS_SPL
    assert "worker.py" in final["command"][1]
    assert any(
        getattr(record, "spl_event", None) == "worker_runtime"
        and record.worker_runtime["worker_runtime"] == LEGACY_WORKER_RUNTIME
        and record.worker_runtime["worker_runtime_reason"] == REASON_IMPORTS_SPL
        for record in caplog.records
    )


def test_user_spl_core_import_in_nested_function_uses_legacy_worker(tmp_path: Path, caplog: Any) -> None:
    yaml_path = tmp_path / "user-spl-core-import.yaml"
    yaml_path.write_text(USER_SPL_CORE_IMPORT_YAML, encoding="utf-8")
    module = build_flat_module_ast(yaml_path)
    text = ast.unparse(module)

    assert "from spl.core.entities.node import InputPort" in text
    assert unsupported_stage1_reason(module) == REASON_IMPORTS_SPL

    final = _run_object(
        tmp_path,
        USER_SPL_CORE_IMPORT_YAML,
        name="user_spl_core_import_probe",
        entrypoint="user_spl_core_import_probe",
        caplog=caplog,
    )

    assert final["status"] == "succeeded", final.get("error")
    assert final["result"]["result"] == "InputPort"
    assert final["worker_runtime"] == LEGACY_WORKER_RUNTIME
    assert final["worker_runtime_reason"] == REASON_IMPORTS_SPL
    assert "worker.py" in final["command"][1]
