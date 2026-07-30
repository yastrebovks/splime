"""Regression gate for the integrated daemon's optional-client boundary.

The local framework/daemon is the runtime product.  Console and notebook
integrations may consume it, but they must never become imports, runtime
dependencies, or startup prerequisites.
"""

from __future__ import annotations

import ast
import asyncio
import socket
import sys
import threading
import time
import tomllib
from pathlib import Path
from typing import Any

from hypercorn.asyncio import serve as hypercorn_serve
from hypercorn.config import Config

from spl import SPLClient
from spl.daemon.doctor import run_doctor
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore


FUNCTION_YAML = """\
- !DFunction
  name: optionality_value
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 46
"""

PIPELINE_YAML = """\
- !DPipeline
  name: optionality_pipeline
  nodes:
  - !DNodeFunction
    uuid: 11111111-1111-4111-8111-111111111111
    func: optionality_value
  links: []
  aliases:
  - - result
    - 11111111-1111-4111-8111-111111111111
---
- !DFunction
  name: optionality_value
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 46
"""

FORBIDDEN_RUNTIME_IMPORT_PREFIXES = (
    "spl_frontend",
    "spl_plugin",
    "splime_jupyter",
)
FORBIDDEN_RUNTIME_DISTRIBUTIONS = {
    "spl-frontend",
    "spl-plugin",
    "splime-jupyter",
}


def test_framework_and_daemon_have_no_optional_client_runtime_dependency() -> None:
    """Keep Console/Jupyter/plugin code outside the framework dependency graph."""

    project_root = Path(__file__).resolve().parents[2]
    pyproject = tomllib.loads((project_root / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = {
        item.split("[", 1)[0].split("=", 1)[0].strip().casefold() for item in pyproject["project"]["dependencies"]
    }
    assert dependencies.isdisjoint(FORBIDDEN_RUNTIME_DISTRIBUTIONS)

    violations: list[str] = []
    for path in sorted((project_root / "src" / "spl").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            imported: str | None = None
            line = getattr(node, "lineno", 0)
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(FORBIDDEN_RUNTIME_IMPORT_PREFIXES):
                        violations.append(f"{path.relative_to(project_root)}:{line}:{alias.name}")
            elif isinstance(node, ast.ImportFrom):
                imported = node.module
            if imported and imported.startswith(FORBIDDEN_RUNTIME_IMPORT_PREFIXES):
                violations.append(f"{path.relative_to(project_root)}:{line}:{imported}")
    assert violations == []


def test_integrated_daemon_local_journey_needs_no_optional_client_or_server(
    tmp_path: Path,
) -> None:
    """Exercise the useful local journey through the real HTTP daemon."""

    store = RegistryStore(tmp_path)
    app = create_app(store, auto_build_envs=False)
    port = _free_loopback_port()
    stop, thread, errors = _serve(app, port)
    client = SPLClient(
        f"http://127.0.0.1:{port}",
        api_token=app.api_token,
    )
    try:
        health = client.health()
        assert health["ok"] is True
        assert health["server"]["connected"] is False
        assert health["server"]["identity_present"] is False
        assert health["server"]["offline"] is False

        client.register_env("default", sys.executable)
        published = client.publish_yaml(
            FUNCTION_YAML,
            name="optionality_value",
            entrypoint="optionality_value",
            local_only=True,
        )
        assert published.name == "optionality_value"
        local_objects = client._daemon.list_objects()  # noqa: SLF001 - exact daemon contract gate.
        local_records = list(local_objects.values()) if isinstance(local_objects, dict) else local_objects
        assert any(item["name"] == published.name for item in local_records)
        assert "optionality_value v1" in client.describe("optionality_value")
        versions = client._daemon.object_versions("optionality_value")  # noqa: SLF001 - exact daemon contract gate.
        assert [item["version"] for item in versions] == [1]

        result = client.call(
            "optionality_value",
            source="local",
            progress=False,
            timeout_seconds=20,
        )
        assert result.mode == "local"
        assert result.output == 46

        client.publish_yaml(
            PIPELINE_YAML,
            name="optionality_pipeline",
            entrypoint="optionality_pipeline",
            local_only=True,
        )
        decomposition = client.decomposition("optionality_pipeline")
        assert len(decomposition["nodes"]) == 1
        graph = client.draw_pipeline("optionality_pipeline")
        assert "optionality_pipeline" in graph.html

        doctor = run_doctor(
            client._daemon,  # noqa: SLF001 - doctor accepts the daemon HTTP client.
            home=tmp_path,
        )
        assert doctor.checks
        assert any(check.name == "daemon" for check in doctor.checks)

        # Additive release/build fields may appear in health without changing a
        # 0.4.x reader's use of the stable fields.
        legacy_projection = {key: health[key] for key in ("ok", "server")}
        assert legacy_projection["ok"] is True
        assert isinstance(legacy_projection["server"], dict)
    finally:
        stop.set()
        thread.join(timeout=5)
        app.runtime.shutdown()
        store.close()
    assert not thread.is_alive()
    assert errors == []


def _free_loopback_port() -> int:
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        return int(reserved.getsockname()[1])


def _serve(
    app: Any,
    port: int,
) -> tuple[threading.Event, threading.Thread, list[BaseException]]:
    stop = threading.Event()
    errors: list[BaseException] = []

    async def shutdown_trigger() -> None:
        while not stop.is_set():
            await asyncio.sleep(0.05)

    async def run_server() -> None:
        config = Config()
        config.bind = [f"127.0.0.1:{port}"]
        config.use_reloader = False
        config.accesslog = None
        config.errorlog = None
        await hypercorn_serve(app, config, shutdown_trigger=shutdown_trigger)

    def target() -> None:
        try:
            asyncio.run(run_server())
        except BaseException as exc:  # pragma: no cover - re-raised below.
            errors.append(exc)

    thread = threading.Thread(
        target=target,
        name="spl-optionality-daemon",
        daemon=True,
    )
    thread.start()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if errors:
            raise RuntimeError("integrated daemon failed to start") from errors[0]
        try:
            SPLClient(
                f"http://127.0.0.1:{port}",
                api_token=app.api_token,
            ).health()
        except Exception:
            time.sleep(0.05)
        else:
            return stop, thread, errors
    stop.set()
    thread.join(timeout=2)
    raise TimeoutError("integrated daemon did not start")
