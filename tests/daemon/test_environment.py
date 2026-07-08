from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Iterator

import pytest

import spl.daemon.environment as environment_module
from spl.daemon.environment import (
    EnvironmentManager,
    _PipEnvironmentBuilder,
    _UvEnvironmentBuilder,
)
from spl.daemon.store import RegistryStore


@pytest.fixture
def store(tmp_path: Path) -> Iterator[RegistryStore]:
    registry = RegistryStore(tmp_path)
    try:
        yield registry
    finally:
        registry.close()


def _object_record() -> dict[str, Any]:
    return {
        "name": "demo_obj",
        "origin": "local",
        "env": "default",
        "env_python": sys.executable,
        "env_python_version": "Python 3.13.test",
        "distributions": [{"package": "pyyaml", "version": "6.0.3"}],
    }


def test_uv_builder_commands_use_relocatable_venv_and_strict_install(
    tmp_path: Path,
) -> None:
    builder = _UvEnvironmentBuilder("/opt/bin/uv")
    spec = {
        "base_python": "/opt/python/bin/python3.13",
        "venv_path": tmp_path / "venv",
        "python_path": tmp_path / "venv" / "bin" / "python",
    }

    assert builder.create_command(spec) == [
        "/opt/bin/uv",
        "venv",
        "--relocatable",
        "--python",
        "/opt/python/bin/python3.13",
        str(tmp_path / "venv"),
    ]
    assert builder.install_command(spec, ["pyyaml==6.0.3"]) == [
        "/opt/bin/uv",
        "pip",
        "install",
        "--strict",
        "--python",
        str(tmp_path / "venv" / "bin" / "python"),
        "pyyaml==6.0.3",
    ]


def test_pip_builder_commands_preserve_existing_venv_and_pip_path(
    tmp_path: Path,
) -> None:
    builder = _PipEnvironmentBuilder()
    spec = {
        "base_python": "/opt/python/bin/python3.13",
        "venv_path": tmp_path / "venv",
        "python_path": tmp_path / "venv" / "bin" / "python",
    }

    assert builder.create_command(spec) == [
        "/opt/python/bin/python3.13",
        "-m",
        "venv",
        str(tmp_path / "venv"),
    ]
    assert builder.install_command(spec, ["pyyaml==6.0.3"]) == [
        str(tmp_path / "venv" / "bin" / "python"),
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "pyyaml==6.0.3",
    ]


def test_environment_manager_prefers_uv_and_hashes_builder(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        environment_module.shutil,
        "which",
        lambda name: "/opt/bin/uv" if name == "uv" else None,
    )
    uv_spec = EnvironmentManager(store).build_spec(_object_record())

    monkeypatch.setattr(environment_module.shutil, "which", lambda name: None)
    pip_spec = EnvironmentManager(store).build_spec(_object_record())

    assert uv_spec["builder"] == "uv"
    assert uv_spec["spec"]["builder"] == "uv"
    assert pip_spec["builder"] == "pip"
    assert pip_spec["spec"]["builder"] == "pip"
    assert uv_spec["spec_hash"] != pip_spec["spec_hash"]


def test_build_spec_caches_python_version_by_path_and_mtime(
    store: RegistryStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    python = tmp_path / "python"
    python.write_text("#!/bin/sh\n", encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> Any:
        calls.append(command)
        return environment_module.subprocess.CompletedProcess(
            command,
            0,
            stdout="Python 3.13.cached\n",
            stderr="",
        )

    monkeypatch.setattr(environment_module.subprocess, "run", fake_run)
    record = _object_record()
    record.pop("env_python_version")
    record["env_python"] = str(python)
    manager = EnvironmentManager(store)

    first = manager.build_spec(record)
    second = manager.build_spec(record)

    assert first["python_version"] == "Python 3.13.cached"
    assert second["python_version"] == "Python 3.13.cached"
    assert calls == [[str(python.absolute()), "--version"]]


def test_uv_build_strategy_logs_builder_and_full_commands(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        environment_module.shutil,
        "which",
        lambda name: "/opt/bin/uv" if name == "uv" else None,
    )
    manager = EnvironmentManager(store)
    spec = manager.build_spec(_object_record())
    commands: list[list[str]] = []

    def fake_run_logged(command: list[str], log: Any) -> None:
        commands.append(command)
        log.write("\n$ " + " ".join(command) + "\n")
        log.write("fake command ok\n")

    monkeypatch.setattr(manager, "_run_logged", fake_run_logged)
    manager._upsert_creating_record(spec)
    manager._build_environment(spec)

    assert commands == [
        [
            "/opt/bin/uv",
            "venv",
            "--relocatable",
            "--python",
            sys.executable,
            str(spec["venv_path"]),
        ],
        [
            "/opt/bin/uv",
            "pip",
            "install",
            "--strict",
            "--python",
            str(spec["python_path"]),
            "pyyaml==6.0.3",
        ],
    ]
    log_text = Path(spec["install_log_path"]).read_text(encoding="utf-8")
    assert "Builder: uv" in log_text
    assert "$ /opt/bin/uv venv --relocatable --python" in log_text
    assert "$ /opt/bin/uv pip install --strict --python" in log_text
    assert store.get_environment_build(spec["spec_hash"])["builder"] == "uv"
    assert not Path(spec["lock_path"]).exists()


def test_missing_uv_falls_back_to_pip_strategy(
    store: RegistryStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(environment_module.shutil, "which", lambda name: None)
    manager = EnvironmentManager(store)
    spec = manager.build_spec(_object_record())
    commands: list[list[str]] = []

    def fake_run_logged(command: list[str], log: Any) -> None:
        commands.append(command)
        log.write("\n$ " + " ".join(command) + "\n")
        log.write("fake command ok\n")

    monkeypatch.setattr(manager, "_run_logged", fake_run_logged)
    manager._upsert_creating_record(spec)
    manager._build_environment(spec)

    assert spec["builder"] == "pip"
    assert commands == [
        [sys.executable, "-m", "venv", str(spec["venv_path"])],
        [
            str(spec["python_path"]),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "pyyaml==6.0.3",
        ],
    ]
    log_text = Path(spec["install_log_path"]).read_text(encoding="utf-8")
    assert "Builder: pip" in log_text
    assert "$ " + sys.executable + " -m venv" in log_text
    assert "pip install --disable-pip-version-check" in log_text
    assert store.get_environment_build(spec["spec_hash"])["builder"] == "pip"
