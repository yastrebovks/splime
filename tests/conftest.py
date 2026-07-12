from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from spl.core.adapter_compat import _reset_adapter_compatibility_warnings

_REAL_DAEMON_HOME = (Path.home() / ".spl-daemon").resolve()
_PYTEST_DAEMON_HOME_ENV = "SPL_PYTEST_DAEMON_HOME"


def pytest_configure(config: pytest.Config) -> None:
    """Install daemon-home isolation before test modules are collected."""

    daemon_home = Path(tempfile.mkdtemp(prefix="spl-pytest-daemon-home-")).resolve()
    os.environ["SPL_DAEMON_HOME"] = str(daemon_home)
    os.environ["SPL_DAEMON_SECRET_BACKEND"] = "file"
    os.environ[_PYTEST_DAEMON_HOME_ENV] = str(daemon_home)
    config._spl_pytest_daemon_home = daemon_home  # type: ignore[attr-defined]


def pytest_unconfigure(config: pytest.Config) -> None:
    """Remove the session daemon home created for pytest."""

    daemon_home = getattr(config, "_spl_pytest_daemon_home", None)
    if isinstance(daemon_home, Path):
        shutil.rmtree(daemon_home, ignore_errors=True)


def _resolve_home(value: str | Path | None) -> Path:
    if value is None:
        return Path(os.environ.get("SPL_DAEMON_HOME", _REAL_DAEMON_HOME)).expanduser().resolve()
    return Path(value).expanduser().resolve()


@pytest.fixture(scope="session", autouse=True)
def isolate_daemon_home_and_secrets_guard() -> Iterator[None]:
    """Fail fast if a test tries to use the live daemon home or keyring."""

    session_home = _resolve_home(os.environ.get("SPL_DAEMON_HOME"))
    if session_home == _REAL_DAEMON_HOME:
        pytest.fail("tests must not use the live daemon home ~/.spl-daemon")
    if os.environ.get("SPL_DAEMON_SECRET_BACKEND") != "file":
        pytest.fail("tests must use SPL_DAEMON_SECRET_BACKEND=file")

    import spl.daemon.storage_base as storage_base
    import spl.daemon.client as daemon_client_shim
    import spl.daemon_client as daemon_client
    from spl.daemon.secret_store import SECRET_BACKEND_ENV, SecretStore

    monkeypatch = pytest.MonkeyPatch()
    original_storage_init = storage_base.StorageBase.__init__
    original_endpoint_file = daemon_client.daemon_endpoint_file
    original_secret_store_init = SecretStore.__init__

    def guarded_storage_init(self: storage_base.StorageBase, home: Path | None = None) -> None:
        target = _resolve_home(home)
        if target == _REAL_DAEMON_HOME:
            pytest.fail("test attempted to open the live daemon home ~/.spl-daemon")
        original_storage_init(self, home)

    def guarded_daemon_endpoint_file(home: str | Path | None = None) -> Path:
        path = original_endpoint_file(home)
        if path.parent.expanduser().resolve() == _REAL_DAEMON_HOME:
            pytest.fail("test attempted to read or write the live daemon endpoint file")
        return path

    def guarded_secret_store_init(self: SecretStore, home: Path) -> None:
        if os.environ.get(SECRET_BACKEND_ENV) != "file":
            pytest.fail("tests must not use the OS keyring; set SPL_DAEMON_SECRET_BACKEND=file")
        if Path(home).expanduser().resolve() == _REAL_DAEMON_HOME:
            pytest.fail("test attempted to open secrets for the live daemon home ~/.spl-daemon")
        original_secret_store_init(self, home)

    monkeypatch.setattr(storage_base.StorageBase, "__init__", guarded_storage_init)
    monkeypatch.setattr(daemon_client, "daemon_endpoint_file", guarded_daemon_endpoint_file)
    monkeypatch.setattr(daemon_client_shim, "daemon_endpoint_file", guarded_daemon_endpoint_file)
    monkeypatch.setattr(SecretStore, "__init__", guarded_secret_store_init)
    try:
        yield
    finally:
        monkeypatch.undo()


@pytest.fixture(autouse=True)
def reset_adapter_compatibility_warnings() -> Iterator[None]:
    """Keep process-global adapter warning dedupe isolated between tests."""

    _reset_adapter_compatibility_warnings()
    yield
    _reset_adapter_compatibility_warnings()
