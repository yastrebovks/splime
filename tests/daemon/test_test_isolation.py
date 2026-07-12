from __future__ import annotations

import os
from pathlib import Path

import pytest

from spl.daemon.secret_store import SecretStore
from spl.daemon.store import RegistryStore
from spl.daemon_client import Client


def test_registry_store_default_home_is_pytest_isolated() -> None:
    expected_home = Path(os.environ["SPL_DAEMON_HOME"]).resolve()
    live_home = (Path.home() / ".spl-daemon").resolve()
    assert expected_home != live_home

    store = RegistryStore()
    try:
        assert store.home == expected_home
    finally:
        store.close()


def test_registry_store_rejects_live_daemon_home() -> None:
    with pytest.raises(pytest.fail.Exception, match="live daemon home"):
        RegistryStore(Path.home() / ".spl-daemon")


def test_daemon_client_rejects_live_daemon_endpoint() -> None:
    with pytest.raises(pytest.fail.Exception, match="live daemon endpoint"):
        Client(daemon_home=Path.home() / ".spl-daemon")


def test_secret_store_rejects_live_daemon_home() -> None:
    with pytest.raises(pytest.fail.Exception, match="live daemon home"):
        SecretStore(Path.home() / ".spl-daemon")
