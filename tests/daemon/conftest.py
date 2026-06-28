from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def use_file_secret_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPL_DAEMON_SECRET_BACKEND", "file")
