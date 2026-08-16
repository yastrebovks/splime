from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from spl.daemon import lifecycle_startup
from spl.daemon.lifecycle_startup import (
    FileIdentity,
    StartupBindingError,
    load_startup_binding,
    revalidate_startup_binding,
)


class _Distribution:
    version = "0.4.6"


def _file_identity(seed: str, *, home: bool = False) -> FileIdentity:
    return FileIdentity(
        path_sha256="sha256:" + seed * 64,
        device=1 if not home else 3,
        inode=2 if not home else 4,
        owner_uid=501,
        mode=0o755 if not home else 0o700,
        sha256=None if home else "sha256:" + "f" * 64,
    )


def _binding_document() -> dict[str, Any]:
    return {
        "schema": "spl.lifecycle.startup-binding",
        "schema_version": 1,
        "supervisor_id": "supervisor-1",
        "spawn_nonce": "spawn-nonce-value",
        "supervisor_proof": "supervisor-proof-value",
        "executable": _file_identity("a").document(),
        "home": _file_identity("b", home=True).document(),
        "release": {
            "distribution": "splime",
            "version": "0.4.6",
            "entry_point": "spl-daemon",
        },
    }


def _binding_fd(document: dict[str, Any]) -> int:
    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, json.dumps(document).encode())
    finally:
        os.close(write_fd)
    return read_fd


def _mock_installed_selection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    executable = tmp_path / "spl-daemon"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    monkeypatch.setattr(lifecycle_startup.os, "getuid", lambda: 501)
    monkeypatch.setattr(lifecycle_startup, "_installed_entry_point_path", lambda _distribution: executable)
    monkeypatch.setattr(
        lifecycle_startup,
        "_protected_file_identity",
        lambda _path, *, include_sha256: _file_identity("a"),
    )
    monkeypatch.setattr(
        lifecycle_startup,
        "_protected_home_identity",
        lambda _path: _file_identity("b", home=True),
    )
    monkeypatch.setattr(lifecycle_startup, "_same_file_without_symlink", lambda _first, _second: True)
    return executable, home


def test_inherited_binding_is_closed_bounded_and_consumed_from_fd(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable, home = _mock_installed_selection(monkeypatch, tmp_path)
    fd = _binding_fd(_binding_document())

    binding, proof = load_startup_binding(
        fd,
        home=home,
        argv0=str(executable),
        distribution=_Distribution(),  # type: ignore[arg-type]
    )

    assert binding is not None
    assert binding.supervisor_id == "supervisor-1"
    assert proof == "supervisor-proof-value"
    with pytest.raises(OSError):
        os.fstat(fd)


@pytest.mark.parametrize(
    "update",
    [
        {"unknown": True},
        {"schema_version": 2},
        {"supervisor_proof": "short"},
        {"release": {"distribution": "splime", "version": "0.4.5", "entry_point": "spl-daemon"}},
    ],
)
def test_inherited_binding_rejects_unknown_skewed_and_weak_documents(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    update: dict[str, Any],
) -> None:
    executable, home = _mock_installed_selection(monkeypatch, tmp_path)
    document = _binding_document()
    document.update(update)

    with pytest.raises(StartupBindingError):
        load_startup_binding(
            _binding_fd(document),
            home=home,
            argv0=str(executable),
            distribution=_Distribution(),  # type: ignore[arg-type]
        )


def test_revalidation_fails_closed_when_home_or_executable_identity_changes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable, home = _mock_installed_selection(monkeypatch, tmp_path)
    binding, _ = load_startup_binding(
        _binding_fd(_binding_document()),
        home=home,
        argv0=str(executable),
        distribution=_Distribution(),  # type: ignore[arg-type]
    )
    assert binding is not None
    monkeypatch.setattr(lifecycle_startup, "_installed_distribution", lambda: _Distribution())
    revalidate_startup_binding(binding, home=home)

    monkeypatch.setattr(
        lifecycle_startup,
        "_protected_home_identity",
        lambda _path: _file_identity("c", home=True),
    )
    with pytest.raises(StartupBindingError, match="identity changed"):
        revalidate_startup_binding(binding, home=home)


def test_home_selection_requires_owner_only_directory_and_refuses_symlink(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    home.chmod(0o755)
    with pytest.raises(StartupBindingError, match="mode 0700"):
        lifecycle_startup._protected_home_identity(home)

    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    with pytest.raises(StartupBindingError, match="non-symlink"):
        lifecycle_startup._protected_home_identity(alias)


def test_executable_selection_refuses_writable_parent(tmp_path: Path) -> None:
    writable = tmp_path / "writable"
    writable.mkdir(mode=0o777)
    writable.chmod(0o777)
    executable = writable / "spl-daemon"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    with pytest.raises(StartupBindingError, match="writable parent"):
        lifecycle_startup._protected_file_identity(executable.absolute(), include_sha256=True)
