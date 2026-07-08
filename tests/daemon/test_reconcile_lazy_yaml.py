"""Reconcile fetches YAML lazily (stage 1.4 of the 0.2.0 plan).

The connect-time reconcile must cost one metadata-only listing per object;
YAML bodies travel only for versions this daemon has never seen, and a
repeated reconcile of an already-synced object performs zero YAML fetches.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore

from .test_object_identity import FUNCTION_YAML, FUNCTION_YAML_V2

OWNER = "owner1"
REMOTE_OBJECT_ID = "srvobj1"


class LazyYamlServerStub:
    """Serves version metadata cheaply and counts every YAML body fetch."""

    def __init__(self, versions: list[dict[str, Any]]) -> None:
        self._versions = versions
        self.yaml_fetches: list[int] = []

    def get_object(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        if version is None:
            latest = self._versions[-1]
            return {**self._strip(latest), "id": REMOTE_OBJECT_ID}
        matched = next(item for item in self._versions if item["version"] == version)
        if include_yaml:
            self.yaml_fetches.append(version)
            return dict(matched)
        return self._strip(matched)

    def list_object_versions(
        self,
        name_or_id: str,
        *,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        assert not include_yaml, "reconcile must list versions without YAML"
        return [self._strip(item) for item in self._versions]

    @staticmethod
    def _strip(item: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in item.items() if key != "yaml"}


@pytest.fixture()
def runtime(tmp_path: Path) -> Iterator[DaemonRuntime]:
    store = RegistryStore(tmp_path)
    store.register_env("default", sys.executable)
    try:
        yield DaemonRuntime(store)
    finally:
        store.close()


def _remote_version(number: int, *, yaml_text: str, content_hash: str) -> dict[str, Any]:
    return {
        "id": REMOTE_OBJECT_ID,
        "version_id": f"srvv{number}",
        "version": number,
        "name": "demo_obj",
        "entrypoint": "demo_obj",
        "env": "default",
        "kind": "function",
        "description": "",
        "owner_id": OWNER,
        "content_hash": content_hash,
        "runtime_config": {"mode": "venv"},
        "yaml": yaml_text,
    }


def test_reconcile_fetches_yaml_only_for_unknown_versions(
    runtime: DaemonRuntime,
) -> None:
    local = runtime.register_object(
        "demo_obj",
        "demo_obj",
        "default",
        yaml_text=FUNCTION_YAML,
        owner_id=OWNER,
    )
    server = LazyYamlServerStub(
        [
            # v1 matches the local content: linking must not need its body.
            _remote_version(1, yaml_text=FUNCTION_YAML, content_hash=local["content_hash"]),
            # v2 is new to this daemon: its body is fetched lazily.
            _remote_version(2, yaml_text=FUNCTION_YAML_V2, content_hash="f" * 64),
        ]
    )

    first = runtime._reconcile_connected_object(
        server,
        {"name": "demo_obj", "library": "default"},
        owner_id=OWNER,
    )

    assert first["status"] == "linked"
    assert first["conflicts"] == []
    assert set(server.yaml_fetches) <= {1, 2}
    assert 2 in server.yaml_fetches, "the genuinely new version needs its YAML"
    versions = runtime.store.list_object_versions("demo_obj", owner_id=OWNER, library="default")
    assert {int(item["version"]) for item in versions} == {1, 2}

    # Steady state: everything is linked — zero YAML round-trips.
    server.yaml_fetches.clear()
    second = runtime._reconcile_connected_object(
        server,
        {"name": "demo_obj", "library": "default"},
        owner_id=OWNER,
    )
    assert second["status"] == "linked"
    assert server.yaml_fetches == []
