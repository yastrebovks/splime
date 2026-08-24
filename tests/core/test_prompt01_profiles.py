from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from spl import SPLClient, UNSET
from spl.core.publications import (
    PUBLIC_ADAPTER_PROFILE_CAPABILITY,
    PUBLIC_OBJECT_PROFILE_CAPABILITY,
)
from spl.daemon.store import RegistryStore
from spl.daemon_client import ClientError


FUNCTION_YAML = """\
- !DFunction
  name: demo
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 1
"""


class _ProfileDaemon:
    def __init__(self, *, capable: bool) -> None:
        self.capable = capable
        self.register_calls: list[dict[str, Any]] = []
        self.profile_calls: list[tuple[str, dict[str, Any]]] = []

    def supports_public_profile_capability(self, capability_id: str) -> bool:
        assert capability_id == PUBLIC_OBJECT_PROFILE_CAPABILITY
        return self.capable

    def require_public_profile_capability(self, capability_id: str) -> dict[str, Any]:
        if not self.capable:
            raise ClientError("unsupported", payload={"code": "feature_not_supported"})
        return {"state": "supported", "version": 1, "reason": None}

    def register_object(self, name: str, **kwargs: Any) -> dict[str, Any]:
        self.register_calls.append({"name": name, **kwargs})
        return {
            "name": name,
            "entrypoint": kwargs["entrypoint"],
            "env": kwargs["env"],
            "yaml_path": "/tmp/object.yaml",
        }

    def update_object_profile(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.profile_calls.append((name, payload))
        return {"name": name, **payload}

    def update_library_adapter_profile(self, adapter_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.profile_calls.append((adapter_id, payload))
        return {"adapter_id": adapter_id, **payload}


def _client(daemon: Any) -> SPLClient:
    client = object.__new__(SPLClient)
    client._daemon = daemon
    client.server_connection = None
    client._user_token = None
    return client


def test_publish_description_intent_is_capability_negotiated() -> None:
    daemon = _ProfileDaemon(capable=True)
    client = _client(daemon)

    client.publish_yaml(FUNCTION_YAML, name="demo", entrypoint="demo")
    client.publish_yaml(
        FUNCTION_YAML,
        name="demo",
        entrypoint="demo",
        description="",
    )

    assert daemon.register_calls[0]["profile_description"] == {"mode": "preserve"}
    assert daemon.register_calls[1]["profile_description"] == {
        "mode": "set",
        "value": "",
    }


def test_old_daemon_receives_legacy_publish_and_rejects_only_new_intent() -> None:
    daemon = _ProfileDaemon(capable=False)
    client = _client(daemon)

    client.publish_yaml(FUNCTION_YAML, name="demo", entrypoint="demo")
    assert "profile_description" not in daemon.register_calls[0]

    with pytest.raises(ClientError) as error:
        client.publish_yaml(
            FUNCTION_YAML,
            name="demo",
            entrypoint="demo",
            description="new",
        )
    assert error.value.code == "feature_not_supported"
    assert len(daemon.register_calls) == 1


def test_grouped_profile_surfaces_preserve_omission_and_adapter_identity() -> None:
    daemon = _ProfileDaemon(capable=True)
    client = _client(daemon)

    client.object.update("demo", public=True, expected_revision=2, wait=False)
    client.update_library_adapter("adapter-1", description="profile")

    assert daemon.profile_calls == [
        (
            "demo",
            {"wait": False, "public": True, "expected_revision": 2},
        ),
        ("adapter-1", {"wait": True, "description": "profile"}),
    ]
    assert UNSET is not None
    assert PUBLIC_ADAPTER_PROFILE_CAPABILITY != PUBLIC_OBJECT_PROFILE_CAPABILITY


def test_capable_local_registration_preserves_or_clears_without_extra_version(
    tmp_path: Path,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        first = store.register_object(
            "demo",
            "demo",
            "default",
            yaml_text=FUNCTION_YAML,
            description="original",
        )
        preserved = store.register_object(
            "demo",
            "demo",
            "default",
            yaml_text=FUNCTION_YAML,
            preserve_description=True,
        )
        assert preserved["version_id"] == first["version_id"]
        assert preserved["description"] == "original"
        assert len(store.list_object_versions("demo")) == 1

        cleared = store.register_object(
            "demo",
            "demo",
            "default",
            yaml_text=FUNCTION_YAML,
            description="",
        )
        assert cleared["version_id"] == first["version_id"]
        assert cleared["description"] == ""
        assert len(store.list_object_versions("demo")) == 1
    finally:
        store.close()
