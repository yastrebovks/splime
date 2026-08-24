from __future__ import annotations

from typing import Any

import pytest

from spl.core.publications import (
    PUBLIC_ADAPTER_PROFILE_CAPABILITY,
    PUBLIC_OBJECT_PROFILE_CAPABILITY,
)
from spl.daemon.publications import require_server_publication_capability
from spl.daemon.remote_client import ServerClient
from spl.daemon.routes._helpers import FeatureNotSupportedError


class _VersionServer:
    def __init__(self, capabilities: list[str]) -> None:
        self.capabilities = capabilities

    def get_server_version(self) -> dict[str, Any]:
        return {"declared": {"contracts": {"daemon_server_capabilities": self.capabilities}}}


class _RecordingServerClient(ServerClient):
    def __init__(self) -> None:
        super().__init__("https://server.invalid", "machine", user_token="user")
        self.requests: list[tuple[str, str, dict[str, Any] | None, dict[str, Any]]] = []

    def _json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        self.requests.append((method, path, payload, kwargs))
        return {"ok": True}


def test_server_capability_is_proven_before_new_mutation() -> None:
    with pytest.raises(FeatureNotSupportedError) as error:
        require_server_publication_capability(_VersionServer([]), PUBLIC_OBJECT_PROFILE_CAPABILITY)
    assert error.value.code == "feature_not_supported"

    require_server_publication_capability(
        _VersionServer([PUBLIC_OBJECT_PROFILE_CAPABILITY, PUBLIC_ADAPTER_PROFILE_CAPABILITY]),
        PUBLIC_ADAPTER_PROFILE_CAPABILITY,
    )


def test_remote_profile_methods_use_authenticated_additive_routes_without_retry() -> None:
    server = _RecordingServerClient()
    server.update_object_profile(
        "demo",
        {"description": "profile", "public": True, "expected_revision": 2},
    )
    server.update_library_adapter_profile("adapter-1", {"public": False})

    object_call, adapter_call = server.requests
    assert object_call[:3] == (
        "PATCH",
        "/objects/demo/profile",
        {"description": "profile", "public": True, "expected_revision": 2},
    )
    assert object_call[3]["allow_transport_retries"] is False
    assert object_call[3]["max_transport_attempts"] == 1
    assert adapter_call[:3] == (
        "PATCH",
        "/library-adapters/adapter-1/profile",
        {"public": False},
    )
