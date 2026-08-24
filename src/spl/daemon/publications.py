"""Capability proof shared by public-profile daemon pass-through routes."""

from __future__ import annotations

from typing import Any

from spl.core.publications import (
    PUBLIC_ADAPTER_PROFILE_CAPABILITY,
    PUBLIC_OBJECT_PROFILE_CAPABILITY,
)
from spl.daemon.routes._helpers import FeatureNotSupportedError


def require_server_publication_capability(server: Any, capability_id: str) -> None:
    if capability_id not in {
        PUBLIC_OBJECT_PROFILE_CAPABILITY,
        PUBLIC_ADAPTER_PROFILE_CAPABILITY,
    }:
        raise ValueError("public profile capability is not recognized")
    document = server.get_server_version()
    declared = document.get("declared") if isinstance(document, dict) else None
    contracts = declared.get("contracts") if isinstance(declared, dict) else None
    capabilities = contracts.get("daemon_server_capabilities") if isinstance(contracts, dict) else None
    if not isinstance(capabilities, list) or capability_id not in capabilities:
        raise FeatureNotSupportedError(
            "central SPL daemon server does not support public Object profiles; "
            "upgrade the server before using this feature"
        )
