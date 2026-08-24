"""Packaged, owner-reviewed trust and public-runtime policy declaration."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from importlib.resources import files
from typing import Any


def _load_declaration() -> dict[str, Any]:
    value = json.loads(files("spl").joinpath("official-registry.json").read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise RuntimeError("packaged official registry declaration is invalid")
    registry = value.get("official_registry")
    if not isinstance(registry, Mapping):
        raise RuntimeError("packaged official registry pin is missing")
    try:
        origin = registry["origin"]
        algorithm = registry["signature_algorithm"]
        key_id = registry["signing_key_id"]
        encoded = registry["signing_public_key_b64"]
        raw = base64.b64decode(encoded, validate=True)
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("packaged official registry pin is malformed") from exc
    expected = f"ed25519-sha256:{hashlib.sha256(raw).hexdigest()}"
    if origin != "https://splime.io" or algorithm != "ed25519" or len(raw) != 32 or key_id != expected:
        raise RuntimeError("packaged official registry pin is inconsistent")
    return value


OFFICIAL_REGISTRY_DECLARATION = _load_declaration()
_OFFICIAL = OFFICIAL_REGISTRY_DECLARATION["official_registry"]
OFFICIAL_REGISTRY_ORIGIN = str(_OFFICIAL["origin"])
OFFICIAL_REGISTRY_ALGORITHM = str(_OFFICIAL["signature_algorithm"])
OFFICIAL_REGISTRY_KEY_ID = str(_OFFICIAL["signing_key_id"])
OFFICIAL_REGISTRY_PUBLIC_KEY_B64 = str(_OFFICIAL["signing_public_key_b64"])
PUBLIC_RUNTIME_POLICY = OFFICIAL_REGISTRY_DECLARATION["public_runtime_policy"]


def official_registry_trusted_keys(origin: str) -> dict[str, dict[str, str]] | None:
    """Return the built-in trust map only for the exact normalized official origin."""

    if origin != OFFICIAL_REGISTRY_ORIGIN:
        return None
    return {origin: {OFFICIAL_REGISTRY_KEY_ID: OFFICIAL_REGISTRY_PUBLIC_KEY_B64}}


__all__ = [
    "OFFICIAL_REGISTRY_ALGORITHM",
    "OFFICIAL_REGISTRY_DECLARATION",
    "OFFICIAL_REGISTRY_KEY_ID",
    "OFFICIAL_REGISTRY_ORIGIN",
    "OFFICIAL_REGISTRY_PUBLIC_KEY_B64",
    "PUBLIC_RUNTIME_POLICY",
    "official_registry_trusted_keys",
]
