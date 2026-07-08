"""Deterministic node fingerprints for retained runs and resume."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, TypeAlias, TypeVar

FINGERPRINT_FORMAT_VERSION = 1

_SHA256_HEX_DIGITS = set("0123456789abcdefABCDEF")

_T = TypeVar("_T")
_NamedItems: TypeAlias = Mapping[str, _T] | Iterable[tuple[str, _T]]
AdapterIdentityItems: TypeAlias = _NamedItems[Mapping[str, Any]]
ArtifactInputItems: TypeAlias = _NamedItems[str]
InlineInputItems: TypeAlias = _NamedItems[Any]


def node_fingerprint(
    *,
    node_content: bytes | None = None,
    node_identity: str | None = None,
    node_version: str | int | None = None,
    input_ports: Iterable[str] | None = None,
    output_ports: Iterable[str] | None = None,
    adapter_identities: AdapterIdentityItems | None = None,
    artifact_inputs: ArtifactInputItems | None = None,
    inline_inputs: InlineInputItems | None = None,
) -> str:
    """Return the SHA-256 fingerprint for a node execution boundary."""

    return _sha256(
        canonical_fingerprint_bytes(
            node_fingerprint_payload(
                node_content=node_content,
                node_identity=node_identity,
                node_version=node_version,
                input_ports=input_ports,
                output_ports=output_ports,
                adapter_identities=adapter_identities,
                artifact_inputs=artifact_inputs,
                inline_inputs=inline_inputs,
            )
        )
    )


def node_fingerprint_payload(
    *,
    node_content: bytes | None = None,
    node_identity: str | None = None,
    node_version: str | int | None = None,
    input_ports: Iterable[str] | None = None,
    output_ports: Iterable[str] | None = None,
    adapter_identities: AdapterIdentityItems | None = None,
    artifact_inputs: ArtifactInputItems | None = None,
    inline_inputs: InlineInputItems | None = None,
) -> dict[str, Any]:
    """Return the canonical payload that is hashed for a node fingerprint."""

    if node_content is None and node_identity is None:
        raise ValueError("node fingerprint requires node_content or node_identity")
    return {
        "fingerprint_format_version": FINGERPRINT_FORMAT_VERSION,
        "node": {
            "content_sha256": None if node_content is None else _sha256(node_content),
            "identity": _normalize_optional_string("node_identity", node_identity),
            "version": None if node_version is None else str(node_version),
        },
        "ports": {
            "inputs": _normalize_ports("input_ports", input_ports),
            "outputs": _normalize_ports("output_ports", output_ports),
        },
        "adapters": _adapter_entries(adapter_identities),
        "inputs": {
            "artifacts": _artifact_input_entries(artifact_inputs),
            "inline": _inline_input_entries(inline_inputs),
        },
    }


def canonical_fingerprint_bytes(payload: Mapping[str, Any]) -> bytes:
    """Return stable JSON bytes for a fingerprint payload."""

    return canonical_json_bytes(payload)


def canonical_json_bytes(value: Any) -> bytes:
    """Return stable JSON bytes for JSON-native inline values."""

    normalized = _normalize_plain(value)
    text = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return "{}\n".format(text).encode("utf-8")


def inline_value_sha256(value: Any) -> str:
    """Return the SHA-256 digest used for JSON-native inline values."""

    return _sha256(canonical_json_bytes(value))


def _adapter_entries(items: AdapterIdentityItems | None) -> list[dict[str, Any]]:
    entries = [
        {"port": name, "identity": _normalize_plain(identity)}
        for name, identity in _named_items("adapter_identities", items)
    ]
    return _sorted_canonical(entries)


def _artifact_input_entries(items: ArtifactInputItems | None) -> list[dict[str, str]]:
    entries = []
    for name, digest in _named_items("artifact_inputs", items):
        _validate_sha256(digest)
        entries.append({"port": name, "sha256": digest.lower()})
    return _sorted_canonical(entries)


def _inline_input_entries(items: InlineInputItems | None) -> list[dict[str, str]]:
    entries = [
        {"port": name, "sha256": inline_value_sha256(value)} for name, value in _named_items("inline_inputs", items)
    ]
    return _sorted_canonical(entries)


def _named_items(name: str, items: _NamedItems[_T] | None) -> list[tuple[str, _T]]:
    if items is None:
        return []
    if isinstance(items, Mapping):
        return [(_normalize_name(name, item_name), value) for item_name, value in items.items()]
    return [(_normalize_name(name, item_name), value) for item_name, value in items]


def _normalize_ports(name: str, ports: Iterable[str] | None) -> list[str]:
    normalized = [] if ports is None else [_normalize_name(name, port) for port in ports]
    if len(set(normalized)) != len(normalized):
        raise ValueError("{} must not contain duplicate names".format(name))
    return normalized


def _normalize_name(context: str, value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("{} names must be strings".format(context))
    if not value:
        raise ValueError("{} names must be non-empty strings".format(context))
    return value


def _normalize_optional_string(name: str, value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("{} must be a string".format(name))
    if not value:
        raise ValueError("{} must be a non-empty string".format(name))
    return value


def _validate_sha256(value: str) -> None:
    if not isinstance(value, str):
        raise TypeError("input sha256 must be a string")
    if len(value) != 64 or any(c not in _SHA256_HEX_DIGITS for c in value):
        raise ValueError("input sha256 must be a 64-character hex string")


def _normalize_plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_plain(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, tuple | list):
        return [_normalize_plain(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, str):
        return value.replace("\r\n", "\n").replace("\r", "\n")
    return value


def _sorted_canonical(values: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(values, key=_canonical_sort_key)


def _canonical_sort_key(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
