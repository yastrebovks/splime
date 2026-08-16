"""Daemon-side preparation and committed receipts for Library Adapters."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from spl.core.library_adapters import (
    LIBRARY_ADAPTER_SCHEMA_VERSION,
    LibraryAdapterContractError,
    domain_hash,
    normalize_publish_request,
)
from spl.daemon.storage_base import DEFAULT_OBJECT_LIBRARY, DEFAULT_OBJECT_OWNER_ID, validate_name

LIBRARY_ADAPTER_PREFLIGHT_HASH_DOMAIN = "spl.library-adapter.preflight/v1"


def prepare_library_adapter_publication(
    store: Any,
    payload: Mapping[str, Any],
    *,
    owner_id: str | None = None,
    library: str | None = None,
) -> dict[str, Any]:
    """Validate a publication completely without mutating registry state."""

    owner = library_adapter_write_owner(store, owner_id)
    library_name = validate_name(str(library or DEFAULT_OBJECT_LIBRARY))
    prepared = normalize_publish_request(payload)
    current = None
    try:
        current = store.get_library_adapter(
            prepared["name"],
            owner_id=owner,
            library=library_name,
        )
    except KeyError:
        pass
    validation_hash = domain_hash(
        LIBRARY_ADAPTER_PREFLIGHT_HASH_DOMAIN,
        {
            "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
            "owner": owner,
            "library": library_name,
            "name": prepared["name"],
            "content_hash": prepared["content_hash"],
            "signature_hash": prepared["signature_hash"],
            "environment_fingerprint": prepared["publication_environment"]["fingerprint"],
        },
    )
    return {
        "schema": "spl.library-adapter-preflight",
        "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
        "status": "valid",
        "owner": owner,
        "library": library_name,
        "name": prepared["name"],
        "directions": list(prepared["directions"]),
        "dependencies": list(prepared["dependencies"]),
        "content_hash": prepared["content_hash"],
        "signature_hash": prepared["signature_hash"],
        "environment_fingerprint": prepared["publication_environment"]["fingerprint"],
        "validation_hash": validation_hash,
        "current": current,
    }


def commit_library_adapter_publication(
    store: Any,
    payload: Mapping[str, Any],
    *,
    owner_id: str | None = None,
    library: str | None = None,
    publisher_id: str | None = None,
) -> dict[str, Any]:
    """Revalidate then atomically create/deduplicate one immutable version."""

    owner = library_adapter_write_owner(store, owner_id)
    library_name = validate_name(str(library or DEFAULT_OBJECT_LIBRARY))
    prepared = normalize_publish_request(payload)
    record = store.publish_library_adapter(
        prepared,
        owner_id=owner,
        library=library_name,
        publisher_id=publisher_id or owner,
    )
    ref = {
        key: record[key]
        for key in (
            "owner",
            "library",
            "name",
            "version",
            "adapter_id",
            "adapter_version_id",
            "content_hash",
            "signature_hash",
        )
    }
    version = {key: value for key, value in record.items() if key not in {"created", "deduplicated"}}
    return {
        "schema": "spl.library-adapter-publication-receipt",
        "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
        "committed": True,
        "created": bool(record["created"]),
        "deduplicated": bool(record["deduplicated"]),
        "ref": ref,
        "version": version,
        "validation": {
            "content_hash": prepared["content_hash"],
            "signature_hash": prepared["signature_hash"],
            "environment_fingerprint": prepared["publication_environment"]["fingerprint"],
        },
    }


def library_adapter_sync_payload(store: Any, ref: Mapping[str, Any]) -> dict[str, Any]:
    """Return the exact source-bearing payload for a capability-gated sync."""

    record = store.resolve_library_adapter_ref(ref, include_source=True)
    return {
        "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
        "library": record["library"],
        "name": record["name"],
        "description": record["description"],
        "semantic_type": record["semantic_type"],
        "semantic_category": record["semantic_category"],
        "save_source": record["save_source"],
        "load_source": record["load_source"],
        "dependencies": record["dependencies"],
        "format_tag": record["format_tag"],
        "media_type": record["media_type"],
        "preferred_extension": record["preferred_extension"],
        "policy": record["policy"],
        "publication_environment": record["publication_environment"],
        "content_hash": record["content_hash"],
        "signature_hash": record["signature_hash"],
        "source_adapter_id": record["adapter_id"],
        "source_version_id": record["adapter_version_id"],
    }


def library_adapter_write_owner(store: Any, requested: str | None) -> str:
    credentials = store.current_server_connection_credentials()
    enrolled = (
        validate_name(str(credentials["owner_id"]))
        if credentials is not None and credentials.get("owner_id")
        else DEFAULT_OBJECT_OWNER_ID
    )
    if requested is None:
        return enrolled
    owner = validate_name(requested)
    if owner != enrolled:
        raise LibraryAdapterContractError(
            "owner_forbidden",
            "Library Adapter publication is limited to the connected owner",
        )
    return owner


__all__ = [
    "LIBRARY_ADAPTER_PREFLIGHT_HASH_DOMAIN",
    "commit_library_adapter_publication",
    "library_adapter_sync_payload",
    "library_adapter_write_owner",
    "prepare_library_adapter_publication",
]
