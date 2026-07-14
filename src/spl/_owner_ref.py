"""Syntactic normalization for SDK user-owner references."""

from __future__ import annotations

from typing import Any, overload


@overload
def normalize_owner_ref(value: None) -> None: ...


@overload
def normalize_owner_ref(value: str) -> str: ...


def normalize_owner_ref(value: object) -> str | None:
    """Return one canonical-id/handle reference unchanged after validation.

    The SDK validates only the unambiguous reference shape.  It never looks up
    a handle: the central server, or the daemon at a local-storage boundary,
    resolves the reference to a canonical user id.
    """

    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("owner must be a canonical user id or @handle")
    if not value.strip():
        raise ValueError("owner must be a canonical user id or @handle")
    marker_count = value.count("@")
    if marker_count > 1 or marker_count == 1 and not value.startswith("@"):
        raise ValueError("owner must be a canonical user id or one leading @handle")
    if value == "@":
        raise ValueError("owner handle must not be empty")
    return value


def canonical_owner_from_response(requested: str | None, response_owner: Any) -> str | None:
    """Prefer the canonical owner returned by a daemon signature round trip."""

    normalized = normalize_owner_ref(requested)
    if response_owner is not None:
        if not isinstance(response_owner, str):
            raise TypeError("remote signature owner id must be a string")
        canonical = normalize_owner_ref(response_owner)
        if canonical is not None and canonical.startswith("@"):
            raise ValueError("remote signature returned a handle instead of a canonical owner id")
        return canonical
    if normalized is not None and normalized.startswith("@"):
        raise ValueError("remote signature did not return a canonical owner id for the requested handle")
    return normalized
