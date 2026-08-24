"""Canonical parsing and formatting for public ``splime://`` references."""

from __future__ import annotations

import re
from dataclasses import dataclass


_HANDLE_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$")
# Kept byte-for-byte aligned with the historical daemon ``validate_name``
# grammar; all-dot identities are rejected by the shared semantic rule below.
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass(frozen=True)
class PublicObjectRef:
    """One canonical public Object identity with an optional numeric version."""

    owner: str
    library: str
    name: str
    version: int | None = None

    def __post_init__(self) -> None:
        _validate_handle(self.owner)
        _validate_name(self.library, segment="library")
        _validate_name(self.name, segment="object")
        if self.version is not None and (
            isinstance(self.version, bool) or not isinstance(self.version, int) or self.version < 1
        ):
            raise ValueError("invalid public reference version segment")

    @property
    def pinned(self) -> bool:
        return self.version is not None

    def __str__(self) -> str:
        return format_public_ref(self)


def parse_public_ref(value: str) -> PublicObjectRef:
    """Parse one closed public reference without performing network I/O."""

    if not isinstance(value, str):
        raise TypeError("public reference must be a string")
    if any(marker in value for marker in ("?", "#", "%", "\\")):
        raise ValueError("invalid public reference encoded/query/fragment segment")
    prefix = "splime://"
    if not value.startswith(prefix):
        raise ValueError("invalid public reference scheme segment; expected splime://")
    segments = value.removeprefix(prefix).split("/")
    if len(segments) != 3:
        raise ValueError("invalid public reference path segment count")
    raw_owner, library, raw_name = segments
    if not raw_owner.startswith("@") or raw_owner.count("@") != 1:
        raise ValueError("invalid public reference owner segment")
    owner = raw_owner[1:]
    _validate_handle(owner)
    _validate_name(library, segment="library")
    if "@" in raw_name:
        if raw_name.count("@") != 1:
            raise ValueError("invalid public reference version segment")
        name, version_text = raw_name.rsplit("@", 1)
        if re.fullmatch(r"[1-9][0-9]*", version_text) is None:
            raise ValueError("invalid public reference version segment")
        version = int(version_text)
    else:
        name = raw_name
        version = None
    _validate_name(name, segment="object")
    return PublicObjectRef(
        owner=owner,
        library=library,
        name=name,
        version=version,
    )


def format_public_ref(reference: PublicObjectRef) -> str:
    """Return the canonical URI for a validated public reference."""

    if not isinstance(reference, PublicObjectRef):
        raise TypeError("reference must be PublicObjectRef")
    suffix = "" if reference.version is None else f"@{reference.version}"
    text = f"splime://@{reference.owner}/{reference.library}/{reference.name}{suffix}"
    parsed = parse_public_ref(text)
    if parsed != reference:
        raise ValueError("public reference contains a non-canonical segment")
    return text


def _validate_handle(value: str) -> None:
    if not isinstance(value, str) or _HANDLE_PATTERN.fullmatch(value) is None:
        raise ValueError("invalid public reference owner segment")


def _validate_name(value: str, *, segment: str) -> None:
    if not isinstance(value, str) or _NAME_PATTERN.fullmatch(value) is None or set(value) == {"."}:
        raise ValueError(f"invalid public reference {segment} segment")


__all__ = ["PublicObjectRef", "format_public_ref", "parse_public_ref"]
