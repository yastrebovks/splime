"""Public profile capability identifiers and the omission sentinel."""

from __future__ import annotations


class _UnsetType:
    __slots__ = ()

    def __repr__(self) -> str:
        return "UNSET"


UNSET = _UnsetType()
PUBLIC_OBJECT_PROFILE_CAPABILITY = "spl.public_object_profile.v1"
PUBLIC_ADAPTER_PROFILE_CAPABILITY = "spl.public_library_adapter_profile.v1"
PUBLIC_PROFILE_CAPABILITY_VERSION = 1

__all__ = [
    "UNSET",
    "PUBLIC_OBJECT_PROFILE_CAPABILITY",
    "PUBLIC_ADAPTER_PROFILE_CAPABILITY",
    "PUBLIC_PROFILE_CAPABILITY_VERSION",
]
