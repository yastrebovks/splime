"""Visibility helpers for local interpreter substitution."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

INTERPRETER_RESOLUTION_KEY = "interpreter_resolution"
INTERPRETER_SUBSTITUTION_KEY = "interpreter_substitution"

_PYTHON_VERSION_RE = re.compile(r"(?P<major>\d+)\.(?P<minor>\d+)")


def interpreter_substitution_from_resolution(
    resolution: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Return the public substitution payload for a resolver decision."""

    if resolution is None or not resolution.get("substituted"):
        return None
    payload = {
        "authored_python": resolution.get("authored_python"),
        "authored_python_version": resolution.get("authored_python_version"),
        "resolved_python": resolution.get("resolved_python"),
        "resolved_python_version": resolution.get("resolved_python_version"),
        "reason": resolution.get("reason"),
    }
    reason_detail = resolution.get("reason_detail")
    if reason_detail is not None:
        payload["reason_detail"] = reason_detail
    payload["minor_mismatch"] = python_minor_mismatch(
        payload.get("authored_python_version"),
        payload.get("resolved_python_version"),
    )
    return payload


def environment_record_interpreter_substitution(
    environment_record: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return substitution payload embedded in an environment-build record."""

    spec = environment_record.get("spec")
    if not isinstance(spec, Mapping):
        return None
    resolution = spec.get(INTERPRETER_RESOLUTION_KEY)
    if not isinstance(resolution, Mapping):
        return None
    return interpreter_substitution_from_resolution(resolution)


def python_minor_mismatch(authored_version: Any, resolved_version: Any) -> bool:
    """Return whether two Python versions differ at major/minor granularity."""

    authored = python_major_minor(authored_version)
    resolved = python_major_minor(resolved_version)
    return authored is not None and resolved is not None and authored != resolved


def python_major_minor(version: Any) -> tuple[int, int] | None:
    """Extract ``(major, minor)`` from strings like ``Python 3.13.1``."""

    if not version:
        return None
    match = _PYTHON_VERSION_RE.search(str(version))
    if match is None:
        return None
    return (int(match.group("major")), int(match.group("minor")))
