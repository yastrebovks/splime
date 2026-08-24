"""Dependency-free daemon identifier validation."""

from __future__ import annotations

import re


NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def validate_name(name: str) -> str:
    """Validate a registry-safe name and return it unchanged."""

    # Keep this rule in sync with spl.daemon.spl_free_runner.validate_name;
    # the runner duplicates it intentionally to stay stdlib-only.
    if not NAME_PATTERN.fullmatch(name) or set(name) == {"."}:
        raise ValueError("name must contain only letters, digits, underscore, dash, and dot, and not only dots")
    return name
