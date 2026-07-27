"""Shared validation for timeout values that must be safe for runtime APIs."""

from __future__ import annotations

import math
from enum import StrEnum


class TimeoutDomain(StrEnum):
    """Supported numeric domains for timeout settings."""

    FINITE = "finite"
    NON_NEGATIVE = "non-negative"
    POSITIVE = "positive"


def validate_timeout_seconds(
    value: object,
    *,
    name: str,
    domain: TimeoutDomain,
    allow_none: bool,
) -> float | None:
    """Return ``value`` as a finite float when it satisfies ``domain``."""

    requirement = {
        TimeoutDomain.FINITE: "a finite number",
        TimeoutDomain.NON_NEGATIVE: "a finite non-negative number",
        TimeoutDomain.POSITIVE: "positive and finite",
    }[domain]
    suffix = " or None" if allow_none else ""
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{name} must be {requirement}")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be {requirement}{suffix}")
    try:
        normalized = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{name} must be {requirement}{suffix}") from exc
    invalid = (
        not math.isfinite(normalized)
        or (domain is TimeoutDomain.NON_NEGATIVE and normalized < 0)
        or (domain is TimeoutDomain.POSITIVE and normalized <= 0)
    )
    if invalid:
        raise ValueError(f"{name} must be {requirement}{suffix}")
    return normalized
