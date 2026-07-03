"""Uniform deprecation warnings for the 0.2.0 API cleanup (WP-07).

Central helper so every deprecated path speaks with one voice and tests can
match one message shape.  See ``docs/migration-0.2.0.md`` for the old → new
table and the removal schedule.
"""

from __future__ import annotations

import warnings


def warn_deprecated(old: str, new: str, *, stacklevel: int = 3) -> None:
    """Emit a uniform ``DeprecationWarning`` pointing callers to ``new``.

    ``stacklevel=3`` attributes the warning to the caller of the deprecated
    API (helper frame + deprecated function frame are skipped).
    """

    warnings.warn(
        f"{old} is deprecated; use {new} instead.",
        DeprecationWarning,
        stacklevel=stacklevel,
    )


def warn_deprecated_import(old_module: str, new_module: str) -> None:
    """Emit a ``DeprecationWarning`` from a legacy import-location shim."""

    warnings.warn(
        f"importing from {old_module} is deprecated; "
        f"import from {new_module} instead. "
        f"The {old_module} shim will be removed in 0.3.0.",
        DeprecationWarning,
        stacklevel=3,
    )
