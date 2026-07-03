"""Deprecated import location — the implementation lives in ``spl.core._common``.

``from spl.core.common import Deployment, lift`` keeps working through 0.2.x,
but the canonical spelling is ``from spl import Deployment, lift``.  This shim
warns once on import and will be removed in 0.3.0 (see
``docs/migration-0.2.0.md``).
"""

from __future__ import annotations

from typing import Any

from spl._deprecate import warn_deprecated_import
from spl.core import _common as _impl
from spl.core._common import (  # noqa: F401 - legacy re-exports, kept intentionally.
    Deployment as Deployment,
    PipelineBuilder as PipelineBuilder,
    Run as Run,
    decode as decode,
    encode as encode,
    lift as lift,
)

# Keep ``from spl.core.common import *`` scoped to the curated legacy surface
# (without ``__all__`` the star-import would also leak ``Any`` and
# ``warn_deprecated_import`` into the caller's namespace).
__all__ = [
    "Deployment",
    "PipelineBuilder",
    "Run",
    "decode",
    "encode",
    "lift",
]

warn_deprecated_import("spl.core.common", "spl")


def __getattr__(name: str) -> Any:
    """Delegate remaining lookups so legacy code keeps working unchanged."""

    return getattr(_impl, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_impl)))
