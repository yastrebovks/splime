"""Deprecated import location — the implementation lives in ``spl._client``.

``from spl.client import SPLClient`` keeps working through 0.2.x, but the
canonical spelling is ``from spl import SPLClient``.  This shim warns once on
import and will be removed in 0.3.0 (see ``docs/migration-0.2.0.md``).
"""

from __future__ import annotations

from typing import Any

from spl import _client as _impl
from spl._client import (  # noqa: F401 - legacy re-exports, kept intentionally.
    ObjectCatalog as ObjectCatalog,
    ObjectList as ObjectList,
    ObjectScope as ObjectScope,
    ObjectTable as ObjectTable,
    OfflinePolicy as OfflinePolicy,
    ProgressOption as ProgressOption,
    PublishedObject as PublishedObject,
    RemoteResult as RemoteResult,
    RemoteRun as RemoteRun,
    RunSource as RunSource,
    SPLClient as SPLClient,
    build_runtime_config as build_runtime_config,
    export_object_to_yaml as export_object_to_yaml,
    export_objects_to_yaml as export_objects_to_yaml,
    prepare_export_object as prepare_export_object,
    read_yaml_input as read_yaml_input,
)
from spl._deprecate import warn_deprecated_import

# Keep ``from spl.client import *`` scoped to the curated legacy surface
# (without ``__all__`` the star-import would also leak ``Any`` and
# ``warn_deprecated_import`` into the caller's namespace).
__all__ = [
    "ObjectCatalog",
    "ObjectList",
    "ObjectScope",
    "ObjectTable",
    "OfflinePolicy",
    "ProgressOption",
    "PublishedObject",
    "RemoteResult",
    "RemoteRun",
    "RunSource",
    "SPLClient",
    "build_runtime_config",
    "export_object_to_yaml",
    "export_objects_to_yaml",
    "prepare_export_object",
    "read_yaml_input",
]

warn_deprecated_import("spl.client", "spl")


def __getattr__(name: str) -> Any:
    """Delegate remaining lookups so legacy code keeps working unchanged."""

    return getattr(_impl, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_impl)))
