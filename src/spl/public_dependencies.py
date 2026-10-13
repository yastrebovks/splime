"""Inspect publication provenance in this interpreter, without importing packages.

This module has no installation or enforcement behavior. Strict runtime locks do
not use it. An installed distribution is availability evidence, not a promise
that all of its imports (or a different package version) will work.
"""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from importlib import metadata
from typing import Any

from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

LOGGER = logging.getLogger(__name__)
_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?\Z")
MAX_DISPLAYED_DEPENDENCIES = 10


def _display_version(value: str | None) -> str:
    # Keep unusual metadata from turning one preview row into an unbounded log.
    text = " ".join(value.split()) if value else "unknown"
    return text if len(text) <= 80 else text[:77] + "..."


def normalize_record(package: Any, version: Any) -> tuple[str, str]:
    if not isinstance(package, str) or not _NAME.fullmatch(package):
        raise ValueError("dependency distribution name is missing or malformed")
    if not isinstance(version, str) or not version.strip():
        raise ValueError(f"{package}: publication version is missing")
    try:
        Version(version)
    except InvalidVersion as exc:
        raise ValueError(f"{package}: publication version {version!r} is malformed") from exc
    return canonicalize_name(package), version


@dataclass(frozen=True)
class DependencyDiagnostic:
    package: str
    recorded_version: str | None
    sources: tuple[str, ...]
    status: str
    installed_version: str | None = None
    loaded_versions: tuple[tuple[str, str | None], ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class DependencyReport:
    dependencies: tuple[DependencyDiagnostic, ...]

    @property
    def missing(self) -> tuple[DependencyDiagnostic, ...]:
        return tuple(item for item in self.dependencies if item.status == "missing")

    def as_dict(self) -> dict[str, Any]:
        return {"dependencies": [asdict(item) for item in self.dependencies]}

    def as_list(self) -> list[dict[str, Any]]:
        """Return every version comparison, without source paths or truncation."""
        return [
            {
                "package": item.package,
                "recorded_version": item.recorded_version,
                "installed_version": item.installed_version,
                "loaded_versions": [{"module": name, "version": version} for name, version in item.loaded_versions],
                "status": item.status,
            }
            for item in self.dependencies
        ]

    @staticmethod
    def preview(items: Sequence[DependencyDiagnostic]) -> str:
        """Render a bounded, source-free preview; exports always remain complete."""
        rows = []
        for item in items[:MAX_DISPLAYED_DEPENDENCIES]:
            installed = (
                "not installed" if item.status == "missing" else f"installed {_display_version(item.installed_version)}"
            )
            row = (
                f"  {_display_version(item.package)}: published {_display_version(item.recorded_version)}; {installed}"
            )
            if item.loaded_versions:
                versions = list(dict.fromkeys(version for _, version in item.loaded_versions))
                row += "; loaded " + ", ".join(_display_version(version) for version in versions[:3])
                if len(versions) > 3:
                    row += f" (+{len(versions) - 3} more)"
            rows.append(row)
        if len(items) > MAX_DISPLAYED_DEPENDENCIES:
            rows.append(f"  ... {len(items) - MAX_DISPLAYED_DEPENDENCIES} more entries.")
        return "\n".join(rows)

    def warn(self) -> None:
        items = tuple(
            item
            for item in self.dependencies
            if item.status in {"mismatched", "unverifiable", "invalid"} or item.detail
        )
        if not items:
            return
        restart = False
        for item in items:
            for _, version in item.loaded_versions:
                try:
                    restart |= Version(version or "") != Version(item.installed_version or "")
                except InvalidVersion:
                    pass
        LOGGER.warning(
            "SPH dependency differences (%s entries); continuing with local packages.\n%s\n"
            "Full list: client.inspect_dependencies(ref).%s",
            len(items),
            self.preview(items),
            " Loaded versions differ from installed packages; restart the kernel to use installed code."
            if restart
            else "",
        )


def inspect_dependencies(records: Sequence[Mapping[str, Any]]) -> DependencyReport:
    """Collect all diagnostics; never execute an import to discover a version.

    A loaded module's literal ``__version__`` is reported only when installed
    metadata associates its top-level import name with exactly one distribution.
    It is kept separate from on-disk metadata and never treated as an upgrade.
    """
    grouped: dict[tuple[str, str], set[str]] = {}
    diagnostics: list[DependencyDiagnostic] = []
    for record in records:
        try:
            package, version = normalize_record(record.get("package"), record.get("version"))
            sources = record.get("sources", ["root"])
            if not isinstance(sources, (list, tuple)) or not all(isinstance(s, str) and s for s in sources):
                raise ValueError("dependency provenance is malformed")
            grouped.setdefault((package, version), set()).update(sources)
        except (AttributeError, ValueError) as exc:
            diagnostics.append(
                DependencyDiagnostic(
                    package=str(record.get("package", "unknown")) if isinstance(record, Mapping) else "unknown",
                    recorded_version=None,
                    sources=("metadata",),
                    status="invalid",
                    detail=str(exc),
                )
            )
    owners = metadata.packages_distributions()
    local: dict[str, tuple[str | None, bool, tuple[tuple[str, str | None], ...]]] = {}
    for package, _ in grouped:
        if package in local:
            continue
        loaded_modules: list[tuple[str, str | None]] = []
        for name, distributions in sorted(owners.items()):
            if {canonicalize_name(d) for d in distributions} != {package}:
                continue
            module = sys.modules.get(name)
            if module is not None:
                value = vars(module).get("__version__")
                loaded_modules.append((name, value if type(value) is str else None))
        installed: str | None
        try:
            installed = metadata.version(package)
            available = True
        except metadata.PackageNotFoundError:
            installed, available = None, bool(loaded_modules)
        local[package] = (installed, available, tuple(loaded_modules))
    for (package, recorded), sources in sorted(grouped.items()):
        installed, available, loaded_versions = local[package]
        status = "present" if available else "missing"
        detail = []
        if available:
            try:
                status = "present" if Version(installed or "") == Version(recorded) else "mismatched"
            except InvalidVersion:
                status = "unverifiable"
                detail.append("The installed version cannot be verified.")
            for name, loaded_version in loaded_versions:
                try:
                    if Version(loaded_version or "") != Version(recorded):
                        status = "mismatched"
                    if installed and Version(loaded_version or "") != Version(installed):
                        detail.append(
                            f"Already-loaded {name} differs from disk; restart the kernel to use installed code."
                        )
                except InvalidVersion:
                    if status == "present":
                        status = "unverifiable"
                    detail.append(f"Already-loaded {name} has no verifiable version; it will not be reloaded.")
        diagnostics.append(
            DependencyDiagnostic(
                package,
                recorded,
                tuple(sorted(sources)),
                status,
                installed,
                loaded_versions,
                " ".join(detail),
            )
        )
    return DependencyReport(tuple(diagnostics))
