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

    def warn(self) -> None:
        for item in self.dependencies:
            if item.status not in {"mismatched", "unverifiable", "invalid"} and not item.detail:
                continue
            local = item.installed_version or "unverifiable"
            loaded = "; ".join(f"loaded {name}={value or 'unverifiable'}" for name, value in item.loaded_versions)
            LOGGER.warning(
                "%s was recorded as %s at publication (%s); installed distribution metadata reports %s%s. "
                "%s Execution will continue with the local package if available.",
                item.package,
                item.recorded_version or "unverifiable",
                ", ".join(item.sources),
                local,
                f"; {loaded}" if loaded else "",
                item.detail,
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
