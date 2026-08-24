"""Closed, browser-independent metadata contract for trusted local clients.

This module belongs to the daemon.  It deliberately has no dependency on an
IDE, Jupyter, or a browser-facing protocol package.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from importlib import metadata as importlib_metadata
from typing import Any

from spl.core import json_contract as m_json_contract
from spl.core.runtime_port_adapters import (
    RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY,
    RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY_VERSION,
    RUNTIME_PORT_ADAPTERS_CAPABILITY,
    RUNTIME_PORT_ADAPTERS_CAPABILITY_VERSION,
)
from spl.core.library_adapters import (
    LIBRARY_ADAPTER_CAPABILITY_VERSION,
    LIBRARY_ADAPTER_CATALOG_CAPABILITY,
    LIBRARY_ADAPTER_PUBLISH_CAPABILITY,
    RUNTIME_LIBRARY_ADAPTER_REF_CAPABILITY,
)
from spl.core.publications import (
    PUBLIC_ADAPTER_PROFILE_CAPABILITY,
    PUBLIC_OBJECT_PROFILE_CAPABILITY,
    PUBLIC_PROFILE_CAPABILITY_VERSION,
)
from spl.daemon.guarded_local_run import (
    GUARDED_LOCAL_RUN_CAPABILITY,
    GUARDED_LOCAL_RUN_CAPABILITY_VERSION,
)
from spl.daemon.browser_adapter_run import (
    ADAPTER_RUN_CAPABILITY,
    ADAPTER_RUN_CAPABILITY_VERSION,
)
from spl.daemon.home_lock import DaemonInstanceIdentity
from spl.daemon.lifecycle import LIFECYCLE_CAPABILITY, LIFECYCLE_CAPABILITY_VERSION
from spl.daemon.prepared_validation import (
    PREPARED_PUBLICATION_CAPABILITY,
    PREPARED_PUBLICATION_CAPABILITY_VERSION,
    PREPARED_VALIDATION_CAPABILITY,
    PREPARED_VALIDATION_CAPABILITY_VERSION,
)
from spl.daemon.runtime_adapter_registry import (
    RUNTIME_ADAPTER_REGISTRY_CAPABILITY,
    RUNTIME_ADAPTER_REGISTRY_CAPABILITY_VERSION,
)
from spl.daemon.routes.source_analysis import (
    SOURCE_ANALYSIS_CAPABILITY,
    SOURCE_ANALYSIS_CAPABILITY_VERSION,
)

DAEMON_CAPABILITIES_SCHEMA = "spl.daemon.meta-capabilities"
DAEMON_CAPABILITIES_SCHEMA_VERSION = 1
DAEMON_PROTOCOL_MINIMUM = 1
DAEMON_PROTOCOL_MAXIMUM = 1
DAEMON_DISTRIBUTION = "splime"
DAEMON_METADATA_UNAVAILABLE_CODE = "daemon_metadata_unavailable"
DAEMON_METADATA_MAX_BYTES = 8 * 1024
MAX_SAFE_INTEGER = 9_007_199_254_740_991

LOCAL_CAPABILITY_VERSIONS: dict[str, int] = {
    "connected.credentials.reveal": 1,
    "object.local.list": 1,
    "object.local.detail": 1,
    "object.local.signature": 1,
    SOURCE_ANALYSIS_CAPABILITY: SOURCE_ANALYSIS_CAPABILITY_VERSION,
    PREPARED_PUBLICATION_CAPABILITY: PREPARED_PUBLICATION_CAPABILITY_VERSION,
    PREPARED_VALIDATION_CAPABILITY: PREPARED_VALIDATION_CAPABILITY_VERSION,
    "run.local.create": 1,
    RUNTIME_ADAPTER_REGISTRY_CAPABILITY: RUNTIME_ADAPTER_REGISTRY_CAPABILITY_VERSION,
    RUNTIME_PORT_ADAPTERS_CAPABILITY: RUNTIME_PORT_ADAPTERS_CAPABILITY_VERSION,
    RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY: RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY_VERSION,
    GUARDED_LOCAL_RUN_CAPABILITY: GUARDED_LOCAL_RUN_CAPABILITY_VERSION,
    ADAPTER_RUN_CAPABILITY: ADAPTER_RUN_CAPABILITY_VERSION,
    LIBRARY_ADAPTER_CATALOG_CAPABILITY: LIBRARY_ADAPTER_CAPABILITY_VERSION,
    LIBRARY_ADAPTER_PUBLISH_CAPABILITY: LIBRARY_ADAPTER_CAPABILITY_VERSION,
    RUNTIME_LIBRARY_ADAPTER_REF_CAPABILITY: LIBRARY_ADAPTER_CAPABILITY_VERSION,
    PUBLIC_OBJECT_PROFILE_CAPABILITY: PUBLIC_PROFILE_CAPABILITY_VERSION,
    PUBLIC_ADAPTER_PROFILE_CAPABILITY: PUBLIC_PROFILE_CAPABILITY_VERSION,
    "run.local.list": 1,
    "run.local.detail": 1,
    "run.remote.detail": 1,
    "run.remote.events": 1,
    "run.remote.list": 1,
    "run.remote.preflight": 1,
    LIFECYCLE_CAPABILITY: LIFECYCLE_CAPABILITY_VERSION,
}

_DOCUMENT_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "release",
        "build",
        "protocol",
        "instance",
        "observed_at",
        "capabilities",
    }
)
_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,63}$")
_CAPABILITY_ID_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+){0,15}$")
_MACHINE_REASON_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
_STARTED_AT_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$")
_OBSERVED_AT_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


class DaemonMetadataUnavailable(RuntimeError):
    """Raised when the daemon cannot prove the required live metadata."""


def build_daemon_capabilities_document(
    identity: DaemonInstanceIdentity | None,
    *,
    release_version: str | None,
    observed_at: str | None = None,
    forbidden_values: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build and validate the exact v1 daemon metadata response."""

    if identity is None:
        raise DaemonMetadataUnavailable("live daemon identity is unavailable")

    if release_version is None:
        raise DaemonMetadataUnavailable("installed daemon release metadata is unavailable")
    if not _VERSION_PATTERN.fullmatch(release_version):
        raise DaemonMetadataUnavailable("installed daemon release metadata is invalid")

    document = {
        "schema": DAEMON_CAPABILITIES_SCHEMA,
        "schema_version": DAEMON_CAPABILITIES_SCHEMA_VERSION,
        "release": {
            "distribution": DAEMON_DISTRIBUTION,
            "version": release_version,
            "evidence": {
                "kind": "installed_distribution_metadata",
            },
        },
        "build": {
            "revision": None,
            "evidence": {
                "kind": "unknown",
                "reason": "build_revision_not_embedded",
            },
        },
        "protocol": {
            "minimum": DAEMON_PROTOCOL_MINIMUM,
            "maximum": DAEMON_PROTOCOL_MAXIMUM,
        },
        "instance": {
            "instance_id": identity.instance_id,
            "generation": identity.generation,
            "started_at": identity.started_at,
        },
        "observed_at": _utc_now() if observed_at is None else observed_at,
        "capabilities": {
            capability_id: {
                "state": "supported",
                "version": version,
                "reason": None,
            }
            for capability_id, version in LOCAL_CAPABILITY_VERSIONS.items()
        },
    }
    validate_daemon_capabilities_document(document)
    serialized = _serialized_document(document)
    if any(value and value in serialized for value in forbidden_values):
        raise DaemonMetadataUnavailable("daemon metadata intersects a forbidden value")
    return document


def load_installed_daemon_release(
    distribution: importlib_metadata.Distribution | None = None,
) -> str:
    """Resolve the installed release once for the lifetime of a daemon app."""

    installed = distribution
    if installed is None:
        try:
            installed = importlib_metadata.distribution(DAEMON_DISTRIBUTION)
        except importlib_metadata.PackageNotFoundError as exc:
            raise DaemonMetadataUnavailable("installed daemon release metadata is unavailable") from exc
    version = installed.version
    if not isinstance(version, str) or not _VERSION_PATTERN.fullmatch(version):
        raise DaemonMetadataUnavailable("installed daemon release metadata is invalid")
    return version


def validate_daemon_capabilities_document(document: Mapping[str, Any]) -> None:
    """Validate the closed daemon-owned v1 document and cross-field rules."""

    _require_exact_keys(document, _DOCUMENT_KEYS, "document")
    if document["schema"] != DAEMON_CAPABILITIES_SCHEMA:
        raise ValueError("daemon metadata schema is not recognized")
    _require_exact_positive_int(
        document["schema_version"],
        DAEMON_CAPABILITIES_SCHEMA_VERSION,
        "schema_version",
    )

    release = _require_mapping(document["release"], "release")
    _require_exact_keys(release, frozenset({"distribution", "version", "evidence"}), "release")
    if release["distribution"] != DAEMON_DISTRIBUTION:
        raise ValueError("release distribution is not recognized")
    if not isinstance(release["version"], str) or not _VERSION_PATTERN.fullmatch(release["version"]):
        raise ValueError("release version is invalid")
    release_evidence = _require_mapping(release["evidence"], "release.evidence")
    _require_exact_keys(release_evidence, frozenset({"kind"}), "release.evidence")
    if release_evidence["kind"] != "installed_distribution_metadata":
        raise ValueError("release evidence is not installed distribution metadata")

    build = _require_mapping(document["build"], "build")
    _require_exact_keys(build, frozenset({"revision", "evidence"}), "build")
    build_evidence = _require_mapping(build["evidence"], "build.evidence")
    _require_exact_keys(build_evidence, frozenset({"kind", "reason"}), "build.evidence")
    revision = build["revision"]
    evidence_kind = build_evidence["kind"]
    evidence_reason = build_evidence["reason"]
    if evidence_kind != "unknown" or revision is not None or evidence_reason != "build_revision_not_embedded":
        raise ValueError("unknown build evidence is contradictory")

    protocol = _require_mapping(document["protocol"], "protocol")
    _require_exact_keys(protocol, frozenset({"minimum", "maximum"}), "protocol")
    _require_exact_positive_int(protocol["minimum"], DAEMON_PROTOCOL_MINIMUM, "protocol.minimum")
    _require_exact_positive_int(protocol["maximum"], DAEMON_PROTOCOL_MAXIMUM, "protocol.maximum")

    instance = _require_mapping(document["instance"], "instance")
    _require_exact_keys(instance, frozenset({"instance_id", "generation", "started_at"}), "instance")
    instance_id = instance["instance_id"]
    if (
        not isinstance(instance_id, str)
        or not 16 <= len(instance_id) <= 64
        or not instance_id.isascii()
        or not instance_id.isalnum()
    ):
        raise ValueError("instance_id is invalid")
    generation = instance["generation"]
    if isinstance(generation, bool) or not isinstance(generation, int) or not 1 <= generation <= MAX_SAFE_INTEGER:
        raise ValueError("generation must be a positive safe integer")
    raw_started_at = instance["started_at"]
    if not isinstance(raw_started_at, str) or not _STARTED_AT_PATTERN.fullmatch(raw_started_at):
        raise ValueError("instance.started_at is not a bounded UTC timestamp")
    started_at = _parse_utc_timestamp(raw_started_at, "instance.started_at")

    observed_at = document["observed_at"]
    if not isinstance(observed_at, str) or not _OBSERVED_AT_PATTERN.fullmatch(observed_at):
        raise ValueError("observed_at is not canonical UTC milliseconds")
    observed = _parse_utc_timestamp(observed_at, "observed_at")
    started_at_milliseconds = started_at.replace(microsecond=(started_at.microsecond // 1000) * 1000)
    if observed < started_at_milliseconds:
        raise ValueError("observed_at precedes instance.started_at")

    capabilities = _require_mapping(document["capabilities"], "capabilities")
    if not set(LOCAL_CAPABILITY_VERSIONS) <= set(capabilities) or len(capabilities) > 64:
        raise ValueError("capabilities do not contain the required bounded catalog")
    for capability_id, raw_capability in capabilities.items():
        if (
            not isinstance(capability_id, str)
            or len(capability_id) > 128
            or not _CAPABILITY_ID_PATTERN.fullmatch(capability_id)
        ):
            raise ValueError("capability identifier is invalid")
        capability = _require_mapping(raw_capability, f"capabilities.{capability_id}")
        _require_exact_keys(
            capability,
            frozenset({"state", "version", "reason"}),
            f"capabilities.{capability_id}",
        )
        expected_version = LOCAL_CAPABILITY_VERSIONS.get(capability_id)
        if expected_version is not None:
            if capability["state"] != "supported" or capability["reason"] is not None:
                raise ValueError(f"{capability_id} support state is contradictory")
            _require_exact_positive_int(
                capability["version"],
                expected_version,
                f"capabilities.{capability_id}.version",
            )
        else:
            _validate_unknown_capability(capability, capability_id)

    encoded = _serialized_document(document).encode("utf-8")
    if len(encoded) > DAEMON_METADATA_MAX_BYTES:
        raise ValueError("daemon metadata exceeds the v1 size limit")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _serialized_document(document: Mapping[str, Any]) -> str:
    return m_json_contract.dumps(
        dict(document),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        separators=None,
    )


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return value


def _require_exact_keys(value: Mapping[str, Any], expected: frozenset[str], name: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{name} fields do not match the v1 contract")


def _require_exact_positive_int(value: Any, expected: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value != expected:
        raise ValueError(f"{name} must be {expected}")


def _validate_unknown_capability(capability: Mapping[str, Any], capability_id: str) -> None:
    state = capability["state"]
    version = capability["version"]
    reason = capability["reason"]
    if state == "supported":
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or not 1 <= version <= MAX_SAFE_INTEGER
            or reason is not None
        ):
            raise ValueError(f"{capability_id} support state is contradictory")
        return
    if state == "unsupported":
        if version is not None or not isinstance(reason, str) or not _MACHINE_REASON_PATTERN.fullmatch(reason):
            raise ValueError(f"{capability_id} support state is contradictory")
        return
    raise ValueError(f"{capability_id} support state is not recognized")


def _parse_utc_timestamp(value: Any, name: str) -> datetime:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise ValueError(f"{name} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{name} must be UTC")
    return parsed
