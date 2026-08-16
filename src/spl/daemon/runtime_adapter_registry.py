"""Closed browser-safe projection of the built-in Run adapter registry."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from importlib import metadata as importlib_metadata
from typing import Any, Final

from spl.adapters import BUILTIN_ADAPTER_IDS
from spl.core import json_contract as m_json_contract
from spl.core.runtime_port_adapters import (
    MAX_CUSTOM_BUNDLE_BYTES,
    MAX_RUNTIME_ADAPTER_BINDINGS,
    MAX_RUNTIME_INPUT_ARTIFACTS,
    MAX_RUNTIME_INPUT_BYTES,
    MAX_RUNTIME_INPUT_TOTAL_BYTES,
    built_in_registry_descriptor,
)
from spl.daemon.home_lock import DaemonInstanceIdentity

RUNTIME_ADAPTER_REGISTRY_SCHEMA: Final = "spl.daemon.runtime-adapters"
RUNTIME_ADAPTER_REGISTRY_SCHEMA_VERSION: Final = 1
RUNTIME_ADAPTER_REGISTRY_CAPABILITY: Final = "spl.ide.runtime_adapter_registry.v1"
RUNTIME_ADAPTER_REGISTRY_CAPABILITY_VERSION: Final = 1
RUNTIME_ADAPTER_REGISTRY_UNAVAILABLE_CODE: Final = "runtime_adapter_registry_unavailable"
RUNTIME_ADAPTER_REGISTRY_MAX_BYTES: Final = 64 * 1024
DAEMON_CONTROL_ENVIRONMENT_SCOPE: Final = "daemon_control_environment"
MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991

_DOCUMENT_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "capability",
        "instance",
        "observed_at",
        "registry_revision",
        "environment",
        "policy",
        "limits",
        "adapters",
    }
)
_ADAPTER_KEYS = frozenset(
    {
        "id",
        "format",
        "label",
        "preferred_extension",
        "media_type",
        "directions",
        "transport",
        "semantic_types",
        "semantic_categories",
        "system_default",
        "required_distributions",
        "builtin",
        "custom",
        "custom_code",
        "local_availability",
        "remote_availability",
    }
)
_LIMIT_KEYS = frozenset(
    {
        "max_bindings",
        "max_input_artifacts",
        "max_input_bytes",
        "max_input_total_bytes",
        "max_custom_bundle_bytes",
    }
)
_FORBIDDEN_KEYS = frozenset(
    {
        "api_token",
        "bundle",
        "db",
        "db_path",
        "function",
        "functions",
        "home",
        "load",
        "path",
        "pid",
        "save",
        "secret",
        "source",
        "token",
    }
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PACKAGE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,159}$")
_STARTED_AT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$")
_OBSERVED_AT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


class RuntimeAdapterRegistryUnavailable(RuntimeError):
    """Raised when the daemon cannot safely project its built-in registry."""


def build_runtime_adapter_registry_document(
    identity: DaemonInstanceIdentity | None,
    *,
    remote_custom_adapter_execution_enabled: bool,
    observed_at: str | None = None,
    forbidden_values: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build and validate one live v1 registry document."""

    if identity is None:
        raise RuntimeAdapterRegistryUnavailable("live daemon identity is unavailable")

    stable_adapters = [built_in_registry_descriptor(adapter_id) for adapter_id in sorted(BUILTIN_ADAPTER_IDS)]
    required_names = sorted(
        {name for adapter in stable_adapters for name in adapter["required_distributions"]},
        key=_canonical_package,
    )
    distribution_states = {name: _distribution_state(name) for name in required_names}
    adapters: list[dict[str, Any]] = []
    for stable in stable_adapters:
        required = [dict(distribution_states[name]) for name in stable["required_distributions"]]
        missing = any(item["state"] == "missing" for item in required)
        adapters.append(
            {
                **stable,
                "required_distributions": required,
                "local_availability": {
                    "scope": DAEMON_CONTROL_ENVIRONMENT_SCOPE,
                    "state": "missing_required_distributions" if missing else "available",
                    "reason": "required_distribution_missing" if missing else None,
                },
                "remote_availability": {
                    "state": "requires_target_preflight",
                    "reason": "target_environment_not_observed",
                },
            }
        )

    limits = _registry_limits()
    document = {
        "schema": RUNTIME_ADAPTER_REGISTRY_SCHEMA,
        "schema_version": RUNTIME_ADAPTER_REGISTRY_SCHEMA_VERSION,
        "capability": {
            "id": RUNTIME_ADAPTER_REGISTRY_CAPABILITY,
            "version": RUNTIME_ADAPTER_REGISTRY_CAPABILITY_VERSION,
        },
        "instance": {
            "instance_id": identity.instance_id,
            "generation": identity.generation,
            "started_at": identity.started_at,
        },
        "observed_at": _utc_now() if observed_at is None else observed_at,
        "registry_revision": _registry_revision(stable_adapters, limits),
        "environment": {"scope": DAEMON_CONTROL_ENVIRONMENT_SCOPE},
        "policy": {
            "remote_custom_adapter_execution": {
                "implemented": True,
                "enabled": remote_custom_adapter_execution_enabled,
            }
        },
        "limits": limits,
        "adapters": adapters,
    }
    validate_runtime_adapter_registry_document(document)
    serialized = _serialized_document(document)
    if any(value and value in serialized for value in forbidden_values):
        raise RuntimeAdapterRegistryUnavailable("runtime adapter registry intersects a forbidden value")
    return document


def validate_runtime_adapter_registry_document(document: Mapping[str, Any]) -> None:
    """Validate the exact, bounded and code-free v1 registry response."""

    _require_exact_keys(document, _DOCUMENT_KEYS, "document")
    if document["schema"] != RUNTIME_ADAPTER_REGISTRY_SCHEMA:
        raise ValueError("runtime adapter registry schema is not recognized")
    _require_exact_int(document["schema_version"], RUNTIME_ADAPTER_REGISTRY_SCHEMA_VERSION, "schema_version")

    capability = _require_mapping(document["capability"], "capability")
    _require_exact_keys(capability, frozenset({"id", "version"}), "capability")
    if capability["id"] != RUNTIME_ADAPTER_REGISTRY_CAPABILITY:
        raise ValueError("runtime adapter registry capability is not recognized")
    _require_exact_int(
        capability["version"],
        RUNTIME_ADAPTER_REGISTRY_CAPABILITY_VERSION,
        "capability.version",
    )

    started_at = _validate_instance(document["instance"])
    observed_at = _validate_observed_at(document["observed_at"])
    if observed_at < started_at.replace(microsecond=(started_at.microsecond // 1000) * 1000):
        raise ValueError("observed_at precedes instance.started_at")

    environment = _require_mapping(document["environment"], "environment")
    _require_exact_keys(environment, frozenset({"scope"}), "environment")
    if environment["scope"] != DAEMON_CONTROL_ENVIRONMENT_SCOPE:
        raise ValueError("environment scope is not recognized")

    policy = _require_mapping(document["policy"], "policy")
    _require_exact_keys(policy, frozenset({"remote_custom_adapter_execution"}), "policy")
    remote_custom = _require_mapping(
        policy["remote_custom_adapter_execution"],
        "policy.remote_custom_adapter_execution",
    )
    _require_exact_keys(
        remote_custom,
        frozenset({"implemented", "enabled"}),
        "policy.remote_custom_adapter_execution",
    )
    if remote_custom["implemented"] is not True or type(remote_custom["enabled"]) is not bool:
        raise ValueError("remote custom adapter execution policy is contradictory")

    limits = _require_mapping(document["limits"], "limits")
    _require_exact_keys(limits, _LIMIT_KEYS, "limits")
    expected_limits = _registry_limits()
    if dict(limits) != expected_limits:
        raise ValueError("runtime adapter registry limits do not match daemon admission")

    adapters = document["adapters"]
    if not isinstance(adapters, list) or len(adapters) != len(BUILTIN_ADAPTER_IDS):
        raise ValueError("runtime adapter registry must contain the exact built-in catalog")
    expected_adapters = {
        adapter_id: built_in_registry_descriptor(adapter_id) for adapter_id in sorted(BUILTIN_ADAPTER_IDS)
    }
    observed_ids = [adapter.get("id") if isinstance(adapter, Mapping) else None for adapter in adapters]
    if observed_ids != sorted(BUILTIN_ADAPTER_IDS):
        raise ValueError("runtime adapter registry IDs are not the exact sorted built-in catalog")
    for raw_adapter in adapters:
        adapter = _require_mapping(raw_adapter, "adapter")
        _validate_adapter(adapter, expected_adapters[str(adapter["id"])])

    revision = document["registry_revision"]
    if not isinstance(revision, str) or not _SHA256.fullmatch(revision):
        raise ValueError("registry_revision must be lowercase SHA-256")
    if revision != _registry_revision(list(expected_adapters.values()), expected_limits):
        raise ValueError("registry_revision does not match the stable built-in catalog")

    _validate_redaction(document)
    if len(_serialized_document(document).encode("utf-8")) > RUNTIME_ADAPTER_REGISTRY_MAX_BYTES:
        raise ValueError("runtime adapter registry exceeds the v1 size limit")


def _validate_adapter(adapter: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    _require_exact_keys(adapter, _ADAPTER_KEYS, f"adapter {expected['id']}")
    for key, expected_value in expected.items():
        if key == "required_distributions":
            continue
        if not _same_json_value(adapter[key], expected_value):
            raise ValueError(f"adapter {expected['id']} static metadata does not match the built-in registry")

    required = adapter["required_distributions"]
    expected_names = expected["required_distributions"]
    if not isinstance(required, list) or len(required) != len(expected_names):
        raise ValueError(f"adapter {expected['id']} required distributions do not match the built-in registry")
    observed_names: list[str] = []
    missing = False
    for raw_distribution in required:
        distribution = _require_mapping(raw_distribution, "required distribution")
        _require_exact_keys(distribution, frozenset({"name", "state", "version"}), "required distribution")
        name = distribution["name"]
        if not isinstance(name, str) or not _PACKAGE.fullmatch(name):
            raise ValueError("required distribution name is invalid")
        observed_names.append(name)
        state = distribution["state"]
        version = distribution["version"]
        if state == "installed":
            if not isinstance(version, str) or not _VERSION.fullmatch(version):
                raise ValueError("installed required distribution version is invalid")
        elif state == "missing":
            if version is not None:
                raise ValueError("missing required distribution cannot have a version")
            missing = True
        else:
            raise ValueError("required distribution state is not recognized")
    if observed_names != expected_names:
        raise ValueError(f"adapter {expected['id']} required distributions do not match the built-in registry")

    local = _require_mapping(adapter["local_availability"], "local_availability")
    _require_exact_keys(local, frozenset({"scope", "state", "reason"}), "local_availability")
    expected_local = {
        "scope": DAEMON_CONTROL_ENVIRONMENT_SCOPE,
        "state": "missing_required_distributions" if missing else "available",
        "reason": "required_distribution_missing" if missing else None,
    }
    if dict(local) != expected_local:
        raise ValueError("local adapter availability contradicts required distributions")

    remote = _require_mapping(adapter["remote_availability"], "remote_availability")
    _require_exact_keys(remote, frozenset({"state", "reason"}), "remote_availability")
    if dict(remote) != {
        "state": "requires_target_preflight",
        "reason": "target_environment_not_observed",
    }:
        raise ValueError("remote adapter availability must require target preflight")


def _distribution_state(name: str) -> dict[str, str | None]:
    if not _PACKAGE.fullmatch(name):
        raise RuntimeAdapterRegistryUnavailable("built-in distribution name is invalid")
    try:
        version = importlib_metadata.version(name)
    except importlib_metadata.PackageNotFoundError:
        return {"name": name, "state": "missing", "version": None}
    if not isinstance(version, str) or not _VERSION.fullmatch(version):
        raise RuntimeAdapterRegistryUnavailable("installed distribution metadata is invalid")
    return {"name": name, "state": "installed", "version": version}


def _registry_limits() -> dict[str, int]:
    return {
        "max_bindings": MAX_RUNTIME_ADAPTER_BINDINGS,
        "max_input_artifacts": MAX_RUNTIME_INPUT_ARTIFACTS,
        "max_input_bytes": MAX_RUNTIME_INPUT_BYTES,
        "max_input_total_bytes": MAX_RUNTIME_INPUT_TOTAL_BYTES,
        "max_custom_bundle_bytes": MAX_CUSTOM_BUNDLE_BYTES,
    }


def _registry_revision(adapters: list[dict[str, Any]], limits: Mapping[str, Any]) -> str:
    stable = {
        "schema_version": RUNTIME_ADAPTER_REGISTRY_SCHEMA_VERSION,
        "limits": dict(limits),
        "adapters": adapters,
    }
    encoded = m_json_contract.dumps(
        stable,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(b"spl.daemon.runtime-adapter-registry/v1\0" + encoded).hexdigest()


def _validate_instance(value: Any) -> datetime:
    instance = _require_mapping(value, "instance")
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
    started_at = instance["started_at"]
    if not isinstance(started_at, str) or not _STARTED_AT.fullmatch(started_at):
        raise ValueError("instance.started_at is not a bounded UTC timestamp")
    return _parse_utc_timestamp(started_at, "instance.started_at")


def _validate_observed_at(value: Any) -> datetime:
    if not isinstance(value, str) or not _OBSERVED_AT.fullmatch(value):
        raise ValueError("observed_at is not canonical UTC milliseconds")
    return _parse_utc_timestamp(value, "observed_at")


def _parse_utc_timestamp(value: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{name} must be UTC")
    return parsed


def _validate_redaction(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str) or key.casefold() in _FORBIDDEN_KEYS:
                raise ValueError("runtime adapter registry contains a forbidden field")
            _validate_redaction(child)
    elif isinstance(value, list):
        for child in value:
            _validate_redaction(child)


def _same_json_value(value: Any, expected: Any) -> bool:
    """Compare JSON values without accepting integers as booleans."""

    if isinstance(expected, Mapping):
        return (
            isinstance(value, Mapping)
            and set(value) == set(expected)
            and all(_same_json_value(value[key], child) for key, child in expected.items())
        )
    if isinstance(expected, list):
        return (
            isinstance(value, list)
            and len(value) == len(expected)
            and all(_same_json_value(item, expected_item) for item, expected_item in zip(value, expected, strict=True))
        )
    return type(value) is type(expected) and value == expected


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return value


def _require_exact_keys(value: Mapping[str, Any], expected: frozenset[str], name: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{name} fields do not match the v1 contract")


def _require_exact_int(value: Any, expected: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value != expected:
        raise ValueError(f"{name} must be {expected}")


def _canonical_package(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


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


__all__ = [
    "DAEMON_CONTROL_ENVIRONMENT_SCOPE",
    "RUNTIME_ADAPTER_REGISTRY_CAPABILITY",
    "RUNTIME_ADAPTER_REGISTRY_CAPABILITY_VERSION",
    "RUNTIME_ADAPTER_REGISTRY_MAX_BYTES",
    "RUNTIME_ADAPTER_REGISTRY_SCHEMA",
    "RUNTIME_ADAPTER_REGISTRY_SCHEMA_VERSION",
    "RUNTIME_ADAPTER_REGISTRY_UNAVAILABLE_CODE",
    "RuntimeAdapterRegistryUnavailable",
    "build_runtime_adapter_registry_document",
    "validate_runtime_adapter_registry_document",
]
