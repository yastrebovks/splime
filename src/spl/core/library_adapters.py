"""Closed contracts for first-class, immutable Library Adapters.

The DTOs in this module are safe to use from the SDK, daemon, server, and
trusted notebook companion.  Executable source is deliberately absent from
catalog and version projections and is handled only by the purpose-specific
publication/source contracts.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal

from spl.core.runtime_port_adapters import (
    MAX_CUSTOM_BUNDLE_BYTES,
    RuntimePortAdapterContractError,
    RuntimeAdapterSemanticState,
    runtime_adapter_semantic_state,
    validate_custom_adapter_source,
)

LIBRARY_ADAPTER_CATALOG_CAPABILITY: Final = "spl.library_adapter_catalog.v1"
LIBRARY_ADAPTER_PUBLISH_CAPABILITY: Final = "spl.library_adapter_publish.v1"
RUNTIME_LIBRARY_ADAPTER_REF_CAPABILITY: Final = "spl.runtime_library_adapter_ref.v1"
LIBRARY_ADAPTER_CAPABILITY_VERSION: Final = 1
LIBRARY_ADAPTER_SCHEMA_VERSION: Final = 1

LIBRARY_ADAPTER_CONTENT_HASH_DOMAIN: Final = "spl.library-adapter.content/v1"
LIBRARY_ADAPTER_SIGNATURE_HASH_DOMAIN: Final = "spl.library-adapter.signature/v1"
LIBRARY_ADAPTER_ENVIRONMENT_HASH_DOMAIN: Final = "spl.library-adapter.environment/v1"

MAX_ADAPTER_SOURCE_BYTES: Final = MAX_CUSTOM_BUNDLE_BYTES
MAX_ADAPTER_DEPENDENCIES: Final = 256
MAX_DEPENDENCY_MODULES: Final = 256
MAX_ENVIRONMENT_DISTRIBUTIONS: Final = 2_048
MAX_ADAPTER_TEXT: Final = 512
MAX_ADAPTER_DESCRIPTION: Final = 4_096
MAX_ADAPTER_MEDIA_TYPE_BYTES: Final = 160
MAX_RUNTIME_LIBRARY_ADAPTER_BINDINGS: Final = 1_024

AdapterDirection = Literal["input", "output"]
LIBRARY_ADAPTER_SEMANTIC_CATEGORIES: Final = frozenset({"json", "text", "binary", "table", "image", "opaque_file"})

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_OWNER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,255}$")
_SEMANTIC = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:\[\], |~-]{0,511}$")
_PACKAGE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,159}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_~-]{0,159}$")
_MODULE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_TAG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,159}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_MEDIA_TYPE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}$")
_EXTENSION = re.compile(r"^\.[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")
_PORT = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}(?:\.[A-Za-z_][A-Za-z0-9_-]{0,127})?$")
_UNSAFE_PRESENTATION = re.compile(
    r"(?:file://|(?:^|[\s('\"`])/(?:[^\s/]+/)+[^\s]*|\b[A-Za-z]:\\|"
    r"(?:^|[\s('\"`])\\\\[^\\\s]+\\[^\s]+|"
    r"\b[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@|"
    r"\b(?:bearer|token|secret|password|credential|api[_ -]?key)\b\s*(?:[:=]|\s)\s*\S+|\bsk-[A-Za-z0-9_-]{8,})",
    re.IGNORECASE,
)

_REF_KEYS = frozenset(
    {
        "owner",
        "library",
        "name",
        "version",
        "adapter_id",
        "adapter_version_id",
        "content_hash",
        "signature_hash",
    }
)
_RUNTIME_REF_DOCUMENT_KEYS = frozenset({"schema_version", "bindings"})
_RUNTIME_REF_BINDING_KEYS = frozenset({"direction", "port", *_REF_KEYS})
_DEPENDENCY_KEYS = frozenset({"package", "version", "modules"})
_DISTRIBUTION_KEYS = frozenset({"package", "version"})
_POLICY_KEYS = frozenset({"local_custom_code", "remote_custom_code"})
_EFFECTIVE_POLICY_KEYS = frozenset({"local_custom", "remote_custom"})
_ENVIRONMENT_KEYS = frozenset({"fingerprint", "distributions"})
_PUBLISH_KEYS = frozenset(
    {
        "schema_version",
        "name",
        "description",
        "semantic_type",
        "semantic_category",
        "save_source",
        "load_source",
        "dependencies",
        "format_tag",
        "media_type",
        "preferred_extension",
        "policy",
        "publication_environment",
        "content_hash",
        "signature_hash",
        "source_adapter_id",
        "source_version_id",
    }
)


class LibraryAdapterContractError(ValueError):
    """Closed validation failure with a stable, presentation-safe code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class LibraryAdapterRef:
    """Exact immutable reference used by one Run port binding."""

    owner: str
    library: str
    name: str
    version: int
    adapter_id: str
    adapter_version_id: str
    content_hash: str
    signature_hash: str

    def __post_init__(self) -> None:
        normalized = normalize_library_adapter_ref(self.to_dict(unchecked=True))
        for key, value in normalized.items():
            object.__setattr__(self, key, value)

    def to_dict(self, *, unchecked: bool = False) -> dict[str, Any]:
        value = {
            "owner": self.owner,
            "library": self.library,
            "name": self.name,
            "version": self.version,
            "adapter_id": self.adapter_id,
            "adapter_version_id": self.adapter_version_id,
            "content_hash": self.content_hash,
            "signature_hash": self.signature_hash,
        }
        return value if unchecked else normalize_library_adapter_ref(value)


@dataclass(frozen=True)
class LibraryAdapterDependency:
    """One exact distribution and its reviewed top-level import roots."""

    package: str
    version: str
    modules: tuple[str, ...]

    def __post_init__(self) -> None:
        [normalized] = normalize_dependencies(
            [
                {
                    "package": self.package,
                    "version": self.version,
                    "modules": list(self.modules),
                }
            ]
        )
        object.__setattr__(self, "package", normalized["package"])
        object.__setattr__(self, "version", normalized["version"])
        object.__setattr__(self, "modules", tuple(normalized["modules"]))

    def to_dict(self) -> dict[str, Any]:
        return {
            "package": self.package,
            "version": self.version,
            "modules": list(self.modules),
        }


@dataclass(frozen=True)
class LibraryAdapterVersion:
    """Code-free projection of one immutable Library Adapter version."""

    ref: LibraryAdapterRef
    description: str
    dependencies: tuple[LibraryAdapterDependency, ...]
    semantic_type: str
    semantic_category: str | None
    directions: tuple[AdapterDirection, ...]
    format_tag: str | None
    media_type: str | None
    preferred_extension: str | None
    effective_policy: tuple[tuple[str, str], ...]
    created_at: str
    publisher: str
    availability: tuple[tuple[str, Any], ...] = ()
    compatibility: tuple[tuple[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.ref.to_dict(),
            "description": self.description,
            "dependencies": [dependency.to_dict() for dependency in self.dependencies],
            "semantic_type": self.semantic_type,
            "semantic_category": self.semantic_category,
            "directions": list(self.directions),
            "format_tag": self.format_tag,
            "media_type": self.media_type,
            "preferred_extension": self.preferred_extension,
            "effective_policy": dict(self.effective_policy),
            "created_at": self.created_at,
            "publisher": self.publisher,
            "availability": dict(self.availability),
            "compatibility": dict(self.compatibility),
        }


@dataclass(frozen=True)
class AdapterCatalogEntry:
    """Bounded browser-safe catalog row; never contains executable source."""

    version: LibraryAdapterVersion

    def to_dict(self) -> dict[str, Any]:
        return self.version.to_dict()


def normalize_library_adapter_ref(value: Mapping[str, Any] | LibraryAdapterRef) -> dict[str, Any]:
    if isinstance(value, LibraryAdapterRef):
        return value.to_dict(unchecked=True)
    raw = _mapping(value, "adapter_ref")
    _exact_keys(raw, _REF_KEYS, "adapter_ref")
    return {
        "owner": _identifier(raw["owner"], "adapter_ref.owner", owner=True),
        "library": _identifier(raw["library"], "adapter_ref.library"),
        "name": _identifier(raw["name"], "adapter_ref.name"),
        "version": _positive_int(raw["version"], "adapter_ref.version"),
        "adapter_id": _identifier(raw["adapter_id"], "adapter_ref.adapter_id", owner=True),
        "adapter_version_id": _identifier(
            raw["adapter_version_id"],
            "adapter_ref.adapter_version_id",
            owner=True,
        ),
        "content_hash": _hash(raw["content_hash"], "adapter_ref.content_hash"),
        "signature_hash": _hash(raw["signature_hash"], "adapter_ref.signature_hash"),
    }


def normalize_runtime_library_adapter_refs(value: Any) -> dict[str, Any]:
    """Validate the additive exact-ref sibling carried beside Run v1 data."""

    raw = _mapping(value, "runtime_library_adapter_refs")
    _exact_keys(raw, _RUNTIME_REF_DOCUMENT_KEYS, "runtime_library_adapter_refs")
    if raw["schema_version"] != LIBRARY_ADAPTER_SCHEMA_VERSION:
        raise LibraryAdapterContractError(
            "runtime_adapter_ref_schema",
            "runtime_library_adapter_refs.schema_version must equal 1",
        )
    bindings = raw["bindings"]
    if not isinstance(bindings, list | tuple) or len(bindings) > MAX_RUNTIME_LIBRARY_ADAPTER_BINDINGS:
        raise LibraryAdapterContractError(
            "runtime_adapter_ref_bindings",
            "runtime_library_adapter_refs.bindings must be a bounded list",
        )
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in bindings:
        binding = _mapping(item, "runtime library adapter binding")
        _exact_keys(binding, _RUNTIME_REF_BINDING_KEYS, "runtime library adapter binding")
        direction = binding["direction"]
        if direction not in {"input", "output"}:
            raise LibraryAdapterContractError(
                "runtime_adapter_ref_direction",
                "runtime Library Adapter direction must be input or output",
            )
        port = _matched_text(binding["port"], _PORT, "runtime library adapter port")
        identity = (str(direction), port)
        if identity in seen:
            raise LibraryAdapterContractError(
                "runtime_adapter_ref_duplicate",
                "runtime Library Adapter port is duplicated",
            )
        seen.add(identity)
        ref = normalize_library_adapter_ref({key: binding[key] for key in _REF_KEYS})
        normalized.append({"direction": direction, "port": port, **ref})
    return {
        "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
        "bindings": sorted(normalized, key=lambda item: (item["direction"], item["port"])),
    }


def normalize_dependencies(value: Any) -> list[dict[str, Any]]:
    """Normalize reviewed distribution evidence and exact import ownership.

    ``modules`` contains top-level import roots proven by source preparation.
    Roots are case-sensitive Python identifiers and may belong to exactly one
    declared distribution.  Keeping that evidence in the immutable content
    hash avoids consulting the daemon control interpreter (and handles roots
    such as ``PIL`` whose distribution is named ``Pillow``).
    """

    if not isinstance(value, list | tuple) or len(value) > MAX_ADAPTER_DEPENDENCIES:
        raise LibraryAdapterContractError(
            "dependencies_invalid",
            "dependencies must be a bounded list",
        )
    result: list[dict[str, Any]] = []
    seen_packages: dict[str, str] = {}
    module_owner: dict[str, str] = {}
    total_modules = 0
    for raw in value:
        item = _mapping(raw, "dependency")
        _exact_keys(item, _DEPENDENCY_KEYS, "dependency")
        package = _matched_text(item["package"], _PACKAGE, "dependency.package")
        version = _matched_text(item["version"], _VERSION, "dependency.version")
        modules_raw = item["modules"]
        if not isinstance(modules_raw, list | tuple) or not modules_raw or len(modules_raw) > MAX_DEPENDENCY_MODULES:
            raise LibraryAdapterContractError(
                "dependency_modules_invalid",
                "dependency.modules must be a non-empty bounded list",
            )
        modules = sorted({_matched_text(module, _MODULE, "dependency.modules") for module in modules_raw})
        if len(modules) != len(modules_raw):
            raise LibraryAdapterContractError(
                "dependency_module_duplicate",
                f"dependency {package!r} repeats an import root",
            )
        total_modules += len(modules)
        if total_modules > MAX_DEPENDENCY_MODULES:
            raise LibraryAdapterContractError(
                "dependency_modules_size",
                "dependencies exceed the aggregate import-root bound",
            )
        key = canonical_package_name(package)
        previous = seen_packages.get(key)
        if previous is not None:
            if previous != version:
                raise LibraryAdapterContractError(
                    "dependency_conflict",
                    f"dependency {package!r} has conflicting exact versions",
                )
            raise LibraryAdapterContractError("dependency_duplicate", f"dependency {package!r} is repeated")
        seen_packages[key] = version
        for module in modules:
            previous_owner = module_owner.get(module)
            if previous_owner is not None:
                raise LibraryAdapterContractError(
                    "dependency_module_ambiguous",
                    f"import root {module!r} is declared by both {previous_owner!r} and {package!r}",
                )
            module_owner[module] = package
        result.append({"package": package, "version": version, "modules": modules})
    return sorted(result, key=lambda item: (canonical_package_name(item["package"]), item["version"]))


def _normalize_distribution_list(
    value: Any,
    *,
    maximum: int,
    label: str,
) -> list[dict[str, str]]:
    if not isinstance(value, list | tuple) or len(value) > maximum:
        raise LibraryAdapterContractError(
            "dependencies_invalid",
            f"{label} must be a bounded list",
        )
    result: list[dict[str, str]] = []
    seen: dict[str, str] = {}
    for raw in value:
        item = _mapping(raw, "dependency")
        _exact_keys(item, _DISTRIBUTION_KEYS, "dependency")
        package = _matched_text(item["package"], _PACKAGE, "dependency.package")
        version = _matched_text(item["version"], _VERSION, "dependency.version")
        key = canonical_package_name(package)
        previous = seen.get(key)
        if previous is not None:
            if previous != version:
                raise LibraryAdapterContractError(
                    "dependency_conflict",
                    f"dependency {package!r} has conflicting exact versions",
                )
            raise LibraryAdapterContractError("dependency_duplicate", f"dependency {package!r} is repeated")
        seen[key] = version
        result.append({"package": package, "version": version})
    return sorted(result, key=lambda item: (canonical_package_name(item["package"]), item["version"]))


def normalize_publish_request(value: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize the shared daemon/server publication v1 request."""

    raw = _mapping(value, "publication")
    unknown = set(raw) - _PUBLISH_KEYS
    if unknown:
        raise LibraryAdapterContractError(
            "publication_fields",
            "publication contains unsupported fields",
        )
    required = _PUBLISH_KEYS - {
        "content_hash",
        "signature_hash",
        "source_adapter_id",
        "source_version_id",
    }
    missing = required - set(raw)
    if missing:
        raise LibraryAdapterContractError("publication_fields", "publication is missing required fields")
    if raw.get("schema_version") != LIBRARY_ADAPTER_SCHEMA_VERSION:
        raise LibraryAdapterContractError("schema_version", "library adapter schema version is not supported")
    dependencies = normalize_dependencies(raw["dependencies"])
    save_source = _optional_source(raw["save_source"], "save_source")
    load_source = _optional_source(raw["load_source"], "load_source")
    if save_source is None and load_source is None:
        raise LibraryAdapterContractError(
            "adapter_functions_missing",
            "at least one of save_source or load_source is required",
        )
    policy = _mapping(raw["policy"], "policy")
    _exact_keys(policy, _POLICY_KEYS, "policy")
    normalized_policy = {
        "local_custom_code": _policy_value(policy["local_custom_code"], "policy.local_custom_code"),
        "remote_custom_code": _policy_value(policy["remote_custom_code"], "policy.remote_custom_code"),
    }
    environment = _mapping(raw["publication_environment"], "publication_environment")
    _exact_keys(environment, _ENVIRONMENT_KEYS, "publication_environment")
    normalized_environment: dict[str, Any] = {
        "fingerprint": _hash(environment["fingerprint"], "publication_environment.fingerprint"),
        "distributions": normalize_environment_distributions(environment["distributions"]),
    }
    if normalized_environment["fingerprint"] != environment_fingerprint(normalized_environment["distributions"]):
        raise LibraryAdapterContractError(
            "environment_fingerprint_mismatch",
            "publication environment fingerprint does not match its exact distributions",
        )
    environment_by_package = {
        canonical_package_name(item["package"]): item["version"] for item in normalized_environment["distributions"]
    }
    for dependency in dependencies:
        if environment_by_package.get(canonical_package_name(dependency["package"])) != dependency["version"]:
            raise LibraryAdapterContractError(
                "dependency_unavailable",
                f"dependency {dependency['package']}=={dependency['version']} is not proven by the publication environment",
            )
    save = None if save_source is None else _validate_source(save_source, role="save", dependencies=dependencies)
    load = None if load_source is None else _validate_source(load_source, role="load", dependencies=dependencies)
    combined_source_bytes = sum(len(item["source"].encode("utf-8")) for item in (save, load) if item is not None)
    if combined_source_bytes > MAX_CUSTOM_BUNDLE_BYTES:
        raise LibraryAdapterContractError(
            "adapter_source_size",
            "combined save/load source exceeds the worker bundle limit",
        )
    if save is not None and load is not None and save["symbol"] == load["symbol"]:
        raise LibraryAdapterContractError(
            "adapter_symbol_collision",
            "save and load functions must use different symbols",
        )
    semantic_type = _matched_text(raw["semantic_type"], _SEMANTIC, "semantic_type")
    semantic_category = raw["semantic_category"]
    if semantic_category is not None and semantic_category not in LIBRARY_ADAPTER_SEMANTIC_CATEGORIES:
        raise LibraryAdapterContractError(
            "semantic_category_invalid",
            "semantic_category is not in the closed Library Adapter category catalog",
        )
    format_tag = _optional_matched_text(raw["format_tag"], _TAG, "format_tag")
    media_type = _optional_matched_text(raw["media_type"], _MEDIA_TYPE, "media_type")
    if media_type is not None and len(media_type.encode("ascii")) > MAX_ADAPTER_MEDIA_TYPE_BYTES:
        raise LibraryAdapterContractError(
            "media_type_invalid",
            f"media_type exceeds {MAX_ADAPTER_MEDIA_TYPE_BYTES} bytes",
        )
    preferred_extension = _optional_matched_text(
        raw["preferred_extension"],
        _EXTENSION,
        "preferred_extension",
    )
    normalized = {
        "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
        "name": _identifier(raw["name"], "name"),
        "description": safe_presentation_text(
            raw["description"],
            "description",
            MAX_ADAPTER_DESCRIPTION,
        ),
        "semantic_type": semantic_type,
        "semantic_category": semantic_category,
        "save_source": None if save is None else save["source"],
        "load_source": None if load is None else load["source"],
        "dependencies": dependencies,
        "format_tag": format_tag,
        "media_type": media_type,
        "preferred_extension": preferred_extension,
        "policy": normalized_policy,
        "publication_environment": normalized_environment,
        "content_hash": None,
        "signature_hash": None,
        "source_adapter_id": _optional_identifier(raw.get("source_adapter_id"), "source_adapter_id"),
        "source_version_id": _optional_identifier(raw.get("source_version_id"), "source_version_id"),
    }
    signature_payload = library_adapter_signature_payload(normalized, save=save, load=load)
    execution_payload = library_adapter_execution_payload(normalized)
    computed_content_hash = domain_hash(LIBRARY_ADAPTER_CONTENT_HASH_DOMAIN, execution_payload)
    computed_signature_hash = domain_hash(LIBRARY_ADAPTER_SIGNATURE_HASH_DOMAIN, signature_payload)
    for key, computed in (
        ("content_hash", computed_content_hash),
        ("signature_hash", computed_signature_hash),
    ):
        supplied = raw.get(key)
        if supplied is not None and _hash(supplied, key) != computed:
            raise LibraryAdapterContractError(f"{key}_mismatch", f"{key} does not match canonical evidence")
        normalized[key] = computed
    normalized["directions"] = directions_for_sources(
        save_source=normalized["save_source"],
        load_source=normalized["load_source"],
    )
    normalized["symbols"] = {
        "save": None if save is None else save["symbol"],
        "load": None if load is None else load["symbol"],
    }
    normalized["signature"] = signature_payload
    return normalized


def library_adapter_execution_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return the exact execution-relevant canonical hash payload."""

    return {
        "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
        "semantic_type": value["semantic_type"],
        "semantic_category": value.get("semantic_category"),
        "save_source": value.get("save_source"),
        "load_source": value.get("load_source"),
        "dependencies": normalize_dependencies(value.get("dependencies", [])),
        "format_tag": value.get("format_tag"),
        "media_type": value.get("media_type"),
        "preferred_extension": value.get("preferred_extension"),
        "policy": dict(value["policy"]),
    }


def library_adapter_signature_payload(
    value: Mapping[str, Any],
    *,
    save: Mapping[str, Any] | None,
    load: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return semantic callable evidence independently from source bytes."""

    def callable_record(item: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if item is None:
            return None
        return {
            "symbol": item["symbol"],
            "parameters": list(item["parameters"]),
            "annotations": {
                "parameters": list(item["annotations"]["parameters"]),
                "return": item["annotations"]["return"],
            },
        }

    return {
        "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
        "semantic_type": value["semantic_type"],
        "semantic_category": value.get("semantic_category"),
        "directions": directions_for_sources(
            save_source=value.get("save_source"),
            load_source=value.get("load_source"),
        ),
        "format_tag": value.get("format_tag"),
        "save": callable_record(save),
        "load": callable_record(load),
    }


def directions_for_sources(*, save_source: str | None, load_source: str | None) -> list[AdapterDirection]:
    result: list[AdapterDirection] = []
    if load_source is not None:
        result.append("input")
    if save_source is not None:
        result.append("output")
    return result


def library_adapter_semantic_compatible(
    port_type: str | None,
    adapter_type: str,
    semantic_category: str | None,
) -> bool:
    """Return whether exact declared types support a conservative recommendation."""

    return library_adapter_semantic_advisory(port_type, adapter_type, semantic_category) == "recommended"


def library_adapter_semantic_advisory(
    port_type: str | None,
    adapter_type: str,
    semantic_category: str | None,
) -> RuntimeAdapterSemanticState:
    """Classify a Library Adapter without treating its broad category as equality."""

    del semantic_category
    return runtime_adapter_semantic_state(
        port_type,
        adapter_type,
        adapter_id="library-adapter",
    )


def environment_fingerprint(distributions: Sequence[Mapping[str, Any]]) -> str:
    return domain_hash(
        LIBRARY_ADAPTER_ENVIRONMENT_HASH_DOMAIN,
        {
            "schema_version": LIBRARY_ADAPTER_SCHEMA_VERSION,
            "distributions": normalize_environment_distributions(list(distributions)),
        },
    )


def normalize_environment_distributions(value: Any) -> list[dict[str, str]]:
    return _normalize_distribution_list(
        value,
        maximum=MAX_ENVIRONMENT_DISTRIBUTIONS,
        label="publication environment distributions",
    )


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def domain_hash(domain: str, value: Any) -> str:
    return hashlib.sha256(domain.encode("utf-8") + b"\0" + canonical_json_bytes(value)).hexdigest()


def canonical_package_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def _validate_source(source: str, *, role: str, dependencies: list[dict[str, str]]) -> dict[str, Any]:
    try:
        return validate_custom_adapter_source(source, role=role, distributions=dependencies)
    except RuntimePortAdapterContractError as exc:
        raise LibraryAdapterContractError(exc.code, str(exc)) from None


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LibraryAdapterContractError(f"{name}_invalid", f"{name} must be an object")
    return value


def _exact_keys(value: Mapping[str, Any], expected: frozenset[str], name: str) -> None:
    if set(value) != set(expected):
        raise LibraryAdapterContractError(f"{name}_fields", f"{name} fields are not recognized")


def _text(value: Any, name: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or len(value.encode("utf-8")) > maximum
        or any(ord(character) < 32 and character not in "\t\n\r" for character in value)
    ):
        raise LibraryAdapterContractError(f"{name}_invalid", f"{name} must be bounded text")
    return value


def safe_presentation_text(value: Any, name: str, maximum: int) -> str:
    """Reject path/credential-shaped material from browser-safe projections."""

    text = _text(value, name, maximum)
    if _UNSAFE_PRESENTATION.search(text):
        raise LibraryAdapterContractError(
            f"{name}_unsafe",
            f"{name} must not contain paths, file URLs, or credential-shaped values",
        )
    return text


def _optional_text(value: Any, name: str, maximum: int) -> str | None:
    if value is None:
        return None
    text = _text(value, name, maximum)
    if not text:
        raise LibraryAdapterContractError(f"{name}_invalid", f"{name} must be non-empty when present")
    return text


def _optional_source(value: Any, name: str) -> str | None:
    if value is None:
        return None
    text = _text(value, name, MAX_ADAPTER_SOURCE_BYTES)
    if not text.strip():
        raise LibraryAdapterContractError(f"{name}_invalid", f"{name} must be non-empty when present")
    return text


def _matched_text(value: Any, pattern: re.Pattern[str], name: str) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise LibraryAdapterContractError(f"{name}_invalid", f"{name} is invalid")
    return value


def _optional_matched_text(value: Any, pattern: re.Pattern[str], name: str) -> str | None:
    return None if value is None else _matched_text(value, pattern, name)


def _identifier(value: Any, name: str, *, owner: bool = False) -> str:
    return _matched_text(value, _OWNER if owner else _NAME, name)


def _optional_identifier(value: Any, name: str) -> str | None:
    return None if value is None else _identifier(value, name, owner=True)


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > 9_007_199_254_740_991:
        raise LibraryAdapterContractError(f"{name}_invalid", f"{name} must be a positive integer")
    return value


def _hash(value: Any, name: str) -> str:
    return _matched_text(value, _HASH, name)


def _policy_value(value: Any, name: str) -> str:
    if value not in {"allow", "deny"}:
        raise LibraryAdapterContractError(f"{name}_invalid", f"{name} must be 'allow' or 'deny'")
    return str(value)


__all__ = [
    "AdapterCatalogEntry",
    "LIBRARY_ADAPTER_CAPABILITY_VERSION",
    "LIBRARY_ADAPTER_CATALOG_CAPABILITY",
    "LIBRARY_ADAPTER_CONTENT_HASH_DOMAIN",
    "LIBRARY_ADAPTER_ENVIRONMENT_HASH_DOMAIN",
    "LIBRARY_ADAPTER_PUBLISH_CAPABILITY",
    "LIBRARY_ADAPTER_SCHEMA_VERSION",
    "LIBRARY_ADAPTER_SIGNATURE_HASH_DOMAIN",
    "LIBRARY_ADAPTER_SEMANTIC_CATEGORIES",
    "MAX_ADAPTER_MEDIA_TYPE_BYTES",
    "LibraryAdapterContractError",
    "LibraryAdapterDependency",
    "LibraryAdapterRef",
    "LibraryAdapterVersion",
    "RUNTIME_LIBRARY_ADAPTER_REF_CAPABILITY",
    "canonical_json_bytes",
    "canonical_package_name",
    "directions_for_sources",
    "domain_hash",
    "environment_fingerprint",
    "library_adapter_execution_payload",
    "library_adapter_signature_payload",
    "library_adapter_semantic_advisory",
    "library_adapter_semantic_compatible",
    "normalize_dependencies",
    "normalize_environment_distributions",
    "normalize_library_adapter_ref",
    "normalize_publish_request",
    "normalize_runtime_library_adapter_refs",
    "safe_presentation_text",
]
