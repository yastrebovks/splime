"""Closed, versioned contracts for external Run port adapters.

This module is deliberately free of daemon and optional-client imports.  It
validates only JSON-compatible metadata; adapter functions and bundle source
are never imported or executed here.
"""

from __future__ import annotations

import ast
import builtins
import hashlib
import inspect
import keyword
import re
import sys
import textwrap
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import metadata as importlib_metadata
from types import FunctionType
from typing import Any, Final, Literal, cast

from spl.adapters import (
    BUILTIN_ADAPTER_IDS,
    JSON,
    OPAQUE_FILE,
    builtin_advisory_semantic_category,
    builtin_presentation,
    builtin_registry_metadata,
    builtin_required_packages,
    canonical_builtin_semantic_type,
    get_builtin_adapter,
)
from spl.core.entities.adapter import RuntimeAdapter
from spl.core.json_contract import dumps as json_dumps
from spl.core.json_contract import validate_json_value

RUNTIME_PORT_ADAPTERS_SCHEMA_VERSION: Final = 1
RUNTIME_PORT_ADAPTERS_CAPABILITY: Final = "spl.remote_run.runtime_port_adapters.v1"
RUNTIME_PORT_ADAPTERS_CAPABILITY_VERSION: Final = 1
RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY: Final = "spl.runtime_adapter_semantic_override.v1"
RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY_VERSION: Final = 1
RUNTIME_ADAPTER_SEMANTIC_ADVISORIES_SCHEMA_VERSION: Final = 1
MAX_RUNTIME_ADAPTER_BINDINGS: Final = 1_024
MAX_RUNTIME_ADAPTER_SEMANTIC_ADVISORIES: Final = MAX_RUNTIME_ADAPTER_BINDINGS
MAX_RUNTIME_INPUT_ARTIFACTS: Final = 1_024
MAX_RUNTIME_INPUT_BYTES: Final = 256 * 1024 * 1024
MAX_RUNTIME_INPUT_TOTAL_BYTES: Final = 512 * 1024 * 1024
MAX_CUSTOM_BUNDLE_BYTES: Final = 512 * 1024

AdapterDirection = Literal["input", "output"]
AdapterResolutionSource = Literal["system_default", "preset", "run_override"]
AdapterTransport = Literal["inline_json", "artifact"]
CustomRemotePolicy = Literal["allow", "deny"]
RuntimeAdapterSemanticState = Literal["recommended", "declared_type_mismatch", "compatibility_unproven"]
RuntimeAdapterSemanticAcknowledgementSource = Literal[
    "plugin_confirmation",
    "sdk_explicit_selection",
    "api_explicit_selection",
    "embedded_contract",
]

_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
_SAFE_PORT = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}(?:\.[A-Za-z_][A-Za-z0-9_-]{0,127})?$")
_SAFE_TAG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,159}$")
_SAFE_TYPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\[\], |]*$")
_SAFE_ADVISORY_TYPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:\[\], |~-]{0,511}$")
_SAFE_ADVISORY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+-]{0,255}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PACKAGE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,159}$")
_REVIEWED_PACKAGE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,159}$")
_REVIEWED_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_~-]{0,159}$")
_REVIEWED_MODULE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_MEDIA_TYPE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}$")
_DYNAMIC_CALLS = frozenset(
    {"__import__", "compile", "eval", "exec", "getattr", "globals", "import_module", "locals", "setattr", "vars"}
)
_IMPORT_HEADER = re.compile(
    r"^# spl-runtime-import-v1 symbol=([A-Za-z][A-Za-z0-9_]*) module=([A-Za-z][A-Za-z0-9_]*) "
    r"package=([A-Za-z0-9][A-Za-z0-9._-]{0,127}) version=([A-Za-z0-9][A-Za-z0-9.!+_-]{0,159})$"
)

_DOCUMENT_KEYS = frozenset({"schema_version", "bindings", "inputs", "custom_bundle"})
_BINDING_KEYS = frozenset(
    {
        "direction",
        "port",
        "external_name",
        "semantic_type",
        "adapter",
        "resolution_source",
        "transport",
        "argument",
        "input_name",
        "result_path",
    }
)
_ADAPTER_KEYS = frozenset(
    {
        "kind",
        "id",
        "key",
        "format_tag",
        "accepted_tags",
        "distributions",
        "save_symbol",
        "load_symbol",
        "bundle_sha256",
        "presentation",
    }
)
_DISTRIBUTION_KEYS = frozenset({"package", "version"})
_PRESENTATION_KEYS = frozenset({"media_type", "preferred_extension"})
_ARGUMENT_KEYS = frozenset({"kind", "name", "index"})
_INPUT_KEYS = frozenset(
    {
        "name",
        "port",
        "size",
        "sha256",
        "format_tag",
        "semantic_type",
        "adapter_id",
        "media_type",
        "content_base64",
        "staged_name",
    }
)
_CUSTOM_BUNDLE_KEYS = frozenset({"name", "size", "sha256", "functions", "content_base64", "staged_name"})
_OUTPUT_RECORD_KEYS = frozenset(
    {"port", "name", "size", "sha256", "format_tag", "adapter_id", "media_type", "result_path"}
)
_SEMANTIC_ADVISORY_DOCUMENT_KEYS = frozenset({"schema_version", "bindings"})
_SEMANTIC_ADVISORY_BINDING_KEYS = frozenset(
    {
        "direction",
        "port",
        "adapter_id",
        "port_semantic_type",
        "adapter_semantic_type",
        "adapter_semantic_category",
        "state",
        "acknowledged",
        "acknowledgement_source",
    }
)
_SEMANTIC_ADVISORY_STATES = frozenset({"recommended", "declared_type_mismatch", "compatibility_unproven"})
_SEMANTIC_ADVISORY_ACKNOWLEDGEMENT_SOURCES = frozenset(
    {"plugin_confirmation", "sdk_explicit_selection", "api_explicit_selection", "embedded_contract"}
)
_SEMANTIC_CATEGORIES = frozenset({"json", "strict_json", "text", "binary", "table", "image", "opaque_file"})


class RuntimePortAdapterContractError(ValueError):
    """Stable, stage-specific runtime adapter contract failure."""

    def __init__(self, code: str, message: str, *, stage: str = "adapter_resolution") -> None:
        self.code = code
        self.stage = stage
        self.safe_message = message
        super().__init__(f"{stage}: {code}: {message}")


@dataclass(frozen=True)
class PortTarget:
    """One authoritative external port address."""

    canonical: str
    short: str
    semantic_type: str | None
    direction: AdapterDirection
    external_name: str


def normalize_adapter_policy(value: Any) -> dict[str, str]:
    """Validate the closed adapter execution policy."""

    if value is None:
        return {"custom_remote": "deny"}
    if not isinstance(value, Mapping):
        raise RuntimePortAdapterContractError("policy_type", "adapter_policy must be a mapping")
    if set(value) != {"custom_remote"}:
        raise RuntimePortAdapterContractError(
            "policy_keys",
            "adapter_policy accepts exactly the `custom_remote` field",
        )
    custom_remote = value["custom_remote"]
    if custom_remote not in {"allow", "deny"}:
        raise RuntimePortAdapterContractError(
            "policy_custom_remote",
            "adapter_policy.custom_remote must be 'allow' or 'deny'",
        )
    return {"custom_remote": cast(CustomRemotePolicy, custom_remote)}


def normalize_public_adapter_mapping(value: Any) -> dict[str, dict[str, Any]] | None:
    """Validate the public call-boundary mapping without interpreting values."""

    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise RuntimePortAdapterContractError("mapping_type", "adapters must be a mapping")
    unknown = set(value) - {"inputs", "outputs"}
    if unknown:
        raise RuntimePortAdapterContractError(
            "mapping_sections",
            "adapters contains unknown section(s): {}".format(", ".join(sorted(map(str, unknown)))),
        )
    normalized: dict[str, dict[str, Any]] = {"inputs": {}, "outputs": {}}
    for section in ("inputs", "outputs"):
        raw = value.get(section, {})
        if not isinstance(raw, Mapping):
            raise RuntimePortAdapterContractError(
                "mapping_section_type",
                f"adapters.{section} must be a mapping",
            )
        for key, adapter in raw.items():
            if not isinstance(key, str) or not _SAFE_PORT.fullmatch(key):
                raise RuntimePortAdapterContractError(
                    "port_reference",
                    f"adapters.{section} keys must be simple ports or canonical alias.port addresses",
                )
            normalized[section][key] = adapter
    return normalized


def runtime_adapter_semantic_state(
    port_type: str | None,
    adapter_type: str,
    *,
    adapter_id: str,
) -> RuntimeAdapterSemanticState:
    """Classify advisory semantic evidence without changing structural admission.

    The result is intentionally narrower than :func:`semantic_type_compatible`:
    an absent, ``Any``, union, or otherwise ambiguous declaration is unproven,
    and a shared broad category never makes two distinct concrete types equal.
    """

    if port_type is None or not str(port_type).strip():
        return "compatibility_unproven"
    normalized = _normalize_type_text(port_type)
    adapter_normalized = _normalize_type_text(adapter_type)
    if _semantic_type_is_ambiguous(normalized) or _semantic_type_is_ambiguous(adapter_normalized):
        return "compatibility_unproven"
    if adapter_id == JSON and _is_declared_json_semantic_type(normalized):
        return "recommended"
    if _canonical_semantic_type(normalized) == _canonical_semantic_type(adapter_normalized):
        return "recommended"
    return "declared_type_mismatch"


def runtime_adapter_semantic_category(adapter_id: str) -> str | None:
    """Return one stable broad category for a built-in adapter, if any."""

    return builtin_advisory_semantic_category(adapter_id)


def normalize_runtime_adapter_semantic_advisories(value: Any) -> dict[str, Any]:
    """Validate the closed, bounded semantic-advisory sibling document."""

    document = _mapping(value, "runtime adapter semantic advisories")
    _exact_keys(document, _SEMANTIC_ADVISORY_DOCUMENT_KEYS, "runtime adapter semantic advisories")
    if document["schema_version"] != RUNTIME_ADAPTER_SEMANTIC_ADVISORIES_SCHEMA_VERSION:
        raise RuntimePortAdapterContractError(
            "semantic_advisory_schema_version",
            "runtime adapter semantic advisories schema_version must be 1",
            stage="admission",
        )
    bindings = _list(document["bindings"], "runtime adapter semantic advisory bindings")
    if len(bindings) > MAX_RUNTIME_ADAPTER_SEMANTIC_ADVISORIES:
        raise RuntimePortAdapterContractError(
            "semantic_advisory_binding_limit",
            f"runtime adapter semantic advisories exceed the {MAX_RUNTIME_ADAPTER_SEMANTIC_ADVISORIES}-binding limit",
            stage="admission",
        )
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in bindings:
        binding = _mapping(raw, "runtime adapter semantic advisory binding")
        _exact_keys(binding, _SEMANTIC_ADVISORY_BINDING_KEYS, "runtime adapter semantic advisory binding")
        direction = binding["direction"]
        if direction not in {"input", "output"}:
            raise RuntimePortAdapterContractError(
                "semantic_advisory_direction",
                "semantic advisory direction must be input or output",
                stage="admission",
            )
        port = _required_port(binding["port"])
        identity = (str(direction), port)
        if identity in seen:
            raise RuntimePortAdapterContractError(
                "semantic_advisory_duplicate",
                f"semantic advisory repeats {direction} port {port!r}",
                stage="admission",
            )
        seen.add(identity)
        adapter_id = binding["adapter_id"]
        if not isinstance(adapter_id, str) or not _SAFE_ADVISORY_ID.fullmatch(adapter_id):
            raise RuntimePortAdapterContractError(
                "semantic_advisory_adapter_id",
                "semantic advisory adapter_id is invalid",
                stage="admission",
            )
        port_semantic_type = _optional_advisory_semantic_type(
            binding["port_semantic_type"], "semantic advisory port_semantic_type"
        )
        adapter_semantic_type = _advisory_semantic_type(
            binding["adapter_semantic_type"], "semantic advisory adapter_semantic_type"
        )
        category = binding["adapter_semantic_category"]
        if category is not None and category not in _SEMANTIC_CATEGORIES:
            raise RuntimePortAdapterContractError(
                "semantic_advisory_category",
                "semantic advisory adapter_semantic_category is not recognized",
                stage="admission",
            )
        state = binding["state"]
        if state not in _SEMANTIC_ADVISORY_STATES:
            raise RuntimePortAdapterContractError(
                "semantic_advisory_state",
                "semantic advisory state is not recognized",
                stage="admission",
            )
        acknowledged = binding["acknowledged"]
        if type(acknowledged) is not bool:
            raise RuntimePortAdapterContractError(
                "semantic_advisory_acknowledged",
                "semantic advisory acknowledged must be a boolean",
                stage="admission",
            )
        acknowledgement_source = binding["acknowledgement_source"]
        if acknowledgement_source is not None and acknowledgement_source not in (
            _SEMANTIC_ADVISORY_ACKNOWLEDGEMENT_SOURCES
        ):
            raise RuntimePortAdapterContractError(
                "semantic_advisory_acknowledgement_source",
                "semantic advisory acknowledgement_source is not recognized",
                stage="admission",
            )
        if acknowledged != (acknowledgement_source is not None):
            raise RuntimePortAdapterContractError(
                "semantic_advisory_acknowledgement",
                "semantic advisory acknowledgement and source must be present together",
                stage="admission",
            )
        if state == "recommended" and acknowledged:
            raise RuntimePortAdapterContractError(
                "semantic_advisory_recommended_acknowledgement",
                "recommended semantic advisories must not carry an acknowledgement",
                stage="admission",
            )
        normalized.append(
            {
                "direction": direction,
                "port": port,
                "adapter_id": adapter_id,
                "port_semantic_type": port_semantic_type,
                "adapter_semantic_type": adapter_semantic_type,
                "adapter_semantic_category": category,
                "state": state,
                "acknowledged": acknowledged,
                "acknowledgement_source": acknowledgement_source,
            }
        )
    return {
        "schema_version": RUNTIME_ADAPTER_SEMANTIC_ADVISORIES_SCHEMA_VERSION,
        "bindings": sorted(normalized, key=lambda item: (item["direction"], item["port"])),
    }


def adapter_descriptor(
    adapter: RuntimeAdapter,
    *,
    adapter_id: str | None,
    custom_bundle_sha256: str | None = None,
    save_symbol: str | None = None,
    load_symbol: str | None = None,
) -> dict[str, Any]:
    """Build the closed, code-free descriptor for one adapter."""

    key = _required_text(adapter.key, "adapter key", maximum=512)
    format_tag = _required_tag(adapter.tag, "adapter format tag")
    accepted_tags = sorted({_required_tag(tag, "adapter accepted tag") for tag in adapter.accepted_tags})
    if format_tag not in accepted_tags:
        raise RuntimePortAdapterContractError(
            "adapter_tags",
            "adapter accepted tags must contain its emitted format tag",
        )
    distributions = _normalize_distribution_objects(adapter.distributions)
    if adapter_id is not None:
        if adapter_id not in BUILTIN_ADAPTER_IDS:
            raise RuntimePortAdapterContractError("adapter_id", f"unknown built-in adapter {adapter_id!r}")
        kind = "builtin"
        presentation = builtin_presentation(adapter_id)
        bundle_digest = None
        save_name = None
        load_name = None
    else:
        kind = "custom"
        if custom_bundle_sha256 is None or not _SHA256.fullmatch(custom_bundle_sha256):
            raise RuntimePortAdapterContractError(
                "custom_bundle_digest",
                "custom adapters require a lowercase SHA-256 bundle digest",
            )
        if not _python_identifier(save_symbol) or not _python_identifier(load_symbol):
            raise RuntimePortAdapterContractError(
                "custom_symbols",
                "custom adapter save/load symbols must be plain Python identifiers",
            )
        presentation = {"media_type": None, "preferred_extension": None}
        bundle_digest = custom_bundle_sha256
        save_name = save_symbol
        load_name = load_symbol
    semantic_type, separator, _ = key.rpartition("@")
    if not separator or not semantic_type:
        raise RuntimePortAdapterContractError("adapter_key", "adapter key must be `<python_type>@<format>`")
    _required_semantic_type(semantic_type)
    return {
        "kind": kind,
        "id": adapter_id if adapter_id is not None else f"custom:{custom_bundle_sha256}",
        "key": key,
        "format_tag": format_tag,
        "accepted_tags": accepted_tags,
        "distributions": distributions,
        "save_symbol": save_name,
        "load_symbol": load_name,
        "bundle_sha256": bundle_digest,
        "presentation": presentation,
    }


def built_in_descriptor(adapter_id: str) -> tuple[RuntimeAdapter, dict[str, Any]]:
    """Resolve and describe one stable built-in adapter ID."""

    adapter = get_builtin_adapter(adapter_id)
    return adapter, adapter_descriptor(adapter, adapter_id=adapter_id)


def built_in_registry_descriptor(adapter_id: str) -> dict[str, Any]:
    """Return one stable, environment-independent built-in catalog row."""

    _, descriptor = built_in_descriptor(adapter_id)
    semantic_type, separator, _ = descriptor["key"].rpartition("@")
    if not separator:
        raise RuntimePortAdapterContractError("adapter_key", "adapter key must be `<python_type>@<format>`")
    metadata = builtin_registry_metadata(adapter_id)
    presentation = descriptor["presentation"]
    system_default = metadata["system_default"]
    return {
        "id": adapter_id,
        "format": {
            "key": descriptor["key"],
            "tag": descriptor["format_tag"],
            "accepted_tags": list(descriptor["accepted_tags"]),
        },
        "label": metadata["label"],
        "preferred_extension": presentation["preferred_extension"],
        "media_type": presentation["media_type"],
        "directions": list(metadata["directions"]),
        "transport": metadata["transport"],
        "semantic_types": [semantic_type],
        "semantic_categories": list(metadata["semantic_categories"]),
        "system_default": {
            "fallback": system_default["fallback"],
            "semantic_types": list(system_default["semantic_types"]),
            "value_types": list(system_default["value_types"]),
        },
        "required_distributions": list(builtin_required_packages(adapter_id)),
        "builtin": True,
        "custom": False,
        "custom_code": False,
    }


def build_custom_bundle(adapters: Sequence[RuntimeAdapter]) -> tuple[dict[str, Any], dict[int, tuple[str, str]]]:
    """Build one deterministic source bundle for valid top-level functions.

    The return mapping is keyed by ``id(adapter)`` and names each save/load
    symbol.  No function is called, imported, compiled, or evaluated here.
    """

    sources: dict[str, str] = {}
    imports_by_symbol: dict[str, list[dict[str, str]]] = {}
    symbols: dict[int, tuple[str, str]] = {}
    for adapter in adapters:
        save_name, save_source, save_imports = _validated_function_source(
            adapter.save,
            role="save",
            distributions=adapter.distributions,
        )
        load_name, load_source, load_imports = _validated_function_source(
            adapter.load,
            role="load",
            distributions=adapter.distributions,
        )
        for name, source, imports in (
            (save_name, save_source, save_imports),
            (load_name, load_source, load_imports),
        ):
            previous = sources.get(name)
            if previous is not None and previous != source:
                raise RuntimePortAdapterContractError(
                    "custom_symbol_collision",
                    f"custom adapter bundle contains conflicting function symbol {name!r}",
                )
            previous_imports = imports_by_symbol.get(name)
            if previous_imports is not None and previous_imports != imports:
                raise RuntimePortAdapterContractError(
                    "custom_dependency_collision",
                    f"custom adapter symbol {name!r} has conflicting import/distribution identity",
                    stage="environment_preflight",
                )
            sources[name] = source
            imports_by_symbol[name] = imports
        symbols[id(adapter)] = (save_name, load_name)
    headers = [
        "# spl-runtime-import-v1 symbol={symbol} module={module} package={package} version={version}".format(
            symbol=symbol,
            **row,
        )
        for symbol in sorted(imports_by_symbol)
        for row in imports_by_symbol[symbol]
    ]
    body = "\n\n".join(sources[name].rstrip() for name in sorted(sources)) + "\n"
    text = ("\n".join(headers) + "\n\n" if headers else "") + body
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_CUSTOM_BUNDLE_BYTES:
        raise RuntimePortAdapterContractError(
            "custom_bundle_size",
            f"custom adapter source bundle exceeds {MAX_CUSTOM_BUNDLE_BYTES} bytes",
        )
    # This is the byte digest checked by daemon/server upload admission.  The
    # higher-level Run fingerprint supplies its own domain separation.
    digest = hashlib.sha256(encoded).hexdigest()
    return (
        {
            "name": "custom-adapters.py",
            "size": len(encoded),
            "sha256": digest,
            "functions": sorted(sources),
            "content_base64": None,
            "staged_name": None,
            "source_bytes": encoded,
        },
        symbols,
    )


def _validated_function_source(
    function: Any,
    *,
    role: str,
    distributions: Sequence[Any],
) -> tuple[str, str, list[dict[str, str]]]:
    if not isinstance(function, FunctionType):
        raise RuntimePortAdapterContractError(
            "custom_function_type",
            f"custom adapter {role} must be a plain Python function",
        )
    if function.__name__ == "<lambda>" or not _python_identifier(function.__name__):
        raise RuntimePortAdapterContractError(
            "custom_function_name",
            f"custom adapter {role} must be a named top-level function, not a lambda",
        )
    if function.__qualname__ != function.__name__ or function.__closure__:
        raise RuntimePortAdapterContractError(
            "custom_function_scope",
            f"custom adapter {role} must be top-level and must not capture a closure",
        )
    try:
        source = textwrap.dedent(inspect.getsource(function)).strip() + "\n"
    except (OSError, TypeError) as exc:
        raise RuntimePortAdapterContractError(
            "custom_source_unavailable",
            f"source is unavailable for custom adapter {role} function {function.__name__!r}",
        ) from exc
    try:
        tree = ast.parse(source, filename=f"<runtime-adapter-{role}>", mode="exec")
    except SyntaxError as exc:
        raise RuntimePortAdapterContractError(
            "custom_source_syntax",
            f"custom adapter {role} source is not valid Python",
        ) from exc
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.FunctionDef):
        raise RuntimePortAdapterContractError(
            "custom_source_shape",
            f"custom adapter {role} source must contain exactly one function definition",
        )
    definition = tree.body[0]
    if definition.name != function.__name__ or definition.decorator_list:
        raise RuntimePortAdapterContractError(
            "custom_source_shape",
            f"custom adapter {role} must be undecorated and retain its declared name",
        )
    _validate_custom_function_definition(definition, role=role)
    imports = _validate_function_imports(
        definition,
        declared_distributions={
            _canonical_package(str(getattr(distribution, "package", ""))): {
                "package": str(getattr(distribution, "package", "")),
                "version": str(getattr(distribution, "version", "")),
            }
            for distribution in distributions
        },
        role=role,
    )
    return definition.name, ast.unparse(definition).strip() + "\n", imports


def validate_custom_adapter_source(
    source: str,
    *,
    role: str,
    distributions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate reviewed adapter source without importing or executing it.

    This is the source-text counterpart to :func:`build_custom_bundle`.  It is
    intentionally additive: the established Run-bound ``Adapter`` path still
    extracts source from live Python functions, while Library Adapter
    publication supplies reviewed source text.  Both paths use the same AST,
    callable-shape, unresolved-global, and import-ownership rules.
    """

    if role not in {"save", "load"}:
        raise RuntimePortAdapterContractError(
            "custom_role",
            "custom adapter role must be 'save' or 'load'",
        )
    if not isinstance(source, str):
        raise RuntimePortAdapterContractError(
            "custom_source_type",
            f"custom adapter {role} source must be text",
        )
    encoded = source.encode("utf-8")
    if not encoded or len(encoded) > MAX_CUSTOM_BUNDLE_BYTES:
        raise RuntimePortAdapterContractError(
            "custom_source_size",
            f"custom adapter {role} source must be non-empty and bounded",
        )
    try:
        tree = ast.parse(source, filename=f"<library-adapter-{role}>", mode="exec")
    except SyntaxError as exc:
        raise RuntimePortAdapterContractError(
            "custom_source_syntax",
            f"custom adapter {role} source is not valid Python",
        ) from exc
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.FunctionDef):
        raise RuntimePortAdapterContractError(
            "custom_source_shape",
            f"custom adapter {role} source must contain exactly one function definition",
        )
    definition = tree.body[0]
    if definition.decorator_list or not _python_identifier(definition.name):
        raise RuntimePortAdapterContractError(
            "custom_source_shape",
            f"custom adapter {role} must be an undecorated named function",
        )
    _validate_custom_function_definition(definition, role=role)
    declared = _normalize_reviewed_source_dependencies(distributions)
    imports = _validate_reviewed_source_imports(
        definition,
        declared_distributions=declared,
        role=role,
    )
    return {
        "role": role,
        "symbol": definition.name,
        "source": ast.unparse(definition).strip() + "\n",
        "imports": imports,
        "parameters": [argument.arg for argument in definition.args.args],
        "annotations": {
            "parameters": [
                None if argument.annotation is None else ast.unparse(argument.annotation)
                for argument in definition.args.args
            ],
            "return": None if definition.returns is None else ast.unparse(definition.returns),
        },
    }


def _validate_reviewed_source_imports(
    definition: ast.FunctionDef,
    *,
    declared_distributions: Mapping[str, Mapping[str, str]],
    role: str,
) -> list[dict[str, str]]:
    """Bind every non-stdlib import to one explicit reviewed module owner."""

    result: list[dict[str, str]] = []
    roots = _function_import_roots(definition)
    for root in roots:
        owner = declared_distributions.get(root)
        if owner is None:
            raise RuntimePortAdapterContractError(
                "custom_import_dependency",
                f"custom adapter {role} import {root!r} is not bound to one exact reviewed distribution",
                stage="environment_preflight",
            )
        result.append(
            {
                "module": root,
                "package": owner["package"],
                "version": owner["version"],
            }
        )
    return sorted(result, key=lambda item: (item["module"], _canonical_package(item["package"])))


def _normalize_reviewed_source_dependencies(
    distributions: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, str]]:
    """Validate the Library Adapter-only dependency/module proof shape."""

    if len(distributions) > 256:
        raise RuntimePortAdapterContractError(
            "custom_dependencies_size",
            "custom adapter dependencies exceed the publication bound",
            stage="environment_preflight",
        )
    owners: dict[str, dict[str, str]] = {}
    packages: dict[str, str] = {}
    for raw in distributions:
        if not isinstance(raw, Mapping) or set(raw) != {"package", "version", "modules"}:
            raise RuntimePortAdapterContractError(
                "custom_dependency_shape",
                "custom adapter dependencies require package, version, and modules",
                stage="environment_preflight",
            )
        package = raw["package"]
        version = raw["version"]
        modules = raw["modules"]
        if not isinstance(package, str) or not _REVIEWED_PACKAGE.fullmatch(package):
            raise RuntimePortAdapterContractError(
                "custom_dependency_shape",
                "custom adapter dependency package is invalid",
                stage="environment_preflight",
            )
        if not isinstance(version, str) or not _REVIEWED_VERSION.fullmatch(version):
            raise RuntimePortAdapterContractError(
                "custom_dependency_shape",
                "custom adapter dependency version is invalid",
                stage="environment_preflight",
            )
        if (
            not isinstance(modules, list | tuple)
            or not modules
            or len(modules) > 256
            or any(not isinstance(module, str) or not _REVIEWED_MODULE.fullmatch(module) for module in modules)
            or len(set(modules)) != len(modules)
        ):
            raise RuntimePortAdapterContractError(
                "custom_dependency_shape",
                "custom adapter dependency modules are invalid",
                stage="environment_preflight",
            )
        package_key = _canonical_package(package)
        if package_key in packages:
            raise RuntimePortAdapterContractError(
                "custom_dependency_duplicate",
                "custom adapter dependency package is repeated",
                stage="environment_preflight",
            )
        packages[package_key] = version
        record = {"package": package, "version": version}
        for module in modules:
            if module in owners:
                raise RuntimePortAdapterContractError(
                    "custom_import_dependency",
                    f"custom adapter import {module!r} has ambiguous reviewed ownership",
                    stage="environment_preflight",
                )
            owners[module] = record
    return owners


def _validate_custom_function_definition(
    definition: ast.FunctionDef,
    *,
    role: str,
    stage: str = "adapter_resolution",
) -> None:
    """Apply the closed custom-source rules at every untrusted boundary."""

    if definition.decorator_list:
        raise RuntimePortAdapterContractError(
            "custom_source_shape",
            f"custom adapter {role} must be undecorated",
            stage=stage,
        )
    if (
        definition.args.posonlyargs
        or definition.args.vararg
        or definition.args.kwarg
        or definition.args.kwonlyargs
        or definition.args.defaults
        or definition.args.kw_defaults
    ):
        raise RuntimePortAdapterContractError(
            "custom_signature",
            f"custom adapter {role} must use ordinary positional-or-keyword parameters",
            stage=stage,
        )
    expected_arity = 2 if role == "save" else 1
    if len(definition.args.args) != expected_arity:
        raise RuntimePortAdapterContractError(
            "custom_signature",
            f"custom adapter {role} must accept exactly {expected_arity} argument(s)",
            stage=stage,
        )
    for node in ast.walk(definition):
        if isinstance(
            node, (ast.AsyncFunctionDef, ast.ClassDef, ast.Global, ast.Nonlocal, ast.Yield, ast.YieldFrom, ast.Await)
        ):
            raise RuntimePortAdapterContractError(
                "custom_construct",
                f"custom adapter {role} contains an unsupported source construct",
                stage=stage,
            )
        if isinstance(node, ast.ImportFrom) and node.level:
            raise RuntimePortAdapterContractError(
                "custom_import",
                f"custom adapter {role} may use only absolute imports",
                stage=stage,
            )
        if isinstance(node, ast.Call):
            dynamic_name = None
            if isinstance(node.func, ast.Name) and node.func.id in _DYNAMIC_CALLS:
                dynamic_name = node.func.id
            elif isinstance(node.func, ast.Attribute) and node.func.attr in _DYNAMIC_CALLS:
                dynamic_name = node.func.attr
            if dynamic_name is not None:
                raise RuntimePortAdapterContractError(
                    "custom_dynamic_call",
                    f"custom adapter {role} may not call {dynamic_name}",
                    stage=stage,
                )
        if isinstance(node, (ast.FunctionDef, ast.Lambda)) and node is not definition:
            raise RuntimePortAdapterContractError(
                "custom_nested_callable",
                f"custom adapter {role} may not contain nested functions or lambdas",
                stage=stage,
            )
    unresolved = _unresolved_function_names(definition)
    if unresolved:
        raise RuntimePortAdapterContractError(
            "custom_unresolved_global",
            "custom adapter {} has unresolved global name(s): {}. Use absolute local imports and literals.".format(
                role,
                ", ".join(unresolved),
            ),
            stage=stage,
        )


def _unresolved_function_names(definition: ast.FunctionDef) -> list[str]:
    local = {argument.arg for argument in definition.args.args}
    for node in ast.walk(definition):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                local.add(alias.asname or alias.name.split(".", 1)[0])
        elif isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Param)):
            local.add(node.id)
    builtins_allowed = set(vars(builtins)) | {"None", "True", "False"}
    loaded = {node.id for node in ast.walk(definition) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}
    return sorted(loaded - local - builtins_allowed)


def _validate_function_imports(
    definition: ast.FunctionDef,
    *,
    declared_distributions: Mapping[str, Mapping[str, str]],
    role: str,
) -> list[dict[str, str]]:
    """Require every non-stdlib import root to be an exact declared package."""

    package_owners = importlib_metadata.packages_distributions()
    result: dict[str, dict[str, str]] = {}
    for root in _function_import_roots(definition):
        owners = {_canonical_package(owner): owner for owner in package_owners.get(root, [])}
        if not root or len(owners) != 1:
            raise RuntimePortAdapterContractError(
                "custom_import_dependency",
                f"custom adapter {role} import {root!r} is not an exact declared distribution",
                stage="environment_preflight",
            )
        [owner_key] = owners
        if owner_key not in declared_distributions:
            raise RuntimePortAdapterContractError(
                "custom_import_dependency",
                f"custom adapter {role} import {root!r} is not an exact declared distribution",
                stage="environment_preflight",
            )
        declared = declared_distributions[owner_key]
        result[root] = {
            "module": root,
            "package": declared["package"],
            "version": declared["version"],
        }
    return sorted(result.values(), key=lambda item: (item["module"], _canonical_package(item["package"])))


def _function_import_roots(definition: ast.FunctionDef) -> list[str]:
    stdlib = set(getattr(sys, "stdlib_module_names", ()))
    roots: set[str] = set()
    for node in ast.walk(definition):
        if isinstance(node, ast.Import):
            candidates = [alias.name.split(".", 1)[0] for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            candidates = [str(node.module or "").split(".", 1)[0]]
        else:
            continue
        roots.update(root for root in candidates if root and root not in stdlib)
    return sorted(roots)


def normalize_wire_document(value: Any, *, allow_content: bool) -> dict[str, Any]:
    """Validate one complete runtime-port-adapter wire document."""

    document = _mapping(value, "runtime_port_adapters")
    _exact_keys(document, _DOCUMENT_KEYS, "runtime_port_adapters")
    if document["schema_version"] != RUNTIME_PORT_ADAPTERS_SCHEMA_VERSION:
        raise RuntimePortAdapterContractError("schema_version", "runtime_port_adapters schema_version must be 1")
    raw_bindings = _list(document["bindings"], "runtime_port_adapters.bindings")
    if len(raw_bindings) > MAX_RUNTIME_ADAPTER_BINDINGS:
        raise RuntimePortAdapterContractError("bindings_size", "too many runtime adapter bindings")
    bindings = [_normalize_binding(item) for item in raw_bindings]
    binding_keys = [(item["direction"], item["port"]) for item in bindings]
    if len(set(binding_keys)) != len(binding_keys):
        raise RuntimePortAdapterContractError("binding_duplicate", "runtime adapter canonical port is duplicated")

    raw_inputs = _list(document["inputs"], "runtime_port_adapters.inputs")
    if len(raw_inputs) > MAX_RUNTIME_INPUT_ARTIFACTS:
        raise RuntimePortAdapterContractError("inputs_size", "too many runtime input artifacts")
    inputs = [_normalize_input(item, allow_content=allow_content) for item in raw_inputs]
    input_names = [item["name"] for item in inputs]
    if len(set(input_names)) != len(input_names):
        raise RuntimePortAdapterContractError("input_duplicate", "runtime input artifact name is duplicated")
    if sum(item["size"] for item in inputs) > MAX_RUNTIME_INPUT_TOTAL_BYTES:
        raise RuntimePortAdapterContractError("input_total_size", "runtime input artifacts exceed the total limit")
    if allow_content and sum(len(cast(str, item["content_base64"])) for item in inputs) > _maximum_input_base64_chars():
        raise RuntimePortAdapterContractError(
            "input_total_content_size",
            "runtime input encoded content exceeds the aggregate limit",
        )
    known_inputs = set(input_names)
    input_by_name = {item["name"]: item for item in inputs}
    for binding in bindings:
        input_name = binding["input_name"]
        if binding["direction"] == "input" and binding["transport"] == "artifact":
            if input_name not in known_inputs:
                raise RuntimePortAdapterContractError(
                    "binding_input_missing",
                    f"input binding {binding['port']!r} references an undeclared staged artifact",
                )
            input_record = input_by_name[input_name]
            adapter = binding["adapter"]
            expected = (
                binding["port"],
                adapter["format_tag"],
                binding["semantic_type"],
                adapter["id"],
                adapter["presentation"]["media_type"],
            )
            observed = (
                input_record["port"],
                input_record["format_tag"],
                input_record["semantic_type"],
                input_record["adapter_id"],
                input_record["media_type"],
            )
            identity_matches = observed[:-1] == expected[:-1]
            media_matches = observed[-1] == expected[-1] or (adapter["id"] == OPAQUE_FILE and observed[-1] is not None)
            if not identity_matches or not media_matches:
                raise RuntimePortAdapterContractError(
                    "binding_input_identity",
                    f"input artifact {input_name!r} does not match its exact binding identity",
                )
        elif input_name is not None:
            raise RuntimePortAdapterContractError(
                "binding_input_unexpected",
                f"binding {binding['port']!r} must not declare input_name",
            )

    custom_bundle = document["custom_bundle"]
    normalized_bundle = None if custom_bundle is None else _normalize_bundle(custom_bundle, allow_content=allow_content)
    if normalized_bundle is not None and normalized_bundle["name"] in known_inputs:
        raise RuntimePortAdapterContractError(
            "input_bundle_name_collision",
            "runtime input artifact name must not collide with the custom adapter bundle namespace",
            stage="admission",
        )
    custom_digests = {
        binding["adapter"]["bundle_sha256"] for binding in bindings if binding["adapter"]["kind"] == "custom"
    }
    if custom_digests:
        if normalized_bundle is None or custom_digests != {normalized_bundle["sha256"]}:
            raise RuntimePortAdapterContractError(
                "custom_bundle_binding",
                "custom adapter bindings must reference the singular declared bundle digest",
            )
        bound_symbols = {
            symbol
            for binding in bindings
            if binding["adapter"]["kind"] == "custom"
            for symbol in (binding["adapter"]["save_symbol"], binding["adapter"]["load_symbol"])
        }
        if set(normalized_bundle["functions"]) != bound_symbols:
            raise RuntimePortAdapterContractError(
                "custom_bundle_functions",
                "custom bundle functions must exactly equal the bound save/load symbols",
            )
    elif normalized_bundle is not None:
        raise RuntimePortAdapterContractError(
            "custom_bundle_unused",
            "a custom bundle was supplied without any custom adapter binding",
        )
    return {
        "schema_version": RUNTIME_PORT_ADAPTERS_SCHEMA_VERSION,
        "bindings": sorted(bindings, key=lambda item: (item["direction"], item["port"])),
        "inputs": sorted(inputs, key=lambda item: item["name"]),
        "custom_bundle": normalized_bundle,
    }


def validate_custom_bundle_dependencies(
    source_bytes: bytes,
    document: Mapping[str, Any],
    *,
    allow_content: bool = True,
    verify_installed_environment: bool = False,
) -> None:
    """Prove custom bundle imports are covered by every bound descriptor.

    ``verify_installed_environment`` is reserved for the isolated Run worker.
    It closes the final trust gap by proving that each wire-declared import
    root is owned by one exact installed distribution before bundle code is
    compiled or executed.  Control-plane callers intentionally leave it off:
    dependency resolution may target an environment other than their own.
    """

    normalized = normalize_wire_document(document, allow_content=allow_content)
    bundle = normalized["custom_bundle"]
    if bundle is None:
        if source_bytes:
            raise RuntimePortAdapterContractError(
                "custom_bundle_unused",
                "custom bundle bytes were supplied without a custom binding",
                stage="environment_preflight",
            )
        return
    try:
        source = source_bytes.decode("utf-8")
        tree = ast.parse(source, filename="<runtime-custom-adapters>", mode="exec")
    except (UnicodeDecodeError, SyntaxError):
        raise RuntimePortAdapterContractError(
            "custom_bundle_source",
            "custom adapter bundle is not valid UTF-8 Python source",
            stage="environment_preflight",
        ) from None
    definitions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef) and not node.decorator_list
    }
    if len(definitions) != len(tree.body) or set(definitions) != set(bundle["functions"]):
        raise RuntimePortAdapterContractError(
            "custom_bundle_functions",
            "custom adapter bundle definitions do not match its descriptor",
            stage="environment_preflight",
        )
    headers_by_symbol: dict[str, dict[str, tuple[str, str]]] = {}
    for line in source.splitlines():
        if not line.startswith("# spl-runtime-import-v1"):
            continue
        match = _IMPORT_HEADER.fullmatch(line)
        if match is None:
            raise RuntimePortAdapterContractError(
                "custom_import_metadata",
                "custom adapter import metadata is malformed",
                stage="environment_preflight",
            )
        symbol, module, package, version = match.groups()
        rows = headers_by_symbol.setdefault(symbol, {})
        if module in rows:
            raise RuntimePortAdapterContractError(
                "custom_import_metadata",
                "custom adapter import metadata is duplicated",
                stage="environment_preflight",
            )
        rows[module] = (package, version)
    packages_by_symbol: dict[str, set[tuple[str, str]]] = {}
    roles_by_symbol: dict[str, set[str]] = {}
    for binding in normalized["bindings"]:
        adapter = binding["adapter"]
        if adapter["kind"] != "custom":
            continue
        packages = {(_canonical_package(item["package"]), item["version"]) for item in adapter["distributions"]}
        for role, symbol in (("save", adapter["save_symbol"]), ("load", adapter["load_symbol"])):
            existing = packages_by_symbol.get(symbol)
            packages_by_symbol[symbol] = packages if existing is None else existing & packages
            roles_by_symbol.setdefault(symbol, set()).add(role)
    if set(headers_by_symbol) - set(definitions):
        raise RuntimePortAdapterContractError(
            "custom_import_metadata",
            "custom adapter import metadata names an undeclared function",
            stage="environment_preflight",
        )
    installed_owners = importlib_metadata.packages_distributions() if verify_installed_environment else {}
    for symbol, definition in definitions.items():
        roles = roles_by_symbol.get(symbol, set())
        if len(roles) != 1:
            raise RuntimePortAdapterContractError(
                "custom_signature",
                f"custom adapter function {symbol!r} must have exactly one save/load role",
                stage="environment_preflight",
            )
        [role] = roles
        _validate_custom_function_definition(
            definition,
            role=role,
            stage="worker_load" if verify_installed_environment else "environment_preflight",
        )
        roots = _function_import_roots(definition)
        headers = headers_by_symbol.get(symbol, {})
        if set(headers) != set(roots):
            raise RuntimePortAdapterContractError(
                "custom_import_dependency",
                f"custom adapter function {symbol!r} import metadata is incomplete",
                stage="environment_preflight",
            )
        declared = packages_by_symbol.get(symbol, set())
        if any((_canonical_package(package), version) not in declared for package, version in headers.values()):
            raise RuntimePortAdapterContractError(
                "custom_import_dependency",
                f"custom adapter function {symbol!r} imports an undeclared exact distribution",
                stage="environment_preflight",
            )
        if verify_installed_environment:
            for module, (package, version) in headers.items():
                expected_package = _canonical_package(package)
                owners = {_canonical_package(owner): owner for owner in installed_owners.get(module, [])}
                if set(owners) != {expected_package}:
                    raise RuntimePortAdapterContractError(
                        "custom_import_environment_owner",
                        f"custom adapter import {module!r} is not owned by its singular declared distribution",
                        stage="worker_load",
                    )
                try:
                    installed_version = importlib_metadata.version(owners[expected_package])
                except importlib_metadata.PackageNotFoundError:
                    raise RuntimePortAdapterContractError(
                        "custom_import_environment_missing",
                        f"custom adapter distribution {package!r} is not installed",
                        stage="worker_load",
                    ) from None
                if installed_version != version:
                    raise RuntimePortAdapterContractError(
                        "custom_import_environment_version",
                        f"custom adapter distribution {package!r} does not match its declared exact version",
                        stage="worker_load",
                    )


def _normalize_binding(value: Any) -> dict[str, Any]:
    binding = _mapping(value, "runtime adapter binding")
    _exact_keys(binding, _BINDING_KEYS, "runtime adapter binding")
    direction = binding["direction"]
    if direction not in {"input", "output"}:
        raise RuntimePortAdapterContractError("binding_direction", "binding direction must be input or output")
    port = _required_port(binding["port"])
    external_name = _required_text(binding["external_name"], "binding external_name", maximum=128)
    semantic_type = None if binding["semantic_type"] is None else _required_semantic_type(binding["semantic_type"])
    adapter = _normalize_adapter_descriptor(binding["adapter"])
    source = binding["resolution_source"]
    if source not in {"system_default", "preset", "run_override"}:
        raise RuntimePortAdapterContractError("binding_source", "binding resolution_source is not recognized")
    adapter_type = str(adapter["key"]).rpartition("@")[0]
    if source == "system_default" and not semantic_type_compatible(
        semantic_type, adapter_type, adapter_id=adapter["id"]
    ):
        raise RuntimePortAdapterContractError(
            "semantic_type_mismatch",
            f"adapter {adapter['id']!r} is incompatible with binding semantic type {semantic_type!r}",
        )
    transport = binding["transport"]
    if transport not in {"inline_json", "artifact"}:
        raise RuntimePortAdapterContractError("binding_transport", "binding transport is not recognized")
    if (transport == "inline_json") != (adapter["id"] == JSON):
        raise RuntimePortAdapterContractError(
            "binding_inline_adapter",
            "adapter 'json' requires inline_json transport and every other adapter requires artifact transport",
        )
    argument = None if binding["argument"] is None else _normalize_argument(binding["argument"])
    if direction == "input" and argument is None:
        raise RuntimePortAdapterContractError("binding_argument", "input bindings require an argument location")
    if direction == "output" and argument is not None:
        raise RuntimePortAdapterContractError("binding_argument", "output bindings must not have an argument location")
    result_path = binding["result_path"]
    if not isinstance(result_path, list) or any(not isinstance(item, str) or not item for item in result_path):
        raise RuntimePortAdapterContractError("binding_result_path", "binding result_path must be a list of names")
    if direction == "input" and result_path:
        raise RuntimePortAdapterContractError("binding_result_path", "input binding result_path must be empty")
    return {
        "direction": direction,
        "port": port,
        "external_name": external_name,
        "semantic_type": semantic_type,
        "adapter": adapter,
        "resolution_source": source,
        "transport": transport,
        "argument": argument,
        "input_name": None if binding["input_name"] is None else _required_name(binding["input_name"]),
        "result_path": list(result_path),
    }


def _normalize_adapter_descriptor(value: Any) -> dict[str, Any]:
    adapter = _mapping(value, "adapter descriptor")
    _exact_keys(adapter, _ADAPTER_KEYS, "adapter descriptor")
    kind = adapter["kind"]
    if kind not in {"builtin", "custom"}:
        raise RuntimePortAdapterContractError("adapter_kind", "adapter kind must be builtin or custom")
    adapter_id = _required_text(adapter["id"], "adapter id", maximum=256)
    if kind == "builtin" and adapter_id not in BUILTIN_ADAPTER_IDS:
        raise RuntimePortAdapterContractError("adapter_id", f"unknown built-in adapter {adapter_id!r}")
    key = _required_text(adapter["key"], "adapter key", maximum=512)
    semantic_type, separator, key_format = key.rpartition("@")
    if not separator or not semantic_type or not key_format:
        raise RuntimePortAdapterContractError("adapter_key", "adapter key must be `<python_type>@<format>`")
    _required_semantic_type(semantic_type)
    format_tag = _required_tag(adapter["format_tag"], "adapter format_tag")
    if key_format != format_tag:
        raise RuntimePortAdapterContractError("adapter_key_format", "adapter key format must match format_tag")
    accepted = adapter["accepted_tags"]
    if not isinstance(accepted, list) or not accepted:
        raise RuntimePortAdapterContractError("adapter_accepted_tags", "accepted_tags must be a non-empty list")
    accepted_tags = sorted({_required_tag(item, "accepted tag") for item in accepted})
    if len(accepted_tags) != len(accepted):
        raise RuntimePortAdapterContractError("adapter_accepted_tags", "accepted_tags must not contain duplicates")
    if format_tag not in accepted_tags:
        raise RuntimePortAdapterContractError("adapter_accepted_tags", "accepted_tags must contain format_tag")
    distributions = _normalize_distribution_dicts(adapter["distributions"])
    presentation = _mapping(adapter["presentation"], "adapter presentation")
    _exact_keys(presentation, _PRESENTATION_KEYS, "adapter presentation")
    media_type = presentation["media_type"]
    preferred_extension = presentation["preferred_extension"]
    if media_type is not None and (not isinstance(media_type, str) or not _MEDIA_TYPE.fullmatch(media_type)):
        raise RuntimePortAdapterContractError("adapter_media_type", "adapter media_type is invalid")
    if preferred_extension is not None and (
        not isinstance(preferred_extension, str)
        or not re.fullmatch(r"\.[A-Za-z0-9][A-Za-z0-9._-]{0,15}", preferred_extension)
    ):
        raise RuntimePortAdapterContractError("adapter_extension", "adapter preferred_extension is invalid")
    save_symbol = adapter["save_symbol"]
    load_symbol = adapter["load_symbol"]
    bundle_sha256 = adapter["bundle_sha256"]
    if kind == "builtin":
        if save_symbol is not None or load_symbol is not None or bundle_sha256 is not None:
            raise RuntimePortAdapterContractError(
                "adapter_builtin_code", "built-in descriptors cannot name bundle code"
            )
        candidate = get_builtin_adapter(adapter_id)
        expected_presentation = builtin_presentation(adapter_id)
        expected_packages = {_canonical_package(package) for package in builtin_required_packages(adapter_id)}
        observed_packages = {_canonical_package(item["package"]) for item in distributions}
        if (
            key != candidate.key
            or format_tag != candidate.tag
            or accepted_tags != sorted(candidate.accepted_tags)
            or {
                "media_type": None if media_type is None else media_type.casefold(),
                "preferred_extension": preferred_extension,
            }
            != expected_presentation
            or observed_packages != expected_packages
        ):
            raise RuntimePortAdapterContractError(
                "adapter_builtin_identity",
                f"built-in adapter {adapter_id!r} does not match its exact registry identity",
            )
    else:
        if not adapter_id.startswith("custom:") or adapter_id.removeprefix("custom:") != bundle_sha256:
            raise RuntimePortAdapterContractError("adapter_custom_id", "custom adapter id must bind its bundle digest")
        if not _SHA256.fullmatch(cast(str, bundle_sha256)):
            raise RuntimePortAdapterContractError("adapter_bundle_digest", "custom adapter bundle digest is invalid")
        if not _python_identifier(save_symbol) or not _python_identifier(load_symbol):
            raise RuntimePortAdapterContractError("adapter_symbols", "custom adapter symbols are invalid")
    return {
        "kind": kind,
        "id": adapter_id,
        "key": key,
        "format_tag": format_tag,
        "accepted_tags": accepted_tags,
        "distributions": distributions,
        "save_symbol": save_symbol,
        "load_symbol": load_symbol,
        "bundle_sha256": bundle_sha256,
        "presentation": {
            "media_type": None if media_type is None else media_type.casefold(),
            "preferred_extension": preferred_extension,
        },
    }


def _normalize_argument(value: Any) -> dict[str, Any]:
    argument = _mapping(value, "binding argument")
    _exact_keys(argument, _ARGUMENT_KEYS, "binding argument")
    kind = argument["kind"]
    if kind == "keyword":
        name = _required_text(argument["name"], "keyword argument name", maximum=128)
        if argument["index"] is not None:
            raise RuntimePortAdapterContractError("argument_index", "keyword argument index must be null")
        return {"kind": kind, "name": name, "index": None}
    if kind == "positional":
        index = argument["index"]
        if type(index) is not int or index < 0:
            raise RuntimePortAdapterContractError("argument_index", "positional argument index must be non-negative")
        if argument["name"] is not None:
            raise RuntimePortAdapterContractError("argument_name", "positional argument name must be null")
        return {"kind": kind, "name": None, "index": index}
    if kind == "default":
        name = _required_text(argument["name"], "default argument name", maximum=128)
        if argument["index"] is not None:
            raise RuntimePortAdapterContractError("argument_index", "default argument index must be null")
        return {"kind": kind, "name": name, "index": None}
    raise RuntimePortAdapterContractError("argument_kind", "argument kind must be keyword, positional, or default")


def _normalize_input(value: Any, *, allow_content: bool) -> dict[str, Any]:
    item = _mapping(value, "runtime input artifact")
    _exact_keys(item, _INPUT_KEYS, "runtime input artifact")
    name = _required_name(item["name"])
    port = _required_port(item["port"])
    size = item["size"]
    if type(size) is not int or size < 0 or size > MAX_RUNTIME_INPUT_BYTES:
        raise RuntimePortAdapterContractError("input_size", "runtime input artifact size is out of bounds")
    sha256 = item["sha256"]
    if not isinstance(sha256, str) or not _SHA256.fullmatch(sha256):
        raise RuntimePortAdapterContractError("input_sha256", "runtime input artifact sha256 is invalid")
    content = item["content_base64"]
    staged_name = item["staged_name"]
    if content is not None and (not allow_content or not isinstance(content, str)):
        raise RuntimePortAdapterContractError("input_content", "embedded runtime input content is not allowed here")
    if content is not None and len(content) != _base64_encoded_size(size):
        raise RuntimePortAdapterContractError(
            "input_content_size",
            "runtime input encoded length does not match its declared size",
        )
    if staged_name is not None:
        if allow_content:
            raise RuntimePortAdapterContractError("input_staged_name", "staged runtime input name is not allowed here")
        staged_name = _required_name(staged_name)
    if (content is None) == (staged_name is None):
        raise RuntimePortAdapterContractError(
            "input_materialization",
            "runtime input must contain exactly one of content_base64 or staged_name",
        )
    return {
        "name": name,
        "port": port,
        "size": size,
        "sha256": sha256,
        "format_tag": _required_tag(item["format_tag"], "input format_tag"),
        "semantic_type": None if item["semantic_type"] is None else _required_semantic_type(item["semantic_type"]),
        "adapter_id": _required_text(item["adapter_id"], "input adapter_id", maximum=256),
        "media_type": _optional_media_type(item["media_type"]),
        "content_base64": content,
        "staged_name": staged_name,
    }


def _normalize_bundle(value: Any, *, allow_content: bool) -> dict[str, Any]:
    bundle = _mapping(value, "custom adapter bundle")
    _exact_keys(bundle, _CUSTOM_BUNDLE_KEYS, "custom adapter bundle")
    name = _required_name(bundle["name"])
    if name != "custom-adapters.py":
        raise RuntimePortAdapterContractError("custom_bundle_name", "custom bundle name must be custom-adapters.py")
    size = bundle["size"]
    if type(size) is not int or size < 1 or size > MAX_CUSTOM_BUNDLE_BYTES:
        raise RuntimePortAdapterContractError("custom_bundle_size", "custom bundle size is out of bounds")
    sha256 = bundle["sha256"]
    if not isinstance(sha256, str) or not _SHA256.fullmatch(sha256):
        raise RuntimePortAdapterContractError("custom_bundle_sha256", "custom bundle sha256 is invalid")
    functions = bundle["functions"]
    if (
        not isinstance(functions, list)
        or not functions
        or any(not _python_identifier(item) for item in functions)
        or len(set(functions)) != len(functions)
    ):
        raise RuntimePortAdapterContractError("custom_bundle_functions", "custom bundle functions are invalid")
    content = bundle["content_base64"]
    staged_name = bundle["staged_name"]
    if content is not None and (not allow_content or not isinstance(content, str)):
        raise RuntimePortAdapterContractError("custom_bundle_content", "embedded custom bundle content is not allowed")
    if content is not None and len(content) != _base64_encoded_size(size):
        raise RuntimePortAdapterContractError(
            "custom_bundle_content_size",
            "custom bundle encoded length does not match its declared size",
        )
    if staged_name is not None:
        if allow_content:
            raise RuntimePortAdapterContractError("custom_bundle_staged", "staged custom bundle name is not allowed")
        staged_name = _required_name(staged_name)
        if staged_name != name:
            raise RuntimePortAdapterContractError(
                "custom_bundle_staged",
                "staged custom bundle name must match its declared safe name",
            )
    if (content is None) == (staged_name is None):
        raise RuntimePortAdapterContractError(
            "custom_bundle_materialization",
            "custom bundle must contain exactly one of content_base64 or staged_name",
        )
    return {
        "name": name,
        "size": size,
        "sha256": sha256,
        "functions": sorted(cast(list[str], functions)),
        "content_base64": content,
        "staged_name": staged_name,
    }


def normalize_runtime_output_record(value: Any) -> dict[str, Any]:
    """Validate one closed worker/server runtime-output identity before I/O."""

    record = _mapping(value, "runtime adapter output")
    _exact_keys(record, _OUTPUT_RECORD_KEYS, "runtime adapter output")
    size = record["size"]
    if type(size) is not int or size < 0 or size > MAX_RUNTIME_INPUT_BYTES:
        raise RuntimePortAdapterContractError(
            "output_size",
            "runtime adapter output size is out of bounds",
            stage="download",
        )
    sha256 = record["sha256"]
    if not isinstance(sha256, str) or not _SHA256.fullmatch(sha256):
        raise RuntimePortAdapterContractError(
            "output_sha256",
            "runtime adapter output sha256 is invalid",
            stage="download",
        )
    adapter_id = _required_text(record["adapter_id"], "output adapter_id", maximum=256)
    if adapter_id not in BUILTIN_ADAPTER_IDS and not any(
        adapter_id.startswith(prefix) and _SHA256.fullmatch(adapter_id.removeprefix(prefix))
        for prefix in ("custom:", "library:")
    ):
        raise RuntimePortAdapterContractError(
            "output_adapter_id",
            "runtime adapter output adapter_id is invalid",
            stage="download",
        )
    result_path = record["result_path"]
    if not isinstance(result_path, list) or any(not isinstance(item, str) or not item for item in result_path):
        raise RuntimePortAdapterContractError(
            "output_result_path",
            "runtime adapter output result_path is invalid",
            stage="download",
        )
    return {
        "port": _required_port(record["port"]),
        "name": _required_name(record["name"]),
        "size": size,
        "sha256": sha256,
        "format_tag": (
            None
            if record["format_tag"] is None and adapter_id.startswith("library:")
            else _required_tag(record["format_tag"], "output format_tag")
        ),
        "adapter_id": adapter_id,
        "media_type": _optional_media_type(record["media_type"]),
        "result_path": list(result_path),
    }


def manifest_port_adapter_section(
    document: Mapping[str, Any],
    *,
    adapter_policy: Mapping[str, Any],
    custom_remote_allowed: bool,
    custom_remote_used: bool,
) -> dict[str, Any]:
    """Return source- and body-free Run manifest evidence."""

    normalized = normalize_wire_document(document, allow_content=False)
    bindings = []
    for binding in normalized["bindings"]:
        adapter = binding["adapter"]
        input_record = next(
            (item for item in normalized["inputs"] if item["name"] == binding["input_name"]),
            None,
        )
        bindings.append(
            {
                "direction": binding["direction"],
                "port": binding["port"],
                "semantic_type": binding["semantic_type"],
                "format_tag": adapter["format_tag"],
                "resolution_source": binding["resolution_source"],
                "adapter_id": adapter["id"],
                "adapter_key": adapter["key"],
                "bundle_sha256": adapter["bundle_sha256"],
                "distributions": adapter["distributions"],
                "artifact_name": None if input_record is None else input_record["name"],
                "artifact_sha256": None if input_record is None else input_record["sha256"],
                "artifact_size": None if input_record is None else input_record["size"],
                "media_type": (
                    adapter["presentation"]["media_type"] if input_record is None else input_record["media_type"]
                ),
                "result_path": binding["result_path"],
            }
        )
    policy = normalize_adapter_policy(adapter_policy)
    custom_requested = policy["custom_remote"] == "allow"
    section = {
        "schema_version": RUNTIME_PORT_ADAPTERS_SCHEMA_VERSION,
        "bindings": bindings,
        "custom_remote": {
            "requested": custom_requested,
            "allowed": bool(custom_remote_allowed),
            "used": bool(custom_remote_used),
        },
        "fingerprint_sha256": runtime_port_adapter_fingerprint(normalized, adapter_policy=policy),
        "failure": None,
    }
    return section


def runtime_port_adapter_fingerprint(
    document: Mapping[str, Any],
    *,
    adapter_policy: Mapping[str, Any],
) -> str:
    """Hash exact identities and input digests without hashing bodies or paths."""

    normalized = normalize_wire_document(document, allow_content=False)
    safe = {
        "schema_version": normalized["schema_version"],
        "bindings": normalized["bindings"],
        "inputs": [
            {key: item[key] for key in ("name", "port", "size", "sha256", "format_tag", "semantic_type", "adapter_id")}
            for item in normalized["inputs"]
        ],
        "custom_bundle": (
            None
            if normalized["custom_bundle"] is None
            else {key: normalized["custom_bundle"][key] for key in ("name", "size", "sha256", "functions")}
        ),
        "adapter_policy": normalize_adapter_policy(adapter_policy),
    }
    encoded = json_dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(b"spl.runtime-port-adapters-fingerprint/v1\0" + encoded).hexdigest()


def merge_adapter_distributions(
    object_distributions: Sequence[Mapping[str, Any]],
    document: Mapping[str, Any],
) -> list[dict[str, str]]:
    """Merge exact Run adapter dependencies, rejecting package conflicts."""

    normalized = normalize_wire_document(document, allow_content=False)
    merged: dict[str, dict[str, str]] = {}
    for raw in object_distributions:
        item = _normalize_distribution_mapping(raw)
        merged[_canonical_package(item["package"])] = item
    for binding in normalized["bindings"]:
        for item in binding["adapter"]["distributions"]:
            key = _canonical_package(item["package"])
            existing = merged.get(key)
            if existing is not None and existing["version"] != item["version"]:
                raise RuntimePortAdapterContractError(
                    "dependency_conflict",
                    "adapter dependency conflicts with Object environment: {} requires {} and {}".format(
                        item["package"],
                        existing["version"],
                        item["version"],
                    ),
                    stage="environment_preflight",
                )
            merged[key] = item
    return sorted(merged.values(), key=lambda item: (_canonical_package(item["package"]), item["version"]))


def port_catalog_from_signature(signature: Mapping[str, Any]) -> dict[AdapterDirection, list[PortTarget]]:
    """Build deterministic external port targets from an additive signature."""

    kind = signature.get("kind")
    inputs = signature.get("inputs")
    outputs = signature.get("outputs")
    if kind not in {"function", "pipeline"} or not isinstance(inputs, list) or not isinstance(outputs, list):
        raise RuntimePortAdapterContractError("signature_shape", "Object signature cannot resolve runtime ports")
    if kind == "function":
        input_targets = [
            PortTarget(
                canonical=_required_text(item.get("name"), "input name", maximum=128),
                short=_required_text(item.get("name"), "input name", maximum=128),
                semantic_type=(str(item.get("type")) if item.get("type") is not None else None),
                direction="input",
                external_name=_required_text(item.get("name"), "input name", maximum=128),
            )
            for item in inputs
            if isinstance(item, Mapping)
        ]
        if not outputs:
            outputs = [{"name": "default", "type": None}]
        output_targets = []
        for item in outputs:
            if not isinstance(item, Mapping):
                continue
            ports = item.get("ports")
            port_items = ports if isinstance(ports, list) and ports else [item]
            for port in port_items:
                if not isinstance(port, Mapping):
                    continue
                name = _required_text(port.get("name") or "default", "output name", maximum=128)
                output_targets.append(
                    PortTarget(
                        canonical=name,
                        short=name,
                        semantic_type=(str(port.get("type")) if port.get("type") is not None else None),
                        direction="output",
                        external_name=name,
                    )
                )
        return {"input": input_targets, "output": output_targets}

    aliases = signature.get("aliases")
    alias_by_node: dict[str, list[str]] = {}
    if isinstance(aliases, list):
        for item in aliases:
            if not isinstance(item, Mapping):
                continue
            alias = item.get("name")
            node_id = item.get("node_id")
            if isinstance(alias, str) and alias and isinstance(node_id, str) and node_id:
                alias_by_node.setdefault(node_id, []).append(alias)
    input_targets = []
    for item in inputs:
        if not isinstance(item, Mapping):
            continue
        short = _required_text(item.get("name"), "input name", maximum=128)
        sources = item.get("sources")
        source_items = sources if isinstance(sources, list) and sources else [item]
        for source in source_items:
            if not isinstance(source, Mapping):
                continue
            node_id = str(source.get("node_id") or "")
            port = str(source.get("port") or short)
            qualifiers = sorted(alias_by_node.get(node_id) or ([node_id] if node_id else []))
            canonical = f"{qualifiers[0]}.{port}" if qualifiers else short
            input_targets.append(
                PortTarget(
                    canonical=canonical,
                    short=port,
                    semantic_type=(str(item.get("type")) if item.get("type") is not None else None),
                    direction="input",
                    external_name=short,
                )
            )
    output_targets = []
    for item in outputs:
        if not isinstance(item, Mapping):
            continue
        alias = item.get("name") if item.get("selector") is not None else None
        ports = item.get("ports")
        port_items = ports if isinstance(ports, list) and ports else [item]
        for port in port_items:
            if not isinstance(port, Mapping):
                continue
            short = _required_text(port.get("name") or "default", "output name", maximum=128)
            node_id = str(item.get("node_id") or "")
            qualifier = str(alias or (sorted(alias_by_node.get(node_id) or [node_id])[0] if node_id else ""))
            canonical = f"{qualifier}.{short}" if qualifier else short
            output_targets.append(
                PortTarget(
                    canonical=canonical,
                    short=short,
                    semantic_type=(str(port.get("type")) if port.get("type") is not None else None),
                    direction="output",
                    external_name=str(alias or short),
                )
            )
    return {"input": input_targets, "output": output_targets}


def resolve_port_reference(reference: str, targets: Sequence[PortTarget]) -> PortTarget:
    """Resolve a canonical address or a unique short port name."""

    exact = [target for target in targets if target.canonical == reference]
    if len(exact) == 1:
        return exact[0]
    short = [target for target in targets if target.short == reference or target.external_name == reference]
    unique = {target.canonical: target for target in short}
    if len(unique) == 1:
        return next(iter(unique.values()))
    if not unique:
        choices = ", ".join(sorted({target.canonical for target in targets})) or "<none>"
        raise RuntimePortAdapterContractError(
            "unknown_port",
            f"unknown runtime adapter port {reference!r}; available canonical ports: {choices}",
        )
    choices = ", ".join(sorted(unique))
    raise RuntimePortAdapterContractError(
        "ambiguous_port",
        f"runtime adapter port {reference!r} is ambiguous; use one of: {choices}",
    )


def semantic_type_compatible(port_type: str | None, adapter_type: str, *, adapter_id: str) -> bool:
    """Return conservative semantic compatibility independent of extensions."""

    if port_type is None or not str(port_type).strip():
        return True
    normalized = _normalize_type_text(port_type)
    adapter_normalized = _normalize_type_text(adapter_type)
    if adapter_id == JSON:
        return normalized in {
            "Any",
            "None",
            "NoneType",
            "bool",
            "builtins.bool",
            "dict",
            "builtins.dict",
            "float",
            "builtins.float",
            "int",
            "builtins.int",
            "list",
            "builtins.list",
            "str",
            "builtins.str",
            "typing.Any",
        } or any(normalized.startswith(prefix) for prefix in ("dict[", "list[", "typing.Dict[", "typing.List["))
    adapter_short = adapter_normalized.rsplit(".", 1)[-1]
    return normalized == adapter_normalized or normalized == adapter_short or normalized.endswith("." + adapter_short)


def validate_wire_document_against_signature(
    document: Mapping[str, Any],
    *,
    signature: Mapping[str, Any],
    args: Sequence[Any] | None,
    kwargs: Mapping[str, Any] | None,
    output: str | None,
    allow_content: bool,
) -> dict[str, Any]:
    """Bind an admitted descriptor to the selected immutable Object surface."""

    normalized = normalize_wire_document(document, allow_content=allow_content)
    catalog = port_catalog_from_signature(signature)
    selected_outputs = _selected_signature_outputs(signature, catalog["output"], output)
    expected = {(target.direction, target.canonical): target for target in [*catalog["input"], *selected_outputs]}
    if len(expected) != len(catalog["input"]) + len(selected_outputs):
        raise RuntimePortAdapterContractError(
            "signature_port_duplicate",
            "Object signature contains duplicate canonical runtime ports",
            stage="admission",
        )
    observed = {(binding["direction"], binding["port"]): binding for binding in normalized["bindings"]}
    if set(observed) != set(expected):
        raise RuntimePortAdapterContractError(
            "binding_port_authority",
            "runtime adapter bindings do not exactly match the selected immutable Object ports",
            stage="admission",
        )

    input_order = signature.get("input_order")
    if not isinstance(input_order, list) or any(not isinstance(item, str) for item in input_order):
        input_order = [str(item.get("name")) for item in signature.get("inputs") or [] if isinstance(item, Mapping)]
    input_metadata = {
        str(item.get("name")): item for item in signature.get("inputs") or [] if isinstance(item, Mapping)
    }
    for identity, target in expected.items():
        binding = observed[identity]
        target_semantic_type = None if target.semantic_type is None else _required_semantic_type(target.semantic_type)
        if binding["external_name"] != target.external_name or binding["semantic_type"] != target_semantic_type:
            raise RuntimePortAdapterContractError(
                "binding_signature_identity",
                f"runtime adapter binding {target.canonical!r} contradicts the immutable Object signature",
                stage="admission",
            )
        if target.direction == "input":
            location, value = _signature_argument_value(
                target.external_name,
                input_order=cast(list[str], input_order),
                metadata=input_metadata.get(target.external_name),
                args=args,
                kwargs=kwargs,
            )
            if binding["argument"] != location:
                raise RuntimePortAdapterContractError(
                    "binding_argument_authority",
                    f"runtime adapter binding {target.canonical!r} targets the wrong call argument",
                    stage="admission",
                )
            if binding["transport"] == "inline_json":
                try:
                    validate_json_value(value, path=f"$.{target.external_name}")
                except (TypeError, ValueError):
                    raise RuntimePortAdapterContractError(
                        "binding_inline_value",
                        f"inline JSON value for {target.canonical!r} is not strict JSON",
                        stage="admission",
                    ) from None
            elif value is not None:
                raise RuntimePortAdapterContractError(
                    "binding_artifact_placeholder",
                    f"artifact binding {target.canonical!r} must replace its call value with null",
                    stage="admission",
                )
        else:
            result_path = _signature_output_result_path(
                signature,
                target,
                selected_outputs,
                output,
            )
            if binding["result_path"] != result_path:
                raise RuntimePortAdapterContractError(
                    "binding_result_authority",
                    f"runtime adapter binding {target.canonical!r} targets the wrong result path",
                    stage="admission",
                )
    input_records = {item["name"]: item for item in normalized["inputs"]}
    grouped_inputs: dict[str, list[dict[str, Any]]] = {}
    for target in catalog["input"]:
        grouped_inputs.setdefault(target.external_name, []).append(observed[("input", target.canonical)])
    for external_name, group in grouped_inputs.items():
        if len(group) < 2:
            continue
        identities = set()
        for binding in group:
            artifact = input_records.get(binding["input_name"])
            identities.add(
                json_dumps(
                    {
                        "adapter": binding["adapter"],
                        "transport": binding["transport"],
                        "argument": binding["argument"],
                        "artifact": (
                            None
                            if artifact is None
                            else {
                                key: artifact[key]
                                for key in ("size", "sha256", "format_tag", "semantic_type", "adapter_id", "media_type")
                            }
                        ),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
        if len(identities) != 1:
            raise RuntimePortAdapterContractError(
                "fanout_adapter_conflict",
                f"fan-out input {external_name!r} has inconsistent adapter or artifact identity",
                stage="admission",
            )
    return normalized


def implicit_json_wire_document(
    *,
    signature: Mapping[str, Any],
    args: Sequence[Any] | None,
    kwargs: Mapping[str, Any] | None,
    output: str | None,
) -> dict[str, Any]:
    """Derive code-free JSON port evidence without changing legacy Run wire."""

    _, descriptor = built_in_descriptor(JSON)
    catalog = port_catalog_from_signature(signature)
    selected_outputs = _selected_signature_outputs(signature, catalog["output"], output)
    input_order = signature.get("input_order")
    if not isinstance(input_order, list) or any(not isinstance(item, str) for item in input_order):
        input_order = [str(item.get("name")) for item in signature.get("inputs") or [] if isinstance(item, Mapping)]
    metadata = {str(item.get("name")): item for item in signature.get("inputs") or [] if isinstance(item, Mapping)}
    bindings: list[dict[str, Any]] = []
    for target in catalog["input"]:
        argument, value = _signature_argument_value(
            target.external_name,
            input_order=cast(list[str], input_order),
            metadata=metadata.get(target.external_name),
            args=args,
            kwargs=kwargs,
        )
        try:
            validate_json_value(value, path=f"$.{target.external_name}")
        except (TypeError, ValueError):
            raise RuntimePortAdapterContractError(
                "legacy_json_value",
                f"legacy input {target.external_name!r} is not strict JSON",
                stage="admission",
            ) from None
        bindings.append(
            {
                "direction": "input",
                "port": target.canonical,
                "external_name": target.external_name,
                "semantic_type": target.semantic_type,
                "adapter": descriptor,
                "resolution_source": "system_default",
                "transport": "inline_json",
                "argument": argument,
                "input_name": None,
                "result_path": [],
            }
        )
    for target in selected_outputs:
        bindings.append(
            {
                "direction": "output",
                "port": target.canonical,
                "external_name": target.external_name,
                "semantic_type": target.semantic_type,
                "adapter": descriptor,
                "resolution_source": "system_default",
                "transport": "inline_json",
                "argument": None,
                "input_name": None,
                "result_path": _signature_output_result_path(signature, target, selected_outputs, output),
            }
        )
    document = {
        "schema_version": RUNTIME_PORT_ADAPTERS_SCHEMA_VERSION,
        "bindings": bindings,
        "inputs": [],
        "custom_bundle": None,
    }
    return validate_wire_document_against_signature(
        document,
        signature=signature,
        args=args,
        kwargs=kwargs,
        output=output,
        allow_content=True,
    )


def implicit_json_manifest_section(
    *,
    signature: Mapping[str, Any],
    args: Sequence[Any] | None,
    kwargs: Mapping[str, Any] | None,
    output: str | None,
) -> dict[str, Any]:
    """Record implicit legacy JSON defaults without tightening legacy admission."""

    # The HTTP/storage JSON contract remains the sole legacy value authority.
    # Semantic annotations historically did not reject otherwise-valid JSON
    # (for example an ``object`` output returning ``None``), so this evidence
    # path deliberately records those annotations without feeding them through
    # the explicit adapter-wire semantic admission gate.
    try:
        validate_json_value(list(args or []), path="$.args")
        validate_json_value(dict(kwargs or {}), path="$.kwargs")
    except (TypeError, ValueError):
        raise RuntimePortAdapterContractError(
            "legacy_json_value",
            "legacy Run arguments are not strict JSON",
            stage="admission",
        ) from None
    adapter, descriptor = built_in_descriptor(JSON)
    del adapter
    catalog = port_catalog_from_signature(signature)
    try:
        selected_outputs = _selected_signature_outputs(signature, catalog["output"], output)
    except RuntimePortAdapterContractError:
        # Preserve the historical worker-side failure boundary for an invalid
        # selector while still recording every declared JSON output default.
        selected_outputs = list(catalog["output"])
    bindings = []
    for target in [*catalog["input"], *selected_outputs]:
        result_path = (
            []
            if target.direction == "input"
            else _signature_output_result_path(signature, target, selected_outputs, output)
        )
        bindings.append(
            {
                "direction": target.direction,
                "port": target.canonical,
                "semantic_type": target.semantic_type,
                "format_tag": descriptor["format_tag"],
                "resolution_source": "system_default",
                "adapter_id": descriptor["id"],
                "adapter_key": descriptor["key"],
                "bundle_sha256": None,
                "distributions": [],
                "artifact_name": None,
                "artifact_sha256": None,
                "artifact_size": None,
                "media_type": descriptor["presentation"]["media_type"],
                "result_path": result_path,
            }
        )
    bindings.sort(key=lambda item: (item["direction"], item["port"]))
    fingerprint_payload = {
        "schema_version": RUNTIME_PORT_ADAPTERS_SCHEMA_VERSION,
        "bindings": bindings,
        "adapter_policy": {"custom_remote": "deny"},
    }
    encoded = json_dumps(
        fingerprint_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "schema_version": RUNTIME_PORT_ADAPTERS_SCHEMA_VERSION,
        "bindings": bindings,
        "custom_remote": {"requested": False, "allowed": False, "used": False},
        "fingerprint_sha256": hashlib.sha256(b"spl.runtime-port-adapters-fingerprint/v1\0" + encoded).hexdigest(),
        "failure": None,
    }


def _selected_signature_outputs(
    signature: Mapping[str, Any],
    targets: Sequence[PortTarget],
    output: str | None,
) -> list[PortTarget]:
    if signature.get("kind") != "pipeline" or output is None:
        return list(targets)
    selected = [
        target for target in targets if target.external_name == output or target.canonical.startswith(output + ".")
    ]
    if not selected:
        raise RuntimePortAdapterContractError(
            "binding_output_authority",
            "selected output does not exist in the immutable Object signature",
            stage="admission",
        )
    return selected


def _signature_output_result_path(
    signature: Mapping[str, Any],
    target: PortTarget,
    targets: Sequence[PortTarget],
    output: str | None,
) -> list[str]:
    if signature.get("kind") == "function":
        return [] if len(targets) == 1 else [target.short]
    if output is not None:
        return [target.short]
    if target.external_name == target.short:
        return [target.short]
    return [target.external_name, target.short]


def _signature_argument_value(
    name: str,
    *,
    input_order: list[str],
    metadata: Mapping[str, Any] | None,
    args: Sequence[Any] | None,
    kwargs: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], Any]:
    if kwargs is not None and name in kwargs:
        return {"kind": "keyword", "name": name, "index": None}, kwargs[name]
    try:
        index = input_order.index(name)
    except ValueError:
        index = -1
    if index >= 0 and args is not None and index < len(args):
        return {"kind": "positional", "name": None, "index": index}, args[index]
    if metadata is not None and (not bool(metadata.get("required")) or metadata.get("default") is not None):
        return {"kind": "default", "name": name, "index": None}, metadata.get("default")
    raise RuntimePortAdapterContractError(
        "binding_missing_input",
        f"required immutable Object input {name!r} was not supplied",
        stage="admission",
    )


def _normalize_type_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value).strip().strip("'\""))


def _semantic_type_is_ambiguous(value: str) -> bool:
    if value.casefold() in {
        "any",
        "typing.any",
        "object",
        "builtins.object",
        "unknown",
        "typing.unknown",
        "...",
    }:
        return True
    if "|" in value or re.match(
        r"^(?:typing\.)?(?:Union|Optional|Literal|Annotated|TypeVar)(?:\[|$)",
        value,
        flags=re.IGNORECASE,
    ):
        return True
    root = value.partition("[")[0]
    canonical_root = _canonical_semantic_type(root)
    return "." not in canonical_root and canonical_root not in {"None", "NoneType"}


def _canonical_semantic_type(value: str) -> str:
    aliases = {
        "str": "builtins.str",
        "bytes": "builtins.bytes",
        "bytearray": "builtins.bytearray",
        "memoryview": "builtins.memoryview",
        "bool": "builtins.bool",
        "int": "builtins.int",
        "float": "builtins.float",
        "dict": "builtins.dict",
        "list": "builtins.list",
        "tuple": "builtins.tuple",
        "set": "builtins.set",
        "Path": "pathlib.Path",
    }
    return canonical_builtin_semantic_type(aliases.get(value, value))


def _is_declared_json_semantic_type(value: str) -> bool:
    canonical = _canonical_semantic_type(value)
    if canonical in {
        "None",
        "NoneType",
        "builtins.bool",
        "builtins.dict",
        "builtins.float",
        "builtins.int",
        "builtins.list",
        "builtins.str",
        "spl.core.json",
    }:
        return True
    return canonical.startswith(("dict[", "builtins.dict[", "list[", "builtins.list[", "typing.Dict[", "typing.List["))


def _advisory_semantic_type(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _SAFE_ADVISORY_TYPE.fullmatch(value):
        raise RuntimePortAdapterContractError(
            "semantic_advisory_type",
            f"{name} must be a safe non-empty string up to 512 characters",
            stage="admission",
        )
    return value


def _optional_advisory_semantic_type(value: Any, name: str) -> str | None:
    if value is None:
        return None
    return _advisory_semantic_type(value, name)


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise RuntimePortAdapterContractError("mapping", f"{name} must be a string-keyed mapping")
    return cast(Mapping[str, Any], value)


def _list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise RuntimePortAdapterContractError("list", f"{name} must be a list")
    return value


def _exact_keys(value: Mapping[str, Any], expected: frozenset[str], name: str) -> None:
    if set(value) != expected:
        missing = sorted(expected - set(value))
        unknown = sorted(set(value) - expected)
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unknown:
            details.append("unknown: " + ", ".join(unknown))
        raise RuntimePortAdapterContractError("closed_keys", f"{name} has invalid fields ({'; '.join(details)})")


def _required_text(value: Any, name: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise RuntimePortAdapterContractError("text", f"{name} must be a non-empty string up to {maximum} characters")
    return value


def _required_name(value: Any) -> str:
    name = _required_text(value, "artifact name", maximum=255)
    if not _SAFE_NAME.fullmatch(name) or name in {".", ".."}:
        raise RuntimePortAdapterContractError("artifact_name", "artifact name must be a safe single filename")
    return name


def _required_port(value: Any) -> str:
    port = _required_text(value, "canonical port", maximum=256)
    if not _SAFE_PORT.fullmatch(port):
        raise RuntimePortAdapterContractError("canonical_port", "canonical port must be a name or alias.port")
    return port


def _required_tag(value: Any, name: str) -> str:
    tag = _required_text(value, name, maximum=160)
    if not _SAFE_TAG.fullmatch(tag):
        raise RuntimePortAdapterContractError("adapter_tag", f"{name} contains unsupported characters")
    return tag


def _required_semantic_type(value: Any) -> str:
    semantic_type = _required_text(value, "semantic type", maximum=256)
    if not _SAFE_TYPE.fullmatch(semantic_type):
        raise RuntimePortAdapterContractError("semantic_type", "semantic type contains unsupported characters")
    return semantic_type


def _optional_media_type(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _MEDIA_TYPE.fullmatch(value):
        raise RuntimePortAdapterContractError("adapter_media_type", "media_type is invalid")
    return value.casefold()


def _base64_encoded_size(decoded_size: int) -> int:
    """Return the one canonical padded-base64 length for decoded bytes."""

    return 4 * ((decoded_size + 2) // 3)


def _maximum_input_base64_chars() -> int:
    """Bound aggregate encoded allocation including per-artifact padding."""

    return 4 * ((MAX_RUNTIME_INPUT_TOTAL_BYTES + (2 * MAX_RUNTIME_INPUT_ARTIFACTS) + 2) // 3)


def _normalize_distribution_objects(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, tuple | list):
        raise RuntimePortAdapterContractError("adapter_distributions", "adapter distributions must be a sequence")
    result = []
    for item in value:
        package = getattr(item, "package", None)
        version = getattr(item, "version", None)
        result.append(_normalize_distribution_mapping({"package": package, "version": version}))
    return sorted(result, key=lambda item: (_canonical_package(item["package"]), item["version"]))


def _normalize_distribution_dicts(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise RuntimePortAdapterContractError("adapter_distributions", "adapter distributions must be a list")
    result = []
    seen: dict[str, str] = {}
    for item in value:
        mapping = _mapping(item, "adapter distribution")
        _exact_keys(mapping, _DISTRIBUTION_KEYS, "adapter distribution")
        normalized = _normalize_distribution_mapping(mapping)
        key = _canonical_package(normalized["package"])
        previous = seen.get(key)
        if previous is not None and previous != normalized["version"]:
            raise RuntimePortAdapterContractError(
                "adapter_distribution_conflict",
                f"adapter repeats distribution {normalized['package']!r} with conflicting versions",
            )
        seen[key] = normalized["version"]
        result.append(normalized)
    return sorted(
        {(item["package"], item["version"]): item for item in result}.values(),
        key=lambda item: (_canonical_package(item["package"]), item["version"]),
    )


def _normalize_distribution_mapping(value: Mapping[str, Any]) -> dict[str, str]:
    package = value.get("package")
    version = value.get("version")
    if not isinstance(package, str) or not _PACKAGE.fullmatch(package):
        raise RuntimePortAdapterContractError("distribution_package", "distribution package is invalid")
    if not isinstance(version, str) or not _VERSION.fullmatch(version):
        raise RuntimePortAdapterContractError("distribution_version", "distribution version is invalid")
    return {"package": package, "version": version}


def _canonical_package(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def _python_identifier(value: Any) -> bool:
    return (
        isinstance(value, str) and value.isidentifier() and not keyword.iskeyword(value) and not value.startswith("_")
    )


__all__ = [
    "AdapterDirection",
    "AdapterResolutionSource",
    "AdapterTransport",
    "CustomRemotePolicy",
    "MAX_CUSTOM_BUNDLE_BYTES",
    "MAX_RUNTIME_ADAPTER_BINDINGS",
    "MAX_RUNTIME_ADAPTER_SEMANTIC_ADVISORIES",
    "MAX_RUNTIME_INPUT_ARTIFACTS",
    "MAX_RUNTIME_INPUT_BYTES",
    "MAX_RUNTIME_INPUT_TOTAL_BYTES",
    "PortTarget",
    "RUNTIME_PORT_ADAPTERS_CAPABILITY",
    "RUNTIME_PORT_ADAPTERS_CAPABILITY_VERSION",
    "RUNTIME_PORT_ADAPTERS_SCHEMA_VERSION",
    "RUNTIME_ADAPTER_SEMANTIC_ADVISORIES_SCHEMA_VERSION",
    "RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY",
    "RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY_VERSION",
    "RuntimeAdapterSemanticAcknowledgementSource",
    "RuntimeAdapterSemanticState",
    "RuntimePortAdapterContractError",
    "adapter_descriptor",
    "build_custom_bundle",
    "built_in_descriptor",
    "built_in_registry_descriptor",
    "implicit_json_manifest_section",
    "implicit_json_wire_document",
    "manifest_port_adapter_section",
    "merge_adapter_distributions",
    "normalize_adapter_policy",
    "normalize_public_adapter_mapping",
    "normalize_runtime_adapter_semantic_advisories",
    "normalize_runtime_output_record",
    "normalize_wire_document",
    "port_catalog_from_signature",
    "resolve_port_reference",
    "runtime_port_adapter_fingerprint",
    "semantic_type_compatible",
    "runtime_adapter_semantic_state",
    "runtime_adapter_semantic_category",
    "validate_custom_bundle_dependencies",
    "validate_custom_adapter_source",
    "validate_wire_document_against_signature",
]
