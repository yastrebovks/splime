"""Exact Library Adapter Run admission and private source staging.

The public Run wire keeps ``runtime_port_adapters`` v1 unchanged.  An additive
``runtime_library_adapter_refs`` sibling selects which valid v1 port bindings
are authoritatively overridden by an immutable Library Adapter version.
Executable source is re-read and re-hashed here, staged owner-only, and is not
compiled or executed until the isolated worker loads it.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spl.core.library_adapters import (
    LIBRARY_ADAPTER_CONTENT_HASH_DOMAIN,
    LIBRARY_ADAPTER_SIGNATURE_HASH_DOMAIN,
    library_adapter_execution_payload,
    library_adapter_signature_payload,
    MAX_ENVIRONMENT_DISTRIBUTIONS,
    normalize_dependencies,
    normalize_runtime_library_adapter_refs,
)
from spl.core.runtime_port_adapters import (
    MAX_CUSTOM_BUNDLE_BYTES,
    RuntimePortAdapterContractError,
    validate_custom_adapter_source,
)

RUNTIME_LIBRARY_ADAPTERS_DIRECTORY = "runtime-library-adapters"
MAX_RUNTIME_LIBRARY_ADAPTER_SOURCE_TOTAL_BYTES = MAX_CUSTOM_BUNDLE_BYTES

_CLAIM_DOCUMENT_KEYS = {"schema_version", "bindings", "environment_fingerprint"}
_CLAIM_BINDING_KEYS = {
    "direction",
    "port",
    "owner",
    "library",
    "name",
    "version",
    "adapter_id",
    "adapter_version_id",
    "content_hash",
    "signature_hash",
    "semantic_type",
    "semantic_category",
    "format_tag",
    "media_type",
    "preferred_extension",
    "dependencies",
    "symbols",
    "effective_policy",
    "save_source",
    "load_source",
}
_EXACT_REF_KEYS = (
    "owner",
    "library",
    "name",
    "version",
    "adapter_id",
    "adapter_version_id",
    "content_hash",
    "signature_hash",
)


@dataclass(frozen=True)
class PreparedRuntimeLibraryAdapters:
    document: dict[str, Any]
    source_bodies: dict[str, bytes]
    merged_distributions: list[dict[str, str]]


def materialize_claimed_runtime_library_adapters(
    value: Any,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Split one closed central claim into public refs and private source facts.

    The central claim is already claim-fenced, but the daemon still treats it
    as untrusted transport.  This parser accepts the source-bearing job shape
    only; :func:`prepare_runtime_library_adapters` subsequently re-parses the
    source and recomputes both immutable hashes before Run mutation.
    """

    if not isinstance(value, Mapping) or set(value) != _CLAIM_DOCUMENT_KEYS:
        raise RuntimePortAdapterContractError(
            "library_adapter_claim_shape",
            "claimed Library Adapter evidence has an invalid document shape",
            stage="admission",
        )
    if value.get("schema_version") != 1 or type(value.get("schema_version")) is not int:
        raise RuntimePortAdapterContractError(
            "library_adapter_claim_schema",
            "claimed Library Adapter evidence has an unsupported schema",
            stage="admission",
        )
    environment_fingerprint = value.get("environment_fingerprint")
    if (
        not isinstance(environment_fingerprint, str)
        or len(environment_fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in environment_fingerprint)
    ):
        raise RuntimePortAdapterContractError(
            "library_adapter_claim_environment",
            "claimed Library Adapter environment evidence is invalid",
            stage="environment_preflight",
        )
    bindings = value.get("bindings")
    if not isinstance(bindings, list):
        raise RuntimePortAdapterContractError(
            "library_adapter_claim_shape",
            "claimed Library Adapter bindings must be a list",
            stage="admission",
        )

    ref_bindings: list[dict[str, Any]] = []
    source_by_version: dict[str, dict[str, Any]] = {}
    for raw in bindings:
        if not isinstance(raw, Mapping) or set(raw) != _CLAIM_BINDING_KEYS:
            raise RuntimePortAdapterContractError(
                "library_adapter_claim_shape",
                "claimed Library Adapter binding has an invalid shape",
                stage="admission",
            )
        symbols = raw.get("symbols")
        policy = raw.get("effective_policy")
        if (
            not isinstance(symbols, Mapping)
            or set(symbols) != {"save", "load"}
            or any(value is not None and not isinstance(value, str) for value in symbols.values())
            or not isinstance(policy, Mapping)
            or set(policy) != {"local_custom", "remote_custom"}
            or any(value not in {"allow", "deny"} for value in policy.values())
        ):
            raise RuntimePortAdapterContractError(
                "library_adapter_claim_shape",
                "claimed Library Adapter callable or policy evidence is invalid",
                stage="admission",
            )
        dependencies = normalize_dependencies(raw.get("dependencies"))
        ref_binding = {
            "direction": raw["direction"],
            "port": raw["port"],
            **{key: raw[key] for key in _EXACT_REF_KEYS},
        }
        ref_bindings.append(ref_binding)
        source_record = {
            **{key: raw[key] for key in _EXACT_REF_KEYS},
            "semantic_type": raw["semantic_type"],
            "semantic_category": raw["semantic_category"],
            "save_source": raw["save_source"],
            "load_source": raw["load_source"],
            "dependencies": dependencies,
            "format_tag": raw["format_tag"],
            "media_type": raw["media_type"],
            "preferred_extension": raw["preferred_extension"],
            "policy": {
                "local_custom_code": policy["local_custom"],
                "remote_custom_code": policy["remote_custom"],
            },
        }
        version_id = str(raw["adapter_version_id"])
        prior = source_by_version.get(version_id)
        if prior is not None and prior != source_record:
            raise RuntimePortAdapterContractError(
                "library_adapter_claim_conflict",
                "claimed Library Adapter evidence conflicts for one immutable version",
                stage="admission",
            )
        source_by_version[version_id] = source_record

    refs = normalize_runtime_library_adapter_refs({"schema_version": 1, "bindings": ref_bindings})
    if not refs["bindings"]:
        raise RuntimePortAdapterContractError(
            "library_adapter_claim_empty",
            "claimed Library Adapter evidence must contain at least one binding",
            stage="admission",
        )
    return refs, [source_by_version[key] for key in sorted(source_by_version)]


def _canonical_json_hash(domain: str, value: Any) -> str:
    from spl.core.library_adapters import domain_hash

    return domain_hash(domain, value)


def _verified_record(record: Mapping[str, Any]) -> dict[str, Any]:
    dependencies = normalize_dependencies(record.get("dependencies"))
    save_source = record.get("save_source")
    load_source = record.get("load_source")
    save = (
        None
        if save_source is None
        else validate_custom_adapter_source(str(save_source), role="save", distributions=dependencies)
    )
    load = (
        None
        if load_source is None
        else validate_custom_adapter_source(str(load_source), role="load", distributions=dependencies)
    )
    if save is None and load is None:
        raise RuntimePortAdapterContractError(
            "library_adapter_functions_missing",
            "Library Adapter immutable version has no callable direction",
            stage="admission",
        )
    canonical = {
        "schema_version": 1,
        "semantic_type": record.get("semantic_type"),
        "semantic_category": record.get("semantic_category"),
        "save_source": None if save is None else save["source"],
        "load_source": None if load is None else load["source"],
        "dependencies": dependencies,
        "format_tag": record.get("format_tag"),
        "media_type": record.get("media_type"),
        "preferred_extension": record.get("preferred_extension"),
        "policy": dict(record.get("policy") or {}),
    }
    content_hash = _canonical_json_hash(
        LIBRARY_ADAPTER_CONTENT_HASH_DOMAIN,
        library_adapter_execution_payload(canonical),
    )
    signature_hash = _canonical_json_hash(
        LIBRARY_ADAPTER_SIGNATURE_HASH_DOMAIN,
        library_adapter_signature_payload(canonical, save=save, load=load),
    )
    if content_hash != record.get("content_hash") or signature_hash != record.get("signature_hash"):
        raise RuntimePortAdapterContractError(
            "library_adapter_hash_mismatch",
            "Library Adapter source/signature no longer matches its immutable reference",
            stage="admission",
        )
    return {
        **canonical,
        "content_hash": content_hash,
        "signature_hash": signature_hash,
        "save": save,
        "load": load,
    }


def _merge_distributions(
    object_distributions: Sequence[Mapping[str, Any]],
    adapter_dependencies: Sequence[Sequence[Mapping[str, Any]]],
) -> list[dict[str, str]]:
    import re

    merged: dict[str, dict[str, str]] = {}
    for group in (object_distributions, *adapter_dependencies):
        for raw in group:
            package = raw.get("package")
            version = raw.get("version")
            if not isinstance(package, str) or not isinstance(version, str):
                raise RuntimePortAdapterContractError(
                    "dependency_shape",
                    "Run dependency evidence requires exact package and version",
                    stage="environment_preflight",
                )
            key = re.sub(r"[-_.]+", "-", package).casefold()
            prior = merged.get(key)
            if prior is not None and prior["version"] != version:
                raise RuntimePortAdapterContractError(
                    "dependency_conflict",
                    f"Library Adapter dependency {package!r} conflicts with the Object environment",
                    stage="environment_preflight",
                )
            merged[key] = {"package": package, "version": version}
            if len(merged) > MAX_ENVIRONMENT_DISTRIBUTIONS:
                raise RuntimePortAdapterContractError(
                    "dependency_size",
                    "merged Object/Adapter dependencies exceed the environment bound",
                    stage="environment_preflight",
                )
    return [merged[key] for key in sorted(merged)]


def prepare_runtime_library_adapters(
    value: Any,
    *,
    runtime_document: Mapping[str, Any],
    object_distributions: Sequence[Mapping[str, Any]],
    resolve_exact: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    execution_target: str = "local",
) -> PreparedRuntimeLibraryAdapters:
    """Resolve, re-read, and verify every exact ref before Run mutation."""

    refs = normalize_runtime_library_adapter_refs(value)
    runtime_bindings = {
        (item["direction"], item["port"]): item
        for item in runtime_document.get("bindings", [])
        if isinstance(item, Mapping)
    }
    execution_bindings: list[dict[str, Any]] = []
    sources: dict[str, bytes] = {}
    dependencies: list[list[dict[str, Any]]] = []
    verified_by_version: dict[str, tuple[Mapping[str, Any], dict[str, Any], str]] = {}
    for binding_ref in refs["bindings"]:
        identity = (binding_ref["direction"], binding_ref["port"])
        runtime_binding = runtime_bindings.get(identity)
        if runtime_binding is None:
            raise RuntimePortAdapterContractError(
                "library_adapter_port_missing",
                "Library Adapter ref does not match a prepared runtime port binding",
                stage="admission",
            )
        version_id = binding_ref["adapter_version_id"]
        cached = verified_by_version.get(version_id)
        if cached is None:
            record = resolve_exact(
                {
                    key: binding_ref[key]
                    for key in (
                        "owner",
                        "library",
                        "name",
                        "version",
                        "adapter_id",
                        "adapter_version_id",
                        "content_hash",
                        "signature_hash",
                    )
                }
            )
            verified = _verified_record(record)
            policy_field = (
                "local_custom_code"
                if execution_target == "local"
                else "remote_custom_code"
                if execution_target == "remote"
                else None
            )
            if policy_field is None:
                raise RuntimePortAdapterContractError(
                    "library_adapter_target",
                    "Library Adapter execution target is not recognized",
                    stage="admission",
                )
            if verified["policy"].get(policy_field) != "allow":
                raise RuntimePortAdapterContractError(
                    f"library_adapter_{execution_target}_policy_denied",
                    f"Library Adapter version denies {execution_target} custom-code execution",
                    stage="admission",
                )
            source_text = (
                "\n".join(item["source"].rstrip() for item in (verified["save"], verified["load"]) if item is not None)
                + "\n"
            )
            source_body = source_text.encode("utf-8")
            source_name = f"library-adapter-{version_id}.py"
            sources[source_name] = source_body
            dependencies.append(verified["dependencies"])
            cached = (record, verified, source_name)
            verified_by_version[version_id] = cached
        record, verified, source_name = cached
        role = "load" if binding_ref["direction"] == "input" else "save"
        callable_record = verified[role]
        if callable_record is None:
            raise RuntimePortAdapterContractError(
                "library_adapter_direction_unsupported",
                f"Library Adapter does not support {binding_ref['direction']} port {binding_ref['port']!r}",
                stage="admission",
            )
        execution_bindings.append(
            {
                **binding_ref,
                "semantic_type": verified["semantic_type"],
                "semantic_category": verified["semantic_category"],
                "format_tag": verified["format_tag"],
                "media_type": verified["media_type"],
                "preferred_extension": verified["preferred_extension"],
                "dependencies": verified["dependencies"],
                "policy": verified["policy"],
                "symbol": callable_record["symbol"],
                "symbols": {
                    "save": None if verified["save"] is None else verified["save"]["symbol"],
                    "load": None if verified["load"] is None else verified["load"]["symbol"],
                },
                "source_name": source_name,
                "source_size": len(sources[source_name]),
                "source_sha256": hashlib.sha256(sources[source_name]).hexdigest(),
                "argument": runtime_binding.get("argument"),
                "input_name": runtime_binding.get("input_name"),
                "result_path": list(runtime_binding.get("result_path") or []),
            }
        )
    if sum(len(body) for body in sources.values()) > MAX_RUNTIME_LIBRARY_ADAPTER_SOURCE_TOTAL_BYTES:
        raise RuntimePortAdapterContractError(
            "library_adapter_source_total_size",
            "Library Adapter sources exceed the per-Run staging bound",
            stage="admission",
        )
    merged = _merge_distributions(object_distributions, dependencies)
    return PreparedRuntimeLibraryAdapters(
        document={"schema_version": 1, "bindings": execution_bindings},
        source_bodies=sources,
        merged_distributions=merged,
    )


def stage_runtime_library_adapters(
    run_dir: Path,
    prepared: PreparedRuntimeLibraryAdapters,
) -> dict[str, Any]:
    target_dir = run_dir / RUNTIME_LIBRARY_ADAPTERS_DIRECTORY
    target_dir.mkdir(mode=0o700, exist_ok=False)
    try:
        target_dir.chmod(0o700)
    except OSError:
        pass
    for name, body in prepared.source_bodies.items():
        target = target_dir / name
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(target, flags, 0o400)
        try:
            os.write(descriptor, body)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return prepared.document


def runtime_library_adapter_manifest(
    prepared: PreparedRuntimeLibraryAdapters,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "bindings": [
            {
                key: binding[key]
                for key in (
                    "direction",
                    "port",
                    "owner",
                    "library",
                    "name",
                    "version",
                    "adapter_id",
                    "adapter_version_id",
                    "content_hash",
                    "signature_hash",
                )
            }
            for binding in prepared.document["bindings"]
        ],
    }


def runtime_library_adapter_metadata(
    prepared: PreparedRuntimeLibraryAdapters,
) -> dict[str, Any]:
    """Return truthful source-free overlay metadata with its own domain hash."""

    bindings = [
        {
            key: binding[key]
            for key in (
                "direction",
                "port",
                "adapter_id",
                "adapter_version_id",
                "content_hash",
                "signature_hash",
                "semantic_type",
                "semantic_category",
                "format_tag",
                "media_type",
                "preferred_extension",
            )
        }
        | {
            "dependencies": [
                {"package": item["package"], "version": item["version"]} for item in binding["dependencies"]
            ]
        }
        for binding in prepared.document["bindings"]
    ]
    body = {"schema_version": 1, "bindings": bindings}
    encoded = json.dumps(
        body,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        **body,
        "fingerprint_sha256": hashlib.sha256(b"spl.runtime-library-adapter-manifest/v1\0" + encoded).hexdigest(),
    }


__all__ = [
    "MAX_RUNTIME_LIBRARY_ADAPTER_SOURCE_TOTAL_BYTES",
    "PreparedRuntimeLibraryAdapters",
    "RUNTIME_LIBRARY_ADAPTERS_DIRECTORY",
    "prepare_runtime_library_adapters",
    "runtime_library_adapter_manifest",
    "runtime_library_adapter_metadata",
    "stage_runtime_library_adapters",
]
