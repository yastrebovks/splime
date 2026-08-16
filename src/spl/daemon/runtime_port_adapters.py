"""Daemon admission and private staging for Run-bound adapter artifacts."""

from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable, Mapping
from typing import Any

from spl.core.runtime_port_adapters import (
    MAX_CUSTOM_BUNDLE_BYTES,
    MAX_RUNTIME_INPUT_BYTES,
    MAX_RUNTIME_INPUT_TOTAL_BYTES,
    RuntimePortAdapterContractError,
    manifest_port_adapter_section,
    merge_adapter_distributions,
    normalize_adapter_policy,
    normalize_wire_document,
    validate_custom_bundle_dependencies,
    validate_wire_document_against_signature,
)

RUNTIME_INPUTS_DIRECTORY = "runtime-inputs"

_CLAIM_KEYS = frozenset(
    {
        "schema_version",
        "bindings",
        "inputs",
        "custom_bundle",
        "adapter_policy",
        "admission_digest_sha256",
    }
)
_CLAIM_BINDING_KEYS = frozenset(
    {
        "direction",
        "port",
        "external_name",
        "semantic_type",
        "adapter_kind",
        "adapter_id",
        "key",
        "format_tag",
        "accepted_tags",
        "distributions",
        "save_symbol",
        "load_symbol",
        "bundle_sha256",
        "resolution_source",
        "transport",
        "input_name",
        "result_path",
        "presentation",
    }
)
_CLAIM_INPUT_KEYS = frozenset(
    {
        "name",
        "port",
        "size",
        "sha256",
        "format_tag",
        "semantic_type",
        "adapter_id",
        "media_type",
        "verified",
        "download_url",
    }
)
_CLAIM_BUNDLE_KEYS = frozenset({"name", "size", "sha256", "functions", "verified", "download_url"})


def _closed_claim_mapping(value: Any, keys: frozenset[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise RuntimePortAdapterContractError(
            "remote_claim_shape",
            f"{label} does not match the closed runtime adapter claim schema",
            stage="admission",
        )
    return value


def _claimed_argument_location(
    external_name: str,
    *,
    signature: Mapping[str, Any],
    args: list[Any] | None,
    kwargs: dict[str, Any] | None,
) -> dict[str, Any]:
    raw_order = signature.get("input_order")
    input_order = (
        [str(item) for item in raw_order]
        if isinstance(raw_order, list) and all(isinstance(item, str) for item in raw_order)
        else [str(item.get("name")) for item in signature.get("inputs") or [] if isinstance(item, Mapping)]
    )
    try:
        index = input_order.index(external_name)
    except ValueError as exc:
        raise RuntimePortAdapterContractError(
            "remote_claim_port",
            "runtime adapter claim references an unknown external argument",
            stage="admission",
        ) from exc
    if index < len(args or []):
        return {"kind": "positional", "name": None, "index": index}
    if external_name in (kwargs or {}):
        return {"kind": "keyword", "name": external_name, "index": None}
    return {"kind": "default", "name": external_name, "index": None}


def materialize_claimed_runtime_port_adapters(
    value: Any,
    *,
    signature: Mapping[str, Any],
    args: list[Any] | None,
    kwargs: dict[str, Any] | None,
    allow_remote_custom_adapters: bool,
    download: Callable[[str, int, str], bytes],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Verify, download and rebuild one central claim as the local wire DTO."""

    claim = _closed_claim_mapping(value, _CLAIM_KEYS, "runtime adapter claim")
    if claim["schema_version"] != 1 or isinstance(claim["schema_version"], bool):
        raise RuntimePortAdapterContractError(
            "remote_claim_schema",
            "runtime adapter claim schema_version must equal 1",
            stage="admission",
        )
    digest = claim["admission_digest_sha256"]
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise RuntimePortAdapterContractError(
            "remote_claim_digest",
            "runtime adapter claim admission digest is invalid",
            stage="admission",
        )
    policy = normalize_adapter_policy(claim["adapter_policy"])
    raw_bundle = claim["custom_bundle"]
    if raw_bundle is not None:
        _closed_claim_mapping(raw_bundle, _CLAIM_BUNDLE_KEYS, "custom adapter claim")
        if policy["custom_remote"] != "allow":
            raise RuntimePortAdapterContractError(
                "remote_custom_caller_denied",
                "custom remote adapters require explicit caller consent",
                stage="admission",
            )
        if not allow_remote_custom_adapters:
            raise RuntimePortAdapterContractError(
                "remote_custom_machine_denied",
                "this target daemon does not permit custom remote adapter execution",
                stage="admission",
            )

    raw_bindings = claim["bindings"]
    raw_inputs = claim["inputs"]
    if not isinstance(raw_bindings, list) or not isinstance(raw_inputs, list):
        raise RuntimePortAdapterContractError(
            "remote_claim_shape",
            "runtime adapter claim bindings and inputs must be lists",
            stage="admission",
        )
    bindings: list[dict[str, Any]] = []
    for raw in raw_bindings:
        binding = _closed_claim_mapping(raw, _CLAIM_BINDING_KEYS, "runtime adapter binding claim")
        argument = (
            _claimed_argument_location(
                str(binding["external_name"]),
                signature=signature,
                args=args,
                kwargs=kwargs,
            )
            if binding["direction"] == "input"
            else None
        )
        bindings.append(
            {
                "direction": binding["direction"],
                "port": binding["port"],
                "external_name": binding["external_name"],
                "semantic_type": binding["semantic_type"],
                "adapter": {
                    "kind": binding["adapter_kind"],
                    "id": binding["adapter_id"],
                    "key": binding["key"],
                    "format_tag": binding["format_tag"],
                    "accepted_tags": binding["accepted_tags"],
                    "distributions": binding["distributions"],
                    "save_symbol": binding["save_symbol"],
                    "load_symbol": binding["load_symbol"],
                    "bundle_sha256": binding["bundle_sha256"],
                    "presentation": binding["presentation"],
                },
                "resolution_source": binding["resolution_source"],
                "transport": binding["transport"],
                "argument": argument,
                "input_name": binding["input_name"],
                "result_path": binding["result_path"],
            }
        )

    preflight_inputs: list[dict[str, Any]] = []
    for raw in raw_inputs:
        item = _closed_claim_mapping(raw, _CLAIM_INPUT_KEYS, "runtime input claim")
        preflight_inputs.append(
            {
                **{
                    key: item[key]
                    for key in (
                        "name",
                        "port",
                        "size",
                        "sha256",
                        "format_tag",
                        "semantic_type",
                        "adapter_id",
                        "media_type",
                    )
                },
                "content_base64": None,
                "staged_name": item["name"],
            }
        )
    preflight_bundle = None
    if raw_bundle is not None:
        assert isinstance(raw_bundle, Mapping)
        preflight_bundle = {
            **{key: raw_bundle[key] for key in ("name", "size", "sha256", "functions")},
            "content_base64": None,
            "staged_name": raw_bundle["name"],
        }
    normalize_wire_document(
        {
            "schema_version": 1,
            "bindings": bindings,
            "inputs": preflight_inputs,
            "custom_bundle": preflight_bundle,
        },
        allow_content=False,
    )

    inputs: list[dict[str, Any]] = []
    declared_total = 0
    for raw in raw_inputs:
        item = _closed_claim_mapping(raw, _CLAIM_INPUT_KEYS, "runtime input claim")
        size = item["size"]
        sha256 = item["sha256"]
        if (
            type(size) is not int
            or size < 0
            or size > MAX_RUNTIME_INPUT_BYTES
            or not isinstance(sha256, str)
            or len(sha256) != 64
            or any(character not in "0123456789abcdef" for character in sha256)
            or item["verified"] is not True
            or not isinstance(item["download_url"], str)
        ):
            raise RuntimePortAdapterContractError(
                "remote_claim_unverified",
                "runtime input claim is not a bounded verified artifact",
                stage="download",
            )
        declared_total += size
        if declared_total > MAX_RUNTIME_INPUT_TOTAL_BYTES:
            raise RuntimePortAdapterContractError(
                "remote_claim_size",
                "runtime input claims exceed the aggregate byte limit",
                stage="download",
            )
        body = download(str(item["download_url"]), size, sha256)
        inputs.append(
            {
                **{
                    key: item[key]
                    for key in (
                        "name",
                        "port",
                        "size",
                        "sha256",
                        "format_tag",
                        "semantic_type",
                        "adapter_id",
                        "media_type",
                    )
                },
                "content_base64": base64.b64encode(body).decode("ascii"),
                "staged_name": None,
            }
        )

    bundle: dict[str, Any] | None = None
    if raw_bundle is not None:
        assert isinstance(raw_bundle, Mapping)
        bundle_size = raw_bundle["size"]
        bundle_sha256 = raw_bundle["sha256"]
        if (
            type(bundle_size) is not int
            or bundle_size < 1
            or bundle_size > MAX_CUSTOM_BUNDLE_BYTES
            or not isinstance(bundle_sha256, str)
            or len(bundle_sha256) != 64
            or any(character not in "0123456789abcdef" for character in bundle_sha256)
            or raw_bundle["verified"] is not True
            or not isinstance(raw_bundle["download_url"], str)
        ):
            raise RuntimePortAdapterContractError(
                "remote_claim_unverified",
                "custom adapter claim is not a bounded verified artifact",
                stage="download",
            )
        body = download(
            str(raw_bundle["download_url"]),
            bundle_size,
            bundle_sha256,
        )
        bundle = {
            **{key: raw_bundle[key] for key in ("name", "size", "sha256", "functions")},
            "content_base64": base64.b64encode(body).decode("ascii"),
            "staged_name": None,
        }
    document = normalize_wire_document(
        {
            "schema_version": 1,
            "bindings": bindings,
            "inputs": inputs,
            "custom_bundle": bundle,
        },
        allow_content=True,
    )
    return document, policy


@dataclass(frozen=True)
class PreparedRuntimePortAdapters:
    """Validated bodies held only until their private files are written."""

    document: dict[str, Any]
    adapter_policy: dict[str, str]
    input_bodies: dict[str, bytes]
    custom_bundle_body: bytes | None
    merged_distributions: list[dict[str, str]]


def prepare_runtime_port_adapters(
    value: Any,
    *,
    adapter_policy: Any,
    object_distributions: list[dict[str, Any]],
    signature: dict[str, Any],
    args: list[Any] | None,
    kwargs: dict[str, Any] | None,
    output: str | None,
) -> PreparedRuntimePortAdapters:
    """Validate an admission completely before the Run directory is created."""

    policy = normalize_adapter_policy(adapter_policy)
    document = validate_wire_document_against_signature(
        value,
        signature=signature,
        args=args,
        kwargs=kwargs,
        output=output,
        allow_content=True,
    )
    bodies: dict[str, bytes] = {}
    staged_inputs: list[dict[str, Any]] = []
    for item in document["inputs"]:
        try:
            body = base64.b64decode(item["content_base64"], validate=True)
        except (ValueError, TypeError) as exc:
            raise RuntimePortAdapterContractError(
                "input_base64", f"runtime input {item['name']!r} is not canonical base64", stage="admission"
            ) from exc
        if (
            base64.b64encode(body).decode("ascii") != item["content_base64"]
            or len(body) != item["size"]
            or hashlib.sha256(body).hexdigest() != item["sha256"]
        ):
            raise RuntimePortAdapterContractError(
                "input_integrity", f"runtime input {item['name']!r} failed size/checksum admission", stage="admission"
            )
        bodies[item["name"]] = body
        staged_inputs.append({**item, "content_base64": None, "staged_name": item["name"]})
    staged_bundle = None
    bundle_body = None
    if document["custom_bundle"] is not None:
        bundle = document["custom_bundle"]
        try:
            bundle_body = base64.b64decode(bundle["content_base64"], validate=True)
        except (ValueError, TypeError) as exc:
            raise RuntimePortAdapterContractError(
                "custom_bundle_base64", "custom adapter bundle is not canonical base64", stage="admission"
            ) from exc
        if (
            base64.b64encode(bundle_body).decode("ascii") != bundle["content_base64"]
            or len(bundle_body) != bundle["size"]
            or hashlib.sha256(bundle_body).hexdigest() != bundle["sha256"]
        ):
            raise RuntimePortAdapterContractError(
                "custom_bundle_integrity", "custom adapter bundle failed size/checksum admission", stage="admission"
            )
        validate_custom_bundle_dependencies(bundle_body, document)
        staged_bundle = {**bundle, "content_base64": None, "staged_name": bundle["name"]}
    staged_document = normalize_wire_document(
        {**document, "inputs": staged_inputs, "custom_bundle": staged_bundle},
        allow_content=False,
    )
    merged = merge_adapter_distributions(object_distributions, staged_document)
    return PreparedRuntimePortAdapters(staged_document, policy, bodies, bundle_body, merged)


def stage_runtime_port_adapters(
    run_dir: Path,
    prepared: PreparedRuntimePortAdapters,
) -> dict[str, Any]:
    """Write admitted bodies as owner-only regular files and return safe metadata."""

    target_dir = run_dir / RUNTIME_INPUTS_DIRECTORY
    target_dir.mkdir(mode=0o700, exist_ok=False)
    try:
        target_dir.chmod(0o700)
    except OSError:
        pass
    for name, body in prepared.input_bodies.items():
        target = target_dir / name
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(target, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)
    if prepared.custom_bundle_body is not None:
        bundle = prepared.document["custom_bundle"]
        target = target_dir / bundle["staged_name"]
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(target, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(prepared.custom_bundle_body)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)
    return prepared.document


def runtime_adapter_manifest(
    prepared: PreparedRuntimePortAdapters,
    *,
    custom_remote_allowed: bool = False,
) -> dict[str, Any]:
    """Build redacted manifest evidence for a local built-in adapter Run."""

    return manifest_port_adapter_section(
        prepared.document,
        adapter_policy=prepared.adapter_policy,
        custom_remote_allowed=custom_remote_allowed,
        custom_remote_used=False,
    )


__all__ = [
    "PreparedRuntimePortAdapters",
    "RUNTIME_INPUTS_DIRECTORY",
    "prepare_runtime_port_adapters",
    "materialize_claimed_runtime_port_adapters",
    "runtime_adapter_manifest",
    "stage_runtime_port_adapters",
]
