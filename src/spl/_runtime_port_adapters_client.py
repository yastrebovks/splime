"""Client-side preparation for Run-bound external port adapters.

Only JSON-compatible metadata and staged bytes cross the daemon boundary.
Caller paths and live adapter objects stay in this process.
"""

from __future__ import annotations

import base64
import hashlib
import re
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any

from spl.adapters import (
    BUILTIN_ADAPTER_IDS,
    JSON,
    OPAQUE_FILE,
    FileInput,
    builtin_id_for_adapter,
    builtin_required_packages,
    get_builtin_adapter,
    system_default_adapter_for_semantic_type,
    system_default_adapter_for_value,
)
from spl.core import json_contract as m_json_contract
from spl.core.entities.adapter import Adapter, BUILTIN_JSON_ADAPTER, RuntimeAdapter
from spl.core.library_adapters import (
    LibraryAdapterRef,
    library_adapter_semantic_advisory,
    normalize_library_adapter_ref,
    normalize_runtime_library_adapter_refs,
)
from spl.core.runtime_port_adapters import (
    MAX_RUNTIME_INPUT_BYTES,
    RuntimePortAdapterContractError,
    adapter_descriptor,
    build_custom_bundle,
    normalize_adapter_policy,
    normalize_public_adapter_mapping,
    normalize_runtime_adapter_semantic_advisories,
    normalize_wire_document,
    port_catalog_from_signature,
    resolve_port_reference,
    runtime_adapter_semantic_category,
    runtime_adapter_semantic_state,
    semantic_type_compatible,
)

_BUILTIN_RUNTIME_IDS = BUILTIN_ADAPTER_IDS
_TOKEN = re.compile(r"[^A-Za-z0-9_.-]+")


class _DiscardAdapterOutput:
    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        return None


_DISCARD_ADAPTER_OUTPUT = _DiscardAdapterOutput()


@dataclass(frozen=True)
class OutputDecoder:
    """Trusted same-process decoder for one selected output binding."""

    port: str
    adapter_id: str
    adapter: RuntimeAdapter


@dataclass(frozen=True)
class PreparedClientRuntimeAdapters:
    """Prepared request values plus client-local output decoder state."""

    document: dict[str, Any]
    adapter_policy: dict[str, str]
    args: list[Any] | None
    kwargs: dict[str, Any] | None
    output_decoders: dict[str, OutputDecoder]
    runtime_library_adapter_refs: dict[str, Any] | None = None
    runtime_adapter_semantic_advisories: dict[str, Any] | None = None


def spl_library_placeholder_save(path: str, value: object) -> None:
    del path, value
    raise RuntimeError("Library Adapter placeholder must be replaced during daemon admission")


def spl_library_placeholder_load(path: str) -> object:
    del path
    raise RuntimeError("Library Adapter placeholder must be replaced during daemon admission")


def server_admission_document(document: Mapping[str, Any]) -> dict[str, Any]:
    """Project the nested local wire into the central server's flat v1 DTO."""

    normalized = normalize_wire_document(document, allow_content=True)
    bindings = []
    for binding in normalized["bindings"]:
        adapter = binding["adapter"]
        bindings.append(
            {
                "direction": binding["direction"],
                "port": binding["port"],
                "external_name": binding["external_name"],
                "semantic_type": binding["semantic_type"],
                "adapter_kind": adapter["kind"],
                "adapter_id": adapter["id"],
                "key": adapter["key"],
                "format_tag": adapter["format_tag"],
                "accepted_tags": adapter["accepted_tags"],
                "distributions": adapter["distributions"],
                "save_symbol": adapter["save_symbol"],
                "load_symbol": adapter["load_symbol"],
                "bundle_sha256": adapter["bundle_sha256"],
                "resolution_source": binding["resolution_source"],
                "transport": binding["transport"],
                "input_name": binding["input_name"],
                "result_path": binding["result_path"],
                "presentation": adapter["presentation"],
            }
        )
    inputs = [
        {
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
        }
        for item in normalized["inputs"]
    ]
    bundle = normalized["custom_bundle"]
    custom_bundle = None if bundle is None else {key: bundle[key] for key in ("name", "size", "sha256", "functions")}
    return {
        "schema_version": normalized["schema_version"],
        "bindings": bindings,
        "inputs": inputs,
        "custom_bundle": custom_bundle,
    }


def requires_runtime_adapter_path(
    *,
    args: list[Any] | None,
    kwargs: dict[str, Any] | None,
    adapters: Any,
) -> bool:
    """Return whether a request needs the additive artifact transport path."""

    if adapters is not None:
        return True
    return any(
        isinstance(value, FileInput) or system_default_adapter_for_value(value) is not None
        for value in [*(args or []), *(kwargs or {}).values()]
    )


def prepare_client_runtime_adapters(
    *,
    signature: Mapping[str, Any],
    args: list[Any] | None,
    kwargs: dict[str, Any] | None,
    output: str | None,
    adapters: Any,
    adapter_policy: Any,
    library_adapter_versions: Mapping[str, Mapping[str, Any]] | None = None,
) -> PreparedClientRuntimeAdapters:
    """Resolve and stage a closed built-in runtime adapter request."""

    policy = normalize_adapter_policy(adapter_policy)
    mapping = normalize_public_adapter_mapping(adapters) or {"inputs": {}, "outputs": {}}
    catalog = port_catalog_from_signature(signature)
    mapping, library_specs = _replace_library_adapter_refs(
        mapping,
        versions=library_adapter_versions or {},
        targets={"inputs": catalog["input"], "outputs": catalog["output"]},
    )
    custom_values = _custom_adapter_values(mapping)
    custom_bundle: dict[str, Any] | None = None
    custom_symbols: dict[int, tuple[str, str]] = {}
    if custom_values:
        custom_bundle, custom_symbols = build_custom_bundle(custom_values)
    explicit_inputs = _resolve_explicit(mapping["inputs"], catalog["input"], direction="input")
    explicit_outputs = _resolve_explicit(mapping["outputs"], catalog["output"], direction="output")
    library_inputs = _resolve_explicit(library_specs["inputs"], catalog["input"], direction="input")
    library_outputs = _resolve_explicit(library_specs["outputs"], catalog["output"], direction="output")
    _validate_grouped_input_specs(explicit_inputs, catalog["input"])
    selected_outputs = _selected_output_targets(signature, catalog["output"], output)
    selected_names = {target.canonical for target in selected_outputs}
    unexpected = sorted(set(explicit_outputs) - selected_names)
    if unexpected:
        raise RuntimePortAdapterContractError(
            "unselected_output",
            "output adapter binding does not belong to the selected result: " + ", ".join(unexpected),
        )
    unexpected_library = sorted(set(library_outputs) - selected_names)
    if unexpected_library:
        raise RuntimePortAdapterContractError(
            "unselected_output",
            "Library Adapter binding does not belong to the selected result: " + ", ".join(unexpected_library),
        )

    prepared_args = None if args is None else list(args)
    prepared_kwargs = None if kwargs is None else dict(kwargs)
    bindings: list[dict[str, Any]] = []
    semantic_advisories: list[dict[str, Any]] = []
    inputs: list[dict[str, Any]] = []
    used_input_names: set[str] = set()
    staged_values: dict[str, tuple[bytes, str | None, str | None]] = {}

    input_order = signature.get("input_order")
    if not isinstance(input_order, list) or any(not isinstance(item, str) for item in input_order):
        input_order = [str(item.get("name")) for item in signature.get("inputs") or [] if isinstance(item, Mapping)]
    input_metadata = {
        str(item.get("name")): item for item in signature.get("inputs") or [] if isinstance(item, Mapping)
    }
    for target in catalog["input"]:
        location, value = _argument_value(
            target.external_name,
            input_order=input_order,
            metadata=input_metadata.get(target.external_name),
            args=args,
            kwargs=kwargs,
        )
        if target.canonical in library_inputs and not isinstance(value, FileInput):
            raise RuntimePortAdapterContractError(
                "library_adapter_input_file",
                "Library Adapter input bindings require spl.adapters.FileInput so code runs only in the worker",
            )
        explicit = explicit_inputs.get(target.canonical)
        adapter_id, adapter, source = _resolve_input_adapter(
            value,
            explicit,
            custom_bundle=custom_bundle,
        )
        descriptor = _descriptor(
            adapter,
            adapter_id=adapter_id,
            custom_bundle=custom_bundle,
            custom_symbols=custom_symbols,
            signature=signature,
            require_client_load=False,
            file_input=isinstance(value, FileInput),
        )
        if source == "system_default":
            _require_compatible(target.semantic_type, descriptor, adapter_id, target.canonical)
        transport = "inline_json" if adapter_id == JSON else "artifact"
        input_name = None
        if transport == "inline_json":
            if isinstance(value, FileInput):
                raise RuntimePortAdapterContractError(
                    "file_json_adapter", "FileInput cannot use the inline JSON adapter"
                )
            m_json_contract.validate_json_value(value, path=f"$.{target.external_name}")
        else:
            staged = staged_values.get(target.external_name)
            if staged is None:
                staged = _stage_input_bytes(value, adapter_id=adapter_id, adapter=adapter)
                staged_values[target.external_name] = staged
            content, media_type, logical_name = staged
            digest = hashlib.sha256(content).hexdigest()
            input_name = _unique_input_name(
                target.canonical,
                logical_name=logical_name,
                digest=digest,
                used=used_input_names,
            )
            inputs.append(
                {
                    "name": input_name,
                    "port": target.canonical,
                    "size": len(content),
                    "sha256": digest,
                    "format_tag": descriptor["format_tag"],
                    "semantic_type": target.semantic_type,
                    "adapter_id": adapter_id,
                    # The v1 placeholder is only a topology carrier for an
                    # exact Library Adapter ref.  Its immutable media type is
                    # re-read and overlaid by daemon admission; a caller's
                    # FileInput hint must not contradict the placeholder's
                    # deliberately null presentation identity.
                    "media_type": (
                        None
                        if target.canonical in library_inputs
                        else (media_type if media_type is not None else descriptor["presentation"]["media_type"])
                    ),
                    "content_base64": base64.b64encode(content).decode("ascii"),
                    "staged_name": None,
                }
            )
            prepared_args, prepared_kwargs = _replace_argument(
                location,
                args=prepared_args,
                kwargs=prepared_kwargs,
                value=None,
            )
        bindings.append(
            _binding(
                direction="input",
                target=target,
                descriptor=descriptor,
                source=source,
                transport=transport,
                argument=location,
                input_name=input_name,
                result_path=[],
            )
        )
        if source == "run_override":
            semantic_advisories.append(
                _client_semantic_advisory(
                    direction="input",
                    target=target,
                    descriptor=descriptor,
                    library_ref=library_inputs.get(target.canonical),
                    library_adapter_versions=library_adapter_versions or {},
                )
            )

    output_decoders: dict[str, OutputDecoder] = {}
    for target in selected_outputs:
        explicit = explicit_outputs.get(target.canonical)
        adapter_id, adapter, source = _resolve_output_adapter(
            target.semantic_type,
            explicit,
            custom_bundle=custom_bundle,
        )
        descriptor = _descriptor(
            adapter,
            adapter_id=adapter_id,
            custom_bundle=custom_bundle,
            custom_symbols=custom_symbols,
            signature=signature,
            require_client_load=True,
            file_input=False,
        )
        if source == "system_default":
            _require_compatible(target.semantic_type, descriptor, adapter_id, target.canonical)
        transport = "inline_json" if adapter_id == JSON else "artifact"
        bindings.append(
            _binding(
                direction="output",
                target=target,
                descriptor=descriptor,
                source=source,
                transport=transport,
                argument=None,
                input_name=None,
                result_path=_output_result_path(signature, target, selected_outputs, output),
            )
        )
        if source == "run_override":
            semantic_advisories.append(
                _client_semantic_advisory(
                    direction="output",
                    target=target,
                    descriptor=descriptor,
                    library_ref=library_outputs.get(target.canonical),
                    library_adapter_versions=library_adapter_versions or {},
                )
            )
        if transport == "artifact":
            if target.canonical not in library_outputs:
                output_decoders[target.canonical] = OutputDecoder(target.canonical, adapter_id, adapter)

    document = normalize_wire_document(
        {
            "schema_version": 1,
            "bindings": bindings,
            "inputs": inputs,
            "custom_bundle": _wire_custom_bundle(custom_bundle),
        },
        allow_content=True,
    )
    library_bindings = [
        {
            "direction": direction,
            "port": port,
            **normalize_library_adapter_ref(ref),
        }
        for direction, refs in (("input", library_inputs), ("output", library_outputs))
        for port, ref in refs.items()
    ]
    runtime_library_adapter_refs = (
        None
        if not library_bindings
        else normalize_runtime_library_adapter_refs({"schema_version": 1, "bindings": library_bindings})
    )
    runtime_adapter_semantic_advisories = (
        None
        if not semantic_advisories
        else normalize_runtime_adapter_semantic_advisories({"schema_version": 1, "bindings": semantic_advisories})
    )
    return PreparedClientRuntimeAdapters(
        document=document,
        adapter_policy=policy,
        args=prepared_args,
        kwargs=prepared_kwargs,
        output_decoders=output_decoders,
        runtime_library_adapter_refs=runtime_library_adapter_refs,
        runtime_adapter_semantic_advisories=runtime_adapter_semantic_advisories,
    )


def _replace_library_adapter_refs(
    mapping: Mapping[str, Mapping[str, Any]],
    *,
    versions: Mapping[str, Mapping[str, Any]],
    targets: Mapping[str, Sequence[Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, LibraryAdapterRef]]]:
    """Replace exact refs with never-executed valid-v1 placeholder adapters."""

    prepared: dict[str, dict[str, Any]] = {"inputs": {}, "outputs": {}}
    refs: dict[str, dict[str, LibraryAdapterRef]] = {"inputs": {}, "outputs": {}}
    placeholders: dict[tuple[str, str], Adapter] = {}
    for section, direction in (("inputs", "input"), ("outputs", "output")):
        for port, value in mapping[section].items():
            if not isinstance(value, LibraryAdapterRef):
                prepared[section][port] = value
                continue
            ref = normalize_library_adapter_ref(value)
            version = versions.get(ref["adapter_version_id"])
            if not isinstance(version, Mapping) or any(version.get(key) != ref[key] for key in ref):
                raise RuntimePortAdapterContractError(
                    "library_adapter_ref_unverified",
                    "Library Adapter reference was not re-read as the exact immutable version",
                    stage="admission",
                )
            directions = version.get("directions")
            if not isinstance(directions, list) or direction not in directions:
                raise RuntimePortAdapterContractError(
                    "library_adapter_direction_unsupported",
                    f"Library Adapter does not support {direction} port {port!r}",
                    stage="admission",
                )
            semantic_type = version.get("semantic_type")
            if not isinstance(semantic_type, str) or not semantic_type:
                raise RuntimePortAdapterContractError(
                    "library_adapter_semantic_type",
                    "Library Adapter immutable semantic type is unavailable",
                    stage="admission",
                )
            target = resolve_port_reference(port, targets[section])
            placeholder_type = target.semantic_type or semantic_type
            declared_tag = version.get("format_tag")
            tag = (
                declared_tag
                if isinstance(declared_tag, str) and declared_tag
                else f"spl.library.{ref['content_hash']}.v1"
            )
            placeholder_key = (ref["adapter_version_id"], placeholder_type)
            placeholder = placeholders.get(placeholder_key)
            if placeholder is None:
                placeholder = Adapter(
                    key=f"{placeholder_type}@{tag}",
                    save=spl_library_placeholder_save,
                    load=spl_library_placeholder_load,
                    py_type=None,
                    format=tag,
                    distributions=(),
                )
                placeholders[placeholder_key] = placeholder
            prepared[section][port] = placeholder
            refs[section][port] = value
    return prepared, refs


def _resolve_explicit(raw: Mapping[str, Any], targets: Sequence[Any], *, direction: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for reference, value in raw.items():
        target = resolve_port_reference(reference, targets)
        if target.canonical in result:
            raise RuntimePortAdapterContractError(
                "duplicate_port", f"adapters.{direction}s resolves {target.canonical!r} more than once"
            )
        result[target.canonical] = value
    return result


def _validate_grouped_input_specs(explicit: Mapping[str, Any], targets: Sequence[Any]) -> None:
    """Prevent fan-out ports from racing to reinterpret one outer argument."""

    missing = object()
    grouped: dict[str, list[Any]] = {}
    for target in targets:
        grouped.setdefault(target.external_name, []).append(explicit.get(target.canonical, missing))
    for external_name, specs in grouped.items():
        if len(specs) < 2:
            continue
        identities = {_public_adapter_spec_identity(spec, missing=missing) for spec in specs}
        if len(identities) != 1:
            raise RuntimePortAdapterContractError(
                "fanout_adapter_conflict",
                f"fan-out input {external_name!r} must use one consistent adapter across all canonical ports",
            )


def _public_adapter_spec_identity(value: Any, *, missing: object) -> tuple[str, Any]:
    if value is missing:
        return ("system_default", None)
    if isinstance(value, str):
        return ("builtin", value)
    if isinstance(value, Adapter) or value is BUILTIN_JSON_ADAPTER:
        builtin_id = builtin_id_for_adapter(value)
        return ("builtin", builtin_id) if builtin_id is not None else ("custom", id(value))
    if isinstance(value, LibraryAdapterRef):
        return ("library", tuple(normalize_library_adapter_ref(value).values()))
    return ("invalid", id(value))


def _custom_adapter_values(mapping: Mapping[str, Mapping[str, Any]]) -> list[RuntimeAdapter]:
    result: list[RuntimeAdapter] = []
    seen: set[int] = set()
    for section in ("inputs", "outputs"):
        for value in mapping[section].values():
            if not isinstance(value, Adapter):
                continue
            if builtin_id_for_adapter(value) is not None or id(value) in seen:
                continue
            seen.add(id(value))
            result.append(value)
    return result


def _resolve_spec(
    value: Any,
    *,
    custom_bundle: Mapping[str, Any] | None,
) -> tuple[str, RuntimeAdapter]:
    if isinstance(value, str):
        if value not in _BUILTIN_RUNTIME_IDS:
            raise RuntimePortAdapterContractError(
                "adapter_id",
                "this runtime supports built-in adapter IDs: " + ", ".join(sorted(_BUILTIN_RUNTIME_IDS)),
            )
        return value, get_builtin_adapter(value)
    if isinstance(value, Adapter) or value is BUILTIN_JSON_ADAPTER:
        adapter_id = builtin_id_for_adapter(value)
        if adapter_id is not None:
            return adapter_id, value
        if custom_bundle is None:
            raise RuntimePortAdapterContractError("custom_bundle", "custom adapter bundle preparation is missing")
        return f"custom:{custom_bundle['sha256']}", value
    raise RuntimePortAdapterContractError(
        "adapter_value", "adapter bindings must be stable built-in IDs or spl Adapter instances"
    )


def _resolve_input_adapter(
    value: Any,
    explicit: Any,
    *,
    custom_bundle: Mapping[str, Any] | None,
) -> tuple[str, RuntimeAdapter, str]:
    if explicit is not None:
        adapter_id, adapter = _resolve_spec(explicit, custom_bundle=custom_bundle)
        return adapter_id, adapter, "run_override"
    if isinstance(value, FileInput):
        return OPAQUE_FILE, get_builtin_adapter(OPAQUE_FILE), "system_default"
    if _is_strict_json(value):
        return JSON, get_builtin_adapter(JSON), "system_default"
    default_id = system_default_adapter_for_value(value)
    if default_id is not None:
        return default_id, get_builtin_adapter(default_id), "system_default"
    raise RuntimePortAdapterContractError(
        "missing_input_adapter",
        f"value of type {type(value).__module__}.{type(value).__qualname__} is not strict JSON; choose an adapter",
    )


def _resolve_output_adapter(
    port_type: str | None,
    explicit: Any,
    *,
    custom_bundle: Mapping[str, Any] | None,
) -> tuple[str, RuntimeAdapter, str]:
    if explicit is not None:
        adapter_id, adapter = _resolve_spec(explicit, custom_bundle=custom_bundle)
        return adapter_id, adapter, "run_override"
    if semantic_type_compatible(port_type, "spl.core.json", adapter_id=JSON):
        return JSON, get_builtin_adapter(JSON), "system_default"
    default_id = system_default_adapter_for_semantic_type(port_type)
    if default_id is not None:
        return default_id, get_builtin_adapter(default_id), "system_default"
    raise RuntimePortAdapterContractError(
        "missing_output_adapter", f"output semantic type {port_type!r} has no safe system default; choose an adapter"
    )


def _descriptor(
    adapter: RuntimeAdapter,
    *,
    adapter_id: str,
    custom_bundle: Mapping[str, Any] | None,
    custom_symbols: Mapping[int, tuple[str, str]],
    signature: Mapping[str, Any],
    require_client_load: bool,
    file_input: bool,
) -> dict[str, Any]:
    if require_client_load or not file_input:
        _verify_caller_distributions(
            adapter,
            stage="client_load" if require_client_load else "client_save",
        )
    if adapter_id.startswith("custom:"):
        if custom_bundle is None or id(adapter) not in custom_symbols:
            raise RuntimePortAdapterContractError("custom_bundle", "custom adapter source metadata is missing")
        save_symbol, load_symbol = custom_symbols[id(adapter)]
        return adapter_descriptor(
            adapter,
            adapter_id=None,
            custom_bundle_sha256=str(custom_bundle["sha256"]),
            save_symbol=save_symbol,
            load_symbol=load_symbol,
        )
    descriptor = adapter_descriptor(adapter, adapter_id=adapter_id)
    required = builtin_required_packages(adapter_id)
    if not required:
        return descriptor
    available = {_canonical_package(item["package"]): item for item in descriptor["distributions"]}
    signature_distributions = {
        _canonical_package(str(item.get("package"))): {
            "package": str(item.get("package")),
            "version": str(item.get("version")),
        }
        for item in signature.get("distributions") or []
        if isinstance(item, Mapping) and item.get("package") and item.get("version")
    }
    for package in required:
        key = _canonical_package(package)
        if key in available:
            continue
        if file_input and key in signature_distributions:
            available[key] = signature_distributions[key]
            continue
        stage = "client_load" if require_client_load else "client_save"
        raise RuntimePortAdapterContractError(
            "missing_dependency",
            f"adapter {adapter_id!r} requires exact distribution {package!r} in the caller or Object environment",
            stage=stage,
        )
    return {
        **descriptor,
        "distributions": sorted(available.values(), key=lambda item: _canonical_package(item["package"])),
    }


def _verify_caller_distributions(adapter: RuntimeAdapter, *, stage: str) -> None:
    for distribution in adapter.distributions:
        try:
            actual = importlib_metadata.version(distribution.package)
        except importlib_metadata.PackageNotFoundError:
            raise RuntimePortAdapterContractError(
                "missing_dependency",
                f"adapter requires {distribution.package}=={distribution.version} in the caller",
                stage=stage,
            ) from None
        if actual != distribution.version:
            raise RuntimePortAdapterContractError(
                "dependency_conflict",
                f"adapter requires {distribution.package}=={distribution.version}; caller has {actual}",
                stage=stage,
            )


def _wire_custom_bundle(bundle: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if bundle is None:
        return None
    return {
        "name": bundle["name"],
        "size": bundle["size"],
        "sha256": bundle["sha256"],
        "functions": bundle["functions"],
        "content_base64": base64.b64encode(bundle["source_bytes"]).decode("ascii"),
        "staged_name": None,
    }


def _canonical_package(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def _argument_value(
    name: str,
    *,
    input_order: list[str],
    metadata: Mapping[str, Any] | None,
    args: list[Any] | None,
    kwargs: dict[str, Any] | None,
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
    raise RuntimePortAdapterContractError("missing_input", f"required input {name!r} was not supplied")


def _replace_argument(
    location: Mapping[str, Any],
    *,
    args: list[Any] | None,
    kwargs: dict[str, Any] | None,
    value: Any,
) -> tuple[list[Any] | None, dict[str, Any] | None]:
    if location["kind"] == "positional":
        assert args is not None
        args[int(location["index"])] = value
        return args, kwargs
    output_kwargs = {} if kwargs is None else kwargs
    output_kwargs[str(location["name"])] = value
    return args, output_kwargs


def _stage_input_bytes(value: Any, *, adapter_id: str, adapter: RuntimeAdapter) -> tuple[bytes, str | None, str | None]:
    if isinstance(value, FileInput):
        return value.staged_bytes(max_bytes=MAX_RUNTIME_INPUT_BYTES), value.media_type, value.name
    if adapter_id == OPAQUE_FILE:
        raise RuntimePortAdapterContractError(
            "opaque_requires_file_input", "opaque-file inputs must be wrapped with spl.adapters.FileInput"
        )
    try:
        with tempfile.TemporaryDirectory(prefix="spl-runtime-input-") as raw_dir:
            directory = Path(raw_dir)
            target = directory / "value"
            if adapter_id.startswith("custom:"):
                with redirect_stdout(_DISCARD_ADAPTER_OUTPUT), redirect_stderr(_DISCARD_ADAPTER_OUTPUT):
                    adapter.save(str(target), value)
            else:
                adapter.save(str(target), value)
            entries = list(directory.iterdir())
            if entries != [target] or target.is_symlink() or not target.is_file():
                raise RuntimePortAdapterContractError(
                    "client_save_shape", "adapter save must create exactly one regular file", stage="client_save"
                )
            staged = FileInput(target, name="value")
            return staged.staged_bytes(max_bytes=MAX_RUNTIME_INPUT_BYTES), None, None
    except RuntimePortAdapterContractError:
        raise
    except BaseException:
        raise RuntimePortAdapterContractError(
            "client_save_failed", f"adapter {adapter_id!r} failed to save its input", stage="client_save"
        ) from None


def _selected_output_targets(signature: Mapping[str, Any], targets: Sequence[Any], output: str | None) -> list[Any]:
    if signature.get("kind") != "pipeline" or output is None:
        return list(targets)
    selected = [
        target for target in targets if target.external_name == output or target.canonical.startswith(output + ".")
    ]
    if not selected:
        choices = sorted({target.external_name for target in targets})
        raise RuntimePortAdapterContractError(
            "unknown_output_selector",
            f"unknown output selector {output!r}; available: {', '.join(choices) or '<none>'}",
        )
    return selected


def _output_result_path(
    signature: Mapping[str, Any], target: Any, targets: Sequence[Any], output: str | None
) -> list[str]:
    if signature.get("kind") == "function":
        return [] if len(targets) == 1 else [target.short]
    if output is not None:
        return [target.short]
    if target.external_name == target.short:
        return [target.short]
    return [target.external_name, target.short]


def _client_semantic_advisory(
    *,
    direction: str,
    target: Any,
    descriptor: Mapping[str, Any],
    library_ref: LibraryAdapterRef | None,
    library_adapter_versions: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if library_ref is not None:
        ref = normalize_library_adapter_ref(library_ref)
        version = library_adapter_versions.get(ref["adapter_version_id"])
        if not isinstance(version, Mapping):
            raise RuntimePortAdapterContractError(
                "library_adapter_ref_unverified",
                "Library Adapter reference was not re-read as the exact immutable version",
                stage="admission",
            )
        adapter_type = str(version["semantic_type"])
        category_value = version.get("semantic_category")
        category = category_value if isinstance(category_value, str) else None
        adapter_id = ref["adapter_id"]
        state = library_adapter_semantic_advisory(target.semantic_type, adapter_type, category)
    else:
        adapter_type = str(descriptor["key"]).rpartition("@")[0]
        adapter_id = str(descriptor["id"])
        category = runtime_adapter_semantic_category(adapter_id)
        state = runtime_adapter_semantic_state(
            target.semantic_type,
            adapter_type,
            adapter_id=adapter_id,
        )
    return {
        "direction": direction,
        "port": target.canonical,
        "adapter_id": adapter_id,
        "port_semantic_type": target.semantic_type,
        "adapter_semantic_type": adapter_type,
        "adapter_semantic_category": category,
        "state": state,
        "acknowledged": state != "recommended",
        "acknowledgement_source": None if state == "recommended" else "sdk_explicit_selection",
    }


def _binding(
    *,
    direction: str,
    target: Any,
    descriptor: Mapping[str, Any],
    source: str,
    transport: str,
    argument: Mapping[str, Any] | None,
    input_name: str | None,
    result_path: list[str],
) -> dict[str, Any]:
    return {
        "direction": direction,
        "port": target.canonical,
        "external_name": target.external_name,
        "semantic_type": target.semantic_type,
        "adapter": dict(descriptor),
        "resolution_source": source,
        "transport": transport,
        "argument": None if argument is None else dict(argument),
        "input_name": input_name,
        "result_path": result_path,
    }


def _require_compatible(port_type: str | None, descriptor: Mapping[str, Any], adapter_id: str, port: str) -> None:
    adapter_type = str(descriptor["key"]).rpartition("@")[0]
    if not semantic_type_compatible(port_type, adapter_type, adapter_id=adapter_id):
        raise RuntimePortAdapterContractError(
            "semantic_type_mismatch",
            f"adapter {adapter_id!r} is incompatible with port {port!r} semantic type {port_type!r}",
        )


def _unique_input_name(port: str, *, logical_name: str | None, digest: str, used: set[str]) -> str:
    suffix = Path(logical_name).suffix if logical_name else ""
    token = _TOKEN.sub("-", port).strip(".-") or "input"
    base = f"input-{token}-{digest[:12]}{suffix}"[:255]
    candidate = base
    index = 2
    while candidate in used:
        candidate = f"{base[:245]}-{index}"
        index += 1
    used.add(candidate)
    return candidate


def _is_strict_json(value: Any) -> bool:
    try:
        m_json_contract.validate_json_value(value)
    except (TypeError, ValueError):
        return False
    return True


__all__ = [
    "OutputDecoder",
    "PreparedClientRuntimeAdapters",
    "prepare_client_runtime_adapters",
    "requires_runtime_adapter_path",
    "server_admission_document",
]
