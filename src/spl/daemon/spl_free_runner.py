"""Stdlib-only runner for SPL-free functional node execution."""

from __future__ import annotations

import argparse
import ast
import builtins
import hashlib
import importlib.abc
import importlib.machinery
import importlib.metadata
import importlib.util
import json
import math
import os
import re
import shutil
import stat
import sys
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from types import ModuleType
from typing import Any

ARTIFACTS_KEY = "__spl_artifacts__"
RESULT_KEY = "__spl_result__"
NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
JSON_SCALARS = frozenset({type(None), bool, int, float, str})
ISOLATED_NODE_INPUTS_SCHEMA_VERSION = 2
ISOLATED_NODE_TRANSPORT_SCHEMA_VERSION = 3
ISOLATED_NODE_MULTI_OUTPUT_TRANSPORT_SCHEMA_VERSION = 4
ISOLATED_NODE_OUTPUT_RESULT_SCHEMA_VERSION = 1
ISOLATED_NODE_MULTI_OUTPUT_RESULT_SCHEMA_VERSION = 2
MAX_RUNTIME_INPUT_BYTES = 256 * 1024 * 1024
MAX_RUNTIME_INPUT_TOTAL_BYTES = 512 * 1024 * 1024
MAX_RUNTIME_OUTPUT_BYTES = MAX_RUNTIME_INPUT_BYTES
MAX_RUNTIME_OUTPUT_TOTAL_BYTES = MAX_RUNTIME_INPUT_TOTAL_BYTES
MAX_CUSTOM_BUNDLE_BYTES = 512 * 1024
MAX_RUNTIME_ADAPTER_BINDINGS = 1_024
SAFE_INPUT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
SAFE_PORT = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}(?:\.[A-Za-z_][A-Za-z0-9_-]{0,127})?$")
SAFE_TAG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,159}$")
SAFE_TYPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\[\], |]*$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
PACKAGE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,159}$")
IMPORT_HEADER = re.compile(
    r"^# spl-runtime-import-v1 symbol=([A-Za-z][A-Za-z0-9_]*) module=([A-Za-z][A-Za-z0-9_]*) "
    r"package=([A-Za-z0-9][A-Za-z0-9._-]{0,127}) version=([A-Za-z0-9][A-Za-z0-9.!+_-]{0,159})$"
)
DYNAMIC_CALLS = frozenset(
    {"__import__", "compile", "eval", "exec", "getattr", "globals", "import_module", "locals", "setattr", "vars"}
)
BUILTIN_LOADERS = {
    "json": ("spl.core.json@json", "json", frozenset({"json"})),
    "opaque-file": ("pathlib.Path@spl.file.opaque.v1", "spl.file.opaque.v1", frozenset({"spl.file.opaque.v1"})),
    "text-file-utf8": (
        "builtins.str@spl.text.utf8.v1",
        "spl.text.utf8.v1",
        frozenset({"spl.text.utf8.v1"}),
    ),
    "binary-file": (
        "builtins.bytes@spl.binary.raw.v1",
        "spl.binary.raw.v1",
        frozenset({"spl.binary.raw.v1"}),
    ),
    "dataframe-json-split": (
        "pandas.core.frame.DataFrame@spl.table.dataframe-json-split.v1",
        "spl.table.dataframe-json-split.v1",
        frozenset({"spl.table.dataframe-json-split.v1"}),
    ),
    "dataframe-csv-semicolon": (
        "pandas.core.frame.DataFrame@spl.table.csv-semicolon-utf8-no-index.v1",
        "spl.table.csv-semicolon-utf8-no-index.v1",
        frozenset({"spl.table.csv-semicolon-utf8-no-index.v1"}),
    ),
    "dataframe-xlsx": (
        "pandas.core.frame.DataFrame@spl.table.xlsx-openpyxl-sheet-data-no-index.v1",
        "spl.table.xlsx-openpyxl-sheet-data-no-index.v1",
        frozenset({"spl.table.xlsx-openpyxl-sheet-data-no-index.v1"}),
    ),
    "png-pillow": (
        "PIL.Image.Image@spl.image.png-pillow.v1",
        "spl.image.png-pillow.v1",
        frozenset({"spl.image.png-pillow.v1"}),
    ),
}
BUILTIN_SAVERS = BUILTIN_LOADERS
BUILTIN_PRESENTATION = {
    "json": {"media_type": "application/json", "preferred_extension": None},
    "opaque-file": {"media_type": None, "preferred_extension": None},
    "text-file-utf8": {"media_type": "text/plain", "preferred_extension": ".txt"},
    "binary-file": {"media_type": "application/octet-stream", "preferred_extension": ".bin"},
    "dataframe-json-split": {"media_type": "application/json", "preferred_extension": ".json"},
    "dataframe-csv-semicolon": {"media_type": "text/csv", "preferred_extension": ".csv"},
    "dataframe-xlsx": {
        "media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "preferred_extension": ".xlsx",
    },
    "png-pillow": {"media_type": "image/png", "preferred_extension": ".png"},
}
BUILTIN_REQUIRED_PACKAGES = {
    "dataframe-json-split": {"pandas"},
    "dataframe-csv-semicolon": {"pandas"},
    "dataframe-xlsx": {"pandas", "openpyxl"},
    "png-pillow": {"pillow"},
}
LEGACY_INVOCATION_REQUIRED_KEYS = frozenset({"args", "kwargs"})
LEGACY_INVOCATION_KEYS = LEGACY_INVOCATION_REQUIRED_KEYS | {
    "function",
    "keep",
    "node_runtime_environments",
    "output",
    "report_local_run",
    "resume",
    "runtime_config",
    "runtimes",
    "timeout_seconds",
}


class NodeStageError(RuntimeError):
    """Closed, safe failure emitted by the SPL-free node boundary."""

    def __init__(self, stage: str, code: str, message: str):
        self.stage = stage
        self.code = code
        self.safe_message = message
        super().__init__(f"{stage}: {code}: {message}")


class _DiscardAdapterOutput:
    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        return None


_DISCARD_ADAPTER_OUTPUT = _DiscardAdapterOutput()


class _SplImportBlocker(importlib.abc.MetaPathFinder):
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
    ) -> None:
        del path, target
        if fullname == "spl" or fullname.startswith("spl."):
            raise ModuleNotFoundError("spl is unavailable inside the SPL-free node runtime")
        return None


@contextmanager
def _spl_free_import_boundary() -> Iterator[None]:
    """Make the runner independent of an accidentally installed conductor package."""

    saved = {name: module for name, module in sys.modules.items() if name == "spl" or name.startswith("spl.")}
    for name in saved:
        sys.modules.pop(name, None)
    original_find_spec = importlib.util.find_spec

    def closed_find_spec(name: str, package: str | None = None) -> importlib.machinery.ModuleSpec | None:
        if name == "spl" or name.startswith("spl."):
            return None
        return original_find_spec(name, package)

    blocker = _SplImportBlocker()
    setattr(importlib.util, "find_spec", closed_find_spec)
    sys.meta_path.insert(0, blocker)
    try:
        yield
    finally:
        setattr(importlib.util, "find_spec", original_find_spec)
        if blocker in sys.meta_path:
            sys.meta_path.remove(blocker)
        for name in list(sys.modules):
            if name == "spl" or name.startswith("spl."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)


def validate_name(name: str) -> str:
    """Validate a registry-safe name and return it unchanged."""

    # Keep this rule in sync with spl.daemon.storage_base.validate_name;
    # the runner duplicates it intentionally to stay stdlib-only.
    if not NAME_PATTERN.fullmatch(name) or set(name) == {"."}:
        raise ValueError("name must contain only letters, digits, underscore, dash, and dot, and not only dots")
    return name


def read_json(path: Path) -> Any:
    """Read a UTF-8 JSON file."""

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_closed_json_object)


def _closed_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("JSON document contains a duplicate field")
        value[key] = item
    return value


def write_json(path: Path, value: Any) -> None:
    """Write a UTF-8 JSON file with stable formatting."""

    payload = _json_dumps(value, ensure_ascii=False, indent=2, sort_keys=True, separators=None)
    _ensure_private_dir(path.parent)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(payload)
    _chmod_owner_file(path)


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except OSError:
        pass


def _chmod_owner_file(path: Path) -> None:
    try:
        path.chmod(0o600)
    except OSError:
        pass


def validate_environment(distributions: list[dict[str, str]]) -> None:
    """Fail when the runner interpreter does not match SPL metadata."""

    mismatches = []
    for distribution in distributions:
        package = distribution["package"]
        expected = distribution["version"]
        try:
            actual = importlib.metadata.version(package)
        except PackageNotFoundError:
            mismatches.append(f"{package}=={expected} is not installed")
            continue
        if actual != expected:
            mismatches.append(f"{package}=={expected} is required, actual version is {actual}")

    if mismatches:
        raise RuntimeError("worker environment does not match SPL metadata: " + "; ".join(mismatches))


def _prepare_invocation(
    payload: Any,
    *,
    input_path: Path,
) -> tuple[list[Any], dict[str, Any], dict[str, Any] | None, ModuleType | None]:
    """Validate v1/v2/v3 input JSON and load artifact values before user code."""

    if not isinstance(payload, dict):
        raise NodeStageError("node_adapter_input_validation", "invocation_shape", "input document must be an object")
    payload_keys = set(payload)
    if (
        "schema_version" not in payload
        and LEGACY_INVOCATION_REQUIRED_KEYS <= payload_keys
        and payload_keys <= LEGACY_INVOCATION_KEYS
    ):
        args, kwargs = _invocation_arguments(payload)
        return args, kwargs, None, None
    if payload_keys != {"schema_version", "args", "kwargs", "runtime_port_adapters"}:
        raise NodeStageError(
            "node_adapter_input_validation",
            "invocation_keys",
            "mixed input document does not match the closed schema",
        )
    if payload["schema_version"] not in {
        ISOLATED_NODE_INPUTS_SCHEMA_VERSION,
        ISOLATED_NODE_TRANSPORT_SCHEMA_VERSION,
        ISOLATED_NODE_MULTI_OUTPUT_TRANSPORT_SCHEMA_VERSION,
    }:
        raise NodeStageError(
            "node_adapter_input_validation",
            "invocation_version",
            "mixed input schema_version is not supported",
        )
    args, kwargs = _invocation_arguments(payload)
    if payload["schema_version"] == ISOLATED_NODE_INPUTS_SCHEMA_VERSION:
        document = _normalize_isolated_input_document(payload["runtime_port_adapters"])
    elif payload["schema_version"] == ISOLATED_NODE_TRANSPORT_SCHEMA_VERSION:
        document = _normalize_isolated_transport_document(payload["runtime_port_adapters"])
    else:
        document = _normalize_isolated_transport_document(
            payload["runtime_port_adapters"],
            multi_output=True,
        )
    input_bindings = [binding for binding in document["bindings"] if binding["direction"] == "input"]
    artifact_argument_names = {binding["argument"]["name"] for binding in input_bindings}
    if artifact_argument_names & set(kwargs):
        raise NodeStageError(
            "node_adapter_input_validation",
            "argument_collision",
            "isolated artifact inputs must not overwrite inline keyword arguments",
        )
    custom_module = _load_verified_custom_module(document, input_path=input_path)
    input_records = {item["name"]: item for item in document["inputs"]}
    snapshot_dir = input_path.parent / "runtime-loaded-inputs"
    snapshot_dir.mkdir(mode=0o700, exist_ok=False)
    loaded_locations: set[tuple[str, str | None, int | None]] = set()
    for binding in input_bindings:
        item = input_records[binding["input_name"]]
        staged_path = input_path.parent / "runtime-inputs" / item["staged_name"]
        try:
            body = _read_verified_runtime_file(
                staged_path,
                expected_size=item["size"],
                expected_sha256=item["sha256"],
            )
            snapshot_path = snapshot_dir / item["name"]
            _write_private_snapshot(snapshot_path, body)
            location = binding["argument"]
            location_key = (location["kind"], location["name"], location["index"])
            if location_key in loaded_locations:
                raise NodeStageError(
                    "node_adapter_input_validation",
                    "argument_duplicate",
                    "isolated artifact inputs target the same argument more than once",
                )
            loaded_locations.add(location_key)
        except NodeStageError:
            raise
        except (OSError, RuntimeError):
            raise NodeStageError(
                "node_adapter_input_validation",
                "artifact_integrity",
                f"artifact input for port {binding['port']!r} failed staging or integrity validation",
            ) from None
        try:
            loader = _adapter_loader(binding, custom_module)
            if binding["adapter"]["kind"] == "custom":
                with redirect_stdout(_DISCARD_ADAPTER_OUTPUT), redirect_stderr(_DISCARD_ADAPTER_OUTPUT):
                    value = loader(str(snapshot_path))
            else:
                value = loader(str(snapshot_path))
        except BaseException:
            raise NodeStageError(
                "node_adapter_load",
                "adapter_load_failed",
                f"load adapter failed for port {binding['port']!r}",
            ) from None
        try:
            _read_verified_runtime_file(
                snapshot_path,
                expected_size=item["size"],
                expected_sha256=item["sha256"],
            )
        except (OSError, RuntimeError):
            raise NodeStageError(
                "node_adapter_input_validation",
                "artifact_mutated",
                f"load adapter changed the verified artifact for port {binding['port']!r}",
            ) from None
        location = binding["argument"]
        if location["kind"] == "positional":
            index = int(location["index"])
            if index >= len(args):
                raise NodeStageError(
                    "node_adapter_input_validation",
                    "argument_missing",
                    "artifact input positional target is unavailable",
                )
            args[index] = value
        else:
            kwargs[str(location["name"])] = value
    return args, kwargs, document, custom_module


def prepare_invocation_inputs(payload: Any, *, input_path: Path) -> tuple[list[Any], dict[str, Any]]:
    """Validate invocation input and return only the historical public pair."""

    args, kwargs, _, _ = _prepare_invocation(payload, input_path=input_path)
    return args, kwargs


def _invocation_arguments(payload: Mapping[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    args = payload["args"]
    kwargs = payload["kwargs"]
    if not isinstance(args, list) or args:
        raise NodeStageError(
            "node_adapter_input_validation",
            "invocation_args",
            "function-node invocation args must be an empty list",
        )
    if not isinstance(kwargs, dict) or any(type(key) is not str for key in kwargs):
        raise NodeStageError(
            "node_adapter_input_validation",
            "invocation_kwargs",
            "function-node invocation kwargs must be a string-keyed object",
        )
    try:
        _validate_json_value(kwargs)
    except ValueError:
        raise NodeStageError(
            "node_adapter_input_validation",
            "invocation_json",
            "inline invocation values are not strict JSON",
        ) from None
    return list(args), dict(kwargs)


def _normalize_isolated_input_document(value: Any) -> dict[str, Any]:
    _exact_mapping(value, {"schema_version", "bindings", "inputs", "custom_bundle"}, "adapter input plan")
    if value["schema_version"] != ISOLATED_NODE_INPUTS_SCHEMA_VERSION:
        raise NodeStageError(
            "node_adapter_input_validation",
            "adapter_plan_version",
            "adapter input plan schema_version is not supported",
        )
    raw_bindings = value["bindings"]
    raw_inputs = value["inputs"]
    if (
        not isinstance(raw_bindings, list)
        or not raw_bindings
        or len(raw_bindings) > MAX_RUNTIME_ADAPTER_BINDINGS
        or not isinstance(raw_inputs, list)
        or not raw_inputs
        or len(raw_inputs) > MAX_RUNTIME_ADAPTER_BINDINGS
    ):
        raise NodeStageError(
            "node_adapter_input_validation",
            "adapter_plan_size",
            "adapter input plan lists are missing or excessive",
        )
    inputs = [_normalize_input_record(item) for item in raw_inputs]
    names = [item["name"] for item in inputs]
    if len(set(names)) != len(names) or sum(item["size"] for item in inputs) > MAX_RUNTIME_INPUT_TOTAL_BYTES:
        raise NodeStageError(
            "node_adapter_input_validation",
            "input_identity",
            "artifact input names are duplicated or exceed the aggregate size limit",
        )
    input_by_name = {item["name"]: item for item in inputs}
    bindings = [_normalize_input_binding(item) for item in raw_bindings]
    ports = [item["port"] for item in bindings]
    if len(set(ports)) != len(ports):
        raise NodeStageError(
            "node_adapter_input_validation",
            "binding_duplicate",
            "artifact input ports are duplicated",
        )
    referenced: set[str] = set()
    for binding in bindings:
        item = input_by_name.get(binding["input_name"])
        if item is None:
            raise NodeStageError(
                "node_adapter_input_validation",
                "binding_input_missing",
                "artifact input binding references an undeclared file",
            )
        referenced.add(item["name"])
        adapter = binding["adapter"]
        if (
            item["port"],
            item["format_tag"],
            item["semantic_type"],
            item["adapter_id"],
            item["media_type"],
        ) != (
            binding["port"],
            adapter["format_tag"],
            binding["semantic_type"],
            adapter["id"],
            adapter["presentation"]["media_type"],
        ):
            raise NodeStageError(
                "node_adapter_input_validation",
                "binding_identity",
                "artifact input does not match its exact binding identity",
            )
    if referenced != set(names):
        raise NodeStageError(
            "node_adapter_input_validation",
            "input_unbound",
            "every artifact input file must have one binding",
        )
    bundle = None if value["custom_bundle"] is None else _normalize_bundle(value["custom_bundle"])
    if bundle is not None and bundle["name"] in set(names):
        raise NodeStageError(
            "node_adapter_input_validation",
            "bundle_name_collision",
            "artifact input name collides with the custom adapter bundle",
        )
    custom = [binding for binding in bindings if binding["adapter"]["kind"] == "custom"]
    if custom:
        digests = {binding["adapter"]["bundle_sha256"] for binding in custom}
        symbols = {binding["adapter"]["load_symbol"] for binding in custom}
        if bundle is None or digests != {bundle["sha256"]} or symbols != set(bundle["functions"]):
            raise NodeStageError(
                "node_adapter_input_validation",
                "bundle_identity",
                "custom adapter bundle does not match its exact bindings",
            )
    elif bundle is not None:
        raise NodeStageError(
            "node_adapter_input_validation",
            "bundle_unused",
            "custom adapter bundle has no custom input binding",
        )
    return {
        "schema_version": ISOLATED_NODE_INPUTS_SCHEMA_VERSION,
        "bindings": sorted(bindings, key=lambda item: item["port"]),
        "inputs": sorted(inputs, key=lambda item: item["name"]),
        "custom_bundle": bundle,
    }


def _normalize_isolated_transport_document(
    value: Any,
    *,
    multi_output: bool = False,
) -> dict[str, Any]:
    _exact_mapping(value, {"schema_version", "bindings", "inputs", "custom_bundle"}, "adapter transport plan")
    expected_version = (
        ISOLATED_NODE_MULTI_OUTPUT_TRANSPORT_SCHEMA_VERSION if multi_output else ISOLATED_NODE_TRANSPORT_SCHEMA_VERSION
    )
    if value["schema_version"] != expected_version:
        raise NodeStageError(
            "node_adapter_output_validation",
            "adapter_plan_version",
            "adapter transport plan schema_version is not supported",
        )
    raw_bindings = value["bindings"]
    raw_inputs = value["inputs"]
    if (
        not isinstance(raw_bindings, list)
        or not raw_bindings
        or len(raw_bindings) > MAX_RUNTIME_ADAPTER_BINDINGS
        or not isinstance(raw_inputs, list)
        or len(raw_inputs) > MAX_RUNTIME_ADAPTER_BINDINGS
    ):
        raise NodeStageError(
            "node_adapter_output_validation",
            "adapter_plan_size",
            "adapter transport plan lists are invalid or excessive",
        )
    inputs = [_normalize_input_record(item) for item in raw_inputs]
    names = [item["name"] for item in inputs]
    if len(set(names)) != len(names) or sum(item["size"] for item in inputs) > MAX_RUNTIME_INPUT_TOTAL_BYTES:
        raise NodeStageError(
            "node_adapter_input_validation",
            "input_identity",
            "artifact input names are duplicated or exceed the aggregate size limit",
        )
    bindings = []
    for item in raw_bindings:
        if not isinstance(item, Mapping):
            raise NodeStageError(
                "node_adapter_output_validation",
                "binding_shape",
                "adapter transport binding must be an object",
            )
        if item.get("direction") == "input":
            bindings.append(_normalize_input_binding(item))
        elif item.get("direction") == "output":
            variant_id = item.get("variant_id") if multi_output else None
            legacy_item = dict(item)
            if multi_output:
                if (
                    set(legacy_item)
                    != {
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
                        "artifact_key",
                        "legacy_key_guard",
                        "variant_id",
                    }
                    or not isinstance(variant_id, str)
                    or not SHA256.fullmatch(variant_id)
                ):
                    raise NodeStageError(
                        "node_adapter_output_validation",
                        "binding_variant",
                        "adapter output variant identity is invalid",
                    )
                legacy_item.pop("variant_id")
            normalized_output = _normalize_output_binding(legacy_item)
            if multi_output:
                normalized_output["variant_id"] = variant_id
            bindings.append(normalized_output)
        else:
            raise NodeStageError(
                "node_adapter_output_validation",
                "binding_direction",
                "adapter transport binding direction is invalid",
            )
    input_bindings = [binding for binding in bindings if binding["direction"] == "input"]
    output_bindings = [binding for binding in bindings if binding["direction"] == "output"]
    if (not multi_output and len(output_bindings) != 1) or (multi_output and len(output_bindings) < 2):
        raise NodeStageError(
            "node_adapter_output_validation",
            "output_binding_count",
            "adapter transport plan has an invalid output binding count",
        )
    ports = [
        (
            binding["direction"],
            binding["port"],
            binding.get("variant_id") if binding["direction"] == "output" else None,
        )
        for binding in bindings
    ]
    if len(set(ports)) != len(ports):
        raise NodeStageError(
            "node_adapter_output_validation",
            "binding_duplicate",
            "adapter transport ports are duplicated",
        )
    if multi_output:
        if len({binding["port"] for binding in output_bindings}) != 1:
            raise NodeStageError(
                "node_adapter_output_validation",
                "output_port",
                "adapter output variants must belong to one output port",
            )
        adapter_identities = [
            _json_dumps(binding["adapter"], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for binding in output_bindings
        ]
        if len(set(adapter_identities)) != len(adapter_identities):
            raise NodeStageError(
                "node_adapter_output_validation",
                "output_adapter_duplicate",
                "adapter output variants must have distinct save identities",
            )
    input_by_name = {item["name"]: item for item in inputs}
    referenced: set[str] = set()
    for binding in input_bindings:
        item = input_by_name.get(binding["input_name"])
        if item is None:
            raise NodeStageError(
                "node_adapter_input_validation",
                "binding_input_missing",
                "artifact input binding references an undeclared file",
            )
        referenced.add(item["name"])
        adapter = binding["adapter"]
        if (
            item["port"],
            item["format_tag"],
            item["semantic_type"],
            item["adapter_id"],
            item["media_type"],
        ) != (
            binding["port"],
            adapter["format_tag"],
            binding["semantic_type"],
            adapter["id"],
            adapter["presentation"]["media_type"],
        ):
            raise NodeStageError(
                "node_adapter_input_validation",
                "binding_identity",
                "artifact input does not match its exact binding identity",
            )
    if referenced != set(names):
        raise NodeStageError(
            "node_adapter_input_validation",
            "input_unbound",
            "every artifact input file must have one binding",
        )
    bundle = None if value["custom_bundle"] is None else _normalize_bundle(value["custom_bundle"])
    if bundle is not None and bundle["name"] in set(names):
        raise NodeStageError(
            "node_adapter_input_validation",
            "bundle_name_collision",
            "artifact input name collides with the custom adapter bundle",
        )
    custom = [binding for binding in bindings if binding["adapter"]["kind"] == "custom"]
    if custom:
        digests = {binding["adapter"]["bundle_sha256"] for binding in custom}
        symbols = {
            binding["adapter"]["load_symbol"] if binding["direction"] == "input" else binding["adapter"]["save_symbol"]
            for binding in custom
        }
        if bundle is None or digests != {bundle["sha256"]} or symbols != set(bundle["functions"]):
            raise NodeStageError(
                "node_adapter_output_validation",
                "bundle_identity",
                "custom adapter bundle does not match its exact role bindings",
            )
    elif bundle is not None:
        raise NodeStageError(
            "node_adapter_output_validation",
            "bundle_unused",
            "custom adapter bundle has no custom transport binding",
        )
    return {
        "schema_version": expected_version,
        "bindings": sorted(
            bindings,
            key=lambda item: (item["direction"], item["port"], str(item.get("variant_id", ""))),
        ),
        "inputs": sorted(inputs, key=lambda item: item["name"]),
        "custom_bundle": bundle,
    }


def _normalize_input_record(value: Any) -> dict[str, Any]:
    keys = {
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
    _exact_mapping(value, keys, "artifact input")
    name = _safe_name(value["name"], "artifact input name")
    staged_name = _safe_name(value["staged_name"], "artifact staged name")
    if name != staged_name:
        raise NodeStageError(
            "node_adapter_input_validation",
            "input_staged_name",
            "artifact staged name must equal its declared name",
        )
    size = value["size"]
    if type(size) is not int or size < 0 or size > MAX_RUNTIME_INPUT_BYTES:
        raise NodeStageError("node_adapter_input_validation", "input_size", "artifact input size is invalid")
    sha256 = value["sha256"]
    if not isinstance(sha256, str) or not SHA256.fullmatch(sha256):
        raise NodeStageError("node_adapter_input_validation", "input_sha256", "artifact input digest is invalid")
    if value["content_base64"] is not None:
        raise NodeStageError(
            "node_adapter_input_validation",
            "input_content",
            "local isolated inputs must use staged files",
        )
    semantic_type = value["semantic_type"]
    if semantic_type is not None and (not isinstance(semantic_type, str) or not SAFE_TYPE.fullmatch(semantic_type)):
        raise NodeStageError(
            "node_adapter_input_validation",
            "input_semantic_type",
            "artifact input semantic type is invalid",
        )
    return {
        "name": name,
        "port": _safe_port(value["port"]),
        "size": size,
        "sha256": sha256,
        "format_tag": _safe_tag(value["format_tag"]),
        "semantic_type": semantic_type,
        "adapter_id": _required_text(value["adapter_id"], "artifact adapter id", 256),
        "media_type": _optional_text(value["media_type"], "artifact media type", 256),
        "content_base64": None,
        "staged_name": staged_name,
    }


def _normalize_input_binding(value: Any) -> dict[str, Any]:
    keys = {
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
        "artifact_key",
        "legacy_key_guard",
    }
    _exact_mapping(value, keys, "artifact input binding")
    if value["direction"] != "input" or value["transport"] != "artifact" or value["result_path"] != []:
        raise NodeStageError(
            "node_adapter_input_validation",
            "binding_transport",
            "mixed invocation bindings must be artifact inputs",
        )
    source = value["resolution_source"]
    if source not in {"system_default", "preset", "run_override"}:
        raise NodeStageError(
            "node_adapter_input_validation",
            "binding_source",
            "artifact input resolution source is invalid",
        )
    semantic_type = value["semantic_type"]
    if semantic_type is not None and (not isinstance(semantic_type, str) or not SAFE_TYPE.fullmatch(semantic_type)):
        raise NodeStageError(
            "node_adapter_input_validation",
            "binding_semantic_type",
            "artifact input semantic type is invalid",
        )
    port = _safe_port(value["port"])
    external_name = _required_text(value["external_name"], "external name", 128)
    if external_name != port:
        raise NodeStageError(
            "node_adapter_input_validation",
            "binding_external_name",
            "artifact input external name must equal its declared port",
        )
    argument = _normalize_argument(value["argument"])
    if argument != {"kind": "keyword", "name": port, "index": None}:
        raise NodeStageError(
            "node_adapter_input_validation",
            "binding_argument",
            "artifact input argument must be the keyword location of its declared port",
        )
    adapter = _normalize_load_descriptor(value["adapter"])
    artifact_key = _required_text(value["artifact_key"], "artifact key", 512)
    guard = value["legacy_key_guard"]
    if type(guard) is not bool or (guard and artifact_key != adapter["key"]):
        raise NodeStageError(
            "node_adapter_input_validation",
            "artifact_key",
            "artifact key does not satisfy its load adapter guard",
        )
    if adapter["format_tag"] not in adapter["accepted_tags"]:
        raise NodeStageError(
            "node_adapter_input_validation",
            "artifact_tag",
            "artifact tag is not accepted by its load adapter",
        )
    return {
        "direction": "input",
        "port": port,
        "external_name": external_name,
        "semantic_type": semantic_type,
        "adapter": adapter,
        "resolution_source": source,
        "transport": "artifact",
        "argument": argument,
        "input_name": _safe_name(value["input_name"], "binding input name"),
        "result_path": [],
        "artifact_key": artifact_key,
        "legacy_key_guard": guard,
    }


def _normalize_load_descriptor(value: Any) -> dict[str, Any]:
    keys = {
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
    _exact_mapping(value, keys, "load adapter descriptor")
    kind = value["kind"]
    adapter_id = _required_text(value["id"], "adapter id", 256)
    key = _required_text(value["key"], "adapter key", 512)
    if "@" not in key or not key.rpartition("@")[0] or not key.rpartition("@")[2]:
        raise NodeStageError("node_adapter_input_validation", "adapter_key", "load adapter key is invalid")
    format_tag = _safe_tag(value["format_tag"])
    accepted = value["accepted_tags"]
    if (
        not isinstance(accepted, list)
        or not accepted
        or any(not isinstance(item, str) or not SAFE_TAG.fullmatch(item) for item in accepted)
        or len(set(accepted)) != len(accepted)
        or format_tag not in accepted
    ):
        raise NodeStageError(
            "node_adapter_input_validation",
            "adapter_tags",
            "load adapter accepted tags are invalid",
        )
    distributions = _normalize_distributions(value["distributions"])
    presentation = value["presentation"]
    _exact_mapping(presentation, {"media_type", "preferred_extension"}, "adapter presentation")
    normalized_presentation = {
        "media_type": _optional_text(presentation["media_type"], "adapter media type", 256),
        "preferred_extension": _optional_text(presentation["preferred_extension"], "adapter extension", 32),
    }
    if kind == "builtin":
        expected = BUILTIN_LOADERS.get(adapter_id)
        observed_packages = {_canonical_package(item["package"]) for item in distributions}
        if (
            expected is None
            or value["save_symbol"] is not None
            or value["load_symbol"] is not None
            or value["bundle_sha256"] is not None
            or (key, format_tag, frozenset(accepted)) != expected
            or normalized_presentation != BUILTIN_PRESENTATION.get(adapter_id)
            or observed_packages != BUILTIN_REQUIRED_PACKAGES.get(adapter_id, set())
        ):
            raise NodeStageError(
                "node_adapter_input_validation",
                "builtin_identity",
                "built-in load adapter identity is invalid",
            )
    elif kind == "custom":
        digest = value["bundle_sha256"]
        symbol = value["load_symbol"]
        if (
            not isinstance(digest, str)
            or not SHA256.fullmatch(digest)
            or adapter_id != f"custom:{digest}"
            or value["save_symbol"] is not None
            or not isinstance(symbol, str)
            or not symbol.isidentifier()
            or normalized_presentation != {"media_type": None, "preferred_extension": None}
        ):
            raise NodeStageError(
                "node_adapter_input_validation",
                "custom_identity",
                "custom load adapter identity is invalid",
            )
    else:
        raise NodeStageError(
            "node_adapter_input_validation",
            "adapter_kind",
            "load adapter kind is not supported",
        )
    return {
        "kind": kind,
        "id": adapter_id,
        "key": key,
        "format_tag": format_tag,
        "accepted_tags": sorted(accepted),
        "distributions": distributions,
        "save_symbol": value["save_symbol"],
        "load_symbol": value["load_symbol"],
        "bundle_sha256": value["bundle_sha256"],
        "presentation": normalized_presentation,
    }


def _normalize_output_binding(value: Any) -> dict[str, Any]:
    try:
        return _normalize_output_binding_value(value)
    except NodeStageError as exc:
        raise NodeStageError(
            "node_adapter_output_validation",
            exc.code,
            "artifact output binding is invalid",
        ) from None


def _normalize_output_binding_value(value: Any) -> dict[str, Any]:
    keys = {
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
        "artifact_key",
        "legacy_key_guard",
    }
    _exact_mapping(value, keys, "artifact output binding")
    if value["direction"] != "output" or value["transport"] != "artifact":
        raise NodeStageError(
            "node_adapter_output_validation",
            "binding_transport",
            "isolated output binding must use artifact transport",
        )
    source = value["resolution_source"]
    if source not in {"system_default", "preset", "run_override"}:
        raise NodeStageError(
            "node_adapter_output_validation",
            "binding_source",
            "artifact output resolution source is invalid",
        )
    semantic_type = value["semantic_type"]
    if semantic_type is not None and (not isinstance(semantic_type, str) or not SAFE_TYPE.fullmatch(semantic_type)):
        raise NodeStageError(
            "node_adapter_output_validation",
            "binding_semantic_type",
            "artifact output semantic type is invalid",
        )
    port = _safe_port(value["port"])
    external_name = _required_text(value["external_name"], "external name", 128)
    if external_name != port:
        raise NodeStageError(
            "node_adapter_output_validation",
            "binding_external_name",
            "artifact output external name must equal its declared port",
        )
    if value["argument"] is not None or value["input_name"] is not None or value["result_path"] != []:
        raise NodeStageError(
            "node_adapter_output_validation",
            "binding_location",
            "artifact output binding cannot declare an input or nested result location",
        )
    adapter = _normalize_save_descriptor(value["adapter"])
    artifact_key = _required_text(value["artifact_key"], "artifact key", 512)
    if artifact_key != adapter["key"] or value["legacy_key_guard"] is not False:
        raise NodeStageError(
            "node_adapter_output_validation",
            "artifact_key",
            "artifact output key does not match its exact save adapter",
        )
    return {
        "direction": "output",
        "port": port,
        "external_name": external_name,
        "semantic_type": semantic_type,
        "adapter": adapter,
        "resolution_source": source,
        "transport": "artifact",
        "argument": None,
        "input_name": None,
        "result_path": [],
        "artifact_key": artifact_key,
        "legacy_key_guard": False,
    }


def _normalize_save_descriptor(value: Any) -> dict[str, Any]:
    try:
        return _normalize_save_descriptor_value(value)
    except NodeStageError as exc:
        raise NodeStageError(
            "node_adapter_output_validation",
            exc.code,
            "artifact output save adapter descriptor is invalid",
        ) from None


def _normalize_save_descriptor_value(value: Any) -> dict[str, Any]:
    keys = {
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
    _exact_mapping(value, keys, "save adapter descriptor")
    kind = value["kind"]
    adapter_id = _required_text(value["id"], "adapter id", 256)
    key = _required_text(value["key"], "adapter key", 512)
    key_head, separator, key_format = key.rpartition("@")
    if not separator or not key_head or not key_format:
        raise NodeStageError("node_adapter_output_validation", "adapter_key", "save adapter key is invalid")
    format_tag = _safe_tag(value["format_tag"])
    accepted = value["accepted_tags"]
    if (
        not isinstance(accepted, list)
        or not accepted
        or any(not isinstance(item, str) or not SAFE_TAG.fullmatch(item) for item in accepted)
        or len(set(accepted)) != len(accepted)
        or format_tag not in accepted
        or key_format != format_tag
    ):
        raise NodeStageError(
            "node_adapter_output_validation",
            "adapter_tags",
            "save adapter tags are invalid",
        )
    distributions = _normalize_distributions(value["distributions"])
    presentation = value["presentation"]
    _exact_mapping(presentation, {"media_type", "preferred_extension"}, "adapter presentation")
    normalized_presentation = {
        "media_type": _optional_text(presentation["media_type"], "adapter media type", 256),
        "preferred_extension": _optional_text(presentation["preferred_extension"], "adapter extension", 32),
    }
    if kind == "builtin":
        expected = BUILTIN_SAVERS.get(adapter_id)
        observed_packages = {_canonical_package(item["package"]) for item in distributions}
        if (
            expected is None
            or value["save_symbol"] is not None
            or value["load_symbol"] is not None
            or value["bundle_sha256"] is not None
            or (key, format_tag, frozenset(accepted)) != expected
            or normalized_presentation != BUILTIN_PRESENTATION.get(adapter_id)
            or observed_packages != BUILTIN_REQUIRED_PACKAGES.get(adapter_id, set())
        ):
            raise NodeStageError(
                "node_adapter_output_validation",
                "builtin_identity",
                "built-in save adapter identity is invalid",
            )
    elif kind == "custom":
        digest = value["bundle_sha256"]
        symbol = value["save_symbol"]
        if (
            not isinstance(digest, str)
            or not SHA256.fullmatch(digest)
            or adapter_id != f"custom:{digest}"
            or not isinstance(symbol, str)
            or not symbol.isidentifier()
            or value["load_symbol"] is not None
            or accepted != [format_tag]
            or normalized_presentation != {"media_type": None, "preferred_extension": None}
        ):
            raise NodeStageError(
                "node_adapter_output_validation",
                "custom_identity",
                "custom save adapter identity is invalid",
            )
    else:
        raise NodeStageError(
            "node_adapter_output_validation",
            "adapter_kind",
            "save adapter kind is not supported",
        )
    return {
        "kind": kind,
        "id": adapter_id,
        "key": key,
        "format_tag": format_tag,
        "accepted_tags": sorted(accepted),
        "distributions": distributions,
        "save_symbol": value["save_symbol"],
        "load_symbol": value["load_symbol"],
        "bundle_sha256": value["bundle_sha256"],
        "presentation": normalized_presentation,
    }


def _normalize_argument(value: Any) -> dict[str, Any]:
    _exact_mapping(value, {"kind", "name", "index"}, "argument location")
    kind = value["kind"]
    if kind in {"keyword", "default"}:
        name = _required_text(value["name"], "argument name", 128)
        if value["index"] is not None:
            raise NodeStageError(
                "node_adapter_input_validation",
                "argument_index",
                "named artifact argument index must be null",
            )
        return {"kind": kind, "name": name, "index": None}
    if kind == "positional":
        index = value["index"]
        if value["name"] is not None or type(index) is not int or index < 0:
            raise NodeStageError(
                "node_adapter_input_validation",
                "argument_index",
                "positional artifact argument location is invalid",
            )
        return {"kind": kind, "name": None, "index": index}
    raise NodeStageError(
        "node_adapter_input_validation",
        "argument_kind",
        "artifact argument location kind is invalid",
    )


def _normalize_bundle(value: Any) -> dict[str, Any]:
    _exact_mapping(value, {"name", "size", "sha256", "functions", "content_base64", "staged_name"}, "bundle")
    name = _safe_name(value["name"], "bundle name")
    staged_name = _safe_name(value["staged_name"], "bundle staged name")
    size = value["size"]
    functions = value["functions"]
    if (
        name != "custom-adapters.py"
        or staged_name != name
        or type(size) is not int
        or size < 1
        or size > MAX_CUSTOM_BUNDLE_BYTES
        or not isinstance(value["sha256"], str)
        or not SHA256.fullmatch(value["sha256"])
        or value["content_base64"] is not None
        or not isinstance(functions, list)
        or not functions
        or any(not isinstance(item, str) or not item.isidentifier() for item in functions)
        or len(set(functions)) != len(functions)
    ):
        raise NodeStageError(
            "node_adapter_input_validation",
            "bundle_shape",
            "custom adapter bundle metadata is invalid",
        )
    return {
        "name": name,
        "size": size,
        "sha256": value["sha256"],
        "functions": sorted(functions),
        "content_base64": None,
        "staged_name": staged_name,
    }


def _normalize_distributions(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise NodeStageError(
            "node_adapter_input_validation",
            "distributions_shape",
            "adapter distributions must be a list",
        )
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in value:
        _exact_mapping(raw, {"package", "version"}, "adapter distribution")
        package = raw["package"]
        version = raw["version"]
        if (
            not isinstance(package, str)
            or not PACKAGE.fullmatch(package)
            or not isinstance(version, str)
            or not VERSION.fullmatch(version)
        ):
            raise NodeStageError(
                "node_adapter_input_validation",
                "distribution_identity",
                "adapter distribution identity is invalid",
            )
        canonical = _canonical_package(package)
        if canonical in seen:
            raise NodeStageError(
                "node_adapter_input_validation",
                "distribution_duplicate",
                "adapter distribution is duplicated",
            )
        seen.add(canonical)
        result.append({"package": package, "version": version})
    return sorted(result, key=lambda item: (_canonical_package(item["package"]), item["version"]))


def _exact_mapping(value: Any, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise NodeStageError(
            "node_adapter_input_validation",
            "protocol_keys",
            f"{label} does not match the closed schema",
        )
    return value


def _safe_name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_INPUT_NAME.fullmatch(value) or value in {".", ".."}:
        raise NodeStageError("node_adapter_input_validation", "unsafe_name", f"{label} is not a safe filename")
    return value


def _safe_port(value: Any) -> str:
    if not isinstance(value, str) or not SAFE_PORT.fullmatch(value):
        raise NodeStageError("node_adapter_input_validation", "unsafe_port", "artifact input port is invalid")
    return value


def _safe_tag(value: Any) -> str:
    if not isinstance(value, str) or not SAFE_TAG.fullmatch(value):
        raise NodeStageError("node_adapter_input_validation", "unsafe_tag", "artifact format tag is invalid")
    return value


def _required_text(value: Any, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise NodeStageError("node_adapter_input_validation", "protocol_text", f"{label} is invalid")
    return value


def _optional_text(value: Any, label: str, maximum: int) -> str | None:
    if value is None:
        return None
    return _required_text(value, label, maximum)


def _canonical_package(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def to_jsonable(value: Any, *, path: str = "$") -> Any:
    """Convert common Python containers into JSON-compatible values."""

    if type(value) in JSON_SCALARS:
        _validate_json_value(value, path=path)
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                _validate_json_value({key: None}, path=path)
                raise AssertionError("JSON key validation unexpectedly accepted a non-string key")
            result[key] = to_jsonable(item, path=_json_child_path(path, key))
        return result
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [to_jsonable(item, path="{}[{}]".format(path, index)) for index, item in enumerate(value)]
    if isinstance(value, set):
        return [
            to_jsonable(item, path="{}[{}]".format(path, index)) for index, item in enumerate(sorted(value, key=repr))
        ]
    raise TypeError("result is not JSON serializable; return JSON-like data or declare artifacts")


def _json_dumps(
    value: Any,
    *,
    ensure_ascii: bool = False,
    indent: int | str | None = None,
    sort_keys: bool = True,
    separators: tuple[str, str] | None = (",", ":"),
) -> str:
    """Mirror the packaged JSON contract inside this stdlib-only runner."""

    _validate_json_value(value)
    return json.dumps(
        value,
        ensure_ascii=ensure_ascii,
        indent=indent,
        sort_keys=sort_keys,
        separators=separators,
        allow_nan=False,
    )


def _validate_json_value(value: Any, *, path: str = "$", active_ids: set[int] | None = None) -> None:
    active_ids = set() if active_ids is None else active_ids
    value_type = type(value)
    if value_type in JSON_SCALARS:
        if value_type is float and not math.isfinite(value):
            raise ValueError("invalid splime JSON value at {}: non-finite floats are not permitted".format(path))
        if value_type is str:
            _validate_unicode_scalar_string(value, path=path, role="string")
        return
    if value_type not in {list, dict}:
        raise ValueError(
            "invalid splime JSON value at {}: unsupported value type `{}`".format(path, value_type.__name__)
        )

    container_id = id(value)
    if container_id in active_ids:
        raise ValueError("invalid splime JSON value at {}: circular container reference is not permitted".format(path))
    active_ids.add(container_id)
    try:
        if value_type is list:
            for index, item in enumerate(value):
                _validate_json_value(item, path="{}[{}]".format(path, index), active_ids=active_ids)
            return
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("invalid splime JSON value at {}: object key {!r} is not a string".format(path, key))
            _validate_unicode_scalar_string(key, path=path, role="object key")
            _validate_json_value(item, path=_json_child_path(path, key), active_ids=active_ids)
    finally:
        active_ids.remove(container_id)


def _json_child_path(path: str, key: str) -> str:
    return "{}[{}]".format(path, json.dumps(key, ensure_ascii=False, allow_nan=False))


def _validate_unicode_scalar_string(value: str, *, path: str, role: str) -> None:
    for index, character in enumerate(value):
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDFFF:
            raise ValueError(
                "invalid splime JSON value at {}: {} contains Unicode surrogate U+{:04X} at character {}; "
                "use Unicode scalar values".format(path, role, code_point, index)
            )


def _read_verified_runtime_file(path: Path, *, expected_size: int, expected_sha256: str) -> bytes:
    before = path.lstat()
    if not _is_regular(before) or before.st_nlink != 1:
        raise RuntimeError("runtime adapter input is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            not _is_regular(opened)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise RuntimeError("runtime adapter input changed before loading")
        chunks: list[bytes] = []
        remaining = MAX_RUNTIME_INPUT_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    current = path.lstat()
    if (
        len(body) > MAX_RUNTIME_INPUT_BYTES
        or not _is_regular(after)
        or not _is_regular(current)
        or after.st_nlink != 1
        or current.st_nlink != 1
        or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
        or (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino)
        or after.st_size != len(body)
        or current.st_size != len(body)
        or len(body) != expected_size
        or hashlib.sha256(body).hexdigest() != expected_sha256
    ):
        raise RuntimeError("runtime adapter input failed identity, size, or checksum verification")
    return body


def _read_bounded_runtime_output(path: Path) -> bytes:
    before = path.lstat()
    if not _is_regular(before) or before.st_nlink != 1:
        raise RuntimeError("runtime adapter output is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            not _is_regular(opened)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise RuntimeError("runtime adapter output changed before validation")
        chunks: list[bytes] = []
        remaining = MAX_RUNTIME_OUTPUT_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    current = path.lstat()
    if (
        len(body) > MAX_RUNTIME_OUTPUT_BYTES
        or not _is_regular(after)
        or not _is_regular(current)
        or after.st_nlink != 1
        or current.st_nlink != 1
        or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
        or (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino)
        or after.st_size != len(body)
        or current.st_size != len(body)
    ):
        raise RuntimeError("runtime adapter output failed bounded identity validation")
    return body


def _write_private_snapshot(path: Path, body: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o400)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _is_regular(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0))
    return stat.S_ISREG(value.st_mode) and not bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _is_regular_directory(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0))
    return stat.S_ISDIR(value.st_mode) and not bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _load_verified_custom_module(document: Mapping[str, Any], *, input_path: Path) -> ModuleType | None:
    bundle = document["custom_bundle"]
    if bundle is None:
        return None
    bundle_path = input_path.parent / "runtime-inputs" / bundle["staged_name"]
    try:
        body = _read_verified_runtime_file(
            bundle_path,
            expected_size=bundle["size"],
            expected_sha256=bundle["sha256"],
        )
        source = body.decode("utf-8")
        tree = ast.parse(source, filename="<runtime-custom-adapters>", mode="exec")
        _validate_custom_bundle_tree(tree, source=source, document=document)
        module = load_module(bundle_path, "_spl_runtime_input_adapters")
    except NodeStageError:
        raise
    except BaseException:
        raise NodeStageError(
            "node_adapter_load",
            "custom_bundle_load",
            "custom adapter bundle could not be verified and loaded",
        ) from None
    for name in bundle["functions"]:
        if not callable(getattr(module, name, None)):
            raise NodeStageError(
                "node_adapter_load",
                "custom_bundle_symbol",
                "custom adapter bundle did not define every declared load function",
            )
    return module


def _validate_custom_bundle_tree(tree: ast.Module, *, source: str, document: Mapping[str, Any]) -> None:
    bundle = document["custom_bundle"]
    definitions = {
        node.name: node for node in tree.body if isinstance(node, ast.FunctionDef) and not node.decorator_list
    }
    if len(definitions) != len(tree.body) or set(definitions) != set(bundle["functions"]):
        raise NodeStageError(
            "node_adapter_load",
            "custom_bundle_source",
            "custom adapter bundle source does not match its declared functions",
        )
    packages_by_symbol: dict[str, set[tuple[str, str]]] = {}
    roles_by_symbol: dict[str, set[str]] = {}
    for binding in document["bindings"]:
        adapter = binding["adapter"]
        if adapter["kind"] != "custom":
            continue
        role = "load" if binding["direction"] == "input" else "save"
        symbol = adapter[f"{role}_symbol"]
        packages = {(_canonical_package(item["package"]), item["version"]) for item in adapter["distributions"]}
        existing = packages_by_symbol.get(symbol)
        packages_by_symbol[symbol] = packages if existing is None else existing & packages
        roles_by_symbol.setdefault(symbol, set()).add(role)
    headers: dict[str, dict[str, tuple[str, str]]] = {}
    for line in source.splitlines():
        if not line.startswith("# spl-runtime-import-v1"):
            continue
        match = IMPORT_HEADER.fullmatch(line)
        if match is None:
            raise NodeStageError(
                "node_adapter_load",
                "custom_import_metadata",
                "custom adapter import metadata is malformed",
            )
        symbol, module, package, version = match.groups()
        rows = headers.setdefault(symbol, {})
        if module in rows:
            raise NodeStageError(
                "node_adapter_load",
                "custom_import_metadata",
                "custom adapter import metadata is duplicated",
            )
        rows[module] = (package, version)
    installed_owners = importlib.metadata.packages_distributions()
    for symbol, definition in definitions.items():
        roles = roles_by_symbol.get(symbol, set())
        if len(roles) != 1:
            raise NodeStageError(
                "node_adapter_load",
                "custom_bundle_symbol",
                "custom adapter function does not have one exact save/load role",
            )
        [role] = roles
        _validate_adapter_function(definition, role=role)
        roots = _function_import_roots(definition)
        declared_headers = headers.get(symbol, {})
        if set(declared_headers) != set(roots):
            raise NodeStageError(
                "node_adapter_load",
                "custom_import_metadata",
                "custom adapter import metadata is incomplete",
            )
        declared_packages = packages_by_symbol.get(symbol)
        if declared_packages is None:
            raise NodeStageError(
                "node_adapter_load",
                "custom_bundle_symbol",
                "custom load function is not bound by the input plan",
            )
        for module, (package, version) in declared_headers.items():
            canonical = _canonical_package(package)
            if (canonical, version) not in declared_packages:
                raise NodeStageError(
                    "node_adapter_load",
                    "custom_import_dependency",
                    "custom adapter import is not bound to its exact declared distribution",
                )
            owners = {_canonical_package(owner): owner for owner in installed_owners.get(module, [])}
            if set(owners) != {canonical}:
                raise NodeStageError(
                    "node_adapter_load",
                    "custom_import_owner",
                    "custom adapter import does not have one exact installed owner",
                )
            try:
                actual = importlib.metadata.version(owners[canonical])
            except PackageNotFoundError:
                raise NodeStageError(
                    "node_adapter_load",
                    "custom_import_missing",
                    "custom adapter distribution is not installed",
                ) from None
            if actual != version:
                raise NodeStageError(
                    "node_adapter_load",
                    "custom_import_version",
                    "custom adapter distribution version does not match",
                )


def _validate_adapter_function(definition: ast.FunctionDef, *, role: str) -> None:
    args = definition.args
    expected_arity = 2 if role == "save" else 1
    if (
        definition.decorator_list
        or args.posonlyargs
        or args.vararg
        or args.kwarg
        or args.kwonlyargs
        or args.defaults
        or args.kw_defaults
        or len(args.args) != expected_arity
    ):
        raise NodeStageError(
            "node_adapter_load",
            "custom_signature",
            f"custom {role} adapter must be one undecorated {expected_arity}-argument function",
        )
    for node in ast.walk(definition):
        if isinstance(
            node,
            (ast.AsyncFunctionDef, ast.ClassDef, ast.Global, ast.Nonlocal, ast.Yield, ast.YieldFrom, ast.Await),
        ):
            raise NodeStageError(
                "node_adapter_load",
                "custom_construct",
                f"custom {role} adapter contains an unsupported source construct",
            )
        if isinstance(node, ast.ImportFrom) and node.level:
            raise NodeStageError(
                "node_adapter_load",
                "custom_import",
                f"custom {role} adapter may use only absolute imports",
            )
        if isinstance(node, ast.Call):
            dynamic_name = None
            if isinstance(node.func, ast.Name) and node.func.id in DYNAMIC_CALLS:
                dynamic_name = node.func.id
            elif isinstance(node.func, ast.Attribute) and node.func.attr in DYNAMIC_CALLS:
                dynamic_name = node.func.attr
            if dynamic_name is not None:
                raise NodeStageError(
                    "node_adapter_load",
                    "custom_dynamic_call",
                    f"custom {role} adapter contains a forbidden dynamic call",
                )
        if isinstance(node, (ast.FunctionDef, ast.Lambda)) and node is not definition:
            raise NodeStageError(
                "node_adapter_load",
                "custom_nested_callable",
                f"custom {role} adapter may not contain nested callables",
            )
    local = {argument.arg for argument in args.args}
    for node in ast.walk(definition):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                local.add(alias.asname or alias.name.split(".", 1)[0])
        elif isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Param)):
            local.add(node.id)
    allowed = set(vars(builtins)) | {"None", "True", "False"}
    loaded = {node.id for node in ast.walk(definition) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}
    if loaded - local - allowed:
        raise NodeStageError(
            "node_adapter_load",
            "custom_unresolved_global",
            f"custom {role} adapter contains unresolved global names",
        )


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


def _adapter_loader(binding: Mapping[str, Any], custom_module: ModuleType | None) -> Any:
    descriptor = binding["adapter"]
    if descriptor["kind"] == "custom":
        if custom_module is None:
            raise RuntimeError("custom adapter module is unavailable")
        loader = getattr(custom_module, descriptor["load_symbol"], None)
        if not callable(loader):
            raise RuntimeError("custom adapter load function is unavailable")
        return loader
    adapter_id = descriptor["id"]
    loaders = {
        "json": _load_json,
        "opaque-file": _load_opaque_file,
        "text-file-utf8": _load_text_file,
        "binary-file": _load_binary_file,
        "dataframe-json-split": _load_dataframe_json_split,
        "dataframe-csv-semicolon": _load_dataframe_csv_semicolon,
        "dataframe-xlsx": _load_dataframe_xlsx,
        "png-pillow": _load_png_pillow,
    }
    loader = loaders.get(adapter_id)
    if loader is None:
        raise RuntimeError("built-in adapter load function is unavailable")
    return loader


def _adapter_saver(binding: Mapping[str, Any], custom_module: ModuleType | None) -> Any:
    descriptor = binding["adapter"]
    if descriptor["kind"] == "custom":
        if custom_module is None:
            raise RuntimeError("custom adapter module is unavailable")
        saver = getattr(custom_module, descriptor["save_symbol"], None)
        if not callable(saver):
            raise RuntimeError("custom adapter save function is unavailable")
        return saver
    savers = {
        "json": _save_json,
        "opaque-file": _save_opaque_file,
        "text-file-utf8": _save_text_file,
        "binary-file": _save_binary_file,
        "dataframe-json-split": _save_dataframe_json_split,
        "dataframe-csv-semicolon": _save_dataframe_csv_semicolon,
        "dataframe-xlsx": _save_dataframe_xlsx,
        "png-pillow": _save_png_pillow,
    }
    saver = savers.get(descriptor["id"])
    if saver is None:
        raise RuntimeError("built-in adapter save function is unavailable")
    return saver


def _save_opaque_file(path: str, value: Any) -> None:
    Path(path).write_bytes(Path(value).read_bytes())


def _save_json(path: str, value: Any) -> None:
    Path(path).write_text(_json_dumps(value), encoding="utf-8", newline="")


def _save_text_file(path: str, value: Any) -> None:
    if type(value) is not str:
        raise TypeError("text-file-utf8 adapter requires an exact str value")
    Path(path).write_text(value, encoding="utf-8", newline="")


def _save_binary_file(path: str, value: Any) -> None:
    if type(value) is not bytes:
        raise TypeError("binary-file adapter requires an exact bytes value")
    Path(path).write_bytes(value)


def _save_dataframe_json_split(path: str, value: Any) -> None:
    Path(path).write_text(str(value.to_json(orient="split", date_format="iso")), encoding="utf-8", newline="")


def _save_dataframe_csv_semicolon(path: str, value: Any) -> None:
    value.to_csv(path, sep=";", encoding="utf-8", index=False)


def _save_dataframe_xlsx(path: str, value: Any) -> None:
    value.to_excel(path, engine="openpyxl", sheet_name="data", index=False)


def _save_png_pillow(path: str, value: Any) -> None:
    value.save(path, format="PNG")


def _load_opaque_file(path: str) -> Path:
    return Path(path)


def _load_json(path: str) -> Any:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    _validate_json_value(value)
    return value


def _load_text_file(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _load_binary_file(path: str) -> bytes:
    return Path(path).read_bytes()


def _load_dataframe_json_split(path: str) -> Any:
    import pandas as pd  # type: ignore[import-untyped]  # Optional runtime dependency.

    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict) or set(document) != {"columns", "index", "data"}:
        raise ValueError("DataFrame JSON-split artifact has an invalid closed shape")
    return pd.DataFrame(data=document["data"], columns=document["columns"], index=document["index"])


def _load_dataframe_csv_semicolon(path: str) -> Any:
    import pandas as pd

    return pd.read_csv(path, sep=";", encoding="utf-8")


def _load_dataframe_xlsx(path: str) -> Any:
    import pandas as pd

    return pd.read_excel(path, engine="openpyxl", sheet_name="data")


def _load_png_pillow(path: str) -> Any:
    # Pillow may be absent, or installed with its own type information.
    from PIL import Image  # type: ignore[import-not-found, unused-ignore]

    with Image.open(path) as source:
        if source.format != "PNG":
            raise ValueError("PNG adapter received non-PNG content")
        source.load()
        return source.copy()


def copy_artifact(source: Path, target: Path) -> None:
    """Copy one artifact file or directory into the run artifact directory."""

    if not source.exists():
        raise ValueError(f"artifact source is not found: {source}")
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
        _chmod_artifact_tree(target)
    else:
        _ensure_private_dir(target.parent)
        shutil.copy2(source, target)
        _chmod_owner_file(target)


def collect_artifacts(value: Any, artifacts_dir: Path) -> tuple[Any, dict[str, str]]:
    """Extract and copy artifacts declared by the function result."""

    if not isinstance(value, Mapping) or ARTIFACTS_KEY not in value:
        return value, {}

    artifact_spec = value[ARTIFACTS_KEY]
    if RESULT_KEY in value:
        result = value[RESULT_KEY]
    else:
        result = {key: item for key, item in value.items() if key not in {ARTIFACTS_KEY, RESULT_KEY}}

    items: Iterable[tuple[Any, Any]]
    if isinstance(artifact_spec, Mapping):
        items = artifact_spec.items()
    elif isinstance(artifact_spec, Sequence) and not isinstance(artifact_spec, str):
        items = ((Path(str(path)).name, path) for path in artifact_spec)
    else:
        raise TypeError("__spl_artifacts__ must be a mapping or a list of paths")

    copied: dict[str, str] = {}
    _ensure_private_dir(artifacts_dir)
    for name, source in items:
        artifact_name = validate_name(str(name))
        source_path = Path(str(source)).expanduser().absolute()
        target_path = artifacts_dir / artifact_name
        copy_artifact(source_path, target_path)
        copied[artifact_name] = str(target_path)

    return result, copied


def _chmod_artifact_tree(path: Path) -> None:
    if path.is_dir():
        try:
            path.chmod(0o700)
        except OSError:
            pass
        for item in path.rglob("*"):
            if item.is_dir():
                try:
                    item.chmod(0o700)
                except OSError:
                    pass
            elif item.is_file():
                _chmod_owner_file(item)
    elif path.is_file():
        _chmod_owner_file(path)


def load_module(module_path: Path, module_name: str) -> ModuleType:
    """Import a generated module by path without consulting PYTHONPATH."""

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import generated module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    previous_module = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        if previous_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous_module
        raise
    return module


def save_planned_output(
    value: Any,
    *,
    binding: Mapping[str, Any],
    custom_module: ModuleType | None,
    work_dir: Path,
    variant_id: str | None = None,
) -> dict[str, Any]:
    """Save and verify one planned artifact output inside the isolated runtime."""

    suffix = "" if variant_id is None else "-{}".format(variant_id[:16])
    temporary_dir = work_dir / "runtime-output-tmp{}".format(suffix)
    output_dir = work_dir / "runtime-outputs"
    temporary_created = False
    try:
        try:
            temporary_dir.mkdir(mode=0o700, exist_ok=False)
            temporary_created = True
            temporary_dir_identity = temporary_dir.lstat()
        except OSError:
            raise NodeStageError(
                "node_adapter_output_validation",
                "adapter_output_namespace",
                "private adapter output namespace is unavailable",
            ) from None
        temporary_path = temporary_dir / "artifact.bin"
        descriptor = os.open(
            temporary_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        os.close(descriptor)
        try:
            saver = _adapter_saver(binding, custom_module)
            if binding["adapter"]["kind"] == "custom":
                with redirect_stdout(_DISCARD_ADAPTER_OUTPUT), redirect_stderr(_DISCARD_ADAPTER_OUTPUT):
                    saver(str(temporary_path), value)
            else:
                saver(str(temporary_path), value)
        except BaseException:
            raise NodeStageError(
                "node_adapter_save",
                "adapter_save_failed",
                f"save adapter failed for port {binding['port']!r}",
            ) from None
        try:
            current_temporary_dir = temporary_dir.lstat()
            if (
                not _is_regular_directory(temporary_dir_identity)
                or not _is_regular_directory(current_temporary_dir)
                or (temporary_dir_identity.st_dev, temporary_dir_identity.st_ino)
                != (current_temporary_dir.st_dev, current_temporary_dir.st_ino)
                or list(temporary_dir.iterdir()) != [temporary_path]
            ):
                raise RuntimeError("adapter output directory identity changed or contains extra files")
            body = _read_bounded_runtime_output(temporary_path)
        except (OSError, RuntimeError):
            raise NodeStageError(
                "node_adapter_output_validation",
                "adapter_output_file",
                f"save adapter did not produce one bounded regular file for port {binding['port']!r}",
            ) from None
        try:
            output_dir.mkdir(mode=0o700, exist_ok=variant_id is not None)
            output_dir_stat = output_dir.lstat()
            if not _is_regular_directory(output_dir_stat):
                raise OSError
        except OSError:
            raise NodeStageError(
                "node_adapter_output_validation",
                "adapter_output_namespace",
                "dedicated adapter output namespace is unavailable",
            ) from None
        port_digest = hashlib.sha256(str(binding["port"]).encode("utf-8")).hexdigest()[:16]
        name = f"output-{port_digest}.bin" if variant_id is None else f"output-{port_digest}-{variant_id[:16]}.bin"
        final_path = output_dir / name
        _write_private_snapshot(final_path, body)
        digest = hashlib.sha256(body).hexdigest()
        try:
            _read_verified_runtime_file(
                final_path,
                expected_size=len(body),
                expected_sha256=digest,
            )
        except (OSError, RuntimeError):
            raise NodeStageError(
                "node_adapter_output_validation",
                "adapter_output_integrity",
                "saved artifact output failed final integrity verification",
            ) from None
        adapter = binding["adapter"]
        result_descriptor: dict[str, Any] = {
            "schema_version": (
                ISOLATED_NODE_OUTPUT_RESULT_SCHEMA_VERSION
                if variant_id is None
                else ISOLATED_NODE_MULTI_OUTPUT_RESULT_SCHEMA_VERSION
            ),
            "transport": "artifact",
            "port": binding["port"],
            "relative_path": f"runtime-outputs/{name}",
            "size": len(body),
            "sha256": digest,
            "adapter_id": adapter["id"],
            "adapter_key": adapter["key"],
            "format_tag": adapter["format_tag"],
        }
        if variant_id is not None:
            result_descriptor["variant_id"] = variant_id
        return result_descriptor
    finally:
        if temporary_created:
            shutil.rmtree(temporary_dir, ignore_errors=True)


def _execute(
    *,
    module_path: Path,
    module_name: str,
    entrypoint: str,
    input_path: Path,
    result_path: Path,
    artifacts_dir: Path,
    env_spec_path: Path | None = None,
) -> dict[str, Any]:
    """Import, call, and persist one generated functional node."""

    try:
        payload = read_json(input_path)
    except BaseException:
        raise NodeStageError(
            "node_adapter_input_validation",
            "invocation_read",
            "input document could not be read or decoded",
        ) from None

    if env_spec_path is not None:
        try:
            validate_environment(read_json(env_spec_path))
        except BaseException:
            raise NodeStageError(
                "node_adapter_load",
                "environment_mismatch",
                "isolated environment does not satisfy exact node and adapter dependencies",
            ) from None

    args, kwargs, transport_document, custom_module = _prepare_invocation(payload, input_path=input_path)

    try:
        module = load_module(module_path, module_name)
        target = getattr(module, entrypoint)
    except BaseException:
        raise NodeStageError(
            "node_execution",
            "entrypoint_load",
            "generated node module or entrypoint could not be loaded",
        ) from None
    if not callable(target):
        raise NodeStageError("node_execution", "entrypoint_type", "generated node entrypoint is not callable")

    try:
        raw_result = target(*args, **kwargs)
    except BaseException:
        raise NodeStageError("node_execution", "function_failed", "node function failed") from None
    if (
        transport_document is not None
        and transport_document["schema_version"] == ISOLATED_NODE_TRANSPORT_SCHEMA_VERSION
    ):
        [output_binding] = [binding for binding in transport_document["bindings"] if binding["direction"] == "output"]
        output_descriptor = save_planned_output(
            raw_result,
            binding=output_binding,
            custom_module=custom_module,
            work_dir=input_path.parent,
        )
        result_payload: dict[str, Any] = {
            "result": None,
            "artifacts": {},
            "runtime_port_adapter_output": output_descriptor,
        }
        write_json(result_path, result_payload)
        return result_payload
    if (
        transport_document is not None
        and transport_document["schema_version"] == ISOLATED_NODE_MULTI_OUTPUT_TRANSPORT_SCHEMA_VERSION
    ):
        output_bindings = [binding for binding in transport_document["bindings"] if binding["direction"] == "output"]
        try:
            output_descriptors = []
            output_total_bytes = 0
            for binding in output_bindings:
                descriptor = save_planned_output(
                    raw_result,
                    binding=binding,
                    custom_module=custom_module,
                    work_dir=input_path.parent,
                    variant_id=str(binding["variant_id"]),
                )
                output_total_bytes += int(descriptor["size"])
                if output_total_bytes > MAX_RUNTIME_OUTPUT_TOTAL_BYTES:
                    raise NodeStageError(
                        "node_adapter_output_validation",
                        "adapter_output_total_size",
                        "artifact outputs exceed the aggregate size limit",
                    )
                output_descriptors.append(descriptor)
        except BaseException:
            shutil.rmtree(input_path.parent / "runtime-outputs", ignore_errors=True)
            for binding in output_bindings:
                shutil.rmtree(
                    input_path.parent / "runtime-output-tmp-{}".format(str(binding["variant_id"])[:16]),
                    ignore_errors=True,
                )
            raise
        result_payload = {
            "result": None,
            "artifacts": {},
            "runtime_port_adapter_outputs": output_descriptors,
        }
        write_json(result_path, result_payload)
        return result_payload
    result_without_artifacts, artifacts = collect_artifacts(raw_result, artifacts_dir)
    result_payload = {
        "result": to_jsonable(result_without_artifacts),
        "artifacts": artifacts,
    }
    write_json(result_path, result_payload)
    return result_payload


def execute(
    *,
    module_path: Path,
    module_name: str,
    entrypoint: str,
    input_path: Path,
    result_path: Path,
    artifacts_dir: Path,
    env_spec_path: Path | None = None,
) -> dict[str, Any]:
    """Execute one functional node without access to the conductor package."""

    with _spl_free_import_boundary():
        return _execute(
            module_path=module_path,
            module_name=module_name,
            entrypoint=entrypoint,
            input_path=input_path,
            result_path=result_path,
            artifacts_dir=artifacts_dir,
            env_spec_path=env_spec_path,
        )


def build_parser() -> argparse.ArgumentParser:
    """Create the runner argument parser."""

    parser = argparse.ArgumentParser(description="Execute one generated SPL function")
    parser.add_argument("--module", required=True, type=Path)
    parser.add_argument("--module-name", required=True)
    parser.add_argument("--entrypoint", required=True)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--artifacts-dir", required=True, type=Path)
    parser.add_argument("--env-spec", default=None, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the SPL-free runner from the command line."""

    args = build_parser().parse_args(argv)
    try:
        execute(
            module_path=args.module,
            module_name=args.module_name,
            entrypoint=args.entrypoint,
            input_path=args.input,
            result_path=args.result,
            artifacts_dir=args.artifacts_dir,
            env_spec_path=args.env_spec,
        )
    except NodeStageError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
