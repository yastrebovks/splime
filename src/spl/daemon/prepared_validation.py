"""Closed, non-mutating preparation and validation for prepared Object IR.

The validation contract accepts only a PreparedObject manifest and canonical
IR.  The additive preparation contract accepts explicit generated Python as
data, parses it statically, validates its exact canonical IR and serializes the
validated legacy registry document.  Neither path imports generated source,
builds an environment, registers an Object, executes a Run or writes daemon
state.
"""

from __future__ import annotations

import ast
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any, Literal, Mapping, cast

from spl.core.entities.adapter import DAdapter
from spl.core.entities.distribution import DDistribution
from spl.core.entities.function import DFunction
from spl.core.entities.module import DImport, DImportFrom
from spl.core.entities.node import (
    DFormattedOutputRef,
    DNodeInputRef,
    DNodeOutputRef,
    InputPort,
    OutputPort,
)
from spl.core.entities.node_function import DNodeFunction
from spl.core.entities.pipeline import DPipeline
from spl.core.entities.scalar import DScalar
from spl.core.source_preparation import (
    CANONICAL_IR_SCHEMA,
    PrepareSourceContractError,
    domain_hash,
    prepare_source,
    prepared_manifest,
    validate_prepared_manifest_and_ir,
)
from spl.daemon.metadata import ObjectIR, extract_object_ir_metadata, validate_object_ir

PREPARED_VALIDATION_CAPABILITY = "object.prepared.validate"
PREPARED_VALIDATION_CAPABILITY_VERSION = 1
PREPARED_VALIDATION_ROUTE = "/objects/validate"
PREPARED_VALIDATION_REQUEST_SCHEMA = "splime.object-validation-request/v1"
PREPARED_VALIDATION_RESULT_SCHEMA = "splime.object-validation-result/v1"
PREPARED_VALIDATION_ERROR_SCHEMA = "spl.daemon.prepared-validation-error"
PREPARED_VALIDATION_SCHEMA_VERSION = 1
PREPARED_VALIDATOR_VERSION = "1.0.0"
PREPARED_VALIDATION_HASH_DOMAIN = "splime.validation/v1"
PREPARED_PUBLICATION_CAPABILITY = "object.source.prepare"
PREPARED_PUBLICATION_CAPABILITY_VERSION = 1
PREPARED_PUBLICATION_ROUTE = "/objects/prepare"
PREPARED_PUBLICATION_REQUEST_SCHEMA = "splime.object-preparation-request/v1"
PREPARED_PUBLICATION_RESULT_SCHEMA = "splime.object-preparation-result/v1"

MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_PREPARATION_REQUEST_BYTES = 3 * 1024 * 1024
MAX_JSON_DEPTH = 40
MAX_JSON_NODES = 220_000
MAX_TOTAL_STRING_CHARS = 2 * 1024 * 1024
MAX_STRING_CHARS = 1024 * 1024
VALIDATION_TIMEOUT_SECONDS = 5.0

ValidationStatus = Literal[
    "valid",
    "invalid",
    "stale",
    "incompatible",
    "reference_unavailable",
]

_REQUEST_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "prepared_manifest",
        "canonical_ir",
        "reference_resolution",
    }
)
_REFERENCE_RESOLUTION_KEYS = frozenset({"mode", "max_age_seconds"})
_PREPARATION_REQUEST_KEYS = frozenset({"schema", "schema_version", "prepare_request"})
_RESULT_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "status",
        "valid",
        "publish_ready",
        "prepared_hash",
        "ir_hash",
        "validation_hash",
        "capability",
        "validator",
        "effective_environment",
        "effective_runtime",
        "reference_snapshot_hash",
        "checks",
        "diagnostics",
        "validated_at",
    }
)
_RESULT_DIAGNOSTIC_CODES = frozenset(
    {
        "reference_resolution_unsupported",
        "ir_semantics_invalid",
        "base_reference_unavailable",
        "base_reference_stale",
        "environment_unavailable",
        "runtime_unsupported",
    }
)
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_UTC_MILLISECONDS_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_HASH_ERROR_SUFFIXES = (
    ".prepared_hash",
    ".ir_hash",
    ".dependencies_hash",
    ".entrypoint_binding",
    ".environment_request_binding",
    ".runtime_request_binding",
    ".base_binding",
)


class PreparedValidationError(RuntimeError):
    """A stable, source-free request or service failure."""

    def __init__(
        self,
        code: str,
        status: HTTPStatus,
        *,
        retryable: bool = False,
    ) -> None:
        self.code = code
        self.status = status
        self.retryable = retryable
        super().__init__(code)


@dataclass(frozen=True)
class PreparedValidationRequest:
    """The parsed, closed v1 daemon request."""

    prepared_manifest: Mapping[str, Any]
    canonical_ir: Mapping[str, Any]
    reference_mode: Literal["none", "cache_only", "online_read"]
    reference_max_age_seconds: int | None


def parse_prepared_validation_request(raw: bytes) -> PreparedValidationRequest:
    """Parse bounded UTF-8 JSON with duplicate-key and non-finite rejection."""

    if not raw:
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
    if len(raw) > MAX_REQUEST_BYTES:
        raise PreparedValidationError("body_too_large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    try:
        text = raw.decode("utf-8", errors="strict")
        value = json.loads(
            text,
            object_pairs_hook=_closed_json_object,
            parse_constant=lambda _value: _reject_json_constant(),
        )
    except PreparedValidationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST) from exc
    _validate_json_budget(value)
    document = _require_mapping(value)
    if set(document) != _REQUEST_KEYS:
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
    if (
        document["schema"] != PREPARED_VALIDATION_REQUEST_SCHEMA
        or type(document["schema_version"]) is not int
        or document["schema_version"] != PREPARED_VALIDATION_SCHEMA_VERSION
    ):
        raise PreparedValidationError("request_incompatible", HTTPStatus.CONFLICT)
    manifest = _require_mapping(document["prepared_manifest"])
    canonical_ir = _require_mapping(document["canonical_ir"])
    resolution = _require_mapping(document["reference_resolution"])
    if set(resolution) != _REFERENCE_RESOLUTION_KEYS:
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
    mode = resolution["mode"]
    if mode not in {"none", "cache_only", "online_read"}:
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
    max_age = resolution["max_age_seconds"]
    if mode in {"none", "online_read"}:
        if max_age is not None:
            raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
    elif type(max_age) is not int or not 1 <= max_age <= 86_400:
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
    return PreparedValidationRequest(
        prepared_manifest=manifest,
        canonical_ir=canonical_ir,
        reference_mode=cast(Literal["none", "cache_only", "online_read"], mode),
        reference_max_age_seconds=cast(int | None, max_age),
    )


def parse_prepared_publication_request(raw: bytes) -> Mapping[str, Any]:
    """Parse one bounded source-preparation document without interpreting source."""

    if not raw:
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
    if len(raw) > MAX_PREPARATION_REQUEST_BYTES:
        raise PreparedValidationError("body_too_large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    try:
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_closed_json_object,
            parse_constant=lambda _value: _reject_json_constant(),
        )
    except PreparedValidationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST) from exc
    _validate_json_budget(value)
    document = _require_mapping(value)
    if set(document) != _PREPARATION_REQUEST_KEYS:
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
    if (
        document["schema"] != PREPARED_PUBLICATION_REQUEST_SCHEMA
        or type(document["schema_version"]) is not int
        or document["schema_version"] != PREPARED_VALIDATION_SCHEMA_VERSION
    ):
        raise PreparedValidationError("request_incompatible", HTTPStatus.CONFLICT)
    return _require_mapping(document["prepare_request"])


def prepare_validated_publication(
    store: Any,
    request: Mapping[str, Any],
) -> dict[str, Any]:
    """Prepare, validate and serialize without registering or executing an Object."""

    try:
        preparation = prepare_source(request)
    except PrepareSourceContractError as exc:
        raise PreparedValidationError(
            "request_incompatible" if "incompatible" in exc.code else "request_invalid",
            HTTPStatus.CONFLICT if "incompatible" in exc.code else HTTPStatus.BAD_REQUEST,
        ) from exc
    diagnostics = [
        str(item.get("code", "prepare.invalid"))
        for item in preparation.get("diagnostics", [])
        if isinstance(item, Mapping)
    ]
    prepared = preparation.get("prepared")
    if preparation.get("status") != "ready" or not isinstance(prepared, Mapping):
        return _prepared_publication_result(
            status="invalid",
            diagnostic_codes=diagnostics,
        )
    manifest = prepared_manifest(prepared)
    canonical_ir = _require_mapping(prepared["canonical_ir"])
    validation = validate_prepared_request(
        store,
        PreparedValidationRequest(
            prepared_manifest=manifest,
            canonical_ir=canonical_ir,
            reference_mode="none",
            reference_max_age_seconds=None,
        ),
    )
    if validation["status"] != "valid" or validation["validation_hash"] is None:
        validation_codes = [
            str(item["code"])
            for item in validation["diagnostics"]
            if isinstance(item, Mapping) and isinstance(item.get("code"), str)
        ]
        return _prepared_publication_result(
            status=cast(ValidationStatus, validation["status"]),
            prepared_hash=cast(str, prepared["prepared_hash"]),
            validation_hash=cast(str | None, validation["validation_hash"]),
            diagnostic_codes=[*diagnostics, *validation_codes],
        )
    try:
        yaml_text, entrypoint, runtime_config = serialize_validated_prepared_object(
            manifest,
            canonical_ir,
        )
    except (MemoryError, RecursionError, TypeError, ValueError) as exc:
        raise PreparedValidationError(
            "serialization_unavailable",
            HTTPStatus.UNPROCESSABLE_ENTITY,
        ) from exc
    return _prepared_publication_result(
        status="ready",
        prepared_hash=cast(str, prepared["prepared_hash"]),
        validation_hash=cast(str, validation["validation_hash"]),
        diagnostic_codes=diagnostics,
        yaml_text=yaml_text,
        entrypoint=entrypoint,
        runtime_config=runtime_config,
    )


def _prepared_publication_result(
    *,
    status: str,
    prepared_hash: str | None = None,
    validation_hash: str | None = None,
    diagnostic_codes: list[str],
    yaml_text: str | None = None,
    entrypoint: str | None = None,
    runtime_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema": PREPARED_PUBLICATION_RESULT_SCHEMA,
        "schema_version": PREPARED_VALIDATION_SCHEMA_VERSION,
        "status": status,
        "prepared_hash": prepared_hash,
        "validation_hash": validation_hash,
        "diagnostic_codes": diagnostic_codes[:64],
        "yaml": yaml_text,
        "entrypoint": entrypoint,
        "runtime_config": runtime_config,
    }


def prepared_validation_contains_value(
    request: PreparedValidationRequest,
    forbidden: str,
) -> bool:
    """Return whether a known secret occurs in any parsed request string."""

    return prepared_publication_contains_value(
        {
            "prepared_manifest": request.prepared_manifest,
            "canonical_ir": request.canonical_ir,
        },
        forbidden,
    )


def prepared_publication_contains_value(
    request: Mapping[str, Any],
    forbidden: str,
) -> bool:
    """Return whether a known secret occurs in one source-preparation request."""

    if not forbidden:
        return False
    stack: list[Any] = [request]
    while stack:
        value = stack.pop()
        if isinstance(value, str):
            if forbidden in value:
                return True
        elif isinstance(value, Mapping):
            stack.extend(value.keys())
            stack.extend(value.values())
        elif isinstance(value, list):
            stack.extend(value)
    return False


def validate_prepared_request(
    store: Any,
    request: PreparedValidationRequest,
    *,
    validated_at: str | None = None,
) -> dict[str, Any]:
    """Validate one prepared manifest without mutation or ambient resolution."""

    manifest = request.prepared_manifest
    canonical_ir = request.canonical_ir
    try:
        validate_prepared_manifest_and_ir(manifest, canonical_ir)
    except PrepareSourceContractError as exc:
        if exc.code.endswith(_HASH_ERROR_SUFFIXES):
            raise PreparedValidationError(
                "prepared_binding_mismatch",
                HTTPStatus.UNPROCESSABLE_ENTITY,
            ) from exc
        if "incompatible" in exc.code:
            raise PreparedValidationError(
                "prepared_contract_incompatible",
                HTTPStatus.CONFLICT,
            ) from exc
        raise PreparedValidationError("prepared_contract_invalid", HTTPStatus.BAD_REQUEST) from exc
    except (RecursionError, TypeError, ValueError) as exc:
        raise PreparedValidationError("prepared_contract_invalid", HTTPStatus.BAD_REQUEST) from exc

    prepared_hash = cast(str, manifest["prepared_hash"])
    ir_hash = cast(str, manifest["ir_hash"])
    if request.reference_mode != "none":
        return _result(
            status="incompatible",
            prepared_hash=prepared_hash,
            ir_hash=ir_hash,
            environment=_environment_not_evaluated(manifest),
            runtime=_runtime_not_evaluated(manifest),
            base_state="not_evaluated",
            ir_state="not_evaluated",
            reference_state="unsupported",
            diagnostic_code="reference_resolution_unsupported",
            validated_at=validated_at,
        )

    try:
        object_ir = _object_ir_from_canonical(canonical_ir)
        validate_object_ir(object_ir, syntax_mode="static_ast")
        # Metadata extraction repeats the pure semantic validator and proves
        # that the current daemon can project the exact IR shape it accepts.
        extract_object_ir_metadata(object_ir, syntax_mode="static_ast")
    except (MemoryError, RecursionError, TypeError, ValueError):
        return _result(
            status="invalid",
            prepared_hash=prepared_hash,
            ir_hash=ir_hash,
            environment=_environment_not_evaluated(manifest),
            runtime=_runtime_not_evaluated(manifest),
            base_state="not_evaluated",
            ir_state="invalid",
            reference_state="not_requested",
            diagnostic_code="ir_semantics_invalid",
            validated_at=validated_at,
        )

    base = cast(Mapping[str, Any] | None, manifest["base"])
    environment_request = cast(Mapping[str, Any] | None, manifest["environment_request"])
    facts = store.prepared_validation_facts(
        base=dict(base) if base is not None else None,
        environment_ref=(cast(str, environment_request["ref"]) if environment_request is not None else None),
    )
    base_state = _base_state(base, facts.get("base"))
    environment = _environment_state(environment_request, facts.get("environment"))
    runtime = _runtime_state(cast(Mapping[str, Any] | None, manifest["runtime_request"]))

    status: ValidationStatus = "valid"
    diagnostic_code: str | None = None
    if base_state == "unavailable":
        status = "reference_unavailable"
        diagnostic_code = "base_reference_unavailable"
    elif base_state == "stale":
        status = "stale"
        diagnostic_code = "base_reference_stale"
    elif environment["state"] == "unavailable":
        status = "reference_unavailable"
        diagnostic_code = "environment_unavailable"
    elif runtime["state"] == "unsupported":
        status = "incompatible"
        diagnostic_code = "runtime_unsupported"

    return _result(
        status=status,
        prepared_hash=prepared_hash,
        ir_hash=ir_hash,
        environment=environment,
        runtime=runtime,
        base_state=base_state,
        ir_state="valid",
        reference_state="not_requested",
        diagnostic_code=diagnostic_code,
        validated_at=validated_at,
    )


def prepared_validation_error_document(
    error: PreparedValidationError,
    *,
    operation: str = "validate_prepared_object",
) -> dict[str, Any]:
    """Return a fixed, closed daemon error without upstream prose."""

    return {
        "schema": PREPARED_VALIDATION_ERROR_SCHEMA,
        "schema_version": PREPARED_VALIDATION_SCHEMA_VERSION,
        "code": error.code,
        "operation": operation,
        "retryable": error.retryable,
    }


def validate_prepared_validation_result(document: Mapping[str, Any]) -> None:
    """Validate the exact source-free daemon result before serialization."""

    if set(document) != _RESULT_KEYS:
        raise ValueError("prepared validation result fields do not match v1")
    if (
        document["schema"] != PREPARED_VALIDATION_RESULT_SCHEMA
        or document["schema_version"] != PREPARED_VALIDATION_SCHEMA_VERSION
    ):
        raise ValueError("prepared validation result schema is not recognized")
    status = document["status"]
    if status not in {"valid", "invalid", "stale", "incompatible", "reference_unavailable"}:
        raise ValueError("prepared validation status is not recognized")
    if type(document["valid"]) is not bool or document["valid"] is not (status == "valid"):
        raise ValueError("prepared validation validity is contradictory")
    if document["publish_ready"] is not False:
        raise ValueError("prepared validation cannot claim publish readiness")
    for field in ("prepared_hash", "ir_hash"):
        if not isinstance(document[field], str) or not _HASH_PATTERN.fullmatch(document[field]):
            raise ValueError("prepared validation hash is invalid")
    validation_hash = document["validation_hash"]
    if validation_hash is not None and (
        not isinstance(validation_hash, str) or not _HASH_PATTERN.fullmatch(validation_hash)
    ):
        raise ValueError("validation_hash is invalid")
    capability = _require_result_mapping(document["capability"])
    if capability != {
        "id": PREPARED_VALIDATION_CAPABILITY,
        "version": PREPARED_VALIDATION_CAPABILITY_VERSION,
    }:
        raise ValueError("prepared validation capability evidence is invalid")
    validator = _require_result_mapping(document["validator"])
    if validator != {"version": PREPARED_VALIDATOR_VERSION, "schema": CANONICAL_IR_SCHEMA}:
        raise ValueError("prepared validator identity is invalid")
    environment = _require_result_mapping(document["effective_environment"])
    if set(environment) != {"ref", "state"} or environment["state"] not in {
        "not_requested",
        "not_evaluated",
        "registered",
        "unavailable",
    }:
        raise ValueError("effective environment is invalid")
    if environment["ref"] is not None and not isinstance(environment["ref"], str):
        raise ValueError("effective environment reference is invalid")
    runtime = _require_result_mapping(document["effective_runtime"])
    if (
        set(runtime) != {"mode", "state"}
        or runtime["mode"] not in {None, "venv", "docker"}
        or runtime["state"] not in {"not_requested", "not_evaluated", "declared_supported", "unsupported"}
    ):
        raise ValueError("effective runtime is invalid")
    if document["reference_snapshot_hash"] is not None:
        raise ValueError("v1 does not resolve reference snapshots")
    checks = _require_result_mapping(document["checks"])
    if set(checks) != {
        "hash_binding",
        "ir_semantics",
        "base",
        "environment",
        "runtime",
        "references",
    }:
        raise ValueError("prepared validation checks are not closed")
    if checks["hash_binding"] != "passed":
        raise ValueError("hash binding check is invalid")
    if checks["ir_semantics"] not in {"valid", "invalid", "not_evaluated"}:
        raise ValueError("IR semantic check is invalid")
    if checks["base"] not in {"not_requested", "not_evaluated", "current", "stale", "unavailable"}:
        raise ValueError("base check is invalid")
    if checks["environment"] != environment["state"] or checks["runtime"] != runtime["state"]:
        raise ValueError("effective facts and checks are contradictory")
    if checks["references"] not in {"not_requested", "unsupported"}:
        raise ValueError("reference check is invalid")
    if (checks["ir_semantics"] == "valid") is (validation_hash is None):
        raise ValueError("validation_hash and IR semantic state are contradictory")
    diagnostics = document["diagnostics"]
    if not isinstance(diagnostics, list) or len(diagnostics) > 1:
        raise ValueError("prepared validation diagnostics are invalid")
    for diagnostic in diagnostics:
        item = _require_result_mapping(diagnostic)
        if (
            set(item) != {"code", "severity"}
            or item["code"] not in _RESULT_DIAGNOSTIC_CODES
            or item["severity"] != "error"
        ):
            raise ValueError("prepared validation diagnostic is invalid")
    timestamp = document["validated_at"]
    if not isinstance(timestamp, str) or not _UTC_MILLISECONDS_PATTERN.fullmatch(timestamp):
        raise ValueError("prepared validation timestamp is invalid")


def _object_ir_from_canonical(canonical_ir: Mapping[str, Any]) -> ObjectIR:
    functions = {
        cast(str, item["name"]): _function_from_canonical(item)
        for item in cast(list[Mapping[str, Any]], canonical_ir["functions"])
    }
    dependency_values: list[Any] = []
    for raw_dependency in cast(list[Mapping[str, Any]], canonical_ir["dependencies"]):
        distribution = DDistribution(
            package=cast(str, raw_dependency["package"]),
            version=cast(str, raw_dependency["version"]),
            modules=tuple(cast(list[str], raw_dependency.get("modules", []))),
        )
        dependency_values.append(distribution)
    for raw_import in cast(list[Mapping[str, Any]], canonical_ir["imports"]):
        if raw_import["kind"] == "module":
            dependency_values.append(
                DImport(
                    module=cast(str, raw_import["module"]),
                    alias=cast(str | None, raw_import["alias"]),
                )
            )
        else:
            dependency_values.append(
                DImportFrom(
                    module=cast(str, raw_import["module"]),
                    target=cast(str, raw_import["target"]),
                    alias=cast(str | None, raw_import["alias"]),
                )
            )

    kind = canonical_ir["kind"]
    entrypoint = cast(str, canonical_ir["name"])
    root: DFunction | DPipeline
    if kind == "function":
        root = functions[entrypoint]
        object_dependencies = [
            *(function for name, function in sorted(functions.items()) if name != entrypoint),
            *dependency_values,
        ]
    else:
        root = _pipeline_from_canonical(canonical_ir, functions)
        object_dependencies = [
            *(function for _, function in sorted(functions.items())),
            *dependency_values,
        ]
    runtime_request = cast(Mapping[str, Any] | None, canonical_ir["runtime_request"])
    runtime_config = {} if runtime_request is None else {"mode": runtime_request["mode"]}
    return ObjectIR(
        documents=((root, tuple(object_dependencies)),),
        entrypoint=entrypoint,
        root=root,
        remote_signatures={},
        runtime_config=runtime_config,
        source=Path("<prepared-object-v1>"),
    )


def serialize_validated_prepared_object(
    prepared_manifest: Mapping[str, Any],
    canonical_ir: Mapping[str, Any],
) -> tuple[str, str, dict[str, Any] | None]:
    """Return an executable SPL/YAML bundle for one exact PreparedObject.

    This is a client-neutral bridge between the parser-only preparation
    contract and the existing daemon registry.  It performs the complete
    manifest/IR and daemon semantic validation again before serializing and
    never imports or executes generated Python source.
    """

    import yaml

    validate_prepared_manifest_and_ir(prepared_manifest, canonical_ir)
    object_ir = _object_ir_from_canonical(canonical_ir)
    validate_object_ir(object_ir, syntax_mode="static_ast")
    extract_object_ir_metadata(object_ir, syntax_mode="static_ast")
    documents = [[root, *dependencies] for root, dependencies in object_ir.documents]
    yaml_text = yaml.dump_all(documents, sort_keys=False, allow_unicode=True)
    if not yaml_text.strip():
        raise ValueError("prepared object serialization is empty")
    runtime_request = cast(Mapping[str, Any] | None, canonical_ir["runtime_request"])
    runtime_config = None if runtime_request is None else {"runtime": {"mode": cast(str, runtime_request["mode"])}}
    return yaml_text, object_ir.entrypoint, runtime_config


def _function_from_canonical(raw: Mapping[str, Any]) -> DFunction:
    return DFunction(
        name=cast(str, raw["name"]),
        body=cast(str, raw["body"]),
        inputs=[
            InputPort(
                name=cast(str, item["name"]),
                typ_=cast(str | None, item["type"]),
                default=(_json_literal_expression(item["default"]) if item["has_default"] is True else None),
            )
            for item in cast(list[Mapping[str, Any]], raw["inputs"])
        ],
        outputs=[
            OutputPort(
                name=cast(str, item["name"]),
                typ_=cast(str | None, item["type"]),
            )
            for item in cast(list[Mapping[str, Any]], raw["outputs"])
        ],
    )


def _pipeline_from_canonical(
    canonical_ir: Mapping[str, Any],
    functions: Mapping[str, DFunction],
) -> DPipeline:
    raw_pipeline = cast(Mapping[str, Any], canonical_ir["pipeline"])
    raw_nodes = cast(list[Mapping[str, Any]], raw_pipeline["nodes"])
    nodes = [DNodeFunction(uuid=cast(str, node["node_id"]), func=cast(str, node["function"])) for node in raw_nodes]
    node_by_id = {cast(str, node["node_id"]): node for node in raw_nodes}
    adapter_by_key = {
        cast(str, adapter["key"]): adapter for adapter in cast(list[Mapping[str, Any]], canonical_ir["adapters"])
    }
    adapters = [
        DAdapter(
            key=key,
            save=cast(str, adapter["save_symbol"]),
            load=cast(str, adapter["load_symbol"]),
        )
        for key, adapter in sorted(adapter_by_key.items())
    ]

    links: list[list[Any]] = []
    bound_targets: set[tuple[str, str]] = set()
    external_bindings: dict[str, list[tuple[str, str]]] = {}
    for edge in cast(list[Mapping[str, Any]], raw_pipeline["edges"]):
        target = cast(Mapping[str, Any], edge["to"])
        target_id = cast(str, target["node_id"])
        target_port = cast(str, target["port"])
        target_key = (target_id, target_port)
        if target_key in bound_targets:
            raise ValueError("prepared pipeline target is bound more than once")
        bound_targets.add(target_key)
        source = cast(Mapping[str, Any], edge["from"])
        if source["kind"] == "input":
            external_name = cast(str, source["name"])
            if external_name != target_port:
                raise ValueError("prepared external input cannot be represented exactly")
            external_bindings.setdefault(external_name, []).append(target_key)
            continue
        target_ref = DNodeInputRef(uuid=target_id, port=target_port)
        if source["kind"] == "literal":
            source_ref: Any = DScalar(source["value"])
        else:
            source_id = cast(str, source["node_id"])
            source_port = cast(str, source["port"])
            adapter_key = edge["adapter"]
            if adapter_key is None:
                source_ref = DNodeOutputRef(uuid=source_id, port=source_port)
            else:
                adapter = adapter_by_key[cast(str, adapter_key)]
                source_ref = DFormattedOutputRef(
                    uuid=source_id,
                    port=source_port,
                    format=cast(str, adapter["format"]),
                )
        links.append([target_ref, source_ref])

    canonical_inputs = {cast(str, item["name"]): item for item in cast(list[Mapping[str, Any]], raw_pipeline["inputs"])}
    if set(canonical_inputs) != set(external_bindings):
        raise ValueError("prepared external input catalog cannot be represented exactly")
    for name, targets in external_bindings.items():
        for node_id, port_name in targets:
            node = node_by_id[node_id]
            port = _port_by_name(cast(list[Mapping[str, Any]], node["inputs"]), port_name)
            if canonical_inputs[name] != port:
                raise ValueError("prepared external input signature does not match its target")

    # An omitted optional input in the canonical plan means the exact default
    # is fixed for this pipeline.  Bind it as a scalar so the legacy IR does
    # not incorrectly advertise an additional external Pipeline input.
    for node in raw_nodes:
        node_id = cast(str, node["node_id"])
        for port in cast(list[Mapping[str, Any]], node["inputs"]):
            key = (node_id, cast(str, port["name"]))
            if key in bound_targets:
                continue
            if port["has_default"] is not True:
                raise ValueError("prepared pipeline has an unbound required input")
            links.append(
                [
                    DNodeInputRef(uuid=node_id, port=cast(str, port["name"])),
                    DScalar(port["default"]),
                ]
            )

    aliases: list[list[str]] = []
    for output in cast(list[Mapping[str, Any]], raw_pipeline["outputs"]):
        node_id = cast(str, output["node_id"])
        node = node_by_id[node_id]
        node_outputs = cast(list[Mapping[str, Any]], node["outputs"])
        if len(node_outputs) != 1 or node_outputs[0]["name"] != output["port"]:
            raise ValueError("prepared pipeline output cannot be represented exactly")
        aliases.append([cast(str, output["name"]), node_id])

    tags = {cast(str, node["node_id"]): {"runtime": cast(str, node["runtime"])} for node in raw_nodes}
    return DPipeline(
        name=cast(str, canonical_ir["name"]),
        nodes=nodes,
        links=links,
        aliases=aliases,
        adapters=adapters,
        tags=tags,
    )


def _port_by_name(
    ports: list[Mapping[str, Any]],
    name: str,
) -> Mapping[str, Any]:
    matches = [port for port in ports if port["name"] == name]
    if len(matches) != 1:
        raise ValueError("prepared pipeline port is unavailable")
    return matches[0]


def _json_literal_expression(value: Any) -> str:
    """Serialize a validated JSON literal as Python syntax without eval."""

    expression = _json_literal_ast(value)
    return ast.unparse(ast.fix_missing_locations(expression))


def _json_literal_ast(value: Any) -> ast.expr:
    if value is None or type(value) in {bool, int, str}:
        return ast.Constant(value=value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("prepared default contains a non-finite number")
        return ast.Constant(value=value)
    if isinstance(value, list):
        return ast.List(elts=[_json_literal_ast(item) for item in value], ctx=ast.Load())
    if isinstance(value, Mapping):
        return ast.Dict(
            keys=[ast.Constant(value=cast(str, key)) for key in value],
            values=[_json_literal_ast(item) for item in value.values()],
        )
    raise ValueError("prepared default is not a JSON literal")


def _base_state(
    request: Mapping[str, Any] | None,
    facts: Mapping[str, Any] | None,
) -> str:
    if request is None:
        return "not_requested"
    if facts is None or facts.get("object_found") is not True or facts.get("version_found") is not True:
        return "unavailable"
    expected_kind = "pipeline" if request["kind"] == "pipeline" else "function"
    expected_hash = cast(str, request["content_hash"])
    if (
        facts.get("owner_id") != request["owner_id"]
        or facts.get("library_id") != request["library_id"]
        or facts.get("object_id") != request["object_id"]
        or facts.get("version_id") != request["version_id"]
        or facts.get("version") != request["version"]
        or facts.get("kind") != expected_kind
        or _prefixed_hash(facts.get("content_hash")) != expected_hash
        or facts.get("current_version_id") != request["version_id"]
        or _prefixed_hash(facts.get("current_content_hash")) != expected_hash
    ):
        return "stale"
    return "current"


def _environment_state(
    request: Mapping[str, Any] | None,
    facts: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if request is None:
        return {"ref": None, "state": "not_requested"}
    ref = cast(str, request["ref"])
    state = "registered" if facts is not None and facts.get("registered") is True else "unavailable"
    return {"ref": ref, "state": state}


def _environment_not_evaluated(manifest: Mapping[str, Any]) -> dict[str, Any]:
    request = cast(Mapping[str, Any] | None, manifest.get("environment_request"))
    return {
        "ref": None if request is None else request.get("ref"),
        "state": "not_evaluated",
    }


def _runtime_state(request: Mapping[str, Any] | None) -> dict[str, Any]:
    if request is None:
        return {"mode": None, "state": "not_requested"}
    mode = request["mode"]
    return {
        "mode": mode,
        "state": "declared_supported" if mode in {"venv", "docker"} else "unsupported",
    }


def _runtime_not_evaluated(manifest: Mapping[str, Any]) -> dict[str, Any]:
    request = cast(Mapping[str, Any] | None, manifest.get("runtime_request"))
    return {
        "mode": None if request is None else request.get("mode"),
        "state": "not_evaluated",
    }


def _prefixed_hash(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return value if value.startswith("sha256:") else f"sha256:{value}"


def _result(
    *,
    status: ValidationStatus,
    prepared_hash: str,
    ir_hash: str,
    environment: Mapping[str, Any],
    runtime: Mapping[str, Any],
    base_state: str,
    ir_state: str,
    reference_state: str,
    diagnostic_code: str | None,
    validated_at: str | None,
) -> dict[str, Any]:
    checks = {
        "hash_binding": "passed",
        "ir_semantics": ir_state,
        "base": base_state,
        "environment": environment["state"],
        "runtime": runtime["state"],
        "references": reference_state,
    }
    validator = {
        "version": PREPARED_VALIDATOR_VERSION,
        "schema": CANONICAL_IR_SCHEMA,
    }
    validation_context = {
        "prepared_hash": prepared_hash,
        "ir_hash": ir_hash,
        "validator": validator,
        "effective_environment": dict(environment),
        "effective_runtime": dict(runtime),
        "reference_snapshot_hash": None,
        "checks": checks,
        "status": status,
    }
    validation_hash = domain_hash(PREPARED_VALIDATION_HASH_DOMAIN, validation_context) if ir_state == "valid" else None
    result = {
        "schema": PREPARED_VALIDATION_RESULT_SCHEMA,
        "schema_version": PREPARED_VALIDATION_SCHEMA_VERSION,
        "status": status,
        "valid": status == "valid",
        "publish_ready": False,
        "prepared_hash": prepared_hash,
        "ir_hash": ir_hash,
        "validation_hash": validation_hash,
        "capability": {
            "id": PREPARED_VALIDATION_CAPABILITY,
            "version": PREPARED_VALIDATION_CAPABILITY_VERSION,
        },
        "validator": validator,
        "effective_environment": dict(environment),
        "effective_runtime": dict(runtime),
        "reference_snapshot_hash": None,
        "checks": checks,
        "diagnostics": ([] if diagnostic_code is None else [{"code": diagnostic_code, "severity": "error"}]),
        "validated_at": validated_at or _utc_now(),
    }
    validate_prepared_validation_result(result)
    return result


def _closed_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
        document[key] = value
    return document


def _reject_json_constant() -> None:
    raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)


def _require_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
    return value


def _require_result_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("prepared validation result field must be an object")
    return value


def _validate_json_budget(value: Any) -> None:
    nodes = 0
    string_chars = 0
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise PreparedValidationError("request_too_complex", HTTPStatus.BAD_REQUEST)
        if item is None or type(item) in {bool, int}:
            continue
        if isinstance(item, float):
            if not math.isfinite(item):
                raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)
            continue
        if isinstance(item, str):
            try:
                item.encode("utf-8", errors="strict")
            except UnicodeEncodeError as exc:
                raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST) from exc
            string_chars += len(item)
            if len(item) > MAX_STRING_CHARS or string_chars > MAX_TOTAL_STRING_CHARS:
                raise PreparedValidationError("request_too_complex", HTTPStatus.BAD_REQUEST)
            continue
        if isinstance(item, Mapping):
            stack.extend((key, depth + 1) for key in item)
            stack.extend((child, depth + 1) for child in item.values())
            continue
        if isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
            continue
        raise PreparedValidationError("request_invalid", HTTPStatus.BAD_REQUEST)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


__all__ = [
    "MAX_PREPARATION_REQUEST_BYTES",
    "MAX_REQUEST_BYTES",
    "PREPARED_PUBLICATION_CAPABILITY",
    "PREPARED_PUBLICATION_CAPABILITY_VERSION",
    "PREPARED_PUBLICATION_REQUEST_SCHEMA",
    "PREPARED_PUBLICATION_RESULT_SCHEMA",
    "PREPARED_PUBLICATION_ROUTE",
    "PREPARED_VALIDATION_CAPABILITY",
    "PREPARED_VALIDATION_CAPABILITY_VERSION",
    "PREPARED_VALIDATION_ROUTE",
    "VALIDATION_TIMEOUT_SECONDS",
    "PreparedValidationError",
    "PreparedValidationRequest",
    "parse_prepared_publication_request",
    "parse_prepared_validation_request",
    "prepare_validated_publication",
    "prepared_publication_contains_value",
    "prepared_validation_contains_value",
    "prepared_validation_error_document",
    "serialize_validated_prepared_object",
    "validate_prepared_validation_result",
    "validate_prepared_request",
]
