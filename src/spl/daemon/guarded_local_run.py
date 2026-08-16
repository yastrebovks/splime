"""Closed contract for one guarded, local-only Run admission.

The contract belongs to the daemon and is intentionally independent of any
Jupyter or browser package.  It binds the existing canonical Object content
hash and :mod:`spl.daemon.signature` result rather than defining another
signature-hash dialect.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Any, Literal, NoReturn

from spl.daemon.signature import build_signature

GUARDED_LOCAL_RUN_ROUTE = "/runs/local-admissions"
GUARDED_LOCAL_RUN_CAPABILITY = "run.local.create.guarded"
GUARDED_LOCAL_RUN_CAPABILITY_VERSION = 1
GUARDED_LOCAL_RUN_REQUEST_SCHEMA = "spl.daemon.guarded-local-run-admission"
GUARDED_LOCAL_RUN_RECEIPT_SCHEMA = "spl.daemon.guarded-local-run-receipt"
GUARDED_LOCAL_RUN_ERROR_SCHEMA = "spl.daemon.guarded-local-run-error"
GUARDED_LOCAL_RUN_SCHEMA_VERSION = 1

MAX_SAFE_INTEGER = 9_007_199_254_740_991
MAX_TIMEOUT_MS = 604_800_000
MAX_INPUTS = 256
MAX_CONTAINER_ITEMS = 1_000
MAX_ARGUMENT_DEPTH = 20
MAX_ARGUMENT_BYTES = 1_048_576
MAX_BODY_BYTES = MAX_ARGUMENT_BYTES + 65_536

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_UPSTREAM_NAME = re.compile(r"^(?![.]+$)[A-Za-z0-9_.-]{1,160}$")
_INSTANCE_IDENTIFIER = re.compile(r"^[A-Za-z0-9]{16,64}$")
_CONTENT_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")

_REQUEST_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "request_id",
        "daemon_instance_id",
        "daemon_generation",
        "object",
        "content_hash",
        "inputs",
        "output_selector",
        "timeout_ms",
        "retention",
        "target",
        "source",
    }
)
_OBJECT_FIELDS = frozenset(
    {
        "owner_id",
        "library",
        "object_name",
        "object_id",
        "expected_current_version_id",
        "selected_version_id",
        "selected_version",
        "function",
        "origin",
    }
)
_INPUT_FIELDS = frozenset({"name", "value"})
_RECEIPT_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "request_id",
        "accepted_http_status",
        "run_id",
        "object",
        "initial_state",
        "created_at",
        "observed_at",
        "target",
        "source",
    }
)
_RECEIPT_OBJECT_FIELDS = frozenset(
    {
        "owner_id",
        "library",
        "object_name",
        "object_id",
        "current_version_id",
        "version_id",
        "version",
        "function",
        "origin",
        "content_hash",
    }
)


class GuardedLocalRunError(RuntimeError):
    """Bounded reason-coded failure for the guarded admission route."""

    def __init__(
        self,
        code: str,
        status: HTTPStatus,
        *,
        retryable: bool = False,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.status = status
        self.retryable = retryable


@dataclass(frozen=True)
class GuardedObjectReference:
    """Exact local Object facts reviewed by a client."""

    owner_id: str
    library: str
    object_name: str
    object_id: str
    expected_current_version_id: str
    selected_version_id: str
    selected_version: int
    function: str | None
    origin: str


@dataclass(frozen=True)
class GuardedInput:
    """One named JSON argument."""

    name: str
    value: Any


@dataclass(frozen=True)
class GuardedLocalRunAdmission:
    """Parsed and structurally validated guarded admission request."""

    request_id: str
    daemon_instance_id: str
    daemon_generation: int
    object: GuardedObjectReference
    content_hash: str
    inputs: tuple[GuardedInput, ...]
    output_selector: str | None
    timeout_ms: int | None
    retention: str
    target: str
    source: str

    def keyword_arguments(self) -> dict[str, Any]:
        """Return the already duplicate-checked named inputs."""

        return {item.name: item.value for item in self.inputs}


def parse_guarded_local_run_request(raw: bytes) -> GuardedLocalRunAdmission:
    """Parse an exact guarded request without using the legacy body reader."""

    if len(raw) > MAX_BODY_BYTES:
        raise GuardedLocalRunError("body_too_large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise GuardedLocalRunError("malformed_json", HTTPStatus.BAD_REQUEST) from exc
    try:
        document = json.loads(
            text,
            object_pairs_hook=_closed_json_object,
            parse_constant=_reject_json_constant,
        )
    except GuardedLocalRunError:
        raise
    except RecursionError as exc:
        raise GuardedLocalRunError(
            "body_too_deep",
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
        ) from exc
    except (json.JSONDecodeError, UnicodeError, ValueError) as exc:
        raise GuardedLocalRunError("malformed_json", HTTPStatus.BAD_REQUEST) from exc
    if not isinstance(document, dict):
        raise GuardedLocalRunError("request_invalid", HTTPStatus.BAD_REQUEST)
    _require_exact_fields(document, _REQUEST_FIELDS)
    if _json_depth(document["inputs"]) > MAX_ARGUMENT_DEPTH:
        raise GuardedLocalRunError("body_too_deep", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    if document["schema"] != GUARDED_LOCAL_RUN_REQUEST_SCHEMA:
        raise GuardedLocalRunError("request_invalid", HTTPStatus.BAD_REQUEST)
    _require_exact_integer(
        document["schema_version"],
        GUARDED_LOCAL_RUN_SCHEMA_VERSION,
    )
    request_id = _require_pattern(document["request_id"], _SAFE_IDENTIFIER)
    daemon_instance_id = _require_pattern(
        document["daemon_instance_id"],
        _INSTANCE_IDENTIFIER,
    )
    daemon_generation = _require_positive_safe_integer(document["daemon_generation"])

    raw_object = _require_mapping(document["object"])
    _require_exact_fields(raw_object, _OBJECT_FIELDS)
    function = raw_object["function"]
    if function is not None:
        function = _require_pattern(function, _UPSTREAM_NAME)
    origin = raw_object["origin"]
    target = document["target"]
    source = document["source"]
    if origin != "local" or target != "local" or source != "local":
        raise GuardedLocalRunError("non_local_request", HTTPStatus.BAD_REQUEST)
    object_reference = GuardedObjectReference(
        owner_id=_require_pattern(raw_object["owner_id"], _UPSTREAM_NAME),
        library=_require_pattern(raw_object["library"], _UPSTREAM_NAME),
        object_name=_require_pattern(raw_object["object_name"], _UPSTREAM_NAME),
        object_id=_require_pattern(raw_object["object_id"], _UPSTREAM_NAME),
        expected_current_version_id=_require_pattern(
            raw_object["expected_current_version_id"],
            _UPSTREAM_NAME,
        ),
        selected_version_id=_require_pattern(
            raw_object["selected_version_id"],
            _UPSTREAM_NAME,
        ),
        selected_version=_require_positive_safe_integer(raw_object["selected_version"]),
        function=function,
        origin=origin,
    )
    if object_reference.selected_version_id != object_reference.expected_current_version_id:
        raise GuardedLocalRunError(
            "selected_version_not_current",
            HTTPStatus.PRECONDITION_FAILED,
        )

    content_hash = _require_pattern(document["content_hash"], _CONTENT_HASH)
    inputs = _parse_inputs(document["inputs"])
    if argument_budget_bytes(document["inputs"]) > MAX_ARGUMENT_BYTES:
        raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)

    output_selector = document["output_selector"]
    if output_selector is not None:
        output_selector = _require_pattern(output_selector, _UPSTREAM_NAME)
    timeout_ms = document["timeout_ms"]
    if timeout_ms is not None:
        if (
            isinstance(timeout_ms, bool)
            or not isinstance(timeout_ms, int)
            or timeout_ms < 0
            or timeout_ms > MAX_TIMEOUT_MS
        ):
            raise GuardedLocalRunError("request_invalid", HTTPStatus.BAD_REQUEST)
    retention = document["retention"]
    if retention not in {"keep", "on_failure", "discard"}:
        raise GuardedLocalRunError("request_invalid", HTTPStatus.BAD_REQUEST)

    return GuardedLocalRunAdmission(
        request_id=request_id,
        daemon_instance_id=daemon_instance_id,
        daemon_generation=daemon_generation,
        object=object_reference,
        content_hash=content_hash,
        inputs=inputs,
        output_selector=output_selector,
        timeout_ms=timeout_ms,
        retention=retention,
        target=target,
        source=source,
    )


def guarded_admission_contains_value(
    admission: GuardedLocalRunAdmission,
    forbidden: str,
) -> bool:
    """Return whether a known secret occurs anywhere in the parsed request."""

    if not forbidden:
        return False

    strings = (
        admission.request_id,
        admission.daemon_instance_id,
        admission.object.owner_id,
        admission.object.library,
        admission.object.object_name,
        admission.object.object_id,
        admission.object.expected_current_version_id,
        admission.object.selected_version_id,
        admission.object.function,
        admission.object.origin,
        admission.content_hash,
        admission.output_selector,
        admission.retention,
        admission.target,
        admission.source,
    )
    if any(value is not None and forbidden in value for value in strings):
        return True
    return any(forbidden in item.name or _json_value_contains(item.value, forbidden) for item in admission.inputs)


def validate_signature_arguments(
    admission: GuardedLocalRunAdmission,
    object_record: dict[str, Any],
) -> None:
    """Validate reviewed names against the existing daemon signature builder."""

    try:
        signature = build_signature(object_record)
        authoritative_function = str(object_record["entrypoint"]) if signature.get("kind") == "function" else None
        if admission.object.function != authoritative_function:
            raise GuardedLocalRunError(
                "function_mismatch",
                HTTPStatus.PRECONDITION_FAILED,
            )
        signature_inputs = {
            str(item["name"]): item
            for item in signature["inputs"]
            if isinstance(item, dict) and isinstance(item.get("name"), str)
        }
        if len(signature_inputs) != len(signature["inputs"]):
            raise ValueError
        request_names = {item.name for item in admission.inputs}
        required_names = {name for name, item in signature_inputs.items() if item.get("required") is True}
        if not request_names <= set(signature_inputs) or not required_names <= request_names:
            raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)

        selectors = {
            str(item["selector"])
            for item in signature["outputs"]
            if isinstance(item, dict) and item.get("selector") is not None
        }
        if admission.output_selector is not None and admission.output_selector not in selectors:
            raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
        if admission.output_selector is None and selectors and len(selectors) == 1:
            # A null selector retains the existing daemon default behavior.
            pass
    except GuardedLocalRunError:
        raise
    except Exception as exc:
        raise GuardedLocalRunError(
            "function_mismatch",
            HTTPStatus.PRECONDITION_FAILED,
        ) from exc


def build_guarded_local_run_receipt(
    admission: GuardedLocalRunAdmission,
    *,
    object_record: dict[str, Any],
    run_state: dict[str, Any],
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Build and revalidate the closed request-bound 202 receipt."""

    function = str(object_record["entrypoint"]) if str(object_record.get("kind")) == "function" else None
    created_at = canonical_utc_timestamp(str(run_state["created_at"]))
    observed = canonical_utc_timestamp(observed_at or utc_now_milliseconds())
    if datetime.fromisoformat(observed.replace("Z", "+00:00")) < datetime.fromisoformat(
        created_at.replace("Z", "+00:00")
    ):
        observed = created_at
    receipt = {
        "schema": GUARDED_LOCAL_RUN_RECEIPT_SCHEMA,
        "schema_version": GUARDED_LOCAL_RUN_SCHEMA_VERSION,
        "request_id": admission.request_id,
        "accepted_http_status": int(HTTPStatus.ACCEPTED),
        "run_id": str(run_state["id"]),
        "object": {
            "owner_id": str(object_record["owner_id"]),
            "library": str(object_record["library"]),
            "object_name": str(object_record["name"]),
            "object_id": str(object_record["id"]),
            "current_version_id": str(object_record["current_version_id"]),
            "version_id": str(object_record["version_id"]),
            "version": int(object_record["version"]),
            "function": function,
            "origin": str(object_record["origin"]),
            "content_hash": f"sha256:{object_record['content_hash']}",
        },
        "initial_state": str(run_state["status"]),
        "created_at": created_at,
        "observed_at": observed,
        "target": "local",
        "source": "local",
    }
    validate_guarded_local_run_receipt(receipt, admission=admission)
    return receipt


def validate_guarded_local_run_receipt(
    receipt: Any,
    *,
    admission: GuardedLocalRunAdmission | None = None,
) -> None:
    """Reject malformed or falsely bound guarded receipts."""

    document = _require_mapping(receipt)
    _require_exact_fields(document, _RECEIPT_FIELDS)
    if document["schema"] != GUARDED_LOCAL_RUN_RECEIPT_SCHEMA:
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
    _require_exact_integer(document["schema_version"], GUARDED_LOCAL_RUN_SCHEMA_VERSION)
    if (
        isinstance(document["accepted_http_status"], bool)
        or not isinstance(document["accepted_http_status"], int)
        or document["accepted_http_status"] != int(HTTPStatus.ACCEPTED)
    ):
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
    _require_pattern(document["request_id"], _SAFE_IDENTIFIER)
    _require_pattern(document["run_id"], _UPSTREAM_NAME)
    if document["initial_state"] != "queued":
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
    created_at = canonical_utc_timestamp(document["created_at"])
    observed_at = canonical_utc_timestamp(document["observed_at"])
    if document["created_at"] != created_at or document["observed_at"] != observed_at:
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
    if datetime.fromisoformat(observed_at.replace("Z", "+00:00")) < datetime.fromisoformat(
        created_at.replace("Z", "+00:00")
    ):
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
    if document["target"] != "local" or document["source"] != "local":
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
    object_identity = _require_mapping(document["object"])
    _require_exact_fields(object_identity, _RECEIPT_OBJECT_FIELDS)
    for field in (
        "owner_id",
        "library",
        "object_name",
        "object_id",
        "current_version_id",
        "version_id",
    ):
        _require_pattern(object_identity[field], _UPSTREAM_NAME)
    _require_positive_safe_integer(object_identity["version"])
    function = object_identity["function"]
    if function is not None:
        _require_pattern(function, _UPSTREAM_NAME)
    if object_identity["origin"] != "local":
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
    _require_pattern(object_identity["content_hash"], _CONTENT_HASH)

    if admission is not None:
        expected = admission.object
        bindings = {
            "request_id": admission.request_id,
            "owner_id": expected.owner_id,
            "library": expected.library,
            "object_name": expected.object_name,
            "object_id": expected.object_id,
            "current_version_id": expected.expected_current_version_id,
            "version_id": expected.selected_version_id,
            "version": expected.selected_version,
            "function": expected.function,
            "origin": expected.origin,
            "content_hash": admission.content_hash,
        }
        if document["request_id"] != bindings.pop("request_id"):
            raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
        if any(object_identity[key] != value for key, value in bindings.items()):
            raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)


def guarded_local_run_error_document(error: GuardedLocalRunError) -> dict[str, Any]:
    """Return the bounded route-specific error shape."""

    return {
        "schema": GUARDED_LOCAL_RUN_ERROR_SCHEMA,
        "schema_version": GUARDED_LOCAL_RUN_SCHEMA_VERSION,
        "code": error.code,
        "retryable": error.retryable,
        "observed_at": utc_now_milliseconds(),
    }


def retention_to_keep(retention: str) -> bool | Literal["on_failure"]:
    """Translate the closed client-neutral vocabulary to the legacy store."""

    if retention == "keep":
        return True
    if retention == "on_failure":
        return "on_failure"
    if retention == "discard":
        return False
    raise ValueError("retention was not validated")


def timeout_ms_to_seconds(timeout_ms: int | None) -> float | None:
    """Translate exact milliseconds to the existing daemon seconds input."""

    return None if timeout_ms is None else timeout_ms / 1000


def utc_now_milliseconds() -> str:
    """Return canonical UTC milliseconds for the closed wire contract."""

    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_utc_timestamp(value: Any) -> str:
    """Normalize an aware upstream timestamp to canonical UTC milliseconds."""

    if not isinstance(value, str) or not value:
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE) from exc
    if parsed.tzinfo is None:
        raise GuardedLocalRunError("admission_failed", HTTPStatus.SERVICE_UNAVAILABLE)
    utc = parsed.astimezone(UTC).replace(
        microsecond=(parsed.astimezone(UTC).microsecond // 1000) * 1000,
    )
    return utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def argument_budget_bytes(value: Any) -> int:
    """Measure the frozen UTF-8 JSON structural argument budget."""

    def measure(item: Any) -> int:
        if item is None:
            return 4
        if item is True:
            return 4
        if item is False:
            return 5
        if isinstance(item, int):
            return len(str(item).encode("ascii"))
        if isinstance(item, float):
            if not math.isfinite(item):
                raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
            if item.is_integer():
                return len(str(int(item)).encode("ascii"))
            return 32
        if isinstance(item, str):
            return len(
                json.dumps(
                    _normalize_unicode_scalars(item),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
            )
        if isinstance(item, list):
            return 2 + max(0, len(item) - 1) + sum(measure(child) for child in item)
        if isinstance(item, dict):
            return 2 + max(0, len(item) - 1) + sum(measure(key) + 1 + measure(child) for key, child in item.items())
        raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)

    return measure(value)


def _parse_inputs(value: Any) -> tuple[GuardedInput, ...]:
    if not isinstance(value, list) or len(value) > MAX_INPUTS:
        raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
    result: list[GuardedInput] = []
    seen: set[str] = set()
    for raw_item in value:
        item = _require_mapping(raw_item, code="arguments_invalid")
        _require_exact_fields(item, _INPUT_FIELDS)
        name = _require_pattern(item["name"], _UPSTREAM_NAME, code="arguments_invalid")
        if name in seen:
            raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
        seen.add(name)
        # The frozen profile measures from the inputs array: input item and
        # value therefore occupy depths one and two respectively.
        _validate_json_argument(item["value"], depth=2)
        result.append(GuardedInput(name=name, value=item["value"]))
    return tuple(result)


def _validate_json_argument(value: Any, *, depth: int) -> None:
    if depth > MAX_ARGUMENT_DEPTH:
        raise GuardedLocalRunError("body_too_deep", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if not -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER:
            raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
        return
    if isinstance(value, float):
        if not math.isfinite(value) or (value.is_integer() and not -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER):
            raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
        return
    if isinstance(value, str):
        _normalize_unicode_scalars(value)
        return
    if isinstance(value, list):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
        for child in value:
            _validate_json_argument(child, depth=depth + 1)
        return
    if isinstance(value, dict):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
        for key, child in value.items():
            if not isinstance(key, str) or not 1 <= len(key) <= 160:
                raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
            _normalize_unicode_scalars(key)
            _validate_json_argument(child, depth=depth + 1)
        return
    raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)


def _json_value_contains(value: Any, forbidden: str) -> bool:
    if isinstance(value, str):
        return forbidden in value
    if isinstance(value, list):
        return any(_json_value_contains(child, forbidden) for child in value)
    if isinstance(value, dict):
        return any(forbidden in key or _json_value_contains(child, forbidden) for key, child in value.items())
    return False


def _normalize_unicode_scalars(value: str) -> str:
    normalized: list[str] = []
    index = 0
    while index < len(value):
        code = ord(value[index])
        if 0xD800 <= code <= 0xDBFF:
            if index + 1 >= len(value):
                raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
            low = ord(value[index + 1])
            if not 0xDC00 <= low <= 0xDFFF:
                raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
            normalized.append(chr(0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00)))
            index += 2
            continue
        if 0xDC00 <= code <= 0xDFFF:
            raise GuardedLocalRunError("arguments_invalid", HTTPStatus.BAD_REQUEST)
        normalized.append(value[index])
        index += 1
    return "".join(normalized)


def _closed_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise GuardedLocalRunError("malformed_json", HTTPStatus.BAD_REQUEST)
        result[key] = value
    return result


def _reject_json_constant(_: str) -> NoReturn:
    raise GuardedLocalRunError("malformed_json", HTTPStatus.BAD_REQUEST)


def _json_depth(value: Any) -> int:
    maximum = 0
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        maximum = max(maximum, depth)
        if isinstance(item, dict):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
    return maximum


def _require_mapping(value: Any, *, code: str = "request_invalid") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GuardedLocalRunError(code, HTTPStatus.BAD_REQUEST)
    return value


def _require_exact_fields(value: dict[str, Any], expected: frozenset[str]) -> None:
    if set(value) - expected:
        raise GuardedLocalRunError("unknown_field", HTTPStatus.BAD_REQUEST)
    if set(value) != expected:
        raise GuardedLocalRunError("request_invalid", HTTPStatus.BAD_REQUEST)


def _require_pattern(
    value: Any,
    pattern: re.Pattern[str],
    *,
    code: str = "request_invalid",
) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise GuardedLocalRunError(code, HTTPStatus.BAD_REQUEST)
    return value


def _require_positive_safe_integer(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > MAX_SAFE_INTEGER:
        raise GuardedLocalRunError("request_invalid", HTTPStatus.BAD_REQUEST)
    return value


def _require_exact_integer(value: Any, expected: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value != expected:
        raise GuardedLocalRunError("request_invalid", HTTPStatus.BAD_REQUEST)


__all__ = [
    "GUARDED_LOCAL_RUN_CAPABILITY",
    "GUARDED_LOCAL_RUN_CAPABILITY_VERSION",
    "GUARDED_LOCAL_RUN_ERROR_SCHEMA",
    "GUARDED_LOCAL_RUN_RECEIPT_SCHEMA",
    "GUARDED_LOCAL_RUN_REQUEST_SCHEMA",
    "GUARDED_LOCAL_RUN_ROUTE",
    "GuardedLocalRunAdmission",
    "GuardedLocalRunError",
    "GuardedObjectReference",
    "MAX_ARGUMENT_BYTES",
    "MAX_ARGUMENT_DEPTH",
    "MAX_BODY_BYTES",
    "argument_budget_bytes",
    "build_guarded_local_run_receipt",
    "canonical_utc_timestamp",
    "guarded_admission_contains_value",
    "guarded_local_run_error_document",
    "parse_guarded_local_run_request",
    "retention_to_keep",
    "timeout_ms_to_seconds",
    "utc_now_milliseconds",
    "validate_guarded_local_run_receipt",
    "validate_signature_arguments",
]
