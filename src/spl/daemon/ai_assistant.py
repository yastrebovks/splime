"""Closed contracts for the additive spl-server AI assistant facade.

The integrated daemon remains a stateless, provider-neutral relay.  It never
owns provider configuration, prompts, credentials, billing state or results.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from copy import deepcopy
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Mapping, cast


AI_ASSISTANT_CAPABILITIES_ROUTE = "/server/ai/assistant/capabilities"
AI_ASSISTANT_ROUTE = "/server/ai/assistant"
AI_ASSISTANT_UPSTREAM_CAPABILITIES_ROUTE = "/ai/assistant/capabilities"
AI_ASSISTANT_UPSTREAM_ROUTE = "/ai/assistant"

CAPABILITIES_SCHEMA = "spl.ai-assistant.capabilities"
REQUEST_SCHEMA = "spl.ai-assistant.request"
RESULT_SCHEMA = "spl.ai-assistant.result"
ERROR_SCHEMA = "spl.ai-assistant.error"
SCHEMA_VERSION = 1

MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
CAPABILITY_TIMEOUT_SECONDS = 5.0
SERVER_TIMEOUT_SECONDS = 60.0

OPERATIONS = frozenset(
    {
        "function_to_node_draft",
        "node_generate",
        "object_call_help",
        "run_failure_explain",
        "object_discover",
        "environment_advise",
    }
)
ERROR_CODES = frozenset(
    {
        "ai_assistant_disabled",
        "provider_unavailable",
        "central_credential_rejected",
        "permission_denied",
        "plan_limit_reached",
        "subscription_inactive",
        "rate_limited",
        "invalid_request",
        "provider_timeout",
        "provider_refused",
        "provider_failed",
        "provider_result_invalid",
        "server_not_connected",
        "server_offline",
        "ai_assistant_protocol_invalid",
    }
)

_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_TIME = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$")
_FORBIDDEN = re.compile(
    r"(?:\bBearer\s+|\bsk-[A-Za-z0-9_-]{8,}|"
    r"(?:api[_-]?key|password|secret|credential)\s*[:=]\s*\S+)",
    re.IGNORECASE,
)


class AIAssistantContractError(RuntimeError):
    """Stable daemon-side assistant protocol failure."""

    def __init__(
        self,
        code: str,
        status: HTTPStatus = HTTPStatus.BAD_REQUEST,
        *,
        retryable: bool = False,
        outcome: str = "not_started",
    ) -> None:
        super().__init__(code)
        self.code = code
        self.status = status
        self.retryable = retryable
        self.outcome = outcome


@dataclass(frozen=True, slots=True)
class AIAssistantRequest:
    document: dict[str, Any]


def parse_request(raw: bytes) -> AIAssistantRequest:
    if len(raw) > MAX_REQUEST_BYTES:
        raise AIAssistantContractError("invalid_request", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    try:
        document = _strict_json(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise AIAssistantContractError("invalid_request") from exc
    validate_request(document)
    return AIAssistantRequest(deepcopy(document))


def validate_capabilities(value: Any) -> None:
    document = _object(
        value,
        {
            "schema",
            "schema_version",
            "observed_at",
            "available",
            "reason",
            "provider_display_name",
            "model_policy_label",
            "prompt_policy_version",
            "operations",
            "request_schema",
            "result_schema",
            "max_request_bytes",
            "storage_disclosure",
            "cancellation_disclosure",
            "disclosure_version",
            "policy_hash",
            "billing",
        },
        response=True,
    )
    if document["schema"] != CAPABILITIES_SCHEMA or document["schema_version"] != 1:
        _fail_response()
    _timestamp(document["observed_at"], response=True)
    if type(document["available"]) is not bool:
        _fail_response()
    if document["available"]:
        if document["reason"] is not None:
            _fail_response()
    elif document["reason"] not in {
        "feature_disabled",
        "provider_sdk_unavailable",
        "provider_credential_missing",
        "provider_configuration_invalid",
    }:
        _fail_response()
    for field in (
        "provider_display_name",
        "model_policy_label",
        "prompt_policy_version",
        "storage_disclosure",
        "cancellation_disclosure",
        "disclosure_version",
    ):
        _text(document[field], maximum=500, response=True)
    if document["request_schema"] != REQUEST_SCHEMA or document["result_schema"] != RESULT_SCHEMA:
        _fail_response()
    if type(document["max_request_bytes"]) is not int or not (1 <= document["max_request_bytes"] <= MAX_REQUEST_BYTES):
        _fail_response()
    if not isinstance(document["policy_hash"], str) or not _HASH.fullmatch(document["policy_hash"]):
        _fail_response()
    operations = document["operations"]
    if type(operations) is not list or len(operations) != len(OPERATIONS):
        _fail_response()
    observed: set[str] = set()
    for raw in operations:
        item = _object(raw, {"id", "label", "credit_cost"}, response=True)
        if item["id"] not in OPERATIONS or item["id"] in observed:
            _fail_response()
        observed.add(item["id"])
        _text(item["label"], maximum=240, response=True)
        if item["credit_cost"] != 1:
            _fail_response()
    _validate_billing(document["billing"], response=True)


def validate_request(
    value: Any,
    *,
    current_capabilities: Mapping[str, Any] | None = None,
) -> None:
    document = _object(
        value,
        {
            "schema",
            "schema_version",
            "request_id",
            "operation",
            "instruction",
            "context",
            "consent",
        },
    )
    if document["schema"] != REQUEST_SCHEMA or document["schema_version"] != 1:
        _fail_request()
    _identifier(document["request_id"])
    operation = document["operation"]
    if operation not in OPERATIONS:
        _fail_request()
    if document["instruction"] is not None:
        _text(document["instruction"], maximum=4_000)
    context = _validate_context(document["context"])
    consent = _object(
        document["consent"],
        {"confirmed", "granted_at", "disclosure_version"},
    )
    if consent["confirmed"] is not True:
        _fail_request()
    _timestamp(consent["granted_at"])
    _text(consent["disclosure_version"], maximum=160)
    if current_capabilities is not None and consent["disclosure_version"] != current_capabilities.get(
        "disclosure_version"
    ):
        _fail_request()
    if operation == "function_to_node_draft" and not context["notebook_cells"]:
        _fail_request()
    if operation == "node_generate" and document["instruction"] is None:
        _fail_request()
    if operation == "object_call_help" and context["object"] is None:
        _fail_request()
    if operation == "run_failure_explain" and context["run"] is None:
        _fail_request()
    if operation == "object_discover" and (document["instruction"] is None or not context["catalog"]):
        _fail_request()
    if operation == "environment_advise" and not context["dependencies"]:
        _fail_request()
    if len(_canonical(document)) > MAX_REQUEST_BYTES:
        _fail_request(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)


def validate_result(
    value: Any,
    *,
    request: Mapping[str, Any],
    current_capabilities: Mapping[str, Any] | None = None,
) -> None:
    document = _object(
        value,
        {
            "schema",
            "schema_version",
            "request_id",
            "operation",
            "provider_display_name",
            "model_policy_label",
            "answer",
            "artifacts",
            "references",
            "warnings",
            "next_steps",
            "usage",
            "generated_at",
            "provenance",
            "billing",
        },
        response=True,
    )
    if (
        document["schema"] != RESULT_SCHEMA
        or document["schema_version"] != 1
        or document["request_id"] != request["request_id"]
        or document["operation"] != request["operation"]
    ):
        _fail_response()
    for field, maximum in (
        ("provider_display_name", 200),
        ("model_policy_label", 320),
        ("answer", 12_000),
    ):
        _text(document[field], maximum=maximum, response=True)
    if current_capabilities is not None and (
        document["provider_display_name"] != current_capabilities.get("provider_display_name")
        or document["model_policy_label"] != current_capabilities.get("model_policy_label")
    ):
        _fail_response()
    artifacts = _list(document["artifacts"], 12, response=True)
    for raw in artifacts:
        artifact = _object(raw, {"kind", "title", "content", "language"}, response=True)
        if artifact["kind"] not in {
            "python",
            "snippet",
            "mapping",
            "checklist",
            "recommendation",
        }:
            _fail_response()
        _text(artifact["title"], maximum=240, response=True)
        _text(artifact["content"], maximum=30_000, response=True)
        if artifact["language"] is not None:
            _text(artifact["language"], maximum=40, response=True)
    references = _list(document["references"], 30, response=True)
    allowed = _reference_ids(request["context"])
    for raw in references:
        reference = _object(raw, {"kind", "id", "label"}, response=True)
        if reference["kind"] not in {"cell", "object", "run", "dependency"}:
            _fail_response()
        _text(reference["id"], maximum=200, response=True)
        _text(reference["label"], maximum=240, response=True)
        if (reference["kind"], reference["id"]) not in allowed:
            _fail_response()
    for field in ("warnings", "next_steps"):
        for item in _list(document[field], 20, response=True):
            _text(item, maximum=2_000, response=True)
    usage = _object(
        document["usage"],
        {"input_tokens", "output_tokens", "total_tokens"},
        response=True,
    )
    for value in usage.values():
        if value is not None and (type(value) is not int or not 0 <= value <= 10**9):
            _fail_response()
    _timestamp(document["generated_at"], response=True)
    provenance = _object(
        document["provenance"],
        {"producer", "release_id", "prompt_policy_version"},
        response=True,
    )
    if provenance["producer"] != "spl_server":
        _fail_response()
    _text(provenance["release_id"], maximum=96, response=True)
    _text(provenance["prompt_policy_version"], maximum=160, response=True)
    _validate_billing(document["billing"], response=True)
    if _forbidden(document):
        _fail_response()


def validate_error(value: Any) -> None:
    document = _object(
        value,
        {
            "schema",
            "schema_version",
            "code",
            "safe_message",
            "retryable",
            "outcome",
            "correlation_id",
            "billing",
        },
        response=True,
    )
    if document["schema"] != ERROR_SCHEMA or document["schema_version"] != 1:
        _fail_response()
    if document["code"] not in ERROR_CODES - {
        "server_not_connected",
        "server_offline",
        "ai_assistant_protocol_invalid",
    }:
        _fail_response()
    _text(document["safe_message"], maximum=512, response=True)
    if type(document["retryable"]) is not bool:
        _fail_response()
    if document["outcome"] not in {"not_started", "not_completed", "unknown"}:
        _fail_response()
    _identifier(document["correlation_id"], response=True)
    if document["billing"] is not None:
        _validate_billing(document["billing"], response=True)


def error_document(
    code: str,
    *,
    safe_message: str,
    retryable: bool,
    outcome: str,
    correlation_id: str,
    billing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    document = {
        "schema": ERROR_SCHEMA,
        "schema_version": 1,
        "code": code,
        "safe_message": safe_message,
        "retryable": retryable,
        "outcome": outcome,
        "correlation_id": correlation_id,
        "billing": deepcopy(billing),
    }
    if code in {"server_not_connected", "server_offline", "ai_assistant_protocol_invalid"}:
        _text(safe_message, maximum=512)
        _identifier(correlation_id)
        return document
    validate_error(document)
    return document


def _validate_context(value: Any) -> dict[str, Any]:
    context = _object(
        value,
        {"notebook_cells", "object", "run", "catalog", "dependencies"},
    )
    cells = _list(context["notebook_cells"], 64)
    total = 0
    for raw in cells:
        cell = _object(raw, {"cell_id", "ordinal", "source", "source_hash"})
        _identifier(cell["cell_id"])
        if type(cell["ordinal"]) is not int or not 0 <= cell["ordinal"] <= 100_000:
            _fail_request()
        _text(cell["source"], maximum=256_000)
        total += len(cell["source"].encode("utf-8"))
        if total > 512_000 or not isinstance(cell["source_hash"], str) or not _HASH.fullmatch(cell["source_hash"]):
            _fail_request()
        digest = hashlib.sha256()
        digest.update(b"splime.ide.notebook-cell-source/v1\0")
        digest.update(cell["source"].encode("utf-8"))
        if cell["source_hash"] != f"sha256:{digest.hexdigest()}":
            _fail_request()
    if context["object"] is not None:
        _validate_object(context["object"])
    if context["run"] is not None:
        _validate_run(context["run"])
    for item in _list(context["catalog"], 100):
        _validate_object(item)
    for raw in _list(context["dependencies"], 200):
        item = _object(raw, {"name", "version", "source"})
        _text(item["name"], maximum=160)
        if item["version"] is not None:
            _text(item["version"], maximum=160)
        if item["source"] not in {"notebook_import", "environment", "object"}:
            _fail_request()
    return context


def _validate_object(value: Any) -> None:
    document = _object(
        value,
        {"object_id", "name", "library", "version", "summary", "inputs", "outputs"},
    )
    _identifier(document["object_id"])
    for field in ("name", "library", "version"):
        _text(document[field], maximum=200)
    if document["summary"] is not None:
        _text(document["summary"], maximum=1_000)
    for field in ("inputs", "outputs"):
        for raw in _list(document[field], 64):
            item = _object(raw, {"name", "annotation", "required"})
            _text(item["name"], maximum=160)
            if item["annotation"] is not None:
                _text(item["annotation"], maximum=320)
            if type(item["required"]) is not bool:
                _fail_request()


def _validate_run(value: Any) -> None:
    document = _object(
        value,
        {
            "run_id",
            "status",
            "object_name",
            "started_at",
            "finished_at",
            "failure_code",
            "failure_message",
            "events",
        },
    )
    _identifier(document["run_id"])
    _text(document["status"], maximum=80)
    if document["object_name"] is not None:
        _text(document["object_name"], maximum=200)
    for field in ("started_at", "finished_at"):
        if document[field] is not None:
            _timestamp(document[field])
    for field in ("failure_code", "failure_message"):
        if document[field] is not None:
            _text(document[field], maximum=2_000)
    for item in _list(document["events"], 40):
        _text(item, maximum=1_000)


def _validate_billing(value: Any, *, response: bool) -> None:
    document = _object(
        value,
        {
            "plan",
            "entitlement",
            "period",
            "limit",
            "used",
            "remaining",
            "enforcement_mode",
            "decision",
            "allow",
            "recovery_action",
        },
        response=response,
    )
    if document["plan"] not in {"cloud_free", "team", "scale"}:
        _fail(response)
    if document["entitlement"] != "managed_ai_analysis_credits":
        _fail(response)
    if not isinstance(document["period"], str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}", document["period"]):
        _fail(response)
    for field in ("limit", "remaining"):
        item = document[field]
        if item is not None and (type(item) is not int or item < 0):
            _fail(response)
    if type(document["used"]) is not int or document["used"] < 0:
        _fail(response)
    if document["enforcement_mode"] not in {"shadow", "cohort", "live"}:
        _fail(response)
    if document["decision"] not in {"allow", "hypothetical_deny", "deny"}:
        _fail(response)
    if type(document["allow"]) is not bool:
        _fail(response)
    _text(document["recovery_action"], maximum=80, response=response)


def _reference_ids(context: Mapping[str, Any]) -> set[tuple[str, str]]:
    result = {("cell", str(item["cell_id"])) for item in context["notebook_cells"]}
    if context["object"] is not None:
        result.add(("object", str(context["object"]["object_id"])))
    result.update(("object", str(item["object_id"])) for item in context["catalog"])
    if context["run"] is not None:
        result.add(("run", str(context["run"]["run_id"])))
    result.update(("dependency", str(item["name"])) for item in context["dependencies"])
    return result


def _object(value: Any, fields: set[str], *, response: bool = False) -> dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        _fail(response)
    return cast(dict[str, Any], value)


def _list(value: Any, maximum: int, *, response: bool = False) -> list[Any]:
    if type(value) is not list or len(value) > maximum:
        _fail(response)
    return cast(list[Any], value)


def _identifier(value: Any, *, response: bool = False) -> None:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        _fail(response)


def _timestamp(value: Any, *, response: bool = False) -> None:
    if not isinstance(value, str) or not _TIME.fullmatch(value):
        _fail(response)


def _text(value: Any, *, maximum: int, response: bool = False) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
        or "\x00" in value
        or _FORBIDDEN.search(value)
    ):
        _fail(response)


def _forbidden(value: Any) -> bool:
    if isinstance(value, str):
        return bool(_FORBIDDEN.search(value))
    if isinstance(value, list):
        return any(_forbidden(item) for item in value)
    if isinstance(value, dict):
        return any(_forbidden(item) for item in value.values())
    return False


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise AIAssistantContractError("invalid_request") from exc


def _strict_json(text: str) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result

    def constant(_value: str) -> None:
        raise ValueError("non-finite JSON")

    value = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
    if _nonfinite(value):
        raise ValueError("non-finite JSON")
    return value


def _nonfinite(value: Any) -> bool:
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, list):
        return any(_nonfinite(item) for item in value)
    if isinstance(value, dict):
        return any(_nonfinite(item) for item in value.values())
    return False


def _fail(response: bool) -> None:
    if response:
        _fail_response()
    _fail_request()


def _fail_request(status: HTTPStatus = HTTPStatus.BAD_REQUEST) -> None:
    raise AIAssistantContractError("invalid_request", status)


def _fail_response() -> None:
    raise AIAssistantContractError("ai_assistant_protocol_invalid", HTTPStatus.BAD_GATEWAY)


__all__ = [
    "AI_ASSISTANT_CAPABILITIES_ROUTE",
    "AI_ASSISTANT_ROUTE",
    "AI_ASSISTANT_UPSTREAM_CAPABILITIES_ROUTE",
    "AI_ASSISTANT_UPSTREAM_ROUTE",
    "CAPABILITY_TIMEOUT_SECONDS",
    "MAX_REQUEST_BYTES",
    "MAX_RESPONSE_BYTES",
    "SERVER_TIMEOUT_SECONDS",
    "AIAssistantContractError",
    "AIAssistantRequest",
    "error_document",
    "parse_request",
    "validate_capabilities",
    "validate_error",
    "validate_request",
    "validate_result",
]
