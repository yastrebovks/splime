"""Closed contracts for the additive central AI Preview daemon facade.

The integrated daemon is only a fixed, authenticated relay.  This module
validates the exact provider-neutral documents it is allowed to forward and
receive; it contains no provider SDK, credential lookup, model policy, prompt,
or persistence.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Any, Mapping, NoReturn

AI_PREVIEW_SCHEMA_VERSION = 1
AI_PREVIEW_CAPABILITIES_SCHEMA = "spl.ai-preview.capabilities"
AI_PREVIEW_REQUEST_SCHEMA = "spl.ai-preview.notebook-pipeline-request"
AI_PREVIEW_RESULT_SCHEMA = "spl.ai-preview.notebook-pipeline-result"
AI_PREVIEW_ERROR_SCHEMA = "spl.ai-preview.error"
AI_PREVIEW_OPERATION = "notebook_pipeline_preview"

AI_PREVIEW_CAPABILITIES_ROUTE = "/server/ai/preview/capabilities"
AI_PREVIEW_ROUTE = "/server/ai/preview"

MAX_AI_PREVIEW_REQUEST_BYTES = 4 * 1024 * 1024
MAX_AI_PREVIEW_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_AI_PREVIEW_CELLS = 512
MAX_AI_PREVIEW_CELL_BYTES = 262_144
MAX_AI_PREVIEW_TOTAL_SOURCE_BYTES = 1_500_000
MAX_AI_PREVIEW_JSON_DEPTH = 24
MAX_AI_PREVIEW_CONTAINER_ITEMS = 10_000
MAX_AI_PREVIEW_SAFE_TEXT_CHARS = 320
MAX_AI_PREVIEW_IDENTIFIER_CHARS = 160
MAX_SAFE_INTEGER = 9_007_199_254_740_991
AI_PREVIEW_CAPABILITY_MAX_AGE_SECONDS = 900.0
# Capability refresh and provider composition use separate budgets. Their
# maximum 65-second sequence remains below the companion's 70-second relay
# budget; neither request is retried.
AI_PREVIEW_CAPABILITY_TIMEOUT_SECONDS = 5.0
AI_PREVIEW_SERVER_TIMEOUT_SECONDS = 60.0

AI_PREVIEW_CAPABILITY_HASH_DOMAIN = "spl.ai-preview.capability/v1"
AI_PREVIEW_CELL_SOURCE_HASH_DOMAIN = "splime.ide.notebook-cell-source/v1"
AI_PREVIEW_EXCLUSIONS_HASH_DOMAIN = "spl.ai-preview.exclusions/v1"
AI_PREVIEW_TRANSFER_HASH_DOMAIN = "spl.ai-preview.transfer/v1"
AI_PREVIEW_CONSENT_HASH_DOMAIN = "spl.ai-preview.consent/v1"

CAPABILITY_REASONS = frozenset(
    {
        "feature_disabled",
        "provider_sdk_unavailable",
        "provider_credential_missing",
        "provider_configuration_invalid",
    }
)
AI_PREVIEW_ERROR_CODES = frozenset(
    {
        "ai_preview_disabled",
        "provider_unavailable",
        "central_credential_rejected",
        "permission_denied",
        "rate_limited",
        "invalid_request",
        "provider_timeout",
        "provider_refused",
        "provider_failed",
        "provider_result_invalid",
        # Daemon-owned transport/connection projections.
        "server_not_connected",
        "server_offline",
        "ai_preview_protocol_invalid",
    }
)
AI_PREVIEW_ERROR_OUTCOMES = frozenset({"not_started", "not_completed", "unknown"})
CENTRAL_AI_PREVIEW_ERROR_SEMANTICS: dict[str, tuple[bool, str]] = {
    "ai_preview_disabled": (False, "not_started"),
    "provider_unavailable": (False, "not_started"),
    "central_credential_rejected": (False, "not_started"),
    "permission_denied": (False, "not_started"),
    "rate_limited": (True, "not_started"),
    "invalid_request": (False, "not_started"),
    "provider_timeout": (False, "unknown"),
    "provider_refused": (False, "not_completed"),
    "provider_failed": (False, "unknown"),
    "provider_result_invalid": (False, "not_completed"),
}

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_RELEASE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,95}$")
_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_UTC_MILLISECONDS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_FORBIDDEN_OUTPUT = re.compile(
    r"(?:https?://|/Users/|/home/|[A-Za-z]:\\|\bBearer\s+|\bsk-[A-Za-z0-9_-]{8,})",
    re.IGNORECASE,
)

_CAPABILITY_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "observed_at",
        "available",
        "reason",
        "provider_display_name",
        "model_policy_label",
        "operation",
        "request_schema",
        "result_schema",
        "max_request_bytes",
        "max_cells",
        "max_cell_bytes",
        "max_total_source_bytes",
        "storage_disclosure",
        "cancellation_disclosure",
        "capability_hash",
    }
)
_REQUEST_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "client_request_id",
        "document_id",
        "document_version",
        "document_revision",
        "snapshot_id",
        "snapshot_hash",
        "facts_id",
        "facts_hash",
        "included_cells",
        "excluded_cells",
        "facts",
        "consent",
        "outputs_included",
        "notebook_metadata_included",
        "runtime_inspected",
        "kernel_queried",
    }
)
_INCLUDED_CELL_FIELDS = frozenset({"cell_id", "ordinal", "kind", "source", "source_hash"})
_EXCLUDED_CELL_FIELDS = frozenset({"cell_id", "ordinal", "reason"})
_CONSENT_FIELDS = frozenset(
    {
        "consent_id",
        "consent_hash",
        "granted_at",
        "capability_observed_at",
        "capability_hash",
        "included_cell_ids",
        "exclusion_hash",
        "transfer_hash",
        "confirmed",
    }
)
_FACTS_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "facts_id",
        "facts_hash",
        "snapshot_id",
        "snapshot_hash",
        "validity",
        "syntax_issues",
        "imports",
        "definitions",
        "definition_use_edges",
        "effects",
        "findings",
        "runtime_inspected",
        "kernel_queried",
    }
)
_FACT_BASE_FIELDS = frozenset({"cell_id", "cell_ordinal", "fact_ordinal"})
_RESULT_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "client_request_id",
        "snapshot_id",
        "snapshot_hash",
        "facts_id",
        "facts_hash",
        "consent_id",
        "consent_hash",
        "provider_display_name",
        "model_policy_label",
        "proposal",
        "usage",
        "publish_ready",
    }
)
_USAGE_FIELDS = frozenset({"input_tokens", "output_tokens", "total_tokens"})
_ERROR_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "code",
        "safe_message",
        "retryable",
        "outcome",
        "correlation_id",
    }
)


class AIPreviewContractError(RuntimeError):
    """Stable machine-coded failure for the daemon AI Preview facade."""

    def __init__(
        self,
        code: str,
        status: HTTPStatus = HTTPStatus.BAD_REQUEST,
        *,
        retryable: bool = False,
        outcome: str = "not_started",
        correlation_id: str | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.status = status
        self.retryable = retryable
        self.outcome = outcome
        self.correlation_id = correlation_id


@dataclass(frozen=True)
class NotebookPipelinePreviewRequest:
    """One validated source-bearing request held only for the route call."""

    document: dict[str, Any]


def parse_ai_preview_request(raw: bytes) -> NotebookPipelinePreviewRequest:
    """Parse one exact request without accepting duplicate or future fields."""

    document = _parse_closed_document(raw)
    validate_ai_preview_request(document)
    return NotebookPipelinePreviewRequest(document=document)


def validate_ai_preview_capabilities(value: Mapping[str, Any]) -> None:
    """Validate the central server's complete capability and consent policy."""

    document = _require_mapping(value, response=True)
    _require_exact_fields(document, _CAPABILITY_FIELDS, response=True)
    _require_schema(document, AI_PREVIEW_CAPABILITIES_SCHEMA, response=True)
    _require_timestamp(document["observed_at"], response=True)
    if type(document["available"]) is not bool:
        _response_fail()
    reason = document["reason"]
    if document["available"]:
        if reason is not None:
            _response_fail()
    elif reason not in CAPABILITY_REASONS:
        _response_fail()
    _require_safe_text(document["provider_display_name"], required=True, response=True)
    _require_safe_text(document["model_policy_label"], required=True, response=True)
    if document["operation"] != AI_PREVIEW_OPERATION:
        _response_fail()
    if document["request_schema"] != AI_PREVIEW_REQUEST_SCHEMA:
        _response_fail()
    if document["result_schema"] != AI_PREVIEW_RESULT_SCHEMA:
        _response_fail()
    _require_bounded_positive_int(
        document["max_request_bytes"],
        maximum=MAX_AI_PREVIEW_REQUEST_BYTES,
        response=True,
    )
    _require_bounded_positive_int(
        document["max_cells"],
        maximum=MAX_AI_PREVIEW_CELLS,
        response=True,
    )
    _require_bounded_positive_int(
        document["max_cell_bytes"],
        maximum=MAX_AI_PREVIEW_CELL_BYTES,
        response=True,
    )
    _require_bounded_positive_int(
        document["max_total_source_bytes"],
        maximum=MAX_AI_PREVIEW_TOTAL_SOURCE_BYTES,
        response=True,
    )
    _require_safe_text(document["storage_disclosure"], required=True, response=True)
    _require_safe_text(document["cancellation_disclosure"], required=True, response=True)
    capability_hash = _require_hash(document["capability_hash"], response=True)
    stable_policy = {key: document[key] for key in sorted(_CAPABILITY_FIELDS - {"observed_at", "capability_hash"})}
    if capability_hash != _domain_hash(
        AI_PREVIEW_CAPABILITY_HASH_DOMAIN,
        _canonical_json(stable_policy, response=True),
    ):
        _response_fail()


def validate_ai_preview_request(
    value: Mapping[str, Any],
    *,
    current_capabilities: Mapping[str, Any] | None = None,
) -> None:
    """Validate one browser-neutral notebook proposal request."""

    document = _require_mapping(value)
    _require_exact_fields(document, _REQUEST_FIELDS)
    _require_schema(document, AI_PREVIEW_REQUEST_SCHEMA)
    _require_identifier(document["client_request_id"])
    _require_identifier(document["document_id"])
    _require_identifier(document["document_version"])
    _require_safe_int(document["document_revision"])
    snapshot_id = _require_identifier(document["snapshot_id"])
    snapshot_hash = _require_hash(document["snapshot_hash"])
    facts_id = _require_identifier(document["facts_id"])
    facts_hash = _require_hash(document["facts_hash"])
    for field in (
        "outputs_included",
        "notebook_metadata_included",
        "runtime_inspected",
        "kernel_queried",
    ):
        if document[field] is not False:
            _request_fail()

    included = _require_list(document["included_cells"], maximum=MAX_AI_PREVIEW_CELLS)
    excluded = _require_list(document["excluded_cells"], maximum=MAX_AI_PREVIEW_CELLS)
    if not included or len(included) + len(excluded) > MAX_AI_PREVIEW_CELLS:
        _request_fail()
    included_ids: list[str] = []
    cell_ids: set[str] = set()
    ordinals: set[int] = set()
    last_included_ordinal = -1
    total_source_bytes = 0
    for raw_cell in included:
        cell = _require_mapping(raw_cell)
        _require_exact_fields(cell, _INCLUDED_CELL_FIELDS)
        cell_id = _require_identifier(cell["cell_id"])
        ordinal = _require_safe_int(cell["ordinal"])
        if cell["kind"] != "code" or cell_id in cell_ids or ordinal in ordinals:
            _request_fail()
        if ordinal <= last_included_ordinal:
            _request_fail()
        source = cell["source"]
        if not isinstance(source, str):
            _request_fail()
        source_bytes = _utf8_bytes(source)
        if len(source_bytes) > MAX_AI_PREVIEW_CELL_BYTES:
            _request_fail("body_too_large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        total_source_bytes += len(source_bytes)
        if total_source_bytes > MAX_AI_PREVIEW_TOTAL_SOURCE_BYTES:
            _request_fail("body_too_large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        source_hash = _require_hash(cell["source_hash"])
        if source_hash != _domain_hash(AI_PREVIEW_CELL_SOURCE_HASH_DOMAIN, source_bytes):
            _request_fail()
        included_ids.append(cell_id)
        cell_ids.add(cell_id)
        ordinals.add(ordinal)
        last_included_ordinal = ordinal

    last_excluded_ordinal = -1
    for raw_cell in excluded:
        cell = _require_mapping(raw_cell)
        _require_exact_fields(cell, _EXCLUDED_CELL_FIELDS)
        cell_id = _require_identifier(cell["cell_id"])
        ordinal = _require_safe_int(cell["ordinal"])
        if cell_id in cell_ids or ordinal in ordinals or ordinal <= last_excluded_ordinal:
            _request_fail()
        if cell["reason"] not in {
            "non_code",
            "user_excluded",
            "secret_like",
            "invalid_syntax",
        }:
            _request_fail()
        cell_ids.add(cell_id)
        ordinals.add(ordinal)
        last_excluded_ordinal = ordinal

    _validate_analysis_facts(
        document["facts"],
        snapshot_id=snapshot_id,
        snapshot_hash=snapshot_hash,
        facts_id=facts_id,
        facts_hash=facts_hash,
        included_cell_ids=set(included_ids),
        known_cell_ids=cell_ids,
    )
    _validate_consent(
        document["consent"],
        request=document,
        included_ids=included_ids,
    )
    if current_capabilities is not None:
        validate_ai_preview_capabilities(current_capabilities)
        consent = _require_mapping(document["consent"])
        if not current_capabilities["available"]:
            code = (
                "ai_preview_disabled"
                if current_capabilities["reason"] == "feature_disabled"
                else "provider_unavailable"
            )
            raise AIPreviewContractError(code, HTTPStatus.SERVICE_UNAVAILABLE)
        if consent["capability_hash"] != current_capabilities["capability_hash"]:
            _request_fail()
        _require_capability_freshness(
            consent["capability_observed_at"],
            current_time=current_capabilities["observed_at"],
        )
        _require_capability_freshness(
            consent["granted_at"],
            current_time=current_capabilities["observed_at"],
        )
        if (
            len(_canonical_json(document)) > current_capabilities["max_request_bytes"]
            or len(included) > current_capabilities["max_cells"]
            or total_source_bytes > current_capabilities["max_total_source_bytes"]
            or any(len(_utf8_bytes(cell["source"])) > current_capabilities["max_cell_bytes"] for cell in included)
        ):
            _request_fail("body_too_large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)


def validate_ai_preview_result(
    value: Mapping[str, Any],
    *,
    request: Mapping[str, Any] | None = None,
    current_capabilities: Mapping[str, Any] | None = None,
) -> None:
    """Validate and, when supplied, bind one central result to its request."""

    document = _require_mapping(value, response=True)
    _require_exact_fields(document, _RESULT_FIELDS, response=True)
    _require_schema(document, AI_PREVIEW_RESULT_SCHEMA, response=True)
    _require_identifier(document["client_request_id"], response=True)
    _require_identifier(document["snapshot_id"], response=True)
    _require_hash(document["snapshot_hash"], response=True)
    _require_identifier(document["facts_id"], response=True)
    _require_hash(document["facts_hash"], response=True)
    _require_identifier(document["consent_id"], response=True)
    _require_hash(document["consent_hash"], response=True)
    _require_safe_text(document["provider_display_name"], required=True, response=True)
    _require_safe_text(document["model_policy_label"], required=True, response=True)
    if current_capabilities is not None:
        validated_capabilities = _require_mapping(
            current_capabilities,
            response=True,
        )
        validate_ai_preview_capabilities(validated_capabilities)
        if (
            document["provider_display_name"] != validated_capabilities["provider_display_name"]
            or document["model_policy_label"] != validated_capabilities["model_policy_label"]
        ):
            _response_fail()
    _validate_usage(document["usage"])
    if document["publish_ready"] is not False:
        _response_fail()
    _validate_proposal(
        document["proposal"],
        snapshot_id=document["snapshot_id"],
        snapshot_hash=document["snapshot_hash"],
        facts_id=document["facts_id"],
        facts_hash=document["facts_hash"],
        request=request,
    )
    if request is not None:
        validated_request = _require_mapping(request)
        validate_ai_preview_request(validated_request)
        _reject_source_echo(document["proposal"], validated_request["included_cells"])
        for field in (
            "client_request_id",
            "snapshot_id",
            "snapshot_hash",
            "facts_id",
            "facts_hash",
        ):
            if document[field] != validated_request[field]:
                _response_fail()
        consent = _require_mapping(validated_request["consent"])
        if document["consent_id"] != consent["consent_id"] or document["consent_hash"] != consent["consent_hash"]:
            _response_fail()


def validate_ai_preview_error(value: Mapping[str, Any]) -> None:
    """Validate one fixed safe error without accepting upstream prose fields."""

    document = _require_mapping(value, response=True)
    _require_exact_fields(document, _ERROR_FIELDS, response=True)
    _require_schema(document, AI_PREVIEW_ERROR_SCHEMA, response=True)
    if document["code"] not in AI_PREVIEW_ERROR_CODES:
        _response_fail()
    _require_safe_text(document["safe_message"], required=True, response=True)
    if type(document["retryable"]) is not bool:
        _response_fail()
    if document["outcome"] not in AI_PREVIEW_ERROR_OUTCOMES:
        _response_fail()
    expected_semantics = CENTRAL_AI_PREVIEW_ERROR_SEMANTICS.get(document["code"])
    if (
        expected_semantics is not None
        and (
            document["retryable"],
            document["outcome"],
        )
        != expected_semantics
    ):
        _response_fail()
    _require_identifier(document["correlation_id"], response=True)


def ai_preview_error_document(
    code: str,
    *,
    safe_message: str,
    retryable: bool,
    outcome: str,
    correlation_id: str,
) -> dict[str, Any]:
    """Build one daemon-owned error document from fixed product copy."""

    document = {
        "schema": AI_PREVIEW_ERROR_SCHEMA,
        "schema_version": AI_PREVIEW_SCHEMA_VERSION,
        "code": code,
        "safe_message": safe_message,
        "retryable": bool(retryable),
        "outcome": outcome,
        "correlation_id": correlation_id,
    }
    validate_ai_preview_error(document)
    return document


def _validate_analysis_facts(
    value: Any,
    *,
    snapshot_id: str,
    snapshot_hash: str,
    facts_id: str,
    facts_hash: str,
    included_cell_ids: set[str],
    known_cell_ids: set[str],
) -> None:
    facts = _require_mapping(value)
    _require_exact_fields(facts, _FACTS_FIELDS)
    _require_schema(facts, "splime.ide.analysis-facts")
    if (
        _require_identifier(facts["facts_id"]) != facts_id
        or _require_hash(facts["facts_hash"]) != facts_hash
        or _require_identifier(facts["snapshot_id"]) != snapshot_id
        or _require_hash(facts["snapshot_hash"]) != snapshot_hash
    ):
        _request_fail()
    if facts["validity"] not in {"valid", "partial", "invalid"}:
        _request_fail()
    if facts["runtime_inspected"] is not False or facts["kernel_queried"] is not False:
        _request_fail()

    arrays = {
        "syntax_issues": (2_000, _validate_syntax_issue, known_cell_ids),
        "imports": (4_000, _validate_import_fact, included_cell_ids),
        "definitions": (4_000, _validate_definition_fact, included_cell_ids),
        "definition_use_edges": (8_000, _validate_definition_use_edge, included_cell_ids),
        "effects": (4_000, _validate_effect_fact, included_cell_ids),
        "findings": (4_000, _validate_finding, known_cell_ids),
    }
    for name, (maximum, validator, allowed_cell_ids) in arrays.items():
        items = _require_list(facts[name], maximum=maximum)
        seen_ordinals: set[int] = set()
        for item in items:
            fact_ordinal = validator(item, known_cell_ids=allowed_cell_ids)
            if fact_ordinal in seen_ordinals:
                _request_fail()
            seen_ordinals.add(fact_ordinal)

    hash_body = {
        "snapshot_id": facts["snapshot_id"],
        "snapshot_hash": facts["snapshot_hash"],
        "validity": facts["validity"],
        "syntax_issues": facts["syntax_issues"],
        "imports": facts["imports"],
        "definitions": facts["definitions"],
        "definition_use_edges": facts["definition_use_edges"],
        "effects": facts["effects"],
        "findings": facts["findings"],
        "runtime_inspected": False,
        "kernel_queried": False,
    }
    if facts_hash != _domain_hash(
        "splime.ide.analysis-facts/v1",
        _canonical_json(hash_body),
    ):
        _request_fail()


def _validate_syntax_issue(value: Any, *, known_cell_ids: set[str]) -> int:
    item, fact_ordinal = _validate_fact_base(
        value,
        extra_fields={"line", "column", "code", "message"},
        known_cell_ids=known_cell_ids,
    )
    _require_bounded_positive_int(item["line"], maximum=MAX_SAFE_INTEGER)
    _require_safe_int(item["column"])
    if item["code"] not in {"python_syntax_error", "python_partial_syntax"}:
        _request_fail()
    if item["message"] not in {
        "Python syntax is invalid in this cell.",
        "Python syntax is incomplete in this cell.",
    }:
        _request_fail()
    return fact_ordinal


def _validate_import_fact(value: Any, *, known_cell_ids: set[str]) -> int:
    item, fact_ordinal = _validate_fact_base(
        value,
        extra_fields={"module", "name", "alias"},
        known_cell_ids=known_cell_ids,
    )
    _require_safe_text(item["module"], required=True)
    for field in ("name", "alias"):
        if item[field] is not None:
            _require_safe_text(item[field], required=True)
    return fact_ordinal


def _validate_definition_fact(value: Any, *, known_cell_ids: set[str]) -> int:
    item, fact_ordinal = _validate_fact_base(
        value,
        extra_fields={"name", "kind", "signature"},
        known_cell_ids=known_cell_ids,
    )
    _require_safe_text(item["name"], required=True)
    if item["kind"] not in {"function", "class", "assignment"}:
        _request_fail()
    if item["signature"] is not None:
        _require_safe_text(item["signature"])
    return fact_ordinal


def _validate_definition_use_edge(value: Any, *, known_cell_ids: set[str]) -> int:
    fields = frozenset(
        {
            "fact_ordinal",
            "from_cell_id",
            "from_cell_ordinal",
            "to_cell_id",
            "to_cell_ordinal",
            "symbol",
            "reason",
        }
    )
    item = _require_mapping(value)
    _require_exact_fields(item, fields)
    fact_ordinal = _require_safe_int(item["fact_ordinal"])
    if (
        _require_identifier(item["from_cell_id"]) not in known_cell_ids
        or _require_identifier(item["to_cell_id"]) not in known_cell_ids
    ):
        _request_fail()
    _require_safe_int(item["from_cell_ordinal"])
    _require_safe_int(item["to_cell_ordinal"])
    _require_safe_text(item["symbol"], required=True)
    if item["reason"] != "definition_use":
        _request_fail()
    return fact_ordinal


def _validate_effect_fact(value: Any, *, known_cell_ids: set[str]) -> int:
    item, fact_ordinal = _validate_fact_base(
        value,
        extra_fields={"kind", "confidence"},
        known_cell_ids=known_cell_ids,
    )
    if item["kind"] not in {
        "file",
        "network",
        "subprocess",
        "environment",
        "output",
        "randomness",
        "time",
    }:
        _request_fail()
    if item["confidence"] not in {"certain", "likely", "possible"}:
        _request_fail()
    return fact_ordinal


def _validate_finding(value: Any, *, known_cell_ids: set[str]) -> int:
    item, fact_ordinal = _validate_fact_base(
        value,
        extra_fields={"kind", "code", "severity", "message"},
        known_cell_ids=known_cell_ids,
    )
    if item["kind"] not in {"secret_like", "untrusted_instruction"}:
        _request_fail()
    if item["code"] not in {
        "secret_assignment",
        "secret_mapping_or_url",
        "bearer_credential",
        "credential_shape",
        "private_key",
        "known_secret_value",
        "untrusted_instruction_text",
    }:
        _request_fail()
    if item["severity"] not in {"warning", "blocker"}:
        _request_fail()
    _require_safe_text(item["message"], required=True)
    return fact_ordinal


def _validate_fact_base(
    value: Any,
    *,
    extra_fields: set[str],
    known_cell_ids: set[str],
) -> tuple[Mapping[str, Any], int]:
    item = _require_mapping(value)
    _require_exact_fields(item, _FACT_BASE_FIELDS | extra_fields)
    if _require_identifier(item["cell_id"]) not in known_cell_ids:
        _request_fail()
    _require_safe_int(item["cell_ordinal"])
    return item, _require_safe_int(item["fact_ordinal"])


def _validate_consent(
    value: Any,
    *,
    request: Mapping[str, Any],
    included_ids: list[str],
) -> None:
    consent = _require_mapping(value)
    _require_exact_fields(consent, _CONSENT_FIELDS)
    _require_identifier(consent["consent_id"])
    for field in (
        "consent_hash",
        "capability_hash",
        "exclusion_hash",
        "transfer_hash",
    ):
        _require_hash(consent[field])
    _require_timestamp(consent["granted_at"])
    _require_timestamp(consent["capability_observed_at"])
    ids = _require_list(consent["included_cell_ids"], maximum=MAX_AI_PREVIEW_CELLS)
    if ids != included_ids or any(_require_identifier(item) != included_ids[index] for index, item in enumerate(ids)):
        _request_fail()
    if consent["confirmed"] is not True:
        _request_fail()

    expected_exclusion_hash = _domain_hash(
        AI_PREVIEW_EXCLUSIONS_HASH_DOMAIN,
        _canonical_json(request["excluded_cells"]),
    )
    if consent["exclusion_hash"] != expected_exclusion_hash:
        _request_fail()
    transfer_projection = {
        "client_request_id": request["client_request_id"],
        "document_id": request["document_id"],
        "document_version": request["document_version"],
        "document_revision": request["document_revision"],
        "snapshot_id": request["snapshot_id"],
        "snapshot_hash": request["snapshot_hash"],
        "facts_id": request["facts_id"],
        "facts_hash": request["facts_hash"],
        "included_cells": [
            {
                "cell_id": cell["cell_id"],
                "ordinal": cell["ordinal"],
                "kind": cell["kind"],
                "source_hash": cell["source_hash"],
            }
            for cell in request["included_cells"]
        ],
        "excluded_cells": request["excluded_cells"],
        "capability_hash": consent["capability_hash"],
    }
    expected_transfer_hash = _domain_hash(
        AI_PREVIEW_TRANSFER_HASH_DOMAIN,
        _canonical_json(transfer_projection),
    )
    if consent["transfer_hash"] != expected_transfer_hash:
        _request_fail()
    consent_projection = {key: consent[key] for key in sorted(_CONSENT_FIELDS - {"consent_hash"})}
    expected_consent_hash = _domain_hash(
        AI_PREVIEW_CONSENT_HASH_DOMAIN,
        _canonical_json(consent_projection),
    )
    if consent["consent_hash"] != expected_consent_hash:
        _request_fail()


def _validate_usage(value: Any) -> None:
    if value is None:
        return
    usage = _require_mapping(value, response=True)
    _require_exact_fields(usage, _USAGE_FIELDS, response=True)
    values: dict[str, int] = {}
    for field in _USAGE_FIELDS:
        values[field] = _require_safe_int(usage[field], response=True)
    if values["total_tokens"] < values["input_tokens"] + values["output_tokens"]:
        _response_fail()


def _validate_proposal(
    value: Any,
    *,
    snapshot_id: str,
    snapshot_hash: str,
    facts_id: str,
    facts_hash: str,
    request: Mapping[str, Any] | None,
) -> None:
    proposal = _require_mapping(value, response=True)
    fields = frozenset(
        {
            "schema",
            "schema_version",
            "proposal_id",
            "snapshot_id",
            "snapshot_hash",
            "facts_id",
            "facts_hash",
            "mode",
            "title",
            "nodes",
            "edges",
            "alternatives",
            "warnings",
            "excluded_cell_ids",
            "unsupported_constructs",
            "publish_ready",
            "allowed_edits",
            "generated_at",
            "provenance",
        }
    )
    _require_exact_fields(proposal, fields, response=True)
    _require_schema(proposal, "splime.ide.splime-proposal", response=True)
    _require_identifier(proposal["proposal_id"], response=True)
    if (
        _require_identifier(proposal["snapshot_id"], response=True) != snapshot_id
        or _require_hash(proposal["snapshot_hash"], response=True) != snapshot_hash
        or _require_identifier(proposal["facts_id"], response=True) != facts_id
        or _require_hash(proposal["facts_hash"], response=True) != facts_hash
        or proposal["mode"] != "central_ai"
    ):
        _response_fail()
    if request is not None and proposal["proposal_id"] != request["client_request_id"]:
        _response_fail()
    _require_safe_text(proposal["title"], required=True, response=True)

    included_cell_ids = {cell["cell_id"] for cell in request["included_cells"]} if request is not None else None
    all_cell_ids = (
        included_cell_ids | {cell["cell_id"] for cell in request["excluded_cells"]}
        if request is not None and included_cell_ids is not None
        else None
    )
    allowed_fact_ids = _fact_identifiers(request["facts"]) if request is not None else None

    node_ids: set[str] = set()
    node_ordinals: set[int] = set()
    nodes = _require_list(proposal["nodes"], maximum=512, response=True)
    for raw_node in nodes:
        node = _require_mapping(raw_node, response=True)
        _require_exact_fields(
            node,
            frozenset({"node_id", "ordinal", "name", "kind", "cell_ids", "claim"}),
            response=True,
        )
        node_id = _require_identifier(node["node_id"], response=True)
        ordinal = _require_safe_int(node["ordinal"], response=True)
        if node_id in node_ids or ordinal in node_ordinals:
            _response_fail()
        node_ids.add(node_id)
        node_ordinals.add(ordinal)
        _require_safe_text(node["name"], required=True, response=True)
        if node["kind"] not in {"source", "function", "transform", "sink"}:
            _response_fail()
        node_cell_ids = _validate_identifier_list(node["cell_ids"], minimum=1, maximum=512)
        if included_cell_ids is not None and not set(node_cell_ids) <= included_cell_ids:
            _response_fail()
        claim = _require_mapping(node["claim"], response=True)
        _require_exact_fields(
            claim,
            frozenset({"summary", "confidence", "basis", "fact_ids"}),
            response=True,
        )
        _require_safe_text(claim["summary"], required=True, response=True)
        if claim["confidence"] not in {"high", "medium", "low"}:
            _response_fail()
        if claim["basis"] != "provider_inference":
            _response_fail()
        claim_fact_ids = _validate_identifier_list(
            claim["fact_ids"],
            minimum=1,
            maximum=2_048,
        )
        if allowed_fact_ids is not None and not set(claim_fact_ids) <= allowed_fact_ids:
            _response_fail()

    edge_ids: set[str] = set()
    edges = _require_list(proposal["edges"], maximum=8_000, response=True)
    for raw_edge in edges:
        edge = _require_mapping(raw_edge, response=True)
        _require_exact_fields(
            edge,
            frozenset(
                {
                    "edge_id",
                    "ordinal",
                    "from_node_id",
                    "to_node_id",
                    "reason",
                    "confidence",
                }
            ),
            response=True,
        )
        edge_id = _require_identifier(edge["edge_id"], response=True)
        if edge_id in edge_ids:
            _response_fail()
        edge_ids.add(edge_id)
        _require_safe_int(edge["ordinal"], response=True)
        if (
            _require_identifier(edge["from_node_id"], response=True) not in node_ids
            or _require_identifier(edge["to_node_id"], response=True) not in node_ids
        ):
            _response_fail()
        if edge["reason"] not in {"definition_use", "cell_order", "explicit_symbol"}:
            _response_fail()
        if edge["confidence"] not in {"high", "medium", "low"}:
            _response_fail()

    alternatives = _require_list(proposal["alternatives"], maximum=1_000, response=True)
    alternative_ids: set[str] = set()
    for raw_alternative in alternatives:
        alternative = _require_mapping(raw_alternative, response=True)
        _require_exact_fields(
            alternative,
            frozenset({"alternative_id", "summary", "affected_node_ids", "confidence"}),
            response=True,
        )
        alternative_id = _require_identifier(alternative["alternative_id"], response=True)
        if alternative_id in alternative_ids:
            _response_fail()
        alternative_ids.add(alternative_id)
        _require_safe_text(alternative["summary"], required=True, response=True)
        affected = _validate_identifier_list(
            alternative["affected_node_ids"],
            minimum=1,
            maximum=512,
        )
        if not set(affected) <= node_ids:
            _response_fail()
        if alternative["confidence"] not in {"high", "medium", "low"}:
            _response_fail()

    warning_ids: set[str] = set()
    warnings = _require_list(proposal["warnings"], maximum=4_000, response=True)
    for raw_warning in warnings:
        warning = _require_mapping(raw_warning, response=True)
        _require_exact_fields(
            warning,
            frozenset({"warning_id", "code", "message", "cell_ids"}),
            response=True,
        )
        warning_id = _require_identifier(warning["warning_id"], response=True)
        if warning_id in warning_ids:
            _response_fail()
        warning_ids.add(warning_id)
        if warning["code"] not in {
            "unsupported_construct",
            "partial_syntax",
            "side_effect",
            "secret_excluded",
            "ambiguous_dependency",
        }:
            _response_fail()
        _require_safe_text(warning["message"], required=True, response=True)
        warning_cells = _validate_identifier_list(
            warning["cell_ids"],
            minimum=1,
            maximum=512,
        )
        if all_cell_ids is not None and not set(warning_cells) <= all_cell_ids:
            _response_fail()

    unsupported = _require_list(
        proposal["unsupported_constructs"],
        maximum=512,
        response=True,
    )
    for raw_item in unsupported:
        item = _require_mapping(raw_item, response=True)
        _require_exact_fields(item, frozenset({"cell_id", "code"}), response=True)
        _require_identifier(item["cell_id"], response=True)
        if all_cell_ids is not None and item["cell_id"] not in all_cell_ids:
            _response_fail()
        if item["code"] not in {
            "syntax_invalid",
            "dynamic_dispatch",
            "runtime_value",
            "not_python",
            "secret_excluded",
        }:
            _response_fail()

    excluded_cell_ids = _validate_identifier_list(
        proposal["excluded_cell_ids"],
        minimum=0,
        maximum=512,
    )
    if request is not None and excluded_cell_ids != [cell["cell_id"] for cell in request["excluded_cells"]]:
        _response_fail()
    edits = _require_list(proposal["allowed_edits"], maximum=2, response=True)
    if edits != ["rename", "exclude"]:
        _response_fail()
    if proposal["publish_ready"] is not False:
        _response_fail()
    _require_timestamp(proposal["generated_at"], response=True)
    _validate_server_provenance(proposal["provenance"])


def _validate_server_provenance(value: Any) -> None:
    provenance = _require_mapping(value, response=True)
    _require_exact_fields(
        provenance,
        frozenset({"producer", "release_id", "observed_at", "protocol_selected"}),
        response=True,
    )
    if provenance["producer"] != "spl_server":
        _response_fail()
    release_id = provenance["release_id"]
    if not isinstance(release_id, str) or not _RELEASE_ID.fullmatch(release_id):
        _response_fail()
    _require_timestamp(provenance["observed_at"], response=True)
    if provenance["protocol_selected"] != 1:
        _response_fail()


def _fact_identifiers(facts: Any) -> set[str]:
    document = _require_mapping(facts)
    identifiers: set[str] = set()
    for category in (
        "syntax_issues",
        "imports",
        "definitions",
        "definition_use_edges",
        "effects",
        "findings",
    ):
        values = _require_list(document[category], maximum=8_000)
        for item in values:
            mapping = _require_mapping(item)
            ordinal = _require_safe_int(mapping["fact_ordinal"])
            identifiers.add(f"fact:{category}:{ordinal}")
    if not identifiers:
        identifiers.add("fact:analysis:empty")
    return identifiers


def _validate_identifier_list(
    value: Any,
    *,
    minimum: int,
    maximum: int,
) -> list[str]:
    items = _require_list(value, maximum=maximum, response=True)
    if len(items) < minimum:
        _response_fail()
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        identifier = _require_identifier(item, response=True)
        if identifier in seen:
            _response_fail()
        seen.add(identifier)
        result.append(identifier)
    return result


def _reject_source_echo(value: Any, cells: Any) -> None:
    output_strings: list[str] = []

    def collect(current: Any) -> None:
        if isinstance(current, str):
            output_strings.append(current)
        elif isinstance(current, list):
            for item in current:
                collect(item)
        elif isinstance(current, Mapping):
            for item in current.values():
                collect(item)

    collect(value)
    output = "\n".join(output_strings)
    for cell in _require_list(cells, maximum=MAX_AI_PREVIEW_CELLS):
        source = _require_mapping(cell)["source"]
        for line in source.splitlines():
            candidate = line.strip()
            if candidate and candidate in output:
                _response_fail()


def _parse_closed_document(raw: bytes) -> dict[str, Any]:
    if not isinstance(raw, bytes):
        _request_fail()
    if len(raw) > MAX_AI_PREVIEW_REQUEST_BYTES:
        _request_fail("body_too_large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AIPreviewContractError("invalid_request") from exc

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result

    def reject_constant(_: str) -> NoReturn:
        raise ValueError("non-finite JSON number")

    try:
        value = json.loads(
            text,
            object_pairs_hook=object_pairs,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise AIPreviewContractError("invalid_request") from exc
    _validate_json_budget(value)
    if not isinstance(value, dict):
        _request_fail()
    return value


def _validate_json_budget(value: Any) -> None:
    containers = 0
    items = 0

    def visit(current: Any, depth: int) -> None:
        nonlocal containers, items
        if depth > MAX_AI_PREVIEW_JSON_DEPTH:
            _request_fail()
        value_type = type(current)
        if current is None or value_type in {bool, int}:
            return
        if value_type is float:
            if not math.isfinite(current):
                _request_fail()
            return
        if value_type is str:
            _utf8_bytes(current)
            return
        if value_type is list:
            containers += 1
            items += len(current)
            if containers + items > MAX_AI_PREVIEW_CONTAINER_ITEMS:
                _request_fail()
            for item in current:
                visit(item, depth + 1)
            return
        if value_type is dict:
            containers += 1
            items += len(current)
            if containers + items > MAX_AI_PREVIEW_CONTAINER_ITEMS:
                _request_fail()
            for key, item in current.items():
                if not isinstance(key, str):
                    _request_fail()
                _utf8_bytes(key)
                visit(item, depth + 1)
            return
        _request_fail()

    visit(value, 0)


def _require_mapping(value: Any, *, response: bool = False) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail_for_context(response)
    return value


def _require_exact_fields(
    value: Mapping[str, Any],
    fields: frozenset[str] | set[str],
    *,
    response: bool = False,
) -> None:
    if set(value) != set(fields):
        _fail_for_context(response)


def _require_schema(
    document: Mapping[str, Any],
    expected: str,
    *,
    response: bool = False,
) -> None:
    if document.get("schema") != expected or document.get("schema_version") != AI_PREVIEW_SCHEMA_VERSION:
        _fail_for_context(response)


def _require_identifier(value: Any, *, response: bool = False) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        _fail_for_context(response)
    return value


def _require_hash(value: Any, *, response: bool = False) -> str:
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        _fail_for_context(response)
    return value


def _require_timestamp(value: Any, *, response: bool = False) -> str:
    if not isinstance(value, str) or not _UTC_MILLISECONDS.fullmatch(value):
        _fail_for_context(response)
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError:
        _fail_for_context(response)
    if parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z") != value:
        _fail_for_context(response)
    return value


def _require_capability_freshness(value: str, *, current_time: str) -> None:
    observed_text = _require_timestamp(value)
    current_text = _require_timestamp(current_time, response=True)
    observed = datetime.strptime(observed_text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    current = datetime.strptime(current_text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    age = (current - observed).total_seconds()
    if age < -5.0 or age > AI_PREVIEW_CAPABILITY_MAX_AGE_SECONDS:
        _request_fail()


def _require_safe_text(
    value: Any,
    *,
    required: bool = False,
    response: bool = False,
) -> str:
    if not isinstance(value, str):
        _fail_for_context(response)
    if required and not value:
        _fail_for_context(response)
    if len(value) > MAX_AI_PREVIEW_SAFE_TEXT_CHARS:
        _fail_for_context(response)
    if _FORBIDDEN_OUTPUT.search(value):
        _fail_for_context(response)
    for character in value:
        codepoint = ord(character)
        if codepoint == 0x7F or 0xD800 <= codepoint <= 0xDFFF or codepoint in {*range(0, 9), 11, 12, *range(14, 32)}:
            _fail_for_context(response)
    return value


def _require_safe_int(value: Any, *, response: bool = False) -> int:
    if type(value) is not int or not 0 <= value <= MAX_SAFE_INTEGER:
        _fail_for_context(response)
    return value


def _require_bounded_positive_int(
    value: Any,
    *,
    maximum: int,
    response: bool = False,
) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        _fail_for_context(response)
    return value


def _require_list(
    value: Any,
    *,
    maximum: int,
    response: bool = False,
) -> list[Any]:
    if not isinstance(value, list) or len(value) > maximum:
        _fail_for_context(response)
    return value


def _canonical_json(value: Any, *, response: bool = False) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (UnicodeEncodeError, ValueError, TypeError) as exc:
        if response:
            raise AIPreviewContractError(
                "ai_preview_protocol_invalid",
                HTTPStatus.BAD_GATEWAY,
            ) from exc
        raise AIPreviewContractError("invalid_request") from exc


def _domain_hash(domain: str, value: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain.encode("ascii"))
    digest.update(b"\0")
    digest.update(value)
    return f"sha256:{digest.hexdigest()}"


def _utf8_bytes(value: str) -> bytes:
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise AIPreviewContractError("invalid_request") from exc
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        _request_fail()
    return encoded


def _fail_for_context(response: bool) -> NoReturn:
    if response:
        _response_fail()
    _request_fail()


def _request_fail(
    code: str = "invalid_request",
    status: HTTPStatus = HTTPStatus.BAD_REQUEST,
) -> NoReturn:
    raise AIPreviewContractError(code, status)


def _response_fail() -> NoReturn:
    raise AIPreviewContractError(
        "ai_preview_protocol_invalid",
        HTTPStatus.BAD_GATEWAY,
    )


__all__ = [
    "AI_PREVIEW_CAPABILITIES_ROUTE",
    "AI_PREVIEW_CAPABILITIES_SCHEMA",
    "AI_PREVIEW_ERROR_CODES",
    "AI_PREVIEW_ERROR_SCHEMA",
    "AI_PREVIEW_OPERATION",
    "AI_PREVIEW_REQUEST_SCHEMA",
    "AI_PREVIEW_RESULT_SCHEMA",
    "AI_PREVIEW_ROUTE",
    "AI_PREVIEW_SCHEMA_VERSION",
    "AIPreviewContractError",
    "MAX_AI_PREVIEW_REQUEST_BYTES",
    "MAX_AI_PREVIEW_RESPONSE_BYTES",
    "NotebookPipelinePreviewRequest",
    "ai_preview_error_document",
    "parse_ai_preview_request",
    "validate_ai_preview_capabilities",
    "validate_ai_preview_error",
    "validate_ai_preview_request",
    "validate_ai_preview_result",
]
