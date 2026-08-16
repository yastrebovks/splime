"""Closed daemon contracts for approved Core Connected prerequisites.

These contracts belong to the daemon and are intentionally independent of a
Jupyter or browser package.  They expose only purpose-specific, authenticated
operations.  Browser-safe projection remains the trusted companion's job.
"""

from __future__ import annotations

import ipaddress
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Any, Mapping, NoReturn
from urllib.parse import urlsplit

CONNECTED_SCHEMA_VERSION = 1
CONNECTED_CREDENTIALS_REVEAL_ROUTE = "/ide/server/credentials/reveal"
CONNECTED_SERVER_CAPABILITIES_ROUTE = "/server/capabilities"
CONNECTED_REMOTE_RUNS_ROUTE = "/server/runs"
CONNECTED_REMOTE_RUN_PREFLIGHT_ROUTE = "/server/runs/preflight"

CONNECTED_CREDENTIALS_REVEAL_REQUEST_SCHEMA = "spl.daemon.connected-credentials-reveal-request"
CONNECTED_CREDENTIALS_REVEAL_SCHEMA = "spl.daemon.connected-credentials-reveal"
CONNECTED_REMOTE_RUN_LIST_SCHEMA = "spl.daemon.connected-run-list"
CONNECTED_REMOTE_RUN_DETAIL_SCHEMA = "spl.daemon.connected-run-detail"
CONNECTED_REMOTE_RUN_EVENTS_SCHEMA = "spl.daemon.connected-run-events"
CONNECTED_REMOTE_RUN_PREFLIGHT_REQUEST_SCHEMA = "spl.daemon.connected-run-preflight-request"
CONNECTED_REMOTE_RUN_PREFLIGHT_SCHEMA = "spl.daemon.connected-run-preflight"
CONNECTED_SERVER_CAPABILITIES_SCHEMA = "spl.daemon.connected-server-capabilities"
CONNECTED_ERROR_SCHEMA = "spl.daemon.connected-error"

CONNECTED_CREDENTIALS_REVEAL_INTENT = "reveal_reusable_central_credentials"
MAX_CONNECTED_BODY_BYTES = 16 * 1024
MAX_UPSTREAM_RESPONSE_BYTES = 1024 * 1024
# The browser contract consumes at most 100 recent Runs.  Keeping the daemon
# wrapper at the same bound also leaves enough room for the complete Run DTO
# inside MAX_JSON_CONTAINER_ITEMS; 200 production-shaped rows can exceed that
# safety budget before the response is projected by the companion.
MAX_REMOTE_RUN_ITEMS = 100
MAX_REMOTE_RUN_EVENT_ITEMS = 200
MAX_JSON_DEPTH = 24
MAX_JSON_CONTAINER_ITEMS = 10_000
MAX_JSON_STRING_LENGTH = 262_144
MAX_SECRET_LENGTH = 8_192
MAX_SERVER_URL_LENGTH = 2_048

SERVER_REMOTE_CAPABILITY_IDS = (
    "spl.remote_run.read.v1",
    "spl.remote_run.preflight.v1",
    "spl.remote_run.create.idempotent.v1",
    "spl.remote_run.cancel.queued.v1",
    "spl.remote_run.retry.terminal_child.v1",
    "spl.runtime_adapter_semantic_override.v1",
)

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_SAFE_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,63}$")
_CAPABILITY_IDENTIFIER = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+){0,15}$")
_UTC_MILLISECONDS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_HEX_COMMIT = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_REVEAL_REQUEST_FIELDS = frozenset({"schema", "schema_version", "connection_id", "intent"})
_REVEAL_RESPONSE_FIELDS = frozenset({"schema", "schema_version", "connection", "credentials", "observed_at"})
_REVEAL_CONNECTION_FIELDS = frozenset(
    {
        "connection_id",
        "remote_connection_id",
        "owner_id",
        "machine_id",
        "server_url",
    }
)
_REVEAL_CREDENTIAL_FIELDS = frozenset({"role", "secret"})
_PREFLIGHT_REQUEST_FIELDS = frozenset({"schema", "schema_version", "request"})
_PREFLIGHT_WIRE_FIELDS = frozenset(
    {
        "object",
        "version",
        "version_id",
        "target_machine_id",
        "object_owner_id",
        "library",
        "function",
        "offline_policy",
    }
)
_PREFLIGHT_REQUIRED_WIRE_FIELDS = frozenset(
    {
        "object",
        "version_id",
        "target_machine_id",
        "object_owner_id",
        "library",
        "offline_policy",
    }
)
_LIST_RESPONSE_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "source",
        "observed_at",
        "completeness",
        "truncated",
        "runs",
    }
)
_DETAIL_RESPONSE_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "source",
        "observed_at",
        "completeness",
        "detail",
    }
)
_EVENTS_RESPONSE_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "source",
        "observed_at",
        "completeness",
        "truncated",
        "events",
    }
)
_CAPABILITIES_RESPONSE_FIELDS = frozenset({"schema", "schema_version", "observed_at", "release", "capabilities"})
_CAPABILITIES_RELEASE_FIELDS = frozenset({"state", "release_id", "version", "evidence", "reason"})
_CAPABILITY_FIELDS = frozenset({"state", "reason"})
_PREFLIGHT_RESPONSE_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "source",
        "observed_at",
        "server_capabilities",
        "preflight",
    }
)
_CENTRAL_PREFLIGHT_FIELDS = frozenset(
    {
        "schema_version",
        "contract",
        "checked_at",
        "can_queue",
        "point_in_time",
        "reservation",
        "resolved_request",
        "object",
        "authorization",
        "execution",
        "environment",
        "runtimes",
        "trust_boundary",
        "telemetry",
        "compatibility",
        "capacity",
        "checks",
    }
)
_CENTRAL_PREFLIGHT_RESOLVED_FIELDS = frozenset({"version_id", "target_machine_id", "offline_policy"})
_CENTRAL_PREFLIGHT_CHECK_FIELDS = frozenset({"code", "outcome", "reason_code", "explanation", "evidence_at"})
_ERROR_FIELDS = frozenset({"schema", "schema_version", "code", "operation", "retryable"})
_VERSION_DOCUMENT_FIELDS = frozenset(
    {
        "contract",
        "schema_version",
        "component",
        "declared",
        "deployment",
        "database_schema",
    }
)
_DECLARED_UNKNOWN_FIELDS = frozenset({"state", "reason_code"})
_DECLARED_PRESENT_FIELDS = frozenset(
    {
        "state",
        "reason_code",
        "schema_version",
        "release_id",
        "component",
        "version",
        "artifact_sha256",
        "release_manifest_sha256",
        "schema_target",
        "evidence_state",
        "contracts",
        "source_repository",
        "source_ref",
        "source_binding",
        "source_commit",
    }
)
_DEPLOYMENT_UNKNOWN_FIELDS = frozenset({"state", "reason_code"})
_DEPLOYMENT_PRESENT_FIELDS = frozenset(
    {
        "state",
        "reason_code",
        "schema_version",
        "release_id",
        "component",
        "version",
        "source_ref",
        "source_commit",
        "artifact_sha256",
        "release_manifest_sha256",
        "schema_target",
        "deployed_at",
        "environment_class",
    }
)


class ConnectedIDEContractError(RuntimeError):
    """Stable machine-coded failure for the additive Connected contracts."""

    def __init__(
        self,
        code: str,
        status: HTTPStatus = HTTPStatus.BAD_REQUEST,
        *,
        retryable: bool = False,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.status = status
        self.retryable = retryable


@dataclass(frozen=True)
class ConnectedCredentialsRevealRequest:
    """One deliberate request for the current stored connection secrets."""

    connection_id: str


@dataclass(frozen=True)
class RemoteRunPreflightRequest:
    """Closed, read-only request accepted by the daemon preflight facade."""

    payload: dict[str, Any]


def parse_connected_credentials_reveal_request(
    raw: bytes,
) -> ConnectedCredentialsRevealRequest:
    """Parse the exact explicit credential-reveal request."""

    document = _parse_closed_document(raw)
    _require_exact_fields(document, _REVEAL_REQUEST_FIELDS)
    if document["schema"] != CONNECTED_CREDENTIALS_REVEAL_REQUEST_SCHEMA:
        _fail("request_invalid")
    _require_schema_version(document["schema_version"])
    if document["intent"] != CONNECTED_CREDENTIALS_REVEAL_INTENT:
        _fail("request_invalid")
    return ConnectedCredentialsRevealRequest(
        connection_id=_require_identifier(document["connection_id"]),
    )


def build_connected_credentials_reveal_response(
    credentials: Mapping[str, Any],
    *,
    observed_at: str | None = None,
    forbidden_values: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build the daemon-owned persisted-secret response without invented facts."""

    machine_secret = _require_secret(credentials.get("token"))
    user_secret = _require_secret(credentials.get("user_token"))
    document = {
        "schema": CONNECTED_CREDENTIALS_REVEAL_SCHEMA,
        "schema_version": CONNECTED_SCHEMA_VERSION,
        "connection": {
            "connection_id": _require_identifier(credentials.get("id")),
            "remote_connection_id": _require_identifier(credentials.get("remote_connection_id")),
            "owner_id": _require_identifier(credentials.get("owner_id")),
            "machine_id": _require_identifier(credentials.get("machine_id")),
            "server_url": validate_connected_server_url(credentials.get("server_url")),
        },
        "credentials": [
            {"role": "user", "secret": user_secret},
            {"role": "machine", "secret": machine_secret},
        ],
        "observed_at": _utc_now() if observed_at is None else observed_at,
    }
    validate_connected_credentials_reveal_response(document)
    encoded = _canonical_json(document)
    if any(value and value in encoded for value in forbidden_values):
        _fail("credential_reveal_unavailable", HTTPStatus.CONFLICT)
    return document


def validate_connected_credentials_reveal_response(
    value: Mapping[str, Any],
) -> None:
    """Validate the exact raw daemon reveal seam used by the companion."""

    document = _require_mapping(value)
    _require_exact_fields(document, _REVEAL_RESPONSE_FIELDS)
    if document["schema"] != CONNECTED_CREDENTIALS_REVEAL_SCHEMA:
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _require_schema_version(document["schema_version"], response=True)
    connection = _require_mapping(document["connection"], response=True)
    _require_exact_fields(connection, _REVEAL_CONNECTION_FIELDS, response=True)
    for field in (
        "connection_id",
        "remote_connection_id",
        "owner_id",
        "machine_id",
    ):
        _require_identifier(connection[field], response=True)
    validate_connected_server_url(connection["server_url"], response=True)
    raw_credentials = document["credentials"]
    if not isinstance(raw_credentials, list) or len(raw_credentials) != 2:
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    roles: list[str] = []
    for raw_credential in raw_credentials:
        credential = _require_mapping(raw_credential, response=True)
        _require_exact_fields(credential, _REVEAL_CREDENTIAL_FIELDS, response=True)
        role = credential["role"]
        if role not in {"user", "machine"}:
            _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
        roles.append(role)
        _require_secret(credential["secret"], response=True)
    if roles != ["user", "machine"]:
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _require_utc_milliseconds(document["observed_at"], response=True)


def parse_remote_run_preflight_request(raw: bytes) -> RemoteRunPreflightRequest:
    """Parse a closed exact-version, exact-target preflight request."""

    document = _parse_closed_document(raw)
    _require_exact_fields(document, _PREFLIGHT_REQUEST_FIELDS)
    if document["schema"] != CONNECTED_REMOTE_RUN_PREFLIGHT_REQUEST_SCHEMA:
        _fail("request_invalid")
    _require_schema_version(document["schema_version"])
    request = _require_mapping(document["request"])
    if not _PREFLIGHT_REQUIRED_WIRE_FIELDS <= set(request) or not set(request) <= _PREFLIGHT_WIRE_FIELDS:
        _fail("request_invalid")
    payload: dict[str, Any] = {}
    for field in (
        "object",
        "version_id",
        "target_machine_id",
        "object_owner_id",
        "library",
    ):
        payload[field] = _require_identifier(request[field])
    version = request.get("version")
    if version is not None:
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            _fail("request_invalid")
        payload["version"] = version
    function = request.get("function")
    if function is not None:
        payload["function"] = _require_identifier(function)
    offline_policy = request["offline_policy"]
    if offline_policy not in {"queue", "wait", "fail_fast"}:
        _fail("request_invalid")
    payload["offline_policy"] = offline_policy
    return RemoteRunPreflightRequest(payload=payload)


def build_remote_run_list_response(
    runs: list[dict[str, Any]],
    *,
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Wrap a bounded central Run list for the trusted companion."""

    truncated = len(runs) > MAX_REMOTE_RUN_ITEMS
    # Validate only the response-bounded prefix.  The central history may be
    # much larger than the IDE contract and must not make a valid bounded read
    # fail merely because rows that will never be returned exceed the wrapper
    # container budget.
    bounded_runs = runs[:MAX_REMOTE_RUN_ITEMS]
    _validate_bounded_json(bounded_runs)
    document = {
        "schema": CONNECTED_REMOTE_RUN_LIST_SCHEMA,
        "schema_version": CONNECTED_SCHEMA_VERSION,
        "source": "server",
        "observed_at": _utc_now() if observed_at is None else observed_at,
        "completeness": "partial" if truncated else "complete",
        "truncated": truncated,
        "runs": bounded_runs,
    }
    validate_remote_run_list_response(document)
    return document


def validate_remote_run_list_response(value: Mapping[str, Any]) -> None:
    document = _require_mapping(value, response=True)
    _validate_wrapper(document, _LIST_RESPONSE_FIELDS, CONNECTED_REMOTE_RUN_LIST_SCHEMA)
    truncated = document["truncated"]
    if not isinstance(truncated, bool):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    completeness = document["completeness"]
    if completeness != ("partial" if truncated else "complete"):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    runs = document["runs"]
    if not isinstance(runs, list) or len(runs) > MAX_REMOTE_RUN_ITEMS:
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    if any(not isinstance(run, dict) for run in runs):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _validate_bounded_json(runs, response=True)


def build_remote_run_detail_response(
    detail: dict[str, Any],
    *,
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Wrap one bounded central Run detail snapshot."""

    _validate_bounded_json(detail)
    document = {
        "schema": CONNECTED_REMOTE_RUN_DETAIL_SCHEMA,
        "schema_version": CONNECTED_SCHEMA_VERSION,
        "source": "server",
        "observed_at": _utc_now() if observed_at is None else observed_at,
        "completeness": "complete",
        "detail": detail,
    }
    validate_remote_run_detail_response(document)
    return document


def validate_remote_run_detail_response(value: Mapping[str, Any]) -> None:
    document = _require_mapping(value, response=True)
    _validate_wrapper(
        document,
        _DETAIL_RESPONSE_FIELDS,
        CONNECTED_REMOTE_RUN_DETAIL_SCHEMA,
    )
    if document["completeness"] != "complete":
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    detail = document["detail"]
    if not isinstance(detail, dict):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _validate_bounded_json(detail, response=True)


def build_remote_run_events_response(
    events: list[dict[str, Any]],
    *,
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Wrap a bounded central Run event snapshot."""

    _validate_bounded_json(events)
    truncated = len(events) > MAX_REMOTE_RUN_EVENT_ITEMS
    document = {
        "schema": CONNECTED_REMOTE_RUN_EVENTS_SCHEMA,
        "schema_version": CONNECTED_SCHEMA_VERSION,
        "source": "server",
        "observed_at": _utc_now() if observed_at is None else observed_at,
        "completeness": "partial" if truncated else "complete",
        "truncated": truncated,
        "events": events[:MAX_REMOTE_RUN_EVENT_ITEMS],
    }
    validate_remote_run_events_response(document)
    return document


def validate_remote_run_events_response(value: Mapping[str, Any]) -> None:
    document = _require_mapping(value, response=True)
    _validate_wrapper(
        document,
        _EVENTS_RESPONSE_FIELDS,
        CONNECTED_REMOTE_RUN_EVENTS_SCHEMA,
    )
    truncated = document["truncated"]
    if not isinstance(truncated, bool):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    if document["completeness"] != ("partial" if truncated else "complete"):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    events = document["events"]
    if not isinstance(events, list) or len(events) > MAX_REMOTE_RUN_EVENT_ITEMS:
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    if any(not isinstance(event, dict) for event in events):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _validate_bounded_json(events, response=True)


def project_remote_run_server_capabilities(
    version_doc: Mapping[str, Any],
    *,
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Allowlist deployed server release evidence and known Run capabilities."""

    document = _require_mapping(version_doc, response=True)
    _require_upstream_exact_fields(document, _VERSION_DOCUMENT_FIELDS)
    if (
        document.get("contract") != "version_authority/v1"
        or document.get("schema_version") != 1
        or document.get("component") != "server"
    ):
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)

    declared = _require_mapping(document.get("declared"), response=True)
    deployment = _require_mapping(document.get("deployment"), response=True)
    declared_state = declared.get("state")
    deployment_state = deployment.get("state")
    if declared_state not in {"present", "unknown"} or deployment_state not in {
        "present",
        "unknown",
    }:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)

    release_id: str | None = None
    version: str | None = None
    advertised: set[str] = set()
    capability_catalog_present = False
    declared_schema_target: int | None = None
    declared_source_ref: str | None = None
    declared_source_commit: str | None = None
    declared_artifact_sha256: str | None = None
    declared_manifest_sha256: str | None = None
    if declared_state == "present":
        _require_upstream_exact_fields(declared, _DECLARED_PRESENT_FIELDS)
        if (
            declared.get("reason_code") != "bundled_release_identity_present"
            or declared.get("schema_version") != 1
            or declared.get("component") != "server"
            or declared.get("evidence_state") != "declared"
        ):
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        release_id = _require_release_value(declared.get("release_id"))
        version = _require_release_version(declared.get("version"))
        declared_schema_target = _require_positive_safe_integer(declared.get("schema_target"))
        declared_source_ref = _require_bounded_text(declared.get("source_ref"))
        _require_bounded_text(declared.get("source_repository"))
        if declared.get("source_binding") != "pinned_commit":
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        declared_source_commit = _optional_hash(
            declared.get("source_commit"),
            _HEX_COMMIT,
        )
        declared_artifact_sha256 = _optional_hash(declared.get("artifact_sha256"), _HEX_64)
        declared_manifest_sha256 = _optional_hash(declared.get("release_manifest_sha256"), _HEX_64)
        contracts = declared.get("contracts")
        if not isinstance(contracts, Mapping):
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        raw_capabilities = contracts.get("daemon_server_capabilities")
        if raw_capabilities is None:
            raw_capabilities = []
        elif not isinstance(raw_capabilities, list):
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        else:
            capability_catalog_present = True
        if len(raw_capabilities) > 64:
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        if any(
            not isinstance(item, str) or len(item) > 128 or not _CAPABILITY_IDENTIFIER.fullmatch(item)
            for item in raw_capabilities
        ):
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        if len(set(raw_capabilities)) != len(raw_capabilities):
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        advertised = set(raw_capabilities)
    else:
        _require_upstream_exact_fields(declared, _DECLARED_UNKNOWN_FIELDS)
        if declared.get("reason_code") != "bundled_release_identity_invalid":
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)

    deployment_reason = deployment.get("reason_code")
    deployment_verified = deployment_state == "present"
    if deployment_verified:
        _require_upstream_exact_fields(deployment, _DEPLOYMENT_PRESENT_FIELDS)
        if (
            deployment_reason != "deployment_receipt_present"
            or deployment.get("schema_version") != 1
            or deployment.get("component") != "server"
        ):
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        deployed_release_id = _require_release_value(deployment.get("release_id"))
        deployed_version = _require_release_version(deployment.get("version"))
        deployed_source_ref = _require_bounded_text(deployment.get("source_ref"))
        deployed_source_commit = _require_hash(
            deployment.get("source_commit"),
            _HEX_COMMIT,
        )
        deployed_artifact_sha256 = _require_hash(deployment.get("artifact_sha256"), _HEX_64)
        deployed_manifest_sha256 = _require_hash(deployment.get("release_manifest_sha256"), _HEX_64)
        deployed_schema_target = _require_positive_safe_integer(deployment.get("schema_target"))
        _require_utc_timestamp(deployment.get("deployed_at"), response=True)
        if deployment.get("environment_class") not in {"staging", "production"}:
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        if (
            release_id is None
            or deployed_release_id != release_id
            or deployed_version != version
            or deployed_schema_target != declared_schema_target
            or deployed_source_ref != declared_source_ref
            or (declared_source_commit is not None and deployed_source_commit != declared_source_commit)
            or (declared_artifact_sha256 is not None and deployed_artifact_sha256 != declared_artifact_sha256)
            or (declared_manifest_sha256 is not None and deployed_manifest_sha256 != declared_manifest_sha256)
        ):
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    else:
        _require_upstream_exact_fields(deployment, _DEPLOYMENT_UNKNOWN_FIELDS)
        if not isinstance(deployment_reason, str) or not deployment_reason.startswith("deployment_receipt_"):
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)

    database_schema = _require_mapping(document.get("database_schema"), response=True)
    _require_upstream_exact_fields(database_schema, frozenset({"current", "target"}))
    database_current = _require_nonnegative_safe_integer(database_schema.get("current"))
    database_target = _require_positive_safe_integer(database_schema.get("target"))
    if declared_schema_target is not None and database_target != declared_schema_target:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    schema_is_current = database_current == database_target

    if deployment_verified:
        release_state = "verified"
        release_evidence = "deployment_receipt"
        release_reason = None
    else:
        release_state = "unverified"
        release_evidence = "declared_identity" if release_id is not None else "unavailable"
        release_reason = str(deployment_reason)

    capabilities: dict[str, dict[str, Any]] = {}
    for capability_id in SERVER_REMOTE_CAPABILITY_IDS:
        if not deployment_verified:
            capabilities[capability_id] = {
                "state": "unavailable",
                "reason": "deployment_evidence_unavailable",
            }
        elif not schema_is_current:
            capabilities[capability_id] = {
                "state": "unavailable",
                "reason": "server_schema_not_current",
            }
        elif not capability_catalog_present:
            capabilities[capability_id] = {
                "state": "unavailable",
                "reason": "server_capability_evidence_absent",
            }
        elif capability_id in advertised:
            capabilities[capability_id] = {"state": "supported", "reason": None}
        else:
            capabilities[capability_id] = {
                "state": "unsupported",
                "reason": "server_capability_not_advertised",
            }

    projected = {
        "schema": CONNECTED_SERVER_CAPABILITIES_SCHEMA,
        "schema_version": CONNECTED_SCHEMA_VERSION,
        "observed_at": _utc_now() if observed_at is None else observed_at,
        "release": {
            "state": release_state,
            "release_id": release_id,
            "version": version,
            "evidence": release_evidence,
            "reason": release_reason,
        },
        "capabilities": capabilities,
    }
    validate_remote_run_server_capabilities(projected)
    return projected


def validate_remote_run_server_capabilities(value: Mapping[str, Any]) -> None:
    document = _require_mapping(value, response=True)
    _require_exact_fields(document, _CAPABILITIES_RESPONSE_FIELDS, response=True)
    if document["schema"] != CONNECTED_SERVER_CAPABILITIES_SCHEMA:
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _require_schema_version(document["schema_version"], response=True)
    _require_utc_milliseconds(document["observed_at"], response=True)
    release = _require_mapping(document["release"], response=True)
    _require_exact_fields(release, _CAPABILITIES_RELEASE_FIELDS, response=True)
    state = release["state"]
    if state not in {"verified", "unverified"}:
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    for field, validator in (
        ("release_id", _require_release_value),
        ("version", _require_release_version),
    ):
        raw_value = release[field]
        if raw_value is not None:
            validator(raw_value)
    if state == "verified":
        if (
            release["release_id"] is None
            or release["version"] is None
            or release["evidence"] != "deployment_receipt"
            or release["reason"] is not None
        ):
            _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    elif (
        release["evidence"] not in {"declared_identity", "unavailable"}
        or not isinstance(release["reason"], str)
        or len(release["reason"]) > 128
    ):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)

    capabilities = _require_mapping(document["capabilities"], response=True)
    if set(capabilities) != set(SERVER_REMOTE_CAPABILITY_IDS):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    for raw_capability in capabilities.values():
        capability = _require_mapping(raw_capability, response=True)
        _require_exact_fields(capability, _CAPABILITY_FIELDS, response=True)
        capability_state = capability["state"]
        reason = capability["reason"]
        if capability_state == "supported":
            if state != "verified" or reason is not None:
                _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
        elif capability_state in {"unsupported", "unavailable"}:
            if not isinstance(reason, str) or len(reason) > 128:
                _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
        else:
            _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)


def build_remote_run_preflight_response(
    preflight: dict[str, Any],
    server_capabilities: Mapping[str, Any],
    expected_request: Mapping[str, Any],
    *,
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Bind one non-reserving central preflight to observed server evidence."""

    validate_central_preflight_response(
        preflight,
        expected_request=expected_request,
    )
    validate_remote_run_server_capabilities(server_capabilities)
    document = {
        "schema": CONNECTED_REMOTE_RUN_PREFLIGHT_SCHEMA,
        "schema_version": CONNECTED_SCHEMA_VERSION,
        "source": "server",
        "observed_at": _utc_now() if observed_at is None else observed_at,
        "server_capabilities": dict(server_capabilities),
        "preflight": preflight,
    }
    validate_remote_run_preflight_response(document)
    return document


def validate_remote_run_preflight_response(value: Mapping[str, Any]) -> None:
    document = _require_mapping(value, response=True)
    _require_exact_fields(document, _PREFLIGHT_RESPONSE_FIELDS, response=True)
    if document["schema"] != CONNECTED_REMOTE_RUN_PREFLIGHT_SCHEMA:
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _require_schema_version(document["schema_version"], response=True)
    if document["source"] != "server":
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _require_utc_milliseconds(document["observed_at"], response=True)
    validate_remote_run_server_capabilities(_require_mapping(document["server_capabilities"], response=True))
    validate_central_preflight_response(_require_mapping(document["preflight"], response=True))


def validate_central_preflight_response(
    value: Mapping[str, Any],
    *,
    expected_request: Mapping[str, Any] | None = None,
) -> None:
    """Require the central operation to be v1, point-in-time and non-reserving."""

    preflight = _require_mapping(value, response=True)
    _require_upstream_exact_fields(preflight, _CENTRAL_PREFLIGHT_FIELDS)
    _validate_bounded_json(preflight, response=True)
    if (
        preflight.get("contract") != "execution_preflight"
        or preflight.get("schema_version") != 1
        or preflight.get("point_in_time") is not True
        or preflight.get("reservation") is not False
        or not isinstance(preflight.get("can_queue"), bool)
    ):
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    _require_utc_timestamp(preflight.get("checked_at"), response=True)
    resolved = _require_mapping(preflight.get("resolved_request"), response=True)
    _require_upstream_exact_fields(resolved, _CENTRAL_PREFLIGHT_RESOLVED_FIELDS)
    _require_identifier(resolved.get("version_id"), response=True)
    target_machine_id = resolved.get("target_machine_id")
    if target_machine_id is not None:
        _require_identifier(target_machine_id, response=True)
    if resolved.get("offline_policy") not in {"queue", "wait", "fail_fast"}:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    raw_checks = preflight.get("checks")
    if not isinstance(raw_checks, list) or not raw_checks or len(raw_checks) > 64:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    has_block = False
    for raw_check in raw_checks:
        check = _require_mapping(raw_check, response=True)
        _require_upstream_exact_fields(check, _CENTRAL_PREFLIGHT_CHECK_FIELDS)
        _require_machine_code(check.get("code"))
        _require_machine_code(check.get("reason_code"))
        if check.get("outcome") not in {"pass", "warning", "block", "unknown"}:
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
        has_block = has_block or check["outcome"] == "block"
        _require_bounded_text(check.get("explanation"))
        evidence_at = check.get("evidence_at")
        if evidence_at is not None:
            _require_utc_timestamp(evidence_at, response=True)
    if preflight["can_queue"] is has_block:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)

    if expected_request is not None:
        _validate_preflight_binding(preflight, expected_request)


def _validate_preflight_binding(
    preflight: Mapping[str, Any],
    expected_request: Mapping[str, Any],
) -> None:
    """Bind central evidence to the exact closed daemon request."""

    request = _require_mapping(expected_request, response=True)
    if not _PREFLIGHT_REQUIRED_WIRE_FIELDS <= set(request) or not set(request) <= _PREFLIGHT_WIRE_FIELDS:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    resolved = _require_mapping(preflight["resolved_request"], response=True)
    for field in ("version_id", "target_machine_id", "offline_policy"):
        if resolved.get(field) != request.get(field):
            _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)

    resolved_object = _require_mapping(preflight.get("object"), response=True)
    object_ref = request.get("object")
    if object_ref not in {resolved_object.get("id"), resolved_object.get("name")}:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    if (
        resolved_object.get("owner_id") != request.get("object_owner_id")
        or resolved_object.get("library_slug") != request.get("library")
        or resolved_object.get("version_id") != request.get("version_id")
    ):
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    if request.get("version") is not None and resolved_object.get("version") != request["version"]:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    if request.get("function") is not None and resolved_object.get("entrypoint") != request["function"]:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)


def connected_error_document(
    code: str,
    operation: str,
    *,
    retryable: bool = False,
) -> dict[str, Any]:
    """Return a closed error without raw central prose or transport details."""

    document = {
        "schema": CONNECTED_ERROR_SCHEMA,
        "schema_version": CONNECTED_SCHEMA_VERSION,
        "code": _require_machine_code(code),
        "operation": _require_machine_code(operation),
        "retryable": bool(retryable),
    }
    validate_connected_error_document(document)
    return document


def validate_connected_error_document(value: Mapping[str, Any]) -> None:
    document = _require_mapping(value, response=True)
    _require_exact_fields(document, _ERROR_FIELDS, response=True)
    if document["schema"] != CONNECTED_ERROR_SCHEMA:
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _require_schema_version(document["schema_version"], response=True)
    _require_machine_code(document["code"])
    _require_machine_code(document["operation"])
    if not isinstance(document["retryable"], bool):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)


def connected_request_path(path: str) -> bool:
    """Return whether one route requires no-store handling."""

    return path in {
        CONNECTED_CREDENTIALS_REVEAL_ROUTE,
        CONNECTED_SERVER_CAPABILITIES_ROUTE,
        CONNECTED_REMOTE_RUNS_ROUTE,
        CONNECTED_REMOTE_RUN_PREFLIGHT_ROUTE,
    } or path.startswith(f"{CONNECTED_REMOTE_RUNS_ROUTE}/")


def connected_limited_post_path(path: str) -> bool:
    return path in {
        CONNECTED_CREDENTIALS_REVEAL_ROUTE,
        CONNECTED_REMOTE_RUN_PREFLIGHT_ROUTE,
    }


def install_connected_request_limit(app: Any) -> None:
    """Apply the closed body cap while receiving either Connected POST body."""

    base_request_class = app.request_class
    if getattr(base_request_class, "_spl_connected_ide_limit", False):
        return

    class ConnectedIDERequest(base_request_class):  # type: ignore[misc, valid-type]
        _spl_connected_ide_limit = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            method = args[0] if args else kwargs.get("method")
            path = args[2] if len(args) > 2 else kwargs.get("path")
            if method == "POST" and isinstance(path, str) and connected_limited_post_path(path):
                configured_limit = kwargs.get("max_content_length")
                kwargs["max_content_length"] = (
                    MAX_CONNECTED_BODY_BYTES
                    if configured_limit is None
                    else min(int(configured_limit), MAX_CONNECTED_BODY_BYTES)
                )
            super().__init__(*args, **kwargs)

    app.request_class = ConnectedIDERequest


def validate_connected_server_url(value: Any, *, response: bool = False) -> str:
    """Validate a proven central origin without credentials or URL state."""

    if not isinstance(value, str) or not value or len(value) > MAX_SERVER_URL_LENGTH:
        _fail_for_context(response)
    if any(ord(character) < 32 or character.isspace() for character in value):
        _fail_for_context(response)
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        _fail_for_context(response)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        _fail_for_context(response)
    if port is not None and not 1 <= port <= 65_535:
        _fail_for_context(response)
    if parsed.scheme == "http" and not _is_loopback_host(parsed.hostname):
        _fail_for_context(response)
    return value.rstrip("/")


def _is_loopback_host(host: str) -> bool:
    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _parse_closed_document(raw: bytes) -> dict[str, Any]:
    if len(raw) > MAX_CONNECTED_BODY_BYTES:
        _fail("body_too_large", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        _fail("malformed_json")
    try:
        document = json.loads(
            text,
            object_pairs_hook=_closed_json_object,
            parse_constant=lambda _value: _fail("malformed_json"),
        )
    except ConnectedIDEContractError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError):
        _fail("malformed_json")
    if not isinstance(document, dict):
        _fail("request_invalid")
    _validate_bounded_json(document)
    return document


def _closed_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail("request_invalid")
        result[key] = value
    return result


def _validate_wrapper(
    document: Mapping[str, Any],
    fields: frozenset[str],
    schema: str,
) -> None:
    _require_exact_fields(document, fields, response=True)
    if document["schema"] != schema or document["source"] != "server":
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    _require_schema_version(document["schema_version"], response=True)
    _require_utc_milliseconds(document["observed_at"], response=True)


def _validate_bounded_json(value: Any, *, response: bool = False) -> None:
    pending: list[tuple[Any, int]] = [(value, 1)]
    container_items = 0
    while pending:
        current, depth = pending.pop()
        if depth > MAX_JSON_DEPTH:
            _fail_for_context(response)
        if current is None or isinstance(current, bool) or isinstance(current, int):
            continue
        if isinstance(current, float):
            if not math.isfinite(current):
                _fail_for_context(response)
            continue
        if isinstance(current, str):
            if len(current) > MAX_JSON_STRING_LENGTH:
                _fail_for_context(response)
            continue
        if isinstance(current, list):
            container_items += len(current)
            pending.extend((item, depth + 1) for item in current)
        elif isinstance(current, dict):
            container_items += len(current)
            for key, item in current.items():
                if not isinstance(key, str) or len(key) > 256:
                    _fail_for_context(response)
                pending.append((item, depth + 1))
        else:
            _fail_for_context(response)
        if container_items > MAX_JSON_CONTAINER_ITEMS:
            _fail_for_context(response)


def _require_mapping(value: Any, *, response: bool = False) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail_for_context(response)
    return value


def _require_exact_fields(
    value: Mapping[str, Any],
    expected: frozenset[str],
    *,
    response: bool = False,
) -> None:
    if set(value) != expected:
        _fail_for_context(response)


def _require_schema_version(value: Any, *, response: bool = False) -> None:
    if isinstance(value, bool) or value != CONNECTED_SCHEMA_VERSION:
        _fail_for_context(response)


def _require_identifier(value: Any, *, response: bool = False) -> str:
    if not isinstance(value, str) or not _SAFE_IDENTIFIER.fullmatch(value):
        _fail_for_context(response)
    return value


def _require_secret(value: Any, *, response: bool = False) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_SECRET_LENGTH
        or any(ord(character) < 32 for character in value)
    ):
        _fail_for_context(response)
    return value


def _require_release_value(value: Any) -> str:
    if not isinstance(value, str) or not _SAFE_IDENTIFIER.fullmatch(value):
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    return value


def _require_release_version(value: Any) -> str:
    if not isinstance(value, str) or not _SAFE_VERSION.fullmatch(value):
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    return value


def _require_bounded_text(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 256 or any(ord(character) < 32 for character in value):
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    return value


def _require_hash(value: Any, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    return value


def _optional_hash(value: Any, pattern: re.Pattern[str]) -> str | None:
    if value is None:
        return None
    return _require_hash(value, pattern)


def _require_positive_safe_integer(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 9_007_199_254_740_991:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    return value


def _require_nonnegative_safe_integer(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 9_007_199_254_740_991:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)
    return value


def _require_upstream_exact_fields(
    value: Mapping[str, Any],
    expected: frozenset[str],
) -> None:
    if set(value) != expected:
        _fail("connected_protocol_incompatible", HTTPStatus.BAD_GATEWAY)


def _require_machine_code(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 80 or not re.fullmatch(r"^[a-z][a-z0-9_]*$", value):
        _fail("server_response_invalid", HTTPStatus.BAD_GATEWAY)
    return value


def _require_utc_milliseconds(value: Any, *, response: bool = False) -> str:
    if not isinstance(value, str) or not _UTC_MILLISECONDS.fullmatch(value):
        _fail_for_context(response)
    return _require_utc_timestamp(value, response=response)


def _require_utc_timestamp(value: Any, *, response: bool = False) -> str:
    if not isinstance(value, str) or not value or len(value) > 64:
        _fail_for_context(response)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail_for_context(response)
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        _fail_for_context(response)
    return value


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _fail_for_context(response: bool) -> NoReturn:
    _fail(
        "server_response_invalid" if response else "request_invalid",
        HTTPStatus.BAD_GATEWAY if response else HTTPStatus.BAD_REQUEST,
    )


def _fail(
    code: str,
    status: HTTPStatus = HTTPStatus.BAD_REQUEST,
    *,
    retryable: bool = False,
) -> NoReturn:
    raise ConnectedIDEContractError(code, status, retryable=retryable)


__all__ = [
    "CONNECTED_CREDENTIALS_REVEAL_REQUEST_SCHEMA",
    "CONNECTED_CREDENTIALS_REVEAL_ROUTE",
    "CONNECTED_CREDENTIALS_REVEAL_SCHEMA",
    "CONNECTED_ERROR_SCHEMA",
    "CONNECTED_REMOTE_RUN_DETAIL_SCHEMA",
    "CONNECTED_REMOTE_RUN_EVENTS_SCHEMA",
    "CONNECTED_REMOTE_RUN_LIST_SCHEMA",
    "CONNECTED_REMOTE_RUN_PREFLIGHT_REQUEST_SCHEMA",
    "CONNECTED_REMOTE_RUN_PREFLIGHT_ROUTE",
    "CONNECTED_REMOTE_RUN_PREFLIGHT_SCHEMA",
    "CONNECTED_REMOTE_RUNS_ROUTE",
    "CONNECTED_SCHEMA_VERSION",
    "CONNECTED_SERVER_CAPABILITIES_ROUTE",
    "CONNECTED_SERVER_CAPABILITIES_SCHEMA",
    "ConnectedCredentialsRevealRequest",
    "ConnectedIDEContractError",
    "MAX_CONNECTED_BODY_BYTES",
    "MAX_REMOTE_RUN_EVENT_ITEMS",
    "MAX_REMOTE_RUN_ITEMS",
    "MAX_UPSTREAM_RESPONSE_BYTES",
    "RemoteRunPreflightRequest",
    "SERVER_REMOTE_CAPABILITY_IDS",
    "build_connected_credentials_reveal_response",
    "build_remote_run_detail_response",
    "build_remote_run_events_response",
    "build_remote_run_list_response",
    "build_remote_run_preflight_response",
    "connected_error_document",
    "connected_limited_post_path",
    "connected_request_path",
    "install_connected_request_limit",
    "parse_connected_credentials_reveal_request",
    "parse_remote_run_preflight_request",
    "project_remote_run_server_capabilities",
    "validate_central_preflight_response",
    "validate_connected_credentials_reveal_response",
    "validate_connected_error_document",
    "validate_connected_server_url",
    "validate_remote_run_detail_response",
    "validate_remote_run_events_response",
    "validate_remote_run_list_response",
    "validate_remote_run_preflight_response",
    "validate_remote_run_server_capabilities",
]
