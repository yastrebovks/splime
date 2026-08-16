from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib
from http import HTTPStatus
import json
from pathlib import Path
import time
from typing import Any
from urllib.error import URLError

import pytest

from spl.daemon.ai_preview import (
    AI_PREVIEW_CAPABILITIES_SCHEMA,
    AI_PREVIEW_CAPABILITY_TIMEOUT_SECONDS,
    AI_PREVIEW_ERROR_SCHEMA,
    AI_PREVIEW_REQUEST_SCHEMA,
    AI_PREVIEW_RESULT_SCHEMA,
    AI_PREVIEW_SERVER_TIMEOUT_SECONDS,
    AIPreviewContractError,
    CENTRAL_AI_PREVIEW_ERROR_SEMANTICS,
    ai_preview_error_document,
    parse_ai_preview_request,
    validate_ai_preview_capabilities,
    validate_ai_preview_error,
    validate_ai_preview_request,
    validate_ai_preview_result,
)
from spl.daemon import remote_client
from spl.daemon.remote_client import (
    ServerClient,
    ServerClientError,
    _ai_preview_error_response,
)
from spl.daemon.routes import ai_preview as ai_preview_routes
from spl.daemon.routes.ai_preview import _central_error_semantics
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()


def _hash(domain: str, value: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain.encode("ascii"))
    digest.update(b"\0")
    digest.update(value)
    return f"sha256:{digest.hexdigest()}"


def valid_capabilities() -> dict[str, Any]:
    document = {
        "schema": AI_PREVIEW_CAPABILITIES_SCHEMA,
        "schema_version": 1,
        "observed_at": "2026-08-03T10:00:00.000Z",
        "available": True,
        "reason": None,
        "provider_display_name": "OpenAI",
        "model_policy_label": "Server-selected AI Preview model",
        "operation": "notebook_pipeline_preview",
        "request_schema": AI_PREVIEW_REQUEST_SCHEMA,
        "result_schema": AI_PREVIEW_RESULT_SCHEMA,
        "max_request_bytes": 4 * 1024 * 1024,
        "max_cells": 512,
        "max_cell_bytes": 262_144,
        "max_total_source_bytes": 1_500_000,
        "storage_disclosure": "Provider storage is disabled for this operation.",
        "cancellation_disclosure": "Local cancellation only discards late delivery.",
    }
    stable = dict(document)
    stable.pop("observed_at")
    document["capability_hash"] = _hash(
        "spl.ai-preview.capability/v1",
        _canonical(stable),
    )
    return document


def valid_request(
    capability: dict[str, Any] | None = None,
    *,
    source: str = "x = load_input_data()\n",
    invalid_excluded_syntax: bool = False,
) -> dict[str, Any]:
    snapshot_hash = f"sha256:{'1' * 64}"
    included = [
        {
            "cell_id": "cell-1",
            "ordinal": 0,
            "kind": "code",
            "source": source,
            "source_hash": _hash(
                "splime.ide.notebook-cell-source/v1",
                source.encode(),
            ),
        }
    ]
    excluded = [
        {
            "cell_id": "cell-2",
            "ordinal": 1,
            "reason": "invalid_syntax" if invalid_excluded_syntax else "non_code",
        }
    ]
    facts_body = {
        "snapshot_id": "snapshot-1",
        "snapshot_hash": snapshot_hash,
        "validity": "partial" if invalid_excluded_syntax else "valid",
        "syntax_issues": (
            [
                {
                    "cell_id": "cell-2",
                    "cell_ordinal": 1,
                    "fact_ordinal": 0,
                    "line": 1,
                    "column": 4,
                    "code": "python_syntax_error",
                    "message": "Python syntax is invalid in this cell.",
                }
            ]
            if invalid_excluded_syntax
            else []
        ),
        "imports": [],
        "definitions": [
            {
                "cell_id": "cell-1",
                "cell_ordinal": 0,
                "fact_ordinal": 0,
                "name": "x",
                "kind": "assignment",
                "signature": None,
            }
        ],
        "definition_use_edges": [],
        "effects": [],
        "findings": [],
        "runtime_inspected": False,
        "kernel_queried": False,
    }
    facts_hash = _hash("splime.ide.analysis-facts/v1", _canonical(facts_body))
    facts = {
        "schema": "splime.ide.analysis-facts",
        "schema_version": 1,
        "facts_id": "facts-1",
        "facts_hash": facts_hash,
        **facts_body,
    }
    capability = capability or valid_capabilities()
    request: dict[str, Any] = {
        "schema": AI_PREVIEW_REQUEST_SCHEMA,
        "schema_version": 1,
        "client_request_id": "request-1",
        "document_id": "document-1",
        "document_version": "document-version-1",
        "document_revision": 1,
        "snapshot_id": "snapshot-1",
        "snapshot_hash": snapshot_hash,
        "facts_id": "facts-1",
        "facts_hash": facts_hash,
        "included_cells": included,
        "excluded_cells": excluded,
        "facts": facts,
        "outputs_included": False,
        "notebook_metadata_included": False,
        "runtime_inspected": False,
        "kernel_queried": False,
    }
    exclusion_hash = _hash(
        "spl.ai-preview.exclusions/v1",
        _canonical(excluded),
    )
    transfer = {
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
                "cell_id": item["cell_id"],
                "ordinal": item["ordinal"],
                "kind": item["kind"],
                "source_hash": item["source_hash"],
            }
            for item in included
        ],
        "excluded_cells": excluded,
        "capability_hash": capability["capability_hash"],
    }
    consent: dict[str, Any] = {
        "consent_id": "consent-1",
        "granted_at": "2026-08-03T10:00:01.000Z",
        "capability_observed_at": capability["observed_at"],
        "capability_hash": capability["capability_hash"],
        "included_cell_ids": ["cell-1"],
        "exclusion_hash": exclusion_hash,
        "transfer_hash": _hash(
            "spl.ai-preview.transfer/v1",
            _canonical(transfer),
        ),
        "confirmed": True,
    }
    consent["consent_hash"] = _hash(
        "spl.ai-preview.consent/v1",
        _canonical(consent),
    )
    request["consent"] = consent
    return request


def valid_result(request: dict[str, Any] | None = None) -> dict[str, Any]:
    request = request or valid_request()
    return {
        "schema": AI_PREVIEW_RESULT_SCHEMA,
        "schema_version": 1,
        "client_request_id": request["client_request_id"],
        "snapshot_id": request["snapshot_id"],
        "snapshot_hash": request["snapshot_hash"],
        "facts_id": request["facts_id"],
        "facts_hash": request["facts_hash"],
        "consent_id": request["consent"]["consent_id"],
        "consent_hash": request["consent"]["consent_hash"],
        "provider_display_name": "OpenAI",
        "model_policy_label": "Server-selected AI Preview model",
        "proposal": {
            "schema": "splime.ide.splime-proposal",
            "schema_version": 1,
            "proposal_id": request["client_request_id"],
            "snapshot_id": request["snapshot_id"],
            "snapshot_hash": request["snapshot_hash"],
            "facts_id": request["facts_id"],
            "facts_hash": request["facts_hash"],
            "mode": "central_ai",
            "title": "Suggested Pipeline",
            "nodes": [
                {
                    "node_id": "node-1",
                    "ordinal": 0,
                    "name": "Load data",
                    "kind": "source",
                    "cell_ids": ["cell-1"],
                    "claim": {
                        "summary": "The cell defines the first step.",
                        "confidence": "high",
                        "basis": "provider_inference",
                        "fact_ids": ["fact:definitions:0"],
                    },
                }
            ],
            "edges": [],
            "alternatives": [],
            "warnings": [],
            "excluded_cell_ids": ["cell-2"],
            "unsupported_constructs": [],
            "publish_ready": False,
            "allowed_edits": ["rename", "exclude"],
            "generated_at": "2026-08-03T10:00:02.000Z",
            "provenance": {
                "producer": "spl_server",
                "release_id": "splime-0.4.6",
                "observed_at": "2026-08-03T10:00:02.000Z",
                "protocol_selected": 1,
            },
        },
        "usage": {
            "input_tokens": 10,
            "output_tokens": 20,
            "total_tokens": 30,
        },
        "publish_ready": False,
    }


def test_capability_request_and_result_contracts_accept_exact_documents() -> None:
    capabilities = valid_capabilities()
    request = valid_request()
    result = valid_result(request)

    validate_ai_preview_capabilities(capabilities)
    validate_ai_preview_request(request)
    parsed = parse_ai_preview_request(json.dumps(request).encode())
    validate_ai_preview_result(result, request=parsed.document)


def test_request_accepts_syntax_fact_for_explicitly_excluded_invalid_cell() -> None:
    capabilities = valid_capabilities()
    request = valid_request(
        capabilities,
        invalid_excluded_syntax=True,
    )

    validate_ai_preview_request(request, current_capabilities=capabilities)


@pytest.mark.parametrize(
    ("mutation", "validator"),
    [
        (lambda value: value.update(provider_url="https://forbidden.example"), validate_ai_preview_capabilities),
        (lambda value: value.update(api_key="provider-secret"), validate_ai_preview_request),
        (lambda value: value.update(raw_response={}), validate_ai_preview_result),
    ],
)
def test_contracts_reject_unknown_fields(mutation: Any, validator: Any) -> None:
    if validator is validate_ai_preview_capabilities:
        document = valid_capabilities()
    elif validator is validate_ai_preview_request:
        document = valid_request()
    else:
        document = valid_result()
    mutation(document)
    with pytest.raises(AIPreviewContractError):
        validator(document)


def test_request_rejects_tampered_source_facts_and_consent_hashes() -> None:
    for path in ("source", "facts", "consent"):
        request = valid_request()
        if path == "source":
            request["included_cells"][0]["source"] = "x = 2\n"
        elif path == "facts":
            request["facts"]["definitions"][0]["name"] = "y"
        else:
            request["consent"]["transfer_hash"] = f"sha256:{'9' * 64}"
        with pytest.raises(AIPreviewContractError) as captured:
            validate_ai_preview_request(request)
        assert captured.value.code == "invalid_request"


def test_capability_hash_excludes_observation_time_but_binds_policy() -> None:
    capabilities = valid_capabilities()
    changed_time = deepcopy(capabilities)
    changed_time["observed_at"] = "2026-08-03T10:01:00.000Z"
    validate_ai_preview_capabilities(changed_time)

    changed_policy = deepcopy(capabilities)
    changed_policy["model_policy_label"] = "Different policy"
    with pytest.raises(AIPreviewContractError):
        validate_ai_preview_capabilities(changed_policy)


def test_result_requires_request_binding_and_provider_neutral_truth() -> None:
    request = valid_request()
    capabilities = valid_capabilities()
    for mutate in (
        lambda result: result.update(snapshot_hash=f"sha256:{'8' * 64}"),
        lambda result: result["proposal"].update(mode="openai_remote"),
        lambda result: result["proposal"]["nodes"][0]["claim"].update(basis="openai_inference"),
        lambda result: result["proposal"]["provenance"].update(producer="jupyter_companion"),
        lambda result: result["proposal"].update(title="https://forbidden.example"),
        lambda result: result["proposal"]["nodes"][0]["claim"].update(
            summary=request["included_cells"][0]["source"].strip()
        ),
        lambda result: result.update(publish_ready=True),
        lambda result: result.update(provider_display_name="Different provider"),
        lambda result: result.update(model_policy_label="Different model policy"),
    ):
        result = valid_result(request)
        mutate(result)
        with pytest.raises(AIPreviewContractError) as captured:
            validate_ai_preview_result(
                result,
                request=request,
                current_capabilities=capabilities,
            )
        assert captured.value.code == "ai_preview_protocol_invalid"


def test_result_rejects_short_exact_source_echo() -> None:
    source_sentinel = "UNIQUECODE7"
    request = valid_request(source=f"{source_sentinel}\n")
    result = valid_result(request)
    result["proposal"]["title"] = source_sentinel

    with pytest.raises(AIPreviewContractError) as captured:
        validate_ai_preview_result(
            result,
            request=request,
            current_capabilities=valid_capabilities(),
        )

    assert captured.value.code == "ai_preview_protocol_invalid"


def test_safe_error_is_closed_and_contains_no_upstream_payload() -> None:
    document = ai_preview_error_document(
        "provider_timeout",
        safe_message="The AI provider did not complete within the bounded time.",
        retryable=False,
        outcome="unknown",
        correlation_id="ai-correlation-1",
    )
    assert document == {
        "schema": AI_PREVIEW_ERROR_SCHEMA,
        "schema_version": 1,
        "code": "provider_timeout",
        "safe_message": "The AI provider did not complete within the bounded time.",
        "retryable": False,
        "outcome": "unknown",
        "correlation_id": "ai-correlation-1",
    }
    assert "traceback" not in json.dumps(document)


def test_remote_client_accepts_only_closed_ai_preview_error_envelopes() -> None:
    valid = ai_preview_error_document(
        "provider_unavailable",
        safe_message="The configured AI provider is unavailable.",
        retryable=False,
        outcome="not_started",
        correlation_id="ai-correlation-1",
    )
    assert _ai_preview_error_response(json.dumps(valid)) == (
        valid["safe_message"],
        "provider_unavailable",
        None,
    )

    adversarial = dict(valid)
    adversarial["raw_provider_error"] = "secret upstream prose"
    assert _ai_preview_error_response(json.dumps(adversarial)) == (
        "central SPL daemon server returned an invalid AI Preview error envelope",
        "ai_preview_protocol_invalid",
        None,
    )
    duplicate = json.dumps(valid)[:-1] + ',"code":"provider_failed"}'
    assert _ai_preview_error_response(duplicate)[1] == "ai_preview_protocol_invalid"


@pytest.mark.parametrize(
    ("code", "status", "retryable", "outcome"),
    [
        ("ai_preview_disabled", HTTPStatus.SERVICE_UNAVAILABLE, False, "not_started"),
        ("provider_unavailable", HTTPStatus.SERVICE_UNAVAILABLE, False, "not_started"),
        ("central_credential_rejected", HTTPStatus.UNAUTHORIZED, False, "not_started"),
        ("permission_denied", HTTPStatus.FORBIDDEN, False, "not_started"),
        ("rate_limited", HTTPStatus.TOO_MANY_REQUESTS, True, "not_started"),
        ("invalid_request", HTTPStatus.BAD_REQUEST, False, "not_started"),
        ("provider_timeout", HTTPStatus.GATEWAY_TIMEOUT, False, "unknown"),
        ("provider_refused", HTTPStatus.UNPROCESSABLE_ENTITY, False, "not_completed"),
        ("provider_failed", HTTPStatus.BAD_GATEWAY, False, "unknown"),
        ("provider_result_invalid", HTTPStatus.BAD_GATEWAY, False, "not_completed"),
    ],
)
def test_central_error_semantics_are_closed_and_preserved(
    code: str,
    status: HTTPStatus,
    retryable: bool,
    outcome: str,
) -> None:
    assert CENTRAL_AI_PREVIEW_ERROR_SEMANTICS[code] == (retryable, outcome)
    assert _central_error_semantics(code, int(status)) == (
        code,
        status,
        retryable,
        outcome,
    )
    document = ai_preview_error_document(
        code,
        safe_message="The operation did not complete.",
        retryable=retryable,
        outcome=outcome,
        correlation_id="ai-correlation-1",
    )
    validate_ai_preview_error(document)

    contradictory = dict(document)
    contradictory["retryable"] = not retryable
    with pytest.raises(AIPreviewContractError):
        validate_ai_preview_error(contradictory)


def _all_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [text for item in value for text in _all_strings(item)]
    if isinstance(value, dict):
        return [text for item in value.values() for text in _all_strings(item)]
    return []


class RecordingAIPreviewServer:
    def __init__(self) -> None:
        self.capability_calls = 0
        self.preview_calls: list[dict[str, Any]] = []
        self.capabilities = valid_capabilities()
        self.capability_error: Exception | None = None
        self.preview_error: Exception | None = None

    def ai_preview_request_contains_configured_credential(
        self,
        payload: dict[str, Any],
    ) -> bool:
        return any(
            credential in value
            for value in _all_strings(payload)
            for credential in ("machine-central-secret", "user-central-secret")
        )

    def get_ai_preview_capabilities(self) -> dict[str, Any]:
        self.capability_calls += 1
        if self.capability_error is not None:
            raise self.capability_error
        return deepcopy(self.capabilities)

    def create_ai_preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.preview_calls.append(deepcopy(payload))
        if self.preview_error is not None:
            raise self.preview_error
        return valid_result(payload)


@pytest.fixture
def ai_preview_app(tmp_path: Path) -> Any:
    store = RegistryStore(tmp_path)
    store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-central-secret",
        user_token="user-central-secret",
        connection={
            "id": "remote-connection-1",
            "owner_id": "owner-1",
            "subject_type": "machine",
            "subject_id": "machine-1",
            "machine_id": "machine-1",
            "display_name": "lab-machine",
            "status": "connected",
            "capabilities": {},
        },
        heartbeat_interval_seconds=60,
    )
    app = create_app(
        store,
        api_token="daemon-master-token",
        auto_build_envs=False,
    )
    fake = RecordingAIPreviewServer()
    timeout_calls: list[float] = []
    credentials = store.current_server_connection_credentials()
    assert credentials is not None
    app.runtime._require_live_server_channel_credentials = lambda: credentials

    def server_client(*args: Any, **kwargs: Any) -> RecordingAIPreviewServer:
        del args
        timeout_calls.append(kwargs["request_timeout_seconds"])
        return fake

    app.runtime._server_client_for_credentials = server_client
    app.ai_preview_test_server = fake
    app.ai_preview_timeout_calls = timeout_calls
    try:
        yield app
    finally:
        app.runtime.shutdown()
        store.close()


async def _http_request(
    app: Any,
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    authenticated: bool = True,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    headers = {"Authorization": f"Bearer {app.api_token}"} if authenticated else {}
    client = app.test_client()
    if method == "GET":
        response = await client.get(path, headers=headers)
    else:
        response = await client.post(path, json=payload, headers=headers)
    body = await response.get_json()
    return (
        response.status_code,
        body,
        {key.casefold(): value for key, value in response.headers.items()},
    )


def test_daemon_routes_are_authenticated_closed_no_store_and_nonpersisting(
    ai_preview_app: Any,
) -> None:
    request = valid_request()
    before_runs = ai_preview_app.runtime.store.list_runs()
    capabilities = asyncio.run(
        _http_request(
            ai_preview_app,
            "GET",
            "/server/ai/preview/capabilities",
        )
    )
    created = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=request,
        )
    )
    unauthenticated = asyncio.run(
        _http_request(
            ai_preview_app,
            "GET",
            "/server/ai/preview/capabilities",
            authenticated=False,
        )
    )

    assert capabilities[0] == created[0] == 200
    assert capabilities[1] == valid_capabilities()
    assert created[1] == valid_result(request)
    assert ai_preview_app.ai_preview_test_server.capability_calls == 2
    assert ai_preview_app.ai_preview_test_server.preview_calls == [request]
    assert ai_preview_app.ai_preview_timeout_calls.count(AI_PREVIEW_CAPABILITY_TIMEOUT_SECONDS) == 2
    assert ai_preview_app.ai_preview_timeout_calls.count(AI_PREVIEW_SERVER_TIMEOUT_SECONDS) == 1
    assert ai_preview_app.runtime.store.list_runs() == before_runs == []
    assert request["included_cells"][0]["source"] not in json.dumps(created[1])
    assert "central-secret" not in json.dumps(capabilities[1]) + json.dumps(created[1])
    assert capabilities[2]["cache-control"] == created[2]["cache-control"] == "no-store"
    assert unauthenticated[0] == 401
    assert unauthenticated[2]["cache-control"] == "no-store"


def test_daemon_route_rejects_unknown_request_before_any_upstream_call(
    ai_preview_app: Any,
) -> None:
    request = valid_request()
    request["provider_url"] = "https://forbidden.example"

    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=request,
        )
    )

    assert response[0] == 400
    assert response[1]["code"] == "invalid_request"
    assert ai_preview_app.ai_preview_test_server.capability_calls == 0
    assert ai_preview_app.ai_preview_test_server.preview_calls == []
    assert "forbidden.example" not in json.dumps(response[1])


def test_daemon_route_requires_current_available_capability_before_post(
    ai_preview_app: Any,
) -> None:
    unavailable = valid_capabilities()
    unavailable["available"] = False
    unavailable["reason"] = "feature_disabled"
    stable = dict(unavailable)
    stable.pop("observed_at")
    stable.pop("capability_hash")
    unavailable["capability_hash"] = _hash(
        "spl.ai-preview.capability/v1",
        _canonical(stable),
    )
    ai_preview_app.ai_preview_test_server.capabilities = unavailable

    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=valid_request(),
        )
    )

    assert response[0] == 503
    assert response[1]["code"] == "ai_preview_disabled"
    assert response[1]["outcome"] == "not_started"
    assert ai_preview_app.ai_preview_test_server.capability_calls == 1
    assert ai_preview_app.ai_preview_test_server.preview_calls == []


def test_daemon_route_enforces_current_server_request_limit_before_post(
    ai_preview_app: Any,
) -> None:
    capabilities = valid_capabilities()
    capabilities["max_request_bytes"] = 512
    stable = dict(capabilities)
    stable.pop("observed_at")
    stable.pop("capability_hash")
    capabilities["capability_hash"] = _hash(
        "spl.ai-preview.capability/v1",
        _canonical(stable),
    )
    request = valid_request(capabilities)
    assert len(_canonical(request)) > capabilities["max_request_bytes"]
    ai_preview_app.ai_preview_test_server.capabilities = capabilities

    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=request,
        )
    )

    assert response[0] == 413
    assert response[1]["code"] == "invalid_request"
    assert response[1]["outcome"] == "not_started"
    assert ai_preview_app.ai_preview_test_server.capability_calls == 1
    assert ai_preview_app.ai_preview_test_server.preview_calls == []


def test_daemon_route_rejects_stale_consent_before_post(
    ai_preview_app: Any,
) -> None:
    request = valid_request()
    request["consent"]["granted_at"] = "2026-08-03T09:44:59.000Z"
    consent_without_hash = dict(request["consent"])
    consent_without_hash.pop("consent_hash")
    request["consent"]["consent_hash"] = _hash(
        "spl.ai-preview.consent/v1",
        _canonical(consent_without_hash),
    )

    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=request,
        )
    )

    assert response[0] == 400
    assert response[1]["code"] == "invalid_request"
    assert response[1]["outcome"] == "not_started"
    assert ai_preview_app.ai_preview_test_server.capability_calls == 1
    assert ai_preview_app.ai_preview_test_server.preview_calls == []


@pytest.mark.asyncio
async def test_daemon_route_bounds_slow_capability_read_before_post(
    ai_preview_app: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ai_preview_routes,
        "AI_PREVIEW_CAPABILITY_TIMEOUT_SECONDS",
        0.01,
    )

    def slow_capability() -> dict[str, Any]:
        time.sleep(0.05)
        return valid_capabilities()

    ai_preview_app.ai_preview_test_server.get_ai_preview_capabilities = slow_capability
    started = time.monotonic()
    response = await _http_request(
        ai_preview_app,
        "POST",
        "/server/ai/preview",
        payload=valid_request(),
    )

    assert time.monotonic() - started < 0.04
    assert response[0] == 503
    assert response[1]["outcome"] == "not_started"
    assert ai_preview_app.ai_preview_test_server.preview_calls == []


@pytest.mark.parametrize(
    "credential",
    ["machine-central-secret", "user-central-secret"],
)
def test_daemon_route_rejects_configured_central_credential_before_post(
    ai_preview_app: Any,
    credential: str,
) -> None:
    request = valid_request(source=f"opaque_value = {credential!r}\n")

    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=request,
        )
    )

    assert response[0] == 400
    assert response[1]["code"] == "invalid_request"
    assert credential not in json.dumps(response[1])
    assert ai_preview_app.ai_preview_test_server.capability_calls == 0
    assert ai_preview_app.ai_preview_test_server.preview_calls == []


def test_daemon_never_serializes_configured_credentials_from_central_response(
    ai_preview_app: Any,
) -> None:
    capability = valid_capabilities()
    capability["model_policy_label"] = "user-central-secret"
    stable = dict(capability)
    stable.pop("observed_at")
    stable.pop("capability_hash")
    capability["capability_hash"] = _hash(
        "spl.ai-preview.capability/v1",
        _canonical(stable),
    )
    ai_preview_app.ai_preview_test_server.capabilities = capability
    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "GET",
            "/server/ai/preview/capabilities",
        )
    )
    assert response[0] == 502
    assert "user-central-secret" not in json.dumps(response[1])

    ai_preview_app.ai_preview_test_server.capabilities = valid_capabilities()

    def credential_result(payload: dict[str, Any]) -> dict[str, Any]:
        document = valid_result(payload)
        document["proposal"]["title"] = "machine-central-secret"
        return document

    ai_preview_app.ai_preview_test_server.create_ai_preview = credential_result
    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=valid_request(),
        )
    )
    assert response[0] == 502
    assert response[1]["outcome"] == "not_completed"
    assert "machine-central-secret" not in json.dumps(response[1])


def test_daemon_route_maps_ambiguous_post_failure_without_retry_or_raw_prose(
    ai_preview_app: Any,
) -> None:
    sentinel = "raw-upstream-source-and-provider-exception-sentinel"
    ai_preview_app.ai_preview_test_server.preview_error = ServerClientError(
        504,
        sentinel,
    )

    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=valid_request(),
        )
    )

    assert response[0] == 503
    assert response[1]["code"] == "server_offline"
    assert response[1]["outcome"] == "unknown"
    assert response[1]["retryable"] is False
    assert sentinel not in json.dumps(response[1])
    assert len(ai_preview_app.ai_preview_test_server.preview_calls) == 1


def test_daemon_route_maps_central_permission_error_without_raw_prose(
    ai_preview_app: Any,
) -> None:
    ai_preview_app.ai_preview_test_server.preview_error = ServerClientError(
        403,
        "raw authorization detail",
        code="permission_denied",
    )

    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=valid_request(),
        )
    )

    assert response[0] == 403
    assert response[1]["code"] == "permission_denied"
    assert response[1]["outcome"] == "not_started"
    assert "raw authorization detail" not in json.dumps(response[1])
    assert len(ai_preview_app.ai_preview_test_server.preview_calls) == 1


@pytest.mark.parametrize(
    ("code", "status", "retryable", "outcome"),
    [
        ("ai_preview_disabled", 503, False, "not_started"),
        ("provider_unavailable", 503, False, "not_started"),
        ("provider_timeout", 504, False, "unknown"),
        ("provider_failed", 502, False, "unknown"),
        ("provider_result_invalid", 502, False, "not_completed"),
    ],
)
def test_daemon_preserves_typed_central_errors_that_share_connectivity_statuses(
    ai_preview_app: Any,
    code: str,
    status: int,
    retryable: bool,
    outcome: str,
) -> None:
    ai_preview_app.ai_preview_test_server.preview_error = ServerClientError(
        status,
        "raw central detail",
        code=code,
    )

    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=valid_request(),
        )
    )

    assert response[0] == status
    assert response[1]["code"] == code
    assert response[1]["retryable"] is retryable
    assert response[1]["outcome"] == outcome
    assert "raw central detail" not in json.dumps(response[1])
    assert len(ai_preview_app.ai_preview_test_server.preview_calls) == 1


def test_daemon_maps_invalid_central_error_envelope_to_protocol_failure(
    ai_preview_app: Any,
) -> None:
    ai_preview_app.ai_preview_test_server.preview_error = ServerClientError(
        502,
        "raw malformed envelope",
        code="ai_preview_protocol_invalid",
    )

    response = asyncio.run(
        _http_request(
            ai_preview_app,
            "POST",
            "/server/ai/preview",
            payload=valid_request(),
        )
    )

    assert response[0] == 502
    assert response[1]["code"] == "ai_preview_protocol_invalid"
    assert response[1]["retryable"] is False
    assert response[1]["outcome"] == "unknown"
    assert "raw malformed envelope" not in json.dumps(response[1])


def test_server_client_uses_fixed_user_authenticated_paths_and_one_post_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )
    calls: list[tuple[Any, ...]] = []

    def fake_request(
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        calls.append((method, path, payload, kwargs))
        return {}

    monkeypatch.setattr(client, "_json_request", fake_request)
    assert client.get_ai_preview_capabilities() == {}
    assert client.create_ai_preview({"closed": True}) == {}
    assert calls[0][0:3] == ("GET", "/ai/preview/capabilities", None)
    assert calls[0][3]["auth"] == "user"
    assert calls[0][3]["allow_transport_retries"] is False
    assert calls[0][3]["max_transport_attempts"] == 1
    assert calls[0][3]["absolute_deadline_seconds"] == (AI_PREVIEW_SERVER_TIMEOUT_SECONDS)
    assert calls[1][0:3] == ("POST", "/ai/preview", {"closed": True})
    assert calls[1][3]["auth"] == "user"
    assert calls[1][3]["allow_transport_retries"] is False
    assert calls[1][3]["max_transport_attempts"] == 1
    assert calls[1][3]["compact_json"] is True
    assert calls[1][3]["absolute_deadline_seconds"] == (AI_PREVIEW_SERVER_TIMEOUT_SECONDS)


def test_server_client_detects_opaque_configured_credentials_without_exposing_them() -> None:
    client = ServerClient(
        "https://splime.io/api",
        "machine-token-opaque",
        user_token="user-token-opaque",
    )

    assert client.ai_preview_request_contains_configured_credential(
        {"included_cells": [{"source": "value = 'user-token-opaque'"}]}
    )
    assert client.ai_preview_request_contains_configured_credential(
        {"included_cells": [{"source": "value = 'machine-token-opaque'"}]}
    )
    assert not client.ai_preview_request_contains_configured_credential(
        {"included_cells": [{"source": "value = 'unrelated'"}]}
    )


def test_server_client_serializes_ai_preview_compactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bodies: list[bytes] = []
    timeouts: list[float] = []

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *args: Any) -> None:
            del args

        def read(self, _limit: int | None = None) -> bytes:
            return b"{}"

    def record(request: Any, **kwargs: Any) -> Response:
        bodies.append(request.data)
        timeouts.append(kwargs["timeout"])
        return Response()

    monkeypatch.setattr(remote_client, "urlopen_verified", record)
    payload = {"unicode": "λ", "nested": [1, {"value": True}]}
    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )

    assert client.create_ai_preview(payload) == {}
    assert len(bodies) == 1
    assert json.loads(bodies[0]) == payload
    assert len(bodies[0]) == len(_canonical(payload))
    assert b": " not in bodies[0]
    assert len(timeouts) == 1
    assert AI_PREVIEW_SERVER_TIMEOUT_SECONDS - 0.1 < timeouts[0] <= (AI_PREVIEW_SERVER_TIMEOUT_SECONDS)


def test_server_client_does_not_replay_ai_preview_after_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def fail_transport(*args: Any, **kwargs: Any) -> Any:
        nonlocal attempts
        del args, kwargs
        attempts += 1
        raise URLError(TimeoutError("provider response boundary is ambiguous"))

    monkeypatch.setattr(remote_client, "urlopen_verified", fail_transport)
    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )

    with pytest.raises(ServerClientError):
        client.create_ai_preview(valid_request())

    assert attempts == 1


def test_server_client_closes_a_trickling_ai_response_at_absolute_deadline() -> None:
    class TrickleResponse:
        def __init__(self) -> None:
            self.closed = False
            self.read_count = 0

        def read1(self, _limit: int) -> bytes:
            self.read_count += 1
            time.sleep(0.006)
            return b"x"

        def close(self) -> None:
            self.closed = True

    response = TrickleResponse()
    started = time.monotonic()
    with pytest.raises(ServerClientError) as captured:
        ServerClient._read_json_text(
            response,
            max_response_bytes=1024,
            deadline=started + 0.01,
        )

    assert time.monotonic() - started < 0.04
    assert captured.value.code == "server_response_timeout"
    assert response.closed is True
    assert response.read_count <= 3
