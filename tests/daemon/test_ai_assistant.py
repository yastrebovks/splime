from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from spl.daemon.ai_assistant import (
    AI_ASSISTANT_CAPABILITIES_ROUTE,
    AI_ASSISTANT_ROUTE,
    AIAssistantContractError,
    parse_request,
    validate_capabilities,
    validate_result,
)
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore


def _billing(*, allow: bool = True) -> dict[str, Any]:
    return {
        "plan": "team",
        "entitlement": "managed_ai_analysis_credits",
        "period": "2026-08",
        "limit": 25,
        "used": 3,
        "remaining": 22,
        "enforcement_mode": "shadow",
        "decision": "allow",
        "allow": allow,
        "recovery_action": "none",
    }


def valid_capabilities() -> dict[str, Any]:
    return {
        "schema": "spl.ai-assistant.capabilities",
        "schema_version": 1,
        "observed_at": "2026-08-04T12:00:00.000Z",
        "available": True,
        "reason": None,
        "provider_display_name": "OpenAI",
        "model_policy_label": "Server-selected splime AI model",
        "prompt_policy_version": "splime-ai-assistant-v1",
        "operations": [
            {"id": operation, "label": operation.replace("_", " "), "credit_cost": 1}
            for operation in (
                "function_to_node_draft",
                "node_generate",
                "object_call_help",
                "run_failure_explain",
                "object_discover",
                "environment_advise",
            )
        ],
        "request_schema": "spl.ai-assistant.request",
        "result_schema": "spl.ai-assistant.result",
        "max_request_bytes": 2 * 1024 * 1024,
        "storage_disclosure": "Source and result are not stored by spl-server.",
        "cancellation_disclosure": "Cancellation may not stop provider computation.",
        "disclosure_version": "splime-ai-assistant-disclosure-v1",
        "policy_hash": "sha256:" + "a" * 64,
        "billing": _billing(),
    }


def valid_request(*, request_id: str = "assistant-request-1") -> dict[str, Any]:
    return {
        "schema": "spl.ai-assistant.request",
        "schema_version": 1,
        "request_id": request_id,
        "operation": "node_generate",
        "instruction": "Create a typed increment Node.",
        "context": {
            "notebook_cells": [],
            "object": None,
            "run": None,
            "catalog": [],
            "dependencies": [],
        },
        "consent": {
            "confirmed": True,
            "granted_at": "2026-08-04T12:00:00.000Z",
            "disclosure_version": "splime-ai-assistant-disclosure-v1",
        },
    }


def valid_result(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": "spl.ai-assistant.result",
        "schema_version": 1,
        "request_id": request["request_id"],
        "operation": request["operation"],
        "provider_display_name": "OpenAI",
        "model_policy_label": "Server-selected splime AI model",
        "answer": "Review this editable draft.",
        "artifacts": [
            {
                "kind": "python",
                "title": "Node draft",
                "content": "def increment(value: int) -> int:\n    return value + 1",
                "language": "python",
            }
        ],
        "references": [],
        "warnings": ["Package availability was not inspected."],
        "next_steps": ["Validate before publication."],
        "usage": {"input_tokens": 20, "output_tokens": 10, "total_tokens": 30},
        "generated_at": "2026-08-04T12:00:01.000Z",
        "provenance": {
            "producer": "spl_server",
            "release_id": "0.4.7",
            "prompt_policy_version": "splime-ai-assistant-v1",
        },
        "billing": {**_billing(), "used": 4, "remaining": 21},
    }


def test_contract_rejects_tampered_notebook_source_hash() -> None:
    request = valid_request()
    source = "def increment(value: int) -> int:\n    return value + 1"
    digest = hashlib.sha256(b"splime.ide.notebook-cell-source/v1\0" + source.encode("utf-8")).hexdigest()
    request["operation"] = "function_to_node_draft"
    request["instruction"] = None
    request["context"]["notebook_cells"] = [
        {
            "cell_id": "cell-1",
            "ordinal": 0,
            "source": source,
            "source_hash": f"sha256:{digest}",
        }
    ]
    parse_request(json.dumps(request).encode())
    request["context"]["notebook_cells"][0]["source"] += "\n# changed"
    with pytest.raises(AIAssistantContractError):
        parse_request(json.dumps(request).encode())


class RecordingServer:
    def __init__(self) -> None:
        self.capabilities = valid_capabilities()
        self.requests: list[dict[str, Any]] = []

    def ai_preview_request_contains_configured_credential(
        self,
        payload: dict[str, Any],
    ) -> bool:
        return "central-secret" in json.dumps(payload)

    def get_ai_assistant_capabilities(self) -> dict[str, Any]:
        return deepcopy(self.capabilities)

    def create_ai_assistant(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.requests.append(deepcopy(payload))
        return valid_result(payload)


@pytest.fixture
def assistant_app(tmp_path: Path) -> Any:
    store = RegistryStore(tmp_path)
    store.save_server_connection(
        server_url="https://splime.io/api",
        token="machine-central-secret",
        user_token="user-central-secret",
        connection={
            "id": "connection-1",
            "owner_id": "owner-1",
            "subject_type": "machine",
            "subject_id": "machine-1",
            "machine_id": "machine-1",
            "display_name": "test-machine",
            "status": "connected",
            "capabilities": {},
        },
        heartbeat_interval_seconds=60,
    )
    app = create_app(store, api_token="daemon-master-token", auto_build_envs=False)
    fake = RecordingServer()
    credentials = store.current_server_connection_credentials()
    assert credentials is not None
    app.runtime._require_live_server_channel_credentials = lambda: credentials
    app.runtime._server_client_for_credentials = lambda *args, **kwargs: fake
    app.assistant_test_server = fake
    try:
        yield app
    finally:
        app.runtime.shutdown()
        store.close()


def test_routes_are_authenticated_no_store_and_nonpersisting(
    assistant_app: Any,
) -> None:
    async def exercise() -> tuple[Any, Any, Any]:
        client = assistant_app.test_client()
        headers = {"Authorization": "Bearer daemon-master-token"}
        capabilities = await client.get(
            AI_ASSISTANT_CAPABILITIES_ROUTE,
            headers=headers,
        )
        created = await client.post(
            AI_ASSISTANT_ROUTE,
            json=valid_request(),
            headers=headers,
        )
        denied = await client.get(AI_ASSISTANT_CAPABILITIES_ROUTE)
        return capabilities, created, denied

    before = assistant_app.runtime.store.list_runs()
    capabilities, created, denied = asyncio.run(exercise())
    assert capabilities.status_code == created.status_code == 200
    capability_body = asyncio.run(capabilities.get_json())
    result_body = asyncio.run(created.get_json())
    validate_capabilities(capability_body)
    validate_result(
        result_body,
        request=valid_request(),
        current_capabilities=valid_capabilities(),
    )
    assert assistant_app.assistant_test_server.requests == [valid_request()]
    assert assistant_app.runtime.store.list_runs() == before == []
    assert capabilities.headers["Cache-Control"] == "no-store"
    assert created.headers["Cache-Control"] == "no-store"
    assert denied.status_code == 401


def test_live_plan_denial_never_posts_upstream(assistant_app: Any) -> None:
    billing = assistant_app.assistant_test_server.capabilities["billing"]
    billing.update(
        {
            "enforcement_mode": "live",
            "decision": "deny",
            "allow": False,
            "remaining": 0,
            "recovery_action": "upgrade_plan",
        }
    )

    async def exercise() -> Any:
        client = assistant_app.test_client()
        return await client.post(
            AI_ASSISTANT_ROUTE,
            json=valid_request(),
            headers={"Authorization": "Bearer daemon-master-token"},
        )

    response = asyncio.run(exercise())
    body = asyncio.run(response.get_json())
    assert response.status_code == 402
    assert body["code"] == "plan_limit_reached"
    assert body["outcome"] == "not_started"
    assert assistant_app.assistant_test_server.requests == []
