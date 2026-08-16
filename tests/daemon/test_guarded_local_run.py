"""Focused contract tests for guarded, exact local-Run admission."""

from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

import spl.daemon.guarded_local_run as guarded_contract
import spl.daemon.server as daemon_server
from spl.daemon.guarded_local_run import MAX_BODY_BYTES
from spl.daemon.home_lock import DaemonHomeLock, DaemonInstanceIdentity
from spl.daemon.server import DaemonRuntime, create_app
from spl.daemon.store import RegistryStore


API_TOKEN = "guardedAdmissionMasterTokenSentinel123456"
ROUTE = "/runs/local-admissions"
FUNCTION_YAML = """\
- !DFunction
  name: fit
  inputs:
  - name: max_depth
    type: int
  outputs:
  - name: default
    type: int
  body: |-
    return max_depth
"""
FUNCTION_YAML_V2 = """\
- !DFunction
  name: fit
  inputs:
  - name: max_depth
    type: int
  outputs:
  - name: default
    type: int
  body: |-
    return max_depth + 1
"""
ERROR_FIELDS = {"schema", "schema_version", "code", "retryable", "observed_at"}


@dataclass
class _AdmissionFixture:
    app: Any
    store: RegistryStore
    lock: DaemonHomeLock
    identity: DaemonInstanceIdentity
    record: dict[str, Any]
    request: dict[str, Any]


@dataclass(frozen=True)
class _Response:
    status: int
    body: Any
    raw: str
    headers: dict[str, str]


@pytest.fixture
def admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[_AdmissionFixture]:
    lock = DaemonHomeLock(tmp_path)
    identity = lock.acquire()
    store = RegistryStore(tmp_path)
    store.register_env("default")
    record = store.register_object(
        "fit_xgboost",
        "fit",
        "default",
        yaml_text=FUNCTION_YAML,
        owner_id="owner_alpha",
        library="ml_models",
    )
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )
    monkeypatch.setattr(app.runtime, "_continue_guarded_local_run", lambda _state: None)
    request = {
        "schema": "spl.daemon.guarded-local-run-admission",
        "schema_version": 1,
        "request_id": "request-001",
        "daemon_instance_id": identity.instance_id,
        "daemon_generation": identity.generation,
        "object": {
            "owner_id": record["owner_id"],
            "library": record["library"],
            "object_name": record["name"],
            "object_id": record["id"],
            "expected_current_version_id": record["current_version_id"],
            "selected_version_id": record["version_id"],
            "selected_version": record["version"],
            "function": record["entrypoint"],
            "origin": "local",
        },
        "content_hash": f"sha256:{record['content_hash']}",
        "inputs": [{"name": "max_depth", "value": 6}],
        "output_selector": None,
        "timeout_ms": 30_000,
        "retention": "on_failure",
        "target": "local",
        "source": "local",
    }
    try:
        yield _AdmissionFixture(
            app=app,
            store=store,
            lock=lock,
            identity=identity,
            record=record,
            request=request,
        )
    finally:
        app.runtime.shutdown()
        store.close()
        lock.release()


def _post(
    app: Any,
    *,
    payload: dict[str, Any] | None = None,
    raw: str | bytes | None = None,
    token: str | None = API_TOKEN,
) -> _Response:
    async def request() -> _Response:
        headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        client = app.test_client()
        if raw is None:
            response = await client.post(ROUTE, json=payload, headers=headers)
        else:
            headers["Content-Type"] = "application/json"
            response = await client.post(ROUTE, data=raw, headers=headers)
        raw_body = (await response.get_data()).decode("utf-8")
        return _Response(
            status=response.status_code,
            body=await response.get_json(),
            raw=raw_body,
            headers={str(key).casefold(): str(value) for key, value in response.headers.items()},
        )

    return asyncio.run(request())


def _get(app: Any, path: str, *, token: str | None = API_TOKEN) -> _Response:
    async def request() -> _Response:
        headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        response = await app.test_client().get(path, headers=headers)
        raw_body = (await response.get_data()).decode("utf-8")
        return _Response(
            status=response.status_code,
            body=await response.get_json(),
            raw=raw_body,
            headers={str(key).casefold(): str(value) for key, value in response.headers.items()},
        )

    return asyncio.run(request())


def _delete(app: Any, path: str, *, token: str | None = API_TOKEN) -> _Response:
    async def request() -> _Response:
        headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        response = await app.test_client().delete(path, headers=headers)
        raw_body = (await response.get_data()).decode("utf-8")
        return _Response(
            status=response.status_code,
            body=await response.get_json(),
            raw=raw_body,
            headers={str(key).casefold(): str(value) for key, value in response.headers.items()},
        )

    return asyncio.run(request())


def _stream_post_without_content_length(app: Any, size: int) -> _Response:
    async def request() -> _Response:
        headers = {
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json",
        }
        connection = app.test_client().request(ROUTE, method="POST", headers=headers)
        async with connection:
            remaining = size
            chunk = b"x" * 65_536
            while remaining:
                part = chunk[: min(len(chunk), remaining)]
                await connection.send(part)
                remaining -= len(part)
            await connection.send_complete()
        response = await connection.as_response()
        raw_body = (await response.get_data()).decode("utf-8")
        return _Response(
            status=response.status_code,
            body=await response.get_json(),
            raw=raw_body,
            headers={str(key).casefold(): str(value) for key, value in response.headers.items()},
        )

    return asyncio.run(request())


def _runs(store: RegistryStore) -> list[dict[str, Any]]:
    return store.list_runs()


def _assert_closed_error(response: _Response, code: str, *, status: int) -> None:
    assert response.status == status
    assert set(response.body) == ERROR_FIELDS
    assert response.body["schema"] == "spl.daemon.guarded-local-run-error"
    assert response.body["schema_version"] == 1
    assert response.body["code"] == code
    assert isinstance(response.body["retryable"], bool)
    datetime.fromisoformat(response.body["observed_at"].replace("Z", "+00:00"))
    assert response.headers["cache-control"] == "no-store"
    assert len(response.raw.encode("utf-8")) <= 512
    for forbidden in (
        API_TOKEN,
        "max_depth",
        "private-path-sentinel",
        "malicious-argument-sentinel",
        "Traceback",
        "Exception",
    ):
        assert forbidden not in response.raw


def _copy_request(admission: _AdmissionFixture) -> dict[str, Any]:
    return copy.deepcopy(admission.request)


def test_authenticated_exact_admission_returns_closed_request_bound_202(
    admission: _AdmissionFixture,
) -> None:
    response = _post(admission.app, payload=admission.request)

    assert response.status == 202
    assert response.headers["cache-control"] == "no-store"
    assert set(response.body) == {
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
    assert response.body["schema"] == "spl.daemon.guarded-local-run-receipt"
    assert response.body["schema_version"] == 1
    assert response.body["request_id"] == admission.request["request_id"]
    assert response.body["accepted_http_status"] == 202
    assert response.body["object"] == {
        "owner_id": admission.record["owner_id"],
        "library": admission.record["library"],
        "object_name": admission.record["name"],
        "object_id": admission.record["id"],
        "current_version_id": admission.record["current_version_id"],
        "version_id": admission.record["version_id"],
        "version": admission.record["version"],
        "function": admission.record["entrypoint"],
        "origin": "local",
        "content_hash": admission.request["content_hash"],
    }
    assert response.body["initial_state"] == "queued"
    assert response.body["target"] == response.body["source"] == "local"
    datetime.fromisoformat(response.body["created_at"].replace("Z", "+00:00"))
    datetime.fromisoformat(response.body["observed_at"].replace("Z", "+00:00"))
    assert set(_runs(admission.store)[0])
    assert _runs(admission.store)[0]["id"] == response.body["run_id"]
    assert _runs(admission.store)[0]["object_version_id"] == admission.record["version_id"]
    for forbidden in ("inputs", "output_selector", "timeout_ms", "retention", "max_depth"):
        assert forbidden not in response.raw


def test_exact_admission_schedules_one_synthetic_local_run_to_success(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = admission.app.runtime
    monkeypatch.setattr(
        runtime,
        "_continue_guarded_local_run",
        lambda state: DaemonRuntime._continue_guarded_local_run(runtime, state),
    )

    response = _post(admission.app, payload=admission.request)

    assert response.status == 202
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        state = admission.store.get_run(response.body["run_id"])
        if state["status"] in {"succeeded", "failed"}:
            break
        time.sleep(0.02)
    else:
        raise AssertionError("guarded local Run did not reach a terminal state")
    assert state["status"] == "succeeded", state.get("error")
    assert state["result"] == {"artifacts": {}, "result": 6}
    assert len(_runs(admission.store)) == 1


def test_route_requires_the_existing_global_bearer_boundary(admission: _AdmissionFixture) -> None:
    missing = _post(admission.app, payload=admission.request, token=None)
    wrong = _post(admission.app, payload=admission.request, token="wrong-token")

    assert missing.status == wrong.status == 401
    assert missing.body == wrong.body == {"error": "missing or invalid local daemon API token"}
    assert missing.headers["cache-control"] == wrong.headers["cache-control"] == "no-store"
    assert _runs(admission.store) == []


def test_route_specific_receive_limit_rejects_stream_without_content_length(
    admission: _AdmissionFixture,
) -> None:
    response = _stream_post_without_content_length(admission.app, MAX_BODY_BYTES + 1)

    _assert_closed_error(response, "body_too_large", status=413)
    assert _runs(admission.store) == []


def test_route_specific_receive_limit_accepts_exact_boundary_for_parsing(
    admission: _AdmissionFixture,
) -> None:
    response = _stream_post_without_content_length(admission.app, MAX_BODY_BYTES)

    _assert_closed_error(response, "malformed_json", status=400)
    assert _runs(admission.store) == []


def test_route_specific_receive_limit_preserves_a_stricter_global_limit(
    admission: _AdmissionFixture,
) -> None:
    original_limit = admission.app.config["MAX_CONTENT_LENGTH"]
    admission.app.config["MAX_CONTENT_LENGTH"] = 1_024
    try:
        response = _stream_post_without_content_length(admission.app, 1_025)
    finally:
        admission.app.config["MAX_CONTENT_LENGTH"] = original_limit

    _assert_closed_error(response, "body_too_large", status=413)
    assert _runs(admission.store) == []


def test_guarded_receive_limit_does_not_tighten_legacy_post_runs(
    admission: _AdmissionFixture,
) -> None:
    async def request() -> tuple[int, Any]:
        response = await admission.app.test_client().post(
            "/runs",
            data=b"{}" + b" " * (MAX_BODY_BYTES + 1),
            headers={
                "Authorization": f"Bearer {API_TOKEN}",
                "Content-Type": "application/json",
            },
        )
        return response.status_code, await response.get_json()

    status, body = asyncio.run(request())

    assert status == 404
    assert body == {"error": "'object'"}
    assert _runs(admission.store) == []


def test_guarded_no_store_hook_does_not_change_legacy_dynamic_run_routes(
    admission: _AdmissionFixture,
) -> None:
    guarded_get = _get(admission.app, ROUTE)
    ordinary_get = _get(admission.app, "/runs/another-missing-run")
    guarded_delete = _delete(admission.app, ROUTE)
    ordinary_delete = _delete(admission.app, "/runs/another-missing-run")

    assert guarded_get.status == ordinary_get.status
    assert guarded_delete.status == ordinary_delete.status
    assert guarded_get.headers.get("cache-control") == ordinary_get.headers.get("cache-control")
    assert guarded_delete.headers.get("cache-control") == ordinary_delete.headers.get("cache-control")
    assert _runs(admission.store) == []


@pytest.mark.parametrize("location", ["request-id", "nested-input"])
def test_daemon_bearer_in_request_body_is_rejected_before_admission(
    admission: _AdmissionFixture,
    location: str,
) -> None:
    payload = _copy_request(admission)
    if location == "request-id":
        payload["request_id"] = API_TOKEN
    else:
        payload["inputs"][0]["value"] = {"nested": [f"prefix-{API_TOKEN}-suffix"]}

    response = _post(admission.app, payload=payload)

    _assert_closed_error(response, "request_invalid", status=400)
    assert _runs(admission.store) == []


def test_metadata_advertises_guarded_and_legacy_local_create_as_separate_capabilities(
    admission: _AdmissionFixture,
) -> None:
    response = _get(admission.app, "/meta/capabilities")

    assert response.status == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.body["capabilities"]["run.local.create"] == {
        "state": "supported",
        "version": 1,
        "reason": None,
    }
    assert response.body["capabilities"]["run.local.create.guarded"] == {
        "state": "supported",
        "version": 1,
        "reason": None,
    }
    assert _runs(admission.store) == []


@pytest.mark.parametrize(
    ("mutate", "expected_code"),
    [
        (lambda value: value.__setitem__("daemon_instance_id", "0" * 32), "daemon_instance_mismatch"),
        (
            lambda value: value.__setitem__("daemon_generation", value["daemon_generation"] + 1),
            "daemon_generation_mismatch",
        ),
        (
            lambda value: value["object"].update(
                selected_version_id="missing_version",
                expected_current_version_id="missing_version",
            ),
            "object_not_found",
        ),
        (lambda value: value["object"].__setitem__("owner_id", "owner_beta"), "object_identity_mismatch"),
        (lambda value: value["object"].__setitem__("library", "other_library"), "object_identity_mismatch"),
        (lambda value: value["object"].__setitem__("object_name", "other_object"), "object_identity_mismatch"),
        (lambda value: value["object"].__setitem__("object_id", "other_object_id"), "object_identity_mismatch"),
        (lambda value: value["object"].__setitem__("origin", "server"), "non_local_request"),
        (lambda value: value["object"].__setitem__("selected_version", 2), "object_version_mismatch"),
        (lambda value: value["object"].__setitem__("function", "predict"), "function_mismatch"),
        (lambda value: value.__setitem__("content_hash", f"sha256:{'0' * 64}"), "content_binding_mismatch"),
    ],
    ids=[
        "instance",
        "generation",
        "missing-version",
        "owner",
        "library",
        "name",
        "object-id",
        "origin",
        "numeric-version",
        "function",
        "content",
    ],
)
def test_exact_precondition_mismatch_creates_no_run(
    admission: _AdmissionFixture,
    mutate: Callable[[dict[str, Any]], None],
    expected_code: str,
) -> None:
    payload = _copy_request(admission)
    mutate(payload)

    response = _post(admission.app, payload=payload)

    expected_status = 400 if expected_code == "non_local_request" else 412
    _assert_closed_error(response, expected_code, status=expected_status)
    assert _runs(admission.store) == []


def test_request_that_selects_a_different_expected_pointer_is_rejected_before_lookup(
    admission: _AdmissionFixture,
) -> None:
    payload = _copy_request(admission)
    payload["object"]["expected_current_version_id"] = "stale_expected_pointer"

    response = _post(admission.app, payload=payload)

    _assert_closed_error(response, "selected_version_not_current", status=412)
    assert _runs(admission.store) == []


def test_historical_selected_version_is_rejected_after_publication(
    admission: _AdmissionFixture,
) -> None:
    current = admission.store.register_object(
        admission.record["name"],
        admission.record["entrypoint"],
        admission.record["env"],
        yaml_text=FUNCTION_YAML_V2,
        owner_id=admission.record["owner_id"],
        library=admission.record["library"],
    )
    assert current["current_version_id"] != admission.record["version_id"]

    response = _post(admission.app, payload=admission.request)

    _assert_closed_error(response, "current_version_mismatch", status=412)
    assert _runs(admission.store) == []


@pytest.mark.parametrize(
    ("change", "expected_code", "status"),
    [
        (lambda value: value.__setitem__("future", True), "unknown_field", 400),
        (lambda value: value["object"].__setitem__("future", True), "unknown_field", 400),
        (lambda value: value["inputs"][0].__setitem__("future", True), "unknown_field", 400),
        (lambda value: value.pop("timeout_ms"), "request_invalid", 400),
        (lambda value: value.__setitem__("schema", "wrong.schema"), "request_invalid", 400),
        (lambda value: value.__setitem__("schema_version", 2), "request_invalid", 400),
        (lambda value: value.__setitem__("schema_version", True), "request_invalid", 400),
        (lambda value: value.__setitem__("daemon_generation", True), "request_invalid", 400),
        (lambda value: value["object"].__setitem__("selected_version", True), "request_invalid", 400),
        (lambda value: value.__setitem__("request_id", "unsafe/request"), "request_invalid", 400),
        (lambda value: value["object"].__setitem__("owner_id", "owner:alpha"), "request_invalid", 400),
        (lambda value: value.__setitem__("content_hash", "not-a-hash"), "request_invalid", 400),
        (lambda value: value.__setitem__("target_machine", "machine_01"), "unknown_field", 400),
        (lambda value: value.__setitem__("target", "remote"), "non_local_request", 400),
        (lambda value: value.__setitem__("source", "server"), "non_local_request", 400),
        (lambda value: value.__setitem__("timeout_ms", True), "request_invalid", 400),
        (lambda value: value.__setitem__("timeout_ms", 604_800_001), "request_invalid", 400),
        (lambda value: value.__setitem__("retention", "forever"), "request_invalid", 400),
        (lambda value: value.__setitem__("inputs", []), "arguments_invalid", 400),
        (
            lambda value: value.__setitem__("inputs", [{"name": "unknown", "value": 6}]),
            "arguments_invalid",
            400,
        ),
        (
            lambda value: value.__setitem__("inputs", [{"name": "unsafe/name", "value": 6}]),
            "arguments_invalid",
            400,
        ),
        (
            lambda value: value.__setitem__(
                "inputs",
                [
                    {"name": "max_depth", "value": 6},
                    {"name": "max_depth", "value": 7},
                ],
            ),
            "arguments_invalid",
            400,
        ),
        (lambda value: value.__setitem__("output_selector", "result"), "arguments_invalid", 400),
        (
            lambda value: value.__setitem__(
                "inputs",
                [{"name": f"arg{index}", "value": index} for index in range(257)],
            ),
            "arguments_invalid",
            400,
        ),
        (
            lambda value: value["inputs"][0].__setitem__("value", list(range(1001))),
            "arguments_invalid",
            400,
        ),
        (
            lambda value: value["inputs"][0].__setitem__("value", 1e20),
            "arguments_invalid",
            400,
        ),
        (
            lambda value: value["inputs"][0].__setitem__("value", "x" * (1024 * 1024)),
            "arguments_invalid",
            400,
        ),
    ],
    ids=[
        "unknown-root",
        "unknown-object",
        "unknown-input-field",
        "missing-root-field",
        "schema",
        "schema-version",
        "schema-version-boolean",
        "generation-boolean",
        "version-boolean",
        "identifier",
        "upstream-identifier-colon",
        "hash",
        "server-only-field",
        "remote-target",
        "server-source",
        "timeout-boolean",
        "timeout",
        "retention",
        "missing-required",
        "unknown-input",
        "invalid-input-name",
        "duplicate-input",
        "output-selector",
        "input-count",
        "container-width",
        "unsafe-integral-float",
        "argument-budget",
    ],
)
def test_strict_request_and_argument_validation_create_no_run(
    admission: _AdmissionFixture,
    change: Callable[[dict[str, Any]], None],
    expected_code: str,
    status: int,
) -> None:
    payload = _copy_request(admission)
    change(payload)

    response = _post(admission.app, payload=payload)

    _assert_closed_error(response, expected_code, status=status)
    assert _runs(admission.store) == []


@pytest.mark.parametrize(
    ("raw", "expected_code", "status"),
    [
        ("{not json", "malformed_json", 400),
        (b"\xff", "malformed_json", 400),
        ("[]", "request_invalid", 400),
        (
            '{"schema":"spl.daemon.guarded-local-run-admission","schema":"spl.daemon.guarded-local-run-admission"}',
            "malformed_json",
            400,
        ),
        (
            json.dumps({"padding": "x" * ((1024 * 1024) + (64 * 1024))}),
            "body_too_large",
            413,
        ),
        ('{"value":NaN}', "malformed_json", 400),
    ],
    ids=["syntax", "utf8", "non-object", "duplicate-key", "body-size", "non-finite"],
)
def test_malformed_duplicate_and_oversize_bodies_are_closed(
    admission: _AdmissionFixture,
    raw: str | bytes,
    expected_code: str,
    status: int,
) -> None:
    response = _post(admission.app, raw=raw)

    _assert_closed_error(response, expected_code, status=status)
    assert _runs(admission.store) == []


def test_argument_depth_and_unsafe_number_are_rejected(admission: _AdmissionFixture) -> None:
    nested: Any = "leaf"
    for _ in range(21):
        nested = [nested]
    depth_payload = _copy_request(admission)
    depth_payload["inputs"][0]["value"] = nested
    depth = _post(admission.app, payload=depth_payload)
    _assert_closed_error(depth, "body_too_deep", status=413)

    unsafe_raw = json.dumps(admission.request).replace('"value": 6', '"value": 9007199254740992')
    unsafe = _post(admission.app, raw=unsafe_raw)
    _assert_closed_error(unsafe, "arguments_invalid", status=400)
    assert _runs(admission.store) == []


def test_missing_live_identity_fails_closed_without_a_run(admission: _AdmissionFixture) -> None:
    admission.app.runtime.daemon_identity = None

    response = _post(admission.app, payload=admission.request)

    _assert_closed_error(response, "live_identity_unavailable", status=503)
    assert _runs(admission.store) == []


def test_receipt_is_bound_to_authoritative_facts_not_mutable_request_aliases(
    admission: _AdmissionFixture,
) -> None:
    payload = _copy_request(admission)
    payload["request_id"] = "receipt-binding-001"

    response = _post(admission.app, payload=payload)

    assert response.status == 202
    assert response.body["request_id"] == "receipt-binding-001"
    assert response.body["run_id"] == _runs(admission.store)[0]["id"]
    assert response.body["object"]["current_version_id"] == admission.record["current_version_id"]
    assert response.body["object"]["version_id"] == _runs(admission.store)[0]["object_version_id"]
    assert response.body["object"]["content_hash"] == f"sha256:{admission.record['content_hash']}"
    assert "max_depth" not in response.raw
    assert "receipt-binding-001" not in json.dumps(_runs(admission.store)[0])


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.__setitem__("accepted_http_status", 202.0),
        lambda value: value.__setitem__(
            "created_at",
            value["created_at"].replace("Z", "+00:00"),
        ),
        lambda value: value.__setitem__(
            "observed_at",
            value["observed_at"].replace("Z", "000Z"),
        ),
        lambda value: value.__setitem__("run_id", f"{value['run_id']}\n"),
    ],
    ids=["http-float", "timestamp-offset", "timestamp-precision", "run-id-newline"],
)
def test_receipt_validator_rejects_noncanonical_wire_values(
    admission: _AdmissionFixture,
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    response = _post(admission.app, payload=admission.request)
    receipt = copy.deepcopy(response.body)
    mutate(receipt)
    parsed_admission = guarded_contract.parse_guarded_local_run_request(json.dumps(admission.request).encode("utf-8"))

    with pytest.raises(guarded_contract.GuardedLocalRunError):
        guarded_contract.validate_guarded_local_run_receipt(
            receipt,
            admission=parsed_admission,
        )


def test_post_admission_preparation_failure_preserves_202_and_failed_run(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = admission.app.runtime

    def fail_preparation(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("disposable preparation failure")

    monkeypatch.setattr(runtime, "_prepare_node_runtime_environments_for_run", fail_preparation)
    monkeypatch.setattr(
        runtime,
        "_continue_guarded_local_run",
        lambda state: DaemonRuntime._continue_guarded_local_run(runtime, state),
    )

    response = _post(admission.app, payload=admission.request)

    assert response.status == 202
    assert response.body["initial_state"] == "queued"
    assert response.body["run_id"] == _runs(admission.store)[0]["id"]
    assert _runs(admission.store)[0]["status"] == "failed"
    assert "precondition" not in response.raw.casefold()


def test_create_run_exception_after_insert_recovers_the_exact_202_receipt(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_create = admission.store.create_run
    created_run_id: list[str] = []

    def insert_then_raise(*args: Any, **kwargs: Any) -> dict[str, Any]:
        state = original_create(*args, **kwargs)
        created_run_id.append(str(state["id"]))
        raise RuntimeError("disposable post-insert exception")

    monkeypatch.setattr(admission.store, "create_run", insert_then_raise)

    response = _post(admission.app, payload=admission.request)

    assert response.status == 202
    assert created_run_id == [response.body["run_id"]]
    assert response.body["request_id"] == admission.request["request_id"]
    assert response.body["object"]["version_id"] == admission.record["version_id"]
    assert response.body["initial_state"] == "queued"
    assert [run["id"] for run in _runs(admission.store)] == created_run_id


def test_precommit_receipt_failure_rolls_back_row_and_new_directory(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing_directories = {path.name for path in admission.store.runs.runs_dir.iterdir()}

    def fail_receipt(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("disposable receipt validation failure")

    monkeypatch.setattr(daemon_server, "build_guarded_local_run_receipt", fail_receipt)

    response = _post(admission.app, payload=admission.request)

    _assert_closed_error(response, "admission_failed", status=503)
    assert _runs(admission.store) == []
    assert {path.name for path in admission.store.runs.runs_dir.iterdir()} == existing_directories


def test_run_id_collision_preserves_preexisting_orphan_directory(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collision_id = "f" * 32
    collision_dir = admission.store.runs.runs_dir / collision_id
    collision_dir.mkdir(mode=0o700)
    sentinel = collision_dir / "sentinel.txt"
    sentinel.write_text("preexisting orphan", encoding="utf-8")

    class _CollisionUuid:
        hex = collision_id

    monkeypatch.setattr(
        "spl.daemon.repositories.run.uuid4",
        lambda: _CollisionUuid(),
    )

    response = _post(admission.app, payload=admission.request)

    _assert_closed_error(response, "admission_failed", status=503)
    assert _runs(admission.store) == []
    assert collision_dir.is_dir()
    assert sentinel.read_text(encoding="utf-8") == "preexisting orphan"


def test_sqlite_commit_denial_cannot_return_202_or_leave_a_guarded_run(
    admission: _AdmissionFixture,
) -> None:
    existing_directories = {path.name for path in admission.store.runs.runs_dir.iterdir()}

    def deny_commit(
        action: int,
        argument_one: str | None,
        _argument_two: str | None,
        _database: str | None,
        _trigger: str | None,
    ) -> int:
        if action == sqlite3.SQLITE_TRANSACTION and argument_one == "COMMIT":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    admission.store._storage._conn.set_authorizer(deny_commit)
    try:
        response = _post(admission.app, payload=admission.request)
    finally:
        admission.store._storage._conn.set_authorizer(None)

    _assert_closed_error(response, "admission_failed", status=503)
    assert _runs(admission.store) == []
    assert {path.name for path in admission.store.runs.runs_dir.iterdir()} == existing_directories


def test_state_file_failure_after_commit_preserves_exact_202_receipt(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        admission.store.runs,
        "_write_run_state_file",
        lambda _state: (_ for _ in ()).throw(OSError("disposable state-file failure")),
    )

    response = _post(admission.app, payload=admission.request)

    assert response.status == 202
    assert response.body["request_id"] == admission.request["request_id"]
    assert response.body["run_id"] == _runs(admission.store)[0]["id"]
    assert response.body["initial_state"] == _runs(admission.store)[0]["status"] == "queued"


def test_create_run_return_copy_cannot_create_false_failure(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_create = admission.store.create_run

    def return_changed_copy(*args: Any, **kwargs: Any) -> dict[str, Any]:
        state = original_create(*args, **kwargs)
        return {**state, "status": "starting"}

    monkeypatch.setattr(admission.store, "create_run", return_changed_copy)

    response = _post(admission.app, payload=admission.request)

    assert response.status == 202
    assert response.body["initial_state"] == "queued"
    assert [state["id"] for state in _runs(admission.store)] == [response.body["run_id"]]


def test_backward_wall_clock_is_clamped_to_created_at(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(guarded_contract, "utc_now_milliseconds", lambda: "2000-01-01T00:00:00.000Z")

    response = _post(admission.app, payload=admission.request)

    assert response.status == 202
    assert response.body["observed_at"] == response.body["created_at"]
    assert len(_runs(admission.store)) == 1


def test_unexpected_continuation_escape_preserves_accepted_receipt(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sentinel = f"arg={API_TOKEN} path={admission.store.home}"
    monkeypatch.setattr(
        admission.app.runtime,
        "_continue_guarded_local_run",
        lambda _state: (_ for _ in ()).throw(RuntimeError(sentinel)),
    )

    response = _post(admission.app, payload=admission.request)

    assert response.status == 202
    assert response.body["run_id"] == _runs(admission.store)[0]["id"]
    assert "failure" not in response.raw.casefold()
    assert sentinel not in caplog.text
    assert API_TOKEN not in caplog.text


def test_publication_cannot_advance_current_pointer_inside_admission_critical_section(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered_create = threading.Event()
    allow_create = threading.Event()
    publication_finished = threading.Event()
    original_create = admission.store.create_run

    def blocked_create(*args: Any, **kwargs: Any) -> dict[str, Any]:
        entered_create.set()
        assert allow_create.wait(timeout=5)
        return original_create(*args, **kwargs)

    monkeypatch.setattr(admission.store, "create_run", blocked_create)
    request_result: list[_Response] = []
    request_error: list[BaseException] = []

    def send_request() -> None:
        try:
            request_result.append(_post(admission.app, payload=admission.request))
        except BaseException as exc:  # pragma: no cover - surfaced by the main assertion
            request_error.append(exc)

    published: list[dict[str, Any]] = []

    def publish_next_version() -> None:
        published.append(
            admission.store.register_object(
                admission.record["name"],
                admission.record["entrypoint"],
                admission.record["env"],
                yaml_text=FUNCTION_YAML_V2,
                owner_id=admission.record["owner_id"],
                library=admission.record["library"],
            )
        )
        publication_finished.set()

    request_thread = threading.Thread(target=send_request, name="guarded-admission-request")
    request_thread.start()
    assert entered_create.wait(timeout=5)
    publication_thread = threading.Thread(target=publish_next_version, name="object-publication")
    publication_thread.start()
    time.sleep(0.05)
    assert not publication_finished.is_set()

    allow_create.set()
    request_thread.join(timeout=10)
    publication_thread.join(timeout=10)

    assert not request_thread.is_alive()
    assert not publication_thread.is_alive()
    assert request_error == []
    assert request_result[0].status == 202
    assert request_result[0].body["object"]["version_id"] == admission.record["version_id"]
    assert _runs(admission.store)[0]["object_version_id"] == admission.record["version_id"]
    assert published[0]["current_version_id"] != admission.record["version_id"]


def test_dropped_response_scenario_has_one_request_and_one_run_without_replay(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_count = 0
    original_create = admission.store.create_run

    def counted_create(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal request_count
        request_count += 1
        return original_create(*args, **kwargs)

    monkeypatch.setattr(admission.store, "create_run", counted_create)

    _post(admission.app, payload=admission.request)  # The client deliberately discards the accepted receipt.

    assert request_count == 1
    assert len(_runs(admission.store)) == 1


def test_unexpected_internal_failure_is_bounded_and_creates_no_run(
    admission: _AdmissionFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_create(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError(f"private-path-sentinel {admission.store.home} malicious-argument-sentinel {API_TOKEN}")

    monkeypatch.setattr(admission.store, "create_run", fail_create)

    response = _post(admission.app, payload=admission.request)

    _assert_closed_error(response, "admission_failed", status=503)
    assert _runs(admission.store) == []
