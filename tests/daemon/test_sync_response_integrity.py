"""Real HTTP response failures must never condemn valid outbound events."""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from spl.daemon.remote_client import ServerClient, ServerClientError, _require_json_dict, _require_json_dict_list
from spl.daemon.heartbeat_service import HeartbeatService
from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore
from spl.daemon.sync_diagnostics import sync_failure
from spl.daemon.telemetry import SYNC_EVENT_MAX_BYTES
from tests.daemon.test_server_channel_recovery import _NoopHeartbeats, _remote_connection


@pytest.fixture
def http_sync(tmp_path):
    state = {"reply": "valid", "batches": [], "applied": Counter(), "heartbeats": 0}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, body, status=200):
            self.send_response(state.get("status", status))
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(state.get("announced_length", len(body))))
            self.end_headers()
            if state.get("body_delay"):
                time.sleep(state["body_delay"])
            try:
                self.wfile.write(body)
            except BrokenPipeError:
                pass

        def do_GET(self):
            self.respond(state["raw"])

        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            body = json.loads(raw)
            if self.path == "/connections/heartbeat":
                state["heartbeats"] += 1
                self.respond(json.dumps(_remote_connection()).encode())
                return
            assert self.path == "/sync"
            events = body["events"]
            state["batches"].append(events)
            if "sync_http_error" in state:
                status, error = state["sync_http_error"]
                self.respond(json.dumps(error).encode(), status=status)
                return
            # Model the server's durable event-ID idempotency contract: the
            # effect precedes the response and a lost receipt never undoes it.
            for event in events:
                if event["id"] not in state["applied"]:
                    state["applied"][event["id"]] += 1
            receipts = [
                {"event_id": event["id"], "kind": event["kind"], "status": "ok", "result": {}} for event in events
            ]
            reply = state["reply"]
            if isinstance(reply, bytes):
                self.respond(reply)
                return
            document = {"connection": _remote_connection(), "event_results": receipts, "jobs": []}
            if reply == "partial":
                document["event_results"] = [
                    receipts[0],
                    {**receipts[1], "kind": "unrelated_kind"},
                    "private-response-sentinel",
                ]
            elif reply == "missing":
                document["event_results"] = receipts[:1]
            elif reply == "unrelated":
                document["event_results"] = [{**receipt, "event_id": "unrelated-id"} for receipt in receipts]
            elif reply == "conflicting":
                document["event_results"] = [
                    receipts[0],
                    {**receipts[0], "status": "error", "retryable": False},
                    receipts[1],
                ]
            elif reply == "malformed-error":
                document["event_results"] = [receipts[0], {**receipts[1], "status": "error", "retryable": "false"}]
            elif reply == "duplicate":
                document["event_results"] = [receipts[0], receipts[0], receipts[1]]
            elif reply == "receipt-patch":
                document["event_results"] = [receipts[0], {**receipts[1], **state["receipt_patch"]}]
            elif reply == "diagnostic-metadata":
                document["event_results"] = [
                    {**receipt, **state["receipt_patch"]}
                    if receipt["event_id"] == state["malformed_event_id"]
                    else receipt
                    for receipt in receipts
                ]
            elif reply == "permanent-error":
                document["event_results"] = [
                    receipts[0],
                    {
                        **receipts[1],
                        "status": "error",
                        "retryable": False,
                        "code": "invalid_payload",
                        "error": "private-response-sentinel",
                    },
                ]
            elif reply == "bad-connection":
                document["connection"] = "private-response-sentinel"
            elif reply == "bad-jobs":
                document["jobs"] = "private-response-sentinel"
            elif reply == "bad-job-record":
                document["jobs"] = [{"run": {"id": ["private-response-sentinel"]}}]
            elif reply == "bad-connection-fields":
                document["connection"] = {"status": {"source": "private-response-sentinel"}}
            elif reply == "connection-patch":
                document["connection"].update(state["connection_patch"])
            elif reply == "jobs-patch":
                document["jobs"] = state["jobs_patch"]
            self.respond(json.dumps(document).encode())

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    store = RegistryStore(tmp_path / "home")
    connection = store.save_server_connection(
        server_url=f"http://127.0.0.1:{http.server_port}",
        token="synthetic-machine",
        user_token="synthetic-user",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    runtime._mark_server_channel_success(store.get_server_connection_credentials(connection["id"]))
    snapshot_hash, _ = runtime.build_machine_library_snapshot_manifest()
    store.record_server_connection_library_snapshot(connection["id"], snapshot_hash=snapshot_hash)
    try:
        yield store, runtime, state
    finally:
        runtime.shutdown()
        store.close()
        http.shutdown()
        http.server_close()
        thread.join(timeout=3)


def queued_pair(store):
    return [
        store.enqueue_sync_event("local_run_update", {"run": {"id": f"synthetic-run-{i}", "owner_id": "owner-a"}})["id"]
        for i in range(2)
    ]


@pytest.mark.parametrize(
    "body",
    [
        b'{"event_results":',
        b"",
        b" ",
        b"null",
        b"[]",
        b'"text"',
        b"{}",
        b'{"event_results":null}',
        b'{"event_results":{}}',
    ],
)
def test_invalid_http_success_retains_events_retries_same_ids_and_deduplicates(http_sync, body):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state["reply"] = body
    result = runtime.sync_once()
    assert all(store.get_sync_event(event_id)["retry"]["will_retry"] for event_id in ids)
    assert result["partial"] is True
    assert result["partial_error"]["category"] == "invalid_response"
    assert result["partial_error"]["retryable"] is True
    assert runtime.server_connection_state()["connected"] is True
    assert len(state["batches"]) == 1, "an unusable response must end this drain, not retry in a tight loop"
    assert set(state["applied"]) == set(ids), "server effects happened before the unusable response"
    state["reply"] = "valid"
    assert runtime.sync_once()["partial"] is False
    assert {event["id"] for event in state["batches"][1]} == set(ids)
    assert all(store.get_sync_event(event_id)["status"] == "sent" for event_id in ids)
    runtime.sync_once()
    assert state["batches"][-1] == []
    assert dict(state["applied"]) == dict.fromkeys(ids, 1)
    assert state["heartbeats"] >= 3


@pytest.mark.parametrize(
    "reply,acknowledged",
    [
        ("partial", [0]),
        ("missing", [0]),
        ("unrelated", []),
        ("conflicting", [1]),
        ("duplicate", [1]),
        ("malformed-error", [0]),
        ("bad-connection", [0, 1]),
        ("bad-jobs", [0, 1]),
        ("bad-job-record", [0, 1]),
        ("bad-connection-fields", [0, 1]),
    ],
)
def test_partial_receipts_preserve_only_unambiguous_matching_acknowledgements(http_sync, reply, acknowledged):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state["reply"] = reply
    result = runtime.sync_once()
    assert result["partial"] is True
    assert result["partial_error"]["category"] == "invalid_response"
    assert "private-response-sentinel" not in json.dumps(result)
    for index, event_id in enumerate(ids):
        row = store.get_sync_event(event_id)
        assert (row["status"] == "sent") is (index in acknowledged)
        if index not in acknowledged:
            assert row["retry"]["will_retry"] is True
    assert len(state["batches"]) == 1
    state["reply"] = "valid"
    runtime.sync_once()
    assert {event["id"] for event in state["batches"][1]} == {
        event_id for index, event_id in enumerate(ids) if index not in acknowledged
    }
    assert dict(state["applied"]) == dict.fromkeys(ids, 1)


@pytest.mark.parametrize("raw,limit", [(b"{", None), (b"{", 64), (b"\xff", None), (b"\xff", 64), (b"x" * 65, 64)])
def test_observed_http_parse_failures_are_typed_and_do_not_open_connectivity_breaker(http_sync, raw, limit):
    store, runtime, state = http_sync
    state["raw"] = raw
    credentials = store.current_server_connection_credentials()
    client = ServerClient(credentials["server_url"], "synthetic")
    with pytest.raises(ServerClientError) as raised:
        client._json_request("GET", "/bounded", max_response_bytes=limit)
    error = raised.value
    assert error.transport_failure is False
    assert runtime._is_server_connectivity_error(error) is False
    diagnostic = sync_failure(error, operation="sync")
    assert diagnostic["category"] == "invalid_response" and diagnostic["retryable"] is True
    assert raw.decode("utf-8", errors="replace") not in error.message or len(raw) < 3


def test_invalid_outbound_event_isolated_before_real_http_request(http_sync):
    store, runtime, state = http_sync
    bad = store.enqueue_sync_event("local_run_update", {"run": {"id": "bad", "owner_id": "owner-a"}})
    with store._lock, store._conn:
        store._conn.execute(
            "UPDATE sync_events SET payload_json=? WHERE id=?",
            (json.dumps({"source": "x" * (SYNC_EVENT_MAX_BYTES + 1)}), bad["id"]),
        )
    valid_ids = queued_pair(store)
    runtime.sync_once()
    rejected = store.get_sync_event(bad["id"])
    assert rejected["status"] == "failed" and rejected["retry"]["will_retry"] is False
    assert all(store.get_sync_event(event_id)["status"] == "sent" for event_id in valid_ids)
    assert all(event["id"] != bad["id"] for batch in state["batches"] for event in batch)
    assert runtime.server_connection_state()["connected"] is True


@pytest.mark.parametrize(
    "patch",
    [
        {"event_id": ["private-response-sentinel"]},
        {"status": "accepted"},
        {"kind": None},
        {"retryable": None},
        {"retryable": 0},
        {"code": {"source": "private-response-sentinel"}},
        {"status_code": "400"},
        {"status_code": True},
        {"status_code": 200},
        {"error": {"source": "private-response-sentinel"}},
    ],
)
def test_malformed_receipt_fields_cannot_ack_or_permanently_fail_event(http_sync, patch):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state.update(reply="receipt-patch", receipt_patch=patch)
    response = runtime.sync_once()
    assert response["partial_error"]["category"] == "invalid_response"
    assert store.get_sync_event(ids[0])["status"] == "sent"
    assert store.get_sync_event(ids[1])["retry"]["will_retry"]
    assert "private-response-sentinel" not in json.dumps(response)


def test_explicit_individual_rejection_is_permanent_without_exposing_response_text(http_sync):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state["reply"] = "permanent-error"
    result = runtime.sync_once()
    assert result["partial"] is False
    assert store.get_sync_event(ids[0])["status"] == "sent"
    row = store.get_sync_event(ids[1])
    assert row["status"] == "failed" and row["retry"]["will_retry"] is False
    assert "private-response-sentinel" not in json.dumps([result, row])
    state["reply"] = "valid"
    runtime.sync_once()
    assert state["batches"][-1] == []


@pytest.mark.parametrize("limit", [None, 64])
def test_incomplete_http_framing_is_response_failure_even_when_json_prefix_is_valid(http_sync, limit):
    store, runtime, state = http_sync
    state.update(raw=b"{}", announced_length=20)
    client = ServerClient(store.current_server_connection_credentials()["server_url"], "synthetic")
    with pytest.raises(ServerClientError) as raised:
        client._json_request("GET", "/bounded", max_response_bytes=limit)
    assert raised.value.transport_failure is False
    assert sync_failure(raised.value, operation="read")["category"] == "invalid_response"
    assert not runtime._is_server_connectivity_error(raised.value)


@pytest.mark.parametrize(
    "mode", ["json-shape", "json-list-shape", "bytes-limit", "bytes-integrity", "response-deadline"]
)
def test_bounded_response_validation_paths_share_nontransport_classification(http_sync, mode):
    store, runtime, state = http_sync
    state["raw"] = b"123"
    client = ServerClient(store.current_server_connection_credentials()["server_url"], "synthetic")
    with pytest.raises(ServerClientError) as raised:
        if mode == "json-shape":
            _require_json_dict(client._json_request("GET", "/bounded", max_response_bytes=64))
        elif mode == "json-list-shape":
            _require_json_dict_list(client._json_request("GET", "/bounded", max_response_bytes=64))
        elif mode == "bytes-limit":
            client._bytes_request("/bounded", max_response_bytes=2)
        elif mode == "bytes-integrity":
            client.artifact_bytes_verified("synthetic-run", "synthetic-artifact", size=3, sha256="0" * 64)
        else:
            state["body_delay"] = 0.1
            client._json_request("GET", "/bounded", max_response_bytes=64, absolute_deadline_seconds=0.02)
    assert raised.value.transport_failure is False
    assert sync_failure(raised.value, operation="read")["category"] == "invalid_response"
    assert not runtime._is_server_connectivity_error(raised.value)


@pytest.mark.parametrize(
    "status,category",
    [(400, "invalid_response"), (401, "authentication"), (403, "authorization"), (503, "invalid_response")],
)
@pytest.mark.parametrize("raw", [b"private-response-sentinel", b"\xff"])
def test_malformed_http_errors_preserve_auth_status_without_exposing_body(http_sync, status, category, raw):
    store, runtime, state = http_sync
    state.update(raw=raw, status=status)
    client = ServerClient(store.current_server_connection_credentials()["server_url"], "synthetic")
    with pytest.raises(ServerClientError) as raised:
        client._json_request("GET", "/bounded", max_response_bytes=64, allow_transport_retries=False)
    assert raised.value.transport_failure is False
    assert sync_failure(raised.value, operation="read")["category"] == category
    assert "private-response-sentinel" not in str(raised.value)
    assert not runtime._is_server_connectivity_error(raised.value)


def test_http_invalid_response_uses_heartbeat_backoff_then_resets_after_recovery(http_sync, monkeypatch):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state["reply"] = b'{"event_results":'
    service = HeartbeatService(store, runtime.sync_once, initial_backoff_seconds=0.1, max_backoff_seconds=0.4)
    delays = []

    def record_wait(connection_id, stop_event, *, delay, interval):
        delays.append(delay)
        assert store.get_server_connection(connection_id)["status"] == "connected"
        if len(delays) == 2:
            state["reply"] = "valid"
        return len(delays) == 3

    monkeypatch.setattr(service, "_wait_until_next_attempt", record_wait)
    service._server_heartbeat_loop(store.current_server_connection_credentials()["id"], "synthetic", threading.Event())
    assert delays == [0.1, 0.2, 60.0]
    assert len(state["batches"]) == 3
    assert all(store.get_sync_event(event_id)["status"] == "sent" for event_id in ids)
    assert dict(state["applied"]) == dict.fromkeys(ids, 1)
    service.shutdown()


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "private-response-sentinel"),
        ("last_seen_at", "private-response-sentinel"),
        ("expires_at", "private-response-sentinel"),
        ("heartbeat_interval_seconds", "NaN"),
        ("heartbeat_interval_seconds", "Infinity"),
    ],
)
def test_invalid_connection_metadata_cannot_overwrite_healthy_lease(http_sync, field, value):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state.update(reply="connection-patch", connection_patch={field: value})
    result = runtime.sync_once()
    assert result["partial_error"]["category"] == "invalid_response"
    assert all(store.get_sync_event(event_id)["status"] == "sent" for event_id in ids)
    assert runtime.server_connection_state()["connected"] is True
    assert "private-response-sentinel" not in json.dumps(result)


@pytest.mark.parametrize(
    "job",
    [
        {"run": {"id": "run-1"}},
        {"run": {"id": "run-1"}, "object_version": {"name": ["private-response-sentinel"], "yaml": "synthetic"}},
        {"run": {"id": "run-1"}, "object_version": {"name": "name", "yaml": []}},
        {"run": {"id": "run-1"}, "object_version": {"name": "name", "yaml": "synthetic"}, "claim_id": 123},
    ],
)
def test_malformed_claimed_job_is_partial_and_cannot_start_worker(http_sync, monkeypatch, job):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state.update(reply="jobs-patch", jobs_patch=[job])

    def unexpected_worker(*args, **kwargs):
        pytest.fail("malformed claimed job must not start a worker")

    monkeypatch.setattr(runtime, "accept_server_job", unexpected_worker)
    result = runtime.sync_once()
    assert result["partial_error"]["category"] == "invalid_response"
    assert result["jobs"] == []
    assert all(store.get_sync_event(event_id)["status"] == "sent" for event_id in ids)
    assert runtime.server_connection_state()["connected"] is True
    assert "private-response-sentinel" not in json.dumps(result)


@pytest.mark.parametrize("category", [["invalid_payload"], {"name": "invalid_payload"}])
def test_diagnostic_metadata_real_heartbeat_preserves_partial_ack_liveness_and_recovery(
    http_sync, monkeypatch, category
):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state.update(
        reply="diagnostic-metadata",
        malformed_event_id=ids[1],
        receipt_patch={"status": "error", "retryable": False, "category": category},
    )
    results, waits = [], []

    def synchronize(**kwargs):
        result = runtime.sync_once(**kwargs)
        results.append(result)
        return result

    service = HeartbeatService(store, synchronize, initial_backoff_seconds=0.1, max_backoff_seconds=0.4)

    def record_wait(connection_id, stop_event, *, delay, interval):
        waits.append(
            {
                "delay": delay,
                "connection": runtime.server_connection_state(),
                "rows": [store.get_sync_event(event_id) for event_id in ids],
                "stored_status": store.get_server_connection(connection_id)["status"],
            }
        )
        if len(waits) == 2:
            state["reply"] = "valid"
        return len(waits) == 4

    monkeypatch.setattr(service, "_wait_until_next_attempt", record_wait)
    try:
        service._server_heartbeat_loop(
            store.current_server_connection_credentials()["id"], "synthetic", threading.Event()
        )
        assert all(wait["stored_status"] == "connected" and wait["connection"]["connected"] is True for wait in waits)
        assert "central_server_unreachable" not in json.dumps(waits)
        assert [wait["delay"] for wait in waits] == [0.1, 0.2, 60.0, 60.0]
        assert len(results) == 4
        for result in results[:2]:
            assert result["partial"] is True
            assert result["partial_error"]["category"] == "invalid_response"
            assert result["partial_error"]["retryable"] is True
        for wait in waits[:2]:
            assert wait["rows"][0]["status"] == "sent"
            assert wait["rows"][1]["retry"]["will_retry"] is True
        assert [sorted(event["id"] for event in batch) for batch in state["batches"]] == [
            sorted(ids),
            [ids[1]],
            [ids[1]],
            [],
        ]
        assert all(row["status"] == "sent" for row in waits[-1]["rows"])
        assert all(result["partial"] is False for result in results[2:])
        assert state["heartbeats"] == 4
        assert dict(state["applied"]) == dict.fromkeys(ids, 1)
    finally:
        service.shutdown()


@pytest.mark.parametrize("field", ["operation", "recommended_action", "actual_bytes", "limit_bytes", "snapshot_id"])
@pytest.mark.parametrize("value", [["private source"], {"source": "private source"}])
def test_diagnostic_metadata_optional_field_types_cannot_retire_event(http_sync, field, value):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state.update(
        reply="diagnostic-metadata",
        malformed_event_id=ids[1],
        receipt_patch={"status": "error", "retryable": False, field: value},
    )
    result = runtime.sync_once()
    assert result["partial"] is True
    assert result["partial_error"]["category"] == "invalid_response"
    assert store.get_sync_event(ids[0])["status"] == "sent"
    assert store.get_sync_event(ids[1])["retry"]["will_retry"] is True
    assert "private source" not in json.dumps(result)
    assert runtime.server_connection_state()["connected"] is True


@pytest.mark.parametrize(
    "fields,expected_retry",
    [
        ({}, True),
        (
            {
                "category": "conflict",
                "operation": "future_sync",
                "recommended_action": "future_action",
                "retryable": False,
            },
            False,
        ),
        (
            {
                "category": "future_category",
                "recommended_action": "future action with spaces",
                "extension": {"nested": True},
                "retryable": False,
            },
            False,
        ),
    ],
)
def test_diagnostic_metadata_legacy_omissions_and_additive_strings_remain_compatible(http_sync, fields, expected_retry):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    state.update(
        reply="diagnostic-metadata",
        malformed_event_id=ids[1],
        receipt_patch={"status": "error", "code": "future_rejection", **fields},
    )
    result = runtime.sync_once()
    assert result["partial"] is False
    assert store.get_sync_event(ids[0])["status"] == "sent"
    assert store.get_sync_event(ids[1])["status"] == "failed"
    assert store.get_sync_event(ids[1])["retry"]["will_retry"] is expected_retry
    assert runtime.server_connection_state()["connected"] is True


@pytest.mark.parametrize(
    "value",
    [None, False, 1, 1.5, "private source\nsynthetic-token", [], {}, ["invalid_payload"], {"name": "invalid_payload"}],
)
def test_diagnostic_metadata_normalization_is_total_for_json_field_types(value):
    payload = {
        key: value
        for key in (
            "category",
            "recommended_action",
            "retryable",
            "actual_bytes",
            "limit_bytes",
            "event_id",
            "snapshot_id",
            "operation",
            "identifier",
        )
    }
    error = ServerClientError(
        422, "private source\nsynthetic-token", code=value, payload=payload, transport_failure=False
    )
    diagnostic = sync_failure(error, operation="sync")
    assert diagnostic["operation"] == "sync"
    assert isinstance(diagnostic["category"], str)
    assert type(diagnostic["retryable"]) is bool
    assert len(json.dumps(diagnostic)) < 1500
    assert "private source" not in json.dumps(diagnostic)
    assert not any(isinstance(item, (list, dict)) for item in diagnostic.values())


@pytest.mark.parametrize("status", [None, True, False, 1, 600, "401", [], {}, [401], {"status": 401}])
def test_diagnostic_metadata_invalid_exception_status_is_bounded_and_type_safe(status):
    diagnostic = sync_failure(ServerClientError(status, "private source", payload={"category": {}}), operation="sync")
    assert diagnostic["status_code"] is None
    assert diagnostic["category"] == "invalid_payload"
    assert "private source" not in json.dumps(diagnostic)


@pytest.mark.parametrize("category", [["invalid_payload"], {"name": "invalid_payload"}, "invalid_payload"])
@pytest.mark.parametrize(
    "status,expected_category,expected_retry",
    [(401, "authentication", False), (403, "authorization", False), (422, "invalid_payload", True)],
)
def test_diagnostic_metadata_structured_http_errors_preserve_authoritative_auth(
    http_sync, category, status, expected_category, expected_retry
):
    store, runtime, state = http_sync
    state.update(
        status=status,
        raw=json.dumps(
            {
                "code": "request_rejected",
                "error": "private source",
                "category": category,
                "retryable": True,
                "recommended_action": "repair_payload",
            }
        ).encode(),
    )
    client = ServerClient(store.current_server_connection_credentials()["server_url"], "synthetic")
    with pytest.raises(ServerClientError) as raised:
        client._json_request("GET", "/bounded", allow_transport_retries=False, max_response_bytes=4096)
    diagnostic = sync_failure(raised.value, operation="sync")
    assert diagnostic["status_code"] == status
    assert diagnostic["category"] == expected_category
    assert diagnostic["retryable"] is expected_retry
    if status in (401, 403):
        assert diagnostic["next_action"] == (
            "reconnect_with_valid_credentials" if status == 401 else "check_operation_permissions"
        )
    assert "private source" not in json.dumps(diagnostic)
    assert not runtime._is_server_connectivity_error(raised.value)


@pytest.mark.parametrize("kind", ["transport", "response", "revoked"])
def test_diagnostic_metadata_cannot_override_authoritative_failure_markers(kind):
    from spl.daemon.remote_client import ServerResponseError

    if kind == "response":
        error = ServerResponseError()
        expected = ("invalid_response", True, "retry_with_backoff")
    elif kind == "transport":
        error = ServerClientError(502, "private source", transport_failure=True)
        expected = ("connectivity", True, "retry_when_server_reachable")
    else:
        error = ServerClientError(403, "private source", code="authentication_failed", transport_failure=False)
        expected = ("authentication", False, "reconnect_with_valid_credentials")
    error.payload = {"category": "invalid_payload", "retryable": False, "recommended_action": "repair_payload"}
    diagnostic = sync_failure(error, operation="sync")
    assert (diagnostic["category"], diagnostic["retryable"], diagnostic["next_action"]) == expected


@pytest.mark.parametrize("marker", [None, False, 1, "true", [], {"true": True}])
def test_diagnostic_metadata_response_marker_requires_true_boolean(marker):
    error = ServerClientError(422, "private source", transport_failure=False)
    error.response_failure = marker
    diagnostic = sync_failure(error, operation="sync")
    assert diagnostic["category"] == "invalid_payload"


def test_diagnostic_metadata_sizes_are_bounded_and_http_status_enum_is_supported():
    from http import HTTPStatus

    payload = {
        "actual_bytes": 1 << 10000,
        "limit_bytes": 1 << 10000,
        "event_id": "x" * 257,
        "snapshot_id": "private source\nsynthetic-token",
        "recommended_action": "x" * 129,
    }
    diagnostic = sync_failure(
        ServerClientError(HTTPStatus.UNAUTHORIZED, "private source", payload=payload), operation="sync"
    )
    assert diagnostic["category"] == "authentication" and diagnostic["status_code"] == 401
    assert not any(key in diagnostic for key in ("actual_bytes", "limit_bytes", "event_id", "snapshot_id"))
    assert len(json.dumps(diagnostic)) < 500


@pytest.mark.parametrize("category", [["invalid_payload"], {"name": "invalid_payload"}])
@pytest.mark.parametrize("status", [401, 403, 404, 409, 502])
def test_diagnostic_metadata_structured_http_heartbeat_uses_safe_persisted_error(
    http_sync, monkeypatch, category, status
):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    code = "authentication_failed" if status == 403 else "synthetic_rejection"
    state["sync_http_error"] = (
        status,
        {
            "code": code,
            "error": "private source\nsynthetic-token",
            "category": category,
            "retryable": True,
            "recommended_action": "repair_payload",
        },
    )
    service = HeartbeatService(store, runtime.sync_once, initial_backoff_seconds=0.1)
    waits = []

    def record_wait(connection_id, stop_event, *, delay, interval):
        waits.append(delay)
        return True

    monkeypatch.setattr(service, "_wait_until_next_attempt", record_wait)
    try:
        service._server_heartbeat_loop(
            store.current_server_connection_credentials()["id"], "synthetic", threading.Event()
        )
        connection = store.current_server_connection()
        assert connection["status"] == ("connected" if status == 502 else "needs_reconnect")
        assert "private source" not in json.dumps(connection)
        assert "synthetic-token" not in json.dumps(connection)
        if status != 502:
            assert code in connection["error"]
        assert waits == [0.1]
        assert state["heartbeats"] == 1
        assert len(state["batches"]) == 1
        assert all(store.get_sync_event(event_id)["retry"]["will_retry"] for event_id in ids)
        assert "central_server_unreachable" not in json.dumps(runtime.server_connection_state())
    finally:
        service.shutdown()


@pytest.mark.parametrize("category", [["invalid_payload"], {"name": "invalid_payload"}])
def test_diagnostic_metadata_http_identity_collision_rejects_only_matching_event_safely(http_sync, category):
    store, runtime, state = http_sync
    ids = queued_pair(store)
    code = "sync_event_identity_collision"
    state["sync_http_error"] = (
        409,
        {
            "code": code,
            "event_id": ids[1],
            "error": "private source\nsynthetic-token",
            "category": category,
            "retryable": False,
        },
    )

    first = runtime.sync_once()

    assert first["partial"] is False
    assert first["event_results"] == [
        {
            "event_id": ids[1],
            "kind": "local_run_update",
            "status": "error",
            "error": code,
            "code": code,
        }
    ]
    rejected = store.get_sync_event(ids[1])
    assert rejected["status"] == "failed" and rejected["retry"]["will_retry"] is False
    pending = store.get_sync_event(ids[0])
    assert pending["status"] == "pending" and pending["retry"]["will_retry"] is True
    assert "private source" not in json.dumps([first, rejected, pending])
    assert "synthetic-token" not in json.dumps([first, rejected, pending])
    assert store.current_server_connection()["status"] == "connected"
    assert "central_server_unreachable" not in json.dumps(runtime.server_connection_state())

    del state["sync_http_error"]
    recovered = runtime.sync_once()
    complete = runtime.sync_once()
    assert recovered["partial"] is False and complete["partial"] is False
    assert store.get_sync_event(ids[0])["status"] == "sent"
    assert store.get_sync_event(ids[1])["status"] == "failed"
    assert [[event["id"] for event in batch] for batch in state["batches"]] == [ids, [ids[0]], []]
    assert state["applied"] == Counter({ids[0]: 1})
    assert store.current_server_connection()["status"] == "connected"
