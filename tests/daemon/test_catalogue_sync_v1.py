from __future__ import annotations

import base64
import hashlib
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

import spl.daemon.catalogue_sync as catalogue_module
from spl.daemon.catalogue_sync import CatalogueSyncService, wire_bytes
from spl.daemon.remote_client import ServerClient, ServerClientError
from spl.daemon.server import DaemonRuntime, create_app
from spl.daemon.store import RegistryStore
from spl.daemon.sync_diagnostics import SyncPayloadError, sync_failure
from spl.daemon.telemetry import SYNC_BATCH_MAX_BYTES, SYNC_EVENT_MAX_BYTES
from tests.daemon.test_server_channel_recovery import _BatchServer, _NoopHeartbeats, _remote_connection, _runtime
from tests.daemon.test_daemon_endpoint import _post_json_from_app


@pytest.fixture
def transport():
    """Exercise real HTTP request bytes independently of the Python caller."""
    state: dict[str, Any] = {"requests": [], "content": {}, "parts": {}, "snapshot": None, "receipts": {}}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:
            pass

        def do_GET(self) -> None:
            if self.path.startswith("/owners/"):
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"object_not_found"}')
                return
            assert self.path == "/version"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                wire_bytes(
                    {
                        "contract": "version_authority/v1",
                        "declared": {
                            "contracts": {
                                "daemon_server_capabilities": state.get("capabilities", ["catalogue_manifest_v1"])
                            }
                        },
                        "deployment": {"state": "unreported"},
                    }
                )
            )

        def do_POST(self) -> None:
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            state["requests"].append((self.path, raw))
            body = json.loads(raw)
            if self.path == "/catalogue-sync/begin":
                assert len(raw) <= 128 * 1024
                state["manifest"] = body
                missing = [obj["sha256"] for obj in body["objects"] if obj["sha256"] not in state["content"]]
                response = {
                    "state": "pending",
                    "snapshot_id": body["snapshot_id"],
                    "missing": missing,
                    "missing_parts": {
                        obj["sha256"]: [i for i in range(obj["part_count"]) if (obj["sha256"], i) not in state["parts"]]
                        for obj in body["objects"]
                        if obj["sha256"] in missing
                    },
                    "max_part_bytes": catalogue_module.CATALOGUE_PART_BYTES,
                }
                response = state["receipts"].get(body["snapshot_id"], response)
            elif self.path == "/catalogue-sync/content":
                assert len(raw) <= 128 * 1024
                content = base64.b64decode(body["data"], validate=True)
                assert len(content) <= 48 * 1024
                digest = body["sha256"]
                state["parts"][(digest, body["part_index"])] = content
                obj = next(obj for obj in state["manifest"]["objects"] if obj["sha256"] == digest)
                if all((digest, i) in state["parts"] for i in range(obj["part_count"])):
                    complete = b"".join(state["parts"][(digest, i)] for i in range(obj["part_count"]))
                    assert hashlib.sha256(complete).hexdigest() == digest
                    state["content"][digest] = complete
                response = {
                    "snapshot_id": body["snapshot_id"],
                    "sha256": digest,
                    "part_index": body["part_index"],
                    "complete": digest in state["content"],
                }
            elif self.path == "/catalogue-sync/commit":
                manifest = state["manifest"]
                if any(obj["sha256"] not in state["content"] for obj in manifest["objects"]):
                    self.send_response(409)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(wire_bytes({"code": "catalogue_incomplete", "error": "Incomplete catalogue"}))
                    return
                items = [json.loads(state["content"][obj["sha256"]]) for obj in manifest["objects"]]
                metadata = [{key: value for key, value in item.items() if key != "yaml"} for item in items]
                assert (
                    hashlib.sha256(wire_bytes({"format_version": 1, "items": metadata})).hexdigest()
                    == manifest["snapshot_hash"]
                )
                state["snapshot"] = items
                response = {
                    "state": "committed",
                    "snapshot_hash": manifest["snapshot_hash"],
                    "snapshot_id": body["snapshot_id"],
                    "id": "synthetic-snapshot",
                    "item_count": len(items),
                    "unchanged": False,
                }
                state["receipts"][body["snapshot_id"]] = response
            elif self.path == "/connections/heartbeat":
                response = _remote_connection()
            else:
                assert self.path == "/sync"
                assert len(raw) <= 16 * 1024 * 1024
                for event in body["events"]:
                    assert len(wire_bytes(event)) <= SYNC_EVENT_MAX_BYTES
                response = {"event_results": [], "jobs": []}
            if state.get("response_fault"):
                response = state["response_fault"](self.path, body, response)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(wire_bytes(response))

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        yield ServerClient(f"http://127.0.0.1:{http.server_port}", "machine", user_token="user"), state
    finally:
        http.shutdown()
        http.server_close()
        thread.join(timeout=3)


def _register(store: RegistryStore, name: str, size: int) -> None:
    text = '界\\"\t' * (size // 6)
    yaml_text = f"- !DFunction\n  name: {name}\n  inputs: []\n  outputs:\n  - name: default\n    type: int\n  body: |-\n    # {text}\n    return 1\n"
    store.register_object(name=name, entrypoint=name, env="test", yaml_text=yaml_text)


def _credentials() -> dict[str, Any]:
    return {
        "server_url": "https://example.invalid",
        "owner_id": "owner",
        "machine_id": "machine",
        "remote_connection_id": "connection",
    }


@pytest.mark.parametrize(
    "phase, fault",
    [
        ("begin", fault)
        for fault in (
            "empty",
            "state",
            "snapshot_id",
            "missing-null",
            "missing-string",
            "missing-object",
            "missing-unknown",
            "missing-duplicate",
            "missing-unhashable",
            "parts-null",
            "parts-list",
            "parts-unknown",
            "parts-absent",
            "parts-empty",
            "parts-string",
            "parts-duplicate",
            "parts-negative",
            "parts-range",
            "parts-bool",
            "parts-unhashable",
            "max-part-bool",
            "max-part-size",
        )
    ]
    + [
        (phase, fault)
        for phase in ("committed-begin", "commit")
        for fault in (
            "hash-absent",
            "hash-wrong",
            "snapshot-absent",
            "snapshot_id",
            "id-absent",
            "count",
            "unchanged",
        )
    ]
    + [("content", fault) for fault in ("empty", "snapshot_id", "sha256", "part_index", "part-bool", "complete")],
)
def test_catalogue_invalid_http_receipt_retains_transfer_and_registration_for_retry(tmp_path, transport, phase, fault):
    server, observed = transport
    store = RegistryStore(tmp_path)
    store.register_env("test", sys.executable)
    _register(store, "receipt", 6000)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    service = runtime.catalogue_sync
    record = next(iter(store.list_objects().values()))
    registration_id = service.defer_registration(record["version_id"], {"revive_removed": True})
    snapshot_hash, items = runtime.build_machine_library_snapshot_manifest()
    credentials = _credentials()

    def corrupt(path, body, response):
        endpoint = "begin" if phase == "committed-begin" else phase
        if path != f"/catalogue-sync/{endpoint}":
            return response
        value = dict(response)
        if phase == "committed-begin":
            value = {
                "state": "committed",
                "snapshot_hash": snapshot_hash,
                "snapshot_id": body["snapshot_id"],
                "id": "synthetic-snapshot",
                "item_count": len(items),
                "unchanged": False,
            }
        digest = observed["manifest"]["objects"][0]["sha256"]
        if fault == "empty":
            return {}
        if fault in ("state", "snapshot_id", "sha256"):
            value[fault] = "private-response-sentinel"
        elif fault == "hash-absent":
            value.pop("snapshot_hash")
        elif fault == "hash-wrong":
            value["snapshot_hash"] = "f" * 64
        elif fault == "snapshot-absent":
            value.pop("snapshot_id")
        elif fault == "id-absent":
            value.pop("id")
        elif fault == "count":
            value["item_count"] = len(items) + 1
        elif fault == "unchanged":
            value["unchanged"] = "false"
        elif fault.startswith("missing-"):
            value["missing"] = {
                "missing-null": None,
                "missing-string": digest,
                "missing-object": {digest: True},
                "missing-unknown": ["f" * 64],
                "missing-duplicate": [digest, digest],
                "missing-unhashable": [{}],
            }[fault]
        elif fault.startswith("parts-"):
            value["missing_parts"] = {
                "parts-null": None,
                "parts-list": [],
                "parts-unknown": {"f" * 64: [0]},
                "parts-absent": {},
                "parts-empty": {digest: []},
                "parts-string": {digest: "0"},
                "parts-duplicate": {digest: [0, 0]},
                "parts-negative": {digest: [-1]},
                "parts-range": {digest: [1]},
                "parts-bool": {digest: [True]},
                "parts-unhashable": {digest: [{}]},
            }[fault]
        elif fault == "max-part-bool":
            value["max_part_bytes"] = True
        elif fault == "max-part-size":
            value["max_part_bytes"] = 1
        elif fault == "part_index":
            value["part_index"] = body["part_index"] + 1
        elif fault == "part-bool":
            value["part_index"] = False
        elif fault == "complete":
            value["complete"] = "true"
        return value

    observed["response_fault"] = corrupt
    try:
        result = service.synchronize(
            credentials=credentials,
            server=server,
            snapshot_hash=snapshot_hash,
            manifest_items=items,
            renew_lease=lambda: None,
        )
        assert result["state"] == "degraded", result
        assert result["diagnostic"]["category"] == "invalid_response", result
        assert result["diagnostic"]["retryable"] is True
        assert result["diagnostic"]["next_action"] == "retry_with_backoff"
        assert "private-response-sentinel" not in json.dumps(result)
        retained = service._read(service.scope(credentials))
        assert retained["registration_intents"] == [[record["version_id"], registration_id]]
        assert service.registration_intents()[record["version_id"]]["registration_id"] == registration_id
        if phase in {"begin", "committed-begin", "content"}:
            assert retained["next_parts"] == {}
            assert retained["uploaded_parts"] == 0
            assert not any(path.endswith("/commit") for path, _ in observed["requests"])
        runtime.shutdown()
        store.close()
        store = RegistryStore(tmp_path)
        service = CatalogueSyncService(store)
        observed["response_fault"] = None
        recovered = service.synchronize(
            credentials=credentials,
            server=server,
            snapshot_hash=snapshot_hash,
            manifest_items=items,
            renew_lease=lambda: None,
        )
        assert recovered["state"] == "current", recovered
        final = service._read(service.scope(credentials))
        assert final["snapshot_id"] == retained["snapshot_id"]
        assert final["sequence"] == retained["sequence"]
        assert service.registration_intents() == {}
        starts = [json.loads(raw) for path, raw in observed["requests"] if path.endswith("/begin")]
        assert len(starts) == 2, "one invalid response ends the tick; retry occurs only on the next invocation"
        assert starts[0] == starts[1]
        assert len([path for path, _ in observed["requests"] if path.endswith("/commit")]) == 1
    finally:
        runtime.shutdown()
        store.close()


def test_large_catalogue_real_wire_resume_cache_deletion_and_empty(tmp_path, transport, monkeypatch):
    server, observed = transport
    store = RegistryStore(tmp_path)
    store.register_env("test", sys.executable)
    for index in range(14):
        _register(store, f"obj{index}", 45_000)
    _register(store, "single_large", 400_000)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    snapshot_hash, items = runtime.build_machine_library_snapshot_manifest()
    assert (
        runtime._sync_event_wire_bytes(
            runtime.build_machine_library_snapshot_event(snapshot_hash=snapshot_hash, manifest_items=items)
        )
        > 512 * 1024
    )
    credentials = _credentials()
    ticks: list[int] = []
    monkeypatch.setattr(catalogue_module, "CATALOGUE_MAX_REQUESTS_PER_TICK", 3)
    result = runtime.catalogue_sync.synchronize(
        credentials=credentials,
        server=server,
        snapshot_hash=snapshot_hash,
        manifest_items=items,
        renew_lease=lambda: ticks.append(1),
    )
    assert result["state"] == "pending"
    assert observed["snapshot"] is None
    first_id = result["snapshot_id"]
    runtime.shutdown()
    store.close()
    store = RegistryStore(tmp_path)
    service = CatalogueSyncService(store)
    for _ in range(100):
        result = service.synchronize(
            credentials=credentials,
            server=server,
            snapshot_hash=snapshot_hash,
            manifest_items=items,
            renew_lease=lambda: ticks.append(1),
        )
        if result["state"] == "current":
            break
    assert result["state"] == "current", result
    assert result["snapshot_id"] == first_id
    assert len(observed["snapshot"]) == 15
    assert len(store.list_objects()) == 15
    assert ticks
    uploads = sum(path.endswith("/content") for path, _ in observed["requests"])
    retained = items[:-1]
    changed_hash = hashlib.sha256(wire_bytes({"format_version": 1, "items": retained})).hexdigest()
    result = service.synchronize(
        credentials=credentials,
        server=server,
        snapshot_hash=changed_hash,
        manifest_items=retained,
        renew_lease=lambda: None,
    )
    assert result["state"] == "current"
    assert result["reused_objects"] == 14
    assert sum(path.endswith("/content") for path, _ in observed["requests"]) == uploads
    empty_hash = hashlib.sha256(wire_bytes({"format_version": 1, "items": []})).hexdigest()
    assert (
        service.synchronize(
            credentials=credentials,
            server=server,
            snapshot_hash=empty_hash,
            manifest_items=[],
            renew_lease=lambda: None,
        )["state"]
        == "current"
    )
    assert observed["snapshot"] == []
    store.close()


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_sync_exact_event_wire_limits_with_unicode_and_escaping(transport, delta):
    server, observed = transport
    event = {"id": "event", "kind": "test", "payload": {"text": '界"\\\n'}}
    event["payload"]["text"] += "x" * (SYNC_EVENT_MAX_BYTES - len(wire_bytes(event)) + delta)
    assert len(wire_bytes(event)) == SYNC_EVENT_MAX_BYTES + delta
    kwargs = {
        "connection_id": "connection",
        "machine_id": "machine",
        "heartbeat_interval_seconds": 60,
        "events": [event],
    }
    if delta > 0:
        with pytest.raises(SyncPayloadError) as rejected:
            server.sync(**kwargs)
        assert rejected.value.diagnostic["actual_bytes"] == SYNC_EVENT_MAX_BYTES + 1
        assert not observed["requests"]
    else:
        server.sync(**kwargs)
        raw = observed["requests"][-1][1]
        assert b"\\n" in raw and "界".encode() in raw


def test_http_request_limit_is_independent_of_event_and_batch(transport):
    server, observed = transport
    with pytest.raises(SyncPayloadError) as rejected:
        server.catalogue_sync_begin({"snapshot_id": "snapshot", "escaped": "\n" * 70_000})
    assert rejected.value.diagnostic["limit_bytes"] == 128 * 1024
    assert rejected.value.diagnostic["actual_bytes"] > 128 * 1024
    assert observed["requests"] == []


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_sync_exact_batch_wire_limits_independent_of_event_limit(transport, delta):
    server, observed = transport
    first = {"id": "first", "kind": "test", "payload": {"text": ""}}
    second = {"id": "second", "kind": "test", "payload": {"text": ""}}
    first["payload"]["text"] = "x" * (SYNC_EVENT_MAX_BYTES - 2 - len(wire_bytes(first)))
    second["payload"]["text"] = "x" * (SYNC_BATCH_MAX_BYTES + delta - len(wire_bytes([first, second])))
    assert len(wire_bytes([first, second])) == SYNC_BATCH_MAX_BYTES + delta
    assert all(len(wire_bytes(event)) <= SYNC_EVENT_MAX_BYTES for event in (first, second))
    if delta > 0:
        with pytest.raises(SyncPayloadError) as rejected:
            server.sync(
                connection_id="connection", machine_id="machine", heartbeat_interval_seconds=60, events=[first, second]
            )
        assert rejected.value.diagnostic["code"] == "sync_batch_too_large"
        assert not observed["requests"]
    else:
        server.sync(
            connection_id="connection", machine_id="machine", heartbeat_interval_seconds=60, events=[first, second]
        )
        assert len(observed["requests"]) == 1


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_exact_catalogue_http_request_limit_includes_envelope(transport, delta):
    server, observed = transport
    body = {"snapshot_id": "snapshot", "objects": [], "metadata": ""}
    body["metadata"] = "x" * (128 * 1024 + delta - len(wire_bytes(body)))
    assert len(wire_bytes(body)) == 128 * 1024 + delta
    if delta > 0:
        with pytest.raises(SyncPayloadError):
            server.catalogue_sync_begin(body)
        assert not observed["requests"]
    else:
        server.catalogue_sync_begin(body)
        assert len(observed["requests"][-1][1]) == 128 * 1024 + delta


def test_runtime_unchanged_snapshot_uses_no_yaml_or_catalogue_requests(tmp_path, transport, monkeypatch):
    server, observed = transport
    store = RegistryStore(tmp_path)
    store.register_env("test", sys.executable)
    _register(store, "single", 400_000)
    connection = store.save_server_connection(
        server_url=server.base_url,
        token="machine",
        user_token="user",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    runtime._mark_server_channel_success(store.get_server_connection_credentials(connection["id"]))
    try:
        for _ in range(10):
            result = runtime.sync_once()
            if result["catalogue_sync"].get("state") == "current":
                break
        assert result["catalogue_sync"]["state"] == "current", result
        prior = len(observed["requests"])
        monkeypatch.setattr(store, "get_object_version", lambda *a, **kw: pytest.fail("unchanged catalogue read YAML"))
        runtime.sync_once()
        assert [path for path, _ in observed["requests"][prior:]] == ["/connections/heartbeat", "/sync"]
        sync = json.loads(observed["requests"][-1][1])
        assert sync["capabilities"]["spl.catalogue_sync.v1"]["state"] == "current"
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.parametrize("supported", [True, False])
def test_registration_route_retains_large_object_and_defers_to_catalogue(tmp_path, transport, monkeypatch, supported):
    server, observed = transport
    observed["capabilities"] = ["catalogue_manifest_v1"] if supported else []
    store = RegistryStore(tmp_path)
    store.register_env("test", sys.executable)
    app = create_app(store, auto_build_envs=False)
    runtime = app.runtime
    monkeypatch.setattr(runtime, "_kick_server_sync", lambda: None)
    monkeypatch.setattr(runtime, "_server_latest_library_snapshot_hash", lambda *args: None)
    connection = store.save_server_connection(
        server_url=server.base_url,
        token="machine",
        user_token="user",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    runtime._mark_server_channel_success(store.get_server_connection_credentials(connection["id"]))
    source = (
        "- !DFunction\n  name: large\n  inputs: []\n  outputs: [{name: default, type: int}]\n  body: |-\n    # "
        + '界\\"' * 100_000
        + "\n    return 1\n"
    )
    try:
        status, body = _post_json_from_app(
            app,
            "/objects",
            {
                "name": "large",
                "entrypoint": "large",
                "env": "test",
                "yaml": source,
                "library": "research",
                "create_library": True,
                "library_display_name": "Research",
            },
        )
        assert status == 201, body
        assert body["sync_event"]["status"] == "deferred"
        assert body["sync_event"]["transport"] == "catalogue_manifest_v1"
        assert body["sync_event"]["diagnostic"]["actual_bytes"] > SYNC_EVENT_MAX_BYTES
        assert body["sync"]["catalogue_sync"]["state"] == "pending"
        assert store.get_object(body["id"], include_yaml=True)["yaml"] == source
        assert store.list_pending_sync_events() == []
        # Intent is durable and does not contain a second copy of the source.
        restarted = CatalogueSyncService(store)
        intent = restarted.registration_intents()[body["version_id"]]
        assert intent["create_library"] is True and intent["revive_removed"] is True
        assert intent["library_display_name"] == "Research" and "yaml" not in intent
        for _ in range(10):
            result = runtime.sync_once()
            if result["catalogue_sync"]["state"] != "pending":
                break
        assert runtime.server_connection_state()["connected"] is True
        assert any(path == "/connections/heartbeat" for path, _ in observed["requests"])
        if supported:
            assert result["catalogue_sync"]["state"] == "current", result
            assert observed["snapshot"][0]["yaml"] == source
            assert observed["snapshot"][0]["create_library"] is True
            assert observed["snapshot"][0]["revive_removed"] is True
            assert restarted.registration_intents() == {}
            _, clean_items = runtime.build_machine_library_snapshot_manifest()
            assert "revive_removed" not in clean_items[0]
        else:
            assert result["catalogue_sync"]["state"] == "degraded"
            assert result["catalogue_sync"]["diagnostic"]["code"] == "catalogue_capability_required"
            assert restarted.registration_intents()[body["version_id"]] == intent
            assert observed["snapshot"] is None
    finally:
        runtime.shutdown()
        store.close()


def test_catalogue_ack_does_not_clear_newer_registration_of_same_version(tmp_path, transport):
    server, _ = transport
    store = RegistryStore(tmp_path)
    store.register_env("test", sys.executable)
    _register(store, "large", 400_000)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    try:
        record = next(iter(store.list_objects().values()))
        service = runtime.catalogue_sync
        first_id = service.defer_registration(record["version_id"], {"revive_removed": True})
        reconciled = runtime._enqueue_object_version_sync_once(record)
        assert reconciled["status"] == "deferred" and reconciled["id"] == first_id
        assert store.list_pending_sync_events() == []
        snapshot_hash, items = runtime.build_machine_library_snapshot_manifest()
        credentials = _credentials()
        service._prepare(service.scope(credentials), {}, "connection", snapshot_hash, items, lambda: None)
        second_id = service.defer_registration(record["version_id"], {"revive_removed": True})
        assert second_id != first_id
        service.record_committed(credentials, snapshot_hash)
        assert service.registration_intents()[record["version_id"]]["registration_id"] == second_id
    finally:
        runtime.shutdown()
        store.close()


def test_large_registration_keeps_explicit_target_library(tmp_path):
    store = RegistryStore(tmp_path)
    store.register_env("test", sys.executable)
    _register(store, "large", 400_000)
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    try:
        record = next(iter(store.list_objects().values()))
        deferred = runtime.enqueue_object_sync(record, library="research", create_library=True)
        assert deferred["status"] == "deferred"
        _, items = runtime.build_machine_library_snapshot_manifest()
        assert items[0]["library_slug"] == "research" and items[0]["create_library"] is True
    finally:
        runtime.shutdown()
        store.close()


def test_catalogue_owner_switch_never_transfers_other_owner_sources(tmp_path, transport):
    server, observed = transport
    store = RegistryStore(tmp_path)
    store.register_env("test", sys.executable)
    for owner in ("owner-a", "owner-b"):
        store.register_object(
            name=f"private-{owner}",
            entrypoint="fn",
            env="test",
            owner_id=owner,
            yaml_text="- !DFunction\n  name: fn\n  inputs: []\n  outputs: [{name: default, type: int}]\n  body: |-\n    # "
            + owner
            + "\n    return 1\n",
        )
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    try:
        for owner in ("owner-a", "owner-b"):
            connection = store.save_server_connection(
                server_url=server.base_url,
                token=f"token-{owner}",
                user_token=f"user-token-{owner}",
                connection={**_remote_connection(), "owner_id": owner},
                heartbeat_interval_seconds=60,
            )
            credentials = store.get_server_connection_credentials(connection["id"])
            snapshot_hash, items = runtime.build_machine_library_snapshot_manifest()
            assert [item["name"] for item in items] == [f"private-{owner}"]
            result = runtime.catalogue_sync.synchronize(
                credentials=credentials,
                server=server,
                snapshot_hash=snapshot_hash,
                manifest_items=items,
                renew_lease=lambda: None,
            )
            assert result["state"] == "current"
            assert [item["name"] for item in observed["snapshot"]] == [f"private-{owner}"]
    finally:
        runtime.shutdown()
        store.close()


def test_permanent_local_catalogue_error_does_not_rebuild_yaml(tmp_path, transport, monkeypatch):
    server, _ = transport
    store = RegistryStore(tmp_path)
    store.register_env("test", sys.executable)
    _register(store, "single", 4000)
    connection = store.save_server_connection(
        server_url=server.base_url,
        token="machine",
        user_token="user",
        connection=_remote_connection(),
        heartbeat_interval_seconds=60,
    )
    runtime = DaemonRuntime(store, heartbeat_service=_NoopHeartbeats())
    runtime._mark_server_channel_success(store.get_server_connection_credentials(connection["id"]))
    monkeypatch.setattr(catalogue_module, "CATALOGUE_OBJECT_MAX_BYTES", 1000)
    try:
        result = runtime.sync_once()
        assert result["catalogue_sync"]["diagnostic"]["code"] == "catalogue_object_too_large"
        monkeypatch.setattr(
            store, "get_object_version", lambda *a, **kw: pytest.fail("permanent catalogue error retried YAML")
        )
        assert runtime.sync_once()["catalogue_sync"] == result["catalogue_sync"]
        assert runtime.server_connection_state()["connected"] is True
    finally:
        runtime.shutdown()
        store.close()


def test_invalid_ephemeral_event_preserves_earlier_valid_event(tmp_path):
    _BatchServer.reset()
    store, runtime, _ = _runtime(tmp_path)
    try:
        result = runtime.sync_once(
            extra_events=[
                {"id": "good", "kind": "test", "payload": {}},
                {"id": "bad", "kind": "test", "payload": {"value": "x" * 300_000}},
            ]
        )
        assert any(event["id"] == "good" for call in _BatchServer.calls for event in call["events"])
        assert any(item["event_id"] == "bad" and item["status"] == "error" for item in result["event_results"])
    finally:
        runtime.shutdown()
        store.close()


def test_legacy_large_catalogue_does_not_block_heartbeat_or_events(tmp_path, monkeypatch):
    _BatchServer.reset()
    store, runtime, connection = _runtime(tmp_path)
    try:
        monkeypatch.setattr(runtime, "build_machine_library_snapshot_manifest", lambda **kwargs: ("changed", []))
        monkeypatch.setattr(
            runtime,
            "build_machine_library_snapshot_event",
            lambda **_: {
                "id": "large",
                "kind": "machine_library_snapshot",
                "payload": {"yaml": "secret-source" * 50_000},
            },
        )
        healthy = store.enqueue_sync_event("local_run_update", {"owner_id": "owner-a", "run": {"id": "healthy"}})
        result = runtime.sync_once()
        assert result["lease_renewed"] is True
        assert store.get_sync_event(healthy["id"])["status"] == "sent"
        assert store.get_server_connection(connection["id"])["status"] == "connected"
        diagnostic = result["catalogue_sync"]["diagnostic"]
        assert diagnostic["code"] == "catalogue_capability_required"
        assert diagnostic["actual_bytes"] > diagnostic["limit_bytes"]
        assert "secret-source" not in json.dumps(diagnostic)
        assert runtime.server_connection_state()["connected"] is True
        runtime.sync_once()
        assert len(_BatchServer.calls) == 2
    finally:
        runtime.shutdown()
        store.close()


def test_permanent_batch_failure_isolates_poison_and_leaves_channel_live(tmp_path):
    class PoisonServer(_BatchServer):
        def sync(self, **kwargs):
            if any(event["payload"].get("poison") for event in kwargs["events"]):
                raise ServerClientError(422, "invalid event body", code="invalid_payload", transport_failure=False)
            return super().sync(**kwargs)

    store, runtime, connection = _runtime(tmp_path)
    runtime.server_connections.server_client_factory = PoisonServer
    # The manager's factory is a stable injected dependency.
    try:
        poison = store.enqueue_sync_event("test", {"owner_id": "owner-a", "poison": True})
        good = store.enqueue_sync_event("test", {"owner_id": "owner-a"})
        runtime.sync_once()
        assert store.get_sync_event(poison["id"])["retry"]["will_retry"] is False
        assert store.get_sync_event(good["id"])["status"] == "sent"
        assert runtime.server_connection_state()["connected"] is True
    finally:
        runtime.shutdown()
        store.close()


def test_revoked_authentication_during_sync_retains_valid_work_and_closed_connectivity_breaker(tmp_path):
    class RevokedServer(_BatchServer):
        def sync(self, **kwargs):
            raise ServerClientError(403, "revoked", code="authentication_failed", transport_failure=False)

    store, runtime, connection = _runtime(tmp_path)
    runtime.server_connections.server_client_factory = RevokedServer
    try:
        event = store.enqueue_sync_event("test", {"owner_id": "owner-a"})
        with pytest.raises(ServerClientError):
            runtime.sync_once()
        assert store.get_sync_event(event["id"])["status"] == "pending"
        assert store.get_server_connection(connection["id"])["status"] == "needs_reconnect"
        state = runtime.server_connection_state()
        assert state["breaker"]["state"] == "closed"
        assert state["code"] == "server_reconnect_required"
    finally:
        runtime.shutdown()
        store.close()


@pytest.mark.parametrize("status", [401, 403])
def test_whoami_rejects_revoked_or_unauthorized_credentials(tmp_path, monkeypatch, status):
    store, runtime, _ = _runtime(tmp_path)

    class IdentityServer:
        def whoami(self):
            raise ServerClientError(status, "identity rejected", transport_failure=False)

        def list_users(self, **kwargs):
            pytest.fail("whoami must never enumerate users")

    monkeypatch.setattr(runtime, "_server_client_for_credentials", lambda *args, **kwargs: IdentityServer())
    try:
        with pytest.raises(ServerClientError) as rejected:
            runtime.server_whoami()
        assert rejected.value.status_code == status
    finally:
        runtime.shutdown()
        store.close()


def test_error_classification_keeps_safe_server_size_fields_and_authentication_code():
    rejection = ServerClientError(
        413,
        "private source body must not escape",
        code="request_body_too_large",
        transport_failure=False,
        payload={
            "actual_bytes": 131073,
            "limit_bytes": 131072,
            "snapshot_id": "snapshot-1",
            "recommended_action": "resume_transfer",
            "yaml": "secret source",
            "token": "secret token",
        },
    )
    diagnostic = sync_failure(rejection, operation="catalogue_sync")
    assert diagnostic["actual_bytes"] == 131073
    assert diagnostic["limit_bytes"] == 131072
    assert diagnostic["identifier"] == "snapshot-1"
    assert diagnostic["next_action"] == "resume_transfer"
    assert "secret" not in json.dumps(diagnostic)
    assert (
        sync_failure(ServerClientError(403, "revoked", code="authentication_failed"), operation="sync")["category"]
        == "authentication"
    )
    assert (
        sync_failure(ServerClientError(503, "busy", transport_failure=False), operation="sync")["category"]
        == "transient_server"
    )


@pytest.mark.parametrize(
    "identity, expected",
    [
        (
            {"owner_id": "owner-a", "subject_id": "machine-1", "subject_type": "machine"},
            {"id": "owner-a", "handle": None, "display_name": "owner-a"},
        ),
        (
            {
                "owner_id": "owner-a",
                "subject_id": "delegate",
                "owner_profile": {"id": "owner-a", "handle": "alice", "display_name": "Alice"},
            },
            {"id": "owner-a", "handle": "alice", "display_name": "Alice"},
        ),
        ({"owner_id": "owner-b", "owner_profile": {"id": "owner-b", "handle": "bob"}}, None),
    ],
)
def test_whoami_owner_subject_fallback_and_switch(tmp_path, monkeypatch, identity, expected):
    store, runtime, _ = _runtime(tmp_path)

    class IdentityServer:
        def whoami(self):
            return identity

    monkeypatch.setattr(runtime, "_server_client_for_credentials", lambda *args, **kwargs: IdentityServer())
    try:
        if expected is None:
            with pytest.raises(ServerClientError, match="connected owner"):
                runtime.server_whoami()
        else:
            result = runtime.server_whoami()
            assert {key: result[key] for key in expected} == expected
    finally:
        runtime.shutdown()
        store.close()
