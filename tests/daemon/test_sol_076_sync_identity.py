from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest

from spl.core.fingerprint import canonical_json_bytes
from spl.daemon.remote_client import (
    SYNC_EVENT_IDENTITY_COLLISION_ERROR_CODE,
    ServerClientError,
    _error_response,
    is_sync_event_identity_collision_error,
)
from spl.daemon.server import DaemonRuntime
from spl.daemon.store import RegistryStore


FIXTURE = Path(__file__).parent / "fixtures" / "sync_envelope_v1.json"


def _contract() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads(FIXTURE.read_text(encoding="utf-8")),
    )


@pytest.mark.parametrize("vector", _contract()["vectors"], ids=lambda row: row["name"])
def test_daemon_canonical_sync_envelope_matches_shared_fixture(
    vector: dict[str, Any],
) -> None:
    canonical = canonical_json_bytes(
        {
            "schema_version": 1,
            "kind": vector["kind"],
            "payload": vector["payload"],
        }
    )

    assert canonical == bytes.fromhex(vector["canonical_hex"])
    assert canonical.endswith(b"\n")
    assert hashlib.sha256(canonical).hexdigest() == vector["sha256"]


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("nan", float("nan")),
        ("positive_infinity", float("inf")),
        ("negative_infinity", float("-inf")),
    ],
)
def test_daemon_canonical_sync_envelope_rejects_nonfinite(
    name: str,
    value: float,
) -> None:
    assert name in _contract()["reject_nonfinite"]
    with pytest.raises(ValueError) as exc_info:
        canonical_json_bytes(
            {
                "schema_version": 1,
                "kind": "object_version",
                "payload": {"value": value},
            }
        )
    message = str(exc_info.value)
    assert '$["payload"]["value"]' in message
    assert {
        "nan": "NaN",
        "positive_infinity": "Infinity",
        "negative_infinity": "-Infinity",
    }[name] in message


def test_sync_event_uuid4_ids_survive_restart_and_database_copy(
    tmp_path: Path,
) -> None:
    original_home = tmp_path / "original"
    copied_home = tmp_path / "copied"
    initial = RegistryStore(original_home)
    try:
        before_restart = {
            initial.enqueue_sync_event("run_update", {"sequence": 1})["id"],
            initial.enqueue_object_version_sync_once({"source_version_id": "source-before-restart"})["id"],
        }
    finally:
        initial.close()

    restarted = RegistryStore(original_home)
    try:
        after_restart = {
            restarted.enqueue_sync_event("run_update", {"sequence": 2})["id"],
            restarted.enqueue_object_version_sync_once({"source_version_id": "source-after-restart"})["id"],
        }
    finally:
        restarted.close()

    copied_home.mkdir(parents=True)
    shutil.copy2(original_home / "daemon.sqlite3", copied_home / "daemon.sqlite3")
    original = RegistryStore(original_home)
    copied = RegistryStore(copied_home)
    try:
        after_copy = {
            original.enqueue_sync_event("run_update", {"copy": "original"})["id"],
            original.enqueue_object_version_sync_once({"source_version_id": "source-original-copy"})["id"],
            copied.enqueue_sync_event("run_update", {"copy": "copied"})["id"],
            copied.enqueue_object_version_sync_once({"source_version_id": "source-copied-copy"})["id"],
        }
    finally:
        original.close()
        copied.close()

    all_generated = before_restart | after_restart | after_copy
    assert len(all_generated) == 8
    assert before_restart.isdisjoint(after_restart)
    assert (before_restart | after_restart).isdisjoint(after_copy)
    assert all(UUID(hex=event_id).version == 4 for event_id in all_generated)


def test_collision_error_contract_carries_exact_event_id() -> None:
    message, code, event_id = _error_response(
        json.dumps(
            {
                "error": "sync event id was reused with different content: event-1",
                "code": SYNC_EVENT_IDENTITY_COLLISION_ERROR_CODE,
                "event_id": "event-1",
            }
        )
    )
    error = ServerClientError(409, message, code=code, event_id=event_id)

    assert message.endswith("event-1")
    assert error.event_id == "event-1"
    assert is_sync_event_identity_collision_error(error)
    assert not is_sync_event_identity_collision_error(
        ServerClientError(
            409,
            message,
            code=SYNC_EVENT_IDENTITY_COLLISION_ERROR_CODE,
        )
    )


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection: object, *, token: str) -> None:
        pass

    def ensure_server_heartbeat(self, connection: object | None = None) -> None:
        pass

    def status(self, connection_id: str | None = None) -> dict[str, object]:
        return {
            "connection_id": connection_id,
            "thread_alive": False,
            "last_tick_at": None,
        }

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


def _remote_connection() -> dict[str, Any]:
    return {
        "id": "remote-connection-1",
        "owner_id": "owner-a",
        "subject_type": "machine",
        "subject_id": "machine-1",
        "machine_id": "machine-1",
        "display_name": "machine-1",
        "capabilities": {},
        "status": "connected",
        "heartbeat_interval_seconds": 60,
    }


def test_batch_collision_terminally_fails_only_named_event_without_reconnect(
    tmp_path: Path,
) -> None:
    class CollisionOnceServer:
        collision_event_id = ""
        calls: list[list[str]] = []
        collided = False

        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def latest_machine_library_snapshot(self, machine_id: str) -> dict[str, Any]:
            del machine_id
            return {}

        def heartbeat_connection(self, **kwargs: Any) -> dict[str, Any]:
            del kwargs
            return _remote_connection()

        def sync(self, **kwargs: Any) -> dict[str, Any]:
            events = list(kwargs["events"])
            type(self).calls.append([str(event["id"]) for event in events])
            if not type(self).collided:
                type(self).collided = True
                raise ServerClientError(
                    409,
                    "sync event id was reused with different content",
                    code=SYNC_EVENT_IDENTITY_COLLISION_ERROR_CODE,
                    event_id=type(self).collision_event_id,
                )
            return {
                "connection": _remote_connection(),
                "event_results": [
                    {
                        "event_id": event["id"],
                        "kind": event["kind"],
                        "status": "ok",
                        "result": {},
                    }
                    for event in events
                ],
                "jobs": [],
            }

    store = RegistryStore(tmp_path)
    runtime: DaemonRuntime | None = None
    try:
        connection = store.save_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-secret",
            user_token="user-token-secret",
            connection=_remote_connection(),
            heartbeat_interval_seconds=60,
        )
        runtime = DaemonRuntime(
            store,
            heartbeat_service=_NoopHeartbeats(),
            server_client_factory=CollisionOnceServer,  # type: ignore[arg-type]
        )
        credentials = store.get_server_connection_credentials(connection["id"])
        runtime._mark_server_channel_success(credentials)  # noqa: SLF001
        snapshot_hash, _ = runtime.build_machine_library_snapshot_manifest()
        store.record_server_connection_library_snapshot(
            connection["id"],
            snapshot_hash=snapshot_hash,
        )
        events = [
            store.enqueue_sync_event(
                "local_run_update",
                {"owner_id": "owner-a", "run": {"id": f"run-{index}"}},
            )
            for index in range(3)
        ]
        CollisionOnceServer.collision_event_id = events[1]["id"]

        first = runtime.sync_once()

        assert first["partial"] is False
        assert first["event_results"] == [
            {
                "event_id": events[1]["id"],
                "kind": "local_run_update",
                "status": "error",
                "error": "sync event id was reused with different content",
                "code": SYNC_EVENT_IDENTITY_COLLISION_ERROR_CODE,
            }
        ]
        assert store.get_sync_event(events[1]["id"])["status"] == "failed"
        assert store.get_sync_event(events[1]["id"])["retry"]["will_retry"] is False
        assert store.get_sync_event(events[0]["id"])["status"] == "pending"
        assert store.get_sync_event(events[2]["id"])["status"] == "pending"
        current_connection = store.get_server_connection(connection["id"])
        assert current_connection["status"] == "connected"
        assert current_connection["error"] is None

        second = runtime.sync_once()

        assert second["partial"] is False
        assert store.get_sync_event(events[0]["id"])["status"] == "sent"
        assert store.get_sync_event(events[2]["id"])["status"] == "sent"
        assert store.get_sync_event(events[1]["id"])["status"] == "failed"
        assert len(CollisionOnceServer.calls) == 2
    finally:
        if runtime is not None:
            runtime.shutdown()
        store.close()
