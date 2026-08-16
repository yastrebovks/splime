from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator

import pytest

from spl.daemon.home_lock import DaemonHomeLock
from spl.daemon.lifecycle_startup import FileIdentity, StartupBinding
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore


API_TOKEN = "daemon-master-test-token"
SUPERVISOR_PROOF = "supervisor-proof-value"


@pytest.fixture
def lifecycle_app(tmp_path: Path) -> Iterator[Any]:
    home = tmp_path / "daemon-home"
    lock = DaemonHomeLock(home)
    identity = lock.acquire()
    store = RegistryStore(home)
    executable = FileIdentity(
        path_sha256="sha256:" + "a" * 64,
        device=1,
        inode=2,
        owner_uid=501,
        mode=0o755,
        sha256="sha256:" + "b" * 64,
    )
    home_identity = FileIdentity(
        path_sha256="sha256:" + "c" * 64,
        device=3,
        inode=4,
        owner_uid=501,
        mode=0o700,
        sha256=None,
    )
    binding = StartupBinding(
        supervisor_id="supervisor-1",
        spawn_nonce="spawn-nonce-value",
        supervisor_proof=SUPERVISOR_PROOF,
        executable=executable,
        home=home_identity,
        release_version="0.4.6",
    )
    app = create_app(
        store,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
        startup_binding=binding,
        startup_supervisor_proof=SUPERVISOR_PROOF,
    )
    try:
        yield app
    finally:
        app.runtime.shutdown()
        store.close()
        lock.release()


def _auth_headers(*, proof: bool = False) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    if proof:
        headers["X-SPL-Supervisor-Proof"] = SUPERVISOR_PROOF
    return headers


def _drain_request(status: dict[str, Any], **updates: Any) -> dict[str, Any]:
    request = {
        "schema": "spl.daemon.lifecycle-drain-request",
        "schema_version": 1,
        "expected_instance_id": status["instance"]["instance_id"],
        "expected_generation": status["instance"]["generation"],
        "expected_admission_revision": status["admission"]["revision"],
        "request_id": "request-1",
        "mode": "when_idle",
        "deadline_ms": 30_000,
    }
    request.update(updates)
    return request


@pytest.mark.asyncio
async def test_lifecycle_status_is_authenticated_closed_and_no_store(lifecycle_app: Any) -> None:
    client = lifecycle_app.test_client()
    denied = await client.get("/lifecycle/status")
    assert denied.status_code == 401
    assert denied.headers["Cache-Control"] == "no-store"
    assert denied.headers["Pragma"] == "no-cache"

    response = await client.get("/lifecycle/status", headers=_auth_headers())
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Pragma"] == "no-cache"
    body = await response.get_json()
    assert set(body) == {
        "schema",
        "schema_version",
        "instance",
        "state",
        "ownership",
        "admission",
        "drain",
        "observed_at",
    }
    serialized = json.dumps(body)
    assert API_TOKEN not in serialized
    assert SUPERVISOR_PROOF not in serialized
    assert "pid" not in serialized.casefold()
    assert str(lifecycle_app.runtime.store.home) not in serialized


@pytest.mark.asyncio
async def test_drain_requires_bearer_and_supervisor_proof_then_cancel_is_exact(lifecycle_app: Any) -> None:
    client = lifecycle_app.test_client()
    status = await (await client.get("/lifecycle/status", headers=_auth_headers())).get_json()
    request = _drain_request(status)

    no_proof = await client.post(
        "/lifecycle/drain",
        json=request,
        headers=_auth_headers(),
    )
    assert no_proof.status_code == 403
    assert (await no_proof.get_json())["code"] == "lifecycle_ownership_unavailable"

    response = await client.post(
        "/lifecycle/drain",
        json=request,
        headers=_auth_headers(proof=True),
    )
    assert response.status_code == 202
    assert response.headers["Cache-Control"] == "no-store"
    receipt = await response.get_json()
    assert receipt["schema"] == "spl.daemon.lifecycle-drain-receipt"
    assert receipt["request_id"] == request["request_id"]

    current = await (await client.get("/lifecycle/status", headers=_auth_headers())).get_json()
    cancel = {
        "schema": "spl.daemon.lifecycle-cancel-request",
        "schema_version": 1,
        "expected_instance_id": status["instance"]["instance_id"],
        "expected_generation": status["instance"]["generation"],
        "expected_admission_revision": current["admission"]["revision"],
        "request_id": "cancel-1",
        "drain_id": receipt["drain_id"],
    }
    cancelled = await client.post(
        "/lifecycle/cancel",
        json=cancel,
        headers=_auth_headers(proof=True),
    )
    assert cancelled.status_code == 200
    assert (await cancelled.get_json())["cancelled_drain_id"] == receipt["drain_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b"{}",
        b'{"schema":"x","schema":"y"}',
        b'{"schema":NaN}',
        b"[]",
        b"{" + b"x" * (8 * 1024) + b"}",
    ],
)
async def test_lifecycle_mutations_reject_malformed_duplicate_and_oversized_bodies(
    lifecycle_app: Any,
    body: bytes,
) -> None:
    response = await lifecycle_app.test_client().post(
        "/lifecycle/drain",
        data=body,
        headers={**_auth_headers(proof=True), "Content-Type": "application/json"},
    )
    assert response.status_code in {400, 413}
    assert response.headers["Cache-Control"] == "no-store"
    document = await response.get_json()
    assert document["schema"] == "spl.daemon.lifecycle-error"
    assert API_TOKEN not in json.dumps(document)
    assert SUPERVISOR_PROOF not in json.dumps(document)


@pytest.mark.asyncio
async def test_startup_binding_receipt_is_proof_bound_one_shot_and_non_sensitive(lifecycle_app: Any) -> None:
    client = lifecycle_app.test_client()
    wrong = await client.get(
        "/lifecycle/startup-binding",
        headers={**_auth_headers(), "X-SPL-Supervisor-Proof": "wrong-proof-value"},
    )
    assert wrong.status_code == 409

    response = await client.get(
        "/lifecycle/startup-binding",
        headers=_auth_headers(proof=True),
    )
    assert response.status_code == 200
    receipt = await response.get_json()
    assert receipt["schema"] == "spl.daemon.lifecycle-startup-receipt"
    serialized = json.dumps(receipt)
    assert SUPERVISOR_PROOF not in serialized
    assert "spawn-nonce" not in serialized
    assert "pid" not in serialized.casefold()
    assert str(lifecycle_app.runtime.store.home) not in serialized

    replay = await client.get(
        "/lifecycle/startup-binding",
        headers=_auth_headers(proof=True),
    )
    assert replay.status_code == 409
