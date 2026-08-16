from __future__ import annotations

from http import HTTPStatus
import threading
from typing import Any

import pytest

from spl.daemon.home_lock import DaemonInstanceIdentity
from spl.daemon.lifecycle import (
    BLOCKER_KEYS,
    ROOT_WORK_KINDS,
    LifecycleAdmissionClosed,
    LifecycleController,
    LifecycleError,
)


PROOF = "supervisor-proof-value"


def _identity() -> DaemonInstanceIdentity:
    return DaemonInstanceIdentity(
        instance_id="a" * 32,
        home_hash="b" * 64,
        generation=7,
        previous_generation=6,
        pid=123,
        started_at="2026-08-02T10:00:00+00:00",
    )


def _controller(
    *,
    exit_callback: Any = None,
    clock: Any = None,
) -> LifecycleController:
    kwargs: dict[str, Any] = {
        "supervisor_id": "supervisor-1",
        "supervisor_proof": PROOF,
        "binding_id": "binding-1",
        "exit_callback": exit_callback,
    }
    if clock is not None:
        kwargs["clock"] = clock
    return LifecycleController(_identity(), **kwargs)


def _drain_request(
    controller: LifecycleController,
    *,
    request_id: str = "request-1",
    deadline_ms: int = 30_000,
) -> dict[str, Any]:
    return {
        "schema": "spl.daemon.lifecycle-drain-request",
        "schema_version": 1,
        "expected_instance_id": _identity().instance_id,
        "expected_generation": _identity().generation,
        "expected_admission_revision": controller.admission_revision,
        "request_id": request_id,
        "mode": "when_idle",
        "deadline_ms": deadline_ms,
    }


def _cancel_request(
    controller: LifecycleController,
    drain_id: str,
    *,
    request_id: str = "cancel-1",
) -> dict[str, Any]:
    return {
        "schema": "spl.daemon.lifecycle-cancel-request",
        "schema_version": 1,
        "expected_instance_id": _identity().instance_id,
        "expected_generation": _identity().generation,
        "expected_admission_revision": controller.admission_revision,
        "request_id": request_id,
        "drain_id": drain_id,
    }


def test_drain_closes_root_admission_but_retains_descendants_until_lineage_seal() -> None:
    exited = threading.Event()
    controller = _controller(exit_callback=exited.set)
    root = controller.reserve_root("local_run", queued=True)
    root.activate()
    root.attach_current_thread()
    terminal = controller.reserve_descendant("terminal_commit", root.lineage_id)

    receipt = controller.request_drain(
        _drain_request(controller),
        supervisor_proof=PROOF,
    )

    assert receipt["claim_boundary"] == "pending"
    assert receipt["blockers"]["local_run"] == 1
    assert receipt["blockers"]["terminal_commit"] == 1
    with pytest.raises(LifecycleAdmissionClosed):
        controller.reserve_root("guarded_local_run")

    controller.mark_claim_boundary_not_required()
    terminal.complete()
    assert not exited.is_set()
    root.detach_current_thread()
    root.complete(seal_lineage=True)
    assert exited.wait(1)
    assert controller.status_document()["state"] == "stopping"


def test_preboundary_sync_claim_is_admitted_after_drain_begins_and_blocks_stop() -> None:
    exited = threading.Event()
    controller = _controller(exit_callback=exited.set)
    sync, claim_jobs = controller.begin_sync()
    assert claim_jobs is True
    sync.activate()

    controller.request_drain(_drain_request(controller), supervisor_proof=PROOF)
    claimed = controller.admit_remote_claim(sync)
    assert claimed.root is True
    assert claimed.lineage_id != sync.lineage_id
    claimed.activate()
    drain_sync, drain_claim_jobs = controller.begin_sync()
    assert drain_claim_jobs is False
    drain_sync.activate()

    controller.mark_claim_boundary_proven()
    sync.complete(seal_lineage=True)
    drain_sync.complete(seal_lineage=True)
    assert controller.status_document()["drain"]["blockers"]["remote_claim"] == 1
    assert not exited.is_set()

    terminal = controller.reserve_descendant("registry_mutation", claimed.lineage_id)
    terminal.activate()
    terminal.complete()
    claimed.complete(seal_lineage=True)
    assert exited.wait(1)


def test_remote_claim_from_no_claim_sync_fails_closed_as_unknown_work() -> None:
    controller = _controller()
    controller.request_drain(_drain_request(controller), supervisor_proof=PROOF)
    drain_sync, claim_jobs = controller.begin_sync()
    assert claim_jobs is False
    drain_sync.activate()

    with pytest.raises(LifecycleError) as captured:
        controller.admit_remote_claim(drain_sync)
    assert captured.value.code == "remote_claim_without_admitted_sync"
    assert controller.status_document()["drain"]["blockers"]["unknown_work"] == 1
    drain_sync.complete(seal_lineage=True)


def test_every_drain_sync_gets_a_fresh_sealable_no_claim_lineage() -> None:
    controller = _controller()
    controller.request_drain(_drain_request(controller), supervisor_proof=PROOF)

    first, first_claims = controller.begin_sync()
    assert first_claims is False
    first.activate()
    controller.mark_claim_boundary_proven()
    first.complete(seal_lineage=True)

    second, second_claims = controller.begin_sync()
    assert second_claims is False
    assert second.lineage_id != first.lineage_id
    second.activate()
    second.complete(seal_lineage=True)
    assert controller.status_document()["drain"]["blockers"]["sync_flush"] == 0


def test_unknown_no_claim_outcome_survives_cancel_and_blocks_a_later_stop() -> None:
    exited = threading.Event()
    controller = _controller(exit_callback=exited.set)
    receipt = controller.request_drain(_drain_request(controller), supervisor_proof=PROOF)
    controller.mark_claim_boundary_failed(outcome_unknown=True)
    cancel = controller.cancel_drain(
        _cancel_request(controller, str(receipt["drain_id"])),
        supervisor_proof=PROOF,
    )
    assert cancel["state"] == "healthy"

    controller.request_drain(
        _drain_request(controller, request_id="request-2"),
        supervisor_proof=PROOF,
    )
    controller.mark_claim_boundary_not_required()
    status = controller.status_document()
    assert status["drain"]["blockers"]["outcome_unknown"] == 1
    assert not exited.is_set()


def test_drain_and_cancel_are_exactly_fenced_and_idempotent() -> None:
    controller = _controller()
    request = _drain_request(controller)

    with pytest.raises(LifecycleError) as missing_proof:
        controller.request_drain(request, supervisor_proof=None)
    assert missing_proof.value.code == "lifecycle_ownership_unavailable"
    assert missing_proof.value.status == HTTPStatus.FORBIDDEN

    receipt = controller.request_drain(request, supervisor_proof=PROOF)
    assert controller.request_drain(request, supervisor_proof=PROOF)["drain_id"] == receipt["drain_id"]
    changed = {**request, "deadline_ms": request["deadline_ms"] + 1}
    with pytest.raises(LifecycleError) as replay:
        controller.request_drain(changed, supervisor_proof=PROOF)
    assert replay.value.code == "request_replay_mismatch"

    cancel_request = _cancel_request(controller, str(receipt["drain_id"]))
    cancelled = controller.cancel_drain(cancel_request, supervisor_proof=PROOF)
    assert controller.cancel_drain(cancel_request, supervisor_proof=PROOF) == cancelled


def test_deadline_expiry_never_forces_active_work() -> None:
    now = [100.0]
    exited = threading.Event()
    controller = _controller(exit_callback=exited.set, clock=lambda: now[0])
    active = controller.reserve_root("environment_build", queued=False)
    active.activate()
    controller.request_drain(
        _drain_request(controller, deadline_ms=100),
        supervisor_proof=PROOF,
    )
    controller.mark_claim_boundary_not_required()

    now[0] = 1000.0
    status = controller.status_document()
    assert status["drain"]["phase"] == "deadline_expired"
    assert status["state"] == "draining"
    assert not exited.is_set()

    active.complete(seal_lineage=True)
    assert exited.wait(1)


def test_blocker_catalog_is_closed_and_nested_thread_context_is_restored() -> None:
    controller = _controller()
    root = controller.reserve_root("remote_claim", queued=False)
    root.activate()
    root.attach_current_thread()
    child = controller.reserve_current_or_root("registry_mutation")
    with child:
        assert controller.current_lineage_id == root.lineage_id
    assert controller.current_lineage_id == root.lineage_id
    root.detach_current_thread()
    root.complete(seal_lineage=True)

    status = controller.status_document()
    assert status["drain"] is None
    controller.request_drain(_drain_request(controller), supervisor_proof=PROOF)
    assert tuple(controller.status_document()["drain"]["blockers"]) == BLOCKER_KEYS


def test_drain_admission_race_never_accepts_root_work_after_closed_revision() -> None:
    for kind in sorted(ROOT_WORK_KINDS):
        for index in range(10):
            controller = _controller()
            barrier = threading.Barrier(2)
            outcomes: list[str] = []

            def admit() -> None:
                barrier.wait()
                try:
                    lease = controller.reserve_root(kind)
                except LifecycleAdmissionClosed:
                    outcomes.append("closed")
                else:
                    outcomes.append("admitted")
                    lease.complete(seal_lineage=True)

            thread = threading.Thread(target=admit)
            thread.start()
            barrier.wait()
            controller.request_drain(
                _drain_request(controller, request_id=f"request-{kind}-{index}"),
                supervisor_proof=PROOF,
            )
            thread.join(timeout=1)
            assert outcomes in (["closed"], ["admitted"])
            with pytest.raises(LifecycleAdmissionClosed):
                controller.reserve_root(kind)
