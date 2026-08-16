"""Instance-fenced daemon lifecycle admission and drain coordination.

This module is daemon-owned and client neutral.  It deliberately contains no
Jupyter or plugin import and exposes no PID or signal operation.
"""

from __future__ import annotations

import hashlib
import re
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from typing import Any, Callable, Literal, Mapping
from uuid import uuid4

from spl.daemon.home_lock import DaemonInstanceIdentity
from spl.daemon.remote_client import SYNC_FLUSH_WITHOUT_CLAIM_CAPABILITY

LIFECYCLE_SCHEMA_VERSION = 1
LIFECYCLE_STATUS_SCHEMA = "spl.daemon.lifecycle-status"
LIFECYCLE_DRAIN_REQUEST_SCHEMA = "spl.daemon.lifecycle-drain-request"
LIFECYCLE_DRAIN_RECEIPT_SCHEMA = "spl.daemon.lifecycle-drain-receipt"
LIFECYCLE_CANCEL_REQUEST_SCHEMA = "spl.daemon.lifecycle-cancel-request"
LIFECYCLE_CANCEL_RECEIPT_SCHEMA = "spl.daemon.lifecycle-cancel-receipt"
LIFECYCLE_STARTUP_RECEIPT_SCHEMA = "spl.daemon.lifecycle-startup-receipt"
LIFECYCLE_ERROR_SCHEMA = "spl.daemon.lifecycle-error"
LIFECYCLE_CAPABILITY = "daemon.lifecycle.when_idle"
LIFECYCLE_CAPABILITY_VERSION = 1

MIN_DRAIN_DEADLINE_MS = 100
MAX_DRAIN_DEADLINE_MS = 24 * 60 * 60 * 1000
MAX_SAFE_INTEGER = 9_007_199_254_740_991

ROOT_WORK_KINDS = frozenset(
    {
        "local_run",
        "guarded_local_run",
        "remote_claim",
        "environment_build",
        "registry_mutation",
        "sync_poll",
        "callback",
    }
)
DESCENDANT_WORK_KINDS = frozenset(
    {
        "local_run",
        "guarded_local_run",
        "environment_build",
        "registry_mutation",
        "terminal_commit",
        "callback",
        "sync_flush",
    }
)
WORK_KINDS = ROOT_WORK_KINDS | DESCENDANT_WORK_KINDS
BLOCKER_KEYS = (
    "local_run",
    "guarded_local_run",
    "remote_claim",
    "environment_build",
    "registry_mutation",
    "terminal_commit",
    "sync_poll",
    "sync_flush",
    "callback",
    "lineage_completion",
    "queued_work",
    "unknown_work",
    "outcome_unknown",
)

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")


class LifecycleError(RuntimeError):
    """Closed lifecycle failure with a stable status and code."""

    def __init__(
        self,
        code: str,
        status: HTTPStatus,
        *,
        operation: str,
        retryable: bool = False,
    ) -> None:
        self.code = code
        self.status = status
        self.operation = operation
        self.retryable = retryable
        super().__init__(code)


class LifecycleAdmissionClosed(LifecycleError):
    """Raised when new root work is attempted after drain admission closes."""

    def __init__(self, operation: str) -> None:
        super().__init__(
            "daemon_draining",
            HTTPStatus.CONFLICT,
            operation=operation,
            retryable=True,
        )


@dataclass
class _WorkRecord:
    work_id: str
    kind: str
    lineage_id: str
    phase: Literal["queued", "active"]


@dataclass
class _LineageRecord:
    lineage_id: str
    sealed: bool = False


@dataclass(frozen=True)
class _DrainRecord:
    drain_id: str
    request_id: str
    mode: Literal["when_idle"]
    deadline_at: str
    deadline_monotonic: float
    admission_revision: int
    request_fingerprint: str


class LifecycleWorkLease:
    """One exact admitted unit of daemon work.

    A lease is intentionally process-local.  It cannot be reconstructed from a
    Run row, endpoint document, PID, lock, or caller-supplied identifier.
    """

    def __init__(
        self,
        controller: LifecycleController,
        *,
        work_id: str,
        lineage_id: str,
        root: bool,
    ) -> None:
        self._controller = controller
        self.work_id = work_id
        self.lineage_id = lineage_id
        self.root = root
        self._closed = False

    def activate(self) -> None:
        self._controller._activate_work(self)

    def attach_current_thread(self) -> None:
        self._controller._attach_current(self)

    def detach_current_thread(self) -> None:
        self._controller._detach_current(self)

    def complete(self, *, seal_lineage: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        self._controller._complete_work(self, seal_lineage=seal_lineage)

    def __enter__(self) -> LifecycleWorkLease:
        self.activate()
        self.attach_current_thread()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc_type, exc, traceback
        self.detach_current_thread()
        self.complete(seal_lineage=self.root)


class LifecycleController:
    """Own atomic root admission, blockers, lineages, and self-exit commit."""

    def __init__(
        self,
        identity: DaemonInstanceIdentity | None,
        *,
        supervisor_id: str | None = None,
        supervisor_proof: str | None = None,
        binding_id: str | None = None,
        exit_callback: Callable[[], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.identity = identity
        self._clock = clock
        self._condition = threading.Condition(threading.RLock())
        self._thread_context = threading.local()
        self._admission_open = True
        self._revision = 1
        self._works: dict[str, _WorkRecord] = {}
        self._lineages: dict[str, _LineageRecord] = {}
        self._drain: _DrainRecord | None = None
        self._claim_boundary: Literal[
            "not_required",
            "pending",
            "proven",
            "failed",
            "outcome_unknown",
        ] = "not_required"
        self._permanent_unknown_work = 0
        self._permanent_outcome_unknown = 0
        self._stopping = False
        self._exit_requested = False
        self._exit_callback = exit_callback
        self._supervisor_id = supervisor_id
        self._supervisor_proof_digest = self._proof_digest(supervisor_proof)
        self._binding_id = binding_id
        self._last_cancel_request_fingerprint: str | None = None
        self._last_cancel_receipt: dict[str, Any] | None = None

    @property
    def admission_revision(self) -> int:
        with self._condition:
            return self._revision

    @property
    def is_draining(self) -> bool:
        with self._condition:
            return self._drain is not None

    @property
    def current_lineage_id(self) -> str | None:
        stack = getattr(self._thread_context, "lineage_stack", None)
        if not isinstance(stack, list) or not stack:
            return None
        value = stack[-1]
        return str(value) if isinstance(value, str) else None

    def bind_exit_callback(self, callback: Callable[[], None]) -> None:
        with self._condition:
            self._exit_callback = callback
            self._maybe_commit_stop_locked()

    def reserve_root(
        self,
        kind: str,
        *,
        lineage_id: str | None = None,
        queued: bool = True,
    ) -> LifecycleWorkLease:
        if kind not in ROOT_WORK_KINDS:
            raise LifecycleError(
                "unknown_work_category",
                HTTPStatus.SERVICE_UNAVAILABLE,
                operation="admit_work",
            )
        with self._condition:
            if not self._admission_open:
                raise LifecycleAdmissionClosed(kind)
            return self._reserve_locked(
                kind,
                lineage_id=lineage_id or f"lineage_{uuid4().hex}",
                root=True,
                queued=queued,
            )

    def reserve_descendant(
        self,
        kind: str,
        lineage_id: str,
        *,
        queued: bool = False,
    ) -> LifecycleWorkLease:
        if kind not in DESCENDANT_WORK_KINDS:
            raise LifecycleError(
                "unknown_work_category",
                HTTPStatus.SERVICE_UNAVAILABLE,
                operation="admit_descendant",
            )
        with self._condition:
            lineage = self._lineages.get(lineage_id)
            if lineage is None or lineage.sealed:
                self._permanent_unknown_work += 1
                self._condition.notify_all()
                raise LifecycleError(
                    "lineage_not_admitted",
                    HTTPStatus.CONFLICT,
                    operation="admit_descendant",
                )
            return self._reserve_locked(
                kind,
                lineage_id=lineage_id,
                root=False,
                queued=queued,
            )

    def reserve_current_or_root(
        self,
        kind: str,
        *,
        queued: bool = False,
    ) -> LifecycleWorkLease:
        lineage_id = self.current_lineage_id
        if lineage_id is not None and kind in DESCENDANT_WORK_KINDS:
            return self.reserve_descendant(kind, lineage_id, queued=queued)
        return self.reserve_root(kind, queued=queued)

    def begin_sync(self) -> tuple[LifecycleWorkLease, bool]:
        """Atomically admit a claim-capable poll or a draining flush."""

        with self._condition:
            if self._admission_open:
                return (
                    self._reserve_locked(
                        "sync_poll",
                        lineage_id=f"lineage_{uuid4().hex}",
                        root=True,
                        queued=False,
                    ),
                    True,
                )
            drain = self._drain
            if drain is None:
                raise LifecycleAdmissionClosed("sync_poll")
            lineage_id = f"drain_{drain.drain_id}_{uuid4().hex}"
            self._lineages.setdefault(lineage_id, _LineageRecord(lineage_id))
            return (
                self._reserve_locked(
                    "sync_flush",
                    lineage_id=lineage_id,
                    root=False,
                    queued=False,
                ),
                False,
            )

    def admit_remote_claim(self, sync_lease: LifecycleWorkLease) -> LifecycleWorkLease:
        """Admit a job returned to one exact pre-boundary sync request."""

        with self._condition:
            sync_record = self._works.get(sync_lease.work_id)
            if sync_record is None or sync_record.kind != "sync_poll":
                self._permanent_unknown_work += 1
                raise LifecycleError(
                    "remote_claim_without_admitted_sync",
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    operation="admit_remote_claim",
                )
            return self._reserve_locked(
                "remote_claim",
                # The claim is admitted only while the exact pre-boundary sync
                # lease is live, but it owns a separate completion lineage.
                # Sealing the poll must never close registration/finalizer work
                # for the remote Run that the poll already accepted.
                lineage_id=f"lineage_{uuid4().hex}",
                root=True,
                queued=True,
            )

    def seal_lineage(self, lineage_id: str) -> None:
        with self._condition:
            lineage = self._lineages.get(lineage_id)
            if lineage is None:
                self._permanent_unknown_work += 1
                raise LifecycleError(
                    "lineage_not_admitted",
                    HTTPStatus.CONFLICT,
                    operation="seal_lineage",
                )
            lineage.sealed = True
            self._drop_sealed_lineage_if_idle_locked(lineage_id)
            self._condition.notify_all()
            self._maybe_commit_stop_locked()

    def record_unknown_work(self) -> None:
        with self._condition:
            self._permanent_unknown_work += 1
            self._condition.notify_all()

    def mark_claim_boundary_not_required(self) -> None:
        with self._condition:
            self._require_drain_locked()
            self._claim_boundary = "not_required"
            self._condition.notify_all()
            self._maybe_commit_stop_locked()

    def mark_claim_boundary_proven(self) -> None:
        with self._condition:
            self._require_drain_locked()
            self._claim_boundary = "proven"
            self._condition.notify_all()
            self._maybe_commit_stop_locked()

    def mark_claim_boundary_failed(self, *, outcome_unknown: bool) -> None:
        with self._condition:
            self._require_drain_locked()
            if self._claim_boundary in {"failed", "outcome_unknown"}:
                return
            self._claim_boundary = "outcome_unknown" if outcome_unknown else "failed"
            if outcome_unknown:
                self._permanent_outcome_unknown += 1
            self._condition.notify_all()

    def request_drain(self, document: Mapping[str, Any], *, supervisor_proof: str | None) -> dict[str, Any]:
        request = parse_drain_request(document)
        fingerprint = _fingerprint(request)
        with self._condition:
            self._require_control_owner_locked(supervisor_proof, operation="drain")
            self._require_identity_locked(
                request["expected_instance_id"],
                request["expected_generation"],
                operation="drain",
            )
            if self._drain is not None:
                if self._drain.request_id == request["request_id"]:
                    if self._drain.request_fingerprint != fingerprint:
                        raise LifecycleError(
                            "request_replay_mismatch",
                            HTTPStatus.CONFLICT,
                            operation="drain",
                        )
                    return self._drain_receipt_locked()
                raise LifecycleError(
                    "drain_already_scheduled",
                    HTTPStatus.CONFLICT,
                    operation="drain",
                )
            if request["expected_admission_revision"] != self._revision:
                raise LifecycleError(
                    "admission_revision_mismatch",
                    HTTPStatus.PRECONDITION_FAILED,
                    operation="drain",
                )
            self._admission_open = False
            self._revision += 1
            deadline_ms = int(request["deadline_ms"])
            self._drain = _DrainRecord(
                drain_id=f"drain_{uuid4().hex}",
                request_id=str(request["request_id"]),
                mode="when_idle",
                deadline_at=_utc_after_milliseconds(deadline_ms),
                deadline_monotonic=self._clock() + (deadline_ms / 1000.0),
                admission_revision=self._revision,
                request_fingerprint=fingerprint,
            )
            self._claim_boundary = "pending"
            self._last_cancel_request_fingerprint = None
            self._last_cancel_receipt = None
            self._condition.notify_all()
            return self._drain_receipt_locked()

    def cancel_drain(self, document: Mapping[str, Any], *, supervisor_proof: str | None) -> dict[str, Any]:
        request = parse_cancel_request(document)
        fingerprint = _fingerprint(request)
        with self._condition:
            self._require_control_owner_locked(supervisor_proof, operation="cancel")
            self._require_identity_locked(
                request["expected_instance_id"],
                request["expected_generation"],
                operation="cancel",
            )
            if self._drain is None:
                if self._last_cancel_request_fingerprint == fingerprint and self._last_cancel_receipt is not None:
                    return dict(self._last_cancel_receipt)
                raise LifecycleError(
                    "drain_not_scheduled",
                    HTTPStatus.CONFLICT,
                    operation="cancel",
                )
            if self._stopping:
                raise LifecycleError(
                    "stop_commit_started",
                    HTTPStatus.CONFLICT,
                    operation="cancel",
                )
            if request["drain_id"] != self._drain.drain_id:
                raise LifecycleError(
                    "drain_id_mismatch",
                    HTTPStatus.PRECONDITION_FAILED,
                    operation="cancel",
                )
            if request["expected_admission_revision"] != self._revision:
                raise LifecycleError(
                    "admission_revision_mismatch",
                    HTTPStatus.PRECONDITION_FAILED,
                    operation="cancel",
                )
            cancelled_drain_id = self._drain.drain_id
            self._drain = None
            self._claim_boundary = "not_required"
            self._admission_open = True
            self._revision += 1
            self._drop_drain_lineages_locked()
            receipt = {
                "schema": LIFECYCLE_CANCEL_RECEIPT_SCHEMA,
                "schema_version": LIFECYCLE_SCHEMA_VERSION,
                "instance_id": self._identity().instance_id,
                "generation": self._identity().generation,
                "request_id": request["request_id"],
                "cancelled_drain_id": cancelled_drain_id,
                "admission_revision": self._revision,
                "state": "healthy",
                "observed_at": _utc_now(),
            }
            self._last_cancel_request_fingerprint = fingerprint
            self._last_cancel_receipt = dict(receipt)
            self._condition.notify_all()
            return receipt

    def status_document(self) -> dict[str, Any]:
        with self._condition:
            return self._status_document_locked()

    def error_document(self, error: LifecycleError) -> dict[str, Any]:
        return {
            "schema": LIFECYCLE_ERROR_SCHEMA,
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "code": error.code,
            "operation": error.operation,
            "retryable": error.retryable,
        }

    def _reserve_locked(
        self,
        kind: str,
        *,
        lineage_id: str,
        root: bool,
        queued: bool,
    ) -> LifecycleWorkLease:
        if kind not in WORK_KINDS:
            raise AssertionError(f"unregistered lifecycle work kind: {kind}")
        lineage = self._lineages.setdefault(lineage_id, _LineageRecord(lineage_id))
        if lineage.sealed:
            raise LifecycleError(
                "lineage_already_sealed",
                HTTPStatus.CONFLICT,
                operation="admit_work",
            )
        work_id = f"work_{uuid4().hex}"
        self._works[work_id] = _WorkRecord(
            work_id=work_id,
            kind=kind,
            lineage_id=lineage_id,
            phase="queued" if queued else "active",
        )
        self._condition.notify_all()
        return LifecycleWorkLease(
            self,
            work_id=work_id,
            lineage_id=lineage_id,
            root=root,
        )

    def _activate_work(self, lease: LifecycleWorkLease) -> None:
        with self._condition:
            record = self._works.get(lease.work_id)
            if record is None:
                raise LifecycleError(
                    "work_lease_not_active",
                    HTTPStatus.CONFLICT,
                    operation="activate_work",
                )
            record.phase = "active"
            self._condition.notify_all()

    def _attach_current(self, lease: LifecycleWorkLease) -> None:
        current = self.current_lineage_id
        if current is not None and current != lease.lineage_id:
            raise LifecycleError(
                "thread_lineage_conflict",
                HTTPStatus.CONFLICT,
                operation="activate_work",
            )
        stack = getattr(self._thread_context, "lineage_stack", None)
        if not isinstance(stack, list):
            stack = []
            self._thread_context.lineage_stack = stack
        stack.append(lease.lineage_id)

    def _detach_current(self, lease: LifecycleWorkLease) -> None:
        stack = getattr(self._thread_context, "lineage_stack", None)
        if isinstance(stack, list) and stack and stack[-1] == lease.lineage_id:
            stack.pop()

    def _complete_work(self, lease: LifecycleWorkLease, *, seal_lineage: bool) -> None:
        with self._condition:
            record = self._works.pop(lease.work_id, None)
            if record is None:
                return
            if seal_lineage:
                lineage = self._lineages.get(record.lineage_id)
                if lineage is not None:
                    lineage.sealed = True
            self._drop_sealed_lineage_if_idle_locked(record.lineage_id)
            self._condition.notify_all()
            self._maybe_commit_stop_locked()

    def _drop_sealed_lineage_if_idle_locked(self, lineage_id: str) -> None:
        lineage = self._lineages.get(lineage_id)
        if lineage is None or not lineage.sealed:
            return
        if any(record.lineage_id == lineage_id for record in self._works.values()):
            return
        self._lineages.pop(lineage_id, None)

    def _drop_drain_lineages_locked(self) -> None:
        for lineage_id in tuple(self._lineages):
            if not lineage_id.startswith("drain_"):
                continue
            if any(record.lineage_id == lineage_id for record in self._works.values()):
                continue
            self._lineages.pop(lineage_id, None)

    def _blockers_locked(self) -> dict[str, int]:
        blockers = {key: 0 for key in BLOCKER_KEYS}
        for record in self._works.values():
            blockers[record.kind] += 1
            if record.phase == "queued":
                blockers["queued_work"] += 1
        for lineage in self._lineages.values():
            if lineage.sealed:
                continue
            if not any(record.lineage_id == lineage.lineage_id for record in self._works.values()):
                blockers["lineage_completion"] += 1
        blockers["unknown_work"] = self._permanent_unknown_work
        blockers["outcome_unknown"] = self._permanent_outcome_unknown
        return blockers

    def _phase_locked(self) -> str | None:
        if self._drain is None:
            return None
        if self._stopping:
            return "stop_commit_started"
        if self._claim_boundary == "pending":
            return "establishing_no_claim_boundary"
        if self._clock() >= self._drain.deadline_monotonic:
            return "deadline_expired"
        return "waiting_for_idle"

    def _state_locked(self) -> str:
        if self._stopping:
            return "stopping"
        if self._drain is None:
            return "healthy"
        if self._phase_locked() == "waiting_for_idle":
            return "stop_scheduled"
        return "draining"

    def _status_document_locked(self) -> dict[str, Any]:
        identity = self._identity()
        drain = None
        if self._drain is not None:
            drain = {
                "drain_id": self._drain.drain_id,
                "request_id": self._drain.request_id,
                "mode": self._drain.mode,
                "phase": self._phase_locked(),
                "deadline_at": self._drain.deadline_at,
                "claim_boundary": self._claim_boundary,
                "blockers": self._blockers_locked(),
            }
        return {
            "schema": LIFECYCLE_STATUS_SCHEMA,
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "instance": {
                "instance_id": identity.instance_id,
                "generation": identity.generation,
            },
            "state": self._state_locked(),
            "ownership": {
                "state": "bound" if self._supervisor_proof_digest is not None else "unbound",
                "binding_id": self._binding_id,
            },
            "admission": {
                "new_root_work": "open" if self._admission_open else "closed",
                "revision": self._revision,
            },
            "drain": drain,
            "observed_at": _utc_now(),
        }

    def _drain_receipt_locked(self) -> dict[str, Any]:
        drain = self._require_drain_locked()
        identity = self._identity()
        return {
            "schema": LIFECYCLE_DRAIN_RECEIPT_SCHEMA,
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "instance_id": identity.instance_id,
            "generation": identity.generation,
            "request_id": drain.request_id,
            "drain_id": drain.drain_id,
            "admission_revision": self._revision,
            "mode": drain.mode,
            "phase": self._phase_locked(),
            "deadline_at": drain.deadline_at,
            "claim_boundary": self._claim_boundary,
            "blockers": self._blockers_locked(),
            "observed_at": _utc_now(),
        }

    def _maybe_commit_stop_locked(self) -> None:
        if self._drain is None or self._stopping:
            return
        if self._claim_boundary not in {"not_required", "proven"}:
            return
        if any(self._blockers_locked().values()):
            return
        if self._exit_callback is None:
            return
        self._stopping = True
        if self._exit_requested:
            return
        self._exit_requested = True
        callback = self._exit_callback

        def request_exit() -> None:
            try:
                callback()
            except Exception:
                # The caller observes the retained child.  There is no signal,
                # PID, or forced fallback if the in-process trigger fails.
                with self._condition:
                    self._permanent_outcome_unknown += 1
                    self._stopping = False
                    self._exit_requested = False
                    self._condition.notify_all()

        threading.Thread(
            target=request_exit,
            name="spl-daemon-self-exit-request",
            daemon=True,
        ).start()

    def _require_control_owner_locked(self, proof: str | None, *, operation: str) -> None:
        if self._supervisor_proof_digest is None or proof is None:
            raise LifecycleError(
                "lifecycle_ownership_unavailable",
                HTTPStatus.FORBIDDEN,
                operation=operation,
            )
        if not secrets.compare_digest(self._supervisor_proof_digest, self._proof_digest(proof) or b""):
            raise LifecycleError(
                "lifecycle_ownership_mismatch",
                HTTPStatus.FORBIDDEN,
                operation=operation,
            )

    def _require_identity_locked(
        self,
        instance_id: Any,
        generation: Any,
        *,
        operation: str,
    ) -> None:
        identity = self._identity()
        if instance_id != identity.instance_id:
            raise LifecycleError(
                "daemon_instance_mismatch",
                HTTPStatus.PRECONDITION_FAILED,
                operation=operation,
            )
        if generation != identity.generation:
            raise LifecycleError(
                "daemon_generation_mismatch",
                HTTPStatus.PRECONDITION_FAILED,
                operation=operation,
            )

    def _require_drain_locked(self) -> _DrainRecord:
        if self._drain is None:
            raise LifecycleError(
                "drain_not_scheduled",
                HTTPStatus.CONFLICT,
                operation="drain",
            )
        return self._drain

    def _identity(self) -> DaemonInstanceIdentity:
        if self.identity is None:
            raise LifecycleError(
                "live_identity_unavailable",
                HTTPStatus.SERVICE_UNAVAILABLE,
                operation="status",
            )
        return self.identity

    @staticmethod
    def _proof_digest(proof: str | None) -> bytes | None:
        if proof is None:
            return None
        return hashlib.sha256(proof.encode("utf-8")).digest()


def parse_drain_request(value: Mapping[str, Any]) -> dict[str, Any]:
    expected = frozenset(
        {
            "schema",
            "schema_version",
            "expected_instance_id",
            "expected_generation",
            "expected_admission_revision",
            "request_id",
            "mode",
            "deadline_ms",
        }
    )
    document = _closed_mapping(value, expected)
    if document["schema"] != LIFECYCLE_DRAIN_REQUEST_SCHEMA:
        raise LifecycleError("request_invalid", HTTPStatus.BAD_REQUEST, operation="drain")
    _require_schema_version(document["schema_version"], operation="drain")
    _require_safe_id(document["expected_instance_id"], operation="drain")
    _require_positive_safe_integer(document["expected_generation"], operation="drain")
    _require_positive_safe_integer(document["expected_admission_revision"], operation="drain")
    _require_safe_id(document["request_id"], operation="drain")
    if document["mode"] != "when_idle":
        raise LifecycleError("request_invalid", HTTPStatus.BAD_REQUEST, operation="drain")
    deadline_ms = document["deadline_ms"]
    if (
        isinstance(deadline_ms, bool)
        or not isinstance(deadline_ms, int)
        or not MIN_DRAIN_DEADLINE_MS <= deadline_ms <= MAX_DRAIN_DEADLINE_MS
    ):
        raise LifecycleError("request_invalid", HTTPStatus.BAD_REQUEST, operation="drain")
    return document


def parse_cancel_request(value: Mapping[str, Any]) -> dict[str, Any]:
    expected = frozenset(
        {
            "schema",
            "schema_version",
            "expected_instance_id",
            "expected_generation",
            "expected_admission_revision",
            "request_id",
            "drain_id",
        }
    )
    document = _closed_mapping(value, expected)
    if document["schema"] != LIFECYCLE_CANCEL_REQUEST_SCHEMA:
        raise LifecycleError("request_invalid", HTTPStatus.BAD_REQUEST, operation="cancel")
    _require_schema_version(document["schema_version"], operation="cancel")
    _require_safe_id(document["expected_instance_id"], operation="cancel")
    _require_positive_safe_integer(document["expected_generation"], operation="cancel")
    _require_positive_safe_integer(document["expected_admission_revision"], operation="cancel")
    _require_safe_id(document["request_id"], operation="cancel")
    _require_safe_id(document["drain_id"], operation="cancel")
    return document


def validate_deployed_no_claim_capability(value: Mapping[str, Any]) -> None:
    """Require exact release/deployment evidence for flush-without-claim."""

    # Reuse the daemon's closed current `/version` validator so a declared
    # capability is trusted only when the deployed release receipt binds the
    # same release, source, artifact, manifest, and current schema target.
    from spl.daemon.connected_ide import project_remote_run_server_capabilities

    projected = project_remote_run_server_capabilities(value)
    release = projected.get("release")
    if not isinstance(release, Mapping) or release.get("state") != "verified":
        raise LifecycleError(
            "server_no_claim_capability_unproven",
            HTTPStatus.PRECONDITION_FAILED,
            operation="sync_flush",
        )
    declared = value.get("declared")
    if not isinstance(declared, Mapping):
        raise LifecycleError(
            "server_no_claim_capability_unproven",
            HTTPStatus.PRECONDITION_FAILED,
            operation="sync_flush",
        )
    contracts = declared.get("contracts")
    if not isinstance(contracts, Mapping):
        raise LifecycleError(
            "server_no_claim_capability_unproven",
            HTTPStatus.PRECONDITION_FAILED,
            operation="sync_flush",
        )
    capabilities = contracts.get("daemon_server_capabilities")
    if not isinstance(capabilities, list) or SYNC_FLUSH_WITHOUT_CLAIM_CAPABILITY not in capabilities:
        raise LifecycleError(
            "server_no_claim_capability_unproven",
            HTTPStatus.PRECONDITION_FAILED,
            operation="sync_flush",
        )


def _closed_mapping(value: Mapping[str, Any], expected: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise LifecycleError("request_invalid", HTTPStatus.BAD_REQUEST, operation="request")
    return dict(value)


def _require_schema_version(value: Any, *, operation: str) -> None:
    if isinstance(value, bool) or value != LIFECYCLE_SCHEMA_VERSION:
        raise LifecycleError("request_invalid", HTTPStatus.BAD_REQUEST, operation=operation)


def _require_positive_safe_integer(value: Any, *, operation: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_SAFE_INTEGER:
        raise LifecycleError("request_invalid", HTTPStatus.BAD_REQUEST, operation=operation)
    return value


def _require_safe_id(value: Any, *, operation: str) -> str:
    if not isinstance(value, str) or not _SAFE_ID.fullmatch(value):
        raise LifecycleError("request_invalid", HTTPStatus.BAD_REQUEST, operation=operation)
    return value


def _fingerprint(value: Mapping[str, Any]) -> str:
    ordered = "\x1f".join(f"{key}={value[key]!r}" for key in sorted(value))
    return hashlib.sha256(ordered.encode("utf-8")).hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _utc_after_milliseconds(milliseconds: int) -> str:
    return (
        (datetime.now(UTC) + timedelta(milliseconds=milliseconds))
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


__all__ = [
    "BLOCKER_KEYS",
    "DESCENDANT_WORK_KINDS",
    "LIFECYCLE_CANCEL_REQUEST_SCHEMA",
    "LIFECYCLE_CAPABILITY",
    "LIFECYCLE_CAPABILITY_VERSION",
    "LIFECYCLE_DRAIN_REQUEST_SCHEMA",
    "LIFECYCLE_SCHEMA_VERSION",
    "LIFECYCLE_STATUS_SCHEMA",
    "LifecycleAdmissionClosed",
    "LifecycleController",
    "LifecycleError",
    "LifecycleWorkLease",
    "ROOT_WORK_KINDS",
    "parse_cancel_request",
    "parse_drain_request",
]
