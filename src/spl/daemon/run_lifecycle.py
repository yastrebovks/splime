"""Authoritative run lifecycle shared with the central server.

``local`` and ``remote`` transitions are monotonic worker/API progress.
``lease_expired`` is the only legal backward edge and is reserved for
central-server recovery.
Resume never transitions a terminal row: it creates a new ``queued`` run with
``parent_run_id`` pointing at the retained terminal run.
"""

from __future__ import annotations

CANONICAL_RUN_STATUSES = frozenset(
    {
        "queued",
        "starting",
        "assigned",
        "fetching_object",
        "preparing",
        "preparing_environment",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "stale",
    }
)
LOCAL_RUN_STATUSES = frozenset({"queued", "starting", "preparing_environment", "running", "succeeded", "failed"})
REMOTE_RUN_STATUSES = frozenset(
    {"queued", "assigned", "fetching_object", "preparing", "running", "succeeded", "failed", "cancelled", "stale"}
)
TERMINAL_RUN_STATUSES = frozenset({"succeeded", "failed", "cancelled", "stale"})
ACTIVE_REMOTE_RUN_STATUSES = REMOTE_RUN_STATUSES - TERMINAL_RUN_STATUSES - {"queued"}

# This exact table is pinned by both repositories' contract tests.
RUN_TRANSITIONS = {
    "local": {
        "queued": frozenset({"starting", "failed"}),
        "starting": frozenset({"preparing_environment", "running", "failed"}),
        "preparing_environment": frozenset({"running", "failed"}),
        "running": frozenset({"succeeded", "failed"}),
        "succeeded": frozenset(),
        "failed": frozenset(),
    },
    "remote": {
        "queued": frozenset({"assigned", "cancelled"}),
        "assigned": frozenset({"fetching_object", "preparing", "running", "succeeded", "failed", "cancelled", "stale"}),
        "fetching_object": frozenset({"preparing", "running", "failed", "cancelled", "stale"}),
        "preparing": frozenset({"running", "failed", "cancelled", "stale"}),
        "running": frozenset({"succeeded", "failed", "cancelled", "stale"}),
        "succeeded": frozenset(),
        "failed": frozenset(),
        "cancelled": frozenset(),
        "stale": frozenset(),
    },
    "lease_expired": {
        "assigned": frozenset({"queued", "failed"}),
        "fetching_object": frozenset({"queued", "failed"}),
        "preparing": frozenset({"queued", "failed"}),
        "running": frozenset({"queued", "failed"}),
    },
}


def allowed_predecessors(target: str, *, mode: str) -> frozenset[str]:
    """Return statuses allowed to transition to ``target`` in ``mode``."""

    transitions = RUN_TRANSITIONS.get(mode)
    if transitions is None:
        raise ValueError(f"unknown run transition mode: {mode}")
    return frozenset(source for source, targets in transitions.items() if target in targets)
