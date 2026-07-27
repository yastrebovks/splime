"""RunRepository aggregate storage."""

from __future__ import annotations

import logging
import shutil
import sqlite3
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from spl._timeout import TimeoutDomain, validate_timeout_seconds
from spl.core import manifest as m_manifest
from spl.core.manifest import (
    ACTIVE_RUN_STATUSES,
    DEFAULT_ON_FAILURE_TTL_SECONDS,
    KeepPolicy,
    build_initial_manifest,
    keep_from_storage,
    keep_to_storage,
    normalize_keep,
    retention_record,
)
from spl.daemon.runtime_config import normalize_runtime_config
from spl.daemon.run_lifecycle import (
    LOCAL_RUN_STATUSES,
    TERMINAL_RUN_STATUSES,
    allowed_predecessors,
)
from spl.daemon.storage_base import (
    DEFAULT_RUN_DELIVERY_LEASE_SECONDS,
    RepositoryBase,
    iso_after_now,
    json_dumps,
    json_loads,
    json_value_loads,
    split_object_function_ref,
    utc_now,
    validate_name,
    write_json,
)
from spl.daemon.worker_runtime_marker import WORKER_RUNTIME_MARKER_FILE


LOGGER = logging.getLogger(__name__)


class RunTransitionError(RuntimeError):
    """Raised when a stale local writer attempts an illegal transition."""


class RunRepository(RepositoryBase):
    """Persist and query run aggregate records."""

    def create_run(
        self,
        object_name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        object_version_id: str | None = None,
        function: str | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        runtimes: dict[str, str] | None = None,
        keep: KeepPolicy = True,
        parent_run_id: str | None = None,
        resume: dict[str, Any] | None = None,
        report_local_run: bool = True,
    ) -> dict[str, Any]:
        """Create a run for an exact object version and persist initial state."""

        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )
        keep_policy = normalize_keep(keep)
        if parent_run_id is not None:
            parent_run_id = validate_name(parent_run_id)
        object_name, function = split_object_function_ref(object_name, function)
        if object_version_id is not None:
            object_record = self.get_object_version(object_version_id, include_yaml=False)
        else:
            object_record = self.get_object(
                object_name,
                version=version,
                include_yaml=False,
                owner_id=owner_id,
                library=library,
            )
        entrypoint = self._run_entrypoint_for(object_record, function)
        runtime_config = normalize_runtime_config(object_record.get("runtime_config"))

        run_id = uuid4().hex
        run_dir = self.runs_dir / run_id
        run_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        try:
            run_dir.chmod(0o700)
        except OSError:
            pass

        input_payload = {
            "args": args or [],
            "kwargs": kwargs or {},
            "output": output,
            "timeout_seconds": timeout_seconds,
            "runtime_config": runtime_config,
            "keep": keep_policy,
            "report_local_run": bool(report_local_run),
        }
        if function is not None:
            input_payload["function"] = function
        if runtimes is not None:
            input_payload["runtimes"] = runtimes
        if resume is not None:
            input_payload["resume"] = resume
        write_json(run_dir / "input.json", input_payload)

        now = utc_now()
        manifest = build_initial_manifest(
            run_id=run_id,
            keep=keep_policy,
            pipeline_name=object_record["name"],
            parent_run_id=parent_run_id,
            created_at=now,
        )
        manifest["pipeline"].update(
            {
                "entrypoint": entrypoint,
                "object_version_id": object_record["version_id"],
                "content_hash": object_record.get("content_hash"),
            }
        )
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO runs(
                    id, object_id, object_version_id, object_name, object_version,
                    entrypoint, env, env_python, status, created_at, run_dir,
                    input_json, result_path, artifacts_dir, env_build_hash,
                    runtime_config_json, keep, manifest_json,
                    retention_enforced, retention_report_mode,
                    retention_sync_required, retention_terminal_queued,
                    retention_delivery_required, retention_delivery_acked,
                    retention_delivery_expires_at, retention_effective_status,
                    retention_outcome_reason
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    object_record["id"],
                    object_record["version_id"],
                    object_record["name"],
                    object_record["version"],
                    entrypoint,
                    object_record["env"],
                    object_record["env_python"],
                    "queued",
                    now,
                    str(run_dir),
                    json_dumps(input_payload),
                    str(run_dir / "result.json"),
                    str(run_dir / "artifacts"),
                    object_record.get("environment_spec_hash"),
                    json_dumps(runtime_config),
                    keep_to_storage(keep_policy),
                    json_dumps(manifest),
                    1,
                    "local" if report_local_run else "remote",
                    0,
                    0,
                    int(report_local_run),
                    int(not report_local_run),
                    None,
                    None,
                    None,
                ),
            )

        state = self.get_run(run_id)
        self._write_run_state_file(state)
        return self.get_run(run_id)

    def _run_entrypoint_for(
        self,
        object_record: dict[str, Any],
        function: str | None,
    ) -> str:
        if function is None:
            return cast(str, object_record["entrypoint"])

        function = validate_name(function)
        for item in object_record.get("functions") or []:
            if item.get("kind") == "function" and item.get("name") == function:
                return function
        for item in object_record.get("internal_objects") or []:
            if item.get("kind") == "function" and item.get("name") == function:
                return function

        available = sorted(
            {
                str(item.get("name"))
                for item in [
                    *(object_record.get("functions") or []),
                    *(object_record.get("internal_objects") or []),
                ]
                if item.get("kind") == "function" and item.get("name")
            }
        )
        raise ValueError(
            f"function is not found in object {object_record['name']}: "
            f"{function}; available: {', '.join(available) or '<none>'}"
        )

    def update_run(self, run_id: str, **changes: Any) -> dict[str, Any]:
        """Merge changes into a run row and return the new state."""

        run_id = validate_name(run_id)
        target_status = changes.get("status")
        if target_status is not None and target_status not in LOCAL_RUN_STATUSES:
            raise ValueError(f"unknown local run status: {target_status}")
        column_values: dict[str, Any] = {}
        for key, value in changes.items():
            column, stored_value = self._run_change_to_column(key, value)
            column_values[column] = stored_value

        if column_values:
            with self._lock, self._conn:
                current = self._conn.execute(
                    "SELECT status FROM runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
                if current is None:
                    raise KeyError(f"run is not found: {run_id}")
                current_status = str(current["status"])
                if target_status == current_status and current_status in TERMINAL_RUN_STATUSES:
                    return self.get_run(run_id)

                assignments = ", ".join(f"{column} = ?" for column in column_values)
                values = list(column_values.values())
                if target_status is None or target_status == current_status:
                    cursor = self._conn.execute(
                        f"UPDATE runs SET {assignments} WHERE id = ? AND status = ?",
                        (*values, run_id, current_status),
                    )
                else:
                    predecessors = sorted(allowed_predecessors(str(target_status), mode="local") & LOCAL_RUN_STATUSES)
                    if not predecessors:
                        self._raise_transition_error(
                            run_id,
                            current_status=current_status,
                            target_status=str(target_status),
                        )
                    placeholders = ", ".join("?" for _ in predecessors)
                    cursor = self._conn.execute(
                        f"UPDATE runs SET {assignments} WHERE id = ? AND status IN ({placeholders})",
                        (*values, run_id, *predecessors),
                    )
                if cursor.rowcount != 1:
                    observed = self._conn.execute(
                        "SELECT status FROM runs WHERE id = ?",
                        (run_id,),
                    ).fetchone()
                    if observed is None:
                        raise KeyError(f"run is not found: {run_id}")
                    self._raise_transition_error(
                        run_id,
                        current_status=str(observed["status"]),
                        target_status=str(target_status or current_status),
                    )

        state = self.get_run(run_id)
        if "status" in changes and state.get("manifest") is not None:
            manifest = self._manifest_for_state(state)
            with self._lock, self._conn:
                self._conn.execute(
                    "UPDATE runs SET manifest_json = ? WHERE id = ?",
                    (json_dumps(manifest), run_id),
                )
            state = self.get_run(run_id)
        self._write_run_state_file(state)
        return self.get_run(run_id)

    def _raise_transition_error(
        self,
        run_id: str,
        *,
        current_status: str,
        target_status: str,
    ) -> None:
        LOGGER.warning(
            "stale local run transition ignored",
            extra={
                "run_id": run_id,
                "from_status": current_status,
                "to_status": target_status,
            },
        )
        raise RunTransitionError(
            f"stale local run update ignored: {run_id} cannot transition from {current_status} to {target_status}"
        )

    def get_run(self, run_id: str) -> dict[str, Any]:
        """Read a run state by id."""

        run_id = validate_name(run_id)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"run is not found: {run_id}")
        return self._run_row_to_state(row)

    def list_runs(self) -> list[dict[str, Any]]:
        """Return all known runs, newest first by creation time."""

        with self._lock:
            rows = self._conn.execute("SELECT * FROM runs ORDER BY created_at DESC").fetchall()
        return [self._run_row_to_state(row) for row in rows]

    def show_run(self, run_id: str, *, include_inline_values: bool = False) -> dict[str, Any]:
        """Return one run with a show-safe manifest payload."""

        state = self.get_run(run_id)
        return m_manifest.sanitize_run_state(state, include_values=include_inline_values)

    def tag_stats(self) -> dict[str, Any]:
        """Aggregate edge artifact tag counts from retained run manifests."""

        manifests: list[dict[str, Any]] = []
        known_dirs: set[Path] = set()
        for state in self.list_runs():
            run_dir = _state_run_dir(state)
            if run_dir is not None:
                known_dirs.add(run_dir.resolve())
            manifest = state.get("manifest")
            if isinstance(manifest, dict):
                manifests.append(manifest)
        manifests.extend(self._orphan_run_manifests(known_dirs))
        return m_manifest.tag_stats_from_manifests(manifests)

    def delete_run(self, run_id: str, *, dry_run: bool = False) -> dict[str, Any]:
        """Delete one inactive run row and directory."""

        result = self.prune_runs(run_id=run_id, dry_run=dry_run)
        if result["count"] == 0:
            if result["skipped_active"]:
                raise RuntimeError("run is active and cannot be pruned: {}".format(run_id))
            raise KeyError("run is not found: {}".format(run_id))
        return result

    def mark_retention_terminal_queued(
        self,
        run_id: str,
        *,
        sync_required: bool,
        effective_status: str | None = None,
        outcome_reason: str | None = None,
    ) -> dict[str, Any]:
        """Persist proof that required terminal reporting is durable.

        ``retention_effective_status`` records only an outcome that differs
        from the run's terminal status.  An omitted outcome on an idempotent
        retry preserves any previously recorded override and reason.
        """

        run_id = validate_name(run_id)
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT status, keep, retention_enforced,
                       retention_effective_status, retention_outcome_reason
                FROM runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"run is not found: {run_id}")
            status = str(row["status"])
            if status not in TERMINAL_RUN_STATUSES:
                raise RuntimeError(
                    "run retention cannot be marked ready before a terminal status: {} is {}".format(run_id, status)
                )
            stored_effective_status = row["retention_effective_status"]
            persisted_effective_status: str | None
            if effective_status is None and stored_effective_status is not None:
                decision_status = str(stored_effective_status)
                persisted_effective_status = decision_status
            else:
                decision_status = status if effective_status is None else effective_status
                persisted_effective_status = decision_status if decision_status != status else None
            if decision_status not in TERMINAL_RUN_STATUSES:
                raise ValueError(f"unknown effective retention status: {decision_status}")
            persisted_outcome_reason = (
                row["retention_outcome_reason"]
                if outcome_reason is None and effective_status is None
                else outcome_reason
            )
            if not bool(row["retention_enforced"]):
                return self.get_run(run_id)
            delivery_deadline = (
                iso_after_now(DEFAULT_RUN_DELIVERY_LEASE_SECONDS)
                if m_manifest.retention_disposition(
                    keep_from_storage(str(row["keep"])),
                    decision_status,
                )
                == "remove"
                else None
            )
            self._conn.execute(
                """
                UPDATE runs
                SET retention_sync_required = ?, retention_terminal_queued = 1,
                    retention_delivery_required = CASE
                        WHEN ? IS NULL THEN 0 ELSE retention_delivery_required
                    END,
                    retention_delivery_acked = CASE
                        WHEN ? IS NULL THEN 1 ELSE retention_delivery_acked
                    END,
                    retention_delivery_expires_at = CASE
                        WHEN retention_delivery_required = 1
                             AND retention_delivery_acked = 0
                             AND ? IS NOT NULL
                        THEN COALESCE(retention_delivery_expires_at, ?)
                        ELSE retention_delivery_expires_at
                    END,
                    retention_effective_status = ?, retention_outcome_reason = ?
                WHERE id = ?
                """,
                (
                    int(sync_required),
                    delivery_deadline,
                    delivery_deadline,
                    delivery_deadline,
                    delivery_deadline,
                    persisted_effective_status,
                    persisted_outcome_reason,
                    run_id,
                ),
            )
            state = self.get_run(run_id)
            manifest = self._manifest_for_state(state)
            self._conn.execute(
                "UPDATE runs SET manifest_json = ? WHERE id = ?",
                (json_dumps(manifest), run_id),
            )
            state = self.get_run(run_id)
        self._write_run_state_file(state)
        return state

    def renew_run_delivery(
        self,
        run_id: str,
        *,
        lease_seconds: float = DEFAULT_RUN_DELIVERY_LEASE_SECONDS,
    ) -> dict[str, Any]:
        """Renew a terminal local run's bounded result/artifact delivery lease."""

        run_id = validate_name(run_id)
        if lease_seconds <= 0:
            raise ValueError("run delivery lease must be positive")
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT status, keep, retention_enforced, retention_delivery_required,
                       retention_delivery_acked, retention_effective_status
                FROM runs WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"run is not found: {run_id}")
            stored_effective_status = row["retention_effective_status"]
            decision_status = str(row["status"] if stored_effective_status is None else stored_effective_status)
            if (
                str(row["status"]) in TERMINAL_RUN_STATUSES
                and bool(row["retention_enforced"])
                and bool(row["retention_delivery_required"])
                and not bool(row["retention_delivery_acked"])
                and m_manifest.retention_disposition(
                    keep_from_storage(str(row["keep"])),
                    decision_status,
                )
                == "remove"
            ):
                self._conn.execute(
                    "UPDATE runs SET retention_delivery_expires_at = ? WHERE id = ?",
                    (iso_after_now(lease_seconds), run_id),
                )
        state = self.get_run(run_id)
        self._write_run_state_file(state)
        return state

    def acknowledge_run_delivery(self, run_id: str) -> dict[str, Any]:
        """Durably acknowledge that a client consumed terminal run data."""

        run_id = validate_name(run_id)
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT status, retention_enforced FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"run is not found: {run_id}")
            if str(row["status"]) not in TERMINAL_RUN_STATUSES:
                raise RuntimeError(
                    f"run delivery cannot be acknowledged before a terminal status: {run_id} is {row['status']}"
                )
            if bool(row["retention_enforced"]):
                self._conn.execute(
                    "UPDATE runs SET retention_delivery_acked = 1 WHERE id = ?",
                    (run_id,),
                )
        state = self.get_run(run_id)
        self._write_run_state_file(state)
        return state

    def set_retention_sync_required(self, run_id: str, *, required: bool) -> dict[str, Any]:
        """Persist whether terminal sync is required before cleanup."""

        run_id = validate_name(run_id)
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE runs
                SET retention_sync_required = ?, retention_terminal_queued = 0
                WHERE id = ? AND retention_enforced = 1
                """,
                (int(required), run_id),
            )
        if cursor.rowcount == 0:
            return self.get_run(run_id)
        state = self.get_run(run_id)
        self._write_run_state_file(state)
        return state

    def enforce_run_retention(
        self,
        run_id: str,
        *,
        blocked_by_sync: bool,
    ) -> dict[str, Any]:
        """Compact and remove one eligible terminal run directory when safe."""

        with self._lock:
            return self._enforce_run_retention_locked(run_id, blocked_by_sync=blocked_by_sync)

    def _enforce_run_retention_locked(
        self,
        run_id: str,
        *,
        blocked_by_sync: bool,
    ) -> dict[str, Any]:
        """Implement retention while the shared store lock is held."""

        state = self.get_run(validate_name(run_id))
        stored_effective_status = state.get("retention_effective_status")
        effective_status = str(state["status"] if stored_effective_status is None else stored_effective_status)
        disposition = m_manifest.retention_disposition(state["keep"], effective_status)
        if not state["retention_enforced"]:
            return {"id": state["id"], "removed": False, "reason": "legacy-grandfathered"}
        if disposition == "active":
            return {"id": state["id"], "removed": False, "reason": "nonterminal"}
        if disposition == "retain":
            return {"id": state["id"], "removed": False, "reason": "policy-retain"}
        manifest = state.get("manifest")
        retention = manifest.get("retention") if isinstance(manifest, dict) else None
        if isinstance(retention, dict) and retention.get("directory_removed") is True:
            return {"id": state["id"], "removed": False, "reason": "already-removed"}
        if not state["retention_terminal_queued"]:
            return {"id": state["id"], "removed": False, "reason": "terminal-sync-not-durable"}
        if blocked_by_sync:
            return {"id": state["id"], "removed": False, "reason": "unsent-sync"}
        if state["retention_delivery_required"] and not state["retention_delivery_acked"]:
            raw_deadline = state.get("retention_delivery_expires_at")
            try:
                deadline = datetime.fromisoformat(str(raw_deadline)) if raw_deadline else None
            except ValueError:
                deadline = None
            if deadline is not None and deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=UTC)
            if deadline is None or datetime.now(UTC) < deadline:
                return {
                    "id": state["id"],
                    "removed": False,
                    "reason": "consumer-delivery-pending",
                    "retry_at": raw_deadline,
                }

        run_dir = self._validated_run_directory(state["id"], state.get("run_dir"))
        compact_manifest = self._compact_manifest(state)
        if run_dir is not None and run_dir.exists():
            shutil.rmtree(run_dir)
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE runs
                SET run_dir = '', input_json = '{}', result_path = '', result_json = NULL,
                    artifacts_dir = '', env_build_hash = NULL, runtime_config_json = '{}',
                    runtime_build_hash = NULL, resolved_runtime = NULL, runtime_backend = NULL,
                    image_tag = NULL, container_id = NULL, resolved_python = NULL,
                    interpreter_substitution_json = NULL, returncode = NULL, command_json = NULL,
                    stdout_path = NULL, stderr_path = NULL, stdout_text = NULL, stderr_text = NULL,
                    manifest_json = ?
                WHERE id = ?
                """,
                (json_dumps(compact_manifest), state["id"]),
            )
        return {"id": state["id"], "removed": True, "reason": "policy-remove"}

    def _compact_manifest(self, state: dict[str, Any]) -> dict[str, Any]:
        raw_manifest = state.get("manifest")
        manifest: dict[str, Any] = raw_manifest if isinstance(raw_manifest, dict) else {}
        raw_nodes = manifest.get("nodes")
        nodes: dict[str, Any] = raw_nodes if isinstance(raw_nodes, dict) else {}
        node_statuses = Counter(
            str(record.get("status") or "unknown") for record in nodes.values() if isinstance(record, dict)
        )
        raw_edges = manifest.get("edges")
        edges: list[Any] = raw_edges if isinstance(raw_edges, list) else []
        raw_pipeline = manifest.get("pipeline")
        pipeline: dict[str, Any] = raw_pipeline if isinstance(raw_pipeline, dict) else {}
        raw_retention = manifest.get("retention")
        retention: dict[str, Any] = raw_retention if isinstance(raw_retention, dict) else {}
        return {
            "schema_version": manifest.get("schema_version", m_manifest.RUN_MANIFEST_SCHEMA_VERSION),
            "run_id": state["id"],
            "parent_run_id": manifest.get("parent_run_id"),
            "status": state.get("status"),
            "keep": state.get("keep"),
            "created_at": state.get("created_at"),
            "started_at": state.get("started_at"),
            "finished_at": state.get("finished_at"),
            "pipeline": {
                "name": pipeline.get("name") or state.get("object"),
                "entrypoint": pipeline.get("entrypoint") or state.get("entrypoint"),
                "object_version_id": pipeline.get("object_version_id") or state.get("object_version_id"),
                "content_hash": pipeline.get("content_hash"),
            },
            "retention": {**retention, "directory_removed": True},
            "summary": {
                "node_count": len(nodes),
                "node_status_counts": dict(sorted(node_statuses.items())),
                "edge_count": len(edges),
            },
        }

    def _validated_run_directory(self, run_id: str, raw_path: Any) -> Path | None:
        if not raw_path:
            return None
        for trusted_root in (self.storage.home, self.runs_dir):
            for component in (*reversed(trusted_root.parents), trusted_root):
                if component.is_symlink():
                    raise RuntimeError(
                        "refusing to remove a run through a symbolic-link daemon root: {}".format(component)
                    )
        run_dir = Path(str(raw_path)).absolute()
        expected = (self.runs_dir / validate_name(run_id)).absolute()
        if run_dir != expected:
            raise RuntimeError("refusing to remove run directory outside the daemon runs directory: {}".format(run_dir))
        if run_dir.is_symlink():
            raise RuntimeError("refusing to remove a run directory symbolic link: {}".format(run_dir))
        return run_dir

    def prune_runs(
        self,
        *,
        run_id: str | None = None,
        statuses: list[str] | tuple[str, ...] | set[str] | None = None,
        older_than_seconds: float | None = None,
        dry_run: bool = False,
        now: datetime | None = None,
        protected_run_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        """Prune inactive run rows and retained run directories."""

        if run_id is not None:
            run_id = validate_name(run_id)
        status_filter = {str(status) for status in statuses or []}
        checked_at = now or datetime.now(UTC)
        candidates: list[dict[str, Any]] = []
        skipped_active: list[dict[str, Any]] = []
        skipped_pending_sync: list[dict[str, Any]] = []
        protected = protected_run_ids or set()
        known_dirs: set[Path] = set()
        for state in self.list_runs():
            run_dir = _state_run_dir(state)
            if run_dir is not None:
                known_dirs.add(run_dir.resolve())
            if run_id is not None and state["id"] != run_id:
                continue
            summary = self._prune_summary(state, source="daemon-row")
            if str(state.get("status") or "") in ACTIVE_RUN_STATUSES:
                skipped_active.append(summary)
                continue
            if str(state["id"]) in protected:
                skipped_pending_sync.append(summary)
                continue
            if self._matches_prune(summary, status_filter, older_than_seconds, checked_at, explicit=run_id is not None):
                candidates.append(summary)

        for summary in self._orphan_run_summaries(known_dirs):
            if run_id is not None and summary["id"] != run_id:
                continue
            if str(summary.get("status") or "") in ACTIVE_RUN_STATUSES:
                skipped_active.append(summary)
                continue
            if self._matches_prune(summary, status_filter, older_than_seconds, checked_at, explicit=run_id is not None):
                candidates.append(summary)

        if not dry_run:
            for item in candidates:
                row_id = item.get("id") if item.get("source") == "daemon-row" else None
                run_dir = item.get("run_dir")
                safe_run_dir = self._validated_run_directory(str(item["id"]), run_dir)
                if safe_run_dir is not None and safe_run_dir.exists():
                    shutil.rmtree(safe_run_dir)
                if isinstance(row_id, str):
                    with self._lock, self._conn:
                        self._conn.execute("DELETE FROM runs WHERE id = ?", (row_id,))

        return {
            "dry_run": dry_run,
            "count": len(candidates),
            "pruned": candidates,
            "skipped_active": skipped_active,
            "skipped_pending_sync": skipped_pending_sync,
            "candidates": candidates if dry_run else [],
        }

    def _run_row_to_state(self, row: sqlite3.Row) -> dict[str, Any]:
        raw_command_json = row["command_json"]
        command, command_readable = json_value_loads(raw_command_json, None)
        raw_input_json = row["input_json"]
        input_payload, input_readable = json_value_loads(raw_input_json, {})
        raw_runtime_config_json = row["runtime_config_json"]
        runtime_config, runtime_config_readable = json_value_loads(
            raw_runtime_config_json,
            {"mode": "venv"},
        )
        raw_interpreter_substitution_json = row["interpreter_substitution_json"]
        interpreter_substitution, interpreter_substitution_readable = json_value_loads(
            raw_interpreter_substitution_json,
            None,
        )
        raw_manifest_json = row["manifest_json"]
        manifest, manifest_readable = json_value_loads(raw_manifest_json, None)
        raw_result_json = row["result_json"]
        result, result_readable = json_value_loads(raw_result_json, None)
        result_present = raw_result_json is not None
        state = {
            "id": row["id"],
            "object": row["object_name"],
            "object_id": row["object_id"],
            "object_version_id": row["object_version_id"],
            "object_version": row["object_version"],
            "entrypoint": row["entrypoint"],
            "env": row["env"],
            "env_python": row["env_python"],
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "run_dir": row["run_dir"],
            "input": input_payload,
            "result_path": row["result_path"],
            "result": result,
            "result_present": result_present,
            "artifacts_dir": row["artifacts_dir"],
            "env_build_hash": row["env_build_hash"],
            "runtime_config": normalize_runtime_config(runtime_config),
            "runtime_build_hash": row["runtime_build_hash"],
            "resolved_runtime": row["resolved_runtime"],
            "runtime_backend": row["runtime_backend"],
            "image_tag": row["image_tag"],
            "container_id": row["container_id"],
            "resolved_python": row["resolved_python"],
            "interpreter_substitution": interpreter_substitution,
            "error": row["error"],
            "returncode": row["returncode"],
            "command": command,
            "stdout_path": row["stdout_path"],
            "stderr_path": row["stderr_path"],
            "stdout": row["stdout_text"],
            "stderr": row["stderr_text"],
            "keep": keep_from_storage(row["keep"]),
            "manifest": manifest,
            "retention_enforced": bool(row["retention_enforced"]),
            "retention_report_mode": row["retention_report_mode"],
            "retention_sync_required": bool(row["retention_sync_required"]),
            "retention_terminal_queued": bool(row["retention_terminal_queued"]),
            "retention_delivery_required": bool(row["retention_delivery_required"]),
            "retention_delivery_acked": bool(row["retention_delivery_acked"]),
            "retention_delivery_expires_at": row["retention_delivery_expires_at"],
            "retention_effective_status": row["retention_effective_status"],
            "retention_outcome_reason": row["retention_outcome_reason"],
        }
        if result_present and not result_readable:
            state["result_json"] = raw_result_json
            state["result_unreadable"] = True
        for field, raw_value, readable in (
            ("command", raw_command_json, command_readable),
            ("input", raw_input_json, input_readable),
            ("runtime_config", raw_runtime_config_json, runtime_config_readable),
            (
                "interpreter_substitution",
                raw_interpreter_substitution_json,
                interpreter_substitution_readable,
            ),
            ("manifest", raw_manifest_json, manifest_readable),
        ):
            if raw_value is not None and not readable:
                state[f"{field}_json"] = raw_value
                state[f"{field}_unreadable"] = True
        state.update(self._run_list_fields(state))
        state.update(self._worker_runtime_marker(state))
        return state

    def _run_change_to_column(self, key: str, value: Any) -> tuple[str, Any]:
        aliases = {
            "command": "command_json",
            "input": "input_json",
            "result": "result_json",
            "runtime_config": "runtime_config_json",
            "interpreter_substitution": "interpreter_substitution_json",
            "manifest": "manifest_json",
        }
        json_columns = {
            "command_json",
            "input_json",
            "interpreter_substitution_json",
            "manifest_json",
            "result_json",
            "runtime_config_json",
        }
        valid_columns = {
            "status",
            "started_at",
            "finished_at",
            "run_dir",
            "result_path",
            "artifacts_dir",
            "env_build_hash",
            "runtime_config_json",
            "runtime_build_hash",
            "resolved_runtime",
            "runtime_backend",
            "image_tag",
            "container_id",
            "resolved_python",
            "interpreter_substitution_json",
            "error",
            "returncode",
            "command_json",
            "input_json",
            "result_json",
            "stdout_path",
            "stderr_path",
            "stdout_text",
            "stderr_text",
            "keep",
            "manifest_json",
        }
        column = aliases.get(key, key)
        if column not in valid_columns:
            raise ValueError(f"unknown run state field: {key}")
        if column == "keep":
            return column, keep_to_storage(normalize_keep(value))
        if key in aliases:
            return column, json_dumps(value)
        if column in json_columns and not isinstance(value, str):
            return column, json_dumps(value)
        return column, value

    def _write_run_state_file(self, state: dict[str, Any]) -> None:
        """Write a diagnostic state snapshot next to worker files."""

        run_dir = state.get("run_dir")
        if run_dir:
            write_json(Path(run_dir) / "state.json", state)

    def _manifest_for_state(self, state: dict[str, Any]) -> dict[str, Any]:
        manifest = dict(state["manifest"])
        status = str(state.get("status"))
        manifest["status"] = status
        if state.get("finished_at") is not None:
            manifest["finished_at"] = state["finished_at"]
        stored_effective_status = state.get("retention_effective_status")
        effective_status = str(status if stored_effective_status is None else stored_effective_status)
        next_retention = retention_record(state["keep"], effective_status)
        previous_retention = manifest.get("retention")
        if (
            isinstance(previous_retention, dict)
            and next_retention.get("expires_at") is not None
            and previous_retention.get("expires_at") is not None
        ):
            next_retention["expires_at"] = previous_retention["expires_at"]
        if state.get("retention_effective_status") is not None:
            next_retention["effective_status"] = effective_status
        if state.get("retention_outcome_reason") is not None:
            next_retention["outcome_reason"] = state["retention_outcome_reason"]
        manifest["retention"] = next_retention
        if state.get("error") is not None:
            manifest["error"] = state["error"]
        return manifest

    def _run_list_fields(self, state: dict[str, Any]) -> dict[str, Any]:
        run_dir = _state_run_dir(state)
        manifest = state.get("manifest")
        manifest_summary: dict[str, Any] = {}
        if isinstance(manifest, dict):
            manifest_summary = m_manifest.manifest_summary(manifest, run_dir=run_dir)
        return {
            "mode": "local",
            "output": (state.get("input") or {}).get("output") if isinstance(state.get("input"), dict) else None,
            "has_manifest": isinstance(manifest, dict),
            "parent_run_id": manifest_summary.get("parent_run_id"),
            "retention": manifest_summary.get("retention"),
            "expires_at": manifest_summary.get("expires_at"),
            "disk_size_bytes": None if run_dir is None else m_manifest.run_dir_size(run_dir),
            "node_runtimes": manifest_summary.get("node_runtimes", []),
            "edge_adapters": manifest_summary.get("edge_adapters", []),
        }

    def _prune_summary(self, state: dict[str, Any], *, source: str) -> dict[str, Any]:
        return {
            "id": state["id"],
            "source": source,
            "status": state.get("status"),
            "keep": state.get("keep"),
            "has_manifest": state.get("has_manifest"),
            "parent_run_id": state.get("parent_run_id"),
            "created_at": state.get("created_at"),
            "finished_at": state.get("finished_at"),
            "expires_at": state.get("expires_at"),
            "run_dir": state.get("run_dir"),
            "disk_size_bytes": state.get("disk_size_bytes"),
        }

    def _orphan_run_summaries(self, known_dirs: set[Path]) -> list[dict[str, Any]]:
        if not self.runs_dir.exists():
            return []
        summaries = []
        for run_dir in sorted((item for item in self.runs_dir.iterdir() if item.is_dir()), key=lambda item: item.name):
            if run_dir.resolve() in known_dirs:
                continue
            manifest_path = run_dir / m_manifest.RUN_MANIFEST_FILENAME
            if manifest_path.exists():
                try:
                    summary = m_manifest.manifest_summary(m_manifest.read_manifest(manifest_path), run_dir=run_dir)
                except (OSError, ValueError, TypeError):
                    summary = self._legacy_orphan_summary(run_dir)
            else:
                summary = self._legacy_orphan_summary(run_dir)
            summary["source"] = "orphan-dir"
            summaries.append(summary)
        return summaries

    def _orphan_run_manifests(self, known_dirs: set[Path]) -> list[dict[str, Any]]:
        if not self.runs_dir.exists():
            return []
        manifests = []
        for run_dir in sorted((item for item in self.runs_dir.iterdir() if item.is_dir()), key=lambda item: item.name):
            if run_dir.resolve() in known_dirs:
                continue
            manifest_path = run_dir / m_manifest.RUN_MANIFEST_FILENAME
            if not manifest_path.exists():
                continue
            try:
                manifests.append(m_manifest.read_manifest(manifest_path))
            except (OSError, ValueError, TypeError):
                continue
        return manifests

    def _legacy_orphan_summary(self, run_dir: Path) -> dict[str, Any]:
        try:
            created_at = datetime.fromtimestamp(run_dir.stat().st_mtime, UTC).isoformat()
        except OSError:
            created_at = None
        return {
            "id": run_dir.name,
            "source": "orphan-dir",
            "status": "legacy",
            "keep": None,
            "has_manifest": False,
            "parent_run_id": None,
            "created_at": created_at,
            "finished_at": None,
            "expires_at": None,
            "run_dir": str(run_dir),
            "disk_size_bytes": m_manifest.run_dir_size(run_dir),
        }

    def _matches_prune(
        self,
        summary: dict[str, Any],
        status_filter: set[str],
        older_than_seconds: float | None,
        now: datetime,
        *,
        explicit: bool,
    ) -> bool:
        if explicit:
            return True
        status = str(summary.get("status") or "")
        if status_filter and status not in status_filter:
            return False
        if older_than_seconds is not None:
            timestamp = m_manifest.parse_utc_timestamp(summary.get("finished_at")) or m_manifest.parse_utc_timestamp(
                summary.get("created_at")
            )
            return timestamp is not None and (now - timestamp).total_seconds() >= older_than_seconds
        if status_filter:
            return True
        expires_at = m_manifest.parse_utc_timestamp(summary.get("expires_at"))
        if expires_at is not None:
            return now >= expires_at
        if not bool(summary.get("has_manifest")):
            timestamp = m_manifest.parse_utc_timestamp(summary.get("created_at"))
            return timestamp is not None and (now - timestamp).total_seconds() >= DEFAULT_ON_FAILURE_TTL_SECONDS
        return False

    def _worker_runtime_marker(self, state: dict[str, Any]) -> dict[str, Any]:
        run_dir = state.get("run_dir")
        if not run_dir:
            return {}
        marker_path = Path(str(run_dir)) / WORKER_RUNTIME_MARKER_FILE
        try:
            marker = json_loads(marker_path.read_text(encoding="utf-8"), None)
        except OSError:
            marker = None
        if not isinstance(marker, dict):
            return {}
        result: dict[str, Any] = {}
        if marker.get("worker_runtime") is not None:
            result["worker_runtime"] = marker["worker_runtime"]
        if marker.get("worker_runtime_reason") is not None:
            result["worker_runtime_reason"] = marker["worker_runtime_reason"]
        if marker.get("generated_module") is not None:
            result["generated_module"] = marker["generated_module"]
        if marker.get("generated_module_name") is not None:
            result["generated_module_name"] = marker["generated_module_name"]
        return result


def _state_run_dir(state: dict[str, Any]) -> Path | None:
    value = state.get("run_dir")
    return Path(str(value)) if value else None
