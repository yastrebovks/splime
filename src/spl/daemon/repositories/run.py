"""RunRepository aggregate storage."""

from __future__ import annotations

import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

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
from spl.daemon.storage_base import (
    RepositoryBase,
    json_dumps,
    json_loads,
    split_object_function_ref,
    utc_now,
    validate_name,
    write_json,
)
from spl.daemon.worker_runtime_marker import WORKER_RUNTIME_MARKER_FILE


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
        runtimes: dict[str, str] | None = None,
        keep: KeepPolicy = "on_failure",
        parent_run_id: str | None = None,
        resume: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a run for an exact object version and persist initial state."""

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
                    runtime_config_json, keep, manifest_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )

        state = self.get_run(run_id)
        self._write_run_state_file(state)
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
        column_values: dict[str, Any] = {}
        for key, value in changes.items():
            column, stored_value = self._run_change_to_column(key, value)
            column_values[column] = stored_value

        if column_values:
            assignments = ", ".join(f"{column} = ?" for column in column_values)
            values = [*column_values.values(), run_id]
            with self._lock, self._conn:
                cursor = self._conn.execute(
                    f"UPDATE runs SET {assignments} WHERE id = ?",
                    values,
                )
            if cursor.rowcount == 0:
                raise KeyError(f"run is not found: {run_id}")

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

    def prune_runs(
        self,
        *,
        run_id: str | None = None,
        statuses: list[str] | tuple[str, ...] | set[str] | None = None,
        older_than_seconds: float | None = None,
        dry_run: bool = False,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Prune inactive run rows and retained run directories."""

        if run_id is not None:
            run_id = validate_name(run_id)
        status_filter = {str(status) for status in statuses or []}
        checked_at = now or datetime.now(UTC)
        candidates: list[dict[str, Any]] = []
        skipped_active: list[dict[str, Any]] = []
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
                if isinstance(row_id, str):
                    with self._lock, self._conn:
                        self._conn.execute("DELETE FROM runs WHERE id = ?", (row_id,))
                run_dir = item.get("run_dir")
                if isinstance(run_dir, str):
                    shutil.rmtree(run_dir, ignore_errors=True)

        return {
            "dry_run": dry_run,
            "count": len(candidates),
            "pruned": candidates,
            "skipped_active": skipped_active,
            "candidates": candidates if dry_run else [],
        }

    def _run_row_to_state(self, row: sqlite3.Row) -> dict[str, Any]:
        command = json_loads(row["command_json"], None)
        result = json_loads(row["result_json"], None)
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
            "input": json_loads(row["input_json"], {}),
            "result_path": row["result_path"],
            "result": result,
            "artifacts_dir": row["artifacts_dir"],
            "env_build_hash": row["env_build_hash"],
            "runtime_config": normalize_runtime_config(json_loads(row["runtime_config_json"], {"mode": "venv"})),
            "runtime_build_hash": row["runtime_build_hash"],
            "resolved_runtime": row["resolved_runtime"],
            "runtime_backend": row["runtime_backend"],
            "image_tag": row["image_tag"],
            "container_id": row["container_id"],
            "resolved_python": row["resolved_python"],
            "interpreter_substitution": json_loads(row["interpreter_substitution_json"], None),
            "error": row["error"],
            "returncode": row["returncode"],
            "command": command,
            "stdout_path": row["stdout_path"],
            "stderr_path": row["stderr_path"],
            "stdout": row["stdout_text"],
            "stderr": row["stderr_text"],
            "keep": keep_from_storage(row["keep"]),
            "manifest": json_loads(row["manifest_json"], None),
        }
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
        manifest["retention"] = retention_record(state["keep"], status)
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
