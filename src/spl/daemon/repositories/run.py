"""RunRepository aggregate storage."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any
from uuid import uuid4

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
    ) -> dict[str, Any]:
        """Create a run for an exact object version and persist initial state."""

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
        run_dir.mkdir(parents=True, exist_ok=False)

        input_payload = {
            "args": args or [],
            "kwargs": kwargs or {},
            "output": output,
            "timeout_seconds": timeout_seconds,
        }
        if function is not None:
            input_payload["function"] = function
        write_json(run_dir / "input.json", input_payload)

        now = utc_now()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO runs(
                    id, object_id, object_version_id, object_name, object_version,
                    entrypoint, env, env_python, status, created_at, run_dir,
                    input_json, result_path, artifacts_dir, env_build_hash,
                    runtime_config_json
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )

        state = self.get_run(run_id)
        self._write_run_state_file(state)
        return state

    def _run_entrypoint_for(
        self,
        object_record: dict[str, Any],
        function: str | None,
    ) -> str:
        if function is None:
            return object_record["entrypoint"]

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
        self._write_run_state_file(state)
        return state

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
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY created_at DESC"
            ).fetchall()
        return [self._run_row_to_state(row) for row in rows]

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
            "runtime_config": normalize_runtime_config(
                json_loads(row["runtime_config_json"], {"mode": "venv"})
            ),
            "runtime_build_hash": row["runtime_build_hash"],
            "resolved_runtime": row["resolved_runtime"],
            "runtime_backend": row["runtime_backend"],
            "image_tag": row["image_tag"],
            "container_id": row["container_id"],
            "resolved_python": row["resolved_python"],
            "error": row["error"],
            "returncode": row["returncode"],
            "command": command,
            "stdout_path": row["stdout_path"],
            "stderr_path": row["stderr_path"],
            "stdout": row["stdout_text"],
            "stderr": row["stderr_text"],
        }
        return state

    def _run_change_to_column(self, key: str, value: Any) -> tuple[str, Any]:
        aliases = {
            "command": "command_json",
            "input": "input_json",
            "result": "result_json",
            "runtime_config": "runtime_config_json",
        }
        json_columns = {
            "command_json",
            "input_json",
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
            "error",
            "returncode",
            "command_json",
            "input_json",
            "result_json",
            "stdout_path",
            "stderr_path",
            "stdout_text",
            "stderr_text",
        }
        column = aliases.get(key, key)
        if column not in valid_columns:
            raise ValueError(f"unknown run state field: {key}")
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
