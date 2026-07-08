from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import pytest

from spl.daemon.environment_base import (
    ABSENT,
    CREATING,
    READY,
    BaseEnvironmentManager,
)
from spl.daemon.store import RegistryStore


class HelperEnvironmentManager(BaseEnvironmentManager):
    missing_ready = False

    def build_spec(self, object_record: dict[str, Any]) -> dict[str, Any]:
        spec_hash = object_record.get("spec_hash", "helper")
        env_dir = self.store.environment_builds_dir / spec_hash
        return {
            "spec_hash": spec_hash,
            "base_python": "python",
            "python_version": "Python 3.13",
            "distributions": [],
            "runtime_packages": [],
            "spec": {},
            "venv_path": env_dir / "venv",
            "python_path": env_dir / "venv" / "bin" / "python",
            "install_log_path": env_dir / "install.log",
            "lock_path": env_dir / "build.lock",
            "force_rebuild": False,
        }

    def _build_environment(self, spec: dict[str, Any]) -> None:
        pass

    def _spec_from_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return self.build_spec({"spec_hash": record["spec_hash"]})

    def _is_ready(self, record: dict[str, Any]) -> bool:
        return record["status"] == READY

    def _validate_rebuild_record(
        self,
        record: dict[str, Any] | None,
        spec_hash: str,
    ) -> dict[str, Any]:
        if record is None:
            raise KeyError(f"environment build is not found: {spec_hash}")
        return record

    def _upsert_creating_record(self, spec: dict[str, Any]) -> dict[str, Any]:
        return self.store.upsert_environment_build(
            spec_hash=spec["spec_hash"],
            base_python=spec["base_python"],
            python_version=spec["python_version"],
            distributions=spec["distributions"],
            runtime_packages=spec["runtime_packages"],
            spec=spec["spec"],
            venv_path=spec["venv_path"],
            python_path=spec["python_path"],
            install_log_path=spec["install_log_path"],
            status=CREATING,
        )

    def _build_thread_name(self, spec_hash: str) -> str:
        return f"helper-env-{spec_hash}"

    def _absent_record(self, spec: dict[str, Any]) -> dict[str, Any]:
        return {
            "spec_hash": spec["spec_hash"],
            "status": ABSENT,
            "python_path": str(spec["python_path"]),
            "install_log_path": str(spec["install_log_path"]),
        }

    def _ready_record_is_missing(
        self,
        record: dict[str, Any],
        spec: dict[str, Any],
    ) -> bool:
        return self.missing_ready

    def _missing_ready_error(self) -> str:
        return "helper resource is missing"

    def _build_failed_message(self, record: dict[str, Any]) -> str:
        return f"helper build failed: {record.get('error') or record['spec_hash']}"

    def _rebuild_failed_message(self, record: dict[str, Any]) -> str:
        return f"helper rebuild failed: {record.get('error') or record['spec_hash']}"

    def _build_lock_timeout_message(self, lock_path: Path) -> str:
        return f"timed out waiting for helper build lock: {lock_path}"

    def _command_timeout_message(self, command: list[str]) -> str:
        return f"helper command timed out: {command[0]}"

    def _command_failed_message(self, command: list[str], returncode: int) -> str:
        return f"helper command failed with exit code {returncode}: {command[0]}"

    def _path_refusal_message(self, target: Path) -> str:
        return f"refusing to modify helper environment outside daemon home: {target}"


@pytest.fixture
def store(tmp_path: Path) -> Iterator[RegistryStore]:
    registry = RegistryStore(tmp_path)
    try:
        yield registry
    finally:
        registry.close()


def test_base_constructor_validates_timeouts(store: RegistryStore) -> None:
    with pytest.raises(ValueError, match="build_timeout_seconds must be positive"):
        HelperEnvironmentManager(store, build_timeout_seconds=0)
    with pytest.raises(ValueError, match="stale_lock_seconds must be positive"):
        HelperEnvironmentManager(store, stale_lock_seconds=0)


def test_base_normalizes_distributions_deterministically(
    store: RegistryStore,
) -> None:
    manager = HelperEnvironmentManager(store)

    assert manager._normalize_distributions(
        [
            {"package": "Requests", "version": 2},
            {"package": "PyYAML", "version": "6.0.2"},
            {"package": "requests", "version": "1"},
        ]
    ) == [
        {"package": "pyyaml", "version": "6.0.2"},
        {"package": "requests", "version": "1"},
        {"package": "requests", "version": "2"},
    ]


def test_base_parse_timestamp_normalizes_to_utc(store: RegistryStore) -> None:
    manager = HelperEnvironmentManager(store)

    assert manager._parse_timestamp(None) is None
    assert manager._parse_timestamp("not-a-timestamp") is None
    assert manager._parse_timestamp("2026-01-02T03:04:05") == datetime(
        2026,
        1,
        2,
        3,
        4,
        5,
        tzinfo=UTC,
    )
    assert manager._parse_timestamp("2026-01-02T05:04:05+02:00") == datetime(
        2026,
        1,
        2,
        3,
        4,
        5,
        tzinfo=UTC,
    )


def test_base_stale_creating_uses_started_at_and_lock_file(
    store: RegistryStore,
) -> None:
    manager = HelperEnvironmentManager(store, stale_lock_seconds=60)
    spec = manager.build_spec({"spec_hash": "stale-helper"})

    assert manager._is_stale_creating({"status": CREATING}, spec) is True
    assert (
        manager._is_stale_creating(
            {
                "status": CREATING,
                "started_at": datetime.now(UTC).isoformat(),
            },
            spec,
        )
        is False
    )
    assert (
        manager._is_stale_creating(
            {
                "status": CREATING,
                "started_at": (datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
            },
            spec,
        )
        is True
    )

    lock_path = Path(spec["lock_path"])
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.touch()
    assert manager._is_stale_creating({"status": CREATING}, spec) is False
    old_time = time.time() - 120
    os.utime(lock_path, (old_time, old_time))
    assert manager._is_stale_creating({"status": CREATING}, spec) is True


def test_base_build_lock_creates_and_removes_lock_file(
    store: RegistryStore,
) -> None:
    manager = HelperEnvironmentManager(store)
    spec = manager.build_spec({"spec_hash": "lock-helper"})
    lock_path = Path(spec["lock_path"])

    with manager._build_lock(spec):
        assert lock_path.exists()

    assert not lock_path.exists()


def test_base_record_or_absent_marks_missing_ready_resource_absent(
    store: RegistryStore,
) -> None:
    manager = HelperEnvironmentManager(store)
    spec = manager.build_spec({"spec_hash": "missing-helper"})
    manager._upsert_creating_record(spec)
    store.update_environment_build(
        spec["spec_hash"],
        status=READY,
        finished_at=datetime.now(UTC).isoformat(),
    )
    manager.missing_ready = True

    record = manager._record_or_absent(spec)

    assert record["status"] == ABSENT
    assert record["error"] == "helper resource is missing"
