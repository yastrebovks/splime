from __future__ import annotations

import os
import stat
import sys
import threading
from pathlib import Path

import pytest

from spl.core import manifest as m_manifest
import spl.daemon.repositories.env as env_repository
from spl.daemon.storage_base import StorageBase
from spl.daemon.store import RegistryStore


FUNCTION_YAML = """\
- !DFunction
  name: demo_obj
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 1
"""


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def _tag_stats_edge(save_tag: str, load_tags: list[str]) -> dict[str, object]:
    return {
        "source": {"node_id": "source", "port": "default"},
        "target": {"node_id": "target", "port": "value"},
        "artifact": {"kind": "artifact", "tag": save_tag, "sha256": "0" * 64},
        "adapter": {
            "save": {"tag": save_tag, "accepted_tags": [save_tag], "source": "pipeline"},
            "load": {"tag": load_tags[0], "accepted_tags": load_tags, "source": "pipeline"},
        },
    }


def _seed_demo_run(store: RegistryStore) -> dict[str, object]:
    store.register_env("default", sys.executable)
    store.register_object("demo_obj", "demo_obj", "default", yaml_text=FUNCTION_YAML)
    return store.create_run("demo_obj", keep=True)


def test_storage_base_owns_paths_and_json_helpers(tmp_path) -> None:
    storage = StorageBase(tmp_path)
    try:
        path = storage.home / "nested" / "value.json"

        storage.write_json(path, {"ok": True})

        assert storage.home == tmp_path.absolute()
        assert storage.objects_dir == storage.home / "objects"
        assert storage.read_json(path, {}) == {"ok": True}
        assert storage.read_json(storage.home / "missing.json", {"missing": True}) == {
            "missing": True,
        }
        if os.name == "posix":
            assert _mode(path.parent) == 0o700
            assert _mode(path) == 0o600
    finally:
        storage.close()


def test_storage_base_close_is_idempotent(tmp_path) -> None:
    storage = StorageBase(tmp_path)

    storage.close()
    storage.close()


def test_store_operation_after_close_raises_clean_runtime_error(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.close()

    with pytest.raises(RuntimeError, match="store is closed"):
        store.list_envs()
    store.close()


def test_store_close_serializes_against_run_updates(tmp_path) -> None:
    for index in range(50):
        store = RegistryStore(tmp_path / f"store-{index}")
        run = _seed_demo_run(store)
        first_update = threading.Event()
        stop = threading.Event()
        errors: list[BaseException] = []

        def update_loop() -> None:
            status = "running"
            while not stop.is_set():
                try:
                    store.update_run(str(run["id"]), status=status)
                    first_update.set()
                    status = "queued" if status == "running" else "running"
                except RuntimeError as exc:
                    if str(exc) == "store is closed":
                        return
                    errors.append(exc)
                    return
                except BaseException as exc:  # pragma: no cover - assertion reports the concrete failure.
                    errors.append(exc)
                    return

        thread = threading.Thread(target=update_loop, name=f"store-close-race-{index}")
        thread.start()
        assert first_update.wait(2)
        store.close()
        stop.set()
        thread.join(2)

        assert not thread.is_alive()
        assert errors == []
        store.close()


def test_register_env_defaults_to_daemon_interpreter(tmp_path, monkeypatch) -> None:
    daemon_python = tmp_path / "daemon-python"
    daemon_python.touch()
    client_python = tmp_path / "client-python"
    client_python.touch()
    monkeypatch.setattr(env_repository.sys, "executable", str(daemon_python))

    store = RegistryStore(tmp_path)
    try:
        env = store.register_env("default")

        assert env["python"] == str(daemon_python.absolute())
        assert env["python"] != str(client_python.absolute())
    finally:
        store.close()


def test_repositories_are_directly_usable_behind_facade(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        env = store.envs.register_env("default", sys.executable)
        obj = store.objects.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )
        run = store.runs.create_run("demo_obj", kwargs={"unused": True})
        event = store.sync_events.enqueue_sync_event("object_version", {"id": obj["id"]})
        signature = store.libraries.save_remote_signature(
            {
                "server_url": "https://splime.io/api",
                "owner_id": "owner-1",
                "object_name": "demo_obj",
                "library": "research",
            },
            {"inputs": [], "outputs": []},
        )
        connection = store.server_connections.save_pending_server_connection(
            server_url="https://splime.io/api",
            token="machine-token-secret",
            user_token="user-token-secret",
            machine_id="machine-1",
        )

        assert store.get_env("default") == env
        assert store.get_object_version(obj["version_id"], include_yaml=False) == obj
        assert store.get_run(run["id"]) == run
        assert store.get_sync_event(event["id"]) == event
        assert (
            store.get_remote_signature(
                {
                    "server_url": "https://splime.io/api",
                    "owner_id": "owner-1",
                    "object_name": "demo_obj",
                    "library": "research",
                }
            )
            == signature
        )
        assert store.get_server_connection(connection["id"]) == connection
        assert store.current_server_connection() is None
    finally:
        store.close()


def test_remote_signature_cache_ref_requires_owner_id(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    ownerless_ref = {
        "server_url": "https://splime.io/api",
        "object_name": "demo_obj",
        "library": "research",
    }
    try:
        with pytest.raises(ValueError, match="cache ref must carry owner_id"):
            store.remote_signature_key_for(ownerless_ref)
        with pytest.raises(ValueError, match="cache ref must carry owner_id"):
            store.get_remote_signature(ownerless_ref)
        with pytest.raises(ValueError, match="cache ref must carry owner_id"):
            store.libraries.save_remote_signature(ownerless_ref, {"inputs": [], "outputs": []})
    finally:
        store.close()


def test_run_repository_persists_keep_and_manifest_state(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.envs.register_env("default", sys.executable)
        store.objects.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )

        run = store.runs.create_run("demo_obj", keep=True)

        assert run["keep"] is True
        assert run["manifest"]["keep"] is True
        assert run["manifest"]["pipeline"]["object_version_id"] == run["object_version_id"]
        assert Path(run["run_dir"]).stat().st_mode & 0o777 == 0o700
        if os.name == "posix":
            run_dir = Path(run["run_dir"])
            assert _mode(store.runs_dir) == 0o700
            assert _mode(run_dir) == 0o700
            assert _mode(run_dir / "input.json") == 0o600
            assert _mode(run_dir / "state.json") == 0o600

        updated = store.runs.update_run(
            run["id"],
            status="failed",
            finished_at="2026-07-08T10:00:00+00:00",
            error="boom",
        )

        assert updated["manifest"]["status"] == "failed"
        assert updated["manifest"]["error"] == "boom"
        assert updated["manifest"]["retention"]["class"] == "keep"
    finally:
        store.close()


def test_run_repository_lists_manifest_fields_and_sanitizes_show(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.envs.register_env("default", sys.executable)
        store.objects.register_object("demo_obj", "demo_obj", "default", yaml_text=FUNCTION_YAML)
        run = store.runs.create_run("demo_obj", keep=True)
        manifest = dict(run["manifest"])
        manifest["nodes"] = {
            "node-1": {
                "id": "node-1",
                "alias": "producer",
                "status": "succeeded",
                "outputs": {
                    "default": {
                        "kind": "json",
                        "tag": "json",
                        "value": {"secret": "not-for-default-cli"},
                        "sha256": "0" * 64,
                    }
                },
            }
        }
        manifest["parent_run_id"] = "parent-run"
        store.runs.update_run(run["id"], manifest=manifest)

        listed = store.runs.get_run(run["id"])
        shown = store.runs.show_run(run["id"])
        shown_full = store.runs.show_run(run["id"], include_inline_values=True)

        assert listed["has_manifest"] is True
        assert listed["parent_run_id"] == "parent-run"
        assert listed["disk_size_bytes"] > 0
        output = shown["manifest"]["nodes"]["node-1"]["outputs"]["default"]
        assert output["value_omitted"] is True
        assert "value" not in output
        assert output["value_preview"] == "<omitted>"
        assert output["value_preview_omitted"] is True
        assert "not-for-default-cli" not in str(shown)
        assert shown_full["manifest"]["nodes"]["node-1"]["outputs"]["default"]["value"] == {
            "secret": "not-for-default-cli"
        }
    finally:
        store.close()


def test_run_repository_tag_stats_reads_rows_and_orphan_manifests(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        assert store.runs.tag_stats() == {
            "runs_scanned": 0,
            "edges_scanned": 0,
            "tags": [],
            "pairs": [],
        }

        store.envs.register_env("default", sys.executable)
        store.objects.register_object("demo_obj", "demo_obj", "default", yaml_text=FUNCTION_YAML)
        run = store.runs.create_run("demo_obj", keep=True)
        manifest = dict(run["manifest"])
        manifest["edges"] = [_tag_stats_edge("json", ["json"])]
        store.runs.update_run(run["id"], manifest=manifest)

        orphan_dir = store.runs_dir / "orphan-run"
        m_manifest.atomic_write_json(
            orphan_dir / m_manifest.RUN_MANIFEST_FILENAME,
            {
                "run_id": "orphan-run",
                "edges": [_tag_stats_edge("parquet", ["parquet"])],
            },
        )

        assert store.runs.tag_stats() == {
            "runs_scanned": 2,
            "edges_scanned": 2,
            "tags": [
                {"tag": "json", "edge_count": 1, "run_count": 1},
                {"tag": "parquet", "edge_count": 1, "run_count": 1},
            ],
            "pairs": [
                {"save_tag": "json", "load_tags": ["json"], "edge_count": 1, "run_count": 1},
                {"save_tag": "parquet", "load_tags": ["parquet"], "edge_count": 1, "run_count": 1},
            ],
        }
    finally:
        store.close()


def test_run_repository_prunes_ttl_status_dry_run_active_and_legacy_dirs(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.envs.register_env("default", sys.executable)
        store.objects.register_object("demo_obj", "demo_obj", "default", yaml_text=FUNCTION_YAML)
        expired = store.runs.create_run("demo_obj")
        expired = store.runs.update_run(expired["id"], status="failed")
        expired_manifest = dict(expired["manifest"])
        expired_manifest["retention"] = {"class": "on_failure", "expires_at": "2000-01-01T00:00:00+00:00"}
        expired = store.runs.update_run(expired["id"], manifest=expired_manifest)
        active = store.runs.create_run("demo_obj")
        legacy_dir = store.runs_dir / "legacy-run"
        legacy_dir.mkdir(parents=True)
        (legacy_dir / "state.json").write_text("{}", encoding="utf-8")
        old_time = 946684800
        os.utime(legacy_dir, (old_time, old_time))

        preview = store.runs.prune_runs(dry_run=True)

        assert preview["dry_run"] is True
        assert {item["id"] for item in preview["candidates"]} == {expired["id"], "legacy-run"}
        assert Path(expired["run_dir"]).exists()
        assert legacy_dir.exists()

        active_preview = store.runs.prune_runs(run_id=active["id"], dry_run=True)
        assert active_preview["count"] == 0
        assert active_preview["skipped_active"][0]["id"] == active["id"]

        result = store.runs.prune_runs()

        assert {item["id"] for item in result["pruned"]} == {expired["id"], "legacy-run"}
        assert not Path(expired["run_dir"]).exists()
        assert not legacy_dir.exists()
        assert Path(active["run_dir"]).exists()
        assert store.runs.get_run(active["id"])["status"] == "queued"
    finally:
        store.close()
