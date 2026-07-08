from __future__ import annotations

import sys

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
    finally:
        storage.close()


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
                    "object_name": "demo_obj",
                    "library": "research",
                }
            )
            == signature
        )
        assert store.current_server_connection()["id"] == connection["id"]
    finally:
        store.close()
