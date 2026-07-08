"""Daemon side of the signature contract (stage 1.3).

KEEP IN SYNC with ``spl-server/tests/test_signature_contract.py``: both files
pin the SAME ``EXPECTED_CORE`` for the same reference object. The daemon and
the server duplicate their signature builders; this contract makes any
semantic drift in the shared core (name/kind/version/env provenance/
inputs/outputs) fail CI on the side that drifted. Envelope keys beyond the
core may differ by design (daemon adds display_name/origin, the server adds
owner/library/execution).

The env fields are version provenance, not an execution decision: the daemon
syncs the author's env name, interpreter path and interpreter version, while
runtime execution resolves a local interpreter by env name/default/daemon.
"""

from __future__ import annotations

import sys
from pathlib import Path

from spl.daemon.server import DaemonRuntime
from spl.daemon.signature import build_signature
from spl.daemon.store import RegistryStore

REFERENCE_YAML = (
    "- !DFunction\n"
    "  name: demo_obj\n"
    "  inputs: []\n"
    "  outputs:\n"
    "  - name: default\n"
    "    type: int\n"
    "  body: |-\n"
    "    return 1\n"
)
AUTHOR_ENV = "author_env"
AUTHOR_ENV_PYTHON = "/opt/spl-author/python3.13/bin/python"
AUTHOR_ENV_PYTHON_VERSION = "Python 3.13.0"

# Shared with the server test — do not edit one side only.
EXPECTED_CORE = {
    "name": "demo_obj",
    "kind": "function",
    "version": 1,
    "env": AUTHOR_ENV,
    "env_python": AUTHOR_ENV_PYTHON,
    "env_python_version": AUTHOR_ENV_PYTHON_VERSION,
    "inputs": [],
    "outputs": [
        {
            "name": "default",
            "notes": "Function calls return the function value directly.",
            "ports": [{"name": "default", "type": "int"}],
            "read": "result.value",
            "result_accessor": "result.value",
            "selector": None,
            "type": "int",
            "value_path": [],
        }
    ],
}
REQUIRED_SHARED_KEYS = {
    "name",
    "kind",
    "version",
    "version_id",
    "description",
    "env",
    "env_python",
    "env_python_version",
    "inputs",
    "outputs",
    "call",
    "internal_functions",
}


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection: object, *, token: str) -> None:
        pass

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


def test_daemon_signature_matches_shared_contract(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env(AUTHOR_ENV, sys.executable)
        version = store.objects.register_object(
            "demo_obj",
            "demo_obj",
            AUTHOR_ENV,
            yaml_text=REFERENCE_YAML,
        )
        with store._lock, store._conn:  # noqa: SLF001 - contract seeds author provenance.
            store._conn.execute(
                "UPDATE object_versions SET env_python = ? WHERE id = ?",
                (AUTHOR_ENV_PYTHON, version["version_id"]),
            )
        store.objects._python_version_cache[  # noqa: SLF001 - cache pin keeps the contract host-independent.
            str(Path(AUTHOR_ENV_PYTHON).expanduser().absolute())
        ] = AUTHOR_ENV_PYTHON_VERSION
        signature = build_signature(store.objects.get_object("demo_obj"))
        runtime = DaemonRuntime(
            store,
            auto_build_envs=False,
            heartbeat_service=_NoopHeartbeats(),
        )
        sync_event = runtime.enqueue_object_sync(version)
    finally:
        store.close()

    core = {key: signature[key] for key in EXPECTED_CORE}
    assert core == EXPECTED_CORE
    assert REQUIRED_SHARED_KEYS <= set(signature)
    assert {
        "env": sync_event["payload"]["env"],
        "env_python": sync_event["payload"]["env_python"],
        "env_python_version": sync_event["payload"]["env_python_version"],
    } == {
        "env": AUTHOR_ENV,
        "env_python": AUTHOR_ENV_PYTHON,
        "env_python_version": AUTHOR_ENV_PYTHON_VERSION,
    }
