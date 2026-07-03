"""Daemon side of the signature contract (stage 1.3).

KEEP IN SYNC with ``spl-server/tests/test_signature_contract.py``: both files
pin the SAME ``EXPECTED_CORE`` for the same reference object. The daemon and
the server duplicate their signature builders; this contract makes any
semantic drift in the shared core (name/kind/version/inputs/outputs) fail CI
on the side that drifted. Envelope keys beyond the core may differ by design
(daemon adds display_name/origin, the server adds owner/library/execution).
"""

from __future__ import annotations

import sys
from pathlib import Path

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

# Shared with the server test — do not edit one side only.
EXPECTED_CORE = {
    "name": "demo_obj",
    "kind": "function",
    "version": 1,
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
    "inputs",
    "outputs",
    "call",
    "internal_functions",
}


def test_daemon_signature_matches_shared_contract(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)
        store.objects.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=REFERENCE_YAML,
        )
        signature = build_signature(store.objects.get_object("demo_obj"))
    finally:
        store.close()

    core = {key: signature[key] for key in EXPECTED_CORE}
    assert core == EXPECTED_CORE
    assert REQUIRED_SHARED_KEYS <= set(signature)
