"""Golden snapshot of the public :mod:`spl` surface.

If this test fails, the public API changed. Either the change is intentional —
then update the expected values below in the SAME PR — or it is accidental and
must be reverted.
"""

from __future__ import annotations

import inspect

import spl
import spl.client
import spl.core.common
import spl.core.entities.node

EXPECTED_ALL = {
    "SPLClient",
    "SPLServerClient",
    "RemoteRun",
    "RemoteResult",
    "PublishedObject",
    "NodeRemote",
    "lift",
    "Deployment",
    "DEFAULT_PORT",
    "InputPort",
    "OutputPort",
    "DDistribution",
    "spl_export_to_file",
    "spl_export_to_dir",
    "spl_import_from_file",
}


def test_public_all_matches_snapshot() -> None:
    assert set(spl.__all__) == EXPECTED_ALL


def test_facade_symbols_are_canonical() -> None:
    assert spl.lift is spl.core.common.lift
    assert spl.Deployment is spl.core.common.Deployment
    assert spl.SPLClient is spl.client.SPLClient
    assert spl.DEFAULT_PORT == spl.core.entities.node.DEFAULT_PORT == "default"


def test_call_signature_keeps_expected_parameters() -> None:
    params = inspect.signature(spl.SPLClient.call).parameters
    for name in ("name", "kwargs", "output", "function", "target_machine"):
        assert name in params


def test_objects_signature_keeps_expected_parameters() -> None:
    params = inspect.signature(spl.SPLClient.objects).parameters
    for name in ("compact", "scope"):
        assert name in params
