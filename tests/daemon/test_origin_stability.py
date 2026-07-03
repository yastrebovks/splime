"""Origin stability: local authorship is sticky (Stage 2.1 of the 0.2.0 plan).

Republishing content never silently demotes a locally authored object to a
``server`` mirror; a mirror the caller republishes themselves is adopted and
becomes ``local``.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from spl.daemon.store import RegistryStore

from .test_object_identity import FUNCTION_YAML


@pytest.fixture()
def store(tmp_path: Path) -> Iterator[RegistryStore]:
    registry = RegistryStore(tmp_path)
    registry.register_env("default", sys.executable)
    try:
        yield registry
    finally:
        registry.close()


def _register(store: RegistryStore, *, origin: str, **kwargs: object) -> dict[str, object]:
    return store.objects.register_object(
        "origin_probe",
        "demo_obj",
        "default",
        yaml_text=FUNCTION_YAML,
        origin=origin,
        **kwargs,
    )


def test_local_origin_survives_server_republish(store: RegistryStore) -> None:
    """A locally authored object stays ``local`` after a server-side dedup."""

    _register(store, origin="local")
    _register(store, origin="server", remote_object_id="remote123")

    record = store.objects.get_object("origin_probe")
    assert record["origin"] == "local"
    # The server linkage is still adopted — only the authorship flag is sticky.
    assert record["object_remote_object_id"] == "remote123"


def test_mirror_is_adopted_by_local_republish(store: RegistryStore) -> None:
    """Republishing a mirrored object yourself takes ownership: server -> local."""

    _register(store, origin="server", remote_object_id="remote123")
    _register(store, origin="local")

    assert store.objects.get_object("origin_probe")["origin"] == "local"


def test_server_origin_is_kept_for_pure_mirrors(store: RegistryStore) -> None:
    """A mirror that is only ever synced from the server stays ``server``."""

    _register(store, origin="server", remote_object_id="remote123")
    _register(store, origin="server", remote_object_id="remote123")

    assert store.objects.get_object("origin_probe")["origin"] == "server"
