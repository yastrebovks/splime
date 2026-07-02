"""Deprecation contract for the 0.2.0 cleanup (WP-07a).

Every legacy path keeps working in 0.1.x but emits ``DeprecationWarning``;
every canonical path stays silent. See ``docs/migration-0.2.0.md``.
"""

from __future__ import annotations

import warnings
from typing import Any

import pytest

from spl import DEFAULT_PORT, Deployment, InputPort, NodeRemote, OutputPort, lift
from spl.client import SPLClient


class _FakeDaemon:
    def __init__(self) -> None:
        self.library_payloads: list[dict[str, Any]] = []

    def server_connection(self) -> dict[str, Any]:
        return {"connected": True, "server_url": "http://fake"}

    def list_objects(self, *, compact: bool = False) -> dict[str, Any]:
        return {"obj": {"name": "obj", "compact": compact}}

    def server_objects(self, **kwargs: Any) -> list[dict[str, Any]]:
        return [dict(kwargs)]

    def run(self, name: str, **kwargs: Any) -> dict[str, Any]:
        return {"id": "run-1", "status": "succeeded", "name": name}

    def create_server_library(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.library_payloads.append(payload)
        return payload

    def grant_server_library(self, ref: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ref": ref, **payload}

    def run_remote_node(
        self,
        payload: dict[str, Any],
        *,
        kwargs: dict[str, Any],
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        return {"value": {"kwargs": kwargs}}


def _client() -> tuple[SPLClient, _FakeDaemon]:
    client = SPLClient(daemon_port=8765)
    fake = _FakeDaemon()
    client._daemon = fake  # type: ignore[assignment]
    return client, fake


def _offline_node() -> NodeRemote:
    return NodeRemote(
        url="http://fake",
        name="obj::inner",
        version="latest",
        inputs=[InputPort(name="amount", typ_="int", default=None)],
        outputs=[OutputPort(name=DEFAULT_PORT, typ_="str")],
    )


def test_flat_library_methods_warn_and_delegate() -> None:
    client, fake = _client()

    with pytest.warns(DeprecationWarning, match=r"library\.create"):
        created = client.create_library("risk", display_name="Risk")
    assert created["slug"] == "risk"
    assert fake.library_payloads[-1]["display_name"] == "Risk"

    with pytest.warns(DeprecationWarning, match=r"library\.grant"):
        granted = client.grant_library("risk", "analyst1", scopes=["execute"])
    assert granted == {
        "ref": "risk",
        "grantee_id": "analyst1",
        "grantee_type": "user",
        "scopes": ["execute"],
    }


def test_canonical_library_namespace_is_silent() -> None:
    client, _ = _client()

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        created = client.library.create("risk")
        granted = client.library.grant("risk", "analyst1", scopes=["execute"])

    assert created["slug"] == "risk"
    assert granted["grantee_id"] == "analyst1"


def test_object_list_aliases_warn_objects_is_silent() -> None:
    client, _ = _client()

    with pytest.warns(DeprecationWarning, match=r"objects\(scope='local'\)"):
        assert client.local_objects() == [{"name": "obj", "compact": False}]
    with pytest.warns(DeprecationWarning, match=r"objects\(scope='server'\)"):
        client.server_objects()

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        client.objects(scope="local")
        client.objects(scope="server")
        client.objects(scope="all")


def test_execution_aliases_warn_submit_is_silent() -> None:
    client, _ = _client()

    with pytest.warns(DeprecationWarning, match=r"submit"):
        client.start("obj")
    with pytest.warns(DeprecationWarning, match=r"offline_policy"):
        client.queue("obj", target_machine="m1")

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        run = client.submit("obj")
    assert run.id == "run-1"


def test_run_node_warns_deployment_path_is_silent() -> None:
    client, _ = _client()
    node = _offline_node()

    with pytest.warns(DeprecationWarning, match=r"Deployment"):
        direct = client.run_node(node, {"amount": 1})
    assert direct == {"kwargs": {"amount": 1}}

    pipeline = lift(node).bind(amount=2).alias("out").render("deprecation_probe")
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        value = Deployment(client, pipeline).run(output="out")
    assert value == {"kwargs": {"amount": 2}}
