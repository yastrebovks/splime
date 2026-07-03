"""Deprecation and removal contract for the 0.2.0 cleanup (WP-07b).

Three layers are pinned here:

* aliases that warned through 0.1.4/0.1.5 are **gone** in 0.2.0;
* the canonical replacements stay silent;
* the new 0.2.0 deprecations (deep-import shims, convenience ``NodeRemote``
  constructor forms) warn while keeping the old behavior.

See ``docs/migration-0.2.0.md``.
"""

from __future__ import annotations

import importlib
import warnings
from typing import Any

import pytest

from spl import DEFAULT_PORT, Deployment, InputPort, NodeRemote, OutputPort, lift
from spl._client import SPLClient

REMOVED_CLIENT_ALIASES = (
    "create_library",
    "get_library",
    "update_library",
    "delete_library",
    "grant_library",
    "revoke_library_grant",
    "add_reference",
    "copy_object",
    "remove_entry",
    "local_objects",
    "server_objects",
    "start",
    "queue",
    "run_node",
    "run_node_result",
)


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


class TestRemovedAliases:
    def test_legacy_client_aliases_are_gone(self) -> None:
        client, _ = _client()
        for alias in REMOVED_CLIENT_ALIASES:
            assert not hasattr(client, alias), (
                f"SPLClient.{alias} was removed in 0.2.0 and must not come back"
            )

    def test_canonical_library_namespace_is_silent(self) -> None:
        client, _ = _client()

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            created = client.library.create("risk")
            granted = client.library.grant("risk", "analyst1", scopes=["execute"])

        assert created["slug"] == "risk"
        assert granted["grantee_id"] == "analyst1"

    def test_canonical_objects_and_submit_are_silent(self) -> None:
        client, _ = _client()

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            client.objects(scope="local")
            client.objects(scope="server")
            client.objects(scope="all")
            run = client.submit("obj")

        assert run.id == "run-1"

    def test_deployment_remote_path_is_silent(self) -> None:
        client, _ = _client()
        node = _offline_node()
        pipeline = lift(node).bind(amount=2).alias("out").render("deprecation_probe")

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            value = Deployment(client, pipeline).run(output="out")

        assert value == {"kwargs": {"amount": 2}}


class TestDeepImportShims:
    def test_spl_client_shim_warns_and_matches_canonical(self) -> None:
        import spl
        import spl.client as legacy_client

        with pytest.warns(DeprecationWarning, match=r"importing from spl\.client"):
            importlib.reload(legacy_client)

        assert legacy_client.SPLClient is spl.SPLClient
        assert legacy_client.RemoteResult is spl.RemoteResult
        # Attribute fallback keeps even private helpers reachable.
        assert legacy_client._progress_callback is not None

    def test_spl_core_common_shim_warns_and_matches_canonical(self) -> None:
        import spl
        import spl.core.common as legacy_common

        with pytest.warns(DeprecationWarning, match=r"importing from spl\.core\.common"):
            importlib.reload(legacy_common)

        assert legacy_common.Deployment is spl.Deployment
        assert legacy_common.lift is spl.lift
        assert legacy_common.Run is not None


class TestNodeRemoteConstructorForms:
    def test_pipeline_keyword_warns(self) -> None:
        with pytest.warns(DeprecationWarning, match=r"NodeRemote\.locate"):
            node = NodeRemote(
                pipeline="demo_pipeline",
                function="happiness",
                inputs=[],
                outputs=[],
            )
        assert node.name == "demo_pipeline::happiness"

    def test_function_keyword_warns(self) -> None:
        with pytest.warns(DeprecationWarning, match=r"NodeRemote\.locate"):
            node = NodeRemote(
                name="demo_pipeline",
                function="happiness",
                inputs=[],
                outputs=[],
            )
        assert node.name == "demo_pipeline::happiness"

    def test_name_in_url_slot_warns(self) -> None:
        with pytest.warns(DeprecationWarning, match=r"NodeRemote\.locate"):
            node = NodeRemote("demo_obj", inputs=[], outputs=[])
        assert node.name == "demo_obj"
        assert node.url == ""

    def test_plain_serialization_constructor_is_silent(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            node = NodeRemote(
                url="http://fake",
                name="demo_obj",
                version="latest",
                inputs=[],
                outputs=[],
            )
        assert node.name == "demo_obj"

    def test_locate_is_silent_for_all_forms(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import spl.daemon_client as daemon_client

        class FakeDaemonClient:
            def resolve_remote_signature(self, ref: dict[str, Any]) -> dict[str, Any]:
                return {
                    "signature": {
                        "inputs": [{"name": "a", "type": "int"}],
                        "outputs": [{"name": "default", "type": "str"}],
                    }
                }

        monkeypatch.setattr(daemon_client, "Client", FakeDaemonClient)

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            by_name = NodeRemote.locate(
                name="demo_obj",
                url="http://fake",
            )
            by_pipeline = NodeRemote.locate(
                pipeline="demo_pipeline",
                function="happiness",
                url="http://fake",
            )
            by_function = NodeRemote.locate(
                name="demo_pipeline",
                function="happiness",
                url="http://fake",
            )

        assert by_name.name == "demo_obj"
        assert by_pipeline.name == "demo_pipeline::happiness"
        assert by_function.name == "demo_pipeline::happiness"

    def test_locate_requires_a_reference(self) -> None:
        with pytest.raises(TypeError, match="requires name or pipeline"):
            NodeRemote.locate(url="http://fake")

    def test_locate_rejects_name_with_pipeline(self) -> None:
        with pytest.raises(TypeError, match="not both"):
            NodeRemote.locate(name="a", pipeline="b")
