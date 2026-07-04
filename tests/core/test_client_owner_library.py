from __future__ import annotations

from typing import Any

import pytest

# The implementation module is patched directly: monkeypatching the
# ``spl.client`` shim would not affect lookups inside ``spl._client``.
import spl._client as spl_client_module
from spl._client import SPLClient
from spl.core.entities.node_remote import NodeRemote
from spl.daemon_client import Client, ClientError
from spl.server_client import SPLServerClient


class RecordingClient(Client):
    def __init__(self) -> None:
        super().__init__("http://daemon.local")
        self.requests: list[tuple[str, str, dict[str, Any] | None]] = []

    def _json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        self.requests.append((method, path, payload))
        return {"id": "remote-run-1", "status": "queued"}


def test_daemon_client_run_sends_owner_library_and_remote_flag() -> None:
    client = RecordingClient()

    client.run(
        "fraud_score",
        kwargs={"customer_id": 42},
        target_machine="gpu-a",
        object_owner_id="alice",
        library="risk",
        offline_policy="queue",
    )

    method, path, payload = client.requests[-1]
    assert method == "POST"
    assert path == "/runs"
    assert payload == {
        "object": "fraud_score",
        "source": "auto",
        "kwargs": {"customer_id": 42},
        "target_machine": "gpu-a",
        "object_owner_id": "alice",
        "library": "risk",
        "offline_policy": "queue",
        "remote": True,
    }


def test_daemon_client_register_object_sends_target_library() -> None:
    client = RecordingClient()

    client.register_object(
        "fraud_score",
        entrypoint="fraud_score",
        env="default",
        yaml_text="objects: []\n",
        library="risk",
        create_library=True,
        library_display_name="Risk",
    )

    method, path, payload = client.requests[-1]
    assert method == "POST"
    assert path == "/objects"
    assert payload == {
        "name": "fraud_score",
        "entrypoint": "fraud_score",
        "env": "default",
        "yaml": "objects: []\n",
        "local_only": False,
        "library": "risk",
        "create_library": True,
        "library_display_name": "Risk",
    }


def test_daemon_client_library_management_paths() -> None:
    client = RecordingClient()

    client.server_libraries(include_accessible=False)
    client.create_server_library({"slug": "risk"})
    client.get_server_library("risk")
    client.update_server_library("risk", {"description": "Updated"})
    with pytest.raises(NotImplementedError, match="not supported"):
        client.delete_server_library("risk")
    client.server_library_grants("risk")
    client.grant_server_library("risk", {"grantee_id": "admin2"})
    client.revoke_server_library_grant("risk", "admin2")
    client.add_server_library_reference("risk", {"name": "source"})
    client.copy_server_library_object("risk", {"name": "source"})
    client.remove_server_library_entry("risk", "source")

    assert client.requests[-10:] == [
        ("GET", "/server/libraries?include_accessible=0", None),
        ("POST", "/server/libraries", {"slug": "risk"}),
        ("GET", "/server/libraries/risk", None),
        ("PUT", "/server/libraries/risk", {"description": "Updated"}),
        ("GET", "/server/libraries/risk/grants", None),
        ("POST", "/server/libraries/risk/grants", {"grantee_id": "admin2"}),
        ("POST", "/server/libraries/risk/grants/admin2/revoke", None),
        ("POST", "/server/libraries/risk/references", {"name": "source"}),
        ("POST", "/server/libraries/risk/copies", {"name": "source"}),
        ("DELETE", "/server/libraries/risk/entries/source", None),
    ]


def test_daemon_client_local_cleanup_paths() -> None:
    client = RecordingClient()

    client.forget("demo obj", owner_id="owner 1", library="risk team")
    client.remove_local("demo_obj")
    client.forget_version("demo_obj", 2, library="research")
    client.prune_stale_mirrors(owner_id="owner-1")

    assert client.requests[-4:] == [
        ("DELETE", "/objects/demo%20obj?owner_id=owner+1&library=risk+team", None),
        ("DELETE", "/objects/demo_obj", None),
        ("DELETE", "/objects/demo_obj/versions/2?library=research", None),
        ("POST", "/objects/prune-stale-mirrors?owner_id=owner-1", None),
    ]


class FakeDaemon:
    def __init__(self) -> None:
        self.run_calls: list[dict[str, Any]] = []
        self.signature_calls: list[dict[str, Any]] = []
        self.remote_node_calls: list[dict[str, Any]] = []
        self.remote_decomposition_calls: list[dict[str, Any]] = []
        self.object_calls: list[dict[str, Any]] = []
        self.decomposition_calls: list[dict[str, Any]] = []
        self.list_object_calls: list[dict[str, Any]] = []
        self.server_object_calls: list[dict[str, Any]] = []
        self.library_calls: list[tuple[Any, ...]] = []
        self.register_object_calls: list[dict[str, Any]] = []
        self.cleanup_calls: list[tuple[Any, ...]] = []
        self.server_connected = False
        self.server_url = "https://splime.io/api"

    def connect_server(
        self,
        *,
        machine_token: str,
        user_token: str,
        server_url: str,
        machine_id: str | None = None,
        display_name: str | None = None,
        capabilities: dict[str, Any] | None = None,
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        del machine_token, user_token
        self.server_connected = True
        self.server_url = server_url
        return {
            "connected": True,
            "connection": {
                "status": "connected",
                "server_url": server_url,
                "machine_id": machine_id,
                "display_name": display_name,
                "capabilities": capabilities or {},
                "heartbeat_interval_seconds": heartbeat_interval_seconds,
            },
        }

    def run(self, object_name: str, **kwargs: Any) -> dict[str, Any]:
        self.run_calls.append({"object_name": object_name, **kwargs})
        return {"id": "remote-run-2", "status": "queued"}

    def wait_remote_run(
        self,
        run_id: str,
        *,
        poll_interval: float,
        timeout_seconds: float | None,
        on_state: Any | None = None,
    ) -> dict[str, Any]:
        state = {
            "id": run_id,
            "status": "succeeded",
            "result": {"result": {"score": 0.91}, "artifacts": {}},
        }
        if on_state is not None:
            # Mirror the real client: the callback sees the terminal state.
            on_state(state)
        return state

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        return {
            "id": run_id,
            "status": "succeeded",
            "result": {"result": {"score": 0.91}, "artifacts": {}},
        }

    def wait_run(
        self,
        run_id: str,
        *,
        poll_interval: float,
        timeout_seconds: float | None,
        on_state: Any | None = None,
    ) -> dict[str, Any]:
        state = {"id": run_id, "status": "succeeded"}
        if on_state is not None:
            # Mirror the real client: the callback sees the terminal state.
            on_state(state)
        return state

    def get_run(self, run_id: str) -> dict[str, Any]:
        return {"id": run_id, "status": "succeeded"}

    def result(self, run_id: str) -> dict[str, Any]:
        return {"result": {"local": True}, "artifacts": {}}

    def list_artifacts(self, run_id: str) -> list[str]:
        return []

    def run_remote_node(
        self,
        node: dict[str, Any],
        *,
        kwargs: dict[str, Any],
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        self.remote_node_calls.append(
            {
                "node": node,
                "kwargs": kwargs,
                "timeout_seconds": timeout_seconds,
            }
        )
        return {
            "value": "Happy",
            "run_id": "remote-run-1",
            "status": "succeeded",
            "run": {"id": "remote-run-1", "status": "succeeded"},
            "payload": {"result": {"raw": "Happy"}, "artifacts": {"plot": "server-path"}},
            "artifacts": {"plot": "server-path"},
        }

    def resolve_remote_decomposition(self, ref: dict[str, Any]) -> dict[str, Any]:
        self.remote_decomposition_calls.append(ref)
        return {
            "decomposition": {
                "functions": [],
                "nodes": [
                    {
                        "node_id": "remote-calc",
                        "kind": "remote",
                        "name": ref["name"],
                        "inputs": [{"name": "a", "type": "int"}],
                        "outputs": [{"name": "default", "type": "str"}],
                    }
                ],
                "links": [],
            },
            "object": {
                "id": "remote-object-1",
                "name": ref["name"],
                "display_name": "Remote Demo Pipeline",
                "kind": "pipeline",
                "version": 3,
                "library": {"slug": ref.get("library") or "default"},
            },
            "remote": {
                "name": ref["name"],
                "library": ref.get("library"),
                "owner_id": ref.get("owner_id"),
                "object_id": "remote-object-1",
            },
        }

    def signature(self, name: str, **kwargs: Any) -> dict[str, Any]:
        self.signature_calls.append({"name": name, **kwargs})
        display_name = "pretty_score" if name == "server.remote-score" else name
        return {
            "name": name,
            "display_name": display_name,
            "version": 7,
            "kind": "function",
            "description": "",
            "inputs": [],
            "outputs": [],
            "call": {
                "example": f'result = client.call("{display_name}", kwargs={{}})',
                "read": "result.value",
            },
        }

    def get_object(
        self,
        name: str,
        *,
        version: int | None = None,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        self.object_calls.append(
            {
                "name": name,
                "version": version,
                "include_yaml": include_yaml,
            }
        )
        return {
            "id": "object-1",
            "name": name,
            "display_name": "Demo Pipeline",
            "kind": "pipeline",
            "version": version or 1,
            "decomposition": {
                "functions": [],
                "nodes": [
                    {
                        "node_id": "calc",
                        "kind": "function",
                        "function": "calculate",
                        "inputs": [{"name": "x", "type": "int"}],
                        "outputs": [{"name": "default", "type": "int"}],
                    }
                ],
                "links": [],
            },
            "yaml": "",
        }

    def decomposition(
        self,
        name: str,
        *,
        version: int | None = None,
    ) -> dict[str, Any]:
        self.decomposition_calls.append({"name": name, "version": version})
        return self.get_object(name, version=version)["decomposition"]

    def server_connection(self) -> dict[str, Any]:
        return {
            "connected": self.server_connected,
            "connection": {
                "status": "connected" if self.server_connected else "offline",
                "server_url": self.server_url,
            },
        }

    def list_objects(self, *, compact: bool = False) -> dict[str, Any]:
        self.list_object_calls.append({"compact": compact})
        return {"local_obj": {"name": "local_obj", "compact": compact}}

    def server_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        self.server_object_calls.append(
            {
                "owner_id": owner_id,
                "library": library,
                "compact": compact,
            }
        )
        return [
            {
                "name": "server_obj",
                "owner_id": owner_id or "owner-1",
                "library": library or "default",
                "compact": compact,
            }
        ]

    def server_libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        self.library_calls.append(("server_libraries", include_accessible))
        return [{"slug": "risk", "accessible": include_accessible}]

    def create_server_library(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.library_calls.append(("create_server_library", payload))
        return {"slug": payload["slug"], "created": True}

    def get_server_library(self, library_ref: str) -> dict[str, Any]:
        self.library_calls.append(("get_server_library", library_ref))
        return {"slug": library_ref}

    def update_server_library(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self.library_calls.append(("update_server_library", library_ref, payload))
        return {"slug": library_ref, **payload}

    def delete_server_library(self, library_ref: str) -> dict[str, Any]:
        self.library_calls.append(("delete_server_library", library_ref))
        return {"slug": library_ref, "deleted": True}

    def server_library_grants(self, library_ref: str) -> list[dict[str, Any]]:
        self.library_calls.append(("server_library_grants", library_ref))
        return [{"library": library_ref, "grantee_id": "admin2"}]

    def grant_server_library(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self.library_calls.append(("grant_server_library", library_ref, payload))
        return {"library": library_ref, **payload}

    def revoke_server_library_grant(
        self,
        library_ref: str,
        grantee: str,
    ) -> dict[str, Any]:
        self.library_calls.append(("revoke_server_library_grant", library_ref, grantee))
        return {"library": library_ref, "grantee": grantee, "revoked": True}

    def add_server_library_reference(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self.library_calls.append(("add_server_library_reference", library_ref, payload))
        return {"library": library_ref, "reference": payload}

    def copy_server_library_object(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self.library_calls.append(("copy_server_library_object", library_ref, payload))
        return {"library": library_ref, "copy": payload}

    def remove_server_library_entry(
        self,
        library_ref: str,
        name: str,
    ) -> dict[str, Any]:
        self.library_calls.append(("remove_server_library_entry", library_ref, name))
        return {"library": library_ref, "name": name, "removed": True}

    def forget(
        self,
        name: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        self.cleanup_calls.append(("forget", name, owner_id, library))
        return {"name": name, "forgotten": True}

    def forget_version(
        self,
        name: str,
        version: str | int,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        self.cleanup_calls.append(("forget_version", name, version, owner_id, library))
        return {"name": name, "version": version, "forgotten": True}

    def prune_stale_mirrors(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        self.cleanup_calls.append(("prune_stale_mirrors", owner_id, library))
        return {"count": 0, "pruned": []}

    def register_object(self, name: str, **kwargs: Any) -> dict[str, Any]:
        self.register_object_calls.append({"name": name, **kwargs})
        return {
            "name": name,
            "entrypoint": kwargs["entrypoint"],
            "env": kwargs["env"],
            "yaml_path": "/tmp/spl-demo.yaml",
            "workdir": kwargs.get("workdir"),
        }


class MissingServerConnectionDaemon(FakeDaemon):
    def server_connection(self) -> dict[str, Any]:
        return {"connected": False, "offline": False, "connection": None}

    def server_machines(self) -> dict[str, Any]:
        raise ClientError("404: active server connection is not found")

    def server_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        self.server_object_calls.append(
            {
                "owner_id": owner_id,
                "library": library,
                "compact": compact,
            }
        )
        raise ClientError("404: active server connection is not found")

    def server_libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        self.library_calls.append(("server_libraries", include_accessible))
        raise ClientError("404: active server connection is not found")


class MissingServerConnectionErrorDaemon(MissingServerConnectionDaemon):
    def server_connection(self) -> dict[str, Any]:
        raise ClientError("404: active server connection is not found")


class FailingServerDaemon(MissingServerConnectionDaemon):
    def server_connection(self) -> dict[str, Any]:
        return {"connected": True, "offline": False, "connection": {"id": "conn-1"}}

    def server_libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        self.library_calls.append(("server_libraries", include_accessible))
        raise ClientError("503: upstream unavailable")


class BrokenConnectionStateDaemon(MissingServerConnectionDaemon):
    def server_connection(self) -> dict[str, Any]:
        raise ClientError("503: connection state unavailable")


def test_daemon_client_decomposition_uses_version_query() -> None:
    client = RecordingClient()

    client.decomposition("demo_pipeline", version=3)

    method, path, payload = client.requests[-1]
    assert method == "GET"
    assert path == "/objects/demo_pipeline/decomposition?version=3"
    assert payload is None


def test_daemon_client_resolves_remote_decomposition() -> None:
    client = RecordingClient()

    client.resolve_remote_decomposition(
        {
            "name": "demo_traktorist_pipeline",
            "library": "tractors",
            "version": "latest",
        }
    )

    method, path, payload = client.requests[-1]
    assert method == "POST"
    assert path == "/remote-decompositions/resolve"
    assert payload == {
        "ref": {
            "name": "demo_traktorist_pipeline",
            "library": "tractors",
            "version": "latest",
        }
    }


def test_spl_client_call_and_signature_accept_owner_library() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    result = client.call(
        "fraud_score",
        owner="alice",
        library="risk",
        target_machine="gpu-a",
        kwargs={"customer_id": 42},
        offline_policy="queue",
    )
    signature = client.signature("fraud_score", owner="alice", library="risk")

    assert result.value == {"score": 0.91}
    assert fake_daemon.run_calls[-1]["object_owner_id"] == "alice"
    assert fake_daemon.run_calls[-1]["library"] == "risk"
    assert fake_daemon.run_calls[-1]["remote"] is True
    assert fake_daemon.run_calls[-1]["target_machine"] == "gpu-a"
    assert signature["name"] == "fraud_score"
    assert fake_daemon.signature_calls[-1] == {
        "name": "fraud_score",
        "version": None,
        "owner_id": "alice",
        "library": "risk",
        "function": None,
    }


def test_spl_client_submit_is_async_alias_for_start() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    run = client.submit("fraud_score", kwargs={"customer_id": 42}, output="score")

    assert run.id == "remote-run-2"
    assert run.mode == "local"
    assert fake_daemon.run_calls[-1] == {
        "object_name": "fraud_score",
        "args": None,
        "kwargs": {"customer_id": 42},
        "output": "score",
        "timeout_seconds": None,
        "target_machine": None,
        "object_owner_id": None,
        "library": None,
        "offline_policy": None,
        "function": None,
        "source": "auto",
        "remote": None,
    }


def test_spl_client_objects_auto_prefers_server_when_connected() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    objects = client.objects(compact=True)

    assert objects == [
        {
            "name": "server_obj",
            "owner_id": "owner-1",
            "library": "default",
            "compact": True,
        }
    ]
    assert fake_daemon.server_object_calls[-1] == {
        "owner_id": None,
        "library": None,
        "compact": True,
    }
    assert fake_daemon.list_object_calls == []


def test_spl_client_objects_auto_uses_local_when_disconnected() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    objects = client.objects(compact=True)

    assert objects == {"local_obj": {"name": "local_obj", "compact": True}}
    assert fake_daemon.list_object_calls[-1] == {"compact": True}
    assert fake_daemon.server_object_calls == []


# ``local_objects()``/``server_objects()`` were removed in 0.2.0 (WP-07b);
# the removal itself is pinned in ``test_deprecations.py``.  The canonical
# ``objects(scope=...)`` behavior stays covered here.


def test_spl_client_objects_server_scope_returns_stable_list() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    objects = client.objects(scope="server", owner="alice", library="risk", compact=True)

    assert objects == [
        {
            "name": "server_obj",
            "owner_id": "alice",
            "library": "risk",
            "compact": True,
        }
    ]
    assert fake_daemon.server_object_calls[-1] == {
        "owner_id": "alice",
        "library": "risk",
        "compact": True,
    }
    assert fake_daemon.list_object_calls == []


def test_spl_client_library_management_methods_use_daemon() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    assert client.libraries(include_accessible=False) == [
        {"slug": "risk", "accessible": False}
    ]
    assert client.library.create(
        "risk",
        display_name="Risk",
        description="Models",
        visibility="public",
        default_machine="gpu-a",
        execution={"mode": "manual"},
    ) == {"slug": "risk", "created": True}
    assert client.library.get("risk") == {"slug": "risk"}
    assert client.library.update(
        "risk",
        display_name="Risk Team",
        description="Updated",
        visibility="private",
        default_machine="gpu-b",
        execution={"mode": "auto"},
    ) == {
        "slug": "risk",
        "display_name": "Risk Team",
        "description": "Updated",
        "visibility": "private",
        "default_machine_id": "gpu-b",
        "execution": {"mode": "auto"},
    }
    assert client.library.grant(
        "risk",
        "admin2",
        scopes=["metadata:read"],
    ) == {
        "library": "risk",
        "grantee_id": "admin2",
        "grantee_type": "user",
        "scopes": ["metadata:read"],
    }
    assert client.library.revoke("risk", "admin2") == {
        "library": "risk",
        "grantee": "admin2",
        "revoked": True,
    }
    assert client.library.add_reference(
        "risk",
        "source",
        owner="alice",
        from_library="default",
        version="latest",
        alias="alice_source",
    ) == {
        "library": "risk",
        "reference": {
            "name": "source",
            "from_library": "default",
            "from_owner": "alice",
            "version": "latest",
            "alias": "alice_source",
        },
    }
    assert client.library.copy_object(
        "source",
        into_library="risk",
        from_owner="alice",
        from_library="default",
        version=3,
        new_name="source_copy",
    ) == {
        "library": "risk",
        "copy": {
            "name": "source",
            "from_library": "default",
            "from_owner": "alice",
            "version": 3,
            "new_name": "source_copy",
        },
    }
    assert client.library.remove_entry("risk", "source") == {
        "library": "risk",
        "name": "source",
        "removed": True,
    }
    with pytest.raises(NotImplementedError, match="not supported"):
        client.library.delete("risk")

    assert fake_daemon.library_calls == [
        ("server_libraries", False),
        (
            "create_server_library",
            {
                "slug": "risk",
                "display_name": "Risk",
                "description": "Models",
                "visibility": "public",
                "default_machine_id": "gpu-a",
                "execution": {"mode": "manual"},
            },
        ),
        ("get_server_library", "risk"),
        (
            "update_server_library",
            "risk",
            {
                "display_name": "Risk Team",
                "description": "Updated",
                "visibility": "private",
                "default_machine_id": "gpu-b",
                "execution": {"mode": "auto"},
            },
        ),
        (
            "grant_server_library",
            "risk",
            {
                "grantee_id": "admin2",
                "grantee_type": "user",
                "scopes": ["metadata:read"],
            },
        ),
        ("revoke_server_library_grant", "risk", "admin2"),
        (
            "add_server_library_reference",
            "risk",
            {
                "name": "source",
                "from_library": "default",
                "from_owner": "alice",
                "version": "latest",
                "alias": "alice_source",
            },
        ),
        (
            "copy_server_library_object",
            "risk",
            {
                "name": "source",
                "from_library": "default",
                "from_owner": "alice",
                "version": 3,
                "new_name": "source_copy",
            },
        ),
        ("remove_server_library_entry", "risk", "source"),
    ]


def test_spl_client_library_namespace_delegates_to_flat_methods() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    library = client.library

    assert library.list(include_accessible=False) == [{"slug": "risk", "accessible": False}]
    assert library.create("risk", display_name="Risk") == {"slug": "risk", "created": True}
    assert library.get("risk") == {"slug": "risk"}
    assert library.update("risk", description="Updated") == {
        "slug": "risk",
        "description": "Updated",
    }
    assert library.grant("risk", "admin2", scopes=["read"]) == {
        "library": "risk",
        "grantee_id": "admin2",
        "grantee_type": "user",
        "scopes": ["read"],
    }
    assert library.revoke("risk", "admin2") == {
        "library": "risk",
        "grantee": "admin2",
        "revoked": True,
    }
    assert library.add_reference(
        "risk",
        "source",
        from_library="default",
        alias="source_alias",
    ) == {
        "library": "risk",
        "reference": {
            "name": "source",
            "from_library": "default",
            "version": "latest",
            "alias": "source_alias",
        },
    }
    assert library.copy_object(
        "source",
        into_library="risk",
        from_library="default",
        new_name="source_copy",
    ) == {
        "library": "risk",
        "copy": {
            "name": "source",
            "from_library": "default",
            "version": "latest",
            "new_name": "source_copy",
        },
    }
    assert library.remove_entry("risk", "source") == {
        "library": "risk",
        "name": "source",
        "removed": True,
    }
    with pytest.raises(NotImplementedError, match="not supported"):
        library.delete("risk")

    assert fake_daemon.library_calls == [
        ("server_libraries", False),
        (
            "create_server_library",
            {
                "slug": "risk",
                "display_name": "Risk",
                "description": "",
                "visibility": "private",
            },
        ),
        ("get_server_library", "risk"),
        ("update_server_library", "risk", {"description": "Updated"}),
        (
            "grant_server_library",
            "risk",
            {
                "grantee_id": "admin2",
                "grantee_type": "user",
                "scopes": ["read"],
            },
        ),
        ("revoke_server_library_grant", "risk", "admin2"),
        (
            "add_server_library_reference",
            "risk",
            {
                "name": "source",
                "from_library": "default",
                "version": "latest",
                "alias": "source_alias",
            },
        ),
        (
            "copy_server_library_object",
            "risk",
            {
                "name": "source",
                "from_library": "default",
                "version": "latest",
                "new_name": "source_copy",
            },
        ),
        ("remove_server_library_entry", "risk", "source"),
    ]


def test_spl_client_server_property_uses_user_token_and_connection_url() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    client.connect_server(
        machine_token="machine-token",
        user_token="user-token",
        server_url="https://server.example/api",
    )

    server = client.server

    assert isinstance(server, SPLServerClient)
    assert server.token == "user-token"
    assert server.base_url == "https://server.example/api"


def test_spl_client_server_property_requires_user_token() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    with pytest.raises(RuntimeError, match="requires a user token"):
        _ = client.server


def test_spl_client_library_management_requires_server_connection() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    with pytest.raises(RuntimeError, match="server-connected SPLClient"):
        client.library.create("risk")

    with pytest.raises(RuntimeError, match="server-connected SPLClient"):
        client.library.add_reference("risk", "source")

    assert fake_daemon.library_calls == []


def test_spl_client_local_cleanup_does_not_require_server_connection() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    assert client.forget("demo_obj", owner="owner-1", library="risk") == {
        "name": "demo_obj",
        "forgotten": True,
    }
    assert client.remove_local("scratch_obj") == {
        "name": "scratch_obj",
        "forgotten": True,
    }
    assert client.forget_version("demo_obj", 2, library="risk") == {
        "name": "demo_obj",
        "version": 2,
        "forgotten": True,
    }
    assert client.prune_stale_mirrors(owner="owner-1") == {
        "count": 0,
        "pruned": [],
    }

    assert fake_daemon.cleanup_calls == [
        ("forget", "demo_obj", "owner-1", "risk"),
        ("forget", "scratch_obj", None, None),
        ("forget_version", "demo_obj", 2, None, "risk"),
        ("prune_stale_mirrors", "owner-1", None),
    ]
    assert fake_daemon.library_calls == []


def test_spl_client_publish_passes_library_to_daemon(monkeypatch) -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon
    monkeypatch.setattr(
        spl_client_module,
        "export_object_to_yaml",
        lambda obj, entrypoint, *, frame_offset: ("objects: []\n", "fraud_score"),
    )

    published = client.publish(
        object(),
        name="risk_score",
        library="risk",
        create=True,
        library_display_name="Risk",
        local_only=True,
    )
    yaml_published = client.publish_yaml(
        "objects: []\n",
        name="risk_yaml",
        entrypoint="risk_yaml",
        library="risk",
        create=True,
        library_display_name="Risk",
    )

    assert published.name == "risk_score"
    assert yaml_published.name == "risk_yaml"
    assert fake_daemon.register_object_calls == [
        {
            "name": "risk_score",
            "entrypoint": "fraud_score",
            "env": "default",
            "yaml_text": "objects: []\n",
            "workdir": None,
            "runtime_config": None,
            "library": "risk",
            "create_library": True,
            "library_display_name": "Risk",
            "local_only": True,
        },
        {
            "name": "risk_yaml",
            "entrypoint": "risk_yaml",
            "env": "default",
            "yaml_text": "objects: []\n",
            "workdir": None,
            "runtime_config": None,
            "library": "risk",
            "create_library": True,
            "library_display_name": "Risk",
            "local_only": False,
        },
    ]


def test_spl_client_objects_explicit_local_remains_local_when_connected() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    objects = client.objects(scope="local", compact=True)

    assert objects == {"local_obj": {"name": "local_obj", "compact": True}}
    assert fake_daemon.list_object_calls[-1] == {"compact": True}
    assert fake_daemon.server_object_calls == []


def test_spl_client_offline_server_helpers_return_empty_states() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = MissingServerConnectionErrorDaemon()
    client._daemon = fake_daemon

    assert client.current_server_connection() == {
        "connected": False,
        "offline": False,
        "connection": None,
    }
    assert client.machines() == {"current_machine_id": None, "machines": []}
    assert client.libraries(include_accessible=False) == []
    assert client.objects(scope="server", compact=True) == []
    assert client.objects(scope="all", compact=True) == {
        "local": {"local_obj": {"name": "local_obj", "compact": True}},
        "server": [],
    }

    assert fake_daemon.library_calls == []
    assert fake_daemon.server_object_calls == []


def test_spl_client_offline_server_helpers_reraise_other_failures() -> None:
    client = SPLClient(daemon_port=8765)
    client._daemon = FailingServerDaemon()

    with pytest.raises(ClientError, match="503: upstream unavailable"):
        client.libraries()

    client._daemon = BrokenConnectionStateDaemon()
    with pytest.raises(ClientError, match="503: connection state unavailable"):
        client.machines()


# ``run_node()``/``run_node_result()`` were removed in 0.2.0 (WP-07b); the
# removal is pinned in ``test_deprecations.py``.  The remote-node bridge that
# ``Deployment`` uses internally keeps its payload contract covered here.


def test_spl_client_remote_node_bridge_payload() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    node = NodeRemote(
        name="demo_traktorist_pipeline::happiness",
        version="latest",
        inputs=[],
        outputs=[],
    )

    value = client._run_node_value(node, {"a": 301}, timeout_seconds=12.5)

    assert value == "Happy"
    assert fake_daemon.remote_node_calls == [
        {
            "node": {
                "uuid": str(node.uuid),
                "url": "",
                "name": "demo_traktorist_pipeline::happiness",
                "version": "latest",
            },
            "kwargs": {"a": 301},
            "timeout_seconds": 12.5,
        }
    ]


def test_spl_client_decomposition_accepts_node_remote() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    node = NodeRemote(
        name="demo_traktorist_pipeline",
        version="latest",
        library="tractors",
        inputs=[],
        outputs=[],
    )

    decomp = client.decomposition(node)

    assert decomp["nodes"][0]["name"] == "demo_traktorist_pipeline"
    assert fake_daemon.remote_decomposition_calls == [
        {
            "uuid": str(node.uuid),
            "url": "",
            "name": "demo_traktorist_pipeline",
            "version": "latest",
            "library": "tractors",
        }
    ]


def test_spl_client_decomposition_accepts_remote_owner_library() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    decomp = client.decomposition(
        "demo_traktorist_pipeline",
        owner="alice",
        library="tractors",
        version=3,
    )

    assert decomp["nodes"][0]["name"] == "demo_traktorist_pipeline"
    assert fake_daemon.remote_decomposition_calls == [
        {
            "name": "demo_traktorist_pipeline",
            "version": 3,
            "owner_id": "alice",
            "library": "tractors",
        }
    ]


def test_spl_client_describe_uses_display_name_for_server_objects() -> None:
    client = SPLClient(daemon_port=8765)
    client._daemon = FakeDaemon()

    description = client.describe("server.remote-score")

    assert description.splitlines()[0] == "pretty_score v7 (function)"
    assert 'client.call("pretty_score"' in description


def test_spl_client_call_and_signature_accept_internal_function() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    client.call("demo_pipeline", function="inner_add", kwargs={"a": 2, "b": 5})
    client.signature("demo_pipeline", function="inner_add")

    assert fake_daemon.run_calls[-1]["object_name"] == "demo_pipeline"
    assert fake_daemon.run_calls[-1]["function"] == "inner_add"
    assert fake_daemon.signature_calls[-1] == {
        "name": "demo_pipeline",
        "version": None,
        "owner_id": None,
        "library": None,
        "function": "inner_add",
    }


def test_spl_client_call_without_remote_selectors_is_local() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    result = client.call("local_score", kwargs={"customer_id": 42})

    assert result.mode == "local"
    assert result.server_side is False
    assert result.value == {"local": True}
    assert fake_daemon.run_calls[-1]["remote"] is None
    assert fake_daemon.run_calls[-1]["target_machine"] is None
    assert fake_daemon.run_calls[-1]["object_owner_id"] is None
    assert fake_daemon.run_calls[-1]["library"] is None


def test_spl_client_call_with_remote_selectors_returns_server_mode() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    result = client.call(
        "fraud_score",
        library="risk",
        target_machine="gpu-a",
        kwargs={"customer_id": 42},
    )

    assert result.mode == "server"
    assert result.server_side is True
    assert result.value == {"score": 0.91}
    assert fake_daemon.run_calls[-1]["remote"] is True


def test_spl_client_draw_pipeline_returns_notebook_widget_for_object() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    widget = client.draw_pipeline("demo_pipeline", version=3, title="Notebook graph")
    rendered = widget._repr_html_()

    assert fake_daemon.object_calls[-1] == {
        "name": "demo_pipeline",
        "version": 3,
        "include_yaml": True,
    }
    assert "Notebook graph" in rendered
    assert "calculate" in rendered
    assert 'data-pipeline-control="fullscreen"' in rendered
    assert "pipeline-graph-shell" in rendered


def test_spl_client_draw_pipeline_returns_notebook_widget_for_node_remote() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon
    node = NodeRemote(
        name="demo_traktorist_pipeline",
        version="latest",
        library="tractors",
        inputs=[],
        outputs=[],
    )

    widget = client.draw_pipeline(node)
    rendered = widget._repr_html_()

    assert fake_daemon.remote_decomposition_calls[-1]["library"] == "tractors"
    assert "Remote Demo Pipeline" in rendered
    assert "demo_traktorist_pipeline" in rendered
    assert "pipeline-graph-shell" in rendered
