from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

import pytest

# The implementation module is patched directly: monkeypatching the
# ``spl.client`` shim would not affect lookups inside ``spl._client``.
import spl._client as spl_client_module
from spl._client import SPLClient
from spl.core import manifest as m_manifest
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


def test_daemon_client_scoped_signature_404_is_not_retried_under_an_owner() -> None:
    class ScopedMissingClient(RecordingClient):
        def _json_request(
            self,
            method: str,
            path: str,
            payload: dict[str, Any] | None = None,
        ) -> Any:
            self.requests.append((method, path, payload))
            raise ClientError(
                "404: server D1 found no accessible object: foreign_score",
                status_code=404,
            )

    client = ScopedMissingClient()

    with pytest.raises(ClientError) as exc_info:
        client.signature("foreign_score", library="shared")

    assert str(exc_info.value) == ("404: server D1 found no accessible object: foreign_score")
    assert client.requests == [("GET", "/objects/foreign_score/signature?library=shared", None)]


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


def test_daemon_and_sdk_sync_operator_surfaces_preserve_arguments() -> None:
    daemon_client = RecordingClient()

    daemon_client.sync_status()
    assert daemon_client.requests[-1] == ("GET", "/server/sync/status", None)
    daemon_client.prune_sync_events(
        status="failed",
        older_than_days=7,
        include_protected=True,
        limit=25,
    )
    assert daemon_client.requests[-1] == (
        "POST",
        "/server/sync/prune?status=failed&older_than_days=7&include_protected=1&limit=25",
        None,
    )

    class OperatorDaemon:
        def sync_status(self) -> dict[str, Any]:
            return {"by_status": {"pending": 3}, "heartbeat": {"thread_alive": True}}

        def prune_sync_events(self, **kwargs: Any) -> dict[str, Any]:
            return kwargs

    client = SPLClient(daemon_port=8765)
    client._daemon = OperatorDaemon()
    assert client.sync_status()["by_status"] == {"pending": 3}
    assert client.prune_sync_events(status="pending", older_than_days=2) == {
        "status": "pending",
        "older_than_days": 2,
        "include_protected": False,
        "limit": 1_000,
    }


def test_daemon_client_run_sends_keep_when_requested() -> None:
    client = RecordingClient()

    client.run("fraud_score", keep=True)

    method, path, payload = client.requests[-1]
    assert method == "POST"
    assert path == "/runs"
    assert payload == {"object": "fraud_score", "source": "auto", "keep": True}


def test_daemon_client_run_sends_runtime_overrides_when_requested() -> None:
    client = RecordingClient()

    client.run("fraud_score", runtimes={"heavy": "venv-subprocess"})

    method, path, payload = client.requests[-1]
    assert method == "POST"
    assert path == "/runs"
    assert payload == {
        "object": "fraud_score",
        "source": "auto",
        "runtimes": {"heavy": "venv-subprocess"},
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
    client.show_run("run 1")
    client.show_run("run 1", full_inline=True)
    client.prune_runs(statuses=["failed"], older_than_seconds=10, dry_run=True)
    client.delete_run("run 1")

    assert client.requests[-8:] == [
        ("DELETE", "/objects/demo%20obj?owner_id=owner+1&library=risk+team", None),
        ("DELETE", "/objects/demo_obj", None),
        ("DELETE", "/objects/demo_obj/versions/2?library=research", None),
        ("POST", "/objects/prune-stale-mirrors?owner_id=owner-1", None),
        ("GET", "/runs/run%201?view=show&full_inline=0", None),
        ("GET", "/runs/run%201?view=show&full_inline=1", None),
        ("POST", "/runs/prune", {"dry_run": True, "statuses": ["failed"], "older_than_seconds": 10}),
        ("DELETE", "/runs/run%201", None),
    ]


def test_daemon_client_dry_run_booleans_use_canonical_wire_values() -> None:
    client = RecordingClient()

    client.prune_runs(dry_run=True)
    client.prune_runs(dry_run=False)
    client.delete_run("run 1", dry_run=True)
    client.delete_run("run 1", dry_run=False)
    client.prune_server_connections(older_than_days=7, dry_run=True)
    client.prune_server_connections(older_than_days=7, dry_run=False)

    assert client.requests[-6:] == [
        ("POST", "/runs/prune", {"dry_run": True}),
        ("POST", "/runs/prune", {"dry_run": False}),
        ("DELETE", "/runs/run%201?dry_run=1", None),
        ("DELETE", "/runs/run%201", None),
        (
            "POST",
            "/server/connections/prune?older_than_days=7&dry_run=1",
            None,
        ),
        (
            "POST",
            "/server/connections/prune?older_than_days=7&dry_run=0",
            None,
        ),
    ]


def test_daemon_client_pull_server_object_sends_ref_payload() -> None:
    client = RecordingClient()

    client.pull_server_object(
        "demo_obj",
        owner_id="owner-1",
        library="risk",
        version=3,
        all_versions=True,
    )

    assert client.requests[-1] == (
        "POST",
        "/server-objects/pull",
        {
            "name": "demo_obj",
            "all_versions": True,
            "owner_id": "owner-1",
            "library": "risk",
            "version": 3,
        },
    )


def test_daemon_client_pull_all_batches_catalog_once_and_aggregates_receipt() -> None:
    class PullAllRecordingClient(Client):
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
            if method == "GET" and path == "/server/objects?owner=owner-1&library=risk&view=summary":
                return [
                    {
                        "name": "score_a",
                        "owner_id": "owner-1",
                        "library": "risk",
                        "current_version": {"version": 2},
                    },
                    {
                        "name": "score_b",
                        "owner_id": "owner-1",
                        "library": "risk",
                        "current_version": {"version": 3},
                    },
                ]
            if method == "POST" and path == "/server-objects/pull":
                assert payload is not None
                if payload["name"] == "score_a":
                    return {
                        "pulled": ["owner-1/risk/score_a@v2"],
                        "skipped": [],
                        "failed": [],
                        "ambiguous_names": [],
                    }
                return {
                    "pulled": [],
                    "skipped": ["owner-1/risk/score_b@v3"],
                    "failed": [],
                    "ambiguous_names": [],
                }
            if method == "GET" and path == "/objects?view=summary":
                return {
                    "score_a": {"name": "score_a", "canonical_name": "owner-1/risk/score_a"},
                    "score_b": {"name": "score_b", "canonical_name": "owner-1/risk/score_b"},
                }
            raise AssertionError((method, path, payload))

    client = PullAllRecordingClient()

    receipt = client.pull_all_server_objects(
        owner_id="owner-1",
        library="risk",
        all_versions=True,
        progress=False,
    )

    assert receipt == {
        "objects_seen": 2,
        "pulled": ["owner-1/risk/score_a@v2"],
        "skipped": ["owner-1/risk/score_b@v3"],
        "failed": [],
        "ambiguous_names": [],
    }
    assert client.requests == [
        ("GET", "/server/objects?owner=owner-1&library=risk&view=summary", None),
        (
            "POST",
            "/server-objects/pull",
            {
                "name": "score_a",
                "all_versions": True,
                "owner_id": "owner-1",
                "library": "risk",
            },
        ),
        (
            "POST",
            "/server-objects/pull",
            {
                "name": "score_b",
                "all_versions": True,
                "owner_id": "owner-1",
                "library": "risk",
            },
        ),
        ("GET", "/objects?view=summary", None),
    ]


def test_daemon_client_pull_all_dry_run_projects_ambiguity_without_final_local_read() -> None:
    class DryRunRecordingClient(Client):
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
            if method == "GET" and path == "/server/objects?library=risk&view=summary":
                return [
                    {
                        "name": "demo_obj",
                        "owner_id": "owner-1",
                        "library": "risk",
                        "current_version": {"version": 4},
                    }
                ]
            if method == "GET" and path == "/objects?view=summary":
                return {"demo_obj": {"name": "demo_obj", "canonical_name": "owner-1/default/demo_obj"}}
            if method == "POST" and path == "/server-objects/pull":
                return {
                    "pulled": ["owner-1/risk/demo_obj@v4"],
                    "skipped": [],
                    "failed": [],
                    "ambiguous_names": [],
                }
            raise AssertionError((method, path, payload))

    client = DryRunRecordingClient()

    receipt = client.pull_all_server_objects(library="risk", dry_run=True, progress=False)

    assert receipt == {
        "objects_seen": 1,
        "pulled": ["owner-1/risk/demo_obj@v4"],
        "skipped": [],
        "failed": [],
        "ambiguous_names": ["demo_obj"],
    }
    assert client.requests == [
        ("GET", "/server/objects?library=risk&view=summary", None),
        ("GET", "/objects?view=summary", None),
        (
            "POST",
            "/server-objects/pull",
            {
                "name": "demo_obj",
                "all_versions": False,
                "owner_id": "owner-1",
                "library": "risk",
                "dry_run": True,
            },
        ),
    ]


def test_daemon_client_pull_all_large_catalog_uses_compact_listing_and_per_object_pulls() -> None:
    class LargeCatalogClient(Client):
        def __init__(self) -> None:
            super().__init__("http://daemon.local")
            self.requests: list[tuple[str, str, dict[str, Any] | None]] = []
            self.catalog = [
                {
                    "name": f"obj_{index}",
                    "owner_id": "owner-1",
                    "library": "default",
                    "current_version": {"version": 1},
                }
                for index in range(500)
            ]

        def _json_request(
            self,
            method: str,
            path: str,
            payload: dict[str, Any] | None = None,
        ) -> Any:
            self.requests.append((method, path, payload))
            if method == "GET" and path == "/server/objects?view=summary":
                assert all("yaml" not in record for record in self.catalog)
                return self.catalog
            if method == "POST" and path == "/server-objects/pull":
                assert payload is not None
                assert "yaml" not in payload
                return {
                    "pulled": [f"owner-1/default/{payload['name']}@v1"],
                    "skipped": [],
                    "failed": [],
                    "ambiguous_names": [],
                }
            if method == "GET" and path == "/objects?view=summary":
                return {
                    record["name"]: {
                        "name": record["name"],
                        "canonical_name": f"owner-1/default/{record['name']}",
                    }
                    for record in self.catalog
                }
            raise AssertionError((method, path, payload))

    client = LargeCatalogClient()

    receipt = client.pull_all_server_objects(progress=False)

    assert receipt["objects_seen"] == 500
    assert len(receipt["pulled"]) == 500
    assert len([request for request in client.requests if request[1] == "/server/objects?view=summary"]) == 1
    assert len([request for request in client.requests if request[1] == "/server-objects/pull"]) == 500


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
        self.input_calls: list[dict[str, Any]] = []
        self.output_calls: list[dict[str, Any]] = []
        self.version_calls: list[dict[str, Any]] = []
        self.library_calls: list[tuple[Any, ...]] = []
        self.user_calls: list[str | None] = []
        self.register_object_calls: list[dict[str, Any]] = []
        self.cleanup_calls: list[tuple[Any, ...]] = []
        self.pull_calls: list[dict[str, Any]] = []
        self.server_connected = False
        self.server_url = "https://splime.io/api"
        self.missing_local_objects: set[str] = set()
        self.missing_runs: set[str] = set()
        self.server_object_records: list[dict[str, Any]] | None = None
        self.server_library_records: list[dict[str, Any]] | None = None
        self.server_user_records: list[dict[str, Any]] = [
            {
                "id": "owner-1",
                "handle": "alice",
                "display_name": "Alice",
                "status": "active",
            }
        ]
        self.whoami_record: dict[str, Any] = {
            "id": "owner-1",
            "owner_id": "owner-1",
            "handle": "alice",
            "display_name": "Alice",
            "server_url": self.server_url,
            "machine_id": "machine-1",
            "connection_status": "connected",
            "live": True,
        }

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

    def resume_run(self, run_id: str, **kwargs: Any) -> dict[str, Any]:
        self.run_calls.append({"resume_run_id": run_id, **kwargs})
        return {"id": "resume-run-1", "status": "queued"}

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

    def show_run(self, run_id: str, *, full_inline: bool = False) -> dict[str, Any]:
        self.cleanup_calls.append(("show_run", run_id, full_inline))
        if run_id in self.missing_runs:
            raise ClientError("404: run is not found: {}".format(run_id))
        return {"id": run_id, "status": "succeeded", "manifest": {"full_inline": full_inline}}

    def list_runs(self) -> list[dict[str, Any]]:
        return [{"id": "run-1", "status": "failed", "keep": "on_failure", "has_manifest": True}]

    def prune_runs(
        self,
        *,
        run_id: str | None = None,
        statuses: list[str] | None = None,
        older_than_seconds: float | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        self.cleanup_calls.append(("prune_runs", run_id, statuses, older_than_seconds, dry_run))
        return {"count": 1, "pruned": [{"id": run_id or "run-1"}], "dry_run": dry_run}

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
        if name in self.missing_local_objects and kwargs.get("owner_id") is None and kwargs.get("library") is None:
            raise ClientError("404: object is not registered: {}".format(name))
        display_name = "pretty_score" if name == "server.remote-score" else name
        return {
            "name": name,
            "display_name": display_name,
            "version": 7,
            "kind": "function",
            "description": "",
            "inputs": [{"name": "amount", "type": "float", "required": True, "default": None}],
            "outputs": [{"name": "default", "selector": None, "read": "result.value"}],
            "call": {
                "example": f'result = client.call("{display_name}", kwargs={{}})',
                "read": "result.value",
            },
        }

    def inputs(self, name: str, **kwargs: Any) -> list[dict[str, Any]]:
        self.input_calls.append({"name": name, **kwargs})
        if name in self.missing_local_objects and kwargs.get("owner_id") is None and kwargs.get("library") is None:
            raise ClientError("404: object is not registered: {}".format(name))
        return self.signature(name, **kwargs)["inputs"]

    def outputs(self, name: str, **kwargs: Any) -> list[dict[str, Any]]:
        self.output_calls.append({"name": name, **kwargs})
        if name in self.missing_local_objects and kwargs.get("owner_id") is None and kwargs.get("library") is None:
            raise ClientError("404: object is not registered: {}".format(name))
        return self.signature(name, **kwargs)["outputs"]

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
        if name in self.missing_local_objects:
            raise ClientError("404: object is not registered: {}".format(name))
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
        if self.server_object_records is not None:
            return list(self.server_object_records)
        return [
            {
                "name": "server_obj",
                "owner_id": owner_id or "owner-1",
                "library": library or "default",
                "compact": compact,
            }
        ]

    def server_users(self, *, handle: str | None = None) -> list[dict[str, Any]]:
        self.user_calls.append(handle)
        if handle is None:
            return list(self.server_user_records)
        normalized = handle.removeprefix("@").casefold()
        return [row for row in self.server_user_records if str(row.get("handle") or "").casefold() == normalized]

    def server_whoami(self) -> dict[str, Any]:
        return dict(self.whoami_record)

    def server_libraries(
        self,
        *,
        owner: str | None = None,
        include_accessible: bool = True,
    ) -> list[dict[str, Any]]:
        if owner is None:
            self.library_calls.append(("server_libraries", include_accessible))
        else:
            self.library_calls.append(("server_libraries", owner, include_accessible))
        if self.server_library_records is not None:
            return list(self.server_library_records)
        return [{"slug": "risk", "accessible": include_accessible}]

    def create_server_library(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.library_calls.append(("create_server_library", payload))
        return {"slug": payload["slug"], "created": True}

    def get_server_library(
        self,
        library_ref: str,
        *,
        owner: str | None = None,
    ) -> dict[str, Any]:
        if owner is None:
            self.library_calls.append(("get_server_library", library_ref))
        else:
            self.library_calls.append(("get_server_library", library_ref, owner))
        return {"slug": library_ref, **({"owner_id": owner} if owner is not None else {})}

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

    def server_library_grants(
        self,
        library_ref: str,
        *,
        owner: str | None = None,
    ) -> list[dict[str, Any]]:
        if owner is None:
            self.library_calls.append(("server_library_grants", library_ref))
        else:
            self.library_calls.append(("server_library_grants", library_ref, owner))
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

    def object_versions(
        self,
        name: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        self.version_calls.append({"name": name, "owner_id": owner_id, "library": library})
        return [{"version": 1, "owner_id": owner_id, "library": library}]

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

    def pull_server_object(self, name: str, **kwargs: Any) -> dict[str, Any]:
        self.pull_calls.append({"name": name, **kwargs})
        return {
            "pulled": ["owner-1/default/{}@v7".format(name)],
            "skipped": [],
            "failed": [],
            "ambiguous_names": [],
        }

    def pull_all_server_objects(self, **kwargs: Any) -> dict[str, Any]:
        self.pull_calls.append({"name": "__all__", **kwargs})
        return {
            "objects_seen": 2,
            "pulled": ["owner-1/default/clean_amount@v7"],
            "skipped": ["owner-1/default/existing@v3"],
            "failed": [],
            "ambiguous_names": [],
        }

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


def _central_server_unreachable_error(
    message: str = "central SPL daemon server is not reachable at https://splime.io/api: connection refused",
) -> ClientError:
    return ClientError(
        "502: {}".format(message),
        status_code=502,
        payload={
            "error": message,
            "code": "central_server_unreachable",
            "offline": True,
        },
    )


class ServerUnreachableDaemon(MissingServerConnectionDaemon):
    def server_connection(self) -> dict[str, Any]:
        return {
            "connected": False,
            "offline": True,
            "connection": {"id": "conn-1", "status": "connected"},
            "code": "central_server_unreachable",
            "error": "central SPL daemon server is not reachable",
        }

    def server_machines(self) -> dict[str, Any]:
        raise _central_server_unreachable_error()

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
        raise _central_server_unreachable_error()

    def server_libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        self.library_calls.append(("server_libraries", include_accessible))
        raise _central_server_unreachable_error()


class LegacyServerUnreachableDaemon(FakeDaemon):
    def __init__(self) -> None:
        super().__init__()
        self.server_connected = True

    def server_machines(self) -> dict[str, Any]:
        raise ClientError("502: central SPL server is not reachable at https://splime.io/api")

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
        raise ClientError("502: central SPL daemon server is not reachable at https://splime.io/api")

    def server_libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        self.library_calls.append(("server_libraries", include_accessible))
        raise ClientError("502: central SPL daemon server is not reachable at https://splime.io/api")


class FailingServerDaemon(MissingServerConnectionDaemon):
    def server_connection(self) -> dict[str, Any]:
        return {"connected": True, "offline": False, "connection": {"id": "conn-1"}}

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
        raise ClientError("503: upstream unavailable")

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


def test_spl_client_owner_slots_and_versions_forward_handles_without_lookup() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    client.signature("fraud_score", owner="@Alice", library="risk")
    versions = client.versions("fraud_score", owner="@Alice", library="risk")

    assert fake_daemon.signature_calls[-1]["owner_id"] == "@Alice"
    assert versions == [{"version": 1, "owner_id": "@Alice", "library": "risk"}]
    assert fake_daemon.version_calls == [{"name": "fraud_score", "owner_id": "@Alice", "library": "risk"}]


def test_spl_client_owner_normalizer_rejects_embedded_marker_before_daemon_call() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    with pytest.raises(ValueError, match="one leading @handle"):
        client.signature("fraud_score", owner="alice@example")

    assert fake_daemon.signature_calls == []


def test_sdk_owner_slot_docstrings_document_handle_inputs() -> None:
    client = SPLClient(daemon_port=8765)
    owner_surfaces = [
        client.objects,
        client.forget,
        client.remove_local,
        client.forget_version,
        client.prune_stale_mirrors,
        client.pull,
        client.pull_all,
        client.signature,
        client.inputs,
        client.outputs,
        client.versions,
        client.decomposition,
        client.describe,
        client.submit,
        client.call,
        client.library.list,
        client.library.get,
        client.library.grants,
        client.library.grant,
        client.library.revoke,
        client.library.add_reference,
        client.library.copy_object,
        NodeRemote.locate,
    ]

    for surface in owner_surfaces:
        assert "@handle" in (inspect.getdoc(surface) or ""), surface


def _server_record(name: str, *, owner_id: str, library: str, version: int) -> dict[str, Any]:
    return {
        "name": name,
        "owner_id": owner_id,
        "library": library,
        "current_version": {"version": version},
    }


_LOCAL_RUN_ID = "20260710T215643Z-b82829775344"
_DAEMON_RUN_ID = "b82829775344b82829775344b8282977"


def _write_local_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, run_id: str = _LOCAL_RUN_ID) -> None:
    runs_home = tmp_path / "runs"
    monkeypatch.setenv("SPL_RUNS_HOME", str(runs_home))
    m_manifest.atomic_write_json(
        runs_home / run_id / m_manifest.RUN_MANIFEST_FILENAME,
        {
            "schema_version": m_manifest.RUN_MANIFEST_SCHEMA_VERSION,
            "run_id": run_id,
            "status": "succeeded",
            "keep": True,
            "created_at": "2026-07-10T21:56:43+00:00",
            "started_at": "2026-07-10T21:56:43+00:00",
            "finished_at": "2026-07-10T21:56:44+00:00",
            "nodes": {},
            "edges": [],
        },
    )


@pytest.mark.parametrize(
    ("run_id", "namespace"),
    [
        (_LOCAL_RUN_ID, "local"),
        (_DAEMON_RUN_ID, "daemon"),
        ("b82829775344", "unknown"),
        ("run-1", "unknown"),
    ],
)
def test_run_id_namespace_classifies_syntax(run_id: str, namespace: str) -> None:
    assert spl_client_module._run_id_namespace(run_id) == namespace


def _assert_g01_offline_bare_name_hint(message: str, name: str) -> None:
    # G-01 in Release/plan-0.4.3.md intentionally replaces the raw local
    # "object is not registered" 404 with an actionable offline bare-name hint.
    assert "404:" in message
    assert "{!r} is not registered locally".format(name) in message
    assert "no server connection" in message
    assert "connect_server" in message
    assert "pull" in message


def _assert_j02_unreachable_bare_name_hint(message: str, name: str) -> None:
    assert "404:" in message
    assert "{!r} is not registered locally".format(name) in message
    assert "server unreachable" in message
    assert "client.pull" in message
    assert "when online" in message


def test_spl_client_detects_server_unreachable_marker_and_legacy_texts() -> None:
    assert spl_client_module._is_server_unreachable(_central_server_unreachable_error())
    assert spl_client_module._is_server_unreachable(
        ClientError("502: central SPL daemon server is not reachable at https://splime.io/api")
    )
    assert spl_client_module._is_server_unreachable(
        ClientError("502: central SPL server is not reachable at https://splime.io/api")
    )
    assert not spl_client_module._is_server_unreachable(ClientError("503: connection state unavailable"))


def test_spl_client_bare_signature_offline_reports_g01_hint() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.missing_local_objects = {"clean_amount"}
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.signature("clean_amount")

    _assert_g01_offline_bare_name_hint(str(exc_info.value), "clean_amount")
    assert fake_daemon.server_object_calls == []


def test_spl_client_bare_signature_unreachable_reports_pull_hint() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = ServerUnreachableDaemon()
    fake_daemon.missing_local_objects = {"clean_amount"}
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.signature("clean_amount")

    _assert_j02_unreachable_bare_name_hint(str(exc_info.value), "clean_amount")
    assert fake_daemon.server_object_calls == []


def test_spl_client_bare_signature_legacy_unreachable_reports_pull_hint() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = LegacyServerUnreachableDaemon()
    fake_daemon.missing_local_objects = {"clean_amount"}
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.signature("clean_amount")

    _assert_j02_unreachable_bare_name_hint(str(exc_info.value), "clean_amount")
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]


def test_spl_client_bare_signature_auto_resolves_unique_server_catalog_match() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.missing_local_objects = {"clean_amount"}
    fake_daemon.server_object_records = [
        _server_record("clean_amount", owner_id="owner-1", library="default", version=10)
    ]
    client._daemon = fake_daemon

    signature = client.signature("clean_amount")

    assert signature["name"] == "clean_amount"
    assert signature["resolved_from_server"] == {
        "name": "clean_amount",
        "library": "default",
        "owner_id": "owner-1",
    }
    assert "resolved from server" in repr(signature)
    assert "library 'default'" in repr(signature)
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]
    assert fake_daemon.signature_calls == [
        {
            "name": "clean_amount",
            "version": None,
            "owner_id": None,
            "library": None,
            "function": None,
        },
        {
            "name": "clean_amount",
            "version": None,
            "owner_id": "owner-1",
            "library": "default",
            "function": None,
        },
    ]


def test_spl_client_bare_signature_reports_ambiguous_server_catalog_matches() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.missing_local_objects = {"order_pipeline"}
    fake_daemon.server_object_records = [
        _server_record("order_pipeline", owner_id="owner-1", library="default", version=10),
        _server_record("order_pipeline", owner_id="owner-1", library="risk", version=1),
    ]
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.signature("order_pipeline")

    message = str(exc_info.value)
    assert "'order_pipeline' is not registered locally" in message
    assert "default (owner owner-1, v10)" in message
    assert "risk (owner owner-1, v1)" in message
    assert "client.signature('order_pipeline', library='...')" in message
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]
    assert len(fake_daemon.signature_calls) == 1


def test_spl_client_bare_signature_reports_no_accessible_server_match() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.missing_local_objects = {"clean_amount"}
    fake_daemon.server_object_records = [_server_record("other", owner_id="owner-1", library="default", version=3)]
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.signature("clean_amount")

    message = str(exc_info.value)
    assert "not registered locally" in message
    assert "no accessible server object named 'clean_amount'" in message
    assert "connected as" in message
    assert "client.objects(scope='server')" in message
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]


def test_spl_client_pull_auto_resolves_bare_server_catalog_match() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.server_object_records = [
        _server_record("clean_amount", owner_id="owner-1", library="default", version=10)
    ]
    client._daemon = fake_daemon

    receipt = client.pull("clean_amount", all_versions=True)

    assert receipt["pulled"] == ["owner-1/default/clean_amount@v7"]
    assert receipt["skipped"] == []
    assert receipt["failed"] == []
    assert receipt["ambiguous_names"] == []
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]
    assert fake_daemon.pull_calls == [
        {
            "name": "clean_amount",
            "owner_id": "owner-1",
            "library": "default",
            "version": None,
            "all_versions": True,
        }
    ]


def test_spl_client_pull_scoped_uses_direct_daemon_ref() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    client.pull("clean_amount", owner="owner-1", library="risk", version=3)

    assert fake_daemon.server_object_calls == []
    assert fake_daemon.pull_calls == [
        {
            "name": "clean_amount",
            "owner_id": "owner-1",
            "library": "risk",
            "version": 3,
            "all_versions": False,
        }
    ]


def test_spl_client_pull_offline_reports_connection_action() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.pull("clean_amount")

    message = str(exc_info.value)
    assert "pull requires a server connection" in message
    assert "no server connection" in message
    assert "connect_server" in message
    assert "client.pull" in message
    assert fake_daemon.pull_calls == []


def test_spl_client_pull_reports_bare_server_catalog_ambiguity() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.server_object_records = [
        _server_record("order_pipeline", owner_id="owner-1", library="default", version=10),
        _server_record("order_pipeline", owner_id="owner-1", library="risk", version=1),
    ]
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.pull("order_pipeline")

    message = str(exc_info.value)
    assert "default (owner owner-1, v10)" in message
    assert "risk (owner owner-1, v1)" in message
    assert "client.pull('order_pipeline', library='...')" in message
    assert fake_daemon.pull_calls == []


def test_spl_client_pull_all_delegates_to_daemon_batch() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    receipt = client.pull_all(owner="owner-1", library="risk", all_versions=True, dry_run=True)

    assert receipt["objects_seen"] == 2
    assert receipt["pulled"] == ["owner-1/default/clean_amount@v7"]
    assert receipt["skipped"] == ["owner-1/default/existing@v3"]
    assert receipt["ambiguous_names"] == []
    assert fake_daemon.pull_calls == [
        {
            "name": "__all__",
            "owner_id": "owner-1",
            "library": "risk",
            "all_versions": True,
            "dry_run": True,
        }
    ]


def test_spl_client_pull_all_offline_reports_connection_action() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.pull_all()

    message = str(exc_info.value)
    assert "pull_all requires a server connection" in message
    assert "no server connection" in message
    assert "connect_server" in message
    assert "client.pull_all" in message
    assert fake_daemon.pull_calls == []


def test_spl_client_bare_decomposition_auto_resolves_unique_server_catalog_match() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.missing_local_objects = {"order_pipeline"}
    fake_daemon.server_object_records = [
        _server_record("order_pipeline", owner_id="owner-1", library="default", version=10)
    ]
    client._daemon = fake_daemon

    decomposition = client.decomposition("order_pipeline")

    assert decomposition["resolved_from_server"] == {
        "name": "order_pipeline",
        "library": "default",
        "owner_id": "owner-1",
    }
    assert "resolved from server" in repr(decomposition)
    assert fake_daemon.remote_decomposition_calls[-1] == {
        "name": "order_pipeline",
        "version": None,
        "owner_id": "owner-1",
        "library": "default",
    }
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]


def test_spl_client_bare_decomposition_reports_server_catalog_ambiguity() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.missing_local_objects = {"order_pipeline"}
    fake_daemon.server_object_records = [
        _server_record("order_pipeline", owner_id="owner-1", library="default", version=10),
        _server_record("order_pipeline", owner_id="owner-1", library="risk", version=1),
    ]
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.decomposition("order_pipeline")

    message = str(exc_info.value)
    assert "default (owner owner-1, v10)" in message
    assert "risk (owner owner-1, v1)" in message
    assert fake_daemon.remote_decomposition_calls == []


@pytest.mark.parametrize("method_name", ["describe", "inputs", "outputs", "decomposition"])
def test_spl_client_bare_metadata_methods_offline_report_g01_hint(method_name: str) -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.missing_local_objects = {"clean_amount"}
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        getattr(client, method_name)("clean_amount")

    _assert_g01_offline_bare_name_hint(str(exc_info.value), "clean_amount")
    assert fake_daemon.server_object_calls == []


@pytest.mark.parametrize("method_name", ["describe", "inputs", "outputs"])
def test_spl_client_bare_metadata_methods_auto_resolve_unique_server_catalog_match(method_name: str) -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.missing_local_objects = {"clean_amount"}
    fake_daemon.server_object_records = [
        _server_record("clean_amount", owner_id="owner-1", library="default", version=10)
    ]
    client._daemon = fake_daemon

    result = getattr(client, method_name)("clean_amount")

    if method_name == "describe":
        assert "Resolved from server: library 'default', owner owner-1" in result
    elif method_name == "inputs":
        assert result == [{"name": "amount", "type": "float", "required": True, "default": None}]
    else:
        assert result == [{"name": "default", "selector": None, "read": "result.value"}]
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]
    assert fake_daemon.signature_calls[-1] == {
        "name": "clean_amount",
        "version": None,
        "owner_id": "owner-1",
        "library": "default",
        "function": None,
    }


@pytest.mark.parametrize("method_name", ["describe", "inputs", "outputs"])
def test_spl_client_bare_metadata_methods_report_server_catalog_ambiguity(method_name: str) -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.missing_local_objects = {"order_pipeline"}
    fake_daemon.server_object_records = [
        _server_record("order_pipeline", owner_id="owner-1", library="default", version=10),
        _server_record("order_pipeline", owner_id="owner-1", library="risk", version=1),
    ]
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        getattr(client, method_name)("order_pipeline")

    message = str(exc_info.value)
    assert "default (owner owner-1, v10)" in message
    assert "risk (owner owner-1, v1)" in message
    assert "library='...'" in message


@pytest.mark.parametrize("method_name", ["describe", "inputs", "outputs"])
def test_spl_client_bare_metadata_methods_report_server_catalog_miss(method_name: str) -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.missing_local_objects = {"clean_amount"}
    fake_daemon.server_object_records = []
    client._daemon = fake_daemon

    with pytest.raises(ClientError, match="no accessible server object named 'clean_amount'"):
        getattr(client, method_name)("clean_amount")

    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]


def test_spl_client_describe_reports_server_freshness_when_local_is_stale() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.server_object_records = [
        _server_record("fraud_score", owner_id="owner-1", library="default", version=10)
    ]
    client._daemon = fake_daemon

    description = client.describe("fraud_score")

    assert "local v7; server has v10 (library 'default') - run/call resolves via source='auto'" in description
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]


def test_spl_client_describe_omits_freshness_when_versions_match() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.server_object_records = [
        _server_record("fraud_score", owner_id="owner-1", library="default", version=7)
    ]
    client._daemon = fake_daemon

    description = client.describe("fraud_score")

    assert "server has" not in description


def test_spl_client_describe_offline_does_not_probe_server_freshness() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = False
    fake_daemon.server_object_records = [
        _server_record("fraud_score", owner_id="owner-1", library="default", version=10)
    ]
    client._daemon = fake_daemon

    description = client.describe("fraud_score")

    assert "server has" not in description
    assert fake_daemon.server_object_calls == []


def test_spl_client_describe_omits_freshness_when_server_unreachable() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = LegacyServerUnreachableDaemon()
    client._daemon = fake_daemon

    description = client.describe("fraud_score")

    assert "server has" not in description
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]


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


def test_spl_client_rejects_run_adapter_overrides_before_daemon_call() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    with pytest.raises(NotImplementedError, match="local Deployment.run"):
        client.submit("fraud_score", adapters={("producer", "default"): object()})

    with pytest.raises(NotImplementedError, match="local Deployment.run"):
        client.call("fraud_score", adapters={("producer", "default"): object()})

    assert fake_daemon.run_calls == []


def test_spl_client_passes_runtime_overrides_to_daemon() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    client.submit("fraud_score", runtimes={"heavy": "venv-subprocess"})

    assert fake_daemon.run_calls[-1]["runtimes"] == {"heavy": "venv-subprocess"}


def test_spl_client_resume_passes_daemon_resume_options() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon
    adapters = {
        "producer.default": {
            "key": "builtins.str@txt",
            "save": "save_text",
            "load": "good_load_text",
            "distributions": [],
        }
    }

    run = client.resume(
        "parent-run",
        from_="consumer",
        kwargs={"seed": 2},
        output="consumer",
        adapters=adapters,
        runtimes={"consumer": "native"},
        keep=True,
    )

    assert run.id == "resume-run-1"
    assert fake_daemon.run_calls[-1] == {
        "resume_run_id": "parent-run",
        "from_": "consumer",
        "kwargs": {"seed": 2},
        "output": "consumer",
        "timeout_seconds": None,
        "adapters": adapters,
        "runtimes": {"consumer": "native"},
        "keep": True,
    }


def test_spl_client_resume_wait_collects_result() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    result = client.resume("parent-run", from_="consumer", wait=True, progress=False)

    assert result.output is True
    assert fake_daemon.run_calls[-1]["resume_run_id"] == "parent-run"


def test_spl_client_run_show_auto_routes_local_run_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _write_local_run(monkeypatch, tmp_path)
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.missing_runs.add(_LOCAL_RUN_ID)
    client._daemon = fake_daemon

    shown = client.run_show(_LOCAL_RUN_ID)

    assert shown["id"] == _LOCAL_RUN_ID
    assert shown["manifest"]["run_id"] == _LOCAL_RUN_ID
    assert fake_daemon.cleanup_calls == []


def test_spl_client_run_show_explicit_daemon_overrides_local_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _write_local_run(monkeypatch, tmp_path)
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.missing_runs.add(_LOCAL_RUN_ID)
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as excinfo:
        client.run_show(_LOCAL_RUN_ID, local=False)

    message = str(excinfo.value)
    assert "run id namespace: local" in message
    assert "run_show('20260710T215643Z-b82829775344', local=True)" in message
    assert fake_daemon.cleanup_calls == [("show_run", _LOCAL_RUN_ID, False)]


def test_spl_client_run_show_explicit_local_overrides_daemon_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _write_local_run(monkeypatch, tmp_path, run_id=_DAEMON_RUN_ID)
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.missing_runs.add(_DAEMON_RUN_ID)
    client._daemon = fake_daemon

    shown = client.run_show(_DAEMON_RUN_ID, local=True)

    assert shown["id"] == _DAEMON_RUN_ID
    assert fake_daemon.cleanup_calls == []


@pytest.mark.parametrize(
    ("run_id", "namespace", "hint"),
    [
        (_DAEMON_RUN_ID, "daemon", "check that the daemon"),
        ("run-1", "unknown", "runs(local=True)"),
    ],
)
def test_spl_client_run_show_daemon_404_mentions_namespace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    run_id: str,
    namespace: str,
    hint: str,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.missing_runs.add(run_id)
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as excinfo:
        client.run_show(run_id)

    message = str(excinfo.value)
    assert "run id namespace: {}".format(namespace) in message
    assert hint in message


def test_spl_client_resume_rejects_local_run_id_without_daemon_call() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    with pytest.raises(ValueError) as excinfo:
        client.resume(_LOCAL_RUN_ID, from_="consumer")

    message = str(excinfo.value)
    assert "local retained run id" in message
    assert "Deployment(<pipeline>).resume('20260710T215643Z-b82829775344', from_=...)" in message
    assert "client.resume() drives daemon runs only" in message
    assert fake_daemon.run_calls == []


def test_spl_client_runs_footer_mentions_local_retained_runs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "empty-runs"))
    assert "runs(local=True)" not in repr(client.runs())

    _write_local_run(monkeypatch, tmp_path)
    rendered = repr(client.runs())

    assert "+ 1 local retained runs - runs(local=True)" in rendered


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


def test_spl_client_objects_rejects_ambiguous_same_slug_with_sorted_owner_candidates() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.server_library_records = [
        {"slug": "risk", "owner_id": "owner-b", "owner_handle": "bob", "owned": False},
        {"slug": "risk", "owner_id": "owner-a", "owner_handle": "alice", "owned": False},
    ]
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.objects(scope="server", library="risk", compact=True)

    assert str(exc_info.value) == (
        "library 'risk' is ambiguous across accessible owners: @alice/risk, @bob/risk. "
        "Pass owner=... to choose one (for example, owner='@alice')."
    )
    assert fake_daemon.library_calls == [("server_libraries", True)]
    assert fake_daemon.server_object_calls == []


@pytest.mark.parametrize("scope", ["server", "auto", "all"])
def test_spl_client_objects_qualifies_the_sole_foreign_library_owner(scope: str) -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.server_library_records = [
        {"slug": "risk", "owner_id": "owner-a", "owner_handle": "alice", "owned": False}
    ]
    client._daemon = fake_daemon

    client.objects(scope=scope, library="risk", compact=True)

    assert fake_daemon.library_calls == [("server_libraries", True)]
    assert fake_daemon.server_object_calls == [{"owner_id": "owner-a", "library": "risk", "compact": True}]


def test_spl_client_objects_keeps_the_owned_single_library_legacy_call() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.server_library_records = [
        {"slug": "risk", "owner_id": "owner-self", "owner_handle": "self", "owned": True}
    ]
    client._daemon = fake_daemon

    client.objects(scope="server", library="risk", compact=True)

    assert fake_daemon.library_calls == [("server_libraries", True)]
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": "risk", "compact": True}]


def test_spl_client_whoami_online_and_offline_cached_shapes() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    assert client.whoami() == fake_daemon.whoami_record

    fake_daemon.whoami_record = {
        **fake_daemon.whoami_record,
        "handle": None,
        "display_name": "owner-1",
        "connection_status": "offline",
        "live": False,
    }
    assert client.whoami() == fake_daemon.whoami_record


def test_spl_client_whoami_without_identity_surfaces_connect_remediation() -> None:
    class NoIdentityDaemon(FakeDaemon):
        def server_whoami(self) -> dict[str, Any]:
            raise ClientError(
                "404: active server connection is not found; connect first with client.connect_server(...)"
            )

    client = SPLClient(daemon_port=8765)
    client._daemon = NoIdentityDaemon()

    with pytest.raises(ClientError) as exc_info:
        client.whoami()

    assert str(exc_info.value) == (
        "404: active server connection is not found; connect first with client.connect_server(...)"
    )


def test_spl_client_users_lists_email_free_rows_and_forwards_handle_filter() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    client._daemon = fake_daemon

    users = client.users()
    filtered = client.users("@Alice")

    assert users == fake_daemon.server_user_records
    assert filtered == fake_daemon.server_user_records
    assert all("email" not in row for row in users)
    assert fake_daemon.user_calls == [None, "@Alice"]


def test_spl_client_library_owner_reads_forward_handles_without_resolution() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    client.library.list(owner="@Alice", include_accessible=False)
    library = client.library.get("risk", owner="@Alice")
    grants = client.library.grants("risk", owner="@Alice")

    assert library == {"slug": "risk", "owner_id": "@Alice"}
    assert grants == [{"library": "risk", "grantee_id": "admin2"}]
    assert fake_daemon.library_calls == [
        ("server_libraries", "@Alice", False),
        ("get_server_library", "risk", "@Alice"),
        ("server_library_grants", "risk", "@Alice"),
    ]


def test_spl_client_library_write_surfaces_foreign_owner_error_unchanged() -> None:
    denial = ClientError(
        "403: only the library owner can modify it",
        status_code=403,
        payload={"error": "only the library owner can modify it"},
    )

    class ForeignWriteDeniedDaemon(FakeDaemon):
        def update_server_library(
            self,
            library_ref: str,
            payload: dict[str, Any],
        ) -> dict[str, Any]:
            self.library_calls.append(("update_server_library", library_ref, payload))
            raise denial

    client = SPLClient(daemon_port=8765)
    fake_daemon = ForeignWriteDeniedDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.library.update("@alice/risk", description="blocked")

    assert exc_info.value is denial
    assert fake_daemon.library_calls == [("update_server_library", "@alice/risk", {"description": "blocked"})]


def test_spl_client_library_management_methods_use_daemon() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    assert client.libraries(include_accessible=False) == [{"slug": "risk", "accessible": False}]
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
        "warning": "local cache only; server copies (if any) stay visible in objects() while connected",
    }
    assert client.remove_local("scratch_obj") == {
        "name": "scratch_obj",
        "forgotten": True,
        "warning": "local cache only; server copies (if any) stay visible in objects() while connected",
    }
    assert client.forget_version("demo_obj", 2, library="risk") == {
        "name": "demo_obj",
        "version": 2,
        "forgotten": True,
        "warning": "local cache only; server copies (if any) stay visible in objects() while connected",
    }
    assert client.prune_stale_mirrors(owner="owner-1") == {
        "count": 0,
        "pruned": [],
    }
    assert client.run_show("run-1", full_inline=True)["manifest"] == {"full_inline": True}
    assert client.prune_runs(run_id="run-1", status="failed", older_than_seconds=10, dry_run=True) == {
        "count": 1,
        "pruned": [{"id": "run-1"}],
        "dry_run": True,
    }
    assert client.runs()[0]["id"] == "run-1"

    assert fake_daemon.cleanup_calls == [
        ("forget", "demo_obj", "owner-1", "risk"),
        ("forget", "scratch_obj", None, None),
        ("forget_version", "demo_obj", 2, None, "risk"),
        ("prune_stale_mirrors", "owner-1", None),
        ("show_run", "run-1", True),
        ("prune_runs", "run-1", ["failed"], 10, True),
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


def test_spl_client_objects_scope_headers() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    assert repr(client.objects(scope="local", compact=True)).startswith("local objects (1):")
    assert repr(client.objects(scope="server", compact=True)).startswith("server objects (1):")
    catalog_text = repr(client.objects(scope="all", compact=True))

    assert "objects (2 = 1 local + 1 server)" in catalog_text
    assert "local objects (1):" in catalog_text
    assert "server objects (1):" in catalog_text


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


def test_spl_client_dead_channel_server_helpers_raise_instead_of_lying_empty() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = ServerUnreachableDaemon()
    client._daemon = fake_daemon

    assert client.current_server_connection() == {
        "connected": False,
        "offline": True,
        "connection": {"id": "conn-1", "status": "connected"},
        "code": "central_server_unreachable",
        "error": "central SPL daemon server is not reachable",
    }
    with pytest.raises(ClientError, match="not reachable"):
        client.machines()
    with pytest.raises(ClientError, match="not reachable"):
        client.libraries(include_accessible=False)
    with pytest.raises(ClientError, match="not reachable"):
        client.objects(scope="server", compact=True)
    assert client.objects(scope="auto", compact=True) == {
        "local_obj": {"name": "local_obj", "compact": True},
    }
    assert client.objects(scope="all", compact=True) == {
        "local": {"local_obj": {"name": "local_obj", "compact": True}},
        "server": [],
    }

    assert fake_daemon.library_calls == [("server_libraries", False)]
    assert fake_daemon.server_object_calls == [{"owner_id": None, "library": None, "compact": True}]


def test_spl_client_legacy_dead_channel_server_helpers_raise_instead_of_lying_empty() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = LegacyServerUnreachableDaemon()
    client._daemon = fake_daemon

    with pytest.raises(ClientError, match="not reachable"):
        client.machines()
    with pytest.raises(ClientError, match="not reachable"):
        client.libraries(include_accessible=False)
    with pytest.raises(ClientError, match="not reachable"):
        client.objects(scope="server", compact=True)
    assert client.objects(scope="auto", compact=True) == {
        "local_obj": {"name": "local_obj", "compact": True},
    }
    assert client.objects(scope="all", compact=True) == {
        "local": {"local_obj": {"name": "local_obj", "compact": True}},
        "server": [],
    }

    assert fake_daemon.library_calls == [("server_libraries", False)]
    assert fake_daemon.server_object_calls == [
        {"owner_id": None, "library": None, "compact": True},
        {"owner_id": None, "library": None, "compact": True},
        {"owner_id": None, "library": None, "compact": True},
    ]


def test_spl_client_offline_server_helpers_reraise_other_failures() -> None:
    client = SPLClient(daemon_port=8765)
    client._daemon = FailingServerDaemon()

    with pytest.raises(ClientError, match="503: upstream unavailable"):
        client.libraries()

    with pytest.raises(ClientError, match="503: upstream unavailable"):
        client.objects(scope="server", compact=True)

    broken_daemon = BrokenConnectionStateDaemon()
    broken_daemon.missing_local_objects = {"clean_amount"}
    client._daemon = broken_daemon
    with pytest.raises(ClientError, match="503: connection state unavailable"):
        client.machines()
    with pytest.raises(ClientError, match="503: connection state unavailable"):
        client.objects(scope="server", compact=True)
    with pytest.raises(ClientError, match="503: connection state unavailable"):
        client.signature("clean_amount")


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


def test_spl_client_preserves_auto_resolve_field_names_through_run_collection() -> None:
    resolution = {
        "auto_resolved": True,
        "requested_library": "risk",
        "resolved_owner_id": "owner-a",
        "resolved_owner_handle": "alice",
        "resolved_library": "risk",
        "resolved_library_id": "library-a",
    }
    resolved_from = dict(resolution)

    class ResolutionDaemon(FakeDaemon):
        def run(self, object_name: str, **kwargs: Any) -> dict[str, Any]:
            self.run_calls.append({"object_name": object_name, **kwargs})
            return {
                "id": "remote-run-resolution",
                "status": "queued",
                "resolution": resolution,
                "resolved_from": resolved_from,
            }

        def wait_remote_run(
            self,
            run_id: str,
            *,
            poll_interval: float,
            timeout_seconds: float | None,
            on_state: Any | None = None,
        ) -> dict[str, Any]:
            del poll_interval, timeout_seconds
            state = {
                "id": run_id,
                "status": "succeeded",
                "result": {"result": {"score": 0.91}, "artifacts": {}},
            }
            if on_state is not None:
                on_state(state)
            return state

        def signature(self, name: str, **kwargs: Any) -> dict[str, Any]:
            signature = super().signature(name, **kwargs)
            signature["resolved_from"] = resolved_from
            return signature

    client = SPLClient(daemon_port=8765)
    fake_daemon = ResolutionDaemon()
    fake_daemon.server_connected = True
    client._daemon = fake_daemon

    submitted = client.submit("fraud_score", library="risk")
    result = client.call("fraud_score", library="risk", progress=False)
    signature = client.signature("fraud_score", library="risk")

    assert submitted.state["resolution"] == resolution
    assert submitted.state["resolved_from"] == resolved_from
    assert result.run["resolution"] == resolution
    assert result.run["resolved_from"] == resolved_from
    assert signature["resolved_from"] == resolved_from
    assert set(result.run["resolution"]) == {
        "auto_resolved",
        "requested_library",
        "resolved_owner_id",
        "resolved_owner_handle",
        "resolved_library",
        "resolved_library_id",
    }


def test_spl_client_local_scoped_call_surfaces_cross_owner_hint_verbatim() -> None:
    hint = (
        "404: 'score' is registered locally under other owners: owner 'user-a' "
        "(library 'default'), owner 'user-b' (library 'default'); canonical candidates: "
        "user-a/default/score, user-b/default/score; pass owner=/library=, or reconnect under that identity"
    )

    class LocalHintDaemon(FakeDaemon):
        def run(self, object_name: str, **kwargs: Any) -> dict[str, Any]:
            self.run_calls.append({"object_name": object_name, **kwargs})
            raise ClientError(hint)

    client = SPLClient(daemon_port=8765)
    fake_daemon = LocalHintDaemon()
    client._daemon = fake_daemon

    with pytest.raises(ClientError) as exc_info:
        client.call("score", library="default", progress=False)

    assert str(exc_info.value) == hint
    assert fake_daemon.run_calls[-1]["remote"] is None


def test_node_remote_canonicalizes_handle_only_after_signature_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import spl.daemon_client as daemon_client

    calls: list[dict[str, Any]] = []

    class CanonicalSignatureDaemon:
        def resolve_remote_signature(self, ref: dict[str, Any]) -> dict[str, Any]:
            calls.append(ref)
            return {
                "signature": {
                    "inputs": [{"name": "amount", "type": "float"}],
                    "outputs": [{"name": "default", "type": "float"}],
                    "remote": {"owner_id": "owner-a", "library": "risk"},
                }
            }

    monkeypatch.setattr(daemon_client, "Client", CanonicalSignatureDaemon)

    resolved = NodeRemote.locate(name="score", owner="@Alice", library="risk")
    explicit = NodeRemote(
        name="score",
        owner="@Alice",
        library="risk",
        inputs=[],
        outputs=[],
    )

    assert calls == [
        {
            "url": "",
            "name": "score",
            "version": "latest",
            "owner_id": "@Alice",
            "library": "risk",
        }
    ]
    assert resolved.owner_id == "owner-a"
    assert explicit.owner_id == "@Alice"


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


def test_spl_client_draw_pipeline_auto_resolves_bare_server_name() -> None:
    client = SPLClient(daemon_port=8765)
    fake_daemon = FakeDaemon()
    fake_daemon.server_connected = True
    fake_daemon.missing_local_objects = {"server_pipeline"}
    fake_daemon.server_object_records = [
        _server_record("server_pipeline", owner_id="owner-1", library="default", version=10)
    ]
    client._daemon = fake_daemon

    widget = client.draw_pipeline("server_pipeline")
    rendered = widget._repr_html_()

    assert fake_daemon.object_calls[-1] == {
        "name": "server_pipeline",
        "version": None,
        "include_yaml": True,
    }
    assert fake_daemon.remote_decomposition_calls[-1] == {
        "name": "server_pipeline",
        "version": None,
        "owner_id": "owner-1",
        "library": "default",
    }
    assert "Remote Demo Pipeline" in rendered
    assert "server_pipeline" in rendered
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
