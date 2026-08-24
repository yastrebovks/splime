from __future__ import annotations

import inspect
from typing import Any

from spl import SPLClient


class _Daemon:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def run_remote_node(self, payload: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("run_remote_node", (payload,), kwargs))
        return {
            "value": 7,
            "payload": {"artifacts": {}},
            "run": {"id": "remote-1", "status": "succeeded"},
        }


def _client() -> SPLClient:
    client = object.__new__(SPLClient)
    client._daemon = _Daemon()  # type: ignore[attr-defined]
    client.server_connection = None
    client._user_token = None  # type: ignore[attr-defined]
    return client


def test_historical_facade_signatures_remain_source_compatible() -> None:
    signatures = {
        name: str(inspect.signature(getattr(SPLClient, name)))
        for name in (
            "start",
            "queue",
            "local_objects",
            "server_objects",
            "create_library",
            "get_library",
            "update_library",
            "delete_library",
            "grant_library",
            "revoke_library_grant",
            "add_reference",
            "copy_object",
            "remove_entry",
            "run_node",
            "run_node_result",
        )
    }
    assert signatures["queue"].startswith("(self, name: 'str', *")
    assert "target_machine: 'str'" in signatures["queue"]
    assert signatures["local_objects"] == "(self, *, compact: 'bool' = False) -> 'list[dict[str, Any]]'"
    assert signatures["run_node"] == (
        "(self, node: 'Any', kwargs: 'dict[str, Any]', *, timeout_seconds: 'float | None' = None) -> 'Any'"
    )


def test_historical_run_and_listing_aliases_delegate(monkeypatch) -> None:
    client = _client()
    submitted: list[dict[str, Any]] = []

    def submit(name: str, **kwargs: Any) -> str:
        submitted.append({"name": name, **kwargs})
        return "run"

    monkeypatch.setattr(client, "submit", submit)
    monkeypatch.setattr(
        client,
        "objects",
        lambda **kwargs: {"one": {"name": "one"}} if kwargs["scope"] == "local" else [{"name": "server"}],
    )

    assert client.start("demo", kwargs={"x": 1}) == "run"
    assert client.start("versioned", version=3) == "run"
    assert client.queue("demo", target_machine="m1") == "run"
    assert submitted[0] == {"name": "demo", "kwargs": {"x": 1}}
    assert submitted[1] == {"name": "versioned", "version": 3}
    assert submitted[2]["offline_policy"] == "queue"
    assert client.local_objects() == [{"name": "one"}]
    assert client.server_objects() == [{"name": "server"}]


def test_historical_library_and_node_aliases_delegate(monkeypatch) -> None:
    client = _client()
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    class Library:
        def __getattr__(self, name: str) -> Any:
            def call(*args: Any, **kwargs: Any) -> dict[str, Any]:
                calls.append((name, args, kwargs))
                return {"operation": name}

            return call

    monkeypatch.setattr(SPLClient, "library", property(lambda _self: Library()))
    assert client.create_library("tools")["operation"] == "create"
    assert client.get_library("tools")["operation"] == "get"
    assert client.update_library("tools", description="updated")["operation"] == "update"
    assert client.delete_library("tools")["operation"] == "delete"
    assert client.grant_library("tools", "alice")["operation"] == "grant"
    assert client.revoke_library_grant("tools", "alice")["operation"] == "revoke"
    assert client.add_reference("tools", "resize")["operation"] == "add_reference"
    assert client.copy_object("resize", into_library="tools")["operation"] == "copy_object"
    assert client.remove_entry("tools", "resize")["operation"] == "remove_entry"

    class Node:
        uuid = "node-1"
        url = "https://example.invalid"
        name = "remote"
        version = "latest"

    assert client.run_node(Node(), {"x": 1}) == 7
    result = client.run_node_result(Node(), kwargs={"x": 1})
    assert result.value == 7
    assert result.mode == "server"
    assert calls[0][0] == "create"
