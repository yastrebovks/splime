"""Scoped signature lookups resolve locally first (regression for 0.1.4 bug).

In 0.1.4, ``Client.signature(name, library=...)`` skipped the local registry
and asked the central server, so a caller's own object in a non-default
library failed offline with "active server connection is not found".
"""

from __future__ import annotations

from typing import Any

import pytest

from spl.daemon_client import Client, ClientError


def _client() -> Client:
    return Client("http://127.0.0.1:1", api_token="test-token")


def _raise(exc: Exception) -> Any:
    raise exc


def test_scoped_signature_resolves_locally_first(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    paths: list[str] = []

    def fake_json_request(method: str, path: str, payload: Any = None) -> dict[str, Any]:
        paths.append(path)
        return {"name": "dup_metric", "kind": "function"}

    monkeypatch.setattr(client, "_json_request", fake_json_request)
    monkeypatch.setattr(
        client,
        "resolve_remote_signature",
        lambda ref, **_: _raise(AssertionError("local scope must not go remote")),
    )

    signature = client.signature("dup_metric", library="metrics-a")

    assert signature["name"] == "dup_metric"
    assert paths[0].startswith("/objects/dup_metric/signature")
    assert "library=metrics-a" in paths[0]


def test_scoped_signature_offline_keeps_local_error(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    local_error = ClientError("404: object is not registered: dup_metric")
    remote_error = ClientError("404: active server connection is not found")

    monkeypatch.setattr(client, "_json_request", lambda *a, **k: _raise(local_error))
    monkeypatch.setattr(
        client, "resolve_remote_signature", lambda ref, **_: _raise(remote_error),
    )

    with pytest.raises(ClientError, match="is not registered"):
        client.signature("dup_metric", library="metrics-a")


def test_scoped_signature_falls_back_to_server(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    local_error = ClientError("404: object is not registered: partner_score")

    monkeypatch.setattr(client, "_json_request", lambda *a, **k: _raise(local_error))
    monkeypatch.setattr(
        client,
        "resolve_remote_signature",
        lambda ref, **_: {"signature": {"name": "partner_score", "owner_id": ref["owner_id"]}},
    )

    signature = client.signature("partner_score", owner_id="admin2")
    assert signature == {"name": "partner_score", "owner_id": "admin2"}


def test_bare_name_signature_never_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client()
    local_error = ClientError("404: object is not registered: missing_fn")

    monkeypatch.setattr(client, "_json_request", lambda *a, **k: _raise(local_error))
    monkeypatch.setattr(
        client,
        "resolve_remote_signature",
        lambda ref, **_: _raise(AssertionError("bare name must not go remote")),
    )

    with pytest.raises(ClientError, match="is not registered"):
        client.signature("missing_fn")
