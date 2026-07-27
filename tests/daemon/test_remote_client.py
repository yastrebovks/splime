from __future__ import annotations

from io import BytesIO
from typing import Any
from urllib.error import HTTPError

import pytest

from spl.daemon import remote_client
from spl.daemon.remote_client import ServerClient


def test_server_client_library_methods_build_central_paths(monkeypatch) -> None:
    client = ServerClient(
        "https://splime.io/api/",
        "machine-token",
        user_token="user-token",
    )
    calls = []

    def fake_json_request(method, path, payload=None, *, auth="machine"):
        calls.append((method, path, payload, auth))
        return {"ok": True}

    monkeypatch.setattr(client, "_json_request", fake_json_request)

    assert client.list_libraries(include_accessible=False) == {"ok": True}
    assert client.create_library({"slug": "risk"}) == {"ok": True}
    assert client.get_library("risk") == {"ok": True}
    assert client.update_library("risk", {"description": "Updated"}) == {"ok": True}
    with pytest.raises(NotImplementedError, match="not supported"):
        client.delete_library("risk")
    assert client.list_library_grants("risk") == {"ok": True}
    assert client.grant_library("risk", {"grantee_id": "admin2"}) == {"ok": True}
    assert client.revoke_library_grant("risk", "admin2") == {"ok": True}
    assert client.add_library_reference("risk", {"name": "source"}) == {"ok": True}
    assert client.copy_object_into_library("risk", {"name": "source"}) == {"ok": True}
    assert client.remove_library_entry("risk", "source") == {"ok": True}

    assert calls == [
        ("GET", "/libraries?include_accessible=0", None, "user"),
        ("POST", "/libraries", {"slug": "risk"}, "user"),
        ("GET", "/libraries/risk", None, "user"),
        ("PUT", "/libraries/risk", {"description": "Updated"}, "user"),
        ("GET", "/libraries/risk/grants", None, "user"),
        ("POST", "/libraries/risk/grants", {"grantee_id": "admin2"}, "user"),
        ("POST", "/libraries/risk/grants/admin2/revoke", None, "user"),
        ("POST", "/libraries/risk/references", {"name": "source"}, "user"),
        ("POST", "/libraries/risk/copies", {"name": "source"}, "user"),
        ("DELETE", "/libraries/risk/entries/source", None, "user"),
    ]


def test_server_client_uses_user_bearer_for_library_admin_calls() -> None:
    client = ServerClient(
        "https://splime.io/api/",
        "machine-token",
        user_token="user-token",
    )

    assert client._headers(auth="machine") == {
        "Accept": "application/json",
        "Authorization": "Bearer machine-token",
        "X-SPL-User-Token": "user-token",
    }
    assert client._headers(auth="user") == {
        "Accept": "application/json",
        "Authorization": "Bearer user-token",
    }


def test_server_client_surfaces_actionable_redirect_error(monkeypatch) -> None:
    def refuse_redirect(request: Any, **kwargs: Any) -> None:
        del kwargs
        raise HTTPError(
            request.full_url,
            302,
            "HTTP 302 redirect refused: configure the client with the final HTTPS endpoint instead",
            {},
            BytesIO(),
        )

    monkeypatch.setattr(remote_client, "urlopen_verified", refuse_redirect)

    with pytest.raises(remote_client.ServerClientError, match="final HTTPS endpoint"):
        ServerClient("https://splime.io/api", "machine-token").list_machines()


def test_streaming_file_request_uses_shared_transport_without_buffering(tmp_path, monkeypatch) -> None:
    calls: dict[str, Any] = {}

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            del args

        def read(self) -> bytes:
            return b'{"ok": true}'

    def fake_open(request: Any, *, timeout: float | None) -> FakeResponse:
        calls["request"] = request
        calls["timeout"] = timeout
        calls["chunks"] = list(request.data)
        return FakeResponse()

    upload = tmp_path / "payload.bin"
    payload = b"a" * (1024 * 1024 + 7)
    upload.write_bytes(payload)

    monkeypatch.setattr(remote_client, "urlopen_verified", fake_open)

    result = ServerClient("https://splime.io/api", "machine-token")._streaming_file_request(
        "PUT",
        "/artifacts/run-1/payload.bin",
        upload,
    )

    assert result == {"ok": True}
    assert calls["request"].full_url == "https://splime.io/api/artifacts/run-1/payload.bin"
    assert calls["request"].get_method() == "PUT"
    assert calls["timeout"] == remote_client.DEFAULT_FILE_TRANSFER_TIMEOUT_SECONDS
    assert [len(chunk) for chunk in calls["chunks"]] == [1024 * 1024, 7]
    assert b"".join(calls["chunks"]) == payload
    headers = {name.casefold(): value for name, value in calls["request"].header_items()}
    assert headers["authorization"] == "Bearer machine-token"
    assert headers["content-length"] == str(len(payload))


def test_owner_and_handle_reads_use_exact_additive_central_paths(monkeypatch) -> None:
    client = ServerClient(
        "https://splime.io/api",
        "machine-token",
        user_token="user-token",
    )
    calls = []

    def fake_json_request(method, path, payload=None, *, auth="machine"):
        calls.append((method, path, payload, auth))
        return []

    monkeypatch.setattr(client, "_json_request", fake_json_request)

    client.list_users()
    client.list_users(handle="@alice")
    client.list_libraries(include_accessible=True)
    client.list_owner_libraries("@alice")
    client.get_library("default")
    client.get_library("default", owner="@alice")
    client.list_library_grants("default")
    client.list_library_grants("default", owner="@alice")
    client.list_objects(library="default")
    client.list_objects(owner_id="@alice", library="default")
    client.get_object("score", owner_id="@alice", library="default")
    client.object_signature("score", owner_id="@alice", library="default")

    assert calls == [
        ("GET", "/users", None, "user"),
        ("GET", "/users?handle=%40alice", None, "user"),
        ("GET", "/libraries?include_accessible=1", None, "user"),
        ("GET", "/owners/%40alice/libraries", None, "user"),
        ("GET", "/libraries/default", None, "user"),
        ("GET", "/owners/%40alice/libraries/default", None, "user"),
        ("GET", "/libraries/default/grants", None, "user"),
        ("GET", "/libraries/default/grants?owner=%40alice", None, "user"),
        ("GET", "/objects?library=default", None, "machine"),
        ("GET", "/objects?owner=%40alice&library=default", None, "machine"),
        (
            "GET",
            "/owners/%40alice/libraries/default/objects/score",
            None,
            "machine",
        ),
        (
            "GET",
            "/owners/%40alice/libraries/default/objects/score/signature",
            None,
            "machine",
        ),
    ]
