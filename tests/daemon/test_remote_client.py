from __future__ import annotations

from spl.daemon.remote_client import ServerClient


def test_server_client_library_methods_build_central_paths(monkeypatch) -> None:
    client = ServerClient(
        "https://splime.io/api/",
        "machine-token",
        user_token="user-token",
    )
    calls = []

    def fake_json_request(method, path, payload=None):
        calls.append((method, path, payload))
        return {"ok": True}

    monkeypatch.setattr(client, "_json_request", fake_json_request)

    assert client.list_libraries(include_accessible=False) == {"ok": True}
    assert client.create_library({"slug": "risk"}) == {"ok": True}
    assert client.get_library("risk") == {"ok": True}
    assert client.update_library("risk", {"description": "Updated"}) == {"ok": True}
    assert client.delete_library("risk") == {"ok": True}
    assert client.list_library_grants("risk") == {"ok": True}
    assert client.grant_library("risk", {"grantee_id": "admin2"}) == {"ok": True}
    assert client.revoke_library_grant("risk", "admin2") == {"ok": True}
    assert client.add_library_reference("risk", {"name": "source"}) == {"ok": True}
    assert client.copy_object_into_library("risk", {"name": "source"}) == {"ok": True}
    assert client.remove_library_entry("risk", "source") == {"ok": True}

    assert calls == [
        ("GET", "/libraries?include_accessible=0", None),
        ("POST", "/libraries", {"slug": "risk"}),
        ("GET", "/libraries/risk", None),
        ("PUT", "/libraries/risk", {"description": "Updated"}),
        ("DELETE", "/libraries/risk", None),
        ("GET", "/libraries/risk/grants", None),
        ("POST", "/libraries/risk/grants", {"grantee_id": "admin2"}),
        ("POST", "/libraries/risk/grants/admin2/revoke", None),
        ("POST", "/libraries/risk/references", {"name": "source"}),
        ("POST", "/libraries/risk/copies", {"name": "source"}),
        ("DELETE", "/libraries/risk/entries/source", None),
    ]
