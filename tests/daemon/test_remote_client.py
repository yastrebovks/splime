from __future__ import annotations

from typing import Any

from spl.daemon import remote_client
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


def test_streaming_file_request_uses_bundled_ca_context(tmp_path, monkeypatch) -> None:
    sentinel_context = object()
    calls: dict[str, Any] = {}

    class FakeResponse:
        status = 200

        def read(self) -> bytes:
            return b'{"ok": true}'

    class FakeHTTPSConnection:
        def __init__(self, host, port=None, *, timeout=None, context=None):
            calls["init"] = {
                "host": host,
                "port": port,
                "timeout": timeout,
                "context": context,
            }

        def putrequest(self, method, path):
            calls["request"] = (method, path)

        def putheader(self, name, value):
            calls.setdefault("headers", []).append((name, value))

        def endheaders(self):
            calls["ended"] = True

        def send(self, chunk):
            calls.setdefault("chunks", []).append(chunk)

        def getresponse(self):
            return FakeResponse()

        def close(self):
            calls["closed"] = True

    upload = tmp_path / "payload.bin"
    upload.write_bytes(b"payload")

    monkeypatch.setattr(remote_client, "verified_https_context", lambda: sentinel_context)
    monkeypatch.setattr(remote_client.http.client, "HTTPSConnection", FakeHTTPSConnection)

    result = ServerClient("https://splime.io/api", "machine-token")._streaming_file_request(
        "PUT",
        "/artifacts/run-1/payload.bin",
        upload,
    )

    assert result == {"ok": True}
    assert calls["init"]["context"] is sentinel_context
    assert calls["init"]["host"] == "splime.io"
    assert calls["request"] == ("PUT", "/api/artifacts/run-1/payload.bin")
    assert calls["chunks"] == [b"payload"]
    assert calls["closed"] is True
