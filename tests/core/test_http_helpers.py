from __future__ import annotations

from urllib.request import Request

from spl import _http


def test_urlopen_verified_uses_bundled_ca_context(monkeypatch) -> None:
    sentinel_context = object()
    calls = {}
    response = object()

    def fake_context():
        return sentinel_context

    def fake_urlopen(request, *, context):
        calls["request"] = request
        calls["context"] = context
        return response

    request = Request("https://splime.io/api/health")
    monkeypatch.setattr(_http, "verified_https_context", fake_context)
    monkeypatch.setattr(_http, "urlopen", fake_urlopen)

    assert _http.urlopen_verified(request) is response
    assert calls == {"request": request, "context": sentinel_context}
