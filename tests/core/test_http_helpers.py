from __future__ import annotations

from urllib.request import Request

from spl import _http


def test_urlopen_verified_uses_bundled_ca_context(monkeypatch) -> None:
    sentinel_context = object()
    calls = {}
    response = object()

    def fake_context():
        return sentinel_context

    def fake_urlopen(request, *, context, timeout):
        calls["request"] = request
        calls["context"] = context
        calls["timeout"] = timeout
        return response

    request = Request("https://splime.io/api/health")
    monkeypatch.setattr(_http, "verified_https_context", fake_context)
    monkeypatch.setattr(_http, "urlopen", fake_urlopen)

    assert _http.urlopen_verified(request) is response
    assert calls == {
        "request": request,
        "context": sentinel_context,
        "timeout": _http.DEFAULT_HTTP_TIMEOUT_SECONDS,
    }


def test_urlopen_verified_passes_explicit_timeout(monkeypatch) -> None:
    calls = {}
    response = object()

    def fake_urlopen(request, *, context, timeout):
        calls["request"] = request
        calls["context"] = context
        calls["timeout"] = timeout
        return response

    request = Request("https://splime.io/api/health")
    monkeypatch.setattr(_http, "verified_https_context", lambda: "ctx")
    monkeypatch.setattr(_http, "urlopen", fake_urlopen)

    assert _http.urlopen_verified(request, timeout=12.5) is response
    assert calls == {
        "request": request,
        "context": "ctx",
        "timeout": 12.5,
    }
