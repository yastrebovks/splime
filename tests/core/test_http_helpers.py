from __future__ import annotations

from typing import Any
from urllib.request import Request

from spl import _http


class _FakeOpener:
    def __init__(self, response: object, calls: dict[str, Any]) -> None:
        self.response = response
        self.calls = calls

    def open(self, request: Request, *, timeout: float | None) -> object:
        self.calls["request"] = request
        self.calls["connect_timeout"] = timeout
        return self.response


def test_urlopen_verified_uses_split_connect_and_read_timeouts(monkeypatch) -> None:
    sentinel_context = object()
    calls: dict[str, Any] = {}
    response = object()

    def fake_context() -> object:
        return sentinel_context

    def fake_build_opener(*handlers: Any) -> _FakeOpener:
        calls["handlers"] = handlers
        return _FakeOpener(response, calls)

    request = Request("https://splime.io/api/health")
    monkeypatch.setattr(_http, "verified_https_context", fake_context)
    monkeypatch.setattr(_http, "build_opener", fake_build_opener)

    assert _http.urlopen_verified(request) is response
    assert calls["request"] is request
    assert calls["connect_timeout"] == _http.DEFAULT_CONNECT_TIMEOUT_SECONDS
    http_handler, https_handler = calls["handlers"]
    assert http_handler._spl_read_timeout == _http.DEFAULT_HTTP_TIMEOUT_SECONDS
    assert https_handler._spl_read_timeout == _http.DEFAULT_HTTP_TIMEOUT_SECONDS
    assert https_handler._spl_context is sentinel_context


def test_urlopen_verified_passes_explicit_timeout(monkeypatch) -> None:
    calls: dict[str, Any] = {}
    response = object()

    def fake_build_opener(*handlers: Any) -> _FakeOpener:
        calls["handlers"] = handlers
        return _FakeOpener(response, calls)

    request = Request("https://splime.io/api/health")
    monkeypatch.setattr(_http, "verified_https_context", lambda: "ctx")
    monkeypatch.setattr(_http, "build_opener", fake_build_opener)

    assert _http.urlopen_verified(request, timeout=60.0, connect_timeout=12.5) is response
    assert calls["request"] is request
    assert calls["connect_timeout"] == 12.5
    assert all(handler._spl_read_timeout == 60.0 for handler in calls["handlers"])


def test_connection_wrapper_marks_only_connect_failures(monkeypatch) -> None:
    def fail_connect(connection: Any) -> None:
        del connection
        raise TimeoutError("TLS handshake timed out")

    monkeypatch.setattr(_http.http.client.HTTPSConnection, "connect", fail_connect)
    connection = _http._PhaseAwareHTTPSConnection(
        "splime.io",
        read_timeout=60.0,
        context=_http.verified_https_context(),
    )

    try:
        connection.connect()
    except _http.ConnectionPhaseError as exc:
        assert isinstance(exc.cause, TimeoutError)
        assert "handshake" in str(exc)
    else:  # pragma: no cover - assertion guard.
        raise AssertionError("connect failure was not classified")


def test_connection_wrapper_switches_to_read_timeout_after_connect(
    monkeypatch,
) -> None:
    calls: list[float | None] = []

    class FakeSocket:
        def settimeout(self, timeout: float | None) -> None:
            calls.append(timeout)

    def connect(connection: Any) -> None:
        connection.sock = FakeSocket()

    monkeypatch.setattr(_http.http.client.HTTPConnection, "connect", connect)
    connection = _http._PhaseAwareHTTPConnection(
        "splime.io",
        timeout=10.0,
        read_timeout=60.0,
    )

    connection.connect()

    assert connection.timeout == 10.0
    assert calls == [60.0]
