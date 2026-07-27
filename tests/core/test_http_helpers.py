from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request

import certifi
import pytest

from spl import _http


class _FakeOpener:
    def __init__(self, response: object, calls: dict[str, Any]) -> None:
        self.response = response
        self.calls = calls

    def open(self, request: Request, *, timeout: float | None) -> object:
        self.calls["request"] = request
        self.calls["connect_timeout"] = timeout
        return self.response


def test_verified_https_context_keeps_default_trust_and_adds_certifi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeContext:
        def load_verify_locations(self, *, cafile: str) -> None:
            calls.append(cafile)

    context = FakeContext()

    def create_default_context() -> FakeContext:
        calls.append("default")
        return context

    _http.verified_https_context.cache_clear()
    monkeypatch.setattr(_http.ssl, "create_default_context", create_default_context)
    try:
        assert _http.verified_https_context() is context
        assert calls == ["default", certifi.where()]
    finally:
        _http.verified_https_context.cache_clear()


@contextmanager
def _running_server(
    handler: type[BaseHTTPRequestHandler],
) -> Iterator[ThreadingHTTPServer]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


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
    assert any(isinstance(handler, _http._NoRedirectHandler) for handler in calls["handlers"])
    http_handler = next(handler for handler in calls["handlers"] if isinstance(handler, _http._SplitTimeoutHTTPHandler))
    https_handler = next(
        handler for handler in calls["handlers"] if isinstance(handler, _http._SplitTimeoutHTTPSHandler)
    )
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
    split_handlers = [
        handler
        for handler in calls["handlers"]
        if isinstance(handler, (_http._SplitTimeoutHTTPHandler, _http._SplitTimeoutHTTPSHandler))
    ]
    assert all(handler._spl_read_timeout == 60.0 for handler in split_handlers)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.invalid/api",
        "http://127.0.0.2:8765/health",
        "http://127.0.0.1.example:8765/health",
        "http://localhost.example:8765/health",
        "http://[::2]:8765/health",
    ],
)
def test_credentialed_plaintext_rejects_every_non_loopback_host(
    url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_build_opener(*handlers: Any) -> _FakeOpener:
        del handlers
        raise AssertionError("unsafe request reached the HTTP opener")

    monkeypatch.setattr(_http, "build_opener", unexpected_build_opener)
    request = Request(
        url,
        headers={
            "Authorization": "Bearer fake-machine-token",
            "X-SPL-User-Token": "fake-user-token",
        },
    )

    with pytest.raises(_http.InsecureCredentialTransportError, match="require HTTPS"):
        _http.urlopen_verified(request)


def test_explicit_body_credentials_also_require_https(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected_build_opener(*handlers: Any) -> _FakeOpener:
        del handlers
        raise AssertionError("unsafe request reached the HTTP opener")

    monkeypatch.setattr(_http, "build_opener", unexpected_build_opener)
    request = Request(
        "http://daemon.example.invalid/server/connect",
        data=b'{"machine_token":"fake","user_token":"fake"}',
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with pytest.raises(_http.InsecureCredentialTransportError, match="require HTTPS"):
        _http.urlopen_verified(request, contains_credentials=True)


@pytest.mark.parametrize(
    "url",
    [
        "http://user:password@example.invalid/api",
        "http://token@example.invalid/api",
    ],
)
def test_url_userinfo_credentials_also_require_https(
    url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_build_opener(*handlers: Any) -> _FakeOpener:
        del handlers
        raise AssertionError("unsafe userinfo request reached the HTTP opener")

    monkeypatch.setattr(_http, "build_opener", unexpected_build_opener)

    with pytest.raises(_http.InsecureCredentialTransportError, match="require HTTPS"):
        _http.urlopen_verified(Request(url))


def test_loopback_plaintext_bypasses_proxies_and_warns_once(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[tuple[Any, ...]] = []
    response = object()

    def fake_build_opener(*handlers: Any) -> _FakeOpener:
        calls.append(handlers)
        return _FakeOpener(response, {})

    _http._warn_loopback_http_once.cache_clear()
    monkeypatch.setattr(_http, "verified_https_context", lambda: "ctx")
    monkeypatch.setattr(_http, "build_opener", fake_build_opener)
    caplog.set_level(logging.WARNING, logger="spl._http")

    for url in (
        "http://127.0.0.1:8765/health",
        "http://localhost:8765/health",
        "http://[::1]:8765/health",
    ):
        request = Request(url, headers={"Authorization": "Bearer fake-local-token"})
        assert _http.urlopen_verified(request) is response

    assert len(calls) == 3
    assert all(
        any(isinstance(handler, ProxyHandler) and handler.proxies == {} for handler in handlers) for handlers in calls
    )
    warnings = [record for record in caplog.records if "explicit loopback development target" in record.message]
    assert len(warnings) == 1
    assert "fake-local-token" not in warnings[0].message


def test_docker_callback_plaintext_exception_is_narrow_and_off_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}
    response = object()

    def fake_build_opener(*handlers: Any) -> _FakeOpener:
        calls["handlers"] = handlers
        return _FakeOpener(response, calls)

    monkeypatch.setattr(_http, "verified_https_context", lambda: "ctx")
    monkeypatch.setattr(_http, "build_opener", fake_build_opener)
    request = Request(
        "http://host.docker.internal:8765/remote-nodes/run",
        data=b"{}",
        headers={"Authorization": "Bearer splcb_fake-callback-capability"},
        method="POST",
    )

    with pytest.raises(_http.InsecureCredentialTransportError, match="require HTTPS"):
        _http.urlopen_verified(request)

    assert (
        _http.urlopen_verified(
            request,
            timeout=None,
            allow_docker_callback_http=True,
        )
        is response
    )
    assert calls["connect_timeout"] == _http.DEFAULT_CONNECT_TIMEOUT_SECONDS
    assert any(isinstance(handler, ProxyHandler) and handler.proxies == {} for handler in calls["handlers"])


@pytest.mark.parametrize(
    ("url", "method", "headers"),
    [
        (
            "http://host.docker.internal:8765/not-the-callback",
            "POST",
            {"Authorization": "Bearer splcb_fake-callback-capability"},
        ),
        (
            "http://host.docker.internal:8765/remote-nodes/run?other=1",
            "POST",
            {"Authorization": "Bearer splcb_fake-callback-capability"},
        ),
        (
            "http://host.docker.internal:8765/remote-nodes/run",
            "GET",
            {"Authorization": "Bearer splcb_fake-callback-capability"},
        ),
        (
            "http://host.docker.internal:8765/remote-nodes/run",
            "POST",
            {"Authorization": "Bearer fake-daemon-master-token"},
        ),
        (
            "http://host.docker.internal:8765/remote-nodes/run",
            "POST",
            {"Authorization": "Bearer splcb_"},
        ),
        (
            "http://host.docker.internal:8765/remote-nodes/run",
            "POST",
            {
                "Authorization": "Bearer fake-machine-token",
                "X-SPL-User-Token": "fake-user-token",
            },
        ),
        *(
            (
                "http://host.docker.internal:8765/remote-nodes/run",
                "POST",
                {
                    "Authorization": "Bearer splcb_fake-callback-capability",
                    header: "fake-secondary-credential",
                },
            )
            for header in ("Cookie", "Proxy-Authorization", "X-SPL-Claim", "X-SPL-User-Token")
        ),
    ],
)
def test_docker_callback_plaintext_exception_rejects_other_shapes(
    url: str,
    method: str,
    headers: dict[str, str],
) -> None:
    request = Request(url, data=b"{}" if method == "POST" else None, headers=headers, method=method)

    with pytest.raises(_http.InsecureCredentialTransportError, match=r"Bearer splcb_\.\.\."):
        _http.urlopen_verified(request, allow_docker_callback_http=True)


def test_docker_callback_plaintext_exception_rejects_body_credentials() -> None:
    request = Request(
        "http://host.docker.internal:8765/remote-nodes/run",
        data=b"{}",
        headers={"Authorization": "Bearer splcb_fake-callback-capability"},
        method="POST",
    )

    with pytest.raises(_http.InsecureCredentialTransportError, match=r"Bearer splcb_\.\.\."):
        _http.urlopen_verified(
            request,
            allow_docker_callback_http=True,
            contains_credentials=True,
        )


def test_redirect_codes_never_replay_credentials_to_another_origin() -> None:
    captured: list[dict[str, str | None]] = []
    origin_requests: list[dict[str, str | None]] = []

    class CaptureHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            captured.append(
                {
                    "authorization": self.headers.get("Authorization"),
                    "user_token": self.headers.get("X-SPL-User-Token"),
                }
            )
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"captured")

        def log_message(self, format_string: str, *args: object) -> None:
            del format_string, args

    with _running_server(CaptureHandler) as capture_server:

        class RedirectHandler(BaseHTTPRequestHandler):
            redirect_code = 302

            def do_GET(self) -> None:
                origin_requests.append(
                    {
                        "authorization": self.headers.get("Authorization"),
                        "user_token": self.headers.get("X-SPL-User-Token"),
                    }
                )
                self.send_response(self.redirect_code)
                self.send_header(
                    "Location",
                    f"http://127.0.0.1:{capture_server.server_port}/capture",
                )
                self.end_headers()

            def log_message(self, format_string: str, *args: object) -> None:
                del format_string, args

        with _running_server(RedirectHandler) as redirect_server:
            for code in (301, 302, 303, 307, 308):
                RedirectHandler.redirect_code = code
                request = Request(
                    f"http://127.0.0.1:{redirect_server.server_port}/start",
                    headers={
                        "Authorization": "Bearer fake-machine-token",
                        "X-SPL-User-Token": "fake-user-token",
                    },
                )
                with pytest.raises(HTTPError) as captured_error:
                    _http.urlopen_verified(request, timeout=2, connect_timeout=2)
                try:
                    assert captured_error.value.code == code
                    assert "configure the client with the final HTTPS endpoint" in str(captured_error.value.reason)
                finally:
                    captured_error.value.close()

            RedirectHandler.redirect_code = 302
            with pytest.raises(HTTPError) as uncredentialed_error:
                _http.urlopen_verified(
                    Request(f"http://127.0.0.1:{redirect_server.server_port}/uncredentialed"),
                    timeout=2,
                    connect_timeout=2,
                )
            uncredentialed_error.value.close()

    assert captured == []
    assert (
        origin_requests[:5]
        == [
            {
                "authorization": "Bearer fake-machine-token",
                "user_token": "fake-user-token",
            }
        ]
        * 5
    )
    assert origin_requests[5] == {"authorization": None, "user_token": None}


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
