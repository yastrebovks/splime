from __future__ import annotations

from contextlib import contextmanager
from email.message import Message
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import threading
from urllib.error import HTTPError

import pytest

from spl.daemon_client import ClientError
from spl.embedded import PublicRegistryTransport, _registry_retry_after


@contextmanager
def registry(statuses, *, retry_after="0", body=b'{"ok":true}'):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((self.command, dict(self.headers)))
            code = statuses[min(len(requests) - 1, len(statuses) - 1)]
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            if code == 429:
                self.send_header("Retry-After", retry_after)
            self.end_headers()
            self.wfile.write(body)

        do_POST = do_GET

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield PublicRegistryTransport(f"http://127.0.0.1:{server.server_port}"), requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_real_http_rate_limit_recovers_without_credentials(monkeypatch):
    sleeps = []
    monkeypatch.setattr("spl.embedded.time.sleep", sleeps.append)
    with registry([429, 200]) as (transport, requests):
        with pytest.warns(RuntimeWarning, match="retrying in 1 seconds") as caught:
            assert transport.json("/public/v1/example") == {"ok": True}
    assert sleeps == [1]
    assert len(caught) == 1
    assert len(requests) == 2
    assert all(method == "GET" and "Authorization" not in headers for method, headers in requests)


def test_permanent_rate_limit_has_finite_retries_and_actionable_error(monkeypatch):
    sleeps = []
    monkeypatch.setattr("spl.embedded.time.sleep", sleeps.append)
    with registry([429]) as (transport, requests):
        with pytest.warns(RuntimeWarning) as caught, pytest.raises(ClientError) as error:
            transport.json("/public/v1/example")
    assert len(caught) == 1
    assert sleeps == [1, 1, 1]
    assert len(requests) == 4
    assert error.value.code == "public_registry_rate_limited"
    assert error.value.status_code == 429


def test_long_retry_after_does_not_block_or_retry_early(monkeypatch):
    sleeps = []
    monkeypatch.setattr("spl.embedded.time.sleep", sleeps.append)
    with registry([429], retry_after="86400") as (transport, requests):
        with pytest.raises(ClientError, match="86400 seconds"):
            transport.json("/public/v1/example")
    assert len(requests) == 1
    assert not sleeps


@pytest.mark.parametrize(
    "status,code",
    [(404, "public_entry_unavailable"), (302, "public_registry_redirect_rejected"), (503, "public_registry_error")],
)
def test_other_failures_are_not_retried(monkeypatch, status, code):
    sleeps = []
    monkeypatch.setattr("spl.embedded.time.sleep", sleeps.append)
    with registry([status, 200]) as (transport, requests):
        with pytest.raises(ClientError) as error:
            transport.json("/public/v1/example")
    assert error.value.code == code
    assert len(requests) == 1
    assert not sleeps


def test_receipt_post_is_not_replayed(monkeypatch):
    sleeps = []
    monkeypatch.setattr("spl.embedded.time.sleep", sleeps.append)
    with registry([429, 200]) as (transport, requests):
        with pytest.raises(ClientError, match="public_run_receipt_unavailable"):
            transport.post_json("/public/v1/receipt", {})
    assert len(requests) == 1 and requests[0][0] == "POST"
    assert not sleeps


@pytest.mark.parametrize("header", [None, "", "-1", "nan", "inf", "1.5", "bad date", "9" * 129])
def test_invalid_retry_after_uses_bounded_backoff(header):
    assert [_registry_retry_after(header, i) for i in range(3)] == [1, 2, 4]


def test_http_date_retry_after_uses_server_deadline(monkeypatch):
    now = 1_800_000_000
    monkeypatch.setattr("spl.embedded.time.time", lambda: now)
    assert _registry_retry_after(formatdate(now + 60, usegmt=True), 0) == 60
    assert _registry_retry_after(formatdate(now - 60, usegmt=True), 0) == 1


def test_elapsed_request_time_exhausts_retry_budget_and_closes_responses(monkeypatch):
    transport = PublicRegistryTransport("https://registry.example.test")
    clock = iter([0, 64, 66])
    monkeypatch.setattr("spl.embedded.time.monotonic", lambda: next(clock))
    sleeps = []
    monkeypatch.setattr("spl.embedded.time.sleep", sleeps.append)
    bodies = []

    def limited(*args, **kwargs):
        body = io.BytesIO(b"ignored server detail")
        bodies.append(body)
        headers = Message()
        headers["Retry-After"] = "1"
        raise HTTPError("https://registry.example.test/example", 429, "limited", headers, body)

    monkeypatch.setattr(transport._opener, "open", limited)
    with pytest.warns(RuntimeWarning), pytest.raises(ClientError, match="public_registry_rate_limited"):
        transport.json("/example")
    assert sleeps == [1]
    assert len(bodies) == 2 and all(body.closed for body in bodies)


def test_successful_retry_still_validates_json(monkeypatch):
    monkeypatch.setattr("spl.embedded.time.sleep", lambda delay: None)
    with registry([429, 200], body=b"not JSON") as (transport, requests):
        with pytest.warns(RuntimeWarning), pytest.raises(ClientError, match="public_registry_invalid"):
            transport.json("/example")
    assert len(requests) == 2
