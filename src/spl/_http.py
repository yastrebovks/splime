"""HTTP helpers shared by central-server clients."""

from __future__ import annotations

import http.client
import ssl
from functools import partial
from functools import lru_cache
from typing import Any
from urllib.request import HTTPHandler, HTTPSHandler, Request, build_opener

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_HTTP_TIMEOUT_SECONDS = 60.0
DEFAULT_FILE_TRANSFER_TIMEOUT_SECONDS = 300.0


class ConnectionPhaseError(OSError):
    """Wrap a failure proven to occur while establishing the connection."""

    def __init__(self, cause: OSError):
        self.cause = cause
        super().__init__(str(cause))


class _PhaseAwareHTTPConnection(http.client.HTTPConnection):
    def __init__(
        self,
        host: str,
        *,
        read_timeout: float | None,
        **kwargs: Any,
    ) -> None:
        self._spl_read_timeout = read_timeout
        super().__init__(host, **kwargs)

    def connect(self) -> None:
        try:
            super().connect()
        except OSError as exc:
            raise ConnectionPhaseError(exc) from exc
        if self.sock is not None:
            self.sock.settimeout(self._spl_read_timeout)


class _PhaseAwareHTTPSConnection(http.client.HTTPSConnection):
    def __init__(
        self,
        host: str,
        *,
        read_timeout: float | None,
        **kwargs: Any,
    ) -> None:
        self._spl_read_timeout = read_timeout
        super().__init__(host, **kwargs)

    def connect(self) -> None:
        try:
            # HTTPSConnection.connect includes TCP connect, any proxy tunnel,
            # and the TLS handshake. No application request has been sent yet.
            super().connect()
        except OSError as exc:
            raise ConnectionPhaseError(exc) from exc
        if self.sock is not None:
            self.sock.settimeout(self._spl_read_timeout)


class _SplitTimeoutHTTPHandler(HTTPHandler):
    def __init__(self, *, read_timeout: float | None) -> None:
        super().__init__()
        self._spl_read_timeout = read_timeout

    def http_open(self, request: Request) -> Any:
        connection = partial(
            _PhaseAwareHTTPConnection,
            read_timeout=self._spl_read_timeout,
        )
        return self.do_open(connection, request)


class _SplitTimeoutHTTPSHandler(HTTPSHandler):
    def __init__(
        self,
        *,
        context: ssl.SSLContext,
        read_timeout: float | None,
    ) -> None:
        super().__init__(context=context)
        self._spl_context = context
        self._spl_read_timeout = read_timeout

    def https_open(self, request: Request) -> Any:
        connection = partial(
            _PhaseAwareHTTPSConnection,
            read_timeout=self._spl_read_timeout,
        )
        return self.do_open(connection, request, context=self._spl_context)


@lru_cache(maxsize=1)
def verified_https_context() -> ssl.SSLContext:
    """Return a certificate-verifying HTTPS context backed by certifi."""

    import certifi

    return ssl.create_default_context(cafile=certifi.where())


def urlopen_verified(
    request: Request,
    *,
    timeout: float | None = DEFAULT_HTTP_TIMEOUT_SECONDS,
    connect_timeout: float | None = DEFAULT_CONNECT_TIMEOUT_SECONDS,
) -> Any:
    """Open a request with verified TLS and separate connect/read budgets."""

    opener = build_opener(
        _SplitTimeoutHTTPHandler(read_timeout=timeout),
        _SplitTimeoutHTTPSHandler(
            context=verified_https_context(),
            read_timeout=timeout,
        ),
    )
    return opener.open(request, timeout=connect_timeout)
