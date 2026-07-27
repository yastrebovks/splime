"""Safe HTTP helpers shared by local-daemon and central-server clients."""

from __future__ import annotations

import http.client
import logging
import ssl
from functools import lru_cache, partial
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import (
    HTTPHandler,
    HTTPRedirectHandler,
    HTTPSHandler,
    ProxyHandler,
    Request,
    build_opener,
)

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_HTTP_TIMEOUT_SECONDS = 60.0
DEFAULT_FILE_TRANSFER_TIMEOUT_SECONDS = 300.0

_LOOPBACK_HTTP_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
_DOCKER_CALLBACK_HTTP_HOST = "host.docker.internal"
_DOCKER_CALLBACK_PATH = "/remote-nodes/run"
_CREDENTIAL_HEADER_NAMES = frozenset(
    {
        "authorization",
        "cookie",
        "proxy-authorization",
        "x-spl-claim",
        "x-spl-user-token",
    }
)
_LOGGER = logging.getLogger(__name__)


class InsecureCredentialTransportError(ValueError):
    """Reject a credentialed request that would use unsafe plaintext HTTP."""


class _NoRedirectHandler(HTTPRedirectHandler):
    """Surface redirects without replaying a request or any of its headers."""

    def redirect_request(
        self,
        request: Request,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> Request | None:
        del message, new_url
        raise HTTPError(
            request.full_url,
            code,
            (
                f"HTTP {code} redirect refused: SPL HTTP requests do not follow redirects; "
                "configure the client with the final HTTPS endpoint instead"
            ),
            headers,
            file_pointer,
        )


@lru_cache(maxsize=1)
def _warn_loopback_http_once() -> None:
    _LOGGER.warning(
        "SPL is sending credentials over plaintext HTTP to an explicit loopback development target; "
        "use HTTPS outside local development"
    )


@lru_cache(maxsize=1)
def _warn_docker_callback_http_once() -> None:
    _LOGGER.warning(
        "SPL is sending a run-scoped callback capability over plaintext HTTP to the local Docker host bridge"
    )


def _credential_header_names(request: Request) -> set[str]:
    return {
        name.casefold()
        for name, value in request.header_items()
        if value and name.casefold() in _CREDENTIAL_HEADER_NAMES
    }


def _authorization_bearer_token(request: Request) -> str | None:
    """Return one syntactically valid bearer token without logging it."""

    authorization = next(
        (value for name, value in request.header_items() if name.casefold() == "authorization"),
        "",
    )
    scheme, separator, token = authorization.partition(" ")
    if scheme.casefold() != "bearer" or not separator or not token:
        return None
    return token


def _validate_transport(
    request: Request,
    *,
    allow_docker_callback_http: bool,
    contains_credentials: bool,
) -> bool:
    """Validate one request and return whether environment proxies must be bypassed."""

    parsed = urlsplit(request.full_url)
    scheme = parsed.scheme.casefold()
    host = (parsed.hostname or "").casefold()
    credential_headers = _credential_header_names(request)
    url_contains_credentials = parsed.username is not None or parsed.password is not None
    is_credentialed = contains_credentials or bool(credential_headers) or url_contains_credentials

    if scheme not in {"http", "https"} or not host:
        raise ValueError("SPL HTTP request URLs must use an absolute http:// or https:// URL with a host")

    if allow_docker_callback_http:
        # Import lazily to keep the generic transport module out of the
        # daemon package's compatibility-import cycle while sharing the exact
        # token namespace with the authority that mints it.
        from spl.daemon.callback_capability import CALLBACK_CAPABILITY_PREFIX

        bearer_token = _authorization_bearer_token(request)
        docker_callback_is_valid = (
            scheme == "http"
            and host == _DOCKER_CALLBACK_HTTP_HOST
            and parsed.path == _DOCKER_CALLBACK_PATH
            and not parsed.query
            and not parsed.fragment
            and request.get_method().upper() == "POST"
            and bearer_token is not None
            and bearer_token.startswith(CALLBACK_CAPABILITY_PREFIX)
            and len(bearer_token) > len(CALLBACK_CAPABILITY_PREFIX)
            and credential_headers == {"authorization"}
            and not contains_credentials
        )
        if not docker_callback_is_valid:
            raise InsecureCredentialTransportError(
                "the Docker plaintext callback exception is limited to a POST authenticated with a "
                "Bearer splcb_... run callback capability to "
                "http://host.docker.internal/remote-nodes/run without a query or any other credential"
            )
        _warn_docker_callback_http_once()
        return True

    is_loopback = host in _LOOPBACK_HTTP_HOSTS
    if scheme == "http" and is_credentialed:
        if not is_loopback:
            raise InsecureCredentialTransportError(
                "credentialed SPL HTTP requests require HTTPS; plaintext is allowed only for the explicit "
                "loopback development hosts 127.0.0.1, ::1, and localhost"
            )
        _warn_loopback_http_once()

    return is_loopback


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
    """Return a verifying context with environment/system and certifi roots."""

    import certifi

    context = ssl.create_default_context()
    context.load_verify_locations(cafile=certifi.where())
    return context


def urlopen_verified(
    request: Request,
    *,
    timeout: float | None = DEFAULT_HTTP_TIMEOUT_SECONDS,
    connect_timeout: float | None = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    allow_docker_callback_http: bool = False,
    contains_credentials: bool = False,
) -> Any:
    """Open a policy-checked request with verified TLS and split timeout budgets.

    ``contains_credentials`` marks authentication material carried in a body
    instead of a recognized header. ``allow_docker_callback_http`` is a narrow
    exception for the run-scoped worker callback route only.
    """

    bypass_environment_proxies = _validate_transport(
        request,
        allow_docker_callback_http=allow_docker_callback_http,
        contains_credentials=contains_credentials,
    )

    handlers: list[Any] = [
        _NoRedirectHandler(),
        _SplitTimeoutHTTPHandler(read_timeout=timeout),
        _SplitTimeoutHTTPSHandler(
            context=verified_https_context(),
            read_timeout=timeout,
        ),
    ]
    if bypass_environment_proxies:
        handlers.insert(0, ProxyHandler({}))
    opener = build_opener(*handlers)
    return opener.open(request, timeout=connect_timeout)
