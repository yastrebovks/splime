"""Client helpers for talking to the central SPL daemon server."""

from __future__ import annotations

import hashlib
import json
import re
import socket
import ssl
import time
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Literal, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request

from spl._http import (
    ConnectionPhaseError,
    DEFAULT_FILE_TRANSFER_TIMEOUT_SECONDS,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    urlopen_verified,
)
from spl.core import json_contract as m_json_contract
from spl.daemon.ai_assistant import (
    AI_ASSISTANT_UPSTREAM_CAPABILITIES_ROUTE,
    AI_ASSISTANT_UPSTREAM_ROUTE,
    MAX_RESPONSE_BYTES as MAX_AI_ASSISTANT_RESPONSE_BYTES,
    AIAssistantContractError,
    validate_error as validate_ai_assistant_error,
)
from spl.daemon.ai_preview import (
    MAX_AI_PREVIEW_RESPONSE_BYTES,
    AIPreviewContractError,
    validate_ai_preview_error,
)
from spl.daemon.connected_ide import MAX_UPSTREAM_RESPONSE_BYTES

DEFAULT_SERVER_URL = "https://splime.io/api"
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
LIBRARY_DELETE_UNSUPPORTED_MESSAGE = (
    "Deleting central-server libraries is not supported by the SPL server API. "
    "Use the Console archive action to hide a library, or remove individual "
    "entries with client.library.remove_entry()."
)
SERVER_GET_RETRY_DELAY_SECONDS = 0.5
SERVER_MAX_TRANSPORT_ATTEMPTS = 3
TRANSIENT_SERVER_STATUS_CODES = frozenset({502, 503, 504})
RUN_CLAIM_FENCING_CAPABILITY = "run_claim_fencing"
RUN_CLAIM_FENCING_VERSION = 1
SYNC_FLUSH_WITHOUT_CLAIM_CAPABILITY = "spl.sync.flush_without_claim.v1"
SYNC_FLUSH_WITHOUT_CLAIM_VERSION = 1
SYNC_CLAIM_CONTROL_ACK = {
    "contract": "spl.sync.flush-without-claim/v1",
    "claim_jobs": False,
    "claim_suppressed": True,
}
RUN_CLAIM_HEADER = "X-Spl-Claim"
RUN_CLAIM_PRIVATE_FIELD = "_spl_claim_id"
STALE_RUN_CLAIM_ERROR_CODE = "stale_run_claim"
SYNC_EVENT_IDENTITY_COLLISION_ERROR_CODE = "sync_event_identity_collision"
PERMANENT_ARCHIVED_SYNC_ERROR_CODES = frozenset(
    {
        "library_archived",
        "resource_archived",
    }
)
FailurePhase = Literal["connection", "post_send", "application"]


class ServerClientError(RuntimeError):
    """Raised when the central daemon server returns an error response."""

    def __init__(
        self,
        status_code: int,
        message: str,
        *,
        code: str | None = None,
        event_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.status_code = status_code
        self.message = message
        self.code = code
        self.event_id = event_id
        self.payload = payload
        super().__init__(f"{status_code}: {message}")


def is_stale_run_claim_error(exc: ServerClientError) -> bool:
    """Return whether the server rejected a superseded worker attempt."""

    return exc.status_code == 409 and exc.code == STALE_RUN_CLAIM_ERROR_CODE


def is_sync_event_identity_collision_error(exc: ServerClientError) -> bool:
    """Return whether one exact queued event identity collided at the server."""

    return exc.status_code == 409 and exc.code == SYNC_EVENT_IDENTITY_COLLISION_ERROR_CODE and bool(exc.event_id)


def is_permanent_archived_sync_error(result: dict[str, Any]) -> bool:
    """Return whether a typed per-event rejection must never be retried."""

    return result.get("status") == "error" and result.get("code") in PERMANENT_ARCHIVED_SYNC_ERROR_CODES


def validate_no_claim_sync_response(response: dict[str, Any]) -> None:
    """Require the exact server acknowledgement for one no-claim sync.

    An empty jobs array is deliberately insufficient: an old server could
    ignore the request field and happen to have no queued work.
    """

    if response.get("claim_control") != SYNC_CLAIM_CONTROL_ACK:
        raise ServerClientError(
            502,
            "central SPL daemon server did not prove claim suppression",
            code="sync_claim_control_invalid",
        )
    jobs = response.get("jobs")
    if not isinstance(jobs, list) or jobs:
        raise ServerClientError(
            502,
            "central SPL daemon server returned work during no-claim sync",
            code="sync_claim_control_invalid",
        )


def _as_json_dict(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value)


def _as_json_list(value: Any) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], value)


def _require_json_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ServerClientError(
            502,
            "central SPL daemon server returned an invalid JSON object",
            code="server_response_invalid",
        )
    return value


def _require_json_dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ServerClientError(
            502,
            "central SPL daemon server returned an invalid JSON list",
            code="server_response_invalid",
        )
    return value


def _error_response(
    raw: str,
    *,
    http_error: HTTPError | None = None,
) -> tuple[str, str | None, str | None]:
    """Return safe structured fields from one JSON error response."""

    if http_error is not None and 300 <= http_error.code < 400:
        return str(http_error.reason), None, None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw, None, None
    if not isinstance(payload, dict):
        return raw, None, None

    error = payload.get("error", payload.get("safe_message", raw))
    code = payload.get("code")
    event_id = payload.get("event_id")
    if isinstance(error, dict):
        nested_code = error.get("code")
        if code is None and isinstance(nested_code, str):
            code = nested_code
        nested_event_id = error.get("event_id")
        if event_id is None and isinstance(nested_event_id, str):
            event_id = nested_event_id
        error = error.get("message") or error.get("error") or raw
    return (
        str(error),
        str(code) if isinstance(code, str) else None,
        str(event_id) if isinstance(event_id, str) else None,
    )


def _ai_preview_error_response(raw: str) -> tuple[str, str, None]:
    """Accept only the central AI Preview endpoint's closed error envelope."""

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result

    try:
        payload = json.loads(raw, object_pairs_hook=object_pairs)
        if not isinstance(payload, dict):
            raise ValueError("AI Preview error is not an object")
        validate_ai_preview_error(payload)
    except (AIPreviewContractError, json.JSONDecodeError, RecursionError, ValueError):
        return (
            "central SPL daemon server returned an invalid AI Preview error envelope",
            "ai_preview_protocol_invalid",
            None,
        )
    return payload["safe_message"], payload["code"], None


def _ai_assistant_error_response(raw: str) -> tuple[str, str, None]:
    """Accept only the central AI assistant's closed error envelope."""

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result

    try:
        payload = json.loads(raw, object_pairs_hook=object_pairs)
        if not isinstance(payload, dict):
            raise ValueError("AI assistant error is not an object")
        validate_ai_assistant_error(payload)
    except (
        AIAssistantContractError,
        json.JSONDecodeError,
        RecursionError,
        ValueError,
    ):
        return (
            "central SPL daemon server returned an invalid AI assistant error envelope",
            "ai_assistant_protocol_invalid",
            None,
        )
    return payload["safe_message"], payload["code"], None


def _claim_headers(claim_id: str | None) -> dict[str, str]:
    """Build the optional claim header without exposing it in a JSON body."""

    if claim_id is None:
        return {}
    if not isinstance(claim_id, str) or not claim_id.strip():
        raise ValueError("claim_id must be a non-empty string when provided")
    return {RUN_CLAIM_HEADER: claim_id}


def _sync_events_for_wire(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Copy sync events while removing daemon-private claim metadata."""

    wire_events: list[dict[str, Any]] = []
    for event in events:
        wire_event = dict(event)
        wire_event.pop(RUN_CLAIM_PRIVATE_FIELD, None)
        payload = wire_event.get("payload")
        if isinstance(payload, dict):
            wire_payload = dict(payload)
            wire_payload.pop(RUN_CLAIM_PRIVATE_FIELD, None)
            wire_event["payload"] = wire_payload
        wire_events.append(wire_event)
    return wire_events


def _file_chunks(source: BinaryIO) -> Iterator[bytes]:
    """Yield bounded chunks without loading an artifact fully into memory."""

    while chunk := source.read(1024 * 1024):
        yield chunk


def _exception_chain(exc: BaseException) -> list[BaseException]:
    """Return causes plus ``URLError.reason`` without following cycles."""

    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    chain: list[BaseException] = []
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        chain.append(current)
        if isinstance(current, URLError) and isinstance(current.reason, BaseException):
            pending.append(current.reason)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return chain


def _is_connection_phase_failure(exc: BaseException) -> bool:
    """Return whether evidence proves failure before the request was sent."""

    for cause in _exception_chain(exc):
        if isinstance(cause, ConnectionPhaseError):
            return True
        if isinstance(
            cause,
            (socket.gaierror, ConnectionRefusedError, ssl.SSLCertVerificationError),
        ):
            return True
        # A raw SSL/timeout error is ambiguous unless it names the handshake.
        # Production transport wraps all connect/handshake failures above;
        # this branch also recognizes the stdlib evidence from the pilot.
        if isinstance(cause, (ssl.SSLError, TimeoutError)) and "handshake" in str(cause).casefold():
            return True
    return False


def _failure_phase(exc: BaseException) -> FailurePhase:
    """Classify transport/application failure with a conservative default."""

    if isinstance(exc, HTTPError):
        if exc.code in TRANSIENT_SERVER_STATUS_CODES:
            return "post_send"
        return "application"
    if _is_connection_phase_failure(exc):
        return "connection"
    if isinstance(exc, (URLError, OSError)):
        return "post_send"
    return "application"


class ServerClient:
    """Small stdlib HTTP client for the central daemon server."""

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
        request_timeout_seconds: float | None = DEFAULT_HTTP_TIMEOUT_SECONDS,
    ):
        self.base_url = base_url.rstrip("/")
        self.machine_token = machine_token
        self.user_token = user_token
        self.request_timeout_seconds = request_timeout_seconds

    def _headers(self, *, auth: str = "machine") -> dict[str, str]:
        token = self.machine_token
        if auth == "user":
            if not self.user_token:
                raise ServerClientError(
                    401,
                    "central SPL daemon server user token is required for this operation",
                )
            token = self.user_token
        elif auth != "machine":
            raise ValueError("auth must be 'machine' or 'user'")
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }
        if auth == "machine" and self.user_token:
            headers["X-SPL-User-Token"] = self.user_token
        return headers

    def _json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        auth: str = "machine",
        extra_headers: dict[str, str] | None = None,
        post_send_retry_safe: bool = False,
        allow_transport_retries: bool = True,
        max_transport_attempts: int = SERVER_MAX_TRANSPORT_ATTEMPTS,
        max_response_bytes: int | None = None,
        compact_json: bool = False,
        absolute_deadline_seconds: float | None = None,
    ) -> Any:
        if max_transport_attempts < 1:
            raise ValueError("max_transport_attempts must be at least 1")
        if max_response_bytes is not None and max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive when provided")
        if absolute_deadline_seconds is not None and absolute_deadline_seconds <= 0:
            raise ValueError("absolute_deadline_seconds must be positive when provided")
        body = None
        headers = self._headers(auth=auth)
        headers.update(extra_headers or {})
        if payload is not None:
            body = m_json_contract.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=False,
                separators=(",", ":") if compact_json else None,
            ).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"

        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        method_is_get = method.upper() == "GET"
        connection_retries = 0
        post_send_retried = False
        attempt = 0
        deadline = None if absolute_deadline_seconds is None else time.monotonic() + absolute_deadline_seconds
        while attempt < max_transport_attempts:
            attempt += 1
            try:
                phase_timeout = self.request_timeout_seconds
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ServerClientError(
                            504,
                            "central SPL daemon server response exceeded its total deadline",
                            code="server_response_timeout",
                        )
                    phase_timeout = remaining if phase_timeout is None else min(phase_timeout, remaining)
                if deadline is None:
                    response_context = urlopen_verified(
                        request,
                        timeout=phase_timeout,
                    )
                else:
                    response_context = urlopen_verified(
                        request,
                        timeout=phase_timeout,
                        connect_timeout=phase_timeout,
                    )
                with response_context as response:
                    raw = (
                        response.read().decode("utf-8")
                        if max_response_bytes is None
                        else self._read_json_text(
                            response,
                            max_response_bytes=max_response_bytes,
                            deadline=deadline,
                        )
                    )
            except HTTPError as exc:
                raw = (
                    exc.read().decode("utf-8")
                    if max_response_bytes is None
                    else self._read_json_text(
                        exc,
                        max_response_bytes=max_response_bytes,
                        deadline=deadline,
                    )
                )
                try:
                    decoded_error = json.loads(raw)
                except json.JSONDecodeError:
                    error_payload = None
                else:
                    error_payload = decoded_error if isinstance(decoded_error, dict) else None
                message: str
                error_code: str | None
                event_id: str | None
                if path in {"/ai/preview", "/ai/preview/capabilities"}:
                    message, error_code, event_id = _ai_preview_error_response(raw)
                elif path in {
                    AI_ASSISTANT_UPSTREAM_ROUTE,
                    AI_ASSISTANT_UPSTREAM_CAPABILITIES_ROUTE,
                }:
                    message, error_code, event_id = _ai_assistant_error_response(raw)
                else:
                    message, error_code, event_id = _error_response(raw, http_error=exc)
                message = f"central SPL daemon server returned {exc.code} at {self.base_url}{path}: {message}"
                phase = _failure_phase(exc)
                if (
                    phase == "post_send"
                    and allow_transport_retries
                    and (method_is_get or post_send_retry_safe)
                    and not post_send_retried
                    and attempt < max_transport_attempts
                ):
                    post_send_retried = True
                    time.sleep(SERVER_GET_RETRY_DELAY_SECONDS)
                    continue
                raise ServerClientError(
                    exc.code,
                    message,
                    code=error_code,
                    event_id=event_id,
                    payload=error_payload,
                ) from exc
            except URLError as exc:
                message = f"central SPL daemon server is not reachable at {self.base_url}: {exc.reason}"
                phase = _failure_phase(exc)
                if (
                    phase == "connection"
                    and allow_transport_retries
                    and connection_retries < 2
                    and attempt < max_transport_attempts
                ):
                    delay = SERVER_GET_RETRY_DELAY_SECONDS * (1 if connection_retries == 0 else 3)
                    connection_retries += 1
                    time.sleep(delay)
                    continue
                if (
                    phase == "post_send"
                    and allow_transport_retries
                    and (method_is_get or post_send_retry_safe)
                    and not post_send_retried
                    and attempt < max_transport_attempts
                ):
                    post_send_retried = True
                    time.sleep(SERVER_GET_RETRY_DELAY_SECONDS)
                    continue
                raise ServerClientError(502, message) from exc
            except OSError as exc:
                message = f"central SPL daemon server is not reachable at {self.base_url}: {exc}"
                phase = _failure_phase(exc)
                if (
                    phase == "connection"
                    and allow_transport_retries
                    and connection_retries < 2
                    and attempt < max_transport_attempts
                ):
                    delay = SERVER_GET_RETRY_DELAY_SECONDS * (1 if connection_retries == 0 else 3)
                    connection_retries += 1
                    time.sleep(delay)
                    continue
                if (
                    phase == "post_send"
                    and allow_transport_retries
                    and (method_is_get or post_send_retry_safe)
                    and not post_send_retried
                    and attempt < max_transport_attempts
                ):
                    post_send_retried = True
                    time.sleep(SERVER_GET_RETRY_DELAY_SECONDS)
                    continue
                raise ServerClientError(502, message) from exc
            break
        else:  # pragma: no cover - every retry branch either succeeds or raises.
            raise AssertionError("server retry loop exhausted without a result")

        if not raw:
            return None
        if max_response_bytes is None:
            return json.loads(raw)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, RecursionError) as exc:
            raise ServerClientError(
                502,
                "central SPL daemon server returned invalid JSON",
                code="server_response_invalid",
            ) from exc

    @staticmethod
    def _read_json_text(
        response: Any,
        *,
        max_response_bytes: int,
        deadline: float | None = None,
    ) -> str:
        read1 = getattr(response, "read1", None)
        if deadline is None or not callable(read1):
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    response.close()
                    raise ServerClientError(
                        504,
                        "central SPL daemon server response exceeded its total deadline",
                        code="server_response_timeout",
                    )
                ServerClient._set_response_read_timeout(response, remaining)
            raw = response.read(max_response_bytes + 1)
            return ServerClient._decode_bounded_json_text(
                raw,
                max_response_bytes=max_response_bytes,
            )
        chunks: list[bytes] = []
        size = 0
        while size <= max_response_bytes:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    response.close()
                    raise ServerClientError(
                        504,
                        "central SPL daemon server response exceeded its total deadline",
                        code="server_response_timeout",
                    )
                ServerClient._set_response_read_timeout(response, remaining)
            chunk = read1(min(65_536, max_response_bytes + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
        return ServerClient._decode_bounded_json_text(
            b"".join(chunks),
            max_response_bytes=max_response_bytes,
        )

    @staticmethod
    def _decode_bounded_json_text(
        raw: bytes,
        *,
        max_response_bytes: int,
    ) -> str:
        if len(raw) > max_response_bytes:
            raise ServerClientError(
                502,
                "central SPL daemon server response exceeds the configured limit",
                code="server_response_too_large",
            )
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ServerClientError(
                502,
                "central SPL daemon server returned invalid UTF-8",
                code="server_response_invalid",
            ) from exc

    @staticmethod
    def _set_response_read_timeout(response: Any, remaining: float) -> None:
        """Apply the shrinking total budget to the underlying response socket."""

        fp = getattr(response, "fp", None)
        raw = getattr(fp, "raw", None)
        candidates = (
            response,
            fp,
            raw,
            getattr(response, "_sock", None),
            getattr(fp, "_sock", None),
            getattr(raw, "_sock", None),
        )
        for candidate in candidates:
            settimeout = getattr(candidate, "settimeout", None)
            if callable(settimeout):
                settimeout(max(0.001, remaining))
                return

    def _bytes_request(
        self,
        path: str,
        *,
        extra_headers: dict[str, str] | None = None,
        max_response_bytes: int | None = None,
    ) -> bytes:
        headers = self._headers()
        headers.update(extra_headers or {})
        request = Request(f"{self.base_url}{path}", headers=headers)
        try:
            with urlopen_verified(request, timeout=DEFAULT_FILE_TRANSFER_TIMEOUT_SECONDS) as response:
                raw = cast(
                    bytes,
                    response.read() if max_response_bytes is None else response.read(max_response_bytes + 1),
                )
                if max_response_bytes is not None and len(raw) > max_response_bytes:
                    raise ServerClientError(
                        502,
                        "central SPL daemon server response exceeds the declared artifact size",
                        code="server_response_too_large",
                    )
                return raw
        except HTTPError as exc:
            error_body = exc.read().decode("utf-8")
            message, error_code, event_id = _error_response(error_body, http_error=exc)
            message = f"central SPL daemon server returned {exc.code} at {self.base_url}{path}: {message}"
            raise ServerClientError(
                exc.code,
                message,
                code=error_code,
                event_id=event_id,
            ) from exc
        except URLError as exc:
            raise ServerClientError(
                502,
                (f"central SPL daemon server is not reachable at {self.base_url}: {exc.reason}"),
            ) from exc

    def _bytes_upload_request(
        self,
        method: str,
        path: str,
        body: bytes,
        *,
        headers: dict[str, str],
    ) -> Any:
        request_headers = {
            **self._headers(),
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(body)),
            **headers,
        }
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers=request_headers,
            method=method,
        )
        try:
            with urlopen_verified(request, timeout=DEFAULT_FILE_TRANSFER_TIMEOUT_SECONDS) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            message, error_code, event_id = _error_response(raw, http_error=exc)
            raise ServerClientError(
                exc.code,
                f"central SPL daemon server returned {exc.code} at {self.base_url}{path}: {message}",
                code=error_code,
                event_id=event_id,
            ) from exc
        except (OSError, URLError) as exc:
            reason = getattr(exc, "reason", exc)
            raise ServerClientError(
                502,
                f"central SPL daemon server is not reachable at {self.base_url}: {reason}",
            ) from exc
        return None if not raw else json.loads(raw)

    def _streaming_file_request(
        self,
        method: str,
        path: str,
        file_path: Path,
        *,
        headers: dict[str, str] | None = None,
    ) -> Any:
        request_headers = {
            **self._headers(),
            "Content-Length": str(file_path.stat().st_size),
        }
        request_headers.update(headers or {})
        with file_path.open("rb") as source:
            request = Request(
                f"{self.base_url}{path}",
                data=_file_chunks(source),
                headers=request_headers,
                method=method,
            )
            try:
                with urlopen_verified(request, timeout=DEFAULT_FILE_TRANSFER_TIMEOUT_SECONDS) as response:
                    raw = response.read().decode("utf-8")
            except HTTPError as exc:
                raw = exc.read().decode("utf-8")
                message, error_code, event_id = _error_response(raw, http_error=exc)
                raise ServerClientError(
                    exc.code,
                    (f"central SPL daemon server returned {exc.code} at {self.base_url}{path}: {message}"),
                    code=error_code,
                    event_id=event_id,
                ) from exc
            except URLError as exc:
                raise ServerClientError(
                    502,
                    (f"central SPL daemon server is not reachable at {self.base_url}: {exc.reason}"),
                ) from exc
            except OSError as exc:
                raise ServerClientError(
                    502,
                    (f"central SPL daemon server is not reachable at {self.base_url}: {exc}"),
                ) from exc

        if not raw:
            return None
        return json.loads(raw)

    def connect_machine(
        self,
        *,
        machine_id: str | None = None,
        display_name: str | None = None,
        capabilities: dict[str, Any] | None = None,
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "display_name": display_name,
            "capabilities": capabilities or {},
        }
        if machine_id is not None:
            payload["machine_id"] = machine_id
        if heartbeat_interval_seconds is not None:
            payload["heartbeat_interval_seconds"] = heartbeat_interval_seconds
        return _as_json_dict(self._json_request("POST", "/connections/connect", payload))

    def heartbeat_connection(
        self,
        *,
        connection_id: str,
        machine_id: str,
        heartbeat_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "connection_id": connection_id,
            "machine_id": machine_id,
        }
        if heartbeat_interval_seconds is not None:
            payload["heartbeat_interval_seconds"] = heartbeat_interval_seconds
        return _as_json_dict(
            self._json_request(
                "POST",
                "/connections/heartbeat",
                payload,
                allow_transport_retries=False,
            )
        )

    def current_connection(self) -> dict[str, Any]:
        # State probes have a five-second single-flight envelope. Two attempts
        # leave room for the phase backoff and the caller's probe bookkeeping.
        return _as_json_dict(
            self._json_request(
                "GET",
                "/connections/current",
                max_transport_attempts=2,
            )
        )

    def disconnect_machine(self) -> dict[str, Any]:
        return _as_json_dict(self._json_request("POST", "/connections/disconnect"))

    def list_machines(self) -> list[dict[str, Any]]:
        return _as_json_list(self._json_request("GET", "/machines"))

    def list_tokens(self) -> list[dict[str, Any]]:
        return _as_json_list(self._json_request("GET", "/tokens", auth="user"))

    def list_users(self, *, handle: str | None = None) -> list[dict[str, Any]]:
        suffix = f"?{urlencode({'handle': handle})}" if handle is not None else ""
        return _as_json_list(self._json_request("GET", f"/users{suffix}", auth="user"))

    def list_libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        query = {"include_accessible": "1" if include_accessible else "0"}
        return _as_json_list(
            self._json_request(
                "GET",
                f"/libraries?{urlencode(query)}",
                auth="user" if self.user_token else "machine",
            )
        )

    def list_owner_libraries(self, owner: str) -> list[dict[str, Any]]:
        return _as_json_list(
            self._json_request(
                "GET",
                f"/owners/{quote(owner)}/libraries",
                auth="user" if self.user_token else "machine",
            )
        )

    def create_library(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _as_json_dict(self._json_request("POST", "/libraries", payload, auth="user"))

    def get_library(self, library_ref: str, *, owner: str | None = None) -> dict[str, Any]:
        path = (
            f"/owners/{quote(owner)}/libraries/{quote(library_ref)}"
            if owner is not None
            else f"/libraries/{quote(library_ref)}"
        )
        return _as_json_dict(
            self._json_request(
                "GET",
                path,
                auth="user" if self.user_token else "machine",
            )
        )

    def update_library(self, library_ref: str, payload: dict[str, Any]) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "PUT",
                f"/libraries/{quote(library_ref)}",
                payload,
                auth="user",
            )
        )

    def delete_library(self, library_ref: str) -> dict[str, Any]:
        raise NotImplementedError(LIBRARY_DELETE_UNSUPPORTED_MESSAGE)

    def list_library_grants(
        self,
        library_ref: str,
        *,
        owner: str | None = None,
    ) -> list[dict[str, Any]]:
        suffix = f"?{urlencode({'owner': owner})}" if owner is not None else ""
        return _as_json_list(
            self._json_request(
                "GET",
                f"/libraries/{quote(library_ref)}/grants{suffix}",
                auth="user",
            )
        )

    def grant_library(self, library_ref: str, payload: dict[str, Any]) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "POST",
                f"/libraries/{quote(library_ref)}/grants",
                payload,
                auth="user",
            )
        )

    def revoke_library_grant(self, library_ref: str, grantee: str) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "POST",
                f"/libraries/{quote(library_ref)}/grants/{quote(grantee)}/revoke",
                auth="user",
            )
        )

    def add_library_reference(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "POST",
                f"/libraries/{quote(library_ref)}/references",
                payload,
                auth="user",
            )
        )

    def copy_object_into_library(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "POST",
                f"/libraries/{quote(library_ref)}/copies",
                payload,
                auth="user",
            )
        )

    def remove_library_entry(self, library_ref: str, name: str) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "DELETE",
                f"/libraries/{quote(library_ref)}/entries/{quote(name)}",
                auth="user",
            )
        )

    def preflight_library_adapter(
        self,
        owner: str,
        library: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "POST",
                f"/owners/{quote(owner)}/libraries/{quote(library)}/adapters/preflight",
                payload,
                auth="machine",
                allow_transport_retries=False,
                max_transport_attempts=1,
            )
        )

    def publish_library_adapter(
        self,
        owner: str,
        library: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        content_hash = payload.get("content_hash")
        headers = {"Idempotency-Key": f"library_adapter_{content_hash}"} if isinstance(content_hash, str) else None
        return _as_json_dict(
            self._json_request(
                "POST",
                f"/owners/{quote(owner)}/libraries/{quote(library)}/adapters",
                payload,
                auth="machine",
                extra_headers=headers,
                allow_transport_retries=False,
                max_transport_attempts=1,
            )
        )

    def list_library_adapters(
        self,
        *,
        owner: str | None = None,
        library: str | None = None,
        query: str | None = None,
        direction: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
        target_machine_id: str | None = None,
        execution_target: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, str] = {"limit": str(limit)}
        if owner is not None:
            params["owner"] = owner
        if library is not None:
            params["library"] = library
        if query is not None:
            params["q"] = query
        if direction is not None:
            params["direction"] = direction
        if cursor is not None:
            params["cursor"] = cursor
        if target_machine_id is not None:
            params["target_machine_id"] = target_machine_id
        if execution_target is not None:
            params["execution_target"] = execution_target
        return _as_json_dict(
            self._json_request(
                "GET",
                f"/library-adapters?{urlencode(params)}",
                auth="machine",
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def get_library_adapter(
        self,
        owner: str,
        library: str,
        name_or_id: str,
        *,
        target_machine_id: str | None = None,
        execution_target: str | None = None,
    ) -> dict[str, Any]:
        params = {
            key: value
            for key, value in (
                ("target_machine_id", target_machine_id),
                ("execution_target", execution_target),
            )
            if value is not None
        }
        query = f"?{urlencode(params)}" if params else ""
        return _as_json_dict(
            self._json_request(
                "GET",
                "/owners/{}/libraries/{}/adapters/{}".format(quote(owner), quote(library), quote(name_or_id)) + query,
                auth="machine",
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def list_library_adapter_versions(
        self,
        owner: str,
        library: str,
        name_or_id: str,
        *,
        limit: int = 100,
        target_machine_id: str | None = None,
        execution_target: str | None = None,
    ) -> list[dict[str, Any]]:
        params = {"limit": str(limit)}
        if target_machine_id is not None:
            params["target_machine_id"] = target_machine_id
        if execution_target is not None:
            params["execution_target"] = execution_target
        return _as_json_list(
            self._json_request(
                "GET",
                "/owners/{}/libraries/{}/adapters/{}/versions?{}".format(
                    quote(owner),
                    quote(library),
                    quote(name_or_id),
                    urlencode(params),
                ),
                auth="machine",
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def get_library_adapter_version(
        self,
        owner: str,
        library: str,
        name_or_id: str,
        adapter_version_id: str,
        *,
        include_source: bool = False,
        target_machine_id: str | None = None,
        execution_target: str | None = None,
    ) -> dict[str, Any]:
        suffix = "/source" if include_source else ""
        params = {
            key: value
            for key, value in (
                ("target_machine_id", target_machine_id),
                ("execution_target", execution_target),
            )
            if value is not None
        }
        query = f"?{urlencode(params)}" if params else ""
        return _as_json_dict(
            self._json_request(
                "GET",
                "/owners/{}/libraries/{}/adapters/{}/versions/{}{}".format(
                    quote(owner),
                    quote(library),
                    quote(name_or_id),
                    quote(adapter_version_id),
                    suffix,
                )
                + query,
                auth="machine",
                max_response_bytes=2 * MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def resolve_local_library_adapter_source(
        self,
        ref: dict[str, Any],
        *,
        target_machine_id: str,
    ) -> dict[str, Any]:
        """Resolve source for one exact remote Run ref; never retry transport."""

        return _as_json_dict(
            self._json_request(
                "POST",
                "/runtime/library-adapters/resolve-local-source",
                {
                    "schema_version": 1,
                    "ref": dict(ref),
                    "target_machine_id": target_machine_id,
                    "requester_custom_code": "allow",
                },
                auth="machine",
                max_response_bytes=2 * MAX_UPSTREAM_RESPONSE_BYTES,
                allow_transport_retries=False,
            )
        )

    def list_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        if owner_id is not None:
            query["owner"] = owner_id
        if library:
            query["library"] = library
        if compact:
            query["view"] = "summary"
        suffix = f"?{urlencode(query)}" if query else ""
        return _as_json_list(self._json_request("GET", f"/objects{suffix}"))

    def latest_machine_library_snapshot(
        self,
        machine_id: str,
        *,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        suffix = "?include_yaml=1" if include_yaml else ""
        return _as_json_dict(
            self._json_request(
                "GET",
                f"/machines/{quote(machine_id)}/library-snapshots/latest{suffix}",
                allow_transport_retries=False,
            )
        )

    def get_object(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        query = []
        if version is not None:
            query.append(f"version={int(version)}")
        if include_yaml:
            query.append("include_yaml=1")
        if owner_id is None and library:
            query.append(urlencode({"library": library}))
        suffix = f"?{'&'.join(query)}" if query else ""
        if owner_id:
            path = f"/owners/{quote(owner_id)}/libraries/{quote(library or 'default')}/objects/{quote(name_or_id)}"
        else:
            path = f"/objects/{quote(name_or_id)}"
        return _as_json_dict(self._json_request("GET", f"{path}{suffix}"))

    def object_signature(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> dict[str, Any]:
        query = []
        if version is not None:
            query.append(f"version={int(version)}")
        if function is not None:
            query.append(urlencode({"function": function}))
        if owner_id is None and library:
            query.append(urlencode({"library": library}))
        suffix = f"?{'&'.join(query)}" if query else ""
        if owner_id:
            path = (
                f"/owners/{quote(owner_id)}/libraries/"
                f"{quote(library or 'default')}/objects/{quote(name_or_id)}/signature"
            )
        else:
            path = f"/objects/{quote(name_or_id)}/signature"
        return _as_json_dict(self._json_request("GET", f"{path}{suffix}"))

    def list_object_versions(
        self,
        name_or_id: str,
        *,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        query = []
        if include_yaml:
            query.append("include_yaml=1")
        if owner_id is None and library:
            query.append(urlencode({"library": library}))
        suffix = f"?{'&'.join(query)}" if query else ""
        if owner_id:
            path = (
                f"/owners/{quote(owner_id)}/libraries/"
                f"{quote(library or 'default')}/objects/{quote(name_or_id)}/versions"
            )
        else:
            path = f"/objects/{quote(name_or_id)}/versions"
        return _as_json_list(
            self._json_request(
                "GET",
                f"{path}{suffix}",
            )
        )

    def sync(
        self,
        *,
        connection_id: str,
        machine_id: str,
        heartbeat_interval_seconds: float,
        events: list[dict[str, Any]],
        capabilities: dict[str, Any] | None = None,
        claim_id: str | None = None,
        claim_jobs: bool | None = None,
    ) -> dict[str, Any]:
        """Send one sync batch with an optional worker-attempt capability."""

        if claim_jobs is not None and not isinstance(claim_jobs, bool):
            raise ValueError("claim_jobs must be a boolean when provided")
        payload = {
            "connection_id": connection_id,
            "machine_id": machine_id,
            "heartbeat_interval_seconds": heartbeat_interval_seconds,
            "capabilities": capabilities or {},
            "events": _sync_events_for_wire(events),
        }
        if claim_jobs is not None:
            payload["claim_jobs"] = claim_jobs
        response = _as_json_dict(
            self._json_request(
                "POST",
                "/sync",
                payload,
                extra_headers=_claim_headers(claim_id),
                allow_transport_retries=False,
            )
        )
        if claim_jobs is False:
            validate_no_claim_sync_response(response)
        return response

    def create_remote_run(
        self,
        payload: dict[str, Any],
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Create one idempotent remote run with user authentication."""

        if not idempotency_key:
            raise ValueError("idempotency_key must not be empty")
        return _as_json_dict(
            self._json_request(
                "POST",
                "/remote-runs",
                payload,
                extra_headers={"Idempotency-Key": idempotency_key},
                post_send_retry_safe=True,
            )
        )

    def create_remote_run_admission(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Create or reconcile one immutable runtime-adapter admission."""

        return _as_json_dict(
            self._json_request(
                "POST",
                "/remote-run-admissions",
                payload,
                post_send_retry_safe=True,
            )
        )

    def get_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "GET",
                f"/remote-run-admissions/{quote(request_id)}",
            )
        )

    def upload_remote_run_admission_input(
        self,
        request_id: str,
        name: str,
        body: bytes,
        *,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        if len(body) != size or hashlib.sha256(body).hexdigest() != sha256:
            raise ValueError("runtime admission input bytes do not match their declaration")
        return _as_json_dict(
            self._bytes_upload_request(
                "PUT",
                f"/remote-run-admissions/{quote(request_id)}/inputs/{quote(name)}",
                body,
                headers={
                    "X-SPL-Artifact-Size": str(size),
                    "X-SPL-Artifact-Sha256": sha256,
                },
            )
        )

    def upload_remote_run_admission_custom_bundle(
        self,
        request_id: str,
        body: bytes,
        *,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        if len(body) != size or hashlib.sha256(body).hexdigest() != sha256:
            raise ValueError("runtime admission custom bundle bytes do not match their declaration")
        return _as_json_dict(
            self._bytes_upload_request(
                "PUT",
                f"/remote-run-admissions/{quote(request_id)}/custom-bundle",
                body,
                headers={
                    "X-SPL-Artifact-Size": str(size),
                    "X-SPL-Artifact-Sha256": sha256,
                },
            )
        )

    def finalize_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "POST",
                f"/remote-run-admissions/{quote(request_id)}/finalize",
                {},
                post_send_retry_safe=True,
            )
        )

    def cancel_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        return _as_json_dict(
            self._json_request(
                "POST",
                f"/remote-run-admissions/{quote(request_id)}/cancel",
                {},
                post_send_retry_safe=True,
            )
        )

    def claimed_runtime_input_bytes(
        self,
        download_url: str,
        *,
        claim_id: str,
        expected_size: int,
        expected_sha256: str,
    ) -> bytes:
        """Download one claim-fenced input from this exact server origin."""

        parsed = urlsplit(download_url)
        if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("runtime input download_url must be a relative server path")
        path = parsed.path
        parts = path.removeprefix("/").split("/")
        safe_parts = all(part and part not in {".", ".."} and "%" not in part and "\\" not in part for part in parts)
        valid_namespace = (len(parts) == 4 and parts[0] == "remote-runs" and parts[2] == "inputs") or (
            len(parts) == 3 and parts[0] == "remote-runs" and parts[2] == "custom-bundle"
        )
        if not safe_parts or not valid_namespace:
            raise ValueError("runtime input download_url is outside the claim input namespace")
        body = self._bytes_request(
            path,
            extra_headers=_claim_headers(claim_id),
            max_response_bytes=expected_size,
        )
        if len(body) != expected_size or hashlib.sha256(body).hexdigest() != expected_sha256:
            raise ServerClientError(
                502,
                "claimed runtime input failed size/checksum verification",
                code="runtime_input_integrity_invalid",
            )
        return body

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        # A remote Run is created on behalf of the requesting user and may
        # target a different owner's granted Machine.  Polling it with the
        # companion's machine credential incorrectly binds the read to that
        # local Machine and makes legitimate cross-Machine Runs fail with a
        # machine-subject mismatch.  The central read route is user-scoped,
        # just like the bounded list/detail/event façades.
        return _as_json_dict(
            self._json_request(
                "GET",
                f"/remote-runs/{quote(run_id)}",
                auth="user",
            )
        )

    def list_remote_runs(self) -> list[dict[str, Any]]:
        """Return the current user's remote Runs through a bounded read."""

        return _require_json_dict_list(
            self._json_request(
                "GET",
                "/remote-runs",
                auth="user",
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def get_remote_run_detail(self, run_id: str) -> dict[str, Any]:
        """Return one authorized remote Run detail snapshot."""

        return _require_json_dict(
            self._json_request(
                "GET",
                f"/remote-runs/{quote(run_id)}/detail",
                auth="user",
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def list_remote_run_events(self, run_id: str) -> list[dict[str, Any]]:
        """Return one authorized remote Run event snapshot."""

        return _require_json_dict_list(
            self._json_request(
                "GET",
                f"/remote-runs/{quote(run_id)}/events",
                auth="user",
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def get_server_version(self) -> dict[str, Any]:
        """Return bounded non-secret server release/capability evidence."""

        return _require_json_dict(
            self._json_request(
                "GET",
                "/version",
                auth="user",
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def object_profile_status(
        self,
        name_or_id: str,
        *,
        library: str | None = None,
    ) -> dict[str, Any]:
        params = {"library": library} if library is not None else {}
        suffix = f"?{urlencode(params)}" if params else ""
        return _require_json_dict(
            self._json_request(
                "GET",
                f"/objects/{quote(name_or_id)}/public-status{suffix}",
                auth="machine",
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def update_object_profile(
        self,
        name_or_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return _require_json_dict(
            self._json_request(
                "PATCH",
                f"/objects/{quote(name_or_id)}/profile",
                payload,
                auth="machine",
                allow_transport_retries=False,
                max_transport_attempts=1,
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def preflight_public_object(
        self,
        name_or_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return _require_json_dict(
            self._json_request(
                "POST",
                f"/objects/{quote(name_or_id)}/public-preflight",
                payload,
                auth="machine",
                post_send_retry_safe=True,
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def library_adapter_profile_status(self, adapter_id: str) -> dict[str, Any]:
        return _require_json_dict(
            self._json_request(
                "GET",
                f"/library-adapters/{quote(adapter_id)}/public-status",
                auth="machine",
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def update_library_adapter_profile(
        self,
        adapter_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return _require_json_dict(
            self._json_request(
                "PATCH",
                f"/library-adapters/{quote(adapter_id)}/profile",
                payload,
                auth="machine",
                allow_transport_retries=False,
                max_transport_attempts=1,
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def preflight_remote_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Evaluate one read-only, non-reserving remote execution preflight."""

        return _require_json_dict(
            self._json_request(
                "POST",
                "/remote-runs/preflight",
                payload,
                auth="user",
                post_send_retry_safe=True,
                max_response_bytes=MAX_UPSTREAM_RESPONSE_BYTES,
            )
        )

    def get_ai_preview_capabilities(self) -> dict[str, Any]:
        """Return the central server's closed AI Preview capability document."""

        return _require_json_dict(
            self._json_request(
                "GET",
                "/ai/preview/capabilities",
                auth="user",
                allow_transport_retries=False,
                max_transport_attempts=1,
                max_response_bytes=MAX_AI_PREVIEW_RESPONSE_BYTES,
                absolute_deadline_seconds=self.request_timeout_seconds,
            )
        )

    def get_ai_assistant_capabilities(self) -> dict[str, Any]:
        """Return the central server's closed AI assistant capabilities."""

        return _require_json_dict(
            self._json_request(
                "GET",
                AI_ASSISTANT_UPSTREAM_CAPABILITIES_ROUTE,
                auth="user",
                allow_transport_retries=False,
                max_transport_attempts=1,
                max_response_bytes=MAX_AI_ASSISTANT_RESPONSE_BYTES,
                absolute_deadline_seconds=self.request_timeout_seconds,
            )
        )

    def ai_preview_request_contains_configured_credential(
        self,
        payload: dict[str, Any],
    ) -> bool:
        """Check notebook evidence against this client's opaque credentials.

        Credential values remain inside the authenticated daemon client;
        callers receive only the boolean result and cannot retrieve a token
        through this seam.
        """

        credentials = tuple(
            value for value in (self.machine_token, self.user_token) if isinstance(value, str) and value
        )

        def contains(value: Any) -> bool:
            if isinstance(value, str):
                return any(credential in value for credential in credentials)
            if isinstance(value, list):
                return any(contains(item) for item in value)
            if isinstance(value, dict):
                return any(contains(item) for item in value.values())
            return False

        return contains(payload)

    def create_ai_preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Submit one non-replayed synchronous AI Preview request.

        A timeout or dropped response can occur after the central server has
        reached the provider.  The operation therefore permits exactly one
        transport attempt and never infers that a retry would be safe.
        """

        return _require_json_dict(
            self._json_request(
                "POST",
                "/ai/preview",
                payload,
                auth="user",
                allow_transport_retries=False,
                max_transport_attempts=1,
                max_response_bytes=MAX_AI_PREVIEW_RESPONSE_BYTES,
                compact_json=True,
                absolute_deadline_seconds=self.request_timeout_seconds,
            )
        )

    def create_ai_assistant(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Submit one non-replayed synchronous AI assistant request."""

        return _require_json_dict(
            self._json_request(
                "POST",
                AI_ASSISTANT_UPSTREAM_ROUTE,
                payload,
                auth="user",
                allow_transport_retries=False,
                max_transport_attempts=1,
                max_response_bytes=MAX_AI_ASSISTANT_RESPONSE_BYTES,
                compact_json=True,
                absolute_deadline_seconds=self.request_timeout_seconds,
            )
        )

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        return _as_json_list(self._json_request("GET", f"/remote-runs/{quote(run_id)}/artifacts"))

    def upload_artifact(
        self,
        run_id: str,
        name: str,
        path: str | Path,
        *,
        claim_id: str | None = None,
    ) -> dict[str, Any]:
        """Upload an artifact under an optional worker-attempt capability."""

        artifact_path = Path(path)
        return _as_json_dict(
            self._streaming_file_request(
                "PUT",
                f"/remote-runs/{quote(run_id)}/artifacts/{quote(name)}",
                artifact_path,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-SPL-Artifact-Sha256": _file_sha256(artifact_path),
                    "X-SPL-Artifact-Size": str(artifact_path.stat().st_size),
                    **_claim_headers(claim_id),
                },
            )
        )

    def artifact_bytes(self, run_id: str, name: str) -> bytes:
        return self._bytes_request(f"/remote-runs/{quote(run_id)}/artifacts/{quote(name)}")

    def artifact_bytes_verified(
        self,
        run_id: str,
        name: str,
        *,
        size: int,
        sha256: str,
    ) -> bytes:
        """Download one artifact under an exact size and digest fence.

        This additive helper is used by browser-safe brokerage.  The existing
        unrestricted trusted SDK method above remains unchanged.
        """

        if type(size) is not int or size < 0:
            raise ValueError("artifact size must be a non-negative integer")
        if not isinstance(sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise ValueError("artifact sha256 must be lowercase hexadecimal")
        data = self._bytes_request(
            f"/remote-runs/{quote(run_id)}/artifacts/{quote(name)}",
            max_response_bytes=size,
        )
        if len(data) != size or hashlib.sha256(data).hexdigest() != sha256:
            raise ServerClientError(
                502,
                "central SPL daemon server artifact differs from its retained metadata",
                code="server_response_invalid",
            )
        return data

    def download_artifact(self, run_id: str, name: str, target: str | Path) -> Path:
        target_path = Path(target)
        if target_path.is_dir():
            target_path = target_path / name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(self.artifact_bytes(run_id, name))
        return target_path


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
