"""Client helpers for talking to the central SPL daemon server."""

from __future__ import annotations

import hashlib
import json
import socket
import ssl
import time
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Literal, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request

from spl._http import (
    ConnectionPhaseError,
    DEFAULT_FILE_TRANSFER_TIMEOUT_SECONDS,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    urlopen_verified,
)
from spl.core import json_contract as m_json_contract

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
RUN_CLAIM_HEADER = "X-Spl-Claim"
RUN_CLAIM_PRIVATE_FIELD = "_spl_claim_id"
STALE_RUN_CLAIM_ERROR_CODE = "stale_run_claim"
SYNC_EVENT_IDENTITY_COLLISION_ERROR_CODE = "sync_event_identity_collision"
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
    ) -> None:
        self.status_code = status_code
        self.message = message
        self.code = code
        self.event_id = event_id
        super().__init__(f"{status_code}: {message}")


def is_stale_run_claim_error(exc: ServerClientError) -> bool:
    """Return whether the server rejected a superseded worker attempt."""

    return exc.status_code == 409 and exc.code == STALE_RUN_CLAIM_ERROR_CODE


def is_sync_event_identity_collision_error(exc: ServerClientError) -> bool:
    """Return whether one exact queued event identity collided at the server."""

    return exc.status_code == 409 and exc.code == SYNC_EVENT_IDENTITY_COLLISION_ERROR_CODE and bool(exc.event_id)


def _as_json_dict(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value)


def _as_json_list(value: Any) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], value)


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

    error = payload.get("error", raw)
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
    ) -> Any:
        if max_transport_attempts < 1:
            raise ValueError("max_transport_attempts must be at least 1")
        body = None
        headers = self._headers(auth=auth)
        headers.update(extra_headers or {})
        if payload is not None:
            body = m_json_contract.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=False,
                separators=None,
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
        while attempt < max_transport_attempts:
            attempt += 1
            try:
                with urlopen_verified(request, timeout=self.request_timeout_seconds) as response:
                    raw = response.read().decode("utf-8")
            except HTTPError as exc:
                raw = exc.read().decode("utf-8")
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
        return json.loads(raw)

    def _bytes_request(self, path: str) -> bytes:
        request = Request(f"{self.base_url}{path}", headers=self._headers())
        try:
            with urlopen_verified(request, timeout=DEFAULT_FILE_TRANSFER_TIMEOUT_SECONDS) as response:
                return cast(bytes, response.read())
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            message, error_code, event_id = _error_response(raw, http_error=exc)
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
    ) -> dict[str, Any]:
        """Send one sync batch with an optional worker-attempt capability."""

        return _as_json_dict(
            self._json_request(
                "POST",
                "/sync",
                {
                    "connection_id": connection_id,
                    "machine_id": machine_id,
                    "heartbeat_interval_seconds": heartbeat_interval_seconds,
                    "capabilities": capabilities or {},
                    "events": _sync_events_for_wire(events),
                },
                extra_headers=_claim_headers(claim_id),
                allow_transport_retries=False,
            )
        )

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

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        return _as_json_dict(self._json_request("GET", f"/remote-runs/{quote(run_id)}"))

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
