"""Advanced/internal HTTP client for the local SPL daemon.

Most user code should go through ``SPLClient``.  This client intentionally
mirrors the daemon's minimal API for advanced integrations and internal
plumbing, while still using only the standard library.
"""

from __future__ import annotations

import json
import os
import secrets
import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal, TextIO, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

DEFAULT_DAEMON_HOST = "127.0.0.1"
DEFAULT_DAEMON_PORT = 8765
DEFAULT_URL = f"http://{DEFAULT_DAEMON_HOST}:{DEFAULT_DAEMON_PORT}"
DEFAULT_SERVER_URL = "https://splime.io/api"
DEFAULT_HTTP_TIMEOUT_SECONDS = 60.0
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
DAEMON_ENDPOINT_FILENAME = "daemon-endpoint.json"
DAEMON_API_TOKEN_ENV = "SPL_DAEMON_API_TOKEN"
LIBRARY_DELETE_UNSUPPORTED_MESSAGE = (
    "Deleting central-server libraries is not supported by the SPL server API. "
    "Use the Console archive action to hide a library, or remove individual "
    "entries with client.library.remove_entry()."
)

OfflinePolicy = Literal["queue", "wait", "fail_fast"]
RunSource = Literal["auto", "local"]


def default_daemon_home() -> Path:
    """Return the default local daemon data directory."""

    return Path(os.environ.get("SPL_DAEMON_HOME", Path.home() / ".spl-daemon"))


def daemon_url(
    host: str = DEFAULT_DAEMON_HOST,
    port: int = DEFAULT_DAEMON_PORT,
) -> str:
    """Build a local daemon HTTP URL from host and port."""

    if port < 1 or port > 65535:
        raise ValueError("daemon port must be between 1 and 65535")
    host = host.strip()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{port}"


def daemon_endpoint_file(home: str | Path | None = None) -> Path:
    """Return the file where the running daemon publishes its current URL."""

    if home is not None:
        return Path(home).absolute() / DAEMON_ENDPOINT_FILENAME
    return default_daemon_home() / DAEMON_ENDPOINT_FILENAME


def read_daemon_endpoint(home: str | Path | None = None) -> dict[str, Any] | None:
    """Read the last endpoint published by a running local daemon."""

    path = daemon_endpoint_file(home)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _write_json_file(path: Path, value: dict[str, Any]) -> None:
    """Write a small JSON document atomically enough for local daemon metadata."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    try:
        tmp_path.chmod(0o600)
    except OSError:
        pass
    tmp_path.replace(path)
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _as_json_dict(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value)


def _as_json_dict_list(value: Any) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], value)


def _as_json_str_list(value: Any) -> list[str]:
    return cast(list[str], value)


def _as_object_listing(value: Any) -> dict[str, Any] | list[dict[str, Any]]:
    return cast(dict[str, Any] | list[dict[str, Any]], value)


def _record_string(value: Any, *keys: str) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, Mapping):
        for key in keys:
            item = value.get(key)
            if item is not None and str(item):
                return str(item)
    return None


def _server_record_name(record: Mapping[str, Any]) -> str | None:
    return (
        _record_string(record.get("name"))
        or _record_string(record.get("display_name"))
        or _record_string(record.get("object_name"))
    )


def _server_record_owner(record: Mapping[str, Any]) -> str | None:
    return _record_string(record.get("owner_id")) or _record_string(record.get("owner"), "id", "owner_id", "name")


def _server_record_library(record: Mapping[str, Any]) -> str | None:
    return _record_string(record.get("library"), "slug", "name", "display_name")


def _server_record_version(record: Mapping[str, Any]) -> str | None:
    current = record.get("current_version")
    if isinstance(current, Mapping):
        for key in ("version", "number", "label", "name"):
            value = current.get(key)
            if value is not None and str(value):
                return str(value)
    for key in ("version", "version_label"):
        value = record.get(key)
        if value is not None and str(value):
            return str(value)
    return None


def _server_record_canonical(
    record: Mapping[str, Any],
    *,
    fallback_owner_id: str | None = None,
    fallback_library: str | None = None,
) -> str | None:
    name = _server_record_name(record)
    if name is None:
        return None
    owner_id = _server_record_owner(record) or fallback_owner_id or "unknown"
    library = _server_record_library(record) or fallback_library or "default"
    return f"{owner_id}/{library}/{name}"


def _server_record_ref(
    record: Mapping[str, Any],
    *,
    fallback_owner_id: str | None = None,
    fallback_library: str | None = None,
) -> str:
    canonical = _server_record_canonical(
        record,
        fallback_owner_id=fallback_owner_id,
        fallback_library=fallback_library,
    )
    if canonical is None:
        return str(record.get("id") or "<unknown server object>")
    version = _server_record_version(record)
    return "{}@v{}".format(canonical, version) if version is not None else canonical


def _object_records(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return [item for item in value.values() if isinstance(item, Mapping)]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _identity_buckets(value: Any) -> dict[str, set[str]]:
    buckets: dict[str, set[str]] = {}
    for record in _object_records(value):
        name = _record_string(record.get("name"))
        if name is None:
            continue
        canonical = _record_string(record.get("canonical_name")) or _server_record_canonical(record)
        if canonical is None:
            continue
        buckets.setdefault(name, set()).add(canonical)
    return buckets


def _add_projected_identity(
    buckets: dict[str, set[str]],
    record: Mapping[str, Any],
    *,
    fallback_owner_id: str | None = None,
    fallback_library: str | None = None,
) -> str | None:
    name = _server_record_name(record)
    canonical = _server_record_canonical(
        record,
        fallback_owner_id=fallback_owner_id,
        fallback_library=fallback_library,
    )
    if name is None or canonical is None:
        return None
    buckets.setdefault(name, set()).add(canonical)
    return name


def generate_daemon_api_token() -> str:
    """Generate the local daemon HTTP API bearer token."""

    return secrets.token_urlsafe(32)


def write_daemon_endpoint(
    home: str | Path | None,
    *,
    bind_host: str,
    host: str,
    port: int,
    api_token: str | None = None,
    updated_at: str | None = None,
) -> dict[str, Any]:
    """Publish the current local daemon URL for clients started later."""

    base_url = daemon_url(host, port)
    payload = {
        "base_url": base_url,
        "bind_host": bind_host,
        "host": host,
        "port": port,
    }
    if api_token is not None:
        payload["api_token"] = api_token
    if updated_at is not None:
        payload["updated_at"] = updated_at
    _write_json_file(daemon_endpoint_file(home), payload)
    return payload


def resolve_api_token(
    base_url: str | None = None,
    *,
    daemon_home: str | Path | None = None,
    explicit_token: str | None = None,
) -> str | None:
    """Resolve the local daemon API bearer token for official clients."""

    if explicit_token:
        return explicit_token
    env_token = os.environ.get(DAEMON_API_TOKEN_ENV)
    if env_token:
        return env_token

    endpoint = read_daemon_endpoint(daemon_home)
    if endpoint is None:
        return None
    endpoint_token = endpoint.get("api_token")
    if not isinstance(endpoint_token, str) or not endpoint_token:
        return None
    if base_url is None:
        return endpoint_token
    endpoint_url = endpoint.get("base_url")
    if isinstance(endpoint_url, str) and endpoint_url.rstrip("/") == base_url.rstrip("/"):
        return endpoint_token
    return None


def clear_daemon_endpoint(
    home: str | Path | None,
    *,
    base_url: str | None = None,
) -> None:
    """Remove a daemon endpoint file when it belongs to the stopping daemon."""

    path = daemon_endpoint_file(home)
    if base_url is not None:
        current = read_daemon_endpoint(home)
        if current is None or current.get("base_url") != base_url.rstrip("/"):
            return
    try:
        path.unlink()
    except FileNotFoundError:
        return


def resolve_base_url(
    base_url: str | None = None,
    *,
    daemon_host: str = DEFAULT_DAEMON_HOST,
    daemon_port: int | None = None,
    daemon_home: str | Path | None = None,
) -> str:
    """Resolve explicit URL, explicit port, saved daemon endpoint, or default."""

    if base_url is not None:
        if daemon_port is not None:
            raise ValueError("pass either base_url or daemon_port, not both")
        return base_url.rstrip("/")
    if daemon_port is not None:
        return daemon_url(daemon_host, daemon_port)

    endpoint = read_daemon_endpoint(daemon_home)
    if endpoint is not None:
        saved_url = endpoint.get("base_url")
        if isinstance(saved_url, str) and saved_url.strip():
            return saved_url.rstrip("/")
        host = endpoint.get("host")
        port = endpoint.get("port")
        if isinstance(host, str) and isinstance(port, int):
            return daemon_url(host, port)

    return DEFAULT_URL


class ClientError(RuntimeError):
    """Raised when the daemon returns an error response."""

    status_code: int | None
    payload: dict[str, Any] | None
    code: str | None

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload
        code = payload.get("code") if payload is not None else None
        self.code = str(code) if code is not None else None


def _client_error_from_http_error(exc: HTTPError, raw: str) -> ClientError:
    payload: dict[str, Any] | None = None
    message = raw
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        pass
    else:
        if isinstance(decoded, dict):
            payload = decoded
            message = str(decoded.get("error", raw))
    return ClientError(f"{exc.code}: {message}", status_code=exc.code, payload=payload)


RunStateCallback = Callable[[dict[str, Any]], None]

_ENVIRONMENT_BUILD_PHASE = "environment_build"
_SLOW_WAIT_STATUSES = frozenset({"queued", "starting", "preparing_environment"})
_OBSERVABILITY_PREVIEW_LIMIT = 3


def _format_duration(seconds: float) -> str:
    """Format elapsed seconds as a short human-readable duration."""

    total = int(max(seconds, 0))
    if total < 60:
        return f"{total}s"
    minutes, remainder = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {remainder:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def _progress_rows(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _short(value: Any, *, width: int = 12) -> str:
    text = "" if value is None else str(value)
    return text[:width] if len(text) > width else text


def _append_remaining(items: list[str], total: int) -> list[str]:
    remaining = total - len(items)
    if remaining > 0:
        return [*items, f"+{remaining} more"]
    return items


def _runtime_resolved_label(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    for key in ("image_tag", "python"):
        resolved = value.get(key)
        if isinstance(resolved, str) and resolved:
            return "{}={}".format(key, resolved)
    parts = [
        "{}={}".format(key, item)
        for key, item in sorted(value.items())
        if isinstance(key, str) and isinstance(item, str) and item
    ]
    return ", ".join(parts) if parts else None


class RunProgressPrinter:
    """Print progress lines for run phases that can stay silent for minutes.

    The printer is a ``RunStateCallback`` for the ``wait_run`` and
    ``wait_remote_run`` polling loops.  It stays quiet for fast runs and only
    speaks up in two situations:

    * the daemon reports an environment build in progress for the run
      (``state["environment"]["status"] == "creating"``) — the first run of an
      object builds a fresh venv or image, which can take minutes;
    * the run stays in one waiting status (``queued``, ``starting``,
      ``preparing_environment``) longer than ``interval_seconds``.
    * the daemon reports resolved edge adapters or node runtimes through
      ``state["run_progress"]``.

    Repeated messages are throttled to one line per ``interval_seconds``, and
    the printer never raises: progress output must not abort a wait loop.
    """

    _LOG_TAIL_LIMIT = 120

    def __init__(
        self,
        *,
        stream: TextIO | None = None,
        interval_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        label: str = "[spl]",
    ):
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._stream = stream
        self._interval = float(interval_seconds)
        self._clock = clock
        self._label = label
        self._phase: str | None = None
        self._phase_started_at = 0.0
        self._last_printed_at: float | None = None
        self._announced_build = False
        self._announced_observability = False

    def __call__(self, state: Mapping[str, Any]) -> None:
        try:
            self._observe(state)
        except Exception:  # noqa: BLE001 - progress output must never raise.
            return

    def _observe(self, state: Mapping[str, Any]) -> None:
        status = str(state.get("status") or "")
        environment = state.get("environment")
        if not isinstance(environment, Mapping):
            environment = {}
        now = self._clock()

        phase = self._phase_for(status, environment)
        if phase != self._phase:
            self._announce_build_finished(status)
            self._phase = phase
            self._phase_started_at = now
            self._last_printed_at = None
        self._announce_observability(state)
        if phase is None:
            return

        building = phase == _ENVIRONMENT_BUILD_PHASE
        if not building and now - self._phase_started_at < self._interval:
            return
        if self._last_printed_at is not None and now - self._last_printed_at < self._interval:
            return

        self._last_printed_at = now
        if building:
            self._announced_build = True
            self._print(self._build_message(environment, now))
        else:
            self._print(self._waiting_message(status, now))

    def _phase_for(self, status: str, environment: Mapping[str, Any]) -> str | None:
        if environment.get("status") == "creating":
            return _ENVIRONMENT_BUILD_PHASE
        if status in _SLOW_WAIT_STATUSES:
            return f"waiting:{status}"
        return None

    def _build_message(self, environment: Mapping[str, Any], now: float) -> str:
        elapsed = environment.get("elapsed_seconds")
        if not isinstance(elapsed, int | float):
            elapsed = now - self._phase_started_at
        runtime_type = str(environment.get("runtime_type") or "venv")
        message = (
            f"{self._label} building the {runtime_type} environment "
            f"({_format_duration(elapsed)}; a first run can take minutes)"
        )
        log_tail = environment.get("log_tail")
        if isinstance(log_tail, str) and log_tail.strip():
            message += f": {log_tail.strip()[: self._LOG_TAIL_LIMIT]}"
        return message

    def _waiting_message(self, status: str, now: float) -> str:
        elapsed = _format_duration(now - self._phase_started_at)
        phase_name = status.replace("_", " ")
        return f"{self._label} run is still {phase_name} after {elapsed}"

    def _announce_build_finished(self, status: str) -> None:
        if not self._announced_build:
            return
        self._announced_build = False
        if status == "running":
            self._print(f"{self._label} environment is ready; running")

    def _announce_observability(self, state: Mapping[str, Any]) -> None:
        if self._announced_observability:
            return
        progress = state.get("run_progress")
        if not isinstance(progress, Mapping):
            return
        message = self._observability_message(progress)
        if message is None:
            return
        self._announced_observability = True
        self._print(message)

    def _observability_message(self, progress: Mapping[str, Any]) -> str | None:
        sections = []
        runtimes = self._runtime_resolution_summary(progress.get("node_runtimes"))
        edges = self._edge_resolution_summary(progress.get("edge_adapters"))
        if runtimes:
            sections.append("runtimes: {}".format(", ".join(runtimes)))
        if edges:
            sections.append("edges: {}".format(", ".join(edges)))
        if not sections:
            return None
        return "{} resolved {}".format(self._label, "; ".join(sections))

    def _runtime_resolution_summary(self, value: Any) -> list[str]:
        rows = _progress_rows(value)
        items = []
        for item in rows[:_OBSERVABILITY_PREVIEW_LIMIT]:
            node = item.get("alias") or _short(item.get("node_id"))
            runtime = item.get("name")
            source = item.get("source")
            summary = "{}={}/{}".format(node or "node", runtime or "runtime", source or "unknown")
            resolved = _runtime_resolved_label(item.get("resolved"))
            if resolved is not None:
                summary = "{} ({})".format(summary, resolved)
            items.append(summary)
        return _append_remaining(items, len(rows))

    def _edge_resolution_summary(self, value: Any) -> list[str]:
        rows = _progress_rows(value)
        items = []
        for item in rows[:_OBSERVABILITY_PREVIEW_LIMIT]:
            edge = "{} -> {}".format(item.get("source") or "source", item.get("target") or "target")
            tag = item.get("tag") or "unknown"
            adapter = item.get("save") or item.get("load") or "adapter"
            source = item.get("source_level") or "unknown"
            items.append("{} tag={} adapter={} source={}".format(edge, tag, adapter, source))
        return _append_remaining(items, len(rows))

    def _print(self, message: str) -> None:
        stream = self._stream if self._stream is not None else sys.stderr
        print(message, file=stream, flush=True)


class PullProgressPrinter:
    """Print throttled pull-all progress lines to stderr."""

    def __init__(
        self,
        *,
        stream: TextIO | None = None,
        interval_seconds: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        label: str = "[spl]",
    ):
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._stream = stream
        self._interval = float(interval_seconds)
        self._clock = clock
        self._label = label
        self._last_printed_at: float | None = None

    def __call__(self, current: int, total: int, ref: str) -> None:
        try:
            self._observe(current, total, ref)
        except Exception:  # noqa: BLE001 - progress output must never raise.
            return

    def _observe(self, current: int, total: int, ref: str) -> None:
        now = self._clock()
        if current != 1 and current != total and self._last_printed_at is not None:
            if now - self._last_printed_at < self._interval:
                return
        self._last_printed_at = now
        stream = self._stream if self._stream is not None else sys.stderr
        print(f"{self._label} pull_all {current}/{total}: {ref}", file=stream, flush=True)


class Client:
    """Advanced/internal wrapper around the local daemon HTTP API."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        daemon_host: str = DEFAULT_DAEMON_HOST,
        daemon_port: int | None = None,
        daemon_home: str | Path | None = None,
        api_token: str | None = None,
    ):
        self.base_url = resolve_base_url(
            base_url,
            daemon_host=daemon_host,
            daemon_port=daemon_port,
            daemon_home=daemon_home,
        )
        self.api_token = resolve_api_token(
            self.base_url,
            daemon_home=daemon_home,
            explicit_token=api_token,
        )

    def _headers(self, *, accept_json: bool = True) -> dict[str, str]:
        headers: dict[str, str] = {}
        if accept_json:
            headers["Accept"] = "application/json"
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def _local_daemon_unreachable(self, exc: URLError) -> ClientError:
        """Build a clear error for the common "daemon is not running" case."""

        reason = getattr(exc, "reason", exc)
        return ClientError(
            f"local SPL daemon is not reachable at {self.base_url} ({reason}). "
            "This URL points to the local daemon, not to the central daemon server. "
            "Start it with "
            "`python -m spl.daemon serve` "
            "or pass the correct `base_url`/`daemon_port` to SPLClient."
        )

    def _json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout: float | None = DEFAULT_HTTP_TIMEOUT_SECONDS,
    ) -> Any:
        """Send one JSON request and decode the JSON response.

        Control-plane calls use the 60 second default.  Blocking execution
        calls pass ``None`` here when the user did not set ``timeout_seconds``
        because urllib cannot split connect and read timeouts.
        """

        body = None
        headers = self._headers()
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"

        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )

        try:
            with urlopen(request, timeout=timeout) as response:  # noqa: S310 - local daemon URL.
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            raise _client_error_from_http_error(exc, raw) from exc
        except URLError as exc:
            raise self._local_daemon_unreachable(exc) from exc

        if not raw:
            return None
        return json.loads(raw)

    def _bytes_request(self, path: str) -> bytes:
        """Download a binary response, used for artifact files."""

        request = Request(
            f"{self.base_url}{path}",
            headers=self._headers(accept_json=False),
            method="GET",
        )
        try:
            with urlopen(request, timeout=DEFAULT_HTTP_TIMEOUT_SECONDS) as response:  # noqa: S310 - local daemon URL.
                return cast(bytes, response.read())
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            raise _client_error_from_http_error(exc, raw) from exc
        except URLError as exc:
            raise self._local_daemon_unreachable(exc) from exc

    def health(self) -> dict[str, Any]:
        """Check that the daemon is reachable."""

        return _as_json_dict(self._json_request("GET", "/health"))

    def connect_server(
        self,
        *,
        machine_token: str,
        user_token: str,
        server_url: str = DEFAULT_SERVER_URL,
        machine_id: str | None = None,
        display_name: str | None = None,
        capabilities: dict[str, Any] | None = None,
        heartbeat_interval_seconds: float | None = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> dict[str, Any]:
        """Ask the local daemon to connect to the central daemon server."""

        payload: dict[str, Any] = {
            "machine_token": machine_token,
            "user_token": user_token,
            "server_url": server_url,
            "display_name": display_name,
            "capabilities": capabilities or {},
        }
        if machine_id is not None:
            payload["machine_id"] = machine_id
        if heartbeat_interval_seconds is not None:
            payload["heartbeat_interval_seconds"] = heartbeat_interval_seconds
        return _as_json_dict(self._json_request("POST", "/server/connect", payload))

    def disconnect_server(self) -> dict[str, Any]:
        """Ask the local daemon to gracefully disconnect from the central server."""

        return _as_json_dict(self._json_request("POST", "/server/disconnect"))

    def server_connection(self, *, probe: bool = True) -> dict[str, Any]:
        """Return central-server state, probing an open breaker by default."""

        path = "/server/connection" if probe else "/server/connection?probe=0"
        return _as_json_dict(self._json_request("GET", path))

    def sync_status(self) -> dict[str, Any]:
        """Return sync-queue counts and heartbeat liveness diagnostics."""

        return _as_json_dict(self._json_request("GET", "/server/sync/status"))

    def prune_sync_events(
        self,
        *,
        status: str,
        older_than_days: int = 0,
        include_protected: bool = False,
        limit: int = 1_000,
    ) -> dict[str, Any]:
        """Prune a bounded queue slice; non-telemetry rows need opt-in."""

        query = urlencode(
            {
                "status": status,
                "older_than_days": str(older_than_days),
                "include_protected": "1" if include_protected else "0",
                "limit": str(limit),
            }
        )
        return _as_json_dict(self._json_request("POST", f"/server/sync/prune?{query}"))

    def server_connections(self) -> list[dict[str, Any]]:
        """Return stored central-server connection attempts."""

        return _as_json_dict_list(self._json_request("GET", "/server/connections"))

    def prune_server_connections(
        self,
        *,
        older_than_days: int = 30,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Prune stale stored central-server connection attempts."""

        query = urlencode(
            {
                "older_than_days": str(older_than_days),
                "dry_run": "1" if dry_run else "0",
            }
        )
        return _as_json_dict(self._json_request("POST", f"/server/connections/prune?{query}"))

    def server_machines(self) -> dict[str, Any]:
        """Return machines visible to the connected user."""

        return _as_json_dict(self._json_request("GET", "/server/machines"))

    def server_users(self, *, handle: str | None = None) -> list[dict[str, Any]]:
        """Return the connected server's email-free user directory."""

        suffix = f"?{urlencode({'handle': handle})}" if handle is not None else ""
        return _as_json_dict_list(self._json_request("GET", f"/server/users{suffix}"))

    def server_whoami(self) -> dict[str, Any]:
        """Return the daemon's connected or cached canonical user identity."""

        return _as_json_dict(self._json_request("GET", "/server/whoami"))

    def server_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        """Return objects visible on the connected central server."""

        query = []
        if owner_id is not None:
            query.append(f"owner={quote(owner_id)}")
        if library is not None:
            query.append(f"library={quote(library)}")
        if compact:
            query.append("view=summary")
        suffix = f"?{'&'.join(query)}" if query else ""
        return _as_json_dict_list(self._json_request("GET", f"/server/objects{suffix}"))

    def pull_server_object(
        self,
        name: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        version: int | None = None,
        all_versions: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Mirror one accessible server object into the local registry.

        Repeat calls are idempotent by content hash. ``dry_run`` reports what
        would be pulled or skipped without writing mirror rows or YAML bodies.
        """

        payload: dict[str, Any] = {
            "name": name,
            "all_versions": all_versions,
        }
        if owner_id is not None:
            payload["owner_id"] = owner_id
        if library is not None:
            payload["library"] = library
        if version is not None:
            payload["version"] = version
        if dry_run:
            payload["dry_run"] = True
        return _as_json_dict(self._json_request("POST", "/server-objects/pull", payload))

    def pull_all_server_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        all_versions: bool = False,
        dry_run: bool = False,
        progress: bool | Callable[[int, int, str], None] = True,
    ) -> dict[str, Any]:
        """Mirror the visible server catalog into the local registry.

        Full-catalog pulls can be large. Prefer ``dry_run=True`` first and use
        owner/library filters when possible; failures for individual objects are
        collected in the receipt and do not roll back earlier imports.
        """

        catalog = self.server_objects(owner_id=owner_id, library=library, compact=True)
        progress_callback: Callable[[int, int, str], None] | None
        if callable(progress):
            progress_callback = progress
        elif progress:
            progress_callback = PullProgressPrinter()
        else:
            progress_callback = None

        pulled: list[str] = []
        skipped: list[str] = []
        failed: list[dict[str, str]] = []
        seen_names: set[str] = set()
        projected_buckets = _identity_buckets(self.list_objects(compact=True)) if dry_run else {}

        total = len(catalog)
        for index, record in enumerate(catalog, start=1):
            ref = _server_record_ref(record, fallback_owner_id=owner_id, fallback_library=library)
            try:
                name = _server_record_name(record)
                if name is None:
                    raise ValueError("server catalog record has no object name")
                record_owner_id = _server_record_owner(record) or owner_id
                record_library = _server_record_library(record) or library
                result = self.pull_server_object(
                    name,
                    owner_id=record_owner_id,
                    library=record_library,
                    all_versions=all_versions,
                    dry_run=dry_run,
                )
                pulled.extend(str(item) for item in result.get("pulled") or [])
                skipped.extend(str(item) for item in result.get("skipped") or [])
                for failed_ref in result.get("failed") or []:
                    failed.append({"ref": str(failed_ref), "reason": "object pull failed"})
                seen_names.add(name)
                if dry_run and not result.get("failed"):
                    _add_projected_identity(
                        projected_buckets,
                        record,
                        fallback_owner_id=record_owner_id,
                        fallback_library=record_library,
                    )
            except Exception as exc:  # noqa: BLE001 - one object must not fail the batch.
                failed.append({"ref": ref, "reason": str(exc)})
            finally:
                if progress_callback is not None:
                    progress_callback(index, total, ref)

        buckets = projected_buckets if dry_run else _identity_buckets(self.list_objects(compact=True))
        return {
            "objects_seen": total,
            "pulled": pulled,
            "skipped": skipped,
            "failed": failed,
            "ambiguous_names": sorted(name for name in seen_names if len(buckets.get(name, set())) > 1),
        }

    def server_libraries(
        self,
        *,
        owner: str | None = None,
        include_accessible: bool = True,
    ) -> list[dict[str, Any]]:
        """Return libraries visible to the connected central-server user."""

        include = "1" if include_accessible else "0"
        query = []
        if owner is not None:
            query.append(f"owner={quote(owner)}")
        query.append(f"include_accessible={include}")
        return _as_json_dict_list(
            self._json_request(
                "GET",
                f"/server/libraries?{'&'.join(query)}",
            )
        )

    def create_server_library(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Create a library through the connected central server."""

        return _as_json_dict(self._json_request("POST", "/server/libraries", payload))

    def get_server_library(
        self,
        library_ref: str,
        *,
        owner: str | None = None,
    ) -> dict[str, Any]:
        """Return one central-server library."""

        suffix = f"?owner={quote(owner)}" if owner is not None else ""
        return _as_json_dict(
            self._json_request(
                "GET",
                f"/server/libraries/{quote(library_ref)}{suffix}",
            )
        )

    def update_server_library(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Update one central-server library."""

        return _as_json_dict(
            self._json_request(
                "PUT",
                f"/server/libraries/{quote(library_ref)}",
                payload,
            )
        )

    def delete_server_library(self, library_ref: str) -> dict[str, Any]:
        """Raise a clear error because server-side library delete is unsupported."""

        raise NotImplementedError(LIBRARY_DELETE_UNSUPPORTED_MESSAGE)

    def server_library_grants(
        self,
        library_ref: str,
        *,
        owner: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return grants for one central-server library."""

        suffix = f"?owner={quote(owner)}" if owner is not None else ""
        return _as_json_dict_list(
            self._json_request(
                "GET",
                f"/server/libraries/{quote(library_ref)}/grants{suffix}",
            )
        )

    def grant_server_library(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Grant access to one central-server library."""

        return _as_json_dict(
            self._json_request(
                "POST",
                f"/server/libraries/{quote(library_ref)}/grants",
                payload,
            )
        )

    def revoke_server_library_grant(
        self,
        library_ref: str,
        grantee: str,
    ) -> dict[str, Any]:
        """Revoke a grantee's access to one central-server library."""

        return _as_json_dict(
            self._json_request(
                "POST",
                f"/server/libraries/{quote(library_ref)}/grants/{quote(grantee)}/revoke",
            )
        )

    def add_server_library_reference(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Add a live reference entry to one central-server library."""

        return _as_json_dict(
            self._json_request(
                "POST",
                f"/server/libraries/{quote(library_ref)}/references",
                payload,
            )
        )

    def copy_server_library_object(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Copy an object snapshot into one central-server library."""

        return _as_json_dict(
            self._json_request(
                "POST",
                f"/server/libraries/{quote(library_ref)}/copies",
                payload,
            )
        )

    def remove_server_library_entry(
        self,
        library_ref: str,
        name: str,
    ) -> dict[str, Any]:
        """Remove an owned object or reference entry from one library."""

        return _as_json_dict(
            self._json_request(
                "DELETE",
                f"/server/libraries/{quote(library_ref)}/entries/{quote(name)}",
            )
        )

    def register_env(self, name: str, python: str | None = None) -> dict[str, Any]:
        """Register a Python executable as a daemon environment."""

        payload = {"name": name}
        if python is not None:
            payload["python"] = python
        return _as_json_dict(
            self._json_request(
                "POST",
                "/envs",
                payload,
            )
        )

    def list_envs(self) -> dict[str, Any]:
        """List daemon environments."""

        return _as_json_dict(self._json_request("GET", "/envs"))

    def list_environment_builds(self) -> list[dict[str, Any]]:
        """List cached virtual environment builds."""

        return _as_json_dict_list(self._json_request("GET", "/environment-builds"))

    def get_environment_build(self, spec_hash: str) -> dict[str, Any]:
        """Return one cached virtual environment build."""

        return _as_json_dict(self._json_request("GET", f"/environment-builds/{quote(spec_hash)}"))

    def rebuild_environment_build(
        self,
        spec_hash: str,
        *,
        wait: bool = False,
    ) -> dict[str, Any]:
        """Force a cached virtual environment to be rebuilt."""

        return _as_json_dict(
            self._json_request(
                "POST",
                f"/environment-builds/{quote(spec_hash)}/rebuild",
                {"wait": wait},
                timeout=None,
            )
            if wait
            else self._json_request(
                "POST",
                f"/environment-builds/{quote(spec_hash)}/rebuild",
                {"wait": wait},
            )
        )

    def prune_docker_images(
        self,
        *,
        spec_hash: str | None = None,
    ) -> list[dict[str, Any]]:
        """Remove cached Docker runtime images and mark their records absent."""

        payload: dict[str, Any] = {}
        if spec_hash is not None:
            payload["spec_hash"] = spec_hash
        return _as_json_dict_list(self._json_request("POST", "/docker-images/prune", payload))

    def list_remote_signatures(self) -> list[dict[str, Any]]:
        """List cached remote object signatures."""

        return _as_json_dict_list(self._json_request("GET", "/remote-signatures"))

    def resolve_remote_signature(
        self,
        ref: dict[str, Any],
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        """Resolve and cache a remote object signature through the daemon."""

        return _as_json_dict(
            self._json_request(
                "POST",
                "/remote-signatures/resolve",
                {"ref": ref, "force": force},
            )
        )

    def resolve_remote_decomposition(
        self,
        ref: dict[str, Any],
    ) -> dict[str, Any]:
        """Resolve a remote pipeline decomposition through the daemon."""

        return _as_json_dict(
            self._json_request(
                "POST",
                "/remote-decompositions/resolve",
                {"ref": ref},
            )
        )

    def run_remote_node(
        self,
        node: dict[str, Any],
        *,
        kwargs: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Run one NodeRemote-like reference through the daemon.

        The daemon endpoint is intentionally blocking: it polls the server-side
        run until completion.  Without a user ``timeout_seconds`` limit, keep
        the HTTP read unbounded instead of applying the short control-plane
        default.
        """

        return _as_json_dict(
            self._json_request(
                "POST",
                "/remote-nodes/run",
                {
                    "node": node,
                    "kwargs": kwargs or {},
                    "timeout_seconds": timeout_seconds,
                },
                timeout=timeout_seconds,
            )
        )

    def register_object(
        self,
        name: str,
        *,
        entrypoint: str,
        env: str,
        yaml_text: str | None = None,
        yaml_path: str | Path | None = None,
        workdir: str | None = None,
        runtime_config: dict[str, Any] | None = None,
        description: str | None = None,
        version_label: str | None = None,
        object_id: str | None = None,
        library: str | None = None,
        create_library: bool = False,
        library_display_name: str | None = None,
        local_only: bool = False,
    ) -> dict[str, Any]:
        """Send a serialized SPL object to the daemon registry.

        By default the client reads ``yaml_path`` and sends its contents.  That
        means the daemon does not need access to the caller's working directory.
        """

        if yaml_text is None:
            if yaml_path is None:
                raise ValueError("yaml_text or yaml_path is required")
            yaml_text = Path(yaml_path).read_text(encoding="utf-8")

        payload = {
            "name": name,
            "entrypoint": entrypoint,
            "env": env,
            "yaml": yaml_text,
            "local_only": local_only,
        }
        if workdir is not None:
            payload["workdir"] = workdir
        if runtime_config is not None:
            payload["runtime_config"] = runtime_config
        if description is not None:
            payload["description"] = description
        if version_label is not None:
            payload["version_label"] = version_label
        if object_id is not None:
            payload["object_id"] = object_id
        if library is not None:
            payload["library"] = library
        if create_library:
            payload["create_library"] = True
        if library_display_name is not None:
            payload["library_display_name"] = library_display_name
        return _as_json_dict(self._json_request("POST", "/objects", payload))

    def list_objects(
        self,
        query: str | None = None,
        *,
        compact: bool = False,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """List registered objects."""

        view = "summary" if compact else None
        if query is None:
            suffix = "?view=summary" if view else ""
            return _as_object_listing(self._json_request("GET", f"/objects{suffix}"))
        query_parts = [f"query={quote(query)}"]
        if view:
            query_parts.append("view=summary")
        return _as_object_listing(self._json_request("GET", f"/objects?{'&'.join(query_parts)}"))

    def search_objects(self, query: str) -> list[dict[str, Any]]:
        """Search registered objects by name, description, and metadata."""

        return _as_json_dict_list(self._json_request("GET", f"/objects/search?q={quote(query)}"))

    def get_object(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        include_yaml: bool = False,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Return one object by name or internal id."""

        path = f"/objects/{quote(name_or_id)}"
        query: dict[str, str] = {}
        if version is not None:
            query["version"] = str(version)
        if include_yaml:
            query["include_yaml"] = "1"
        if owner_id is not None:
            query["owner_id"] = owner_id
        if library is not None:
            query["library"] = library
        if query:
            path = f"{path}?{urlencode(query)}"
        return _as_json_dict(self._json_request("GET", path))

    def signature(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> dict[str, Any]:
        """Return a compact call/read signature for one object.

        Scoped lookups (``owner_id``/``library``) resolve against the LOCAL
        registry first — the caller's own objects in any library work fully
        offline. Only when the local registry has no match does the daemon ask
        the central server (cross-owner objects that are not cached yet).
        """

        path = f"/objects/{quote(name_or_id)}/signature"
        query = []
        if version is not None:
            query.append(f"version={version}")
        if function is not None:
            query.append(f"function={quote(function)}")
        if owner_id is not None:
            query.append(f"owner_id={quote(owner_id)}")
        if library is not None:
            query.append(f"library={quote(library)}")
        if query:
            path = f"{path}?{'&'.join(query)}"
        try:
            return _as_json_dict(self._json_request("GET", path))
        except ClientError as local_error:
            if library is not None:
                raise
            if owner_id is None and library is None:
                raise
            if not str(local_error).startswith("404:"):
                raise
            ref: dict[str, Any] = {"object_name": name_or_id}
            if owner_id is not None:
                ref["owner_id"] = owner_id
            if library is not None:
                ref["library"] = library
            if version is not None:
                ref["version"] = version
            if function is not None:
                ref["function"] = function
            try:
                return _as_json_dict(self.resolve_remote_signature(ref)["signature"])
            except ClientError as remote_error:
                # Offline: the local "is not registered" message is the
                # useful one; "no server connection" would only mislead.
                if "server connection" in str(remote_error):
                    raise local_error from None
                raise

    def inputs(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return the required/optional inputs for one object."""

        if owner_id is not None or library is not None:
            return _as_json_dict_list(
                self.signature(
                    name_or_id,
                    version=version,
                    owner_id=owner_id,
                    library=library,
                    function=function,
                )["inputs"]
            )

        path = f"/objects/{quote(name_or_id)}/inputs"
        query = []
        if version is not None:
            query.append(f"version={version}")
        if function is not None:
            query.append(f"function={quote(function)}")
        if query:
            path = f"{path}?{'&'.join(query)}"
        return _as_json_dict_list(self._json_request("GET", path))

    def outputs(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return supported output selectors and result accessors."""

        if owner_id is not None or library is not None:
            return _as_json_dict_list(
                self.signature(
                    name_or_id,
                    version=version,
                    owner_id=owner_id,
                    library=library,
                    function=function,
                )["outputs"]
            )

        path = f"/objects/{quote(name_or_id)}/outputs"
        query = []
        if version is not None:
            query.append(f"version={version}")
        if function is not None:
            query.append(f"function={quote(function)}")
        if query:
            path = f"{path}?{'&'.join(query)}"
        return _as_json_dict_list(self._json_request("GET", path))

    def decomposition(
        self,
        name_or_id: str,
        *,
        version: int | None = None,
    ) -> dict[str, Any]:
        """Return normalized functions, pipeline nodes, and links."""

        path = f"/objects/{quote(name_or_id)}/decomposition"
        if version is not None:
            path = f"{path}?version={version}"
        return _as_json_dict(self._json_request("GET", path))

    def object_versions(
        self,
        name_or_id: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return all versions of one object."""

        path = f"/objects/{quote(name_or_id)}/versions"
        query: dict[str, str] = {}
        if owner_id is not None:
            query["owner_id"] = owner_id
        if library is not None:
            query["library"] = library
        if query:
            path = f"{path}?{urlencode(query)}"
        return _as_json_dict_list(self._json_request("GET", path))

    def forget(
        self,
        name_or_id: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove one local daemon object without contacting the central server."""

        path = f"/objects/{quote(name_or_id)}"
        query: dict[str, str] = {}
        if owner_id is not None:
            query["owner_id"] = owner_id
        if library is not None:
            query["library"] = library
        if query:
            path = f"{path}?{urlencode(query)}"
        return _as_json_dict(self._json_request("DELETE", path))

    def remove_local(
        self,
        name_or_id: str,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Alias for :meth:`forget`."""

        return self.forget(name_or_id, owner_id=owner_id, library=library)

    def forget_version(
        self,
        name_or_id: str,
        version_ref: str | int,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove one local object version without contacting the server."""

        path = f"/objects/{quote(name_or_id)}/versions/{quote(str(version_ref))}"
        query: dict[str, str] = {}
        if owner_id is not None:
            query["owner_id"] = owner_id
        if library is not None:
            query["library"] = library
        if query:
            path = f"{path}?{urlencode(query)}"
        return _as_json_dict(self._json_request("DELETE", path))

    def prune_stale_mirrors(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove locally cached server-origin mirror rows."""

        path = "/objects/prune-stale-mirrors"
        query: dict[str, str] = {}
        if owner_id is not None:
            query["owner_id"] = owner_id
        if library is not None:
            query["library"] = library
        if query:
            path = f"{path}?{urlencode(query)}"
        return _as_json_dict(self._json_request("POST", path))

    def run(
        self,
        object_name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        version_id: str | None = None,
        function: str | None = None,
        target_machine: str | None = None,
        object_owner_id: str | None = None,
        library: str | None = None,
        offline_policy: OfflinePolicy | None = None,
        source: RunSource = "auto",
        remote: bool | None = None,
        keep: bool | str | None = None,
        runtimes: dict[str, str] | None = None,
        wait: bool = False,
    ) -> dict[str, Any]:
        """Start a daemon run and return its initial state."""

        payload: dict[str, Any] = {"object": object_name, "source": source}
        if args is not None:
            payload["args"] = args
        if kwargs is not None:
            payload["kwargs"] = kwargs
        if output is not None:
            payload["output"] = output
        if timeout_seconds is not None:
            payload["timeout_seconds"] = timeout_seconds
        if version is not None:
            payload["version"] = version
        if version_id is not None:
            payload["version_id"] = version_id
        if function is not None:
            payload["function"] = function
        if target_machine is not None:
            payload["target_machine"] = target_machine
        if object_owner_id is not None:
            payload["object_owner_id"] = object_owner_id
        if library is not None:
            payload["library"] = library
        if offline_policy is not None:
            payload["offline_policy"] = offline_policy
        if remote is not None:
            payload["remote"] = remote
        elif object_owner_id is not None or library is not None:
            payload["remote"] = True
        if keep is not None:
            payload["keep"] = keep
        if runtimes is not None:
            payload["runtimes"] = runtimes
        if wait:
            payload["wait"] = True
        # Some compatibility daemon routes may honor wait=True by holding the
        # request open until the run finishes.  urllib cannot set separate
        # connect/read timeouts, so an unbounded run must not inherit the
        # control-plane 60-second read cap.
        if wait:
            return _as_json_dict(self._json_request("POST", "/runs", payload, timeout=timeout_seconds))
        return _as_json_dict(self._json_request("POST", "/runs", payload))

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        """Read one server-side remote run state through the local daemon."""

        return _as_json_dict(self._json_request("GET", f"/remote-runs/{quote(run_id)}"))

    def list_remote_artifacts(self, run_id: str) -> list[str]:
        """List artifact names for a server-side remote run."""

        return _as_json_str_list(self._json_request("GET", f"/remote-runs/{quote(run_id)}/artifacts"))

    def download_remote_artifact(
        self,
        run_id: str,
        artifact_name: str,
        target: str | Path,
    ) -> Path:
        """Download one server-side artifact through the local daemon."""

        target_path = Path(target)
        if target_path.is_dir():
            target_path = target_path / artifact_name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        data = self._bytes_request(f"/remote-runs/{quote(run_id)}/artifacts/{quote(artifact_name)}")
        target_path.write_bytes(data)
        return target_path

    def wait_remote_run(
        self,
        run_id: str,
        *,
        poll_interval: float = 0.5,
        timeout_seconds: float | None = None,
        on_state: RunStateCallback | None = None,
    ) -> dict[str, Any]:
        """Poll a server-side remote run until it reaches a terminal state.

        ``on_state`` is invoked with every polled state, including the final
        one; exceptions raised by the callback abort the wait.
        """

        started = time.monotonic()
        while True:
            state = self.get_remote_run(run_id)
            if on_state is not None:
                on_state(state)
            if state["status"] in {"succeeded", "failed", "cancelled", "stale"}:
                return state
            if timeout_seconds is not None and time.monotonic() - started > timeout_seconds:
                raise TimeoutError(f"remote run did not finish within {timeout_seconds} seconds")
            time.sleep(poll_interval)

    def get_run(self, run_id: str) -> dict[str, Any]:
        """Read one run state."""

        return _as_json_dict(self._json_request("GET", f"/runs/{quote(run_id)}"))

    def resume_run(
        self,
        run_id: str,
        *,
        from_: Any,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        adapters: dict[str, Any] | None = None,
        runtimes: dict[str, str] | None = None,
        keep: bool | str | None = None,
        wait: bool = False,
    ) -> dict[str, Any]:
        """Start a daemon resume run and return its state."""

        payload: dict[str, Any] = {"from": from_}
        if kwargs is not None:
            payload["kwargs"] = kwargs
        if output is not None:
            payload["output"] = output
        if timeout_seconds is not None:
            payload["timeout_seconds"] = timeout_seconds
        if adapters is not None:
            payload["adapters"] = adapters
        if runtimes is not None:
            payload["runtimes"] = runtimes
        if keep is not None:
            payload["keep"] = keep
        state = _as_json_dict(
            self._json_request(
                "POST",
                f"/runs/{quote(run_id)}/resume",
                payload,
                timeout=timeout_seconds if wait else DEFAULT_HTTP_TIMEOUT_SECONDS,
            )
        )
        if wait:
            return self.wait_run(state["id"], timeout_seconds=timeout_seconds)
        return state

    def show_run(self, run_id: str, *, full_inline: bool = False) -> dict[str, Any]:
        """Read one run with a show-safe manifest."""

        query = urlencode({"view": "show", "full_inline": "1" if full_inline else "0"})
        return _as_json_dict(self._json_request("GET", f"/runs/{quote(run_id)}?{query}"))

    def list_runs(self) -> list[dict[str, Any]]:
        """List known runs."""

        return _as_json_dict_list(self._json_request("GET", "/runs"))

    def run_tag_stats(self) -> dict[str, Any]:
        """Aggregate edge tag counts from local daemon run manifests."""

        return _as_json_dict(self._json_request("GET", "/runs/tag-stats"))

    def prune_runs(
        self,
        *,
        run_id: str | None = None,
        statuses: list[str] | None = None,
        older_than_seconds: float | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Prune inactive daemon runs by id, status, age, or retention TTL."""

        payload: dict[str, Any] = {"dry_run": dry_run}
        if run_id is not None:
            payload["run_id"] = run_id
        if statuses is not None:
            payload["statuses"] = statuses
        if older_than_seconds is not None:
            payload["older_than_seconds"] = older_than_seconds
        return _as_json_dict(self._json_request("POST", "/runs/prune", payload))

    def delete_run(self, run_id: str, *, dry_run: bool = False) -> dict[str, Any]:
        """Delete one inactive daemon run."""

        query = urlencode({"dry_run": "1"}) if dry_run else ""
        suffix = f"?{query}" if query else ""
        return _as_json_dict(self._json_request("DELETE", f"/runs/{quote(run_id)}{suffix}"))

    def wait_run(
        self,
        run_id: str,
        *,
        poll_interval: float = 0.25,
        timeout_seconds: float | None = None,
        on_state: RunStateCallback | None = None,
    ) -> dict[str, Any]:
        """Poll a run until it reaches a terminal state.

        ``on_state`` is invoked with every polled state, including the final
        one; exceptions raised by the callback abort the wait.
        """

        started = time.monotonic()
        while True:
            state = self.get_run(run_id)
            if on_state is not None:
                on_state(state)
            if state["status"] in {"succeeded", "failed"}:
                return state
            if timeout_seconds is not None and time.monotonic() - started > timeout_seconds:
                raise TimeoutError(f"run did not finish within {timeout_seconds} seconds")
            time.sleep(poll_interval)

    def result(self, run_id: str) -> dict[str, Any]:
        """Return a completed run's result payload."""

        return _as_json_dict(self._json_request("GET", f"/runs/{quote(run_id)}/result"))

    def list_artifacts(self, run_id: str) -> list[str]:
        """List artifact names for a run."""

        return _as_json_str_list(self._json_request("GET", f"/runs/{quote(run_id)}/artifacts"))

    def download_artifact(
        self,
        run_id: str,
        artifact_name: str,
        target: str | Path,
    ) -> Path:
        """Download one artifact file into ``target`` and return its path."""

        target_path = Path(target)
        if target_path.is_dir():
            target_path = target_path / artifact_name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        data = self._bytes_request(f"/runs/{quote(run_id)}/artifacts/{quote(artifact_name)}")
        target_path.write_bytes(data)
        return target_path
