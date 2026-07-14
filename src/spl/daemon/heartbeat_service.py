"""Server heartbeat thread management for the local daemon."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from spl.daemon.remote_client import DEFAULT_HEARTBEAT_INTERVAL_SECONDS, ServerClientError
from spl.daemon.repositories.server_connection import SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
from spl.daemon.store import RegistryStore, utc_now

LOGGER = logging.getLogger(__name__)

HEARTBEAT_INITIAL_BACKOFF_SECONDS = 30.0
HEARTBEAT_MAX_BACKOFF_SECONDS = 300.0
HEARTBEAT_WATCHDOG_INTERVAL_SECONDS = 30.0
HEARTBEAT_MIN_STALE_SECONDS = 90.0
HEARTBEAT_THREAD_JOIN_SECONDS = 16.0


class HeartbeatService:
    """Own recoverable, supervised heartbeat loops for server connections."""

    def __init__(
        self,
        store: RegistryStore,
        sync_once: Callable[..., dict[str, Any]],
        *,
        initial_backoff_seconds: float = HEARTBEAT_INITIAL_BACKOFF_SECONDS,
        max_backoff_seconds: float = HEARTBEAT_MAX_BACKOFF_SECONDS,
        watchdog_interval_seconds: float = HEARTBEAT_WATCHDOG_INTERVAL_SECONDS,
    ):
        self.store = store
        self.sync_once = sync_once
        self.initial_backoff_seconds = max(0.001, float(initial_backoff_seconds))
        self.max_backoff_seconds = max(self.initial_backoff_seconds, float(max_backoff_seconds))
        self.watchdog_interval_seconds = max(0.01, float(watchdog_interval_seconds))
        self._lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._stops: dict[str, threading.Event] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._last_tick_monotonic: dict[str, float] = {}
        self._last_tick_at: dict[str, str] = {}
        self._watchdog_stop = threading.Event()
        self._watchdog_thread: threading.Thread | None = None

    def restore_server_heartbeat(self) -> None:
        """Resume heartbeat for persisted identity, including needs-reconnect."""

        self._start_watchdog()
        try:
            credentials = self.store.current_server_connection_credentials()
        except RuntimeError as exc:
            if self._store_closed(exc):
                return
            LOGGER.exception("heartbeat credentials unavailable during restore")
            return
        except Exception:
            LOGGER.exception("heartbeat credentials unavailable during restore")
            return
        if credentials is not None:
            self.ensure_server_heartbeat(credentials)

    def start_server_heartbeat(
        self,
        connection: dict[str, Any],
        *,
        token: str,
    ) -> None:
        """Replace older loops with one heartbeat for ``connection``."""

        self._start_watchdog()
        connection_id = str(connection["id"])
        with self._lifecycle_lock:
            self._stop_and_join_all(except_connection_id=connection_id)
            try:
                credentials = self.store.get_server_connection_credentials(connection_id)
            except Exception:
                credentials = {**connection, "token": token}
            self._ensure_server_heartbeat_locked(credentials, force=True)

    def ensure_server_heartbeat(self, connection: dict[str, Any] | None = None) -> None:
        """Start or recover one heartbeat without ever duplicating its thread."""

        if self._watchdog_stop.is_set():
            return
        self._start_watchdog()
        try:
            credentials = connection or self.store.current_server_connection_credentials()
        except RuntimeError as exc:
            if self._store_closed(exc):
                return
            LOGGER.exception("heartbeat credentials unavailable during supervision")
            return
        except Exception:
            LOGGER.exception("heartbeat credentials unavailable during supervision")
            return
        if credentials is None:
            return
        with self._lifecycle_lock:
            self._ensure_server_heartbeat_locked(credentials)

    def stop_server_heartbeat(self, connection_id: str) -> None:
        """Stop the heartbeat loop for one local connection."""

        with self._lifecycle_lock:
            with self._lock:
                event = self._stops.get(connection_id)
                thread = self._threads.get(connection_id)
                if event is not None:
                    event.set()
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=HEARTBEAT_THREAD_JOIN_SECONDS)
            with self._lock:
                if self._threads.get(connection_id) is thread and (thread is None or not thread.is_alive()):
                    self._threads.pop(connection_id, None)
                    self._stops.pop(connection_id, None)

    def status(self, connection_id: str | None = None) -> dict[str, Any]:
        """Return heartbeat thread/tick diagnostics for sync status views."""

        if connection_id is None:
            connection = self.store.current_server_connection()
            connection_id = str(connection["id"]) if connection is not None else None
        with self._lock:
            thread = self._threads.get(connection_id or "")
            tick_at = self._last_tick_at.get(connection_id or "")
            tick_monotonic = self._last_tick_monotonic.get(connection_id or "")
        return {
            "connection_id": connection_id,
            "thread_alive": bool(thread is not None and thread.is_alive()),
            "last_tick_at": tick_at,
            "seconds_since_tick": None if tick_monotonic is None else max(0.0, time.monotonic() - tick_monotonic),
            "watchdog_interval_seconds": self.watchdog_interval_seconds,
        }

    def shutdown(self) -> None:
        """Stop all heartbeat/watchdog activity and wait briefly for threads."""

        self._watchdog_stop.set()
        with self._lifecycle_lock:
            self._stop_and_join_all()
        watchdog = self._watchdog_thread
        if watchdog is not None and watchdog is not threading.current_thread():
            watchdog.join(timeout=2)

    def _ensure_server_heartbeat_locked(
        self,
        credentials: dict[str, Any],
        *,
        force: bool = False,
    ) -> None:
        connection_id = str(credentials["id"])
        interval = self._heartbeat_interval(credentials.get("heartbeat_interval_seconds"))
        stale_after = max(HEARTBEAT_MIN_STALE_SECONDS, interval * 2)
        with self._lock:
            thread = self._threads.get(connection_id)
            stop_event = self._stops.get(connection_id)
            last_tick = self._last_tick_monotonic.get(connection_id)
            alive = bool(thread is not None and thread.is_alive())
            stale = last_tick is None or time.monotonic() - last_tick > stale_after
            if alive and not force and not stale:
                return
            if alive and stop_event is not None:
                stop_event.set()
        if alive and thread is not None and thread is not threading.current_thread():
            thread.join(timeout=HEARTBEAT_THREAD_JOIN_SECONDS)
            if thread.is_alive():
                LOGGER.error(
                    "heartbeat thread is wedged; duplicate restart refused for %s",
                    connection_id,
                )
                return
        with self._lock:
            current = self._threads.get(connection_id)
            if current is not None and current is not thread and current.is_alive():
                return
            stop_event = threading.Event()
            thread = threading.Thread(
                target=self._server_heartbeat_loop,
                args=(connection_id, str(credentials.get("token") or ""), stop_event),
                name=f"spl-server-heartbeat-{connection_id}",
                daemon=True,
            )
            self._stops[connection_id] = stop_event
            self._threads[connection_id] = thread
            self._last_tick_monotonic[connection_id] = time.monotonic()
            self._last_tick_at[connection_id] = utc_now()
            thread.start()

    def _stop_and_join_all(self, *, except_connection_id: str | None = None) -> None:
        with self._lock:
            entries = [
                (connection_id, self._stops.get(connection_id), thread)
                for connection_id, thread in self._threads.items()
                if connection_id != except_connection_id
            ]
            for _, event, _ in entries:
                if event is not None:
                    event.set()
        for _, _, thread in entries:
            if thread is not threading.current_thread():
                thread.join(timeout=HEARTBEAT_THREAD_JOIN_SECONDS)
        with self._lock:
            for connection_id, _, thread in entries:
                if self._threads.get(connection_id) is thread and not thread.is_alive():
                    self._threads.pop(connection_id, None)
                    self._stops.pop(connection_id, None)

    def _server_heartbeat_loop(
        self,
        connection_id: str,
        token: str,
        stop_event: threading.Event,
    ) -> None:
        """Keep a lease alive; all non-shutdown failures record and retry."""

        del token
        backoff = self.initial_backoff_seconds
        while not stop_event.is_set():
            interval = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
            delay = self.initial_backoff_seconds
            try:
                self._record_tick(connection_id)
                credentials = self.store.get_server_connection_credentials(connection_id)
                interval = self._heartbeat_interval(credentials.get("heartbeat_interval_seconds"))
                result = self.sync_once(connection_id=connection_id, probe_server_channel=True)
                partial_error = result.get("partial_error")
                if isinstance(partial_error, dict):
                    error = str(partial_error.get("message") or "sync batch failed")
                    status_code = partial_error.get("status_code")
                    if isinstance(status_code, int):
                        raise ServerClientError(status_code, error)
                    raise RuntimeError(error)
            except ServerClientError as exc:
                lease_rejected = exc.status_code in {401, 403, 404, 409}
                self._record_heartbeat_error(
                    connection_id,
                    status=(SERVER_CONNECTION_STATUS_NEEDS_RECONNECT if lease_rejected else "heartbeat_failed"),
                    error=(
                        f"lease rejected by server ({exc.status_code}): {exc.message}"
                        if lease_rejected
                        else exc.message
                    ),
                )
                delay = backoff
                backoff = min(self.max_backoff_seconds, backoff * 2)
            except RuntimeError as exc:
                if self._store_closed(exc):
                    LOGGER.info("heartbeat stopped because the store is closed: %s", connection_id)
                    return
                self._record_heartbeat_error(
                    connection_id,
                    status="heartbeat_failed",
                    error=repr(exc),
                )
                delay = backoff
                backoff = min(self.max_backoff_seconds, backoff * 2)
            except Exception as exc:
                self._record_heartbeat_error(
                    connection_id,
                    status="heartbeat_failed",
                    error=repr(exc),
                )
                delay = backoff
                backoff = min(self.max_backoff_seconds, backoff * 2)
            else:
                delay = interval
                backoff = self.initial_backoff_seconds
            if self._wait_until_next_attempt(
                connection_id,
                stop_event,
                delay=delay,
                interval=interval,
            ):
                return

    def _record_tick(self, connection_id: str) -> None:
        tick_monotonic = time.monotonic()
        try:
            tick_at = utc_now()
        except Exception:
            tick_at = None
            LOGGER.exception("heartbeat tick timestamp unavailable for %s", connection_id)
        with self._lock:
            self._last_tick_monotonic[connection_id] = tick_monotonic
            if tick_at is not None:
                self._last_tick_at[connection_id] = tick_at

    def _wait_until_next_attempt(
        self,
        connection_id: str,
        stop_event: threading.Event,
        *,
        delay: float,
        interval: float,
    ) -> bool:
        """Wait without making an intentional backoff look like a wedged thread."""

        deadline = time.monotonic() + max(0.0, delay)
        stale_after = max(HEARTBEAT_MIN_STALE_SECONDS, interval * 2)
        pulse_seconds = max(0.001, stale_after / 2)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if stop_event.wait(min(remaining, pulse_seconds)):
                return True
            self._record_tick(connection_id)

    def _record_heartbeat_error(
        self,
        connection_id: str,
        *,
        status: str,
        error: str,
    ) -> None:
        try:
            self.store.record_server_connection_error(
                connection_id,
                status=status,
                error=error,
            )
        except Exception as record_exc:
            LOGGER.exception(
                "heartbeat failure could not be persisted for %s; original=%s; recorder=%r",
                connection_id,
                error,
                record_exc,
            )

    def _start_watchdog(self) -> None:
        with self._lock:
            if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
                return
            if self._watchdog_stop.is_set():
                return
            thread = threading.Thread(
                target=self._watchdog_loop,
                name="spl-server-heartbeat-watchdog",
                daemon=True,
            )
            self._watchdog_thread = thread
            thread.start()

    def _watchdog_loop(self) -> None:
        while not self._watchdog_stop.wait(self.watchdog_interval_seconds):
            try:
                credentials = self.store.current_server_connection_credentials()
                if credentials is not None:
                    self.ensure_server_heartbeat(credentials)
            except RuntimeError as exc:
                if self._store_closed(exc):
                    return
                LOGGER.exception("heartbeat watchdog transient failure")
            except Exception:
                LOGGER.exception("heartbeat watchdog transient failure")

    @staticmethod
    def _heartbeat_interval(value: Any) -> float:
        try:
            interval = float(value)
        except (TypeError, ValueError):
            return DEFAULT_HEARTBEAT_INTERVAL_SECONDS
        return interval if interval > 0 else DEFAULT_HEARTBEAT_INTERVAL_SECONDS

    @staticmethod
    def _store_closed(exc: RuntimeError) -> bool:
        return str(exc) == "store is closed"
