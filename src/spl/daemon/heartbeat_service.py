"""Server heartbeat thread management for the local daemon."""

from __future__ import annotations

import threading
from typing import Any, Callable

from spl.daemon.remote_client import ServerClientError
from spl.daemon.store import RegistryStore


class HeartbeatService:
    """Own background heartbeat loops for central-server connections."""

    def __init__(
        self,
        store: RegistryStore,
        sync_once: Callable[..., dict[str, Any]],
    ):
        self.store = store
        self.sync_once = sync_once
        self._lock = threading.Lock()
        self._stops: dict[str, threading.Event] = {}
        self._threads: dict[str, threading.Thread] = {}

    def restore_server_heartbeat(self) -> None:
        """Resume heartbeat loop for a persisted active connection, if present."""

        credentials = self.store.current_server_connection_credentials()
        if credentials is not None:
            self.start_server_heartbeat(credentials, token=credentials["token"])

    def start_server_heartbeat(
        self,
        connection: dict[str, Any],
        *,
        token: str,
    ) -> None:
        """Start one background heartbeat loop and stop older loops."""

        stop_event = threading.Event()
        with self._lock:
            for event in self._stops.values():
                event.set()
            self._stops.clear()
            self._threads.clear()
            self._stops[connection["id"]] = stop_event

        thread = threading.Thread(
            target=self._server_heartbeat_loop,
            args=(connection["id"], token, stop_event),
            name=f"spl-server-heartbeat-{connection['id']}",
            daemon=True,
        )
        with self._lock:
            self._threads[connection["id"]] = thread
        thread.start()

    def stop_server_heartbeat(self, connection_id: str) -> None:
        """Stop the heartbeat loop for one local connection."""

        with self._lock:
            event = self._stops.pop(connection_id, None)
            self._threads.pop(connection_id, None)
        if event is not None:
            event.set()

    def shutdown(self) -> None:
        """Stop all heartbeat loops and wait briefly for their threads."""

        with self._lock:
            stop_events = list(self._stops.values())
            threads = list(self._threads.values())
            self._stops.clear()
            self._threads.clear()
        for event in stop_events:
            event.set()
        current_thread = threading.current_thread()
        for thread in threads:
            if thread is not current_thread:
                thread.join(timeout=2)

    def _server_heartbeat_loop(
        self,
        connection_id: str,
        token: str,
        stop_event: threading.Event,
    ) -> None:
        """Keep the central-server connection lease alive while daemon runs."""

        first_tick = True
        while not stop_event.is_set():
            credentials = self.store.get_server_connection_credentials(connection_id)
            interval = float(credentials["heartbeat_interval_seconds"])
            if not first_tick and stop_event.wait(interval):
                return
            first_tick = False

            try:
                self.sync_once(connection_id=connection_id)
            except ServerClientError as exc:
                status = "stale" if exc.status_code in {401, 403, 404, 409} else "heartbeat_failed"
                self.store.record_server_connection_error(
                    connection_id,
                    status=status,
                    error=exc.message,
                )
                if status == "stale":
                    return
            except Exception as exc:
                self.store.record_server_connection_error(
                    connection_id,
                    status="heartbeat_failed",
                    error=repr(exc),
                )
