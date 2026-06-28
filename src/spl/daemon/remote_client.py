"""Client helpers for talking to the central SPL daemon server."""

from __future__ import annotations

import json
import hashlib
import http.client
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen


DEFAULT_SERVER_URL = "https://splime.io/api"
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0


class ServerClientError(RuntimeError):
    """Raised when the central daemon server returns an error response."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"{status_code}: {message}")


class ServerClient:
    """Small stdlib HTTP client for the central daemon server."""

    def __init__(
        self,
        base_url: str,
        machine_token: str,
        *,
        user_token: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.machine_token = machine_token
        self.user_token = user_token

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.machine_token}",
        }
        if self.user_token:
            headers["X-SPL-User-Token"] = self.user_token
        return headers

    def _json_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
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
            with urlopen(request) as response:  # noqa: S310 - configured server URL.
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            try:
                message = json.loads(raw).get("error", raw)
            except json.JSONDecodeError:
                message = raw
            message = (
                f"central SPL daemon server returned {exc.code} at "
                f"{self.base_url}{path}: {message}"
            )
            raise ServerClientError(exc.code, message) from exc
        except URLError as exc:
            raise ServerClientError(
                502,
                (
                    "central SPL daemon server is not reachable at "
                    f"{self.base_url}: {exc.reason}"
                ),
            ) from exc

        if not raw:
            return None
        return json.loads(raw)

    def _bytes_request(self, path: str) -> bytes:
        request = Request(f"{self.base_url}{path}", headers=self._headers())
        try:
            with urlopen(request) as response:  # noqa: S310 - configured server URL.
                return response.read()
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            try:
                message = json.loads(raw).get("error", raw)
            except json.JSONDecodeError:
                message = raw
            message = (
                f"central SPL daemon server returned {exc.code} at "
                f"{self.base_url}{path}: {message}"
            )
            raise ServerClientError(exc.code, message) from exc
        except URLError as exc:
            raise ServerClientError(
                502,
                (
                    "central SPL daemon server is not reachable at "
                    f"{self.base_url}: {exc.reason}"
                ),
            ) from exc

    def _streaming_file_request(
        self,
        method: str,
        path: str,
        file_path: Path,
        *,
        headers: dict[str, str] | None = None,
    ) -> Any:
        url = urlparse(self.base_url)
        if url.scheme not in {"http", "https"}:
            raise ServerClientError(400, f"unsupported server URL scheme: {url.scheme}")
        connection_class = (
            http.client.HTTPSConnection
            if url.scheme == "https"
            else http.client.HTTPConnection
        )
        host = url.hostname
        if not host:
            raise ServerClientError(400, f"invalid server URL: {self.base_url}")
        connection = connection_class(host, url.port, timeout=300)
        request_path = f"{url.path.rstrip('/')}{path}"
        request_headers = {
            **self._headers(),
            "Content-Length": str(file_path.stat().st_size),
        }
        request_headers.update(headers or {})
        try:
            connection.putrequest(method, request_path)
            for name, value in request_headers.items():
                connection.putheader(name, value)
            connection.endheaders()
            with file_path.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    connection.send(chunk)
            response = connection.getresponse()
            raw_bytes = response.read()
        except OSError as exc:
            raise ServerClientError(
                502,
                (
                    "central SPL daemon server is not reachable at "
                    f"{self.base_url}: {exc}"
                ),
            )
        finally:
            connection.close()
        raw = raw_bytes.decode("utf-8")
        if response.status >= 400:
            try:
                message = json.loads(raw).get("error", raw)
            except json.JSONDecodeError:
                message = raw
            raise ServerClientError(
                response.status,
                (
                    f"central SPL daemon server returned {response.status} at "
                    f"{self.base_url}{path}: {message}"
                ),
            )
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
        return self._json_request("POST", "/connections/connect", payload)

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
        return self._json_request("POST", "/connections/heartbeat", payload)

    def current_connection(self) -> dict[str, Any]:
        return self._json_request("GET", "/connections/current")

    def disconnect_machine(self) -> dict[str, Any]:
        return self._json_request("POST", "/connections/disconnect")

    def list_machines(self) -> list[dict[str, Any]]:
        return self._json_request("GET", "/machines")

    def list_libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        query = {"include_accessible": "1" if include_accessible else "0"}
        return self._json_request("GET", f"/libraries?{urlencode(query)}")

    def create_library(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._json_request("POST", "/libraries", payload)

    def get_library(self, library_ref: str) -> dict[str, Any]:
        return self._json_request("GET", f"/libraries/{quote(library_ref)}")

    def update_library(self, library_ref: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._json_request("PUT", f"/libraries/{quote(library_ref)}", payload)

    def delete_library(self, library_ref: str) -> dict[str, Any]:
        return self._json_request("DELETE", f"/libraries/{quote(library_ref)}")

    def list_library_grants(self, library_ref: str) -> list[dict[str, Any]]:
        return self._json_request("GET", f"/libraries/{quote(library_ref)}/grants")

    def grant_library(self, library_ref: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._json_request(
            "POST",
            f"/libraries/{quote(library_ref)}/grants",
            payload,
        )

    def revoke_library_grant(self, library_ref: str, grantee: str) -> dict[str, Any]:
        return self._json_request(
            "POST",
            f"/libraries/{quote(library_ref)}/grants/{quote(grantee)}/revoke",
        )

    def add_library_reference(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._json_request(
            "POST",
            f"/libraries/{quote(library_ref)}/references",
            payload,
        )

    def copy_object_into_library(
        self,
        library_ref: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._json_request(
            "POST",
            f"/libraries/{quote(library_ref)}/copies",
            payload,
        )

    def remove_library_entry(self, library_ref: str, name: str) -> dict[str, Any]:
        return self._json_request(
            "DELETE",
            f"/libraries/{quote(library_ref)}/entries/{quote(name)}",
        )

    def list_objects(
        self,
        *,
        owner_id: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        if library and owner_id is None:
            query["library"] = library
        if compact:
            query["view"] = "summary"
        suffix = f"?{urlencode(query)}" if query else ""
        if owner_id:
            path = (
                f"/owners/{quote(owner_id)}/libraries/"
                f"{quote(library or 'default')}/objects"
            )
        else:
            path = "/objects"
        return self._json_request("GET", f"{path}{suffix}")

    def latest_machine_library_snapshot(
        self,
        machine_id: str,
        *,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        suffix = "?include_yaml=1" if include_yaml else ""
        return self._json_request(
            "GET",
            f"/machines/{quote(machine_id)}/library-snapshots/latest{suffix}",
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
            path = (
                f"/owners/{quote(owner_id)}/libraries/"
                f"{quote(library or 'default')}/objects/{quote(name_or_id)}"
            )
        else:
            path = f"/objects/{quote(name_or_id)}"
        return self._json_request("GET", f"{path}{suffix}")

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
        return self._json_request("GET", f"{path}{suffix}")

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
        return self._json_request(
            "GET",
            f"{path}{suffix}",
        )

    def sync(
        self,
        *,
        connection_id: str,
        machine_id: str,
        heartbeat_interval_seconds: float,
        events: list[dict[str, Any]],
        capabilities: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._json_request(
            "POST",
            "/sync",
            {
                "connection_id": connection_id,
                "machine_id": machine_id,
                "heartbeat_interval_seconds": heartbeat_interval_seconds,
                "capabilities": capabilities or {},
                "events": events,
            },
        )

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        return self._json_request("GET", f"/remote-runs/{quote(run_id)}")

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        return self._json_request("GET", f"/remote-runs/{quote(run_id)}/artifacts")

    def upload_artifact(self, run_id: str, name: str, path: str | Path) -> dict[str, Any]:
        artifact_path = Path(path)
        return self._streaming_file_request(
            "PUT",
            f"/remote-runs/{quote(run_id)}/artifacts/{quote(name)}",
            artifact_path,
            headers={
                "Content-Type": "application/octet-stream",
                "X-SPL-Artifact-Sha256": _file_sha256(artifact_path),
                "X-SPL-Artifact-Size": str(artifact_path.stat().st_size),
            },
        )

    def artifact_bytes(self, run_id: str, name: str) -> bytes:
        return self._bytes_request(
            f"/remote-runs/{quote(run_id)}/artifacts/{quote(name)}"
        )

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
