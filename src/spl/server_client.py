"""Advanced/internal direct client for the central SPL daemon server.

Most user code should go through ``SPLClient``.  ``SPLServerClient`` talks to
the central server directly with one bearer token for advanced integrations,
external execution tokens, and internal plumbing.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request

from spl._http import urlopen_verified

DEFAULT_SERVER_URL = "https://splime.io/api"
TERMINAL_REMOTE_RUN_STATUSES = {"succeeded", "failed", "cancelled", "stale"}

OfflinePolicy = Literal["queue", "wait", "fail_fast"]
RemoteRunScope = Literal["owned", "target", "object", "all"]


def _url_part(value: str) -> str:
    return quote(value, safe="")


class ServerClientError(RuntimeError):
    """Raised when the central server returns an error response."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"{status_code}: {message}")


@dataclass(frozen=True)
class ServerCallResult:
    """Completed server run plus optional downloaded artifacts."""

    run: dict[str, Any]
    detail: dict[str, Any]
    downloaded_artifacts: dict[str, Path] = field(default_factory=dict)

    @property
    def value(self) -> Any:
        result = self.detail.get("result")
        if isinstance(result, dict) and "value" in result:
            return result["value"]
        if result is not None:
            return result
        raw = self.run.get("result")
        if isinstance(raw, dict) and "value" in raw:
            return raw["value"]
        return raw

    @property
    def artifacts(self) -> list[dict[str, Any]]:
        return list(self.detail.get("artifacts") or [])


class ServerRemoteRun:
    """Handle for a central-server remote run."""

    def __init__(self, client: "SPLServerClient", state: dict[str, Any]):
        self._client = client
        self.state = state

    @property
    def id(self) -> str:
        return self.state["id"]

    @property
    def status(self) -> str:
        return self.state["status"]

    @property
    def mode(self) -> str:
        return "server"

    def refresh(self) -> dict[str, Any]:
        self.state = self._client.get_run(self.id)
        return self.state

    def wait(
        self,
        *,
        poll_interval: float = 0.5,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        while True:
            state = self.refresh()
            if state["status"] in TERMINAL_REMOTE_RUN_STATUSES:
                return state
            if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
                raise TimeoutError(f"remote run {self.id!r} did not finish in time")
            time.sleep(max(0.0, poll_interval))

    def detail(self) -> dict[str, Any]:
        return self._client.get_run_detail(self.id)

    def events(self) -> list[dict[str, Any]]:
        return self._client.list_events(self.id)

    def artifact_names(self) -> list[str]:
        return [item["name"] for item in self._client.list_artifacts(self.id)]

    def artifact_bytes(self, name: str) -> bytes:
        return self._client.artifact_bytes(self.id, name)

    def download_artifact(self, name: str, target: str | Path) -> Path:
        return self._client.download_artifact(self.id, name, target)

    def download_artifacts(self, target_dir: str | Path) -> dict[str, Path]:
        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)
        return {
            name: self._client.download_artifact(self.id, name, target_path)
            for name in self.artifact_names()
        }

    def cancel(self) -> dict[str, Any]:
        self.state = self._client.cancel_run(self.id)
        return self.state

    def retry(self) -> "ServerRemoteRun":
        return self._client.retry_run(self.id)

    def collect(
        self,
        *,
        artifacts_dir: str | Path | None = None,
        poll_interval: float = 0.5,
        timeout_seconds: float | None = None,
    ) -> ServerCallResult:
        final_state = self.wait(
            poll_interval=poll_interval,
            timeout_seconds=timeout_seconds,
        )
        if final_state["status"] != "succeeded":
            error = final_state.get("error") or "remote run returned no error message"
            raise RuntimeError(
                f"server run {self.id!r} ended as "
                f"{final_state.get('status')!r}: {error}"
            )
        detail = self.detail()
        downloaded = (
            self.download_artifacts(artifacts_dir)
            if artifacts_dir is not None
            else {}
        )
        return ServerCallResult(
            run=final_state,
            detail=detail,
            downloaded_artifacts=downloaded,
        )


class SPLServerClient:
    """Advanced direct stdlib HTTP client for the central SPL daemon server."""

    def __init__(
        self,
        token: str,
        *,
        base_url: str = DEFAULT_SERVER_URL,
    ):
        if not token:
            raise ValueError("token is required")
        self.token = token
        self.base_url = base_url.rstrip("/")

    @classmethod
    def external_token(
        cls,
        token: str,
        *,
        base_url: str = DEFAULT_SERVER_URL,
    ) -> "SPLExternalTokenClient":
        """Return a restricted facade for ``library_execution_token`` use."""

        return SPLExternalTokenClient(token, base_url=base_url)

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

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
            with urlopen_verified(request) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            try:
                message = json.loads(raw).get("error", raw)
            except json.JSONDecodeError:
                message = raw
            raise ServerClientError(
                exc.code,
                f"central SPL server returned {exc.code} at {self.base_url}{path}: {message}",
            ) from exc
        except URLError as exc:
            raise ServerClientError(
                502,
                f"central SPL server is not reachable at {self.base_url}: {exc.reason}",
            ) from exc
        if not raw:
            return None
        return json.loads(raw)

    def _bytes_request(self, path: str) -> bytes:
        request = Request(f"{self.base_url}{path}", headers=self._headers())
        try:
            with urlopen_verified(request) as response:
                return response.read()
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            try:
                message = json.loads(raw).get("error", raw)
            except json.JSONDecodeError:
                message = raw
            raise ServerClientError(
                exc.code,
                f"central SPL server returned {exc.code} at {self.base_url}{path}: {message}",
            ) from exc
        except URLError as exc:
            raise ServerClientError(
                502,
                f"central SPL server is not reachable at {self.base_url}: {exc.reason}",
            ) from exc

    def objects(
        self,
        *,
        owner: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        path = (
            f"/owners/{_url_part(owner)}/libraries/{_url_part(library or 'default')}/objects"
            if owner
            else "/objects"
        )
        query = {}
        if library and owner is None:
            query["library"] = library
        if compact:
            query["view"] = "summary"
        return self._json_request("GET", self._with_query(path, query))

    def get_object(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
        include_yaml: bool = False,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if version is not None:
            query["version"] = int(version)
        if include_yaml:
            query["include_yaml"] = "1"
        if library and owner is None:
            query["library"] = library
        path = self._object_path(name_or_id, owner=owner, library=library)
        return self._json_request("GET", self._with_query(path, query))

    def signature(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
        function: str | None = None,
    ) -> dict[str, Any]:
        return self._object_view(
            name_or_id,
            "signature",
            owner=owner,
            library=library,
            version=version,
            function=function,
        )

    def inputs(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
        function: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._object_view(
            name_or_id,
            "inputs",
            owner=owner,
            library=library,
            version=version,
            function=function,
        )

    def outputs(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
        function: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._object_view(
            name_or_id,
            "outputs",
            owner=owner,
            library=library,
            version=version,
            function=function,
        )

    def decomposition(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
    ) -> dict[str, Any]:
        return self._object_view(
            name_or_id,
            "decomposition",
            owner=owner,
            library=library,
            version=version,
        )

    def versions(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        include_yaml: bool = False,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        if include_yaml:
            query["include_yaml"] = "1"
        if library and owner is None:
            query["library"] = library
        path = f"{self._object_path(name_or_id, owner=owner, library=library)}/versions"
        return self._json_request("GET", self._with_query(path, query))

    def start(
        self,
        name: str,
        *,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        version_id: str | None = None,
        function: str | None = None,
        target_owner: str | None = None,
        access_token: str | None = None,
        correlation_id: str | None = None,
        parent_run_id: str | None = None,
        context: dict[str, Any] | None = None,
        offline_policy: OfflinePolicy | None = None,
    ) -> ServerRemoteRun:
        payload: dict[str, Any] = {"object": name}
        if target_machine is not None:
            payload["target_machine_id"] = target_machine
        if target_owner is not None:
            payload["target_owner_id"] = target_owner
        if owner is not None:
            payload["object_owner_id"] = owner
        if library is not None:
            payload["library"] = library
        if args is not None:
            payload["args"] = args
        if kwargs is not None:
            payload["kwargs"] = kwargs
        if output is not None:
            payload["output"] = output
        if timeout_seconds is not None:
            payload["timeout_seconds"] = timeout_seconds
        if version is not None:
            payload["version"] = int(version)
        if version_id is not None:
            payload["version_id"] = version_id
        if function is not None:
            payload["function"] = function
        if access_token is not None:
            payload["access_token"] = access_token
        if correlation_id is not None:
            payload["correlation_id"] = correlation_id
        if parent_run_id is not None:
            payload["parent_run_id"] = parent_run_id
        if context:
            payload["context"] = context
        if offline_policy is not None:
            payload["offline_policy"] = offline_policy
        return ServerRemoteRun(self, self._json_request("POST", "/remote-runs", payload))

    def call(
        self,
        name: str,
        *,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        wait_timeout_seconds: float | None = None,
        poll_interval: float = 0.5,
        artifacts_dir: str | Path | None = None,
        version: int | None = None,
        version_id: str | None = None,
        function: str | None = None,
        target_owner: str | None = None,
        access_token: str | None = None,
        correlation_id: str | None = None,
        parent_run_id: str | None = None,
        context: dict[str, Any] | None = None,
        offline_policy: OfflinePolicy | None = None,
    ) -> ServerCallResult:
        run = self.start(
            name,
            target_machine=target_machine,
            owner=owner,
            library=library,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            version=version,
            version_id=version_id,
            function=function,
            target_owner=target_owner,
            access_token=access_token,
            correlation_id=correlation_id,
            parent_run_id=parent_run_id,
            context=context,
            offline_policy=offline_policy,
        )
        return run.collect(
            artifacts_dir=artifacts_dir,
            poll_interval=poll_interval,
            timeout_seconds=wait_timeout_seconds,
        )

    def runs(self, *, scope: RemoteRunScope | None = None) -> list[dict[str, Any]]:
        query = {"scope": scope} if scope else {}
        return self._json_request("GET", self._with_query("/remote-runs", query))

    def list_runs(self, *, scope: RemoteRunScope | None = None) -> list[dict[str, Any]]:
        return self.runs(scope=scope)

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self._json_request("GET", f"/remote-runs/{_url_part(run_id)}")

    def get_run_detail(self, run_id: str) -> dict[str, Any]:
        return self._json_request("GET", f"/remote-runs/{_url_part(run_id)}/detail")

    def list_events(self, run_id: str) -> list[dict[str, Any]]:
        return self._json_request("GET", f"/remote-runs/{_url_part(run_id)}/events")

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        return self._json_request("GET", f"/remote-runs/{_url_part(run_id)}/artifacts")

    def artifact_bytes(self, run_id: str, name: str) -> bytes:
        return self._bytes_request(
            f"/remote-runs/{_url_part(run_id)}/artifacts/{_url_part(name)}"
        )

    def download_artifact(self, run_id: str, name: str, target: str | Path) -> Path:
        target_path = Path(target)
        if target_path.is_dir():
            target_path = target_path / name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(self.artifact_bytes(run_id, name))
        return target_path

    def cancel_run(self, run_id: str) -> dict[str, Any]:
        return self._json_request("POST", f"/remote-runs/{_url_part(run_id)}/cancel")

    def retry_run(self, run_id: str) -> ServerRemoteRun:
        state = self._json_request("POST", f"/remote-runs/{_url_part(run_id)}/retry")
        return ServerRemoteRun(self, state)

    def _object_view(
        self,
        name_or_id: str,
        suffix: str,
        *,
        owner: str | None,
        library: str | None,
        version: int | None,
        function: str | None = None,
    ) -> Any:
        query: dict[str, Any] = {}
        if version is not None:
            query["version"] = int(version)
        if function is not None:
            query["function"] = function
        if library and owner is None:
            query["library"] = library
        path = f"{self._object_path(name_or_id, owner=owner, library=library)}/{suffix}"
        return self._json_request("GET", self._with_query(path, query))

    def _object_path(
        self,
        name_or_id: str,
        *,
        owner: str | None,
        library: str | None,
    ) -> str:
        if owner:
            return (
                f"/owners/{_url_part(owner)}/libraries/"
                f"{_url_part(library or 'default')}/objects/{_url_part(name_or_id)}"
            )
        return f"/objects/{_url_part(name_or_id)}"

    def _with_query(self, path: str, query: dict[str, Any]) -> str:
        clean: list[tuple[str, Any]] = []
        for key, value in query.items():
            if value is None or value == "" or value is False:
                continue
            clean.append((key, "1" if value is True else value))
        if not clean:
            return path
        return f"{path}?{urlencode(clean)}"


class SPLExternalTokenClient:
    """Restricted direct client for external library execution tokens.

    This facade intentionally exposes only callable-surface reads, remote-run
    launch/read, events, and artifact download helpers.  It does not expose
    machine management, token management, grants, admin/settings, cancel, retry,
    or broad object listing helpers.
    """

    def __init__(
        self,
        token: str,
        *,
        base_url: str = DEFAULT_SERVER_URL,
    ):
        self._client = SPLServerClient(token, base_url=base_url)

    @property
    def token(self) -> str:
        return self._client.token

    @property
    def base_url(self) -> str:
        return self._client.base_url

    def signature(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
        function: str | None = None,
    ) -> dict[str, Any]:
        return self._client.signature(
            name_or_id,
            owner=owner,
            library=library,
            version=version,
            function=function,
        )

    def inputs(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
        function: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._client.inputs(
            name_or_id,
            owner=owner,
            library=library,
            version=version,
            function=function,
        )

    def outputs(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
        function: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._client.outputs(
            name_or_id,
            owner=owner,
            library=library,
            version=version,
            function=function,
        )

    def decomposition(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
    ) -> dict[str, Any]:
        return self._client.decomposition(
            name_or_id,
            owner=owner,
            library=library,
            version=version,
        )

    def get_object(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
    ) -> dict[str, Any]:
        return self._client.get_object(
            name_or_id,
            owner=owner,
            library=library,
            version=version,
            include_yaml=False,
        )

    def versions(
        self,
        name_or_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._client.versions(
            name_or_id,
            owner=owner,
            library=library,
            include_yaml=False,
        )

    def start(
        self,
        name: str,
        *,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        version_id: str | None = None,
        function: str | None = None,
        correlation_id: str | None = None,
        context: dict[str, Any] | None = None,
        offline_policy: OfflinePolicy | None = None,
    ) -> ServerRemoteRun:
        return self._client.start(
            name,
            target_machine=target_machine,
            owner=owner,
            library=library,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            version=version,
            version_id=version_id,
            function=function,
            correlation_id=correlation_id,
            context=context,
            offline_policy=offline_policy,
        )

    def call(
        self,
        name: str,
        *,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        wait_timeout_seconds: float | None = None,
        poll_interval: float = 0.5,
        artifacts_dir: str | Path | None = None,
        version: int | None = None,
        version_id: str | None = None,
        function: str | None = None,
        correlation_id: str | None = None,
        context: dict[str, Any] | None = None,
        offline_policy: OfflinePolicy | None = None,
    ) -> ServerCallResult:
        run = self.start(
            name,
            target_machine=target_machine,
            owner=owner,
            library=library,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            version=version,
            version_id=version_id,
            function=function,
            correlation_id=correlation_id,
            context=context,
            offline_policy=offline_policy,
        )
        return run.collect(
            artifacts_dir=artifacts_dir,
            poll_interval=poll_interval,
            timeout_seconds=wait_timeout_seconds,
        )

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self._client.get_run(run_id)

    def get_run_detail(self, run_id: str) -> dict[str, Any]:
        return self._client.get_run_detail(run_id)

    def list_events(self, run_id: str) -> list[dict[str, Any]]:
        return self._client.list_events(run_id)

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        return self._client.list_artifacts(run_id)

    def artifact_bytes(self, run_id: str, name: str) -> bytes:
        return self._client.artifact_bytes(run_id, name)

    def download_artifact(self, run_id: str, name: str, target: str | Path) -> Path:
        return self._client.download_artifact(run_id, name, target)


ServerClient = SPLServerClient
ExternalExecutionClient = SPLExternalTokenClient
