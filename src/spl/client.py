"""User-facing client for publishing and running SPL objects on the daemon.

This module is the thin "framework side" of the daemon integration.  Code that
already uses SPL should not need to know about HTTP endpoints, run directories,
or worker subprocesses.  The intended workflow is:

    from spl.client import SPLClient

    client = SPLClient()
    client.publish(my_function, name="sum", env="default")
    result = client.call("sum", kwargs={"x": 1, "y": 2})

The module only imports ``spl.core`` inside export helpers.  That keeps basic
registry operations, such as listing remote objects, usable even from a small
environment that has the daemon client but does not currently have all core
dependencies imported yet.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field, replace
from html import escape
from pathlib import Path
from typing import Any, Literal, cast, overload

from spl.core.entities.node import DEFAULT_PORT
from spl.daemon_client import (
    DEFAULT_DAEMON_HOST,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_SERVER_URL,
    Client,
    ClientError,
)
from spl.server_client import SPLServerClient

OfflinePolicy = Literal["queue", "wait", "fail_fast"]
ObjectScope = Literal["auto", "local", "server", "all"]
RunSource = Literal["auto", "local"]
_NO_SERVER_CONNECTION_MESSAGE = "active server connection is not found"


def _preview(value: Any, *, limit: int = 80) -> str:
    preview = repr(value)
    if len(preview) <= limit:
        return preview
    return f"{preview[: limit - 3]}..."


def _is_missing_server_connection(exc: Exception) -> bool:
    return (
        isinstance(exc, ClientError)
        and _NO_SERVER_CONNECTION_MESSAGE in str(exc)
    )


def _warn_deprecated(old: str, new: str) -> None:
    """Emit the standard 0.2.0 deprecation warning (see docs/migration-0.2.0.md)."""

    warnings.warn(
        f"{old} is deprecated and will be removed in splime 0.2.0; "
        f"use {new} instead.",
        DeprecationWarning,
        stacklevel=3,
    )


@dataclass(frozen=True, repr=False)
class PublishedObject:
    """Receipt returned after an object is stored in the daemon registry.

    The full daemon document stays available via ``.raw`` but is kept out of
    the ``repr`` so a notebook does not print a multi-kilobyte blob.
    """

    name: str
    entrypoint: str
    env: str
    yaml_path: str
    workdir: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def version(self) -> str | None:
        """Best-effort human version, or ``None`` if the daemon did not send one."""

        current = self.raw.get("current_version")
        if isinstance(current, dict):
            for key in ("number", "version", "label", "name"):
                value = current.get(key)
                if value is not None:
                    return str(value)
        for key in ("version", "version_label"):
            value = self.raw.get(key)
            if value is not None:
                return str(value)
        return None

    @property
    def library(self) -> str | None:
        """Library the object was published into, when the daemon sent one."""

        value = self.raw.get("library")
        return None if value is None else str(value)

    def __repr__(self) -> str:
        suffix = f" v{self.version}" if self.version is not None else ""
        return f"Published {self.name}{suffix} (env={self.env})"

    def _repr_html_(self) -> str:
        rows = {
            "name": self.name,
            "version": self.version or "—",
            "library": self.library or "—",
            "env": self.env,
            "entrypoint": self.entrypoint,
        }
        body = "".join(
            "<tr>"
            f"<th style='text-align:left'>{escape(key)}</th>"
            f"<td><code>{escape(value)}</code></td>"
            "</tr>"
            for key, value in rows.items()
        )
        return f"<table><tbody>{body}</tbody></table>"


_CATALOG_HEADERS = ("name", "kind", "version", "library", "inputs")


def _catalog_rows(
    payload: dict[str, Any] | list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Flatten a payload (records list OR name-keyed dict) to display rows."""

    if isinstance(payload, dict):
        records = [
            {**value, "name": str(value.get("display_name") or value.get("name") or key)}
            for key, value in payload.items()
            if isinstance(value, dict)
        ]
    else:
        records = [record for record in payload if isinstance(record, dict)]

    rows: list[dict[str, str]] = []
    for record in records:
        library = record.get("library")
        library_name = (
            library.get("display_name") or library.get("slug")
            if isinstance(library, dict)
            else library
        )
        version_value = record.get("version")
        if version_value is None:
            current = record.get("current_version")
            if isinstance(current, dict):
                version_value = current.get("version") or current.get("number")
            else:
                version_value = current
        rows.append(
            {
                "name": str(record.get("display_name") or record.get("name") or ""),
                "kind": str(record.get("kind") or ""),
                "version": str(version_value or ""),
                "library": str(library_name or ""),
                "inputs": str(len(record.get("inputs") or [])),
            }
        )
    return rows


def _rows_to_text(rows: list[dict[str, str]], title: str) -> str:
    if not rows:
        return f"{title}: (empty)"
    widths = {
        header: max(len(header), *(len(row[header]) for row in rows))
        for header in _CATALOG_HEADERS
    }
    head = "  ".join(header.ljust(widths[header]) for header in _CATALOG_HEADERS)
    body = "\n".join(
        "  ".join(row[header].ljust(widths[header]) for header in _CATALOG_HEADERS)
        for row in rows
    )
    return f"{title} ({len(rows)}):\n{head}\n{body}"


def _rows_to_html(rows: list[dict[str, str]], title: str) -> str:
    head = "".join(
        f"<th style='text-align:left'>{escape(header)}</th>"
        for header in _CATALOG_HEADERS
    )
    body = "".join(
        "<tr>"
        + "".join(f"<td>{escape(row[header])}</td>" for header in _CATALOG_HEADERS)
        + "</tr>"
        for row in rows
    )
    return (
        f"<div><b>{escape(title)}</b> ({len(rows)})"
        f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"
    )


class ObjectList(list[dict[str, Any]]):
    """Object records that print as a compact table; ``.raw`` is a plain list."""

    def __repr__(self) -> str:
        return _rows_to_text(_catalog_rows(list(self)), "objects")

    def _repr_html_(self) -> str:
        return _rows_to_html(_catalog_rows(list(self)), "objects")

    @property
    def raw(self) -> list[dict[str, Any]]:
        return list(self)


class ObjectTable(dict[str, Any]):
    """Name-keyed object records with a compact table ``repr``; ``.raw`` is a plain dict."""

    def __repr__(self) -> str:
        return _rows_to_text(_catalog_rows(dict(self)), "objects")

    def _repr_html_(self) -> str:
        return _rows_to_html(_catalog_rows(dict(self)), "objects")

    @property
    def raw(self) -> dict[str, Any]:
        return dict(self)


class ObjectCatalog(dict[str, Any]):
    """Local+server catalog that prints per-scope tables; ``.raw`` is a plain dict."""

    def __repr__(self) -> str:
        return "\n\n".join(
            _rows_to_text(_catalog_rows(value), str(key)) for key, value in self.items()
        )

    def _repr_html_(self) -> str:
        return "".join(
            _rows_to_html(_catalog_rows(value), str(key)) for key, value in self.items()
        )

    @property
    def raw(self) -> dict[str, Any]:
        return dict(self)


def _wrap_objects(
    value: dict[str, Any] | list[dict[str, Any]],
) -> ObjectTable | ObjectList:
    """Wrap a payload in a view type, preserving its runtime shape."""

    return ObjectTable(value) if isinstance(value, dict) else ObjectList(value)


@dataclass(frozen=True)
class RemoteResult:
    """Completed run result plus downloaded artifact locations.

    ``payload`` is the daemon's JSON result document.  It contains the actual
    return value under ``result`` and daemon-side artifact paths under
    ``artifacts``.  ``downloaded_artifacts`` is populated only when the caller
    asks this client to download artifacts into a local directory.
    """

    run: dict[str, Any]
    payload: dict[str, Any]
    mode: str = "local"
    downloaded_artifacts: dict[str, Path] = field(default_factory=dict)

    @property
    def value(self) -> Any:
        """Return the user's JSON-compatible result value."""

        return self.payload.get("result")

    @property
    def output(self) -> Any:
        """Return the user's unwrapped result value.

        Rule: if ``payload['result']`` is a dict and contains the key
        ``'default'`` -> return ``result['default']``; else if it has exactly
        one key -> return that value; else return ``result`` as-is.
        """

        result = self.payload.get("result")
        if isinstance(result, dict):
            if DEFAULT_PORT in result:
                return result[DEFAULT_PORT]
            if len(result) == 1:
                return next(iter(result.values()))
        return result

    @property
    def artifacts(self) -> dict[str, str]:
        """Return daemon-side artifact paths keyed by artifact name."""

        artifacts = self.payload.get("artifacts")
        if isinstance(artifacts, dict):
            return cast("dict[str, str]", artifacts)
        return {}

    @property
    def server_side(self) -> bool:
        """Return whether the result came from a central-server run."""

        return self.mode == "server"

    def __repr__(self) -> str:
        return f"RemoteResult(mode={self.mode!r}, output={_preview(self.output)})"

    def _repr_html_(self) -> str:
        rows = {
            "mode": self.mode,
            "output": _preview(self.output),
            "artifacts": str(len(self.artifacts)),
        }
        body = "".join(
            "<tr>"
            f"<th style='text-align:left'>{escape(key)}</th>"
            f"<td><code>{escape(value)}</code></td>"
            "</tr>"
            for key, value in rows.items()
        )
        return f"<table><tbody>{body}</tbody></table>"


class RemoteRun:
    """Handle for a run that was started on the daemon.

    The handle is intentionally lazy.  A caller can inspect state, wait for
    completion, fetch the result, or download artifacts without remembering raw
    endpoint names.
    """

    def __init__(
        self,
        client: "SPLClient",
        state: dict[str, Any],
        *,
        server_side: bool = False,
    ):
        self._client = client
        self.state = state
        self.server_side = server_side

    @property
    def id(self) -> str:
        """Return the daemon run id."""

        return self.state["id"]

    @property
    def status(self) -> str:
        """Return the last known daemon status."""

        return self.state["status"]

    @property
    def mode(self) -> str:
        """Return ``local`` for daemon worker runs and ``server`` for remote runs."""

        return "server" if self.server_side else "local"

    def refresh(self) -> dict[str, Any]:
        """Refresh and return the run state from the daemon."""

        if self.server_side:
            self.state = self._client._daemon.get_remote_run(self.id)
        else:
            self.state = self._client._daemon.get_run(self.id)
        return self.state

    def wait(
        self,
        *,
        poll_interval: float = 0.25,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Wait until the run succeeds or fails, then return final state."""

        if self.server_side:
            self.state = self._client._daemon.wait_remote_run(
                self.id,
                poll_interval=poll_interval,
                timeout_seconds=timeout_seconds,
            )
        else:
            self.state = self._client._daemon.wait_run(
                self.id,
                poll_interval=poll_interval,
                timeout_seconds=timeout_seconds,
            )
        return self.state

    def result(self) -> dict[str, Any]:
        """Return the daemon result payload for this run."""

        if self.server_side:
            self.refresh()
            return self.state.get("result") or {}
        return self._client._daemon.result(self.id)

    def artifact_names(self) -> list[str]:
        """Return artifact names produced by this run."""

        if self.server_side:
            return self._client._daemon.list_remote_artifacts(self.id)
        return self._client._daemon.list_artifacts(self.id)

    def download_artifacts(self, target_dir: str | Path) -> dict[str, Path]:
        """Download all run artifacts into ``target_dir``."""

        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)
        downloaded: dict[str, Path] = {}
        for name in self.artifact_names():
            if self.server_side:
                downloaded[name] = self._client._daemon.download_remote_artifact(
                    self.id,
                    name,
                    target_path,
                )
            else:
                downloaded[name] = self._client._daemon.download_artifact(
                    self.id,
                    name,
                    target_path,
                )
        return downloaded

    def collect(
        self,
        *,
        artifacts_dir: str | Path | None = None,
        poll_interval: float = 0.25,
        timeout_seconds: float | None = None,
    ) -> RemoteResult:
        """Wait for completion, return result, and optionally download artifacts."""

        final_state = self.wait(
            poll_interval=poll_interval,
            timeout_seconds=timeout_seconds,
        )
        if final_state["status"] != "succeeded":
            error = final_state.get("error") or "run returned no error message"
            raise RuntimeError(
                f"{self.mode} run {self.id!r} ended as "
                f"{final_state.get('status')!r}: {error}"
            )

        payload = self.result()
        downloaded = (
            self.download_artifacts(artifacts_dir)
            if artifacts_dir is not None
            else {}
        )
        return RemoteResult(
            run=final_state,
            payload=payload,
            mode=self.mode,
            downloaded_artifacts=downloaded,
        )


class _LibraryAdmin:
    """Grouped library-management operations, reachable via ``SPLClient.library``."""

    def __init__(self, client: "SPLClient") -> None:
        self._c = client

    def list(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        return self._c.libraries(include_accessible=include_accessible)

    def create(
        self,
        slug: str,
        *,
        display_name: str | None = None,
        description: str = "",
        visibility: str = "private",
        default_machine: str | None = None,
        execution: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a central-server library owned by the connected user."""

        self._c._require_server_connection("creating a library")
        payload: dict[str, Any] = {
            "slug": slug,
            "display_name": display_name or slug,
            "description": description,
            "visibility": visibility,
        }
        if default_machine is not None:
            payload["default_machine_id"] = default_machine
        if execution is not None:
            payload["execution"] = execution
        return self._c._daemon.create_server_library(payload)

    def get(self, ref: str) -> dict[str, Any]:
        """Return one central-server library by slug or id."""

        self._c._require_server_connection("reading a library")
        return self._c._daemon.get_server_library(ref)

    def update(
        self,
        ref: str,
        *,
        display_name: str | None = None,
        description: str | None = None,
        visibility: str | None = None,
        default_machine: str | None = None,
        execution: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Update mutable metadata for one central-server library."""

        self._c._require_server_connection("updating a library")
        payload: dict[str, Any] = {}
        if display_name is not None:
            payload["display_name"] = display_name
        if description is not None:
            payload["description"] = description
        if visibility is not None:
            payload["visibility"] = visibility
        if default_machine is not None:
            payload["default_machine_id"] = default_machine
        if execution is not None:
            payload["execution"] = execution
        return self._c._daemon.update_server_library(ref, payload)

    def delete(self, ref: str) -> dict[str, Any]:
        """Delete or archive one central-server library when supported upstream."""

        self._c._require_server_connection("deleting a library")
        return self._c._daemon.delete_server_library(ref)

    def grant(
        self,
        ref: str,
        grantee: str,
        *,
        grantee_type: str = "user",
        scopes: list[str] | None = None,
    ) -> dict[str, Any]:
        """Grant a user or team access to one central-server library."""

        self._c._require_server_connection("granting library access")
        payload: dict[str, Any] = {
            "grantee_id": grantee,
            "grantee_type": grantee_type,
        }
        if scopes is not None:
            payload["scopes"] = scopes
        return self._c._daemon.grant_server_library(ref, payload)

    def revoke(self, ref: str, grantee: str) -> dict[str, Any]:
        """Revoke a grantee's access to one central-server library."""

        self._c._require_server_connection("revoking library access")
        return self._c._daemon.revoke_server_library_grant(ref, grantee)

    def add_reference(
        self,
        into_library: str,
        name: str,
        *,
        owner: str | None = None,
        from_library: str = "default",
        version: str | int | None = "latest",
        alias: str | None = None,
    ) -> dict[str, Any]:
        """Add a live reference entry from another library into ``into_library``."""

        self._c._require_server_connection("adding a library reference")
        payload: dict[str, Any] = {
            "name": name,
            "from_library": from_library,
        }
        if owner is not None:
            payload["from_owner"] = owner
        if version is not None:
            payload["version"] = version
        if alias is not None:
            payload["alias"] = alias
        return self._c._daemon.add_server_library_reference(into_library, payload)

    def copy_object(
        self,
        name: str,
        *,
        into_library: str,
        from_owner: str | None = None,
        from_library: str = "default",
        version: str | int | None = "latest",
        new_name: str | None = None,
    ) -> dict[str, Any]:
        """Copy an object snapshot into a library owned by the connected user."""

        self._c._require_server_connection("copying an object into a library")
        payload: dict[str, Any] = {
            "name": name,
            "from_library": from_library,
        }
        if from_owner is not None:
            payload["from_owner"] = from_owner
        if version is not None:
            payload["version"] = version
        if new_name is not None:
            payload["new_name"] = new_name
        return self._c._daemon.copy_server_library_object(into_library, payload)

    def remove_entry(self, library: str, name: str) -> dict[str, Any]:
        """Remove an owned object or reference entry from a central-server library."""

        self._c._require_server_connection("removing a library entry")
        return self._c._daemon.remove_server_library_entry(library, name)


class SPLClient:
    """High-level client used by SPL users to interact with the local daemon."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        daemon_host: str = DEFAULT_DAEMON_HOST,
        daemon_port: int | None = None,
        daemon_home: str | Path | None = None,
        machine_token: str | None = None,
        user_token: str | None = None,
        server_url: str = DEFAULT_SERVER_URL,
        machine_id: str | None = None,
        display_name: str | None = None,
        capabilities: dict[str, Any] | None = None,
        heartbeat_interval_seconds: float | None = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        api_token: str | None = None,
    ):
        self._daemon = Client(
            base_url,
            daemon_host=daemon_host,
            daemon_port=daemon_port,
            daemon_home=daemon_home,
            api_token=api_token,
        )
        self.server_connection: dict[str, Any] | None = None
        self._user_token: str | None = None
        if machine_token is not None or user_token is not None:
            if not machine_token or not user_token:
                raise ValueError("machine_token and user_token must be provided together")
            self.server_connection = self.connect_server(
                machine_token=machine_token,
                user_token=user_token,
                server_url=server_url,
                machine_id=machine_id,
                display_name=display_name,
                capabilities=capabilities,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
            )

    def health(self) -> dict[str, Any]:
        """Check that the local daemon is reachable."""

        return self._daemon.health()

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
        """Connect the local daemon to the central daemon server.

        Calling this method is optional.  A plain ``SPLClient()`` remains fully
        local and never contacts the central server.
        """

        connection = self._daemon.connect_server(
            machine_token=machine_token,
            user_token=user_token,
            server_url=server_url,
            machine_id=machine_id,
            display_name=display_name,
            capabilities=capabilities,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )
        self._user_token = user_token
        self.server_connection = connection
        return self.server_connection

    def disconnect_server(self) -> dict[str, Any]:
        """Gracefully disconnect the local daemon from the central server."""

        response = self._daemon.disconnect_server()
        self._user_token = None
        self.server_connection = None
        return response

    @property
    def server(self) -> SPLServerClient:
        """Advanced direct central-server client for callers with a user token."""

        if self._user_token is None:
            raise RuntimeError(
                "SPLClient.server requires a user token supplied to "
                "SPLClient(..., user_token=...) or connect_server(...)."
            )
        conn = self.current_server_connection()
        connection = conn.get("connection")
        nested_url = (
            connection.get("server_url")
            if isinstance(connection, dict)
            else None
        )
        server_url = conn.get("server_url") or nested_url or DEFAULT_SERVER_URL
        return SPLServerClient(token=self._user_token, base_url=server_url)

    @property
    def library(self) -> _LibraryAdmin:
        """Grouped library administration (create/grant/reference/copy/...)."""

        return _LibraryAdmin(self)

    def current_server_connection(self) -> dict[str, Any]:
        """Return local daemon state for the central-server connection."""

        try:
            state = self._daemon.server_connection()
        except Exception as exc:
            if _is_missing_server_connection(exc):
                return {"connected": False, "offline": False, "connection": None}
            raise
        state.setdefault("connected", bool(state.get("server_url")))
        return state

    def machines(self) -> dict[str, Any]:
        """Return the user's machines, or an empty listing when not connected."""

        if not self._has_server_connection():
            return {"current_machine_id": None, "machines": []}
        return self._daemon.server_machines()

    def libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        """Return visible libraries, or an empty list when not connected."""

        if not self._has_server_connection():
            return []
        return self._daemon.server_libraries(include_accessible=include_accessible)

    def create_library(
        self,
        slug: str,
        *,
        display_name: str | None = None,
        description: str = "",
        visibility: str = "private",
        default_machine: str | None = None,
        execution: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Deprecated alias of :meth:`library.create` (see docs/migration-0.2.0.md)."""

        _warn_deprecated("SPLClient.create_library()", "SPLClient.library.create()")
        return self.library.create(
            slug,
            display_name=display_name,
            description=description,
            visibility=visibility,
            default_machine=default_machine,
            execution=execution,
        )

    def get_library(self, ref: str) -> dict[str, Any]:
        """Deprecated alias of :meth:`library.get`."""

        _warn_deprecated("SPLClient.get_library()", "SPLClient.library.get()")
        return self.library.get(ref)

    def update_library(
        self,
        ref: str,
        *,
        display_name: str | None = None,
        description: str | None = None,
        visibility: str | None = None,
        default_machine: str | None = None,
        execution: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Deprecated alias of :meth:`library.update`."""

        _warn_deprecated("SPLClient.update_library()", "SPLClient.library.update()")
        return self.library.update(
            ref,
            display_name=display_name,
            description=description,
            visibility=visibility,
            default_machine=default_machine,
            execution=execution,
        )

    def delete_library(self, ref: str) -> dict[str, Any]:
        """Deprecated alias of :meth:`library.delete`."""

        _warn_deprecated("SPLClient.delete_library()", "SPLClient.library.delete()")
        return self.library.delete(ref)

    def grant_library(
        self,
        ref: str,
        grantee: str,
        *,
        grantee_type: str = "user",
        scopes: list[str] | None = None,
    ) -> dict[str, Any]:
        """Deprecated alias of :meth:`library.grant`."""

        _warn_deprecated("SPLClient.grant_library()", "SPLClient.library.grant()")
        return self.library.grant(
            ref,
            grantee,
            grantee_type=grantee_type,
            scopes=scopes,
        )

    def revoke_library_grant(self, ref: str, grantee: str) -> dict[str, Any]:
        """Deprecated alias of :meth:`library.revoke`."""

        _warn_deprecated(
            "SPLClient.revoke_library_grant()", "SPLClient.library.revoke()",
        )
        return self.library.revoke(ref, grantee)

    def add_reference(
        self,
        into_library: str,
        name: str,
        *,
        owner: str | None = None,
        from_library: str = "default",
        version: str | int | None = "latest",
        alias: str | None = None,
    ) -> dict[str, Any]:
        """Deprecated alias of :meth:`library.add_reference`."""

        _warn_deprecated(
            "SPLClient.add_reference()", "SPLClient.library.add_reference()",
        )
        return self.library.add_reference(
            into_library,
            name,
            owner=owner,
            from_library=from_library,
            version=version,
            alias=alias,
        )

    def copy_object(
        self,
        name: str,
        *,
        into_library: str,
        from_owner: str | None = None,
        from_library: str = "default",
        version: str | int | None = "latest",
        new_name: str | None = None,
    ) -> dict[str, Any]:
        """Deprecated alias of :meth:`library.copy_object`."""

        _warn_deprecated("SPLClient.copy_object()", "SPLClient.library.copy_object()")
        return self.library.copy_object(
            name,
            into_library=into_library,
            from_owner=from_owner,
            from_library=from_library,
            version=version,
            new_name=new_name,
        )

    def remove_entry(self, library: str, name: str) -> dict[str, Any]:
        """Deprecated alias of :meth:`library.remove_entry`."""

        _warn_deprecated("SPLClient.remove_entry()", "SPLClient.library.remove_entry()")
        return self.library.remove_entry(library, name)

    def register_env(self, name: str = "default", python: str | None = None) -> dict[str, Any]:
        """Register a Python executable as a daemon environment.

        By default the daemon registers its own interpreter.  This keeps the
        simplest local workflow working both when the daemon runs natively and
        when it runs in a container:

            client.register_env()
            client.publish(my_function, env="default")
        """

        return self._daemon.register_env(name, python)

    def publish(
        self,
        obj: Any,
        *,
        name: str | None = None,
        env: str = "default",
        entrypoint: str | None = None,
        workdir: str | None = None,
        runtime_config: dict[str, Any] | str | Path | None = None,
        runtime: str | None = None,
        python: str | None = None,
        base_image: str | None = None,
        dependency_frame_offset: int = 0,
        library: str | None = None,
        create: bool = False,
        library_display_name: str | None = None,
        local_only: bool = False,
    ) -> PublishedObject:
        """Serialize a live function/pipeline and store it in the daemon.

        ``name`` is the daemon registry name.  ``entrypoint`` is the object name
        inside the generated SPL/YAML file.  They can differ, which lets a user
        publish the same function under several daemon aliases.

        ``dependency_frame_offset`` is only needed when ``publish`` itself is
        wrapped by user helper functions.  Leave it at ``0`` for direct notebook
        use.

        ``library`` targets a central-server library during sync.  Missing
        non-default libraries are rejected unless ``create=True`` is passed.
        """

        yaml_text, resolved_entrypoint = export_object_to_yaml(
            obj,
            entrypoint,
            frame_offset=4 + dependency_frame_offset,
        )
        registry_name = name or resolved_entrypoint
        record = self._daemon.register_object(
            registry_name,
            entrypoint=resolved_entrypoint,
            env=env,
            yaml_text=yaml_text,
            workdir=workdir,
            runtime_config=build_runtime_config(
                runtime_config,
                runtime=runtime,
                python=python,
                base_image=base_image,
            ),
            library=library,
            create_library=create,
            library_display_name=library_display_name,
            local_only=local_only,
        )
        return PublishedObject(
            name=record["name"],
            entrypoint=record["entrypoint"],
            env=record["env"],
            yaml_path=record["yaml_path"],
            workdir=record.get("workdir"),
            raw=record,
        )

    def publish_yaml(
        self,
        yaml: str | Path,
        *,
        name: str,
        entrypoint: str,
        env: str = "default",
        workdir: str | None = None,
        runtime_config: dict[str, Any] | str | Path | None = None,
        runtime: str | None = None,
        python: str | None = None,
        base_image: str | None = None,
        library: str | None = None,
        create: bool = False,
        library_display_name: str | None = None,
        local_only: bool = False,
    ) -> PublishedObject:
        """Store an already generated SPL/YAML document in the daemon.

        ``yaml`` can be YAML text or a path to a YAML file.  A string is treated
        as a path when it points to an existing file; otherwise it is sent as
        YAML text.  This method covers the explicit requirement "send generated
        YAML" and is useful when the object was exported earlier or produced by
        another process.  ``create=True`` asks the server to create the target
        library if it does not already exist.
        """

        yaml_text = read_yaml_input(yaml)
        record = self._daemon.register_object(
            name,
            entrypoint=entrypoint,
            env=env,
            yaml_text=yaml_text,
            workdir=workdir,
            runtime_config=build_runtime_config(
                runtime_config,
                runtime=runtime,
                python=python,
                base_image=base_image,
            ),
            library=library,
            create_library=create,
            library_display_name=library_display_name,
            local_only=local_only,
        )
        return PublishedObject(
            name=record["name"],
            entrypoint=record["entrypoint"],
            env=record["env"],
            yaml_path=record["yaml_path"],
            workdir=record.get("workdir"),
            raw=record,
        )

    def local_objects(self, *, compact: bool = False) -> list[dict[str, Any]]:
        """Deprecated alias of :meth:`objects` with ``scope='local'``."""

        _warn_deprecated("SPLClient.local_objects()", "SPLClient.objects(scope='local')")
        return self._object_records(self.objects(scope="local", compact=compact))

    def server_objects(
        self,
        *,
        owner: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        """Deprecated alias of :meth:`objects` with ``scope='server'``."""

        _warn_deprecated(
            "SPLClient.server_objects()", "SPLClient.objects(scope='server')",
        )
        return list(
            self.objects(
                scope="server",
                owner=owner,
                library=library,
                compact=compact,
            )
        )

    @staticmethod
    def _object_records(records: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
        if isinstance(records, list):
            return list(records)
        return [
            dict(record) if isinstance(record, dict) else {"name": name, "value": record}
            for name, record in records.items()
        ]

    @overload
    def objects(
        self,
        *,
        compact: bool = False,
        scope: Literal["local"],
        owner: None = None,
        library: None = None,
    ) -> ObjectTable: ...

    @overload
    def objects(
        self,
        *,
        compact: bool = False,
        scope: Literal["server"],
        owner: str | None = None,
        library: str | None = None,
    ) -> ObjectList: ...

    @overload
    def objects(
        self,
        *,
        compact: bool = False,
        scope: Literal["all"],
        owner: str | None = None,
        library: str | None = None,
    ) -> ObjectCatalog: ...

    @overload
    def objects(
        self,
        *,
        compact: bool = False,
        scope: Literal["auto"] = "auto",
        owner: str | None = None,
        library: str | None = None,
    ) -> ObjectTable | ObjectList: ...

    def objects(
        self,
        *,
        compact: bool = False,
        scope: ObjectScope = "auto",
        owner: str | None = None,
        library: str | None = None,
    ) -> ObjectTable | ObjectList | ObjectCatalog:
        """Return objects from the local cache, server catalog, or both.

        The returned views subclass ``dict``/``list``, so indexing, iteration,
        and ``json.dumps`` keep working; they only add a compact ``repr`` and a
        ``.raw`` property with the plain payload.
        """

        if scope == "auto":
            scope = (
                "server"
                if owner is not None or library is not None or self._has_server_connection()
                else "local"
            )
        if scope == "local":
            if owner is not None or library is not None:
                raise ValueError("owner/library require scope='server', scope='all', or scope='auto'")
            return _wrap_objects(self._daemon.list_objects(compact=compact))
        if scope == "server":
            if owner is None and library is None and not self._has_server_connection():
                return ObjectList([])
            return _wrap_objects(
                self._daemon.server_objects(
                    owner_id=owner,
                    library=library,
                    compact=compact,
                )
            )
        if scope == "all":
            local_objects = self._daemon.list_objects(compact=compact)
            server_objects = (
                []
                if owner is None and library is None and not self._has_server_connection()
                else self._daemon.server_objects(
                    owner_id=owner,
                    library=library,
                    compact=compact,
                )
            )
            return ObjectCatalog(
                {
                    "local": local_objects,
                    "server": server_objects,
                }
            )
        raise ValueError("scope must be 'auto', 'local', 'server', or 'all'")

    def forget(
        self,
        name: str,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove one local daemon object without requiring a server connection."""

        return self._daemon.forget(name, owner_id=owner, library=library)

    def remove_local(
        self,
        name: str,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Alias for :meth:`forget`."""

        return self.forget(name, owner=owner, library=library)

    def forget_version(
        self,
        name: str,
        version: str | int,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove one local object version without contacting the server."""

        return self._daemon.forget_version(
            name,
            version,
            owner_id=owner,
            library=library,
        )

    def prune_stale_mirrors(
        self,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove locally cached server-origin mirror rows."""

        return self._daemon.prune_stale_mirrors(owner_id=owner, library=library)

    def _has_server_connection(self) -> bool:
        if self.server_connection is not None:
            return bool(self.server_connection.get("connected"))
        try:
            state = self._daemon.server_connection()
        except Exception as exc:
            if _is_missing_server_connection(exc):
                return False
            raise
        if bool(state.get("connected")):
            return True
        connection = state.get("connection") or state.get("remote_connection") or {}
        return connection.get("status") == "connected"

    def _require_server_connection(self, operation: str) -> None:
        try:
            state = self._daemon.server_connection()
        except Exception as exc:
            raise RuntimeError(
                f"{operation} requires a server-connected SPLClient. "
                "Construct SPLClient(machine_token=..., user_token=...) or call "
                "client.connect_server(...) first."
            ) from exc
        if state.get("connected"):
            self.server_connection = state
            return
        raise RuntimeError(
            f"{operation} requires a server-connected SPLClient. "
            "Construct SPLClient(machine_token=..., user_token=...) or call "
            "client.connect_server(...) first."
        )

    def signature(
        self,
        name: str,
        *,
        version: int | None = None,
        owner: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> dict[str, Any]:
        """Return a concise call/read signature for one daemon object."""

        return self._daemon.signature(
            name,
            version=version,
            owner_id=owner,
            library=library,
            function=function,
        )

    def inputs(
        self,
        name: str,
        *,
        version: int | None = None,
        owner: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return the inputs that can be passed through ``kwargs``."""

        return self._daemon.inputs(
            name,
            version=version,
            owner_id=owner,
            library=library,
            function=function,
        )

    def outputs(
        self,
        name: str,
        *,
        version: int | None = None,
        owner: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return output selectors and how to read ``result.value``."""

        return self._daemon.outputs(
            name,
            version=version,
            owner_id=owner,
            library=library,
            function=function,
        )

    def decomposition(
        self,
        name: Any,
        *,
        version: int | None = None,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Return normalized function/node/link metadata for one object."""

        if self._is_node_remote(name):
            return self._remote_decomposition_response(name, version=version)["decomposition"]
        if owner is not None or library is not None:
            return self._remote_decomposition_response(
                {
                    "name": str(name),
                    "version": version,
                    "owner_id": owner,
                    "library": library,
                }
            )["decomposition"]
        return self._daemon.decomposition(str(name), version=version)

    def pipeline_widget(
        self,
        pipeline: Any,
        *,
        version: int | None = None,
        title: str | None = None,
        height: int = 560,
        theme: str = "dark",
    ) -> Any:
        """Return a rich Jupyter display object for a pipeline graph.

        ``pipeline`` can be a registered object name, a ``PublishedObject``, or
        a live ``spl.core.entities.pipeline.Pipeline`` instance.  In notebooks,
        use it as the last expression in a cell or call ``.display()`` on the
        returned object.
        """

        from spl.core.entities.node_remote import NodeRemote
        from spl.core.entities.pipeline import Pipeline
        from spl.pipeline_widget import PipelineGraphWidget, pipeline_to_decomposition

        if isinstance(pipeline, PublishedObject):
            pipeline = pipeline.name

        if isinstance(pipeline, NodeRemote):
            if version is not None and pipeline.version not in {"", "latest", "current", "TODO"}:
                raise ValueError("pass the version either on NodeRemote or draw_pipeline(...), not both")
            response = self._remote_decomposition_response(pipeline, version=version)
            decomposition = response["decomposition"]
            if not decomposition.get("nodes"):
                raise ValueError(f"remote object is not a pipeline or has no nodes: {pipeline.name}")
            record = response.get("object") or {}
            remote = response.get("remote") or {}
            object_name = (
                title
                or record.get("display_name")
                or record.get("name")
                or remote.get("name")
                or pipeline.name
            )
            return PipelineGraphWidget(
                decomposition,
                {
                    **record,
                    "remote": remote,
                    "id": record.get("id") or remote.get("object_id") or pipeline.name,
                    "name": record.get("name") or remote.get("name") or pipeline.name,
                    "displayName": object_name,
                },
                height=height,
                theme=theme,
            )

        if isinstance(pipeline, Pipeline):
            if version is not None:
                raise ValueError("version is only supported for registered objects")
            object_name = title or pipeline.name or "Pipeline"
            return PipelineGraphWidget(
                pipeline_to_decomposition(pipeline),
                {
                    "id": pipeline.name or "pipeline",
                    "name": object_name,
                    "displayName": object_name,
                },
                height=height,
                theme=theme,
            )

        if isinstance(pipeline, str):
            record = self._daemon.get_object(
                pipeline,
                version=version,
                include_yaml=True,
            )
            decomposition = record.get("decomposition") or self.decomposition(
                pipeline,
                version=version,
            )
            if not decomposition.get("nodes"):
                raise ValueError(f"object is not a pipeline or has no nodes: {pipeline}")
            object_name = title or record.get("display_name") or record.get("name") or pipeline
            return PipelineGraphWidget(
                decomposition,
                {
                    **record,
                    "id": record.get("id") or pipeline,
                    "name": record.get("name") or pipeline,
                    "displayName": object_name,
                },
                height=height,
                theme=theme,
            )

        raise TypeError(
            "pipeline_widget expects an object name, PublishedObject, "
            "spl.core Pipeline, or NodeRemote"
        )

    def draw_pipeline(
        self,
        pipeline: Any,
        *,
        version: int | None = None,
        title: str | None = None,
        height: int = 560,
        theme: str = "dark",
    ) -> Any:
        """Alias for ``pipeline_widget`` with a notebook-oriented name."""

        return self.pipeline_widget(
            pipeline,
            version=version,
            title=title,
            height=height,
            theme=theme,
        )

    def describe(
        self,
        name: str,
        *,
        version: int | None = None,
        owner: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> str:
        """Return a readable object description for notebooks and logs."""

        signature = self.signature(
            name,
            version=version,
            owner=owner,
            library=library,
            function=function,
        )
        display_name = signature.get("display_name") or signature["name"]
        lines = [
            (
                f"{display_name} "
                f"v{signature['version']} ({signature['kind']})"
            )
        ]
        if signature.get("description"):
            lines.append(signature["description"])

        if (
            function is None
            and signature.get("kind") == "pipeline"
            and signature.get("internal_functions")
        ):
            lines.append("Functions:")
            for item in signature["internal_functions"]:
                lines.append(f"  - {item['name']}")

        lines.append("Inputs:")
        if signature["inputs"]:
            for item in signature["inputs"]:
                required = "required" if item["required"] else "optional"
                default = (
                    ""
                    if item["default"] is None
                    else f", default={item['default']}"
                )
                lines.append(
                    f"  - {item['name']}: {item['type'] or 'Any'} "
                    f"({required}{default})"
                )
        else:
            lines.append("  - none")

        lines.append("Outputs:")
        if signature["outputs"]:
            for item in signature["outputs"]:
                selector = (
                    f'output="{item["selector"]}"'
                    if item["selector"] is not None
                    else "no output selector"
                )
                lines.append(
                    f"  - {item['name']}: {selector}; read {item['read']}"
                )
        else:
            lines.append("  - none")

        lines.append(f"Example: {signature['call']['example']}")
        lines.append(f"Read: {signature['call']['read']}")
        return "\n".join(lines)

    def envs(self) -> dict[str, Any]:
        """Return registered daemon environments."""

        return self._daemon.list_envs()

    def environment_builds(self) -> list[dict[str, Any]]:
        """Return cached daemon venv builds."""

        return self._daemon.list_environment_builds()

    def rebuild_environment(
        self,
        spec_hash: str,
        *,
        wait: bool = False,
    ) -> dict[str, Any]:
        """Force a cached daemon venv build to be recreated."""

        return self._daemon.rebuild_environment_build(spec_hash, wait=wait)

    def runs(self) -> list[dict[str, Any]]:
        """Return known daemon runs, newest first."""

        return self._daemon.list_runs()

    def _start_run(
        self,
        name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        offline_policy: OfflinePolicy | None = None,
        function: str | None = None,
        source: RunSource = "auto",
    ) -> RemoteRun:
        """Shared implementation behind ``submit``/``call`` (and legacy aliases)."""

        remote = target_machine is not None or owner is not None or library is not None
        state = self._daemon.run(
            name,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            target_machine=target_machine,
            object_owner_id=owner,
            library=library,
            offline_policy=offline_policy,
            function=function,
            source=source,
            remote=remote or None,
        )
        return RemoteRun(self, state, server_side=remote)

    def start(self, name: str, **kwargs: Any) -> RemoteRun:
        """Deprecated alias of :meth:`submit` (same signature and behavior)."""

        _warn_deprecated("SPLClient.start()", "SPLClient.submit()")
        return self._start_run(name, **kwargs)

    def submit(
        self,
        name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        offline_policy: OfflinePolicy | None = None,
        function: str | None = None,
        source: RunSource = "auto",
    ) -> RemoteRun:
        """Canonical async entry point: start a run, return a handle immediately.

        The default path is local daemon execution.  Passing ``target_machine``,
        ``owner``, or ``library`` intentionally selects central-server remote
        execution through the connected daemon.
        """

        return self._start_run(
            name,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            target_machine=target_machine,
            owner=owner,
            library=library,
            offline_policy=offline_policy,
            function=function,
            source=source,
        )

    def queue(
        self,
        name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        target_machine: str,
        owner: str | None = None,
        library: str | None = None,
        function: str | None = None,
        source: RunSource = "auto",
    ) -> RemoteRun:
        """Deprecated alias of :meth:`submit` with ``offline_policy='queue'``."""

        _warn_deprecated(
            "SPLClient.queue()", "SPLClient.submit(..., offline_policy='queue')",
        )
        return self._start_run(
            name,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            target_machine=target_machine,
            owner=owner,
            library=library,
            function=function,
            offline_policy="queue",
            source=source,
        )

    def call(
        self,
        name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        artifacts_dir: str | Path | None = None,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        offline_policy: OfflinePolicy | None = None,
        function: str | None = None,
        source: RunSource = "auto",
    ) -> RemoteResult:
        """Run an object, wait for completion, and return result/artifacts.

        With only ``name``/``args``/``kwargs`` this is a local daemon worker
        call.  Passing ``target_machine``, ``owner``, or ``library`` makes it a
        server-side remote run through the daemon.  The returned
        ``RemoteResult.mode`` is therefore either ``"local"`` or ``"server"``.
        """

        run = self._start_run(
            name,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            target_machine=target_machine,
            owner=owner,
            library=library,
            offline_policy=offline_policy,
            function=function,
            source=source,
        )
        return run.collect(
            artifacts_dir=artifacts_dir,
            timeout_seconds=timeout_seconds,
        )

    def _run_node_value(
        self,
        node: Any,
        kwargs: dict[str, Any],
        *,
        timeout_seconds: float | None = None,
    ) -> Any:
        """Run a ``NodeRemote`` and return its value (internal; used by Deployment)."""

        payload = self._remote_node_payload(node)
        response = self._daemon.run_remote_node(
            payload,
            kwargs=kwargs,
            timeout_seconds=timeout_seconds,
        )
        return response.get("value")

    def run_node(
        self,
        node: Any,
        kwargs: dict[str, Any],
        *,
        timeout_seconds: float | None = None,
    ) -> Any:
        """Deprecated: run a ``NodeRemote`` directly.

        Wire the node into a pipeline instead:
        ``Deployment(client, lift(node)...).run(...)``.
        """

        _warn_deprecated(
            "SPLClient.run_node()",
            "Deployment(client, pipeline).run(...) with NodeRemote.locate(...)",
        )
        return self._run_node_value(node, kwargs, timeout_seconds=timeout_seconds)

    def run_node_result(
        self,
        node: Any,
        *,
        kwargs: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> RemoteResult:
        """Deprecated: run a ``NodeRemote`` and return metadata plus the value.

        Wire the node into a pipeline instead:
        ``Deployment(client, pipeline).run(...)``.
        """

        _warn_deprecated(
            "SPLClient.run_node_result()",
            "Deployment(client, pipeline).run(...) with NodeRemote.locate(...)",
        )
        payload = self._remote_node_payload(node)
        response = self._daemon.run_remote_node(
            payload,
            kwargs=kwargs or {},
            timeout_seconds=timeout_seconds,
        )
        value = response.get("value")
        raw_payload = response.get("payload")
        result_payload = dict(raw_payload) if isinstance(raw_payload, dict) else {}
        result_payload["result"] = value
        result_payload.setdefault("artifacts", response.get("artifacts") or {})

        run = response.get("run")
        if not isinstance(run, dict):
            run = {
                "id": response.get("run_id"),
                "status": response.get("status") or "succeeded",
            }
        return RemoteResult(
            run=run,
            payload=result_payload,
            mode="server",
            downloaded_artifacts={},
        )

    def _is_node_remote(self, value: Any) -> bool:
        try:
            from spl.core.entities.node_remote import NodeRemote
        except Exception:
            return False
        return isinstance(value, NodeRemote)

    def _remote_node_payload(
        self,
        node: Any,
        *,
        version: int | str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "uuid": str(node.uuid),
            "url": getattr(node, "url", ""),
            "name": node.name,
            "version": node.version if version is None else version,
        }
        for attr in ("target_machine", "owner_id", "library"):
            value = getattr(node, attr, None)
            if value is not None:
                payload[attr] = value
        return payload

    def _remote_decomposition_response(
        self,
        remote: Any,
        *,
        version: int | None = None,
    ) -> dict[str, Any]:
        ref = (
            self._remote_node_payload(remote, version=version)
            if self._is_node_remote(remote)
            else dict(remote)
        )
        if version is not None:
            ref["version"] = version
        return self._daemon.resolve_remote_decomposition(ref)


def export_object_to_yaml(
    obj: Any,
    entrypoint: str | None = None,
    *,
    frame_offset: int = 3,
) -> tuple[str, str]:
    """Serialize a live SPL object to YAML text.

    The existing core exporter writes to a file and assumes it was called
    directly from the user's module/notebook.  This helper uses the same core IR
    utilities with an explicit frame offset.  That keeps notebook-defined
    functions working without changing ``spl.core``.
    """

    export_obj, resolved_entrypoint = prepare_export_object(obj, entrypoint)
    return export_objects_to_yaml([export_obj], frame_offset=frame_offset), resolved_entrypoint


def read_yaml_input(yaml: str | Path) -> str:
    """Read YAML text from a path-like value or return raw YAML text.

    Notebook examples often use ``Path('spl/demo/_bundle.yaml')``.  Shell-style
    snippets often use the same path as a string.  Supporting both keeps the
    user API small without adding a separate ``publish_yaml_file`` method.
    """

    if isinstance(yaml, Path):
        return yaml.read_text(encoding="utf-8")

    possible_path = Path(yaml)
    if "\n" not in yaml and possible_path.exists():
        return possible_path.read_text(encoding="utf-8")

    return yaml


def build_runtime_config(
    runtime_config: dict[str, Any] | str | Path | None = None,
    *,
    runtime: str | None = None,
    python: str | None = None,
    base_image: str | None = None,
) -> dict[str, Any] | None:
    """Build a daemon runtime config from explicit options or a sidecar file."""

    config: dict[str, Any]
    if runtime_config is None:
        config = {}
    elif isinstance(runtime_config, dict):
        config = dict(runtime_config)
    else:
        import yaml

        loaded = yaml.safe_load(Path(runtime_config).read_text(encoding="utf-8"))
        if loaded is None:
            config = {}
        elif isinstance(loaded, dict):
            config = loaded
        else:
            raise ValueError("runtime_config file must contain a YAML mapping")

    if "runtime" in config and isinstance(config["runtime"], dict):
        target = dict(config["runtime"])
        config = {"runtime": target}
    else:
        target = config

    if runtime is not None:
        target["mode"] = runtime
    if python is not None:
        target["python"] = python
    if base_image is not None:
        target["base_image"] = base_image

    if not config and runtime is None and python is None and base_image is None:
        return None
    return config


def export_objects_to_yaml(xs: list[Any], *, frame_offset: int = 2) -> str:
    """Serialize SPL objects to one YAML bundle.

    ``frame_offset`` is passed to the existing dependency scanner.  Use ``2``
    when this helper is called directly by user code, and ``3`` when it is
    called through ``SPLClient.publish``.  This mirrors the hard-coded offset in
    ``spl.core.ir.utils.spl_export_to_file`` while allowing this client wrapper
    to stay compatible with notebook globals such as ``np``, ``sympy`` and
    ``XGBRegressor``.
    """

    import yaml

    from spl.core.entities.control import DSPLSelfImport
    from spl.core.ir.parse import get_top_level_deps

    top_level_deps = get_top_level_deps(frame_offset, xs)

    mapping = {
        root: DSPLSelfImport(name=cast(Any, root).name)
        for (root, _) in top_level_deps
        if hasattr(root, "name")
    }

    normalized_deps = {
        root: [mapping.get(dependency, dependency) for dependency in dependencies]
        for root, dependencies in top_level_deps
    }

    return yaml.dump_all(
        [[root, *dependencies] for root, dependencies in normalized_deps.items()],
        sort_keys=False,
        allow_unicode=True,
    )


def prepare_export_object(obj: Any, entrypoint: str | None) -> tuple[Any, str]:
    """Return an object ready for core export and the exported entrypoint name."""

    from spl.core.entities.pipeline import Pipeline

    if isinstance(obj, Pipeline):
        if entrypoint is None:
            if obj.name is None:
                raise ValueError(
                    "unnamed pipeline requires entrypoint; "
                    "use pipeline.render(name) or publish(..., entrypoint='name')"
                )
            return obj, obj.name
        return replace(obj, name=entrypoint), entrypoint

    if callable(obj) and hasattr(obj, "__name__"):
        function_name = obj.__name__
        if entrypoint is not None and entrypoint != function_name:
            raise ValueError(
                "function entrypoint must match function.__name__; "
                "use publish(..., name='daemon_alias') for daemon aliases"
            )
        return obj, function_name

    raise TypeError("SPL client can publish a Python function or spl.core Pipeline")
