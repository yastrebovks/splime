"""User-facing client for publishing and running SPL objects on the daemon.

This module is the thin "framework side" of the daemon integration.  Code that
already uses SPL should not need to know about HTTP endpoints, run directories,
or worker subprocesses.  The intended workflow is:

    from spl import SPLClient

    client = SPLClient()
    client.publish(my_function, name="sum", env="default")
    result = client.call("sum", kwargs={"x": 1, "y": 2})

The module only imports ``spl.core`` inside export helpers.  That keeps basic
registry operations, such as listing remote objects, usable even from a small
environment that has the daemon client but does not currently have all core
dependencies imported yet.
"""

from __future__ import annotations

import builtins
import enum
import hashlib
import os
import re
import stat
import tempfile
import warnings
from collections.abc import Mapping
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field, replace
from html import escape
from pathlib import Path
from typing import Any, Literal, cast, overload

from spl._views import (
    _owner_label,
    _server_resolution_label,
    ActionReceiptView,
    ConnectionStatusView,
    DecompositionView,
    EnvTableView,
    EnvironmentBuildListView,
    HealthView,
    InputListView,
    LibraryListView,
    MachineListView,
    OutputListView,
    RunListView,
    RunRecordView,
    SignatureView,
    VersionListView,
    object_catalog_title,
    object_scope_title,
    wrap_action,
)
from spl._owner_ref import normalize_owner_ref
from spl._runtime_port_adapters_client import (
    PreparedClientRuntimeAdapters,
    prepare_client_runtime_adapters,
    requires_runtime_adapter_path,
    server_admission_document,
)
from spl.adapters import BUILTIN_ADAPTER_IDS, ArtifactHandle, OPAQUE_FILE, get_builtin_adapter
from spl.core import manifest as m_manifest
from spl.core.entities.node import DEFAULT_PORT
from spl.core.library_adapters import (
    LIBRARY_ADAPTER_CATALOG_CAPABILITY,
    RUNTIME_LIBRARY_ADAPTER_REF_CAPABILITY,
    LibraryAdapterRef,
    normalize_library_adapter_ref,
)
from spl.core.runtime_port_adapters import (
    RuntimePortAdapterContractError,
    normalize_adapter_policy,
    normalize_public_adapter_mapping,
    normalize_runtime_output_record,
)
from spl.core.publications import (
    PUBLIC_ADAPTER_PROFILE_CAPABILITY,
    PUBLIC_OBJECT_PROFILE_CAPABILITY,
    UNSET,
)
from spl.daemon_client import (
    DEFAULT_DAEMON_HOST,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_SERVER_URL,
    Client,
    ClientError,
    RunProgressPrinter,
    RunStateCallback,
)
from spl.server_client import SPLServerClient

OfflinePolicy = Literal["queue", "wait", "fail_fast"]
ObjectScope = Literal["auto", "local", "server", "all"]
REMOTE_RUN_POLL_INTERVAL_SECONDS = 1.0
RunSource = Literal["auto", "local"]
RunIdNamespace = Literal["local", "daemon", "unknown"]
ProgressOption = bool | RunStateCallback
_NO_SERVER_CONNECTION_MESSAGE = "active server connection is not found"
_SERVER_UNREACHABLE_CODE = "central_server_unreachable"
_SERVER_UNREACHABLE_MESSAGES = (
    "central SPL daemon server is not reachable",
    "central SPL server is not reachable",
)
_LIBRARY_DELETE_UNSUPPORTED_MESSAGE = (
    "Deleting central-server libraries is not supported by the SPL server API. "
    "Use the Console archive action to hide a library, or remove individual "
    "entries with client.library.remove_entry()."
)
_LOCAL_CACHE_ONLY_WARNING = "local cache only; server copies (if any) stay visible in objects() while connected"
_LOCAL_RUN_ID_RE = re.compile(r"\d{8}T\d{6}Z-[0-9a-fA-F]{12}")
_DAEMON_RUN_ID_RE = re.compile(r"[0-9a-fA-F]{32}")


class _DiscardAdapterOutput:
    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        return None


_DISCARD_ADAPTER_OUTPUT = _DiscardAdapterOutput()


class _NativeResultState(enum.Enum):
    ABSENT = "absent"


_NO_NATIVE_VALUE = _NativeResultState.ABSENT


def _preview(value: Any, *, limit: int = 80) -> str:
    preview = repr(value)
    if len(preview) <= limit:
        return preview
    return f"{preview[: limit - 3]}..."


def _native_preview(value: Any) -> str:
    """Summarize an in-memory result without rendering or serializing it."""
    if type(value) in {type(None), bool, int, float, str}:
        return _preview(value)
    typ = type(value)
    type_name = typ.__qualname__ if typ.__module__ == "builtins" else f"{typ.__module__}.{typ.__qualname__}"
    try:
        shape = getattr(value, "shape", None)
    except Exception:
        shape = None
    if type(shape) is tuple and all(type(item) is int for item in shape):
        return f"<{type_name} shape={shape}>"
    if type(value) in {dict, list, tuple, set, frozenset}:
        return f"<{type_name} length={len(value)}>"
    return f"<{type_name}>"


def _replace_result_path(result: Any, raw_path: Any, value: Any) -> Any:
    """Replace one closed result path without accepting arbitrary traversal."""

    if not isinstance(raw_path, list) or any(not isinstance(item, str) or not item for item in raw_path):
        raise RuntimeError("client_load: malformed runtime adapter result path")
    if not raw_path:
        return value
    if not isinstance(result, dict):
        raise RuntimeError("client_load: runtime adapter result path does not address a mapping")
    output = dict(result)
    cursor = output
    for part in raw_path[:-1]:
        child = cursor.get(part)
        if not isinstance(child, dict):
            raise RuntimeError("client_load: runtime adapter result path is missing")
        copied = dict(child)
        cursor[part] = copied
        cursor = copied
    if raw_path[-1] not in cursor:
        raise RuntimeError("client_load: runtime adapter result path is missing")
    cursor[raw_path[-1]] = value
    return output


def _adapter_distribution_rows(adapter: Any) -> list[dict[str, str]]:
    return sorted(
        [{"package": distribution.package, "version": distribution.version} for distribution in adapter.distributions],
        key=lambda item: (re.sub(r"[-_.]+", "-", item["package"]).casefold(), item["version"]),
    )


def _library_adapter_refs_from_mapping(value: Any) -> list[LibraryAdapterRef]:
    """Collect explicit immutable refs without treating arbitrary mappings as code."""

    if not isinstance(value, Mapping):
        return []
    result: list[LibraryAdapterRef] = []
    for section_name in ("inputs", "outputs"):
        section = value.get(section_name)
        if not isinstance(section, Mapping):
            continue
        result.extend(item for item in section.values() if isinstance(item, LibraryAdapterRef))
    return result


def _library_adapter_ref_from_record(value: Mapping[str, Any]) -> LibraryAdapterRef:
    """Build the exact immutable Run ref from a code-free version record."""

    try:
        return LibraryAdapterRef(
            owner=value["owner"],
            library=value["library"],
            name=value["name"],
            version=value["version"],
            adapter_id=value["adapter_id"],
            adapter_version_id=value["adapter_version_id"],
            content_hash=value["content_hash"],
            signature_hash=value["signature_hash"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimePortAdapterContractError(
            "library_adapter_record_invalid",
            "Library Adapter version metadata is incomplete or invalid",
            stage="admission",
        ) from exc


def _read_downloaded_artifact(path: Path, *, expected_size: Any, expected_sha256: Any) -> bytes:
    if type(expected_size) is not int or expected_size < 0:
        raise RuntimeError("download: runtime adapter output size is invalid")
    try:
        before = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(before.st_mode):
            raise RuntimeError("download: runtime adapter output is not a regular file")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            chunks: list[bytes] = []
            remaining = expected_size + 1
            while remaining:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        current = path.lstat()
    except OSError:
        raise RuntimeError("download: runtime adapter output could not be opened safely") from None
    data = b"".join(chunks)
    if (
        (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
        or not stat.S_ISREG(current.st_mode)
        or (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino)
        or after.st_size != expected_size
        or current.st_size != expected_size
        or len(data) != expected_size
        or hashlib.sha256(data).hexdigest() != expected_sha256
    ):
        raise RuntimeError("download: runtime adapter output failed size/checksum verification")
    return data


def _write_adapter_decode_snapshot(directory: Path, data: bytes) -> Path:
    """Create one private immutable-by-convention decoder input snapshot."""

    path = directory / "verified-output"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o400)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    return path


def _is_missing_server_connection(exc: Exception) -> bool:
    message = str(exc)
    return isinstance(exc, ClientError) and _NO_SERVER_CONNECTION_MESSAGE in message


def _is_server_unreachable(exc: Exception) -> bool:
    """Return whether the daemon says the central server cannot be reached."""

    if not isinstance(exc, ClientError):
        return False
    if getattr(exc, "code", None) == _SERVER_UNREACHABLE_CODE:
        return True
    payload = getattr(exc, "payload", None)
    if isinstance(payload, Mapping) and payload.get("code") == _SERVER_UNREACHABLE_CODE:
        return True
    message = str(exc)
    return _SERVER_UNREACHABLE_CODE in message or any(text in message for text in _SERVER_UNREACHABLE_MESSAGES)


def _is_server_unavailable(exc: Exception) -> bool:
    return _is_missing_server_connection(exc) or _is_server_unreachable(exc)


def _is_404_error(exc: Exception) -> bool:
    return isinstance(exc, ClientError) and str(exc).startswith("404:")


def _run_id_namespace(run_id: str) -> RunIdNamespace:
    """Return the syntactic namespace for a retained-run id."""

    if _LOCAL_RUN_ID_RE.fullmatch(run_id):
        return "local"
    if _DAEMON_RUN_ID_RE.fullmatch(run_id):
        return "daemon"
    return "unknown"


def _run_show_not_found_message(run_id: str, namespace: RunIdNamespace) -> str:
    if namespace == "local":
        hint = "This looks like a local retained run id; try `run_show({!r}, local=True)`.".format(run_id)
    elif namespace == "daemon":
        hint = "This looks like a daemon run id; check that the daemon is using the expected run store."
    else:
        hint = (
            "This run id does not match the local or daemon id format; check the daemon run id, "
            "or use `runs(local=True)` for local retained runs."
        )
    return "404: run {!r} was not found by the daemon (run id namespace: {}). {}".format(
        run_id,
        namespace,
        hint,
    )


def _local_retained_run_count() -> int:
    try:
        return len(m_manifest.list_local_runs())
    except OSError:
        return 0


def _with_local_cache_warning(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {**dict(payload), "warning": _LOCAL_CACHE_ONLY_WARNING}


def _server_connection_mapping(state: Mapping[str, Any]) -> Mapping[str, Any]:
    connection = state.get("connection") or state.get("remote_connection") or {}
    return connection if isinstance(connection, Mapping) else {}


def _state_is_server_unreachable(state: Mapping[str, Any]) -> bool:
    if state.get("code") == _SERVER_UNREACHABLE_CODE:
        return True
    return bool(state.get("offline")) and bool(_server_connection_mapping(state))


def _state_has_server_connection(state: Mapping[str, Any]) -> bool:
    if _state_is_server_unreachable(state):
        return False
    connected = state.get("connected")
    if connected is not None:
        return bool(connected)
    return _server_connection_mapping(state).get("status") == "connected"


def _normalize_server_connection_state(state: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(state)
    normalized.setdefault("connected", bool(normalized.get("server_url")))
    if _state_is_server_unreachable(normalized):
        normalized["connected"] = False
        normalized["offline"] = True
    elif not normalized.get("connected"):
        normalized.setdefault("offline", bool(_server_connection_mapping(normalized)))
    else:
        normalized.setdefault("offline", False)
    return normalized


def _progress_callback(progress: ProgressOption) -> RunStateCallback | None:
    """Turn a ``progress`` option into a wait-loop state callback.

    ``True`` builds a fresh stderr printer (fresh throttling state per wait),
    ``False`` disables feedback, and a callable is passed through as-is.
    """

    if callable(progress):
        return progress
    if progress:
        return RunProgressPrinter()
    return None


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
            f"<tr><th style='text-align:left'>{escape(key)}</th><td><code>{escape(value)}</code></td></tr>"
            for key, value in rows.items()
        )
        return f"<table><tbody>{body}</tbody></table>"


@dataclass(frozen=True)
class _BareServerRef:
    name: str
    owner_id: str | None
    library: str | None


_CATALOG_HEADERS = ("name", "kind", "version", "library", "inputs")
_CATALOG_HEADERS_WITH_OWNER = ("name", "kind", "version", "owner", "library", "inputs")


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
        library_name = library.get("display_name") or library.get("slug") if isinstance(library, dict) else library
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
                "owner": _owner_label(record),
                "owner_visible": str("owner_handle" in record),
                "library": str(library_name or ""),
                "inputs": str(len(record.get("inputs") or [])),
            }
        )
    return rows


def _catalog_headers(rows: list[dict[str, str]]) -> tuple[str, ...]:
    owners = {row.get("owner") or "" for row in rows}
    owners.discard("")
    owner_visible = any(row.get("owner_visible") == "True" for row in rows)
    return _CATALOG_HEADERS_WITH_OWNER if owner_visible or len(owners) > 1 else _CATALOG_HEADERS


def _rows_to_text(rows: list[dict[str, str]], title: str) -> str:
    if not rows:
        return f"{title}: (empty)"
    headers = _catalog_headers(rows)
    widths = {header: max(len(header), *(len(row[header]) for row in rows)) for header in headers}
    head = "  ".join(header.ljust(widths[header]) for header in headers)
    body = "\n".join("  ".join(row[header].ljust(widths[header]) for header in headers) for row in rows)
    return f"{title} ({len(rows)}):\n{head}\n{body}"


def _rows_to_html(rows: list[dict[str, str]], title: str) -> str:
    headers = _catalog_headers(rows)
    head = "".join(f"<th style='text-align:left'>{escape(header)}</th>" for header in headers)
    body = "".join("<tr>" + "".join(f"<td>{escape(row[header])}</td>" for header in headers) + "</tr>" for row in rows)
    return (
        f"<div><b>{escape(title)}</b> ({len(rows)})"
        f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"
    )


def _record_string(value: Any, *keys: str) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, Mapping):
        for key in keys:
            item = value.get(key)
            if item is not None and str(item):
                return str(item)
    return None


def _server_record_names(record: Mapping[str, Any]) -> set[str]:
    names = {record.get("display_name"), record.get("name"), record.get("object_name")}
    return {str(name) for name in names if isinstance(name, str) and name}


def _server_record_owner(record: Mapping[str, Any]) -> str | None:
    return _record_string(record.get("owner_id")) or _record_string(record.get("owner"), "id", "owner_id", "name")


def _server_record_library(record: Mapping[str, Any]) -> str | None:
    return _record_string(record.get("library"), "slug", "name", "display_name")


def _library_record_slug(record: Mapping[str, Any]) -> str | None:
    return _record_string(record.get("slug")) or _record_string(record.get("library"), "slug", "name")


def _library_record_owner(record: Mapping[str, Any]) -> str | None:
    return _record_string(record.get("owner_id")) or _record_string(record.get("owner"), "id", "owner_id")


def _library_record_owner_ref(record: Mapping[str, Any]) -> str | None:
    handle = _record_string(record.get("owner_handle"))
    if handle is not None:
        return f"@{handle}"
    return _library_record_owner(record)


def _library_candidate_label(record: Mapping[str, Any]) -> str:
    owner_ref = _library_record_owner_ref(record) or "<unknown owner>"
    slug = _library_record_slug(record) or "<unknown library>"
    return "{}/{}".format(owner_ref, slug)


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


def _server_candidate_label(record: Mapping[str, Any]) -> str:
    library = _server_record_library(record) or "<unknown library>"
    owner = _server_record_owner(record)
    version = _server_record_version(record)
    details = []
    if owner is not None:
        details.append("owner {}".format(owner))
    if version is not None:
        details.append("v{}".format(version))
    return "{} ({})".format(library, ", ".join(details)) if details else library


def _resolved_from_server_record(ref: _BareServerRef) -> dict[str, str]:
    record = {"name": ref.name}
    if ref.library is not None:
        record["library"] = ref.library
    if ref.owner_id is not None:
        record["owner_id"] = ref.owner_id
    return record


def _int_version(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _server_freshness_line(
    signature: Mapping[str, Any],
    server_records: list[dict[str, Any]],
    *,
    owner: str | None,
    library: str | None,
) -> str | None:
    local_version = _int_version(signature.get("version"))
    name = signature.get("name")
    if local_version is None or not isinstance(name, str) or not name:
        return None
    candidates = [
        record
        for record in server_records
        if isinstance(record, Mapping)
        and name in _server_record_names(record)
        and (owner is None or _server_record_owner(record) == owner)
        and (library is None or _server_record_library(record) == library)
    ]
    if len(candidates) != 1:
        return None
    candidate = candidates[0]
    server_version = _int_version(_server_record_version(candidate))
    if server_version is None or server_version <= local_version:
        return None
    server_library = _server_record_library(candidate)
    library_label = "library {!r}".format(server_library) if server_library else "library <unknown>"
    return "local v{}; server has v{} ({}) - run/call resolves via source='auto'".format(
        local_version,
        server_version,
        library_label,
    )


def _local_object_missing(exc: Exception) -> bool:
    message = str(exc)
    return isinstance(exc, ClientError) and message.startswith("404:") and "object is not registered" in message


def _preserve_run_resolution(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
) -> dict[str, Any]:
    """Carry immediate D1 annotations through later run polling responses."""

    merged = dict(current)
    for key in ("resolution", "resolved_from", "runtime_port_adapters"):
        if key not in merged and key in previous:
            merged[key] = previous[key]
    return merged


class ObjectList(list[dict[str, Any]]):
    """Object records that print as a compact table; ``.raw`` is a plain list."""

    def __init__(self, payload: list[dict[str, Any]] | None = None, *, title: str | None = None):
        super().__init__(payload or [])
        self._title = title or object_scope_title("server")

    def __repr__(self) -> str:
        return _rows_to_text(_catalog_rows(list(self)), self._title)

    def _repr_html_(self) -> str:
        return _rows_to_html(_catalog_rows(list(self)), self._title)

    @property
    def raw(self) -> list[dict[str, Any]]:
        return list(self)


class ObjectTable(dict[str, Any]):
    """Name-keyed object records with a compact table ``repr``; ``.raw`` is a plain dict."""

    def __init__(self, payload: dict[str, Any] | None = None, *, title: str | None = None):
        super().__init__(payload or {})
        self._title = title or object_scope_title("local")

    def __repr__(self) -> str:
        return _rows_to_text(_catalog_rows(dict(self)), self._title)

    def _repr_html_(self) -> str:
        return _rows_to_html(_catalog_rows(dict(self)), self._title)

    @property
    def raw(self) -> dict[str, Any]:
        return dict(self)


class ObjectCatalog(dict[str, Any]):
    """Local+server catalog that prints per-scope tables; ``.raw`` is a plain dict."""

    def __repr__(self) -> str:
        local_rows = _catalog_rows(self.get("local") or {})
        server_rows = _catalog_rows(self.get("server") or [])
        return "\n\n".join(
            [
                "{}:".format(object_catalog_title(len(local_rows), len(server_rows))),
                _rows_to_text(local_rows, object_scope_title("local")),
                _rows_to_text(server_rows, object_scope_title("server")),
            ]
        )

    def _repr_html_(self) -> str:
        local_rows = _catalog_rows(self.get("local") or {})
        server_rows = _catalog_rows(self.get("server") or [])
        return (
            "<div><b>{}</b></div>".format(escape(object_catalog_title(len(local_rows), len(server_rows))))
            + _rows_to_html(local_rows, object_scope_title("local"))
            + _rows_to_html(server_rows, object_scope_title("server"))
        )

    @property
    def raw(self) -> dict[str, Any]:
        return dict(self)


def _wrap_objects(
    value: dict[str, Any] | list[dict[str, Any]],
    *,
    title: str | None = None,
) -> ObjectTable | ObjectList:
    """Wrap a payload in a view type, preserving its runtime shape."""

    return ObjectTable(value, title=title) if isinstance(value, dict) else ObjectList(value, title=title)


@dataclass(frozen=True)
class RemoteResult:
    """Completed run result plus downloaded artifact locations.

    ``payload`` is the host's JSON result document. Legacy/isolated execution
    stores the return value under ``result``; current-process execution stores
    only a JSON-safe descriptor there and keeps the actual object privately for
    ``value``/``output``. ``downloaded_artifacts`` is populated only when the
    caller asks this client to download artifacts into a local directory.
    """

    run: dict[str, Any]
    payload: dict[str, Any]
    mode: str = "local"
    downloaded_artifacts: dict[str, Path] = field(default_factory=dict)
    _native_value: Any = field(default=_NO_NATIVE_VALUE, repr=False, compare=False)

    def __eq__(self, other: object) -> bool:
        """Legacy field equality; native wrappers compare by identity only."""
        if type(other) is not type(self):
            return NotImplemented
        if self._native_value is not _NO_NATIVE_VALUE or other._native_value is not _NO_NATIVE_VALUE:
            return self is other
        return (self.run, self.payload, self.mode, self.downloaded_artifacts) == (
            other.run,
            other.payload,
            other.mode,
            other.downloaded_artifacts,
        )

    @property
    def value(self) -> Any:
        """Return the user's result, including native current-process values."""

        if self._native_value is not _NO_NATIVE_VALUE:
            return self._native_value
        return self.payload.get("result")

    @property
    def output(self) -> Any:
        """Return the user's unwrapped result value.

        Rule: if the result is a dict and contains the key
        ``'default'`` -> return ``result['default']``; else if it has exactly
        one key -> return that value; else return ``result`` as-is.
        """

        result = self.value
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
        preview = _native_preview(self.value) if self._native_value is not _NO_NATIVE_VALUE else _preview(self.output)
        return f"RemoteResult(mode={self.mode!r}, output={preview})"

    def _repr_html_(self) -> str:
        rows = {
            "mode": self.mode,
            "output": (
                _native_preview(self.value) if self._native_value is not _NO_NATIVE_VALUE else _preview(self.output)
            ),
            "artifacts": str(len(self.artifacts)),
        }
        body = "".join(
            f"<tr><th style='text-align:left'>{escape(key)}</th><td><code>{escape(value)}</code></td></tr>"
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
        runtime_adapter_plan: PreparedClientRuntimeAdapters | None = None,
    ):
        self._client = client
        self.state = RunRecordView(state)
        self.server_side = server_side
        self._runtime_adapter_plan = runtime_adapter_plan

    @property
    def id(self) -> str:
        """Return the daemon run id."""

        return cast(str, self.state["id"])

    @property
    def status(self) -> str:
        """Return the last known daemon status."""

        return cast(str, self.state["status"])

    @property
    def mode(self) -> str:
        """Return ``local`` for daemon worker runs and ``server`` for remote runs."""

        return "server" if self.server_side else "local"

    def refresh(self) -> dict[str, Any]:
        """Refresh and return the run state from the daemon."""

        if self.server_side:
            current = self._client._daemon.get_remote_run(self.id)
        else:
            current = self._client._daemon.get_run(self.id)
        self.state = RunRecordView(_preserve_run_resolution(self.state, current))
        return self.state

    def wait(
        self,
        *,
        poll_interval: float = 0.25,
        timeout_seconds: float | None = None,
        progress: ProgressOption = True,
    ) -> RunRecordView:
        """Wait until the run succeeds or fails, then return final state.

        ``progress`` controls feedback during slow phases (first-run
        environment builds, queued server-side runs): ``True`` prints short
        status lines to stderr, ``False`` waits silently, and a callable
        receives every polled state instead.
        """

        on_state = _progress_callback(progress)
        if self.server_side:
            current = self._client._daemon.wait_remote_run(
                self.id,
                poll_interval=poll_interval,
                timeout_seconds=timeout_seconds,
                on_state=on_state,
            )
        else:
            current = self._client._daemon.wait_run(
                self.id,
                poll_interval=poll_interval,
                timeout_seconds=timeout_seconds,
                on_state=on_state,
            )
        self.state = RunRecordView(_preserve_run_resolution(self.state, current))
        return self.state

    def result(self) -> dict[str, Any]:
        """Return the daemon result payload for this run."""

        if self.server_side:
            # ``wait_remote_run`` returns the complete terminal snapshot. Do
            # not perform a redundant status GET after success: the central
            # lease may be briefly reconnecting even though this exact Run
            # and its result are already known. Direct pre-terminal ``result``
            # calls still refresh as before.
            if self.status not in {"succeeded", "failed", "cancelled", "stale"} or "result" not in self.state:
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
        if target_path.is_symlink():
            raise ValueError("artifact target directory must not be a symbolic link")
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

    def _acknowledge_transient_delivery(self) -> None:
        """Best-effort additive handshake after terminal data is consumed."""

        if self.server_side:
            return
        try:
            self._client._daemon.acknowledge_run_delivery(self.id)
        except ClientError as exc:
            if exc.status_code in {404, 405}:
                # A 0.4.5 client can still collect from a 0.4.4 daemon.  That
                # daemon does not enforce transient cleanup, so no data is at risk.
                return
            warnings.warn(
                f"run {self.id!r} was delivered, but its retention acknowledgment failed: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )
        except Exception as exc:
            warnings.warn(
                f"run {self.id!r} was delivered, but its retention acknowledgment failed: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )

    def collect(
        self,
        *,
        artifacts_dir: str | Path | None = None,
        poll_interval: float = 0.25,
        timeout_seconds: float | None = None,
        progress: ProgressOption = True,
    ) -> RemoteResult:
        """Wait for completion, return result, and optionally download artifacts.

        See :meth:`wait` for the meaning of ``progress``.
        """

        final_state = self.wait(
            poll_interval=poll_interval,
            timeout_seconds=timeout_seconds,
            progress=progress,
        )
        status = str(final_state["status"])
        transient = m_manifest.retention_disposition(final_state.get("keep", True), status) == "remove"
        if status != "succeeded":
            error = final_state.get("error") or "run returned no error message"
            if transient:
                self._acknowledge_transient_delivery()
            raise RuntimeError(f"{self.mode} run {self.id!r} ended as {final_state.get('status')!r}: {error}")

        payload = self.result()
        raw_runtime_outputs = payload.get("runtime_port_adapter_outputs")
        if isinstance(raw_runtime_outputs, list):
            try:
                prevalidated = [normalize_runtime_output_record(record) for record in raw_runtime_outputs]
            except RuntimePortAdapterContractError:
                raise RuntimeError("download: malformed runtime adapter output metadata") from None
            if len({record["port"] for record in prevalidated}) != len(prevalidated) or len(
                {record["name"] for record in prevalidated}
            ) != len(prevalidated):
                raise RuntimeError("download: runtime adapter output metadata is duplicated")
        downloaded = self.download_artifacts(artifacts_dir) if artifacts_dir is not None else {}
        if isinstance(payload.get("runtime_port_adapter_outputs"), list):
            payload, downloaded = self._decode_runtime_adapter_outputs(
                payload,
                downloaded=downloaded,
                artifacts_dir=artifacts_dir,
            )
        result = RemoteResult(
            run=final_state,
            payload=payload,
            mode=self.mode,
            downloaded_artifacts=downloaded,
        )
        if transient:
            self._acknowledge_transient_delivery()
        return result

    def _decode_runtime_adapter_outputs(
        self,
        payload: dict[str, Any],
        *,
        downloaded: dict[str, Path],
        artifacts_dir: str | Path | None,
    ) -> tuple[dict[str, Any], dict[str, Path]]:
        """Verify, decode, and splice trusted adapter-backed outputs."""

        records = payload.get("runtime_port_adapter_outputs")
        if not isinstance(records, list) or not records:
            return payload, downloaded
        try:
            normalized_records = [normalize_runtime_output_record(record) for record in records]
        except RuntimePortAdapterContractError:
            raise RuntimeError("download: malformed runtime adapter output metadata") from None
        if len({record["port"] for record in normalized_records}) != len(normalized_records) or len(
            {record["name"] for record in normalized_records}
        ) != len(normalized_records):
            raise RuntimeError("download: runtime adapter output metadata is duplicated")
        temporary = (
            tempfile.TemporaryDirectory(
                prefix="spl-runtime-output-",
                # Resolve only the framework-selected temp root.  User-supplied
                # artifact destinations remain untrusted and unresolved.
                dir=Path(tempfile.gettempdir()).resolve(strict=True),
            )
            if artifacts_dir is None
            else None
        )
        target_dir = Path(temporary.name) if temporary is not None else Path(artifacts_dir)  # type: ignore[arg-type]
        result_payload = dict(payload)
        try:
            for record in normalized_records:
                port = record["port"]
                name = record["name"]
                decoder = (
                    None if self._runtime_adapter_plan is None else self._runtime_adapter_plan.output_decoders.get(port)
                )
                adapter_id = record["adapter_id"]
                manifest_binding = self._verify_recovered_output_identity(port, adapter_id, record)
                recovery_reason: str | None = None
                expected_distributions = manifest_binding.get("distributions")
                if decoder is not None and _adapter_distribution_rows(decoder.adapter) != expected_distributions:
                    recovery_reason = "the trusted decoder distribution set does not match the admitted Run"
                    decoder = None
                if decoder is None and adapter_id in BUILTIN_ADAPTER_IDS:
                    from spl._runtime_port_adapters_client import OutputDecoder

                    candidate = get_builtin_adapter(adapter_id)
                    if _adapter_distribution_rows(candidate) == expected_distributions:
                        decoder = OutputDecoder(port, adapter_id, candidate)
                    else:
                        recovery_reason = "install the exact admitted adapter distributions before decoding"
                path = downloaded.get(name)
                if path is None:
                    if self.server_side:
                        path = self._client._daemon.download_remote_artifact(self.id, name, target_dir)
                    else:
                        path = self._client._daemon.download_artifact(self.id, name, target_dir)
                    if artifacts_dir is not None:
                        downloaded[name] = path
                data = _read_downloaded_artifact(
                    path,
                    expected_size=record.get("size"),
                    expected_sha256=record.get("sha256"),
                )
                if decoder is None or decoder.adapter_id == OPAQUE_FILE:
                    if decoder is None:
                        recovery = recovery_reason or (
                            "supply an explicitly trusted matching custom adapter and collect again with "
                            "artifacts_dir=..."
                            if adapter_id.startswith("custom:")
                            else "install the exact admitted adapter distributions before decoding"
                        )
                    elif artifacts_dir is None:
                        recovery = "call collect(artifacts_dir=...) to retain this opaque artifact"
                    else:
                        recovery = None
                    value: Any = ArtifactHandle(
                        name=name,
                        size=len(data),
                        sha256=record["sha256"],
                        adapter_id=adapter_id,
                        format_tag=record["format_tag"],
                        path=path if artifacts_dir is not None else None,
                        media_type=record.get("media_type"),
                        recovery=recovery,
                    )
                else:
                    try:
                        with tempfile.TemporaryDirectory(prefix="spl-runtime-decode-") as raw_decode_dir:
                            snapshot = _write_adapter_decode_snapshot(Path(raw_decode_dir), data)
                            if decoder.adapter_id.startswith("custom:"):
                                with redirect_stdout(_DISCARD_ADAPTER_OUTPUT), redirect_stderr(_DISCARD_ADAPTER_OUTPUT):
                                    value = decoder.adapter.load(str(snapshot))
                            else:
                                value = decoder.adapter.load(str(snapshot))
                    except BaseException:
                        raise RuntimeError(
                            f"client_load: adapter {decoder.adapter_id!r} failed to decode output {port!r}"
                        ) from None
                result_payload["result"] = _replace_result_path(
                    result_payload.get("result"),
                    record["result_path"],
                    value,
                )
        finally:
            if temporary is not None:
                temporary.cleanup()
        return result_payload, downloaded

    def _verify_recovered_output_identity(
        self,
        port: str,
        adapter_id: str,
        record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Bind recovered result metadata to the admitted manifest descriptor."""

        manifest = self.state.get("manifest")
        if adapter_id.startswith("library:") and isinstance(manifest, Mapping):
            metadata = manifest.get("runtime_library_adapter_metadata")
            metadata_bindings = metadata.get("bindings") if isinstance(metadata, Mapping) else None
            library_binding = next(
                (
                    item
                    for item in metadata_bindings or []
                    if isinstance(item, Mapping) and item.get("direction") == "output" and item.get("port") == port
                ),
                None,
            )
            transport = manifest.get("runtime_port_adapters")
            transport_bindings = transport.get("bindings") if isinstance(transport, Mapping) else None
            transport_binding = next(
                (
                    item
                    for item in transport_bindings or []
                    if isinstance(item, Mapping) and item.get("direction") == "output" and item.get("port") == port
                ),
                None,
            )
            if (
                library_binding is None
                or transport_binding is None
                or adapter_id != f"library:{library_binding.get('content_hash')}"
                or library_binding.get("format_tag") != record.get("format_tag")
                or library_binding.get("media_type") != record.get("media_type")
                or transport_binding.get("artifact_name") != record.get("name")
                or transport_binding.get("artifact_size") != record.get("size")
                or transport_binding.get("artifact_sha256") != record.get("sha256")
                or transport_binding.get("result_path") != record.get("result_path")
            ):
                raise RuntimeError("client_load: Library Adapter output identity does not match its Run manifest")
            return library_binding
        section = manifest.get("runtime_port_adapters") if isinstance(manifest, Mapping) else None
        bindings = section.get("bindings") if isinstance(section, Mapping) else None
        if not isinstance(bindings, list):
            return self._verify_remote_admission_output_identity(port, adapter_id, record)
        match = next(
            (
                binding
                for binding in bindings
                if isinstance(binding, Mapping) and binding.get("direction") == "output" and binding.get("port") == port
            ),
            None,
        )
        if (
            match is None
            or match.get("adapter_id") != adapter_id
            or match.get("format_tag") != record.get("format_tag")
            or match.get("artifact_name") != record.get("name")
            or match.get("artifact_size") != record.get("size")
            or match.get("artifact_sha256") != record.get("sha256")
            or match.get("media_type") != record.get("media_type")
            or match.get("result_path") != record.get("result_path")
        ):
            raise RuntimeError("client_load: runtime adapter output identity does not match its Run manifest")
        return match

    def _verify_remote_admission_output_identity(
        self,
        port: str,
        adapter_id: str,
        record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Bind a recovered remote output to immutable admission evidence."""

        evidence = self.state.get("runtime_port_adapters")
        evidence_keys = {
            "schema_version",
            "bindings",
            "inputs",
            "custom_bundle",
            "adapter_policy",
            "admission_digest_sha256",
        }
        if not isinstance(evidence, Mapping) or set(evidence) != evidence_keys | {"terminal"}:
            raise RuntimeError("client_load: runtime adapter admission evidence is missing")
        digest = evidence.get("admission_digest_sha256")
        if (
            evidence.get("schema_version") != 1
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise RuntimeError("client_load: runtime adapter admission evidence is malformed")
        bindings = evidence.get("bindings")
        inputs = evidence.get("inputs")
        if not isinstance(bindings, list) or not isinstance(inputs, list):
            raise RuntimeError("client_load: runtime adapter admission evidence is malformed")
        if self._runtime_adapter_plan is not None:
            expected = server_admission_document(self._runtime_adapter_plan.document)
            observed = {
                "schema_version": evidence["schema_version"],
                "bindings": bindings,
                "inputs": inputs,
                "custom_bundle": evidence["custom_bundle"],
            }
            if observed != expected or evidence.get("adapter_policy") != self._runtime_adapter_plan.adapter_policy:
                raise RuntimeError("client_load: remote admission differs from the submitted adapter plan")

        terminal = evidence.get("terminal")
        if not isinstance(terminal, Mapping) or set(terminal) != {
            "schema_version",
            "custom_execution",
            "outputs",
            "failure",
        }:
            raise RuntimeError("client_load: runtime adapter terminal evidence is missing")
        custom_execution = terminal.get("custom_execution")
        terminal_outputs = terminal.get("outputs")
        if (
            terminal.get("schema_version") != 1
            or terminal.get("failure") is not None
            or not isinstance(custom_execution, Mapping)
            or set(custom_execution) != {"requested", "allowed", "used"}
            or any(type(custom_execution.get(key)) is not bool for key in ("requested", "allowed", "used"))
            or not isinstance(terminal_outputs, list)
        ):
            raise RuntimeError("client_load: runtime adapter terminal evidence is malformed")
        requested = evidence.get("adapter_policy") == {"custom_remote": "allow"}
        has_custom_bundle = evidence.get("custom_bundle") is not None
        if (
            custom_execution["requested"] is not requested
            or (custom_execution["used"] and not custom_execution["allowed"])
            or (custom_execution["used"] and not has_custom_bundle)
            or (has_custom_bundle and not all(custom_execution[key] for key in ("requested", "allowed", "used")))
        ):
            raise RuntimeError("client_load: runtime adapter custom execution evidence is contradictory")

        binding_keys = {
            "direction",
            "port",
            "external_name",
            "semantic_type",
            "adapter_kind",
            "adapter_id",
            "key",
            "format_tag",
            "accepted_tags",
            "distributions",
            "save_symbol",
            "load_symbol",
            "bundle_sha256",
            "resolution_source",
            "transport",
            "input_name",
            "result_path",
            "presentation",
        }
        matches = [
            binding
            for binding in bindings
            if isinstance(binding, Mapping)
            and set(binding) == binding_keys
            and binding.get("direction") == "output"
            and binding.get("port") == port
        ]
        if len(matches) != 1:
            raise RuntimeError("client_load: remote output binding is missing or duplicated")
        match = matches[0]
        expected_output_ports = {
            binding.get("port")
            for binding in bindings
            if isinstance(binding, Mapping)
            and set(binding) == binding_keys
            and binding.get("direction") == "output"
            and binding.get("transport") == "artifact"
        }
        terminal_output_keys = {
            "port",
            "adapter_id",
            "format_tag",
            "semantic_type",
            "artifact_name",
            "size",
            "sha256",
            "media_type",
            "result_path",
        }
        if (
            any(not isinstance(item, Mapping) or set(item) != terminal_output_keys for item in terminal_outputs)
            or len({item.get("port") for item in terminal_outputs}) != len(terminal_outputs)
            or {item.get("port") for item in terminal_outputs} != expected_output_ports
        ):
            raise RuntimeError("client_load: runtime adapter terminal outputs are malformed")
        terminal_matches = [item for item in terminal_outputs if item.get("port") == port]
        if len(terminal_matches) != 1:
            raise RuntimeError("client_load: runtime adapter terminal output is missing or duplicated")
        terminal_output = terminal_matches[0]
        distributions = match.get("distributions")
        presentation = match.get("presentation")
        valid_distributions = isinstance(distributions, list) and all(
            isinstance(item, Mapping)
            and set(item) == {"package", "version"}
            and isinstance(item.get("package"), str)
            and isinstance(item.get("version"), str)
            for item in distributions
        )
        if (
            not valid_distributions
            or not isinstance(presentation, Mapping)
            or set(presentation) != {"media_type", "preferred_extension"}
            or match.get("adapter_id") != adapter_id
            or match.get("format_tag") != record.get("format_tag")
            or match.get("result_path") != record.get("result_path")
            or presentation.get("media_type") != record.get("media_type")
            or terminal_output.get("adapter_id") != match.get("adapter_id")
            or terminal_output.get("format_tag") != match.get("format_tag")
            or terminal_output.get("semantic_type") != match.get("semantic_type")
            or terminal_output.get("artifact_name") != record.get("name")
            or terminal_output.get("size") != record.get("size")
            or terminal_output.get("sha256") != record.get("sha256")
            or terminal_output.get("media_type") != record.get("media_type")
            or terminal_output.get("result_path") != record.get("result_path")
        ):
            raise RuntimeError("client_load: runtime adapter output identity does not match remote admission")
        return {
            "adapter_id": match["adapter_id"],
            "format_tag": match["format_tag"],
            "distributions": distributions,
            "result_path": match["result_path"],
            "media_type": presentation["media_type"],
            "artifact_name": terminal_output["artifact_name"],
            "artifact_size": terminal_output["size"],
            "artifact_sha256": terminal_output["sha256"],
        }


class _LibraryAdmin:
    """Grouped library-management operations, reachable via ``SPLClient.library``."""

    def __init__(self, client: "SPLClient") -> None:
        self._c = client

    def list(
        self,
        *,
        owner: str | None = None,
        include_accessible: bool = True,
    ) -> builtins.list[dict[str, Any]]:
        """Return visible libraries, optionally for one canonical id or ``@handle``."""

        return self._c.libraries(owner=owner, include_accessible=include_accessible)

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
        return wrap_action(
            self._c._daemon.create_server_library(payload),
            "library created",
        )

    def get(self, ref: str, *, owner: str | None = None) -> dict[str, Any]:
        """Return one library for an optional canonical owner id or ``@handle``."""

        self._c._require_server_connection("reading a library")
        owner_ref = normalize_owner_ref(owner)
        if owner_ref is None:
            payload = self._c._daemon.get_server_library(ref)
        else:
            payload = self._c._daemon.get_server_library(ref, owner=owner_ref)
        return wrap_action(payload, "library")

    def grants(
        self,
        ref: str,
        *,
        owner: str | None = None,
    ) -> builtins.list[dict[str, Any]]:
        """Return owned-library grants; ``owner`` accepts an id or ``@handle``.

        An explicit foreign owner is addressable but remains owner-authorized
        by the server, so its 403 response is surfaced unchanged.
        """

        self._c._require_server_connection("reading library grants")
        owner_ref = normalize_owner_ref(owner)
        if owner_ref is None:
            return self._c._daemon.server_library_grants(ref)
        return self._c._daemon.server_library_grants(ref, owner=owner_ref)

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
        return wrap_action(
            self._c._daemon.update_server_library(ref, payload),
            "library updated",
        )

    def delete(self, ref: str) -> dict[str, Any]:
        """Raise a clear error because server-side library delete is unsupported."""

        self._c._require_server_connection("deleting a library")
        raise NotImplementedError(_LIBRARY_DELETE_UNSUPPORTED_MESSAGE)

    def grant(
        self,
        ref: str,
        grantee: str,
        *,
        grantee_type: str = "user",
        scopes: builtins.list[str] | None = None,
    ) -> dict[str, Any]:
        """Grant access; a user ``grantee`` may be a canonical id or ``@handle``."""

        self._c._require_server_connection("granting library access")
        grantee_ref = normalize_owner_ref(grantee) if grantee_type == "user" else grantee
        payload: dict[str, Any] = {
            "grantee_id": grantee_ref,
            "grantee_type": grantee_type,
        }
        if scopes is not None:
            payload["scopes"] = scopes
        return wrap_action(
            self._c._daemon.grant_server_library(ref, payload),
            "library grant",
        )

    def revoke(self, ref: str, grantee: str) -> dict[str, Any]:
        """Revoke a grantee id or ``@handle`` from an owned library."""

        self._c._require_server_connection("revoking library access")
        return wrap_action(
            self._c._daemon.revoke_server_library_grant(ref, normalize_owner_ref(grantee)),
            "library grant revoked",
        )

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
        """Add a live reference from a source owner id or ``@handle``."""

        self._c._require_server_connection("adding a library reference")
        payload: dict[str, Any] = {
            "name": name,
            "from_library": from_library,
        }
        owner_ref = normalize_owner_ref(owner)
        if owner_ref is not None:
            payload["from_owner"] = owner_ref
        if version is not None:
            payload["version"] = version
        if alias is not None:
            payload["alias"] = alias
        return wrap_action(
            self._c._daemon.add_server_library_reference(into_library, payload),
            "library reference",
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
        """Copy from an optional canonical ``from_owner`` id or ``@handle``."""

        self._c._require_server_connection("copying an object into a library")
        payload: dict[str, Any] = {
            "name": name,
            "from_library": from_library,
        }
        owner_ref = normalize_owner_ref(from_owner)
        if owner_ref is not None:
            payload["from_owner"] = owner_ref
        if version is not None:
            payload["version"] = version
        if new_name is not None:
            payload["new_name"] = new_name
        return wrap_action(
            self._c._daemon.copy_server_library_object(into_library, payload),
            "library copy",
        )

    def remove_entry(self, library: str, name: str) -> dict[str, Any]:
        """Remove an owned object or reference entry from a central-server library."""

        self._c._require_server_connection("removing a library entry")
        return wrap_action(
            self._c._daemon.remove_server_library_entry(library, name),
            "library entry removed",
        )


class _ObjectAdmin:
    """Grouped owner administration without colliding with ``objects()``."""

    def __init__(self, client: "SPLClient") -> None:
        self._c = client

    def update(
        self,
        name_or_id: str,
        *,
        library: str | None = None,
        description: Any = UNSET,
        public: Any = UNSET,
        expected_revision: int | None = None,
        wait: bool = True,
        execution_profile: str | None = None,
    ) -> dict[str, Any]:
        """Update one revisioned profile and optionally activate/withdraw it."""

        self._c._daemon.require_public_profile_capability(PUBLIC_OBJECT_PROFILE_CAPABILITY)
        payload: dict[str, Any] = {"wait": wait}
        if execution_profile is not None:
            payload["execution_profile"] = execution_profile
        if library is not None:
            payload["library"] = library
        if description is not UNSET:
            payload["description"] = description
        if public is not UNSET:
            payload["public"] = public
        if expected_revision is not None:
            payload["expected_revision"] = expected_revision
        return self._c._daemon.update_object_profile(name_or_id, payload)

    def status(
        self,
        name_or_id: str,
        *,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Return the authenticated owner's current profile/publication state."""

        return self._c._daemon.object_profile_status(name_or_id, library=library)

    def preflight(
        self,
        name_or_id: str,
        *,
        library: str | None = None,
        execution_profile: str | None = None,
    ) -> dict[str, Any]:
        """Validate and materialize a candidate without activating it."""

        options: dict[str, Any] = {"library": library}
        if execution_profile is not None:
            options["execution_profile"] = execution_profile
        return self._c._daemon.preflight_public_object(name_or_id, **options)


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
        self._embedded_backend: Any | None = None
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

    @classmethod
    def embedded(
        cls,
        registry_url: str = "https://splime.io",
        cache_dir: str | Path | None = None,
        *,
        run_receipts: bool | None = None,
        trusted_keys: Mapping[str, Mapping[str, str | bytes]] | None = None,
    ) -> "SPLClient":
        """Create a daemon-free client for verified public releases only.

        Signed current-process-v1 releases run in this interpreter using its
        packages; legacy releases retain isolated worker execution. Selection
        is authenticated at publication and cannot be overridden by a call.

        ``run_receipts=False`` disables privacy-minimal best-effort public Run
        counters. ``SPL_PUBLIC_RUN_RECEIPTS=0`` provides the same opt-out.
        """

        from spl.embedded import EmbeddedBackend, UnsupportedEmbeddedDaemon

        client = cls.__new__(cls)
        client._embedded_backend = EmbeddedBackend(
            registry_url,
            cache_dir,
            run_receipts=run_receipts,
            trusted_keys=trusted_keys,
        )
        client._daemon = cast(Client, UnsupportedEmbeddedDaemon())
        client.server_connection = None
        client._user_token = None
        return client

    @property
    def mode(self) -> str:
        """Return ``embedded`` or the historical integrated-daemon mode."""

        return "embedded" if self._embedded_backend is not None else "daemon"

    def health(self) -> dict[str, Any]:
        """Check that the local daemon is reachable."""

        return HealthView(self._daemon.health())

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
        return ConnectionStatusView(self.server_connection)

    def disconnect_server(self) -> dict[str, Any]:
        """Gracefully disconnect the local daemon from the central server."""

        response = self._daemon.disconnect_server()
        self._user_token = None
        self.server_connection = None
        return response

    @property
    def server(self) -> SPLServerClient:
        """Advanced direct central-server client for callers with a user token."""

        if self._embedded_backend is not None:
            raise ClientError(
                "feature_not_supported: server administration is unavailable in SPLClient embedded mode",
                payload={
                    "code": "feature_not_supported",
                    "error": "server administration is unavailable in SPLClient embedded mode",
                },
            )
        if self._user_token is None:
            raise RuntimeError(
                "SPLClient.server requires a user token supplied to "
                "SPLClient(..., user_token=...) or connect_server(...)."
            )
        conn = self.current_server_connection()
        connection = conn.get("connection")
        nested_url = connection.get("server_url") if isinstance(connection, dict) else None
        server_url = conn.get("server_url") or nested_url or DEFAULT_SERVER_URL
        return SPLServerClient(token=self._user_token, base_url=server_url)

    @property
    def library(self) -> _LibraryAdmin:
        """Grouped library administration (create/grant/reference/copy/...)."""

        return _LibraryAdmin(self)

    @property
    def object(self) -> _ObjectAdmin:
        """Grouped Object profile/publication administration."""

        return _ObjectAdmin(self)

    # Historical 0.1.x spellings are deliberately retained as thin facades.
    # Their signatures come from the released wheels rather than from the
    # evolving grouped API below.
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
        return self.library.create(
            slug,
            display_name=display_name,
            description=description,
            visibility=visibility,
            default_machine=default_machine,
            execution=execution,
        )

    def get_library(self, ref: str) -> dict[str, Any]:
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
        return self.library.update(
            ref,
            display_name=display_name,
            description=description,
            visibility=visibility,
            default_machine=default_machine,
            execution=execution,
        )

    def delete_library(self, ref: str) -> dict[str, Any]:
        return self.library.delete(ref)

    def grant_library(
        self,
        ref: str,
        grantee: str,
        *,
        grantee_type: str = "user",
        scopes: list[str] | None = None,
    ) -> dict[str, Any]:
        return self.library.grant(
            ref,
            grantee,
            grantee_type=grantee_type,
            scopes=scopes,
        )

    def revoke_library_grant(self, ref: str, grantee: str) -> dict[str, Any]:
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
        return self.library.copy_object(
            name,
            into_library=into_library,
            from_owner=from_owner,
            from_library=from_library,
            version=version,
            new_name=new_name,
        )

    def remove_entry(self, library: str, name: str) -> dict[str, Any]:
        return self.library.remove_entry(library, name)

    def current_server_connection(self, *, probe: bool = True) -> dict[str, Any]:
        """Return server state, probing an open channel unless ``probe=False``."""

        try:
            state = self._daemon.server_connection() if probe else self._daemon.server_connection(probe=False)
        except Exception as exc:
            if _is_missing_server_connection(exc):
                return ConnectionStatusView({"connected": False, "offline": False, "connection": None})
            if _is_server_unreachable(exc):
                return ConnectionStatusView({"connected": False, "offline": True, "connection": None})
            raise
        return ConnectionStatusView(_normalize_server_connection_state(state))

    def sync_status(self) -> dict[str, Any]:
        """Return outbound queue counts and heartbeat thread diagnostics."""

        return self._daemon.sync_status()

    def prune_sync_events(
        self,
        *,
        status: str,
        older_than_days: int = 0,
        include_protected: bool = False,
        limit: int = 1_000,
    ) -> dict[str, Any]:
        """Prune old sync rows, protecting object/version events by default."""

        return self._daemon.prune_sync_events(
            status=status,
            older_than_days=older_than_days,
            include_protected=include_protected,
            limit=limit,
        )

    def machines(self) -> dict[str, Any]:
        """Return the user's machines, or an empty listing without an identity.

        A configured identity with a dead server channel raises the daemon's
        offline error. Only an offline-by-design daemon with no server
        identity keeps the historical empty-list behavior.
        """

        if not self._server_identity_present():
            return MachineListView({"current_machine_id": None, "machines": []})
        return MachineListView(self._daemon.server_machines())

    def whoami(self) -> dict[str, Any]:
        """Return the daemon's live or cached canonical server identity.

        The exact mapping includes id/owner id, handle, display name, server,
        machine, connection status, and ``live``. With no identity-bearing
        connection, the daemon's actionable ``connect_server(...)`` error is
        surfaced unchanged.
        """

        return self._daemon.server_whoami()

    def users(self, handle: str | None = None) -> list[dict[str, Any]]:
        """Return the email-free server user directory.

        ``handle`` accepts either ``alice`` or ``@alice`` and is forwarded to
        the server without a client-side directory lookup. Rows contain only
        id, handle, display name, and status.
        """

        handle_ref = normalize_owner_ref(handle)
        return self._daemon.server_users(handle=handle_ref)

    def libraries(
        self,
        *,
        owner: str | None = None,
        include_accessible: bool = True,
    ) -> list[dict[str, Any]]:
        """Return visible libraries, optionally for an owner id or ``@handle``.

        The owner is forwarded unchanged for server-side resolution. An
        ownerless offline catalog keeps the existing empty-list behavior;
        explicit owner requests surface connection/server errors instead of
        silently selecting another namespace.
        """

        owner_ref = normalize_owner_ref(owner)
        if owner_ref is None and not self._server_identity_present():
            return LibraryListView([])
        if owner_ref is None:
            libraries = self._daemon.server_libraries(include_accessible=include_accessible)
        else:
            libraries = self._daemon.server_libraries(
                owner=owner_ref,
                include_accessible=include_accessible,
            )
        return LibraryListView(libraries)

    def library_adapters(
        self,
        *,
        owner: str | None = None,
        library: str | None = None,
        query: str | None = None,
        direction: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Return one bounded, source-free Library Adapter catalog page."""

        return self._daemon.list_library_adapters(
            owner_id=normalize_owner_ref(owner),
            library=library,
            query=query,
            direction=direction,
            limit=limit,
            cursor=cursor,
        )

    def library_adapter(
        self,
        adapter_id: str,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Return one code-free current Library Adapter version."""

        return self._daemon.get_library_adapter(
            adapter_id,
            owner_id=normalize_owner_ref(owner),
            library=library,
        )

    def library_adapter_versions(
        self,
        adapter_id: str,
        *,
        limit: int = 100,
        cursor: str | None = None,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Return bounded immutable version history for one Adapter."""

        return self._daemon.library_adapter_versions(
            adapter_id,
            limit=limit,
            cursor=cursor,
            owner_id=normalize_owner_ref(owner),
            library=library,
        )

    def preflight_library_adapter(
        self,
        publication: Mapping[str, Any],
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Statically validate reviewed Adapter source without executing it."""

        return self._daemon.preflight_library_adapter(
            publication,
            owner_id=normalize_owner_ref(owner),
            library=library,
        )

    def publish_library_adapter(
        self,
        publication: Mapping[str, Any],
        *,
        owner: str | None = None,
        library: str | None = None,
        local_only: bool = False,
    ) -> dict[str, Any]:
        """Publish or exact-deduplicate one immutable Library Adapter version."""

        return self._daemon.publish_library_adapter(
            publication,
            owner_id=normalize_owner_ref(owner),
            library=library,
            local_only=local_only,
        )

    def update_library_adapter(
        self,
        adapter_id: str,
        *,
        description: Any = UNSET,
        public: Any = UNSET,
        expected_revision: int | None = None,
        wait: bool = True,
    ) -> dict[str, Any]:
        """Update one Adapter profile without making the Adapter callable."""

        self._daemon.require_public_profile_capability(PUBLIC_ADAPTER_PROFILE_CAPABILITY)
        payload: dict[str, Any] = {"wait": wait}
        if description is not UNSET:
            payload["description"] = description
        if public is not UNSET:
            payload["public"] = public
        if expected_revision is not None:
            payload["expected_revision"] = expected_revision
        return self._daemon.update_library_adapter_profile(adapter_id, payload)

    # The grouped ``client.library.*`` surface remains canonical. The flat
    # 0.1.x spellings above are retained as source-compatibility facades.

    def register_env(self, name: str = "default", python: str | None = None) -> dict[str, Any]:
        """Register a Python executable as a daemon environment.

        By default the daemon registers its own interpreter.  This keeps the
        simplest local workflow working both when the daemon runs natively and
        when it runs in a container:

            client.register_env()
            client.publish(my_function, env="default")
        """

        return wrap_action(self._daemon.register_env(name, python), "env registered")

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
        description: Any = UNSET,
    ) -> PublishedObject:
        """Serialize a live function/pipeline and store it in the daemon.

        ``name`` is the daemon registry name.  ``entrypoint`` is the object name
        inside the generated SPL/YAML file.  They can differ, which lets a user
        publish the same function under several daemon aliases.

        ``dependency_frame_offset`` is only needed when ``publish`` itself is
        wrapped by user helper functions.  Leave it at ``0`` for direct notebook
        use.

        Live functions are validated while producing YAML.  Signatures and
        closures that the 0.4.x format cannot represent raise before the daemon
        registration request is made.

        ``library`` targets a central-server library during sync.  Missing
        non-default libraries are rejected unless ``create=True`` is passed.
        """

        yaml_text, resolved_entrypoint = export_object_to_yaml(
            obj,
            entrypoint,
            frame_offset=4 + dependency_frame_offset,
        )
        registry_name = name or resolved_entrypoint
        profile_description = self._publish_description_intent(description)
        profile_options: dict[str, Any] = (
            {} if profile_description is None else {"profile_description": profile_description}
        )
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
            **profile_options,
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
        description: Any = UNSET,
    ) -> PublishedObject:
        """Store an already generated SPL/YAML document in the daemon.

        ``yaml`` can be YAML text or a path to a YAML file.  A string is treated
        as a path when it points to an existing file; otherwise it is sent as
        YAML text.  This method covers the explicit requirement "send generated
        YAML" and is useful when the object was exported earlier or produced by
        another process.  ``create=True`` asks the server to create the target
        library if it does not already exist.  Because this path accepts an
        already-serialized document rather than a live callable, it does not
        run live-function signature or closure validation; existing
        ``!DFunction`` YAML remains compatible and is registered unchanged.
        """

        yaml_text = read_yaml_input(yaml)
        profile_description = self._publish_description_intent(description)
        profile_options: dict[str, Any] = (
            {} if profile_description is None else {"profile_description": profile_description}
        )
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
            **profile_options,
        )
        return PublishedObject(
            name=record["name"],
            entrypoint=record["entrypoint"],
            env=record["env"],
            yaml_path=record["yaml_path"],
            workdir=record.get("workdir"),
            raw=record,
        )

    def _publish_description_intent(self, description: Any) -> dict[str, Any] | None:
        """Negotiate the 0.4.8 preserve/set/clear registration operation."""

        supports = getattr(
            self._daemon,
            "supports_public_profile_capability",
            None,
        )
        capable = bool(callable(supports) and supports(PUBLIC_OBJECT_PROFILE_CAPABILITY))
        if description is UNSET:
            return {"mode": "preserve"} if capable else None
        require = getattr(self._daemon, "require_public_profile_capability", None)
        if not callable(require):
            raise ClientError(
                "local SPL daemon does not support public Object profiles; "
                "upgrade and restart the daemon before setting description",
                payload={"code": "feature_not_supported"},
            )
        require(PUBLIC_OBJECT_PROFILE_CAPABILITY)
        if not isinstance(description, str):
            raise ValueError("description must be a string")
        return {"mode": "set", "value": description}

    def local_objects(self, *, compact: bool = False) -> list[dict[str, Any]]:
        """Historical facade for ``objects(scope='local')``."""

        records = self.objects(scope="local", compact=compact)
        if isinstance(records, list):
            return list(records)
        return [
            dict(record) if isinstance(record, dict) else {"name": name, "value": record}
            for name, record in records.items()
        ]

    def server_objects(
        self,
        *,
        owner: str | None = None,
        library: str | None = None,
        compact: bool = False,
    ) -> list[dict[str, Any]]:
        """Historical facade for ``objects(scope='server')``."""

        return list(
            self.objects(
                scope="server",
                owner=owner,
                library=library,
                compact=compact,
            )
        )

    def _owner_for_library_listing(self, library: str) -> str | None:
        """Return the sole foreign owner or reject an ambiguous library slug."""

        matches = [
            record
            for record in self.libraries()
            if isinstance(record, Mapping) and _library_record_slug(record) == library
        ]
        matches.sort(key=lambda record: (_library_record_owner(record) or "", _library_record_slug(record) or ""))
        if len(matches) > 1:
            candidates = ", ".join(_library_candidate_label(record) for record in matches)
            first_owner = _library_record_owner_ref(matches[0]) or "<owner>"
            raise ClientError(
                "library {!r} is ambiguous across accessible owners: {}. "
                "Pass owner=... to choose one (for example, owner={!r}).".format(
                    library,
                    candidates,
                    first_owner,
                )
            )
        if len(matches) == 1 and matches[0].get("owned") is False:
            return _library_record_owner(matches[0])
        return None

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
        ``.raw`` property with the plain payload. ``scope="auto"`` means the
        accessible server catalog when connected and local-only when offline.

        ``owner`` accepts a canonical user id or ``@handle`` and is resolved by
        the server. When ``library`` is set without ``owner`` on a connected
        server leg, the SDK intentionally disambiguates more strictly than the
        legacy server endpoint: multiple accessible owners raise with
        copy-pasteable candidates, while one foreign owner is sent explicitly.
        """

        owner_ref = normalize_owner_ref(owner)
        requested_scope = scope
        if scope == "auto":
            scope = (
                "server" if owner_ref is not None or library is not None or self._has_server_connection() else "local"
            )
        if scope == "local":
            if owner_ref is not None or library is not None:
                raise ValueError("owner/library require scope='server', scope='all', or scope='auto'")
            return _wrap_objects(self._daemon.list_objects(compact=compact), title=object_scope_title("local"))
        listing_owner = owner_ref
        if (
            scope in {"server", "all"}
            and listing_owner is None
            and library is not None
            and self._has_server_connection()
        ):
            listing_owner = self._owner_for_library_listing(library)
        if scope == "server":
            if listing_owner is None and library is None and not self._server_identity_present():
                return ObjectList([], title=object_scope_title("server"))
            try:
                server_objects = self._daemon.server_objects(
                    owner_id=listing_owner,
                    library=library,
                    compact=compact,
                )
            except Exception as exc:
                if not _is_server_unavailable(exc):
                    raise
                if requested_scope == "auto" and listing_owner is None and library is None:
                    return _wrap_objects(self._daemon.list_objects(compact=compact), title=object_scope_title("local"))
                raise
            return _wrap_objects(server_objects, title=object_scope_title("server"))
        if scope == "all":
            local_objects = self._daemon.list_objects(compact=compact)
            if listing_owner is None and library is None and not self._has_server_connection():
                server_objects = []
            else:
                try:
                    server_objects = self._daemon.server_objects(
                        owner_id=listing_owner,
                        library=library,
                        compact=compact,
                    )
                except Exception as exc:
                    if not _is_server_unavailable(exc):
                        raise
                    server_objects = []
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
        """Remove one local object for an optional owner id or ``@handle``.

        Handles are resolved only by the daemon's local boundary. Offline
        handle input therefore surfaces its actionable server-connection error.
        """

        owner_ref = normalize_owner_ref(owner)
        return wrap_action(
            _with_local_cache_warning(self._daemon.forget(name, owner_id=owner_ref, library=library)),
            "object forgotten",
        )

    def remove_local(
        self,
        name: str,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Alias for :meth:`forget`; ``owner`` accepts an id or ``@handle``."""

        return self.forget(name, owner=owner, library=library)

    def forget_version(
        self,
        name: str,
        version: str | int,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Remove one local version for an optional owner id or ``@handle``."""

        owner_ref = normalize_owner_ref(owner)
        return wrap_action(
            _with_local_cache_warning(
                self._daemon.forget_version(
                    name,
                    version,
                    owner_id=owner_ref,
                    library=library,
                )
            ),
            "object version forgotten",
        )

    def prune_stale_mirrors(
        self,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Prune mirrors for an optional canonical owner id or ``@handle``."""

        owner_ref = normalize_owner_ref(owner)
        return wrap_action(
            self._daemon.prune_stale_mirrors(owner_id=owner_ref, library=library),
            "stale mirrors pruned",
        )

    def pull(
        self,
        name: str,
        *,
        owner: str | None = None,
        library: str | None = None,
        version: int | None = None,
        all_versions: bool = False,
    ) -> ActionReceiptView:
        """Mirror one accessible server object into the local daemon cache.

        A pulled server-origin mirror can be inspected and called while the
        daemon is offline. The method requires an active server connection for
        the download itself; repeat pulls are idempotent and report unchanged
        versions under ``skipped``. ``owner`` accepts a canonical id or
        ``@handle`` and is forwarded for server-side resolution.
        """

        owner_id = normalize_owner_ref(owner)
        if not self._has_server_connection():
            raise ClientError(self._pull_requires_server_connection_message())
        resolved_name = name
        resolved_library = library
        if owner_id is None and library is None:
            ref = self._resolve_bare_server_ref(name, example_method="pull")
            resolved_name = ref.name
            owner_id = ref.owner_id
            resolved_library = ref.library
        return wrap_action(
            self._daemon.pull_server_object(
                resolved_name,
                owner_id=owner_id,
                library=resolved_library,
                version=version,
                all_versions=all_versions,
            ),
            "object pulled",
        )

    def pull_all(
        self,
        *,
        owner: str | None = None,
        library: str | None = None,
        all_versions: bool = False,
        dry_run: bool = False,
    ) -> ActionReceiptView:
        """Mirror the visible server catalog into the local daemon cache.

        The visible catalog can be large. Start with ``dry_run=True`` and
        narrow the batch with ``owner=`` or ``library=`` before downloading a
        whole catalog. ``dry_run`` returns the same receipt shape without
        writing local mirror rows or YAML files. ``owner`` accepts a canonical
        id or ``@handle`` and is resolved by the server.
        """

        owner_ref = normalize_owner_ref(owner)
        if not self._has_server_connection():
            raise ClientError(self._pull_all_requires_server_connection_message())
        return wrap_action(
            self._daemon.pull_all_server_objects(
                owner_id=owner_ref,
                library=library,
                all_versions=all_versions,
                dry_run=dry_run,
            ),
            "server catalog pull plan" if dry_run else "server catalog pulled",
        )

    def _has_server_connection(self) -> bool:
        if self.server_connection is not None:
            return _state_has_server_connection(self.server_connection)
        try:
            state = self._daemon.server_connection()
        except Exception as exc:
            if _is_server_unavailable(exc):
                return False
            raise
        return _state_has_server_connection(state)

    def _server_identity_present(self) -> bool:
        """Return whether the daemon has a stored central-server identity."""

        state = self.server_connection
        if state is None:
            try:
                state = self._daemon.server_connection()
            except Exception as exc:
                if _is_missing_server_connection(exc):
                    return False
                if _is_server_unreachable(exc):
                    return False
                raise
        if not isinstance(state, Mapping):
            return False
        explicit = state.get("identity_present")
        if explicit is not None:
            return bool(explicit)
        return bool(_server_connection_mapping(state)) or _state_has_server_connection(state)

    def _server_connection_label(self) -> str:
        state = self.server_connection
        if state is None:
            try:
                state = self._daemon.server_connection()
            except Exception:
                state = None
        connection = (
            state.get("connection") or state.get("remote_connection") or state if isinstance(state, dict) else {}
        )
        if isinstance(connection, Mapping):
            for key in ("display_name", "machine_id", "id", "user_id", "server_url"):
                value = connection.get(key)
                if value is not None and str(value):
                    return str(value)
        return "the active server connection"

    @staticmethod
    def _offline_bare_server_ref_message(name: str) -> str:
        return (
            "404: {!r} is not registered locally and the daemon has no server connection; "
            "reconnect with client.connect_server(...) to search the server catalog, or "
            "client.pull(...) the object next time you are online."
        ).format(name)

    @staticmethod
    def _unreachable_bare_server_ref_message(name: str) -> str:
        return (
            "404: {!r} is not registered locally; server unreachable; "
            "mirror it with client.pull(...) when online, or reconnect with "
            "client.connect_server(...) to search the server catalog."
        ).format(name)

    @staticmethod
    def _pull_requires_server_connection_message() -> str:
        return (
            "404: pull requires a server connection; the daemon has no server connection. "
            "Reconnect with client.connect_server(...) to search the server catalog, "
            "then retry client.pull(...)."
        )

    @staticmethod
    def _pull_all_requires_server_connection_message() -> str:
        return (
            "404: pull_all requires a server connection; the daemon has no server connection. "
            "Reconnect with client.connect_server(...) to search the server catalog, "
            "then retry client.pull_all(...)."
        )

    def _resolve_bare_server_ref(self, name: str, *, example_method: str = "signature") -> _BareServerRef:
        """Resolve a bare object name through the visible server catalog after a local 404.

        This is intentionally catalog-driven: no default library is assumed.
        Ambiguous server names fail loudly and ask the caller to pass
        ``owner=``/``library=`` explicitly.
        """

        try:
            connection_state = (
                self.server_connection if self.server_connection is not None else self._daemon.server_connection()
            )
        except Exception as exc:
            if _is_server_unreachable(exc):
                raise ClientError(self._unreachable_bare_server_ref_message(name)) from exc
            if _is_missing_server_connection(exc):
                raise ClientError(self._offline_bare_server_ref_message(name)) from exc
            raise
        if not _state_has_server_connection(connection_state):
            if _state_is_server_unreachable(connection_state):
                raise ClientError(self._unreachable_bare_server_ref_message(name))
            raise ClientError(self._offline_bare_server_ref_message(name))
        try:
            server_records = self._daemon.server_objects(compact=True)
        except Exception as exc:
            if _is_server_unreachable(exc):
                raise ClientError(self._unreachable_bare_server_ref_message(name)) from exc
            if _is_missing_server_connection(exc):
                raise ClientError(self._offline_bare_server_ref_message(name)) from exc
            raise
        candidates = [
            record for record in server_records if isinstance(record, Mapping) and name in _server_record_names(record)
        ]
        if len(candidates) == 1:
            record = candidates[0]
            record_name = _record_string(record.get("name")) or name
            return _BareServerRef(
                name=record_name,
                owner_id=_server_record_owner(record),
                library=_server_record_library(record),
            )
        if candidates:
            choices = ", ".join(_server_candidate_label(record) for record in candidates)
            raise ClientError(
                "{!r} is not registered locally; found on the server in: {}. "
                "Pass library=... and owner=... to disambiguate, for example "
                "client.{}({!r}, library='...').".format(name, choices, example_method, name)
            )
        raise ClientError(
            "{!r} is not registered locally; no accessible server object named {!r} (connected as {}). "
            "Run client.objects(scope='server') to inspect the accessible catalog, or pass "
            "owner=... and library=... if you know the server scope.".format(
                name,
                name,
                self._server_connection_label(),
            )
        )

    def _signature_payload(
        self,
        name: str,
        *,
        version: int | None,
        version_id: str | None = None,
        owner: str | None,
        library: str | None,
        function: str | None,
    ) -> dict[str, Any]:
        owner_ref = normalize_owner_ref(owner)
        signature_options: dict[str, Any] = {
            "version": version,
            "owner_id": owner_ref,
            "library": library,
            "function": function,
        }
        if version_id is not None:
            signature_options["version_id"] = version_id
        try:
            return self._daemon.signature(name, **signature_options)
        except ClientError as exc:
            if owner_ref is not None or library is not None or not _local_object_missing(exc):
                raise
            ref = self._resolve_bare_server_ref(name)
            signature_options["owner_id"] = ref.owner_id
            signature_options["library"] = ref.library
            signature = dict(self._daemon.signature(ref.name, **signature_options))
            signature["resolved_from_server"] = _resolved_from_server_record(ref)
            return signature

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
        """Return a concise call/read signature for one daemon object.

        Bare names still resolve locally first. When connected to the server and
        the local registry returns 404, the client looks up the exact name in the
        accessible server catalog and retries only if that catalog has a single
        unambiguous match. ``owner`` accepts a canonical user id or ``@handle``;
        the SDK forwards it unchanged and performs no directory lookup.
        """

        return SignatureView(
            self._signature_payload(
                name,
                version=version,
                owner=owner,
                library=library,
                function=function,
            )
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
        """Return inputs; ``owner`` accepts a canonical id or ``@handle``."""

        owner_ref = normalize_owner_ref(owner)
        try:
            return InputListView(
                self._daemon.inputs(
                    name,
                    version=version,
                    owner_id=owner_ref,
                    library=library,
                    function=function,
                )
            )
        except ClientError as exc:
            if owner_ref is not None or library is not None or not _local_object_missing(exc):
                raise
            signature = self._signature_payload(
                name,
                version=version,
                owner=owner_ref,
                library=library,
                function=function,
            )
            return InputListView(signature["inputs"])

    def outputs(
        self,
        name: str,
        *,
        version: int | None = None,
        owner: str | None = None,
        library: str | None = None,
        function: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return outputs; ``owner`` accepts a canonical id or ``@handle``."""

        owner_ref = normalize_owner_ref(owner)
        try:
            return OutputListView(
                self._daemon.outputs(
                    name,
                    version=version,
                    owner_id=owner_ref,
                    library=library,
                    function=function,
                )
            )
        except ClientError as exc:
            if owner_ref is not None or library is not None or not _local_object_missing(exc):
                raise
            signature = self._signature_payload(
                name,
                version=version,
                owner=owner_ref,
                library=library,
                function=function,
            )
            return OutputListView(signature["outputs"])

    def versions(
        self,
        name: str,
        *,
        owner: str | None = None,
        library: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return object versions for an optional owner id or ``@handle``."""

        return VersionListView(
            self._daemon.object_versions(
                name,
                owner_id=normalize_owner_ref(owner),
                library=library,
            )
        )

    def decomposition(
        self,
        name: Any,
        *,
        version: int | None = None,
        owner: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Return decomposition for an optional owner id or ``@handle``."""

        if self._is_node_remote(name):
            return DecompositionView(self._remote_decomposition_response(name, version=version)["decomposition"])
        owner_ref = normalize_owner_ref(owner)
        if owner_ref is not None or library is not None:
            response = self._remote_decomposition_response(
                {
                    "name": str(name),
                    "version": version,
                    "owner_id": owner_ref,
                    "library": library,
                }
            )
            return DecompositionView(response["decomposition"])
        try:
            return DecompositionView(self._daemon.decomposition(str(name), version=version))
        except ClientError as exc:
            if not _local_object_missing(exc):
                raise
            ref = self._resolve_bare_server_ref(str(name))
            response = self._remote_decomposition_response(
                {
                    "name": ref.name,
                    "version": version,
                    "owner_id": ref.owner_id,
                    "library": ref.library,
                }
            )
            decomposition = dict(response["decomposition"])
            decomposition["resolved_from_server"] = _resolved_from_server_record(ref)
            return DecompositionView(decomposition)

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
            if version is not None and pipeline.version not in {"", "latest", "current"}:
                raise ValueError("pass the version either on NodeRemote or draw_pipeline(...), not both")
            response = self._remote_decomposition_response(pipeline, version=version)
            decomposition = response["decomposition"]
            if not decomposition.get("nodes"):
                raise ValueError(f"remote object is not a pipeline or has no nodes: {pipeline.name}")
            record = response.get("object") or {}
            remote = response.get("remote") or {}
            object_name = (
                title or record.get("display_name") or record.get("name") or remote.get("name") or pipeline.name
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
            try:
                record = self._daemon.get_object(
                    pipeline,
                    version=version,
                    include_yaml=True,
                )
            except ClientError as exc:
                if not _local_object_missing(exc):
                    raise
                ref = self._resolve_bare_server_ref(pipeline)
                response = self._remote_decomposition_response(
                    {
                        "name": ref.name,
                        "version": version,
                        "owner_id": ref.owner_id,
                        "library": ref.library,
                    }
                )
                decomposition = dict(response["decomposition"])
                decomposition["resolved_from_server"] = _resolved_from_server_record(ref)
                if not decomposition.get("nodes"):
                    raise ValueError(f"remote object is not a pipeline or has no nodes: {pipeline}")
                record = response.get("object") or {}
                remote = response.get("remote") or {}
                object_name = (
                    title or record.get("display_name") or record.get("name") or remote.get("name") or pipeline
                )
                return PipelineGraphWidget(
                    decomposition,
                    {
                        **record,
                        "remote": remote,
                        "id": record.get("id") or remote.get("object_id") or pipeline,
                        "name": record.get("name") or remote.get("name") or pipeline,
                        "displayName": object_name,
                    },
                    height=height,
                    theme=theme,
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

        raise TypeError("pipeline_widget expects an object name, PublishedObject, spl.core Pipeline, or NodeRemote")

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
        """Describe an object for an optional canonical owner id or ``@handle``."""

        signature = self.signature(
            name,
            version=version,
            owner=owner,
            library=library,
            function=function,
        )
        freshness_line = None
        if (
            function is None
            and "resolved_from_server" not in signature
            and "resolved_from" not in signature
            and self._has_server_connection()
        ):
            try:
                server_records = self._daemon.server_objects(compact=True)
            except Exception as exc:
                if not _is_server_unavailable(exc):
                    raise
            else:
                freshness_line = _server_freshness_line(
                    signature,
                    server_records,
                    owner=owner,
                    library=library,
                )
        display_name = signature.get("display_name") or signature["name"]
        lines = [(f"{display_name} v{signature['version']} ({signature['kind']})")]
        resolution = signature.get("resolved_from_server")
        if not isinstance(resolution, Mapping):
            resolution = signature.get("resolved_from")
        server_resolution = _server_resolution_label(resolution)
        if server_resolution is not None:
            lines.append(f"Resolved from server: {server_resolution}")
        if signature.get("description"):
            lines.append(signature["description"])

        if function is None and signature.get("kind") == "pipeline" and signature.get("internal_functions"):
            lines.append("Functions:")
            for item in signature["internal_functions"]:
                lines.append(f"  - {item['name']}")

        lines.append("Inputs:")
        if signature["inputs"]:
            for item in signature["inputs"]:
                required = "required" if item["required"] else "optional"
                default = "" if item["default"] is None else f", default={item['default']}"
                lines.append(f"  - {item['name']}: {item['type'] or 'Any'} ({required}{default})")
        else:
            lines.append("  - none")

        lines.append("Outputs:")
        if signature["outputs"]:
            for item in signature["outputs"]:
                selector = f'output="{item["selector"]}"' if item["selector"] is not None else "no output selector"
                lines.append(f"  - {item['name']}: {selector}; read {item['read']}")
        else:
            lines.append("  - none")

        lines.append(f"Example: {signature['call']['example']}")
        lines.append(f"Read: {signature['call']['read']}")
        if freshness_line is not None:
            lines.append(freshness_line)
        return "\n".join(lines)

    def envs(self) -> dict[str, Any]:
        """Return registered daemon environments."""

        return EnvTableView(self._daemon.list_envs())

    def environment_builds(self) -> list[dict[str, Any]]:
        """Return cached daemon venv builds."""

        return EnvironmentBuildListView(self._daemon.list_environment_builds())

    def rebuild_environment(
        self,
        spec_hash: str,
        *,
        wait: bool = False,
    ) -> dict[str, Any]:
        """Force a cached daemon venv build to be recreated."""

        return wrap_action(
            self._daemon.rebuild_environment_build(spec_hash, wait=wait),
            "environment build",
        )

    def runs(self, *, local: bool = False) -> list[dict[str, Any]]:
        """Return known daemon runs, newest first."""

        if local:
            return RunListView(m_manifest.list_local_runs())
        daemon_runs = self._daemon.list_runs()
        return RunListView(daemon_runs, local_retained_count=_local_retained_run_count())

    def run_show(self, run_id: str, *, full_inline: bool = False, local: bool | None = None) -> dict[str, Any]:
        """Return one retained run manifest.

        Inline JSON values are summarized by default; pass ``full_inline=True``
        to include the full manifest values. With ``local`` unset, local-format
        retained run ids are read from the local manifest store automatically;
        explicit ``local=True``/``False`` still selects one namespace.
        """

        namespace = _run_id_namespace(run_id)
        use_local = local is True or (local is None and namespace == "local")
        if use_local:
            payload = m_manifest.show_local_run(run_id, include_inline_values=full_inline)
            return RunRecordView(payload)

        try:
            payload = self._daemon.show_run(run_id, full_inline=full_inline)
        except ClientError as exc:
            if not _is_404_error(exc):
                raise
            raise ClientError(_run_show_not_found_message(run_id, namespace)) from None
        return RunRecordView(payload)

    def resume(
        self,
        run_id: str,
        *,
        from_: Any,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        adapters: dict[str, Any] | None = None,
        runtimes: str | dict[str, str] | None = None,
        keep: bool | str | None = None,
        wait: bool = False,
        artifacts_dir: str | Path | None = None,
        progress: ProgressOption = True,
    ) -> RemoteRun | RemoteResult:
        """Resume a retained daemon pipeline run from selected recalculation nodes."""

        if _run_id_namespace(run_id) == "local":
            raise ValueError(
                "This is a local retained run id; resume it with "
                "`Deployment(<pipeline>).resume({!r}, from_=...)`. "
                "client.resume() drives daemon runs only.".format(run_id)
            )

        state = self._daemon.resume_run(
            run_id,
            from_=from_,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            adapters=adapters,
            runtimes=runtimes,
            keep=keep,
        )
        run = RemoteRun(self, state)
        if not wait:
            return run
        return run.collect(
            artifacts_dir=artifacts_dir,
            timeout_seconds=timeout_seconds,
            progress=progress,
        )

    def prune_runs(
        self,
        *,
        run_id: str | None = None,
        status: str | list[str] | None = None,
        older_than_seconds: float | None = None,
        dry_run: bool = False,
        local: bool = False,
    ) -> dict[str, Any]:
        """Prune inactive retained runs by id, status, age, or retention TTL."""

        statuses = [status] if isinstance(status, str) else status
        result = (
            m_manifest.prune_local_runs(
                run_id=run_id,
                statuses=statuses,
                older_than_seconds=older_than_seconds,
                dry_run=dry_run,
            )
            if local
            else self._daemon.prune_runs(
                run_id=run_id,
                statuses=statuses,
                older_than_seconds=older_than_seconds,
                dry_run=dry_run,
            )
        )
        return wrap_action(result, "runs pruned")

    def _resolve_run_adapter_selectors(
        self,
        adapters: Any,
        *,
        owner: str | None,
        library: str | None,
    ) -> Any:
        """Resolve short Library Adapter selectors to immutable exact refs.

        Built-in string IDs retain precedence.  Every Library Adapter selector
        is re-read before Run admission and becomes a full ``LibraryAdapterRef``;
        no mutable name or numeric version crosses the execution boundary.
        """

        mapping = normalize_public_adapter_mapping(adapters)
        if mapping is None:
            return None
        resolved: dict[str, dict[str, Any]] = {"inputs": {}, "outputs": {}}
        for section in ("inputs", "outputs"):
            for port, value in mapping[section].items():
                if isinstance(value, LibraryAdapterRef):
                    resolved[section][port] = value
                    continue
                if isinstance(value, str) and value in BUILTIN_ADAPTER_IDS:
                    resolved[section][port] = value
                    continue
                if isinstance(value, str):
                    selector: Mapping[str, Any] = {"name": value}
                elif isinstance(value, Mapping):
                    selector = value
                else:
                    resolved[section][port] = value
                    continue
                resolved[section][port] = self._resolve_library_adapter_selector(
                    selector,
                    owner=owner,
                    library=library,
                )
        return resolved

    def _resolve_library_adapter_selector(
        self,
        selector: Mapping[str, Any],
        *,
        owner: str | None,
        library: str | None,
    ) -> LibraryAdapterRef:
        capability_check = getattr(self._daemon, "require_library_adapter_capability", None)
        if not callable(capability_check):
            raise ClientError(
                "local SPL daemon does not support Library Adapter name/version selectors; "
                "upgrade and restart the daemon"
            )
        capability_check(LIBRARY_ADAPTER_CATALOG_CAPABILITY)
        allowed = {"name", "version", "version_id", "owner", "library"}
        unknown = sorted(set(selector) - allowed)
        if unknown:
            raise RuntimePortAdapterContractError(
                "library_adapter_selector_fields",
                "Library Adapter selector contains unknown field(s): " + ", ".join(map(str, unknown)),
                stage="admission",
            )
        name = selector.get("name")
        if not isinstance(name, str) or not name:
            raise RuntimePortAdapterContractError(
                "library_adapter_selector_name",
                "Library Adapter selector requires a non-empty name",
                stage="admission",
            )
        version = selector.get("version")
        version_id = selector.get("version_id")
        if version is not None and version_id is not None:
            raise RuntimePortAdapterContractError(
                "library_adapter_selector_version",
                "Library Adapter selector accepts either version or version_id, not both",
                stage="admission",
            )
        if version is not None and (isinstance(version, bool) or not isinstance(version, int) or version < 1):
            raise RuntimePortAdapterContractError(
                "library_adapter_selector_version",
                "Library Adapter version must be a positive integer",
                stage="admission",
            )
        if version_id is not None and (not isinstance(version_id, str) or not version_id):
            raise RuntimePortAdapterContractError(
                "library_adapter_selector_version_id",
                "Library Adapter version_id must be a non-empty string",
                stage="admission",
            )
        selector_owner = selector.get("owner", owner)
        selector_library = selector.get("library", library)
        if selector_owner is not None and not isinstance(selector_owner, str):
            raise RuntimePortAdapterContractError(
                "library_adapter_selector_owner",
                "Library Adapter owner must be a string",
                stage="admission",
            )
        if selector_library is not None and (not isinstance(selector_library, str) or not selector_library):
            raise RuntimePortAdapterContractError(
                "library_adapter_selector_library",
                "Library Adapter library must be a non-empty string",
                stage="admission",
            )

        current = self._find_library_adapter_by_name(
            name,
            owner=selector_owner,
            library=selector_library,
        )
        selected: Mapping[str, Any]
        if version_id is not None:
            try:
                selected = self._daemon.get_library_adapter_version(
                    current["adapter_id"],
                    version_id,
                    owner_id=current["owner"],
                    library=current["library"],
                )
            except ClientError:
                raise RuntimePortAdapterContractError(
                    "library_adapter_version_not_found",
                    f"Library Adapter {name!r} has no accessible version_id {version_id!r}",
                    stage="admission",
                ) from None
        elif version is not None:
            selected = self._find_library_adapter_version(
                current,
                version=version,
            )
        else:
            selected = current
        if selected.get("name") != name:
            raise RuntimePortAdapterContractError(
                "library_adapter_identity_mismatch",
                "resolved Library Adapter version does not belong to the requested name",
                stage="admission",
            )
        return _library_adapter_ref_from_record(selected)

    def _find_library_adapter_by_name(
        self,
        name: str,
        *,
        owner: str | None,
        library: str | None,
    ) -> Mapping[str, Any]:
        cursor: str | None = None
        matches: list[Mapping[str, Any]] = []
        for _page in range(20):
            page = self._daemon.list_library_adapters(
                owner_id=owner,
                library=library,
                query=name,
                limit=100,
                cursor=cursor,
            )
            items = page.get("items")
            if not isinstance(items, list):
                raise RuntimePortAdapterContractError(
                    "library_adapter_catalog_invalid",
                    "Library Adapter catalog response is invalid",
                    stage="admission",
                )
            matches.extend(item for item in items if isinstance(item, Mapping) and item.get("name") == name)
            cursor_value = page.get("next_cursor")
            if cursor_value is None:
                break
            if not isinstance(cursor_value, str) or not cursor_value:
                raise RuntimePortAdapterContractError(
                    "library_adapter_catalog_invalid",
                    "Library Adapter catalog cursor is invalid",
                    stage="admission",
                )
            cursor = cursor_value
        else:
            raise RuntimePortAdapterContractError(
                "library_adapter_catalog_bounded",
                "Library Adapter name resolution exceeded the bounded catalog scan",
                stage="admission",
            )
        identities = {(item.get("owner"), item.get("library"), item.get("adapter_id")): item for item in matches}
        if not identities:
            raise RuntimePortAdapterContractError(
                "library_adapter_not_found",
                f"Library Adapter {name!r} is not available in the selected owner/Library scope",
                stage="admission",
            )
        if len(identities) != 1:
            choices = ", ".join(
                sorted(f"{item.get('owner')}/{item.get('library')}/{name}" for item in identities.values())
            )
            raise RuntimePortAdapterContractError(
                "library_adapter_ambiguous",
                f"Library Adapter {name!r} is ambiguous; specify owner and library ({choices})",
                stage="admission",
            )
        return next(iter(identities.values()))

    def _find_library_adapter_version(
        self,
        current: Mapping[str, Any],
        *,
        version: int,
    ) -> Mapping[str, Any]:
        cursor: str | None = None
        for _page in range(20):
            page = self._daemon.library_adapter_versions(
                current["adapter_id"],
                owner_id=current["owner"],
                library=current["library"],
                limit=100,
                cursor=cursor,
            )
            items = page.get("items")
            if not isinstance(items, list):
                raise RuntimePortAdapterContractError(
                    "library_adapter_versions_invalid",
                    "Library Adapter version response is invalid",
                    stage="admission",
                )
            for item in items:
                if isinstance(item, Mapping) and item.get("version") == version:
                    return item
            cursor_value = page.get("next_cursor")
            if cursor_value is None:
                break
            if not isinstance(cursor_value, str) or not cursor_value:
                raise RuntimePortAdapterContractError(
                    "library_adapter_versions_invalid",
                    "Library Adapter version cursor is invalid",
                    stage="admission",
                )
            cursor = cursor_value
        raise RuntimePortAdapterContractError(
            "library_adapter_version_not_found",
            f"Library Adapter {current.get('name')!r} has no accessible version {version}",
            stage="admission",
        )

    def _start_run(
        self,
        name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        version_id: str | None = None,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        offline_policy: OfflinePolicy | None = None,
        function: str | None = None,
        source: RunSource = "auto",
        adapters: Any | None = None,
        adapter_policy: Mapping[str, Any] | None = None,
        runtimes: str | dict[str, str] | None = None,
        keep: bool | str | None = None,
    ) -> RemoteRun:
        """Shared implementation behind ``submit``/``call`` (and legacy aliases)."""

        if isinstance(adapters, Mapping) and any(not isinstance(key, str) for key in adapters):
            raise NotImplementedError(
                "tuple-key adapter overrides belong to local Deployment.run; "
                "SPLClient.call/submit adapters must use closed inputs/outputs sections"
            )

        normalize_adapter_policy(adapter_policy)
        owner_ref = normalize_owner_ref(owner)
        adapters = self._resolve_run_adapter_selectors(
            adapters,
            owner=owner_ref,
            library=library,
        )
        runtime_plan: PreparedClientRuntimeAdapters | None = None
        send_runtime_semantic_advisories = False
        if requires_runtime_adapter_path(args=args, kwargs=kwargs, adapters=adapters):
            capability_check = getattr(self._daemon, "require_runtime_port_adapters_capability", None)
            if not callable(capability_check):
                raise ClientError(
                    "local SPL daemon does not support runtime port adapters; "
                    "upgrade and restart the daemon before using adapters or FileInput"
                )
            capability_check()
            library_adapter_versions: dict[str, Mapping[str, Any]] = {}
            library_refs = _library_adapter_refs_from_mapping(adapters)
            if library_refs:
                library_capability_check = getattr(
                    self._daemon,
                    "require_library_adapter_capability",
                    None,
                )
                if not callable(library_capability_check):
                    raise ClientError(
                        "local SPL daemon does not support exact Library Adapter Run references; "
                        "upgrade and restart the daemon"
                    )
                library_capability_check(RUNTIME_LIBRARY_ADAPTER_REF_CAPABILITY)
                for ref_value in library_refs:
                    ref = normalize_library_adapter_ref(ref_value)
                    if ref["adapter_version_id"] in library_adapter_versions:
                        continue
                    version_record = self._daemon.get_library_adapter_version(
                        ref["adapter_id"],
                        ref["adapter_version_id"],
                        owner_id=ref["owner"],
                        library=ref["library"],
                    )
                    if any(version_record.get(key) != expected for key, expected in ref.items()):
                        raise RuntimePortAdapterContractError(
                            "library_adapter_ref_unverified",
                            "Library Adapter reference does not match the exact immutable daemon version",
                            stage="admission",
                        )
                    library_adapter_versions[ref["adapter_version_id"]] = version_record
            signature = self._signature_payload(
                name,
                version=version,
                version_id=version_id,
                owner=owner,
                library=library,
                function=function,
            )
            runtime_plan = prepare_client_runtime_adapters(
                signature=signature,
                args=args,
                kwargs=kwargs,
                output=output,
                adapters=adapters,
                adapter_policy=adapter_policy,
                library_adapter_versions=library_adapter_versions,
            )
            args = runtime_plan.args
            kwargs = runtime_plan.kwargs
            semantic_advisories = runtime_plan.runtime_adapter_semantic_advisories
            if semantic_advisories is not None:
                requires_override = any(item.get("state") != "recommended" for item in semantic_advisories["bindings"])
                semantic_capability_check = getattr(
                    self._daemon,
                    "require_runtime_adapter_semantic_override_capability",
                    None,
                )
                if callable(semantic_capability_check):
                    try:
                        semantic_capability_check()
                    except ClientError:
                        if requires_override:
                            raise
                    else:
                        send_runtime_semantic_advisories = True
                elif requires_override:
                    raise ClientError(
                        "local SPL daemon does not support explicit runtime adapter semantic advisories; "
                        "upgrade and restart the daemon before selecting this adapter"
                    )

        scoped = owner_ref is not None or library is not None
        remote = target_machine is not None or (source != "local" and scoped and self._has_server_connection())
        # ``daemon_client.Client.run`` retains a legacy convenience fallback
        # that treats owner/library selectors as a remote-run request when the
        # caller omits ``remote``.  Preserve an explicit local (or offline
        # scoped) decision as ``False`` so that fallback cannot silently change
        # the route after this layer has selected the matching result namespace.
        remote_override = remote if remote or source == "local" or scoped else None
        run_kwargs: dict[str, Any] = {
            "args": args,
            "kwargs": kwargs,
            "output": output,
            "timeout_seconds": timeout_seconds,
            "target_machine": target_machine,
            "object_owner_id": owner_ref,
            "library": library,
            "offline_policy": offline_policy,
            "function": function,
            "source": source,
            "remote": remote_override,
        }
        if version is not None:
            run_kwargs["version"] = version
        if version_id is not None:
            run_kwargs["version_id"] = version_id
        if runtimes is not None:
            run_kwargs["runtimes"] = runtimes
        if keep is not None:
            run_kwargs["keep"] = keep
        if runtime_plan is not None:
            run_kwargs["runtime_port_adapters"] = runtime_plan.document
            run_kwargs["adapter_policy"] = runtime_plan.adapter_policy
            if runtime_plan.runtime_library_adapter_refs is not None:
                run_kwargs["runtime_library_adapter_refs"] = runtime_plan.runtime_library_adapter_refs
            if send_runtime_semantic_advisories:
                run_kwargs["runtime_adapter_semantic_advisories"] = runtime_plan.runtime_adapter_semantic_advisories
        state = self._daemon.run(name, **run_kwargs)
        return RemoteRun(
            self,
            state,
            server_side=remote,
            runtime_adapter_plan=runtime_plan,
        )

    def start(
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
        **compat_kwargs: Any,
    ) -> RemoteRun:
        """Historical 0.1.x facade for :meth:`submit`."""

        options: dict[str, Any] = {
            key: value
            for key, value in {
                "args": args,
                "kwargs": kwargs,
                "output": output,
                "timeout_seconds": timeout_seconds,
                "target_machine": target_machine,
                "owner": owner,
                "library": library,
                "offline_policy": offline_policy,
                "function": function,
            }.items()
            if value is not None
        }
        if source != "auto":
            options["source"] = source
        options.update(compat_kwargs)
        return self.submit(name, **options)

    def submit(
        self,
        name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        version_id: str | None = None,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        offline_policy: OfflinePolicy | None = None,
        function: str | None = None,
        source: RunSource = "auto",
        adapters: Any | None = None,
        adapter_policy: Mapping[str, Any] | None = None,
        runtimes: str | dict[str, str] | None = None,
        keep: bool | str | None = None,
    ) -> RemoteRun:
        """Canonical async entry point: start a run, return a handle immediately.

        The default path is local daemon execution.  Passing ``target_machine``,
        or passing ``owner``/``library`` while connected selects central-server
        remote execution through the daemon. Pass ``version`` to select a
        numbered Object version, or ``version_id`` to bind the Run to an exact
        Object version identity. Offline scoped calls remain local
        first; a canonical owner id can address a local mirror, while an
        ``@handle`` is rejected by the daemon with the actionable offline
        handle message. The SDK never resolves the handle. ``adapters`` accepts
        closed per-Run ``inputs``/``outputs`` bindings. Library Adapter values
        may be a name (current version) or a closed ``name`` plus ``version``
        or ``version_id`` selector; the call-level ``version_id`` continues to
        select the Object. ``runtimes`` accepts either per-Pipeline-alias
        overrides or one runtime name for the whole Function/Pipeline.
        ``adapter_policy``
        controls explicit custom execution consent without changing the Object.
        """

        return self._start_run(
            name,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            version=version,
            version_id=version_id,
            target_machine=target_machine,
            owner=owner,
            library=library,
            offline_policy=offline_policy,
            function=function,
            source=source,
            adapters=adapters,
            adapter_policy=adapter_policy,
            runtimes=runtimes,
            keep=keep,
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
        """Historical 0.1.x queued-run facade."""

        options: dict[str, Any] = {
            key: value
            for key, value in {
                "args": args,
                "kwargs": kwargs,
                "output": output,
                "timeout_seconds": timeout_seconds,
                "target_machine": target_machine,
                "owner": owner,
                "library": library,
                "function": function,
            }.items()
            if value is not None
        }
        options["offline_policy"] = "queue"
        if source != "auto":
            options["source"] = source
        return self.submit(name, **options)

    def call(
        self,
        name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        version_id: str | None = None,
        artifacts_dir: str | Path | None = None,
        target_machine: str | None = None,
        owner: str | None = None,
        library: str | None = None,
        offline_policy: OfflinePolicy | None = None,
        function: str | None = None,
        source: RunSource = "auto",
        adapters: Any | None = None,
        adapter_policy: Mapping[str, Any] | None = None,
        runtimes: str | dict[str, str] | None = None,
        keep: bool | str | None = None,
        progress: ProgressOption = True,
        trust: bool = False,
    ) -> RemoteResult:
        """Run an object, wait for completion, and return result/artifacts.

        With only ``name``/``args``/``kwargs`` this is a local daemon worker
        call. Passing ``target_machine``, or passing ``owner``/``library`` while
        connected, makes it a server-side remote run through the daemon.
        ``owner`` accepts a canonical user id or ``@handle`` and is forwarded
        unchanged for daemon/server resolution. The returned
        ``RemoteResult.mode`` is therefore either ``"local"`` or ``"server"``.
        Pass ``version`` to select a numbered Object version, or ``version_id``
        to bind the Run to an exact Object version identity.

        While waiting, slow phases (a first-run environment build, a queued
        server-side run) print short progress lines to stderr.  Pass
        ``progress=False`` to wait silently, or a callable to receive every
        polled run state instead. ``adapters`` accepts independent per-Run
        ``inputs``/``outputs`` bindings, including independent Library Adapter
        version selectors for every port. ``runtimes`` accepts a Pipeline alias
        mapping or one whole-target runtime name. Custom remote execution additionally
        requires ``adapter_policy={"custom_remote": "allow"}``.
        """

        if self._embedded_backend is not None:
            unsupported = {
                "output": output,
                "version": version,
                "version_id": version_id,
                "target_machine": target_machine,
                "owner": owner,
                "library": library,
                "offline_policy": offline_policy,
                "function": function,
                "adapters": adapters,
                "adapter_policy": adapter_policy,
                "runtimes": runtimes,
                "keep": keep,
            }
            if source != "auto":
                unsupported["source"] = source
            supplied = sorted(key for key, value in unsupported.items() if value is not None)
            if supplied:
                message = "embedded public calls do not support: " + ", ".join(supplied)
                raise ClientError(
                    f"feature_not_supported: {message}",
                    payload={"code": "feature_not_supported", "error": message},
                )
            embedded_result = self._embedded_backend.call(
                name,
                args=args,
                kwargs=kwargs,
                timeout_seconds=timeout_seconds,
                artifacts_dir=artifacts_dir,
                trust=trust,
            )
            return RemoteResult(
                run=embedded_result.run,
                payload=embedded_result.payload,
                mode="embedded",
                downloaded_artifacts=embedded_result.downloaded_artifacts,
                _native_value=(embedded_result.native_value if embedded_result.has_native_value else _NO_NATIVE_VALUE),
            )

        if trust:
            message = "trust is available only for SPLClient embedded public calls"
            raise ClientError(
                f"feature_not_supported: {message}",
                payload={"code": "feature_not_supported", "error": message},
            )

        run = self._start_run(
            name,
            args=args,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            version=version,
            version_id=version_id,
            target_machine=target_machine,
            owner=owner,
            library=library,
            offline_policy=offline_policy,
            function=function,
            source=source,
            adapters=adapters,
            adapter_policy=adapter_policy,
            runtimes=runtimes,
            keep=keep,
        )
        return run.collect(
            artifacts_dir=artifacts_dir,
            # Central Run snapshots may contain bounded inline inputs and
            # results. Polling those documents four times per second creates
            # avoidable load during batch workloads; local Runs stay at the
            # established low-latency default.
            poll_interval=(REMOTE_RUN_POLL_INTERVAL_SECONDS if run.server_side else 0.25),
            timeout_seconds=timeout_seconds,
            progress=progress,
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
        """Historical facade for direct ``NodeRemote`` execution."""

        return self._run_node_value(node, kwargs, timeout_seconds=timeout_seconds)

    def run_node_result(
        self,
        node: Any,
        *,
        kwargs: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> RemoteResult:
        """Historical facade returning metadata for direct ``NodeRemote`` execution."""

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
        ref = self._remote_node_payload(remote, version=version) if self._is_node_remote(remote) else dict(remote)
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

    mapping = {root: DSPLSelfImport(name=cast(Any, root).name) for (root, _) in top_level_deps if hasattr(root, "name")}

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
                    "unnamed pipeline requires entrypoint; use pipeline.render(name) or publish(..., entrypoint='name')"
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
