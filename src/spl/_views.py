"""Compact notebook/log views for service-shaped SPL payloads.

The classes in this module deliberately subclass ``dict`` or ``list``.  Public
client methods can return them without breaking existing code that indexes,
iterates, calls ``.get()``, compares with plain containers, or serializes with
``json.dumps``.  Only human-facing representations are compacted.
"""

from __future__ import annotations

from collections.abc import Mapping
from html import escape
from pathlib import Path
from typing import Any

_EMPTY = "—"
_DEFAULT_CELL_LIMIT = 72


def preview(value: Any, *, limit: int = _DEFAULT_CELL_LIMIT) -> str:
    """Return a single-line, bounded representation of ``value``."""

    if value is None:
        text = _EMPTY
    elif isinstance(value, str):
        text = value
    elif isinstance(value, Path):
        text = str(value)
    else:
        text = repr(value)
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 3)]}..."


def details_to_text(title: str, rows: list[tuple[str, Any]]) -> str:
    """Render key/value rows for terminal ``repr`` output."""

    if not rows:
        return f"{title}: (empty)"
    width = max(len(label) for label, _ in rows)
    body = "\n".join(
        f"{label.ljust(width)}: {preview(value, limit=100)}"
        for label, value in rows
    )
    return f"{title}:\n{body}"


def details_to_html(title: str, rows: list[tuple[str, Any]]) -> str:
    """Render key/value rows for notebook rich display."""

    body = "".join(
        "<tr>"
        f"<th style='text-align:left'>{escape(label)}</th>"
        f"<td><code>{escape(preview(value, limit=160))}</code></td>"
        "</tr>"
        for label, value in rows
    )
    return (
        f"<div><b>{escape(title)}</b>"
        f"<table><tbody>{body}</tbody></table></div>"
    )


def table_to_text(
    title: str,
    headers: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> str:
    """Render a compact fixed-width table."""

    if not rows:
        return f"{title}: (empty)"
    normalized = [
        {header: preview(row.get(header), limit=_DEFAULT_CELL_LIMIT) for header in headers}
        for row in rows
    ]
    widths = {
        header: max(len(header), *(len(row[header]) for row in normalized))
        for header in headers
    }
    head = "  ".join(header.ljust(widths[header]) for header in headers)
    body = "\n".join(
        "  ".join(row[header].ljust(widths[header]) for header in headers)
        for row in normalized
    )
    return f"{title} ({len(rows)}):\n{head}\n{body}"


def table_to_html(
    title: str,
    headers: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> str:
    """Render a compact HTML table for notebooks."""

    head = "".join(
        f"<th style='text-align:left'>{escape(header)}</th>" for header in headers
    )
    body = "".join(
        "<tr>"
        + "".join(
            f"<td>{escape(preview(row.get(header), limit=160))}</td>"
            for header in headers
        )
        + "</tr>"
        for row in rows
    )
    return (
        f"<div><b>{escape(title)}</b> ({len(rows)})"
        f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"
    )


def plain(value: Any) -> Any:
    """Return plain containers from view objects, recursively."""

    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [plain(item) for item in value]
    return value


def short_id(value: Any, *, width: int = 12) -> str:
    text = "" if value is None else str(value)
    return text[:width] if len(text) > width else text


def _name(record: Mapping[str, Any]) -> str:
    return str(
        record.get("display_name")
        or record.get("name")
        or record.get("object")
        or record.get("slug")
        or record.get("id")
        or _EMPTY
    )


def _version(record: Mapping[str, Any]) -> str:
    current = record.get("current_version")
    if isinstance(current, Mapping):
        value = current.get("version") or current.get("number") or current.get("label")
    else:
        value = record.get("version") or record.get("version_label") or current
    return _EMPTY if value is None else str(value)


def _library_name(value: Any) -> str:
    if isinstance(value, Mapping):
        value = value.get("display_name") or value.get("slug") or value.get("name")
    return _EMPTY if value is None else str(value)


def _status(value: Any) -> str:
    return _EMPTY if value is None else str(value)


class CompactDict(dict[str, Any]):
    """A dict with compact ``repr`` and plain ``.raw`` access."""

    title = "record"

    def __init__(self, payload: Mapping[str, Any] | None = None, *, title: str | None = None):
        super().__init__(payload or {})
        self._title = title or self.title

    @property
    def raw(self) -> dict[str, Any]:
        return plain(dict(self))

    def _summary_rows(self) -> list[tuple[str, Any]]:
        return [(str(key), value) for key, value in list(self.items())[:8]]

    def __repr__(self) -> str:
        return details_to_text(self._title, self._summary_rows())

    def _repr_html_(self) -> str:
        return details_to_html(self._title, self._summary_rows())


class CompactList(list[Any]):
    """A list with compact ``repr`` and plain ``.raw`` access."""

    title = "items"
    headers: tuple[str, ...] = ("item",)

    def __init__(self, payload: list[Any] | None = None, *, title: str | None = None):
        super().__init__(payload or [])
        self._title = title or self.title

    @property
    def raw(self) -> list[Any]:
        return plain(list(self))

    def _table_rows(self) -> list[dict[str, Any]]:
        return [{"item": item} for item in self]

    def __repr__(self) -> str:
        return table_to_text(self._title, self.headers, self._table_rows())

    def _repr_html_(self) -> str:
        return table_to_html(self._title, self.headers, self._table_rows())


class HealthView(CompactDict):
    title = "health"

    def _summary_rows(self) -> list[tuple[str, Any]]:
        counts = self.get("counts") if isinstance(self.get("counts"), Mapping) else {}
        db = self.get("db") if isinstance(self.get("db"), Mapping) else {}
        server = self.get("server") if isinstance(self.get("server"), Mapping) else {}
        builds = (
            self.get("environment_builds")
            if isinstance(self.get("environment_builds"), Mapping)
            else {}
        )
        by_status = builds.get("by_status") if isinstance(builds, Mapping) else {}
        return [
            ("ok", self.get("ok")),
            ("db", f"{db.get('path', _EMPTY)} (exists={db.get('exists', _EMPTY)})"),
            ("server", "connected" if server.get("connected") else "offline"),
            (
                "counts",
                ", ".join(
                    f"{key}={value}" for key, value in sorted(counts.items())
                )
                or _EMPTY,
            ),
            (
                "env builds",
                ", ".join(
                    f"{key}={value}" for key, value in sorted((by_status or {}).items())
                )
                or _EMPTY,
            ),
        ]


class ConnectionStatusView(CompactDict):
    title = "server connection"

    def _summary_rows(self) -> list[tuple[str, Any]]:
        connection = self.get("connection")
        if not isinstance(connection, Mapping):
            connection = self.get("remote_connection")
        if not isinstance(connection, Mapping):
            connection = {}
        return [
            ("connected", self.get("connected")),
            ("server", self.get("server_url") or connection.get("server_url")),
            ("status", connection.get("status")),
            ("machine", connection.get("machine_id")),
            ("owner", connection.get("owner_id")),
            ("connected at", connection.get("connected_at")),
        ]


class MachineListView(CompactDict):
    title = "machines"
    headers = ("current", "id", "display", "status", "last_seen")

    @property
    def machines(self) -> list[dict[str, Any]]:
        machines = self.get("machines")
        if not isinstance(machines, list):
            return []
        return [item for item in machines if isinstance(item, dict)]

    def _table_rows(self) -> list[dict[str, Any]]:
        current_id = self.get("current_machine_id")
        return [
            {
                "current": "*" if item.get("id") == current_id or item.get("is_current") else "",
                "id": item.get("id") or item.get("public_id"),
                "display": item.get("display_name") or item.get("name"),
                "status": item.get("status"),
                "last_seen": item.get("last_seen_at") or item.get("updated_at"),
            }
            for item in self.machines
        ]

    def __repr__(self) -> str:
        return table_to_text(self.title, self.headers, self._table_rows())

    def _repr_html_(self) -> str:
        return table_to_html(self.title, self.headers, self._table_rows())


class LibraryListView(CompactList):
    title = "libraries"
    headers = ("slug", "display", "owner", "access", "visibility", "default_machine")

    def _table_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "slug": item.get("slug") or item.get("name") or item.get("id"),
                "display": item.get("display_name") or item.get("name"),
                "owner": item.get("owner_id"),
                "access": ",".join(item.get("access") or item.get("scopes") or []),
                "visibility": item.get("visibility"),
                "default_machine": item.get("default_machine_id"),
            }
            for item in self
            if isinstance(item, Mapping)
        ]


class EnvTableView(CompactDict):
    title = "envs"
    headers = ("name", "python", "updated")

    def _table_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "name": key,
                "python": item.get("python"),
                "updated": item.get("updated_at") or item.get("created_at"),
            }
            for key, item in sorted(self.items())
            if isinstance(item, Mapping)
        ]

    def __repr__(self) -> str:
        return table_to_text(self.title, self.headers, self._table_rows())

    def _repr_html_(self) -> str:
        return table_to_html(self.title, self.headers, self._table_rows())


class EnvironmentBuildListView(CompactList):
    title = "environment builds"
    headers = ("hash", "status", "runtime", "python", "updated")

    def _table_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "hash": short_id(item.get("spec_hash") or item.get("hash") or item.get("id")),
                "status": item.get("status"),
                "runtime": item.get("runtime") or item.get("mode") or item.get("runtime_mode"),
                "python": item.get("base_python") or item.get("python"),
                "updated": item.get("updated_at") or item.get("created_at"),
            }
            for item in self
            if isinstance(item, Mapping)
        ]


class ActionReceiptView(CompactDict):
    title = "receipt"

    def _summary_rows(self) -> list[tuple[str, Any]]:
        keys = (
            "status",
            "action",
            "name",
            "display_name",
            "slug",
            "kind",
            "version",
            "library",
            "env",
            "python",
            "removed",
            "deleted",
            "pruned",
            "count",
            "id",
        )
        rows = [(key, self.get(key)) for key in keys if key in self]
        if rows:
            return rows
        return super()._summary_rows()


class ObjectRecordView(CompactDict):
    title = "object"

    def _summary_rows(self) -> list[tuple[str, Any]]:
        return [
            ("name", _name(self)),
            ("kind", self.get("kind") or self.get("object_kind") or self.get("type")),
            ("version", _version(self)),
            ("library", _library_name(self.get("library"))),
            ("env", self.get("env")),
            ("entrypoint", self.get("entrypoint")),
            ("inputs", len(self.get("inputs") or [])),
            ("outputs", len(self.get("outputs") or [])),
            ("yaml", self.get("yaml_path") or ("included" if self.get("yaml") else None)),
        ]


class ObjectListView(CompactList):
    title = "objects"
    headers = ("name", "kind", "version", "library", "inputs")

    def __init__(self, payload: list[Any] | None = None, *, title: str | None = None):
        super().__init__(
            [
                item if isinstance(item, ObjectRecordView) else ObjectRecordView(item)
                for item in payload or []
                if isinstance(item, Mapping)
            ],
            title=title,
        )

    def _table_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "name": _name(item),
                "kind": item.get("kind") or item.get("object_kind") or item.get("type"),
                "version": _version(item),
                "library": _library_name(item.get("library")),
                "inputs": len(item.get("inputs") or []),
            }
            for item in self
            if isinstance(item, Mapping)
        ]


class SignatureView(CompactDict):
    title = "signature"

    def _summary_rows(self) -> list[tuple[str, Any]]:
        call = self.get("call") if isinstance(self.get("call"), Mapping) else {}
        return [
            ("name", _name(self)),
            ("kind", self.get("kind")),
            ("version", self.get("version")),
            ("inputs", len(self.get("inputs") or [])),
            ("outputs", len(self.get("outputs") or [])),
            ("functions", len(self.get("internal_functions") or [])),
            ("example", call.get("example")),
            ("read", call.get("read")),
        ]

    def __repr__(self) -> str:
        lines = [details_to_text(self.title, self._summary_rows())]
        inputs = InputListView(list(self.get("inputs") or []))
        outputs = OutputListView(list(self.get("outputs") or []))
        if inputs:
            lines.append(repr(inputs))
        if outputs:
            lines.append(repr(outputs))
        return "\n\n".join(lines)

    def _repr_html_(self) -> str:
        return (
            details_to_html(self.title, self._summary_rows())
            + InputListView(list(self.get("inputs") or []))._repr_html_()
            + OutputListView(list(self.get("outputs") or []))._repr_html_()
        )


class InputListView(CompactList):
    title = "inputs"
    headers = ("name", "type", "required", "default", "sources")

    def _table_rows(self) -> list[dict[str, Any]]:
        rows = []
        for item in self:
            if not isinstance(item, Mapping):
                continue
            sources = item.get("sources")
            source_count = len(sources) if isinstance(sources, list) else 0
            rows.append(
                {
                    "name": item.get("name"),
                    "type": item.get("type") or "Any",
                    "required": item.get("required"),
                    "default": item.get("default"),
                    "sources": source_count or _EMPTY,
                }
            )
        return rows


class OutputListView(CompactList):
    title = "outputs"
    headers = ("name", "selector", "type", "read")

    def _table_rows(self) -> list[dict[str, Any]]:
        rows = []
        for item in self:
            if not isinstance(item, Mapping):
                continue
            ports = item.get("ports")
            output_type = None
            if isinstance(ports, list) and ports:
                output_type = ",".join(
                    str(port.get("type") or "Any")
                    for port in ports
                    if isinstance(port, Mapping)
                )
            rows.append(
                {
                    "name": item.get("name"),
                    "selector": item.get("selector"),
                    "type": item.get("type") or output_type or "Any",
                    "read": item.get("read"),
                }
            )
        return rows


class DecompositionView(CompactDict):
    title = "decomposition"
    headers = ("node", "kind", "inputs", "outputs")

    def _node_rows(self) -> list[dict[str, Any]]:
        nodes = self.get("nodes") if isinstance(self.get("nodes"), list) else []
        return [
            {
                "node": item.get("name") or item.get("function") or item.get("node_id"),
                "kind": item.get("kind"),
                "inputs": len(item.get("inputs") or []),
                "outputs": len(item.get("outputs") or []),
            }
            for item in nodes
            if isinstance(item, Mapping)
        ]

    def __repr__(self) -> str:
        rows = [
            ("nodes", len(self.get("nodes") or [])),
            ("functions", len(self.get("functions") or [])),
            ("links", len(self.get("links") or [])),
        ]
        return (
            details_to_text(self.title, rows)
            + "\n\n"
            + table_to_text("nodes", self.headers, self._node_rows())
        )

    def _repr_html_(self) -> str:
        rows = [
            ("nodes", len(self.get("nodes") or [])),
            ("functions", len(self.get("functions") or [])),
            ("links", len(self.get("links") or [])),
        ]
        return details_to_html(self.title, rows) + table_to_html(
            "nodes",
            self.headers,
            self._node_rows(),
        )


class RunRecordView(CompactDict):
    title = "run"

    def _summary_rows(self) -> list[tuple[str, Any]]:
        result = self.get("result")
        artifact_count = 0
        if isinstance(result, Mapping):
            artifacts = result.get("artifacts")
            artifact_count = len(artifacts) if isinstance(artifacts, Mapping | list) else 0
        return [
            ("id", self.get("id")),
            ("status", self.get("status")),
            ("mode", self.get("mode") or self.get("source")),
            ("object", self.get("object") or self.get("object_name")),
            ("output", self.get("output")),
            ("created", self.get("created_at")),
            ("started", self.get("started_at")),
            ("finished", self.get("finished_at")),
            ("artifacts", artifact_count),
            ("error", self.get("error")),
        ]


class RunListView(CompactList):
    title = "runs"
    headers = ("id", "status", "mode", "object", "output", "created", "error")

    def __init__(self, payload: list[Any] | None = None, *, title: str | None = None):
        super().__init__(
            [
                item if isinstance(item, RunRecordView) else RunRecordView(item)
                for item in payload or []
                if isinstance(item, Mapping)
            ],
            title=title,
        )

    def _table_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "id": short_id(item.get("id")),
                "status": _status(item.get("status")),
                "mode": item.get("mode") or item.get("source"),
                "object": item.get("object") or item.get("object_name"),
                "output": item.get("output"),
                "created": item.get("created_at"),
                "error": item.get("error"),
            }
            for item in self
            if isinstance(item, Mapping)
        ]


class EventRecordView(CompactDict):
    title = "event"

    def _summary_rows(self) -> list[tuple[str, Any]]:
        payload = self.get("payload")
        payload_keys = ",".join(payload.keys()) if isinstance(payload, Mapping) else None
        return [
            ("time", self.get("created_at")),
            ("status", self.get("status")),
            ("message", self.get("message") or self.get("type")),
            ("run", self.get("run_id")),
            ("payload", payload_keys),
        ]


class EventListView(CompactList):
    title = "events"
    headers = ("time", "status", "message", "payload")

    def __init__(self, payload: list[Any] | None = None, *, title: str | None = None):
        super().__init__(
            [
                item if isinstance(item, EventRecordView) else EventRecordView(item)
                for item in payload or []
                if isinstance(item, Mapping)
            ],
            title=title,
        )

    def _table_rows(self) -> list[dict[str, Any]]:
        rows = []
        for item in self:
            if not isinstance(item, Mapping):
                continue
            payload = item.get("payload")
            rows.append(
                {
                    "time": item.get("created_at"),
                    "status": item.get("status"),
                    "message": item.get("message") or item.get("type"),
                    "payload": ",".join(payload.keys()) if isinstance(payload, Mapping) else _EMPTY,
                }
            )
        return rows


class ArtifactListView(CompactList):
    title = "artifacts"
    headers = ("name", "size", "format", "key")

    def _table_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "name": item.get("name") or item.get("filename"),
                "size": item.get("size") or item.get("bytes"),
                "format": item.get("format") or item.get("content_type"),
                "key": item.get("key"),
            }
            for item in self
            if isinstance(item, Mapping)
        ]


class VersionListView(CompactList):
    title = "versions"
    headers = ("version", "label", "id", "created", "env")

    def _table_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "version": _version(item),
                "label": item.get("version_label") or item.get("label"),
                "id": short_id(item.get("version_id") or item.get("id")),
                "created": item.get("created_at") or item.get("version_created_at"),
                "env": item.get("env"),
            }
            for item in self
            if isinstance(item, Mapping)
        ]


def wrap_action(payload: Mapping[str, Any], title: str) -> ActionReceiptView:
    return ActionReceiptView(payload, title=title)
