"""Public Run-bound value and file adapters.

The adapters in this module describe transport for one Run invocation.  They
do not become part of an immutable Object version and they are deliberately
lazy: optional libraries are imported only inside adapter save/load functions
running in the caller or isolated Run worker.
"""

from __future__ import annotations

import hashlib
import json as _stdlib_json
import os
import re
import stat
from dataclasses import dataclass
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any, Final, cast

from spl.core.entities.adapter import Adapter, BUILTIN_JSON_ADAPTER, RuntimeAdapter
from spl.core.entities.distribution import DDistribution

JSON: Final = "json"
OPAQUE_FILE: Final = "opaque-file"
TEXT_FILE_UTF8: Final = "text-file-utf8"
BINARY_FILE: Final = "binary-file"
DATAFRAME_JSON_SPLIT: Final = "dataframe-json-split"
DATAFRAME_CSV_SEMICOLON: Final = "dataframe-csv-semicolon"
DATAFRAME_XLSX: Final = "dataframe-xlsx"
PNG_PILLOW: Final = "png-pillow"

BUILTIN_ADAPTER_IDS: Final[frozenset[str]] = frozenset(
    {
        JSON,
        OPAQUE_FILE,
        TEXT_FILE_UTF8,
        BINARY_FILE,
        DATAFRAME_JSON_SPLIT,
        DATAFRAME_CSV_SEMICOLON,
        DATAFRAME_XLSX,
        PNG_PILLOW,
    }
)

MAX_FILE_INPUT_BYTES: Final = 256 * 1024 * 1024
_HASH_CHUNK_BYTES: Final = 1024 * 1024
_SAFE_LOGICAL_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
_SAFE_MEDIA_TYPE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}$")


class AdapterDependencyError(RuntimeError):
    """A lazily resolved built-in adapter dependency is unavailable."""


def _is_reparse_point(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0))
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _is_regular(value: os.stat_result) -> bool:
    return stat.S_ISREG(value.st_mode) and not _is_reparse_point(value)


def _same_file(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _safe_logical_name(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("file input name must be a string")
    if not _SAFE_LOGICAL_NAME.fullmatch(value) or value in {".", ".."}:
        raise ValueError("file input name must be a safe single filename")
    return value


def _safe_media_type(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("file input media_type must be a string or None")
    if not _SAFE_MEDIA_TYPE.fullmatch(value):
        raise ValueError("file input media_type must be a valid type/subtype token")
    return value.casefold()


def _read_regular_file(path: Path, *, max_bytes: int = MAX_FILE_INPUT_BYTES) -> tuple[bytes, os.stat_result]:
    """Read one identity-pinned regular file without following its final link."""

    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    try:
        before = path.lstat()
    except OSError as exc:
        raise ValueError("file input must name one existing regular file") from exc
    if not _is_regular(before):
        raise ValueError("file input must be a regular file and must not be a symlink")

    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        if not _is_regular(opened) or not _same_file(before, opened):
            raise ValueError("file input changed while it was being opened")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(_HASH_CHUNK_BYTES, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > max_bytes or opened.st_size > max_bytes:
            raise ValueError(f"file input exceeds the {max_bytes}-byte staging limit")
        after_open = os.fstat(descriptor)
        after_path = path.lstat()
        if (
            not _is_regular(after_open)
            or not _is_regular(after_path)
            or not _same_file(opened, after_open)
            or not _same_file(opened, after_path)
            or after_open.st_size != len(data)
        ):
            raise ValueError("file input changed while it was being read")
        return data, opened
    except OSError as exc:
        raise ValueError("file input could not be opened safely") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


@dataclass(frozen=True, init=False)
class FileInput:
    """Client-side instruction to stage one opaque regular file.

    The source path is never serialized.  Construction binds the source file's
    current identity, size, and digest; staging re-reads and compares them so a
    replacement race cannot silently change the admitted bytes.
    """

    _source: Path
    name: str
    media_type: str | None
    size: int
    sha256: str
    _device: int
    _inode: int

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        media_type: str | None = None,
        name: str | None = None,
    ) -> None:
        if not isinstance(path, str | os.PathLike):
            raise TypeError("file input path must be a string or path-like value")
        source = Path(path).expanduser().absolute()
        logical_name = _safe_logical_name(name if name is not None else source.name)
        data, identity = _read_regular_file(source)
        object.__setattr__(self, "_source", source)
        object.__setattr__(self, "name", logical_name)
        object.__setattr__(self, "media_type", _safe_media_type(media_type))
        object.__setattr__(self, "size", len(data))
        object.__setattr__(self, "sha256", hashlib.sha256(data).hexdigest())
        object.__setattr__(self, "_device", int(identity.st_dev))
        object.__setattr__(self, "_inode", int(identity.st_ino))

    def staged_bytes(self, *, max_bytes: int = MAX_FILE_INPUT_BYTES) -> bytes:
        """Return revalidated bytes for private client staging."""

        data, identity = _read_regular_file(self._source, max_bytes=max_bytes)
        digest = hashlib.sha256(data).hexdigest()
        if (
            int(identity.st_dev) != self._device
            or int(identity.st_ino) != self._inode
            or len(data) != self.size
            or digest != self.sha256
        ):
            raise ValueError("file input changed after FileInput was constructed")
        return data

    def __repr__(self) -> str:
        return "FileInput(name={!r}, media_type={!r}, size={}, sha256={!r})".format(
            self.name,
            self.media_type,
            self.size,
            self.sha256,
        )


@dataclass(frozen=True)
class ArtifactHandle:
    """Safe local representation of one retained adapter-backed artifact."""

    name: str
    size: int
    sha256: str
    adapter_id: str
    format_tag: str | None
    path: Path | None = None
    media_type: str | None = None
    recovery: str | None = None

    def __post_init__(self) -> None:
        _safe_logical_name(self.name)
        if type(self.size) is not int or self.size < 0:
            raise ValueError("artifact handle size must be a non-negative integer")
        if not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
            raise ValueError("artifact handle sha256 must be lowercase SHA-256 hex")
        if self.adapter_id not in BUILTIN_ADAPTER_IDS and not self.adapter_id.startswith(("custom:", "library:")):
            raise ValueError("artifact handle adapter_id is not recognized")
        _safe_media_type(self.media_type)


def _copy_opaque_save(path: str, value: Any) -> None:
    source = Path(value)
    data, _ = _read_regular_file(source)
    Path(path).write_bytes(data)


def _opaque_load(path: str) -> Path:
    source = Path(path)
    identity = source.lstat()
    if not _is_regular(identity):
        raise ValueError("opaque artifact is not a regular file")
    return source


def _text_save(path: str, value: Any) -> None:
    if type(value) is not str:
        raise TypeError("text-file-utf8 adapter requires an exact str value")
    Path(path).write_text(value, encoding="utf-8", newline="")


def _text_load(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _binary_save(path: str, value: Any) -> None:
    if type(value) is not bytes:
        raise TypeError("binary-file adapter requires an exact bytes value")
    Path(path).write_bytes(value)


def _binary_load(path: str) -> bytes:
    return Path(path).read_bytes()


def _dataframe_json_split_save(path: str, value: Any) -> None:
    text = value.to_json(orient="split", date_format="iso")
    Path(path).write_text(cast(str, text), encoding="utf-8", newline="")


def _dataframe_json_split_load(path: str) -> Any:
    import pandas as pd  # type: ignore[import-untyped]  # Optional runtime dependency.

    document = _stdlib_json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict) or set(document) != {"columns", "index", "data"}:
        raise ValueError("DataFrame JSON-split artifact has an invalid closed shape")
    return pd.DataFrame(data=document["data"], columns=document["columns"], index=document["index"])


def _dataframe_csv_semicolon_save(path: str, value: Any) -> None:
    value.to_csv(path, sep=";", encoding="utf-8", index=False)


def _dataframe_csv_semicolon_load(path: str) -> Any:
    import pandas as pd

    return pd.read_csv(path, sep=";", encoding="utf-8")


def _dataframe_xlsx_save(path: str, value: Any) -> None:
    value.to_excel(path, engine="openpyxl", sheet_name="data", index=False)


def _dataframe_xlsx_load(path: str) -> Any:
    import pandas as pd

    return pd.read_excel(path, engine="openpyxl", sheet_name="data")


def _png_pillow_save(path: str, value: Any) -> None:
    value.save(path, format="PNG")


def _png_pillow_load(path: str) -> Any:
    from PIL import Image  # type: ignore[import-not-found]  # Optional runtime dependency.

    with Image.open(path) as source:
        if source.format != "PNG":
            raise ValueError("png-pillow adapter requires PNG content")
        source.load()
        return source.copy()


_STATIC_BUILTINS: Final[dict[str, RuntimeAdapter]] = {
    JSON: BUILTIN_JSON_ADAPTER,
    OPAQUE_FILE: Adapter(
        key="pathlib.Path@spl.file.opaque.v1",
        save=_copy_opaque_save,
        load=_opaque_load,
        # ``Path`` is an implementation-selected concrete class whose module
        # changed in Python 3.13.  The stable semantic key is the authority.
        py_type=None,
        format="spl.file.opaque.v1",
    ),
    TEXT_FILE_UTF8: Adapter(
        key="builtins.str@spl.text.utf8.v1",
        save=_text_save,
        load=_text_load,
        py_type=str,
        format="spl.text.utf8.v1",
    ),
    BINARY_FILE: Adapter(
        key="builtins.bytes@spl.binary.raw.v1",
        save=_binary_save,
        load=_binary_load,
        py_type=bytes,
        format="spl.binary.raw.v1",
    ),
}


_BUILTIN_PRESENTATION: Final[dict[str, dict[str, str | None]]] = {
    JSON: {"preferred_extension": None, "media_type": "application/json"},
    OPAQUE_FILE: {"preferred_extension": None, "media_type": None},
    TEXT_FILE_UTF8: {"preferred_extension": ".txt", "media_type": "text/plain"},
    BINARY_FILE: {"preferred_extension": ".bin", "media_type": "application/octet-stream"},
    DATAFRAME_JSON_SPLIT: {"preferred_extension": ".json", "media_type": "application/json"},
    DATAFRAME_CSV_SEMICOLON: {"preferred_extension": ".csv", "media_type": "text/csv"},
    DATAFRAME_XLSX: {
        "preferred_extension": ".xlsx",
        "media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    },
    PNG_PILLOW: {"preferred_extension": ".png", "media_type": "image/png"},
}

_BUILTIN_LABELS: Final[dict[str, str]] = {
    JSON: "JSON",
    OPAQUE_FILE: "Opaque file",
    TEXT_FILE_UTF8: "Text file / UTF-8",
    BINARY_FILE: "Binary file",
    DATAFRAME_JSON_SPLIT: "DataFrame / JSON split",
    DATAFRAME_CSV_SEMICOLON: "DataFrame / CSV (semicolon)",
    DATAFRAME_XLSX: "DataFrame / XLSX",
    PNG_PILLOW: "Pillow image / PNG",
}

_BUILTIN_SEMANTIC_CATEGORIES: Final[dict[str, tuple[str, ...]]] = {
    JSON: ("strict_json",),
    OPAQUE_FILE: ("opaque_file",),
    TEXT_FILE_UTF8: ("text",),
    BINARY_FILE: ("binary",),
    DATAFRAME_JSON_SPLIT: ("table",),
    DATAFRAME_CSV_SEMICOLON: ("table",),
    DATAFRAME_XLSX: ("table",),
    PNG_PILLOW: ("image",),
}

_BUILTIN_REQUIRED_PACKAGES: Final[dict[str, tuple[str, ...]]] = {
    DATAFRAME_JSON_SPLIT: ("pandas",),
    DATAFRAME_CSV_SEMICOLON: ("pandas",),
    DATAFRAME_XLSX: ("pandas", "openpyxl"),
    PNG_PILLOW: ("Pillow",),
}

_SYSTEM_DEFAULT_VALUE_TYPES: Final[dict[str, frozenset[str]]] = {
    DATAFRAME_JSON_SPLIT: frozenset({"pandas.DataFrame", "pandas.core.frame.DataFrame"}),
}

_SYSTEM_DEFAULT_SEMANTIC_TYPES: Final[dict[str, frozenset[str]]] = {
    DATAFRAME_JSON_SPLIT: frozenset({"DataFrame", "pd.DataFrame", "pandas.DataFrame", "pandas.core.frame.DataFrame"}),
}


def _distribution(package: str) -> DDistribution | None:
    try:
        version = importlib_metadata.version(package)
    except importlib_metadata.PackageNotFoundError as exc:
        # FileInput intentionally lets a caller stage typed bytes without the
        # optional decoding library.  The daemon later binds the exact worker
        # version from the Object/Run environment during preflight.
        del exc
        return None
    return DDistribution(package=package, version=version)


def _available_distributions(*packages: str) -> tuple[DDistribution, ...]:
    return tuple(distribution for package in packages if (distribution := _distribution(package)) is not None)


def _optional_builtin(adapter_id: str) -> RuntimeAdapter:
    if adapter_id == DATAFRAME_JSON_SPLIT:
        distributions = _available_distributions("pandas")
        return Adapter(
            key="pandas.core.frame.DataFrame@spl.table.dataframe-json-split.v1",
            save=_dataframe_json_split_save,
            load=_dataframe_json_split_load,
            py_type=None,
            format="spl.table.dataframe-json-split.v1",
            distributions=distributions,
        )
    if adapter_id == DATAFRAME_CSV_SEMICOLON:
        distributions = _available_distributions("pandas")
        return Adapter(
            key="pandas.core.frame.DataFrame@spl.table.csv-semicolon-utf8-no-index.v1",
            save=_dataframe_csv_semicolon_save,
            load=_dataframe_csv_semicolon_load,
            py_type=None,
            format="spl.table.csv-semicolon-utf8-no-index.v1",
            distributions=distributions,
        )
    if adapter_id == DATAFRAME_XLSX:
        distributions = _available_distributions("pandas", "openpyxl")
        return Adapter(
            key="pandas.core.frame.DataFrame@spl.table.xlsx-openpyxl-sheet-data-no-index.v1",
            save=_dataframe_xlsx_save,
            load=_dataframe_xlsx_load,
            py_type=None,
            format="spl.table.xlsx-openpyxl-sheet-data-no-index.v1",
            distributions=distributions,
        )
    if adapter_id == PNG_PILLOW:
        distributions = _available_distributions("Pillow")
        return Adapter(
            key="PIL.Image.Image@spl.image.png-pillow.v1",
            save=_png_pillow_save,
            load=_png_pillow_load,
            py_type=None,
            format="spl.image.png-pillow.v1",
            distributions=distributions,
        )
    raise KeyError(adapter_id)


def get_builtin_adapter(adapter_id: str) -> RuntimeAdapter:
    """Resolve one stable built-in adapter ID without eager optional imports."""

    if not isinstance(adapter_id, str):
        raise TypeError("adapter ID must be a string")
    try:
        return _STATIC_BUILTINS[adapter_id]
    except KeyError:
        if adapter_id not in BUILTIN_ADAPTER_IDS:
            choices = ", ".join(sorted(BUILTIN_ADAPTER_IDS))
            raise ValueError(f"unknown built-in adapter {adapter_id!r}; expected one of: {choices}") from None
        return _optional_builtin(adapter_id)


def builtin_id_for_adapter(adapter: RuntimeAdapter) -> str | None:
    """Return the stable ID for a built-in-equivalent adapter, if any.

    Built-ins are matched by their stable key and format identity instead of
    object identity because the optional factories intentionally return fresh
    ``Adapter`` values and callers may retain them across SDK operations.
    """

    for adapter_id in sorted(BUILTIN_ADAPTER_IDS):
        candidate = get_builtin_adapter(adapter_id)
        if (
            adapter.key == candidate.key
            and adapter.tag == candidate.tag
            and frozenset(adapter.accepted_tags) == frozenset(candidate.accepted_tags)
            and adapter.save is candidate.save
            and adapter.load is candidate.load
            and tuple(adapter.distributions) == tuple(candidate.distributions)
        ):
            return adapter_id
    return None


def builtin_presentation(adapter_id: str) -> dict[str, str | None]:
    """Return safe presentation metadata for a stable built-in ID."""

    if adapter_id not in BUILTIN_ADAPTER_IDS:
        raise ValueError(f"unknown built-in adapter {adapter_id!r}")
    return dict(_BUILTIN_PRESENTATION[adapter_id])


def builtin_required_packages(adapter_id: str) -> tuple[str, ...]:
    """Return optional distribution names declared by the built-in registry."""

    if adapter_id not in BUILTIN_ADAPTER_IDS:
        raise ValueError(f"unknown built-in adapter {adapter_id!r}")
    return _BUILTIN_REQUIRED_PACKAGES.get(adapter_id, ())


def builtin_registry_metadata(adapter_id: str) -> dict[str, Any]:
    """Return immutable browser-safe registry facts for one built-in.

    Environment availability deliberately does not belong here.  A daemon can
    project those facts for its own control environment without turning that
    observation into a claim about a selected Run or remote target.
    """

    if adapter_id not in BUILTIN_ADAPTER_IDS:
        raise ValueError(f"unknown built-in adapter {adapter_id!r}")
    return {
        "label": _BUILTIN_LABELS[adapter_id],
        "directions": ("input", "output"),
        "transport": "inline_json" if adapter_id == JSON else "artifact",
        "semantic_categories": _BUILTIN_SEMANTIC_CATEGORIES[adapter_id],
        "system_default": {
            "fallback": adapter_id == JSON,
            "semantic_types": tuple(sorted(_SYSTEM_DEFAULT_SEMANTIC_TYPES.get(adapter_id, ()))),
            "value_types": tuple(sorted(_SYSTEM_DEFAULT_VALUE_TYPES.get(adapter_id, ()))),
        },
    }


def canonical_builtin_semantic_type(value: str) -> str:
    """Canonicalize only stable aliases owned by the built-in registry."""

    aliases = {
        "pd.DataFrame": "pandas.core.frame.DataFrame",
        "pandas.DataFrame": "pandas.core.frame.DataFrame",
        "pl.DataFrame": "polars.dataframe.frame.DataFrame",
        "polars.DataFrame": "polars.dataframe.frame.DataFrame",
        "Image.Image": "PIL.Image.Image",
        "PIL.Image": "PIL.Image.Image",
    }
    return aliases.get(value, value)


def builtin_advisory_semantic_category(adapter_id: str) -> str | None:
    """Return the single stable advisory category for a built-in ID."""

    categories = _BUILTIN_SEMANTIC_CATEGORIES.get(adapter_id)
    return categories[0] if categories is not None and len(categories) == 1 else None


def system_default_adapter_for_value(value: Any) -> str | None:
    """Resolve a registry-declared semantic value default without importing it."""

    typ = type(value)
    identity = f"{typ.__module__}.{typ.__qualname__}"
    for adapter_id, identities in _SYSTEM_DEFAULT_VALUE_TYPES.items():
        if identity in identities:
            return adapter_id
    return None


def system_default_adapter_for_semantic_type(value: str | None) -> str | None:
    """Resolve a registry-declared semantic annotation default."""

    if value is None:
        return None
    normalized = re.sub(r"\s+", "", str(value).strip().strip("'\""))
    for adapter_id, aliases in _SYSTEM_DEFAULT_SEMANTIC_TYPES.items():
        if normalized in aliases:
            return adapter_id
    return None


def dataframe_json_split_adapter() -> RuntimeAdapter:
    """Return the lazy pandas JSON-split adapter."""

    return get_builtin_adapter(DATAFRAME_JSON_SPLIT)


def dataframe_csv_semicolon_adapter() -> RuntimeAdapter:
    """Return the lazy semicolon/UTF-8/no-index pandas CSV adapter."""

    return get_builtin_adapter(DATAFRAME_CSV_SEMICOLON)


def dataframe_xlsx_adapter() -> RuntimeAdapter:
    """Return the lazy openpyxl/data/no-index pandas XLSX adapter."""

    return get_builtin_adapter(DATAFRAME_XLSX)


def png_pillow_adapter() -> RuntimeAdapter:
    """Return the lazy Pillow PNG adapter when Pillow is installed."""

    return get_builtin_adapter(PNG_PILLOW)


__all__ = [
    "Adapter",
    "AdapterDependencyError",
    "ArtifactHandle",
    "BINARY_FILE",
    "BUILTIN_ADAPTER_IDS",
    "DATAFRAME_CSV_SEMICOLON",
    "DATAFRAME_JSON_SPLIT",
    "DATAFRAME_XLSX",
    "DDistribution",
    "FileInput",
    "JSON",
    "OPAQUE_FILE",
    "PNG_PILLOW",
    "RuntimeAdapter",
    "TEXT_FILE_UTF8",
    "builtin_id_for_adapter",
    "builtin_advisory_semantic_category",
    "canonical_builtin_semantic_type",
    "builtin_presentation",
    "builtin_registry_metadata",
    "builtin_required_packages",
    "dataframe_csv_semicolon_adapter",
    "dataframe_json_split_adapter",
    "dataframe_xlsx_adapter",
    "get_builtin_adapter",
    "png_pillow_adapter",
    "system_default_adapter_for_semantic_type",
    "system_default_adapter_for_value",
]
