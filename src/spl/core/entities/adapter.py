import ast
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import FunctionType
from typing import Any, Generator, Protocol, cast

import yaml

from spl.core.entities.distribution import DDistribution
from spl.core.entities.function import get_function_metadata
from spl.core.ir.common import DBase
from spl.core.ir.parse import _branch, ir_parse
from spl.core.ir.unparse import ir_unparse

JSON_ADAPTER_FORMAT = "json"
JSON_ADAPTER_KEY = "spl.core.json@json"
JSON_NATIVE_TYPES = frozenset({str, int, float, bool, dict, list})


def _validate_non_empty_string(name: str, value: str) -> None:
    if not isinstance(value, str):
        raise TypeError("adapter {} must be a string".format(name))
    if not value:
        raise ValueError("adapter {} must be a non-empty string".format(name))


def _format_from_key(key: str) -> str:
    _validate_non_empty_string("key", key)
    key_head, separator, key_format = key.rpartition("@")
    if not key_head or not separator or not key_format:
        raise ValueError("adapter key must be `<python_type>@<format>`")
    return key_format


class SaveAdapter(Protocol):
    """Adapter half that writes an artifact and declares its artifact tag."""

    @property
    def key(self) -> str:
        """Return the Python-side resolution hint for this save half."""
        ...

    @property
    def save(self) -> Callable[..., Any]:
        """Return the artifact writer callable."""
        ...

    @property
    def tag(self) -> str:
        """Return the tag written into artifact references."""
        ...

    @property
    def distributions(self) -> tuple[DDistribution, ...]:
        """Return packages needed by this half."""
        ...


class LoadAdapter(Protocol):
    """Adapter half that loads artifacts with declared accepted tags."""

    @property
    def key(self) -> str:
        """Return the Python-side resolution hint for this load half."""
        ...

    @property
    def load(self) -> Callable[..., Any]:
        """Return the artifact reader callable."""
        ...

    @property
    def accepted_tags(self) -> frozenset[str]:
        """Return artifact tags accepted by this load half."""
        ...

    @property
    def legacy_key_guard(self) -> bool:
        """Return whether legacy key equality must also hold."""
        ...

    @property
    def distributions(self) -> tuple[DDistribution, ...]:
        """Return packages needed by this half."""
        ...


class RuntimeAdapter(SaveAdapter, LoadAdapter, Protocol):
    """Adapter value that can both save and load artifacts."""


def make_key(py_type: type[Any], format: str) -> str:
    """Return the stable adapter key for a Python type and storage format."""

    if not isinstance(py_type, type):
        raise TypeError("adapter python type must be a type")
    _validate_non_empty_string("format", format)
    return "{}.{}@{}".format(py_type.__module__, py_type.__qualname__, format)


def _validate_function(name: str, value: Callable[..., Any]) -> None:
    if not isinstance(value, FunctionType):
        raise TypeError("adapter {} must be a function".format(name))


def _normalize_distributions(value: Any) -> tuple[DDistribution, ...]:
    if not isinstance(value, tuple | list):
        raise TypeError("adapter distributions must be a tuple")

    distributions = tuple(value)
    if any(not isinstance(x, DDistribution) for x in distributions):
        raise TypeError("adapter distributions must contain DDistribution values")
    return tuple(sorted(distributions))


def _normalize_tags(value: Any) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, tuple | list | set | frozenset):
        raise TypeError("adapter accepted_tags must be a tuple")

    tags = tuple(value)
    if not tags:
        raise ValueError("adapter accepted_tags must not be empty")
    for tag in tags:
        _validate_non_empty_string("tag", tag)
    return tuple(sorted(set(tags)))


def _validate_py_type(value: type[Any] | None) -> None:
    if value is not None and not isinstance(value, type):
        raise TypeError("adapter py_type must be a type or None")


def _validate_key_format(key: str, py_type: type[Any] | None, format: str) -> None:
    _validate_non_empty_string("format", format)
    if _format_from_key(key) != format:
        raise ValueError("adapter key format does not match format")
    if py_type is not None and key != make_key(py_type, format):
        raise ValueError("adapter key does not match python type and format")


def _function_name(func: Callable[..., Any]) -> str:
    metadata = get_function_metadata(cast(FunctionType, func))
    return metadata.name


def _callable_identity(func: Callable[..., Any]) -> str:
    if isinstance(func, FunctionType):
        return _function_name(func)
    return "{}.{}".format(type(func).__module__, type(func).__qualname__)


def adapter_identity(adapter: RuntimeAdapter) -> dict[str, Any]:
    """Return a JSON-serializable identity for a runtime adapter."""

    return {
        "key": adapter.key,
        "tag": adapter.tag,
        "accepted_tags": sorted(adapter.accepted_tags),
        "save": _callable_identity(adapter.save),
        "load": _callable_identity(adapter.load),
        "distributions": [
            {"package": distribution.package, "version": distribution.version} for distribution in adapter.distributions
        ],
    }


def _json_save(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def _json_load(path: str) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _json_example() -> Any:
    return {"adapter": "json", "values": [1, True, "ok"]}


@dataclass(frozen=True)
class Adapter:
    """Versioned save/load pair for materializing values as artifacts."""

    key: str
    save: Callable[..., Any]
    load: Callable[..., Any]
    py_type: type[Any] | None
    format: str
    distributions: tuple[DDistribution, ...] = ()
    example: Callable[[], Any] | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        _validate_function("save", self.save)
        _validate_function("load", self.load)
        if self.example is not None and not callable(self.example):
            raise TypeError("adapter example must be callable")
        _validate_py_type(self.py_type)
        _validate_key_format(key=self.key, py_type=self.py_type, format=self.format)
        object.__setattr__(self, "distributions", _normalize_distributions(self.distributions))

    def __hash__(self) -> int:
        return hash((self.key, self.tag, _function_name(self.save), _function_name(self.load), self.distributions))

    @property
    def tag(self) -> str:
        """Return the artifact tag produced by the save half."""

        return self.format

    @property
    def accepted_tags(self) -> frozenset[str]:
        """Return artifact tags accepted by the load half."""

        return frozenset((self.format,))

    @property
    def legacy_key_guard(self) -> bool:
        """Return whether legacy adapter key equality is required."""

        return True


@dataclass(frozen=True)
class BuiltInJsonAdapter:
    """Built-in logical adapter for JSON-native values."""

    key: str = JSON_ADAPTER_KEY
    tag: str = JSON_ADAPTER_FORMAT
    distributions: tuple[DDistribution, ...] = ()

    def __post_init__(self) -> None:
        if _format_from_key(self.key) != self.tag:
            raise ValueError("json adapter key format does not match tag")
        object.__setattr__(self, "distributions", _normalize_distributions(self.distributions))

    def __hash__(self) -> int:
        return hash((self.key, self.tag, _function_name(self.save), _function_name(self.load), self.distributions))

    @property
    def save(self) -> Callable[..., Any]:
        """Return the built-in JSON writer."""

        return _json_save

    @property
    def load(self) -> Callable[..., Any]:
        """Return the built-in JSON reader."""

        return _json_load

    @property
    def example(self) -> Callable[[], Any]:
        """Return a sample JSON-native value for adapter probes."""

        return _json_example

    @property
    def accepted_tags(self) -> frozenset[str]:
        """Return artifact tags accepted by the built-in JSON reader."""

        return frozenset((self.tag,))

    @property
    def legacy_key_guard(self) -> bool:
        """Return whether legacy adapter key equality is required."""

        return False


BUILTIN_JSON_ADAPTER = BuiltInJsonAdapter()


@dataclass(frozen=True)
class DAdapter(DBase):
    """Serialized Adapter value for pipeline YAML."""

    key: str
    save: str
    load: str
    distributions: tuple[DDistribution, ...] = ()

    def __post_init__(self) -> None:
        _format_from_key(self.key)
        _validate_non_empty_string("save", self.save)
        _validate_non_empty_string("load", self.load)
        object.__setattr__(self, "distributions", _normalize_distributions(self.distributions))

    def __hash__(self) -> int:
        return hash((self.key, _format_from_key(self.key), self.save, self.load, self.distributions))


@dataclass(frozen=True)
class DSaveAdapter(DBase):
    """Serialized save half for pipeline YAML."""

    key: str
    tag: str
    save: str
    distributions: tuple[DDistribution, ...] = ()

    def __post_init__(self) -> None:
        if _format_from_key(self.key) != self.tag:
            raise ValueError("save adapter key format does not match tag")
        _validate_non_empty_string("save", self.save)
        object.__setattr__(self, "distributions", _normalize_distributions(self.distributions))

    def __hash__(self) -> int:
        return hash((self.key, self.tag, self.save, self.distributions))


@dataclass(frozen=True)
class DLoadAdapter(DBase):
    """Serialized load half for pipeline YAML."""

    key: str
    accepted_tags: tuple[str, ...]
    load: str
    distributions: tuple[DDistribution, ...] = ()

    def __post_init__(self) -> None:
        _format_from_key(self.key)
        accepted_tags = _normalize_tags(self.accepted_tags)
        _validate_non_empty_string("load", self.load)
        object.__setattr__(self, "accepted_tags", accepted_tags)
        object.__setattr__(self, "distributions", _normalize_distributions(self.distributions))

    def __hash__(self) -> int:
        return hash((self.key, self.accepted_tags, self.load, self.distributions))


yaml.add_representer(
    DAdapter,
    lambda dumper, data: dumper.represent_mapping(
        "!DAdapter", {"key": data.key, "save": data.save, "load": data.load, "distributions": list(data.distributions)}
    ),
)

yaml.add_representer(
    DSaveAdapter,
    lambda dumper, data: dumper.represent_mapping(
        "!DSaveAdapter",
        {"key": data.key, "tag": data.tag, "save": data.save, "distributions": list(data.distributions)},
    ),
)

yaml.add_representer(
    DLoadAdapter,
    lambda dumper, data: dumper.represent_mapping(
        "!DLoadAdapter",
        {
            "key": data.key,
            "accepted_tags": list(data.accepted_tags),
            "load": data.load,
            "distributions": list(data.distributions),
        },
    ),
)


def _construct_dadapter(loader: Any, node: Any) -> DAdapter:
    return DAdapter(**loader.construct_mapping(node, deep=True))


def _construct_dsave_adapter(loader: Any, node: Any) -> DSaveAdapter:
    return DSaveAdapter(**loader.construct_mapping(node, deep=True))


def _construct_dload_adapter(loader: Any, node: Any) -> DLoadAdapter:
    return DLoadAdapter(**loader.construct_mapping(node, deep=True))


yaml.add_constructor("!DAdapter", _construct_dadapter)
yaml.add_constructor("!DSaveAdapter", _construct_dsave_adapter)
yaml.add_constructor("!DLoadAdapter", _construct_dload_adapter)


@ir_parse.register(lambda x: isinstance(x, Adapter))
def _ir_parse__adapter(x: Adapter, name: str | None = None) -> _branch:
    def mk_dependencies(frame_offset: int) -> Generator[Any]:
        yield ir_parse(x.save)
        yield ir_parse(x.load)

    return _branch(
        x,
        lambda: DAdapter(
            key=x.key, save=_function_name(x.save), load=_function_name(x.load), distributions=x.distributions
        ),
        mk_dependencies,
    )


def _unparse_distributions(distributions: tuple[DDistribution, ...]) -> ast.Tuple:
    return ast.Tuple(
        elts=[
            ast.Call(
                func=ast.Name(id="DDistribution", ctx=ast.Load()),
                keywords=[
                    ast.keyword(arg="package", value=ast.Constant(value=x.package)),
                    ast.keyword(arg="version", value=ast.Constant(value=x.version)),
                ],
            )
            for x in distributions
        ],
        ctx=ast.Load(),
    )


@ir_unparse.register(lambda x: isinstance(x, DAdapter))
def _ir_unparse__adapter(x: DAdapter, source: Path) -> Generator[ast.stmt]:
    yield ast.Assign(
        targets=[ast.Name(id="_adapter", ctx=ast.Store())],
        value=ast.Call(
            func=ast.Name(id="Adapter", ctx=ast.Load()),
            keywords=[
                ast.keyword(arg="key", value=ast.Constant(value=x.key)),
                ast.keyword(arg="save", value=ast.Name(id=x.save, ctx=ast.Load())),
                ast.keyword(arg="load", value=ast.Name(id=x.load, ctx=ast.Load())),
                ast.keyword(arg="py_type", value=ast.Constant(value=None)),
                ast.keyword(arg="format", value=ast.Constant(value=_format_from_key(x.key))),
                ast.keyword(arg="distributions", value=_unparse_distributions(x.distributions)),
            ],
        ),
    )
