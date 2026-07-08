import ast
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import FunctionType
from typing import Any, Generator, cast

import yaml

from spl.core.entities.distribution import DDistribution
from spl.core.entities.function import get_function_metadata
from spl.core.ir.common import DBase
from spl.core.ir.parse import _branch, ir_parse
from spl.core.ir.unparse import ir_unparse


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


@dataclass(frozen=True)
class Adapter:
    """Versioned save/load pair for materializing values as artifacts."""

    key: str
    save: Callable[..., Any]
    load: Callable[..., Any]
    py_type: type[Any] | None
    format: str
    distributions: tuple[DDistribution, ...] = ()

    def __post_init__(self) -> None:
        _validate_function("save", self.save)
        _validate_function("load", self.load)
        _validate_py_type(self.py_type)
        _validate_key_format(key=self.key, py_type=self.py_type, format=self.format)
        object.__setattr__(self, "distributions", _normalize_distributions(self.distributions))

    def __hash__(self) -> int:
        return hash((self.key, _function_name(self.save), _function_name(self.load), self.distributions))


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
        return hash((self.key, self.save, self.load, self.distributions))


yaml.add_representer(
    DAdapter,
    lambda dumper, data: dumper.represent_mapping(
        "!DAdapter", {"key": data.key, "save": data.save, "load": data.load, "distributions": list(data.distributions)}
    ),
)


def _construct_dadapter(loader: Any, node: Any) -> DAdapter:
    return DAdapter(**loader.construct_mapping(node, deep=True))


yaml.add_constructor("!DAdapter", _construct_dadapter)


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
