import ast
import sys
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from types import FunctionType, ModuleType
from typing import Any, Generator, cast

import yaml

from spl.core.entities.distribution import get_dependencies_from_distribution
from spl.core.ir.common import DBase
from spl.core.ir.parse import _attach, ir_parse
from spl.core.ir.unparse import ir_unparse


@dataclass(frozen=True)
class DImport(DBase):
    module: str
    alias: str | None = None


yaml.add_representer(DImport, lambda dumper, data: dumper.represent_mapping("!DImport", data.__dict__))


yaml.add_constructor(
    "!DImport", lambda loader, node: DImport(**cast(dict[str, Any], loader.construct_mapping(cast(Any, node))))
)


@dataclass(frozen=True)
class DImportFrom(DBase):
    module: str
    target: str
    alias: str | None = None


yaml.add_representer(DImportFrom, lambda dumper, data: dumper.represent_mapping("!DImportFrom", data.__dict__))


yaml.add_constructor(
    "!DImportFrom", lambda loader, node: DImportFrom(**cast(dict[str, Any], loader.construct_mapping(cast(Any, node))))
)


@ir_parse.register(lambda x: isinstance(x, ModuleType))
def _ir_parse__module_import(x: ModuleType, name: str | None = None) -> _attach:
    return _attach(
        chain(
            [DImport(module=x.__name__, alias=None if x.__name__ == name else name)],
            get_dependencies_from_distribution(x),
        )
    )


@ir_parse.register(lambda x: (isinstance(x, FunctionType) or isinstance(x, type)) and (hasattr(x, "__module__")))
def _ir_parse__object_import(x: type | FunctionType, name: str | None = None) -> _attach:
    m = sys.modules[x.__module__]

    match [k for k, v in m.__dict__.items() if v == x]:
        case []:
            raise ValueError("variable {} not found in module {}".format(name, m.__name__))

        case [orig_name, *_]:
            return _attach(
                chain(
                    [DImportFrom(module=m.__name__, target=orig_name, alias=None if orig_name == name else name)],
                    get_dependencies_from_distribution(m),
                )
            )

    raise AssertionError("unreachable module import pattern")


@ir_unparse.register(lambda x: isinstance(x, DImport))
def _ir_unparse__module_import(x: DImport, source: Path) -> Generator[ast.stmt]:
    yield ast.Import(names=[ast.alias(name=x.module, asname=x.alias)])


@ir_unparse.register(lambda x: isinstance(x, DImportFrom))
def _ir_unparse__object_import(x: DImportFrom, source: Path) -> Generator[ast.stmt]:
    yield ast.ImportFrom(module=x.module, names=[ast.alias(name=x.target, asname=x.alias)], level=0)
