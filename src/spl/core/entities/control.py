from dataclasses import dataclass
from pathlib import Path

import yaml

from spl.core.ir.common import DBase
from spl.core.ir.unparse import ir_unparse


@dataclass(frozen = True)
class DSPLSelfImport(DBase):
    name: str

yaml.add_representer(
    DSPLSelfImport,
    lambda dumper, data: dumper.represent_mapping('!DSPLSelfImport', data.__dict__))

yaml.add_constructor(
    '!DSPLSelfImport',
    lambda loader, node: DSPLSelfImport(**loader.construct_mapping(node)))


@dataclass(frozen = True)
class DSPLImport(DBase):
    path: str
    name: str

yaml.add_representer(
    DSPLImport,
    lambda dumper, data: dumper.represent_mapping('!DSPLImport', data.__dict__))

yaml.add_constructor(
    '!DSPLImport',
    lambda loader, node: DSPLImport(**loader.construct_mapping(node)))


@ir_unparse.register(lambda x: isinstance(x, DSPLSelfImport))
def _ir_unparse__spl_self_import(x: DSPLSelfImport, source: Path):
    yield from []


@ir_unparse.register(lambda x: isinstance(x, DSPLImport))
def _ir_unparse__spl_import(x: DSPLImport, source: Path):
    yield from []

