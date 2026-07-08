from itertools import chain
from types import UnionType

from spl.core.ir.parse import _attach, ir_parse


@ir_parse.register(lambda x: x is None)
def _ir_parse__none(x: None, name: str | None = None) -> _attach:
    return _attach(())


@ir_parse.register(lambda x: isinstance(x, UnionType))
def _ir_parse__union(x: UnionType, name: str | None = None) -> _attach:
    return _attach(chain.from_iterable(map(ir_parse, x.__args__)))
