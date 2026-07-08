import ast
from functools import partial
from itertools import chain
from pathlib import Path
from typing import cast

from spl.core.ir.common import DBase, NamedDBase, mk_dispatcher

IIFE_NAME = "_"

# ir_unparse :: (x: Any, source: Path) -> Generator[ast]
ir_unparse = mk_dispatcher()


def mk_top_level_ast(d: tuple[DBase, list[DBase]], source: Path) -> ast.Module:
    (root, dependencies) = d
    name = cast(NamedDBase, root).name

    return ast.fix_missing_locations(
        ast.Module(
            [
                ast.FunctionDef(
                    name=IIFE_NAME,
                    args=ast.arguments(posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]),
                    body=[
                        *chain.from_iterable(map(partial(ir_unparse, source=source), dependencies)),
                        *ir_unparse(root, source),
                        ast.Return(value=ast.Name(id=name, ctx=ast.Load())),
                    ],
                    decorator_list=[],
                ),
                ast.Assign(
                    targets=[ast.Name(id=name, ctx=ast.Store())],
                    value=ast.Call(func=ast.Name(id=IIFE_NAME, ctx=ast.Load())),
                ),
            ]
        )
    )
