from dataclasses import dataclass
from collections.abc import Iterable
from typing import Any, Callable

from spl.core.ir.common import DBase, mk_dispatcher

# ir_parse :: (x: Any, name: str | None = None, dependencies: bool = False)
ir_parse = mk_dispatcher()


@dataclass(frozen=True)
class _branch:  # noqa: N801
    x: Any
    mk_root: Callable[[], DBase]
    mk_dependencies: Callable[[int], Iterable[Any]] = lambda _: ()


@dataclass(frozen=True)
class _attach:  # noqa: N801
    dependencies: Iterable[Any]


@dataclass(frozen=True)
class _set_cursor:  # noqa: N801
    cursor: Any | None


def stack_push(stack: list[Any], *vs: Any) -> list[Any]:
    return [*stack, *vs]


def stack_pop(stack: list[Any]) -> tuple[list[Any], Any]:
    match stack:
        case []:
            raise StopIteration()

        case [*new_stack, v]:
            return (new_stack, v)

    raise AssertionError("unreachable stack pattern")


def get_top_level_deps(frame_offset: int, xs: list[Any]) -> list[tuple[DBase, list[DBase]]]:
    refs_root: dict[Any, DBase] = {}
    refs_dependencies: dict[Any, list[Any]] = {}

    cursor: Any | None = None

    stack: list[Any] = []
    for x in xs:
        stack = stack_push(stack, ir_parse(x))

    while len(stack):
        (stack, x) = stack_pop(stack)
        match x:
            case _set_cursor(new_cursor):
                cursor = new_cursor

            case _branch(x, mk_root, mk_dependencies):
                if (x in refs_root) and (cursor is not None):
                    refs_dependencies[cursor] = [*refs_dependencies[cursor], refs_root[x]]
                else:
                    root = mk_root()
                    dependencies = mk_dependencies(frame_offset)

                    refs_root[x] = root
                    refs_dependencies[x] = []
                    stack = stack_push(stack, root, _set_cursor(cursor), _attach(dependencies))
                    cursor = x

            case _attach(dependencies):
                stack = stack_push(stack, *dependencies)

            case _:
                if cursor is not None:
                    refs_dependencies[cursor] = [*refs_dependencies[cursor], x]

    # python 3.7+ maintains order of insertions, we rely on it
    return list(zip(refs_root.values(), refs_dependencies.values(), strict=True))
