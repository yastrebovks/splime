"""Inline first-party (local) helper functions into the serialized entity.

When a wrapped function depends on other functions that the user defined in
their own local files -- plain modules on disk that are **not** installed via
pip and are **not** part of the standard library -- a bare ``from my_module
import helper`` cannot be reproduced on another machine or inside the daemon:
the local module simply is not importable there.

This module teaches :data:`spl.core.ir.parse.ir_parse` to treat such helpers
exactly like ``__main__`` functions: it inlines their source as a
:class:`~spl.core.entities.function.DFunction` and recurses into *their* own
dependencies, following the import graph across as many local files as needed
(``top`` -> ``helpers`` -> ``deeper`` -> ...).  Because every local helper is
inlined, no ``from local_module import ...`` statement is ever emitted into the
serialized output -- the local import is excluded from the "include" section by
construction.

Third-party (pip) objects and the standard library keep the existing behaviour:
they are referenced through imports and recorded as distributions, never
inlined.

Dispatch priority
-----------------
The handler registered here intentionally takes precedence over the generic
``from module import name`` handler in :mod:`spl.core.entities.module`.  Priority
is established purely through import order in ``spl/core/__init__.py`` (this
module is imported before ``spl.core.entities.module``), so no existing file
needs its dispatch predicate changed.  ``is_inlinable_local_function`` is also
written to be strictly narrower than the generic predicate and to never raise,
so even if the order were lost the worst case is the previous behaviour, not a
crash.
"""

from __future__ import annotations

import ast
import builtins
import inspect
import sys
import sysconfig
import textwrap
from dataclasses import replace
from functools import lru_cache
from itertools import chain
from pathlib import Path
from types import FunctionType
from typing import Any, Generator, cast

from spl.core.entities.control import DSPLImport
from spl.core.entities.function import (
    DFunction,
    LOCATION_DUNDER_NAME,
    get_dependency_names_from_bytecode,
    get_static_import_dependencies,
    serialize_function,
)
from spl.core.entities.node import InputPort
from spl.core.ir.parse import _attach, _branch, ir_parse

# --------------------------------------------------------------------------- #
# Locality detection: is a module first-party user code, or third-party?
# --------------------------------------------------------------------------- #


@lru_cache(maxsize=None)
def _environment_roots() -> tuple[Path, ...]:
    """Filesystem roots that mark an interpreter-managed / third-party module."""
    roots: set[Path] = set()
    for key in ("stdlib", "platstdlib", "purelib", "platlib"):
        value = sysconfig.get_paths().get(key)
        if value:
            roots.add(Path(value))
    for value in (sys.prefix, sys.base_prefix, sys.exec_prefix, sys.base_exec_prefix):
        if value:
            roots.add(Path(value))

    resolved: set[Path] = set()
    for root in roots:
        try:
            resolved.add(root.resolve())
        except OSError:
            continue
    return tuple(resolved)


def _is_within(path: Path, roots: tuple[Path, ...]) -> bool:
    try:
        resolved = path.resolve()
    except OSError:
        return False
    for root in roots:
        try:
            resolved.relative_to(root)
            return True
        except ValueError:
            continue
    return False


@lru_cache(maxsize=None)
def _module_is_local(module_name: str) -> bool:
    """Return ``True`` for first-party modules that we should inline.

    The decision is made by *file location*, not by whether the module belongs
    to an installed distribution.  This is deliberate.  A package that is being
    actively developed is frequently installed in editable mode
    (``pip install -e .``), which registers it as a distribution even though its
    source still lives in the working tree and is not published anywhere.  Such
    a package must be inlined ("gutted") exactly like loose files in a folder --
    referencing it as a pip dependency would produce an artifact that cannot be
    reconstructed on another machine.

    So both first-party shapes are treated identically:

    * loose modules in a directory on ``sys.path``;
    * an unpublished package under development (incl. an editable install).

    Only genuinely environment-installed code -- the standard library and
    packages that live under ``site-packages`` / the interpreter prefix -- is
    left to the import handlers so it can be referenced + pinned as a
    ``DDistribution``.
    """
    if not module_name or module_name == "__main__":
        return False

    top_level = module_name.split(".")[0]
    if top_level in sys.stdlib_module_names or top_level in sys.builtin_module_names:
        return False

    module = sys.modules.get(module_name)
    file = getattr(module, "__file__", None) if module is not None else None
    if not file:
        # No importable source on disk (builtin / namespace / C-extension):
        # we could not inline it anyway, so leave it to the import handlers.
        return False

    path = Path(file)
    if "site-packages" in path.parts or "dist-packages" in path.parts:
        return False
    if _is_within(path, _environment_roots()):
        return False
    return True


# --------------------------------------------------------------------------- #
# Source parsing + dependency extraction.
#
# Dependencies are resolved against ``func.__globals__`` -- the namespace of the
# file that defines the function -- never the interpreter call stack.  That is
# precisely what lets the recursion cross file boundaries: each helper resolves
# the names *it* references in *its own* module.
# --------------------------------------------------------------------------- #


@lru_cache(maxsize=None)
def _function_def(func: FunctionType) -> ast.FunctionDef | None:
    """Parse a single ``def`` from a function's source, or ``None`` if unusable."""
    try:
        source = textwrap.dedent(inspect.getsource(func))
    except (OSError, TypeError):
        return None
    try:
        module = ast.parse(source)
    except SyntaxError:
        return None
    body = module.body
    if len(body) == 1 and isinstance(body[0], ast.FunctionDef):
        return body[0]
    return None


def _class_def(cls: type[Any]) -> ast.ClassDef | None:
    """Parse one source-backed local class, or return ``None`` if unsafe."""

    try:
        source = textwrap.dedent(inspect.getsource(cls))
    except (OSError, TypeError):
        return None
    try:
        module = ast.parse(source)
    except SyntaxError:
        return None
    body = module.body
    if len(body) == 1 and isinstance(body[0], ast.ClassDef):
        return body[0]
    return None


def is_inlinable_local_function(x: Any) -> bool:
    """Dispatch predicate: a pure-Python function from a first-party local file.

    Evaluated by the dispatcher against *every* object, so it must be cheap for
    the common (non-function) case and must never raise.
    """
    try:
        if not isinstance(x, FunctionType):
            return False
        module_name = getattr(x, "__module__", None)
        if module_name in (None, "__main__"):
            return False
        if not _module_is_local(module_name):
            return False
        return _function_def(x) is not None
    except Exception:
        return False


def is_inlinable_local_class(x: Any) -> bool:
    """Return whether a local Python class can be captured from exact source."""

    try:
        if not isinstance(x, type):
            return False
        module_name = getattr(x, "__module__", None)
        if module_name in (None, "__main__"):
            return False
        if not _module_is_local(module_name):
            return False
        return _class_def(x) is not None
    except Exception:
        return False


def _annotation_sources(tree: ast.FunctionDef) -> Generator[str]:
    """Source text of argument annotations and defaults (parity with __main__)."""
    for argument in tree.args.args:
        if argument.annotation is not None:
            yield ast.unparse(argument.annotation)
    for default in tree.args.defaults:
        yield ast.unparse(default)


def _referenced_names(func: FunctionType, tree: ast.FunctionDef) -> list[str]:
    names: set[str] = set(get_dependency_names_from_bytecode(func))
    for source in _annotation_sources(tree):
        names.update(get_dependency_names_from_bytecode(source))
    return sorted(name for name in names if name not in vars(builtins))


def _local_dependencies(func: FunctionType, tree: ast.FunctionDef) -> Generator[Any]:
    """Yield IR for every name the function reads, resolved in its own module."""
    namespace = func.__globals__
    names = _referenced_names(func, tree)

    missing = [name for name in names if name not in namespace]
    if missing:
        raise ValueError(
            "cannot inline local function {!r}: undefined names {}".format(func.__qualname__, ", ".join(missing))
        )

    for name in names:
        # Aliased imports are rebound by the handler below (which sees both the
        # bound name and the real def name), so this stays a plain dispatch.
        yield ir_parse(namespace[name], name)
    yield from get_static_import_dependencies(tree)


def _local_class_dependencies(cls: type[Any], tree: ast.ClassDef) -> Generator[Any]:
    """Yield external names required to reconstruct one captured local class."""

    module = sys.modules.get(cls.__module__)
    if module is None:
        raise ValueError("cannot capture local class from an unloaded module")
    namespace = vars(module)
    compiler_names = {"__annotations__", "__name__"}
    names = sorted(
        name
        for name in set(get_dependency_names_from_bytecode(ast.unparse(tree)))
        if name not in vars(builtins) and name not in compiler_names
    )
    missing = [name for name in names if name not in namespace]
    if missing:
        raise ValueError(
            "cannot capture local class {!r}: undefined names {}".format(
                cls.__qualname__,
                ", ".join(missing),
            )
        )
    for name in names:
        yield ir_parse(namespace[name], name)


def _serialize_local_class_factory(
    cls: type[Any],
    tree: ast.ClassDef,
    exported_name: str,
) -> DFunction:
    """Encode a constructible local class through the released ``DFunction`` tag.

    The 0.4.x YAML vocabulary has no class-definition element.  For the
    historical Adapter closure, callers only construct values; a source-backed
    factory preserves that behavior without adding a tag older readers reject.
    Unsupported class call signatures fail closed instead of widening the IR.
    """

    signature = inspect.signature(cls)
    inputs: list[InputPort] = []
    call_arguments: list[ast.expr] = []
    for parameter in signature.parameters.values():
        if parameter.kind not in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }:
            raise ValueError(
                f"cannot capture local class {cls.__qualname__!r}: "
                "only positional constructor parameters are supported by the released DFunction schema"
            )
        default = None if parameter.default is inspect.Parameter.empty else repr(parameter.default)
        inputs.append(InputPort(name=parameter.name, typ_=None, default=default))
        call_arguments.append(ast.Name(id=parameter.name, ctx=ast.Load()))
    body = ast.Module(
        body=[
            tree,
            ast.Assign(
                targets=[
                    ast.Attribute(
                        value=ast.Name(id=tree.name, ctx=ast.Load()),
                        attr="__module__",
                        ctx=ast.Store(),
                    )
                ],
                value=ast.Constant(value=cls.__module__),
            ),
            ast.Assign(
                targets=[
                    ast.Attribute(
                        value=ast.Name(id=tree.name, ctx=ast.Load()),
                        attr="__qualname__",
                        ctx=ast.Store(),
                    )
                ],
                value=ast.Constant(value=cls.__qualname__),
            ),
            ast.Return(
                value=ast.Call(
                    func=ast.Name(id=tree.name, ctx=ast.Load()),
                    args=call_arguments,
                    keywords=[],
                )
            ),
        ],
        type_ignores=[],
    )
    return DFunction(
        name=exported_name,
        body=ast.unparse(ast.fix_missing_locations(body)),
        inputs=inputs,
        outputs=None,
    )


# --------------------------------------------------------------------------- #
# ir_parse handler: inline the local function instead of importing it.
# --------------------------------------------------------------------------- #


@ir_parse.register(is_inlinable_local_function)
def _ir_parse__local_function(x: FunctionType, name: str | None = None) -> _attach | _branch:

    if hasattr(x, LOCATION_DUNDER_NAME):
        # Already an spl-imported function: reference its source file, do not
        # inline it again (mirrors the __main__ handler).
        return _attach(chain([DSPLImport(*getattr(x, LOCATION_DUNDER_NAME))]))

    tree = _function_def(x)
    tree = cast(ast.FunctionDef, tree)
    exported_name = name or tree.name
    return _branch(
        x,
        lambda: replace(serialize_function(x, tree), name=exported_name),
        lambda _frame_offset: _local_dependencies(x, tree),
    )


@ir_parse.register(is_inlinable_local_class)
def _ir_parse__local_class(x: type[Any], name: str | None = None) -> _attach | _branch:
    tree = cast(ast.ClassDef, _class_def(x))
    exported_name = name or tree.name
    return _branch(
        x,
        lambda: _serialize_local_class_factory(x, tree, exported_name),
        lambda _frame_offset: _local_class_dependencies(x, tree),
    )
