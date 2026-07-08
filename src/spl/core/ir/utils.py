import sys
from collections.abc import Callable, Iterable
from itertools import chain, repeat
from operator import itemgetter
from pathlib import Path
from typing import Any, TypeVar, cast

import yaml

from spl.core.entities.adapter import DAdapter, DLoadAdapter, DSaveAdapter
from spl.core.entities.artifact import DArtifactRef
from spl.core.entities.control import DSPLImport, DSPLSelfImport
from spl.core.entities.distribution import DDistribution, validate_distributions
from spl.core.entities.function import DFunction
from spl.core.entities.local_function import DLocalAlias
from spl.core.entities.module import DImport, DImportFrom
from spl.core.entities.node import (
    DFormattedOutputRef,
    DNodeInputRef,
    DNodeOutputRef,
    InputPort,
    OutputPort,
)
from spl.core.entities.node_function import DNodeFunction
from spl.core.entities.node_remote import DNodeRemote
from spl.core.entities.pipeline import DPipeline
from spl.core.entities.scalar import DScalar
from spl.core.ir.common import DBase, NamedDBase
from spl.core.ir.parse import get_top_level_deps
from spl.core.ir.unparse import mk_top_level_ast

_T = TypeVar("_T", bound=DBase)


class SPLSafeLoader(yaml.SafeLoader):
    pass


def _construct_dataclass(cls: type[_T]) -> Callable[[Any, Any], _T]:
    def _construct(loader: Any, node: Any) -> _T:
        return cls(**loader.construct_mapping(node, deep=True))

    return _construct


def _construct_dfunction(loader: Any, node: Any) -> DFunction:
    data = loader.construct_mapping(node, deep=True)
    return DFunction(
        name=data["name"],
        body=data["body"],
        inputs=[InputPort(name=x["name"], typ_=x["type"], default=x.get("default")) for x in data["inputs"]],
        outputs=None
        if data["outputs"] is None
        else [OutputPort(name=x["name"], typ_=x["type"]) for x in data["outputs"]],
    )


SPL_YAML_CONSTRUCTORS = {
    "!DSPLSelfImport": _construct_dataclass(DSPLSelfImport),
    "!DSPLImport": _construct_dataclass(DSPLImport),
    "!DDistribution": _construct_dataclass(DDistribution),
    "!DFunction": _construct_dfunction,
    "!DLocalAlias": _construct_dataclass(DLocalAlias),
    "!DImport": _construct_dataclass(DImport),
    "!DImportFrom": _construct_dataclass(DImportFrom),
    "!DFormattedOutputRef": _construct_dataclass(DFormattedOutputRef),
    "!DNodeInputRef": _construct_dataclass(DNodeInputRef),
    "!DNodeOutputRef": _construct_dataclass(DNodeOutputRef),
    "!DNodeFunction": _construct_dataclass(DNodeFunction),
    "!DNodeRemote": _construct_dataclass(DNodeRemote),
    "!DPipeline": _construct_dataclass(DPipeline),
    "!DScalar": _construct_dataclass(DScalar),
    "!DArtifactRef": _construct_dataclass(DArtifactRef),
    "!DAdapter": _construct_dataclass(DAdapter),
    "!DSaveAdapter": _construct_dataclass(DSaveAdapter),
    "!DLoadAdapter": _construct_dataclass(DLoadAdapter),
}


for _tag, _constructor in SPL_YAML_CONSTRUCTORS.items():
    SPLSafeLoader.add_constructor(_tag, _constructor)


def spl_export_to_file(fname: Path, xs: list[Any]) -> None:
    top_level_deps = get_top_level_deps(2, xs)

    mapping = {
        root: DSPLSelfImport(name=cast(NamedDBase, root).name) for (root, _) in top_level_deps if hasattr(root, "name")
    }

    deps_by_root = {root: [mapping.get(x, x) for x in dependencies] for root, dependencies in top_level_deps}

    fname.write_text(
        yaml.dump_all(
            [[root, *dependencies] for root, dependencies in deps_by_root.items()], sort_keys=False, allow_unicode=True
        )
    )


def spl_export_to_dir(dname: Path, xs: list[Any]) -> None:
    top_level_deps = get_top_level_deps(2, xs)
    mapping = {
        root: DSPLImport(path="./{}.yaml".format(cast(NamedDBase, root).name), name=cast(NamedDBase, root).name)
        for (root, _) in top_level_deps
    }

    deps_by_root = {root: [mapping.get(x, x) for x in dependencies] for root, dependencies in top_level_deps}

    for root, dependencies in deps_by_root.items():
        fname = dname / "{}.yaml".format(cast(NamedDBase, root).name)
        fname.write_text(yaml.dump([root, *dependencies], sort_keys=False, allow_unicode=True))


def mk_top_level_deps_closure(fnames: Iterable[Path]) -> list[tuple[Path, tuple[DBase, list[DBase]]]]:
    top_level_deps: list[tuple[Path, tuple[DBase, list[DBase]]]] = []
    imports = set(map(Path.absolute, fnames))
    queue = list(imports)

    while len(queue) > 0:
        fname, *queue = queue

        top_level_deps_new: list[tuple[DBase, list[DBase]]] = [
            (cast(DBase, root), cast(list[DBase], dependencies))
            for (root, *dependencies) in yaml.load_all(fname.read_text(), Loader=SPLSafeLoader)
        ]

        top_level_deps = [*top_level_deps, *zip(repeat(fname), top_level_deps_new, strict=False)]

        # TODO: filter by x.name
        new_imports = sorted(
            set(
                [
                    (fname.parent / x.path).absolute()
                    for x in chain.from_iterable([dependencies for (_, dependencies) in top_level_deps_new])
                    if isinstance(x, DSPLImport)
                ]
            )
            - imports
        )

        imports = set([*imports, *new_imports])
        queue = [*queue, *new_imports]

    return top_level_deps


def spl_import_from_file(fname: Path, globals: dict[str, Any] | None = None) -> None:
    if globals is None:
        globals = sys._getframe(1).f_globals

    top_level_deps = mk_top_level_deps_closure([fname])

    validate_distributions(list(map(itemgetter(1), top_level_deps)), str(fname.absolute()))

    for fname, (root, dependencies) in top_level_deps[::-1]:
        expr = mk_top_level_ast((root, dependencies), fname)
        eval(  # noqa: S307
            compile(expr, str(fname), mode="exec"), globals=globals
        )
