import importlib
import logging
import sys
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, packages_distributions
from pathlib import Path
from types import ModuleType
from typing import Any, Generator, cast

from spl.core._yaml import yaml

from spl.core.ir.common import DBase
from spl.core.ir.unparse import ir_unparse


@dataclass(frozen=True)
class DDistribution(DBase):
    package: str
    version: str
    # Import roots owned by this distribution.  This is intentionally explicit:
    # import names and distribution names are not interchangeable (for example,
    # ``yaml`` is provided by ``PyYAML``).
    modules: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if isinstance(self.modules, str) or not all(isinstance(module, str) for module in self.modules):
            raise TypeError("distribution modules must be a sequence of import-root strings")
        modules = tuple(sorted(set(self.modules)))
        if any(not module.isidentifier() for module in modules):
            raise ValueError("distribution modules must contain valid top-level import names")
        object.__setattr__(self, "modules", modules)

    def __lt__(self, other: "DDistribution") -> bool:
        return (self.package, self.version, self.modules) < (other.package, other.version, other.modules)


def _represent_distribution(dumper: Any, data: DDistribution) -> Any:
    value: dict[str, Any] = {"package": data.package, "version": data.version}
    if data.modules:
        value["modules"] = list(data.modules)
    return dumper.represent_mapping("!DDistribution", value)


yaml.add_representer(DDistribution, _represent_distribution)

yaml.add_constructor(
    "!DDistribution",
    lambda loader, node: DDistribution(**cast(dict[str, Any], loader.construct_mapping(cast(Any, node)))),
)


def get_dependencies_from_distribution(module: ModuleType) -> Generator[DDistribution]:
    root = module.__name__.partition(".")[0]
    yield from get_dependencies_from_module_name(root, required=True)


def get_dependencies_from_module_name(module_name: str, *, required: bool = False) -> Generator[DDistribution]:
    """Resolve an import root to its installed distribution without importing it."""

    root = module_name.partition(".")[0]
    if root in sys.stdlib_module_names or root in sys.builtin_module_names:
        return

    distributions = packages_distributions()
    packages = distributions.get(root)
    if not packages:
        if required:
            raise ValueError(f"cannot resolve an installed distribution for non-standard module {module_name!r}")
        return

    for package in sorted(set(packages)):
        yield DDistribution(package=package, version=importlib.metadata.version(package), modules=(root,))


def validate_distributions(deps: list[tuple[DBase, list[DBase]]], source: str) -> None:
    distributions = sorted(
        {dependency for _, dependencies in deps for dependency in dependencies if isinstance(dependency, DDistribution)}
    )

    for x in distributions:
        try:
            if (version := importlib.metadata.version(x.package)) != x.version:
                logging.warning(
                    "{}: distribution mismatch: {} == {} (actual {})".format(source, x.package, x.version, version)
                )
        except PackageNotFoundError:
            logging.warning("{}: distribution is not found: {} == {}".format(source, x.package, x.version))


@ir_unparse.register(lambda x: isinstance(x, DDistribution))
def _ir_unparse__distribution(x: DDistribution, source: Path) -> Generator[Any]:
    yield from []
