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

    def __lt__(self, other: "DDistribution") -> bool:
        return (self.package, self.version) < (other.package, other.version)


yaml.add_representer(DDistribution, lambda dumper, data: dumper.represent_mapping("!DDistribution", data.__dict__))

yaml.add_constructor(
    "!DDistribution",
    lambda loader, node: DDistribution(**cast(dict[str, Any], loader.construct_mapping(cast(Any, node)))),
)


def get_dependencies_from_distribution(module: ModuleType) -> Generator[DDistribution]:
    root = module.__name__.partition(".")[0]
    if root in sys.stdlib_module_names or root in sys.builtin_module_names:
        return

    distributions = packages_distributions()
    packages = distributions.get(root)
    if not packages:
        raise ValueError(f"cannot resolve an installed distribution for non-standard module {module.__name__!r}")

    for package in sorted(set(packages)):
        yield DDistribution(package=package, version=importlib.metadata.version(package))


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
