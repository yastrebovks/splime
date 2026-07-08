from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from spl import Deployment
from spl.core.entities.adapter import DLoadAdapter, DSaveAdapter
from spl.core.entities.artifact import DArtifactRef
from spl.core.entities.control import DSPLSelfImport
from spl.core.entities.pipeline import Pipeline
from spl.core.ir.common import DBase, NamedDBase
from spl.core.ir.parse import get_top_level_deps
from spl.core.ir.utils import SPLSafeLoader, spl_import_from_file
from spl.daemon.canonical import _canonical_spl_documents

CORPUS_ROOT = Path(__file__).with_name("corpus")
V02X_CORPUS = CORPUS_ROOT / "v02x"
V040_CORPUS = CORPUS_ROOT / "v040"
V02X_FILES = tuple(sorted(V02X_CORPUS.glob("*.yaml")))
V040_FILES = tuple(sorted(V040_CORPUS.glob("*.yaml")))
NEW_040_FIELD_RE = re.compile(r"^\s*tags?:\s", re.MULTILINE)

FUNCTION_CASES = {
    "functional_node.yaml": ("compat_constant", 41),
}
PIPELINE_CASES = {
    "scalar_pipeline.yaml": ("scalar_pipeline", "sum", 7),
    "adapter_alias_pipeline.yaml": ("adapter_pipeline", "result", "loaded:hello|consumed"),
    "multinode_dag.yaml": ("multinode_dag", "total", 12),
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_documents(path: Path) -> list[list[DBase]]:
    documents = list(yaml.load_all(_read(path), Loader=SPLSafeLoader))
    assert documents, path
    assert all(isinstance(document, list) and document for document in documents)
    return cast(list[list[DBase]], documents)


def _namespace_from(path: Path) -> dict[str, Any]:
    namespace: dict[str, Any] = {"__name__": "__main__"}
    spl_import_from_file(path, namespace)
    return namespace


def _export_objects(objects: Iterable[Any]) -> str:
    top_level_deps = get_top_level_deps(2, list(objects))
    mapping = {
        root: DSPLSelfImport(name=cast(NamedDBase, root).name) for root, _ in top_level_deps if hasattr(root, "name")
    }
    return yaml.dump_all(
        [
            [root, *[mapping.get(dependency, dependency) for dependency in dependencies]]
            for root, dependencies in top_level_deps
        ],
        sort_keys=False,
        allow_unicode=True,
    )


def _execute_function(path: Path, entrypoint: str) -> Any:
    return _namespace_from(path)[entrypoint]()


def _execute_pipeline(path: Path, entrypoint: str, output: str) -> Any:
    pipeline = cast(Pipeline, _namespace_from(path)[entrypoint])
    return Deployment(pipeline).run(output=output)


@pytest.mark.parametrize("path", V02X_FILES, ids=lambda path: path.name)
def test_v02x_corpus_loads_with_safe_loader(path: Path) -> None:
    _load_documents(path)


def test_v02x_corpus_covers_legacy_yaml_tags() -> None:
    combined = "\n".join(_read(path) for path in V02X_FILES)

    for tag in (
        "!DFunction",
        "!DPipeline",
        "!DAdapter",
        "!DDistribution",
        "!DScalar",
        "!DArtifactRef",
        "!DNodeFunction",
        "!DNodeInputRef",
        "!DNodeOutputRef",
        "!DFormattedOutputRef",
    ):
        assert tag in combined


@pytest.mark.parametrize("path", V02X_FILES, ids=lambda path: path.name)
def test_v02x_corpus_omits_040_additive_fields(path: Path) -> None:
    assert NEW_040_FIELD_RE.search(_read(path)) is None


def test_v040_corpus_directory_exists_for_additive_formats() -> None:
    assert V040_CORPUS.is_dir()


@pytest.mark.parametrize("path", V040_FILES, ids=lambda path: path.name)
def test_v040_corpus_loads_with_safe_loader(path: Path) -> None:
    _load_documents(path)


def test_v040_corpus_covers_adapter_halves_and_artifact_tags() -> None:
    combined = "\n".join(_read(path) for path in V040_FILES)
    documents = _load_documents(V040_CORPUS / "adapter_halves_artifact_tag.yaml")
    root = documents[0]

    assert "!DSaveAdapter" in combined
    assert "!DLoadAdapter" in combined
    assert re.search(r"^\s+tag:\s+json$", combined, re.MULTILINE)
    assert isinstance(root[0], DSaveAdapter)
    assert isinstance(root[1], DLoadAdapter)
    assert isinstance(root[2], DArtifactRef)
    assert root[0].tag == "json"
    assert root[1].accepted_tags == ("json", "ndjson")
    assert root[2].tag == "json"


@pytest.mark.parametrize("name", sorted(FUNCTION_CASES))
def test_v02x_function_documents_execute(name: str) -> None:
    entrypoint, expected = FUNCTION_CASES[name]

    assert _execute_function(V02X_CORPUS / name, entrypoint) == expected


@pytest.mark.parametrize("name", sorted(PIPELINE_CASES))
def test_v02x_pipeline_documents_execute(name: str) -> None:
    entrypoint, output, expected = PIPELINE_CASES[name]

    assert _execute_pipeline(V02X_CORPUS / name, entrypoint, output) == expected


@pytest.mark.parametrize("path", V02X_FILES, ids=lambda path: path.name)
def test_v02x_corpus_yaml_round_trip_is_equivalent(path: Path) -> None:
    dumped = yaml.dump_all(_load_documents(path), sort_keys=False, allow_unicode=True)

    assert _canonical_spl_documents(dumped) == _canonical_spl_documents(_read(path))


@pytest.mark.parametrize("path", V040_FILES, ids=lambda path: path.name)
def test_v040_corpus_yaml_round_trip_is_equivalent(path: Path) -> None:
    dumped = yaml.dump_all(_load_documents(path), sort_keys=False, allow_unicode=True)

    assert _canonical_spl_documents(dumped) == _canonical_spl_documents(_read(path))


@pytest.mark.parametrize("name", sorted(PIPELINE_CASES))
def test_v02x_pipeline_ir_round_trip_remains_executable(tmp_path: Path, name: str) -> None:
    entrypoint, output, expected = PIPELINE_CASES[name]
    source_path = V02X_CORPUS / name
    pipeline = cast(Pipeline, _namespace_from(source_path)[entrypoint])
    round_trip_path = tmp_path / name

    round_trip_path.write_text(_export_objects([pipeline]), encoding="utf-8")

    assert _execute_pipeline(round_trip_path, entrypoint, output) == expected
