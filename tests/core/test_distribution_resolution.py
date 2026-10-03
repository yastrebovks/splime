from __future__ import annotations

import json
import ast
import sys
from types import ModuleType
import urllib.parse

import pytest

from spl.core.ir.utils import spl_export_to_file
from spl.core.entities import distribution as m_distribution
from spl.core.entities.distribution import DDistribution, get_dependencies_from_distribution
from spl.core.entities.function import get_dependencies_from_ast


def _json_round_trip(value):
    return json.loads(json.dumps(value))


def _body_import_probe(value):
    import yaml as yaml_alias
    from packaging.version import Version

    return yaml_alias.safe_load(value), Version("1")


@pytest.mark.parametrize("module", [json, urllib.parse, sys])
def test_standard_library_modules_do_not_create_distribution_dependencies(module: ModuleType) -> None:
    assert list(get_dependencies_from_distribution(module)) == []


def test_live_function_export_imports_stdlib_without_distribution_metadata(tmp_path) -> None:
    output = tmp_path / "json-round-trip.spl.yaml"

    spl_export_to_file(output, [_json_round_trip])

    text = output.read_text(encoding="utf-8")
    assert "module: json" in text
    assert "package: json" not in text


def test_live_function_export_captures_imports_inside_body(tmp_path) -> None:
    output = tmp_path / "body-imports.spl.yaml"

    spl_export_to_file(output, [_body_import_probe])

    text = output.read_text(encoding="utf-8")
    assert "package: PyYAML" in text and "modules:\n  - yaml" in text
    assert "package: packaging" in text and "- packaging" in text


def test_installed_third_party_module_uses_top_level_distribution_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("example_runtime.client")
    module.__package__ = "example_runtime"
    monkeypatch.setattr(
        m_distribution,
        "packages_distributions",
        lambda: {"example_runtime": ["Example-Runtime", "Example-Runtime"]},
    )
    monkeypatch.setattr(
        m_distribution.importlib.metadata,
        "version",
        lambda package: "1.2.3" if package == "Example-Runtime" else "unexpected",
    )

    assert list(get_dependencies_from_distribution(module)) == [
        DDistribution(package="Example-Runtime", version="1.2.3", modules=("example_runtime",))
    ]


def test_single_file_third_party_module_is_not_silently_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("singlefile_runtime")
    module.__package__ = ""
    monkeypatch.setattr(
        m_distribution,
        "packages_distributions",
        lambda: {"singlefile_runtime": ["singlefile-runtime"]},
    )
    monkeypatch.setattr(m_distribution.importlib.metadata, "version", lambda _package: "4.5.6")

    assert list(get_dependencies_from_distribution(module)) == [
        DDistribution(package="singlefile-runtime", version="4.5.6", modules=("singlefile_runtime",))
    ]


def test_function_body_alias_and_from_imports_capture_distribution_ownership(monkeypatch) -> None:
    tree = ast.parse(
        "def probe():\n    import example_runtime.client as client\n    from yaml import safe_load\n    return client, safe_load\n"
    ).body[0]
    monkeypatch.setattr(
        m_distribution,
        "packages_distributions",
        lambda: {"example_runtime": ["Example-Runtime"], "yaml": ["PyYAML"]},
    )
    monkeypatch.setattr(
        m_distribution.importlib.metadata,
        "version",
        lambda package: {"Example-Runtime": "1.2.3", "PyYAML": "6.0.3"}[package],
    )
    assert list(get_dependencies_from_ast(0, tree)) == [
        DDistribution(package="Example-Runtime", version="1.2.3", modules=("example_runtime",)),
        DDistribution(package="PyYAML", version="6.0.3", modules=("yaml",)),
    ]


def test_unknown_non_standard_module_fails_with_controlled_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("unregistered_runtime")
    monkeypatch.setattr(m_distribution, "packages_distributions", lambda: {})

    with pytest.raises(
        ValueError,
        match="cannot resolve an installed distribution.*unregistered_runtime",
    ):
        list(get_dependencies_from_distribution(module))
