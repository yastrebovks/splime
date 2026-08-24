from __future__ import annotations

import copy
import importlib.util
import json
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


ROOT = Path(__file__).parents[2]
RAW_PATH = ROOT / "release" / "0.4.8" / "historical-api-signatures.json"


def _comparator() -> ModuleType:
    path = ROOT / "tools" / "compare_historical_api.py"
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location("prompt03e_compare_historical_api", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _raw(tmp_path: Path, mutation: Callable[[dict[str, Any]], None]) -> Path:
    document = json.loads(RAW_PATH.read_text(encoding="utf-8"))
    mutation(document["versions"][0])
    path = tmp_path / "historical-api-signatures.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _assert_first_version_fails(module: ModuleType, raw_path: Path) -> None:
    result = module.compare(raw_path)
    assert result["versions"][0]["status"] == "FAIL"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row["behavioral_results"]["start"]["call"].update(
            {"return_shape": {"type": "builtins.set", "sequence_length": 3}}
        ),
        lambda row: row["behavioral_results"]["create_library"]["call"]["return_shape"]["mapping_keys"].append(
            "historical_required_key"
        ),
        lambda row: row["behavioral_results"]["run_node"]["call"].update(
            {"return_shape": {"type": "builtins.list", "sequence_length": 1}}
        ),
        lambda row: row["behavioral_results"].pop("start"),
    ],
    ids=(
        "incompatible-return-type",
        "missing-required-mapping-key",
        "scalar-to-sequence",
        "missing-historical-behavior",
    ),
)
def test_historical_observed_return_mutations_fail(
    tmp_path: Path,
    mutation: Callable[[dict[str, Any]], None],
) -> None:
    module = _comparator()
    _assert_first_version_fails(module, _raw(tmp_path, mutation))


def test_current_call_failure_fails_the_affected_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _comparator()
    current = module.build_snapshot()
    current["behavioral_results"]["start"]["call"] = {
        "status": "FAIL",
        "error_type": "RuntimeError",
    }
    current["behavioral_results"]["start"]["status"] = "FAIL"
    monkeypatch.setattr(module, "build_snapshot", lambda: copy.deepcopy(current))
    _assert_first_version_fails(module, RAW_PATH)


def test_changed_meaningful_error_category_fails_the_affected_version(tmp_path: Path) -> None:
    def mutate(row: dict[str, Any]) -> None:
        row["behavioral_results"]["start"]["error_path"]["category"] = "unexpected_keyword"

    module = _comparator()
    _assert_first_version_fails(module, _raw(tmp_path, mutate))
