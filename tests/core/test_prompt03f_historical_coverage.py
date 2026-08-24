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
NEW_BEHAVIOR_METHODS = ("objects", "submit", "publish", "publish_yaml")


def _comparator() -> ModuleType:
    path = ROOT / "tools" / "compare_historical_api.py"
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location("prompt03f_compare_historical_api", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _document() -> dict[str, Any]:
    return json.loads(RAW_PATH.read_text(encoding="utf-8"))


def _row(document: dict[str, Any], version: str) -> dict[str, Any]:
    return next(row for row in document["versions"] if row["version"] == version)


def _mutated_path(
    tmp_path: Path,
    *,
    version: str,
    mutation: Callable[[dict[str, Any]], None],
) -> Path:
    document = _document()
    mutation(_row(document, version))
    path = tmp_path / "historical-api-signatures.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _assert_only_version_fails(result: dict[str, Any], version: str) -> None:
    statuses = {row["version"]: row["status"] for row in result["versions"]}
    assert statuses[version] == "FAIL"
    assert {item for item, status in statuses.items() if status != "PASS"} == {version}


def test_present_callable_missing_structural_signature_fails_only_affected_version(
    tmp_path: Path,
) -> None:
    module = _comparator()
    path = _mutated_path(
        tmp_path,
        version="0.1.4",
        mutation=lambda row: row["structural_signatures"].pop("SPLClient.start"),
    )
    _assert_only_version_fails(module.compare(path), "0.1.4")


def test_present_callable_missing_annotation_is_a_controlled_version_failure(
    tmp_path: Path,
) -> None:
    module = _comparator()
    path = _mutated_path(
        tmp_path,
        version="0.2.3",
        mutation=lambda row: row["annotation_evidence"].pop("SPLClient.objects"),
    )
    _assert_only_version_fails(module.compare(path), "0.2.3")


@pytest.mark.parametrize("evidence", ("structural_signatures", "annotation_evidence"))
def test_constructor_evidence_fails_closed(
    tmp_path: Path,
    evidence: str,
) -> None:
    module = _comparator()
    path = _mutated_path(
        tmp_path,
        version="0.4.4",
        mutation=lambda row: row[evidence].pop("SPLClient"),
    )
    _assert_only_version_fails(module.compare(path), "0.4.4")


def test_all_present_methods_have_real_behavior_forms_and_non_null_comparisons() -> None:
    module = _comparator()
    assert set(NEW_BEHAVIOR_METHODS).issubset(module.BEHAVIOR_FORMS)

    document = _document()
    for version in ("0.1.4", "0.2.3"):
        historical = _row(document, version)
        for name in NEW_BEHAVIOR_METHODS:
            if historical["callable_presence"][name]:
                behavior = historical["behavioral_results"][name]
                assert behavior["status"] == "PASS"
                assert behavior["call"]["status"] == "PASS"
                assert behavior["call"]["return_shape"] is not None

    result = module.compare(RAW_PATH)
    for version in result["versions"]:
        for row in version["return_shapes"]:
            if row["callable"] == "SPLClient":
                continue
            if row["status"] == "PASS":
                assert row.get("historical_observed") is not None
                assert row.get("current_observed") is not None


def test_tracked_verdict_has_complete_per_version_and_aggregate_coverage() -> None:
    raw = _document()
    comparison = json.loads((ROOT / "release" / "0.4.8" / "historical-api-comparison.json").read_text(encoding="utf-8"))
    expected_total = 0
    for historical, verdict in zip(raw["versions"], comparison["versions"], strict=True):
        present = {name for name, value in historical["callable_presence"].items() if value}
        expected_total += len(present)
        assert verdict["status"] == "PASS"
        assert verdict["coverage"] == {
            "present_callables": len(present),
            "structural_signatures": len(present),
            "annotation_rows": len(present),
            "successful_behavioral_calls": len(present),
            "compared_return_rows": len(present),
            "required_gaps": 0,
            "required_gap_details": [],
            "null_observation_pass_rows": 0,
            "constructor_structural": True,
            "constructor_annotations": True,
        }
        compared = {
            row["callable"].removeprefix("SPLClient.")
            for row in verdict["return_shapes"]
            if row["callable"] != "SPLClient"
        }
        assert compared == present

    assert comparison["coverage_audit"] == {
        "versions": len(raw["versions"]),
        "present_callables": expected_total,
        "structural_signatures": expected_total,
        "annotation_rows": expected_total,
        "successful_behavioral_calls": expected_total,
        "compared_return_rows": expected_total,
        "required_gaps": 0,
        "null_observation_pass_rows": 0,
    }


@pytest.mark.parametrize("method", NEW_BEHAVIOR_METHODS)
def test_each_new_behavior_missing_from_later_version_fails_only_that_version(
    tmp_path: Path,
    method: str,
) -> None:
    module = _comparator()
    path = _mutated_path(
        tmp_path,
        version="0.2.3",
        mutation=lambda row: row["behavioral_results"].pop(method),
    )
    _assert_only_version_fails(module.compare(path), "0.2.3")


@pytest.mark.parametrize("method", NEW_BEHAVIOR_METHODS)
def test_each_new_current_behavior_failure_fails_only_affected_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
) -> None:
    module = _comparator()
    current = module.build_snapshot()
    current["behavioral_results"][method] = {
        "status": "FAIL",
        "call_binding": {"status": "PASS"},
        "call": {"status": "FAIL", "error_type": "RuntimeError"},
        "error_path": {"status": "PASS", "error_type": "TypeError", "category": "missing_required"},
    }
    monkeypatch.setattr(module, "build_snapshot", lambda: copy.deepcopy(current))

    document = _document()
    for row in document["versions"]:
        if row["version"] != "0.2.3":
            row["callable_presence"][method] = False
            row["structural_signatures"].pop(f"SPLClient.{method}", None)
            row["annotation_evidence"].pop(f"SPLClient.{method}", None)
    path = tmp_path / "historical-api-signatures.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    _assert_only_version_fails(module.compare(path), "0.2.3")
