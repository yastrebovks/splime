from __future__ import annotations

import json
import hashlib
from pathlib import Path


def test_historical_signature_evidence_is_raw_structural_and_behavioral_per_version() -> None:
    path = Path(__file__).parents[2] / "release" / "0.4.8" / "historical-api-signatures.json"
    document = json.loads(path.read_text(encoding="utf-8"))

    assert document["schema_version"] == 2
    assert document["generated_by"] == "tools/regenerate_historical_evidence.py"
    assert document["probe"]["schema"] == "splime.historical_api_probe.v2"
    assert document["probe"]["tool"] == "tools/probe_historical_api.py"
    assert len(document["versions"]) == 18
    assert {row["status"] for row in document["versions"]} == {"PASS"}
    for row in document["versions"]:
        assert row["artifact"]["url"].startswith("https://files.pythonhosted.org/")
        assert len(row["artifact"]["sha256"]) == 64
        assert row["interpreter"]
        assert row["raw_signatures"]
        assert row["structural_signatures"]
        assert row["annotation_evidence"]
        assert row["behavioral_results"]


def test_historical_verdict_is_a_separate_current_facade_comparison() -> None:
    root = Path(__file__).parents[2] / "release" / "0.4.8"
    raw_path = root / "historical-api-signatures.json"
    comparison_path = root / "historical-api-comparison.json"
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    comparison = json.loads(comparison_path.read_text(encoding="utf-8"))

    assert raw["schema"] == "splime.historical_api_evidence.v2"
    assert comparison["schema"] == "splime.historical_api_comparison.v1"
    assert comparison["raw_evidence_sha256"] == hashlib.sha256(raw_path.read_bytes()).hexdigest()
    assert comparison["current_facade"]["version"] == "0.4.8"
    assert len(comparison["versions"]) == len(raw["versions"]) == 18
    assert {row["status"] for row in comparison["versions"]} == {"PASS"}
    for row in comparison["versions"]:
        assert row["call_forms"]
        assert row["parameter_contracts"]
        assert row["return_shapes"]
        assert row["error_behaviors"]

    inventory = json.loads((root / "historical-artifacts.json").read_text(encoding="utf-8"))
    for row in inventory["api_rows"]:
        assert "release/0.4.8/historical-api-signatures.json" in row["evidence"]
        assert "release/0.4.8/historical-api-comparison.json" in row["evidence"]
