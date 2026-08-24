from __future__ import annotations

import json
from pathlib import Path


def test_published_048_extension_is_exact_and_passed() -> None:
    root = Path(__file__).parents[2]
    evidence = json.loads((root / "release" / "0.4.9" / "historical-extension.json").read_text(encoding="utf-8"))

    assert evidence["schema"] == "splime.historical_compatibility_extension.v1"
    assert evidence["release_target"] == "0.4.9"
    assert evidence["extends"] == "release/0.4.8/historical-artifacts.json"
    assert evidence["version"] == "0.4.8"
    assert {row["filename"] for row in evidence["artifacts"]} == {
        "splime-0.4.8-py3-none-any.whl",
        "splime-0.4.8.tar.gz",
    }
    assert all(row["url"].startswith("https://files.pythonhosted.org/") for row in evidence["artifacts"])
    assert all(len(row["sha256"]) == 64 for row in evidence["artifacts"])
    assert evidence["api_probe"] == {
        "schema": "splime.historical_api_probe.v2",
        "status": "PASS",
        "behavioral_rows": 19,
        "failures": [],
    }
    assert evidence["facade_comparison"]["status"] == "PASS"
    assert evidence["facade_comparison"]["required_gaps"] == 0
    assert evidence["facade_comparison"]["null_observation_pass_rows"] == 0
