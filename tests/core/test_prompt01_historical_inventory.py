from __future__ import annotations

import json
from pathlib import Path


EXPECTED_RELEASES = {
    "0.1.2",
    "0.1.3",
    "0.1.4",
    "0.1.5",
    "0.2.0",
    "0.2.1",
    "0.2.2",
    "0.2.3",
    "0.2.4",
    "0.2.5",
    "0.3.0",
    "0.4.0",
    "0.4.2",
    "0.4.3",
    "0.4.4",
    "0.4.5",
    "0.4.6",
    "0.4.7",
}


def _inventory() -> dict[str, object]:
    path = Path(__file__).parents[2] / "release" / "0.4.8" / "historical-artifacts.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_every_published_release_has_both_verified_artifacts_and_api_evidence() -> None:
    inventory = _inventory()
    artifacts = inventory["artifacts"]
    api_rows = inventory["api_rows"]
    assert isinstance(artifacts, list)
    assert isinstance(api_rows, list)
    assert {row["version"] for row in api_rows} == EXPECTED_RELEASES
    for version in EXPECTED_RELEASES:
        release_artifacts = [row for row in artifacts if row["version"] == version]
        assert {row["filename"].removeprefix(f"splime-{version}") for row in release_artifacts} == {
            "-py3-none-any.whl",
            ".tar.gz",
        }
        assert all(row["status"] == "tested" for row in release_artifacts)
        assert all(row["requires_python"] == ">=3.13" for row in release_artifacts)
        assert all(row["yanked"] is False for row in release_artifacts)


def test_every_compatibility_row_is_explicit_and_names_executable_evidence() -> None:
    inventory = _inventory()
    for group in (
        "artifacts",
        "api_rows",
        "protocol_rows",
        "absent_registry_versions",
    ):
        rows = inventory[group]
        assert isinstance(rows, list) and rows
        for row in rows:
            assert row["status"] in {"tested", "blocked", "not_applicable"}
            assert row["reason"]
            if row["status"] == "tested":
                assert row["evidence"]


def test_non_contiguous_registry_versions_are_not_inferred() -> None:
    inventory = _inventory()
    absent = {row["version"]: row["status"] for row in inventory["absent_registry_versions"]}
    assert absent == {
        "0.1.0": "not_applicable",
        "0.1.1": "not_applicable",
        "0.4.1": "not_applicable",
    }
