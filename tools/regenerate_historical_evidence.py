#!/usr/bin/env python3
"""Regenerate the 18-version raw/structural/behavioral compatibility corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import ssl
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import certifi


VERSIONS = (
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
)


def _download(row: dict[str, Any], target: Path) -> bytes:
    parsed = urlsplit(str(row["url"]))
    if (parsed.scheme, parsed.hostname) != ("https", "files.pythonhosted.org"):
        raise ValueError(f"historical artifact origin is not official PyPI: {row['url']}")
    request = Request(str(row["url"]), headers={"User-Agent": "splime-historical-evidence/2"})
    context = ssl.create_default_context(cafile=certifi.where())
    with urlopen(request, timeout=30, context=context) as response:  # noqa: S310 - exact origin checked twice.
        final = urlsplit(str(response.geturl()))
        if (final.scheme, final.hostname) != ("https", "files.pythonhosted.org"):
            raise ValueError(f"historical artifact redirect left official PyPI: {response.geturl()}")
        data = bytes(response.read())
    if hashlib.sha256(data).hexdigest() != row["sha256"]:
        raise ValueError(f"historical artifact hash mismatch: {row['filename']}")
    target.write_bytes(data)
    return data


def regenerate(
    *,
    inventory_path: Path,
    artifact_dir: Path,
    output_path: Path,
    check: bool = False,
) -> dict[str, Any]:
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    wheel_rows = {str(row["version"]): row for row in inventory["artifacts"] if str(row["filename"]).endswith(".whl")}
    if tuple(sorted(wheel_rows, key=lambda value: tuple(map(int, value.split("."))))) != VERSIONS:
        raise ValueError("historical wheel inventory does not match the published version set")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    probe_path = Path(__file__).with_name("probe_historical_api.py")
    rows: list[dict[str, Any]] = []
    for version in VERSIONS:
        artifact = wheel_rows[version]
        wheel = artifact_dir / str(artifact["filename"])
        data = wheel.read_bytes() if wheel.is_file() else _download(artifact, wheel)
        observed_hash = hashlib.sha256(data).hexdigest()
        if observed_hash != artifact["sha256"]:
            raise ValueError(f"historical artifact hash mismatch: {wheel.name}")
        environment = {
            key: value for key, value in os.environ.items() if key not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"}
        }
        environment.update(
            {
                "PYTHONPATH": str(wheel),
                "PYTHONNOUSERSITE": "1",
                "PYTHONUTF8": "1",
            }
        )
        completed = subprocess.run(
            [sys.executable, str(probe_path), "--output", "compact"],
            cwd=artifact_dir,
            env=environment,
            text=True,
            capture_output=True,
            timeout=120,
            check=False,
        )
        if completed.returncode:
            raise ValueError(f"historical probe failed for {version}: {completed.stderr[-1000:]}")
        probe = json.loads(completed.stdout)
        if probe.get("schema") != "splime.historical_api_probe.v2" or probe.get("version") != version:
            raise ValueError(f"historical probe identity disagrees for {version}")
        behavioral = probe["behavioral_results"]
        failures = [name for name, result in behavioral.items() if result["status"] == "FAIL"]
        status = "PASS" if not failures else "FAIL"
        rows.append(
            {
                "version": version,
                "status": status,
                "artifact": {
                    "filename": artifact["filename"],
                    "url": artifact["url"],
                    "sha256": observed_hash,
                    "requires_python": artifact["requires_python"],
                    "yanked": artifact["yanked"],
                },
                "interpreter": probe["interpreter"],
                "module_exports": probe["module_exports"],
                "callable_presence": probe["callable_presence"],
                "raw_signatures": probe["raw_signatures"],
                "structural_signatures": probe["structural_signatures"],
                "annotation_evidence": probe["annotation_evidence"],
                "behavioral_results": behavioral,
                "failures": failures,
            }
        )
    document = {
        "schema": "splime.historical_api_evidence.v2",
        "schema_version": 2,
        "generated_by": "tools/regenerate_historical_evidence.py",
        "probe": {
            "schema": "splime.historical_api_probe.v2",
            "version": 2,
            "tool": "tools/probe_historical_api.py",
        },
        "normalization": {
            "name": "annotation-display-only-v1",
            "scope": "comparison only; raw signatures remain unchanged",
            "rules": [
                "strip one balanced outer quote from annotation display",
                "collapse insignificant annotation whitespace",
            ],
            "never_normalized": [
                "parameter order",
                "parameter names",
                "parameter kinds",
                "required/default state",
                "default representation",
                "behavioral calls",
                "return/view shapes",
                "error paths",
            ],
        },
        "versions": rows,
    }
    rendered = json.dumps(document, indent=2, sort_keys=False) + "\n"
    if check:
        if not output_path.is_file() or output_path.read_text(encoding="utf-8") != rendered:
            raise ValueError("tracked historical evidence is stale")
    else:
        output_path.write_text(rendered, encoding="utf-8")
    return document


def main() -> int:
    root = Path(__file__).parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, default=root / "release/0.4.8/historical-artifacts.json")
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=root / "release/0.4.8/historical-api-signatures.json")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    document = regenerate(
        inventory_path=args.inventory.resolve(),
        artifact_dir=args.artifact_dir.resolve(),
        output_path=args.output.resolve(),
        check=args.check,
    )
    counts = {status: sum(row["status"] == status for row in document["versions"]) for status in ("PASS", "FAIL")}
    print(f"historical evidence: PASS={counts['PASS']} FAIL={counts['FAIL']}")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
