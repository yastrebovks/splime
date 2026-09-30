#!/usr/bin/env python3
"""Verify a published predecessor and its recorded compatibility extension."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import ssl
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import certifi

from compare_historical_api import compare


def _download(row: dict[str, Any], target: Path) -> None:
    parsed = urlsplit(str(row["url"]))
    if (parsed.scheme, parsed.hostname) != ("https", "files.pythonhosted.org"):
        raise ValueError("historical extension artifact origin is not official PyPI")
    request = Request(str(row["url"]), headers={"User-Agent": "splime-history-extension/1"})
    context = ssl.create_default_context(cafile=certifi.where())
    with urlopen(request, timeout=30, context=context) as response:  # noqa: S310 - exact origin checked.
        final = urlsplit(str(response.geturl()))
        if (final.scheme, final.hostname) != ("https", "files.pythonhosted.org"):
            raise ValueError("historical extension redirect left official PyPI")
        data = bytes(response.read())
    if len(data) != row["size"] or hashlib.sha256(data).hexdigest() != row["sha256"]:
        raise ValueError(f"historical extension artifact mismatch: {row['filename']}")
    target.write_bytes(data)


def verify(evidence_path: Path) -> dict[str, Any]:
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    version = evidence.get("version")
    release_target = evidence.get("release_target")
    if (
        evidence.get("schema") != "splime.historical_compatibility_extension.v1"
        or evidence.get("schema_version") != 1
        or not isinstance(version, str)
        or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None
        or not isinstance(release_target, str)
        or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", release_target) is None
        or release_target == version
    ):
        raise ValueError("historical compatibility extension identity is invalid")
    artifacts = evidence.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 2:
        raise ValueError("historical compatibility extension artifact inventory is invalid")
    by_filename = {str(row.get("filename")): row for row in artifacts if isinstance(row, dict)}
    wheel_name = f"splime-{version}-py3-none-any.whl"
    if set(by_filename) != {wheel_name, f"splime-{version}.tar.gz"}:
        raise ValueError("historical compatibility extension must bind wheel and sdist")

    with tempfile.TemporaryDirectory(prefix=f"splime-{version}-extension-") as raw:
        root = Path(raw)
        for filename, row in by_filename.items():
            _download(row, root / filename)
        wheel = root / wheel_name
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
        probe_path = Path(__file__).with_name("probe_historical_api.py")
        completed = subprocess.run(
            [sys.executable, str(probe_path), "--output", "compact"],
            cwd=root,
            env=environment,
            text=True,
            capture_output=True,
            timeout=120,
            check=False,
        )
        if completed.returncode:
            raise ValueError(f"published {version} probe failed: {completed.stderr[-1000:]}")
        probe = json.loads(completed.stdout)
        behavioral = probe["behavioral_results"]
        failures = [name for name, result in behavioral.items() if result["status"] == "FAIL"]
        raw_evidence = {
            "schema": "splime.historical_api_evidence.v2",
            "schema_version": 2,
            "generated_by": "tools/verify_published_compatibility_extension.py",
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
            "versions": [
                {
                    "version": version,
                    "status": "PASS" if not failures else "FAIL",
                    "artifact": by_filename[wheel_name],
                    "interpreter": probe["interpreter"],
                    "module_exports": probe["module_exports"],
                    "callable_presence": probe["callable_presence"],
                    "raw_signatures": probe["raw_signatures"],
                    "structural_signatures": probe["structural_signatures"],
                    "annotation_evidence": probe["annotation_evidence"],
                    "behavioral_results": behavioral,
                    "failures": failures,
                }
            ],
        }
        raw_path = root / "raw.json"
        raw_path.write_text(json.dumps(raw_evidence), encoding="utf-8")
        comparison = compare(raw_path)
        (compared,) = comparison["versions"]

    expected_probe = evidence["api_probe"]
    if (
        probe.get("schema") != expected_probe.get("schema")
        or len(behavioral) != expected_probe.get("behavioral_rows")
        or failures != expected_probe.get("failures")
        or ("PASS" if not failures else "FAIL") != expected_probe.get("status")
    ):
        raise ValueError(f"published {version} probe disagrees with recorded evidence")
    expected_comparison = evidence["facade_comparison"]
    observed_comparison = {"status": compared["status"]} | {
        key: compared["coverage"][key] for key in expected_comparison if key != "status"
    }
    if observed_comparison != expected_comparison:
        raise ValueError(f"published {version} facade comparison disagrees with recorded evidence")
    return {
        "version": version,
        "release_target": release_target,
        "artifacts": len(artifacts),
        "behavioral_rows": len(behavioral),
        "comparison": observed_comparison,
    }


def main() -> int:
    root = Path(__file__).parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--evidence",
        type=Path,
        default=root / "release" / "0.4.10" / "historical-extension.json",
    )
    args = parser.parse_args()
    result = verify(args.evidence.resolve())
    print(
        f"published {result['version']} compatibility with {result['release_target']}: "
        f"artifacts={result['artifacts']} behavioral={result['behavioral_rows']} "
        f"status={result['comparison']['status']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
