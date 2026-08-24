#!/usr/bin/env python3
"""Verify the bounded 0.4.8 historical artifact compatibility inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


ALLOWED_STATUSES = {"tested", "blocked", "not_applicable"}


def load_inventory(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise ValueError("historical artifact inventory schema is invalid")
    return value


def verify_inventory(value: dict[str, Any], artifact_dir: Path | None = None) -> None:
    artifacts = value.get("artifacts")
    rows = value.get("api_rows")
    protocols = value.get("protocol_rows")
    absent = value.get("absent_registry_versions")
    if not all(isinstance(item, list) for item in (artifacts, rows, protocols, absent)):
        raise ValueError("historical artifact inventory rows are missing")
    for group_name, group in (
        ("artifact", artifacts),
        ("api", rows),
        ("protocol", protocols),
        ("absent", absent),
    ):
        for index, row in enumerate(group):
            if not isinstance(row, dict) or row.get("status") not in ALLOWED_STATUSES:
                raise ValueError(f"{group_name} row {index} has no explicit status")
            if not isinstance(row.get("reason"), str) or not row["reason"]:
                raise ValueError(f"{group_name} row {index} has no reason")
    identities: set[tuple[str, str]] = set()
    for artifact in artifacts:
        identity = (artifact["version"], artifact["filename"])
        if identity in identities:
            raise ValueError(f"duplicate artifact inventory row: {identity}")
        identities.add(identity)
        digest = artifact.get("sha256")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ValueError(f"invalid SHA-256 for {artifact['filename']}")
        if artifact_dir is not None:
            path = artifact_dir / artifact["filename"]
            observed = hashlib.sha256(path.read_bytes()).hexdigest()
            if observed != digest:
                raise ValueError(f"artifact hash mismatch for {artifact['filename']}: {observed}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inventory",
        type=Path,
        default=Path(__file__).parents[1] / "release" / "0.4.8" / "historical-artifacts.json",
    )
    parser.add_argument("--artifact-dir", type=Path)
    args = parser.parse_args()
    verify_inventory(load_inventory(args.inventory), args.artifact_dir)
    print("historical artifact inventory: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
