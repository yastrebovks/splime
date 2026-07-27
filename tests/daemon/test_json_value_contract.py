from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest

from spl.daemon import spl_free_runner
from spl.daemon import storage_base
from spl.daemon import worker
from spl.daemon.canonical import canonicalize

_FIXTURE_SHA256 = "94c4dad7ec0c5e8e5f265f91531abfc4c17324be0cc5741204c9ea7ec6d1977c"


def _fixture_bytes() -> bytes:
    local_path = Path(__file__).resolve().parents[1] / "fixtures" / "json-values-v1.json"
    workspace_path = Path(__file__).resolve().parents[3] / "Release" / "contracts" / "json-values-v1.json"
    local_bytes = local_path.read_bytes()
    assert hashlib.sha256(local_bytes).hexdigest() == _FIXTURE_SHA256
    if workspace_path.exists():
        workspace_bytes = workspace_path.read_bytes()
        assert workspace_bytes == local_bytes
        return workspace_bytes
    return local_bytes


def _fixture_cases() -> list[dict[str, Any]]:
    payload = cast(dict[str, Any], json.loads(_fixture_bytes()))
    assert payload["contract"] == "splime-json-value-v1"
    return cast(list[dict[str, Any]], payload["cases"])


def _non_finite(token: str) -> float:
    return {
        "nan": float("nan"),
        "infinity": float("inf"),
        "negative_infinity": float("-inf"),
    }[token]


def _replace_marker(value: Any, marker: str, replacement: Any) -> Any:
    if value == marker:
        return replacement
    if isinstance(value, list):
        return [_replace_marker(item, marker, replacement) for item in value]
    if isinstance(value, dict):
        return {key: _replace_marker(item, marker, replacement) for key, item in value.items()}
    return value


def _fixture_value(case: dict[str, Any]) -> Any:
    construct = case["construct"]
    if construct == "literal":
        return copy.deepcopy(case["value"])
    if construct == "negative_zero":
        return -0.0
    if construct == "unsafe_integer":
        return int(cast(str, case["token"]))
    if construct == "lone_surrogate":
        return _lone_surrogate(cast(str, case["token"]))
    if construct == "nested_lone_surrogate":
        return _replace_marker(
            copy.deepcopy(case["value"]),
            cast(str, case["marker"]),
            _lone_surrogate(cast(str, case["token"])),
        )
    if construct == "non_finite":
        return _non_finite(cast(str, case["token"]))
    if construct == "nested_non_finite":
        return _replace_marker(
            copy.deepcopy(case["value"]),
            cast(str, case["marker"]),
            _non_finite(cast(str, case["token"])),
        )
    if construct == "non_string_key":
        return {case["key"]: case["value"]}
    raise AssertionError("unknown fixture constructor: {}".format(construct))


def _lone_surrogate(token: str) -> str:
    return {"high": "\ud800", "low": "\udfff"}[token]


@pytest.mark.parametrize("case", _fixture_cases(), ids=lambda case: cast(str, case["id"]))
def test_daemon_writers_follow_shared_json_value_contract(
    case: dict[str, Any],
    tmp_path: Path,
) -> None:
    value = _fixture_value(case)
    worker_path = tmp_path / "worker.json"
    storage_path = tmp_path / "storage.json"
    standalone_path = tmp_path / "standalone.json"

    if case["accepted"]:
        assert spl_free_runner._json_dumps(value) == case["serialized"]
        assert json.loads(storage_base.json_dumps(value)) == value
        for writer, path in (
            (worker.write_json, worker_path),
            (storage_base.write_json, storage_path),
            (spl_free_runner.write_json, standalone_path),
        ):
            writer(path, value)
            assert json.loads(path.read_text(encoding="utf-8")) == value
        return

    expected_path = cast(str, case["path"])
    for serialize in (storage_base.json_dumps, spl_free_runner._json_dumps):
        with pytest.raises(ValueError) as exc_info:
            serialize(value)
        assert expected_path in str(exc_info.value)

    for writer, path in (
        (worker.write_json, worker_path),
        (storage_base.write_json, storage_path),
        (spl_free_runner.write_json, standalone_path),
    ):
        path.write_text("preserved", encoding="utf-8")
        with pytest.raises(ValueError) as exc_info:
            writer(path, value)
        assert expected_path in str(exc_info.value)
        assert path.read_text(encoding="utf-8") == "preserved"


def test_daemon_canonicalization_does_not_coerce_non_string_keys() -> None:
    with pytest.raises(ValueError) as exc_info:
        canonicalize({"metadata": {7: "seven"}})

    assert '$["metadata"]' in str(exc_info.value)
    assert "JSON object keys must be strings" in str(exc_info.value)
