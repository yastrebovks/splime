from __future__ import annotations

import copy
import hashlib
import json
import math
import random
from pathlib import Path
from typing import Any, cast

import pytest

from spl import Deployment, lift
from spl.core import manifest as m_manifest
from spl.core import resume as m_resume
from spl.core._common import Run
from spl.core.adapter_compat import _is_json_native_type_name
from spl.core.entities.adapter import BUILTIN_JSON_ADAPTER, JSON_NATIVE_TYPES
from spl.core.entities.node import DEFAULT_PORT
from spl.core.fingerprint import inline_value_sha256
from spl.core.json_contract import JSON_SCALARS, compact_json_fits, dumps, is_json_value, validate_json_value

_FIXTURE_SHA256 = "94c4dad7ec0c5e8e5f265f91531abfc4c17324be0cc5741204c9ea7ec6d1977c"
_CALLS: dict[str, int] = {}


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


def _none_source() -> None:
    _CALLS["source"] = _CALLS.get("source", 0) + 1
    return None


def _consume_none(value: Any) -> str:
    _CALLS["consumer"] = _CALLS.get("consumer", 0) + 1
    return "saw-null" if value is None else "wrong"


def _consume_json_input(payload: Any) -> str:
    _CALLS["json-input"] = _CALLS.get("json-input", 0) + 1
    return "accepted"


def _none_pipeline() -> Any:
    lift_any = cast(Any, lift)
    source = lift_any(_none_source).alias("source")
    return lift_any(_consume_none).bind(value=source).alias("consumer").render("none_contract")


def _manifest(run: Run) -> dict[str, Any]:
    assert run.manifest_path is not None
    return cast(dict[str, Any], json.loads(run.manifest_path.read_text(encoding="utf-8")))


def _node_by_alias(manifest: dict[str, Any], alias: str) -> dict[str, Any]:
    return cast(dict[str, Any], next(node for node in manifest["nodes"].values() if node["alias"] == alias))


def test_shared_json_value_fixture_matches_recursive_contract() -> None:
    for case in _fixture_cases():
        value = _fixture_value(case)
        if case["accepted"]:
            assert is_json_value(value), case["id"]
            validate_json_value(value)
            assert dumps(value) == case["serialized"]
            if case["construct"] == "negative_zero":
                assert math.copysign(1.0, value) == -1.0
            continue

        assert not is_json_value(value), case["id"]
        with pytest.raises(ValueError) as exc_info:
            validate_json_value(value)
        assert case["path"] in str(exc_info.value), case["id"]
        with pytest.raises(ValueError):
            dumps(value)


def test_contract_rejects_unsupported_and_circular_values_with_paths() -> None:
    circular: list[Any] = []
    circular.append(circular)

    assert not is_json_value((1, 2))
    assert not is_json_value(b"bytes")
    assert not is_json_value(circular)
    with pytest.raises(ValueError, match=r"\$\[0\].*circular"):
        validate_json_value(circular)
    with pytest.raises(ValueError, match=r"\$\[\"payload\"\].*builtins.bytes"):
        validate_json_value({"payload": b"bytes"})
    with pytest.raises(ValueError, match="builtins.tuple"):
        inline_value_sha256((1, 2))


def test_contract_formatting_knobs_preserve_existing_writer_bytes(tmp_path: Path) -> None:
    value = {"unicode": "Γειά", "nested": {"ok": True}}
    assert dumps(value, ensure_ascii=True, separators=None) == json.dumps(value, ensure_ascii=True, sort_keys=True)

    path = tmp_path / "manifest.json"
    m_manifest.atomic_write_json(path, value)
    assert path.read_text(encoding="utf-8") == json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


@pytest.mark.parametrize(
    "value",
    [
        None,
        False,
        True,
        -(1 << 53) + 1,
        (1 << 53) - 1,
        -(1 << 53),
        1 << 53,
        -123456789012345678901234567890,
        123456789012345678901234567890,
        -0.0,
        0.1,
        -1e20,
        1e20,
        1e-300,
        float((1 << 53) - 1),
        "",
        '\x00\x01\b\f\n\r\t\x1f"\\/',
        "Γειά 😀 \u2028 \u2029",
        [None, True, -0.0, "雪"],
        {"z": [1, 2], "a\n😀": {'quote"': "backslash\\"}},
    ],
)
def test_compact_json_fits_matches_compact_utf8_boundaries(value: Any) -> None:
    encoded = dumps(value, ensure_ascii=False, sort_keys=False).encode("utf-8")

    assert not compact_json_fits(value, len(encoded) - 1)
    assert compact_json_fits(value, len(encoded))
    assert compact_json_fits(value, len(encoded) + 1)


def test_compact_json_fits_matches_randomized_valid_trees() -> None:
    generator = random.Random(0x5A17)
    string_characters = ["\x00", "\b", "\t", "\n", "\x1f", '"', "\\", "/", "a", "Γ", "雪", "😀", "\u2028"]
    scalar_values: list[Any] = [
        None,
        False,
        True,
        -(1 << 53) + 1,
        -17,
        0,
        (1 << 53) - 1,
        -(1 << 53),
        1 << 53,
        -123456789012345678901234567890,
        123456789012345678901234567890,
        -0.0,
        0.1,
        -1.25e-100,
        -1e20,
        1e20,
        float((1 << 53) - 1),
    ]

    def random_string() -> str:
        return "".join(generator.choice(string_characters) for _ in range(generator.randrange(16)))

    def random_value(depth: int) -> Any:
        if depth == 0 or generator.randrange(3) == 0:
            if generator.randrange(3) == 0:
                return random_string()
            return generator.choice(scalar_values)
        if generator.choice((False, True)):
            return [random_value(depth - 1) for _ in range(generator.randrange(5))]
        result: dict[str, Any] = {}
        for index in range(generator.randrange(5)):
            result["{}:{}".format(index, random_string())] = random_value(depth - 1)
        return result

    for _ in range(250):
        value = random_value(4)
        encoded_size = len(dumps(value, ensure_ascii=False, sort_keys=False).encode("utf-8"))
        assert compact_json_fits(value, encoded_size)
        assert not compact_json_fits(value, encoded_size - 1)


def test_compact_json_fits_allows_reused_containers_but_rejects_cycles() -> None:
    shared = [{"unicode": "😀"}]
    reused = [shared, shared]
    encoded_size = len(dumps(reused, ensure_ascii=False, sort_keys=False).encode("utf-8"))
    assert compact_json_fits(reused, encoded_size)

    circular: list[Any] = []
    circular.append(circular)
    with pytest.raises(ValueError, match=r"\$\[0\].*circular"):
        compact_json_fits(circular, 1_000_000)


def test_compact_json_fits_validates_every_reached_value_with_canonical_errors() -> None:
    circular: list[Any] = []
    circular.append(circular)
    invalid_values: list[Any] = [
        object(),
        (1, 2),
        {1: "non-string key"},
        {"nested": b"bytes"},
        "\ud800",
        {"\udfff": None},
        {"nested": ["\ud800"]},
        float("nan"),
        float("inf"),
        circular,
    ]

    for value in invalid_values:
        with pytest.raises(ValueError) as canonical_error:
            validate_json_value(value)
        with pytest.raises(ValueError) as bounded_error:
            compact_json_fits(value, 1_000_000)
        assert str(bounded_error.value) == str(canonical_error.value)


def test_compact_json_fits_stops_before_unreached_invalid_remainder() -> None:
    circular: list[Any] = []
    circular.append(circular)
    value: list[Any] = ["x" * 10_000, circular, "\ud800", object()]

    with pytest.raises(ValueError, match="circular"):
        validate_json_value(value)
    assert not compact_json_fits(value, 4)


def test_compact_json_fits_stops_at_budget_before_python_recursion_limit() -> None:
    value: Any = None
    for _ in range(2_000):
        value = [value]

    assert not compact_json_fits(value, 16)
    with pytest.raises(RecursionError):
        compact_json_fits(value, 1_000_000)


@pytest.mark.parametrize("max_bytes", [-1, True, 1.5, "1"])
def test_compact_json_fits_rejects_invalid_byte_budgets(max_bytes: Any) -> None:
    with pytest.raises(ValueError, match="non-negative integer"):
        compact_json_fits(None, max_bytes)


def test_none_is_json_native_for_runtime_static_checks_and_builtin_adapter(tmp_path: Path) -> None:
    assert type(None) in JSON_SCALARS
    assert type(None) in JSON_NATIVE_TYPES
    assert _is_json_native_type_name("None")
    assert _is_json_native_type_name("NoneType")
    assert _is_json_native_type_name("builtins.NoneType")

    path = tmp_path / "none.json"
    BUILTIN_JSON_ADAPTER.save(str(path), None)
    assert path.read_text(encoding="utf-8") == "null"
    assert BUILTIN_JSON_ADAPTER.load(str(path)) is None


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), "\ud800"])
def test_builtin_json_adapter_rejects_before_truncating_existing_file(
    invalid: Any,
    tmp_path: Path,
) -> None:
    path = tmp_path / "preserved.json"
    path.write_text("preserved", encoding="utf-8")

    with pytest.raises(ValueError):
        BUILTIN_JSON_ADAPTER.save(str(path), invalid)

    assert path.read_text(encoding="utf-8") == "preserved"


@pytest.mark.parametrize("keep", [False, True])
def test_nested_nonfinite_input_is_rejected_before_callback_for_every_keep_policy(
    keep: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _CALLS.clear()
    lift_any = cast(Any, lift)
    pipeline = lift_any(_consume_json_input).alias("consumer").render("json_input_contract")
    run = cast(
        Run,
        Deployment(pipeline).run(
            keep=keep,
            payload={"deep": [float("nan")]},
        ),
    )

    with pytest.raises(ValueError, match=r'\$\["deep"\]\[0\].*non-finite'):
        run.value("consumer")

    assert _CALLS == {}
    if keep:
        manifest = _manifest(run)
        assert manifest["status"] == "failed"
        assert _node_by_alias(manifest, "consumer")["status"] == "failed"
    else:
        assert run.manifest_path is None


def test_none_output_records_null_and_old_resume_fails_loudly_then_recomputes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _CALLS.clear()
    pipeline = _none_pipeline()
    run = cast(Run, Deployment(pipeline).run(keep=True))
    with run:
        assert run.value("consumer") == "saw-null"

    current_manifest = _manifest(run)
    source_record = _node_by_alias(current_manifest, "source")
    consumer_record = _node_by_alias(current_manifest, "consumer")
    null_record = source_record["outputs"][DEFAULT_PORT]
    assert null_record == {
        "kind": "json",
        "tag": "json",
        "value": None,
        "sha256": inline_value_sha256(None),
    }
    assert consumer_record["inputs"]["value"] == null_record
    assert run._artifacts_dir is None
    assert _CALLS == {"source": 1, "consumer": 1}

    source = pipeline.aliases["source"]
    consumer = pipeline.aliases["consumer"]
    old_record = m_manifest.unfreezable_record("value was not materialized as an artifact")
    old_consumer_fingerprint = run._node_fingerprint(
        consumer,
        {"value": old_record},
        cast(dict[str, Any], consumer_record["adapters"]),
    )
    assert old_consumer_fingerprint != consumer_record["fingerprint"]["sha256"]
    assert (
        run._node_fingerprint(
            source,
            cast(dict[str, Any], source_record["inputs"]),
            cast(dict[str, Any], source_record["adapters"]),
        )
        == source_record["fingerprint"]["sha256"]
    )

    assert run._manifest_writer is not None
    old_manifest = run._manifest_writer.data
    _node_by_alias(old_manifest, "source")["outputs"][DEFAULT_PORT] = old_record
    _node_by_alias(old_manifest, "consumer")["inputs"]["value"] = old_record
    _node_by_alias(old_manifest, "consumer")["fingerprint"]["sha256"] = old_consumer_fingerprint
    run._manifest_writer.write()

    with pytest.raises(m_resume.ResumeValidationError, match="source:default is unfreezable.*from_='source'"):
        run.resume(from_="consumer", keep=True)

    recomputed = cast(Run, run.resume(from_="source", keep=True))
    with recomputed:
        assert recomputed.value("consumer") == "saw-null"
    assert _CALLS == {"source": 2, "consumer": 2}
    assert _node_by_alias(_manifest(recomputed), "source")["outputs"][DEFAULT_PORT]["value"] is None

    frozen = cast(Run, recomputed.resume(from_=[], keep=True))
    with frozen:
        assert frozen.value("consumer") == "saw-null"
    frozen_manifest = _manifest(frozen)
    assert _node_by_alias(frozen_manifest, "source")["status"] == "frozen"
    assert _node_by_alias(frozen_manifest, "consumer")["status"] == "frozen"
    assert _CALLS == {"source": 2, "consumer": 2}
