from __future__ import annotations

import builtins
import copy
import hashlib
import importlib
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any

import pytest

from spl.core.source_preparation import (
    IR_HASH_DOMAIN,
    MAX_SOURCE_DOCUMENT_BYTES,
    PREPARED_HASH_DOMAIN,
    PREPARED_OBJECT_SCHEMA,
    PREPARE_SOURCE_REQUEST_SCHEMA,
    PREPARE_SOURCE_RESULT_SCHEMA,
    SOURCE_HASH_DOMAIN,
    SEMANTIC_PORTS_SCHEMA,
    PrepareSourceContractError,
    PreparationCancelled,
    canonical_json_bytes,
    domain_hash,
    prepare_source,
    prepared_manifest,
    validate_prepared_manifest_and_ir,
    validate_prepared_object,
)


FIXTURES = Path(__file__).with_name("fixtures")
REPOSITORY_ROOT = Path(__file__).parents[3]


def _fixture_text(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _fixture_json(name: str) -> dict[str, Any]:
    return json.loads(_fixture_text(name))


def _request(
    source_name: str = "function_source.py",
    plan_name: str = "function_plan.json",
    *,
    logical_path: str | None = None,
    base: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source_path = logical_path or f"spl_generated/{source_name}"
    plan = _fixture_json(plan_name)
    return {
        "schema": PREPARE_SOURCE_REQUEST_SCHEMA,
        "schema_version": 1,
        "sources": [
            {
                "logical_path": source_path,
                "language": "python",
                "text": _fixture_text(source_name),
            }
        ],
        "proposal_ir": plan,
        "entrypoints": [plan["name"]],
        "environment_request": None,
        "runtime_request": {"mode": "venv"},
        "target": {
            "python_language": "3.13",
            "spl_ir_schema": "splime.object-ir/v1",
            "preparation_protocol": 1,
        },
        "base": base,
    }


def _ready(request: dict[str, Any]) -> dict[str, Any]:
    result = prepare_source(request)
    assert result["schema"] == PREPARE_SOURCE_RESULT_SCHEMA
    assert result["status"] == "ready", result["diagnostics"]
    assert result["prepared"] is not None
    return result["prepared"]


def _manual_source_hash(source_documents: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    digest.update(b"splime.source/v1\0")
    for document in sorted(source_documents, key=lambda item: item["logical_path"]):
        path = document["logical_path"].encode("utf-8")
        text = document["normalized_text"].encode("utf-8")
        digest.update(len(path).to_bytes(8, "big"))
        digest.update(path)
        digest.update(len(text).to_bytes(8, "big"))
        digest.update(text)
    return f"sha256:{digest.hexdigest()}"


def test_function_preparation_uses_frozen_contract_and_hash_domains() -> None:
    prepared = _ready(_request())

    assert prepared["schema"] == PREPARED_OBJECT_SCHEMA
    assert prepared["schema"] == "splime.prepared-object/v1"
    assert prepared["source_hash"] == _manual_source_hash(prepared["source_documents"])
    assert prepared["source_hash"].startswith("sha256:")
    assert prepared["ir_hash"] == domain_hash(IR_HASH_DOMAIN, prepared["canonical_ir"])
    assert SOURCE_HASH_DOMAIN == "splime.source/v1"
    assert IR_HASH_DOMAIN == "splime.ir/v1"
    assert PREPARED_HASH_DOMAIN == "splime.prepared/v1"

    manifest_without_hash = prepared_manifest(prepared, include_prepared_hash=False)
    assert prepared["prepared_hash"] == domain_hash(PREPARED_HASH_DOMAIN, manifest_without_hash)
    assert "object_hash" not in prepared
    assert "registry_content_hash" not in prepared
    validate_prepared_object(prepared)


def test_semantic_port_contract_preserves_reviewed_source_and_overlays_only_port_metadata() -> None:
    source = """\
import pandas as pd

def cat_encoding(df, dict_categories: dict, unique_count: int = 10):
    categorical_columns = list(df.loc[:, df.dtypes == object].columns)
    return df
"""
    request = _request()
    request["sources"][0]["text"] = source
    request["proposal_ir"] = {
        **request["proposal_ir"],
        "name": "cat_encoding",
        "dependencies": [{"package": "pandas", "version": "3.0.3", "modules": ["pandas"]}],
    }
    request["entrypoints"] = ["cat_encoding"]
    legacy = _ready(copy.deepcopy(request))
    request["proposal_ir"]["semantic_ports"] = {
        "schema": SEMANTIC_PORTS_SCHEMA,
        "schema_version": 1,
        "inputs": [{"name": "df", "type": "pandas.DataFrame"}],
        "outputs": [{"name": "default", "type": "pandas.DataFrame"}],
    }
    prepared = _ready(request)

    assert prepared["source_documents"] == legacy["source_documents"]
    assert prepared["source_hash"] == legacy["source_hash"]
    legacy_function = next(item for item in legacy["canonical_ir"]["functions"] if item["name"] == "cat_encoding")
    prepared_function = next(item for item in prepared["canonical_ir"]["functions"] if item["name"] == "cat_encoding")
    assert prepared_function["body"] == legacy_function["body"]
    assert prepared_function["inputs"][0]["type"] == "pandas.DataFrame"
    assert prepared_function["outputs"] == [{"name": "default", "type": "pandas.DataFrame"}]
    assert prepared["signature"]["inputs"][0]["type"] == "pandas.DataFrame"
    assert prepared["signature"]["outputs"][0]["type"] == "pandas.DataFrame"
    assert prepared["normalized_plan"]["semantic_ports"] == request["proposal_ir"]["semantic_ports"]
    assert "adapter_id" not in json.dumps(prepared["canonical_ir"])
    validate_prepared_object(prepared)


def test_semantic_port_contract_rejects_unknown_conflicting_and_pipeline_ports() -> None:
    request = _request()
    request["proposal_ir"]["semantic_ports"] = {
        "schema": SEMANTIC_PORTS_SCHEMA,
        "schema_version": 1,
        "inputs": [{"name": "left", "type": "str"}],
        "outputs": [],
    }
    conflict = prepare_source(request)
    assert conflict["status"] == "invalid"
    assert [item["code"] for item in conflict["diagnostics"]] == ["plan.semantic_type_conflict"]

    request["proposal_ir"]["semantic_ports"]["inputs"] = [{"name": "missing", "type": "str"}]
    unknown = prepare_source(request)
    assert unknown["status"] == "invalid"
    assert [item["code"] for item in unknown["diagnostics"]] == ["plan.semantic_input_missing"]

    pipeline = _request("pipeline_source.py", "pipeline_plan.json")
    pipeline["proposal_ir"]["semantic_ports"] = {
        "schema": SEMANTIC_PORTS_SCHEMA,
        "schema_version": 1,
        "inputs": [],
        "outputs": [{"name": "default", "type": "str"}],
    }
    with pytest.raises(PrepareSourceContractError) as raised:
        prepare_source(pipeline)
    assert raised.value.code == "prepare.semantic_ports_kind"


def test_pipeline_preparation_is_closed_and_deterministic() -> None:
    request = _request("pipeline_source.py", "pipeline_plan.json")
    first = _ready(request)

    reordered = copy.deepcopy(request)
    reordered["proposal_ir"]["nodes"].reverse()
    reordered["proposal_ir"]["edges"].reverse()
    second = _ready(reordered)

    assert canonical_json_bytes(first) == canonical_json_bytes(second)
    assert first["signature"]["kind"] == "pipeline"
    assert first["signature"]["inputs"] == [
        {
            "name": "value",
            "type": "int",
            "required": True,
            "default": None,
            "has_default": False,
        }
    ]
    assert [item["name"] for item in first["signature"]["outputs"]] == ["result"]


def test_pickle_adapter_is_preserved_with_a_trusted_only_warning() -> None:
    prepared = _ready(_request("xgboost_full_source.py", "xgboost_full_plan.json"))

    assert {
        "key": "sympy.core.expr.Expr@pickle",
        "python_type": "sympy.core.expr.Expr",
        "format": "pickle",
        "save_symbol": "save_sympy_pickle",
        "load_symbol": "load_sympy_pickle",
    } in prepared["normalized_plan"]["adapters"]
    assert [item["code"] for item in prepared["diagnostics"]] == ["plan.adapter_pickle_trusted_only"]


def test_xgboost_representative_static_source_is_supported_without_execution() -> None:
    """The approved Pilot fixture needs aliases, nested helpers, lambdas, and local imports."""

    prepared = _ready(_request("xgboost_source.py", "xgboost_plan.json"))

    assert prepared["canonical_ir"]["name"] == "xgboost_pipeline"
    assert prepared["canonical_ir"]["imports"] == [{"kind": "module", "module": "xgboost", "alias": "xgb"}]
    assert prepared["dependencies"] == [{"package": "xgboost", "version": "3.3.0", "modules": ["xgboost"]}]


def test_lgbm_hyperopt_representative_pipeline_is_fully_bound_and_static() -> None:
    """Prepare the synthetic behavior fixture without importing or executing ML code."""

    prepared = _ready(_request("lgbm_hyperopt_source.py", "lgbm_hyperopt_plan.json"))

    assert prepared["canonical_ir"]["name"] == "lgbm_hyperopt_pipeline"
    assert [item["function"] for item in prepared["canonical_ir"]["pipeline"]["nodes"]] == [
        "hyperopt_objective",
        "prepare_training",
        "prepare_validation",
        "run_hyperopt",
        "train_lgbm",
    ]
    assert prepared["dependencies"] == [
        {"package": "hyperopt", "version": "0.2.7", "modules": ["hyperopt"]},
        {"package": "lightgbm", "version": "4.6.0", "modules": ["lightgbm"]},
        {"package": "numpy", "version": "2.3.2", "modules": ["numpy"]},
        {"package": "pandas", "version": "2.3.1", "modules": ["pandas"]},
        {"package": "scikit-learn", "version": "1.7.1", "modules": ["sklearn"]},
    ]
    input_names = [item["name"] for item in prepared["signature"]["inputs"]]
    assert "df" not in input_names
    assert "prepare_training__df" in input_names
    assert "prepare_validation__df" in input_names
    assert prepared["signature"]["outputs"][0]["name"] == "best_params"


def test_cross_document_implicit_function_reference_is_rejected() -> None:
    request = _request()
    request["sources"] = [
        {
            "logical_path": "spl_generated/root.py",
            "language": "python",
            "text": "def add_values(left: int, right: int = 1) -> int:\n    return helper(left)\n",
        },
        {
            "logical_path": "spl_generated/helper.py",
            "language": "python",
            "text": "def helper(value: int) -> int:\n    return value\n",
        },
    ]

    result = prepare_source(request)

    assert result["status"] == "invalid"
    assert result["prepared"] is None
    assert {item["code"] for item in result["diagnostics"]} == {"source.unresolved_symbol"}


def test_full_xgboost_function_node_pipeline_fixture_prepares_exact_graph() -> None:
    prepared = _ready(_request("xgboost_full_source.py", "xgboost_full_plan.json"))

    pipeline = prepared["canonical_ir"]["pipeline"]
    assert prepared["canonical_ir"]["name"] == "xgb_experiment"
    assert len(pipeline["nodes"]) == 8
    assert len(pipeline["edges"]) == 19
    assert [item["name"] for item in pipeline["outputs"]] == ["metrics", "report"]
    assert [item["name"] for item in pipeline["inputs"]] == [
        "formula_depth",
        "n_features",
        "n_samples",
        "seed",
    ]
    assert prepared["runtime_request"] == {"mode": "venv"}
    assert prepared["environment_request"] is None
    assert prepared["serialized_ir"]["media_type"] == "application/vnd.splime.yaml"


def test_dynamic_source_is_invalid_and_never_returns_partial_prepared_object() -> None:
    request = _request()
    request["sources"][0]["text"] = (
        "def add_values(left: int, right: int = 1) -> int:\n    return eval('left + right')\n"
    )

    result = prepare_source(request)

    assert result["status"] == "invalid"
    assert result["prepared"] is None
    assert {item["code"] for item in result["diagnostics"]} == {"source.dynamic_call_unsupported"}


@pytest.mark.parametrize(
    ("body", "code"),
    [
        ("break", "source.break_outside_loop"),
        ("continue", "source.continue_outside_loop"),
        ("return [item async for item in values]", "source.async_comprehension_unsupported"),
        ("from __future__ import annotations\n    return left", "source.future_import_unsupported"),
    ],
)
def test_compile_time_only_invalidity_is_rejected_without_compilation(body: str, code: str) -> None:
    request = _request()
    request["sources"][0]["text"] = f"def add_values(left: int, right: int = 1) -> int:\n    {body}\n"

    result = prepare_source(request)

    assert result["status"] == "invalid"
    assert result["prepared"] is None
    assert code in {item["code"] for item in result["diagnostics"]}


def test_duplicate_nested_parameter_is_rejected_without_compilation() -> None:
    request = _request()
    request["sources"][0]["text"] = (
        "def add_values(left: int, right: int = 1) -> int:\n"
        "    def helper(value, value):\n"
        "        return value\n"
        "    return helper(left)\n"
    )

    result = prepare_source(request)

    assert result["status"] == "invalid"
    assert result["prepared"] is None
    assert {item["code"] for item in result["diagnostics"]} == {"source.duplicate_parameter"}


def test_malformed_request_unknown_field_is_a_closed_contract_error() -> None:
    request = _request()
    request["unexpected"] = True

    with pytest.raises(PrepareSourceContractError) as raised:
        prepare_source(request)

    assert raised.value.code == "request.fields"


def test_python_keywords_are_rejected_as_object_and_entrypoint_names() -> None:
    request = _request()
    request["proposal_ir"]["name"] = "for"
    request["entrypoints"] = ["for"]

    with pytest.raises(PrepareSourceContractError) as raised:
        prepare_source(request)

    assert raised.value.code in {"prepare.plan_name", "prepare.entrypoint"}


@pytest.mark.parametrize(
    ("logical_path", "plan_name"),
    [
        ("spl generated/function_source.py", "add_values"),
        ("spl_generated/fünction_source.py", "add_values"),
        ("spl_generated/function_source.py", "add_välues"),
    ],
)
def test_portable_v1_paths_and_identifiers_are_ascii_closed(
    logical_path: str,
    plan_name: str,
) -> None:
    request = _request(logical_path=logical_path)
    request["proposal_ir"]["name"] = plan_name
    request["entrypoints"] = [plan_name]

    with pytest.raises(PrepareSourceContractError) as raised:
        prepare_source(request)

    assert raised.value.code in {
        "prepare.logical_path",
        "prepare.plan_name",
    }


def test_prepared_source_ir_and_manifest_tampering_fail_closed() -> None:
    prepared = _ready(_request())

    source_tamper = copy.deepcopy(prepared)
    source_tamper["source_documents"][0]["normalized_text"] += "# changed\n"
    with pytest.raises(PrepareSourceContractError):
        validate_prepared_object(source_tamper)

    ir_tamper = copy.deepcopy(prepared)
    ir_tamper["canonical_ir"]["functions"][0]["body"] = "return 999"
    with pytest.raises(PrepareSourceContractError):
        validate_prepared_object(ir_tamper)

    manifest = prepared_manifest(prepared)
    canonical_ir = copy.deepcopy(prepared["canonical_ir"])
    canonical_ir["functions"][0]["body"] = "return 999"
    with pytest.raises(PrepareSourceContractError):
        validate_prepared_manifest_and_ir(manifest, canonical_ir)

    plan_tamper = _ready(_request("pipeline_source.py", "pipeline_plan.json"))
    plan_tamper["normalized_plan"]["nodes"][0]["runtime"] = "venv-subprocess"
    plan_tamper["plan_hash"] = domain_hash("splime.plan/v1", plan_tamper["normalized_plan"])
    plan_tamper["prepared_hash"] = domain_hash(
        PREPARED_HASH_DOMAIN,
        prepared_manifest(plan_tamper, include_prepared_hash=False),
    )
    with pytest.raises(PrepareSourceContractError) as raised:
        validate_prepared_object(plan_tamper)
    assert raised.value.code == "prepared.source_plan_ir_binding"


def test_base_identity_is_bound_into_prepared_hash_and_revalidated() -> None:
    base = {
        "kind": "object",
        "owner_id": "owner_base_001",
        "library_id": "library_base_001",
        "object_id": "object_base_001",
        "version_id": "version_base_007",
        "version": 7,
        "content_hash": "sha256:" + "a" * 64,
    }
    without_base = _ready(_request())
    with_base = _ready(_request(base=base))

    assert with_base["base"] == base
    assert prepared_manifest(with_base)["base"] == base
    assert with_base["prepared_hash"] != without_base["prepared_hash"]

    tampered = copy.deepcopy(with_base)
    tampered["base"]["version_id"] = "version_base_008"
    with pytest.raises(PrepareSourceContractError):
        validate_prepared_object(tampered)

    unsafe_version = _request(base={**base, "version": 9_007_199_254_740_992})
    with pytest.raises(PrepareSourceContractError) as raised:
        prepare_source(unsafe_version)
    assert raised.value.code == "prepare.base_version"


def test_newline_and_comment_identity_rules_are_deterministic() -> None:
    original_request = _request()
    crlf_request = copy.deepcopy(original_request)
    crlf_request["sources"][0]["text"] = crlf_request["sources"][0]["text"].replace("\n", "\r\n")
    with_comment_request = copy.deepcopy(original_request)
    with_comment_request["sources"][0]["text"] += "# reviewed comment\n"

    original = _ready(original_request)
    crlf = _ready(crlf_request)
    with_comment = _ready(with_comment_request)

    assert canonical_json_bytes(original) == canonical_json_bytes(crlf)
    assert original["source_hash"] != with_comment["source_hash"]
    assert original["ir_hash"] == with_comment["ir_hash"]
    assert original["prepared_hash"] != with_comment["prepared_hash"]


def test_canonical_json_uses_the_frozen_cross_language_number_profile() -> None:
    assert canonical_json_bytes(
        {
            "fixed": 0.000001,
            "integer": 2.0,
            "negative_zero": -0.0,
            "scientific": 0.0000001,
            "tenth": 0.1,
        }
    ) == (b'{"fixed":0.000001,"integer":2,"negative_zero":0,"scientific":1e-7,"tenth":0.1}')

    with pytest.raises(PrepareSourceContractError) as raised:
        canonical_json_bytes({"unsafe": 10**20})
    assert raised.value.code == "prepare.json_number_range"


def test_integral_float_python_default_is_rejected_instead_of_changing_type() -> None:
    request = _request()
    request["sources"][0]["text"] = (
        "def add_values(left: int, right: float = 2.0) -> int:\n    return left + int(right)\n"
    )

    result = prepare_source(request)

    assert result["status"] == "invalid"
    assert result["prepared"] is None
    assert {item["code"] for item in result["diagnostics"]} == {"source.default_unsupported"}


def test_cross_process_serialization_is_byte_identical() -> None:
    request = _request("pipeline_source.py", "pipeline_plan.json")
    script = """
import json
import sys
from spl.core.source_preparation import canonical_json_bytes, prepare_source
request = json.load(sys.stdin)
sys.stdout.buffer.write(canonical_json_bytes(prepare_source(request)))
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "src")

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPOSITORY_ROOT,
        env=environment,
        input=json.dumps(request).encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        timeout=20,
    )

    assert completed.stdout == canonical_json_bytes(prepare_source(request))
    assert completed.stderr == b""


def test_preparation_does_not_use_io_network_process_clock_randomness_or_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _request()

    def denied(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("forbidden side effect")

    monkeypatch.setattr(builtins, "open", denied)
    monkeypatch.setattr(builtins, "eval", denied)
    monkeypatch.setattr(builtins, "exec", denied)
    monkeypatch.setattr(Path, "open", denied)
    monkeypatch.setattr(Path, "read_text", denied)
    monkeypatch.setattr(Path, "write_text", denied)
    monkeypatch.setattr(importlib, "import_module", denied)
    monkeypatch.setattr(subprocess, "run", denied)
    monkeypatch.setattr(subprocess, "Popen", denied)
    monkeypatch.setattr(socket, "create_connection", denied)
    monkeypatch.setattr(urllib.request, "urlopen", denied)
    monkeypatch.setattr(time, "time", denied)
    monkeypatch.setattr(uuid, "uuid4", denied)

    before = tuple(tmp_path.iterdir())
    prepared = _ready(request)
    after = tuple(tmp_path.iterdir())

    assert before == after == ()
    assert prepared["diagnostics"] == []


def test_unicode_source_map_columns_are_codepoint_based() -> None:
    source = 'def transform(value: str) -> str:\n    return value + "π"\n'
    request = _request()
    request["sources"][0]["text"] = source
    request["proposal_ir"]["name"] = "transform"
    request["entrypoints"] = ["transform"]

    prepared = _ready(request)
    [symbol] = prepared["source_map"]["symbols"]

    assert symbol["span"]["end_line"] == 1
    assert symbol["span"]["end_column"] == len('    return value + "π"')


def test_source_size_bound_and_cancellation_fail_without_partial_result() -> None:
    oversized = _request()
    oversized["sources"][0]["text"] = "x" * (MAX_SOURCE_DOCUMENT_BYTES + 1)
    with pytest.raises(PrepareSourceContractError) as raised:
        prepare_source(oversized)
    assert raised.value.code == "source.text.string"

    calls = 0

    def cancel_during_work() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 3

    with pytest.raises(PreparationCancelled):
        prepare_source(_request("pipeline_source.py", "pipeline_plan.json"), cancelled=cancel_during_work)

    with pytest.raises(PreparationCancelled):
        prepare_source(_request(), cancelled=lambda: True)


@pytest.mark.parametrize(
    "body, code",
    [
        ('api_token = "sk-fixture-secret-123456789"\n    return left + right', "source.secret_like_literal"),
        ('output_path = "/Users/example/private.txt"\n    return left + right', "source.host_path_literal"),
    ],
)
def test_secret_like_and_host_path_literals_fail_without_echo(body: str, code: str) -> None:
    request = _request()
    request["sources"][0]["text"] = "def add_values(left: int, right: int = 1) -> int:\n    " + body + "\n"

    result = prepare_source(request)

    assert result["status"] == "invalid"
    assert result["prepared"] is None
    assert code in {item["code"] for item in result["diagnostics"]}
    assert all("sk-fixture" not in item["message"] for item in result["diagnostics"])


def test_serialized_ir_is_the_accepted_yaml_compatibility_artifact() -> None:
    prepared = _ready(_request("pipeline_source.py", "pipeline_plan.json"))

    assert prepared["serialized_ir"]["media_type"] == "application/vnd.splime.yaml"
    assert any(
        item["logical_path"].endswith(".spl.yaml") and item["media_type"] == "application/vnd.splime.yaml"
        for item in prepared["generated_files"]
    )
