from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from spl.core.source_preparation import (
    IR_HASH_DOMAIN,
    PREPARED_HASH_DOMAIN,
    domain_hash,
    prepare_source,
    prepared_manifest,
)
from spl.daemon import metadata as daemon_metadata
from spl.daemon.prepared_validation import (
    MAX_REQUEST_BYTES,
    PreparedValidationError,
    PreparedValidationRequest,
    parse_prepared_validation_request,
    validate_prepared_request,
    validate_prepared_validation_result,
)
from spl.daemon.store import RegistryStore

FIXTURES = Path(__file__).parents[1] / "core" / "prompt16" / "fixtures"
VALIDATED_AT = "2026-08-02T12:00:00.000Z"


def _prepared(
    source_name: str = "function_source.py",
    plan_name: str = "function_plan.json",
    *,
    environment_request: dict[str, str] | None = None,
    base: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = json.loads((FIXTURES / plan_name).read_text(encoding="utf-8"))
    result = prepare_source(
        {
            "schema": "splime.prepare-source-request/v1",
            "schema_version": 1,
            "sources": [
                {
                    "logical_path": f"spl_generated/{source_name}",
                    "language": "python",
                    "text": (FIXTURES / source_name).read_text(encoding="utf-8"),
                }
            ],
            "proposal_ir": plan,
            "entrypoints": [plan["name"]],
            "environment_request": environment_request,
            "runtime_request": {"mode": "venv"},
            "target": {
                "python_language": "3.13",
                "spl_ir_schema": "splime.object-ir/v1",
                "preparation_protocol": 1,
            },
            "base": base,
        }
    )
    assert result["status"] == "ready", result["diagnostics"]
    return result["prepared"]


def _request(prepared: dict[str, Any], *, mode: str = "none") -> PreparedValidationRequest:
    return PreparedValidationRequest(
        prepared_manifest=prepared_manifest(prepared),
        canonical_ir=prepared["canonical_ir"],
        reference_mode=mode,  # type: ignore[arg-type]
        reference_max_age_seconds=(300 if mode == "cache_only" else None),
    )


def _wire(prepared: dict[str, Any], *, mode: str = "none") -> dict[str, Any]:
    return {
        "schema": "splime.object-validation-request/v1",
        "schema_version": 1,
        "prepared_manifest": prepared_manifest(prepared),
        "canonical_ir": prepared["canonical_ir"],
        "reference_resolution": {
            "mode": mode,
            "max_age_seconds": 300 if mode == "cache_only" else None,
        },
    }


def _registry_digest(store: RegistryStore) -> str:
    dump = "\n".join(store._conn.iterdump()).encode("utf-8")
    return hashlib.sha256(dump).hexdigest()


def _file_digest(root: Path) -> dict[str, tuple[int, int, str]]:
    return {
        str(path.relative_to(root)): (
            path.stat().st_mode,
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.mark.parametrize(
    ("source_name", "plan_name"),
    [
        ("function_source.py", "function_plan.json"),
        ("pipeline_source.py", "pipeline_plan.json"),
    ],
)
def test_prepared_validation_is_static_source_free_and_non_mutating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_name: str,
    plan_name: str,
) -> None:
    prepared = _prepared(source_name, plan_name)
    store = RegistryStore(tmp_path)
    before_registry = _registry_digest(store)
    before_files = _file_digest(tmp_path)

    def reject_compile(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("daemon prepared validation must not compile")

    monkeypatch.setattr(daemon_metadata, "compile", reject_compile, raising=False)
    try:
        result = validate_prepared_request(
            store,
            _request(prepared),
            validated_at=VALIDATED_AT,
        )
        repeated = validate_prepared_request(
            store,
            _request(prepared),
            validated_at="2026-08-02T12:00:01.000Z",
        )

        validate_prepared_validation_result(result)
        assert result["status"] == "valid"
        assert result["valid"] is True
        assert result["publish_ready"] is False
        assert result["validation_hash"] == repeated["validation_hash"]
        assert result["validated_at"] != repeated["validated_at"]
        serialized = json.dumps(result, sort_keys=True)
        assert prepared["source_documents"][0]["normalized_text"] not in serialized
        assert "body" not in result
        assert _registry_digest(store) == before_registry
        assert _file_digest(tmp_path) == before_files
    finally:
        store.close()


def test_full_xgboost_prepared_object_passes_the_same_static_subset(tmp_path: Path) -> None:
    prepared = _prepared("xgboost_full_source.py", "xgboost_full_plan.json")
    store = RegistryStore(tmp_path)
    try:
        result = validate_prepared_request(
            store,
            _request(prepared),
            validated_at=VALIDATED_AT,
        )

        assert result["status"] == "valid"
        assert result["valid"] is True
        assert result["publish_ready"] is False
    finally:
        store.close()


def test_xgboost_import_alias_is_bound_in_daemon_ir(tmp_path: Path) -> None:
    prepared = _prepared("xgboost_source.py", "xgboost_plan.json")
    store = RegistryStore(tmp_path)
    try:
        result = validate_prepared_request(
            store,
            _request(prepared),
            validated_at=VALIDATED_AT,
        )

        assert result["status"] == "valid"
        assert prepared["canonical_ir"]["imports"] == [{"kind": "module", "module": "xgboost", "alias": "xgb"}]
    finally:
        store.close()


def test_semantic_failure_is_closed_and_hash_tampering_is_an_error(tmp_path: Path) -> None:
    prepared = _prepared()
    invalid_ir = copy.deepcopy(prepared["canonical_ir"])
    invalid_ir["functions"][0]["body"] = "yield 1"
    invalid_manifest = prepared_manifest(prepared)
    invalid_manifest["ir_hash"] = domain_hash(IR_HASH_DOMAIN, invalid_ir)
    invalid_manifest["prepared_hash"] = ""
    invalid_manifest["prepared_hash"] = domain_hash(PREPARED_HASH_DOMAIN, invalid_manifest)
    store = RegistryStore(tmp_path)
    try:
        result = validate_prepared_request(
            store,
            PreparedValidationRequest(
                prepared_manifest=invalid_manifest,
                canonical_ir=invalid_ir,
                reference_mode="none",
                reference_max_age_seconds=None,
            ),
            validated_at=VALIDATED_AT,
        )
        assert result["status"] == "invalid"
        assert result["validation_hash"] is None
        assert result["diagnostics"] == [{"code": "ir_semantics_invalid", "severity": "error"}]

        tampered_manifest = copy.deepcopy(invalid_manifest)
        tampered_manifest["ir_hash"] = "sha256:" + "0" * 64
        with pytest.raises(PreparedValidationError) as error:
            validate_prepared_request(
                store,
                PreparedValidationRequest(
                    prepared_manifest=tampered_manifest,
                    canonical_ir=invalid_ir,
                    reference_mode="none",
                    reference_max_age_seconds=None,
                ),
            )
        assert error.value.code == "prepared_binding_mismatch"
    finally:
        store.close()


def test_base_and_environment_facts_are_exact_without_path_projection(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("prepared-env", sys.executable)
        first = store.register_object(
            "prepared_base",
            "prepared_base",
            "prepared-env",
            yaml_text="""\
- !DFunction
  name: prepared_base
  inputs: []
  outputs:
  - name: default
    type: int
  body: return 1
""",
        )
        base = {
            "kind": "object",
            "owner_id": first["owner_id"],
            "library_id": first["library"],
            "object_id": first["id"],
            "version_id": first["version_id"],
            "version": first["version"],
            "content_hash": f"sha256:{first['content_hash']}",
        }
        prepared = _prepared(
            environment_request={"ref": "prepared-env"},
            base=base,
        )
        current = validate_prepared_request(store, _request(prepared), validated_at=VALIDATED_AT)
        assert current["status"] == "valid"
        assert current["checks"]["base"] == "current"
        assert current["effective_environment"] == {
            "ref": "prepared-env",
            "state": "registered",
        }

        store.register_object(
            "prepared_base",
            "prepared_base",
            "prepared-env",
            yaml_text="""\
- !DFunction
  name: prepared_base
  inputs: []
  outputs:
  - name: default
    type: int
  body: return 2
""",
            object_id=first["id"],
            owner_id=first["owner_id"],
            library=first["library"],
        )
        stale = validate_prepared_request(store, _request(prepared), validated_at=VALIDATED_AT)
        assert stale["status"] == "stale"
        assert stale["diagnostics"] == [{"code": "base_reference_stale", "severity": "error"}]
        assert str(tmp_path) not in json.dumps(stale, sort_keys=True)

        missing_environment = _prepared(environment_request={"ref": "not-registered"})
        unavailable = validate_prepared_request(
            store,
            _request(missing_environment),
            validated_at=VALIDATED_AT,
        )
        assert unavailable["status"] == "reference_unavailable"
        assert unavailable["effective_environment"] == {
            "ref": "not-registered",
            "state": "unavailable",
        }
    finally:
        store.close()


def test_reference_modes_fail_closed_without_lookup(tmp_path: Path) -> None:
    prepared = _prepared()
    store = RegistryStore(tmp_path)
    try:
        for mode in ("cache_only", "online_read"):
            request = parse_prepared_validation_request(
                json.dumps(_wire(prepared, mode=mode), separators=(",", ":")).encode("utf-8")
            )
            assert request.reference_max_age_seconds == (300 if mode == "cache_only" else None)
            result = validate_prepared_request(
                store,
                request,
                validated_at=VALIDATED_AT,
            )
            assert result["status"] == "incompatible"
            assert result["checks"]["references"] == "unsupported"
            assert result["publish_ready"] is False
    finally:
        store.close()


@pytest.mark.parametrize(
    "raw",
    [
        b'{"schema":"x","schema":"y"}',
        b'{"value":NaN}',
        b'{"value":"\\ud800"}',
        b"not-json",
    ],
)
def test_request_parser_rejects_duplicate_nonfinite_unicode_and_malformed_json(raw: bytes) -> None:
    with pytest.raises(PreparedValidationError) as error:
        parse_prepared_validation_request(raw)
    assert error.value.code == "request_invalid"


def test_request_parser_is_closed_and_bounded() -> None:
    prepared = _prepared()
    raw = json.dumps(_wire(prepared), separators=(",", ":")).encode("utf-8")
    parsed = parse_prepared_validation_request(raw)
    assert parsed.reference_mode == "none"

    extra = _wire(prepared)
    extra["future"] = True
    with pytest.raises(PreparedValidationError) as error:
        parse_prepared_validation_request(json.dumps(extra).encode("utf-8"))
    assert error.value.code == "request_invalid"

    with pytest.raises(PreparedValidationError) as error:
        parse_prepared_validation_request(b"{" + b" " * MAX_REQUEST_BYTES + b"}")
    assert error.value.code == "body_too_large"
