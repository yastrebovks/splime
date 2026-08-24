#!/usr/bin/env python3
"""Compare every historical SPLClient contract with the 0.4.8 facade.

The historical probe document remains immutable raw evidence.  This tool
creates the separate compatibility verdict by exercising old call bindings
against the current signatures and comparing parameter, return/view, and
error contracts.  Added optional parameters are compatible; removing or
tightening any historical form is not.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
from pathlib import Path
from typing import Any

from probe_historical_api import BEHAVIOR_FORMS, build_snapshot


SCHEMA = "splime.historical_api_comparison.v1"
PUBLIC_VIEW_BASES = {
    "ObjectCatalog": "mapping",
    "ObjectList": "sequence",
    "ObjectTable": "mapping",
}


def _normalize_annotation(value: str) -> str:
    value = " ".join(value.strip().split())
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return value


def _type_atoms(value: str) -> set[str]:
    """Return top-level union atoms, expanding public view subclasses."""

    value = _normalize_annotation(value)
    if value == "<empty>":
        return {value}
    atoms: list[str] = []
    start = 0
    depth = 0
    for index, character in enumerate(value):
        if character in "[({":
            depth += 1
        elif character in "])}":
            depth -= 1
        elif character == "|" and depth == 0:
            atoms.append(value[start:index].strip())
            start = index + 1
    atoms.append(value[start:].strip())
    expanded: set[str] = set()
    view_bases = {
        "ObjectTable": "dict[str, Any]",
        "ObjectCatalog": "dict[str, Any]",
        "ObjectList": "list[dict[str, Any]]",
    }
    for atom in atoms:
        expanded.add(view_bases.get(atom, atom))
    return expanded


def _stable_default(value: Any) -> str:
    if value is inspect.Parameter.empty:
        return "<empty>"
    if value is None or isinstance(value, (bool, int, float, str)):
        return repr(value)
    return f"<{type(value).__module__}.{type(value).__qualname__}>"


def _shape_category(shape: Any) -> str | None:
    if not isinstance(shape, dict) or not isinstance(shape.get("type"), str):
        return None
    wrapper_base = PUBLIC_VIEW_BASES.get(_wrapper_name(shape["type"]))
    if wrapper_base is not None:
        return "wrapper"
    markers = [
        ("mapping_keys", "mapping"),
        ("sequence_length", "sequence"),
        ("scalar", "scalar"),
        ("attribute_names", "wrapper"),
    ]
    observed = [category for key, category in markers if key in shape]
    return observed[0] if len(observed) == 1 else None


def _wrapper_name(value: str) -> str:
    return value.rpartition(".")[2]


def _observed_return_contract(
    historical_behavior: Any,
    current_behavior: Any,
) -> tuple[bool, list[str]]:
    """Compare one deterministic public-call result without erasing shape."""

    failures: list[str] = []
    if not isinstance(historical_behavior, dict):
        return False, ["historical behavioral evidence is missing"]
    if historical_behavior.get("status") != "PASS":
        failures.append("historical behavioral probe did not pass")
    if not isinstance(current_behavior, dict):
        return False, [*failures, "current behavioral evidence is missing"]
    if current_behavior.get("status") != "PASS":
        failures.append("current behavioral probe did not pass")
    historical_call = historical_behavior.get("call")
    current_call = current_behavior.get("call")
    if not isinstance(historical_call, dict) or historical_call.get("status") != "PASS":
        failures.append("historical call result is missing or failed")
    if not isinstance(current_call, dict) or current_call.get("status") != "PASS":
        failures.append("current call result is missing or failed")
    if failures:
        return False, failures

    historical_shape = historical_call.get("return_shape")
    current_shape = current_call.get("return_shape")
    historical_category = _shape_category(historical_shape)
    current_category = _shape_category(current_shape)
    if historical_category is None or current_category is None:
        return False, ["observed return shape is missing or malformed"]
    assert isinstance(historical_shape, dict) and isinstance(current_shape, dict)
    historical_type = str(historical_shape["type"])
    current_type = str(current_shape["type"])
    historical_base = PUBLIC_VIEW_BASES.get(_wrapper_name(historical_type))
    current_base = PUBLIC_VIEW_BASES.get(_wrapper_name(current_type))
    if historical_category != current_category and not (
        historical_category in {"mapping", "sequence"} and current_category == "wrapper"
    ):
        return False, [f"return category changed from {historical_category} to {current_category}"]
    if historical_category in {"mapping", "sequence"} and current_category == "wrapper":
        if current_base != historical_category:
            failures.append("public wrapper is not covariant with the historical container")
        elif historical_category == "mapping":
            historical_keys = set(historical_shape.get("mapping_keys") or [])
            current_keys = set(current_shape.get("mapping_keys") or [])
            if not historical_keys.issubset(current_keys):
                failures.append("mapping/view lost required keys")
        elif historical_shape.get("sequence_length") != current_shape.get("sequence_length"):
            failures.append("deterministic sequence shape changed")
        return not failures, failures
    if historical_category == "wrapper":
        if current_category != "wrapper" or _wrapper_name(historical_type) != _wrapper_name(current_type):
            failures.append("public wrapper type changed")
        historical_attributes = set(historical_shape.get("attribute_names") or [])
        current_attributes = set(current_shape.get("attribute_names") or [])
        if not historical_attributes.issubset(current_attributes):
            failures.append("public wrapper lost required attributes")
        if historical_base == "mapping":
            historical_keys = set(historical_shape.get("mapping_keys") or [])
            current_keys = set(current_shape.get("mapping_keys") or [])
            if not historical_keys.issubset(current_keys):
                failures.append("mapping/view lost required keys")
        elif historical_base == "sequence" and historical_shape.get("sequence_length") != current_shape.get(
            "sequence_length"
        ):
            failures.append("deterministic sequence shape changed")
    else:
        if historical_category != current_category:
            return False, [f"return category changed from {historical_category} to {current_category}"]
        if historical_type != current_type:
            failures.append("observed return type changed")
        if historical_category == "mapping":
            historical_keys = set(historical_shape.get("mapping_keys") or [])
            current_keys = set(current_shape.get("mapping_keys") or [])
            if not historical_keys.issubset(current_keys):
                failures.append("mapping/view lost required keys")
        elif historical_category == "sequence":
            if historical_shape.get("sequence_length") != current_shape.get("sequence_length"):
                failures.append("deterministic sequence shape changed")
        elif historical_shape.get("scalar") != current_shape.get("scalar"):
            failures.append("deterministic scalar behavior changed")
    return not failures, failures


def _current_callable(name: str) -> Any:
    import spl

    if name == "SPLClient":
        return spl.SPLClient
    return getattr(spl.SPLClient, name.removeprefix("SPLClient."))


def _old_parameter_value(name: str) -> str:
    return f"historical:{name}"


def _call_forms(
    name: str,
    old_parameters: list[dict[str, Any]],
    current_signature: inspect.Signature,
) -> list[dict[str, Any]]:
    is_method = name != "SPLClient"
    parameters = old_parameters[1:] if is_method else old_parameters

    def build(*, include_optional: bool, keyword_parameter: str | None = None) -> tuple[list[Any], dict[str, Any]]:
        args: list[Any] = [object()] if is_method else []
        kwargs: dict[str, Any] = {}
        for parameter in parameters:
            kind = parameter["kind"]
            required = bool(parameter["required"])
            if kind in {"VAR_POSITIONAL", "VAR_KEYWORD"}:
                continue
            if not required and not include_optional and parameter["name"] != keyword_parameter:
                continue
            value = _old_parameter_value(str(parameter["name"]))
            if kind == "POSITIONAL_ONLY":
                args.append(value)
            elif kind == "POSITIONAL_OR_KEYWORD" and keyword_parameter is None:
                args.append(value)
            else:
                kwargs[str(parameter["name"])] = value
        return args, kwargs

    candidates: list[tuple[str, list[Any], dict[str, Any]]] = []
    minimal_args, minimal_kwargs = build(include_optional=False)
    candidates.append(("minimal", minimal_args, minimal_kwargs))
    maximal_args, maximal_kwargs = build(include_optional=True)
    candidates.append(("all-declared-parameters", maximal_args, maximal_kwargs))
    for parameter in parameters:
        if parameter["kind"] == "POSITIONAL_OR_KEYWORD":
            args, kwargs = build(include_optional=False, keyword_parameter=str(parameter["name"]))
            candidates.append((f"keyword:{parameter['name']}", args, kwargs))
    if any(parameter["kind"] == "VAR_POSITIONAL" for parameter in parameters):
        args, kwargs = build(include_optional=False)
        candidates.append(("var-positional", [*args, "historical:vararg"], kwargs))
    if any(parameter["kind"] == "VAR_KEYWORD" for parameter in parameters):
        args, kwargs = build(include_optional=False)
        candidates.append(("var-keyword", args, {**kwargs, "historical_extra": "value"}))

    forms: list[dict[str, Any]] = []
    for form, args, kwargs in candidates:
        try:
            bound = current_signature.bind(*args, **kwargs)
        except TypeError as exc:
            forms.append(
                {
                    "callable": name,
                    "form": form,
                    "status": "FAIL",
                    "error_type": type(exc).__name__,
                }
            )
        else:
            forms.append(
                {
                    "callable": name,
                    "form": form,
                    "status": "PASS",
                    "bound_parameters": list(bound.arguments),
                }
            )
    return forms


def _parameter_contracts(
    name: str,
    old_parameters: list[dict[str, Any]],
    current_signature: inspect.Signature,
    old_annotations: dict[str, str],
    current_annotations: dict[str, str],
) -> list[dict[str, Any]]:
    current = list(current_signature.parameters.values())
    by_name = {parameter.name: parameter for parameter in current}
    contracts: list[dict[str, Any]] = []
    for index, old in enumerate(old_parameters):
        old_name = str(old["name"])
        old_kind = str(old["kind"])
        if old_kind == "VAR_KEYWORD":
            matches = [parameter for parameter in current if parameter.kind.name == old_kind]
            observed = matches[0] if matches else None
        elif old_kind == "VAR_POSITIONAL":
            matches = [parameter for parameter in current if parameter.kind.name == old_kind]
            observed = matches[0] if matches else None
        else:
            observed = by_name.get(old_name)
        failures: list[str] = []
        if observed is None:
            failures.append("parameter is absent")
        else:
            if observed.kind.name != old_kind:
                failures.append("parameter kind changed")
            if bool(old["required"]) != (observed.default is inspect.Parameter.empty):
                failures.append("required/default state changed")
            if not old["required"] and str(old["default"]) != _stable_default(observed.default):
                failures.append("default changed")
            old_annotation = _normalize_annotation(old_annotations.get(old_name, "<empty>"))
            current_annotation = _normalize_annotation(current_annotations.get(observed.name, "<empty>"))
            if not _type_atoms(old_annotation).issubset(_type_atoms(current_annotation)):
                failures.append("annotation contract changed")
        contracts.append(
            {
                "callable": name,
                "historical_index": index,
                "parameter": old_name,
                "kind": old_kind,
                "status": "FAIL" if failures else "PASS",
                "failures": failures,
            }
        )

    historical_names = {
        str(parameter["name"])
        for parameter in old_parameters
        if parameter["kind"] not in {"VAR_POSITIONAL", "VAR_KEYWORD"}
    }
    for parameter in current:
        if parameter.name not in historical_names and parameter.kind not in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }:
            failures = [] if parameter.default is not inspect.Parameter.empty else ["new required parameter"]
            contracts.append(
                {
                    "callable": name,
                    "parameter": parameter.name,
                    "kind": parameter.kind.name,
                    "scope": "current-addition",
                    "status": "FAIL" if failures else "PASS",
                    "failures": failures,
                }
            )
    return contracts


def _error_behaviors(
    name: str,
    old_parameters: list[dict[str, Any]],
    current_signature: inspect.Signature,
    historical_behavior: dict[str, Any] | None,
    current_behavior: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    is_method = name != "SPLClient"
    base_args: list[Any] = [object()] if is_method else []
    rows: list[dict[str, Any]] = []
    for old in old_parameters[1:] if is_method else old_parameters:
        if not old["required"] or old["kind"] in {"VAR_POSITIONAL", "VAR_KEYWORD"}:
            continue
        try:
            current_signature.bind(*base_args)
        except TypeError as exc:
            rows.append(
                {
                    "callable": name,
                    "case": f"missing-required:{old['name']}",
                    "status": "PASS",
                    "error_type": type(exc).__name__,
                }
            )
        else:
            rows.append(
                {
                    "callable": name,
                    "case": f"missing-required:{old['name']}",
                    "status": "FAIL",
                    "error_type": None,
                }
            )
        break
    if historical_behavior is not None and historical_behavior.get("status") != "NOT_APPLICABLE":
        historical_error = historical_behavior.get("error_path", {})
        current_error = (current_behavior or {}).get("error_path", {})
        compatible = (
            historical_error.get("status") == "PASS"
            and current_error.get("status") == "PASS"
            and historical_error.get("error_type") == current_error.get("error_type") == "TypeError"
            and historical_error.get("category") == current_error.get("category")
        )
        rows.append(
            {
                "callable": name,
                "case": "recorded-meaningful-error-path",
                "historical": historical_error,
                "current": current_error,
                "status": "PASS" if compatible else "FAIL",
            }
        )
    if not rows:
        rows.append(
            {
                "callable": name,
                "case": "no-required-error-contract",
                "status": "PASS",
            }
        )
    return rows


def _parameters(document: Any) -> list[dict[str, Any]] | None:
    if not isinstance(document, dict) or not isinstance(document.get("parameters"), list):
        return None
    if not all(isinstance(item, dict) for item in document["parameters"]):
        return None
    return document["parameters"]


def _annotations(document: Any) -> dict[str, str] | None:
    parameters = _parameters(document)
    if parameters is None or not isinstance(document.get("return"), str):
        return None
    if not all(isinstance(item.get("name"), str) and isinstance(item.get("annotation"), str) for item in parameters):
        return None
    return {str(item["name"]): str(item["annotation"]) for item in parameters}


def _successful_behavior(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and value.get("status") == "PASS"
        and isinstance(value.get("call"), dict)
        and value["call"].get("status") == "PASS"
        and isinstance(value["call"].get("return_shape"), dict)
    )


def compare(raw_path: Path) -> dict[str, Any]:
    raw_bytes = raw_path.read_bytes()
    raw = json.loads(raw_bytes)
    if raw.get("schema") != "splime.historical_api_evidence.v2":
        raise ValueError("historical raw evidence schema is unsupported")
    current_snapshot = build_snapshot()
    current_structural_all = current_snapshot.get("structural_signatures", {})
    current_annotations_all = current_snapshot.get("annotation_evidence", {})
    current_behavior_all = current_snapshot.get("behavioral_results", {})
    current_presence = current_snapshot.get("callable_presence", {})
    rows: list[dict[str, Any]] = []
    for historical in raw["versions"]:
        call_forms: list[dict[str, Any]] = []
        parameter_contracts: list[dict[str, Any]] = []
        return_shapes: list[dict[str, Any]] = []
        error_behaviors: list[dict[str, Any]] = []
        evidence_contracts: list[dict[str, Any]] = []
        present_names = sorted(name for name, present in historical.get("callable_presence", {}).items() if present)
        coverage: dict[str, Any] = {
            "present_callables": len(present_names),
            "structural_signatures": 0,
            "annotation_rows": 0,
            "successful_behavioral_calls": 0,
            "compared_return_rows": 0,
            "required_gaps": 0,
            "required_gap_details": [],
            "null_observation_pass_rows": 0,
            "constructor_structural": False,
            "constructor_annotations": False,
        }

        def require(qualified: str, evidence: str, condition: bool) -> None:
            if condition:
                return
            detail = f"{qualified}:{evidence}"
            evidence_contracts.append(
                {
                    "callable": qualified,
                    "evidence": evidence,
                    "status": "FAIL",
                    "failures": ["required comparison evidence is missing or malformed"],
                }
            )
            coverage["required_gap_details"].append(detail)

        for name in present_names:
            qualified = f"SPLClient.{name}"
            old_structural = historical.get("structural_signatures", {}).get(qualified)
            old_parameters = _parameters(old_structural)
            old_annotations_document = historical.get("annotation_evidence", {}).get(qualified)
            old_annotations = _annotations(old_annotations_document)
            old_behavior = historical.get("behavioral_results", {}).get(name)
            current_structural = current_structural_all.get(qualified)
            current_parameters = _parameters(current_structural)
            current_annotations_document = current_annotations_all.get(qualified)
            current_annotations = _annotations(current_annotations_document)
            current_behavior = current_behavior_all.get(name)

            require(qualified, "historical structural signature", old_parameters is not None)
            require(qualified, "historical annotation row", old_annotations is not None)
            require(qualified, "historical successful behavioral call", _successful_behavior(old_behavior))
            require(qualified, "current callable presence", current_presence.get(name) is True)
            require(qualified, "current structural signature", current_parameters is not None)
            require(qualified, "current annotation row", current_annotations is not None)
            require(qualified, "current successful behavioral call", _successful_behavior(current_behavior))
            require(qualified, "controlled behavior form", name in BEHAVIOR_FORMS)
            if old_parameters is not None:
                coverage["structural_signatures"] += 1
            if old_annotations is not None:
                coverage["annotation_rows"] += 1
            if _successful_behavior(old_behavior):
                coverage["successful_behavioral_calls"] += 1
            if not all(
                (
                    old_parameters is not None,
                    old_annotations is not None,
                    _successful_behavior(old_behavior),
                    current_presence.get(name) is True,
                    current_parameters is not None,
                    current_annotations is not None,
                    _successful_behavior(current_behavior),
                    name in BEHAVIOR_FORMS,
                )
            ):
                continue
            try:
                signature = inspect.signature(_current_callable(qualified))
            except (AttributeError, TypeError, ValueError):
                require(qualified, "inspectable current callable", False)
                continue

            call_forms.extend(_call_forms(qualified, old_parameters, signature))
            parameter_contracts.extend(
                _parameter_contracts(
                    qualified,
                    old_parameters,
                    signature,
                    old_annotations,
                    current_annotations,
                )
            )
            assert isinstance(old_annotations_document, dict)
            assert isinstance(current_annotations_document, dict)
            historical_return = _normalize_annotation(old_annotations_document["return"])
            current_return = _normalize_annotation(current_annotations_document["return"])
            observed_compatible, observed_failures = _observed_return_contract(old_behavior, current_behavior)
            declared_compatible = _type_atoms(current_return).issubset(_type_atoms(historical_return))
            return_row = {
                "callable": qualified,
                "historical_declared": historical_return,
                "current_declared": current_return,
                "historical_observed": old_behavior["call"],
                "current_observed": current_behavior["call"],
                "comparison_basis": (
                    "declared return contract plus exact observed category/type, required mapping or wrapper "
                    "members, sequence length and scalar behavior for every present callable's controlled call"
                ),
                "failures": [
                    *([] if declared_compatible else ["declared return contract changed"]),
                    *observed_failures,
                ],
                "status": "PASS" if declared_compatible and observed_compatible else "FAIL",
            }
            return_shapes.append(return_row)
            coverage["compared_return_rows"] += 1
            if return_row["status"] == "PASS" and (
                return_row["historical_observed"] is None or return_row["current_observed"] is None
            ):
                coverage["null_observation_pass_rows"] += 1
            error_behaviors.extend(
                _error_behaviors(
                    qualified,
                    old_parameters,
                    signature,
                    old_behavior,
                    current_behavior,
                )
            )

        constructor_structural = historical.get("structural_signatures", {}).get("SPLClient")
        constructor_parameters = _parameters(constructor_structural)
        old_constructor_doc = historical.get("annotation_evidence", {}).get("SPLClient")
        old_constructor_annotations = _annotations(old_constructor_doc)
        current_constructor_parameters = _parameters(current_structural_all.get("SPLClient"))
        current_constructor_doc = current_annotations_all.get("SPLClient")
        current_constructor_annotations = _annotations(current_constructor_doc)
        require("SPLClient", "historical constructor structural signature", constructor_parameters is not None)
        require("SPLClient", "historical constructor annotation row", old_constructor_annotations is not None)
        require("SPLClient", "current constructor structural signature", current_constructor_parameters is not None)
        require("SPLClient", "current constructor annotation row", current_constructor_annotations is not None)
        coverage["constructor_structural"] = constructor_parameters is not None
        coverage["constructor_annotations"] = old_constructor_annotations is not None
        if all(
            (
                constructor_parameters is not None,
                old_constructor_annotations is not None,
                current_constructor_parameters is not None,
                current_constructor_annotations is not None,
            )
        ):
            try:
                signature = inspect.signature(_current_callable("SPLClient"))
            except (AttributeError, TypeError, ValueError):
                require("SPLClient", "inspectable current constructor", False)
            else:
                call_forms.extend(_call_forms("SPLClient", constructor_parameters, signature))
                parameter_contracts.extend(
                    _parameter_contracts(
                        "SPLClient",
                        constructor_parameters,
                        signature,
                        old_constructor_annotations,
                        current_constructor_annotations,
                    )
                )
                assert isinstance(old_constructor_doc, dict)
                assert isinstance(current_constructor_doc, dict)
                return_shapes.append(
                    {
                        "callable": "SPLClient",
                        "historical_declared": _normalize_annotation(old_constructor_doc["return"]),
                        "current_declared": _normalize_annotation(current_constructor_doc["return"]),
                        "comparison_basis": "constructor return contract",
                        "status": "PASS",
                    }
                )
                error_behaviors.extend(_error_behaviors("SPLClient", constructor_parameters, signature, None, None))

        coverage["required_gaps"] = len(coverage["required_gap_details"])
        groups = (evidence_contracts, call_forms, parameter_contracts, return_shapes, error_behaviors)
        failures = [
            f"{item['callable']}:{item.get('evidence', item.get('form', item.get('case', item.get('parameter', 'contract'))))}"
            for group in groups
            for item in group
            if item["status"] != "PASS"
        ]
        rows.append(
            {
                "version": historical["version"],
                "status": "FAIL" if failures else "PASS",
                "coverage": coverage,
                "evidence_contracts": evidence_contracts,
                "call_forms": call_forms,
                "parameter_contracts": parameter_contracts,
                "return_shapes": return_shapes,
                "error_behaviors": error_behaviors,
                "failures": failures,
            }
        )

    coverage_audit = {
        "versions": len(rows),
        "present_callables": sum(row["coverage"]["present_callables"] for row in rows),
        "structural_signatures": sum(row["coverage"]["structural_signatures"] for row in rows),
        "annotation_rows": sum(row["coverage"]["annotation_rows"] for row in rows),
        "successful_behavioral_calls": sum(row["coverage"]["successful_behavioral_calls"] for row in rows),
        "compared_return_rows": sum(row["coverage"]["compared_return_rows"] for row in rows),
        "required_gaps": sum(row["coverage"]["required_gaps"] for row in rows),
        "null_observation_pass_rows": sum(row["coverage"]["null_observation_pass_rows"] for row in rows),
    }
    return {
        "schema": SCHEMA,
        "schema_version": 1,
        "generated_by": "tools/compare_historical_api.py",
        "raw_evidence": "release/0.4.8/historical-api-signatures.json",
        "raw_evidence_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "current_facade": {
            "version": current_snapshot["version"],
            "probe_schema": current_snapshot["schema"],
            "raw_signatures": current_snapshot["raw_signatures"],
        },
        "comparison_policy": {
            "added_optional_parameters": "compatible",
            "removed_or_tightened_historical_forms": "failure",
            "parameter_kinds_defaults_annotations": "must preserve each historical parameter",
            "return_views": (
                "every applicable controlled historical/current call must pass; category and concrete container/scalar "
                "type are stable; historical mapping keys and public-wrapper attributes are required; wrapper module "
                "moves with the same public class name and additive attributes are covariant"
            ),
            "errors": "historical missing-required category and TypeError behavior must remain meaningful",
            "raw_self_probe_is_verdict": False,
        },
        "coverage_audit": coverage_audit,
        "versions": rows,
    }


def main() -> int:
    root = Path(__file__).parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw",
        type=Path,
        default=root / "release/0.4.8/historical-api-signatures.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "release/0.4.8/historical-api-comparison.json",
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    document = compare(args.raw.resolve())
    rendered = json.dumps(document, indent=2, sort_keys=False) + "\n"
    if args.check:
        if not args.output.is_file() or args.output.read_text(encoding="utf-8") != rendered:
            raise ValueError("tracked historical facade comparison is stale")
    else:
        args.output.write_text(rendered, encoding="utf-8")
    passed = sum(row["status"] == "PASS" for row in document["versions"])
    failed = len(document["versions"]) - passed
    print(f"historical facade comparison: PASS={passed} FAIL={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
