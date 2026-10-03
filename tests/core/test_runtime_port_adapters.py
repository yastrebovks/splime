from __future__ import annotations

import base64
import hashlib
import importlib.metadata
import os
import socket
from copy import deepcopy
from pathlib import Path

import pytest

from spl.adapters import (
    BINARY_FILE,
    BUILTIN_ADAPTER_IDS,
    OPAQUE_FILE,
    TEXT_FILE_UTF8,
    FileInput,
    builtin_id_for_adapter,
    get_builtin_adapter,
    system_default_adapter_for_value,
)
from spl._runtime_port_adapters_client import server_admission_document
from spl.core.entities.adapter import Adapter
from spl.core.entities.distribution import DDistribution
from spl.core.runtime_port_adapters import (
    RuntimePortAdapterContractError,
    adapter_descriptor,
    build_custom_bundle,
    built_in_descriptor,
    built_in_registry_descriptor,
    merge_adapter_distributions,
    normalize_adapter_policy,
    normalize_public_adapter_mapping,
    normalize_runtime_adapter_semantic_advisories,
    normalize_wire_document,
    port_catalog_from_signature,
    resolve_port_reference,
    runtime_adapter_semantic_state,
    validate_custom_bundle_dependencies,
)


def save_upper(path: str, value: str) -> None:
    from pathlib import Path as LocalPath

    LocalPath(path).write_text(value.upper(), encoding="utf-8")


def load_upper(path: str) -> str:
    from pathlib import Path as LocalPath

    return LocalPath(path).read_text(encoding="utf-8").lower()


def save_with_pytest_import(path: str, value: str) -> None:
    import pytest
    from pathlib import Path as LocalPath

    del pytest
    LocalPath(path).write_text(value, encoding="utf-8")


def save_with_pil_import(path: str, value: str) -> None:
    import PIL
    from pathlib import Path as LocalPath

    del PIL
    LocalPath(path).write_text(value, encoding="utf-8")


def save_with_sklearn_import(path: str, value: str) -> None:
    import sklearn
    from pathlib import Path as LocalPath

    del sklearn
    LocalPath(path).write_text(value, encoding="utf-8")


def save_with_unknown_import(path: str, value: str) -> None:
    import unowned_runtime_module
    from pathlib import Path as LocalPath

    del unowned_runtime_module
    LocalPath(path).write_text(value, encoding="utf-8")


def save_with_cv2_import(path: str, value: str) -> None:
    import cv2
    from pathlib import Path as LocalPath

    del cv2
    LocalPath(path).write_text(value, encoding="utf-8")


def save_with_qualified_dynamic_import(path: str, value: str) -> None:
    import importlib

    importlib.import_module(value)


def save_with_unqualified_dynamic_import(path: str, value: str) -> None:
    from importlib import import_module

    import_module(value)


def save_unresolved(path: str, value: str) -> None:
    Path(path).write_text(prefix + value, encoding="utf-8")  # noqa: F821


def save_qualified_eval(path: str, value: str) -> None:
    import builtins

    builtins.eval(value)


def save_with_nested_callable(path: str, value: str) -> None:
    from pathlib import Path as LocalPath

    def identity(candidate: str) -> str:
        return candidate

    LocalPath(path).write_text(identity(value), encoding="utf-8")


def _wire_document(*, descriptor: dict[str, object], content: bytes = b"value") -> dict[str, object]:
    digest = hashlib.sha256(content).hexdigest()
    return {
        "schema_version": 1,
        "bindings": [
            {
                "direction": "input",
                "port": "value",
                "external_name": "value",
                "semantic_type": "str",
                "adapter": descriptor,
                "resolution_source": "run_override",
                "transport": "artifact",
                "argument": {"kind": "keyword", "name": "value", "index": None},
                "input_name": "input-value.txt",
                "result_path": [],
            }
        ],
        "inputs": [
            {
                "name": "input-value.txt",
                "port": "value",
                "size": len(content),
                "sha256": digest,
                "format_tag": descriptor["format_tag"],
                "semantic_type": "str",
                "adapter_id": descriptor["id"],
                "media_type": descriptor["presentation"]["media_type"],
                "content_base64": base64.b64encode(content).decode("ascii"),
                "staged_name": None,
            }
        ],
        "custom_bundle": None,
    }


def test_public_builtin_ids_are_stable_and_lazy() -> None:
    assert BUILTIN_ADAPTER_IDS == {
        "json",
        "opaque-file",
        "text-file-utf8",
        "binary-file",
        "dataframe-json-split",
        "dataframe-csv-semicolon",
        "dataframe-xlsx",
        "png-pillow",
    }
    assert get_builtin_adapter(OPAQUE_FILE).key == "pathlib.Path@spl.file.opaque.v1"
    assert get_builtin_adapter(TEXT_FILE_UTF8).tag == "spl.text.utf8.v1"
    assert get_builtin_adapter(BINARY_FILE).tag == "spl.binary.raw.v1"


def test_browser_registry_descriptor_is_stable_code_free_and_environment_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(package: str) -> str:
        raise importlib.metadata.PackageNotFoundError(package)

    monkeypatch.setattr("spl.adapters.importlib_metadata.version", unavailable)
    rows = [built_in_registry_descriptor(adapter_id) for adapter_id in sorted(BUILTIN_ADAPTER_IDS)]

    assert [row["id"] for row in rows] == sorted(BUILTIN_ADAPTER_IDS)
    assert all(
        set(row)
        == {
            "id",
            "format",
            "label",
            "preferred_extension",
            "media_type",
            "directions",
            "transport",
            "semantic_types",
            "semantic_categories",
            "system_default",
            "required_distributions",
            "builtin",
            "custom",
            "custom_code",
        }
        for row in rows
    )
    by_id = {row["id"]: row for row in rows}
    assert by_id["json"] == {
        "id": "json",
        "format": {"key": "spl.core.json@json", "tag": "json", "accepted_tags": ["json"]},
        "label": "JSON",
        "preferred_extension": None,
        "media_type": "application/json",
        "directions": ["input", "output"],
        "transport": "inline_json",
        "semantic_types": ["spl.core.json"],
        "semantic_categories": ["strict_json"],
        "system_default": {"fallback": True, "semantic_types": [], "value_types": []},
        "required_distributions": [],
        "builtin": True,
        "custom": False,
        "custom_code": False,
    }
    assert by_id["dataframe-json-split"]["system_default"] == {
        "fallback": False,
        "semantic_types": ["DataFrame", "pandas.DataFrame", "pandas.core.frame.DataFrame", "pd.DataFrame"],
        "value_types": ["pandas.DataFrame", "pandas.core.frame.DataFrame"],
    }
    assert by_id["dataframe-xlsx"]["required_distributions"] == ["pandas", "openpyxl"]
    assert all(row["builtin"] is True and row["custom"] is False and row["custom_code"] is False for row in rows)
    assert not {"source", "save", "load", "functions"} & {key for row in rows for key in row}


def test_runtime_adapter_corpus_external_network_guard_is_active() -> None:
    probe = socket.socket()
    probe.close()
    with pytest.raises(AssertionError, match="external network access attempted"):
        probe.connect(("192.0.2.1", 9))


def test_file_input_binds_bytes_without_exposing_path_in_repr(tmp_path: Path) -> None:
    source = tmp_path / "secret-location.bin"
    source.write_bytes(b"\x00\xffpayload")
    value = FileInput(source, name="payload.bin", media_type="application/octet-stream")

    assert value.name == "payload.bin"
    assert value.size == len(b"\x00\xffpayload")
    assert value.staged_bytes() == b"\x00\xffpayload"
    assert str(tmp_path) not in repr(value)


def test_file_input_rejects_symlink_directory_and_replacement(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"first")
    link = tmp_path / "link.bin"
    link.symlink_to(source)

    with pytest.raises(ValueError, match="regular file"):
        FileInput(link)
    with pytest.raises(ValueError, match="regular file"):
        FileInput(tmp_path)

    value = FileInput(source)
    # Keep the original inode allocated; unlink/recreate may reuse it on Linux.
    original = tmp_path / "original.bin"
    source.rename(original)
    source.write_bytes(b"first")
    assert not source.samefile(original)
    with pytest.raises(ValueError, match="changed"):
        value.staged_bytes()


def test_file_input_rejects_fifo_and_traversal_logical_name(tmp_path: Path) -> None:
    if hasattr(os, "mkfifo"):
        fifo = tmp_path / "payload.pipe"
        os.mkfifo(fifo)
        with pytest.raises(ValueError, match="regular file"):
            FileInput(fifo)

    source = tmp_path / "payload.bin"
    source.write_bytes(b"payload")
    with pytest.raises(ValueError, match="safe single filename"):
        FileInput(source, name="../../escaped.bin")


def test_closed_public_mapping_and_policy() -> None:
    assert normalize_public_adapter_mapping({"inputs": {"value": "json"}}) == {
        "inputs": {"value": "json"},
        "outputs": {},
    }
    assert normalize_adapter_policy(None) == {"custom_remote": "deny"}
    assert normalize_adapter_policy({"custom_remote": "allow"}) == {"custom_remote": "allow"}
    with pytest.raises(RuntimePortAdapterContractError, match="unknown section"):
        normalize_public_adapter_mapping({"edges": {}})
    with pytest.raises(RuntimePortAdapterContractError, match="exactly"):
        normalize_adapter_policy({"custom_remote": "deny", "other": True})


def test_wire_document_is_closed_and_binds_declared_artifact() -> None:
    _, descriptor = built_in_descriptor(TEXT_FILE_UTF8)
    document = _wire_document(descriptor=descriptor)
    normalized = normalize_wire_document(document, allow_content=True)
    assert normalized["bindings"][0]["port"] == "value"
    assert normalized["inputs"][0]["sha256"] == hashlib.sha256(b"value").hexdigest()

    malformed = {**document, "caller_path": "/private/value.txt"}
    with pytest.raises(RuntimePortAdapterContractError, match="unknown"):
        normalize_wire_document(malformed, allow_content=True)


def test_explicit_semantic_mismatch_is_advisory_but_default_remains_strict() -> None:
    _, descriptor = built_in_descriptor(BINARY_FILE)
    explicit = _wire_document(descriptor=descriptor)
    assert normalize_wire_document(explicit, allow_content=True)["bindings"][0]["resolution_source"] == ("run_override")

    automatic = deepcopy(explicit)
    automatic["bindings"][0]["resolution_source"] = "system_default"
    with pytest.raises(RuntimePortAdapterContractError) as mismatch:
        normalize_wire_document(automatic, allow_content=True)
    assert mismatch.value.code == "semantic_type_mismatch"


@pytest.mark.parametrize(
    ("port_type", "adapter_type", "adapter_id", "expected"),
    [
        ("str", "builtins.str", "text-file-utf8", "recommended"),
        ("Path", "pathlib.Path", "opaque-file", "recommended"),
        (
            "pandas.DataFrame",
            "pandas.core.frame.DataFrame",
            "dataframe-json-split",
            "recommended",
        ),
        ("dict[str, int]", "spl.core.json", "json", "recommended"),
        ("typing.List[str]", "spl.core.json", "json", "recommended"),
        ("None", "spl.core.json", "json", "recommended"),
        ("Any", "spl.core.json", "json", "compatibility_unproven"),
        ("typing.Any", "spl.core.json", "json", "compatibility_unproven"),
        ("typing.ANY", "spl.core.json", "json", "compatibility_unproven"),
        ("builtins.Object", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        (None, "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("unknown", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("typing.Unknown", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("Widget", "acme.Widget", "custom:widget", "compatibility_unproven"),
        ("widget", "acme.Widget", "custom:widget", "compatibility_unproven"),
        ("DataFrame", "pandas.core.frame.DataFrame", "dataframe-json-split", "compatibility_unproven"),
        ("Optional[str]", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("str | None", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("typing.Union[str, bytes]", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("Literal[str]", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("typing.Annotated[str, meta]", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("TypeVar[T]", "builtins.str", "text-file-utf8", "compatibility_unproven"),
        ("acme.Widget", "acme.Widget", "custom:widget", "recommended"),
        ("acme.Widget", "other.Widget", "custom:widget", "declared_type_mismatch"),
        (
            "pandas.DataFrame",
            "polars.dataframe.frame.DataFrame",
            "custom:frame",
            "declared_type_mismatch",
        ),
        ("bytes", "spl.core.json", "json", "declared_type_mismatch"),
    ],
)
def test_semantic_advisory_evaluator_parity_vectors(
    port_type: str | None,
    adapter_type: str,
    adapter_id: str,
    expected: str,
) -> None:
    assert runtime_adapter_semantic_state(port_type, adapter_type, adapter_id=adapter_id) == expected


def test_semantic_advisory_closed_acknowledgement_contract() -> None:
    binding = {
        "direction": "input",
        "port": "value",
        "adapter_id": "binary-file",
        "port_semantic_type": "str",
        "adapter_semantic_type": "builtins.bytes",
        "adapter_semantic_category": "binary",
        "state": "declared_type_mismatch",
        "acknowledged": True,
        "acknowledgement_source": "sdk_explicit_selection",
    }
    normalized = normalize_runtime_adapter_semantic_advisories({"schema_version": 1, "bindings": [binding]})
    assert normalized["bindings"] == [binding]
    with pytest.raises(RuntimePortAdapterContractError):
        normalize_runtime_adapter_semantic_advisories({"schema_version": 1, "bindings": [{**binding, "unknown": True}]})
    with pytest.raises(RuntimePortAdapterContractError):
        normalize_runtime_adapter_semantic_advisories(
            {
                "schema_version": 1,
                "bindings": [
                    {
                        **binding,
                        "state": "recommended",
                        "acknowledged": True,
                    }
                ],
            }
        )


def test_custom_bundle_accepts_plain_functions_and_rejects_unresolved_globals() -> None:
    adapter = Adapter(
        key="builtins.str@upper-text.v1",
        save=save_upper,
        load=load_upper,
        py_type=str,
        format="upper-text.v1",
        distributions=(),
    )
    bundle, symbols = build_custom_bundle([adapter])
    save_name, load_name = symbols[id(adapter)]
    descriptor = adapter_descriptor(
        adapter,
        adapter_id=None,
        custom_bundle_sha256=bundle["sha256"],
        save_symbol=save_name,
        load_symbol=load_name,
    )
    assert descriptor["kind"] == "custom"
    assert descriptor["bundle_sha256"] == bundle["sha256"]
    assert b"def save_upper" in bundle["source_bytes"]

    invalid = Adapter(
        key="builtins.str@unresolved.v1",
        save=save_unresolved,
        load=load_upper,
        py_type=str,
        format="unresolved.v1",
    )
    with pytest.raises(RuntimePortAdapterContractError, match="unresolved global"):
        build_custom_bundle([invalid])


def test_custom_bundle_rejects_lambda_closure_nested_and_malformed_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_prefix = ["captured-"]

    def save_closure(path: str, value: str) -> None:
        Path(path).write_text(captured_prefix[0] + value, encoding="utf-8")

    invalid_functions = [
        (lambda path, value: Path(path).write_text(value), "custom_function_name"),
        (save_closure, "custom_function_scope"),
        (save_with_nested_callable, "custom_nested_callable"),
    ]
    for save, expected_code in invalid_functions:
        adapter = Adapter(
            key="builtins.str@invalid-source.v1",
            save=save,
            load=load_upper,
            py_type=str,
            format="invalid-source.v1",
        )
        with pytest.raises(RuntimePortAdapterContractError) as raised:
            build_custom_bundle([adapter])
        assert raised.value.code == expected_code

    malformed = Adapter(
        key="builtins.str@malformed-source.v1",
        save=save_upper,
        load=load_upper,
        py_type=str,
        format="malformed-source.v1",
    )
    monkeypatch.setattr(
        "spl.core.runtime_port_adapters.inspect.getsource",
        lambda function: "def save_upper(:\n",
    )
    with pytest.raises(RuntimePortAdapterContractError) as raised:
        build_custom_bundle([malformed])
    assert raised.value.code == "custom_source_syntax"


@pytest.mark.parametrize(
    ("save", "package", "root"),
    [
        (save_with_pil_import, "Pillow", "PIL"),
        (save_with_sklearn_import, "scikit-learn", "sklearn"),
    ],
)
def test_custom_import_ownership_accepts_one_exact_declared_distribution(
    monkeypatch: pytest.MonkeyPatch,
    save: object,
    package: str,
    root: str,
) -> None:
    monkeypatch.setattr(
        "spl.core.runtime_port_adapters.importlib_metadata.packages_distributions",
        lambda: {root: [package]},
    )
    adapter = Adapter(
        key="builtins.str@owned-import.v1",
        save=save,
        load=load_upper,
        py_type=str,
        format="owned-import.v1",
        distributions=(DDistribution(package=package, version="1.2.3"),),
    )

    bundle, _ = build_custom_bundle([adapter])

    assert bundle["source_bytes"]


@pytest.mark.parametrize(
    ("save", "owners", "package"),
    [
        (save_with_unknown_import, {}, "unknown-package"),
        (save_with_cv2_import, {"cv2": ["opencv-python", "opencv-contrib-python"]}, "opencv-python"),
    ],
)
def test_custom_import_ownership_rejects_unowned_or_ambiguous_roots_before_admission(
    monkeypatch: pytest.MonkeyPatch,
    save: object,
    owners: dict[str, list[str]],
    package: str,
) -> None:
    monkeypatch.setattr(
        "spl.core.runtime_port_adapters.importlib_metadata.packages_distributions",
        lambda: owners,
    )
    adapter = Adapter(
        key="builtins.str@unsafe-import.v1",
        save=save,
        load=load_upper,
        py_type=str,
        format="unsafe-import.v1",
        distributions=(DDistribution(package=package, version="1.2.3"),),
    )

    with pytest.raises(RuntimePortAdapterContractError) as raised:
        build_custom_bundle([adapter])

    assert raised.value.code == "custom_import_dependency"
    assert raised.value.stage == "environment_preflight"


@pytest.mark.parametrize("save", [save_with_qualified_dynamic_import, save_with_unqualified_dynamic_import])
def test_custom_source_rejects_dynamic_import_module_before_admission(save: object) -> None:
    adapter = Adapter(
        key="builtins.str@dynamic-import.v1",
        save=save,
        load=load_upper,
        py_type=str,
        format="dynamic-import.v1",
    )

    with pytest.raises(RuntimePortAdapterContractError) as raised:
        build_custom_bundle([adapter])

    assert raised.value.code == "custom_dynamic_call"


def test_worker_environment_rejects_forged_import_distribution_ownership() -> None:
    pytest_version = importlib.metadata.version("pytest")
    packaging_version = importlib.metadata.version("packaging")
    adapter = Adapter(
        key="builtins.str@forged-import.v1",
        save=save_with_pytest_import,
        load=load_upper,
        py_type=str,
        format="forged-import.v1",
        distributions=(
            DDistribution(package="pytest", version=pytest_version),
            DDistribution(package="packaging", version=packaging_version),
        ),
    )
    bundle, symbols = build_custom_bundle([adapter])
    forged_source = bundle["source_bytes"].replace(
        f"module=pytest package=pytest version={pytest_version}".encode(),
        f"module=pytest package=packaging version={packaging_version}".encode(),
    )
    assert forged_source != bundle["source_bytes"]
    digest = hashlib.sha256(forged_source).hexdigest()
    save_name, load_name = symbols[id(adapter)]
    descriptor = adapter_descriptor(
        adapter,
        adapter_id=None,
        custom_bundle_sha256=digest,
        save_symbol=save_name,
        load_symbol=load_name,
    )
    document = _wire_document(descriptor=descriptor)
    document["custom_bundle"] = {
        "name": "custom-adapters.py",
        "size": len(forged_source),
        "sha256": digest,
        "functions": bundle["functions"],
        "content_base64": base64.b64encode(forged_source).decode("ascii"),
        "staged_name": None,
    }

    validate_custom_bundle_dependencies(forged_source, document)
    with pytest.raises(RuntimePortAdapterContractError) as raised:
        validate_custom_bundle_dependencies(
            forged_source,
            document,
            verify_installed_environment=True,
        )
    assert raised.value.code == "custom_import_environment_owner"
    assert raised.value.stage == "worker_load"


@pytest.mark.parametrize(
    ("verify_installed_environment", "expected_stage"),
    [(False, "environment_preflight"), (True, "worker_load")],
)
def test_forged_bundle_source_cannot_bypass_daemon_or_worker_ast_rules(
    verify_installed_environment: bool,
    expected_stage: str,
) -> None:
    adapter = Adapter(
        key="builtins.str@forged-nested-lambda.v1",
        save=save_upper,
        load=load_upper,
        py_type=str,
        format="forged-nested-lambda.v1",
    )
    bundle, symbols = build_custom_bundle([adapter])
    needle = b"def save_upper(path: str, value: str) -> None:\n"
    forged_source = bundle["source_bytes"].replace(
        needle,
        needle + b"    transform = lambda candidate: candidate\n",
    )
    assert forged_source != bundle["source_bytes"]
    digest = hashlib.sha256(forged_source).hexdigest()
    save_name, load_name = symbols[id(adapter)]
    descriptor = adapter_descriptor(
        adapter,
        adapter_id=None,
        custom_bundle_sha256=digest,
        save_symbol=save_name,
        load_symbol=load_name,
    )
    document = _wire_document(descriptor=descriptor)
    document["custom_bundle"] = {
        "name": bundle["name"],
        "size": len(forged_source),
        "sha256": digest,
        "functions": bundle["functions"],
        "content_base64": base64.b64encode(forged_source).decode("ascii"),
        "staged_name": None,
    }

    with pytest.raises(RuntimePortAdapterContractError) as raised:
        validate_custom_bundle_dependencies(
            forged_source,
            document,
            verify_installed_environment=verify_installed_environment,
        )

    assert raised.value.code == "custom_nested_callable"
    assert raised.value.stage == expected_stage


def test_worker_environment_rejects_declared_distribution_that_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest_version = importlib.metadata.version("pytest")
    adapter = Adapter(
        key="builtins.str@missing-installed-import.v1",
        save=save_with_pytest_import,
        load=load_upper,
        py_type=str,
        format="missing-installed-import.v1",
        distributions=(DDistribution(package="pytest", version=pytest_version),),
    )
    bundle, symbols = build_custom_bundle([adapter])
    save_name, load_name = symbols[id(adapter)]
    descriptor = adapter_descriptor(
        adapter,
        adapter_id=None,
        custom_bundle_sha256=bundle["sha256"],
        save_symbol=save_name,
        load_symbol=load_name,
    )
    document = _wire_document(descriptor=descriptor)
    document["custom_bundle"] = {
        "name": bundle["name"],
        "size": bundle["size"],
        "sha256": bundle["sha256"],
        "functions": bundle["functions"],
        "content_base64": base64.b64encode(bundle["source_bytes"]).decode("ascii"),
        "staged_name": None,
    }
    monkeypatch.setattr(
        "spl.core.runtime_port_adapters.importlib_metadata.packages_distributions",
        lambda: {"pytest": ["pytest"]},
    )

    def missing_version(package: str) -> str:
        raise importlib.metadata.PackageNotFoundError(package)

    monkeypatch.setattr(
        "spl.core.runtime_port_adapters.importlib_metadata.version",
        missing_version,
    )

    with pytest.raises(RuntimePortAdapterContractError) as raised:
        validate_custom_bundle_dependencies(
            bundle["source_bytes"],
            document,
            verify_installed_environment=True,
        )

    assert raised.value.code == "custom_import_environment_missing"
    assert raised.value.stage == "worker_load"


def test_environment_distribution_merge_is_exact_and_conflicts_fail() -> None:
    adapter = Adapter(
        key="builtins.str@upper-text.v1",
        save=save_upper,
        load=load_upper,
        py_type=str,
        format="upper-text.v1",
        distributions=(DDistribution(package="Example_Package", version="1.0"),),
    )
    bundle, symbols = build_custom_bundle([adapter])
    save_name, load_name = symbols[id(adapter)]
    descriptor = adapter_descriptor(
        adapter,
        adapter_id=None,
        custom_bundle_sha256=bundle["sha256"],
        save_symbol=save_name,
        load_symbol=load_name,
    )
    document = _wire_document(descriptor=descriptor)
    document["custom_bundle"] = {
        key: bundle[key] for key in ("name", "size", "sha256", "functions", "content_base64", "staged_name")
    }
    document["custom_bundle"]["content_base64"] = base64.b64encode(bundle["source_bytes"]).decode("ascii")
    normalized = normalize_wire_document(document, allow_content=True)
    for item in normalized["inputs"]:
        item["content_base64"] = None
        item["staged_name"] = item["name"]
    normalized["custom_bundle"]["content_base64"] = None
    normalized["custom_bundle"]["staged_name"] = "custom-adapters.py"

    assert merge_adapter_distributions([], normalized) == [{"package": "Example_Package", "version": "1.0"}]
    with pytest.raises(RuntimePortAdapterContractError, match="dependency conflicts"):
        merge_adapter_distributions(
            [{"package": "example-package", "version": "2.0"}],
            normalized,
        )


def test_pipeline_ports_require_canonical_address_when_short_name_is_ambiguous() -> None:
    signature = {
        "kind": "pipeline",
        "aliases": [
            {"name": "left", "node_id": "node-left"},
            {"name": "right", "node_id": "node-right"},
        ],
        "inputs": [
            {
                "name": "value",
                "type": "str",
                "sources": [
                    {"node_id": "node-left", "port": "value"},
                    {"node_id": "node-right", "port": "value"},
                ],
            }
        ],
        "outputs": [],
    }
    catalog = port_catalog_from_signature(signature)

    with pytest.raises(RuntimePortAdapterContractError, match="ambiguous"):
        resolve_port_reference("value", catalog["input"])
    assert resolve_port_reference("left.value", catalog["input"]).canonical == "left.value"


def test_builtin_identity_cannot_be_spoofed_by_matching_key_and_tag() -> None:
    builtin = get_builtin_adapter(TEXT_FILE_UTF8)
    spoof = Adapter(
        key=builtin.key,
        save=save_upper,
        load=load_upper,
        py_type=str,
        format=builtin.tag,
    )
    assert builtin_id_for_adapter(spoof) is None


def test_registry_default_supports_public_pandas_three_identity_without_import() -> None:
    dataframe_type = type("DataFrame", (), {})
    dataframe_type.__module__ = "pandas"
    assert system_default_adapter_for_value(dataframe_type()) == "dataframe-json-split"


def test_qualified_dynamic_custom_call_is_rejected() -> None:
    adapter = Adapter(
        key="builtins.str@qualified-eval.v1",
        save=save_qualified_eval,
        load=load_upper,
        py_type=str,
        format="qualified-eval.v1",
    )
    with pytest.raises(RuntimePortAdapterContractError, match="may not call eval"):
        build_custom_bundle([adapter])


def test_server_projection_is_flat_closed_and_body_free() -> None:
    _, descriptor = built_in_descriptor(TEXT_FILE_UTF8)
    projected = server_admission_document(_wire_document(descriptor=descriptor))
    assert set(projected) == {"schema_version", "bindings", "inputs", "custom_bundle"}
    assert set(projected["bindings"][0]) == {
        "direction",
        "port",
        "external_name",
        "semantic_type",
        "adapter_kind",
        "adapter_id",
        "key",
        "format_tag",
        "accepted_tags",
        "distributions",
        "save_symbol",
        "load_symbol",
        "bundle_sha256",
        "resolution_source",
        "transport",
        "input_name",
        "result_path",
        "presentation",
    }
    assert "content_base64" not in repr(projected)


def test_generic_runtime_modules_have_no_library_or_extension_branches() -> None:
    repository = Path(__file__).resolve().parents[2]
    modules = [
        repository / "src/spl/_runtime_port_adapters_client.py",
        repository / "src/spl/core/runtime_port_adapters.py",
        repository / "src/spl/daemon/runtime_port_adapters.py",
        repository / "src/spl/daemon/worker.py",
    ]
    forbidden = ("pandas", "dataframe", "pillow", "numpy", '".csv"', '".xlsx"', '".png"')
    for module in modules:
        source = module.read_text(encoding="utf-8").casefold()
        assert not any(token in source for token in forbidden), module
