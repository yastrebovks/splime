from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from spl._client import SPLClient
from spl.adapters import BINARY_FILE, TEXT_FILE_UTF8
from spl.core.library_adapters import (
    LibraryAdapterContractError,
    LibraryAdapterDependency,
    LibraryAdapterRef,
    environment_fingerprint,
    library_adapter_semantic_advisory,
    library_adapter_semantic_compatible,
    normalize_dependencies,
    normalize_publish_request,
    normalize_runtime_library_adapter_refs,
    safe_presentation_text,
)
from spl.daemon_client import ClientError


FIXTURE = Path(__file__).parents[1] / "fixtures" / "library_adapter_hash_v1.json"


def _fixture() -> dict[str, object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_shared_hash_fixture_binds_execution_but_excludes_non_ascii_presentation() -> None:
    fixture = _fixture()
    request = deepcopy(fixture["request"])
    expected = fixture["expected"]

    prepared = normalize_publish_request(request)
    assert prepared["content_hash"] == expected["content_hash"]
    assert prepared["signature_hash"] == expected["signature_hash"]
    assert prepared["publication_environment"]["fingerprint"] == expected["environment_fingerprint"]
    assert prepared["save_source"] == expected["save_source"]
    assert prepared["load_source"] == expected["load_source"]

    presentation_only = deepcopy(request)
    presentation_only["description"] = "Nouvelle présentation — こんにちは"
    assert normalize_publish_request(presentation_only)["content_hash"] == prepared["content_hash"]
    assert normalize_publish_request(presentation_only)["signature_hash"] == prepared["signature_hash"]

    execution_change = deepcopy(request)
    execution_change["policy"]["remote_custom_code"] = "allow"
    changed = normalize_publish_request(execution_change)
    assert changed["content_hash"] != prepared["content_hash"]
    assert changed["signature_hash"] == prepared["signature_hash"]


def test_dependency_modules_are_exact_closed_and_aggregate_bounded() -> None:
    assert normalize_dependencies([{"package": "Pillow", "version": "11.0.0", "modules": ["PIL"]}]) == [
        {"package": "Pillow", "version": "11.0.0", "modules": ["PIL"]}
    ]
    with pytest.raises(LibraryAdapterContractError) as ambiguous:
        normalize_dependencies(
            [
                {"package": "Pillow", "version": "11.0.0", "modules": ["PIL"]},
                {"package": "other", "version": "1", "modules": ["PIL"]},
            ]
        )
    assert ambiguous.value.code == "dependency_module_ambiguous"
    with pytest.raises(LibraryAdapterContractError, match="aggregate"):
        normalize_dependencies(
            [
                {
                    "package": "many",
                    "version": "1",
                    "modules": [f"module_{index}" for index in range(256)],
                },
                {"package": "one_more", "version": "1", "modules": ["overflow"]},
            ]
        )


@pytest.mark.parametrize("category", ["Table", " table", "opaque-file", "dataframe"])
def test_semantic_categories_reject_aliases_and_normalization(category: str) -> None:
    request = deepcopy(_fixture()["request"])
    request["semantic_category"] = category
    with pytest.raises(LibraryAdapterContractError, match="semantic_category"):
        normalize_publish_request(request)


def test_semantic_category_compatibility_is_conservative() -> None:
    assert library_adapter_semantic_compatible("builtins.str", "str", "text")
    assert library_adapter_semantic_compatible("pd.DataFrame", "pandas.core.frame.DataFrame", "table")
    assert not library_adapter_semantic_compatible("pandas.DataFrame", "polars.DataFrame", "table")
    assert not library_adapter_semantic_compatible("builtins.str", "custom.Text", "text")
    assert not library_adapter_semantic_compatible("pathlib.Path", "custom.File", "opaque_file")
    assert not library_adapter_semantic_compatible("pandas.core.frame.DataFrame", "custom.Frame", "table")
    assert not library_adapter_semantic_compatible("str", "custom.Frame", "table")
    assert library_adapter_semantic_advisory("Any", "custom.Frame", "table") == "compatibility_unproven"
    assert library_adapter_semantic_advisory(None, "custom.Frame", "table") == "compatibility_unproven"


@pytest.mark.parametrize(
    "unsafe",
    [
        "file:///etc/shadow",
        "/Users/example/private.txt",
        r"C:\\Users\\example\\private.txt",
        "https://user:password@example.invalid/item",
        "Bearer private-token-value",
        "api_key=private-value",
    ],
)
def test_browser_safe_description_rejects_paths_and_credentials(unsafe: str) -> None:
    with pytest.raises(LibraryAdapterContractError):
        safe_presentation_text(unsafe, "description", 4096)


def test_public_exact_ref_and_dependency_dtos_round_trip() -> None:
    ref = LibraryAdapterRef(
        owner="owner",
        library="default",
        name="adapter",
        version=3,
        adapter_id="adapter-id",
        adapter_version_id="version-id",
        content_hash="a" * 64,
        signature_hash="b" * 64,
    )
    dependency = LibraryAdapterDependency("Pillow", "11.0.0", ("PIL",))
    assert dependency.to_dict()["modules"] == ["PIL"]
    assert (
        normalize_runtime_library_adapter_refs(
            {
                "schema_version": 1,
                "bindings": [{"direction": "output", "port": "default", **ref.to_dict()}],
            }
        )["bindings"][0]["adapter_version_id"]
        == "version-id"
    )


def test_environment_fingerprint_is_order_canonical() -> None:
    left = [
        {"package": "z-package", "version": "1"},
        {"package": "A_Package", "version": "2"},
    ]
    assert environment_fingerprint(left) == environment_fingerprint(list(reversed(left)))


def test_media_type_enforces_shared_160_byte_boundary() -> None:
    accepted = deepcopy(_fixture()["request"])
    accepted["media_type"] = ("a" * 32) + "/" + ("b" * 127)
    assert len(accepted["media_type"].encode("ascii")) == 160
    assert normalize_publish_request(accepted)["media_type"] == accepted["media_type"]

    rejected = deepcopy(accepted)
    rejected["media_type"] = ("a" * 33) + "/" + ("b" * 127)
    with pytest.raises(LibraryAdapterContractError) as error:
        normalize_publish_request(rejected)
    assert error.value.code == "media_type_invalid"


def test_sdk_rereads_and_serializes_one_exact_library_adapter_ref() -> None:
    ref = LibraryAdapterRef(
        owner="owner_alpha",
        library="shared_functions",
        name="utf8-result",
        version=3,
        adapter_id="adapter-id",
        adapter_version_id="adapter-version-id",
        content_hash="a" * 64,
        signature_hash="b" * 64,
    )

    class ExactRefDaemon:
        def __init__(self) -> None:
            self.capabilities: list[str] = []
            self.exact_reads: list[tuple[str, str, str | None, str | None]] = []
            self.run_payload: dict[str, Any] | None = None

        def require_runtime_port_adapters_capability(self) -> dict[str, Any]:
            return {"id": "spl.runtime_port_adapters.v1"}

        def require_runtime_adapter_semantic_override_capability(self) -> dict[str, Any]:
            return {"state": "supported", "version": 1, "reason": None}

        def require_library_adapter_capability(self, capability_id: str) -> dict[str, Any]:
            self.capabilities.append(capability_id)
            return {"id": capability_id}

        def get_library_adapter_version(
            self,
            adapter_id: str,
            adapter_version_id: str,
            *,
            owner_id: str | None = None,
            library: str | None = None,
        ) -> dict[str, Any]:
            self.exact_reads.append((adapter_id, adapter_version_id, owner_id, library))
            return {
                **ref.to_dict(),
                "directions": ["output"],
                "semantic_type": "builtins.str",
                "semantic_category": "text",
                "format_tag": None,
            }

        def signature(self, name: str, **selectors: Any) -> dict[str, Any]:
            del selectors
            return {
                "name": name,
                "kind": "function",
                "inputs": [],
                "input_order": [],
                "outputs": [
                    {
                        "name": "default",
                        "type": "builtins.str",
                        "selector": None,
                        "read": "result.value",
                    }
                ],
            }

        def run(self, name: str, **payload: Any) -> dict[str, Any]:
            self.run_payload = {"object": name, **payload}
            return {"id": "run-with-library-adapter", "status": "queued"}

    daemon = ExactRefDaemon()
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]

    client.submit(
        "render_text",
        source="local",
        adapters={"outputs": {"default": ref}},
    )

    assert daemon.exact_reads == [
        (
            "adapter-id",
            "adapter-version-id",
            "owner_alpha",
            "shared_functions",
        )
    ]
    assert daemon.run_payload is not None
    assert daemon.run_payload["runtime_library_adapter_refs"] == {
        "schema_version": 1,
        "bindings": [{"direction": "output", "port": "default", **ref.to_dict()}],
    }
    assert daemon.run_payload["runtime_adapter_semantic_advisories"] == {
        "schema_version": 1,
        "bindings": [
            {
                "direction": "output",
                "port": "default",
                "adapter_id": "adapter-id",
                "port_semantic_type": "builtins.str",
                "adapter_semantic_type": "builtins.str",
                "adapter_semantic_category": "text",
                "state": "recommended",
                "acknowledged": False,
                "acknowledgement_source": None,
            }
        ],
    }
    assert "save_source" not in json.dumps(daemon.run_payload, sort_keys=True)
    assert "load_source" not in json.dumps(daemon.run_payload, sort_keys=True)


def test_sdk_resolves_per_port_library_adapter_name_and_version_selectors() -> None:
    def record(version: int) -> dict[str, Any]:
        return {
            "owner": "owner_alpha",
            "library": "shared_functions",
            "name": "utf8-result",
            "version": version,
            "adapter_id": "adapter-id",
            "adapter_version_id": f"adapter-version-{version}",
            "content_hash": format(version, "064x"),
            "signature_hash": format(version + 10, "064x"),
            "directions": ["output"],
            "semantic_type": "builtins.str",
            "semantic_category": "text",
            "format_tag": None,
        }

    class SelectorDaemon:
        def __init__(self) -> None:
            self.run_payloads: list[dict[str, Any]] = []

        def require_runtime_port_adapters_capability(self) -> dict[str, Any]:
            return {"id": "spl.runtime_port_adapters.v1"}

        def require_runtime_adapter_semantic_override_capability(self) -> dict[str, Any]:
            return {"state": "supported", "version": 1, "reason": None}

        def require_library_adapter_capability(self, capability_id: str) -> dict[str, Any]:
            return {"id": capability_id}

        def list_library_adapters(self, **kwargs: Any) -> dict[str, Any]:
            assert kwargs["owner_id"] == "owner_alpha"
            assert kwargs["library"] == "shared_functions"
            assert kwargs["query"] == "utf8-result"
            return {"items": [record(3)], "next_cursor": None, "truncated": False}

        def library_adapter_versions(self, adapter_id: str, **kwargs: Any) -> dict[str, Any]:
            assert adapter_id == "adapter-id"
            assert kwargs["owner_id"] == "owner_alpha"
            assert kwargs["library"] == "shared_functions"
            return {"items": [record(3), record(1)], "next_cursor": None, "truncated": False}

        def get_library_adapter_version(
            self,
            adapter_id: str,
            adapter_version_id: str,
            **kwargs: Any,
        ) -> dict[str, Any]:
            assert adapter_id == "adapter-id"
            assert kwargs["owner_id"] == "owner_alpha"
            assert kwargs["library"] == "shared_functions"
            return record(int(adapter_version_id.rsplit("-", 1)[1]))

        def signature(self, name: str, **selectors: Any) -> dict[str, Any]:
            assert selectors["version_id"] == "object-version-id"
            return {
                "name": name,
                "kind": "function",
                "inputs": [],
                "input_order": [],
                "outputs": [
                    {
                        "name": "default",
                        "type": "builtins.str",
                        "selector": None,
                        "read": "result.value",
                    }
                ],
            }

        def run(self, name: str, **payload: Any) -> dict[str, Any]:
            self.run_payloads.append({"object": name, **payload})
            return {"id": f"run-{len(self.run_payloads)}", "status": "queued"}

    daemon = SelectorDaemon()
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]

    selectors: list[Any] = [
        "utf8-result",
        {"name": "utf8-result", "version": 1},
        {"name": "utf8-result", "version_id": "adapter-version-1"},
    ]
    for selector in selectors:
        client.submit(
            "render_text",
            owner="owner_alpha",
            library="shared_functions",
            version_id="object-version-id",
            source="local",
            adapters={"outputs": {"default": selector}},
        )

    selected_versions = [
        payload["runtime_library_adapter_refs"]["bindings"][0]["version"] for payload in daemon.run_payloads
    ]
    assert selected_versions == [3, 1, 1]
    assert [payload["version_id"] for payload in daemon.run_payloads] == [
        "object-version-id",
        "object-version-id",
        "object-version-id",
    ]


def test_old_daemon_compatible_explicit_selection_survives_but_mismatch_fails_before_mutation() -> None:
    class OldDaemon:
        def __init__(self) -> None:
            self.run_payload: dict[str, Any] | None = None

        def require_runtime_port_adapters_capability(self) -> dict[str, Any]:
            return {"state": "supported", "version": 1, "reason": None}

        def signature(self, name: str, **_selectors: Any) -> dict[str, Any]:
            return {
                "name": name,
                "kind": "function",
                "inputs": [],
                "input_order": [],
                "outputs": [{"name": "default", "type": "str", "selector": None, "read": "result"}],
            }

        def run(self, name: str, **payload: Any) -> dict[str, Any]:
            self.run_payload = {"object": name, **payload}
            return {"id": "old-daemon-run", "status": "queued"}

    daemon = OldDaemon()
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]
    client.submit("render", source="local", adapters={"outputs": {"default": TEXT_FILE_UTF8}})
    assert daemon.run_payload is not None
    assert "runtime_adapter_semantic_advisories" not in daemon.run_payload

    daemon.run_payload = None
    with pytest.raises(ClientError, match="semantic advisories"):
        client.submit("render", source="local", adapters={"outputs": {"default": BINARY_FILE}})
    assert daemon.run_payload is None
