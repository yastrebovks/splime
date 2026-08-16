from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest

from spl.core.library_adapters import environment_fingerprint, normalize_publish_request
from spl.core.runtime_port_adapters import RuntimePortAdapterContractError
from spl.daemon.runtime_library_adapters import (
    materialize_claimed_runtime_library_adapters,
    prepare_runtime_library_adapters,
)
from spl.daemon.server import DaemonRuntime


SAVE_TEXT = """def save_text(path: str, value: str) -> None:
    from pathlib import Path as LocalPath
    LocalPath(path).write_text(value, encoding='utf-8')
"""
LOAD_TEXT = """def load_text(path: str) -> str:
    from pathlib import Path as LocalPath
    return LocalPath(path).read_text(encoding='utf-8')
"""
REF_KEYS = (
    "owner",
    "library",
    "name",
    "version",
    "adapter_id",
    "adapter_version_id",
    "content_hash",
    "signature_hash",
)


def _record(
    *,
    save_source: str | None = SAVE_TEXT,
    load_source: str | None = LOAD_TEXT,
    remote_policy: str = "allow",
) -> dict[str, Any]:
    prepared = normalize_publish_request(
        {
            "schema_version": 1,
            "name": "text-adapter",
            "description": "Safe adapter",
            "semantic_type": "builtins.str",
            "semantic_category": "text",
            "save_source": save_source,
            "load_source": load_source,
            "dependencies": [],
            "format_tag": None,
            "media_type": None,
            "preferred_extension": None,
            "policy": {
                "local_custom_code": "allow",
                "remote_custom_code": remote_policy,
            },
            "publication_environment": {
                "fingerprint": environment_fingerprint([]),
                "distributions": [],
            },
        }
    )
    return {
        "owner": "owner",
        "library": "default",
        "name": "text-adapter",
        "version": 7,
        "adapter_id": "adapter-local",
        "adapter_version_id": "adapter-version-local",
        **prepared,
        "effective_policy": {
            "local_custom": "allow",
            "remote_custom": remote_policy,
        },
    }


def _refs(record: dict[str, Any], *, direction: str = "output") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "bindings": [
            {
                "direction": direction,
                "port": "value" if direction == "input" else "default",
                **{key: record[key] for key in REF_KEYS},
            }
        ],
    }


def _runtime_document(*, direction: str = "output") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "bindings": [
            {
                "direction": direction,
                "port": "value" if direction == "input" else "default",
                "semantic_type": "builtins.str",
                "argument": "value" if direction == "input" else None,
                "input_name": "value" if direction == "input" else None,
                "result_path": [] if direction == "output" else [],
            }
        ],
    }


def test_prepare_rechecks_exact_source_hash_signature_policy_and_partial_direction() -> None:
    record = _record(save_source=SAVE_TEXT, load_source=None)
    prepared = prepare_runtime_library_adapters(
        _refs(record),
        runtime_document=_runtime_document(),
        object_distributions=[],
        resolve_exact=lambda _ref: record,
    )
    assert prepared.document["bindings"][0]["symbol"] == "save_text"
    assert list(prepared.source_bodies) == ["library-adapter-adapter-version-local.py"]

    stale = deepcopy(record)
    stale["save_source"] = SAVE_TEXT.replace("value,", "value.upper(),")
    with pytest.raises(RuntimePortAdapterContractError) as mismatch:
        prepare_runtime_library_adapters(
            _refs(record),
            runtime_document=_runtime_document(),
            object_distributions=[],
            resolve_exact=lambda _ref: stale,
        )
    assert mismatch.value.code == "library_adapter_hash_mismatch"

    with pytest.raises(RuntimePortAdapterContractError) as partial:
        prepare_runtime_library_adapters(
            _refs(record, direction="input"),
            runtime_document=_runtime_document(direction="input"),
            object_distributions=[],
            resolve_exact=lambda _ref: record,
        )
    assert partial.value.code == "library_adapter_direction_unsupported"

    denied = _record(remote_policy="deny")
    with pytest.raises(RuntimePortAdapterContractError) as policy:
        prepare_runtime_library_adapters(
            _refs(denied),
            runtime_document=_runtime_document(),
            object_distributions=[],
            resolve_exact=lambda _ref: denied,
            execution_target="remote",
        )
    assert policy.value.code == "library_adapter_remote_policy_denied"


def test_explicit_library_semantic_mismatch_does_not_remove_structural_hard_gates() -> None:
    record = _record(save_source=SAVE_TEXT, load_source=None)
    runtime_document = _runtime_document()
    runtime_document["bindings"][0]["semantic_type"] = "custom.Report"
    prepared = prepare_runtime_library_adapters(
        _refs(record),
        runtime_document=runtime_document,
        object_distributions=[],
        resolve_exact=lambda _ref: record,
    )
    assert prepared.document["bindings"][0]["semantic_type"] == "builtins.str"

    wrong_direction = _record(save_source=None, load_source=LOAD_TEXT)
    with pytest.raises(RuntimePortAdapterContractError) as direction:
        prepare_runtime_library_adapters(
            _refs(record),
            runtime_document=runtime_document,
            object_distributions=[],
            resolve_exact=lambda _ref: wrong_direction,
        )
    assert direction.value.code == "library_adapter_direction_unsupported"


def test_claimed_source_projection_is_closed_split_and_environment_bound() -> None:
    record = _record()
    claim_binding = {
        "direction": "output",
        "port": "default",
        **{key: record[key] for key in REF_KEYS},
        "semantic_type": record["semantic_type"],
        "semantic_category": record["semantic_category"],
        "format_tag": record["format_tag"],
        "media_type": record["media_type"],
        "preferred_extension": record["preferred_extension"],
        "dependencies": record["dependencies"],
        "symbols": record["symbols"],
        "effective_policy": record["effective_policy"],
        "save_source": record["save_source"],
        "load_source": record["load_source"],
    }
    refs, sources = materialize_claimed_runtime_library_adapters(
        {
            "schema_version": 1,
            "bindings": [claim_binding],
            "environment_fingerprint": "a" * 64,
        }
    )
    assert refs == _refs(record)
    assert len(sources) == 1
    assert sources[0]["policy"] == record["policy"]
    assert "effective_policy" not in sources[0]
    assert "direction" not in sources[0]

    unknown = deepcopy(claim_binding)
    unknown["unexpected"] = True
    with pytest.raises(RuntimePortAdapterContractError) as shape:
        materialize_claimed_runtime_library_adapters(
            {
                "schema_version": 1,
                "bindings": [unknown],
                "environment_fingerprint": "a" * 64,
            }
        )
    assert shape.value.code == "library_adapter_claim_shape"


class _LinkedStore:
    def library_adapter_remote_link(self, _ref: dict[str, Any]) -> dict[str, str]:
        return {
            "owner": "central-owner",
            "library": "default",
            "adapter_id": "adapter-central",
            "adapter_version_id": "adapter-version-central",
        }


class _CentralServer:
    def __init__(self, record: dict[str, Any]) -> None:
        self.record = record
        self.calls: list[tuple[object, ...]] = []

    def get_library_adapter_version(self, *args: object, **kwargs: object) -> dict[str, Any]:
        self.calls.append((*args, kwargs))
        return deepcopy(self.record)


class _SharedSourceStore:
    def resolve_library_adapter_ref(
        self,
        _ref: dict[str, Any],
        *,
        include_source: bool,
    ) -> dict[str, Any]:
        assert include_source is True
        raise KeyError("shared exact version is not mirrored locally")


class _SharedSourceServer:
    def __init__(self, record: dict[str, Any]) -> None:
        self.record = deepcopy(record)
        self.calls: list[tuple[dict[str, Any], str]] = []

    def resolve_local_library_adapter_source(
        self,
        ref: dict[str, Any],
        *,
        target_machine_id: str,
    ) -> dict[str, Any]:
        self.calls.append((dict(ref), target_machine_id))
        return {
            "schema": "spl.library-adapter-local-execution-source",
            "schema_version": 1,
            "execution_target": "local",
            "target_machine_id": target_machine_id,
            "adapter": deepcopy(self.record),
        }


def test_remote_admission_translates_synced_local_ids_only_after_exact_central_reread() -> None:
    local = _record()
    central = {
        **{key: local[key] for key in REF_KEYS},
        "owner": "central-owner",
        "version": 41,
        "adapter_id": "adapter-central",
        "adapter_version_id": "adapter-version-central",
    }
    server = _CentralServer(central)
    runtime = SimpleNamespace(store=_LinkedStore())
    translated = DaemonRuntime._translate_remote_library_adapter_refs(
        runtime,
        server,
        _refs(local),
    )
    [binding] = translated["bindings"]
    assert binding["version"] == 41
    assert binding["adapter_id"] == "adapter-central"
    assert binding["adapter_version_id"] == "adapter-version-central"
    assert server.calls[0][:4] == (
        "central-owner",
        "default",
        "adapter-central",
        "adapter-version-central",
    )

    contradictory = deepcopy(central)
    contradictory["content_hash"] = "f" * 64
    with pytest.raises(RuntimePortAdapterContractError) as mismatch:
        DaemonRuntime._translate_remote_library_adapter_refs(
            runtime,
            _CentralServer(contradictory),
            _refs(local),
        )
    assert mismatch.value.code == "remote_library_adapter_identity_mismatch"


def test_direct_sdk_local_shared_ref_uses_exact_central_source_contract() -> None:
    record = _record()
    server = _SharedSourceServer(record)
    runtime = SimpleNamespace(
        store=_SharedSourceStore(),
        _require_live_server_channel_credentials=lambda: {"machine_id": "local-machine"},
        _server_client_for_credentials=lambda _credentials: server,
    )

    sources = DaemonRuntime._resolve_local_library_adapter_sources(
        runtime,
        _refs(record),
    )

    assert sources == [record]
    assert server.calls == [
        (
            {key: record[key] for key in REF_KEYS},
            "local-machine",
        )
    ]

    server.record["content_hash"] = "f" * 64
    with pytest.raises(RuntimePortAdapterContractError) as mismatch:
        DaemonRuntime._resolve_local_library_adapter_sources(
            runtime,
            _refs(record),
        )
    assert mismatch.value.code == "library_adapter_ref_unverified"
