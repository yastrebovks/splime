import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from spl import Deployment, lift
from spl.adapters import TEXT_FILE_UTF8
from spl.core._common import Run
from spl.core.entities.adapter import Adapter, adapter_identity, make_key
from spl.core.entities.distribution import DDistribution
from spl.core.fingerprint import (
    FINGERPRINT_FORMAT_VERSION,
    canonical_fingerprint_bytes,
    inline_value_sha256,
    node_fingerprint,
    node_fingerprint_payload,
)
from spl.core.runtime_port_adapters import built_in_descriptor, runtime_port_adapter_fingerprint


class Box:
    pass


def _identity(value: int) -> int:
    return value


def _save_box(path: str, obj: Box) -> None:
    del path, obj


def _save_box_alt(path: str, obj: Box) -> None:
    del path, obj


def _load_box(path: str) -> Box:
    del path
    return Box()


def _load_box_alt(path: str) -> Box:
    del path
    return Box()


def _adapter_identity(*, format: str = "box", alt: bool = False) -> dict[str, Any]:
    adapter = Adapter(
        key=make_key(Box, format),
        save=_save_box_alt if alt else _save_box,
        load=_load_box_alt if alt else _load_box,
        py_type=Box,
        format=format,
        distributions=(DDistribution(package="boxlib", version="1"),),
    )
    return adapter_identity(adapter)


def _fingerprint(
    *,
    adapter_identities: Any | None = None,
    artifact_inputs: Any | None = None,
    inline_inputs: Any | None = None,
    input_ports: tuple[str, ...] = ("rows", "kwargs"),
    output_ports: tuple[str, ...] = ("default",),
    node_content: bytes = b'{"function":"normalize","body":"return value"}\n',
    node_version: str = "v1",
    execution_context_sha256: str | None = None,
) -> str:
    return node_fingerprint(
        node_content=node_content,
        node_version=node_version,
        input_ports=input_ports,
        output_ports=output_ports,
        adapter_identities=adapter_identities
        if adapter_identities is not None
        else {"rows": _adapter_identity(), "default": _adapter_identity(format="json")},
        artifact_inputs=artifact_inputs if artifact_inputs is not None else {"rows": "a" * 64, "metadata": "b" * 64},
        inline_inputs=inline_inputs if inline_inputs is not None else {"kwargs": {"b": 2, "a": 1}},
        execution_context_sha256=execution_context_sha256,
    )


def test_payload_contains_format_version_inside_hashed_payload() -> None:
    payload = node_fingerprint_payload(node_content=b"node", node_version="v1")
    canonical = canonical_fingerprint_bytes(payload)

    assert payload["fingerprint_format_version"] == FINGERPRINT_FORMAT_VERSION
    assert b'"fingerprint_format_version":1' in canonical
    assert len(node_fingerprint(node_content=b"node", node_version="v1")) == 64


def test_inline_value_sha256_uses_canonical_json_order() -> None:
    assert inline_value_sha256({"b": [2, 1], "a": True}) == inline_value_sha256({"a": True, "b": [2, 1]})


def test_kwargs_order_does_not_change_fingerprint() -> None:
    assert _fingerprint(inline_inputs={"kwargs": {"b": 2, "a": 1}}) == _fingerprint(
        inline_inputs={"kwargs": {"a": 1, "b": 2}}
    )


def test_edge_order_does_not_change_fingerprint() -> None:
    assert _fingerprint(artifact_inputs=[("metadata", "b" * 64), ("rows", "a" * 64)]) == _fingerprint(
        artifact_inputs=[("rows", "a" * 64), ("metadata", "b" * 64)]
    )


def test_adapter_order_does_not_change_fingerprint() -> None:
    adapters = [("rows", _adapter_identity()), ("default", _adapter_identity(format="json"))]

    assert _fingerprint(adapter_identities=adapters) == _fingerprint(adapter_identities=list(reversed(adapters)))


def test_adapter_change_changes_fingerprint() -> None:
    original = {"rows": _adapter_identity(), "default": _adapter_identity(format="json")}
    changed = {"rows": _adapter_identity(alt=True), "default": _adapter_identity(format="json")}

    assert _fingerprint(adapter_identities=original) != _fingerprint(adapter_identities=changed)


def test_input_sha_change_changes_fingerprint() -> None:
    assert _fingerprint(artifact_inputs={"rows": "a" * 64}) != _fingerprint(artifact_inputs={"rows": "b" * 64})


def test_run_adapter_context_changes_node_fingerprint_without_changing_legacy_payload() -> None:
    legacy_payload = node_fingerprint_payload(node_content=b"node")

    assert "execution_context_sha256" not in legacy_payload
    assert _fingerprint(execution_context_sha256="a" * 64) != _fingerprint(execution_context_sha256="b" * 64)


def test_pipeline_node_fingerprint_binds_runtime_adapter_document_without_changing_node_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPL_RUNS_HOME", str(tmp_path / "runs"))
    _, descriptor = built_in_descriptor(TEXT_FILE_UTF8)
    document: dict[str, Any] = {
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
                "size": 1,
                "sha256": hashlib.sha256(b"x").hexdigest(),
                "format_tag": descriptor["format_tag"],
                "semantic_type": "str",
                "adapter_id": descriptor["id"],
                "media_type": descriptor["presentation"]["media_type"],
                "content_base64": None,
                "staged_name": "input-value.txt",
            }
        ],
        "custom_bundle": None,
    }
    changed_digest = copy.deepcopy(document)
    changed_digest["inputs"][0]["sha256"] = hashlib.sha256(b"y").hexdigest()
    changed_binding = copy.deepcopy(document)
    changed_binding["bindings"][0]["resolution_source"] = "preset"
    policy = {"custom_remote": "deny"}
    contexts = [
        runtime_port_adapter_fingerprint(document, adapter_policy=policy),
        runtime_port_adapter_fingerprint(copy.deepcopy(document), adapter_policy=policy),
        runtime_port_adapter_fingerprint(changed_digest, adapter_policy=policy),
        runtime_port_adapter_fingerprint(changed_binding, adapter_policy=policy),
    ]
    pipeline = lift(_identity).alias("identity").render("runtime_fingerprint_pipeline")
    node = pipeline.aliases["identity"]
    node_content_sha256: set[str] = set()
    node_fingerprints: list[str] = []
    for context in contexts:
        run = Deployment(pipeline, _runtime_adapter_fingerprint_sha256=context).run(keep=True, value=7)
        assert isinstance(run, Run)
        with run:
            assert run.value("identity") == 7
        manifest = run.manifest_snapshot
        assert manifest is not None
        [record] = manifest["nodes"].values()
        node_fingerprints.append(record["fingerprint"]["sha256"])
        node_content_sha256.add(hashlib.sha256(run._node_content(node)).hexdigest())  # noqa: SLF001

    assert len(node_content_sha256) == 1
    assert node_fingerprints[0] == node_fingerprints[1]
    assert node_fingerprints[0] != node_fingerprints[2]
    assert node_fingerprints[0] != node_fingerprints[3]


def test_inline_value_change_changes_fingerprint() -> None:
    assert _fingerprint(inline_inputs={"kwargs": {"value": 1}}) != _fingerprint(inline_inputs={"kwargs": {"value": 2}})


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ({"node_version": "v1"}, {"node_version": "v2"}),
        ({"node_content": b"node-a"}, {"node_content": b"node-b"}),
        ({"input_ports": ("left", "right")}, {"input_ports": ("right", "left")}),
        ({"output_ports": ("value", "log")}, {"output_ports": ("log", "value")}),
    ],
)
def test_node_identity_or_port_order_change_changes_fingerprint(left: dict[str, Any], right: dict[str, Any]) -> None:
    assert _fingerprint(**left) != _fingerprint(**right)


def test_node_identity_can_stand_in_for_registered_content() -> None:
    assert node_fingerprint(node_identity="object-version-a") != node_fingerprint(node_identity="object-version-b")


def test_invalid_sha256_is_rejected_before_hashing() -> None:
    with pytest.raises(ValueError, match="64-character hex"):
        node_fingerprint(node_content=b"node", artifact_inputs={"rows": "not-a-sha"})


def test_payload_is_json_serializable() -> None:
    payload = node_fingerprint_payload(
        node_identity="object-version-a",
        adapter_identities={"rows": _adapter_identity()},
        artifact_inputs={"rows": "A" * 64},
        inline_inputs={"kwargs": {"left": 1, "right": 2}},
    )

    dumped = json.dumps(payload, sort_keys=True)

    assert '"sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' in dumped
