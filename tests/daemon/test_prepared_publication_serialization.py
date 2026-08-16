from __future__ import annotations

from spl.core.source_preparation import prepare_source, prepared_manifest
from spl.daemon.metadata import extract_object_ir_metadata, load_object_ir
from spl.daemon.prepared_validation import serialize_validated_prepared_object


def test_validated_prepared_object_serializes_to_existing_registry_yaml() -> None:
    result = prepare_source(
        {
            "schema": "splime.prepare-source-request/v1",
            "schema_version": 1,
            "sources": [
                {
                    "logical_path": "spl_generated/scale.py",
                    "language": "python",
                    "text": ("def scale(value: float, factor: float = 2) -> float:\n    return value * factor\n"),
                }
            ],
            "proposal_ir": {
                "schema": "splime.generated-plan/v1",
                "schema_version": 1,
                "kind": "function",
                "name": "scale",
                "nodes": [],
                "edges": [],
                "outputs": [],
                "dependencies": [],
                "adapters": [],
            },
            "entrypoints": ["scale"],
            "environment_request": None,
            "runtime_request": None,
            "target": {
                "python_language": "3.13",
                "spl_ir_schema": "splime.object-ir/v1",
                "preparation_protocol": 1,
            },
            "base": None,
        }
    )
    assert result["status"] == "ready"
    prepared = result["prepared"]

    yaml_text, entrypoint, runtime_config = serialize_validated_prepared_object(
        prepared_manifest(prepared),
        prepared["canonical_ir"],
    )
    loaded = load_object_ir(yaml_text, entrypoint)
    metadata = extract_object_ir_metadata(loaded)

    assert entrypoint == "scale"
    assert runtime_config is None
    assert metadata["kind"] == "function"
    assert metadata["inputs"][0]["name"] == "value"
    assert "return value * factor" in yaml_text


def test_semantic_port_metadata_serializes_types_without_rewriting_function_body() -> None:
    source = "def passthrough(frame):\n    return frame\n"
    result = prepare_source(
        {
            "schema": "splime.prepare-source-request/v1",
            "schema_version": 1,
            "sources": [{"logical_path": "spl_generated/passthrough.py", "language": "python", "text": source}],
            "proposal_ir": {
                "schema": "splime.generated-plan/v1",
                "schema_version": 1,
                "kind": "function",
                "name": "passthrough",
                "nodes": [],
                "edges": [],
                "outputs": [],
                "dependencies": [],
                "adapters": [],
                "semantic_ports": {
                    "schema": "splime.semantic-ports/v1",
                    "schema_version": 1,
                    "inputs": [{"name": "frame", "type": "pandas.DataFrame"}],
                    "outputs": [{"name": "default", "type": "pandas.DataFrame"}],
                },
            },
            "entrypoints": ["passthrough"],
            "environment_request": None,
            "runtime_request": None,
            "target": {
                "python_language": "3.13",
                "spl_ir_schema": "splime.object-ir/v1",
                "preparation_protocol": 1,
            },
            "base": None,
        }
    )
    assert result["status"] == "ready"
    prepared = result["prepared"]
    assert prepared["source_documents"][0]["normalized_text"] == source

    yaml_text, entrypoint, _ = serialize_validated_prepared_object(
        prepared_manifest(prepared),
        prepared["canonical_ir"],
    )
    loaded = load_object_ir(yaml_text, entrypoint)
    metadata = extract_object_ir_metadata(loaded)

    assert metadata["inputs"][0]["type"] == "pandas.DataFrame"
    assert metadata["outputs"][0]["type"] == "pandas.DataFrame"
    assert "return frame" in yaml_text
    assert "adapter" not in yaml_text.casefold()
