from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

import spl.daemon.metadata as metadata_module
from spl.daemon.services.sync import SyncVisibilityService
from spl.daemon.store import RegistryStore


FUNCTION_YAML = """\
- !DFunction
  name: demo_obj
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 1
"""


PIPELINE_YAML = """\
- !DPipeline
  name: demo_obj
  nodes: []
  links: []
  aliases: []
"""


def test_metadata_yaml_rejects_python_object_tags_before_execution(tmp_path) -> None:
    marker = tmp_path / "metadata-loader-executed"
    payload = "__import__('pathlib').Path({!r}).write_text('pwned')".format(str(marker))
    yaml_text = (
        "!!python/object/apply:builtins.eval\n"
        "- |\n"
        f"  {payload}\n"
    )

    with pytest.raises(yaml.constructor.ConstructorError, match="python/object/apply"):
        metadata_module.extract_metadata(yaml_text, "demo_obj")

    assert not marker.exists()


def test_object_kind_is_stable_for_local_versions(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        first = store.register_object(
            "demo_obj",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )

        assert first["kind"] == "function"
        assert first["object_kind"] == "function"
        assert first["version_kind"] == "function"

        with pytest.raises(ValueError, match="object kind is stable"):
            store.register_object(
                "demo_obj",
                "demo_obj",
                "default",
                yaml_text=PIPELINE_YAML,
            )
    finally:
        store.close()


def test_server_mirror_exposes_source_name_aliases(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        record = store.register_object(
            "server.remote-object-1",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            origin="server",
            remote_owner_id="admin1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            remote_name="demo_obj",
        )

        assert record["name"] == "server.remote-object-1"
        assert record["local_registry_name"] == "server.remote-object-1"
        assert record["display_name"] == "demo_obj"
        assert record["remote_display_name"] == "demo_obj"
        assert record["remote_name"] == "demo_obj"
        assert record["source_owner_id"] == "admin1"
        assert record["source_object_id"] == "remote-object-1"
        assert record["source_object_name"] == "demo_obj"
        assert record["source_version_id"] == "remote-version-1"
        assert record["remote_identity"]["local_registry_name"] == (
            "server.remote-object-1"
        )
        assert record["remote_identity"]["source_object_name"] == "demo_obj"
        assert record["remote_identity"]["storage_remote_name"] == "demo_obj"
        assert record["compatibility"]["remote_name"]["replacement"] == (
            "source_object_name"
        )

        resolved = store.get_object("demo_obj")
        assert resolved["name"] == "server.remote-object-1"
        assert resolved["display_name"] == "demo_obj"
    finally:
        store.close()


def test_server_mirror_display_name_lookup_rejects_ambiguous_matches(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        store.register_object(
            "server.remote-object-1",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            origin="server",
            remote_owner_id="admin1",
            remote_object_id="remote-object-1",
            remote_version_id="remote-version-1",
            remote_name="demo_obj",
        )
        store.register_object(
            "server.remote-object-2",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
            origin="server",
            remote_owner_id="admin2",
            remote_object_id="remote-object-2",
            remote_version_id="remote-version-2",
            remote_name="demo_obj",
        )

        with pytest.raises(ValueError, match="ambiguous locally"):
            store.get_object("demo_obj")
    finally:
        store.close()


def test_object_decomposition_persists_functions_nodes_and_links(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        function_record = store.register_object(
            "demo_function",
            "demo_obj",
            "default",
            yaml_text=FUNCTION_YAML,
        )
        function_decomposition = store.get_object_decomposition(
            function_record["version_id"]
        )

        assert [item["name"] for item in function_decomposition["functions"]] == [
            "demo_obj"
        ]
        assert function_decomposition["functions"][0]["role"] == "top_level"
        assert function_decomposition["nodes"] == []
        assert function_decomposition["links"] == []

        bundle_path = (
            Path(__file__).resolve().parents[2]
            / "spl-core"
            / "spl"
            / "demo"
            / "_bundle.yaml"
        )
        pipeline_record = store.register_object(
            "test_pipeline",
            "test_pipeline",
            "default",
            yaml_text=bundle_path.read_text(encoding="utf-8"),
        )
        decomposition = store.get_object_decomposition(pipeline_record["version_id"])

        assert pipeline_record["kind"] == "pipeline"
        assert len(decomposition["functions"]) >= 3
        assert len(decomposition["nodes"]) == len(decomposition["functions"])
        assert len(decomposition["links"]) >= 1
        assert {node["kind"] for node in decomposition["nodes"]} == {"function"}
        assert pipeline_record["decomposition"]["links"] == decomposition["links"]
    finally:
        store.close()


def test_sync_visibility_exposes_retry_state(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        event = store.enqueue_sync_event(
            "object_version",
            {"name": "demo_obj", "version": 1},
        )
        failed = store.mark_sync_event_failed(event["id"], "server rejected event")

        assert failed["retry"]["will_retry"] is True
        assert failed["retry"]["next_attempt"] == 2
        assert failed["retry"]["last_error"] == "server rejected event"

        service = SyncVisibilityService(store)
        pending_events = service.pending_events()
        summary = service.summary(pending_events)

        assert len(pending_events) == 1
        assert pending_events[0]["retry"]["will_retry"] is True
        assert summary["pending"] == 1
        assert summary["retryable"] == 1
        assert summary["by_status"] == {"failed": 1}
        assert summary["by_kind"] == {"object_version": 1}
        assert summary["last_error"] == "server rejected event"
        assert summary["next_action"] == "will_retry_on_next_sync"
    finally:
        store.close()


def test_pipeline_decomposition_validation_rejects_bad_links(
    tmp_path,
    monkeypatch,
) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        def invalid_metadata(*args, **kwargs):
            return {
                "entrypoint": "bad_pipeline",
                "kind": "pipeline",
                "inputs": [],
                "outputs": [],
                "pipeline_nodes": [
                    {
                        "id": "node_a",
                        "kind": "function",
                        "function": "demo_obj",
                        "inputs": [],
                        "outputs": [],
                    }
                ],
                "internal_objects": [],
                "links": [
                    {
                        "from": {"node_id": "missing_node", "port": "x"},
                        "to": {"kind": "scalar", "value": 1},
                    }
                ],
                "distributions": [],
            }

        monkeypatch.setattr(metadata_module, "extract_metadata", invalid_metadata)

        with pytest.raises(ValueError, match="pipeline link target node"):
            store.register_object(
                "bad_pipeline",
                "bad_pipeline",
                "default",
                yaml_text=PIPELINE_YAML,
            )
    finally:
        store.close()
