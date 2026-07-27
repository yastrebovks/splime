import sys

import pytest

from spl.daemon.metadata import extract_metadata
from spl.daemon.store import RegistryStore


DUPLICATE_UUID_PIPELINE_YAML = """\
- !DPipeline
  name: duplicate_pipeline
  nodes:
  - !DNodeFunction
    uuid: 11111111-1111-4111-8111-111111111111
    func: left_function
  - !DNodeFunction
    uuid: 11111111-1111-4111-8111-111111111111
    func: right_function
  links: []
  aliases:
  - [left, 11111111-1111-4111-8111-111111111111]
  - [right, 11111111-1111-4111-8111-111111111111]
"""

NON_STRING_UUID_PIPELINE_YAML = """\
- !DPipeline
  name: invalid_uuid_pipeline
  nodes:
  - !DNodeFunction
    uuid: 7
    func: left_function
  - !DNodeFunction
    uuid: 7
    func: right_function
  links: []
  aliases: []
"""

SEMANTIC_DUPLICATE_UUID_PIPELINE_YAML = """\
- !DPipeline
  name: semantic_duplicate_pipeline
  nodes:
  - !DNodeFunction
    uuid: AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA
    func: left_function
  - !DNodeFunction
    uuid: aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa
    func: right_function
  links: []
  aliases:
  - [left, AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA]
  - [right, aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa]
"""


def _assert_duplicate_diagnostic(message: str) -> None:
    assert "pipeline has duplicate node uuid `11111111-1111-4111-8111-111111111111`" in message
    assert "`left`" in message
    assert "`right`" in message
    assert "<daemon-metadata>:pipeline.nodes[0]" in message
    assert "<daemon-metadata>:pipeline.nodes[1]" in message


def test_daemon_metadata_rejects_duplicate_uuid_before_keying_nodes() -> None:
    with pytest.raises(ValueError) as exc_info:
        extract_metadata(DUPLICATE_UUID_PIPELINE_YAML, "duplicate_pipeline")

    _assert_duplicate_diagnostic(str(exc_info.value))


def test_daemon_metadata_rejects_non_string_uuid_before_keying_nodes() -> None:
    with pytest.raises(ValueError) as exc_info:
        extract_metadata(NON_STRING_UUID_PIPELINE_YAML, "invalid_uuid_pipeline")

    assert str(exc_info.value) == (
        "pipeline node uuid must be a non-empty string (location: `<daemon-metadata>:pipeline.nodes[0]`)"
    )


def test_daemon_metadata_rejects_semantically_equal_uuid_spellings() -> None:
    with pytest.raises(ValueError) as exc_info:
        extract_metadata(SEMANTIC_DUPLICATE_UUID_PIPELINE_YAML, "semantic_duplicate_pipeline")

    message = str(exc_info.value)
    assert "duplicate node uuid `aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa`" in message
    assert "aliases: `left`, `right`" in message


def test_registration_rejects_duplicate_uuid_without_persisting_object(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    try:
        store.register_env("default", sys.executable)

        with pytest.raises(ValueError) as exc_info:
            store.register_object(
                "duplicate_pipeline",
                "duplicate_pipeline",
                "default",
                yaml_text=DUPLICATE_UUID_PIPELINE_YAML,
            )

        _assert_duplicate_diagnostic(str(exc_info.value))
        assert store.list_objects() == {}
        assert store._conn.execute("SELECT COUNT(*) FROM object_versions").fetchone()[0] == 0
    finally:
        store.close()


def test_registration_duplicate_diagnostic_uses_yaml_path_when_available(tmp_path) -> None:
    yaml_path = tmp_path / "duplicate-pipeline.yaml"
    yaml_path.write_text(DUPLICATE_UUID_PIPELINE_YAML, encoding="utf-8")
    store = RegistryStore(tmp_path / "daemon-home")
    try:
        store.register_env("default", sys.executable)

        with pytest.raises(ValueError) as exc_info:
            store.register_object(
                "duplicate_pipeline",
                "duplicate_pipeline",
                "default",
                yaml_path=str(yaml_path),
            )

        message = str(exc_info.value)
        assert "{}:pipeline.nodes[0]".format(yaml_path) in message
        assert "{}:pipeline.nodes[1]".format(yaml_path) in message
        assert store.list_objects() == {}
    finally:
        store.close()
