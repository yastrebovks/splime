"""Executable acceptance examples for immutable Library Adapters.

The optional pandas/openpyxl/Pillow examples skip in the lean unit-test
environment.  The owner acceptance interpreter carries those distributions,
so the same tests exercise real worker save/load calls there without changing
or installing into an SPL environment.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator, Mapping
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any

import pytest

from spl._runtime_port_adapters_client import prepare_client_runtime_adapters
from spl.adapters import FileInput
from spl.core.library_adapters import (
    LibraryAdapterRef,
    environment_fingerprint,
    normalize_publish_request,
)
from spl.daemon import worker as worker_module
from spl.daemon.signature import build_signature
from spl.daemon.store import RegistryStore


OWNER = "owner_alpha"
LIBRARY = "adapter_acceptance"

TEXT_OBJECT_YAML = """\
- !DFunction
  name: render_text
  inputs:
  - name: value
    type: str
  outputs:
  - name: default
    type: str
  body: |-
    return value.upper()
"""

TABLE_OBJECT_YAML = """\
- !DFunction
  name: echo_table
  inputs:
  - name: value
    type: pd.DataFrame
  outputs:
  - name: default
    type: pd.DataFrame
  body: |-
    return value
- !DImport
  module: pandas
  alias: pd
"""

IMAGE_OBJECT_YAML = """\
- !DFunction
  name: inspect_image
  inputs:
  - name: value
    type: Image.Image
  outputs:
  - name: default
    type: list
  body: |-
    return [value.size[0], value.size[1], list(value.getpixel((0, 0)))]
- !DImport
  module: PIL.Image
  alias: Image
"""

LEGACY_OBJECT_YAML = """\
- !DFunction
  name: legacy_json
  inputs:
  - name: value
    type: str
  outputs:
  - name: default
    type: str
  body: |-
    return value.upper()
"""


@pytest.fixture
def adapter_store(tmp_path: Path) -> Iterator[RegistryStore]:
    with RegistryStore(tmp_path / "daemon") as store:
        store.register_env("default", sys.executable)
        yield store


def _dependency(package: str, module: str) -> dict[str, Any]:
    return {
        "package": package,
        "version": importlib_metadata.version(package),
        "modules": [module],
    }


def _publication(
    *,
    name: str,
    semantic_type: str,
    semantic_category: str | None,
    save_source: str | None,
    load_source: str | None,
    dependencies: list[dict[str, Any]] | None = None,
    format_tag: str | None = None,
    media_type: str | None = None,
    preferred_extension: str | None = None,
) -> dict[str, Any]:
    dependencies = dependencies or []
    environment = [{"package": item["package"], "version": item["version"]} for item in dependencies]
    return normalize_publish_request(
        {
            "schema_version": 1,
            "name": name,
            "description": f"Acceptance Adapter {name}",
            "semantic_type": semantic_type,
            "semantic_category": semantic_category,
            "save_source": save_source,
            "load_source": load_source,
            "dependencies": dependencies,
            "format_tag": format_tag,
            "media_type": media_type,
            "preferred_extension": preferred_extension,
            "policy": {
                "local_custom_code": "allow",
                "remote_custom_code": "deny",
            },
            "publication_environment": {
                "fingerprint": environment_fingerprint(environment),
                "distributions": environment,
            },
        }
    )


def _publish(
    store: RegistryStore,
    publication: Mapping[str, Any],
) -> dict[str, Any]:
    return store.publish_library_adapter(
        publication,
        owner_id=OWNER,
        library=LIBRARY,
        publisher_id=OWNER,
    )


def _ref(version: Mapping[str, Any]) -> LibraryAdapterRef:
    return LibraryAdapterRef(
        **{
            key: version[key]
            for key in (
                "owner",
                "library",
                "name",
                "version",
                "adapter_id",
                "adapter_version_id",
                "content_hash",
                "signature_hash",
            )
        }
    )


def _register(
    store: RegistryStore,
    *,
    name: str,
    entrypoint: str,
    yaml_text: str,
) -> dict[str, Any]:
    return store.register_object(
        name,
        entrypoint,
        "default",
        yaml_text=yaml_text,
        owner_id=OWNER,
        library=LIBRARY,
    )


def _execute(
    store: RegistryStore,
    object_record: Mapping[str, Any],
    *,
    kwargs: dict[str, Any],
    adapters: dict[str, dict[str, Any]],
    versions: list[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    signature = build_signature(
        object_record,
        function=str(object_record["entrypoint"]),
    )
    prepared = prepare_client_runtime_adapters(
        signature=signature,
        args=None,
        kwargs=kwargs,
        output=None,
        adapters=adapters,
        adapter_policy={"custom_remote": "deny"},
        library_adapter_versions={str(item["adapter_version_id"]): item for item in versions},
    )
    state = store.create_run(
        str(object_record["name"]),
        args=prepared.args,
        kwargs=prepared.kwargs,
        object_version_id=str(object_record["version_id"]),
        function=str(object_record["entrypoint"]),
        owner_id=OWNER,
        library=LIBRARY,
        runtime_port_adapters=prepared.document,
        runtime_library_adapter_refs=prepared.runtime_library_adapter_refs,
        adapter_policy=prepared.adapter_policy,
    )
    exact_object = store.get_object_version(
        str(object_record["version_id"]),
        include_yaml=True,
    )
    run_dir = Path(state["run_dir"])
    object_yaml = run_dir / "acceptance-object.yaml"
    object_yaml.write_text(str(exact_object["yaml"]), encoding="utf-8")
    result = worker_module.execute(
        object_yaml=object_yaml,
        entrypoint=str(exact_object["entrypoint"]),
        input_path=run_dir / "input.json",
        result_path=run_dir / "acceptance-result.json",
        artifacts_dir=Path(state["artifacts_dir"]),
    )
    return state, result


def _artifact_bytes(result: Mapping[str, Any]) -> tuple[dict[str, Any], bytes]:
    [record] = result["runtime_port_adapter_outputs"]
    path = Path(result["artifacts"][record["name"]])
    return record, path.read_bytes()


def test_acceptance_load_only_image_upload_executes_in_worker(
    adapter_store: RegistryStore,
    tmp_path: Path,
) -> None:
    Image = pytest.importorskip("PIL.Image")
    dependency = _dependency("Pillow", "PIL")
    published = _publish(
        adapter_store,
        _publication(
            name="pillow-load-only",
            semantic_type="PIL.Image.Image",
            semantic_category="image",
            save_source=None,
            load_source="""def load_png(path: str):
    from PIL import Image
    with Image.open(path) as image:
        return image.copy()
""",
            dependencies=[dependency],
            format_tag="image.png.v1",
            media_type="image/png",
            preferred_extension=".png",
        ),
    )
    image_path = tmp_path / "input.png"
    Image.new("RGB", (2, 1), (12, 34, 56)).save(image_path, format="PNG")
    object_record = _register(
        adapter_store,
        name="image_acceptance",
        entrypoint="inspect_image",
        yaml_text=IMAGE_OBJECT_YAML,
    )

    _state, result = _execute(
        adapter_store,
        object_record,
        kwargs={"value": FileInput(image_path, media_type="image/png")},
        adapters={"inputs": {"value": _ref(published)}, "outputs": {}},
        versions=[published],
    )

    assert published["directions"] == ["input"]
    assert result["result"] == [2, 1, [12, 34, 56]]
    assert "runtime_port_adapter_outputs" not in result


def test_acceptance_save_only_utf8_text_is_downloadable_artifact(
    adapter_store: RegistryStore,
) -> None:
    published = _publish(
        adapter_store,
        _publication(
            name="utf8-save-only",
            semantic_type="builtins.str",
            semantic_category="text",
            save_source="""def save_utf8(path: str, value) -> None:
    from pathlib import Path as LocalPath
    LocalPath(path).write_text(value, encoding='utf-8')
""",
            load_source=None,
            format_tag="text.utf8.v1",
            media_type="text/plain",
            preferred_extension=".txt",
        ),
    )
    object_record = _register(
        adapter_store,
        name="text_acceptance",
        entrypoint="render_text",
        yaml_text=TEXT_OBJECT_YAML,
    )

    _state, result = _execute(
        adapter_store,
        object_record,
        kwargs={"value": "héllo"},
        adapters={"inputs": {}, "outputs": {"default": _ref(published)}},
        versions=[published],
    )

    record, body = _artifact_bytes(result)
    assert published["directions"] == ["output"]
    assert record["name"].endswith(".txt")
    assert record["adapter_id"] == f"library:{published['content_hash']}"
    assert body == "HÉLLO".encode()


def test_acceptance_pandas_semicolon_csv_round_trip_in_worker(
    adapter_store: RegistryStore,
    tmp_path: Path,
) -> None:
    pd = pytest.importorskip("pandas")
    dependency = _dependency("pandas", "pandas")
    published = _publish(
        adapter_store,
        _publication(
            name="pandas-csv-semicolon",
            semantic_type="pandas.core.frame.DataFrame",
            semantic_category="table",
            save_source="""def save_csv(path: str, value) -> None:
    value.to_csv(path, sep=';', index=False)
""",
            load_source="""def load_csv(path: str):
    import pandas as pd
    return pd.read_csv(path, sep=';')
""",
            dependencies=[dependency],
            format_tag="table.csv.semicolon.v1",
            media_type="text/csv",
            preferred_extension=".csv",
        ),
    )
    source = tmp_path / "input.csv"
    source.write_text("name;score\nalpha;1\nbeta;2\n", encoding="utf-8")
    object_record = _register(
        adapter_store,
        name="csv_acceptance",
        entrypoint="echo_table",
        yaml_text=TABLE_OBJECT_YAML,
    )

    _state, result = _execute(
        adapter_store,
        object_record,
        kwargs={"value": FileInput(source, media_type="text/csv")},
        adapters={
            "inputs": {"value": _ref(published)},
            "outputs": {"default": _ref(published)},
        },
        versions=[published],
    )

    record, body = _artifact_bytes(result)
    observed = pd.read_csv(__import__("io").BytesIO(body), sep=";")
    assert published["directions"] == ["input", "output"]
    assert record["name"].endswith(".csv")
    assert observed.to_dict(orient="records") == [
        {"name": "alpha", "score": 1},
        {"name": "beta", "score": 2},
    ]


def test_acceptance_pandas_xlsx_round_trip_in_worker(
    adapter_store: RegistryStore,
    tmp_path: Path,
) -> None:
    pd = pytest.importorskip("pandas")
    if importlib.util.find_spec("openpyxl") is None:
        pytest.skip("openpyxl is unavailable")
    dependencies = [
        _dependency("pandas", "pandas"),
        _dependency("openpyxl", "openpyxl"),
    ]
    published = _publish(
        adapter_store,
        _publication(
            name="pandas-xlsx",
            semantic_type="pandas.core.frame.DataFrame",
            semantic_category="table",
            save_source="""def save_xlsx(path: str, value) -> None:
    value.to_excel(path, index=False, engine='openpyxl')
""",
            load_source="""def load_xlsx(path: str):
    import pandas as pd
    return pd.read_excel(path, engine='openpyxl')
""",
            dependencies=dependencies,
            format_tag="table.xlsx.v1",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            preferred_extension=".xlsx",
        ),
    )
    source = tmp_path / "input.xlsx"
    pd.DataFrame([{"name": "alpha", "score": 1}]).to_excel(
        source,
        index=False,
        engine="openpyxl",
    )
    object_record = _register(
        adapter_store,
        name="xlsx_acceptance",
        entrypoint="echo_table",
        yaml_text=TABLE_OBJECT_YAML,
    )

    _state, result = _execute(
        adapter_store,
        object_record,
        kwargs={
            "value": FileInput(
                source,
                media_type=("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            )
        },
        adapters={
            "inputs": {"value": _ref(published)},
            "outputs": {"default": _ref(published)},
        },
        versions=[published],
    )

    record, body = _artifact_bytes(result)
    observed = pd.read_excel(__import__("io").BytesIO(body), engine="openpyxl")
    assert record["name"].endswith(".xlsx")
    assert observed.to_dict(orient="records") == [{"name": "alpha", "score": 1}]


def test_acceptance_null_optional_metadata_stays_null_through_worker(
    adapter_store: RegistryStore,
) -> None:
    published = _publish(
        adapter_store,
        _publication(
            name="exact-version-only-text",
            semantic_type="builtins.str",
            semantic_category="text",
            save_source="""def save_exact(path: str, value) -> None:
    from pathlib import Path as LocalPath
    LocalPath(path).write_bytes(value.encode('utf-8'))
""",
            load_source="""def load_exact(path: str):
    from pathlib import Path as LocalPath
    return LocalPath(path).read_bytes().decode('utf-8')
""",
        ),
    )
    object_record = _register(
        adapter_store,
        name="null_metadata_acceptance",
        entrypoint="render_text",
        yaml_text=TEXT_OBJECT_YAML,
    )

    _state, result = _execute(
        adapter_store,
        object_record,
        kwargs={"value": "nulls"},
        adapters={"inputs": {}, "outputs": {"default": _ref(published)}},
        versions=[published],
    )

    record, body = _artifact_bytes(result)
    assert published["format_tag"] is None
    assert published["media_type"] is None
    assert published["preferred_extension"] is None
    assert published["compatibility"]["cross_version"] == "exact_version_only"
    assert record["format_tag"] is None
    assert record["media_type"] is None
    assert body == b"NULLS"


def test_acceptance_new_adapter_version_does_not_republish_object(
    adapter_store: RegistryStore,
) -> None:
    first = _publish(
        adapter_store,
        _publication(
            name="versioned-text",
            semantic_type="builtins.str",
            semantic_category="text",
            save_source="""def save_versioned(path: str, value) -> None:
    from pathlib import Path as LocalPath
    LocalPath(path).write_text('v1:' + value, encoding='utf-8')
""",
            load_source=None,
            preferred_extension=".txt",
        ),
    )
    second = _publish(
        adapter_store,
        _publication(
            name="versioned-text",
            semantic_type="builtins.str",
            semantic_category="text",
            save_source="""def save_versioned(path: str, value) -> None:
    from pathlib import Path as LocalPath
    LocalPath(path).write_text('v2:' + value, encoding='utf-8')
""",
            load_source=None,
            preferred_extension=".txt",
        ),
    )
    object_record = _register(
        adapter_store,
        name="adapter_version_acceptance",
        entrypoint="render_text",
        yaml_text=TEXT_OBJECT_YAML,
    )
    object_version_id = str(object_record["version_id"])

    first_state, first_result = _execute(
        adapter_store,
        object_record,
        kwargs={"value": "same object"},
        adapters={"inputs": {}, "outputs": {"default": _ref(first)}},
        versions=[first],
    )
    second_state, second_result = _execute(
        adapter_store,
        object_record,
        kwargs={"value": "same object"},
        adapters={"inputs": {}, "outputs": {"default": _ref(second)}},
        versions=[second],
    )

    assert first["adapter_id"] == second["adapter_id"]
    assert (first["version"], second["version"]) == (1, 2)
    assert first_state["object_version_id"] == object_version_id
    assert second_state["object_version_id"] == object_version_id
    assert _artifact_bytes(first_result)[1] == b"v1:SAME OBJECT"
    assert _artifact_bytes(second_result)[1] == b"v2:SAME OBJECT"
    assert (
        first_state["manifest"]["runtime_library_adapter_refs"]["bindings"][0]["adapter_version_id"]
        == first["adapter_version_id"]
    )
    assert (
        second_state["manifest"]["runtime_library_adapter_refs"]["bindings"][0]["adapter_version_id"]
        == second["adapter_version_id"]
    )
    assert adapter_store.get_object_version(object_version_id)["version"] == 1


def test_acceptance_legacy_json_run_has_no_additive_adapter_fields(
    adapter_store: RegistryStore,
) -> None:
    object_record = _register(
        adapter_store,
        name="legacy_json_acceptance",
        entrypoint="legacy_json",
        yaml_text=LEGACY_OBJECT_YAML,
    )
    state = adapter_store.create_run(
        str(object_record["name"]),
        kwargs={"value": "legacy"},
        object_version_id=str(object_record["version_id"]),
        function=str(object_record["entrypoint"]),
        owner_id=OWNER,
        library=LIBRARY,
    )
    run_dir = Path(state["run_dir"])
    input_payload = (run_dir / "input.json").read_text(encoding="utf-8")
    exact_object = adapter_store.get_object_version(
        str(object_record["version_id"]),
        include_yaml=True,
    )
    object_yaml = run_dir / "legacy-object.yaml"
    object_yaml.write_text(str(exact_object["yaml"]), encoding="utf-8")
    result = worker_module.execute(
        object_yaml=object_yaml,
        entrypoint=str(exact_object["entrypoint"]),
        input_path=run_dir / "input.json",
        result_path=run_dir / "legacy-result.json",
        artifacts_dir=Path(state["artifacts_dir"]),
    )

    assert "runtime_library_adapter" not in input_payload
    assert "runtime_port_adapters" not in input_payload
    assert result == {"result": "LEGACY", "artifacts": {}}
