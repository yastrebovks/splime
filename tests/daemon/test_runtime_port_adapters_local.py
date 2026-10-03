from __future__ import annotations

import base64
import hashlib
import shutil
import sys
import time
from copy import deepcopy
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any

import pytest

from spl import SPLClient
from spl._client import RemoteRun
from spl._runtime_port_adapters_client import prepare_client_runtime_adapters
from spl.adapters import (
    BINARY_FILE,
    DATAFRAME_CSV_SEMICOLON,
    DATAFRAME_JSON_SPLIT,
    DATAFRAME_XLSX,
    DDistribution,
    OPAQUE_FILE,
    PNG_PILLOW,
    TEXT_FILE_UTF8,
    Adapter,
    ArtifactHandle,
    FileInput,
)
from spl.daemon.server import DaemonRuntime
from spl.daemon.signature import build_signature
from spl.daemon.store import RegistryStore, utc_now
from spl.core.runtime_port_adapters import RuntimePortAdapterContractError
from spl.daemon import worker as worker_module
from spl.daemon import server as server_module


def save_reverse_text(path: str, value: str) -> None:
    from pathlib import Path as LocalPath

    print("CUSTOM-ADAPTER-SECRET")
    LocalPath(path).write_text(value[::-1], encoding="utf-8")


def load_reverse_text(path: str) -> str:
    from pathlib import Path as LocalPath

    print("CUSTOM-ADAPTER-SECRET")
    return LocalPath(path).read_text(encoding="utf-8")[::-1]


def load_secret_failure(path: str) -> str:
    print("CUSTOM-FAILURE-SECRET")
    raise RuntimeError("CUSTOM-FAILURE-SECRET")


def save_custom_dataframe_pipe(path: str, value: object) -> None:
    import pandas as pd

    del pd
    value.to_csv(path, sep="|", encoding="utf-8", index=False)


def load_custom_dataframe_pipe(path: str) -> object:
    import pandas as pd

    return pd.read_csv(path, sep="|", encoding="utf-8")


def save_custom_pillow_png(path: str, value: object) -> None:
    from PIL import Image

    del Image
    value.save(path, format="PNG")


def load_custom_pillow_png(path: str) -> object:
    from PIL import Image

    with Image.open(path) as source:
        source.load()
        return source.copy()


class _NoopHeartbeats:
    def restore_server_heartbeat(self) -> None:
        pass

    def start_server_heartbeat(self, connection: object, *, token: str) -> None:
        pass

    def ensure_server_heartbeat(self, connection: object | None = None) -> None:
        pass

    def status(self, connection_id: str | None = None) -> dict[str, object]:
        return {"connection_id": connection_id}

    def stop_server_heartbeat(self, connection_id: str) -> None:
        pass

    def shutdown(self) -> None:
        pass


class _RuntimeDaemon:
    def __init__(self, runtime: DaemonRuntime, store: RegistryStore):
        self.runtime = runtime
        self.store = store
        self.run_calls: list[dict[str, Any]] = []
        self.signature_calls: list[dict[str, Any]] = []

    def require_runtime_port_adapters_capability(self) -> dict[str, Any]:
        return {"state": "supported", "version": 1, "reason": None}

    def require_runtime_adapter_semantic_override_capability(self) -> dict[str, Any]:
        return {"state": "supported", "version": 1, "reason": None}

    def signature(self, name: str, **selectors: Any) -> dict[str, Any]:
        self.signature_calls.append({"object": name, **selectors})
        version_id = selectors.get("version_id")
        if version_id is not None:
            record = self.store.get_object_version(version_id, include_yaml=False)
        else:
            record = self.store.get_object(name, version=selectors.get("version"), include_yaml=False)
        return build_signature(record)

    def run(self, name: str, **payload: Any) -> dict[str, Any]:
        self.run_calls.append({"object": name, **payload})
        accepted = {
            key: value
            for key, value in payload.items()
            if key
            in {
                "args",
                "kwargs",
                "output",
                "timeout_seconds",
                "version",
                "object_version_id",
                "function",
                "object_owner_id",
                "library",
                "source",
                "runtimes",
                "keep",
                "runtime_port_adapters",
                "runtime_adapter_semantic_advisories",
                "adapter_policy",
            }
        }
        if "version_id" in payload:
            accepted["object_version_id"] = payload["version_id"]
        accepted.pop("remote", None)
        accepted.pop("target_machine", None)
        accepted.pop("offline_policy", None)
        return self.runtime.start_run(name, **accepted)

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self.store.get_run(run_id)

    def wait_run(self, run_id: str, **options: Any) -> dict[str, Any]:
        del options
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            state = self.store.get_run(run_id)
            if state["status"] in {"succeeded", "failed"}:
                return state
            time.sleep(0.02)
        raise TimeoutError(run_id)

    def result(self, run_id: str) -> dict[str, Any]:
        return dict(self.store.get_run(run_id)["result"])

    def list_artifacts(self, run_id: str) -> list[str]:
        directory = Path(self.store.get_run(run_id)["artifacts_dir"])
        return sorted(item.name for item in directory.iterdir()) if directory.exists() else []

    def download_artifact(self, run_id: str, name: str, target: Path) -> Path:
        source = Path(self.store.get_run(run_id)["artifacts_dir"]) / name
        destination = target / name if target.is_dir() else target
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return destination

    def acknowledge_run_delivery(self, run_id: str) -> dict[str, Any]:
        return {"id": run_id}


def _yaml(name: str, type_name: str, body: str) -> str:
    return f"""\
- !DFunction
  name: {name}
  inputs:
  - name: value
    type: {type_name}
  outputs:
  - name: default
    type: {type_name}
  body: |-
    {body}
"""


def _ready(runtime: DaemonRuntime, record: dict[str, Any]) -> None:
    spec = runtime.environment_manager.build_spec(record)
    ready = runtime.store.upsert_environment_build(
        spec_hash=spec["spec_hash"],
        base_python=spec["base_python"],
        python_version=spec["python_version"],
        distributions=spec["distributions"],
        runtime_packages=spec["runtime_packages"],
        spec=spec["spec"],
        venv_path=Path(sys.executable).parent,
        python_path=Path(sys.executable),
        install_log_path=Path(spec["install_log_path"]),
        status="ready",
    )
    runtime.store.update_environment_build(
        ready["spec_hash"], status="ready", started_at=utc_now(), finished_at=utc_now()
    )


@pytest.fixture
def local_client(tmp_path: Path) -> tuple[SPLClient, _RuntimeDaemon, DaemonRuntime, RegistryStore]:
    store = RegistryStore(tmp_path / "daemon-home")
    store.register_env("default", sys.executable)
    runtime = DaemonRuntime(store, auto_build_envs=False, heartbeat_service=_NoopHeartbeats())
    records = [
        runtime.register_object(
            "text_echo", "text_echo", "default", yaml_text=_yaml("text_echo", "str", "return value.upper()")
        ),
        runtime.register_object(
            "binary_echo",
            "binary_echo",
            "default",
            yaml_text=_yaml("binary_echo", "bytes", "return value + bytes([255])"),
        ),
        runtime.register_object(
            "binary_identity",
            "binary_identity",
            "default",
            yaml_text=_yaml("binary_identity", "bytes", "return value"),
        ),
        runtime.register_object(
            "opaque_echo",
            "opaque_echo",
            "default",
            yaml_text=_yaml("opaque_echo", "null", "return value"),
        ),
        runtime.register_object(
            "opaque_path_echo",
            "opaque_path_echo",
            "default",
            yaml_text="""\
- !DFunction
  name: opaque_path_echo
  inputs:
  - name: value
    type: pathlib.Path
  outputs:
  - name: default
    type: pathlib.Path
  body: |-
    assert isinstance(value, pathlib.Path)
    return value
- !DImport
  module: pathlib
  alias: pathlib
""",
        ),
    ]
    for record in records:
        _ready(runtime, record)
    daemon = _RuntimeDaemon(runtime, store)
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]
    try:
        yield client, daemon, runtime, store
    finally:
        runtime.shutdown()
        store.close()


def test_text_call_and_binary_submit_collect_decode_locally(local_client: Any, tmp_path: Path) -> None:
    client, _, _, _ = local_client
    text = client.call(
        "text_echo",
        kwargs={"value": "hello"},
        adapters={"inputs": {"value": TEXT_FILE_UTF8}, "outputs": {"default": TEXT_FILE_UTF8}},
        artifacts_dir=tmp_path / "text-results",
        progress=False,
    )
    assert text.output == "HELLO"
    assert next(iter(text.downloaded_artifacts.values())).read_bytes() == b"HELLO"

    run = client.submit(
        "binary_echo",
        kwargs={"value": b"\x00\xfe"},
        adapters={"inputs": {"value": BINARY_FILE}, "outputs": {"default": BINARY_FILE}},
    )
    binary = run.collect(progress=False)
    assert binary.output == b"\x00\xfe\xff"


def test_opaque_file_never_crosses_admission_as_caller_path(local_client: Any, tmp_path: Path) -> None:
    client, daemon, _, store = local_client
    source = tmp_path / "private-source.bin"
    source.write_bytes(b"\x00\xffopaque")
    result = client.call(
        "opaque_echo",
        kwargs={"value": FileInput(source, media_type="application/octet-stream")},
        adapters={"inputs": {"value": OPAQUE_FILE}, "outputs": {"default": OPAQUE_FILE}},
        progress=False,
    )
    assert isinstance(result.output, ArtifactHandle)
    assert result.output.sha256
    call = daemon.run_calls[-1]
    assert str(source) not in repr(call)
    state = store.get_run(result.run["id"])
    assert str(source) not in repr(state["input"])


def test_explicit_pathlib_path_function_round_trips_opaque_file(
    local_client: Any,
    tmp_path: Path,
) -> None:
    client, daemon, _, store = local_client
    source = tmp_path / "arbitrary-private-input.bin"
    content = b"\x00\xffopaque-path\x80"
    source.write_bytes(content)

    result = client.call(
        "opaque_path_echo",
        kwargs={"value": FileInput(source, media_type="application/octet-stream")},
        adapters={"inputs": {"value": OPAQUE_FILE}, "outputs": {"default": OPAQUE_FILE}},
        artifacts_dir=tmp_path / "opaque-path-result",
        progress=False,
    )

    assert isinstance(result.output, ArtifactHandle)
    assert result.output.path is not None
    assert result.output.path.read_bytes() == content
    assert next(iter(result.downloaded_artifacts.values())).read_bytes() == content
    assert str(source) not in repr(daemon.run_calls[-1])
    assert str(source) not in repr(store.get_run(result.run["id"]))


def test_empty_binary_and_opaque_files_complete_the_full_artifact_lifecycle(
    local_client: Any,
    tmp_path: Path,
) -> None:
    client, daemon, _, store = local_client
    binary = client.call(
        "binary_identity",
        kwargs={"value": b""},
        adapters={"inputs": {"value": BINARY_FILE}, "outputs": {"default": BINARY_FILE}},
        artifacts_dir=tmp_path / "empty-binary-result",
        progress=False,
    )
    assert binary.output == b""
    assert len(binary.downloaded_artifacts) == 1
    assert next(iter(binary.downloaded_artifacts.values())).read_bytes() == b""

    source = tmp_path / "private-empty-source.bin"
    source.write_bytes(b"")
    opaque = client.call(
        "opaque_echo",
        kwargs={"value": FileInput(source, media_type="application/octet-stream")},
        adapters={"inputs": {"value": OPAQUE_FILE}, "outputs": {"default": OPAQUE_FILE}},
        artifacts_dir=tmp_path / "empty-opaque-result",
        progress=False,
    )
    assert isinstance(opaque.output, ArtifactHandle)
    assert opaque.output.size == 0
    assert opaque.output.sha256 == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert opaque.output.path is not None
    assert opaque.output.path.read_bytes() == b""
    assert len(opaque.downloaded_artifacts) == 1
    assert next(iter(opaque.downloaded_artifacts.values())).read_bytes() == b""
    assert str(source) not in repr(daemon.run_calls[-1])
    assert str(source) not in repr(store.get_run(opaque.run["id"]))


def test_explicit_mismatch_client_save_failure_has_no_run_side_effect(local_client: Any) -> None:
    client, _, _, store = local_client
    before = store.list_runs()
    with pytest.raises(ValueError, match="client_save_failed"):
        client.submit(
            "text_echo",
            kwargs={"value": "hello"},
            adapters={"inputs": {"value": BINARY_FILE}},
        )
    assert store.list_runs() == before


@pytest.mark.parametrize(
    ("bindings", "expected_code"),
    [
        ({"missing": TEXT_FILE_UTF8}, "unknown_port"),
        ({"left.value": TEXT_FILE_UTF8, "value": TEXT_FILE_UTF8}, "duplicate_port"),
    ],
)
def test_unknown_and_duplicate_canonical_public_bindings_have_no_run_side_effect(
    local_client: Any,
    monkeypatch: pytest.MonkeyPatch,
    bindings: dict[str, str],
    expected_code: str,
) -> None:
    client, daemon, _, store = local_client
    pipeline_signature = {
        "kind": "pipeline",
        "aliases": [{"name": "left", "node_id": "node-left"}],
        "input_order": ["value"],
        "inputs": [
            {
                "name": "value",
                "type": "str",
                "required": True,
                "default": None,
                "sources": [{"node_id": "node-left", "port": "value"}],
            }
        ],
        "outputs": [{"name": "default", "type": "str"}],
    }
    monkeypatch.setattr(daemon, "signature", lambda *args, **kwargs: pipeline_signature)
    before_runs = store.list_runs()
    before_calls = list(daemon.run_calls)

    with pytest.raises(RuntimePortAdapterContractError) as raised:
        client.submit(
            "text_echo",
            kwargs={"value": "hello"},
            adapters={"inputs": bindings},
        )

    assert raised.value.code == expected_code
    assert daemon.run_calls == before_calls
    assert store.list_runs() == before_runs


def test_missing_custom_distribution_fails_preflight_without_run_side_effect(
    local_client: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, daemon, _, store = local_client
    package = "spl-runtime-adapter-package-that-is-not-installed"
    original_version = importlib_metadata.version

    def installed_version(candidate: str) -> str:
        if candidate == package:
            raise importlib_metadata.PackageNotFoundError(candidate)
        return original_version(candidate)

    monkeypatch.setattr(importlib_metadata, "version", installed_version)
    adapter = Adapter(
        key="builtins.str@missing-dependency.v1",
        save=save_reverse_text,
        load=load_reverse_text,
        py_type=str,
        format="missing-dependency.v1",
        distributions=(DDistribution(package=package, version="1.0.0"),),
    )
    before_runs = store.list_runs()
    before_calls = list(daemon.run_calls)

    with pytest.raises(RuntimePortAdapterContractError) as raised:
        client.submit(
            "text_echo",
            kwargs={"value": "hello"},
            adapters={"inputs": {"value": adapter}},
        )

    assert raised.value.code == "missing_dependency"
    assert raised.value.stage == "client_save"
    assert daemon.run_calls == before_calls
    assert store.list_runs() == before_runs


def test_png_file_input_rejects_media_mismatch_and_corrupt_extension_spoof(
    local_client: Any,
    tmp_path: Path,
) -> None:
    image_module = pytest.importorskip("PIL.Image")
    client, daemon, runtime, store = local_client
    image_record = runtime.register_object(
        "inspect_png",
        "inspect_png",
        "default",
        yaml_text="""\
- !DFunction
  name: inspect_png
  inputs:
  - name: image
    type: Image.Image
  outputs:
  - name: default
    type: str
  body: |-
    return f"{image.size[0]}x{image.size[1]}"
- !DImport
  module: PIL.Image
  alias: Image
""",
    )
    _ready(runtime, image_record)
    adapters = {"inputs": {"image": PNG_PILLOW}, "outputs": {"default": TEXT_FILE_UTF8}}

    valid_png = tmp_path / "valid.png"
    image_module.new("RGB", (2, 3), (1, 2, 3)).save(valid_png, format="PNG")
    before_runs = store.list_runs()
    before_calls = list(daemon.run_calls)
    with pytest.raises(RuntimePortAdapterContractError) as media_error:
        client.submit(
            "inspect_png",
            kwargs={"image": FileInput(valid_png, media_type="image/jpeg")},
            adapters=adapters,
        )
    assert media_error.value.code == "binding_input_identity"
    assert daemon.run_calls == before_calls
    assert store.list_runs() == before_runs

    corrupt_png = tmp_path / "extension-spoof.png"
    corrupt_png.write_bytes(b"this is not PNG content")
    run = client.submit(
        "inspect_png",
        kwargs={"image": FileInput(corrupt_png, media_type="image/png")},
        adapters=adapters,
    )
    with pytest.raises(RuntimeError, match="input_adapter_load_failed"):
        run.collect(progress=False)
    state = store.get_run(run.id)
    assert state["status"] == "failed"
    assert state["manifest"]["runtime_port_adapters"]["failure"]["stage"] == "worker_load"


def test_input_cannot_collide_with_custom_bundle_namespace_before_run_mutation(local_client: Any) -> None:
    _, daemon, runtime, store = local_client
    adapter = Adapter(
        key="builtins.str@reverse-text.v1",
        save=save_reverse_text,
        load=load_reverse_text,
        py_type=str,
        format="reverse-text.v1",
    )
    plan = prepare_client_runtime_adapters(
        signature=daemon.signature("text_echo"),
        args=None,
        kwargs={"value": "collision"},
        output=None,
        adapters={"inputs": {"value": adapter}, "outputs": {"default": adapter}},
        adapter_policy=None,
    )
    malicious = deepcopy(plan.document)
    malicious["inputs"][0]["name"] = "custom-adapters.py"
    input_binding = next(binding for binding in malicious["bindings"] if binding["direction"] == "input")
    input_binding["input_name"] = "custom-adapters.py"
    before = store.list_runs()

    with pytest.raises(RuntimePortAdapterContractError, match="custom adapter bundle namespace"):
        runtime.start_run(
            "text_echo",
            kwargs=plan.kwargs,
            runtime_port_adapters=malicious,
            adapter_policy=plan.adapter_policy,
        )

    assert store.list_runs() == before


def test_sdk_explicit_mismatch_emits_acknowledged_advisory_without_new_argument(local_client: Any) -> None:
    _, daemon, _, _ = local_client
    plan = prepare_client_runtime_adapters(
        signature=daemon.signature("text_echo"),
        args=None,
        kwargs={"value": "semantic-review"},
        output=None,
        adapters={"inputs": {"value": TEXT_FILE_UTF8}, "outputs": {"default": BINARY_FILE}},
        adapter_policy=None,
    )
    assert plan.runtime_adapter_semantic_advisories == {
        "schema_version": 1,
        "bindings": [
            {
                "direction": "input",
                "port": "value",
                "adapter_id": TEXT_FILE_UTF8,
                "port_semantic_type": "str",
                "adapter_semantic_type": "builtins.str",
                "adapter_semantic_category": "text",
                "state": "recommended",
                "acknowledged": False,
                "acknowledgement_source": None,
            },
            {
                "direction": "output",
                "port": "default",
                "adapter_id": BINARY_FILE,
                "port_semantic_type": "str",
                "adapter_semantic_type": "builtins.bytes",
                "adapter_semantic_category": "binary",
                "state": "declared_type_mismatch",
                "acknowledged": True,
                "acknowledgement_source": "sdk_explicit_selection",
            },
        ],
    }


def test_embedded_preset_mismatch_is_synthesized_as_authoritative_intent(local_client: Any) -> None:
    _, daemon, _, store = local_client
    plan = prepare_client_runtime_adapters(
        signature=daemon.signature("text_echo"),
        args=None,
        kwargs={"value": "embedded"},
        output=None,
        adapters={"inputs": {"value": TEXT_FILE_UTF8}, "outputs": {"default": BINARY_FILE}},
        adapter_policy=None,
    )
    document = deepcopy(plan.document)
    for binding in document["bindings"]:
        binding["resolution_source"] = "preset" if binding["direction"] == "output" else "system_default"
    state = store.create_run(
        "text_echo",
        kwargs=plan.kwargs,
        runtime_port_adapters=document,
        adapter_policy=plan.adapter_policy,
    )
    assert state["manifest"]["runtime_adapter_semantic_advisories"] == {
        "schema_version": 1,
        "bindings": [
            {
                "direction": "output",
                "port": "default",
                "adapter_id": BINARY_FILE,
                "port_semantic_type": "str",
                "adapter_semantic_type": "builtins.bytes",
                "adapter_semantic_category": "binary",
                "state": "declared_type_mismatch",
                "acknowledged": True,
                "acknowledgement_source": "embedded_contract",
            }
        ],
    }


def test_forged_custom_source_is_rejected_before_daemon_run_mutation(local_client: Any) -> None:
    _, daemon, runtime, store = local_client
    adapter = Adapter(
        key="builtins.str@forged-nested-lambda.v1",
        save=save_reverse_text,
        load=load_reverse_text,
        py_type=str,
        format="forged-nested-lambda.v1",
    )
    plan = prepare_client_runtime_adapters(
        signature=daemon.signature("text_echo"),
        args=None,
        kwargs={"value": "forged"},
        output=None,
        adapters={"inputs": {"value": adapter}, "outputs": {"default": adapter}},
        adapter_policy=None,
    )
    forged = deepcopy(plan.document)
    bundle = forged["custom_bundle"]
    source = base64.b64decode(bundle["content_base64"], validate=True)
    needle = b"def save_reverse_text(path: str, value: str) -> None:\n"
    source = source.replace(
        needle,
        needle + b"    transform = lambda candidate: candidate\n",
    )
    assert len(source) > bundle["size"]
    digest = hashlib.sha256(source).hexdigest()
    bundle.update(
        {
            "size": len(source),
            "sha256": digest,
            "content_base64": base64.b64encode(source).decode("ascii"),
        }
    )
    for binding in forged["bindings"]:
        if binding["adapter"]["kind"] == "custom":
            binding["adapter"]["id"] = f"custom:{digest}"
            binding["adapter"]["bundle_sha256"] = digest
    for runtime_input in forged["inputs"]:
        runtime_input["adapter_id"] = f"custom:{digest}"
    before = store.list_runs()

    with pytest.raises(RuntimePortAdapterContractError) as raised:
        runtime.start_run(
            "text_echo",
            kwargs=plan.kwargs,
            runtime_port_adapters=forged,
            adapter_policy=plan.adapter_policy,
        )

    assert raised.value.code == "custom_nested_callable"
    assert raised.value.stage == "environment_preflight"
    assert store.list_runs() == before


def test_exact_version_id_uses_the_same_version_signature(local_client: Any) -> None:
    client, _, runtime, store = local_client
    first = store.get_object("text_echo", version=1, include_yaml=False)
    second = runtime.register_object(
        "text_echo",
        "text_echo",
        "default",
        yaml_text=_yaml("text_echo", "str", "return value + '-v2'").replace("name: value", "name: other"),
    )
    _ready(runtime, second)

    result = client.call(
        "text_echo",
        version_id=first["version_id"],
        kwargs={"value": "v1"},
        adapters={"inputs": {"value": TEXT_FILE_UTF8}, "outputs": {"default": TEXT_FILE_UTF8}},
        progress=False,
    )
    assert result.output == "V1"


def test_omitted_version_uses_authoritative_current_and_binds_immutable_version(
    local_client: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, daemon, runtime, store = local_client
    authoritative = store.get_object("text_echo", version=1, include_yaml=False)
    newer_local = runtime.register_object(
        "text_echo",
        "text_echo",
        "default",
        yaml_text=_yaml("text_echo", "str", "return value + '-newer-local'"),
    )
    _ready(runtime, newer_local)
    refresh_calls: list[dict[str, Any]] = []

    def authoritative_refresh(
        object_name: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        refresh_calls.append({"object": object_name, "version": version, "owner_id": owner_id, "library": library})
        return {"current_version": authoritative}

    monkeypatch.setattr(runtime, "refresh_server_object_if_available", authoritative_refresh)

    result = client.call(
        "text_echo",
        kwargs={"value": "v1"},
        adapters={"inputs": {"value": TEXT_FILE_UTF8}, "outputs": {"default": TEXT_FILE_UTF8}},
        progress=False,
    )

    assert result.output == "V1"
    assert daemon.signature_calls[-1] == {
        "object": "text_echo",
        "version": None,
        "owner_id": None,
        "library": None,
        "function": None,
    }
    assert refresh_calls == [{"object": "text_echo", "version": None, "owner_id": None, "library": None}]
    assert "version" not in daemon.run_calls[-1]
    assert "version_id" not in daemon.run_calls[-1]
    assert "object_version_id" not in daemon.run_calls[-1]
    assert result.run["object_version_id"] == authoritative["version_id"]
    assert result.run["object_version"] == authoritative["version"]
    state = store.get_run(result.run["id"])
    assert state["object_version_id"] == authoritative["version_id"]
    assert state["object_version"] == authoritative["version"]
    assert state["manifest"]["pipeline"]["object_version_id"] == authoritative["version_id"]
    assert state["object_version_id"] != newer_local["version_id"]


def test_worker_output_rejects_symlink_without_following_it(tmp_path: Path, monkeypatch: Any) -> None:
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"must-not-be-read")

    class _SymlinkAdapter:
        @staticmethod
        def save(path: str, value: Any) -> None:
            del value
            Path(path).symlink_to(outside)

        @staticmethod
        def load(path: str) -> Any:
            return Path(path)

    monkeypatch.setattr(worker_module, "get_builtin_adapter", lambda adapter_id: _SymlinkAdapter())
    binding = {
        "port": "default",
        "result_path": [],
        "adapter": {
            "kind": "builtin",
            "id": TEXT_FILE_UTF8,
            "format_tag": "spl.text.utf8.v1",
            "presentation": {"preferred_extension": ".txt", "media_type": "text/plain"},
        },
    }
    with pytest.raises(RuntimeError, match="bounded regular file"):
        worker_module._materialize_runtime_output(  # noqa: SLF001 - adversarial worker boundary.
            "value",
            binding,
            artifacts_dir=tmp_path / "artifacts",
            used_names=set(),
        )
    assert outside.read_bytes() == b"must-not-be-read"


def test_custom_adapter_executes_only_in_worker_and_console_output_is_redacted(local_client: Any) -> None:
    client, _, _, store = local_client
    adapter = Adapter(
        key="builtins.str@reverse-text.v1",
        save=save_reverse_text,
        load=load_reverse_text,
        py_type=str,
        format="reverse-text.v1",
    )
    result = client.call(
        "text_echo",
        kwargs={"value": "custom"},
        adapters={"inputs": {"value": adapter}, "outputs": {"default": adapter}},
        progress=False,
    )
    assert result.output == "CUSTOM"
    state = store.get_run(result.run["id"])
    assert "CUSTOM-ADAPTER-SECRET" not in repr(state)
    assert state["manifest"]["runtime_port_adapters"]["bindings"][0]["bundle_sha256"]


def test_remote_custom_manifest_marks_used_only_after_worker_invocation(local_client: Any) -> None:
    _, daemon, runtime, store = local_client
    adapter = Adapter(
        key="builtins.str@reverse-text.v1",
        save=save_reverse_text,
        load=load_reverse_text,
        py_type=str,
        format="reverse-text.v1",
    )
    plan = prepare_client_runtime_adapters(
        signature=daemon.signature("text_echo"),
        args=None,
        kwargs={"value": "remote"},
        output=None,
        adapters={"inputs": {"value": adapter}, "outputs": {"default": adapter}},
        adapter_policy={"custom_remote": "allow"},
    )
    initial = runtime.start_run(
        "text_echo",
        kwargs=plan.kwargs,
        runtime_port_adapters=plan.document,
        adapter_policy=plan.adapter_policy,
        custom_remote_allowed=True,
    )
    assert initial["manifest"]["runtime_port_adapters"]["custom_remote"] == {
        "requested": True,
        "allowed": True,
        "used": False,
    }

    final = daemon.wait_run(initial["id"])
    assert final["status"] == "succeeded"
    assert store.get_run(initial["id"])["manifest"]["runtime_port_adapters"]["custom_remote"] == {
        "requested": True,
        "allowed": True,
        "used": True,
    }


def test_custom_adapter_failure_is_closed_and_manifested_without_secret(local_client: Any) -> None:
    client, _, _, store = local_client
    adapter = Adapter(
        key="builtins.str@secret-failure.v1",
        save=save_reverse_text,
        load=load_secret_failure,
        py_type=str,
        format="secret-failure.v1",
    )
    run = client.submit(
        "text_echo",
        kwargs={"value": "custom"},
        adapters={"inputs": {"value": adapter}},
    )
    with pytest.raises(RuntimeError, match="input_adapter_load_failed"):
        run.collect(progress=False)
    state = store.get_run(run.id)
    assert "CUSTOM-FAILURE-SECRET" not in repr(state)
    assert state["manifest"]["runtime_port_adapters"]["failure"]["stage"] == "worker_load"


@pytest.mark.parametrize(
    ("failure_point", "expected_stage"),
    [("environment", "environment_preflight"), ("terminal_verify", "worker_save")],
)
def test_adapted_control_plane_failures_retain_lifecycle_stage(
    local_client: Any,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
    expected_stage: str,
) -> None:
    client, _, runtime, _ = local_client
    if failure_point == "environment":
        original_backend_for = runtime.runtime_backends.backend_for

        def failing_backend_for(record: dict[str, Any]) -> Any:
            backend = original_backend_for(record)

            def fail_environment(*args: Any, **kwargs: Any) -> dict[str, Any]:
                del args, kwargs
                raise RuntimeError("PRIVATE-ENVIRONMENT-DETAIL")

            monkeypatch.setattr(backend, "ensure_ready", fail_environment)
            return backend

        monkeypatch.setattr(runtime.runtime_backends, "backend_for", failing_backend_for)
    else:

        def fail_terminal_verify(*args: Any, **kwargs: Any) -> None:
            del args, kwargs
            raise RuntimeError("PRIVATE-OUTPUT-INTEGRITY-DETAIL")

        monkeypatch.setattr(server_module, "_verify_terminal_runtime_output_files", fail_terminal_verify)

    run = client.submit(
        "text_echo",
        kwargs={"value": "stage"},
        adapters={"inputs": {"value": TEXT_FILE_UTF8}, "outputs": {"default": TEXT_FILE_UTF8}},
    )
    state = run.wait(progress=False)

    assert state["status"] == "failed"
    assert state["manifest"]["runtime_port_adapters"]["failure"] == {
        "stage": expected_stage,
        "reason": "adapted run failed safely",
    }
    assert str(state["error"]).startswith(f"{expected_stage}:")
    assert "PRIVATE-" not in repr(state)


def test_exact_cat_encoding_matrix_and_png_to_text_use_generic_lifecycle(
    local_client: Any,
    tmp_path: Path,
) -> None:
    pd = pytest.importorskip("pandas")
    image_module = pytest.importorskip("PIL.Image")
    client, daemon, runtime, store = local_client
    frame_yaml = """\
- !DFunction
  name: cat_encoding
  inputs:
  - name: df
    type: pd.DataFrame
  - name: dict_categories
    type: dict
  - name: unique_count
    type: int
    default: '10'
  outputs:
  - name: default
    type: pd.DataFrame
  body: |-
    categorical_columns = list(df.loc[:, df.dtypes == object].columns)
    le_col = [
        col for col in categorical_columns
        if len(df.loc[:, col].unique()) >= unique_count
    ]
    oh_col = [
        col for col in categorical_columns
        if len(df.loc[:, col].unique()) < unique_count
    ]
    for item in categorical_columns:
        if item.lower() in list(df[item].unique()):
            df[item] = df[item].map(dict_categories[item.lower()])
    df = pd.get_dummies(df, columns=oh_col, dummy_na=True, sparse=False)
    return df
- !DImport
  module: pandas
  alias: pd
"""
    image_yaml = """\
- !DFunction
  name: describe_image
  inputs:
  - name: image
    type: Image.Image
  outputs:
  - name: default
    type: str
  body: |-
    return f"Image size: {image.size[0]} x {image.size[1]}; mode: {image.mode}"
- !DImport
  module: PIL.Image
  alias: Image
"""
    frame_record = runtime.register_object("cat_encoding", "cat_encoding", "default", yaml_text=frame_yaml)
    image_record = runtime.register_object("describe_image", "describe_image", "default", yaml_text=image_yaml)
    _ready(runtime, frame_record)
    _ready(runtime, image_record)

    immutable_before = store.get_object("cat_encoding", include_yaml=True)
    frame = pd.DataFrame({"measurement": [1, 2, 3], "species": ["setosa", "versicolor", "setosa"]})
    # pandas 3 infers a dedicated string dtype; this Object deliberately selects
    # object-typed categories, so exercise the same transformation on both majors.
    frame["species"] = frame["species"].astype(object)
    expected = pd.DataFrame(
        {
            "measurement": [1, 2, 3],
            "species_setosa": [True, False, True],
            "species_versicolor": [False, True, False],
            "species_nan": [False, False, False],
        }
    )
    cases = [
        ("implicit_json_split", None, DATAFRAME_JSON_SPLIT, DATAFRAME_JSON_SPLIT),
        (
            "csv_to_csv",
            {"inputs": {"df": DATAFRAME_CSV_SEMICOLON}, "outputs": {"default": DATAFRAME_CSV_SEMICOLON}},
            DATAFRAME_CSV_SEMICOLON,
            DATAFRAME_CSV_SEMICOLON,
        ),
        (
            "csv_to_xlsx",
            {"inputs": {"df": DATAFRAME_CSV_SEMICOLON}, "outputs": {"default": DATAFRAME_XLSX}},
            DATAFRAME_CSV_SEMICOLON,
            DATAFRAME_XLSX,
        ),
        (
            "xlsx_to_json_split",
            {"inputs": {"df": DATAFRAME_XLSX}, "outputs": {"default": DATAFRAME_JSON_SPLIT}},
            DATAFRAME_XLSX,
            DATAFRAME_JSON_SPLIT,
        ),
    ]
    completed: list[Any] = []
    for _, adapters, expected_input_id, expected_output_id in cases:
        result = client.call(
            "cat_encoding",
            kwargs={"df": frame, "dict_categories": {}, "unique_count": 10},
            adapters=adapters,
            progress=False,
        )
        pd.testing.assert_frame_equal(result.output, expected)
        bindings = result.run["manifest"]["runtime_port_adapters"]["bindings"]
        df_input = next(item for item in bindings if item["direction"] == "input" and item["port"] == "df")
        default_output = next(item for item in bindings if item["direction"] == "output")
        assert df_input["adapter_id"] == expected_input_id
        assert default_output["adapter_id"] == expected_output_id
        completed.append(result)

    assert {item.run["object_version_id"] for item in completed} == {frame_record["version_id"]}
    assert (
        completed[1].run["manifest"]["runtime_port_adapters"]["fingerprint_sha256"]
        != completed[2].run["manifest"]["runtime_port_adapters"]["fingerprint_sha256"]
    )
    assert store.get_object("cat_encoding", include_yaml=True) == immutable_before

    submitted = client.submit(
        "cat_encoding",
        kwargs={"df": frame, "dict_categories": {}, "unique_count": 10},
        adapters={"inputs": {"df": DATAFRAME_CSV_SEMICOLON}, "outputs": {"default": DATAFRAME_XLSX}},
        keep=True,
    )
    submitted_result = submitted.collect(progress=False)
    pd.testing.assert_frame_equal(submitted_result.output, expected)
    assert submitted_result.run["object_version_id"] == frame_record["version_id"]

    restarted_client = SPLClient(daemon_port=8765)
    restarted_client._daemon = daemon  # type: ignore[assignment]
    recovered = RemoteRun(restarted_client, submitted_result.run).collect(
        artifacts_dir=tmp_path / "cat-restarted",
        progress=False,
    )
    pd.testing.assert_frame_equal(recovered.output, expected)
    assert recovered.run["object_version_id"] == frame_record["version_id"]
    assert store.get_object("cat_encoding", include_yaml=True) == immutable_before

    custom_dataframe = Adapter(
        key="pandas.core.frame.DataFrame@spl.test.pipe-separated-no-index.v1",
        save=save_custom_dataframe_pipe,
        load=load_custom_dataframe_pipe,
        py_type=None,
        format="spl.test.pipe-separated-no-index.v1",
        distributions=(DDistribution(package="pandas", version=importlib_metadata.version("pandas")),),
    )
    custom_frame_result = client.call(
        "cat_encoding",
        kwargs={"df": frame, "dict_categories": {}, "unique_count": 10},
        adapters={"inputs": {"df": custom_dataframe}, "outputs": {"default": custom_dataframe}},
        progress=False,
    )
    pd.testing.assert_frame_equal(custom_frame_result.output, expected)
    custom_frame_ids = [
        binding["adapter_id"]
        for binding in custom_frame_result.run["manifest"]["runtime_port_adapters"]["bindings"]
        if binding["port"] in {"df", "default"}
    ]
    assert len(custom_frame_ids) == 2
    assert len(set(custom_frame_ids)) == 1
    assert custom_frame_ids[0].startswith("custom:")

    image = image_module.new("RGB", (4, 3), (1, 2, 3))
    in_memory = client.call(
        "describe_image",
        kwargs={"image": image},
        adapters={"inputs": {"image": PNG_PILLOW}, "outputs": {"default": TEXT_FILE_UTF8}},
        artifacts_dir=tmp_path / "image-memory",
        progress=False,
    )
    assert in_memory.output == "Image size: 4 x 3; mode: RGB"
    assert next(iter(in_memory.downloaded_artifacts.values())).read_bytes() == in_memory.output.encode()

    custom_pillow = Adapter(
        key="PIL.Image.Image@spl.test.png-pillow-custom.v1",
        save=save_custom_pillow_png,
        load=load_custom_pillow_png,
        py_type=None,
        format="spl.test.png-pillow-custom.v1",
        distributions=(DDistribution(package="Pillow", version=importlib_metadata.version("Pillow")),),
    )
    custom_image = client.call(
        "describe_image",
        kwargs={"image": image},
        adapters={"inputs": {"image": custom_pillow}, "outputs": {"default": TEXT_FILE_UTF8}},
        progress=False,
    )
    assert custom_image.output == in_memory.output
    custom_image_binding = next(
        binding
        for binding in custom_image.run["manifest"]["runtime_port_adapters"]["bindings"]
        if binding["direction"] == "input"
    )
    assert custom_image_binding["adapter_id"].startswith("custom:")
    assert custom_image_binding["distributions"] == [
        {"package": "Pillow", "version": importlib_metadata.version("Pillow")}
    ]

    image_path = tmp_path / "source.png"
    image.save(image_path, format="PNG")
    from_file = client.call(
        "describe_image",
        kwargs={"image": FileInput(image_path, media_type="image/png")},
        adapters={"inputs": {"image": PNG_PILLOW}, "outputs": {"default": TEXT_FILE_UTF8}},
        progress=False,
    )
    assert from_file.output == in_memory.output
    assert store.get_object("describe_image")["version_id"] == image_record["version_id"]


def test_recovered_run_uses_builtin_id_but_never_auto_executes_custom_decoder(
    local_client: Any,
    tmp_path: Path,
) -> None:
    client, _, _, _ = local_client
    builtin = client.submit(
        "text_echo",
        kwargs={"value": "restart"},
        adapters={"inputs": {"value": TEXT_FILE_UTF8}, "outputs": {"default": TEXT_FILE_UTF8}},
    )
    builtin_state = builtin.wait(progress=False)
    recovered_builtin = RemoteRun(client, builtin_state).collect(progress=False)
    assert recovered_builtin.output == "RESTART"

    adapter = Adapter(
        key="builtins.str@reverse-text.v1",
        save=save_reverse_text,
        load=load_reverse_text,
        py_type=str,
        format="reverse-text.v1",
    )
    custom = client.submit(
        "text_echo",
        kwargs={"value": "restart"},
        adapters={"inputs": {"value": adapter}, "outputs": {"default": adapter}},
    )
    custom_state = custom.wait(progress=False)
    recovered_custom = RemoteRun(client, custom_state).collect(
        artifacts_dir=tmp_path / "recovered-custom",
        progress=False,
    )
    assert isinstance(recovered_custom.output, ArtifactHandle)
    assert recovered_custom.output.adapter_id.startswith("custom:")
    assert recovered_custom.output.path is not None
    assert recovered_custom.output.path.read_bytes() == b"TRATSER"
    assert "explicitly trusted" in str(recovered_custom.output.recovery)
