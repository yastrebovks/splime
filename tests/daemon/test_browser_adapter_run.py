"""End-to-end contracts for the additive browser adapter Run bridge."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any

import pytest

from spl.adapters import BINARY_FILE, JSON, PNG_PILLOW, TEXT_FILE_UTF8
from spl.core import json_contract as m_json_contract
from spl.core.library_adapters import environment_fingerprint, normalize_publish_request
from spl.core.runtime_port_adapters import built_in_descriptor
from spl.daemon.browser_adapter_run import (
    MAX_ARTIFACT_DOWNLOAD_BYTES,
    MAX_PNG_PREVIEW_BYTES,
    AdapterRunError,
    BrowserAdapterRunBroker,
    _ArtifactGrant,
    semantic_signature_fields,
    semantic_signature_hash,
)
from spl.daemon import worker as worker_module
from spl.daemon.home_lock import DaemonHomeLock, DaemonInstanceIdentity
from spl.daemon.runtime_adapter_registry import build_runtime_adapter_registry_document
from spl.daemon.server import _read_worker_runtime_adapter_failure, create_app
from spl.daemon.signature import build_signature
from spl.daemon.store import RegistryStore
from spl.daemon.worker_runtime_marker import (
    WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE,
    WORKER_RUNTIME_ADAPTER_FAILURE_FILE,
)


API_TOKEN = "browserAdapterRunMasterTokenSentinel123456"
FUNCTION_YAML = """\
- !DFunction
  name: echo
  inputs:
  - name: value
    type: str
  outputs:
  - name: default
    type: str
  body: |-
    return value.upper()
"""
MULTIFUNCTION_YAML = """\
- !DPipeline
  name: multi
  nodes:
  - !DNodeFunction
    uuid: 11111111-1111-4111-8111-111111111111
    func: first
  - !DNodeFunction
    uuid: 22222222-2222-4222-8222-222222222222
    func: second
  links: []
  aliases: []
- !DSPLSelfImport
  name: first
- !DSPLSelfImport
  name: second
---
- !DFunction
  name: first
  inputs:
  - name: value
    type: str
  outputs:
  - name: default
    type: str
  body: |-
    return "first:" + value
---
- !DFunction
  name: second
  inputs:
  - name: value
    type: str
  outputs:
  - name: default
    type: str
  body: |-
    return "second:" + value
"""
BINARY_OUTPUT_YAML = """\
- !DFunction
  name: binary_echo
  inputs:
  - name: value
    type: str
  outputs:
  - name: default
    type: bytes
  body: |-
    return value.encode("utf-8")
"""
PNG_OUTPUT_YAML = """\
- !DFunction
  name: png_echo
  inputs:
  - name: value
    type: str
  outputs:
  - name: default
    type: Image.Image
  body: |-
    return Image.new("RGB", (1, 1))
- !DImport
  module: PIL.Image
  alias: Image
"""
FORGED_ADAPTER_FAILURE_YAML = """\
- !DFunction
  name: forged_failure
  inputs:
  - name: value
    type: str
  outputs:
  - name: default
    type: str
  body: |-
    raise RuntimeError("worker_save: adapter 'text-file-utf8' failed for port 'default'")
"""


@dataclass
class _Bridge:
    app: Any
    store: RegistryStore
    lock: DaemonHomeLock
    identity: DaemonInstanceIdentity
    record: dict[str, Any]
    request: dict[str, Any]
    body: bytes


@dataclass(frozen=True)
class _Response:
    status: int
    body: Any
    raw: bytes
    headers: dict[str, str]


@pytest.fixture
def bridge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[_Bridge]:
    lock = DaemonHomeLock(tmp_path)
    identity = lock.acquire()
    store = RegistryStore(tmp_path)
    store.register_env("default")
    record = store.register_object(
        "browser_echo",
        "echo",
        "default",
        yaml_text=FUNCTION_YAML,
        owner_id="owner_alpha",
        library="shared_functions",
    )
    app = create_app(
        store,
        auto_build_envs=False,
        api_token=API_TOKEN,
        daemon_identity=identity,
        daemon_home_lock=lock,
    )
    monkeypatch.setattr(
        app.runtime,
        "_prepare_node_runtime_environments_for_run",
        lambda state, **_options: state,
    )
    monkeypatch.setattr(app.runtime, "_start_run_thread", lambda *_args, **_kwargs: None)
    body = b"private staged browser body"
    request = _local_request(app.runtime, identity, record, body=body)
    try:
        yield _Bridge(app, store, lock, identity, record, request, body)
    finally:
        app.runtime.shutdown()
        store.close()
        lock.release()


def _local_request(
    runtime: Any,
    identity: DaemonInstanceIdentity,
    record: dict[str, Any],
    *,
    body: bytes,
    request_id: str = "adapter-local-request-001",
    output_adapter_id: str = TEXT_FILE_UTF8,
) -> dict[str, Any]:
    selected_function = record["entrypoint"] if record.get("kind") == "function" else None
    signature = build_signature(record, function=selected_function)
    fields = semantic_signature_fields(signature, function=selected_function)
    _, input_descriptor = built_in_descriptor(TEXT_FILE_UTF8)
    _, output_descriptor = built_in_descriptor(output_adapter_id)
    registry = build_runtime_adapter_registry_document(
        identity,
        remote_custom_adapter_execution_enabled=runtime.allow_remote_custom_adapters,
    )
    content_hash = str(record["content_hash"])
    if not content_hash.startswith("sha256:"):
        content_hash = "sha256:" + content_hash
    return {
        "schema": "spl.daemon.adapter-run-preflight-request",
        "schema_version": 1,
        "request_id": request_id,
        "daemon_instance_id": identity.instance_id,
        "daemon_generation": identity.generation,
        "registry_revision": registry["registry_revision"],
        "object": {
            "owner_id": record["owner_id"],
            "library": record["library"],
            "object_name": record["name"],
            "object_id": record["id"],
            "expected_current_version_id": record["current_version_id"],
            "selected_version_id": record["version_id"],
            "selected_version": record["version"],
            "function": selected_function,
            "origin": "local",
        },
        "content_hash": content_hash,
        "signature_evidence": {
            "semantic_fields": fields,
            "semantic_hash": semantic_signature_hash(fields),
            "review_hash": "sha256:" + "1" * 64,
        },
        "target": {"kind": "local", "machine_id": None, "offline_policy": None},
        "inputs": [
            {
                "name": "value",
                "port": "value",
                "kind": "staged",
                "value": None,
                "adapter_id": TEXT_FILE_UTF8,
                "format_tag": input_descriptor["format_tag"],
                "logical_name": "browser-input.txt",
                "media_type": "text/plain",
                "size": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
            }
        ],
        "outputs": [
            {
                "port": "default",
                "adapter_id": output_adapter_id,
                "format_tag": output_descriptor["format_tag"],
            }
        ],
        "output_selector": None,
        "timeout_ms": 30_000,
        "retention": "keep",
        "adapter_policy": {"custom_remote": "deny"},
        "source": "browser",
    }


def _text_library_publication() -> dict[str, Any]:
    return normalize_publish_request(
        {
            "schema_version": 1,
            "name": "browser-text-library-adapter",
            "description": "Browser text Library Adapter",
            "semantic_type": "builtins.str",
            "semantic_category": "text",
            "save_source": """def save_text(path: str, value: str) -> None:
    from pathlib import Path as LocalPath
    LocalPath(path).write_text(value, encoding='utf-8')
""",
            "load_source": None,
            "dependencies": [],
            "format_tag": None,
            "media_type": None,
            "preferred_extension": None,
            "policy": {
                "local_custom_code": "allow",
                "remote_custom_code": "deny",
            },
            "publication_environment": {
                "fingerprint": environment_fingerprint([]),
                "distributions": [],
            },
        }
    )


def _text_library_load_publication() -> dict[str, Any]:
    publication = _text_library_publication()
    return normalize_publish_request(
        {
            key: value
            for key, value in publication.items()
            if key
            in {
                "schema_version",
                "name",
                "description",
                "semantic_type",
                "semantic_category",
                "save_source",
                "load_source",
                "dependencies",
                "format_tag",
                "media_type",
                "preferred_extension",
                "policy",
                "publication_environment",
            }
        }
        | {
            "name": "browser-text-library-loader",
            "save_source": None,
            "load_source": """def load_text(path: str) -> str:
    from pathlib import Path as LocalPath
    return LocalPath(path).read_text(encoding='utf-8')
""",
        }
    )


async def _request(
    app: Any,
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | None = None,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    authenticated: bool = True,
) -> _Response:
    request_headers = dict(headers or {})
    if authenticated:
        request_headers["Authorization"] = f"Bearer {API_TOKEN}"
    client = app.test_client()
    if payload is not None:
        response = await client.open(path, method=method, json=payload, headers=request_headers)
    else:
        response = await client.open(path, method=method, data=data, headers=request_headers)
    raw = await response.get_data()
    return _Response(
        response.status_code,
        await response.get_json(silent=True),
        raw,
        {str(key).casefold(): str(value) for key, value in response.headers.items()},
    )


def _preflight_and_create(bridge: _Bridge, request: dict[str, Any] | None = None) -> tuple[Any, Any]:
    request = copy.deepcopy(request or bridge.request)
    preflight = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))
    assert preflight.status == 200
    create = copy.deepcopy(request)
    create["schema"] = "spl.daemon.adapter-run-admission-create"
    create["preflight"] = {
        "id": preflight.body["preflight_id"],
        "digest_sha256": preflight.body["digest_sha256"],
        "expires_at": preflight.body["expires_at"],
    }
    admitted = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-admissions", payload=create))
    assert admitted.status == 201
    return preflight, admitted


def _upload(bridge: _Bridge, request: dict[str, Any] | None = None) -> _Response:
    request = request or bridge.request
    item = request["inputs"][0]
    return asyncio.run(
        _request(
            bridge.app,
            "PUT",
            f"/runs/adapter-admissions/{request['request_id']}/inputs/{item['name']}",
            data=bridge.body,
            headers={
                "Content-Type": item["media_type"],
                "Content-Length": str(len(bridge.body)),
                "X-Spl-Content-SHA256": item["sha256"],
            },
        )
    )


def test_semantic_mismatch_requires_bound_plugin_confirmation_and_persists_evidence(
    bridge: _Bridge,
) -> None:
    request = _local_request(
        bridge.app.runtime,
        bridge.identity,
        bridge.record,
        body=bridge.body,
        request_id="adapter-semantic-mismatch",
        output_adapter_id=BINARY_FILE,
    )
    advisory = {
        "direction": "output",
        "port": "default",
        "adapter_id": BINARY_FILE,
        "port_semantic_type": "str",
        "adapter_semantic_type": "builtins.bytes",
        "adapter_semantic_category": "binary",
        "state": "declared_type_mismatch",
        "acknowledged": False,
        "acknowledgement_source": None,
    }
    request["runtime_adapter_semantic_advisories"] = {
        "schema_version": 1,
        "bindings": [advisory],
    }
    preflight = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))
    assert preflight.status == 200
    assert preflight.body["runtime_adapter_semantic_advisories"]["bindings"] == [advisory]

    create = copy.deepcopy(request)
    create["schema"] = "spl.daemon.adapter-run-admission-create"
    create["preflight"] = {
        "id": preflight.body["preflight_id"],
        "digest_sha256": preflight.body["digest_sha256"],
        "expires_at": preflight.body["expires_at"],
    }
    rejected = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-admissions", payload=create))
    assert rejected.status == 400
    assert rejected.body["code"] == "adapter_binding_invalid"
    assert bridge.store.list_runs() == []

    create["runtime_adapter_semantic_advisories"]["bindings"][0].update(
        {"acknowledged": True, "acknowledgement_source": "plugin_confirmation"}
    )
    admitted = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-admissions", payload=create))
    assert admitted.status == 201
    confirmed = create["runtime_adapter_semantic_advisories"]
    assert admitted.body["runtime_adapter_semantic_advisories"] == confirmed
    bridge.request = request
    assert _upload(bridge, request).status == 200
    finalized = _finalize(bridge, request["request_id"])
    assert finalized.status == 202
    assert finalized.body["receipt"]["runtime_adapter_semantic_advisories"] == confirmed
    state = bridge.store.get_run(finalized.body["run_id"])
    assert state["manifest"]["runtime_adapter_semantic_advisories"] == confirmed
    bridge.app.runtime._execute_run_with_lifecycle(finalized.body["run_id"], True)
    failed = bridge.store.get_run(finalized.body["run_id"])
    assert failed["status"] == "failed"
    assert failed["returncode"] == WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE
    failure_record = Path(failed["run_dir"]) / WORKER_RUNTIME_ADAPTER_FAILURE_FILE
    assert json.loads(failure_record.read_text(encoding="utf-8")) == {
        "schema_version": 1,
        "kind": "operation",
        "stage": "worker_save",
        "direction": "output",
        "port": "default",
        "adapter_id": BINARY_FILE,
    }
    assert failed["manifest"]["runtime_port_adapters"]["failure"] == {
        "schema_version": 1,
        "code": "output_adapter_save_failed",
        "stage": "worker_save",
        "direction": "output",
        "port": "default",
        "adapter_id": BINARY_FILE,
        "adapter_ref": None,
        "message": "The selected output Adapter could not save this value.",
        "retryable": False,
        "fallback_used": False,
    }
    assert len(bridge.store.list_runs()) == 1
    result = asyncio.run(
        _request(
            bridge.app,
            "GET",
            f"/runs/{finalized.body['run_id']}/adapter-result",
        )
    )
    assert result.status == 200
    assert result.body["failure"]["code"] == "output_adapter_save_failed"
    assert result.body["failure"]["port"] == "default"
    assert result.body["failure"]["fallback_used"] is False


def _finalize(bridge: _Bridge, request_id: str | None = None) -> _Response:
    request_id = request_id or bridge.request["request_id"]
    return asyncio.run(
        _request(
            bridge.app,
            "POST",
            f"/runs/adapter-admissions/{request_id}/finalize",
            payload={
                "schema": "spl.daemon.adapter-run-finalize",
                "schema_version": 1,
                "request_id": request_id,
            },
        )
    )


def _inline_json_request(
    bridge: _Bridge,
    *,
    request_id: str,
    value: Any,
) -> dict[str, Any]:
    _, descriptor = built_in_descriptor(JSON)
    request = copy.deepcopy(bridge.request)
    request["request_id"] = request_id
    request["inputs"] = [
        {
            "name": "value",
            "port": "value",
            "kind": "inline_json",
            "value": value,
            "adapter_id": JSON,
            "format_tag": descriptor["format_tag"],
            "logical_name": None,
            "media_type": None,
            "size": None,
            "sha256": None,
        }
    ]
    return request


def test_local_library_output_ref_is_revalidated_and_staged_privately(
    bridge: _Bridge,
) -> None:
    published = bridge.store.publish_library_adapter(
        _text_library_publication(),
        owner_id="owner_alpha",
        library="shared_functions",
        publisher_id="owner_alpha",
    )
    request = copy.deepcopy(bridge.request)
    request["outputs"] = [
        {
            "port": "default",
            "adapter_id": f"library:{published['content_hash']}",
            "format_tag": None,
        }
    ]
    request["runtime_library_adapter_refs"] = {
        "schema_version": 1,
        "bindings": [
            {
                "direction": "output",
                "port": "default",
                **{
                    key: published[key]
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
                },
            }
        ],
    }

    preflight, _admitted = _preflight_and_create(bridge, request)
    assert preflight.body["bindings"][1]["adapter_id"] == request["outputs"][0]["adapter_id"]
    assert preflight.body["bindings"][1]["format_tag"] is None
    assert _upload(bridge, request).status == 200
    projection, created = bridge.app.runtime.browser_adapter_runs.finalize(request["request_id"])
    assert created is True

    state = bridge.store.get_run(projection["receipt"]["run_id"])
    manifest = state["manifest"]
    assert manifest["runtime_library_adapter_refs"] == request["runtime_library_adapter_refs"]
    [metadata] = manifest["runtime_library_adapter_metadata"]["bindings"]
    assert metadata["semantic_category"] == "text"
    assert metadata["format_tag"] is None
    assert metadata["media_type"] is None
    source_directory = Path(state["run_dir"]) / "runtime-library-adapters"
    source_files = list(source_directory.iterdir())
    assert len(source_files) == 1
    assert source_files[0].read_text(encoding="utf-8").startswith("def save_text")

    object_record = bridge.store.get_object_version(
        bridge.record["version_id"],
        include_yaml=True,
    )
    object_yaml = Path(state["run_dir"]) / "library-adapter-object.yaml"
    object_yaml.write_text(object_record["yaml"], encoding="utf-8")
    worker_result = worker_module.execute(
        object_yaml=object_yaml,
        entrypoint=object_record["entrypoint"],
        input_path=Path(state["run_dir"]) / "input.json",
        result_path=Path(state["run_dir"]) / "library-adapter-result.json",
        artifacts_dir=Path(state["artifacts_dir"]),
    )
    [output_record] = worker_result["runtime_port_adapter_outputs"]
    assert output_record["adapter_id"] == f"library:{published['content_hash']}"
    assert output_record["format_tag"] is None
    assert output_record["media_type"] is None
    assert Path(worker_result["artifacts"][output_record["name"]]).read_bytes() == (
        bridge.body.decode("utf-8").upper().encode("utf-8")
    )


def test_local_library_load_only_ref_executes_only_in_worker(bridge: _Bridge) -> None:
    published = bridge.store.publish_library_adapter(
        _text_library_load_publication(),
        owner_id="owner_alpha",
        library="shared_functions",
        publisher_id="owner_alpha",
    )
    request = copy.deepcopy(bridge.request)
    request["inputs"][0]["adapter_id"] = f"library:{published['content_hash']}"
    request["inputs"][0]["format_tag"] = None
    request["runtime_library_adapter_refs"] = {
        "schema_version": 1,
        "bindings": [
            {
                "direction": "input",
                "port": "value",
                **{
                    key: published[key]
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
                },
            }
        ],
    }

    _preflight_and_create(bridge, request)
    assert _upload(bridge, request).status == 200
    projection, created = bridge.app.runtime.browser_adapter_runs.finalize(request["request_id"])
    assert created is True
    state = bridge.store.get_run(projection["receipt"]["run_id"])
    object_record = bridge.store.get_object_version(
        bridge.record["version_id"],
        include_yaml=True,
    )
    object_yaml = Path(state["run_dir"]) / "library-loader-object.yaml"
    object_yaml.write_text(object_record["yaml"], encoding="utf-8")
    worker_result = worker_module.execute(
        object_yaml=object_yaml,
        entrypoint=object_record["entrypoint"],
        input_path=Path(state["run_dir"]) / "input.json",
        result_path=Path(state["run_dir"]) / "library-loader-result.json",
        artifacts_dir=Path(state["artifacts_dir"]),
    )
    [output_record] = worker_result["runtime_port_adapter_outputs"]
    assert output_record["adapter_id"] == TEXT_FILE_UTF8
    assert Path(worker_result["artifacts"][output_record["name"]]).read_bytes() == (
        bridge.body.decode("utf-8").upper().encode("utf-8")
    )


def test_authenticated_local_stream_finalize_result_download_and_restart_projection(
    bridge: _Bridge,
) -> None:
    unauthorized = asyncio.run(
        _request(
            bridge.app,
            "POST",
            "/runs/adapter-preflights",
            payload=bridge.request,
            authenticated=False,
        )
    )
    assert unauthorized.status == 401
    assert unauthorized.headers["cache-control"] == "no-store"

    preflight, admitted = _preflight_and_create(bridge)
    assert preflight.body["target"] == {
        "kind": "local",
        "machine_id": None,
        "offline_policy": None,
    }
    assert preflight.body["bindings"] == [
        {
            "direction": "input",
            "port": "value",
            "adapter_id": TEXT_FILE_UTF8,
            "format_tag": bridge.request["inputs"][0]["format_tag"],
            "state": "available",
            "reason": None,
        },
        {
            "direction": "output",
            "port": "default",
            "adapter_id": TEXT_FILE_UTF8,
            "format_tag": bridge.request["outputs"][0]["format_tag"],
            "state": "available",
            "reason": None,
        },
    ]
    assert admitted.body["state"] == "staging"
    assert admitted.headers["cache-control"] == "no-store"

    uploaded = _upload(bridge)
    assert uploaded.status == 200
    assert uploaded.body["state"] == "uploaded"
    first = _finalize(bridge)
    second = _finalize(bridge)
    assert first.status == 202
    assert second.status == 200
    assert first.body == second.body
    run_id = first.body["run_id"]
    assert first.body["receipt"]["target"] == bridge.request["target"]
    assert len(bridge.store.list_runs()) == 1
    state = bridge.store.get_run(run_id)
    assert state["manifest"]["browser_adapter_admission"] == {
        "schema_version": 1,
        "request_id": bridge.request["request_id"],
        "request_digest_sha256": admitted.body["request_digest_sha256"],
        "target": bridge.request["target"],
    }

    output = b"HELLO FROM RETAINED TEXT"
    artifact_name = "runtime-default.spl-text-utf8-v1.txt"
    artifact_path = Path(state["artifacts_dir"]) / artifact_name
    artifact_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    artifact_path.write_bytes(output)
    bridge.store.update_run(run_id, status="running")
    bridge.store.update_run(
        run_id,
        status="succeeded",
        result={
            "result": {"value": None},
            "runtime_port_adapter_outputs": [
                {
                    "port": "default",
                    "name": artifact_name,
                    "adapter_id": TEXT_FILE_UTF8,
                    "format_tag": bridge.request["outputs"][0]["format_tag"],
                    "media_type": "text/plain",
                    "size": len(output),
                    "sha256": hashlib.sha256(output).hexdigest(),
                    "result_path": ["result", "value"],
                }
            ],
        },
    )
    projected = asyncio.run(_request(bridge.app, "GET", f"/runs/{run_id}/adapter-result"))
    assert projected.status == 200
    assert projected.body["state"] == "succeeded"
    assert projected.body["target"] == bridge.request["target"]
    [item] = projected.body["outputs"]
    assert item["availability"] == "retained"
    assert item["reason"] is None
    assert item["preview"] == {
        "kind": "text",
        "available": True,
        "text": output.decode("utf-8"),
        "json": None,
        "truncated": False,
    }
    assert item["provenance"] == {
        "run_id": run_id,
        "port": "default",
        "adapter_id": TEXT_FILE_UTF8,
        "format_tag": bridge.request["outputs"][0]["format_tag"],
        "sha256": hashlib.sha256(output).hexdigest(),
    }
    downloaded = asyncio.run(
        _request(
            bridge.app,
            "GET",
            f"/runs/{run_id}/adapter-artifacts/{item['download_handle']}",
        )
    )
    assert downloaded.status == 200
    assert downloaded.raw == output
    assert downloaded.headers["cache-control"] == "no-store"
    assert downloaded.headers["content-length"] == str(len(output))
    artifact_path.unlink()
    expired_download = asyncio.run(
        _request(
            bridge.app,
            "GET",
            f"/runs/{run_id}/adapter-artifacts/{item['download_handle']}",
        )
    )
    assert expired_download.status == 410
    assert expired_download.body["code"] == "artifact_expired"
    expired_result = bridge.app.runtime.browser_adapter_runs.project_result(run_id)
    assert expired_result["outputs"][0]["availability"] == "expired"
    assert expired_result["outputs"][0]["reason"] == "artifact_expired"
    assert expired_result["outputs"][0]["download_handle"] is None

    durable = list(bridge.app.runtime.browser_adapter_runs.finalized_root.glob("*.json"))
    assert len(durable) == 1
    durable_raw = durable[0].read_text(encoding="utf-8")
    assert bridge.body.decode("utf-8") not in durable_raw
    assert str(bridge.store.home) not in durable_raw
    assert API_TOKEN not in durable_raw
    bridge.app.runtime.browser_adapter_runs.shutdown()
    restored = BrowserAdapterRunBroker(bridge.app.runtime)
    bridge.app.runtime.browser_adapter_runs = restored
    restarted_projection = restored.project_result(run_id)
    assert restarted_projection["state"] == "succeeded"
    assert restarted_projection["outputs"][0]["sha256"] == hashlib.sha256(output).hexdigest()


def test_retained_binary_artifact_downloads_through_guarded_browser_routes(
    bridge: _Bridge,
) -> None:
    record = bridge.store.register_object(
        "browser_binary_echo",
        "binary_echo",
        "default",
        yaml_text=BINARY_OUTPUT_YAML,
        owner_id="owner_alpha",
        library="shared_functions",
    )
    request = _local_request(
        bridge.app.runtime,
        bridge.identity,
        record,
        body=bridge.body,
        request_id="adapter-local-binary-artifact",
        output_adapter_id=BINARY_FILE,
    )
    _preflight_and_create(bridge, request)
    assert _upload(bridge, request).status == 200
    finalized = _finalize(bridge, request["request_id"])
    assert finalized.status == 202
    run_id = finalized.body["run_id"]
    state = bridge.store.get_run(run_id)
    output = b"\x00BINARY\xffOUTPUT"
    artifact_name = "runtime-default.spl-binary-raw-v1.bin"
    artifact_path = Path(state["artifacts_dir"]) / artifact_name
    artifact_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    artifact_path.write_bytes(output)
    bridge.store.update_run(run_id, status="running")
    bridge.store.update_run(
        run_id,
        status="succeeded",
        result={
            "result": {"value": None},
            "runtime_port_adapter_outputs": [
                {
                    "port": "default",
                    "name": artifact_name,
                    "adapter_id": BINARY_FILE,
                    "format_tag": request["outputs"][0]["format_tag"],
                    "media_type": "application/octet-stream",
                    "size": len(output),
                    "sha256": hashlib.sha256(output).hexdigest(),
                    "result_path": ["result", "value"],
                }
            ],
        },
    )

    result = asyncio.run(_request(bridge.app, "GET", f"/runs/{run_id}/adapter-result"))
    assert result.status == 200
    [projected] = result.body["outputs"]
    assert projected["availability"] == "retained"
    assert projected["preview"] == {
        "kind": "none",
        "available": False,
        "text": None,
        "json": None,
        "truncated": False,
    }
    assert projected["download_handle"] is not None
    downloaded = asyncio.run(
        _request(
            bridge.app,
            "GET",
            f"/runs/{run_id}/adapter-artifacts/{projected['download_handle']}",
        )
    )
    assert downloaded.status == 200
    assert downloaded.raw == output
    assert downloaded.headers["content-type"].startswith("application/octet-stream")
    assert str(artifact_path) not in repr(result.body)


def test_discarded_terminal_artifact_projects_expired_without_download_authority(
    bridge: _Bridge,
) -> None:
    request = copy.deepcopy(bridge.request)
    request["request_id"] = "adapter-local-discarded-artifact"
    request["retention"] = "discard"
    _preflight_and_create(bridge, request)
    assert _upload(bridge, request).status == 200
    finalized = _finalize(bridge, request["request_id"])
    assert finalized.status == 202
    run_id = finalized.body["run_id"]
    state = bridge.store.get_run(run_id)
    assert state["manifest"]["keep"] is False
    discarded = b"DISCARDED TERMINAL TEXT"
    bridge.store.update_run(run_id, status="running")
    bridge.store.update_run(
        run_id,
        status="succeeded",
        result={
            "result": {"value": None},
            "runtime_port_adapter_outputs": [
                {
                    "port": "default",
                    "name": "runtime-default.discarded.txt",
                    "adapter_id": TEXT_FILE_UTF8,
                    "format_tag": request["outputs"][0]["format_tag"],
                    "media_type": "text/plain",
                    "size": len(discarded),
                    "sha256": hashlib.sha256(discarded).hexdigest(),
                    "result_path": ["result", "value"],
                }
            ],
        },
    )

    result = asyncio.run(_request(bridge.app, "GET", f"/runs/{run_id}/adapter-result"))
    assert result.status == 200
    [projected] = result.body["outputs"]
    assert projected["availability"] == "expired"
    assert projected["reason"] == "artifact_expired"
    assert projected["download_handle"] is None
    assert projected["preview"]["available"] is False
    assert discarded not in result.raw


def test_upload_mismatch_interruption_cancellation_and_expiry_never_create_a_run(
    bridge: _Bridge,
) -> None:
    _, admitted = _preflight_and_create(bridge)
    request_id = bridge.request["request_id"]
    wrong = asyncio.run(
        _request(
            bridge.app,
            "PUT",
            f"/runs/adapter-admissions/{request_id}/inputs/value",
            data=bridge.body,
            headers={
                "Content-Type": "text/plain",
                "Content-Length": str(len(bridge.body)),
                "X-Spl-Content-SHA256": "0" * 64,
            },
        )
    )
    assert wrong.status == 400
    assert wrong.body == {
        "schema": "spl.daemon.adapter-run-error",
        "schema_version": 1,
        "code": "upload_digest_mismatch",
        "stage": "upload",
        "retryable": False,
    }

    admission = bridge.app.runtime.browser_adapter_runs._admissions[request_id]

    async def interrupted() -> AsyncIterator[bytes]:
        yield bridge.body[:5]
        raise ConnectionError("simulated client disconnect")

    with pytest.raises(ConnectionError, match="disconnect"):
        asyncio.run(
            bridge.app.runtime.browser_adapter_runs.upload(
                request_id,
                "value",
                interrupted(),
                content_length=len(bridge.body),
                digest_header=hashlib.sha256(bridge.body).hexdigest(),
                media_type="text/plain",
            )
        )
    assert admission.uploading == set()
    assert list(admission.directory.iterdir()) == []
    cancelled = asyncio.run(_request(bridge.app, "DELETE", f"/runs/adapter-admissions/{request_id}"))
    assert cancelled.status == 200
    assert cancelled.body["state"] == "cancelled"
    assert not admission.directory.exists()
    assert _finalize(bridge).body["code"] == "admission_inactive"
    assert bridge.store.list_runs() == []

    expiring = copy.deepcopy(bridge.request)
    expiring["request_id"] = "adapter-local-request-expiry"
    _preflight_and_create(bridge, expiring)
    expiring_admission = bridge.app.runtime.browser_adapter_runs._admissions[expiring["request_id"]]
    expiring_admission.expires_at = "2000-01-01T00:00:00.000Z"
    reconciled = bridge.app.runtime.browser_adapter_runs.reconcile(expiring["request_id"])
    assert reconciled["state"] == "expired"
    assert not expiring_admission.directory.exists()
    assert bridge.store.list_runs() == []
    assert admitted.body["items"][0]["uploaded"] is False


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("registry", "registry_revision_mismatch"),
        ("generation", "daemon_identity_mismatch"),
        ("object_version", "object_changed"),
        ("port", "adapter_binding_invalid"),
        ("logical_name", "request_invalid"),
        ("request_slash", "request_invalid"),
        ("request_percent", "request_invalid"),
        ("request_traversal", "request_invalid"),
    ],
)
def test_exact_precondition_and_declaration_mismatch_matrix(
    bridge: _Bridge,
    case: str,
    expected_code: str,
) -> None:
    request = copy.deepcopy(bridge.request)
    if case == "registry":
        request["registry_revision"] = "0" * 64
    elif case == "generation":
        request["daemon_generation"] += 1
    elif case == "object_version":
        request["object"]["selected_version_id"] = "different-version-id"
    elif case == "port":
        request["inputs"][0]["name"] = "unknown_port"
        request["inputs"][0]["port"] = "unknown_port"
    elif case == "logical_name":
        request["inputs"][0]["logical_name"] = "../private-input.txt"
    elif case == "request_slash":
        request["request_id"] = "unsafe/request"
    elif case == "request_percent":
        request["request_id"] = "unsafe%request"
    else:
        request["request_id"] = "../unsafe-request"
    response = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))
    assert response.status in {400, 412}
    assert response.body["code"] == expected_code
    assert response.body["stage"] == "preflight"
    assert bridge.store.list_runs() == []


def test_upload_size_media_and_preflight_bound_logical_name_mismatches(
    bridge: _Bridge,
) -> None:
    _preflight_and_create(bridge)
    request_id = bridge.request["request_id"]
    item = bridge.request["inputs"][0]
    wrong_size = asyncio.run(
        _request(
            bridge.app,
            "PUT",
            f"/runs/adapter-admissions/{request_id}/inputs/value",
            data=bridge.body,
            headers={
                "Content-Type": "text/plain",
                "Content-Length": str(len(bridge.body) - 1),
                "X-Spl-Content-SHA256": item["sha256"],
            },
        )
    )
    assert wrong_size.body["code"] == "upload_size_mismatch"
    wrong_media = asyncio.run(
        _request(
            bridge.app,
            "PUT",
            f"/runs/adapter-admissions/{request_id}/inputs/value",
            data=bridge.body,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(bridge.body)),
                "X-Spl-Content-SHA256": item["sha256"],
            },
        )
    )
    assert wrong_media.body["code"] == "upload_media_mismatch"
    assert bridge.store.list_runs() == []

    bound = copy.deepcopy(bridge.request)
    bound["request_id"] = "adapter-local-logical-name-mismatch"
    logical_preflight = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=bound))
    assert logical_preflight.status == 200
    create = copy.deepcopy(bound)
    # The proof cannot authorize a changed logical filename even when all body
    # bytes and adapter identities are equal.
    create["inputs"][0]["logical_name"] = "different-safe-name.txt"
    create["schema"] = "spl.daemon.adapter-run-admission-create"
    create["preflight"] = {
        "id": logical_preflight.body["preflight_id"],
        "digest_sha256": logical_preflight.body["digest_sha256"],
        "expires_at": logical_preflight.body["expires_at"],
    }
    rebound = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-admissions", payload=create))
    assert rebound.status == 412
    assert rebound.body["code"] == "target_preflight_required"
    assert bridge.store.list_runs() == []


@pytest.mark.parametrize(
    "unsafe_value",
    [
        "/etc/shadow",
        "/root/.ssh/id_rsa",
        "/opt/private/key",
        "/usr/local/secret",
        "/users/example/private.txt",
        "read from /Users/example/private.txt",
        r"C:\Users\example\private.txt",
        r"\\server\share\private.txt",
        "file:///etc/shadow",
    ],
)
def test_browser_preflight_rejects_raw_host_paths_in_inline_json(
    bridge: _Bridge,
    unsafe_value: str,
) -> None:
    request = _inline_json_request(
        bridge,
        request_id="adapter-unsafe-path",
        value=unsafe_value,
    )
    response = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))
    assert response.status == 400
    assert response.body["code"] == "request_invalid"
    assert unsafe_value.encode() not in response.raw
    assert bridge.store.list_runs() == []


def test_browser_preflight_rejects_raw_host_path_inline_json_keys(
    bridge: _Bridge,
) -> None:
    request = _inline_json_request(
        bridge,
        request_id="adapter-unsafe-path-key",
        value={"/etc/shadow": "redacted-value"},
    )
    response = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))
    assert response.status == 400
    assert response.body["code"] == "request_invalid"
    assert b"/etc/shadow" not in response.raw
    assert b"redacted-value" not in response.raw
    assert bridge.store.list_runs() == []


@pytest.mark.parametrize(
    "unsafe_key",
    [
        "password",
        "token",
        "api_key",
        "providerKey",
        "privateKey",
        "private-key",
        "refreshToken",
        "accessToken",
        "clientSecret",
        "central_password",
        "credentialPayload",
    ],
)
def test_browser_preflight_rejects_credential_shaped_inline_json_keys(
    bridge: _Bridge,
    unsafe_key: str,
) -> None:
    request = _inline_json_request(
        bridge,
        request_id="adapter-unsafe-key",
        value={unsafe_key: "redacted-value"},
    )
    response = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))
    assert response.status == 400
    assert response.body["code"] == "request_invalid"
    assert b"redacted-value" not in response.raw
    assert bridge.store.list_runs() == []


@pytest.mark.parametrize(
    "unsafe_value",
    [
        "Bearer providerSecretValue123",
        "sk-providerSecret12345678",
        "spl-" + "a" * 43,
        "https://user:password@example.invalid/resource",
        "https://example.invalid/?access_token=private-value",
        "api_key=private-value",
        "provider-secret-privatevalue",
        "central-master-token-privatevalue",
    ],
)
def test_browser_preflight_rejects_credential_shaped_inline_json_values(
    bridge: _Bridge,
    unsafe_value: str,
) -> None:
    request = _inline_json_request(
        bridge,
        request_id="adapter-unsafe-credential-value",
        value=unsafe_value,
    )
    response = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))
    assert response.status == 400
    assert response.body["code"] == "request_invalid"
    assert unsafe_value.encode() not in response.raw
    assert bridge.app.runtime.browser_adapter_runs._preflights == {}
    assert bridge.app.runtime.browser_adapter_runs._admissions == {}
    assert bridge.store.list_runs() == []


def test_browser_admission_parser_rejects_credential_value_before_mutation(
    bridge: _Bridge,
) -> None:
    request = _inline_json_request(
        bridge,
        request_id="adapter-admission-unsafe-credential-value",
        value="safe reviewed value",
    )
    preflight = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))
    assert preflight.status == 200
    create = copy.deepcopy(request)
    create["schema"] = "spl.daemon.adapter-run-admission-create"
    create["preflight"] = {
        "id": preflight.body["preflight_id"],
        "digest_sha256": preflight.body["digest_sha256"],
        "expires_at": preflight.body["expires_at"],
    }
    create["inputs"][0]["value"] = "Bearer providerSecretValue123"
    response = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-admissions", payload=create))
    assert response.status == 400
    assert response.body["code"] == "request_invalid"
    assert b"providerSecretValue123" not in response.raw
    assert bridge.app.runtime.browser_adapter_runs._admissions == {}
    assert bridge.store.list_runs() == []


@pytest.mark.parametrize(
    "safe_value",
    [
        "team/alpha",
        "https://example.invalid/two/segments",
        "Bearer brief",
        "sk-short",
        "spl-short",
        "https://user@example.invalid/resource",
        "https://example.invalid/?token_count=2&password_policy=local",
        "provider-secret-short",
        {"token_count": 2, "password_policy": "local-only", "secretary": "Ada"},
    ],
)
def test_browser_preflight_path_and_key_filter_preserves_benign_json(
    bridge: _Bridge,
    safe_value: Any,
) -> None:
    request = _inline_json_request(
        bridge,
        request_id="adapter-benign-safety-value",
        value=safe_value,
    )
    response = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))
    assert response.status == 200
    assert response.body["can_finalize"] is True
    assert bridge.store.list_runs() == []


def test_two_runs_change_independent_bindings_without_new_object_version(
    bridge: _Bridge,
) -> None:
    _, json_descriptor = built_in_descriptor(JSON)
    versions_before = bridge.store.list_object_versions(
        bridge.record["name"],
        owner_id=bridge.record["owner_id"],
        library=bridge.record["library"],
    )

    text_input_json_output = copy.deepcopy(bridge.request)
    text_input_json_output["request_id"] = "adapter-bindings-run-1"
    text_input_json_output["outputs"] = [
        {
            "port": "default",
            "adapter_id": JSON,
            "format_tag": json_descriptor["format_tag"],
        }
    ]
    _preflight_and_create(bridge, text_input_json_output)
    assert _upload(bridge, text_input_json_output).status == 200
    first = _finalize(bridge, text_input_json_output["request_id"])
    assert first.status == 202

    json_input_text_output = copy.deepcopy(bridge.request)
    json_input_text_output["request_id"] = "adapter-bindings-run-2"
    json_input_text_output["inputs"] = [
        {
            "name": "value",
            "port": "value",
            "kind": "inline_json",
            "value": "second Run",
            "adapter_id": JSON,
            "format_tag": json_descriptor["format_tag"],
            "logical_name": None,
            "media_type": None,
            "size": None,
            "sha256": None,
        }
    ]
    _preflight_and_create(bridge, json_input_text_output)
    second = _finalize(bridge, json_input_text_output["request_id"])
    assert second.status == 202
    assert first.body["run_id"] != second.body["run_id"]

    runs = bridge.store.list_runs()
    assert len(runs) == 2
    fingerprints = {run["manifest"]["runtime_port_adapters"]["fingerprint_sha256"] for run in runs}
    assert len(fingerprints) == 2
    versions_after = bridge.store.list_object_versions(
        bridge.record["name"],
        owner_id=bridge.record["owner_id"],
        library=bridge.record["library"],
    )
    assert [record["version_id"] for record in versions_after] == [record["version_id"] for record in versions_before]


def test_exact_selected_function_flows_into_local_run_creation(bridge: _Bridge) -> None:
    record = bridge.store.register_object(
        "browser_multifunction",
        "multi",
        "default",
        yaml_text=MULTIFUNCTION_YAML,
        owner_id="owner_alpha",
        library="shared_functions",
    )
    request = _local_request(
        bridge.app.runtime,
        bridge.identity,
        record,
        body=bridge.body,
        request_id="adapter-multifunction-request",
    )
    request["object"]["function"] = "second"
    signature = build_signature(record, function="second")
    fields = semantic_signature_fields(signature, function="second")
    request["signature_evidence"] = {
        "semantic_fields": fields,
        "semantic_hash": semantic_signature_hash(fields),
        "review_hash": "sha256:" + "2" * 64,
    }
    _preflight_and_create(bridge, request)
    assert _upload(bridge, request).status == 200
    finalized = _finalize(bridge, request["request_id"])
    assert finalized.status == 202
    state = bridge.store.get_run(finalized.body["run_id"])
    assert state["entrypoint"] == "second"
    assert state["input"]["function"] == "second"
    assert finalized.body["receipt"]["object"]["function"] == "second"


def test_lost_finalize_response_reconciles_committed_marker_without_replay(
    bridge: _Bridge,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _preflight_and_create(bridge)
    assert _upload(bridge).status == 200
    original_start = bridge.app.runtime.start_run
    start_calls = 0

    def response_lost(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal start_calls
        start_calls += 1
        original_start(*args, **kwargs)
        raise OSError("simulated response loss after commit")

    original_list = bridge.store.list_runs
    list_calls = 0

    def first_reconcile_is_unavailable() -> list[dict[str, Any]]:
        nonlocal list_calls
        list_calls += 1
        if list_calls <= 3:
            raise OSError("simulated local read interruption")
        return original_list()

    monkeypatch.setattr(bridge.app.runtime, "start_run", response_lost)
    monkeypatch.setattr(bridge.store, "list_runs", first_reconcile_is_unavailable)
    ambiguous = _finalize(bridge)
    assert ambiguous.status == 503
    assert ambiguous.body["code"] == "outcome_unknown"
    assert ambiguous.body["retryable"] is True
    unsafe_cancel = asyncio.run(
        _request(
            bridge.app,
            "DELETE",
            f"/runs/adapter-admissions/{bridge.request['request_id']}",
        )
    )
    assert unsafe_cancel.status == 503
    assert unsafe_cancel.body["code"] == "outcome_unknown"
    reconciled = asyncio.run(
        _request(
            bridge.app,
            "GET",
            f"/runs/adapter-admissions/{bridge.request['request_id']}",
        )
    )
    assert reconciled.status == 200
    assert reconciled.body["state"] == "finalized"
    assert reconciled.body["run_id"] == original_list()[0]["id"]
    repeated = _finalize(bridge)
    assert repeated.status == 200
    assert repeated.body["run_id"] == reconciled.body["run_id"]
    assert start_calls == 1
    assert len(original_list()) == 1


def test_restart_after_local_commit_reconciles_marker_before_mutation_replay(
    bridge: _Bridge,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _preflight_and_create(bridge)
    assert _upload(bridge).status == 200
    original_start = bridge.app.runtime.start_run
    start_calls = 0

    def response_lost(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal start_calls
        start_calls += 1
        original_start(*args, **kwargs)
        raise OSError("simulated process loss after local commit")

    original_list = bridge.store.list_runs

    def unavailable_during_first_process() -> list[dict[str, Any]]:
        raise OSError("simulated process loss before receipt persistence")

    monkeypatch.setattr(bridge.app.runtime, "start_run", response_lost)
    monkeypatch.setattr(bridge.store, "list_runs", unavailable_during_first_process)
    ambiguous = _finalize(bridge)
    assert ambiguous.status == 503
    assert ambiguous.body["code"] == "outcome_unknown"
    assert start_calls == 1

    bridge.app.runtime.browser_adapter_runs.shutdown()
    monkeypatch.setattr(bridge.store, "list_runs", original_list)
    restored = BrowserAdapterRunBroker(bridge.app.runtime)
    bridge.app.runtime.browser_adapter_runs = restored

    _preflight_and_create(bridge)
    assert _upload(bridge).status == 200
    reconciled = _finalize(bridge)
    assert reconciled.status == 200
    assert reconciled.body["state"] == "finalized"
    assert reconciled.body["run_id"] == original_list()[0]["id"]
    assert start_calls == 1
    assert len(original_list()) == 1


def test_png_preview_is_not_advertised_above_companion_limit(bridge: _Bridge) -> None:
    broker = bridge.app.runtime.browser_adapter_runs
    small = _ArtifactGrant(
        "run-1",
        "local",
        "small.png",
        "small.png",
        "image/png",
        MAX_PNG_PREVIEW_BYTES,
        "a" * 64,
    )
    large = _ArtifactGrant(
        "run-1",
        "local",
        "large.png",
        "large.png",
        "image/png",
        MAX_PNG_PREVIEW_BYTES + 1,
        "b" * 64,
    )
    assert broker._artifact_preview(small) == {
        "kind": "png",
        "available": True,
        "text": None,
        "json": None,
        "truncated": False,
    }
    assert broker._artifact_preview(large) == {
        "kind": "png",
        "available": False,
        "text": None,
        "json": None,
        "truncated": True,
    }


def test_failed_run_projects_every_selected_output_as_unavailable(bridge: _Bridge) -> None:
    _preflight_and_create(bridge)
    assert _upload(bridge).status == 200
    finalized = _finalize(bridge)
    run_id = finalized.body["run_id"]
    bridge.store.update_run(run_id, status="running")
    bridge.store.update_run(run_id, status="failed", error="PRIVATE TRACEBACK VALUE")
    result = bridge.app.runtime.browser_adapter_runs.project_result(run_id)
    assert result["state"] == "failed"
    assert result["outputs"] == [
        {
            "port": "default",
            "direction": "output",
            "adapter_id": TEXT_FILE_UTF8,
            "format_tag": bridge.request["outputs"][0]["format_tag"],
            "logical_name": "default.txt",
            "media_type": "text/plain",
            "size": None,
            "sha256": None,
            "availability": "unavailable",
            "reason": "result_unavailable",
            "preview": {
                "kind": "text",
                "available": False,
                "text": None,
                "json": None,
                "truncated": False,
            },
            "download_handle": None,
            "provenance": {
                "run_id": run_id,
                "port": "default",
                "adapter_id": TEXT_FILE_UTF8,
                "format_tag": bridge.request["outputs"][0]["format_tag"],
                "sha256": None,
            },
        }
    ]
    assert result["failure"] == {
        "stage": "object_execution",
        "reason_code": "execution_failed",
        "message": "Execution failed.",
    }
    assert "PRIVATE" not in repr(result)


def test_real_worker_load_failure_projects_bounded_useful_sanitized_error(
    bridge: _Bridge,
) -> None:
    private_body = b"PRIVATE-WORKER-INPUT-SENTINEL-\xff"
    request = _local_request(
        bridge.app.runtime,
        bridge.identity,
        bridge.record,
        body=private_body,
        request_id="adapter-local-real-worker-failure",
    )
    bridge.body = private_body
    bridge.request = request
    _preflight_and_create(bridge, request)
    assert _upload(bridge, request).status == 200
    finalized = _finalize(bridge, request["request_id"])
    assert finalized.status == 202
    run_id = finalized.body["run_id"]

    # The fixture suppresses background threads for deterministic admission
    # tests. Execute the accepted Run synchronously through the real worker
    # lifecycle so invalid UTF-8 reaches the built-in text adapter loader.
    bridge.app.runtime._execute_run_with_lifecycle(run_id, True)
    state = bridge.store.get_run(run_id)
    assert state["status"] == "failed"
    assert state["returncode"] == WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE
    failure_record = Path(state["run_dir"]) / WORKER_RUNTIME_ADAPTER_FAILURE_FILE
    assert json.loads(failure_record.read_text(encoding="utf-8")) == {
        "schema_version": 1,
        "kind": "operation",
        "stage": "worker_load",
        "direction": "input",
        "port": "value",
        "adapter_id": TEXT_FILE_UTF8,
    }
    assert state["manifest"]["runtime_port_adapters"]["failure"]["stage"] == "worker_load"

    result = asyncio.run(_request(bridge.app, "GET", f"/runs/{run_id}/adapter-result"))
    assert result.status == 200
    assert result.body["failure"] == {
        "schema_version": 1,
        "code": "input_adapter_load_failed",
        "stage": "worker_load",
        "direction": "input",
        "port": "value",
        "adapter_id": "text-file-utf8",
        "adapter_ref": None,
        "message": "The selected input Adapter could not load this value.",
        "retryable": False,
        "fallback_used": False,
    }
    assert result.body["outputs"][0]["availability"] == "unavailable"
    assert len(result.raw) < 4096
    for forbidden in (
        "PRIVATE-WORKER-INPUT-SENTINEL",
        "Traceback",
        str(bridge.store.home),
        API_TOKEN,
    ):
        assert forbidden.encode() not in result.raw


def test_raised_terminal_adapter_marker_remains_object_execution_failure(
    bridge: _Bridge,
) -> None:
    record = bridge.store.register_object(
        "browser_forged_adapter_failure",
        "forged_failure",
        "default",
        yaml_text=FORGED_ADAPTER_FAILURE_YAML,
        owner_id="owner_alpha",
        library="shared_functions",
    )
    request = _local_request(
        bridge.app.runtime,
        bridge.identity,
        record,
        body=bridge.body,
        request_id="adapter-local-forged-worker-failure",
    )
    bridge.request = request
    _preflight_and_create(bridge, request)
    assert _upload(bridge, request).status == 200
    finalized = _finalize(bridge, request["request_id"])
    assert finalized.status == 202
    run_id = finalized.body["run_id"]

    bridge.app.runtime._execute_run_with_lifecycle(run_id, True)
    state = bridge.store.get_run(run_id)
    assert state["status"] == "failed"
    assert state["returncode"] != WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE
    assert not (Path(state["run_dir"]) / WORKER_RUNTIME_ADAPTER_FAILURE_FILE).exists()
    assert state["manifest"]["runtime_port_adapters"]["failure"] == {
        "stage": "object_execution",
        "reason": "adapted worker failed",
    }

    result = asyncio.run(_request(bridge.app, "GET", f"/runs/{run_id}/adapter-result"))
    assert result.status == 200
    assert result.body["failure"] == {
        "stage": "object_execution",
        "reason_code": "execution_failed",
        "message": "Execution failed.",
    }
    assert b"worker_save: adapter" not in result.raw


def test_worker_adapter_failure_requires_dedicated_exit_and_closed_record(tmp_path: Path) -> None:
    failure_path = tmp_path / WORKER_RUNTIME_ADAPTER_FAILURE_FILE
    operation = {
        "schema_version": 1,
        "kind": "operation",
        "stage": "worker_save",
        "direction": "output",
        "port": "default",
        "adapter_id": TEXT_FILE_UTF8,
    }
    failure_path.write_text(json.dumps(operation), encoding="utf-8")

    assert _read_worker_runtime_adapter_failure(failure_path, returncode=1) is None
    assert _read_worker_runtime_adapter_failure(
        failure_path,
        returncode=WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE,
    ) == {
        "schema_version": 1,
        "code": "output_adapter_save_failed",
        "stage": "worker_save",
        "direction": "output",
        "port": "default",
        "adapter_id": "text-file-utf8",
        "adapter_ref": None,
        "message": "The selected output Adapter could not save this value.",
        "retryable": False,
        "fallback_used": False,
    }

    failure_path.write_text(json.dumps({**operation, "untrusted": True}), encoding="utf-8")
    assert (
        _read_worker_runtime_adapter_failure(
            failure_path,
            returncode=WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE,
        )
        is None
    )

    failure_path.write_text(
        json.dumps({"schema_version": 1, "kind": "stage", "stage": "worker_save"}),
        encoding="utf-8",
    )
    assert _read_worker_runtime_adapter_failure(
        failure_path,
        returncode=WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE,
    ) == {
        "stage": "worker_save",
        "reason": "runtime adapter worker stage failed",
    }


def test_oversized_or_malformed_output_never_receives_a_download_handle(
    bridge: _Bridge,
) -> None:
    _preflight_and_create(bridge)
    assert _upload(bridge).status == 200
    finalized = _finalize(bridge)
    run_id = finalized.body["run_id"]
    bridge.store.update_run(run_id, status="running")
    bridge.store.update_run(
        run_id,
        status="succeeded",
        result={
            "result": {"value": None},
            "runtime_port_adapter_outputs": [
                {
                    "port": "default",
                    "name": "oversized.txt",
                    "adapter_id": TEXT_FILE_UTF8,
                    "format_tag": bridge.request["outputs"][0]["format_tag"],
                    "media_type": "text/plain",
                    "size": MAX_ARTIFACT_DOWNLOAD_BYTES + 1,
                    "sha256": "a" * 64,
                    "result_path": ["result", "value"],
                }
            ],
        },
    )
    [output] = bridge.app.runtime.browser_adapter_runs.project_result(run_id)["outputs"]
    assert output["availability"] == "unavailable"
    assert output["reason"] == "result_unavailable"
    assert output["size"] is None
    assert output["sha256"] is None
    assert output["provenance"]["sha256"] is None
    assert output["download_handle"] is None


def _remote_request(bridge: _Bridge, request_id: str) -> dict[str, Any]:
    request = copy.deepcopy(bridge.request)
    request["request_id"] = request_id
    request["object"]["origin"] = "server"
    request["target"] = {
        "kind": "remote",
        "machine_id": "machine-remote-1",
        "offline_policy": "queue",
    }
    return request


def _remote_admission_digest(payload: dict[str, Any]) -> str:
    canonical = {
        "schema_version": 1,
        "run": {key: value for key, value in payload["run"].items() if key != "access_token"},
        "runtime_port_adapters": payload["runtime_port_adapters"],
        "adapter_policy": payload["adapter_policy"],
    }
    return hashlib.sha256(m_json_contract.dumps(canonical).encode("utf-8")).hexdigest()


class _RemoteServer:
    def __init__(self, record: dict[str, Any]) -> None:
        self.record = copy.deepcopy(record)
        self.admission: dict[str, Any] | None = None
        self.uploads: list[tuple[str, bytes]] = []
        self.run = {
            "id": "remote-adapter-run-1",
            "status": "queued",
            "created_at": datetime.now(UTC).isoformat(),
        }
        self.artifacts: dict[str, bytes] = {}

    def get_object(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return copy.deepcopy(self.record)

    def get_server_version(self) -> dict[str, Any]:
        return {"declared": {"contracts": {"daemon_server_capabilities": ["spl.remote_run.runtime_port_adapters.v1"]}}}

    def list_machines(self) -> list[dict[str, Any]]:
        return [
            {
                "id": "machine-remote-1",
                "capabilities": {
                    "spl.remote_run.runtime_port_adapters.v1": {
                        "schema_version": 1,
                        "transport": True,
                        "custom_adapter_execution": {"implemented": True, "enabled": False},
                    }
                },
            }
        ]

    def preflight_remote_run(self, request: dict[str, Any]) -> dict[str, Any]:
        obj = self.record
        return {
            "schema_version": 1,
            "contract": "execution_preflight",
            "checked_at": "2026-08-10T12:00:00+00:00",
            "can_queue": True,
            "point_in_time": True,
            "reservation": False,
            "resolved_request": {
                "version_id": request["version_id"],
                "target_machine_id": request["target_machine_id"],
                "offline_policy": request["offline_policy"],
            },
            "object": {
                "id": obj["id"],
                "name": obj["name"],
                "owner_id": obj["owner_id"],
                "library_slug": obj["library"],
                "version": obj["version"],
                "version_id": obj["version_id"],
                "entrypoint": obj["entrypoint"],
            },
            "authorization": {},
            "execution": {},
            "environment": {},
            "runtimes": {},
            "trust_boundary": {},
            "telemetry": {},
            "compatibility": {},
            "capacity": {},
            "checks": [
                {
                    "code": "object_identity",
                    "outcome": "pass",
                    "reason_code": "exact_object_version_resolved",
                    "explanation": "Exact immutable Object version resolved.",
                    "evidence_at": "2026-08-10T12:00:00+00:00",
                }
            ],
        }

    def create_remote_run_admission(self, payload: dict[str, Any]) -> dict[str, Any]:
        items = [
            {
                "role": "input",
                "name": item["name"],
                "size": item["size"],
                "sha256": item["sha256"],
                "uploaded": False,
            }
            for item in payload["runtime_port_adapters"]["inputs"]
        ]
        self.admission = {
            "schema_version": 1,
            "request_id": payload["request_id"],
            "request_digest_sha256": _remote_admission_digest(payload),
            "runtime_port_adapters": payload["runtime_port_adapters"],
            "adapter_policy": payload["adapter_policy"],
            "state": "staging",
            "items": items,
            "run_id": None,
        }
        return copy.deepcopy(self.admission)

    def get_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        assert self.admission is not None and self.admission["request_id"] == request_id
        return copy.deepcopy(self.admission)

    def upload_remote_run_admission_input(
        self,
        request_id: str,
        name: str,
        body: bytes,
        *,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        assert self.admission is not None and self.admission["request_id"] == request_id
        assert len(body) == size and hashlib.sha256(body).hexdigest() == sha256
        self.uploads.append((name, body))
        item = next(item for item in self.admission["items"] if item["name"] == name)
        item["uploaded"] = True
        return copy.deepcopy(item)

    def finalize_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        assert self.admission is not None and self.admission["request_id"] == request_id
        assert all(item["uploaded"] for item in self.admission["items"])
        self.admission["state"] = "finalized"
        self.admission["run_id"] = self.run["id"]
        return copy.deepcopy(self.run)

    def cancel_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        assert self.admission is not None and self.admission["request_id"] == request_id
        self.admission["state"] = "cancelled"
        return copy.deepcopy(self.admission)

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        assert run_id == self.run["id"]
        return copy.deepcopy(self.run)

    def list_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        assert run_id == self.run["id"]
        return [{"name": name} for name in sorted(self.artifacts)]

    def artifact_bytes_verified(
        self,
        run_id: str,
        name: str,
        *,
        size: int,
        sha256: str,
    ) -> bytes:
        assert run_id == self.run["id"]
        body = self.artifacts[name]
        assert len(body) == size and hashlib.sha256(body).hexdigest() == sha256
        return body

    def artifact_bytes(self, run_id: str, name: str) -> bytes:
        assert run_id == self.run["id"]
        return self.artifacts[name]


def test_fake_remote_uses_existing_atomic_server_admission_lifecycle(
    bridge: _Bridge,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _remote_request(bridge, "adapter-remote-request-001")
    server = _RemoteServer(bridge.record)
    credentials = {"id": "connection-1", "machine_id": "machine-local"}
    monkeypatch.setattr(
        bridge.app.runtime,
        "_require_live_server_channel_credentials",
        lambda *_args, **_kwargs: credentials,
    )
    monkeypatch.setattr(
        bridge.app.runtime,
        "_server_client_for_credentials",
        lambda *_args, **_kwargs: server,
    )
    monkeypatch.setattr(bridge.app.runtime, "_kick_server_sync", lambda *_args, **_kwargs: None)

    preflight, admitted = _preflight_and_create(bridge, request)
    assert preflight.body["target"] == request["target"]
    assert admitted.body["target"] == request["target"]
    uploaded = _upload(bridge, request)
    assert uploaded.status == 200
    finalized = _finalize(bridge, request["request_id"])
    assert finalized.status == 202
    assert finalized.body["receipt"]["target"] == request["target"]
    assert finalized.body["run_id"] == server.run["id"]
    assert server.admission is not None
    assert server.admission["state"] == "finalized"
    assert len(server.uploads) == 1
    assert server.uploads[0] == ("value", bridge.body)
    assert bridge.store.list_runs() == []

    output = b"REMOTE TEXT OUTPUT"
    name = "runtime-default.remote.txt"
    digest = hashlib.sha256(output).hexdigest()
    server.artifacts[name] = output
    server.run = {
        **server.run,
        "status": "succeeded",
        "result": {"result": {"value": None}},
        "runtime_port_adapters": {
            "terminal": {
                "outputs": [
                    {
                        "port": "default",
                        "adapter_id": TEXT_FILE_UTF8,
                        "format_tag": request["outputs"][0]["format_tag"],
                        "artifact_name": name,
                        "size": len(output),
                        "sha256": digest,
                        "media_type": "text/plain",
                    }
                ]
            }
        },
    }
    result = bridge.app.runtime.browser_adapter_runs.project_result(server.run["id"])
    assert result["target"] == request["target"]
    assert result["outputs"][0]["preview"]["text"] == output.decode("utf-8")
    handle = result["outputs"][0]["download_handle"]
    assert bridge.app.runtime.browser_adapter_runs.artifact_bytes(server.run["id"], handle)[0] == output


def test_retained_png_artifact_downloads_through_fake_remote_guarded_routes(
    bridge: _Bridge,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_version = importlib_metadata.version

    def available_version(package: str) -> str:
        return "10.0.0" if package == "Pillow" else original_version(package)

    monkeypatch.setattr("spl.adapters.importlib_metadata.version", available_version)
    record = bridge.store.register_object(
        "browser_png_echo",
        "png_echo",
        "default",
        yaml_text=PNG_OUTPUT_YAML,
        owner_id="owner_alpha",
        library="shared_functions",
    )
    request = _local_request(
        bridge.app.runtime,
        bridge.identity,
        record,
        body=bridge.body,
        request_id="adapter-remote-png-artifact",
        output_adapter_id=PNG_PILLOW,
    )
    request["object"]["origin"] = "server"
    request["target"] = {
        "kind": "remote",
        "machine_id": "machine-remote-1",
        "offline_policy": "queue",
    }
    server = _RemoteServer(record)
    monkeypatch.setattr(
        bridge.app.runtime,
        "_require_live_server_channel_credentials",
        lambda *_args, **_kwargs: {"id": "connection-1", "machine_id": "machine-local"},
    )
    monkeypatch.setattr(
        bridge.app.runtime,
        "_server_client_for_credentials",
        lambda *_args, **_kwargs: server,
    )
    monkeypatch.setattr(bridge.app.runtime, "_kick_server_sync", lambda *_args, **_kwargs: None)

    _preflight_and_create(bridge, request)
    assert _upload(bridge, request).status == 200
    finalized = _finalize(bridge, request["request_id"])
    assert finalized.status == 202
    run_id = finalized.body["run_id"]

    output = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    name = "runtime-default.spl-png-pillow-v1.png"
    digest = hashlib.sha256(output).hexdigest()
    server.artifacts[name] = output
    server.run = {
        **server.run,
        "status": "succeeded",
        "result": {"result": {"value": None}},
        "runtime_port_adapters": {
            "terminal": {
                "outputs": [
                    {
                        "port": "default",
                        "adapter_id": PNG_PILLOW,
                        "format_tag": request["outputs"][0]["format_tag"],
                        "artifact_name": name,
                        "size": len(output),
                        "sha256": digest,
                        "media_type": "image/png",
                    }
                ]
            }
        },
    }
    result = asyncio.run(_request(bridge.app, "GET", f"/runs/{run_id}/adapter-result"))
    assert result.status == 200
    [projected] = result.body["outputs"]
    assert projected["availability"] == "retained"
    assert projected["preview"] == {
        "kind": "png",
        "available": True,
        "text": None,
        "json": None,
        "truncated": False,
    }
    downloaded = asyncio.run(
        _request(
            bridge.app,
            "GET",
            f"/runs/{run_id}/adapter-artifacts/{projected['download_handle']}",
        )
    )
    assert downloaded.status == 200
    assert downloaded.raw == output
    assert downloaded.headers["content-type"].startswith("image/png")
    assert API_TOKEN.encode() not in downloaded.raw


def test_colon_identifiers_cross_request_result_and_download_routes(
    bridge: _Bridge,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _remote_request(bridge, "adapter:remote:request:001")
    server = _RemoteServer(bridge.record)
    server.run["id"] = "remote:adapter:run:001"
    monkeypatch.setattr(
        bridge.app.runtime,
        "_require_live_server_channel_credentials",
        lambda *_args, **_kwargs: {"id": "connection-1", "machine_id": "machine-local"},
    )
    monkeypatch.setattr(
        bridge.app.runtime,
        "_server_client_for_credentials",
        lambda *_args, **_kwargs: server,
    )
    monkeypatch.setattr(bridge.app.runtime, "_kick_server_sync", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        bridge.app.runtime.browser_adapter_runs,
        "_artifact_handle",
        lambda _grant: "artifact:handle:001",
    )

    _preflight_and_create(bridge, request)
    assert _upload(bridge, request).status == 200
    finalized = _finalize(bridge, request["request_id"])
    assert finalized.status == 202
    assert finalized.body["run_id"] == server.run["id"]

    output = b"COLON IDENTIFIER OUTPUT"
    name = "runtime-default.colon.txt"
    digest = hashlib.sha256(output).hexdigest()
    server.artifacts[name] = output
    server.run = {
        **server.run,
        "status": "succeeded",
        "result": {"result": {"value": None}},
        "runtime_port_adapters": {
            "terminal": {
                "outputs": [
                    {
                        "port": "default",
                        "adapter_id": TEXT_FILE_UTF8,
                        "format_tag": request["outputs"][0]["format_tag"],
                        "artifact_name": name,
                        "size": len(output),
                        "sha256": digest,
                        "media_type": "text/plain",
                    }
                ]
            }
        },
    }
    result = asyncio.run(_request(bridge.app, "GET", f"/runs/{server.run['id']}/adapter-result"))
    assert result.status == 200
    [projected] = result.body["outputs"]
    assert projected["download_handle"] == "artifact:handle:001"

    downloaded = asyncio.run(
        _request(
            bridge.app,
            "GET",
            f"/runs/{server.run['id']}/adapter-artifacts/{projected['download_handle']}",
        )
    )
    assert downloaded.status == 200
    assert downloaded.raw == output


def test_remote_target_capability_shape_and_object_fences_fail_closed(
    bridge: _Bridge,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _remote_request(bridge, "adapter-remote-request-fence")
    server = _RemoteServer(bridge.record)
    monkeypatch.setattr(
        bridge.app.runtime,
        "_require_live_server_channel_credentials",
        lambda *_args, **_kwargs: {"id": "connection-1", "machine_id": "machine-local"},
    )
    monkeypatch.setattr(
        bridge.app.runtime,
        "_server_client_for_credentials",
        lambda *_args, **_kwargs: server,
    )
    original_machines = server.list_machines
    malformed = original_machines()
    malformed[0]["capabilities"]["spl.remote_run.runtime_port_adapters.v1"]["extra"] = True
    monkeypatch.setattr(server, "list_machines", lambda: malformed)
    with pytest.raises(AdapterRunError) as capability_error:
        bridge.app.runtime.browser_adapter_runs.preflight(request)
    assert (capability_error.value.code, capability_error.value.stage) == (
        "target_unavailable",
        "preflight",
    )

    monkeypatch.setattr(server, "list_machines", original_machines)
    changed = copy.deepcopy(request)
    changed["object"]["selected_version_id"] = "different-version-id"
    with pytest.raises(AdapterRunError) as version_error:
        bridge.app.runtime.browser_adapter_runs.preflight(changed)
    assert version_error.value.code == "object_changed"

    custom_policy = copy.deepcopy(request)
    custom_policy["adapter_policy"] = {"custom_remote": "allow"}
    denied = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=custom_policy))
    assert denied.status == 400
    assert denied.body["code"] == "request_invalid"


def test_remote_missing_dependency_preflight_blocks_without_mutation_or_leak(
    bridge: _Bridge,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _remote_request(bridge, "adapter-remote-missing-dependency")
    server = _RemoteServer(bridge.record)
    explanation = "Dependency missing at /private/worker; credential=remote-secret-sentinel"
    credentials = {
        "id": "connection-1",
        "machine_id": "machine-local",
        "access_token": "remote-access-token-sentinel",
    }
    original_preflight = server.preflight_remote_run

    def blocked_preflight(payload: dict[str, Any]) -> dict[str, Any]:
        response = original_preflight(payload)
        response["can_queue"] = False
        response["checks"] = [
            {
                "code": "runtime_dependency",
                "outcome": "block",
                "reason_code": "dependency_missing",
                "explanation": explanation,
                "evidence_at": "2026-08-10T12:00:00+00:00",
            }
        ]
        return response

    monkeypatch.setattr(
        bridge.app.runtime,
        "_require_live_server_channel_credentials",
        lambda *_args, **_kwargs: credentials,
    )
    monkeypatch.setattr(
        bridge.app.runtime,
        "_server_client_for_credentials",
        lambda *_args, **_kwargs: server,
    )
    monkeypatch.setattr(server, "preflight_remote_run", blocked_preflight)

    response = asyncio.run(_request(bridge.app, "POST", "/runs/adapter-preflights", payload=request))

    assert response.status == 409
    assert response.body == {
        "schema": "spl.daemon.adapter-run-error",
        "schema_version": 1,
        "code": "target_unavailable",
        "stage": "preflight",
        "retryable": False,
    }
    assert server.admission is None
    assert server.uploads == []
    assert bridge.store.list_runs() == []
    assert bridge.app.runtime.browser_adapter_runs._preflights == {}
    assert bridge.app.runtime.browser_adapter_runs._admissions == {}
    for forbidden in (explanation, credentials["access_token"], str(bridge.store.home)):
        assert forbidden.encode() not in response.raw
