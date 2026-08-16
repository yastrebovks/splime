from __future__ import annotations

import base64
import hashlib
import json
import stat
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from spl import SPLClient
from spl._client import RemoteRun
from spl._runtime_port_adapters_client import (
    PreparedClientRuntimeAdapters,
    prepare_client_runtime_adapters,
    server_admission_document,
)
from spl.adapters import BINARY_FILE, TEXT_FILE_UTF8
from spl.core import json_contract as m_json_contract
from spl.core.entities.adapter import Adapter
from spl.core.runtime_port_adapters import RuntimePortAdapterContractError
from spl.daemon.remote_client import ServerClientError
from spl.daemon.runtime_port_adapters import materialize_claimed_runtime_port_adapters
from spl.daemon.server import DaemonRuntime
from spl.daemon_client import _safe_write_download


def save_reverse(path: str, value: str) -> None:
    from pathlib import Path as LocalPath

    LocalPath(path).write_text(value[::-1], encoding="utf-8")


def load_reverse(path: str) -> str:
    from pathlib import Path as LocalPath

    return LocalPath(path).read_text(encoding="utf-8")[::-1]


def _signature() -> dict[str, Any]:
    return {
        "kind": "function",
        "input_order": ["value"],
        "inputs": [
            {
                "name": "value",
                "type": "str",
                "required": True,
                "default": None,
                "sources": [{"node_id": None, "function": "echo", "port": "value"}],
            }
        ],
        "outputs": [
            {
                "name": "default",
                "type": "str",
                "selector": None,
                "ports": [{"name": "default", "type": "str"}],
            }
        ],
        "aliases": [],
        "distributions": [],
    }


def _plan(*, custom: bool = False, policy: str = "deny") -> PreparedClientRuntimeAdapters:
    adapter: Any = TEXT_FILE_UTF8
    if custom:
        adapter = Adapter(
            key="builtins.str@spl.test.reverse-text.v1",
            save=save_reverse,
            load=load_reverse,
            py_type=str,
            format="spl.test.reverse-text.v1",
        )
    return prepare_client_runtime_adapters(
        signature=_signature(),
        args=None,
        kwargs={"value": "payload"},
        output=None,
        adapters={"inputs": {"value": adapter}, "outputs": {"default": adapter}},
        adapter_policy={"custom_remote": policy},
    )


def test_remote_embedded_preset_synthesis_uses_v3_advisory_intent() -> None:
    plan = prepare_client_runtime_adapters(
        signature=_signature(),
        args=None,
        kwargs={"value": "payload"},
        output=None,
        adapters={"inputs": {"value": TEXT_FILE_UTF8}, "outputs": {"default": BINARY_FILE}},
        adapter_policy=None,
    )
    document = json.loads(json.dumps(plan.document))
    for binding in document["bindings"]:
        binding["resolution_source"] = "preset" if binding["direction"] == "output" else "system_default"
    advisories = DaemonRuntime._synthesize_remote_runtime_adapter_semantic_advisories(
        object(),
        document,
        remote_library_refs=None,
    )
    assert advisories is not None
    assert advisories["bindings"] == [
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
    ]


def test_central_terminal_v2_projects_typed_library_save_failure_with_exact_ref() -> None:
    exact_ref = {
        "owner": "owner_alpha",
        "library": "shared",
        "name": "binary-writer",
        "version": 4,
        "adapter_id": "adapter-id",
        "adapter_version_id": "adapter-version-id",
        "content_hash": "a" * 64,
        "signature_hash": "b" * 64,
    }
    state = {
        "status": "failed",
        "manifest": {
            "runtime_port_adapters": {
                "custom_remote": {"requested": False, "allowed": False, "used": False},
                "bindings": [
                    {
                        "direction": "output",
                        "port": "default",
                        "adapter_id": "placeholder-adapter",
                    }
                ],
                "failure": {
                    "schema_version": 1,
                    "code": "output_adapter_save_failed",
                    "stage": "worker_save",
                    "direction": "output",
                    "port": "default",
                    "adapter_id": "library:" + "a" * 64,
                    "adapter_ref": exact_ref,
                    "message": "The selected output Adapter could not save this value.",
                    "retryable": False,
                    "fallback_used": False,
                },
            },
            "runtime_library_adapter_refs": {
                "schema_version": 1,
                "bindings": [{"direction": "output", "port": "default", **exact_ref}],
            },
        },
    }
    evidence = DaemonRuntime._runtime_adapter_terminal_evidence(
        SimpleNamespace(allow_remote_custom_adapters=False),
        state,
        runtime_claim={},
        result=None,
    )
    assert evidence["schema_version"] == 2
    assert evidence["failure"] == {
        **state["manifest"]["runtime_port_adapters"]["failure"],
        "adapter_id": exact_ref["adapter_id"],
        "adapter_ref": exact_ref,
    }


def _request_digest(payload: dict[str, Any]) -> str:
    canonical = {
        "schema_version": payload["schema_version"],
        "run": {key: value for key, value in payload["run"].items() if key != "access_token"},
        "runtime_port_adapters": payload["runtime_port_adapters"],
        "adapter_policy": payload["adapter_policy"],
    }
    if "runtime_library_adapter_refs" in payload:
        canonical["runtime_library_adapter_refs"] = payload["runtime_library_adapter_refs"]
    if "runtime_adapter_semantic_advisories" in payload:
        canonical["runtime_adapter_semantic_advisories"] = payload["runtime_adapter_semantic_advisories"]
    return hashlib.sha256(m_json_contract.dumps(canonical).encode("utf-8")).hexdigest()


class AdmissionServer:
    def __init__(self) -> None:
        self.admission: dict[str, Any] | None = None
        self.uploads: list[tuple[str, str]] = []
        self.cancelled = False
        self.get_admission_calls = 0
        self.create_error: ServerClientError | None = None

    def create_remote_run_admission(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.create_error is not None:
            raise self.create_error
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
        bundle = payload["runtime_port_adapters"]["custom_bundle"]
        if bundle is not None:
            items.append(
                {
                    "role": "custom_bundle",
                    "name": bundle["name"],
                    "size": bundle["size"],
                    "sha256": bundle["sha256"],
                    "uploaded": False,
                }
            )
        self.admission = {
            "schema_version": payload["schema_version"],
            "request_id": payload["request_id"],
            "request_digest_sha256": _request_digest(payload),
            "runtime_port_adapters": payload["runtime_port_adapters"],
            "adapter_policy": payload["adapter_policy"],
            "state": "staging",
            "items": items,
            "run_id": None,
        }
        if "runtime_library_adapter_refs" in payload:
            self.admission["runtime_library_adapter_refs"] = payload["runtime_library_adapter_refs"]
        if "runtime_adapter_semantic_advisories" in payload:
            self.admission["runtime_adapter_semantic_advisories"] = payload["runtime_adapter_semantic_advisories"]
        return self.admission

    def get_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        self.get_admission_calls += 1
        assert self.admission is not None and self.admission["request_id"] == request_id
        return self.admission

    def upload_remote_run_admission_input(
        self,
        request_id: str,
        name: str,
        body: bytes,
        *,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        assert len(body) == size and hashlib.sha256(body).hexdigest() == sha256
        self.uploads.append(("input", name))
        assert self.admission is not None
        item = next(item for item in self.admission["items"] if item["role"] == "input" and item["name"] == name)
        item["uploaded"] = True
        return item

    def upload_remote_run_admission_custom_bundle(
        self,
        request_id: str,
        body: bytes,
        *,
        size: int,
        sha256: str,
    ) -> dict[str, Any]:
        assert len(body) == size and hashlib.sha256(body).hexdigest() == sha256
        self.uploads.append(("custom_bundle", "custom-adapters.py"))
        assert self.admission is not None
        item = next(item for item in self.admission["items"] if item["role"] == "custom_bundle")
        item["uploaded"] = True
        return item

    def finalize_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        assert self.admission is not None
        assert all(item["uploaded"] for item in self.admission["items"])
        self.admission["state"] = "finalized"
        self.admission["run_id"] = "remote-run-1"
        return self.admission

    def cancel_remote_run_admission(self, request_id: str) -> dict[str, Any]:
        assert self.admission is not None
        self.cancelled = True
        self.admission["state"] = "cancelled"
        return self.admission

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        assert run_id == "remote-run-1"
        assert self.admission is not None
        return {
            "id": run_id,
            "status": "queued",
            "runtime_port_adapters": {
                **self.admission["runtime_port_adapters"],
                "adapter_policy": self.admission["adapter_policy"],
                "admission_digest_sha256": self.admission["request_digest_sha256"],
            },
        }


def test_requester_uses_atomic_admission_and_binds_full_request_digest() -> None:
    plan = _plan()
    server = AdmissionServer()
    runtime = object.__new__(DaemonRuntime)
    run_payload = {"object": "echo", "args": [], "kwargs": plan.kwargs, "output": None}

    result = runtime._create_remote_runtime_adapter_run(  # noqa: SLF001
        server,  # type: ignore[arg-type]
        request_id="request-1",
        payload=run_payload,
        document=plan.document,
        adapter_policy=plan.adapter_policy,
    )

    assert result["id"] == "remote-run-1"
    assert server.uploads == [("input", plan.document["inputs"][0]["name"])]
    assert server.admission is not None
    assert server.admission["state"] == "finalized"

    conflicting = AdmissionServer()
    prior_payload = {
        "schema_version": 1,
        "request_id": "request-1",
        "run": {**run_payload, "object": "different"},
        "runtime_port_adapters": server_admission_document(plan.document),
        "adapter_policy": plan.adapter_policy,
    }
    conflicting.create_remote_run_admission(prior_payload)
    conflicting.create_error = ServerClientError(
        502,
        "ambiguous response",
    )
    with pytest.raises(RuntimePortAdapterContractError, match="different runtime adapter admission"):
        runtime._create_remote_runtime_adapter_run(  # noqa: SLF001
            conflicting,  # type: ignore[arg-type]
            request_id="request-1",
            payload=run_payload,
            document=plan.document,
            adapter_policy=plan.adapter_policy,
        )
    assert conflicting.cancelled


def test_requester_uses_v3_admission_when_semantic_advisories_are_present() -> None:
    plan = _plan()
    assert plan.runtime_adapter_semantic_advisories is not None
    server = AdmissionServer()
    runtime = object.__new__(DaemonRuntime)
    run_payload = {"object": "echo", "args": [], "kwargs": plan.kwargs, "output": None}

    runtime._create_remote_runtime_adapter_run(  # noqa: SLF001
        server,  # type: ignore[arg-type]
        request_id="request-v3",
        payload=run_payload,
        document=plan.document,
        adapter_policy=plan.adapter_policy,
        runtime_adapter_semantic_advisories=plan.runtime_adapter_semantic_advisories,
    )

    assert server.admission is not None
    assert server.admission["schema_version"] == 3
    assert server.admission["runtime_library_adapter_refs"] == {
        "schema_version": 1,
        "bindings": [],
    }
    assert server.admission["runtime_adapter_semantic_advisories"] == plan.runtime_adapter_semantic_advisories
    assert server.admission["request_digest_sha256"] == _request_digest(
        {
            "schema_version": 3,
            "request_id": "request-v3",
            "run": run_payload,
            "runtime_port_adapters": server_admission_document(plan.document),
            "adapter_policy": plan.adapter_policy,
            "runtime_library_adapter_refs": {"schema_version": 1, "bindings": []},
            "runtime_adapter_semantic_advisories": plan.runtime_adapter_semantic_advisories,
        }
    )


def test_v3_admission_accepts_omitted_empty_library_adapter_projection() -> None:
    advisories = {
        "schema_version": 1,
        "bindings": [
            {
                "direction": "input",
                "port": "value",
                "adapter_id": "text-file-utf8",
                "port_semantic_type": "str",
                "adapter_semantic_type": "builtins.str",
                "adapter_semantic_category": "text",
                "state": "recommended",
                "acknowledged": False,
                "acknowledgement_source": None,
            }
        ],
    }
    admission = {
        "schema_version": 3,
        "request_id": "request-v3",
        "request_digest_sha256": "a" * 64,
        "runtime_port_adapters": {"schema_version": 1},
        "adapter_policy": {"custom_remote": "deny"},
        "runtime_adapter_semantic_advisories": advisories,
    }

    assert DaemonRuntime._runtime_admission_projection_matches(
        admission,
        request_id="request-v3",
        request_digest_sha256="a" * 64,
        document={"schema_version": 1},
        adapter_policy={"custom_remote": "deny"},
        runtime_library_adapter_refs={"schema_version": 1, "bindings": []},
        runtime_adapter_semantic_advisories=advisories,
    )


def test_application_identity_collision_is_never_recovered_as_transport_ambiguity() -> None:
    plan = _plan()
    server = AdmissionServer()
    server.create_error = ServerClientError(
        409,
        "identity conflict",
        code="runtime_admission_identity_conflict",
    )
    runtime = object.__new__(DaemonRuntime)

    with pytest.raises(ServerClientError) as raised:
        runtime._create_remote_runtime_adapter_run(  # noqa: SLF001
            server,  # type: ignore[arg-type]
            request_id="request-1",
            payload={"object": "echo", "args": [], "kwargs": plan.kwargs},
            document=plan.document,
            adapter_policy=plan.adapter_policy,
        )
    assert raised.value.code == "runtime_admission_identity_conflict"
    assert server.get_admission_calls == 0


def _claim(plan: PreparedClientRuntimeAdapters) -> tuple[dict[str, Any], dict[str, bytes]]:
    flat = server_admission_document(plan.document)
    bodies = {item["name"]: base64.b64decode(item["content_base64"], validate=True) for item in plan.document["inputs"]}
    flat["inputs"] = [
        {
            **item,
            "verified": True,
            "download_url": f"/remote-runs/run-1/inputs/{item['name']}",
        }
        for item in flat["inputs"]
    ]
    if flat["custom_bundle"] is not None:
        bundle = plan.document["custom_bundle"]
        assert bundle is not None
        bodies[bundle["name"]] = base64.b64decode(bundle["content_base64"], validate=True)
        flat["custom_bundle"] = {
            **flat["custom_bundle"],
            "verified": True,
            "download_url": "/remote-runs/run-1/custom-bundle",
        }
    return (
        {
            **flat,
            "adapter_policy": plan.adapter_policy,
            "admission_digest_sha256": "a" * 64,
        },
        bodies,
    )


@pytest.mark.parametrize(
    ("caller_policy", "machine_enabled", "allowed"),
    [
        ("deny", False, False),
        ("allow", False, False),
        ("deny", True, False),
        ("allow", True, True),
    ],
)
def test_target_custom_remote_policy_requires_two_sided_consent(
    caller_policy: str,
    machine_enabled: bool,
    allowed: bool,
) -> None:
    plan = _plan(custom=True, policy=caller_policy)
    claim, bodies = _claim(plan)
    downloads: list[str] = []

    def download(url: str, size: int, sha256: str) -> bytes:
        downloads.append(url)
        name = "custom-adapters.py" if url.endswith("/custom-bundle") else url.rsplit("/", 1)[-1]
        body = bodies[name]
        assert len(body) == size and hashlib.sha256(body).hexdigest() == sha256
        return body

    if allowed:
        document, policy = materialize_claimed_runtime_port_adapters(
            claim,
            signature=_signature(),
            args=plan.args,
            kwargs=plan.kwargs,
            allow_remote_custom_adapters=machine_enabled,
            download=download,
        )
        assert policy == {"custom_remote": "allow"}
        assert document["custom_bundle"] is not None
        assert len(downloads) == 2
    else:
        with pytest.raises(RuntimePortAdapterContractError, match="custom remote|does not permit"):
            materialize_claimed_runtime_port_adapters(
                claim,
                signature=_signature(),
                args=plan.args,
                kwargs=plan.kwargs,
                allow_remote_custom_adapters=machine_enabled,
                download=download,
            )
        assert downloads == []


def test_adapted_remote_terminal_result_is_path_free_and_output_bound() -> None:
    secret_root = Path("/private/target-daemon/runs/local-1")
    body = b"payload"
    digest = hashlib.sha256(body).hexdigest()
    record = {
        "port": "default",
        "name": "result-default.txt",
        "size": len(body),
        "sha256": digest,
        "format_tag": "spl.text.utf8.v1",
        "adapter_id": TEXT_FILE_UTF8,
        "media_type": "text/plain",
        "result_path": [],
    }
    raw = {
        "result": None,
        "artifacts": {record["name"]: str(secret_root / record["name"])},
        "runtime_port_adapter_outputs": [record],
        "manifest": {"run_dir": str(secret_root)},
    }
    projected = DaemonRuntime._path_free_runtime_adapter_remote_result(  # noqa: SLF001
        raw,
        artifacts=[{"name": record["name"], "size": len(body), "sha256": digest}],
    )

    assert projected == {
        "result": None,
        "artifacts": {record["name"]: record["name"]},
        "runtime_port_adapter_outputs": [record],
    }
    assert str(secret_root) not in json.dumps(projected)


def test_machine_capability_reports_custom_runtime_policy(tmp_path: Path) -> None:
    pytest.importorskip("quart")
    from spl.daemon.server import create_app
    from spl.daemon.store import RegistryStore

    store = RegistryStore(tmp_path)
    app = create_app(store, allow_remote_custom_adapters=True)
    try:
        capability = app.runtime._authoritative_server_capabilities({})[  # noqa: SLF001
            "spl.remote_run.runtime_port_adapters.v1"
        ]
        assert capability == {
            "schema_version": 1,
            "transport": True,
            "custom_adapter_execution": {"implemented": True, "enabled": True},
        }
    finally:
        app.runtime.shutdown()
        store.close()


def test_importing_sdk_does_not_enable_remote_custom_policy() -> None:
    assert SPLClient is not None


class CompletedAdapterDaemon:
    def __init__(self, state: dict[str, Any], body: bytes) -> None:
        self.state = state
        self.body = body
        self.download_targets: list[Path] = []
        self.download_target_modes: list[int] = []

    def wait_remote_run(self, run_id: str, **options: Any) -> dict[str, Any]:
        del options
        assert run_id == self.state["id"]
        return self.state

    def get_remote_run(self, run_id: str) -> dict[str, Any]:
        assert run_id == self.state["id"]
        return self.state

    def list_remote_artifacts(self, run_id: str) -> list[str]:
        assert run_id == self.state["id"]
        return ["result-default.txt"]

    def download_remote_artifact(self, run_id: str, name: str, target: Path) -> Path:
        assert run_id == self.state["id"] and name == "result-default.txt"
        self.download_targets.append(target)
        self.download_target_modes.append(stat.S_IMODE(target.stat().st_mode))
        return _safe_write_download(target / name, self.body)


def _completed_remote_state(plan: PreparedClientRuntimeAdapters, body: bytes) -> dict[str, Any]:
    digest = hashlib.sha256(body).hexdigest()
    record = {
        "port": "default",
        "name": "result-default.txt",
        "size": len(body),
        "sha256": digest,
        "format_tag": "spl.text.utf8.v1",
        "adapter_id": TEXT_FILE_UTF8,
        "media_type": "text/plain",
        "result_path": [],
    }
    admission = server_admission_document(plan.document)
    return {
        "id": "remote-run-complete",
        "status": "succeeded",
        "keep": True,
        "result": {
            "result": None,
            "artifacts": {record["name"]: record["name"]},
            "runtime_port_adapter_outputs": [record],
        },
        "runtime_port_adapters": {
            **admission,
            "adapter_policy": plan.adapter_policy,
            "admission_digest_sha256": "a" * 64,
            "terminal": {
                "schema_version": 1,
                "custom_execution": {"requested": False, "allowed": False, "used": False},
                "outputs": [
                    {
                        "port": "default",
                        "adapter_id": TEXT_FILE_UTF8,
                        "format_tag": "spl.text.utf8.v1",
                        "semantic_type": "str",
                        "artifact_name": record["name"],
                        "size": len(body),
                        "sha256": digest,
                        "media_type": "text/plain",
                        "result_path": [],
                    }
                ],
                "failure": None,
            },
        },
    }


def test_completed_remote_collect_and_restart_bind_durable_terminal_evidence(tmp_path: Path) -> None:
    plan = _plan()
    body = b"REMOTE"
    state = _completed_remote_state(plan, body)
    client = SPLClient(daemon_port=8765)
    client._daemon = CompletedAdapterDaemon(state, body)  # type: ignore[assignment]

    same_process = RemoteRun(client, state, server_side=True, runtime_adapter_plan=plan).collect(
        artifacts_dir=tmp_path / "same-process",
        progress=False,
    )
    restarted = RemoteRun(client, state, server_side=True).collect(
        artifacts_dir=tmp_path / "restarted",
        progress=False,
    )

    assert same_process.output == "REMOTE"
    assert restarted.output == "REMOTE"
    assert next(iter(restarted.downloaded_artifacts.values())).read_bytes() == body


def test_default_runtime_output_staging_uses_canonical_private_root_and_cleans_up() -> None:
    plan = _plan()
    body = b"REMOTE"
    state = _completed_remote_state(plan, body)
    daemon = CompletedAdapterDaemon(state, body)
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]

    result = RemoteRun(client, state, server_side=True, runtime_adapter_plan=plan).collect(progress=False)

    assert result.output == "REMOTE"
    assert result.downloaded_artifacts == {}
    assert len(daemon.download_targets) == 1
    stage = daemon.download_targets[0]
    assert stage.parent == Path(tempfile.gettempdir()).resolve(strict=True)
    assert daemon.download_target_modes == [0o700]
    assert not stage.exists()


def test_default_runtime_output_staging_cleans_up_after_download_verification_failure() -> None:
    plan = _plan()
    state = _completed_remote_state(plan, b"REMOTE")
    daemon = CompletedAdapterDaemon(state, b"BROKEN")
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="failed size/checksum verification"):
        RemoteRun(client, state, server_side=True, runtime_adapter_plan=plan).collect(progress=False)

    assert len(daemon.download_targets) == 1
    assert not daemon.download_targets[0].exists()


def test_default_runtime_output_staging_canonicalizes_symlinked_temp_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_temp_root = tmp_path / "private" / "var" / "folders"
    real_temp_root.mkdir(parents=True)
    reported_temp_root = tmp_path / "var" / "folders"
    reported_temp_root.parent.symlink_to(real_temp_root.parent, target_is_directory=True)
    monkeypatch.setattr("spl._client.tempfile.gettempdir", lambda: str(reported_temp_root))
    plan = _plan()
    body = b"REMOTE"
    state = _completed_remote_state(plan, body)
    daemon = CompletedAdapterDaemon(state, body)
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]

    result = RemoteRun(client, state, server_side=True, runtime_adapter_plan=plan).collect(progress=False)

    assert result.output == "REMOTE"
    assert daemon.download_targets[0].parent == real_temp_root
    assert not daemon.download_targets[0].exists()


def test_user_supplied_runtime_output_destination_still_rejects_symlink_traversal(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(outside, target_is_directory=True)
    plan = _plan()
    body = b"REMOTE"
    state = _completed_remote_state(plan, body)
    daemon = CompletedAdapterDaemon(state, body)
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]

    with pytest.raises(ValueError, match="must not traverse a symbolic link"):
        RemoteRun(client, state, server_side=True, runtime_adapter_plan=plan).collect(
            artifacts_dir=alias / "nested",
            progress=False,
        )

    assert not (outside / "nested" / "result-default.txt").exists()


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_terminal_run_without_success_never_allocates_default_output_staging(
    status: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan()
    body = b"REMOTE"
    state = _completed_remote_state(plan, body)
    state.update({"status": status, "error": f"run {status}"})
    daemon = CompletedAdapterDaemon(state, body)
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]
    monkeypatch.setattr(
        "spl._client.tempfile.TemporaryDirectory",
        lambda *args, **kwargs: pytest.fail("terminal non-success must not allocate output staging"),
    )

    with pytest.raises(RuntimeError, match=f"ended as '{status}'"):
        RemoteRun(client, state, server_side=True, runtime_adapter_plan=plan).collect(progress=False)

    assert daemon.download_targets == []


def test_wait_timeout_never_allocates_default_output_staging(monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan()
    body = b"REMOTE"
    state = _completed_remote_state(plan, body)
    daemon = CompletedAdapterDaemon(state, body)
    client = SPLClient(daemon_port=8765)
    client._daemon = daemon  # type: ignore[assignment]
    run = RemoteRun(client, state, server_side=True, runtime_adapter_plan=plan)
    monkeypatch.setattr(run, "wait", lambda **options: (_ for _ in ()).throw(TimeoutError("timed out")))
    monkeypatch.setattr(
        "spl._client.tempfile.TemporaryDirectory",
        lambda *args, **kwargs: pytest.fail("timeout must not allocate output staging"),
    )

    with pytest.raises(TimeoutError, match="timed out"):
        run.collect(progress=False)

    assert daemon.download_targets == []


def test_download_writer_is_anchored_across_intermediate_ancestor_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted = tmp_path / "trusted"
    (trusted / "sub").mkdir(parents=True)
    moved = tmp_path / "moved"
    attacker = tmp_path / "attacker"
    attacker.mkdir()
    original_open = __import__("os").open
    swapped = False

    def swapping_open(path: Any, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
        nonlocal swapped
        if path == "sub" and dir_fd is not None and not swapped:
            swapped = True
            trusted.rename(moved)
            trusted.symlink_to(attacker, target_is_directory=True)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr("spl.daemon_client.os.open", swapping_open)
    _safe_write_download(trusted / "sub" / "result.txt", b"anchored")

    assert swapped
    assert (moved / "sub" / "result.txt").read_bytes() == b"anchored"
    assert not (attacker / "sub" / "result.txt").exists()
