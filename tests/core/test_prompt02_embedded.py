from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import os
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization

from spl import SPLClient
from spl.daemon_client import ClientError
from spl.daemon.worker import WorkerNodeEnvironmentProvider
from spl.core.ir.utils import spl_compile_to_source
from spl.embedded import (
    EmbeddedBackend,
    PublicRunReceiptEmitter,
    verify_public_bundle,
)
from spl.public import parse_public_ref
from spl.runtime_environment import (
    PUBLIC_EMBEDDED_EXECUTOR,
    PUBLIC_RUNTIME_POLICY_NAME,
    PUBLIC_RUNTIME_POLICY_VERSION,
    PUBLIC_RUNTIME_RESOLVER_NAME,
    PUBLIC_RUNTIME_SPDX_ALLOWLIST,
    environment_identity,
    runtime_lock_document,
)


FIXTURE = Path(__file__).parents[1] / "fixtures" / "public_manifest_v1.json"
REGISTRY = "https://registry.example.test"
FIXTURE_PUBLIC_KEY = "ebVWLo/mVPlAeLES6KmLp5AfhTrmlb7X4OORC60ElmQ="
FIXTURE_KEY_ID = "ed25519-sha256:65b60673d6ed884bf01c2c222d82ada0740f29ac3355d6a925c81f17f47a27b8"
TRUSTED_KEYS = {REGISTRY: {FIXTURE_KEY_ID: FIXTURE_PUBLIC_KEY}}
COMPILED_EXECUTION = {
    "kind": "spl-compiled-python-v1",
    "member": "object.py",
    "compiler": "spl.core.ir.utils.spl_compile_to_source",
}


def _compile_yaml_bytes(value: bytes) -> bytes:
    with tempfile.TemporaryDirectory(prefix="splime-prompt02-compile-") as raw:
        source = Path(raw) / "object.yaml"
        source.write_bytes(value)
        return spl_compile_to_source(source).encode("utf-8")


def _fixture() -> dict[str, object]:
    value = json.loads(FIXTURE.read_text(encoding="utf-8"))
    manifest = copy.deepcopy(value["manifest"])
    original_bundle = base64.b64decode(value["bundle_base64"], validate=True)
    with zipfile.ZipFile(io.BytesIO(original_bundle)) as archive:
        entries = [(info, archive.read(info)) for info in archive.infolist()]
    object_yaml = next(data for info, data in entries if info.filename == "object.yaml")
    entries.append((_regular_info("object.py"), _compile_yaml_bytes(object_yaml)))
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
        for info, data in entries:
            archive.writestr(info, data)
    bundle = stream.getvalue()
    manifest["bundle_hash"] = hashlib.sha256(bundle).hexdigest()
    manifest["members"] = [
        {"path": info.filename, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)} for info, data in entries
    ]
    manifest["execution"] = COMPILED_EXECUTION
    manifest["runtime_locks"] = [{**_test_runtime_lock(), "names": ["root"]}]
    manifest_hash = hashlib.sha256(_canonical(manifest)).hexdigest()
    signature = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33))).sign(
        f"{manifest_hash}:{manifest['bundle_hash']}".encode("ascii")
    )
    value.update(
        {
            "bundle_base64": base64.b64encode(bundle).decode("ascii"),
            "bundle_hash": manifest["bundle_hash"],
            "manifest": manifest,
            "manifest_hash": manifest_hash,
            "release_id": manifest_hash,
            "signature": {
                "algorithm": "ed25519",
                "key_id": FIXTURE_KEY_ID,
                "value": base64.b64encode(signature).decode("ascii"),
            },
        }
    )
    return value


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()


def _test_artifact(project: str, version: str) -> dict[str, object]:
    normalized = project.replace("-", "_")
    return {
        "project": project,
        "version": version,
        "direct": True,
        "parent_requirements": [f"{project}=={version}"],
        "evaluated_requirements": [],
        "filename": f"{normalized}-{version}-py3-none-any.whl",
        "size": 1,
        "sha256": hashlib.sha256(f"{project}=={version}".encode()).hexdigest(),
        "tags": ["py3-none-any"],
        "requires_python": ">=3.13",
        "yanked": False,
        "root_is_purelib": True,
        "native_members": [],
        "license_expression": "MIT",
        "metadata_url": f"https://pypi.org/pypi/{project}/{version}/json",
        "metadata_sha256": "3" * 64,
        "source": {
            "kind": "official-pypi",
            "url": f"https://files.pythonhosted.org/packages/{normalized}-{version}-py3-none-any.whl",
        },
    }


def _test_runtime_lock(
    *,
    python: str = "3.13",
    artifacts: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    closure = artifacts or []
    return runtime_lock_document(
        python=python,
        requirements=[{"requirement": f"{item['project']}=={item['version']}", "extras": []} for item in closure],
        executor=PUBLIC_EMBEDDED_EXECUTOR,
        artifacts=closure,
        policy={
            "name": PUBLIC_RUNTIME_POLICY_NAME,
            "version": PUBLIC_RUNTIME_POLICY_VERSION,
            "spdx_allowlist": list(PUBLIC_RUNTIME_SPDX_ALLOWLIST),
            "wheel_policy": "non-yanked-universal-pure-python-only",
        },
        resolver={"name": PUBLIC_RUNTIME_RESOLVER_NAME, "version": 1},
    )


def _verify_bundle(
    envelope: dict[str, object],
    bundle: bytes,
    *,
    target_dir: Path | None = None,
) -> dict[str, object]:
    return verify_public_bundle(
        envelope,
        bundle,
        registry_url=REGISTRY,
        trusted_keys=TRUSTED_KEYS,
        target_dir=target_dir,
    )


def _signed_archive(entries: list[tuple[zipfile.ZipInfo, bytes]]) -> tuple[dict[str, object], bytes]:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
        for info, data in entries:
            archive.writestr(info, data)
    bundle = stream.getvalue()
    fixture = _fixture()
    manifest = copy.deepcopy(fixture["manifest"])
    manifest["bundle_hash"] = hashlib.sha256(bundle).hexdigest()
    manifest["members"] = [
        {
            "path": info.filename,
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
        }
        for info, data in entries
    ]
    if any(info.filename == "object.py" for info, _data in entries):
        manifest["execution"] = COMPILED_EXECUTION
    return _signed_manifest(manifest, bundle)


def _signed_manifest(manifest: dict[str, object], bundle: bytes) -> tuple[dict[str, object], bytes]:
    manifest_hash = hashlib.sha256(_canonical(manifest)).hexdigest()
    private_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    signature = private_key.sign(f"{manifest_hash}:{manifest['bundle_hash']}".encode("ascii"))
    envelope = {
        "manifest": manifest,
        "manifest_hash": manifest_hash,
        "bundle_hash": manifest["bundle_hash"],
        "signature": {
            "algorithm": "ed25519",
            "key_id": FIXTURE_KEY_ID,
            "value": base64.b64encode(signature).decode("ascii"),
        },
    }
    return envelope, bundle


def _cache_release(
    client: SPLClient,
    reference: str,
    envelope: dict[str, object],
    bundle: bytes,
) -> EmbeddedBackend:
    backend = client._embedded_backend
    assert isinstance(backend, EmbeddedBackend)
    backend.trusted_keys = TRUSTED_KEYS
    backend._install_bundle(envelope, bundle)
    manifest = envelope["manifest"]
    assert isinstance(manifest, dict)
    resolved = {
        "schema": "spl.public_resolution.v1",
        "registry_url": backend.transport.registry_url,
        "reference": reference,
        "release": {
            "id": envelope["manifest_hash"],
            "bundle_hash": envelope["bundle_hash"],
            "manifest_hash": envelope["manifest_hash"],
            "version": manifest["version"],
            "version_id": manifest["version_id"],
        },
    }
    resolution = backend._resolution_path(parse_public_ref(reference))
    resolution.parent.mkdir(parents=True, exist_ok=True)
    resolution.write_bytes(_canonical(resolved))
    return backend


def _regular_info(name: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_STORED
    info.create_system = 3
    info.external_attr = 0o100644 << 16
    return info


def test_substituted_envelope_key_is_not_a_trust_anchor() -> None:
    """A valid attacker signature must not authenticate its own supplied key."""

    envelope, bundle = _signed_archive(
        [
            (_regular_info("object.yaml"), b"- !DFunction\n  name: add\n"),
            (_regular_info("object.json"), b"{}"),
        ]
    )
    manifest = copy.deepcopy(envelope["manifest"])
    assert isinstance(manifest, dict)
    identity = manifest["identity"]
    assert isinstance(identity, dict)
    identity["name"] = "attacker-substitution"
    manifest_hash = hashlib.sha256(_canonical(manifest)).hexdigest()
    attacker = Ed25519PrivateKey.generate()
    attacker_public_key = attacker.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    substituted = {
        "manifest": manifest,
        "manifest_hash": manifest_hash,
        "bundle_hash": envelope["bundle_hash"],
        "signature": {
            "algorithm": "ed25519",
            "public_key": base64.b64encode(attacker_public_key).decode("ascii"),
            "value": base64.b64encode(
                attacker.sign(f"{manifest_hash}:{envelope['bundle_hash']}".encode("ascii"))
            ).decode("ascii"),
        },
    }

    with pytest.raises(ClientError) as error:
        verify_public_bundle(substituted, bundle)
    assert error.value.code in {
        "public_trust_anchor_required",
        "public_signature_key_untrusted",
    }


def test_public_signing_key_is_bound_to_registry_origin_and_known_key_id() -> None:
    fixture = _fixture()
    bundle = base64.b64decode(fixture.pop("bundle_base64"), validate=True)

    with pytest.raises(ClientError) as wrong_origin:
        verify_public_bundle(
            fixture,
            bundle,
            registry_url="https://other-registry.example.test",
            trusted_keys=TRUSTED_KEYS,
        )
    assert wrong_origin.value.code == "public_signature_key_untrusted"

    unknown = copy.deepcopy(fixture)
    unknown["signature"]["key_id"] = "ed25519-sha256:" + "0" * 64
    with pytest.raises(ClientError) as unknown_key:
        _verify_bundle(unknown, bundle)
    assert unknown_key.value.code == "public_signature_key_untrusted"


def test_cached_release_cannot_replay_across_registry_origins(tmp_path: Path) -> None:
    fixture = _fixture()
    bundle = base64.b64decode(fixture.pop("bundle_base64"), validate=True)
    first = EmbeddedBackend(REGISTRY, tmp_path, trusted_keys=TRUSTED_KEYS)
    first._install_bundle(fixture, bundle)
    resolved = {
        "release": {
            "bundle_hash": fixture["bundle_hash"],
        }
    }
    replay = EmbeddedBackend(
        "https://other-registry.example.test",
        tmp_path,
        trusted_keys=TRUSTED_KEYS,
    )
    with pytest.raises(ClientError) as error:
        replay._cached_release(resolved)
    assert error.value.code == "public_signature_key_untrusted"


def test_embedded_construction_does_not_construct_a_daemon(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("daemon client was constructed")

    monkeypatch.setattr("spl._client.Client.__init__", forbidden)
    client = SPLClient.embedded(
        registry_url=REGISTRY,
        cache_dir=tmp_path,
        trusted_keys=TRUSTED_KEYS,
    )
    assert client.mode == "embedded"
    assert not (tmp_path / "daemon").exists()
    with pytest.raises(ClientError) as error:
        client.health()
    assert error.value.code == "feature_not_supported"


def test_embedded_rejects_plain_names_and_untrusted_execution(tmp_path: Path) -> None:
    client = SPLClient.embedded(
        registry_url="https://registry.example.test",
        cache_dir=tmp_path,
    )
    with pytest.raises(ValueError, match="splime"):
        client.call("local-name", progress=False)


def test_embedded_run_receipts_are_minimal_ordered_and_execution_safe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[dict[str, object]] = []

    class Emitter:
        def emit(self, **payload: object) -> None:
            events.append(payload)

    backend = EmbeddedBackend(
        "https://registry.example.test",
        tmp_path,
        receipt_emitter=Emitter(),
    )
    bundle_dir = tmp_path / "prepared"
    bundle_dir.mkdir()
    (bundle_dir / "object.yaml").write_text("- !DFunction\n  name: add\n", encoding="utf-8")
    (bundle_dir / "object.py").write_text("def add():\n    return 5\n", encoding="utf-8")
    resolved = {
        "reference": "splime://@alice/default/add@1",
        "release": {"id": "release-minimal-1"},
    }
    envelope = {
        "bundle_hash": "1" * 64,
        "manifest_hash": "a" * 64,
        "signature": {"key_id": FIXTURE_KEY_ID},
        "manifest": {
            "callable": True,
            "identity": {"entry_type": "object"},
            "execution": COMPILED_EXECUTION,
            "runtime_locks": [{**_test_runtime_lock(), "names": ["root"]}],
        },
    }
    monkeypatch.setattr(
        backend,
        "_resolve_and_cache",
        lambda reference: (resolved, envelope, bundle_dir),
    )
    monkeypatch.setattr(backend, "_trusted", lambda bundle_hash: True)
    monkeypatch.setattr(
        backend,
        "_ensure_environments",
        lambda manifest, **kwargs: {"root": Path(sys.executable)},
    )
    monkeypatch.setattr(
        backend,
        "_execute",
        lambda *args, **kwargs: ({"id": "local", "status": "succeeded"}, {"result": 5}, {}),
    )

    result = backend.call(
        "splime://@alice/default/add@1",
        args=[2, 3],
        kwargs=None,
        timeout_seconds=None,
        artifacts_dir=None,
        trust=False,
    )

    assert result[1] == {"result": 5}
    assert [event["state"] for event in events] == ["started", "success"]
    assert events[0]["event_id"] == events[1]["event_id"]
    assert all(
        set(event)
        == {
            "release_id",
            "event_id",
            "state",
            "runtime_family",
            "occurred_at",
        }
        for event in events
    )
    assert "result" not in json.dumps(events)


def test_embedded_run_receipt_failure_and_opt_out_never_change_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class OfflineEmitter:
        def emit(self, **payload: object) -> None:
            raise OSError("offline")

    client = SPLClient.embedded(
        "https://registry.example.test",
        tmp_path / "disabled",
        run_receipts=False,
    )
    assert client._embedded_backend is not None
    assert client._embedded_backend.receipt_emitter is None

    backend = EmbeddedBackend(
        "https://registry.example.test",
        tmp_path / "offline",
        receipt_emitter=OfflineEmitter(),
    )
    bundle_dir = tmp_path / "offline" / "prepared"
    bundle_dir.mkdir(parents=True)
    (bundle_dir / "object.yaml").write_text("- !DFunction\n  name: fail\n", encoding="utf-8")
    (bundle_dir / "object.py").write_text("def fail():\n    return 9\n", encoding="utf-8")
    monkeypatch.setattr(
        backend,
        "_resolve_and_cache",
        lambda reference: (
            {"reference": "splime://@alice/default/fail@1", "release": {"id": "release-fail-1"}},
            {
                "bundle_hash": "2" * 64,
                "manifest_hash": "b" * 64,
                "signature": {"key_id": FIXTURE_KEY_ID},
                "manifest": {
                    "callable": True,
                    "identity": {"entry_type": "object"},
                    "execution": COMPILED_EXECUTION,
                    "runtime_locks": [{**_test_runtime_lock(), "names": ["root"]}],
                },
            },
            bundle_dir,
        ),
    )
    monkeypatch.setattr(backend, "_trusted", lambda bundle_hash: True)
    monkeypatch.setattr(
        backend,
        "_ensure_environments",
        lambda manifest, **kwargs: {"root": Path(sys.executable)},
    )
    monkeypatch.setattr(
        backend,
        "_execute",
        lambda *args, **kwargs: ({"id": "local", "status": "succeeded"}, {"result": 9}, {}),
    )
    assert backend.call(
        "splime://@alice/default/fail@1",
        args=[],
        kwargs={},
        timeout_seconds=None,
        artifacts_dir=None,
        trust=False,
    )[1] == {"result": 9}

    terminal_events: list[dict[str, object]] = []

    class RecordingEmitter:
        def emit(self, **payload: object) -> None:
            terminal_events.append(payload)

    backend.receipt_emitter = RecordingEmitter()
    monkeypatch.setattr(
        backend,
        "_execute",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("private failure detail")),
    )
    with pytest.raises(RuntimeError, match="private failure detail"):
        backend.call(
            "splime://@alice/default/fail@1",
            args=["secret argument"],
            kwargs={},
            timeout_seconds=None,
            artifacts_dir=None,
            trust=False,
        )
    assert [event["state"] for event in terminal_events] == ["started", "failure"]
    assert terminal_events[0]["event_id"] == terminal_events[1]["event_id"]
    assert "private failure detail" not in json.dumps(terminal_events)
    assert "secret argument" not in json.dumps(terminal_events)


def _run_receipt_payload(state: str, *, event_id: str = "event-lifecycle-1") -> dict[str, str]:
    return {
        "release_id": "release-lifecycle-1",
        "event_id": event_id,
        "state": state,
        "runtime_family": "cpython",
        "occurred_at": "2026-08-25T12:00:00+00:00",
    }


def test_run_receipts_flush_at_normal_short_lived_interpreter_exit(tmp_path: Path) -> None:
    output = tmp_path / "received.jsonl"
    source_root = Path(__file__).parents[2] / "src"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(source_root), environment.get("PYTHONPATH", "")) if part
    )
    script = r"""
import json
import sys
import time
from pathlib import Path

from spl.embedded import PublicRunReceiptEmitter

output = Path(sys.argv[1])

class RecordingTransport:
    def post_json(self, path, value):
        time.sleep(0.1)
        with output.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"path": path, **value}, sort_keys=True) + "\n")

emitter = PublicRunReceiptEmitter("https://registry.example.test")
emitter.transport = RecordingTransport()
base = {
    "release_id": "release-process-exit-1",
    "event_id": "event-process-exit-1",
    "runtime_family": "cpython",
    "occurred_at": "2026-08-25T12:00:00+00:00",
}
emitter.emit(**base, state="started")
emitter.emit(**base, state="success")
"""

    completed = subprocess.run(
        [sys.executable, "-c", script, str(output)],
        cwd=Path(__file__).parents[2],
        env=environment,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    received = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert [item["state"] for item in received] == ["started", "success"]
    assert {item["path"] for item in received} == {"/public/v1/run-receipts"}


def test_run_receipt_flush_has_a_hard_bound_when_transport_stalls() -> None:
    entered = threading.Event()
    release = threading.Event()

    class StalledTransport:
        def post_json(self, path: str, value: object) -> None:
            entered.set()
            release.wait(2)

    emitter = PublicRunReceiptEmitter("https://registry.example.test")
    emitter.transport = StalledTransport()
    emitter.emit(**_run_receipt_payload("started"))
    assert entered.wait(1)

    started_at = time.monotonic()
    assert emitter.flush(0.05) is False
    elapsed = time.monotonic() - started_at

    assert 0.04 <= elapsed < 0.5
    release.set()
    assert emitter.flush(1) is True


def test_long_object_execution_keeps_receipts_ordered_and_terminal_emit_nonblocking() -> None:
    started_delivered = threading.Event()
    terminal_entered = threading.Event()
    release_terminal = threading.Event()
    events: list[str] = []

    class ControlledTransport:
        def post_json(self, path: str, value: dict[str, object]) -> None:
            events.append(str(value["state"]))
            if value["state"] == "started":
                started_delivered.set()
                return
            terminal_entered.set()
            release_terminal.wait(2)

    emitter = PublicRunReceiptEmitter("https://registry.example.test")
    emitter.transport = ControlledTransport()
    emitter.emit(**_run_receipt_payload("started", event_id="event-long-run-1"))
    assert started_delivered.wait(1)

    time.sleep(0.15)  # The shutdown allowance must not start while the Object is running.
    emitted_at = time.monotonic()
    emitter.emit(**_run_receipt_payload("success", event_id="event-long-run-1"))
    emit_elapsed = time.monotonic() - emitted_at

    assert emit_elapsed < 0.05
    assert terminal_entered.wait(1)
    assert emitter.flush(0.02) is False
    release_terminal.set()
    assert emitter.flush(1) is True
    assert events == ["started", "success"]


def test_run_receipt_emitter_flushes_concurrent_events_without_lifecycle_races() -> None:
    event_count = 16
    ready = threading.Barrier(event_count + 1)
    received: list[tuple[str, str]] = []
    received_lock = threading.Lock()

    class RecordingTransport:
        def post_json(self, path: str, value: dict[str, object]) -> None:
            with received_lock:
                received.append((str(value["event_id"]), str(value["state"])))

    emitter = PublicRunReceiptEmitter("https://registry.example.test")
    emitter.transport = RecordingTransport()

    def emit_event(index: int) -> None:
        event_id = f"event-concurrent-{index}"
        ready.wait()
        emitter.emit(**_run_receipt_payload("started", event_id=event_id))
        emitter.emit(**_run_receipt_payload("success", event_id=event_id))

    with ThreadPoolExecutor(max_workers=event_count) as pool:
        futures = [pool.submit(emit_event, index) for index in range(event_count)]
        ready.wait()
        for future in futures:
            future.result(timeout=2)

    assert emitter.flush(2) is True
    assert len(received) == event_count * 2
    for index in range(event_count):
        event_id = f"event-concurrent-{index}"
        assert [state for observed_id, state in received if observed_id == event_id] == [
            "started",
            "success",
        ]


def test_cross_repository_fixture_is_identical_and_verifiable(tmp_path: Path) -> None:
    server_fixture = Path(__file__).parents[3] / "spl-server" / "tests" / "fixtures" / "public_manifest_v1.json"
    assert FIXTURE.read_bytes() == server_fixture.read_bytes()
    fixture = _fixture()
    bundle = base64.b64decode(fixture.pop("bundle_base64"), validate=True)
    extracted = tmp_path / "extracted"
    manifest = _verify_bundle(fixture, bundle, target_dir=extracted)
    assert manifest["entrypoint"] == "add"
    assert sorted(path.name for path in extracted.iterdir()) == [
        "object.json",
        "object.py",
        "object.yaml",
    ]


@pytest.mark.parametrize(
    "attack",
    ["traversal", "absolute", "symlink", "device", "duplicate", "casefold", "compressed"],
)
def test_archive_adversarial_members_fail_before_extraction(
    attack: str,
    tmp_path: Path,
) -> None:
    if attack == "traversal":
        entries = [(_regular_info("../escape.py"), b"raise SystemExit")]
    elif attack == "absolute":
        entries = [(_regular_info("/tmp/escape.py"), b"raise SystemExit")]
    elif attack == "symlink":
        link = zipfile.ZipInfo("object.yaml", date_time=(1980, 1, 1, 0, 0, 0))
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        entries = [(link, b"/tmp/target")]
    elif attack == "device":
        device = zipfile.ZipInfo("object.yaml", date_time=(1980, 1, 1, 0, 0, 0))
        device.create_system = 3
        device.external_attr = (stat.S_IFCHR | 0o600) << 16
        entries = [(device, b"device")]
    elif attack == "duplicate":
        entries = [
            (_regular_info("object.yaml"), b"one"),
            (_regular_info("object.yaml"), b"two"),
        ]
    elif attack == "casefold":
        entries = [
            (_regular_info("Object.yaml"), b"one"),
            (_regular_info("object.yaml"), b"two"),
        ]
    else:
        compressed = _regular_info("object.yaml")
        compressed.compress_type = zipfile.ZIP_DEFLATED
        entries = [(compressed, b"0" * 10_000)]
    envelope, bundle = _signed_archive(entries)
    with pytest.raises(ClientError) as error:
        _verify_bundle(envelope, bundle, target_dir=tmp_path / "unsafe")
    assert error.value.code == "public_archive_unsafe"
    assert not (tmp_path / "escape.py").exists()


def test_oversized_declared_member_is_rejected_before_archive_read(
    tmp_path: Path,
) -> None:
    fixture = _fixture()
    bundle = base64.b64decode(fixture.pop("bundle_base64"), validate=True)
    manifest = fixture["manifest"]
    assert isinstance(manifest, dict)
    members = manifest["members"]
    assert isinstance(members, list)
    members[0]["size"] = 64 * 1024 * 1024 + 1
    envelope, bundle = _signed_manifest(manifest, bundle)
    with pytest.raises(ClientError) as error:
        _verify_bundle(envelope, bundle, target_dir=tmp_path / "oversized")
    assert error.value.code == "public_archive_unsafe"


def test_bad_signature_and_member_hash_fail_before_extraction(tmp_path: Path) -> None:
    fixture = _fixture()
    bundle = base64.b64decode(fixture.pop("bundle_base64"), validate=True)
    bad_signature = copy.deepcopy(fixture)
    bad_signature["signature"]["value"] = base64.b64encode(b"x" * 64).decode()
    with pytest.raises(ClientError) as signature_error:
        _verify_bundle(bad_signature, bundle, target_dir=tmp_path / "signature")
    assert signature_error.value.code == "public_signature_invalid"

    bad_member = copy.deepcopy(fixture)
    bad_member["manifest"]["members"][0]["sha256"] = "0" * 64
    bad_member["manifest_hash"] = hashlib.sha256(_canonical(bad_member["manifest"])).hexdigest()
    private_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    bad_member["signature"]["value"] = base64.b64encode(
        private_key.sign(f"{bad_member['manifest_hash']}:{bad_member['bundle_hash']}".encode())
    ).decode()
    with pytest.raises(ClientError) as member_error:
        _verify_bundle(bad_member, bundle, target_dir=tmp_path / "member")
    assert member_error.value.code == "public_archive_disagreement"


def test_concurrent_install_and_corrupt_cache_recovery(tmp_path: Path) -> None:
    fixture = _fixture()
    bundle = base64.b64decode(fixture.pop("bundle_base64"), validate=True)
    backend = EmbeddedBackend(REGISTRY, tmp_path, trusted_keys=TRUSTED_KEYS)
    corrupt = tmp_path / "bundles" / fixture["bundle_hash"]
    corrupt.mkdir(parents=True)
    (corrupt / "bundle.zip").write_bytes(b"partial")

    with ThreadPoolExecutor(max_workers=4) as executor:
        paths = list(executor.map(lambda _: backend._install_bundle(fixture, bundle), range(4)))
    assert len(set(paths)) == 1
    _verify_bundle(
        json.loads((paths[0] / "manifest.json").read_text(encoding="utf-8")),
        (paths[0] / "bundle.zip").read_bytes(),
    )
    assert not list((tmp_path / "bundles").glob(".*-*"))


def test_interrupted_environment_build_recovers_atomically(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from spl import embedded as embedded_module

    backend = EmbeddedBackend("https://registry.example.test", tmp_path)
    lock = {**_test_runtime_lock(), "names": ["root"]}
    projection = embedded_module._installed_framework_projection()
    digest = environment_identity(
        lock,
        interpreter_abi=getattr(sys.implementation, "cache_tag", "unknown"),
        framework_identity=projection[0],
    )
    incomplete = tmp_path / "environments" / digest
    incomplete.mkdir(parents=True)
    (incomplete / "partial").write_text("interrupted", encoding="utf-8")

    interrupted = True
    write_environment_tree = embedded_module._write_environment_tree

    def write(directory: Path, files: object) -> None:
        nonlocal interrupted
        if interrupted:
            interrupted = False
            (directory / "partial").write_text("interrupted", encoding="utf-8")
            raise OSError("private materialization path")
        write_environment_tree(directory, files)

    monkeypatch.setattr(embedded_module, "_write_environment_tree", write)
    monkeypatch.setattr(
        backend,
        "_environment_healthy",
        lambda python: python.is_file(),
    )
    with pytest.raises(ClientError) as first_error:
        backend._ensure_environment(lock)
    assert first_error.value.code == "embedded_environment_build_failed"
    assert first_error.value.__cause__ is None
    assert "private materialization path" not in str(first_error.value)
    assert "Traceback" not in str(first_error.value)
    assert not incomplete.exists()
    assert not list((tmp_path / "environments").glob(".*-*"))

    with ThreadPoolExecutor(max_workers=4) as executor:
        pythons = list(executor.map(lambda _: backend._ensure_environment(lock), range(4)))
    assert len(set(pythons)) == 1
    assert pythons[0].is_file()
    assert (incomplete / "ready.json").is_file()


def _empty_environment(tmp_path: Path) -> tuple[EmbeddedBackend, dict[str, object], Path]:
    backend = EmbeddedBackend("https://registry.example.test", tmp_path, run_receipts=False)
    lock = {**_test_runtime_lock(), "names": ["root"]}
    python = backend._ensure_environment(lock)
    return backend, lock, python


def _make_environment_mutable(environment: Path) -> None:
    for path in [environment, *environment.rglob("*")]:
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(0o755)
        elif path.is_file():
            path.chmod(0o644)


def _reseal_environment(environment: Path) -> None:
    for path in environment.rglob("*"):
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(0o555)
        elif path.is_file():
            path.chmod(0o555 if path == environment / "bin" / "python" else 0o444)
    environment.chmod(0o555)


def _external_health_executable(path: Path, canary: Path) -> None:
    path.write_text(
        f"#!/bin/sh\nprintf ran > {canary!s}\nexit 0\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


@pytest.mark.parametrize(
    "case",
    [
        "absolute-python",
        "traversal-python",
        "sibling-python",
        "malformed",
        "oversized",
        "duplicate-key",
        "unknown-field",
        "missing-field",
        "wrong-schema",
        "wrong-schema-version",
        "wrong-identity",
    ],
)
def test_environment_marker_is_non_authoritative_and_closed(tmp_path: Path, case: str) -> None:
    backend, lock, python = _empty_environment(tmp_path / "cache")
    environment = python.parent.parent
    marker = environment / "ready.json"
    external = tmp_path / "external-health"
    canary = tmp_path / "external-ran"
    _external_health_executable(external, canary)
    original = json.loads(marker.read_text(encoding="utf-8"))
    _make_environment_mutable(environment)
    if case == "absolute-python":
        original["python"] = str(external)
        body = _canonical(original)
    elif case == "traversal-python":
        original["python"] = "../external-health"
        body = _canonical(original)
    elif case == "sibling-python":
        original["python"] = f"../{'f' * 64}/bin/python"
        body = _canonical(original)
    elif case == "malformed":
        body = b'{"schema":'
    elif case == "oversized":
        body = b"x" * (64 * 1024 + 1)
    elif case == "duplicate-key":
        body = b'{"schema":"first","schema":"second"}'
    elif case == "unknown-field":
        original["unexpected"] = True
        body = _canonical(original)
    elif case == "missing-field":
        original.pop("lock_hash")
        body = _canonical(original)
    elif case == "wrong-schema":
        original["schema"] = "spl.public_environment_cache.v0"
        body = _canonical(original)
    elif case == "wrong-schema-version":
        original["schema_version"] = "1"
        body = _canonical(original)
    else:
        original["identity"] = "0" * 64
        body = _canonical(original)
    marker.write_bytes(body)
    marker.chmod(0o444)
    _reseal_environment(environment)

    selected = backend._ensure_environment(lock)

    assert selected == environment / "bin" / "python"
    assert selected != external
    assert not canary.exists()
    repaired = json.loads((environment / "ready.json").read_text(encoding="utf-8"))
    assert repaired["python"] == "bin/python"
    assert repaired["identity"] == environment.name


def test_environment_marker_symlink_never_reads_or_executes_external_target(tmp_path: Path) -> None:
    backend, lock, python = _empty_environment(tmp_path / "cache")
    environment = python.parent.parent
    marker = environment / "ready.json"
    external = tmp_path / "private-marker-payload"
    canary = tmp_path / "external-ran"
    executable = tmp_path / "external-health"
    _external_health_executable(executable, canary)
    external.write_text(
        json.dumps({"python": str(executable), "private": str(tmp_path / "credential")}),
        encoding="utf-8",
    )
    _make_environment_mutable(environment)
    marker.unlink()
    marker.symlink_to(external)
    _reseal_environment(environment)

    selected = backend._ensure_environment(lock)

    assert selected == environment / "bin" / "python"
    assert not canary.exists()
    assert external.is_file()
    assert "private" not in (environment / "ready.json").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "case",
    ["symlink", "directory", "different-executable", "external-script", "hard-link"],
)
def test_canonical_environment_interpreter_is_verified_before_execution(tmp_path: Path, case: str) -> None:
    backend, lock, python = _empty_environment(tmp_path / "cache")
    environment = python.parent.parent
    canary = tmp_path / "tampered-interpreter-ran"
    external = tmp_path / "external-health"
    _external_health_executable(external, canary)
    _make_environment_mutable(environment)
    python.unlink()
    if case == "symlink":
        python.symlink_to(external)
    elif case == "directory":
        python.mkdir()
    elif case == "different-executable":
        alternate = shutil.which("true")
        assert alternate is not None
        python.write_bytes(Path(alternate).read_bytes())
        python.chmod(0o555)
    elif case == "external-script":
        python.write_bytes(external.read_bytes())
        python.chmod(0o555)
    else:
        os.link(external, python)
    _reseal_environment(environment)

    selected = backend._ensure_environment(lock)

    assert selected == environment / "bin" / "python"
    assert not selected.is_symlink()
    assert selected.read_bytes() == Path(sys.executable).resolve().read_bytes()
    assert not canary.exists()


@pytest.mark.parametrize(
    "case",
    [
        "worker",
        "other-module",
        "add",
        "remove",
        "rename",
        "truncate",
        "symlink",
        "hard-link",
        "metadata",
        "direct-url",
    ],
)
def test_framework_projection_is_rederived_before_cached_code_runs(tmp_path: Path, case: str) -> None:
    backend, lock, python = _empty_environment(tmp_path / "cache")
    environment = python.parent.parent
    site_packages = environment / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    worker = site_packages / "spl" / "daemon" / "worker.py"
    other = site_packages / "spl" / "adapters.py"
    metadata = next(site_packages.glob("splime-*.dist-info/METADATA"))
    canary = tmp_path / "tampered-framework-ran"
    _make_environment_mutable(environment)
    if case == "worker":
        worker.write_text(
            worker.read_text(encoding="utf-8") + f"\nPath({str(canary)!r}).write_text('ran', encoding='utf-8')\n",
            encoding="utf-8",
        )
    elif case == "other-module":
        other.write_text(
            other.read_text(encoding="utf-8")
            + f"\n__import__('pathlib').Path({str(canary)!r}).write_text('ran', encoding='utf-8')\n",
            encoding="utf-8",
        )
    elif case == "add":
        (site_packages / "spl" / "undeclared.py").write_text("value = 1\n", encoding="utf-8")
    elif case == "remove":
        other.unlink()
    elif case == "rename":
        other.rename(other.with_name("renamed.py"))
    elif case == "truncate":
        other.write_bytes(b"")
    elif case == "symlink":
        other.unlink()
        other.symlink_to(worker)
    elif case == "hard-link":
        external = tmp_path / "framework-hard-link"
        external.write_bytes(other.read_bytes())
        other.unlink()
        os.link(external, other)
    elif case == "metadata":
        metadata.write_bytes(metadata.read_bytes() + b"X-Tampered: true\n")
    else:
        (metadata.parent / "direct_url.json").write_text(
            json.dumps({"url": str(tmp_path / "private-checkout")}),
            encoding="utf-8",
        )
    _reseal_environment(environment)

    selected = backend._ensure_environment(lock)

    assert selected == environment / "bin" / "python"
    assert not canary.exists()
    assert not (metadata.parent / "direct_url.json").exists()
    assert not (site_packages / "spl" / "undeclared.py").exists()


@pytest.mark.parametrize(
    "relative",
    [
        "sitecustomize.py",
        "sitecustomize/__init__.py",
        "usercustomize.py",
        "usercustomize/__init__.py",
        "startup.pth",
        "nested/startup.pth",
    ],
)
def test_cached_startup_hooks_are_rejected_without_executing_canaries(tmp_path: Path, relative: str) -> None:
    backend, lock, python = _empty_environment(tmp_path / "cache")
    environment = python.parent.parent
    site_packages = environment / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    canary = tmp_path / "startup-hook-ran"
    target = site_packages / relative
    _make_environment_mutable(environment)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.suffix == ".pth":
        target.write_text(
            f"import pathlib; pathlib.Path({str(canary)!r}).write_text('ran', encoding='utf-8')\n",
            encoding="utf-8",
        )
    else:
        target.write_text(
            f"from pathlib import Path\nPath({str(canary)!r}).write_text('ran', encoding='utf-8')\n",
            encoding="utf-8",
        )
    _reseal_environment(environment)

    selected = backend._ensure_environment(lock)

    assert selected == environment / "bin" / "python"
    assert not canary.exists()
    assert not target.exists()


def test_coordinated_environment_and_marker_tampering_cannot_redefine_authority(tmp_path: Path) -> None:
    backend, lock, python = _empty_environment(tmp_path / "cache")
    environment = python.parent.parent
    site_packages = environment / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    worker = site_packages / "spl" / "daemon" / "worker.py"
    marker = environment / "ready.json"
    canary = tmp_path / "coordinated-tamper-ran"
    _make_environment_mutable(environment)
    worker.write_text(
        worker.read_text(encoding="utf-8") + f"\nPath({str(canary)!r}).write_text('ran', encoding='utf-8')\n",
        encoding="utf-8",
    )
    value = json.loads(marker.read_text(encoding="utf-8"))
    value["framework"]["package_sha256"] = hashlib.sha256(worker.read_bytes()).hexdigest()
    value["identity"] = environment.name
    marker.write_bytes(_canonical(value))
    _reseal_environment(environment)

    assert backend._ensure_environment(lock) == environment / "bin" / "python"
    assert not canary.exists()
    assert hashlib.sha256(worker.read_bytes()).hexdigest() != value["framework"]["package_sha256"]


def test_coordinated_interpreter_and_marker_substitution_never_executes_external_code(tmp_path: Path) -> None:
    backend, lock, python = _empty_environment(tmp_path / "cache")
    environment = python.parent.parent
    marker = environment / "ready.json"
    external = tmp_path / "external-interpreter"
    canary = tmp_path / "coordinated-interpreter-ran"
    _external_health_executable(external, canary)
    _make_environment_mutable(environment)
    python.unlink()
    python.write_bytes(external.read_bytes())
    python.chmod(0o555)
    value = json.loads(marker.read_text(encoding="utf-8"))
    value["python"] = str(external)
    marker.write_bytes(_canonical(value))
    _reseal_environment(environment)

    selected = backend._ensure_environment(lock)

    assert selected == environment / "bin" / "python"
    assert selected.read_bytes() == Path(sys.executable).resolve().read_bytes()
    assert json.loads(marker.read_text(encoding="utf-8"))["python"] == "bin/python"
    assert not canary.exists()


def test_marker_copy_and_digest_directory_substitution_rebuild_exact_environment(tmp_path: Path) -> None:
    backend = EmbeddedBackend("https://registry.example.test", tmp_path / "cache", run_receipts=False)
    first_lock = {**_test_runtime_lock(python="3.13"), "names": ["root"]}
    second_lock = {**_test_runtime_lock(python=platform.python_version()), "names": ["root"]}
    first_python = backend._ensure_environment(first_lock)
    second_python = backend._ensure_environment(second_lock)
    first_environment = first_python.parent.parent
    second_environment = second_python.parent.parent
    assert first_environment != second_environment
    _make_environment_mutable(first_environment)
    (first_environment / "ready.json").write_bytes((second_environment / "ready.json").read_bytes())
    _reseal_environment(first_environment)

    assert backend._ensure_environment(first_lock) == first_environment / "bin" / "python"
    repaired = json.loads((first_environment / "ready.json").read_text(encoding="utf-8"))
    assert repaired["identity"] == first_environment.name

    from spl import embedded as embedded_module

    embedded_module._remove_cache_path(first_environment)
    first_environment.symlink_to(second_environment, target_is_directory=True)
    selected = backend._ensure_environment(first_lock)
    assert selected == first_environment / "bin" / "python"
    assert not first_environment.is_symlink()
    assert second_python.is_file()


def test_cache_cleanup_skips_an_entry_with_an_active_use_lock(tmp_path: Path) -> None:
    from spl import embedded as embedded_module

    backend = EmbeddedBackend("https://registry.example.test", tmp_path)
    held = "1" * 64
    removable = "2" * 64
    (tmp_path / "bundles" / held).mkdir(parents=True)
    (tmp_path / "bundles" / removable).mkdir(parents=True)
    with embedded_module._hash_lock(tmp_path, f"bundle-{held}"):
        backend._cleanup_cache_directory(
            "bundles",
            maximum=1,
            lock_prefix="bundle-",
            preserve=set(),
        )
    assert (tmp_path / "bundles" / held).is_dir()
    assert not (tmp_path / "bundles" / removable).exists()


def test_interrupted_bundle_download_leaves_no_resolution_or_partial_cache(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = SPLClient.embedded("https://registry.example.test", tmp_path)
    backend = client._embedded_backend
    assert isinstance(backend, EmbeddedBackend)
    fixture = _fixture()

    def document(path: str) -> dict[str, object]:
        if path == "/version":
            return {"declared": {"contracts": {"daemon_server_capabilities": ["spl.public_distribution.v1"]}}}
        if path.endswith("/releases/1"):
            return {
                "release": {
                    "id": fixture["release_id"],
                    "version": 1,
                    "version_id": fixture["manifest"]["version_id"],
                    "manifest_hash": fixture["manifest_hash"],
                    "bundle_hash": fixture["bundle_hash"],
                    "manifest_url": "/fixture/manifest",
                    "bundle_url": "/fixture/bundle",
                }
            }
        assert path == "/fixture/manifest"
        return fixture

    interrupted = ClientError(
        "public_registry_incomplete: transfer interrupted",
        payload={"code": "public_registry_incomplete"},
    )
    monkeypatch.setattr(backend.transport, "json", document)
    monkeypatch.setattr(
        backend.transport,
        "bytes",
        lambda path, accept: (_ for _ in ()).throw(interrupted),
    )
    with pytest.raises(ClientError) as error:
        client.call("splime://@alice/default/add@1", trust=True, progress=False)
    assert error.value.code == "public_registry_incomplete"
    assert not (tmp_path / "resolutions").exists()
    assert not (tmp_path / "bundles").exists()


def test_exact_cached_function_runs_offline_with_trust_and_no_daemon_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture = _fixture()
    bundle = base64.b64decode(fixture.pop("bundle_base64"), validate=True)
    client = SPLClient.embedded(
        registry_url=REGISTRY,
        cache_dir=tmp_path,
        trusted_keys=TRUSTED_KEYS,
    )
    backend = client._embedded_backend
    assert backend is not None
    backend._install_bundle(fixture, bundle)
    reference = "splime://@alice/default/add@1"
    resolved = {
        "schema": "spl.public_resolution.v1",
        "registry_url": "https://registry.example.test",
        "reference": reference,
        "release": {
            "id": fixture["release_id"],
            "bundle_hash": fixture["bundle_hash"],
            "manifest_hash": fixture["manifest_hash"],
            "version": 1,
            "version_id": fixture["manifest"]["version_id"],
        },
    }
    resolution = backend._resolution_path(parse_public_ref(reference))
    resolution.parent.mkdir(parents=True)
    resolution.write_bytes(_canonical(resolved))
    monkeypatch.setattr(
        backend,
        "_ensure_environment_locked",
        lambda manifest, **kwargs: Path(sys.executable),
    )
    monkeypatch.setenv("SPL_DAEMON_TOKEN", "must-not-enter-worker")
    from spl import embedded as embedded_module

    real_run_process_tree = embedded_module.run_process_tree

    def inspected_worker(*args: object, **kwargs: object) -> object:
        command = args[0]
        assert isinstance(command, list)
        assert "spl.daemon.worker" in command
        assert "splime_public_worker" not in command
        environment = kwargs.get("env")
        assert isinstance(environment, dict)
        assert "PYTHONPATH" not in environment
        assert "SPL_DAEMON_TOKEN" not in environment
        assert not any("TOKEN" in key or "SECRET" in key for key in environment)
        return real_run_process_tree(*args, **kwargs)

    monkeypatch.setattr(embedded_module, "run_process_tree", inspected_worker)
    monkeypatch.setattr(
        backend.transport,
        "json",
        lambda path: (_ for _ in ()).throw(AssertionError("offline exact release used network")),
    )
    with pytest.raises(ClientError) as trust_error:
        client.call(reference, args=[2, 3], progress=False)
    assert trust_error.value.code == "public_trust_required"
    result = client.call(reference, args=[2, 3], trust=True, progress=False)
    assert result.mode == "embedded"
    assert result.output == 5
    assert result.run["release_id"] == fixture["release_id"]


def test_unversioned_reference_never_uses_cached_latest_offline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = SPLClient.embedded(
        registry_url="https://registry.example.test",
        cache_dir=tmp_path,
    )
    backend = client._embedded_backend
    assert backend is not None
    unavailable = ClientError(
        "public_registry_unavailable",
        payload={"code": "public_registry_unavailable"},
    )
    monkeypatch.setattr(
        backend.transport,
        "json",
        lambda path: (_ for _ in ()).throw(unavailable),
    )
    with pytest.raises(ClientError) as error:
        client.call("splime://@alice/default/add", trust=True, progress=False)
    assert error.value.code == "public_registry_unavailable"


def test_runtime_locks_create_one_environment_per_distinct_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    backend = EmbeddedBackend("https://registry.example.test", tmp_path)
    root = {**_test_runtime_lock(), "names": ["root"]}
    root_alias = {**root, "names": ["worker-a"]}
    other = {
        **_test_runtime_lock(
            artifacts=[_test_artifact("certifi", "2026.8.3")],
        ),
        "names": ["worker-b"],
    }
    built: list[str] = []

    def ensure(lock: dict[str, object], **kwargs: object) -> Path:
        built.append(str(lock["lock_hash"]))
        return tmp_path / str(lock["lock_hash"])

    monkeypatch.setattr(backend, "_ensure_environment", ensure)
    environments = backend._ensure_environments({"runtime_locks": [root, root_alias, other]})
    assert len(built) == 2
    assert environments["root"] == environments["worker-a"]
    assert environments["worker-b"] != environments["root"]


def test_worker_selects_alias_specific_embedded_pipeline_environment(
    tmp_path: Path,
) -> None:
    default_python = tmp_path / "root" / "bin" / "python"
    special_python = tmp_path / "special" / "bin" / "python"
    provider = WorkerNodeEnvironmentProvider(
        {
            "venv-subprocess": {
                "default": {
                    "python_path": str(default_python),
                    "lock_hash": "1" * 64,
                },
                "heavy": {
                    "python_path": str(special_python),
                    "lock_hash": "2" * 64,
                },
            }
        }
    )
    spec = {"node_runtime": "venv-subprocess", "distributions": []}
    default = provider.prepare_for_node(spec, node_label="ordinary")
    special = provider.prepare_for_node(spec, node_label="heavy")
    assert default.python_path == default_python
    assert special.python_path == special_python
    assert default.metadata["spec_hash"] != special.metadata["spec_hash"]


def test_trust_is_bound_to_each_exact_content_hash(tmp_path: Path) -> None:
    backend = EmbeddedBackend("https://registry.example.test", tmp_path)
    first = "1" * 64
    second = "2" * 64
    backend._approve(first)
    assert backend._trusted(first)
    assert not backend._trusted(second)


def test_old_server_capability_fails_before_public_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = SPLClient.embedded("https://registry.example.test", tmp_path)
    backend = client._embedded_backend
    assert isinstance(backend, EmbeddedBackend)
    requested: list[str] = []

    def old_server(path: str) -> dict[str, object]:
        requested.append(path)
        return {"contracts": {"daemon_server_capabilities": []}}

    monkeypatch.setattr(backend.transport, "json", old_server)
    with pytest.raises(ClientError) as error:
        client.call("splime://@alice/default/add@1", trust=True, progress=False)
    assert error.value.code == "feature_not_supported"
    assert requested == ["/version"]


def test_nested_node_remote_is_rejected_before_trust_or_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    entries = [
        (
            _regular_info("object.yaml"),
            b"- !DPipeline\n  name: add\n  nodes:\n  - !DNodeRemote\n    uuid: "
            b"11111111-1111-4111-8111-111111111111\n    url: https://private.example\n"
            b"    name: remote\n    version: 1\n  links: []\n  aliases: []\n",
        ),
        (_regular_info("object.json"), b"{}"),
    ]
    envelope, bundle = _signed_archive(entries)
    client = SPLClient.embedded("https://registry.example.test", tmp_path)
    backend = _cache_release(client, "splime://@alice/default/add@1", envelope, bundle)
    monkeypatch.setattr(
        backend,
        "_ensure_environments",
        lambda manifest: (_ for _ in ()).throw(AssertionError("environment was built")),
    )
    with pytest.raises(ClientError) as error:
        client.call("splime://@alice/default/add@1", trust=True, progress=False)
    assert error.value.code == "node_remote_not_allowed"


def test_exact_pipeline_and_artifact_function_run_from_cached_bundles(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pipeline_yaml = b"""- !DPipeline
  name: public_pipeline
  nodes:
  - !DNodeFunction
    uuid: 11111111-1111-4111-8111-111111111111
    func: seed
  - !DNodeFunction
    uuid: 22222222-2222-4222-8222-222222222222
    func: increment
  links:
  - - !DNodeInputRef
      uuid: 22222222-2222-4222-8222-222222222222
      port: value
    - !DNodeOutputRef
      uuid: 11111111-1111-4111-8111-111111111111
      port: default
  aliases:
  - - total
    - 22222222-2222-4222-8222-222222222222
  tags:
    22222222-2222-4222-8222-222222222222:
      runtime: venv-subprocess
---
- !DFunction
  name: seed
  inputs: []
  outputs:
  - name: default
    type: int
  body: |-
    return 4
---
- !DFunction
  name: increment
  inputs:
  - name: value
    type: int
  outputs:
  - name: default
    type: int
  body: |-
    return value + 1
"""
    entries = [
        (_regular_info("object.yaml"), pipeline_yaml),
        (_regular_info("object.json"), b"{}"),
        (_regular_info("object.py"), _compile_yaml_bytes(pipeline_yaml)),
    ]
    pipeline_envelope, pipeline_bundle = _signed_archive(entries)
    pipeline_manifest = pipeline_envelope["manifest"]
    assert isinstance(pipeline_manifest, dict)
    pipeline_manifest.update(
        {
            "entrypoint": "public_pipeline",
            "kind": "pipeline",
            "version_id": "version-fixed-pipeline-1",
            "identity": {
                "entry_id": "object-fixed-pipeline",
                "entry_type": "object",
                "library": "default",
                "name": "public-pipeline",
                "owner": "@alice",
                "owner_id": "author-fixed",
                "uri": "splime://@alice/default/public-pipeline",
            },
        }
    )
    pipeline_envelope, pipeline_bundle = _signed_manifest(pipeline_manifest, pipeline_bundle)
    client = SPLClient.embedded("https://registry.example.test", tmp_path / "pipeline")
    backend = _cache_release(
        client,
        "splime://@alice/default/public-pipeline@1",
        pipeline_envelope,
        pipeline_bundle,
    )
    monkeypatch.setattr(
        backend,
        "_ensure_environment",
        lambda lock, **kwargs: Path(sys.executable),
    )
    pipeline_result = client.call(
        "splime://@alice/default/public-pipeline@1",
        trust=True,
        progress=False,
    )
    assert pipeline_result.mode == "embedded"
    assert pipeline_result.output == {"default": 5}

    artifact_yaml = b"""- !DFunction
  name: make_artifact
  inputs:
  - name: text
    type: str
  outputs:
  - name: default
    type: dict
  body: |-
    from pathlib import Path
    Path("artifact.txt").write_text(text, encoding="utf-8")
    return {
        "__spl_result__": {"length": len(text)},
        "__spl_artifacts__": {"artifact.txt": "artifact.txt"},
    }
"""
    artifact_entries = [
        (_regular_info("object.yaml"), artifact_yaml),
        (_regular_info("object.json"), b"{}"),
        (_regular_info("object.py"), _compile_yaml_bytes(artifact_yaml)),
    ]
    artifact_envelope, artifact_bundle = _signed_archive(artifact_entries)
    artifact_manifest = artifact_envelope["manifest"]
    assert isinstance(artifact_manifest, dict)
    artifact_manifest.update(
        {
            "entrypoint": "make_artifact",
            "version_id": "version-fixed-artifact-1",
            "identity": {
                "entry_id": "object-fixed-artifact",
                "entry_type": "object",
                "library": "default",
                "name": "make-artifact",
                "owner": "@alice",
                "owner_id": "author-fixed",
                "uri": "splime://@alice/default/make-artifact",
            },
        }
    )
    artifact_envelope, artifact_bundle = _signed_manifest(artifact_manifest, artifact_bundle)
    artifact_client = SPLClient.embedded("https://registry.example.test", tmp_path / "artifact")
    artifact_backend = _cache_release(
        artifact_client,
        "splime://@alice/default/make-artifact@1",
        artifact_envelope,
        artifact_bundle,
    )
    monkeypatch.setattr(
        artifact_backend,
        "_ensure_environment",
        lambda lock, **kwargs: Path(sys.executable),
    )
    destination = tmp_path / "downloaded"
    artifact_result = artifact_client.call(
        "splime://@alice/default/make-artifact@1",
        kwargs={"text": "embedded artifact"},
        artifacts_dir=destination,
        trust=True,
        progress=False,
    )
    assert artifact_result.value == {"length": 17}
    assert artifact_result.downloaded_artifacts["artifact.txt"].read_text() == ("embedded artifact")
