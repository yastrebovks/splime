"""Cross-repository anonymous distribution and daemon-free execution gate.

Run with both source trees on ``PYTHONPATH``.  The central Quart application is
real and serves the production public routes over a loopback socket; only the
environment build is replaced with the already-compatible test interpreter.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import socket
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from hypercorn.asyncio import serve as hypercorn_serve
from hypercorn.config import Config

from daemon_server.app import create_app
from daemon_server.store import ServerStore
from spl import SPLClient
from spl.embedded import EmbeddedBackend


FUNCTION_YAML = """- !DFunction
  name: multiply
  inputs:
  - name: left
    type: int
  - name: right
    type: int
  outputs:
  - name: default
    type: int
  body: |-
    return left * right
"""

PIPELINE_YAML = """- !DPipeline
  name: multi_runtime
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
    11111111-1111-4111-8111-111111111111:
      runtime: venv-subprocess
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


def test_real_public_api_download_call_and_offline_exact_cache(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    packages_before = sorted(
        (distribution.metadata["Name"], distribution.version)
        for distribution in importlib.metadata.distributions()
        if distribution.metadata["Name"]
    )
    store = ServerStore(tmp_path / "server")
    store.create_user(
        user_id="author-user",
        display_name="Alice",
        email="alice@example.test",
    )
    store.claim_user_handle("author-user", "alice")
    store.publish_object(
        owner_id="author-user",
        name="multiply",
        entrypoint="multiply",
        yaml_text=FUNCTION_YAML,
        description="Multiply two integers.",
        kind="function",
        metadata={"kind": "function", "entrypoint": "multiply"},
        distributions=[],
        runtime_config={"mode": "venv"},
        library="default",
        content_hash=hashlib.sha256(FUNCTION_YAML.encode()).hexdigest(),
    )
    store.publication_service.update_object(
        owner_id="author-user",
        name_or_id="multiply",
        library="default",
        public=True,
        expected_revision=0,
    )
    release = store._conn.execute(
        """
        SELECT signing_key_id, signing_public_key
        FROM public_releases
        WHERE entry_type = 'object'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    assert release is not None
    port = _free_loopback_port()
    stop, thread, errors = _serve(create_app(store), port)

    def no_daemon(*args: object, **kwargs: object) -> None:
        raise AssertionError("embedded client constructed a daemon transport")

    monkeypatch.setattr("spl._client.Client.__init__", no_daemon)
    registry_url = f"http://127.0.0.1:{port}"
    client = SPLClient.embedded(
        registry_url,
        tmp_path / "public-cache",
        trusted_keys={
            registry_url: {
                str(release["signing_key_id"]): str(release["signing_public_key"]),
            }
        },
    )
    backend = client._embedded_backend
    assert isinstance(backend, EmbeddedBackend)
    reference = "splime://@alice/default/multiply@1"
    try:
        result = client.call(
            reference,
            args=[6, 7],
            trust=True,
            progress=False,
        )
        assert result.mode == "embedded"
        assert result.output == 42
    finally:
        stop.set()
        thread.join(timeout=10)
        store.close()
    assert not thread.is_alive()
    assert errors == []

    monkeypatch.setattr(
        backend.transport,
        "json",
        lambda path: (_ for _ in ()).throw(AssertionError("offline exact call used network")),
    )
    offline = client.call(reference, args=[8, 9], progress=False)
    assert offline.output == 72
    assert not (tmp_path / "public-cache" / "daemon").exists()
    packages_after = sorted(
        (distribution.metadata["Name"], distribution.version)
        for distribution in importlib.metadata.distributions()
        if distribution.metadata["Name"]
    )
    assert packages_after == packages_before


def test_real_producer_multi_runtime_pipeline_runs_online_then_offline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store = ServerStore(tmp_path / "server")
    store.create_user(
        user_id="author-user",
        display_name="Alice",
        email="alice@example.test",
    )
    store.claim_user_handle("author-user", "alice")
    store.publish_object(
        owner_id="author-user",
        name="multi-runtime",
        entrypoint="multi_runtime",
        yaml_text=PIPELINE_YAML,
        description="Two exact runtime assignments.",
        kind="pipeline",
        metadata={
            "kind": "pipeline",
            "entrypoint": "multi_runtime",
            "pipeline_nodes": [
                {
                    "id": "seed",
                    "name": "seed",
                    "runtime_lock": {"python": "3.13", "dependencies": []},
                },
                {
                    "id": "increment",
                    "name": "increment",
                    "runtime_lock": {"python": "3.13.14", "dependencies": []},
                },
            ],
        },
        distributions=[],
        runtime_config={"mode": "venv"},
        library="default",
        content_hash=hashlib.sha256(PIPELINE_YAML.encode()).hexdigest(),
    )
    active = store.publication_service.update_object(
        owner_id="author-user",
        name_or_id="multi-runtime",
        library="default",
        public=True,
        expected_revision=0,
    )
    locks = active["release"]["manifest"]["runtime_locks"]
    assert len(locks) == 2
    assert {name for lock in locks for name in lock["names"]} == {
        "root",
        "seed",
        "increment",
    }
    release = store._conn.execute(
        """
        SELECT signing_key_id, signing_public_key
        FROM public_releases WHERE id = ?
        """,
        (active["active_release_id"],),
    ).fetchone()
    assert release is not None
    port = _free_loopback_port()
    stop, thread, errors = _serve(create_app(store), port)
    registry_url = f"http://127.0.0.1:{port}"
    client = SPLClient.embedded(
        registry_url,
        tmp_path / "public-cache",
        trusted_keys={
            registry_url: {
                str(release["signing_key_id"]): str(release["signing_public_key"]),
            }
        },
    )
    backend = client._embedded_backend
    assert isinstance(backend, EmbeddedBackend)
    reference = "splime://@alice/default/multi-runtime@1"
    try:
        online = client.call(reference, trust=True, progress=False)
        assert online.output == {"default": 5}
        assert len(list((tmp_path / "public-cache" / "environments").iterdir())) == 2
    finally:
        stop.set()
        thread.join(timeout=10)
        store.close()
    assert not thread.is_alive()
    assert errors == []
    monkeypatch.setattr(
        backend.transport,
        "json",
        lambda path: (_ for _ in ()).throw(AssertionError("offline exact multi-runtime call used network")),
    )
    offline = client.call(reference, progress=False)
    assert offline.output == {"default": 5}
    assert len(list((tmp_path / "public-cache" / "environments").iterdir())) == 2


def test_root_embeds_and_executes_verified_component_after_dependency_withdrawal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store = ServerStore(tmp_path / "server")
    store.create_user(user_id="author-user", display_name="Alice", email="alice@example.test")
    store.claim_user_handle("author-user", "alice")
    dependency_yaml = FUNCTION_YAML.replace("multiply", "dependency")
    dependency = store.publish_object(
        owner_id="author-user",
        name="dependency",
        entrypoint="dependency",
        yaml_text=dependency_yaml,
        description="Embedded dependency.",
        kind="function",
        metadata={"kind": "function", "entrypoint": "dependency"},
        distributions=[],
        runtime_config={"mode": "venv"},
        library="default",
        content_hash=hashlib.sha256(dependency_yaml.encode()).hexdigest(),
    )
    dependency_active = store.publication_service.update_object(
        owner_id="author-user",
        name_or_id="dependency",
        library="default",
        public=True,
        expected_revision=0,
    )
    root_yaml = FUNCTION_YAML.replace("multiply", "root")
    store.publish_object(
        owner_id="author-user",
        name="root",
        entrypoint="root",
        yaml_text=root_yaml,
        description="Self-contained root.",
        kind="function",
        metadata={
            "kind": "function",
            "entrypoint": "root",
            "dependency": {
                "object_id": dependency["id"],
                "version_id": dependency["version_id"],
            },
        },
        distributions=[],
        runtime_config={"mode": "venv"},
        library="default",
        content_hash=hashlib.sha256(root_yaml.encode()).hexdigest(),
    )
    root = store.publication_service.update_object(
        owner_id="author-user",
        name_or_id="root",
        library="default",
        public=True,
        expected_revision=0,
    )
    store.publication_service.update_object(
        owner_id="author-user",
        name_or_id="dependency",
        library="default",
        public=False,
        expected_revision=int(dependency_active["revision"]),
    )
    release = store._conn.execute(
        "SELECT signing_key_id, signing_public_key FROM public_releases WHERE id = ?",
        (root["active_release_id"],),
    ).fetchone()
    assert release is not None
    port = _free_loopback_port()
    stop, thread, errors = _serve(create_app(store), port)
    registry_url = f"http://127.0.0.1:{port}"
    client = SPLClient.embedded(
        registry_url,
        tmp_path / "public-cache",
        trusted_keys={
            registry_url: {
                str(release["signing_key_id"]): str(release["signing_public_key"]),
            }
        },
    )
    backend = client._embedded_backend
    assert isinstance(backend, EmbeddedBackend)
    try:
        result = client.call(
            "splime://@alice/default/root@1",
            args=[6, 7],
            trust=True,
            progress=False,
        )
        assert result.output == 42
        bundle_dirs = list((tmp_path / "public-cache" / "bundles").iterdir())
        assert len(bundle_dirs) == 1
        assert list(bundle_dirs[0].glob("components/*/bundle.zip"))
        assert list(bundle_dirs[0].glob("components/*/manifest.json"))
    finally:
        stop.set()
        thread.join(timeout=10)
        store.close()
    assert errors == []


def _free_loopback_port() -> int:
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        return int(reserved.getsockname()[1])


def _serve(
    app: Any,
    port: int,
) -> tuple[threading.Event, threading.Thread, list[BaseException]]:
    stop = threading.Event()
    errors: list[BaseException] = []

    async def shutdown_trigger() -> None:
        while not stop.is_set():
            await asyncio.sleep(0.05)

    async def run_server() -> None:
        config = Config()
        config.bind = [f"127.0.0.1:{port}"]
        config.use_reloader = False
        config.accesslog = None
        config.errorlog = None
        await hypercorn_serve(app, config, shutdown_trigger=shutdown_trigger)

    def target() -> None:
        try:
            asyncio.run(run_server())
        except BaseException as exc:  # pragma: no cover - surfaced below.
            errors.append(exc)

    thread = threading.Thread(target=target, name="public-registry-loopback", daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if errors:
            raise RuntimeError("public registry failed to start") from errors[0]
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return stop, thread, errors
        except OSError:
            time.sleep(0.05)
    stop.set()
    thread.join(timeout=2)
    raise TimeoutError("public registry did not start")
