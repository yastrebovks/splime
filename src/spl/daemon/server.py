"""Quart server for the local SPL daemon.

The daemon remains local-first: by default it binds to ``127.0.0.1`` and exposes
an API that can run arbitrary registered SPL objects in the selected Python
environment.  Do not bind it to an untrusted network.

Endpoints:

    GET  /health
    GET  /envs
    POST /envs
    GET  /objects
    POST /objects
    POST /objects/prepare
    POST /objects/validate
    GET  /objects/<name-or-id>
    GET  /objects/<name-or-id>/versions
    GET  /objects/search?q=<text>
    GET  /environment-builds
    GET  /environment-builds/<spec-hash>
    POST /environment-builds/<spec-hash>/rebuild
    GET  /remote-signatures
    POST /remote-signatures/resolve
    POST /remote-decompositions/resolve
    POST /remote-nodes/run
    POST /server-objects/pull
    GET  /server/connection
    GET  /server/users
    GET  /server/whoami
    GET  /server/sync/status
    POST /server/sync/prune
    GET  /server/libraries
    POST /server/libraries
    GET  /server/libraries/<ref>
    PUT  /server/libraries/<ref>
    DELETE /server/libraries/<ref>  (501 until upstream archive/delete exists)
    GET  /server/libraries/<ref>/grants
    POST /server/libraries/<ref>/grants
    POST /server/libraries/<ref>/grants/<grantee>/revoke
    POST /server/libraries/<ref>/references
    POST /server/libraries/<ref>/copies
    DELETE /server/libraries/<ref>/entries/<name>
    POST /server/connect
    POST /server/disconnect
    GET  /server/ai/preview/capabilities
    POST /server/ai/preview
    GET  /runs
    POST /runs
    POST /runs/local-admissions
    GET  /runs/<id>
    GET  /runs/<id>/result
    POST /runs/<id>/delivery-ack
    GET  /runs/<id>/artifacts
    GET  /runs/<id>/artifacts/<name>
"""

from __future__ import annotations

import base64
import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from http import HTTPStatus
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any, Callable, Literal, cast
from urllib.parse import unquote, urlparse, urlunparse
from uuid import uuid4

import yaml

from spl._process import run_process_tree
from spl._timeout import TimeoutDomain, validate_timeout_seconds
from spl.core import json_contract as m_json_contract
from spl.core import manifest as m_manifest
from spl.core import resume as m_resume
from spl.core.node_runtime import REMOTE_RUN_RUNTIME_OVERRIDES_CAPABILITY
from spl.core.runtime_port_adapters import (
    RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY,
    RUNTIME_PORT_ADAPTERS_CAPABILITY,
    RuntimePortAdapterContractError,
    normalize_adapter_policy,
    normalize_runtime_adapter_semantic_advisories,
    normalize_runtime_output_record,
    normalize_wire_document,
    runtime_adapter_semantic_category,
    runtime_adapter_semantic_state,
)
from spl.core.library_adapters import (
    LIBRARY_ADAPTER_PUBLISH_CAPABILITY,
    MAX_ENVIRONMENT_DISTRIBUTIONS,
    RUNTIME_LIBRARY_ADAPTER_REF_CAPABILITY,
    canonical_package_name,
    environment_fingerprint,
    library_adapter_semantic_advisory,
    normalize_environment_distributions,
    normalize_library_adapter_ref,
    normalize_publish_request,
    normalize_runtime_library_adapter_refs,
)
from spl._runtime_port_adapters_client import server_admission_document
from spl.core.node_runtime import (
    DOCKER_NODE_RUNTIME,
    NODE_RUNTIME_BACKENDS,
    RUNTIME_TAG_NAME,
    explicit_docker_image_spec_hash,
)
from spl.core.entities.pipeline import DPipeline, Pipeline
from spl.core.ir.utils import SPLSafeLoader, spl_import_from_file
from spl.daemon.callback_capability import (
    CALLBACK_CAPABILITY_ENV,
    CallbackCapabilityAuthority,
    callback_capability_ttl_seconds,
)
from spl.daemon.artifact_access import (
    LOCAL_ARTIFACT_SCAN_MAX_ENTRIES,
    ArtifactDirectory,
)
from spl.daemon.browser_adapter_run import BrowserAdapterRunBroker
from spl.daemon.docker_environment import DockerEnvironmentManager, ensure_docker_cli_on_path
from spl.daemon.docker_pool import DockerPool
from spl.daemon.environment import EnvironmentBuildError
from spl.daemon.environment import EnvironmentManager as VenvEnvironmentManager
from spl.daemon.environment_base import (
    ABSENT,
    CREATING,
    FAILED,
    READY,
    EnvironmentManagerProtocol,
)
from spl.daemon.heartbeat_service import HeartbeatService
from spl.daemon.guarded_local_run import (
    GuardedLocalRunAdmission,
    GuardedLocalRunError,
    build_guarded_local_run_receipt,
    retention_to_keep,
    timeout_ms_to_seconds,
    validate_signature_arguments,
)
from spl.daemon.home_lock import DaemonHomeLock, DaemonInstanceIdentity
from spl.daemon.interpreter_visibility import environment_record_interpreter_substitution
from spl.daemon.lifecycle import (
    LifecycleAdmissionClosed,
    LifecycleController,
    LifecycleError,
    LifecycleWorkLease,
    validate_deployed_no_claim_capability,
)
from spl.daemon.lifecycle_startup import (
    StartupBinding,
    StartupBindingError,
    load_startup_binding,
    revalidate_startup_binding,
)
from spl.daemon.library_adapters import library_adapter_sync_payload
from spl.daemon.repositories.server_connection import SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
from spl.daemon.remote_client import (
    RUN_CLAIM_FENCING_CAPABILITY,
    RUN_CLAIM_FENCING_VERSION,
    RUN_CLAIM_PRIVATE_FIELD,
    STALE_RUN_CLAIM_ERROR_CODE,
    ServerClient,
    ServerClientError,
    is_permanent_archived_sync_error,
    is_stale_run_claim_error,
    is_sync_event_identity_collision_error,
)
from spl.daemon.routes._helpers import RouteContext
from spl.daemon.routes.ai_assistant import register_ai_assistant_routes
from spl.daemon.routes.ai_preview import register_ai_preview_routes
from spl.daemon.routes.artifacts import register_artifact_routes
from spl.daemon.routes.adapter_runs import (
    install_adapter_run_request_limit,
    register_adapter_run_routes,
)
from spl.daemon.routes.diagnostics import register_diagnostics_routes
from spl.daemon.routes.envs import register_env_routes
from spl.daemon.routes.guarded_runs import install_guarded_run_request_limit, register_guarded_run_routes
from spl.daemon.routes.libraries import register_library_routes
from spl.daemon.routes.lifecycle import (
    install_lifecycle_request_limit,
    register_lifecycle_routes,
)
from spl.daemon.routes.library_adapters import (
    install_library_adapter_request_limit,
    register_library_adapter_routes,
)
from spl.daemon.routes.meta import register_meta_routes
from spl.daemon.routes.objects import register_object_routes
from spl.daemon.routes.prepared_validation import (
    install_prepared_validation_request_limit,
    register_prepared_validation_routes,
)
from spl.daemon.routes.remote import register_remote_routes
from spl.daemon.routes.runs import register_run_routes
from spl.daemon.routes.server_connections import register_server_connection_routes
from spl.daemon.routes.source_analysis import (
    install_source_analysis_request_limit,
    register_source_analysis_routes,
)
from spl.daemon.runtime_backend import (
    RUNTIME_BACKENDS,
    RunContext,
    RuntimeBackendRegistry,
    RuntimeBackendServices,
)
from spl.daemon.runtime_config import normalize_runtime_config
from spl.daemon.runtime_library_adapters import (
    materialize_claimed_runtime_library_adapters,
)
from spl.daemon.runtime_port_adapters import materialize_claimed_runtime_port_adapters
from spl.daemon.signature import build_signature
from spl.daemon.spl_free_generator import (
    LEGACY_WORKER_RUNTIME,
    read_worker_runtime_marker,
)
from spl.daemon.runtime_dependencies import (
    DockerEnvironmentManagerProtocol,
    DockerPoolRunnerProtocol,
    HeartbeatsProtocol,
    ServerClientFactoryProtocol,
    ServerClientProtocol,
    ServerConnectionsProtocol,
    SyncVisibilityProtocol,
)
from spl.daemon.server_connection import (
    SERVER_OFFLINE_MESSAGE,
    SERVER_PROXY_TIMEOUT_SECONDS,
    SERVER_UNREACHABLE_CODE,
    HandleRequiresServerConnectionError,
    ServerConnectionManager,
    ServerOfflineError,
)
from spl.daemon.services.sync import SyncVisibilityService
from spl.daemon.store import (
    DEFAULT_OBJECT_LIBRARY,
    DEFAULT_OBJECT_OWNER_ID,
    RegistryStore,
    split_object_function_ref,
    utc_now,
    validate_name,
    write_json,
)
from spl.daemon.storage_base import DEFAULT_RUN_DELIVERY_LEASE_SECONDS
from spl.daemon.telemetry import (
    DEFAULT_TELEMETRY_LEVEL,
    SYNC_BATCH_MAX_BYTES,
    SYNC_EVENT_BATCH_LIMIT,
    SYNC_EVENT_MAX_BYTES,
    SYNC_EVENT_PAYLOAD_BUDGET,
    SYNC_EVENT_SCAN_PAGE_LIMIT,
    TelemetryLevel,
    TelemetryPolicy,
    count_local_artifacts_bounded,
    local_run_proof,
)
from spl.daemon_client import (
    DAEMON_API_TOKEN_ENV,
    clear_daemon_endpoint,
    daemon_url,
    generate_daemon_api_token,
    write_daemon_endpoint,
)
from spl.daemon.worker_runtime_marker import (
    WORKER_MANIFEST_HANDOFF_FILE,
    WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE,
    WORKER_RUNTIME_ADAPTER_FAILURE_FILE,
    WORKER_RUNTIME_CUSTOM_ADAPTER_USED_FILE,
    WORKER_RUNTIME_MARKER_FILE,
)

LOCAL_RUN_TEXT_ARTIFACT_MAX_BYTES = 256 * 1024
LOCAL_RUN_TEXT_ARTIFACT_MAX_COUNT = 100
# Leave room for the run envelope, telemetry summary, and redaction expansion.
# The final wire-size fitter remains authoritative.
LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES = min(
    192 * 1024,
    SYNC_EVENT_PAYLOAD_BUDGET,
)
RUN_RETENTION_CLEANUP_RETRY_SECONDS = 30.0
LOCAL_RUN_TEXT_ARTIFACT_EXTENSIONS = {
    ".csv",
    ".htm",
    ".html",
    ".json",
    ".log",
    ".md",
    ".txt",
    ".tsv",
    ".yaml",
    ".yml",
}
DEFAULT_PORT_SCAN_LIMIT = 100
DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES = int(
    os.environ.get("SPL_DAEMON_INLINE_REMOTE_ARTIFACT_MAX_BYTES", str(5 * 1024 * 1024))
)
_RESULT_NOT_PROVIDED = object()
DEFAULT_INLINE_REMOTE_ARTIFACT_TOTAL_MAX_BYTES = int(
    os.environ.get(
        "SPL_DAEMON_INLINE_REMOTE_ARTIFACT_TOTAL_MAX_BYTES",
        str(20 * 1024 * 1024),
    )
)
SERVER_CHANNEL_MIN_LIVENESS_WINDOW_SECONDS = 5.0
SERVER_CHANNEL_LIVENESS_MULTIPLIER = 2.0
SERVER_CHANNEL_FAILURE_THRESHOLD = 2
SERVER_CHANNEL_PROBE_TIMEOUT_SECONDS = 5.0
# A probe GET may use the one allowed retry plus its 0.5-second delay. Keeping
# each transport attempt at two seconds bounds the whole single-flight below
# the five-second probe budget.
SERVER_CHANNEL_PROBE_ATTEMPT_TIMEOUT_SECONDS = 2.0
SERVER_CHANNEL_LEASE_REJECTION_STATUSES = frozenset({401, 403, 404, 409})
SERVER_CHANNEL_LEASE_METHODS = frozenset(
    {
        "connect_machine",
        "current_connection",
        "heartbeat_connection",
        "sync",
    }
)
SYNC_REQUEST_TIMEOUT_SECONDS = 15.0
TELEMETRY_POLICY_CAPABILITY = "spl.telemetry_policy.v1"
WORKER_OPERATIONS_CAPABILITY = "spl.worker_operations.v1"
WORKER_OPERATIONS_SCHEMA_VERSION = 1
EXECUTION_MANIFEST_CAPABILITY = "spl.execution_manifest.v1"
EXECUTION_MANIFEST_CAPABILITY_VERSION = 1
WORKER_BUILD_CAPABILITY = "spl.worker_build.v1"
WORKER_BUILD_SCHEMA_VERSION = 1
LOGGER = logging.getLogger(__name__)

_RUNTIME_ADAPTER_FAILURE_MAX_BYTES = 2048


def _read_worker_runtime_adapter_failure(
    path: Path,
    *,
    returncode: int,
) -> dict[str, Any] | None:
    """Read one closed failure record emitted by the worker adapter wrapper."""

    if returncode != WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE:
        return None
    try:
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > _RUNTIME_ADAPTER_FAILURE_MAX_BYTES
        ):
            return None
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            body = os.read(descriptor, _RUNTIME_ADAPTER_FAILURE_MAX_BYTES + 1)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        current = path.lstat()
    except OSError:
        return None
    identity = (before.st_dev, before.st_ino)
    stable_file = (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
    if not (
        stat.S_ISREG(opened.st_mode)
        and identity == (opened.st_dev, opened.st_ino)
        and identity == (after.st_dev, after.st_ino)
        and identity == (current.st_dev, current.st_ino)
        and stable_file == (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
        and stable_file == (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        and stable_file == (current.st_size, current.st_mtime_ns, current.st_ctime_ns)
        and len(body) == before.st_size
    ):
        return None
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        return None
    stage = payload.get("stage")
    if stage not in {"worker_load", "worker_save"}:
        return None
    kind = payload.get("kind")
    if kind == "stage":
        if set(payload) != {"schema_version", "kind", "stage"}:
            return None
        return {"stage": stage, "reason": "runtime adapter worker stage failed"}
    if kind != "operation" or set(payload) != {
        "schema_version",
        "kind",
        "stage",
        "direction",
        "port",
        "adapter_id",
    }:
        return None
    direction = payload.get("direction")
    if direction != ("input" if stage == "worker_load" else "output"):
        return None
    port = payload.get("port")
    adapter_id = payload.get("adapter_id")
    if (
        not isinstance(port, str)
        or not port
        or len(port) > 256
        or not isinstance(adapter_id, str)
        or not adapter_id
        or len(adapter_id) > 256
    ):
        return None
    input_failure = stage == "worker_load"
    return {
        "schema_version": 1,
        "code": "input_adapter_load_failed" if input_failure else "output_adapter_save_failed",
        "stage": stage,
        "direction": direction,
        "port": port,
        "adapter_id": adapter_id,
        "adapter_ref": None,
        "message": (
            "The selected input Adapter could not load this value."
            if input_failure
            else "The selected output Adapter could not save this value."
        ),
        "retryable": False,
        "fallback_used": False,
    }


def _validated_runtime_adapter_failure(
    failure: dict[str, Any] | None,
    initial_manifest: dict[str, Any],
) -> dict[str, Any] | None:
    """Bind parsed operation evidence to exactly one admitted adapter binding."""

    if failure is None or failure.get("schema_version") != 1:
        return failure
    section = initial_manifest.get("runtime_port_adapters")
    bindings = section.get("bindings") if isinstance(section, dict) else None
    if not isinstance(bindings, list):
        return None
    matches = [
        item
        for item in bindings
        if isinstance(item, dict)
        and item.get("direction") == failure.get("direction")
        and item.get("port") == failure.get("port")
    ]
    if len(matches) != 1:
        return None
    expected_adapter_ids = {matches[0].get("adapter_id")}
    ref_document = initial_manifest.get("runtime_library_adapter_refs")
    if isinstance(ref_document, dict):
        refs = [
            item
            for item in ref_document.get("bindings") or []
            if isinstance(item, dict)
            and item.get("direction") == failure.get("direction")
            and item.get("port") == failure.get("port")
        ]
        if len(refs) > 1:
            return None
        if refs:
            expected_adapter_ids = {
                refs[0].get("adapter_id"),
                f"library:{refs[0].get('content_hash')}",
            }
    if failure.get("adapter_id") not in expected_adapter_ids:
        return None
    return failure


def _manifest_with_runtime_adapter_failure(
    manifest: dict[str, Any],
    initial_manifest: dict[str, Any],
    failure: dict[str, Any],
    *,
    custom_used: bool = False,
) -> dict[str, Any]:
    """Retain admitted adapter evidence and attach one closed failure."""

    initial_section = initial_manifest.get("runtime_port_adapters")
    if not isinstance(initial_section, dict):
        return manifest
    section = _runtime_adapter_section_with_usage(initial_section, custom_used=custom_used)
    closed_failure = dict(failure)
    if closed_failure.get("schema_version") == 1:
        ref_document = initial_manifest.get("runtime_library_adapter_refs")
        if isinstance(ref_document, dict):
            exact_ref = next(
                (
                    item
                    for item in ref_document.get("bindings") or []
                    if isinstance(item, dict)
                    and item.get("direction") == closed_failure.get("direction")
                    and item.get("port") == closed_failure.get("port")
                ),
                None,
            )
            if exact_ref is not None:
                closed_failure["adapter_ref"] = {
                    key: exact_ref[key]
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
    section["failure"] = closed_failure
    result = {**manifest, "runtime_port_adapters": section}
    for key in (
        "runtime_library_adapter_refs",
        "runtime_library_adapter_metadata",
        "runtime_adapter_semantic_advisories",
    ):
        value = initial_manifest.get(key)
        if isinstance(value, dict):
            result[key] = value
    return result


def _manifest_with_runtime_adapter_outputs(
    manifest: dict[str, Any],
    initial_manifest: dict[str, Any],
    runtime_document: dict[str, Any],
    raw_records: Any,
    runtime_library_adapters: dict[str, Any] | None = None,
    *,
    custom_used: bool = False,
) -> dict[str, Any]:
    """Bind terminal output artifacts to admitted descriptor evidence."""

    initial_section = initial_manifest.get("runtime_port_adapters")
    if not isinstance(initial_section, dict):
        raise RuntimeError("runtime adapter manifest evidence is missing")
    document = normalize_wire_document(runtime_document, allow_content=False)
    expected = {
        binding["port"]: binding
        for binding in document["bindings"]
        if binding["direction"] == "output" and binding["transport"] == "artifact"
    }
    library_by_port = {
        str(item.get("port")): item
        for item in (runtime_library_adapters or {}).get("bindings", [])
        if isinstance(item, dict) and item.get("direction") == "output"
    }
    if not isinstance(raw_records, list):
        raise RuntimeError("runtime adapter output metadata is missing")
    records = [normalize_runtime_output_record(record) for record in raw_records]
    by_port = {record["port"]: record for record in records}
    if (
        len(by_port) != len(records)
        or len({record["name"] for record in records}) != len(records)
        or set(by_port) != set(expected)
    ):
        raise RuntimeError("runtime adapter output metadata does not match admitted output ports")
    for port, record in by_port.items():
        binding = expected[port]
        adapter = binding["adapter"]
        library = library_by_port.get(port)
        expected_adapter_id = adapter["id"] if library is None else f"library:{library['content_hash']}"
        expected_format_tag = adapter["format_tag"] if library is None else library.get("format_tag")
        expected_media_type = adapter["presentation"]["media_type"] if library is None else library.get("media_type")
        if (
            record["adapter_id"] != expected_adapter_id
            or record["format_tag"] != expected_format_tag
            or record["media_type"] != expected_media_type
            or record["result_path"] != binding["result_path"]
        ):
            raise RuntimeError("runtime adapter output metadata contradicts its admitted descriptor")

    bindings = []
    for raw_binding in initial_section.get("bindings") or []:
        binding = dict(raw_binding)
        if binding.get("direction") == "output" and binding.get("port") in by_port:
            record = by_port[str(binding["port"])]
            binding.update(
                {
                    "artifact_name": record["name"],
                    "artifact_size": record["size"],
                    "artifact_sha256": record["sha256"],
                    "media_type": record["media_type"],
                    "result_path": record["result_path"],
                }
            )
        bindings.append(binding)
    section = _runtime_adapter_section_with_usage(initial_section, custom_used=custom_used)
    section["bindings"] = bindings
    result = {**manifest, "runtime_port_adapters": section}
    for key in (
        "runtime_library_adapter_refs",
        "runtime_library_adapter_metadata",
        "runtime_adapter_semantic_advisories",
    ):
        value = initial_manifest.get(key)
        if isinstance(value, dict):
            result[key] = value
    return result


def _runtime_adapter_section_with_usage(
    initial_section: dict[str, Any],
    *,
    custom_used: bool,
) -> dict[str, Any]:
    section = dict(initial_section)
    custom = section.get("custom_remote")
    if not isinstance(custom, dict):
        raise RuntimeError("runtime adapter custom execution evidence is missing")
    requested = custom.get("requested") is True
    allowed = custom.get("allowed") is True
    section["custom_remote"] = {
        "requested": requested,
        "allowed": allowed,
        "used": bool(custom_used and requested and allowed),
    }
    return section


def _worker_runtime_custom_adapter_used(run_dir: Path) -> bool:
    marker = run_dir / WORKER_RUNTIME_CUSTOM_ADAPTER_USED_FILE
    try:
        before = marker.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size != len(b"used\n"):
            return False
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(marker, flags)
        try:
            opened = os.fstat(descriptor)
            body = os.read(descriptor, len(b"used\n") + 1)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        current = marker.lstat()
    except OSError:
        return False
    identity = (before.st_dev, before.st_ino)
    return (
        stat.S_ISREG(opened.st_mode)
        and identity == (opened.st_dev, opened.st_ino)
        and identity == (after.st_dev, after.st_ino)
        and identity == (current.st_dev, current.st_ino)
        and body == b"used\n"
    )


def _verify_terminal_runtime_output_files(artifacts_dir: Path, raw_records: Any) -> None:
    """Re-prove canonical output bytes after the isolated worker has exited."""

    if not isinstance(raw_records, list):
        raise RuntimeError("runtime adapter output metadata is missing")
    for raw_record in raw_records:
        record = normalize_runtime_output_record(raw_record)
        path = artifacts_dir / record["name"]
        try:
            before = path.lstat()
            if not stat.S_ISREG(before.st_mode):
                raise RuntimeError("runtime adapter output is not a regular file")
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path, flags)
            try:
                opened = os.fstat(descriptor)
                remaining = record["size"] + 1
                chunks: list[bytes] = []
                while remaining:
                    chunk = os.read(descriptor, min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                after = os.fstat(descriptor)
            finally:
                os.close(descriptor)
            current = path.lstat()
        except OSError:
            raise RuntimeError("runtime adapter output could not be verified after worker exit") from None
        body = b"".join(chunks)
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
            or (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino)
            or len(body) != record["size"]
            or after.st_size != record["size"]
            or current.st_size != record["size"]
            or hashlib.sha256(body).hexdigest() != record["sha256"]
        ):
            raise RuntimeError("runtime adapter output failed terminal size/checksum verification")


EnvironmentManagerFactory = Callable[..., EnvironmentManagerProtocol]
DockerEnvironmentManagerFactory = Callable[..., DockerEnvironmentManagerProtocol]
DockerPoolFactory = Callable[..., DockerPoolRunnerProtocol]
RuntimeBackendRegistryFactory = Callable[
    [RuntimeBackendServices],
    RuntimeBackendRegistry,
]
SyncVisibilityFactory = Callable[[RegistryStore], SyncVisibilityProtocol]
ServerConnectionManagerFactory = Callable[
    [RegistryStore, ServerClientFactoryProtocol],
    ServerConnectionsProtocol,
]
HeartbeatServiceFactory = Callable[
    [RegistryStore, Callable[..., dict[str, Any]]],
    HeartbeatsProtocol,
]
RemoteSignatureCacheUpdate = tuple[dict[str, Any], dict[str, Any], str, str | None]


class _ServerRunSuperseded(RuntimeError):
    """Stop reporting for one remote run attempt rejected by claim fencing."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(f"remote run attempt was superseded: {run_id}")


def _default_environment_manager_factory(
    store: RegistryStore,
    **kwargs: Any,
) -> EnvironmentManagerProtocol:
    return VenvEnvironmentManager(store, **kwargs)


def _default_docker_environment_manager_factory(
    store: RegistryStore,
    **kwargs: Any,
) -> DockerEnvironmentManagerProtocol:
    return DockerEnvironmentManager(store, **kwargs)


def _default_docker_pool_factory(
    store: RegistryStore,
    docker_environment_manager: DockerEnvironmentManagerProtocol,
    *,
    daemon_base_url: str,
    enabled: bool,
    pool_size: int,
    idle_timeout_seconds: float,
    prewarm: bool,
    daemon_identity: DaemonInstanceIdentity | None,
    daemon_home_lock: DaemonHomeLock | None,
) -> DockerPoolRunnerProtocol:
    return DockerPool(
        store,
        docker_environment_manager,
        daemon_base_url=daemon_base_url,
        enabled=enabled,
        pool_size=pool_size,
        idle_timeout_seconds=idle_timeout_seconds,
        prewarm=prewarm,
        identity=daemon_identity,
        startup_cleanup_authority=daemon_home_lock,
    )


def _default_runtime_backend_registry_factory(
    services: RuntimeBackendServices,
) -> RuntimeBackendRegistry:
    return RuntimeBackendRegistry(services)


def _default_sync_visibility_factory(store: RegistryStore) -> SyncVisibilityProtocol:
    return SyncVisibilityService(store)


def _default_server_client_factory(
    base_url: str,
    machine_token: str,
    *,
    user_token: str | None = None,
    request_timeout_seconds: float | None = None,
) -> ServerClientProtocol:
    client_kwargs: dict[str, Any] = {"user_token": user_token}
    if request_timeout_seconds is not None:
        client_kwargs["request_timeout_seconds"] = request_timeout_seconds
    return ServerClient(
        base_url,
        machine_token,
        **client_kwargs,
    )


def _default_server_connection_manager_factory(
    store: RegistryStore,
    server_client_factory: ServerClientFactoryProtocol,
) -> ServerConnectionManager:
    return ServerConnectionManager(store, server_client_factory)


def _default_heartbeat_service_factory(
    store: RegistryStore,
    sync_once: Callable[..., dict[str, Any]],
) -> HeartbeatService:
    return HeartbeatService(store, sync_once)


class _ChannelObservedServerClient:
    """Observe logical server calls without changing their payload contract."""

    def __init__(
        self,
        delegate: ServerClientProtocol,
        *,
        success: Callable[[], None],
        failure: Callable[[ServerClientError, bool], None],
    ) -> None:
        self._delegate = delegate
        self._success = success
        self._failure = failure

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._delegate, name)
        if not callable(target):
            return target

        def observed(*args: Any, **kwargs: Any) -> Any:
            try:
                result = target(*args, **kwargs)
            except ServerClientError as exc:
                if is_stale_run_claim_error(exc):
                    # Claim rejection proves the authenticated channel is live;
                    # it invalidates only this worker attempt, not the lease.
                    self._success()
                    raise
                lease_rejected = (
                    name in SERVER_CHANNEL_LEASE_METHODS and exc.status_code in SERVER_CHANNEL_LEASE_REJECTION_STATUSES
                )
                if lease_rejected or exc.status_code in {502, 503, 504}:
                    self._failure(exc, lease_rejected)
                else:
                    # A non-connectivity HTTP response still proves that the
                    # authenticated server channel answered this request.
                    self._success()
                raise
            self._success()
            return result

        return observed


class DaemonRuntime:
    """Coordinates registry operations and worker subprocesses."""

    def __init__(
        self,
        store: RegistryStore,
        *,
        auto_build_envs: bool = True,
        env_build_timeout_seconds: float | None = None,
        env_stale_lock_seconds: float | None = None,
        daemon_base_url: str = "http://127.0.0.1:8765",
        docker_pool_enabled: bool = False,
        docker_pool_size: int = 0,
        docker_idle_timeout_seconds: float = 300.0,
        docker_prewarm: bool = False,
        allow_remote_custom_adapters: bool = False,
        telemetry: TelemetryLevel = DEFAULT_TELEMETRY_LEVEL,
        telemetry_sensitive_fields: tuple[str, ...] | list[str] = (),
        environment_manager: EnvironmentManagerProtocol | None = None,
        docker_environment_manager: DockerEnvironmentManagerProtocol | None = None,
        docker_pool: DockerPoolRunnerProtocol | None = None,
        runtime_backends: RuntimeBackendRegistry | None = None,
        sync_visibility: SyncVisibilityProtocol | None = None,
        server_connections: ServerConnectionsProtocol | None = None,
        heartbeat_service: HeartbeatsProtocol | None = None,
        daemon_identity: DaemonInstanceIdentity | None = None,
        daemon_home_lock: DaemonHomeLock | None = None,
        startup_binding: StartupBinding | None = None,
        startup_supervisor_proof: str | None = None,
        server_client_factory: ServerClientFactoryProtocol = (_default_server_client_factory),
        environment_manager_factory: EnvironmentManagerFactory = (_default_environment_manager_factory),
        docker_environment_manager_factory: DockerEnvironmentManagerFactory = (
            _default_docker_environment_manager_factory
        ),
        docker_pool_factory: DockerPoolFactory = _default_docker_pool_factory,
        runtime_backend_registry_factory: RuntimeBackendRegistryFactory = (_default_runtime_backend_registry_factory),
        sync_visibility_factory: SyncVisibilityFactory = (_default_sync_visibility_factory),
        server_connection_manager_factory: ServerConnectionManagerFactory = (
            _default_server_connection_manager_factory
        ),
        heartbeat_service_factory: HeartbeatServiceFactory = (_default_heartbeat_service_factory),
    ):
        # Desktop/GUI launchers can provide a system-only PATH.  Activate an
        # exact standard Docker installation before managers perform startup
        # cleanup, environment builds, or worker command construction.
        self._docker_cli_path = ensure_docker_cli_on_path()
        self.store = store
        self.daemon_identity = daemon_identity
        self.daemon_home_lock = daemon_home_lock
        self.startup_binding = startup_binding
        self.lifecycle = LifecycleController(
            daemon_identity,
            supervisor_id=(startup_binding.supervisor_id if startup_binding is not None else None),
            supervisor_proof=startup_supervisor_proof,
            binding_id=(startup_binding.binding_id if startup_binding is not None else None),
        )
        self.callback_capabilities = CallbackCapabilityAuthority(self._callback_run_status)
        self.telemetry_policy = TelemetryPolicy(
            telemetry,
            tuple(telemetry_sensitive_fields),
        )
        telemetry_log_level = (
            logging.INFO if self.telemetry_policy.level == DEFAULT_TELEMETRY_LEVEL else logging.WARNING
        )
        LOGGER.log(
            telemetry_log_level,
            "daemon telemetry level=%s raw_values_mirrored=%s redaction=%s",
            self.telemetry_policy.level,
            self.telemetry_policy.level == "full",
            "best_effort",
            extra={"spl_event": "daemon_telemetry_policy"},
        )
        self.auto_build_envs = auto_build_envs
        self.allow_remote_custom_adapters = bool(allow_remote_custom_adapters)
        self.browser_adapter_runs = BrowserAdapterRunBroker(self)
        self.daemon_base_url = daemon_base_url.rstrip("/")
        self.server_client_factory = server_client_factory
        manager_kwargs = {}
        if env_build_timeout_seconds is not None:
            manager_kwargs["build_timeout_seconds"] = env_build_timeout_seconds
        if env_stale_lock_seconds is not None:
            manager_kwargs["stale_lock_seconds"] = env_stale_lock_seconds
        self.environment_manager = environment_manager or environment_manager_factory(
            store,
            **manager_kwargs,
        )
        self.docker_environment_manager = docker_environment_manager or docker_environment_manager_factory(
            store, **manager_kwargs
        )
        self.docker_pool = docker_pool or docker_pool_factory(
            store,
            self.docker_environment_manager,
            daemon_base_url=self.daemon_base_url,
            enabled=docker_pool_enabled,
            pool_size=docker_pool_size,
            idle_timeout_seconds=docker_idle_timeout_seconds,
            prewarm=docker_prewarm,
            daemon_identity=daemon_identity,
            daemon_home_lock=daemon_home_lock,
        )
        backend_services = RuntimeBackendServices(
            environment_manager=self.environment_manager,
            docker_environment_manager=self.docker_environment_manager,
            docker_pool=self.docker_pool,
        )
        self.runtime_backends = runtime_backends or runtime_backend_registry_factory(backend_services)
        for manager in (
            self.environment_manager,
            self.docker_environment_manager,
            self.docker_pool,
        ):
            bind_lifecycle = getattr(manager, "bind_lifecycle", None)
            if callable(bind_lifecycle):
                bind_lifecycle(self.lifecycle)
        self.sync_visibility = sync_visibility or sync_visibility_factory(store)
        self._server_sync_lock = threading.Lock()
        self._server_channel_lock = threading.Lock()
        self._server_channel_success_at: dict[str, float] = {}
        self._server_channel_failure_count: dict[str, int] = {}
        self._server_channel_failed: set[str] = set()
        self._server_channel_half_open: set[str] = set()
        self._server_channel_probe_events: dict[str, threading.Event] = {}
        self._server_channel_last_probe_result: dict[str, dict[str, Any]] = {}
        self._server_attempt_lock = threading.Lock()
        self._superseded_server_attempts: set[tuple[str, str | None]] = set()
        self._run_threads_lock = threading.Lock()
        self._run_threads: list[threading.Thread] = []
        self._lifecycle_run_leases_lock = threading.Lock()
        self._lifecycle_run_leases: dict[str, LifecycleWorkLease] = {}
        self._lifecycle_run_lineages: dict[str, str] = {}
        self._lifecycle_drain_threads_lock = threading.Lock()
        self._lifecycle_drain_threads: dict[str, threading.Thread] = {}
        self._shutdown_lock = threading.Lock()
        self._shutdown_complete = False
        self._retention_condition = threading.Condition()
        self._retention_deadlines: dict[str, float] = {}
        self._retention_scheduler_stopped = False
        self._retention_scheduler = threading.Thread(
            target=self._run_retention_scheduler,
            name="spl-run-retention-scheduler",
            daemon=True,
        )
        self._retention_scheduler_started = False
        self.server_connections = server_connections or server_connection_manager_factory(store, server_client_factory)
        self.heartbeat_service = heartbeat_service or heartbeat_service_factory(
            store,
            self.sync_once,
        )
        self._normalize_persisted_run_telemetry()
        self.docker_pool.cleanup_stale_containers()
        self.sweep_run_retention()
        self.restore_server_heartbeat()

    def _callback_run_status(self, run_id: str) -> str | None:
        """Return authoritative local status for callback authentication."""

        try:
            return str(self.store.get_run(run_id)["status"])
        except KeyError:
            return None

    def telemetry_status(self) -> dict[str, Any]:
        """Return the active nonsecret central telemetry policy summary."""

        return self.telemetry_policy.status()

    def lifecycle_status(self) -> dict[str, Any]:
        """Return exact live lifecycle truth without granting control."""

        home_lock = self.daemon_home_lock
        identity = self.daemon_identity
        if identity is None or home_lock is None or not home_lock.is_acquired or home_lock.identity is not identity:
            raise LifecycleError(
                "live_identity_unavailable",
                HTTPStatus.SERVICE_UNAVAILABLE,
                operation="status",
            )
        return self.lifecycle.status_document()

    def run_registry_mutation(
        self,
        operation: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Run one existing durable/configuration mutation under admission."""

        lease = self.lifecycle.reserve_current_or_root("registry_mutation")
        with lease:
            return operation(*args, **kwargs)

    def consume_lifecycle_startup_receipt(self, supervisor_proof: str | None) -> dict[str, Any]:
        """Consume the one server-only direct-child readiness receipt."""

        binding = self.startup_binding
        identity = self.daemon_identity
        home_lock = self.daemon_home_lock
        if (
            binding is None
            or identity is None
            or home_lock is None
            or not home_lock.is_acquired
            or home_lock.identity is not identity
        ):
            raise StartupBindingError("startup binding is unavailable")
        return binding.consume_receipt(
            identity,
            proof=supervisor_proof,
            observed_at=datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        )

    def request_lifecycle_drain(
        self,
        document: dict[str, Any],
        *,
        supervisor_proof: str | None,
    ) -> dict[str, Any]:
        """Close root admission and establish the central no-claim boundary."""

        receipt = self.lifecycle.request_drain(
            document,
            supervisor_proof=supervisor_proof,
        )
        drain_id = str(receipt["drain_id"])
        with self._lifecycle_drain_threads_lock:
            existing = self._lifecycle_drain_threads.get(drain_id)
            if existing is None or not existing.is_alive():
                thread = threading.Thread(
                    target=self._establish_lifecycle_claim_boundary,
                    args=(drain_id,),
                    name=f"spl-lifecycle-drain-{drain_id}",
                    daemon=True,
                )
                self._lifecycle_drain_threads[drain_id] = thread
                thread.start()
        return receipt

    def cancel_lifecycle_drain(
        self,
        document: dict[str, Any],
        *,
        supervisor_proof: str | None,
    ) -> dict[str, Any]:
        """Cancel the exact current schedule before stop commit."""

        return self.lifecycle.cancel_drain(
            document,
            supervisor_proof=supervisor_proof,
        )

    def _establish_lifecycle_claim_boundary(self, drain_id: str) -> None:
        """Run one serialized draining sync or prove no central lease exists."""

        try:
            credentials = self.store.current_server_connection_credentials()
            if credentials is None:
                self.lifecycle.mark_claim_boundary_not_required()
                return
            self.sync_once(
                connection_id=str(credentials["id"]),
                probe_server_channel=True,
            )
        except LifecycleError:
            # The sync wrapper records whether capability evidence or a sent
            # request failed.  Cancellation can also remove the drain while
            # this bounded coordinator is still returning.
            return
        except Exception:
            try:
                self.lifecycle.mark_claim_boundary_failed(outcome_unknown=False)
            except LifecycleError:
                pass
        finally:
            with self._lifecycle_drain_threads_lock:
                thread = self._lifecycle_drain_threads.get(drain_id)
                if thread is threading.current_thread():
                    self._lifecycle_drain_threads.pop(drain_id, None)

    def _normalize_persisted_run_telemetry(self) -> None:
        """Apply the active privacy policy before any restored sync can run."""

        expired = self.store.sync_events.expire_sync_event_payloads()
        rewritten = 0
        compacted = 0
        quarantined = 0
        for event in self.store.sync_events.list_run_sync_events():
            event_id = str(event["id"])
            kind = str(event["kind"])
            payload = event.get("payload")
            proof = local_run_proof(kind, payload)
            if event.get("status") == "sent":
                self.store.mark_sync_event_sent(event_id)
                compacted += 1
                continue
            if proof is None:
                raw_run = payload.get("run") if isinstance(payload, dict) else None
                raw_id = raw_run.get("id") if isinstance(raw_run, dict) else None
                try:
                    safe_id = validate_name(raw_id) if isinstance(raw_id, str) else "unknown"
                except ValueError:
                    safe_id = "unknown"
                raw_status = raw_run.get("status") if isinstance(raw_run, dict) else None
                known_statuses = m_manifest.ACTIVE_RUN_STATUSES | m_manifest.TERMINAL_RUN_STATUSES
                safe_status = str(raw_status) if raw_status in known_statuses else "unknown"
                safe_payload = {
                    "run": {
                        "id": safe_id,
                        "object_name": "unknown",
                        "object_display_name": "unknown",
                        "local_object_name": "unknown",
                        "status": safe_status,
                        "telemetry_level": "metadata",
                        "telemetry": {
                            "level": "metadata",
                            "redaction": "best_effort",
                            "availability": {
                                "input": False,
                                "result": False,
                                "streams": False,
                                "artifact_bodies": False,
                            },
                        },
                        "source_result_present": False,
                        "input_mirrored": False,
                        "result_mirrored": False,
                        "streams_mirrored": False,
                        "artifact_bodies_mirrored": False,
                    }
                }
                self.store.sync_events.rewrite_run_sync_event(
                    event_id,
                    safe_payload,
                    local_run_id=None,
                )
                rewritten += 1
                continue
            local_run_id, local_status = proof
            if kind == "run_update":
                normalized = dict(payload) if isinstance(payload, dict) else {}
                detail = normalized.get("payload")
                normalized_detail = dict(detail) if isinstance(detail, dict) else {}
                normalized_detail["local_run"] = {
                    "id": local_run_id,
                    "status": local_status,
                }
                normalized["payload"] = normalized_detail
                self.store.sync_events.rewrite_run_sync_event(
                    event_id,
                    normalized,
                    local_run_id=local_run_id,
                )
                rewritten += 1
                continue
            try:
                state = self.store.get_run(local_run_id)
            except KeyError:
                self.store.sync_events.rewrite_run_sync_event(
                    event_id,
                    {"run": {"id": local_run_id, "status": local_status}},
                    local_run_id=None,
                )
                self.store.mark_sync_event_failed(
                    event_id,
                    "local run no longer exists; telemetry payload was scrubbed",
                    retryable=False,
                )
                quarantined += 1
                continue
            self.store.sync_events.rewrite_run_sync_event(
                event_id,
                {"run": self._local_run_sync_payload(state)},
                local_run_id=local_run_id,
            )
            rewritten += 1
        if expired or rewritten or compacted or quarantined:
            LOGGER.info(
                "normalized persisted run telemetry expired=%d rewritten=%d compacted=%d quarantined=%d",
                expired,
                rewritten,
                compacted,
                quarantined,
                extra={"spl_event": "run_telemetry_queue_normalized"},
            )

    def _bind_lifecycle_run_lease(
        self,
        run_id: str,
        lease: LifecycleWorkLease,
    ) -> None:
        """Bind one process-local admission lease to an exact durable Run."""

        with self._lifecycle_run_leases_lock:
            if run_id in self._lifecycle_run_leases:
                self.lifecycle.record_unknown_work()
                raise LifecycleError(
                    "run_admission_already_bound",
                    HTTPStatus.CONFLICT,
                    operation="admit_work",
                )
            self._lifecycle_run_leases[run_id] = lease
            self._lifecycle_run_lineages[run_id] = lease.lineage_id

    def _release_lifecycle_run_lease(
        self,
        run_id: str,
        *,
        seal_lineage: bool | None = None,
    ) -> None:
        """Release one exact Run lease after all terminal descendants finish."""

        with self._lifecycle_run_leases_lock:
            lease = self._lifecycle_run_leases.pop(run_id, None)
            self._lifecycle_run_lineages.pop(run_id, None)
        if lease is not None:
            lease.complete(
                seal_lineage=lease.root if seal_lineage is None else seal_lineage,
            )

    def _release_unbound_lifecycle_lease(self, lease: LifecycleWorkLease) -> None:
        """Close a reservation that failed before an exact Run was bound."""

        with self._lifecycle_run_leases_lock:
            bound = any(candidate is lease for candidate in self._lifecycle_run_leases.values())
        if not bound:
            lease.complete(seal_lineage=lease.root)

    def _reserve_run_descendant(
        self,
        run_id: str,
        kind: str,
        *,
        queued: bool = False,
    ) -> LifecycleWorkLease | None:
        """Reserve a known descendant without reconstructing a lineage."""

        with self._lifecycle_run_leases_lock:
            lineage_id = self._lifecycle_run_lineages.get(run_id)
        if lineage_id is None:
            return None
        return self.lifecycle.reserve_descendant(
            kind,
            lineage_id,
            queued=queued,
        )

    def run_worker_callback(
        self,
        parent_run_id: str | None,
        node: dict[str, Any],
        *,
        kwargs: dict[str, Any],
        timeout_seconds: float | None,
    ) -> dict[str, Any]:
        """Retain callback work under its exact parent Run when available."""

        lease = self._reserve_run_descendant(parent_run_id, "callback") if parent_run_id is not None else None
        if lease is None:
            lease = self.lifecycle.reserve_root("callback", queued=False)
        with lease:
            return self.run_remote_node(
                node,
                kwargs=kwargs,
                timeout_seconds=timeout_seconds,
            )

    def _server_client(
        self,
        server_url: str,
        token: str,
        *,
        user_token: str | None,
        request_timeout_seconds: float | None = None,
    ) -> ServerClientProtocol:
        return self.server_connections.server_client(
            server_url,
            token,
            user_token=user_token,
            request_timeout_seconds=request_timeout_seconds,
        )

    def _server_client_for_credentials(
        self,
        credentials: dict[str, Any],
        *,
        request_timeout_seconds: float | None = None,
    ) -> ServerClientProtocol:
        delegate = self.server_connections.server_client_for_credentials(
            credentials,
            request_timeout_seconds=request_timeout_seconds,
        )
        return cast(
            ServerClientProtocol,
            _ChannelObservedServerClient(
                delegate,
                success=lambda: self._mark_server_channel_success(credentials),
                failure=lambda error, immediate: self._mark_server_channel_failure(
                    credentials,
                    error=error,
                    immediate=immediate,
                ),
            ),
        )

    @staticmethod
    def _server_channel_key(credentials: dict[str, Any]) -> str:
        return str(credentials.get("id") or credentials.get("remote_connection_id") or "")

    @staticmethod
    def _server_channel_window_seconds(credentials: dict[str, Any]) -> float:
        interval = DaemonRuntime._safe_heartbeat_interval(credentials)
        return max(
            SERVER_CHANNEL_MIN_LIVENESS_WINDOW_SECONDS,
            interval * SERVER_CHANNEL_LIVENESS_MULTIPLIER,
        )

    def _mark_server_channel_success(self, credentials: dict[str, Any]) -> None:
        key = self._server_channel_key(credentials)
        if not key:
            return
        with self._server_channel_lock:
            self._server_channel_success_at[key] = time.monotonic()
            self._server_channel_failure_count.pop(key, None)
            self._server_channel_failed.discard(key)

    def _mark_server_channel_failure(
        self,
        credentials: dict[str, Any] | None,
        *,
        error: BaseException | None = None,
        immediate: bool = False,
    ) -> None:
        if credentials is None:
            return
        # The pre-F04 no-error seam is an explicit hard-open signal used by
        # recovery code/tests. Observed transient calls always pass the real
        # exception and therefore use the two-consecutive-failure threshold.
        hard_open = immediate or error is None
        if error is not None and getattr(error, "_spl_server_channel_failure_recorded", False):
            return
        if error is not None:
            try:
                setattr(error, "_spl_server_channel_failure_recorded", True)
            except (AttributeError, TypeError):
                pass
        key = self._server_channel_key(credentials)
        if not key:
            return
        with self._server_channel_lock:
            failure_count = self._server_channel_failure_count.get(key, 0) + 1
            self._server_channel_failure_count[key] = failure_count
            if hard_open or failure_count >= SERVER_CHANNEL_FAILURE_THRESHOLD:
                self._server_channel_success_at.pop(key, None)
                self._server_channel_failed.add(key)

    def _mark_current_server_channel_failure(
        self,
        *,
        error: BaseException | None = None,
        immediate: bool = False,
    ) -> None:
        self._mark_server_channel_failure(
            self.store.current_server_connection_credentials(),
            error=error,
            immediate=immediate,
        )

    def _server_channel_breaker_status(
        self,
        credentials: dict[str, Any] | None,
    ) -> dict[str, Any]:
        key = self._server_channel_key(credentials or {})
        with self._server_channel_lock:
            if key and key in self._server_channel_half_open:
                state = "half_open"
            elif key and key in self._server_channel_failed:
                state = "open"
            else:
                state = "closed"
            last_probe = self._server_channel_last_probe_result.get(key)
            status = {
                "state": state,
                "consecutive_failures": self._server_channel_failure_count.get(key, 0),
                "last_probe_result": dict(last_probe) if last_probe is not None else None,
            }
            if last_probe is not None:
                status["last_probe_at"] = last_probe.get("at")
                status["last_probe_ok"] = bool(last_probe["ok"])
            return status

    def _probe_server_channel(
        self,
        credentials: dict[str, Any],
    ) -> tuple[bool, str | None]:
        """Admit the side-effecting liveness probe before network or storage."""

        lease = self.lifecycle.reserve_current_or_root("registry_mutation")
        with lease:
            return self._probe_server_channel_admitted(credentials)

    def _probe_server_channel_admitted(
        self,
        credentials: dict[str, Any],
    ) -> tuple[bool, str | None]:
        """Run or join one bounded authenticated liveness probe per channel."""

        key = self._server_channel_key(credentials)
        if not key:
            return False, "stored server connection has no local channel id"

        with self._server_channel_lock:
            probe_event = self._server_channel_probe_events.get(key)
            leader = probe_event is None
            if probe_event is None:
                probe_event = threading.Event()
                self._server_channel_probe_events[key] = probe_event
                self._server_channel_half_open.add(key)

        if not leader:
            completed = probe_event.wait(SERVER_CHANNEL_PROBE_TIMEOUT_SECONDS)
            with self._server_channel_lock:
                result = self._server_channel_last_probe_result.get(key)
            if not completed or result is None:
                return False, "server channel probe did not complete within its bounded wait"
            return bool(result["ok"]), cast(str | None, result.get("detail"))

        ok = False
        detail: str | None = None
        try:
            server = self._server_client_for_credentials(
                credentials,
                request_timeout_seconds=SERVER_CHANNEL_PROBE_ATTEMPT_TIMEOUT_SECONDS,
            )
            remote_connection = server.current_connection()
            expected_remote_id = str(credentials.get("remote_connection_id") or "")
            actual_remote_id = str(remote_connection.get("id") or "")
            if not actual_remote_id or actual_remote_id != expected_remote_id:
                raise ServerClientError(
                    409,
                    "authenticated server channel returned a different connection identity",
                )
            self._mark_server_channel_success(credentials)
            ok = True
        except ServerClientError as exc:
            detail = exc.message
            identity_rejected = exc.status_code in SERVER_CHANNEL_LEASE_REJECTION_STATUSES
            self._mark_server_channel_failure(
                credentials,
                error=exc,
                immediate=identity_rejected,
            )
            if identity_rejected:
                try:
                    self.store.record_server_connection_error(
                        credentials["id"],
                        status=SERVER_CONNECTION_STATUS_NEEDS_RECONNECT,
                        error=detail,
                    )
                except Exception:
                    LOGGER.exception("could not mark rejected server channel for re-handshake")
        except Exception as exc:
            detail = str(exc) or repr(exc)
            self._mark_server_channel_failure(credentials, error=exc)
        finally:
            probe_result = {
                "ok": ok,
                "at": utc_now(),
                "detail": detail,
            }
            with self._server_channel_lock:
                self._server_channel_last_probe_result[key] = probe_result
                self._server_channel_half_open.discard(key)
                active_event = self._server_channel_probe_events.pop(key, None)
                if active_event is not None:
                    active_event.set()
        return ok, detail

    def _server_channel_is_live(
        self,
        credentials: dict[str, Any] | None,
        *,
        supervise_heartbeat: bool = True,
    ) -> bool:
        """Return true only for a recently proven central-server channel.

        Stored identity rows remain valid offline, but a persisted
        ``status='connected'`` row is not proof that the TCP channel is live
        after a daemon restart or network loss.  The circuit closes after an
        authenticated success and opens after consecutive transient failures
        (or immediately when the server rejects the stored lease identity).
        """

        if credentials is None:
            return False
        if supervise_heartbeat:
            self.heartbeat_service.ensure_server_heartbeat(credentials)
        if not credentials.get("remote_connection_id") or credentials.get("status") != "connected":
            return False
        key = self._server_channel_key(credentials)
        if not key:
            return False
        with self._server_channel_lock:
            if key in self._server_channel_failed:
                return False
            success_at = self._server_channel_success_at.get(key)
        if success_at is None:
            return False
        return time.monotonic() - success_at <= self._server_channel_window_seconds(credentials)

    def _require_live_server_channel_credentials(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        credentials = self._require_connected_server_credentials(credentials)
        if not self._server_channel_is_live(credentials):
            recovered, detail = self._probe_server_channel(credentials)
            if recovered:
                return credentials
            raise ServerOfflineError(
                SERVER_OFFLINE_MESSAGE,
                code=SERVER_UNREACHABLE_CODE,
                detail=detail,
            )
        return credentials

    def resolve_user_ref(self, owner_ref: str) -> str:
        """Resolve one owner reference immediately before local storage access."""

        owner_ref = str(owner_ref)
        if not owner_ref.startswith("@"):
            return validate_name(owner_ref)

        credentials = self.store.current_server_connection_credentials()
        try:
            credentials = self._require_live_server_channel_credentials(credentials)
        except (KeyError, ServerOfflineError):
            raise HandleRequiresServerConnectionError(owner_ref)
        server = self._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
        )
        try:
            users = server.list_users(handle=owner_ref)
        except ServerClientError as exc:
            if self._is_server_connectivity_error(exc):
                self._mark_server_channel_failure(credentials, error=exc)
                raise HandleRequiresServerConnectionError(owner_ref) from exc
            raise
        if len(users) != 1 or not users[0].get("id"):
            raise KeyError(f"user handle is not found: {owner_ref}")
        return validate_name(str(users[0]["id"]))

    def server_whoami(self) -> dict[str, Any]:
        """Return live directory identity or the stored canonical fallback."""

        connection_state = self.server_connection_state(probe=True)
        connection = self.store.current_server_connection()
        if connection is None or not connection.get("owner_id"):
            raise KeyError("active server connection is not found; connect first with client.connect_server(...)")
        owner_id = validate_name(str(connection["owner_id"]))
        result = {
            "id": owner_id,
            "owner_id": owner_id,
            "handle": None,
            "display_name": owner_id,
            "server_url": connection["server_url"],
            "machine_id": connection["machine_id"],
            "connection_status": connection["status"],
            "live": bool(connection_state["connected"]),
        }
        credentials = self.store.current_server_connection_credentials()
        if not result["live"]:
            return result
        server = self._server_client_for_credentials(
            cast(dict[str, Any], credentials),
            request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
        )
        try:
            users = server.list_users()
        except ServerClientError as exc:
            if exc.status_code == 404:
                return result
            if self._is_server_connectivity_error(exc):
                self._mark_server_channel_failure(credentials, error=exc)
                return result
            raise
        self._mark_server_channel_success(cast(dict[str, Any], credentials))
        user = next((item for item in users if item.get("id") == owner_id), None)
        if user is not None:
            result["handle"] = user.get("handle")
            result["display_name"] = user.get("display_name") or owner_id
        return result

    def server_connection_state(self, *, probe: bool = False) -> dict[str, Any]:
        """Report identity and channel liveness, optionally probing an open breaker.

        The runtime seam defaults to a cold read so heartbeat, health, and
        diagnostics machinery cannot accidentally perform network I/O. The
        user-facing ``GET /server/connection`` route explicitly enables the
        probe by default and exposes ``?probe=0`` for this cold variant.
        """

        connection = self.store.current_server_connection()
        credentials = self.store.current_server_connection_credentials()
        identity_present = bool(
            connection is not None and connection.get("owner_id") and connection.get("remote_connection_id")
        )
        live = identity_present and self._server_channel_is_live(
            credentials,
            supervise_heartbeat=False,
        )
        probe_detail: str | None = None
        if (
            probe
            and identity_present
            and not live
            and credentials is not None
            and credentials.get("status") == "connected"
            and credentials.get("remote_connection_id")
        ):
            live, probe_detail = self._probe_server_channel(credentials)
        state: dict[str, Any] = {
            "connected": live,
            "live": live,
            "identity_present": identity_present,
            "offline": identity_present and not live,
            "connection": connection,
            "breaker": self._server_channel_breaker_status(credentials),
        }
        if identity_present and not live:
            state.update(
                {
                    "code": SERVER_UNREACHABLE_CODE,
                    "error": SERVER_OFFLINE_MESSAGE,
                }
            )
            if probe_detail is not None:
                state["detail"] = probe_detail
        return state

    def sync_status(self, *, include_identity_scope: bool = False) -> dict[str, Any]:
        """Return bounded queue and heartbeat diagnostics for operators."""

        connection_state = self.server_connection_state(probe=False)
        connection = connection_state.get("connection") or {}
        queue = self.store.sync_event_status_summary()
        result = {
            **connection_state,
            "by_status": queue["by_status"],
            "oldest_event": queue["oldest_event"],
            "last_error": queue["last_error"],
            "heartbeat": self.heartbeat_service.status(connection.get("id")),
            "telemetry": self.telemetry_status(),
        }
        if include_identity_scope:
            current_owner_id = connection.get("owner_id")
            identity = self.store.pending_sync_event_identity_summary(
                str(current_owner_id) if current_owner_id else None
            )
            current_owner_pending = identity["pre_enrollment_pending"]
            if current_owner_id:
                current_owner_pending += identity["pending_by_owner"].get(str(current_owner_id), 0)
            total_pending = int(queue["by_status"].get("pending", 0))
            # Opt-in, aggregate-only evidence lets optional IDE clients
            # distinguish work sendable by the current connection from rows
            # intentionally held for another stored identity. Existing callers
            # keep the byte-compatible legacy response unless they opt in.
            result["identity_scope"] = {
                "current_owner_pending": current_owner_pending,
                "held_for_other_identities": max(0, total_pending - current_owner_pending),
            }
        return result

    def prune_sync_events(
        self,
        *,
        status: str,
        older_than_days: int,
        include_protected: bool = False,
        limit: int = 1_000,
    ) -> dict[str, Any]:
        """Prune a bounded queue slice, protecting non-telemetry events."""

        return cast(
            dict[str, Any],
            self.run_registry_mutation(
                self.store.prune_sync_events,
                status=status,
                older_than_days=older_than_days,
                include_protected=include_protected,
                limit=limit,
            ),
        )

    def _local_sync_status(self) -> dict[str, Any]:
        credentials = self.store.current_server_connection_credentials()
        live = self._server_channel_is_live(credentials, supervise_heartbeat=False)
        status = {
            "connected": live,
            "offline": credentials is not None and not live,
            "event_results": [],
            "jobs": [],
            "sync": self.sync_visibility.summary(),
        }
        if credentials is not None and not live:
            status["error"] = SERVER_OFFLINE_MESSAGE
            status["code"] = SERVER_UNREACHABLE_CODE
        return status

    @staticmethod
    def _pull_requires_server_connection_message() -> str:
        return (
            "pull requires a server connection; the daemon has no server connection. "
            "Reconnect with client.connect_server(...) to search the server catalog, "
            "then retry client.pull(...)."
        )

    @staticmethod
    def _unknown_worker_sync_operations(reason: str) -> dict[str, Any]:
        return {
            "evidence": "unknown",
            "pending": None,
            "retryable": None,
            "by_status": {
                "pending": None,
                "failed": None,
                "sent": None,
            },
            "oldest_pending_at": None,
            "reason": reason,
        }

    def _worker_sync_operations(self) -> dict[str, Any]:
        """Return exact aggregate queue evidence without payload or error content."""

        try:
            summary = self.store.sync_event_status_summary()
            raw_by_status = summary["by_status"]
            if not isinstance(raw_by_status, dict):
                raise TypeError("sync by_status is not a mapping")
            allowed_statuses = {"pending", "failed", "sent"}
            if set(raw_by_status) - allowed_statuses:
                raise ValueError("sync status vocabulary is not recognized")
            by_status = {
                status: self._nonnegative_operation_count(raw_by_status.get(status, 0))
                for status in ("pending", "failed", "sent")
            }
            retryable = self._nonnegative_operation_count(summary["retryable"])
            if retryable < by_status["pending"] or retryable > by_status["pending"] + by_status["failed"]:
                raise ValueError("retryable sync count contradicts status counts")
            oldest_pending_at = summary.get("oldest_pending_at")
            if by_status["pending"]:
                oldest_pending_at = self._operation_timestamp(oldest_pending_at)
                if oldest_pending_at is None:
                    raise ValueError("oldest pending timestamp is unavailable")
            else:
                oldest_pending_at = None
        except Exception:
            return self._unknown_worker_sync_operations("worker_sync_summary_unavailable")
        return {
            "evidence": "observed",
            "pending": by_status["pending"],
            "retryable": retryable,
            "by_status": by_status,
            "oldest_pending_at": oldest_pending_at,
        }

    @staticmethod
    def _unknown_worker_environment_builds(reason: str) -> dict[str, Any]:
        return {
            "evidence": "unknown",
            "total": None,
            "by_status": {
                ABSENT: None,
                CREATING: None,
                READY: None,
                FAILED: None,
            },
            "runtime_types": None,
            "latest_updated_at": None,
            "reason": reason,
        }

    def _worker_environment_builds(self) -> dict[str, Any]:
        """Return aggregate cached-build evidence without local specifications."""

        try:
            records = self.store.list_environment_builds()
            if not isinstance(records, list):
                raise TypeError("environment build records are not a list")
            by_status = {
                ABSENT: 0,
                CREATING: 0,
                READY: 0,
                FAILED: 0,
            }
            runtime_types: set[str] = set()
            updated_at_values: list[str] = []
            for record in records:
                if not isinstance(record, dict):
                    raise TypeError("environment build record is not a mapping")
                status = record.get("status")
                if status not in by_status:
                    raise ValueError("environment build status is not recognized")
                runtime_type = record.get("runtime_type")
                if runtime_type not in RUNTIME_BACKENDS:
                    raise ValueError("environment build runtime is not recognized")
                updated_at = self._operation_timestamp(record.get("updated_at"))
                if updated_at is None:
                    raise ValueError("environment build timestamp is unavailable")
                by_status[str(status)] += 1
                runtime_types.add(str(runtime_type))
                updated_at_values.append(updated_at)
        except Exception:
            return self._unknown_worker_environment_builds("worker_environment_build_summary_unavailable")
        return {
            "evidence": "observed",
            "total": len(records),
            "by_status": by_status,
            "runtime_types": sorted(runtime_types),
            "latest_updated_at": max(updated_at_values) if updated_at_values else None,
        }

    def _worker_operations_capability(self) -> dict[str, Any]:
        """Return versioned, allowlisted Worker evidence for the central server."""

        return {
            "schema_version": WORKER_OPERATIONS_SCHEMA_VERSION,
            "observed_at": utc_now(),
            "sync": self._worker_sync_operations(),
            "environment_builds": self._worker_environment_builds(),
            "runtimes": {
                "implemented_object_modes": sorted(RUNTIME_BACKENDS),
                "implemented_node_modes": sorted(NODE_RUNTIME_BACKENDS),
                "availability": "unverified",
                "reason": "runtime_availability_not_probed",
            },
            "diagnostics": {
                "availability": "local_only",
                "command": "spl-daemon doctor --json",
                "sharing": "explicit_consent_required",
            },
        }

    @staticmethod
    def _execution_manifest_capability() -> dict[str, Any]:
        """Describe the bounded terminal evidence this daemon can report."""

        return {
            "schema_version": EXECUTION_MANIFEST_CAPABILITY_VERSION,
            "terminal_summary": True,
            "artifact_producer_evidence": True,
            "full_manifest": False,
        }

    @staticmethod
    def _worker_build_capability() -> dict[str, Any]:
        """Return installed-package evidence without claiming artifact provenance."""

        try:
            package_version: str | None = importlib_metadata.version("splime")
        except importlib_metadata.PackageNotFoundError:
            package_version = None
        return {
            "schema_version": WORKER_BUILD_SCHEMA_VERSION,
            "package": "splime",
            "package_version": package_version,
            "version_evidence": ("installed_distribution_metadata" if package_version is not None else "unknown"),
            "artifact_sha256": None,
            "source_ref": None,
            "protocols": {
                RUN_CLAIM_FENCING_CAPABILITY: RUN_CLAIM_FENCING_VERSION,
                EXECUTION_MANIFEST_CAPABILITY: EXECUTION_MANIFEST_CAPABILITY_VERSION,
                WORKER_OPERATIONS_CAPABILITY: WORKER_OPERATIONS_SCHEMA_VERSION,
            },
        }

    @staticmethod
    def _nonnegative_operation_count(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("operation count must be a non-negative integer")
        return value

    @staticmethod
    def _operation_timestamp(value: Any) -> str | None:
        if not isinstance(value, str) or not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(UTC).isoformat()

    def _authoritative_server_capabilities(
        self,
        capabilities: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Advertise daemon-owned server capabilities without trusting input."""

        advertised = dict(capabilities or {})
        advertised[RUN_CLAIM_FENCING_CAPABILITY] = RUN_CLAIM_FENCING_VERSION
        advertised[TELEMETRY_POLICY_CAPABILITY] = self.telemetry_policy.status()
        advertised[WORKER_OPERATIONS_CAPABILITY] = self._worker_operations_capability()
        advertised[EXECUTION_MANIFEST_CAPABILITY] = self._execution_manifest_capability()
        advertised[WORKER_BUILD_CAPABILITY] = self._worker_build_capability()
        advertised[RUNTIME_PORT_ADAPTERS_CAPABILITY] = {
            "schema_version": 1,
            "transport": True,
            "custom_adapter_execution": {
                "implemented": True,
                "enabled": self.allow_remote_custom_adapters,
            },
        }
        advertised[RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY] = {
            "schema_version": 1,
            "advisory": True,
        }
        library_environment = self._runtime_library_adapter_environment()
        advertised[RUNTIME_LIBRARY_ADAPTER_REF_CAPABILITY] = {
            "schema_version": 1,
            "execution": True,
            "custom_adapter_execution": {
                "implemented": True,
                "enabled": self.allow_remote_custom_adapters,
            },
            "environment": library_environment,
        }
        return advertised

    @staticmethod
    def _runtime_library_adapter_environment() -> dict[str, Any]:
        """Return bounded, path-free exact distribution evidence.

        Distribution metadata is inspected without importing optional
        packages.  Invalid or ambiguously duplicated records are omitted, so
        central availability can only produce a conservative false negative.
        """

        candidates: dict[str, dict[str, str]] = {}
        ambiguous: set[str] = set()
        try:
            installed = importlib_metadata.distributions()
        except Exception:
            installed = ()
        for distribution in installed:
            try:
                name = distribution.metadata.get("Name")
                version = distribution.version
                [record] = normalize_environment_distributions([{"package": name, "version": version}])
            except (AttributeError, KeyError, TypeError, ValueError):
                continue
            key = canonical_package_name(record["package"])
            prior = candidates.get(key)
            if prior is not None and prior != record:
                ambiguous.add(key)
                candidates.pop(key, None)
                continue
            if key not in ambiguous:
                candidates[key] = record
        distributions = [candidates[key] for key in sorted(candidates) if key not in ambiguous][
            :MAX_ENVIRONMENT_DISTRIBUTIONS
        ]
        return {
            "fingerprint": environment_fingerprint(distributions),
            "distributions": distributions,
        }

    def connect_server(
        self,
        *,
        server_url: str,
        machine_token: str,
        user_token: str,
        machine_id: str | None,
        display_name: str | None,
        capabilities: dict[str, Any],
        heartbeat_interval_seconds: float | None,
    ) -> dict[str, Any]:
        """Connect to the central daemon server and start lease heartbeats."""

        return cast(
            dict[str, Any],
            self.run_registry_mutation(
                self._connect_server_admitted,
                server_url=server_url,
                machine_token=machine_token,
                user_token=user_token,
                machine_id=machine_id,
                display_name=display_name,
                capabilities=capabilities,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
            ),
        )

    def _connect_server_admitted(
        self,
        *,
        server_url: str,
        machine_token: str,
        user_token: str,
        machine_id: str | None,
        display_name: str | None,
        capabilities: dict[str, Any],
        heartbeat_interval_seconds: float | None,
    ) -> dict[str, Any]:
        """Perform one already-admitted central connection mutation."""

        result = self.server_connections.connect_server(
            server_url=server_url,
            machine_token=machine_token,
            user_token=user_token,
            machine_id=machine_id,
            display_name=display_name,
            capabilities=self._authoritative_server_capabilities(capabilities),
            heartbeat_interval_seconds=heartbeat_interval_seconds,
        )
        if not result.get("reused") and result.get("connection") is not None:
            self.start_server_heartbeat(result["connection"], token=machine_token)
        connection = result.get("connection") or {}
        network_verified = bool(result.get("refreshed") or not result.get("reused"))
        if connection.get("owner_id") and connection.get("remote_connection_id"):
            credentials = self.store.get_server_connection_credentials(connection["id"])
            if network_verified:
                self._mark_server_channel_success(credentials)
            try:
                credentials = self._require_live_server_channel_credentials(credentials)
            except (KeyError, ServerOfflineError):
                result["reconcile"] = {
                    "skipped": True,
                    "reason": "server_channel_not_live",
                }
            else:
                result["reconcile"] = self.reconcile_connected_objects(credentials)
        return result

    def _matching_server_connection(
        self,
        *,
        server_url: str,
        machine_token: str,
        user_token: str,
        machine_id: str | None,
    ) -> dict[str, Any] | None:
        """Return the active connection when the requested credentials match.

        Repeated ``SPLClient(machine_token=..., user_token=...)`` calls are a
        normal notebook workflow.  They should reuse the local daemon's existing
        lease instead of asking the server to connect the same token again.
        """

        return self.server_connections.matching_server_connection(
            server_url=server_url,
            machine_token=machine_token,
            user_token=user_token,
            machine_id=machine_id,
        )

    @staticmethod
    def _is_server_connectivity_error(exc: ServerClientError) -> bool:
        return ServerConnectionManager._is_server_connectivity_error(exc)

    @staticmethod
    def _offline_machine_id(
        machine_token: str,
        *,
        machine_id: str | None,
        display_name: str | None,
    ) -> str:
        return ServerConnectionManager._offline_machine_id(
            machine_token,
            machine_id=machine_id,
            display_name=display_name,
        )

    def _restore_pending_server_connection(
        self,
        credentials: dict[str, Any],
    ) -> dict[str, Any]:
        return self.server_connections.restore_pending_server_connection(credentials)

    def _require_connected_server_credentials(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.server_connections.require_connected_server_credentials(credentials)

    def _remote_connection_snapshot(self, connection: dict[str, Any]) -> dict[str, Any]:
        """Build a server-like connection payload from the local cached row."""

        return self.server_connections.remote_connection_snapshot(connection)

    def disconnect_server(self) -> dict[str, Any]:
        """Gracefully disconnect the current central-server lease."""

        return cast(
            dict[str, Any],
            self.run_registry_mutation(self._disconnect_server_admitted),
        )

    def _disconnect_server_admitted(self) -> dict[str, Any]:
        """Perform one already-admitted central disconnect mutation."""

        credentials = self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError("active server connection is not found")

        self.stop_server_heartbeat(credentials["id"])
        return self.server_connections.disconnect_server(credentials)

    def restore_server_heartbeat(self) -> None:
        """Resume heartbeat loop for a persisted active connection, if present."""

        self.heartbeat_service.restore_server_heartbeat()

    def start_server_heartbeat(
        self,
        connection: dict[str, Any],
        *,
        token: str,
    ) -> None:
        """Start one background heartbeat loop and stop older loops."""

        self.heartbeat_service.start_server_heartbeat(connection, token=token)

    def stop_server_heartbeat(self, connection_id: str) -> None:
        """Stop the heartbeat loop for one local connection."""

        self.heartbeat_service.stop_server_heartbeat(connection_id)

    def enqueue_object_sync(
        self,
        record: dict[str, Any],
        *,
        library: str | None = None,
        create_library: bool = False,
        library_display_name: str | None = None,
    ) -> dict[str, Any]:
        """Queue a freshly registered local object version for server sync."""

        return cast(
            dict[str, Any],
            self.run_registry_mutation(
                self._enqueue_object_sync_admitted,
                record,
                library=library,
                create_library=create_library,
                library_display_name=library_display_name,
            ),
        )

    def enqueue_library_adapter_sync(self, ref: dict[str, Any]) -> dict[str, Any]:
        """Capability-gate and send one source-bearing Adapter publication.

        The existing generic sync event is intentionally not the primary path:
        its 256 KiB bound is smaller than the established 512 KiB custom-code
        bundle bound.  This direct mutation disables transport retries; an
        ambiguous response is reported as deferred and is never replayed by
        this call.
        """

        credentials = self.store.current_server_connection_credentials()
        if credentials is None or not credentials.get("remote_connection_id"):
            return {"state": "deferred", "reason": "server_connection_unavailable"}
        if not self._server_channel_is_live(credentials, supervise_heartbeat=False):
            return {"state": "deferred", "reason": "server_connection_offline"}
        server = self._server_client_for_credentials(credentials)
        version = server.get_server_version()
        declared = version.get("declared")
        contracts = declared.get("contracts") if isinstance(declared, dict) else None
        capabilities = contracts.get("daemon_server_capabilities") if isinstance(contracts, dict) else None
        if not isinstance(capabilities, list) or LIBRARY_ADAPTER_PUBLISH_CAPABILITY not in capabilities:
            return {"state": "unsupported", "reason": "server_capability_not_advertised"}
        payload = library_adapter_sync_payload(self.store, ref)
        owner = str(ref["owner"])
        library = str(payload.pop("library"))
        remote = server.publish_library_adapter(owner, library, payload)
        remote_ref = remote.get("ref") if isinstance(remote.get("ref"), dict) else remote.get("version")
        if isinstance(remote_ref, dict):
            prepared = normalize_publish_request(payload)
            self.store.publish_library_adapter(
                prepared,
                owner_id=ref["owner"],
                library=ref["library"],
                publisher_id=ref["owner"],
                adapter_id=ref["adapter_id"],
                remote_owner_id=remote_ref.get("owner"),
                remote_adapter_id=remote_ref.get("adapter_id"),
                remote_version_id=remote_ref.get("adapter_version_id"),
            )
        return {"state": "synced", "reason": None, "remote": remote}

    def _enqueue_object_sync_admitted(
        self,
        record: dict[str, Any],
        *,
        library: str | None = None,
        create_library: bool = False,
        library_display_name: str | None = None,
    ) -> dict[str, Any]:
        """Persist one already-admitted Object sync event."""

        version = self.store.get_object(
            record["id"],
            version=record["version"],
            include_yaml=True,
        )
        payload = self._object_sync_payload_for_version(version)
        if library:
            payload["library"] = validate_name(library)
        if create_library:
            payload["create_library"] = True
        if library_display_name:
            payload["library_display_name"] = str(library_display_name)
        payload["revive_removed"] = True
        return self.store.enqueue_sync_event("object_version", payload)

    def _object_sync_payload_for_version(
        self,
        version: dict[str, Any],
    ) -> dict[str, Any]:
        payload = {
            "name": version["name"],
            "entrypoint": version["entrypoint"],
            "env": version["env"],
            "env_python": version.get("env_python"),
            "env_python_version": version.get("env_python_version"),
            "kind": version.get("kind") or version.get("type") or "unknown",
            "description": version.get("description") or "",
            "version_label": version.get("version_label"),
            "yaml": version["yaml"],
            "content_hash": version.get("content_hash") or version["yaml_sha256"],
            "metadata": version.get("metadata") or {},
            "distributions": version.get("distributions") or [],
            "runtime_config": version.get("runtime_config") or {"mode": "venv"},
            "source_object_id": version["id"],
            "source_version_id": version["version_id"],
        }
        if version.get("owner_id"):
            payload["owner_id"] = version["owner_id"]
        if version.get("library"):
            payload["library"] = version["library"]
        if version.get("content_hash"):
            payload["content_hash"] = version["content_hash"]
        return payload

    def reconcile_connected_objects(
        self,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Link local objects to the connected owner's server namespace."""

        credentials = credentials or self.store.current_server_connection_credentials()
        if (
            credentials is None
            or not credentials.get("remote_connection_id")
            or not credentials.get("owner_id")
            or credentials.get("status") != "connected"
        ):
            return {"skipped": True, "reason": "not_connected"}

        owner_id = validate_name(str(credentials["owner_id"]))
        report: dict[str, Any] = {
            "owner_id": owner_id,
            "rekey": self.store.rekey_local_placeholder_objects(owner_id),
            "objects": [],
            "conflicts": [],
            "pending_sync_events": [],
        }
        server = self._server_client_for_credentials(credentials)
        for identity in self.store.list_object_identities(owner_id=owner_id):
            if str(identity["name"]).startswith("server."):
                continue
            object_report = self._reconcile_connected_object(
                server,
                identity,
                owner_id=owner_id,
            )
            report["objects"].append(object_report)
            report["conflicts"].extend(object_report.get("conflicts") or [])
            report["pending_sync_events"].extend(object_report.get("pending_sync_events") or [])
        return report

    def _reconcile_connected_object(
        self,
        server: ServerClientProtocol,
        identity: dict[str, Any],
        *,
        owner_id: str,
    ) -> dict[str, Any]:
        library = validate_name(str(identity.get("library") or DEFAULT_OBJECT_LIBRARY))
        name = validate_name(str(identity["name"]))
        canonical_name = f"{owner_id}/{library}/{name}"
        report: dict[str, Any] = {
            "canonical_name": canonical_name,
            "status": "checked",
            "conflicts": [],
            "imported_versions": [],
            "pending_sync_events": [],
        }
        try:
            remote_current = server.get_object(
                name,
                include_yaml=False,
                owner_id=owner_id,
                library=library,
            )
        except ServerClientError as exc:
            if exc.status_code == 404:
                report["status"] = "local_only"
                report["pending_sync_events"] = self._enqueue_local_only_object_syncs(
                    owner_id=owner_id,
                    library=library,
                    name=name,
                )
                return report
            raise

        remote_object_id = remote_current.get("id")
        self.store.link_object_remote_identity(
            owner_id=owner_id,
            library=library,
            name=name,
            remote_owner_id=remote_current.get("owner_id") or owner_id,
            remote_object_id=remote_object_id,
            source_object_name=remote_current.get("name") or name,
        )
        # One metadata-only request: version numbers + content_hash suffice for
        # linking and conflict detection; YAML bodies are fetched lazily and
        # only for versions that actually need importing.
        remote_versions = server.list_object_versions(
            name,
            include_yaml=False,
            owner_id=owner_id,
            library=library,
        )
        if not remote_versions:
            remote_versions = [remote_current]
        self._ensure_server_object_envs(remote_versions)

        local_versions = self.store.list_object_versions(
            name,
            owner_id=owner_id,
            library=library,
        )
        local_by_hash = {item["content_hash"]: item for item in local_versions if item.get("content_hash")}
        local_by_version = {int(item["version"]): item for item in local_versions}
        remote_hashes = {item.get("content_hash") for item in remote_versions if item.get("content_hash")}
        remote_max_version = max(
            (int(item.get("version") or 0) for item in remote_versions),
            default=0,
        )
        conflict_local_version_ids: set[str] = set()

        for remote_version in sorted(
            remote_versions,
            key=lambda item: int(item.get("version") or 0),
        ):
            remote_hash = remote_version.get("content_hash")
            remote_number = int(remote_version.get("version") or 0)
            remote_version_id = remote_version.get("version_id")
            if remote_version_id:
                already_imported = self.store.get_object_by_remote_version(
                    remote_version_id,
                    include_yaml=False,
                )
                if already_imported is not None:
                    # Steady state: the version is linked locally already —
                    # no conflict is possible and no YAML round-trip happens.
                    report["imported_versions"].append(already_imported["version_id"])
                    continue
            local_at_version = local_by_version.get(remote_number)
            if (
                remote_hash
                and local_at_version is not None
                and local_at_version.get("content_hash") != remote_hash
                and remote_hash not in local_by_hash
            ):
                conflict = self._record_object_reconcile_conflict(
                    owner_id=owner_id,
                    library=library,
                    name=name,
                    local_version=local_at_version,
                    remote_version=remote_version,
                    remote_object_id=remote_object_id,
                )
                report["conflicts"].append(conflict)
                conflict_local_version_ids.add(local_at_version["version_id"])
                continue
            if remote_hash and remote_hash in local_by_hash:
                self.store.link_object_remote_identity(
                    owner_id=owner_id,
                    library=library,
                    name=name,
                    remote_owner_id=remote_version.get("owner_id") or owner_id,
                    remote_object_id=remote_version.get("id") or remote_object_id,
                    source_object_name=remote_version.get("name") or name,
                )
            imported = self._import_reconcile_remote_version(
                remote_version,
                owner_id=owner_id,
                library=library,
                remote_object_id=remote_object_id,
                server=server,
            )
            report["imported_versions"].append(imported["version_id"])

        for local_version in self.store.list_object_versions(
            name,
            owner_id=owner_id,
            library=library,
        ):
            if local_version.get("remote_version_id"):
                continue
            if local_version.get("version_id") in conflict_local_version_ids:
                continue
            if local_version.get("content_hash") in remote_hashes:
                continue
            if int(local_version["version"]) <= remote_max_version and remote_hashes:
                continue
            event = self._enqueue_object_version_sync_once(local_version)
            report["pending_sync_events"].append(event)
        report["status"] = "linked"
        return report

    def _import_reconcile_remote_version(
        self,
        remote_version: dict[str, Any],
        *,
        owner_id: str,
        library: str,
        remote_object_id: str | None,
        server: ServerClientProtocol | None = None,
    ) -> dict[str, Any]:
        yaml_text = remote_version.get("yaml")
        if not yaml_text:
            existing = self.store.get_object_by_remote_version(
                remote_version["version_id"],
                include_yaml=False,
            )
            if existing is not None:
                return existing
            if server is not None:
                # Lazy body fetch: only versions that are genuinely new to
                # this daemon cost a YAML round-trip.
                remote_number = remote_version.get("version")
                fetched = server.get_object(
                    remote_version["name"],
                    version=(int(remote_number) if remote_number is not None else None),
                    include_yaml=True,
                    owner_id=owner_id,
                    library=library,
                )
                if fetched:
                    remote_version = {**remote_version, **fetched}
                    yaml_text = remote_version.get("yaml")
        if not yaml_text:
            raise RuntimeError(f"server did not return YAML for object version {remote_version.get('version_id')}")
        return self.register_object(
            remote_version["name"],
            remote_version["entrypoint"],
            remote_version.get("env") or "default",
            yaml_text=yaml_text,
            owner_id=owner_id,
            library=library,
            description=remote_version.get("description") or "",
            version_label=remote_version.get("version_label"),
            origin="server",
            remote_owner_id=remote_version.get("owner_id") or owner_id,
            remote_object_id=remote_version.get("id") or remote_object_id,
            remote_version_id=remote_version.get("version_id"),
            source_object_name=remote_version["name"],
            runtime_config=remote_version.get("runtime_config"),
        )

    def _enqueue_local_only_object_syncs(
        self,
        *,
        owner_id: str,
        library: str,
        name: str,
    ) -> list[dict[str, Any]]:
        events = []
        for version in self.store.list_object_versions(
            name,
            owner_id=owner_id,
            library=library,
        ):
            if version.get("remote_version_id"):
                continue
            events.append(self._enqueue_object_version_sync_once(version))
        return events

    def _enqueue_object_version_sync_once(
        self,
        version: dict[str, Any],
    ) -> dict[str, Any]:
        full_version = self.store.get_object_version(
            version["version_id"],
            include_yaml=True,
        )
        payload = self._object_sync_payload_for_version(full_version)
        payload["revive_removed"] = False
        return self.store.enqueue_object_version_sync_once(payload)

    def _record_object_reconcile_conflict(
        self,
        *,
        owner_id: str,
        library: str,
        name: str,
        local_version: dict[str, Any],
        remote_version: dict[str, Any],
        remote_object_id: str | None,
    ) -> dict[str, Any]:
        payload = {
            "canonical_name": f"{owner_id}/{library}/{name}",
            "owner_id": owner_id,
            "library": library,
            "name": name,
            "reason": "divergent_content",
            "local_object_id": local_version["id"],
            "local_version_id": local_version["version_id"],
            "local_version": local_version["version"],
            "local_content_hash": local_version.get("content_hash"),
            "remote_object_id": remote_object_id,
            "remote_version_id": remote_version.get("version_id"),
            "remote_version": remote_version.get("version"),
            "remote_content_hash": remote_version.get("content_hash"),
        }
        return self.store.record_object_conflict_once(payload)

    def build_machine_library_snapshot_manifest(self) -> tuple[str, list[dict[str, Any]]]:
        """Build a lightweight, stable manifest for the current local library."""

        items = []
        for record in self.store.list_objects().values():
            items.append(
                {
                    "library_slug": record.get("library") or DEFAULT_OBJECT_LIBRARY,
                    "name": record["name"],
                    "display_name": record.get("display_name") or record["name"],
                    "description": record.get("description") or "",
                    "local_object_id": record["id"],
                    "local_version_id": record["version_id"],
                    "version": record["version"],
                    "version_label": record.get("version_label"),
                    "entrypoint": record["entrypoint"],
                    "env": record.get("env"),
                    "env_python": record.get("env_python"),
                    "env_python_version": record.get("env_python_version"),
                    "kind": record.get("kind") or record.get("type") or "unknown",
                    "origin": record.get("origin") or "local",
                    "yaml_sha256": record["yaml_sha256"],
                    "content_hash": record.get("content_hash") or record["yaml_sha256"],
                    "metadata": record.get("metadata") or {},
                    "distributions": record.get("distributions") or [],
                    "runtime_config": record.get("runtime_config") or {"mode": "venv"},
                    "remote_owner_id": record.get("remote_owner_id") or record.get("object_remote_owner_id"),
                    "remote_object_id": record.get("remote_object_id") or record.get("object_remote_object_id"),
                    "remote_version_id": record.get("remote_version_id"),
                }
            )
        items.sort(
            key=lambda item: (
                item["library_slug"],
                item["name"],
                item.get("local_version_id") or "",
            )
        )
        manifest = {"format_version": 1, "items": items}
        snapshot_hash = hashlib.sha256(
            m_json_contract.dumps(
                manifest,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return snapshot_hash, items

    def build_machine_library_snapshot_event(
        self,
        *,
        snapshot_hash: str,
        manifest_items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build a self-contained snapshot only after the manifest changed."""

        items = []
        for item in manifest_items:
            version = self.store.get_object_version(
                item["local_version_id"],
                include_yaml=True,
            )
            items.append({**item, "yaml": version["yaml"]})
        return {
            "id": f"machine_library_snapshot_{snapshot_hash}",
            "kind": "machine_library_snapshot",
            "payload": {
                "format_version": 1,
                "snapshot_hash": snapshot_hash,
                "items": items,
            },
        }

    def register_object(
        self,
        name: str,
        entrypoint: str,
        env: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Admit one durable Object/descendant registration mutation."""

        lease = self.lifecycle.reserve_current_or_root("registry_mutation")
        with lease:
            return self._register_object_admitted(
                name,
                entrypoint,
                env,
                **kwargs,
            )

    def _register_object_admitted(
        self,
        name: str,
        entrypoint: str,
        env: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Register an object and resolve remote-node signatures if needed."""

        kwargs = dict(kwargs)
        if kwargs.get("owner_id") is not None:
            kwargs["owner_id"] = self.resolve_user_ref(str(kwargs["owner_id"]))
        self._adopt_server_object_identity_for_publish(name, kwargs)
        cache_updates: list[RemoteSignatureCacheUpdate] = []
        resolved_signatures: dict[str, dict[str, Any]] = {}

        def resolve_for_registration(ref: dict[str, Any]) -> dict[str, Any]:
            normalized = self._normalize_remote_ref(ref)
            cache_key = json.dumps(normalized, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            if cache_key not in resolved_signatures:
                resolved_signatures[cache_key] = self._resolve_remote_signature(
                    normalized,
                    force=False,
                    cache_updates=cache_updates,
                )
            return resolved_signatures[cache_key]

        def write_signature_cache() -> None:
            for ref, signature, status, error in cache_updates:
                self.store.save_remote_signature_in_current_transaction(
                    ref,
                    signature,
                    status=status,
                    error=error,
                )

        return self.store.register_object(
            name,
            entrypoint,
            env,
            remote_signature_resolver=resolve_for_registration,
            remote_signature_cache_writer=write_signature_cache,
            **kwargs,
        )

    def _adopt_server_object_identity_for_publish(
        self,
        name: str,
        kwargs: dict[str, Any],
    ) -> None:
        """Attach server identity before the first local row for a canonical key."""

        if kwargs.get("object_id") is not None or kwargs.get("remote_object_id"):
            return
        if kwargs.get("origin", "local") != "local":
            return

        credentials = self.store.current_server_connection_credentials()
        if (
            credentials is None
            or not credentials.get("remote_connection_id")
            or not credentials.get("owner_id")
            or credentials.get("status") != "connected"
        ):
            return
        if not self._server_channel_is_live(credentials, supervise_heartbeat=False):
            LOGGER.info(
                "skipping server identity adoption for %s: server channel is not live",
                name,
                extra={
                    "spl_event": "server_identity_adoption_skipped_offline",
                    "object": name,
                    "connection_id": credentials.get("id"),
                },
            )
            return

        owner_id = validate_name(str(kwargs.get("owner_id") or credentials["owner_id"]))
        library = validate_name(str(kwargs.get("library") or DEFAULT_OBJECT_LIBRARY))
        object_name = validate_name(name)

        try:
            self.store.get_object(
                object_name,
                owner_id=owner_id,
                library=library,
                include_yaml=False,
            )
            return
        except KeyError:
            pass

        server = self._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
        )
        try:
            remote_current = server.get_object(
                object_name,
                include_yaml=False,
                owner_id=owner_id,
                library=library,
            )
        except ServerClientError as exc:
            if exc.status_code == 404:
                return
            if self._is_server_connectivity_error(exc):
                self._mark_server_channel_failure(credentials, error=exc)
                LOGGER.info(
                    "skipping server identity adoption for %s: server channel failed",
                    name,
                    extra={
                        "spl_event": "server_identity_adoption_skipped_offline",
                        "object": name,
                        "connection_id": credentials.get("id"),
                        "error": exc.message,
                    },
                )
                return
            raise

        kwargs["owner_id"] = owner_id
        kwargs["library"] = library
        kwargs.setdefault("remote_owner_id", remote_current.get("owner_id") or owner_id)
        kwargs.setdefault("remote_object_id", remote_current.get("id"))
        kwargs.setdefault("source_object_name", remote_current.get("name") or object_name)

    def resolve_remote_signature(
        self,
        ref: dict[str, Any],
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        """Resolve a NodeRemote reference through the central server.

        The current framework stores only ``url/name/version`` in DNodeRemote.
        The daemon treats that as a server object reference and caches the
        resolved call signature locally, so metadata extraction and worker
        import can proceed without repeatedly asking the server.
        """

        return cast(
            dict[str, Any],
            self.run_registry_mutation(
                self._resolve_remote_signature,
                ref,
                force=force,
                cache_updates=None,
            ),
        )

    def _resolve_remote_signature(
        self,
        ref: dict[str, Any],
        *,
        force: bool,
        cache_updates: list[RemoteSignatureCacheUpdate] | None,
    ) -> dict[str, Any]:
        normalized = self._normalize_remote_ref(ref)
        cache_ref = self._remote_signature_cache_ref(normalized)
        cached = self.store.get_remote_signature(cache_ref) if cache_ref.get("owner_id") else None
        if cached is not None and cached["status"] == "resolved" and not force:
            return cast(dict[str, Any], cached["signature"])

        credentials: dict[str, Any] | None = None
        try:
            credentials = self._credentials_for_remote_ref(normalized)
            cache_ref = self._remote_signature_cache_ref(normalized, credentials=credentials)
            server = self._server_client_for_credentials(
                credentials,
                request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
            )
            signature = server.object_signature(
                normalized["object_name"],
                version=self._remote_ref_version(normalized),
                owner_id=normalized.get("owner_id") or cache_ref.get("owner_id"),
                library=normalized.get("library"),
                function=normalized.get("function"),
            )
            canonical_owner_id = self._server_record_owner(signature)
            if canonical_owner_id is None:
                canonical_owner_id = cache_ref.get("owner_id")
            if canonical_owner_id is None:
                raise ValueError("central server signature did not return a canonical owner_id")
            canonical_owner_id = validate_name(str(canonical_owner_id))
            signature["remote"] = {
                "url": normalized["server_url"],
                "name": normalized["object_name"],
                "function": normalized.get("function"),
                "requested_version": normalized.get("version"),
                "version_id": signature.get("version_id"),
                "owner_id": canonical_owner_id,
                "library": self._server_record_library(signature) or normalized.get("library"),
            }
            cache_ref = {**normalized, "owner_id": canonical_owner_id}
            self._record_remote_signature_cache_update(
                cache_updates,
                cache_ref,
                signature,
                status="resolved",
                error=None,
            )
            return signature
        except Exception as exc:
            if isinstance(exc, ServerClientError) and self._is_server_connectivity_error(exc):
                self._mark_server_channel_failure(credentials, error=exc)
            if cache_ref.get("owner_id"):
                unavailable_signature = dict(cached["signature"]) if cached is not None else {}
                self._record_remote_signature_cache_update(
                    cache_updates,
                    cache_ref,
                    unavailable_signature,
                    status="unavailable",
                    error=repr(exc),
                )
            if cached is not None and cached.get("signature"):
                signature = dict(cached["signature"])
                signature["cache_status"] = "stale"
                signature["cache_error"] = repr(exc)
                return signature
            raise

    def _record_remote_signature_cache_update(
        self,
        cache_updates: list[RemoteSignatureCacheUpdate] | None,
        ref: dict[str, Any],
        signature: dict[str, Any],
        *,
        status: str,
        error: str | None,
    ) -> None:
        if cache_updates is None:
            self.store.save_remote_signature(
                ref,
                signature,
                status=status,
                error=error,
            )
            return
        cache_updates.append((dict(ref), dict(signature), status, error))

    def remote_signature_cache_record(self, ref: dict[str, Any]) -> dict[str, Any] | None:
        """Return the owner-concrete remote signature cache row for diagnostics."""

        normalized = self._normalize_remote_ref(ref)
        if normalized.get("owner_id") is not None:
            normalized["owner_id"] = self.resolve_user_ref(str(normalized["owner_id"]))
        cache_ref = self._remote_signature_cache_ref(normalized)
        if not cache_ref.get("owner_id"):
            return None
        return self.store.get_remote_signature(cache_ref)

    def resolve_remote_decomposition(self, ref: dict[str, Any]) -> dict[str, Any]:
        """Resolve a remote object graph through the connected central server."""

        normalized = self._normalize_remote_ref(ref)
        credentials = self._credentials_for_remote_ref(normalized)
        server = self._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
        )
        try:
            record = server.get_object(
                normalized["object_name"],
                version=self._remote_ref_version(normalized),
                include_yaml=False,
                owner_id=normalized.get("owner_id"),
                library=normalized.get("library"),
            )
        except ServerClientError as exc:
            if self._is_server_connectivity_error(exc):
                self._mark_server_channel_failure(credentials, error=exc)
            raise
        canonical_owner_id = self._server_record_owner(record)
        if canonical_owner_id is None:
            canonical_owner_id = self._remote_signature_cache_ref(
                normalized,
                credentials=credentials,
            ).get("owner_id")
        if canonical_owner_id is None:
            raise ValueError("central server object response did not return a canonical owner_id")
        canonical_owner_id = validate_name(str(canonical_owner_id))
        return {
            "decomposition": record.get("decomposition") or {},
            "object": record,
            "remote": {
                "url": normalized["server_url"],
                "name": normalized["object_name"],
                "function": normalized.get("function"),
                "requested_version": normalized.get("version"),
                "owner_id": canonical_owner_id,
                "library": self._server_record_library(record) or normalized.get("library"),
                "version_id": record.get("version_id"),
                "object_id": record.get("id"),
            },
        }

    def _normalize_remote_ref(self, ref: dict[str, Any]) -> dict[str, Any]:
        raw_url = str(ref.get("server_url") or ref.get("url") or "").rstrip("/")
        url, path_owner, path_library = self._split_remote_url(raw_url)
        object_name = str(ref.get("object_name") or ref.get("object") or ref.get("name") or "")
        raw_function = ref.get("function") or ref.get("entrypoint")
        if not object_name:
            raise ValueError("remote node requires object name")
        object_name, function = split_object_function_ref(object_name, raw_function)
        if not url:
            credentials = self.store.current_server_connection_credentials()
            if credentials is None:
                raise KeyError(
                    "active server connection is not found; remote Function/Pipeline "
                    "nodes require the daemon to be connected before resolution"
                )
            url = credentials["server_url"]
        return {
            "server_url": url,
            "owner_id": ref.get("owner_id") or ref.get("owner") or path_owner,
            "library": ref.get("library") or ref.get("library_slug") or path_library,
            "object_name": object_name,
            "function": function,
            "version": ref.get("version"),
            "version_id": ref.get("version_id"),
            "target_machine": ref.get("target_machine") or ref.get("target_machine_id"),
        }

    def _remote_signature_cache_ref(
        self,
        ref: dict[str, Any],
        *,
        credentials: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return the owner-concrete ref used for remote signature cache IO."""

        if ref.get("owner_id"):
            try:
                owner_id = validate_name(str(ref["owner_id"]))
            except ValueError:
                return {**ref, "owner_id": None}
            return {**ref, "owner_id": owner_id}
        credentials = credentials if credentials is not None else self.store.current_server_connection_credentials()
        if credentials is None:
            return ref
        if credentials.get("server_url", "").rstrip("/") != ref["server_url"].rstrip("/"):
            return ref
        credentials_owner_id = credentials.get("owner_id")
        if not credentials_owner_id:
            return ref
        return {**ref, "owner_id": str(credentials_owner_id)}

    def _split_remote_url(self, raw_url: str) -> tuple[str, str | None, str | None]:
        """Extract optional owner/library from NodeRemote.url.

        The current framework gives NodeRemote only ``url/name/version``.  To
        keep it usable for shared libraries without changing framework code, the
        daemon accepts both a plain server URL and a scoped URL:
        ``https://splime.io/api/owners/alice/libraries/math``.
        """

        if not raw_url:
            return "", None, None
        parsed = urlparse(raw_url)
        parts = [part for part in parsed.path.split("/") if part]
        owner_id = None
        library = None
        if len(parts) >= 4 and parts[-4] == "owners" and parts[-2] == "libraries":
            owner_id = unquote(parts[-3])
            library = unquote(parts[-1])
            parts = parts[:-4]
        elif len(parts) >= 2 and parts[-2] == "libraries":
            library = parts[-1]
            parts = parts[:-2]
        base_path = "/" + "/".join(parts) if parts else ""
        base_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                base_path.rstrip("/"),
                "",
                "",
                "",
            )
        ).rstrip("/")
        return base_url, owner_id, library

    def _credentials_for_remote_ref(self, ref: dict[str, Any]) -> dict[str, Any]:
        credentials = self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError(
                "active server connection is not found; remote Function/Pipeline "
                "node requires daemon server credentials before it can resolve "
                "or run"
            )
        credentials = self._require_live_server_channel_credentials(credentials)
        if credentials["server_url"].rstrip("/") != ref["server_url"].rstrip("/"):
            raise KeyError(
                f"remote node points to a different server than the active daemon connection: {ref['server_url']}"
            )
        return credentials

    def _remote_ref_version(self, ref: dict[str, Any]) -> int | None:
        version = ref.get("version")
        if version is None:
            return None
        version_text = str(version)
        # 0.2.0 dropped the legacy 'TODO' placeholder from this alias set
        # (docs/migration-0.2.0.md); only real aliases stay recognized.
        if version_text in {"", "latest", "current"}:
            return None
        try:
            return int(version_text)
        except (TypeError, ValueError):
            return None

    def start_remote_run(
        self,
        object_name: str,
        *,
        target_machine: str | None = None,
        object_owner_id: str | None = None,
        library: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        object_version_id: str | None = None,
        function: str | None = None,
        correlation_id: str | None = None,
        parent_run_id: str | None = None,
        context: dict[str, Any] | None = None,
        offline_policy: str | None = None,
        runtimes: str | dict[str, str] | None = None,
        runtime_port_adapters: dict[str, Any] | None = None,
        runtime_library_adapter_refs: dict[str, Any] | None = None,
        runtime_adapter_semantic_advisories: dict[str, Any] | None = None,
        adapter_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a server-side run through the idempotent direct route."""

        # Preserve the legacy pure-input preflight before consulting runtime
        # state.  Admission still occurs before any external work or mutation.
        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )
        return cast(
            dict[str, Any],
            self.run_registry_mutation(
                self._start_remote_run_admitted,
                object_name,
                target_machine=target_machine,
                object_owner_id=object_owner_id,
                library=library,
                args=args,
                kwargs=kwargs,
                output=output,
                timeout_seconds=timeout_seconds,
                version=version,
                object_version_id=object_version_id,
                function=function,
                correlation_id=correlation_id,
                parent_run_id=parent_run_id,
                context=context,
                offline_policy=offline_policy,
                runtimes=runtimes,
                runtime_port_adapters=runtime_port_adapters,
                runtime_library_adapter_refs=runtime_library_adapter_refs,
                runtime_adapter_semantic_advisories=runtime_adapter_semantic_advisories,
                adapter_policy=adapter_policy,
            ),
        )

    def _start_remote_run_admitted(
        self,
        object_name: str,
        *,
        target_machine: str | None = None,
        object_owner_id: str | None = None,
        library: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        object_version_id: str | None = None,
        function: str | None = None,
        correlation_id: str | None = None,
        parent_run_id: str | None = None,
        context: dict[str, Any] | None = None,
        offline_policy: str | None = None,
        runtimes: str | dict[str, str] | None = None,
        runtime_port_adapters: dict[str, Any] | None = None,
        runtime_library_adapter_refs: dict[str, Any] | None = None,
        runtime_adapter_semantic_advisories: dict[str, Any] | None = None,
        adapter_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Perform one already-admitted central Run creation."""

        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )
        object_name, function = split_object_function_ref(object_name, function)
        credentials = self._require_live_server_channel_credentials()
        resolved_offline_policy = offline_policy or (
            "fail_fast" if target_machine and target_machine != credentials["machine_id"] else "queue"
        )
        if resolved_offline_policy not in {"queue", "wait", "fail_fast"}:
            raise ValueError("offline_policy must be 'queue', 'wait', or 'fail_fast'")

        payload: dict[str, Any] = {
            "object": object_name,
            "version": version,
            "version_id": object_version_id,
            "args": args or [],
            "kwargs": kwargs or {},
            "output": output,
            "timeout_seconds": timeout_seconds,
            "offline_policy": resolved_offline_policy,
        }
        if function is not None:
            payload["function"] = function
        if target_machine:
            payload["target_machine_id"] = target_machine
        if object_owner_id:
            payload["object_owner_id"] = object_owner_id
        if library:
            payload["library"] = library
        if correlation_id:
            payload["correlation_id"] = correlation_id
        if parent_run_id:
            payload["parent_run_id"] = parent_run_id
        if context:
            payload["context"] = context
        if runtimes is not None:
            payload["runtimes"] = runtimes

        idempotency_key = (
            "remote_run_request_"
            + hashlib.sha256(f"spl.remote_run_request:{correlation_id}".encode("utf-8")).hexdigest()
            if correlation_id
            else "remote_run_request_" + uuid4().hex
        )
        if resolved_offline_policy == "fail_fast" and target_machine and target_machine != credentials["machine_id"]:
            self._raise_if_target_machine_offline(credentials, target_machine)
        server = self._server_client_for_credentials(credentials)
        if runtimes is not None:
            self._require_server_runtime_override_capability(server)
        if runtime_port_adapters is None:
            if runtime_adapter_semantic_advisories is not None:
                raise RuntimePortAdapterContractError(
                    "semantic_advisory_transport_missing",
                    "runtime adapter semantic advisories require runtime port transport",
                    stage="admission",
                )
            run = server.create_remote_run(
                payload,
                idempotency_key=idempotency_key,
            )
        else:
            document = normalize_wire_document(runtime_port_adapters, allow_content=True)
            policy = normalize_adapter_policy(adapter_policy)
            semantic_advisories = (
                None
                if runtime_adapter_semantic_advisories is None
                else normalize_runtime_adapter_semantic_advisories(runtime_adapter_semantic_advisories)
            )
            semantic_supported = self._require_server_runtime_port_adapter_capability(
                server,
                require_library_refs=runtime_library_adapter_refs is not None,
                require_semantic_override=bool(
                    semantic_advisories is not None
                    and any(item["state"] != "recommended" for item in semantic_advisories["bindings"])
                ),
            )
            remote_library_refs = (
                None
                if runtime_library_adapter_refs is None
                else self._translate_remote_library_adapter_refs(
                    server,
                    runtime_library_adapter_refs,
                )
            )
            remote_semantic_advisories = None
            if semantic_supported:
                remote_semantic_advisories = (
                    self._synthesize_remote_runtime_adapter_semantic_advisories(
                        server,
                        document,
                        remote_library_refs=remote_library_refs,
                    )
                    if semantic_advisories is None
                    else self._translate_remote_runtime_adapter_semantic_advisories(
                        semantic_advisories,
                        local_library_refs=runtime_library_adapter_refs,
                        remote_library_refs=remote_library_refs,
                    )
                )
            if (
                remote_semantic_advisories is not None
                and any(item["state"] != "recommended" for item in remote_semantic_advisories["bindings"])
                and not semantic_supported
            ):
                raise RuntimePortAdapterContractError(
                    "remote_semantic_override_unsupported",
                    "the connected SPL server does not support explicit runtime adapter semantic advisories",
                    stage="admission",
                )
            run = self._create_remote_runtime_adapter_run(
                server,
                request_id=idempotency_key,
                payload=payload,
                document=document,
                adapter_policy=policy,
                runtime_library_adapter_refs=remote_library_refs,
                runtime_adapter_semantic_advisories=remote_semantic_advisories,
            )
        # A separate sync may claim the new job for this machine. The keyed
        # POST and all of its retries have completed before the sync lock is
        # acquired by this background kick.
        self._kick_server_sync(credentials["id"])
        return run

    @staticmethod
    def _require_server_runtime_override_capability(server: ServerClientProtocol) -> None:
        """Reject remote runtime overrides before mutation on an older server."""

        version = server.get_server_version()
        declared = version.get("declared")
        contracts = declared.get("contracts") if isinstance(declared, dict) else None
        capabilities = contracts.get("daemon_server_capabilities") if isinstance(contracts, dict) else None
        if not isinstance(capabilities, list) or REMOTE_RUN_RUNTIME_OVERRIDES_CAPABILITY not in capabilities:
            raise RuntimeError(
                "the connected SPL server does not support remote Run runtime overrides; upgrade the server first"
            )

    @staticmethod
    def _require_server_runtime_port_adapter_capability(
        server: ServerClientProtocol,
        *,
        require_library_refs: bool = False,
        require_semantic_override: bool = False,
    ) -> bool:
        """Fail before central mutation unless the server advertises v1."""

        version = server.get_server_version()
        declared = version.get("declared")
        contracts = declared.get("contracts") if isinstance(declared, dict) else None
        capabilities = contracts.get("daemon_server_capabilities") if isinstance(contracts, dict) else None
        if not isinstance(capabilities, list) or RUNTIME_PORT_ADAPTERS_CAPABILITY not in capabilities:
            raise RuntimePortAdapterContractError(
                "remote_server_unsupported",
                "the connected SPL server does not support runtime port adapter admissions",
                stage="admission",
            )
        if require_library_refs and RUNTIME_LIBRARY_ADAPTER_REF_CAPABILITY not in capabilities:
            raise RuntimePortAdapterContractError(
                "remote_library_adapter_server_unsupported",
                "the connected SPL server does not support exact Library Adapter Run references",
                stage="admission",
            )
        semantic_supported = RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY in capabilities
        if require_semantic_override and not semantic_supported:
            raise RuntimePortAdapterContractError(
                "remote_semantic_override_unsupported",
                "the connected SPL server does not support explicit runtime adapter semantic advisories",
                stage="admission",
            )
        return semantic_supported

    def _translate_remote_library_adapter_refs(
        self,
        server: ServerClientProtocol,
        value: dict[str, Any],
    ) -> dict[str, Any]:
        """Re-read central identities and translate proven synced local refs."""

        refs = normalize_runtime_library_adapter_refs(value)
        translated: list[dict[str, Any]] = []
        for binding in refs["bindings"]:
            local_ref = {
                key: binding[key]
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
            try:
                link = self.store.library_adapter_remote_link(local_ref)
            except KeyError:
                link = None
            try:
                if link is None:
                    central = server.get_library_adapter_version(
                        local_ref["owner"],
                        local_ref["library"],
                        local_ref["adapter_id"],
                        local_ref["adapter_version_id"],
                        include_source=False,
                    )
                else:
                    central = server.get_library_adapter_version(
                        link["owner"],
                        link["library"],
                        link["adapter_id"],
                        link["adapter_version_id"],
                        include_source=False,
                    )
                central_ref = normalize_library_adapter_ref(
                    {
                        key: central[key]
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
            except Exception:
                raise RuntimePortAdapterContractError(
                    "remote_library_adapter_identity_unavailable",
                    "the exact central Library Adapter version is unavailable",
                    stage="admission",
                ) from None
            if link is None:
                matches = central_ref == local_ref
            else:
                matches = (
                    central_ref["owner"] == link["owner"]
                    and central_ref["library"] == link["library"]
                    and central_ref["adapter_id"] == link["adapter_id"]
                    and central_ref["adapter_version_id"] == link["adapter_version_id"]
                    and central_ref["name"] == local_ref["name"]
                    and central_ref["content_hash"] == local_ref["content_hash"]
                    and central_ref["signature_hash"] == local_ref["signature_hash"]
                )
            if not matches:
                raise RuntimePortAdapterContractError(
                    "remote_library_adapter_identity_mismatch",
                    "the central Library Adapter identity contradicts the exact local version",
                    stage="admission",
                )
            translated.append(
                {
                    "direction": binding["direction"],
                    "port": binding["port"],
                    **central_ref,
                }
            )
        return normalize_runtime_library_adapter_refs({"schema_version": 1, "bindings": translated})

    @staticmethod
    def _translate_remote_runtime_adapter_semantic_advisories(
        value: dict[str, Any],
        *,
        local_library_refs: dict[str, Any] | None,
        remote_library_refs: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Translate only proven Library adapter identities in advisory evidence."""

        advisories = normalize_runtime_adapter_semantic_advisories(value)
        if local_library_refs is None:
            return advisories
        if remote_library_refs is None:
            raise RuntimePortAdapterContractError(
                "remote_semantic_advisory_identity",
                "remote Library Adapter advisory identity is unavailable",
                stage="admission",
            )
        local_refs = normalize_runtime_library_adapter_refs(local_library_refs)
        remote_refs = normalize_runtime_library_adapter_refs(remote_library_refs)
        local_by_identity = {(item["direction"], item["port"]): item for item in local_refs["bindings"]}
        remote_by_identity = {(item["direction"], item["port"]): item for item in remote_refs["bindings"]}
        if set(local_by_identity) != set(remote_by_identity):
            raise RuntimePortAdapterContractError(
                "remote_semantic_advisory_identity",
                "remote Library Adapter advisory bindings changed during translation",
                stage="admission",
            )
        translated = []
        for item in advisories["bindings"]:
            identity = (item["direction"], item["port"])
            local_ref = local_by_identity.get(identity)
            if local_ref is None:
                translated.append(item)
                continue
            if item["adapter_id"] != local_ref["adapter_id"]:
                raise RuntimePortAdapterContractError(
                    "remote_semantic_advisory_identity",
                    "Library Adapter advisory does not match its exact local reference",
                    stage="admission",
                )
            translated.append({**item, "adapter_id": remote_by_identity[identity]["adapter_id"]})
        return normalize_runtime_adapter_semantic_advisories({"schema_version": 1, "bindings": translated})

    @staticmethod
    def _synthesize_remote_runtime_adapter_semantic_advisories(
        server: ServerClientProtocol,
        document: dict[str, Any],
        *,
        remote_library_refs: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Re-read exact remote facts and synthesize explicit API/embedded intent."""

        refs: dict[str, Any] = (
            {"schema_version": 1, "bindings": []}
            if remote_library_refs is None
            else normalize_runtime_library_adapter_refs(remote_library_refs)
        )
        refs_by_identity = {(item["direction"], item["port"]): item for item in refs["bindings"]}
        exact_versions: dict[str, dict[str, Any]] = {}
        advisories: list[dict[str, Any]] = []
        for binding in document["bindings"]:
            source = binding["resolution_source"]
            if source == "system_default":
                continue
            identity = (binding["direction"], binding["port"])
            ref = refs_by_identity.get(identity)
            if ref is None:
                adapter = binding["adapter"]
                adapter_id = str(adapter["id"])
                adapter_type = str(adapter["key"]).rpartition("@")[0]
                category = runtime_adapter_semantic_category(adapter_id)
                state = runtime_adapter_semantic_state(
                    binding.get("semantic_type"),
                    adapter_type,
                    adapter_id=adapter_id,
                )
            else:
                version_id = str(ref["adapter_version_id"])
                exact = exact_versions.get(version_id)
                if exact is None:
                    raw = server.get_library_adapter_version(
                        ref["owner"],
                        ref["library"],
                        ref["adapter_id"],
                        ref["adapter_version_id"],
                        include_source=False,
                    )
                    if not isinstance(raw, dict) or any(
                        raw.get(key) != ref[key]
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
                    ):
                        raise RuntimePortAdapterContractError(
                            "remote_library_adapter_identity_mismatch",
                            "the exact central Library Adapter facts are unavailable for semantic review",
                            stage="admission",
                        )
                    exact = raw
                    exact_versions[version_id] = exact
                adapter_type_value = exact.get("semantic_type")
                if not isinstance(adapter_type_value, str) or not adapter_type_value:
                    raise RuntimePortAdapterContractError(
                        "remote_library_adapter_semantic_type",
                        "the exact central Library Adapter semantic type is unavailable",
                        stage="admission",
                    )
                adapter_type = adapter_type_value
                category_value = exact.get("semantic_category")
                category = category_value if isinstance(category_value, str) else None
                adapter_id = str(ref["adapter_id"])
                state = library_adapter_semantic_advisory(
                    binding.get("semantic_type"),
                    adapter_type,
                    category,
                )
            acknowledgement_source = (
                None
                if state == "recommended"
                else ("embedded_contract" if source == "preset" else "api_explicit_selection")
            )
            advisories.append(
                {
                    "direction": binding["direction"],
                    "port": binding["port"],
                    "adapter_id": adapter_id,
                    "port_semantic_type": binding.get("semantic_type"),
                    "adapter_semantic_type": adapter_type,
                    "adapter_semantic_category": category,
                    "state": state,
                    "acknowledged": acknowledgement_source is not None,
                    "acknowledgement_source": acknowledgement_source,
                }
            )
        if not advisories:
            return None
        return normalize_runtime_adapter_semantic_advisories({"schema_version": 1, "bindings": advisories})

    @staticmethod
    def _runtime_admission_projection_matches(
        admission: dict[str, Any],
        *,
        request_id: str,
        request_digest_sha256: str,
        document: dict[str, Any],
        adapter_policy: dict[str, str],
        runtime_library_adapter_refs: dict[str, Any] | None,
        runtime_adapter_semantic_advisories: dict[str, Any] | None,
    ) -> bool:
        expected_schema = (
            3
            if runtime_adapter_semantic_advisories is not None
            else (2 if runtime_library_adapter_refs is not None else 1)
        )
        return (
            admission.get("schema_version") == expected_schema
            and admission.get("request_id") == request_id
            and admission.get("request_digest_sha256") == request_digest_sha256
            and admission.get("runtime_port_adapters") == document
            and admission.get("adapter_policy") == adapter_policy
            and (
                runtime_library_adapter_refs is None
                or admission.get(
                    "runtime_library_adapter_refs",
                    {"schema_version": 1, "bindings": []},
                )
                == runtime_library_adapter_refs
            )
            and (
                runtime_adapter_semantic_advisories is None
                or admission.get("runtime_adapter_semantic_advisories") == runtime_adapter_semantic_advisories
            )
        )

    def _create_remote_runtime_adapter_run(
        self,
        server: ServerClientProtocol,
        *,
        request_id: str,
        payload: dict[str, Any],
        document: dict[str, Any],
        adapter_policy: dict[str, str],
        runtime_library_adapter_refs: dict[str, Any] | None = None,
        runtime_adapter_semantic_advisories: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create, upload and atomically finalize one adapted remote Run."""

        flat_document = server_admission_document(document)
        normalized_library_refs = (
            None
            if runtime_library_adapter_refs is None
            else normalize_runtime_library_adapter_refs(runtime_library_adapter_refs)
        )
        normalized_semantic_advisories = (
            None
            if runtime_adapter_semantic_advisories is None
            else normalize_runtime_adapter_semantic_advisories(runtime_adapter_semantic_advisories)
        )
        schema_version = (
            3 if normalized_semantic_advisories is not None else (2 if normalized_library_refs is not None else 1)
        )
        admission_library_refs = (
            normalized_library_refs
            if normalized_library_refs is not None
            else ({"schema_version": 1, "bindings": []} if schema_version in {2, 3} else None)
        )
        request = {
            "schema_version": schema_version,
            "request_id": request_id,
            "run": payload,
            "runtime_port_adapters": flat_document,
            "adapter_policy": adapter_policy,
        }
        if admission_library_refs is not None:
            request["runtime_library_adapter_refs"] = admission_library_refs
        if normalized_semantic_advisories is not None:
            request["runtime_adapter_semantic_advisories"] = normalized_semantic_advisories
        canonical_request = {
            "schema_version": schema_version,
            "run": {key: value for key, value in payload.items() if key != "access_token"},
            "runtime_port_adapters": flat_document,
            "adapter_policy": adapter_policy,
        }
        if admission_library_refs is not None:
            canonical_request["runtime_library_adapter_refs"] = admission_library_refs
        if normalized_semantic_advisories is not None:
            canonical_request["runtime_adapter_semantic_advisories"] = normalized_semantic_advisories
        request_digest_sha256 = hashlib.sha256(m_json_contract.dumps(canonical_request).encode("utf-8")).hexdigest()
        input_bodies = {
            item["name"]: base64.b64decode(item["content_base64"], validate=True) for item in document["inputs"]
        }
        bundle = document["custom_bundle"]
        bundle_body = None if bundle is None else base64.b64decode(bundle["content_base64"], validate=True)

        admission: dict[str, Any] | None = None
        try:
            try:
                admission = server.create_remote_run_admission(request)
            except ServerClientError as original:
                if original.status_code not in {502, 503, 504}:
                    raise
                try:
                    admission = server.get_remote_run_admission(request_id)
                except Exception:
                    raise original
            if not self._runtime_admission_projection_matches(
                admission,
                request_id=request_id,
                request_digest_sha256=request_digest_sha256,
                document=flat_document,
                adapter_policy=adapter_policy,
                runtime_library_adapter_refs=admission_library_refs,
                runtime_adapter_semantic_advisories=normalized_semantic_advisories,
            ):
                raise RuntimePortAdapterContractError(
                    "remote_admission_mismatch",
                    "central server returned a different runtime adapter admission",
                    stage="admission",
                )
            if admission.get("state") in {"cancelled", "expired"}:
                raise RuntimePortAdapterContractError(
                    "remote_admission_inactive",
                    "runtime adapter admission is no longer active",
                    stage="admission",
                )

            if admission.get("state") != "finalized":
                items = admission.get("items")
                if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
                    raise RuntimePortAdapterContractError(
                        "remote_admission_shape",
                        "central server returned invalid runtime admission items",
                        stage="admission",
                    )
                for item in items:
                    if item.get("uploaded") is True:
                        continue
                    role = item.get("role")
                    name = item.get("name")
                    try:
                        if role == "input" and isinstance(name, str) and name in input_bodies:
                            server.upload_remote_run_admission_input(
                                request_id,
                                name,
                                input_bodies[name],
                                size=item["size"],
                                sha256=item["sha256"],
                            )
                        elif role == "custom_bundle" and bundle is not None and bundle_body is not None:
                            server.upload_remote_run_admission_custom_bundle(
                                request_id,
                                bundle_body,
                                size=item["size"],
                                sha256=item["sha256"],
                            )
                        else:
                            raise RuntimePortAdapterContractError(
                                "remote_admission_item",
                                "central server requested an undeclared runtime admission item",
                                stage="upload",
                            )
                    except ServerClientError as original:
                        if original.status_code not in {502, 503, 504}:
                            raise
                        recovered = server.get_remote_run_admission(request_id)
                        recovered_items = recovered.get("items")
                        uploaded = isinstance(recovered_items, list) and any(
                            isinstance(candidate, dict)
                            and candidate.get("role") == role
                            and candidate.get("name") == name
                            and candidate.get("uploaded") is True
                            for candidate in recovered_items
                        )
                        if not uploaded:
                            raise original
                try:
                    admission = server.finalize_remote_run_admission(request_id)
                except ServerClientError as original:
                    if original.status_code not in {502, 503, 504}:
                        raise
                    recovered = server.get_remote_run_admission(request_id)
                    if recovered.get("state") != "finalized":
                        raise original
                    admission = recovered
            run_id = admission.get("run_id")
            if admission.get("state") != "finalized" or not isinstance(run_id, str) or not run_id:
                raise RuntimePortAdapterContractError(
                    "remote_admission_not_finalized",
                    "central server did not prove atomic runtime admission finalization",
                    stage="admission",
                )
            return server.get_remote_run(run_id)
        except Exception:
            if admission is not None and admission.get("state") == "staging":
                try:
                    current = server.get_remote_run_admission(request_id)
                    if current.get("state") == "staging":
                        server.cancel_remote_run_admission(request_id)
                except Exception:
                    pass
            raise

    def _raise_if_target_machine_offline(
        self,
        credentials: dict[str, Any],
        target_machine: str,
    ) -> None:
        """Fail before queueing when the caller expects an immediate remote result."""

        server = self._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
        )
        machines = server.list_machines()
        machine = next((item for item in machines if item.get("id") == target_machine), None)
        if machine is None:
            return
        if machine.get("status") == "online":
            return
        raise RuntimeError(
            "target machine "
            f"{target_machine!r} is {machine.get('status') or 'offline'}; "
            "the run was not queued. Use "
            "client.submit(..., offline_policy='queue') to register the task "
            "and poll it later."
        )

    @staticmethod
    def _server_record_string(value: Any, *keys: str) -> str | None:
        if isinstance(value, str) and value:
            return value
        if isinstance(value, dict):
            for key in keys:
                item = value.get(key)
                if item is not None and str(item):
                    return str(item)
        return None

    @classmethod
    def _server_record_names(cls, record: dict[str, Any]) -> set[str]:
        names = {record.get("display_name"), record.get("name"), record.get("object_name")}
        return {str(name) for name in names if isinstance(name, str) and name}

    @classmethod
    def _server_record_owner(cls, record: dict[str, Any]) -> str | None:
        return cls._server_record_string(record.get("owner_id")) or cls._server_record_string(
            record.get("owner"),
            "id",
            "owner_id",
            "name",
        )

    @classmethod
    def _server_record_library(cls, record: dict[str, Any]) -> str | None:
        return cls._server_record_string(record.get("library"), "slug", "name", "display_name")

    @classmethod
    def _server_record_version(cls, record: dict[str, Any]) -> str | None:
        current = record.get("current_version")
        if isinstance(current, dict):
            for key in ("version", "number", "label", "name"):
                value = current.get(key)
                if value is not None and str(value):
                    return str(value)
        for key in ("version", "version_label"):
            value = record.get(key)
            if value is not None and str(value):
                return str(value)
        return None

    @classmethod
    def _server_candidate_label(cls, record: dict[str, Any]) -> str:
        library = cls._server_record_library(record) or "<unknown library>"
        owner = cls._server_record_owner(record)
        version = cls._server_record_version(record)
        details = []
        if owner is not None:
            details.append("owner {}".format(owner))
        if version is not None:
            details.append("v{}".format(version))
        return "{} ({})".format(library, ", ".join(details)) if details else library

    def _resolve_pull_server_ref(
        self,
        server: ServerClientProtocol,
        name: str,
        *,
        owner_id: str | None,
        library: str | None,
    ) -> tuple[str, str | None, str | None]:
        if owner_id is not None or library is not None:
            return name, owner_id, library
        candidates = [
            record
            for record in server.list_objects(compact=True)
            if isinstance(record, dict) and name in self._server_record_names(record)
        ]
        if len(candidates) == 1:
            record = candidates[0]
            return (
                self._server_record_string(record.get("name")) or name,
                self._server_record_owner(record),
                self._server_record_library(record),
            )
        if candidates:
            choices = ", ".join(self._server_candidate_label(record) for record in candidates)
            raise ValueError(
                "{!r} is not registered locally; found on the server in: {}. "
                "Pass library=... and owner=... to disambiguate, for example "
                "client.pull({!r}, library='...').".format(name, choices, name)
            )
        raise KeyError(
            "{!r} is not registered locally; no accessible server object named {!r}. "
            "Run client.objects(scope='server') to inspect the accessible catalog, or pass "
            "owner=... and library=... if you know the server scope.".format(name, name)
        )

    def _remote_version_ref(
        self,
        remote_version: dict[str, Any],
        *,
        fallback_owner_id: str | None,
        fallback_library: str | None,
        fallback_name: str,
    ) -> str:
        owner_id = self._server_version_owner_id(remote_version, fallback_owner_id)
        library = self._server_version_library(remote_version, fallback_library)
        name = validate_name(str(remote_version.get("name") or fallback_name))
        version = int(remote_version.get("version") or 0)
        return f"{owner_id}/{library}/{name}@v{version}"

    def _remote_version_is_cached_by_content(
        self,
        remote_version: dict[str, Any],
        *,
        owner_id: str,
        library: str,
        name: str,
    ) -> bool:
        remote_hash = remote_version.get("content_hash")
        if not remote_hash:
            return False
        try:
            local_versions = self.store.list_object_versions(
                name,
                owner_id=owner_id,
                library=library,
            )
        except KeyError:
            return False
        return any(item.get("content_hash") == remote_hash for item in local_versions)

    def _ambiguous_local_bare_names(self, names: set[str]) -> set[str]:
        buckets: dict[str, set[str]] = {name: set() for name in names}
        for identity in self.store.list_object_identities():
            name = str(identity.get("name") or "")
            if name in buckets:
                buckets[name].add(str(identity["canonical_name"]))
        return {name for name, canonicals in buckets.items() if len(canonicals) > 1}

    def pull_server_object(
        self,
        object_name: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        all_versions: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Mirror one accessible server object into the local registry."""

        if dry_run:
            return self._pull_server_object_admitted(
                object_name,
                version=version,
                owner_id=owner_id,
                library=library,
                all_versions=all_versions,
                dry_run=True,
            )
        return cast(
            dict[str, Any],
            self.run_registry_mutation(
                self._pull_server_object_admitted,
                object_name,
                version=version,
                owner_id=owner_id,
                library=library,
                all_versions=all_versions,
                dry_run=False,
            ),
        )

    def _pull_server_object_admitted(
        self,
        object_name: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        all_versions: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Perform one read-only plan or already-admitted mirror mutation."""

        credentials = self.store.current_server_connection_credentials()
        try:
            credentials = self._require_live_server_channel_credentials(credentials)
        except (KeyError, ServerOfflineError) as exc:
            raise KeyError(self._pull_requires_server_connection_message()) from exc

        server = self._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
        )
        resolved_name, resolved_owner_id, resolved_library = self._resolve_pull_server_ref(
            server,
            validate_name(str(object_name)),
            owner_id=owner_id,
            library=library,
        )
        before_ambiguous = self._ambiguous_local_bare_names({resolved_name})
        remote_current = server.get_object(
            resolved_name,
            version=version,
            include_yaml=False,
            owner_id=resolved_owner_id,
            library=resolved_library,
        )
        remote_object_id = remote_current.get("id")
        if all_versions:
            remote_versions = server.list_object_versions(
                resolved_name,
                include_yaml=False,
                owner_id=resolved_owner_id,
                library=resolved_library,
            )
            if not remote_versions:
                remote_versions = [remote_current]
        else:
            remote_versions = [remote_current]

        if not dry_run:
            self._ensure_server_object_envs(remote_versions)
        pulled: list[str] = []
        skipped: list[str] = []
        failed: list[str] = []
        imported_names = {resolved_name}
        for remote_version in sorted(remote_versions, key=lambda item: int(item.get("version") or 0)):
            remote_name = validate_name(str(remote_version.get("name") or resolved_name))
            imported_names.add(remote_name)
            remote_owner_id = self._server_version_owner_id(remote_version, resolved_owner_id)
            remote_library = self._server_version_library(remote_version, resolved_library)
            version_ref = self._remote_version_ref(
                remote_version,
                fallback_owner_id=resolved_owner_id,
                fallback_library=resolved_library,
                fallback_name=resolved_name,
            )
            already_imported = False
            remote_version_id = remote_version.get("version_id")
            if remote_version_id is not None:
                already_imported = (
                    self.store.get_object_by_remote_version(
                        str(remote_version_id),
                        include_yaml=False,
                    )
                    is not None
                )
            if not already_imported:
                already_imported = self._remote_version_is_cached_by_content(
                    remote_version,
                    owner_id=remote_owner_id,
                    library=remote_library,
                    name=remote_name,
                )
            if dry_run:
                if already_imported:
                    skipped.append(version_ref)
                else:
                    pulled.append(version_ref)
                continue
            try:
                self._import_reconcile_remote_version(
                    remote_version,
                    owner_id=remote_owner_id,
                    library=remote_library,
                    remote_object_id=remote_object_id,
                    server=server,
                )
            except Exception:
                failed.append(version_ref)
                continue
            if already_imported:
                skipped.append(version_ref)
            else:
                pulled.append(version_ref)

        after_ambiguous = self._ambiguous_local_bare_names(imported_names)
        return {
            "pulled": pulled,
            "skipped": skipped,
            "failed": failed,
            "ambiguous_names": sorted(after_ambiguous - before_ambiguous),
        }

    def import_server_object(
        self,
        object_name: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any]:
        """Pull missing server object versions into the local registry.

        The first import of a server object mirrors the full version history.
        Later refreshes are cheap: the daemon checks the server's current
        version id first and downloads YAML only when that version is absent
        locally.
        """

        credentials = self.store.current_server_connection_credentials()
        if credentials is None:
            raise KeyError(f"object is not registered locally and active server connection is not found: {object_name}")
        credentials = self._require_live_server_channel_credentials(credentials)

        server = self._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
        )

        server_scope: dict[str, Any] = {}
        if owner_id is not None:
            server_scope["owner_id"] = owner_id
        if library is not None:
            server_scope["library"] = library

        remote_current = server.get_object(
            object_name,
            version=version,
            include_yaml=False,
            **server_scope,
        )
        resolved_from = remote_current.get("resolved_from")

        def with_resolution(record: dict[str, Any]) -> dict[str, Any]:
            if not isinstance(resolved_from, dict):
                return record
            return {**record, "resolved_from": dict(resolved_from)}

        self._ensure_server_object_envs([remote_current])
        remote_object_id = remote_current["id"]
        remote_version_id = remote_current["version_id"]
        existing_current = self.store.get_object_by_remote_version(
            remote_version_id,
            include_yaml=False,
        )
        if existing_current is not None:
            return {
                "source": "server",
                "name": object_name,
                "remote_object_id": remote_object_id,
                "current_version": with_resolution(existing_current),
                "versions": [existing_current],
                "refreshed": False,
            }

        if self.store.list_object_versions_by_remote_object(remote_object_id):
            remote_versions = [
                server.get_object(
                    object_name,
                    version=version,
                    include_yaml=True,
                    **server_scope,
                )
            ]
        else:
            remote_versions = server.list_object_versions(
                object_name,
                include_yaml=True,
                **server_scope,
            )
        if not remote_versions:
            raise KeyError(f"object is not registered on server: {object_name}")

        self._ensure_server_object_envs(remote_versions)

        imported = []
        for remote_version in sorted(remote_versions, key=lambda item: int(item["version"])):
            existing_version = self.store.get_object_by_remote_version(
                remote_version["version_id"],
                include_yaml=False,
            )
            if existing_version is not None:
                imported.append(existing_version)
                continue

            imported.append(
                self._import_reconcile_remote_version(
                    remote_version,
                    owner_id=self._server_version_owner_id(remote_version, owner_id),
                    library=self._server_version_library(remote_version, library),
                    remote_object_id=remote_version.get("id") or remote_object_id,
                    server=server,
                )
            )

        current_version = self.store.get_object_by_remote_version(
            remote_version_id,
            include_yaml=False,
        )
        if current_version is None and imported:
            # Identical-content server versions dedupe to one local row.  A
            # colliding remote_version_id is logged and intentionally not linked.
            current_version = imported[-1]
        if current_version is None:
            raise KeyError(f"server object version was not imported: {remote_version_id}")

        return {
            "source": "server",
            "name": object_name,
            "remote_object_id": remote_object_id,
            "current_version": with_resolution(current_version),
            "versions": imported,
            "refreshed": True,
        }

    def _server_version_owner_id(self, version: dict[str, Any], fallback: str | None = None) -> str:
        owner_id = self._server_record_owner(version) or fallback
        return validate_name(str(owner_id or DEFAULT_OBJECT_OWNER_ID))

    def _server_version_library(self, version: dict[str, Any], fallback: str | None = None) -> str:
        library = version.get("library_slug") or version.get("library") or fallback
        if isinstance(library, dict):
            library = library.get("slug") or library.get("name")
        return validate_name(str(library or DEFAULT_OBJECT_LIBRARY))

    def _ensure_server_object_envs(self, remote_versions: list[dict[str, Any]]) -> None:
        """Map server env names to local Python executables when first seen."""

        for env in sorted({item.get("env") or "default" for item in remote_versions}):
            if not self._has_local_env(env):
                self._register_auto_server_env(env)

    def _register_auto_server_env(self, name: str) -> dict[str, Any]:
        try:
            base_python = self.store.get_env("default")["python"]
            if not Path(str(base_python)).expanduser().exists():
                raise KeyError("default environment python is missing")
        except KeyError:
            base_python = sys.executable
        return self.register_env(name, base_python)

    def register_env(self, name: str, python: str | None = None) -> dict[str, Any]:
        """Admit one exact environment registry mutation."""

        lease = self.lifecycle.reserve_current_or_root("registry_mutation")
        with lease:
            return self.store.register_env(name, python)

    def refresh_server_object_if_available(
        self,
        object_name: str,
        *,
        version: int | None = None,
        owner_id: str | None = None,
        library: str | None = None,
    ) -> dict[str, Any] | None:
        """Best-effort server refresh before a local auto-sourced run.

        ``source="auto"`` must keep local objects runnable when the central
        server is offline or does not have the object.  Operational problems
        that mean "cannot check for updates" are therefore soft failures here;
        semantic problems, such as a missing local environment for a real server
        object, still surface to the caller.
        """

        credentials = self.store.current_server_connection_credentials()
        if not self._server_channel_is_live(credentials, supervise_heartbeat=False):
            return None

        try:
            return self.import_server_object(
                object_name,
                version=version,
                owner_id=owner_id,
                library=library,
            )
        except ServerOfflineError:
            return None
        except KeyError as exc:
            message = str(exc)
            if "active server connection is not found" in message or "object is not registered on server" in message:
                return None
            raise
        except ServerClientError as exc:
            if exc.status_code in {404, 502, 503, 504}:
                return None
            raise

    def _has_local_env(self, name: str) -> bool:
        try:
            env = self.store.get_env(name)
        except KeyError:
            return False
        python = env.get("python")
        return bool(python) and Path(str(python)).expanduser().exists()

    def sync_once(
        self,
        *,
        connection_id: str | None = None,
        extra_events: list[dict[str, Any]] | None = None,
        probe_server_channel: bool = False,
    ) -> dict[str, Any]:
        """Run one lifecycle-coordinated sync or draining no-claim flush."""

        lease, claim_jobs = self.lifecycle.begin_sync()
        lease.activate()
        lease.attach_current_thread()
        request_state = {"sent": False}
        try:
            result = self._sync_once_admitted(
                connection_id=connection_id,
                extra_events=extra_events,
                probe_server_channel=probe_server_channel,
                claim_jobs=claim_jobs,
                sync_lease=lease,
                note_request_sent=lambda: request_state.__setitem__("sent", True),
            )
            if not claim_jobs:
                if result.get("connected") is False or result.get("partial"):
                    raise LifecycleError(
                        "server_no_claim_boundary_unproven",
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        operation="sync_flush",
                        retryable=True,
                    )
                self.lifecycle.mark_claim_boundary_proven()
            return result
        except LifecycleError:
            if not claim_jobs:
                try:
                    self.lifecycle.mark_claim_boundary_failed(outcome_unknown=request_state["sent"])
                except LifecycleError:
                    pass
            raise
        except Exception:
            if not claim_jobs:
                try:
                    self.lifecycle.mark_claim_boundary_failed(outcome_unknown=request_state["sent"])
                except LifecycleError:
                    pass
            raise
        finally:
            lease.detach_current_thread()
            lease.complete(seal_lineage=True)

    def _sync_once_admitted(
        self,
        *,
        connection_id: str | None = None,
        extra_events: list[dict[str, Any]] | None = None,
        probe_server_channel: bool = False,
        claim_jobs: bool,
        sync_lease: LifecycleWorkLease,
        note_request_sent: Callable[[], None],
    ) -> dict[str, Any]:
        """Renew the lease, then exchange bounded event batches and jobs."""

        credentials = (
            self.store.get_server_connection_credentials(connection_id)
            if connection_id is not None
            else self.store.current_server_connection_credentials()
        )
        if credentials is None:
            return {
                "connected": False,
                "event_results": [],
                "jobs": [],
                "sync": self.sync_visibility.summary(),
            }
        credentials = {
            **credentials,
            "capabilities": self._authoritative_server_capabilities(credentials.get("capabilities")),
        }

        if probe_server_channel and credentials.get("status") == "needs_reconnect":
            credentials = self._reconnect_server_credentials(credentials)

        if not probe_server_channel and not self._server_channel_is_live(
            credentials,
            supervise_heartbeat=False,
        ):
            return {
                "connected": False,
                "offline": True,
                "event_results": [],
                "jobs": [],
                "error": SERVER_OFFLINE_MESSAGE,
                "code": SERVER_UNREACHABLE_CODE,
                "sync": self.sync_visibility.summary(),
            }

        if not credentials.get("remote_connection_id"):
            try:
                credentials = self._restore_pending_server_connection(credentials)
            except ServerClientError as exc:
                if not self._is_server_connectivity_error(exc):
                    raise
                self._mark_server_channel_failure(credentials, error=exc)
                self.store.record_server_connection_error(
                    credentials["id"],
                    status="connect_failed",
                    error=exc.message,
                )
                return {
                    "connected": False,
                    "offline": True,
                    "event_results": [],
                    "jobs": [],
                    "error": SERVER_OFFLINE_MESSAGE,
                    "detail": exc.message,
                    "sync": self.sync_visibility.summary(),
                }
            self._mark_server_channel_success(credentials)

        snapshot_hash, manifest_items = self.build_machine_library_snapshot_manifest()
        with self._server_sync_lock:
            # Lock order: _server_sync_lock -> repository/store locks.  Each
            # read -> bounded request -> acknowledgement cycle remains inside
            # this section, so a second caller cannot resend an in-flight row.
            # The explicit client timeout bounds every network hold to 15s.
            server = self._server_client_for_credentials(
                credentials,
                request_timeout_seconds=SYNC_REQUEST_TIMEOUT_SECONDS,
            )
            if not claim_jobs:
                try:
                    validate_deployed_no_claim_capability(server.get_server_version())
                except LifecycleError:
                    raise
                except Exception as exc:
                    raise LifecycleError(
                        "server_no_claim_capability_unproven",
                        HTTPStatus.PRECONDITION_FAILED,
                        operation="sync_flush",
                    ) from exc
            credentials = self._renew_server_lease(server, credentials)
            credentials_owner_id = str(credentials.get("owner_id") or DEFAULT_OBJECT_OWNER_ID)
            sync_before = self.sync_visibility.summary()
            snapshot_event = None
            if snapshot_hash != credentials.get("last_library_snapshot_hash"):
                remote_snapshot_hash = self._server_latest_library_snapshot_hash(
                    server,
                    credentials["machine_id"],
                )
                if remote_snapshot_hash == snapshot_hash:
                    self.store.record_server_connection_library_snapshot(
                        credentials["id"],
                        snapshot_hash=snapshot_hash,
                    )
                else:
                    snapshot_event = self.build_machine_library_snapshot_event(
                        snapshot_hash=snapshot_hash,
                        manifest_items=manifest_items,
                    )

            bootstrap_events: list[dict[str, Any]] = []
            if snapshot_event is not None:
                bootstrap_events.append(snapshot_event)
            bootstrap_events.extend(extra_events or [])
            attempted_ids: set[str] = set()
            event_results: list[dict[str, Any]] = []
            jobs: list[dict[str, Any]] = []
            batches = 0
            sent_request = False
            drain_error: str | None = None
            drain_status_code: int | None = None
            snapshot_event_id = snapshot_event["id"] if snapshot_event is not None else None
            held_count = 0
            held_owner_ids: set[str] = set()
            adopted_count = 0

            while True:
                if bootstrap_events:
                    events = self._take_ephemeral_sync_batch(bootstrap_events)
                    pending_ids: set[str] = set()
                else:
                    events, batch_held_count, batch_held_owners, batch_adopted = self._next_pending_sync_batch(
                        owner_id=credentials_owner_id,
                        attempted_ids=attempted_ids,
                    )
                    held_count = max(held_count, batch_held_count)
                    held_owner_ids.update(batch_held_owners)
                    adopted_count += batch_adopted
                    pending_ids = {str(event["id"]) for event in events}
                    attempted_ids.update(pending_ids)

                if not events and sent_request:
                    break
                claim_id = self._sync_batch_claim_id(events)
                wire_events = [self._sync_event_for_wire(event) for event in events]
                try:
                    if batches:
                        credentials = self._renew_server_lease(server, credentials)
                    note_request_sent()
                    sync_kwargs = {
                        "connection_id": credentials["remote_connection_id"],
                        "machine_id": credentials["machine_id"],
                        "heartbeat_interval_seconds": self._safe_heartbeat_interval(credentials),
                        "events": wire_events,
                        "capabilities": self._authoritative_server_capabilities(credentials.get("capabilities")),
                        "claim_id": claim_id,
                    }
                    if claim_jobs:
                        # Exact legacy wire behavior: an old client omits the
                        # additive field and may claim one job.
                        response = server.sync(**sync_kwargs)
                    else:
                        response = server.sync(**sync_kwargs, claim_jobs=False)
                except ServerClientError as exc:
                    collision_event = (
                        next(
                            (event for event in events if str(event.get("id")) == exc.event_id),
                            None,
                        )
                        if is_sync_event_identity_collision_error(exc)
                        else None
                    )
                    if collision_event is not None:
                        sent_request = True
                        batches += 1
                        self._mark_server_channel_success(credentials)
                        assert exc.event_id is not None
                        if exc.event_id in pending_ids:
                            self.store.mark_sync_event_failed(
                                exc.event_id,
                                exc.message,
                                retryable=False,
                            )
                        event_results.append(
                            {
                                "event_id": exc.event_id,
                                "kind": collision_event.get("kind"),
                                "status": "error",
                                "error": exc.message,
                                "code": exc.code,
                            }
                        )
                        # A batch-level 409 has no receipts for its other
                        # items. They stay pending and are exact-replayed on
                        # the next sync; attempted_ids prevents a hot loop.
                        continue
                    if is_stale_run_claim_error(exc):
                        sent_request = True
                        batches += 1
                        self._mark_server_channel_success(credentials)
                        event_results.extend(
                            self._reject_stale_sync_events(
                                events,
                            )
                        )
                        continue
                    if self._is_server_connectivity_error(exc):
                        self._mark_server_channel_failure(credentials, error=exc)
                    if sent_request:
                        drain_error = exc.message
                        drain_status_code = exc.status_code
                        self.store.record_server_connection_error(
                            credentials["id"],
                            status=(
                                SERVER_CONNECTION_STATUS_NEEDS_RECONNECT
                                if exc.status_code in {401, 403, 404, 409}
                                else "heartbeat_failed"
                            ),
                            error=exc.message,
                        )
                        break
                    raise
                except Exception as exc:
                    self._mark_server_channel_failure(credentials, error=exc)
                    if sent_request:
                        drain_error = repr(exc)
                        self.store.record_server_connection_error(
                            credentials["id"],
                            status="heartbeat_failed",
                            error=drain_error,
                        )
                        break
                    raise

                sent_request = True
                batches += 1
                connection = response.get("connection")
                if connection:
                    credentials = self.store.record_server_connection_heartbeat(
                        credentials["id"],
                        remote_connection=connection,
                    )
                self._mark_server_channel_success(credentials)
                batch_results = list(response.get("event_results", []))
                event_results.extend(batch_results)
                jobs.extend(response.get("jobs", []))
                for result in batch_results:
                    event_id = result.get("event_id")
                    if event_id == snapshot_event_id:
                        if result.get("status") == "ok":
                            self.store.record_server_connection_library_snapshot(
                                credentials["id"],
                                snapshot_hash=snapshot_hash,
                            )
                        continue
                    if event_id not in pending_ids:
                        continue
                    if result.get("status") == "ok":
                        self.store.mark_sync_event_sent(event_id)
                    else:
                        self.store.mark_sync_event_failed(
                            event_id,
                            result.get("error") or "sync event failed",
                            retryable=not is_permanent_archived_sync_error(result),
                        )

            if adopted_count:
                LOGGER.info(
                    "adopted %s pre-enrollment events as %s",
                    adopted_count,
                    credentials_owner_id,
                    extra={
                        "spl_event": "sync_events_adopted",
                        "sync_event_count": adopted_count,
                        "owner_id": credentials_owner_id,
                    },
                )
            if held_count:
                LOGGER.info(
                    "held for another identity: %s sync events while connected as %s; event owners: %s",
                    held_count,
                    credentials_owner_id,
                    ", ".join(sorted(held_owner_ids)) or "unknown",
                    extra={
                        "spl_event": "sync_events_held_for_another_identity",
                        "sync_event_count": held_count,
                        "owner_id": credentials_owner_id,
                        "held_owner_ids": sorted(held_owner_ids),
                    },
                )

        for job in jobs:
            self.accept_server_job(
                job,
                credentials["id"],
                parent_sync_lease=sync_lease,
            )
        self.sweep_run_retention()
        response = {
            "connection": self._remote_connection_snapshot(credentials),
            "event_results": event_results,
            "jobs": jobs,
            "batches": batches,
            "lease_renewed": True,
            "partial": drain_error is not None,
            "sync": {
                "before": sync_before,
                "after": self.sync_visibility.summary(),
            },
        }
        if drain_error is not None:
            response["error"] = drain_error
            response["partial_error"] = {
                "message": drain_error,
                "status_code": drain_status_code,
            }
        return response

    def _reconnect_server_credentials(self, credentials: dict[str, Any]) -> dict[str, Any]:
        """Re-handshake a stored identity after its previous lease was rejected."""

        server = self._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SYNC_REQUEST_TIMEOUT_SECONDS,
        )
        try:
            remote = server.connect_machine(
                machine_id=credentials["machine_id"],
                display_name=credentials.get("display_name"),
                capabilities=self._authoritative_server_capabilities(credentials.get("capabilities")),
                heartbeat_interval_seconds=self._safe_heartbeat_interval(credentials),
            )
        except Exception as exc:
            self._mark_server_channel_failure(credentials, error=exc)
            raise
        connection = self.store.complete_server_connection(
            credentials["id"],
            remote_connection=remote,
            heartbeat_interval_seconds=self._safe_heartbeat_interval(credentials),
        )
        refreshed = self.store.get_server_connection_credentials(connection["id"])
        self._mark_server_channel_success(refreshed)
        return refreshed

    def _renew_server_lease(
        self,
        server: ServerClientProtocol,
        credentials: dict[str, Any],
    ) -> dict[str, Any]:
        """Renew the lease before any queued payload is considered."""

        try:
            remote = server.heartbeat_connection(
                connection_id=credentials["remote_connection_id"],
                machine_id=credentials["machine_id"],
                heartbeat_interval_seconds=self._safe_heartbeat_interval(credentials),
            )
        except Exception as exc:
            self._mark_server_channel_failure(credentials, error=exc)
            raise
        refreshed = self.store.record_server_connection_heartbeat(
            credentials["id"],
            remote_connection=remote,
        )
        self._mark_server_channel_success(refreshed)
        return self.store.get_server_connection_credentials(refreshed["id"])

    @staticmethod
    def _safe_heartbeat_interval(credentials: dict[str, Any]) -> float:
        try:
            interval = float(credentials.get("heartbeat_interval_seconds") or 60.0)
        except (TypeError, ValueError):
            return 60.0
        return interval if interval > 0 else 60.0

    def _next_pending_sync_batch(
        self,
        *,
        owner_id: str,
        attempted_ids: set[str],
    ) -> tuple[list[dict[str, Any]], int, set[str], int]:
        batch: list[dict[str, Any]] = []
        batch_bytes = 2
        offset = 0
        held_count = 0
        held_owner_ids: set[str] = set()
        adopted = 0
        while len(batch) < SYNC_EVENT_BATCH_LIMIT:
            page = self.store.list_pending_sync_events(
                limit=SYNC_EVENT_SCAN_PAGE_LIMIT,
                offset=offset,
            )
            if not page:
                break
            for stored_event in page:
                event_id = str(stored_event["id"])
                if event_id in attempted_ids:
                    continue
                event_owner_id = self._sync_event_owner_id(stored_event.get("payload") or {})
                if event_owner_id not in {None, DEFAULT_OBJECT_OWNER_ID, owner_id}:
                    assert event_owner_id is not None
                    held_count += 1
                    held_owner_ids.add(event_owner_id)
                    continue
                event = (
                    self._sync_event_with_owner(stored_event, owner_id)
                    if event_owner_id in {None, DEFAULT_OBJECT_OWNER_ID}
                    else stored_event
                )
                wire_event = {"id": event["id"], "kind": event["kind"], "payload": event["payload"]}
                try:
                    event_attempt = self._sync_event_claim_attempt(wire_event)
                    event_bytes = self._sync_event_wire_bytes(wire_event)
                except (TypeError, ValueError) as exc:
                    self.store.mark_sync_event_failed(
                        event_id,
                        f"sync event cannot be sent safely: {exc}",
                        retryable=False,
                    )
                    attempted_ids.add(event_id)
                    continue
                if event_bytes > SYNC_EVENT_MAX_BYTES:
                    self.store.mark_sync_event_failed(
                        event_id,
                        f"sync event payload exceeds {SYNC_EVENT_MAX_BYTES} byte limit ({event_bytes} bytes)",
                        retryable=False,
                    )
                    attempted_ids.add(event_id)
                    continue
                if batch:
                    batch_attempt = self._sync_event_claim_attempt(batch[0])
                    if event_attempt != batch_attempt and (event_attempt is not None or batch_attempt is not None):
                        return batch, held_count, held_owner_ids, adopted
                if batch and batch_bytes + event_bytes + 1 > SYNC_BATCH_MAX_BYTES:
                    return batch, held_count, held_owner_ids, adopted
                batch.append(wire_event)
                batch_bytes += event_bytes + 1
                if event_owner_id in {None, DEFAULT_OBJECT_OWNER_ID}:
                    adopted += 1
                if len(batch) >= SYNC_EVENT_BATCH_LIMIT:
                    break
            if len(page) < SYNC_EVENT_SCAN_PAGE_LIMIT or batch:
                break
            offset += len(page)
        return batch, held_count, held_owner_ids, adopted

    def _take_ephemeral_sync_batch(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        batch: list[dict[str, Any]] = []
        batch_bytes = 2
        while events and len(batch) < SYNC_EVENT_BATCH_LIMIT:
            event = events[0]
            event_attempt = self._sync_event_claim_attempt(event)
            if batch:
                batch_attempt = self._sync_event_claim_attempt(batch[0])
                if event_attempt != batch_attempt and (event_attempt is not None or batch_attempt is not None):
                    break
            event_bytes = self._sync_event_wire_bytes(event)
            if event_bytes > SYNC_EVENT_MAX_BYTES:
                raise ValueError(
                    f"ephemeral sync event payload exceeds {SYNC_EVENT_MAX_BYTES} byte limit ({event_bytes} bytes)"
                )
            if batch and batch_bytes + event_bytes + 1 > SYNC_BATCH_MAX_BYTES:
                break
            batch.append(events.pop(0))
            batch_bytes += event_bytes + 1
        return batch

    @staticmethod
    def _sync_event_wire_bytes(event: dict[str, Any]) -> int:
        wire_event = DaemonRuntime._sync_event_for_wire(event)
        return len(
            m_json_contract.dumps(
                wire_event,
                ensure_ascii=False,
                sort_keys=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )

    @staticmethod
    def _sync_event_for_wire(event: dict[str, Any]) -> dict[str, Any]:
        """Return an event envelope without daemon-private claim metadata."""

        wire_event = {
            "id": event["id"],
            "kind": event["kind"],
            "payload": dict(event.get("payload") or {}),
        }
        wire_event["payload"].pop(RUN_CLAIM_PRIVATE_FIELD, None)
        return wire_event

    @staticmethod
    def _sync_event_claim_id(event: dict[str, Any]) -> str | None:
        """Read and validate a claim kept only in the daemon-private envelope."""

        payload = event.get("payload")
        if not isinstance(payload, dict):
            return None
        claim_id = payload.get(RUN_CLAIM_PRIVATE_FIELD)
        if claim_id is None:
            return None
        if event.get("kind") != "run_update":
            raise ValueError("private run claim metadata is only valid for run_update events")
        if not isinstance(claim_id, str) or not claim_id.strip():
            raise ValueError("private run claim metadata must be a non-empty string")
        return claim_id

    @classmethod
    def _sync_event_claim_attempt(
        cls,
        event: dict[str, Any],
    ) -> tuple[str, str] | None:
        """Return the complete identity of one fenced run-update attempt."""

        claim_id = cls._sync_event_claim_id(event)
        if claim_id is None:
            return None
        payload = event.get("payload")
        if not isinstance(payload, dict):  # pragma: no cover - claim validation above.
            raise ValueError("claimed run update must have an object payload")
        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("claimed run update must identify a non-empty run_id")
        return run_id, claim_id

    @classmethod
    def _sync_batch_claim_id(cls, events: list[dict[str, Any]]) -> str | None:
        """Return the one claim shared by a homogeneous sync batch."""

        attempts = {cls._sync_event_claim_attempt(event) for event in events}
        attempts.discard(None)
        if len(attempts) > 1:
            raise RuntimeError("sync batch contains multiple private run attempts")
        attempt = next(iter(attempts), None)
        if attempt is not None and any(cls._sync_event_claim_attempt(event) != attempt for event in events):
            raise RuntimeError("claimed sync batch contains an unfenced event")
        return attempt[1] if attempt is not None else None

    def _reject_stale_sync_events(
        self,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Make stale worker events terminal without failing the server channel."""

        results: list[dict[str, Any]] = []
        rejected_attempts: set[tuple[str, str | None]] = set()
        for event in events:
            event_id = str(event["id"])
            payload = event.get("payload") or {}
            run_id = str(payload.get("run_id") or "")
            claim_id = self._sync_event_claim_id(event)
            attempt = (run_id, claim_id)
            if run_id and attempt not in rejected_attempts:
                self._mark_server_attempt_superseded(run_id, claim_id)
                rejected_attempts.add(attempt)
            results.append(
                {
                    "event_id": event_id,
                    "kind": event.get("kind"),
                    "status": "error",
                    "code": STALE_RUN_CLAIM_ERROR_CODE,
                    "error": "run was reclaimed by a newer attempt",
                }
            )
        return results

    def _mark_server_attempt_superseded(
        self,
        run_id: str,
        claim_id: str | None,
    ) -> None:
        """Remember a stale attempt and log only its non-secret run identity."""

        key = (run_id, claim_id)
        with self._server_attempt_lock:
            first_rejection = key not in self._superseded_server_attempts
            self._superseded_server_attempts.add(key)
        if first_rejection:
            LOGGER.warning(
                "remote run attempt was superseded; further reports are suppressed for run %s",
                run_id,
                extra={
                    "spl_event": "remote_run_attempt_superseded",
                    "run_id": run_id,
                },
            )
        self._suppress_queued_server_attempt_events(run_id, claim_id)

    def _suppress_queued_server_attempt_events(
        self,
        run_id: str,
        claim_id: str | None,
    ) -> None:
        """Make every queued report for one known-stale attempt nonretryable."""

        error = "remote run was reclaimed by a newer attempt; stale update will not be retried"
        for event in self.store.list_pending_sync_events(limit=None):
            if event.get("kind") != "run_update":
                continue
            payload = event.get("payload")
            if not isinstance(payload, dict):
                continue
            if payload.get("run_id") != run_id:
                continue
            if payload.get(RUN_CLAIM_PRIVATE_FIELD) != claim_id:
                continue
            self.store.mark_sync_event_failed(
                str(event["id"]),
                error,
                retryable=False,
            )

    def _server_attempt_is_superseded(
        self,
        run_id: str,
        claim_id: str | None,
    ) -> bool:
        """Return whether central claim fencing rejected this exact attempt."""

        with self._server_attempt_lock:
            return (run_id, claim_id) in self._superseded_server_attempts

    def _sync_events_for_owner(
        self,
        events: list[dict[str, Any]],
        *,
        owner_id: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
        sendable: list[dict[str, Any]] = []
        held: list[dict[str, Any]] = []
        adopted = 0
        for event in events:
            event_owner_id = self._sync_event_owner_id(event.get("payload") or {})
            if event_owner_id is None or event_owner_id == DEFAULT_OBJECT_OWNER_ID:
                adopted += 1
                sendable.append(self._sync_event_with_owner(event, owner_id))
                continue
            if event_owner_id == owner_id:
                sendable.append(event)
                continue
            held.append(event)
        return sendable, held, adopted

    def _sync_event_with_owner(
        self,
        event: dict[str, Any],
        owner_id: str,
    ) -> dict[str, Any]:
        payload = dict(event.get("payload") or {})
        if isinstance(payload.get("run"), dict):
            run_payload = dict(payload["run"])
            run_payload["owner_id"] = owner_id
            payload["run"] = run_payload
        else:
            payload["owner_id"] = owner_id
        return {**event, "payload": payload}

    def _sync_event_owner_id(self, payload: dict[str, Any]) -> str | None:
        owner_id = payload.get("owner_id")
        if owner_id is None and isinstance(payload.get("run"), dict):
            owner_id = payload["run"].get("owner_id")
        if owner_id is None:
            return None
        owner_text = str(owner_id)
        return owner_text or None

    def _server_latest_library_snapshot_hash(
        self,
        server: ServerClientProtocol,
        machine_id: str,
    ) -> str | None:
        """Return the server's latest snapshot hash when it can be checked cheaply."""

        try:
            snapshot = server.latest_machine_library_snapshot(machine_id)
        except ServerClientError:
            return None
        return snapshot.get("snapshot_hash")

    def accept_server_job(
        self,
        job: dict[str, Any],
        connection_id: str,
        *,
        parent_sync_lease: LifecycleWorkLease | None = None,
    ) -> None:
        """Run one server job in a background thread."""

        lease = (
            self.lifecycle.admit_remote_claim(parent_sync_lease)
            if parent_sync_lease is not None
            else self.lifecycle.reserve_root("remote_claim", queued=True)
        )
        try:
            remote_run_id = validate_name(str(job["run"]["id"]))
            thread = threading.Thread(
                target=self._execute_server_job_with_lifecycle,
                args=(job, connection_id, lease),
                name=f"spl-server-job-{remote_run_id}",
                daemon=True,
            )
            thread.start()
        except Exception:
            # The central server has already assigned this job.  If its exact
            # identity cannot be retained locally, Stop must fail closed.
            self.lifecycle.record_unknown_work()
            lease.complete(seal_lineage=lease.root)
            raise

    def _execute_server_job_with_lifecycle(
        self,
        job: dict[str, Any],
        connection_id: str,
        lease: LifecycleWorkLease,
    ) -> None:
        """Retain a pre-boundary remote claim through terminal handoff."""

        lease.activate()
        lease.attach_current_thread()
        try:
            self._execute_server_job(job, connection_id)
        except Exception:
            # The server has already assigned this work.  If an unexpected
            # failure escapes the normal terminal-handoff path, its outcome
            # cannot be proven and Stop must remain blocked.  Keep the log
            # fixed so neither the job payload nor an exception value leaks.
            self.lifecycle.record_unknown_work()
            LOGGER.error(
                "accepted server job escaped its terminal handoff",
                extra={"spl_event": "remote_run_outcome_unproven"},
            )
        finally:
            lease.detach_current_thread()
            lease.complete(seal_lineage=lease.root)

    @staticmethod
    def _job_claim_id(job: dict[str, Any]) -> str | None:
        """Return a negotiated top-level claim without accepting malformed data."""

        claim_id = job.get("claim_id")
        if claim_id is None:
            return None
        if not isinstance(claim_id, str) or not claim_id.strip():
            raise ValueError("server job claim_id must be a non-empty string when provided")
        return claim_id

    def _execute_server_job(self, job: dict[str, Any], connection_id: str) -> None:
        """Execute one server-assigned job locally and sync the result back."""

        run = job["run"]
        version = job["object_version"]
        run_id = run["id"]
        local_name = version["name"]
        local_run: dict[str, Any] | None = None
        runtime_claim = job.get("runtime_port_adapters")
        runtime_library_claim = job.get("runtime_library_adapter_refs")
        runtime_semantic_claim = job.get("runtime_adapter_semantic_advisories")
        try:
            claim_id = self._job_claim_id(job)
        except ValueError:
            self.lifecycle.record_unknown_work()
            LOGGER.error(
                "server job was refused because its claim metadata is invalid for run %s",
                run_id,
                extra={
                    "spl_event": "remote_run_invalid_claim",
                    "run_id": run_id,
                },
            )
            return

        try:
            if runtime_library_claim is not None and runtime_claim is None:
                raise RuntimePortAdapterContractError(
                    "library_adapter_transport_missing",
                    "claimed Library Adapter refs require runtime port transport",
                    stage="admission",
                )
            if runtime_semantic_claim is not None and runtime_claim is None:
                raise RuntimePortAdapterContractError(
                    "semantic_advisory_transport_missing",
                    "claimed semantic advisories require runtime port transport",
                    stage="admission",
                )
            if runtime_claim is not None:
                if claim_id is None:
                    raise RuntimePortAdapterContractError(
                        "remote_claim_missing",
                        "an adapted remote Run requires a claim-fencing identity",
                        stage="admission",
                    )
                self._send_claim_bound_runtime_progress_now(
                    connection_id,
                    run_id=run_id,
                    status="fetching_object",
                    message="registering object bundle in local daemon",
                    claim_id=claim_id,
                )
            elif not self._send_server_run_update(
                connection_id,
                run_id=run_id,
                status="fetching_object",
                message="registering object bundle in local daemon",
                claim_id=claim_id,
            ):
                return
            self._ensure_server_object_envs([version])
            object_record = self.register_object(
                local_name,
                version["entrypoint"],
                version["env"] or "default",
                yaml_text=version["yaml"],
                owner_id=self._server_version_owner_id(version),
                library=self._server_version_library(version),
                description=version.get("description") or version["name"],
                version_label=version.get("version_label"),
                origin="server",
                remote_owner_id=version.get("owner_id"),
                remote_object_id=version.get("id"),
                remote_version_id=version.get("version_id"),
                source_object_name=version["name"],
                runtime_config=version.get("runtime_config"),
            )
            if runtime_claim is None and not self._send_server_run_update(
                connection_id,
                run_id=run_id,
                status="running",
                claim_id=claim_id,
            ):
                return
            function = (
                run.get("entrypoint")
                if run.get("entrypoint") and run.get("entrypoint") != version["entrypoint"]
                else None
            )
            runtime_document = None
            runtime_policy = None
            runtime_library_refs = None
            runtime_library_sources = None
            runtime_semantic_advisories = None
            if runtime_claim is not None:
                if claim_id is None:
                    raise RuntimePortAdapterContractError(
                        "remote_claim_missing",
                        "an adapted remote Run requires a claim-fencing identity",
                        stage="admission",
                    )
                credentials = self.store.get_server_connection_credentials(connection_id)
                server = self._server_client_for_credentials(credentials)
                signature = build_signature(object_record, function=function)

                def download_claimed_input(
                    download_url: str,
                    expected_size: int,
                    expected_sha256: str,
                ) -> bytes:
                    return server.claimed_runtime_input_bytes(
                        download_url,
                        claim_id=claim_id,
                        expected_size=expected_size,
                        expected_sha256=expected_sha256,
                    )

                runtime_document, runtime_policy = materialize_claimed_runtime_port_adapters(
                    runtime_claim,
                    signature=signature,
                    args=run.get("args"),
                    kwargs=run.get("kwargs"),
                    allow_remote_custom_adapters=self.allow_remote_custom_adapters,
                    download=download_claimed_input,
                )
                if runtime_semantic_claim is not None:
                    runtime_semantic_advisories = normalize_runtime_adapter_semantic_advisories(runtime_semantic_claim)
                if runtime_library_claim is not None:
                    (
                        runtime_library_refs,
                        runtime_library_sources,
                    ) = materialize_claimed_runtime_library_adapters(runtime_library_claim)
                try:
                    self._send_claim_bound_runtime_progress_now(
                        connection_id,
                        run_id=run_id,
                        status="running",
                        message="runtime adapter inputs verified",
                        claim_id=claim_id,
                    )
                except Exception:
                    for item in runtime_document.get("inputs") or []:
                        if isinstance(item, dict):
                            item["content_base64"] = None
                    runtime_bundle = runtime_document.get("custom_bundle")
                    if isinstance(runtime_bundle, dict):
                        runtime_bundle["content_base64"] = None
                    runtime_document.clear()
                    raise
            library_run_arguments: dict[str, Any] = {}
            if runtime_library_refs is not None:
                library_run_arguments = {
                    "runtime_library_adapter_refs": runtime_library_refs,
                    "runtime_library_adapter_sources": runtime_library_sources,
                    "_runtime_library_adapter_execution_target": "remote",
                }
            local_run = self.start_run(
                local_name,
                args=run.get("args"),
                kwargs=run.get("kwargs"),
                output=run.get("output"),
                timeout_seconds=run.get("timeout_seconds"),
                object_version_id=object_record["version_id"],
                function=function,
                source="local",
                runtimes=(run.get("context") or {}).get("_spl_runtime_overrides"),
                report_local_run=False,
                keep="on_failure",
                runtime_port_adapters=runtime_document,
                runtime_adapter_semantic_advisories=runtime_semantic_advisories,
                _runtime_adapter_semantic_claim=True,
                adapter_policy=runtime_policy,
                custom_remote_allowed=(
                    runtime_document is not None
                    and runtime_policy is not None
                    and runtime_policy["custom_remote"] == "allow"
                    and self.allow_remote_custom_adapters
                ),
                **library_run_arguments,
            )
            credentials = self.store.get_server_connection_credentials(connection_id)
            progress_interval = max(
                1.0,
                min(float(credentials["heartbeat_interval_seconds"]), 60.0),
            )

            def report_progress() -> None:
                if not self._send_server_run_update(
                    connection_id,
                    run_id=run_id,
                    status="running",
                    message="remote run lease renewed",
                    claim_id=claim_id,
                ):
                    raise _ServerRunSuperseded(run_id)

            final_state = self._wait_local_run(
                local_run["id"],
                timeout_seconds=run.get("timeout_seconds"),
                progress_callback=report_progress,
                progress_interval_seconds=progress_interval,
            )
            if final_state["status"] != "succeeded":
                manifest_evidence, _ = self._claim_bound_manifest_evidence(
                    final_state,
                    claim_id=claim_id,
                    server_object_version_id=version.get("version_id"),
                )
                terminal_payload: dict[str, Any] = {
                    "local_run": self._local_run_delivery_proof(final_state),
                }
                if manifest_evidence is not None:
                    terminal_payload["manifest_evidence"] = manifest_evidence
                execution_telemetry = self._claim_bound_execution_telemetry(
                    final_state,
                    claim_id=claim_id,
                )
                if execution_telemetry is not None:
                    terminal_payload["execution_telemetry"] = execution_telemetry
                if runtime_claim is not None:
                    terminal_payload["runtime_port_adapters_terminal"] = self._runtime_adapter_terminal_evidence(
                        final_state,
                        runtime_claim=runtime_claim,
                        result=None,
                    )
                queued = self._send_server_run_update(
                    connection_id,
                    run_id=run_id,
                    status="failed",
                    error=final_state.get("error") or "local run failed",
                    payload=terminal_payload,
                    claim_id=claim_id,
                )
                self._mark_remote_local_terminal_queued(
                    final_state,
                    queued=queued,
                    effective_status="failed",
                    outcome_reason="local-execution-failed",
                )
                return

            completed_state = self.store.get_run(final_state["id"])
            if not completed_state.get("result_present"):
                raise RuntimeError("local run succeeded without committing a result value")
            result = completed_state["result"]
            manifest_evidence, artifact_producers = self._claim_bound_manifest_evidence(
                final_state,
                claim_id=claim_id,
                server_object_version_id=version.get("version_id"),
            )
            artifact_evidence_kwargs: dict[str, Any] = {}
            if manifest_evidence is not None:
                artifact_evidence_kwargs = {
                    "manifest_evidence": manifest_evidence,
                    "artifact_producers": artifact_producers,
                }
            artifacts = self._prepare_remote_run_artifacts(
                connection_id,
                run_id,
                final_state,
                claim_id=claim_id,
                **artifact_evidence_kwargs,
            )
            if runtime_claim is not None:
                result = self._path_free_runtime_adapter_remote_result(
                    result,
                    artifacts=artifacts,
                )
            terminal_payload = {
                "local_run": self._local_run_delivery_proof(final_state),
            }
            if manifest_evidence is not None:
                terminal_payload["manifest_evidence"] = manifest_evidence
            execution_telemetry = self._claim_bound_execution_telemetry(
                {**final_state, **completed_state},
                claim_id=claim_id,
            )
            if execution_telemetry is not None:
                terminal_payload["execution_telemetry"] = execution_telemetry
            if runtime_claim is not None:
                terminal_payload["runtime_port_adapters_terminal"] = self._runtime_adapter_terminal_evidence(
                    completed_state,
                    runtime_claim=runtime_claim,
                    result=result,
                )
            queued = self._send_server_run_update(
                connection_id,
                run_id=run_id,
                status="succeeded",
                result=result,
                payload=terminal_payload,
                artifacts=artifacts,
                claim_id=claim_id,
            )
            self._mark_remote_local_terminal_queued(
                final_state,
                queued=queued,
                effective_status="succeeded",
                outcome_reason="server-handoff-succeeded",
            )
        except _ServerRunSuperseded:
            return
        except Exception as exc:
            if self._server_attempt_is_superseded(run_id, claim_id):
                return
            local_state = None
            if local_run is not None:
                try:
                    local_state = self.store.get_run(str(local_run["id"]))
                except KeyError:
                    local_state = None
            manifest_evidence, _ = self._claim_bound_manifest_evidence(
                local_state,
                claim_id=claim_id,
                server_object_version_id=version.get("version_id"),
            )
            terminal_payload = {}
            if local_state is not None:
                terminal_payload["local_run"] = self._local_run_delivery_proof(local_state)
            if manifest_evidence is not None:
                terminal_payload["manifest_evidence"] = manifest_evidence
            execution_telemetry = self._claim_bound_execution_telemetry(
                local_state,
                claim_id=claim_id,
            )
            if execution_telemetry is not None:
                terminal_payload["execution_telemetry"] = execution_telemetry
            if runtime_claim is not None:
                terminal_payload["runtime_port_adapters_terminal"] = self._runtime_adapter_terminal_evidence(
                    local_state,
                    runtime_claim=runtime_claim,
                    result=None,
                )
            queued = self._send_server_run_update(
                connection_id,
                run_id=run_id,
                status="failed",
                error=("runtime adapter remote execution failed safely" if runtime_claim is not None else repr(exc)),
                payload=terminal_payload,
                claim_id=claim_id,
            )
            if local_state is not None:
                local_status = str(local_state.get("status"))
                self._mark_remote_local_terminal_queued(
                    local_state,
                    queued=queued,
                    effective_status="failed" if local_status == "succeeded" else local_status,
                    outcome_reason=(
                        "server-handoff-failed" if local_status == "succeeded" else "local-execution-failed"
                    ),
                )

    def _claim_bound_execution_telemetry(
        self,
        state: dict[str, Any] | None,
        *,
        claim_id: str | None,
    ) -> dict[str, Any] | None:
        """Return per-execution evidence only for a negotiated run attempt."""

        if claim_id is None or state is None:
            return None
        artifact_count, scan_truncated, directory_available = count_local_artifacts_bounded(state)
        omissions: list[str] = []
        if scan_truncated:
            omissions.append("artifact_scan_limit")
        if not directory_available:
            omissions.append("artifact_directory_unavailable")
        return self.telemetry_policy.build_execution_telemetry_envelope(
            state,
            observed_at=utc_now(),
            artifact_count=artifact_count,
            artifact_count_truncated=scan_truncated or not directory_available,
            preflight_omissions=tuple(omissions),
        )

    def _mark_remote_local_terminal_queued(
        self,
        local_state: dict[str, Any],
        *,
        queued: bool,
        effective_status: str,
        outcome_reason: str,
    ) -> None:
        """Release a server-job run only after its terminal update is durable."""

        if not queued or str(local_state.get("status")) not in m_manifest.TERMINAL_RUN_STATUSES:
            return
        local_run_id = str(local_state["id"])
        if not local_state.get("retention_enforced"):
            persisted = self.store.get_run(local_run_id)
            if not persisted.get("retention_enforced"):
                return
        self.store.runs.mark_retention_terminal_queued(
            local_run_id,
            sync_required=True,
            effective_status=effective_status,
            outcome_reason=outcome_reason,
        )
        self.store.enforce_run_retention(local_run_id)

    def _send_server_run_update(
        self,
        connection_id: str,
        *,
        run_id: str,
        status: str,
        result: Any = _RESULT_NOT_PROVIDED,
        error: str | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
        artifacts: list[dict[str, Any]] | None = None,
        claim_id: str | None = None,
    ) -> bool:
        """Queue one remote-run mutation unless its attempt was superseded."""

        if claim_id is not None and (not isinstance(claim_id, str) or not claim_id.strip()):
            raise ValueError("claim_id must be a non-empty string when provided")
        credentials = self.store.get_server_connection_credentials(connection_id)
        event_payload: dict[str, Any] = {
            "run_id": run_id,
            "status": status,
            "error": error,
            "message": message,
            "payload": payload or {},
            "artifacts": artifacts or [],
        }
        if result is not _RESULT_NOT_PROVIDED:
            event_payload["result"] = result
        if credentials.get("owner_id"):
            event_payload["owner_id"] = credentials["owner_id"]
        if claim_id is not None:
            event_payload[RUN_CLAIM_PRIVATE_FIELD] = claim_id
        with self._server_attempt_lock:
            if (run_id, claim_id) in self._superseded_server_attempts:
                return False
            self.store.enqueue_sync_event("run_update", event_payload)
        self._kick_server_sync(connection_id)
        return True

    def _send_claim_bound_runtime_progress_now(
        self,
        connection_id: str,
        *,
        run_id: str,
        status: str,
        message: str,
        claim_id: str,
    ) -> None:
        """Synchronously fence an adapted attempt before local code admission."""

        credentials = self.store.get_server_connection_credentials(connection_id)
        server = self._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SYNC_REQUEST_TIMEOUT_SECONDS,
        )
        event_payload: dict[str, Any] = {
            "run_id": run_id,
            "status": status,
            "error": None,
            "message": message,
            "payload": {},
            "artifacts": [],
        }
        if credentials.get("owner_id"):
            event_payload["owner_id"] = credentials["owner_id"]
        event_id = (
            "runtime_adapter_fence_" + hashlib.sha256(f"{run_id}\0{claim_id}\0{status}".encode("utf-8")).hexdigest()
        )
        try:
            response = server.sync(
                connection_id=credentials["remote_connection_id"],
                machine_id=credentials["machine_id"],
                heartbeat_interval_seconds=self._safe_heartbeat_interval(credentials),
                events=[{"id": event_id, "kind": "run_update", "payload": event_payload}],
                capabilities=self._authoritative_server_capabilities(credentials.get("capabilities")),
                claim_id=claim_id,
                claim_jobs=False,
            )
        except ServerClientError as exc:
            if is_stale_run_claim_error(exc):
                self._mark_server_attempt_superseded(run_id, claim_id)
                raise _ServerRunSuperseded(run_id) from exc
            raise
        results = response.get("event_results")
        if (
            not isinstance(results, list)
            or len(results) != 1
            or not isinstance(results[0], dict)
            or results[0].get("event_id") != event_id
            or results[0].get("status") != "ok"
        ):
            raise RuntimeError("central server did not confirm the claim-bound runtime progress fence")

    def _wait_local_run(
        self,
        run_id: str,
        *,
        timeout_seconds: float | None,
        progress_callback: Callable[[], None] | None = None,
        progress_interval_seconds: float = 60.0,
    ) -> dict[str, Any]:
        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )
        started = time.monotonic()
        next_progress = started + progress_interval_seconds
        while True:
            state = self.store.get_run(run_id)
            if state["status"] in {"succeeded", "failed"}:
                return state
            now = time.monotonic()
            if timeout_seconds is not None and now - started > timeout_seconds:
                raise TimeoutError(f"local run did not finish within {timeout_seconds} seconds")
            if progress_callback is not None and now >= next_progress:
                progress_callback()
                next_progress = now + progress_interval_seconds
            time.sleep(0.25)

    def _wait_server_run(
        self,
        run_id: str,
        *,
        timeout_seconds: float | None,
    ) -> dict[str, Any]:
        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )
        credentials = self._require_live_server_channel_credentials()
        server = self._server_client_for_credentials(
            credentials,
            request_timeout_seconds=SERVER_PROXY_TIMEOUT_SECONDS,
        )
        started = time.monotonic()
        while True:
            state = server.get_remote_run(run_id)
            if state["status"] in {"succeeded", "failed", "cancelled", "stale"}:
                return state
            if timeout_seconds is not None and time.monotonic() - started > timeout_seconds:
                raise TimeoutError(f"remote node run did not finish within {timeout_seconds} seconds")
            time.sleep(0.5)

    def run_remote_node(
        self,
        node: dict[str, Any],
        *,
        kwargs: dict[str, Any],
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Execute a NodeRemote through the central server and return its result."""

        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )
        ref = self._normalize_remote_ref(node)
        signature = self.resolve_remote_signature(ref)
        target_machine = (
            ref.get("target_machine")
            or signature.get("target_machine")
            or (signature.get("execution") or {}).get("default_machine_id")
        )
        remote_ref = signature.get("remote_ref") or {}
        signature_remote = signature.get("remote") or {}
        object_owner_id = (
            signature_remote.get("owner_id")
            or remote_ref.get("owner_id")
            or signature.get("owner_id")
            or ref.get("owner_id")
        )
        library = signature_remote.get("library") or remote_ref.get("library") or ref.get("library")

        output = self._remote_node_output_selector(signature)
        if not target_machine:
            raise RuntimeError(
                "remote node cannot run because no target machine was selected; "
                "set target_machine on the remote reference or configure "
                "execution.default_machine_id for the server object "
                f"{ref['object_name']!r}"
            )
        remote_run = self.start_remote_run(
            signature.get("id") or ref["object_name"],
            target_machine=target_machine,
            object_owner_id=object_owner_id,
            library=library,
            kwargs=kwargs,
            output=output,
            timeout_seconds=timeout_seconds,
            object_version_id=signature.get("version_id"),
            function=ref.get("function") or signature.get("function"),
            correlation_id=uuid4().hex,
            context={
                "remote_ref": remote_ref,
                "node": {
                    "url": ref.get("server_url"),
                    "name": ref.get("object_name"),
                    "function": ref.get("function"),
                    "version": ref.get("version"),
                },
            },
        )
        final_state = self._wait_server_run(
            remote_run["id"],
            timeout_seconds=timeout_seconds,
        )
        if final_state["status"] != "succeeded":
            status = final_state.get("status") or "unknown"
            error = final_state.get("error") or "remote run returned no error message"
            object_kind = signature.get("kind") or "object"
            raise RuntimeError(
                f"remote {object_kind} node {ref['object_name']!r} failed on "
                f"machine {target_machine!r}: run {remote_run['id']} ended as "
                f"{status!r} ({error})"
            )
        payload = final_state.get("result") or {}
        value = self._remote_node_result_value(
            payload,
            signature,
            output=output,
        )
        artifacts = payload.get("artifacts") if isinstance(payload, dict) else {}
        return {
            "value": value,
            "run_id": remote_run["id"],
            "status": final_state.get("status"),
            "run": final_state,
            "payload": payload if isinstance(payload, dict) else {"result": value},
            "artifacts": artifacts or {},
        }

    def _remote_node_output_selector(self, signature: dict[str, Any]) -> str | None:
        selectors = [
            item.get("selector") for item in signature.get("outputs") or [] if item.get("selector") is not None
        ]
        selectors = [str(item) for item in selectors]
        if len(selectors) > 1:
            raise RuntimeError(
                "remote Function/Pipeline node has multiple selectable outputs; "
                "the current NodeRemote shape cannot choose one explicitly"
            )
        return selectors[0] if selectors else None

    def _remote_node_result_value(
        self,
        payload: Any,
        signature: dict[str, Any],
        *,
        output: str | None,
    ) -> Any:
        value = payload.get("result") if isinstance(payload, dict) else payload
        if (signature.get("kind") or "unknown") != "pipeline":
            return value
        if not isinstance(value, dict):
            return value

        selected = None
        for item in signature.get("outputs") or []:
            if output is not None and item.get("selector") == output:
                selected = item
                break
        if selected is None and len(signature.get("outputs") or []) == 1:
            selected = (signature.get("outputs") or [None])[0]
        if selected is None:
            return value

        value_path = selected.get("value_path") or []
        if value_path and len(value_path) == 1 and value_path[0] in value:
            return value[value_path[0]]
        return value

    def _encode_local_artifacts(self, run_state: dict[str, Any]) -> list[dict[str, Any]]:
        artifacts_dir = Path(run_state["artifacts_dir"])
        if not artifacts_dir.exists():
            return []
        encoded = []
        for path in sorted(artifacts_dir.iterdir()):
            if path.is_file():
                encoded.append(
                    {
                        "name": path.name,
                        "data_base64": base64.b64encode(path.read_bytes()).decode("ascii"),
                    }
                )
        return encoded

    @staticmethod
    def _local_run_delivery_proof(state: dict[str, Any]) -> dict[str, str]:
        """Return the only local-run fields required by durable handoff proof."""

        return {"id": str(state["id"]), "status": str(state["status"])}

    @staticmethod
    def _path_free_runtime_adapter_remote_result(
        value: Any,
        *,
        artifacts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Project an adapted worker result without target-local filesystem paths."""

        if not isinstance(value, dict):
            raise RuntimeError("adapted worker result is not a JSON object")
        artifact_names = {
            validate_name(str(item["name"]))
            for item in artifacts
            if isinstance(item, dict) and item.get("name") is not None
        }
        projected: dict[str, Any] = {
            "result": value.get("result"),
            "artifacts": {name: name for name in sorted(artifact_names)},
        }
        raw_outputs = value.get("runtime_port_adapter_outputs")
        if raw_outputs is not None:
            if not isinstance(raw_outputs, list):
                raise RuntimeError("adapted worker output metadata is not a list")
            outputs = [normalize_runtime_output_record(item) for item in raw_outputs]
            if (
                len({item["port"] for item in outputs}) != len(outputs)
                or len({item["name"] for item in outputs}) != len(outputs)
                or any(item["name"] not in artifact_names for item in outputs)
            ):
                raise RuntimeError("adapted worker output metadata is not bound to uploaded artifacts")
            projected["runtime_port_adapter_outputs"] = outputs
        return projected

    def _runtime_adapter_terminal_evidence(
        self,
        state: dict[str, Any] | None,
        *,
        runtime_claim: Any,
        result: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Return the closed, claim-safe adapter evidence retained centrally."""

        manifest = state.get("manifest") if isinstance(state, dict) else None
        section = manifest.get("runtime_port_adapters") if isinstance(manifest, dict) else None
        custom = section.get("custom_remote") if isinstance(section, dict) else None
        if (
            isinstance(custom, dict)
            and set(custom) == {"requested", "allowed", "used"}
            and all(isinstance(custom.get(key), bool) for key in ("requested", "allowed", "used"))
        ):
            custom_execution = {
                "requested": custom["requested"],
                "allowed": custom["allowed"],
                "used": custom["used"],
            }
        else:
            policy = (
                normalize_adapter_policy(runtime_claim.get("adapter_policy"))
                if isinstance(runtime_claim, dict)
                else {"custom_remote": "deny"}
            )
            requested = policy["custom_remote"] == "allow"
            custom_execution = {
                "requested": requested,
                "allowed": requested and self.allow_remote_custom_adapters,
                "used": False,
            }

        outputs: list[dict[str, Any]] = []
        raw_outputs = result.get("runtime_port_adapter_outputs") if isinstance(result, dict) else None
        manifest_bindings = section.get("bindings") if isinstance(section, dict) else None
        if raw_outputs is not None:
            if not isinstance(raw_outputs, list) or not isinstance(manifest_bindings, list):
                raise RuntimeError("terminal runtime adapter evidence is incomplete")
            records = [normalize_runtime_output_record(item) for item in raw_outputs]
            for record in records:
                matches = [
                    binding
                    for binding in manifest_bindings
                    if isinstance(binding, dict)
                    and binding.get("direction") == "output"
                    and binding.get("port") == record["port"]
                ]
                if len(matches) != 1:
                    raise RuntimeError("terminal runtime adapter binding is missing or duplicated")
                binding = matches[0]
                if (
                    binding.get("adapter_id") != record["adapter_id"]
                    or binding.get("format_tag") != record["format_tag"]
                    or binding.get("artifact_name") != record["name"]
                    or binding.get("artifact_size") != record["size"]
                    or binding.get("artifact_sha256") != record["sha256"]
                    or binding.get("media_type") != record["media_type"]
                    or binding.get("result_path") != record["result_path"]
                ):
                    raise RuntimeError("terminal runtime adapter output differs from its local manifest")
                outputs.append(
                    {
                        "port": record["port"],
                        "adapter_id": record["adapter_id"],
                        "format_tag": record["format_tag"],
                        "semantic_type": binding.get("semantic_type"),
                        "artifact_name": record["name"],
                        "size": record["size"],
                        "sha256": record["sha256"],
                        "media_type": record["media_type"],
                        "result_path": record["result_path"],
                    }
                )
            outputs.sort(key=lambda item: item["port"])

        failure = None
        terminal_schema_version = 1
        if not isinstance(state, dict) or state.get("status") != "succeeded":
            raw_failure = section.get("failure") if isinstance(section, dict) else None
            typed_failure = _validated_runtime_adapter_failure(
                dict(raw_failure) if isinstance(raw_failure, dict) else None,
                manifest if isinstance(manifest, dict) else {},
            )
            if isinstance(typed_failure, dict) and typed_failure.get("schema_version") == 1:
                failure = dict(typed_failure)
                ref_document = manifest.get("runtime_library_adapter_refs") if isinstance(manifest, dict) else None
                refs = [
                    item
                    for item in (ref_document or {}).get("bindings", [])
                    if isinstance(item, dict)
                    and item.get("direction") == failure["direction"]
                    and item.get("port") == failure["port"]
                ]
                if len(refs) > 1:
                    raise RuntimeError("terminal runtime Library Adapter failure binding is duplicated")
                if refs:
                    exact_ref = {
                        key: refs[0][key]
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
                    failure["adapter_id"] = exact_ref["adapter_id"]
                    failure["adapter_ref"] = exact_ref
                terminal_schema_version = 2
            else:
                stage = raw_failure.get("stage") if isinstance(raw_failure, dict) else "admission"
                if not isinstance(stage, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", stage):
                    stage = "admission"
                failure = {"stage": stage, "reason_code": "adapter_stage_failed"}
        return {
            "schema_version": terminal_schema_version,
            "custom_execution": custom_execution,
            "outputs": outputs,
            "failure": failure,
        }

    def _claim_bound_manifest_evidence(
        self,
        state: dict[str, Any] | None,
        *,
        claim_id: str | None,
        server_object_version_id: str | None,
    ) -> tuple[dict[str, Any] | None, dict[m_manifest.ArtifactProducerKey, dict[str, Any]]]:
        """Return bounded evidence only for an exact, claim-fenced attempt."""

        if state is None or claim_id is None or server_object_version_id is None:
            return None, {}
        raw_manifest = state.get("manifest")
        if not isinstance(raw_manifest, dict):
            return None, {}
        try:
            if raw_manifest.get("run_id") != state.get("id"):
                raise ValueError("manifest run identity does not match local state")
            if raw_manifest.get("status") != state.get("status"):
                raise ValueError("manifest status does not match local state")
            evidence = m_manifest.terminal_manifest_evidence(raw_manifest)
            local_version_id = evidence["summary"].get("object_version_id")
            if not isinstance(local_version_id, str):
                raise ValueError("manifest is missing its local object version")
            local_version = self.store.get_object_version(
                local_version_id,
                include_yaml=False,
            )
            if local_version.get("remote_version_id") != server_object_version_id:
                raise ValueError("manifest object version is not bound to the server job")
            evidence["summary"]["object_version_id"] = server_object_version_id
            return (
                {
                    **evidence,
                    "source": "worker_retained_manifest",
                },
                m_manifest.manifest_artifact_producers(raw_manifest),
            )
        except (KeyError, TypeError, ValueError):
            LOGGER.warning(
                "terminal manifest evidence is unavailable for local run %s",
                state.get("id"),
                extra={
                    "spl_event": "terminal_manifest_evidence_unavailable",
                    "local_run_id": state.get("id"),
                },
            )
            return None, {}

    @staticmethod
    def _artifact_producer_evidence(
        metadata: dict[str, Any],
        *,
        manifest_evidence: dict[str, Any] | None,
        artifact_producers: dict[m_manifest.ArtifactProducerKey, dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Return one exact producer binding, never a filename inference."""

        if manifest_evidence is None:
            return None
        key = (
            str(metadata["name"]),
            str(metadata["sha256"]).casefold(),
            int(metadata["size"]),
        )
        producer = artifact_producers.get(key)
        if producer is None:
            return None
        return {
            "manifest_digest_sha256": manifest_evidence["digest_sha256"],
            "node_id": producer["node_id"],
            "alias": producer["alias"],
            "output_port": producer["output_port"],
        }

    def _prepare_remote_run_artifacts(
        self,
        connection_id: str,
        run_id: str,
        run_state: dict[str, Any],
        *,
        claim_id: str | None = None,
        manifest_evidence: dict[str, Any] | None = None,
        artifact_producers: dict[m_manifest.ArtifactProducerKey, dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Prepare result artifacts without publishing from a stale attempt."""

        if self._server_attempt_is_superseded(run_id, claim_id):
            raise _ServerRunSuperseded(run_id)
        artifacts_dir = Path(run_state["artifacts_dir"])
        if not artifacts_dir.exists():
            return []

        credentials = self.store.get_server_connection_credentials(connection_id)
        if credentials is None:
            raise ServerOfflineError(SERVER_OFFLINE_MESSAGE)
        server = self._server_client_for_credentials(credentials)

        prepared: list[dict[str, Any]] = []
        inline_bytes = 0
        for path in sorted(artifacts_dir.iterdir()):
            if self._server_attempt_is_superseded(run_id, claim_id):
                raise _ServerRunSuperseded(run_id)
            if not path.is_file():
                continue
            metadata = self._artifact_file_metadata(path)
            producer_evidence = self._artifact_producer_evidence(
                metadata,
                manifest_evidence=manifest_evidence,
                artifact_producers=artifact_producers or {},
            )
            if producer_evidence is not None:
                metadata["producer_evidence"] = producer_evidence
            inline_allowed = (
                metadata["size"] <= DEFAULT_INLINE_REMOTE_ARTIFACT_MAX_BYTES
                and inline_bytes + metadata["size"] <= DEFAULT_INLINE_REMOTE_ARTIFACT_TOTAL_MAX_BYTES
            )
            if inline_allowed:
                inline_bytes += metadata["size"]
                prepared.append(
                    {
                        **metadata,
                        "transfer_mode": "inline_base64",
                        "data_base64": base64.b64encode(path.read_bytes()).decode("ascii"),
                    }
                )
                continue

            try:
                if claim_id is None:
                    uploaded = server.upload_artifact(run_id, path.name, path)
                else:
                    uploaded = server.upload_artifact(
                        run_id,
                        path.name,
                        path,
                        claim_id=claim_id,
                    )
            except ServerClientError as exc:
                if not is_stale_run_claim_error(exc):
                    raise
                self._mark_server_attempt_superseded(run_id, claim_id)
                raise _ServerRunSuperseded(run_id) from exc
            self._assert_uploaded_artifact_matches(metadata, uploaded)
            prepared.append(
                {
                    **metadata,
                    "transfer_mode": "direct_upload",
                    "uploaded": True,
                    "server_artifact_id": uploaded.get("id"),
                }
            )
        return prepared

    def _artifact_file_metadata(self, path: Path) -> dict[str, Any]:
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
        return {
            "name": validate_name(path.name),
            "size": size,
            "sha256": digest.hexdigest(),
        }

    @staticmethod
    def _assert_uploaded_artifact_matches(
        expected: dict[str, Any],
        uploaded: dict[str, Any],
    ) -> None:
        if uploaded.get("name") != expected["name"]:
            raise RuntimeError(
                f"uploaded artifact name mismatch: expected {expected['name']}, got {uploaded.get('name')}"
            )
        if int(uploaded.get("size", -1)) != int(expected["size"]):
            raise RuntimeError(
                "uploaded artifact size mismatch: "
                f"{expected['name']} expected {expected['size']}, got {uploaded.get('size')}"
            )
        if str(uploaded.get("sha256", "")).casefold() != str(expected["sha256"]).casefold():
            raise RuntimeError(
                "uploaded artifact checksum mismatch: "
                f"{expected['name']} expected {expected['sha256']}, got {uploaded.get('sha256')}"
            )

    def prepare_object_environment(self, object_record: dict[str, Any]) -> dict[str, Any]:
        """Start a cached runtime build for an object version when configured."""

        backend = self.runtime_backends.backend_for(object_record)
        if not self.auto_build_envs:
            return backend.status_for_object(object_record)
        if object_record.get("origin") == "server":
            status = backend.status_for_object(object_record)
            status["auto_build_skipped"] = "server_imported_object"
            return status
        try:
            status = backend.ensure_ready(object_record, wait=False)
            backend.after_prepare(object_record)
            return status
        except EnvironmentBuildError:
            return backend.status_for_object(object_record)

    def start_run(
        self,
        object_name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        object_version_id: str | None = None,
        function: str | None = None,
        object_owner_id: str | None = None,
        library: str | None = None,
        source: str = "auto",
        report_local_run: bool = True,
        runtimes: str | dict[str, str] | None = None,
        keep: Any = True,
        runtime_port_adapters: dict[str, Any] | None = None,
        runtime_library_adapter_refs: dict[str, Any] | None = None,
        runtime_adapter_semantic_advisories: dict[str, Any] | None = None,
        runtime_library_adapter_sources: list[dict[str, Any]] | None = None,
        _runtime_library_adapter_execution_target: Literal["local", "remote"] = "local",
        adapter_policy: dict[str, Any] | None = None,
        custom_remote_allowed: bool = False,
        _browser_adapter_admission: dict[str, Any] | None = None,
        _runtime_adapter_semantic_claim: bool = False,
    ) -> dict[str, Any]:
        """Atomically admit then create one legacy-compatible local Run."""

        # Keep the established pure-input failure boundary ahead of runtime
        # lookup while retaining admission before every durable side effect.
        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )
        if (
            runtime_port_adapters is not None
            and runtime_library_adapter_refs is not None
            and runtime_library_adapter_sources is None
            and _runtime_library_adapter_execution_target == "local"
        ):
            # Direct SDK Runs do not have the browser broker's preflight
            # record. Resolve source through the same purpose-specific,
            # no-retry central contract before reserving lifecycle/mutation
            # authority. Local versions remain a store-only read.
            runtime_library_adapter_sources = self._resolve_local_library_adapter_sources(runtime_library_adapter_refs)
        lease = self.lifecycle.reserve_current_or_root("local_run", queued=True)
        lease.activate()
        lease.attach_current_thread()
        try:
            return self._start_run_admitted(
                object_name,
                args=args,
                kwargs=kwargs,
                output=output,
                timeout_seconds=timeout_seconds,
                version=version,
                object_version_id=object_version_id,
                function=function,
                object_owner_id=object_owner_id,
                library=library,
                source=source,
                report_local_run=report_local_run,
                runtimes=runtimes,
                keep=keep,
                runtime_port_adapters=runtime_port_adapters,
                runtime_library_adapter_refs=runtime_library_adapter_refs,
                runtime_adapter_semantic_advisories=runtime_adapter_semantic_advisories,
                runtime_library_adapter_sources=runtime_library_adapter_sources,
                _runtime_library_adapter_execution_target=(_runtime_library_adapter_execution_target),
                adapter_policy=adapter_policy,
                custom_remote_allowed=custom_remote_allowed,
                _browser_adapter_admission=_browser_adapter_admission,
                _runtime_adapter_semantic_claim=_runtime_adapter_semantic_claim,
                lifecycle_lease=lease,
            )
        except Exception:
            self._release_unbound_lifecycle_lease(lease)
            raise
        finally:
            lease.detach_current_thread()

    def _resolve_local_library_adapter_sources(
        self,
        value: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Resolve exact local/shared source before a direct SDK Run mutation."""

        refs = normalize_runtime_library_adapter_refs(value)
        source_by_version: dict[str, dict[str, Any]] = {}
        credentials: dict[str, Any] | None = None
        server: Any | None = None
        for binding in refs["bindings"]:
            ref = {
                key: binding[key]
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
            version_id = str(ref["adapter_version_id"])
            if version_id in source_by_version:
                continue
            try:
                record = self.store.resolve_library_adapter_ref(
                    ref,
                    include_source=True,
                )
            except KeyError:
                if credentials is None:
                    credentials = self._require_live_server_channel_credentials()
                    server = self._server_client_for_credentials(credentials)
                assert server is not None
                response = server.resolve_local_library_adapter_source(
                    ref,
                    target_machine_id=str(credentials["machine_id"]),
                )
                if (
                    not isinstance(response, dict)
                    or set(response)
                    != {
                        "schema",
                        "schema_version",
                        "execution_target",
                        "target_machine_id",
                        "adapter",
                    }
                    or response.get("schema") != "spl.library-adapter-local-execution-source"
                    or response.get("schema_version") != 1
                    or response.get("execution_target") != "local"
                    or response.get("target_machine_id") != credentials["machine_id"]
                    or not isinstance(response.get("adapter"), dict)
                ):
                    raise RuntimePortAdapterContractError(
                        "library_adapter_source_response",
                        "shared Library Adapter source response is invalid",
                        stage="admission",
                    )
                record = dict(response["adapter"])
            if any(record.get(key) != expected for key, expected in ref.items()):
                raise RuntimePortAdapterContractError(
                    "library_adapter_ref_unverified",
                    "Library Adapter source does not match the exact immutable Run ref",
                    stage="admission",
                )
            source_by_version[version_id] = dict(record)
        return [source_by_version[key] for key in sorted(source_by_version)]

    def _start_run_admitted(
        self,
        object_name: str,
        *,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        version: int | None = None,
        object_version_id: str | None = None,
        function: str | None = None,
        object_owner_id: str | None = None,
        library: str | None = None,
        source: str = "auto",
        report_local_run: bool = True,
        runtimes: str | dict[str, str] | None = None,
        keep: Any = True,
        runtime_port_adapters: dict[str, Any] | None = None,
        runtime_library_adapter_refs: dict[str, Any] | None = None,
        runtime_adapter_semantic_advisories: dict[str, Any] | None = None,
        runtime_library_adapter_sources: list[dict[str, Any]] | None = None,
        _runtime_library_adapter_execution_target: Literal["local", "remote"] = "local",
        adapter_policy: dict[str, Any] | None = None,
        custom_remote_allowed: bool = False,
        _browser_adapter_admission: dict[str, Any] | None = None,
        _runtime_adapter_semantic_claim: bool = False,
        lifecycle_lease: LifecycleWorkLease,
    ) -> dict[str, Any]:
        """Create a run and execute it in a background worker thread."""

        if source not in {"auto", "local"}:
            raise ValueError("source must be 'auto' or 'local'")
        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )

        object_name, function = split_object_function_ref(object_name, function)
        server_owner_ref = object_owner_id
        resolved_version_id = object_version_id
        if source == "auto" and object_version_id is None:
            refresh = self.refresh_server_object_if_available(
                object_name,
                version=version,
                owner_id=server_owner_ref,
                library=library,
            )
            if refresh and refresh.get("current_version"):
                resolved_version_id = refresh["current_version"]["version_id"]

        local_owner_id = self.resolve_user_ref(str(server_owner_ref)) if server_owner_ref is not None else None
        try:
            state = self.store.create_run(
                object_name,
                args=args,
                kwargs=kwargs,
                output=output,
                timeout_seconds=timeout_seconds,
                version=version,
                object_version_id=resolved_version_id,
                function=function,
                owner_id=local_owner_id,
                library=library,
                runtimes=runtimes,
                keep=keep,
                runtime_port_adapters=runtime_port_adapters,
                runtime_library_adapter_refs=runtime_library_adapter_refs,
                runtime_adapter_semantic_advisories=runtime_adapter_semantic_advisories,
                runtime_library_adapter_sources=runtime_library_adapter_sources,
                _runtime_library_adapter_execution_target=(_runtime_library_adapter_execution_target),
                adapter_policy=adapter_policy,
                custom_remote_allowed=custom_remote_allowed,
                report_local_run=report_local_run,
                _browser_adapter_admission=_browser_adapter_admission,
                _runtime_adapter_semantic_claim=_runtime_adapter_semantic_claim,
            )
        except KeyError as exc:
            can_import = source == "auto" and resolved_version_id is None and "object is not registered" in str(exc)
            if not can_import:
                raise
            # No local fallback exists, so this second import attempt is strict:
            # if the server is unavailable, report that instead of hiding it
            # behind a local "object is not registered" error.
            imported = self.import_server_object(
                object_name,
                version=version,
                owner_id=server_owner_ref,
                library=library,
            )
            resolved_version_id = imported["current_version"]["version_id"]
            imported_owner_id = imported["current_version"].get("owner_id")
            if imported_owner_id is not None:
                local_owner_id = validate_name(str(imported_owner_id))
            state = self.store.create_run(
                object_name,
                args=args,
                kwargs=kwargs,
                output=output,
                timeout_seconds=timeout_seconds,
                version=version,
                object_version_id=resolved_version_id,
                function=function,
                owner_id=local_owner_id,
                library=library,
                runtimes=runtimes,
                keep=keep,
                runtime_port_adapters=runtime_port_adapters,
                runtime_library_adapter_refs=runtime_library_adapter_refs,
                runtime_adapter_semantic_advisories=runtime_adapter_semantic_advisories,
                runtime_library_adapter_sources=runtime_library_adapter_sources,
                _runtime_library_adapter_execution_target=(_runtime_library_adapter_execution_target),
                adapter_policy=adapter_policy,
                custom_remote_allowed=custom_remote_allowed,
                report_local_run=report_local_run,
                _browser_adapter_admission=_browser_adapter_admission,
                _runtime_adapter_semantic_claim=_runtime_adapter_semantic_claim,
            )
        self._bind_lifecycle_run_lease(str(state["id"]), lifecycle_lease)
        try:
            state = self._prepare_node_runtime_environments_for_run(state, report_local_run=report_local_run)
        except Exception as exc:
            return self._fail_run_before_worker(state, report_local_run=report_local_run, error=str(exc) or repr(exc))
        self._update_local_run(
            state["id"],
            report_local_run=report_local_run,
            status="starting",
        )
        self._start_run_thread(state["id"], report_local_run)
        return self.store.get_run(state["id"])

    def start_guarded_local_run(
        self,
        admission: GuardedLocalRunAdmission,
    ) -> dict[str, Any]:
        """Atomically close lifecycle admission around an exact guarded Run."""

        try:
            lease = self.lifecycle.reserve_current_or_root(
                "guarded_local_run",
                queued=True,
            )
        except LifecycleAdmissionClosed as exc:
            raise GuardedLocalRunError(
                "daemon_draining",
                HTTPStatus.CONFLICT,
            ) from exc
        lease.activate()
        lease.attach_current_thread()
        try:
            return self._start_guarded_local_run_admitted(
                admission,
                lifecycle_lease=lease,
            )
        except Exception:
            self._release_unbound_lifecycle_lease(lease)
            raise
        finally:
            lease.detach_current_thread()

    def _start_guarded_local_run_admitted(
        self,
        admission: GuardedLocalRunAdmission,
        *,
        lifecycle_lease: LifecycleWorkLease,
    ) -> dict[str, Any]:
        """Atomically admit one exact current local Object call.

        The shared registry lock is retained from the first authoritative
        comparison through Run insertion. Object publication uses the same
        reentrant lock, so it cannot advance the current pointer inside this
        admission decision.
        """

        try:
            with self.store._storage._lock:
                home_lock = self.daemon_home_lock
                identity = self.daemon_identity
                if (
                    identity is None
                    or home_lock is None
                    or not home_lock.is_acquired
                    or home_lock.identity is not identity
                ):
                    raise GuardedLocalRunError(
                        "live_identity_unavailable",
                        HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                if admission.daemon_instance_id != identity.instance_id:
                    raise GuardedLocalRunError(
                        "daemon_instance_mismatch",
                        HTTPStatus.PRECONDITION_FAILED,
                    )
                if admission.daemon_generation != identity.generation:
                    raise GuardedLocalRunError(
                        "daemon_generation_mismatch",
                        HTTPStatus.PRECONDITION_FAILED,
                    )

                try:
                    object_record = self.store.get_object_version(
                        admission.object.selected_version_id,
                        include_yaml=False,
                    )
                except KeyError as exc:
                    raise GuardedLocalRunError(
                        "object_not_found",
                        HTTPStatus.PRECONDITION_FAILED,
                    ) from exc

                expected_object_identity = (
                    admission.object.owner_id,
                    admission.object.library,
                    admission.object.object_name,
                    admission.object.object_id,
                    admission.object.origin,
                )
                authoritative_object_identity = (
                    object_record.get("owner_id"),
                    object_record.get("library"),
                    object_record.get("name"),
                    object_record.get("id"),
                    object_record.get("origin"),
                )
                if authoritative_object_identity != expected_object_identity:
                    raise GuardedLocalRunError(
                        "object_identity_mismatch",
                        HTTPStatus.PRECONDITION_FAILED,
                    )
                if object_record.get("current_version_id") != admission.object.expected_current_version_id:
                    raise GuardedLocalRunError(
                        "current_version_mismatch",
                        HTTPStatus.PRECONDITION_FAILED,
                    )
                if (
                    object_record.get("version_id") != admission.object.selected_version_id
                    or object_record.get("version") != admission.object.selected_version
                ):
                    raise GuardedLocalRunError(
                        "object_version_mismatch",
                        HTTPStatus.PRECONDITION_FAILED,
                    )
                if f"sha256:{object_record.get('content_hash')}" != admission.content_hash:
                    raise GuardedLocalRunError(
                        "content_binding_mismatch",
                        HTTPStatus.PRECONDITION_FAILED,
                    )
                validate_signature_arguments(admission, object_record)

                receipt: dict[str, Any] | None = None
                admitted_state: dict[str, Any] | None = None
                admission_committed = False

                def bind_receipt_before_commit(candidate: dict[str, Any]) -> None:
                    nonlocal admitted_state, receipt
                    if (
                        candidate.get("status") != "queued"
                        or candidate.get("object_id") != admission.object.object_id
                        or candidate.get("object_version_id") != admission.object.selected_version_id
                    ):
                        raise GuardedLocalRunError(
                            "admission_failed",
                            HTTPStatus.SERVICE_UNAVAILABLE,
                        )
                    receipt = build_guarded_local_run_receipt(
                        admission,
                        object_record=object_record,
                        run_state=candidate,
                    )
                    admitted_state = candidate

                def confirm_admission_commit() -> None:
                    nonlocal admission_committed
                    admission_committed = True

                try:
                    self.store.create_run(
                        str(object_record["name"]),
                        kwargs=admission.keyword_arguments(),
                        output=admission.output_selector,
                        timeout_seconds=timeout_ms_to_seconds(admission.timeout_ms),
                        object_version_id=admission.object.selected_version_id,
                        function=None,
                        owner_id=admission.object.owner_id,
                        library=admission.object.library,
                        keep=retention_to_keep(admission.retention),
                        report_local_run=True,
                        _precommit_check=bind_receipt_before_commit,
                        _postcommit_confirm=confirm_admission_commit,
                    )
                except Exception as exc:
                    if not admission_committed or receipt is None or admitted_state is None:
                        raise GuardedLocalRunError(
                            "admission_failed",
                            HTTPStatus.SERVICE_UNAVAILABLE,
                        ) from exc
                if not admission_committed or receipt is None or admitted_state is None:
                    raise GuardedLocalRunError(
                        "admission_failed",
                        HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                self._bind_lifecycle_run_lease(
                    str(admitted_state["id"]),
                    lifecycle_lease,
                )
        except GuardedLocalRunError:
            raise
        except Exception as exc:
            raise GuardedLocalRunError(
                "admission_failed",
                HTTPStatus.SERVICE_UNAVAILABLE,
            ) from exc

        try:
            self._continue_guarded_local_run(admitted_state)
        except Exception:
            LOGGER.error(
                "accepted guarded local Run continuation failed",
                extra={"spl_event": "guarded_local_run_continuation_failure"},
            )
        return receipt

    def _continue_guarded_local_run(self, state: dict[str, Any]) -> None:
        """Prepare and schedule an already accepted Run without changing its receipt."""

        try:
            prepared = self._prepare_node_runtime_environments_for_run(
                state,
                report_local_run=True,
            )
        except Exception as exc:
            try:
                self._fail_run_before_worker(
                    state,
                    report_local_run=True,
                    error=str(exc) or repr(exc),
                )
            except Exception:
                LOGGER.exception(
                    "accepted guarded local Run could not record preparation failure",
                    extra={"spl_event": "guarded_local_run_preparation_failure"},
                )
            return

        try:
            self._update_local_run(
                prepared["id"],
                report_local_run=True,
                status="starting",
            )
            self._start_run_thread(prepared["id"], True)
        except Exception:
            try:
                self._fail_run_before_worker(
                    prepared,
                    report_local_run=True,
                    error="accepted local Run could not be scheduled",
                )
            except Exception:
                LOGGER.exception(
                    "accepted guarded local Run could not record scheduling failure",
                    extra={"spl_event": "guarded_local_run_scheduling_failure"},
                )

    def resume_run(
        self,
        run_id: str,
        *,
        from_: Any,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        adapters: dict[str, Any] | None = None,
        runtimes: str | dict[str, str] | None = None,
        keep: Any = True,
        report_local_run: bool = True,
    ) -> dict[str, Any]:
        """Atomically admit one local resume lineage."""

        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )
        lease = self.lifecycle.reserve_current_or_root("local_run", queued=True)
        lease.activate()
        lease.attach_current_thread()
        try:
            return self._resume_run_admitted(
                run_id,
                from_=from_,
                kwargs=kwargs,
                output=output,
                timeout_seconds=timeout_seconds,
                adapters=adapters,
                runtimes=runtimes,
                keep=keep,
                report_local_run=report_local_run,
                lifecycle_lease=lease,
            )
        except Exception:
            self._release_unbound_lifecycle_lease(lease)
            raise
        finally:
            lease.detach_current_thread()

    def _resume_run_admitted(
        self,
        run_id: str,
        *,
        from_: Any,
        kwargs: dict[str, Any] | None = None,
        output: str | None = None,
        timeout_seconds: float | None = None,
        adapters: dict[str, Any] | None = None,
        runtimes: str | dict[str, str] | None = None,
        keep: Any = True,
        report_local_run: bool = True,
        lifecycle_lease: LifecycleWorkLease,
    ) -> dict[str, Any]:
        """Create a child run that resumes one retained daemon pipeline run."""

        if m_manifest.normalize_keep(keep) is False:
            raise ValueError(
                "keep=False is incompatible with resume because the child run would discard the state needed "
                "for another resume; use keep=True (keep='on_failure' retains the child only if it fails)"
            )
        validate_timeout_seconds(
            timeout_seconds,
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )
        parent = self.store.get_run(validate_name(run_id))
        if str(parent.get("status")) not in {"failed", "succeeded"}:
            raise RuntimeError(
                "resume requires a terminal retained run; current status is `{}`".format(parent.get("status"))
            )
        parent_run_dir = Path(str(parent["run_dir"]))
        if not parent.get("run_dir") or not parent_run_dir.is_dir():
            raise RuntimeError(
                "resume requires retained run files, but run `{}` was removed by its keep policy; "
                "launch the parent with keep=True".format(parent["id"])
            )
        parent_manifest_path = self._worker_manifest_path(parent_run_dir)
        if parent_manifest_path is not None:
            parent_manifest_dir = parent_manifest_path.parent
            try:
                parent_manifest = cast(
                    dict[str, Any],
                    json.loads(parent_manifest_path.read_text(encoding="utf-8")),
                )
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError("resume requires a readable retained worker manifest") from exc
        else:
            manifest = parent.get("manifest")
            if not isinstance(manifest, dict):
                raise RuntimeError("resume requires a retained pipeline manifest")
            parent_manifest = manifest
            parent_manifest_dir = parent_run_dir

        object_record = self.store.get_object_version(parent["object_version_id"])
        pipeline = self._load_pipeline_entrypoint(
            parent_run_dir / "object.yaml",
            str(parent["entrypoint"]),
        )
        m_resume.plan_resume(
            pipeline=pipeline,
            parent_manifest=parent_manifest,
            parent_run_dir=parent_manifest_dir,
            from_=from_,
            kwargs=kwargs or {},
        )

        raw_parent_input = parent.get("input")
        parent_input = cast(dict[str, Any], raw_parent_input) if isinstance(raw_parent_input, dict) else {}
        child_kwargs = dict(parent_input.get("kwargs") or {})
        if kwargs is not None:
            child_kwargs.update(kwargs)
        resume_payload = {
            "parent_run_id": parent["id"],
            "parent_run_dir": str(parent_manifest_dir),
            "from": from_,
            "kwargs": kwargs or {},
        }
        if adapters is not None:
            resume_payload["adapters"] = adapters

        state = self.store.create_run(
            str(parent["object"]),
            kwargs=child_kwargs,
            output=output if output is not None else parent_input.get("output"),
            timeout_seconds=timeout_seconds if timeout_seconds is not None else parent_input.get("timeout_seconds"),
            object_version_id=object_record["version_id"],
            function=parent.get("function"),
            runtimes=runtimes if runtimes is not None else parent_input.get("runtimes"),
            keep=keep,
            parent_run_id=parent["id"],
            resume=resume_payload,
            report_local_run=report_local_run,
        )
        self._bind_lifecycle_run_lease(str(state["id"]), lifecycle_lease)
        state = self._stage_object_docker_resume_parent(
            state,
            object_record=object_record,
            parent_manifest_dir=parent_manifest_dir,
            report_local_run=report_local_run,
        )
        try:
            state = self._prepare_node_runtime_environments_for_run(state, report_local_run=report_local_run)
        except Exception as exc:
            return self._fail_run_before_worker(state, report_local_run=report_local_run, error=str(exc) or repr(exc))
        self._update_local_run(
            state["id"],
            report_local_run=report_local_run,
            status="starting",
        )
        self._start_run_thread(state["id"], report_local_run)
        return self.store.get_run(state["id"])

    def _start_run_thread(self, run_id: str, report_local_run: bool) -> None:
        thread = threading.Thread(
            target=self._execute_run_with_lifecycle,
            args=(run_id, report_local_run),
            name=f"spl-run-{run_id}",
            daemon=True,
        )
        with self._run_threads_lock:
            self._run_threads = [candidate for candidate in self._run_threads if candidate.is_alive()]
            self._run_threads.append(thread)
        try:
            thread.start()
        except Exception:
            self._release_lifecycle_run_lease(run_id)
            raise

    def _execute_run_with_lifecycle(
        self,
        run_id: str,
        report_local_run: bool,
    ) -> None:
        """Run the worker while retaining exact process-local admission."""

        with self._lifecycle_run_leases_lock:
            lease = self._lifecycle_run_leases.get(run_id)
        if lease is None:
            self.lifecycle.record_unknown_work()
            return
        lease.activate()
        lease.attach_current_thread()
        try:
            self._execute_run(run_id, report_local_run)
        except Exception:
            # An accepted local Run that escapes its normal terminal-state
            # write is not safe to classify as idle.  Do not log the exception
            # value because it can contain user arguments or paths.
            self.lifecycle.record_unknown_work()
            LOGGER.error(
                "accepted local Run escaped its terminal commit",
                extra={"spl_event": "local_run_outcome_unproven", "run_id": run_id},
            )
        finally:
            lease.detach_current_thread()
            self._release_lifecycle_run_lease(run_id)

    def _fail_run_before_worker(
        self,
        state: dict[str, Any],
        *,
        report_local_run: bool,
        error: str,
    ) -> dict[str, Any]:
        run_id = str(state["id"])
        try:
            final_state = self._update_local_run_terminal(
                run_id,
                report_local_run=report_local_run,
                status="failed",
                finished_at=utc_now(),
                error=error,
            )
            return final_state if final_state is not None else self.store.get_run(run_id)
        finally:
            self._release_lifecycle_run_lease(run_id)

    def _stage_object_docker_resume_parent(
        self,
        state: dict[str, Any],
        *,
        object_record: dict[str, Any],
        parent_manifest_dir: Path,
        report_local_run: bool,
    ) -> dict[str, Any]:
        runtime_config = normalize_runtime_config(object_record.get("runtime_config"))
        if runtime_config.get("mode") != "docker":
            return state
        input_payload = dict(state.get("input") or {})
        raw_resume = input_payload.get("resume")
        if not isinstance(raw_resume, dict):
            return state

        staged_name = "resume-parent"
        run_dir = Path(state["run_dir"])
        staged_parent_dir = run_dir / staged_name
        shutil.rmtree(staged_parent_dir, ignore_errors=True)
        shutil.copytree(parent_manifest_dir, staged_parent_dir)

        resume_payload = dict(raw_resume)
        resume_payload["parent_run_dir"] = staged_name
        input_payload["resume"] = resume_payload
        write_json(run_dir / "input.json", input_payload)
        return self._update_local_run(
            state["id"],
            report_local_run=report_local_run,
            input=input_payload,
        )

    def _prepare_node_runtime_environments_for_run(
        self,
        state: dict[str, Any],
        *,
        report_local_run: bool,
    ) -> dict[str, Any]:
        input_payload = dict(state.get("input") or {})
        object_record = self.store.get_object_version(state["object_version_id"])
        pipeline = self._load_optional_pipeline_entrypoint(object_record, str(state["entrypoint"]))
        if not self._run_selects_node_docker(
            pipeline,
            runtime_config=input_payload.get("runtime_config"),
            runtimes=input_payload.get("runtimes"),
        ):
            return state

        raw_node_runtime_environments = input_payload.get("node_runtime_environments")
        node_runtime_environments = (
            dict(raw_node_runtime_environments) if isinstance(raw_node_runtime_environments, dict) else {}
        )
        node_runtime_environments[DOCKER_NODE_RUNTIME] = self._resolve_node_docker_environment(object_record)
        input_payload["node_runtime_environments"] = node_runtime_environments
        run_dir = Path(state["run_dir"])
        write_json(run_dir / "input.json", input_payload)
        return self._update_local_run(
            state["id"],
            report_local_run=report_local_run,
            input=input_payload,
        )

    def _load_optional_pipeline_entrypoint(
        self,
        object_record: dict[str, Any],
        entrypoint: str,
    ) -> DPipeline | None:
        """Read pipeline runtime tags without executing Object imports.

        Run admission happens in the daemon's deliberately dependency-light
        process, while Object imports belong to the selected worker
        environment.  Importing the YAML bundle here made an ordinary
        Function fail before its worker started whenever a user dependency
        (for example pandas) was absent from the daemon environment.

        The safe loader constructs only SPL IR dataclasses.  That is enough to
        inspect ``DPipeline.tags`` for per-node Docker selection and keeps
        every user import behind the worker boundary.
        """

        if object_record.get("kind") != "pipeline":
            return None
        for document in yaml.load_all(str(object_record["yaml"]), Loader=SPLSafeLoader):
            if not isinstance(document, list) or not document:
                continue
            target = document[0]
            if isinstance(target, DPipeline) and target.name == entrypoint:
                return target
        return None

    def _run_selects_node_docker(
        self,
        pipeline: Pipeline | DPipeline | None,
        *,
        runtime_config: Any,
        runtimes: Any,
    ) -> bool:
        config = runtime_config if isinstance(runtime_config, dict) else {}
        if config.get("node_runtime") == DOCKER_NODE_RUNTIME:
            return True
        if runtimes == DOCKER_NODE_RUNTIME:
            return pipeline is not None
        if isinstance(runtimes, dict) and any(value == DOCKER_NODE_RUNTIME for value in runtimes.values()):
            return True
        if pipeline is None:
            return False
        return any(
            node_tags.get(RUNTIME_TAG_NAME) == DOCKER_NODE_RUNTIME for node_tags in (pipeline.tags or {}).values()
        )

    def _resolve_node_docker_environment(self, object_record: dict[str, Any]) -> dict[str, Any]:
        runtime_config = normalize_runtime_config(object_record.get("runtime_config"))
        explicit_image = self._explicit_node_docker_image(runtime_config)
        if explicit_image is not None:
            return {
                "image_tag": explicit_image,
                "spec_hash": explicit_docker_image_spec_hash(explicit_image),
                "source": "runtime_config.docker.image",
            }

        docker_record = self._node_docker_build_record(object_record, runtime_config)
        environment_record = self.docker_environment_manager.ensure_ready(docker_record, wait=True)
        return {
            "image_tag": environment_record["image_tag"],
            "spec_hash": environment_record.get("spec_hash"),
            "source": "object-env-spec",
        }

    def _explicit_node_docker_image(self, runtime_config: dict[str, Any]) -> str | None:
        docker_config = runtime_config.get("docker")
        if not isinstance(docker_config, dict):
            return None
        image = docker_config.get("image")
        if image is None:
            return None
        image_tag = str(image)
        if not image_tag:
            raise ValueError('runtime_config["docker"]["image"] must be a non-empty string')
        return image_tag

    def _node_docker_build_record(
        self,
        object_record: dict[str, Any],
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        if runtime_config.get("mode") == "docker":
            return object_record
        raw_docker_config = runtime_config.get("docker")
        docker_config: dict[str, Any] = dict(raw_docker_config) if isinstance(raw_docker_config, dict) else {}
        build_config = {"mode": "docker", **dict(docker_config)}
        build_config.pop("image", None)
        return {**object_record, "runtime_config": build_config}

    def _execute_run(self, run_id: str, report_local_run: bool = True) -> None:
        """Launch the worker process and persist the final run state."""

        state = self.store.get_run(run_id)
        object_record = self.store.get_object_version(state["object_version_id"])
        raw_run_input = state.get("input")
        run_input: dict[str, Any] = raw_run_input if isinstance(raw_run_input, dict) else {}
        effective_runtime_config = run_input.get("runtime_config")
        if isinstance(effective_runtime_config, dict):
            object_record = {
                **object_record,
                "runtime_config": effective_runtime_config,
            }
        runtime_port_adapters = run_input.get("runtime_port_adapters")
        if isinstance(runtime_port_adapters, dict):
            merged_distributions = run_input.get("runtime_adapter_distributions")
            if not isinstance(merged_distributions, list):
                raise RuntimeError("runtime adapter environment preflight metadata is missing")
            object_record = {
                **object_record,
                "distributions": merged_distributions,
                "runtime_port_adapters": True,
            }
        run_dir = Path(state["run_dir"])
        result_path = Path(state["result_path"])
        artifacts_dir = Path(state["artifacts_dir"])
        input_path = run_dir / "input.json"
        object_yaml_path = run_dir / "object.yaml"
        env_spec_path = run_dir / "env-spec.json"
        remote_signatures_path = run_dir / "remote-signatures.json"
        stdout_path = run_dir / "stdout.txt"
        stderr_path = run_dir / "stderr.txt"
        worker_path = Path(__file__).with_name("worker.py")
        spl_free_runner_path = Path(__file__).with_name("spl_free_runner.py")
        worker_runtime_marker_path = run_dir / WORKER_RUNTIME_MARKER_FILE
        worker_runtime_adapter_failure_path = run_dir / WORKER_RUNTIME_ADAPTER_FAILURE_FILE

        object_yaml_path.write_text(object_record["yaml"], encoding="utf-8")
        write_json(env_spec_path, object_record["distributions"])
        write_json(
            remote_signatures_path,
            {"nodes": [node for node in object_record.get("pipeline_nodes") or [] if node.get("kind") == "remote"]},
        )

        self._update_local_run(
            run_id,
            report_local_run=report_local_run,
            status="preparing_environment",
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
        )

        timeout: float | None = None
        workdir = Path(object_record.get("workdir") or str(run_dir))
        workdir.mkdir(parents=True, exist_ok=True)
        generated_modules_dir = self.store.home / "generated-modules"
        ctx = RunContext(
            object_record=object_record,
            run_id=run_id,
            run_dir=run_dir,
            workdir=workdir,
            input_path=input_path,
            object_yaml_path=object_yaml_path,
            result_path=result_path,
            artifacts_dir=artifacts_dir,
            env_spec_path=env_spec_path,
            remote_signatures_path=remote_signatures_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            worker_path=worker_path,
            spl_free_runner_path=spl_free_runner_path,
            generated_modules_dir=generated_modules_dir,
            worker_runtime_marker_path=worker_runtime_marker_path,
            entrypoint=state["entrypoint"],
            daemon_base_url=self.daemon_base_url,
        )

        adapter_failure_stage = "environment_preflight"
        try:
            timeout = self._read_timeout(input_path)
            backend = self.runtime_backends.backend_for(object_record)
            environment_status = backend.status_for_object(object_record)
            if environment_status.get("spec_hash"):
                self._update_local_run(
                    run_id,
                    report_local_run=report_local_run,
                    env_build_hash=environment_status["spec_hash"],
                    runtime_build_hash=environment_status["spec_hash"],
                )
            with backend:
                environment_record = backend.ensure_ready(object_record)
                command = backend.build_command(ctx)
                worker_runtime = read_worker_runtime_marker(worker_runtime_marker_path)
                self._log_worker_runtime(object_record, worker_runtime)
                env = os.environ.copy()
                env.pop(DAEMON_API_TOKEN_ENV, None)
                env.pop(CALLBACK_CAPABILITY_ENV, None)
                worker_identity_env = getattr(self.docker_pool, "worker_identity_env", None)
                if callable(worker_identity_env):
                    env.update(worker_identity_env(run_id))
                if worker_runtime is not None and worker_runtime.get("worker_runtime") == LEGACY_WORKER_RUNTIME:
                    env["PYTHONPATH"] = self._worker_pythonpath(env)
                remote_nodes = [
                    node
                    for node in object_record.get("pipeline_nodes") or []
                    if isinstance(node, dict) and node.get("kind") == "remote"
                ]
                if remote_nodes:
                    env[CALLBACK_CAPABILITY_ENV] = self.callback_capabilities.mint(
                        run_id,
                        remote_nodes,
                        ttl_seconds=callback_capability_ttl_seconds(timeout),
                    )
                self._log_interpreter_substitution(object_record, environment_record)
                self._update_local_run(
                    run_id,
                    report_local_run=report_local_run,
                    status="running",
                    started_at=utc_now(),
                    command=command,
                    env_build_hash=environment_record["spec_hash"],
                    runtime_build_hash=environment_record["spec_hash"],
                    **backend.run_state_fields(),
                )
                adapter_failure_stage = "object_execution"
                worker_runtime_adapter_failure_path.unlink(missing_ok=True)
                try:
                    try:
                        completed = run_process_tree(
                            command,
                            cwd=workdir,
                            env=env,
                            timeout=timeout,
                        )
                    except subprocess.TimeoutExpired:
                        self.callback_capabilities.revoke_run(run_id)
                        try:
                            backend.handle_timeout(ctx)
                        except Exception:
                            LOGGER.exception(
                                "runtime backend failed to quarantine timed-out run %s",
                                run_id,
                                extra={"spl_event": "run_timeout_quarantine_failed", "run_id": run_id},
                            )
                        quarantine_run = getattr(self.docker_pool, "quarantine_run_containers", None)
                        if callable(quarantine_run):
                            try:
                                quarantine_run(run_id)
                            except Exception:
                                LOGGER.exception(
                                    "Docker cleanup failed for timed-out run %s",
                                    run_id,
                                    extra={"spl_event": "run_timeout_docker_cleanup_failed", "run_id": run_id},
                                )
                        raise
                finally:
                    env.pop(CALLBACK_CAPABILITY_ENV, None)
                adapter_failure_stage = "worker_save"
                stdout_path.write_text(completed.stdout, encoding="utf-8")
                stderr_path.write_text(completed.stderr, encoding="utf-8")
                after_run = backend.after_run(ctx)
                if after_run:
                    self._update_local_run(
                        run_id,
                        report_local_run=report_local_run,
                        **after_run,
                    )

                if completed.returncode == 0 and result_path.exists():
                    result_payload = json.loads(result_path.read_text(encoding="utf-8"))
                    manifest_payload = result_payload.pop("manifest", None)
                    if isinstance(runtime_port_adapters, dict):
                        _verify_terminal_runtime_output_files(
                            artifacts_dir,
                            result_payload.get("runtime_port_adapter_outputs", []),
                        )
                    if backend.process_result(ctx, result_payload):
                        write_json(result_path, result_payload)
                    elif manifest_payload is not None:
                        write_json(result_path, result_payload)
                    success_changes: dict[str, Any] = {
                        "status": "succeeded",
                        "finished_at": utc_now(),
                        "returncode": completed.returncode,
                        "result": result_payload,
                        "stdout_text": completed.stdout,
                        "stderr_text": completed.stderr,
                    }
                    if isinstance(manifest_payload, dict):
                        success_changes["manifest"] = self._daemon_manifest_payload(
                            manifest_payload,
                            run_id=run_id,
                            parent_run_id=state.get("parent_run_id"),
                            object_record=object_record,
                        )
                    initial_runtime_section = (state.get("manifest") or {}).get("runtime_port_adapters")
                    if (
                        isinstance(initial_runtime_section, dict)
                        and not isinstance(runtime_port_adapters, dict)
                        and "manifest" in success_changes
                    ):
                        success_changes["manifest"] = {
                            **success_changes["manifest"],
                            "runtime_port_adapters": initial_runtime_section,
                        }
                    if isinstance(runtime_port_adapters, dict):
                        success_manifest = dict(success_changes.get("manifest") or state.get("manifest") or {})
                        success_changes["manifest"] = _manifest_with_runtime_adapter_outputs(
                            success_manifest,
                            dict(state.get("manifest") or {}),
                            runtime_port_adapters,
                            result_payload.get("runtime_port_adapter_outputs", []),
                            runtime_library_adapters=(
                                run_input.get("runtime_library_adapters")
                                if isinstance(run_input.get("runtime_library_adapters"), dict)
                                else None
                            ),
                            custom_used=_worker_runtime_custom_adapter_used(run_dir),
                        )
                    # Terminal writes are the final store access in the run thread.
                    self._update_local_run_terminal(
                        run_id,
                        report_local_run=report_local_run,
                        **success_changes,
                    )
                else:
                    error = completed.stderr.strip() or completed.stdout.strip()
                    if completed.returncode == 0:
                        error = "worker finished without writing result.json"
                    adapter_failure = _validated_runtime_adapter_failure(
                        _read_worker_runtime_adapter_failure(
                            worker_runtime_adapter_failure_path,
                            returncode=completed.returncode,
                        ),
                        dict(state.get("manifest") or {}),
                    )
                    if adapter_failure is None and isinstance(runtime_port_adapters, dict):
                        adapter_failure = {
                            "stage": "object_execution",
                            "reason": "adapted worker failed",
                        }
                    safe_stdout = completed.stdout
                    safe_stderr = completed.stderr
                    if adapter_failure is not None:
                        error = (
                            f"{adapter_failure['code']}: {adapter_failure['message']}"
                            if adapter_failure.get("schema_version") == 1
                            else f"{adapter_failure['stage']}: {adapter_failure['reason']}"
                        )
                        safe_stdout = ""
                        safe_stderr = error
                        stdout_path.write_text(safe_stdout, encoding="utf-8")
                        stderr_path.write_text(safe_stderr, encoding="utf-8")
                    manifest_payload = self._read_worker_manifest(run_dir)
                    failure_changes: dict[str, Any] = {
                        "status": "failed",
                        "finished_at": utc_now(),
                        "returncode": completed.returncode,
                        "error": error,
                        "stdout_text": safe_stdout,
                        "stderr_text": safe_stderr,
                    }
                    projected_manifest = (
                        self._daemon_manifest_payload(
                            manifest_payload,
                            run_id=run_id,
                            parent_run_id=state.get("parent_run_id"),
                            object_record=object_record,
                        )
                        if manifest_payload is not None
                        else dict(state.get("manifest") or {})
                    )
                    initial_runtime_section = (state.get("manifest") or {}).get("runtime_port_adapters")
                    if isinstance(initial_runtime_section, dict):
                        projected_manifest = {
                            **projected_manifest,
                            "runtime_port_adapters": initial_runtime_section,
                        }
                    if manifest_payload is not None:
                        failure_changes["manifest"] = projected_manifest
                    if adapter_failure is not None:
                        failure_changes["manifest"] = _manifest_with_runtime_adapter_failure(
                            projected_manifest,
                            dict(state.get("manifest") or {}),
                            adapter_failure,
                            custom_used=_worker_runtime_custom_adapter_used(run_dir),
                        )
                    # Terminal writes are the final store access in the run thread.
                    self._update_local_run_terminal(
                        run_id,
                        report_local_run=report_local_run,
                        **failure_changes,
                    )
        except subprocess.TimeoutExpired as exc:
            stdout = self._subprocess_text(exc.stdout)
            stderr = self._subprocess_text(exc.stderr)
            timeout_adapter_failure = None
            if isinstance(runtime_port_adapters, dict):
                timeout_adapter_failure = {
                    "stage": "object_execution",
                    "reason": "adapted worker timed out",
                }
                stdout = ""
                stderr = "object_execution: adapted worker timed out"
            stdout_path.write_text(stdout, encoding="utf-8")
            stderr_path.write_text(stderr, encoding="utf-8")
            timeout_changes: dict[str, Any] = {
                "status": "failed",
                "finished_at": utc_now(),
                "error": f"run timed out after {timeout} seconds",
                "stdout_text": stdout,
                "stderr_text": stderr,
            }
            manifest_payload = self._read_worker_manifest(run_dir)
            if manifest_payload is not None:
                timeout_changes["manifest"] = self._daemon_manifest_payload(
                    manifest_payload,
                    run_id=run_id,
                    parent_run_id=state.get("parent_run_id"),
                    object_record=object_record,
                )
            if timeout_adapter_failure is not None:
                timeout_changes["manifest"] = _manifest_with_runtime_adapter_failure(
                    dict(timeout_changes.get("manifest") or state.get("manifest") or {}),
                    dict(state.get("manifest") or {}),
                    timeout_adapter_failure,
                    custom_used=_worker_runtime_custom_adapter_used(run_dir),
                )
            # Terminal writes are the final store access in the run thread.
            self._update_local_run_terminal(run_id, report_local_run=report_local_run, **timeout_changes)
        except Exception as exc:
            adapted_error = isinstance(runtime_port_adapters, dict)
            error_changes: dict[str, Any] = {
                "status": "failed",
                "finished_at": utc_now(),
                "error": (f"{adapter_failure_stage}: adapted run failed safely" if adapted_error else repr(exc)),
            }
            manifest_payload = self._read_worker_manifest(run_dir)
            if manifest_payload is not None:
                error_changes["manifest"] = self._daemon_manifest_payload(
                    manifest_payload,
                    run_id=run_id,
                    parent_run_id=state.get("parent_run_id"),
                    object_record=object_record,
                )
            if adapted_error:
                error_changes["manifest"] = _manifest_with_runtime_adapter_failure(
                    dict(error_changes.get("manifest") or state.get("manifest") or {}),
                    dict(state.get("manifest") or {}),
                    {"stage": adapter_failure_stage, "reason": "adapted run failed safely"},
                    custom_used=_worker_runtime_custom_adapter_used(run_dir),
                )
            # Terminal writes are the final store access in the run thread.
            self._update_local_run_terminal(run_id, report_local_run=report_local_run, **error_changes)

    def _worker_manifest_path(self, run_dir: Path) -> Path | None:
        handoff = run_dir / WORKER_MANIFEST_HANDOFF_FILE
        if handoff.is_file():
            return handoff
        state_dir = run_dir / "pipeline-state"
        if not state_dir.exists():
            return None
        manifests = sorted(
            state_dir.glob("*/manifest.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        return manifests[0] if manifests else None

    def _read_worker_manifest(self, run_dir: Path) -> dict[str, Any] | None:
        manifest_path = self._worker_manifest_path(run_dir)
        if manifest_path is None:
            return None
        try:
            return cast(dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            return None

    def _daemon_manifest_payload(
        self,
        manifest: dict[str, Any],
        *,
        run_id: str,
        parent_run_id: Any,
        object_record: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(manifest)
        payload["run_id"] = run_id
        payload["parent_run_id"] = parent_run_id
        pipeline = dict(payload.get("pipeline") or {})
        pipeline["object_version_id"] = object_record["version_id"]
        pipeline["content_hash"] = object_record.get("content_hash")
        payload["pipeline"] = pipeline
        return payload

    def _load_pipeline_entrypoint(self, object_yaml_path: Path, entrypoint: str) -> Pipeline:
        namespace = self._pipeline_import_namespace()
        spl_import_from_file(object_yaml_path, namespace)
        target = namespace.get(entrypoint)
        if not isinstance(target, Pipeline):
            raise RuntimeError("daemon resume is supported only for registered pipelines")
        return target

    @staticmethod
    def _pipeline_import_namespace() -> dict[str, Any]:
        """Seed names omitted by the current ``DNodeRemote`` unparser."""

        from uuid import UUID

        from spl.core.entities.node_remote import NodeRemote

        return {"NodeRemote": NodeRemote, "UUID": UUID}

    def _update_local_run(
        self,
        run_id: str,
        *,
        report_local_run: bool,
        **changes: Any,
    ) -> dict[str, Any]:
        state = self.store.update_run(run_id, **changes)
        if report_local_run:
            self.enqueue_local_run_update(state)
        return state

    def renew_run_delivery(self, run_id: str) -> dict[str, Any]:
        """Renew the bounded compatibility lease before serving run data."""

        return cast(
            dict[str, Any],
            self.run_registry_mutation(
                self._renew_run_delivery_admitted,
                run_id,
            ),
        )

    def _renew_run_delivery_admitted(self, run_id: str) -> dict[str, Any]:
        """Renew one already-admitted delivery lease."""

        state = self.store.renew_run_delivery(
            validate_name(run_id),
            lease_seconds=DEFAULT_RUN_DELIVERY_LEASE_SECONDS,
        )
        self._schedule_run_retention(state)
        return state

    def acknowledge_run_delivery(self, run_id: str) -> dict[str, Any]:
        """Record client consumption and immediately retry safe cleanup."""

        return cast(
            dict[str, Any],
            self.run_registry_mutation(
                self._acknowledge_run_delivery_admitted,
                run_id,
            ),
        )

    def _acknowledge_run_delivery_admitted(self, run_id: str) -> dict[str, Any]:
        """Acknowledge one already-admitted delivery mutation."""

        result = self.store.acknowledge_run_delivery(validate_name(run_id))
        self._cancel_run_retention_deadline(validate_name(run_id))
        return result

    def _schedule_run_retention(self, state: dict[str, Any]) -> None:
        """Schedule expiry cleanup for one transient local delivery lease."""

        stored_effective_status = state.get("retention_effective_status")
        decision_status = str(state["status"] if stored_effective_status is None else stored_effective_status)
        if (
            not state.get("retention_enforced")
            or not state.get("retention_delivery_required")
            or state.get("retention_delivery_acked")
            or m_manifest.retention_disposition(
                state["keep"],
                decision_status,
            )
            != "remove"
        ):
            return
        raw_deadline = state.get("retention_delivery_expires_at")
        if not raw_deadline:
            return
        try:
            deadline = datetime.fromisoformat(str(raw_deadline))
        except ValueError:
            LOGGER.error(
                "invalid run delivery lease deadline for %s: %r",
                state.get("id"),
                raw_deadline,
            )
            return
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        run_id = validate_name(str(state["id"]))
        with self._retention_condition:
            if self._retention_scheduler_stopped:
                return
            self._retention_deadlines[run_id] = deadline.timestamp()
            if not self._retention_scheduler_started:
                self._retention_scheduler_started = True
                self._retention_scheduler.start()
            self._retention_condition.notify_all()

    def _cancel_run_retention_deadline(self, run_id: str) -> None:
        with self._retention_condition:
            self._retention_deadlines.pop(run_id, None)
            self._retention_condition.notify_all()

    def _schedule_run_retention_retry(self, run_id: str) -> None:
        """Retry a transient filesystem cleanup failure without a busy loop."""

        with self._retention_condition:
            if self._retention_scheduler_stopped:
                return
            self._retention_deadlines[run_id] = time.time() + RUN_RETENTION_CLEANUP_RETRY_SECONDS
            if not self._retention_scheduler_started:
                self._retention_scheduler_started = True
                self._retention_scheduler.start()
            self._retention_condition.notify_all()

    def _run_retention_scheduler(self) -> None:
        """Enforce all delivery deadlines from one bounded scheduler thread."""

        while True:
            due: list[str] = []
            with self._retention_condition:
                while not self._retention_scheduler_stopped:
                    if not self._retention_deadlines:
                        self._retention_condition.wait()
                        continue
                    now = time.time()
                    next_deadline = min(self._retention_deadlines.values())
                    delay = next_deadline - now
                    if delay > 0:
                        self._retention_condition.wait(timeout=delay)
                        continue
                    due = [run_id for run_id, deadline in self._retention_deadlines.items() if deadline <= now]
                    for run_id in due:
                        self._retention_deadlines.pop(run_id, None)
                    break
                else:
                    return

            for run_id in due:
                try:
                    outcome = cast(
                        dict[str, Any],
                        self.run_registry_mutation(
                            self.store.enforce_run_retention,
                            run_id,
                        ),
                    )
                    if outcome.get("reason") == "consumer-delivery-pending":
                        self._schedule_run_retention(self.store.get_run(run_id))
                except LifecycleAdmissionClosed:
                    # Drain owns shutdown. A later standalone startup sweep can
                    # recover the untouched durable retention decision.
                    continue
                except (KeyError, OSError, RuntimeError, ValueError) as exc:
                    LOGGER.error(
                        "run retention scheduler skipped %s: %s",
                        run_id,
                        exc,
                        extra={"spl_event": "run_retention_cleanup_failed", "run_id": run_id},
                    )
                    if isinstance(exc, OSError):
                        self._schedule_run_retention_retry(run_id)

    def _update_local_run_terminal(
        self,
        run_id: str,
        *,
        report_local_run: bool,
        **changes: Any,
    ) -> dict[str, Any] | None:
        lease = self._reserve_run_descendant(run_id, "terminal_commit")
        attached = False
        if lease is not None:
            lease.activate()
            if self.lifecycle.current_lineage_id is None:
                lease.attach_current_thread()
                attached = True
        try:
            return self._update_local_run_terminal_admitted(
                run_id,
                report_local_run=report_local_run,
                **changes,
            )
        finally:
            if lease is not None:
                if attached:
                    lease.detach_current_thread()
                lease.complete()

    def _update_local_run_terminal_admitted(
        self,
        run_id: str,
        *,
        report_local_run: bool,
        **changes: Any,
    ) -> dict[str, Any] | None:
        # Revoke the worker callback authority before publishing a terminal
        # status so a concurrent reader cannot observe terminal truth while a
        # capability for the completed Run is still usable.
        self.callback_capabilities.revoke_run(run_id)
        try:
            self._update_local_run(
                run_id,
                report_local_run=report_local_run,
                **changes,
            )
            if report_local_run:
                sync_state = self.store.sync_events.run_sync_state(run_id)
                if sync_state["terminal_queued"]:
                    self.store.runs.mark_retention_terminal_queued(run_id, sync_required=True)
                elif self.store.current_server_connection_credentials() is None:
                    self.store.runs.mark_retention_terminal_queued(run_id, sync_required=False)
            else:
                self.store.runs.set_retention_sync_required(run_id, required=True)
            outcome = self.store.enforce_run_retention(run_id)
            if outcome.get("reason") == "consumer-delivery-pending":
                self._schedule_run_retention(self.store.get_run(run_id))
            return self.store.get_run(run_id)
        except RuntimeError as exc:
            if str(exc) != "store is closed":
                raise
            LOGGER.warning("run state write skipped: store closed during shutdown")
            return None

    def sweep_run_retention(self) -> dict[str, Any]:
        """Recover and enforce safe terminal run cleanup after a restart or sync."""

        return cast(
            dict[str, Any],
            self.run_registry_mutation(self._sweep_run_retention_admitted),
        )

    def _sweep_run_retention_admitted(self) -> dict[str, Any]:
        """Run one already-admitted retention recovery sweep."""

        results: list[dict[str, Any]] = []
        for state in self.store.list_runs():
            if not state.get("retention_enforced"):
                continue
            stored_effective_status = state.get("retention_effective_status")
            decision_status = str(state["status"] if stored_effective_status is None else stored_effective_status)
            if (
                m_manifest.retention_disposition(
                    state["keep"],
                    decision_status,
                )
                != "remove"
            ):
                continue
            try:
                if not state.get("retention_terminal_queued"):
                    sync_state = self.store.sync_events.run_sync_state(str(state["id"]))
                    if sync_state["terminal_queued"]:
                        self.store.runs.mark_retention_terminal_queued(
                            str(state["id"]),
                            sync_required=True,
                        )
                    elif state.get("retention_report_mode") == "local":
                        if self.store.current_server_connection_credentials() is None:
                            self.store.runs.mark_retention_terminal_queued(
                                str(state["id"]),
                                sync_required=False,
                            )
                        else:
                            self.enqueue_local_run_update(state)
                            recovered = self.store.sync_events.run_sync_state(str(state["id"]))
                            if recovered["terminal_queued"]:
                                self.store.runs.mark_retention_terminal_queued(
                                    str(state["id"]),
                                    sync_required=True,
                                )
                outcome = self.store.enforce_run_retention(str(state["id"]))
                results.append(outcome)
                if outcome.get("reason") == "consumer-delivery-pending":
                    self._schedule_run_retention(self.store.get_run(str(state["id"])))
            except (OSError, RuntimeError, ValueError) as exc:
                LOGGER.error(
                    "run retention sweep skipped %s: %s",
                    state.get("id"),
                    exc,
                    extra={"spl_event": "run_retention_cleanup_failed", "run_id": state.get("id")},
                )
                if isinstance(exc, OSError):
                    self._schedule_run_retention_retry(validate_name(str(state["id"])))
                results.append({"id": state.get("id"), "removed": False, "reason": repr(exc)})
        return {"count": len(results), "runs": results}

    def _log_interpreter_substitution(
        self,
        object_record: dict[str, Any],
        environment_record: dict[str, Any],
    ) -> None:
        substitution = environment_record_interpreter_substitution(environment_record)
        if substitution is None:
            return
        payload = {
            "event": "interpreter_substitution",
            "object": object_record.get("name"),
            "version": object_record.get("version"),
            "version_id": object_record.get("version_id"),
            "authored_python": substitution.get("authored_python"),
            "authored_python_version": substitution.get("authored_python_version"),
            "resolved_python": substitution.get("resolved_python"),
            "resolved_python_version": substitution.get("resolved_python_version"),
            "reason": substitution.get("reason"),
            "reason_detail": substitution.get("reason_detail"),
        }
        LOGGER.info(
            "interpreter_substitution %s",
            m_json_contract.dumps(payload, ensure_ascii=True, sort_keys=True, separators=None),
            extra={
                "spl_event": "interpreter_substitution",
                "interpreter_substitution": payload,
            },
        )

    def enqueue_local_run_update(self, state: dict[str, Any]) -> dict[str, Any] | None:
        """Queue a local-only run status for central-server observability."""

        if self.store.current_server_connection_credentials() is None:
            return None
        event = self.store.enqueue_sync_event(
            "local_run_update",
            {"run": self._local_run_sync_payload(state)},
        )
        self._kick_server_sync()
        return event

    def _kick_server_sync(self, connection_id: str | None = None) -> None:
        credentials = (
            self.store.get_server_connection_credentials(connection_id)
            if connection_id is not None
            else self.store.current_server_connection_credentials()
        )
        if credentials is None:
            return
        if not self._server_channel_is_live(credentials, supervise_heartbeat=False):
            return
        thread = threading.Thread(
            target=self._sync_once_safely,
            args=(credentials["id"],),
            name=f"spl-server-sync-kick-{credentials['id']}",
            daemon=True,
        )
        thread.start()

    def _sync_once_safely(self, connection_id: str) -> None:
        try:
            self.sync_once(connection_id=connection_id)
        except Exception as exc:
            try:
                self._mark_server_channel_failure(
                    self.store.get_server_connection_credentials(connection_id),
                    error=exc,
                )
                self.store.record_server_connection_error(
                    connection_id,
                    status="heartbeat_failed",
                    error=repr(exc),
                )
            except RuntimeError as store_exc:
                if str(store_exc) != "store is closed":
                    raise

    def _local_run_sync_payload(self, state: dict[str, Any]) -> dict[str, Any]:
        object_label = self._local_run_object_label(state)
        full_artifacts: list[dict[str, Any]] | None = None
        preflight_omissions: tuple[str, ...] = ()
        if self.telemetry_policy.level == "full":
            full_artifacts, preflight_omissions = self._collect_local_run_text_artifacts(state)
        artifact_count, artifact_scan_truncated, artifact_directory_available = count_local_artifacts_bounded(state)
        artifact_count_truncated = artifact_scan_truncated or not artifact_directory_available
        if artifact_scan_truncated:
            preflight_omissions = tuple(dict.fromkeys((*preflight_omissions, "artifact_scan_limit")))
        if not artifact_directory_available:
            preflight_omissions = tuple(dict.fromkeys((*preflight_omissions, "artifact_directory_unavailable")))
        return self.telemetry_policy.build_local_run_payload(
            state,
            object_label,
            full_artifacts=full_artifacts,
            artifact_count=artifact_count,
            artifact_count_truncated=artifact_count_truncated,
            preflight_omissions=preflight_omissions,
        )

    @staticmethod
    def _local_result_present(state: dict[str, Any]) -> bool:
        """Return explicit result presence, with one-release legacy fallback."""

        result_present = state.get("result_present")
        if isinstance(result_present, bool):
            return result_present
        return state.get("result") is not None

    def _local_run_object_label(self, state: dict[str, Any]) -> dict[str, Any]:
        local_name = state.get("object") or "local_object"
        label = {
            "display_name": local_name,
            "local_name": local_name,
            "owner_id": None,
            "remote_object_id": None,
            "remote_version_id": None,
        }
        version_id = state.get("object_version_id")
        if not version_id:
            return label
        try:
            record = self.store.get_object_version(version_id, include_yaml=False)
        except KeyError:
            return label
        display_name = (
            record.get("display_name") or record.get("object_remote_name") or record.get("name") or local_name
        )
        label["display_name"] = display_name
        label["remote_object_id"] = record.get("remote_object_id") or record.get("object_remote_object_id")
        label["remote_version_id"] = record.get("remote_version_id")
        label["owner_id"] = record.get("owner_id")
        return label

    def _local_run_text_artifacts(self, state: dict[str, Any]) -> list[dict[str, Any]]:
        artifacts, _ = self._collect_local_run_text_artifacts(state)
        return artifacts

    def _collect_local_run_text_artifacts(
        self,
        state: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        artifacts: list[dict[str, Any]] = []
        omissions: list[str] = []
        remaining_bytes = LOCAL_RUN_TEXT_ARTIFACT_COLLECTION_MAX_BYTES

        def omit(reason: str) -> None:
            if reason not in omissions:
                omissions.append(reason)

        def add_text(name: str, value: Any, *, kind: str, content_type: str) -> None:
            nonlocal remaining_bytes
            if not isinstance(value, str) or not value:
                return
            if len(artifacts) >= LOCAL_RUN_TEXT_ARTIFACT_MAX_COUNT or remaining_bytes <= 0:
                omit("artifact_collection_limit")
                return
            payload = self._bounded_text_artifact_payload(
                name,
                value,
                kind=kind,
                content_type=content_type,
                max_content_bytes=min(
                    LOCAL_RUN_TEXT_ARTIFACT_MAX_BYTES,
                    remaining_bytes,
                ),
            )
            artifacts.append(payload)
            remaining_bytes -= len(payload["content_text"].encode("utf-8"))
            if payload["truncated"]:
                omit("artifact_content_limit")

        if self._local_result_present(state):
            if state.get("result_unreadable") is True and isinstance(state.get("result_json"), str):
                add_text(
                    "result.json",
                    state["result_json"],
                    kind="result",
                    content_type="application/json",
                )
            elif len(artifacts) >= LOCAL_RUN_TEXT_ARTIFACT_MAX_COUNT or remaining_bytes <= 0:
                omit("artifact_collection_limit")
            else:
                result_text = self._bounded_json_text(
                    state.get("result"),
                    min(LOCAL_RUN_TEXT_ARTIFACT_MAX_BYTES, remaining_bytes),
                )
                if result_text is None:
                    # The top-level full-telemetry result remains independently
                    # gated. Avoid materializing an oversized duplicate artifact.
                    omit("result_artifact_limit")
                else:
                    payload = self._text_artifact_payload(
                        "result.json",
                        result_text,
                        kind="result",
                        content_type="application/json",
                    )
                    artifacts.append(payload)
                    remaining_bytes -= len(result_text.encode("utf-8"))
        if state.get("stdout"):
            add_text(
                "stdout.txt",
                state["stdout"],
                kind="stdout",
                content_type="text/plain; charset=utf-8",
            )
        if state.get("stderr"):
            add_text(
                "stderr.txt",
                state["stderr"],
                kind="stderr",
                content_type="text/plain; charset=utf-8",
            )

        raw_artifacts_dir = state.get("artifacts_dir")
        if not isinstance(raw_artifacts_dir, str) or not raw_artifacts_dir:
            return artifacts, tuple(omissions)
        artifacts_dir = Path(raw_artifacts_dir)
        directory = ArtifactDirectory.open(artifacts_dir)
        if directory is None:
            omit("artifact_directory_unavailable")
            return artifacts, tuple(omissions)
        directory_artifact_start = len(artifacts)
        directory_remaining_start = remaining_bytes
        with directory:
            try:
                for index, entry in enumerate(directory.iter_entries()):
                    if index >= LOCAL_ARTIFACT_SCAN_MAX_ENTRIES:
                        omit("artifact_scan_limit")
                        break
                    entry_path = Path(entry.name)
                    if entry_path.suffix.lower() not in LOCAL_RUN_TEXT_ARTIFACT_EXTENSIONS:
                        continue
                    if not directory.entry_is_regular(entry):
                        omit("unsafe_artifact_entry")
                        continue
                    if len(artifacts) >= LOCAL_RUN_TEXT_ARTIFACT_MAX_COUNT or remaining_bytes <= 0:
                        omit("artifact_collection_limit")
                        break
                    file_payload = self._file_text_artifact_payload(
                        artifacts_dir / entry.name,
                        directory=directory,
                        max_content_bytes=min(
                            LOCAL_RUN_TEXT_ARTIFACT_MAX_BYTES,
                            remaining_bytes,
                        ),
                    )
                    if file_payload is None:
                        omit("artifact_read_error")
                        continue
                    artifacts.append(file_payload)
                    remaining_bytes -= len(file_payload["content_text"].encode("utf-8"))
                    if file_payload["truncated"]:
                        omit("artifact_content_limit")
            except (OSError, TypeError, NotImplementedError):
                omit("artifact_directory_unavailable")
            if not directory.verify_root():
                del artifacts[directory_artifact_start:]
                remaining_bytes = directory_remaining_start
                omit("artifact_directory_unavailable")
        return artifacts, tuple(omissions)

    def _file_text_artifact_payload(
        self,
        path: Path,
        *,
        directory: ArtifactDirectory | None = None,
        max_content_bytes: int = LOCAL_RUN_TEXT_ARTIFACT_MAX_BYTES,
    ) -> dict[str, Any] | None:
        if max_content_bytes < 0:
            raise ValueError("max_content_bytes must be nonnegative")
        owned_directory: ArtifactDirectory | None = None
        if directory is None:
            owned_directory = ArtifactDirectory.open(path.parent)
            directory = owned_directory
        if directory is None:
            return None
        try:
            file_read = directory.read_regular(path.name, max_content_bytes)
        finally:
            if owned_directory is not None:
                owned_directory.close()
        if file_read is None:
            return None
        data = file_read.data
        content_type = self._artifact_content_type(path)
        size = file_read.size
        truncated = size > max_content_bytes or len(data) > max_content_bytes
        decoded = data[:max_content_bytes].decode(
            "utf-8",
            errors="replace",
        )
        text = self._utf8_prefix(decoded, max_content_bytes)
        truncated = truncated or text != decoded
        try:
            return self._text_artifact_payload(
                f"artifact.{path.name}",
                text,
                kind="artifact",
                content_type=content_type,
                size=size,
                truncated=truncated,
            )
        except ValueError:
            return None

    def _bounded_text_artifact_payload(
        self,
        name: str,
        content_text: str,
        *,
        kind: str,
        content_type: str,
        max_content_bytes: int,
    ) -> dict[str, Any]:
        bounded, size, truncated = self._bounded_utf8_artifact_text(
            content_text,
            max_content_bytes,
        )
        return self._text_artifact_payload(
            name,
            bounded,
            kind=kind,
            content_type=content_type,
            size=size,
            truncated=truncated,
        )

    @staticmethod
    def _bounded_json_text(value: Any, max_bytes: int) -> str | None:
        """Serialize a JSON value only when it fits, without building an oversized copy."""

        try:
            if not m_json_contract.compact_json_fits(value, max_bytes):
                return None
            return m_json_contract.dumps(value, ensure_ascii=False, sort_keys=False)
        except (RecursionError, ValueError):
            return None

    @staticmethod
    def _bounded_utf8_artifact_text(
        value: str,
        max_bytes: int,
    ) -> tuple[str, int, bool]:
        """Return bounded text, its exact-or-lower-bound size, and size honesty.

        A value that fits is measured exactly. Once the byte cap is crossed,
        the returned size is ``max_bytes + 1``: a truthful lower bound that
        avoids traversing an arbitrarily large tail. The boolean explicitly
        marks that lower-bound measurement.
        """

        if max_bytes < 0:
            raise ValueError("max_bytes must be nonnegative")
        total_characters = len(value)
        if total_characters == 0:
            return "", 0, False
        chunks: list[str] = []
        remaining = max_bytes
        consumed_characters = 0
        while consumed_characters < total_characters:
            if remaining == 0:
                return "".join(chunks), max_bytes + 1, True
            # A Unicode code point occupies at least one UTF-8 byte, so looking
            # at remaining + 1 characters is sufficient to prove overflow.
            character_count = min(8192, remaining + 1)
            chunk = value[
                consumed_characters : min(
                    total_characters,
                    consumed_characters + character_count,
                )
            ]
            try:
                encoded = chunk.encode("utf-8")
            except UnicodeEncodeError:
                chunk = "".join("\ufffd" if 0xD800 <= ord(character) <= 0xDFFF else character for character in chunk)
                encoded = chunk.encode("utf-8")
            if len(encoded) > remaining:
                chunks.append(encoded[:remaining].decode("utf-8", errors="ignore"))
                return "".join(chunks), max_bytes + 1, True
            chunks.append(chunk)
            consumed_characters += len(chunk)
            remaining -= len(encoded)
        return "".join(chunks), max_bytes - remaining, False

    @staticmethod
    def _utf8_prefix(value: str, max_bytes: int) -> str:
        if max_bytes <= 0:
            return ""
        chunks: list[str] = []
        remaining = max_bytes
        for offset in range(0, len(value), 8192):
            chunk = value[offset : offset + 8192]
            encoded = chunk.encode("utf-8")
            if len(encoded) <= remaining:
                chunks.append(chunk)
                remaining -= len(encoded)
                if remaining == 0:
                    break
                continue
            chunks.append(encoded[:remaining].decode("utf-8", errors="ignore"))
            break
        return "".join(chunks)

    def _text_artifact_payload(
        self,
        name: str,
        content_text: str,
        *,
        kind: str,
        content_type: str,
        size: int | None = None,
        truncated: bool = False,
    ) -> dict[str, Any]:
        encoded = content_text.encode("utf-8")
        payload: dict[str, Any] = {
            "name": validate_name(name),
            "kind": kind,
            "content_type": content_type,
            "size": size if size is not None else len(encoded),
            "content_text": content_text,
            "truncated": truncated,
        }
        if content_type == "application/json" and not truncated:
            try:
                content_json = json.loads(content_text)
                m_json_contract.validate_json_value(content_json)
                payload["content_json"] = content_json
            except (json.JSONDecodeError, ValueError):
                pass
        return payload

    def _artifact_content_type(self, path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix == ".json":
            return "application/json"
        if suffix in {".htm", ".html"}:
            return "text/html; charset=utf-8"
        if suffix == ".csv":
            return "text/csv; charset=utf-8"
        if suffix == ".tsv":
            return "text/tab-separated-values; charset=utf-8"
        if suffix in {".yaml", ".yml"}:
            return "application/yaml"
        return "text/plain; charset=utf-8"

    def _worker_pythonpath(self, env: dict[str, str]) -> str:
        """Make this checkout's ``src`` directory visible to the worker."""

        src_dir = Path(__file__).parents[2]
        current = env.get("PYTHONPATH")
        if current:
            return os.pathsep.join([str(src_dir), current])
        return str(src_dir)

    def _log_worker_runtime(
        self,
        object_record: dict[str, Any],
        worker_runtime: dict[str, Any] | None,
    ) -> None:
        if worker_runtime is None:
            return
        payload = {
            "event": "worker_runtime",
            "object": object_record.get("name"),
            "version": object_record.get("version"),
            "version_id": object_record.get("version_id"),
            "worker_runtime": worker_runtime.get("worker_runtime"),
            "worker_runtime_reason": worker_runtime.get("worker_runtime_reason"),
        }
        LOGGER.info(
            "worker_runtime %s",
            m_json_contract.dumps(payload, ensure_ascii=True, sort_keys=True, separators=None),
            extra={
                "spl_event": "worker_runtime",
                "worker_runtime": payload,
            },
        )

    def shutdown(self) -> None:
        with self._shutdown_lock:
            if self._shutdown_complete:
                return
            self._shutdown_complete = True
        self.callback_capabilities.clear()
        self.browser_adapter_runs.shutdown()
        with self._retention_condition:
            self._retention_scheduler_stopped = True
            self._retention_deadlines.clear()
            self._retention_condition.notify_all()
        if self._retention_scheduler.is_alive():
            self._retention_scheduler.join(timeout=5.0)
        self.heartbeat_service.shutdown()
        self._join_run_threads(timeout_seconds=30.0)
        self.docker_pool.shutdown()

    def _join_run_threads(self, *, timeout_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        with self._run_threads_lock:
            threads = [thread for thread in self._run_threads if thread.is_alive()]
            self._run_threads = threads
        for thread in threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            thread.join(remaining)
        with self._run_threads_lock:
            alive = [thread for thread in self._run_threads if thread.is_alive()]
            self._run_threads = alive
        if alive:
            LOGGER.warning(
                "run threads still alive during shutdown: %s",
                ", ".join(thread.name for thread in alive),
            )

    def _read_timeout(self, input_path: Path) -> float | None:
        """Read an optional run timeout from the stored input payload."""

        payload = json.loads(input_path.read_text(encoding="utf-8"))
        return validate_timeout_seconds(
            payload.get("timeout_seconds"),
            name="timeout_seconds",
            domain=TimeoutDomain.NON_NEGATIVE,
            allow_none=True,
        )

    def _subprocess_text(self, value: str | bytes | None) -> str:
        """Normalize TimeoutExpired stdout/stderr values."""

        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value


def _load_quart() -> tuple[Any, Any, Any]:
    """Import Quart lazily so non-server daemon commands stay dependency-light."""

    try:
        from quart import Quart, Response, request
    except ModuleNotFoundError as exc:
        if exc.name == "quart":
            raise RuntimeError(
                "Quart is required to run the SPL daemon server. "
                "Install it in the daemon environment, for example: pip install quart"
            ) from exc
        raise
    return Quart, Response, request


def create_app(
    store: RegistryStore,
    *,
    auto_build_envs: bool = True,
    env_build_timeout_seconds: float | None = None,
    env_stale_lock_seconds: float | None = None,
    daemon_base_url: str = "http://127.0.0.1:8765",
    docker_pool_enabled: bool = False,
    docker_pool_size: int = 0,
    docker_idle_timeout_seconds: float = 300.0,
    docker_prewarm: bool = False,
    allow_remote_custom_adapters: bool = False,
    telemetry: TelemetryLevel = DEFAULT_TELEMETRY_LEVEL,
    telemetry_sensitive_fields: tuple[str, ...] | list[str] = (),
    api_token: str | None = None,
    environment_manager: EnvironmentManagerProtocol | None = None,
    docker_environment_manager: DockerEnvironmentManagerProtocol | None = None,
    docker_pool: DockerPoolRunnerProtocol | None = None,
    runtime_backends: RuntimeBackendRegistry | None = None,
    sync_visibility: SyncVisibilityProtocol | None = None,
    server_client_factory: ServerClientFactoryProtocol = _default_server_client_factory,
    daemon_identity: DaemonInstanceIdentity | None = None,
    daemon_home_lock: DaemonHomeLock | None = None,
    startup_binding: StartupBinding | None = None,
    startup_supervisor_proof: str | None = None,
) -> Any:
    """Create a Quart application bound to one registry store."""

    Quart, Response, request = _load_quart()
    app = Quart(__name__)
    install_adapter_run_request_limit(app)
    install_guarded_run_request_limit(app)
    install_lifecycle_request_limit(app)
    install_library_adapter_request_limit(app)
    install_prepared_validation_request_limit(app)
    install_source_analysis_request_limit(app)
    local_api_token = api_token or generate_daemon_api_token()
    app.api_token = local_api_token
    runtime = DaemonRuntime(
        store,
        auto_build_envs=auto_build_envs,
        env_build_timeout_seconds=env_build_timeout_seconds,
        env_stale_lock_seconds=env_stale_lock_seconds,
        daemon_base_url=daemon_base_url,
        docker_pool_enabled=docker_pool_enabled,
        docker_pool_size=docker_pool_size,
        docker_idle_timeout_seconds=docker_idle_timeout_seconds,
        docker_prewarm=docker_prewarm,
        allow_remote_custom_adapters=allow_remote_custom_adapters,
        telemetry=telemetry,
        telemetry_sensitive_fields=telemetry_sensitive_fields,
        environment_manager=environment_manager,
        docker_environment_manager=docker_environment_manager,
        docker_pool=docker_pool,
        runtime_backends=runtime_backends,
        sync_visibility=sync_visibility,
        server_client_factory=server_client_factory,
        daemon_identity=daemon_identity,
        daemon_home_lock=daemon_home_lock,
        startup_binding=startup_binding,
        startup_supervisor_proof=startup_supervisor_proof,
    )
    app.runtime = runtime

    context = RouteContext(
        runtime=runtime,
        response_cls=Response,
        request=request,
        local_api_token=local_api_token,
        callback_capabilities=runtime.callback_capabilities,
    )
    app.before_request(context.require_local_api_auth)

    register_meta_routes(app, runtime=runtime, context=context)
    register_lifecycle_routes(app, runtime=runtime, context=context)
    register_diagnostics_routes(
        app,
        runtime=runtime,
        json_response=context.json_response,
        route_errors=context.route_errors,
    )
    register_server_connection_routes(app, runtime=runtime, context=context)
    register_ai_preview_routes(app, runtime=runtime, context=context)
    register_ai_assistant_routes(app, runtime=runtime, context=context)
    register_library_routes(app, runtime=runtime, context=context)
    register_library_adapter_routes(app, runtime=runtime, context=context)
    register_object_routes(app, runtime=runtime, context=context)
    register_source_analysis_routes(app, runtime=runtime, context=context)
    register_prepared_validation_routes(app, runtime=runtime, context=context)
    register_env_routes(app, runtime=runtime, context=context)
    register_remote_routes(app, runtime=runtime, context=context)
    register_run_routes(app, runtime=runtime, context=context)
    register_guarded_run_routes(app, runtime=runtime, context=context)
    register_adapter_run_routes(app, runtime=runtime, context=context)
    register_artifact_routes(app, runtime=runtime, context=context)

    return app


def make_server(
    host: str,
    port: int,
    store: RegistryStore,
    **runtime_kwargs: Any,
) -> Any:
    """Backward-compatible factory name; returns a Quart app."""

    _ = (host, port)
    return create_app(store, **runtime_kwargs)


def _port_is_available(host: str, port: int) -> bool:
    """Return whether a local TCP server can bind to ``host:port``."""

    try:
        with socket.create_server((host, port), backlog=1):
            return True
    except OSError:
        return False


def select_daemon_port(
    host: str,
    preferred_port: int,
    *,
    auto_port: bool = True,
    scan_limit: int = DEFAULT_PORT_SCAN_LIMIT,
) -> int:
    """Select an available port, optionally scanning upward from the preference."""

    if preferred_port < 1 or preferred_port > 65535:
        raise ValueError("daemon port must be between 1 and 65535")
    if scan_limit < 1:
        raise ValueError("port_scan_limit must be positive")

    attempts = scan_limit if auto_port else 1
    last_port = min(65535, preferred_port + attempts - 1)
    for candidate in range(preferred_port, last_port + 1):
        if _port_is_available(host, candidate):
            return candidate

    if auto_port:
        raise OSError(f"no free daemon port found on {host} from {preferred_port} to {last_port}")
    raise OSError(f"daemon port {preferred_port} is already busy on {host}")


def _client_host_for_bind_host(host: str) -> str:
    """Return a loopback host that local clients can use for wildcard binds."""

    if host in {"", "0.0.0.0"}:
        return "127.0.0.1"
    if host == "::":
        return "::1"
    return host


def _run_daemon_app(app: Any, *, host: str, port: int) -> None:
    """Serve until interruption or the daemon-owned self-exit trigger fires."""

    run_task = getattr(app, "run_task", None)
    if not callable(run_task):
        # Backward-compatible seam used by existing startup/lock tests.
        app.run(host=host, port=port)
        return

    async def run_until_exit() -> None:
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        app.runtime.lifecycle.bind_exit_callback(lambda: loop.call_soon_threadsafe(shutdown_event.set))
        await run_task(
            host=host,
            port=port,
            shutdown_trigger=shutdown_event.wait,
        )

    asyncio.run(run_until_exit())


def serve(
    host: str = "127.0.0.1",
    port: int = 8765,
    home: Path | None = None,
    *,
    auto_port: bool = True,
    port_scan_limit: int = DEFAULT_PORT_SCAN_LIMIT,
    auto_build_envs: bool = True,
    env_build_timeout_seconds: float | None = None,
    env_stale_lock_seconds: float | None = None,
    docker_pool_enabled: bool = False,
    docker_pool_size: int = 0,
    docker_idle_timeout_seconds: float = 300.0,
    docker_prewarm: bool = False,
    allow_remote_custom_adapters: bool = False,
    telemetry: TelemetryLevel = DEFAULT_TELEMETRY_LEVEL,
    telemetry_sensitive_fields: tuple[str, ...] | list[str] = (),
    environment_manager: EnvironmentManagerProtocol | None = None,
    docker_environment_manager: DockerEnvironmentManagerProtocol | None = None,
    docker_pool: DockerPoolRunnerProtocol | None = None,
    runtime_backends: RuntimeBackendRegistry | None = None,
    sync_visibility: SyncVisibilityProtocol | None = None,
    server_client_factory: ServerClientFactoryProtocol = _default_server_client_factory,
    startup_binding_fd: int | None = None,
) -> None:
    """Run the local daemon until interrupted."""

    home_lock = DaemonHomeLock(home)
    startup_binding, startup_supervisor_proof = load_startup_binding(
        startup_binding_fd,
        home=home_lock.home,
    )
    daemon_identity = home_lock.acquire()
    store: RegistryStore | None = None
    published_base_url: str | None = None
    app: Any | None = None
    try:
        if startup_binding is not None:
            revalidate_startup_binding(startup_binding, home=home_lock.home)
        store = RegistryStore(home_lock.home)
        selected_port = select_daemon_port(
            host,
            port,
            auto_port=auto_port,
            scan_limit=port_scan_limit,
        )
        client_host = _client_host_for_bind_host(host)
        api_token = generate_daemon_api_token()
        base_url = daemon_url(client_host, selected_port)
        app = create_app(
            store,
            auto_build_envs=auto_build_envs,
            env_build_timeout_seconds=env_build_timeout_seconds,
            env_stale_lock_seconds=env_stale_lock_seconds,
            daemon_base_url=base_url,
            docker_pool_enabled=docker_pool_enabled,
            docker_pool_size=docker_pool_size,
            docker_idle_timeout_seconds=docker_idle_timeout_seconds,
            docker_prewarm=docker_prewarm,
            allow_remote_custom_adapters=allow_remote_custom_adapters,
            telemetry=telemetry,
            telemetry_sensitive_fields=telemetry_sensitive_fields,
            api_token=api_token,
            environment_manager=environment_manager,
            docker_environment_manager=docker_environment_manager,
            docker_pool=docker_pool,
            runtime_backends=runtime_backends,
            sync_visibility=sync_visibility,
            server_client_factory=server_client_factory,
            daemon_identity=daemon_identity,
            daemon_home_lock=home_lock,
            startup_binding=startup_binding,
            startup_supervisor_proof=startup_supervisor_proof,
        )
        endpoint = write_daemon_endpoint(
            store.home,
            bind_host=host,
            host=client_host,
            port=selected_port,
            api_token=api_token,
            updated_at=utc_now(),
        )
        published_base_url = str(endpoint["base_url"])
        if selected_port != port:
            print(f"SPL daemon port {port} is busy; using {selected_port} instead")
        print(f"SPL daemon listening on {daemon_url(host, selected_port)}")
        print(f"SPL daemon client endpoint: {published_base_url}")
        print(f"SPL daemon home: {store.home}")
        _run_daemon_app(app, host=host, port=selected_port)
    except KeyboardInterrupt:
        print("\nSPL daemon stopped")
    finally:
        try:
            if published_base_url is not None:
                clear_daemon_endpoint(home_lock.home, base_url=published_base_url)
        finally:
            try:
                if app is not None:
                    try:
                        app.runtime.shutdown()
                    except Exception:
                        pass
            finally:
                try:
                    if store is not None:
                        store.close()
                finally:
                    home_lock.release()
