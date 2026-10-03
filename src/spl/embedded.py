"""Daemon-free verified public release transport, cache, and worker runtime."""

from __future__ import annotations

import atexit
import base64
import hashlib
import importlib.metadata
import io
import json
import os
import platform
import queue
import re
import shutil
import ssl
import stat
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from collections.abc import Iterable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from http.client import IncompleteRead
from pathlib import Path, PurePosixPath
from typing import Any, NamedTuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
from uuid import uuid4

import certifi
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from spl._process import run_process_tree
from spl.daemon_client import ClientError
from spl.official_registry import official_registry_trusted_keys
from spl.public_contract import (
    CURRENT_PROCESS_MANIFEST_SCHEMA,
    CURRENT_PROCESS_CONTRACT,
    COMPILED_EXECUTION,
    ContractError,
    dependency_graph,
    validate_compiled_members,
    validate_native_runtime_config,
)
from spl.public import PublicObjectRef, format_public_ref, parse_public_ref
from spl.public_artifact_policy import (
    PublicArtifactPolicyError,
    inspect_wheel_archive,
)
from spl.runtime_environment import (
    PUBLIC_EMBEDDED_EXECUTOR,
    environment_identity,
    installed_framework_distribution,
    python_constraint_matches,
    validate_installed_framework,
    validate_runtime_lock,
)


PUBLIC_DISTRIBUTION_CAPABILITY = "spl.public_distribution.v1"
PUBLIC_MANIFEST_SCHEMA = "spl.public_object_manifest.v1"
MAX_BUNDLE_BYTES = 64 * 1024 * 1024
MAX_MEMBER_COUNT = 4096
MAX_MEMBER_BYTES = 64 * 1024 * 1024
MAX_COMPRESSION_RATIO = 100
LOCK_TIMEOUT_SECONDS = 120.0
MAX_BUNDLE_CACHE_ENTRIES = 64
MAX_ENVIRONMENT_CACHE_ENTRIES = 32
MAX_ARTIFACT_CACHE_ENTRIES = 128
MAX_WHEEL_BYTES = 128 * 1024 * 1024
MAX_FRAMEWORK_PROJECTION_BYTES = 64 * 1024 * 1024
MAX_FRAMEWORK_PROJECTION_FILES = 8192
MAX_ENVIRONMENT_MARKER_BYTES = 64 * 1024
MAX_ENVIRONMENT_FILES = 32768
MAX_ENVIRONMENT_BYTES = 1024 * 1024 * 1024
MAX_BASE_INTERPRETER_BYTES = 256 * 1024 * 1024
PUBLIC_ENVIRONMENT_CACHE_SCHEMA = "spl.public_environment_cache.v1"
PUBLIC_ENVIRONMENT_CACHE_SCHEMA_VERSION = 1
PUBLIC_ENVIRONMENT_BUILDER = "minimal-copy-v1"
MAX_RUN_RECEIPT_QUEUE = 64
MAX_RUN_RECEIPT_RESPONSE_BYTES = 64 * 1024
RUN_RECEIPT_TIMEOUT_SECONDS = 2.0
RUN_RECEIPT_SHUTDOWN_TIMEOUT_SECONDS = 4.5
PUBLIC_RUN_RECEIPTS_ENV = "SPL_PUBLIC_RUN_RECEIPTS"
_THREAD_LOCK_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.Lock] = {}
_RUN_RECEIPT_EMITTERS_LOCK = threading.Lock()
_RUN_RECEIPT_EMITTERS: set[PublicRunReceiptEmitter] = set()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _error(code: str, message: str, *, status_code: int | None = None) -> ClientError:
    return ClientError(
        f"{code}: {message}",
        status_code=status_code,
        payload={"code": code, "error": message},
    )


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


class PublicRegistryTransport:
    """Credential-free HTTPS transport with redirects disabled."""

    def __init__(self, registry_url: str, *, timeout_seconds: float = 30.0) -> None:
        self.registry_url = _registry_url(registry_url)
        self.timeout_seconds = timeout_seconds
        self._ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._opener = build_opener(_NoRedirect(), HTTPSHandler(context=self._ssl_context))

    def json(self, path: str) -> dict[str, Any]:
        data = self.bytes(path, accept="application/json")
        try:
            value = json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _error("public_registry_invalid", "registry returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise _error("public_registry_invalid", "registry returned a non-object JSON document")
        return value

    def bytes(self, path: str, *, accept: str) -> bytes:
        request = Request(
            self._url(path),
            method="GET",
            headers={"Accept": accept, "User-Agent": "splime-embedded"},
        )
        try:
            with self._opener.open(request, timeout=self.timeout_seconds) as response:
                length = response.headers.get("Content-Length")
                try:
                    expected_length = None if length is None else int(length)
                except (TypeError, ValueError) as exc:
                    raise _error("public_registry_invalid", "registry content length is invalid") from exc
                if expected_length is not None and expected_length > MAX_BUNDLE_BYTES:
                    raise _error("public_bundle_too_large", "registry bundle exceeds the client limit")
                data = bytes(response.read(MAX_BUNDLE_BYTES + 1))
        except HTTPError as exc:
            if exc.code == 404:
                raise _error(
                    "public_entry_unavailable",
                    "public entry is unavailable",
                    status_code=404,
                ) from exc
            if 300 <= exc.code < 400:
                raise _error("public_registry_redirect_rejected", "registry redirects are not accepted") from exc
            raise _error("public_registry_error", f"registry request failed with HTTP {exc.code}") from exc
        except IncompleteRead as exc:
            raise _error("public_registry_incomplete", "public registry transfer was interrupted") from exc
        except (OSError, URLError) as exc:
            raise _error("public_registry_unavailable", "public registry is unavailable") from exc
        if len(data) > MAX_BUNDLE_BYTES:
            raise _error("public_bundle_too_large", "registry bundle exceeds the client limit")
        if expected_length is not None and len(data) != expected_length:
            raise _error("public_registry_incomplete", "public registry transfer was interrupted")
        return data

    def post_json(self, path: str, value: Mapping[str, Any]) -> None:
        """Post one small credential-free receipt and discard its response."""

        data = _canonical_json(value)
        if len(data) > 4_096:
            raise ValueError("public receipt exceeds the client limit")
        request = Request(
            self._url(path),
            data=data,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "splime-embedded",
            },
        )
        try:
            with self._opener.open(request, timeout=self.timeout_seconds) as response:
                response.read(MAX_RUN_RECEIPT_RESPONSE_BYTES + 1)
        except (HTTPError, IncompleteRead, OSError, URLError) as exc:
            raise _error(
                "public_run_receipt_unavailable",
                "public run receipt was not accepted",
            ) from exc

    def _url(self, path: str) -> str:
        if not path.startswith("/") or "//" in path:
            raise ValueError("public registry path is invalid")
        return self.registry_url + path


class PublicArtifactTransport:
    """Fetch only exact official-PyPI file URLs without redirects."""

    def __init__(self) -> None:
        context = ssl.create_default_context(cafile=certifi.where())
        self._opener = build_opener(_NoRedirect(), HTTPSHandler(context=context))

    def wheel(self, url: str, *, expected_size: int) -> bytes:
        parsed = urlsplit(url)
        if f"{parsed.scheme}://{parsed.netloc}" != "https://files.pythonhosted.org":
            raise _error("public_artifact_origin_rejected", "wheel URL is not official PyPI file hosting")
        request = Request(url, headers={"Accept": "application/octet-stream", "User-Agent": "splime-embedded"})
        try:
            with self._opener.open(request, timeout=30.0) as response:
                final = urlsplit(str(response.geturl()))
                if f"{final.scheme}://{final.netloc}" != "https://files.pythonhosted.org":
                    raise _error("public_artifact_origin_rejected", "wheel redirect left official PyPI hosting")
                length = response.headers.get("Content-Length")
                if length is not None and int(length) != expected_size:
                    raise _error("public_artifact_size_mismatch", "wheel response length disagrees with the lock")
                data = bytes(response.read(MAX_WHEEL_BYTES + 1))
        except HTTPError as exc:
            if 300 <= exc.code < 400:
                raise _error("public_artifact_origin_rejected", "wheel redirects are not accepted") from exc
            raise _error("public_artifact_unavailable", "wheel download failed") from exc
        except (IncompleteRead, OSError, URLError, ValueError) as exc:
            raise _error("public_artifact_unavailable", "wheel download failed") from exc
        if len(data) != expected_size or len(data) > MAX_WHEEL_BYTES:
            raise _error("public_artifact_size_mismatch", "wheel response length disagrees with the lock")
        return data


def _registry_url(value: str) -> str:
    parsed = urlsplit(str(value).rstrip("/"))
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("registry_url must not contain credentials")
    if parsed.query or parsed.fragment or not parsed.hostname:
        raise ValueError("registry_url must be an absolute origin without query or fragment")
    loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if parsed.scheme != "https" and not (parsed.scheme == "http" and loopback):
        raise ValueError("registry_url must use HTTPS except for a loopback test server")
    if parsed.path not in {"", "/"}:
        raise ValueError("registry_url must not contain a path")
    return str(value).rstrip("/")


class PublicRunReceiptEmitter:
    """Best-effort sender with a bounded flush at normal interpreter shutdown."""

    def __init__(self, registry_url: str) -> None:
        self.transport = PublicRegistryTransport(
            registry_url,
            timeout_seconds=RUN_RECEIPT_TIMEOUT_SECONDS,
        )
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=MAX_RUN_RECEIPT_QUEUE)
        self._started = False
        self._lock = threading.Lock()
        self._pending = 0
        self._pending_condition = threading.Condition()

    def emit(self, **payload: Any) -> None:
        """Queue one exact allowlisted payload, dropping it under pressure."""

        expected = {
            "release_id",
            "event_id",
            "state",
            "runtime_family",
            "occurred_at",
        }
        if set(payload) != expected:
            return
        with self._pending_condition:
            try:
                self._queue.put_nowait(dict(payload))
            except queue.Full:
                return
            self._pending += 1
            _track_run_receipt_emitter(self)
        self._start()

    def flush(self, timeout_seconds: float = RUN_RECEIPT_SHUTDOWN_TIMEOUT_SECONDS) -> bool:
        """Wait at most ``timeout_seconds`` for already queued receipts."""

        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        deadline = time.monotonic() + timeout_seconds
        with self._pending_condition:
            while self._pending:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._pending_condition.wait(remaining)
            return True

    def _start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            threading.Thread(
                target=self._drain,
                name="splime-public-run-receipts",
                daemon=True,
            ).start()

    def _drain(self) -> None:
        while True:
            document = self._queue.get()
            try:
                self.transport.post_json("/public/v1/run-receipts", document)
            except Exception:
                pass
            finally:
                self._queue.task_done()
                with self._pending_condition:
                    self._pending -= 1
                    if self._pending == 0:
                        _untrack_run_receipt_emitter(self)
                        self._pending_condition.notify_all()


def _track_run_receipt_emitter(emitter: PublicRunReceiptEmitter) -> None:
    with _RUN_RECEIPT_EMITTERS_LOCK:
        _RUN_RECEIPT_EMITTERS.add(emitter)


def _untrack_run_receipt_emitter(emitter: PublicRunReceiptEmitter) -> None:
    with _RUN_RECEIPT_EMITTERS_LOCK:
        _RUN_RECEIPT_EMITTERS.discard(emitter)


def _flush_pending_run_receipts() -> None:
    """Give every active emitter a fair share of one hard shutdown budget."""

    deadline = time.monotonic() + RUN_RECEIPT_SHUTDOWN_TIMEOUT_SECONDS
    while True:
        with _RUN_RECEIPT_EMITTERS_LOCK:
            active = tuple(_RUN_RECEIPT_EMITTERS)
        if not active:
            return
        for index, emitter in enumerate(active):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            emitter.flush(remaining / (len(active) - index))
        if time.monotonic() >= deadline:
            return


atexit.register(_flush_pending_run_receipts)


_DEFAULT_RECEIPT_EMITTER = object()


def _run_receipts_enabled(value: bool | None) -> bool:
    if value is not None:
        return bool(value)
    raw = str(os.environ.get(PUBLIC_RUN_RECEIPTS_ENV, "1")).strip().casefold()
    return raw not in {"0", "false", "no", "off"}


def _runtime_family() -> str:
    implementation = platform.python_implementation().casefold()
    if implementation == "cpython":
        return "cpython"
    if implementation == "pypy":
        return "pypy"
    return "other"


def default_public_cache_dir() -> Path:
    """Return the platform-appropriate public runtime cache root."""

    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "splime" / "public"
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
        return base / "splime" / "public"
    return Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "splime" / "public"


@contextmanager
def _hash_lock(root: Path, digest: str) -> Iterator[None]:
    lock_identity = str((root / digest).absolute())
    with _THREAD_LOCK_GUARD:
        thread_lock = _THREAD_LOCKS.setdefault(lock_identity, threading.Lock())
    with thread_lock:
        with _interprocess_hash_lock(root, digest):
            yield


@contextmanager
def _interprocess_hash_lock(root: Path, digest: str) -> Iterator[None]:
    lock_dir = root / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{digest}.lock"
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    acquired = False
    try:
        deadline = time.monotonic() + LOCK_TIMEOUT_SECONDS
        while True:
            try:
                if _acquire_file_lock(descriptor):
                    acquired = True
                    break
            except OSError:
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for the embedded cache lock")
            time.sleep(0.05)
        yield
    finally:
        try:
            if acquired:
                _release_file_lock(descriptor)
        finally:
            os.close(descriptor)


@contextmanager
def _try_hash_lock(root: Path, digest: str) -> Iterator[bool]:
    lock_identity = str((root / digest).absolute())
    with _THREAD_LOCK_GUARD:
        thread_lock = _THREAD_LOCKS.setdefault(lock_identity, threading.Lock())
    if not thread_lock.acquire(blocking=False):
        yield False
        return
    descriptor: int | None = None
    acquired = False
    try:
        lock_dir = root / "locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            lock_dir / f"{digest}.lock",
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            acquired = _acquire_file_lock(descriptor)
        except OSError:
            acquired = False
        yield acquired
    finally:
        if descriptor is not None:
            try:
                if acquired:
                    _release_file_lock(descriptor)
            finally:
                os.close(descriptor)
        thread_lock.release()


def _acquire_file_lock(descriptor: int) -> bool:
    if os.name == "nt":  # pragma: no cover - exercised on Windows CI/hosts.
        import msvcrt

        if os.fstat(descriptor).st_size == 0:
            os.write(descriptor, b"\0")
        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            getattr(msvcrt, "locking")(descriptor, getattr(msvcrt, "LK_NBLCK"), 1)
        except OSError:
            return False
        return True
    import fcntl

    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _release_file_lock(descriptor: int) -> None:
    if os.name == "nt":  # pragma: no cover - exercised on Windows CI/hosts.
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        getattr(msvcrt, "locking")(descriptor, getattr(msvcrt, "LK_UNLCK"), 1)
        return
    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_UN)


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw = tempfile.mkstemp(prefix=f".{path.name}-", suffix=".tmp", dir=path.parent)
    temporary = Path(raw)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        # Windows cannot open a directory through the CRT os.open API.
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def verify_public_bundle(
    envelope: Mapping[str, Any],
    bundle_bytes: bytes,
    *,
    registry_url: str | None = None,
    trusted_keys: Mapping[str, Mapping[str, str | bytes]] | None = None,
    target_dir: Path | None = None,
    _depth: int = 0,
) -> dict[str, Any]:
    """Verify an origin-bound signature, hashes and the closed archive."""

    try:
        manifest = envelope["manifest"]
        manifest_hash = str(envelope["manifest_hash"])
        bundle_hash = str(envelope["bundle_hash"])
        signature = envelope["signature"]
    except (KeyError, TypeError) as exc:
        raise _error("public_manifest_invalid", "manifest envelope is incomplete") from exc
    if not isinstance(manifest, Mapping) or manifest.get("schema") not in {
        PUBLIC_MANIFEST_SCHEMA,
        CURRENT_PROCESS_MANIFEST_SCHEMA,
    }:
        raise _error("public_manifest_incompatible", "public manifest schema is unsupported")
    if manifest.get("bundle_format") != "zip-store-v1":
        raise _error("public_manifest_incompatible", "public bundle format is unsupported")
    canonical = _canonical_json(manifest)
    if _sha256(canonical) != manifest_hash:
        raise _error("public_manifest_hash_mismatch", "public manifest hash does not match")
    if len(bundle_bytes) > MAX_BUNDLE_BYTES or _sha256(bundle_bytes) != bundle_hash:
        raise _error("public_bundle_hash_mismatch", "public bundle hash does not match")
    if manifest.get("bundle_hash") != bundle_hash:
        raise _error("public_manifest_bundle_mismatch", "manifest and bundle identities disagree")
    if not isinstance(signature, Mapping) or "public_key" in signature:
        raise _error(
            "public_signature_key_untrusted",
            "public release envelopes must not supply their own trust key",
        )
    if registry_url is None or trusted_keys is None:
        raise _error(
            "public_trust_anchor_required",
            "an origin-bound external public signing key is required",
        )
    origin = _registry_url(registry_url)
    key_id = signature.get("key_id")
    origin_keys = trusted_keys.get(origin)
    if not isinstance(key_id, str) or not isinstance(origin_keys, Mapping) or key_id not in origin_keys:
        raise _error(
            "public_signature_key_untrusted",
            "the public release signing key is not trusted for this registry origin",
        )
    try:
        if signature.get("algorithm") != "ed25519":
            raise ValueError
        encoded_key = origin_keys[key_id]
        raw_key = (
            bytes(encoded_key)
            if isinstance(encoded_key, (bytes, bytearray))
            else base64.b64decode(encoded_key, validate=True)
        )
        expected_key_id = f"ed25519-sha256:{_sha256(raw_key)}"
        if key_id != expected_key_id:
            raise ValueError
        public_key = Ed25519PublicKey.from_public_bytes(raw_key)
        public_key.verify(
            base64.b64decode(signature["value"], validate=True),
            f"{manifest_hash}:{bundle_hash}".encode("ascii"),
        )
    except (InvalidSignature, KeyError, TypeError, ValueError) as exc:
        raise _error("public_signature_invalid", "public release signature is invalid") from exc
    if _depth > 32:
        raise _error("public_manifest_invalid", "embedded component closure is too deep")
    members = _validated_archive_members(manifest, bundle_bytes)
    _verify_component_closure(
        manifest,
        members,
        registry_url=origin,
        trusted_keys=trusted_keys,
        depth=_depth,
    )
    if manifest.get("schema") == CURRENT_PROCESS_MANIFEST_SCHEMA:
        try:
            if (
                manifest.get("schema_version") != 2
                or manifest.get("execution_contract") != CURRENT_PROCESS_CONTRACT
                or manifest.get("execution") != COMPILED_EXECUTION
                or manifest.get("runtime_locks") != []
                or manifest.get("required_capabilities") != [CURRENT_PROCESS_CONTRACT["framework"]]
            ):
                raise ContractError("unsupported current-process execution contract")
            content = {info.filename: data for info, data in members}
            graph = dependency_graph(content)
            if manifest.get("dependency_graph") != graph:
                raise ContractError("signed dependency inventory is missing or disagrees with captured metadata")
            descriptor = json.loads(content["object.json"])
            if (
                manifest.get("dependencies") != descriptor["distributions"]
                or manifest.get("entrypoint") != descriptor.get("entrypoint")
                or manifest.get("kind") != descriptor.get("kind")
            ):
                raise ContractError("object descriptor and signed manifest disagree")
            constraints = manifest.get("python_constraints")
            if not isinstance(constraints, Mapping) or constraints.get("runtime") != descriptor.get("runtime_config"):
                raise ContractError("runtime metadata is missing or inconsistent")
            python = constraints.get("python")
            if python is not None and (not isinstance(python, str) or not re.fullmatch(r"3\.13(?:\.\d+)?", python)):
                raise ContractError("publication Python must be from the supported 3.13 family")
            validate_native_runtime_config(constraints.get("runtime"))
            entrypoint = manifest.get("entrypoint")
            kind = manifest.get("kind")
            if not isinstance(entrypoint, str) or not isinstance(kind, str):
                raise ContractError("object entrypoint metadata is missing or invalid")
            validate_compiled_members(content, entrypoint, kind)
        except (ValueError, TypeError, KeyError) as exc:
            raise _error("public_contract_invalid", str(exc)) from exc
    elif "execution_contract" in manifest:
        raise _error("public_manifest_incompatible", "a legacy manifest cannot select a new execution contract")
    if target_dir is not None:
        _extract_members(bundle_bytes, members, target_dir)
    return dict(manifest)


def _verify_component_closure(
    manifest: Mapping[str, Any],
    members: list[tuple[zipfile.ZipInfo, bytes]],
    *,
    registry_url: str,
    trusted_keys: Mapping[str, Mapping[str, str | bytes]],
    depth: int,
) -> None:
    raw_components = manifest.get("components")
    if not isinstance(raw_components, list) or not raw_components:
        raise _error("public_manifest_invalid", "component closure inventory is missing")
    member_bytes = {info.filename: data for info, data in members}
    root_count = 0
    seen_paths: set[str] = set()
    for raw_component in raw_components:
        if not isinstance(raw_component, Mapping):
            raise _error("public_manifest_invalid", "component closure entry is malformed")
        path = raw_component.get("path")
        if not isinstance(path, str) or not path or path in seen_paths:
            raise _error("public_manifest_invalid", "component closure path is invalid")
        seen_paths.add(path)
        if path == "root":
            root_count += 1
            continue
        required = {
            "type",
            "identity",
            "version_id",
            "version",
            "content_hash",
            "bundle_hash",
            "manifest_hash",
            "bundle_member",
            "manifest_member",
            "execution_root",
            "execution",
        }
        if not required.issubset(raw_component):
            raise _error("public_manifest_invalid", "component closure identity is incomplete")
        bundle_member = _safe_member_name(raw_component["bundle_member"])
        manifest_member = _safe_member_name(raw_component["manifest_member"])
        try:
            nested_bundle = member_bytes[bundle_member]
            nested_envelope = json.loads(member_bytes[manifest_member])
        except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _error("public_archive_disagreement", "embedded component bytes are missing or invalid") from exc
        if not isinstance(nested_envelope, Mapping):
            raise _error("public_archive_disagreement", "embedded component envelope is malformed")
        if (
            nested_envelope.get("bundle_hash") != raw_component["bundle_hash"]
            or nested_envelope.get("manifest_hash") != raw_component["manifest_hash"]
            or raw_component["content_hash"] != raw_component["bundle_hash"]
        ):
            raise _error("public_resolution_mismatch", "embedded component identity disagrees")
        nested_manifest = verify_public_bundle(
            nested_envelope,
            nested_bundle,
            registry_url=registry_url,
            trusted_keys=trusted_keys,
            _depth=depth + 1,
        )
        if raw_component["type"] == "object" and nested_manifest.get("schema") != manifest.get("schema"):
            raise _error(
                "public_component_profile_mismatch", "Object components must share the parent execution contract"
            )
        nested_identity = nested_manifest.get("identity")
        if not isinstance(nested_identity, Mapping) or (
            nested_identity.get("entry_type") != raw_component["type"]
            or nested_identity.get("entry_id") != raw_component["identity"]
            or nested_manifest.get("version_id") != raw_component["version_id"]
            or nested_manifest.get("version") != raw_component["version"]
        ):
            raise _error("public_resolution_mismatch", "embedded component release identity disagrees")
        execution_root = raw_component.get("execution_root")
        execution = raw_component.get("execution")
        if not isinstance(execution_root, str) or not isinstance(execution, Mapping):
            raise _error("public_manifest_invalid", "embedded component execution binding is malformed")
        _safe_member_name(f"{execution_root}/component")
        nested_members = _validated_archive_members(nested_manifest, nested_bundle)
        for nested_info, nested_data in nested_members:
            copied_member = f"{execution_root}/{nested_info.filename}"
            if member_bytes.get(copied_member) != nested_data:
                raise _error(
                    "public_archive_disagreement",
                    "embedded component execution bytes disagree with its signed release",
                )
        try:
            with zipfile.ZipFile(io.BytesIO(nested_bundle)) as archive:
                nested_yaml = archive.read("object.yaml")
        except KeyError:
            nested_yaml = b""
        if b"noderemote" in nested_yaml.lower():
            raise _error("node_remote_not_allowed", "embedded component contains NodeRemote")
        if raw_component["type"] == "object":
            expected_member = f"{execution_root}/object.yaml"
            if (
                dict(execution)
                != {
                    "kind": "spl-object",
                    "object_yaml_member": expected_member,
                }
                or member_bytes.get(expected_member) != nested_yaml
            ):
                raise _error("public_archive_disagreement", "embedded Object is not executable from exact bytes")
        else:
            try:
                adapter_document = json.loads(
                    next(data for info, data in nested_members if info.filename == "adapter.json")
                )
            except (StopIteration, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _error("public_archive_disagreement", "embedded Adapter source is malformed") from exc
            if manifest.get("schema") == CURRENT_PROCESS_MANIFEST_SCHEMA and (
                not isinstance(adapter_document, Mapping)
                or not isinstance(adapter_document.get("dependencies"), list)
                or adapter_document["dependencies"] != nested_manifest.get("dependencies")
            ):
                raise _error(
                    "public_contract_invalid", "signed Adapter dependency metadata is incomplete or inconsistent"
                )
            adapter_member = f"{execution_root}/adapter.py"
            expected_execution = {
                "kind": "python-library-adapter",
                "module": execution_root.replace("/", ".") + ".adapter",
                "module_member": adapter_member,
            }
            if dict(execution) != expected_execution or member_bytes.get(adapter_member) != _embedded_adapter_module(
                adapter_document
            ):
                raise _error("public_archive_disagreement", "embedded Adapter is not executable from exact source")
    if root_count != 1:
        raise _error("public_manifest_invalid", "component closure must identify exactly one root")


def _embedded_adapter_module(adapter_document: Mapping[str, Any]) -> bytes:
    sources = [
        value.rstrip() + "\n"
        for key in ("save_source", "load_source")
        if isinstance((value := adapter_document.get(key)), str) and value.strip()
    ]
    if not sources:
        raise _error("public_archive_disagreement", "embedded Adapter has no signed executable source")
    return ("# Generated only from signed Library Adapter source.\n\n" + "\n".join(sources)).encode("utf-8")


def _validated_archive_members(manifest: Mapping[str, Any], bundle_bytes: bytes) -> list[tuple[zipfile.ZipInfo, bytes]]:
    raw_members = manifest.get("members")
    if not isinstance(raw_members, list) or not raw_members:
        raise _error("public_manifest_invalid", "manifest has no closed archive member inventory")
    declared: dict[str, tuple[int, str]] = {}
    for item in raw_members:
        if not isinstance(item, Mapping) or set(item) != {"path", "sha256", "size"}:
            raise _error("public_manifest_invalid", "archive member evidence is malformed")
        name = _safe_member_name(item["path"])
        size = item["size"]
        digest = item["sha256"]
        if type(size) is not int or size < 0 or size > MAX_MEMBER_BYTES:
            raise _error("public_archive_unsafe", "archive member size is unsafe")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise _error("public_manifest_invalid", "archive member hash is malformed")
        if name in declared or name.casefold() in {value.casefold() for value in declared}:
            raise _error("public_archive_unsafe", "archive member identity collides")
        declared[name] = (size, digest)
    if len(declared) > MAX_MEMBER_COUNT or sum(size for size, _ in declared.values()) > MAX_BUNDLE_BYTES:
        raise _error("public_archive_unsafe", "archive inventory exceeds safety limits")
    result: list[tuple[zipfile.ZipInfo, bytes]] = []
    observed: set[str] = set()
    try:
        with zipfile.ZipFile(io.BytesIO(bundle_bytes)) as archive:
            infos = archive.infolist()
            if len(infos) != len(declared) or len(infos) > MAX_MEMBER_COUNT:
                raise _error("public_archive_disagreement", "archive membership disagrees with manifest")
            for info in infos:
                name = _safe_member_name(info.filename)
                mode = info.external_attr >> 16
                if name in observed or name.casefold() in {value.casefold() for value in observed}:
                    raise _error("public_archive_unsafe", "archive contains duplicate member names")
                if info.is_dir() or not stat.S_ISREG(mode):
                    raise _error("public_archive_unsafe", "archive contains a non-regular member")
                if info.file_size > MAX_MEMBER_BYTES:
                    raise _error("public_archive_unsafe", "archive member exceeds the size limit")
                if info.compress_size == 0 and info.file_size:
                    raise _error("public_archive_unsafe", "archive member has an invalid compression ratio")
                if info.compress_size and info.file_size / info.compress_size > MAX_COMPRESSION_RATIO:
                    raise _error("public_archive_unsafe", "archive member exceeds the compression ratio limit")
                if info.compress_type != zipfile.ZIP_STORED:
                    raise _error("public_archive_unsafe", "archive compression does not match zip-store-v1")
                data = archive.read(info)
                expected = declared.get(name)
                if expected is None or expected != (len(data), _sha256(data)):
                    raise _error("public_archive_disagreement", "archive member evidence disagrees with manifest")
                observed.add(name)
                result.append((info, data))
    except zipfile.BadZipFile as exc:
        raise _error("public_archive_invalid", "public bundle is not a valid ZIP archive") from exc
    if observed != set(declared):
        raise _error("public_archive_disagreement", "archive member set disagrees with manifest")
    return result


def _safe_member_name(value: Any) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise _error("public_archive_unsafe", "archive member name is invalid")
    path = PurePosixPath(value)
    normalized = str(path)
    if normalized != value or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise _error("public_archive_unsafe", "archive member path is unsafe")
    return normalized


def _extract_members(
    _bundle_bytes: bytes,
    members: list[tuple[zipfile.ZipInfo, bytes]],
    target: Path,
) -> None:
    target.mkdir(parents=True, exist_ok=False)
    for info, data in members:
        path = target.joinpath(*PurePosixPath(info.filename).parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            0o400,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)


def _read_bounded_regular(path: Path, *, maximum: int, label: str) -> bytes:
    try:
        before = path.lstat()
    except OSError:
        raise _error("public_artifact_unsafe", f"{label} is not a safe regular file") from None
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise _error("public_artifact_unsafe", f"{label} is not a safe regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise _error("public_artifact_unsafe", f"{label} is not a safe regular file") from None
    try:
        details = os.fstat(descriptor)
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_nlink != 1
            or (details.st_dev, details.st_ino) != (before.st_dev, before.st_ino)
            or details.st_size < 0
            or details.st_size > maximum
        ):
            raise _error("public_artifact_unsafe", f"{label} is not a bounded regular file")
        chunks: list[bytes] = []
        observed = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, maximum + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > maximum:
                raise _error("public_artifact_unsafe", f"{label} exceeds the size limit")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if (
            observed != details.st_size
            or (after.st_dev, after.st_ino) != (details.st_dev, details.st_ino)
            or after.st_size != observed
        ):
            raise _error("public_artifact_unsafe", f"{label} changed while it was read")
        try:
            current = path.lstat()
        except OSError:
            raise _error("public_artifact_unsafe", f"{label} changed while it was read") from None
        if (
            not stat.S_ISREG(current.st_mode)
            or current.st_nlink != 1
            or (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino)
            or current.st_size != observed
        ):
            raise _error("public_artifact_unsafe", f"{label} changed while it was read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _projection_files(root: Path) -> list[tuple[PurePosixPath, bytes]]:
    result: list[tuple[PurePosixPath, bytes]] = []
    total = 0

    def visit(directory: Path, relative: PurePosixPath) -> None:
        nonlocal total
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name.casefold())
        except OSError:
            raise _error("embedded_framework_invalid", "installed framework files are unavailable") from None
        folded: set[str] = set()
        for entry in entries:
            if entry.name == "__pycache__" or entry.name.endswith((".pyc", ".pyo")):
                continue
            if entry.name.casefold() in folded:
                raise _error("embedded_framework_invalid", "installed framework paths collide")
            folded.add(entry.name.casefold())
            path = Path(entry.path)
            child = relative / entry.name
            try:
                details = os.stat(entry.path, follow_symlinks=False)
            except OSError:
                raise _error("embedded_framework_invalid", "installed framework files are unavailable") from None
            if stat.S_ISDIR(details.st_mode):
                visit(path, child)
                continue
            if not stat.S_ISREG(details.st_mode):
                raise _error("embedded_framework_invalid", "installed framework contains a non-regular file")
            data = _read_bounded_regular(
                path,
                maximum=MAX_FRAMEWORK_PROJECTION_BYTES,
                label="installed framework file",
            )
            total += len(data)
            if total > MAX_FRAMEWORK_PROJECTION_BYTES or len(result) >= MAX_FRAMEWORK_PROJECTION_FILES:
                raise _error("embedded_framework_invalid", "installed framework projection exceeds the safety limit")
            result.append((child, data))

    visit(root, PurePosixPath())
    if not result:
        raise _error("embedded_framework_invalid", "installed framework projection is empty")
    return result


def _projection_hash(files: list[tuple[PurePosixPath, bytes]]) -> str:
    evidence = [{"path": str(path), "size": len(data), "sha256": _sha256(data)} for path, data in files]
    return _sha256(_canonical_json(evidence))


def _remove_cache_path(path: Path) -> None:
    if path.is_symlink() or (path.exists() and not path.is_dir()):
        path.unlink(missing_ok=True)
    elif path.is_dir():
        for current, directories, files in os.walk(path, topdown=False, followlinks=False):
            if os.name == "nt":
                for name in files:
                    child = Path(current) / name
                    details = child.lstat()
                    if stat.S_ISREG(details.st_mode) and details.st_nlink == 1:
                        os.chmod(child, stat.S_IREAD | stat.S_IWRITE, follow_symlinks=False)
            for directory in directories:
                child = Path(current) / directory
                if child.is_symlink():
                    continue
                try:
                    os.chmod(child, 0o700, follow_symlinks=False)
                except OSError:
                    pass
            try:
                os.chmod(current, 0o700, follow_symlinks=False)
            except OSError:
                pass
        shutil.rmtree(path)


def _runtime_projection_violation(name: str) -> str | None:
    parts = PurePosixPath(name).parts
    projected = parts
    for index, part in enumerate(parts[:-1]):
        if part.casefold().endswith(".data") and parts[index + 1].casefold() in {
            "purelib",
            "platlib",
        }:
            projected = parts[index + 2 :]
            break
    if not projected:
        return None
    first = projected[0].casefold()
    if first in {
        "spl",
        "spl.py",
        "spl.pyc",
        "spl.pyo",
        "splime_public_worker",
        "splime_public_worker.py",
    }:
        return "wheel attempts to replace the installed splime framework"
    if first in {
        "sitecustomize",
        "sitecustomize.py",
        "sitecustomize.pyc",
        "sitecustomize.pyo",
        "usercustomize",
        "usercustomize.py",
        "usercustomize.pyc",
        "usercustomize.pyo",
    } or any(part.casefold().endswith(".pth") for part in projected):
        return "wheel attempts to install a Python startup hook"
    return None


def _installed_framework_projection() -> tuple[
    dict[str, str],
    str,
    list[tuple[PurePosixPath, bytes]],
    str,
    list[tuple[PurePosixPath, bytes]],
]:
    try:
        authority = validate_installed_framework(PUBLIC_EMBEDDED_EXECUTOR)
        distribution = installed_framework_distribution()
    except ValueError as exc:
        raise _error("embedded_framework_incompatible", str(exc)) from exc
    except importlib.metadata.PackageNotFoundError as exc:
        raise _error("embedded_framework_incompatible", "installed splime framework is unavailable") from exc

    distribution_root = Path(str(distribution.locate_file(""))).resolve()
    direct_url_text = distribution.read_text("direct_url.json")
    package_root = (distribution_root / "spl").resolve()
    if direct_url_text is not None:
        try:
            direct_url = json.loads(direct_url_text)
            if not isinstance(direct_url, Mapping) or not isinstance(direct_url.get("url"), str):
                raise ValueError
            raw_dir_info = direct_url.get("dir_info")
            if raw_dir_info is not None and not isinstance(raw_dir_info, Mapping):
                raise ValueError
            editable = isinstance(raw_dir_info, Mapping) and raw_dir_info.get("editable") is True
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise _error("embedded_framework_incompatible", "installed framework origin is malformed") from exc
        if editable:
            parsed = urlsplit(str(direct_url["url"]))
            if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
                raise _error("embedded_framework_incompatible", "installed framework origin is unsupported")
            editable_root = Path(unquote(parsed.path)).resolve()
            expected_roots = [
                (editable_root / "src" / "spl").resolve(),
                (editable_root / "spl").resolve(),
            ]
            available_roots = [path for path in expected_roots if path.is_dir() and not path.is_symlink()]
            if len(available_roots) != 1:
                raise _error("embedded_framework_incompatible", "installed editable framework origin is ambiguous")
            package_root = available_roots[0]
    if not package_root.is_dir() or package_root.is_symlink():
        raise _error(
            "embedded_framework_incompatible",
            "installed splime distribution has no safe spl package",
        )

    raw_dist_info = getattr(distribution, "_path", None)
    dist_info_root = Path(raw_dist_info).resolve() if isinstance(raw_dist_info, Path) else Path()
    if not dist_info_root.name.casefold().endswith(".dist-info"):
        metadata_candidates = [
            item
            for item in (distribution.files or [])
            if item.name == "METADATA" and len(item.parts) >= 2 and item.parts[0].casefold().endswith(".dist-info")
        ]
        if len(metadata_candidates) != 1:
            raise _error("embedded_framework_incompatible", "installed framework metadata is incomplete")
        dist_info_root = Path(str(distribution.locate_file(metadata_candidates[0]))).parent.resolve()
    if not dist_info_root.is_dir() or dist_info_root.is_symlink():
        raise _error("embedded_framework_incompatible", "installed framework metadata is unsafe")

    package_files = _projection_files(package_root)
    metadata_files = [
        (relative, data)
        for relative, data in _projection_files(dist_info_root)
        if relative == PurePosixPath("METADATA")
    ]
    if len(metadata_files) != 1:
        raise _error("embedded_framework_incompatible", "installed framework metadata is incomplete")
    identity = {
        **authority,
        "package_sha256": _projection_hash(package_files),
        "metadata_sha256": _projection_hash(metadata_files),
    }
    return identity, "spl", package_files, dist_info_root.name, metadata_files


def _write_projection_tree(
    site_packages: Path,
    name: str,
    files: list[tuple[PurePosixPath, bytes]],
) -> None:
    root = site_packages / name
    root.mkdir(parents=True, exist_ok=False)
    for relative, data in files:
        path = root.joinpath(*relative.parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            0o400,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)


def _project_installed_framework(
    venv_path: Path,
    projection: tuple[
        dict[str, str],
        str,
        list[tuple[PurePosixPath, bytes]],
        str,
        list[tuple[PurePosixPath, bytes]],
    ],
) -> None:
    _identity, package_name, package_files, metadata_name, metadata_files = projection
    if os.name == "nt":
        site_packages = venv_path / "Lib" / "site-packages"
    else:
        version = f"python{sys.version_info.major}.{sys.version_info.minor}"
        site_packages = venv_path / "lib" / version / "site-packages"
    site_packages.mkdir(parents=True, exist_ok=True)
    _write_projection_tree(site_packages, package_name, package_files)
    _write_projection_tree(site_packages, metadata_name, metadata_files)


def _environment_python_relative() -> PurePosixPath:
    if os.name == "nt":  # pragma: no cover - exercised on Windows CI/hosts.
        return PurePosixPath("Scripts/python.exe")
    return PurePosixPath("bin/python")


def _environment_site_packages_relative() -> PurePosixPath:
    if os.name == "nt":  # pragma: no cover - exercised on Windows CI/hosts.
        return PurePosixPath("Lib/site-packages")
    return PurePosixPath(f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages")


def _environment_scripts_relative() -> PurePosixPath:
    return PurePosixPath("Scripts" if os.name == "nt" else "bin")


def _base_interpreter_projection() -> tuple[Path, bytes]:
    try:
        executable = Path(getattr(sys, "_base_executable", sys.executable)).resolve(strict=True)
        projected_executable = executable
        if os.name == "nt":
            import sysconfig
            import venv

            # Use CPython's venv redirector: copying python.exe alone cannot
            # locate its DLLs, and a nested venv needs the base Python home.
            scripts = (
                executable.parent if sysconfig.is_python_build() else Path(venv.__file__).parent / "scripts" / "nt"
            )
            debug = "_d" if executable.stem.casefold().endswith("_d") else ""
            threaded = "t" if sysconfig.get_config_var("Py_GIL_DISABLED") else ""
            projected_executable = scripts / f"venvlauncher{threaded}{debug}.exe"
        data = _read_bounded_regular(
            projected_executable,
            maximum=MAX_BASE_INTERPRETER_BYTES,
            label="base interpreter",
        )
    except (OSError, ClientError):
        raise _error("embedded_framework_incompatible", "base interpreter is unavailable") from None
    if not data:
        raise _error("embedded_framework_incompatible", "base interpreter is unavailable")
    return executable, data


def _pyvenv_configuration(executable: Path) -> bytes:
    return (
        f"home = {executable.parent}\n"
        "include-system-site-packages = false\n"
        f"version = {platform.python_version()}\n"
        f"executable = {executable}\n"
    ).encode("utf-8")


def _environment_marker_document(
    *,
    digest: str,
    lock_hash: str,
    framework_identity: Mapping[str, str],
) -> dict[str, Any]:
    return {
        "schema": PUBLIC_ENVIRONMENT_CACHE_SCHEMA,
        "schema_version": PUBLIC_ENVIRONMENT_CACHE_SCHEMA_VERSION,
        "identity": digest,
        "lock_hash": lock_hash,
        "framework": dict(framework_identity),
        "builder": PUBLIC_ENVIRONMENT_BUILDER,
        "python": str(_environment_python_relative()),
    }


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _read_environment_marker(path: Path) -> dict[str, Any]:
    body = _read_bounded_regular(
        path,
        maximum=MAX_ENVIRONMENT_MARKER_BYTES,
        label="environment marker",
    )
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("environment marker is malformed") from exc
    expected_fields = {
        "schema",
        "schema_version",
        "identity",
        "lock_hash",
        "framework",
        "builder",
        "python",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise ValueError("environment marker has an unsupported shape")
    python = value.get("python")
    if (
        value.get("schema") != PUBLIC_ENVIRONMENT_CACHE_SCHEMA
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != PUBLIC_ENVIRONMENT_CACHE_SCHEMA_VERSION
        or not isinstance(value.get("identity"), str)
        or not isinstance(value.get("lock_hash"), str)
        or not isinstance(value.get("framework"), Mapping)
        or value.get("builder") != PUBLIC_ENVIRONMENT_BUILDER
        or not isinstance(python, str)
    ):
        raise ValueError("environment marker is unsupported")
    relative = PurePosixPath(python)
    if (
        relative.is_absolute()
        or str(relative) != python
        or "\\" in python
        or any(part in {"", ".", ".."} for part in relative.parts)
        or relative != _environment_python_relative()
    ):
        raise ValueError("environment marker interpreter is invalid")
    return dict(value)


def _wheel_install_path(
    name: str,
    *,
    data_root: str,
    project: str,
) -> tuple[PurePosixPath, int]:
    path = PurePosixPath(name)
    site_packages = _environment_site_packages_relative()
    if path.parts[0].casefold() != data_root.casefold():
        return site_packages / path, 0o444
    if len(path.parts) < 3:
        raise _error("public_artifact_unsafe", "wheel data projection is malformed")
    scheme = path.parts[1].casefold()
    rest = PurePosixPath(*path.parts[2:])
    if scheme in {"purelib", "platlib"}:
        return site_packages / rest, 0o444
    if scheme == "scripts":
        return _environment_scripts_relative() / rest, 0o555
    if scheme == "headers":
        normalized = re.sub(r"[-_.]+", "-", project).casefold()
        return PurePosixPath(
            "include", f"site/python{sys.version_info.major}.{sys.version_info.minor}", normalized
        ) / rest, 0o444
    if scheme == "data":
        return rest, 0o444
    raise _error("public_artifact_unsafe", "wheel data projection uses an unsupported scheme")


def _relative_import_path(path: PurePosixPath) -> str | None:
    site_packages = _environment_site_packages_relative()
    if path.parts[: len(site_packages.parts)] != site_packages.parts:
        return None
    remainder = path.parts[len(site_packages.parts) :]
    return str(PurePosixPath(*remainder)) if remainder else None


def _validate_expected_environment_files(
    files: Mapping[PurePosixPath, tuple[bytes, int]],
) -> set[PurePosixPath]:
    if not files or len(files) > MAX_ENVIRONMENT_FILES:
        raise _error("embedded_environment_build_failed", "environment inventory exceeds the safety limit")
    total = sum(len(data) for data, _mode in files.values())
    if total > MAX_ENVIRONMENT_BYTES:
        raise _error("embedded_environment_build_failed", "environment inventory exceeds the safety limit")
    folded: dict[str, PurePosixPath] = {}
    directories: set[PurePosixPath] = set()
    file_paths = set(files)
    for relative in files:
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise _error("embedded_environment_build_failed", "environment inventory is unsafe")
        identity = str(relative).casefold()
        if identity in folded:
            raise _error("embedded_environment_build_failed", "environment inventory paths collide")
        folded[identity] = relative
        parent = relative.parent
        while parent != PurePosixPath("."):
            if parent in file_paths:
                raise _error("embedded_environment_build_failed", "environment inventory paths collide")
            directories.add(parent)
            parent = parent.parent
    directory_folded: set[str] = set()
    for directory in directories:
        identity = str(directory).casefold()
        if identity in directory_folded or identity in folded:
            raise _error("embedded_environment_build_failed", "environment inventory paths collide")
        directory_folded.add(identity)
    return directories


def _expected_environment_files(
    *,
    lock: Mapping[str, Any],
    digest: str,
    projection: tuple[
        dict[str, str],
        str,
        list[tuple[PurePosixPath, bytes]],
        str,
        list[tuple[PurePosixPath, bytes]],
    ],
    wheels: list[Path],
) -> dict[PurePosixPath, tuple[bytes, int]]:
    framework_identity, package_name, package_files, metadata_name, metadata_files = projection
    base_executable, base_bytes = _base_interpreter_projection()
    site_packages = _environment_site_packages_relative()
    files: dict[PurePosixPath, tuple[bytes, int]] = {
        _environment_python_relative(): (base_bytes, 0o555),
        PurePosixPath("pyvenv.cfg"): (_pyvenv_configuration(base_executable), 0o444),
        PurePosixPath("ready.json"): (
            _canonical_json(
                _environment_marker_document(
                    digest=digest,
                    lock_hash=str(lock["lock_hash"]),
                    framework_identity=framework_identity,
                )
            ),
            0o444,
        ),
    }

    def add(
        relative: PurePosixPath,
        data: bytes,
        mode: int,
        *,
        framework_file: bool = False,
    ) -> None:
        if relative in files:
            raise _error("embedded_environment_build_failed", "environment inventory paths collide")
        import_path = _relative_import_path(relative)
        if import_path is not None:
            violation = _runtime_projection_violation(import_path)
            if violation is not None and not framework_file:
                raise _error("public_artifact_unsafe", violation)
        files[relative] = (data, mode)

    for relative, data in package_files:
        add(
            site_packages / package_name / relative,
            data,
            0o444,
            framework_file=True,
        )
    for relative, data in metadata_files:
        add(site_packages / metadata_name / relative, data, 0o444)

    artifacts = list(lock["artifacts"])
    if len(wheels) != len(artifacts):
        raise _error("embedded_environment_build_failed", "environment artifact inventory is incomplete")
    for wheel, artifact in zip(wheels, artifacts, strict=True):
        payload = EmbeddedBackend._verify_wheel_file(wheel, artifact)
        try:
            members = inspect_wheel_archive(payload)
        except PublicArtifactPolicyError as exc:
            raise _error("public_artifact_unsafe", str(exc)) from exc
        metadata_names = [name for name in members if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise _error("public_artifact_unsafe", "wheel metadata inventory is invalid")
        dist_info = PurePosixPath(metadata_names[0]).parts[0]
        data_root = f"{dist_info[: -len('.dist-info')]}.data"
        for name, data in members.items():
            relative, mode = _wheel_install_path(
                name,
                data_root=data_root,
                project=str(artifact["project"]),
            )
            add(relative, data, mode)
    _validate_expected_environment_files(files)
    return files


def _write_environment_tree(
    directory: Path,
    files: Mapping[PurePosixPath, tuple[bytes, int]],
) -> None:
    try:
        with os.scandir(directory) as entries:
            directory_empty = next(entries, None) is None
    except OSError:
        raise _error("embedded_environment_build_failed", "temporary environment is unsafe") from None
    if directory.is_symlink() or not directory.is_dir() or not directory_empty:
        raise _error("embedded_environment_build_failed", "temporary environment is unsafe")
    directories = _validate_expected_environment_files(files)
    for relative in sorted(directories, key=lambda item: (len(item.parts), str(item))):
        directory.joinpath(*relative.parts).mkdir(mode=0o700)
    for relative, (data, mode) in sorted(files.items(), key=lambda item: str(item[0])):
        path = directory.joinpath(*relative.parts)
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)
        path.chmod(mode)
    for relative in sorted(directories, key=lambda item: (-len(item.parts), str(item))):
        directory.joinpath(*relative.parts).chmod(0o555)
    directory.chmod(0o555)


def _cache_mode_matches(mode: int, expected: int) -> bool:
    if os.name == "nt":
        # The Windows directory read-only attribute is not an access mode.
        # Directory identity and exact file inventory are checked separately.
        if stat.S_ISDIR(mode):
            return True
        return bool(mode & stat.S_IREAD) and bool(mode & stat.S_IWRITE) == bool(expected & stat.S_IWRITE)
    return stat.S_IMODE(mode) == expected


def _verify_environment_inventory(
    directory: Path,
    files: Mapping[PurePosixPath, tuple[bytes, int]],
) -> bool:
    try:
        root_details = directory.lstat()
    except OSError:
        return False
    if (
        not stat.S_ISDIR(root_details.st_mode)
        or directory.is_symlink()
        or not _cache_mode_matches(root_details.st_mode, 0o555)
    ):
        return False
    expected_directories = _validate_expected_environment_files(files)
    actual_files: set[PurePosixPath] = set()
    actual_directories: set[PurePosixPath] = set()
    folded: set[str] = set()

    def visit(path: Path, relative: PurePosixPath) -> bool:
        try:
            entries = list(os.scandir(path))
        except OSError:
            return False
        local_folded: set[str] = set()
        for entry in entries:
            if entry.name.casefold() in local_folded:
                return False
            local_folded.add(entry.name.casefold())
            child = relative / entry.name
            identity = str(child).casefold()
            if identity in folded:
                return False
            folded.add(identity)
            try:
                details = os.stat(entry.path, follow_symlinks=False)
            except OSError:
                return False
            child_path = Path(entry.path)
            if stat.S_ISDIR(details.st_mode):
                if not _cache_mode_matches(details.st_mode, 0o555):
                    return False
                actual_directories.add(child)
                if not visit(child_path, child):
                    return False
                continue
            expected = files.get(child)
            if (
                expected is None
                or not stat.S_ISREG(details.st_mode)
                or details.st_nlink != 1
                or not _cache_mode_matches(details.st_mode, expected[1])
                or details.st_size != len(expected[0])
            ):
                return False
            try:
                data = _read_bounded_regular(
                    child_path,
                    maximum=len(expected[0]),
                    label="environment file",
                )
            except (ClientError, OSError):
                return False
            if data != expected[0] or _sha256(data) != _sha256(expected[0]):
                return False
            actual_files.add(child)
        return True

    if not visit(directory, PurePosixPath(".")):
        return False
    return actual_files == set(files) and actual_directories == expected_directories


class EmbeddedCallResult(NamedTuple):
    run: dict[str, Any]
    payload: dict[str, Any]
    downloaded_artifacts: dict[str, Path]
    native_value: Any
    has_native_value: bool


class EmbeddedBackend:
    """Resolve, verify, cache and execute public Objects without a daemon."""

    def __init__(
        self,
        registry_url: str,
        cache_dir: str | Path | None,
        *,
        run_receipts: bool | None = None,
        trusted_keys: Mapping[str, Mapping[str, str | bytes]] | None = None,
        receipt_emitter: Any = _DEFAULT_RECEIPT_EMITTER,
    ) -> None:
        self.transport = PublicRegistryTransport(registry_url)
        self.cache_root = Path(cache_dir) if cache_dir is not None else default_public_cache_dir()
        self.artifact_transport = PublicArtifactTransport()
        self.trusted_keys = (
            trusted_keys if trusted_keys is not None else official_registry_trusted_keys(self.transport.registry_url)
        )
        if receipt_emitter is _DEFAULT_RECEIPT_EMITTER:
            self.receipt_emitter = (
                PublicRunReceiptEmitter(self.transport.registry_url) if _run_receipts_enabled(run_receipts) else None
            )
        else:
            self.receipt_emitter = receipt_emitter

    def call(
        self,
        reference_text: str,
        *,
        args: list[Any] | None,
        kwargs: dict[str, Any] | None,
        timeout_seconds: float | None,
        artifacts_dir: str | Path | None,
        trust: bool,
    ) -> "EmbeddedCallResult":
        reference = parse_public_ref(reference_text)
        resolved, envelope, bundle_dir = self._resolve_and_cache(reference)
        if envelope["manifest"].get("schema") == CURRENT_PROCESS_MANIFEST_SCHEMA:
            return self._call_inprocess(
                resolved,
                envelope,
                bundle_dir,
                args=args,
                kwargs=kwargs,
                timeout_seconds=timeout_seconds,
                artifacts_dir=artifacts_dir,
                trust=trust,
            )
        bundle_hash = str(envelope["bundle_hash"])
        trust_identity = self._trust_identity(envelope)
        with _hash_lock(self.cache_root, f"bundle-{bundle_hash}"):
            manifest = envelope["manifest"]
            if not manifest.get("callable") or manifest.get("identity", {}).get("entry_type") != "object":
                raise _error(
                    "public_entry_not_callable",
                    "public Library Adapters are components, not callable Functions",
                )
            object_yaml = bundle_dir / "object.yaml"
            if not object_yaml.is_file():
                raise _error("public_archive_disagreement", "callable release has no object.yaml member")
            if b"noderemote" in object_yaml.read_bytes().lower():
                raise _error("node_remote_not_allowed", "embedded public execution rejects NodeRemote")
            execution = manifest.get("execution")
            expected_execution = {
                "kind": "spl-compiled-python-v1",
                "member": "object.py",
                "compiler": "spl.core.ir.utils.spl_compile_to_source",
            }
            object_python = bundle_dir / "object.py"
            if execution != expected_execution or not object_python.is_file():
                raise _error(
                    "public_archive_disagreement",
                    "callable release has no exact producer-compiled execution member",
                )
            self._runtime_locks(manifest)
            if trust:
                self._approve(trust_identity)
            if not self._trusted(trust_identity):
                raise _error(
                    "public_trust_required",
                    "first execution of this exact public content hash requires trust=True",
                )
            with self._locked_environments(manifest, bundle_dir=bundle_dir) as environments:
                event_id = str(uuid4())
                self._emit_run_receipt(resolved, event_id=event_id, state="started")
                try:
                    result = self._execute(
                        resolved,
                        manifest,
                        object_python=object_python,
                        environments=environments,
                        args=args or [],
                        kwargs=kwargs or {},
                        timeout_seconds=timeout_seconds,
                        artifacts_dir=artifacts_dir,
                    )
                except BaseException:
                    self._emit_run_receipt(resolved, event_id=event_id, state="failure")
                    raise
                self._emit_run_receipt(resolved, event_id=event_id, state="success")
                run, payload, downloaded = result
                return EmbeddedCallResult(run, payload, downloaded, None, False)

    def _call_inprocess(
        self,
        resolved: Mapping[str, Any],
        envelope: Mapping[str, Any],
        bundle_dir: Path,
        *,
        args: list[Any] | None,
        kwargs: dict[str, Any] | None,
        timeout_seconds: float | None,
        artifacts_dir: str | Path | None,
        trust: bool,
    ) -> "EmbeddedCallResult":
        from spl import public_inprocess

        # Retain authenticated bytes in memory, then release the bundle lock.
        # User code may reenter this client or run concurrently with other calls.
        with _hash_lock(self.cache_root, f"bundle-{bundle_dir.name}"):
            body = (bundle_dir / "bundle.zip").read_bytes()
            manifest = verify_public_bundle(
                envelope,
                body,
                registry_url=self.transport.registry_url,
                trusted_keys=self.trusted_keys,
            )
            if not manifest.get("callable") or manifest.get("identity", {}).get("entry_type") != "object":
                raise _error("public_entry_not_callable", "release is not a callable Object")
            members = {info.filename: data for info, data in _validated_archive_members(manifest, body)}
            public_inprocess.admit(
                manifest,
                args=[] if args is None else args,
                kwargs={} if kwargs is None else kwargs,
                timeout_seconds=timeout_seconds,
            )
            identity = self._trust_identity(envelope)
            if trust:
                self._approve(identity)
            if not self._trusted(identity):
                raise _error(
                    "public_trust_required",
                    "Execution in your current process requires trust=True for this exact release",
                )
            public_inprocess.admit_dependencies(manifest)
        run_id = f"embedded-{uuid4().hex}"
        run_dir = self.cache_root / "artifacts" / run_id
        event_id = str(uuid4())
        with _hash_lock(self.cache_root, f"artifact-{run_id}"):
            run_dir.mkdir(parents=True, exist_ok=False)
            self._emit_run_receipt(resolved, event_id=event_id, state="started")
            try:
                payload, native_value = public_inprocess.execute(
                    manifest,
                    members,
                    identity="_spl_public_" + str(envelope["manifest_hash"]),
                    args=[] if args is None else args,
                    kwargs={} if kwargs is None else kwargs,
                    run_dir=run_dir,
                )
                downloaded = public_inprocess.download_artifacts(payload, artifacts_dir)
            except BaseException:
                self._emit_run_receipt(resolved, event_id=event_id, state="failure")
                raise
            self._emit_run_receipt(resolved, event_id=event_id, state="success")
        self._cleanup_cache_directory(
            "artifacts", maximum=MAX_ARTIFACT_CACHE_ENTRIES, lock_prefix="artifact-", preserve={run_id}
        )
        return EmbeddedCallResult(
            {
                "id": run_id,
                "status": "succeeded",
                "mode": "embedded",
                "execution_profile": CURRENT_PROCESS_CONTRACT["profile"],
                "reference": resolved["reference"],
                "release_id": resolved["release"]["id"],
            },
            payload,
            downloaded,
            native_value,
            True,
        )

    def _emit_run_receipt(
        self,
        resolved: Mapping[str, Any],
        *,
        event_id: str,
        state: str,
    ) -> None:
        emitter = self.receipt_emitter
        if emitter is None:
            return
        try:
            emitter.emit(
                release_id=str(resolved["release"]["id"]),
                event_id=event_id,
                state=state,
                runtime_family=_runtime_family(),
                occurred_at=datetime.now(UTC).isoformat(),
            )
        except Exception:
            pass

    def _resolve_and_cache(self, reference: PublicObjectRef) -> tuple[dict[str, Any], dict[str, Any], Path]:
        mapping_path = self._resolution_path(reference)
        if reference.pinned and mapping_path.is_file():
            try:
                resolved = json.loads(mapping_path.read_text(encoding="utf-8"))
                envelope, directory = self._cached_release(resolved)
                self._validate_resolution_binding(reference, resolved, envelope)
                return resolved, envelope, directory
            except (ClientError, OSError, ValueError, json.JSONDecodeError):
                pass
        self._require_capability()
        owner = quote(f"@{reference.owner}", safe="@")
        library = quote(reference.library, safe="")
        name = quote(reference.name, safe="")
        path = f"/public/v1/objects/{owner}/{library}/{name}"
        if reference.version is not None:
            path += f"/releases/{reference.version}"
        document = self.transport.json(path)
        release = document.get("release") or document.get("active_release")
        if not isinstance(release, dict):
            raise _error("public_registry_invalid", "registry release projection is incomplete")
        resolved = {
            "schema": "spl.public_resolution.v1",
            "registry_url": self.transport.registry_url,
            "reference": format_public_ref(reference),
            "release": release,
        }
        envelope = self.transport.json(str(release["manifest_url"]))
        self._validate_resolution_binding(reference, resolved, envelope)
        bundle_bytes = self.transport.bytes(
            str(release["bundle_url"]),
            accept="application/vnd.splime.public-bundle+zip",
        )
        bundle_dir = self._install_bundle(envelope, bundle_bytes)
        if reference.pinned:
            _atomic_write(mapping_path, _canonical_json(resolved))
        return resolved, envelope, bundle_dir

    def _validate_resolution_binding(
        self,
        reference: PublicObjectRef,
        resolved: Mapping[str, Any],
        envelope: Mapping[str, Any],
    ) -> None:
        """Bind a registry projection to the signed immutable Object identity."""

        release = resolved.get("release")
        manifest = envelope.get("manifest")
        identity = manifest.get("identity") if isinstance(manifest, Mapping) else None
        expected_uri = f"splime://@{reference.owner}/{reference.library}/{reference.name}"
        if not isinstance(release, Mapping) or not isinstance(manifest, Mapping) or not isinstance(identity, Mapping):
            raise _error("public_resolution_mismatch", "public release identity is incomplete")
        expected_version = reference.version
        comparisons = (
            identity.get("entry_type") == "object",
            identity.get("owner") == f"@{reference.owner}",
            identity.get("library") == reference.library,
            identity.get("name") == reference.name,
            identity.get("uri") == expected_uri,
            manifest.get("version") == release.get("version"),
            manifest.get("version_id") == release.get("version_id"),
            envelope.get("manifest_hash") == release.get("manifest_hash"),
            envelope.get("bundle_hash") == release.get("bundle_hash"),
        )
        if expected_version is not None and manifest.get("version") != expected_version:
            raise _error("public_resolution_mismatch", "pinned public version identity disagrees")
        if not all(comparisons):
            raise _error("public_resolution_mismatch", "public release identity disagrees with its reference")

    def _require_capability(self) -> None:
        version = self.transport.json("/version")
        declared = version.get("declared")
        contracts = declared.get("contracts") if isinstance(declared, Mapping) else version.get("contracts")
        capabilities = contracts.get("daemon_server_capabilities") if isinstance(contracts, Mapping) else None
        if not isinstance(capabilities, list) or PUBLIC_DISTRIBUTION_CAPABILITY not in capabilities:
            raise _error(
                "feature_not_supported",
                "server does not advertise anonymous public distribution",
            )

    def _install_bundle(self, envelope: dict[str, Any], bundle_bytes: bytes) -> Path:
        bundle_hash = str(envelope.get("bundle_hash") or "")
        if re.fullmatch(r"[0-9a-f]{64}", bundle_hash) is None:
            raise _error("public_manifest_invalid", "bundle identity is malformed")
        cache_identity = bundle_hash
        if envelope.get("manifest", {}).get("schema") == CURRENT_PROCESS_MANIFEST_SCHEMA:
            manifest_hash = str(envelope.get("manifest_hash") or "")
            if re.fullmatch(r"[0-9a-f]{64}", manifest_hash) is None:
                raise _error("public_manifest_invalid", "manifest identity is malformed")
            cache_identity += "-" + manifest_hash
        directory = self.cache_root / "bundles" / cache_identity
        with _hash_lock(self.cache_root, f"bundle-{cache_identity}"):
            if directory.is_dir():
                try:
                    verify_public_bundle(
                        json.loads((directory / "manifest.json").read_text(encoding="utf-8")),
                        (directory / "bundle.zip").read_bytes(),
                        registry_url=self.transport.registry_url,
                        trusted_keys=self.trusted_keys,
                    )
                    return directory
                except (ClientError, OSError, ValueError, json.JSONDecodeError):
                    _remove_cache_path(directory)
            directory.parent.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=f".{bundle_hash}-", dir=directory.parent))
            try:
                extracted = temporary / "extracted"
                verify_public_bundle(
                    envelope,
                    bundle_bytes,
                    registry_url=self.transport.registry_url,
                    trusted_keys=self.trusted_keys,
                    target_dir=extracted,
                )
                _atomic_write(temporary / "bundle.zip", bundle_bytes)
                _atomic_write(temporary / "manifest.json", _canonical_json(envelope))
                for child in extracted.iterdir():
                    os.replace(child, temporary / child.name)
                extracted.rmdir()
                os.replace(temporary, directory)
            finally:
                if temporary.exists():
                    _remove_cache_path(temporary)
        self._cleanup_cache_directory(
            "bundles",
            maximum=MAX_BUNDLE_CACHE_ENTRIES,
            lock_prefix="bundle-",
            preserve={cache_identity},
        )
        return directory

    def _cached_release(self, resolved: Mapping[str, Any]) -> tuple[dict[str, Any], Path]:
        release = resolved.get("release")
        if not isinstance(release, Mapping):
            raise _error("public_cache_corrupt", "cached resolution is malformed")
        bundle_hash = str(release.get("bundle_hash") or "")
        manifest_hash = str(release.get("manifest_hash") or "")
        if re.fullmatch(r"[0-9a-f]{64}", bundle_hash) is None or (
            manifest_hash and re.fullmatch(r"[0-9a-f]{64}", manifest_hash) is None
        ):
            raise _error("public_cache_corrupt", "cached content identities are malformed")
        directory = self.cache_root / "bundles" / bundle_hash
        current_process_directory = self.cache_root / "bundles" / f"{bundle_hash}-{manifest_hash}"
        if current_process_directory.is_dir():
            directory = current_process_directory
        envelope = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        verify_public_bundle(
            envelope,
            (directory / "bundle.zip").read_bytes(),
            registry_url=self.transport.registry_url,
            trusted_keys=self.trusted_keys,
        )
        return envelope, directory

    def _resolution_path(self, reference: PublicObjectRef) -> Path:
        key = _sha256(f"{self.transport.registry_url}\0{format_public_ref(reference)}".encode("utf-8"))
        return self.cache_root / "resolutions" / f"{key}.json"

    def _trust_identity(self, envelope: Mapping[str, Any]) -> str:
        signature = envelope.get("signature")
        if not isinstance(signature, Mapping):
            raise _error("public_manifest_invalid", "manifest envelope signature is incomplete")
        values = (
            self.transport.registry_url,
            str(signature.get("key_id") or ""),
            str(envelope.get("manifest_hash") or ""),
            str(envelope.get("bundle_hash") or ""),
        )
        return _sha256("\0".join(values).encode("utf-8"))

    def _approve(self, trust_identity: str) -> None:
        _atomic_write(self.cache_root / "trust" / trust_identity, b"trusted-exact-content-v2\n")

    def _trusted(self, trust_identity: str) -> bool:
        path = self.cache_root / "trust" / trust_identity
        try:
            return path.read_bytes() == b"trusted-exact-content-v2\n"
        except OSError:
            return False

    def _ensure_environments(
        self,
        manifest: Mapping[str, Any],
        *,
        bundle_dir: Path | None = None,
    ) -> dict[str, Path]:
        """Create one reusable environment for every distinct manifest lock."""

        locks = self._runtime_locks(manifest)
        by_hash: dict[str, Path] = {}
        result: dict[str, Path] = {}
        for lock in locks:
            lock_hash = str(lock["lock_hash"])
            environment = by_hash.get(lock_hash)
            if environment is None:
                environment = self._ensure_environment(lock, bundle_dir=bundle_dir)
                by_hash[lock_hash] = environment
            for name in lock["names"]:
                if name in result and result[name] != environment:
                    raise _error("public_manifest_invalid", "runtime lock name is ambiguous")
                result[str(name)] = environment
        if "root" not in result:
            raise _error("public_manifest_invalid", "runtime locks do not define the root environment")
        return result

    def _runtime_locks(self, manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
        raw_locks = manifest.get("runtime_locks")
        if raw_locks is None:
            raise _error(
                "public_manifest_incompatible",
                "executable public release has no artifact-complete runtime lock",
            )
        if not isinstance(raw_locks, list) or not raw_locks or len(raw_locks) > 64:
            raise _error("public_manifest_invalid", "runtime lock inventory is malformed")
        try:
            locks = [validate_runtime_lock(item) for item in raw_locks]
            for lock in locks:
                validate_installed_framework(lock["executor"])
            return locks
        except ValueError as exc:
            raise _error("public_manifest_invalid", str(exc)) from exc

    @contextmanager
    def _locked_environments(
        self,
        manifest: Mapping[str, Any],
        *,
        bundle_dir: Path | None = None,
    ) -> Iterator[dict[str, Path]]:
        """Build, verify, select, and use every runtime under uninterrupted locks."""

        locks = self._runtime_locks(manifest)
        projection = _installed_framework_projection()
        prepared = [(lock, self._validated_environment_digest(lock, projection[0])) for lock in locks]
        digests = {digest for _lock, digest in prepared}
        try:
            with ExitStack() as stack:
                for digest in sorted(digests):
                    stack.enter_context(_hash_lock(self.cache_root, f"environment-{digest}"))
                by_digest: dict[str, Path] = {}
                result: dict[str, Path] = {}
                for lock, digest in prepared:
                    environment = by_digest.get(digest)
                    if environment is None:
                        environment = self._ensure_environment_locked(
                            lock,
                            digest=digest,
                            projection=projection,
                            bundle_dir=bundle_dir,
                        )
                        by_digest[digest] = environment
                    for name in lock["names"]:
                        if name in result and result[name] != environment:
                            raise _error("public_manifest_invalid", "runtime lock name is ambiguous")
                        result[str(name)] = environment
                if "root" not in result:
                    raise _error("public_manifest_invalid", "runtime locks do not define the root environment")
                yield result
        finally:
            self._cleanup_cache_directory(
                "environments",
                maximum=MAX_ENVIRONMENT_CACHE_ENTRIES,
                lock_prefix="environment-",
                preserve=digests,
            )

    @staticmethod
    def _validated_environment_digest(
        lock: Mapping[str, Any],
        framework_identity: Mapping[str, str],
    ) -> str:
        target = lock.get("target")
        python_constraint = target.get("python") if isinstance(target, Mapping) else None
        try:
            matches = python_constraint_matches(python_constraint)
        except ValueError as exc:
            raise _error("public_manifest_invalid", str(exc)) from exc
        if not matches:
            raise _error(
                "embedded_python_unavailable",
                f"release requires Python {python_constraint}; running interpreter is "
                f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            )
        return environment_identity(
            lock,
            interpreter_abi=getattr(sys.implementation, "cache_tag", "unknown"),
            framework_identity=framework_identity,
        )

    def _ensure_environment(
        self,
        lock: Mapping[str, Any],
        *,
        bundle_dir: Path | None = None,
    ) -> Path:
        projection = _installed_framework_projection()
        digest = self._validated_environment_digest(lock, projection[0])
        with _hash_lock(self.cache_root, f"environment-{digest}"):
            python = self._ensure_environment_locked(
                lock,
                digest=digest,
                projection=projection,
                bundle_dir=bundle_dir,
            )
        self._cleanup_cache_directory(
            "environments",
            maximum=MAX_ENVIRONMENT_CACHE_ENTRIES,
            lock_prefix="environment-",
            preserve={digest},
        )
        return python

    def _ensure_environment_locked(
        self,
        lock: Mapping[str, Any],
        *,
        digest: str,
        projection: tuple[
            dict[str, str],
            str,
            list[tuple[PurePosixPath, bytes]],
            str,
            list[tuple[PurePosixPath, bytes]],
        ],
        bundle_dir: Path | None,
    ) -> Path:
        """Return one verified environment while its exact hash lock is held."""

        try:
            return self._ensure_environment_locked_impl(
                lock,
                digest=digest,
                projection=projection,
                bundle_dir=bundle_dir,
            )
        except OSError:
            raise _error(
                "embedded_environment_build_failed",
                "environment materialization was interrupted",
            ) from None

    def _ensure_environment_locked_impl(
        self,
        lock: Mapping[str, Any],
        *,
        digest: str,
        projection: tuple[
            dict[str, str],
            str,
            list[tuple[PurePosixPath, bytes]],
            str,
            list[tuple[PurePosixPath, bytes]],
        ],
        bundle_dir: Path | None,
    ) -> Path:
        """Materialize one environment after the caller acquires its hash lock."""

        framework_identity = projection[0]
        environment_root = self.cache_root / "environments"
        if environment_root.is_symlink() or (environment_root.exists() and not environment_root.is_dir()):
            _remove_cache_path(environment_root)
        environment_root.mkdir(parents=True, exist_ok=True)
        directory = environment_root / digest
        marker = directory / "ready.json"
        marker_document = _environment_marker_document(
            digest=digest,
            lock_hash=str(lock["lock_hash"]),
            framework_identity=framework_identity,
        )
        wheels = self._materialize_wheels(lock, bundle_dir=bundle_dir)
        expected_files = _expected_environment_files(
            lock=lock,
            digest=digest,
            projection=projection,
            wheels=wheels,
        )
        if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
            _remove_cache_path(directory)
        python = directory.joinpath(*_environment_python_relative().parts)
        try:
            ready = _read_environment_marker(marker)
            marker_valid = _canonical_json(ready) == _canonical_json(marker_document)
        except (ClientError, OSError, ValueError):
            marker_valid = False
        if marker_valid and _verify_environment_inventory(directory, expected_files):
            if self._environment_healthy(python):
                return python
        if directory.exists():
            _remove_cache_path(directory)
        temporary = Path(tempfile.mkdtemp(prefix=f".{digest}-", dir=directory.parent))
        try:
            try:
                _write_environment_tree(temporary, expected_files)
            except OSError:
                raise _error(
                    "embedded_environment_build_failed",
                    "environment materialization was interrupted",
                ) from None
            temporary_python = temporary.joinpath(*_environment_python_relative().parts)
            ready = _read_environment_marker(temporary / "ready.json")
            if (
                _canonical_json(ready) != _canonical_json(marker_document)
                or not _verify_environment_inventory(temporary, expected_files)
                or not self._environment_healthy(temporary_python)
            ):
                raise _error(
                    "embedded_environment_build_failed",
                    "new environment failed integrity verification",
                )
            os.replace(temporary, directory)
            python = directory.joinpath(*_environment_python_relative().parts)
            if not _verify_environment_inventory(directory, expected_files) or not self._environment_healthy(python):
                _remove_cache_path(directory)
                raise _error(
                    "embedded_environment_build_failed",
                    "new environment failed integrity verification",
                )
        finally:
            if temporary.exists():
                _remove_cache_path(temporary)
        return python

    @staticmethod
    def _environment_healthy(python: Path) -> bool:
        if not python.is_file():
            return False
        try:
            completed = run_process_tree(
                [
                    str(python),
                    "-B",
                    "-I",
                    "-c",
                    (
                        "import importlib.metadata as m; "
                        "from spl.daemon.worker import PUBLIC_EMBEDDED_HOST_CONTRACT as c; "
                        "assert c == 'spl.public_embedded_host.v1'; "
                        "assert tuple(map(int, m.version('splime').split('.'))) >= (0, 4, 9)"
                    ),
                ],
                env={
                    "PATH": str(python.parent),
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                    "PYTHONUTF8": "1",
                    "PYTHONNOUSERSITE": "1",
                },
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired):
            return False
        return completed.returncode == 0

    def _materialize_wheels(
        self,
        lock: Mapping[str, Any],
        *,
        bundle_dir: Path | None,
    ) -> list[Path]:
        del bundle_dir
        artifacts = list(lock["artifacts"])
        result: list[Path] = []
        for artifact in artifacts:
            digest = str(artifact["sha256"])
            filename = str(artifact["filename"])
            directory = self.cache_root / "wheels" / digest
            path = directory / filename
            with _hash_lock(self.cache_root, f"wheel-{digest}"):
                if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
                    _remove_cache_path(directory)
                if path.is_file():
                    try:
                        self._verify_wheel_file(path, artifact)
                    except (ClientError, OSError, ValueError, zipfile.BadZipFile):
                        _remove_cache_path(directory)
                if not path.is_file():
                    source = artifact["source"]
                    data = self.artifact_transport.wheel(
                        str(source["url"]),
                        expected_size=int(artifact["size"]),
                    )
                    if len(data) != artifact["size"] or _sha256(data) != digest:
                        raise _error("public_artifact_hash_mismatch", "wheel bytes disagree with the signed lock")
                    directory.parent.mkdir(parents=True, exist_ok=True)
                    temporary = Path(tempfile.mkdtemp(prefix=f".{digest}-", dir=directory.parent))
                    try:
                        temporary_path = temporary / filename
                        _atomic_write(temporary_path, data)
                        self._verify_wheel_file(temporary_path, artifact)
                        os.replace(temporary, directory)
                    finally:
                        if temporary.exists():
                            _remove_cache_path(temporary)
                self._verify_wheel_file(path, artifact)
            result.append(path)
        return result

    @staticmethod
    def _verify_wheel_file(path: Path, artifact: Mapping[str, Any]) -> bytes:
        data = _read_bounded_regular(path, maximum=MAX_WHEEL_BYTES, label="cached wheel")
        if len(data) != artifact["size"] or _sha256(data) != artifact["sha256"]:
            raise _error("public_artifact_hash_mismatch", "cached wheel bytes disagree with the signed lock")
        try:
            members = inspect_wheel_archive(data)
        except PublicArtifactPolicyError as exc:
            raise _error("public_artifact_unsafe", str(exc)) from exc
        names = list(members)
        violation = next(
            (message for name in names if (message := _runtime_projection_violation(name)) is not None),
            None,
        )
        if violation is not None:
            raise _error("public_artifact_unsafe", violation)
        if any(name.casefold().endswith((".so", ".dylib", ".dll", ".pyd", ".a", ".lib", ".exe")) for name in names):
            raise _error("public_artifact_unsafe", "wheel contains native members")
        wheel_names = [name for name in names if name.endswith(".dist-info/WHEEL")]
        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(wheel_names) != 1 or len(metadata_names) != 1:
            raise _error("public_artifact_unsafe", "wheel metadata inventory is invalid")
        try:
            import email

            wheel_metadata = email.message_from_bytes(members[wheel_names[0]])
            metadata = email.message_from_bytes(members[metadata_names[0]])
        except (UnicodeDecodeError, ValueError) as exc:
            raise _error("public_artifact_unsafe", "wheel metadata is malformed") from exc
        if str(wheel_metadata.get("Root-Is-Purelib", "")).casefold() != "true":
            raise _error("public_artifact_unsafe", "wheel is not pure Python")
        project = re.sub(r"[-_.]+", "-", str(metadata.get("Name", ""))).casefold()
        expected_project = re.sub(r"[-_.]+", "-", str(artifact.get("project", ""))).casefold()
        tags = sorted(str(value) for value in wheel_metadata.get_all("Tag", []))
        requires_dist = sorted(str(value) for value in metadata.get_all("Requires-Dist", []))
        expected_requirements = sorted(
            str(value["requirement"])
            for value in artifact.get("evaluated_requirements", [])
            if isinstance(value, Mapping)
        )
        if (
            project != expected_project
            or project == "splime"
            or str(metadata.get("Version", "")) != artifact.get("version")
            or str(metadata.get("Requires-Python", "")) != artifact.get("requires_python")
            or str(metadata.get("License-Expression", "")) != artifact.get("license_expression")
            or tags != artifact.get("tags")
            or requires_dist != expected_requirements
        ):
            raise _error("public_artifact_unsafe", "wheel metadata disagrees with the signed lock")
        return data

    @contextmanager
    def _environment_use_locks(self, environments: Iterable[Path]) -> Iterator[None]:
        environment_root = (self.cache_root / "environments").absolute()
        digests: set[str] = set()
        for python in environments:
            path = Path(python).absolute()
            try:
                relative = path.relative_to(environment_root)
            except ValueError:
                continue
            if len(relative.parts) >= 2 and re.fullmatch(r"[0-9a-f]{64}", relative.parts[0]):
                digests.add(relative.parts[0])
        with ExitStack() as stack:
            for digest in sorted(digests):
                stack.enter_context(_hash_lock(self.cache_root, f"environment-{digest}"))
            yield

    def _cleanup_cache_directory(
        self,
        namespace: str,
        *,
        maximum: int,
        lock_prefix: str,
        preserve: set[str],
    ) -> None:
        root = self.cache_root / namespace
        if root.is_symlink() or not root.is_dir():
            return
        entries = [
            item for item in root.iterdir() if item.is_dir() and not item.is_symlink() and not item.name.startswith(".")
        ]
        if len(entries) <= maximum:
            return
        entries.sort(key=lambda item: (item.stat().st_mtime_ns, item.name))
        remove_count = len(entries) - maximum
        for entry in entries:
            if remove_count <= 0:
                break
            if entry.name in preserve:
                continue
            with _try_hash_lock(self.cache_root, f"{lock_prefix}{entry.name}") as acquired:
                if not acquired or not entry.is_dir():
                    continue
                _remove_cache_path(entry)
                remove_count -= 1

    def _execute(
        self,
        resolved: Mapping[str, Any],
        manifest: Mapping[str, Any],
        *,
        object_python: Path,
        environments: Mapping[str, Path],
        args: list[Any],
        kwargs: dict[str, Any],
        timeout_seconds: float | None,
        artifacts_dir: str | Path | None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Path]]:
        environment = environments["root"]
        run_id = f"embedded-{uuid4().hex}"
        run_dir = self.cache_root / "artifacts" / run_id
        with _hash_lock(self.cache_root, f"artifact-{run_id}"):
            run_dir.mkdir(parents=True, exist_ok=False)
            input_path = run_dir / "input.json"
            result_path = run_dir / "result.json"
            retained = run_dir / "files"
            retained.mkdir()
            _atomic_write(
                input_path,
                _canonical_json(
                    {
                        "args": args,
                        "kwargs": kwargs,
                        "runtime_config": manifest.get("python_constraints", {}).get("runtime") or {},
                        "node_runtime_environments": {
                            "venv-subprocess": {
                                "default": {
                                    "python_path": str(environment),
                                    "lock_hash": _environment_lock_hash(environment),
                                },
                                **{
                                    name: {
                                        "python_path": str(python),
                                        "lock_hash": _environment_lock_hash(python),
                                    }
                                    for name, python in environments.items()
                                    if name != "root"
                                },
                            }
                        },
                        "work_dir": str(run_dir),
                        "keep": True,
                    }
                ),
            )
            environment_values = {
                "PATH": str(environment.parent),
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PYTHONUTF8": "1",
                "PYTHONNOUSERSITE": "1",
                "SPL_EMBEDDED_MODE": "1",
                "TMPDIR": str(run_dir),
            }
            command = [
                str(environment),
                "-B",
                "-I",
                "-m",
                "spl.daemon.worker",
                "--object-python",
                str(object_python),
                "--entrypoint",
                str(manifest.get("entrypoint") or ""),
                "--input",
                str(input_path),
                "--result",
                str(result_path),
                "--artifacts-dir",
                str(retained),
            ]
            try:
                completed = run_process_tree(
                    command,
                    cwd=object_python.parent,
                    env=environment_values,
                    timeout=timeout_seconds,
                )
            except subprocess.TimeoutExpired as exc:
                raise _error("embedded_run_timeout", "embedded public run timed out") from exc
            if completed.returncode or not result_path.is_file():
                raise _error("embedded_run_failed", "embedded public execution failed")
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            downloaded: dict[str, Path] = {}
            if artifacts_dir is not None:
                destination = Path(artifacts_dir)
                destination.mkdir(parents=True, exist_ok=True)
                for name, raw_path in (payload.get("artifacts") or {}).items():
                    source = Path(str(raw_path))
                    target = destination / _safe_artifact_name(str(name))
                    shutil.copyfile(source, target)
                    downloaded[str(name)] = target
        self._cleanup_cache_directory(
            "artifacts",
            maximum=MAX_ARTIFACT_CACHE_ENTRIES,
            lock_prefix="artifact-",
            preserve={run_id},
        )
        run = {
            "id": run_id,
            "status": "succeeded",
            "mode": "embedded",
            "reference": resolved["reference"],
            "release_id": resolved["release"]["id"],
        }
        return run, payload, downloaded


def _safe_artifact_name(value: str) -> str:
    if not value or PurePosixPath(value).name != value or value in {".", ".."}:
        raise _error("embedded_artifact_invalid", "worker returned an unsafe artifact name")
    return value


def _environment_lock_hash(python: Path) -> str | None:
    """Read a cache marker's immutable lock identity without exposing paths."""

    marker = python.parent.parent / "ready.json"
    try:
        value = _read_environment_marker(marker)
    except (ClientError, OSError, ValueError):
        return None
    lock_hash = value.get("lock_hash")
    return str(lock_hash) if isinstance(lock_hash, str) else None


class UnsupportedEmbeddedDaemon:
    """Mode guard that gives every daemon-only surface one stable failure."""

    def __getattr__(self, name: str) -> Any:
        def unsupported(*args: Any, **kwargs: Any) -> Any:
            raise _error(
                "feature_not_supported",
                f"{name} is unavailable in SPLClient embedded mode",
            )

        return unsupported


__all__ = [
    "EmbeddedBackend",
    "PublicRegistryTransport",
    "UnsupportedEmbeddedDaemon",
    "default_public_cache_dir",
    "PublicRunReceiptEmitter",
    "verify_public_bundle",
]
