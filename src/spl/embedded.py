"""Daemon-free verified public release transport, cache, and worker runtime."""

from __future__ import annotations

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
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
from uuid import uuid4

import certifi
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from spl._process import run_process_tree
from spl.daemon_client import ClientError
from spl.official_registry import official_registry_trusted_keys
from spl.public import PublicObjectRef, format_public_ref, parse_public_ref
from spl.public_artifact_policy import (
    PublicArtifactPolicyError,
    inspect_wheel_archive,
)
from spl.runtime_environment import (
    PUBLIC_EMBEDDED_HOST_CONTRACT,
    default_venv_command_builder,
    environment_identity,
    python_constraint_matches,
    validate_runtime_lock,
    venv_python_path,
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
MAX_RUN_RECEIPT_QUEUE = 64
MAX_RUN_RECEIPT_RESPONSE_BYTES = 64 * 1024
RUN_RECEIPT_TIMEOUT_SECONDS = 2.0
PUBLIC_RUN_RECEIPTS_ENV = "SPL_PUBLIC_RUN_RECEIPTS"
_THREAD_LOCK_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.Lock] = {}


def _tree_hash(root: Path) -> str:
    """Hash the installed framework package without mutable bytecode caches."""

    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root)
        if "__pycache__" in relative.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        if path.is_symlink():
            raise _error("embedded_framework_invalid", "installed splime package contains a symlink")
        if path.is_dir():
            continue
        if not path.is_file():
            raise _error("embedded_framework_invalid", "installed splime package contains a non-file member")
        data = path.read_bytes()
        digest.update(relative.as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(data).digest())
    return digest.hexdigest()


def _runtime_projection_violation(name: str) -> str | None:
    """Reject wheel members that can pre-empt the embedded framework host."""

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
    if first in {"spl", "spl.py", "spl.pyc", "spl.pyo"}:
        return "Object dependencies cannot replace the installed splime authority"
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
        return "Object dependencies cannot install Python startup hooks"
    return None


def _version_tuple(value: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", value)
    if match is None:
        raise _error("embedded_framework_invalid", "installed splime version is malformed")
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def _installed_framework_authority(executor: Mapping[str, Any]) -> dict[str, Any]:
    """Return the exact installed ``spl`` package used as execution authority."""

    if executor.get("contract") != PUBLIC_EMBEDDED_HOST_CONTRACT:
        raise _error("public_manifest_invalid", "installed framework executor contract is unsupported")
    try:
        version = importlib.metadata.version("splime")
    except importlib.metadata.PackageNotFoundError as exc:
        raise _error("embedded_framework_unavailable", "splime is not installed") from exc
    minimum = str(executor.get("minimum_version") or "")
    if _version_tuple(version) < _version_tuple(minimum):
        raise _error(
            "embedded_framework_upgrade_required",
            f"public execution requires splime>={minimum}; installed version is {version}",
        )
    package_root = Path(__file__).resolve().parent
    identity = _tree_hash(package_root)
    return {
        "version": version,
        "package_root": package_root,
        "tree_hash": identity,
        "identity": _sha256(f"{PUBLIC_EMBEDDED_HOST_CONTRACT}\0{version}\0{identity}".encode("utf-8")),
    }


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
    """Bounded daemon-thread sender; local execution never waits for telemetry."""

    def __init__(self, registry_url: str) -> None:
        self.transport = PublicRegistryTransport(
            registry_url,
            timeout_seconds=RUN_RECEIPT_TIMEOUT_SECONDS,
        )
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=MAX_RUN_RECEIPT_QUEUE)
        self._started = False
        self._lock = threading.Lock()

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
        try:
            self._queue.put_nowait(dict(payload))
        except queue.Full:
            return
        self._start()

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
    if not isinstance(manifest, Mapping) or manifest.get("schema") != PUBLIC_MANIFEST_SCHEMA:
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
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Path]]:
        reference = parse_public_ref(reference_text)
        resolved, envelope, bundle_dir = self._resolve_and_cache(reference)
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
            if trust:
                self._approve(trust_identity)
            if not self._trusted(trust_identity):
                raise _error(
                    "public_trust_required",
                    "first execution of this exact public content hash requires trust=True",
                )
            environments = self._ensure_environments(manifest, bundle_dir=bundle_dir)
            with self._environment_use_locks(environments.values()):
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
                return result

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
        directory = self.cache_root / "bundles" / bundle_hash
        with _hash_lock(self.cache_root, f"bundle-{bundle_hash}"):
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
                    shutil.rmtree(directory)
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
                    shutil.rmtree(temporary)
        self._cleanup_cache_directory(
            "bundles",
            maximum=MAX_BUNDLE_CACHE_ENTRIES,
            lock_prefix="bundle-",
            preserve={bundle_hash},
        )
        return directory

    def _cached_release(self, resolved: Mapping[str, Any]) -> tuple[dict[str, Any], Path]:
        release = resolved.get("release")
        if not isinstance(release, Mapping):
            raise _error("public_cache_corrupt", "cached resolution is malformed")
        bundle_hash = str(release.get("bundle_hash") or "")
        directory = self.cache_root / "bundles" / bundle_hash
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
            return [validate_runtime_lock(item) for item in raw_locks]
        except ValueError as exc:
            raise _error("public_manifest_invalid", str(exc)) from exc

    def _ensure_environment(
        self,
        lock: Mapping[str, Any],
        *,
        bundle_dir: Path | None = None,
    ) -> Path:
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
        authority = _installed_framework_authority(lock["executor"])
        base_identity = environment_identity(
            lock,
            interpreter_abi=getattr(sys.implementation, "cache_tag", "unknown"),
        )
        digest = _sha256(f"{base_identity}\0{authority['identity']}".encode("utf-8"))
        directory = self.cache_root / "environments" / digest
        marker = directory / "ready.json"
        with _hash_lock(self.cache_root, f"environment-{digest}"):
            if marker.is_file():
                try:
                    ready = json.loads(marker.read_text(encoding="utf-8"))
                    python = Path(str(ready["python"]))
                    framework_path = Path(str(ready["framework_path"]))
                    if (
                        ready["identity"] == digest
                        and ready["lock_hash"] == lock["lock_hash"]
                        and ready["framework_identity"] == authority["identity"]
                        and framework_path.is_dir()
                        and _tree_hash(framework_path) == authority["tree_hash"]
                        and self._environment_healthy(python)
                    ):
                        return python
                except (KeyError, OSError, ValueError, json.JSONDecodeError):
                    pass
            if directory.exists():
                shutil.rmtree(directory)
            directory.parent.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=f".{digest}-", dir=directory.parent))
            try:
                python = venv_python_path(temporary)
                builder = default_venv_command_builder()
                wheels = self._materialize_wheels(lock, bundle_dir=bundle_dir)
                spec = {
                    "base_python": sys.executable,
                    "venv_path": temporary,
                    "python_path": python,
                }
                commands = [builder.create_command(spec)]
                if wheels:
                    install_command = builder.install_command(
                        spec,
                        [str(path) for path in wheels],
                    )
                    install_index = install_command.index("install") + 1
                    install_command[install_index:install_index] = [
                        "--no-index",
                        "--no-deps",
                    ]
                    commands.append(install_command)
                for command in commands:
                    try:
                        built = run_process_tree(command, timeout=600)
                    except (OSError, subprocess.TimeoutExpired) as exc:
                        raise _error(
                            "embedded_environment_build_failed",
                            "environment build command was interrupted",
                        ) from exc
                    if built.returncode:
                        raise _error(
                            "embedded_environment_build_failed",
                            (built.stderr or "environment installation failed")[-2000:],
                        )
                framework_path = self._project_framework_authority(
                    python,
                    temporary,
                    authority,
                )
                final_python = directory / python.relative_to(temporary)
                final_framework_path = directory / framework_path.relative_to(temporary)
                _atomic_write(
                    temporary / "ready.json",
                    _canonical_json(
                        {
                            "identity": digest,
                            "lock_hash": lock["lock_hash"],
                            "builder": builder.name,
                            "python": str(final_python),
                            "framework_path": str(final_framework_path),
                            "framework_identity": authority["identity"],
                        }
                    ),
                )
                os.replace(temporary, directory)
                if not self._environment_healthy(final_python):
                    shutil.rmtree(directory)
                    raise _error(
                        "embedded_environment_build_failed",
                        "new environment failed its worker import health check",
                    )
            finally:
                if temporary.exists():
                    shutil.rmtree(temporary)
        self._cleanup_cache_directory(
            "environments",
            maximum=MAX_ENVIRONMENT_CACHE_ENTRIES,
            lock_prefix="environment-",
            preserve={digest},
        )
        return Path(json.loads(marker.read_text(encoding="utf-8"))["python"])

    @staticmethod
    def _project_framework_authority(
        python: Path,
        environment: Path,
        authority: Mapping[str, Any],
    ) -> Path:
        """Copy only the installed ``spl`` package into the isolated runtime."""

        discovered = run_process_tree(
            [
                str(python),
                "-I",
                "-c",
                "import sysconfig; print(sysconfig.get_paths()['purelib'])",
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
        if discovered.returncode:
            raise _error("embedded_environment_build_failed", "environment package path discovery failed")
        environment_root = environment.resolve()
        purelib = Path(discovered.stdout.strip()).resolve()
        try:
            relative_purelib = purelib.relative_to(environment_root)
        except ValueError as exc:
            raise _error("embedded_environment_build_failed", "environment package path escaped its root") from exc
        target = environment / relative_purelib / "spl"
        if target.exists():
            raise _error("embedded_environment_build_failed", "an Object dependency attempted to provide spl")
        shutil.copytree(
            Path(authority["package_root"]),
            target,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        if _tree_hash(target) != authority["tree_hash"]:
            raise _error("embedded_environment_build_failed", "installed framework projection changed during copy")
        return target

    @staticmethod
    def _environment_healthy(python: Path) -> bool:
        if not python.is_file():
            return False
        try:
            completed = run_process_tree(
                [str(python), "-I", "-m", "spl.daemon.worker", "--health-check"],
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
        return completed.returncode == 0 and completed.stdout.strip() == PUBLIC_EMBEDDED_HOST_CONTRACT

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
                if path.is_file():
                    try:
                        self._verify_wheel_file(path, artifact)
                    except (ClientError, OSError, ValueError, zipfile.BadZipFile):
                        shutil.rmtree(directory)
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
                            shutil.rmtree(temporary)
                self._verify_wheel_file(path, artifact)
            result.append(path)
        return result

    @staticmethod
    def _verify_wheel_file(path: Path, artifact: Mapping[str, Any]) -> None:
        data = path.read_bytes()
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
        if artifact.get("project") == "splime":
            raise _error("public_artifact_unsafe", "Object dependencies cannot replace the installed splime authority")
        if violation is not None:
            raise _error("public_artifact_unsafe", violation)
        if any(name.casefold().endswith((".so", ".dylib", ".dll", ".pyd", ".a", ".lib", ".exe")) for name in names):
            raise _error("public_artifact_unsafe", "wheel contains native members")
        wheel_names = [name for name in names if name.endswith(".dist-info/WHEEL")]
        if len(wheel_names) != 1:
            raise _error("public_artifact_unsafe", "wheel metadata inventory is invalid")
        wheel_metadata = members[wheel_names[0]].decode("utf-8", errors="strict")
        if "Root-Is-Purelib: true" not in wheel_metadata:
            raise _error("public_artifact_unsafe", "wheel is not pure Python")

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
        if not root.is_dir():
            return
        entries = [item for item in root.iterdir() if item.is_dir() and not item.name.startswith(".")]
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
                shutil.rmtree(entry)
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
                message = (completed.stderr or completed.stdout or "embedded worker failed")[-2000:]
                raise _error("embedded_run_failed", message)
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
        value = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    lock_hash = value.get("lock_hash") if isinstance(value, Mapping) else None
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
