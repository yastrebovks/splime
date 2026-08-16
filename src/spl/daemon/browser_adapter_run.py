"""Guarded browser-safe admission for Run-bound built-in adapters.

This module is daemon-owned and has no Jupyter or plugin dependency.  It is
an additive broker around the existing runtime-port-adapter implementation:
browser callers declare code-free built-in identities, upload raw bodies into
private request-owned staging, and finalize exactly once into the established
local or central-server execution path.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import stat
import threading
from collections.abc import AsyncIterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from typing import Any, Final, Literal, NoReturn, cast

from spl.adapters import JSON
from spl.core import json_contract as m_json_contract
from spl.core.runtime_port_adapters import (
    MAX_RUNTIME_INPUT_BYTES,
    MAX_RUNTIME_INPUT_TOTAL_BYTES,
    RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY,
    RuntimePortAdapterContractError,
    built_in_descriptor,
    adapter_descriptor,
    build_custom_bundle,
    implicit_json_wire_document,
    normalize_adapter_policy,
    normalize_runtime_adapter_semantic_advisories,
    normalize_wire_document,
    port_catalog_from_signature,
    runtime_port_adapter_fingerprint,
    runtime_adapter_semantic_category,
    runtime_adapter_semantic_state,
    validate_wire_document_against_signature,
)
from spl.core.entities.adapter import Adapter
from spl.core.library_adapters import (
    library_adapter_semantic_advisory,
    normalize_library_adapter_ref,
    normalize_runtime_library_adapter_refs,
)
from spl.daemon.connected_ide import validate_central_preflight_response
from spl.daemon.remote_client import ServerClientError
from spl.daemon.runtime_adapter_registry import build_runtime_adapter_registry_document
from spl.daemon.signature import build_signature

ADAPTER_RUN_CAPABILITY: Final = "run.adapter.create.guarded"
ADAPTER_RUN_CAPABILITY_VERSION: Final = 1
ADAPTER_RUN_PREFLIGHT_ROUTE: Final = "/runs/adapter-preflights"
ADAPTER_RUN_ADMISSIONS_ROUTE: Final = "/runs/adapter-admissions"
ADAPTER_RUN_SCHEMA_VERSION: Final = 1

PREFLIGHT_REQUEST_SCHEMA: Final = "spl.daemon.adapter-run-preflight-request"
PREFLIGHT_SCHEMA: Final = "spl.daemon.adapter-run-preflight"
CREATE_REQUEST_SCHEMA: Final = "spl.daemon.adapter-run-admission-create"
ADMISSION_SCHEMA: Final = "spl.daemon.adapter-run-admission"
UPLOAD_RECEIPT_SCHEMA: Final = "spl.daemon.adapter-run-upload-receipt"
FINALIZE_REQUEST_SCHEMA: Final = "spl.daemon.adapter-run-finalize"
RUN_RECEIPT_SCHEMA: Final = "spl.daemon.adapter-run-receipt"
RESULT_SCHEMA: Final = "spl.daemon.adapter-run-result"
ERROR_SCHEMA: Final = "spl.daemon.adapter-run-error"

SEMANTIC_SIGNATURE_DOMAIN: Final = b"spl.daemon.adapter-run-semantic-signature.v1\0"
MAX_JSON_BODY_BYTES: Final = 2 * 1024 * 1024
MAX_PREVIEW_BYTES: Final = 64 * 1024
MAX_PNG_PREVIEW_BYTES: Final = 8 * 1024 * 1024
MAX_ARTIFACT_DOWNLOAD_BYTES: Final = 256 * 1024 * 1024
MAX_ACTIVE_PREFLIGHTS: Final = 128
MAX_ACTIVE_ADMISSIONS: Final = 32
MAX_STAGING_DECLARED_BYTES: Final = 1024 * 1024 * 1024
MAX_ARTIFACT_GRANTS: Final = 8192
MAX_IN_MEMORY_RUN_BINDINGS: Final = 512
PREFLIGHT_TTL_SECONDS: Final = 120
ADMISSION_TTL_SECONDS: Final = 15 * 60
MAX_DURABLE_RUN_BINDINGS: Final = 2048
MAX_DURABLE_BINDING_TOTAL_BYTES: Final = 64 * 1024 * 1024
DURABLE_BINDING_SCHEMA: Final = "spl.daemon.adapter-run-finalized-binding"

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_SAFE_ITEM = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,254}$")
_SAFE_FILE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
_HASH = re.compile(r"^sha256:[0-9a-f]{64}$")
_RAW_HASH = re.compile(r"^[0-9a-f]{64}$")
_MEDIA_TYPE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}$")
_UTC_MILLISECONDS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_POSIX_PATH = re.compile(r"(?:^|[\s=:(\[{'\"])(/[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)+)")
_WINDOWS_PATH = re.compile(r"(?:^|[\s=:(\[{'\"])(?:[A-Za-z]:\\|\\\\[^\\\s]+\\)[^\s,;)\]}'\"]+")
_FILE_URL = re.compile(r"(?i)\bfile:(?://)?")
_FORBIDDEN_CREDENTIAL_VALUE_PATTERNS = (
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{6,}"),
    re.compile(
        r"(?i)(?:[?&#]|^)(?:token|access[_-]?token|api[_-]?key|"
        r"secret|password|authorization|credential)=[^&#\s]+"
    ),
    re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@"),
    re.compile(r"\b(?:sk-[A-Za-z0-9_-]{8,}|spl-[A-Za-z0-9_-]{43})\b", re.IGNORECASE),
    re.compile(
        r"(?i)\b(?:daemon|provider|central)[-_](?:master[-_])?"
        r"(?:token|secret)[-_][A-Za-z0-9_-]{6,}\b"
    ),
)
_CAMEL_ACRONYM_BOUNDARY = re.compile(r"([A-Z]+)([A-Z][a-z])")
_CAMEL_WORD_BOUNDARY = re.compile(r"([a-z0-9])([A-Z])")
_KEY_SEPARATOR = re.compile(r"[^A-Za-z0-9]+")
_FORBIDDEN_KEY_EXACT = frozenset(
    {
        "token",
        "secret",
        "password",
        "authorization",
        "credential",
        "api_key",
        "provider_key",
        "private_key",
        "authorization_header",
        "authorization_value",
        "credential_value",
        "credential_blob",
        "credential_payload",
    }
)
_FORBIDDEN_KEY_SUFFIX = (
    "_token",
    "_secret",
    "_password",
    "_api_key",
    "_provider_key",
    "_private_key",
)
_COMMON_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "request_id",
        "daemon_instance_id",
        "daemon_generation",
        "registry_revision",
        "object",
        "content_hash",
        "signature_evidence",
        "target",
        "inputs",
        "outputs",
        "output_selector",
        "timeout_ms",
        "retention",
        "adapter_policy",
        "source",
    }
)
_LIBRARY_REFS_FIELD = "runtime_library_adapter_refs"
_SEMANTIC_ADVISORIES_FIELD = "runtime_adapter_semantic_advisories"
_CREATE_FIELDS = _COMMON_FIELDS | {"preflight"}
_OBJECT_FIELDS = frozenset(
    {
        "owner_id",
        "library",
        "object_name",
        "object_id",
        "expected_current_version_id",
        "selected_version_id",
        "selected_version",
        "function",
        "origin",
    }
)
_SIGNATURE_EVIDENCE_FIELDS = frozenset({"semantic_fields", "semantic_hash", "review_hash"})
_SEMANTIC_FIELDS = frozenset({"kind", "function", "inputs", "outputs"})
_SEMANTIC_INPUT_FIELDS = frozenset({"name", "type", "required"})
_SEMANTIC_OUTPUT_FIELDS = frozenset({"name", "selector", "type"})
_TARGET_FIELDS = frozenset({"kind", "machine_id", "offline_policy"})
_INPUT_FIELDS = frozenset(
    {
        "name",
        "port",
        "kind",
        "value",
        "adapter_id",
        "format_tag",
        "logical_name",
        "media_type",
        "size",
        "sha256",
    }
)
_OUTPUT_FIELDS = frozenset({"port", "adapter_id", "format_tag"})
_PREFLIGHT_PROOF_FIELDS = frozenset({"id", "digest_sha256", "expires_at"})

_ERROR_CODES = frozenset(
    {
        "request_invalid",
        "body_too_large",
        "daemon_identity_mismatch",
        "registry_revision_mismatch",
        "object_changed",
        "signature_mismatch",
        "adapter_unavailable",
        "adapter_binding_invalid",
        "target_preflight_required",
        "target_preflight_stale",
        "target_unavailable",
        "admission_not_found",
        "admission_conflict",
        "admission_expired",
        "admission_inactive",
        "item_not_found",
        "item_already_uploaded",
        "upload_size_mismatch",
        "upload_digest_mismatch",
        "upload_media_mismatch",
        "inputs_incomplete",
        "finalize_failed",
        "outcome_unknown",
        "run_not_found",
        "result_unavailable",
        "artifact_not_found",
        "artifact_expired",
        "artifact_mismatch",
    }
)
_ERROR_STAGES = frozenset({"preflight", "admission", "upload", "finalize", "reconcile", "result", "download"})


def browser_library_placeholder_save(path: str, value: object) -> None:
    del path, value
    raise RuntimeError("Library Adapter placeholder must be replaced during Run admission")


def browser_library_placeholder_load(path: str) -> object:
    del path
    raise RuntimeError("Library Adapter placeholder must be replaced during Run admission")


class AdapterRunError(RuntimeError):
    """Closed reason-coded browser adapter failure."""

    def __init__(
        self,
        code: str,
        stage: str,
        status: HTTPStatus,
        *,
        retryable: bool = False,
    ) -> None:
        if code not in _ERROR_CODES or stage not in _ERROR_STAGES:
            raise ValueError("adapter Run error is outside the closed catalog")
        self.code = code
        self.stage = stage
        self.status = status
        self.retryable = retryable
        super().__init__(code)


@dataclass
class _Preflight:
    preflight_id: str
    request_id: str
    digest_sha256: str
    expires_at: str
    document: dict[str, Any]


@dataclass
class _Admission:
    request_id: str
    request_digest_sha256: str
    created_at: str
    expires_at: str
    document: dict[str, Any]
    state: Literal[
        "staging",
        "ready",
        "finalized",
        "cancelled",
        "expired",
        "outcome_unknown",
    ]
    directory: Path
    uploaded: set[str] = field(default_factory=set)
    uploading: set[str] = field(default_factory=set)
    run_id: str | None = None
    receipt: dict[str, Any] | None = None
    runtime_fingerprint: str | None = None


@dataclass(frozen=True)
class _ArtifactGrant:
    run_id: str
    target: Literal["local", "remote"]
    artifact_name: str
    logical_name: str
    media_type: str
    size: int
    sha256: str


def parse_adapter_run_request(raw: bytes, *, create: bool) -> dict[str, Any]:
    """Parse one closed preflight or create request."""

    if len(raw) > MAX_JSON_BODY_BYTES:
        raise AdapterRunError(
            "body_too_large", "admission" if create else "preflight", HTTPStatus.REQUEST_ENTITY_TOO_LARGE
        )
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_closed_object,
            parse_constant=lambda _value: _raise_request_invalid(),
        )
    except AdapterRunError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise AdapterRunError(
            "request_invalid",
            "admission" if create else "preflight",
            HTTPStatus.BAD_REQUEST,
        ) from exc
    if not isinstance(value, dict):
        _invalid("admission" if create else "preflight")
    expected = _CREATE_FIELDS if create else _COMMON_FIELDS
    observed_fields = set(value)
    if not set(expected) <= observed_fields or observed_fields - set(expected) - {
        _LIBRARY_REFS_FIELD,
        _SEMANTIC_ADVISORIES_FIELD,
    }:
        _invalid("admission" if create else "preflight")
    schema = CREATE_REQUEST_SCHEMA if create else PREFLIGHT_REQUEST_SCHEMA
    if value["schema"] != schema or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        _invalid("admission" if create else "preflight")
    normalized = _normalize_common(value, stage="admission" if create else "preflight")
    if _LIBRARY_REFS_FIELD in value:
        try:
            normalized[_LIBRARY_REFS_FIELD] = normalize_runtime_library_adapter_refs(value[_LIBRARY_REFS_FIELD])
        except ValueError:
            _invalid("admission" if create else "preflight")
    if _SEMANTIC_ADVISORIES_FIELD in value:
        try:
            normalized[_SEMANTIC_ADVISORIES_FIELD] = normalize_runtime_adapter_semantic_advisories(
                value[_SEMANTIC_ADVISORIES_FIELD]
            )
        except ValueError:
            _invalid("admission" if create else "preflight")
    _validate_library_ref_selections(
        normalized,
        stage="admission" if create else "preflight",
    )
    normalized["schema"] = schema
    if create:
        proof = _mapping(value["preflight"], "admission")
        _exact(proof, _PREFLIGHT_PROOF_FIELDS, "admission")
        normalized["preflight"] = {
            "id": _identifier(proof["id"], "admission"),
            "digest_sha256": _raw_hash(proof["digest_sha256"], "admission"),
            "expires_at": _timestamp(proof["expires_at"], "admission"),
        }
    return normalized


def parse_finalize_request(raw: bytes, *, request_id: str) -> dict[str, Any]:
    if len(raw) > 4096:
        raise AdapterRunError("body_too_large", "finalize", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_closed_object)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise AdapterRunError("request_invalid", "finalize", HTTPStatus.BAD_REQUEST) from exc
    if not isinstance(value, dict) or set(value) != {"schema", "schema_version", "request_id"}:
        raise AdapterRunError("request_invalid", "finalize", HTTPStatus.BAD_REQUEST)
    if (
        value["schema"] != FINALIZE_REQUEST_SCHEMA
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["request_id"] != request_id
    ):
        raise AdapterRunError("request_invalid", "finalize", HTTPStatus.BAD_REQUEST)
    return cast(dict[str, Any], value)


def semantic_signature_fields(signature: Mapping[str, Any], *, function: str | None) -> dict[str, Any]:
    """Project exact, value-free fields that define adapter port semantics."""

    kind = signature.get("kind")
    if kind not in {"function", "pipeline"}:
        raise AdapterRunError("signature_mismatch", "preflight", HTTPStatus.PRECONDITION_FAILED)
    raw_inputs = signature.get("inputs")
    raw_outputs = signature.get("outputs")
    if not isinstance(raw_inputs, list) or not isinstance(raw_outputs, list):
        raise AdapterRunError("signature_mismatch", "preflight", HTTPStatus.PRECONDITION_FAILED)
    inputs = []
    for raw in raw_inputs:
        if not isinstance(raw, Mapping):
            raise AdapterRunError("signature_mismatch", "preflight", HTTPStatus.PRECONDITION_FAILED)
        inputs.append(
            {
                "name": _bounded_text(raw.get("name"), maximum=128),
                "type": _optional_bounded_text(raw.get("type"), maximum=512),
                "required": bool(raw.get("required")),
            }
        )
    outputs = []
    for raw in raw_outputs:
        if not isinstance(raw, Mapping):
            raise AdapterRunError("signature_mismatch", "preflight", HTTPStatus.PRECONDITION_FAILED)
        outputs.append(
            {
                "name": _bounded_text(raw.get("name"), maximum=128),
                "selector": _optional_bounded_text(raw.get("selector"), maximum=128),
                "type": _optional_bounded_text(raw.get("type"), maximum=512),
            }
        )
    return {"kind": kind, "function": function, "inputs": inputs, "outputs": outputs}


def semantic_signature_hash(fields: Mapping[str, Any]) -> str:
    encoded = m_json_contract.dumps(dict(fields), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return "sha256:" + hashlib.sha256(SEMANTIC_SIGNATURE_DOMAIN + encoded).hexdigest()


def adapter_run_error_document(error: AdapterRunError) -> dict[str, Any]:
    return {
        "schema": ERROR_SCHEMA,
        "schema_version": ADAPTER_RUN_SCHEMA_VERSION,
        "code": error.code,
        "stage": error.stage,
        "retryable": error.retryable,
    }


class BrowserAdapterRunBroker:
    """Process-local, daemon-generation-fenced adapter admission authority."""

    def __init__(self, runtime: Any, *, root: Path | None = None) -> None:
        self.runtime = runtime
        self.root = (root or (runtime.store.home / "browser-adapter-admissions")).absolute()
        self.staging_root = self.root / "staging"
        self.finalized_root = self.root / "finalized"
        self._lock = threading.RLock()
        self._preflights: dict[str, _Preflight] = {}
        self._admissions: dict[str, _Admission] = {}
        self._run_admissions: dict[str, _Admission] = {}
        self._artifact_grants: dict[str, _ArtifactGrant] = {}
        self._handle_key = secrets.token_bytes(32)
        self._reset_staging_root()
        self._prune_durable_bindings()
        self._load_durable_bindings()

    def shutdown(self) -> None:
        """Remove only this daemon instance's incomplete private staging."""

        with self._lock:
            for admission in self._admissions.values():
                self._cleanup_directory(admission.directory)
            self._preflights.clear()
            self._admissions.clear()
            self._run_admissions.clear()
            self._artifact_grants.clear()
        self._cleanup_directory(self.staging_root)

    def preflight(self, document: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._expire_locked()
            authoritative = self._validate_current(document, stage="preflight", remote_preflight=True)
            authoritative_semantic = authoritative["runtime_adapter_semantic_advisories"]
            canonical_document = dict(document)
            if authoritative_semantic is not None:
                canonical_document[_SEMANTIC_ADVISORIES_FIELD] = authoritative_semantic
            digest = _request_digest(canonical_document)
            now = _utc_now()
            expires_at = _utc_after(PREFLIGHT_TTL_SECONDS)
            preflight_id = "adapter-preflight-" + secrets.token_hex(16)
            record = _Preflight(
                preflight_id,
                document["request_id"],
                digest,
                expires_at,
                canonical_document,
            )
            if len(self._preflights) >= MAX_ACTIVE_PREFLIGHTS:
                oldest = min(
                    self._preflights,
                    key=lambda key: (self._preflights[key].expires_at, key),
                )
                self._preflights.pop(oldest, None)
            self._preflights[preflight_id] = record
            bindings = [
                {
                    "direction": binding["direction"],
                    "port": binding["port"],
                    "adapter_id": binding["adapter_id"],
                    "format_tag": binding["format_tag"],
                    "state": "available",
                    "reason": None,
                }
                for binding in authoritative["bindings"]
            ]
            response = {
                "schema": PREFLIGHT_SCHEMA,
                "schema_version": 1,
                "preflight_id": preflight_id,
                "digest_sha256": digest,
                "request_id": document["request_id"],
                "observed_at": now,
                "expires_at": expires_at,
                "target": dict(document["target"]),
                "bindings": bindings,
                "can_finalize": True,
                "reason": None,
            }
            if authoritative_semantic is not None:
                response[_SEMANTIC_ADVISORIES_FIELD] = authoritative_semantic
            return response

    def create(self, document: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        with self._lock:
            self._expire_locked()
            digest = _request_digest(document)
            existing = self._admissions.get(document["request_id"])
            if existing is not None:
                if existing.request_digest_sha256 != digest:
                    raise AdapterRunError("admission_conflict", "admission", HTTPStatus.CONFLICT)
                return self._projection(existing), False

            proof = document["preflight"]
            preflight = self._preflights.get(proof["id"])
            if (
                preflight is None
                or preflight.request_id != document["request_id"]
                or preflight.digest_sha256 != proof["digest_sha256"]
                or preflight.expires_at != proof["expires_at"]
                or preflight.digest_sha256 != digest
            ):
                raise AdapterRunError("target_preflight_required", "admission", HTTPStatus.PRECONDITION_FAILED)
            if _parse_timestamp(preflight.expires_at) <= datetime.now(UTC):
                raise AdapterRunError("target_preflight_stale", "admission", HTTPStatus.PRECONDITION_FAILED)
            authoritative = self._validate_current(document, stage="admission", remote_preflight=False)
            authoritative_semantic = authoritative["runtime_adapter_semantic_advisories"]
            if authoritative_semantic is not None:
                document = {**document, _SEMANTIC_ADVISORIES_FIELD: authoritative_semantic}
            active = [
                admission
                for admission in self._admissions.values()
                if admission.state in {"staging", "ready", "outcome_unknown"}
            ]
            if len(active) >= MAX_ACTIVE_ADMISSIONS:
                raise AdapterRunError(
                    "admission_conflict",
                    "admission",
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    retryable=True,
                )
            declared = sum(
                int(item["size"])
                for admission in active
                for item in admission.document["inputs"]
                if item["kind"] == "staged"
            ) + sum(int(item["size"]) for item in document["inputs"] if item["kind"] == "staged")
            if declared > MAX_STAGING_DECLARED_BYTES:
                raise AdapterRunError(
                    "body_too_large",
                    "admission",
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                )

            request_dir = self.staging_root / secrets.token_hex(24)
            request_dir.mkdir(mode=0o700, exist_ok=False)
            _require_private_directory(request_dir)
            now = _utc_now()
            staged = [item for item in document["inputs"] if item["kind"] == "staged"]
            admission = _Admission(
                request_id=document["request_id"],
                request_digest_sha256=digest,
                created_at=now,
                expires_at=_utc_after(ADMISSION_TTL_SECONDS),
                document=document,
                state="ready" if not staged else "staging",
                directory=request_dir,
            )
            self._admissions[admission.request_id] = admission
            return self._projection(admission), True

    def reconcile(self, request_id: str) -> dict[str, Any]:
        with self._lock:
            self._expire_locked()
            admission = self._require_admission(request_id, stage="reconcile")
            if admission.state == "outcome_unknown":
                if admission.document["target"]["kind"] == "remote":
                    self._reconcile_remote_unknown(admission)
                else:
                    self._reconcile_local_unknown(admission)
            return self._projection(admission)

    async def upload(
        self,
        request_id: str,
        name: str,
        chunks: AsyncIterable[bytes],
        *,
        content_length: int | None,
        digest_header: str | None,
        media_type: str | None,
    ) -> dict[str, Any]:
        name = _item_name(name, "upload")
        with self._lock:
            self._expire_locked()
            admission = self._require_admission(request_id, stage="upload")
            if admission.state not in {"staging", "ready"}:
                raise AdapterRunError("admission_inactive", "upload", HTTPStatus.CONFLICT)
            item = _staged_item(admission.document, name)
            if item is None:
                raise AdapterRunError("item_not_found", "upload", HTTPStatus.NOT_FOUND)
            if name in admission.uploaded or name in admission.uploading:
                raise AdapterRunError("item_already_uploaded", "upload", HTTPStatus.CONFLICT)
            if content_length is None or content_length != item["size"]:
                raise AdapterRunError("upload_size_mismatch", "upload", HTTPStatus.BAD_REQUEST)
            if digest_header != item["sha256"]:
                raise AdapterRunError("upload_digest_mismatch", "upload", HTTPStatus.BAD_REQUEST)
            expected_media = item["media_type"]
            if media_type is None or media_type.split(";", 1)[0].strip().casefold() != expected_media:
                raise AdapterRunError("upload_media_mismatch", "upload", HTTPStatus.BAD_REQUEST)
            admission.uploading.add(name)
            directory = admission.directory

        temporary = directory / ("." + secrets.token_hex(24) + ".upload")
        final = directory / (name + ".body")
        descriptor: int | None = None
        size = 0
        digest = hashlib.sha256()
        try:
            _require_private_directory(directory)
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(temporary, flags, 0o600)
            for_write = descriptor
            async for chunk in chunks:
                if not isinstance(chunk, bytes):
                    raise AdapterRunError("request_invalid", "upload", HTTPStatus.BAD_REQUEST)
                size += len(chunk)
                if size > item["size"] or size > MAX_RUNTIME_INPUT_BYTES:
                    raise AdapterRunError("upload_size_mismatch", "upload", HTTPStatus.BAD_REQUEST)
                digest.update(chunk)
                view = memoryview(chunk)
                while view:
                    written = os.write(for_write, view)
                    view = view[written:]
            os.fsync(descriptor)
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1 or opened.st_size != size:
                raise AdapterRunError("upload_size_mismatch", "upload", HTTPStatus.BAD_REQUEST)
            os.close(descriptor)
            descriptor = None
            if size != item["size"]:
                raise AdapterRunError("upload_size_mismatch", "upload", HTTPStatus.BAD_REQUEST)
            observed_digest = digest.hexdigest()
            if observed_digest != item["sha256"]:
                raise AdapterRunError("upload_digest_mismatch", "upload", HTTPStatus.BAD_REQUEST)
            with self._lock:
                current = self._require_admission(request_id, stage="upload")
                if current is not admission or current.state not in {"staging", "ready"}:
                    raise AdapterRunError("admission_inactive", "upload", HTTPStatus.CONFLICT)
                if final.exists():
                    raise AdapterRunError("item_already_uploaded", "upload", HTTPStatus.CONFLICT)
                temporary.replace(final)
                _require_private_regular_file(final, expected_size=size)
                admission.uploading.discard(name)
                admission.uploaded.add(name)
                required = {row["name"] for row in admission.document["inputs"] if row["kind"] == "staged"}
                admission.state = "ready" if admission.uploaded == required else "staging"
                return {
                    "schema": UPLOAD_RECEIPT_SCHEMA,
                    "schema_version": 1,
                    "request_id": request_id,
                    "name": name,
                    "size": size,
                    "sha256": observed_digest,
                    "state": "uploaded",
                    "observed_at": _utc_now(),
                }
        finally:
            if descriptor is not None:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)
            with self._lock:
                admission.uploading.discard(name)

    def finalize(self, request_id: str) -> tuple[dict[str, Any], bool]:
        with self._lock:
            self._expire_locked()
            admission = self._require_admission(request_id, stage="finalize")
            if cast(str, admission.state) == "finalized":
                return self._projection(admission), False
            if admission.state == "outcome_unknown":
                raise AdapterRunError("outcome_unknown", "finalize", HTTPStatus.SERVICE_UNAVAILABLE)
            if admission.state != "ready" or admission.uploading:
                if admission.state in {"cancelled", "expired"}:
                    raise AdapterRunError("admission_inactive", "finalize", HTTPStatus.CONFLICT)
                raise AdapterRunError("inputs_incomplete", "finalize", HTTPStatus.CONFLICT)

            authoritative = self._validate_current(
                admission.document,
                stage="finalize",
                remote_preflight=admission.document["target"]["kind"] == "remote",
            )
            wire, kwargs = self._wire_document(
                admission,
                authoritative["signature"],
                include_bodies=True,
                stage="finalize",
                library_records=authoritative["library_records"],
            )
            fingerprint = runtime_port_adapter_fingerprint(
                _staged_wire_projection(wire), adapter_policy=admission.document["adapter_policy"]
            )
            admission.runtime_fingerprint = fingerprint
            try:
                if admission.document["target"]["kind"] == "local":
                    state = self._finalize_local(admission, wire, kwargs)
                else:
                    # The central admission lifecycle can commit before its
                    # response is observed. Fence replay immediately before
                    # entering that mutation path.
                    admission.state = "outcome_unknown"
                    state = self._finalize_remote(admission, wire, kwargs)
            except AdapterRunError:
                raise
            except Exception as exc:
                if admission.document["target"]["kind"] == "remote":
                    self._reconcile_remote_unknown(admission)
                    if cast(str, admission.state) == "finalized":
                        return self._projection(admission), True
                    raise AdapterRunError(
                        "outcome_unknown", "finalize", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True
                    ) from None
                self._reconcile_local_unknown(admission)
                if cast(str, admission.state) == "finalized":
                    return self._projection(admission), True
                raise AdapterRunError(
                    "outcome_unknown", "finalize", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True
                ) from exc

            if cast(str, admission.state) == "finalized":
                return self._projection(admission), False
            try:
                self._commit_finalized(admission, state, fingerprint)
            except Exception as exc:
                raise AdapterRunError(
                    "outcome_unknown", "finalize", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True
                ) from exc
            return self._projection(admission), True

    def cancel(self, request_id: str) -> dict[str, Any]:
        with self._lock:
            self._expire_locked()
            admission = self._require_admission(request_id, stage="reconcile")
            if admission.state == "cancelled":
                return self._projection(admission)
            if admission.state in {"finalized", "expired"}:
                raise AdapterRunError("admission_inactive", "reconcile", HTTPStatus.CONFLICT)
            if admission.state == "outcome_unknown":
                if admission.document["target"]["kind"] == "remote":
                    self._reconcile_remote_unknown(admission)
                else:
                    self._reconcile_local_unknown(admission)
                if cast(str, admission.state) == "finalized":
                    raise AdapterRunError("admission_inactive", "reconcile", HTTPStatus.CONFLICT)
                if admission.document["target"]["kind"] == "local":
                    raise AdapterRunError(
                        "outcome_unknown",
                        "reconcile",
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        retryable=True,
                    )
                try:
                    server = self._remote_server()
                    central_id = _remote_admission_id(request_id)
                    current = server.get_remote_run_admission(central_id)
                    if current.get("state") == "staging":
                        server.cancel_remote_run_admission(central_id)
                except Exception:
                    raise AdapterRunError(
                        "outcome_unknown", "reconcile", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True
                    ) from None
            admission.state = "cancelled"
            self._cleanup_directory(admission.directory)
            return self._projection(admission)

    def project_result(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            admission = self._run_admissions.get(run_id)
            if admission is None:
                admission = self._load_durable_binding_for_run(run_id)
            if admission is None:
                raise AdapterRunError("run_not_found", "result", HTTPStatus.NOT_FOUND)
            target = cast(Literal["local", "remote"], admission.document["target"]["kind"])
            try:
                state = (
                    self.runtime.store.get_run(run_id)
                    if target == "local"
                    else self._remote_server().get_remote_run(run_id)
                )
            except (KeyError, ServerClientError) as exc:
                raise AdapterRunError("run_not_found", "result", HTTPStatus.NOT_FOUND) from exc
            status = _canonical_run_state(state.get("status"))
            outputs: list[dict[str, Any]] = []
            failure = None
            if status in {"succeeded", "failed", "cancelled", "timed_out"}:
                outputs = self._terminal_outputs(
                    admission,
                    state,
                    target=target,
                    successful=status == "succeeded",
                )
                if status != "succeeded":
                    failure = _safe_failure(state)
            result = {
                "schema": RESULT_SCHEMA,
                "schema_version": 1,
                "run_id": run_id,
                "state": status,
                "target": dict(admission.document["target"]),
                "outputs": outputs,
                "failure": failure,
            }
            semantic = admission.document.get(_SEMANTIC_ADVISORIES_FIELD)
            if isinstance(semantic, dict):
                result[_SEMANTIC_ADVISORIES_FIELD] = semantic
            return result

    def artifact_bytes(self, run_id: str, handle: str) -> tuple[bytes, str, str]:
        with self._lock:
            admission = self._run_admissions.get(run_id)
            grant = self._artifact_grants.get(handle)
            if admission is None or grant is None or grant.run_id != run_id:
                raise AdapterRunError("artifact_not_found", "download", HTTPStatus.NOT_FOUND)
            if not hmac.compare_digest(handle, self._artifact_handle(grant)):
                raise AdapterRunError("artifact_mismatch", "download", HTTPStatus.CONFLICT)
            if grant.target == "local":
                try:
                    state = self.runtime.store.get_run(run_id)
                except KeyError as exc:
                    raise AdapterRunError("run_not_found", "download", HTTPStatus.NOT_FOUND) from exc
                raw_dir = state.get("artifacts_dir")
                if not raw_dir:
                    raise AdapterRunError("artifact_expired", "download", HTTPStatus.GONE)
                path = Path(str(raw_dir)) / grant.artifact_name
                try:
                    data = _read_verified_file(path, size=grant.size, sha256=grant.sha256)
                except FileNotFoundError as exc:
                    raise AdapterRunError("artifact_expired", "download", HTTPStatus.GONE) from exc
            else:
                server = self._remote_server()
                try:
                    read_verified = getattr(server, "artifact_bytes_verified", None)
                    if not callable(read_verified):
                        raise AdapterRunError(
                            "result_unavailable",
                            "download",
                            HTTPStatus.SERVICE_UNAVAILABLE,
                            retryable=True,
                        )
                    data = read_verified(
                        run_id,
                        grant.artifact_name,
                        size=grant.size,
                        sha256=grant.sha256,
                    )
                except AdapterRunError:
                    raise
                except ServerClientError as exc:
                    if exc.status_code == 404:
                        raise AdapterRunError("artifact_expired", "download", HTTPStatus.GONE) from exc
                    raise AdapterRunError(
                        "result_unavailable", "download", HTTPStatus.SERVICE_UNAVAILABLE, retryable=True
                    ) from exc
                if len(data) != grant.size or hashlib.sha256(data).hexdigest() != grant.sha256:
                    raise AdapterRunError("artifact_mismatch", "download", HTTPStatus.CONFLICT)
            return data, grant.media_type, grant.logical_name

    def _validate_current(
        self,
        document: Mapping[str, Any],
        *,
        stage: str,
        remote_preflight: bool,
    ) -> dict[str, Any]:
        identity = self.runtime.daemon_identity
        home_lock = self.runtime.daemon_home_lock
        if identity is None or home_lock is None or not home_lock.is_acquired or home_lock.identity is not identity:
            raise AdapterRunError("daemon_identity_mismatch", stage, HTTPStatus.SERVICE_UNAVAILABLE)
        if (
            document["daemon_instance_id"] != identity.instance_id
            or document["daemon_generation"] != identity.generation
        ):
            raise AdapterRunError("daemon_identity_mismatch", stage, HTTPStatus.PRECONDITION_FAILED)
        registry = build_runtime_adapter_registry_document(
            identity,
            remote_custom_adapter_execution_enabled=self.runtime.allow_remote_custom_adapters,
        )
        if document["registry_revision"] != registry["registry_revision"]:
            raise AdapterRunError("registry_revision_mismatch", stage, HTTPStatus.PRECONDITION_FAILED)

        target = document["target"]
        object_ref = document["object"]
        if target["kind"] == "local":
            try:
                record = self.runtime.store.get_object_version(object_ref["selected_version_id"], include_yaml=False)
            except KeyError as exc:
                raise AdapterRunError("object_changed", stage, HTTPStatus.PRECONDITION_FAILED) from exc
            self._validate_object_record(document, record, stage=stage, current_record=record)
            function = object_ref["function"]
            signature = build_signature(record, function=function)
            availability_reason = "installed_distribution_metadata"
        else:
            server = self._remote_server()
            try:
                record = server.get_object(
                    object_ref["object_name"],
                    version=object_ref["selected_version"],
                    include_yaml=False,
                    owner_id=object_ref["owner_id"],
                    library=object_ref["library"],
                )
                current = server.get_object(
                    object_ref["object_name"],
                    include_yaml=False,
                    owner_id=object_ref["owner_id"],
                    library=object_ref["library"],
                )
            except Exception as exc:
                raise AdapterRunError(
                    "target_unavailable", stage, HTTPStatus.SERVICE_UNAVAILABLE, retryable=True
                ) from exc
            self._validate_object_record(document, record, stage=stage, current_record=current)
            function = object_ref["function"]
            signature = build_signature(record, function=function)
            if remote_preflight:
                self._validate_remote_target(document, server, stage=stage)
            availability_reason = "target_preflight_verified"

        fields = semantic_signature_fields(signature, function=function)
        evidence = document["signature_evidence"]
        if fields != evidence["semantic_fields"] or semantic_signature_hash(fields) != evidence["semantic_hash"]:
            raise AdapterRunError("signature_mismatch", stage, HTTPStatus.PRECONDITION_FAILED)

        registry_rows = {row["id"]: row for row in registry["adapters"]}
        library_records = self._resolve_library_adapter_records(document, stage=stage)
        port_semantic_types = {
            (target.direction, target.canonical): target.semantic_type
            for target in (
                *port_catalog_from_signature(signature)["input"],
                *port_catalog_from_signature(signature)["output"],
            )
        }
        bindings = [
            *(
                {
                    "direction": "input",
                    "port": item["port"],
                    "adapter_id": item["adapter_id"],
                    "format_tag": item["format_tag"],
                }
                for item in document["inputs"]
            ),
            *({"direction": "output", **item} for item in document["outputs"]),
        ]
        semantic_bindings: list[dict[str, Any]] = []
        for binding in bindings:
            port_semantic_type = port_semantic_types.get((binding["direction"], binding["port"]))
            library_record = library_records.get((binding["direction"], binding["port"]))
            if library_record is not None:
                directions = library_record.get("directions")
                effective_policy = library_record.get("effective_policy")
                policy_key = "local_custom" if target["kind"] == "local" else "remote_custom"
                if (
                    not isinstance(directions, list)
                    or binding["direction"] not in directions
                    or binding["format_tag"] != library_record.get("format_tag")
                    or not isinstance(effective_policy, Mapping)
                    or effective_policy.get(policy_key) != "allow"
                ):
                    raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
                adapter_type = str(library_record.get("semantic_type") or "")
                category_value = library_record.get("semantic_category")
                category = category_value if isinstance(category_value, str) else None
                state = library_adapter_semantic_advisory(
                    port_semantic_type,
                    adapter_type,
                    category,
                )
                semantic_bindings.append(
                    {
                        "direction": binding["direction"],
                        "port": binding["port"],
                        "adapter_id": str(library_record["adapter_id"]),
                        "port_semantic_type": port_semantic_type,
                        "adapter_semantic_type": adapter_type,
                        "adapter_semantic_category": category,
                        "state": state,
                        "acknowledged": False,
                        "acknowledgement_source": None,
                    }
                )
                continue
            row = registry_rows.get(binding["adapter_id"])
            if (
                row is None
                or row["format"]["tag"] != binding["format_tag"]
                or binding["direction"] not in row["directions"]
            ):
                raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
            if target["kind"] == "local" and row["local_availability"]["state"] != "available":
                raise AdapterRunError("adapter_unavailable", stage, HTTPStatus.FAILED_DEPENDENCY)
            semantic_types = row.get("semantic_types")
            if not isinstance(semantic_types, list) or len(semantic_types) != 1:
                raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
            adapter_type = str(semantic_types[0])
            state = runtime_adapter_semantic_state(
                port_semantic_type,
                adapter_type,
                adapter_id=str(binding["adapter_id"]),
            )
            semantic_bindings.append(
                {
                    "direction": binding["direction"],
                    "port": binding["port"],
                    "adapter_id": binding["adapter_id"],
                    "port_semantic_type": port_semantic_type,
                    "adapter_semantic_type": adapter_type,
                    "adapter_semantic_category": runtime_adapter_semantic_category(str(binding["adapter_id"])),
                    "state": state,
                    "acknowledged": False,
                    "acknowledgement_source": None,
                }
            )
        semantic_advisories = _validate_browser_semantic_advisories(
            document,
            semantic_bindings,
            stage=stage,
        )
        probe = _synthetic_admission(document)
        self._wire_document(
            probe,
            signature,
            include_bodies=False,
            stage=stage,
            library_records=library_records,
        )
        return {
            "signature": signature,
            "record": record,
            "bindings": bindings,
            "availability_reason": availability_reason,
            "library_records": library_records,
            "runtime_adapter_semantic_advisories": semantic_advisories,
        }

    def _resolve_library_adapter_records(
        self,
        document: Mapping[str, Any],
        *,
        stage: str,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        refs = document.get(_LIBRARY_REFS_FIELD)
        if not isinstance(refs, Mapping):
            return {}
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for binding in refs.get("bindings") or []:
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
            try:
                if document["target"]["kind"] == "local":
                    try:
                        record = self.runtime.store.resolve_library_adapter_ref(
                            ref,
                            include_source=True,
                        )
                    except KeyError:
                        credentials = self.runtime._require_live_server_channel_credentials()
                        response = self.runtime._server_client_for_credentials(
                            credentials
                        ).resolve_local_library_adapter_source(
                            ref,
                            target_machine_id=str(credentials["machine_id"]),
                        )
                        if (
                            not isinstance(response, Mapping)
                            or response.get("schema") != "spl.library-adapter-local-execution-source"
                            or response.get("schema_version") != 1
                            or response.get("execution_target") != "local"
                            or response.get("target_machine_id") != credentials["machine_id"]
                            or not isinstance(response.get("adapter"), Mapping)
                        ):
                            raise KeyError("Library Adapter source response is invalid")
                        record = dict(response["adapter"])
                else:
                    record = self._remote_server().get_library_adapter_version(
                        ref["owner"],
                        ref["library"],
                        ref["adapter_id"],
                        ref["adapter_version_id"],
                        include_source=False,
                    )
            except Exception as exc:
                raise AdapterRunError(
                    "adapter_unavailable",
                    stage,
                    HTTPStatus.NOT_FOUND,
                ) from exc
            if any(record.get(key) != value for key, value in ref.items()):
                raise AdapterRunError("adapter_unavailable", stage, HTTPStatus.NOT_FOUND)
            result[(binding["direction"], binding["port"])] = dict(record)
        return result

    def _validate_object_record(
        self,
        document: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        stage: str,
        current_record: Mapping[str, Any],
    ) -> None:
        expected = document["object"]
        observed_content = str(record.get("content_hash") or "")
        if not observed_content.startswith("sha256:"):
            observed_content = "sha256:" + observed_content
        observed_library = record.get("library")
        if isinstance(observed_library, Mapping):
            observed_library = observed_library.get("slug")
        current_version_id = current_record.get("current_version_id") or current_record.get("version_id")
        observed = (
            str(record.get("owner_id") or ""),
            str(observed_library or "default"),
            str(record.get("name") or ""),
            str(record.get("id") or ""),
            str(current_version_id or ""),
            str(record.get("version_id") or ""),
            record.get("version"),
            observed_content,
        )
        wanted = (
            expected["owner_id"],
            expected["library"],
            expected["object_name"],
            expected["object_id"],
            expected["expected_current_version_id"],
            expected["selected_version_id"],
            expected["selected_version"],
            document["content_hash"],
        )
        if observed != wanted:
            raise AdapterRunError("object_changed", stage, HTTPStatus.PRECONDITION_FAILED)

    def _validate_remote_target(self, document: Mapping[str, Any], server: Any, *, stage: str) -> None:
        try:
            semantic = document.get(_SEMANTIC_ADVISORIES_FIELD)
            require_semantic_override = bool(
                isinstance(semantic, Mapping)
                and any(
                    isinstance(item, Mapping) and item.get("state") != "recommended"
                    for item in semantic.get("bindings") or []
                )
            )
            self.runtime._require_server_runtime_port_adapter_capability(
                server,
                require_library_refs=_LIBRARY_REFS_FIELD in document,
                require_semantic_override=require_semantic_override,
            )
            machine_id = document["target"]["machine_id"]
            machine = next((row for row in server.list_machines() if row.get("id") == machine_id), None)
            capability = (
                machine.get("capabilities", {}).get("spl.remote_run.runtime_port_adapters.v1")
                if isinstance(machine, Mapping)
                else None
            )
            custom = capability.get("custom_adapter_execution") if isinstance(capability, Mapping) else None
            if (
                not isinstance(capability, Mapping)
                or set(capability) != {"schema_version", "transport", "custom_adapter_execution"}
                or capability.get("schema_version") != 1
                or capability.get("transport") is not True
                or not isinstance(custom, Mapping)
                or set(custom) != {"implemented", "enabled"}
                or type(custom.get("implemented")) is not bool
                or type(custom.get("enabled")) is not bool
            ):
                raise AdapterRunError("target_unavailable", stage, HTTPStatus.FAILED_DEPENDENCY)
            if require_semantic_override:
                semantic_capability = (
                    machine.get("capabilities", {}).get(RUNTIME_ADAPTER_SEMANTIC_OVERRIDE_CAPABILITY)
                    if isinstance(machine, Mapping) and isinstance(machine.get("capabilities"), Mapping)
                    else None
                )
                if semantic_capability != {"schema_version": 1, "advisory": True}:
                    raise AdapterRunError("target_unavailable", stage, HTTPStatus.FAILED_DEPENDENCY)
            request = _central_preflight_request(document)
            preflight = server.preflight_remote_run(request)
            validate_central_preflight_response(preflight, expected_request=request)
            if preflight.get("can_queue") is not True:
                raise AdapterRunError("target_unavailable", stage, HTTPStatus.CONFLICT)
        except AdapterRunError:
            raise
        except Exception as exc:
            raise AdapterRunError("target_unavailable", stage, HTTPStatus.SERVICE_UNAVAILABLE, retryable=True) from exc

    def _wire_document(
        self,
        admission: _Admission,
        signature: Mapping[str, Any],
        *,
        include_bodies: bool,
        stage: str,
        library_records: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        document = admission.document
        semantic_document = document.get(_SEMANTIC_ADVISORIES_FIELD)
        explicit_semantic_bindings = {
            (str(item.get("direction")), str(item.get("port")))
            for item in (semantic_document or {}).get("bindings", [])
            if isinstance(item, Mapping)
        }
        kwargs: dict[str, Any] = {}
        for item in document["inputs"]:
            external = _external_name(signature, item["port"])
            if item["kind"] == "omitted":
                continue
            candidate = item["value"] if item["kind"] == "inline_json" else None
            if external in kwargs and kwargs[external] != candidate:
                raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
            kwargs[external] = candidate
        try:
            template = implicit_json_wire_document(
                # This call supplies only authoritative argument/result-path
                # topology. Explicit non-JSON bindings must not be rejected by
                # the temporary JSON descriptor before their selected adapter
                # is installed and the real signature is validated below.
                signature=_json_template_signature(signature),
                args=None,
                kwargs=kwargs,
                output=document["output_selector"],
            )
        except RuntimePortAdapterContractError as exc:
            raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST) from exc
        template_bindings = {(row["direction"], row["port"]): row for row in template["bindings"]}
        catalog = port_catalog_from_signature(signature)
        semantic_types: dict[tuple[str, str], str | None] = {
            (target.direction, target.canonical): target.semantic_type
            for target in (*catalog["input"], *catalog["output"])
        }
        library_records = library_records or {}
        placeholder_adapters: dict[tuple[str, str], Adapter] = {}
        for identity, record in library_records.items():
            semantic_type = semantic_types.get(identity) or record.get("semantic_type")
            if not isinstance(semantic_type, str) or not semantic_type:
                raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
            declared_tag = record.get("format_tag")
            tag = (
                declared_tag
                if isinstance(declared_tag, str) and declared_tag
                else f"spl.library.{record['content_hash']}.v1"
            )
            placeholder_adapters[identity] = Adapter(
                key=f"{semantic_type}@{tag}",
                save=browser_library_placeholder_save,
                load=browser_library_placeholder_load,
                py_type=None,
                format=tag,
                distributions=(),
            )
        placeholder_bundle = None
        placeholder_symbols: dict[int, tuple[str, str]] = {}
        if placeholder_adapters:
            placeholder_bundle, placeholder_symbols = build_custom_bundle(list(placeholder_adapters.values()))

        def selected_descriptor(direction: str, item: Mapping[str, Any]) -> dict[str, Any]:
            placeholder = placeholder_adapters.get((direction, str(item["port"])))
            if placeholder is None:
                _, descriptor = built_in_descriptor(str(item["adapter_id"]))
                if descriptor["format_tag"] != item["format_tag"]:
                    raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
                return descriptor
            assert placeholder_bundle is not None
            save_symbol, load_symbol = placeholder_symbols[id(placeholder)]
            return adapter_descriptor(
                placeholder,
                adapter_id=None,
                custom_bundle_sha256=str(placeholder_bundle["sha256"]),
                save_symbol=save_symbol,
                load_symbol=load_symbol,
            )

        bindings = []
        wire_inputs = []
        for item in document["inputs"]:
            base = template_bindings.get(("input", item["port"]))
            if base is None:
                raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
            descriptor = selected_descriptor("input", item)
            artifact = item["kind"] == "staged"
            bindings.append(
                {
                    **base,
                    "semantic_type": semantic_types[("input", item["port"])],
                    "adapter": descriptor,
                    "resolution_source": (
                        "run_override" if ("input", item["port"]) in explicit_semantic_bindings else "system_default"
                    ),
                    "transport": "artifact" if artifact else "inline_json",
                    "input_name": item["name"] if artifact else None,
                }
            )
            if artifact:
                content = None
                staged_name = item["name"]
                if include_bodies:
                    body = _read_verified_file(
                        admission.directory / (item["name"] + ".body"),
                        size=item["size"],
                        sha256=item["sha256"],
                    )
                    content = base64.b64encode(body).decode("ascii")
                    staged_name = None
                wire_inputs.append(
                    {
                        "name": item["name"],
                        "port": item["port"],
                        "size": item["size"],
                        "sha256": item["sha256"],
                        "format_tag": descriptor["format_tag"],
                        "semantic_type": semantic_types[("input", item["port"])],
                        "adapter_id": descriptor["id"],
                        "media_type": (None if ("input", item["port"]) in placeholder_adapters else item["media_type"]),
                        "content_base64": content,
                        "staged_name": staged_name,
                    }
                )
        for item in document["outputs"]:
            base = template_bindings.get(("output", item["port"]))
            if base is None:
                raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
            descriptor = selected_descriptor("output", item)
            bindings.append(
                {
                    **base,
                    "semantic_type": semantic_types[("output", item["port"])],
                    "adapter": descriptor,
                    "resolution_source": (
                        "run_override" if ("output", item["port"]) in explicit_semantic_bindings else "system_default"
                    ),
                    "transport": (
                        "inline_json"
                        if item["adapter_id"] == JSON and ("output", item["port"]) not in placeholder_adapters
                        else "artifact"
                    ),
                }
            )
        wire_bundle = None
        if placeholder_bundle is not None:
            wire_bundle = {
                "name": placeholder_bundle["name"],
                "size": placeholder_bundle["size"],
                "sha256": placeholder_bundle["sha256"],
                "functions": placeholder_bundle["functions"],
                "content_base64": (
                    base64.b64encode(placeholder_bundle["source_bytes"]).decode("ascii") if include_bodies else None
                ),
                "staged_name": None if include_bodies else placeholder_bundle["name"],
            }
        raw = {
            "schema_version": 1,
            "bindings": bindings,
            "inputs": wire_inputs,
            "custom_bundle": wire_bundle,
        }
        try:
            wire = validate_wire_document_against_signature(
                raw,
                signature=signature,
                args=None,
                kwargs=kwargs,
                output=document["output_selector"],
                allow_content=include_bodies,
            )
        except RuntimePortAdapterContractError as exc:
            raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST) from exc
        return wire, kwargs

    def _finalize_local(self, admission: _Admission, wire: dict[str, Any], kwargs: dict[str, Any]) -> dict[str, Any]:
        document = admission.document
        with self.runtime.store._storage._lock:
            authoritative = self._validate_current(
                document,
                stage="finalize",
                remote_preflight=False,
            )
            source_by_version = {
                str(record["adapter_version_id"]): dict(record) for record in authoritative["library_records"].values()
            }
            # A previous daemon process may have committed the exact Run but
            # exited before persisting this broker's safe receipt binding.
            # Reconcile the immutable Run marker while holding the same store
            # lock that fences creation, so a retry can never replay mutation.
            self._reconcile_local_unknown(admission)
            if admission.state == "finalized" and admission.run_id is not None:
                return cast(dict[str, Any], self.runtime.store.get_run(admission.run_id))
            admission.state = "outcome_unknown"
            return cast(
                dict[str, Any],
                self.runtime.start_run(
                    document["object"]["object_name"],
                    kwargs=kwargs,
                    output=document["output_selector"],
                    timeout_seconds=_timeout_seconds(document["timeout_ms"]),
                    object_version_id=document["object"]["selected_version_id"],
                    function=document["object"]["function"],
                    object_owner_id=document["object"]["owner_id"],
                    library=document["object"]["library"],
                    source="local",
                    keep=_keep_policy(document["retention"]),
                    runtime_port_adapters=wire,
                    runtime_library_adapter_refs=document.get(_LIBRARY_REFS_FIELD),
                    runtime_adapter_semantic_advisories=document.get(_SEMANTIC_ADVISORIES_FIELD),
                    runtime_library_adapter_sources=[source_by_version[key] for key in sorted(source_by_version)],
                    adapter_policy=document["adapter_policy"],
                    _browser_adapter_admission={
                        "schema_version": 1,
                        "request_id": admission.request_id,
                        "request_digest_sha256": admission.request_digest_sha256,
                        "target": dict(document["target"]),
                    },
                ),
            )

    def _finalize_remote(self, admission: _Admission, wire: dict[str, Any], kwargs: dict[str, Any]) -> dict[str, Any]:
        document = admission.document
        return cast(
            dict[str, Any],
            self.runtime.start_remote_run(
                document["object"]["object_name"],
                target_machine=document["target"]["machine_id"],
                object_owner_id=document["object"]["owner_id"],
                library=document["object"]["library"],
                kwargs=kwargs,
                output=document["output_selector"],
                timeout_seconds=_timeout_seconds(document["timeout_ms"]),
                version=document["object"]["selected_version"],
                object_version_id=document["object"]["selected_version_id"],
                function=document["object"]["function"],
                correlation_id=document["request_id"],
                offline_policy=document["target"]["offline_policy"],
                runtime_port_adapters=wire,
                runtime_library_adapter_refs=document.get(_LIBRARY_REFS_FIELD),
                runtime_adapter_semantic_advisories=document.get(_SEMANTIC_ADVISORIES_FIELD),
                adapter_policy=document["adapter_policy"],
            ),
        )

    def _commit_finalized(
        self,
        admission: _Admission,
        state: Mapping[str, Any],
        fingerprint: str,
    ) -> None:
        run_id = str(state.get("id") or "")
        if not _SAFE_ID.fullmatch(run_id):
            raise AdapterRunError("finalize_failed", "finalize", HTTPStatus.SERVICE_UNAVAILABLE)
        admission.run_id = run_id
        self._run_admissions[run_id] = admission
        admission.receipt = self._receipt(admission, state, fingerprint)
        self._persist_finalized_binding(admission)
        admission.document = {
            "target": dict(admission.document["target"]),
            "inputs": [dict(item) for item in admission.document["inputs"] if item["kind"] == "staged"],
            "outputs": [dict(item) for item in admission.document["outputs"]],
            "output_selector": admission.document["output_selector"],
            **(
                {_SEMANTIC_ADVISORIES_FIELD: admission.document[_SEMANTIC_ADVISORIES_FIELD]}
                if _SEMANTIC_ADVISORIES_FIELD in admission.document
                else {}
            ),
        }
        admission.state = "finalized"
        self._cleanup_directory(admission.directory)
        self._evict_finalized_bindings(exclude_run_id=run_id)

    def _reconcile_local_unknown(self, admission: _Admission) -> None:
        """Finish receipt projection only when the already-created Run is known."""

        if admission.run_id is None:
            matches = []
            try:
                for candidate in self.runtime.store.list_runs():
                    manifest = candidate.get("manifest")
                    marker = manifest.get("browser_adapter_admission") if isinstance(manifest, Mapping) else None
                    if not isinstance(marker, Mapping):
                        continue
                    if (
                        marker.get("schema_version") == 1
                        and marker.get("request_id") == admission.request_id
                        and marker.get("request_digest_sha256") == admission.request_digest_sha256
                        and marker.get("target") == admission.document["target"]
                    ):
                        matches.append(candidate)
            except Exception:
                return
            if len(matches) != 1:
                return
            admission.run_id = str(matches[0]["id"])
            self._run_admissions[admission.run_id] = admission
        try:
            state = self.runtime.store.get_run(admission.run_id)
            manifest = state.get("manifest")
            runtime_section = manifest.get("runtime_port_adapters") if isinstance(manifest, Mapping) else None
            manifest_fingerprint = (
                runtime_section.get("fingerprint_sha256") if isinstance(runtime_section, Mapping) else None
            )
            fingerprint = admission.runtime_fingerprint or manifest_fingerprint
            if (
                not isinstance(fingerprint, str)
                or not _RAW_HASH.fullmatch(fingerprint)
                or manifest_fingerprint != fingerprint
            ):
                return
            self._commit_finalized(admission, state, fingerprint)
        except Exception:
            return

    def _receipt(self, admission: _Admission, state: Mapping[str, Any], fingerprint: str) -> dict[str, Any]:
        document = admission.document
        obj = document["object"]
        receipt = {
            "schema": RUN_RECEIPT_SCHEMA,
            "schema_version": 1,
            "request_id": admission.request_id,
            "accepted_http_status": 202,
            "run_id": str(state["id"]),
            "initial_state": _canonical_run_state(state.get("status")),
            "target": dict(document["target"]),
            "created_at": _canonical_upstream_time(state.get("created_at")),
            "object": {
                "owner_id": obj["owner_id"],
                "library": obj["library"],
                "object_name": obj["object_name"],
                "object_id": obj["object_id"],
                "current_version_id": obj["expected_current_version_id"],
                "version_id": obj["selected_version_id"],
                "version": obj["selected_version"],
                "function": obj["function"],
                "origin": obj["origin"],
                "content_hash": document["content_hash"],
            },
            "runtime_port_adapters_digest_sha256": fingerprint,
        }
        if _SEMANTIC_ADVISORIES_FIELD in document:
            receipt[_SEMANTIC_ADVISORIES_FIELD] = document[_SEMANTIC_ADVISORIES_FIELD]
        return receipt

    def _reconcile_remote_unknown(self, admission: _Admission) -> None:
        try:
            server = self._remote_server()
            current = server.get_remote_run_admission(_remote_admission_id(admission.request_id))
            state = current.get("state")
            if state == "finalized" and isinstance(current.get("run_id"), str):
                run = server.get_remote_run(current["run_id"])
                fingerprint = admission.runtime_fingerprint
                if not isinstance(fingerprint, str) or not _RAW_HASH.fullmatch(fingerprint):
                    return
                self._commit_finalized(admission, run, fingerprint)
            elif state in {"cancelled", "expired"}:
                admission.state = cast(Any, state)
                self._cleanup_directory(admission.directory)
        except Exception:
            return

    def _terminal_outputs(
        self,
        admission: _Admission,
        state: Mapping[str, Any],
        *,
        target: Literal["local", "remote"],
        successful: bool,
    ) -> list[dict[str, Any]]:
        result = state.get("result")
        result_mapping = result if isinstance(result, Mapping) else {}
        artifact_records = _runtime_output_records(state, result_mapping, target=target)
        records_by_port = {str(item.get("port")): item for item in artifact_records if isinstance(item, Mapping)}
        artifacts_available = self._available_artifacts(state, target=target)
        outputs = []
        for selected in admission.document["outputs"]:
            port = selected["port"]
            if not successful:
                outputs.append(_unavailable_output_projection(cast(str, admission.run_id), selected))
                continue
            if selected["adapter_id"] == JSON:
                value = _result_value_for_port(result_mapping, port, admission.document["output_selector"])
                try:
                    if not m_json_contract.compact_json_fits(value, MAX_PREVIEW_BYTES):
                        raise ValueError("browser JSON result exceeds the safe preview bound")
                    encoded = m_json_contract.dumps(
                        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8")
                except (RecursionError, TypeError, ValueError):
                    outputs.append(_unavailable_output_projection(cast(str, admission.run_id), selected))
                    continue
                outputs.append(
                    _output_projection(
                        run_id=cast(str, admission.run_id),
                        port=port,
                        adapter_id=selected["adapter_id"],
                        format_tag=selected["format_tag"],
                        logical_name=_safe_output_name(port, ".json"),
                        media_type="application/json",
                        size=len(encoded),
                        sha256=hashlib.sha256(encoded).hexdigest(),
                        availability="retained",
                        reason=None,
                        preview={
                            "kind": "json",
                            "available": True,
                            "text": None,
                            "json": value,
                            "truncated": False,
                        },
                        download_handle=None,
                    )
                )
                continue
            record = records_by_port.get(port)
            if record is None:
                outputs.append(_unavailable_output_projection(cast(str, admission.run_id), selected))
                continue
            artifact_name = str(record.get("artifact_name") or record.get("name") or "")
            raw_size = record.get("size")
            digest = str(record.get("sha256") or "")
            raw_media_type = record.get("media_type")
            library_output = str(selected["adapter_id"]).startswith("library:")
            if library_output:
                expected_media_type = None
                media_type = raw_media_type if isinstance(raw_media_type, str) else None
            else:
                _, descriptor = built_in_descriptor(selected["adapter_id"])
                expected_media_type = descriptor["presentation"]["media_type"]
                media_type = str(raw_media_type or "")
            if (
                not _SAFE_FILE.fullmatch(artifact_name)
                or artifact_name in {".", ".."}
                or type(raw_size) is not int
                or not 0 <= raw_size <= MAX_ARTIFACT_DOWNLOAD_BYTES
                or not _RAW_HASH.fullmatch(digest)
                or (media_type is not None and not _MEDIA_TYPE.fullmatch(media_type))
                or record.get("adapter_id") != selected["adapter_id"]
                or record.get("format_tag") != selected["format_tag"]
                or (not library_output and expected_media_type is not None and media_type != expected_media_type)
                or artifacts_available is None
            ):
                outputs.append(_unavailable_output_projection(cast(str, admission.run_id), selected))
                continue
            size = raw_size
            retained = artifact_name in artifacts_available
            availability = "retained" if retained else "expired"
            grant = _ArtifactGrant(
                run_id=cast(str, admission.run_id),
                target=target,
                artifact_name=artifact_name,
                logical_name=artifact_name,
                media_type=media_type or "application/octet-stream",
                size=size,
                sha256=digest,
            )
            handle = self._artifact_handle(grant) if retained else None
            if handle is not None:
                self._artifact_grants[handle] = grant
                while len(self._artifact_grants) > MAX_ARTIFACT_GRANTS:
                    self._artifact_grants.pop(next(iter(self._artifact_grants)))
            preview = self._artifact_preview(grant) if retained else _empty_preview(media_type)
            outputs.append(
                _output_projection(
                    run_id=cast(str, admission.run_id),
                    port=port,
                    adapter_id=selected["adapter_id"],
                    format_tag=selected["format_tag"],
                    logical_name=artifact_name,
                    media_type=media_type,
                    size=size,
                    sha256=digest,
                    availability=availability,
                    reason=None if retained else "artifact_expired",
                    preview=preview,
                    download_handle=handle,
                )
            )
        return outputs

    def _available_artifacts(self, state: Mapping[str, Any], *, target: Literal["local", "remote"]) -> set[str] | None:
        if target == "local":
            raw_dir = state.get("artifacts_dir")
            directory = Path(str(raw_dir)) if raw_dir else None
            if directory is None or not directory.is_dir():
                return set()
            try:
                return {entry.name for entry in directory.iterdir() if entry.is_file() and not entry.is_symlink()}
            except OSError:
                return None
        try:
            records = self._remote_server().list_artifacts(str(state["id"]))
        except Exception:
            return None
        return {str(record.get("name")) for record in records if isinstance(record, Mapping)}

    def _artifact_preview(self, grant: _ArtifactGrant) -> dict[str, Any]:
        if grant.media_type == "image/png":
            available = grant.size <= MAX_PNG_PREVIEW_BYTES
            return {
                "kind": "png",
                "available": available,
                "text": None,
                "json": None,
                "truncated": not available,
            }
        if grant.media_type not in {"text/plain", "text/csv"}:
            return _empty_preview(grant.media_type)
        try:
            if grant.target == "local":
                state = self.runtime.store.get_run(grant.run_id)
                raw_dir = state.get("artifacts_dir")
                if not raw_dir:
                    return _empty_preview(grant.media_type)
                path = Path(str(raw_dir)) / grant.artifact_name
                data = _read_verified_prefix(path, size=grant.size, sha256=grant.sha256)
            else:
                if grant.size > MAX_PREVIEW_BYTES:
                    return {
                        "kind": "csv" if grant.media_type == "text/csv" else "text",
                        "available": False,
                        "text": None,
                        "json": None,
                        "truncated": True,
                    }
                server = self._remote_server()
                read_verified = getattr(server, "artifact_bytes_verified", None)
                if not callable(read_verified):
                    return _empty_preview(grant.media_type)
                data = read_verified(
                    grant.run_id,
                    grant.artifact_name,
                    size=grant.size,
                    sha256=grant.sha256,
                )
            text = data.decode("utf-8")
        except Exception:
            return _empty_preview(grant.media_type)
        return {
            "kind": "csv" if grant.media_type == "text/csv" else "text",
            "available": True,
            "text": text,
            "json": None,
            "truncated": grant.size > MAX_PREVIEW_BYTES,
        }

    def _artifact_handle(self, grant: _ArtifactGrant) -> str:
        payload = "\0".join((grant.run_id, grant.target, grant.artifact_name, str(grant.size), grant.sha256)).encode(
            "utf-8"
        )
        return "ar1_" + hmac.new(self._handle_key, payload, hashlib.sha256).hexdigest()

    def _remote_server(self) -> Any:
        credentials = self.runtime._require_live_server_channel_credentials()
        return self.runtime._server_client_for_credentials(credentials)

    def _require_admission(self, request_id: str, *, stage: str) -> _Admission:
        request_id = _identifier(request_id, stage)
        admission = self._admissions.get(request_id)
        if admission is None:
            raise AdapterRunError("admission_not_found", stage, HTTPStatus.NOT_FOUND)
        return admission

    def _expire_locked(self) -> None:
        now = datetime.now(UTC)
        self._preflights = {
            key: value for key, value in self._preflights.items() if _parse_timestamp(value.expires_at) > now
        }
        for admission in self._admissions.values():
            if admission.state in {"staging", "ready"} and _parse_timestamp(admission.expires_at) <= now:
                admission.state = "expired"
                self._cleanup_directory(admission.directory)

    def _projection(self, admission: _Admission) -> dict[str, Any]:
        projection = {
            "schema": ADMISSION_SCHEMA,
            "schema_version": 1,
            "request_id": admission.request_id,
            "request_digest_sha256": admission.request_digest_sha256,
            "state": admission.state,
            "created_at": admission.created_at,
            "expires_at": admission.expires_at,
            "target": dict(admission.document["target"]),
            "items": [
                {
                    "name": item["name"],
                    "port": item["port"],
                    "logical_name": item["logical_name"],
                    "media_type": item["media_type"],
                    "size": item["size"],
                    "sha256": item["sha256"],
                    "uploaded": item["name"] in admission.uploaded,
                }
                for item in admission.document["inputs"]
                if item["kind"] == "staged"
            ],
            "run_id": admission.run_id,
            "receipt": admission.receipt,
        }
        semantic = admission.document.get(_SEMANTIC_ADVISORIES_FIELD)
        if isinstance(semantic, dict):
            projection[_SEMANTIC_ADVISORIES_FIELD] = semantic
        return projection

    def _persist_finalized_binding(self, admission: _Admission) -> None:
        """Persist only the safe result projection binding, never input bodies."""

        if admission.run_id is None or admission.receipt is None:
            raise RuntimeError("finalized adapter Run binding is incomplete")
        staged_inputs = [dict(item) for item in admission.document["inputs"] if item["kind"] == "staged"]
        record = {
            "schema": DURABLE_BINDING_SCHEMA,
            "schema_version": 1,
            "request_id": admission.request_id,
            "request_digest_sha256": admission.request_digest_sha256,
            "created_at": admission.created_at,
            "expires_at": admission.expires_at,
            "run_id": admission.run_id,
            "target": dict(admission.document["target"]),
            "staged_inputs": staged_inputs,
            "outputs": [dict(item) for item in admission.document["outputs"]],
            "output_selector": admission.document["output_selector"],
            "receipt": admission.receipt,
        }
        if _contains_path(record) or _contains_forbidden_key(record):
            raise RuntimeError("finalized adapter Run binding is not browser-safe")
        destination = self.finalized_root / (_durable_binding_name(admission.run_id) + ".json")
        _atomic_private_json(destination, record)
        self._prune_durable_bindings()

    def _load_durable_bindings(self) -> None:
        """Restore bounded safe result bindings after a daemon restart."""

        for path in sorted(self.finalized_root.glob("*.json")):
            try:
                if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_JSON_BODY_BYTES:
                    continue
                raw = path.read_bytes()
                value = json.loads(raw.decode("utf-8"), object_pairs_hook=_closed_object)
                admission = self._admission_from_durable(value)
            except Exception:
                continue
            if (
                admission.request_id in self._admissions
                or admission.run_id is None
                or admission.run_id in self._run_admissions
            ):
                continue
            self._admissions[admission.request_id] = admission
            self._run_admissions[admission.run_id] = admission
        self._evict_finalized_bindings()

    def _load_durable_binding_for_run(self, run_id: str) -> _Admission | None:
        if not isinstance(run_id, str) or not _SAFE_ID.fullmatch(run_id):
            return None
        path = self.finalized_root / (_durable_binding_name(run_id) + ".json")
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_JSON_BODY_BYTES:
                return None
            value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_closed_object)
            admission = self._admission_from_durable(value)
        except Exception:
            return None
        if admission.run_id != run_id:
            return None
        self._admissions[admission.request_id] = admission
        self._run_admissions[run_id] = admission
        self._evict_finalized_bindings(exclude_run_id=run_id)
        return admission

    def _evict_finalized_bindings(self, *, exclude_run_id: str | None = None) -> None:
        finalized = sorted(
            (
                admission
                for admission in self._run_admissions.values()
                if admission.state == "finalized" and admission.run_id != exclude_run_id
            ),
            key=lambda admission: (admission.created_at, admission.run_id or ""),
        )
        excess = max(0, len(self._run_admissions) - MAX_IN_MEMORY_RUN_BINDINGS)
        for admission in finalized[:excess]:
            if admission.run_id is not None:
                self._run_admissions.pop(admission.run_id, None)
            self._admissions.pop(admission.request_id, None)
        if excess:
            retained_runs = set(self._run_admissions)
            self._artifact_grants = {
                handle: grant for handle, grant in self._artifact_grants.items() if grant.run_id in retained_runs
            }

    def _admission_from_durable(self, value: Any) -> _Admission:
        record = _mapping(value, "result")
        _exact(
            record,
            {
                "schema",
                "schema_version",
                "request_id",
                "request_digest_sha256",
                "created_at",
                "expires_at",
                "run_id",
                "target",
                "staged_inputs",
                "outputs",
                "output_selector",
                "receipt",
            },
            "result",
        )
        if record["schema"] != DURABLE_BINDING_SCHEMA or record["schema_version"] != 1:
            _invalid("result")
        request_id = _identifier(record["request_id"], "result")
        request_digest = _raw_hash(record["request_digest_sha256"], "result")
        created_at = _timestamp(record["created_at"], "result")
        expires_at = _timestamp(record["expires_at"], "result")
        run_id = _identifier(record["run_id"], "result")
        target = _normalize_target(record["target"], stage="result")
        staged_inputs = _normalize_inputs(record["staged_inputs"], stage="result")
        if any(item["kind"] != "staged" for item in staged_inputs):
            _invalid("result")
        outputs = _normalize_outputs(record["outputs"], stage="result")
        selector = record["output_selector"]
        if selector is not None:
            selector = _identifier(selector, "result")
        receipt = _normalize_durable_receipt(record["receipt"])
        if (
            receipt.get("schema") != RUN_RECEIPT_SCHEMA
            or receipt.get("schema_version") != 1
            or receipt.get("request_id") != request_id
            or receipt.get("run_id") != run_id
            or receipt.get("target") != target
        ):
            _invalid("result")
        document = {
            "target": target,
            "inputs": staged_inputs,
            "outputs": outputs,
            "output_selector": selector,
        }
        semantic = receipt.get(_SEMANTIC_ADVISORIES_FIELD)
        if isinstance(semantic, dict):
            document[_SEMANTIC_ADVISORIES_FIELD] = semantic
        if _contains_path(record) or _contains_forbidden_key(record):
            _invalid("result")
        return _Admission(
            request_id=request_id,
            request_digest_sha256=request_digest,
            created_at=created_at,
            expires_at=expires_at,
            document=document,
            state="finalized",
            directory=self.staging_root / ("restored-" + _durable_binding_name(run_id)),
            uploaded={item["name"] for item in staged_inputs},
            run_id=run_id,
            receipt=receipt,
        )

    def _prune_durable_bindings(self) -> None:
        files = [path for path in self.finalized_root.glob("*.json") if path.is_file() and not path.is_symlink()]
        files.sort(key=lambda path: (path.stat().st_mtime_ns, path.name), reverse=True)
        retained_bytes = 0
        for index, path in enumerate(files):
            try:
                size = path.stat().st_size
            except OSError:
                continue
            if (
                index >= MAX_DURABLE_RUN_BINDINGS
                or size > MAX_JSON_BODY_BYTES
                or retained_bytes + size > MAX_DURABLE_BINDING_TOTAL_BYTES
            ):
                path.unlink(missing_ok=True)
            else:
                retained_bytes += size

    def _reset_staging_root(self) -> None:
        if self.root.exists():
            if self.root.is_symlink() or not self.root.is_dir():
                raise RuntimeError("browser adapter staging root is not a private directory")
        else:
            self.root.mkdir(mode=0o700, parents=True)
        try:
            self.root.chmod(0o700)
        except OSError:
            pass
        _require_private_directory(self.root)
        for child in self.root.iterdir():
            if child not in {self.staging_root, self.finalized_root}:
                self._cleanup_directory(child)
        if self.staging_root.exists():
            if self.staging_root.is_symlink() or not self.staging_root.is_dir():
                raise RuntimeError("browser adapter staging root is not a private directory")
            for child in self.staging_root.iterdir():
                self._cleanup_directory(child)
        else:
            self.staging_root.mkdir(mode=0o700)
        try:
            self.staging_root.chmod(0o700)
        except OSError:
            pass
        _require_private_directory(self.staging_root)
        if self.finalized_root.exists():
            if self.finalized_root.is_symlink() or not self.finalized_root.is_dir():
                raise RuntimeError("browser adapter binding root is not a private directory")
        else:
            self.finalized_root.mkdir(mode=0o700)
        try:
            self.finalized_root.chmod(0o700)
        except OSError:
            pass
        _require_private_directory(self.finalized_root)

    @staticmethod
    def _cleanup_directory(path: Path) -> None:
        try:
            if path.is_symlink():
                path.unlink(missing_ok=True)
            elif path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass


def _normalize_durable_receipt(value: Any) -> dict[str, Any]:
    receipt = _mapping(value, "result")
    base_fields = {
        "schema",
        "schema_version",
        "request_id",
        "accepted_http_status",
        "run_id",
        "initial_state",
        "target",
        "created_at",
        "object",
        "runtime_port_adapters_digest_sha256",
    }
    if frozenset(receipt) not in {
        frozenset(base_fields),
        frozenset(base_fields | {_SEMANTIC_ADVISORIES_FIELD}),
    }:
        _invalid("result")
    if (
        receipt["schema"] != RUN_RECEIPT_SCHEMA
        or receipt["schema_version"] != 1
        or receipt["accepted_http_status"] != 202
    ):
        _invalid("result")
    initial_state = receipt["initial_state"]
    if initial_state not in {
        "queued",
        "starting",
        "preparing_environment",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "timed_out",
        "unknown",
    }:
        _invalid("result")
    target = _normalize_target(receipt["target"], stage="result")
    raw_object = _mapping(receipt["object"], "result")
    _exact(
        raw_object,
        {
            "owner_id",
            "library",
            "object_name",
            "object_id",
            "current_version_id",
            "version_id",
            "version",
            "function",
            "origin",
            "content_hash",
        },
        "result",
    )
    version = raw_object["version"]
    if type(version) is not int or version < 1:
        _invalid("result")
    function = raw_object["function"]
    if function is not None:
        function = _identifier(function, "result")
    origin = raw_object["origin"]
    if origin not in {"local", "server"} or (target["kind"], origin) not in {
        ("local", "local"),
        ("remote", "server"),
    }:
        _invalid("result")
    normalized = {
        "schema": RUN_RECEIPT_SCHEMA,
        "schema_version": 1,
        "request_id": _identifier(receipt["request_id"], "result"),
        "accepted_http_status": 202,
        "run_id": _identifier(receipt["run_id"], "result"),
        "initial_state": initial_state,
        "target": target,
        "created_at": _timestamp(receipt["created_at"], "result"),
        "object": {
            "owner_id": _identifier(raw_object["owner_id"], "result"),
            "library": _identifier(raw_object["library"], "result"),
            "object_name": _identifier(raw_object["object_name"], "result"),
            "object_id": _identifier(raw_object["object_id"], "result"),
            "current_version_id": _identifier(raw_object["current_version_id"], "result"),
            "version_id": _identifier(raw_object["version_id"], "result"),
            "version": version,
            "function": function,
            "origin": origin,
            "content_hash": _prefixed_hash(raw_object["content_hash"], "result"),
        },
        "runtime_port_adapters_digest_sha256": _raw_hash(
            receipt["runtime_port_adapters_digest_sha256"],
            "result",
        ),
    }
    if _SEMANTIC_ADVISORIES_FIELD in receipt:
        try:
            normalized[_SEMANTIC_ADVISORIES_FIELD] = normalize_runtime_adapter_semantic_advisories(
                receipt[_SEMANTIC_ADVISORIES_FIELD]
            )
        except ValueError:
            _invalid("result")
    return normalized


def _normalize_common(value: Mapping[str, Any], *, stage: str) -> dict[str, Any]:
    request_id = _identifier(value["request_id"], stage)
    daemon_instance_id = _identifier(value["daemon_instance_id"], stage)
    generation = value["daemon_generation"]
    if type(generation) is not int or not 1 <= generation <= 9_007_199_254_740_991:
        _invalid(stage)
    registry_revision = _raw_hash(value["registry_revision"], stage)
    object_ref = _mapping(value["object"], stage)
    _exact(object_ref, _OBJECT_FIELDS, stage)
    function = object_ref["function"]
    if function is not None:
        function = _identifier(function, stage)
    origin = object_ref["origin"]
    if origin not in {"local", "server"}:
        _invalid(stage)
    selected_version = object_ref["selected_version"]
    if type(selected_version) is not int or selected_version < 1:
        _invalid(stage)
    normalized_object = {
        "owner_id": _identifier(object_ref["owner_id"], stage),
        "library": _identifier(object_ref["library"], stage),
        "object_name": _identifier(object_ref["object_name"], stage),
        "object_id": _identifier(object_ref["object_id"], stage),
        "expected_current_version_id": _identifier(object_ref["expected_current_version_id"], stage),
        "selected_version_id": _identifier(object_ref["selected_version_id"], stage),
        "selected_version": selected_version,
        "function": function,
        "origin": origin,
    }
    content_hash = _prefixed_hash(value["content_hash"], stage)
    signature = _normalize_signature_evidence(value["signature_evidence"], stage=stage)
    target = _normalize_target(value["target"], stage=stage)
    if (target["kind"], origin) not in {("local", "local"), ("remote", "server")}:
        _invalid(stage)
    inputs = _normalize_inputs(value["inputs"], stage=stage)
    outputs = _normalize_outputs(value["outputs"], stage=stage)
    if len(inputs) + len(outputs) > 1024:
        _invalid(stage)
    staged_total = sum(item["size"] or 0 for item in inputs if item["kind"] == "staged")
    if staged_total > MAX_RUNTIME_INPUT_TOTAL_BYTES:
        raise AdapterRunError("body_too_large", stage, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    selector = value["output_selector"]
    if selector is not None:
        selector = _identifier(selector, stage)
    timeout = value["timeout_ms"]
    if timeout is not None and (type(timeout) is not int or not 0 <= timeout <= 604_800_000):
        _invalid(stage)
    retention = value["retention"]
    if retention not in {"keep", "on_failure", "discard"}:
        _invalid(stage)
    policy = normalize_adapter_policy(value["adapter_policy"])
    expected_remote_policy = "allow" if target["kind"] == "remote" and _LIBRARY_REFS_FIELD in value else "deny"
    if policy != {"custom_remote": expected_remote_policy} or value["source"] != "browser":
        _invalid(stage)
    normalized = {
        "schema": value["schema"],
        "schema_version": 1,
        "request_id": request_id,
        "daemon_instance_id": daemon_instance_id,
        "daemon_generation": generation,
        "registry_revision": registry_revision,
        "object": normalized_object,
        "content_hash": content_hash,
        "signature_evidence": signature,
        "target": target,
        "inputs": inputs,
        "outputs": outputs,
        "output_selector": selector,
        "timeout_ms": timeout,
        "retention": retention,
        "adapter_policy": policy,
        "source": "browser",
    }
    if (
        _contains_path(normalized)
        or _contains_forbidden_key(normalized)
        or _contains_forbidden_credential_value(normalized)
    ):
        _invalid(stage)
    return normalized


def _normalize_signature_evidence(value: Any, *, stage: str) -> dict[str, Any]:
    evidence = _mapping(value, stage)
    _exact(evidence, _SIGNATURE_EVIDENCE_FIELDS, stage)
    fields = _mapping(evidence["semantic_fields"], stage)
    _exact(fields, _SEMANTIC_FIELDS, stage)
    if fields["kind"] not in {"function", "pipeline"}:
        _invalid(stage)
    function = fields["function"]
    if function is not None:
        function = _identifier(function, stage)
    raw_inputs = fields["inputs"]
    raw_outputs = fields["outputs"]
    if not isinstance(raw_inputs, list) or not isinstance(raw_outputs, list):
        _invalid(stage)
    inputs = []
    for raw in raw_inputs:
        item = _mapping(raw, stage)
        _exact(item, _SEMANTIC_INPUT_FIELDS, stage)
        if type(item["required"]) is not bool:
            _invalid(stage)
        inputs.append(
            {
                "name": _identifier(item["name"], stage),
                "type": _optional_bounded_text(item["type"], maximum=512),
                "required": item["required"],
            }
        )
    outputs = []
    for raw in raw_outputs:
        item = _mapping(raw, stage)
        _exact(item, _SEMANTIC_OUTPUT_FIELDS, stage)
        selector = item["selector"]
        if selector is not None:
            selector = _identifier(selector, stage)
        outputs.append(
            {
                "name": _identifier(item["name"], stage),
                "selector": selector,
                "type": _optional_bounded_text(item["type"], maximum=512),
            }
        )
    semantic_fields = {"kind": fields["kind"], "function": function, "inputs": inputs, "outputs": outputs}
    return {
        "semantic_fields": semantic_fields,
        "semantic_hash": _prefixed_hash(evidence["semantic_hash"], stage),
        "review_hash": _prefixed_hash(evidence["review_hash"], stage),
    }


def _normalize_target(value: Any, *, stage: str) -> dict[str, Any]:
    target = _mapping(value, stage)
    _exact(target, _TARGET_FIELDS, stage)
    kind = target["kind"]
    machine_id = target["machine_id"]
    offline_policy = target["offline_policy"]
    if kind == "local":
        if machine_id is not None or offline_policy is not None:
            _invalid(stage)
    elif kind == "remote":
        machine_id = _identifier(machine_id, stage)
        if offline_policy not in {"queue", "wait", "fail_fast"}:
            _invalid(stage)
    else:
        _invalid(stage)
    return {"kind": kind, "machine_id": machine_id, "offline_policy": offline_policy}


def _normalize_inputs(value: Any, *, stage: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > 1024:
        _invalid(stage)
    result = []
    seen: set[str] = set()
    for raw in value:
        item = _mapping(raw, stage)
        _exact(item, _INPUT_FIELDS, stage)
        name = _item_name(item["name"], stage)
        port = _item_name(item["port"], stage)
        if name != port or name in seen:
            _invalid(stage)
        seen.add(name)
        kind = item["kind"]
        adapter_id = _identifier(item["adapter_id"], stage)
        format_tag = (
            None
            if item["format_tag"] is None and adapter_id.startswith("library:")
            else _item_name(item["format_tag"], stage)
        )
        if kind in {"inline_json", "omitted"}:
            if any(item[key] is not None for key in ("logical_name", "media_type", "size", "sha256")):
                _invalid(stage)
            if adapter_id != JSON:
                _invalid(stage)
            if kind == "omitted":
                if item["value"] is not None:
                    _invalid(stage)
            else:
                try:
                    m_json_contract.validate_json_value(item["value"], path=f"$.inputs.{port}")
                except (TypeError, ValueError) as exc:
                    raise AdapterRunError("request_invalid", stage, HTTPStatus.BAD_REQUEST) from exc
            result.append(
                {
                    "name": name,
                    "port": port,
                    "kind": kind,
                    "value": item["value"],
                    "adapter_id": adapter_id,
                    "format_tag": format_tag,
                    "logical_name": None,
                    "media_type": None,
                    "size": None,
                    "sha256": None,
                }
            )
        elif kind == "staged":
            if item["value"] is not None or adapter_id == JSON:
                _invalid(stage)
            logical_name = item["logical_name"]
            media_type = item["media_type"]
            size = item["size"]
            if (
                not isinstance(logical_name, str)
                or not _SAFE_FILE.fullmatch(logical_name)
                or logical_name in {".", ".."}
            ):
                _invalid(stage)
            if not isinstance(media_type, str) or not _MEDIA_TYPE.fullmatch(media_type):
                _invalid(stage)
            if type(size) is not int or not 0 <= size <= MAX_RUNTIME_INPUT_BYTES:
                _invalid(stage)
            result.append(
                {
                    "name": name,
                    "port": port,
                    "kind": kind,
                    "value": None,
                    "adapter_id": adapter_id,
                    "format_tag": format_tag,
                    "logical_name": logical_name,
                    "media_type": media_type.casefold(),
                    "size": size,
                    "sha256": _raw_hash(item["sha256"], stage),
                }
            )
        else:
            _invalid(stage)
    return result


def _normalize_outputs(value: Any, *, stage: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value or len(value) > 1024:
        _invalid(stage)
    result = []
    seen: set[str] = set()
    for raw in value:
        item = _mapping(raw, stage)
        _exact(item, _OUTPUT_FIELDS, stage)
        port = _item_name(item["port"], stage)
        if port in seen:
            _invalid(stage)
        seen.add(port)
        result.append(
            {
                "port": port,
                "adapter_id": _identifier(item["adapter_id"], stage),
                "format_tag": (
                    None
                    if item["format_tag"] is None and str(item["adapter_id"]).startswith("library:")
                    else _item_name(item["format_tag"], stage)
                ),
            }
        )
    return result


def _request_digest(document: Mapping[str, Any]) -> str:
    common = {key: document[key] for key in sorted(_COMMON_FIELDS - {"schema"})}
    if _LIBRARY_REFS_FIELD in document:
        common[_LIBRARY_REFS_FIELD] = document[_LIBRARY_REFS_FIELD]
    if _SEMANTIC_ADVISORIES_FIELD in document:
        semantic = normalize_runtime_adapter_semantic_advisories(document[_SEMANTIC_ADVISORIES_FIELD])
        common[_SEMANTIC_ADVISORIES_FIELD] = {
            "schema_version": semantic["schema_version"],
            "bindings": [
                {**item, "acknowledged": False, "acknowledgement_source": None} for item in semantic["bindings"]
            ],
        }
    encoded = m_json_contract.dumps(common, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(b"spl.daemon.adapter-run-request.v1\0" + encoded).hexdigest()


def _validate_browser_semantic_advisories(
    document: Mapping[str, Any],
    authoritative_bindings: list[dict[str, Any]],
    *,
    stage: str,
) -> dict[str, Any] | None:
    authoritative = normalize_runtime_adapter_semantic_advisories(
        {"schema_version": 1, "bindings": authoritative_bindings}
    )
    supplied = document.get(_SEMANTIC_ADVISORIES_FIELD)
    if supplied is None:
        if any(item["state"] != "recommended" for item in authoritative["bindings"]):
            raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
        return None
    observed = normalize_runtime_adapter_semantic_advisories(supplied)
    expected_by_identity = {(item["direction"], item["port"]): item for item in authoritative["bindings"]}
    observed_by_identity = {(item["direction"], item["port"]): item for item in observed["bindings"]}
    if not set(observed_by_identity) <= set(expected_by_identity):
        raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
    required_nonrecommended = {
        identity for identity, item in expected_by_identity.items() if item["state"] != "recommended"
    }
    if not required_nonrecommended <= set(observed_by_identity):
        raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
    immutable_fields = (
        "direction",
        "port",
        "port_semantic_type",
        "adapter_semantic_type",
        "adapter_semantic_category",
        "state",
    )
    bindings = []
    refs = document.get(_LIBRARY_REFS_FIELD)
    refs_by_identity = {
        (item["direction"], item["port"]): item
        for item in (refs or {}).get("bindings", [])
        if isinstance(item, Mapping)
    }
    for identity, item in observed_by_identity.items():
        expected = expected_by_identity[identity]
        ref = refs_by_identity.get(identity)
        adapter_identity_matches = item["adapter_id"] == expected["adapter_id"] or (
            ref is not None
            and item["adapter_id"] == f"library:{ref['content_hash']}"
            and expected["adapter_id"] == ref["adapter_id"]
        )
        if not adapter_identity_matches or any(item[key] != expected[key] for key in immutable_fields):
            raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
        if stage == "preflight":
            valid_ack = item["acknowledged"] is False and item["acknowledgement_source"] is None
        elif expected["state"] == "recommended":
            valid_ack = item["acknowledged"] is False and item["acknowledgement_source"] is None
        else:
            valid_ack = item["acknowledged"] is True and item["acknowledgement_source"] == "plugin_confirmation"
        if not valid_ack:
            raise AdapterRunError("adapter_binding_invalid", stage, HTTPStatus.BAD_REQUEST)
        bindings.append({**item, "adapter_id": expected["adapter_id"]})
    return normalize_runtime_adapter_semantic_advisories({"schema_version": 1, "bindings": bindings})


def _validate_library_ref_selections(document: Mapping[str, Any], *, stage: str) -> None:
    refs = document.get(_LIBRARY_REFS_FIELD)
    selected = {("input", item["port"]): item for item in document["inputs"]} | {
        ("output", item["port"]): item for item in document["outputs"]
    }
    library_selected = {
        identity for identity, item in selected.items() if str(item["adapter_id"]).startswith("library:")
    }
    if not isinstance(refs, Mapping):
        if library_selected:
            _invalid(stage)
        return
    ref_bindings = refs.get("bindings")
    if not isinstance(ref_bindings, list):
        _invalid(stage)
    identities = {(item["direction"], item["port"]) for item in ref_bindings}
    if identities != library_selected:
        _invalid(stage)
    for ref in ref_bindings:
        item = selected[(ref["direction"], ref["port"])]
        if item["adapter_id"] != f"library:{ref['content_hash']}":
            _invalid(stage)


def _synthetic_admission(document: Mapping[str, Any]) -> _Admission:
    return _Admission(
        request_id=str(document["request_id"]),
        request_digest_sha256=_request_digest(document),
        created_at=_utc_now(),
        expires_at=_utc_after(1),
        document=dict(document),
        state="ready",
        directory=Path("/nonexistent-browser-adapter-preflight"),
    )


def _central_preflight_request(document: Mapping[str, Any]) -> dict[str, Any]:
    obj = document["object"]
    target = document["target"]
    payload = {
        "object": obj["object_name"],
        "version": obj["selected_version"],
        "version_id": obj["selected_version_id"],
        "target_machine_id": target["machine_id"],
        "object_owner_id": obj["owner_id"],
        "library": obj["library"],
        "offline_policy": target["offline_policy"],
    }
    if obj["function"] is not None:
        payload["function"] = obj["function"]
    return payload


def _remote_admission_id(request_id: str) -> str:
    return "remote_run_request_" + hashlib.sha256(f"spl.remote_run_request:{request_id}".encode("utf-8")).hexdigest()


def _staged_wire_projection(document: Mapping[str, Any]) -> dict[str, Any]:
    normalized = normalize_wire_document(document, allow_content=True)
    bundle = normalized.get("custom_bundle")
    return normalize_wire_document(
        {
            **normalized,
            "inputs": [{**item, "content_base64": None, "staged_name": item["name"]} for item in normalized["inputs"]],
            "custom_bundle": (
                None
                if bundle is None
                else {
                    **bundle,
                    "content_base64": None,
                    "staged_name": bundle["name"],
                }
            ),
        },
        allow_content=False,
    )


def _json_template_signature(signature: Mapping[str, Any]) -> dict[str, Any]:
    """Keep signature topology while making the temporary JSON template neutral."""

    template = dict(signature)
    template["inputs"] = [
        {**item, "type": "typing.Any"} if isinstance(item, Mapping) else item for item in signature.get("inputs") or []
    ]
    outputs = []
    for raw in signature.get("outputs") or []:
        if not isinstance(raw, Mapping):
            outputs.append(raw)
            continue
        item = {**raw, "type": "typing.Any"}
        ports = raw.get("ports")
        if isinstance(ports, list):
            item["ports"] = [{**port, "type": "typing.Any"} if isinstance(port, Mapping) else port for port in ports]
        outputs.append(item)
    template["outputs"] = outputs
    return template


def _external_name(signature: Mapping[str, Any], port: str) -> str:
    kind = signature.get("kind")
    if kind == "function":
        return port
    for item in signature.get("inputs") or []:
        if not isinstance(item, Mapping):
            continue
        external = str(item.get("name") or "")
        for source in item.get("sources") or []:
            if not isinstance(source, Mapping):
                continue
            canonical = f"{source.get('function') or source.get('node_id')}.{source.get('port')}"
            if canonical == port:
                return external
    # The authoritative wire validation rejects an unresolved port.
    return port


def _runtime_output_records(state: Mapping[str, Any], result: Mapping[str, Any], *, target: str) -> list[Any]:
    if target == "local":
        records = result.get("runtime_port_adapter_outputs")
        return records if isinstance(records, list) else []
    runtime = state.get("runtime_port_adapters")
    terminal = runtime.get("terminal") if isinstance(runtime, Mapping) else None
    records = terminal.get("outputs") if isinstance(terminal, Mapping) else None
    return records if isinstance(records, list) else []


def _result_value_for_port(result: Mapping[str, Any], port: str, output_selector: str | None) -> Any:
    wrapped = result.get("result")
    value = wrapped.get("value") if isinstance(wrapped, Mapping) and "value" in wrapped else wrapped
    if output_selector is not None and isinstance(value, Mapping) and output_selector in value:
        value = value[output_selector]
    if port == "default":
        return value
    if isinstance(value, Mapping):
        short = port.rsplit(".", 1)[-1]
        if port in value:
            return value[port]
        if short in value:
            return value[short]
    return value


def _output_projection(
    *,
    run_id: str,
    port: str,
    adapter_id: str,
    format_tag: str | None,
    logical_name: str,
    media_type: str | None,
    size: int | None,
    sha256: str | None,
    availability: str,
    reason: str | None,
    preview: dict[str, Any],
    download_handle: str | None,
) -> dict[str, Any]:
    return {
        "port": port,
        "direction": "output",
        "adapter_id": adapter_id,
        "format_tag": format_tag,
        "logical_name": logical_name,
        "media_type": media_type,
        "size": size,
        "sha256": sha256,
        "availability": availability,
        "reason": reason,
        "preview": preview,
        "download_handle": download_handle,
        "provenance": {
            "run_id": run_id,
            "port": port,
            "adapter_id": adapter_id,
            "format_tag": format_tag,
            "sha256": sha256,
        },
    }


def _unavailable_output_projection(
    run_id: str,
    selected: Mapping[str, Any],
) -> dict[str, Any]:
    library_output = str(selected["adapter_id"]).startswith("library:")
    if library_output:
        extension = ".bin"
        media_type = None
    else:
        _, descriptor = built_in_descriptor(str(selected["adapter_id"]))
        extension = descriptor["presentation"]["preferred_extension"]
        if extension is None:
            extension = ".json" if selected["adapter_id"] == JSON else ".bin"
        media_type = descriptor["presentation"]["media_type"] or "application/octet-stream"
    return _output_projection(
        run_id=run_id,
        port=str(selected["port"]),
        adapter_id=str(selected["adapter_id"]),
        format_tag=(selected["format_tag"] if isinstance(selected["format_tag"], str) else None),
        logical_name=_safe_output_name(str(selected["port"]), extension),
        media_type=media_type,
        size=None,
        sha256=None,
        availability="unavailable",
        reason="result_unavailable",
        preview=_empty_preview(media_type),
        download_handle=None,
    )


def _empty_preview(media_type: str | None) -> dict[str, Any]:
    kind = (
        "png"
        if media_type == "image/png"
        else "csv"
        if media_type == "text/csv"
        else "text"
        if media_type == "text/plain"
        else "json"
        if media_type == "application/json"
        else "none"
    )
    return {"kind": kind, "available": False, "text": None, "json": None, "truncated": False}


def _safe_failure(state: Mapping[str, Any]) -> dict[str, Any]:
    manifest = state.get("manifest")
    runtime = manifest.get("runtime_port_adapters") if isinstance(manifest, Mapping) else None
    failure = runtime.get("failure") if isinstance(runtime, Mapping) else None
    if isinstance(failure, Mapping) and set(failure) == {
        "schema_version",
        "code",
        "stage",
        "direction",
        "port",
        "adapter_id",
        "adapter_ref",
        "message",
        "retryable",
        "fallback_used",
    }:
        code = failure.get("code")
        stage = failure.get("stage")
        direction = failure.get("direction")
        expected = {
            "input_adapter_load_failed": (
                "worker_load",
                "input",
                "The selected input Adapter could not load this value.",
            ),
            "output_adapter_save_failed": (
                "worker_save",
                "output",
                "The selected output Adapter could not save this value.",
            ),
        }.get(str(code))
        adapter_ref = failure.get("adapter_ref")
        try:
            normalized_ref = None if adapter_ref is None else normalize_library_adapter_ref(adapter_ref)
        except ValueError:
            normalized_ref = None
            expected = None
        if (
            failure.get("schema_version") == 1
            and expected is not None
            and (stage, direction, failure.get("message")) == expected
            and isinstance(failure.get("port"), str)
            and _SAFE_ITEM.fullmatch(str(failure["port"]))
            and isinstance(failure.get("adapter_id"), str)
            and _SAFE_ID.fullmatch(str(failure["adapter_id"]))
            and failure.get("retryable") is False
            and failure.get("fallback_used") is False
        ):
            return {**dict(failure), "adapter_ref": normalized_ref}
    raw_stage = str(failure.get("stage")) if isinstance(failure, Mapping) else "object_execution"
    stage = (
        raw_stage
        if raw_stage
        in {
            "adapter_resolution",
            "environment_preflight",
            "worker_load",
            "object_execution",
            "worker_save",
            "result_projection",
        }
        else "object_execution"
    )
    reason_map = {
        "adapter_resolution": "adapter_unavailable",
        "environment_preflight": "dependency_missing",
        "worker_load": "input_decode_failed",
        "object_execution": "execution_failed",
        "worker_save": "output_encode_failed",
        "result_projection": "result_unavailable",
    }
    reason = reason_map[stage]
    return {"stage": stage, "reason_code": reason, "message": reason.replace("_", " ").capitalize() + "."}


def _canonical_run_state(value: Any) -> str:
    state = str(value or "unknown")
    mapping = {
        "assigned": "starting",
        "preparing": "preparing_environment",
        "timeout": "timed_out",
        "timed_out": "timed_out",
        "stale": "failed",
    }
    state = mapping.get(state, state)
    return (
        state
        if state
        in {"queued", "starting", "preparing_environment", "running", "succeeded", "failed", "cancelled", "timed_out"}
        else "unknown"
    )


def _read_verified_file(path: Path, *, size: int, sha256: str) -> bytes:
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size != size:
        raise FileNotFoundError(path)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise FileNotFoundError(path)
        chunks: list[bytes] = []
        digest = hashlib.sha256()
        remaining = size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
        if len(data) != size or after.st_size != size or digest.hexdigest() != sha256:
            raise AdapterRunError("artifact_mismatch", "download", HTTPStatus.CONFLICT)
        return data
    finally:
        os.close(descriptor)


def _read_verified_prefix(path: Path, *, size: int, sha256: str) -> bytes:
    if size <= MAX_PREVIEW_BYTES:
        return _read_verified_file(path, size=size, sha256=sha256)
    # Verify the entire digest while retaining only the bounded prefix.
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size != size:
        raise FileNotFoundError(path)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        digest = hashlib.sha256()
        prefix = bytearray()
        total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            total += len(chunk)
            if len(prefix) < MAX_PREVIEW_BYTES:
                prefix.extend(chunk[: MAX_PREVIEW_BYTES - len(prefix)])
        if total != size or digest.hexdigest() != sha256:
            raise AdapterRunError("artifact_mismatch", "result", HTTPStatus.CONFLICT)
        return bytes(prefix)
    finally:
        os.close(descriptor)


def _durable_binding_name(run_id: str) -> str:
    return hashlib.sha256(("spl-browser-adapter-run\0" + run_id).encode("utf-8")).hexdigest()


def _atomic_private_json(path: Path, value: Mapping[str, Any]) -> None:
    data = m_json_contract.dumps(value).encode("utf-8")
    if len(data) > MAX_JSON_BODY_BYTES:
        raise RuntimeError("finalized adapter Run binding exceeds the private cache bound")
    _require_private_directory(path.parent)
    temporary = path.parent / ("." + secrets.token_hex(24) + ".tmp")
    descriptor: int | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(temporary, flags, 0o600)
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        temporary.replace(path)
        parent_descriptor = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            os.fsync(parent_descriptor)
        finally:
            os.close(parent_descriptor)
        stored = path.lstat()
        if not stat.S_ISREG(stored.st_mode) or stored.st_nlink != 1 or stored.st_size != len(data):
            raise RuntimeError("finalized adapter Run binding was not stored safely")
    finally:
        if descriptor is not None:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _require_private_directory(path: Path) -> None:
    value = path.lstat()
    if not stat.S_ISDIR(value.st_mode) or stat.S_ISLNK(value.st_mode):
        raise AdapterRunError("admission_conflict", "admission", HTTPStatus.CONFLICT)


def _require_private_regular_file(path: Path, *, expected_size: int) -> None:
    value = path.lstat()
    if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1 or value.st_size != expected_size:
        raise AdapterRunError("upload_size_mismatch", "upload", HTTPStatus.BAD_REQUEST)


def _staged_item(document: Mapping[str, Any], name: str) -> dict[str, Any] | None:
    return next(
        (item for item in document["inputs"] if item["kind"] == "staged" and item["name"] == name),
        None,
    )


def _timeout_seconds(value: int | None) -> float | None:
    return None if value is None else value / 1000


def _keep_policy(value: str) -> bool | Literal["on_failure"]:
    if value == "keep":
        return True
    if value == "discard":
        return False
    return "on_failure"


def _safe_output_name(port: str, extension: str) -> str:
    token = re.sub(r"[^A-Za-z0-9._-]+", "-", port).strip(".-") or "output"
    return (token[: 254 - len(extension)] + extension)[:255]


def _canonical_upstream_time(value: Any) -> str:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except ValueError:
        return _utc_now()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _utc_after(seconds: int) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _closed_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _raise_request_invalid() -> NoReturn:
    raise ValueError("non-finite JSON number")


def _mapping(value: Any, stage: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _invalid(stage)
    return cast(Mapping[str, Any], value)


def _exact(value: Mapping[str, Any], fields: frozenset[str] | set[str], stage: str) -> None:
    if set(value) != set(fields):
        _invalid(stage)


def _identifier(value: Any, stage: str) -> str:
    if not isinstance(value, str) or not _SAFE_ID.fullmatch(value):
        _invalid(stage)
    return value


def _item_name(value: Any, stage: str) -> str:
    if not isinstance(value, str) or not _SAFE_ITEM.fullmatch(value) or value in {".", ".."}:
        _invalid(stage)
    return value


def _raw_hash(value: Any, stage: str) -> str:
    if not isinstance(value, str) or not _RAW_HASH.fullmatch(value):
        _invalid(stage)
    return value


def _prefixed_hash(value: Any, stage: str) -> str:
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        _invalid(stage)
    return value


def _timestamp(value: Any, stage: str) -> str:
    if not isinstance(value, str) or not _UTC_MILLISECONDS.fullmatch(value):
        _invalid(stage)
    try:
        parsed = _parse_timestamp(value)
    except ValueError:
        _invalid(stage)
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        _invalid(stage)
    return value


def _bounded_text(value: Any, *, maximum: int) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= maximum:
        raise AdapterRunError("signature_mismatch", "preflight", HTTPStatus.PRECONDITION_FAILED)
    return value


def _optional_bounded_text(value: Any, *, maximum: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > maximum:
        raise AdapterRunError("signature_mismatch", "preflight", HTTPStatus.PRECONDITION_FAILED)
    return value


def _contains_path(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(_contains_path(str(key)) or _contains_path(child) for key, child in value.items())
    if isinstance(value, list):
        return any(_contains_path(child) for child in value)
    if not isinstance(value, str):
        return False
    return (
        _POSIX_PATH.search(value) is not None
        or _WINDOWS_PATH.search(value) is not None
        or _FILE_URL.search(value) is not None
    )


def _normalize_safety_key(value: str) -> str:
    with_acronyms = _CAMEL_ACRONYM_BOUNDARY.sub(r"\1_\2", value)
    with_words = _CAMEL_WORD_BOUNDARY.sub(r"\1_\2", with_acronyms)
    return _KEY_SEPARATOR.sub("_", with_words).strip("_").casefold()


def _contains_forbidden_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = _normalize_safety_key(str(key))
            if normalized in _FORBIDDEN_KEY_EXACT or normalized.endswith(_FORBIDDEN_KEY_SUFFIX):
                return True
            if _contains_forbidden_key(child):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden_key(child) for child in value)
    return False


def _contains_forbidden_credential_value(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(_contains_forbidden_credential_value(child) for child in value.values())
    if isinstance(value, list):
        return any(_contains_forbidden_credential_value(child) for child in value)
    if not isinstance(value, str):
        return False
    return any(pattern.search(value) is not None for pattern in _FORBIDDEN_CREDENTIAL_VALUE_PATTERNS)


def _invalid(stage: str) -> NoReturn:
    raise AdapterRunError("request_invalid", stage, HTTPStatus.BAD_REQUEST)


__all__ = [
    "ADAPTER_RUN_ADMISSIONS_ROUTE",
    "ADAPTER_RUN_CAPABILITY",
    "ADAPTER_RUN_CAPABILITY_VERSION",
    "ADAPTER_RUN_PREFLIGHT_ROUTE",
    "ADAPTER_RUN_SCHEMA_VERSION",
    "AdapterRunError",
    "BrowserAdapterRunBroker",
    "MAX_JSON_BODY_BYTES",
    "adapter_run_error_document",
    "parse_adapter_run_request",
    "parse_finalize_request",
    "semantic_signature_fields",
    "semantic_signature_hash",
]
