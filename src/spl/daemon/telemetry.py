"""Privacy policy and bounded payload construction for daemon telemetry."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Any, Literal, cast

from spl.core import json_contract as m_json_contract
from spl.core.redaction import (
    normalize_json_pointers,
    redact_text,
    redact_value,
    sensitive_values_at_common_keys,
    sensitive_values_at_pointers,
)
from spl.daemon.artifact_access import count_regular_files_bounded

TelemetryLevel = Literal["metadata", "diagnostic", "full"]

TELEMETRY_LEVELS = ("metadata", "diagnostic", "full")
DEFAULT_TELEMETRY_LEVEL: TelemetryLevel = "metadata"
TELEMETRY_REDACTION_MODE = "best_effort"
TELEMETRY_PAYLOAD_TTL_SECONDS = 7 * 24 * 60 * 60
TELEMETRY_ERROR_MAX_BYTES = 8 * 1024
TELEMETRY_STREAM_MAX_BYTES = 32 * 1024
TELEMETRY_FULL_ERROR_MAX_BYTES = 64 * 1024
TELEMETRY_RUNTIME_DETAIL_MAX_BYTES = 32 * 1024
TELEMETRY_SENSITIVE_VALUE_LIMIT = 256
TELEMETRY_NODE_DETAIL_LIMIT = 100
TELEMETRY_METADATA_TEXT_MAX_WIRE_BYTES = 200
TELEMETRY_METADATA_STREAM_SIZE_MAX_BYTES = TELEMETRY_STREAM_MAX_BYTES

# These are the daemon/server sync contract limits. Keep every producer on this
# one definition so queue admission and batching cannot drift apart.
SYNC_EVENT_BATCH_LIMIT = 50
SYNC_EVENT_SCAN_PAGE_LIMIT = 200
SYNC_EVENT_MAX_BYTES = 256 * 1024
SYNC_BATCH_MAX_BYTES = 512 * 1024
SYNC_EVENT_PAYLOAD_BUDGET = 240 * 1024

_UNPARSED_RESULT = object()


@dataclass(frozen=True)
class TelemetryPolicy:
    """One daemon-wide telemetry level and its explicit sensitive fields."""

    level: TelemetryLevel = DEFAULT_TELEMETRY_LEVEL
    sensitive_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        normalized_level = normalize_telemetry_level(self.level)
        normalized_fields = normalize_json_pointers(self.sensitive_fields)
        object.__setattr__(self, "level", normalized_level)
        object.__setattr__(self, "sensitive_fields", normalized_fields)

    def status(self) -> dict[str, Any]:
        """Return a nonsecret operational summary for health and diagnostics."""

        return {
            "level": self.level,
            "default": self.level == DEFAULT_TELEMETRY_LEVEL,
            "redaction": TELEMETRY_REDACTION_MODE,
            "sensitive_field_count": len(self.sensitive_fields),
            "raw_values_mirrored": self.level == "full",
            "diagnostic_text_mirrored": self.level in {"diagnostic", "full"},
        }

    def build_local_run_payload(
        self,
        state: dict[str, Any],
        object_label: dict[str, Any],
        *,
        full_artifacts: list[dict[str, Any]] | None = None,
        artifact_count: int | None = None,
        artifact_count_truncated: bool = False,
        preflight_omissions: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        """Build one bounded, level-specific local-run telemetry projection."""

        source_result_present = _source_result_present(state)
        input_payload = state.get("input") if isinstance(state.get("input"), dict) else {}
        artifacts = full_artifacts or []
        omissions = list(dict.fromkeys(preflight_omissions))
        input_fits = True
        result_fits = True
        artifacts_fit = True
        sensitive_values: tuple[str, ...] = ()
        result_source: Any = state.get("result")
        parsed_unreadable_result: Any = _UNPARSED_RESULT
        unreadable_result_json: str | None = None

        if self.level in {"diagnostic", "full"}:
            input_fits = _json_component_fits(input_payload)
            result_source, result_fits, parsed_unreadable_result = _result_redaction_source(
                state,
                self.sensitive_fields,
            )
            result_fits = not source_result_present or result_fits
            if self.level == "full":
                artifacts_fit = _json_component_fits(artifacts)

            sensitive_source: dict[str, Any] = {}
            if input_fits:
                sensitive_source["input"] = input_payload
            if source_result_present and result_fits:
                sensitive_source["result"] = result_source
            explicit_sensitive_values = sensitive_values_at_pointers(
                sensitive_source,
                self.sensitive_fields,
                max_values=TELEMETRY_SENSITIVE_VALUE_LIMIT + 1,
            )
            common_sensitive_values = sensitive_values_at_common_keys(
                sensitive_source,
                max_values=TELEMETRY_SENSITIVE_VALUE_LIMIT + 1,
            )
            discovered_values = tuple(dict.fromkeys((*explicit_sensitive_values, *common_sensitive_values)))
            sensitive_values = discovered_values[:TELEMETRY_SENSITIVE_VALUE_LIMIT]
            if not input_fits or not result_fits or len(discovered_values) > TELEMETRY_SENSITIVE_VALUE_LIMIT:
                _add_omission(omissions, "sensitive_value_discovery_limit")
            if self.level == "full":
                raw_result_json = state.get("result_json")
                if result_fits and isinstance(raw_result_json, str):
                    if parsed_unreadable_result is _UNPARSED_RESULT:
                        unreadable_result_json = raw_result_json
                    else:
                        unreadable_result_json = _redact_unreadable_result_json(
                            parsed_unreadable_result,
                            sensitive_fields=self.sensitive_fields,
                            sensitive_values=sensitive_values,
                        )
                        if unreadable_result_json is None:
                            result_fits = False
                            _add_omission(omissions, "result_redaction_limit")
                if not input_fits:
                    _add_omission(omissions, "input_preflight_limit")
                if not result_fits:
                    _add_omission(omissions, "result_preflight_limit")
                if not artifacts_fit:
                    _add_omission(omissions, "artifact_bodies_preflight_limit")

        telemetry = _telemetry_summary(
            state,
            level=self.level,
            source_result_present=source_result_present,
            artifact_count=len(artifacts) if artifact_count is None else artifact_count,
            artifact_count_truncated=artifact_count_truncated,
            omissions=omissions,
        )
        payload: dict[str, Any] = {
            "id": state["id"],
            "object_name": object_label["display_name"],
            "object_display_name": object_label["display_name"],
            "local_object_name": object_label["local_name"],
            "object_id": state.get("object_id"),
            "object_version_id": state.get("object_version_id"),
            "object_version": state.get("object_version"),
            "owner_id": object_label.get("owner_id"),
            "remote_object_id": object_label.get("remote_object_id"),
            "remote_version_id": object_label.get("remote_version_id"),
            "entrypoint": state.get("entrypoint"),
            "env": state.get("env"),
            "status": state.get("status"),
            "started_at": state.get("started_at"),
            "finished_at": state.get("finished_at"),
            "created_at": state.get("created_at"),
            "telemetry_level": self.level,
            "telemetry": telemetry,
            "source_result_present": source_result_present,
            "input_mirrored": False,
            "result_mirrored": False,
            "streams_mirrored": False,
            "artifact_bodies_mirrored": False,
        }
        telemetry["omissions"] = omissions

        if self.level == "diagnostic":
            _add_diagnostic_content(payload, state, sensitive_values, omissions)
        elif self.level == "full":
            _add_full_content(
                payload,
                state,
                artifacts if artifacts_fit else [],
                sensitive_fields=self.sensitive_fields,
                sensitive_values=sensitive_values,
                mirror_input=input_fits,
                mirror_result=result_fits,
                unreadable_result_json=unreadable_result_json,
                omissions=omissions,
            )

        if not omissions:
            payload["telemetry"].pop("omissions", None)
        _synchronize_availability(payload)
        _fit_payload(payload)
        _synchronize_availability(payload)
        return payload


def normalize_telemetry_level(value: str) -> TelemetryLevel:
    """Validate a daemon telemetry level."""

    normalized = str(value).strip().casefold()
    if normalized not in TELEMETRY_LEVELS:
        raise ValueError("telemetry level must be one of: metadata, diagnostic, full")
    return cast(TelemetryLevel, normalized)


def event_wire_size(event_id: str, kind: str, payload: dict[str, Any]) -> int:
    """Return the exact compact JSON byte size used by the sync wire envelope."""

    value = {"id": event_id, "kind": kind, "payload": payload}
    return len(m_json_contract.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8"))


def local_run_proof(kind: str, payload: Any) -> tuple[str, str] | None:
    """Extract the local run id/status proof from a run-related sync event."""

    if not isinstance(payload, dict):
        return None
    if kind == "local_run_update":
        local = payload.get("run")
    elif kind == "run_update":
        detail = payload.get("payload")
        local = detail.get("local_run") if isinstance(detail, dict) else None
        if local is None and isinstance(payload.get("local_run_id"), str):
            local = {
                "id": payload["local_run_id"],
                "status": payload.get("local_run_status") or payload.get("status"),
            }
    else:
        return None
    if not isinstance(local, dict):
        return None
    run_id = local.get("id")
    status = local.get("status")
    if not isinstance(run_id, str) or not isinstance(status, str):
        return None
    return run_id, status


def _telemetry_summary(
    state: dict[str, Any],
    *,
    level: TelemetryLevel,
    source_result_present: bool,
    artifact_count: int,
    artifact_count_truncated: bool,
    omissions: list[str],
) -> dict[str, Any]:
    raw_input = state.get("input")
    input_payload: dict[str, Any] = raw_input if isinstance(raw_input, dict) else {}
    raw_args = input_payload.get("args")
    args: list[Any] = raw_args if isinstance(raw_args, list) else []
    raw_kwargs = input_payload.get("kwargs")
    kwargs: dict[str, Any] = raw_kwargs if isinstance(raw_kwargs, dict) else {}
    raw_manifest = state.get("manifest")
    manifest: dict[str, Any] = raw_manifest if isinstance(raw_manifest, dict) else {}
    manifest_nodes = manifest.get("nodes")
    raw_nodes: dict[str, Any] = manifest_nodes if isinstance(manifest_nodes, dict) else {}
    manifest_edges = manifest.get("edges")
    raw_edges: list[Any] = manifest_edges if isinstance(manifest_edges, list) else []
    nodes: list[dict[str, Any]] = []
    bounded_nodes = list(islice(raw_nodes.items(), TELEMETRY_NODE_DETAIL_LIMIT))
    bounded_nodes.sort(key=lambda item: item[0] if type(item[0]) is str else "")
    node_details_truncated = len(raw_nodes) > len(bounded_nodes)
    if node_details_truncated:
        _add_omission(omissions, "node_detail_limit")
    for node_id, raw_node in bounded_nodes:
        node = raw_node if isinstance(raw_node, dict) else {}
        raw_fingerprint = node.get("fingerprint")
        fingerprint: dict[str, Any] = raw_fingerprint if isinstance(raw_fingerprint, dict) else {}
        nodes.append(
            {
                "id": _bounded_metadata_text(node.get("id") or node_id, omissions=omissions),
                "alias": _bounded_metadata_text(node.get("alias"), omissions=omissions),
                "name": _bounded_metadata_text(node.get("name"), omissions=omissions),
                "kind": _bounded_metadata_text(node.get("kind"), omissions=omissions),
                "status": _bounded_metadata_text(node.get("status"), omissions=omissions),
                "fingerprint_sha256": _bounded_metadata_text(
                    fingerprint.get("sha256"),
                    omissions=omissions,
                ),
            }
        )
    raw_pipeline = manifest.get("pipeline")
    pipeline: dict[str, Any] = raw_pipeline if isinstance(raw_pipeline, dict) else {}
    error_type = _error_type(state.get("error"))
    error = (
        {
            "type": error_type,
            "message": "[details withheld by metadata telemetry]",
        }
        if error_type is not None
        else None
    )
    stdout_bytes, stdout_bytes_truncated = _bounded_text_byte_count(
        state.get("stdout"),
        TELEMETRY_METADATA_STREAM_SIZE_MAX_BYTES,
    )
    stderr_bytes, stderr_bytes_truncated = _bounded_text_byte_count(
        state.get("stderr"),
        TELEMETRY_METADATA_STREAM_SIZE_MAX_BYTES,
    )
    if stdout_bytes_truncated or stderr_bytes_truncated:
        _add_omission(omissions, "stream_size_limit")
    summary = {
        "argument_count": len(args),
        "keyword_argument_count": len(kwargs),
        "node_count": len(raw_nodes),
        "node_detail_count": len(nodes),
        "edge_count": len(raw_edges),
        "artifact_count": artifact_count,
        "artifact_count_truncated": artifact_count_truncated,
        "stdout_bytes": stdout_bytes,
        "stderr_bytes": stderr_bytes,
        "duration_ms": _duration_ms(state.get("started_at"), state.get("finished_at")),
        "source_result_present": source_result_present,
    }
    if node_details_truncated:
        summary["node_detail_count_truncated"] = True
    if stdout_bytes_truncated:
        summary["stdout_bytes_truncated"] = True
    if stderr_bytes_truncated:
        summary["stderr_bytes_truncated"] = True
    return {
        "level": level,
        "redaction": TELEMETRY_REDACTION_MODE,
        "availability": {
            "input": False,
            "result": False,
            "streams": False,
            "artifact_bodies": False,
        },
        "summary": summary,
        "nodes": nodes,
        "hashes": {
            "pipeline_content_hash": _bounded_metadata_text(
                pipeline.get("content_hash"),
                omissions=omissions,
            ),
            "runtime_build_hash": _bounded_metadata_text(
                state.get("runtime_build_hash"),
                omissions=omissions,
            ),
        },
        "error": error,
    }


def _add_diagnostic_content(
    payload: dict[str, Any],
    state: dict[str, Any],
    sensitive_values: tuple[str, ...],
    omissions: list[str],
) -> None:
    error = state.get("error")
    if isinstance(error, str) and error:
        bounded_error, error_truncated = _bounded_utf8(error, TELEMETRY_ERROR_MAX_BYTES)
        if error_truncated:
            _add_omission(omissions, "error_text_limit")
        payload["error"] = _truncate_utf8(
            redact_text(bounded_error, sensitive_values=sensitive_values),
            TELEMETRY_ERROR_MAX_BYTES,
        )
        telemetry_error = payload["telemetry"].get("error")
        if isinstance(telemetry_error, dict):
            telemetry_error["message"] = payload["error"]

    artifacts: list[dict[str, Any]] = []
    for field, name, kind in (
        ("stdout", "stdout.txt", "stdout"),
        ("stderr", "stderr.txt", "stderr"),
    ):
        raw = state.get(field)
        if not isinstance(raw, str) or not raw:
            continue
        bounded_raw, raw_size, raw_truncated = _bounded_utf8_with_size(
            raw,
            TELEMETRY_STREAM_MAX_BYTES,
        )
        if raw_truncated:
            _add_omission(omissions, "stream_text_limit")
        redacted_content = redact_text(bounded_raw, sensitive_values=sensitive_values)
        content, _, redacted_truncated = _bounded_utf8_with_size(
            redacted_content,
            TELEMETRY_STREAM_MAX_BYTES,
        )
        artifacts.append(
            {
                "name": name,
                "kind": kind,
                "content_type": "text/plain; charset=utf-8",
                "content_text": content,
                "size_bytes": raw_size,
                "truncated": raw_truncated or redacted_truncated,
            }
        )
    if artifacts:
        payload["artifacts"] = artifacts
        payload["streams_mirrored"] = True


def _add_full_content(
    payload: dict[str, Any],
    state: dict[str, Any],
    full_artifacts: list[dict[str, Any]],
    *,
    sensitive_fields: tuple[str, ...],
    sensitive_values: tuple[str, ...],
    mirror_input: bool,
    mirror_result: bool,
    unreadable_result_json: str | None,
    omissions: list[str],
) -> None:
    input_payload = state.get("input") if isinstance(state.get("input"), dict) else {}
    if mirror_input:
        payload["input"] = input_payload
        payload["input_mirrored"] = True
    source_result_present = _source_result_present(state)
    if not source_result_present:
        payload["result"] = None
        payload["result_present"] = False
    elif mirror_result:
        payload["result"] = state.get("result")
        payload["result_present"] = True
        payload["result_mirrored"] = True
        if state.get("result_unreadable") is True and isinstance(state.get("result_json"), str):
            payload["result_unreadable"] = True
    error = state.get("error")
    if isinstance(error, str):
        payload["error"], error_truncated = _bounded_utf8(error, TELEMETRY_FULL_ERROR_MAX_BYTES)
        if error_truncated:
            _add_omission(omissions, "error_text_limit")
    elif _json_component_fits(error, TELEMETRY_FULL_ERROR_MAX_BYTES):
        payload["error"] = error
    else:
        _add_omission(omissions, "error_text_limit")
    for field in (
        "runtime_backend",
        "worker_runtime",
        "worker_runtime_reason",
        "runtime_build_hash",
        "resolved_runtime",
        "interpreter_substitution",
        "image_tag",
        "container_id",
    ):
        value = state.get(field)
        if _json_component_fits(value, TELEMETRY_RUNTIME_DETAIL_MAX_BYTES):
            payload[field] = value
        else:
            _add_omission(omissions, "runtime_detail_preflight_limit")
    payload["artifacts"] = _project_unreadable_result_artifacts(
        full_artifacts,
        state=state,
        mirror_result=mirror_result,
        unreadable_result_json=unreadable_result_json,
    )
    payload["streams_mirrored"] = any(
        artifact.get("kind") in {"stdout", "stderr"} and "content_text" in artifact for artifact in full_artifacts
    )
    payload["artifact_bodies_mirrored"] = any(
        artifact.get("kind") == "artifact" and "content_text" in artifact for artifact in full_artifacts
    )
    redacted = redact_value(
        payload,
        sensitive_pointers=sensitive_fields,
        sensitive_values=sensitive_values,
        discover_pointer_values=False,
    )
    if unreadable_result_json is not None and mirror_result:
        redacted["result_json"] = unreadable_result_json
    _synchronize_json_artifact_text(redacted)
    payload.clear()
    payload.update(redacted)


def _synchronize_json_artifact_text(payload: dict[str, Any]) -> None:
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        return
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        if artifact.get("content_type") != "application/json" or "content_json" not in artifact:
            continue
        artifact["content_text"] = m_json_contract.dumps(
            artifact["content_json"],
            ensure_ascii=False,
            sort_keys=True,
        )


def _fit_payload(payload: dict[str, Any]) -> None:
    if _payload_fits(payload):
        return
    omissions = payload["telemetry"].setdefault("omissions", [])
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, list):
        while artifacts:
            artifacts.pop()
            _add_omission(omissions, "artifact_bodies")
            _synchronize_artifact_flags(payload)
            if _payload_fits(payload):
                return
    if "result" in payload or "result_json" in payload:
        payload.pop("result", None)
        payload.pop("result_json", None)
        payload.pop("result_unreadable", None)
        payload.pop("result_present", None)
        payload["result_mirrored"] = False
        if payload.get("source_result_present") is True:
            _add_omission(omissions, "result")
        if _payload_fits(payload):
            return
    if "input" in payload:
        payload.pop("input", None)
        payload["input_mirrored"] = False
        _add_omission(omissions, "input")
        if _payload_fits(payload):
            return
    payload.pop("artifacts", None)
    payload["streams_mirrored"] = False
    payload["artifact_bodies_mirrored"] = False
    error = payload.get("error")
    payload["error"] = _truncate_utf8(error, 1024) or None if isinstance(error, str) else None
    _add_omission(omissions, "diagnostic_text")
    if _payload_fits(payload):
        return

    telemetry = payload["telemetry"]
    nodes = telemetry.get("nodes")
    if isinstance(nodes, list) and nodes:
        nodes.clear()
        summary = telemetry.get("summary")
        if isinstance(summary, dict):
            summary["node_detail_count"] = 0
            summary["node_detail_count_truncated"] = True
        _add_omission(omissions, "node_details")
        if _payload_fits(payload):
            return

    _bound_overflow_metadata_fields(payload, omissions)
    if not _payload_fits(payload):
        raise ValueError("telemetry identity exceeds the sync event budget; local run id cannot be truncated safely")


def _synchronize_availability(payload: dict[str, Any]) -> None:
    availability = payload["telemetry"]["availability"]
    availability["input"] = bool(payload.get("input_mirrored"))
    availability["result"] = bool(payload.get("result_mirrored"))
    availability["streams"] = bool(payload.get("streams_mirrored"))
    availability["artifact_bodies"] = bool(payload.get("artifact_bodies_mirrored"))


def _synchronize_artifact_flags(payload: dict[str, Any]) -> None:
    artifacts = payload.get("artifacts")
    entries = artifacts if isinstance(artifacts, list) else []
    payload["streams_mirrored"] = any(
        isinstance(artifact, dict)
        and artifact.get("kind") in {"stdout", "stderr"}
        and ("content_text" in artifact or "content_json" in artifact)
        for artifact in entries
    )
    payload["artifact_bodies_mirrored"] = any(
        isinstance(artifact, dict)
        and artifact.get("kind") == "artifact"
        and ("content_text" in artifact or "content_json" in artifact)
        for artifact in entries
    )


def _source_result_present(state: dict[str, Any]) -> bool:
    explicit = state.get("result_present")
    return explicit if isinstance(explicit, bool) else state.get("result") is not None


def _result_redaction_source(
    state: dict[str, Any],
    sensitive_fields: tuple[str, ...],
) -> tuple[Any, bool, Any]:
    raw_result_json = state.get("result_json")
    if state.get("result_unreadable") is not True or not isinstance(raw_result_json, str):
        result = state.get("result")
        return result, _json_component_fits(result), _UNPARSED_RESULT
    if not _json_component_fits(raw_result_json):
        return raw_result_json, False, _UNPARSED_RESULT
    if not _has_result_pointer(sensitive_fields):
        return raw_result_json, True, _UNPARSED_RESULT
    try:
        parsed = json.loads(raw_result_json, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, RecursionError, ValueError):
        # A configured result pointer is a privacy boundary.  If the legacy
        # text cannot be structurally addressed, omit it instead of silently
        # bypassing that pointer.
        return raw_result_json, False, _UNPARSED_RESULT
    return parsed, True, parsed


def _redact_unreadable_result_json(
    parsed_result: Any,
    *,
    sensitive_fields: tuple[str, ...],
    sensitive_values: tuple[str, ...],
) -> str | None:
    """Structurally redact bounded legacy JSON without admitting it as a value."""

    try:
        wrapper = redact_value(
            {"result": parsed_result},
            sensitive_pointers=sensitive_fields,
            sensitive_values=sensitive_values,
            discover_pointer_values=False,
        )
        raw = json.dumps(
            wrapper["result"],
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (RecursionError, TypeError, ValueError):
        return None
    return raw if _json_component_fits(raw) else None


def _project_unreadable_result_artifacts(
    artifacts: list[dict[str, Any]],
    *,
    state: dict[str, Any],
    mirror_result: bool,
    unreadable_result_json: str | None,
) -> list[dict[str, Any]]:
    if state.get("result_unreadable") is not True:
        return artifacts
    projected: list[dict[str, Any]] = []
    for artifact in artifacts:
        if artifact.get("kind") != "result":
            projected.append(artifact)
        elif mirror_result and unreadable_result_json is not None:
            sanitized = dict(artifact)
            sanitized["content_text"] = unreadable_result_json
            sanitized.pop("content_json", None)
            projected.append(sanitized)
    return projected


def _has_result_pointer(sensitive_fields: tuple[str, ...]) -> bool:
    return any(pointer == "/result" or pointer.startswith("/result/") for pointer in sensitive_fields)


def _reject_json_constant(value: str) -> None:
    raise ValueError("non-finite JSON constant is not permitted: {}".format(value))


def _json_component_fits(value: Any, max_bytes: int = SYNC_EVENT_PAYLOAD_BUDGET) -> bool:
    try:
        return m_json_contract.compact_json_fits(value, max_bytes)
    except (RecursionError, ValueError):
        return False


def _add_omission(omissions: list[str], reason: str) -> None:
    if reason not in omissions:
        omissions.append(reason)


def _payload_fits(payload: dict[str, Any]) -> bool:
    try:
        return m_json_contract.compact_json_fits(payload, SYNC_EVENT_PAYLOAD_BUDGET)
    except (RecursionError, ValueError):
        return False


def _bounded_metadata_text(
    value: Any,
    limit: int = TELEMETRY_METADATA_TEXT_MAX_WIRE_BYTES,
    *,
    omissions: list[str] | None = None,
) -> str | None:
    """Return scalar text bounded by its compact JSON string-content bytes."""

    if value is None:
        return None
    if isinstance(value, str):
        text = value
    elif type(value) in {bool, int, float} and m_json_contract.is_json_value(value):
        text = str(value)
    else:
        if omissions is not None:
            _add_omission(omissions, "metadata_value_unavailable")
        return None

    result: list[str] = []
    used = 0
    changed = False
    consumed = 0
    for character in text:
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDFFF:
            character = "\ufffd"
            code_point = ord(character)
            changed = True
        if character in {'"', "\\", "\b", "\f", "\n", "\r", "\t"}:
            wire_size = 2
        elif code_point < 0x20:
            wire_size = 6
        else:
            wire_size = len(character.encode("utf-8"))
        if used + wire_size > limit:
            changed = True
            break
        result.append(character)
        used += wire_size
        consumed += 1
    if consumed < len(text):
        changed = True
    if changed and omissions is not None:
        _add_omission(omissions, "metadata_text_limit")
    return "".join(result)


def _bounded_text_byte_count(value: Any, max_bytes: int) -> tuple[int, bool]:
    """Count at most ``max_bytes`` of UTF-8 text and report an inexact count."""

    if not isinstance(value, str):
        return 0, False
    measured = 0
    consumed = 0
    for character in value:
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDFFF:
            return measured, True
        character_bytes = len(character.encode("utf-8"))
        if measured + character_bytes > max_bytes:
            return measured, True
        measured += character_bytes
        consumed += 1
        if measured == max_bytes and consumed < len(value):
            return measured, True
    return measured, consumed < len(value)


def _bound_overflow_metadata_fields(payload: dict[str, Any], omissions: list[str]) -> None:
    """Bound optional metadata fields after richer projections have been removed."""

    changed = False
    for field in (
        "object_name",
        "object_display_name",
        "local_object_name",
        "object_id",
        "object_version_id",
        "object_version",
        "owner_id",
        "remote_object_id",
        "remote_version_id",
        "entrypoint",
        "env",
        "status",
        "started_at",
        "finished_at",
        "created_at",
    ):
        value = payload.get(field)
        if value is None or type(value) in {bool, int, float} and m_json_contract.is_json_value(value):
            continue
        if type(value) is str:
            bounded = _bounded_metadata_text(value, omissions=omissions)
            if bounded == value:
                continue
            payload[field] = bounded
            changed = True
        else:
            payload[field] = None
            _add_omission(omissions, "metadata_value_unavailable")
            changed = True
    if changed:
        _add_omission(omissions, "metadata_fields")


def _duration_ms(started_at: Any, finished_at: Any) -> int | None:
    if not isinstance(started_at, str) or not isinstance(finished_at, str):
        return None
    if len(started_at) > 128 or len(finished_at) > 128:
        return None
    try:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        finished = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0, int((finished - started).total_seconds() * 1000))


def _error_type(error: Any) -> str | None:
    if not isinstance(error, str) or not error:
        return None
    bounded_tail = error[-8192:]
    if not bounded_tail.strip():
        return "Error" if len(error) > len(bounded_tail) else None
    for line in reversed(bounded_tail.splitlines()):
        candidate = line.strip().split(":", 1)[0].strip()
        leaf = candidate.rsplit(".", 1)[-1]
        if (
            candidate
            and all(part.isidentifier() for part in candidate.split("."))
            and leaf.endswith(("Error", "Exception", "Warning", "Exit", "Interrupt"))
        ):
            return candidate[:200]
    return "Error"


def _bounded_utf8(value: str, max_bytes: int) -> tuple[str, bool]:
    bounded, _, truncated = _bounded_utf8_with_size(value, max_bytes)
    return bounded, truncated


def _bounded_utf8_with_size(value: str, max_bytes: int) -> tuple[str, int, bool]:
    """Return sanitized bounded text, an exact-or-lower-bound size, and truncation."""

    if max_bytes < 0:
        raise ValueError("max_bytes must be nonnegative")
    prefix, size, truncated = _utf8_prefix_with_size(value, max_bytes)
    if not truncated:
        return prefix, size, False
    suffix = "\n[truncated]"
    suffix_size = len(suffix.encode("utf-8"))
    if suffix_size > max_bytes:
        return _utf8_prefix_with_size(suffix, max_bytes)[0], size, True
    prefix, _, _ = _utf8_prefix_with_size(value, max_bytes - suffix_size)
    return prefix + suffix, size, True


def _utf8_prefix(value: str, max_bytes: int) -> tuple[str, bool]:
    prefix, _, truncated = _utf8_prefix_with_size(value, max_bytes)
    return prefix, truncated


def _utf8_prefix_with_size(value: str, max_bytes: int) -> tuple[str, int, bool]:
    """Measure only through overflow and replace non-UTF-8 surrogate code units."""

    if max_bytes < 0:
        raise ValueError("max_bytes must be nonnegative")
    chunks: list[str] = []
    remaining = max_bytes
    consumed_characters = 0
    total_characters = len(value)
    while consumed_characters < total_characters:
        if remaining == 0:
            return "".join(chunks), max_bytes + 1, True
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


def _truncate_utf8(value: str, max_bytes: int) -> str:
    return _bounded_utf8(value, max_bytes)[0]


def count_local_artifacts_bounded(state: dict[str, Any]) -> tuple[int, bool, bool]:
    """Return a bounded artifact count, cap status, and directory availability."""

    raw_path = state.get("artifacts_dir")
    file_count = 0
    truncated = False
    available = True
    if isinstance(raw_path, str) and raw_path:
        result = count_regular_files_bounded(Path(raw_path))
        file_count = result.count
        truncated = result.truncated
        available = result.available
    synthesized_count = (
        int(_source_result_present(state)) + int(bool(state.get("stdout"))) + int(bool(state.get("stderr")))
    )
    return file_count + synthesized_count, truncated, available


def count_local_artifacts(state: dict[str, Any]) -> int:
    """Return the backward-compatible count from the bounded artifact scan."""

    return count_local_artifacts_bounded(state)[0]


def parse_sensitive_fields_env(raw: str | None) -> tuple[str, ...]:
    """Parse the daemon environment's JSON-array sensitive-field setting."""

    if raw is None or not raw.strip():
        return ()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("SPL_DAEMON_TELEMETRY_SENSITIVE_FIELDS must be a JSON array of JSON Pointers") from exc
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("SPL_DAEMON_TELEMETRY_SENSITIVE_FIELDS must be a JSON array of JSON Pointers")
    return normalize_json_pointers(value)
