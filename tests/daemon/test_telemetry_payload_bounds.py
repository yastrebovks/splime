"""Adversarial work and allocation bounds for daemon telemetry projection."""

from __future__ import annotations

import json
from typing import Any

import pytest

from spl.core import json_contract as m_json_contract
from spl.core.redaction import REDACTED_VALUE
from spl.daemon import telemetry as m_telemetry
from spl.daemon.telemetry import (
    SYNC_EVENT_PAYLOAD_BUDGET,
    TELEMETRY_ERROR_MAX_BYTES,
    TELEMETRY_FULL_ERROR_MAX_BYTES,
    TELEMETRY_METADATA_STREAM_SIZE_MAX_BYTES,
    TELEMETRY_METADATA_TEXT_MAX_WIRE_BYTES,
    TELEMETRY_NODE_DETAIL_LIMIT,
    TELEMETRY_SENSITIVE_VALUE_LIMIT,
    TELEMETRY_STREAM_MAX_BYTES,
    TelemetryPolicy,
)


class _TraversalBomb:
    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        raise AssertionError("an omitted telemetry tail was deep-copied")

    def __str__(self) -> str:
        raise AssertionError("an omitted telemetry tail was stringified")


class _TailSliceBomb(str):
    """Reject slice-based traversal after a caller's declared byte cap."""

    forbidden_slice_start: int

    def __new__(cls, value: str, *, forbidden_slice_start: int) -> _TailSliceBomb:
        instance = super().__new__(cls, value)
        instance.forbidden_slice_start = forbidden_slice_start
        return instance

    def __getitem__(self, key: int | slice) -> str:
        if isinstance(key, slice) and (key.start or 0) >= self.forbidden_slice_start:
            raise AssertionError("oversized diagnostic stream tail was traversed")
        return super().__getitem__(key)


def _state() -> dict[str, Any]:
    return {
        "id": "bounded-telemetry-run",
        "object": "bounded_object",
        "status": "failed",
        "input": {"args": [], "kwargs": {}},
        "result": {"ok": True},
        "result_present": True,
        "error": "ValueError: ordinary failure",
        "stdout": "ordinary stdout",
        "stderr": "ordinary stderr",
        "created_at": "2026-07-15T10:00:00+00:00",
        "started_at": "2026-07-15T10:00:01+00:00",
        "finished_at": "2026-07-15T10:00:02+00:00",
        "artifacts_dir": "",
        "manifest": {"pipeline": {}, "nodes": {}, "edges": []},
    }


def _label() -> dict[str, Any]:
    return {
        "display_name": "bounded_object",
        "local_name": "bounded_object",
        "owner_id": None,
        "remote_object_id": None,
        "remote_version_id": None,
    }


def test_metadata_never_traverses_raw_input_or_result_values() -> None:
    bomb = _TraversalBomb()
    state = _state()
    state["input"] = {"args": [], "kwargs": {}, "private_tail": bomb}
    state["result"] = bomb

    payload = TelemetryPolicy().build_local_run_payload(state, _label())

    assert payload["telemetry_level"] == "metadata"
    assert "input" not in payload
    assert "result" not in payload
    assert payload["input_mirrored"] is False
    assert payload["result_mirrored"] is False


def test_full_telemetry_omits_oversized_components_before_unvisited_tails() -> None:
    bomb = _TraversalBomb()
    oversized = "x" * (SYNC_EVENT_PAYLOAD_BUDGET + 1)
    state = _state()
    state["input"] = {
        "args": [],
        "kwargs": {},
        "oversized": oversized,
        "unvisited": bomb,
    }
    state["result"] = [oversized, bomb]
    artifacts = [
        {
            "name": "oversized.txt",
            "kind": "artifact",
            "content_type": "text/plain",
            "content_text": oversized,
        },
        {"unvisited": bomb},
    ]

    payload = TelemetryPolicy("full").build_local_run_payload(
        state,
        _label(),
        full_artifacts=artifacts,
    )

    assert "input" not in payload
    assert "result" not in payload
    assert payload.get("artifacts") == []
    assert payload["input_mirrored"] is False
    assert payload["result_mirrored"] is False
    assert payload["artifact_bodies_mirrored"] is False
    assert set(payload["telemetry"]["omissions"]) >= {
        "input_preflight_limit",
        "result_preflight_limit",
        "artifact_bodies_preflight_limit",
        "sensitive_value_discovery_limit",
    }
    assert len(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")) <= (SYNC_EVENT_PAYLOAD_BUDGET)


def test_unreadable_result_json_uses_the_same_preflight_gate() -> None:
    state = _state()
    state.update(
        {
            "result": None,
            "result_present": True,
            "result_unreadable": True,
            "result_json": "x" * (SYNC_EVENT_PAYLOAD_BUDGET + 1),
        }
    )

    payload = TelemetryPolicy("full").build_local_run_payload(state, _label())

    assert payload["source_result_present"] is True
    assert payload["result_mirrored"] is False
    assert "result" not in payload
    assert "result_json" not in payload
    assert "result_preflight_limit" in payload["telemetry"]["omissions"]


def test_unreadable_contract_invalid_result_honors_structural_result_pointer() -> None:
    secret = "UNREADABLE_POINTER_SECRET"
    raw_result = json.dumps(
        {
            "private": secret,
            "repeated": secret,
            "unsafe": (1 << 53),
        },
        separators=(",", ":"),
    )
    state = _state()
    state.update(
        {
            "result": None,
            "result_present": True,
            "result_unreadable": True,
            "result_json": raw_result,
        }
    )
    artifacts = [
        {
            "name": "result.json",
            "kind": "result",
            "content_type": "application/json",
            "content_text": raw_result,
        }
    ]

    payload = TelemetryPolicy("full", ("/result/private",)).build_local_run_payload(
        state,
        _label(),
        full_artifacts=artifacts,
    )

    wire = m_json_contract.dumps(payload, ensure_ascii=False, sort_keys=True)
    mirrored_result = json.loads(payload["result_json"])
    result_artifact = next(artifact for artifact in payload["artifacts"] if artifact["kind"] == "result")
    assert secret not in wire
    assert mirrored_result == {
        "private": REDACTED_VALUE,
        "repeated": REDACTED_VALUE,
        "unsafe": (1 << 53),
    }
    assert json.loads(result_artifact["content_text"]) == mirrored_result
    assert payload["result"] is None
    assert payload["result_mirrored"] is True


def test_unreadable_result_preserves_exact_raw_json_without_result_pointer() -> None:
    raw_result = '{ "unsafe": 9007199254740992, "ordinary": true }'
    state = _state()
    state.update(
        {
            "result": None,
            "result_present": True,
            "result_unreadable": True,
            "result_json": raw_result,
        }
    )

    payload = TelemetryPolicy("full", ("/input/kwargs/private",)).build_local_run_payload(
        state,
        _label(),
    )

    assert payload["result_json"] == raw_result
    assert payload["result_mirrored"] is True


def test_unreadable_result_is_omitted_when_configured_pointer_cannot_be_applied() -> None:
    state = _state()
    state.update(
        {
            "result": None,
            "result_present": True,
            "result_unreadable": True,
            "result_json": "NaN",
        }
    )
    artifacts = [
        {
            "name": "result.json",
            "kind": "result",
            "content_type": "application/json",
            "content_text": "NaN",
        }
    ]

    payload = TelemetryPolicy("full", ("/result/private",)).build_local_run_payload(
        state,
        _label(),
        full_artifacts=artifacts,
    )

    assert payload["result_mirrored"] is False
    assert "result" not in payload
    assert "result_json" not in payload
    assert not any(artifact.get("kind") == "result" for artifact in payload.get("artifacts", []))
    assert "result_preflight_limit" in payload["telemetry"]["omissions"]


def test_unreadable_result_redaction_expansion_is_omitted_before_wire_admission() -> None:
    repeated = ["x"] * 20_000
    raw_result = json.dumps(
        {"private": "x", "repeated": repeated, "unsafe": (1 << 53)},
        separators=(",", ":"),
    )
    state = _state()
    state.update(
        {
            "result": None,
            "result_present": True,
            "result_unreadable": True,
            "result_json": raw_result,
        }
    )

    payload = TelemetryPolicy("full", ("/result/private",)).build_local_run_payload(state, _label())

    assert payload["result_mirrored"] is False
    assert "result_json" not in payload
    assert set(payload["telemetry"]["omissions"]) >= {
        "result_preflight_limit",
        "result_redaction_limit",
    }
    assert len(m_json_contract.dumps(payload).encode("utf-8")) <= SYNC_EVENT_PAYLOAD_BUDGET


def test_diagnostic_text_is_bounded_before_redaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tail = "UNVISITED_DIAGNOSTIC_TAIL"
    state = _state()
    state["error"] = "ValueError: " + ("e" * (TELEMETRY_ERROR_MAX_BYTES * 4)) + tail
    state["stdout"] = ("o" * (TELEMETRY_STREAM_MAX_BYTES * 4)) + tail
    state["stderr"] = ("s" * (TELEMETRY_STREAM_MAX_BYTES * 4)) + tail
    original_redact_text = m_telemetry.redact_text
    observed_sizes: list[int] = []

    def observe_redaction(text: str, *, sensitive_values: tuple[str, ...] = ()) -> str:
        observed_sizes.append(len(text.encode("utf-8")))
        return original_redact_text(text, sensitive_values=sensitive_values)

    monkeypatch.setattr(m_telemetry, "redact_text", observe_redaction)

    payload = TelemetryPolicy("diagnostic").build_local_run_payload(state, _label())

    assert observed_sizes[0] <= TELEMETRY_ERROR_MAX_BYTES
    assert all(size <= TELEMETRY_STREAM_MAX_BYTES for size in observed_sizes[1:])
    assert tail not in json.dumps(payload, sort_keys=True)
    assert set(payload["telemetry"]["omissions"]) >= {
        "error_text_limit",
        "stream_text_limit",
    }


def test_diagnostic_stream_size_is_a_bounded_lower_bound() -> None:
    cap = TELEMETRY_STREAM_MAX_BYTES
    state = _state()
    state["stdout"] = _TailSliceBomb(
        "x" * (cap * 4),
        forbidden_slice_start=cap,
    )
    state["stderr"] = ""

    payload = TelemetryPolicy("diagnostic").build_local_run_payload(state, _label())
    artifact = payload["artifacts"][0]

    assert artifact["name"] == "stdout.txt"
    assert artifact["size_bytes"] == cap + 1
    assert artifact["truncated"] is True
    assert len(artifact["content_text"].encode("utf-8")) <= cap
    assert "stream_text_limit" in payload["telemetry"]["omissions"]


def test_diagnostic_text_sanitizes_lone_surrogates() -> None:
    state = _state()
    state["error"] = "ValueError: before\ud800after"
    state["stdout"] = "before\ud800after"
    state["stderr"] = ""

    payload = TelemetryPolicy("diagnostic").build_local_run_payload(state, _label())
    artifact = payload["artifacts"][0]
    sanitized = "before\ufffdafter"

    assert payload["error"] == f"ValueError: {sanitized}"
    assert artifact["content_text"] == sanitized
    assert artifact["size_bytes"] == len(sanitized.encode("utf-8"))
    assert artifact["truncated"] is False
    assert "\ud800" not in m_json_contract.dumps(payload, ensure_ascii=False)


def test_full_error_is_bounded_before_structural_redaction() -> None:
    tail = "UNVISITED_FULL_ERROR_TAIL"
    state = _state()
    state["error"] = "ValueError: " + ("e" * (TELEMETRY_FULL_ERROR_MAX_BYTES * 4)) + tail

    payload = TelemetryPolicy("full").build_local_run_payload(state, _label())

    assert len(payload["error"].encode("utf-8")) <= TELEMETRY_FULL_ERROR_MAX_BYTES
    assert tail not in payload["error"]
    assert "error_text_limit" in payload["telemetry"]["omissions"]


def test_sensitive_value_discovery_is_capped_before_replacement_work() -> None:
    state = _state()
    secrets = {
        "token_{:03d}".format(index): "secret-value-{:03d}".format(index)
        for index in range(TELEMETRY_SENSITIVE_VALUE_LIMIT + 20)
    }
    state["input"] = {"args": [], "kwargs": secrets}

    payload = TelemetryPolicy("full").build_local_run_payload(state, _label())
    wire = json.dumps(payload, sort_keys=True)

    assert "sensitive_value_discovery_limit" in payload["telemetry"]["omissions"]
    assert "secret-value-000" not in wire
    assert "secret-value-275" not in wire
    assert wire.count("[REDACTED]") >= len(secrets)


def test_metadata_node_projection_never_reaches_the_oversized_mapping_tail() -> None:
    state = _state()
    bomb = _TraversalBomb()
    nodes: dict[Any, Any] = {"node-{:03d}".format(index): {} for index in range(TELEMETRY_NODE_DETAIL_LIMIT)}
    nodes[bomb] = {"name": bomb}
    state["manifest"] = {"pipeline": {}, "nodes": nodes, "edges": []}

    payload = TelemetryPolicy().build_local_run_payload(state, _label())
    telemetry = payload["telemetry"]

    assert telemetry["summary"]["node_count"] == TELEMETRY_NODE_DETAIL_LIMIT + 1
    assert telemetry["summary"]["node_detail_count"] == TELEMETRY_NODE_DETAIL_LIMIT
    assert telemetry["summary"]["node_detail_count_truncated"] is True
    assert "node_detail_limit" in telemetry["omissions"]
    assert [node["id"] for node in telemetry["nodes"]] == [
        "node-{:03d}".format(index) for index in range(TELEMETRY_NODE_DETAIL_LIMIT)
    ]


def test_metadata_stream_sizes_are_bounded_lower_bounds_with_explicit_honesty() -> None:
    state = _state()
    state["stdout"] = "x" * (TELEMETRY_METADATA_STREAM_SIZE_MAX_BYTES + 1) + "\ud800"
    state["stderr"] = "\N{SNOWMAN}" * 100

    payload = TelemetryPolicy().build_local_run_payload(state, _label())
    summary = payload["telemetry"]["summary"]

    assert summary["stdout_bytes"] == TELEMETRY_METADATA_STREAM_SIZE_MAX_BYTES
    assert summary["stdout_bytes_truncated"] is True
    assert summary["stderr_bytes"] == len(state["stderr"].encode("utf-8"))
    assert "stderr_bytes_truncated" not in summary
    assert "stream_size_limit" in payload["telemetry"]["omissions"]


def test_metadata_text_limits_apply_to_compact_json_wire_bytes() -> None:
    state = _state()
    escaped = "\x01" * TELEMETRY_METADATA_TEXT_MAX_WIRE_BYTES
    state["manifest"] = {
        "pipeline": {"content_hash": escaped},
        "nodes": {
            "node-{:03d}".format(index): {
                "id": escaped,
                "alias": escaped,
                "name": escaped,
                "kind": escaped,
                "status": escaped,
                "fingerprint": {"sha256": escaped},
            }
            for index in range(TELEMETRY_NODE_DETAIL_LIMIT)
        },
        "edges": [],
    }

    payload = TelemetryPolicy().build_local_run_payload(state, _label())
    wire = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")

    assert len(payload["telemetry"]["nodes"]) == TELEMETRY_NODE_DETAIL_LIMIT
    assert "metadata_text_limit" in payload["telemetry"]["omissions"]
    assert len(wire) <= SYNC_EVENT_PAYLOAD_BUDGET
    assert all(
        len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        <= TELEMETRY_METADATA_TEXT_MAX_WIRE_BYTES + 2
        for node in payload["telemetry"]["nodes"]
        for value in node.values()
        if isinstance(value, str)
    )


def test_oversized_optional_metadata_degrades_without_preventing_sync() -> None:
    state = _state()
    oversized = "\x01" * (SYNC_EVENT_PAYLOAD_BUDGET + 1)
    label = _label()
    label["display_name"] = oversized
    label["local_name"] = oversized

    payload = TelemetryPolicy().build_local_run_payload(state, label)
    wire = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")

    assert payload["id"] == state["id"]
    assert payload["status"] == state["status"]
    assert set(payload["telemetry"]["omissions"]) >= {
        "metadata_fields",
        "metadata_text_limit",
    }
    assert len(wire) <= SYNC_EVENT_PAYLOAD_BUDGET
