"""Privacy and identity contracts for central execution-manifest evidence."""

from __future__ import annotations

from copy import deepcopy

import pytest

from spl.core import manifest as m_manifest


def _terminal_manifest() -> dict:
    return {
        "schema_version": 1,
        "run_id": "local-run-1",
        "status": "succeeded",
        "finished_at": "2026-07-29T12:00:00+00:00",
        "pipeline": {
            "object_version_id": "local-version-1",
            "content_hash": "c" * 64,
        },
        "inputs": {"secret": "must-not-cross-in-evidence"},
        "nodes": {
            "node-a": {
                "id": "node-a",
                "alias": "producer",
                "outputs": {
                    "result": {
                        "kind": "artifact",
                        "sha256": "a" * 64,
                        "ref": {
                            "uri": "artifacts/result.bin",
                            "sha256": "a" * 64,
                            "size": 7,
                        },
                    }
                },
            }
        },
        "edges": [],
    }


def test_terminal_manifest_evidence_is_deterministic_and_allowlisted() -> None:
    manifest = _terminal_manifest()
    reordered = {key: manifest[key] for key in reversed(tuple(manifest))}

    first = m_manifest.terminal_manifest_evidence(manifest)
    second = m_manifest.terminal_manifest_evidence(reordered)

    assert first == second
    assert set(first) == {
        "schema_version",
        "digest_sha256",
        "captured_at",
        "summary",
    }
    assert first["summary"] == {
        "node_count": 1,
        "edge_count": 0,
        "pipeline_content_hash": "c" * 64,
        "object_version_id": "local-version-1",
    }
    assert "must-not-cross-in-evidence" not in repr(first)


def test_terminal_manifest_evidence_rejects_nonterminal_or_incomplete_state() -> None:
    active = _terminal_manifest()
    active["status"] = "running"
    with pytest.raises(ValueError, match="terminal"):
        m_manifest.terminal_manifest_evidence(active)

    incomplete = _terminal_manifest()
    incomplete["nodes"] = None
    with pytest.raises(ValueError, match="structure"):
        m_manifest.terminal_manifest_evidence(incomplete)


def test_artifact_producer_requires_exact_ref_digest_and_size() -> None:
    producers = m_manifest.manifest_artifact_producers(_terminal_manifest())

    assert producers == {
        ("result.bin", "a" * 64, 7): {
            "node_id": "node-a",
            "alias": "producer",
            "output_port": "result",
        }
    }
    assert ("result.bin", "b" * 64, 7) not in producers
    assert ("result.bin", "a" * 64, 8) not in producers


def test_artifact_producer_omits_ambiguous_or_inferred_bindings() -> None:
    manifest = _terminal_manifest()
    duplicate = deepcopy(manifest["nodes"]["node-a"])
    duplicate["id"] = "node-b"
    duplicate["alias"] = "other"
    manifest["nodes"]["node-b"] = duplicate
    manifest["nodes"]["node-c"] = {
        "id": "node-c",
        "alias": "filename-only",
        "outputs": {
            "result": {
                "kind": "artifact",
                "ref": {
                    "uri": "elsewhere/result.bin",
                    "sha256": "b" * 64,
                    "size": 7,
                },
            }
        },
    }

    producers = m_manifest.manifest_artifact_producers(manifest)

    assert ("result.bin", "a" * 64, 7) not in producers
    assert ("result.bin", "b" * 64, 7) not in producers
