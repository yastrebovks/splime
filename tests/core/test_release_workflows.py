"""Static fail-closed contracts for the cross-repository release workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


SPL_ROOT = Path(__file__).resolve().parents[2]
PUBLISH_WORKFLOW = SPL_ROOT / ".github" / "workflows" / "publish-to-pypi.yml"
VERIFY_WORKFLOW = SPL_ROOT / ".github" / "workflows" / "verify-release-assets.yml"


def _workflow(path: Path) -> dict[str, Any]:
    payload = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(payload, dict)
    return payload


def test_publish_workflow_uses_external_source_and_built_evidence() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    jobs = workflow["jobs"]
    text = PUBLISH_WORKFLOW.read_text(encoding="utf-8")

    assert set(jobs) == {
        "source-chain",
        "test",
        "build",
        "install-test",
        "publish-testpypi",
        "publish-pypi",
    }
    assert set(jobs["build"]["needs"]) == {"source-chain", "test"}
    assert "--emit-source-evidence" in text
    assert "artifacts/source-release-manifest.json" in text
    assert "--stage source" in text
    assert "release-source-evidence" in text
    assert "--emit-built-evidence" in text
    assert "artifacts/release-manifest.json" in text
    assert "--stage built" in text
    assert "release-built-bom-and-artifacts" in text
    assert "--new-declaration" not in text
    assert "--manifest-only" not in text


def test_publish_workflow_builds_every_authoritative_component_from_pinned_source() -> None:
    text = PUBLISH_WORKFLOW.read_text(encoding="utf-8")
    signing_key = SPL_ROOT / ".github" / "release-signing-public-key.asc"

    assert "verify-tag" in text
    assert "secrets.SPL_RELEASE_SIGNING_PUBLIC_KEY" not in text
    assert "github.workflow_sha" in text
    assert signing_key.is_file()
    assert "31E24377474710AF950C81C6B8C5D1937087FA85" in text
    assert "SPL_RELEASE_GITLAB_KNOWN_HOSTS" in text
    assert 'checkout --detach "${SERVER_COMMIT}"' in text
    assert 'checkout --detach "${CONSOLE_COMMIT}"' in text
    assert "tools/build_release_artifacts.py" in text
    assert "python -m build --wheel" in text
    assert "python -m tools.build_console_artifact" in text
    assert "reproducibility/python" in text
    assert "reproducibility/server" in text
    assert "reproducibility/console" in text
    for component in ("framework", "daemon", "server", "console"):
        assert f'--component-artifact "{component}=' in text


def test_publish_workflow_makes_the_public_cookbook_contract_mandatory() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    test_job = workflow["jobs"]["test"]
    text = PUBLISH_WORKFLOW.read_text(encoding="utf-8")

    assert inputs["cookbook-url"]["required"] == "true"
    assert inputs["cookbook-sha256"]["required"] == "true"
    assert set(workflow["on"]) == {"push", "release", "workflow_dispatch"}
    assert workflow["on"]["push"]["branches"] == ["main"]
    assert workflow["on"]["push"]["tags"] == ["v*.*.*"]
    assert workflow["on"]["release"]["types"] == ["published"]
    assert workflow["env"]["RELEASE_TAG"] == (
        "${{ (github.event_name == 'push' && github.ref_type == 'branch' && 'v0.4.6') || "
        "inputs.release-tag || github.event.release.tag_name || github.ref_name }}"
    )
    assert workflow["env"]["PUBLIC_COOKBOOK_URL"] == (
        "${{ inputs.cookbook-url || 'https://splime.io/downloads/splime-cookbook.ipynb' }}"
    )
    assert workflow["env"]["PUBLIC_COOKBOOK_SHA256"] == (
        "${{ inputs.cookbook-sha256 || '15ae5b809223426f26b998fd3f5b6aef1bf10e9d0b6e74dfdeef2572405f368d' }}"
    )
    checkout = test_job["steps"][0]["with"]
    assert checkout["ref"] == "${{ env.RELEASE_TAG }}"
    assert "Fetch and verify the reviewed canonical public cookbook" in text
    assert "--proto '=https' --proto-redir '=https'" in text
    assert "sha256sum --check --strict" in text
    assert "SPL_RELEASE_COOKBOOK_PATH=" in text
    assert test_job["steps"][-1]["run"] == 'python -m pytest -m "not smoke" -q'
    assert workflow["jobs"]["publish-testpypi"]["if"] == (
        "(github.event_name == 'push' && github.ref_type == 'tag') || "
        "(github.event_name == 'workflow_dispatch' && inputs.target == 'testpypi')"
    )
    assert workflow["jobs"]["publish-pypi"]["if"] == (
        "github.event_name == 'release' || "
        "(github.event_name == 'push' && github.ref_type == 'branch' && github.ref_name == 'main') || "
        "(github.event_name == 'workflow_dispatch' && inputs.target == 'pypi')"
    )


def test_publish_jobs_receive_only_the_verified_reviewed_bundle() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    jobs = workflow["jobs"]
    text = PUBLISH_WORKFLOW.read_text(encoding="utf-8")

    for name, environment in (
        ("publish-testpypi", "testpypi"),
        ("publish-pypi", "pypi"),
    ):
        job = jobs[name]
        assert job["needs"] == ["install-test"]
        assert job["environment"]["name"] == environment
        assert job["permissions"]["id-token"] == "write"
        rendered_steps = "\n".join(str(step) for step in job["steps"])
        assert "release-built-bom-and-artifacts" in rendered_steps
        assert "artifacts/python/" in rendered_steps
        assert "evidence" in rendered_steps and "built" in rendered_steps
    assert "skip-existing" not in text
    assert "packages-dir: dist/" not in text
    assert "Observe exact PyPI publication handoff" in text
    assert "pypi-publication-evidence.json" in text
    assert "PyPI filename set does not match the built BOM" in text
    assert "PyPI hashes do not match the built BOM" in text
    assert "PyPI bytes do not match the built BOM" in text
    assert 'parsed.scheme != "https"' in text


def test_published_verification_downloads_an_explicit_external_manifest() -> None:
    workflow = _workflow(VERIFY_WORKFLOW)
    jobs = workflow["jobs"]
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    text = VERIFY_WORKFLOW.read_text(encoding="utf-8")

    assert set(inputs) == {
        "source-repository",
        "release-tag",
        "release-manifest-url",
        "release-manifest-sha256",
        "server-version-url",
        "server-ready-url",
    }
    assert all(value["required"] == "true" for value in inputs.values())
    checkout = jobs["verify-urls"]["steps"][0]["with"]
    assert checkout["repository"] == "${{ inputs.source-repository }}"
    assert checkout["ref"] == "${{ inputs.release-tag }}"
    assert "RELEASE_MANIFEST_URL" in text
    assert "RELEASE_MANIFEST_SHA256" in text
    assert 'parsed.scheme != "https"' in text
    assert "hashlib.sha256(payload).hexdigest() != expected" in text
    assert "--manifest release-evidence/release-manifest.json" in text
    assert "--source-repository ." in text
    assert "git verify-tag" in text
    assert "verified-published-release-manifest" in text
    assert 'd["manifest_digest"]' in text
    assert 'docker pull "$IMAGE_REF"' in text
    assert 'docker run -d --name splime-release-smoke "$IMAGE_REF"' in text
    assert jobs["clean-install"]["needs"] == "verify-urls"
    assert jobs["docker-smoke"]["needs"] == "verify-urls"


def test_release_workflows_are_valid_static_job_graphs() -> None:
    for path in (PUBLISH_WORKFLOW, VERIFY_WORKFLOW):
        workflow = _workflow(path)
        assert set(workflow) >= {"name", "on", "jobs"}
        assert workflow["jobs"]
        for job in workflow["jobs"].values():
            assert isinstance(job.get("steps"), list) and job["steps"]
            for step in job["steps"]:
                assert ("run" in step) ^ ("uses" in step)
