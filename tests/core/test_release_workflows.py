"""Static fail-closed contracts for the SPLime package release workflows."""

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


def test_publish_workflow_is_package_only() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    jobs = workflow["jobs"]
    text = PUBLISH_WORKFLOW.read_text(encoding="utf-8")

    assert set(jobs) == {
        "build",
        "install-test",
        "publish-testpypi",
        "publish-pypi",
    }
    assert "needs" not in jobs["build"]
    assert "spl-server" not in text
    assert "spl-frontend" not in text
    assert "SPL_RELEASE_GITLAB" not in text
    assert "source-chain" not in text
    assert "build_console_artifact" not in text


def test_publish_workflow_verifies_the_exact_signed_tag() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    text = PUBLISH_WORKFLOW.read_text(encoding="utf-8")
    signing_key = SPL_ROOT / ".github" / "release-signing-public-key.asc"

    assert signing_key.is_file()
    assert "github.workflow_sha" in text
    assert "git verify-tag" in text
    assert "31E24377474710AF950C81C6B8C5D1937087FA85" in text
    assert "secrets.SPL_RELEASE_SIGNING_PUBLIC_KEY" not in text
    checkout = workflow["jobs"]["build"]["steps"][1]["with"]
    assert checkout["ref"] == "${{ env.RELEASE_TAG }}"
    assert checkout["fetch-depth"] == "0"
    assert checkout["persist-credentials"] == "false"


def test_publish_workflow_supports_automatic_and_manual_publication() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]

    assert set(workflow["on"]) == {"push", "release", "workflow_dispatch"}
    assert workflow["on"]["push"]["branches"] == ["main"]
    assert workflow["on"]["push"]["tags"] == ["v*.*.*"]
    assert workflow["on"]["release"]["types"] == ["published"]
    assert set(inputs) == {"release-tag", "target"}
    assert inputs["release-tag"]["required"] == "true"
    assert inputs["target"]["options"] == ["testpypi", "pypi"]
    assert workflow["env"]["RELEASE_TAG"] == (
        "${{ (github.event_name == 'push' && github.ref_type == 'branch' && 'v0.4.6') || "
        "inputs.release-tag || github.event.release.tag_name || github.ref_name }}"
    )
    assert workflow["jobs"]["publish-testpypi"]["if"] == (
        "(github.event_name == 'push' && github.ref_type == 'tag') || "
        "(github.event_name == 'workflow_dispatch' && inputs.target == 'testpypi')"
    )
    assert workflow["jobs"]["publish-pypi"]["if"] == (
        "github.event_name == 'release' || "
        "(github.event_name == 'push' && github.ref_type == 'branch' && github.ref_name == 'main') || "
        "(github.event_name == 'workflow_dispatch' && inputs.target == 'pypi')"
    )


def test_publication_does_not_repeat_the_already_passed_package_suite() -> None:
    text = PUBLISH_WORKFLOW.read_text(encoding="utf-8")

    assert "pytest" not in text
    assert "ruff" not in text
    assert "mypy" not in text
    assert "SPL_RELEASE_COOKBOOK" not in text


def test_build_is_reproducible_and_every_install_uses_the_exact_wheel() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    jobs = workflow["jobs"]
    text = PUBLISH_WORKFLOW.read_text(encoding="utf-8")

    assert "tools/build_release_artifacts.py" in text
    assert "dist-reproducibility" in text
    assert "wheel/sdist build is not reproducible" in text
    assert "python -m twine check" in text
    assert "python-artifacts.sha256" in text
    assert "splime-python-release" in text
    assert jobs["install-test"]["needs"] == ["build"]
    assert jobs["install-test"]["strategy"]["matrix"]["os"] == [
        "ubuntu-latest",
        "macos-latest",
        "windows-latest",
    ]


def test_publish_jobs_use_oidc_and_only_the_verified_package_directory() -> None:
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
        assert "splime-python-release" in rendered_steps
        assert "sha256sum --check --strict" in rendered_steps
        assert "packages-dir" in rendered_steps
        assert "package/dist/" in rendered_steps
    assert "skip-existing" not in text
    assert "Verify PyPI publication" in text


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
