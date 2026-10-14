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
        "build-sdk",
        "sdk-install-test",
        "publish-sdk",
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

    assert "verify-tag" in text
    assert "SPL_RELEASE_SIGNING_PUBLIC_KEY" in text
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


def test_coordinated_workflow_requires_the_public_cookbook_contract() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    test_job = workflow["jobs"]["test"]
    text = PUBLISH_WORKFLOW.read_text(encoding="utf-8")

    assert inputs["cookbook-url"]["required"] == "false"
    assert inputs["cookbook-sha256"]["required"] == "false"
    assert set(workflow["on"]) == {"workflow_dispatch"}
    assert workflow["env"]["PUBLIC_COOKBOOK_URL"]
    assert workflow["env"]["PUBLIC_COOKBOOK_SHA256"]
    checkout = test_job["steps"][0]["with"]
    assert checkout["ref"] == "${{ env.RELEASE_TAG }}"
    cookbook = next(
        step["run"]
        for step in test_job["steps"]
        if step.get("name") == "Fetch and verify the reviewed canonical public cookbook"
    )
    assert 'test -n "${PUBLIC_COOKBOOK_URL}"' in cookbook
    assert 'test -n "${PUBLIC_COOKBOOK_SHA256}"' in cookbook
    assert test_job["if"] == "inputs.release-scope == 'coordinated'"
    assert "--proto '=https' --proto-redir '=https'" in text
    assert "sha256sum --check --strict" in text
    assert "SPL_RELEASE_COOKBOOK_PATH=" in text
    assert any(step.get("run") == 'python -m pytest -m "not smoke" -q' for step in test_job["steps"])
    assert workflow["jobs"]["publish-testpypi"]["if"] == "inputs.target == 'testpypi'"
    assert workflow["jobs"]["publish-pypi"]["if"] == "inputs.target == 'pypi'"


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


def test_sdk_and_coordinated_publication_share_the_existing_trusted_workflow() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    jobs = workflow["jobs"]
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]
    assert inputs["release-scope"]["default"] == "sdk"
    assert inputs["release-scope"]["options"] == ["sdk", "coordinated"]
    assert inputs["target"]["default"] == "verify"
    assert workflow["permissions"] == {"contents": "read"}
    assert jobs["build-sdk"]["if"] == "inputs.release-scope == 'sdk'"
    assert "needs" not in jobs["build-sdk"]
    for name in ("source-chain", "test"):
        assert jobs[name]["if"] == "inputs.release-scope == 'coordinated'"
    assert set(jobs["build"]["needs"]) == {"source-chain", "test"}
    assert jobs["install-test"]["needs"] == ["build"]
    assert jobs["sdk-install-test"]["needs"] == "build-sdk"
    assert set(jobs["publish-sdk"]["needs"]) == {"build-sdk", "sdk-install-test"}
    assert jobs["publish-sdk"]["if"] == "inputs.release-scope == 'sdk' && inputs.target != 'verify'"
    for name in ("publish-pypi", "publish-testpypi"):
        assert jobs[name]["needs"] == ["install-test"]
    for name, job in jobs.items():
        assert "always()" not in job.get("if", "")
        if job.get("permissions", {}).get("id-token") == "write":
            assert name in {"publish-sdk", "publish-pypi", "publish-testpypi"}
            assert any(str(step.get("uses", "")).startswith("pypa/gh-action-pypi-publish@") for step in job["steps"])
    assert not (PUBLISH_WORKFLOW.parent / "publish-sdk-to-pypi.yml").exists()


def test_signed_release_retention_correction_is_bound_to_one_test_file(tmp_path) -> None:
    import hashlib
    import subprocess

    workflow = _workflow(PUBLISH_WORKFLOW)
    step = next(
        step
        for step in workflow["jobs"]["test"]["steps"]
        if step.get("name") == "Apply the reviewed v0.4.13 retention test correction"
    )
    assert step["if"] == "inputs.release-tag == 'v0.4.13'"
    assert step["env"]["RELEASE_WORKFLOW_SHA"] == "${{ github.workflow_sha }}"
    patch = SPL_ROOT / ".github/release-test-fixes/v0.4.13-retention.patch"
    relative = "tests/daemon/test_sol_083_retention.py"
    headers = [line for line in patch.read_text().splitlines() if line.startswith("diff --git ")]
    assert headers == [f"diff --git a/{relative} b/{relative}"]
    fixed = (SPL_ROOT / relative).read_bytes()
    target = tmp_path / relative
    target.parent.mkdir(parents=True)
    target.write_bytes(fixed)
    subprocess.run(["git", "apply", "--reverse", str(patch)], cwd=tmp_path, check=True)
    original = target.read_bytes()
    assert hashlib.sha256(original).hexdigest() in step["run"]
    assert 'git apply --check "${RUNNER_TEMP}/v0.4.13-retention.patch"' in step["run"]
    assert f"test \"$(git diff --name-only)\" = '{relative}'" in step["run"]
    subprocess.run(["git", "apply", str(patch)], cwd=tmp_path, check=True)
    assert target.read_bytes() == fixed


def test_public_release_artifacts_exclude_private_server_bytes() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    steps = workflow["jobs"]["build"]["steps"]
    uploads = [
        step["with"]["path"] for step in steps if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert uploads
    assert all("/server/" not in paths and "spl-server" not in paths for paths in uploads)
    inventory = next(
        step["run"] for step in steps if step["name"] == "Create the reviewed public artifact checksum inventory"
    )
    assert '(root / "server")' not in inventory
    assert '--component-artifact "server=${SERVER_WHEEL}"' in PUBLISH_WORKFLOW.read_text()


def test_sdk_preparation_is_signed_independent_and_installed_on_each_platform() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    jobs = {key: workflow["jobs"][key] for key in ("build-sdk", "sdk-install-test", "publish-sdk")}
    text = "\n".join(step.get("run", "") for job in jobs.values() for step in job["steps"])
    assert set(workflow["on"]) == {"workflow_dispatch"}
    assert workflow["on"]["workflow_dispatch"]["inputs"]["target"]["default"] == "verify"
    assert jobs["build-sdk"]["if"] == "inputs.release-scope == 'sdk'"
    assert "git verify-tag" in text and "SPL_RELEASE_SIGNING_PUBLIC_KEY" in text
    assert "SDK package metadata must match the release declaration" in text
    assert "sdk_only" in text
    assert "gitlab.com" not in text and "git clone" not in text
    assert "SPL_RELEASE_GITLAB" not in text and "--emit-built-evidence" not in text
    assert "SDK artifacts are not reproducible" in text
    matrix = jobs["sdk-install-test"]["strategy"]["matrix"]
    assert set(matrix["os"]) == {"ubuntu-latest", "macos-latest", "windows-latest"}
    assert len(matrix["dependencies"]) == 2
    assert jobs["sdk-install-test"]["needs"] == "build-sdk"
    assert jobs["sdk-install-test"]["defaults"]["run"]["shell"] == "bash"
    assert "integration_tests/installed_contract.py" in text
    assert "'-I'" in text and "'site-packages' in spl.__file__" in text
    upload = next(
        step for step in jobs["build-sdk"]["steps"] if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    )
    assert set(upload["with"]["path"].splitlines()) == {
        "artifacts/python/*.whl",
        "artifacts/python/*.tar.gz",
        "artifacts/sdk-build-evidence.json",
    }
    publish = jobs["publish-sdk"]
    assert set(publish["needs"]) == {"build-sdk", "sdk-install-test"}
    assert publish["if"] == "inputs.release-scope == 'sdk' && inputs.target != 'verify'"
    assert publish["permissions"]["id-token"] == "write"
    assert publish["environment"]["name"] == "${{ inputs.target }}"
    assert "skip-existing" not in text
    assert "downloaded index bytes differ from verified SDK" in text
    assert "test-files.pythonhosted.org" in text


def _embedded_python(step: dict[str, Any]) -> str:
    return step["run"].split("python - <<'PY'\n", 1)[1].split("\nPY", 1)[0]


def test_sdk_publication_gate_rejects_changed_or_extra_package_bytes(tmp_path, monkeypatch) -> None:
    import hashlib
    import json

    import pytest

    workflow = _workflow(PUBLISH_WORKFLOW)
    publish = workflow["jobs"]["publish-sdk"]
    step = next(
        step
        for step in publish["steps"]
        if step.get("name") == "Reverify SDK evidence and package allowlist before publication"
    )
    script = compile(_embedded_python(step), str(PUBLISH_WORKFLOW), "exec")
    package_root = tmp_path / "artifacts" / "python"
    package_root.mkdir(parents=True)
    filenames = ["splime-0.4.13-py3-none-any.whl", "splime-0.4.13.tar.gz"]
    for name in filenames:
        (package_root / name).write_bytes(b"synthetic-test-artifact")
    evidence = {
        "evidence_scope": "sdk_only",
        "source_commit": "a" * 40,
        "version": "0.4.13",
        "artifacts": {name: hashlib.sha256((package_root / name).read_bytes()).hexdigest() for name in filenames},
    }
    (package_root.parent / "sdk-build-evidence.json").write_text(json.dumps(evidence))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SDK_COMMIT", "a" * 40)
    monkeypatch.setenv("SDK_VERSION", "0.4.13")
    exec(script, {})
    (package_root / filenames[0]).write_bytes(b"changed")
    with pytest.raises(SystemExit, match="digest mismatch"):
        exec(script, {})
    (package_root / filenames[0]).write_bytes(b"synthetic-test-artifact")
    (package_root / "spl_server-private.whl").write_bytes(b"must-not-be-published")
    with pytest.raises(SystemExit, match="allowlist mismatch"):
        exec(script, {})


def test_sdk_workflow_embedded_python_compiles() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    for job in workflow["jobs"].values():
        assert isinstance(job.get("steps"), list) and job["steps"]
        for step in job["steps"]:
            assert ("run" in step) ^ ("uses" in step)
            if "python - <<'PY'\n" in step.get("run", ""):
                compile(_embedded_python(step), str(PUBLISH_WORKFLOW), "exec")


def test_coordinated_release_gates_upload_on_real_installed_acceptance() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    steps = workflow["jobs"]["build"]["steps"]
    names = [step.get("name") for step in steps]
    acceptance_name = "Run real isolated author registry and anonymous consumers"
    assert acceptance_name in names, "real release_candidate_packages.py acceptance must gate the built artifact upload"
    assert names.index("Materialize and verify the external built BOM") < names.index(acceptance_name)
    assert names.index(acceptance_name) < names.index("Upload reviewed external BOM and artifacts")
    acceptance = steps[names.index(acceptance_name)]
    assert acceptance.get("continue-on-error", "false") == "false"
    assert "if" not in acceptance
    assert acceptance["timeout-minutes"] == "20"
    script = _embedded_python(acceptance)
    assert "release_candidate_packages.py" in script
    assert "--author-python" in script and "--consumer-python" in script
    assert "--case5-catalog" not in script
    assert "2.3.5" in script and "2.2.3" in script and "2.5.4" in script and "3.0.6" in script
    assert '"-I"' in script and '"check"' in script
    # No broad upload may expose fixture credentials, signing keys or private server bytes.
    for job in workflow["jobs"].values():
        for step in job["steps"]:
            if str(step.get("uses", "")).startswith("actions/upload-artifact@"):
                paths = step["with"]["path"]
                assert "acceptance" not in paths and "runner.temp" not in paths


def test_coordinated_release_discovers_all_daemon_and_console_regressions() -> None:
    workflow = _workflow(PUBLISH_WORKFLOW)
    steps = workflow["jobs"]["test"]["steps"]
    assert any(step.get("run") == 'python -m pytest -m "not smoke" -q' for step in steps)
    console = [step for step in steps if step.get("name") == "Run complete Console syntax and regression suite"]
    assert len(console) == 1, "new command/cache regressions must be part of coordinated CI"
    assert console[0]["working-directory"] == "../spl-frontend"
    assert console[0]["run"] == "npm run check"
    fixtures = next(
        step for step in steps if step.get("name") == "Materialize reviewed public landing fixtures for Console tests"
    )
    assert steps.index(fixtures) < steps.index(console[0])
    assert "hashlib.sha256(payload).hexdigest() != expected" in fixtures["run"]
    assert "Path('../swpl-landing')" in fixtures["run"]
    for filename in ("index.html", "privacy.html", "terms.html"):
        assert filename in fixtures["run"]
    assert console[0].get("continue-on-error", "false") == "false"
    assert any(str(step.get("uses", "")).startswith("actions/setup-node@") for step in steps)


def test_real_acceptance_setup_checks_bytes_and_isolates_each_interpreter(tmp_path, monkeypatch) -> None:
    import hashlib
    import json
    import subprocess
    import venv

    import pytest

    step = next(
        step
        for step in _workflow(PUBLISH_WORKFLOW)["jobs"]["build"]["steps"]
        if step.get("name") == "Run real isolated author registry and anonymous consumers"
    )
    script = compile(_embedded_python(step), str(PUBLISH_WORKFLOW), "exec")
    workspace = tmp_path / "checkout" / "release-workspace"
    artifacts = workspace / "artifacts"
    artifacts.mkdir(parents=True)
    components = {}
    for component, package in (("framework", "splime"), ("server", "spl_server")):
        wheel = artifacts / f"{package}-0.4.13-py3-none-any.whl"
        wheel.write_bytes(f"synthetic-{component}".encode())
        components[component] = {
            "artifact": {
                "path": str(wheel.relative_to(workspace)),
                "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
            }
        }
    manifest = {"version": "0.4.13", "evidence": {"state": "built"}, "components": components}
    (artifacts / "release-manifest.json").write_text(json.dumps(manifest))
    scratch = tmp_path / "runner-temp" / "acceptance"
    monkeypatch.setenv("GITHUB_WORKSPACE", str(workspace.parent))
    monkeypatch.setenv("CI_ACCEPTANCE_ROOT", str(scratch))
    monkeypatch.setenv("PYTHONPATH", "/unwanted/editable/src")
    monkeypatch.setenv("SPL_USER_TOKEN", "synthetic-secret-must-not-propagate")
    monkeypatch.setenv("UV_PROJECT_ENVIRONMENT", "/unwanted/shared-env")
    created = []
    calls = []

    def create(builder, path):
        assert builder.with_pip and not builder.system_site_packages
        created.append(Path(path))

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        assert argv[1] == "-I"
        assert kwargs["check"] is True
        assert kwargs["cwd"] == scratch / "outside-checkouts"
        assert not any(key.startswith(("SPL", "PYTHON", "UV_")) for key in kwargs["env"])
        assert Path(kwargs["env"]["HOME"]).is_relative_to(scratch)
        if "--author-python" in argv:
            evidence = scratch / "evidence"
            evidence.mkdir()
            (evidence / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "passed",
                        "consumers": [
                            {"versions": {"numpy": "2.3.5", "pandas": "2.2.3"}},
                            {"versions": {"numpy": "2.5.4", "pandas": "3.0.6"}},
                        ],
                    }
                )
            )

    monkeypatch.setattr(venv.EnvBuilder, "create", create)
    monkeypatch.setattr(subprocess, "run", run)
    exec(script, {})
    assert created == [scratch / "envs" / name for name in ("author", "matching", "alternate")]
    installs = [argv for argv, _ in calls if "install" in argv]
    assert len(installs) == 3
    assert [any("spl_server-" in value for value in argv) for argv in installs] == [True, False, False]
    assert all(any("splime-0.4.13-py3-none-any.whl" in value for value in argv) for argv in installs)
    assert [
        (next(v for v in argv if v.startswith("numpy==")), next(v for v in argv if v.startswith("pandas==")))
        for argv in installs
    ] == [
        ("numpy==2.3.5", "pandas==2.2.3"),
        ("numpy==2.3.5", "pandas==2.2.3"),
        ("numpy==2.5.4", "pandas==3.0.6"),
    ]
    assert len([argv for argv, _ in calls if "check" in argv]) == 3
    harness, kwargs = calls[-1]
    assert harness.count("--consumer-python") == 2 and kwargs["timeout"] == 600
    assert harness[2] == str(workspace / "spl" / "integration_tests" / "release_candidate_packages.py")
    assert "--case5-catalog" not in harness
    calls.clear()
    created.clear()
    (artifacts / "splime-0.4.13-py3-none-any.whl").write_bytes(b"changed after BOM")
    with pytest.raises(SystemExit, match="checksum mismatch"):
        exec(script, {})
    assert not calls and not created


def test_installed_sdk_gate_copies_response_regressions_outside_checkout(monkeypatch) -> None:
    import importlib.util
    import subprocess

    runner = SPL_ROOT / "integration_tests" / "installed_contract.py"
    spec = importlib.util.spec_from_file_location("installed_contract_test", runner)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    calls = []
    real_run = subprocess.run

    def run(argv, **kwargs):
        calls.append(argv)
        root = kwargs["cwd"]
        assert not root.is_relative_to(SPL_ROOT)
        assert argv[1:5] == ["-I", "-m", "pytest", "-q"]
        for name in (
            "test_sync_response_integrity.py",
            "test_server_channel_recovery.py",
            "test_catalogue_sync_v1.py",
            "test_daemon_endpoint.py",
        ):
            assert (root / "tests" / "daemon" / name).read_bytes() == (
                SPL_ROOT / "tests" / "daemon" / name
            ).read_bytes()
        assert (root / "tests" / "__init__.py").exists()
        assert (root / "tests" / "daemon" / "__init__.py").exists()
        assert "PYTHONPATH" not in kwargs["env"] and kwargs["check"]
        # Exercise pytest's actual path collection, including macOS /var aliases,
        # without making this workflow test depend on an installed SDK build.
        for conftest in root.rglob("conftest.py"):
            conftest.write_text("")
        (root / "test_installed_identity.py").write_text("def test_installed_identity(): pass\n")
        for name in (
            "test_public_current_process.py",
            "test_unified_result_access.py",
            "test_public_registry_transport.py",
        ):
            (root / "tests/core" / name).write_text("def test_selected_core(): pass\n")
        for name in ("test_server_channel_recovery.py", "test_daemon_endpoint.py"):
            (root / "tests/daemon" / name).write_text("SENTINEL = True\ndef test_helper_unwanted(): pass\n")
        for name in ("test_sync_response_integrity.py", "test_catalogue_sync_v1.py"):
            (root / "tests/daemon" / name).write_text(
                "from tests.daemon import test_daemon_endpoint, test_server_channel_recovery\n"
                "assert test_daemon_endpoint.SENTINEL and test_server_channel_recovery.SENTINEL\n"
                "def test_selected_daemon(): pass\n"
            )
        collected = real_run([*argv, "--collect-only"], capture_output=True, text=True, **kwargs)
        nodes = [line for line in collected.stdout.splitlines() if "::test_" in line]
        assert len(nodes) == 6, collected.stdout
        assert all("test_helper_unwanted" not in node for node in nodes), collected.stdout
        assert any("test_catalogue_sync_v1.py::test_selected_daemon" in node for node in nodes)
        assert any("test_sync_response_integrity.py::test_selected_daemon" in node for node in nodes)

    monkeypatch.setattr(subprocess, "run", run)
    module.main()
    assert len(calls) == 1
    workflow = _workflow(SPL_ROOT / ".github" / "workflows" / "public-current-process.yml")
    paths = workflow["on"]["pull_request"]["paths"]
    assert "tests/daemon/test_sync_response_integrity.py" in paths
    assert "tests/daemon/test_server_channel_recovery.py" in paths
    assert "tests/daemon/test_catalogue_sync_v1.py" in paths
    assert "tests/daemon/test_daemon_endpoint.py" in paths
    for path in (SPL_ROOT / ".github/workflows/public-current-process.yml", PUBLISH_WORKFLOW):
        assert "integration_tests/installed_contract.py" in path.read_text()
