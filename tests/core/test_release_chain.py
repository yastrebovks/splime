"""Contracts for the 0.4.6 source-to-deployment release evidence chain."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import tarfile
import zipfile

import pytest

from tools import verify_published_release
from tools.generate_release_identity import (
    console_build_identity,
    console_source_identity,
    generate,
    release_manifest,
    server_release_identity,
    validate_manifest_declaration_projection,
    write_console_build_identity,
    write_release_evidence_manifest,
)
from tools.release_chain import (
    ReleaseChainError,
    _parse_pypi_artifacts,
    load_json,
    materialize_built_evidence,
    materialize_published_evidence,
    materialize_source_evidence,
    sha256_file,
    validate_built_evidence,
    validate_declared_contract,
    validate_deployment_receipt,
    validate_published_evidence,
    validate_source_pins,
    verify_release_chain,
    write_source_evidence,
)


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
SPL_ROOT = WORKSPACE_ROOT / "spl"


def test_old_tracked_commit_identity_cycle_cannot_name_its_final_clean_head(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "old-cycle"
    repository.mkdir()
    _git(repository, "init")
    _git(repository, "config", "user.name", "Release Test")
    _git(repository, "config", "user.email", "release-test@example.invalid")
    identity = repository / "identity.json"
    identity.write_text('{"commit": null}\n', encoding="utf-8")
    _git(repository, "add", "identity.json")
    _git(repository, "commit", "-m", "identity declaration")
    embedded_commit = _git(repository, "rev-parse", "HEAD")

    identity.write_text(
        f'{{"commit": "{embedded_commit}"}}\n',
        encoding="utf-8",
    )
    _git(repository, "add", "identity.json")
    _git(repository, "commit", "-m", "write the commit into tracked identity")
    final_clean_head = _git(repository, "rev-parse", "HEAD")

    assert embedded_commit != final_clean_head
    assert _git(repository, "status", "--porcelain") == ""


def test_declared_release_contract_is_coherent_but_not_built_evidence() -> None:
    contract = load_json(SPL_ROOT / "release-contract.json")
    manifest = load_json(SPL_ROOT / "release-manifest.json")

    validate_declared_contract(
        contract,
        manifest,
        workspace_root=WORKSPACE_ROOT,
    )
    assert manifest["evidence"] == {
        "state": "declared",
        "reason_code": "release_evidence_pending",
        "generated_at": None,
    }
    assert all(component["source_commit"] is None for component in manifest["components"].values())
    with pytest.raises(ReleaseChainError):
        validate_source_pins(
            contract,
            workspace_root=WORKSPACE_ROOT,
            manifest=manifest,
        )


def test_worker_build_projection_is_an_explicit_release_contract_path() -> None:
    contract = load_json(SPL_ROOT / "release-contract.json")
    manifest = load_json(SPL_ROOT / "release-manifest.json")
    matrix = load_json(SPL_ROOT / contract["compatibility_matrix"])

    capabilities = contract["contracts"]["daemon_server_capabilities"]
    assert "spl.worker_build.v1" in capabilities
    assert manifest["contracts"]["daemon_server_capabilities"] == capabilities
    assert manifest["components"]["daemon"]["contracts"]["daemon_server_capabilities"] == capabilities
    assert manifest["components"]["server"]["contracts"]["daemon_server_capabilities"] == capabilities

    row = next(row for row in matrix["rows"] if row["path"] == "daemon_worker_build_projection")
    assert row["status"] == "tested"
    assert row["required_contract"] == "spl.worker_build.v1"
    assert "never proves execution compatibility or readiness" in row["fallback"]


def test_generator_never_fabricates_source_build_or_publication_evidence() -> None:
    contract = load_json(SPL_ROOT / "release-contract.json")

    manifest = release_manifest(contract)

    assert manifest["schema_version"] == 2
    assert manifest["source_date_epoch"] is None
    assert manifest["console"]["integrity_sha256"] is None
    assert all(
        component["artifact"]["sha256"] is None and component["artifact"]["path"] is None
        for component in manifest["components"].values()
    )
    assert all(artifact["url"] is None and artifact["sha256"] is None for artifact in manifest["python"]["artifacts"])
    assert all(artifact["sha256"] is None for artifact in manifest["github_release"]["assets"])
    assert "release-manifest.json" not in {artifact["name"] for artifact in manifest["github_release"]["assets"]}
    assert manifest["docker"]["source_path"] == "deploy/dockerhub"
    assert manifest["docker"]["manifest_digest"] is None
    assert manifest["docker"]["platform_digests"] == {
        "linux/amd64": None,
        "linux/arm64": None,
    }


def test_tracked_server_declaration_never_embeds_its_own_commit_or_hashes() -> None:
    contract = deepcopy(load_json(SPL_ROOT / "release-contract.json"))
    contract["components"]["server"]["source_commit"] = "a" * 40

    declaration = server_release_identity(contract)

    assert declaration["source"] == {
        "repository": contract["components"]["server"]["repository"],
        "ref": contract["components"]["server"]["source_ref"],
        "binding": "pinned_commit",
        "commit": None,
    }
    assert declaration["artifact_sha256"] is None
    assert declaration["release_manifest_sha256"] is None
    assert declaration["evidence_state"] == "declared"


def test_legacy_console_null_projection_never_embeds_commit_or_bundle_hash() -> None:
    contract = deepcopy(load_json(SPL_ROOT / "release-contract.json"))
    contract["components"]["console"]["source_commit"] = "a" * 40

    declaration = console_source_identity(contract)

    assert declaration["source"] == {
        "repository": contract["components"]["console"]["repository"],
        "ref": contract["components"]["console"]["source_ref"],
        "binding": "pinned_commit",
        "commit": None,
    }
    assert declaration["build"] == {"built_at": None}
    assert declaration["evidence_state"] == "declared"


def test_manifest_projection_allows_evidence_to_mature_but_not_identity_to_drift() -> None:
    contract = load_json(SPL_ROOT / "release-contract.json")
    declaration = release_manifest(contract)
    mature = deepcopy(declaration)
    mature["source_date_epoch"] = 1785369600
    mature["python"]["artifacts"][0]["sha256"] = "a" * 64
    mature["python"]["artifacts"][1]["sha256"] = "b" * 64
    mature["python"]["artifacts"][0]["url"] = "https://files.example.invalid/splime-0.4.6-py3-none-any.whl"
    mature["python"]["artifacts"][1]["url"] = "https://files.example.invalid/splime-0.4.6.tar.gz"
    mature["console"]["integrity_sha256"] = "c" * 64
    for index, (name, component) in enumerate(mature["components"].items()):
        if component["source_binding"] == "pinned_commit":
            component["source_commit"] = f"{index + 1:x}" * 40
        component["artifact"]["path"] = f"artifacts/component-{index}.bin"
        component["artifact"]["sha256"] = f"{index + 1:x}" * 64
    mature["public_artifacts"][0]["sha256"] = "d" * 64
    mature["evidence"] = {
        "state": "published",
        "reason_code": "release_published",
        "generated_at": "2026-07-29T12:00:00+00:00",
    }

    validate_manifest_declaration_projection(mature, declaration)

    leaky = deepcopy(mature)
    leaky["builder"] = {
        "hostname": "private-host",
        "pid": 1234,
        "credential": "must-not-cross-the-boundary",
    }
    with pytest.raises(ReleaseChainError, match="identity fields"):
        validate_manifest_declaration_projection(leaky, declaration)

    mature["server"]["schema_target"] += 1
    with pytest.raises(ReleaseChainError, match="identity fields"):
        validate_manifest_declaration_projection(mature, declaration)


def test_release_evidence_manifest_is_artifact_side(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    contract = load_json(SPL_ROOT / "release-contract.json")
    output = Path("artifacts/release-manifest.json")

    write_release_evidence_manifest(
        workspace_root=workspace,
        output_path=output,
        contract=contract,
        check=False,
    )
    assert load_json(workspace / output) == release_manifest(contract)
    with pytest.raises(ReleaseChainError, match="artifact-side"):
        write_release_evidence_manifest(
            workspace_root=workspace,
            output_path=workspace / "spl" / "release-manifest.json",
            contract=contract,
            check=False,
        )


def test_console_build_evidence_is_artifact_side_and_requires_real_evidence(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    contract = load_json(SPL_ROOT / "release-contract.json")
    payload = console_build_identity(
        contract,
        source_commit="a" * 40,
        built_at="2026-07-29T12:00:00+00:00",
    )
    output = Path("artifacts/console/build.json")

    write_console_build_identity(
        workspace_root=workspace,
        output_path=output,
        payload=payload,
        check=False,
    )
    write_console_build_identity(
        workspace_root=workspace,
        output_path=output,
        payload=payload,
        check=True,
    )
    assert load_json(workspace / output) == payload
    with pytest.raises(ReleaseChainError, match="artifact-side"):
        write_console_build_identity(
            workspace_root=workspace,
            output_path=workspace / "spl-frontend" / "build.json",
            payload=payload,
            check=False,
        )


def test_built_stage_binds_every_component_and_console_build_identity(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    wheel = workspace / "artifacts" / "python" / "splime-0.4.6-py3-none-any.whl"
    _write_test_wheel(
        wheel,
        distribution="splime",
        version="0.4.6",
    )
    server_wheel = workspace / "artifacts" / "server" / "spl_server-0.4.6-py3-none-any.whl"
    _write_test_wheel(
        server_wheel,
        distribution="spl-server",
        version="0.4.6",
    )
    manifest = {
        "version": "0.4.6",
        "release_id": "splime-0.4.6",
        "packages": {"console": "0.4.6"},
        "components": {},
        "python": {
            "artifacts": [
                {
                    "filename": wheel.name,
                    "path": str(wheel.relative_to(workspace)),
                    "sha256": sha256_file(wheel),
                }
            ]
        },
        "console": {
            "integrity_path": "artifacts/console/static-integrity.json",
        },
    }
    console_archive = workspace / "artifacts" / "splime-console-0.4.6.tar.gz"
    component_paths = {
        "framework": wheel,
        "daemon": wheel,
        "server": server_wheel,
        "console": console_archive,
    }
    component_identifiers = {
        "framework": "splime",
        "daemon": "splime",
        "server": "spl-server",
        "console": "splime-console",
    }
    for name in ("framework", "daemon", "server", "console"):
        manifest["components"][name] = {
            "repository": f"https://example.invalid/{name}.git",
            "source_ref": "v0.4.6",
            "source_commit": "1" * 40,
            "contracts": (
                {
                    "console_server": "console-server/v1",
                    "persisted_data": 4,
                }
                if name == "console"
                else {}
            ),
            "artifact": {
                "identifier": component_identifiers[name],
                "path": str(component_paths[name].relative_to(workspace)),
                "sha256": None,
            },
        }
    build = workspace / "artifacts" / "console" / "build.json"
    build.parent.mkdir()
    build.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "release_id": "splime-0.4.6",
                "component": "console",
                "version": "0.4.6",
                "evidence_state": "built",
                "source": {
                    "repository": "https://example.invalid/console.git",
                    "binding": "pinned_commit",
                    "ref": "v0.4.6",
                    "commit": "1" * 40,
                },
                "build": {
                    "built_at": "2026-07-29T12:00:00+00:00",
                },
                "contracts": {
                    "console_server": "console-server/v1",
                    "persisted_data": 4,
                },
            }
        ),
        encoding="utf-8",
    )
    integrity = workspace / "artifacts" / "console" / "static-integrity.json"
    integrity.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "release_id": "splime-0.4.6",
                "build": "./build.json",
                "assets": {"./build.json": sha256_file(build)},
            }
        ),
        encoding="utf-8",
    )
    manifest["console"]["integrity_sha256"] = sha256_file(integrity)
    _write_console_test_archive(
        console_archive,
        stage_root=integrity.parent,
        version="0.4.6",
    )
    for name, path in component_paths.items():
        manifest["components"][name]["artifact"]["sha256"] = sha256_file(path)

    validate_built_evidence(manifest, workspace_root=workspace)

    build.write_text("tampered", encoding="utf-8")
    with pytest.raises(ReleaseChainError, match="build.json"):
        validate_built_evidence(manifest, workspace_root=workspace)


def test_deployment_receipt_is_exact_allowlisted_evidence(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    manifest_path = workspace / "spl" / "release-manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest = {
        "release_id": "splime-0.4.6",
        "packages": {"server": "0.4.6"},
        "components": {
            "server": {
                "source_ref": "v0.4.6",
                "source_commit": "1" * 40,
                "artifact": {"sha256": "2" * 64},
            }
        },
        "server": {"schema_target": 32},
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    receipt = {
        "schema_version": 1,
        "release_id": "splime-0.4.6",
        "component": "server",
        "version": "0.4.6",
        "source_ref": "v0.4.6",
        "source_commit": "1" * 40,
        "artifact_sha256": "2" * 64,
        "release_manifest_sha256": sha256_file(manifest_path),
        "schema_target": 32,
        "deployed_at": "2026-07-29T12:00:00+00:00",
        "environment_class": "staging",
    }
    receipt_path = workspace / "receipt.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    validate_deployment_receipt(
        manifest,
        workspace_root=workspace,
        receipt_path=receipt_path,
    )

    receipt["hostname"] = "must-not-cross-the-boundary"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ReleaseChainError, match="non-allowlisted"):
        validate_deployment_receipt(
            manifest,
            workspace_root=workspace,
            receipt_path=receipt_path,
        )


def _git(repository: Path, *arguments: str) -> str:
    process = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )
    assert process.returncode == 0, process.stderr or process.stdout
    return process.stdout.strip()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{json.dumps(payload, indent=2)}\n", encoding="utf-8")


def _write_test_wheel(
    path: Path,
    *,
    distribution: str,
    version: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dist_info = f"{distribution.replace('-', '_')}-{version}.dist-info"
    with zipfile.ZipFile(path, mode="w") as wheel:
        wheel.writestr(
            f"{dist_info}/METADATA",
            f"Metadata-Version: 2.4\nName: {distribution}\nVersion: {version}\n",
        )


def _write_console_test_archive(
    path: Path,
    *,
    stage_root: Path,
    version: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    archive_root = f"splime-console-{version}"
    with tarfile.open(path, mode="w:gz") as archive:
        for source in sorted(stage_root.rglob("*")):
            archive.add(
                source,
                arcname=f"{archive_root}/{source.relative_to(stage_root).as_posix()}",
                recursive=False,
            )


def _component_repository(
    root: Path,
    name: str,
    files: dict[str, str],
) -> tuple[Path, str]:
    repository = root / name
    repository.mkdir()
    _git(repository, "init")
    _git(repository, "config", "user.name", "Release Test")
    _git(repository, "config", "user.email", "release-test@example.invalid")
    for relative, content in files.items():
        target = repository / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    _git(repository, "add", ".")
    _git(repository, "commit", "-m", "immutable component source")
    commit = _git(repository, "rev-parse", "HEAD")
    _git(repository, "tag", "v0.4.6")
    return repository, commit


def _disposable_built_release(tmp_path: Path) -> tuple[Path, dict, dict]:
    workspace = tmp_path / "release-workspace"
    workspace.mkdir()
    _component_repository(
        workspace,
        "spl-server",
        {
            "pyproject.toml": '[project]\nname = "spl-server"\nversion = "0.4.6"\n',
        },
    )
    frontend_config = (
        'export const APP_VERSION = "0.4.6";\n'
        'export const APP_RELEASE_ID = "splime-0.4.6";\n'
        "export const RELEASE_MANIFEST_URL = "
        '"https://github.com/yastrebovks/splime/releases/download/v0.4.6/release-manifest.json";\n'
        'export const PYPI_RELEASE_URL = "https://pypi.org/project/splime/0.4.6/";\n'
        "export const COMPONENT_VERSIONS = {\n"
        '  framework: "0.4.6",\n'
        '  daemon: "0.4.6",\n'
        "};\n"
    )
    _, frontend_commit = _component_repository(
        workspace,
        "spl-frontend",
        {
            "package.json": '{\n  "name": "splime-console",\n  "version": "0.4.6",\n  "type": "module"\n}\n',
            "config.js": frontend_config,
            "index.html": '<script>const releaseId = "splime-0.4.6";</script>\n',
        },
    )

    contract = {
        "schema_version": 1,
        "release_id": "splime-0.4.6",
        "version": "0.4.6",
        "source_tag": "v0.4.6",
        "components": {
            "framework": {
                "repository": "https://example.invalid/framework",
                "workspace": "spl",
                "package": "splime",
                "version": "0.4.6",
                "source_ref": "v0.4.6",
                "source_binding": "pinned_commit",
                "source_commit": None,
                "artifact": "splime",
            },
            "daemon": {
                "repository": "https://example.invalid/framework",
                "workspace": "spl",
                "package": "splime",
                "version": "0.4.6",
                "source_ref": "v0.4.6",
                "source_binding": "pinned_commit",
                "source_commit": None,
                "artifact": "splime",
            },
            "server": {
                "repository": "https://example.invalid/server",
                "workspace": "spl-server",
                "package": "spl-server",
                "version": "0.4.6",
                "source_ref": "v0.4.6",
                "source_binding": "pinned_commit",
                "source_commit": None,
                "artifact": "spl-server",
            },
            "console": {
                "repository": "https://example.invalid/console",
                "workspace": "spl-frontend",
                "package": "splime-console",
                "version": "0.4.6",
                "source_ref": "v0.4.6",
                "source_binding": "pinned_commit",
                "source_commit": None,
                "artifact": "splime-console",
            },
        },
        "contracts": {
            "console_server": "console-server/v1",
            "daemon_server_capabilities": [
                "spl.remote_run_claim.v1",
                "spl.execution_manifest.v1",
                "spl.worker_operations.v1",
                "spl.worker_build.v1",
            ],
            "manifest_evidence": "spl.execution_manifest.v1",
            "console_persisted_data": 4,
            "release_manifest": 2,
            "deployment_receipt": 1,
        },
        "server_schema_target": 32,
        "compatibility_matrix": "release/compatibility-matrix.json",
    }

    spl_root = workspace / "spl"
    (spl_root / "docs" / "source").mkdir(parents=True)
    (spl_root / "pyproject.toml").write_text(
        '[project]\nname = "splime"\nversion = "0.4.6"\n',
        encoding="utf-8",
    )
    (spl_root / "docs" / "source" / "conf.py").write_text(
        'release = "0.4.6"\n',
        encoding="utf-8",
    )
    _write_json(spl_root / "release-contract.json", contract)
    _write_json(
        spl_root / "release" / "compatibility-matrix.json",
        {
            "schema_version": 1,
            "release_id": "splime-0.4.6",
            "rows": [
                {
                    "path": "daemon_to_server",
                    "status": "tested",
                    "producer": "daemon 0.4.6",
                    "consumer": "server 0.4.6",
                    "required_contract": "spl.execution_manifest.v1",
                    "fallback": "evidence remains unknown",
                    "test_gate": "disposable multi-repository integration",
                }
            ],
        },
    )
    _write_json(
        workspace / "spl-server" / "src" / "daemon_server" / "release-identity.json",
        server_release_identity(contract),
    )
    server_root = workspace / "spl-server"
    _git(server_root, "add", "src/daemon_server/release-identity.json")
    _git(server_root, "commit", "--amend", "--no-edit")
    _git(server_root, "tag", "--force", "v0.4.6")
    server_commit = _git(server_root, "rev-parse", "HEAD")
    _write_json(spl_root / "release-manifest.json", release_manifest(contract))
    _git(spl_root, "init")
    _git(spl_root, "config", "user.name", "Release Test")
    _git(spl_root, "config", "user.email", "release-test@example.invalid")
    _git(spl_root, "add", ".")
    _git(spl_root, "commit", "-m", "immutable framework source and declarations")
    framework_commit = _git(spl_root, "rev-parse", "HEAD")
    _git(spl_root, "tag", "v0.4.6")

    artifacts = workspace / "artifacts"
    manifest = release_manifest(contract)
    manifest["components"]["framework"]["source_commit"] = framework_commit
    manifest["components"]["daemon"]["source_commit"] = framework_commit
    manifest["components"]["server"]["source_commit"] = server_commit
    manifest["components"]["console"]["source_commit"] = frontend_commit
    manifest["source_date_epoch"] = 1785369600

    for artifact in manifest["python"]["artifacts"]:
        path = workspace / artifact["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".whl":
            _write_test_wheel(
                path,
                distribution="splime",
                version="0.4.6",
            )
        else:
            path.write_bytes(artifact["filename"].encode())
        artifact["sha256"] = sha256_file(path)
    splime_wheel = next(
        workspace / artifact["path"]
        for artifact in manifest["python"]["artifacts"]
        if artifact["filename"].endswith(".whl")
    )
    server_wheel = artifacts / "server" / "spl_server-0.4.6-py3-none-any.whl"
    _write_test_wheel(
        server_wheel,
        distribution="spl-server",
        version="0.4.6",
    )

    build = console_build_identity(
        contract,
        source_commit=frontend_commit,
        built_at="2026-07-29T12:00:00+00:00",
    )
    build_path = workspace / "artifacts" / "console" / "build.json"
    _write_json(build_path, build)
    integrity_path = workspace / manifest["console"]["integrity_path"]
    _write_json(
        integrity_path,
        {
            "schema_version": 2,
            "release_id": "splime-0.4.6",
            "build": "./build.json",
            "assets": {"./build.json": sha256_file(build_path)},
        },
    )
    manifest["console"]["integrity_sha256"] = sha256_file(integrity_path)
    console_archive = artifacts / "splime-console-0.4.6.tar.gz"
    _write_console_test_archive(
        console_archive,
        stage_root=integrity_path.parent,
        version="0.4.6",
    )
    component_paths = {
        "framework": splime_wheel,
        "daemon": splime_wheel,
        "server": server_wheel,
        "console": console_archive,
    }
    for name, path in component_paths.items():
        manifest["components"][name]["artifact"]["path"] = str(path.relative_to(workspace))
        manifest["components"][name]["artifact"]["sha256"] = sha256_file(path)
    manifest["evidence"] = {
        "state": "built",
        "reason_code": "release_built",
        "generated_at": "2026-07-29T12:00:00+00:00",
    }
    _write_json(artifacts / "release-manifest.json", manifest)
    return workspace, contract, manifest


def test_real_disposable_multi_repository_source_and_built_chain(
    tmp_path: Path,
) -> None:
    workspace, contract, _ = _disposable_built_release(tmp_path)

    generate(
        workspace_root=workspace,
        contract=contract,
        check=True,
    )
    verify_release_chain(
        workspace_root=workspace,
        contract_path=workspace / "spl" / "release-contract.json",
        manifest_path=workspace / "artifacts" / "release-manifest.json",
        stage="built",
    )
    assert _git(workspace / "spl", "status", "--porcelain") == ""
    with pytest.raises(ReleaseChainError, match="artifact staging root"):
        verify_release_chain(
            workspace_root=workspace,
            contract_path=workspace / "spl" / "release-contract.json",
            manifest_path=workspace / "spl" / "release-manifest.json",
            stage="built",
        )


def test_source_evidence_is_materialized_outside_clean_repositories(
    tmp_path: Path,
) -> None:
    workspace, contract, _ = _disposable_built_release(tmp_path)
    repositories = [
        workspace / "spl",
        workspace / "spl-server",
        workspace / "spl-frontend",
    ]
    before = {
        str(repository): (
            _git(repository, "rev-parse", "HEAD"),
            _git(repository, "status", "--porcelain"),
        )
        for repository in repositories
    }

    source = materialize_source_evidence(
        contract,
        load_json(workspace / "spl" / "release-manifest.json"),
        workspace_root=workspace,
        observed_at="2026-07-30T09:00:00+00:00",
    )
    output = workspace / "artifacts" / "source-release-manifest.json"
    write_source_evidence(
        workspace_root=workspace,
        output_path=output,
        payload=source,
    )

    assert source["evidence"] == {
        "state": "source",
        "reason_code": "release_source_verified",
        "generated_at": "2026-07-30T09:00:00+00:00",
    }
    assert source["source_date_epoch"] > 0
    assert all(component["source_commit"] for component in source["components"].values())
    assert load_json(output) == source
    serialized = json.dumps(source)
    assert str(workspace) not in serialized
    assert all(
        _git(repository, "status", "--porcelain") == before[str(repository)][1]
        and _git(repository, "rev-parse", "HEAD") == before[str(repository)][0]
        for repository in repositories
    )

    with pytest.raises(ReleaseChainError, match="artifact staging root"):
        write_source_evidence(
            workspace_root=workspace,
            output_path=workspace / "spl" / "source-evidence.json",
            payload=source,
        )


def test_built_evidence_materializer_binds_semantic_component_artifacts(
    tmp_path: Path,
) -> None:
    workspace, contract, expected = _disposable_built_release(tmp_path)
    source = materialize_source_evidence(
        contract,
        load_json(workspace / "spl" / "release-manifest.json"),
        workspace_root=workspace,
        observed_at="2026-07-30T09:00:00+00:00",
    )
    component_artifacts = {
        name: workspace / component["artifact"]["path"] for name, component in expected["components"].items()
    }

    built = materialize_built_evidence(
        contract,
        source,
        workspace_root=workspace,
        component_artifacts=component_artifacts,
        observed_at="2026-07-30T09:15:00+00:00",
    )

    assert built["evidence"] == {
        "state": "built",
        "reason_code": "release_built",
        "generated_at": "2026-07-30T09:15:00+00:00",
    }
    assert built["components"]["framework"]["artifact"] == built["components"]["daemon"]["artifact"]
    assert built["components"]["server"]["artifact"]["path"].endswith(".whl")
    assert built["components"]["console"]["artifact"]["path"].endswith("splime-console-0.4.6.tar.gz")

    wrong_components = dict(component_artifacts)
    wrong_components["daemon"] = component_artifacts["server"]
    with pytest.raises(
        ReleaseChainError,
        match="daemon must bind the exact declared SPLime wheel",
    ):
        materialize_built_evidence(
            contract,
            source,
            workspace_root=workspace,
            component_artifacts=wrong_components,
            observed_at="2026-07-30T09:15:00+00:00",
        )


def test_published_evidence_materializer_is_exact_deterministic_and_external(
    tmp_path: Path,
) -> None:
    workspace, contract, built = _disposable_built_release(tmp_path)
    source_asset = workspace / "artifacts" / "source-release-manifest.json"
    _write_json(source_asset, {"release_id": "splime-0.4.6", "state": "source"})
    bom_asset = workspace / "artifacts" / "release-artifact-bom.sha256"
    bom_asset.write_text("reviewed external inventory\n", encoding="utf-8")
    github_assets = {asset["name"]: workspace / asset["path"] for asset in built["github_release"]["assets"]}
    pypi_artifacts = {
        artifact["filename"]: (
            f"https://files.pythonhosted.org/packages/reviewed/{artifact['filename']}",
            artifact["sha256"],
        )
        for artifact in built["python"]["artifacts"]
    }
    arguments = {
        "workspace_root": workspace,
        "pypi_artifacts": pypi_artifacts,
        "github_assets": github_assets,
        "public_artifact_hashes": {
            "https://splime.io/downloads/splime-cookbook.ipynb": "a" * 64,
        },
        "docker_manifest_digest": f"sha256:{'b' * 64}",
        "docker_platform_digests": {
            "linux/amd64": f"sha256:{'c' * 64}",
            "linux/arm64": f"sha256:{'d' * 64}",
        },
        "observed_at": "2026-07-30T09:30:00+00:00",
    }

    first = materialize_published_evidence(contract, built, **arguments)
    second = materialize_published_evidence(contract, built, **arguments)

    assert first == second
    assert first["evidence"] == {
        "state": "published",
        "reason_code": "release_published",
        "generated_at": "2026-07-30T09:30:00+00:00",
    }
    assert first["docker"]["manifest_digest"] == f"sha256:{'b' * 64}"
    assert first["docker"]["platform_digests"] == {
        "linux/amd64": f"sha256:{'c' * 64}",
        "linux/arm64": f"sha256:{'d' * 64}",
    }
    assert {
        artifact["filename"]: (artifact["url"], artifact["sha256"]) for artifact in first["python"]["artifacts"]
    } == pypi_artifacts
    assert all(asset["sha256"] for asset in first["github_release"]["assets"])
    inventory = workspace / "artifacts" / "release-artifact-bom.sha256"
    inventory_lines = inventory.read_text(encoding="utf-8").splitlines()
    assert inventory_lines == sorted(inventory_lines, key=lambda line: line.split("  ", 1)[1])
    inventory_paths = {line.split("  ", 1)[1] for line in inventory_lines}
    assert "release-manifest.json" not in inventory_paths
    assert "published/release-manifest.json" not in inventory_paths
    assert all("release-artifact-bom.sha256" not in line for line in inventory_lines)
    for line in inventory_lines:
        digest, relative = line.split("  ", 1)
        assert sha256_file(workspace / "artifacts" / relative) == digest
    validate_published_evidence(first, workspace_root=workspace)
    verify_published_release.require_publishable_manifest(first)

    output = workspace / "artifacts" / "published" / "release-manifest.json"
    write_source_evidence(
        workspace_root=workspace,
        output_path=output,
        payload=first,
    )
    assert load_json(output) == first
    verify_release_chain(
        workspace_root=workspace,
        contract_path=workspace / "spl" / "release-contract.json",
        manifest_path=output,
        stage="published",
    )
    assert not (workspace / "spl" / "published-release-manifest.json").exists()

    incomplete_assets = dict(github_assets)
    incomplete_assets.pop("static-integrity.json")
    with pytest.raises(ReleaseChainError, match="exactly match"):
        materialize_published_evidence(
            contract,
            built,
            **{**arguments, "github_assets": incomplete_assets},
        )
    with pytest.raises(ReleaseChainError, match="immutable"):
        materialize_published_evidence(
            contract,
            built,
            **{**arguments, "docker_manifest_digest": "b" * 64},
        )
    incomplete_pypi = dict(pypi_artifacts)
    incomplete_pypi.pop("splime-0.4.6.tar.gz")
    with pytest.raises(ReleaseChainError, match="PyPI artifact inputs must exactly match"):
        materialize_published_evidence(
            contract,
            built,
            **{**arguments, "pypi_artifacts": incomplete_pypi},
        )
    wrong_pypi_hash = dict(pypi_artifacts)
    wheel_name = "splime-0.4.6-py3-none-any.whl"
    wrong_pypi_hash[wheel_name] = (
        wrong_pypi_hash[wheel_name][0],
        "e" * 64,
    )
    with pytest.raises(ReleaseChainError, match="checksum does not match built evidence"):
        materialize_published_evidence(
            contract,
            built,
            **{**arguments, "pypi_artifacts": wrong_pypi_hash},
        )
    mutable_pypi_url = dict(pypi_artifacts)
    mutable_pypi_url[wheel_name] = (
        f"http://files.pythonhosted.org/packages/reviewed/{wheel_name}",
        mutable_pypi_url[wheel_name][1],
    )
    with pytest.raises(ReleaseChainError, match="credential-free HTTPS"):
        materialize_published_evidence(
            contract,
            built,
            **{**arguments, "pypi_artifacts": mutable_pypi_url},
        )
    with pytest.raises(ReleaseChainError, match="duplicate --pypi-artifact"):
        _parse_pypi_artifacts(
            [
                f"{wheel_name}=https://files.example.invalid/{wheel_name}={'a' * 64}",
                f"{wheel_name}=https://files.example.invalid/{wheel_name}={'a' * 64}",
            ]
        )

    github_assets["release-artifact-bom.sha256"].write_text(
        "tampered inventory\n",
        encoding="utf-8",
    )
    with pytest.raises(ReleaseChainError, match="checksum mismatch"):
        validate_published_evidence(first, workspace_root=workspace)

    dirty_path = workspace / "spl-server" / "untracked-release-input.txt"
    dirty_path.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(ReleaseChainError, match="repository is dirty"):
        materialize_published_evidence(contract, built, **arguments)
    dirty_path.unlink()

    declaration = load_json(workspace / "spl" / "release-manifest.json")
    with pytest.raises(ReleaseChainError):
        materialize_published_evidence(
            contract,
            declaration,
            **arguments,
        )


def test_real_source_chain_rejects_wrong_pin_unsigned_tag_and_dirty_tree(
    tmp_path: Path,
) -> None:
    workspace, contract, manifest = _disposable_built_release(tmp_path)
    wrong_pin = deepcopy(manifest)
    wrong_pin["components"]["server"]["source_commit"] = "f" * 40
    with pytest.raises(ReleaseChainError, match="does not resolve"):
        validate_source_pins(
            contract,
            workspace_root=workspace,
            manifest=wrong_pin,
        )

    tracked_pin = deepcopy(contract)
    tracked_pin["components"]["server"]["source_commit"] = manifest["components"]["server"]["source_commit"]
    with pytest.raises(ReleaseChainError, match="tracked declaration"):
        validate_source_pins(
            tracked_pin,
            workspace_root=workspace,
            manifest=manifest,
        )

    unsigned = deepcopy(contract)
    unsigned["components"]["framework"]["source_binding"] = "signed_tag_external_provenance"
    unsigned_manifest = deepcopy(manifest)
    unsigned_manifest["components"]["framework"]["source_binding"] = "signed_tag_external_provenance"
    unsigned_manifest["components"]["framework"]["source_commit"] = None
    with pytest.raises(ReleaseChainError, match="verify-tag"):
        validate_source_pins(
            unsigned,
            workspace_root=workspace,
            manifest=unsigned_manifest,
        )

    untracked = workspace / "spl-server" / "untracked.txt"
    untracked.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(ReleaseChainError, match="repository is dirty"):
        validate_source_pins(
            contract,
            workspace_root=workspace,
            manifest=manifest,
        )
    untracked.unlink()

    tracked = workspace / "spl-server" / "pyproject.toml"
    tracked.write_text(
        f"{tracked.read_text(encoding='utf-8')}# tracked mutation\n",
        encoding="utf-8",
    )
    with pytest.raises(ReleaseChainError, match="repository is dirty"):
        validate_source_pins(
            contract,
            workspace_root=workspace,
            manifest=manifest,
        )


def test_real_source_chain_rejects_wrong_ref_and_post_tag_commit(
    tmp_path: Path,
) -> None:
    workspace, contract, manifest = _disposable_built_release(tmp_path)
    server = workspace / "spl-server"
    _git(server, "tag", "v0.4.5")
    wrong_existing_ref = deepcopy(contract)
    wrong_existing_ref["components"]["server"]["source_ref"] = "v0.4.5"
    with pytest.raises(ReleaseChainError, match="approved lockstep source_tag"):
        validate_declared_contract(
            wrong_existing_ref,
            manifest,
            workspace_root=workspace,
        )

    wrong_ref = deepcopy(contract)
    wrong_ref["components"]["server"]["source_ref"] = "v9.9.9"
    with pytest.raises(ReleaseChainError, match="rev-parse"):
        validate_source_pins(
            wrong_ref,
            workspace_root=workspace,
            manifest=manifest,
        )

    (server / "post-tag-change.txt").write_text("changed\n", encoding="utf-8")
    _git(server, "add", "post-tag-change.txt")
    _git(server, "commit", "-m", "source mutation after tag")
    with pytest.raises(ReleaseChainError, match="does not match"):
        validate_source_pins(
            contract,
            workspace_root=workspace,
            manifest=manifest,
        )


def test_real_built_chain_rejects_tampered_artifact_and_build_commit(
    tmp_path: Path,
) -> None:
    workspace, _, manifest = _disposable_built_release(tmp_path)
    server_artifact = workspace / manifest["components"]["server"]["artifact"]["path"]
    original_server_artifact = server_artifact.read_bytes()
    server_artifact.write_bytes(b"tampered")
    with pytest.raises(ReleaseChainError, match="server artifact checksum mismatch"):
        validate_built_evidence(manifest, workspace_root=workspace)

    server_artifact.write_bytes(original_server_artifact)
    build_path = workspace / "artifacts" / "console" / "build.json"
    build = load_json(build_path)
    build["source"]["commit"] = "f" * 40
    _write_json(build_path, build)
    integrity_path = workspace / manifest["console"]["integrity_path"]
    integrity = load_json(integrity_path)
    integrity["assets"]["./build.json"] = sha256_file(build_path)
    _write_json(integrity_path, integrity)
    manifest["console"]["integrity_sha256"] = sha256_file(integrity_path)
    console_archive = workspace / manifest["components"]["console"]["artifact"]["path"]
    _write_console_test_archive(
        console_archive,
        stage_root=integrity_path.parent,
        version="0.4.6",
    )
    manifest["components"]["console"]["artifact"]["sha256"] = sha256_file(console_archive)
    with pytest.raises(ReleaseChainError, match="source commit does not match"):
        validate_built_evidence(manifest, workspace_root=workspace)


def test_built_console_identity_rejects_private_or_local_fields(
    tmp_path: Path,
) -> None:
    workspace, _, manifest = _disposable_built_release(tmp_path)
    build_path = workspace / "artifacts" / "console" / "build.json"
    build = load_json(build_path)
    build["builder"] = {
        "hostname": "private-host",
        "local_path": "/private/worktree",
        "credential": "must-not-cross-the-boundary",
    }
    _write_json(build_path, build)
    integrity_path = workspace / manifest["console"]["integrity_path"]
    integrity = load_json(integrity_path)
    integrity["assets"]["./build.json"] = sha256_file(build_path)
    _write_json(integrity_path, integrity)
    manifest["console"]["integrity_sha256"] = sha256_file(integrity_path)
    console_archive = workspace / manifest["components"]["console"]["artifact"]["path"]
    _write_console_test_archive(
        console_archive,
        stage_root=integrity_path.parent,
        version="0.4.6",
    )
    manifest["components"]["console"]["artifact"]["sha256"] = sha256_file(console_archive)

    with pytest.raises(ReleaseChainError, match="non-allowlisted fields"):
        validate_built_evidence(manifest, workspace_root=workspace)
