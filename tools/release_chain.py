"""Validate SPLime release evidence from declared source through deployment.

The verifier is intentionally local and deterministic. It never turns a version
constant into evidence: later stages require clean pinned repositories, exact
artifact bytes, Console integrity, the server schema target, and a staged
deployment receipt.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile
from typing import Any
from urllib.parse import urlparse
import zipfile


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DOCKER_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_RECEIPT_KEYS = frozenset(
    {
        "schema_version",
        "release_id",
        "component",
        "version",
        "source_ref",
        "source_commit",
        "artifact_sha256",
        "release_manifest_sha256",
        "schema_target",
        "deployed_at",
        "environment_class",
    }
)
_REQUIRED_COMPONENTS = frozenset({"framework", "daemon", "server", "console"})
_REQUIRED_DOCKER_PLATFORMS = frozenset({"linux/amd64", "linux/arm64"})
_STAGES = ("contract", "source", "built", "published", "deployed")


class ReleaseChainError(ValueError):
    """A release evidence claim is incomplete, incoherent, or mismatched."""


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ReleaseChainError(f"{path} must contain a JSON object")
    return payload


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_manifest_declaration_projection(
    manifest: dict[str, Any],
    declaration: dict[str, Any],
) -> None:
    """Allow evidence to mature without changing its reviewed declaration."""

    projected = deepcopy(manifest)
    try:
        projected["source_date_epoch"] = None
        for artifact in projected["python"]["artifacts"]:
            artifact["url"] = None
            artifact["sha256"] = None
        projected["console"]["integrity_sha256"] = None
        for component in projected["components"].values():
            component["source_commit"] = None
            component["artifact"]["path"] = None
            component["artifact"]["sha256"] = None
        for artifact in projected["public_artifacts"]:
            artifact["sha256"] = None
        for artifact in projected["github_release"]["assets"]:
            artifact["sha256"] = None
        projected["docker"]["manifest_digest"] = None
        for platform in projected["docker"]["platform_digests"]:
            projected["docker"]["platform_digests"][platform] = None
        if set(projected["evidence"]) != set(declaration["evidence"]):
            raise ReleaseChainError("release-manifest evidence keys are not the generated declaration keys")
        projected["evidence"] = deepcopy(declaration["evidence"])
    except (AttributeError, KeyError, TypeError) as exc:
        raise ReleaseChainError("release-manifest does not preserve the generated declaration shape") from exc
    if projected != declaration:
        raise ReleaseChainError("release-manifest identity fields do not match the generated declaration")


def validate_declared_contract(
    contract: dict[str, Any],
    manifest: dict[str, Any],
    *,
    workspace_root: Path,
) -> None:
    """Validate the additive v2 declaration and every public version source."""

    if contract.get("schema_version") != 1:
        raise ReleaseChainError("release-contract schema_version must be 1")
    if manifest.get("schema_version") != 2:
        raise ReleaseChainError("release-manifest schema_version must be 2")

    release_id = _required_text(contract, "release_id", "release-contract")
    version = _required_text(contract, "version", "release-contract")
    if release_id != f"splime-{version}":
        raise ReleaseChainError("release_id must be splime-<version>")
    source_tag = _required_text(contract, "source_tag", "release-contract")
    if source_tag != f"v{version}":
        raise ReleaseChainError("release-contract source_tag must be v<version>")
    if manifest.get("release_id") != release_id or manifest.get("version") != version:
        raise ReleaseChainError("release-manifest identity does not match release-contract")

    components = contract.get("components")
    if not isinstance(components, dict) or set(components) != _REQUIRED_COMPONENTS:
        raise ReleaseChainError("release-contract components must be exactly framework, daemon, server, console")
    manifest_components = manifest.get("components")
    if not isinstance(manifest_components, dict) or set(manifest_components) != _REQUIRED_COMPONENTS:
        raise ReleaseChainError("release-manifest components must be exactly framework, daemon, server, console")

    packages = manifest.get("packages")
    if not isinstance(packages, dict):
        raise ReleaseChainError("release-manifest packages must be an object")
    evidence_state = manifest.get("evidence", {}).get("state")
    for name in sorted(_REQUIRED_COMPONENTS):
        component = components[name]
        if not isinstance(component, dict):
            raise ReleaseChainError(f"component {name} must be an object")
        for key in (
            "repository",
            "workspace",
            "package",
            "version",
            "source_ref",
            "source_binding",
            "artifact",
        ):
            _required_text(component, key, f"component {name}")
        if component["source_binding"] not in {
            "signed_tag_external_provenance",
            "pinned_commit",
        }:
            raise ReleaseChainError(f"component {name} source_binding is invalid")
        if component.get("source_commit") is not None:
            raise ReleaseChainError(f"component {name} tracked declaration must not contain a final source commit")
        if component["version"] != version:
            raise ReleaseChainError(f"component {name} version must match release version")
        if component["source_ref"] != source_tag:
            raise ReleaseChainError(f"component {name} source_ref must match the approved lockstep source_tag")
        if packages.get(name) != version:
            raise ReleaseChainError(f"manifest packages.{name} does not match release version")
        declared = manifest_components.get(name)
        if not isinstance(declared, dict):
            raise ReleaseChainError(f"manifest component {name} is missing")
        for key in ("repository", "source_ref", "source_binding", "package_version"):
            if declared.get(key) != component.get({"package_version": "version"}.get(key, key)):
                raise ReleaseChainError(f"manifest component {name}.{key} is not generated")
        evidence_commit = declared.get("source_commit")
        if evidence_state == "declared" and evidence_commit is not None:
            raise ReleaseChainError(f"tracked manifest component {name} must not store a final source commit")
        if evidence_state in {"source", "built", "published"}:
            _required_git_commit(
                evidence_commit,
                f"manifest component {name}.source_commit",
            )
        elif evidence_commit is not None:
            _required_git_commit(
                evidence_commit,
                f"manifest component {name}.source_commit",
            )
        artifact = declared.get("artifact")
        if not isinstance(artifact, dict) or artifact.get("identifier") != component["artifact"]:
            raise ReleaseChainError(f"manifest component {name}.artifact is not generated")

    contracts = contract.get("contracts")
    if not isinstance(contracts, dict) or manifest.get("contracts") != contracts:
        raise ReleaseChainError("release-manifest contracts do not match release-contract")

    docker = manifest.get("docker")
    if not isinstance(docker, dict):
        raise ReleaseChainError("release-manifest docker section is missing")
    framework = components["framework"]
    if (
        docker.get("source_repository") != framework["repository"]
        or docker.get("source_ref") != source_tag
        or docker.get("source_path") != "deploy/dockerhub"
    ):
        raise ReleaseChainError("Docker source authority must be the signed framework tag at deploy/dockerhub")

    matrix_path = workspace_root / "spl" / _required_text(contract, "compatibility_matrix", "release-contract")
    matrix = load_json(matrix_path)
    if matrix.get("release_id") != release_id:
        raise ReleaseChainError("compatibility matrix release_id does not match")
    rows = matrix.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ReleaseChainError("compatibility matrix must contain tested path rows")
    for row in rows:
        if not isinstance(row, dict):
            raise ReleaseChainError("compatibility matrix rows must be objects")
        for key in ("path", "producer", "consumer", "required_contract", "fallback", "test_gate"):
            _required_text(row, key, f"compatibility row {row!r}")
        if row.get("status") not in {"tested", "unverified"}:
            raise ReleaseChainError("compatibility row status must be tested or unverified")

    _validate_public_versions(
        workspace_root=workspace_root,
        version=version,
        release_id=release_id,
    )
    _validate_manifest_declaration(manifest)


def validate_source_pins(
    contract: dict[str, Any],
    *,
    workspace_root: Path,
    manifest: dict[str, Any] | None = None,
) -> None:
    """Require every component to resolve to a clean, exact immutable source."""

    checked: set[tuple[str, str]] = set()
    verified_tags: set[tuple[str, str]] = set()
    for name, component in contract["components"].items():
        workspace = _safe_workspace(workspace_root, component["workspace"])
        ref = _required_text(component, "source_ref", f"component {name}")
        binding = component["source_binding"]
        resolved = _git(workspace, "rev-parse", f"{ref}^{{commit}}")
        if binding == "pinned_commit":
            # The full object id is post-commit evidence held by the central
            # artifact-side manifest. It must not be copied into any tracked
            # component declaration.
            if component.get("source_commit") is not None:
                raise ReleaseChainError(f"component {name} tracked declaration contains a final source commit")
            evidence_component = manifest.get("components", {}).get(name) if isinstance(manifest, dict) else None
            if not isinstance(evidence_component, dict):
                raise ReleaseChainError(f"component {name} needs external post-commit source evidence")
            commit = _required_git_commit(
                evidence_component.get("source_commit"),
                f"component {name} external source_commit",
            )
            if resolved != commit:
                raise ReleaseChainError(f"component {name} source_ref {ref} does not resolve to {commit}")
        else:
            tag_key = (str(workspace), ref)
            if tag_key not in verified_tags:
                _git(workspace, "verify-tag", ref)
                verified_tags.add(tag_key)
            if component.get("source_commit") is not None:
                raise ReleaseChainError(f"component {name} must not embed its signed tag commit")
            evidence_component = manifest.get("components", {}).get(name) if isinstance(manifest, dict) else None
            if not isinstance(evidence_component, dict):
                raise ReleaseChainError(f"component {name} needs external resolved tag evidence")
            commit = _required_git_commit(
                evidence_component.get("source_commit"),
                f"component {name} external source_commit",
            )
            if resolved != commit:
                raise ReleaseChainError(f"component {name} signed source_ref {ref} does not resolve to {commit}")
        key = (str(workspace), commit)
        if key in checked:
            continue
        checked.add(key)
        observed = _git(workspace, "rev-parse", "HEAD")
        if observed != commit:
            raise ReleaseChainError(f"component {name} source_commit {commit} does not match {observed}")
        if _git(workspace, "status", "--porcelain"):
            raise ReleaseChainError(f"component {name} repository is dirty")


def materialize_source_evidence(
    contract: dict[str, Any],
    declaration: dict[str, Any],
    *,
    workspace_root: Path,
    observed_at: str,
) -> dict[str, Any]:
    """Create post-commit source evidence without writing into a repository.

    Pinned component commits are resolved only after their source revisions
    exist. Signed tag components keep the tag as their binding and are verified
    cryptographically. The resulting payload is suitable only for the external
    artifact staging directory.
    """

    _required_iso_timestamp(observed_at, "source evidence observed_at")
    evidence = deepcopy(declaration)
    for name, component in contract["components"].items():
        workspace = _safe_workspace(workspace_root, component["workspace"])
        ref = _required_text(component, "source_ref", f"component {name}")
        evidence["components"][name]["source_commit"] = _git(
            workspace,
            "rev-parse",
            f"{ref}^{{commit}}",
        )

    framework = contract["components"]["framework"]
    framework_workspace = _safe_workspace(
        workspace_root,
        framework["workspace"],
    )
    epoch_text = _git(
        framework_workspace,
        "show",
        "-s",
        "--format=%ct",
        framework["source_ref"],
    )
    try:
        source_date_epoch = int(epoch_text)
    except ValueError as exc:
        raise ReleaseChainError("framework source commit did not provide a valid source epoch") from exc
    if source_date_epoch <= 0:
        raise ReleaseChainError("framework source epoch must be positive")
    evidence["source_date_epoch"] = source_date_epoch
    evidence["evidence"] = {
        "state": "source",
        "reason_code": "release_source_verified",
        "generated_at": observed_at,
    }

    validate_declared_contract(
        contract,
        evidence,
        workspace_root=workspace_root,
    )
    validate_manifest_declaration_projection(evidence, declaration)
    validate_source_pins(
        contract,
        workspace_root=workspace_root,
        manifest=evidence,
    )
    return evidence


def write_source_evidence(
    *,
    workspace_root: Path,
    output_path: Path,
    payload: dict[str, Any],
) -> None:
    """Atomically write source evidence only below ``artifacts/``."""

    output = _artifact_output_path(
        workspace_root,
        output_path,
        "source evidence manifest",
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    rendered = f"{json.dumps(payload, indent=2, sort_keys=False)}\n"
    temporary = output.with_name(f".{output.name}.source-evidence.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)


def materialize_built_evidence(
    contract: dict[str, Any],
    source_manifest: dict[str, Any],
    *,
    workspace_root: Path,
    component_artifacts: dict[str, Path],
    observed_at: str,
) -> dict[str, Any]:
    """Bind exact staged component, Python, and Console bytes to source."""

    if set(component_artifacts) != _REQUIRED_COMPONENTS:
        raise ReleaseChainError("built evidence needs exactly framework, daemon, server, and console artifacts")
    _required_iso_timestamp(observed_at, "built evidence observed_at")
    validate_declared_contract(
        contract,
        source_manifest,
        workspace_root=workspace_root,
    )
    declaration = load_json(workspace_root / "spl" / "release-manifest.json")
    validate_manifest_declaration_projection(source_manifest, declaration)
    _require_evidence_stage(source_manifest, "source")
    validate_source_pins(
        contract,
        workspace_root=workspace_root,
        manifest=source_manifest,
    )

    evidence = deepcopy(source_manifest)
    for name, artifact_path in component_artifacts.items():
        path = _artifact_output_path(
            workspace_root,
            artifact_path,
            f"component {name} artifact",
        )
        if not path.is_file():
            raise ReleaseChainError(f"component {name} artifact is missing: {path}")
        evidence["components"][name]["artifact"]["path"] = str(path.relative_to(workspace_root.resolve()))
        evidence["components"][name]["artifact"]["sha256"] = sha256_file(path)

    for artifact in evidence["python"]["artifacts"]:
        path = _safe_artifact(
            workspace_root,
            _required_text(
                artifact,
                "path",
                f"Python artifact {artifact.get('filename')!r}",
            ),
        )
        artifact["sha256"] = sha256_file(path)

    console_integrity = _safe_artifact(
        workspace_root,
        _required_text(
            evidence["console"],
            "integrity_path",
            "manifest console",
        ),
    )
    evidence["console"]["integrity_sha256"] = sha256_file(console_integrity)
    evidence["evidence"] = {
        "state": "built",
        "reason_code": "release_built",
        "generated_at": observed_at,
    }

    validate_declared_contract(
        contract,
        evidence,
        workspace_root=workspace_root,
    )
    validate_manifest_declaration_projection(evidence, declaration)
    validate_built_evidence(evidence, workspace_root=workspace_root)
    return evidence


def materialize_published_evidence(
    contract: dict[str, Any],
    built_manifest: dict[str, Any],
    *,
    workspace_root: Path,
    pypi_artifacts: dict[str, tuple[str, str]],
    github_assets: dict[str, Path],
    public_artifact_hashes: dict[str, str],
    docker_manifest_digest: str,
    docker_platform_digests: dict[str, str],
    observed_at: str,
) -> dict[str, Any]:
    """Bind reviewed publication inputs to exact built evidence.

    The transition is deterministic for the supplied bytes, hashes, digests,
    and timestamp. It reads and rebuilds evidence only below ``artifacts/``
    and returns a payload that can only be written below that external staging
    root.
    """

    _required_iso_timestamp(observed_at, "published evidence observed_at")
    validate_declared_contract(
        contract,
        built_manifest,
        workspace_root=workspace_root,
    )
    declaration = load_json(workspace_root / "spl" / "release-manifest.json")
    validate_manifest_declaration_projection(built_manifest, declaration)
    _require_evidence_stage(built_manifest, "built")
    validate_source_pins(
        contract,
        workspace_root=workspace_root,
        manifest=built_manifest,
    )
    validate_built_evidence(built_manifest, workspace_root=workspace_root)

    evidence = deepcopy(built_manifest)
    python_artifacts = evidence.get("python", {}).get("artifacts")
    if not isinstance(python_artifacts, list) or not python_artifacts:
        raise ReleaseChainError("published evidence needs declared PyPI artifacts")
    python_by_name: dict[str, dict[str, Any]] = {}
    for artifact in python_artifacts:
        if not isinstance(artifact, dict):
            raise ReleaseChainError("PyPI artifact declaration must be an object")
        filename = _required_text(artifact, "filename", "PyPI artifact")
        if filename in python_by_name:
            raise ReleaseChainError(f"duplicate PyPI artifact filename {filename!r}")
        python_by_name[filename] = artifact
    if set(pypi_artifacts) != set(python_by_name):
        raise ReleaseChainError("published evidence PyPI artifact inputs must exactly match the declaration")
    pypi_urls: set[str] = set()
    for filename, artifact in python_by_name.items():
        url, supplied_sha = pypi_artifacts[filename]
        url = _required_https_url(url, f"PyPI artifact {filename} URL")
        if Path(urlparse(url).path).name != filename:
            raise ReleaseChainError(f"PyPI artifact URL does not end in {filename}")
        if url in pypi_urls:
            raise ReleaseChainError("PyPI artifact URLs must be unique")
        pypi_urls.add(url)
        observed_sha = _required_sha(
            supplied_sha,
            f"PyPI artifact {filename} sha256",
        )
        built_sha = _required_sha(
            artifact.get("sha256"),
            f"built PyPI artifact {filename} sha256",
        )
        if observed_sha != built_sha:
            raise ReleaseChainError(f"PyPI artifact {filename} checksum does not match built evidence")
        artifact["url"] = url

    declared_assets = evidence.get("github_release", {}).get("assets")
    if not isinstance(declared_assets, list) or not declared_assets:
        raise ReleaseChainError("published evidence needs declared GitHub release assets")
    assets_by_name: dict[str, dict[str, Any]] = {}
    for asset in declared_assets:
        if not isinstance(asset, dict):
            raise ReleaseChainError("GitHub release asset declaration must be an object")
        name = _required_text(asset, "name", "GitHub release asset")
        if name in assets_by_name:
            raise ReleaseChainError(f"duplicate GitHub release asset name {name!r}")
        assets_by_name[name] = asset
    if set(github_assets) != set(assets_by_name):
        raise ReleaseChainError("published evidence GitHub asset inputs must exactly match the declaration")
    for name, asset in assets_by_name.items():
        supplied = _artifact_output_path(
            workspace_root,
            github_assets[name],
            f"GitHub release asset {name}",
        )
        declared_path = _required_text(asset, "path", f"GitHub release asset {name}")
        expected = (workspace_root / declared_path).resolve()
        if supplied != expected:
            raise ReleaseChainError(f"GitHub release asset {name} must use declared path {declared_path}")
        if supplied.name != name:
            raise ReleaseChainError(f"GitHub release asset path does not end in {name}")

    public_artifacts = evidence.get("public_artifacts")
    if not isinstance(public_artifacts, list) or not public_artifacts:
        raise ReleaseChainError("published evidence needs declared public artifacts")
    declared_urls = {
        _required_text(artifact, "url", "public artifact")
        for artifact in public_artifacts
        if isinstance(artifact, dict)
    }
    if len(declared_urls) != len(public_artifacts):
        raise ReleaseChainError("public artifact declarations must use unique URLs")
    if set(public_artifact_hashes) != declared_urls:
        raise ReleaseChainError("published evidence public artifact inputs must exactly match the declaration")
    for artifact in public_artifacts:
        url = artifact["url"]
        artifact["sha256"] = _required_sha(
            public_artifact_hashes[url],
            f"public artifact {url} sha256",
        )

    evidence["docker"]["manifest_digest"] = _required_docker_digest(
        docker_manifest_digest,
        "Docker manifest digest",
    )
    if set(docker_platform_digests) != _REQUIRED_DOCKER_PLATFORMS:
        raise ReleaseChainError("Docker platform digests must be exactly linux/amd64 and linux/arm64")
    evidence["docker"]["platform_digests"] = {
        platform: _required_docker_digest(
            docker_platform_digests[platform],
            f"Docker {platform} digest",
        )
        for platform in sorted(_REQUIRED_DOCKER_PLATFORMS)
    }
    if len(set(evidence["docker"]["platform_digests"].values())) != len(_REQUIRED_DOCKER_PLATFORMS):
        raise ReleaseChainError("Docker platform digests must be distinct")

    evidence["evidence"] = {
        "state": "published",
        "reason_code": "release_published",
        "generated_at": observed_at,
    }
    _rebuild_artifact_checksum_inventory(
        evidence,
        workspace_root=workspace_root,
    )
    for name, asset in assets_by_name.items():
        supplied = _artifact_output_path(
            workspace_root,
            github_assets[name],
            f"GitHub release asset {name}",
        )
        if not supplied.is_file() or supplied.is_symlink():
            raise ReleaseChainError(f"GitHub release asset is missing or not a regular file: {supplied}")
        asset["sha256"] = sha256_file(supplied)
    validate_declared_contract(
        contract,
        evidence,
        workspace_root=workspace_root,
    )
    validate_manifest_declaration_projection(evidence, declaration)
    validate_published_evidence(evidence, workspace_root=workspace_root)
    return evidence


def validate_built_evidence(
    manifest: dict[str, Any],
    *,
    workspace_root: Path,
) -> None:
    """Require each declared artifact and Console integrity digest to match bytes."""

    components = manifest["components"]
    for name, component in components.items():
        artifact = component.get("artifact")
        if not isinstance(artifact, dict):
            raise ReleaseChainError(f"manifest component {name}.artifact must be an object")
        path_value = _required_text(artifact, "path", f"component {name} artifact")
        expected = _required_sha(
            artifact.get("sha256"),
            f"component {name} artifact sha256",
        )
        path = _safe_artifact(workspace_root, path_value)
        observed = sha256_file(path)
        if observed != expected:
            raise ReleaseChainError(f"component {name} artifact checksum mismatch: {observed}")

    python_artifacts = manifest.get("python", {}).get("artifacts")
    if not isinstance(python_artifacts, list) or not python_artifacts:
        raise ReleaseChainError("manifest Python artifacts must be a non-empty list")
    for artifact in python_artifacts:
        if not isinstance(artifact, dict):
            raise ReleaseChainError("manifest Python artifact must be an object")
        filename = _required_text(artifact, "filename", "manifest Python artifact")
        path = _safe_artifact(
            workspace_root,
            _required_text(artifact, "path", f"Python artifact {filename}"),
        )
        if path.name != filename:
            raise ReleaseChainError(f"Python artifact path does not end in {filename}")
        expected = _required_sha(
            artifact.get("sha256"),
            f"Python artifact {filename} sha256",
        )
        if sha256_file(path) != expected:
            raise ReleaseChainError(f"Python artifact checksum mismatch: {filename}")

    _validate_component_artifact_semantics(
        manifest,
        workspace_root=workspace_root,
    )

    console = manifest.get("console")
    if not isinstance(console, dict):
        raise ReleaseChainError("manifest console section is missing")
    integrity_path = _safe_artifact(
        workspace_root,
        _required_text(console, "integrity_path", "manifest console"),
    )
    integrity_sha = _required_sha(
        console.get("integrity_sha256"),
        "manifest console integrity_sha256",
    )
    if sha256_file(integrity_path) != integrity_sha:
        raise ReleaseChainError("Console integrity manifest checksum mismatch")
    integrity = load_json(integrity_path)
    if integrity.get("schema_version") != 2:
        raise ReleaseChainError("built v2 release requires Console integrity schema 2")
    if integrity.get("release_id") != manifest["release_id"]:
        raise ReleaseChainError("Console integrity release_id does not match")
    assets = integrity.get("assets")
    if not isinstance(assets, dict) or "./build.json" not in assets:
        raise ReleaseChainError("Console integrity must include ./build.json")
    for relative_url, expected in assets.items():
        if not isinstance(relative_url, str) or not relative_url.startswith("./") or relative_url.startswith("./."):
            raise ReleaseChainError("Console integrity contains an invalid asset path")
        expected_sha = _required_sha(
            expected,
            f"Console integrity asset {relative_url}",
        )
        asset_path = (integrity_path.parent / relative_url[2:]).resolve()
        try:
            asset_path.relative_to(integrity_path.parent.resolve())
        except ValueError as exc:
            raise ReleaseChainError(f"Console integrity asset escapes its root: {relative_url}") from exc
        if not asset_path.is_file() or sha256_file(asset_path) != expected_sha:
            raise ReleaseChainError(f"Console integrity asset checksum mismatch: {relative_url}")
    build = load_json(integrity_path.parent / "build.json")
    _validate_console_build_identity(build, manifest)
    _validate_console_archive(
        manifest,
        workspace_root=workspace_root,
        stage_root=integrity_path.parent,
    )


def validate_published_evidence(
    manifest: dict[str, Any],
    *,
    workspace_root: Path,
) -> None:
    """Require all publication bindings and local GitHub asset bytes to match."""

    _require_evidence_stage(manifest, "published")
    validate_built_evidence(manifest, workspace_root=workspace_root)
    python_artifacts = manifest.get("python", {}).get("artifacts")
    if not isinstance(python_artifacts, list) or not python_artifacts:
        raise ReleaseChainError("published manifest needs PyPI artifacts")
    pypi_names: set[str] = set()
    pypi_urls: set[str] = set()
    for artifact in python_artifacts:
        if not isinstance(artifact, dict):
            raise ReleaseChainError("published PyPI artifact must be an object")
        filename = _required_text(artifact, "filename", "published PyPI artifact")
        url = _required_https_url(
            artifact.get("url"),
            f"published PyPI artifact {filename} URL",
        )
        if Path(urlparse(url).path).name != filename:
            raise ReleaseChainError(f"published PyPI artifact URL does not end in {filename}")
        if filename in pypi_names or url in pypi_urls:
            raise ReleaseChainError("published PyPI artifact filenames and URLs must be unique")
        pypi_names.add(filename)
        pypi_urls.add(url)
        _required_sha(
            artifact.get("sha256"),
            f"published PyPI artifact {filename} sha256",
        )

    release = manifest.get("github_release")
    if not isinstance(release, dict):
        raise ReleaseChainError("published manifest needs GitHub release evidence")
    assets = release.get("assets")
    if not isinstance(assets, list) or not assets:
        raise ReleaseChainError("published manifest needs GitHub release assets")
    _required_https_url(release.get("url"), "GitHub release URL")
    names: set[str] = set()
    urls: set[str] = set()
    for asset in assets:
        if not isinstance(asset, dict):
            raise ReleaseChainError("GitHub release asset must be an object")
        name = _required_text(asset, "name", "GitHub release asset")
        url = _required_https_url(asset.get("url"), f"GitHub release asset {name} URL")
        if Path(urlparse(url).path).name != name:
            raise ReleaseChainError(f"GitHub release asset URL does not end in {name}")
        if name in names or url in urls:
            raise ReleaseChainError("GitHub release asset names and URLs must be unique")
        names.add(name)
        urls.add(url)
        path = _safe_artifact(
            workspace_root,
            _required_text(asset, "path", f"GitHub release asset {name}"),
        )
        if path.name != name:
            raise ReleaseChainError(f"GitHub release asset path does not end in {name}")
        expected = _required_sha(
            asset.get("sha256"),
            f"GitHub release asset {name} sha256",
        )
        if sha256_file(path) != expected:
            raise ReleaseChainError(f"GitHub release asset checksum mismatch: {name}")

    for artifact in manifest.get("public_artifacts", []):
        if not isinstance(artifact, dict):
            raise ReleaseChainError("published public artifact must be an object")
        _required_https_url(artifact.get("url"), "published public artifact URL")
        _required_sha(artifact.get("sha256"), "published public artifact sha256")

    docker = manifest.get("docker")
    if not isinstance(docker, dict):
        raise ReleaseChainError("published manifest needs Docker evidence")
    _required_docker_digest(
        docker.get("manifest_digest"),
        "published Docker manifest digest",
    )
    platform_digests = docker.get("platform_digests")
    if not isinstance(platform_digests, dict) or set(platform_digests) != _REQUIRED_DOCKER_PLATFORMS:
        raise ReleaseChainError("published Docker platform digests must be exactly linux/amd64 and linux/arm64")
    digests = [
        _required_docker_digest(
            platform_digests[platform],
            f"published Docker {platform} digest",
        )
        for platform in sorted(_REQUIRED_DOCKER_PLATFORMS)
    ]
    if len(set(digests)) != len(digests):
        raise ReleaseChainError("published Docker platform digests must be distinct")


def _rebuild_artifact_checksum_inventory(
    manifest: dict[str, Any],
    *,
    workspace_root: Path,
) -> None:
    """Atomically rebuild the non-self-referential external artifact BOM."""

    release = manifest.get("github_release")
    assets = release.get("assets") if isinstance(release, dict) else None
    if not isinstance(assets, list):
        raise ReleaseChainError("checksum inventory needs declared GitHub release assets")
    inventory_assets = [
        asset for asset in assets if isinstance(asset, dict) and asset.get("name") == "release-artifact-bom.sha256"
    ]
    if len(inventory_assets) != 1:
        raise ReleaseChainError("checksum inventory declaration must be unique")
    inventory = _artifact_output_path(
        workspace_root,
        Path(_required_text(inventory_assets[0], "path", "checksum inventory")),
        "checksum inventory",
    )

    included: set[Path] = set()
    for asset in assets:
        if not isinstance(asset, dict):
            raise ReleaseChainError("GitHub release asset declaration must be an object")
        if asset.get("name") == "release-artifact-bom.sha256":
            continue
        path = _safe_artifact(
            workspace_root,
            _required_text(asset, "path", "GitHub release asset"),
        )
        if path.is_symlink():
            raise ReleaseChainError(f"checksum inventory rejects symlink artifact: {path}")
        included.add(path)

    console = manifest.get("console")
    if not isinstance(console, dict):
        raise ReleaseChainError("checksum inventory needs Console evidence")
    console_integrity = _safe_artifact(
        workspace_root,
        _required_text(console, "integrity_path", "manifest console"),
    )
    for path in console_integrity.parent.rglob("*"):
        if path.is_symlink():
            raise ReleaseChainError(f"checksum inventory rejects symlink artifact: {path}")
        if path.is_file():
            included.add(path.resolve())

    artifact_root = (workspace_root / "artifacts").resolve()
    if inventory in included:
        raise ReleaseChainError("checksum inventory must not hash itself")
    lines = [
        f"{sha256_file(path)}  {path.relative_to(artifact_root).as_posix()}"
        for path in sorted(included, key=lambda candidate: candidate.relative_to(artifact_root).as_posix())
    ]
    rendered = "\n".join(lines) + "\n"
    inventory.parent.mkdir(parents=True, exist_ok=True)
    temporary = inventory.with_name(f".{inventory.name}.checksum-inventory.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(inventory)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_component_artifact_semantics(
    manifest: dict[str, Any],
    *,
    workspace_root: Path,
) -> None:
    """Bind component labels to the artifacts that actually implement them."""

    version = _required_text(manifest, "version", "release manifest")
    python_artifacts = manifest["python"]["artifacts"]
    wheels = [
        artifact
        for artifact in python_artifacts
        if isinstance(artifact, dict)
        and isinstance(artifact.get("filename"), str)
        and artifact["filename"].endswith(".whl")
    ]
    if len(wheels) != 1:
        raise ReleaseChainError("built release needs exactly one SPLime wheel")
    framework_wheel = wheels[0]
    framework_path = _required_text(
        framework_wheel,
        "path",
        "SPLime wheel",
    )
    framework_sha = _required_sha(
        framework_wheel.get("sha256"),
        "SPLime wheel sha256",
    )
    _validate_wheel_identity(
        _safe_artifact(workspace_root, framework_path),
        expected_name="splime",
        expected_version=version,
    )

    components = manifest["components"]
    for name in ("framework", "daemon"):
        artifact = components[name]["artifact"]
        if (
            artifact.get("identifier") != "splime"
            or artifact.get("path") != framework_path
            or artifact.get("sha256") != framework_sha
        ):
            raise ReleaseChainError(f"component {name} must bind the exact declared SPLime wheel")

    server_artifact = components["server"]["artifact"]
    if server_artifact.get("identifier") != "spl-server":
        raise ReleaseChainError("component server artifact identifier is invalid")
    server_path = _safe_artifact(
        workspace_root,
        _required_text(server_artifact, "path", "server component artifact"),
    )
    _validate_wheel_identity(
        server_path,
        expected_name="spl-server",
        expected_version=version,
    )

    console_artifact = components["console"]["artifact"]
    if console_artifact.get("identifier") != "splime-console":
        raise ReleaseChainError("component console artifact identifier is invalid")
    expected_console_name = f"splime-console-{version}.tar.gz"
    console_path = _safe_artifact(
        workspace_root,
        _required_text(console_artifact, "path", "Console component artifact"),
    )
    if console_path.name != expected_console_name:
        raise ReleaseChainError(f"Console component artifact must be named {expected_console_name}")


def _validate_wheel_identity(
    path: Path,
    *,
    expected_name: str,
    expected_version: str,
) -> None:
    if path.suffix != ".whl":
        raise ReleaseChainError(f"{expected_name} component artifact must be a wheel")
    try:
        with zipfile.ZipFile(path) as wheel:
            metadata_names = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
            if len(metadata_names) != 1:
                raise ReleaseChainError(f"{expected_name} wheel must contain exactly one METADATA file")
            metadata = wheel.read(metadata_names[0]).decode("utf-8")
    except (OSError, UnicodeDecodeError, zipfile.BadZipFile) as exc:
        raise ReleaseChainError(f"{expected_name} component artifact is not a readable wheel") from exc
    fields: dict[str, str] = {}
    for line in metadata.splitlines():
        key, separator, value = line.partition(":")
        if separator and key in {"Name", "Version"}:
            fields[key] = value.strip()
    normalized_name = fields.get("Name", "").casefold().replace("_", "-")
    if normalized_name != expected_name.casefold().replace("_", "-") or fields.get("Version") != expected_version:
        raise ReleaseChainError(f"{expected_name} wheel identity does not match release {expected_version}")


def _validate_console_archive(
    manifest: dict[str, Any],
    *,
    workspace_root: Path,
    stage_root: Path,
) -> None:
    """Require the Console archive to be an exact copy of the verified stage."""

    version = _required_text(manifest, "version", "release manifest")
    artifact = manifest["components"]["console"]["artifact"]
    archive_path = _safe_artifact(
        workspace_root,
        _required_text(artifact, "path", "Console component artifact"),
    )
    try:
        archive_path.relative_to(stage_root.resolve())
    except ValueError:
        pass
    else:
        raise ReleaseChainError("Console archive must be outside its staged payload directory")

    expected_files = {
        path.relative_to(stage_root).as_posix(): sha256_file(path) for path in stage_root.rglob("*") if path.is_file()
    }
    archive_root = f"splime-console-{version}"
    observed_files: dict[str, str] = {}
    observed_names: set[str] = set()
    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            for member in archive.getmembers():
                if member.name in observed_names:
                    raise ReleaseChainError("Console archive contains duplicate members")
                observed_names.add(member.name)
                path = Path(member.name)
                if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != archive_root:
                    raise ReleaseChainError("Console archive contains an unsafe member path")
                if member.isdir():
                    continue
                if not member.isfile():
                    raise ReleaseChainError("Console archive contains a non-regular member")
                relative = Path(*path.parts[1:]).as_posix()
                stream = archive.extractfile(member)
                if stream is None:
                    raise ReleaseChainError("Console archive member cannot be read")
                observed_files[relative] = hashlib.sha256(stream.read()).hexdigest()
    except (OSError, tarfile.TarError) as exc:
        raise ReleaseChainError("Console component artifact is not a readable tar.gz archive") from exc
    if observed_files != expected_files:
        raise ReleaseChainError("Console archive does not exactly match the verified staged payload")


def validate_deployment_receipt(
    manifest: dict[str, Any],
    *,
    workspace_root: Path,
    receipt_path: Path,
    manifest_path: Path | None = None,
) -> None:
    """Require an allowlisted staged receipt to match the built release."""

    receipt = load_json(receipt_path)
    extra = set(receipt) - _RECEIPT_KEYS
    if extra:
        raise ReleaseChainError(f"deployment receipt contains non-allowlisted keys: {sorted(extra)!r}")
    for key in _RECEIPT_KEYS:
        if key not in receipt:
            raise ReleaseChainError(f"deployment receipt is missing {key}")
    if receipt.get("schema_version") != 1 or receipt.get("component") != "server":
        raise ReleaseChainError("deployment receipt identity is invalid")
    if receipt.get("release_id") != manifest.get("release_id"):
        raise ReleaseChainError("deployment receipt release_id does not match")
    if receipt.get("version") != manifest.get("packages", {}).get("server"):
        raise ReleaseChainError("deployment receipt server version does not match")

    server = manifest["components"]["server"]
    server_commit = _required_git_commit(
        server.get("source_commit"),
        "manifest server source_commit",
    )
    server_artifact_sha = _required_sha(
        server.get("artifact", {}).get("sha256"),
        "manifest server artifact sha256",
    )
    _required_git_commit(receipt.get("source_commit"), "deployment receipt source_commit")
    _required_sha(receipt.get("artifact_sha256"), "deployment receipt artifact_sha256")
    _required_sha(
        receipt.get("release_manifest_sha256"),
        "deployment receipt release_manifest_sha256",
    )
    if receipt.get("source_ref") != server.get("source_ref"):
        raise ReleaseChainError("deployment receipt source_ref does not match")
    if receipt.get("source_commit") != server_commit:
        raise ReleaseChainError("deployment receipt source_commit does not match")
    if receipt.get("artifact_sha256") != server_artifact_sha:
        raise ReleaseChainError("deployment receipt artifact checksum does not match")

    checked_manifest_path = (
        manifest_path if manifest_path is not None else workspace_root / "spl" / "release-manifest.json"
    )
    manifest_sha = sha256_file(checked_manifest_path)
    if receipt.get("release_manifest_sha256") != manifest_sha:
        raise ReleaseChainError("deployment receipt manifest checksum does not match")
    if receipt.get("schema_target") != manifest.get("server", {}).get("schema_target"):
        raise ReleaseChainError("deployment receipt schema target does not match")
    _required_iso_timestamp(
        receipt.get("deployed_at"),
        "deployment receipt deployed_at",
    )
    environment_class = _required_text(receipt, "environment_class", "deployment receipt")
    if environment_class not in {"staging", "production"}:
        raise ReleaseChainError("deployment receipt environment_class is invalid")


def verify_release_chain(
    *,
    workspace_root: Path,
    contract_path: Path,
    manifest_path: Path,
    stage: str,
    receipt_path: Path | None = None,
) -> None:
    if stage not in _STAGES:
        raise ReleaseChainError(f"unknown release stage {stage!r}")
    contract = load_json(contract_path)
    manifest = load_json(manifest_path)
    declaration = load_json(workspace_root / "spl" / "release-manifest.json")
    if stage in {"source", "built", "published", "deployed"}:
        _require_artifact_side_file(
            workspace_root,
            manifest_path,
            "source and build evidence manifest",
        )
    validate_declared_contract(contract, manifest, workspace_root=workspace_root)
    validate_manifest_declaration_projection(manifest, declaration)
    _require_evidence_stage(manifest, stage)
    if stage in {"source", "built", "published", "deployed"}:
        validate_source_pins(
            contract,
            workspace_root=workspace_root,
            manifest=manifest,
        )
    if stage in {"built", "published", "deployed"}:
        validate_built_evidence(manifest, workspace_root=workspace_root)
    if stage == "published":
        validate_published_evidence(manifest, workspace_root=workspace_root)
    if stage == "deployed":
        if receipt_path is None:
            raise ReleaseChainError("deployed verification requires --receipt")
        validate_deployment_receipt(
            manifest,
            workspace_root=workspace_root,
            receipt_path=receipt_path,
            manifest_path=manifest_path,
        )


def _validate_manifest_declaration(manifest: dict[str, Any]) -> None:
    state = manifest.get("evidence", {}).get("state")
    if state not in {
        "declared",
        "source",
        "built",
        "published",
        "deployed",
        "verified",
        "mismatch",
        "unknown",
    }:
        raise ReleaseChainError("manifest evidence.state is invalid")
    server = manifest.get("server")
    if not isinstance(server, dict) or not isinstance(server.get("schema_target"), int):
        raise ReleaseChainError("manifest server.schema_target must be an integer")
    if server["schema_target"] <= 0:
        raise ReleaseChainError("manifest server.schema_target must be positive")
    if state in {"deployed", "verified"}:
        raise ReleaseChainError("deployment and verification are receipt outcomes, not manifest declarations")
    source_date_epoch = manifest.get("source_date_epoch")
    if source_date_epoch is not None and (
        isinstance(source_date_epoch, bool) or not isinstance(source_date_epoch, int) or source_date_epoch <= 0
    ):
        raise ReleaseChainError("manifest source_date_epoch must be null or a positive integer")
    evidence = manifest.get("evidence")
    if not isinstance(evidence, dict) or set(evidence) != {
        "state",
        "reason_code",
        "generated_at",
    }:
        raise ReleaseChainError("manifest evidence must use the exact declaration keys")
    _required_text(evidence, "reason_code", "manifest evidence")
    if state in {"source", "built", "published"}:
        if source_date_epoch is None:
            raise ReleaseChainError("source and built manifests require source_date_epoch")
        _required_iso_timestamp(
            evidence.get("generated_at"),
            "manifest evidence.generated_at",
        )
        for name, component in manifest["components"].items():
            _required_git_commit(
                component.get("source_commit"),
                f"manifest component {name}.source_commit",
            )
    if state in {"built", "published"}:
        for name, component in manifest["components"].items():
            artifact = component.get("artifact")
            if not isinstance(artifact, dict):
                raise ReleaseChainError(f"manifest component {name}.artifact is missing")
            _required_text(artifact, "path", f"manifest component {name}.artifact")
            _required_sha(
                artifact.get("sha256"),
                f"manifest component {name}.artifact sha256",
            )
        _required_sha(
            manifest.get("console", {}).get("integrity_sha256"),
            "manifest console.integrity_sha256",
        )
    if state == "published":
        python_artifacts = manifest.get("python", {}).get("artifacts")
        public_artifacts = manifest.get("public_artifacts")
        if not isinstance(python_artifacts, list) or not python_artifacts:
            raise ReleaseChainError("published manifest needs Python artifacts")
        if not isinstance(public_artifacts, list) or not public_artifacts:
            raise ReleaseChainError("published manifest needs public artifacts")
        for artifact in python_artifacts:
            filename = _required_text(artifact, "filename", "published PyPI artifact")
            url = _required_https_url(
                artifact.get("url"),
                f"published PyPI artifact {filename} URL",
            )
            if Path(urlparse(url).path).name != filename:
                raise ReleaseChainError(f"published PyPI artifact URL does not end in {filename}")
            _required_sha(artifact.get("sha256"), "published PyPI artifact sha256")
        for artifact in public_artifacts:
            _required_sha(artifact.get("sha256"), "published artifact sha256")
        github_assets = manifest.get("github_release", {}).get("assets")
        if not isinstance(github_assets, list) or not github_assets:
            raise ReleaseChainError("published manifest needs GitHub release assets")
        for artifact in github_assets:
            if not isinstance(artifact, dict):
                raise ReleaseChainError("published GitHub release asset must be an object")
            _required_sha(
                artifact.get("sha256"),
                f"published GitHub asset {artifact.get('name')!r} sha256",
            )
        docker = manifest.get("docker", {})
        _required_docker_digest(
            docker.get("manifest_digest"),
            "published Docker manifest digest",
        )
        platform_digests = docker.get("platform_digests")
        if not isinstance(platform_digests, dict) or set(platform_digests) != _REQUIRED_DOCKER_PLATFORMS:
            raise ReleaseChainError("published Docker platform digests must be exactly linux/amd64 and linux/arm64")
        for platform, digest in platform_digests.items():
            _required_docker_digest(
                digest,
                f"published Docker {platform} digest",
            )


def _validate_console_build_identity(
    build: dict[str, Any],
    manifest: dict[str, Any],
) -> None:
    if set(build) != {
        "schema_version",
        "component",
        "release_id",
        "version",
        "evidence_state",
        "source",
        "build",
        "contracts",
    }:
        raise ReleaseChainError("Console build identity contains non-allowlisted fields")
    if build.get("schema_version") != 1 or build.get("component") != "console":
        raise ReleaseChainError("Console build identity contract is invalid")
    if build.get("release_id") != manifest.get("release_id") or build.get("version") != manifest.get(
        "packages", {}
    ).get("console"):
        raise ReleaseChainError("Console build identity does not match the manifest")
    if build.get("evidence_state") not in {"built", "published"}:
        raise ReleaseChainError("Console build identity is not built evidence")
    source = build.get("source")
    console = manifest["components"]["console"]
    if not isinstance(source, dict) or set(source) != {
        "repository",
        "ref",
        "binding",
        "commit",
    }:
        raise ReleaseChainError("Console build source contains non-allowlisted fields")
    if source.get("binding") != "pinned_commit":
        raise ReleaseChainError("Console build source binding is invalid")
    if source.get("repository") != console.get("repository"):
        raise ReleaseChainError("Console build source repository does not match")
    if source.get("ref") != console.get("source_ref"):
        raise ReleaseChainError("Console build source ref does not match")
    commit = _required_git_commit(source.get("commit"), "Console build source commit")
    if commit != console.get("source_commit"):
        raise ReleaseChainError("Console build source commit does not match")
    _required_iso_timestamp(build.get("build", {}).get("built_at"), "Console build time")
    if build.get("contracts") != console.get("contracts"):
        raise ReleaseChainError("Console build contracts do not match")
    build_fields = build.get("build")
    if not isinstance(build_fields, dict) or set(build_fields) != {"built_at"}:
        raise ReleaseChainError("Console build identity must not embed its own final bundle checksum")


def _validate_public_versions(
    *,
    workspace_root: Path,
    version: str,
    release_id: str,
) -> None:
    expected = {
        workspace_root / "spl" / "pyproject.toml": rf'^version = "{re.escape(version)}"$',
        workspace_root / "spl" / "docs" / "source" / "conf.py": rf'^release = "{re.escape(version)}"$',
        workspace_root / "spl-server" / "pyproject.toml": rf'^version = "{re.escape(version)}"$',
        workspace_root / "spl-frontend" / "package.json": rf'"version": "{re.escape(version)}"',
        workspace_root / "spl-frontend" / "config.js": rf'APP_RELEASE_ID = "{re.escape(release_id)}"',
        workspace_root / "spl-frontend" / "index.html": re.escape(f'const releaseId = "{release_id}"'),
    }
    for path, pattern in expected.items():
        if not path.is_file():
            raise ReleaseChainError(f"public version source is missing: {path}")
        if re.search(pattern, path.read_text(encoding="utf-8"), re.MULTILINE) is None:
            raise ReleaseChainError(f"public version source is stale: {path}")
    server_identity = load_json(workspace_root / "spl-server" / "src" / "daemon_server" / "release-identity.json")
    if server_identity.get("release_id") != release_id or server_identity.get("version") != version:
        raise ReleaseChainError("generated server release identity is stale")
    if (
        server_identity.get("evidence_state") != "declared"
        or server_identity.get("source", {}).get("commit") is not None
        or server_identity.get("artifact_sha256") is not None
        or server_identity.get("release_manifest_sha256") is not None
    ):
        raise ReleaseChainError("tracked server release identity must remain declaration-only")


def _safe_workspace(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if path.parent != root.resolve():
        raise ReleaseChainError(f"component workspace escapes workspace root: {relative}")
    if not (path / ".git").exists():
        raise ReleaseChainError(f"component workspace is not a Git repository: {path}")
    return path


def _safe_artifact(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    artifact_root = (root / "artifacts").resolve()
    try:
        path.relative_to(artifact_root)
    except ValueError as exc:
        raise ReleaseChainError(f"artifact path is not under the staging root: {relative}") from exc
    if not path.is_file():
        raise ReleaseChainError(f"artifact is missing: {path}")
    return path


def _artifact_output_path(
    workspace_root: Path,
    path: Path,
    label: str,
) -> Path:
    resolved = path.resolve() if path.is_absolute() else (workspace_root / path).resolve()
    artifact_root = (workspace_root / "artifacts").resolve()
    try:
        resolved.relative_to(artifact_root)
    except ValueError as exc:
        raise ReleaseChainError(f"{label} must be under the artifact staging root") from exc
    return resolved


def _require_artifact_side_file(
    workspace_root: Path,
    path: Path,
    label: str,
) -> Path:
    resolved = path.resolve()
    artifact_root = (workspace_root / "artifacts").resolve()
    try:
        resolved.relative_to(artifact_root)
    except ValueError as exc:
        raise ReleaseChainError(f"{label} must be under the artifact staging root") from exc
    if not resolved.is_file():
        raise ReleaseChainError(f"{label} is missing: {resolved}")
    return resolved


def _require_evidence_stage(manifest: dict[str, Any], stage: str) -> None:
    state = manifest.get("evidence", {}).get("state")
    allowed = {
        "contract": {
            "declared",
            "source",
            "built",
            "published",
            "mismatch",
            "unknown",
        },
        "source": {"source", "built", "published"},
        "built": {"built", "published"},
        "published": {"published"},
        "deployed": {"built", "published"},
    }
    if state not in allowed[stage]:
        raise ReleaseChainError(f"release stage {stage} cannot be proven by evidence state {state!r}")


def _git(workspace: Path, *args: str) -> str:
    process = subprocess.run(
        ["git", *args],
        cwd=workspace,
        check=False,
        capture_output=True,
        text=True,
    )
    if process.returncode:
        detail = process.stderr.strip() or process.stdout.strip()
        raise ReleaseChainError(f"git {' '.join(args)} failed in {workspace}: {detail}")
    return process.stdout.strip()


def _required_text(payload: dict[str, Any], key: str, label: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ReleaseChainError(f"{label}.{key} must be non-empty text")
    return value


def _required_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ReleaseChainError(f"{label} must be a lowercase SHA-256")
    return value


def _required_docker_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or _DOCKER_DIGEST_RE.fullmatch(value) is None:
        raise ReleaseChainError(f"{label} must be an immutable sha256:<64-hex> digest")
    return value


def _required_https_url(value: Any, label: str) -> str:
    if not isinstance(value, str) or any(character.isspace() for character in value):
        raise ReleaseChainError(f"{label} must be a credential-free HTTPS URL")
    try:
        parsed = urlparse(value)
        valid = (
            parsed.scheme == "https"
            and parsed.hostname is not None
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
        )
    except ValueError:
        valid = False
    if not valid:
        raise ReleaseChainError(f"{label} must be a credential-free HTTPS URL")
    return value


def _required_git_commit(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) not in {40, 64}
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ReleaseChainError(f"{label} must be a full lowercase Git object id")
    return value


def _required_iso_timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ReleaseChainError(f"{label} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReleaseChainError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReleaseChainError(f"{label} must include a timezone")
    return value


def _parse_cli_map(
    items: list[str],
    *,
    option: str,
    value_factory: Any,
) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in items:
        key, separator, value = item.partition("=")
        if not separator or not key or not value:
            raise ReleaseChainError(f"{option} must use NAME=VALUE")
        if key in parsed:
            raise ReleaseChainError(f"duplicate {option} key {key!r}")
        parsed[key] = value_factory(value)
    return parsed


def _parse_pypi_artifacts(items: list[str]) -> dict[str, tuple[str, str]]:
    parsed: dict[str, tuple[str, str]] = {}
    for item in items:
        filename, separator, remainder = item.partition("=")
        url, digest_separator, digest = remainder.rpartition("=")
        if not separator or not digest_separator or not filename or not url or not digest:
            raise ReleaseChainError("--pypi-artifact must use FILENAME=URL=SHA256")
        if filename in parsed:
            raise ReleaseChainError(f"duplicate --pypi-artifact key {filename!r}")
        parsed[filename] = (url, digest)
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser()
    default_root = Path(__file__).resolve().parents[2]
    parser.add_argument("--workspace-root", type=Path, default=default_root)
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "release-contract.json",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "release-manifest.json",
    )
    parser.add_argument("--stage", choices=_STAGES, default="contract")
    parser.add_argument("--receipt", type=Path)
    parser.add_argument(
        "--emit-source-evidence",
        type=Path,
        help="write a post-commit source manifest below <workspace>/artifacts",
    )
    parser.add_argument(
        "--emit-built-evidence",
        type=Path,
        help="write exact built evidence below <workspace>/artifacts",
    )
    parser.add_argument(
        "--emit-published-evidence",
        type=Path,
        help="write exact publication-bound evidence below <workspace>/artifacts",
    )
    parser.add_argument(
        "--component-artifact",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="staged component artifact; repeat for all four components",
    )
    parser.add_argument(
        "--github-asset",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="reviewed GitHub release asset; repeat for every declared non-manifest asset",
    )
    parser.add_argument(
        "--pypi-artifact",
        action="append",
        default=[],
        metavar="FILENAME=URL=SHA256",
        help="observed PyPI filename, credential-free HTTPS URL, and SHA-256; repeat for wheel and sdist",
    )
    parser.add_argument(
        "--public-artifact",
        action="append",
        default=[],
        metavar="URL=SHA256",
        help="observed public artifact URL and SHA-256; repeat for every declaration",
    )
    parser.add_argument(
        "--docker-manifest-digest",
        help="published multi-architecture Docker manifest digest",
    )
    parser.add_argument(
        "--docker-platform-digest",
        action="append",
        default=[],
        metavar="PLATFORM=DIGEST",
        help="published Docker platform digest; requires linux/amd64 and linux/arm64",
    )
    parser.add_argument(
        "--observed-at",
        help="timezone-aware ISO-8601 observation time for emitted evidence",
    )
    args = parser.parse_args()
    try:
        emitters = [
            args.emit_source_evidence,
            args.emit_built_evidence,
            args.emit_published_evidence,
        ]
        if sum(output is not None for output in emitters) > 1:
            raise ReleaseChainError(
                "--emit-source-evidence, --emit-built-evidence, and --emit-published-evidence are mutually exclusive"
            )
        if args.emit_source_evidence is not None:
            if args.stage != "contract" or args.receipt is not None:
                raise ReleaseChainError("--emit-source-evidence cannot be combined with a later-stage verification")
            if not args.observed_at:
                raise ReleaseChainError("--emit-source-evidence requires --observed-at")
            contract = load_json(args.contract.resolve())
            declaration = load_json(args.manifest.resolve())
            payload = materialize_source_evidence(
                contract,
                declaration,
                workspace_root=args.workspace_root.resolve(),
                observed_at=args.observed_at,
            )
            write_source_evidence(
                workspace_root=args.workspace_root.resolve(),
                output_path=args.emit_source_evidence,
                payload=payload,
            )
            print("release source evidence generated")
            return 0
        if args.emit_built_evidence is not None:
            if args.stage != "contract" or args.receipt is not None:
                raise ReleaseChainError("--emit-built-evidence cannot be combined with a later-stage verification")
            if not args.observed_at:
                raise ReleaseChainError("--emit-built-evidence requires --observed-at")
            component_artifacts: dict[str, Path] = {}
            for item in args.component_artifact:
                name, separator, path_text = item.partition("=")
                if not separator or not name or not path_text:
                    raise ReleaseChainError("--component-artifact must use NAME=PATH")
                if name in component_artifacts:
                    raise ReleaseChainError(f"duplicate component artifact {name!r}")
                component_artifacts[name] = Path(path_text)
            contract = load_json(args.contract.resolve())
            source_manifest = load_json(args.manifest.resolve())
            payload = materialize_built_evidence(
                contract,
                source_manifest,
                workspace_root=args.workspace_root.resolve(),
                component_artifacts=component_artifacts,
                observed_at=args.observed_at,
            )
            write_source_evidence(
                workspace_root=args.workspace_root.resolve(),
                output_path=args.emit_built_evidence,
                payload=payload,
            )
            print("release built evidence generated")
            return 0
        if args.emit_published_evidence is not None:
            if args.stage != "contract" or args.receipt is not None:
                raise ReleaseChainError("--emit-published-evidence cannot be combined with a later-stage verification")
            if not args.observed_at:
                raise ReleaseChainError("--emit-published-evidence requires --observed-at")
            if not args.docker_manifest_digest:
                raise ReleaseChainError("--emit-published-evidence requires --docker-manifest-digest")
            github_assets = _parse_cli_map(
                args.github_asset,
                option="--github-asset",
                value_factory=Path,
            )
            pypi_artifacts = _parse_pypi_artifacts(args.pypi_artifact)
            public_artifact_hashes = _parse_cli_map(
                args.public_artifact,
                option="--public-artifact",
                value_factory=str,
            )
            docker_platform_digests = _parse_cli_map(
                args.docker_platform_digest,
                option="--docker-platform-digest",
                value_factory=str,
            )
            contract = load_json(args.contract.resolve())
            built_manifest = load_json(args.manifest.resolve())
            payload = materialize_published_evidence(
                contract,
                built_manifest,
                workspace_root=args.workspace_root.resolve(),
                pypi_artifacts=pypi_artifacts,
                github_assets=github_assets,
                public_artifact_hashes=public_artifact_hashes,
                docker_manifest_digest=args.docker_manifest_digest,
                docker_platform_digests=docker_platform_digests,
                observed_at=args.observed_at,
            )
            write_source_evidence(
                workspace_root=args.workspace_root.resolve(),
                output_path=args.emit_published_evidence,
                payload=payload,
            )
            print("release published evidence generated")
            return 0
        verify_release_chain(
            workspace_root=args.workspace_root.resolve(),
            contract_path=args.contract.resolve(),
            manifest_path=args.manifest.resolve(),
            stage=args.stage,
            receipt_path=args.receipt.resolve() if args.receipt else None,
        )
    except ReleaseChainError as exc:
        parser.exit(1, f"release chain {args.stage} failed: {exc}\n")
    print(f"release chain {args.stage} verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
