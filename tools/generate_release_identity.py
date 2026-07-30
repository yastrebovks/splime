"""Generate the public SPLime release identity from one reviewed contract.

Artifact hashes, source commits, build times, and deployment receipts are
evidence produced later. This generator deliberately emits them as ``null``
for a new declaration instead of guessing or promoting a dirty worktree.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any

from tools.release_chain import (
    ReleaseChainError,
    load_json,
    validate_declared_contract,
    validate_manifest_declaration_projection,
)


def release_manifest(contract: dict[str, Any]) -> dict[str, Any]:
    version = contract["version"]
    release_id = contract["release_id"]
    release_manifest_url = f"https://github.com/yastrebovks/splime/releases/download/v{version}/release-manifest.json"
    github_asset_base = f"https://github.com/yastrebovks/splime/releases/download/v{version}"
    packages = {name: component["version"] for name, component in contract["components"].items()}
    components = {
        name: {
            "repository": component["repository"],
            "source_ref": component["source_ref"],
            "source_binding": component["source_binding"],
            "source_commit": component.get("source_commit"),
            "package_version": component["version"],
            "artifact": {
                "identifier": component["artifact"],
                "path": None,
                "sha256": None,
            },
            "contracts": _component_contracts(name, contract["contracts"]),
        }
        for name, component in contract["components"].items()
    }
    return {
        "schema_version": 2,
        "release_id": release_id,
        "version": version,
        "source_date_epoch": None,
        "packages": packages,
        "python": {
            "distribution": "splime",
            "requires_python": ">=3.13",
            "project_url": f"https://pypi.org/project/splime/{version}/",
            "install_requirement": f"splime=={version}",
            "artifacts": [
                {
                    "filename": f"splime-{version}-py3-none-any.whl",
                    "path": f"artifacts/python/splime-{version}-py3-none-any.whl",
                    "url": None,
                    "sha256": None,
                },
                {
                    "filename": f"splime-{version}.tar.gz",
                    "path": f"artifacts/python/splime-{version}.tar.gz",
                    "url": None,
                    "sha256": None,
                },
            ],
        },
        "console": {
            "url": "https://splime.io/app/",
            "integrity_url": (f"https://splime.io/app/static-integrity.json?release={release_id}"),
            "integrity_path": "artifacts/console/static-integrity.json",
            "integrity_sha256": None,
            "cache_policy": ("unhashed assets revalidate; only content-hashed assets are immutable"),
        },
        "server": {
            "schema_target": contract["server_schema_target"],
            "deployment_receipt_schema": contract["contracts"]["deployment_receipt"],
        },
        "contracts": contract["contracts"],
        "compatibility": {
            "matrix": contract["compatibility_matrix"],
            "console_server": "lockstep",
            "daemon_server": "capability_negotiated",
            "n_minus_one": "only for explicitly tested matrix rows",
        },
        "components": components,
        "install_page_urls": [
            "https://splime.io/install.html",
            f"https://pypi.org/project/splime/{version}/",
            release_manifest_url,
        ],
        "public_artifacts": [
            {
                "url": "https://splime.io/downloads/splime-cookbook.ipynb",
                "sha256": None,
            }
        ],
        "manifest_url": release_manifest_url,
        "github_release": {
            "url": f"https://github.com/yastrebovks/splime/releases/tag/v{version}",
            "assets": [
                {
                    "name": "source-release-manifest.json",
                    "path": "artifacts/source-release-manifest.json",
                    "url": f"{github_asset_base}/source-release-manifest.json",
                    "sha256": None,
                },
                {
                    "name": "release-artifact-bom.sha256",
                    "path": "artifacts/release-artifact-bom.sha256",
                    "url": f"{github_asset_base}/release-artifact-bom.sha256",
                    "sha256": None,
                },
                {
                    "name": f"splime-{version}-py3-none-any.whl",
                    "path": f"artifacts/python/splime-{version}-py3-none-any.whl",
                    "url": f"{github_asset_base}/splime-{version}-py3-none-any.whl",
                    "sha256": None,
                },
                {
                    "name": f"splime-{version}.tar.gz",
                    "path": f"artifacts/python/splime-{version}.tar.gz",
                    "url": f"{github_asset_base}/splime-{version}.tar.gz",
                    "sha256": None,
                },
                {
                    "name": f"spl_server-{version}-py3-none-any.whl",
                    "path": f"artifacts/server/spl_server-{version}-py3-none-any.whl",
                    "url": f"{github_asset_base}/spl_server-{version}-py3-none-any.whl",
                    "sha256": None,
                },
                {
                    "name": f"splime-console-{version}.tar.gz",
                    "path": f"artifacts/splime-console-{version}.tar.gz",
                    "url": f"{github_asset_base}/splime-console-{version}.tar.gz",
                    "sha256": None,
                },
                {
                    "name": "static-integrity.json",
                    "path": "artifacts/console/static-integrity.json",
                    "url": f"{github_asset_base}/static-integrity.json",
                    "sha256": None,
                },
            ],
        },
        "docker": {
            "repository": "yastrebovks/spl-daemon",
            "tag": version,
            "source_repository": contract["components"]["framework"]["repository"],
            "source_ref": contract["source_tag"],
            "source_path": "deploy/dockerhub",
            "manifest_digest": None,
            "platform_digests": {
                "linux/amd64": None,
                "linux/arm64": None,
            },
            "advertised_in_console": False,
            "verification_url": (f"https://hub.docker.com/v2/repositories/yastrebovks/spl-daemon/tags/{version}"),
            "publication_url": (f"https://hub.docker.com/r/yastrebovks/spl-daemon/tags?name={version}"),
        },
        "publication_order": [
            "pypi",
            "docker_hub",
            "github_release",
            "landing_and_console",
        ],
        "evidence": {
            "state": "declared",
            "reason_code": "release_evidence_pending",
            "generated_at": None,
        },
    }


def console_build_identity(
    contract: dict[str, Any],
    *,
    source_commit: str,
    built_at: str,
) -> dict[str, Any]:
    """Return artifact-side Console build evidence.

    This payload is generated only after the source commit and payload artifact
    exist. It belongs in a staging/artifact directory, not in the component's
    tracked source tree.
    """

    if (
        len(source_commit) not in {40, 64}
        or source_commit != source_commit.casefold()
        or any(character not in "0123456789abcdef" for character in source_commit)
    ):
        raise ReleaseChainError("Console build source commit must be a full lowercase Git object id")
    try:
        timestamp = datetime.fromisoformat(built_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReleaseChainError("Console build time must be ISO-8601") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ReleaseChainError("Console build time must include a timezone")

    component = contract["components"]["console"]
    return {
        "schema_version": 1,
        "component": "console",
        "release_id": contract["release_id"],
        "version": contract["version"],
        "evidence_state": "built",
        "source": {
            "repository": component["repository"],
            "ref": component["source_ref"],
            "binding": component["source_binding"],
            "commit": source_commit,
        },
        "build": {
            "built_at": built_at,
        },
        "contracts": {
            "console_server": contract["contracts"]["console_server"],
            "persisted_data": contract["contracts"]["console_persisted_data"],
        },
    }


def console_source_identity(contract: dict[str, Any]) -> dict[str, Any]:
    """Return the legacy null Console projection for compatibility tests.

    The identity generator intentionally does not write this payload into the
    Console source tree. ``build.json`` is generated only as artifact-side
    evidence after the exact Console commit is known.
    """

    component = contract["components"]["console"]
    return {
        "schema_version": 1,
        "component": "console",
        "release_id": contract["release_id"],
        "version": contract["version"],
        "evidence_state": "declared",
        "source": {
            "repository": component["repository"],
            "ref": component["source_ref"],
            "binding": component["source_binding"],
            "commit": None,
        },
        "build": {
            "built_at": None,
        },
        "contracts": {
            "console_server": contract["contracts"]["console_server"],
            "persisted_data": contract["contracts"]["console_persisted_data"],
        },
    }


def server_release_identity(contract: dict[str, Any]) -> dict[str, Any]:
    """Return the source-controlled server declaration.

    A declaration may identify the ref and binding policy, but it must never
    contain the commit or artifact hashes of the repository that contains it.
    Those values only exist in the central manifest and deployment receipt
    after the component commit is immutable.
    """

    component = contract["components"]["server"]
    return {
        "schema_version": 1,
        "component": "server",
        "release_id": contract["release_id"],
        "version": contract["version"],
        "evidence_state": "declared",
        "source": {
            "repository": component["repository"],
            "ref": component["source_ref"],
            "binding": component["source_binding"],
            "commit": None,
        },
        "artifact_sha256": None,
        "release_manifest_sha256": None,
        "schema_target": contract["server_schema_target"],
        "contracts": {
            "console_server": contract["contracts"]["console_server"],
            "daemon_server_capabilities": contract["contracts"]["daemon_server_capabilities"],
        },
    }


def generate(
    *,
    workspace_root: Path,
    contract: dict[str, Any],
    check: bool,
    new_declaration: bool = False,
) -> None:
    manifest_path = workspace_root / "spl" / "release-manifest.json"
    declaration = release_manifest(contract)
    exact_json_outputs = {
        workspace_root / "spl-server" / "src" / "daemon_server" / "release-identity.json": (
            server_release_identity(contract)
        ),
    }
    exact_rendered_outputs = {
        path: f"{json.dumps(payload, indent=2, sort_keys=False)}\n" for path, payload in exact_json_outputs.items()
    }
    exact_rendered_outputs.update(
        _render_text_versions(
            workspace_root=workspace_root,
            contract=contract,
        )
    )
    stale_exact = [
        path
        for path, rendered in exact_rendered_outputs.items()
        if not path.is_file() or path.read_text(encoding="utf-8") != rendered
    ]
    if check:
        if stale_exact:
            raise ReleaseChainError(f"generated release identity is stale: {stale_exact[0]}")
        if not manifest_path.is_file():
            raise ReleaseChainError(f"generated release identity is stale: {manifest_path}")
        manifest = load_json(manifest_path)
        validate_declared_contract(
            contract,
            manifest,
            workspace_root=workspace_root,
        )
        rendered_manifest = f"{json.dumps(declaration, indent=2, sort_keys=False)}\n"
        if manifest_path.read_text(encoding="utf-8") != rendered_manifest:
            raise ReleaseChainError(
                "tracked release-manifest must remain the generated declaration; "
                "mature evidence belongs under the artifact staging root"
            )
        return
    if not new_declaration:
        raise ReleaseChainError(
            "writing identities requires --new-declaration; this prevents mature evidence from being reset to null"
        )
    rendered_outputs = {
        manifest_path: f"{json.dumps(declaration, indent=2, sort_keys=False)}\n",
        **exact_rendered_outputs,
    }
    stale = [
        path
        for path, rendered in rendered_outputs.items()
        if not path.is_file() or path.read_text(encoding="utf-8") != rendered
    ]
    temporary_outputs: dict[Path, Path] = {}
    try:
        for path in stale:
            temporary = path.with_name(f".{path.name}.release-identity.tmp")
            temporary.write_text(rendered_outputs[path], encoding="utf-8")
            temporary_outputs[path] = temporary
        for path, temporary in temporary_outputs.items():
            temporary.replace(path)
    finally:
        for temporary in temporary_outputs.values():
            temporary.unlink(missing_ok=True)


def _render_text_versions(
    *,
    workspace_root: Path,
    contract: dict[str, Any],
) -> dict[Path, str]:
    version = contract["version"]
    release_id = contract["release_id"]
    replacements = {
        workspace_root / "spl" / "pyproject.toml": [
            (r'(?m)^version = "[^"]+"$', f'version = "{version}"'),
        ],
        workspace_root / "spl" / "docs" / "source" / "conf.py": [
            (r'(?m)^release = "[^"]+"$', f'release = "{version}"'),
        ],
        workspace_root / "spl-server" / "pyproject.toml": [
            (r'(?m)^version = "[^"]+"$', f'version = "{version}"'),
        ],
        workspace_root / "spl-frontend" / "package.json": [
            (
                r'(?m)^  "version": "[^"]+",',
                f'  "version": "{version}",',
            ),
        ],
        workspace_root / "spl-frontend" / "config.js": [
            (
                r'(?m)^export const APP_VERSION = "[^"]+";$',
                f'export const APP_VERSION = "{version}";',
            ),
            (
                r'(?m)^export const APP_RELEASE_ID = "[^"]+";$',
                f'export const APP_RELEASE_ID = "{release_id}";',
            ),
            (
                r'(?m)^export const RELEASE_MANIFEST_URL = "[^"]+";$',
                "export const RELEASE_MANIFEST_URL = "
                '"https://github.com/yastrebovks/splime/releases/download/'
                f'v{version}/release-manifest.json";',
            ),
            (
                r'(?m)^export const PYPI_RELEASE_URL = "[^"]+";$',
                f'export const PYPI_RELEASE_URL = "https://pypi.org/project/splime/{version}/";',
            ),
            (
                r'(?m)^  framework: "[^"]+",',
                f'  framework: "{version}",',
            ),
            (
                r'(?m)^  daemon: "[^"]+",',
                f'  daemon: "{version}",',
            ),
        ],
        workspace_root / "spl-frontend" / "index.html": [
            (r"splime-\d+\.\d+\.\d+", release_id),
        ],
    }
    rendered_outputs: dict[Path, str] = {}
    for path, rules in replacements.items():
        original = path.read_text(encoding="utf-8")
        rendered = original
        for pattern, replacement in rules:
            rendered, count = re.subn(pattern, replacement, rendered)
            if count == 0:
                raise ReleaseChainError(f"public version pattern {pattern!r} was not found in {path}")
        rendered_outputs[path] = rendered
    return rendered_outputs


def _component_contracts(
    name: str,
    contracts: dict[str, Any],
) -> dict[str, Any]:
    if name == "console":
        return {
            "console_server": contracts["console_server"],
            "persisted_data": contracts["console_persisted_data"],
        }
    if name == "server":
        return {
            "console_server": contracts["console_server"],
            "daemon_server_capabilities": contracts["daemon_server_capabilities"],
            "deployment_receipt": contracts["deployment_receipt"],
        }
    if name == "daemon":
        return {
            "daemon_server_capabilities": contracts["daemon_server_capabilities"],
            "manifest_evidence": contracts["manifest_evidence"],
        }
    return {"manifest_evidence": contracts["manifest_evidence"]}


def write_console_build_identity(
    *,
    workspace_root: Path,
    output_path: Path,
    payload: dict[str, Any],
    check: bool,
) -> None:
    """Write or check build evidence only below the artifact staging root."""

    output = _artifact_side_output(
        workspace_root=workspace_root,
        output_path=output_path,
        label="Console build evidence",
    )
    rendered = f"{json.dumps(payload, indent=2, sort_keys=False)}\n"
    if check:
        if not output.is_file() or output.read_text(encoding="utf-8") != rendered:
            raise ReleaseChainError(f"Console build evidence is stale: {output}")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.release-identity.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)


def write_release_evidence_manifest(
    *,
    workspace_root: Path,
    output_path: Path,
    contract: dict[str, Any],
    check: bool,
) -> None:
    """Create or check the artifact-side copy that may mature into evidence."""

    output = _artifact_side_output(
        workspace_root=workspace_root,
        output_path=output_path,
        label="Release evidence manifest",
    )
    declaration = release_manifest(contract)
    if check:
        if not output.is_file():
            raise ReleaseChainError(f"Release evidence manifest is missing: {output}")
        manifest = load_json(output)
        validate_declared_contract(
            contract,
            manifest,
            workspace_root=workspace_root,
        )
        validate_manifest_declaration_projection(manifest, declaration)
        return
    rendered = f"{json.dumps(declaration, indent=2, sort_keys=False)}\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.release-identity.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)


def _artifact_side_output(
    *,
    workspace_root: Path,
    output_path: Path,
    label: str,
) -> Path:
    artifact_root = (workspace_root / "artifacts").resolve()
    output = output_path.resolve() if output_path.is_absolute() else (workspace_root / output_path).resolve()
    try:
        output.relative_to(artifact_root)
    except ValueError as exc:
        raise ReleaseChainError(f"{label} must be artifact-side under {artifact_root}") from exc
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    default_root = Path(__file__).resolve().parents[2]
    parser.add_argument("--workspace-root", type=Path, default=default_root)
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "release-contract.json",
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--new-declaration",
        action="store_true",
        help="replace the current generated identities with a fresh null-evidence declaration",
    )
    parser.add_argument(
        "--console-build-output",
        type=Path,
        help="write/check post-commit Console build evidence under <workspace>/artifacts",
    )
    parser.add_argument(
        "--evidence-manifest-output",
        type=Path,
        help="write/check the evidence manifest copy under <workspace>/artifacts",
    )
    parser.add_argument("--source-commit")
    parser.add_argument("--built-at")
    args = parser.parse_args()
    contract = load_json(args.contract.resolve())
    try:
        if args.evidence_manifest_output is not None:
            if args.console_build_output is not None:
                raise ReleaseChainError("--evidence-manifest-output and --console-build-output are mutually exclusive")
            if args.check == args.new_declaration:
                raise ReleaseChainError("evidence manifest output requires exactly one of --check or --new-declaration")
            write_release_evidence_manifest(
                workspace_root=args.workspace_root.resolve(),
                output_path=args.evidence_manifest_output,
                contract=contract,
                check=args.check,
            )
            print(
                "Release evidence manifest is current"
                if args.check
                else "Release evidence manifest declaration generated"
            )
            return 0
        if args.console_build_output is not None:
            if args.new_declaration:
                raise ReleaseChainError("--console-build-output and --new-declaration are mutually exclusive")
            if not all(
                isinstance(value, str) and value
                for value in (
                    args.source_commit,
                    args.built_at,
                )
            ):
                raise ReleaseChainError("Console build evidence requires --source-commit and --built-at")
            write_console_build_identity(
                workspace_root=args.workspace_root.resolve(),
                output_path=args.console_build_output,
                payload=console_build_identity(
                    contract,
                    source_commit=args.source_commit,
                    built_at=args.built_at,
                ),
                check=args.check,
            )
            print("Console build evidence is current" if args.check else "Console build evidence generated")
            return 0
        generate(
            workspace_root=args.workspace_root.resolve(),
            contract=contract,
            check=args.check,
            new_declaration=args.new_declaration,
        )
    except ReleaseChainError as exc:
        parser.exit(1, f"release identity generation failed: {exc}\n")
    print("release identity is current" if args.check else "release identity generated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
