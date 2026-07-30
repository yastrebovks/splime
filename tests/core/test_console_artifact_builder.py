"""Focused contracts for the deterministic Console release artifact."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
import stat
import subprocess
import tarfile

import pytest

from tools.build_console_artifact import (
    ConsoleArtifactError,
    build_console_artifact,
    is_deployable_source_path,
)
from tools.release_chain import load_json


SPL_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = SPL_ROOT.parent
CONTRACT = load_json(SPL_ROOT / "release-contract.json")
SOURCE_DATE_EPOCH = 1_784_138_400
BUILT_AT = "2026-07-16T12:00:00+00:00"

_SOURCE_FILES = {
    "actions/activity.js": b"export const activity = true;\n",
    "api/client.js": b"export const client = true;\n",
    "app.js": b'import "./bootstrap/consoleApp.js";\n',
    "assets/logo.svg": b"<svg></svg>\n",
    "auth/firebase.js": b"export const auth = true;\n",
    "bootstrap/consoleApp.js": b"export const startConsoleApp = true;\n",
    "components/shell.js": b"export const shell = true;\n",
    "config.js": b'export const APP_RELEASE_ID = "splime-0.4.6";\n',
    "controllers/events.js": b"export const events = true;\n",
    "domain/truth.js": b"export const truth = true;\n",
    "downloads/spl-framework-cold-start.ipynb": b'{"cells":[]}\n',
    "icons.js": b"export const icon = true;\n",
    "index.html": b'<script type="module" src="./app.js"></script>\n',
    "mappers.js": b"export const mapper = true;\n",
    "releaseCoherence.js": b"export const coherent = true;\n",
    "releaseGate.js": b"export const releaseGate = true;\n",
    "routes/router.js": b"export const router = true;\n",
    "state/storage.js": b"export const storage = true;\n",
    "styles.css": b'@import "./styles/theme.css";\n',
    "styles/theme.css": b":root { color: black; }\n",
    "supportBundle.js": b"export const support = true;\n",
    "ui/tokens.js": b"export const token = true;\n",
    "utils.js": b"export const utility = true;\n",
    "vendor/elk.js": b"export const layout = true;\n",
    "vendor/elkjs-LICENSE.md": b"license\n",
}
_EXCLUDED_FILES = {
    "README.md": b"developer documentation\n",
    "artifacts/private-build.txt": b"private-build\n",
    "build.json": b'{"source_side":"must not ship"}\n',
    "node_modules/private-package/index.js": b"private package\n",
    "package.json": b'{"private":true}\n',
    "private/account-export.json": b'{"secret":"must not ship"}\n',
    "server.js": b"development server\n",
    "static-integrity.json": b'{"source_side":"must not ship"}\n',
    "tests/release.test.mjs": b"test source\n",
    "tools/generate.mjs": b"build tool\n",
}


def test_console_artifact_is_byte_reproducible_exact_and_source_immutable(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    frontend = workspace / "spl-frontend"
    commit = _create_frontend_repository(frontend)
    source_before = _source_snapshot(frontend)
    status_before = _git(frontend, "status", "--porcelain=v1", "--untracked-files=all")

    first = build_console_artifact(
        workspace_root=workspace,
        frontend_root=frontend,
        contract=CONTRACT,
        output_root=workspace / "artifacts" / "first",
        source_commit=commit,
        built_at=BUILT_AT,
        source_date_epoch=SOURCE_DATE_EPOCH,
    )
    second = build_console_artifact(
        workspace_root=workspace,
        frontend_root=frontend,
        contract=CONTRACT,
        output_root=workspace / "artifacts" / "second",
        source_commit=commit,
        built_at=BUILT_AT,
        source_date_epoch=SOURCE_DATE_EPOCH,
    )
    repeated = build_console_artifact(
        workspace_root=workspace,
        frontend_root=frontend,
        contract=CONTRACT,
        output_root=workspace / "artifacts" / "first",
        source_commit=commit,
        built_at=BUILT_AT,
        source_date_epoch=SOURCE_DATE_EPOCH,
    )

    assert first.archive_path.name == "splime-console-0.4.6.tar.gz"
    assert first.archive_path.read_bytes() == second.archive_path.read_bytes()
    assert repeated.archive_path.read_bytes() == first.archive_path.read_bytes()
    assert _tree_bytes(first.stage_directory) == _tree_bytes(second.stage_directory)

    expected_staged = set(_SOURCE_FILES) | {"build.json", "static-integrity.json"}
    assert set(_tree_bytes(first.stage_directory)) == expected_staged
    assert not any(
        path == prefix or path.startswith(f"{prefix}/")
        for path in expected_staged
        for prefix in ("tests", "tools", "node_modules", "artifacts", "private")
    )

    build = json.loads((first.stage_directory / "build.json").read_text(encoding="utf-8"))
    assert build["evidence_state"] == "built"
    assert build["source"]["commit"] == commit
    assert build["build"]["built_at"] == BUILT_AT
    assert "source_side" not in build
    assert not {
        "archive_sha256",
        "artifact_sha256",
        "bundle_sha256",
        "integrity_sha256",
        "sha256",
    }.intersection(_nested_keys(build))

    integrity = json.loads(first.integrity_path.read_text(encoding="utf-8"))
    expected_integrity_paths = {
        f"./{path}"
        for path in expected_staged
        if PurePosixPath(path).suffix in {".css", ".js", ".json"} and path != "static-integrity.json"
    }
    assert integrity == {
        "schema_version": 2,
        "release_id": "splime-0.4.6",
        "build": "./build.json",
        "assets": {
            path: _sha256(first.stage_directory / path.removeprefix("./")) for path in sorted(expected_integrity_paths)
        },
    }

    with tarfile.open(first.archive_path, mode="r:gz") as archive:
        members = archive.getmembers()
        archived_files = {member.name for member in members if member.isfile()}
        assert archived_files == {f"splime-console-0.4.6/{path}" for path in expected_staged}
        assert all(
            not member.name.startswith("/")
            and ".." not in PurePosixPath(member.name).parts
            and member.uid == 0
            and member.gid == 0
            and member.mtime == SOURCE_DATE_EPOCH
            and stat.S_IMODE(member.mode) == (0o755 if member.isdir() else 0o644)
            for member in members
        )

    assert _git(frontend, "status", "--porcelain=v1", "--untracked-files=all") == status_before == ""
    assert _git(frontend, "rev-parse", "HEAD") == commit
    assert _source_snapshot(frontend) == source_before


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("app.js", True),
        ("components/shell.js", True),
        ("assets/logo.svg", True),
        ("downloads/example.ipynb", True),
        ("vendor/LICENSE.md", True),
        ("build.json", False),
        ("static-integrity.json", False),
        ("tests/app.test.mjs", False),
        ("tools/build.mjs", False),
        ("node_modules/dependency.js", False),
        ("artifacts/private.json", False),
        ("private/account.json", False),
        ("assets/private.pem", False),
        ("../app.js", False),
        ("/app.js", False),
        ("assets/../../secret.svg", False),
        ("assets\\logo.svg", False),
        ("assets/.private.svg", False),
    ],
)
def test_deployable_path_allowlist_is_explicit_and_safe(path: str, expected: bool) -> None:
    assert is_deployable_source_path(path) is expected


def test_builder_rejects_dirty_mismatched_symlinked_and_outside_sources(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    frontend = workspace / "spl-frontend"
    commit = _create_frontend_repository(frontend)

    with pytest.raises(ConsoleArtifactError, match="under"):
        build_console_artifact(
            workspace_root=workspace,
            frontend_root=frontend,
            contract=CONTRACT,
            output_root=workspace / "outside",
            source_commit=commit,
            built_at=BUILT_AT,
            source_date_epoch=SOURCE_DATE_EPOCH,
        )
    with pytest.raises(ConsoleArtifactError, match="does not match"):
        build_console_artifact(
            workspace_root=workspace,
            frontend_root=frontend,
            contract=CONTRACT,
            output_root=workspace / "artifacts",
            source_commit="f" * 40,
            built_at=BUILT_AT,
            source_date_epoch=SOURCE_DATE_EPOCH,
        )

    (frontend / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(ConsoleArtifactError, match="must be clean"):
        build_console_artifact(
            workspace_root=workspace,
            frontend_root=frontend,
            contract=CONTRACT,
            output_root=workspace / "artifacts",
            source_commit=commit,
            built_at=BUILT_AT,
            source_date_epoch=SOURCE_DATE_EPOCH,
        )
    (frontend / "untracked.txt").unlink()

    outside = workspace / "outside.svg"
    outside.write_text("<svg></svg>\n", encoding="utf-8")
    logo = frontend / "assets" / "logo.svg"
    logo.unlink()
    logo.symlink_to(outside)
    _git(frontend, "add", "assets/logo.svg")
    _git(frontend, "commit", "-m", "plant deployable symlink")
    symlink_commit = _git(frontend, "rev-parse", "HEAD")
    with pytest.raises(ConsoleArtifactError, match="not a regular Git blob"):
        build_console_artifact(
            workspace_root=workspace,
            frontend_root=frontend,
            contract=CONTRACT,
            output_root=workspace / "artifacts",
            source_commit=symlink_commit,
            built_at=BUILT_AT,
            source_date_epoch=SOURCE_DATE_EPOCH,
        )


def _create_frontend_repository(repository: Path) -> str:
    repository.mkdir(parents=True)
    _git(repository, "init", "--quiet")
    _git(repository, "config", "user.name", "Console Artifact Test")
    _git(repository, "config", "user.email", "console-artifact@example.invalid")
    for relative_path, payload in {**_SOURCE_FILES, **_EXCLUDED_FILES}.items():
        destination = repository / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
    _git(repository, "add", "-f", ".")
    _git(repository, "commit", "--quiet", "-m", "Console source")
    return _git(repository, "rev-parse", "HEAD")


def _git(repository: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.strip()


def _source_snapshot(repository: Path) -> dict[str, tuple[str, int, int]]:
    result = {}
    for path in sorted(repository.rglob("*")):
        if ".git" in path.relative_to(repository).parts or not path.is_file():
            continue
        relative_path = path.relative_to(repository).as_posix()
        metadata = path.lstat()
        payload = path.read_bytes() if not path.is_symlink() else str(path.readlink()).encode()
        result[relative_path] = (
            hashlib.sha256(payload).hexdigest(),
            stat.S_IMODE(metadata.st_mode),
            metadata.st_mtime_ns,
        )
    return result


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def _nested_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value).union(*(map(_nested_keys, value.values())))
    if isinstance(value, list):
        return set().union(*(map(_nested_keys, value)))
    return set()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
