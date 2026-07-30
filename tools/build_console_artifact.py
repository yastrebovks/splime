"""Build the deterministic, artifact-side SPLime Console release payload.

The Console source repository contains declarations and development tooling.
This builder reads the reviewed deployable files from an exact clean Git
commit, replaces the source-side release declarations with post-commit build
evidence, and writes only below ``<workspace>/artifacts``.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import tarfile
import tempfile
from typing import Any

from tools.generate_release_identity import console_build_identity
from tools.release_chain import ReleaseChainError, load_json


_PUBLIC_JAVASCRIPT_DIRECTORIES = frozenset(
    {
        "actions",
        "api",
        "auth",
        "bootstrap",
        "components",
        "controllers",
        "domain",
        "routes",
        "state",
        "ui",
    }
)
_PUBLIC_DIRECTORY_SUFFIXES = {
    **{directory: frozenset({".js"}) for directory in _PUBLIC_JAVASCRIPT_DIRECTORIES},
    "assets": frozenset({".png", ".svg"}),
    "downloads": frozenset({".ipynb", ".ps1", ".sh", ".zip"}),
    "styles": frozenset({".css"}),
    "vendor": frozenset({".js", ".md"}),
}
_PUBLIC_FILES = frozenset(
    {
        "app.js",
        "config.js",
        "icons.js",
        "index.html",
        "mappers.js",
        "releaseCoherence.js",
        "releaseGate.js",
        "styles.css",
        "supportBundle.js",
        "utils.js",
    }
)
_GENERATED_FILES = frozenset({"build.json", "static-integrity.json"})
_INTEGRITY_SUFFIXES = frozenset({".css", ".js", ".json"})
_SAFE_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_FULL_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_MAX_GZIP_EPOCH = (1 << 32) - 1


class ConsoleArtifactError(RuntimeError):
    """Raised when Console artifact input or output is not release-safe."""


@dataclass(frozen=True)
class ConsoleArtifact:
    """Paths produced by one deterministic Console build."""

    stage_directory: Path
    archive_path: Path
    integrity_path: Path


@dataclass(frozen=True)
class _GitBlob:
    path: str
    object_id: str


def build_console_artifact(
    *,
    workspace_root: Path,
    frontend_root: Path,
    contract: dict[str, Any],
    output_root: Path,
    source_commit: str,
    built_at: str,
    source_date_epoch: int,
) -> ConsoleArtifact:
    """Build a Console staging tree and deterministic ``tar.gz`` archive.

    ``source_commit`` is supplied by the external source-evidence stage. The
    source repository must be clean and at that exact commit both before and
    after the build. Files are read from Git blobs rather than from the working
    tree, so ignored local files can never enter the artifact.
    """

    workspace = workspace_root.resolve()
    frontend = frontend_root.resolve()
    artifacts_root = (workspace / "artifacts").resolve()
    output = output_root.resolve()
    _require_below(output, artifacts_root, label="Console artifact output")
    _require_source_date_epoch(source_date_epoch)
    commit = _require_full_git_object_id(source_commit)
    _require_clean_source(frontend, expected_commit=commit)
    _validate_contract(contract)

    source_blobs = _deployable_blobs(frontend, commit)
    missing_files = sorted(_PUBLIC_FILES - set(source_blobs))
    if missing_files:
        raise ConsoleArtifactError(f"Console source is missing required deployable files: {missing_files!r}")
    missing_directories = sorted(
        directory
        for directory in _PUBLIC_DIRECTORY_SUFFIXES
        if not any(path.startswith(f"{directory}/") for path in source_blobs)
    )
    if missing_directories:
        raise ConsoleArtifactError(
            f"Console source is missing required deployable directories: {missing_directories!r}"
        )

    version = contract["version"]
    stage_directory = output / "console"
    archive_path = output / f"splime-console-{version}.tar.gz"
    integrity_path = stage_directory / "static-integrity.json"
    output.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=".console-artifact-", dir=output) as temporary:
        temporary_root = Path(temporary)
        candidate_stage = temporary_root / "console"
        candidate_stage.mkdir(mode=0o755)
        for relative_path, blob in sorted(source_blobs.items()):
            destination = candidate_stage / relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(_git_bytes(frontend, "cat-file", "blob", blob.object_id))
            destination.chmod(0o644)

        build_payload = console_build_identity(
            contract,
            source_commit=commit,
            built_at=built_at,
        )
        _assert_non_self_referential_build_identity(build_payload)
        _write_json(candidate_stage / "build.json", build_payload)
        integrity_payload = _static_integrity(candidate_stage, release_id=contract["release_id"])
        _write_json(candidate_stage / "static-integrity.json", integrity_payload)
        _canonicalize_stage_modes(candidate_stage)

        candidate_archive = temporary_root / archive_path.name
        _write_deterministic_archive(
            candidate_stage,
            candidate_archive,
            root_name=f"splime-console-{version}",
            source_date_epoch=source_date_epoch,
        )

        # A concurrent source edit invalidates the post-commit build. Detect it
        # before making any candidate visible under the durable artifact paths.
        _require_clean_source(frontend, expected_commit=commit)
        publish_stage = _directory_needs_publish(candidate_stage, stage_directory)
        publish_archive = _file_needs_publish(candidate_archive, archive_path)
        if publish_stage:
            candidate_stage.replace(stage_directory)
        if publish_archive:
            candidate_archive.replace(archive_path)

    return ConsoleArtifact(
        stage_directory=stage_directory,
        archive_path=archive_path,
        integrity_path=integrity_path,
    )


def is_deployable_source_path(path: str) -> bool:
    """Return whether a repository path belongs to the reviewed static allowlist."""

    if not _is_safe_relative_path(path) or path in _GENERATED_FILES:
        return False
    if path in _PUBLIC_FILES:
        return True
    pure_path = PurePosixPath(path)
    if len(pure_path.parts) < 2:
        return False
    allowed_suffixes = _PUBLIC_DIRECTORY_SUFFIXES.get(pure_path.parts[0])
    return allowed_suffixes is not None and pure_path.suffix.casefold() in allowed_suffixes


def _deployable_blobs(repository: Path, commit: str) -> dict[str, _GitBlob]:
    raw_tree = _git_bytes(repository, "ls-tree", "-rz", "--full-tree", commit)
    result: dict[str, _GitBlob] = {}
    for raw_record in raw_tree.split(b"\0"):
        if not raw_record:
            continue
        try:
            metadata, raw_path = raw_record.split(b"\t", 1)
            mode, object_type, raw_object_id = metadata.split(b" ", 2)
            path = raw_path.decode("utf-8", errors="strict")
            object_id = raw_object_id.decode("ascii", errors="strict")
        except (UnicodeDecodeError, ValueError) as exc:
            raise ConsoleArtifactError("Console Git tree contains an undecodable entry") from exc
        top_level = path.split("/", 1)[0]
        if top_level in _PUBLIC_DIRECTORY_SUFFIXES and not is_deployable_source_path(path):
            raise ConsoleArtifactError(f"public Console directory contains a non-allowlisted path: {path!r}")
        if not is_deployable_source_path(path):
            continue
        if mode not in {b"100644", b"100755"} or object_type != b"blob":
            raise ConsoleArtifactError(f"deployable Console path is not a regular Git blob: {path!r}")
        if not _FULL_GIT_OBJECT_ID.fullmatch(object_id):
            raise ConsoleArtifactError(f"deployable Console blob has an invalid object id: {path!r}")
        result[path] = _GitBlob(path=path, object_id=object_id)
    return result


def _static_integrity(stage: Path, *, release_id: str) -> dict[str, Any]:
    assets: dict[str, str] = {}
    for path in sorted(stage.rglob("*")):
        if not path.is_file() or path.name == "static-integrity.json":
            continue
        if path.suffix.casefold() not in _INTEGRITY_SUFFIXES:
            continue
        relative_path = path.relative_to(stage).as_posix()
        if not _is_safe_relative_path(relative_path):
            raise ConsoleArtifactError(f"staged Console asset has an unsafe path: {relative_path!r}")
        assets[f"./{relative_path}"] = _sha256(path)
    if "./build.json" not in assets:
        raise ConsoleArtifactError("Console integrity graph must include artifact-side build.json")
    return {
        "schema_version": 2,
        "release_id": release_id,
        "build": "./build.json",
        "assets": assets,
    }


def _write_deterministic_archive(
    source: Path,
    destination: Path,
    *,
    root_name: str,
    source_date_epoch: int,
) -> None:
    paths = sorted(source.rglob("*"), key=lambda path: path.relative_to(source).as_posix())
    directory_names = {root_name}
    file_paths: list[tuple[str, Path]] = []
    for path in paths:
        relative_path = path.relative_to(source).as_posix()
        if not _is_safe_relative_path(relative_path):
            raise ConsoleArtifactError(f"refusing unsafe Console archive path: {relative_path!r}")
        archive_name = f"{root_name}/{relative_path}"
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise ConsoleArtifactError(f"refusing non-regular Console archive member: {relative_path!r}")
        if path.is_dir():
            directory_names.add(archive_name)
        else:
            file_paths.append((archive_name, path))
            parent = PurePosixPath(archive_name).parent
            while parent.as_posix() != ".":
                directory_names.add(parent.as_posix())
                parent = parent.parent

    with destination.open("wb") as raw_archive:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_archive,
            compresslevel=9,
            mtime=source_date_epoch,
        ) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive:
                for directory_name in sorted(directory_names):
                    member = _tar_info(
                        f"{directory_name}/",
                        source_date_epoch=source_date_epoch,
                        mode=0o755,
                    )
                    member.type = tarfile.DIRTYPE
                    archive.addfile(member)
                for archive_name, path in sorted(file_paths):
                    payload = path.read_bytes()
                    member = _tar_info(
                        archive_name,
                        source_date_epoch=source_date_epoch,
                        mode=0o644,
                    )
                    member.size = len(payload)
                    archive.addfile(member, io.BytesIO(payload))


def _tar_info(name: str, *, source_date_epoch: int, mode: int) -> tarfile.TarInfo:
    member = tarfile.TarInfo(name)
    member.mode = mode
    member.uid = 0
    member.gid = 0
    member.uname = ""
    member.gname = ""
    member.mtime = source_date_epoch
    return member


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        f"{json.dumps(payload, indent=2, sort_keys=False)}\n",
        encoding="utf-8",
        newline="\n",
    )
    path.chmod(0o644)


def _canonicalize_stage_modes(stage: Path) -> None:
    stage.chmod(0o755)
    for path in stage.rglob("*"):
        if path.is_symlink():
            raise ConsoleArtifactError(f"staged Console tree contains a symlink: {path}")
        path.chmod(0o755 if path.is_dir() else 0o644)


def _directory_needs_publish(candidate: Path, destination: Path) -> bool:
    if destination.exists():
        if destination.is_symlink() or not destination.is_dir() or not _trees_are_identical(candidate, destination):
            raise ConsoleArtifactError(f"Console staging output already exists with different content: {destination}")
        return False
    return True


def _file_needs_publish(candidate: Path, destination: Path) -> bool:
    if destination.exists():
        if destination.is_symlink() or not destination.is_file() or candidate.read_bytes() != destination.read_bytes():
            raise ConsoleArtifactError(f"Console artifact already exists with different content: {destination}")
        return False
    return True


def _trees_are_identical(left: Path, right: Path) -> bool:
    def snapshot(root: Path) -> dict[str, tuple[int, bytes]] | None:
        result: dict[str, tuple[int, bytes]] = {}
        for path in root.rglob("*"):
            if path.is_symlink() or (not path.is_file() and not path.is_dir()):
                return None
            relative_path = path.relative_to(root).as_posix()
            mode = stat.S_IMODE(path.stat().st_mode)
            result[f"{relative_path}/" if path.is_dir() else relative_path] = (
                mode,
                b"" if path.is_dir() else path.read_bytes(),
            )
        return result

    return snapshot(left) == snapshot(right)


def _assert_non_self_referential_build_identity(payload: dict[str, Any]) -> None:
    forbidden_keys = {
        "archive_sha256",
        "artifact_sha256",
        "bundle_sha256",
        "integrity_sha256",
        "sha256",
    }

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            overlap = forbidden_keys.intersection(value)
            if overlap:
                raise ConsoleArtifactError(
                    f"Console build identity must not contain final artifact hashes: {sorted(overlap)!r}"
                )
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(payload)


def _validate_contract(contract: dict[str, Any]) -> None:
    try:
        version = contract["version"]
        release_id = contract["release_id"]
        console = contract["components"]["console"]
    except (KeyError, TypeError) as exc:
        raise ConsoleArtifactError("release contract has no Console identity") from exc
    if not isinstance(version, str) or not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ConsoleArtifactError("release contract version must be a three-part numeric version")
    if release_id != f"splime-{version}":
        raise ConsoleArtifactError("release contract release_id does not match its version")
    if not isinstance(console, dict) or console.get("version") != version:
        raise ConsoleArtifactError("release contract Console version does not match the release")


def _require_clean_source(repository: Path, *, expected_commit: str) -> None:
    if not repository.is_dir():
        raise ConsoleArtifactError(f"Console source repository does not exist: {repository}")
    top_level = _git_text(repository, "rev-parse", "--show-toplevel")
    if Path(top_level).resolve() != repository:
        raise ConsoleArtifactError(f"Console source must be the Git repository root: {repository}")
    observed_commit = _git_text(repository, "rev-parse", "--verify", "HEAD^{commit}")
    if observed_commit != expected_commit:
        raise ConsoleArtifactError(
            f"Console source HEAD {observed_commit} does not match external source commit {expected_commit}"
        )
    status_output = _git_text(repository, "status", "--porcelain=v1", "--untracked-files=all")
    if status_output:
        raise ConsoleArtifactError("Console source repository must be clean before artifact construction")


def _require_full_git_object_id(value: str) -> str:
    if not isinstance(value, str) or not _FULL_GIT_OBJECT_ID.fullmatch(value):
        raise ConsoleArtifactError("Console source commit must be a full lowercase Git object id")
    return value


def _require_source_date_epoch(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= _MAX_GZIP_EPOCH:
        raise ConsoleArtifactError(f"SOURCE_DATE_EPOCH must be an integer between 1 and {_MAX_GZIP_EPOCH}")


def _require_below(path: Path, root: Path, *, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ConsoleArtifactError(f"{label} must remain under {root}") from exc
    if path == root:
        return
    if any(parent.is_symlink() for parent in [path, *path.parents] if parent != root.parent):
        raise ConsoleArtifactError(f"{label} must not traverse symlinks")


def _is_safe_relative_path(path: str) -> bool:
    if not isinstance(path, str) or not path or "\\" in path or "\0" in path:
        return False
    pure_path = PurePosixPath(path)
    return (
        not pure_path.is_absolute()
        and pure_path.as_posix() == path
        and all(part not in {"", ".", ".."} and _SAFE_SEGMENT.fullmatch(part) is not None for part in pure_path.parts)
    )


def _git_text(repository: Path, *arguments: str) -> str:
    return _git_bytes(repository, *arguments).decode("utf-8", errors="strict").strip()


def _git_bytes(repository: Path, *arguments: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", os.fspath(repository), *arguments],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ConsoleArtifactError(f"Git command failed ({' '.join(arguments)}): {detail}")
    return completed.stdout


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    """Build the Console artifact from explicit post-commit evidence."""

    default_workspace = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace-root", type=Path, default=default_workspace)
    parser.add_argument("--frontend-root", type=Path)
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "release-contract.json",
    )
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--built-at", required=True)
    parser.add_argument("--source-date-epoch", type=int)
    args = parser.parse_args()

    workspace = args.workspace_root.resolve()
    source_date_epoch = args.source_date_epoch
    if source_date_epoch is None:
        raw_epoch = os.environ.get("SOURCE_DATE_EPOCH", "")
        try:
            source_date_epoch = int(raw_epoch)
        except ValueError:
            parser.error("SOURCE_DATE_EPOCH or --source-date-epoch must be a positive integer")
    frontend = (args.frontend_root or workspace / "spl-frontend").resolve()
    output = (args.output_root or workspace / "artifacts").resolve()
    try:
        result = build_console_artifact(
            workspace_root=workspace,
            frontend_root=frontend,
            contract=load_json(args.contract.resolve()),
            output_root=output,
            source_commit=args.source_commit,
            built_at=args.built_at,
            source_date_epoch=source_date_epoch,
        )
    except (ConsoleArtifactError, ReleaseChainError) as exc:
        parser.exit(1, f"Console artifact build failed: {exc}\n")
    print(f"Console staging tree: {result.stage_directory}")
    print(f"Console archive: {result.archive_path}")
    print(f"Console integrity: {result.integrity_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
