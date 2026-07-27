"""Regression tests for deterministic build and published-release controls."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tarfile
from typing import Any

import pytest

from tools import build_release_artifacts
from tools import verify_published_release


def _python_manifest(payload: bytes) -> dict[str, Any]:
    return {
        "version": "0.4.5",
        "python": {
            "artifacts": [
                {
                    "filename": "splime-0.4.5-py3-none-any.whl",
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            ]
        },
    }


def _console_fixture() -> tuple[dict[str, Any], bytes, dict[str, tuple[bytes, dict[str, str]]]]:
    console_url = "https://splime.test/app/"
    integrity_url = f"{console_url}static-integrity.json?release=splime-0.4.5"
    asset_url = f"{console_url}app.js"
    asset = b"console asset"
    integrity = json.dumps(
        {
            "release_id": "splime-0.4.5",
            "assets": {"./app.js": hashlib.sha256(asset).hexdigest()},
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    revalidate = {"cache-control": "no-cache, max-age=0, must-revalidate"}
    manifest = {
        "release_id": "splime-0.4.5",
        "console": {
            "url": console_url,
            "integrity_url": integrity_url,
            "integrity_sha256": hashlib.sha256(integrity).hexdigest(),
        },
    }
    responses = {
        console_url: (b"<html>splime-0.4.5</html>", {"cache-control": "no-store, max-age=0"}),
        integrity_url: (integrity, revalidate),
        asset_url: (asset, revalidate),
    }
    return manifest, integrity, responses


def _write_mode_variant_sdist(
    path: Path,
    *,
    directory_mode: int,
    file_mode: int,
    symlink_mode: int,
    hardlink_mode: int,
) -> None:
    with tarfile.open(path, "w:gz") as archive:
        directory = tarfile.TarInfo("splime-0.4.5")
        directory.type = tarfile.DIRTYPE
        directory.mode = directory_mode
        archive.addfile(directory)

        payload = b"release contents\n"
        regular_file = tarfile.TarInfo("splime-0.4.5/README.md")
        regular_file.mode = file_mode
        regular_file.size = len(payload)
        archive.addfile(regular_file, io.BytesIO(payload))

        symlink = tarfile.TarInfo("splime-0.4.5/README-link.md")
        symlink.type = tarfile.SYMTYPE
        symlink.linkname = "README.md"
        symlink.mode = symlink_mode
        archive.addfile(symlink)

        hardlink = tarfile.TarInfo("splime-0.4.5/README-hardlink.md")
        hardlink.type = tarfile.LNKTYPE
        hardlink.linkname = "splime-0.4.5/README.md"
        hardlink.mode = hardlink_mode
        archive.addfile(hardlink)


def test_normalize_sdist_canonicalizes_member_modes(tmp_path: Path) -> None:
    first_source = tmp_path / "first-source.tar.gz"
    second_source = tmp_path / "second-source.tar.gz"
    first_output = tmp_path / "first-output.tar.gz"
    second_output = tmp_path / "second-output.tar.gz"
    _write_mode_variant_sdist(
        first_source,
        directory_mode=0o700,
        file_mode=0o600,
        symlink_mode=0o700,
        hardlink_mode=0o600,
    )
    _write_mode_variant_sdist(
        second_source,
        directory_mode=0o775,
        file_mode=0o664,
        symlink_mode=0o777,
        hardlink_mode=0o664,
    )

    build_release_artifacts.normalize_sdist(
        first_source,
        first_output,
        source_date_epoch=1_784_073_600,
    )
    build_release_artifacts.normalize_sdist(
        second_source,
        second_output,
        source_date_epoch=1_784_073_600,
    )

    assert first_output.read_bytes() == second_output.read_bytes()
    with tarfile.open(first_output, "r:gz") as archive:
        members = {member.name: member for member in archive.getmembers()}
        modes = {name: member.mode for name, member in members.items()}
        payload = archive.extractfile(members["splime-0.4.5/README.md"])
        assert payload is not None
        assert payload.read() == b"release contents\n"
    assert modes == {
        "splime-0.4.5": 0o755,
        "splime-0.4.5/README.md": 0o644,
        "splime-0.4.5/README-link.md": 0o777,
        "splime-0.4.5/README-hardlink.md": 0o644,
    }
    assert modes["splime-0.4.5/README.md"] & 0o111 == 0
    assert members["splime-0.4.5"].type == tarfile.DIRTYPE
    assert members["splime-0.4.5/README.md"].type == tarfile.REGTYPE
    assert members["splime-0.4.5/README-link.md"].linkname == "README.md"
    assert members["splime-0.4.5/README-hardlink.md"].linkname == "splime-0.4.5/README.md"


@pytest.mark.parametrize(
    "member_type",
    [
        tarfile.CHRTYPE,
        tarfile.BLKTYPE,
        tarfile.FIFOTYPE,
        tarfile.CONTTYPE,
        tarfile.GNUTYPE_SPARSE,
    ],
    ids=["character-device", "block-device", "fifo", "contiguous", "gnu-sparse"],
)
def test_normalize_sdist_rejects_special_members(tmp_path: Path, member_type: bytes) -> None:
    source = tmp_path / "special-source.tar.gz"
    destination = tmp_path / "special-output.tar.gz"
    with tarfile.open(source, "w:gz") as archive:
        member = tarfile.TarInfo("splime-0.4.5/special")
        member.type = member_type
        member.mode = 0o777
        archive.addfile(member)

    with pytest.raises(ValueError, match="unsupported special member"):
        build_release_artifacts.normalize_sdist(
            source,
            destination,
            source_date_epoch=1_784_073_600,
        )


def test_build_main_passes_locked_epoch_to_backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI epoch must control both the backend build and sdist normalization."""

    locked_epoch = 1_784_073_600
    monkeypatch.delenv("SOURCE_DATE_EPOCH", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_release_artifacts.py",
            "--out-dir",
            str(tmp_path / "dist"),
            "--source-date-epoch",
            str(locked_epoch),
        ],
    )

    def fake_run(command: list[str], *, check: bool, env: dict[str, str]) -> None:
        assert check is True
        assert env["SOURCE_DATE_EPOCH"] == str(locked_epoch)
        assert env["PATH"] == os.environ["PATH"]
        raw_dir = Path(command[-1])
        (raw_dir / "splime-0.4.5-py3-none-any.whl").write_bytes(b"wheel")
        with tarfile.open(raw_dir / "splime-0.4.5.tar.gz", "w:gz") as archive:
            data = b"sdist"
            member = tarfile.TarInfo("splime-0.4.5/README.md")
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))

    monkeypatch.setattr(build_release_artifacts.subprocess, "run", fake_run)

    assert build_release_artifacts.main() == 0
    assert {path.name for path in (tmp_path / "dist").iterdir()} == {
        "splime-0.4.5-py3-none-any.whl",
        "splime-0.4.5.tar.gz",
    }


def test_verify_pypi_accepts_exact_declared_release(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"wheel bytes"
    manifest = _python_manifest(payload)
    filename = manifest["python"]["artifacts"][0]["filename"]
    digest = manifest["python"]["artifacts"][0]["sha256"]
    project_url = "https://files.test/release.whl"
    project = {
        "urls": [
            {
                "filename": filename,
                "digests": {"sha256": digest},
                "url": project_url,
            }
        ]
    }
    monkeypatch.setattr(
        verify_published_release,
        "fetch",
        lambda url: (json.dumps(project).encode(), {}) if url.endswith("/json") else (payload, {}),
    )

    verify_published_release.verify_pypi(manifest)


def test_verify_pypi_rejects_unexpected_filename(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"wheel bytes"
    manifest = _python_manifest(payload)
    artifact = manifest["python"]["artifacts"][0]
    project = {
        "urls": [
            {
                "filename": artifact["filename"],
                "digests": {"sha256": artifact["sha256"]},
                "url": "https://files.test/release.whl",
            },
            {
                "filename": "unexpected.tar.gz",
                "digests": {"sha256": hashlib.sha256(b"extra").hexdigest()},
                "url": "https://files.test/unexpected.tar.gz",
            },
        ]
    }
    monkeypatch.setattr(
        verify_published_release,
        "fetch",
        lambda url: (json.dumps(project).encode(), {}) if url.endswith("/json") else (payload, {}),
    )

    with pytest.raises(SystemExit, match="filename set"):
        verify_published_release.verify_pypi(manifest)


def test_verify_pypi_rejects_metadata_hash_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"wheel bytes"
    manifest = _python_manifest(payload)
    artifact = manifest["python"]["artifacts"][0]
    project = {
        "urls": [
            {
                "filename": artifact["filename"],
                "digests": {"sha256": hashlib.sha256(b"wrong").hexdigest()},
                "url": "https://files.test/release.whl",
            }
        ]
    }
    monkeypatch.setattr(
        verify_published_release,
        "fetch",
        lambda url: (json.dumps(project).encode(), {}) if url.endswith("/json") else (payload, {}),
    )

    with pytest.raises(SystemExit, match="metadata checksum mismatch"):
        verify_published_release.verify_pypi(manifest)


def test_verify_public_artifacts_accepts_exact_revalidated_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b'{"nbformat":4}'
    url = "https://splime.test/downloads/splime-cookbook.ipynb"
    manifest = {
        "public_artifacts": [
            {
                "url": url,
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        ]
    }
    monkeypatch.setattr(
        verify_published_release,
        "fetch",
        lambda requested_url: (
            (
                payload,
                {"cache-control": "no-cache, max-age=0, must-revalidate"},
            )
            if requested_url == url
            else pytest.fail(f"unexpected URL: {requested_url}")
        ),
    )

    verify_published_release.verify_public_artifacts(manifest)


def test_verify_public_artifacts_rejects_hash_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b'{"nbformat":4}'
    url = "https://splime.test/downloads/splime-cookbook.ipynb"
    manifest = {
        "public_artifacts": [
            {
                "url": url,
                "sha256": hashlib.sha256(payload + b"\n").hexdigest(),
            }
        ]
    }
    monkeypatch.setattr(
        verify_published_release,
        "fetch",
        lambda requested_url: (
            (
                payload,
                {"cache-control": "no-cache, max-age=0, must-revalidate"},
            )
            if requested_url == url
            else pytest.fail(f"unexpected URL: {requested_url}")
        ),
    )

    with pytest.raises(SystemExit, match="public artifact checksum mismatch"):
        verify_published_release.verify_public_artifacts(manifest)


def test_verify_public_artifacts_rejects_empty_manifest() -> None:
    with pytest.raises(SystemExit, match="non-empty public artifacts"):
        verify_published_release.verify_public_artifacts({"public_artifacts": []})


def test_verify_console_accepts_pinned_complete_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    manifest, _, responses = _console_fixture()
    monkeypatch.setattr(verify_published_release, "fetch", responses.__getitem__)

    verify_published_release.verify_console(manifest)


def test_verify_console_rejects_empty_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    manifest, _, responses = _console_fixture()
    empty_integrity = json.dumps(
        {"release_id": manifest["release_id"], "assets": {}},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    manifest["console"]["integrity_sha256"] = hashlib.sha256(empty_integrity).hexdigest()
    responses[manifest["console"]["integrity_url"]] = (
        empty_integrity,
        {"cache-control": "no-cache, max-age=0, must-revalidate"},
    )
    monkeypatch.setattr(verify_published_release, "fetch", responses.__getitem__)

    with pytest.raises(SystemExit, match="non-empty assets"):
        verify_published_release.verify_console(manifest)


def test_verify_console_rejects_unpinned_integrity_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    manifest, integrity, responses = _console_fixture()
    manifest["console"]["integrity_sha256"] = hashlib.sha256(integrity + b"\n").hexdigest()
    monkeypatch.setattr(verify_published_release, "fetch", responses.__getitem__)

    with pytest.raises(SystemExit, match="integrity manifest checksum mismatch"):
        verify_published_release.verify_console(manifest)


def test_verify_console_requires_complete_revalidation_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    manifest, _, responses = _console_fixture()
    integrity_url = manifest["console"]["integrity_url"]
    integrity, _ = responses[integrity_url]
    responses[integrity_url] = (integrity, {"cache-control": "no-cache"})
    monkeypatch.setattr(verify_published_release, "fetch", responses.__getitem__)

    with pytest.raises(SystemExit, match="max-age=0"):
        verify_published_release.verify_console(manifest)
