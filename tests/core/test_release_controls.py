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


SPL_ROOT = Path(__file__).resolve().parents[2]
DOCKER_ROOT = SPL_ROOT / "deploy" / "dockerhub"


def _python_manifest(payload: bytes) -> dict[str, Any]:
    filename = "splime-0.4.5-py3-none-any.whl"
    return {
        "schema_version": 2,
        "version": "0.4.5",
        "python": {
            "artifacts": [
                {
                    "filename": filename,
                    "url": f"https://files.test/{filename}",
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            ]
        },
    }


def test_canonical_docker_context_is_exact_bounded_and_private_free() -> None:
    expected = {
        ".dockerignore",
        "Dockerfile",
        "README.md",
        "docker-compose.yml",
        "publish.sh",
    }
    observed = {path.name for path in DOCKER_ROOT.iterdir()}

    assert observed == expected
    assert all((DOCKER_ROOT / name).is_file() for name in expected)
    assert all(not (DOCKER_ROOT / name).is_symlink() for name in expected)
    assert (DOCKER_ROOT / ".dockerignore").read_text(encoding="utf-8") == (
        "# This image installs splime from PyPI and copies nothing from the build\n"
        "# context, so ignore everything to keep the context empty and builds fast.\n"
        "*\n"
    )
    dockerfile = (DOCKER_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY ." not in dockerfile
    assert dockerfile.count("COPY ") == 1
    assert "COPY --from=docker:27-cli" in dockerfile


def test_canonical_docker_source_pins_the_exact_release_and_oci_identity() -> None:
    manifest = json.loads((SPL_ROOT / "release-manifest.json").read_text(encoding="utf-8"))
    version = manifest["version"]
    dockerfile = (DOCKER_ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = (DOCKER_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    publish = DOCKER_ROOT / "publish.sh"
    publish_text = publish.read_text(encoding="utf-8")
    combined = "\n".join((dockerfile, compose, publish_text, (DOCKER_ROOT / "README.md").read_text(encoding="utf-8")))

    assert version == "0.4.6"
    assert f"ARG SPL_VERSION={version}" in dockerfile
    assert 'python -m pip install "splime==${SPL_VERSION}"' in dockerfile
    assert f"image: {manifest['docker']['repository']}:{version}" in compose
    assert f'VERSION="${{1:-{version}}}"' in publish_text
    assert "0.4.5" not in combined
    assert os.access(publish, os.X_OK)
    for label in (
        "org.opencontainers.image.title",
        "org.opencontainers.image.description",
        "org.opencontainers.image.version",
        "org.opencontainers.image.source",
        "org.opencontainers.image.url",
        "org.opencontainers.image.licenses",
    ):
        assert label in dockerfile
    assert 'org.opencontainers.image.version="${SPL_VERSION}"' in dockerfile


def test_canonical_docker_runtime_is_non_root_loopback_first_and_socket_opt_in() -> None:
    dockerfile = (DOCKER_ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = (DOCKER_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    readme = (DOCKER_ROOT / "README.md").read_text(encoding="utf-8")

    assert "ARG SPL_UID=10001" in dockerfile
    assert "ARG SPL_GID=10001" in dockerfile
    assert "USER spl:spl" in dockerfile
    assert "127.0.0.1:8765:8765" in compose
    assert "127.0.0.1:8765:8765" in readme
    assert "docker.sock" not in compose
    assert "/var/run/docker.sock" in readme
    assert "socket is **not** mounted" in readme.casefold()
    assert "cap_drop:" in compose and "- ALL" in compose
    assert "no-new-privileges:true" in compose


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
    project_url = manifest["python"]["artifacts"][0]["url"]
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


def test_verify_pypi_rejects_metadata_url_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"wheel bytes"
    manifest = _python_manifest(payload)
    artifact = manifest["python"]["artifacts"][0]
    project = {
        "urls": [
            {
                "filename": artifact["filename"],
                "digests": {"sha256": artifact["sha256"]},
                "url": f"https://mirror.test/{artifact['filename']}",
            }
        ]
    }
    monkeypatch.setattr(
        verify_published_release,
        "fetch",
        lambda url: (json.dumps(project).encode(), {}) if url.endswith("/json") else (payload, {}),
    )

    with pytest.raises(SystemExit, match="metadata URL mismatch"):
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


def test_verify_github_release_assets_requires_exact_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asset = b"reviewed release asset"
    release_url = "https://github.test/releases/tag/v0.4.6"
    asset_url = "https://github.test/releases/download/v0.4.6/asset.bin"
    manifest = {
        "github_release": {
            "url": release_url,
            "assets": [
                {
                    "name": "asset.bin",
                    "url": asset_url,
                    "sha256": hashlib.sha256(asset).hexdigest(),
                }
            ],
        }
    }
    responses = {
        release_url: (b"release page", {}),
        asset_url: (asset, {}),
    }
    monkeypatch.setattr(verify_published_release, "fetch", responses.__getitem__)

    verify_published_release.verify_github_release_assets(manifest)

    manifest["github_release"]["assets"][0]["url"] = "https://github.test/releases/download/v0.4.6/other.bin"
    with pytest.raises(SystemExit, match="URL does not end"):
        verify_published_release.verify_github_release_assets(manifest)
    manifest["github_release"]["assets"][0]["url"] = asset_url

    responses[asset_url] = (asset + b" drift", {})
    with pytest.raises(SystemExit, match="checksum mismatch"):
        verify_published_release.verify_github_release_assets(manifest)


def test_verify_docker_requires_manifest_and_platform_digests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verification_url = "https://hub.test/tags/0.4.6"
    publication_url = "https://hub.test/repository/tags?name=0.4.6"
    manifest = {
        "docker": {
            "tag": "0.4.6",
            "manifest_digest": f"sha256:{'a' * 64}",
            "platform_digests": {
                "linux/amd64": f"sha256:{'b' * 64}",
                "linux/arm64": f"sha256:{'c' * 64}",
            },
            "verification_url": verification_url,
            "publication_url": publication_url,
        }
    }
    tag = {
        "name": "0.4.6",
        "digest": f"sha256:{'a' * 64}",
        "images": [
            {
                "os": "linux",
                "architecture": "amd64",
                "digest": f"sha256:{'b' * 64}",
            },
            {
                "os": "linux",
                "architecture": "arm64",
                "digest": f"sha256:{'c' * 64}",
            },
        ],
    }

    def fake_fetch(url: str) -> tuple[bytes, dict[str, str]]:
        if url == verification_url:
            return json.dumps(tag).encode(), {}
        if url == publication_url:
            return b"tags", {}
        return pytest.fail(f"unexpected URL: {url}")

    monkeypatch.setattr(verify_published_release, "fetch", fake_fetch)
    verify_published_release.verify_docker(manifest)

    tag["images"][1]["digest"] = f"sha256:{'d' * 64}"
    with pytest.raises(SystemExit, match="platform digest mismatch"):
        verify_published_release.verify_docker(manifest)


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


def test_v1_release_manifest_remains_a_supported_historical_reader() -> None:
    verify_published_release.require_publishable_manifest({"schema_version": 1, "release_id": "splime-0.4.5"})


def test_declared_v2_manifest_cannot_be_published() -> None:
    with pytest.raises(SystemExit, match="not publishable"):
        verify_published_release.require_publishable_manifest(
            {
                "schema_version": 2,
                "release_id": "splime-0.4.6",
                "evidence": {"state": "declared"},
            }
        )


def test_v2_publishable_state_still_requires_exact_component_evidence() -> None:
    with pytest.raises(SystemExit, match="exactly framework"):
        verify_published_release.require_publishable_manifest(
            {
                "schema_version": 2,
                "release_id": "splime-0.4.6",
                "evidence": {"state": "published"},
                "components": {
                    "server": {
                        "source_commit": None,
                        "artifact": {"sha256": None},
                    }
                },
                "python": {"artifacts": []},
                "console": {"integrity_sha256": None},
            }
        )


def test_v2_publishable_guard_rejects_empty_artifact_sets() -> None:
    component = {
        "source_binding": "pinned_commit",
        "source_ref": "v0.4.6",
        "source_commit": "1" * 40,
        "artifact": {"sha256": "2" * 64},
    }
    with pytest.raises(SystemExit, match="non-empty Python artifacts"):
        verify_published_release.require_publishable_manifest(
            {
                "schema_version": 2,
                "release_id": "splime-0.4.6",
                "version": "0.4.6",
                "evidence": {"state": "published"},
                "components": {
                    "framework": {
                        **component,
                        "source_binding": "signed_tag_external_provenance",
                    },
                    "daemon": {
                        **component,
                        "source_binding": "signed_tag_external_provenance",
                    },
                    "server": component,
                    "console": component,
                },
                "python": {"artifacts": []},
                "public_artifacts": [{"url": "x", "sha256": "3" * 64}],
                "console": {"integrity_sha256": "4" * 64},
            }
        )


def test_v2_publishable_guard_requires_github_and_immutable_docker_evidence() -> None:
    component = {
        "source_binding": "pinned_commit",
        "source_ref": "v0.4.6",
        "source_commit": "1" * 40,
        "artifact": {"sha256": "2" * 64},
    }
    manifest = {
        "schema_version": 2,
        "release_id": "splime-0.4.6",
        "version": "0.4.6",
        "evidence": {"state": "published"},
        "components": {
            "framework": {
                **component,
                "source_binding": "signed_tag_external_provenance",
            },
            "daemon": {
                **component,
                "source_binding": "signed_tag_external_provenance",
            },
            "server": component,
            "console": component,
        },
        "python": {
            "artifacts": [
                {
                    "filename": "splime.whl",
                    "url": "https://files.test/splime.whl",
                    "sha256": "3" * 64,
                }
            ]
        },
        "public_artifacts": [{"url": "https://public.test/a", "sha256": "4" * 64}],
        "console": {"integrity_sha256": "5" * 64},
        "github_release": {
            "assets": [{"name": "asset.bin", "sha256": "6" * 64}],
        },
        "docker": {
            "manifest_digest": f"sha256:{'7' * 64}",
            "platform_digests": {
                "linux/amd64": f"sha256:{'8' * 64}",
                "linux/arm64": f"sha256:{'9' * 64}",
            },
        },
    }

    verify_published_release.require_publishable_manifest(manifest)

    manifest["docker"]["manifest_digest"] = None
    with pytest.raises(SystemExit, match="immutable Docker manifest digest"):
        verify_published_release.require_publishable_manifest(manifest)


def test_signed_source_tag_must_identify_the_checked_out_commit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    observations = iter(["", "1" * 40, "2" * 40])
    monkeypatch.setattr(
        verify_published_release,
        "_run_git",
        lambda _repository, *_arguments: next(observations),
    )

    with pytest.raises(SystemExit, match="does not identify"):
        verify_published_release.verify_signed_source_tag(
            {"version": "0.4.6"},
            repository=tmp_path,
        )


def test_signed_source_tag_must_match_both_external_bom_components(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    commit = "1" * 40
    manifest = {
        "version": "0.4.6",
        "components": {
            name: {
                "source_binding": "signed_tag_external_provenance",
                "source_ref": "v0.4.6",
                "source_commit": commit,
            }
            for name in ("framework", "daemon")
        },
    }
    observations = iter(["", commit, commit])
    monkeypatch.setattr(
        verify_published_release,
        "_run_git",
        lambda _repository, *_arguments: next(observations),
    )

    verify_published_release.verify_signed_source_tag(
        manifest,
        repository=tmp_path,
    )

    manifest["components"]["daemon"]["source_commit"] = "2" * 40
    observations = iter(["", commit, commit])
    with pytest.raises(SystemExit, match="external BOM component daemon"):
        verify_published_release.verify_signed_source_tag(
            manifest,
            repository=tmp_path,
        )


def test_published_manifest_remote_bytes_must_match_reviewed_bom_exactly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "release_id": "splime-0.4.6",
                "version": "0.4.6",
                "manifest_url": "https://release.test/release-manifest.json",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        verify_published_release,
        "require_publishable_manifest",
        lambda _manifest: None,
    )
    monkeypatch.setattr(
        verify_published_release,
        "verify_signed_source_tag",
        lambda _manifest, *, repository: None,
    )
    monkeypatch.setattr(
        verify_published_release,
        "fetch",
        lambda _url: (
            json.dumps(json.loads(manifest_path.read_text(encoding="utf-8"))).encode(),
            {},
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "verify_published_release.py",
            "--manifest",
            str(manifest_path),
        ],
    )

    with pytest.raises(SystemExit, match="bytes do not match"):
        verify_published_release.main()


def test_console_build_identity_rejects_non_allowlisted_provenance() -> None:
    manifest = {
        "release_id": "splime-0.4.6",
        "packages": {"console": "0.4.6"},
        "components": {
            "console": {
                "repository": "https://example.invalid/console",
                "source_ref": "v0.4.6",
                "source_commit": "1" * 40,
                "contracts": {"console_server": "console-server/v1"},
            }
        },
    }
    build = {
        "schema_version": 1,
        "component": "console",
        "release_id": "splime-0.4.6",
        "version": "0.4.6",
        "evidence_state": "built",
        "source": {
            "repository": "https://example.invalid/console",
            "ref": "v0.4.6",
            "binding": "pinned_commit",
            "commit": "1" * 40,
        },
        "build": {"built_at": "2026-07-30T12:00:00+00:00"},
        "contracts": {"console_server": "console-server/v1"},
    }
    verify_published_release.require_console_build_identity(build, manifest)

    build["builder"] = {
        "hostname": "private-host",
        "credential": "must-not-cross-the-boundary",
    }
    with pytest.raises(SystemExit, match="does not match"):
        verify_published_release.require_console_build_identity(build, manifest)


def test_server_deployment_verification_separates_receipt_from_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = {
        "schema_version": 2,
        "release_id": "splime-0.4.6",
        "packages": {"server": "0.4.6"},
        "server": {"schema_target": 32},
        "components": {
            "server": {
                "source_ref": "v0.4.6",
                "source_commit": "a" * 40,
                "artifact": {"sha256": "b" * 64},
            }
        },
    }
    manifest_bytes = json.dumps(manifest).encode()
    receipt = {
        "schema_version": 1,
        "release_id": manifest["release_id"],
        "component": "server",
        "version": "0.4.6",
        "source_ref": "v0.4.6",
        "source_commit": "a" * 40,
        "artifact_sha256": "b" * 64,
        "release_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "schema_target": 32,
        "deployed_at": "2026-07-29T20:00:00+00:00",
        "environment_class": "staging",
    }
    responses: dict[str, dict[str, Any]] = {
        "https://server.test/version": {
            "contract": "version_authority/v1",
            "schema_version": 1,
            "component": "server",
            "deployment": {
                "state": "present",
                "reason_code": "deployment_receipt_present",
                **receipt,
            },
            "database_schema": {"current": 32, "target": 32},
        },
        "https://server.test/ready": {"ready": True, "checks": {}},
    }

    def fake_fetch(url: str) -> tuple[bytes, dict[str, str]]:
        return (
            json.dumps(responses[url]).encode(),
            {"cache-control": "no-cache, max-age=0, must-revalidate"},
        )

    monkeypatch.setattr(verify_published_release, "fetch", fake_fetch)
    verify_published_release.verify_server_deployment(
        manifest,
        manifest_bytes=manifest_bytes,
        version_url="https://server.test/version",
        ready_url="https://server.test/ready",
    )

    responses["https://server.test/ready"]["ready"] = False
    with pytest.raises(SystemExit, match="not operationally ready"):
        verify_published_release.verify_server_deployment(
            manifest,
            manifest_bytes=manifest_bytes,
            version_url="https://server.test/version",
            ready_url="https://server.test/ready",
        )
