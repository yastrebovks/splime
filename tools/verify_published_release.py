"""Fail closed when a published SPLime release URL or checksum drifts."""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

USER_AGENT = "splime-release-verifier/0.4.6"


def main() -> int:
    """Verify PyPI, Docker Hub, and the deployed Console against one manifest."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=Path("release-manifest.json"))
    parser.add_argument(
        "--source-repository",
        type=Path,
        default=Path("."),
        help="clean framework repository containing the signed release tag",
    )
    parser.add_argument("--manifest-only", action="store_true")
    parser.add_argument(
        "--server-version-url",
        help="deployed server /version URL; required for a v2 published release",
    )
    parser.add_argument(
        "--server-ready-url",
        help="deployed server /ready URL; required for a v2 published release",
    )
    args = parser.parse_args()
    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    require_publishable_manifest(manifest)
    if manifest.get("schema_version") == 2:
        verify_signed_source_tag(
            manifest,
            repository=args.source_repository.resolve(),
        )
    if args.manifest_only:
        print(f"verified publishable manifest {manifest['release_id']}")
        return 0

    remote_manifest_bytes, _ = fetch(manifest["manifest_url"])
    if remote_manifest_bytes != manifest_bytes:
        raise SystemExit("published release manifest bytes do not match the reviewed external BOM")

    for url in manifest["install_page_urls"]:
        fetch(url)

    verify_pypi(manifest)
    verify_public_artifacts(manifest)
    verify_github_release_assets(manifest)
    verify_docker(manifest)
    verify_console(manifest)
    if manifest.get("schema_version") == 2:
        if not args.server_version_url or not args.server_ready_url:
            raise SystemExit("v2 publication verification requires --server-version-url and --server-ready-url")
        verify_server_deployment(
            manifest,
            manifest_bytes=manifest_bytes,
            version_url=args.server_version_url,
            ready_url=args.server_ready_url,
        )
    print(f"verified published release {manifest['release_id']}")
    return 0


def verify_pypi(manifest: dict[str, Any]) -> None:
    """Verify PyPI metadata and every declared distribution byte-for-byte."""

    version = manifest["version"]
    body, _ = fetch(f"https://pypi.org/pypi/splime/{version}/json")
    project = json.loads(body)
    artifacts = manifest["python"]["artifacts"]
    expected_filenames = [artifact["filename"] for artifact in artifacts]
    published_items = project["urls"]
    published_filenames = [item["filename"] for item in published_items]
    if len(set(expected_filenames)) != len(expected_filenames):
        raise SystemExit("release manifest contains duplicate Python artifact filenames")
    if len(set(published_filenames)) != len(published_filenames):
        raise SystemExit("PyPI metadata contains duplicate release filenames")
    if set(published_filenames) != set(expected_filenames):
        raise SystemExit(
            "PyPI filename set mismatch: "
            f"expected={sorted(expected_filenames)!r} published={sorted(published_filenames)!r}"
        )
    published = {item["filename"]: item for item in published_items}
    for artifact in artifacts:
        filename = artifact["filename"]
        item = published[filename]
        if item["digests"]["sha256"] != artifact["sha256"]:
            raise SystemExit(f"PyPI metadata checksum mismatch for {filename}")
        declared_url = artifact.get("url")
        if manifest.get("schema_version") == 2:
            if not _is_credential_free_https_url(declared_url):
                raise SystemExit(f"PyPI artifact {filename} has no credential-free HTTPS URL")
            if Path(urllib.parse.urlparse(declared_url).path).name != filename:
                raise SystemExit(f"PyPI artifact URL does not end in {filename}")
            if item.get("url") != declared_url:
                raise SystemExit(f"PyPI metadata URL mismatch for {filename}")
        download_url = declared_url if isinstance(declared_url, str) else item["url"]
        data, _ = fetch(download_url)
        if hashlib.sha256(data).hexdigest() != artifact["sha256"]:
            raise SystemExit(f"downloaded PyPI checksum mismatch for {filename}")


def verify_public_artifacts(manifest: dict[str, Any]) -> None:
    """Verify exact public documentation/download bytes and cache policy."""

    artifacts = manifest["public_artifacts"]
    if not isinstance(artifacts, list) or not artifacts:
        raise SystemExit("release manifest must declare non-empty public artifacts")
    for artifact in artifacts:
        url = artifact["url"]
        data, headers = fetch(url)
        if hashlib.sha256(data).hexdigest() != artifact["sha256"]:
            raise SystemExit(f"deployed public artifact checksum mismatch: {url}")
        require_revalidation(headers, label=f"public artifact {url}")


def verify_docker(manifest: dict[str, Any]) -> None:
    """Verify the immutable multi-arch and platform digests behind the tag."""

    body, _ = fetch(manifest["docker"]["verification_url"])
    tag = json.loads(body)
    if tag.get("name") != manifest["docker"]["tag"]:
        raise SystemExit("Docker Hub returned the wrong release tag")
    if tag.get("digest") != manifest["docker"]["manifest_digest"]:
        raise SystemExit("Docker Hub manifest digest does not match the release manifest")
    expected = manifest["docker"]["platform_digests"]
    observed: dict[str, str] = {}
    images = tag.get("images")
    if not isinstance(images, list):
        raise SystemExit("Docker Hub tag response has no platform images")
    for image in images:
        if not isinstance(image, dict):
            continue
        platform = f"{image.get('os')}/{image.get('architecture')}"
        if platform not in expected:
            continue
        if platform in observed:
            raise SystemExit(f"Docker Hub returned duplicate platform evidence for {platform}")
        digest = image.get("digest")
        if not isinstance(digest, str):
            raise SystemExit(f"Docker Hub returned an invalid platform digest for {platform}")
        observed[platform] = digest
    if observed != expected:
        raise SystemExit(f"Docker Hub platform digest mismatch: expected={expected!r} observed={observed!r}")
    fetch(manifest["docker"]["publication_url"])


def verify_github_release_assets(manifest: dict[str, Any]) -> None:
    """Verify every declared non-self-referential GitHub release asset."""

    release = manifest.get("github_release")
    if not isinstance(release, dict):
        raise SystemExit("release manifest has no GitHub release evidence")
    fetch(release["url"])
    assets = release.get("assets")
    if not isinstance(assets, list) or not assets:
        raise SystemExit("release manifest must declare non-empty GitHub release assets")
    names: set[str] = set()
    urls: set[str] = set()
    for asset in assets:
        name = asset.get("name")
        url = asset.get("url")
        digest = asset.get("sha256")
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(url, str)
            or not url.startswith("https://")
            or not _is_sha256(digest)
        ):
            raise SystemExit("GitHub release asset declaration is incomplete")
        if Path(urllib.parse.urlparse(url).path).name != name:
            raise SystemExit(f"GitHub release asset URL does not end in {name}")
        if name in names or url in urls:
            raise SystemExit("GitHub release asset names and URLs must be unique")
        names.add(name)
        urls.add(url)
        payload, _ = fetch(url)
        if hashlib.sha256(payload).hexdigest() != digest:
            raise SystemExit(f"GitHub release asset checksum mismatch: {name}")


def verify_console(manifest: dict[str, Any]) -> None:
    """Verify the deployed shell, cache policy, and complete module graph hashes."""

    console_url = manifest["console"]["url"]
    shell, shell_headers = fetch(console_url)
    if manifest["release_id"].encode() not in shell:
        raise SystemExit("deployed Console shell does not identify the manifest release")
    require_cache_directives(shell_headers, label="deployed Console shell", required={"no-store": None})

    integrity_bytes, integrity_headers = fetch(manifest["console"]["integrity_url"])
    require_revalidation(integrity_headers, label="deployed integrity manifest")
    if hashlib.sha256(integrity_bytes).hexdigest() != manifest["console"]["integrity_sha256"]:
        raise SystemExit("deployed Console integrity manifest checksum mismatch")
    integrity = json.loads(integrity_bytes)
    if integrity.get("release_id") != manifest["release_id"]:
        raise SystemExit("deployed Console integrity manifest has the wrong release id")
    assets = integrity.get("assets")
    if not isinstance(assets, dict) or not assets:
        raise SystemExit("deployed Console integrity manifest must contain non-empty assets")
    if manifest.get("schema_version") == 2:
        if integrity.get("schema_version") != 2:
            raise SystemExit("v2 release requires a v2 Console integrity manifest")
        build_url = integrity.get("build")
        if build_url != "./build.json" or build_url not in assets:
            raise SystemExit("v2 Console integrity must cover ./build.json")
    for relative_url, expected_sha256 in assets.items():
        if not isinstance(relative_url, str) or not relative_url.startswith("./"):
            raise SystemExit("deployed Console integrity manifest contains an invalid asset URL")
        if not _is_sha256(expected_sha256):
            raise SystemExit(f"deployed Console integrity manifest contains an invalid checksum: {relative_url!r}")
        url = urllib.parse.urljoin(console_url, relative_url.removeprefix("./"))
        data, headers = fetch(url)
        require_revalidation(headers, label=f"unhashed Console asset {url}")
        if hashlib.sha256(data).hexdigest() != expected_sha256:
            raise SystemExit(f"deployed Console checksum mismatch: {url}")
        if relative_url == "./build.json":
            build = json.loads(data)
            require_console_build_identity(build, manifest)


def verify_server_deployment(
    manifest: dict[str, Any],
    *,
    manifest_bytes: bytes,
    version_url: str,
    ready_url: str,
) -> None:
    """Verify deployment receipt evidence and operational readiness separately."""

    version_bytes, version_headers = fetch(version_url)
    require_revalidation(version_headers, label="deployed server version")
    try:
        version = json.loads(version_bytes)
    except json.JSONDecodeError as exc:
        raise SystemExit("deployed server /version is not valid JSON") from exc
    if (
        not isinstance(version, dict)
        or version.get("contract") != "version_authority/v1"
        or version.get("schema_version") != 1
        or version.get("component") != "server"
    ):
        raise SystemExit("deployed server /version contract is invalid")
    deployment = version.get("deployment")
    if not isinstance(deployment, dict) or deployment.get("state") != "present":
        raise SystemExit("deployed server has no authoritative deployment receipt")
    receipt = {key: value for key, value in deployment.items() if key not in {"state", "reason_code"}}
    required_receipt_keys = {
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
    if set(receipt) != required_receipt_keys:
        raise SystemExit("deployed server receipt fields are not the exact allowlist")

    expected = manifest["components"]["server"]
    expected_artifact = expected.get("artifact")
    if (
        receipt.get("schema_version") != 1
        or receipt.get("component") != "server"
        or receipt.get("release_id") != manifest.get("release_id")
        or receipt.get("version") != manifest.get("packages", {}).get("server")
        or receipt.get("source_ref") != expected.get("source_ref")
        or receipt.get("source_commit") != expected.get("source_commit")
        or not isinstance(expected_artifact, dict)
        or receipt.get("artifact_sha256") != expected_artifact.get("sha256")
        or receipt.get("release_manifest_sha256") != hashlib.sha256(manifest_bytes).hexdigest()
        or receipt.get("schema_target") != manifest.get("server", {}).get("schema_target")
        or receipt.get("environment_class") not in {"staging", "production"}
        or not _is_git_commit(receipt.get("source_commit"))
        or not _is_sha256(receipt.get("artifact_sha256"))
        or not _is_sha256(receipt.get("release_manifest_sha256"))
        or not _is_aware_iso(receipt.get("deployed_at"))
    ):
        raise SystemExit("deployed server receipt does not match the release manifest")

    schema = version.get("database_schema")
    expected_schema = manifest.get("server", {}).get("schema_target")
    if (
        not isinstance(schema, dict)
        or schema.get("current") != expected_schema
        or schema.get("target") != expected_schema
    ):
        raise SystemExit("deployed server schema does not match the release manifest")

    ready_bytes, ready_headers = fetch(ready_url)
    require_revalidation(ready_headers, label="deployed server readiness")
    try:
        ready = json.loads(ready_bytes)
    except json.JSONDecodeError as exc:
        raise SystemExit("deployed server /ready is not valid JSON") from exc
    if not isinstance(ready, dict) or ready.get("ready") is not True:
        raise SystemExit("deployed server is not operationally ready")


def require_publishable_manifest(manifest: dict[str, Any]) -> None:
    """Accept historical v1 manifests and fail closed on incomplete v2 evidence."""

    schema_version = manifest.get("schema_version")
    if schema_version == 1:
        return
    if schema_version != 2:
        raise SystemExit(f"unsupported release manifest schema {schema_version!r}")
    state = manifest.get("evidence", {}).get("state")
    if state != "published":
        raise SystemExit("release manifest is not publishable; evidence.state must be published")
    components = manifest.get("components")
    if not isinstance(components, dict) or set(components) != {
        "framework",
        "daemon",
        "server",
        "console",
    }:
        raise SystemExit("v2 release manifest must contain exactly framework, daemon, server, and console")
    for component_name, component in components.items():
        source_binding = component.get("source_binding")
        has_source_evidence = source_binding in {"signed_tag_external_provenance", "pinned_commit"} and _is_git_commit(
            component.get("source_commit")
        )
        if not has_source_evidence:
            raise SystemExit(f"release component {component_name} has no valid source binding")
        artifact = component.get("artifact")
        if not isinstance(artifact, dict) or not _is_sha256(artifact.get("sha256")):
            raise SystemExit(f"release component {component_name} has no artifact checksum")
    python_artifacts = manifest.get("python", {}).get("artifacts")
    if not isinstance(python_artifacts, list) or not python_artifacts:
        raise SystemExit("release manifest must contain non-empty Python artifacts")
    for artifact in python_artifacts:
        if not _is_sha256(artifact.get("sha256")):
            raise SystemExit(f"Python artifact {artifact.get('filename')!r} has no checksum")
        filename = artifact.get("filename")
        url = artifact.get("url")
        if not isinstance(filename, str) or not _is_credential_free_https_url(url):
            raise SystemExit(f"Python artifact {filename!r} has no credential-free HTTPS URL")
        if Path(urllib.parse.urlparse(url).path).name != filename:
            raise SystemExit(f"Python artifact URL does not end in {filename}")
    if not _is_sha256(manifest.get("console", {}).get("integrity_sha256")):
        raise SystemExit("release manifest has no Console integrity checksum")
    public_artifacts = manifest.get("public_artifacts")
    if not isinstance(public_artifacts, list) or not public_artifacts:
        raise SystemExit("release manifest must contain non-empty public artifacts")
    for artifact in public_artifacts:
        if not _is_sha256(artifact.get("sha256")):
            raise SystemExit(f"public artifact {artifact.get('url')!r} has no checksum")
    github_release = manifest.get("github_release")
    github_assets = github_release.get("assets") if isinstance(github_release, dict) else None
    if not isinstance(github_assets, list) or not github_assets:
        raise SystemExit("release manifest must contain non-empty GitHub release assets")
    for artifact in github_assets:
        if not isinstance(artifact, dict) or not _is_sha256(artifact.get("sha256")):
            raise SystemExit(f"GitHub release asset {artifact.get('name')!r} has no checksum")
    docker = manifest.get("docker")
    if not isinstance(docker, dict) or not _is_docker_digest(docker.get("manifest_digest")):
        raise SystemExit("release manifest has no immutable Docker manifest digest")
    platform_digests = docker.get("platform_digests")
    if not isinstance(platform_digests, dict) or set(platform_digests) != {
        "linux/amd64",
        "linux/arm64",
    }:
        raise SystemExit("release manifest must contain exact Docker platform digests")
    if not all(_is_docker_digest(digest) for digest in platform_digests.values()):
        raise SystemExit("release manifest contains an invalid Docker platform digest")


def require_console_build_identity(
    build: dict[str, Any],
    manifest: dict[str, Any],
) -> None:
    """Bind deployed Console metadata to the reviewed source and contracts."""

    console = manifest["components"]["console"]
    source = build.get("source")
    if (
        set(build)
        != {
            "schema_version",
            "component",
            "release_id",
            "version",
            "evidence_state",
            "source",
            "build",
            "contracts",
        }
        or build.get("schema_version") != 1
        or build.get("component") != "console"
        or build.get("release_id") != manifest["release_id"]
        or build.get("version") != manifest["packages"]["console"]
        or build.get("evidence_state") not in {"built", "published"}
        or not isinstance(source, dict)
        or set(source) != {"repository", "ref", "binding", "commit"}
        or source.get("repository") != console.get("repository")
        or source.get("binding") != "pinned_commit"
        or source.get("ref") != console.get("source_ref")
        or source.get("commit") != console.get("source_commit")
        or not _is_git_commit(source.get("commit"))
        or build.get("contracts") != console.get("contracts")
        or not isinstance(build.get("build"), dict)
        or set(build["build"]) != {"built_at"}
        or not _is_aware_iso(build["build"].get("built_at"))
    ):
        raise SystemExit("deployed Console build identity does not match")


def verify_signed_source_tag(
    manifest: dict[str, Any],
    *,
    repository: Path,
) -> None:
    """Require the local reviewed manifest to come from its signed exact tag."""

    tag = f"v{manifest['version']}"
    _run_git(repository, "verify-tag", tag)
    tag_commit = _run_git(repository, "rev-parse", f"{tag}^{{commit}}")
    if _run_git(repository, "rev-parse", "HEAD") != tag_commit:
        raise SystemExit("signed source tag does not identify the checked-out source")
    for component_name in ("framework", "daemon"):
        component = manifest.get("components", {}).get(component_name)
        if (
            not isinstance(component, dict)
            or component.get("source_binding") != "signed_tag_external_provenance"
            or component.get("source_ref") != tag
            or component.get("source_commit") != tag_commit
        ):
            raise SystemExit(f"signed source tag does not match external BOM component {component_name}")


def _run_git(repository: Path, *arguments: str) -> str:
    process = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )
    if process.returncode:
        detail = process.stderr.strip() or process.stdout.strip()
        raise SystemExit(f"signed source tag verification failed: {detail}")
    return process.stdout.strip()


def require_revalidation(headers: dict[str, str], *, label: str) -> None:
    """Require the complete cache policy for an unhashed public asset."""

    require_cache_directives(
        headers,
        label=label,
        required={"no-cache": None, "max-age": "0", "must-revalidate": None},
    )


def require_cache_directives(
    headers: dict[str, str],
    *,
    label: str,
    required: dict[str, str | None],
) -> None:
    """Parse Cache-Control and require exact directive names and values."""

    directives: dict[str, str | None] = {}
    for part in headers.get("cache-control", "").casefold().split(","):
        name, separator, value = part.strip().partition("=")
        if name:
            directives[name] = value.strip().strip('"') if separator else None
    for name, expected_value in required.items():
        if name not in directives:
            suffix = f"={expected_value}" if expected_value is not None else ""
            raise SystemExit(f"{label} is missing Cache-Control {name}{suffix}")
        if expected_value is not None and directives[name] != expected_value:
            raise SystemExit(f"{label} must use Cache-Control {name}={expected_value}")


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _is_git_commit(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) in {40, 64}
        and all(character in "0123456789abcdef" for character in value)
    )


def _is_docker_digest(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("sha256:") and _is_sha256(value.removeprefix("sha256:"))


def _is_credential_free_https_url(value: Any) -> bool:
    if not isinstance(value, str) or any(character.isspace() for character in value):
        return False
    try:
        parsed = urllib.parse.urlparse(value)
        return (
            parsed.scheme == "https"
            and parsed.hostname is not None
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
        )
    except ValueError:
        return False


def _is_aware_iso(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def fetch(url: str) -> tuple[bytes, dict[str, str]]:
    """Fetch one required release URL or terminate with a concise status."""

    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = response.status
            body = response.read()
            headers: dict[str, str] = {}
            for key, value in response.headers.items():
                normalized = key.casefold()
                previous = headers.get(normalized)
                headers[normalized] = f"{previous}, {value}" if previous is not None else value
    except urllib.error.HTTPError as error:
        raise SystemExit(f"release URL failed with HTTP {error.code}: {url}") from error
    except urllib.error.URLError as error:
        raise SystemExit(f"release URL is unreachable: {url}: {error.reason}") from error
    if status != 200:
        raise SystemExit(f"release URL returned HTTP {status}: {url}")
    print(f"HTTP {status} {url}")
    return body, headers


if __name__ == "__main__":
    raise SystemExit(main())
