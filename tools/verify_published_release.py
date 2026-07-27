"""Fail closed when a published SPLime release URL or checksum drifts."""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

USER_AGENT = "splime-release-verifier/0.4.5"


def main() -> int:
    """Verify PyPI, Docker Hub, and the deployed Console against one manifest."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=Path("release-manifest.json"))
    args = parser.parse_args()
    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)

    remote_manifest_bytes, _ = fetch(manifest["manifest_url"])
    if json.loads(remote_manifest_bytes) != manifest:
        raise SystemExit("published release manifest does not match the checked-in manifest")

    for url in manifest["install_page_urls"]:
        fetch(url)

    verify_pypi(manifest)
    verify_public_artifacts(manifest)
    verify_docker(manifest)
    verify_console(manifest)
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
        data, _ = fetch(item["url"])
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
    """Verify that the manifest's exact Docker Hub tag exists."""

    body, _ = fetch(manifest["docker"]["verification_url"])
    tag = json.loads(body)
    if tag.get("name") != manifest["docker"]["tag"]:
        raise SystemExit("Docker Hub returned the wrong release tag")
    fetch(manifest["docker"]["publication_url"])


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
