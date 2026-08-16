#!/usr/bin/env bash
set -euo pipefail

VERSION="0.4.7"
IMAGE="yastrebovks/spl-daemon"
PLATFORMS="linux/amd64,linux/arm64"
PACKAGE_REVISION="3f42945a96fe2347ba13c5fd89f10120137cdf6b"
UV_VERSION="0.11.25"
PYTHON_BASE_DIGEST="sha256:ffb752e139c0a19692a43af8d8523b274222dd68eebad5d583b45c2201c6e30a"
DOCKER_CLI_DIGEST="sha256:851f91d241214e7c6db86513b270d58776379aacc5eb9c4a87e5b47115e3065c"
WHEEL_SHA256="57f795eb4995f63617a89806e886d410051eb4646e50a2dcb8f2f2fc5b457bf7"
SDIST_SHA256="5c3c20d635f01cc6d17fdf9d0064c9d55a8c068defd9fef073ec8fe8a9ba7b77"
MODE="${1:-sources}"

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
CONTEXT="${ROOT}/deploy/dockerhub"
LOCAL_TAG_PREFIX="${IMAGE}:${VERSION}-release-smoke"
SOURCE_REVISION="$(git -C "${ROOT}" rev-parse HEAD)"
BUILD_DATE="$(git -C "${ROOT}" show -s --format=%cI HEAD)"
BUILD_ARGUMENTS=(
  --build-arg "SPL_VERSION=${VERSION}"
  --build-arg "UV_VERSION=${UV_VERSION}"
  --build-arg "SPL_PACKAGE_REVISION=${PACKAGE_REVISION}"
  --build-arg "SPL_SOURCE_REVISION=${SOURCE_REVISION}"
  --build-arg "SPL_BUILD_DATE=${BUILD_DATE}"
)

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1"
}

assert_sources() {
  [[ -f "${CONTEXT}/Dockerfile" ]] || fail "canonical Docker context is missing"
  grep -Fxq "ARG SPL_VERSION=${VERSION}" "${CONTEXT}/Dockerfile" \
    || fail "Dockerfile does not default to ${VERSION}"
  grep -Fxq "ARG UV_VERSION=${UV_VERSION}" "${CONTEXT}/Dockerfile" \
    || fail "Dockerfile does not pin the tested uv version"
  grep -Fxq "ARG SPL_PACKAGE_REVISION=${PACKAGE_REVISION}" "${CONTEXT}/Dockerfile" \
    || fail "Dockerfile does not bind the package release commit"
  grep -Fq "FROM python:3.13-slim@${PYTHON_BASE_DIGEST}" "${CONTEXT}/Dockerfile" \
    || fail "Python base image is not pinned to the release digest"
  grep -Fq "FROM docker:27-cli@${DOCKER_CLI_DIGEST}" "${CONTEXT}/Dockerfile" \
    || fail "Docker CLI image is not pinned to the release digest"
  grep -Fq "image: ${IMAGE}:${VERSION}" "${CONTEXT}/docker-compose.yml" \
    || fail "Compose image is not pinned to ${VERSION}"
  [[ "$(find "${CONTEXT}" -mindepth 1 -maxdepth 1 -type f -print | wc -l | tr -d ' ')" == "5" ]] \
    || fail "canonical Docker context contains an unexpected file"
}

assert_pypi() {
  local metadata_directory metadata_file result
  need curl
  metadata_directory="$(mktemp -d "${TMPDIR:-/tmp}/splime-pypi-${VERSION}.XXXXXX")"
  metadata_file="${metadata_directory}/release.json"
  result=0
  curl --fail --silent --show-error --location \
    --retry 3 --retry-all-errors --connect-timeout 10 --max-time 30 \
    "https://pypi.org/pypi/splime/${VERSION}/json" \
    --output "${metadata_file}" || result=$?
  if [[ "${result}" == "0" ]]; then
    python3 - "${metadata_file}" "${VERSION}" "${WHEEL_SHA256}" "${SDIST_SHA256}" <<'PY' \
      || result=$?
import json
from pathlib import Path
import sys

metadata, version, wheel_hash, sdist_hash = sys.argv[1:]
payload = json.loads(Path(metadata).read_text(encoding="utf-8"))
if payload["info"]["version"] != version:
    raise SystemExit("PyPI returned a different SPLime version")
files = {
    item["filename"]: item["digests"]["sha256"]
    for item in payload["urls"]
}
expected = {
    f"splime-{version}-py3-none-any.whl": wheel_hash,
    f"splime-{version}.tar.gz": sdist_hash,
}
if files != expected:
    raise SystemExit(f"unexpected PyPI artifact identity: {files}")
print(f"PyPI splime {version}: exact wheel and sdist hashes verified")
PY
  fi
  unlink "${metadata_file}" 2>/dev/null || true
  rmdir "${metadata_directory}" 2>/dev/null || true
  [[ "${result}" == "0" ]] || fail "exact PyPI ${VERSION} verification failed"
}

smoke_image() {
  local tag="$1" platform="${2:-}" name label_version label_revision package_revision
  local -a run_arguments
  name="splime-release-smoke-${VERSION//./-}-$PPID"
  run_arguments=(-d --name "${name}")
  if [[ -n "${platform}" ]]; then
    run_arguments+=(--platform "${platform}")
  fi
  docker run "${run_arguments[@]}" "${tag}" >/dev/null
  for _ in $(seq 1 40); do
    state="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${name}")"
    if [[ "${state}" == "healthy" ]]; then
      docker exec "${name}" python -m spl.daemon --version | grep -Fx "spl-daemon ${VERSION}" >/dev/null
      docker exec "${name}" python -m pip check >/dev/null
      docker exec "${name}" uv --version \
        | awk -v expected="${UV_VERSION}" '$1 == "uv" && $2 == expected {found=1} END {exit !found}'
      label_version="$(docker inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "${name}")"
      label_revision="$(docker inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "${name}")"
      package_revision="$(docker inspect --format '{{index .Config.Labels "io.splime.package.revision"}}' "${name}")"
      if [[ "${label_version}" != "${VERSION}" \
        || "${label_revision}" != "${SOURCE_REVISION}" \
        || "${package_revision}" != "${PACKAGE_REVISION}" ]]; then
        docker rm -f "${name}" >/dev/null
        fail "container OCI version/source labels are incorrect"
      fi
      docker rm -f "${name}" >/dev/null
      printf 'PASS: %s (%s) is healthy and reports SPLime %s.\n' \
        "${tag}" "${platform:-native}" "${VERSION}"
      return 0
    fi
    if [[ "${state}" == "unhealthy" || "${state}" == "exited" || "${state}" == "dead" ]]; then
      docker logs "${name}" >&2 || true
      docker rm -f "${name}" >/dev/null || true
      fail "container smoke failed in state ${state}"
    fi
    sleep 1
  done
  docker logs "${name}" >&2 || true
  docker rm -f "${name}" >/dev/null || true
  fail "container did not become healthy"
}

check_sources() {
  need docker
  assert_sources
  [[ "$(git -C "${ROOT}" rev-parse "v${VERSION}^{}")" == "${PACKAGE_REVISION}" ]] \
    || fail "v${VERSION} does not resolve to the expected release commit"
  docker buildx version >/dev/null
  printf 'PASS: Docker source controls for %s are ready.\n' "${VERSION}"
}

check_release() {
  check_sources
  need python3
  assert_pypi
  printf 'PASS: Docker release inputs for %s are ready.\n' "${VERSION}"
}

build_local() {
  local platform architecture local_tag
  check_release
  for platform in linux/amd64 linux/arm64; do
    architecture="${platform##*/}"
    local_tag="${LOCAL_TAG_PREFIX}-${architecture}"
    docker buildx build \
      --platform "${platform}" \
      "${BUILD_ARGUMENTS[@]}" \
      --tag "${local_tag}" \
      --load \
      "${CONTEXT}"
    smoke_image "${local_tag}" "${platform}"
  done
}

push_multiarch() {
  [[ "${SPLIME_CONFIRM_DOCKER:-}" == "${IMAGE}:${VERSION}" ]] \
    || fail "set SPLIME_CONFIRM_DOCKER=${IMAGE}:${VERSION} to authorize Docker Hub publication"
  [[ -z "$(git -C "${ROOT}" status --porcelain)" ]] \
    || fail "commit the Docker release controls before publication"
  [[ "$(git -C "${ROOT}" rev-parse github/main)" == "${SOURCE_REVISION}" ]] \
    || fail "push the Docker release-control commit to github/main before publication"
  build_local
  docker buildx build \
    --platform "${PLATFORMS}" \
    "${BUILD_ARGUMENTS[@]}" \
    --provenance=true \
    --sbom=true \
    --tag "${IMAGE}:${VERSION}" \
    --tag "${IMAGE}:0.4" \
    --tag "${IMAGE}:latest" \
    --push \
    "${CONTEXT}"
  verify_remote
}

verify_remote() {
  local manifest_directory manifest_file version_digest minor_digest latest_digest
  need docker
  manifest_directory="$(mktemp -d "${TMPDIR:-/tmp}/splime-docker-${VERSION}.XXXXXX")"
  manifest_file="${manifest_directory}/manifest.txt"
  docker buildx imagetools inspect "${IMAGE}:${VERSION}" > "${manifest_file}"
  grep -Fq 'linux/amd64' "${manifest_file}" \
    || fail "published manifest lacks linux/amd64"
  grep -Fq 'linux/arm64' "${manifest_file}" \
    || fail "published manifest lacks linux/arm64"
  version_digest="$(awk '$1 == "Digest:" {print $2; exit}' "${manifest_file}")"
  minor_digest="$(docker buildx imagetools inspect "${IMAGE}:0.4" | awk '$1 == "Digest:" {print $2; exit}')"
  latest_digest="$(docker buildx imagetools inspect "${IMAGE}:latest" | awk '$1 == "Digest:" {print $2; exit}')"
  [[ -n "${version_digest}" && "${version_digest}" == "${minor_digest}" && "${version_digest}" == "${latest_digest}" ]] \
    || fail "0.4.7, 0.4 and latest do not resolve to one manifest digest"
  unlink "${manifest_file}"
  rmdir "${manifest_directory}"
  docker pull "${IMAGE}:${VERSION}"
  smoke_image "${IMAGE}:${VERSION}"
}

case "${MODE}" in
  sources) check_sources ;;
  check) check_release ;;
  build) build_local ;;
  push) push_multiarch ;;
  verify) verify_remote ;;
  *)
    printf 'Usage: %s {sources|check|build|push|verify}\n' "$0" >&2
    exit 2
    ;;
esac
