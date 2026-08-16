#!/usr/bin/env bash
set -euo pipefail

VERSION="0.4.7"
IMAGE="yastrebovks/spl-daemon"
PLATFORMS="linux/amd64,linux/arm64"
MODE="${1:-sources}"

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
CONTEXT="${ROOT}/deploy/dockerhub"
LOCAL_TAG="${IMAGE}:${VERSION}-release-smoke"

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
  grep -Fq "image: ${IMAGE}:${VERSION}" "${CONTEXT}/docker-compose.yml" \
    || fail "Compose image is not pinned to ${VERSION}"
  [[ "$(find "${CONTEXT}" -mindepth 1 -maxdepth 1 -type f -print | wc -l | tr -d ' ')" == "5" ]] \
    || fail "canonical Docker context contains an unexpected file"
}

assert_pypi() {
  python3 - "${VERSION}" <<'PY'
import json
import sys
import urllib.request

version = sys.argv[1]
request = urllib.request.Request(
    f"https://pypi.org/pypi/splime/{version}/json",
    headers={"User-Agent": f"splime-docker-release/{version}"},
)
with urllib.request.urlopen(request, timeout=20) as response:
    payload = json.load(response)
if payload["info"]["version"] != version:
    raise SystemExit("PyPI returned a different SPLime version")
files = {item["filename"] for item in payload["urls"]}
expected = {f"splime-{version}-py3-none-any.whl", f"splime-{version}.tar.gz"}
if files != expected:
    raise SystemExit(f"unexpected PyPI artifact set: {files}")
print(f"PyPI splime {version}: {', '.join(sorted(files))}")
PY
}

smoke_image() {
  local tag="$1" name
  name="splime-release-smoke-${VERSION//./-}-$PPID"
  docker run -d --name "${name}" "${tag}" >/dev/null
  for _ in $(seq 1 40); do
    state="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${name}")"
    if [[ "${state}" == "healthy" ]]; then
      docker exec "${name}" python -m spl.daemon --version | grep -Fx "spl-daemon ${VERSION}" >/dev/null
      docker rm -f "${name}" >/dev/null
      printf 'PASS: %s is healthy and reports SPLime %s.\n' "${tag}" "${VERSION}"
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
  check_release
  docker buildx build \
    --platform linux/amd64 \
    --build-arg "SPL_VERSION=${VERSION}" \
    --tag "${LOCAL_TAG}" \
    --load \
    "${CONTEXT}"
  smoke_image "${LOCAL_TAG}"
}

push_multiarch() {
  [[ "${SPLIME_CONFIRM_DOCKER:-}" == "${IMAGE}:${VERSION}" ]] \
    || fail "set SPLIME_CONFIRM_DOCKER=${IMAGE}:${VERSION} to authorize Docker Hub publication"
  build_local
  docker buildx build \
    --platform "${PLATFORMS}" \
    --build-arg "SPL_VERSION=${VERSION}" \
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
  need docker
  docker buildx imagetools inspect "${IMAGE}:${VERSION}" > /tmp/splime-docker-${VERSION}-manifest.txt
  grep -Fq 'linux/amd64' /tmp/splime-docker-${VERSION}-manifest.txt \
    || fail "published manifest lacks linux/amd64"
  grep -Fq 'linux/arm64' /tmp/splime-docker-${VERSION}-manifest.txt \
    || fail "published manifest lacks linux/arm64"
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
