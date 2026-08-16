#!/usr/bin/env bash
# Build and publish the splime daemon image to Docker Hub (multi-arch).
#
# Prerequisites (once):
#   docker login                                   # to your Docker Hub account (yastrebovks)
#   docker buildx create --use --name splime-builder
#
# Usage:
#   SPLIME_CONFIRM_DOCKER=yastrebovks/spl-daemon:0.4.7 ./publish.sh 0.4.7
#
# Prefer release/0.4.7/update-docker.sh, which also verifies PyPI and runs an
# exact-image smoke test. This bounded context helper retains the historical
# direct buildx entry point but never pushes without an exact confirmation.
set -euo pipefail

VERSION="${1:-0.4.7}"
IMAGE="yastrebovks/spl-daemon"
PLATFORMS="linux/amd64,linux/arm64"

if [[ "${SPLIME_CONFIRM_DOCKER:-}" != "${IMAGE}:${VERSION}" ]]; then
  echo "Refusing to push. Set SPLIME_CONFIRM_DOCKER=${IMAGE}:${VERSION}." >&2
  exit 2
fi

cd "$(dirname "$0")"

echo "Building ${IMAGE}:${VERSION} (+ latest) for ${PLATFORMS} ..."
docker buildx build \
  --platform "${PLATFORMS}" \
  --build-arg "SPL_VERSION=${VERSION}" \
  --tag "${IMAGE}:${VERSION}" \
  --tag "${IMAGE}:latest" \
  --push \
  .

echo "Pushed ${IMAGE}:${VERSION} and ${IMAGE}:latest"
