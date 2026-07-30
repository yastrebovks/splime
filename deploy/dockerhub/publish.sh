#!/usr/bin/env bash
# Build and publish the splime daemon image to Docker Hub (multi-arch).
#
# Prerequisites (once):
#   docker login                                   # to your Docker Hub account (yastrebovks)
#   docker buildx create --use --name splime-builder
#
# Usage:
#   ./publish.sh            # builds and pushes the configured version + latest
#   ./publish.sh <version>  # builds and pushes a specific version + latest
set -euo pipefail

VERSION="${1:-0.4.6}"
IMAGE="yastrebovks/spl-daemon"
PLATFORMS="linux/amd64,linux/arm64"

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
