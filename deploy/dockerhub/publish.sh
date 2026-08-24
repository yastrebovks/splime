#!/usr/bin/env bash
# Compatibility entry point for the canonical, guarded Docker release script.
#
# Prerequisites (once):
#   docker login                                   # to your Docker Hub account (yastrebovks)
#   docker buildx create --use --name splime-builder
#
# Usage:
#   SPLIME_CONFIRM_DOCKER=yastrebovks/spl-daemon:0.4.9 ./publish.sh 0.4.9
#
# This file intentionally contains no second build/push implementation.
set -euo pipefail

VERSION="${1:-0.4.9}"
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)"
CANONICAL="${ROOT}/release/${VERSION}/update-docker.sh"

if [[ ! -x "${CANONICAL}" ]]; then
  printf 'No canonical Docker release script for SPLime %s: %s\n' \
    "${VERSION}" "${CANONICAL}" >&2
  exit 2
fi

exec "${CANONICAL}" push
