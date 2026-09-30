#!/usr/bin/env bash
set -euo pipefail

VERSION="0.4.10"
TAG="v${VERSION}"
MODE="${1:-check}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
OUT="${ROOT}/dist/release-${VERSION}"
WORKSPACE_ROOT="${SPLIME_RELEASE_WORKSPACE_ROOT:-${ROOT}/..}"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

python_bin() {
  if [[ -n "${SPLIME_RELEASE_TEST_PYTHON:-}" ]]; then
    [[ -x "${SPLIME_RELEASE_TEST_PYTHON}" ]] || fail "SPLIME_RELEASE_TEST_PYTHON is not executable"
    printf '%s\n' "${SPLIME_RELEASE_TEST_PYTHON}"
  elif [[ -x "${ROOT}/.venv/bin/python" ]]; then
    printf '%s\n' "${ROOT}/.venv/bin/python"
  else
    command -v python3.13 || fail "Python 3.13 is required"
  fi
}

assert_identity() {
  "$(python_bin)" - "${ROOT}" "${VERSION}" <<'PY'
import json
from pathlib import Path
import sys
import tomllib

root = Path(sys.argv[1])
version = sys.argv[2]
project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
contract = json.loads((root / "release-contract.json").read_text(encoding="utf-8"))
manifest = json.loads((root / "release-manifest.json").read_text(encoding="utf-8"))
if project["project"]["version"] != version:
    raise SystemExit("pyproject version is stale")
if contract["version"] != version or contract["source_tag"] != f"v{version}":
    raise SystemExit("release contract identity is stale")
if manifest["version"] != version or manifest["evidence"]["state"] != "declared":
    raise SystemExit("tracked release manifest must be the current declaration")
if manifest["python"]["install_requirement"] != f"splime=={version}":
    raise SystemExit("release install requirement is stale")
PY
}

run_checks() {
  local python docs_out
  python="$(python_bin)"
  docs_out="$(mktemp -d "${TMPDIR:-/tmp}/splime-docs-${VERSION}.XXXXXX")"
  trap 'rm -rf -- "${docs_out}"' RETURN
  assert_identity
  git -C "${ROOT}" diff --check
  (
    cd "${ROOT}"
    "${python}" -m tools.generate_release_identity --workspace-root "${WORKSPACE_ROOT}" --check
    "${python}" tools/verify_published_compatibility_extension.py
    "${python}" -m ruff format --check src tests tools
    "${python}" -m ruff check src tests tools
    "${python}" -m mypy src/spl
    "${python}" -m pytest -q
    "${python}" -m sphinx -W --keep-going -b html docs/source "${docs_out}"
  )
  printf 'PASS: SPLime %s source is release-ready.\n' "${VERSION}"
}

build_artifacts() {
  local python source_date_epoch
  python="$(python_bin)"
  source_date_epoch="$(git -C "${ROOT}" log -1 --format=%ct HEAD)"
  mkdir -p "${OUT}"
  (
    cd "${ROOT}"
    "${python}" -m tools.build_release_artifacts \
      --out-dir "${OUT}/python" --source-date-epoch "${source_date_epoch}"
  )
}

dispatch() {
  local target="$1"
  command -v gh >/dev/null 2>&1 || fail "gh is required"
  [[ "${SPLIME_RELEASE_CONFIRM:-}" == "${VERSION}" ]] \
    || fail "set SPLIME_RELEASE_CONFIRM=${VERSION} to authorize the workflow dispatch"
  [[ "${SPL_RELEASE_COOKBOOK_URL:-}" == https://* ]] \
    || fail "SPL_RELEASE_COOKBOOK_URL must be the reviewed HTTPS cookbook URL"
  [[ "${SPL_RELEASE_COOKBOOK_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] \
    || fail "SPL_RELEASE_COOKBOOK_SHA256 must be the reviewed lowercase SHA-256"
  [[ -n "$(git -C "${ROOT}" ls-remote --tags github "refs/tags/${TAG}")" ]] \
    || fail "signed remote tag ${TAG} is missing"
  gh workflow run publish-to-pypi.yml \
    --repo yastrebovks/splime \
    --ref "${TAG}" \
    -f "release-tag=${TAG}" \
    -f "cookbook-url=${SPL_RELEASE_COOKBOOK_URL}" \
    -f "cookbook-sha256=${SPL_RELEASE_COOKBOOK_SHA256}" \
    -f "target=${target}"
}

case "${MODE}" in
  check) run_checks ;;
  artifacts)
    assert_identity
    git -C "${ROOT}" diff --check
    build_artifacts
    ;;
  dispatch-testpypi) dispatch testpypi ;;
  dispatch-pypi) dispatch pypi ;;
  *)
    printf 'Usage: %s {check|artifacts|dispatch-testpypi|dispatch-pypi}\n' "$0" >&2
    exit 2
    ;;
esac
