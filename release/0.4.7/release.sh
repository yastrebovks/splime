#!/usr/bin/env bash
set -euo pipefail

VERSION="0.4.7"
TAG="v${VERSION}"
REMOTE="github"
BRANCH="main"
REPOSITORY="yastrebovks/splime"
SIGNING_FINGERPRINT="31E24377474710AF950C81C6B8C5D1937087FA85"

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/../.." && pwd -P)"
OUT="${ROOT}/dist/release-${VERSION}"
MODE="${1:-check}"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1"
}

python_bin() {
  if [[ -x "${ROOT}/.venv/bin/python" ]]; then
    printf '%s\n' "${ROOT}/.venv/bin/python"
  else
    command -v python3.13 || fail "Python 3.13 is required"
  fi
}

release_python() {
  local environment python
  environment="${OUT}/release-tool-venv"
  python="${environment}/bin/python"
  if [[ ! -x "${python}" ]]; then
    "$(python_bin)" -m venv "${environment}" >&2
  fi
  if ! "${python}" -c 'import build, twine' >/dev/null 2>&1; then
    "${python}" -m pip install --disable-pip-version-check \
      "build==1.5.0" "twine==6.2.0" >&2
  fi
  printf '%s\n' "${python}"
}

assert_identity() {
  [[ "$(git -C "${ROOT}" branch --show-current)" == "${BRANCH}" ]] \
    || fail "release must run from branch ${BRANCH}"
  [[ "$(git -C "${ROOT}" remote get-url "${REMOTE}")" == "git@github.com:${REPOSITORY}.git" ]] \
    || fail "remote ${REMOTE} is not git@github.com:${REPOSITORY}.git"

  "$(python_bin)" - "${ROOT}" "${VERSION}" <<'PY'
import json
from pathlib import Path
import sys
import tomllib

root = Path(sys.argv[1])
expected = sys.argv[2]
project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
contract = json.loads((root / "release-contract.json").read_text(encoding="utf-8"))
manifest = json.loads((root / "release-manifest.json").read_text(encoding="utf-8"))
matrix = json.loads((root / "release/compatibility-matrix.json").read_text(encoding="utf-8"))
observed = {
    "pyproject": project["project"]["version"],
    "contract": contract["version"],
    "manifest": manifest["version"],
}
if set(observed.values()) != {expected}:
    raise SystemExit(f"release versions do not agree: {observed}")
if contract["source_tag"] != f"v{expected}":
    raise SystemExit("release contract source tag is stale")
if contract["server_schema_target"] != 43 or manifest["server"]["schema_target"] != 43:
    raise SystemExit("server schema target must be 43")
if matrix["release_id"] != f"splime-{expected}":
    raise SystemExit("compatibility matrix release identity is stale")
PY

  [[ -f "${ROOT}/.github/release-signing-public-key.asc" ]] \
    || fail "versioned release signing public key is missing"
}

assert_no_sensitive_paths() {
  "$(python_bin)" - "${ROOT}" <<'PY'
from pathlib import Path
import re
import subprocess
import sys

root = Path(sys.argv[1])
payload = subprocess.check_output(
    ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
    cwd=root,
)
paths = [item.decode() for item in payload.split(b"\0") if item]
pattern = re.compile(
    r"(^|/)(\.env(?:\..*)?|.*(?:token|secret|credential).*|id_rsa|.*\.(?:pem|key|pwd))$",
    re.IGNORECASE,
)
allowed = {
    ".github/release-signing-public-key.asc",
    "src/spl/daemon/secret_store.py",
}
blocked = [path for path in paths if pattern.search(path) and path not in allowed]
if blocked:
    raise SystemExit("sensitive-looking release paths:\n" + "\n".join(blocked))
PY
}

assert_complete_diff_clean() {
  # Plain `git diff --check` omits untracked files. Build a disposable index so
  # the check covers the exact tree that `commit` will stage without mutating
  # the operator's real index.
  local temporary_directory temporary_index result
  temporary_directory="$(mktemp -d "${TMPDIR:-/tmp}/splime-release-index.XXXXXX")"
  temporary_index="${temporary_directory}/index"
  result=0
  GIT_INDEX_FILE="${temporary_index}" git -C "${ROOT}" read-tree HEAD
  GIT_INDEX_FILE="${temporary_index}" git -C "${ROOT}" add --all
  GIT_INDEX_FILE="${temporary_index}" git -C "${ROOT}" diff --cached --check \
    || result=$?
  unlink "${temporary_index}"
  rmdir "${temporary_directory}"
  return "${result}"
}

build_artifacts() {
  local python first second smoke wheel source_date_epoch
  python="$(release_python)"
  first="${OUT}/dist"
  second="${OUT}/dist-reproducibility"
  smoke="${OUT}/smoke-venv"
  source_date_epoch="$(git -C "${ROOT}" log -1 --format=%ct HEAD)"
  mkdir -p "${first}" "${second}"

  "${python}" -m tools.build_release_artifacts \
    --out-dir "${first}" --source-date-epoch "${source_date_epoch}"
  "${python}" -m tools.build_release_artifacts \
    --out-dir "${second}" --source-date-epoch "${source_date_epoch}"
  "${python}" - "${first}" "${second}" <<'PY'
from pathlib import Path
import hashlib
import sys

def digests(root: Path) -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.iterdir())
        if path.is_file()
    }

first, second = map(Path, sys.argv[1:])
left, right = digests(first), digests(second)
if len(left) != 2 or left != right:
    raise SystemExit(f"release build is not reproducible: {left} != {right}")
for name, digest in left.items():
    print(f"{digest}  {name}")
PY

  "${python}" -m twine check "${first}"/*
  wheel="$(find "${first}" -maxdepth 1 -type f -name "splime-${VERSION}-*.whl" -print)"
  [[ -n "${wheel}" && "$(printf '%s\n' "${wheel}" | wc -l | tr -d ' ')" == "1" ]] \
    || fail "exactly one ${VERSION} wheel is required"
  "${python}" -m venv --clear "${smoke}"
  "${smoke}/bin/python" -m pip install --disable-pip-version-check "${wheel}"
  "${smoke}/bin/python" -m pip check
  [[ "$("${smoke}/bin/python" -m spl.daemon --version)" == "spl-daemon ${VERSION}" ]] \
    || fail "installed wheel reports an unexpected daemon version"
  (
    cd "${first}"
    shasum -a 256 ./* > "${OUT}/python-artifacts.sha256"
  )
}

run_checks() {
  local python
  python="$(python_bin)"
  assert_identity
  assert_no_sensitive_paths
  assert_complete_diff_clean
  "${python}" -m ruff format --check "${ROOT}/src" "${ROOT}/tests"
  "${python}" -m ruff check "${ROOT}/src" "${ROOT}/tests"
  "${python}" -m mypy "${ROOT}/src/spl"
  (
    cd "${ROOT}"
    "${python}" -m pytest -m "not smoke" -q
  )
  build_artifacts
  printf 'PASS: SPLime %s is locally release-ready.\n' "${VERSION}"
}

assert_signing_key() {
  need gpg
  gpg --batch --list-secret-keys --with-colons "${SIGNING_FINGERPRINT}" \
    | awk -F: -v expected="${SIGNING_FINGERPRINT}" '$1 == "fpr" && $10 == expected {found=1} END {exit !found}' \
    || fail "secret release-signing key ${SIGNING_FINGERPRINT} is unavailable"
}

prepare_gpg_terminal() {
  local signing_tty
  [[ -t 0 && -t 1 ]] \
    || fail "signed release commit requires an interactive terminal; run this command directly in Terminal/iTerm"
  signing_tty="$(tty)"
  [[ "${signing_tty}" == /dev/* ]] \
    || fail "could not resolve the interactive terminal for GPG pinentry"
  export GPG_TTY="${signing_tty}"
  if command -v gpg-connect-agent >/dev/null 2>&1; then
    gpg-connect-agent updatestartuptty /bye >/dev/null
  fi
}

commit_release() {
  [[ "${SPLIME_RELEASE_CONFIRM:-}" == "${VERSION}" ]] \
    || fail "set SPLIME_RELEASE_CONFIRM=${VERSION} to authorize commit and tag creation"
  [[ -z "$(git -C "${ROOT}" tag --list "${TAG}")" ]] || fail "local tag ${TAG} already exists"
  prepare_gpg_terminal
  assert_signing_key
  run_checks
  git -C "${ROOT}" add --all
  git -C "${ROOT}" diff --cached --check
  git -C "${ROOT}" diff --cached --stat
  git -C "${ROOT}" commit -S"${SIGNING_FINGERPRINT}" -m "release: SPLime ${VERSION}"
  git -C "${ROOT}" tag -s -u "${SIGNING_FINGERPRINT}" -m "SPLime ${VERSION}" "${TAG}"
  git -C "${ROOT}" verify-tag "${TAG}"
  printf 'Created signed commit and tag %s locally. Nothing was pushed.\n' "${TAG}"
}

push_release() {
  need ssh
  assert_identity
  [[ -z "$(git -C "${ROOT}" status --porcelain)" ]] || fail "worktree must be clean before push"
  [[ "$(git -C "${ROOT}" rev-list -n1 "${TAG}")" == "$(git -C "${ROOT}" rev-parse HEAD)" ]] \
    || fail "${TAG} does not point to HEAD"
  git -C "${ROOT}" verify-tag "${TAG}"
  [[ -z "$(git -C "${ROOT}" ls-remote --tags "${REMOTE}" "refs/tags/${TAG}")" ]] \
    || fail "remote tag ${TAG} already exists"
  git -C "${ROOT}" fetch "${REMOTE}" "${BRANCH}"
  git -C "${ROOT}" merge-base --is-ancestor "${REMOTE}/${BRANCH}" HEAD \
    || fail "local release is not a fast-forward of ${REMOTE}/${BRANCH}"
  git -C "${ROOT}" push --atomic "${REMOTE}" "HEAD:${BRANCH}" "refs/tags/${TAG}"
  printf 'Pushed %s atomically. GitHub TestPyPI workflow is now expected to run.\n' "${TAG}"
}

ensure_local_artifacts() {
  # Rebuild after the release commit so local artifacts use the same source
  # epoch and bytes as the exact-tag GitHub workflow.
  build_artifacts
  [[ -f "${OUT}/dist/splime-${VERSION}-py3-none-any.whl" ]] \
    || fail "release wheel is missing"
  [[ -f "${OUT}/dist/splime-${VERSION}.tar.gz" ]] || fail "release sdist is missing"
}

draft_release() {
  need gh
  assert_identity
  ensure_local_artifacts
  gh auth status --hostname github.com >/dev/null
  gh release view "${TAG}" --repo "${REPOSITORY}" >/dev/null 2>&1 \
    && fail "GitHub Release ${TAG} already exists"
  [[ -n "$(git -C "${ROOT}" ls-remote --tags "${REMOTE}" "refs/tags/${TAG}")" ]] \
    || fail "push the signed tag before creating a draft"
  gh release create "${TAG}" \
    --repo "${REPOSITORY}" \
    --verify-tag \
    --draft \
    --title "SPLime ${VERSION}" \
    --notes-file "${SCRIPT_DIR}/release-notes.md" \
    "${OUT}/dist/splime-${VERSION}-py3-none-any.whl" \
    "${OUT}/dist/splime-${VERSION}.tar.gz" \
    "${OUT}/python-artifacts.sha256" \
    "${ROOT}/release-manifest.json"
  printf 'Created draft GitHub Release %s. PyPI was not triggered.\n' "${TAG}"
}

publish_release() {
  need gh
  [[ "${SPLIME_CONFIRM_PYPI:-}" == "${VERSION}" ]] \
    || fail "set SPLIME_CONFIRM_PYPI=${VERSION}; publishing the release triggers production PyPI"
  gh auth status --hostname github.com >/dev/null
  [[ "$(gh release view "${TAG}" --repo "${REPOSITORY}" --json isDraft --jq .isDraft)" == "true" ]] \
    || fail "${TAG} must exist as a reviewed draft"
  gh release edit "${TAG}" --repo "${REPOSITORY}" --draft=false --latest
  printf 'Published GitHub Release %s. The trusted PyPI workflow is now expected to run.\n' "${TAG}"
}

case "${MODE}" in
  check) run_checks ;;
  artifacts)
    assert_identity
    assert_no_sensitive_paths
    build_artifacts
    ;;
  commit) commit_release ;;
  push) push_release ;;
  draft) draft_release ;;
  publish) publish_release ;;
  *)
    printf 'Usage: %s {check|artifacts|commit|push|draft|publish}\n' "$0" >&2
    exit 2
    ;;
esac
