#!/usr/bin/env bash
set -euo pipefail

VERSION="0.4.9"
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
  if [[ -n "${SPLIME_RELEASE_TEST_PYTHON:-}" ]]; then
    [[ -x "${SPLIME_RELEASE_TEST_PYTHON}" ]] \
      || fail "SPLIME_RELEASE_TEST_PYTHON is not executable"
    printf '%s\n' "${SPLIME_RELEASE_TEST_PYTHON}"
  elif [[ -x "${ROOT}/.venv/bin/python" ]]; then
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
registry = json.loads((root / "src/spl/official-registry.json").read_text(encoding="utf-8"))
workflow = (root / ".github/workflows/publish-to-pypi.yml").read_text(encoding="utf-8")

observed = {
    "pyproject": project["project"]["version"],
    "contract": contract["version"],
    "manifest": manifest["version"],
}
if set(observed.values()) != {expected}:
    raise SystemExit(f"release versions do not agree: {observed}")
if project["project"]["name"] != "splime":
    raise SystemExit("the framework release must contain only the splime distribution")
if contract["source_tag"] != f"v{expected}":
    raise SystemExit("release contract source tag is stale")
if contract["server_schema_target"] != 48 or manifest["server"]["schema_target"] != 48:
    raise SystemExit("server schema target must be 48")
if matrix["release_id"] != f"splime-{expected}":
    raise SystemExit("compatibility matrix release identity is stale")

expected_policy = {
    "version": expected,
    "runtime_lock_schema": "spl.public_runtime_lock.v3",
    "executor": f"splime>={expected}",
}
for label, document in {
    "contract": contract,
    "manifest": manifest,
    "registry": registry,
}.items():
    policy = document["public_runtime_policy"]
    for field, value in expected_policy.items():
        if policy.get(field) != value:
            raise SystemExit(f"{label} public runtime {field} is stale")

artifacts = manifest["python"]["artifacts"]
expected_artifacts = {
    f"splime-{expected}-py3-none-any.whl",
    f"splime-{expected}.tar.gz",
}
if manifest["python"]["distribution"] != "splime":
    raise SystemExit("release manifest declares an unexpected Python distribution")
if {item.get("filename") for item in artifacts} != expected_artifacts:
    raise SystemExit("release manifest must declare exactly the splime wheel and sdist")
if manifest["python"]["install_requirement"] != f"splime=={expected}":
    raise SystemExit("release manifest install requirement is stale")
if manifest["publication_order"][0] != "pypi":
    raise SystemExit("release publication order must start with PyPI")
if contract["components"]["framework"]["package"] != "splime":
    raise SystemExit("framework component package is stale")
if contract["components"]["daemon"]["package"] != "splime":
    raise SystemExit("daemon component must ship in the same splime distribution")

forbidden = "splime" + "-public-worker"
active_roots = [root / "src", root / "tools", root / ".github"]
active_files = [root / "pyproject.toml", root / "release-contract.json", root / "release-manifest.json"]
for active_root in active_roots:
    active_files.extend(path for path in active_root.rglob("*") if path.is_file())
for path in active_files:
    if forbidden in path.read_text(encoding="utf-8", errors="ignore"):
        raise SystemExit(f"forbidden second distribution reference: {path.relative_to(root)}")
if forbidden in workflow:
    raise SystemExit("trusted-publishing workflow references a forbidden second distribution")
if "packages-dir: package/dist/" not in workflow:
    raise SystemExit("trusted-publishing workflow does not bind the exact package directory")

for required in (
    root / "release" / expected / "README.md",
    root / "release" / expected / "release-notes.md",
    root / "release" / expected / "historical-extension.json",
):
    if not required.is_file():
        raise SystemExit(f"required release control is missing: {required.relative_to(root)}")
PY

  [[ -f "${ROOT}/.github/release-signing-public-key.asc" ]] \
    || fail "versioned release signing public key is missing"
}

assert_index_clean() {
  git -C "${ROOT}" diff --cached --quiet \
    || fail "the real Git index must be empty before the release script stages anything"
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

assert_version_unpublished() {
  "$(python_bin)" - "${VERSION}" <<'PY'
import json
import ssl
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import certifi

version = sys.argv[1]
url = f"https://pypi.org/pypi/splime/{version}/json"
try:
    context = ssl.create_default_context(cafile=certifi.where())
    with urlopen(
        Request(url, headers={"User-Agent": "splime-release-preflight/1"}),
        timeout=30,
        context=context,
    ) as response:
        payload = json.loads(response.read())
except HTTPError as exc:
    if exc.code == 404:
        raise SystemExit(0)
    raise SystemExit(f"PyPI preflight returned HTTP {exc.code}") from exc
except (OSError, URLError, ValueError) as exc:
    raise SystemExit(f"PyPI preflight failed closed: {exc}") from exc
raise SystemExit(f"splime=={version} already exists on PyPI: {payload.get('info', {}).get('version')}")
PY
}

build_artifacts() {
  local python first second smoke wheel source_date_epoch
  python="$(release_python)"
  first="${OUT}/dist"
  second="${OUT}/dist-reproducibility"
  smoke="${OUT}/smoke-venv"
  source_date_epoch="$(git -C "${ROOT}" log -1 --format=%ct HEAD)"
  rm -rf -- "${first}" "${second}"
  mkdir -p "${first}" "${second}"

  (
    cd "${ROOT}"
    "${python}" -m tools.build_release_artifacts \
      --out-dir "${first}" --source-date-epoch "${source_date_epoch}"
    "${python}" -m tools.build_release_artifacts \
      --out-dir "${second}" --source-date-epoch "${source_date_epoch}"
  )
  "${python}" - "${first}" "${second}" "${VERSION}" <<'PY'
from email.parser import BytesParser
from pathlib import Path
import hashlib
import sys
import tarfile
import zipfile

first, second = map(Path, sys.argv[1:3])
version = sys.argv[3]

def digests(root: Path) -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.iterdir())
        if path.is_file()
    }

left, right = digests(first), digests(second)
expected = {f"splime-{version}-py3-none-any.whl", f"splime-{version}.tar.gz"}
if set(left) != expected or left != right:
    raise SystemExit(f"release build is not reproducible or has unexpected artifacts: {left} != {right}")

forbidden = "splime" + "-public-worker"
wheel = first / f"splime-{version}-py3-none-any.whl"
with zipfile.ZipFile(wheel) as archive:
    names = archive.namelist()
    metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
    metadata = BytesParser().parsebytes(archive.read(metadata_name))
    if metadata["Name"] != "splime" or metadata["Version"] != version:
        raise SystemExit("wheel metadata identity is incorrect")
    if any(forbidden in value.casefold() for value in metadata.get_all("Requires-Dist", [])):
        raise SystemExit("wheel depends on a forbidden second distribution")
    if any(forbidden in name.casefold() for name in names):
        raise SystemExit("wheel contains a forbidden second distribution")
with tarfile.open(first / f"splime-{version}.tar.gz", "r:gz") as archive:
    if any(forbidden in name.casefold() for name in archive.getnames()):
        raise SystemExit("sdist contains a forbidden second distribution")

for name, digest in sorted(left.items()):
    print(f"{digest}  {name}")
PY

  "${python}" -m twine check "${first}"/*
  wheel="${first}/splime-${VERSION}-py3-none-any.whl"
  "${python}" -m venv --clear "${smoke}"
  "${smoke}/bin/python" -m pip install --disable-pip-version-check "${wheel}"
  "${smoke}/bin/python" -m pip check
  [[ "$("${smoke}/bin/python" -m spl.daemon --version)" == "spl-daemon ${VERSION}" ]] \
    || fail "installed wheel reports an unexpected daemon version"
  [[ "$("${smoke}/bin/python" -I -m spl.daemon.worker --health-check)" == "spl.public_embedded_host.v1" ]] \
    || fail "installed wheel does not expose the embedded host contract"
  "${smoke}/bin/python" - <<'PY'
import importlib.metadata

forbidden = "splime" + "-public-worker"
try:
    importlib.metadata.version(forbidden)
except importlib.metadata.PackageNotFoundError:
    pass
else:
    raise SystemExit("clean release environment contains a forbidden second distribution")
PY
  (
    cd "${first}"
    shasum -a 256 ./* > "${OUT}/python-artifacts.sha256"
  )
}

run_checks() {
  local python cookbook docs_out
  python="$(python_bin)"
  cookbook="${SPL_RELEASE_COOKBOOK_PATH:-${ROOT}/../../Notebooks/splime-cookbook.ipynb}"
  docs_out="$(mktemp -d "${TMPDIR:-/tmp}/splime-docs-${VERSION}.XXXXXX")"

  assert_identity
  assert_index_clean
  assert_no_sensitive_paths
  assert_complete_diff_clean
  assert_version_unpublished
  [[ -f "${cookbook}" ]] \
    || fail "set SPL_RELEASE_COOKBOOK_PATH to the reviewed public cookbook"
  export SPL_RELEASE_COOKBOOK_PATH="${cookbook}"

  "${python}" -m ruff format --check "${ROOT}/src" "${ROOT}/tests" "${ROOT}/tools"
  "${python}" -m ruff check "${ROOT}/src" "${ROOT}/tests" "${ROOT}/tools"
  "${python}" -m mypy "${ROOT}/src/spl"
  (
    cd "${ROOT}"
    "${python}" -m pytest -q
    "${python}" tools/verify_published_compatibility_extension.py
    "${python}" -m sphinx -W --keep-going -b html docs/source "${docs_out}"
  )
  rmdir "${docs_out}" 2>/dev/null || true
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
  [[ "${SPLIME_CONFIRM_PUSH:-}" == "${VERSION}" ]] \
    || fail "set SPLIME_CONFIRM_PUSH=${VERSION} to authorize the atomic branch/tag push"
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
  printf 'Pushed %s atomically. The TestPyPI workflow is now expected to run.\n' "${TAG}"
}

ensure_local_artifacts() {
  build_artifacts
  [[ -f "${OUT}/dist/splime-${VERSION}-py3-none-any.whl" ]] \
    || fail "release wheel is missing"
  [[ -f "${OUT}/dist/splime-${VERSION}.tar.gz" ]] || fail "release sdist is missing"
}

verify_index_artifacts() {
  local index_root
  index_root="$1"
  "$(release_python)" - "${OUT}" "${VERSION}" "${index_root}" <<'PY'
from pathlib import Path
import hashlib
import json
import ssl
import sys
from urllib.request import Request, urlopen

import certifi

out = Path(sys.argv[1])
version = sys.argv[2]
index_root = sys.argv[3]
expected = {
    path.name: hashlib.sha256(path.read_bytes()).hexdigest()
    for path in sorted((out / "dist").iterdir())
    if path.is_file()
}
if set(expected) != {f"splime-{version}-py3-none-any.whl", f"splime-{version}.tar.gz"}:
    raise SystemExit("local release artifact set is incomplete")
url = f"{index_root}/pypi/splime/{version}/json"
context = ssl.create_default_context(cafile=certifi.where())
with urlopen(
    Request(url, headers={"User-Agent": "splime-release-verifier/1"}),
    timeout=30,
    context=context,
) as response:
    payload = json.loads(response.read())
observed = {item["filename"]: item["digests"]["sha256"] for item in payload.get("urls", [])}
if observed != expected:
    raise SystemExit(f"package index artifacts disagree: {observed} != {expected}")
print(f"PASS: {url} exposes the exact reviewed wheel and sdist")
PY
}

draft_release() {
  need gh
  [[ "${SPLIME_CONFIRM_DRAFT:-}" == "${VERSION}" ]] \
    || fail "set SPLIME_CONFIRM_DRAFT=${VERSION} to authorize GitHub draft creation"
  assert_identity
  assert_version_unpublished
  ensure_local_artifacts
  verify_index_artifacts "https://test.pypi.org"
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
  printf 'Created reviewed-artifact draft %s. Production PyPI was not triggered.\n' "${TAG}"
}

publish_release() {
  need gh
  [[ "${SPLIME_CONFIRM_PYPI:-}" == "${VERSION}" ]] \
    || fail "set SPLIME_CONFIRM_PYPI=${VERSION}; publishing the release triggers production PyPI"
  assert_version_unpublished
  ensure_local_artifacts
  verify_index_artifacts "https://test.pypi.org"
  gh auth status --hostname github.com >/dev/null
  [[ "$(gh release view "${TAG}" --repo "${REPOSITORY}" --json isDraft --jq .isDraft)" == "true" ]] \
    || fail "${TAG} must exist as a reviewed draft"
  gh release edit "${TAG}" --repo "${REPOSITORY}" --draft=false --latest
  printf 'Published GitHub Release %s. The trusted production-PyPI workflow is now expected to run.\n' "${TAG}"
}

verify_release() {
  assert_identity
  ensure_local_artifacts
  verify_index_artifacts "https://pypi.org"
  printf 'PASS: SPLime %s production PyPI artifacts match the signed source build.\n' "${VERSION}"
}

case "${MODE}" in
  check) run_checks ;;
  artifacts)
    assert_identity
    assert_index_clean
    assert_no_sensitive_paths
    assert_complete_diff_clean
    assert_version_unpublished
    build_artifacts
    ;;
  commit) commit_release ;;
  push) push_release ;;
  draft) draft_release ;;
  publish) publish_release ;;
  verify) verify_release ;;
  *)
    printf 'Usage: %s {check|artifacts|commit|push|draft|publish|verify}\n' "$0" >&2
    exit 2
    ;;
esac
