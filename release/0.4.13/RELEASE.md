# 0.4.13 candidate and release execution

Release identity: `splime-0.4.13`; declared source refs: `v0.4.13`.
The declaration is not proof of a signed ref, publication, or deployment.
Local preparation does not authorize external release execution. Historical
tags, receipts, Public Hub objects and environments must remain unchanged.

The final release source additionally includes compact dependency inspection
and bounded HTTP 429 retries discovered by the Case4/Case5 release validation.
See `CASE_VALIDATION.md` and `DEPENDENCY_DIAGNOSTICS.md`. Build the final SDK
from the signed tag; the historical candidate wheels below do not contain all
final corrections.

The earlier local candidate is
`artifacts/candidate-0.4.13-20261013-diagnostic-metadata` in the parent workspace.
Use its `evidence/candidate-artifacts.json`, `SHA256SUMS` and the accompanying
`ACCEPTANCE.md` for the selected bytes and validation results. It rebuilds the
SDK after the malformed-diagnostic correction and retains byte-identical
server/Console artifacts from `candidate-0.4.13-20261013-review-fixes`.
That candidate and the older `candidate-0.4.13-20261013/finalv5` SDK/server and
`finalv4` Console remain historical; reused Console tests are explicitly labeled
in the new inventory. All external execution gates below still apply.

The SDK-only and public PR installed-wheel matrices now include the actual-HTTP
sync-response and catalogue-receipt regression suites. The coordinated workflow
also requires the complete Console check and a real loopback author/registry/
two-consumer run against checksum-bound built wheels before public artifact
upload. These gates are configured and their wiring is locally tested; remote
execution is still required. Local Case5 input is not a CI prerequisite.

The coordinated CI workflow applies a narrowly scoped correction to
`tests/daemon/test_sol_083_retention.py` for the already signed `v0.4.13` tag.
The fixture now holds the scheduler condition during setup and queue inspection,
including a deliberately slow setup case, so lease expiry cannot race the
assertion. The original file hash and the changed-file allowlist are checked
before running the suite. Source/build jobs and published package bytes remain
unchanged.

Console integration tests also require the three canonical landing/legal HTML
pages. Coordinated CI downloads their reviewed public HTTPS bytes and verifies
pinned SHA-256 values before the tests, without adding a private-repository
credential or silently skipping integration coverage.

## Artifacts and source authority

The SDK wheel/sdist are public. The `spl-server` wheel is private and must be
transferred through the private deployment channel. It remains bound by the
central manifest's server component checksum, but is absent from the public
GitHub asset allowlist. Never upload the candidate directory recursively: it
also contains private server artifacts, test homes and local test keys.

The source release gate requires clean exact component commits and a verified
signed SDK tag. Working-tree candidates can be tested, but do not satisfy that
gate. After source review, commit only this release's changes in the component
repositories and synchronize the approved public SDK mirror. Create the signed
SDK `v0.4.13` tag first. Use SDK mode in the existing publication workflow below
to publish its exact verified bytes. Only after that publication can the private server index lock
be resolved, committed and tagged `v0.4.13` for the first time. Finalize the
Console tag after its review. Never create a provisional server release tag or
move an existing tag. The full coordinated source gate runs after all these
immutable refs exist; the SDK-only manifest is not full-release evidence.
The development SDK's `github` remote currently points at `git@github.com:Release/spl.git`;
the public mirror is `Release/spl`, whose `github` remote is
`git@github.com:yastrebovks/splime.git`. Inspect remotes before any authorized push.

Set `WORKSPACE` to the absolute approved release workspace path. The commands
in this source/build section are stage 2: run them **after** the SDK-only PyPI
publication, server lock commit and all component tags described below.
From that clean release workspace (`spl` is the approved SDK checkout):

```sh
cd "$WORKSPACE/spl"
python -m tools.generate_release_identity --check
python -m tools.release_chain --stage contract
git verify-tag v0.4.13
python -m tools.release_chain --emit-source-evidence "$WORKSPACE/artifacts/source-release-manifest.json" \
  --observed-at "$RELEASE_OBSERVED_AT"
python -m tools.build_release_artifacts --out-dir ../artifacts/python \
  --source-date-epoch "$SOURCE_DATE_EPOCH"
```

`RELEASE_OBSERVED_AT` must be the real timezone-qualified observation timestamp;
`SOURCE_DATE_EPOCH` must come from the approved source evidence. Build the private
server wheel without uploading it to public CI artifacts:

```sh
cd "$WORKSPACE/spl-server"
SOURCE_DATE_EPOCH="$SOURCE_DATE_EPOCH" python -m build --wheel --outdir "$WORKSPACE/artifacts/server"
cd "$WORKSPACE/spl"
```
Use the exact Console source commit from the source manifest, the real build
time and the source epoch:

```sh
python -m tools.build_console_artifact --workspace-root "$WORKSPACE" \
  --frontend-root "$WORKSPACE/spl-frontend" --output-root "$WORKSPACE/artifacts" \
  --source-commit "$CONSOLE_COMMIT" --built-at "$RELEASE_OBSERVED_AT" \
  --source-date-epoch "$SOURCE_DATE_EPOCH"
```

`WORKSPACE` is the absolute approved release workspace path; `CONSOLE_COMMIT`
is its exact reviewed Console commit.
All Console deployable files are read from clean Git blobs. Regenerate and test
the archive after any final source change; never copy a source `build.json` into it.

```sh
python -m tools.release_chain --manifest "$WORKSPACE/artifacts/source-release-manifest.json" \
  --emit-built-evidence "$WORKSPACE/artifacts/built-release-manifest.json" \
  --component-artifact framework="$WORKSPACE/artifacts/python/splime-0.4.13-py3-none-any.whl" \
  --component-artifact daemon="$WORKSPACE/artifacts/python/splime-0.4.13-py3-none-any.whl" \
  --component-artifact server="$WORKSPACE/artifacts/server/spl_server-0.4.13-py3-none-any.whl" \
  --component-artifact console="$WORKSPACE/artifacts/splime-console-0.4.13.tar.gz" \
  --observed-at "$RELEASE_OBSERVED_AT"
```

The tool resolves relative component artifact paths against its workspace root;
use absolute artifact paths if executing from a different working directory.

## SDK publication, private server lock, Docker

Prerequisites: reviewed source and asset list, passing CI on Linux/macOS/Windows,
signed SDK tag, trusted release-signing public key in the workflow secret,
the existing PyPI trusted publisher for `publish-to-pypi.yml` and its protected
environment approval (configure TestPyPI separately if using it), and explicit permission
for package publication. Both modes use the existing workflow filename and publisher;
no additional PyPI publisher is needed. SDK mode has no private repository inputs
and exports only SDK artifacts and SDK-only evidence. Cookbook inputs are optional
for SDK mode and remain mandatory for coordinated verification. A GitHub release or tag
push does not publish PyPI.

```sh
gh workflow run publish-to-pypi.yml --repo yastrebovks/splime --ref main \
  -f release-tag=v0.4.13 -f release-scope=sdk -f target=verify
# Only after verification passes and SDK publication is authorized:
gh workflow run publish-to-pypi.yml --repo yastrebovks/splime --ref main \
  -f release-tag=v0.4.13 -f release-scope=sdk -f target=pypi
```

Verify real PyPI wheel/sdist URLs, downloaded byte hashes, clean installation,
runtime version and `pip check`. Do not substitute candidate hashes for observed
published hashes. Only then regenerate the private server's tracked index lock:

```sh
cd "$WORKSPACE/spl-server"
uv lock --no-sources --python 3.13
uv lock --check --python 3.13
uv run --locked --python 3.13 --extra test python -m pytest
```

The current 0.4.12 lock is preserved pending that external prerequisite.
Local candidate tests install the SDK and server wheel together using explicit
paths; no claim is made that unpublished 0.4.13 exists on PyPI. Review and commit
the final lock, create the server's `v0.4.13` source ref for the first time, and
finalize the Console ref. Run the source/build section above. Then run the
existing coordinated workflow in verification mode (do not republish the SDK):

```sh
gh workflow run publish-to-pypi.yml --repo yastrebovks/splime --ref main \
  -f release-tag=v0.4.13 -f release-scope=coordinated -f target=verify \
  -f cookbook-url="$COOKBOOK_URL" -f cookbook-sha256="$COOKBOOK_SHA256"
```

This stage additionally requires the reviewed cookbook HTTPS URL/hash and
configured private GitLab read credentials/pinned host keys. Public CI transfer
artifacts exclude the server wheel; its checksum remains bound in the coordinated
component manifest. Preserve the wheel through the private deployment channel.

Build and test `linux/amd64` and `linux/arm64` daemon images from published PyPI
bytes, using the existing `splime-builder`. Test non-root execution, persistent
home, authenticated health and SIGINT cleanup before an authorized push:

```sh
cd "$WORKSPACE/spl/deploy/dockerhub"
./publish.sh 0.4.13
docker buildx imagetools inspect yastrebovks/spl-daemon:0.4.13
```

This script **pushes** both the version and `latest`; it is an execution step.
Record the actual multi-platform index and both platform digests. Feed them,
observed PyPI artifacts, cookbook hash and the exact closed public GitHub asset
list to the release chain. Variables below must contain observed values, with
Docker digests in `sha256:<64 hex>` form. The BOM is the reviewed checksum file
for the public assets. Never fabricate digests or upload server code.

```sh
cd "$WORKSPACE/spl"
python -m tools.release_chain --manifest "$WORKSPACE/artifacts/built-release-manifest.json" \
  --emit-published-evidence "$WORKSPACE/artifacts/published-release-manifest.json" \
  --github-asset "source-release-manifest.json=$WORKSPACE/artifacts/source-release-manifest.json" \
  --github-asset "release-artifact-bom.sha256=$WORKSPACE/artifacts/release-artifact-bom.sha256" \
  --github-asset "splime-0.4.13-py3-none-any.whl=$WORKSPACE/artifacts/python/splime-0.4.13-py3-none-any.whl" \
  --github-asset "splime-0.4.13.tar.gz=$WORKSPACE/artifacts/python/splime-0.4.13.tar.gz" \
  --github-asset "splime-console-0.4.13.tar.gz=$WORKSPACE/artifacts/splime-console-0.4.13.tar.gz" \
  --github-asset "static-integrity.json=$WORKSPACE/artifacts/console/static-integrity.json" \
  --pypi-artifact "splime-0.4.13-py3-none-any.whl=$PYPI_WHEEL_URL=$PYPI_WHEEL_SHA256" \
  --pypi-artifact "splime-0.4.13.tar.gz=$PYPI_SDIST_URL=$PYPI_SDIST_SHA256" \
  --public-artifact "$COOKBOOK_URL=$COOKBOOK_SHA256" \
  --docker-manifest-digest "$DOCKER_INDEX_DIGEST" \
  --docker-platform-digest "linux/amd64=$DOCKER_AMD64_DIGEST" \
  --docker-platform-digest "linux/arm64=$DOCKER_ARM64_DIGEST" \
  --observed-at "$RELEASE_OBSERVED_AT"
```

Upload only the six explicit public assets above plus the generated published
manifest renamed `release-manifest.json`, through the reviewed GitHub release
workflow or the explicit command below. `PUBLIC_RELEASE_NOTES` must point to
reviewed public release notes, and the tag must already exist on GitHub.
The private server wheel is deliberately absent from this list.

```sh
cp "$WORKSPACE/artifacts/published-release-manifest.json" "$WORKSPACE/artifacts/release-manifest.json"
gh release create v0.4.13 --repo yastrebovks/splime --verify-tag \
  --title "Splime 0.4.13" --notes-file "$PUBLIC_RELEASE_NOTES" \
  "$WORKSPACE/artifacts/source-release-manifest.json" \
  "$WORKSPACE/artifacts/release-artifact-bom.sha256" \
  "$WORKSPACE/artifacts/python/splime-0.4.13-py3-none-any.whl" \
  "$WORKSPACE/artifacts/python/splime-0.4.13.tar.gz" \
  "$WORKSPACE/artifacts/splime-console-0.4.13.tar.gz" \
  "$WORKSPACE/artifacts/console/static-integrity.json" \
  "$WORKSPACE/artifacts/release-manifest.json"
```

## Deployment, migration and rollback

Deploy server first, Console and its static-serving configuration together next,
and daemons last. Back up the schema-48 SQLite database with SQLite's backup
API, including application state and signing-key backup under existing secret
controls, before the forward-only migration to schema 49. Migration 49 adds
catalogue sequence fences, resumable transfers, bounded parts and content cache;
existing complete machine snapshots remain intact. Partial content is never
used as the current catalogue. Abandoned staging expires after 24 hours via the
maintenance service and on transfer admission.

Use the official updater with real artifact and manifest hashes. `PUBLISHED_MANIFEST`
must be the exact bytes uploaded as `release-manifest.json`, and
`PUBLISHED_MANIFEST_SHA256` their SHA-256. The later production verifier binds
the deployment receipt to this final published manifest, not the earlier built
manifest.

```sh
cd "$WORKSPACE/spl-server"
./deploy/update-spl-server.sh deploy \
  --artifact "$PRIVATE_SERVER_WHEEL" --sha256 "$PRIVATE_SERVER_SHA256" \
  --evidence-manifest "$PUBLISHED_MANIFEST" --manifest-sha256 "$PUBLISHED_MANIFEST_SHA256" \
  --source-ref v0.4.13 --source-commit "$SERVER_COMMIT" \
  --service spl-daemon-server.service --home /var/lib/spl-daemon-server \
  --ready-url http://127.0.0.1:9876/ready
```

Do not run 0.4.12 server code against schema 49. Server rollback restores the
verified pre-migration backup and matching old artifacts together. Console
rollback atomically restores its complete archive and serving configuration;
never overwrite individual modules across builds. Its mutable deployment
identity must revalidate on reload. Keep authentication session continuity.

New daemons use v1 only when advertised. Old servers continue accepting small
legacy snapshots; oversized legacy catalogues show an actionable sync failure
while heartbeats continue. The server fences legacy snapshot replay after a
machine starts v1. A daemon downgrade for such a machine requires a coordinated
server/database rollback or an explicitly provisioned fresh machine identity;
it must not erase sequence fences to permit stale writes.

Post-deployment, inspect `/api/version` and `/api/ready` for version 0.4.13,
schema 49, exact manifest/deployment receipt binding, and negotiated capability.
Run `node "$WORKSPACE/spl-frontend/tools/production-sanity.mjs"`; run
the following verification against the actual published manifest and deployment.
`RELEASE_MANIFEST_SHA256` is its reviewed observed checksum; the workflow downloads
and checks it before testing public artifacts and all three platform installs.

```sh
cd "$WORKSPACE/spl"
python -m tools.verify_published_release \
  --manifest "$WORKSPACE/artifacts/published-release-manifest.json" \
  --source-repository "$WORKSPACE/spl" \
  --server-version-url https://splime.io/api/version \
  --server-ready-url https://splime.io/api/ready
gh workflow run verify-release-assets.yml --repo yastrebovks/splime --ref main \
  -f source-repository=yastrebovks/splime -f release-tag=v0.4.13 \
  -f release-manifest-url=https://github.com/yastrebovks/splime/releases/download/v0.4.13/release-manifest.json \
  -f release-manifest-sha256="$RELEASE_MANIFEST_SHA256" \
  -f server-version-url=https://splime.io/api/version \
  -f server-ready-url=https://splime.io/api/ready
```

Test retained large catalogue sync, a
restricted-token `whoami`, warm Console reload and recovery in production
without publishing new Hub artifacts unless separately authorized.
