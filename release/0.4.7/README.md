# SPLime 0.4.7 release runbook

This directory contains the owner-operated release controls. Nothing here
publishes by default.

## Required order

1. From the repository root, run `release/0.4.7/release.sh check`.
   Run `release/0.4.7/update-docker.sh sources` as the pre-publication Docker
   source/layout check; it does not pull, build, or publish an image.
2. Review `git diff`, `git status`, `CHANGELOG.md`, and the generated artifacts
   under `dist/release-0.4.7/`.
3. Export `SPLIME_RELEASE_CONFIRM=0.4.7` and run
   `release/0.4.7/release.sh commit` to create one signed commit and the signed
   `v0.4.7` tag. This does not contact GitHub.
4. Run `release/0.4.7/release.sh push`. The atomic push triggers the existing
   GitHub workflow for TestPyPI.
5. After that workflow is green, run `release/0.4.7/release.sh draft` to create
   a draft GitHub Release with the exact local wheel, sdist, checksums, release
   notes, and declaration manifest.
6. Review the draft. Publishing it also triggers the trusted PyPI environment.
   Export `SPLIME_CONFIRM_PYPI=0.4.7` and run
   `release/0.4.7/release.sh publish` only when that side effect is intended.
7. After PyPI 0.4.7 is observable, run
   `release/0.4.7/update-docker.sh check` and
   `release/0.4.7/update-docker.sh build`, inspect the smoke result, then export
   `SPLIME_CONFIRM_DOCKER=yastrebovks/spl-daemon:0.4.7` and run
   `release/0.4.7/update-docker.sh push`. Finish with the independent
   `release/0.4.7/update-docker.sh verify` command.

## Trust boundaries

- GitHub publication uses remote `github`, branch `main`, and a tag signed by
  fingerprint `31E24377474710AF950C81C6B8C5D1937087FA85`.
- Pushing `v0.4.7` publishes to TestPyPI through GitHub OIDC.
- Publishing the GitHub Release publishes to PyPI through GitHub OIDC.
- The Docker image installs the already-published exact PyPI version; it never
  copies the dirty checkout or local credentials into the image.
- The Docker gate binds the exact published wheel and sdist hashes, the signed
  package-release commit, the separately committed Docker packaging revision,
  pinned multi-arch base-image digests, and tested `uv` version.
- Docker `0.4` and `latest` are moved only by the explicit `push` command.

## Operator prerequisites

- Python 3.13 and the repository development environment for the local gates.
  The release script creates an ignored, isolated build/twine environment.
- The secret GPG key matching fingerprint
  `31E24377474710AF950C81C6B8C5D1937087FA85` for `commit`.
- An authenticated GitHub CLI (`gh auth login`) for `draft` and `publish`.
- Docker with Buildx and an authenticated Docker Hub session for image
  publication. The exact PyPI 0.4.7 artifacts must already be observable.

## Deliberate merge decision

The public repository's package-only PyPI workflow and its matching regression
test are retained. The older multi-repository workflow from the development
tree and its matching regression test are preserved byte-for-byte under
`archived-development-release-controls/` for audit, but are not active: they
would reintroduce cross-repository credentials and gates into the public
package publication path.
