# SPLime 0.4.8 release runbook

This directory contains owner-operated controls. Nothing here publishes by
default, and the framework release remains separate from the later Docker
image release.

## Required order

1. Export `SPL_RELEASE_COOKBOOK_PATH` with the absolute path to the reviewed
   canonical cookbook, then run `release/0.4.8/release.sh check` from the
   repository root.
2. Review the complete diff, `git status`, `CHANGELOG.md`, compatibility
   evidence, and deterministic artifacts under `dist/release-0.4.8/`.
3. Export `SPLIME_RELEASE_CONFIRM=0.4.8` and run
   `release/0.4.8/release.sh commit` to create the signed commit and signed
   `v0.4.8` tag locally. This does not contact GitHub.
4. Run `release/0.4.8/release.sh push`. The atomic branch/tag push triggers the
   package-only GitHub workflow for TestPyPI.
5. After that workflow is green, run `release/0.4.8/release.sh draft`, inspect
   the draft GitHub Release and its exact wheel, sdist and checksum inventory.
6. Export `SPLIME_CONFIRM_PYPI=0.4.8` and run
   `release/0.4.8/release.sh publish` only when production PyPI publication is
   intended.
7. After exact PyPI 0.4.8 artifacts are observable, create and review a
   separate Docker release-control commit based on the hardened 0.4.7 control.
   It must replace the deliberate `SPL_PACKAGE_REVISION=unpublished` marker,
   pin both PyPI hashes and the Docker packaging revision, and pass multi-arch
   build/smoke verification before Docker Hub publication.

## Trust boundaries

- GitHub publication uses remote `github`, branch `main`, and a tag signed by
  fingerprint `31E24377474710AF950C81C6B8C5D1937087FA85`.
- The active PyPI workflow remains the public repository's package-only,
  signed-tag trusted-publishing workflow. It does not require private sibling
  repository credentials.
- `release.sh check` requires the reviewed cookbook and runs lint, formatting,
  strict typing, the complete non-smoke suite, deterministic double-build,
  Twine validation and an isolated wheel install.
- The official public registry key is a public trust anchor. The production
  signing private key is never stored in this repository or read by these
  controls.
- Docker publication is intentionally unavailable in this preparation tree;
  `deploy/dockerhub/publish.sh` fails closed until the separate exact 0.4.8
  Docker control exists.

## Operator prerequisites

- Python 3.13 and the repository's locked development environment.
- The reviewed canonical cookbook file.
- The secret GPG key matching the fingerprint above for the owner-only signed
  commit/tag step.
- An authenticated GitHub CLI only for the owner-only draft/publish steps.

No commit, tag, push, package publication or image publication is performed by
the preparation or check commands.
