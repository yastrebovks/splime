# SPLime 0.4.9 release runbook

This corrective release publishes exactly one Python distribution: `splime`.
There is no separate Public worker project, wheel, dependency, or publication
step. Public Objects execute through the installed `splime` framework.

## Required order

1. Run the complete framework tests, lint, formatting, mypy, deterministic
   double-build, clean-wheel installation, cookbook, Case1, and Case2 gates.
2. Run `python tools/verify_published_compatibility_extension.py` to bind the
   published 0.4.8 wheel and sdist and compare its API with the 0.4.9 facade.
3. Review and commit the framework release from this directory, create the
   signed `v0.4.9` tag, and push it to the public GitHub repository.
4. Let the signed-tag workflow build, install-test, and publish the exact two
   artifacts to TestPyPI. After reviewing that run, create the GitHub release
   for `v0.4.9` (or explicitly dispatch the same workflow with target `pypi`)
   so trusted publishing uploads only `splime==0.4.9`; verify both production
   PyPI hashes and a clean installation.
5. Only after PyPI exposes `splime==0.4.9`, regenerate `spl-server/uv.lock`,
   rerun its full tests and typing-delta gate, then commit and deploy server
   0.4.9. Never substitute a path, VCS, or second package source in that lock.
6. Commit and deploy the matching Console 0.4.9 static tree, its regenerated
   `static-integrity.json`, and the landing/install pages pinned to 0.4.9; the
   server schema remains 48.
7. Perform anonymous and non-owner Public Hub execution checks against the
   exact production release before declaring the rollout complete.

## Owner release script

`release.sh` keeps every state-changing step separate and fail-closed. The
`check` and `artifacts` modes do not commit, tag, push, publish, or deploy:

```bash
./release/0.4.9/release.sh check
./release/0.4.9/release.sh artifacts
```

The script uses `.venv/bin/python` by default. Set
`SPLIME_RELEASE_TEST_PYTHON` only to select another reviewed Python 3.13 test
environment. `SPL_RELEASE_COOKBOOK_PATH` may select the reviewed public
cookbook; in this release workspace the script finds the canonical public
notebook automatically.

Run each owner-authorized transition separately:

```bash
SPLIME_RELEASE_CONFIRM=0.4.9 ./release/0.4.9/release.sh commit
SPLIME_CONFIRM_PUSH=0.4.9 ./release/0.4.9/release.sh push
SPLIME_CONFIRM_DRAFT=0.4.9 ./release/0.4.9/release.sh draft
SPLIME_CONFIRM_PYPI=0.4.9 ./release/0.4.9/release.sh publish
./release/0.4.9/release.sh verify
```

`commit` requires an interactive terminal and the exact versioned GPG key,
creates a signed commit and signed tag locally, and pushes nothing. `push` is
atomic and triggers only the TestPyPI path. `draft` first requires the exact
TestPyPI wheel and sdist. `publish` changes the reviewed GitHub draft to a
published release, which triggers trusted production-PyPI publishing. The
script never accepts a PyPI password or directly uploads with Twine.
