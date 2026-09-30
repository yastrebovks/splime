# SPLime 0.4.10 release runbook

This release publishes one Python distribution, `splime`, and coordinates the
exact `v0.4.10` framework, server, and Console sources through external release
evidence. Tracked manifests remain declaration-only; do not commit built or
published hashes back into a source repository.

## Required order

1. Run `./release/0.4.10/release.sh check` from the reviewed framework source.
2. Commit and push the matching server and Console changes, then create their
   exact `v0.4.10` refs. The source-evidence job resolves those refs to immutable
   commits and rejects dirty or mismatched checkouts.
3. Create the signed framework commit and signed `v0.4.10` tag, push them
   atomically, and run `dispatch-testpypi`. Review the complete source evidence,
   reproducible cross-repository BOM, three-platform wheel installs, and exact
   TestPyPI artifact hashes.
4. Run `dispatch-pypi` only after the TestPyPI run is accepted. The workflow
   publishes the same reviewed wheel and sdist and emits the exact PyPI handoff.
5. Publish the multi-architecture daemon image from `deploy/dockerhub`, bind its
   immutable digests, the public cookbook hash, PyPI URLs/hashes, and all GitHub
   asset bytes into an external `published` manifest.
6. Create the GitHub release with the exact seven declared assets plus the final
   external `release-manifest.json`. Its independent SHA-256 is the deployment
   input; the manifest never lists or hashes itself.
7. Regenerate the server lock against public `splime==0.4.10`, rerun the server
   gates, build its exact wheel, and deploy it with the evidence-gated updater.
   Deploy the Console archive built from its pinned commit and verify both
   `/api/version` and `/api/ready` before announcing the release.

The release script does not infer approval. Its local modes are read-only;
workflow dispatches require exact environment variables and use GitHub trusted
publishing rather than a local PyPI credential:

```bash
./release/0.4.10/release.sh check
./release/0.4.10/release.sh artifacts

export SPL_RELEASE_COOKBOOK_URL=https://splime.io/downloads/splime-cookbook.ipynb
export SPL_RELEASE_COOKBOOK_SHA256=<reviewed-64-hex-digest>
SPLIME_RELEASE_CONFIRM=0.4.10 ./release/0.4.10/release.sh dispatch-testpypi
SPLIME_RELEASE_CONFIRM=0.4.10 ./release/0.4.10/release.sh dispatch-pypi
```

`dispatch-pypi` is irreversible. It must not be run until the exact TestPyPI
workflow run and artifact bundle have been reviewed.
