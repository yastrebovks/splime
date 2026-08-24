# SPLime 0.4.8

SPLime 0.4.8 adds Public Objects through additive, capability-negotiated
surfaces while preserving the established framework, daemon, Library,
Adapter, Run, lifecycle, sync and private `NodeRemote` behavior.

## Highlights

- Revision-aware Object and Library Adapter Descriptions; description-only
  edits do not create a code version or change immutable release identity.
- Strict public eligibility and atomic activation of signed, content-addressed
  Function, Pipeline and Library Adapter releases. Public dependency closure
  rejects private components and `NodeRemote` without changing their private
  execution paths.
- `SPLClient.embedded()` resolves, verifies and executes exact public releases
  locally without a daemon, machine registration or user account. Exact cached
  releases support explicit-trust offline reuse.
- Origin-bound Ed25519 verification for the official `https://splime.io`
  registry and a deterministic, dependency-minimal public worker.
- Historical Python API facades restored and verified against every actually
  published SPLime artifact from 0.1.2 through 0.4.7.

## Compatibility

- Existing `SPLClient()` remains daemon-backed; existing call, submit, Run,
  Library, Adapter, YAML/IR and private `NodeRemote` contracts are unchanged.
- New profile/public operations are sent only after capability negotiation and
  fail with `feature_not_supported` before mutation on older components.
- The recorded compatibility evidence covers 18 published versions and 36
  independently hash-verified wheel/sdist artifacts. The raw evidence is under
  `release/0.4.8/`.
- Server and Console 0.4.8 remain lockstep at server schema 48; daemon/server
  integration remains capability-negotiated.

## Installation

```bash
python3.13 -m pip install --upgrade "splime==0.4.8"
```

The Docker image is a separate post-PyPI release. Its release-control commit
must bind the signed package commit, exact published wheel/sdist hashes and
the final Docker packaging revision before any image build or push.

See `CHANGELOG.md` for the complete history and compatibility notes.
