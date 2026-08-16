# SPLime 0.4.7

SPLime 0.4.7 extends the local framework and integrated daemon without
changing the legacy Object, Pipeline, Run, or JSON call defaults.

## Highlights

- Per-Run input and output adapters for local and remote execution, including
  built-in JSON, text, binary, DataFrame, spreadsheet, and image transports.
- Independently versioned Library Adapters with owner/Library identity, exact
  version binding, synchronization, ACLs, dependency checks, and explicit
  local/remote custom-code policy.
- Semantic adapter compatibility is advisory: recommended choices are ranked
  first, while an explicit mismatch remains possible and is recorded in Run
  diagnostics.
- Object-wide and per-Function runtime selection supports mixed native,
  virtual-environment, and Docker execution in one Pipeline.
- Additive, authenticated IDE contracts for daemon metadata and lifecycle,
  guarded Runs, static source analysis/preparation, deterministic Preview, and
  central AI relay.
- Safer macOS temporary artifact staging and stricter exact-version remote
  adapter admission.

## Compatibility

- Omitting `adapters` preserves the established JSON request/result contract.
- Existing edge adapters, `.as_format(...)`, resume behavior, immutable Object
  identities, and local-only daemon operation remain supported.
- Clients and servers negotiate all new operations through capabilities.
  Unsupported new operations fail closed without changing legacy behavior.
- Server and Console 0.4.7 remain lockstep; daemon/server integration is
  capability-negotiated according to `release/compatibility-matrix.json`.

## Installation

```bash
python3.13 -m pip install --upgrade "splime==0.4.7"
```

The container image is published separately as
`yastrebovks/spl-daemon:0.4.7` after the exact PyPI package is observable.

See `CHANGELOG.md` for the complete history and compatibility notes.
