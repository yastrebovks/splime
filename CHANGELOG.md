# Changelog

All notable changes to the `splime` package are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-07-09

Preparation release for multilingual pipelines, implemented on Python only:
adapters split into save/load halves with artifact tags, runs become persistent
data with manifests and resume, and runtime becomes a per-node property. The
public API and 0.2.x/0.3.x YAML stay compatible; the only intended default
change is `keep="on_failure"` for local runs.

### Added

- Save/load adapter halves behind the unchanged public `Adapter` facade;
  artifact refs carry an additive `tag`; mismatched halves fail loudly on the
  tag comparison before any bytes are read; additive `!DSaveAdapter` and
  `!DLoadAdapter` YAML forms.
- Built-in `json` default edge adapter with a four-level resolution hierarchy
  (port default -> pipeline -> edge -> run override) that records the winning
  source level per edge; the JSON-native inline short-circuit stays byte-
  identical (ADR-002).
- Run-level adapter overrides keyed by `(alias, port)` on `run()` and
  `resume()` — no node republish needed.
- Static save/load tag compatibility warnings at pipeline build and daemon
  registration, plus an optional `example` probe via
  `spl-daemon doctor --pipeline`.
- Versioned run manifests with deterministic node fingerprints (single core
  fingerprint module); retained state lives under `SPL_RUNS_HOME` /
  `<daemon_home>/runs` with owner-only permissions.
- Resume from a recalculation set `S`: selected nodes plus descendants
  recompute, everything else is frozen and digest-validated; every resume
  creates a new run with `parent_run_id` lineage.
- Run management: `spl-daemon run-list/run-show/run-prune`, daemon
  `POST /runs/<id>/resume`, `DELETE /runs/<id>`, `/runs/prune`,
  `/runs/tag-stats`; client `runs()/run_show()/resume()/prune_runs()` with
  local variants; 7-day retention TTL for kept failures.
- Per-node runtime tags (`native`, `venv-subprocess`, reserved `docker`) with
  adapter-style resolution recorded in manifests and run progress;
  `venv-subprocess` executes function nodes through the SPL-free runner and
  honors `runtime_config["node_timeout_seconds"]`.
- Local edge-tag statistics aggregated from retained manifests.
- Converter-node recipe in the cookbook and a bilingual
  fail -> change -> continue demo notebook.
- Server-connection quality: technical machine display names are reconciled
  with server-side token names, and the daemon client exposes `list_tokens()`.

### Changed

- `keep="on_failure"` is the new default for local runs; successful default
  runs still clean up (manifest materialization is deferred for successful
  runs per ADR-003), failed runs retain state with a TTL.
- Retained state is redacted by default: `run-show` summarizes inline values
  unless full output is explicitly requested.
- Reserved `run()`/`resume()` parameter names emit a warning when they collide
  with free pipeline input ports.

### Fixed

- Daemon-side `venv-subprocess` nodes: functions restored from `object.yaml`
  no longer break module generation (YAML `inspect.getsource` results fall
  back to the IR function body).
- Non-JSON inputs into `venv-subprocess` fail fast with a clear error naming
  the node and port instead of a raw serialization traceback.
- Adapter compatibility warnings deduplicate by content, not object identity.

## [0.3.0] - 2026-07-08

Portability and correctness release for environment resolution, worker
execution, release gates, and public-surface checks. No breaking changes to the
public API, HTTP contracts, database schema, `object.yaml` format, or run-dir
file protocol; new run-report fields are additive.

### Added

- Local interpreter resolution by environment name for server-origin objects;
  `env_python` is provenance, not an execution authority. Substitutions are
  logged, exposed in run state/progress, and reported by doctor on minor-version
  mismatch.
- SPL-free execution for supported functional nodes: the daemon generates a
  flat Python module and runs it with a stdlib-only runner; pipelines, async,
  decorated, and spl-importing functions stay on the legacy worker with an
  explicit `worker_runtime` marker.
- Environment builds prefer `uv venv --relocatable` + `uv pip install --strict`
  with a transparent pip fallback; the selected builder is recorded in build
  records, `install.log`, and the environment spec hash.
- Parameterized release gates: `Release/release.sh VERSION` and
  `tools/bump_version.py` centralize version sweeps and pre-publish checks.

### Changed

- Daemon and server signature contracts pin provenance semantics for `env`,
  `env_python`, and `env_python_version`.
- Python HTTP clients get default timeouts (60 s control, 300 s streaming);
  calls that block until a run finishes honor the caller's `timeout_seconds`
  and are exempt from the default cap.
- Enrollment accepts only `http`/`https` URLs with a non-empty host.
- Public examples and installers use the canonical `from spl import SPLClient`.

### Fixed

- Deduplicated synced versions no longer relink a non-null `remote_version_id`;
  collisions are logged.
- Shim/facade re-exports are locked by parameterized public-API tests.
- Landing checker validates public notebook markers and Docker deployment pins
  against the package version.

## [0.2.5] - 2026-07-07

Presentation and pipeline-output release. No breaking changes: the new view
types subclass `dict`/`list`, so code that indexes, iterates, calls `.get()`,
compares with plain containers, or serializes with `json.dumps` is unaffected.

### Added

- Compact notebook/terminal views for service-shaped payloads (new internal
  `spl/_views.py`). Object listings, run records, signatures, decompositions,
  inputs/outputs, artifact and event lists now render as bounded, human-readable
  tables in the terminal (`__repr__`) and in Jupyter (`_repr_html_`) instead of
  dumping raw JSON. Each view is a thin `dict`/`list` subclass, so the
  underlying data and its programmatic access are unchanged.
- `PipelineGraphWidget` prints a one-line summary (title, node/link/port counts)
  instead of its graph JSON.

### Changed

- Pipeline results are normalized into typed artifacts on the daemon worker.
  Values the JSON protocol cannot carry are materialized through registered type
  adapters, walking nested mappings/sequences with stable, de-duplicated artifact
  names. Values with no adapter raise a clear `TypeError` naming the path and type
  and pointing to `add_adapter(...)`. Explicit `__spl_artifacts__` declarations are
  copied through as before.

### Tests

- Seven new tests: four for the presentation views and three integration cases
  for the worker's result normalization and artifact naming.

## [0.2.4] - 2026-07-05

Library-governance release for the client SDK and daemon; the client-side
counterpart to the server's `libraries:write` scope. No breaking changes.

### Changed

- Central-library admin operations (create/update/delete, grants, references,
  copies, entry removal) authenticate with the user token as
  `Authorization: Bearer <user_token>`; reads use the user token when present and
  fall back to the machine token. With no user token, the client fails fast with a
  clear `401` instead of sending a request the server would reject.

### Fixed

- Deleting a whole central-server library now fails clearly instead of calling a
  missing endpoint: `client.library.delete()` raises `NotImplementedError`
  (pointing to the Console archive action or `client.library.remove_entry()`), and
  the daemon route `DELETE /server/libraries/<ref>` returns `501 Not Implemented`.

## [0.2.3] - 2026-07-04

Bugfix release. No API changes.

### Fixed

- Publishing a function defined inside an `if`/`with`/another function no longer
  fails with `IndentationError`: the source is dedented before `ast.parse` in both
  places that read it (`serialize_function` and the IR parser entry).

## [0.2.2] - 2026-07-04

Hotfix release. Version 0.2.1 was prepared but never published; its fixes ship
here. No breaking API changes.

### Fixed

- Nodes pulled from a server run on machines with a different Python layout: the
  daemon resolves a usable interpreter for server-origin objects (stored path →
  same-named env → local `default` → the daemon's own interpreter) and repairs a
  local env whose interpreter has disappeared.
- TLS certificate verification is consistent across host Python installs — all
  HTTPS calls to the central server (including streaming artifact uploads) verify
  against the bundled `certifi` CA store. New pinned runtime dependency: `certifi`.

### Changed (from the unpublished 0.2.1)

- README "Project status" no longer pins a hardcoded version number.
- `sphinx-build -W` no longer fails on a duplicate `NodeRemote` object description.

## [0.2.0] - 2026-07-03

- Public API cleanup (WP-07b): implementation moved behind `spl` facade with
  warning shims, deprecated 0.1.4 aliases removed.
- Run progress and `doctor` hardening, docs regeneration, canonical test suite.

## [0.1.5] - 2026-07-03

- Fixed scoped signature lookup.

## [0.1.4] - 2026-07-02

- Object identity and reconcile; API reorganization (facade, receipts, views,
  deprecations); cookbook smoke test.

## [0.1.3] - 2026-07-01

- `register_env` defaults to the daemon interpreter (seamless native and
  container workflows).

## [0.1.2] - 2026-06-30

- Early pre-release packaging.

## [0.1.0] - 2026-06-29

- Initial release: turn trusted Python functions into versioned, portable nodes
  reusable across projects and executed locally or remotely.

[0.3.0]: https://github.com/yastrebovks/splime/compare/v0.2.5...v0.3.0
[0.2.5]: https://github.com/yastrebovks/splime/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/yastrebovks/splime/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/yastrebovks/splime/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/yastrebovks/splime/compare/v0.2.0...v0.2.2
[0.2.0]: https://github.com/yastrebovks/splime/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/yastrebovks/splime/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/yastrebovks/splime/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/yastrebovks/splime/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/yastrebovks/splime/compare/v0.1.0...v0.1.2
[0.1.0]: https://github.com/yastrebovks/splime/releases/tag/v0.1.0
