# Splime 0.4.13 implementation and acceptance history

All work belongs to `splime-0.4.13`. Production, package registries, Docker
registries, and the public Hub remain unchanged during preparation.

Inspection on 2026-10-13 found three active component repositories (`spl`,
`spl-server`, `spl-frontend`) plus the private landing repository. The legacy
`spl-daemon` checkout contains unrelated work and is excluded. Existing SDK
documentation modifications are preserved. Historical releases and tags are
inputs, never regenerated evidence.

Before the initial implementation, the daemon already avoided reading YAML when the catalogue fingerprint
is unchanged. A changed catalogue still forms one ephemeral event, and size
validation occurs before pending events are admitted. SDK `whoami()` calls the
general user directory. Console snapshots already have TTL caching, bounded
concurrency, session generation cancellation, and visibility-aware polling;
the initial HTML is empty and static verification is serial with `no-store`.

1. Add capability-negotiated `catalogue_manifest_v1`, bounded object-content
   parts, durable server staging, hash/completeness checks, and atomic current
   snapshot replacement. Fence owner, machine, connection and generation;
   reuse accepted content and retain small legacy snapshots for old servers.
2. Separate catalogue/event failure diagnostics from network liveness. Keep
   heartbeats and independent queued work progressing, with permanent failures
   retained for inspection instead of acknowledged or retried forever.
3. Add owner profile fields to `/whoami` and consume only that self-identity
   endpoint. Keep safe older-server/offline behavior and credential isolation.
4. Measure Console startup, render an inline branded bounded startup shell,
   verify static bytes concurrently, execute the verified dependency graph,
   and scope/coalesce API refreshes without weakening authentication checks.
5. Run focused transport, persistence, identity, browser and existing suites;
   build and install candidate wheels in dedicated environments and exercise
   an isolated author daemon, registry and anonymous consumers over real HTTP.
6. Generate release declarations, preserve private artifact boundaries, build
   the final Console archive, record exact candidate checksums, and document
   signed-source/publication/deployment prerequisites separately from tests.
7. Review technical correctness and end-to-end acceptance separately. Record
   unexecuted platform and external gates explicitly; no inferred passes.

The initial evidence root is `artifacts/candidate-0.4.13-20261013` in the parent workspace.
All test environments, homes, databases, logs and servers live under dedicated
paths and loopback ports. No shared daemon or Jupyter kernel is reconfigured.

## Review-driven replacement candidate

The independent review reproduced two failures despite the initial green suites:
an unusable HTTP `/sync` response could permanently retire valid outgoing events,
and stale commercial-revision recovery could reuse an unexpired catalogue cache.
The replacement work adds regressions against actual HTTP bytes and the actual
Console API/snapshot/command integration before changing those implementations.

The replacement evidence root is
`artifacts/candidate-0.4.13-20261013-review-fixes`. Initial `finalv5` SDK/server
artifacts and the `finalv4` Console remain historical, with their original logs
and checksums preserved. The replacement inventory and `ACCEPTANCE.md` identify
the selected artifacts, current results and remaining external gates. The
release identity stays `splime-0.4.13`; no source tag or publication is inferred
from local validation.

## Diagnostic metadata correction

A later review confirmed both preceding fixes and found an unguarded set
membership check on upstream `category`: a JSON array/object could escape
receipt normalization, block an independent valid acknowledgement and make the
real heartbeat loop report a healthy server as unreachable.

The current evidence root is
`artifacts/candidate-0.4.13-20261013-diagnostic-metadata`. This revision validates
known optional diagnostic field types per receipt, keeps unknown string values
forward-compatible and makes diagnostic normalization type-safe and bounded.
Confirmed HTTP authentication and local transport/response evidence take
precedence over optional body hints. New actual-HTTP heartbeat regressions are
added to the existing installed-wheel/CI suite before applying the fix.

The SDK wheel and sdist are rebuilt. Unchanged server and Console production
inputs are independently checked, with exact existing artifacts retained.
Previous Console/browser results are explicitly historical; new SDK, server,
installed-package and real-HTTP/Case5 results belong to this revision. The
current acceptance report and inventory distinguish those evidence classes.
