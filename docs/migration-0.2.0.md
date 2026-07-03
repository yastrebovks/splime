# Migration plan: 0.1.x → 0.2.0

Status: **executed in 0.2.0 (WP-07b, 2026-07-03).** The aliases that had
warned since 0.1.4 are now removed; the deep-import locations and the
convenience `NodeRemote` constructor forms keep working but emit
`DeprecationWarning` and will be removed in 0.3.0.

## Old → new (the table users will see)

| Old path (worked in 0.1.x) | Canonical path | Status |
| --- | --- | --- |
| `client.create_library(...)` / `get_library` / `update_library` / `delete_library` | `client.library.create/get/update/delete(...)` | warned 0.1.4–0.1.5; **removed in 0.2.0** |
| `client.grant_library(...)` / `revoke_library_grant(...)` | `client.library.grant/revoke(...)` | warned 0.1.4–0.1.5; **removed in 0.2.0** |
| `client.add_reference(...)` / `client.copy_object(...)` / `client.remove_entry(...)` | `client.library.add_reference/copy_object/remove_entry(...)` | warned 0.1.4–0.1.5; **removed in 0.2.0** |
| `client.local_objects(...)` | `client.objects(scope='local')` | warned 0.1.4–0.1.5; **removed in 0.2.0** |
| `client.server_objects(...)` | `client.objects(scope='server', ...)` | warned 0.1.4–0.1.5; **removed in 0.2.0** |
| `client.start(...)` | `client.submit(...)` (same signature) | warned 0.1.4–0.1.5; **removed in 0.2.0** |
| `client.queue(...)` | `client.submit(..., offline_policy='queue')` | warned 0.1.4–0.1.5; **removed in 0.2.0** |
| `client.run_node(...)` / `client.run_node_result(...)` | `Deployment(client, p).run(...)` with `NodeRemote.locate(...)` | warned 0.1.4–0.1.5; **removed in 0.2.0** |
| `from spl.client import SPLClient` (deep import) | `from spl import SPLClient` | **warns since 0.2.0** (shim module; removal in 0.3.0) |
| `from spl.core.common import Deployment, lift` | `from spl import Deployment, lift` | **warns since 0.2.0** (shim module; removal in 0.3.0) |
| `from spl.core.entities.node import DEFAULT_PORT, InputPort, OutputPort` | `from spl import DEFAULT_PORT, InputPort, OutputPort` | still silent; module move deferred to 0.3.0 (entities are cross-imported by the whole core — see note below) |
| `from spl.core.entities.distribution import DDistribution` | `from spl import DDistribution` | still silent; module move deferred to 0.3.0 (same note) |
| `NodeRemote(name=...)` positional-url form / `NodeRemote(pipeline=..., function=...)` ctor forms | `NodeRemote.locate(...)` | **warns since 0.2.0**; the plain serialization constructor (`url=..., name=..., inputs=..., outputs=...`) stays silent |
| `result.value['default']` in happy-path code | `result.output` | resolved: `.value` stays raw forever; `.output` is the documented reading |
| `run(...)[node][DEFAULT_PORT]` | `Deployment(p).run(..., output='alias')` / `Run.value(alias)` | docs-only nudge |
| `version='TODO'` accepted as a version alias | `version='latest'` | **dropped in 0.2.0** (both SDK and daemon alias sets) |

Implementation notes:

* the implementations live in `spl/_client.py` and `spl/core/_common.py`;
  `spl/client.py` and `spl/core/common.py` are import-time warning shims that
  re-export everything (plus a `__getattr__` fallback), so legacy code keeps
  working unchanged until 0.3.0;
* `spl/_deprecate.py` is the single warning helper (`warn_deprecated`,
  `warn_deprecated_import`);
* the `spl.core.entities.node`/`.distribution` moves were deliberately
  deferred: the entities are imported by 15+/7+ modules including the IR and
  the daemon worker, and the facade already exports every promised name, so
  the shim benefit did not justify the churn in 0.2.0;
* enforced by `tests/core/test_deprecations.py` (removals pinned, shims warn,
  canonical paths silent).

## Checklist for executing WP-07 (done in 0.2.0)

1. ~~Add `DeprecationWarning` to every "old path" row above.~~ Done for the
   0.2.0 rows; entities deep-import rows are deferred to 0.3.0.
2. ~~Update `__all__` and the golden snapshot~~ — `spl.__all__` did not shrink
   (only `SPLClient` methods were removed), the snapshot stayed intact.
3. ~~Bump `version = "0.2.0"`~~ — done; link this document from the release
   notes (release stage #42).
4. Keep the 0.2.0 warnings (shims, `NodeRemote` ctor forms) for at least one
   release before removal in 0.3.0.
