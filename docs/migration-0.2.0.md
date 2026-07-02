# Migration plan: 0.1.x → 0.2.0

Status: **stage one shipped in 0.1.4.** Rows marked *(warns since 0.1.4)*
already emit `DeprecationWarning` while keeping the old behavior intact —
nothing breaks in 0.1.x. The actual removals (WP-07, the only breaking work
package) land in 0.2.0, no earlier than one release after the warning
appeared.

## Old → new (the table users will see)

| Old path (works in 0.1.x) | Canonical path | Status |
| --- | --- | --- |
| `client.create_library(...)` / `get_library` / `update_library` / `delete_library` | `client.library.create/get/update/delete(...)` | warns since 0.1.4 |
| `client.grant_library(...)` / `revoke_library_grant(...)` | `client.library.grant/revoke(...)` | warns since 0.1.4 |
| `client.add_reference(...)` / `client.copy_object(...)` / `client.remove_entry(...)` | `client.library.add_reference/copy_object/remove_entry(...)` | warns since 0.1.4 |
| `client.local_objects(...)` | `client.objects(scope='local')` | warns since 0.1.4 |
| `client.server_objects(...)` | `client.objects(scope='server', ...)` | warns since 0.1.4 |
| `client.start(...)` | `client.submit(...)` (same signature) | warns since 0.1.4 |
| `client.queue(...)` | `client.submit(..., offline_policy='queue')` | warns since 0.1.4 |
| `client.run_node(...)` / `client.run_node_result(...)` | `Deployment(client, p).run(...)` with `NodeRemote.locate(...)` | warns since 0.1.4 |
| `from spl.client import SPLClient` (deep import) | `from spl import SPLClient` | 0.2.0 (needs module moves) |
| `from spl.core.common import Deployment, lift` | `from spl import Deployment, lift` | 0.2.0 (needs module moves) |
| `from spl.core.entities.node import DEFAULT_PORT, InputPort, OutputPort` | `from spl import DEFAULT_PORT, InputPort, OutputPort` | 0.2.0 (needs module moves) |
| `from spl.core.entities.distribution import DDistribution` | `from spl import DDistribution` | 0.2.0 (needs module moves) |
| `NodeRemote(name=...)` / `NodeRemote(pipeline=..., function=...)` ctor forms | `NodeRemote.locate(...)` | 0.2.0 (ctor is still documented in 0.1.4 cookbooks) |
| `result.value['default']` in happy-path code | `result.output` | docs-only nudge; `.value` stays |
| `run(...)[node][DEFAULT_PORT]` | `Deployment(p).run(..., output='alias')` / `Run.value(alias)` | docs-only nudge |
| `version='TODO'` accepted as a version alias | `version='latest'` (drop the `'TODO'` sentinel) | 0.2.0 |

Notes on the "warns since 0.1.4" mechanics: the implementations moved into the
canonical entry points (`client.library.*`, `SPLClient._start_run`,
`SPLClient._run_node_value`), so canonical calls — including the internal
`Deployment` remote path — never emit the warning; only the legacy public
aliases do. Enforced by `tests/core/test_deprecations.py`.

## Open decision for 0.2.0

- Flip `RemoteResult.value` to unwrapped semantics and expose the raw port
  dict as `.raw_value`? (RFC WP-07 lists it as optional.) Default plan: keep
  `.value` raw forever, promote `.output` in all docs — less breakage.

## Checklist for executing WP-07 (in 0.2.0, not before)

1. Add `DeprecationWarning` to every "old path" row above.
2. Update `__all__` and the golden snapshot `tests/core/test_public_api.py`
   intentionally, in the same PR.
3. Bump `version = "0.2.0"`, link this document from the release notes.
4. Keep warnings for at least one release before any removal.
