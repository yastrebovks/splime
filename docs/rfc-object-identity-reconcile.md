# RFC: Canonical object identity & daemon reconcile

Status: draft
Scope of change: `spl/` only (the umbrella package). `Release/spl` is a deploy mirror of a finished, tested package and MUST NOT be edited during implementation.

## 1. Summary

The daemon can hold two separate registry rows that both denote the same logical
object (e.g. `order_pipeline`): the locally-authored object and a mirror of the
server's copy. A later `publish` correctly bumps the version of the local row but
never reconciles it with the mirror, and name resolution then fails with
`object display name is ambiguous locally`. This RFC defines a single, canonical
identity model so this cannot happen again, for **any** object kind (function,
pipeline, and any future kind) and for the content they carry (adapters,
distributions, nodes, ports, artifacts).

The rule, in one line: **an object's identity is `(owner, library, name)`; a
version's identity is the content hash of its canonical definition; a daemon is a
cache/working-copy of that canonical namespace.**

## 2. Problem (observed)

Reproduction (native daemon, previously connected, now offline):

- `client.publish(p, name='order_pipeline', ...)` succeeds and returns
  `.../objects/order_pipeline/versions/6.yaml` — i.e. it appended **version 6** to
  the local-authored object. Versioning works.
- `client.describe('order_pipeline')` / `client.call('order_pipeline', ...)` raise:
  `400: object display name is ambiguous locally: order_pipeline; use one of:
  order_pipeline, server.45cbce4188aa492092b6c7a2eac3e952`.
- The daemon has **no active server connection** (`client.machines()` →
  `404: active server connection is not found`). So resolution is a pure local
  SQLite lookup; the collision is baked into the local store and persists offline.

Evidence in code:

- Local resolver `EnvRepository`/`ObjectRepository.get_object` runs
  `WHERE o.name = ? OR o.id = ? OR o.remote_name = ?` and raises on `len(row) > 1`
  (`spl/src/spl/daemon/repositories/object.py` ~684-709, ~773-793).
- Local publish matches an existing object by `remote_object_id` (if given) else
  by local `name` — never by `remote_name`
  (`object.py` ~123-136), then appends `MAX(version)+1` (~185-195).
- A mirrored server object is stored under the synthetic local name
  `server.<remote_object_id>` with the display name kept in `remote_name`
  (`spl/src/spl/daemon/server.py` `_server_object_local_name` ~963; import ~914-945).

## 3. Root cause

Two identity models that do not align:

| Layer  | Object identity key                         | Version identity |
|--------|---------------------------------------------|------------------|
| Server | `UNIQUE(owner_id, library_id, name)` (`spl-server/src/daemon_server/store.py:289`) | `UNIQUE(object_id, version)` (`:310`) |
| Daemon | `objects.name UNIQUE` — **flat, single namespace** (`spl/src/spl/daemon/storage_base.py:193`) | `MAX(version)+1` |

Because the daemon's local `name` is globally unique, it **cannot** store two
objects that share a display name (my `order_pipeline`, alice's `order_pipeline`,
the server mirror of `order_pipeline`). To sidestep the `UNIQUE(name)` constraint
it renames the mirror to `server.<id>` and keeps the real name in `remote_name`.
Then:

1. `publish` keys by `name`, so it can only version the row literally named
   `order_pipeline`; it never recognizes the `server.<id>` mirror as the same
   object → the two rows stay forked.
2. `get_object` keys by `name OR remote_name`, so a plain name lookup matches
   both rows → ambiguous, with **no tie-break / priority** to pick one.

The daemon never reconciles a local object with its canonical server identity, and
the resolver refuses to choose instead of preferring the caller's own object.

## 4. Goals & non-goals

Goals:

- One canonical identity per object, identical on daemon and server:
  `(owner, library, name, kind)` where `kind` is a stable attribute (see 6.1).
- Republishing the same name is always a new **version** of the same object
  (never a fork, never an overwrite of history). Identical content is idempotent.
- A plain name lookup for the caller's own object is **always** unambiguous.
- Switching or reconnecting daemons is **seamless**: the same object appears with
  one linear-per-owner version history, as if on a single machine.
- The model is **kind-agnostic**: functions, pipelines, and future kinds obey the
  same rules; adapters/distributions/nodes are content **inside** a version and are
  covered by content addressing.
- The daemon works fully **offline** on its local cache, including cleanup.

Non-goals (this RFC):

- Changing the server's identity model (it is already correct).
- Multi-writer real-time conflict resolution beyond deterministic content-hash
  convergence + explicit conflict surfacing.
- Editing `Release/spl`, `spl-core`, `spl-daemon`, `Reserve`, `MVP` (frozen).

## 5. Invariants (the contract — every one must be test-enforced)

- I1. `(owner, library, name)` uniquely identifies an object locally and remotely;
  `kind` is fixed at first version and cannot change.
- I2. A version's identity is `content_hash` = SHA-256 over the object's canonical
  serialized definition (the full SPL/YAML incl. adapters, distributions, nodes,
  ports, entrypoint) plus the fields that affect execution semantics (env spec,
  runtime config). Two publishes with equal `content_hash` under the same object
  are the **same** version (idempotent); a differing hash is the **next** version.
- I3. `publish(name=X)` for the caller's own `(owner, library)` is find-or-create
  by canonical key + append/dedup version; it never creates a second object for X
  and never rewrites an existing version.
- I4. Resolving a plain `name` for the caller's own `(owner, library)` returns a
  single object deterministically. Cross-owner/library access requires explicit
  `owner=`/`library=`/scoped-URL; only that can be ambiguous, and even then
  resolution follows a documented priority, never a bare `ValueError` for the
  common case.
- I5. On (re)connect, a local object and the server object with the same
  `(owner, library, name)` are linked into one; version histories merge by
  `content_hash` (equal content ⇒ one version). Genuinely divergent content under
  the same key is surfaced as an explicit **conflict**, never a silent duplicate.
- I6. No user-facing identifier is ever the synthetic `server.<hex>` form. Mirrors
  are stored under their real `(owner, library, name)` with an `origin` flag.
- I7. Every mutation of the object/version tables is a single transaction; a failed
  publish/reconcile leaves the store unchanged (no partial rows).
- I8. Offline `forget/remove` prunes a local object (or a single version, or a
  stale mirror) without any server round-trip.
- I9. A one-time healing migration converges any existing corrupt store (a
  local-authored row + a `server.<id>` mirror sharing a canonical key) into one
  object, deduping versions by `content_hash`, and is idempotent + reversible via a
  pre-migration backup.

## 6. Design

### 6.1 Canonical object identity (kind-agnostic)

Replace the daemon's flat `objects.name UNIQUE` with the server's key. Local
`objects` gains an owner/library dimension:

- `owner_id TEXT NOT NULL` — the connected user's id when known; a stable local
  placeholder (e.g. `local`) when the daemon has never been paired. On first
  successful connect, local placeholder objects are rekeyed to the real owner
  (see 6.5).
- `library TEXT NOT NULL DEFAULT 'default'` — the target library slug.
- `UNIQUE(owner_id, library, name)` replaces `UNIQUE(name)`.
- `origin` stays (`local` | `server`); `source_object_name` supersedes the
  deprecated `remote_name` (a migration is already in flight, see 8) and is NOT
  used as a resolution key anymore (it becomes descriptive only).
- `kind` remains stable-once-set. `(owner, library, name)` does not include `kind`;
  a name is one object of one kind. Publishing a different kind under an existing
  `(owner, library, name)` is an error (as today, `object.py` ~149-157), not a new
  object.

Mirrors of server objects are stored under their **real** `(owner, library, name)`
with `origin='server'` and the server linkage fields (`remote_owner_id`,
`remote_object_id`, `remote_version_id`). The `server.<hex>` naming scheme is
removed entirely.

### 6.2 Content-addressed versions

Reuse the existing `yaml_sha256` (`object.py:104`) but define version identity
precisely and completely:

- `content_hash = sha256(canonical_bytes)` where `canonical_bytes` is a **stable,
  canonical** serialization of the object definition: sorted keys, normalized
  whitespace, and inclusion of everything that changes execution — nodes, ports,
  entrypoint, adapters (`Adapter` key/format/save/load identity + declared
  `distributions`), scalars, links, and the resolved env/runtime spec. Provide a
  single `canonicalize(object_def) -> bytes` used by BOTH publish and reconcile so
  the hash is identical across daemons.
- Per object, `object_versions UNIQUE(object_id, content_hash)`; versions carry a
  monotonically increasing integer `version` for human ordering.
- `publish`: if `content_hash` already exists for the object → return that version
  (idempotent, no new row); else insert `version = MAX(version)+1` and set it
  current.

This makes republishing identical content a no-op, and makes "the same pipeline
published from two daemons" converge to one version instead of forking (I2, I5).

### 6.3 Publish / register = reconcile by canonical key

`register_object` becomes: resolve-or-create the object row by
`(owner, library, name)` for the caller (owner from the active connection, or the
local placeholder offline), enforce stable `kind`, then dedup/append the version by
`content_hash`, set `current_version_id`, all in one transaction (I3, I7).

When connected, before creating a brand-new object row, the daemon checks whether
the server already owns `(owner, library, name)`; if so it adopts the server's
`remote_object_id` and appends the version there rather than creating an unlinked
local object. This is the single behavior that prevents the fork at the source.

### 6.4 Deterministic resolution (no ambiguous error for the common case)

`get_object(name_or_id)` resolves in this order, and the first hit wins:

1. exact internal `id`;
2. exact `(caller_owner, caller_library, name)` — the caller's own object;
3. exact `(caller_owner, <any library>, name)` if a single match;
4. if the caller explicitly passed `owner=`/`library=`/scoped-URL, resolve strictly
   within that scope.

`source_object_name` is NEVER a resolution key. Ambiguity (`ValueError`) is only
possible for an **explicitly cross-scope** lookup that truly matches multiple
owners/libraries; the bare-name path for the caller's own object cannot raise it
(I4). The error message, when it must appear, uses canonical `owner/library/name`,
never `server.<hex>` (I6).

### 6.5 Reconcile on (re)connect

On a successful server connection the daemon runs a reconcile pass:

- Rekey local placeholder-owner objects to the connected owner id.
- For each `(owner, library, name)` that exists both locally (authored) and on the
  server, link them (set `remote_object_id`), then merge version histories by
  `content_hash`: shared hashes collapse to one version; server-only versions are
  imported; local-only versions are marked pending-sync.
- If the same `(owner, library, name)` has **divergent** content that cannot be
  ordered as an append (e.g. two different definitions both claiming to be the
  head), record a `conflict` and expose it via `client.objects(...)`/diagnostics;
  do NOT create a duplicate object and do NOT silently pick.

Mirrors imported for objects the caller does not own keep `origin='server'` under
their real `(owner, library, name)` and never collide with the caller's namespace.

### 6.6 Offline forget / remove

Add daemon-local removal that needs no server:

- `DELETE /objects/<name>` (and a client `forget`/`remove_local`) removing the
  object row + its versions transactionally (cascade), by canonical key or id.
- optional single-version removal and a "prune stale mirrors" maintenance call.
- server-side `remove_entry`/`delete_library` remain separate and continue to
  require a connection.

### 6.7 Healing migration (existing corrupt stores)

A one-time, idempotent migration (run on daemon start / explicit command):

1. Back up `daemon.sqlite3` first.
2. Find groups that share a canonical identity today: a `server.<hex>` mirror whose
   `source_object_name`/`remote_name` equals a local-authored object's `name` in
   the same target library/owner.
3. Merge each group into one object row keyed `(owner, library, name)`, dedup
   versions by `content_hash`, keep the union of version history, set the correct
   current version, drop the `server.<hex>` name.
4. Report every merge and any unresolved conflict; never delete data without the
   backup; be safe to run twice.

### 6.8 Cross-entity coverage

- **Functions & pipelines**: same code path, same identity rules; `kind` only
  gates "cannot change kind", never identity or resolution. No pipeline-specific
  branch may remain.
- **Adapters, distributions, nodes, ports, scalars**: they are content **inside**
  an object version; `canonicalize()` (6.2) MUST include them, so changing an
  adapter or a declared distribution yields a new `content_hash` (new version), and
  identical adapters converge. Add explicit tests that an adapter-only change bumps
  the version and an identical re-publish does not.
- **Other registry entities** (envs, libraries, machines): they already have their
  own natural keys (`env.name`, `UNIQUE(owner, slug)`, machine id). This RFC does
  not change them, but any entity that syncs to the server MUST follow the same
  principle — canonical key + content/id linkage + no synthetic `x.<hex>` local
  names + deterministic resolution. Document this as the standing rule for new
  entity types.

## 7. Daemon schema changes (local)

In `spl/src/spl/daemon/storage_base.py`:

- `objects`: add `owner_id`, `library`; replace `UNIQUE(name)` with
  `UNIQUE(owner_id, library, name)`; keep `origin`; rename/alias `remote_name` →
  `source_object_name` per the in-flight migration (keep a read alias for one
  release).
- `object_versions`: add `content_hash`; add `UNIQUE(object_id, content_hash)`.
- Provide an in-place SQLite schema migration (versioned) that is transactional and
  backed up, plus the healing data migration (6.7).

## 8. Compatibility & migration

- `remote_name` is already deprecated in favor of `source_object_name`
  (`object.py` ~854-887 compatibility block). Complete that rename; keep a
  read-only alias for one release; remove the field from all resolution keys.
- Old stores must heal automatically and idempotently (6.7); a `--no-heal` escape
  hatch and a dry-run mode are required.
- The server API is unchanged; the daemon becomes a faithful cache of it.

## 9. Test matrix (mandatory — see Codex prompt parts for exact cases)

- Unit: identity uniqueness; kind-stability; content-hash idempotency;
  adapter/distribution change ⇒ new version; resolution priority; transactional
  rollback on failure.
- Regression: exact reproduction of the observed bug — a local-authored object plus
  a same-name server mirror must resolve to one object, and the original
  `ambiguous locally` path must be impossible for the caller's own object.
- Reconcile: connect with server-having-the-object ⇒ link + merge, no fork;
  divergent content ⇒ explicit conflict, no duplicate.
- Migration: a fixture store with the corrupt two-row shape heals to one object,
  idempotent on re-run, reversible via backup.
- Cross-entity: functions and pipelines exercised through the same paths;
  adapter-only change and identical re-publish behaviors.
- Property-based: random publish/republish/rename/connect sequences preserve I1–I9.
- No test or fixture may hardcode a machine-specific path, username, token, or the
  `server.<hex>` form.

## 10. Rollout

Implement in `spl/` across the ordered Codex parts (schema+migration → content
hashing → publish reconcile → resolution → connect reconcile+heal → forget →
cross-entity+hardening), each with its own green test run, then run the full
`spl/tests` suite. Only after everything is green does the finished package get
mirrored to `Release/spl` for deploy.
