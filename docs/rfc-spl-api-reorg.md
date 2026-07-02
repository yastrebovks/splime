# SPL API Reorganization — Implementation Plan (RFC for Codex)

**Audience:** the Codex coding agent.
**Repository under change:** the `spl` package only (`/Users/kirill/Projects/SPL_v2/spl`).
**Status:** IMPLEMENTED through WP-01…WP-06, WP-07a and WP-08 (2026-07-02).
Only WP-07b (the breaking removals, `0.2.0`) remains. Read "Implementation
status" below before touching anything — do not re-implement finished
packages.

---

## Implementation status (as of 2026-07-02)

Verified by the full test suite (193 passed / 2 skipped on the sandbox runner;
its 10 known failures are Python-3.10 sandbox artifacts, not code) and, for
daemon-facing parts, against a live ``spl-daemon serve``.

| WP | Status | Shipped artifacts |
| --- | --- | --- |
| WP-01 single import facade | **Done** | `src/spl/__init__.py` (15 names); golden snapshot `tests/core/test_public_api.py` |
| WP-02 receipt + catalog views | **Done** | `PublishedObject` (repr < 200 chars, `_repr_html_`, `.version`/`.library`); `ObjectTable`/`ObjectList`/`ObjectCatalog` + `_wrap_objects` wired into `objects()`; `tests/core/test_presentation_views.py` |
| WP-03 unified result access | **Done** | `RemoteResult.output`, `Deployment.run(output=)`, `Run.value()`; `tests/core/test_unified_result_access.py` |
| WP-04 friendly errors / empty states | **Done** | empty offline `machines()`/`libraries()`/`objects(scope='all')`/`current_server_connection()`; ambiguous-name fixed at the resolver (identity RFC); multi-library hint in the `get_object` KeyError |
| WP-05 one client, layered internals | **Done** | `SPLClient.server`; `SPLClient.library` namespace now OWNS the library implementations; internal/advanced docstrings on `DaemonClient`/`SPLServerClient` |
| WP-06 collapse the surface | **Done** | `local_objects`/`server_objects` delegate to `objects(scope=...)`; `submit()` is the async canon; `NodeRemote.locate(...)` (extended with `owner`/`library`) |
| WP-07 breaking cleanup | **Split.** **07a done (compatible half):** `DeprecationWarning` on the flat library methods, `local_objects`/`server_objects`, `start`, `queue`, `run_node`, `run_node_result`; implementations inverted into canonical paths (`library.*`, `_start_run`, `_run_node_value`) so canonical calls — including the internal `Deployment` remote path — never warn; `tests/core/test_deprecations.py`. **07b remains for 0.2.0:** the actual removals + `__all__` shrink; deep-import shim warnings (require physical module moves first); `NodeRemote` ctor-form deprecation (the ctor is still documented in the 0.1.4 cookbooks; **`NodeRemote` itself is NOT removed — `locate()` becomes the only documented spelling**); the `.value` decision; dropping the `'TODO'` version sentinel. Tracked in `docs/migration-0.2.0.md`. |
| WP-08 cookbook as acceptance test | **Done** | `tests/core/test_cookbook_smoke.py` (`smoke` marker; skips cleanly without a daemon; offline tests verified against a live daemon); `docs/source/cookbook.rst` added to the toctree |

Related work shipped alongside (keep in mind when rebasing; do not undo):

- Facade re-exports restored + guarded after a `ruff --fix` F401 incident:
  `spl/daemon/store.py` re-exports with `__all__`, `spl/daemon/environment.py`
  re-exports with the `X as X` alias idiom. Do not "clean" them again.
- Daemon YAML display-cache namespaced by owner/library
  (`ObjectRepository._object_yaml_cache_path`).
- README quickstart uses `from spl import SPLClient` and `result.output`;
  the Libraries section uses `client.library.*`.
- The reference notebooks in `Notebooks/` were rewritten offline-first
  (explicitly outside this RFC's scope guard, at the owner's request).

---

## How to use this document

- The plan is split into **work packages (WP-01 … WP-08)**. Each WP is a standalone task
  sized to fit a single Codex run: it lists exactly which files to read, so you do not need
  the whole repository in context.
- **WP-00 is the constitution.** Its rules apply to *every* WP. Read it first, every time.
- Do the WPs **in order** (01 → 02 → … → 08); each builds on the previous one.
- Every chat prompt you receive points to exactly one WP section in this file. Implement
  that section, obeying WP-00, then run all verification gates.

---

## Context & diagnosis (why we are doing this)

The umbrella `spl` package is *technically complete but too heavy to use*. The README
promises "plain Python functions, no DSL to learn — publish, then call by name," but the
real cookbook (`Notebooks/splime-cookbook-вц.ipynb`, `Notebooks/All_Test.ipynb`) forces
users to learn the internal object model. Measured pain points:

| Problem | Evidence |
| --- | --- |
| Scattered entry point | 7 deep import paths reaching into `spl.core.entities.*`, although `spl/__init__.py` already re-exports some names. |
| Three overlapping clients | `SPLClient` (`client.py`, 1364 lines) wraps `DaemonClient` (`daemon_client.py`, 968 lines); `SPLServerClient` (`server_client.py`, 787 lines) is a parallel path. |
| Bloated surface | ~40 public methods on `SPLClient`; `call()` has 13–14 parameters; listing has 3 methods; execution has 5. |
| Inconsistent result shape | Value read four ways: `run(...)[node][DEFAULT_PORT]`, `.value['default']`, `.value[DEFAULT_PORT]`, bare `.value`. |
| Errors as control flow | `machines()` raises `ClientError 404` with no connection; `describe('order_pipeline')` raises "object display name is ambiguous locally". Nearly every notebook cell is wrapped in `try/except`. |
| **Huge outputs** | `publish()` prints a `PublishedObject` repr of **22,072 chars on one line** (whole daemon document in `raw`); `objects(scope='all', compact=True)` prints **12,536 chars / 355 lines** even with `compact=True`. |

Root cause of the huge outputs: there is **no presentation layer** — methods return raw
transport JSON. `describe()` is the one method with a curated output, which is exactly why
it feels usable. The plan generalizes that idea.

---

## WP-00 — Constitution (applies to EVERY work package)

**Working scope.** Your working directory is only `/Users/kirill/Projects/SPL_v2/spl`
(referred to below as the package root). You must not read or modify anything outside it:
`Release/spl` (the deploy showcase for an already-tested package), the sibling packages
`spl-core`, `spl-daemon`, `spl-server`, `spl-frontend`, `swpl-landing`, and the repo-root
`Notebooks/`. If a task seems to require changes outside `spl/`, STOP and report.

**Verification gates (mandatory before you finish; run from the package root):**

```bash
just lint        # ruff: B,E,F,I,N,W,ICN,INP,S,T20,RET,RUF,PTH
just typecheck   # ruff PYI,TC + mypy --strict over ./src and ./tests
just unit        # ruff PT + pytest -m 'not smoke'
just smoke       # pytest -m 'smoke'  (if the env has no daemon, say so explicitly in the PR)
just check       # ruff FIX: no FIXME/TODO markers left
```

The PR is not done until all five are green. Paste the gate output into the PR description.

**Hard code rules.**

- mypy `strict = true`; the package ships types (`py.typed`). Every new or changed public
  symbol is fully annotated. No `# type: ignore` without a one-line justification next to it.
- No new dependencies without separate approval. Do not bump the version in `pyproject.toml`
  except in WP-07.
- Minimal, on-topic diffs. No incidental reformatting or renames outside the task.
- Never touch secrets, `dev/.env`, or `.venv-*`.
- One WP = one branch = one PR = atomic commits.

**Backward-compatibility policy.** Until WP-07 (the major one), NO public name is removed or
changes its return type. Allowed only: adding symbols, changing `__repr__`/`_repr_html_`,
adding properties or parameters that default to the old behavior. Anything breaking goes
behind a `DeprecationWarning` and only in WP-07.

**Golden API snapshot.** WP-01 creates `tests/core/test_public_api.py` pinning the public
surface (`spl.__all__` + signatures of key methods). Any WP that intentionally changes the
surface updates the snapshot in the same PR so the reviewer sees the delta.

**On ambiguity or any conflict with existing tests, STOP and ask. Never edit tests to make
your own code pass.**

---

## WP-01 — Baseline guardrails + single import facade (non-breaking)

**Goal.** Put a safety net in place and remove deep imports: `from spl import …` must be
enough for users.

**Read:** `src/spl/__init__.py`, `src/spl/core/__init__.py`, `src/spl/core/common.py`
(only the names `lift`, `Deployment`), `src/spl/core/entities/node.py` (`DEFAULT_PORT`,
`InputPort`, `OutputPort`), `src/spl/core/entities/distribution.py` (`DDistribution`), the
header of `src/spl/client.py`, `src/spl/server_client.py` (the name `SPLServerClient`).

**Changes.**

1. `tests/core/test_public_api.py`: a snapshot test — compare `set(spl.__all__)` and
   `str(inspect.signature(...))` for `SPLClient.call`, `SPLClient.publish`,
   `SPLClient.objects`, `NodeRemote.__init__` against an expected value embedded in the test.
   The test must fail on any surface drift.
2. `src/spl/__init__.py`: re-export exactly the objects that are currently imported from deep
   paths (the same objects the notebooks import today via
   `from spl.core.common import Deployment, lift`): `lift`, `Deployment`, `DEFAULT_PORT`,
   `InputPort`, `OutputPort`, `DDistribution`, `SPLServerClient`, `spl_export_to_file`,
   `spl_export_to_dir`, `spl_import_from_file`. Update `__all__`.

**Compat traps.** Import order: `core` must not import `client` — otherwise a cycle. Put the
facade imports at the end of `__init__.py`. Re-exporting is additive; the old deep paths must
keep working.

**Tests.** `import spl` and identity checks: `spl.lift is spl.core.common.lift`,
`spl.DEFAULT_PORT == 'default'`, `spl.SPLClient is spl.client.SPLClient`. Plus the snapshot
from step 1.

**Definition of Done.**
`from spl import SPLClient, lift, Deployment, NodeRemote, DEFAULT_PORT, InputPort, OutputPort, DDistribution`
works; the snapshot is committed; all gates green.

**Ready code.** A turnkey diff for `src/spl/__init__.py` and the full
`tests/core/test_public_api.py` are in **Appendix C** — apply them verbatim, then run the
gates. Do not reorder the imports: the order shown is already ruff-isort correct.

---

## WP-02 — Presentation layer: `publish()` receipt + `objects()` catalog view (non-breaking)

**Goal.** Kill the huge outputs (measured: `publish` → 22,072 chars on a single line;
`objects(scope='all', compact=True)` → 12,536 chars / 355 lines). Invariant: **no public
method prints raw transport JSON as its primary `repr`; the raw payload is reachable only via
`.raw`.**

**Read:** `src/spl/client.py` (the `PublishedObject` class at ~lines 37–47, the `objects()`
method ~662–729, and `describe()` ~926–997 as the reference for a good output),
`src/spl/daemon_client.py` (`list_objects`, and the `register_object`/publish response — to
find the authoritative numeric version field).

**Changes.**

1. `PublishedObject` (currently `@dataclass(frozen=True)`): mark the class `repr=False`, make
   the `raw` field `field(default_factory=dict, repr=False)`; add a curated `__repr__` like
   `Published <name> v<version> (env=<env>)` and a `_repr_html_` (small table). `.raw` stays
   accessible.
   - Version correctness: take the number from the authoritative numeric field in the daemon
     response (find it in the publish/register_object code). If there is no reliable field,
     print without a version — do NOT parse `yaml_path` heuristically.
2. Catalog: introduce lightweight view wrappers that **subclass the existing container** so
   access does not break:
   - `class ObjectList(list[dict[str, Any]])` for `scope in {local, server}` — with
     `__repr__`/`_repr_html_` that render a compact table
     (`name, kind, version, library, #inputs`) and a `.raw` property (plain `list`).
   - `class ObjectCatalog(dict[str, Any])` for `scope='all'` — local/server sections in
     `_repr_html_`, keys `'local'`/`'server'` preserved, `.raw` → plain `dict`.
   - `objects()` wraps its return in these types. Since they subclass `list`/`dict`,
     indexing, iteration, and `json.dumps` keep working → non-breaking.
   - `compact=True` actually trims the display (show only name/kind/version/library). Do NOT
     change the server `?view=summary` contract.

**Compat traps.** `scope='local'` must remain a list under `isinstance`; `scope='all'` a
dict. mypy strict: parametrize generic subclasses (`list[dict[str, Any]]`).

**Tests.** `len(repr(published)) < 200`; `hasattr(published, '_repr_html_')`;
`isinstance(client.objects(scope='local'), list)`;
`set(client.objects(scope='all')) == {'local','server'}`; `.raw` returns plain types;
`json.dumps(client.objects(...))` does not fail.

**Definition of Done.** publish prints one line; `objects()` prints a compact table; all
previous access patterns work; gates green.

---

## WP-03 — Unified result access (additive)

**Goal.** Remove `run(...)[node][DEFAULT_PORT]` and `.value['default']` from the happy path.

**Read:** `src/spl/client.py` (`RemoteResult` ~50–82), `src/spl/core/common.py`
(`Deployment.run` ~198, the `Run` class ~229–260).

**Changes.**

1. `RemoteResult.output` (new property): if `payload['result']` is a dict and contains the
   key `'default'` → return `result['default']`; else if it has exactly one key → return that
   value; else return `result` as-is. Do NOT change `.value` (it stays raw — compatibility).
   Add a compact `__repr__`/`_repr_html_`.
2. `Deployment.run(output=<alias>)`: when `output` is given, return the value of that alias at
   `DEFAULT_PORT` directly. Without `output`, keep the current return shape (non-breaking).
   Add `Run.value(alias=None, port=DEFAULT_PORT)`.

**Compat traps.** Document the unwrap rule verbatim in the docstring. The old expressions
`[node][DEFAULT_PORT]` and `.value['default']` must still yield the same result.

**Tests.** Parity: for a pipeline with a named output and for a bare function, the new
`.output` / `run(output=...)` return exactly what the old paths returned.

**Definition of Done.** `client.call(...).output` and `Deployment(p).run(output='result')`
yield a plain value; the old paths stay green in tests.

---

## WP-04 — Friendly errors, empty states, and the ambiguous-name fix

**Goal.** Remove `try/except` around routine calls and fix `describe('order_pipeline')`.

**Read:** `src/spl/client.py` (`machines`, `libraries`, `current_server_connection`,
`objects`, `describe`), `src/spl/daemon_client.py` (where
`ClientError 404 'active server connection is not found'` and
`object display name is ambiguous locally` are raised), `tests/daemon/test_object_identity.py`.

**Changes.**

1. Empty states: `machines()` with no connection → `{'current_machine_id': None, 'machines': []}`;
   `libraries()` → `[]`; `objects(scope='server'/'all')` → an empty catalog;
   `current_server_connection()` → `{'connected': False, ...}`. Distinguish "no connection"
   (return empty) from "connection exists but the request failed" (re-raise). Map to empty
   ONLY the specific "active server connection is not found"; do not swallow other 4xx/5xx.
2. Ambiguous-name: when the conflict is between a local object and its own server shadow of
   the same logical object, resolve to the local one instead of raising. Genuinely distinct
   objects still raise with the disambiguation hint.
   - **Extra caution:** this touches identity-reconcile logic. First read
     `test_object_identity.py` and the resolver code. If the fix would change identity
     semantics beyond display-name resolution, STOP and report. `test_object_identity` must
     not regress.

**Tests.** Offline flow without tokens: `machines()`, `libraries()`, `objects(scope='all')`
do not raise. `describe('order_pipeline')` resolves when a local+shadow pair exists. The
two-genuinely-distinct-names case still raises with the hint.

**Definition of Done.** Routine calls in the offline scenario need no `try/except`;
ambiguous-name is fixed with no identity regressions.

---

## WP-05 — One client, layered internals (reorg; mostly non-breaking)

**Goal.** Everything reachable through a single `SPLClient`; importing `spl.server_client` /
`spl.daemon_client` directly becomes optional.

**Read:** `src/spl/client.py` (composition with `_daemon`), `src/spl/server_client.py`
(public methods), `src/spl/daemon_client.py`.

**Changes (no physical file moves in this step — to avoid breaking import paths).**

1. `SPLClient.server` — a lazy property returning a configured `SPLServerClient` (advanced
   access), so the direct import is no longer needed.
2. `SPLClient.libraries` — a namespace object with
   `create/get/update/delete/grant/revoke/add_reference/copy_object/remove_entry` (per the
   library-management RFC). The flat methods (`create_library`, etc.) stay as-is for now
   (warnings come in WP-07). The namespace is additive.
3. In the docstrings of `DaemonClient` and `SPLServerClient`, mark them internal/advanced.

**Traps.** This is the riskiest WP — if context is tight, split into 05a (`.server` +
routing) and 05b (`.libraries`). No module moves and no import-path changes. Full gates +
both smoke scenarios (after WP-08) green.

**Definition of Done.** Public tasks are solved through `SPLClient` without importing the
server/daemon client; old imports still work.

---

## WP-06 — Collapse the surface (additive, still non-breaking)

**Goal.** One canonical path per task; nothing removed.

**Read:** `src/spl/client.py` (`local_objects`, `server_objects`, `start`, `queue`,
`run_node`, `run_node_result`), `src/spl/core/entities/node_remote.py` (constructors).

**Changes.**

1. `local_objects`/`server_objects` → delegate to `objects(scope=...)`.
2. Execution canon: `call()` (sync) + `submit()` (async handle). Add `submit()` as an alias of
   `start` if missing; keep `queue`/`run_node`/`run_node_result` as thin wrappers.
3. `NodeRemote.locate(name=…, pipeline=…, function=…, url=…, version=…)` — the single
   documented factory path; the existing `__init__` forms keep working.

**Definition of Done.** One documented canonical way per operation; the surface is not shrunk
but is consolidated.

---

## WP-07 — Breaking cleanup behind deprecations (major bump 0.2.0)

**Goal.** A minimal public surface with a correct migration path.

**Changes.**

1. `DeprecationWarning` on: deep-import shims, flat library methods (superseded by
   `.libraries`), `local_objects`/`server_objects`, redundant execution methods, extra
   `NodeRemote` forms. Optional: flip `RemoteResult.value` to the unwrapped semantics and
   expose the old behavior via `.raw_value`.
2. Update `__all__`, bump `version = "0.2.0"`, add a migration note in `spl/docs` with an
   old→new table.
3. Intentionally update the golden snapshot (WP-01).

**Definition of Done.** Every removed/renamed symbol has a warning + a migration entry for
≥1 release; both smoke scenarios pass on the new API; gates green.

---

## WP-08 — Cookbook as a DX acceptance test (inside `spl/`)

**Goal.** Lock in "it's convenient now" mechanically. The reference notebooks live outside
`spl/` — do NOT touch them; encode the contract inside the package.

**Changes.**

1. `tests/core/test_cookbook_smoke.py` (marked `smoke`) reproducing the new-API happy path and
   asserting the DX invariants: a working `import spl` only (≤2 import lines); offline calls
   (`machines`, `libraries`, `objects(scope='all')`, `describe`) do not raise; the result
   reads as a plain value via `.output`; `len(repr(publish(...))) < 200`; the catalog prints
   compactly.
2. `spl/docs/source/cookbook.md` — the same scenario as documentation.

**Traps.** The test must fail on any DX regression (a return of `try/except` necessity
surfacing as an offline exception, growth of `repr`, deeper imports). Do not tie the smoke
test to a live server: where a daemon is needed, use the existing fixtures in
`tests/daemon/conftest.py`.

**Definition of Done.** `just smoke` runs the cookbook smoke; it goes red on any DX rollback.

---

## Order & properties

Dependencies are linear: **01 → 02 → 03 → 04 → 05 → 06 → 07 → 08**. WP-01…06 and 08 are
non-breaking (ship one at a time, patch releases); WP-07 is the only breaking one, in
`0.2.0`. WP-02, 03, 04 close the "heavy" complaints (huge outputs, result format, errors) and
give the fastest visible payoff.

---

## Appendix A — Key files (package root = `spl/`)

| Path | Role |
| --- | --- |
| `src/spl/__init__.py` | Top-level facade / public exports. |
| `src/spl/client.py` | `SPLClient`, `RemoteRun`, `RemoteResult`, `PublishedObject`. |
| `src/spl/daemon_client.py` | Low-level daemon transport (`DaemonClient`). |
| `src/spl/server_client.py` | Direct server transport (`SPLServerClient`). |
| `src/spl/core/common.py` | `lift`, `Deployment`, `Run`, `PipelineBuilder`. |
| `src/spl/core/entities/node.py` | `DEFAULT_PORT`, `InputPort`, `OutputPort`. |
| `src/spl/core/entities/node_remote.py` | `NodeRemote`. |
| `src/spl/core/entities/distribution.py` | `DDistribution`. |
| `tests/core/`, `tests/daemon/` | Existing test suites (do not regress). |

## Appendix B — Verification toolchain (from `.justfile` + `pyproject.toml`)

- Runner: `just` + `uv`. Venvs are `.venv-test`, `.venv-build`, etc.
- `mypy --strict` is enabled; `ruff` line length 120; pytest markers: `smoke` only;
  `asyncio_mode = "strict"`; `--strict-markers`.
- The package is typed (`src/spl/py.typed`), so keep the public API mypy-clean.

---

## Appendix C — WP-01 ready code (apply verbatim)

Verified against the current tree: `lift` is a module-level name
(`lift = PipelineBuilder.lift` in `core/common.py`), `Deployment` is a module-level class,
`DEFAULT_PORT`/`InputPort`/`OutputPort` live in `core/entities/node.py`, `DDistribution` in
`core/entities/distribution.py`, and none of these modules import the top-level `spl`
package, so there is no import cycle. The import order below is already ruff-isort correct
(first-party block, `spl.client` < `spl.core` < `spl.core.common` < `spl.core.entities.*` <
`spl.server_client`; members ordered constants → classes → functions). Do not resort it.

### C.1 — New `src/spl/__init__.py` (full file)

```python
from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from spl.client import PublishedObject, RemoteResult, RemoteRun, SPLClient
from spl.core import spl_export_to_dir, spl_export_to_file, spl_import_from_file
from spl.core.common import Deployment, lift
from spl.core.entities.distribution import DDistribution
from spl.core.entities.node import DEFAULT_PORT, InputPort, OutputPort
from spl.core.entities.node_remote import NodeRemote
from spl.server_client import SPLServerClient

__all__ = [
    "SPLClient",
    "SPLServerClient",
    "RemoteRun",
    "RemoteResult",
    "PublishedObject",
    "NodeRemote",
    "lift",
    "Deployment",
    "DEFAULT_PORT",
    "InputPort",
    "OutputPort",
    "DDistribution",
    "spl_export_to_file",
    "spl_export_to_dir",
    "spl_import_from_file",
]
```

### C.2 — Unified diff view

```diff
 from spl.client import PublishedObject, RemoteResult, RemoteRun, SPLClient
+from spl.core import spl_export_to_dir, spl_export_to_file, spl_import_from_file
+from spl.core.common import Deployment, lift
+from spl.core.entities.distribution import DDistribution
+from spl.core.entities.node import DEFAULT_PORT, InputPort, OutputPort
 from spl.core.entities.node_remote import NodeRemote
+from spl.server_client import SPLServerClient

 __all__ = [
     "SPLClient",
+    "SPLServerClient",
     "RemoteRun",
     "RemoteResult",
     "PublishedObject",
     "NodeRemote",
+    "lift",
+    "Deployment",
+    "DEFAULT_PORT",
+    "InputPort",
+    "OutputPort",
+    "DDistribution",
+    "spl_export_to_file",
+    "spl_export_to_dir",
+    "spl_import_from_file",
 ]
```

### C.3 — New `tests/core/test_public_api.py` (full file)

Bare `assert` is intentional and matches the existing suite (see
`tests/core/test_smoke.py`). `tests/core/` already has `__init__.py`, so no INP001 issue.

```python
"""Golden snapshot of the public :mod:`spl` surface.

If this test fails, the public API changed. Either the change is intentional — then update
the expected values below in the SAME PR — or it is accidental and must be reverted.
"""

from __future__ import annotations

import inspect

import spl
import spl.client
import spl.core.common
import spl.core.entities.node

EXPECTED_ALL = {
    "SPLClient",
    "SPLServerClient",
    "RemoteRun",
    "RemoteResult",
    "PublishedObject",
    "NodeRemote",
    "lift",
    "Deployment",
    "DEFAULT_PORT",
    "InputPort",
    "OutputPort",
    "DDistribution",
    "spl_export_to_file",
    "spl_export_to_dir",
    "spl_import_from_file",
}


def test_public_all_matches_snapshot() -> None:
    assert set(spl.__all__) == EXPECTED_ALL


def test_facade_symbols_are_canonical() -> None:
    assert spl.lift is spl.core.common.lift
    assert spl.Deployment is spl.core.common.Deployment
    assert spl.SPLClient is spl.client.SPLClient
    assert spl.DEFAULT_PORT == spl.core.entities.node.DEFAULT_PORT == "default"


def test_call_signature_keeps_expected_parameters() -> None:
    params = inspect.signature(spl.SPLClient.call).parameters
    for name in ("name", "kwargs", "output", "function", "target_machine"):
        assert name in params


def test_objects_signature_keeps_expected_parameters() -> None:
    params = inspect.signature(spl.SPLClient.objects).parameters
    for name in ("compact", "scope"):
        assert name in params
```

### C.4 — Fallback note (only if a cycle ever appears)

Direct import of `spl.server_client` is safe today. If a future refactor makes it import the
top-level `spl` package (creating a cycle at init time), convert only that one line to a lazy
`__getattr__` in `src/spl/__init__.py` and keep `SPLServerClient` in `__all__`:

```python
def __getattr__(name: str) -> object:
    if name == "SPLServerClient":
        from spl.server_client import SPLServerClient

        return SPLServerClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```

---

## Appendix D — WP-02 ready code

All edits are in `src/spl/client.py`. Verified: `publish()` builds
`PublishedObject(..., raw=record)` where `record` is the daemon `register_object` response;
`objects()` returns `self._daemon.list_objects(...)` / `server_objects(...)`.

### D.1 — Replace the `PublishedObject` dataclass

```python
@dataclass(frozen=True, repr=False)
class PublishedObject:
    """Receipt returned after an object is stored in the daemon registry.

    The full daemon document stays available via ``.raw`` but is kept out of the
    ``repr`` so a notebook does not print a multi-kilobyte blob.
    """

    name: str
    entrypoint: str
    env: str
    yaml_path: str
    workdir: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def version(self) -> str | None:
        """Best-effort human version, or ``None`` if the daemon did not send one."""

        current = self.raw.get("current_version")
        if isinstance(current, dict):
            for key in ("number", "version", "label", "name"):
                value = current.get(key)
                if value is not None:
                    return str(value)
        for key in ("version", "version_label"):
            value = self.raw.get(key)
            if value is not None:
                return str(value)
        return None

    def __repr__(self) -> str:
        suffix = f" v{self.version}" if self.version is not None else ""
        return f"Published {self.name}{suffix} (env={self.env})"

    def _repr_html_(self) -> str:
        rows = {
            "name": self.name,
            "version": self.version or "—",
            "env": self.env,
            "entrypoint": self.entrypoint,
        }
        body = "".join(
            f"<tr><th style='text-align:left'>{k}</th><td>{v}</td></tr>" for k, v in rows.items()
        )
        return f"<table><tbody>{body}</tbody></table>"
```

> **Verify (1 line).** The `version` field name is a best guess. In a daemon test, publish one
> object and print `published.raw.get("current_version")` and `published.raw.keys()`; lock the
> real key in `version`. If nothing carries a number, `version` stays `None` and the repr just
> omits it — still correct.

### D.2 — Add catalog view models (near `PublishedObject`)

```python
def _catalog_rows(payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, str]]:
    """Flatten a catalog payload (list of records OR dict keyed by name) to display rows."""

    records: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, dict):
                records.append({"name": value.get("display_name") or value.get("name") or key, **value})
    else:
        records = [r for r in payload if isinstance(r, dict)]

    rows: list[dict[str, str]] = []
    for r in records:
        library = r.get("library")
        lib_name = library.get("display_name") if isinstance(library, dict) else library
        rows.append(
            {
                "name": str(r.get("display_name") or r.get("name") or ""),
                "kind": str(r.get("kind") or ""),
                "version": str(r.get("version") or r.get("current_version") or ""),
                "library": str(lib_name or ""),
                "inputs": str(len(r.get("inputs") or [])),
            }
        )
    return rows


_CATALOG_HEADERS = ("name", "kind", "version", "library", "inputs")


def _rows_to_text(rows: list[dict[str, str]], title: str) -> str:
    if not rows:
        return f"{title}: (empty)"
    widths = {h: max(len(h), *(len(r[h]) for r in rows)) for h in _CATALOG_HEADERS}
    head = "  ".join(h.ljust(widths[h]) for h in _CATALOG_HEADERS)
    body = "\n".join("  ".join(r[h].ljust(widths[h]) for h in _CATALOG_HEADERS) for r in rows)
    return f"{title} ({len(rows)}):\n{head}\n{body}"


def _rows_to_html(rows: list[dict[str, str]], title: str) -> str:
    head = "".join(f"<th style='text-align:left'>{h}</th>" for h in _CATALOG_HEADERS)
    body = "".join("<tr>" + "".join(f"<td>{r[h]}</td>" for h in _CATALOG_HEADERS) + "</tr>" for r in rows)
    return f"<div><b>{title}</b> ({len(rows)})<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"


class ObjectList(list[dict[str, Any]]):
    """A list of object records that prints as a compact table; `.raw` gives a plain list."""

    def __repr__(self) -> str:
        return _rows_to_text(_catalog_rows(list(self)), "objects")

    def _repr_html_(self) -> str:
        return _rows_to_html(_catalog_rows(list(self)), "objects")

    @property
    def raw(self) -> list[dict[str, Any]]:
        return list(self)


class ObjectCatalog(dict[str, Any]):
    """A local+server catalog mapping that prints compact tables; `.raw` gives a plain dict."""

    def __repr__(self) -> str:
        return "\n\n".join(_rows_to_text(_catalog_rows(v), str(k)) for k, v in self.items())

    def _repr_html_(self) -> str:
        return "".join(_rows_to_html(_catalog_rows(v), str(k)) for k, v in self.items())

    @property
    def raw(self) -> dict[str, Any]:
        return dict(self)


def _wrap_objects(value: dict[str, Any] | list[dict[str, Any]]) -> ObjectList | ObjectCatalog:
    """Wrap a payload in a view type, preserving its runtime shape (list stays list-like)."""

    return ObjectCatalog(value) if isinstance(value, dict) else ObjectList(value)
```

### D.3 — Wire `objects()` to wrap (change only the 3 `return` lines)

```python
# scope == "local":
return _wrap_objects(self._daemon.list_objects(compact=compact))

# scope == "server":
return _wrap_objects(self._daemon.server_objects(owner_id=owner, library=library, compact=compact))

# scope == "all":
return ObjectCatalog(
    {
        "local": self._daemon.list_objects(compact=compact),
        "server": self._daemon.server_objects(owner_id=owner, library=library, compact=compact),
    }
)
```

Also update the `@overload` return annotations of `objects` so mypy stays happy: `local`/`server`
→ `ObjectList`, `all` → `ObjectCatalog`, and the implementation signature →
`ObjectList | ObjectCatalog`. Because these subclass `list`/`dict`, all existing indexing,
iteration and `json.dumps(...)` keep working (non-breaking).

---

## Appendix E — WP-03 ready code

### E.1 — `RemoteResult` in `src/spl/client.py`

Ensure the module imports `DEFAULT_PORT` (add `from spl.core.entities.node import DEFAULT_PORT`
if missing). Then make the payload silent in `repr` and add `.output`:

```python
@dataclass(frozen=True, repr=False)
class RemoteResult:
    run: dict[str, Any] = field(repr=False)
    payload: dict[str, Any] = field(repr=False)
    mode: str = "local"
    downloaded_artifacts: dict[str, Path] = field(default_factory=dict)

    # ... keep the existing value / artifacts / server_side properties unchanged ...

    @property
    def output(self) -> Any:
        """Unwrap the default single output.

        Rule: if ``value`` is a dict containing ``'default'`` -> return that entry;
        elif it is a dict with exactly one key -> return that value; else return
        ``value`` unchanged.
        """
        result = self.value
        if isinstance(result, dict):
            if DEFAULT_PORT in result:
                return result[DEFAULT_PORT]
            if len(result) == 1:
                return next(iter(result.values()))
        return result

    def __repr__(self) -> str:
        preview = repr(self.output)
        if len(preview) > 80:
            preview = preview[:77] + "..."
        return f"RemoteResult(mode={self.mode!r}, output={preview})"
```

### E.2 — `Run.value(...)` in `src/spl/core/common.py`

Add `DEFAULT_PORT` to the existing `spl.core.entities.node` import, then add two methods to `Run`
(it already has `self._pipeline`, `__getitem__`, and the pipeline exposes `get_node_by_alias`):

```python
    def value(self, alias: str | None = None, *, port: str = DEFAULT_PORT) -> Any:
        """Return one output value directly, without ``[node][port]`` indexing."""

        return self[self._resolve_alias_node(alias)][port]

    def _resolve_alias_node(self, alias: str | None) -> Node:
        if alias is None:
            raise ValueError("Run.value() requires alias=... (the name passed to .alias())")
        return self._pipeline.get_node_by_alias(alias)
```

### E.3 — Result usage after WP-03 (docs / cookbook)

```python
# local pipeline — was: run[node][DEFAULT_PORT]
with Deployment(p).run(amount=300, bonus=10, scale=1) as run:
    result = run.value("result")

# daemon/remote — was: client.call(...).value["default"]
result = client.call("order_pipeline", kwargs={"amount": 300}, output="result").output
```

---

## Appendix F — WP-04 ready code

### F.1 — Empty states in `src/spl/client.py` (verbatim)

`SPLClient` already has `self._has_server_connection()` (used by `objects()` auto scope), so gate
on it instead of matching an error string:

```python
def machines(self) -> dict[str, Any]:
    """Return the user's machines, or an empty listing when not connected."""

    if not self._has_server_connection():
        return {"current_machine_id": None, "machines": []}
    return self._daemon.server_machines()


def libraries(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
    """Return visible libraries, or an empty list when not connected."""

    if not self._has_server_connection():
        return []
    return self._daemon.server_libraries(include_accessible=include_accessible)


def current_server_connection(self) -> dict[str, Any]:
    state = self._daemon.server_connection()
    state.setdefault("connected", bool(state.get("server_url")))
    return state
```

In `objects()`, guard the server branches so an offline `scope='server'`/`scope='all'` returns
empty instead of raising (only when `owner`/`library` are not explicitly requested):

```python
if scope == "server":
    if owner is None and library is None and not self._has_server_connection():
        return ObjectList([])
    return _wrap_objects(self._daemon.server_objects(owner_id=owner, library=library, compact=compact))

if scope == "all":
    server = (
        []
        if owner is None and library is None and not self._has_server_connection()
        else self._daemon.server_objects(owner_id=owner, library=library, compact=compact)
    )
    return ObjectCatalog({"local": self._daemon.list_objects(compact=compact), "server": server})
```

Remove the `self._require_server_connection("listing server libraries")` line from `libraries()`.
Do **not** touch the `_require_server_connection` calls in the *mutating* library methods
(`create_library`, `grant_library`, …) — those should still fail fast.

> **Verify.** Confirm `_has_server_connection()` does not itself raise when the daemon is up but
> not connected to a server (it is already called in `objects()` today, so it is safe). The
> offline scenario here still assumes the local daemon is running.

### F.2 — Ambiguous-name: investigate, then implement (do NOT fabricate)

The resolver was not in the files read for this RFC. Locate it first:

```bash
grep -rn "ambiguous" src/spl
grep -rn "display name is ambiguous" src/spl
```

Then read `tests/daemon/test_object_identity.py` and the resolver. Rule to implement: when the
candidates are a **local object and its own server shadow of the same logical object**, resolve
to the local one; when they are **genuinely distinct** objects, keep raising with the existing
disambiguation hint. **STOP and report** if the fix would change identity semantics beyond
display-name resolution, or if it would touch `test_object_identity` expectations.

---

## Appendix G — WP-05 ready code

### G.1 — `.library` admin namespace (verbatim)

Named **`library`** (singular) on purpose, so the existing `libraries()` *list* method is not
clobbered. All delegated methods already exist on `SPLClient` (lines ~318–462).

```python
class _LibraryAdmin:
    """Grouped library-management operations, reachable via ``SPLClient.library``."""

    def __init__(self, client: "SPLClient") -> None:
        self._c = client

    def list(self, *, include_accessible: bool = True) -> list[dict[str, Any]]:
        return self._c.libraries(include_accessible=include_accessible)

    def create(self, slug: str, **kwargs: Any) -> dict[str, Any]:
        return self._c.create_library(slug, **kwargs)

    def get(self, ref: str) -> dict[str, Any]:
        return self._c.get_library(ref)

    def update(self, ref: str, **kwargs: Any) -> dict[str, Any]:
        return self._c.update_library(ref, **kwargs)

    def delete(self, ref: str) -> dict[str, Any]:
        return self._c.delete_library(ref)

    def grant(self, ref: str, grantee: str, **kwargs: Any) -> dict[str, Any]:
        return self._c.grant_library(ref, grantee, **kwargs)

    def revoke(self, ref: str, grantee: str) -> dict[str, Any]:
        return self._c.revoke_library_grant(ref, grantee)

    def add_reference(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self._c.add_reference(*args, **kwargs)

    def copy_object(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self._c.copy_object(*args, **kwargs)

    def remove_entry(self, library: str, name: str) -> dict[str, Any]:
        return self._c.remove_entry(library, name)
```

On `SPLClient`:

```python
@property
def library(self) -> _LibraryAdmin:
    """Grouped library administration (create/grant/reference/copy/…)."""

    return _LibraryAdmin(self)
```

### G.2 — `.server` accessor: investigate token source first

```bash
grep -n "user_token\|machine_token\|self\._user_token\|token" src/spl/client.py | head
```

If `SPLClient` retains the user token, wire a lazy property; otherwise store it in `__init__`
first (small, additive change), then:

```python
@property
def server(self) -> SPLServerClient:
    conn = self.current_server_connection()
    return SPLServerClient(token=self._user_token, base_url=conn.get("server_url") or DEFAULT_SERVER_URL)
```

**STOP and report** if the user token is not available on the client — do not read it from the
daemon secret store from here without approval.

---

## Appendix H — WP-06 ready code

### H.1 — `submit()` execution alias (`src/spl/client.py`)

```python
def submit(self, name: str, **kwargs: Any) -> RemoteRun:
    """Canonical async entry point. Thin alias of :meth:`start`."""

    return self.start(name, **kwargs)
```

### H.2 — `NodeRemote.locate(...)` (`src/spl/core/entities/node_remote.py`)

```python
@classmethod
def locate(
    cls,
    *,
    name: str | None = None,
    pipeline: str | None = None,
    function: str | None = None,
    url: str | None = None,
    version: str = "latest",
    target_machine: str | None = None,
) -> "NodeRemote":
    """The single documented way to reference a remote object."""

    return cls(
        name=name,
        pipeline=pipeline,
        function=function,
        url=url,
        version=version,
        target_machine=target_machine,
    )
```

### H.3 — Route the list aliases through `objects()` (optional, keeps return types)

```python
def local_objects(self, *, compact: bool = False) -> list[dict[str, Any]]:
    return self._object_records(self.objects(scope="local", compact=compact))


def server_objects(
    self, *, owner: str | None = None, library: str | None = None, compact: bool = False
) -> list[dict[str, Any]]:
    return list(self.objects(scope="server", owner=owner, library=library, compact=compact))
```

---

## Appendix I — WP-07 ready code

### I.1 — Deprecation helper (new `src/spl/_deprecate.py`)

```python
from __future__ import annotations

import warnings


def warn_deprecated(old: str, new: str) -> None:
    """Emit a uniform DeprecationWarning pointing callers to the replacement."""

    warnings.warn(f"{old} is deprecated; use {new} instead.", DeprecationWarning, stacklevel=3)
```

### I.2 — Application pattern and call-site map

Put `warn_deprecated(...)` as the first line of each deprecated method/shim:

| Deprecated | Replacement |
| --- | --- |
| `client.local_objects()` / `client.server_objects()` | `client.objects(scope="local" / "server")` |
| flat `client.create_library` / `grant_library` / `add_reference` / `copy_object` / … | `client.library.create` / `.grant` / `.add_reference` / `.copy_object` / … |
| `client.queue()` / `client.run_node()` / `client.run_node_result()` | `client.call()` (sync) / `client.submit()` (async) |
| extra `NodeRemote(...)` construction forms | `NodeRemote.locate(...)` |
| `RemoteResult.value` (raw dict) | `RemoteResult.output`; expose old behavior as `.raw_value` |
| deep imports `from spl.core.entities.* import …` | `from spl import …` |

### I.3 — Release mechanics

Bump `version = "0.2.0"` in `pyproject.toml`; add `docs/migration-0.2.md` with the table above;
update the golden snapshot in `tests/core/test_public_api.py` intentionally (this is the one PR
where `EXPECTED_ALL` may shrink).

---

## Appendix J — WP-08 ready code

Offline-safe cookbook smoke (no daemon/network) — guards the WP-01/02/03 wins. Add
`tests/core/test_cookbook_smoke.py`:

```python
"""DX acceptance smoke: the cookbook happy path must stay light. Marked ``smoke``."""

from __future__ import annotations

import pytest

import spl
import spl.client
import spl.core.common
from spl import Deployment, PublishedObject, lift
from spl.client import ObjectCatalog, ObjectList, RemoteResult

pytestmark = pytest.mark.smoke


def _order_pipeline() -> object:
    def classify_amount(amount: int) -> str:
        return "priority" if amount == 300 else "standard"

    def build_order(amount: int, bonus: int, status: str, scale: int = 1) -> dict:
        return {"total": (amount + bonus) * scale, "status": status}

    return (
        lift(build_order)
        .bind(status=lift(classify_amount))
        .alias("result")
        .render("order_pipeline")
    )


def test_single_import_facade() -> None:
    assert spl.SPLClient is spl.client.SPLClient
    assert spl.lift is spl.core.common.lift


def test_local_pipeline_reads_as_plain_value() -> None:
    p = _order_pipeline()
    with Deployment(p).run(amount=300, bonus=10, scale=1) as run:
        result = run.value("result")  # WP-03: no [node][DEFAULT_PORT]
    assert result == {"total": 310, "status": "priority"}


def test_published_repr_is_compact() -> None:
    published = PublishedObject(
        name="order_pipeline",
        entrypoint="order_pipeline",
        env="default",
        yaml_path="/x/versions/6.yaml",
        raw={"blob": "x" * 20000},
    )
    assert len(repr(published)) < 200  # WP-02: no 22k dump


def test_catalog_repr_is_compact_and_backward_compatible() -> None:
    payload = [
        {"name": "p", "kind": "pipeline", "inputs": [{"name": "a"}], "library": {"display_name": "Default"}}
    ]
    listed = ObjectList(payload)
    assert isinstance(listed, list)
    assert len(repr(listed)) < 400
    catalog = ObjectCatalog({"local": payload, "server": []})
    assert isinstance(catalog, dict)
    assert set(catalog) == {"local", "server"}


def test_remote_result_output_unwraps_default() -> None:
    assert RemoteResult(run={}, payload={"result": {"default": 42}}, mode="local").output == 42
    assert RemoteResult(run={}, payload={"result": "standard"}, mode="local").output == "standard"
```

> The daemon/server-dependent checks (offline `machines()`/`libraries()` not raising) belong in a
> separate test that uses the fixtures in `tests/daemon/conftest.py`; keep this file network-free
> so it never flakes.

---

## Appendix index

| Appendix | Work package | Turnkey? |
| --- | --- | --- |
| C | WP-01 facade | Fully verbatim. |
| D | WP-02 presentation | Verbatim; verify one daemon version-field name. |
| E | WP-03 result access | Fully verbatim. |
| F | WP-04 errors | Empty states verbatim; ambiguous-name = investigate-then-implement. |
| G | WP-05 one client | `.library` verbatim; `.server` = verify token source. |
| H | WP-06 collapse | Fully verbatim. |
| I | WP-07 deprecations | Helper verbatim; apply the map mechanically. |
| J | WP-08 smoke | Fully verbatim (network-free). |
