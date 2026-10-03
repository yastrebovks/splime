:orphan:

Current-process profile: audit correction verification
======================================================

Local verification: **2026-10-03**, unreleased target **0.4.11**. This records the
follow-up fixes for implicit server source selection and cyclic explicit result
wrappers. Usage is in :doc:`public-embedded-runtime`. No production example URI,
service or registry publication was modified. This is implementation evidence,
not publication/deployment readiness or cross-platform verification.

Scope and reproduced failures
-----------------------------

The working trees already contained extensive changes. Pre-edit copies for this
pass are under ``/private/tmp/spl-two-findings/baseline``; unrelated edits,
release numbers, signed evidence, frozen fixtures and release declarations were
preserved. Applicable instruction files and repository boundaries were checked.

A disposable copy of the original server configuration and lock, without an
adjacent ``spl`` directory, reproduced ``Distribution not found`` for
``splime==0.4.11 @ directory+../spl`` under ``uv lock --check --offline``.
Two explicit artifact/result wrappers whose ``__spl_result__`` references point
to each other reproduced ``RecursionError: maximum recursion depth exceeded``.

Native result-wrapper correction
--------------------------------

Native normalization now follows replacement aliases iteratively, recording the
identities visited in that chain. Reaching an already memoized concrete result
is valid; revisiting an unresolved wrapper raises ``ValueError`` with its result
path, for example::

   result.answer.default: cyclic explicit result-wrapper chain

The diagnostic does not render or serialize caller data. No result-size or
nesting cutoff, input mutation, retry or new execution boundary was introduced.
Existing discovery, artifact conversion and memoized container construction are
retained; the legacy JSON/artifact branch is unchanged.

Six added test cases cover chains of 2, 5 and 2000 cyclic wrappers, a valid
2000-wrapper chain with shared references, concrete tuple/list cycles with an
artifact declaration and a direct self-wrapper, and a signed current-process
Pipeline producing the invalid two-wrapper cycle. They verify the precise path,
no dataset repr, unchanged caller containers, artifact copying once, ordinary
list/dict cycles, alias identity, and one execution with ``started``/``failure``
receipts and private module cleanup.

Standalone server and explicit local development
------------------------------------------------

The default ``spl-server/pyproject.toml`` no longer selects a local framework.
Its dependency remains exactly ``splime==0.4.11``. The documented normal commands
use consistency-checked locking, with CPython 3.13 selected for these tests::

   uv lock --check --python 3.13
   uv run --locked --python 3.13 --extra test python -m pytest

**The final index-based lock remains blocked by publication sequencing.** A real
online uv resolution confirmed that version 0.4.11 is absent from the index.
The tracked ``uv.lock`` retains the last genuine prerelease directory resolution,
with an explicit warning that it is stale for the default project. It was not
replaced with fabricated registry metadata, deleted, or downgraded. In particular,
its old ``../spl`` entry is not evidence of a completed standalone frozen workflow.
Do not use ``--frozen`` to bypass project/lock consistency checking.

A fresh standalone default copy with no adjacent framework rejects both
``uv lock --check --offline`` and ``uv run --locked`` because the exact index
version is unavailable, without attempting to load ``../spl``. After publication
of the matching framework, maintainers must run ``uv lock --no-sources``, review
and commit the actual generated registry artifacts/hashes, then pass the lock
check and locked tests. Offline checks additionally need cached metadata.

Before that publication, explicitly opt into one local source in a disposable
server copy (use an actual absolute path)::

   uv add --no-sync --python 3.13 --bounds exact --editable /absolute/path/to/spl

Or explicitly choose a freshly built framework wheel in a standalone layout::

   uv add --no-sync --python 3.13 --bounds exact /absolute/path/to/splime-0.4.11-py3-none-any.whl

Then run the same lock check and locked test commands. These choices write a
source override and a generated lock only in the selected copy. The exact wheel
metadata requirement is preserved; there is no source/index fallback. Local
configuration and locks must not replace the eventual shared index lock.
Discarding the disposable copy leaves the shared project unchanged.

The mechanism was verified with installed **uv 0.11.25**, rather than assumed:
``uv add --no-sync --bounds exact`` preserves the exact requirement and generates
real source/wheel locks. An attempted ``--config-file`` override was rejected by
uv because ``sources`` is not allowed in ``uv.toml``. Both explicit layouts
passed ``uv lock --check --offline`` and actual ``uv run --locked --extra test``
commands. Dependencies absent from the uv cache were downloaded only into the
disposable environments/cache; offline sync before that download correctly failed.

Clean uv environments also exposed a missing test dependency: existing server
publication tests import NumPy, but the test extra did not include it. The extra
now pins ``numpy==2.1.3``; runtime dependencies are unchanged. Both uv selections
were repeated successfully after this correction.

``tools/verify_mypy_delta.py`` was inspected: it selects its archived historical
baseline with ``uv run --project <baseline> --locked`` for both baseline and
candidate diagnostics. It needs no source-selection change. Its frozen baseline
and typing declaration were not modified. The nine tooling regression tests
passed; the full historical mypy delta gate was not rerun.

Fresh regression results
------------------------

Environment: disposable ``/private/tmp/spl-current-process-test``, CPython
3.13.14, macOS 26.6 arm64, NumPy **2.1.3**, pandas **2.2.3**. Separate uv-managed
virtual environments were created under ``/private/tmp/spl-two-findings``.
Daemon homes, secret backends, run directories, registry stores and pytest
storage were isolated. The user's active Python environment was not modified.

* Client selection: **863 passed, 25 skipped, 2 warnings**. It includes public
  current-process execution, unified result access, dependency/strict lock
  policies, native node and Pipeline semantics, adapters/artifacts, isolated
  artifact inputs/outputs, evidence, resume, JSON, runtime port adapters, owner
  Library calls, daemon publication routes, wire contracts, release chain,
  Console artifact builder and official registry checks. Skips are Docker-only;
  Docker was unavailable. Warnings are intentional duplicate-ZIP fixtures.
* Standalone server copy, explicit local wheel, actual locked uv invocation:
  **122 passed**. Selection: current-process publication, public Object
  publication/distribution/package closure/catalog, worker machine projection,
  and mypy verification tooling.
* Additional actual daemon/worker artifact selection: **4 passed, 66 deselected**.
* Coordinated copy with explicitly selected editable framework at
  ``../framework-source``: **24 passed**, overlapping the server selection.
* Initial current-process and unified result check: **65 passed**, overlapping
  the client selection. These overlapping runs are not additional distinct tests.
* Scoped Ruff, release identity generator ``--check``, real local uv lock checks,
  installed wheel metadata checks and ``pip check`` passed.

Logs and disposable layouts are under ``/private/tmp/spl-two-findings``. The
initial broad client command had a mistyped test filename and collected no tests;
the corrected selection above completed. Clean server tests initially failed on
missing NumPy, then passed after its test-extra correction. There are no remaining
automated test failures in the final selections. The normal index-lock check is
still expected to fail for the publication prerequisite described above.

The previous independent audit's 1,051 Python passes, 21 Console passes and 25
Docker skips are historical, not evidence for this pass. Console JavaScript and
remote Linux/Windows matrix tests were not rerun here.

Final installed-wheel acceptance
--------------------------------

After the final source/configuration changes, both wheels were rebuilt and
installed into the disposable Python environment. Every packaged source member
was compared byte-for-byte with its wheel member and installed file: **138 client
files and 87 server files** matched. Installed imports were verified to come from
``site-packages``. The final wheels have SHA-256 values:

* ``splime-0.4.11-py3-none-any.whl``:
  ``fab4213a91dc16f5fe385c58080cda405b4524ef3d00dd5bcd1751a9de82f730``.
* ``spl_server-0.4.11-py3-none-any.whl``:
  ``481ba3ff3509026de1f93cecdc943cd2b6abe0899b1f61d51ed84f4ef2f73056``.

The actual server wheel has ``Requires-Dist: splime==0.4.11``, no local URL/path
requirement, and NumPy only under ``extra == "test"``. These are real local build
hashes, not publication evidence.

``integration_tests/public_current_process_packages.py`` ran in separate
producer/consumer interpreters from ``/private/tmp`` with ``PYTHONPATH`` unset.
It created a signed disposable local publication and guarded package resolution.
The consumer verified the accepted ``SPLClient.embedded().call(...).value`` and
``head()`` behavior: a real DataFrame, same identity with visible mutation,
a distinct explicit copy, MultiIndex, nullable/categorical/timestamp/missing
values, and a 3-by-4 result. Caller/function PID was **35273**. Recorded NumPy
2.3.5 versus installed 2.1.3 warned and executed; matching pandas 2.2.3 did not
warn. Neither package identity/location/version changed. Subprocess, daemon and
environment entry points were guarded, no worker was imported, previews/payload
contained no dataset cells, and both calls emitted ``started``/``success``.

To repeat with a disposable CPython 3.13 environment containing the project's
build/runtime dependencies, from the workspace root::

   "$TEST_PYTHON" -m pip wheel --no-deps --no-build-isolation ./spl ./spl-server -w /tmp/spl-wheels
   "$TEST_PYTHON" -m pip install --force-reinstall /tmp/spl-wheels/splime-0.4.11-py3-none-any.whl /tmp/spl-wheels/spl_server-0.4.11-py3-none-any.whl
   "$TEST_PYTHON" -m pip check

From outside the checkout, run the absolute script path with ``PYTHONPATH`` unset,
first ``produce /tmp/new-evidence`` and then ``consume /tmp/new-evidence``. The
script additionally needs NumPy 2.1.3 and pandas 2.2.3. This pip acceptance is
separate from the real uv checks above and does not complete the index/frozen gate.

Changed files and release status
--------------------------------

Only these six files were edited in this follow-up pass:

* ``spl/src/spl/execution_results.py``.
* ``spl/tests/core/test_public_current_process.py``.
* ``spl/docs/source/public-current-process-verification.rst`` (this document).
* ``spl-server/pyproject.toml``.
* ``spl-server/uv.lock`` (prerelease limitation comment only).
* ``spl-server/README.md``.

Version 0.4.11 and release declarations remain unchanged. Declaration coherence
passes; historical 0.4.9/0.4.10 strict locks and signed fixtures are preserved.
The release manifest is still ``declared / release_evidence_pending``. Publication
of the framework and final registry-lock generation/revalidation must precede
claiming a normal standalone frozen workflow or release readiness. No package was
published and no service was deployed during this verification.
