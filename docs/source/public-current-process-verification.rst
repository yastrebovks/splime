:orphan:

Current-process profile: implementation verification
===================================================

Local verification date: 2026-10-02. This records development evidence, not a
published release, deployment approval or a cross-platform compatibility claim.
The user-facing contract and examples are in :doc:`public-embedded-runtime`.

Scope and baseline
------------------

The workspace contains separate ``spl``, ``spl-server`` and ``Release/spl``
repositories. The working ``spl`` tree already contained extensive tracked and
untracked changes. Existing files changed for this task were saved before
editing under ``/private/tmp/spl-current-process-baseline``; unrelated work was
preserved. The release checkout was inspected and left unchanged.

The initial embedded/distribution baseline passed 76 tests. The release
checkout and server declared 0.4.10, while the working client package declared
0.4.9. The client package metadata was aligned to the server's existing exact
``splime==0.4.10`` requirement for installed-package interoperability. The
already-used ``packaging`` library is now an explicit client dependency.

Implementation and compatibility decisions
------------------------------------------

* ``execution_profile="current-process-v1"`` is an explicit selection for new
  Object publications. Omission keeps the legacy profile; immutable versions
  cannot switch execution semantics.
* Signed manifest schema v2 authenticates the execution contract and captured
  dependency graph. The shared static inventory covers object IR, descriptors,
  nested nodes, component manifests and Library Adapter dependencies without
  importing published code. No transitive resolver is implied.
* The server bypasses wheel resolution and artifact-complete locks only for
  this profile. Native runtime and existing structural/private-infrastructure
  checks remain. Validation evidence explicitly excludes environment
  reproduction and the wheel-dependent SPDX check.
* The client verifies and admits before loading the signed compiled execution
  member. Missing packages produce a complete structured error; available
  mismatched or unverifiable versions warn and proceed. Installed metadata and
  reliable already-loaded version evidence are reported separately.
* Per-call bundled import namespaces, authenticated in-memory source and
  run-local artifact storage avoid worker/process-global state assumptions.
  Shared result normalization was extracted with the worker retaining its
  runtime adapter callback. Execution is attempted once; exceptions and
  KeyboardInterrupt propagate.
* V1 releases retain the worker path and exact v3 lock semantics. The strict
  lock validator accepts the inspected 0.4.9 and 0.4.10 framework policy
  identities without rewriting either signed lock. V2 uses a manifest-specific
  cache identity; trust already binds origin, signing key, manifest and bundle.
  A frozen actual 0.4.10 legacy verifier proves v2 rejection at its schema gate.

Verification results
--------------------

All local runs used a disposable environment at
``/private/tmp/spl-current-process-test``: CPython 3.13.14, macOS 26.6 arm64,
NumPy 2.1.3. Normal run storage was redirected to disposable paths.

* Client regression selection: **595 passed, 14 skipped**. This includes the
  new profile, existing embedded execution, strict runtime closure,
  distribution capture, native graph/adapter/artifact/JSON behavior, owner
  Library workflow and daemon publication routes. Docker-dependent tests were
  skipped because Docker was unavailable. One warning was the intentional
  duplicate-ZIP adversarial fixture.
* Server regression selection: **99 passed** across the new profile, public
  Object publication, distribution, package closure and catalog tests.
* Additional shared worker/native artifact selection: **71 passed,
  66 deselected**. This overlaps the main client selection; counts must not be
  added as distinct tests.
* After the last static metadata admission guards, focused client/server
  selections passed **31 and 8 tests**, respectively. Pytest reported cleanup
  warnings for earlier temporary read-only test directories; assertions passed.
* Scoped client and server Ruff checks passed. The client lock consistency
  check passed using ``uv lock --check --offline`` with the disposable Python.
* Both packages were rebuilt and installed as wheels after the final code
  changes. ``pip check`` reported no broken requirements. Producer and consumer
  ran in separate fresh interpreters outside the source trees and asserted
  imports came from ``site-packages``.

The final installed-package producer created an authenticated release recording
NumPy **2.3.5**, with wheel resolution replaced by a failure sentinel. The
consumer already had NumPy **2.1.3** loaded. Its computation returned
``{"pid": 87714, "sum": 10, "version": "2.1.3"}``; the caller PID was also
87714. A warning identified both versions and their sources. NumPy identity,
version and file location were unchanged. The worker was never imported,
subprocess/daemon/environment entry points were guarded against invocation,
and execution events were ``started`` then ``success``. Framework-managed
``sys.path``, working directory, environment and stdout/stderr were unchanged.

Reproduction entry points are
``tests/core/test_public_current_process.py``,
``../spl-server/tests/test_public_current_process.py`` and
``integration_tests/public_current_process_packages.py``. For the last one,
install locally built client/server wheels and NumPy 2.1.3 into a disposable
Python 3.13 environment, then invoke its ``produce`` and ``consume`` modes with
the same new temporary evidence directory, from outside the source repositories.

Review findings addressed
-------------------------

A separate final-diff review checked the requested acceptance criteria against
the producer, verifier, loader, cache and legacy paths. It found and fixed an
old-cache error regression, insufficient signed adapter metadata cross-checks,
bundle-lock reentrancy, cache collisions for identical executable bytes under
different profiles, and ambiguous same-named definitions flattened by the
existing component compiler. The latter are rejected before compilation.
Static metadata handling also rejects duplicate/reserved keys, unsafe component
paths, incomplete import records and cyclic/excessively deep graphs, while
ignoring runtime-like keys inside literal scalar data. Relevant regressions
were added and rerun.

Limits and outstanding release gate
----------------------------------

Only the local macOS arm64 environment was executed. A Python 3.13
Linux/macOS/Windows GitHub Actions matrix is configured in
``.github/workflows/public-current-process.yml`` but has not run here. No claim
is made that skipped Docker tests or every repository test passed.

The release-wide declaration check
``test_declared_release_contract_is_coherent_but_not_built_evidence`` remains
blocked by mixed 0.4.9/0.4.10 workspace declarations. The unchanged validation
helper also failed on a reconstruction using the pre-edit client metadata,
then identifying the server's existing 0.4.10 declaration as stale relative to
the 0.4.9 release contract. After client package alignment it identifies the
client as well. Coordinated release declarations/evidence must be reconciled
before publishing. This task does not rewrite release history or claim a
completed release validation chain.

The profile is limited to CPython 3.13 and a framework implementing its explicit
feature contract. It cannot honor hard timeouts, explicit isolated/remote nodes,
or dynamic ``importlib`` discovery of bundled modules. It checks authenticated
captured dependencies, not arbitrary dynamic imports or a reconstructed
transitive lock. Namespaces are not a security sandbox, and a successful
dependency preflight does not prove different library versions are compatible.

No packages were installed into or removed from the active user environment.
No production service, live public release or package registry was modified.
