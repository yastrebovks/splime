Anonymous public releases and embedded execution
================================================

Current-process execution (explicit publication opt-in)
-------------------------------------------------------

For a **new public Object version**, select ``current-process-v1`` through the
ordinary profile workflow::

   author.object.preflight("my_function", execution_profile="current-process-v1")
   author.object.update(
       "my_function", public=True, execution_profile="current-process-v1"
   )

``author`` is the usual connected ``SPLClient`` used to publish the Object.
Publication preflight compiles and checks the candidate without activating it.
The HTTP ``public-preflight``, ``profile`` and ``public-activate`` Object routes
accept the same ``execution_profile`` field. Unknown selections are rejected.
Omitting selection for a new release retains the legacy isolated profile.
Omitting it when reactivating an existing release preserves that release's
profile. An immutable public version cannot change profiles: publish a new
Object version to choose different execution semantics.

Consumers keep the ordinary public call::

   client = SPLClient.embedded()
   result = client.call("splime://@alice/default/my_function@1", trust=True)
   result.value
   result.run["execution_profile"]  # "current-process-v1" for this profile

Only a signed v2 manifest selecting this profile executes in the caller's
current Python process. The authenticated producer-compiled ``object.py`` is
executed, without consumer recompilation. ``result.mode`` remains ``embedded``
and declared artifacts and native Pipeline adapter artifacts retain the
established result contract. For this profile, ``result.value`` is the actual
in-memory Python return value. A caller-owned DataFrame can pass through a
Function or Pipeline without JSON, pickle or a temporary-file round-trip.
For a Function whose body returns its input::

   frame = pandas.DataFrame({"value": [1, 2, 3]})
   run = client.call(ref, kwargs={"start_df": frame}, trust=True)
   assert run.value is frame

Function results are direct return values. Pipeline selection remains separate:
``run.value`` retains its existing alias/output-port mapping, for example
``run.value["answer"]["default"]``. Native support does not unwrap that structure.

Nested dictionaries, lists and tuples may contain native values as well. The
framework does not import or depend on pandas for this behavior. Explicit
Pipeline adapters and ``__spl_artifacts__`` declarations still materialize and
copy artifacts as requested. Inputs are passed by reference: published code may
mutate a caller-owned object, and Splime does not add an implicit defensive
copy. Ordinary native results are process-local, are not persisted or uploaded,
and cannot be resumed or reconstructed after that process exits.
``RemoteResult.payload["result"]`` contains only
a small JSON-safe ``native-in-memory`` type/shape descriptor; it is bookkeeping
metadata and cannot reconstruct the native value. ``repr(result)`` and the HTML
view summarize type, shape or container length without rendering the full
dataset or invoking the value's equality. Legacy ``RemoteResult`` instances
retain field-based equality. If either wrapper contains a native value, equality
is wrapper identity only, even if two wrappers refer to the same DataFrame.
An actual native ``None`` remains distinct from absence of a native value,
including when a legacy wrapper is copied or reconstructed.

Ordinary native Pipeline values retain their types, identity, shared references
and cycles, including NaN/infinity, sets, ranges and native mapping keys. No
ordinary container is rebuilt unless an explicit artifact conversion requires
changing its contents. Unchanged subgraphs retain their identity even then.
Native container evidence is marked ``unfreezable``; JSON serialization cannot
prove preservation of aliasing or cycles for resume. Explicit artifacts retain
their existing artifact evidence.

Dependencies belong to the current kernel/interpreter and are used as they
are. Package versions recorded at publication are provenance:

* All recorded distributions available with matching versions: execute.
* Available distributions with different versions: log visible, non-fatal
  warnings identifying the distribution, publication version, local version
  evidence and source component; attempt execution.
* Any recorded distribution missing: stop before loading any published module
  or adapter. ``ClientError.code`` is ``public_dependencies_missing``;
  ``payload["missing"]`` lists every missing package/version/source and
  ``payload["dependencies"]`` contains the complete inspection report.

Install missing packages yourself into the interpreter running the call. In a
notebook, ``%pip install <distribution-name>`` targets the current kernel; in
Python use that kernel's ``sys.executable`` with ``-m pip``. Recorded versions
are displayed for reference. Different components can record different
versions of one distribution, so the diagnostic deliberately supplies no
combined pinned installation command. Restart the kernel after changing
packages that are already imported.

No packages are installed, upgraded or downgraded by this profile. It creates
no environment, starts no worker or daemon, and never falls back to isolated
execution. Preflight uses distribution metadata without importing dependencies.
When a loaded module has reliable version evidence, diagnostics distinguish it
from installed metadata; the module is never reloaded or removed. Unverifiable
local versions receive a diagnostic and are allowed. Malformed or incomplete
publication records are rejected as contract errors, rather than reported as
missing packages. The inventory covers captured IR dependencies, nested nodes,
adapters, included component descriptors and their captured dependency fields.
Static ``import`` and ``from ... import ...`` statements are discovered even
inside Function bodies; aliases do not affect discovery. Each external import
root must have exactly one captured distribution owner (for example, import
root ``yaml`` belongs to distribution ``PyYAML``). Different components may
record distinct versions of that same owner; each version keeps its own
provenance and diagnostic. Distribution names are never guessed from import
names. Missing, partial or ambiguous owners fail publication and consumer
verification before code executes.
Dynamic ``importlib`` imports remain outside the static contract, and the
profile does not reconstruct a transitive PyPI lock. Admission does not
establish semantic compatibility: subsequent
import/library/user errors preserve their exception and traceback, and code is
never retried automatically.

The initial profile supports CPython 3.13 and a Splime build implementing
``spl.public_current_process.v1`` (framework minimum 0.4.10). Publication's
Python patch version is provenance within that explicitly supported family.
Native Functions and native Pipelines are supported, including their captured
Library Adapters. Root ``mode=venv`` metadata describes the author's environment
and is accepted. Explicit per-node Docker, subprocess, remote runtime boundaries
and timeout declarations are rejected. Object components must themselves have
the current-process contract; non-callable signed Library Adapter source
components can retain their v1 format. Runtime overrides, per-call adapters and
output selectors retain the existing embedded API restrictions.

``timeout_seconds`` is rejected before execution: arbitrary Python/native code
cannot be safely terminated in a caller's process. KeyboardInterrupt propagates.
Private per-call namespaces and bundled import caches prevent release name
collisions, including lazy Python import statements, without modifying
``sys.path`` or caller-owned logical ``sys.modules`` entries. Unique private
module names are registered so Python features such as dataclasses and
``typing.get_type_hints`` can resolve their defining module after return.
Functions retain their globals, and bundle-defined classes retain the private
namespace through an internal class attribute; this also supports instances
without weak-reference slots. After a successful call, only the private
``sys.modules`` entries become weak module references. User return values keep
their actual types and identities. This avoids a strong module/global/result
cycle rooted in the interpreter's registry. Cleanup occurs when the namespace
becomes unreachable and Python's cyclic collector runs, independently of the
``RemoteResult`` wrapper. Keeping a function, class, instance or traceback alive
can intentionally delay cleanup. Failures and KeyboardInterrupt remove the
private registrations immediately, without touching caller-owned entries.

Lifecycle inspection has no result-size cutoff. It visits builtin containers
with identity-based cycle detection and treats opaque datasets as leaves.
Ordinary class definitions are anchored at creation, including classes created
later by a returned function. Dynamically constructed classes returned directly
or inside builtin containers are anchored too. Arbitrary dynamically constructed
classes hidden inside an opaque third-party object cannot be discovered without
inspecting that object's internals. Private weak module registrations are not a
stable module-identity or serialization API. Dynamic
``importlib.import_module`` discovery of bundled modules is unsupported; use
ordinary import statements. Because the existing producer compiler inlines
``DSPLImport`` definitions, same-named definitions from multiple inlined
components are rejected explicitly; separate calls/releases may share names.
Namespace separation is **not a sandbox**. Imports,
native libraries, user side effects and kernel failures share the caller's
process. The framework does not change the working directory, environment or
stdout/stderr; relative paths in user code therefore refer to the caller's
working directory. Framework Pipeline run storage stays in this call's cache.

V2 authentication includes the execution contract and dependency graph. Trust
is bound to registry origin, signing key, manifest hash and bundle hash, so
approval of an isolated release does not approve in-process execution. V2 cache
entries also include the manifest identity. Existing v1 manifests/signatures,
caches and v3 exact locks are not rewritten or reinterpreted. Old clients reject
v2 at their existing schema gate before loading code. New clients still execute
v1 releases through the isolated worker path.

For this profile the producer does not contact PyPI or resolve/download wheels.
Consequently the wheel-closure SPDX policy is not evaluated for user-managed
packages. Existing source/private-infrastructure/component/archive policies
remain applicable. Signed validation evidence explicitly states that no
reproducible environment was built and does not claim wheel or SPDX validation.

See :doc:`public-current-process-verification` for local verification evidence,
platform limits and the status of the 0.4.11 release declarations.

Legacy isolated execution
-------------------------

The remainder of this page describes v1 releases and their strict v3 locks.

``SPLClient.embedded()`` resolves and executes signed public Function and
Pipeline releases without constructing or contacting ``spl-daemon``.  The
registry origin is configured on the client; a ``splime://`` reference can
never override it.

Signer authority does not come from the downloaded manifest envelope.  The
envelope carries only a stable ``key_id``.  The client selects Ed25519 public
key material from an external trust set bound to the normalized registry
origin.  A custom registry must be configured explicitly, for example::

   trusted_keys = {
       "https://registry.example": {
           "ed25519-sha256:<sha256-of-raw-public-key>": "<base64-raw-public-key>",
       },
   }
   client = SPLClient.embedded(
       registry_url="https://registry.example",
       trusted_keys=trusted_keys,
   )

Release 0.4.9 packages the owner-approved trust pin for the one exact origin
``https://splime.io``::

   key_id = ed25519-sha256:635b0f38b59fa4a513723bc8e347d807dae535996173ffebfba2fcca73cbe91e
   public_key_base64 = DDWqFt/l2ZaRFMgvdCdyoCcQ15ixQyMjx4nqtmvqTz4=

``SPLClient.embedded()`` loads this pin automatically only for that exact
origin.  Port variants, subdomains, lookalike hosts and every custom registry
still require an explicit caller-supplied trust set.  A key delivered in the
same bundle response is never accepted as a trust anchor.

Use an exact version for reproducible and offline-capable execution::

   from spl import SPLClient

   client = SPLClient.embedded(
       registry_url="https://splime.io",
       cache_dir=None,
   )
   result = client.call(
       "splime://@alice/image-tools/resize-image@3",
       args=["photo.jpg"],
       trust=True,
   )

The first execution of each authenticated exact release requires
``trust=True``.  Consent is bound to registry origin, trusted key ID, manifest
hash and bundle hash: publishing a new version, changing signed bytes or
copying a cache from another registry requires a new decision.  ``trust=True``
accepts execution risk; it never authenticates a signing key.  An unversioned
URI always revalidates the current active release and is never silently
replaced by a stale cached "latest" while offline.

Result and mode contract
------------------------

Embedded calls return the existing :class:`spl.RemoteResult` additive view.
``result.mode`` is ``"embedded"``; ``result.run`` contains the unique local run
ID, ``status``, exact public ``reference`` and immutable ``release_id``.
``result.value``, ``result.output``, ``result.artifacts`` and
``result.downloaded_artifacts`` retain their daemon-backed meanings.  Existing
local and server calls continue to report ``"local"`` and ``"server"``.

Cache and isolation
-------------------

Verified bundles are keyed by bundle SHA-256.  Environments are keyed by the
exact runtime/dependency lock and interpreter ABI, with one environment reused
for every Pipeline runtime name that shares that lock.  Pinned releases whose
bundle, resolution and trust decision are fully cached can run offline.
Each signed root archive also carries the verified immutable manifest and
bundle bytes for every referenced public Object or Library Adapter component;
later withdrawal of a component cannot change an existing root release.
Mutable profile Description text is served by the catalog and is not part of
the immutable manifest, bundle hash or exact-version identity.
Downloads and builds use per-hash interprocess locks, temporary siblings,
verification and atomic rename.  Retained artifacts use a unique embedded run
ID.  Cache cleanup is bounded and skips entries whose use lock is held.

Every executable lock uses ``spl.public_runtime_lock.v3`` and includes the
complete CPython 3.13 third-party dependency closure plus the exact
installed-framework executor contract (minimum 0.4.9 or 0.4.10 as signed).  Dependency bytes are accepted only from
official PyPI JSON metadata and ``https://files.pythonhosted.org``.  Selected
files must be non-yanked ``py3-none-any`` pure-Python wheels with matching
name, version, size, SHA-256, ``Requires-Python``, dependency metadata and an
allowlisted SPDX expression.  Direct/VCS/local requirements, alternate
indexes, sdists, native members, ambiguous licensing and incomplete or
conflicting closures fail closed.

The first online execution stores every verified wheel under its SHA-256.
Environment construction projects only those verified local wheel files; it
does not invoke pip or resolve additional dependencies.  A later exact execution is therefore offline-reproducible;
missing or corrupt cache content is never replaced by an implicit index
lookup.  Release bundles contain no first-party runtime artifact.  The client
projects only its verified installed ``splime`` package and distribution
metadata into the dependency environment; ``spl.daemon.worker`` executes
the authenticated producer-compiled member in a subprocess.  Dependency wheels that project ``spl``, a
``.pth`` file, ``sitecustomize`` or ``usercustomize`` are rejected before
installation.

The environment is a virtual environment.  It prevents dependency installation
from changing the caller's active Python environment, but **it is not an
operating-system sandbox**.  A trusted public Object runs with the user's OS
permissions.  Embedded workers receive a closed environment and do not inherit
daemon tokens, server user tokens, Console credentials or ambient secret-store
variables.

Unsupported surfaces
--------------------

Plain local names, Library Adapters, ``NodeRemote``, anonymous server-side
execution and daemon/server administration are unavailable in embedded mode.
They fail closed with stable ``feature_not_supported`` or the more specific
public admission error.  ``SPLClient()`` remains the unchanged integrated-daemon
client.

Anonymous run receipts
----------------------

Embedded mode enables best-effort public run receipts by default.  Set
``run_receipts=False`` on ``SPLClient.embedded()`` or set
``SPL_PUBLIC_RUN_RECEIPTS=0`` to disable them.  Delivery is asynchronous,
bounded, credential-free, and never changes the result of local execution when
the network is offline, slow, rate-limited, or rejects a receipt.

Each receipt contains only the exact public release ID, a fresh random event
ID, ``started`` or terminal ``success``/``failure`` state, a coarse Python
runtime family, and an occurrence timestamp.  Arguments, results, artifacts,
paths, exception text, credentials, user identity, and stable installation or
device identity are never included.  Counts are observed and approximate:
``runs`` counts the first accepted start for an event and
``successful_runs`` counts its first valid transition to success.  Raw
idempotency rows are retained for 30 days; aggregates can remain after pruning.

Platform support
----------------

The ``current-process-v1`` profile supports CPython 3.13 on Linux, macOS and
Windows. The caller owns its dependencies and native values; no timed worker
process is created.

Legacy isolated releases retain the existing POSIX-only timed-worker contract.
On Windows these releases fail before user code runs because Windows Job Object
process-tree containment is not implemented. ``trust=True`` cannot change a
release's signed profile. Use a release published with ``current-process-v1``
for native Windows execution, or a supported POSIX host for legacy releases.
