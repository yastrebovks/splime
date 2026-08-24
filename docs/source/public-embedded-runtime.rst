Anonymous public releases and embedded execution
================================================

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

Release 0.4.8 packages the owner-approved trust pin for the one exact origin
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

Every executable lock uses ``spl.public_runtime_lock.v2`` and includes the
complete CPython 3.13 dependency closure plus the exact
``splime-public-worker==0.4.8`` wheel.  Third-party bytes are accepted only from
official PyPI JSON metadata and ``https://files.pythonhosted.org``.  Selected
files must be non-yanked ``py3-none-any`` pure-Python wheels with matching
name, version, size, SHA-256, ``Requires-Python``, dependency metadata and an
allowlisted SPDX expression.  Direct/VCS/local requirements, alternate
indexes, sdists, native members, ambiguous licensing and incomplete or
conflicting closures fail closed.

The first online execution stores every verified wheel under its SHA-256.
Environment installation consumes only those local files with ``--no-index``
and ``--no-deps``.  A later exact execution is therefore offline-reproducible;
missing or corrupt cache content is never replaced by an implicit index
lookup.  Candidate release bundles carry the deterministic dependency-free
worker wheel.  Production activation instead requires a separately published,
official-PyPI worker declaration matching the same 0.4.8 contract.

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
