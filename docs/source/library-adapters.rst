Library Adapters
================

Library Adapters are publishable, immutable save/load codecs. They belong to
an owner and Library, but they are a separate aggregate from Functions, Nodes,
Pipelines and the eight built-in runtime adapters.

Identity and versioning
-----------------------

The stable resource identity is::

   owner / Library / adapter name

Each semantic change creates an immutable version within that resource. A Run
never binds ``latest``. It carries this closed exact reference beside the
existing runtime-port-adapter v1 document:

.. code-block:: json

   {
     "owner": "ky-monetech.mx",
     "library": "default",
     "name": "pandas-csv-semicolon",
     "version": 2,
     "adapter_id": "adapter-identity",
     "adapter_version_id": "immutable-version-identity",
     "content_hash": "...64 lowercase hex characters...",
     "signature_hash": "...64 lowercase hex characters..."
   }

All eight fields are required. Name and version are useful presentation facts;
the IDs and hashes prevent a stale or ambiguous display name from becoming
execution authority. The daemon re-reads the exact version and recomputes its
source and signature hashes before creating a Run.

Adapter versions are independent of Object versions. Selecting Adapter v2 for
a Run does not publish a new Function, Node or Pipeline. A Run that used
Adapter v1 continues to resolve v1 after v2 is published.

Publication is transactional per Adapter aggregate. Repeating identical
canonical content deduplicates on ``(adapter_id, content_hash)``. Changed
execution content receives the next registry-assigned immutable version.
Clients must not predict that number. Concurrent publications cannot move the
current pointer backwards; an A/B/A publication sequence leaves B current
while the repeated A resolves to its original version.

Partial save/load semantics
---------------------------

Directions are derived from source that actually exists; they are not a client
claim.

``load`` only
   Supports an ``input`` binding. The worker decodes a staged file and supplies
   the resulting Python value to the Object. It cannot encode an output.

``save`` only
   Supports an ``output`` binding. The worker encodes the Object result as a
   retained artifact. It cannot decode an input.

``save`` and ``load``
   Supports both compatible directions as one atomic version. The callables
   are never published as two Function Objects or versioned independently.

At least one callable is required. Selecting an unsupported direction fails
before Run creation, and no other adapter is substituted.

The callable conventions are:

.. code-block:: python

   def save(path: str, value) -> None:
       ...

   def load(path: str):
       ...

They must be plain top-level functions with reviewed static source. Static
analysis validates their shape and imports but never calls, compiles or imports
them in the browser, Jupyter Server, daemon control process or central server.
Definition and invocation happen only in the guarded worker.

Canonical hashes and nullable metadata
--------------------------------------

The content hash binds every execution-relevant field:

* canonical save and load source (including which direction is absent);
* semantic Python type and optional closed semantic category;
* exact dependency packages, versions and owned import roots;
* format tag, MIME type and preferred extension, including explicit ``null``;
* effective local and remote custom-code policy.

Description is presentation-only and does not change content identity. The
semantic signature hash is separate: it binds the semantic type/category,
derived directions, format tag, callable symbols, parameter lists and
annotations. It does not pretend that two versions are content-identical.

``format_tag``, ``media_type`` and ``preferred_extension`` are independently
nullable. A missing MIME type may use ``application/octet-stream`` for HTTP
transport, but the immutable Adapter metadata stays null. A missing extension
does not authorize inference from source or MIME type. With no format tag,
cross-version compatibility remains ``exact_version_only``.

Dependencies and compatibility
------------------------------

Each dependency declaration has exactly these fields:

.. code-block:: json

   {"package": "pandas", "version": "2.3.1", "modules": ["pandas"]}

The reviewed module roots establish unambiguous distribution ownership.
Publication binds them to an exact active-environment inventory and rejects
missing, conflicting or ambiguous evidence before mutation. It never imports
an optional package merely to inspect it.

At Run admission, Object and Adapter requirements are merged by canonical
distribution name. Conflicting exact versions fail before the Run row and
directory are created. Semantic recommendation is conservative: exact Python
types and explicit canonical aliases are ``recommended``. A different concrete
type is ``declared_type_mismatch`` even when both declarations share ``table``,
``image`` or another broad category. Missing, ``Any``, union and ambiguous bare
types are ``compatibility_unproven``. Extensions, MIME types and broad categories
never prove concrete type equality.

Semantic advisory and operation failure
---------------------------------------

Semantic state is advisory only for an explicitly selected SDK/API Adapter,
a reviewed embedded Pipeline ``preset``, or a browser selection confirmed by
the user. Direction, format, exact dependencies, ACL/policy, hashes, staging
limits, target capability and every other admission check remain hard gates.
Automatic/default resolution remains strict and never consumes an override.

New-capability Runs record the closed
``runtime_adapter_semantic_advisories`` sibling per explicit
``(direction, canonical port)``. It separately records port type, exact Adapter
type, optional stable category, state and acknowledgement source. A recommended
binding is always unacknowledged. SDK/API mismatches record explicit selection,
embedded presets record ``embedded_contract``, and the browser accepts only a
preflight-bound ``plugin_confirmation``. An old browser request may omit a
recommended row, but omission never authorizes mismatch or unproven
compatibility.

The SDK does not add a public argument or interactive confirmation. A daemon or
remote target that cannot prove ``spl.runtime_adapter_semantic_override.v1``
rejects a nonrecommended explicit selection before mutation. Compatible calls
remain usable with an older daemon by omitting the additive sibling. For remote
execution the daemon re-reads exact central Library Adapter facts before sending
the v3 admission; v1/v2 admission behavior is unchanged.

If a selected loader or saver fails in the worker, the Run terminates with
``input_adapter_load_failed`` or ``output_adapter_save_failed``. Terminal
evidence names the exact direction and port, the execution Adapter ID, and the
exact Library ref when applicable. It is bounded and path/source/exception
free. There is no fallback Adapter, retry, replay, or local output substitution.

Catalog, source and capabilities
--------------------------------

Library Adapters use separate additive capabilities:

* ``spl.library_adapter_catalog.v1``;
* ``spl.library_adapter_publish.v1``;
* ``spl.runtime_library_adapter_ref.v1``.
* ``spl.runtime_adapter_semantic_override.v1``.

The existing ``spl.ide.runtime_adapter_registry.v1`` remains the exact
eight-item built-in registry. It is not converted into a mixed registry.

Catalog, search, detail and version-history reads are bounded, cursor-paginated
and source-free. Rows may expose safe direction, dependency, compatibility and
effective availability summaries. They never expose executable source, raw
policy documents, publication-environment inventories, filesystem paths,
credentials or raw exceptions. Exact source is available only from the
authenticated, ``no-store`` editor/execution route.

When connected, the daemon reads the central catalog authoritatively. A live
central failure is an unavailable response, not a fallback to possibly stale
local rows. Local target availability and remote target availability are
evaluated separately. Remote selection names a target Machine explicitly.

Synchronization records the proven mapping between local and central Adapter
and version IDs. Before remote admission, a local exact ref is translated
through that mapping and the daemon re-reads the exact central version. It uses
the central version number it observes and never guesses ``vN + 1``.

Run and security boundary
-------------------------

The existing runtime-port-adapter v1 transport remains unchanged. Library refs
form a closed additive sibling keyed by ``(direction, canonical port)``.
Admission overlays the verified Library implementation onto valid v1 binding
topology, stages source owner-only, and records exact refs in the Run manifest
and terminal evidence. Source is not stored in the public manifest.

Library Adapter code is custom code. Local execution requires the immutable
version's local policy to allow it. Remote execution requires all of:

* the immutable Adapter's remote policy;
* requester consent with ``adapter_policy={"custom_remote": "allow"}``;
* target implementation and enablement of custom Adapter execution;
* exact target dependency and environment availability.

Publication alone never enables remote execution. Staging limits, content
digests, request fencing, cancellation, expiry, retention and non-replay rules
are inherited from the guarded runtime adapter bridge. Pickle is not used.

SDK publication and discovery
-----------------------------

The Jupyter workflow prepares the reviewed environment evidence. The same
closed request can be used from Python. This minimal standard-library Adapter
has no external dependencies:

.. code-block:: python

   from spl import LibraryAdapterRef, SPLClient
   from spl.core.library_adapters import environment_fingerprint

   client = SPLClient()
   distributions = []
   publication = {
       "schema_version": 1,
       "name": "utf8-text",
       "description": "UTF-8 text artifact",
       "semantic_type": "builtins.str",
       "semantic_category": "text",
       "save_source": (
           "def save_utf8(path: str, value) -> None:\n"
           "    from pathlib import Path\n"
           "    Path(path).write_text(value, encoding='utf-8')\n"
       ),
       "load_source": None,
       "dependencies": [],
       "format_tag": "text.utf8.v1",
       "media_type": "text/plain",
       "preferred_extension": ".txt",
       "policy": {
           "local_custom_code": "allow",
           "remote_custom_code": "deny",
       },
       "publication_environment": {
           "fingerprint": environment_fingerprint(distributions),
           "distributions": distributions,
       },
   }

   review = client.preflight_library_adapter(
       publication,
       owner="local",
       library="default",
   )
   assert review["status"] == "valid"

   receipt = client.publish_library_adapter(
       publication,
       owner="local",
       library="default",
       local_only=True,
   )
   adapter = LibraryAdapterRef(**receipt["ref"])

Preflight is read-only. The publication receipt distinguishes a created
version from an exact deduplication and returns the committed exact ref.

Discover code-free entries and immutable history with:

.. code-block:: python

   page = client.library_adapters(
       owner="local",
       library="default",
       direction="output",
       query="utf8",
       limit=25,
   )
   detail = client.library_adapter(
       adapter.adapter_id,
       owner=adapter.owner,
       library=adapter.library,
   )
   history = client.library_adapter_versions(
       adapter.adapter_id,
       owner=adapter.owner,
       library=adapter.library,
       limit=25,
   )

Bind the exact version per Run. This does not republish the Object:

.. code-block:: python

   result = client.call(
       "render_report",
       kwargs={"title": "Résumé"},
       adapters={"outputs": {"default": adapter}},
       artifacts_dir="./splime-results",
   )

For a load-only Adapter, pass a ``FileInput``. The path remains client-local;
only bounded verified bytes and a logical name are staged:

.. code-block:: python

   from spl.adapters import FileInput

   result = client.call(
       "inspect_image",
       kwargs={"image": FileInput("./sample.png", media_type="image/png")},
       adapters={"inputs": {"image": exact_image_adapter_ref}},
   )

The SDK re-reads the exact owner/Library/Adapter version before serializing the
Run. It sends only the closed exact-ref sibling; executable source is resolved
by the authorized daemon/central contracts.

Legacy behavior and migration
-----------------------------

Runs with no explicit Adapter binding retain the existing JSON/default wire
and worker behavior. No migration adds bindings to old Objects, Pipelines or
Runs. Direct ``Adapter`` instances, ``custom:<sha256>`` bundles and embedded
Pipeline adapters are unchanged.

The database migration adds Library Adapter aggregate/version tables and sync
link columns without rewriting existing semantic rows. The v0.4.5 migration
fixture test opens, backs up, migrates, reopens and restores a representative
database, then verifies its old Object, Run and worker evidence. Rolling back
the candidate binary leaves those old records usable; a pre-feature binary
simply has no capability for the additive tables.

Executable acceptance evidence
------------------------------

``tests/daemon/test_library_adapter_acceptance.py`` proves these examples
through publication, exact-ref admission and the real worker boundary:

#. load-only Pillow image upload;
#. save-only downloadable UTF-8 ``.txt`` output;
#. save+load pandas semicolon CSV round-trip;
#. save+load pandas/openpyxl XLSX round-trip;
#. save+load custom bytes with all optional metadata null;
#. Adapter v1 and v2 Runs against one unchanged Object version;
#. legacy JSON Run with no additive Adapter fields.

The Pillow/pandas/openpyxl cases skip cleanly in a lean test environment. Run
the same module in an interpreter containing those reviewed dependencies to
exercise all seven cases.
