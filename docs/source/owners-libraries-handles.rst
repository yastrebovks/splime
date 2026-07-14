Owners, libraries, and handles
==============================

Connected splime installations identify a library by the pair
``(canonical owner id, library slug)``. Two people can therefore own distinct
libraries with the same slug. A handle such as ``@alice`` is a human-friendly
reference to an owner; it is not the stored identity of that owner.

Identity and handle lifecycle
-----------------------------

Canonical user ids are opaque, stable values used in database rows, runs,
grants, sync events, cached objects, and YAML. Raw canonical ids remain valid
in every owner field. Handles are additive input and display sugar: the SDK
passes them to the daemon or server, which resolves them to a canonical id at
the boundary. No value beginning with ``@`` is persisted as an owner id.

A server gives every user a provisional handle. The user may claim a different
handle once in Console Settings or through ``POST /users/<user_ref>/handle``;
after that claim the handle is permanent. Handles are instance-wide and
case-insensitively unique. Their normalized form is 2--64 lowercase ASCII
characters, starts and ends with a letter or digit, and may contain internal
``_`` or ``-`` characters. Reserved values cannot be claimed. The leading
``@`` is accepted in references but is not part of the stored handle.

Inspect your identity and the directory
---------------------------------------

``whoami()`` returns the daemon's live server identity, or its cached canonical
identity when the server connection is offline:

.. code-block:: python

   me = client.whoami()
   me["owner_id"]
   me["handle"]             # "bob", without the leading @
   me["live"]               # False when this is cached offline identity

The mapping contains ``id``, ``owner_id``, ``handle``, ``display_name``,
``server_url``, ``machine_id``, ``connection_status``, and ``live``.
``users()`` is an authenticated, email-free directory containing only id,
handle, display name, and status:

.. code-block:: python

   users = client.users()
   alice = client.users(handle="@alice")[0]
   alice["id"]              # canonical id

Address a shared library explicitly
-----------------------------------

Library reads accept an optional canonical owner id or handle. Writes stay in
the connected user's namespace and do not gain a foreign ``owner=`` argument.

.. code-block:: python

   visible = client.library.list()
   shared = client.library.get("default", owner="@alice")
   assert shared["owned"] is False

   objects = client.objects(
       scope="server",
       owner="@alice",
       library="default",
   )
   signature = client.signature(
       "shared_fn",
       owner="@alice",
       library="default",
   )
   result = client.call(
       "shared_fn",
       owner="@alice",
       library="default",
   )

The list views prefer ``@handle`` labels and fall back to canonical ids. Their
``.raw`` payload is unchanged. ``owned`` says whether a visible library belongs
to the caller; ``owner_handle`` is an additive presentation field.

D1 resolution for library-scoped reads and runs
-----------------------------------------------

When ``library=<slug>`` is supplied without ``owner``, the server uses one
locked ladder. It checks the caller's own library first. If the requested
object is absent there, it considers only accessible same-slug libraries and
only direct object containment. Exactly one matching foreign library resolves;
more than one is an ambiguity error; zero is a normal not-found error, with
accessible same-slug library hints when useful. Inaccessible libraries never
appear in candidates or hints.

D1 applies to reads and runs only. Publish, grant, revoke, entry removal, and
other writes never auto-select a foreign owner. A run also requires execute
scope: a read-only shared object can resolve for ``signature()`` and still be
unavailable to ``call()``.

Worked example 1 -- your own object wins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Bob and Alice both have ``common_fn`` in their respective ``default``
libraries. Bob's call stays in Bob's namespace and has no resolution
annotation:

.. code-block:: python

   result = client.call("common_fn", library="default")
   assert "resolution" not in result.run.raw

Worked example 2 -- one shared match resolves
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Bob has no ``shared_fn`` in his own ``default`` library, and Alice's accessible
``default`` is the only match. The run succeeds and the receipt shows the
resolved scope:

.. code-block:: python

   result = client.call("shared_fn", library="default")
   print(result.run)
   # run:
   # ...
   # resolved  : @alice/default

   resolution = result.run.raw["resolution"]
   assert resolution["auto_resolved"] is True
   assert resolution["resolved_owner_handle"] == "alice"
   assert resolution["resolved_library"] == "default"

Read responses use ``resolved_from``; run responses use ``resolution``. Both
carry canonical resolved ids alongside the display handle and library slug.

Worked example 3 -- ambiguity is loud
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If both Alice's and Carol's accessible ``default`` libraries contain
``shared_fn``, the server returns a conflict with sorted, copy-pasteable
candidates:

.. code-block:: text

   @alice/default
   @carol/default

Choose one explicitly and retry:

.. code-block:: python

   result = client.call(
       "shared_fn",
       owner="@alice",
       library="default",
   )

The SDK is deliberately stricter for ``objects(library=...)``: when multiple
owners expose that slug, pass ``owner=`` even if only one of those libraries
contains a particular object.

Offline behavior
----------------

Handles resolve through the server. A pulled object is usable offline because
the local mirror stores its canonical owner id, but a new ``@handle`` cannot be
resolved while disconnected. In that case the daemon says to connect with
``client.connect_server(...)`` or pass the canonical owner id. Use
``client.whoami()`` for cached identity and warm required mirrors with
``client.pull(...)`` while online.

API reference
-------------

The generated API reference exposes :meth:`spl._client.SPLClient.whoami`,
:meth:`spl._client.SPLClient.users`, :meth:`spl._client.SPLClient.objects`,
:meth:`spl._client.SPLClient.call`, :meth:`spl._client._LibraryAdmin.list`, and
:meth:`spl._client._LibraryAdmin.get`, including their owner parameters.
