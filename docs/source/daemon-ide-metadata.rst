Authenticated daemon metadata
=============================

The integrated daemon exposes one additive, read-only metadata route for
trusted local clients:

.. code-block:: text

   GET /meta/capabilities
   Authorization: Bearer <local daemon API token>

The route uses the same bearer authentication as the existing local daemon
API. It has no anonymous form. A worker callback capability is scoped to its
existing callback route and cannot read this document. Successful and
unavailable responses include ``Cache-Control: no-store``.

Version 1 response
------------------

The daemon-owned schema is ``spl.daemon.meta-capabilities`` version 1. A
successful response is bounded to 8 KiB and has this shape:

.. code-block:: json

   {
     "schema": "spl.daemon.meta-capabilities",
     "schema_version": 1,
     "release": {
       "distribution": "splime",
       "version": "0.4.6",
       "evidence": {
         "kind": "installed_distribution_metadata"
       }
     },
     "build": {
       "revision": null,
       "evidence": {
         "kind": "unknown",
         "reason": "build_revision_not_embedded"
       }
     },
     "protocol": {
       "minimum": 1,
       "maximum": 1
     },
     "instance": {
       "instance_id": "0123456789abcdef0123456789abcdef",
       "generation": 7,
       "started_at": "2026-07-31T10:00:00.123456+00:00"
     },
     "observed_at": "2026-07-31T10:00:00.234Z",
     "capabilities": {
       "object.local.list": {
         "state": "supported",
         "version": 1,
         "reason": null
       },
       "object.local.detail": {
         "state": "supported",
         "version": 1,
         "reason": null
       },
       "object.local.signature": {
         "state": "supported",
         "version": 1,
         "reason": null
       },
       "run.local.create": {
         "state": "supported",
         "version": 1,
         "reason": null
       },
       "run.local.create.guarded": {
         "state": "supported",
         "version": 1,
         "reason": null
       },
       "run.local.list": {
         "state": "supported",
         "version": 1,
         "reason": null
       },
       "run.local.detail": {
         "state": "supported",
         "version": 1,
         "reason": null
       }
     }
   }

The installed release is resolved once when the daemon app is created, so the
reported release cannot change within one daemon generation. The current
package does not embed verified build-revision provenance; version 1 therefore
reports a typed ``unknown`` build revision instead of reading a source checkout
or guessing from release declarations.

Identity and capability truth
-----------------------------

The route succeeds only while ``DaemonHomeLock`` remains acquired and owns the
exact ``DaemonInstanceIdentity`` passed into the running app. It preserves that
identity's stable ``instance_id``, positive monotonic ``generation``, and exact
``started_at`` value. ``observed_at`` is produced for each request.

The seven capability records describe only daemon-owned upstream operations.
``object.local.list`` is the primitive that a later trusted companion may map
to browser-facing ``object.local.search`` after implementing its bounded
filtering adapter. ``run.local.create`` retains the legacy operation's
semantics; ``run.local.create.guarded`` identifies the distinct current-state
fenced admission route. This endpoint alone does not enable browser Object or
Run capabilities.

Version 1 readers must ignore well-formed unknown capability IDs. Every
capability record remains closed to ``state``, ``version``, and ``reason``;
the complete response remains size-bounded. A semantic change to an existing
capability requires a capability version change. A change to the document
shape requires a schema version change.

Unavailable response
--------------------

If live lock ownership, installed release evidence, document validation, or
the secret-exclusion check cannot be proven, the route fails closed:

.. code-block:: json

   {
     "error": "live daemon metadata is unavailable",
     "code": "daemon_metadata_unavailable"
   }

The response status is ``503`` and does not echo an exception or untrusted
metadata.

Security and compatibility
--------------------------

The document never reads the registry, central server state, environments,
telemetry, Objects, Runs, diagnostics, or endpoint files. It contains no API
token, PID, home/database path, socket, URL, counts, payload, traceback,
command, arguments, log, result, or artifact body.

The route is independently ignorable. Existing health, diagnostics, Object,
Run, CLI, startup, shutdown, authentication, and lifecycle behavior does not
depend on it. ``splime-jupyter`` remains an optional client: the daemon does
not import, discover, declare, activate, or require that distribution.

Rollback consists of removing the route registration and its daemon-owned
module. There is no database migration, lifecycle change, or stored state to
reverse.
