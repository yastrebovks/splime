Guarded local Run admission
===========================

The daemon exposes an additive, authenticated admission operation for trusted
local clients that need to prove an exact reviewed Object call is still
current when its Run row is created.

``POST /runs/local-admissions`` is advertised as
``run.local.create.guarded`` version ``1``. It is separate from the legacy
``POST /runs`` route. Existing clients, historical-version behavior, remote
submission, response shapes, and errors on the legacy route are unchanged.

Trust and atomicity
-------------------

The guarded request is closed and local-only. It binds the live daemon
instance and generation, canonical Object owner/Library/name/ID, expected and
selected current version, top-level function, stored canonical content hash,
named JSON inputs, output selector, timeout, and retention. Unknown fields,
remote fields, duplicate JSON keys, invalid types, and out-of-policy sizes are
rejected before Run creation.

The daemon retains the registry's shared reentrant lock while it compares the
live Object state and inserts the Run. Object publication uses the same lock,
so a publication cannot advance the current version between comparison and
insertion. The existing daemon signature builder validates allowed and
required argument names and the output selector. The stored canonical Object
``content_hash`` provides the signature/content binding; this route does not
define a second signature-hash algorithm.

An accepted request returns a closed HTTP 202 receipt that binds the client
request ID, Run ID, authoritative Object/version/function, initial ``queued``
state, local source/target, and observation time. The request ID is not an
idempotency key: the daemon stores no request ledger and does not replay or
deduplicate requests. A client that loses the response must reconcile through
read-only Run evidence and must not automatically resubmit.

Every guarded-route failure is reason-coded and omits arguments, paths,
internal records, exception prose, and credentials. Guarded responses use
``Cache-Control: no-store``. The daemon's existing bearer authentication
continues to protect the route.

Compatibility and rollback
--------------------------

The guarded operation adds no database migration and no dependency on an IDE
or Jupyter package. Rolling it back consists of no longer advertising
``run.local.create.guarded`` and unregistering its distinct route. Runs already
accepted through it are ordinary local Runs and need no data rollback.
