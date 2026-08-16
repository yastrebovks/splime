Non-mutating prepared Object validation
=======================================

The integrated daemon advertises ``object.prepared.validate`` version ``1``
and exposes one additive operation:

.. code-block:: text

   POST /objects/validate
   Authorization: Bearer <local daemon API token>
   Content-Type: application/json

The route uses existing daemon bearer authentication, accepts at most 2 MiB,
has strict UTF-8/duplicate-key/non-finite/depth/node/string bounds, performs
work off the request loop with a five-second response deadline, and returns
``Cache-Control: no-store``. Errors are fixed source-free documents; request
content and exception prose are never echoed.

Closed request
--------------

``splime.object-validation-request/v1`` contains exactly:

* a source-free ``splime.prepared-manifest/v1``;
* canonical ``splime.object-ir/v1``; and
* a reference-resolution record.

Version 1 implements only ``mode: none`` with ``max_age_seconds: null``.
``cache_only`` and ``online_read`` are recognized future modes but fail closed
as incompatible; no cache or network fallback occurs.

The daemon recomputes the IR, dependency, and prepared-manifest bindings. It
then reconstructs current daemon entities in memory and repeats static AST,
identity, signature, port, graph, adapter, environment-request, runtime, and
optional exact-base checks. Existing publication continues to use its legacy
syntax validation path; only this operation opts into non-compiling AST
validation.

Closed result
-------------

``splime.object-validation-result/v1`` reports exactly one of ``valid``,
``invalid``, ``stale``, ``incompatible``, or ``reference_unavailable``. It
includes the prepared and IR hashes, capability and validator versions,
bounded check states and diagnostics, effective declared facts, a separate
``splime.validation/v1`` hash when IR semantics were evaluated, and a response
timestamp.

``publish_ready`` is always ``false`` in this layer. Even ``valid`` proves
only that the exact prepared evidence is statically acceptable in the reported
daemon context. It does not prove that files exist or are current, publication
authorization, name availability, a publication receipt, or Run authority.

Transport timeout or lost-response truth belongs to the trusted client. The
daemon cannot claim ``outcome_unknown`` for a response it successfully emits;
a companion may project that distinct state without replaying this request.

Read-only facts and non-mutation
--------------------------------

Optional base checks read only exact owner, Library reference, Object/version,
current-version, kind, and content-hash columns. Optional environment checks
read only whether the named environment is registered. They do not expose a
host path or inspect an interpreter. Runtime support is declared from the
closed v1 runtime catalog and does not build or probe anything.

The operation does not import or execute generated code, start a target
interpreter, resolve a remote reference, build an environment, or write a
file, cache, registry row, Object, ObjectVersion, environment, sync event,
Run, artifact, credential, connection, or server state. A deadline never
authorizes background mutation.

Security, optionality, and rollback
-----------------------------------

The route never accepts generated source and never returns an IR body,
credential, endpoint, PID, host path, raw diagnostic, command, arguments,
traceback, stream, result, log, or artifact body. A request containing the
current daemon API token anywhere in its prepared evidence is rejected.

The capability and route are independently ignorable. Daemon startup, CLI,
publish/call, authentication, existing APIs, Objects, Runs, sync, and lifecycle
do not require a Jupyter client or this route. Rollback removes the route,
capability advertisement, validator module, and read-only repository helper;
there is no migration or stored validation state to reverse.
