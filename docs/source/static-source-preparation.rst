Static generated-source preparation
===================================

``prepare_source()`` is a client-neutral, deterministic boundary for an
*explicitly supplied* generated Python bundle and a closed structured plan.
It does not generate source from a notebook or proposal. It parses and checks
the inputs without importing a module, producing bytecode, executing Python,
loading a dependency, querying a kernel, or using the filesystem, network,
subprocesses, environment, clock, or randomness.

This API establishes static representability only. A successful
``PreparedObject`` does not write a file, reserve or publish an Object, build
an environment, authorize publication, or execute a Run.

Version 1 request and result
----------------------------

The request is a closed ``splime.prepare-source-request/v1`` object containing:

* one to sixteen UTF-8 Python documents under relative logical paths;
* one ``splime.generated-plan/v1`` Function or Pipeline plan;
* exact sorted entrypoints;
* optional environment, runtime, and base Object/Pipeline identity;
* target Python grammar ``3.13``, IR schema ``splime.object-ir/v1``, and
  preparation protocol ``1``.

The result is ``splime.prepare-source-result/v1``. ``ready`` contains one
complete ``splime.prepared-object/v1``. ``invalid`` contains stable bounded
diagnostics and ``prepared: null``; it never returns a partially hashed object.
Malformed or incompatible protocol input raises a closed contract error.

Supported generated subset
--------------------------

Version 1 deliberately supports a small portable subset:

* module scope contains only an optional docstring, absolute imports, and
  undecorated synchronous function definitions;
* public function parameters are positional-or-keyword, with finite
  JSON-literal defaults and statically representable annotations;
* nested helpers, lambdas, and absolute local imports are representable;
  nested classes, async/generator/dynamic-scope features, relative or star
  imports, and dynamic namespace/evaluation calls are rejected;
* logical paths and protocol identifiers use the frozen portable ASCII
  subset, while source text and string values remain canonical UTF-8;
* every non-standard-library import must be owned by one exact declared
  dependency; and
* Pipeline Nodes, ports, edges, literals, adapters, outputs, runtimes, cycles,
  and required-input bindings are checked against the parsed signatures.

The compiler records function bodies as normalized AST source but never runs
them. Obvious embedded credential and absolute host-path literals fail closed.
The v1 adapter catalog preserves ``pickle`` only with a stable trusted-only
warning for later review.

PreparedObject and canonicalization
-----------------------------------

A ``PreparedObject`` binds all of these independent values:

* normalized logical source documents and their content hashes;
* a canonical generated plan and ``plan_hash``;
* canonical structured IR, explicit imports, and ``ir_hash``;
* the canonical JSON-as-YAML compatibility serialization;
* an in-memory generated-file plan (not a filesystem receipt);
* deterministic source-map and dependency hashes;
* normalized signatures, environment/runtime requests, and optional exact
  base identity; and
* compiler ``1.0.0``, canonicalization ``1``, schema, target, and protocol
  identities.

Source line endings are normalized to LF and an initial UTF-8 BOM is removed.
Comments, blank lines, quote spelling, and trailing whitespace otherwise remain
in normalized source identity. The canonical JSON profile uses Unicode scalar
strings, Unicode-code-point key ordering, finite binary64 values, ECMAScript
shortest number spelling, safe JSON integers only, and ``0`` for negative zero.
Integral Python float defaults are rejected because a JSON/TypeScript round
trip cannot preserve their Python runtime type.

JSON is emitted as a YAML 1.2-compatible byte representation for later review;
that representation is not the legacy registry payload and is never loaded by
the preparer.

Hash domains
------------

All hashes are lowercase ``sha256:<64 hex>`` values and use the domain bytes,
one NUL byte, then their exact canonical payload:

============================  =============================================
Domain                        Meaning
============================  =============================================
``splime.source/v1``          sorted logical paths and normalized source bytes
``splime.plan/v1``            canonical structured generated plan
``splime.ir/v1``              canonical structured SPL IR
``splime.source-map/v1``      canonical logical source map
``splime.dependencies/v1``    canonical dependency list
``splime.prepared/v1``        source-free prepared manifest with all bindings
============================  =============================================

``prepared_hash`` is not a daemon registry ``content_hash``. The latter is
created only by actual registration and may include resolved daemon facts.
No validation or publication rule may infer equality between the two.

Source-free validation manifest
-------------------------------

``prepared_manifest()`` removes source, IR serialization, source-map bodies,
and diagnostic prose. It retains their hashes, generated logical-path/hash
pairs, entrypoints, compiler/target identity, execution requests, and optional
base identity. The daemon receives this manifest plus canonical IR; it does not
receive generated Python from this operation.

Rollback and compatibility
--------------------------

The module is additive and has no stored state or migration. Existing import,
export, publication, call, Object, Run, authentication, and lifecycle behavior
does not use it. Removing the public export and module rolls back this static
surface without changing persisted data. Existing clients may ignore it.
