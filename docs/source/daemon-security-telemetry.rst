Daemon transport and telemetry
==============================

The local daemon executes registered code. Keep it on a trusted machine and do
not expose its listener to an untrusted network.

Central-server transport
------------------------

Every central-server/client transport that carries a daemon, machine, or user
credential requires HTTPS. URL username/password userinfo counts as a
credential even when no credential header is present. Plain HTTP is accepted
for the exact loopback development hosts ``127.0.0.1``, ``::1``, and
``localhost``; the daemon logs that exception once at WARNING. Credentialed
requests never follow redirects.

The one separate Docker callback exception is an exact ``POST`` to
``http://host.docker.internal/remote-nodes/run`` carrying only a
``Bearer splcb_...`` run capability. It rejects queries, other routes/methods,
body credentials, and every additional credential header, bypasses environment
proxies, and logs once at WARNING. It does not permit general daemon or server
traffic over the Docker bridge.

After upgrading, reconnect with
``spl-daemon server-connect --server-url https://your-server/api`` (or pass the
final HTTPS ``server_url=`` to the Python client). Do not use an HTTP endpoint
or one that redirects. Private CAs remain compatible through Python/OpenSSL's
normal trust configuration (including ``SSL_CERT_FILE``); the certifi roots
are loaded additively. Disabling TLS is not a supported substitute.

NodeRemote callbacks use an opaque, in-memory capability scoped to one active
local run and its registered remote nodes. Registered identities are compared
with their JSON types intact, so values such as ``true``, ``1``, and ``1.0``
cannot alias one another. The worker environment and callback protocol receive
this capability instead of the daemon master token. The callback capability
expires and is revoked when the local run becomes terminal.
A run with an explicit timeout receives that timeout plus a 30-second cleanup
grace; a run without a timeout is capped at 24 hours and must be restarted if it
legitimately runs longer.

Native and ``venv-subprocess`` runtimes execute trusted code under the daemon
user's operating-system identity. Neither is an OS security boundary: code
running there can read that user's daemon endpoint state, including its
persisted API token. Use the Docker worker boundary or a separately permissioned
OS account when running code that is not trusted with the local daemon user's
files. The callback capability limits the normal worker protocol; it does not
turn a same-user process into a sandbox or prevent arbitrary same-UID file
reads.

Daemon ownership and Docker isolation
-------------------------------------

One daemon process exclusively owns a daemon home. It takes an advisory lock on
``daemon.lock`` before opening SQLite or publishing ``daemon-endpoint.json``.
Starting a second daemon for that home fails with the existing endpoint and
owner PID; use a different ``--home`` for an independent daemon. The lock and
the stable ``daemon-identity.json`` file require a local filesystem with
working advisory-lock semantics.

Docker object runs use a new per-run container by default. The container gets a
read-write bind mount for that run directory only; daemon/framework source
needed by the worker is mounted read-only. Container names and labels include
the stable daemon-home identity, home hash, daemon generation, and run where
applicable. Startup and timeout cleanup select those labels and verify immutable
container IDs, so one daemon does not remove another daemon's workloads.
This process/filesystem boundary depends on the configured mounts and runtime
options and on trust in the host Docker daemon; mounting additional host paths
or Docker control sockets widens it.

The warm pool is deliberately off by default. It can be enabled only with both
``--docker-pool-enabled`` and a positive ``--docker-pool-size``. Programmatic
callers must additionally provide the stable daemon identity and an acquired
matching home-lock authority; ``docker_pool_enabled=True`` alone is rejected.
This opt-in has weaker file
isolation: **pooled containers share the runs directory with every other run on
this daemon; enable only for single-tenant, mutually-trusting workloads.** The
daemon logs that warning when the pool is enabled. ``--docker-prewarm`` is
rejected unless pooling is explicitly enabled.

On the 2026-07-15 audit host (Apple Silicon Docker Desktop 29.5.3,
``python:3.13-slim-trixie`` already local), 15 hardened one-shot
``docker run --rm`` starts had a 119.9 ms median and 137.1 ms p95 start-to-exit
time. Native Python startup measured 14.7 ms median in the same sample, for an
estimated 105.1 ms median container increment. This is a host-specific
measurement, not a latency guarantee; image pulls and application startup are
additional.

Finite native worker and subprocess-node timeouts terminate the whole POSIX
process group, escalating from ``SIGTERM`` to ``SIGKILL``. Docker timeouts
quarantine the lease and kill/remove the exact owned container before it can be
reused. Equivalent Windows descendant termination requires a Job Object and is
not implemented in this release, so every finite managed-subprocess timeout
(workers, subprocess nodes, environment builds, and interpreter probes) fails
closed on Windows instead of claiming to kill a process tree. In practice this
means fresh local daemon environment preparation/execution is POSIX-only in
0.4.5; Windows client-only use remains separate. The Windows home lock uses
``msvcrt.locking``, but Python ``chmod`` cannot establish a private Windows
DACL: until Windows execution support is restored and ACL-tested, keep any
Windows daemon home under an already owner-private user-profile ACL.

For the one-time upgrade transition, the daemon removes an unlabeled legacy
``splime-pool-*`` container only when its owner-only CID file belongs to the
locked home and Docker inspection confirms that home's exact ``/runs`` mount.
Unattributable legacy containers are left untouched rather than being removed
by a global name-prefix sweep. The daemon logs each such immutable container
ID. An operator can inspect only that candidate with ``docker inspect <id>``
and, after verifying its mounts/process and that no daemon owns it, remove that
exact ID with ``docker rm -f <id>``. Never clean legacy containers with a
``splime-pool-*`` name-prefix filter.

Telemetry levels
----------------

The daemon's central observability telemetry is independent of functional
remote-run result delivery. A machine executing a run requested by the server
must still return that requested result and its requested artifacts. The
telemetry setting controls the separate mirror used to show local-only daemon
runs in the Console.

``metadata`` (default)
   Sends run and object identifiers/display names, entrypoint and environment
   names, status, timestamps and duration, argument/result-presence flags,
   counts for arguments, keyword arguments, nodes, edges, artifacts and stream
   bytes, node ids/aliases/names/kinds/statuses/fingerprints, pipeline/runtime
   hashes, and an error type with the fixed message
   ``[details withheld by metadata telemetry]``. It sends no input values,
   result values, error details, stdout/stderr text, local paths, container
   details, artifact names, or artifact bodies.

``diagnostic``
   Sends all metadata plus error text truncated to 8 KiB and stdout/stderr
   truncated to 32 KiB each. It still sends no inputs, results, or arbitrary
   text-artifact bodies.

``full``
   Opts in to mirrored inputs, results, errors, stdout/stderr and supported text
   artifact bodies. The daemon logs this level at WARNING. Content is redacted
   and bounded before it is persisted, so an oversized component can be
   explicitly omitted rather than leaving an unsendable raw queue row. Artifact
   files are opened without following symlinks, and collection has per-file,
   aggregate-byte, accepted-body-count, and directory-scan limits.

Metadata inspects at most the first 100 node records for detail, bounds each
text value to 200 compact-JSON wire bytes (including escape expansion), and
measures at most 32 KiB of each stream for its byte-count summary. A larger
node set or stream reports ``node_detail_count_truncated``,
``stdout_bytes_truncated``, or ``stderr_bytes_truncated`` and lists the
corresponding omission; the reported count is then a lower bound. These work
limits apply even though no stream contents leave the machine at metadata.

Every projection has a 240 KiB payload budget inside the hard 256 KiB sync-event
limit. Before recursive redaction or JSON materialization, each input, result,
and artifact-list component must fit the projection budget independently;
otherwise that component is omitted and its availability flag remains false.
Full error text is bounded to 64 KiB before redaction, and each runtime-detail
field to 32 KiB. The final fitter can omit additional components when their
combined encoded size would exceed the projection budget. Reasons are listed
in ``telemetry.omissions``.

Artifact body collection reads at most 256 KiB from one file, emits at most 100
bodies and 192 KiB of aggregate text, and inspects at most 1,000 directory
entries plus one truncation sentinel. All entries count toward that scan limit,
including unsupported extensions. A capped or unavailable scan reports an
honest lower-bound count with ``artifact_count_truncated=true`` instead of
presenting it as exact. Synthesized stdout/stderr and legacy result text also
stop as soon as the content cap is crossed. Their ``size`` remains exact when
the text fits; with ``truncated=true`` it is a lower bound, while regular-file
sizes remain exact from the opened file descriptor.

On POSIX, the collector pins the directory by descriptor and uses no-follow,
descriptor-relative opens with identity checks. On platforms without those
Python primitives, including Windows, it rejects symlink/reparse roots and
files and revalidates path/opened-file identities before and after the bounded
read. Python's Windows standard library cannot make that fallback handle-
relative, so a privileged attacker capable of a precisely timed swap-away-and-
back race remains a theoretical limitation. Keep untrusted workers behind a
real OS/container boundary; telemetry file checks are not a sandbox.

Set the level when starting the daemon::

   spl-daemon serve --telemetry metadata
   spl-daemon serve --telemetry diagnostic
   spl-daemon serve --telemetry full

The environment equivalent is ``SPL_DAEMON_TELEMETRY``. The active level and
content-availability flags are visible in ``/health``, ``/diagnostics``, the
sync-status response, and Console run detail.

The daemon also overwrites the nonsecret ``spl.telemetry_policy.v1`` machine
capability on every central-server connect, reconnect, and sync. It contains the
active level, best-effort redaction mode, configured sensitive-field count, and
whether diagnostic text or raw values are eligible to be mirrored. This lets a
compatible server describe the selected Worker's policy in a point-in-time Run
preflight without exposing field paths or values. Missing or malformed
capabilities from older Workers remain unknown; the advertisement does not
override payload bounds, make redaction a privacy boundary, or guarantee that a
queued Run will execute under an unchanged policy.

Worker operations evidence
--------------------------

The daemon also overwrites ``spl.worker_operations.v1`` on every connect,
reconnect, and sync. This is a versioned, nonsecret allowlist for the Console's
Worker operations view; caller-provided content under that key is ignored. The
payload has this shape::

   {
     "schema_version": 1,
     "observed_at": "<daemon timestamp>",
     "sync": {
       "evidence": "observed | unknown",
       "pending": 0,
       "retryable": 0,
       "by_status": {"pending": 0, "failed": 0, "sent": 0},
       "oldest_pending_at": null
     },
     "environment_builds": {
       "evidence": "observed | unknown",
       "total": 0,
       "by_status": {
         "absent": 0,
         "creating": 0,
         "ready": 0,
         "failed": 0
       },
       "runtime_types": [],
       "latest_updated_at": null
     },
     "runtimes": {
       "implemented_object_modes": ["docker", "venv"],
       "implemented_node_modes": ["docker", "native", "venv-subprocess"],
       "availability": "unverified",
       "reason": "runtime_availability_not_probed"
     },
     "diagnostics": {
       "availability": "local_only",
       "command": "spl-daemon doctor --json",
       "sharing": "explicit_consent_required"
     }
   }

``sync`` uses exact database aggregates rather than the bounded diagnostic event
page. ``retryable`` counts pending events plus failed events marked retryable.
The build totals cover known cached build records only; a zero ``absent`` count
does not prove that every Object requirement has a materialized build. Runtime
names describe implementations present in this daemon version. They do not
prove that Docker, an interpreter, an image, or a compatible environment is
currently available, and they are not capacity evidence.

If a local aggregate cannot be read or validated, its section reports
``evidence: unknown``, null values, and a stable reason code instead of zeroes.
The server should use its receipt timestamp for network freshness because the
daemon's ``observed_at`` clock may differ. Heartbeat current, snapshot receipt,
sync backlog, and build state remain independent facts.

The capability never includes event payloads or kinds, errors, logs, paths,
package or environment specifications, tokens or token hints, Object identities,
Run identities, images, or doctor output. ``spl-daemon doctor --json`` remains a
local operator command. Exporting any doctor result requires a separately
approved allowlist and an explicit consent interaction; arbitrary local
diagnostics must not be uploaded.

Terminal manifest evidence
--------------------------

A claim-fencing-capable daemon advertises ``spl.execution_manifest.v1`` and
may attach an allowlisted evidence envelope to a terminal ``run_update``. The
envelope contains only schema version, deterministic SHA-256 digest, capture
time, source, node/edge counts, exact central Object version, and Pipeline
content hash. The full manifest, graph, input/output values, paths, commands,
errors, and runtime records remain local.

Artifact producer evidence is attached only when one retained manifest output
reference matches the uploaded artifact's relative name, SHA-256, and size.
The daemon never infers a producer from a filename, current Object graph, or
event order, and ambiguous matches remain unknown. The server derives the
accepted attempt from the current claim; the daemon does not assert an attempt
number. A digest identifies what the authenticated daemon recorded, not a
signature, sandbox attestation, or proof against a compromised Worker.

``spl.worker_build.v1`` separately advertises the installed ``splime`` package
version and supported protocol versions. Source ref and artifact digest remain
unknown until release/deployment attestation supplies them; equal package
versions alone do not prove compatibility.

Central execution capability boundaries
---------------------------------------

The central Worker protocol remains outbound polling through ``/sync``. It has
no server-push, SSE, WebSocket, live-log, tail, or streaming transport.
Stdout/stderr content can be mirrored only as bounded terminal telemetry under
the selected policy; its presence never means that Console can follow a Run
live.

``Resume`` remains a local retained-Pipeline operation. The local daemon route
and SDK create a child from eligible retained state on that same daemon.
There is no central Resume command, eligibility endpoint, or hosted-Console
bridge. A central Retry may execute user code again and is not Resume.

Execution approval is a separately approved 0.5 roadmap contract, not an
implemented daemon status or claim gate in this slice. The daemon does not
recognize ``pending_approval``, approve or reject Runs, or infer approval from
queue permission. Older and current unprotected queue semantics therefore
remain unchanged.

Redaction
---------

Diagnostic and full telemetry apply one shared best-effort redactor for common
secret keys and text shapes, including bearer tokens, AWS access keys, private
keys, password assignments, Authorization headers, and credential-bearing
connection strings. Best-effort redaction is not a privacy boundary; use the
default metadata level when raw values must remain local.

Mark application-specific fields with repeatable JSON Pointers::

   spl-daemon serve \
     --telemetry diagnostic \
     --telemetry-sensitive-field /input/kwargs/customer_secret

The environment equivalent
``SPL_DAEMON_TELEMETRY_SENSITIVE_FIELDS`` is a JSON array of JSON Pointer
strings. Values selected by these pointers are also removed when repeated in
errors or streams. Repeated-value discovery is capped at 256 distinct values so
redaction work remains bounded. Repeated text replacements operate on original
spans only, so inserted ``[REDACTED]`` markers cannot be reprocessed or
amplified. The selected fields themselves are still structurally replaced, and
a cap hit is recorded as an omission. If a legacy result is valid JSON syntax
but violates the browser-safe value contract, a ``/result/...`` pointer is
applied to a bounded parsed copy and the result remains an outer JSON string.
If that pointer cannot be applied safely, the result is omitted rather than
sent unredacted. Do not put secret *values* in command-line arguments; the
option contains field paths only.

Queue retention and upgrades
----------------------------

Run-linked sync rows have a seven-day payload TTL and an authentic database
cascade to their local run. Acknowledged rows are compacted immediately. When
a run is deleted, its linked rows are deleted in the same database operation;
expired local telemetry is removed before a sync attempt. Functional remote-job
results are never expired while unsent.

On first 0.4.5 startup, queued historical local-run projections are rewritten
under the configured policy before heartbeat sync begins. The new default is
therefore metadata even for a pre-upgrade queue. Data already accepted and
retained by a central server is historical server data and is not erased by a
daemon upgrade. It remains subject to the central deployment's backup and data
retention procedure; changing the daemon level is not a historical erase API.
