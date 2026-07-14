Cookbook
========

splime is offline-first: everything on this page runs with just the local
daemon — no account, no tokens, no network. The same scenario is enforced
mechanically by ``tests/core/test_cookbook_smoke.py`` (``just smoke``).

Setup
-----

.. code-block:: bash

   pip install splime
   spl-daemon serve        # local daemon on http://127.0.0.1:8765

One import is enough:

.. code-block:: python

   from spl import SPLClient

   client = SPLClient()    # local-first: never contacts a server
   client.health()

Offline by default
------------------

Server-only listings come back as empty states, never as errors:

.. code-block:: python

   client.current_server_connection()   # {'connected': False, ...}
   client.machines()                    # {'current_machine_id': None, 'machines': []}
   client.libraries()                   # []

Publish and call
----------------

``publish()`` stores the function as a versioned object and returns a short
receipt; the full daemon document stays in ``.raw``:

.. code-block:: python

   def daily_total(date: str) -> float:
       prices = {'2026-06-08': [11.0, 6.5, 24.5]}
       return sum(prices.get(date, []))

   client.register_env('default')                    # once per daemon
   client.publish(daily_total, name='daily_total')   # Published daily_total v1 (env=default)

   result = client.call('daily_total', kwargs={'date': '2026-06-08'})
   result.mode      # 'local'
   result.output    # 42.0  — the unwrapped value
   result.value     # {'default': 42.0} — the raw port dict

Environment resolution
----------------------

For server-origin objects, the stored ``env_python`` records provenance: the
interpreter the author used when the version was defined. When a local daemon
runs that version, it resolves the executable locally by env name, then by the
local ``default`` env, then by the daemon interpreter. If that substitutes the
authored interpreter, the daemon emits one ``interpreter_substitution`` log
record and exposes the authored and resolved interpreter pair in run state and
environment progress; ``spl-daemon doctor`` also warns when their Python
major/minor versions differ.

HTTP timeout model
------------------

Short daemon and SDK control-plane calls, such as register, list, status, and
poll requests, use a 60 second HTTP timeout by default. Blocking execution
calls are different: ``/remote-nodes/run`` and wait-style run requests hold the
HTTP response open while the daemon or server polls the run to a terminal
state. For those calls, ``timeout_seconds`` becomes the HTTP timeout when the
caller provides it; without a user timeout, the client leaves the read
unbounded so legitimate long runs are not cut off by the control-plane default.

Versions without ceremony
-------------------------

Republishing identical code is a no-op; any real change becomes the next
version. History is never overwritten:

.. code-block:: python

   client.publish(daily_total, name='daily_total')   # same version (idempotent)
   # ...edit the function...
   client.publish(daily_total, name='daily_total')   # next version

Pipelines
---------

.. code-block:: python

   from spl import Deployment, lift

   def classify_amount(amount: int) -> str:
       return 'priority' if amount == 300 else 'standard'

   def build_order(amount: int, bonus: int, status: str, scale: int = 1) -> dict:
       total = (amount + bonus) * scale
       return {'amount': amount, 'bonus': bonus, 'total': total, 'status': status}

   p = (
       lift(build_order)
       .bind(status=lift(classify_amount))
       .alias('result')
       .render('order_pipeline')
   )

   Deployment(p).run(amount=300, bonus=10, output='result')   # in-process dry-run

   client.publish(p, name='order_pipeline')
   client.call('order_pipeline', kwargs={'amount': 300, 'bonus': 10}, output='result').output

Functions inside a pipeline stay callable on their own:

.. code-block:: python

   client.call('order_pipeline', kwargs={'amount': 301}, function='classify_amount').output
   client.call('order_pipeline::classify_amount', kwargs={'amount': 300}).output

Catalog, async runs, cleanup
----------------------------

.. code-block:: python

   client.objects()                       # compact table; .raw for the payload
   client.describe('order_pipeline')      # human-readable summary

   run = client.submit('order_pipeline', kwargs={'amount': 300, 'bonus': 10}, output='result')
   run.status
   run.collect().output
   client.runs()                         # includes keep, manifest, parent id, disk size
   client.run_show(run.id)               # inline JSON values are summarized by default
   client.prune_runs(dry_run=True)        # TTL/status/id candidates without deleting

   client.forget_version('daily_total', 1)   # local cleanup, no server needed
   client.forget('daily_total')

Warm the cache, go offline
--------------------------

When you know you will lose the server connection, pull the server objects you
need into the local daemon cache while you are still online. A pulled mirror is
stored as server-origin metadata plus YAML; after disconnect,
``signature()``, ``describe()``, ``inputs()``, ``outputs()``, and normal
``call()`` execution read the local mirror.

For one object, use the explicit owner/library when you have it. A bare name is
also accepted when the connected server catalog contains a single visible
match:

.. code-block:: python

   connection = client.current_server_connection()
   if not connection.get("connected"):
       print("Connect once with client.connect_server(...) before warming the cache.")
   else:
       server_objects = client.objects(scope="server", library="risk")
       if server_objects:
           first = server_objects[0]
           client.pull(
               first["name"],
               owner=first.get("owner_id"),
               library="risk",
           )
           client.signature(first["name"], owner=first.get("owner_id"), library="risk")

For a library or the whole visible catalog, plan first. ``pull_all`` can touch a
large catalog; ``dry_run=True`` returns the same receipt shape without writing
mirror rows or YAML bodies:

.. code-block:: python

   plan = client.pull_all(library="risk", dry_run=True)
   print(plan["objects_seen"], len(plan["pulled"]), len(plan["skipped"]))

   if not plan["failed"]:
       receipt = client.pull_all(library="risk")
       print(receipt["pulled"], receipt["skipped"], receipt["failed"])

The mirror lifecycle is deliberately boring. When you reconnect with
``client.connect_server(...)``, the daemon's normal reconcile pass refreshes
server-origin mirrors that still exist. While connected, ``client.describe()``
shows when the visible server catalog has a newer version than your local
mirror. If an object was removed from the server catalog,
``client.prune_stale_mirrors(owner=..., library=...)`` removes stale
server-origin rows without touching local-origin objects. To drop one cached
mirror yourself, use ``client.forget(...)`` or ``client.forget_version(...)``;
those are local cleanup operations and do not delete anything from the server.

Retained runs and retention
---------------------------

Failed local ``Deployment`` runs are retained by default with
``keep='on_failure'`` so a later resume can inspect the manifest and artifacts.
Successful default runs still clean up. ``keep=True`` keeps successful and
failed runs until explicit prune. ``on_failure`` retained state expires after
seven days by default; ``spl-daemon run-prune`` removes expired inactive runs,
and accepts ``--dry-run``, ``--status``, ``--older-than-seconds`` and an
optional run id. Use ``--local`` to manage ``$SPL_RUNS_HOME`` retained
``Deployment`` runs instead of daemon runs. ``run-show`` previews inline values
by default; pass ``--full-inline`` only when the full local data should be
printed.

Retained state is pipeline data. It may include artifact files, manifest
metadata, inline JSON inputs and outputs, and keyword arguments such as values
passed through ``kwargs``. Local ``Deployment`` state lives under
``$SPL_RUNS_HOME`` (default ``~/.splime/runs``); daemon state lives under
``<daemon-home>/runs``. These directories and manifest files are created as
owner-only files on POSIX systems. ``run-show`` and ``client.run_show()``
summarize inline values by default and omit previews for obvious sensitive keys
such as passwords, secrets, and tokens; full inline values are shown only with
``--full-inline`` / ``full_inline=True``. ``run-prune`` is the supported way to
remove retained state; deleting files by hand can break resume lineage.

Run observability
-----------------

Retained manifests and ``run-show`` include the resolved adapter/tag for each
pipeline edge and the selected runtime for each node, with the source level
that won the hierarchy. Edge rows use ``producer.port -> consumer.port`` and
show whether the adapter came from the port default, pipeline, edge format, or
run override. Runtime rows show the node alias, runtime name, and whether it
came from the default, object runtime config, node tag, or run override.
Daemon polling exposes the same compact data as additive ``run_progress``
fields; ``progress=True`` prints a one-line summary when that data is present.
``spl-daemon run-list --tag-stats`` aggregates edge tag counts from retained
daemon run manifests; add ``--local`` to read retained ``Deployment`` run
manifests under ``$SPL_RUNS_HOME`` instead. The command only reads local
manifest files or the local daemon store, starts no background collection, and
does not contact the server or upload the aggregate.

.. _converter-node-pattern:

Converter Nodes For Adapter Tags
--------------------------------

Adapter tags describe the bytes on a pipeline edge: for example,
``csv-lines`` or ``json-rows``. A save half writes one tag, and a load half
declares which tags it accepts. If the producer writes a tag the consumer
does not accept, splime warns before the edge is used.

Prefer a run-level adapter override when the edge bytes are already compatible
and only the load half needs to change for one run; an override is the
``Deployment.run(adapters={...})`` replacement for one output edge. Use a
converter node when the formats are genuinely different, or when you cannot
change the consumer node's adapter. A converter is just a normal node placed
between producer and consumer: it loads tag A, returns a new Python value, and
saves tag B.

.. code-block:: python

   import json
   from dataclasses import replace
   from pathlib import Path

   from spl import Deployment, lift
   from spl.core.adapter_compat import find_pipeline_adapter_compatibility_issues
   from spl.core.entities.adapter import Adapter, make_key


   def extract_csv() -> str:
       return "name,score\nAda,7\nGrace,9\n"


   def save_csv_rows(path: str, value: str) -> None:
       Path(path).write_text(value, encoding="utf-8")


   def load_csv_rows(path: str) -> str:
       return Path(path).read_text(encoding="utf-8")


   def save_json_rows(path: str, value: dict) -> None:
       Path(path).write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


   def load_json_rows(path: str) -> dict:
       return json.loads(Path(path).read_text(encoding="utf-8"))


   def max_score(value: dict) -> int:
       return max(int(row["score"]) for row in value["rows"])


   class DictCsvEdgeAdapter(Adapter):
       @property
       def accepted_tags(self) -> frozenset[str]:
           return frozenset({"json"})


   extract = lift(extract_csv).alias("extract")
   broken = (
       lift(max_score)
       .bind(value=extract.as_format("csv-lines"))
       .alias("score")
       .render("broken_scores")
       .add_adapter(str, "csv-lines", save=save_csv_rows, load=load_csv_rows)
   )
   load_half = DictCsvEdgeAdapter(
       key=make_key(dict, "csv-lines"),
       save=save_json_rows,
       load=load_json_rows,
       py_type=dict,
       format="csv-lines",
   )
   broken = replace(broken, adapters={**broken.adapters, load_half.key: load_half})

   issue = find_pipeline_adapter_compatibility_issues(broken)[0]
   issue.warning_message
   # adapter tag mismatch on edge extract.default -> score.value: save tag
   # `csv-lines` ... accepted tags: json; hint: use `.as_format()`,
   # a run-level adapter override, or an explicit converter node
   # (cookbook: Converter Nodes For Adapter Tags)

   def csv_to_json_rows(value: str) -> dict:
       header, *lines = value.strip().splitlines()
       columns = header.split(",")
       return {"rows": [dict(zip(columns, line.split(","))) for line in lines]}


   extract = lift(extract_csv).alias("extract")
   convert = (
       lift(csv_to_json_rows)
       .bind(value=extract.as_format("csv-lines"))
       .alias("convert")
   )
   fixed = (
       lift(max_score)
       .bind(value=convert.as_format("json"))
       .alias("score")
       .render("fixed_scores")
       .add_adapter(str, "csv-lines", save=save_csv_rows, load=load_csv_rows)
   )

   find_pipeline_adapter_compatibility_issues(fixed)   # ()
   Deployment(fixed).run(output="score", keep=False)   # 9

Per-node runtime tags
---------------------

Pipeline node runtimes are resolved as default ``native`` →
``runtime_config['node_runtime']`` → pipeline tag → run override. Tags live on
the pipeline, not on the reusable node object:

.. code-block:: python

   pipeline = pipeline.with_node_runtime('heavy_step', 'venv-subprocess')
   Deployment(pipeline).run(runtimes={'heavy_step': 'native'})

Choose the smallest runtime that gives the isolation you need:

.. list-table::
   :header-rows: 1
   :widths: 14 28 34 34

   * - Runtime
     - Isolation
     - Requirements and image owner
     - Limits
   * - ``native``
     - Runs in the conductor process.
     - No extra tools and no image.
     - Supports the full in-process Python path. ``node_timeout_seconds`` does
       not apply.
   * - ``venv-subprocess``
     - Runs a function node in a separate SPL-free Python process.
     - Uses the current interpreter or daemon-provided node environment.
       No Docker image.
     - Function nodes only; inputs must be JSON-native; honors
       ``node_timeout_seconds``.
   * - ``docker``
     - Runs a function node in a Docker container with only the SPL-free runner
       work directory mounted.
     - Needs the Docker CLI and a responsive Docker daemon. Daemon runs use the
       object environment image; local ``Deployment`` needs an explicit
       ``runtime_config.docker.image``. Missing explicit images are pulled, if
       possible, by the host Docker daemon before the node runs.
     - Function nodes only; inputs must be JSON-native; honors
       ``node_timeout_seconds`` and kills the container on timeout. No SPL
       package, no ``PYTHONPATH`` injection, and no nested Docker inside an
       object-level Docker worker. Node containers default to no network.

For daemon runs, per-node ``docker`` uses the object-level environment spec:
the daemon builds or reuses the Docker image before starting the worker and the
manifest records the resolved ``image_tag``. Local client runs do not build
images in 0.4.x, so they must pass an explicit image:

.. code-block:: python

   import os

   from spl import Deployment, lift

   def seed() -> int:
       return 21

   def double(value: int) -> int:
       return value * 2

   if os.environ.get("RUN_SPL_DOCKER_EXAMPLE") == "1":
       seed_node = lift(seed).alias("seed")
       pipeline = (
           lift(double)
           .bind(value=seed_node)
           .alias("double")
           .render("docker_node_example")
           .with_node_runtime("double", "docker")
       )
       result = Deployment(
           pipeline,
           runtime_config={"docker": {"image": "python:3.13-slim"}},
       ).run(output="double")
       assert result == 42

Docker node containers default to ``--network none`` for isolation. If an
explicit image needs network access while the node function runs, set
``runtime_config={"docker": {"image": "...", "network": "enabled"}}``. Pulling a
missing image is done by the host Docker daemon and does not depend on the
node container's ``network`` setting.

Set ``runtime_config={"node_timeout_seconds": 10}`` on ``Deployment`` to bound
non-native per-node subprocess runtimes. ``native`` runs in the conductor
process and intentionally has no per-node timeout.

``venv-subprocess`` and ``docker`` both receive inputs through ``input.json``,
so input values must be JSON-native. If a node needs an arbitrary Python object
from an adapter edge, run that node with ``native``, insert a converter node as
in `Converter Nodes For Adapter Tags`_, or wait for artifact-file input
transport in a later 0.4.x update.

Work with a library someone shared with you
-------------------------------------------

This connected recipe is the exception to the offline-first sections above.
Alice and Bob each own a library whose slug is ``default``. Alice has granted
Bob read and execute access to hers, which contains ``shared_fn``. Connect the
local daemon before running the steps.

If the grant does not exist yet, open Console **Access**, choose **Request
access**, enter ``@alice`` as the owner, ``library`` as the resource type,
``default`` as the resource, and select **Library execute**. This is an
existing access-request path; the owner must approve the request before the
calls below can execute.

First confirm which account the daemon represents and inspect the email-free
directory:

.. code-block:: python

   me = client.whoami()
   assert me["handle"] == "bob"

   alice = client.users(handle="@alice")[0]
   assert alice["handle"] == "alice"

The visible library list includes both same-slug rows. ``owned`` separates
Bob's library from the one Alice shared:

.. code-block:: python

   libraries = client.library.list()
   shared = client.library.get("default", owner="@alice")
   assert shared["owned"] is False

Read the shared catalog and signature with an explicit owner:

.. code-block:: python

   objects = client.objects(
       scope="server",
       owner="@alice",
       library="default",
   )
   assert any(row["name"] == "shared_fn" for row in objects)

   signature = client.signature(
       "shared_fn",
       owner="@alice",
       library="default",
   )
   assert signature["name"] == "shared_fn"

An explicit call is deterministic even when more same-slug libraries become
accessible:

.. code-block:: python

   explicit = client.call(
       "shared_fn",
       owner="@alice",
       library="default",
       progress=False,
   )
   explicit.output

With ``owner`` omitted, D1 can select Alice only when her accessible library is
the unique foreign ``default`` containing the function. The run receipt records
what happened:

.. code-block:: python

   automatic = client.call(
       "shared_fn",
       library="default",
       progress=False,
   )
   automatic.run.raw["resolution"]["resolved_owner_handle"]   # "alice"
   print(automatic.run)   # includes: resolved  : @alice/default

Pass ``owner=`` whenever you already know the intended scope. D1 is a
read-and-run convenience, never a write target selector. For the complete
resolution ladder and offline boundary, see :doc:`owners-libraries-handles`.
