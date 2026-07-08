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

``venv-subprocess`` executes a Python function node in a separate SPL-free
runner process and records the selected runtime plus source in retained run
manifests. Per-node ``docker`` is reserved for a later 0.4.x follow-up; the
existing object-level Docker worker backend is unchanged.

Set ``runtime_config={"node_timeout_seconds": 10}`` on ``Deployment`` to bound
non-native per-node subprocess runtimes. ``native`` runs in the conductor
process and intentionally has no per-node timeout.

In 0.4.0, ``venv-subprocess`` receives inputs through ``input.json``, so input
values must be JSON-native. If a node needs an arbitrary Python object from an
adapter edge, run that node with ``native``, insert a converter node as in
`Converter Nodes For Adapter Tags`_, or wait for artifact-file input transport
in a later 0.4.x update.

Optional: connect a server
--------------------------

Connecting adds private worker machines, teams and shared libraries. The same
``call()`` becomes a remote run with ``target_machine=``; library management
lives under ``client.library.*``; a published object can join a new pipeline
via ``NodeRemote.locate(...)``. See the reference notebooks for the full
connected scenario.
