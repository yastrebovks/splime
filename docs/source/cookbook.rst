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

   client.forget_version('daily_total', 1)   # local cleanup, no server needed
   client.forget('daily_total')

Optional: connect a server
--------------------------

Connecting adds private worker machines, teams and shared libraries. The same
``call()`` becomes a remote run with ``target_machine=``; library management
lives under ``client.library.*``; a published object can join a new pipeline
via ``NodeRemote.locate(...)``. See the reference notebooks for the full
connected scenario.
