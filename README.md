# splime

**Reuse Python functions across projects without rewriting or redeploying them.**

splime turns trusted Python functions into versioned, portable **nodes** that can be
reused across projects and executed locally or remotely. You publish a function to a
private library once, then any project can call it by name, run it where the data or the
hardware lives, and read back the result and artifacts — without copying code or
redeploying.

- **Python-first.** Plain Python functions and pipelines, no DSL to learn.
- **Reuse-first.** Publish once, call by name from anywhere.
- **Private-team-first.** Your own libraries and workers, with explicit ownership and scoped access.
- **Local or remote.** The same call runs on your machine during development, or on a private worker that has the data, the GPU, or the credentials.

> splime is a private node registry and execution layer — not a workflow orchestrator, a
> scheduler, or a public marketplace. It does not replace Airflow, Prefect, or Temporal.

---

## Requirements

- Python **3.13+**

## Install

```bash
pip install splime
```

The distribution is named `splime`; the Python import package is `spl`.

## Quickstart

**1. Start the local daemon** (it stores your objects and runs workers):

```bash
spl-daemon serve            # listens on http://127.0.0.1:8765 by default
```

**2. Publish a function and call it** — a plain `SPLClient()` is fully local and never
contacts a server:

```python
from spl import SPLClient

def daily_total(date: str) -> float:
    prices = {"2026-06-08": [11.0, 6.5, 24.5]}
    return sum(prices.get(date, []))

client = SPLClient()                       # local-first; no server contact
client.publish(daily_total, name="daily_total")

result = client.call("daily_total", kwargs={"date": "2026-06-08"})
print(result.mode)     # "local"
print(result.output)   # 42.0  (unwrapped value; result.value keeps the raw port dict)
```

That is the whole loop: define a function, `publish` it as a versioned node, then `call`
it by name and get back the value (plus logs and any artifacts).

## Run it where the data lives

The same `call` becomes a remote run when you point it at a library, an owner, or a
target machine. This requires a connected splime server and a private worker; the local
daemon builds an isolated environment on the worker before executing.

```python
client = SPLClient(user_token="…", machine_token="…")   # connect the daemon to your server

result = client.call(
    "daily_total",
    kwargs={"date": "2026-06-08"},
    target_machine="gpu-box",        # hand the run to a private worker
)
print(result.mode)    # "server"
```

`SPLClient()` without tokens stays entirely local — connecting to a server is always
optional.

## Libraries

Libraries group versioned objects and control who can see and run them. Creating and
curating libraries uses a server-connected client:

```python
client.library.create("risk", display_name="Risk", visibility="private")
client.publish(risk_score, name="risk_score", library="risk")

# Grant scoped access to a teammate
client.library.grant("risk", "analyst1", scopes=["metadata:read", "objects:read", "execute"])
```

A library can also reference a live object from another library (`add_reference`, follows
`latest`) or take an owned snapshot with provenance (`copy_object`).

## Security & trust

splime runs code that you publish on purpose, on machines you control. It is built around:

- **explicit ownership** of every published object,
- **scoped access** grants per library (read metadata, read objects, execute),
- **private worker boundaries** — the server coordinates, your own workers execute,
- **isolated environments** built by the daemon before a run,
- an **auditable run history**.

## How it fits together

| Piece | What it does |
| --- | --- |
| `spl.core` | Serializes Python functions and pipelines to a portable SPL/YAML form. |
| `SPLClient` | The user-facing client: publish, call, manage libraries and runs. |
| `spl-daemon` | A local runtime that stores objects, builds environments, and executes workers. |

## Development

```bash
git clone https://github.com/yastrebovks/splime
cd splime
pip install -e '.[test]'
pytest
```

## Project status

Alpha (`0.1.0`). The API may change between releases. Feedback and issues are welcome at
the [issue tracker](https://github.com/yastrebovks/splime/issues).

## Links

- Website: https://splime.io
- Source: https://github.com/yastrebovks/splime

## License

Licensed under the [Apache License 2.0](LICENSE).
