# splime Roadmap

_Last updated: 2026-08-25, on the 0.4.x line. This document describes direction, not promises: design decisions listed here are fixed, but scope and timing of unreleased items can change. Feedback directly shapes this list — [issues](https://github.com/yastrebovks/splime/issues) are the best place to push back._

## Where splime is today (shipped, 0.4.x)

splime turns trusted Python functions into versioned, portable **nodes** that can be reused across projects and executed locally, on a private worker, or straight from a public catalog, with no daemon and no account.

The project grew out of a 2025 [Ask HN about code that is valuable but "not an app"](https://news.ycombinator.com/item?id=43667887) — an ML model, a CLI tool, a pile of small functions in several languages. The lesson from that thread still shapes the design, though we understand it more precisely now: people rightly refuse to *delegate trust* to a stranger's code, which is a statement about execution, not about distribution. So splime removes friction inside a trusted team, and the public catalog distributes signed, immutable releases that run only on the user's own machine after explicit consent — trust stays where it belongs.

- **Local-first core.** A local daemon with a SQLite registry; nodes identified by `(owner, library, name)`, versions content-addressed by hash. Publishing identical code twice does not create a new version.
- **Deep Python integration.** Functions are captured from live objects: AST for ports, bytecode walk for dependencies, automatic distribution pinning from the author's environment.
- **File-edge transport with tags.** Non-JSON values travel between nodes as file artifacts through save/load adapter halves. The tag is the contract and is validated before any bytes are read; adapter resolution is layered (port default → pipeline → edge → run override) and the chosen level is recorded.
- **Dependency and runtime environments.** Environments are built per spec hash with `uv venv --relocatable` + `uv pip install --strict` (pip fallback). Supported function nodes run through a stdlib-only runner in a venv where splime itself is not installed. Native and venv-subprocess execute trusted code under the conductor's OS identity (the daemon user for daemon-managed runs); dependency/process separation is not an OS sandbox. Docker (with `--network none` by default) or a separate OS identity provides the boundary for code that must not read same-UID daemon files, subject to configured mounts and Docker-host trust.
- **Runs as data.** `keep="on_failure"` retains failed local runs with versioned manifests and deterministic node fingerprints. `resume` recomputes a chosen node set plus descendants while frozen results are digest-validated; every resume is a new run with `parent_run_id` lineage.
- **Public Objects and Library Adapters (introduced in 0.4.8).** Any Function, Pipeline or Library Adapter whose dependency closure passes a strict eligibility check — no `NodeRemote`, no private dependencies, no declared secret or credential bindings, pure-Python pinned dependencies from official PyPI only — can be activated as an immutable, Ed25519-signed public release. Every Public Object has a permanent page (`splime.io/@owner/library/name`). Blocked candidates get a stable `PUBLICATION_BLOCKED` error naming the path in the graph, the reason and the fix; dependencies are never auto-published.
- **Embedded execution (introduced in 0.4.8; current runtime contract in 0.4.10).** `SPLClient.embedded()` resolves a `splime://` reference, verifies the signed manifest and bundle hashes, builds a content-addressed virtual environment and executes locally through the installed `splime` framework — no separate public-worker distribution, daemon, machine registration or account. First execution of an exact release requires explicit `trust=True` bound to that content hash; a new version means a new decision. Pinned cached releases run offline. A venv isolates dependencies; it is not an OS sandbox.
- **Team layer.** A coordination server and Console (managed, not open-source) handle users, teams, tokens, machines, libraries, grants, and remote runs. Worker machines enroll outbound and poll for work — there is no inbound "execute this" port on a worker.

The framework, the local daemon and the embedded runtime are open source (this repository, `pip install splime`). The coordination server, the catalog service and the Console are the managed part of the project.

## Next: 0.5.0 — command nodes and the first non-Python language

The 0.4.x model — file edges with tags, per-node runtimes, run manifests — was built to make multi-language support a natural extension rather than a rewrite. 0.5.0 turns that into practice with **three integration tiers**, from zero-cost to native:

### Tier 1 — Command node: any executable becomes a node

A declaration file next to any tool, no code changes inside the tool:

```yaml
name: resize-images
runtime: docker
image: python:3.12-slim
command: "python resize.py --width {width} --in {photos} --out {thumbs}"
inputs:
  photos: {tag: zip}
  width:  {tag: json, default: 512}
outputs:
  thumbs: {tag: zip}
deps: {kind: pip, lockfile: requirements.lock}
```

The port name doubles as the `{placeholder}` in the command — that is the only new concept a user learns. Scalars travel JSON-inline without files; everything derivable is derived; validation errors point to the line and to the fix. `spl node init` scaffolds a commented declaration, `spl node test --local` round-trips it before registration. For pipelines, manifests, resume, and observability a command node is indistinguishable from a Python node.

### Tier 2 — Declared foreign node: code + declaration + native lockfile

Code in language X, a port declaration, and the ecosystem's own dependency manifest. The dependency closure is fixed by the native toolchain (lockfile), the interface is declared by the author, and the stored version is `hash(declaration + code artifact + lockfile)` — changing code or dependencies must change the version.

`spl node from-git <url> --rev <commit>` wraps an existing repository as a node: pinned commit, content hash, declaration from the repo (or supplied at import). Honest boundaries: tools with reproducible runs fit best, a "file in → file out" CLI is the ideal case, a library without a CLI needs a small wrapper — and **importing does not make code trusted**; the trust boundary stays at registration by a team that trusts the code.

### Tier 3 — Thin SDK for one pilot language

The SDK does three things only: declare ports in code, generate the declaration file, implement the runner protocol. No code parsing, no magic. One pilot language first; a second is a separate, evidence-based decision after the pilot is validated.

### The decision behind all three: declarations instead of parsers

Recursive dependency discovery is a Python privilege — it works because Python has live-object introspection. It does not transfer to other languages, and we will not build semantic parsers of foreign languages to fake it. Foreign nodes are **artifact + declaration + lockfile**. The asymmetry — Python deep, other languages declared — is a deliberate position, not technical debt.

### Also in 0.5.0

- **One runner protocol, several implementations.** Command nodes and SDK nodes speak the same file protocol the Python stdlib runner uses today (`input.json` → call → `artifacts/` → `result.json`).
- **Observability for foreign nodes.** The run report shows the final substituted command, runtime and image/env, applied edge tags; the tool's stderr is delivered in full.
- **Trust hygiene.** Declarations cannot contain secrets; environment passthrough is an explicit allowlist; the `system` runtime requires explicit confirmation at registration.
- **A real approval lifecycle for remote runs** — pending → approved/rejected states with authorization, expiry, and an audit trail, replacing a setting that previously looked like a control but was not enforced end-to-end.

### Invariants

The Python path does not degrade — not in developer experience, not in performance. All YAML changes are additive; existing objects keep loading byte-identically.

## Explicitly out of scope

To keep expectations honest, things splime is **not** heading toward in this cycle:

- **selling code.** The public catalog is a distribution channel, not a marketplace: objects are free, releases are signed, and nothing in this cycle adds prices, purchases or paid tiers to catalog entries;
- **arbitrary-code delegation** — no execution of unreviewed code on someone else's workers, in any tier. Team nodes are packaged and executed by a team that trusts their code; a public object executes only on the machine of a user who explicitly accepted that exact signed release;
- an any-language platform before the single pilot SDK language is validated in practice;
- semantic parsers of foreign languages (rejected as a model; a targeted revisit for one high-value language is possible only after the declaration path is validated);
- replacing orchestrators (Airflow/Prefect/Temporal) or distributed-compute engines (Ray) — different layer, different job.

## Feedback

The fastest way to influence this roadmap is to try the 0.4.x release (`pip install splime`, Python 3.13+), publish a first public object, and open an issue: what you wrap first, where the declaration format fights you, which language should be the pilot SDK, whether foreign nodes should eventually reach the public catalog. Design documents live in `docs/` in this repository.
