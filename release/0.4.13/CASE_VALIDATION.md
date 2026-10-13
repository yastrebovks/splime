# Case4 and Case5 release validation — 2026-10-14

The release workspace stores executed notebooks, logs and local-only data in
`artifacts/release-0.4.13-20261014`. None of those input tables, prediction rows,
test homes, private keys or server wheels are public release assets.

Case notebook guards and requirements now select SDK 0.4.13. The original
notebooks were not overwritten with test output. Publication switches were
disabled in isolated execution copies; existing public references were consumed
anonymously with run receipts disabled. Case5's author notebook now keeps its
local registry after publication because chunked catalogue synchronization no
longer requires the legacy 0.4.12 cleanup workaround.

## Case4

- All four notebooks execute from fresh kernels: fixture acquisition, analysis,
  HTML rendering/export-import equality, and publication dry-run.
- All five previously published immutable `@1` objects in
  `@splime.project/case4-hn` execute against one real Hacker News snapshot.
  The separate stages and complete public pipeline produce identical HTML.
- The combined Case4 and Case5 packaging regression suite passes 52 tests and
  64 AST/parity subtests.

## Case5

- The full original notebook executes all 51 code cells, including its 71 core
  assertions and both signed local-registry workflows. The author notebook
  trains locally, exports all 14 artifacts, verifies frozen-model round trips
  and performs publication dry-runs.
- The six local source files join one-to-one by `application_id`: 566 rows,
  27,392 joined columns, 26,942 eligible candidate features. Dataset content
  stays local. The target is `first_pd1`; other outcomes and identifiers are
  excluded from predictors.
- Consumers use independently installed SDK wheels, separate empty caches and
  kernels, without author imports, daemon identity or registration. They execute
  full rule search, frozen-model prediction and composition of all reusable
  stages using the actual existing SPH references.
- The pinned environment uses NumPy 2.3.5, pandas 2.2.3, scikit-learn 1.8.0 and
  PyArrow 25.0.1. The alternate environment uses NumPy 2.5.4, pandas 3.0.6,
  scikit-learn 1.9.1 and PyArrow 26.0.0. Both run Python 3.13.14.
- All 566 frozen-model predictions match exactly across environments. Refit
  predictions and metrics also match on this dataset, but the model identity
  changes from `8dd0bd46cbd9168c94d6` to `721126b87c8f1f3614a2`; retraining across
  library versions is not promised to be bit-identical.
- The later holdout contains 191 applications: ROC AUC 0.6757576, macro-F1
  0.6330149, accuracy 0.6335079. Point-in-time availability of source features
  remains unconfirmed: these are research backtest results, not prospective
  performance guarantees.

## Failure discovered and repaired

Two concurrent consumer environments initially hit the production registry's
HTTP 429 limit while downloading the 14-object catalogue. The old transport
failed immediately. SDK 0.4.13 now retries idempotent GET requests, respects
`Retry-After` (seconds or HTTP date), emits a short wait warning, and stops after
three retries or its 65-second retry budget. Longer waits fail explicitly with
`public_registry_rate_limited` and HTTP status 429. POST receipts are not replayed.

Eighteen new transport regressions cover real HTTP recovery, finite retries,
oversized waits, malformed/date headers, elapsed-time bounds, response closure,
credential-free requests, redirects, other failures and receipt non-replay.
They run in the installed-wheel Linux/macOS/Windows publication matrix.

## Other local gates

- Server: 1,173 tests passed.
- Console: 626 tests and 35 Chromium scenarios passed; retained archive source
  bytes match the reviewed source. Final archive build identity is generated
  again from the immutable Console commit.
- Landing: static contract and ten Node tests passed.
- Real loopback author/server/consumer verification passed with schema 49,
  all 14 retained Case5 objects and an author token lacking `teams:read`.
- Installed SDK follow-up: 292 tests passed outside the checkout. The optional
  pytest-asyncio configuration warning does not represent a skipped test.
- Mypy with the declared toolchain passes all 138 SDK source modules.

The first full SDK run exposed a missing optional test-environment dependency
(`openpyxl`), not a runtime regression. It was installed before the final full
suite rerun. Final full-suite results, signed source identities, published bytes
and deployment receipts belong to the final release evidence; this document
does not substitute for those external gates.
