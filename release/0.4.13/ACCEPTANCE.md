# Splime 0.4.13 — исправление diagnostic metadata

> Дополнение к SDK: компактные сообщения о зависимостях и
> `client.inspect_dependencies(ref)` собраны отдельно в
> `candidate-0.4.13-20261013-dependency-diagnostics`.
> Его артефакты и проверки описаны в [DEPENDENCY_DIAGNOSTICS.md](DEPENDENCY_DIAGNOSTICS.md).
> Приведённые ниже хеши и полные прогоны относятся к базовому кандидату;
> они не обозначаются как повторные проверки нового SDK.

Базовый локальный кандидат: **candidate-0.4.13-20261013-diagnostic-metadata**, версия **0.4.13**.
Все требуемые локальные проверки завершены. Кандидат готов к следующему
этапу release review; внешняя готовность остаётся заблокирована перечисленными
ниже source/CI/publication/deployment gates. Signed-source или publication
receipts не создавались и не подменялись локальной инвентаризацией.

Корень: `/Users/kirill/Projects/SPL_v2/artifacts/candidate-0.4.13-20261013-diagnostic-metadata`. Пути ниже относительны этому корню.
Выбранные байты: `evidence/candidate-artifacts.json`, `SHA256SUMS`.
Новые результаты: `evidence/validation-summary.json`; точные команды:
`evidence/validation-commands.json`.

Предыдущий `candidate-0.4.13-20261013-review-fixes` полностью остаётся
историческим: его SDK и результаты не подтверждают новое исправление.
Точные server/Console artifacts сохранены после нового независимого сравнения
входных файлов. Исторические **626 Console tests и 30+5 Chromium scenarios**
не запускались повторно и не переобозначены как новые проверки. Более ранние
`candidate-0.4.13-20261013/finalv5` и `finalv4` также остаются историей.

## Причина и границы исправления

`sync_failure()` выполнял set membership для upstream `category` без проверки
типа. JSON array/object вызывал `TypeError`. Receipt validator пропускал поле,
а нормализация всего mixed batch происходила перед применением подтверждений.
Поэтому валидный ACK не сохранялся, а реальный HeartbeatService записывал
`heartbeat_failed` и `central_server_unreachable`, несмотря на успешный heartbeat.

Изменены четыре production files в SDK:

- `src/spl/daemon/remote_client.py`: per-receipt type validation известных
  optional diagnostic fields (`category`, `operation`, `recommended_action`,
  `snapshot_id`, byte counters), с сохранением существующих ID/kind/status/code
  checks. Повреждённая receipt исключается отдельно; независимые валидные ACK
  сохраняются. Её `retryable=false` не является основанием окончательно
  отклонить событие. Пропущенные optional fields, неизвестные строковые значения
  и неизвестные additive keys остаются совместимыми.
- `src/spl/daemon/sync_diagnostics.py`: category/status проверяются до set
  membership; bool не принимается за HTTP integer, `HTTPStatus` поддерживается.
  Response marker требует `True`, counters ограничены 0..2^63−1, codes/actions
  и IDs ограничены длиной и безопасным форматом. Nested payloads и error text
  не копируются. Подтверждённые authentication/authorization и локальные
  transport/invalid-response признаки приоритетнее противоречащих body hints.
- `src/spl/daemon/heartbeat_service.py`: сохраняется нормализованный code
  вместо произвольного HTTP error message; при преобразовании partial failure
  code сохраняется. Lease rejection/reconnect и backoff semantics прежние.
- `src/spl/daemon/server.py`: две записи сырого error message в существующей
  ветке authoritative HTTP409 identity collision заменены нормализованным code.
  Правильное collided event остаётся permanently failed; независимое событие
  сохраняет возможность доставки.

Это не blanket exception suppression. Ранее исправленные malformed-JSON sync,
targeted commercial-cache recovery и account/session fencing сохранены.
Console/server production inputs и их version metadata не менялись.

## Fail-before / pass-after

Основные 65 новых regression tests находятся в существующем
`spl/tests/daemon/test_sync_response_integrity.py`, поэтому входят в полный SDK
suite и уже подключённый installed-wheel runner.

| Этап | До соответствующего исправления | После |
|---|---:|---:|
| Основная metadata/type/auth precedence проверка | 41 failed, 12 passed, 61 deselected | 53 passed в первом green run |
| Утечка HTTP error text через heartbeat diagnostics | 8 failed, 2 passed, 114 deselected | Включена в итоговые 65 passed |
| HTTP409 collision diagnostic text | 2 failed, 124 deselected | Оба сценария passed |
| Все новые regressions после последней правки | — | **65 passed**, 0 failed/errors/skipped; 61 deselected |
| Affected sync/heartbeat/catalogue suites | — | **420 passed**, 0 failed/errors/skipped |

Red evidence: `logs/daemon-metadata-red.*`,
`logs/daemon-metadata-heartbeat-red.*`, `logs/daemon-metadata-collision-red.*`.
Отдельная ошибка команды collision test из неверного cwd сохранена как
command-error evidence и не считается воспроизведением дефекта. Реальный red
run выполнен из `spl` до правки collision branch. Промежуточные 63/418 green
logs сохранены как `logs/*-pre-collision.*`, выбранные финальные —
`logs/daemon-metadata-final.*` и `logs/daemon-focused-final.*`.

Первый полный SDK run после production fix дал **2752 passed, 2 failed,
0 errors/skipped**: два прежних lease-rejection tests ожидали сохранения сырого
текста upstream error. Reconnect-state checks уже проходили. В
`tests/daemon/test_server_connection.py` ожидания заменены точными безопасными
кодами; добавлены проверки отсутствия private sentinel и token, прежние проверки
reconnect, identity и сохранения credentials оставлены. Затем весь файл дал
**16 passed**, а полный SDK suite
повторён с результатом ниже. Неуспешный run сохранён в
`logs/sdk-full-before-safe-error-assertions.log` и XML. После этого run менялись
только tests/reporting: production code и package inputs остались прежними,
поэтому результаты сборки, installed-wheel и HTTP acceptance актуальны.

Principal tests используют literal HTTP response bytes на disposable loopback
server, настоящий DaemonRuntime и настоящий HeartbeatService loop. Подменён
только wait seam, чтобы наблюдать интервалы без реального ожидания минут.
Для array/object category mixed response даёт `partial=True`,
`invalid_response`, первый event становится `sent`, второй остаётся retryable.
Наблюдаемые batches: оба IDs → только второй ID → только второй ID → пусто.
Исходный ID сохраняется, эффекты дедуплицируются, после завершения новой доставки
нет. Все четыре heartbeats оставляют connection `connected`; ложного
`central_server_unreachable` нет. Интервалы **0.1 → 0.2 → 60 → 60 seconds**
доказывают backoff и reset после recovery.

Structured HTTP401/403/404/409/502 error tests проверяют safe stored diagnostics
и сохранение подтверждённой HTTP information; direct JSON-type tests дополняют,
а не заменяют настоящий HTTP/runtime путь. Legacy omissions и unknown typed
categories поддержаны. Genuine transport/auth/permanent failures, poison
isolation, claim fencing, no-claim и catalogue behavior покрыты affected suites.

## Новые проверки этой ревизии

В командах `C` — абсолютный корень кандидата выше, `P=$C/envs/test/bin/python`.
Рабочий каталог `spl`, кроме явно указанного `spl-server`. Наследованные
`SPL*`/`PYTHON*` variables удалены; все установочные environments новые.
Счётчики пересекаются и не суммируются в число уникальных tests.

| Проверка / команда | Passed | Failed / errors / skipped | Evidence |
|---|---:|---|---|
| SDK: `$P -m pytest -q --junitxml=$C/logs/sdk-full.xml` | 2754 | 0 / 0 / 0 | `logs/sdk-full.log`, XML |
| Server: `$C/envs/server-test/bin/python -m pytest -q --junitxml=$C/logs/server-full.xml`, cwd `spl-server` | 1173 | 0 / 0 / 0 | `logs/server-full.log`, XML |
| New metadata: `$P -m pytest -q tests/daemon/test_sync_response_integrity.py -k diagnostic_metadata` | 65 | 0 / 0 / 0 | `logs/daemon-metadata-final.log`, XML |
| Affected suite, полная команда в `evidence/daemon-review.md` | 420 | 0 / 0 / 0 | `logs/daemon-focused-final.log`, XML |
| `$P -m pytest -q tests/daemon/test_server_connection.py` | 16 | 0 / 0 / 0 | `logs/server-connection-followup.log`, XML |
| `$P -m pytest -q tests/core/test_release_workflows.py tests/core/test_release_chain.py` | 35 | 0 / 0 / 0 | `logs/server-ci-workflows.log`, XML |
| Matching: `$C/envs/matching/bin/python -I integration_tests/installed_contract.py` | 265 | 0 / 0 / 0 | `logs/installed-matching.log` |
| Alternate: `$C/envs/alternate/bin/python -I integration_tests/installed_contract.py` | 265 | 0 / 0 / 0 | `logs/installed-alternate.log` |

Ruff check и format check passed; mypy `--python-version 3.13 src` —
138 source files без ошибок. `tools.generate_release_identity --check` и
`tools.release_chain --stage contract` passed. CLI clean install сообщает
`spl-daemon 0.4.13`. Static logs: `sdk-ruff-final`, `sdk-format-final`,
`sdk-mypy`, `identity-final`, `contract-final`, `clean-sdk-version`.

Installed warning counts: matching 0,
alternate 0; их фактическое содержание
сохранено в полных logs, failures/errors/skips отсутствуют. Общие временные
каталоги пользователя вручную не удалялись. Source SDK warnings также остаются
в полном log; warnings не скрываются и не считаются skipped tests.

Server suite использует текущие server source files, побайтно совпадающие с
retained wheel, новый installed SDK без SDK source override и NumPy 2.1.3
согласно server test dependency set. HTTP acceptance отдельно запускает
установленный server wheel. Независимо
проверены четыре установленных изменённых модуля против wheel/source:
`evidence/server-installed-sdk-identity.json`.

## Installed-wheel и real HTTP acceptance

Matching: **NumPy 2.3.5 / pandas 2.2.3**; alternate:
**NumPy 2.5.4 / pandas 3.0.6**. В каждой матрице тесты копируются вне checkout,
interpreter запускается с `-I`, homes/caches отдельные; `spl` и четыре новых
модуля загружаются из final wheel в site-packages. Editable/shared daemon
imports не используются. Число tests: 69 native/identity + 70 catalogue +
126 sync-response = 265 на каждую dependency set.

Новый `release_candidate_packages.py` run использует отдельные installed author
и два anonymous SDK-only consumer environments, synthetic signing keys и
disposable loopback registry. **221 HTTP requests**, 14 releases
опубликованы локально и сохранены; **1,032,538 YAML bytes**.
Наибольший catalogue request — **65,771 bytes**
при лимите **131,072**. Restricted whoami
успешен, `/users` forbidden. Оба consumers проверяют exact signed manifests и
bundles, frozen predictions, cold/warm behavior, запрет network при warm run,
и отдельное обучение на synthetic rows. Доступов к author/shared state нет.

Повторный **Case5** run: **14 actual bundles**, **492,789 YAML
bytes**, **73 HTTP requests**; все 14 синхронизированы,
публично активированы в отдельном локальном registry и сохранены.
Это catalogue synchronization/publication acceptance. **Prediction, training
и rule search на приватном Dataset не выполнялись; Dataset rows не читались.**
Synthetic execution — отдельная проверка, не доказательство Dataset execution.

Команды используют `$C/envs/author/bin/python -I
integration_tests/release_candidate_packages.py`, новый `--root`, точный
`--author-python` и два `--consumer-python`; Case5 отдельно использует
`--case5-catalog /Users/kirill/Projects/SPL_v2/Demo/Case5/sph_exports/catalog.json`.
Полные аргументы в command inventory. Новые receipts:
`integration-synthetic/summary.json`, `integration-case5/summary.json` и logs.
Production, существующий Public Hub и пользовательские credentials не использованы.

## Артефакты, сохранённые результаты и provenance

| Selected path | Состояние / доступ | SHA-256 |
|---|---|---|
| `built/python/splime-0.4.13-py3-none-any.whl` | Новая сборка; public | `fa0c1eaa8e2415e116df73830ea3f37cf68a55c526731d5cf018b5cd27ed64dd` |
| `built/python/splime-0.4.13.tar.gz` | Новая сборка; public | `9ab3cfdb23993eb26a6372d0b76e667f84abd6de92df860106649e8bfc7bbfde` |
| `built/server/spl_server-0.4.13-py3-none-any.whl` | Сохранённые байты; private | `1a47d795c8fd31c8ef4df7336a712450c3cdb0411d28ef624a74f636e5379a53` |
| `built/splime-console-0.4.13.tar.gz` | Сохранённые байты; public | `fb51bb87e6227111b4909e47a32e8745dcb54fcb995b4e345c5f4408aafe7ddb` |
| `built/console/static-integrity.json` | Сохранённые байты; public | `2b82460f4c6f11ff1ab6e457670afc5682f52fa6374ba343dd3d9dc7a17665df` |

SDK wheel/sdist вновь собраны дважды repository build tool с локальным
`source_date_epoch=1791892800`; обе пары совпадают побайтно. Это параметр
воспроизводимости, а не signed-source authority. Wheel package members и
sdist source files совпадают с final source; PKG-INFO проверен против wheel
METADATA. Clean installation и `uv pip check` для clean-sdk, author, matching,
alternate и server-test прошли. Imports/version и installed-source hashes
зафиксированы в `evidence/clean-install.json` и installed receipts.

Server wheel и Console archive не пересобирались: новое сравнение подтвердило
неизменность source/packaging inputs и retained bytes. Проверены 88 server
source members, wheel RECORD, 109 Console files и 108 integrity assets.
Console exact build ID прежний:
`sha256:37b83cb62ea930273e4bb3ac91d403c2ec93c91413f2c0edb4bf23c4ad6051b9`.
Предыдущая reproducibility server/Console сохранена как историческое evidence.
Полные bindings и прежние Console/browser log paths:
`evidence/retained-components.json`, `evidence/retained-components.md`.

Сохранность 914 файлов
предыдущего candidate проверена по исходным SHA-256; все шесть его environment
inventories совпадают. HEAD/branches и unrelated existing file hashes сохранены,
включая legacy repositories, mirror, landing и пользовательскую документацию.
Notebooks, datasets, running shared daemon/Jupyter и existing Hub publications
не изменялись. Предыдущие reports скопированы в `historical-reports/`.

## CI и оставшиеся внешние gates

Новые tests автоматически входят в существующий explicit installed runner:
он копирует и собирает весь `test_sync_response_integrity.py`, а также catalogue
и native suites. SDK-only/public PR matrix уже выполняет его на Linux/macOS/
Windows × две dependency sets. Full SDK CI также собирает эти tests. Новые
35 workflow/release-chain checks подтверждают wiring; workflow менять не
потребовалось. Full server pytest остаётся в server GitLab job.

Mandatory coordinated author/registry/two-consumer harness и полный Console
check остаются подключены перед public artifact upload. Private server wheel
и fixture state не входят в public allowlist. **Remote CI не запускался**;
локальные тесты не представлены как результаты GitHub/GitLab jobs.

Новое выполнение source gate ожидаемо заблокировано отсутствующим `v0.4.13`;
успешный source manifest не создан (`evidence/source-gate.json`). Дальше нужны:

1. Reviewed component commits/public mirror и проверенные immutable signed refs.
2. Remote OS/dependency matrix и private coordinated CI, publisher/protected
   environment configuration.
3. Отдельно разрешённая SDK index publication, после неё — обновление server
   index lock (сейчас намеренно 0.4.12), review/commit и первый server release tag.
4. Docker platform execution, real auth-provider/production nginx/CDN checks,
   затем отдельно разрешённые release assets и deployment.

Публикация пакетов, push tags, deployment и изменение внешних Public Hub
objects не выполнялись. Local inventory не является signed-source, CI,
publication или deployment receipt. Release sequence и rollback — в
`RELEASE.md`. Candidate directory содержит private server и test homes;
public transfer возможен только по явному allowlist.
