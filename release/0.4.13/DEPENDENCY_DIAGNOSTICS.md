# 0.4.13 SDK follow-up: compact public dependency diagnostics

Local SDK artifact directory:
`artifacts/candidate-0.4.13-20261013-dependency-diagnostics` (workspace-relative).
This follows the coordinated `candidate-0.4.13-20261013-diagnostic-metadata`.
The base SDK wheel does not contain these changes. Server and Console sources,
artifacts, the user's Jupyter environments and shared daemon were not modified.
Nothing was published, tagged or deployed.

## Behavior

Public `current-process-v1` calls emit one warning per call, with at most ten
dependency entries. Rows contain package, publication version, installed version
and any already-loaded version evidence. Component paths and per-row prose are
omitted. A hidden-entry count and the inspection method explain how to obtain
the full list. Matching versions remain silent. Missing-package errors use the
same bounded preview and still prevent execution; their existing complete
`dependencies` and `missing` payloads remain compatible.

`SPLClient.embedded().inspect_dependencies(ref)` returns a complete ordinary
list of JSON-serializable dictionaries: `package`, `recorded_version`,
`installed_version`, `loaded_versions` and `status`. There is no truncation or
source path in this export. Multiple published versions of one package remain
separate records. Installed and already-loaded versions stay distinct, and the
kernel-restart hint is retained when their verified versions differ.

Inspection uses the existing signed-release verification and cache, fetches
on a cold cache, and reads current interpreter metadata on every invocation.
It does not execute the artifact, approve trust, import its dependencies,
emit run receipts, install packages or start a daemon. Isolated legacy releases
and daemon-mode clients return `feature_not_supported` for this API.

```python
from spl import SPLClient
import json
from pathlib import Path

client = SPLClient.embedded(run_receipts=False)
ref = "splime://@owner/library/artifact@1"  # replace with your pinned reference
dependencies = client.inspect_dependencies(ref)
differences = [item for item in dependencies if item["status"] != "present"]
Path("dependencies.json").write_text(
    json.dumps(dependencies, ensure_ascii=False, indent=2), encoding="utf-8"
)
```

## Validation performed for this follow-up

- Public API, embedded execution, references and result access: **179 passed**.
  One expected warning comes from the existing duplicate ZIP-member negative test.
- Current-process and result-access tests on alternate source dependencies
  (NumPy 2.5.4 / pandas 3.0.6): **77 passed**.
- Fresh SDK-only environment with the new wheel and the alternate dependencies:
  **78 passed**, including wheel/source identity for all 138 Python modules.
  Tests were copied outside the checkout and run with isolated Python.
- Ruff and formatting checks on changed Python files passed; mypy passed for
  all **138 source files**. `pip check` in the fresh environment passed.
- Producer/consumer interoperability passed. The producer uses the retained
  base author environment; the consumer uses this new SDK wheel without the
  private server package. A signed DataFrame artifact executes twice, preserving
  native values and kernel state. Each call emits one two-row warning for
  NumPy/Pandas; inspection itself emits no warning or run receipt and grants no trust.

Nine new tests cover bounded output with 125 entries, full independent exports,
missing packages, loaded/disk version differences and equivalence, unusual long
version metadata, inspection without execution/trust, invalid signatures, fresh
version reads, and rejection of unsupported modes/profiles. Existing assertions
and the package interoperability harness were updated for the new output.
The harness's consumer previously required installed `spl-server` metadata;
that irrelevant producer-only requirement was removed after a fresh SDK-only
consumer exposed it. The initial failed harness log is retained separately.

The wheel and source archive were built through `tools.build_release_artifacts`
from a disposable source copy with local `SOURCE_DATE_EPOCH=1791892800`.
No earlier candidate artifacts or environments were overwritten. This build
was not repeated for a new reproducibility claim. The full SDK/server suites,
Console browser suite, remote OS matrix and production infrastructure were not
rerun for this focused follow-up. External release gates from the base acceptance
still apply; this document does not declare publication readiness.

## Selected SDK artifacts

| Artifact | SHA-256 |
| --- | --- |
| `built/python/splime-0.4.13-py3-none-any.whl` | `c9ccbf57dad0f87d10b8581b4dba734c020742e204d031a84a6ca24849fd63bd` |
| `built/python/splime-0.4.13.tar.gz` | `1434ea2e1458417daff8a3ce75a86dd6e012cf76c811cb73b1391889bc7ea1c9` |

Paths are relative to the SDK artifact directory above. `SDK_FOLLOWUP.json`,
`SHA256SUMS` and `logs/` there record the selected files and installed-wheel
evidence. Only explicitly selected SDK wheel/sdist files are release assets;
the environment and evidence directory are not publication assets.
