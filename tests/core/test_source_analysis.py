from __future__ import annotations

import ast
import builtins
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from spl import analyze_selected_code, inspect_active_kernel_environment
from spl.core import source_analysis as m_source_analysis
from spl.core.source_analysis import (
    ACTIVE_KERNEL_ENVIRONMENT_REQUEST_SCHEMA,
    SELECTED_CODE_ANALYSIS_REQUEST_SCHEMA,
    SourceAnalysisContractError,
)
from spl.core.source_preparation import GENERATED_PLAN_SCHEMA, PREPARE_SOURCE_REQUEST_SCHEMA, prepare_source


_PROMPT16_FIXTURES = Path(__file__).with_name("prompt16") / "fixtures"


def _request(
    source: str,
    *,
    kind: str = "cell",
    function_name: str = "generated_function",
    supporting: list[dict[str, str]] | None = None,
    downstream: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        "schema": SELECTED_CODE_ANALYSIS_REQUEST_SCHEMA,
        "schema_version": 1,
        "document": {"id": "notebook-1", "revision": "revision-7"},
        "selection": {"kind": kind, "sources": [{"cell_id": "cell-1", "source": source}]},
        "function_name": function_name,
        "supporting_import_sources": supporting or [],
        "downstream_sources": downstream or [],
    }


def _ready(source: str, **kwargs: Any) -> dict[str, Any]:
    result = analyze_selected_code(_request(source, **kwargs))
    assert result["status"] == "ready", result["diagnostics"]
    assert result["function"] is not None
    return result


def _environment_request(analysis: dict[str, Any], roots: list[str]) -> dict[str, Any]:
    binding = analysis["binding"]
    return {
        "schema": ACTIVE_KERNEL_ENVIRONMENT_REQUEST_SCHEMA,
        "schema_version": 1,
        "analysis_binding": {
            "analysis_hash": analysis["analysis_hash"],
            "selected_source_hash": binding["selected_source_hash"],
            "generated_source_hash": binding["generated_source_hash"],
            "document_id": binding["document_id"],
            "document_revision": binding["document_revision"],
        },
        "kernel": {"id": "kernel-123", "name": "python3"},
        "import_roots": roots,
    }


def test_preserves_supported_top_level_function_and_binding() -> None:
    source = "import numpy as np\n\ndef score(x):\n    # preserved marker\n    return np.asarray(x)\n"

    first = _ready(source, function_name="ignored_for_preserved_function")
    second = _ready(source, function_name="ignored_for_preserved_function")

    function = first["function"]
    assert function["name"] == "score"
    assert "# preserved marker" in function["source"]
    assert function["inputs"] == [{"name": "x", "annotation": None, "required": True, "has_default": False}]
    assert function["outputs"] == [{"name": "default", "annotation": None}]
    assert function["import_roots"] == ["numpy"]
    assert function["transformation"] == "preserved_function"
    assert first["analysis_hash"] == second["analysis_hash"]
    assert first["binding"]["selected_source_hash"].startswith("sha256:")
    assert first["binding"]["generated_source_hash"].startswith("sha256:")


def test_wraps_imperative_selection_with_free_input_and_proven_output() -> None:
    result = _ready(
        "import numpy as np\nscaled = np.asarray(values) * factor\n",
        function_name="scale_values",
        downstream=[{"cell_id": "cell-2", "source": "print(scaled)\n"}],
    )

    function = result["function"]
    assert function["inputs"] == [
        {"name": "values", "annotation": None, "required": True, "has_default": False},
        {"name": "factor", "annotation": None, "required": True, "has_default": False},
    ]
    assert function["outputs"] == [{"name": "default", "annotation": None}]
    assert "def scale_values(values, factor):" in function["source"]
    assert "return scaled" in function["source"]
    assert function["import_roots"] == ["numpy"]


def test_multiple_selected_cells_keep_order_and_self_update_becomes_input() -> None:
    request = _request("placeholder = 0\nplaceholder\n", kind="cells", function_name="update")
    request["selection"]["sources"] = [
        {"cell_id": "cell-a", "source": "value = value + increment\n"},
        {"cell_id": "cell-b", "source": "result = value * 2\nresult\n"},
    ]

    result = analyze_selected_code(request)

    assert result["status"] == "ready", result["diagnostics"]
    assert result["function"]["inputs"] == [
        {"name": "value", "annotation": None, "required": True, "has_default": False},
        {"name": "increment", "annotation": None, "required": True, "has_default": False},
    ]
    assert result["function"]["source"].index("value = value + increment") < result["function"]["source"].index(
        "result = value * 2"
    )


def test_supporting_import_is_visible_in_generated_source_not_an_input() -> None:
    result = _ready(
        "answer = np.asarray(values)\nanswer\n",
        supporting=[{"cell_id": "cell-import", "source": "import numpy as np\n"}],
    )

    function = result["function"]
    assert function["source"].startswith("import numpy as np\n")
    assert function["inputs"] == [{"name": "values", "annotation": None, "required": True, "has_default": False}]
    assert function["import_roots"] == ["numpy"]


def test_duplicate_equivalent_import_binding_is_not_a_conflict() -> None:
    result = _ready(
        "def optimize(trials):\n    return fmin(score, hp.uniform('x', 0, 1), trials=trials)\n",
        supporting=[
            {"cell_id": "import-a", "source": "from hyperopt import hp\n"},
            {
                "cell_id": "import-b",
                "source": "from hyperopt import fmin, hp, tpe\n",
            },
            {"cell_id": "helper", "source": "def score(value):\n    return value\n"},
        ],
    )

    assert result["function"]["inputs"] == [
        {"name": "trials", "annotation": None, "required": True, "has_default": False}
    ]
    assert "def score(value):" in result["function"]["source"]
    assert result["function"]["import_roots"] == ["hyperopt"]


def test_static_dependency_closure_preserves_helper_constants_and_explicit_data_inputs() -> None:
    result = _ready(
        (
            "def optimize(trials):\n"
            "    space = {'depth': scope.int(hp.uniform('depth', 1, 8))}\n"
            "    return fmin(score, space, algo=tpe.suggest, trials=trials, max_evals=k)\n"
        ),
        supporting=[
            {
                "cell_id": "imports",
                "source": (
                    "import lightgbm as lgb\n"
                    "import numpy as np\n"
                    "from hyperopt import STATUS_OK, fmin, hp, tpe\n"
                    "from hyperopt.pyll import scope\n"
                    "from sklearn.model_selection import StratifiedKFold\n"
                ),
            },
            {"cell_id": "config", "source": "k = 20\np = 0.5\n"},
            {
                "cell_id": "objective",
                "source": (
                    "skf = StratifiedKFold(n_splits=3, shuffle=True, random_state=7)\n\n"
                    "def score(params):\n"
                    "    values = []\n"
                    "    for train_index, val_index in skf.split(X_train_csr, y_train):\n"
                    "        model = lgb.train(params, X_train_csr[train_index, :])\n"
                    "        values.append(model.best_score['train']['auc'])\n"
                    "    return {'loss': -np.mean(values) ** p, 'status': STATUS_OK}\n"
                ),
            },
        ],
    )

    function = result["function"]
    assert function["transformation"] == "preserved_function"
    assert function["inputs"] == [
        {"name": "trials", "annotation": None, "required": True, "has_default": False},
        {"name": "X_train_csr", "annotation": None, "required": True, "has_default": False},
        {"name": "y_train", "annotation": None, "required": True, "has_default": False},
    ]
    assert "k = 20" in function["source"]
    assert "p = 0.5" in function["source"]
    assert "skf = StratifiedKFold(n_splits=3, shuffle=True, random_state=7)" in function["source"]
    assert "    def score(params):" in function["source"]
    assert function["import_roots"] == ["hyperopt", "lightgbm", "numpy", "sklearn"]


def test_dynamic_supporting_value_becomes_explicit_input_instead_of_being_executed() -> None:
    result = _ready(
        "def optimize(trials):\n    return run_search(trials, max_evals=k)\n",
        supporting=[
            {"cell_id": "config", "source": "k = read_config()\n"},
            {"cell_id": "helper", "source": "def run_search(trials, max_evals):\n    return max_evals\n"},
        ],
    )

    assert result["function"]["inputs"] == [
        {"name": "trials", "annotation": None, "required": True, "has_default": False},
        {"name": "k", "annotation": None, "required": True, "has_default": False},
    ]
    assert "read_config" not in result["function"]["source"]


def test_prior_import_and_later_use_make_common_notebook_selection_publishable() -> None:
    result = _ready(
        "frame = pd.DataFrame(rows)\n",
        function_name="build_frame",
        supporting=[
            {"cell_id": "cell-import", "source": "import pandas as pd\n"},
            {"cell_id": "cell-unrelated", "source": "ignored = unknown(\n"},
        ],
        downstream=[
            {"cell_id": "cell-partial", "source": "print(\n"},
            {"cell_id": "cell-use", "source": "display(frame)\n"},
        ],
    )

    function = result["function"]
    assert function["source"].startswith("import pandas as pd\n")
    assert "def build_frame(rows):" in function["source"]
    assert "return frame" in function["source"]
    assert function["inputs"] == [
        {
            "name": "rows",
            "annotation": None,
            "required": True,
            "has_default": False,
        }
    ]
    assert function["import_roots"] == ["pandas"]


def test_generated_function_is_accepted_by_static_source_preparation() -> None:
    analysis = _ready("import numpy as np\nresult = np.asarray(source)\nresult\n", function_name="as_array")
    source = analysis["function"]["source"]
    prepared = prepare_source(
        {
            "schema": PREPARE_SOURCE_REQUEST_SCHEMA,
            "schema_version": 1,
            "sources": [{"logical_path": "spl_generated/as_array.py", "language": "python", "text": source}],
            "proposal_ir": {
                "schema": GENERATED_PLAN_SCHEMA,
                "schema_version": 1,
                "kind": "function",
                "name": "as_array",
                "nodes": [],
                "edges": [],
                "outputs": [],
                "dependencies": [{"package": "numpy", "version": "2.5.0", "modules": ["numpy"]}],
                "adapters": [],
            },
            "entrypoints": ["as_array"],
            "environment_request": None,
            "runtime_request": {"mode": "venv"},
            "target": {
                "python_language": "3.13",
                "spl_ir_schema": "splime.object-ir/v1",
                "preparation_protocol": 1,
            },
            "base": None,
        }
    )

    assert prepared["status"] == "ready", prepared["diagnostics"]
    assert prepared["prepared"]["signature"]["inputs"][0]["name"] == "source"


@pytest.mark.parametrize(
    ("source", "code"),
    [
        ("value = (\n", "analysis.syntax_error"),
        ("@decorator\ndef f(x):\n    return x\n", "analysis.decorator_unsupported"),
        ("def f(x):\n    def inner():\n        return x\n    return inner()\n", "analysis.closure_unsupported"),
        ("module = __import__('unsafe')\nmodule\n", "analysis.dynamic_import_or_execution"),
        ("os.remove(path)\nresult = path\nresult\n", "analysis.side_effect_unsupported"),
        ("%pip install unsafe\nvalue = 1\nvalue\n", "analysis.unsafe_magic"),
    ],
)
def test_analysis_fails_closed_with_stable_reason(source: str, code: str) -> None:
    result = analyze_selected_code(_request(source, function_name="draft"))

    assert result["status"] == "invalid"
    assert result["function"] is None
    assert code in {item["code"] for item in result["diagnostics"]}
    assert result["binding"]["generated_source_hash"] is None


@pytest.mark.parametrize(
    "source",
    [
        'api_token = "ordinary-looking-value"\nresult = source\nresult\n',
        'credential = "sk-fixture-secret-123456789"\nresult = source\nresult\n',
        'value = "Bearer fixture-token-123456789"\nvalue\n',
        'value = "-----BEGIN PRIVATE KEY-----"\nvalue\n',
    ],
)
def test_secret_like_source_fails_before_generated_source(source: str) -> None:
    result = analyze_selected_code(_request(source, function_name="draft"))

    assert result["status"] == "invalid"
    assert result["function"] is None
    assert {item["code"] for item in result["diagnostics"]} == {"source.secret_like_literal"}
    assert result["binding"]["generated_source_hash"] is None
    assert "fixture" not in json.dumps(result)


def test_secret_like_name_without_embedded_string_is_not_misclassified() -> None:
    result = _ready("token_count = source + 1\ntoken_count\n")

    assert result["function"]["outputs"] == [{"name": "default", "annotation": None}]


def test_multiple_or_unproven_outputs_fail_closed() -> None:
    multiple = analyze_selected_code(
        _request(
            "left = source + 1\nright = source + 2\n",
            downstream=[{"cell_id": "cell-2", "source": "print(left, right)\n"}],
        )
    )
    missing = analyze_selected_code(_request("value = source + 1\n"))

    assert {item["code"] for item in multiple["diagnostics"]} == {"analysis.multiple_outputs_unsupported"}
    assert {item["code"] for item in missing["diagnostics"]} == {"analysis.output_unproven"}


def test_allowed_jupyter_magic_is_normalized_but_not_executed() -> None:
    result = _ready("%time value = source + 1\nvalue\n")

    assert "%time" not in result["function"]["source"]
    assert "value = source + 1" in result["function"]["source"]


def test_contract_is_closed_and_rejects_invalid_binding() -> None:
    request = _request("value = source\nvalue\n")
    request["unknown"] = True
    with pytest.raises(SourceAnalysisContractError, match="analysis.request.fields"):
        analyze_selected_code(request)

    analysis = _ready("value = source\nvalue\n")
    environment = _environment_request(analysis, [])
    environment["analysis_binding"]["analysis_hash"] = "not-a-hash"
    with pytest.raises(SourceAnalysisContractError, match="environment.analysis_binding.analysis_hash"):
        inspect_active_kernel_environment(environment)


class _FakeDistribution:
    def __init__(self, name: str, version: str, direct_url: dict[str, Any] | None = None) -> None:
        self.metadata = {"Name": name}
        self.version = version
        self._direct_url = direct_url

    def read_text(self, name: str) -> str | None:
        assert name == "direct_url.json"
        return json.dumps(self._direct_url) if self._direct_url is not None else None


def _install_fake_metadata(
    monkeypatch: pytest.MonkeyPatch,
    mapping: dict[str, list[str]],
    distributions: dict[str, _FakeDistribution],
) -> None:
    by_normalized = {name.lower(): distribution for name, distribution in distributions.items()}

    def lookup(name: str) -> _FakeDistribution:
        try:
            return by_normalized[name.lower()]
        except KeyError as error:
            raise m_source_analysis.metadata.PackageNotFoundError(name) from error

    monkeypatch.setattr(m_source_analysis.metadata, "packages_distributions", lambda: mapping)
    monkeypatch.setattr(m_source_analysis.metadata, "distributions", lambda: list(distributions.values()))
    monkeypatch.setattr(m_source_analysis.metadata, "distribution", lookup)
    monkeypatch.setattr(m_source_analysis.metadata, "version", lambda name: lookup(name).version)


def test_active_kernel_dependency_evidence_resolves_exact_versions_and_stdlib(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_metadata(
        monkeypatch,
        {"numpy": ["numpy"], "sklearn": ["scikit-learn"]},
        {
            "numpy": _FakeDistribution("numpy", "2.5.0"),
            "scikit-learn": _FakeDistribution("scikit-learn", "1.9.0"),
        },
    )
    analysis = _ready(
        "import numpy as np\nfrom sklearn.metrics import accuracy_score\nvalue = np.asarray(source)\nvalue\n"
    )

    result = inspect_active_kernel_environment(_environment_request(analysis, ["numpy", "sklearn", "json"]))

    assert result["status"] == "verified"
    assert result["binding"]["analysis_hash"] == analysis["analysis_hash"]
    assert result["kernel"]["id"] == "kernel-123"
    assert result["kernel"]["python_version"] == sys.version.split()[0]
    assert result["inventory_hash"].startswith("sha256:")
    assert result["dependencies"] == [
        {
            "import_root": "numpy",
            "classification": "installed",
            "distribution": {"name": "numpy", "version": "2.5.0", "origin": "installed"},
            "candidates": [],
        },
        {
            "import_root": "sklearn",
            "classification": "installed",
            "distribution": {"name": "scikit-learn", "version": "1.9.0", "origin": "installed"},
            "candidates": [],
        },
    ]
    assert result["excluded_imports"] == [{"import_root": "json", "reason": "standard_library"}]


def test_representative_lgbm_hyperopt_analysis_keeps_only_used_exact_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the behavior-only fixture without importing any ML package.

    The shared notebook import cell intentionally includes ``json`` and
    ``xgboost`` even though no selected Function uses them.  Uses inside the
    objective's nested branch must still be found, while those unrelated
    imports must not leak into generated source or dependency evidence.
    """

    source = (_PROMPT16_FIXTURES / "lgbm_hyperopt_source.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="<synthetic-lgbm-hyperopt>", mode="exec")
    function_sources = {
        item.name: ast.unparse(item).rstrip() + "\n" for item in tree.body if isinstance(item, ast.FunctionDef)
    }
    supporting_imports = (
        "import json\n"
        "import lightgbm as lgb\n"
        "import numpy as np\n"
        "import pandas as pd\n"
        "import xgboost as xgb\n"
        "from hyperopt import STATUS_OK, Trials, fmin, hp, tpe\n"
        "from sklearn.model_selection import train_test_split\n"
    )
    expected_roots = {
        "prepare_training": ["numpy", "pandas"],
        "prepare_validation": ["pandas", "sklearn"],
        "train_lgbm": ["lightgbm"],
        "hyperopt_objective": ["hyperopt", "lightgbm", "numpy"],
        "run_hyperopt": ["hyperopt"],
    }
    analyses: dict[str, dict[str, Any]] = {}
    for index, (name, roots) in enumerate(expected_roots.items(), 1):
        analysis = _ready(
            function_sources[name],
            function_name=name,
            supporting=[{"cell_id": "imports", "source": supporting_imports}],
        )
        analyses[name] = analysis
        assert analysis["function"]["name"] == name
        assert analysis["function"]["import_roots"] == roots
        assert "import xgboost" not in analysis["function"]["source"]
        assert "import json" not in analysis["function"]["source"]
        assert analysis["binding"]["selected_cell_ids"] == ["cell-1"]
        assert index <= 5

    # Imports used only inside a conditional branch and nested calls remain
    # visible to the framework-owned AST analysis.
    objective_source = analyses["hyperopt_objective"]["function"]["source"]
    assert "import lightgbm as lgb" in objective_source
    assert "import numpy as np" in objective_source
    assert "from hyperopt import STATUS_OK" in objective_source

    _install_fake_metadata(
        monkeypatch,
        {
            "hyperopt": ["hyperopt"],
            "lightgbm": ["lightgbm"],
            "numpy": ["numpy"],
            "pandas": ["pandas"],
            "sklearn": ["scikit-learn"],
            "xgboost": ["xgboost"],
        },
        {
            "hyperopt": _FakeDistribution("hyperopt", "0.2.7"),
            "lightgbm": _FakeDistribution("lightgbm", "4.6.0"),
            "numpy": _FakeDistribution("numpy", "2.3.2"),
            "pandas": _FakeDistribution("pandas", "2.3.1"),
            "scikit-learn": _FakeDistribution("scikit-learn", "1.7.1"),
            "xgboost": _FakeDistribution("xgboost", "3.0.2"),
        },
    )
    used_roots = sorted({root for analysis in analyses.values() for root in analysis["function"]["import_roots"]})
    assert used_roots == ["hyperopt", "lightgbm", "numpy", "pandas", "sklearn"]

    environment = inspect_active_kernel_environment(
        _environment_request(analyses["hyperopt_objective"], [*used_roots, "json"])
    )

    assert environment["status"] == "verified", environment["diagnostics"]
    assert [
        (
            item["import_root"],
            item["distribution"]["name"],
            item["distribution"]["version"],
        )
        for item in environment["dependencies"]
    ] == [
        ("hyperopt", "hyperopt", "0.2.7"),
        ("lightgbm", "lightgbm", "4.6.0"),
        ("numpy", "numpy", "2.3.2"),
        ("pandas", "pandas", "2.3.1"),
        ("sklearn", "scikit-learn", "1.7.1"),
    ]
    assert environment["excluded_imports"] == [{"import_root": "json", "reason": "standard_library"}]
    assert "xgboost" not in {item["import_root"] for item in environment["dependencies"]}


def test_representative_nested_hyperopt_objective_fails_closed_instead_of_omitting_it() -> None:
    result = analyze_selected_code(
        _request(
            (
                "def run_hyperopt(search_df, trials):\n"
                "    def objective(params):\n"
                "        model = lgb.LGBMRegressor(**params)\n"
                "        model.fit(search_df, search_df['target'])\n"
                "        return {'loss': float(np.mean(model.predict(search_df))), "
                "'status': STATUS_OK}\n"
                "    space = {'learning_rate': hp.uniform('learning_rate', 0.01, 0.2)}\n"
                "    return fmin(objective, space, algo=tpe.suggest, trials=trials, max_evals=5)\n"
            ),
            function_name="run_hyperopt",
            supporting=[
                {
                    "cell_id": "imports",
                    "source": (
                        "import lightgbm as lgb\nimport numpy as np\nfrom hyperopt import STATUS_OK, fmin, hp, tpe\n"
                    ),
                }
            ],
        )
    )

    assert result["status"] == "invalid"
    assert result["function"] is None
    assert {item["code"] for item in result["diagnostics"]} == {"analysis.closure_unsupported"}
    assert result["binding"]["generated_source_hash"] is None


def test_active_kernel_inventory_count_bound_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_metadata(
        monkeypatch,
        {"numpy": ["numpy"]},
        {
            "numpy": _FakeDistribution("numpy", "2.5.0"),
            "package-a": _FakeDistribution("package-a", "1.0.0"),
            "package-b": _FakeDistribution("package-b", "1.0.0"),
        },
    )
    monkeypatch.setattr(m_source_analysis, "MAX_ENVIRONMENT_DISTRIBUTIONS", 2)
    analysis = _ready("import numpy as np\nresult = np.asarray(source)\nresult\n")

    result = inspect_active_kernel_environment(_environment_request(analysis, ["numpy"]))

    assert result["status"] == "unverified"
    assert result["inventory_hash"] is None
    assert result["dependencies"] == []
    assert {item["code"] for item in result["diagnostics"]} == {"environment.metadata_bounds_exceeded"}


def test_active_kernel_distribution_candidate_bound_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_metadata(
        monkeypatch,
        {"namespace": ["namespace-a", "namespace-b"]},
        {
            "namespace-a": _FakeDistribution("namespace-a", "1.0.0"),
            "namespace-b": _FakeDistribution("namespace-b", "2.0.0"),
        },
    )
    monkeypatch.setattr(m_source_analysis, "MAX_DISTRIBUTIONS_PER_IMPORT_ROOT", 1)
    analysis = _ready("import namespace\nresult = source\nresult\n")

    result = inspect_active_kernel_environment(_environment_request(analysis, ["namespace"]))

    assert result["status"] == "unverified"
    assert result["inventory_hash"] is None
    assert result["dependencies"] == []
    assert {item["code"] for item in result["diagnostics"]} == {"environment.metadata_bounds_exceeded"}


def test_active_kernel_direct_url_metadata_bound_fails_closed_without_path_leakage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_metadata(
        monkeypatch,
        {"editablemod": ["editable-project"]},
        {
            "editable-project": _FakeDistribution(
                "editable-project",
                "1.2.3",
                {"url": "file:///sensitive/user/project", "dir_info": {"editable": True}},
            )
        },
    )
    monkeypatch.setattr(m_source_analysis, "MAX_DIRECT_URL_BYTES", 8)
    analysis = _ready("import editablemod\nresult = source\nresult\n")

    result = inspect_active_kernel_environment(_environment_request(analysis, ["editablemod"]))

    assert result["status"] == "unverified"
    assert result["dependencies"] == []
    assert {item["code"] for item in result["diagnostics"]} == {"environment.metadata_bounds_exceeded"}
    assert "sensitive" not in json.dumps(result)


def test_active_kernel_classifies_editable_packaged_local_ambiguous_unpacked_and_unresolved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    (tmp_path / "loosemodule.py").write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.setattr(sys, "path", [str(tmp_path)])
    _install_fake_metadata(
        monkeypatch,
        {
            "editablemod": ["editable-project"],
            "localpkg": ["local-project"],
            "namespace": ["namespace-a", "namespace-b"],
        },
        {
            "editable-project": _FakeDistribution(
                "editable-project", "1.2.3", {"url": "file:///redacted", "dir_info": {"editable": True}}
            ),
            "local-project": _FakeDistribution("local-project", "2.0.0", {"url": "file:///redacted"}),
            "namespace-a": _FakeDistribution("namespace-a", "1.0.0"),
            "namespace-b": _FakeDistribution("namespace-b", "2.0.0"),
        },
    )
    analysis = _ready("result = source\nresult\n")

    result = inspect_active_kernel_environment(
        _environment_request(analysis, ["editablemod", "localpkg", "namespace", "loosemodule", "missingmod"])
    )

    by_root = {item["import_root"]: item for item in result["dependencies"]}
    assert by_root["editablemod"]["classification"] == "editable"
    assert by_root["localpkg"]["classification"] == "packaged_local"
    assert by_root["namespace"]["classification"] == "ambiguous"
    assert by_root["loosemodule"]["classification"] == "unpackaged_local"
    assert by_root["missingmod"]["classification"] == "unresolved"
    assert result["status"] == "unverified"
    assert {item["code"] for item in result["diagnostics"]} == {
        "dependency.ambiguous_distribution",
        "dependency.unpackaged_local",
        "dependency.unresolved",
    }
    assert "/redacted" not in json.dumps(result)


def test_analysis_and_environment_never_call_import_eval_or_exec(monkeypatch: pytest.MonkeyPatch) -> None:
    analysis_request = _request("import malicious_dependency\nresult = source\nresult\n")
    original_import = builtins.__import__
    original_eval = builtins.eval
    original_exec = builtins.exec
    calls: list[str] = []

    def forbidden(name: str) -> Callable[..., Any]:
        def fail(*_args: Any, **_kwargs: Any) -> Any:
            calls.append(name)
            raise AssertionError(name)

        return fail

    builtins.__import__ = forbidden("import")  # type: ignore[assignment]
    builtins.eval = forbidden("eval")  # type: ignore[assignment]
    builtins.exec = forbidden("exec")  # type: ignore[assignment]
    try:
        analysis = analyze_selected_code(analysis_request)
    finally:
        builtins.__import__ = original_import  # type: ignore[assignment]
        builtins.eval = original_eval  # type: ignore[assignment]
        builtins.exec = original_exec  # type: ignore[assignment]

    assert analysis["status"] == "ready"
    assert calls == []

    _install_fake_metadata(
        monkeypatch,
        {"malicious_dependency": ["malicious-distribution"]},
        {"malicious-distribution": _FakeDistribution("malicious-distribution", "9.9.9")},
    )
    builtins.__import__ = forbidden("import")  # type: ignore[assignment]
    builtins.eval = forbidden("eval")  # type: ignore[assignment]
    builtins.exec = forbidden("exec")  # type: ignore[assignment]
    try:
        environment = inspect_active_kernel_environment(
            _environment_request(analysis, analysis["function"]["import_roots"])
        )
    finally:
        builtins.__import__ = original_import  # type: ignore[assignment]
        builtins.eval = original_eval  # type: ignore[assignment]
        builtins.exec = original_exec  # type: ignore[assignment]

    assert environment["status"] == "verified"
    assert calls == []


def test_source_or_document_change_stales_binding_hashes() -> None:
    first = _ready("result = source + 1\nresult\n")
    changed = _ready("result = source + 2\nresult\n")
    request = _request("result = source + 1\nresult\n")
    request["document"]["revision"] = "revision-8"
    revised = analyze_selected_code(request)

    assert first["binding"]["selected_source_hash"] != changed["binding"]["selected_source_hash"]
    assert first["binding"]["generated_source_hash"] != changed["binding"]["generated_source_hash"]
    assert first["analysis_hash"] != revised["analysis_hash"]
