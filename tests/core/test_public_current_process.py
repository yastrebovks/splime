"""Authenticated current-process calls, including an actual NumPy computation."""

from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import os
import sys
import gc
import zipfile
from concurrent.futures import ThreadPoolExecutor
from importlib import metadata
from pathlib import Path
from types import ModuleType

import pytest
import pandas as pd
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from spl import SPLClient
from spl.core.ir.utils import spl_compile_to_source
from spl.daemon_client import ClientError
from spl.embedded import verify_public_bundle
from spl.public import parse_public_ref
from spl.public_contract import (
    COMPILED_EXECUTION,
    CURRENT_PROCESS_CONTRACT,
    CURRENT_PROCESS_MANIFEST_SCHEMA,
    ContractError,
    dependency_graph,
)
from spl.public_dependencies import inspect_dependencies

REGISTRY = "https://registry.example.test"
KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
KEY_ID = "ed25519-sha256:" + hashlib.sha256(KEY.public_key().public_bytes_raw()).hexdigest()
KEYS = {REGISTRY: {KEY_ID: KEY.public_key().public_bytes_raw()}}
REF = "splime://@alice/default/example@1"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def signed(manifest, bundle):
    digest = hashlib.sha256(canonical(manifest)).hexdigest()
    return {
        "manifest": manifest,
        "manifest_hash": digest,
        "bundle_hash": manifest["bundle_hash"],
        "signature": {
            "algorithm": "ed25519",
            "key_id": KEY_ID,
            "value": base64.b64encode(KEY.sign(f"{digest}:{manifest['bundle_hash']}".encode())).decode(),
        },
    }


def function_yaml(body="return 7", dependencies=()):
    def distribution_yaml(package, version):
        modules = f"  modules: [{package}]\n" if package.isidentifier() else ""
        return f"- !DDistribution\n  package: {package}\n  version: '{version}'\n{modules}"

    return (
        "- !DFunction\n  name: example\n  inputs: []\n  outputs:\n  - name: default\n    type: int\n  body: |-\n"
        + "\n".join("    " + line for line in body.splitlines())
        + "\n"
        + "".join(distribution_yaml(p, v) for p, v in dependencies)
    ).encode()


def fixture(
    tmp_path,
    *,
    body="return 7",
    dependencies=(),
    yaml_bytes=None,
    compiled_prefix="",
    version=1,
    runtime=None,
    extra=None,
    kind="function",
    raw_graph=False,
):
    source = yaml_bytes or function_yaml(body, dependencies)
    path = tmp_path / "compile" / "object.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(source)
    descriptor = {
        "entrypoint": "example",
        "kind": kind,
        "distributions": [
            {"package": p, "version": v, **({"modules": [p]} if p.isidentifier() else {})} for p, v in dependencies
        ],
        "runtime_config": runtime or {"mode": "venv"},
    }
    members = {
        "object.yaml": source,
        "object.json": canonical(descriptor),
        "object.py": (compiled_prefix + spl_compile_to_source(path)).encode(),
        **(extra or {}),
    }
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", zipfile.ZIP_STORED) as archive:
        for name, value in sorted(members.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, value)
    bundle = data.getvalue()
    manifest = {
        "schema": CURRENT_PROCESS_MANIFEST_SCHEMA,
        "schema_version": 2,
        "execution_contract": copy.deepcopy(CURRENT_PROCESS_CONTRACT),
        "execution": COMPILED_EXECUTION,
        "entrypoint": "example",
        "kind": kind,
        "callable": True,
        "version": version,
        "version_id": f"v{version}",
        "bundle_hash": hashlib.sha256(bundle).hexdigest(),
        "bundle_format": "zip-store-v1",
        "identity": {
            "entry_type": "object",
            "entry_id": "example",
            "owner": "@alice",
            "library": "default",
            "name": "example",
            "uri": "splime://@alice/default/example",
        },
        "members": [
            {"path": n, "sha256": hashlib.sha256(b).hexdigest(), "size": len(b)} for n, b in sorted(members.items())
        ],
        "runtime_locks": [],
        "dependencies": descriptor["distributions"],
        "python_constraints": {"python": "3.13", "runtime": descriptor["runtime_config"]},
        "dependency_graph": [] if raw_graph else dependency_graph(members),
        "components": [{"path": "root"}],
        "required_capabilities": [CURRENT_PROCESS_CONTRACT["framework"]],
    }
    return signed(manifest, bundle), bundle


def cached(tmp_path, envelope, bundle, *, client=None):
    client = client or SPLClient.embedded(REGISTRY, tmp_path / "cache", trusted_keys=KEYS, run_receipts=False)
    backend = client._embedded_backend
    backend._install_bundle(envelope, bundle)
    reference = REF.rsplit("@", 1)[0] + "@" + str(envelope["manifest"]["version"])
    resolved = {
        "registry_url": REGISTRY,
        "reference": reference,
        "release": {
            "id": envelope["manifest_hash"],
            "manifest_hash": envelope["manifest_hash"],
            "bundle_hash": envelope["bundle_hash"],
            "version": envelope["manifest"]["version"],
            "version_id": envelope["manifest"]["version_id"],
        },
    }
    path = backend._resolution_path(parse_public_ref(reference))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical(resolved))
    return client


def record(package, version, source="root"):
    return {"package": package, "version": version, "sources": [source]}


@pytest.mark.parametrize(
    "versions,expected",
    [
        (["1"], ["present"]),
        (["2"], ["mismatched"]),
        (["2", "3"], ["mismatched", "mismatched"]),
    ],
)
def test_versions_and_normalization(monkeypatch, versions, expected, caplog):
    monkeypatch.setattr(metadata, "version", lambda name: "1.0")
    monkeypatch.setattr(metadata, "packages_distributions", lambda: {})
    report = inspect_dependencies([record("My_Package", version, "root.node") for version in versions])
    assert [x.status for x in report.dependencies] == expected
    assert all(x.package == "my-package" for x in report.dependencies)
    report.warn()
    if "mismatched" in expected:
        assert "publication (root.node)" in caplog.text and "metadata reports 1.0" in caplog.text


@pytest.mark.parametrize("missing", [["absent-one"], ["absent-one", "absent-two"]])
def test_all_missing_and_mismatched(monkeypatch, missing):
    def version(name):
        if name in missing:
            raise metadata.PackageNotFoundError(name)
        return "1"

    monkeypatch.setattr(metadata, "version", version)
    monkeypatch.setattr(metadata, "packages_distributions", lambda: {})
    report = inspect_dependencies([record(name, "2.3.5") for name in missing] + [record("present", "9")])
    assert [x.package for x in report.missing] == missing
    assert len(report.dependencies) == len(missing) + 1


def test_conflicting_component_provenance_deduplicated(monkeypatch):
    monkeypatch.setattr(metadata, "version", lambda name: "1")
    monkeypatch.setattr(metadata, "packages_distributions", lambda: {})
    report = inspect_dependencies(
        [
            record("X_Y", "2", "node"),
            record("x-y", "2", "adapter"),
            record("x.y", "3", "component"),
            record("x-y", "2", "node"),
        ]
    )
    assert len(report.dependencies) == 2
    assert report.dependencies[0].sources == ("adapter", "node")
    assert report.dependencies[1].recorded_version == "3"


def test_unverifiable_malformed_and_loaded(monkeypatch, caplog):
    module = ModuleType("sample_module")
    module.__version__ = "1"
    monkeypatch.setitem(sys.modules, "sample_module", module)
    monkeypatch.setattr(metadata, "packages_distributions", lambda: {"sample_module": ["sample-dist"]})
    monkeypatch.setattr(metadata, "version", lambda name: "2")
    report = inspect_dependencies([record("sample-dist", "2")])
    assert report.dependencies[0].status == "mismatched"
    report.warn()
    assert "loaded sample_module=1" in caplog.text and "restart the kernel" in caplog.text
    assert sys.modules["sample_module"] is module
    del module.__version__
    assert inspect_dependencies([record("sample-dist", "2")]).dependencies[0].status == "unverifiable"
    monkeypatch.setattr(metadata, "version", lambda name: "not-a-version")
    assert inspect_dependencies([record("sample-dist", "2")]).dependencies[0].status == "unverifiable"
    assert inspect_dependencies([record("sample-dist", "bad-version")]).dependencies[0].status == "invalid"
    monkeypatch.setattr(metadata, "version", lambda name: (_ for _ in ()).throw(metadata.PackageNotFoundError(name)))
    assert inspect_dependencies([record("sample-dist", "2")]).dependencies[0].status == "unverifiable"


def test_missing_stops_module_code_and_preserves_full_report(tmp_path):
    sentinel = tmp_path / "executed"
    env, bundle = fixture(
        tmp_path,
        dependencies=[("spl-test-absent-one", "2.3.5"), ("spl-test-absent-two", "1")],
        compiled_prefix=f"from pathlib import Path\nPath({str(sentinel)!r}).touch()\n",
    )
    client = cached(tmp_path, env, bundle)
    events = []
    client._embedded_backend._emit_run_receipt = lambda *a, **k: events.append(k["state"])
    with pytest.raises(ClientError) as caught:
        client.call(REF, trust=True)
    assert caught.value.code == "public_dependencies_missing"
    assert len(caught.value.payload["missing"]) == 2
    assert "spl-test-absent-one==2.3.5" in str(caught.value)
    assert "environment used by this kernel/interpreter" in str(caught.value)
    assert not sentinel.exists() and events == []


def test_real_numpy_in_caller_pid_unchanged(tmp_path, caplog, monkeypatch):
    import numpy as np

    before = (np, np.__version__, np.__file__, os.getpid())
    env, bundle = fixture(
        tmp_path,
        body="import os\nimport numpy as np\nreturn {'pid': os.getpid(), 'sum': int(np.arange(5).sum()), 'version': np.__version__}",
        dependencies=[("numpy", "2.3.5")],
    )
    client = cached(tmp_path, env, bundle)

    def forbidden(*args, **kwargs):
        pytest.fail("in-process call attempted an isolated execution operation")

    backend = client._embedded_backend
    for name in ["_locked_environments", "_ensure_environments", "_execute", "_materialize_wheels"]:
        monkeypatch.setattr(backend, name, forbidden)
    import subprocess

    monkeypatch.setattr(subprocess, "Popen", forbidden)
    from spl.daemon_client import Client

    monkeypatch.setattr(Client, "__init__", forbidden)
    result = client.call(REF, trust=True)
    assert result.value == {"pid": os.getpid(), "sum": 10, "version": np.__version__}
    assert (np, np.__version__, np.__file__, os.getpid()) == before
    assert result.run["execution_profile"] == "current-process-v1" and result.mode == "embedded"
    if np.__version__ != "2.3.5":
        assert "numpy was recorded as 2.3.5" in caplog.text
        assert np.__version__ in caplog.text


def test_native_pipeline_and_framework_state(tmp_path):
    source = b"""- !DPipeline
  name: example
  nodes:
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000001
    func: inner
  links: []
  aliases:
  - [default, 00000000-0000-0000-0000-000000000001]
---
- !DFunction
  name: inner
  inputs: []
  outputs:
  - name: default
    type: int
  body: return 42
"""
    env, bundle = fixture(tmp_path, yaml_bytes=source, kind="pipeline")
    client = cached(tmp_path, env, bundle)
    before = (list(sys.path), os.getcwd(), dict(os.environ), sys.stdout, sys.stderr)
    result = client.call(REF, trust=True)
    assert result.output == {"default": 42}
    assert result.payload["manifest"]["status"] == "succeeded"
    assert (sys.path, os.getcwd(), dict(os.environ), sys.stdout, sys.stderr) == before


def test_native_dataframe_input_output_preserves_identity(tmp_path):
    source = b"""- !DFunction
  name: example
  inputs:
  - {name: start_df, type: null, default: null}
  outputs:
  - {name: default, type: null}
  body: return start_df
"""
    env, bundle = fixture(tmp_path, yaml_bytes=source)
    frame = pd.DataFrame({"value": [1, 2, 3]})
    result = cached(tmp_path, env, bundle).call(REF, kwargs={"start_df": frame}, trust=True)
    assert result.value is frame
    assert result.output is frame
    assert result.payload["result"]["kind"] == "native-in-memory"
    assert result.payload["result"]["shape"] == [3, 1]
    json.dumps(result.payload)
    assert "DataFrame" in repr(result) and "value" not in repr(result)
    second = cached(tmp_path, env, bundle).call(REF, kwargs={"start_df": frame}, trust=True)
    assert result is not second and (result == second) is False


def test_native_dataframe_transform_preserves_rich_values_and_mutation(tmp_path):
    source = b"""- !DFunction
  name: example
  inputs:
  - {name: start_df, type: null, default: null}
  outputs:
  - {name: default, type: null}
  body: |-
    start_df['was_missing'] = start_df['nullable'].isna()
    return start_df.assign(shifted=start_df['nullable'] + 10)
"""
    env, bundle = fixture(tmp_path, yaml_bytes=source)
    index = pd.MultiIndex.from_tuples([("a", 1), ("a", 2), ("b", 1)], names=["group", "row"])
    frame = pd.DataFrame(
        {
            "nullable": pd.array([1, pd.NA, 3], dtype="Int64"),
            "category": pd.Categorical(["private-cell-marker", "other", None]),
            "when": pd.to_datetime(["2026-01-01", None, "2026-03-01"], utc=True),
        },
        index=index,
    )
    result = cached(tmp_path, env, bundle).call(REF, kwargs={"start_df": frame}, trust=True)
    transformed = result.value
    assert transformed is not frame
    assert frame["was_missing"].tolist() == [False, True, False]
    pd.testing.assert_frame_equal(transformed, frame.assign(shifted=frame["nullable"] + 10))
    pd.testing.assert_frame_equal(transformed.head(2), transformed.iloc[:2])
    assert result.payload["result"]["shape"] == [3, 5]
    del result
    gc.collect()
    assert transformed.loc[("b", 1), "shifted"] == 13


def test_native_arbitrary_values_none_aliasing_and_safe_preview(tmp_path):
    np = pytest.importorskip("numpy")
    source = b"""- !DFunction
  name: example
  inputs:
  - {name: start_value, type: null, default: null}
  outputs:
  - {name: default, type: null}
  body: return start_value
"""
    env, bundle = fixture(tmp_path, yaml_bytes=source)
    client = cached(tmp_path, env, bundle)

    class UnsafeDisplay:
        @property
        def shape(self):
            raise RuntimeError("shape must be presentation-safe")

        def __repr__(self):
            raise RuntimeError("repr must not be evaluated")

    array = np.arange(4)
    custom = UnsafeDisplay()
    nested = {"array": array, "again": array, "custom": custom, "none": None}
    nested_result = client.call(REF, kwargs={"start_value": nested}, trust=True)
    assert nested_result.value is nested
    assert nested_result.value["array"] is array
    assert nested_result.value["again"] is array
    assert nested_result.value["custom"] is custom

    none_result = client.call(REF, kwargs={"start_value": None})
    assert none_result.value is None and none_result.output is None
    assert none_result.payload["result"]["type"] == "builtins.NoneType"

    custom_result = client.call(REF, kwargs={"start_value": custom})
    assert custom_result.value is custom
    assert "UnsafeDisplay" in repr(custom_result)
    assert "shape" not in custom_result.payload["result"]


def test_native_pipeline_preserves_multiple_output_structure(tmp_path):
    source = b"""- !DPipeline
  name: example
  nodes:
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000001
    func: left
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000002
    func: right
  links: []
  aliases:
  - [left_result, 00000000-0000-0000-0000-000000000001]
  - [right_result, 00000000-0000-0000-0000-000000000002]
---
- !DFunction
  name: left
  inputs:
  - {name: start_df, type: null, default: null}
  outputs:
  - {name: default, type: null}
  body: return start_df
---
- !DFunction
  name: right
  inputs:
  - {name: start_df, type: null, default: null}
  outputs:
  - {name: default, type: null}
  body: |-
    return {'nested': [start_df]}
"""
    env, bundle = fixture(tmp_path, yaml_bytes=source, kind="pipeline")
    frame = pd.DataFrame({"value": ["never-persist-this-cell", "other"]})
    result = cached(tmp_path, env, bundle).call(REF, kwargs={"start_df": frame}, trust=True)
    assert set(result.value) == {"left_result", "right_result"}
    assert result.value["left_result"]["default"] is frame
    assert result.value["right_result"]["default"]["nested"][0] is frame
    assert result.output is result.value
    assert "never-persist-this-cell" not in json.dumps(result.payload["manifest"])


def test_native_pipeline_nested_values_and_constructor_name_collisions(tmp_path, monkeypatch):
    names = [
        "output",
        "adapters",
        "runtimes",
        "keep",
        "run_id",
        "parent_run_id",
        "resume_plan",
        "runtime_config",
        "node_environment_provider",
        "runtime_env_spec",
        "runtime_adapter_fingerprint_sha256",
    ]
    inputs = "\n".join(f"  - {{name: {name}, type: null, default: null}}" for name in names + ["start_df"])
    returned = ", ".join(f"{name!r}: {name}" for name in names)
    source = f"""- !DPipeline
  name: example
  nodes:
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000001
    func: inner
  links: []
  aliases:
  - [default, 00000000-0000-0000-0000-000000000001]
---
- !DFunction
  name: inner
  inputs:
{inputs}
  outputs:
  - {{name: default, type: null}}
  body: |-
    return {{'controls': {{{returned}}}, 'nested': [{{'frame': start_df}}]}}
""".encode()
    env, bundle = fixture(tmp_path, yaml_bytes=source, kind="pipeline")
    frame = pd.DataFrame({"x": [4, 5]})
    values = {name: f"input-{name}" for name in names}
    values["start_df"] = frame

    def forbidden(*args, **kwargs):
        pytest.fail("a constructor-name collision selected subprocess execution")

    import subprocess

    monkeypatch.setattr(subprocess, "Popen", forbidden)
    result = cached(tmp_path, env, bundle).call(REF, kwargs=values, trust=True)
    output = result.output["default"]
    assert output["controls"] == {name: f"input-{name}" for name in names}
    assert output["nested"][0]["frame"] is frame


def test_private_module_registration_lifetime_and_safe_repr(tmp_path):
    source = b"""- !DFunction
  name: example
  inputs: []
  outputs:
  - {name: default, type: null}
  body: |-
    from dataclasses import dataclass
    @dataclass
    class Row:
        value: "int"
        def __repr__(self):
            raise AssertionError('full native repr must not run')
    return Row(7)
"""
    env, bundle = fixture(tmp_path, yaml_bytes=source)
    result = cached(tmp_path, env, bundle).call(REF, trust=True)
    value = result.value
    module_name = type(value).__module__
    assert module_name in sys.modules
    assert sys.modules[module_name].__name__ == module_name
    assert value.value == 7
    assert module_name in repr(result)
    del result
    gc.collect()
    assert module_name in sys.modules
    del value
    gc.collect()
    assert module_name not in sys.modules


def test_errors_tracebacks_events_and_keyboard_interrupt(tmp_path):
    gc.collect()
    private_before = {name for name in sys.modules if name.startswith("_spl_public_")}
    for index, body in enumerate(["raise ImportError('real incompatible symbol')", "raise KeyboardInterrupt()"]):
        env, bundle = fixture(tmp_path, body=body, version=index + 1)
        client = cached(tmp_path, env, bundle)
        events = []
        client._embedded_backend._emit_run_receipt = lambda *a, **k: events.append(k["state"])
        before = (list(sys.path), os.getcwd(), dict(os.environ), sys.stdout, sys.stderr)
        with pytest.raises(ImportError if index == 0 else KeyboardInterrupt) as caught:
            client.call(REF.rsplit("@", 1)[0] + f"@{index + 1}", trust=True)
        assert "example" in [x.name for x in caught.traceback]
        assert events == ["started", "failure"]
        assert (sys.path, os.getcwd(), dict(os.environ), sys.stdout, sys.stderr) == before
    assert {name for name in sys.modules if name.startswith("_spl_public_")} == private_before


def test_result_artifact_and_arbitrary_native_result(tmp_path):
    path = tmp_path / "input.txt"
    path.write_text("artifact content")
    body = f"return {{'__spl_result__': {{'answer': 42}}, '__spl_artifacts__': {{'a.txt': {str(path)!r}}}}}"
    env, bundle = fixture(tmp_path, body=body)
    result = cached(tmp_path, env, bundle).call(REF, trust=True, artifacts_dir=tmp_path / "downloads")
    assert result.value == {"answer": 42}
    assert result.downloaded_artifacts["a.txt"].read_text() == "artifact content"
    env, bundle = fixture(tmp_path, body="return object()", version=2)
    native = cached(tmp_path, env, bundle).call(REF[:-1] + "2", trust=True)
    assert type(native.value) is object
    assert native.payload["result"] == {
        "kind": "native-in-memory",
        "type": "builtins.object",
        "reconstructable": False,
    }


@pytest.mark.parametrize("option", [{"timeout_seconds": 1}, {"runtimes": "native"}])
def test_unsupported_options_before_code(tmp_path, option):
    sentinel = tmp_path / "executed"
    env, bundle = fixture(tmp_path, compiled_prefix=f"from pathlib import Path\nPath({str(sentinel)!r}).touch()\n")
    client = cached(tmp_path, env, bundle)
    with pytest.raises((ClientError, TypeError, ValueError)):
        client.call(REF, trust=True, **option)
    assert not sentinel.exists()


def test_non_string_keyword_name_is_rejected_before_code(tmp_path):
    sentinel = tmp_path / "executed"
    env, bundle = fixture(tmp_path, compiled_prefix=f"from pathlib import Path\nPath({str(sentinel)!r}).touch()\n")
    with pytest.raises(ClientError, match="keyword argument names must be strings"):
        cached(tmp_path, env, bundle).call(REF, trust=True, kwargs={1: "value"})
    assert not sentinel.exists()


def test_trust_profile_unknown_contract_and_tampering(tmp_path):
    env, bundle = fixture(tmp_path)
    client = cached(tmp_path, env, bundle)
    with pytest.raises(ClientError, match="trust"):
        client.call(REF)
    for key, value in [
        ("dependency_graph", None),
        ("execution_contract", {"profile": "future"}),
        ("required_capabilities", []),
        ("runtime_locks", [{}]),
    ]:
        manifest = copy.deepcopy(env["manifest"])
        manifest[key] = value
        with pytest.raises(ClientError):
            verify_public_bundle(signed(manifest, bundle), bundle, registry_url=REGISTRY, trusted_keys=KEYS)
    for altered_env, altered_bundle in [
        (dict(env, signature=dict(env["signature"], value="AAAA")), bundle),
        (env, bundle + b"tampered"),
    ]:
        with pytest.raises(ClientError):
            verify_public_bundle(altered_env, altered_bundle, registry_url=REGISTRY, trusted_keys=KEYS)
    old = copy.deepcopy(env)
    old["manifest"]["schema"] = "spl.public_object_manifest.v1"
    old["manifest"].pop("execution_contract")
    old = signed(old["manifest"], bundle)
    client._embedded_backend._approve(client._embedded_backend._trust_identity(old))
    assert not client._embedded_backend._trusted(client._embedded_backend._trust_identity(env))


def test_cached_extracted_code_cannot_replace_authenticated_code(tmp_path):
    env, bundle = fixture(tmp_path)
    client = cached(tmp_path, env, bundle)
    path = (
        client._embedded_backend.cache_root / "bundles" / f"{env['bundle_hash']}-{env['manifest_hash']}" / "object.py"
    )
    path.chmod(0o600)
    path.write_text("raise RuntimeError('tampered')")
    assert client.call(REF, trust=True).output == 7


def test_repeated_concurrent_and_distinct_releases(tmp_path):
    gc.collect()
    private_before = {name for name in sys.modules if name.startswith("_spl_public_")}
    env, bundle = fixture(
        tmp_path, compiled_prefix="counter = 0\n", body="global counter\ncounter += 1\nreturn counter"
    )
    client = cached(tmp_path, env, bundle)
    assert client.call(REF, trust=True).output == 1
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(lambda _: client.call(REF).output, range(8))) == [1] * 8
    env2, bundle2 = fixture(tmp_path, body="return 99", version=2)
    cached(tmp_path, env2, bundle2, client=client)
    assert client.call(REF[:-1] + "2", trust=True).output == 99
    assert client.call(REF).output == 1
    gc.collect()
    assert {name for name in sys.modules if name.startswith("_spl_public_")} == private_before


def test_nested_graph_adapters_and_conflicts(tmp_path):
    source = (
        function_yaml(dependencies=[("numpy", "2.3.5")])
        + b"""---
- !DFunction
  name: nested
  inputs: []
  outputs: []
  body: return 1
- !DDistribution
  package: numpy
  version: '2.0'
"""
    )
    extra = {
        "_spl_components/c_a/adapter.json": canonical({"dependencies": [{"package": "Other.Dist", "version": "3"}]})
    }
    env, _ = fixture(tmp_path, yaml_bytes=source, extra=extra)
    graph = env["manifest"]["dependency_graph"]
    assert {(x["package"], x["version"]) for x in graph} == {("numpy", "2.3.5"), ("numpy", "2.0"), ("other-dist", "3")}
    assert any("nested" in s for x in graph for s in x["sources"])
    assert any("adapter.json" in s for x in graph for s in x["sources"])


@pytest.mark.parametrize(
    "runtime",
    [
        {"mode": "venv", "node_runtime": "venv-subprocess"},
        {"mode": "docker"},
        {"mode": "venv", "node_timeout_seconds": 1},
    ],
)
def test_runtime_boundaries_rejected_statically(tmp_path, runtime):
    with pytest.raises(ContractError):
        fixture(tmp_path, runtime=runtime)


def test_actual_legacy_verifier_rejects_v2_before_execution(tmp_path):
    import spl.embedded as embedded

    code = Path(__file__).parents[1] / "fixtures" / "legacy_public_verifier_0_4_10.txt"
    namespace = dict(vars(embedded))
    namespace["PUBLIC_MANIFEST_SCHEMA"] = "spl.public_object_manifest.v1"
    exec(compile(code.read_text(), str(code), "exec"), namespace)
    env, bundle = fixture(tmp_path)
    with pytest.raises(ClientError) as caught:
        namespace["verify_public_bundle"](env, bundle, registry_url=REGISTRY, trusted_keys=KEYS)
    assert caught.value.code == "public_manifest_incompatible"


def test_reentrant_call_releases_bundle_lock(tmp_path, monkeypatch):
    import builtins

    env, bundle = fixture(tmp_path, body="import builtins\nreturn builtins._spl_test_reentrant()")
    client = cached(tmp_path, env, bundle)
    entered = False

    def callback():
        nonlocal entered
        if entered:
            return 10
        entered = True
        return client.call(REF).output + 1

    monkeypatch.setattr(builtins, "_spl_test_reentrant", callback, raising=False)
    assert client.call(REF, trust=True).output == 11


def test_lazy_bundle_imports_are_private_per_call_and_release(tmp_path, monkeypatch):
    # A caller-owned module of the same name must not be used, replaced or removed.
    user_module = ModuleType("_spl_components")
    monkeypatch.setitem(sys.modules, "_spl_components", user_module)
    for version in [1, 2]:
        extra = {
            "_spl_components/__init__.py": b"",
            "_spl_components/component/__init__.py": b"",
            "_spl_components/component/value.py": f"counter = {version * 10}\n".encode(),
        }
        body = "from _spl_components.component import value\nvalue.counter += 1\nreturn value.counter"
        env, bundle = fixture(tmp_path, body=body, extra=extra, version=version)
        client = cached(tmp_path, env, bundle)
        ref = REF[:-1] + str(version)
        with ThreadPoolExecutor(max_workers=2) as pool:
            assert list(pool.map(lambda _: client.call(ref, trust=True).output, range(2))) == [version * 10 + 1] * 2
        assert sys.modules["_spl_components"] is user_module
        assert not hasattr(user_module, "component")
        assert "_spl_components.component.value" not in sys.modules


def test_released_0410_lock_keeps_exact_signed_identity():
    from spl.runtime_environment import runtime_lock_document, validate_runtime_lock, PUBLIC_EMBEDDED_EXECUTOR
    from spl.runtime_environment import PUBLIC_RUNTIME_POLICY_NAME, PUBLIC_RUNTIME_SPDX_ALLOWLIST

    lock = runtime_lock_document(
        python="3.13",
        requirements=[],
        artifacts=[],
        executor={**PUBLIC_EMBEDDED_EXECUTOR, "minimum_version": "0.4.10"},
        policy={
            "name": PUBLIC_RUNTIME_POLICY_NAME,
            "version": "0.4.10",
            "spdx_allowlist": list(PUBLIC_RUNTIME_SPDX_ALLOWLIST),
            "wheel_policy": "non-yanked-universal-pure-python-only",
        },
        resolver={"name": "splime-pypi-closure", "version": 1},
    )
    lock["names"] = ["root"]
    assert validate_runtime_lock(lock) == lock
    lock["policy"]["wheel_policy"] = "anything"
    with pytest.raises(ValueError):
        validate_runtime_lock(lock)


def test_native_pipeline_adapter_artifact(tmp_path, caplog):
    source = b"""- !DPipeline
  name: example
  nodes:
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000001
    func: make_array
  links: []
  aliases:
  - [default, 00000000-0000-0000-0000-000000000001]
  adapters:
  - !DAdapter
    key: numpy.ndarray@npy
    save: save_array
    load: load_array
    distributions:
    - !DDistribution
      package: numpy
      version: '2.3.5'
---
- !DFunction
  name: make_array
  inputs: []
  outputs:
  - name: default
    type: numpy.ndarray
  body: |-
    import numpy as np
    return np.arange(3)
- !DDistribution
  package: numpy
  version: '2.3.5'
---
- !DFunction
  name: save_array
  inputs:
  - {name: path, type: str}
  - {name: value, type: numpy.ndarray}
  outputs: []
  body: |-
    import numpy as np
    np.save(path, value)
- !DDistribution
  package: numpy
  version: '2.3.5'
---
- !DFunction
  name: load_array
  inputs:
  - {name: path, type: str}
  outputs:
  - {name: default, type: numpy.ndarray}
  body: |-
    import numpy as np
    return np.load(path)
- !DDistribution
  package: numpy
  version: '2.3.5'
"""
    source = source.replace(b"\n- !DDistribution", b"\n- !DImport\n  module: numpy\n- !DDistribution")
    source = source.replace(
        b"\n      version: '2.3.5'\n",
        b"\n      version: '2.3.5'\n      modules: [numpy]\n",
    ).replace(
        b"\n  version: '2.3.5'\n",
        b"\n  version: '2.3.5'\n  modules: [numpy]\n",
    )
    env, bundle = fixture(tmp_path, yaml_bytes=source, kind="pipeline")
    result = cached(tmp_path, env, bundle).call(REF, trust=True, artifacts_dir=tmp_path / "downloads")
    import numpy as np

    paths = result.downloaded_artifacts
    assert len(paths) == 1
    assert np.load(next(iter(paths.values()))).tolist() == [0, 1, 2]
    assert result.output["default"]["__spl_artifact_ref__"] is True
    assert "numpy was recorded as 2.3.5" in caplog.text


def test_old_and_new_contracts_with_identical_bundle_bytes_coexist(tmp_path):
    env, bundle = fixture(tmp_path)
    client = cached(tmp_path, env, bundle)
    legacy = copy.deepcopy(env["manifest"])
    legacy["schema"] = "spl.public_object_manifest.v1"
    legacy["schema_version"] = 1
    legacy.pop("execution_contract")
    legacy_envelope = signed(legacy, bundle)
    old_dir = client._embedded_backend._install_bundle(legacy_envelope, bundle)
    assert old_dir.name == env["bundle_hash"]
    assert client.call(REF, trust=True).output == 7
    assert json.loads((old_dir / "manifest.json").read_text())["manifest"]["schema"].endswith(".v1")


def test_syntax_and_dependency_metadata_rejected_before_compilation(tmp_path):
    source = function_yaml() + b"- !DImport\n  module: numpy\n"
    with pytest.raises(ContractError, match="no captured distribution mapping"):
        fixture(tmp_path, yaml_bytes=source)
    source = function_yaml() + b"- !DDistribution\n  package: numpy\n  version: 'not-a-version'\n"
    with pytest.raises(ContractError, match="malformed"):
        fixture(tmp_path, yaml_bytes=source)
    with pytest.raises(ContractError, match="async and generator"):
        fixture(tmp_path, body="yield 1")


def test_component_definition_collision_and_scalar_literals():
    members = {
        "object.yaml": function_yaml() + b"- !DSPLImport\n  path: child.yaml\n  name: example\n",
        "object.json": canonical({"distributions": []}),
        "child.yaml": function_yaml("return 8"),
    }
    with pytest.raises(ContractError, match="duplicate inlined definition"):
        dependency_graph(members)
    members.pop("child.yaml")
    members["object.yaml"] = (
        function_yaml() + b"- !DScalar\n  value: {runtime: docker, dependencies: 'literal user data'}\n"
    )
    members["object.json"] = canonical(
        {
            "distributions": [],
            "runtime_config": {"mode": "venv"},
            "metadata": {"mode": "read", "runtime": "display", "dependencies": "literal user data"},
        }
    )
    assert dependency_graph(members) == []


def test_static_function_body_imports_require_complete_unambiguous_mapping():
    source = b"""- !DFunction
  name: example
  inputs: []
  outputs:
  - {name: default, type: int}
  body: |-
    import numpy as np
    from pandas import DataFrame
    return int(np.arange(1).sum()) + len(DataFrame())
- !DDistribution
  package: numpy
  version: '2'
  modules: [numpy]
"""
    members = {
        "object.yaml": source,
        "object.json": canonical({"distributions": [{"package": "numpy", "version": "2", "modules": ["numpy"]}]}),
    }
    with pytest.raises(ContractError, match="pandas.*no captured distribution mapping"):
        dependency_graph(members)
    complete = source + b"- !DDistribution\n  package: pandas\n  version: '3'\n  modules: [pandas]\n"
    members["object.yaml"] = complete
    members["object.json"] = canonical(
        {
            "distributions": [
                {"package": "numpy", "version": "2", "modules": ["numpy"]},
                {"package": "pandas", "version": "3", "modules": ["pandas"]},
            ]
        }
    )
    graph = dependency_graph(members)
    assert {tuple(item["modules"]) for item in graph} == {("numpy",), ("pandas",)}
    ambiguous = complete + b"- !DDistribution\n  package: other\n  version: '1'\n  modules: [numpy]\n"
    members["object.yaml"] = ambiguous
    with pytest.raises(ContractError, match="ambiguous distribution owners"):
        dependency_graph(members)


def test_static_import_allows_distinct_versions_of_one_distribution_with_provenance():
    source = b"""- !DFunction
  name: example
  inputs: []
  outputs:
  - {name: default, type: int}
  body: |-
    import numpy as np
    return int(np.arange(1).sum())
- !DDistribution
  package: numpy
  version: '2.0'
  modules: [numpy]
- !DDistribution
  package: numpy
  version: '3.0'
  modules: [numpy]
"""
    members = {
        "object.yaml": source,
        "object.json": canonical(
            {
                "distributions": [
                    {"package": "numpy", "version": "2.0", "modules": ["numpy"]},
                    {"package": "numpy", "version": "3.0", "modules": ["numpy"]},
                ]
            }
        ),
    }
    graph = dependency_graph(members)
    assert {(item["package"], item["version"]) for item in graph} == {("numpy", "2.0"), ("numpy", "3.0")}
    assert all(item["sources"] for item in graph)


def test_success_with_real_matching_version_has_no_dependency_warning(tmp_path, caplog):
    import numpy as np

    env, bundle = fixture(
        tmp_path, body="import numpy as np\nreturn int(np.arange(3).sum())", dependencies=[("numpy", np.__version__)]
    )
    assert cached(tmp_path, env, bundle).call(REF, trust=True).value == 3
    assert not [r for r in caplog.records if r.name == "spl.public_dependencies"]


def test_large_and_shared_native_function_results_have_no_lifecycle_size_limit(tmp_path):
    body = "shared = list(range(100_001))\nvalue = [shared] * 100_002\nvalue.append(value)\nreturn value"
    env, bundle = fixture(tmp_path, body=body)
    result = cached(tmp_path, env, bundle).call(REF, trust=True)
    value = result.value
    assert len(value[0]) == 100_001 and value[0][-1] == 100_000
    assert value[0] is value[-2] and value[-1] is value
    json.dumps(result.payload, allow_nan=False)


@pytest.mark.parametrize("returned", ["Row(7)", "Row", "create", "global_row", "dynamic"])
def test_returned_definitions_outlive_wrapper_and_eventually_release_modules(tmp_path, returned):
    import typing

    body = (
        """from dataclasses import dataclass
@dataclass(slots=True)
class Row:
    value: "GlobalInt"
def create():
    @dataclass(slots=True)
    class Later:
        value: "GlobalInt"
    return Later(7)
global global_row
global_row = Row(7)
dynamic = type('Dynamic', (), {'__module__': __name__, 'value': 7})()
return """
        + returned
    )
    env, bundle = fixture(tmp_path, body=body, compiled_prefix="GlobalInt = int\n")
    result = cached(tmp_path, env, bundle).call(REF, trust=True)
    value = result.value
    name = value.__module__ if returned in {"Row", "create"} else type(value).__module__
    del result
    gc.collect()
    assert name in sys.modules
    instance = value() if returned == "create" else value(7) if returned == "Row" else value
    assert instance.value == 7
    if returned != "dynamic":
        assert typing.get_type_hints(type(instance))["value"] is int
    del value, instance
    gc.collect()
    assert name not in sys.modules


def test_returned_function_can_lazy_import_after_wrapper_deletion(tmp_path):
    extra = {
        "_spl_components/__init__.py": b"",
        "_spl_components/later.py": b"from dataclasses import dataclass\n@dataclass(slots=True)\nclass Row:\n    value: 'int'\n",
    }
    body = "def later():\n    from _spl_components.later import Row\n    return Row(7)\nreturn later"
    env, bundle = fixture(tmp_path, body=body, extra=extra)
    result = cached(tmp_path, env, bundle).call(REF, trust=True)
    later = result.value
    name = later.__module__
    del result
    gc.collect()
    row = later()
    del later
    gc.collect()
    assert row.value == 7 and name in sys.modules
    del row
    gc.collect()
    assert not any(key == name or key.startswith(name + ".") for key in sys.modules)


@pytest.mark.parametrize(
    "body",
    [
        "def values():\n    yield 1\nreturn list(values())",
        "async def helper():\n    await other()\nreturn [1]",
    ],
)
def test_nested_async_generator_helpers_do_not_change_sync_entrypoint(tmp_path, body):
    env, bundle = fixture(tmp_path, body=body)
    assert cached(tmp_path, env, bundle).call(REF, trust=True).value == [1]


def test_nested_helper_import_capture_and_real_generator_rejection(tmp_path):
    for body in ("yield 1", "yield from [1]"):
        with pytest.raises(ContractError, match="async and generator"):
            fixture(tmp_path, body=body)
    with pytest.raises(ContractError, match="pandas.*no captured distribution mapping"):
        fixture(tmp_path, body="def values():\n    import pandas\n    yield 1\nreturn list(values())")
    from spl.public_contract import validate_compiled_members

    members = {"object.yaml": function_yaml(), "object.py": b"async def example():\n    return 1\n"}
    with pytest.raises(ContractError, match="async and generator"):
        validate_compiled_members(members, "example", "function")


def _native_linked_pipeline():
    return b"""- !DPipeline
  name: example
  nodes:
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000001
    func: first
  - !DNodeFunction
    uuid: 00000000-0000-0000-0000-000000000002
    func: second
  links:
  - - !DNodeInputRef
      uuid: 00000000-0000-0000-0000-000000000002
      port: value
    - !DNodeOutputRef
      uuid: 00000000-0000-0000-0000-000000000001
      port: default
  aliases:
  - [answer, 00000000-0000-0000-0000-000000000002]
---
- !DFunction
  name: first
  inputs:
  - {name: start_value, type: null, default: null}
  outputs:
  - {name: default, type: null}
  body: return start_value
---
- !DFunction
  name: second
  inputs:
  - {name: value, type: null, default: null}
  - {name: start_value, type: null, default: null}
  outputs:
  - {name: default, type: null}
  body: |-
    assert value is start_value
    return value
"""


def test_native_pipeline_links_preserve_types_cycles_aliases_and_evidence(tmp_path):
    import numpy as np

    shared = [1, 2]
    cycle = []
    cycle.append(cycle)
    key = object()
    frame = pd.DataFrame({"x": pd.array([1, pd.NA], dtype="Int64")})
    array = np.arange(4)
    values = [
        float("nan"),
        float("inf"),
        cycle,
        {"a": shared, "b": shared},
        range(8),
        {1, 2, 10},
        {key: [frame, array]},
        (frame, array),
    ]
    env, bundle = fixture(tmp_path, yaml_bytes=_native_linked_pipeline(), kind="pipeline")
    client = cached(tmp_path, env, bundle)
    for value in values:
        result = client.call(REF, kwargs={"start_value": value}, trust=True)
        assert set(result.value) == {"answer"}
        assert result.value["answer"]["default"] is value
        manifest = result.payload["manifest"]
        json.dumps(result.payload, allow_nan=False)
        assert all(node["outputs"]["default"]["kind"] == "unfreezable" for node in manifest["nodes"].values())
    assert values[3]["a"] is values[3]["b"] and cycle[0] is cycle
    assert values[6][key][0] is frame and values[6][key][1] is array


def test_native_nested_explicit_artifacts_preserve_unchanged_shared_values(tmp_path):
    from spl.execution_results import PipelineResultNormalizer
    from types import SimpleNamespace

    path = tmp_path / "input.txt"
    path.write_text("artifact bytes")
    frame = pd.DataFrame({"x": [1, 2]})
    shared = [frame]
    declaration = {"__spl_result__": shared, "__spl_artifacts__": {"data.txt": str(path)}}
    value = {"plain": shared, "declared": declaration, "again": declaration}
    value["cycle"] = value
    pipeline = SimpleNamespace(resolve_adapter=lambda **kwargs: None)
    normalizer = PipelineResultNormalizer(pipeline, tmp_path / "files", allow_native_values=True)
    result = normalizer.normalize(value)
    assert result["plain"] is result["declared"] is result["again"] is shared
    assert result["cycle"] is result
    assert value["declared"] is declaration
    assert Path(normalizer.artifacts["data.txt"]).read_text() == "artifact bytes"


def test_legacy_set_order_and_native_set_identity(tmp_path):
    from spl.execution_results import PipelineResultNormalizer
    from types import SimpleNamespace

    pipeline = SimpleNamespace(resolve_adapter=lambda **kwargs: None)
    value = {1, 2, 10}
    assert PipelineResultNormalizer(pipeline, tmp_path).normalize(value) == [1, 10, 2]
    assert PipelineResultNormalizer(pipeline, tmp_path, allow_native_values=True).normalize(value) is value


@pytest.mark.parametrize("length", [2, 5, 2000])
def test_native_unresolved_explicit_wrapper_cycles_have_safe_path_diagnostic(tmp_path, length):
    from spl.execution_results import PipelineResultNormalizer
    from types import SimpleNamespace

    class PrivateData:
        def __repr__(self):
            pytest.fail("diagnostic must not render caller data")

    private = PrivateData()
    wrappers = [{"__spl_artifacts__": {}, "private": private} for _ in range(length)]
    for index, wrapper in enumerate(wrappers):
        wrapper["__spl_result__"] = wrappers[(index + 1) % length]
    value = {"answer": {"default": wrappers[0]}}
    normalizer = PipelineResultNormalizer(
        SimpleNamespace(resolve_adapter=lambda **kwargs: None),
        tmp_path,
        allow_native_values=True,
    )
    with pytest.raises(ValueError) as caught:
        normalizer.normalize(value)
    assert str(caught.value) == "result.answer.default: cyclic explicit result-wrapper chain"
    for index, wrapper in enumerate(wrappers):
        assert wrapper["__spl_result__"] is wrappers[(index + 1) % length]
        assert wrapper["private"] is private


def test_native_long_valid_wrapper_chain_and_shared_aliases(tmp_path):
    from spl.execution_results import PipelineResultNormalizer
    from types import SimpleNamespace

    frame = pd.DataFrame({"x": [1, 2]})
    leaf = [frame]
    wrapper = leaf
    for _ in range(2000):
        wrapper = {"__spl_artifacts__": {}, "__spl_result__": wrapper}
    normalizer = PipelineResultNormalizer(
        SimpleNamespace(resolve_adapter=lambda **kwargs: None),
        tmp_path,
        allow_native_values=True,
    )
    result = normalizer.normalize({"first": wrapper, "second": wrapper, "plain": leaf})
    assert result["first"] is result["second"] is result["plain"] is leaf
    assert leaf[0] is frame


def test_native_wrapper_cycles_through_concrete_containers_and_artifacts(tmp_path):
    from spl.execution_results import PipelineResultNormalizer
    from types import SimpleNamespace

    source = tmp_path / "input.txt"
    source.write_text("artifact content")
    wrapper = {"__spl_artifacts__": {"data.txt": str(source)}}
    items = [wrapper]
    pair = (items,)
    items.append(pair)
    wrapper["__spl_result__"] = pair
    direct = {"__spl_artifacts__": {}, "payload": object()}
    direct["__spl_result__"] = direct
    direct["self"] = direct
    plain_list = []
    plain_list.append(plain_list)
    plain_dict = {}
    plain_dict["self"] = plain_dict
    value = {
        "a": wrapper,
        "b": wrapper,
        "pair": pair,
        "direct": direct,
        "plain_list": plain_list,
        "plain_dict": plain_dict,
    }
    normalizer = PipelineResultNormalizer(
        SimpleNamespace(resolve_adapter=lambda **kwargs: None),
        tmp_path / "files",
        allow_native_values=True,
    )
    result = normalizer.normalize(value)
    assert result["a"] is result["b"] is result["pair"]
    assert result["pair"][0][0] is result["pair"][0][1] is result["pair"]
    assert result["direct"]["self"] is result["direct"]
    assert result["direct"]["payload"] is direct["payload"]
    assert result["plain_list"] is plain_list and result["plain_dict"] is plain_dict
    assert Path(normalizer.artifacts["data.txt"]).read_text() == "artifact content"
    assert list(normalizer.artifacts) == ["data.txt"]
    assert items[0] is wrapper and items[1] is pair and wrapper["__spl_result__"] is pair
    assert direct["__spl_result__"] is direct and direct["self"] is direct


def test_signed_native_pipeline_rejects_wrapper_cycle_once_with_failure_cleanup(tmp_path):
    gc.collect()
    private_before = {name for name in sys.modules if name.startswith("_spl_public_")}
    source = _native_linked_pipeline().replace(
        b"    return value",
        b"""    start_value.append("executed")
    a = {"__spl_artifacts__": {}}
    b = {"__spl_artifacts__": {}}
    a["__spl_result__"] = b
    b["__spl_result__"] = a
    return a""",
    )
    envelope, bundle = fixture(tmp_path, yaml_bytes=source, kind="pipeline")
    client = cached(tmp_path, envelope, bundle)
    events = []
    client._embedded_backend._emit_run_receipt = lambda *a, **k: events.append(k["state"])
    calls = []
    before = (list(sys.path), os.getcwd(), dict(os.environ), sys.stdout, sys.stderr)
    with pytest.raises(ValueError, match="result.answer.default: cyclic explicit result-wrapper chain"):
        client.call(REF, kwargs={"start_value": calls}, trust=True)
    assert calls == ["executed"]
    assert events == ["started", "failure"]
    assert (sys.path, os.getcwd(), dict(os.environ), sys.stdout, sys.stderr) == before
    assert {name for name in sys.modules if name.startswith("_spl_public_")} == private_before
