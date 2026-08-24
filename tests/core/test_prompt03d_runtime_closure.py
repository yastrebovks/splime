from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import stat
import subprocess
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from spl.embedded import EmbeddedBackend
from spl.core.ir.utils import spl_compile_to_source
from spl.runtime_environment import (
    PUBLIC_RUNTIME_LOCK_SCHEMA,
    PUBLIC_RUNTIME_POLICY_NAME,
    PUBLIC_RUNTIME_POLICY_VERSION,
    PUBLIC_RUNTIME_RESOLVER_NAME,
    PUBLIC_RUNTIME_SPDX_ALLOWLIST,
    runtime_lock_document,
    validate_runtime_lock,
)


FRAMEWORK_ROOT = Path(__file__).parents[2]
WORKSPACE = next(
    (
        root
        for root in (Path(__file__).parents[3], Path(__file__).parents[4])
        if (root / "spl-server" / "src" / "daemon_server").is_dir()
    ),
    Path(__file__).parents[3],
)


def _wheel(name: str, version: str) -> bytes:
    dist = f"{name.replace('-', '_')}-{version}.dist-info"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
        members = {
            f"{name.replace('-', '_')}/__init__.py": b"",
            f"{dist}/METADATA": (
                f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\n"
                f"License-Expression: {'Apache-2.0' if name == 'splime-public-worker' else 'MIT'}\n"
                "Requires-Python: >=3.13\n" + "\n"
            ).encode(),
            f"{dist}/WHEEL": b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n",
            f"{dist}/RECORD": b"",
        }
        if name == "splime-public-worker":
            members["splime_public_worker/__main__.py"] = b"_AUTHORITY_ROOT = 'signed'\nfrom spl.daemon import worker\n"
            members["splime_public_worker/_authority/spl/daemon/worker.py"] = b"# signed fixture authority\n"
        for path, data in members.items():
            info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data)
    return stream.getvalue()


def _artifact(name: str, version: str, payload: bytes, *, source: dict[str, str], worker: bool) -> dict[str, object]:
    base: dict[str, object] = {
        "project": name,
        "version": version,
        "filename": f"{name.replace('-', '_')}-{version}-py3-none-any.whl",
        "size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "tags": ["py3-none-any"],
        "requires_python": ">=3.13",
        "yanked": False,
        "root_is_purelib": True,
        "native_members": [],
        "license_expression": "Apache-2.0" if worker else "MIT",
        "source": source,
    }
    if worker:
        base["build_identity"] = "4" * 64
        base["requires_dist"] = []
    else:
        base.update(
            {
                "direct": True,
                "parent_requirements": [f"{name}=={version}"],
                "evaluated_requirements": [],
                "metadata_url": f"https://pypi.org/pypi/{name}/{version}/json",
                "metadata_sha256": "5" * 64,
            }
        )
    return base


def _lock(worker: dict[str, object], artifacts: list[dict[str, object]]) -> dict[str, object]:
    return {
        **runtime_lock_document(
            python="3.13",
            requirements=[{"requirement": f"{item['project']}=={item['version']}", "extras": []} for item in artifacts],
            worker=worker,
            artifacts=artifacts,
            policy={
                "name": PUBLIC_RUNTIME_POLICY_NAME,
                "version": PUBLIC_RUNTIME_POLICY_VERSION,
                "spdx_allowlist": list(PUBLIC_RUNTIME_SPDX_ALLOWLIST),
                "wheel_policy": "non-yanked-universal-pure-python-only",
            },
            resolver={"name": PUBLIC_RUNTIME_RESOLVER_NAME, "version": 1},
        ),
        "names": ["root"],
    }


def test_direct_package_version_lock_is_rejected_as_under_specified() -> None:
    with pytest.raises(ValueError, match="complete artifact closure"):
        validate_runtime_lock(
            {
                "runtime": "venv",
                "python": "3.13",
                "dependencies": [{"package": "example", "version": "1.0"}],
                "lock_hash": "0" * 64,
                "names": ["root"],
            }
        )


def test_runtime_lock_requires_an_exact_worker_and_every_wheel_hash() -> None:
    assert PUBLIC_RUNTIME_LOCK_SCHEMA == "spl.public_runtime_lock.v2"
    with pytest.raises(ValueError, match="worker artifact"):
        validate_runtime_lock(
            {
                "schema": PUBLIC_RUNTIME_LOCK_SCHEMA,
                "schema_version": 2,
                "target": {"implementation": "cpython", "python": "3.13", "extras": []},
                "policy": {"name": "splime-public-python-artifacts", "version": "0.4.8"},
                "resolver": {"name": "splime-pypi-closure", "version": 1},
                "requirements": [],
                "artifacts": [],
                "lock_hash": "0" * 64,
                "names": ["root"],
            }
        )


def test_verified_wheel_cache_is_concurrent_corruption_safe_and_offline_reusable(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    worker_bytes = _wheel("splime-public-worker", "0.4.8")
    worker_digest = hashlib.sha256(worker_bytes).hexdigest()
    member = f"runtime-artifacts/{worker_digest}/splime_public_worker-0.4.8-py3-none-any.whl"
    member_path = bundle.joinpath(*member.split("/"))
    member_path.parent.mkdir(parents=True)
    member_path.write_bytes(worker_bytes)
    worker = _artifact(
        "splime-public-worker",
        "0.4.8",
        worker_bytes,
        source={"kind": "candidate-bundle", "bundle_member": member},
        worker=True,
    )
    dependency_bytes = _wheel("example", "1.0")
    dependency_url = "https://files.pythonhosted.org/packages/example-1.0-py3-none-any.whl"
    dependency = _artifact(
        "example",
        "1.0",
        dependency_bytes,
        source={"kind": "official-pypi", "url": dependency_url},
        worker=False,
    )
    lock = _lock(worker, [dependency])
    backend = EmbeddedBackend("https://registry.example.test", tmp_path / "cache", run_receipts=False)
    calls = 0

    def online(url: str, *, expected_size: int) -> bytes:
        nonlocal calls
        calls += 1
        assert url == dependency_url
        assert expected_size == len(dependency_bytes)
        return dependency_bytes

    backend.artifact_transport.wheel = online
    with ThreadPoolExecutor(max_workers=4) as executor:
        paths = list(executor.map(lambda _: backend._materialize_wheels(lock, bundle_dir=bundle), range(4)))
    assert calls == 1
    assert all(paths[0] == item for item in paths)

    backend.artifact_transport.wheel = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("offline cache used network")
    )
    assert backend._materialize_wheels(lock, bundle_dir=bundle) == paths[0]

    dependency_cache = paths[0][1]
    dependency_cache.write_bytes(b"corrupt")
    backend.artifact_transport.wheel = online
    repaired = backend._materialize_wheels(lock, bundle_dir=bundle)
    assert repaired[1].read_bytes() == dependency_bytes
    assert calls == 2


def _unsafe_wheel(
    name: str,
    *,
    mode: int = stat.S_IFREG | 0o644,
    compression: int = zipfile.ZIP_STORED,
) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=compression) as archive:
        members = {
            name: b"payload",
            "unsafe-1.0.dist-info/METADATA": b"Metadata-Version: 2.4\nName: unsafe\nVersion: 1.0\n\n",
            "unsafe-1.0.dist-info/WHEEL": (b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n"),
        }
        for path, data in members.items():
            info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = mode << 16
            info.compress_type = compression
            archive.writestr(info, data)
    return stream.getvalue()


@pytest.mark.parametrize(
    "payload",
    [
        _unsafe_wheel("link", mode=stat.S_IFLNK | 0o777),
        _unsafe_wheel("hardlink", mode=stat.S_IFREG | stat.S_ISGID | 0o644),
        _unsafe_wheel("device", mode=stat.S_IFCHR | 0o600),
        _unsafe_wheel("C:/absolute.py"),
        _unsafe_wheel("compressed.py", compression=zipfile.ZIP_BZIP2),
    ],
)
def test_consumer_rejects_adversarial_wheel_members(tmp_path: Path, payload: bytes) -> None:
    path = tmp_path / "unsafe-1.0-py3-none-any.whl"
    path.write_bytes(payload)
    artifact = {
        "size": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    with pytest.raises(Exception, match="unsafe|regular|compression"):
        EmbeddedBackend._verify_wheel_file(path, artifact)


@pytest.mark.parametrize(
    "expression",
    [
        "(MIT",
        "MIT)",
        "()",
        "MIT AND",
        "AND MIT",
        "MIT OR OR Apache-2.0",
        "MIT Apache-2.0",
        "((MIT) OR (Apache-2.0 AND BSD-3-Clause)) trailing",
    ],
)
def test_consumer_spdx_parser_is_strict_and_balanced(expression: str) -> None:
    from spl.runtime_environment import spdx_allowed

    assert spdx_allowed(expression) is False


def test_isolated_public_bridge_executes_the_authoritative_contract_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker_path = WORKSPACE / "spl-server" / "src" / "daemon_server" / "public_worker.py"
    spec = importlib.util.spec_from_file_location("prompt03d_public_worker", worker_path)
    assert spec is not None and spec.loader is not None
    worker_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker_module)
    built = worker_module.build_worker_wheel(
        output_dir=tmp_path / "worker",
        license_path=FRAMEWORK_ROOT / "LICENSE",
        notice_path=FRAMEWORK_ROOT / "NOTICE",
    )
    worker_payload = Path(built["path"]).read_bytes()
    worker = _artifact(
        "splime-public-worker",
        "0.4.8",
        worker_payload,
        source={
            "kind": "candidate-bundle",
            "bundle_member": (f"runtime-artifacts/{built['sha256']}/{built['filename']}"),
        },
        worker=True,
    )
    worker["build_identity"] = built["build_identity"]
    lock = _lock(worker, [])
    backend = EmbeddedBackend("https://registry.example.test", tmp_path / "cache", run_receipts=False)
    monkeypatch.setattr(
        backend,
        "_materialize_wheels",
        lambda lock, **kwargs: [Path(built["path"])],
    )
    python = backend._ensure_environment(lock)
    assert not (python.parent.parent / "execution-authority").exists()

    def invoke(
        object_yaml: Path,
        entrypoint: str,
        payload: dict[str, object],
        *,
        case: str,
    ) -> dict[str, object]:
        work = tmp_path / "runs" / case
        work.mkdir(parents=True)
        input_path = work / "input.json"
        result_path = work / "result.json"
        object_python = work / "object.py"
        object_python.write_text(spl_compile_to_source(object_yaml), encoding="utf-8")
        input_path.write_text(json.dumps(payload), encoding="utf-8")
        environment = {
            "PATH": str(python.parent),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONUTF8": "1",
            "PYTHONNOUSERSITE": "1",
            "SPL_EMBEDDED_MODE": "1",
            "TMPDIR": str(work),
        }
        completed = subprocess.run(
            [
                str(python),
                "-I",
                "-m",
                "splime_public_worker",
                "--object-python",
                str(object_python),
                "--entrypoint",
                entrypoint,
                "--input",
                str(input_path),
                "--result",
                str(result_path),
                "--artifacts-dir",
                str(work / "artifacts"),
            ],
            cwd=work,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        return json.loads(result_path.read_text(encoding="utf-8"))

    corpus = FRAMEWORK_ROOT / "tests" / "compat" / "corpus" / "v02x"
    assert (
        invoke(
            corpus / "functional_node.yaml",
            "compat_constant",
            {"args": [], "kwargs": {}},
            case="function",
        )["result"]
        == 41
    )
    assert invoke(
        corpus / "scalar_pipeline.yaml",
        "scalar_pipeline",
        {"args": [], "kwargs": {}, "output": "sum"},
        case="scalar",
    )["result"] == {"default": 7}
    assert invoke(
        corpus / "multinode_dag.yaml",
        "multinode_dag",
        {"args": [], "kwargs": {}, "output": None},
        case="aliases",
    )["result"] == {"doubled": {"default": 8}, "total": {"default": 12}}

    adapter_source = (corpus / "adapter_alias_pipeline.yaml").read_text(encoding="utf-8")
    adapter_source = adapter_source.replace(
        "- !DSPLSelfImport\n  name: load_text",
        "  tags:\n"
        "    1bbccde7-8414-419b-b21a-b174cfa6e68c:\n"
        "      runtime: venv-subprocess\n"
        "- !DSPLSelfImport\n  name: load_text",
    )
    adapter_yaml = tmp_path / "adapter-multi-runtime.yaml"
    adapter_yaml.write_text(adapter_source, encoding="utf-8")
    assert invoke(
        adapter_yaml,
        "adapter_pipeline",
        {
            "args": [],
            "kwargs": {},
            "output": "result",
            "node_runtime_environments": {
                "venv-subprocess": {
                    "default": {
                        "python_path": str(python),
                        "lock_hash": lock["lock_hash"],
                    }
                }
            },
        },
        case="formatted-adapter-multi-runtime",
    )["result"] == {"default": "loaded:hello|consumed"}

    source_ports = tmp_path / "source-ports.yaml"
    source_ports.write_text(
        """- !DPipeline
  name: source_ports
  nodes:
  - !DNodeFunction
    uuid: 11111111-1111-4111-8111-111111111111
    func: scored
  - !DNodeFunction
    uuid: 22222222-2222-4222-8222-222222222222
    func: label
  links:
  - - !DNodeInputRef
      uuid: 11111111-1111-4111-8111-111111111111
      port: value
    - !DScalar
      value: 6
  - - !DNodeInputRef
      uuid: 22222222-2222-4222-8222-222222222222
      port: score
    - !DNodeOutputRef
      uuid: 11111111-1111-4111-8111-111111111111
      port: score
  aliases:
  - - answer
    - 22222222-2222-4222-8222-222222222222
  adapters: []
- !DSPLSelfImport
  name: scored
- !DSPLSelfImport
  name: label
---
- !DFunction
  name: scored
  inputs:
  - name: value
    type: int
    default: null
  outputs:
  - name: score
    type: int
  body: |-
    return value * 2
---
- !DFunction
  name: label
  inputs:
  - name: score
    type: int
    default: null
  outputs:
  - name: default
    type: str
  body: |-
    return f'score:{score}'
""",
        encoding="utf-8",
    )
    assert invoke(
        source_ports,
        "source_ports",
        {"args": [], "kwargs": {}, "output": "answer"},
        case="source-port",
    )["result"] == {"default": "score:12"}

    multi_output = tmp_path / "multi-output.yaml"
    multi_output.write_text(
        """- !DFunction
  name: pair
  inputs:
  - name: value
    type: int
    default: null
  outputs:
  - name: left
    type: int
  - name: right
    type: int
  body: |-
    left = int(imported_math.sqrt(value * value))
    return {'left': left, 'right': left + 1}
- !DImport
  module: math
  alias: imported_math
""",
        encoding="utf-8",
    )
    assert invoke(
        multi_output,
        "pair",
        {"args": [10], "kwargs": {}},
        case="multiple-outputs-and-import",
    )["result"] == {"left": 10, "right": 11}

    artifact_yaml = tmp_path / "artifact.yaml"
    artifact_yaml.write_text(
        """- !DFunction
  name: artifact
  inputs: []
  outputs:
  - name: default
    type: dict
  body: |-
    from pathlib import Path
    Path('proof.txt').write_text('authority', encoding='utf-8')
    return {'__spl_result__': 'done', '__spl_artifacts__': {'proof.txt': 'proof.txt'}}
""",
        encoding="utf-8",
    )
    artifact_result = invoke(
        artifact_yaml,
        "artifact",
        {"args": [], "kwargs": {}},
        case="artifact",
    )
    assert artifact_result["result"] == "done"
    assert Path(artifact_result["artifacts"]["proof.txt"]).read_text() == "authority"
