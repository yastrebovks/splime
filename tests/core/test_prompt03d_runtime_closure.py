from __future__ import annotations

import hashlib
import importlib.metadata
import io
import json
import stat
import subprocess
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from spl.daemon_client import ClientError
from spl.embedded import EmbeddedBackend
from spl.core.ir.utils import spl_compile_to_source
from spl.runtime_environment import (
    PUBLIC_EMBEDDED_EXECUTOR,
    PUBLIC_RUNTIME_LOCK_SCHEMA,
    PUBLIC_RUNTIME_POLICY_NAME,
    PUBLIC_RUNTIME_POLICY_VERSION,
    PUBLIC_RUNTIME_RESOLVER_NAME,
    PUBLIC_RUNTIME_SPDX_ALLOWLIST,
    runtime_lock_document,
    validate_runtime_lock,
)


WORKSPACE = Path(__file__).parents[3]


def test_installed_distribution_exposes_the_public_embedded_host_contract() -> None:
    from spl.daemon.worker import PUBLIC_EMBEDDED_HOST_CONTRACT

    assert importlib.metadata.version("splime") == "0.4.11"
    assert PUBLIC_EMBEDDED_HOST_CONTRACT == "spl.public_embedded_host.v1"


def _wheel(
    name: str,
    version: str,
    *,
    extra_files: dict[str, bytes] | None = None,
) -> bytes:
    dist = f"{name.replace('-', '_')}-{version}.dist-info"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
        members = {
            f"{name.replace('-', '_')}/__init__.py": b"",
            f"{dist}/METADATA": (
                f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\n"
                "License-Expression: MIT\n"
                "Requires-Python: >=3.13\n" + "\n"
            ).encode(),
            f"{dist}/WHEEL": b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n",
            f"{dist}/RECORD": b"",
        }
        members.update(extra_files or {})
        for path, data in members.items():
            info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data)
    return stream.getvalue()


def _artifact(
    name: str,
    version: str,
    payload: bytes,
    *,
    source: dict[str, str],
) -> dict[str, object]:
    return {
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
        "license_expression": "MIT",
        "source": source,
        "direct": True,
        "parent_requirements": [f"{name}=={version}"],
        "evaluated_requirements": [],
        "metadata_url": f"https://pypi.org/pypi/{name}/{version}/json",
        "metadata_sha256": "5" * 64,
    }


def _lock(artifacts: list[dict[str, object]]) -> dict[str, object]:
    return {
        **runtime_lock_document(
            python="3.13",
            requirements=[{"requirement": f"{item['project']}=={item['version']}", "extras": []} for item in artifacts],
            executor=PUBLIC_EMBEDDED_EXECUTOR,
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
    with pytest.raises(ValueError, match="installed-framework v3"):
        validate_runtime_lock(
            {
                "runtime": "venv",
                "python": "3.13",
                "dependencies": [{"package": "example", "version": "1.0"}],
                "lock_hash": "0" * 64,
                "names": ["root"],
            }
        )


def test_runtime_lock_requires_exact_v3_installed_framework_executor() -> None:
    assert PUBLIC_RUNTIME_LOCK_SCHEMA == "spl.public_runtime_lock.v3"
    with pytest.raises(ValueError, match="installed-framework v3"):
        validate_runtime_lock(
            {
                "schema": PUBLIC_RUNTIME_LOCK_SCHEMA,
                "schema_version": 2,
                "target": {"implementation": "cpython", "python": "3.13", "extras": []},
                "policy": {"name": "splime-public-python-artifacts", "version": "0.4.9"},
                "resolver": {"name": "splime-pypi-closure", "version": 1},
                "requirements": [],
                "worker": {"project": "splime-public-worker"},
                "artifacts": [],
                "lock_hash": "0" * 64,
                "names": ["root"],
            }
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("kind", "worker-wheel"),
        ("project", "other-framework"),
        ("contract", "spl.public_embedded_host.v0"),
        ("minimum_version", "0.4.8"),
    ],
)
def test_coordinated_executor_tamper_cannot_rehash_into_v3(field: str, value: str) -> None:
    lock = _lock([])
    executor = dict(lock["executor"])
    executor[field] = value
    lock["executor"] = executor
    identity = {key: item for key, item in lock.items() if key not in {"lock_hash", "names"}}
    lock["lock_hash"] = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    with pytest.raises(ValueError, match="executor"):
        validate_runtime_lock(lock)


def test_installed_framework_version_and_contract_fail_before_environment_creation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from spl import runtime_environment
    from spl.daemon import worker

    lock = _lock([])
    backend = EmbeddedBackend("https://registry.example.test", tmp_path, run_receipts=False)

    class OldDistribution:
        version = "0.4.8"

    monkeypatch.setattr(runtime_environment, "installed_framework_distribution", OldDistribution)
    with pytest.raises(ClientError, match="minimum version"):
        backend._ensure_environments({"runtime_locks": [lock]})
    assert not (tmp_path / "environments").exists()

    monkeypatch.undo()
    monkeypatch.setattr(worker, "PUBLIC_EMBEDDED_HOST_CONTRACT", "spl.public_embedded_host.v0")
    with pytest.raises(ClientError, match="contract is incompatible"):
        backend._ensure_environments({"runtime_locks": [lock]})
    assert not (tmp_path / "environments").exists()


def test_runtime_lock_rejects_first_party_artifacts_and_project_collisions() -> None:
    payload = _wheel("splime", "0.4.9")
    first_party = _artifact(
        "splime",
        "0.4.9",
        payload,
        source={"kind": "official-pypi", "url": "https://files.pythonhosted.org/packages/splime.whl"},
    )
    with pytest.raises(ValueError, match="cannot replace"):
        _lock([first_party])

    dependency = _artifact(
        "Example_Project",
        "1.0",
        _wheel("Example_Project", "1.0"),
        source={"kind": "official-pypi", "url": "https://files.pythonhosted.org/packages/example.whl"},
    )
    collision = {**dependency, "project": "example-project"}
    with pytest.raises(ValueError, match="duplicate project"):
        _lock([dependency, collision])


def test_verified_wheel_cache_is_concurrent_corruption_safe_and_offline_reusable(tmp_path: Path) -> None:
    dependency_bytes = _wheel("example", "1.0")
    dependency_url = "https://files.pythonhosted.org/packages/example-1.0-py3-none-any.whl"
    dependency = _artifact(
        "example",
        "1.0",
        dependency_bytes,
        source={"kind": "official-pypi", "url": dependency_url},
    )
    lock = _lock([dependency])
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
        paths = list(executor.map(lambda _: backend._materialize_wheels(lock, bundle_dir=None), range(4)))
    assert calls == 1
    assert all(paths[0] == item for item in paths)
    assert len(paths[0]) == 1

    backend.artifact_transport.wheel = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("offline cache used network")
    )
    assert backend._materialize_wheels(lock, bundle_dir=None) == paths[0]

    dependency_cache = paths[0][0]
    dependency_cache.write_bytes(b"corrupt")
    backend.artifact_transport.wheel = online
    repaired = backend._materialize_wheels(lock, bundle_dir=None)
    assert repaired[0].read_bytes() == dependency_bytes
    assert calls == 2


def _make_cached_environment_mutable(environment: Path) -> None:
    for path in [environment, *environment.rglob("*")]:
        if path.is_symlink():
            continue
        path.chmod(0o755 if path.is_dir() else 0o644)


def _reseal_cached_environment(environment: Path) -> None:
    for path in environment.rglob("*"):
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(0o555)
        elif path.is_file():
            path.chmod(0o555 if path == environment / "bin" / "python" else 0o444)
    environment.chmod(0o555)


def _dependency_environment(tmp_path: Path) -> tuple[EmbeddedBackend, dict[str, object], bytes, Path, int]:
    payload = _wheel(
        "example",
        "1.0",
        extra_files={
            "example/module.py": b"VALUE = 1\n",
            "example/data.txt": b"authoritative-data\n",
        },
    )
    artifact = _artifact(
        "example",
        "1.0",
        payload,
        source={
            "kind": "official-pypi",
            "url": "https://files.pythonhosted.org/packages/example-1.0-py3-none-any.whl",
        },
    )
    lock = _lock([artifact])
    backend = EmbeddedBackend("https://registry.example.test", tmp_path / "cache", run_receipts=False)
    calls = 0

    def download(url: str, *, expected_size: int) -> bytes:
        nonlocal calls
        calls += 1
        assert expected_size == len(payload)
        return payload

    backend.artifact_transport.wheel = download
    python = backend._ensure_environment(lock)
    return backend, lock, payload, python, calls


@pytest.mark.parametrize(
    "case",
    [
        "module",
        "undeclared-module",
        "delete",
        "package-data",
        "executable",
        "metadata",
        "spl-package",
        "spl-module",
        "data-purelib",
        "data-platlib",
    ],
)
def test_dependency_environment_tampering_rebuilds_from_verified_wheel(
    tmp_path: Path,
    case: str,
) -> None:
    backend, lock, _payload, python, calls = _dependency_environment(tmp_path)
    assert calls == 1
    environment = python.parent.parent
    site_packages = environment / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    module = site_packages / "example" / "module.py"
    data = site_packages / "example" / "data.txt"
    metadata = site_packages / "example-1.0.dist-info" / "METADATA"
    _make_cached_environment_mutable(environment)
    if case == "module":
        module.write_text("VALUE = 2\n", encoding="utf-8")
    elif case == "undeclared-module":
        (site_packages / "injected.py").write_text("VALUE = 1\n", encoding="utf-8")
    elif case == "delete":
        module.unlink()
    elif case == "package-data":
        data.write_text("substituted-data\n", encoding="utf-8")
    elif case == "executable":
        executable = environment / "bin" / "injected"
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o755)
    elif case == "metadata":
        metadata.write_bytes(metadata.read_bytes() + b"X-Tampered: true\n")
    elif case == "spl-package":
        target = site_packages / "spl" / "injected.py"
        target.write_text("VALUE = 1\n", encoding="utf-8")
    elif case == "spl-module":
        (site_packages / "spl.py").write_text("VALUE = 1\n", encoding="utf-8")
    elif case == "data-purelib":
        target = site_packages / "impostor-1.0.data" / "purelib" / "spl" / "injected.py"
        target.parent.mkdir(parents=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
    else:
        target = site_packages / "impostor-1.0.data" / "platlib" / "spl.py"
        target.parent.mkdir(parents=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
    _reseal_cached_environment(environment)
    backend.artifact_transport.wheel = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("valid offline wheel cache used the network")
    )

    selected = backend._ensure_environment(lock)

    assert selected == environment / "bin" / "python"
    assert module.read_bytes() == b"VALUE = 1\n"
    assert data.read_bytes() == b"authoritative-data\n"
    assert not (site_packages / "injected.py").exists()
    assert not (site_packages / "spl.py").exists()
    assert not (site_packages / "spl" / "injected.py").exists()
    assert not (site_packages / "impostor-1.0.data").exists()


def test_valid_environment_reuse_does_not_rebuild_or_redownload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from spl import embedded as embedded_module

    backend, lock, _payload, python, calls = _dependency_environment(tmp_path)
    assert calls == 1
    monkeypatch.setattr(
        embedded_module,
        "_write_environment_tree",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("valid cache rebuilt")),
    )
    backend.artifact_transport.wheel = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("valid cache redownloaded")
    )

    assert backend._ensure_environment(lock) == python


def test_corrupt_environment_rebuilds_offline_but_corrupt_wheel_redownloads_once(tmp_path: Path) -> None:
    backend, lock, payload, python, calls = _dependency_environment(tmp_path)
    assert calls == 1
    environment = python.parent.parent
    module = next(environment.glob("lib/python*/site-packages/example/module.py"))
    _make_cached_environment_mutable(environment)
    module.write_text("VALUE = 2\n", encoding="utf-8")
    _reseal_cached_environment(environment)
    backend.artifact_transport.wheel = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("valid wheel cache used network")
    )
    assert backend._ensure_environment(lock) == python

    _make_cached_environment_mutable(environment)
    module.write_text("VALUE = 3\n", encoding="utf-8")
    _reseal_cached_environment(environment)
    wheel = next((tmp_path / "cache" / "wheels").glob("*/*.whl"))
    wheel.chmod(0o600)
    wheel.write_bytes(b"corrupt")
    redownloads = 0

    def redownload(url: str, *, expected_size: int) -> bytes:
        nonlocal redownloads
        redownloads += 1
        assert expected_size == len(payload)
        return payload

    backend.artifact_transport.wheel = redownload
    assert backend._ensure_environment(lock) == python
    assert redownloads == 1
    assert module.read_bytes() == b"VALUE = 1\n"


@pytest.mark.parametrize(
    "member",
    [
        "spl/__init__.py",
        "impostor-1.0.data/purelib/spl/__init__.py",
        "impostor-1.0.data/platlib/spl.py",
        "bootstrap.pth",
        "sitecustomize.py",
        "usercustomize/__init__.py",
    ],
)
def test_dependency_wheel_cannot_preempt_installed_framework(tmp_path: Path, member: str) -> None:
    payload = _wheel("impostor", "1.0", extra_files={member: b"raise RuntimeError('preempted')\n"})
    artifact = _artifact(
        "impostor",
        "1.0",
        payload,
        source={"kind": "official-pypi", "url": "https://files.pythonhosted.org/packages/impostor.whl"},
    )
    path = tmp_path / str(artifact["filename"])
    path.write_bytes(payload)

    with pytest.raises(ClientError, match="replace|startup hook"):
        EmbeddedBackend._verify_wheel_file(path, artifact)


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
    tmp_path: Path,
) -> None:
    lock = _lock([])
    backend = EmbeddedBackend("https://registry.example.test", tmp_path / "cache", run_receipts=False)
    python = backend._ensure_environment(lock)
    assert not (python.parent.parent / "execution-authority").exists()
    assert not list((python.parent.parent / "lib").glob("**/splime_public_worker*"))

    def invoke(
        object_yaml: Path,
        entrypoint: str,
        payload: dict[str, object],
        *,
        case: str,
        modules: dict[str, str] | None = None,
    ) -> dict[str, object]:
        work = tmp_path / "runs" / case
        work.mkdir(parents=True)
        input_path = work / "input.json"
        result_path = work / "result.json"
        object_python = work / "object.py"
        object_python.write_text(spl_compile_to_source(object_yaml), encoding="utf-8")
        for name, source in (modules or {}).items():
            (work / name).write_text(source, encoding="utf-8")
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
                "spl.daemon.worker",
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

    corpus = WORKSPACE / "spl" / "tests" / "compat" / "corpus" / "v02x"
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
        "    distributions:\n    - !DDistribution\n      package: PyYAML\n      version: 6.0.3",
        "    distributions: []",
    )
    adapter_source = adapter_source.replace(
        "- !DSPLSelfImport\n  name: load_text",
        "  tags:\n"
        "    1bbccde7-8414-419b-b21a-b174cfa6e68c:\n"
        "      runtime: venv-subprocess\n"
        "- !DImportFrom\n  module: public_adapter\n  target: load_text\n  alias: null",
    )
    adapter_source = adapter_source.replace(
        "- !DSPLSelfImport\n  name: save_text",
        "- !DImportFrom\n  module: public_adapter\n  target: save_text\n  alias: null",
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
        modules={
            "public_adapter.py": (
                "def load_text(path):\n"
                "    with open(path, encoding='utf-8') as handle:\n"
                "        return 'loaded:' + handle.read()\n\n"
                "def save_text(path, value):\n"
                "    with open(path, 'w', encoding='utf-8') as handle:\n"
                "        handle.write(value)\n"
            )
        },
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


def test_no_separate_public_worker_distribution_surface_exists() -> None:
    forbidden = "splime" + "-public-worker"
    worker = WORKSPACE / "spl-server" / "src" / "daemon_server" / "public_worker.py"
    workflow = WORKSPACE / "spl-server" / ".gitlab" / "ci" / "public-worker-release.yml"
    verifier = WORKSPACE / "spl-server" / "tools" / "verify_public_worker_release.py"

    assert not worker.exists()
    assert not workflow.exists()
    assert not verifier.exists()
    assert forbidden not in (WORKSPACE / "spl-server" / "pyproject.toml").read_text(encoding="utf-8")
