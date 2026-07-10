"""Worker process for executing one registered SPL object.

The daemon itself should not import and execute user objects in-process.  This
worker is launched as a subprocess with the Python executable registered for an
object.  That gives the MVP its most important boundary: each object runs with
the packages and interpreter of its own environment.

The worker receives file paths instead of a network connection:

* ``input.json`` contains call arguments and optional pipeline output selector;
* ``result.json`` is written on success;
* ``artifacts/`` receives files declared by the object result;
* stdout/stderr are captured by the daemon for diagnostics.

For the first version, arguments and return values are JSON-like.  This keeps
the protocol transparent and avoids silently pickling arbitrary objects.  Large
or non-JSON outputs should be returned as artifacts.
"""

from __future__ import annotations

import os
import sys


def _prefer_runtime_env_over_pythonpath_site_packages() -> None:
    version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    protected_candidates = [
        os.path.normcase(os.path.abspath(os.path.join(sys.base_prefix, "Lib"))),
        os.path.normcase(os.path.abspath(os.path.join(sys.prefix, "Lib", "site-packages"))),
        os.path.normcase(os.path.abspath(os.path.join(sys.base_prefix, "lib", version))),
        os.path.normcase(os.path.abspath(os.path.join(sys.prefix, "lib", version, "site-packages"))),
        os.path.normcase(os.path.abspath(os.path.join(sys.prefix, "lib64", version, "site-packages"))),
    ]
    protected_indexes = [
        index for index, item in enumerate(sys.path) if os.path.normcase(os.path.abspath(item)) in protected_candidates
    ]
    if not protected_indexes:
        return

    first_protected_index = min(protected_indexes)
    early_external_site_packages = [
        item
        for index, item in enumerate(sys.path)
        if index < first_protected_index
        and "site-packages" in os.path.normcase(os.path.abspath(item))
        and os.path.normcase(os.path.abspath(item)) not in protected_candidates
    ]
    if not early_external_site_packages:
        return

    sys.path[:] = [item for item in sys.path if item not in early_external_site_packages]
    last_protected_index = max(
        index for index, item in enumerate(sys.path) if os.path.normcase(os.path.abspath(item)) in protected_candidates
    )
    for item in reversed(early_external_site_packages):
        sys.path.insert(last_protected_index + 1, item)


_prefer_runtime_env_over_pythonpath_site_packages()

import argparse
import importlib.metadata
import json
import re
import shutil
from collections.abc import Iterable, Mapping, Sequence
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from typing import Any, Literal, cast, overload
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from spl.core import node_runtime as m_node_runtime
from spl.core.entities.adapter import Adapter
from spl.core.entities.distribution import DDistribution
from spl.daemon.store import validate_name

ARTIFACTS_KEY = "__spl_artifacts__"
ARTIFACT_REF_KEY = "__spl_artifact_ref__"
RESULT_KEY = "__spl_result__"
_JSON_SCALAR_TYPES = (str, int, float, bool)
_ARTIFACT_NAME_TOKEN_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")
DEFAULT_REMOTE_NODE_HTTP_TIMEOUT_SECONDS: float | None = None


def read_json(path: Path) -> Any:
    """Read a UTF-8 JSON file."""

    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    """Write a UTF-8 JSON file with stable formatting."""

    _ensure_private_dir(path.parent)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
    _chmod_owner_file(path)


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except OSError:
        pass


def _chmod_owner_file(path: Path) -> None:
    try:
        path.chmod(0o600)
    except OSError:
        pass


class RemoteNodeClient:
    """Small worker-side bridge back to the local daemon for NodeRemote runs."""

    def __init__(
        self,
        daemon_url: str,
        *,
        timeout_seconds: float | None = None,
    ):
        self.daemon_url = daemon_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def run_node(self, node: Any, kwargs: dict[str, Any]) -> Any:
        node_payload: dict[str, Any] = {
            "uuid": str(node.uuid),
            "url": node.url,
            "name": node.name,
            "version": node.version,
        }
        payload: dict[str, Any] = {
            "node": node_payload,
            "kwargs": kwargs,
            "timeout_seconds": self.timeout_seconds,
        }
        target_machine = getattr(node, "target_machine", None)
        if target_machine is not None:
            node_payload["target_machine"] = target_machine
        owner_id = getattr(node, "owner_id", None)
        if owner_id is not None:
            node_payload["owner_id"] = owner_id
        library = getattr(node, "library", None)
        if library is not None:
            node_payload["library"] = library
        request = Request(
            f"{self.daemon_url}/remote-nodes/run",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
            },
            method="POST",
        )
        try:
            # /remote-nodes/run is a blocking call: the daemon polls the
            # server-side run until the node reaches a terminal state.  urllib
            # exposes one socket timeout for both connect and read, so an
            # unbounded run must not inherit the short control-plane default.
            timeout = (
                self.timeout_seconds if self.timeout_seconds is not None else DEFAULT_REMOTE_NODE_HTTP_TIMEOUT_SECONDS
            )
            with urlopen(request, timeout=timeout) as response:  # noqa: S310 - local daemon URL.
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            try:
                message = json.loads(raw).get("error", raw)
            except json.JSONDecodeError:
                message = raw
            raise RuntimeError(f"remote node call failed: {message}") from exc
        except URLError as exc:
            raise RuntimeError(f"local daemon is not reachable for remote node call: {exc.reason}") from exc
        return json.loads(raw).get("value")


class WorkerNodeEnvironmentProvider:
    """Resolve worker-provided node runtime environments."""

    def __init__(self, node_runtime_environments: Mapping[str, Any] | None = None):
        self.node_runtime_environments = dict(node_runtime_environments or {})
        self.default_provider = m_node_runtime.CurrentPythonEnvironmentProvider()

    def prepare(
        self,
        spec: Mapping[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> m_node_runtime.PreparedNodeEnvironment:
        runtime_name = spec.get("node_runtime")
        if runtime_name != m_node_runtime.DOCKER_NODE_RUNTIME:
            return self.default_provider.prepare(spec, wait=wait, retry_failed=retry_failed)

        docker_environment = self.node_runtime_environments.get(m_node_runtime.DOCKER_NODE_RUNTIME)
        metadata = docker_environment if isinstance(docker_environment, Mapping) else {}
        image_tag = metadata.get("image_tag")
        if not isinstance(image_tag, str) or not image_tag:
            return m_node_runtime.PreparedNodeEnvironment(name="docker-image", python_path=None, metadata={})
        return m_node_runtime.PreparedNodeEnvironment(
            name="docker-image",
            python_path=None,
            metadata={
                "image_tag": image_tag,
                "spec_hash": metadata.get("spec_hash"),
                "source": metadata.get("source"),
            },
        )


def validate_environment(distributions: list[dict[str, str]]) -> None:
    """Fail fast when the worker interpreter does not match SPL metadata.

    The daemon selects a registered Python executable, but the SPL object itself
    describes package versions through ``DDistribution`` records.  Checking them
    inside the worker makes the run exact for the interpreter that will actually
    execute user code.
    """

    mismatches = []
    for distribution in distributions:
        package = distribution["package"]
        expected = distribution["version"]
        try:
            actual = importlib.metadata.version(package)
        except PackageNotFoundError:
            mismatches.append(f"{package}=={expected} is not installed")
            continue
        if actual != expected:
            mismatches.append(f"{package}=={expected} is required, actual version is {actual}")

    if mismatches:
        raise RuntimeError("worker environment does not match SPL metadata: " + "; ".join(mismatches))


def to_jsonable(value: Any) -> Any:
    """Convert common Python containers into JSON-compatible values.

    The function is intentionally strict for unknown objects.  A daemon that
    silently converts everything with ``repr`` would be hard to use correctly:
    the caller might think it received a reusable result while actually getting
    a display string.
    """

    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [to_jsonable(item) for item in value]
    if isinstance(value, set):
        return [to_jsonable(item) for item in sorted(value, key=repr)]
    raise TypeError("result is not JSON serializable; return JSON-like data or declare artifacts")


def safe_artifact_name(name: str) -> str:
    """Validate an artifact name before writing under the artifacts directory."""

    return validate_name(name)


def copy_artifact(source: Path, target: Path) -> None:
    """Copy one artifact file or directory into the run artifact directory."""

    if not source.exists():
        raise ValueError(f"artifact source is not found: {source}")
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
        _chmod_artifact_tree(target)
    else:
        _ensure_private_dir(target.parent)
        shutil.copy2(source, target)
        _chmod_owner_file(target)


def collect_artifacts(value: Any, artifacts_dir: Path) -> tuple[Any, dict[str, str]]:
    """Extract and copy artifacts declared by the function result.

    Convention for MVP::

        {
          "__spl_result__": {"score": 0.91},
          "__spl_artifacts__": {"model.pkl": "relative/or/absolute/path.pkl"}
        }

    If ``__spl_result__`` is omitted, the result is the original dictionary
    without the two reserved SPL keys.
    """

    if not isinstance(value, Mapping) or ARTIFACTS_KEY not in value:
        return value, {}

    artifact_spec = value[ARTIFACTS_KEY]
    if RESULT_KEY in value:
        result = value[RESULT_KEY]
    else:
        result = {key: item for key, item in value.items() if key not in {ARTIFACTS_KEY, RESULT_KEY}}

    items: Iterable[tuple[Any, Any]]
    if isinstance(artifact_spec, Mapping):
        items = artifact_spec.items()
    elif isinstance(artifact_spec, Sequence) and not isinstance(artifact_spec, str):
        items = ((Path(str(path)).name, path) for path in artifact_spec)
    else:
        raise TypeError("__spl_artifacts__ must be a mapping or a list of paths")

    copied: dict[str, str] = {}
    _ensure_private_dir(artifacts_dir)
    for name, source in items:
        artifact_name = safe_artifact_name(str(name))
        source_path = Path(str(source)).expanduser().absolute()
        target_path = artifacts_dir / artifact_name
        copy_artifact(source_path, target_path)
        copied[artifact_name] = str(target_path)

    return result, copied


def _type_name(value: Any) -> str:
    typ = type(value)
    if typ.__module__ == "builtins":
        return typ.__qualname__
    return f"{typ.__module__}.{typ.__qualname__}"


def _result_path(parts: Sequence[str]) -> str:
    return ".".join(parts)


def _artifact_name_token(value: str) -> str:
    token = _ARTIFACT_NAME_TOKEN_PATTERN.sub("_", str(value)).strip("._-")
    return token or "value"


def _with_numeric_suffix(name: str, index: int) -> str:
    stem, separator, suffix = name.rpartition(".")
    if stem and separator and suffix:
        return f"{stem}-{index}.{suffix}"
    return f"{name}-{index}"


def _chmod_artifact_tree(path: Path) -> None:
    if path.is_dir():
        try:
            path.chmod(0o700)
        except OSError:
            pass
        for item in path.rglob("*"):
            if item.is_dir():
                try:
                    item.chmod(0o700)
                except OSError:
                    pass
            elif item.is_file():
                _chmod_owner_file(item)
    elif path.is_file():
        _chmod_owner_file(path)


class PipelineResultNormalizer:
    """Convert final pipeline values into the daemon's JSON/artifact protocol."""

    def __init__(self, pipeline: Any, artifacts_dir: Path):
        self.pipeline = pipeline
        self.artifacts_dir = artifacts_dir
        self.artifacts: dict[str, str] = {}
        self._used_artifact_names: set[str] = set()

    def normalize(self, value: Any, path: tuple[str, ...] = ("result",)) -> Any:
        if value is None or isinstance(value, _JSON_SCALAR_TYPES):
            return value
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, Mapping):
            if ARTIFACTS_KEY in value:
                result = self._result_from_explicit_artifact_mapping(value)
                self._copy_declared_artifacts(value[ARTIFACTS_KEY])
                return self.normalize(result, path)
            return {str(key): self.normalize(item, (*path, str(key))) for key, item in value.items()}
        if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
            return [self.normalize(item, (*path, str(index))) for index, item in enumerate(value)]
        if isinstance(value, set):
            return [self.normalize(item, (*path, str(index))) for index, item in enumerate(sorted(value, key=repr))]
        return self._materialize_adapter_artifact(value, path)

    @staticmethod
    def _result_from_explicit_artifact_mapping(value: Mapping[Any, Any]) -> Any:
        if RESULT_KEY in value:
            return value[RESULT_KEY]
        return {key: item for key, item in value.items() if key not in {ARTIFACTS_KEY, RESULT_KEY}}

    def _copy_declared_artifacts(self, artifact_spec: Any) -> None:
        items: Iterable[tuple[Any, Any]]
        if isinstance(artifact_spec, Mapping):
            items = artifact_spec.items()
        elif isinstance(artifact_spec, Sequence) and not isinstance(
            artifact_spec,
            str | bytes | bytearray,
        ):
            items = ((Path(str(path)).name, path) for path in artifact_spec)
        else:
            raise TypeError("__spl_artifacts__ must be a mapping or a list of paths")

        _ensure_private_dir(self.artifacts_dir)
        for name, source in items:
            artifact_name = self._reserve_artifact_name(safe_artifact_name(str(name)))
            source_path = Path(str(source)).expanduser().absolute()
            target_path = self.artifacts_dir / artifact_name
            if source_path.resolve() != target_path.resolve():
                copy_artifact(source_path, target_path)
            self.artifacts[artifact_name] = str(target_path)

    def _materialize_adapter_artifact(
        self,
        value: Any,
        path: tuple[str, ...],
    ) -> dict[str, Any]:
        adapter = self._resolve_adapter(value, path)
        artifact_name = self._artifact_name(path, adapter.format)
        artifact_path = self.artifacts_dir / artifact_name
        _ensure_private_dir(self.artifacts_dir)

        try:
            adapter.save(str(artifact_path), value)
        except BaseException:
            artifact_path.unlink(missing_ok=True)
            raise

        from spl.core.entities.artifact import compute_sha256

        size = artifact_path.stat().st_size
        sha256 = compute_sha256(artifact_path)
        self.artifacts[artifact_name] = str(artifact_path)
        _chmod_owner_file(artifact_path)
        return {
            ARTIFACT_REF_KEY: True,
            "name": artifact_name,
            "key": adapter.key,
            "format": adapter.format,
            "size": size,
            "sha256": sha256,
        }

    def _resolve_adapter(self, value: Any, path: tuple[str, ...]) -> Any:
        try:
            adapter = self.pipeline.resolve_adapter(py_type=type(value))
        except ValueError as exc:
            raise TypeError(
                f"{_result_path(path)} {_type_name(value)} is not JSON serializable; "
                f"add_adapter({_type_name(value)}, ...) or remove ambiguous adapters"
            ) from exc
        if adapter is None:
            raise TypeError(
                f"{_result_path(path)} {_type_name(value)} is not JSON serializable; "
                f"add_adapter({_type_name(value)}, ...)"
            )
        return adapter

    def _artifact_name(self, path: tuple[str, ...], format_name: str) -> str:
        parts = [_artifact_name_token(part) for part in path[1:]]
        if len(parts) > 1 and parts[-1] == "default":
            parts = parts[:-1]
        if not parts:
            parts = ["result"]
        base_name = ".".join(parts)
        format_token = _artifact_name_token(format_name)
        return self._reserve_artifact_name(f"{base_name}.{format_token}")

    def _reserve_artifact_name(self, name: str) -> str:
        candidate = safe_artifact_name(name)
        index = 2
        while candidate in self._used_artifact_names:
            candidate = safe_artifact_name(_with_numeric_suffix(name, index))
            index += 1
        self._used_artifact_names.add(candidate)
        return candidate


@overload
def run_pipeline(
    pipeline: Any,
    kwargs: dict[str, Any],
    output: str | None,
    *,
    daemon_url: str,
    timeout_seconds: float | None,
    artifacts_dir: Path,
    namespace: Mapping[str, Any] | None = None,
    runtime_config: dict[str, Any] | None = None,
    runtime_env_spec: list[dict[str, Any]] | None = None,
    node_runtime_environments: Mapping[str, Any] | None = None,
    runtimes: dict[str, str] | None = None,
    resume: Mapping[str, Any] | None = None,
    include_manifest: Literal[False] = False,
) -> tuple[Any, dict[str, str]]: ...


@overload
def run_pipeline(
    pipeline: Any,
    kwargs: dict[str, Any],
    output: str | None,
    *,
    daemon_url: str,
    timeout_seconds: float | None,
    artifacts_dir: Path,
    namespace: Mapping[str, Any] | None = None,
    runtime_config: dict[str, Any] | None = None,
    runtime_env_spec: list[dict[str, Any]] | None = None,
    node_runtime_environments: Mapping[str, Any] | None = None,
    runtimes: dict[str, str] | None = None,
    resume: Mapping[str, Any] | None = None,
    include_manifest: Literal[True],
) -> tuple[Any, dict[str, str], dict[str, Any] | None]: ...


def run_pipeline(
    pipeline: Any,
    kwargs: dict[str, Any],
    output: str | None,
    *,
    daemon_url: str,
    timeout_seconds: float | None,
    artifacts_dir: Path,
    namespace: Mapping[str, Any] | None = None,
    runtime_config: dict[str, Any] | None = None,
    runtime_env_spec: list[dict[str, Any]] | None = None,
    node_runtime_environments: Mapping[str, Any] | None = None,
    runtimes: dict[str, str] | None = None,
    resume: Mapping[str, Any] | None = None,
    include_manifest: bool = False,
) -> tuple[Any, dict[str, str]] | tuple[Any, dict[str, str], dict[str, Any] | None]:
    """Run a ``spl.core`` pipeline without changing the existing core files.

    The current core exposes ``Deployment`` and ``Run`` but does not provide a
    direct "give me the final output" helper.  This adapter supplies the minimal
    selection rules the daemon needs:

    * if ``output`` is given, it must be a pipeline alias;
    * otherwise aliases are returned as a dictionary;
    * a single-node pipeline can be returned without an alias;
    * multi-node pipelines should define aliases for daemon use.
    """

    from spl.core._common import Deployment

    client = RemoteNodeClient(daemon_url, timeout_seconds=timeout_seconds)
    node_environment_provider = WorkerNodeEnvironmentProvider(node_runtime_environments)
    try:
        deployment = Deployment(
            client,
            pipeline,
            runtime_config=runtime_config,
            node_environment_provider=node_environment_provider,
            runtime_env_spec=runtime_env_spec,
        )
    except TypeError:
        # Older framework builds did not require a client for local-only
        # pipelines.  Keep the worker tolerant while NodeRemote is still moving.
        # The provider still carries daemon-prepared node Docker environments.
        deployment = Deployment(
            pipeline,
            runtime_config=runtime_config,
            node_environment_provider=node_environment_provider,
            runtime_env_spec=runtime_env_spec,
        )
    normalizer = PipelineResultNormalizer(pipeline, artifacts_dir)
    previous_runs_home = os.environ.get("SPL_RUNS_HOME")
    os.environ["SPL_RUNS_HOME"] = str(artifacts_dir.parent / "pipeline-state")
    try:
        if resume is None:
            run = deployment.run(runtimes=runtimes, keep=True, **kwargs)
        else:
            run = deployment.resume(
                _resume_parent_run_dir(resume, artifacts_dir=artifacts_dir),
                from_=_resume_from_selection(resume),
                adapters=_adapter_overrides_from_payload(pipeline, namespace or {}, resume.get("adapters")),
                runtimes=runtimes,
                kwargs=_resume_kwargs(resume),
                keep=True,
            )
        with run:
            if output is not None:
                result = run[pipeline.get_node_by_alias(output)]
            elif pipeline.aliases:
                result = {
                    alias: run[node]
                    for alias, node in sorted(
                        pipeline.aliases.items(),
                        key=lambda item: item[0],
                    )
                }
            elif len(pipeline.nodes) == 1:
                [node] = list(pipeline.nodes)
                result = run[node]
            else:
                raise ValueError("pipeline has multiple nodes and no aliases; pass output or register aliases")
    finally:
        if previous_runs_home is None:
            os.environ.pop("SPL_RUNS_HOME", None)
        else:
            os.environ["SPL_RUNS_HOME"] = previous_runs_home

    run_manifest = read_json(run.manifest_path) if run.manifest_path is not None else None
    if output is not None:
        normalized = normalizer.normalize(result, ("result", output))
        if include_manifest:
            return normalized, normalizer.artifacts, run_manifest
        return normalized, normalizer.artifacts

    normalized = normalizer.normalize(result)
    if include_manifest:
        return normalized, normalizer.artifacts, run_manifest
    return normalized, normalizer.artifacts


def _resume_parent_run_dir(resume: Mapping[str, Any], *, artifacts_dir: Path) -> str:
    value = resume.get("parent_run_dir")
    if not isinstance(value, str) or not value:
        raise ValueError("resume parent_run_dir must be a non-empty string")
    path = Path(value)
    if path.is_absolute():
        return value
    return str(artifacts_dir.parent / path)


def _resume_from_selection(resume: Mapping[str, Any]) -> Any:
    if "from" in resume:
        return resume["from"]
    if "from_" in resume:
        return resume["from_"]
    raise ValueError("resume requires `from`")


def _resume_kwargs(resume: Mapping[str, Any]) -> dict[str, Any] | None:
    value = resume.get("kwargs")
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError("resume kwargs must be a mapping")
    return value


def _adapter_overrides_from_payload(
    pipeline: Any, namespace: Mapping[str, Any], payload: Any
) -> dict[tuple[str, str], Adapter] | None:
    if payload is None:
        return None
    if not isinstance(payload, Mapping):
        raise TypeError("resume adapter overrides must be a mapping")
    overrides: dict[tuple[str, str], Adapter] = {}
    for raw_key, raw_spec in payload.items():
        alias, port = _adapter_override_target(raw_key, raw_spec)
        overrides[(alias, port)] = _adapter_from_payload(pipeline, namespace, raw_spec)
    return overrides


def _adapter_override_target(raw_key: Any, raw_spec: Any) -> tuple[str, str]:
    if (
        isinstance(raw_spec, Mapping)
        and isinstance(raw_spec.get("alias"), str)
        and isinstance(raw_spec.get("port"), str)
    ):
        return validate_name(raw_spec["alias"]), validate_name(raw_spec["port"])
    if not isinstance(raw_key, str) or "." not in raw_key:
        raise ValueError("resume adapter override key must be `alias.port`")
    alias, port = raw_key.rsplit(".", 1)
    return validate_name(alias), validate_name(port)


def _adapter_from_payload(pipeline: Any, namespace: Mapping[str, Any], raw_spec: Any) -> Adapter:
    if isinstance(raw_spec, str):
        adapter = pipeline.resolve_adapter(key=raw_spec)
        if adapter is None:
            raise ValueError("resume adapter override references unknown adapter key `{}`".format(raw_spec))
        return cast(Adapter, adapter)
    if not isinstance(raw_spec, Mapping):
        raise TypeError("resume adapter override spec must be a mapping or adapter key")
    key = _required_string(raw_spec, "key")
    save = namespace[_required_string(raw_spec, "save")]
    load = namespace[_required_string(raw_spec, "load")]
    _, separator, format_name = key.rpartition("@")
    if not separator or not format_name:
        raise ValueError("resume adapter override key must be `<python_type>@<format>`")
    return Adapter(
        key=key,
        save=save,
        load=load,
        py_type=None,
        format=str(raw_spec.get("format") or format_name),
        distributions=_adapter_distributions(raw_spec.get("distributions")),
    )


def _required_string(spec: Mapping[str, Any], key: str) -> str:
    value = spec.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError("resume adapter override `{}` must be a non-empty string".format(key))
    return value


def _adapter_distributions(value: Any) -> tuple[DDistribution, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise TypeError("resume adapter override distributions must be a list")
    distributions: list[DDistribution] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise TypeError("resume adapter override distribution entries must be mappings")
        distributions.append(
            DDistribution(
                package=_required_string(item, "package"),
                version=_required_string(item, "version"),
            )
        )
    return tuple(distributions)


def load_entrypoint(
    object_yaml: Path,
    entrypoint: str,
    *,
    remote_signatures_path: Path | None = None,
) -> Any:
    """Import a serialized SPL file and return the requested object."""

    target, _ = load_entrypoint_with_namespace(
        object_yaml,
        entrypoint,
        remote_signatures_path=remote_signatures_path,
    )
    return target


def load_entrypoint_with_namespace(
    object_yaml: Path,
    entrypoint: str,
    *,
    remote_signatures_path: Path | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Import a serialized SPL file and return the entrypoint plus namespace."""

    from spl.core.ir.utils import spl_import_from_file

    remote_ports = _read_remote_ports(remote_signatures_path)
    _install_node_remote_hydration(remote_ports)

    namespace: dict[str, Any] = {}
    _seed_node_remote_namespace(namespace)
    spl_import_from_file(object_yaml, globals=namespace)
    try:
        return namespace[entrypoint], namespace
    except KeyError as exc:
        raise KeyError(f"entrypoint is not found in SPL file: {entrypoint}") from exc


def _read_remote_ports(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    payload = read_json(path)
    return {
        str(item["id"]): {
            "inputs": item.get("inputs") or [],
            "outputs": item.get("outputs") or [],
            "remote": item.get("remote") or {},
        }
        for item in payload.get("nodes", [])
        if item.get("kind") == "remote" and item.get("id")
    }


def _seed_node_remote_namespace(namespace: dict[str, Any]) -> None:
    """Provide names missing from the current framework's DNodeRemote unparse."""

    from uuid import UUID

    from spl.core.entities.node_remote import NodeRemote

    namespace.update(
        {
            "NodeRemote": NodeRemote,
            "UUID": UUID,
        }
    )


def _install_node_remote_hydration(remote_ports: dict[str, dict[str, Any]]) -> None:
    """Patch NodeRemote construction in this worker process with sidecar ports."""

    if not remote_ports:
        return

    from spl.core.entities.node import InputPort, OutputPort
    from spl.core.entities.node_remote import NodeRemote

    node_remote_type = cast(Any, NodeRemote)
    if getattr(node_remote_type, "__spl_daemon_hydrated__", False):
        node_remote_type.__spl_daemon_remote_ports__ = remote_ports
        return

    original_init = node_remote_type.__init__

    def hydrated_init(
        self: Any,
        url: str | None = None,
        name: str | None = None,
        version: str = "latest",
        inputs: list[Any] | None = None,
        outputs: list[Any] | None = None,
        uuid: Any = None,
        **kwargs: Any,
    ) -> None:
        current_ports = getattr(node_remote_type, "__spl_daemon_remote_ports__", {})
        metadata = current_ports.get(str(uuid))
        if metadata is not None and not inputs and not outputs:
            inputs = [
                InputPort(
                    name=str(item.get("name") or "default"),
                    typ_=item.get("type"),
                    default=item.get("default"),
                )
                for item in metadata.get("inputs") or []
            ]
            outputs = [
                OutputPort(
                    name=str(item.get("name") or "default"),
                    typ_=item.get("type"),
                )
                for item in metadata.get("outputs") or []
            ]
        original_init(self, url, name, version, inputs, outputs, uuid=uuid, **kwargs)
        if metadata is not None:
            remote = metadata.get("remote") or {}
            for attr in ("owner_id", "library", "target_machine"):
                if remote.get(attr) is not None:
                    object.__setattr__(self, attr, remote[attr])

    node_remote_type.__spl_daemon_hydrated__ = True
    node_remote_type.__spl_daemon_remote_ports__ = remote_ports
    node_remote_type.__init__ = hydrated_init


def execute(
    *,
    object_yaml: Path,
    entrypoint: str,
    input_path: Path,
    result_path: Path,
    artifacts_dir: Path,
    env_spec_path: Path | None = None,
    remote_signatures_path: Path | None = None,
    daemon_url: str = "http://127.0.0.1:8765",
) -> dict[str, Any]:
    """Load, call, and persist one function or pipeline result."""

    payload = read_json(input_path)
    args = payload.get("args", [])
    kwargs = payload.get("kwargs", {})
    output = payload.get("output")
    runtime_config = payload.get("runtime_config")
    runtimes = payload.get("runtimes")

    runtime_env_spec: list[dict[str, Any]] = []
    if env_spec_path is not None:
        runtime_env_spec = read_json(env_spec_path)
        validate_environment(runtime_env_spec)

    target, namespace = load_entrypoint_with_namespace(
        object_yaml,
        entrypoint,
        remote_signatures_path=remote_signatures_path,
    )

    from spl.core.entities.pipeline import Pipeline

    if isinstance(target, Pipeline):
        result_without_artifacts, artifacts, manifest = run_pipeline(
            target,
            kwargs,
            output,
            daemon_url=daemon_url,
            timeout_seconds=payload.get("timeout_seconds"),
            artifacts_dir=artifacts_dir,
            namespace=namespace,
            runtime_config=runtime_config if isinstance(runtime_config, dict) else None,
            runtime_env_spec=runtime_env_spec,
            node_runtime_environments=(
                payload.get("node_runtime_environments")
                if isinstance(payload.get("node_runtime_environments"), Mapping)
                else None
            ),
            runtimes=runtimes if isinstance(runtimes, dict) else None,
            resume=payload.get("resume") if isinstance(payload.get("resume"), Mapping) else None,
            include_manifest=True,
        )
    elif callable(target):
        raw_result = target(*args, **kwargs)
        result_without_artifacts, artifacts = collect_artifacts(raw_result, artifacts_dir)
        manifest = None
    else:
        raise TypeError(f"entrypoint is not callable or Pipeline: {entrypoint}")

    result_payload = {
        "result": to_jsonable(result_without_artifacts),
        "artifacts": artifacts,
    }
    if manifest is not None:
        result_payload["manifest"] = manifest
    write_json(result_path, result_payload)
    return result_payload


def build_parser() -> argparse.ArgumentParser:
    """Create the worker argument parser."""

    parser = argparse.ArgumentParser(description="Execute one SPL object")
    parser.add_argument("--object-yaml", required=True, type=Path)
    parser.add_argument("--entrypoint", required=True)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--artifacts-dir", required=True, type=Path)
    parser.add_argument("--env-spec", default=None, type=Path)
    parser.add_argument("--remote-signatures", default=None, type=Path)
    parser.add_argument("--daemon-url", default="http://127.0.0.1:8765")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the worker from the command line."""

    args = build_parser().parse_args(argv)
    execute(
        object_yaml=args.object_yaml,
        entrypoint=args.entrypoint,
        input_path=args.input,
        result_path=args.result,
        artifacts_dir=args.artifacts_dir,
        env_spec_path=args.env_spec,
        remote_signatures_path=args.remote_signatures,
        daemon_url=args.daemon_url,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
