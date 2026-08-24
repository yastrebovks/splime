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
import ast
import __future__
import hashlib
import importlib.metadata
import json
import re
import shutil
import stat
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from typing import Any, Literal, cast, overload
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request

from spl._http import urlopen_verified
from spl.adapters import get_builtin_adapter
from spl.core import json_contract as m_json_contract
from spl.core import manifest as m_manifest
from spl.core import node_runtime as m_node_runtime
from spl.core.entities.adapter import Adapter
from spl.core.entities.distribution import DDistribution
from spl.core.library_adapters import (
    LIBRARY_ADAPTER_CONTENT_HASH_DOMAIN,
    LIBRARY_ADAPTER_SIGNATURE_HASH_DOMAIN,
    domain_hash,
    library_adapter_execution_payload,
    library_adapter_signature_payload,
    normalize_dependencies,
    normalize_runtime_library_adapter_refs,
)
from spl.core.runtime_port_adapters import (
    MAX_CUSTOM_BUNDLE_BYTES,
    MAX_RUNTIME_INPUT_BYTES,
    RuntimePortAdapterContractError,
    normalize_wire_document,
    runtime_port_adapter_fingerprint,
    validate_custom_adapter_source,
    validate_custom_bundle_dependencies,
)


PUBLIC_EMBEDDED_HOST_CONTRACT = "spl.public_embedded_host.v1"

from spl.daemon.callback_capability import CALLBACK_CAPABILITY_ENV
from spl.daemon.name_validation import validate_name
from spl.daemon.worker_runtime_marker import (
    WORKER_MANIFEST_HANDOFF_FILE,
    WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE,
    WORKER_RUNTIME_ADAPTER_FAILURE_FILE,
    WORKER_RUNTIME_CUSTOM_ADAPTER_USED_FILE,
)

ARTIFACTS_KEY = "__spl_artifacts__"
ARTIFACT_REF_KEY = "__spl_artifact_ref__"
RESULT_KEY = "__spl_result__"
_ARTIFACT_NAME_TOKEN_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")
_RUNTIME_ADAPTER_REF_KEY = "__spl_runtime_adapter_artifact__"
DEFAULT_REMOTE_NODE_HTTP_TIMEOUT_SECONDS: float | None = None
_RUNTIME_CUSTOM_ADAPTER_USED_PATH: Path | None = None
_RUNTIME_LIBRARY_ADAPTERS_DIRECTORY = "runtime-library-adapters"


class _WorkerRuntimeAdapterFailure(RuntimeError):
    """Internal-only failure carrying closed adapter-stage evidence."""

    def __init__(self, message: str, evidence: Mapping[str, Any]):
        super().__init__(message)
        self.evidence = dict(evidence)


def _runtime_adapter_operation_failure(
    stage: Literal["worker_load", "worker_save"],
    binding: Mapping[str, Any],
) -> _WorkerRuntimeAdapterFailure:
    direction = "input" if stage == "worker_load" else "output"
    adapter_id = str(binding["adapter"]["id"])
    port = str(binding["port"])
    return _WorkerRuntimeAdapterFailure(
        f"{stage}: adapter {adapter_id!r} failed for port {port!r}",
        {
            "schema_version": 1,
            "kind": "operation",
            "stage": stage,
            "direction": direction,
            "port": port,
            "adapter_id": adapter_id,
        },
    )


def _runtime_adapter_stage_failure(
    stage: Literal["worker_load", "worker_save"],
    error: Exception,
) -> _WorkerRuntimeAdapterFailure:
    prefix = f"{stage}: "
    raw_message = str(error)
    message = (
        raw_message if raw_message.startswith(prefix) and "\n" not in raw_message else f"{stage}: adapter stage failed"
    )
    return _WorkerRuntimeAdapterFailure(
        message,
        {
            "schema_version": 1,
            "kind": "stage",
            "stage": stage,
        },
    )


class _DiscardAdapterOutput:
    """Non-buffering sink for untrusted custom adapter console output."""

    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        return None


_DISCARD_ADAPTER_OUTPUT = _DiscardAdapterOutput()


def read_json(path: Path) -> Any:
    """Read a UTF-8 JSON file."""

    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    """Write a UTF-8 JSON file with stable formatting."""

    payload = m_json_contract.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, separators=None)
    _ensure_private_dir(path.parent)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(payload)
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
        callback_capability: str | None = None,
        timeout_seconds: float | None = None,
    ):
        self.daemon_url = daemon_url.rstrip("/")
        self.callback_capability = callback_capability
        self.timeout_seconds = timeout_seconds

    def run_node(self, node: Any, kwargs: dict[str, Any]) -> Any:
        if not self.callback_capability:
            raise RuntimeError("remote node callback capability is missing; start this pipeline through the SPL daemon")
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
            data=m_json_contract.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=False,
                separators=None,
            ).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.callback_capability}",
                "Content-Type": "application/json; charset=utf-8",
            },
            method="POST",
        )
        try:
            # /remote-nodes/run is a blocking call: the daemon polls the
            # server-side run until the node reaches a terminal state.  The
            # shared helper keeps its bounded connect timeout while allowing
            # the response read to remain unbounded when no run timeout exists.
            timeout = (
                self.timeout_seconds if self.timeout_seconds is not None else DEFAULT_REMOTE_NODE_HTTP_TIMEOUT_SECONDS
            )
            daemon_target = urlparse(self.daemon_url)
            with urlopen_verified(
                request,
                timeout=timeout,
                allow_docker_callback_http=(
                    daemon_target.scheme.casefold() == "http" and daemon_target.hostname == "host.docker.internal"
                ),
            ) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            if 300 <= exc.code < 400:
                message = str(exc.reason)
            else:
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
        return self.prepare_for_node(
            spec,
            node_label=None,
            wait=wait,
            retry_failed=retry_failed,
        )

    def prepare_for_node(
        self,
        spec: Mapping[str, Any],
        *,
        node_label: str | None,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> m_node_runtime.PreparedNodeEnvironment:
        """Resolve an optional daemon-prepared environment for one node alias."""

        runtime_name = spec.get("node_runtime")
        if runtime_name == m_node_runtime.VENV_SUBPROCESS_NODE_RUNTIME:
            raw_environments = self.node_runtime_environments.get(m_node_runtime.VENV_SUBPROCESS_NODE_RUNTIME)
            environments = raw_environments if isinstance(raw_environments, Mapping) else {}
            raw_environment = environments.get(node_label) or environments.get("default")
            environment = raw_environment if isinstance(raw_environment, Mapping) else {}
            python_path = environment.get("python_path")
            if isinstance(python_path, str) and python_path:
                return m_node_runtime.PreparedNodeEnvironment(
                    name="prepared-venv",
                    python_path=Path(python_path),
                    metadata={
                        "spec_hash": environment.get("lock_hash"),
                        "source": "worker-input",
                    },
                )
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


def to_jsonable(value: Any, *, path: str = "$") -> Any:
    """Convert common Python containers into JSON-compatible values.

    The function is intentionally strict for unknown objects.  A daemon that
    silently converts everything with ``repr`` would be hard to use correctly:
    the caller might think it received a reusable result while actually getting
    a display string.
    """

    if type(value) in m_json_contract.JSON_SCALARS:
        m_json_contract.validate_json_value(value, path=path)
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                m_json_contract.validate_json_value({key: None}, path=path)
                raise AssertionError("JSON key validation unexpectedly accepted a non-string key")
            result[key] = to_jsonable(item, path=_json_child_path(path, key))
        return result
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [to_jsonable(item, path="{}[{}]".format(path, index)) for index, item in enumerate(value)]
    if isinstance(value, set):
        return [
            to_jsonable(item, path="{}[{}]".format(path, index)) for index, item in enumerate(sorted(value, key=repr))
        ]
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


def _json_child_path(path: str, key: str) -> str:
    return "{}[{}]".format(
        path,
        m_json_contract.dumps(key, ensure_ascii=False, sort_keys=False),
    )


def _json_path(parts: Sequence[str]) -> str:
    path = "$"
    for part in parts:
        path = _json_child_path(path, part)
    return path


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


def _read_verified_runtime_file(
    path: Path,
    *,
    expected_size: int,
    expected_sha256: str,
) -> bytes:
    """Read one no-follow regular file and bind its admitted identity."""

    body = _read_bounded_regular_file(path)
    if len(body) != expected_size or hashlib.sha256(body).hexdigest() != expected_sha256:
        raise RuntimeError("worker_load: runtime adapter input failed size/checksum verification")
    return body


def _read_bounded_regular_file(path: Path) -> bytes:
    """Return bytes from one identity-pinned, bounded, no-follow file."""

    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or path.is_symlink():
        raise RuntimeError("worker_load: runtime adapter input is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise RuntimeError("worker_load: runtime adapter input changed before loading")
        chunks: list[bytes] = []
        remaining = MAX_RUNTIME_INPUT_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        body = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        current = path.lstat()
    except OSError:
        raise RuntimeError("worker_load: runtime adapter file changed while it was read") from None
    if (
        not stat.S_ISREG(current.st_mode)
        or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
        or (current.st_dev, current.st_ino) != (after.st_dev, after.st_ino)
        or after.st_size != len(body)
        or current.st_size != len(body)
    ):
        raise RuntimeError("worker_load: runtime adapter file changed while it was read")
    if len(body) > MAX_RUNTIME_INPUT_BYTES:
        raise RuntimeError("worker_load: runtime adapter file exceeds the size limit")
    return body


def _load_runtime_custom_namespace(
    document: Mapping[str, Any],
    *,
    input_path: Path,
) -> dict[str, Any]:
    """Verify and define custom adapter functions only in this worker."""

    bundle = document.get("custom_bundle")
    if not isinstance(bundle, Mapping):
        return {}
    bundle_path = input_path.parent / "runtime-inputs" / str(bundle["staged_name"])
    try:
        body = _read_verified_runtime_file(
            bundle_path,
            expected_size=int(bundle["size"]),
            expected_sha256=str(bundle["sha256"]),
        )
    except OSError:
        raise RuntimeError("worker_load: custom adapter bundle is unavailable") from None
    try:
        validate_custom_bundle_dependencies(
            body,
            document,
            allow_content=False,
            verify_installed_environment=True,
        )
    except RuntimePortAdapterContractError:
        raise RuntimeError("worker_load: custom adapter dependency verification failed") from None
    try:
        source = body.decode("utf-8")
        tree = ast.parse(source, filename="<runtime-custom-adapters>", mode="exec")
    except (UnicodeDecodeError, SyntaxError):
        raise RuntimeError("worker_load: custom adapter bundle is not valid UTF-8 Python source") from None
    definitions = []
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef) or node.decorator_list:
            raise RuntimeError("worker_load: custom adapter bundle may contain only plain function definitions")
        definitions.append(node.name)
    if sorted(definitions) != sorted(bundle["functions"]) or len(definitions) != len(set(definitions)):
        raise RuntimeError("worker_load: custom adapter bundle symbols do not match its descriptor")
    namespace: dict[str, Any] = {
        "__builtins__": __builtins__,
        "__name__": "_spl_runtime_custom_adapters",
    }
    try:
        code = compile(
            tree,
            "<runtime-custom-adapters>",
            "exec",
            flags=__future__.annotations.compiler_flag,
            dont_inherit=True,
        )
        with redirect_stdout(_DISCARD_ADAPTER_OUTPUT), redirect_stderr(_DISCARD_ADAPTER_OUTPUT):
            exec(code, namespace, namespace)  # noqa: S102 - worker is the sole authorized execution boundary.
    except BaseException:
        raise RuntimeError("worker_load: custom adapter bundle could not be defined safely") from None
    for name in definitions:
        if not callable(namespace.get(name)):
            raise RuntimeError("worker_load: custom adapter bundle did not define every declared function")
    return namespace


def _runtime_adapter_functions(
    binding: Mapping[str, Any],
    custom_namespace: Mapping[str, Any],
) -> tuple[Any, Any]:
    descriptor = binding["adapter"]
    if descriptor["kind"] == "builtin":
        adapter = get_builtin_adapter(str(descriptor["id"]))
        return adapter.save, adapter.load
    save = custom_namespace.get(str(descriptor["save_symbol"]))
    load = custom_namespace.get(str(descriptor["load_symbol"]))
    if descriptor["kind"] == "library":
        if save is not None and not callable(save):
            raise RuntimeError("worker_load: trusted Library Adapter save symbol is unavailable")
        if load is not None and not callable(load):
            raise RuntimeError("worker_load: trusted Library Adapter load symbol is unavailable")
        return save, load
    if not callable(save) or not callable(load):
        raise RuntimeError("worker_load: trusted custom adapter symbols are unavailable")
    return save, load


def _write_runtime_input_snapshot(directory: Path, name: str, body: bytes) -> Path:
    """Materialize verified bytes under a worker-owned no-follow path."""

    target = directory / name
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(target, flags, 0o400)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    return target


def _invoke_runtime_adapter(binding: Mapping[str, Any], function: Any, *args: Any) -> Any:
    """Call custom code with non-buffering console redaction inside the worker."""

    if binding["adapter"]["kind"] in {"custom", "library"}:
        marker_path = _RUNTIME_CUSTOM_ADAPTER_USED_PATH
        if marker_path is None:
            raise RuntimeError("custom adapter execution marker is unavailable")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(marker_path, flags, 0o400)
        except FileExistsError:
            descriptor = None
        except OSError:
            raise RuntimeError("custom adapter execution marker could not be retained") from None
        if descriptor is not None:
            try:
                os.write(descriptor, b"used\n")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        try:
            with redirect_stdout(_DISCARD_ADAPTER_OUTPUT), redirect_stderr(_DISCARD_ADAPTER_OUTPUT):
                return function(*args)
        except BaseException:
            raise RuntimeError("custom adapter invocation failed") from None
    return function(*args)


def _canonical_distribution_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def _load_runtime_library_adapters(
    payload: Mapping[str, Any],
    *,
    input_path: Path,
    runtime_document: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Verify staged immutable source, then compile it only in this worker."""

    raw = payload.get("runtime_library_adapters")
    if raw is None:
        return dict(runtime_document), {}
    if not isinstance(raw, Mapping) or set(raw) != {"schema_version", "bindings"}:
        raise RuntimeError("worker_load: runtime Library Adapter document is malformed")
    raw_bindings = raw.get("bindings")
    if raw.get("schema_version") != 1 or not isinstance(raw_bindings, list) or len(raw_bindings) > 1_024:
        raise RuntimeError("worker_load: runtime Library Adapter document is malformed")
    ref_keys = {
        "direction",
        "port",
        "owner",
        "library",
        "name",
        "version",
        "adapter_id",
        "adapter_version_id",
        "content_hash",
        "signature_hash",
    }
    execution_keys = ref_keys | {
        "semantic_type",
        "semantic_category",
        "format_tag",
        "media_type",
        "preferred_extension",
        "dependencies",
        "policy",
        "symbol",
        "symbols",
        "source_name",
        "source_size",
        "source_sha256",
        "argument",
        "input_name",
        "result_path",
    }
    if any(not isinstance(item, Mapping) or set(item) != execution_keys for item in raw_bindings):
        raise RuntimeError("worker_load: runtime Library Adapter binding is malformed")
    try:
        normalized_refs = normalize_runtime_library_adapter_refs(
            {
                "schema_version": 1,
                "bindings": [{key: item[key] for key in ref_keys} for item in raw_bindings],
            }
        )
    except ValueError:
        raise RuntimeError("worker_load: runtime Library Adapter reference is malformed") from None
    ref_by_identity = {(item["direction"], item["port"]): item for item in normalized_refs["bindings"]}
    binding_by_identity = {(item["direction"], item["port"]): item for item in runtime_document.get("bindings", [])}
    if not ref_by_identity or not set(ref_by_identity) <= set(binding_by_identity):
        raise RuntimeError("worker_load: runtime Library Adapter port is unavailable")

    installed_owners = importlib.metadata.packages_distributions()
    source_groups: dict[str, list[Mapping[str, Any]]] = {}
    dependencies_by_source: dict[str, list[dict[str, Any]]] = {}
    total_source_bytes = 0
    for raw_binding in raw_bindings:
        identity = (raw_binding["direction"], raw_binding["port"])
        transport_binding = binding_by_identity[identity]
        if (
            transport_binding.get("transport") != "artifact"
            or raw_binding.get("argument") != transport_binding.get("argument")
            or raw_binding.get("input_name") != transport_binding.get("input_name")
            or raw_binding.get("result_path") != transport_binding.get("result_path")
        ):
            raise RuntimeError("worker_load: runtime Library Adapter transport identity changed")
        source_name = raw_binding.get("source_name")
        source_size = raw_binding.get("source_size")
        source_sha256 = raw_binding.get("source_sha256")
        symbol = raw_binding.get("symbol")
        if (
            not isinstance(source_name, str)
            or validate_name(source_name) != source_name
            or type(source_size) is not int
            or not 1 <= source_size <= MAX_CUSTOM_BUNDLE_BYTES
            or not isinstance(source_sha256, str)
            or not re.fullmatch(r"[0-9a-f]{64}", source_sha256)
            or not isinstance(symbol, str)
            or not symbol.isidentifier()
        ):
            raise RuntimeError("worker_load: runtime Library Adapter source identity is malformed")
        try:
            dependencies = normalize_dependencies(raw_binding.get("dependencies"))
        except ValueError:
            raise RuntimeError("worker_load: runtime Library Adapter dependencies are malformed") from None
        previous = dependencies_by_source.setdefault(source_name, dependencies)
        if previous != dependencies:
            raise RuntimeError("worker_load: runtime Library Adapter dependency evidence conflicts")
        source_groups.setdefault(source_name, []).append(raw_binding)

    functions: dict[str, Any] = {}
    for source_name, bindings in source_groups.items():
        first = bindings[0]
        expected_size = int(first["source_size"])
        expected_sha256 = str(first["source_sha256"])
        if any(
            item["source_size"] != expected_size
            or item["source_sha256"] != expected_sha256
            or item["adapter_version_id"] != first["adapter_version_id"]
            for item in bindings
        ):
            raise RuntimeError("worker_load: runtime Library Adapter source evidence conflicts")
        total_source_bytes += expected_size
        if total_source_bytes > MAX_CUSTOM_BUNDLE_BYTES:
            raise RuntimeError("worker_load: runtime Library Adapter sources exceed the Run bound")
        source_path = input_path.parent / _RUNTIME_LIBRARY_ADAPTERS_DIRECTORY / source_name
        try:
            body = _read_verified_runtime_file(
                source_path,
                expected_size=expected_size,
                expected_sha256=expected_sha256,
            )
            source = body.decode("utf-8")
            tree = ast.parse(source, filename="<runtime-library-adapter>", mode="exec")
        except (OSError, UnicodeDecodeError, SyntaxError):
            raise RuntimeError("worker_load: runtime Library Adapter source failed verification") from None
        definitions = {
            node.name: node for node in tree.body if isinstance(node, ast.FunctionDef) and not node.decorator_list
        }
        expected_roles: dict[str, str] = {}
        for item in bindings:
            symbols = item.get("symbols")
            if not isinstance(symbols, Mapping) or set(symbols) != {"save", "load"}:
                raise RuntimeError("worker_load: runtime Library Adapter symbols are malformed")
            for role in ("save", "load"):
                symbol = symbols[role]
                if symbol is None:
                    continue
                if not isinstance(symbol, str) or not symbol.isidentifier():
                    raise RuntimeError("worker_load: runtime Library Adapter symbols are malformed")
                previous_role = expected_roles.setdefault(symbol, role)
                if previous_role != role:
                    raise RuntimeError("worker_load: runtime Library Adapter symbol roles conflict")
        if len(definitions) != len(tree.body) or set(definitions) != set(expected_roles):
            raise RuntimeError("worker_load: runtime Library Adapter functions do not match admission")
        dependencies = dependencies_by_source[source_name]
        installed_packages: dict[str, tuple[str, str]] = {}
        for dependency in dependencies:
            package = dependency["package"]
            expected_version = dependency["version"]
            try:
                actual_version = importlib.metadata.version(package)
            except importlib.metadata.PackageNotFoundError:
                raise RuntimeError("worker_load: runtime Library Adapter dependency is unavailable") from None
            if actual_version != expected_version:
                raise RuntimeError("worker_load: runtime Library Adapter dependency version changed")
            installed_packages[_canonical_distribution_name(package)] = (package, expected_version)
            for module in dependency["modules"]:
                owners = {_canonical_distribution_name(owner) for owner in installed_owners.get(module, [])}
                if owners != {_canonical_distribution_name(package)}:
                    raise RuntimeError("worker_load: runtime Library Adapter module ownership is unproven")
        validated_by_role: dict[str, Mapping[str, Any]] = {}
        for symbol, role in expected_roles.items():
            canonical_source = ast.unparse(definitions[symbol]).strip() + "\n"
            try:
                validated = validate_custom_adapter_source(
                    canonical_source,
                    role=role,
                    distributions=dependencies,
                )
            except RuntimePortAdapterContractError:
                raise RuntimeError("worker_load: runtime Library Adapter callable failed revalidation") from None
            if validated["symbol"] != symbol:
                raise RuntimeError("worker_load: runtime Library Adapter callable identity changed")
            validated_by_role[role] = validated
        policy = first.get("policy")
        if not isinstance(policy, Mapping) or set(policy) != {
            "local_custom_code",
            "remote_custom_code",
        }:
            raise RuntimeError("worker_load: runtime Library Adapter policy evidence is malformed")
        canonical = {
            "schema_version": 1,
            "semantic_type": first["semantic_type"],
            "semantic_category": first["semantic_category"],
            "save_source": (None if "save" not in validated_by_role else validated_by_role["save"]["source"]),
            "load_source": (None if "load" not in validated_by_role else validated_by_role["load"]["source"]),
            "dependencies": dependencies,
            "format_tag": first["format_tag"],
            "media_type": first["media_type"],
            "preferred_extension": first["preferred_extension"],
            "policy": dict(policy),
        }
        computed_content_hash = domain_hash(
            LIBRARY_ADAPTER_CONTENT_HASH_DOMAIN,
            library_adapter_execution_payload(canonical),
        )
        computed_signature_hash = domain_hash(
            LIBRARY_ADAPTER_SIGNATURE_HASH_DOMAIN,
            library_adapter_signature_payload(
                canonical,
                save=validated_by_role.get("save"),
                load=validated_by_role.get("load"),
            ),
        )
        if any(
            item["content_hash"] != computed_content_hash
            or item["signature_hash"] != computed_signature_hash
            or item["policy"] != dict(policy)
            for item in bindings
        ):
            raise RuntimeError("worker_load: runtime Library Adapter immutable hash evidence changed")
        namespace: dict[str, Any] = {
            "__builtins__": __builtins__,
            "__name__": "_spl_runtime_library_adapter",
        }
        try:
            code = compile(
                tree,
                "<runtime-library-adapter>",
                "exec",
                flags=__future__.annotations.compiler_flag,
                dont_inherit=True,
            )
            with redirect_stdout(_DISCARD_ADAPTER_OUTPUT), redirect_stderr(_DISCARD_ADAPTER_OUTPUT):
                exec(code, namespace, namespace)  # noqa: S102 - isolated worker-only boundary.
        except BaseException:
            raise RuntimeError("worker_load: runtime Library Adapter could not be defined safely") from None
        for symbol in expected_roles:
            function = namespace.get(symbol)
            if not callable(function):
                raise RuntimeError("worker_load: runtime Library Adapter callable is unavailable")
            functions[f"{source_name}\0{symbol}"] = function

    overlaid = {**runtime_document, "bindings": [dict(item) for item in runtime_document["bindings"]]}
    for raw_binding in raw_bindings:
        identity = (raw_binding["direction"], raw_binding["port"])
        index = next(
            index for index, item in enumerate(overlaid["bindings"]) if (item["direction"], item["port"]) == identity
        )
        prior = overlaid["bindings"][index]
        callable_key = f"{raw_binding['source_name']}\0{raw_binding['symbol']}"
        overlaid["bindings"][index] = {
            **prior,
            "transport": "artifact",
            "adapter": {
                "kind": "library",
                "id": f"library:{raw_binding['content_hash']}",
                "key": f"{raw_binding['semantic_type']}@{raw_binding['format_tag'] or ('exact-' + raw_binding['content_hash'])}",
                "format_tag": raw_binding["format_tag"],
                "accepted_tags": ([] if raw_binding["format_tag"] is None else [raw_binding["format_tag"]]),
                "distributions": [
                    {"package": item["package"], "version": item["version"]} for item in raw_binding["dependencies"]
                ],
                "save_symbol": callable_key if raw_binding["direction"] == "output" else None,
                "load_symbol": callable_key if raw_binding["direction"] == "input" else None,
                "bundle_sha256": raw_binding["content_hash"],
                "presentation": {
                    "media_type": raw_binding["media_type"],
                    "preferred_extension": raw_binding["preferred_extension"],
                },
            },
        }
    return overlaid, functions


def _apply_runtime_adapter_inputs_unchecked(
    payload: Mapping[str, Any],
    *,
    input_path: Path,
    args: list[Any],
    kwargs: dict[str, Any],
) -> tuple[list[Any], dict[str, Any], dict[str, Any] | None, dict[str, Any]]:
    """Verify and decode staged built-in inputs only inside this worker."""

    raw_document = payload.get("runtime_port_adapters")
    if raw_document is None:
        return args, kwargs, None, {}
    document = normalize_wire_document(raw_document, allow_content=False)
    custom_namespace = _load_runtime_custom_namespace(document, input_path=input_path)
    document, library_namespace = _load_runtime_library_adapters(
        payload,
        input_path=input_path,
        runtime_document=document,
    )
    custom_namespace.update(library_namespace)
    inputs = {item["name"]: item for item in document["inputs"]}
    artifact_bindings = [
        binding
        for binding in document["bindings"]
        if binding["direction"] == "input" and binding["transport"] == "artifact"
    ]
    snapshot_dir = input_path.parent / "runtime-loaded-inputs"
    if artifact_bindings:
        snapshot_dir.mkdir(mode=0o700, exist_ok=False)
    loaded_values: dict[tuple[Any, Any, Any], Any] = {}
    for binding in document["bindings"]:
        if binding["direction"] != "input" or binding["transport"] != "artifact":
            continue
        item = inputs[binding["input_name"]]
        staged_path = input_path.parent / "runtime-inputs" / item["staged_name"]
        try:
            verified_body = _read_verified_runtime_file(
                staged_path,
                expected_size=item["size"],
                expected_sha256=item["sha256"],
            )
        except OSError:
            raise RuntimeError("worker_load: runtime adapter input is unavailable") from None
        try:
            staged_path.chmod(0o400)
        except OSError:
            pass
        snapshot_path = _write_runtime_input_snapshot(snapshot_dir, item["name"], verified_body)
        location = binding["argument"]
        location_key = (location["kind"], location["name"], location["index"])
        try:
            if location_key in loaded_values:
                value = loaded_values[location_key]
            else:
                _, load = _runtime_adapter_functions(binding, custom_namespace)
                value = _invoke_runtime_adapter(binding, load, str(snapshot_path))
                loaded_values[location_key] = value
            _read_verified_runtime_file(
                snapshot_path,
                expected_size=item["size"],
                expected_sha256=item["sha256"],
            )
        except Exception:
            raise _runtime_adapter_operation_failure("worker_load", binding) from None
        if location["kind"] == "positional":
            index = int(location["index"])
            if index >= len(args):
                raise RuntimeError("worker_load: positional runtime adapter target is missing")
            args[index] = value
        else:
            kwargs[str(location["name"])] = value
    return args, kwargs, document, custom_namespace


def apply_runtime_adapter_inputs(
    payload: Mapping[str, Any],
    *,
    input_path: Path,
    args: list[Any],
    kwargs: dict[str, Any],
) -> tuple[list[Any], dict[str, Any], dict[str, Any] | None, dict[str, Any]]:
    """Verify and decode staged inputs with worker-owned failure evidence."""

    try:
        return _apply_runtime_adapter_inputs_unchecked(
            payload,
            input_path=input_path,
            args=args,
            kwargs=kwargs,
        )
    except _WorkerRuntimeAdapterFailure:
        raise
    except Exception as exc:
        raise _runtime_adapter_stage_failure("worker_load", exc) from None


def _runtime_output_name(binding: Mapping[str, Any], used: set[str]) -> str:
    token = _artifact_name_token(str(binding["port"]))
    extension = binding["adapter"]["presentation"].get("preferred_extension") or ""
    base = safe_artifact_name(f"runtime-{token}{extension}")
    candidate = base
    index = 2
    while candidate in used:
        candidate = safe_artifact_name(_with_numeric_suffix(base, index))
        index += 1
    used.add(candidate)
    return candidate


def _materialize_runtime_output_unchecked(
    value: Any,
    binding: Mapping[str, Any],
    *,
    artifacts_dir: Path,
    used_names: set[str],
    custom_namespace: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, str], dict[str, Any]]:
    adapter_id = str(binding["adapter"]["id"])
    save, _ = _runtime_adapter_functions(binding, custom_namespace or {})
    name = _runtime_output_name(binding, used_names)
    target = artifacts_dir / name
    _ensure_private_dir(artifacts_dir)
    with tempfile.TemporaryDirectory(prefix=".spl-runtime-save-", dir=artifacts_dir) as raw_stage_dir:
        stage_dir = Path(raw_stage_dir)
        stage_target = stage_dir / "adapter-output"
        try:
            _invoke_runtime_adapter(binding, save, str(stage_target), value)
        except Exception:
            raise _runtime_adapter_operation_failure("worker_save", binding) from None
        try:
            identity = stage_target.lstat()
            entries = list(stage_dir.iterdir())
        except OSError:
            raise RuntimeError("worker_save: adapter did not produce a regular output file") from None
        if (
            entries != [stage_target]
            or not stat.S_ISREG(identity.st_mode)
            or identity.st_size > MAX_RUNTIME_INPUT_BYTES
        ):
            raise RuntimeError("worker_save: adapter output must be one bounded regular file")
        try:
            body = _read_bounded_regular_file(stage_target)
        except (OSError, RuntimeError):
            raise RuntimeError("worker_save: adapter output could not be verified") from None
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(target, flags, 0o600)
            try:
                with os.fdopen(descriptor, "wb", closefd=False) as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
            finally:
                os.close(descriptor)
            canonical_body = _read_bounded_regular_file(target)
        except (OSError, RuntimeError):
            target.unlink(missing_ok=True)
            raise RuntimeError("worker_save: canonical adapter output could not be retained") from None
        if canonical_body != body:
            target.unlink(missing_ok=True)
            raise RuntimeError("worker_save: canonical adapter output changed before retention")
    _chmod_owner_file(target)
    digest = hashlib.sha256(body).hexdigest()
    record = {
        "port": binding["port"],
        "name": name,
        "size": len(body),
        "sha256": digest,
        "format_tag": binding["adapter"]["format_tag"],
        "adapter_id": adapter_id,
        "media_type": binding["adapter"]["presentation"].get("media_type"),
        "result_path": binding["result_path"],
    }
    reference = {
        _RUNTIME_ADAPTER_REF_KEY: True,
        "port": binding["port"],
        "name": name,
        "sha256": digest,
    }
    return reference, {name: str(target)}, record


def _materialize_runtime_output(
    value: Any,
    binding: Mapping[str, Any],
    *,
    artifacts_dir: Path,
    used_names: set[str],
    custom_namespace: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, str], dict[str, Any]]:
    """Materialize one output and retain only closed worker-owned failure facts."""

    try:
        return _materialize_runtime_output_unchecked(
            value,
            binding,
            artifacts_dir=artifacts_dir,
            used_names=used_names,
            custom_namespace=custom_namespace,
        )
    except _WorkerRuntimeAdapterFailure:
        raise
    except Exception as exc:
        raise _runtime_adapter_stage_failure("worker_save", exc) from None


def _materialize_runtime_outputs_unchecked(
    value: Any,
    document: Mapping[str, Any] | None,
    *,
    artifacts_dir: Path,
    custom_namespace: Mapping[str, Any] | None = None,
) -> tuple[Any, dict[str, str], list[dict[str, Any]]]:
    """Apply artifact output bindings to a callable result."""

    if document is None:
        return value, {}, []
    result = value
    artifacts: dict[str, str] = {}
    records: list[dict[str, Any]] = []
    used: set[str] = set()
    for binding in document["bindings"]:
        if binding["direction"] != "output" or binding["transport"] != "artifact":
            continue
        path = list(binding["result_path"])
        if not path:
            selected = result
        else:
            cursor = result
            for part in path:
                if not isinstance(cursor, Mapping) or part not in cursor:
                    raise RuntimeError("worker_save: runtime output result path is missing")
                cursor = cursor[part]
            selected = cursor
        reference, copied, record = _materialize_runtime_output(
            selected,
            binding,
            artifacts_dir=artifacts_dir,
            used_names=used,
            custom_namespace=custom_namespace,
        )
        if not path:
            result = reference
        else:
            result = _replace_mapping_path(result, path, reference)
        artifacts.update(copied)
        records.append(record)
    return result, artifacts, records


def materialize_runtime_outputs(
    value: Any,
    document: Mapping[str, Any] | None,
    *,
    artifacts_dir: Path,
    custom_namespace: Mapping[str, Any] | None = None,
) -> tuple[Any, dict[str, str], list[dict[str, Any]]]:
    """Apply output bindings with worker-owned adapter-stage evidence."""

    try:
        return _materialize_runtime_outputs_unchecked(
            value,
            document,
            artifacts_dir=artifacts_dir,
            custom_namespace=custom_namespace,
        )
    except _WorkerRuntimeAdapterFailure:
        raise
    except Exception as exc:
        raise _runtime_adapter_stage_failure("worker_save", exc) from None


def _replace_mapping_path(value: Any, path: list[str], replacement: Any) -> Any:
    if not isinstance(value, Mapping):
        raise RuntimeError("worker_save: runtime output path does not address a mapping")
    result = dict(value)
    cursor = result
    for part in path[:-1]:
        child = cursor.get(part)
        if not isinstance(child, Mapping):
            raise RuntimeError("worker_save: runtime output path is missing")
        copied = dict(child)
        cursor[part] = copied
        cursor = copied
    cursor[path[-1]] = replacement
    return result


class PipelineResultNormalizer:
    """Convert final pipeline values into the daemon's JSON/artifact protocol."""

    def __init__(
        self,
        pipeline: Any,
        artifacts_dir: Path,
        *,
        runtime_adapter_document: Mapping[str, Any] | None = None,
        runtime_custom_namespace: Mapping[str, Any] | None = None,
        runtime_output_records: list[dict[str, Any]] | None = None,
        runtime_result_prefix: Sequence[str] = (),
    ):
        self.pipeline = pipeline
        self.artifacts_dir = artifacts_dir
        self.artifacts: dict[str, str] = {}
        self._used_artifact_names: set[str] = set()
        self._runtime_custom_namespace = dict(runtime_custom_namespace or {})
        self._runtime_output_records = runtime_output_records
        self._runtime_result_prefix = tuple(runtime_result_prefix)
        self._runtime_output_bindings = {
            tuple(binding["result_path"]): binding
            for binding in (runtime_adapter_document or {}).get("bindings", [])
            if binding["direction"] == "output" and binding["transport"] == "artifact"
        }

    def normalize(self, value: Any, path: tuple[str, ...] = ("result",)) -> Any:
        result_path = tuple(path[1:])
        if (
            self._runtime_result_prefix
            and result_path[: len(self._runtime_result_prefix)] == self._runtime_result_prefix
        ):
            result_path = result_path[len(self._runtime_result_prefix) :]
        runtime_binding = self._runtime_output_bindings.get(result_path)
        if runtime_binding is not None:
            reference, copied, record = _materialize_runtime_output(
                value,
                runtime_binding,
                artifacts_dir=self.artifacts_dir,
                used_names=self._used_artifact_names,
                custom_namespace=self._runtime_custom_namespace,
            )
            self.artifacts.update(copied)
            if self._runtime_output_records is not None:
                self._runtime_output_records.append(record)
            return reference
        if type(value) in m_json_contract.JSON_SCALARS:
            m_json_contract.validate_json_value(value, path=_json_path(path))
            return value
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, Mapping):
            if ARTIFACTS_KEY in value:
                explicit_result = self._result_from_explicit_artifact_mapping(value)
                self._copy_declared_artifacts(value[ARTIFACTS_KEY])
                return self.normalize(explicit_result, path)
            normalized_mapping: dict[str, Any] = {}
            for key, item in value.items():
                if type(key) is not str:
                    m_json_contract.validate_json_value({key: None}, path=_json_path(path))
                    raise AssertionError("JSON key validation unexpectedly accepted a non-string key")
                normalized_mapping[key] = self.normalize(item, (*path, key))
            return normalized_mapping
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
    callback_capability: str | None = None,
    timeout_seconds: float | None,
    artifacts_dir: Path,
    namespace: Mapping[str, Any] | None = None,
    runtime_config: dict[str, Any] | None = None,
    runtime_env_spec: list[dict[str, Any]] | None = None,
    node_runtime_environments: Mapping[str, Any] | None = None,
    runtimes: str | dict[str, str] | None = None,
    resume: Mapping[str, Any] | None = None,
    keep: m_manifest.KeepPolicy = True,
    runtime_adapter_document: Mapping[str, Any] | None = None,
    runtime_custom_namespace: Mapping[str, Any] | None = None,
    runtime_output_records: list[dict[str, Any]] | None = None,
    runtime_adapter_fingerprint_sha256: str | None = None,
    include_manifest: Literal[False] = False,
) -> tuple[Any, dict[str, str]]: ...


@overload
def run_pipeline(
    pipeline: Any,
    kwargs: dict[str, Any],
    output: str | None,
    *,
    daemon_url: str,
    callback_capability: str | None = None,
    timeout_seconds: float | None,
    artifacts_dir: Path,
    namespace: Mapping[str, Any] | None = None,
    runtime_config: dict[str, Any] | None = None,
    runtime_env_spec: list[dict[str, Any]] | None = None,
    node_runtime_environments: Mapping[str, Any] | None = None,
    runtimes: str | dict[str, str] | None = None,
    resume: Mapping[str, Any] | None = None,
    keep: m_manifest.KeepPolicy = True,
    runtime_adapter_document: Mapping[str, Any] | None = None,
    runtime_custom_namespace: Mapping[str, Any] | None = None,
    runtime_output_records: list[dict[str, Any]] | None = None,
    runtime_adapter_fingerprint_sha256: str | None = None,
    include_manifest: Literal[True],
) -> tuple[Any, dict[str, str], dict[str, Any] | None]: ...


def run_pipeline(
    pipeline: Any,
    kwargs: dict[str, Any],
    output: str | None,
    *,
    daemon_url: str,
    callback_capability: str | None = None,
    timeout_seconds: float | None,
    artifacts_dir: Path,
    namespace: Mapping[str, Any] | None = None,
    runtime_config: dict[str, Any] | None = None,
    runtime_env_spec: list[dict[str, Any]] | None = None,
    node_runtime_environments: Mapping[str, Any] | None = None,
    runtimes: str | dict[str, str] | None = None,
    resume: Mapping[str, Any] | None = None,
    keep: m_manifest.KeepPolicy = True,
    runtime_adapter_document: Mapping[str, Any] | None = None,
    runtime_custom_namespace: Mapping[str, Any] | None = None,
    runtime_output_records: list[dict[str, Any]] | None = None,
    runtime_adapter_fingerprint_sha256: str | None = None,
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

    client = RemoteNodeClient(
        daemon_url,
        callback_capability=callback_capability,
        timeout_seconds=timeout_seconds,
    )
    node_environment_provider = WorkerNodeEnvironmentProvider(node_runtime_environments)
    try:
        deployment = Deployment(
            client,
            pipeline,
            runtime_config=runtime_config,
            node_environment_provider=node_environment_provider,
            runtime_env_spec=runtime_env_spec,
            _runtime_adapter_fingerprint_sha256=runtime_adapter_fingerprint_sha256,
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
            _runtime_adapter_fingerprint_sha256=runtime_adapter_fingerprint_sha256,
        )
    normalizer = PipelineResultNormalizer(
        pipeline,
        artifacts_dir,
        runtime_adapter_document=runtime_adapter_document,
        runtime_custom_namespace=runtime_custom_namespace,
        runtime_output_records=runtime_output_records,
        runtime_result_prefix=(() if output is None else (output,)),
    )
    previous_runs_home = os.environ.get("SPL_RUNS_HOME")
    os.environ["SPL_RUNS_HOME"] = str(artifacts_dir.parent / "pipeline-state")
    try:
        try:
            if resume is None:
                run = deployment.run(runtimes=runtimes, keep=keep, **kwargs)
            else:
                run = deployment.resume(
                    _resume_parent_run_dir(resume, artifacts_dir=artifacts_dir),
                    from_=_resume_from_selection(resume),
                    adapters=_adapter_overrides_from_payload(pipeline, namespace or {}, resume.get("adapters")),
                    runtimes=runtimes,
                    kwargs=_resume_kwargs(resume),
                    keep=keep,
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

                # ``output`` selects the value returned by the daemon; it does
                # not turn a pipeline run into a partial graph evaluation.
                # Run.close() deliberately requires every pipeline node to
                # have a terminal manifest record, so execute any disconnected
                # or downstream nodes that were not needed to obtain the
                # selected value before leaving the context manager.
                for node in sorted(pipeline.nodes, key=lambda item: str(item.uuid)):
                    run[node]
        except BaseException:
            snapshot = run.manifest_snapshot if "run" in locals() else None
            if snapshot is not None and run.manifest_path is None:
                write_json(artifacts_dir.parent / WORKER_MANIFEST_HANDOFF_FILE, snapshot)
            raise
    finally:
        if previous_runs_home is None:
            os.environ.pop("SPL_RUNS_HOME", None)
        else:
            os.environ["SPL_RUNS_HOME"] = previous_runs_home

    run_manifest = run.manifest_snapshot
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


def load_compiled_entrypoint_with_namespace(
    object_python: Path,
    entrypoint: str,
) -> tuple[Any, dict[str, Any]]:
    """Execute a producer-compiled, signed SPL member without a YAML parser."""

    namespace: dict[str, Any] = {
        "__file__": str(object_python),
        "__name__": "__spl_public_object__",
        "__package__": None,
    }
    bundle_root = str(object_python.parent.absolute())
    if bundle_root not in sys.path:
        # The exact embedded authority is inserted at index zero by the public
        # worker.  Signed bundle modules follow it and cannot shadow ``spl``.
        sys.path.insert(1, bundle_root)
    source = object_python.read_text(encoding="utf-8")
    exec(compile(source, str(object_python), mode="exec"), namespace)  # noqa: S102
    try:
        return namespace[entrypoint], namespace
    except KeyError as exc:
        raise KeyError(f"entrypoint is not found in compiled SPL member: {entrypoint}") from exc


def _read_remote_ports(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    payload = read_json(path)
    return {
        str(item.get("node_id") or item["id"]): {
            "inputs": item.get("inputs") or [],
            "outputs": item.get("outputs") or [],
            "remote": item.get("remote") or {},
        }
        for item in payload.get("nodes", [])
        if item.get("kind") == "remote" and (item.get("node_id") or item.get("id"))
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
    object_yaml: Path | None = None,
    object_python: Path | None = None,
    entrypoint: str,
    input_path: Path,
    result_path: Path,
    artifacts_dir: Path,
    env_spec_path: Path | None = None,
    remote_signatures_path: Path | None = None,
    daemon_url: str = "http://127.0.0.1:8765",
) -> dict[str, Any]:
    """Load, call, and persist one function or pipeline result."""

    global _RUNTIME_CUSTOM_ADAPTER_USED_PATH

    callback_capability = os.environ.pop(CALLBACK_CAPABILITY_ENV, None)
    _RUNTIME_CUSTOM_ADAPTER_USED_PATH = input_path.parent / WORKER_RUNTIME_CUSTOM_ADAPTER_USED_FILE
    payload = read_json(input_path)
    args = list(payload.get("args", []))
    kwargs = dict(payload.get("kwargs", {}))
    output = payload.get("output")
    runtime_config = payload.get("runtime_config")
    runtimes = payload.get("runtimes")
    keep = m_manifest.normalize_keep(payload.get("keep", True))

    runtime_env_spec: list[dict[str, Any]] = []
    if env_spec_path is not None:
        runtime_env_spec = read_json(env_spec_path)
        validate_environment(runtime_env_spec)

    args, kwargs, runtime_adapter_document, runtime_custom_namespace = apply_runtime_adapter_inputs(
        payload,
        input_path=input_path,
        args=args,
        kwargs=kwargs,
    )
    raw_runtime_fingerprint_document = payload.get("runtime_port_adapters")
    runtime_adapter_fingerprint_sha256 = (
        None
        if raw_runtime_fingerprint_document is None
        else runtime_port_adapter_fingerprint(
            normalize_wire_document(raw_runtime_fingerprint_document, allow_content=False),
            adapter_policy=payload.get("adapter_policy", {"custom_remote": "deny"}),
        )
    )

    if (object_yaml is None) == (object_python is None):
        raise ValueError("exactly one serialized or compiled SPL object is required")
    if object_python is not None:
        if remote_signatures_path is not None:
            raise ValueError("compiled public execution does not admit remote signatures")
        target, namespace = load_compiled_entrypoint_with_namespace(object_python, entrypoint)
    else:
        assert object_yaml is not None
        target, namespace = load_entrypoint_with_namespace(
            object_yaml,
            entrypoint,
            remote_signatures_path=remote_signatures_path,
        )

    from spl.core.entities.pipeline import Pipeline

    runtime_outputs: list[dict[str, Any]] = []
    if isinstance(target, Pipeline):
        result_without_artifacts, artifacts, manifest = run_pipeline(
            target,
            kwargs,
            output,
            daemon_url=daemon_url,
            callback_capability=callback_capability,
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
            runtimes=runtimes if isinstance(runtimes, (str, dict)) else None,
            resume=payload.get("resume") if isinstance(payload.get("resume"), Mapping) else None,
            keep=keep,
            runtime_adapter_document=runtime_adapter_document,
            runtime_custom_namespace=runtime_custom_namespace,
            runtime_output_records=runtime_outputs,
            runtime_adapter_fingerprint_sha256=runtime_adapter_fingerprint_sha256,
            include_manifest=True,
        )
    elif callable(target):
        raw_result = target(*args, **kwargs)
        adapted_result, runtime_artifacts, runtime_outputs = materialize_runtime_outputs(
            raw_result,
            runtime_adapter_document,
            artifacts_dir=artifacts_dir,
            custom_namespace=runtime_custom_namespace,
        )
        result_without_artifacts, artifacts = collect_artifacts(adapted_result, artifacts_dir)
        artifacts.update(runtime_artifacts)
        manifest = None
    else:
        raise TypeError(f"entrypoint is not callable or Pipeline: {entrypoint}")

    result_payload = {
        "result": to_jsonable(result_without_artifacts),
        "artifacts": artifacts,
    }
    if runtime_outputs:
        result_payload["runtime_port_adapter_outputs"] = runtime_outputs
    if manifest is not None:
        result_payload["manifest"] = manifest
    write_json(result_path, result_payload)
    return result_payload


def build_parser() -> argparse.ArgumentParser:
    """Create the worker argument parser."""

    parser = argparse.ArgumentParser(description="Execute one SPL object")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--object-yaml", type=Path)
    source.add_argument("--object-python", type=Path)
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

    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments == ["--health-check"]:
        print(PUBLIC_EMBEDDED_HOST_CONTRACT)
        return 0
    args = build_parser().parse_args(arguments)
    try:
        execute(
            object_yaml=args.object_yaml,
            object_python=args.object_python,
            entrypoint=args.entrypoint,
            input_path=args.input,
            result_path=args.result,
            artifacts_dir=args.artifacts_dir,
            env_spec_path=args.env_spec,
            remote_signatures_path=args.remote_signatures,
            daemon_url=args.daemon_url,
        )
    except _WorkerRuntimeAdapterFailure as exc:
        failure_path = args.input.parent / WORKER_RUNTIME_ADAPTER_FAILURE_FILE
        try:
            write_json(failure_path, exc.evidence)
        except OSError:
            pass
        stage = exc.evidence.get("stage")
        print(f"{stage}: runtime adapter stage failed", file=sys.stderr, flush=True)
        return WORKER_RUNTIME_ADAPTER_FAILURE_EXIT_CODE
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
