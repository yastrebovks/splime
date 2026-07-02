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
        os.path.normcase(
            os.path.abspath(os.path.join(sys.prefix, "Lib", "site-packages"))
        ),
        os.path.normcase(
            os.path.abspath(os.path.join(sys.base_prefix, "lib", version))
        ),
        os.path.normcase(
            os.path.abspath(os.path.join(sys.prefix, "lib", version, "site-packages"))
        ),
        os.path.normcase(
            os.path.abspath(os.path.join(sys.prefix, "lib64", version, "site-packages"))
        ),
    ]
    protected_indexes = [
        index
        for index, item in enumerate(sys.path)
        if os.path.normcase(os.path.abspath(item)) in protected_candidates
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

    sys.path[:] = [
        item for item in sys.path if item not in early_external_site_packages
    ]
    last_protected_index = max(
        index
        for index, item in enumerate(sys.path)
        if os.path.normcase(os.path.abspath(item)) in protected_candidates
    )
    for item in reversed(early_external_site_packages):
        sys.path.insert(last_protected_index + 1, item)


_prefer_runtime_env_over_pythonpath_site_packages()

import argparse
import importlib.metadata
import json
import shutil
from collections.abc import Mapping, Sequence
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from spl.daemon.store import validate_name

ARTIFACTS_KEY = "__spl_artifacts__"
RESULT_KEY = "__spl_result__"


def read_json(path: Path) -> Any:
    """Read a UTF-8 JSON file."""

    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    """Write a UTF-8 JSON file with stable formatting."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


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
        payload = {
            "node": {
                "uuid": str(node.uuid),
                "url": node.url,
                "name": node.name,
                "version": node.version,
            },
            "kwargs": kwargs,
            "timeout_seconds": self.timeout_seconds,
        }
        target_machine = getattr(node, "target_machine", None)
        if target_machine is not None:
            payload["node"]["target_machine"] = target_machine
        owner_id = getattr(node, "owner_id", None)
        if owner_id is not None:
            payload["node"]["owner_id"] = owner_id
        library = getattr(node, "library", None)
        if library is not None:
            payload["node"]["library"] = library
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
            with urlopen(request) as response:  # noqa: S310 - local daemon URL.
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8")
            try:
                message = json.loads(raw).get("error", raw)
            except json.JSONDecodeError:
                message = raw
            raise RuntimeError(f"remote node call failed: {message}") from exc
        except URLError as exc:
            raise RuntimeError(
                f"local daemon is not reachable for remote node call: {exc.reason}"
            ) from exc
        return json.loads(raw).get("value")


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
            mismatches.append(
                f"{package}=={expected} is required, actual version is {actual}"
            )

    if mismatches:
        raise RuntimeError(
            "worker environment does not match SPL metadata: "
            + "; ".join(mismatches)
        )


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
    raise TypeError(
        "result is not JSON serializable; return JSON-like data or declare artifacts"
    )


def safe_artifact_name(name: str) -> str:
    """Validate an artifact name before writing under the artifacts directory."""

    return validate_name(name)


def copy_artifact(source: Path, target: Path) -> None:
    """Copy one artifact file or directory into the run artifact directory."""

    if not source.exists():
        raise ValueError(f"artifact source is not found: {source}")
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


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
        result = {
            key: item
            for key, item in value.items()
            if key not in {ARTIFACTS_KEY, RESULT_KEY}
        }

    if isinstance(artifact_spec, Mapping):
        items = artifact_spec.items()
    elif isinstance(artifact_spec, Sequence) and not isinstance(artifact_spec, str):
        items = ((Path(str(path)).name, path) for path in artifact_spec)
    else:
        raise TypeError("__spl_artifacts__ must be a mapping or a list of paths")

    copied: dict[str, str] = {}
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    for name, source in items:
        artifact_name = safe_artifact_name(str(name))
        source_path = Path(str(source)).expanduser().absolute()
        target_path = artifacts_dir / artifact_name
        copy_artifact(source_path, target_path)
        copied[artifact_name] = str(target_path)

    return result, copied


def run_pipeline(
    pipeline: Any,
    kwargs: dict[str, Any],
    output: str | None,
    *,
    daemon_url: str,
    timeout_seconds: float | None,
) -> Any:
    """Run a ``spl.core`` pipeline without changing the existing core files.

    The current core exposes ``Deployment`` and ``Run`` but does not provide a
    direct "give me the final output" helper.  This adapter supplies the minimal
    selection rules the daemon needs:

    * if ``output`` is given, it must be a pipeline alias;
    * otherwise aliases are returned as a dictionary;
    * a single-node pipeline can be returned without an alias;
    * multi-node pipelines should define aliases for daemon use.
    """

    from spl.core.common import Deployment

    client = RemoteNodeClient(daemon_url, timeout_seconds=timeout_seconds)
    try:
        deployment = Deployment(client, pipeline)
    except TypeError:
        # Older framework builds did not require a client for local-only
        # pipelines.  Keep the worker tolerant while NodeRemote is still moving.
        deployment = Deployment(pipeline)
    run = deployment.run(**kwargs)

    if output is not None:
        return run[pipeline.get_node_by_alias(output)]

    if pipeline.aliases:
        return {
            alias: run[node]
            for alias, node in sorted(pipeline.aliases.items(), key=lambda item: item[0])
        }

    if len(pipeline.nodes) == 1:
        [node] = list(pipeline.nodes)
        return run[node]

    raise ValueError(
        "pipeline has multiple nodes and no aliases; pass output or register aliases"
    )


def load_entrypoint(
    object_yaml: Path,
    entrypoint: str,
    *,
    remote_signatures_path: Path | None = None,
) -> Any:
    """Import a serialized SPL file and return the requested object."""

    from spl.core.ir.utils import spl_import_from_file

    remote_ports = _read_remote_ports(remote_signatures_path)
    _install_node_remote_hydration(remote_ports)

    namespace: dict[str, Any] = {}
    _seed_node_remote_namespace(namespace)
    spl_import_from_file(object_yaml, globals=namespace)
    try:
        return namespace[entrypoint]
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

    if getattr(NodeRemote, "__spl_daemon_hydrated__", False):
        NodeRemote.__spl_daemon_remote_ports__ = remote_ports
        return

    original_init = NodeRemote.__init__

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
        current_ports = getattr(NodeRemote, "__spl_daemon_remote_ports__", {})
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

    NodeRemote.__spl_daemon_hydrated__ = True
    NodeRemote.__spl_daemon_remote_ports__ = remote_ports
    NodeRemote.__init__ = hydrated_init


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

    if env_spec_path is not None:
        validate_environment(read_json(env_spec_path))

    target = load_entrypoint(
        object_yaml,
        entrypoint,
        remote_signatures_path=remote_signatures_path,
    )

    from spl.core.entities.pipeline import Pipeline

    if isinstance(target, Pipeline):
        raw_result = run_pipeline(
            target,
            kwargs,
            output,
            daemon_url=daemon_url,
            timeout_seconds=payload.get("timeout_seconds"),
        )
    elif callable(target):
        raw_result = target(*args, **kwargs)
    else:
        raise TypeError(f"entrypoint is not callable or Pipeline: {entrypoint}")

    result_without_artifacts, artifacts = collect_artifacts(raw_result, artifacts_dir)
    result_payload = {
        "result": to_jsonable(result_without_artifacts),
        "artifacts": artifacts,
    }
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
