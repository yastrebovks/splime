"""Per-node runtime resolution and execution backends."""

from __future__ import annotations

import ast
import hashlib
import inspect
import json
import os
import shutil
import subprocess
import sys
import textwrap
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from spl.core.entities.function import DFunction
from spl.core.entities.node import InputPort, Node, OutputPort
from spl.core.entities.node_function import NodeFunction
from spl.core.entities.pipeline import Pipeline
from spl.core.fingerprint import canonical_json_bytes
from spl.core.ir.parse import _branch, ir_parse

NATIVE_NODE_RUNTIME = "native"
VENV_SUBPROCESS_NODE_RUNTIME = "venv-subprocess"
DOCKER_NODE_RUNTIME = "docker"
RUNTIME_TAG_NAME = "runtime"
NODE_TIMEOUT_SECONDS_KEY = "node_timeout_seconds"
_NODE_TIMEOUT_SECONDS_ERROR = 'runtime_config["{}"] must be a positive number or None'.format(NODE_TIMEOUT_SECONDS_KEY)

RunRuntimeOverrides = Mapping[str, str]
NormalizedRunRuntimeOverrides = dict[Node, str]


class NodeRuntimeResolutionSource(StrEnum):
    """Source level that selected a node runtime."""

    DEFAULT = "default"
    OBJECT_RUNTIME_CONFIG = "object-runtime-config"
    NODE_TAG = "node-tag"
    RUN_OVERRIDE = "run-override"


@dataclass(frozen=True)
class NodeRuntimeResolution:
    """Resolved runtime name and the source level that selected it."""

    name: str
    source: NodeRuntimeResolutionSource


@dataclass(frozen=True)
class PreparedNodeEnvironment:
    """Resolved execution environment for one node runtime."""

    name: str
    python_path: Path | None
    metadata: dict[str, Any]


class NodeEnvironmentProvider(Protocol):
    """Prepare or locate the Python environment used by a node runtime."""

    def prepare(
        self,
        spec: Mapping[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> PreparedNodeEnvironment:
        """Return the environment for ``spec``."""
        ...


@dataclass(frozen=True)
class NodeRuntimeContext:
    """Execution inputs shared by node runtime backends."""

    node: Node
    node_label: str
    inputs: dict[InputPort, Any]
    output_port: OutputPort
    callback: Callable[[Node, dict[InputPort, Any]], dict[str, Any]]
    work_dir: Path
    environment_provider: NodeEnvironmentProvider
    runtime_config: Mapping[str, Any]
    environment_spec: Sequence[Mapping[str, Any]]


class NodeRuntimeBackend(Protocol):
    """Minimal execution contract for interchangeable node runtimes."""

    name: str

    def prepare(self, context: NodeRuntimeContext) -> PreparedNodeEnvironment:
        """Prepare the runtime environment."""
        ...

    def execute(self, context: NodeRuntimeContext, environment: PreparedNodeEnvironment) -> dict[str, Any]:
        """Execute one node and return output values by port name."""
        ...


class CurrentPythonEnvironmentProvider:
    """Environment provider that resolves to the current interpreter."""

    def prepare(
        self,
        spec: Mapping[str, Any],
        *,
        wait: bool = True,
        retry_failed: bool = False,
    ) -> PreparedNodeEnvironment:
        del wait, retry_failed
        spec_hash = hashlib.sha256(canonical_json_bytes(spec)).hexdigest()
        return PreparedNodeEnvironment(
            name="current-python",
            python_path=Path(sys.executable),
            metadata={
                "spec_hash": spec_hash,
                "spec": dict(spec),
            },
        )


class NativeNodeRuntime:
    """Execute a node in the current conductor process."""

    name = NATIVE_NODE_RUNTIME

    def prepare(self, context: NodeRuntimeContext) -> PreparedNodeEnvironment:
        del context
        return PreparedNodeEnvironment(name="current-process", python_path=Path(sys.executable), metadata={})

    def execute(self, context: NodeRuntimeContext, environment: PreparedNodeEnvironment) -> dict[str, Any]:
        del environment
        return context.callback(context.node, context.inputs)


class VenvSubprocessNodeRuntime:
    """Execute one JSON-native function node through the SPL-free subprocess runner.

    ``runtime_config["node_timeout_seconds"]`` optionally bounds subprocess
    execution time; native nodes still run without a per-node timeout.
    """

    name = VENV_SUBPROCESS_NODE_RUNTIME

    def prepare(self, context: NodeRuntimeContext) -> PreparedNodeEnvironment:
        return context.environment_provider.prepare(_runtime_spec(self.name, context))

    def execute(self, context: NodeRuntimeContext, environment: PreparedNodeEnvironment) -> dict[str, Any]:
        if not isinstance(context.node, NodeFunction):
            raise RuntimeError("venv-subprocess runtime supports function nodes only")
        if environment.python_path is None:
            raise RuntimeError("venv-subprocess runtime requires a Python executable")

        input_json = _subprocess_input_json(context)
        module_text = _generated_node_module_text(context.node, context.node_label)
        work_dir = context.work_dir
        artifacts_dir = work_dir / "artifacts"
        input_path = work_dir / "input.json"
        result_path = work_dir / "result.json"
        env_spec_path = work_dir / "env-spec.json"
        stdout_path = work_dir / "stdout.txt"
        stderr_path = work_dir / "stderr.txt"
        runner_path = work_dir / "spl_free_runner.py"
        module_path = work_dir / "node_module.py"
        module_name = "_spl_node_{}".format(str(context.node.uuid).replace("-", "_"))

        work_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        artifacts_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        _write_generated_node_module(module_path, module_text)
        _copy_spl_free_runner(runner_path)
        input_path.write_text(input_json, encoding="utf-8")
        _write_json(env_spec_path, list(context.environment_spec))

        command = [
            str(environment.python_path),
            str(runner_path),
            "--module",
            str(module_path),
            "--module-name",
            module_name,
            "--entrypoint",
            context.node.func.__name__,
            "--input",
            str(input_path),
            "--result",
            str(result_path),
            "--artifacts-dir",
            str(artifacts_dir),
            "--env-spec",
            str(env_spec_path),
        ]
        timeout_seconds = node_timeout_seconds(context.runtime_config)
        try:
            completed = subprocess.run(
                command,
                cwd=work_dir,
                env=_subprocess_env_without_project_pythonpath(),
                text=True,
                capture_output=True,
                check=False,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            stdout_path.write_text(_subprocess_output_text(exc.stdout), encoding="utf-8")
            stderr_path.write_text(_subprocess_output_text(exc.stderr), encoding="utf-8")
            raise RuntimeError(
                "node runtime `{}` timed out after {}s for `{}`".format(
                    self.name, _format_timeout_seconds(timeout_seconds), context.node.func.__name__
                )
            ) from exc
        stdout_path.write_text(completed.stdout, encoding="utf-8")
        stderr_path.write_text(completed.stderr, encoding="utf-8")
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip() or "no subprocess output"
            raise RuntimeError(
                "node runtime `{}` failed for `{}` with return code {}: {}".format(
                    self.name, context.node.func.__name__, completed.returncode, detail
                )
            )
        if not result_path.exists():
            raise RuntimeError("node runtime `{}` finished without writing result.json".format(self.name))
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        return {context.output_port.name: payload.get("result")}


class DockerNodeRuntime:
    """Reserved per-node Docker runtime placeholder."""

    name = DOCKER_NODE_RUNTIME

    def prepare(self, context: NodeRuntimeContext) -> PreparedNodeEnvironment:
        return context.environment_provider.prepare(_runtime_spec(self.name, context))

    def execute(self, context: NodeRuntimeContext, environment: PreparedNodeEnvironment) -> dict[str, Any]:
        del context, environment
        raise NotImplementedError("per-node docker runtime is reserved for a 0.4.x follow-up")


NodeRuntimeFactory = Callable[[], NodeRuntimeBackend]


NODE_RUNTIME_BACKENDS: dict[str, NodeRuntimeFactory] = {
    NATIVE_NODE_RUNTIME: NativeNodeRuntime,
    VENV_SUBPROCESS_NODE_RUNTIME: VenvSubprocessNodeRuntime,
    DOCKER_NODE_RUNTIME: DockerNodeRuntime,
}


class NodeRuntimeRegistry:
    """Create node runtime backends from explicit runtime names."""

    def __init__(self, backends: Mapping[str, NodeRuntimeFactory] | None = None):
        self.backends = dict(backends or NODE_RUNTIME_BACKENDS)

    def backend_for(self, runtime_name: str) -> NodeRuntimeBackend:
        try:
            factory = self.backends[runtime_name]
        except KeyError as exc:
            raise ValueError("unsupported node runtime: {}".format(runtime_name)) from exc
        return factory()


def resolve_node_runtime(
    pipeline: Pipeline,
    node: Node,
    *,
    runtime_config: Mapping[str, Any] | None = None,
    run_override: str | None = None,
    default_runtime: str = NATIVE_NODE_RUNTIME,
) -> NodeRuntimeResolution:
    """Resolve the runtime for ``node`` and report the selected source level."""

    if node not in pipeline.nodes:
        raise ValueError("node runtime resolution received a node outside the pipeline")
    resolution = NodeRuntimeResolution(_validate_runtime_name(default_runtime), NodeRuntimeResolutionSource.DEFAULT)
    config = runtime_config or {}
    configured = config.get("node_runtime")
    if configured is not None:
        resolution = NodeRuntimeResolution(
            _validate_runtime_name(configured),
            NodeRuntimeResolutionSource.OBJECT_RUNTIME_CONFIG,
        )
    node_tags = pipeline.tags.get(str(node.uuid), {})
    tagged = node_tags.get(RUNTIME_TAG_NAME)
    if tagged is not None:
        resolution = NodeRuntimeResolution(_validate_runtime_name(tagged), NodeRuntimeResolutionSource.NODE_TAG)
    if run_override is not None:
        resolution = NodeRuntimeResolution(
            _validate_runtime_name(run_override), NodeRuntimeResolutionSource.RUN_OVERRIDE
        )
    return resolution


def validate_run_runtime_overrides(
    pipeline: Pipeline,
    runtimes: RunRuntimeOverrides | None,
) -> NormalizedRunRuntimeOverrides:
    """Validate run-level runtime overrides and normalize aliases to nodes."""

    if runtimes is None:
        return {}
    if not isinstance(runtimes, Mapping):
        raise TypeError("run runtime overrides must be a mapping")
    normalized: NormalizedRunRuntimeOverrides = {}
    for alias, runtime_name in runtimes.items():
        if not isinstance(alias, str) or not alias:
            raise ValueError("run runtime override alias must be a non-empty string")
        if alias not in pipeline.aliases:
            raise ValueError("run runtime override references unknown alias `{}`".format(alias))
        normalized[pipeline.aliases[alias]] = _validate_runtime_name(runtime_name)
    return normalized


def validate_node_runtime_config(runtime_config: Mapping[str, Any] | None) -> dict[str, Any]:
    """Validate run-level node runtime configuration and return a copy."""

    config = dict(runtime_config or {})
    node_timeout_seconds(config)
    return config


def node_timeout_seconds(runtime_config: Mapping[str, Any]) -> float | None:
    """Return the configured non-native node runtime timeout in seconds."""

    value = runtime_config.get(NODE_TIMEOUT_SECONDS_KEY)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(_NODE_TIMEOUT_SECONDS_ERROR)
    timeout_seconds = float(value)
    if timeout_seconds <= 0:
        raise ValueError(_NODE_TIMEOUT_SECONDS_ERROR)
    return timeout_seconds


def runtime_manifest_record(
    resolution: NodeRuntimeResolution,
    environment: PreparedNodeEnvironment,
) -> dict[str, Any]:
    """Return a manifest record for a resolved node runtime."""

    resolved: dict[str, Any] = {}
    if environment.python_path is not None:
        resolved["python"] = str(environment.python_path)
    metadata = dict(environment.metadata)
    return {
        "name": resolution.name,
        "source": str(resolution.source),
        "config_hash": metadata.get("spec_hash"),
        "resolved": resolved,
    }


def _validate_runtime_name(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("node runtime name must be a non-empty string")
    return value


def _runtime_spec(runtime_name: str, context: NodeRuntimeContext) -> dict[str, Any]:
    return {
        "node_runtime": runtime_name,
        "runtime_config": dict(context.runtime_config),
        "distributions": list(context.environment_spec),
    }


def _subprocess_input_json(context: NodeRuntimeContext) -> str:
    kwargs = []
    for port, value in sorted(context.inputs.items(), key=lambda item: item[0].name):
        try:
            encoded_value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "node `{}` port `{}` cannot run with venv-subprocess in 0.4.0 because input value type `{}` "
                "is not JSON-serializable: {}; execute this node with the native runtime, insert a converter "
                "node (cookbook: Converter Nodes For Adapter Tags), or wait for artifact-file input transport "
                "(0.4.x).".format(context.node_label, port.name, _type_name(value), exc)
            ) from exc
        kwargs.append("{}:{}".format(json.dumps(port.name, ensure_ascii=False), encoded_value))
    return '{{"args":[],"kwargs":{{{}}}}}'.format(",".join(kwargs))


def _type_name(value: Any) -> str:
    typ = type(value)
    return "{}.{}".format(typ.__module__, typ.__qualname__)


def _subprocess_output_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _format_timeout_seconds(timeout_seconds: float | None) -> str:
    if timeout_seconds is None:
        return "unknown"
    return "{:g}".format(timeout_seconds)


class _SourceRecoveryError(Exception):
    pass


def _generated_node_module_text(node: NodeFunction, node_label: str) -> str:
    from spl.daemon.spl_free_generator import filter_spl_runtime_scaffolding, unsupported_stage1_reason

    module, failures = _recover_node_module(node)
    if module is None:
        details = "; ".join(failures) if failures else "no source candidate was available"
        raise RuntimeError(
            "node `{}` function `{}` source is not recoverable; venv-subprocess requires a source-visible "
            "or IR-parsable function node ({})".format(node_label, node.func.__name__, details)
        )

    reason = unsupported_stage1_reason(module)
    if reason is not None:
        raise RuntimeError(
            "venv-subprocess runtime cannot execute node `{}` function `{}` via spl-free runner: {}".format(
                node_label, node.func.__name__, reason
            )
        )
    filtered = filter_spl_runtime_scaffolding(module)
    return ast.unparse(filtered) + "\n"


def _recover_node_module(node: NodeFunction) -> tuple[ast.Module | None, list[str]]:
    failures: list[str] = []
    try:
        return _module_from_source_text(inspect.getsource(node.func), node.func.__name__), failures
    except (OSError, SyntaxError, TypeError, _SourceRecoveryError) as exc:
        failures.append("inspect.getsource: {}".format(exc))

    try:
        return _module_from_dfunction(_dfunction_from_ir(node), node.func.__name__), failures
    except (KeyError, SyntaxError, TypeError, ValueError, _SourceRecoveryError) as exc:
        failures.append("ir_parse: {}".format(exc))
    return None, failures


def _module_from_source_text(source: str, func_name: str) -> ast.Module:
    module = ast.parse(textwrap.dedent(source))
    _validate_top_level_function(module, func_name)
    return module


def _dfunction_from_ir(node: NodeFunction) -> DFunction:
    parsed = ir_parse(node.func)
    if isinstance(parsed, _branch):
        root = parsed.mk_root()
        if isinstance(root, DFunction):
            return root
    if isinstance(parsed, DFunction):
        return parsed
    raise ValueError("IR did not produce DFunction for `{}`".format(node.func.__name__))


def _module_from_dfunction(dfunction: DFunction, func_name: str) -> ast.Module:
    outputs = dfunction.outputs or []
    returns = None
    if outputs and outputs[0].typ_ is not None:
        returns = ast.parse(outputs[0].typ_, mode="eval").body
    function_def = ast.FunctionDef(
        name=dfunction.name,
        args=ast.arguments(
            posonlyargs=[],
            args=[ast.arg(arg=port.name, annotation=_annotation_expr(port.typ_)) for port in dfunction.inputs],
            vararg=None,
            kwonlyargs=[],
            kw_defaults=[],
            kwarg=None,
            defaults=[
                ast.parse(port.default, mode="eval").body for port in dfunction.inputs if port.default is not None
            ],
        ),
        body=ast.parse(textwrap.dedent(dfunction.body)).body,
        decorator_list=[],
        returns=returns,
    )
    module = ast.fix_missing_locations(ast.Module(body=[function_def], type_ignores=[]))
    _validate_top_level_function(module, func_name)
    return module


def _annotation_expr(value: str | None) -> ast.expr | None:
    if value is None:
        return None
    return ast.parse(value, mode="eval").body


def _validate_top_level_function(module: ast.Module, func_name: str) -> None:
    if any(isinstance(stmt, ast.FunctionDef) and stmt.name == func_name for stmt in module.body):
        return
    raise _SourceRecoveryError("top-level function `{}` is not present".format(func_name))


def _write_generated_node_module(module_path: Path, module_text: str) -> None:
    module_path.write_text(module_text, encoding="utf-8")


def _copy_spl_free_runner(runner_path: Path) -> None:
    import spl.daemon.spl_free_runner as spl_free_runner

    source = Path(str(spl_free_runner.__file__))
    shutil.copy2(source, runner_path)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _subprocess_env_without_project_pythonpath() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    return env
