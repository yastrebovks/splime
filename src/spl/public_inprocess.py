"""Current-process host for the authenticated current-process-v1 contract.

Private namespaces are an import mechanism, not a security sandbox. No worker,
installer, daemon, environment mutation, caller-owned logical module mutation
or timeout emulation belongs here.
"""

from __future__ import annotations

import builtins
import importlib.util
import sys
import weakref
from collections.abc import Mapping
from pathlib import Path
from threading import RLock
from types import ModuleType
from typing import Any, cast
from uuid import uuid4

from packaging.version import Version

from spl.daemon_client import ClientError
from spl.execution_results import PipelineResultNormalizer, collect_artifacts
from spl.public_contract import CURRENT_PROCESS_CONTRACT, ContractError
from spl.public_dependencies import inspect_dependencies
from spl.runtime_environment import installed_framework_distribution

PUBLIC_CURRENT_PROCESS_CONTRACT = "spl.public_current_process.v1"


def _error(code: str, message: str, **details: Any) -> ClientError:
    return ClientError(f"{code}: {message}", payload={"code": code, "error": message, **details})


def admit(manifest: Mapping[str, Any], *, args: Any, kwargs: Any, timeout_seconds: float | None) -> None:
    if timeout_seconds is not None:
        raise _error(
            "inprocess_timeout_unsupported",
            "A hard timeout cannot safely terminate caller-process code. "
            "Omit timeout_seconds; no execution was started.",
        )
    if sys.version_info[:2] != (3, 13) or sys.implementation.name != "cpython":
        raise _error("public_python_incompatible", "current-process-v1 supports CPython 3.13 only")
    try:
        installed = installed_framework_distribution()
        if Version(installed.version) < Version(CURRENT_PROCESS_CONTRACT["minimum_framework_version"]):
            raise ValueError("installed Splime version is too old")
    except (ValueError, ModuleNotFoundError) as exc:
        raise _error("public_framework_incompatible", str(exc)) from exc
    if not isinstance(args, list) or not isinstance(kwargs, dict):
        raise _error("public_arguments_invalid", "args must be a list and kwargs a mapping")
    if any(type(name) is not str for name in kwargs):
        raise _error("public_arguments_invalid", "keyword argument names must be strings")
    if manifest.get("kind") == "pipeline":
        if args:
            raise _error("public_arguments_invalid", "Pipeline calls accept keyword arguments only")


def admit_dependencies(manifest: Mapping[str, Any]) -> None:
    report = inspect_dependencies(manifest["dependency_graph"])
    if report.missing:
        rows = [f"  {item.package}=={item.recorded_version} ({', '.join(item.sources)})" for item in report.missing]
        message = (
            "Execution was not started. Missing dependencies:\n"
            + "\n".join(rows)
            + "\n\nInstall the missing packages into the environment used by this kernel/interpreter. "
            "The versions shown were recorded at publication, not simultaneous installation requirements."
        )
        raise _error(
            "public_dependencies_missing",
            message,
            **report.as_dict(),
            missing=[
                {"package": x.package, "version": x.recorded_version, "sources": list(x.sources)}
                for x in report.missing
            ],
        )
    report.warn()


class _BundleNamespace:
    """Per-call module cache for static and lazy bundled imports.

    Python import statements in signed code use this private importer. Ordinary
    third-party imports retain Python's normal importer and caller-owned modules.
    Modules use per-call, unguessable ``sys.modules`` names so Python features
    such as ``dataclasses`` can resolve their defining module.  Caller-owned
    logical module names are never inserted or replaced.
    """

    def __init__(self, members: Mapping[str, bytes], identity: str, source_root: Path):
        self.members = members
        self.identity = identity
        self.source_root = source_root
        self.modules: dict[str, ModuleType] = {}
        self.registrations: dict[str, Any] = {}
        self.released = False
        self.lock = RLock()
        self.owner_attribute = "__spl_module_owner_" + uuid4().hex
        self.builtins = {**vars(builtins), "__import__": self.import_module, "__build_class__": self.build_class}
        # The finalizer owns only weak registry entries, never this namespace,
        # its modules, globals, classes or returned values.
        weakref.finalize(self, self._unregister, self.registrations)

    @staticmethod
    def _unregister(registrations: dict[str, Any]) -> None:
        for name, registered in tuple(registrations.items()):
            if sys.modules.get(name) is registered:
                del sys.modules[name]
        registrations.clear()

    def _register(self, module: ModuleType) -> None:
        registered = cast(ModuleType, weakref.proxy(module)) if self.released else module
        self.registrations[module.__name__] = registered
        sys.modules[module.__name__] = registered

    def build_class(self, *args: Any, **kwargs: Any) -> Any:
        cls = builtins.__build_class__(*args, **kwargs)
        if isinstance(cls, type):
            # An instance always retains its class, even without weakref slots.
            # Also covers classes created later by a returned function.
            type.__setattr__(cls, self.owner_attribute, self)
        return cls

    def import_module(
        self, name: str, globals: Any = None, locals: Any = None, fromlist: Any = (), level: int = 0
    ) -> Any:
        absolute = importlib.util.resolve_name("." * level + name, globals.get("__package__")) if level else name
        if absolute.split(".")[0] != "_spl_components":
            return builtins.__import__(name, globals, locals, fromlist, level)
        with self.lock:
            module = self.load(absolute)
            for item in fromlist or ():
                if item != "*" and item not in vars(module) and self.member(f"{absolute}.{item}") is not None:
                    self.load(f"{absolute}.{item}")
            return module if fromlist else self.load("_spl_components")

    def member(self, name: str) -> str | None:
        base = name.replace(".", "/")
        return next((path for path in (base + "/__init__.py", base + ".py") if path in self.members), None)

    def load(self, name: str) -> ModuleType:
        if name in self.modules:
            return self.modules[name]
        member = self.member(name)
        if member is None:
            raise ModuleNotFoundError(f"authenticated bundle has no module {name!r}")
        parent, _, child = name.rpartition(".")
        parent_module = self.load(parent) if parent else None
        module = ModuleType(f"{self.identity}.{name}")
        namespace = vars(module)
        namespace.update(
            __file__=str(self.source_root / member),
            __package__=name if member.endswith("/__init__.py") else parent,
            __builtins__=self.builtins,
        )
        if member.endswith("/__init__.py"):
            namespace["__path__"] = []
        self.modules[name] = module
        self._register(module)
        try:
            exec(compile(self.members[member], namespace["__file__"], "exec", dont_inherit=True), namespace)
        except BaseException:
            self.modules.pop(name, None)
            registered = self.registrations.pop(module.__name__)
            if sys.modules.get(module.__name__) is registered:
                del sys.modules[module.__name__]
            raise
        if parent_module is not None:
            setattr(parent_module, child, module)
        return module

    def entrypoint(self, name: str) -> Any:
        module = ModuleType(self.identity)
        namespace = vars(module)
        namespace.update(
            __file__=str(self.source_root / "object.py"),
            __package__="",
            __builtins__=self.builtins,
        )
        self.modules[""] = module
        self._register(module)
        try:
            exec(compile(self.members["object.py"], namespace["__file__"], "exec", dont_inherit=True), namespace)
            return namespace[name]
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        self._unregister(self.registrations)
        self.modules.clear()

    def retain_for(self, value: Any) -> None:
        """Release strong registry roots, leaving ordinary Python ownership.

        Functions retain globals/builtins; classes retain this namespace via
        build_class (including slots-only instances). This scan also anchors
        dynamically constructed classes in builtin result containers. It is
        iterative, cycle-safe, unbounded, and never inspects opaque datasets.
        """
        seen: set[int] = set()
        stack = [value]
        while stack:
            item = stack.pop()
            marker = id(item)
            if marker in seen:
                continue
            seen.add(marker)
            cls = item if isinstance(item, type) else type(item)
            item_module = type.__getattribute__(cls, "__module__")
            if type(item_module) is str and (
                item_module == self.identity or item_module.startswith(self.identity + ".")
            ):
                type.__setattr__(cls, self.owner_attribute, self)
                continue
            if type(item) is dict:
                stack.extend(item.keys())
                stack.extend(item.values())
            elif type(item) in {list, tuple, set, frozenset}:
                stack.extend(item)
        # A strong sys.modules -> module -> globals -> class/function cycle
        # would otherwise keep every return value alive indefinitely. Only
        # our private registry uses proxies; user values are never replaced.
        self.released = True
        for module in self.modules.values():
            if sys.modules.get(module.__name__) is self.registrations.get(module.__name__):
                self._register(module)
            else:
                # User code may have changed even this private registration.
                # Do not retain a strong module through finalizer bookkeeping.
                self.registrations.pop(module.__name__, None)


def execute(
    manifest: Mapping[str, Any],
    members: Mapping[str, bytes],
    *,
    identity: str,
    args: list[Any],
    kwargs: dict[str, Any],
    run_dir: Path,
) -> tuple[dict[str, Any], Any]:
    source_root = run_dir / "source"
    for name, body in members.items():
        if name.endswith(".py"):
            path = source_root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
    namespace = _BundleNamespace(members, identity + "_" + uuid4().hex, source_root)
    try:
        target = namespace.entrypoint(manifest["entrypoint"])
        return _execute_target(namespace, target, args=args, kwargs=kwargs, run_dir=run_dir)
    except BaseException:
        namespace.close()
        raise


def _execute_target(
    namespace: _BundleNamespace,
    target: Any,
    *,
    args: list[Any],
    kwargs: dict[str, Any],
    run_dir: Path,
) -> tuple[dict[str, Any], Any]:
    from spl.core._common import Deployment, Run
    from spl.core.entities.pipeline import Pipeline

    files = run_dir / "files"
    files.mkdir(parents=True, exist_ok=True)
    if isinstance(target, Pipeline):
        # Native metadata has already been checked before definitions execute.
        # Run-local storage avoids the worker's process-global SPL_RUNS_HOME.
        deployment = Deployment(target)
        run = Run(deployment._callback, target, keep=True, _input_values=kwargs, _native_values=True)
        run._runs_home = run_dir / "pipeline-state"
        normalizer = PipelineResultNormalizer(target, files, allow_native_values=True)
        with run:
            if target.aliases:
                value = {alias: run[node] for alias, node in sorted(target.aliases.items())}
            elif len(target.nodes) == 1:
                value = run[next(iter(target.nodes))]
            else:
                raise ValueError("pipeline has multiple nodes and no aliases")
            for node in sorted(target.nodes, key=lambda node: str(node.uuid)):
                run[node]
            # Normalize while transient artifact values remain available.
            result = normalizer.normalize(value)
        payload = {
            "result": _native_descriptor(result),
            "artifacts": normalizer.artifacts,
            "manifest": run.manifest_snapshot,
        }
        namespace.retain_for(result)
        return payload, result
    if not callable(target):
        raise ContractError("published entrypoint is not a function or Pipeline")
    value, artifacts = collect_artifacts(target(*args, **kwargs), files)
    namespace.retain_for(value)
    return {"result": _native_descriptor(value), "artifacts": artifacts}, value


def _native_descriptor(value: Any) -> dict[str, Any]:
    typ = type(value)
    descriptor: dict[str, Any] = {
        "kind": "native-in-memory",
        "type": f"{typ.__module__}.{typ.__qualname__}",
        "reconstructable": False,
    }
    try:
        shape = getattr(value, "shape", None)
    except Exception:
        # Result presentation must not make a successful native call fail just
        # because an arbitrary object's descriptive property raises.
        shape = None
    if type(shape) is tuple and all(type(item) is int for item in shape):
        descriptor["shape"] = list(shape)
    elif type(value) in {dict, list, tuple, set, frozenset}:
        descriptor["length"] = len(value)
    return descriptor


def download_artifacts(payload: Mapping[str, Any], destination: str | Path | None) -> dict[str, Path]:
    from spl.execution_results import copy_artifact, safe_artifact_name

    if destination is None:
        return {}
    directory = Path(destination)
    directory.mkdir(parents=True, exist_ok=True)
    downloaded = {}
    for name, source in payload.get("artifacts", {}).items():
        path = directory / safe_artifact_name(name)
        if Path(source).resolve() != path.resolve():
            copy_artifact(Path(source), path)
        downloaded[name] = path
    return downloaded
