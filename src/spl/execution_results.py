"""Side-effect-free JSON/artifact result protocol shared by execution hosts."""

from __future__ import annotations

import re
import shutil
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from spl.core import json_contract as m_json_contract
from spl.daemon.name_validation import validate_name

ARTIFACTS_KEY = "__spl_artifacts__"
ARTIFACT_REF_KEY = "__spl_artifact_ref__"
RESULT_KEY = "__spl_result__"
_ARTIFACT_NAME_TOKEN_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")


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


class PipelineResultNormalizer:
    """Convert final pipeline values into the daemon's JSON/artifact protocol."""

    def __init__(
        self,
        pipeline: Any,
        artifacts_dir: Path,
        *,
        materialize_output: Callable[..., Any] | None = None,
        runtime_adapter_document: Mapping[str, Any] | None = None,
        runtime_custom_namespace: Mapping[str, Any] | None = None,
        runtime_output_records: list[dict[str, Any]] | None = None,
        runtime_result_prefix: Sequence[str] = (),
        allow_native_values: bool = False,
    ):
        self._materialize_output = materialize_output
        self.pipeline = pipeline
        self.artifacts_dir = artifacts_dir
        self.artifacts: dict[str, str] = {}
        self._used_artifact_names: set[str] = set()
        self._runtime_custom_namespace = dict(runtime_custom_namespace or {})
        self._runtime_output_records = runtime_output_records
        self._runtime_result_prefix = tuple(runtime_result_prefix)
        self._allow_native_values = allow_native_values
        self._runtime_output_bindings = {
            tuple(binding["result_path"]): binding
            for binding in (runtime_adapter_document or {}).get("bindings", [])
            if binding["direction"] == "output" and binding["transport"] == "artifact"
        }

    def normalize(self, value: Any, path: tuple[str, ...] = ("result",)) -> Any:
        if self._allow_native_values:
            return self._normalize_native(value, path)
        result_path = tuple(path[1:])
        if (
            self._runtime_result_prefix
            and result_path[: len(self._runtime_result_prefix)] == self._runtime_result_prefix
        ):
            result_path = result_path[len(self._runtime_result_prefix) :]
        runtime_binding = self._runtime_output_bindings.get(result_path)
        if runtime_binding is not None:
            if self._materialize_output is None:
                raise ValueError("runtime output binding requires an execution host")
            reference, copied, record = self._materialize_output(
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
            normalized = [self.normalize(item, (*path, str(index))) for index, item in enumerate(value)]
            return normalized
        if isinstance(value, set):
            normalized = [
                self.normalize(item, (*path, str(index))) for index, item in enumerate(sorted(value, key=repr))
            ]
            return normalized
        return self._materialize_adapter_artifact(value, path)

    def _normalize_native(self, value: Any, path: tuple[str, ...]) -> Any:
        """Copy only ancestors of explicit artifact conversions, never plain data.

        Discovery is iterative and cycle-safe. Opaque objects (including arrays,
        frames and custom mappings) are not traversed. Unchanged subgraphs are
        returned verbatim; a memo preserves shared references when conversion
        does require new enclosing containers.
        """
        values: dict[int, Any] = {}
        parents: dict[int, set[int]] = {}
        replacements: dict[int, tuple[bool, Any]] = {}
        wrapper_paths: dict[int, tuple[str, ...]] = {}
        pending = [(value, path)]
        while pending:
            item, item_path = pending.pop()
            marker = id(item)
            if marker in values:
                continue
            values[marker] = item
            result_path = item_path[1:]
            if (
                self._runtime_result_prefix
                and result_path[: len(self._runtime_result_prefix)] == self._runtime_result_prefix
            ):
                result_path = result_path[len(self._runtime_result_prefix) :]
            binding = self._runtime_output_bindings.get(result_path)
            children = []
            if binding is not None:
                if self._materialize_output is None:
                    raise ValueError("runtime output binding requires an execution host")
                reference, copied, record = self._materialize_output(
                    item,
                    binding,
                    artifacts_dir=self.artifacts_dir,
                    used_names=self._used_artifact_names,
                    custom_namespace=self._runtime_custom_namespace,
                )
                self.artifacts.update(copied)
                if self._runtime_output_records is not None:
                    self._runtime_output_records.append(record)
                replacements[marker] = (False, reference)
            elif isinstance(item, Mapping) and ARTIFACTS_KEY in item:
                wrapper_paths[marker] = item_path
                replacement = self._result_from_explicit_artifact_mapping(item)
                self._copy_declared_artifacts(item[ARTIFACTS_KEY])
                if replacement is item:
                    replacement = {key: child for key, child in item.items() if key not in {ARTIFACTS_KEY, RESULT_KEY}}
                replacements[marker] = (True, replacement)
                children = [(replacement, item_path)]
            elif type(item) is dict:
                children = [
                    (child, (*item_path, key if type(key) is str else str(index)))
                    for index, (key, child) in enumerate(item.items())
                ]
            elif type(item) in {list, tuple, set, frozenset}:
                children = [(child, (*item_path, str(index))) for index, child in enumerate(item)]
            elif type(item) not in m_json_contract.JSON_SCALARS and not isinstance(item, Path):
                try:
                    adapter = self.pipeline.resolve_adapter(py_type=type(item))
                except ValueError as exc:
                    raise TypeError(f"{_result_path(item_path)} has ambiguous adapters for {_type_name(item)}") from exc
                if adapter is not None:
                    replacements[marker] = (False, self._materialize_adapter_artifact(item, item_path))
            for child, child_path in children:
                parents.setdefault(id(child), set()).add(marker)
                pending.append((child, child_path))

        affected = set(replacements)
        ancestors = list(affected)
        while ancestors:
            for parent in parents.get(ancestors.pop(), ()):
                if parent not in affected:
                    affected.add(parent)
                    ancestors.append(parent)
        memo = {marker: item for marker, item in values.items() if marker not in affected}

        def convert(item: Any) -> Any:
            # Resolve aliases before constructing a container. A repeated alias
            # here has no concrete result; cycles through memoized containers do.
            alias_ids: set[int] = set()
            while (marker := id(item)) not in memo:
                if marker in alias_ids:
                    raise ValueError(f"{_result_path(wrapper_paths[marker])}: cyclic explicit result-wrapper chain")
                alias_ids.add(marker)
                if marker not in replacements or not replacements[marker][0]:
                    break
                item = replacements[marker][1]
            if marker in memo:
                result = memo[marker]
            elif marker in replacements:
                result = replacements[marker][1]
            elif type(item) in {dict, list}:
                result = {} if type(item) is dict else []
                for alias in alias_ids:
                    memo[alias] = result
                if isinstance(result, dict):
                    result.update((key, convert(child)) for key, child in item.items())
                else:
                    result.extend(convert(child) for child in item)
            else:
                children = [convert(child) for child in item]
                # An intervening mutable container may have closed a tuple cycle.
                if marker in memo:
                    result = memo[marker]
                elif type(item) is tuple:
                    result = tuple(children)
                else:
                    try:
                        result = type(item)(children)
                    except TypeError:
                        result = children  # Artifact references are unhashable.
            for alias in alias_ids:
                memo[alias] = result
            return result

        return convert(value)

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
