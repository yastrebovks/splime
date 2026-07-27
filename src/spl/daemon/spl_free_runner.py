"""Stdlib-only runner for SPL-free functional node execution."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import math
import os
import re
import shutil
import sys
from collections.abc import Iterable, Mapping, Sequence
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from types import ModuleType
from typing import Any

ARTIFACTS_KEY = "__spl_artifacts__"
RESULT_KEY = "__spl_result__"
NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
JSON_SCALARS = frozenset({type(None), bool, int, float, str})


def validate_name(name: str) -> str:
    """Validate a registry-safe name and return it unchanged."""

    # Keep this rule in sync with spl.daemon.storage_base.validate_name;
    # the runner duplicates it intentionally to stay stdlib-only.
    if not NAME_PATTERN.fullmatch(name) or set(name) == {"."}:
        raise ValueError("name must contain only letters, digits, underscore, dash, and dot, and not only dots")
    return name


def read_json(path: Path) -> Any:
    """Read a UTF-8 JSON file."""

    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    """Write a UTF-8 JSON file with stable formatting."""

    payload = _json_dumps(value, ensure_ascii=False, indent=2, sort_keys=True, separators=None)
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


def validate_environment(distributions: list[dict[str, str]]) -> None:
    """Fail when the runner interpreter does not match SPL metadata."""

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
    """Convert common Python containers into JSON-compatible values."""

    if type(value) in JSON_SCALARS:
        _validate_json_value(value, path=path)
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                _validate_json_value({key: None}, path=path)
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


def _json_dumps(
    value: Any,
    *,
    ensure_ascii: bool = False,
    indent: int | str | None = None,
    sort_keys: bool = True,
    separators: tuple[str, str] | None = (",", ":"),
) -> str:
    """Mirror the packaged JSON contract inside this stdlib-only runner."""

    _validate_json_value(value)
    return json.dumps(
        value,
        ensure_ascii=ensure_ascii,
        indent=indent,
        sort_keys=sort_keys,
        separators=separators,
        allow_nan=False,
    )


def _validate_json_value(value: Any, *, path: str = "$", active_ids: set[int] | None = None) -> None:
    active_ids = set() if active_ids is None else active_ids
    value_type = type(value)
    if value_type in JSON_SCALARS:
        if value_type is float and not math.isfinite(value):
            raise ValueError("invalid splime JSON value at {}: non-finite floats are not permitted".format(path))
        if value_type is str:
            _validate_unicode_scalar_string(value, path=path, role="string")
        return
    if value_type not in {list, dict}:
        raise ValueError(
            "invalid splime JSON value at {}: unsupported value type `{}`".format(path, value_type.__name__)
        )

    container_id = id(value)
    if container_id in active_ids:
        raise ValueError("invalid splime JSON value at {}: circular container reference is not permitted".format(path))
    active_ids.add(container_id)
    try:
        if value_type is list:
            for index, item in enumerate(value):
                _validate_json_value(item, path="{}[{}]".format(path, index), active_ids=active_ids)
            return
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("invalid splime JSON value at {}: object key {!r} is not a string".format(path, key))
            _validate_unicode_scalar_string(key, path=path, role="object key")
            _validate_json_value(item, path=_json_child_path(path, key), active_ids=active_ids)
    finally:
        active_ids.remove(container_id)


def _json_child_path(path: str, key: str) -> str:
    return "{}[{}]".format(path, json.dumps(key, ensure_ascii=False, allow_nan=False))


def _validate_unicode_scalar_string(value: str, *, path: str, role: str) -> None:
    for index, character in enumerate(value):
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDFFF:
            raise ValueError(
                "invalid splime JSON value at {}: {} contains Unicode surrogate U+{:04X} at character {}; "
                "use Unicode scalar values".format(path, role, code_point, index)
            )


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
    """Extract and copy artifacts declared by the function result."""

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
        artifact_name = validate_name(str(name))
        source_path = Path(str(source)).expanduser().absolute()
        target_path = artifacts_dir / artifact_name
        copy_artifact(source_path, target_path)
        copied[artifact_name] = str(target_path)

    return result, copied


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


def load_module(module_path: Path, module_name: str) -> ModuleType:
    """Import a generated module by path without consulting PYTHONPATH."""

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import generated module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    previous_module = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        if previous_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous_module
        raise
    return module


def execute(
    *,
    module_path: Path,
    module_name: str,
    entrypoint: str,
    input_path: Path,
    result_path: Path,
    artifacts_dir: Path,
    env_spec_path: Path | None = None,
) -> dict[str, Any]:
    """Import, call, and persist one generated functional node."""

    payload = read_json(input_path)
    args = payload.get("args", [])
    kwargs = payload.get("kwargs", {})

    if env_spec_path is not None:
        validate_environment(read_json(env_spec_path))

    module = load_module(module_path, module_name)
    try:
        target = getattr(module, entrypoint)
    except AttributeError as exc:
        raise KeyError(f"entrypoint is not found in generated module: {entrypoint}") from exc
    if not callable(target):
        raise TypeError(f"entrypoint is not callable: {entrypoint}")

    raw_result = target(*args, **kwargs)
    result_without_artifacts, artifacts = collect_artifacts(raw_result, artifacts_dir)
    result_payload = {
        "result": to_jsonable(result_without_artifacts),
        "artifacts": artifacts,
    }
    write_json(result_path, result_payload)
    return result_payload


def build_parser() -> argparse.ArgumentParser:
    """Create the runner argument parser."""

    parser = argparse.ArgumentParser(description="Execute one generated SPL function")
    parser.add_argument("--module", required=True, type=Path)
    parser.add_argument("--module-name", required=True)
    parser.add_argument("--entrypoint", required=True)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--artifacts-dir", required=True, type=Path)
    parser.add_argument("--env-spec", default=None, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the SPL-free runner from the command line."""

    args = build_parser().parse_args(argv)
    execute(
        module_path=args.module,
        module_name=args.module_name,
        entrypoint=args.entrypoint,
        input_path=args.input,
        result_path=args.result,
        artifacts_dir=args.artifacts_dir,
        env_spec_path=args.env_spec,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
