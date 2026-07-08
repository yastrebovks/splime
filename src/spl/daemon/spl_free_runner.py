"""Stdlib-only runner for SPL-free functional node execution."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
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

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


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


def to_jsonable(value: Any) -> Any:
    """Convert common Python containers into JSON-compatible values."""

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
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    for name, source in items:
        artifact_name = validate_name(str(name))
        source_path = Path(str(source)).expanduser().absolute()
        target_path = artifacts_dir / artifact_name
        copy_artifact(source_path, target_path)
        copied[artifact_name] = str(target_path)

    return result, copied


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
