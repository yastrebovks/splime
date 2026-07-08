"""Runtime configuration for SPL object versions."""

from __future__ import annotations

import re
from typing import Any

DEFAULT_RUNTIME_MODE = "venv"
DEFAULT_DOCKER_PYTHON = "3.13"
DEFAULT_DOCKER_DISTRO = "trixie"
SUPPORTED_RUNTIME_MODES = {"venv", "docker"}
SUPPORTED_DOCKER_NETWORK_MODES = {"auto", "none", "enabled"}
DEFAULT_DOCKER_PIDS_LIMIT = 256
DEFAULT_DOCKER_TMPFS_SIZE = "512m"


def normalize_runtime_config(value: dict[str, Any] | None) -> dict[str, Any]:
    """Return a validated, JSON-compatible runtime configuration.

    ``value`` may be either a direct runtime config or a document with a
    top-level ``runtime`` key.  This keeps sidecar YAML files ergonomic without
    changing the SPL IR YAML format.
    """

    raw = dict(value or {})
    if "runtime" in raw:
        nested = raw["runtime"]
        if not isinstance(nested, dict):
            raise ValueError("runtime config field 'runtime' must be a mapping")
        raw = dict(nested)

    mode = str(raw.get("mode") or raw.get("type") or DEFAULT_RUNTIME_MODE).lower()
    if mode not in SUPPORTED_RUNTIME_MODES:
        raise ValueError("runtime mode must be 'venv' or 'docker'")

    if mode == "venv":
        return {"mode": "venv"}

    python = str(raw.get("python") or raw.get("python_version") or DEFAULT_DOCKER_PYTHON)
    _validate_python_version(python)
    distro = str(raw.get("distro") or DEFAULT_DOCKER_DISTRO).lower()
    base_image = str(raw.get("base_image") or raw.get("image") or f"python:{python}-slim-{distro}")
    network = str(raw.get("network") or "auto").lower()
    if network not in SUPPORTED_DOCKER_NETWORK_MODES:
        raise ValueError("docker runtime network must be 'auto', 'none', or 'enabled'")
    apt_packages = _string_list(raw.get("apt_packages") or [])
    limits = _normalize_limits(raw.get("limits") or raw)

    config = {
        "mode": "docker",
        "python": python,
        "base_image": base_image,
        "network": network,
        "apt_packages": apt_packages,
        "limits": limits,
        "read_only": _bool_value(raw.get("read_only"), True),
        "tmpfs": str(raw.get("tmpfs") or f"/tmp:rw,nosuid,size={DEFAULT_DOCKER_TMPFS_SIZE}"),
        "env": _normalize_env(raw.get("env") or {}),
        "cap_drop": str(raw.get("cap_drop") or "ALL"),
        "no_new_privileges": _bool_value(raw.get("no_new_privileges"), True),
        "init": _bool_value(raw.get("init"), True),
    }
    if raw.get("pull") is not None:
        config["pull"] = bool(raw["pull"])
    return config


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("runtime config value must be a list of strings")
    items = [str(item).strip() for item in value]
    if any(not item for item in items):
        raise ValueError("runtime config string list contains an empty value")
    return items


def _normalize_limits(raw: dict[str, Any]) -> dict[str, Any]:
    limits: dict[str, Any] = {}
    memory = raw.get("memory")
    if memory is not None:
        limits["memory"] = str(memory)
    cpus = raw.get("cpus")
    if cpus is not None:
        cpus_value = float(cpus)
        if cpus_value <= 0:
            raise ValueError("docker runtime cpus limit must be positive")
        limits["cpus"] = str(cpus)
    pids_limit = raw.get("pids_limit", raw.get("pids-limit", DEFAULT_DOCKER_PIDS_LIMIT))
    if pids_limit is not None:
        pids_value = int(pids_limit)
        if pids_value <= 0:
            raise ValueError("docker runtime pids_limit must be positive")
        limits["pids_limit"] = pids_value
    return limits


def _normalize_env(raw: dict[str, Any]) -> dict[str, str]:
    if not isinstance(raw, dict):
        raise ValueError("docker runtime env must be a mapping")
    return {str(key): str(value) for key, value in raw.items()}


def _bool_value(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _validate_python_version(value: str) -> None:
    if not re.fullmatch(r"3\.(?:1[3-9]|[2-9][0-9])(?:\.\d+)?", value):
        raise ValueError(
            "docker runtime python must be 3.13 or newer because SPL packages currently require Python >= 3.13"
        )
