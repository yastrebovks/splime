"""Daemon-independent runtime-lock and virtual-environment primitives.

The integrated daemon and the public embedded client deliberately share these
command and identity rules.  Storage, lifecycle, and locking stay with their
respective callers, but there is only one definition of an exact dependency
lock and one way to construct its virtual environment.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from spl.public_artifact_policy import SPDX_ALLOWLIST as PUBLIC_RUNTIME_SPDX_ALLOWLIST
from spl.public_artifact_policy import spdx_allowed


_PACKAGE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.+_-]*$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
PUBLIC_RUNTIME_LOCK_SCHEMA = "spl.public_runtime_lock.v2"
PUBLIC_RUNTIME_POLICY_NAME = "splime-public-python-artifacts"
PUBLIC_RUNTIME_POLICY_VERSION = "0.4.8"
PUBLIC_RUNTIME_RESOLVER_NAME = "splime-pypi-closure"


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def normalize_exact_dependencies(value: Any) -> list[dict[str, str]]:
    """Validate and canonically sort an exact public dependency lock."""

    if not isinstance(value, list):
        raise ValueError("dependency lock must be a list")
    normalized: list[dict[str, str]] = []
    identities: set[str] = set()
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {"package", "version"}:
            raise ValueError("dependency lock entry must contain only package and version")
        package = item.get("package")
        version = item.get("version")
        if (
            not isinstance(package, str)
            or _PACKAGE_PATTERN.fullmatch(package) is None
            or not isinstance(version, str)
            or _VERSION_PATTERN.fullmatch(version) is None
        ):
            raise ValueError("dependency lock is not exact")
        identity = re.sub(r"[-_.]+", "-", package).casefold()
        if identity in identities:
            raise ValueError("dependency lock contains a duplicate package")
        identities.add(identity)
        normalized.append({"package": package, "version": version})
    return sorted(
        normalized,
        key=lambda item: (re.sub(r"[-_.]+", "-", item["package"]).casefold(), item["version"]),
    )


def exact_requirements(value: Any) -> list[str]:
    """Return display-only exact requirements from legacy internal dependency rows."""

    return [f"{item['package']}=={item['version']}" for item in normalize_exact_dependencies(value)]


def python_constraint_matches(constraint: Any) -> bool:
    """Return whether the current interpreter satisfies an exact prefix."""

    if constraint in (None, ""):
        return True
    if not isinstance(constraint, str) or re.fullmatch(r"\d+\.\d+(?:\.\d+)?", constraint) is None:
        raise ValueError("Python constraint must be an exact major.minor or major.minor.patch")
    expected = tuple(int(part) for part in constraint.split("."))
    actual = (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)
    return expected == actual[: len(expected)]


def runtime_lock_document(
    *,
    python: Any,
    requirements: Any,
    worker: Any,
    artifacts: Any,
    policy: Any,
    resolver: Any,
    extras: Any = None,
    runtime: str = "venv",
) -> dict[str, Any]:
    """Build the identity-bearing portion of one artifact-complete public lock."""

    if runtime != "venv":
        raise ValueError("public embedded runtime supports only venv locks")
    if python not in (None, ""):
        python_constraint_matches(python)
    if not isinstance(python, str) or re.fullmatch(r"3\.13(?:\.\d+)?", python) is None:
        raise ValueError("public runtime lock target must be CPython 3.13")
    normalized_extras = _string_list(extras or [], "target extras")
    normalized_requirements = _requirements(requirements)
    normalized_worker = _worker_artifact(worker)
    normalized_artifacts = sorted(
        (_third_party_artifact(item) for item in _mapping_list(artifacts, "artifact closure")),
        key=lambda item: (item["project"], item["version"], item["filename"]),
    )
    projects = [normalized_worker["project"], *(item["project"] for item in normalized_artifacts)]
    if len(projects) != len(set(projects)):
        raise ValueError("artifact closure contains a duplicate project")
    normalized_policy = _policy(policy)
    normalized_resolver = _resolver(resolver)
    document: dict[str, Any] = {
        "schema": PUBLIC_RUNTIME_LOCK_SCHEMA,
        "schema_version": 2,
        "runtime": runtime,
        "target": {
            "implementation": "cpython",
            "python": python,
            "extras": normalized_extras,
        },
        "policy": normalized_policy,
        "resolver": normalized_resolver,
        "requirements": normalized_requirements,
        "worker": normalized_worker,
        "artifacts": normalized_artifacts,
    }
    document["lock_hash"] = hashlib.sha256(_canonical_json(document)).hexdigest()
    return document


def validate_runtime_lock(value: Any) -> dict[str, Any]:
    """Validate a serialized runtime lock and its self-declared identity."""

    if not isinstance(value, Mapping):
        raise ValueError("runtime lock must be a mapping")
    if value.get("schema") != PUBLIC_RUNTIME_LOCK_SCHEMA or value.get("schema_version") != 2:
        raise ValueError("public runtime lock must contain the complete artifact closure")
    allowed = {
        "schema",
        "schema_version",
        "runtime",
        "target",
        "policy",
        "resolver",
        "requirements",
        "worker",
        "artifacts",
        "lock_hash",
        "names",
    }
    if "worker" not in value:
        raise ValueError("worker artifact is missing")
    if set(value) != allowed:
        raise ValueError("runtime lock contains unsupported or missing fields")
    target = value.get("target")
    if not isinstance(target, Mapping) or set(target) != {"implementation", "python", "extras"}:
        raise ValueError("runtime lock target is malformed")
    document = runtime_lock_document(
        runtime=str(value.get("runtime") or "venv"),
        python=target.get("python"),
        extras=target.get("extras"),
        requirements=value.get("requirements"),
        worker=value.get("worker"),
        artifacts=value.get("artifacts"),
        policy=value.get("policy"),
        resolver=value.get("resolver"),
    )
    lock_hash = value.get("lock_hash")
    if not isinstance(lock_hash, str) or lock_hash != document["lock_hash"]:
        raise ValueError("runtime lock hash does not match its exact contents")
    raw_names = value.get("names") or ["root"]
    if (
        not isinstance(raw_names, list)
        or not raw_names
        or any(not isinstance(name, str) or not name for name in raw_names)
    ):
        raise ValueError("runtime lock names must be a non-empty string list")
    names = sorted(set(raw_names))
    return {**document, "names": names}


def _mapping_list(value: Any, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    if any(not isinstance(item, Mapping) for item in value):
        raise ValueError(f"{label} entry is malformed")
    return list(value)


def _string_list(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{label} must be a string list")
    return sorted(set(value))


def _requirements(value: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in _mapping_list(value, "direct requirements"):
        if set(item) != {"requirement", "extras"}:
            raise ValueError("direct requirement evidence is malformed")
        requirement = item.get("requirement")
        if not isinstance(requirement, str) or not requirement or "@" in requirement:
            raise ValueError("direct requirement must be an index requirement")
        result.append({"requirement": requirement, "extras": _string_list(item.get("extras"), "requirement extras")})
    return sorted(result, key=lambda item: (item["requirement"].casefold(), item["extras"]))


def _policy(value: Any) -> dict[str, Any]:
    expected = {
        "name": PUBLIC_RUNTIME_POLICY_NAME,
        "version": PUBLIC_RUNTIME_POLICY_VERSION,
        "spdx_allowlist": list(PUBLIC_RUNTIME_SPDX_ALLOWLIST),
        "wheel_policy": "non-yanked-universal-pure-python-only",
    }
    if not isinstance(value, Mapping) or dict(value) != expected:
        raise ValueError("runtime lock policy does not match the reviewed 0.4.8 policy")
    return expected


def _resolver(value: Any) -> dict[str, Any]:
    expected = {"name": PUBLIC_RUNTIME_RESOLVER_NAME, "version": 1}
    if not isinstance(value, Mapping) or dict(value) != expected:
        raise ValueError("runtime lock resolver identity is unsupported")
    return expected


def _base_artifact(value: Any, *, worker: bool) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("worker artifact is missing" if worker else "artifact closure entry is malformed")
    common = {
        "project",
        "version",
        "filename",
        "size",
        "sha256",
        "tags",
        "requires_python",
        "yanked",
        "root_is_purelib",
        "native_members",
        "license_expression",
        "source",
    }
    required = common | (
        {"build_identity", "requires_dist"}
        if worker
        else {"direct", "parent_requirements", "evaluated_requirements", "metadata_url", "metadata_sha256"}
    )
    if set(value) != required:
        raise ValueError(
            "worker artifact evidence is incomplete" if worker else "artifact closure evidence is incomplete"
        )
    project = value.get("project")
    version = value.get("version")
    filename = value.get("filename")
    size = value.get("size")
    digest = value.get("sha256")
    if (
        not isinstance(project, str)
        or _PACKAGE_PATTERN.fullmatch(project) is None
        or not isinstance(version, str)
        or _VERSION_PATTERN.fullmatch(version) is None
        or not isinstance(filename, str)
        or not filename.endswith(".whl")
        or type(size) is not int
        or size <= 0
        or not isinstance(digest, str)
        or _SHA256_PATTERN.fullmatch(digest) is None
    ):
        raise ValueError("wheel artifact identity is malformed")
    tags = _string_list(value.get("tags"), "wheel tags")
    if not tags or any(not tag.endswith("-none-any") for tag in tags):
        raise ValueError("wheel artifact is not universal pure Python")
    if value.get("yanked") is not False or value.get("root_is_purelib") is not True:
        raise ValueError("wheel artifact is not eligible")
    if value.get("native_members") != []:
        raise ValueError("wheel artifact contains native members")
    license_expression = value.get("license_expression")
    if not isinstance(license_expression, str) or not spdx_allowed(license_expression):
        raise ValueError("wheel artifact SPDX license is not allowed")
    source = value.get("source")
    if not isinstance(source, Mapping):
        raise ValueError("wheel artifact source is malformed")
    normalized: dict[str, Any] = {
        "project": re.sub(r"[-_.]+", "-", project).casefold(),
        "version": version,
        "filename": filename,
        "size": size,
        "sha256": digest,
        "tags": tags,
        "requires_python": value.get("requires_python"),
        "yanked": False,
        "root_is_purelib": True,
        "native_members": [],
        "license_expression": license_expression,
    }
    if worker:
        if normalized["project"] != "splime-public-worker" or version != "0.4.8":
            raise ValueError("worker artifact identity is unsupported")
        if set(source) != {"kind", "bundle_member"} or source.get("kind") != "candidate-bundle":
            if set(source) != {"kind", "url"} or source.get("kind") != "official-pypi":
                raise ValueError("worker artifact source is unsupported")
        build_identity = value.get("build_identity")
        if not isinstance(build_identity, str) or _SHA256_PATTERN.fullmatch(build_identity) is None:
            raise ValueError("worker artifact build identity is malformed")
        normalized["build_identity"] = build_identity
        requires_dist = _string_list(value.get("requires_dist"), "worker requirements")
        if requires_dist:
            raise ValueError("self-contained public worker must not declare external requirements")
        normalized["requires_dist"] = requires_dist
    else:
        if set(source) != {"kind", "url"} or source.get("kind") != "official-pypi":
            raise ValueError("third-party artifacts must use official PyPI")
        if value.get("direct") not in {True, False}:
            raise ValueError("artifact provenance is malformed")
        normalized.update(
            {
                "direct": value["direct"],
                "parent_requirements": _string_list(value.get("parent_requirements"), "parent requirements"),
                "evaluated_requirements": _mapping_list(value.get("evaluated_requirements"), "evaluated requirements"),
                "metadata_url": value.get("metadata_url"),
                "metadata_sha256": value.get("metadata_sha256"),
            }
        )
        if not isinstance(normalized["metadata_url"], str) or not normalized["metadata_url"].startswith(
            "https://pypi.org/pypi/"
        ):
            raise ValueError("artifact metadata origin is not official PyPI")
        if (
            not isinstance(normalized["metadata_sha256"], str)
            or _SHA256_PATTERN.fullmatch(normalized["metadata_sha256"]) is None
        ):
            raise ValueError("artifact metadata identity is malformed")
    if set(source) == {"kind", "url"}:
        url = source.get("url")
        if not isinstance(url, str) or not url.startswith("https://files.pythonhosted.org/"):
            raise ValueError("artifact file origin is not official PyPI")
        normalized["source"] = {"kind": source["kind"], "url": url}
    else:
        member = source.get("bundle_member")
        if not isinstance(member, str) or not member.startswith("runtime-artifacts/") or ".." in member.split("/"):
            raise ValueError("worker bundle member is unsafe")
        normalized["source"] = {"kind": "candidate-bundle", "bundle_member": member}
    return normalized


def _worker_artifact(value: Any) -> dict[str, Any]:
    return _base_artifact(value, worker=True)


def _third_party_artifact(value: Any) -> dict[str, Any]:
    return _base_artifact(value, worker=False)


def environment_identity(
    lock: Mapping[str, Any],
    *,
    interpreter_abi: str,
) -> str:
    """Bind an environment only to its signed lock and interpreter ABI."""

    validated = validate_runtime_lock(lock)
    return hashlib.sha256(
        _canonical_json(
            {
                "runtime_lock_hash": validated["lock_hash"],
                "interpreter_abi": interpreter_abi,
            }
        )
    ).hexdigest()


def venv_python_path(venv_path: Path) -> Path:
    """Return the interpreter location inside a virtual environment."""

    if os.name == "nt":
        return venv_path / "Scripts" / "python.exe"
    return venv_path / "bin" / "python"


class VenvCommandBuilder(Protocol):
    """Commands required to create and populate a virtual environment."""

    @property
    def name(self) -> str: ...

    def create_command(self, spec: Mapping[str, Any]) -> list[str]: ...

    def install_command(
        self,
        spec: Mapping[str, Any],
        requirements: Sequence[str],
    ) -> list[str]: ...


@dataclass(frozen=True)
class PipVenvCommandBuilder:
    """Build a venv with the selected interpreter and pip."""

    name: str = "pip"

    def create_command(self, spec: Mapping[str, Any]) -> list[str]:
        return [str(spec["base_python"]), "-m", "venv", str(spec["venv_path"])]

    def install_command(
        self,
        spec: Mapping[str, Any],
        requirements: Sequence[str],
    ) -> list[str]:
        return [
            str(spec["python_path"]),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            *requirements,
        ]


@dataclass(frozen=True)
class UvVenvCommandBuilder:
    """Build a relocatable venv and populate it with uv."""

    executable: str
    name: str = "uv"

    def create_command(self, spec: Mapping[str, Any]) -> list[str]:
        return [
            self.executable,
            "venv",
            "--relocatable",
            "--python",
            str(spec["base_python"]),
            str(spec["venv_path"]),
        ]

    def install_command(
        self,
        spec: Mapping[str, Any],
        requirements: Sequence[str],
    ) -> list[str]:
        return [
            self.executable,
            "pip",
            "install",
            "--strict",
            "--python",
            str(spec["python_path"]),
            *requirements,
        ]


def default_venv_command_builder() -> VenvCommandBuilder:
    """Select the same preferred venv command strategy for every caller."""

    uv_executable = shutil.which("uv")
    if uv_executable:
        return UvVenvCommandBuilder(uv_executable)
    return PipVenvCommandBuilder()


__all__ = [
    "PipVenvCommandBuilder",
    "PUBLIC_RUNTIME_LOCK_SCHEMA",
    "PUBLIC_RUNTIME_POLICY_NAME",
    "PUBLIC_RUNTIME_POLICY_VERSION",
    "PUBLIC_RUNTIME_RESOLVER_NAME",
    "PUBLIC_RUNTIME_SPDX_ALLOWLIST",
    "spdx_allowed",
    "UvVenvCommandBuilder",
    "VenvCommandBuilder",
    "default_venv_command_builder",
    "environment_identity",
    "exact_requirements",
    "normalize_exact_dependencies",
    "python_constraint_matches",
    "runtime_lock_document",
    "validate_runtime_lock",
    "venv_python_path",
]
