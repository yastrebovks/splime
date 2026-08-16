"""One-shot inherited startup binding for an explicitly supervised daemon."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import stat
import sys
import threading
from dataclasses import dataclass
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any, Mapping

from spl.daemon.home_lock import DaemonInstanceIdentity
from spl.daemon.lifecycle import LIFECYCLE_SCHEMA_VERSION, LIFECYCLE_STARTUP_RECEIPT_SCHEMA

STARTUP_BINDING_SCHEMA = "spl.lifecycle.startup-binding"
STARTUP_BINDING_MAX_BYTES = 16 * 1024
STARTUP_PROOF_HEADER = "X-SPL-Supervisor-Proof"
STARTUP_DISTRIBUTION = "splime"
STARTUP_ENTRY_POINT = "spl-daemon"

_BINDING_FIELDS = frozenset(
    {
        "schema",
        "schema_version",
        "supervisor_id",
        "spawn_nonce",
        "supervisor_proof",
        "executable",
        "home",
        "release",
    }
)
_EXPECTED_FILE_FIELDS = frozenset(
    {
        "path_sha256",
        "device",
        "inode",
        "owner_uid",
        "mode",
        "sha256",
    }
)
_EXPECTED_HOME_FIELDS = frozenset({"path_sha256", "device", "inode", "owner_uid", "mode"})
_EXPECTED_RELEASE_FIELDS = frozenset({"distribution", "version", "entry_point"})


class StartupBindingError(RuntimeError):
    """Raised before serving when inherited ownership evidence is unsafe."""


@dataclass(frozen=True)
class FileIdentity:
    path_sha256: str
    device: int
    inode: int
    owner_uid: int
    mode: int
    sha256: str | None

    def document(self) -> dict[str, Any]:
        document: dict[str, Any] = {
            "path_sha256": self.path_sha256,
            "device": self.device,
            "inode": self.inode,
            "owner_uid": self.owner_uid,
            "mode": self.mode,
        }
        if self.sha256 is not None:
            document["sha256"] = self.sha256
        return document


class StartupBinding:
    """In-memory direct-child binding; its proof and nonce are never serialized."""

    def __init__(
        self,
        *,
        supervisor_id: str,
        spawn_nonce: str,
        supervisor_proof: str,
        executable: FileIdentity,
        home: FileIdentity,
        release_version: str,
    ) -> None:
        self.supervisor_id = supervisor_id
        self.binding_id = (
            "binding_"
            + hashlib.sha256(f"{supervisor_id}\x1f{spawn_nonce}\x1f{executable.sha256}".encode("utf-8")).hexdigest()[
                :40
            ]
        )
        self._proof_digest = hashlib.sha256(supervisor_proof.encode("utf-8")).digest()
        self.executable = executable
        self.home = home
        self.release_version = release_version
        self._lock = threading.Lock()
        self._consumed = False

    def proof_matches(self, value: str | None) -> bool:
        if value is None:
            return False
        return secrets.compare_digest(
            self._proof_digest,
            hashlib.sha256(value.encode("utf-8")).digest(),
        )

    def consume_receipt(
        self,
        identity: DaemonInstanceIdentity,
        *,
        proof: str | None,
        observed_at: str,
    ) -> dict[str, Any]:
        if not self.proof_matches(proof):
            raise StartupBindingError("startup binding proof is invalid")
        with self._lock:
            if self._consumed:
                raise StartupBindingError("startup binding receipt was already consumed")
            self._consumed = True
        return {
            "schema": LIFECYCLE_STARTUP_RECEIPT_SCHEMA,
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "binding_id": self.binding_id,
            "supervisor_id": self.supervisor_id,
            "instance": {
                "instance_id": identity.instance_id,
                "generation": identity.generation,
            },
            "release": {
                "distribution": STARTUP_DISTRIBUTION,
                "version": self.release_version,
                "evidence": "installed_distribution_record",
            },
            "executable_identity": self.executable.path_sha256,
            "home_identity": self.home.path_sha256,
            "observed_at": observed_at,
        }


def load_startup_binding(
    fd: int | None,
    *,
    home: Path,
    argv0: str | None = None,
    distribution: importlib_metadata.Distribution | None = None,
) -> tuple[StartupBinding | None, str | None]:
    """Consume and validate a bounded inherited binding document once."""

    if fd is None:
        return None, None
    if isinstance(fd, bool) or not isinstance(fd, int) or fd < 0:
        raise StartupBindingError("startup binding descriptor is invalid")
    raw = _read_inherited_binding(fd)
    document = _closed_object(raw, _BINDING_FIELDS, "startup binding")
    if document["schema"] != STARTUP_BINDING_SCHEMA or document["schema_version"] != LIFECYCLE_SCHEMA_VERSION:
        raise StartupBindingError("startup binding schema is not supported")

    supervisor_id = _bounded_identifier(document["supervisor_id"], "supervisor_id")
    spawn_nonce = _bounded_secret(document["spawn_nonce"], "spawn_nonce")
    supervisor_proof = _bounded_secret(document["supervisor_proof"], "supervisor_proof")

    if os.name != "posix" or not hasattr(os, "getuid") or os.getuid() == 0:
        raise StartupBindingError("supervised lifecycle Start requires a non-root POSIX process")

    installed = distribution or _installed_distribution()
    release = _closed_mapping(document["release"], _EXPECTED_RELEASE_FIELDS, "release")
    if (
        release["distribution"] != STARTUP_DISTRIBUTION
        or release["entry_point"] != STARTUP_ENTRY_POINT
        or release["version"] != installed.version
    ):
        raise StartupBindingError("installed release identity does not match the startup binding")

    launch_path = _installed_entry_point_path(installed)
    supplied_argv0 = Path(argv0 if argv0 is not None else sys.argv[0])
    if not supplied_argv0.is_absolute() or not _same_file_without_symlink(supplied_argv0, launch_path):
        raise StartupBindingError("daemon was not launched through the protected installed entry point")

    executable = _protected_file_identity(launch_path, include_sha256=True)
    _require_expected_identity(
        executable,
        _closed_mapping(document["executable"], _EXPECTED_FILE_FIELDS, "executable"),
        include_sha256=True,
    )
    home_identity = _protected_home_identity(home)
    _require_expected_identity(
        home_identity,
        _closed_mapping(document["home"], _EXPECTED_HOME_FIELDS, "home"),
        include_sha256=False,
    )
    return (
        StartupBinding(
            supervisor_id=supervisor_id,
            spawn_nonce=spawn_nonce,
            supervisor_proof=supervisor_proof,
            executable=executable,
            home=home_identity,
            release_version=str(installed.version),
        ),
        supervisor_proof,
    )


def startup_binding_selection(
    executable: Path,
    home: Path,
    *,
    distribution: importlib_metadata.Distribution | None = None,
) -> dict[str, Any]:
    """Return exact server-only identities for a trusted supervisor preflight."""

    installed = distribution or _installed_distribution()
    installed_path = _installed_entry_point_path(installed)
    if not _same_file_without_symlink(executable, installed_path):
        raise StartupBindingError("selected executable is not the installed spl-daemon entry point")
    return {
        "executable": _protected_file_identity(installed_path, include_sha256=True).document(),
        "home": _protected_home_identity(home).document(),
        "release": {
            "distribution": STARTUP_DISTRIBUTION,
            "version": installed.version,
            "entry_point": STARTUP_ENTRY_POINT,
        },
    }


def revalidate_startup_binding(binding: StartupBinding, *, home: Path) -> None:
    """Recheck exact executable/home identities after acquiring the home lock."""

    installed = _installed_distribution()
    if str(installed.version) != binding.release_version:
        raise StartupBindingError("installed release identity changed during startup")
    executable = _protected_file_identity(
        _installed_entry_point_path(installed),
        include_sha256=True,
    )
    home_identity = _protected_home_identity(home)
    if executable != binding.executable or home_identity != binding.home:
        raise StartupBindingError("startup selection identity changed before daemon readiness")


def _read_inherited_binding(fd: int) -> dict[str, Any]:
    try:
        with os.fdopen(fd, "rb", closefd=True) as stream:
            encoded = stream.read(STARTUP_BINDING_MAX_BYTES + 1)
    except OSError as exc:
        raise StartupBindingError("startup binding could not be read") from exc
    if not encoded or len(encoded) > STARTUP_BINDING_MAX_BYTES:
        raise StartupBindingError("startup binding size is invalid")
    try:
        value = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise StartupBindingError("startup binding is not valid JSON") from exc
    return _closed_object(value, _BINDING_FIELDS, "startup binding")


def _installed_distribution() -> importlib_metadata.Distribution:
    try:
        return importlib_metadata.distribution(STARTUP_DISTRIBUTION)
    except importlib_metadata.PackageNotFoundError as exc:
        raise StartupBindingError("installed release metadata is unavailable") from exc


def _installed_entry_point_path(distribution: importlib_metadata.Distribution) -> Path:
    entry_points = [
        item
        for item in distribution.entry_points
        if item.group == "console_scripts" and item.name == STARTUP_ENTRY_POINT and item.value == "spl.daemon.cli:main"
    ]
    if len(entry_points) != 1:
        raise StartupBindingError("installed spl-daemon entry point is ambiguous")
    candidates = [
        item
        for item in distribution.files or ()
        if Path(str(item)).name == STARTUP_ENTRY_POINT and item.hash is not None
    ]
    if len(candidates) != 1:
        raise StartupBindingError("installed spl-daemon RECORD entry is unavailable")
    record = candidates[0]
    path = Path(record.locate()).absolute()
    identity = _protected_file_identity(path, include_sha256=True)
    if record.hash is None or record.hash.mode != "sha256":
        raise StartupBindingError("installed spl-daemon RECORD hash is unsupported")
    expected = base64.urlsafe_b64decode(record.hash.value + "=" * (-len(record.hash.value) % 4)).hex()
    if identity.sha256 != f"sha256:{expected}":
        raise StartupBindingError("installed spl-daemon entry point does not match RECORD")
    return path


def _protected_file_identity(path: Path, *, include_sha256: bool) -> FileIdentity:
    if not path.is_absolute():
        raise StartupBindingError("executable path must be absolute")
    try:
        linked = path.lstat()
        resolved = path.resolve(strict=True)
        current = resolved.stat()
    except OSError as exc:
        raise StartupBindingError("selected executable is unavailable") from exc
    if stat.S_ISLNK(linked.st_mode) or not stat.S_ISREG(current.st_mode):
        raise StartupBindingError("selected executable must be a non-symlink regular file")
    if current.st_uid not in {os.getuid(), 0} or current.st_mode & 0o022:
        raise StartupBindingError("selected executable ownership or permissions are unsafe")
    _require_protected_parent(resolved.parent)
    digest = f"sha256:{_sha256_file(resolved)}" if include_sha256 else None
    return FileIdentity(
        path_sha256=_path_digest(resolved),
        device=current.st_dev,
        inode=current.st_ino,
        owner_uid=current.st_uid,
        mode=stat.S_IMODE(current.st_mode),
        sha256=digest,
    )


def _protected_home_identity(home: Path) -> FileIdentity:
    if not home.is_absolute():
        raise StartupBindingError("daemon home path must be absolute")
    try:
        linked = home.lstat()
        resolved = home.resolve(strict=True)
        current = resolved.stat()
    except OSError as exc:
        raise StartupBindingError("daemon home is unavailable") from exc
    if stat.S_ISLNK(linked.st_mode) or not stat.S_ISDIR(current.st_mode):
        raise StartupBindingError("daemon home must be a non-symlink directory")
    if current.st_uid != os.getuid() or stat.S_IMODE(current.st_mode) != 0o700:
        raise StartupBindingError("daemon home must be owned by the current user with mode 0700")
    return FileIdentity(
        path_sha256=_path_digest(resolved),
        device=current.st_dev,
        inode=current.st_ino,
        owner_uid=current.st_uid,
        mode=stat.S_IMODE(current.st_mode),
        sha256=None,
    )


def _require_protected_parent(path: Path) -> None:
    current = path
    while True:
        metadata = current.stat()
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o022:
            raise StartupBindingError("selected executable has a writable parent directory")
        if current.parent == current:
            return
        current = current.parent


def _same_file_without_symlink(first: Path, second: Path) -> bool:
    try:
        if stat.S_ISLNK(first.lstat().st_mode) or stat.S_ISLNK(second.lstat().st_mode):
            return False
        return first.samefile(second)
    except OSError:
        return False


def _require_expected_identity(
    observed: FileIdentity,
    expected: Mapping[str, Any],
    *,
    include_sha256: bool,
) -> None:
    observed_document = observed.document()
    expected_document = dict(expected)
    if not include_sha256:
        observed_document.pop("sha256", None)
    if observed_document != expected_document:
        raise StartupBindingError("startup selection identity changed before daemon readiness")


def _closed_object(value: Any, expected: frozenset[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise StartupBindingError(f"{name} fields do not match the v1 contract")
    return dict(value)


def _closed_mapping(value: Any, expected: frozenset[str], name: str) -> Mapping[str, Any]:
    return _closed_object(value, expected, name)


def _bounded_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 160 or not value.isascii():
        raise StartupBindingError(f"{name} is invalid")
    if any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-" for character in value
    ):
        raise StartupBindingError(f"{name} is invalid")
    return value


def _bounded_secret(value: Any, name: str) -> str:
    if not isinstance(value, str) or not 16 <= len(value) <= 512 or not value.isascii():
        raise StartupBindingError(f"{name} is invalid")
    return value


def _path_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(os.fsencode(path)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "STARTUP_BINDING_SCHEMA",
    "STARTUP_PROOF_HEADER",
    "StartupBinding",
    "StartupBindingError",
    "load_startup_binding",
    "revalidate_startup_binding",
    "startup_binding_selection",
]
