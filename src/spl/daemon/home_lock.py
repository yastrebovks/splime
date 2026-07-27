"""Exclusive process ownership and stable identity for one daemon home.

The lock file deliberately keeps a stable inode for its whole lifetime.  Never
replace or unlink it: advisory locks attach to the opened file rather than its
path.  Durable per-home identity lives in a separate atomically replaced file,
so a crash while owner metadata is updated cannot silently change the Docker
namespace on the next start.
"""

from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
import secrets
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Protocol, TypeGuard, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request

from spl._http import urlopen_verified
from spl.core import json_contract as m_json_contract
from spl.daemon.storage_base import default_home, utc_now
from spl.daemon_client import read_daemon_endpoint

LOGGER = logging.getLogger(__name__)

DAEMON_HOME_LOCK_FILENAME = "daemon.lock"
DAEMON_HOME_IDENTITY_FILENAME = "daemon-identity.json"
DAEMON_HOME_LOCK_PROBE_SECONDS = 1.0
DAEMON_HOME_LOCK_RETRY_SECONDS = 0.05
DAEMON_HOME_LOCK_RETRIES = 10
DAEMON_HOME_METADATA_MAX_BYTES = 16 * 1024
DAEMON_HEALTH_MAX_BYTES = 64 * 1024
DAEMON_HOME_IDENTITY_SCHEMA_VERSION = 1
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


@dataclass(frozen=True)
class DaemonInstanceIdentity:
    """Stable home identity plus the generation of one daemon process."""

    instance_id: str
    home_hash: str
    generation: int
    previous_generation: int | None
    pid: int
    started_at: str
    stale_takeover: bool = False


class DaemonHomeLockedError(RuntimeError):
    """Raised when another process still owns a daemon home."""


class DaemonHomeLock:
    """Hold the exclusive advisory lock for a daemon home."""

    def __init__(self, home: str | Path | None = None):
        selected_home = default_home() if home is None else Path(home)
        self.home = selected_home.expanduser().absolute()
        self.path = self.home / DAEMON_HOME_LOCK_FILENAME
        self.identity_path = self.home / DAEMON_HOME_IDENTITY_FILENAME
        self._fd: int | None = None
        self.identity: DaemonInstanceIdentity | None = None

    def __enter__(self) -> DaemonHomeLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        self.release()

    @property
    def is_acquired(self) -> bool:
        """Return whether this process still owns the advisory lock."""

        return self._fd is not None

    def acquire(self) -> DaemonInstanceIdentity:
        """Acquire ownership before any store or endpoint mutation."""

        if self._fd is not None:
            raise RuntimeError("daemon home lock is already acquired")

        self.home.mkdir(mode=0o700, parents=True, exist_ok=True)
        _set_owner_only(self.home, 0o700)
        fd = _open_lock_file(self.path)
        acquired = False
        try:
            _initialize_lock_byte(fd)
            for attempt in range(DAEMON_HOME_LOCK_RETRIES):
                try:
                    _lock_fd(fd)
                    acquired = True
                    break
                except BlockingIOError:
                    owner = _read_lock_payload(fd)
                    if attempt in {0, DAEMON_HOME_LOCK_RETRIES - 1}:
                        endpoint = read_daemon_endpoint(self.home)
                        if _probe_endpoint(endpoint, self.home):
                            raise _already_running_error(endpoint, owner, self.home)
                    if attempt + 1 < DAEMON_HOME_LOCK_RETRIES:
                        time.sleep(DAEMON_HOME_LOCK_RETRY_SECONDS)
                        continue
                    pid = _owner_pid(owner)
                    raise DaemonHomeLockedError(
                        f"daemon home {self.home} is locked by pid {pid}; its endpoint is not answering. "
                        "The daemon may still be starting or may be unresponsive. Wait for startup to finish "
                        "or stop that process; use a different --home to run another daemon."
                    )

            if not acquired:  # Defensive: the loop either acquires or raises.
                raise RuntimeError("daemon home lock acquisition ended without a result")

            previous = _read_lock_payload(fd)
            previous_endpoint = read_daemon_endpoint(self.home)
            # A pre-0.4.5 daemon does not hold daemon.lock.  The authenticated,
            # home-bound probe prevents a new daemon from sharing its store.
            if _probe_endpoint(previous_endpoint, self.home):
                raise _already_running_error(previous_endpoint, previous, self.home)

            home_hash = _home_hash(self.home)
            instance_id, previous_generation, generation = _advance_identity(
                self.identity_path,
                home_hash=home_hash,
                previous_lock_payload=previous,
            )
            previous_state = previous.get("state")
            lock_generation = _valid_generation(previous.get("generation"))
            clean_previous_stop = previous_state == "stopped" and lock_generation == previous_generation
            stale_takeover = previous_endpoint is not None or (
                previous_generation is not None and not clean_previous_stop
            )
            identity = DaemonInstanceIdentity(
                instance_id=instance_id,
                home_hash=home_hash,
                generation=generation,
                previous_generation=previous_generation,
                pid=os.getpid(),
                started_at=utc_now(),
                stale_takeover=stale_takeover,
            )
            self._fd = fd
            self.identity = identity
            self._write_state("running")
            if stale_takeover:
                LOGGER.warning(
                    "taking over stale daemon home state for %s (previous pid=%s, generation=%s)",
                    self.home,
                    previous.get("pid"),
                    previous_generation,
                )
            return identity
        except BaseException:
            if acquired:
                try:
                    _unlock_fd(fd)
                except OSError:
                    pass
            os.close(fd)
            self._fd = None
            self.identity = None
            raise

    def release(self) -> None:
        """Record a clean stop, unlock, and retain metadata for recovery."""

        fd = self._fd
        if fd is None:
            return
        try:
            try:
                self._write_state("stopped")
            except OSError as exc:
                LOGGER.warning("could not record clean daemon-home shutdown for %s: %s", self.home, exc)
        finally:
            try:
                _unlock_fd(fd)
            finally:
                os.close(fd)
                self._fd = None

    def _write_state(self, state: str) -> None:
        fd = self._fd
        identity = self.identity
        if fd is None or identity is None:
            raise RuntimeError("daemon home lock is not acquired")
        payload = {
            "generation": identity.generation,
            "home_hash": identity.home_hash,
            "instance_id": identity.instance_id,
            "pid": identity.pid,
            "started_at": identity.started_at,
            "state": state,
            "updated_at": utc_now(),
        }
        encoded = (
            m_json_contract.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, separators=None) + "\n"
        ).encode("utf-8")
        if len(encoded) > DAEMON_HOME_METADATA_MAX_BYTES:
            raise RuntimeError("daemon home lock metadata exceeds its fixed size limit")
        # Byte zero is the Windows lock range.  Whitespace keeps the remainder
        # valid JSON while allowing metadata to be rewritten under that lock.
        os.lseek(fd, 0, os.SEEK_SET)
        _write_all(fd, b"\n")
        os.ftruncate(fd, 1)
        os.lseek(fd, 1, os.SEEK_SET)
        _write_all(fd, encoded)
        os.fsync(fd)
        _set_owner_only(self.path, 0o600)


def _open_lock_file(path: Path) -> int:
    flags = os.O_RDWR | os.O_CREAT
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.set_inheritable(fd, False)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise RuntimeError(f"daemon home lock is not a regular file: {path}")
        _set_owner_only(path, 0o600)
    except BaseException:
        os.close(fd)
        raise
    return fd


def _initialize_lock_byte(fd: int) -> None:
    if os.fstat(fd).st_size != 0:
        return
    os.lseek(fd, 0, os.SEEK_SET)
    _write_all(fd, b"\n")
    os.fsync(fd)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("could not write daemon home metadata")
        view = view[written:]


def _read_lock_payload(fd: int) -> dict[str, Any]:
    try:
        size = os.fstat(fd).st_size
        if size <= 0 or size > DAEMON_HOME_METADATA_MAX_BYTES:
            return {}
        raw = os.pread(fd, size, 0) if hasattr(os, "pread") else _read_fd_portably(fd, size)
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_fd_portably(fd: int, size: int) -> bytes:
    position = os.lseek(fd, 0, os.SEEK_CUR)
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        return os.read(fd, size)
    finally:
        os.lseek(fd, position, os.SEEK_SET)


def _advance_identity(
    path: Path,
    *,
    home_hash: str,
    previous_lock_payload: dict[str, Any],
) -> tuple[str, int | None, int]:
    stored = _read_identity(path)
    lock_home_hash = previous_lock_payload.get("home_hash")
    lock_instance = previous_lock_payload.get("instance_id")
    lock_generation = _valid_generation(previous_lock_payload.get("generation"))
    if stored is None:
        lock_matches_home = lock_home_hash == home_hash and _valid_instance_id(lock_instance)
        instance_id = str(lock_instance) if lock_matches_home else secrets.token_hex(16)
        previous_generation = lock_generation if lock_matches_home else None
    else:
        stored_home_hash = stored.get("home_hash")
        if stored_home_hash != home_hash:
            raise RuntimeError(
                f"daemon identity at {path} belongs to a different canonical home; "
                "restore the matching home or remove the copied identity file before starting"
            )
        stored_instance = stored.get("instance_id")
        stored_generation = stored.get("generation")
        if not _valid_instance_id(stored_instance) or _valid_generation(stored_generation) is None:
            raise RuntimeError(f"daemon identity metadata is invalid: {path}")
        assert isinstance(stored_instance, str)
        instance_id = stored_instance
        stored_generation_value = _valid_generation(stored_generation)
        assert stored_generation_value is not None
        previous_generation = stored_generation_value
        lock_has_identity = lock_home_hash is not None or lock_instance is not None or lock_generation is not None
        if lock_has_identity:
            if lock_home_hash != home_hash or lock_instance != instance_id:
                raise RuntimeError(
                    f"daemon lock metadata disagrees with the durable identity at {path}; "
                    "restore the matching daemon home before starting"
                )
            if lock_generation is not None:
                previous_generation = max(previous_generation, lock_generation)

    generation = (previous_generation or 0) + 1
    _write_identity(
        path,
        {
            "generation": generation,
            "home_hash": home_hash,
            "instance_id": instance_id,
            "schema_version": DAEMON_HOME_IDENTITY_SCHEMA_VERSION,
            "updated_at": utc_now(),
        },
    )
    return instance_id, previous_generation, generation


def _read_identity(path: Path) -> dict[str, Any] | None:
    try:
        path_stat = path.lstat()
        if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISREG(path_stat.st_mode):
            raise RuntimeError(f"daemon identity metadata is invalid: {path}")
        with path.open("rb") as handle:
            raw = handle.read(DAEMON_HOME_METADATA_MAX_BYTES + 1)
        if not raw or len(raw) > DAEMON_HOME_METADATA_MAX_BYTES:
            raise RuntimeError(f"daemon identity metadata is invalid: {path}")
        value = json.loads(raw.decode("utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"daemon identity metadata is invalid: {path}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != DAEMON_HOME_IDENTITY_SCHEMA_VERSION:
        raise RuntimeError(f"daemon identity metadata is invalid: {path}")
    return value


def _write_identity(path: Path, payload: dict[str, Any]) -> None:
    encoded = (
        m_json_contract.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, separators=None) + "\n"
    ).encode("utf-8")
    temp_path = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(temp_path, flags, 0o600)
        try:
            os.set_inheritable(fd, False)
            _write_all(fd, encoded)
            os.fsync(fd)
        finally:
            os.close(fd)
        _set_owner_only(temp_path, 0o600)
        temp_path.replace(path)
        _set_owner_only(path, 0o600)
        _fsync_parent(path)
    finally:
        temp_path.unlink(missing_ok=True)


def _fsync_parent(path: Path) -> None:
    """Make an atomic identity replacement durable on local POSIX filesystems."""

    directory_flag = getattr(os, "O_DIRECTORY", 0)
    if directory_flag == 0:  # Windows has no directory fsync through os.open.
        return
    fd = os.open(path.parent, os.O_RDONLY | directory_flag)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _valid_instance_id(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and 16 <= len(value) <= 64 and value.isascii() and value.isalnum()


def _valid_generation(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return None
    return value


def _probe_endpoint(endpoint: dict[str, Any] | None, home: Path) -> bool:
    if endpoint is None:
        return False
    base_url = endpoint.get("base_url")
    if not isinstance(base_url, str) or not base_url:
        return False
    parsed = urlparse(base_url)
    try:
        port = parsed.port
    except ValueError:
        return False
    if (
        parsed.scheme not in {"http", "https"}
        or (parsed.hostname or "").casefold() not in _LOOPBACK_HOSTS
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        return False

    headers = {"Accept": "application/json"}
    token = endpoint.get("api_token")
    if isinstance(token, str) and token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(f"{base_url.rstrip('/')}/health", headers=headers, method="GET")
    try:
        with urlopen_verified(
            request,
            timeout=DAEMON_HOME_LOCK_PROBE_SECONDS,
            connect_timeout=DAEMON_HOME_LOCK_PROBE_SECONDS,
        ) as response:
            raw = response.read(DAEMON_HEALTH_MAX_BYTES + 1)
            if len(raw) > DAEMON_HEALTH_MAX_BYTES or not (200 <= int(response.status) < 300):
                return False
        health = json.loads(raw.decode("utf-8"))
    except (HTTPError, URLError, OSError, TimeoutError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return False
    if not isinstance(health, dict) or health.get("ok") is not True:
        return False
    database = health.get("db")
    database_path = database.get("path") if isinstance(database, dict) else None
    if not isinstance(database_path, str) or not database_path:
        return False
    return _canonical_path(Path(database_path)) == _canonical_path(home / "daemon.sqlite3")


def _already_running_error(
    endpoint: dict[str, Any] | None,
    owner: dict[str, Any],
    home: Path,
) -> DaemonHomeLockedError:
    base_url = endpoint.get("base_url") if endpoint is not None else None
    url = base_url if isinstance(base_url, str) and base_url else "<unknown>"
    return DaemonHomeLockedError(
        f"a daemon is already running for this home: {url}, pid {_owner_pid(owner)}. "
        f"Use a different --home to run another daemon instead of sharing {home}."
    )


def _owner_pid(owner: dict[str, Any]) -> str:
    pid = owner.get("pid")
    if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0:
        return str(pid)
    return "unknown"


def _canonical_path(path: Path) -> str:
    return os.path.normcase(str(path.expanduser().resolve(strict=False)))


def _home_hash(home: Path) -> str:
    return hashlib.sha256(_canonical_path(home).encode("utf-8")).hexdigest()


def _set_owner_only(path: Path, mode: int) -> None:
    path.chmod(mode)
    if os.name != "nt" and stat.S_IMODE(path.stat().st_mode) != mode:
        raise PermissionError(f"could not set owner-only permissions on {path}")


if os.name == "nt":  # pragma: no cover - exercised by the Windows CI job.
    import msvcrt

    class _MsvcrtLocking(Protocol):
        LK_NBLCK: int
        LK_UNLCK: int

        def locking(self, fd: int, mode: int, count: int) -> None: ...

    _msvcrt_locking = cast(_MsvcrtLocking, msvcrt)

    def _lock_fd(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            _msvcrt_locking.locking(fd, _msvcrt_locking.LK_NBLCK, 1)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise BlockingIOError(exc.errno, str(exc)) from exc
            raise

    def _unlock_fd(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        _msvcrt_locking.locking(fd, _msvcrt_locking.LK_UNLCK, 1)

else:
    import fcntl

    def _lock_fd(fd: int) -> None:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                raise BlockingIOError(exc.errno, str(exc)) from exc
            raise

    def _unlock_fd(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)
