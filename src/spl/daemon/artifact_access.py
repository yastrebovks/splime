"""Bounded, no-follow access to daemon-produced local artifacts."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self

LOCAL_ARTIFACT_SCAN_MAX_ENTRIES = 1_000


@dataclass(frozen=True)
class ArtifactFileRead:
    """A bounded file read and the source file's observed byte size."""

    data: bytes
    size: int


@dataclass(frozen=True)
class ArtifactFileCount:
    """A lower-bound regular-file count from one bounded directory scan."""

    count: int
    truncated: bool
    available: bool


def directory_fd_supported() -> bool:
    """Return whether secure handle-relative directory operations are available."""

    return (
        bool(getattr(os, "O_NOFOLLOW", 0))
        and os.scandir in os.supports_fd
        and os.open in os.supports_dir_fd
        and os.stat in os.supports_dir_fd
    )


def _is_reparse_point(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0))
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _is_safe_directory(value: os.stat_result) -> bool:
    return stat.S_ISDIR(value.st_mode) and not _is_reparse_point(value)


def _is_safe_regular_file(value: os.stat_result) -> bool:
    return stat.S_ISREG(value.st_mode) and not _is_reparse_point(value)


def _same_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _read_fd_bounded(file_fd: int, max_bytes: int) -> bytes:
    remaining = max_bytes + 1
    chunks: list[bytes] = []
    while remaining > 0:
        chunk = os.read(file_fd, min(64 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class ArtifactDirectory:
    """An artifact directory pinned by FD where possible, or identity-checked."""

    def __init__(self, path: Path, identity: os.stat_result, directory_fd: int | None) -> None:
        self.path = path
        self._identity = identity
        self.directory_fd = directory_fd

    @classmethod
    def open(cls, path: Path) -> Self | None:
        """Open a real non-reparse directory without following its final component."""

        try:
            before = path.lstat()
        except OSError:
            return None
        if not _is_safe_directory(before):
            return None
        if directory_fd_supported():
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            directory_fd: int | None = None
            try:
                directory_fd = os.open(path, flags)
                opened = os.fstat(directory_fd)
                if not _is_safe_directory(opened) or not _same_identity(before, opened):
                    os.close(directory_fd)
                    return None
                return cls(path, opened, directory_fd)
            except (OSError, TypeError, NotImplementedError):
                if directory_fd is not None:
                    os.close(directory_fd)
                # A platform that advertises the secure descriptor-relative
                # path must fail closed if opening it fails. The path-based
                # fallback exists only for platforms (notably Windows) where
                # Python does not expose those primitives.
                return None
        # Windows does not expose Python's dir_fd APIs. The fallback validates
        # the root before every operation and pins each opened file by identity.
        try:
            current = path.lstat()
        except OSError:
            return None
        if not _is_safe_directory(current) or not _same_identity(before, current):
            return None
        return cls(path, current, None)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the pinned directory handle, if this platform supplied one."""

        if self.directory_fd is not None:
            os.close(self.directory_fd)
            self.directory_fd = None

    def verify_root(self) -> bool:
        """Verify that the pinned/fallback root still denotes the opened directory."""

        try:
            if self.directory_fd is not None:
                current = os.fstat(self.directory_fd)
            else:
                current = self.path.lstat()
        except OSError:
            return False
        return _is_safe_directory(current) and _same_identity(self._identity, current)

    def iter_entries(self) -> Iterator[os.DirEntry[str]]:
        """Iterate directory entries without resolving their targets."""

        if not self.verify_root():
            raise OSError("artifact directory changed before scanning")
        target: int | Path = self.directory_fd if self.directory_fd is not None else self.path
        with os.scandir(target) as entries:
            yield from entries

    @staticmethod
    def entry_is_regular(entry: os.DirEntry[str]) -> bool:
        """Return whether an entry itself is a regular, non-reparse file."""

        try:
            return _is_safe_regular_file(entry.stat(follow_symlinks=False))
        except OSError:
            return False

    def read_regular(self, name: str, max_bytes: int) -> ArtifactFileRead | None:
        """Read at most ``max_bytes + 1`` bytes from an identity-pinned file."""

        if max_bytes < 0:
            raise ValueError("max_bytes must be nonnegative")
        if not name or Path(name).name != name or name in {".", ".."}:
            return None
        file_fd: int | None = None
        try:
            if not self.verify_root():
                return None
            before = self._stat_name(name)
            if before is None or not _is_safe_regular_file(before):
                return None
            if not self.verify_root():
                return None
            flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            if self.directory_fd is not None:
                file_fd = os.open(name, flags, dir_fd=self.directory_fd)
            else:
                file_fd = os.open(self.path / name, flags)
            opened = os.fstat(file_fd)
            if not _is_safe_regular_file(opened) or not _same_identity(before, opened):
                return None
            if not self.verify_root():
                return None
            data = _read_fd_bounded(file_fd, max_bytes)
            opened_after = os.fstat(file_fd)
            path_after = self._stat_name(name)
            if (
                not _is_safe_regular_file(opened_after)
                or not _same_identity(opened, opened_after)
                or path_after is None
                or not _is_safe_regular_file(path_after)
                or not _same_identity(opened, path_after)
                or not self.verify_root()
            ):
                return None
            return ArtifactFileRead(data=data, size=max(opened.st_size, len(data)))
        except (OSError, TypeError, NotImplementedError):
            return None
        finally:
            if file_fd is not None:
                os.close(file_fd)

    def _stat_name(self, name: str) -> os.stat_result | None:
        try:
            if self.directory_fd is not None:
                return os.stat(name, dir_fd=self.directory_fd, follow_symlinks=False)
            return (self.path / name).lstat()
        except (OSError, TypeError, NotImplementedError):
            return None


def count_regular_files_bounded(
    path: Path,
    *,
    max_entries: int = LOCAL_ARTIFACT_SCAN_MAX_ENTRIES,
) -> ArtifactFileCount:
    """Count regular files while consuming at most one sentinel past the cap."""

    if max_entries < 0:
        raise ValueError("max_entries must be nonnegative")
    directory = ArtifactDirectory.open(path)
    if directory is None:
        return ArtifactFileCount(count=0, truncated=False, available=False)
    with directory:
        count = 0
        truncated = False
        try:
            for index, entry in enumerate(directory.iter_entries()):
                if index >= max_entries:
                    truncated = True
                    break
                if directory.entry_is_regular(entry):
                    count += 1
        except (OSError, TypeError, NotImplementedError):
            return ArtifactFileCount(count=0, truncated=False, available=False)
        if not directory.verify_root():
            return ArtifactFileCount(count=0, truncated=False, available=False)
        return ArtifactFileCount(count=count, truncated=truncated, available=True)
