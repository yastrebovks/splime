"""Shared fail-closed policy for public wheel archives and SPDX expressions."""

from __future__ import annotations

import io
import re
import stat
import struct
import zipfile
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Final


SPDX_ALLOWLIST: Final = (
    "0BSD",
    "Apache-2.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "MIT",
    "PSF-2.0",
    "Python-2.0",
    "Unlicense",
    "Zlib",
)
MAX_WHEEL_MEMBERS: Final = 4096
MAX_WHEEL_MEMBER_BYTES: Final = 64 * 1024 * 1024
MAX_WHEEL_UNCOMPRESSED_BYTES: Final = 256 * 1024 * 1024
MAX_WHEEL_COMPRESSION_RATIO: Final = 100
SUPPORTED_WHEEL_COMPRESSION: Final = frozenset({zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED})
_ALLOWED_EXTRA_FIELD_IDS: Final = frozenset({0x5455})


class PublicArtifactPolicyError(ValueError):
    """The archive or license expression violates the public policy."""


def spdx_allowed(expression: str) -> bool:
    """Accept only a balanced boolean expression over the exact allowlist."""

    if not isinstance(expression, str) or not expression:
        return False
    tokens = re.findall(r"[A-Za-z0-9.-]+|[()]", expression)
    if not tokens or "".join(tokens) != re.sub(r"\s+", "", expression):
        return False
    position = 0

    def primary() -> bool:
        nonlocal position
        if position >= len(tokens):
            return False
        token = tokens[position]
        if token == "(":
            position += 1
            if not expression_node():
                return False
            if position >= len(tokens) or tokens[position] != ")":
                return False
            position += 1
            return True
        if token in {"AND", "OR", ")"} or token not in SPDX_ALLOWLIST:
            return False
        position += 1
        return True

    def expression_node() -> bool:
        nonlocal position
        if not primary():
            return False
        while position < len(tokens) and tokens[position] in {"AND", "OR"}:
            position += 1
            if not primary():
                return False
        return True

    return (
        "LicenseRef" not in expression
        and "DocumentRef" not in expression
        and "WITH" not in tokens
        and expression_node()
        and position == len(tokens)
    )


def _safe_member_name(name: str) -> str:
    if not name or "\x00" in name or "\\" in name or name.startswith("/") or re.match(r"^[A-Za-z]:", name) is not None:
        raise PublicArtifactPolicyError("wheel contains an unsafe archive path")
    path = PurePosixPath(name)
    if str(path) != name or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise PublicArtifactPolicyError("wheel contains an unsafe archive path")
    return name


def _validate_extra_fields(info: zipfile.ZipInfo) -> None:
    offset = 0
    extra = info.extra
    while offset < len(extra):
        if len(extra) - offset < 4:
            raise PublicArtifactPolicyError("wheel contains malformed archive metadata")
        field_id, size = struct.unpack_from("<HH", extra, offset)
        offset += 4
        if len(extra) - offset < size:
            raise PublicArtifactPolicyError("wheel contains malformed archive metadata")
        if field_id not in _ALLOWED_EXTRA_FIELD_IDS:
            raise PublicArtifactPolicyError("wheel contains unsupported link or archive metadata")
        offset += size


def _validate_regular(info: zipfile.ZipInfo) -> None:
    if info.is_dir() or info.filename.endswith("/"):
        raise PublicArtifactPolicyError("wheel contains a non-regular member")
    if info.create_system == 3:
        mode = info.external_attr >> 16
        if not stat.S_ISREG(mode) or mode & (stat.S_ISUID | stat.S_ISGID | stat.S_ISVTX):
            raise PublicArtifactPolicyError("wheel contains a symlink, hard-link marker, device, or non-regular member")
    elif info.create_system == 0:
        if info.external_attr & 0x10:
            raise PublicArtifactPolicyError("wheel contains a non-regular member")
    else:
        raise PublicArtifactPolicyError("wheel member platform metadata is unsupported")
    _validate_extra_fields(info)


def inspect_wheel_archive(payload: bytes) -> Mapping[str, bytes]:
    """Validate and read a bounded wheel archive without trusting headers alone."""

    if not isinstance(payload, bytes):
        raise PublicArtifactPolicyError("wheel payload must be bytes")
    members: dict[str, bytes] = {}
    folded: set[str] = set()
    total = 0
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            infos = archive.infolist()
            if not infos or len(infos) > MAX_WHEEL_MEMBERS:
                raise PublicArtifactPolicyError("wheel member count exceeds the safety limit")
            for info in infos:
                name = _safe_member_name(info.filename)
                folded_name = name.casefold()
                if name in members or folded_name in folded:
                    raise PublicArtifactPolicyError("wheel contains duplicate or case-colliding archive paths")
                folded.add(folded_name)
                _validate_regular(info)
                if info.compress_type not in SUPPORTED_WHEEL_COMPRESSION:
                    raise PublicArtifactPolicyError("wheel uses unsupported compression")
                if info.file_size < 0 or info.file_size > MAX_WHEEL_MEMBER_BYTES:
                    raise PublicArtifactPolicyError("wheel member exceeds the uncompressed size limit")
                if info.compress_size == 0 and info.file_size:
                    raise PublicArtifactPolicyError("wheel member has an invalid compression ratio")
                if info.compress_size and info.file_size > info.compress_size * MAX_WHEEL_COMPRESSION_RATIO:
                    raise PublicArtifactPolicyError("wheel member exceeds the compression-ratio limit")
                total += info.file_size
                if total > MAX_WHEEL_UNCOMPRESSED_BYTES:
                    raise PublicArtifactPolicyError("wheel total uncompressed size exceeds the safety limit")
                chunks: list[bytes] = []
                observed = 0
                with archive.open(info, "r") as source:
                    while True:
                        chunk = source.read(min(1024 * 1024, MAX_WHEEL_MEMBER_BYTES + 1 - observed))
                        if not chunk:
                            break
                        observed += len(chunk)
                        if observed > MAX_WHEEL_MEMBER_BYTES or observed > info.file_size:
                            raise PublicArtifactPolicyError("wheel member decompressed beyond its declared size")
                        chunks.append(chunk)
                data = b"".join(chunks)
                if observed != info.file_size:
                    raise PublicArtifactPolicyError("wheel member size disagrees after decompression")
                members[name] = data
    except (OSError, RuntimeError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        raise PublicArtifactPolicyError("wheel is not a valid bounded ZIP archive") from exc
    return members


__all__ = [
    "MAX_WHEEL_COMPRESSION_RATIO",
    "MAX_WHEEL_MEMBER_BYTES",
    "MAX_WHEEL_MEMBERS",
    "MAX_WHEEL_UNCOMPRESSED_BYTES",
    "PublicArtifactPolicyError",
    "SPDX_ALLOWLIST",
    "SUPPORTED_WHEEL_COMPRESSION",
    "inspect_wheel_archive",
    "spdx_allowed",
]
