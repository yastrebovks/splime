from __future__ import annotations

import importlib.util
import io
import stat
import struct
import zipfile
from pathlib import Path

import pytest

from spl.runtime_environment import spdx_allowed as consumer_spdx_allowed
import spl.public_artifact_policy as consumer_policy


FRAMEWORK_ROOT = Path(__file__).parents[2]
WORKSPACE = next(
    (
        root
        for root in (Path(__file__).parents[3], Path(__file__).parents[4])
        if (root / "spl-server" / "src" / "daemon_server").is_dir()
    ),
    Path(__file__).parents[3],
)


def _producer_module():
    path = WORKSPACE / "spl-server" / "src" / "daemon_server" / "public_artifact_policy.py"
    spec = importlib.util.spec_from_file_location("producer_public_artifact_policy", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "expression",
    [
        "MIT",
        "MIT OR Apache-2.0",
        "(MIT AND BSD-3-Clause)",
        "((MIT) OR (Apache-2.0 AND BSD-3-Clause))",
        "(MIT",
        "MIT)",
        "()",
        "MIT AND",
        "AND MIT",
        "MIT OR OR Apache-2.0",
        "MIT Apache-2.0",
        "Apache-2.0 WITH LLVM-exception",
        "LicenseRef-Private",
    ],
)
def test_producer_and_consumer_use_identical_spdx_grammar(expression: str) -> None:
    producer = _producer_module()
    assert producer.spdx_allowed(expression) is consumer_spdx_allowed(expression)


def test_shared_policy_sources_are_byte_identical() -> None:
    producer = WORKSPACE / "spl-server" / "src" / "daemon_server" / "public_artifact_policy.py"
    consumer = FRAMEWORK_ROOT / "src" / "spl" / "public_artifact_policy.py"
    assert producer.read_bytes() == consumer.read_bytes()


def _archive(
    members: list[tuple[str, bytes]],
    *,
    mode: int = stat.S_IFREG | 0o644,
    compression: int = zipfile.ZIP_STORED,
    extra: bytes = b"",
) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=compression) as archive:
        for name, payload in members:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = mode << 16
            info.compress_type = compression
            info.extra = extra
            archive.writestr(info, payload)
    return stream.getvalue()


@pytest.mark.parametrize(
    "case",
    [
        "symlink",
        "hardlink-metadata",
        "device",
        "fifo",
        "traversal",
        "windows-absolute",
        "duplicate",
        "casefold-collision",
        "member-count",
        "member-size",
        "total-size",
        "unsupported-compression",
        "decompression-bomb",
    ],
)
def test_producer_and_consumer_reject_the_same_adversarial_archives(case: str, monkeypatch: pytest.MonkeyPatch) -> None:
    producer = _producer_module()
    modules = (producer, consumer_policy)
    if case == "symlink":
        payload = _archive([("link", b"target")], mode=stat.S_IFLNK | 0o777)
    elif case == "hardlink-metadata":
        # Info-ZIP Unix metadata can encode link semantics; only the harmless
        # extended-timestamp field is accepted by the shared policy.
        payload = _archive(
            [("hardlink", b"target")],
            extra=struct.pack("<HH", 0x000D, 0),
        )
    elif case == "device":
        payload = _archive([("device", b"x")], mode=stat.S_IFCHR | 0o600)
    elif case == "fifo":
        payload = _archive([("fifo", b"x")], mode=stat.S_IFIFO | 0o600)
    elif case == "traversal":
        payload = _archive([("../escape.py", b"x")])
    elif case == "windows-absolute":
        payload = _archive([("C:/escape.py", b"x")])
    elif case == "duplicate":
        payload = _archive([("same.py", b"a"), ("same.py", b"b")])
    elif case == "casefold-collision":
        payload = _archive([("same.py", b"a"), ("SAME.py", b"b")])
    elif case == "member-count":
        for module in modules:
            monkeypatch.setattr(module, "MAX_WHEEL_MEMBERS", 2)
        payload = _archive([("a", b"a"), ("b", b"b"), ("c", b"c")])
    elif case == "member-size":
        for module in modules:
            monkeypatch.setattr(module, "MAX_WHEEL_MEMBER_BYTES", 4)
        payload = _archive([("large", b"12345")])
    elif case == "total-size":
        for module in modules:
            monkeypatch.setattr(module, "MAX_WHEEL_UNCOMPRESSED_BYTES", 8)
        payload = _archive([("a", b"12345"), ("b", b"12345")])
    elif case == "unsupported-compression":
        payload = _archive([("compressed", b"payload")], compression=zipfile.ZIP_BZIP2)
    else:
        for module in modules:
            monkeypatch.setattr(module, "MAX_WHEEL_COMPRESSION_RATIO", 2)
        payload = _archive(
            [("bomb", b"0" * 4096)],
            compression=zipfile.ZIP_DEFLATED,
        )

    for module in modules:
        with pytest.raises(module.PublicArtifactPolicyError):
            module.inspect_wheel_archive(payload)
