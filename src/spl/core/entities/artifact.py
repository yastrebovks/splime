import ast
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator

import yaml

from spl.core.ir.common import DBase
from spl.core.ir.parse import _branch, ir_parse
from spl.core.ir.unparse import ir_unparse

_HASH_CHUNK_SIZE = 1024 * 1024
_SHA256_HEX_DIGITS = set("0123456789abcdefABCDEF")


def _validate_non_empty_string(name: str, value: str) -> None:
    if not isinstance(value, str):
        raise TypeError("artifact ref {} must be a string".format(name))
    if not value:
        raise ValueError("artifact ref {} must be a non-empty string".format(name))


def _validate_sha256(value: str) -> None:
    _validate_non_empty_string("sha256", value)
    if len(value) != 64 or any(c not in _SHA256_HEX_DIGITS for c in value):
        raise ValueError("artifact ref sha256 must be a 64-character hex string")


def _validate_size(value: int) -> None:
    if type(value) is not int:
        raise TypeError("artifact ref size must be an integer")
    if value < 0:
        raise ValueError("artifact ref size must be non-negative")


def _validate_artifact_ref(key: str, uri: str, sha256: str, size: int) -> None:
    _validate_non_empty_string("key", key)
    _validate_non_empty_string("uri", uri)
    _validate_sha256(sha256)
    _validate_size(size)


@dataclass(frozen=True)
class ArtifactRef:
    """Reference to a materialized artifact on the local filesystem."""

    key: str
    uri: str
    sha256: str
    size: int

    def __post_init__(self) -> None:
        _validate_artifact_ref(key=self.key, uri=self.uri, sha256=self.sha256, size=self.size)


@dataclass(frozen=True)
class DArtifactRef(DBase):
    """Serialized ArtifactRef value for pipeline YAML."""

    key: str
    uri: str
    sha256: str
    size: int

    def __post_init__(self) -> None:
        _validate_artifact_ref(key=self.key, uri=self.uri, sha256=self.sha256, size=self.size)


yaml.add_representer(DArtifactRef, lambda dumper, data: dumper.represent_mapping("!DArtifactRef", data.__dict__))


def _construct_dartifact_ref(loader: Any, node: Any) -> DArtifactRef:
    return DArtifactRef(**loader.construct_mapping(node))


yaml.add_constructor("!DArtifactRef", _construct_dartifact_ref)


def compute_sha256(path: Path) -> str:
    """Compute a file's SHA-256 digest with chunked reads."""

    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(_HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mk_empty_dependencies(frame_offset: int) -> Generator[DBase]:
    yield from ()


@ir_parse.register(lambda x: isinstance(x, ArtifactRef))
def _ir_parse__artifact_ref(x: ArtifactRef, name: str | None = None) -> _branch:
    return _branch(x, lambda: DArtifactRef(key=x.key, uri=x.uri, sha256=x.sha256, size=x.size), _mk_empty_dependencies)


@ir_unparse.register(lambda x: isinstance(x, DArtifactRef))
def _ir_unparse__artifact_ref(x: DArtifactRef, source: Path) -> Generator[ast.stmt]:
    yield ast.Assign(
        targets=[ast.Name(id="_link_to", ctx=ast.Store())],
        value=ast.Call(
            func=ast.Name(id="ArtifactRef", ctx=ast.Load()),
            keywords=[
                ast.keyword(arg="key", value=ast.Constant(value=x.key)),
                ast.keyword(arg="uri", value=ast.Constant(value=x.uri)),
                ast.keyword(arg="sha256", value=ast.Constant(value=x.sha256)),
                ast.keyword(arg="size", value=ast.Constant(value=x.size)),
            ],
        ),
    )
