import ast
import hashlib
from pathlib import Path
from typing import Any, cast

import yaml

from spl.core.entities.artifact import ArtifactRef, DArtifactRef, compute_sha256
from spl.core.ir.parse import ir_parse
from spl.core.ir.unparse import ir_unparse
from spl.core.ir.utils import SPLSafeLoader


def _reconstruct_artifact_ref(x: DArtifactRef) -> ArtifactRef:
    module = ast.fix_missing_locations(ast.Module(
        body = list(ir_unparse(x, source = Path('artifact.yaml'))),
        type_ignores = []))
    namespace: dict[str, Any] = {'ArtifactRef': ArtifactRef}

    exec(compile(module, 'artifact.yaml', mode = 'exec'), namespace)  # noqa: S102

    return cast(ArtifactRef, namespace['_link_to'])


def test_artifact_ref_yaml_ir_round_trip(tmp_path: Path) -> None:
    path = tmp_path / 'value.bin'
    path.write_bytes(b'artifact contents')
    ref = ArtifactRef(
        key = 'value',
        uri = str(path),
        sha256 = compute_sha256(path),
        size = path.stat().st_size)
    root = ir_parse(ref).mk_root()

    dumped = yaml.dump(root, sort_keys = False)
    loaded = cast(DArtifactRef, yaml.load(dumped, Loader = SPLSafeLoader))
    reconstructed = _reconstruct_artifact_ref(loaded)

    assert root == DArtifactRef(
        key = ref.key,
        uri = ref.uri,
        sha256 = ref.sha256,
        size = ref.size)
    assert reconstructed == ref


def test_compute_sha256_is_stable_for_multi_chunk_file(tmp_path: Path) -> None:
    path = tmp_path / 'large.bin'
    blocks = [
        b'a' * (1024 * 1024),
        b'b' * (1024 * 1024),
        b'tail']
    expected = hashlib.sha256()

    with path.open('wb') as f:
        for block in blocks:
            f.write(block)
            expected.update(block)

    assert compute_sha256(path) == expected.hexdigest()
    assert compute_sha256(path) == expected.hexdigest()
