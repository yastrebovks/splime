"""Build reproducible SPLime wheel and source distribution artifacts."""

from __future__ import annotations

import argparse
import gzip
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


_REGULAR_FILE_MODE = 0o644
_DIRECTORY_MODE = 0o755
_SYMLINK_MODE = 0o777
_HARDLINK_MODE = 0o644
_UNSUPPORTED_SPECIAL_MEMBER_TYPES = frozenset(
    {
        tarfile.CHRTYPE,
        tarfile.BLKTYPE,
        tarfile.FIFOTYPE,
        tarfile.CONTTYPE,
        tarfile.GNUTYPE_SPARSE,
    }
)


def _canonical_member_mode(member: tarfile.TarInfo) -> int:
    """Return a canonical mode for a supported sdist member."""

    if member.type in (tarfile.REGTYPE, tarfile.AREGTYPE):
        return _REGULAR_FILE_MODE
    if member.type == tarfile.DIRTYPE:
        return _DIRECTORY_MODE
    if member.type == tarfile.SYMTYPE:
        return _SYMLINK_MODE
    if member.type == tarfile.LNKTYPE:
        return _HARDLINK_MODE
    if member.type in _UNSUPPORTED_SPECIAL_MEMBER_TYPES:
        raise ValueError(f"sdist contains unsupported special member {member.name!r} of type {member.type!r}")
    raise ValueError(f"sdist contains unsupported tar member {member.name!r} of type {member.type!r}")


def main() -> int:
    """Build artifacts and normalize the sdist archive metadata."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=Path("dist"))
    parser.add_argument(
        "--source-date-epoch",
        type=int,
        default=int(os.environ.get("SOURCE_DATE_EPOCH", "0")),
    )
    args = parser.parse_args()
    if args.source_date_epoch <= 0:
        parser.error("SOURCE_DATE_EPOCH or --source-date-epoch must be a positive integer")

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="splime-release-build-") as temporary:
        raw_dir = Path(temporary) / "raw"
        raw_dir.mkdir()
        build_environment = os.environ.copy()
        build_environment["SOURCE_DATE_EPOCH"] = str(args.source_date_epoch)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--sdist",
                "--wheel",
                "--outdir",
                str(raw_dir),
            ],
            check=True,
            env=build_environment,
        )
        artifacts = sorted(raw_dir.iterdir())
        wheels = [path for path in artifacts if path.suffix == ".whl"]
        sdists = [path for path in artifacts if path.name.endswith(".tar.gz")]
        if len(wheels) != 1 or len(sdists) != 1:
            raise RuntimeError(f"expected one wheel and one sdist, found {artifacts!r}")
        shutil.copyfile(wheels[0], out_dir / wheels[0].name)
        normalize_sdist(
            sdists[0],
            out_dir / sdists[0].name,
            source_date_epoch=args.source_date_epoch,
        )
    return 0


def normalize_sdist(source: Path, destination: Path, *, source_date_epoch: int) -> None:
    """Rewrite an sdist with stable ordering, modes, ownership, and timestamps."""

    with tempfile.TemporaryDirectory(prefix="splime-sdist-normalize-") as temporary:
        tar_path = Path(temporary) / "release.tar"
        with tarfile.open(source, mode="r:gz") as source_tar:
            with tarfile.open(tar_path, mode="w", format=tarfile.PAX_FORMAT) as target_tar:
                for member in sorted(source_tar.getmembers(), key=lambda item: item.name):
                    member.mode = _canonical_member_mode(member)
                    member.uid = 0
                    member.gid = 0
                    member.uname = ""
                    member.gname = ""
                    member.mtime = source_date_epoch
                    member.pax_headers = {}
                    payload = (
                        source_tar.extractfile(member) if member.type in (tarfile.REGTYPE, tarfile.AREGTYPE) else None
                    )
                    target_tar.addfile(member, payload)
        with tar_path.open("rb") as source_file, destination.open("wb") as destination_file:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=destination_file,
                mtime=source_date_epoch,
            ) as compressed:
                shutil.copyfileobj(source_file, compressed)


if __name__ == "__main__":
    raise SystemExit(main())
