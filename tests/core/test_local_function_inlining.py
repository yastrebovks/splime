"""Local (first-party) functions are inlined recursively across files.

These tests pin the behaviour added in ``spl.core.entities.local_function``:

* a wrapped function that imports helpers from the user's own local modules has
  those helpers inlined into the serialized output -- recursively, following the
  import graph across several files (``top`` -> ``mid`` -> ``leaf``);
* aliased local imports (``from leaf import leaf as lf``) are rebound so the
  reconstructed code still runs;
* no ``from local_module import ...`` statement leaks into the output, and the
  artifact round-trips and executes after the local package is removed;
* third-party (pip) objects are still referenced through imports + distribution
  metadata, never inlined.
"""

from __future__ import annotations

import importlib
import sys
import textwrap
from pathlib import Path

from spl.core.ir.utils import spl_export_to_file, spl_import_from_file


def _write(path: Path, body: str) -> None:
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")


def _make_package(root: Path, name: str) -> Path:
    package = root / name
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    return package


def _forget_package(name: str) -> None:
    for module in list(sys.modules):
        if module == name or module.startswith(name + "."):
            del sys.modules[module]


def test_inlines_local_functions_across_deep_files(tmp_path: Path) -> None:
    src = tmp_path / "src"
    package = _make_package(src, "proj_inline")
    _write(package / "leaf.py", """
        def leaf(value):
            return value * 2
    """)
    _write(package / "mid.py", """
        from proj_inline.leaf import leaf as lf

        def mid(value):
            return lf(value) + 1
    """)
    _write(package / "top.py", """
        from proj_inline.mid import mid

        def top(value):
            return mid(value) + 10
    """)

    out = tmp_path / "top.spl.yaml"
    sys.path.insert(0, str(src))
    try:
        top = importlib.import_module("proj_inline.top").top
        assert top(5) == 21  # sanity: (((5 * 2) + 1) + 10)

        spl_export_to_file(out, [top])
    finally:
        sys.path.remove(str(src))
        _forget_package("proj_inline")

    text = out.read_text(encoding="utf-8")

    # Every local helper is inlined as a DFunction, across all three files.
    assert "!DFunction" in text
    assert "name: top" in text
    assert "name: mid" in text
    assert "name: leaf" in text

    # The aliased local import (`leaf as lf`) is rebound, not imported.
    assert "!DLocalAlias" in text

    # No local import leaks into the "include" section, and the local package
    # name appears nowhere in the artifact.
    assert "!DImportFrom" not in text
    assert "!DImport" not in text
    assert "proj_inline" not in text

    # The artifact reconstructs and runs with the local package gone entirely.
    assert "proj_inline" not in sys.modules
    namespace: dict = {}
    spl_import_from_file(out, globals=namespace)
    assert namespace["leaf"](5) == 10
    assert namespace["mid"](5) == 11
    assert namespace["top"](5) == 21


def test_unpublished_dev_package_is_gutted_not_referenced(tmp_path: Path) -> None:
    """A package under development is inlined like loose files, even when it is
    a registered distribution (an editable ``pip install -e .``).

    Locality is decided by where the source lives, not by distribution
    metadata, so an unpublished package that happens to be installed editable is
    still gutted -- referencing it as a pip dependency would yield an artifact
    that cannot be rebuilt elsewhere.
    """
    import importlib.metadata as importlib_metadata

    src = tmp_path / "src"
    package = _make_package(src, "devpkg")
    _write(package / "helpers.py", """
        def boost(v):
            return v * 2
    """)
    _write(package / "core.py", """
        from devpkg.helpers import boost

        def reticulate(value):
            return boost(value) + 100
    """)

    # Simulate an editable install: a .dist-info on the path makes the package a
    # registered distribution even though its source stays in the working tree.
    dist_info = src / "devpkg-9.9.9.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: devpkg\nVersion: 9.9.9\n", encoding="utf-8")
    (dist_info / "top_level.txt").write_text("devpkg\n", encoding="utf-8")
    (dist_info / "RECORD").write_text("", encoding="utf-8")

    out = tmp_path / "dev.spl.yaml"
    sys.path.insert(0, str(src))
    importlib.invalidate_caches()
    try:
        # Precondition: it really is seen as an installed distribution.
        assert "devpkg" in importlib_metadata.packages_distributions()
        reticulate = importlib.import_module("devpkg.core").reticulate
        spl_export_to_file(out, [reticulate])
    finally:
        sys.path.remove(str(src))
        _forget_package("devpkg")

    text = out.read_text(encoding="utf-8")
    # Both the entry and its sibling-file dependency are inlined ...
    assert "name: reticulate" in text
    assert "name: boost" in text
    # ... and nothing is referenced as an import or pinned as a distribution.
    assert "!DImportFrom" not in text
    assert "!DDistribution" not in text
    assert "devpkg" not in text

    # Rebuilds and runs with the dev package gone.
    namespace: dict = {}
    spl_import_from_file(out, globals=namespace)
    assert namespace["reticulate"](5) == 110


def test_third_party_functions_are_imported_not_inlined(tmp_path: Path) -> None:
    src = tmp_path / "src"
    package = _make_package(src, "proj_pip")
    # `yaml` (PyYAML) is a real installed distribution and `safe_load` is a
    # plain Python function -- exactly the case we must NOT inline.
    _write(package / "use.py", """
        from yaml import safe_load

        def use(text):
            return safe_load(text)
    """)

    out = tmp_path / "use.spl.yaml"
    sys.path.insert(0, str(src))
    try:
        use = importlib.import_module("proj_pip.use").use
        spl_export_to_file(out, [use])
    finally:
        sys.path.remove(str(src))
        _forget_package("proj_pip")

    text = out.read_text(encoding="utf-8")

    # The local wrapper is inlined ...
    assert "name: use" in text
    # ... but the pip function is referenced by import + distribution metadata.
    assert "!DImportFrom" in text
    assert "safe_load" in text
    assert "!DDistribution" in text
