"""Portable CI runner: execute native-profile and sync/catalogue tests on the wheel.

No private server repository is required. Test code and fixtures are copied
outside the checkout; the child interpreter is isolated and has no pytest
pythonpath setting. The broader source suite remains a separate CI step.
"""

from pathlib import Path
import os
import shutil
import subprocess
import sys
import tempfile


def main():
    source = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="splime-installed-contract-") as directory:
        root = Path(directory).resolve()
        tests = root / "tests"
        (tests / "core").mkdir(parents=True)
        (tests / "fixtures").mkdir()
        (tests / "daemon").mkdir()
        for name in (
            "test_public_current_process.py",
            "test_unified_result_access.py",
            "test_public_registry_transport.py",
        ):
            shutil.copyfile(source / "tests/core" / name, tests / "core" / name)
        for relative in (
            "__init__.py",
            "conftest.py",
            "daemon/__init__.py",
            "daemon/conftest.py",
            "daemon/test_sync_response_integrity.py",
            "daemon/test_server_channel_recovery.py",
            "daemon/test_catalogue_sync_v1.py",
            "daemon/test_daemon_endpoint.py",
        ):
            shutil.copyfile(source / "tests" / relative, tests / relative)
        shutil.copyfile(
            source / "tests/fixtures/legacy_public_verifier_0_4_10.txt",
            tests / "fixtures/legacy_public_verifier_0_4_10.txt",
        )
        (root / "pytest.ini").write_text("[pytest]\nasyncio_mode = strict\n")
        (root / "test_installed_identity.py").write_text(
            "import spl\nfrom importlib.metadata import version\n"
            "def test_installed_wheel():\n"
            "    assert 'site-packages' in str(spl.__file__)\n"
            "    assert version('splime')\n"
        )
        env = {key: value for key, value in os.environ.items() if not key.startswith(("SPL", "PYTHON"))}
        env.update(
            SPL_DAEMON_HOME=str(root / "daemon-home"),
            SPL_RUNS_HOME=str(root / "runs"),
            SPL_DAEMON_SECRET_BACKEND="file",
        )
        # Helpers remain importable; their broader suites have a separate source
        # gate. Collect the portable sync and catalogue suites on installed code.
        subprocess.run(
            [
                sys.executable,
                "-I",
                "-m",
                "pytest",
                "-q",
                str(root / "test_installed_identity.py"),
                str(tests / "core"),
                str(tests / "daemon/test_sync_response_integrity.py"),
                str(tests / "daemon/test_catalogue_sync_v1.py"),
            ],
            cwd=root,
            env=env,
            check=True,
        )


if __name__ == "__main__":
    main()
