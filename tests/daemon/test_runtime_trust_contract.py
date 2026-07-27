from __future__ import annotations

import re
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

LOCKED_RUNTIME_CLAIMS = {
    Path("README.md"): (
        "native and venv-subprocess runtimes execute trusted code under the "
        "conductor's os identity—the daemon user for daemon-managed runs",
        "docker or a deliberately separate os identity is required when code must not read same-uid daemon files",
        "scoped callback capabilities limit the authority intentionally passed over the worker protocol",
        "do not protect against arbitrary same-uid file reads",
    ),
    Path("docs/source/cookbook.rst"): (
        "native and venv-subprocess runtimes execute trusted code under the "
        "conductor's os identity (the daemon user for daemon-managed runs)",
        "docker or a deliberately separate os identity is required when code must not read same-uid daemon files",
        "scoped callback capabilities limit the authority intentionally passed over the worker protocol",
        "do not protect against arbitrary same-uid file reads",
    ),
    Path("ROADMAP.md"): (
        "native and venv-subprocess execute trusted code under the conductor's "
        "os identity (the daemon user for daemon-managed runs)",
        "dependency/process separation is not an os sandbox",
        "docker (with --network none by default) or a separate os identity "
        "provides the boundary for code that must not read same-uid daemon files",
        "subject to configured mounts and docker-host trust",
    ),
    Path("docs/source/daemon-security-telemetry.rst"): (
        "native and venv-subprocess runtimes execute trusted code under the daemon user's operating-system identity",
        "neither is an os security boundary",
        "use the docker worker boundary or a separately permissioned os account",
        "the callback capability limits the normal worker protocol",
        "prevent arbitrary same-uid file reads",
    ),
    Path("CHANGELOG.md"): (
        "native and venv-subprocess runtimes execute trusted code under the "
        "daemon's os user and filesystem permissions",
        "the scoped capability limits the normal worker protocol",
        "it does not prevent arbitrary same-uid file reads",
        "use docker, subject to reviewed mounts and docker-host trust, or os-account isolation",
    ),
}

REMOVED_ISOLATION_CLAIMS = (
    "daemon builds an isolated environment on the worker before executing",
    "isolated environments built by the daemon before a run",
    "isolated execution",
    "choose the smallest runtime that gives the isolation you need",
)


def _plain_text(relative_path: Path) -> str:
    content = (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")
    without_markup = content.replace("`", "").replace("*", "")
    return re.sub(r"\s+", " ", without_markup).lower()


def test_runtime_documentation_states_the_locked_trust_contract() -> None:
    for relative_path, required_claims in LOCKED_RUNTIME_CLAIMS.items():
        content = _plain_text(relative_path)
        missing = [claim for claim in required_claims if claim not in content]
        assert not missing, f"{relative_path} is missing trust claims: {missing}"


def test_runtime_documentation_does_not_restore_false_isolation_claims() -> None:
    for relative_path in LOCKED_RUNTIME_CLAIMS:
        content = _plain_text(relative_path)
        restored = [claim for claim in REMOVED_ISOLATION_CLAIMS if claim in content]
        assert not restored, f"{relative_path} restored false isolation claims: {restored}"
