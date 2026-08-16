"""Explicit current-client to immutable public-0.4.5 daemon acceptance gate.

This module lives outside the default testpath because it requires a separate
Python environment containing the archived public ``v0.4.5`` release.  Set
``SPL_V045_PYTHON`` to that environment's interpreter before running it.
"""

from __future__ import annotations

import importlib.metadata
import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from spl import SPLClient
from spl.adapters import TEXT_FILE_UTF8, FileInput
from spl.daemon_client import ClientError


@pytest.fixture(autouse=True)
def _deny_external_network(monkeypatch: pytest.MonkeyPatch) -> None:
    original_connect = socket.socket.connect

    def guarded_connect(sock: socket.socket, address: object) -> Any:
        if isinstance(address, tuple) and str(address[0]) in {"127.0.0.1", "::1", "localhost"}:
            return original_connect(sock, address)
        raise AssertionError(f"external network access attempted: {address!r}")

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)


def _reserve_loopback_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


def _wait_for_old_daemon(
    daemon_home: Path,
    process: subprocess.Popen[str],
) -> tuple[SPLClient, dict[str, Any]]:
    deadline = time.monotonic() + 15
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout is not None else ""
            raise RuntimeError(f"public 0.4.5 daemon exited during startup:\n{output}")
        try:
            client = SPLClient(daemon_home=daemon_home)
            health = dict(client._daemon.health())  # noqa: SLF001 - explicit cross-version gate.
            return client, health
        except BaseException as exc:  # noqa: BLE001 - bounded startup polling.
            last_error = exc
            time.sleep(0.05)
    raise TimeoutError("public 0.4.5 daemon did not become healthy") from last_error


def test_current_client_calls_public_v045_daemon_and_fails_adapters_before_mutation(
    tmp_path: Path,
) -> None:
    old_python = Path(os.environ["SPL_V045_PYTHON"]).absolute()
    old_daemon = old_python.parent / "spl-daemon"
    assert old_python.is_file()
    assert old_daemon.is_file()
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    environment["PYTHONNOUSERSITE"] = "1"
    old_version = subprocess.run(
        [
            str(old_python),
            "-c",
            "import importlib.metadata as m; print(m.version('splime'))",
        ],
        check=True,
        capture_output=True,
        env=environment,
        text=True,
        timeout=10,
    ).stdout.strip()
    assert old_version == "0.4.5"
    old_daemon_version = subprocess.run(
        [str(old_daemon), "--version"],
        check=True,
        capture_output=True,
        env=environment,
        text=True,
        timeout=10,
    ).stdout.strip()
    assert old_daemon_version == "spl-daemon 0.4.5"
    assert importlib.metadata.version("splime") != old_version

    daemon_home = tmp_path / "v0.4.5-daemon-home"
    port = _reserve_loopback_port()
    environment["SPL_DAEMON_HOME"] = str(daemon_home)
    process = subprocess.Popen(
        [
            str(old_daemon),
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--home",
            str(daemon_home),
            "--no-auto-port",
            "--no-auto-build-envs",
        ],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        client, health = _wait_for_old_daemon(daemon_home, process)
        assert health["ok"] is True
        client.register_env("default")
        client.publish_yaml(
            """\
- !DFunction
  name: legacy_add
  inputs:
  - name: left
    type: int
  - name: right
    type: int
  outputs:
  - name: default
    type: int
  body: |-
    return left + right
""",
            name="legacy_add",
            entrypoint="legacy_add",
            env="default",
            local_only=True,
        )
        signature = client.signature("legacy_add")
        assert [item["name"] for item in signature["inputs"]] == ["left", "right"]
        result = client.call(
            "legacy_add",
            kwargs={"left": 19, "right": 23},
            source="local",
            keep=True,
            progress=False,
        )
        assert result.mode == "local"
        assert result.output == 42
        assert result.run["status"] == "succeeded"

        run_ids_before = {str(item["id"]) for item in client.runs()}
        private_input = tmp_path / "old-daemon-private-input.bin"
        private_input.write_bytes(b"not-json")
        rejected_requests = [
            (
                {"left": 19, "right": 23},
                {"outputs": {"default": TEXT_FILE_UTF8}},
            ),
            ({"left": FileInput(private_input), "right": 23}, None),
        ]
        for kwargs, adapters in rejected_requests:
            with pytest.raises(
                ClientError,
                match="upgrade and restart the daemon before using adapters or FileInput",
            ):
                client.submit(
                    "legacy_add",
                    kwargs=kwargs,
                    source="local",
                    adapters=adapters,
                )
            assert {str(item["id"]) for item in client.runs()} == run_ids_before
        assert {str(item["id"]) for item in client.runs()} == run_ids_before
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
