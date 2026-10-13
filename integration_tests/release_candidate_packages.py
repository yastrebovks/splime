"""Isolated wheel-only author/registry/anonymous consumer acceptance harness.

Run from any directory with --author-python (SDK + private server wheels),
--consumer-python (SDK wheel + dependencies; repeat for each dependency set),
--root (new evidence directory), and --version. No production service is used.
Optional --case5-catalog adds a LOCAL ONLY check of the 14 existing YAML bundles.
The script copies itself into isolated working directories and uses -I so an
editable checkout cannot accidentally satisfy any package import.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata as metadata
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def installed(version, server=False):
    import spl

    assert "site-packages" in str(spl.__file__), spl.__file__
    assert metadata.version("splime") == version
    if server:
        import daemon_server

        assert "site-packages" in str(daemon_server.__file__), daemon_server.__file__
        assert metadata.version("spl-server") == version
    return {name: metadata.version(name) for name in ("splime", "numpy", "pandas")}


def request(url, method="GET", body=None, token=None):
    data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    with urlopen(Request(url, data=data, method=method, headers=headers), timeout=30) as response:
        return json.load(response)


def server(root, version):
    from daemon_server.app import create_app
    from daemon_server.auth import USER_TOKEN_SCOPES, MACHINE_TOKEN_SCOPES
    from daemon_server.store import ServerStore
    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    installed(version, server=True)
    cfg = json.loads((root / "config.json").read_text())
    store = ServerStore(root / "registry-home")
    store.create_user(user_id="author-a", display_name="Candidate author A")
    store.claim_user_handle("author-a", "candidate.author")
    # This disposable loopback fixture downloads 14 signed releases per consumer.
    # Raise only its public-distribution budget; production defaults stay intact.
    store.update_operational_settings(actor_user_id="author-a", values={"commercial_rate_limit": 1000})
    user = store.create_token(
        owner_id="author-a",
        subject_type="user",
        subject_id="author-a",
        scopes=[s for s in USER_TOKEN_SCOPES if s != "teams:read"],
    )
    machine = store.create_token(
        owner_id="author-a", subject_type="machine", subject_id="candidate-machine", scopes=MACHINE_TOKEN_SCOPES
    )
    private = root / "private-credentials.json"
    private.touch(mode=0o600)
    write_json(private, {"user": user["token"], "machine": machine["token"]})
    signed = store.publication_service._sign({"manifest_hash": "0" * 64, "bundle_hash": "0" * 64})
    write_json(
        root / "registry-public.json", {"registry": cfg["registry"], "keys": {signed["key_id"]: signed["public_key"]}}
    )
    app = create_app(store)

    @app.after_request
    async def evidence(response):
        from quart import request as incoming

        with (root / "network.jsonl").open("a") as stream:
            stream.write(
                json.dumps(
                    {
                        "method": incoming.method,
                        "path": incoming.path,
                        "request_bytes": len(await incoming.get_data()),
                        "status": response.status_code,
                    }
                )
                + "\n"
            )
        return response

    async def stop():
        while not (root / "stop").exists():
            await asyncio.sleep(0.1)

    config = Config()
    config.bind = [cfg["registry"].removeprefix("http://")]
    config.accesslog = None
    try:
        asyncio.run(serve(app, config, shutdown_trigger=stop))
    finally:
        write_json(
            root / "server-result.json",
            {
                "snapshot_items": store.get_latest_machine_library_snapshot("author-a", "candidate-machine")[
                    "item_count"
                ],
                "schema_version": store.schema_version_status(),
            },
        )
        store.close()


def synthetic_source(padding=40000):
    return (
        """- !DFunction
  name: score
  inputs:
  - {name: frame, type: null, default: null}
  - {name: operation, type: null, default: "'predict'"}
  outputs:
  - {name: default, type: null}
  body: |-
    import numpy as np
    import pandas as pd
    if operation == 'train':
        # Retraining is a separate exercise from frozen prediction parity.
        x = frame['x'].to_numpy(dtype=float, copy=True)
        y = frame['target'].to_numpy(dtype=float, copy=True)
        design = np.column_stack((np.ones(len(x)), x))
        coefficients = np.linalg.lstsq(design, y, rcond=None)[0]
        residual = design @ coefficients - y
        return {'coefficients': coefficients.tolist(),
                'rmse': float(np.sqrt(np.mean(residual ** 2))),
                'training_rows': len(frame),
                'numpy': np.__version__, 'pandas': pd.__version__}
    if operation != 'predict':
        raise ValueError('unknown synthetic model operation')
    # Frozen synthetic model: no training data or external files.
    values = frame['x'].to_numpy(dtype=float, copy=True)
    values[:] = np.nan_to_num(values, nan=0.0)
    result = frame.copy(deep=True)
    result['probability'] = 1.0 / (1.0 + np.exp(-(values * 0.5 - 1.0)))
    result['prediction'] = (result['probability'] >= 0.5).astype(int)
    return result
"""
        + "    # "
        + ('Ω\\quoted" ' * (padding // 10))
        + "\n"
        + """- !DDistribution
  package: numpy
  version: '2.3.5'
  modules: [numpy]
- !DDistribution
  package: pandas
  version: '2.2.3'
  modules: [pandas]
"""
    )


def author(root, version):
    from spl.daemon.store import RegistryStore
    from spl.daemon.server import DaemonRuntime

    versions = installed(version, server=True)
    cfg = json.loads((root / "config.json").read_text())
    creds = json.loads((root / "private-credentials.json").read_text())
    store = RegistryStore(root / "author-home")
    runtime = DaemonRuntime(store, auto_build_envs=False, daemon_base_url="http://127.0.0.1:1")
    try:
        runtime.register_env("default", sys.executable)
        runtime.connect_server(
            server_url=cfg["registry"],
            machine_token=creds["machine"],
            user_token=creds["user"],
            machine_id="candidate-machine",
            display_name="Candidate author",
            capabilities={},
            heartbeat_interval_seconds=5,
        )
        identity = runtime.server_whoami()
        assert identity["owner_id"] == "author-a", identity
        try:
            request(cfg["registry"] + "/users", token=creds["user"])
        except HTTPError as error:
            assert error.code == 403
        else:
            raise AssertionError("restricted user directory request was accepted")
        entries = [
            {"name": f"model_{i:02}", "entrypoint": "score", "source": synthetic_source(400_000 if i == 0 else 40_000)}
            for i in range(14)
        ]
        if cfg.get("case5_catalog"):
            entries = [
                {**item, "source": Path(item["bundle"]).read_text()}
                for item in json.loads(Path(cfg["case5_catalog"]).read_text())
            ]
        assert len(entries) == 14
        deferred_registrations = []
        for item in entries:
            record = runtime.register_object(
                item["name"],
                item["entrypoint"],
                "default",
                yaml_text=item["source"],
                owner_id="author-a",
                library="default",
                description="Local candidate validation fixture",
                runtime_config={"mode": "venv"},
            )
            admission = runtime.enqueue_object_sync(record, library="default")
            if admission["status"] == "deferred":
                deferred_registrations.append({"name": item["name"], "admission": admission})
        if not cfg.get("case5_catalog"):
            assert len(deferred_registrations) == 1
            assert deferred_registrations[0]["admission"]["diagnostic"]["actual_bytes"] > 262144
        deadline = time.monotonic() + 60
        while True:
            sync = runtime.sync_once()
            if sync.get("catalogue_sync", {}).get("state") == "current":
                break
            if time.monotonic() >= deadline:
                raise AssertionError(f"catalogue did not commit: {sync.get('catalogue_sync')}")
            time.sleep(0.1)
        # Keep every object registered throughout public activation and final sync.
        releases = []
        for item in entries:
            active = request(
                cfg["registry"] + "/objects/" + item["name"] + "/public-activate",
                "POST",
                {"execution_profile": "current-process-v1", "wait": True},
                creds["user"],
            )
            release = active["release"]
            releases.append(
                {
                    "name": item["name"],
                    "ref": release["manifest"]["identity"]["uri"] + "@" + str(release["version"]),
                    "manifest_hash": release["manifest_hash"],
                    "bundle_hash": release["bundle_hash"],
                }
            )
        runtime.sync_once()
        assert len(store.list_object_identities()) == 14
        write_json(
            root / "author-result.json",
            {
                "versions": versions,
                "retained_objects": 14,
                "yaml_utf8_bytes": sum(len(i["source"].encode()) for i in entries),
                "large_object_catalogue_registrations": deferred_registrations,
                "identity_owner": identity["owner_id"],
                "directory_forbidden": True,
                "sync": sync,
                "releases": releases,
                "case5": bool(cfg.get("case5_catalog")),
            },
        )
    finally:
        runtime.shutdown()
        store.close()


def consumer(root, version):
    import numpy as np
    import pandas as pd
    from spl import SPLClient
    from spl.public import parse_public_ref

    versions = installed(version)
    cfg = json.loads((root / "consumer-config.json").read_text())
    assert not any("TOKEN" in key and key.startswith("SPL") for key in os.environ)
    assert not (root / "cache").exists()
    forbidden = [Path(p).resolve() for p in cfg["forbidden"]]
    violations = []

    def audit(event, args):
        if event == "open" and isinstance(args[0], (str, bytes, os.PathLike)):
            path = Path(os.fsdecode(args[0])).resolve()
            if any(path == p or p in path.parents for p in forbidden):
                violations.append(str(path))
                raise AssertionError("consumer attempted author/shared-state access")
        if event in {"subprocess.Popen", "os.system"}:
            raise AssertionError("anonymous execution attempted subprocess/daemon startup")

    sys.addaudithook(audit)
    client = SPLClient.embedded(
        cfg["registry"], root / "cache", trusted_keys={cfg["registry"]: cfg["keys"]}, run_receipts=False
    )
    frame = pd.DataFrame({"x": [-2.0, 0.0, 2.0, 4.0, float("nan")]})
    expected = 1 / (1 + np.exp(-(np.nan_to_num(frame.x.to_numpy(copy=True)) * 0.5 - 1)))
    training_frame = pd.DataFrame({"x": [-3.0, -1.0, 0.0, 1.0, 3.0, 5.0], "target": [-5.0, -1.0, 1.0, 3.0, 7.0, 11.0]})
    results = []
    for release in cfg["releases"]:
        first = client.call(release["ref"], kwargs={"frame": frame}, trust=True, progress=False).value
        np.testing.assert_allclose(first.probability, expected, atol=1e-15, rtol=0)
        # Exact-version warm cache must work with network forbidden.
        transport = client._embedded_backend.transport
        original_json, original_bytes = transport.json, transport.bytes

        def no_network(*args, **kwargs):
            raise AssertionError("warm exact-version execution attempted network")

        transport.json = transport.bytes = no_network
        try:
            warm = client.call(release["ref"], kwargs={"frame": frame}, progress=False).value
            pd.testing.assert_frame_equal(first, warm)
            # Resolution re-verifies the signed cached manifest and ZIP. Bind
            # both identities to the exact version activated by this author.
            _, envelope, _ = client._embedded_backend._resolve_and_cache(parse_public_ref(release["ref"]))
            assert envelope["manifest_hash"] == release["manifest_hash"]
            assert envelope["bundle_hash"] == release["bundle_hash"]
            trained = client.call(
                release["ref"], kwargs={"frame": training_frame, "operation": "train"}, progress=False
            ).value
            assert trained["training_rows"] == len(training_frame)
            assert len(trained["coefficients"]) == 2 and np.isfinite(trained["coefficients"]).all()
            assert np.isfinite(trained["rmse"]) and trained["rmse"] < 1e-10
            assert trained["numpy"] == versions["numpy"] and trained["pandas"] == versions["pandas"]
        finally:
            transport.json, transport.bytes = original_json, original_bytes
        results.append(
            {
                "ref": release["ref"],
                "predictions": first.prediction.tolist(),
                "probabilities": first.probability.tolist(),
                "manifest_hash": envelope["manifest_hash"],
                "bundle_hash": envelope["bundle_hash"],
                "retraining": trained,
            }
        )
    assert "spl.daemon.worker" not in sys.modules
    assert not violations
    assert not (root / "daemon").exists()
    write_json(
        root / "result.json",
        {
            "versions": versions,
            "anonymous": True,
            "cold_and_warm": True,
            "exact_published_manifest_and_bundle": True,
            "separate_synthetic_retraining": True,
            "author_or_shared_state_accesses": violations,
            "results": results,
        },
    )


def isolated_env(root):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SPL", "PYTHON", "UV_"))}
    env.update(
        HOME=str(root / "home"),
        SPL_DAEMON_HOME=str(root / "daemon"),
        SPL_RUNS_HOME=str(root / "runs"),
        SPL_DAEMON_SECRET_BACKEND="file",
    )
    (root / "home").mkdir(parents=True, exist_ok=True)
    return env


def orchestrate(args):
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    write_json(
        root / "config.json",
        {
            "registry": f"http://127.0.0.1:{port}",
            "case5_catalog": str(args.case5_catalog.resolve()) if args.case5_catalog else None,
        },
    )
    script = root / "runner.py"
    shutil.copyfile(__file__, script)
    env = isolated_env(root)
    command = [str(args.author_python.absolute()), "-I", str(script)]
    with (root / "server.log").open("w") as log:
        process = subprocess.Popen(
            command + ["--worker", "server", "--root", str(root), "--version", args.version],
            cwd=root,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 30
            while True:
                try:
                    request(f"http://127.0.0.1:{port}/version")
                    break
                except Exception:
                    if process.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("registry did not become ready; see server.log")
                    time.sleep(0.1)
            with (root / "author.log").open("w") as author_log:
                subprocess.run(
                    command + ["--worker", "author", "--root", str(root), "--version", args.version],
                    cwd=root,
                    env=env,
                    stdout=author_log,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=300,
                )
            public = json.loads((root / "registry-public.json").read_text())
            author_result = json.loads((root / "author-result.json").read_text())
            if not args.case5_catalog:
                for index, python in enumerate(args.consumer_python):
                    target = root / f"consumer-{index}"
                    target.mkdir()
                    shutil.copyfile(script, target / "runner.py")
                    write_json(
                        target / "consumer-config.json",
                        {
                            **public,
                            "releases": author_result["releases"],
                            "forbidden": [
                                str(root / "author-home"),
                                str(root / "registry-home"),
                                str(root / "private-credentials.json"),
                                str(Path.home() / ".spl-daemon"),
                                str(Path(__file__).resolve().parents[1] / "src"),
                            ],
                        },
                    )
                    with (target / "consumer.log").open("w") as consumer_log:
                        subprocess.run(
                            [
                                str(python.absolute()),
                                "-I",
                                str(target / "runner.py"),
                                "--worker",
                                "consumer",
                                "--root",
                                str(target),
                                "--version",
                                args.version,
                            ],
                            cwd=target,
                            env=isolated_env(target),
                            stdout=consumer_log,
                            stderr=subprocess.STDOUT,
                            check=True,
                            timeout=120,
                        )
        finally:
            (root / "stop").touch()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=10)
            (root / "private-credentials.json").unlink(missing_ok=True)
    assert process.returncode == 0, "registry failed; inspect server.log"
    network = [json.loads(line) for line in (root / "network.jsonl").read_text().splitlines()]
    assert not any(row["method"] != "GET" and row["path"].startswith("/public") for row in network)
    server_result = json.loads((root / "server-result.json").read_text())
    assert server_result["snapshot_items"] == 14
    assert server_result["schema_version"] == {"current": 49, "target": 49}
    catalogue_requests = [row for row in network if row["path"].startswith("/catalogue-sync/")]
    assert catalogue_requests, "candidate did not negotiate the catalogue transfer capability"
    max_catalogue_bytes = max(row["request_bytes"] for row in catalogue_requests)
    assert max_catalogue_bytes <= 131072
    write_json(
        root / "summary.json",
        {
            "status": "passed",
            "platform": sys.platform,
            "server": server_result,
            "author": author_result,
            "consumers": [json.loads(p.read_text()) for p in sorted(root.glob("consumer-*/result.json"))],
            "requests": len(network),
            "max_request_bytes": max(row["request_bytes"] for row in network),
            "catalogue_request_count": len(catalogue_requests),
            "max_catalogue_request_bytes": max_catalogue_bytes,
            "catalogue_request_limit_bytes": 131072,
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--version", default="0.4.13")
    parser.add_argument("--worker", choices=("server", "author", "consumer"))
    parser.add_argument("--author-python", type=Path)
    parser.add_argument("--consumer-python", type=Path, action="append", default=[])
    parser.add_argument("--case5-catalog", type=Path)
    options = parser.parse_args()
    if options.worker:
        {"server": server, "author": author, "consumer": consumer}[options.worker](options.root, options.version)
    else:
        if not options.author_python or (not options.consumer_python and not options.case5_catalog):
            parser.error("supply installed-wheel author/consumer Python executables")
        orchestrate(options)
