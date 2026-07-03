"""Command line interface for the minimal SPL daemon.

The CLI is intentionally thin and maps one-to-one to the client/server API.  It
is enough for the MVP workflow:

1. start the local daemon;
2. register a Python environment;
3. register a serialized SPL YAML object;
4. run that object and fetch the result/artifacts.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from spl.daemon_client import (
    DEFAULT_DAEMON_PORT,
    DEFAULT_SERVER_URL,
    DEFAULT_URL,
    Client,
    RunProgressPrinter,
)


def print_json(value: Any) -> None:
    """Print a JSON value for shell-friendly output."""

    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def parse_json_arg(value: str, expected_type: type) -> Any:
    """Parse and validate a JSON command line argument."""

    parsed = json.loads(value)
    if not isinstance(parsed, expected_type):
        raise argparse.ArgumentTypeError(
            f"expected JSON {expected_type.__name__}, got {type(parsed).__name__}"
        )
    return parsed


def read_runtime_config(
    path: Path | None,
    *,
    runtime: str | None,
    python: str | None,
    base_image: str | None,
) -> dict[str, Any] | None:
    """Read a runtime sidecar YAML file and merge explicit CLI overrides."""

    config: dict[str, Any] = {}
    if path is not None:
        import yaml

        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        if loaded is None:
            config = {}
        elif isinstance(loaded, dict):
            config = loaded
        else:
            raise argparse.ArgumentTypeError(
                "--runtime-config must contain a YAML mapping"
            )

    if "runtime" in config and isinstance(config["runtime"], dict):
        target = dict(config["runtime"])
        config = {"runtime": target}
    else:
        target = config
    if runtime is not None:
        target["mode"] = runtime
    if python is not None:
        target["python"] = python
    if base_image is not None:
        target["base_image"] = base_image

    if not config and runtime is None and python is None and base_image is None:
        return None
    return config


def build_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser."""

    parser = argparse.ArgumentParser(prog="python -m spl.daemon")
    parser.add_argument(
        "--url",
        default=None,
        help=f"daemon base URL for client commands; defaults to the running daemon endpoint or {DEFAULT_URL}",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="start the local daemon")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=DEFAULT_DAEMON_PORT)
    serve.add_argument("--home", type=Path, default=None)
    serve.add_argument(
        "--auto-port",
        dest="auto_port",
        action="store_true",
        default=True,
        help="try the next ports when the requested port is busy",
    )
    serve.add_argument(
        "--no-auto-port",
        dest="auto_port",
        action="store_false",
        help="fail when the requested port is busy",
    )
    serve.add_argument(
        "--port-scan-limit",
        type=int,
        default=100,
        help="maximum number of sequential ports to try when auto-port is enabled",
    )
    serve.add_argument(
        "--auto-build-envs",
        dest="auto_build_envs",
        action="store_true",
        default=True,
        help="build cached venvs immediately after object registration",
    )
    serve.add_argument(
        "--no-auto-build-envs",
        dest="auto_build_envs",
        action="store_false",
        help="build cached venvs only when an object is first run",
    )
    serve.add_argument(
        "--env-build-timeout",
        type=float,
        default=None,
        help="seconds before venv creation or pip install is failed",
    )
    serve.add_argument(
        "--env-stale-lock-timeout",
        type=float,
        default=None,
        help="seconds before an abandoned venv build lock may be reused",
    )
    serve.add_argument(
        "--docker-pool-size",
        type=int,
        default=0,
        help="maximum warm Docker containers to keep; 0 disables the pool",
    )
    serve.add_argument(
        "--docker-idle-timeout",
        type=float,
        default=300.0,
        help="seconds before an idle warm Docker container is evicted",
    )
    serve.add_argument(
        "--docker-prewarm",
        action="store_true",
        help="after Docker object registration, build the image and warm a pooled container",
    )

    subparsers.add_parser("health", help="show daemon health details")

    doctor = subparsers.add_parser(
        "doctor",
        help="diagnose the local splime setup (interpreter, home, daemon, server)",
    )
    doctor.add_argument(
        "--home",
        type=Path,
        default=None,
        help="daemon home directory to inspect; defaults to SPL_DAEMON_HOME",
    )
    doctor.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="print machine-readable check results",
    )

    server_connect = subparsers.add_parser(
        "server-connect",
        help="connect the local daemon to the central server",
    )
    server_connect.add_argument("--server-url", default=None)
    server_connect.add_argument("--machine-token", required=True)
    server_connect.add_argument("--user-token", required=True)
    server_connect.add_argument("--machine-id", default=None)
    server_connect.add_argument("--display-name", default=None)
    server_connect.add_argument("--capabilities", default="{}", help="JSON object")
    server_connect.add_argument("--heartbeat-interval", type=float, default=None)

    subparsers.add_parser(
        "server-disconnect",
        help="disconnect the local daemon from the central server",
    )
    subparsers.add_parser(
        "server-connection",
        help="show the current central-server connection",
    )
    subparsers.add_parser(
        "server-connections",
        help="list stored central-server connection attempts",
    )
    subparsers.add_parser(
        "server-machines",
        help="list machines visible to the connected user",
    )

    env_add = subparsers.add_parser("env-add", help="register a Python executable")
    env_add.add_argument("name")
    env_add.add_argument("python")

    subparsers.add_parser("env-list", help="list registered environments")

    subparsers.add_parser("env-build-list", help="list cached venv builds")

    subparsers.add_parser("image-list", help="list cached Docker runtime images")

    image_show = subparsers.add_parser(
        "image-show",
        help="show one cached Docker runtime image record",
    )
    image_show.add_argument("spec_hash")

    image_rebuild = subparsers.add_parser(
        "image-rebuild",
        help="force one cached Docker runtime image to be rebuilt",
    )
    image_rebuild.add_argument("spec_hash")
    image_rebuild.add_argument(
        "--wait",
        action="store_true",
        help="wait until the rebuild finishes",
    )

    image_prune = subparsers.add_parser(
        "image-prune",
        help="remove cached Docker runtime images",
    )
    image_prune.add_argument(
        "spec_hash",
        nargs="?",
        default=None,
        help="optional Docker image spec hash to prune; omit to prune all Docker images",
    )

    env_build_show = subparsers.add_parser(
        "env-build-show",
        help="show one cached venv build",
    )
    env_build_show.add_argument("spec_hash")

    env_build_rebuild = subparsers.add_parser(
        "env-build-rebuild",
        help="force one cached venv build to be recreated",
    )
    env_build_rebuild.add_argument("spec_hash")
    env_build_rebuild.add_argument(
        "--wait",
        action="store_true",
        help="wait until the rebuild finishes",
    )

    subparsers.add_parser(
        "remote-signature-list",
        help="list cached remote object signatures",
    )

    remote_signature_resolve = subparsers.add_parser(
        "remote-signature-resolve",
        help="resolve and cache a remote object signature",
    )
    remote_signature_resolve.add_argument(
        "ref",
        help="JSON object, for example {\"url\":\"https://splime.io/api\",\"name\":\"demo\"}",
    )
    remote_signature_resolve.add_argument(
        "--force",
        action="store_true",
        help="refresh the signature even when a cache entry exists",
    )

    object_add = subparsers.add_parser("object-add", help="register SPL YAML")
    object_add.add_argument("name")
    object_add.add_argument("yaml_path", type=Path)
    object_add.add_argument("--entrypoint", required=True)
    object_add.add_argument("--env", required=True)
    object_add.add_argument("--description", default=None)
    object_add.add_argument("--version-label", default=None)
    object_add.add_argument(
        "--runtime",
        choices=["venv", "docker"],
        default=None,
        help="runtime launcher for this object version; defaults to venv",
    )
    object_add.add_argument(
        "--python",
        dest="python_version",
        default=None,
        help="Python version for Docker runtime, for example 3.13",
    )
    object_add.add_argument(
        "--base-image",
        default=None,
        help="Docker base image override, for example python:3.13-slim-trixie",
    )
    object_add.add_argument(
        "--runtime-config",
        type=Path,
        default=None,
        help="YAML sidecar with a top-level runtime mapping",
    )
    object_add.add_argument(
        "--object-id",
        default=None,
        help="explicitly append a version to this existing object id",
    )
    object_add.add_argument(
        "--workdir",
        default=None,
        help="optional worker cwd; defaults to the per-run directory",
    )
    object_add.add_argument(
        "--local-only",
        action="store_true",
        help="store only in the local daemon and skip server sync",
    )

    object_list = subparsers.add_parser("object-list", help="list registered objects")
    object_list.add_argument("--query", default=None, help="optional registry search text")
    object_list.add_argument(
        "--compact",
        action="store_true",
        help="show human-sized object summaries",
    )

    object_show = subparsers.add_parser("object-show", help="show one object")
    object_show.add_argument("name_or_id")
    object_show.add_argument("--version", type=int, default=None)

    object_signature = subparsers.add_parser(
        "object-signature",
        help="show object inputs, outputs, and result accessors",
    )
    object_signature.add_argument("name_or_id")
    object_signature.add_argument("--version", type=int, default=None)

    object_inputs = subparsers.add_parser(
        "object-inputs",
        help="show object call inputs",
    )
    object_inputs.add_argument("name_or_id")
    object_inputs.add_argument("--version", type=int, default=None)

    object_outputs = subparsers.add_parser(
        "object-outputs",
        help="show object output selectors and result accessors",
    )
    object_outputs.add_argument("name_or_id")
    object_outputs.add_argument("--version", type=int, default=None)

    object_versions = subparsers.add_parser(
        "object-versions",
        help="list versions for one object",
    )
    object_versions.add_argument("name_or_id")

    run = subparsers.add_parser("run", help="start an object run")
    run.add_argument("object")
    run.add_argument("--args", default="[]", help="JSON list of positional args")
    run.add_argument("--kwargs", default="{}", help="JSON object of keyword args")
    run.add_argument("--output", default=None, help="pipeline alias to return")
    run.add_argument("--timeout", type=float, default=None)
    run.add_argument("--version", type=int, default=None)
    run.add_argument("--version-id", default=None)
    run.add_argument(
        "--target-machine",
        default=None,
        help="run through the central server on this machine id",
    )
    run.add_argument(
        "--source",
        choices=["auto", "local"],
        default="auto",
        help="for local runs, refresh from server before running or stay local only",
    )
    run.add_argument(
        "--wait",
        action="store_true",
        help="wait for completion and print the final result when available",
    )
    run.add_argument(
        "--no-progress",
        dest="progress",
        action="store_false",
        default=True,
        help="with --wait, do not print slow-phase progress lines to stderr",
    )

    run_status = subparsers.add_parser("run-status", help="show one run state")
    run_status.add_argument("run_id")

    subparsers.add_parser("run-list", help="list known runs")

    run_result = subparsers.add_parser("run-result", help="show one run result")
    run_result.add_argument("run_id")

    artifact_list = subparsers.add_parser("artifact-list", help="list run artifacts")
    artifact_list.add_argument("run_id")

    artifact_get = subparsers.add_parser("artifact-get", help="download an artifact")
    artifact_get.add_argument("run_id")
    artifact_get.add_argument("name")
    artifact_get.add_argument("target", type=Path)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Execute the CLI command."""

    args = build_parser().parse_args(argv)

    if args.command == "serve":
        try:
            from spl.daemon.server import serve

            serve(
                host=args.host,
                port=args.port,
                home=args.home,
                auto_port=args.auto_port,
                port_scan_limit=args.port_scan_limit,
                auto_build_envs=args.auto_build_envs,
                env_build_timeout_seconds=args.env_build_timeout,
                env_stale_lock_seconds=args.env_stale_lock_timeout,
                docker_pool_size=args.docker_pool_size,
                docker_idle_timeout_seconds=args.docker_idle_timeout,
                docker_prewarm=args.docker_prewarm,
            )
            return 0
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    client = Client(args.url)

    try:
        if args.command == "health":
            print_json(client.health())
        elif args.command == "doctor":
            from spl.daemon.doctor import run_doctor

            report = run_doctor(client, home=args.home)
            if args.as_json:
                print_json(report.to_payload())
            else:
                print(report.render())
            return report.exit_code
        elif args.command == "server-connect":
            capabilities = parse_json_arg(args.capabilities, dict)
            print_json(
                client.connect_server(
                    machine_token=args.machine_token,
                    user_token=args.user_token,
                    server_url=args.server_url
                    if args.server_url is not None
                    else DEFAULT_SERVER_URL,
                    machine_id=args.machine_id,
                    display_name=args.display_name,
                    capabilities=capabilities,
                    heartbeat_interval_seconds=args.heartbeat_interval,
                )
            )
        elif args.command == "server-disconnect":
            print_json(client.disconnect_server())
        elif args.command == "server-connection":
            print_json(client.server_connection())
        elif args.command == "server-connections":
            print_json(client.server_connections())
        elif args.command == "server-machines":
            print_json(client.server_machines())
        elif args.command == "env-add":
            print_json(client.register_env(args.name, args.python))
        elif args.command == "env-list":
            print_json(client.list_envs())
        elif args.command == "env-build-list":
            print_json(client.list_environment_builds())
        elif args.command == "image-list":
            print_json(
                [
                    record
                    for record in client.list_environment_builds()
                    if record.get("runtime_type") == "docker"
                ]
            )
        elif args.command == "image-show":
            record = client.get_environment_build(args.spec_hash)
            if record.get("runtime_type") != "docker":
                raise ValueError(f"environment build is not a Docker image: {args.spec_hash}")
            print_json(record)
        elif args.command == "image-rebuild":
            print_json(
                client.rebuild_environment_build(
                    args.spec_hash,
                    wait=args.wait,
                )
            )
        elif args.command == "image-prune":
            print_json(client.prune_docker_images(spec_hash=args.spec_hash))
        elif args.command == "env-build-show":
            print_json(client.get_environment_build(args.spec_hash))
        elif args.command == "env-build-rebuild":
            print_json(
                client.rebuild_environment_build(
                    args.spec_hash,
                    wait=args.wait,
                )
            )
        elif args.command == "remote-signature-list":
            print_json(client.list_remote_signatures())
        elif args.command == "remote-signature-resolve":
            print_json(
                client.resolve_remote_signature(
                    parse_json_arg(args.ref, dict),
                    force=args.force,
                )
            )
        elif args.command == "object-add":
            print_json(
                client.register_object(
                    args.name,
                    entrypoint=args.entrypoint,
                    env=args.env,
                    yaml_path=args.yaml_path,
                    workdir=args.workdir,
                    runtime_config=read_runtime_config(
                        args.runtime_config,
                        runtime=args.runtime,
                        python=args.python_version,
                        base_image=args.base_image,
                    ),
                    description=args.description,
                    version_label=args.version_label,
                    object_id=args.object_id,
                    local_only=args.local_only,
                )
            )
        elif args.command == "object-list":
            print_json(client.list_objects(query=args.query, compact=args.compact))
        elif args.command == "object-show":
            print_json(client.get_object(args.name_or_id, version=args.version))
        elif args.command == "object-signature":
            print_json(client.signature(args.name_or_id, version=args.version))
        elif args.command == "object-inputs":
            print_json(client.inputs(args.name_or_id, version=args.version))
        elif args.command == "object-outputs":
            print_json(client.outputs(args.name_or_id, version=args.version))
        elif args.command == "object-versions":
            print_json(client.object_versions(args.name_or_id))
        elif args.command == "run":
            positional_args = parse_json_arg(args.args, list)
            keyword_args = parse_json_arg(args.kwargs, dict)
            state = client.run(
                args.object,
                args=positional_args,
                kwargs=keyword_args,
                output=args.output,
                timeout_seconds=args.timeout,
                version=args.version,
                version_id=args.version_id,
                target_machine=args.target_machine,
                source=args.source,
            )
            if not args.wait:
                print_json(state)
                return 0

            on_state = RunProgressPrinter() if args.progress else None
            if args.target_machine is not None:
                final_state = client.wait_remote_run(
                    state["id"],
                    timeout_seconds=args.timeout,
                    on_state=on_state,
                )
                print_json(
                    {
                        "run": final_state,
                        "payload": final_state.get("result") or {},
                    }
                )
                return 0

            final_state = client.wait_run(state["id"], on_state=on_state)
            if final_state["status"] == "succeeded":
                print_json(
                    {
                        "run": final_state,
                        "payload": client.result(final_state["id"]),
                    }
                )
            else:
                print_json({"run": final_state})
        elif args.command == "run-status":
            print_json(client.get_run(args.run_id))
        elif args.command == "run-list":
            print_json(client.list_runs())
        elif args.command == "run-result":
            print_json(client.result(args.run_id))
        elif args.command == "artifact-list":
            print_json(client.list_artifacts(args.run_id))
        elif args.command == "artifact-get":
            path = client.download_artifact(args.run_id, args.name, args.target)
            print_json({"path": str(path)})
        else:
            raise AssertionError(f"unhandled command: {args.command}")
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0
