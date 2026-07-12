"""Cookbook smoke test: the offline DX contract, mechanically enforced (WP-08).

Runs against a temporary local daemon so the smoke path cannot mutate the user's
live daemon home. Every assertion is a DX invariant from the cookbook: if this
file goes red, the cookbook experience regressed — offline calls that raise,
huge reprs, or the need for deeper imports.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Iterator

import pytest

# DX invariant: the introductory cookbook path works with imports from ``spl`` only;
# adapter recipes may use the stable advanced adapter modules.
from spl import Deployment, SPLClient, lift
from spl.core.adapter_compat import find_pipeline_adapter_compatibility_issues
from spl.core.entities.adapter import Adapter, make_key
from spl.daemon.server import create_app
from spl.daemon.store import RegistryStore
from spl.daemon_client import Client

pytestmark = pytest.mark.smoke

SMOKE_OBJECT = "cookbook_smoke_daily_total"


def cookbook_smoke_daily_total(date: str) -> float:
    prices = {"2026-06-08": [11.0, 6.5, 24.5]}
    return sum(prices.get(date, []))


class CookbookDictCsvEdgeAdapter(Adapter):
    @property
    def accepted_tags(self) -> frozenset[str]:
        """Return the tag accepted by the consumer load half in the recipe."""

        return frozenset({"json"})


def cookbook_extract_csv() -> str:
    return "name,score\nAda,7\nGrace,9\n"


def cookbook_save_csv_rows(path: str, value: str) -> None:
    Path(path).write_text(value, encoding="utf-8")


def cookbook_load_csv_rows(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def cookbook_save_json_rows(path: str, value: dict) -> None:
    Path(path).write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def cookbook_load_json_rows(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def cookbook_max_score(value: dict) -> int:
    return max(int(row["score"]) for row in value["rows"])


def cookbook_csv_to_json_rows(value: str) -> dict:
    header, *lines = value.strip().splitlines()
    columns = header.split(",")
    return {"rows": [dict(zip(columns, line.split(","))) for line in lines]}


def _free_local_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _serve_app_in_thread(app, port: int) -> tuple[threading.Event, threading.Thread, list[BaseException]]:
    stop_event = threading.Event()
    errors: list[BaseException] = []

    def _run() -> None:
        from hypercorn.asyncio import serve as hypercorn_serve
        from hypercorn.config import Config

        async def _shutdown_trigger() -> None:
            while not stop_event.is_set():
                await asyncio.sleep(0.05)

        async def _serve() -> None:
            config = Config()
            config.bind = [f"127.0.0.1:{port}"]
            config.use_reloader = False
            config.accesslog = None
            config.errorlog = None
            await hypercorn_serve(app, config, shutdown_trigger=_shutdown_trigger)

        try:
            asyncio.run(_serve())
        except BaseException as exc:  # pragma: no cover - re-raised by caller.
            errors.append(exc)

    thread = threading.Thread(target=_run, name=f"spl-cookbook-smoke-daemon-{port}", daemon=True)
    thread.start()

    client = Client(f"http://127.0.0.1:{port}", api_token=app.api_token)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if errors:
            raise RuntimeError("cookbook smoke daemon failed to start") from errors[0]
        try:
            client.health()
            return stop_event, thread, errors
        except Exception:
            time.sleep(0.05)

    stop_event.set()
    thread.join(timeout=2.0)
    raise TimeoutError("cookbook smoke daemon did not start")


@pytest.fixture(scope="module")
def client(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SPLClient]:
    home = tmp_path_factory.mktemp("cookbook-smoke-daemon")
    store = RegistryStore(home)
    app = create_app(store, api_token="cookbook-smoke-token")
    port = _free_local_port()
    stop_event, thread, errors = _serve_app_in_thread(app, port)
    try:
        yield SPLClient(base_url=f"http://127.0.0.1:{port}", api_token=app.api_token)
    finally:
        stop_event.set()
        thread.join(timeout=2.0)
        app.runtime.shutdown()
        store.close()
        if errors:
            raise RuntimeError("cookbook smoke daemon failed") from errors[0]


def test_offline_listings_never_raise(client: SPLClient) -> None:
    """Server-only listings return empty states offline — no try/except needed."""

    connection = client.current_server_connection()
    machines = client.machines()
    libraries = client.libraries()
    catalog = client.objects(scope="all")

    assert isinstance(connection, dict) and "connected" in connection
    assert isinstance(machines, dict) and "machines" in machines
    assert isinstance(libraries, list)
    assert set(catalog) == {"local", "server"}
    if not connection.get("connected"):
        assert machines == {"current_machine_id": None, "machines": []}
        assert libraries == []
        assert catalog["server"] == []


def test_publish_receipt_call_output_describe(client: SPLClient) -> None:
    """publish -> short receipt; call -> plain value via .output; describe reads."""

    client.register_env("default")
    try:
        receipt = client.publish(cookbook_smoke_daily_total, name=SMOKE_OBJECT)
        assert len(repr(receipt)) < 200, "publish receipt must stay one short line"
        assert hasattr(receipt, "_repr_html_")
        assert isinstance(receipt.raw, dict) and receipt.raw

        result = client.call(SMOKE_OBJECT, kwargs={"date": "2026-06-08"})
        assert result.mode == "local"
        assert result.output == 42.0
        # Functions return a plain value; the port dict appears for pipelines
        # (where .output unwraps it and .value keeps it raw).
        assert result.value == 42.0

        assert SMOKE_OBJECT in client.describe(SMOKE_OBJECT)
    finally:
        with suppress(Exception):
            client.forget(SMOKE_OBJECT)


def test_catalog_prints_compactly(client: SPLClient) -> None:
    """objects() renders a table, not a JSON blob, and stays a plain container."""

    table = client.objects(scope="local")
    assert isinstance(table, dict)
    json.dumps(table)  # container semantics: still JSON-serializable
    assert hasattr(table, "_repr_html_")
    assert hasattr(table, "raw") and isinstance(table.raw, dict)

    text = repr(table)
    # Compactness scales with the number of objects: ~one short row each,
    # never a raw transport dump.
    assert len(text) < 200 * (len(table) + 3), "objects() repr must stay tabular"
    assert "yaml" not in text, "objects() repr must not leak object bodies"


def test_cache_warming_recipe_is_safe_without_server(client: SPLClient) -> None:
    """Cookbook cache-warming recipe: offline exits; online plans before batch."""

    connection = client.current_server_connection()
    if not connection.get("connected"):
        server_objects = client.objects(scope="server")
        assert list(server_objects) == []
        return

    plan = client.pull_all(dry_run=True)
    assert {"objects_seen", "pulled", "skipped", "failed", "ambiguous_names"} <= set(plan)

    server_objects = client.objects(scope="server")
    if not server_objects:
        return

    first = server_objects[0]
    library = first.get("library")
    if isinstance(library, dict):
        library = library.get("slug") or library.get("name") or library.get("display_name")
    receipt = client.pull(
        first["name"],
        owner=first.get("owner_id"),
        library=library if isinstance(library, str) else None,
    )
    assert {"pulled", "skipped", "failed", "ambiguous_names"} <= set(receipt)


def test_converter_node_recipe_warns_then_runs() -> None:
    """Cookbook converter-node recipe: mismatch warning, converter, green run."""

    extract = lift(cookbook_extract_csv).alias("extract")
    broken = (
        lift(cookbook_max_score)
        .bind(value=extract.as_format("csv-lines"))
        .alias("score")
        .render("broken_scores")
        .add_adapter(str, "csv-lines", save=cookbook_save_csv_rows, load=cookbook_load_csv_rows)
    )
    load_half = CookbookDictCsvEdgeAdapter(
        key=make_key(dict, "csv-lines"),
        save=cookbook_save_json_rows,
        load=cookbook_load_json_rows,
        py_type=dict,
        format="csv-lines",
    )
    broken = replace(broken, adapters={**broken.adapters, load_half.key: load_half})

    issues = find_pipeline_adapter_compatibility_issues(broken)

    assert len(issues) == 1
    assert issues[0].edge == "extract.default -> score.value"
    assert issues[0].save_tag == "csv-lines"
    assert issues[0].accepted_tags == ("json",)
    assert "Converter Nodes For Adapter Tags" in issues[0].warning_message

    extract = lift(cookbook_extract_csv).alias("extract")
    convert = lift(cookbook_csv_to_json_rows).bind(value=extract.as_format("csv-lines")).alias("convert")
    fixed = (
        lift(cookbook_max_score)
        .bind(value=convert.as_format("json"))
        .alias("score")
        .render("fixed_scores")
        .add_adapter(str, "csv-lines", save=cookbook_save_csv_rows, load=cookbook_load_csv_rows)
    )

    assert find_pipeline_adapter_compatibility_issues(fixed) == ()
    assert Deployment(fixed).run(output="score", keep=False) == 9
