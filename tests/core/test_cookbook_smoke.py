"""Cookbook smoke test: the offline DX contract, mechanically enforced (WP-08).

Runs against a live local daemon (``just smoke``); each test skips cleanly when
the daemon is not running. Every assertion is a DX invariant from the cookbook:
if this file goes red, the cookbook experience regressed — offline calls that
raise, huge reprs, or the need for deeper imports.
"""

from __future__ import annotations

import json
from contextlib import suppress
from dataclasses import replace
from pathlib import Path

import pytest

# DX invariant: the introductory cookbook path works with imports from ``spl`` only;
# adapter recipes may use the stable advanced adapter modules.
from spl import Deployment, SPLClient, lift
from spl.core.adapter_compat import find_pipeline_adapter_compatibility_issues
from spl.core.entities.adapter import Adapter, make_key

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


@pytest.fixture(scope="module")
def client() -> SPLClient:
    candidate = SPLClient()
    try:
        candidate.health()
    except Exception:  # noqa: BLE001 - any transport error means "no daemon"
        pytest.skip("local daemon is not running (start it: spl-daemon serve)")
    return candidate


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
