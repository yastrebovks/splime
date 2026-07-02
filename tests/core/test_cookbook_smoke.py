"""Cookbook smoke test: the offline DX contract, mechanically enforced (WP-08).

Runs against a live local daemon (``just smoke``); each test skips cleanly when
the daemon is not running. Every assertion is a DX invariant from the cookbook:
if this file goes red, the cookbook experience regressed — offline calls that
raise, huge reprs, or the need for deeper imports.
"""

from __future__ import annotations

import json
from contextlib import suppress

import pytest

# DX invariant: the whole cookbook works with imports from ``spl`` only.
from spl import SPLClient

pytestmark = pytest.mark.smoke

SMOKE_OBJECT = "cookbook_smoke_daily_total"


def cookbook_smoke_daily_total(date: str) -> float:
    prices = {"2026-06-08": [11.0, 6.5, 24.5]}
    return sum(prices.get(date, []))


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
        assert result.value["default"] == 42.0  # raw port dict stays available

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
