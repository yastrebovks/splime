"""WP-02 presentation invariants: no public repr dumps raw transport JSON."""

from __future__ import annotations

import json

from spl._client import ObjectCatalog, ObjectList, ObjectTable, PublishedObject

_BIG_RAW = {
    "name": "daily_total",
    "version": 3,
    "library": "default",
    "yaml": "x" * 30_000,
    "metadata": {"blob": "y" * 5_000},
}

_LOCAL_PAYLOAD = {
    "daily_total": {
        "name": "daily_total",
        "kind": "function",
        "version": 3,
        "library": "default",
        "inputs": [{"name": "date"}],
    },
    "order_pipeline": {
        "name": "order_pipeline",
        "kind": "pipeline",
        "version": 1,
        "library": "risk",
        "inputs": [],
    },
}

_SERVER_PAYLOAD = [
    {
        "name": "risk_score",
        "kind": "function",
        "version": 7,
        "library": {"slug": "risk", "display_name": "Risk"},
        "inputs": [{"name": "amount"}, {"name": "history"}],
    }
]


def _published() -> PublishedObject:
    return PublishedObject(
        name="daily_total",
        entrypoint="daily_total",
        env="default",
        yaml_path="/tmp/x.yaml",
        raw=_BIG_RAW,
    )


def test_published_repr_is_compact() -> None:
    published = _published()
    assert len(repr(published)) < 200
    assert "Published daily_total" in repr(published)
    assert "v3" in repr(published)
    assert hasattr(published, "_repr_html_")
    assert len(published._repr_html_()) < 2_000


def test_published_raw_stays_accessible() -> None:
    assert _published().raw is _BIG_RAW
    assert _published().version == "3"
    assert _published().library == "default"


def test_object_views_preserve_container_semantics() -> None:
    table = ObjectTable(_LOCAL_PAYLOAD)
    listing = ObjectList(_SERVER_PAYLOAD)
    catalog = ObjectCatalog({"local": _LOCAL_PAYLOAD, "server": _SERVER_PAYLOAD})

    assert isinstance(table, dict)
    assert isinstance(listing, list)
    assert isinstance(catalog, dict)
    assert set(catalog) == {"local", "server"}
    assert table["daily_total"]["version"] == 3
    assert listing[0]["name"] == "risk_score"
    assert json.dumps(table)
    assert json.dumps(listing)
    assert json.dumps(catalog)
    assert table.raw == dict(_LOCAL_PAYLOAD)
    assert listing.raw == list(_SERVER_PAYLOAD)
    assert catalog.raw == {"local": _LOCAL_PAYLOAD, "server": _SERVER_PAYLOAD}


def test_object_views_render_compact_tables() -> None:
    table_text = repr(ObjectTable(_LOCAL_PAYLOAD))
    assert "daily_total" in table_text
    assert "order_pipeline" in table_text
    assert "kind" in table_text
    assert len(table_text) < 1_000

    listing_text = repr(ObjectList(_SERVER_PAYLOAD))
    assert "risk_score" in listing_text
    assert "Risk" in listing_text

    catalog = ObjectCatalog({"local": _LOCAL_PAYLOAD, "server": _SERVER_PAYLOAD})
    catalog_text = repr(catalog)
    assert "local" in catalog_text
    assert "server" in catalog_text
    assert hasattr(catalog, "_repr_html_")


def test_empty_views_render_placeholders() -> None:
    assert repr(ObjectList([])) == "objects: (empty)"
    assert "(empty)" in repr(ObjectCatalog({"local": {}, "server": []}))
