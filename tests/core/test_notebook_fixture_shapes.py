"""Regression coverage for the unchanged Part 4 acceptance-notebook fixture."""

from __future__ import annotations

import json
import time
import warnings
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import yaml

from spl import Deployment, lift
from spl.core.adapter_compat import AdapterCompatibilityWarning
from spl.core.entities.adapter import Adapter, make_key
from spl.core.entities.function import get_dependency_names_from_bytecode, serialize_function
from spl.core.ir.utils import spl_import_from_file


# The definitions below are copied verbatim from
# Notebooks/splime-cookbook-вц.ipynb Part 4, cell 73. Their ordinary Python
# scoping is the contract under test; do not refactor them around the serializer.
@dataclass(frozen=True)
class LedgerBatch:
    region: str
    lines: tuple[str, ...]


@dataclass(frozen=True)
class ParsedOrders:
    region: str
    totals: tuple[int, ...]


@dataclass(frozen=True)
class ScoreCard:
    region: str
    minimum_total: int
    kept_totals: tuple[int, ...]


CALLS = {"extract": 0, "parse": 0, "score": 0, "report": 0}


def reset_calls() -> None:
    for key in CALLS:
        CALLS[key] = 0


def extract_ledger(region: str) -> LedgerBatch:
    CALLS["extract"] += 1
    time.sleep(0.15)
    samples = {"north": (120, 260, 420), "south": (90, 310, 510)}
    totals = samples.get(region, samples["north"])
    return LedgerBatch(region=region, lines=tuple(f"order-{i},{t}" for i, t in enumerate(totals, 1)))


def parse_ledger(batch: LedgerBatch) -> ParsedOrders:
    CALLS["parse"] += 1
    return ParsedOrders(region=batch.region, totals=tuple(int(line.split(",")[1]) for line in batch.lines))


def score_orders(parsed: ParsedOrders, minimum_total: int) -> ScoreCard:
    CALLS["score"] += 1
    kept = tuple(t for t in parsed.totals if t >= minimum_total)
    return ScoreCard(region=parsed.region, minimum_total=minimum_total, kept_totals=kept)


def format_report(card: ScoreCard) -> str:
    CALLS["report"] += 1
    return f"{card.region}: {len(card.kept_totals)} orders >= {card.minimum_total}; total={sum(card.kept_totals)}"


def save_ledger(path: str, value: LedgerBatch) -> None:
    Path(path).write_text(json.dumps({"region": value.region, "lines": value.lines}, sort_keys=True), encoding="utf-8")


def load_ledger_v1(path: str) -> LedgerBatch:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return LedgerBatch(region=payload["region"], lines=tuple(payload["lines"]))


def load_ledger_never(path: str) -> LedgerBatch:
    raise AssertionError("this loader must never run; the tag check fails first")


def save_parsed(path: str, value: ParsedOrders) -> None:
    Path(path).write_text(
        json.dumps({"region": value.region, "totals": value.totals}, sort_keys=True), encoding="utf-8"
    )


def load_parsed(path: str) -> ParsedOrders:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return ParsedOrders(region=payload["region"], totals=tuple(payload["totals"]))


def save_score(path: str, value: ScoreCard) -> None:
    Path(path).write_text(
        json.dumps(
            {"region": value.region, "minimum_total": value.minimum_total, "kept_totals": value.kept_totals},
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def load_score(path: str) -> ScoreCard:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return ScoreCard(
        region=payload["region"], minimum_total=int(payload["minimum_total"]), kept_totals=tuple(payload["kept_totals"])
    )


class LedgerV2OnlyAdapter(Adapter):
    # A load half that only accepts a tag nobody writes yet — the classic silent-CSV bug, made loud.
    @property
    def accepted_tags(self) -> frozenset[str]:
        return frozenset({"ledger-v2"})


def good_ledger_adapter() -> Adapter:
    return Adapter(
        key=make_key(LedgerBatch, "ledger-v1"),
        save=save_ledger,
        load=load_ledger_v1,
        py_type=LedgerBatch,
        format="ledger-v1",
    )


def build_pipeline(*, broken_ledger_load: bool):
    extract = lift(extract_ledger).alias("extract")
    parse = lift(parse_ledger).bind(batch=extract).alias("parse")
    score = lift(score_orders).bind(parsed=parse).alias("score")
    p = lift(format_report).bind(card=score).alias("report").render("acc040_local")
    p = (
        p.add_adapter(LedgerBatch, "ledger-v1", save=save_ledger, load=load_ledger_v1)
        .add_adapter(ParsedOrders, "parsed-json", save=save_parsed, load=load_parsed)
        .add_adapter(ScoreCard, "score-json", save=save_score, load=load_score)
    )
    if broken_ledger_load:
        bad = LedgerV2OnlyAdapter(
            key=make_key(LedgerBatch, "ledger-v1"),
            save=save_ledger,
            load=load_ledger_never,
            py_type=LedgerBatch,
            format="ledger-v1",
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", AdapterCompatibilityWarning)
            p = replace(p, adapters={**p.adapters, bad.key: bad})
    return p


def test_notebook_score_orders_publishes_and_round_trips(tmp_path: Path) -> None:
    """The exact genexpr fixture is publishable and keeps invocation semantics."""

    dependency_names = set(get_dependency_names_from_bytecode(score_orders))
    assert "minimum_total" not in dependency_names
    assert {"CALLS", "ScoreCard"} <= dependency_names

    payload = serialize_function(score_orders)
    assert lift(score_orders) is not None
    path = tmp_path / "score-orders.yaml"
    path.write_text(yaml.dump([payload], sort_keys=False, allow_unicode=True), encoding="utf-8")
    namespace: dict[str, Any] = {
        "__name__": "__main__",
        "ParsedOrders": ParsedOrders,
        "ScoreCard": ScoreCard,
        "CALLS": CALLS,
    }
    spl_import_from_file(path, namespace)
    rebuilt = namespace["score_orders"]
    parsed = ParsedOrders(region="north", totals=(120, 260, 420))

    reset_calls()
    expected = score_orders(parsed, 200)
    reset_calls()
    assert rebuilt(parsed, 200) == expected


def test_notebook_part4_build_pipeline_runs_unchanged() -> None:
    """The exact Part 4 build_pipeline fixture still runs unchanged."""

    reset_calls()
    pipeline = build_pipeline(broken_ledger_load=False)
    assert (
        Deployment(pipeline).run(
            output="report",
            keep=False,
            region="north",
            minimum_total=200,
        )
        == "north: 2 orders >= 200; total=680"
    )
    assert CALLS == {"extract": 1, "parse": 1, "score": 1, "report": 1}

    reset_calls()
    rebuilt = build_pipeline(broken_ledger_load=False)
    assert (
        Deployment(rebuilt).run(
            output="report",
            keep=False,
            region="north",
            minimum_total=200,
        )
        == "north: 2 orders >= 200; total=680"
    )
