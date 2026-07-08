from __future__ import annotations

import warnings
from dataclasses import replace
from typing import Any, cast

from spl import lift
from spl.core.adapter_compat import (
    AdapterCompatibilityWarning,
    _reset_adapter_compatibility_warnings,
    find_pipeline_adapter_compatibility_issues,
)
from spl.core.entities.adapter import Adapter, make_key
from spl.core.entities.pipeline import Pipeline
from spl.daemon.doctor import WARN, check_pipeline_adapter_tags


def _make_text() -> str:
    return "text"


def _consume_text(value: str) -> str:
    return value


def _make_other_text() -> str:
    return "other"


def _save_text(path: str, obj: str) -> None:
    del path, obj


def _load_text(path: str) -> str:
    del path
    return ""


class _CsvLoadTsvAdapter(Adapter):
    @property
    def accepted_tags(self) -> frozenset[str]:
        """Return a deliberately incompatible accepted tag set."""

        return frozenset({"tsv"})


class _PipeLoadJsonAdapter(Adapter):
    @property
    def accepted_tags(self) -> frozenset[str]:
        """Return a second deliberately incompatible accepted tag set."""

        return frozenset({"json"})


def _mismatched_adapter(format: str, adapter_type: type[Adapter]) -> Adapter:
    return adapter_type(
        key=make_key(str, format),
        save=_save_text,
        load=_load_text,
        py_type=str,
        format=format,
    )


def _single_mismatch_pipeline() -> Pipeline:
    lift_any = cast(Any, lift)
    producer = lift_any(_make_text).alias("producer")
    pipeline = lift_any(_consume_text).bind(value=producer.as_format("csv")).alias("consumer").render("single")
    adapter = _mismatched_adapter("csv", _CsvLoadTsvAdapter)
    return replace(pipeline, adapters={adapter.key: adapter})


def _two_mismatch_pipeline() -> Pipeline:
    lift_any = cast(Any, lift)
    first_producer = lift_any(_make_text).alias("producer")
    first = lift_any(_consume_text).bind(value=first_producer.as_format("csv")).alias("consumer")
    second_producer = lift_any(_make_other_text).alias("other_producer")
    second = lift_any(_consume_text).bind(value=second_producer.as_format("pipe")).alias("other_consumer")
    pipeline = (
        (first.pipeline | second.pipeline).add_alias(first.root, "consumer").add_alias(second.root, "other_consumer")
    )
    csv_adapter = _mismatched_adapter("csv", _CsvLoadTsvAdapter)
    pipe_adapter = _mismatched_adapter("pipe", _PipeLoadJsonAdapter)
    return replace(pipeline, name="two", adapters={csv_adapter.key: csv_adapter, pipe_adapter.key: pipe_adapter})


def _adapter_warnings(captured: list[warnings.WarningMessage]) -> list[warnings.WarningMessage]:
    return [item for item in captured if issubclass(item.category, AdapterCompatibilityWarning)]


def test_builder_chain_warns_once_for_same_adapter_mismatch_content() -> None:
    pipeline = _single_mismatch_pipeline()

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", AdapterCompatibilityWarning)
        pipeline._validate_consistency()
        pipeline.add_alias(pipeline.aliases["producer"], "producer")
        pipeline.add_alias(pipeline.aliases["consumer"], "consumer")

    adapter_warnings = _adapter_warnings(captured)
    assert len(adapter_warnings) == 1
    assert "producer.default -> consumer.value" in str(adapter_warnings[0].message)


def test_adapter_compatibility_warning_reset_allows_warning_again() -> None:
    pipeline = _single_mismatch_pipeline()

    with warnings.catch_warnings(record=True) as first:
        warnings.simplefilter("always", AdapterCompatibilityWarning)
        pipeline._validate_consistency()
    with warnings.catch_warnings(record=True) as suppressed:
        warnings.simplefilter("always", AdapterCompatibilityWarning)
        pipeline._validate_consistency()

    _reset_adapter_compatibility_warnings()

    with warnings.catch_warnings(record=True) as after_reset:
        warnings.simplefilter("always", AdapterCompatibilityWarning)
        pipeline._validate_consistency()

    assert len(_adapter_warnings(first)) == 1
    assert _adapter_warnings(suppressed) == []
    assert len(_adapter_warnings(after_reset)) == 1


def test_distinct_adapter_mismatch_contents_emit_distinct_warnings() -> None:
    pipeline = _two_mismatch_pipeline()

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", AdapterCompatibilityWarning)
        pipeline._validate_consistency()

    messages = sorted(str(item.message) for item in _adapter_warnings(captured))
    assert len(messages) == 2
    assert any("producer.default -> consumer.value" in message for message in messages)
    assert any("other_producer.default -> other_consumer.value" in message for message in messages)


def test_find_pipeline_adapter_compatibility_issues_ignores_warning_dedupe() -> None:
    pipeline = _two_mismatch_pipeline()

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", AdapterCompatibilityWarning)
        pipeline._validate_consistency()
        pipeline._validate_consistency()

    assert len(_adapter_warnings(captured)) == 2
    issues = find_pipeline_adapter_compatibility_issues(pipeline)
    assert len(issues) == 2
    assert {issue.edge for issue in issues} == {
        "producer.default -> consumer.value",
        "other_producer.default -> other_consumer.value",
    }


def test_doctor_adapter_tag_check_ignores_warning_dedupe() -> None:
    pipeline = _two_mismatch_pipeline()

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always", AdapterCompatibilityWarning)
        pipeline._validate_consistency()
        pipeline._validate_consistency()

    result = check_pipeline_adapter_tags(pipeline)

    assert len(_adapter_warnings(captured)) == 2
    assert result.status == WARN
    assert "2 total mismatches" in result.detail
