"""Adversarial bounds for shared text redaction."""

from __future__ import annotations

from spl.core.redaction import REDACTED_VALUE, redact_text


def test_sensitive_replacements_never_reprocess_or_expand_redaction_markers() -> None:
    text = "secret {} secret".format(REDACTED_VALUE)

    redacted = redact_text(
        text,
        sensitive_values=("secret", "REDACTED", "[", "]", REDACTED_VALUE),
    )

    assert redacted == "{} {} {}".format(REDACTED_VALUE, REDACTED_VALUE, REDACTED_VALUE)


def test_sensitive_replacement_output_has_a_strict_linear_bound() -> None:
    text = "x" * (32 * 1024)

    redacted = redact_text(text, sensitive_values=("x", "REDACTED", "[", "]"))

    assert redacted == REDACTED_VALUE * len(text)
    assert len(redacted) == len(text) * len(REDACTED_VALUE)


def test_reported_overlapping_cascade_values_cannot_reprocess_markers() -> None:
    values = (
        "SECRET",
        "REDACTE",
        "REDACT",
        "REDA",
        "RED",
        "RE",
        "E",
        "D",
        "A",
        "C",
        "T",
        "[",
        "]",
    )
    text = "SECRET" * 1000

    redacted = redact_text(text, sensitive_values=values)

    assert redacted == REDACTED_VALUE * 1000
    assert len(redacted) == 1000 * len(REDACTED_VALUE)


def test_longest_original_sensitive_span_wins_once() -> None:
    assert redact_text(
        "abcdef abc",
        sensitive_values=("abc", "abcdef"),
    ) == "{} {}".format(REDACTED_VALUE, REDACTED_VALUE)
