from __future__ import annotations

import pytest

from spl.public import PublicObjectRef, format_public_ref, parse_public_ref


@pytest.mark.parametrize(
    ("text", "version"),
    [
        ("splime://@alice/image-tools/resize-image", None),
        ("splime://@alice/image-tools/resize-image@3", 3),
        ("splime://@alice/a.b/object_name@42", 42),
    ],
)
def test_public_reference_round_trip(text: str, version: int | None) -> None:
    parsed = parse_public_ref(text)
    assert parsed == PublicObjectRef(
        owner="alice",
        library=text.split("/")[3],
        name=text.split("/")[4].split("@")[0],
        version=version,
    )
    assert format_public_ref(parsed) == text


@pytest.mark.parametrize(
    "text",
    [
        "http://@alice/lib/object",
        "splime://alice/lib/object",
        "splime://@alice//object",
        "splime://@alice/../object",
        "splime://@alice/lib/..",
        "splime://@alice/lib/../object",
        "splime://@Alice/lib/object",
        "splime://@alice/lib/object@03",
        "splime://@alice/lib/object@latest",
        "splime://@alice/lib/object@3@4",
        "splime://@alice/lib/object?registry=https://evil.test",
        "splime://@alice/lib/object#fragment",
        "splime://user:password@alice/lib/object",
        "splime://@alice/lib/%6fbject",
    ],
)
def test_public_reference_rejects_ambiguous_or_unsafe_text(text: str) -> None:
    with pytest.raises(ValueError):
        parse_public_ref(text)


def test_public_reference_constructor_cannot_bypass_segment_validation() -> None:
    with pytest.raises(ValueError, match="library"):
        PublicObjectRef(owner="alice", library="..", name="object")
    with pytest.raises(ValueError, match="version"):
        PublicObjectRef(owner="alice", library="default", name="object", version=True)
