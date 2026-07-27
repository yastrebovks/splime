"""Recursive JSON value validation and fail-closed serialization."""

from __future__ import annotations

import json
import math
from typing import Any

JSON_SCALARS: frozenset[type[Any]] = frozenset({type(None), bool, int, float, str})


def is_json_value(value: Any) -> bool:
    """Return whether ``value`` satisfies the recursive splime JSON contract."""

    try:
        validate_json_value(value)
    except (RecursionError, ValueError):
        return False
    return True


def validate_json_value(value: Any, *, path: str = "$") -> None:
    """Raise a path-annotated error for the first JSON contract violation."""

    if not isinstance(path, str) or not path:
        raise ValueError("JSON validation path must be a non-empty string")
    _validate_json_value(value, path=path, active_container_ids=set())


def dumps(
    value: Any,
    *,
    ensure_ascii: bool = False,
    indent: int | str | None = None,
    sort_keys: bool = True,
    separators: tuple[str, str] | None = (",", ":"),
) -> str:
    """Serialize one valid JSON value with non-finite numbers disabled."""

    validate_json_value(value)
    return json.dumps(
        value,
        ensure_ascii=ensure_ascii,
        indent=indent,
        sort_keys=sort_keys,
        separators=separators,
        allow_nan=False,
    )


def compact_json_fits(value: Any, max_bytes: int) -> bool:
    """Return whether compact UTF-8 JSON fits without traversing past the budget.

    For a valid value, this is equivalent to measuring
    ``dumps(value, ensure_ascii=False, sort_keys=False).encode("utf-8")``. The
    compact encoding is measured incrementally instead of being materialized.

    Values encountered before the size limit are validated against the same
    recursive contract as :func:`dumps`. Once the encoded size is known to
    exceed ``max_bytes``, the remaining value is deliberately not traversed;
    therefore, an invalid value beyond that point does not replace the
    ``False`` overflow result with a validation error. Callers use this only to
    omit an oversized component, never to accept or persist an otherwise
    unvalidated value.
    """

    if type(max_bytes) is not int or max_bytes < 0:
        raise ValueError("max_bytes must be a non-negative integer")

    class SizeLimitExceeded(Exception):
        pass

    remaining = max_bytes
    active_container_ids: set[int] = set()

    def consume(size: int) -> None:
        nonlocal remaining
        if size > remaining:
            raise SizeLimitExceeded
        remaining -= size

    def measure_string(text: str, *, path: str, role: str) -> None:
        consume(2)
        for index, character in enumerate(text):
            code_point = ord(character)
            if 0xD800 <= code_point <= 0xDFFF:
                raise ValueError(
                    "invalid splime JSON value at {}: {} contains Unicode surrogate "
                    "U+{:04X} at character {}; use Unicode scalar values".format(
                        path,
                        role,
                        code_point,
                        index,
                    )
                )
            if character in {'"', "\\", "\b", "\f", "\n", "\r", "\t"}:
                consume(2)
            elif code_point < 0x20:
                consume(6)
            else:
                consume(len(character.encode("utf-8")))

    def measure(current: Any, *, path: str) -> None:
        value_type = type(current)
        if current is None:
            consume(4)
            return
        if value_type is bool:
            consume(4 if current else 5)
            return
        if value_type is int:
            consume(len(str(current).encode("ascii")))
            return
        if value_type is float:
            if not math.isfinite(current):
                raise ValueError(
                    "invalid splime JSON value at {}: non-finite float {} is not permitted; "
                    "use a finite number or null".format(path, _non_finite_name(current))
                )
            consume(len(json.dumps(current, allow_nan=False).encode("ascii")))
            return
        if value_type is str:
            measure_string(current, path=path, role="string")
            return
        if value_type is list:
            container_id = _enter_container(
                current,
                path=path,
                active_container_ids=active_container_ids,
            )
            try:
                consume(1)
                for index, item in enumerate(current):
                    if index:
                        consume(1)
                    measure(item, path="{}[{}]".format(path, index))
                consume(1)
            finally:
                active_container_ids.remove(container_id)
            return
        if value_type is dict:
            container_id = _enter_container(
                current,
                path=path,
                active_container_ids=active_container_ids,
            )
            try:
                consume(1)
                for index, (key, item) in enumerate(current.items()):
                    if type(key) is not str:
                        raise ValueError(
                            "invalid splime JSON value at {}: object key {!r} has type `{}.{}`; "
                            "JSON object keys must be strings".format(
                                path,
                                key,
                                type(key).__module__,
                                type(key).__qualname__,
                            )
                        )
                    if index:
                        consume(1)
                    measure_string(key, path=path, role="object key")
                    consume(1)
                    measure(item, path=_object_item_path(path, key))
                consume(1)
            finally:
                active_container_ids.remove(container_id)
            return
        raise ValueError(
            "invalid splime JSON value at {}: value type `{}.{}` is not supported; "
            "use null, bool, int, finite float, str, list, or dict".format(
                path,
                value_type.__module__,
                value_type.__qualname__,
            )
        )

    try:
        measure(value, path="$")
    except SizeLimitExceeded:
        return False
    return True


def _validate_json_value(value: Any, *, path: str, active_container_ids: set[int]) -> None:
    value_type = type(value)
    if value_type in JSON_SCALARS:
        if value_type is float and not math.isfinite(value):
            raise ValueError(
                "invalid splime JSON value at {}: non-finite float {} is not permitted; "
                "use a finite number or null".format(path, _non_finite_name(value))
            )
        if value_type is str:
            _validate_unicode_scalar_string(value, path=path, role="string")
        return

    if value_type is list:
        _validate_list(value, path=path, active_container_ids=active_container_ids)
        return

    if value_type is dict:
        _validate_dict(value, path=path, active_container_ids=active_container_ids)
        return

    raise ValueError(
        "invalid splime JSON value at {}: value type `{}.{}` is not supported; "
        "use null, bool, int, finite float, str, list, or dict".format(
            path,
            value_type.__module__,
            value_type.__qualname__,
        )
    )


def _validate_list(value: list[Any], *, path: str, active_container_ids: set[int]) -> None:
    container_id = _enter_container(value, path=path, active_container_ids=active_container_ids)
    try:
        for index, item in enumerate(value):
            _validate_json_value(item, path="{}[{}]".format(path, index), active_container_ids=active_container_ids)
    finally:
        active_container_ids.remove(container_id)


def _validate_dict(value: dict[Any, Any], *, path: str, active_container_ids: set[int]) -> None:
    container_id = _enter_container(value, path=path, active_container_ids=active_container_ids)
    try:
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError(
                    "invalid splime JSON value at {}: object key {!r} has type `{}.{}`; "
                    "JSON object keys must be strings".format(
                        path,
                        key,
                        type(key).__module__,
                        type(key).__qualname__,
                    )
                )
            _validate_unicode_scalar_string(key, path=path, role="object key")
            _validate_json_value(
                item,
                path=_object_item_path(path, key),
                active_container_ids=active_container_ids,
            )
    finally:
        active_container_ids.remove(container_id)


def _enter_container(value: list[Any] | dict[Any, Any], *, path: str, active_container_ids: set[int]) -> int:
    container_id = id(value)
    if container_id in active_container_ids:
        raise ValueError("invalid splime JSON value at {}: circular container reference is not permitted".format(path))
    active_container_ids.add(container_id)
    return container_id


def _object_item_path(path: str, key: str) -> str:
    return "{}[{}]".format(path, json.dumps(key, ensure_ascii=False))


def _non_finite_name(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if value > 0:
        return "Infinity"
    return "-Infinity"


def _validate_unicode_scalar_string(value: str, *, path: str, role: str) -> None:
    for index, character in enumerate(value):
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDFFF:
            raise ValueError(
                "invalid splime JSON value at {}: {} contains Unicode surrogate "
                "U+{:04X} at character {}; use Unicode scalar values".format(
                    path,
                    role,
                    code_point,
                    index,
                )
            )
