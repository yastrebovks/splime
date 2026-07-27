"""Best-effort redaction helpers for values leaving the local process."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from copy import deepcopy
from typing import Any

REDACTED_VALUE = "[REDACTED]"
SENSITIVE_KEY_FRAGMENTS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "credential",
        "authorization",
        "api_key",
        "apikey",
        "access_key",
        "private_key",
        "client_secret",
    }
)

_TEXT_PATTERNS = (
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(
        r"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----.*?"
        r"-----END(?: [A-Z0-9]+)? PRIVATE KEY-----",
        re.DOTALL,
    ),
    re.compile(r"(?im)^(\s*authorization\s*:\s*).+$"),
    re.compile(
        r"(?i)\b(password|passwd|pwd|api[_-]?key|token|client[_-]?secret|"
        r"access[_-]?key|secret)\s*([:=])\s*([^\s;&,]+)"
    ),
    re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^\s/:@]+:)([^\s/@]+)(@)"),
    re.compile(
        r"(?i)(?P<prefix>[\"'](?:password|passwd|pwd|api[_-]?key|token|"
        r"client[_-]?secret|access[_-]?key|secret)[\"']\s*:\s*)"
        r"(?P<quote>[\"'])(?P<value>.*?)(?P=quote)"
    ),
)


def is_sensitive_key(key: str) -> bool:
    """Return whether a mapping key commonly identifies a secret value."""

    normalized = key.casefold().replace("-", "_").replace(".", "_")
    return any(fragment in normalized for fragment in SENSITIVE_KEY_FRAGMENTS)


def value_looks_sensitive(value: Any, key_path: tuple[str, ...] = ()) -> bool:
    """Return whether a JSON-like value contains an obvious secret shape."""

    if any(is_sensitive_key(key) for key in key_path):
        return True
    if isinstance(value, Mapping):
        return any(is_sensitive_key(str(key)) or value_looks_sensitive(item) for key, item in value.items())
    if isinstance(value, list):
        return any(value_looks_sensitive(item) for item in value)
    if isinstance(value, str):
        return redact_text(value) != value
    return False


def normalize_json_pointers(pointers: Sequence[str] | None) -> tuple[str, ...]:
    """Validate and deduplicate RFC 6901 JSON Pointer paths."""

    normalized: list[str] = []
    for raw in pointers or ():
        pointer = str(raw)
        if not pointer.startswith("/"):
            raise ValueError("telemetry sensitive fields must be JSON Pointers beginning with '/'")
        _decode_pointer(pointer)
        if pointer not in normalized:
            normalized.append(pointer)
    return tuple(normalized)


def sensitive_values_at_pointers(
    value: Any,
    pointers: Sequence[str],
    *,
    max_values: int | None = None,
) -> tuple[str, ...]:
    """Return scalar string forms selected by explicit JSON Pointers."""

    _validate_max_values(max_values)
    selected: list[str] = []
    seen: set[str] = set()
    for pointer in normalize_json_pointers(pointers):
        try:
            current = _resolve_pointer(value, pointer)
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        for item in _scalar_strings(current):
            if item and item not in seen:
                if max_values is not None and len(selected) >= max_values:
                    return tuple(selected)
                seen.add(item)
                selected.append(item)
    return tuple(selected)


def sensitive_values_at_common_keys(
    value: Any,
    *,
    max_values: int | None = None,
) -> tuple[str, ...]:
    """Return string values nested below commonly sensitive mapping keys."""

    _validate_max_values(max_values)
    selected: list[str] = []
    seen: set[str] = set()

    def add(candidate: str) -> bool:
        if not candidate or candidate in seen:
            return True
        if max_values is not None and len(selected) >= max_values:
            return False
        seen.add(candidate)
        selected.append(candidate)
        return True

    def visit(current: Any) -> bool:
        if isinstance(current, Mapping):
            for key, item in current.items():
                if is_sensitive_key(str(key)):
                    for candidate in _string_values(item):
                        if not add(candidate):
                            return False
                elif not visit(item):
                    return False
        elif isinstance(current, list):
            for item in current:
                if not visit(item):
                    return False
        return True

    visit(value)
    return tuple(selected)


def redact_value(
    value: Any,
    *,
    sensitive_pointers: Sequence[str] = (),
    sensitive_values: Sequence[str] = (),
    discover_pointer_values: bool = True,
) -> Any:
    """Return a recursively redacted copy of a JSON-like value.

    Redaction is deliberately best effort. Callers requiring a hard privacy
    boundary must omit raw values rather than relying on this helper.
    """

    pointers = frozenset(normalize_json_pointers(sensitive_pointers))
    discovered = sensitive_values_at_pointers(value, tuple(pointers)) if discover_pointer_values else ()
    values = tuple(dict.fromkeys((*sensitive_values, *discovered)))
    return _redact_value(deepcopy(value), (), pointers, values)


def redact_text(text: str, *, sensitive_values: Sequence[str] = ()) -> str:
    """Redact common credential shapes and caller-marked values from text."""

    redacted = _replace_sensitive_values(str(text), sensitive_values)
    redacted = _TEXT_PATTERNS[0].sub("Bearer " + REDACTED_VALUE, redacted)
    redacted = _TEXT_PATTERNS[1].sub(REDACTED_VALUE, redacted)
    redacted = _TEXT_PATTERNS[2].sub(REDACTED_VALUE, redacted)
    redacted = _TEXT_PATTERNS[3].sub(r"\1" + REDACTED_VALUE, redacted)
    redacted = _TEXT_PATTERNS[4].sub(r"\1\2" + REDACTED_VALUE, redacted)
    redacted = _TEXT_PATTERNS[5].sub(r"\1" + REDACTED_VALUE + r"\3", redacted)
    redacted = _TEXT_PATTERNS[6].sub(_redact_json_secret_assignment, redacted)
    return redacted


def _replace_sensitive_values(text: str, sensitive_values: Sequence[str]) -> str:
    """Replace original sensitive spans once without scanning replacements.

    The previous repeated ``str.replace`` loop allowed a later sensitive value
    such as ``"REDACTED"`` or ``"["`` to match markers inserted by an earlier
    replacement.  Apart from corrupting the marker, carefully chosen values
    could multiply the output on every pass. A single literal-alternation
    substitution examines each original text segment once. Existing markers
    split those segments and are indivisible, already-redacted spans. Longest
    matches win deterministically.

    Pattern construction is bounded by the total caller-value size; telemetry
    independently caps its value count and source payload size. Output is at
    most ``n * len(REDACTED_VALUE)`` characters for ``n`` input characters.
    """

    candidates = sorted(
        {item for item in sensitive_values if item and item != REDACTED_VALUE},
        key=lambda item: (-len(item), item),
    )
    if not candidates:
        return text

    pattern = re.compile("|".join(re.escape(candidate) for candidate in candidates))
    return REDACTED_VALUE.join(pattern.sub(REDACTED_VALUE, segment) for segment in text.split(REDACTED_VALUE))


def _redact_json_secret_assignment(match: re.Match[str]) -> str:
    quote = match.group("quote")
    return f"{match.group('prefix')}{quote}{REDACTED_VALUE}{quote}"


def _redact_value(
    value: Any,
    path: tuple[str, ...],
    pointers: frozenset[str],
    sensitive_values: Sequence[str],
) -> Any:
    pointer = _encode_pointer(path)
    if pointer in pointers or (path and is_sensitive_key(path[-1])):
        return REDACTED_VALUE
    if isinstance(value, Mapping):
        return {
            str(key): _redact_value(
                item,
                (*path, str(key)),
                pointers,
                sensitive_values,
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _redact_value(item, (*path, str(index)), pointers, sensitive_values) for index, item in enumerate(value)
        ]
    if isinstance(value, str):
        return redact_text(value, sensitive_values=sensitive_values)
    return value


def _decode_pointer(pointer: str) -> tuple[str, ...]:
    if pointer == "":
        return ()
    parts = pointer[1:].split("/")
    decoded: list[str] = []
    for part in parts:
        index = 0
        while index < len(part):
            if part[index] == "~":
                if index + 1 >= len(part) or part[index + 1] not in {"0", "1"}:
                    raise ValueError("telemetry sensitive field contains an invalid JSON Pointer escape")
                index += 2
            else:
                index += 1
        decoded.append(part.replace("~1", "/").replace("~0", "~"))
    return tuple(decoded)


def _encode_pointer(parts: tuple[str, ...]) -> str:
    if not parts:
        return ""
    return "/" + "/".join(part.replace("~", "~0").replace("/", "~1") for part in parts)


def _resolve_pointer(value: Any, pointer: str) -> Any:
    current = value
    for part in _decode_pointer(pointer):
        if isinstance(current, Mapping):
            current = current[part]
        elif isinstance(current, list):
            if part == "-":
                raise IndexError(part)
            current = current[int(part)]
        else:
            raise TypeError("JSON Pointer traverses a scalar value")
    return current


def _scalar_strings(value: Any) -> Iterator[str]:
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _scalar_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _scalar_strings(item)
    elif value is not None:
        yield str(value)


def _string_values(value: Any) -> Iterator[str]:
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _string_values(item)
    elif isinstance(value, list):
        for item in value:
            yield from _string_values(item)
    elif isinstance(value, str):
        yield value


def _validate_max_values(max_values: int | None) -> None:
    if max_values is not None and (type(max_values) is not int or max_values < 0):
        raise ValueError("max_values must be a non-negative integer or None")
