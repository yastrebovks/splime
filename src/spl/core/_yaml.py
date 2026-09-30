"""Optional PyYAML registration seam for the execution-only authority.

The installed ``splime`` distribution requires PyYAML and therefore observes
the ordinary module unchanged.  The signed public worker executes producer-
compiled Python and never parses or emits YAML; its embedded first-party
authority can consequently import the runtime entity types without acquiring
an unsigned YAML implementation.
"""

from __future__ import annotations

from typing import Any

_yaml: Any
try:
    import yaml as _yaml
except ModuleNotFoundError:
    _yaml = None


class _ExecutionSafeLoader:
    @classmethod
    def add_constructor(cls, *args: Any, **kwargs: Any) -> None:
        del cls, args, kwargs


class _ExecutionOnlyYaml:
    """Accept inert entity registrations and reject every YAML operation."""

    Dumper = object
    Node = object
    SafeLoader = _ExecutionSafeLoader

    @staticmethod
    def add_constructor(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    @staticmethod
    def add_representer(*args: Any, **kwargs: Any) -> None:
        del args, kwargs

    def __getattr__(self, name: str) -> Any:
        raise ModuleNotFoundError(
            "PyYAML is required for SPL serialization; the signed public worker "
            "accepts only producer-compiled execution members"
        ) from None


yaml: Any = _yaml if _yaml is not None else _ExecutionOnlyYaml()

__all__ = ["yaml"]
