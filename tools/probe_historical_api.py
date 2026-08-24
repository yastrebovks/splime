"""Emit raw, structural and behavioral evidence for one installed SPLime wheel."""

from __future__ import annotations

import argparse
import importlib.metadata
import inspect
import json
import platform
import sys
from types import MethodType
from collections.abc import Mapping, Sequence
from typing import Any


PROBE_SCHEMA = "splime.historical_api_probe.v2"
PROBE_VERSION = 2
CLIENT_METHODS = (
    "start",
    "queue",
    "local_objects",
    "server_objects",
    "objects",
    "create_library",
    "get_library",
    "update_library",
    "delete_library",
    "grant_library",
    "revoke_library_grant",
    "add_reference",
    "copy_object",
    "remove_entry",
    "run_node",
    "run_node_result",
    "submit",
    "publish",
    "publish_yaml",
)


def _probe_publication_function(value: int) -> int:
    """Small source-backed callable used to exercise the real publisher."""

    return value + 1


BEHAVIOR_FORMS: dict[str, tuple[tuple[Any, ...], dict[str, Any]]] = {
    "start": (("probe",), {}),
    "queue": (("probe",), {"target_machine": "probe-machine"}),
    "local_objects": ((), {"compact": True}),
    "server_objects": ((), {"owner": "probe-owner", "compact": True}),
    "objects": ((), {"compact": True, "scope": "local"}),
    "create_library": (("probe",), {}),
    "get_library": (("probe",), {}),
    "update_library": (("probe",), {"description": "probe"}),
    "delete_library": (("probe",), {}),
    "grant_library": (("probe", "reader"), {}),
    "revoke_library_grant": (("probe", "reader"), {}),
    "add_reference": (("probe", "entry"), {}),
    "copy_object": (("entry",), {"into_library": "probe"}),
    "remove_entry": (("probe", "entry"), {}),
    "run_node": ((None, {}), {}),
    "run_node_result": ((None,), {}),
    "submit": (("probe",), {}),
    "publish": ((_probe_publication_function,), {"name": "probe"}),
    "publish_yaml": (
        ("probe: !DFunction\n  source: |\n    def probe(value):\n        return value + 1\n",),
        {"name": "probe", "entrypoint": "probe"},
    ),
}


def _raw_signature(value: Any) -> str | None:
    try:
        return str(inspect.signature(value))
    except (TypeError, ValueError):
        return None


def _stable_repr(value: Any) -> str:
    if value is inspect.Parameter.empty:
        return "<empty>"
    if value is None or isinstance(value, (bool, int, float, str)):
        return repr(value)
    return f"<{type(value).__module__}.{type(value).__qualname__}>"


def _structural_signature(value: Any) -> dict[str, Any] | None:
    try:
        signature = inspect.signature(value)
    except (TypeError, ValueError):
        return None
    return {
        "parameters": [
            {
                "name": parameter.name,
                "kind": parameter.kind.name,
                "required": parameter.default is inspect.Parameter.empty,
                "default": _stable_repr(parameter.default),
            }
            for parameter in signature.parameters.values()
        ]
    }


def _annotation_evidence(value: Any) -> dict[str, Any] | None:
    try:
        signature = inspect.signature(value)
    except (TypeError, ValueError):
        return None
    return {
        "parameters": [
            {
                "name": parameter.name,
                "annotation": _stable_repr(parameter.annotation),
            }
            for parameter in signature.parameters.values()
        ],
        "return": _stable_repr(signature.return_annotation),
    }


def _shape(value: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
    }
    if isinstance(value, Mapping):
        result["mapping_keys"] = sorted(str(key) for key in value)
    elif isinstance(value, (list, tuple)):
        result["sequence_length"] = len(value)
    elif value is None or isinstance(value, (bool, int, float, str)):
        result["scalar"] = value
    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, Mapping):
        result["attribute_names"] = sorted(str(key) for key in attributes)
    return result


class _Universal:
    """Deterministic recording double used as the receiver's collaborators."""

    def __init__(
        self,
        path: str = "self",
        trace: list[dict[str, Any]] | None = None,
        client_type: type[Any] | None = None,
    ) -> None:
        self._path = path
        self._trace = trace if trace is not None else []
        self._client_type = client_type

    def __getattr__(self, name: str) -> Any:
        if self._client_type is not None:
            if name == "_resolve_run_adapter_selectors":
                return lambda adapters, **kwargs: adapters
            if name == "_has_server_connection":
                return lambda: False
            if name == "_object_records":
                candidate = inspect.getattr_static(self._client_type, name, None)
                if isinstance(candidate, staticmethod):
                    return candidate.__func__
            if name in {
                "objects",
                "start",
                "submit",
                "_start_run",
                "_run_node_value",
                "_publish_description_intent",
                "_server_identity_present",
            }:
                candidate = getattr(self._client_type, name, None)
                if callable(candidate):
                    return MethodType(candidate, self)
        return _Universal(f"{self._path}.{name}", self._trace)

    def __call__(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self._trace.append(
            {
                "path": self._path,
                "positional_count": len(args),
                "keyword_names": sorted(kwargs),
            }
        )
        if self._path.endswith(".run_remote_node"):
            return {
                "value": 7,
                "payload": {"result": 7, "artifacts": {}},
                "run": {"id": "probe-run", "status": "succeeded"},
                "artifacts": {},
            }
        if self._path.endswith(".run"):
            return {"id": "probe-run", "status": "queued"}
        if self._path.endswith(".list_objects"):
            return {"probe": {"name": "probe", "kind": "function", "version": 1}}
        if self._path.endswith(".server_objects"):
            return [{"name": "probe", "kind": "function", "version": 1}]
        if self._path.endswith(".register_object"):
            return {
                "name": str(args[0]) if args else "probe",
                "entrypoint": str(kwargs.get("entrypoint") or "probe"),
                "env": str(kwargs.get("env") or "default"),
                "yaml_path": "/tmp/splime-historical-probe/probe.yaml",
                "workdir": kwargs.get("workdir"),
            }
        if self._path.endswith(".supports_public_profile_capability"):
            return False
        return {"id": "probe", "name": "probe", "status": "ok"}

    def __bool__(self) -> bool:
        return False

    def __iter__(self):
        return iter(())

    def __len__(self) -> int:
        return 0

    def __str__(self) -> str:
        return "probe"

    def __fspath__(self) -> str:
        return "/tmp/splime-historical-probe"


def _behavior(method_name: str, method: Any, client_type: type[Any]) -> dict[str, Any]:
    raw_args, kwargs = BEHAVIOR_FORMS[method_name]
    trace: list[dict[str, Any]] = []
    receiver = _Universal(trace=trace, client_type=client_type)
    args = tuple(_Universal("node", trace) if value is None else value for value in raw_args)
    try:
        bound = inspect.signature(method).bind(receiver, *args, **kwargs)
        call_binding = {
            "status": "PASS",
            "bound_parameters": list(bound.arguments),
        }
    except TypeError as exc:
        return {
            "status": "FAIL",
            "call_binding": {"status": "FAIL", "error_type": type(exc).__name__},
            "call": {"status": "NOT_RUN"},
            "error_path": {"status": "NOT_RUN"},
        }
    try:
        returned = method(receiver, *args, **kwargs)
    except Exception as exc:  # noqa: BLE001 - historical probe records the boundary.
        call = {
            "status": "FAIL",
            "error_type": type(exc).__name__,
            "message": str(exc)[:240],
            "trace": trace,
        }
    else:
        call = {"status": "PASS", "return_shape": _shape(returned), "trace": trace}
    signature = inspect.signature(method)
    required = [
        parameter
        for index, parameter in enumerate(signature.parameters.values())
        if index > 0
        and parameter.default is inspect.Parameter.empty
        and parameter.kind not in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
    ]
    error_category = "missing_required" if required else "unexpected_keyword"
    try:
        if required:
            method(receiver)
        else:
            method(receiver, __splime_probe_unexpected__=True)
    except TypeError as exc:
        error_path = {"status": "PASS", "error_type": type(exc).__name__, "category": error_category}
    except Exception as exc:  # noqa: BLE001 - a body error is evidence, but not the intended contract error.
        error_path = {"status": "FAIL", "error_type": type(exc).__name__, "category": "body_error"}
    else:
        error_path = {"status": "FAIL", "error_type": None, "category": "unexpected_keyword_accepted"}
    status = "PASS" if call["status"] == "PASS" and error_path["status"] == "PASS" else "FAIL"
    return {
        "status": status,
        "call_binding": call_binding,
        "call": call,
        "error_path": error_path,
    }


def build_snapshot() -> dict[str, Any]:
    """Return deterministic artifact truth without constructing a real client."""

    import spl

    client_type = spl.SPLClient
    callables = {"SPLClient": client_type}
    callables.update(
        {f"SPLClient.{name}": getattr(client_type, name) for name in CLIENT_METHODS if hasattr(client_type, name)}
    )
    behavior: dict[str, Any] = {}
    for name in BEHAVIOR_FORMS:
        method = getattr(client_type, name, None)
        behavior[name] = (
            _behavior(name, method, client_type)
            if method is not None
            else {"status": "NOT_APPLICABLE", "reason": "callable absent in published artifact"}
        )
    module_exports = getattr(spl, "__all__", ())
    return {
        "schema": PROBE_SCHEMA,
        "schema_version": PROBE_VERSION,
        "distribution": "splime",
        "version": importlib.metadata.version("splime"),
        "interpreter": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
            "cache_tag": sys.implementation.cache_tag,
        },
        "requires_python": importlib.metadata.metadata("splime").get("Requires-Python"),
        "module_exports": sorted(str(item) for item in module_exports),
        "raw_signatures": {name: _raw_signature(value) for name, value in callables.items()},
        "structural_signatures": {name: _structural_signature(value) for name, value in callables.items()},
        "annotation_evidence": {name: _annotation_evidence(value) for name, value in callables.items()},
        "behavioral_results": behavior,
        "callable_presence": {name: hasattr(client_type, name) for name in CLIENT_METHODS},
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", choices=("pretty", "compact"), default="pretty")
    args = parser.parse_args(argv)
    indent = 2 if args.output == "pretty" else None
    print(json.dumps(build_snapshot(), indent=indent, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
