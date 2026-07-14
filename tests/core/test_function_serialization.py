from __future__ import annotations

import ast
import inspect
import textwrap
from pathlib import Path
from types import FunctionType
from typing import Any

import pytest
import yaml

from spl import SPLClient, lift
from spl.core.entities.function import (
    DFunction,
    _ir_unparse__function,
    get_dependencies_from_bytecode,
    get_dependency_names_from_bytecode,
    serialize_function,
)
from spl.core.ir.utils import SPLSafeLoader, spl_import_from_file


def _supported_plain(a, b):
    return a + b


def _supported_defaults(a, b=2):
    return a * b


def _supported_annotations(a: int, label: str = "value") -> str:
    return f"{label}:{a}"


def _supported_default_expressions(a=(1 + 2) * 3, b={"values": [1, 2]}):
    return a, b


def _supported_generator_expression(items, threshold):
    return tuple(item for item in items if item >= threshold)


def _supported_parameter_comprehensions(items):
    return (
        [item + 1 for item in items],
        {item + 2 for item in items},
        {item: item + 3 for item in items},
    )


def _supported_nested_comprehension(rows):
    offset = 4
    return [[item + offset for item in row] for row in rows]


def _supported_body_lambda(items, offset):
    return sorted(items, key=lambda item: item + offset)


_GLOBAL_COMPREHENSION_FACTOR = 5


def _supported_comprehension_with_global(items, threshold):
    return [item * _GLOBAL_COMPREHENSION_FACTOR for item in items if item >= threshold]


def _unsupported_posonly(a, /):
    return a


def _unsupported_kwonly_required(*, value):
    return value


def _unsupported_kwonly_default(*, value=1):
    return value


def _unsupported_varargs(*items):
    return items


def _unsupported_kwargs(**extra):
    return extra


def _unsupported_posonly_kwonly(a, /, *, b):
    return a, b


def _unsupported_posonly_varargs(a, /, *items):
    return a, items


def _unsupported_posonly_kwargs(a, /, **extra):
    return a, extra


def _unsupported_kwonly_varargs(*items, value):
    return items, value


def _unsupported_kwonly_kwargs(*, value, **extra):
    return value, extra


def _unsupported_varargs_kwargs(*items, **extra):
    return items, extra


def _sol_006_audit_signature(a, /, b=2, *items, c=3, **extra):
    return a, b, items, c, extra


def _make_direct_closure() -> FunctionType:
    captured = 7

    def direct_closure(value):
        return value + captured

    return direct_closure


_DIRECT_CLOSURE = _make_direct_closure()


def _nested_closure(value):
    captured = 3

    def add_captured():
        return value + captured

    return add_captured()


_NAMED_LAMBDA = lambda value: value + 1  # noqa: E731


_SUPPORTED_CASES = (
    pytest.param(_supported_plain, (2, 3), {}, id="plain-args"),
    pytest.param(_supported_defaults, (4,), {}, id="args-and-defaults"),
    pytest.param(_supported_annotations, (5,), {"label": "score"}, id="annotations"),
    pytest.param(_supported_generator_expression, ((1, 4, 7), 4), {}, id="generator-over-parameter"),
    pytest.param(_supported_parameter_comprehensions, ((1, 2, 3),), {}, id="list-set-dict-comprehensions"),
    pytest.param(_supported_nested_comprehension, (((1, 2), (3,)),), {}, id="nested-comprehension-over-local"),
    pytest.param(_nested_closure, (5,), {}, id="inner-def-over-parameter-and-local"),
    pytest.param(_supported_body_lambda, ((3, 1, 2), 10), {}, id="body-lambda-over-parameter"),
    pytest.param(
        _supported_comprehension_with_global,
        ((1, 3, 5), 3),
        {},
        id="comprehension-with-real-global",
    ),
)

_UNSUPPORTED_CASES = (
    pytest.param(_unsupported_posonly, ("positional-only",), id="posonly"),
    pytest.param(_unsupported_kwonly_required, ("keyword-only",), id="kwonly-required"),
    pytest.param(_unsupported_kwonly_default, ("keyword-only",), id="kwonly-default"),
    pytest.param(_unsupported_varargs, ("*args",), id="varargs"),
    pytest.param(_unsupported_kwargs, ("**kwargs",), id="kwargs"),
    pytest.param(
        _unsupported_posonly_kwonly,
        ("positional-only", "keyword-only"),
        id="posonly-and-kwonly",
    ),
    pytest.param(_unsupported_posonly_varargs, ("positional-only", "*args"), id="posonly-and-varargs"),
    pytest.param(_unsupported_posonly_kwargs, ("positional-only", "**kwargs"), id="posonly-and-kwargs"),
    pytest.param(_unsupported_kwonly_varargs, ("keyword-only", "*args"), id="kwonly-and-varargs"),
    pytest.param(_unsupported_kwonly_kwargs, ("keyword-only", "**kwargs"), id="kwonly-and-kwargs"),
    pytest.param(_unsupported_varargs_kwargs, ("*args", "**kwargs"), id="varargs-and-kwargs"),
    pytest.param(
        _sol_006_audit_signature,
        ("positional-only", "keyword-only", "*args", "**kwargs"),
        id="audit-all-four",
    ),
    pytest.param(_DIRECT_CLOSURE, ("captured",), id="closure-over-local"),
)


class _RecordingDaemon:
    def __init__(self) -> None:
        self.register_object_calls: list[dict[str, Any]] = []

    def register_object(self, name: str, **kwargs: Any) -> dict[str, Any]:
        self.register_object_calls.append({"name": name, **kwargs})
        return {
            "name": name,
            "entrypoint": kwargs["entrypoint"],
            "env": kwargs["env"],
            "yaml_path": "unexpected.yaml",
        }


def _publish_error(func: FunctionType) -> tuple[ValueError, _RecordingDaemon]:
    client = SPLClient(daemon_port=8765)
    daemon = _RecordingDaemon()
    client._daemon = daemon  # type: ignore[assignment]

    with pytest.raises(ValueError) as raised:
        client.publish(func)

    return raised.value, daemon


def _round_trip_function(tmp_path: Path, func: FunctionType) -> tuple[DFunction, FunctionType]:
    payload = serialize_function(func)
    yaml_text = yaml.dump([payload], sort_keys=False, allow_unicode=True)
    loaded = yaml.load(yaml_text, Loader=SPLSafeLoader)
    assert loaded == [payload]

    path = tmp_path / "function.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    dependency_names = set(get_dependency_names_from_bytecode(func))
    namespace: dict[str, Any] = {
        "__name__": "__main__",
        **{name: func.__globals__[name] for name in dependency_names if name in func.__globals__},
    }
    spl_import_from_file(path, globals=namespace)
    return payload, namespace[payload.name]


def test_serialize_function_accepts_nested_definition() -> None:
    # Regression: ``inspect.getsource`` keeps the original indentation, so a
    # function defined inside a block (a notebook ``if RUN_*:`` guard, another
    # function body) failed with ``IndentationError: unexpected indent``
    # before the ``textwrap.dedent`` fix.
    if True:

        def nested_probe(x: int, factor: float = 1.5) -> float:
            return x * factor

    payload = serialize_function(nested_probe)
    assert payload.name == "nested_probe"
    assert [port.name for port in payload.inputs] == ["x", "factor"]
    assert payload.inputs[0].typ_ == "int"
    assert payload.inputs[1].default == "1.5"
    assert "return x * factor" in payload.body


def test_lift_accepts_nested_definition() -> None:
    # Same regression through the public entry point used by notebooks.
    if True:

        def nested_double(x: int) -> int:
            return 2 * x

    node = lift(nested_double)  # raised IndentationError before the fix
    assert node is not None


@pytest.mark.parametrize(("func", "args", "kwargs"), _SUPPORTED_CASES)
def test_supported_signature_matrix_round_trips_and_invokes_equally(
    tmp_path: Path,
    func: FunctionType,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> None:
    payload, rebuilt = _round_trip_function(tmp_path, func)

    original_signature = inspect.signature(func)
    rebuilt_signature = inspect.signature(rebuilt)
    assert list(rebuilt_signature.parameters) == list(original_signature.parameters)
    assert [parameter.kind for parameter in rebuilt_signature.parameters.values()] == [
        parameter.kind for parameter in original_signature.parameters.values()
    ]
    assert [parameter.default for parameter in rebuilt_signature.parameters.values()] == [
        parameter.default for parameter in original_signature.parameters.values()
    ]
    assert rebuilt(*args, **kwargs) == func(*args, **kwargs)
    assert payload.name == func.__name__


def test_supported_annotations_round_trip_exactly() -> None:
    payload = serialize_function(_supported_annotations)

    assert [(port.name, port.typ_, port.default) for port in payload.inputs] == [
        ("a", "int", None),
        ("label", "str", "'value'"),
    ]
    assert payload.outputs is not None
    assert [(port.name, port.typ_) for port in payload.outputs] == [("default", "str")]


def test_supported_function_yaml_bytes_keep_legacy_shape() -> None:
    assert yaml.dump([serialize_function(_supported_defaults)], sort_keys=False, allow_unicode=True) == textwrap.dedent(
        """\
        - !DFunction
          name: _supported_defaults
          inputs:
          - name: a
            type: null
            default: null
          - name: b
            type: null
            default: '2'
          outputs:
          - name: default
            type: null
          body: |-
            return a * b
        """
    )


def test_supported_default_expressions_keep_exact_ast_alignment(tmp_path: Path) -> None:
    source_node = ast.parse(textwrap.dedent(inspect.getsource(_supported_default_expressions))).body[0]
    assert isinstance(source_node, ast.FunctionDef)
    payload = serialize_function(_supported_default_expressions, source_node)
    rebuilt_node = next(_ir_unparse__function(payload, tmp_path / "function.yaml"))
    assert isinstance(rebuilt_node, ast.FunctionDef)

    assert [ast.unparse(default) for default in rebuilt_node.args.defaults] == [
        ast.unparse(default) for default in source_node.args.defaults
    ]
    assert [port.default for port in payload.inputs] == ["(1 + 2) * 3", "{'values': [1, 2]}"]


@pytest.mark.parametrize(("func", "expected_fragments"), _UNSUPPORTED_CASES)
def test_unsupported_signature_matrix_rejects_before_registration(
    func: FunctionType,
    expected_fragments: tuple[str, ...],
) -> None:
    error, daemon = _publish_error(func)
    message = str(error)

    assert func.__name__ in message
    assert all(fragment in message for fragment in expected_fragments)
    assert daemon.register_object_calls == []


@pytest.mark.parametrize("func", [_sol_006_audit_signature, _DIRECT_CLOSURE])
def test_lift_rejects_unsupported_live_functions(func: FunctionType) -> None:
    with pytest.raises(ValueError):
        lift(func)


def test_dependency_scanner_distinguishes_closures_from_inner_scope_cells() -> None:
    assert "captured" in set(get_dependency_names_from_bytecode(_DIRECT_CLOSURE))
    assert {"captured", "value"}.isdisjoint(get_dependency_names_from_bytecode(_nested_closure))

    names = set(get_dependency_names_from_bytecode(_supported_comprehension_with_global))
    assert "_GLOBAL_COMPREHENSION_FACTOR" in names
    assert {"items", "threshold"}.isdisjoint(names)


def test_dependency_resolution_does_not_treat_generator_parameters_as_globals() -> None:
    assert list(get_dependencies_from_bytecode(0, _supported_generator_expression)) == []


def test_named_lambda_source_shape_remains_unsupported() -> None:
    # inspect.getsource returns the assignment, not a FunctionDef.  Lambdas are
    # outside the 0.4.x serializer and this task deliberately does not add them.
    with pytest.raises(AttributeError, match="has no attribute 'args'"):
        serialize_function(_NAMED_LAMBDA)


def test_sol_006_audit_signature_rejects_all_unsupported_parameter_kinds() -> None:
    error, daemon = _publish_error(_sol_006_audit_signature)
    message = str(error)
    assert "_sol_006_audit_signature" in message
    assert "positional-only" in message
    assert "keyword-only" in message
    assert "*args" in message
    assert "**kwargs" in message
    assert "plain positional-or-keyword parameters only" in message
    assert "refactor" in message
    assert daemon.register_object_calls == []
