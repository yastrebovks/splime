from __future__ import annotations

from uuid import uuid4

import pytest
import yaml

import spl.daemon_client as daemon_client
from spl import Deployment, SPLClient, lift
from spl.core.entities.function import DFunction, METADATA_DUNDER_NAME
from spl.core.entities.node import (
    DNodeInputRef,
    InputPort,
    NodeInputRef,
    NodeOutputRef,
    OutputPort,
)
from spl.core.entities.node_function import DNodeFunction, NodeFunction
from spl.core.entities.node_remote import NodeRemote
from spl.core.entities.pipeline import DPipeline, Pipeline
from spl.core.entities.scalar import DScalar, Scalar
from spl.core.ir.utils import mk_top_level_deps_closure, spl_import_from_file


def _attach_metadata(
    func,
    *,
    inputs: list[InputPort] | None = None,
    outputs: list[OutputPort] | None = None,
):
    setattr(
        func,
        METADATA_DUNDER_NAME,
        DFunction(
            name=func.__name__,
            body="return None",
            inputs=inputs or [],
            outputs=outputs or [OutputPort("default", None)],
        ),
    )
    return func


def test_pipeline_rejects_duplicate_link_targets() -> None:
    def add_one(x):
        return x + 1

    _attach_metadata(
        add_one,
        inputs=[InputPort("x", "int", None)],
        outputs=[OutputPort("default", "int")],
    )
    node = NodeFunction(add_one)
    ref = NodeInputRef(node, node.get_input_port("x"))
    pipeline = Pipeline(nodes={node}).add_link(ref, Scalar(1))

    with pytest.raises(ValueError, match="already linked"):
        pipeline.add_link(ref, Scalar(2))


def test_pipeline_rejects_alias_conflicts_and_unknown_alias_nodes() -> None:
    def left():
        return 1

    def right():
        return 2

    _attach_metadata(left)
    _attach_metadata(right)
    left_node = NodeFunction(left)
    right_node = NodeFunction(right)

    with pytest.raises(ValueError, match="unknown node"):
        Pipeline(nodes={left_node}).add_alias(right_node, "score")

    left_pipeline = Pipeline(nodes={left_node}).add_alias(left_node, "score")
    right_pipeline = Pipeline(nodes={right_node}).add_alias(right_node, "score")

    with pytest.raises(ValueError, match="alias conflict"):
        left_pipeline | right_pipeline


def test_deployment_uses_named_single_output_port() -> None:
    def score():
        return 7

    _attach_metadata(score, outputs=[OutputPort("score", "int")])
    node = NodeFunction(score)
    run = Deployment(None, Pipeline(nodes={node})).run()

    assert run[node] == {"score": 7}


def test_deployment_accepts_pipeline_without_client_for_compatibility() -> None:
    def score():
        return 7

    _attach_metadata(score, outputs=[OutputPort("score", "int")])
    node = NodeFunction(score)
    run = Deployment(Pipeline(nodes={node})).run()

    assert run[node] == {"score": 7}


def test_deployment_rejects_multi_output_nodes() -> None:
    def pair():
        return {"left": 1, "right": 2}

    _attach_metadata(
        pair,
        outputs=[
            OutputPort("left", "int"),
            OutputPort("right", "int"),
        ],
    )
    node = NodeFunction(pair)
    run = Deployment(None, Pipeline(nodes={node})).run()

    with pytest.raises(RuntimeError, match="exactly one output"):
        run[node]


def test_yaml_pipeline_import_round_trip_preserves_links_and_aliases(tmp_path) -> None:
    node_id = str(uuid4())
    function = DFunction(
        name="add_one",
        body="return x + 1",
        inputs=[InputPort("x", "int", None)],
        outputs=[OutputPort("default", "int")],
    )
    pipeline = DPipeline(
        name="answer_pipeline",
        nodes=[DNodeFunction(uuid=node_id, func="add_one")],
        links=[
            [
                DNodeInputRef(uuid=node_id, port="x"),
                DScalar(41),
            ]
        ],
        aliases=[["answer", node_id]],
    )
    path = tmp_path / "bundle.yaml"
    path.write_text(
        yaml.dump_all([[pipeline, function]], sort_keys=False),
        encoding="utf-8",
    )
    namespace = {}

    spl_import_from_file(path, namespace)

    imported_pipeline = namespace["answer_pipeline"]
    imported_node = imported_pipeline.get_node_by_alias("answer")
    run = Deployment(None, imported_pipeline).run()

    assert imported_node.func(1) == 2
    assert imported_node in imported_pipeline.nodes
    assert len(imported_pipeline.links) == 1
    assert run[imported_node] == {"default": 42}


def test_yaml_import_rejects_python_object_tags_before_execution(tmp_path) -> None:
    marker = tmp_path / "yaml-loader-executed"
    payload = "__import__('pathlib').Path({!r}).write_text('pwned')".format(str(marker))
    path = tmp_path / "malicious.yaml"
    path.write_text(
        f"!!python/object/apply:builtins.eval\n- |\n  {payload}\n",
        encoding="utf-8",
    )

    with pytest.raises(yaml.constructor.ConstructorError, match="python/object/apply"):
        mk_top_level_deps_closure([path])

    assert not marker.exists()


def test_pipeline_builder_round_trip_keeps_linked_pipeline_output() -> None:
    def add_one(x):
        return x + 1

    def double(y):
        return y * 2

    _attach_metadata(
        add_one,
        inputs=[InputPort("x", "int", None)],
        outputs=[OutputPort("default", "int")],
    )
    _attach_metadata(
        double,
        inputs=[InputPort("y", "int", None)],
        outputs=[OutputPort("default", "int")],
    )

    first = lift(add_one).bind(x=20)
    second = lift(double).bind(y=first).alias("answer").render("pipe")
    node = second.get_node_by_alias("answer")

    assert len(second.links) == 2
    assert any(isinstance(value, NodeOutputRef) for _, value in second.links)
    assert Deployment(None, second).run()[node] == {"default": 42}


def test_pipeline_widget_renders_live_pipeline_graph() -> None:
    def add_one(x):
        return x + 1

    def double(y):
        return y * 2

    _attach_metadata(
        add_one,
        inputs=[InputPort("x", "int", None)],
        outputs=[OutputPort("default", "int")],
    )
    _attach_metadata(
        double,
        inputs=[InputPort("y", "int", None)],
        outputs=[OutputPort("default", "int")],
    )

    first = lift(add_one).bind(x=20)
    pipeline = lift(double).bind(y=first).alias("answer").render("live_pipe")
    widget = SPLClient(daemon_port=8765).draw_pipeline(pipeline)
    rendered = widget._repr_html_()

    assert "live_pipe" in rendered
    assert "add_one" in rendered
    assert "double" in rendered
    assert 'data-pipeline-control="fit"' in rendered
    assert "default -&gt; y" in rendered or "default -&amp;gt; y" in rendered


def test_node_remote_defaults_resolve_ports_from_daemon(monkeypatch) -> None:
    calls = []

    class FakeDaemonClient:
        def resolve_remote_signature(self, ref):
            calls.append(ref)
            return {
                "signature": {
                    "inputs": [
                        {
                            "name": "a",
                            "type": "int",
                            "default": None,
                        }
                    ],
                    "outputs": [
                        {
                            "name": "default",
                            "type": "str",
                        }
                    ],
                }
            }

    monkeypatch.setattr(daemon_client, "Client", FakeDaemonClient)

    node = NodeRemote(
        name="demo_traktorist_pipeline::happiness",
        version="latest",
    )

    assert node.url == ""
    assert node.name == "demo_traktorist_pipeline::happiness"
    assert node.inputs == [InputPort("a", "int", None)]
    assert node.outputs == [OutputPort("default", "str")]
    assert calls == [
        {
            "url": "",
            "name": "demo_traktorist_pipeline::happiness",
            "version": "latest",
        }
    ]

    pipeline = lift(node).bind(a=301).alias("feeling").render("remote_feeling")

    assert pipeline.get_node_by_alias("feeling") is node


def test_node_remote_accepts_pipeline_function_alias(monkeypatch) -> None:
    calls = []

    class FakeDaemonClient:
        def resolve_remote_signature(self, ref):
            calls.append(ref)
            return {
                "signature": {
                    "inputs": [
                        {
                            "name": "a",
                            "type": "int",
                            "default": None,
                        }
                    ],
                    "outputs": [
                        {
                            "name": "default",
                            "type": "str",
                        }
                    ],
                }
            }

    monkeypatch.setattr(daemon_client, "Client", FakeDaemonClient)

    with pytest.warns(DeprecationWarning, match=r"NodeRemote\.locate"):
        node = NodeRemote(
            pipeline="demo_traktorist_pipeline",
            function="happiness",
        )

    assert node.url == ""
    assert node.name == "demo_traktorist_pipeline::happiness"
    assert node.version == "latest"
    assert node.inputs == [InputPort("a", "int", None)]
    assert node.outputs == [OutputPort("default", "str")]
    assert calls == [
        {
            "url": "",
            "name": "demo_traktorist_pipeline::happiness",
            "version": "latest",
        }
    ]


def test_node_remote_accepts_library_owner_and_target_machine(monkeypatch) -> None:
    calls = []

    class FakeDaemonClient:
        def resolve_remote_signature(self, ref):
            calls.append(ref)
            return {
                "signature": {
                    "inputs": [],
                    "outputs": [{"name": "default", "type": "str"}],
                }
            }

    monkeypatch.setattr(daemon_client, "Client", FakeDaemonClient)

    node = NodeRemote(
        name="demo_traktorist_pipeline",
        owner="alice",
        library="tractors",
        target_machine="tractor-gpu",
    )

    assert node.owner_id == "alice"
    assert node.library == "tractors"
    assert node.target_machine == "tractor-gpu"
    assert calls == [
        {
            "url": "",
            "name": "demo_traktorist_pipeline",
            "version": "latest",
            "owner_id": "alice",
            "library": "tractors",
            "target_machine": "tractor-gpu",
        }
    ]


def test_node_remote_locate_is_canonical_factory(monkeypatch) -> None:
    calls = []

    class FakeDaemonClient:
        def resolve_remote_signature(self, ref):
            calls.append(ref)
            return {
                "signature": {
                    "inputs": [{"name": "a", "type": "int"}],
                    "outputs": [{"name": "default", "type": "str"}],
                }
            }

    monkeypatch.setattr(daemon_client, "Client", FakeDaemonClient)

    node = NodeRemote.locate(
        pipeline="demo_traktorist_pipeline",
        function="happiness",
        url="https://splime.io/api",
        version="3",
        target_machine="tractor-gpu",
    )

    assert node.url == "https://splime.io/api"
    assert node.name == "demo_traktorist_pipeline::happiness"
    assert node.version == "3"
    assert node.target_machine == "tractor-gpu"
    assert node.inputs == [InputPort("a", "int", None)]
    assert node.outputs == [OutputPort("default", "str")]
    assert calls == [
        {
            "url": "https://splime.io/api",
            "name": "demo_traktorist_pipeline::happiness",
            "version": "3",
            "target_machine": "tractor-gpu",
        }
    ]


def test_node_remote_keeps_explicit_legacy_constructor(monkeypatch) -> None:
    class FailingDaemonClient:
        def resolve_remote_signature(self, ref):
            raise AssertionError("explicit ports should not be resolved")

    monkeypatch.setattr(daemon_client, "Client", FailingDaemonClient)

    node = NodeRemote(
        url="https://splime.io/api",
        name="demo_traktorist_pipeline::happiness",
        version="latest",
        inputs=[InputPort("a", "int", None)],
        outputs=[OutputPort("default", "str")],
    )

    assert node.url == "https://splime.io/api"
    assert node.inputs == [InputPort("a", "int", None)]
