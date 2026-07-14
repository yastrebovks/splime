"""WP-02 presentation invariants: no public repr dumps raw transport JSON."""

from __future__ import annotations

import json

from spl import SPLClient
from spl._client import ObjectCatalog, ObjectList, ObjectTable, PublishedObject
from spl._views import (
    ActionReceiptView,
    ArtifactListView,
    ConnectionStatusView,
    DecompositionView,
    EnvTableView,
    EnvironmentBuildListView,
    EventListView,
    HealthView,
    InputListView,
    LibraryListView,
    MachineListView,
    ObjectListView,
    ObjectRecordView,
    OutputListView,
    RunListView,
    RunRecordView,
    SignatureView,
)
from spl.pipeline_widget import PipelineGraphWidget
from spl.server_client import SPLServerClient, ServerCallResult

_BIG_RAW = {
    "name": "daily_total",
    "version": 3,
    "library": "default",
    "yaml": "x" * 30_000,
    "metadata": {"blob": "y" * 5_000},
}

_LOCAL_PAYLOAD = {
    "daily_total": {
        "name": "daily_total",
        "kind": "function",
        "version": 3,
        "library": "default",
        "inputs": [{"name": "date"}],
    },
    "order_pipeline": {
        "name": "order_pipeline",
        "kind": "pipeline",
        "version": 1,
        "library": "risk",
        "inputs": [],
    },
}

_SERVER_PAYLOAD = [
    {
        "name": "risk_score",
        "kind": "function",
        "version": 7,
        "library": {"slug": "risk", "display_name": "Risk"},
        "inputs": [{"name": "amount"}, {"name": "history"}],
    }
]

_SIGNATURE_PAYLOAD = {
    "name": "order_pipeline",
    "display_name": "Order Pipeline",
    "kind": "pipeline",
    "version": 42,
    "inputs": [
        {
            "name": "amount",
            "type": "int",
            "required": True,
            "default": None,
            "sources": [{"node_id": "node-1", "function": "build_order"}],
            "ui": {"widget": "number", "blob": "x" * 5_000},
        }
    ],
    "outputs": [
        {
            "name": "result",
            "selector": "result",
            "read": 'result.value["default"]',
            "ports": [{"name": "default", "type": "dict"}],
        }
    ],
    "internal_functions": [{"name": "build_order"}],
    "call": {
        "example": 'result = client.call("order_pipeline", kwargs={"amount": 300})',
        "read": 'result.value["default"]',
        "schema": {"blob": "y" * 5_000},
    },
}

_DECOMPOSITION_PAYLOAD = {
    "nodes": [
        {
            "node_id": "node-1",
            "name": "build_order",
            "kind": "function",
            "inputs": [{"name": "amount"}],
            "outputs": [{"name": "default"}],
        }
    ],
    "functions": [{"name": "build_order"}],
    "links": [{"source": "a", "target": "b"}],
}

_RUN_PAYLOAD = {
    "id": "run-1234567890abcdef",
    "status": "succeeded",
    "object": "order_pipeline",
    "output": "result",
    "created_at": "2026-07-07T00:00:00+00:00",
    "command": "python " + "x" * 10_000,
    "result": {"artifacts": {"thumbnail.png": "/tmp/thumbnail.png"}},
}

_EVENT_PAYLOAD = {
    "id": "evt-123",
    "run_id": _RUN_PAYLOAD["id"],
    "status": "queued",
    "message": "remote run created",
    "created_at": "2026-07-07T00:00:01+00:00",
    "payload": {"object_owner_id": "owner", "blob": "z" * 10_000},
}

_OBJECT_PAYLOAD = {
    "name": "order_pipeline",
    "kind": "pipeline",
    "version": 7,
    "library": {"display_name": "Default library"},
    "inputs": [{"name": "amount"}],
    "outputs": [{"name": "result"}],
    "yaml": "body" * 10_000,
}


def _published() -> PublishedObject:
    return PublishedObject(
        name="daily_total",
        entrypoint="daily_total",
        env="default",
        yaml_path="/tmp/x.yaml",
        raw=_BIG_RAW,
    )


def test_published_repr_is_compact() -> None:
    published = _published()
    assert len(repr(published)) < 200
    assert "Published daily_total" in repr(published)
    assert "v3" in repr(published)
    assert hasattr(published, "_repr_html_")
    assert len(published._repr_html_()) < 2_000


def test_published_raw_stays_accessible() -> None:
    assert _published().raw is _BIG_RAW
    assert _published().version == "3"
    assert _published().library == "default"


def test_object_views_preserve_container_semantics() -> None:
    table = ObjectTable(_LOCAL_PAYLOAD)
    listing = ObjectList(_SERVER_PAYLOAD)
    catalog = ObjectCatalog({"local": _LOCAL_PAYLOAD, "server": _SERVER_PAYLOAD})

    assert isinstance(table, dict)
    assert isinstance(listing, list)
    assert isinstance(catalog, dict)
    assert set(catalog) == {"local", "server"}
    assert table["daily_total"]["version"] == 3
    assert listing[0]["name"] == "risk_score"
    assert json.dumps(table)
    assert json.dumps(listing)
    assert json.dumps(catalog)
    assert table.raw == dict(_LOCAL_PAYLOAD)
    assert listing.raw == list(_SERVER_PAYLOAD)
    assert catalog.raw == {"local": _LOCAL_PAYLOAD, "server": _SERVER_PAYLOAD}


def test_object_views_render_compact_tables() -> None:
    table_text = repr(ObjectTable(_LOCAL_PAYLOAD))
    assert table_text.startswith("local objects (2):")
    assert "daily_total" in table_text
    assert "order_pipeline" in table_text
    assert "kind" in table_text
    assert len(table_text) < 1_000

    listing_text = repr(ObjectList(_SERVER_PAYLOAD))
    assert listing_text.startswith("server objects (1):")
    assert "risk_score" in listing_text
    assert "Risk" in listing_text

    catalog = ObjectCatalog({"local": _LOCAL_PAYLOAD, "server": _SERVER_PAYLOAD})
    catalog_text = repr(catalog)
    assert "objects (3 = 2 local + 1 server)" in catalog_text
    assert "local objects (2):" in catalog_text
    assert "server objects (1):" in catalog_text
    assert hasattr(catalog, "_repr_html_")


def test_object_table_shows_owner_only_for_multi_owner_local_catalog() -> None:
    one_owner_text = repr(
        ObjectTable(
            {
                "daily_total": {
                    "name": "daily_total",
                    "owner_id": "owner-a",
                    "kind": "function",
                    "version": 1,
                    "library": "default",
                    "inputs": [],
                },
                "risk_score": {
                    "name": "risk_score",
                    "owner_id": "owner-a",
                    "kind": "function",
                    "version": 1,
                    "library": "risk",
                    "inputs": [],
                },
            }
        )
    )
    assert "owner" not in one_owner_text.splitlines()[1].split()

    multi_owner_text = repr(
        ObjectTable(
            {
                "owner-a/default/clean_amount": {
                    "name": "clean_amount",
                    "owner_id": "owner-a",
                    "kind": "function",
                    "version": 1,
                    "library": "default",
                    "inputs": [],
                },
                "owner-b/default/clean_amount": {
                    "name": "clean_amount",
                    "owner_id": "owner-b",
                    "kind": "function",
                    "version": 1,
                    "library": "default",
                    "inputs": [],
                },
            }
        )
    )
    header = multi_owner_text.splitlines()[1].split()
    assert "owner" in header
    assert "owner-a" in multi_owner_text
    assert "owner-b" in multi_owner_text


def test_empty_views_render_placeholders() -> None:
    assert repr(ObjectList([])) == "server objects: (empty)"
    assert "(empty)" in repr(ObjectCatalog({"local": {}, "server": []}))


def test_service_views_preserve_container_semantics_and_hide_raw_blobs() -> None:
    views = [
        HealthView(
            {
                "ok": True,
                "counts": {"objects": 3, "runs": 9},
                "db": {"exists": True, "path": "/tmp/daemon.sqlite3"},
                "server": {"connected": True, "connection": {"token": "x" * 5_000}},
                "environment_builds": {"by_status": {"ready": 2}},
            }
        ),
        ConnectionStatusView(
            {
                "connected": True,
                "server_url": "https://splime.io/api",
                "connection": {"status": "connected", "machine_id": "machine-1"},
            }
        ),
        MachineListView(
            {
                "current_machine_id": "machine-1",
                "machines": [
                    {
                        "id": "machine-1",
                        "display_name": "local",
                        "status": "online",
                        "last_seen_at": "now",
                    }
                ],
            }
        ),
        EnvTableView({"default": {"python": "/tmp/.venv/bin/python", "updated_at": "now"}}),
        ActionReceiptView({"name": "risk", "status": "created", "blob": "x" * 5_000}),
        SignatureView(_SIGNATURE_PAYLOAD),
        DecompositionView(_DECOMPOSITION_PAYLOAD),
        RunRecordView(_RUN_PAYLOAD),
        ObjectRecordView(_OBJECT_PAYLOAD),
    ]
    list_views = [
        LibraryListView(
            [
                {
                    "slug": "default",
                    "display_name": "Default library",
                    "access": ["execute", "metadata:read"],
                }
            ]
        ),
        EnvironmentBuildListView([{"spec_hash": "abcdef123456", "status": "ready", "base_python": "/bin/python"}]),
        InputListView(_SIGNATURE_PAYLOAD["inputs"]),
        OutputListView(_SIGNATURE_PAYLOAD["outputs"]),
        RunListView([_RUN_PAYLOAD]),
        EventListView([_EVENT_PAYLOAD]),
        ArtifactListView([{"name": "thumbnail.png", "size": 70, "format": "png"}]),
        ObjectListView([_OBJECT_PAYLOAD]),
    ]

    for view in views:
        assert isinstance(view, dict)
        assert view.raw == dict(view)
        assert json.dumps(view)
        rendered = repr(view)
        assert len(rendered) < 1_500
        assert "xxxxx" not in rendered
        assert "yyyyy" not in rendered
        assert "zzzzz" not in rendered
        assert "bodybody" not in rendered
        assert hasattr(view, "_repr_html_")

    for view in list_views:
        assert isinstance(view, list)
        assert json.dumps(view)
        rendered = repr(view)
        assert len(rendered) < 1_500
        assert "xxxxx" not in rendered
        assert "yyyyy" not in rendered
        assert "zzzzz" not in rendered
        assert "bodybody" not in rendered
        assert hasattr(view, "_repr_html_")


def test_run_record_view_shows_observability_tables() -> None:
    payload = {
        **_RUN_PAYLOAD,
        "node_runtimes": [
            {"alias": "producer", "name": "native", "source": "default"},
            {
                "alias": "consumer",
                "name": "docker",
                "source": "node-tag",
                "resolved": {"image_tag": "python:3.13-slim"},
            },
        ],
        "edge_adapters": [
            {
                "source": "producer.default",
                "target": "consumer.value",
                "tag": "txt",
                "save": "save_text",
                "load": "load_text",
                "source_level": "pipeline",
            }
        ],
    }

    rendered = repr(RunRecordView(payload))
    listed = repr(RunListView([payload]))

    assert "node runtimes" in rendered
    assert "producer.default -> consumer.value" in rendered
    assert "save_text -> load_text" in rendered
    assert "docker" in rendered
    assert "image_tag=python:3.13-slim" in rendered
    assert "edge adapters" not in listed
    assert "producer.default" not in listed


class _PresentationDaemon:
    def health(self) -> dict[str, object]:
        return {"ok": True, "counts": {"objects": 1}, "db": {"exists": True}}

    def server_connection(self) -> dict[str, object]:
        return {
            "connected": True,
            "server_url": "https://splime.io/api",
            "connection": {"status": "connected", "machine_id": "machine-1"},
        }

    def server_machines(self) -> dict[str, object]:
        return {
            "current_machine_id": "machine-1",
            "machines": [{"id": "machine-1", "status": "online"}],
        }

    def server_libraries(self, *, include_accessible: bool = True) -> list[dict[str, object]]:
        return [{"slug": "default", "display_name": "Default library"}]

    def register_env(self, name: str, python: str | None) -> dict[str, object]:
        return {"name": name, "python": python or "/bin/python"}

    def list_envs(self) -> dict[str, object]:
        return {"default": {"python": "/bin/python"}}

    def list_environment_builds(self) -> list[dict[str, object]]:
        return [{"spec_hash": "abcdef123456", "status": "ready"}]

    def rebuild_environment_build(self, spec_hash: str, *, wait: bool) -> dict[str, object]:
        return {"spec_hash": spec_hash, "status": "ready", "wait": wait}

    def signature(self, *args, **kwargs) -> dict[str, object]:
        return dict(_SIGNATURE_PAYLOAD)

    def inputs(self, *args, **kwargs) -> list[dict[str, object]]:
        return list(_SIGNATURE_PAYLOAD["inputs"])

    def outputs(self, *args, **kwargs) -> list[dict[str, object]]:
        return list(_SIGNATURE_PAYLOAD["outputs"])

    def decomposition(self, *args, **kwargs) -> dict[str, object]:
        return dict(_DECOMPOSITION_PAYLOAD)

    def list_runs(self) -> list[dict[str, object]]:
        return [dict(_RUN_PAYLOAD)]

    def forget(self, *args, **kwargs) -> dict[str, object]:
        return {"name": "demo", "removed": True}

    def forget_version(self, *args, **kwargs) -> dict[str, object]:
        return {"name": "demo", "version": 1, "removed": True}

    def prune_stale_mirrors(self, *args, **kwargs) -> dict[str, object]:
        return {"pruned": 2}


def _presentation_client() -> SPLClient:
    client = SPLClient.__new__(SPLClient)
    client._daemon = _PresentationDaemon()
    client.server_connection = None
    return client


def test_spl_client_methods_return_compact_views() -> None:
    client = _presentation_client()

    results = [
        client.health(),
        client.current_server_connection(),
        client.machines(),
        client.libraries(),
        client.register_env("default"),
        client.envs(),
        client.environment_builds(),
        client.rebuild_environment("abcdef", wait=True),
        client.signature("order_pipeline"),
        client.inputs("order_pipeline"),
        client.outputs("order_pipeline"),
        client.decomposition("order_pipeline"),
        client.runs(),
        client.forget("demo"),
        client.forget_version("demo", 1),
        client.prune_stale_mirrors(),
    ]

    assert isinstance(results[1], dict)
    assert results[1].get("connected") is True
    assert isinstance(results[2], dict)
    assert results[2].get("current_machine_id") == "machine-1"
    assert isinstance(results[3], list)
    assert results[3][0]["slug"] == "default"
    assert isinstance(results[12][0], dict)

    for result in results:
        assert json.dumps(result)
        assert len(repr(result)) < 1_500
        assert hasattr(result, "_repr_html_")


def test_spl_client_machines_does_not_hide_daemon_machine_payload() -> None:
    class MachinesDaemon(_PresentationDaemon):
        def server_connection(self) -> dict[str, object]:
            return {"connected": False, "connection": {"status": "heartbeat_failed"}}

        def server_machines(self) -> dict[str, object]:
            return {
                "current_machine_id": "machine-1",
                "machines": [{"id": "machine-1", "display_name": "Pair3", "status": "online"}],
            }

    client = SPLClient.__new__(SPLClient)
    client._daemon = MachinesDaemon()
    client.server_connection = None

    machines = client.machines()

    assert machines["machines"][0]["display_name"] == "Pair3"
    assert "Pair3" in repr(machines)


class _FakeServerClient(SPLServerClient):
    def __init__(self) -> None:
        self.token = "token"
        self.base_url = "https://splime.io/api"

    def _json_request(self, method: str, path: str, payload=None):
        if path.startswith("/objects/demo/signature"):
            return dict(_SIGNATURE_PAYLOAD)
        if path.startswith("/objects/demo/inputs"):
            return list(_SIGNATURE_PAYLOAD["inputs"])
        if path.startswith("/objects/demo/outputs"):
            return list(_SIGNATURE_PAYLOAD["outputs"])
        if path.startswith("/objects/demo/decomposition"):
            return dict(_DECOMPOSITION_PAYLOAD)
        if path.startswith("/objects/demo/versions"):
            return [{"version": 7, "id": "version-123", "env": "default"}]
        if path.startswith("/objects/demo"):
            return dict(_OBJECT_PAYLOAD)
        if path == "/objects":
            return [dict(_OBJECT_PAYLOAD)]
        if path == "/remote-runs":
            return [dict(_RUN_PAYLOAD)] if method == "GET" else dict(_RUN_PAYLOAD)
        if path.endswith("/events"):
            return [dict(_EVENT_PAYLOAD)]
        if path.endswith("/artifacts"):
            return [{"name": "thumbnail.png", "size": 70, "format": "png"}]
        if path.endswith("/detail") or "/remote-runs/" in path and method == "GET":
            return dict(_RUN_PAYLOAD)
        if path.endswith("/cancel") or path.endswith("/retry"):
            return dict(_RUN_PAYLOAD)
        raise AssertionError(f"unexpected request: {method} {path}")


def test_server_client_methods_return_compact_views() -> None:
    client = _FakeServerClient()
    run = client.start("demo")

    results = [
        client.objects(),
        client.get_object("demo"),
        client.signature("demo"),
        client.inputs("demo"),
        client.outputs("demo"),
        client.decomposition("demo"),
        client.versions("demo"),
        client.runs(),
        client.get_run("run-1"),
        client.get_run_detail("run-1"),
        client.list_events("run-1"),
        client.list_artifacts("run-1"),
        client.cancel_run("run-1"),
        client.retry_run("run-1").state,
        run.state,
        run.detail(),
        run.events(),
    ]

    for result in results:
        assert isinstance(result, dict | list)
        assert json.dumps(result)
        assert len(repr(result)) < 1_500
        assert hasattr(result, "_repr_html_")

    call_result = ServerCallResult(
        run=dict(_RUN_PAYLOAD),
        detail={"result": {"value": {"ok": True}}, "artifacts": [{"name": "a.txt"}]},
    )
    assert call_result.value == {"ok": True}
    assert len(repr(call_result)) < 500
    assert hasattr(call_result, "_repr_html_")


def test_pipeline_graph_widget_repr_is_compact() -> None:
    widget = PipelineGraphWidget(
        _DECOMPOSITION_PAYLOAD,
        {"name": "order_pipeline", "yaml": "body" * 10_000},
    )

    rendered = repr(widget)

    assert "PipelineGraphWidget" in rendered
    assert "order_pipeline" in rendered
    assert "decomposition=" not in rendered
    assert "bodybody" not in rendered
    assert len(rendered) < 300


def test_owner_aware_object_and_library_views_use_handle_then_id() -> None:
    object_payload = [
        {
            "name": "risk_score",
            "kind": "function",
            "version": 7,
            "owner_id": "owner-a",
            "owner_handle": "alice",
            "library": {"owner_id": "owner-a", "slug": "default"},
            "inputs": [],
        },
        {
            "name": "risk_report",
            "kind": "pipeline",
            "version": 3,
            "owner_id": "owner-b",
            "owner_handle": None,
            "library": {"owner_id": "owner-b", "slug": "default"},
            "inputs": [],
        },
        {
            "name": "legacy_local",
            "kind": "function",
            "version": 1,
            "library": "local",
            "inputs": [],
        },
    ]
    object_view = ObjectListView(object_payload)
    object_text = repr(object_view)

    assert object_text.splitlines()[1].split() == ["name", "kind", "version", "owner", "library", "inputs"]
    assert "@alice" in object_text
    assert "owner-b" in object_text
    assert "legacy_local" in object_text and "—" in object_text
    assert object_view.raw == object_payload

    record_with_handle = ObjectRecordView(object_payload[0])
    record_with_id = ObjectRecordView(object_payload[1])
    copied_record = ObjectRecordView(
        {
            **object_payload[0],
            "library": {
                "owner_id": "owner-b",
                "owner_handle": "bob",
                "slug": "shared",
                "display_name": "Shared library",
            },
        }
    )
    assert "\nowner     : @alice\n" in repr(record_with_handle)
    assert "\nowner     : owner-b\n" in repr(record_with_id)
    assert "\nlibrary   : @bob/shared\n" in repr(copied_record)

    library_payload = [
        {
            "slug": "default",
            "display_name": "Alice default",
            "owner_id": "owner-a",
            "owner_handle": "alice",
            "owned": True,
            "access": ["execute"],
        },
        {
            "slug": "shared",
            "display_name": "Legacy shared",
            "owner_id": "owner-b",
            "access": ["metadata:read"],
        },
    ]
    library_view = LibraryListView(library_payload)
    library_text = repr(library_view)
    assert library_text.splitlines()[1].split() == [
        "slug",
        "display",
        "owner",
        "owned",
        "access",
        "visibility",
        "default_machine",
    ]
    assert "@alice" in library_text
    assert "owner-b" in library_text
    assert "True" in library_text
    assert library_view.raw == library_payload


def test_action_receipt_renders_owner_and_auto_resolved_scope() -> None:
    payload = {
        "status": "queued",
        "library": "default",
        "owner_id": "caller-id",
        "owner_handle": "caller",
        "resolution": {
            "auto_resolved": True,
            "requested_library": "default",
            "resolved_owner_id": "owner-a",
            "resolved_owner_handle": "alice",
            "resolved_library": "default",
            "resolved_library_id": "library-a",
        },
        "id": "run-1",
    }

    view = ActionReceiptView(payload)
    rendered = repr(view)

    assert "library : default\nowner   : @caller\nresolved: @alice/default" in rendered
    assert view.raw == payload


def test_signature_resolution_prefers_handle_for_old_and_d1_annotations() -> None:
    old_retry_payload = {
        **_SIGNATURE_PAYLOAD,
        "resolved_from_server": {
            "library": "default",
            "owner_id": "owner-a",
            "owner_handle": "alice",
        },
    }
    d1_payload = {
        **_SIGNATURE_PAYLOAD,
        "resolved_from": {
            "auto_resolved": True,
            "requested_library": "default",
            "resolved_owner_id": "owner-b",
            "resolved_owner_handle": "bob",
            "resolved_library": "default",
            "resolved_library_id": "library-b",
        },
    }
    id_fallback_payload = {
        **_SIGNATURE_PAYLOAD,
        "resolved_from_server": {"library": "shared", "owner_id": "owner-c"},
    }

    assert "resolved from server: library 'default', owner @alice" in repr(SignatureView(old_retry_payload))
    assert "resolved from server: library 'default', owner @bob" in repr(SignatureView(d1_payload))
    assert "resolved from server: library 'shared', owner owner-c" in repr(SignatureView(id_fallback_payload))


def test_client_describe_uses_handle_aware_resolution_label() -> None:
    class DescribeDaemon:
        def signature(self, *args, **kwargs) -> dict[str, object]:
            return {
                **_SIGNATURE_PAYLOAD,
                "resolved_from": {
                    "auto_resolved": True,
                    "requested_library": "default",
                    "resolved_owner_id": "owner-a",
                    "resolved_owner_handle": "alice",
                    "resolved_library": "default",
                    "resolved_library_id": "library-a",
                },
            }

    client = SPLClient.__new__(SPLClient)
    client._daemon = DescribeDaemon()
    client.server_connection = {"connected": True}

    rendered = client.describe("order_pipeline")

    assert "Resolved from server: library 'default', owner @alice" in rendered


def test_legacy_payload_repr_snapshots_and_raw_are_unchanged() -> None:
    old_object = {
        "name": "order_pipeline",
        "kind": "pipeline",
        "version": 7,
        "owner_id": "owner-a",
        "library": {"owner_id": "owner-b", "display_name": "Default library"},
        "inputs": [{"name": "amount"}],
        "outputs": [{"name": "result"}],
        "yaml": "body",
    }
    old_library = [
        {
            "slug": "default",
            "display_name": "Default library",
            "owner_id": "owner-a",
            "access": ["execute", "metadata:read"],
        }
    ]
    old_receipt = {"name": "risk", "status": "created", "owner_id": "owner-a"}
    old_signature = {
        "name": "demo",
        "kind": "function",
        "version": 1,
        "inputs": [],
        "outputs": [],
        "internal_functions": [],
        "call": {},
    }

    object_record = ObjectRecordView(old_object)
    object_list = ObjectListView([old_object])
    library_list = LibraryListView(old_library)
    receipt = ActionReceiptView(old_receipt)
    signature = SignatureView(old_signature)

    assert repr(object_record) == (
        "object:\n"
        "name      : order_pipeline\n"
        "kind      : pipeline\n"
        "version   : 7\n"
        "library   : Default library\n"
        "env       : —\n"
        "entrypoint: —\n"
        "inputs    : 1\n"
        "outputs   : 1\n"
        "yaml      : included"
    )
    assert repr(object_list) == (
        "objects (1):\n"
        "name            kind      version  library          inputs\n"
        "order_pipeline  pipeline  7        Default library  1     "
    )
    assert repr(library_list) == (
        "libraries (1):\n"
        "slug     display          owner    access                 visibility  default_machine\n"
        "default  Default library  owner-a  execute,metadata:read  —           —              "
    )
    assert repr(receipt) == "receipt:\nstatus: created\nname  : risk"
    assert repr(signature) == (
        "signature:\n"
        "name     : demo\n"
        "kind     : function\n"
        "version  : 1\n"
        "inputs   : 0\n"
        "outputs  : 0\n"
        "functions: 0\n"
        "example  : —\n"
        "read     : —"
    )
    assert object_record.raw == old_object
    assert object_list.raw == [old_object]
    assert library_list.raw == old_library
    assert receipt.raw == old_receipt
    assert signature.raw == old_signature


class _OwnerCatalogPresentationDaemon:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.payload = [
            {
                "name": "risk_score",
                "kind": "function",
                "version": 7,
                "owner_id": "owner-a",
                "owner_handle": "alice",
                "library": {"owner_id": "owner-a", "slug": "default"},
                "inputs": [],
            }
        ]

    def server_libraries(self, *, include_accessible: bool = True) -> list[dict[str, object]]:
        self.calls.append(("libraries", include_accessible))
        return [
            {
                "slug": "default",
                "owner_id": "owner-a",
                "owner_handle": "alice",
                "owned": True,
            }
        ]

    def server_objects(
        self,
        *,
        owner_id: str | None,
        library: str | None,
        compact: bool,
    ) -> list[dict[str, object]]:
        self.calls.append(("objects", owner_id, library, compact))
        return self.payload


def test_client_server_library_listing_shows_owner_without_repr_network_calls() -> None:
    daemon = _OwnerCatalogPresentationDaemon()
    client = SPLClient.__new__(SPLClient)
    client._daemon = daemon
    client.server_connection = {"connected": True}

    objects = client.objects(scope="server", library="default")
    calls_before_repr = list(daemon.calls)
    rendered = repr(objects)

    assert "owner" in rendered.splitlines()[1].split()
    assert "@alice" in rendered
    assert objects.raw == daemon.payload
    assert daemon.calls == calls_before_repr
