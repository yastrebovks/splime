from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from spl.core.library_adapters import (
    LibraryAdapterContractError,
    environment_fingerprint,
    normalize_publish_request,
)
from spl.daemon.library_adapters import (
    commit_library_adapter_publication,
    prepare_library_adapter_publication,
)
from spl.daemon.repositories.library_adapter import (
    MAX_LIBRARY_ADAPTER_CATALOG_ENTRY_BYTES,
    MAX_LIBRARY_ADAPTER_PAGE_BYTES,
)
from spl.daemon.store import RegistryStore
from spl.daemon.server import create_app


SAVE_TEXT = """def save_text(path: str, value: str) -> None:
    from pathlib import Path as LocalPath
    LocalPath(path).write_text(value, encoding='utf-8')
"""
LOAD_TEXT = """def load_text(path: str) -> str:
    from pathlib import Path as LocalPath
    return LocalPath(path).read_text(encoding='utf-8')
"""
REF_KEYS = (
    "owner",
    "library",
    "name",
    "version",
    "adapter_id",
    "adapter_version_id",
    "content_hash",
    "signature_hash",
)


def _publication(
    *,
    name: str = "text-file",
    save_source: str | None = SAVE_TEXT,
    load_source: str | None = LOAD_TEXT,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "name": name,
        "description": "Safe text adapter",
        "semantic_type": "builtins.str",
        "semantic_category": "text",
        "save_source": save_source,
        "load_source": load_source,
        "dependencies": [],
        "format_tag": None,
        "media_type": None,
        "preferred_extension": None,
        "policy": {
            "local_custom_code": "allow",
            "remote_custom_code": "deny",
        },
        "publication_environment": {
            "fingerprint": environment_fingerprint([]),
            "distributions": [],
        },
    }


def _maximal_catalog_publication(*, name: str, format_index: int) -> dict[str, object]:
    dependencies = [
        {
            "package": f"pkg{index:03d}" + ("p" * 154),
            "version": "1" + ("v" * 159),
            "modules": [f"mod{index:03d}" + ("m" * 122)],
        }
        for index in range(256)
    ]
    distributions = [{"package": item["package"], "version": item["version"]} for item in dependencies]
    request = _publication(name=name)
    request.update(
        {
            "description": "d" * 4_096,
            "semantic_type": "T" * 512,
            "dependencies": dependencies,
            "format_tag": f"format{format_index:03d}" + ("f" * 151),
            "media_type": "application/" + ("x" * 127),
            "preferred_extension": "." + ("x" * 32),
            "publication_environment": {
                "fingerprint": environment_fingerprint(distributions),
                "distributions": distributions,
            },
        }
    )
    return request


@pytest.mark.parametrize(
    ("save_source", "load_source", "directions"),
    [
        (SAVE_TEXT, None, ["output"]),
        (None, LOAD_TEXT, ["input"]),
        (SAVE_TEXT, LOAD_TEXT, ["input", "output"]),
    ],
)
def test_store_accepts_partial_directions_and_catalog_never_leaks_source(
    tmp_path: Path,
    save_source: str | None,
    load_source: str | None,
    directions: list[str],
) -> None:
    with RegistryStore(tmp_path / "daemon") as store:
        prepared = normalize_publish_request(
            _publication(
                name="adapter-" + "-".join(directions),
                save_source=save_source,
                load_source=load_source,
            )
        )
        committed = store.publish_library_adapter(
            prepared,
            owner_id="owner",
            library="default",
            publisher_id="owner",
        )
        assert committed["directions"] == directions
        code_free = store.get_library_adapter(committed["adapter_id"])
        assert code_free["semantic_category"] == "text"
        assert code_free["format_tag"] is None
        assert code_free["media_type"] is None
        assert code_free["preferred_extension"] is None
        for forbidden in (
            "save_source",
            "load_source",
            "policy",
            "publication_environment",
            "signature",
        ):
            assert forbidden not in code_free
        source = store.resolve_library_adapter_ref(
            {key: committed[key] for key in REF_KEYS},
            include_source=True,
        )
        assert source["save_source"] == save_source
        assert source["load_source"] == load_source


def test_neither_direction_fails_before_mutation(tmp_path: Path) -> None:
    with RegistryStore(tmp_path / "daemon") as store:
        with pytest.raises(LibraryAdapterContractError) as rejected:
            commit_library_adapter_publication(
                store,
                _publication(save_source=None, load_source=None),
                owner_id="local",
                library="default",
            )
        assert rejected.value.code == "adapter_functions_missing"
        assert store.list_library_adapters()["items"] == []


def test_exact_dedup_does_not_regress_current_version(tmp_path: Path) -> None:
    with RegistryStore(tmp_path / "daemon") as store:
        first = commit_library_adapter_publication(
            store,
            _publication(),
            owner_id="local",
            library="default",
        )
        changed_request = _publication()
        changed_request["save_source"] = SAVE_TEXT.replace(
            "write_text(value,",
            "write_text(value.upper(),",
        )
        second = commit_library_adapter_publication(
            store,
            changed_request,
            owner_id="local",
            library="default",
        )
        repeated = commit_library_adapter_publication(
            store,
            _publication(),
            owner_id="local",
            library="default",
        )

        assert first["version"]["version"] == 1
        assert second["version"]["version"] == 2
        assert repeated["ref"] == first["ref"]
        assert repeated["deduplicated"] is True
        current = store.get_library_adapter(
            "text-file",
            owner_id="local",
            library="default",
        )
        assert current["adapter_version_id"] == second["ref"]["adapter_version_id"]
        assert current["version"] == 2


def test_concurrent_identical_publish_creates_one_immutable_version(tmp_path: Path) -> None:
    with RegistryStore(tmp_path / "daemon") as store:
        prepared = normalize_publish_request(_publication())

        def publish() -> dict[str, object]:
            return store.publish_library_adapter(
                prepared,
                owner_id="owner",
                library="default",
                publisher_id="owner",
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _index: publish(), range(16)))
        assert sum(result["created"] is True for result in results) == 1
        assert len({result["adapter_version_id"] for result in results}) == 1
        assert len(store.list_library_adapter_versions(results[0]["adapter_id"])["items"]) == 1


def test_preflight_is_read_only_and_pagination_is_deterministic(tmp_path: Path) -> None:
    with RegistryStore(tmp_path / "daemon") as store:
        validation = prepare_library_adapter_publication(
            store,
            _publication(name="not-committed"),
            owner_id="local",
            library="default",
        )
        assert validation["status"] == "valid"
        assert store.list_library_adapters()["items"] == []

        for index in range(5):
            request = deepcopy(_publication(name=f"adapter-{index}"))
            prepared = normalize_publish_request(request)
            store.publish_library_adapter(
                prepared,
                owner_id="owner",
                library="default",
                publisher_id="owner",
            )
        first = store.list_library_adapters(limit=2)
        second = store.list_library_adapters(limit=2, cursor=first["next_cursor"])
        third = store.list_library_adapters(limit=2, cursor=second["next_cursor"])
        assert [item["name"] for item in first["items"] + second["items"] + third["items"]] == [
            f"adapter-{index}" for index in range(5)
        ]
        assert first["truncated"] is second["truncated"] is True
        assert third["truncated"] is False
        assert third["next_cursor"] is None


def test_maximal_local_catalog_and_version_pages_are_byte_bounded(
    tmp_path: Path,
) -> None:
    token = "libraryAdapterBoundedPageToken123456"
    store = RegistryStore(tmp_path / "daemon")
    app = create_app(store, api_token=token, auto_build_envs=False)
    catalog_names = {f"maximal-{index}" for index in range(6)}
    version_adapter_id = ""
    try:
        for index, name in enumerate(sorted(catalog_names)):
            committed = store.publish_library_adapter(
                normalize_publish_request(_maximal_catalog_publication(name=name, format_index=index)),
                owner_id="owner",
                library="default",
                publisher_id="owner",
            )
            projected_bytes = len(
                json.dumps(
                    {key: value for key, value in committed.items() if key not in {"created", "deduplicated"}},
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            assert projected_bytes <= MAX_LIBRARY_ADAPTER_CATALOG_ENTRY_BYTES

        for index in range(6):
            committed = store.publish_library_adapter(
                normalize_publish_request(
                    _maximal_catalog_publication(
                        name="maximal-version-history",
                        format_index=100 + index,
                    )
                ),
                owner_id="owner",
                library="default",
                publisher_id="owner",
            )
            version_adapter_id = str(committed["adapter_id"])

        async def get(path: str) -> tuple[int, bytes, dict[str, Any]]:
            response = await app.test_client().get(
                path,
                headers={"Authorization": f"Bearer {token}"},
            )
            return (
                response.status_code,
                await response.get_data(),
                await response.get_json(),
            )

        def collect(path: str) -> tuple[list[dict[str, Any]], list[int]]:
            items: list[dict[str, Any]] = []
            response_sizes: list[int] = []
            cursor: str | None = None
            while True:
                separator = "&" if "?" in path else "?"
                request_path = path if cursor is None else f"{path}{separator}cursor={cursor}"
                status, raw, page = asyncio.run(get(request_path))
                assert status == 200
                response_sizes.append(len(raw))
                assert len(raw) <= MAX_LIBRARY_ADAPTER_PAGE_BYTES
                assert len(raw) < 4 * 1024 * 1024
                items.extend(page["items"])
                cursor = page["next_cursor"]
                assert page["truncated"] is (cursor is not None)
                if cursor is None:
                    break
            return items, response_sizes

        catalog_items, catalog_sizes = collect("/library-adapters?limit=100")
        assert catalog_sizes[0] > 256 * 1024
        assert len(catalog_sizes) > 1
        assert {item["name"] for item in catalog_items} == catalog_names | {"maximal-version-history"}
        assert len({item["adapter_id"] for item in catalog_items}) == len(catalog_items)

        version_items, version_sizes = collect(f"/library-adapters/{version_adapter_id}/versions?limit=100")
        assert version_sizes[0] > 256 * 1024
        assert len(version_sizes) > 1
        assert {item["version"] for item in version_items} == set(range(1, 7))
        assert len({item["adapter_version_id"] for item in version_items}) == 6
    finally:
        app.runtime.shutdown()
        store.close()


class _ConnectedCatalogServer:
    def __init__(self, record: dict[str, Any]) -> None:
        self.record = record
        self.calls: list[dict[str, Any]] = []
        self.fail = False

    def get_server_version(self) -> dict[str, Any]:
        return {"declared": {"contracts": {"daemon_server_capabilities": ["spl.library_adapter_catalog.v1"]}}}

    def list_library_adapters(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("central catalog unavailable")
        return {
            "schema": "spl.library-adapter-catalog",
            "schema_version": 1,
            "items": [deepcopy(self.record)],
            "next_cursor": None,
            "truncated": False,
            "coverage": {"server_only": "must be stripped"},
        }

    def get_library_adapter(
        self,
        _owner: str,
        _library: str,
        _adapter_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.calls.append(kwargs)
        return deepcopy(self.record)

    def get_library_adapter_version(
        self,
        _owner: str,
        _library: str,
        _adapter_id: str,
        _version_id: str,
        *,
        include_source: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.calls.append(kwargs)
        result = deepcopy(self.record)
        if include_source:
            result.update(
                {
                    "save_source": SAVE_TEXT,
                    "load_source": LOAD_TEXT,
                    "symbols": {"save": "save_text", "load": "load_text"},
                    "policy": {
                        "local_custom_code": "allow",
                        "remote_custom_code": "allow",
                    },
                    "publication_environment": {
                        "fingerprint": environment_fingerprint([]),
                        "distributions": [],
                    },
                    "signature": {},
                }
            )
        return result


def test_connected_catalog_forwards_target_normalizes_shape_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = "libraryAdapterRouteToken123456"
    store = RegistryStore(tmp_path / "daemon")
    publication = _publication()
    publication["policy"]["remote_custom_code"] = "allow"
    prepared = normalize_publish_request(publication)
    local = store.publish_library_adapter(
        prepared,
        owner_id="local",
        library="default",
        publisher_id="local",
    )
    central_record = {key: value for key, value in local.items() if key not in {"created", "deduplicated"}}
    central_record["owner"] = "central-owner"
    central_record["adapter_id"] = "central-adapter"
    central_record["adapter_version_id"] = "central-version"
    central_record["effective_policy"] = {
        "local_custom": "allow",
        "remote_custom": "allow",
    }
    central_record["availability"] = {
        "state": "unavailable",
        "reason_code": "target_dependencies_unavailable",
        "environment_fingerprint": "e" * 64,
        "missing_dependencies": [{"package": "private-package", "version": "1"}],
    }
    server = _ConnectedCatalogServer(central_record)
    app = create_app(store, api_token=token, auto_build_envs=False)
    credentials = {
        "remote_connection_id": "connection-1",
        "machine_id": "local-machine",
    }
    monkeypatch.setattr(
        store,
        "current_server_connection_credentials",
        lambda: credentials,
    )
    monkeypatch.setattr(
        app.runtime,
        "_server_channel_is_live",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        app.runtime,
        "_server_client_for_credentials",
        lambda _credentials: server,
    )
    monkeypatch.setattr(app.runtime, "resolve_user_ref", lambda owner: owner)

    async def get(path: str) -> tuple[int, dict[str, Any]]:
        response = await app.test_client().get(
            path,
            headers={"Authorization": f"Bearer {token}"},
        )
        return response.status_code, await response.get_json()

    try:
        status, body = asyncio.run(get("/library-adapters?limit=10&target_machine_id=remote-machine"))
        assert status == 200
        assert set(body) == {
            "schema",
            "schema_version",
            "items",
            "next_cursor",
            "truncated",
            "revision",
            "instance",
        }
        assert server.calls[-1]["target_machine_id"] == "remote-machine"
        assert server.calls[-1]["execution_target"] == "remote"
        [item] = body["items"]
        assert item["availability"] == {
            "local": {
                "state": "unavailable",
                "reason": "local_target_not_selected",
            },
            "remote": {
                "state": "unavailable",
                "reason": "target_dependencies_unavailable",
            },
        }
        encoded = repr(body)
        assert "coverage" not in encoded
        assert "environment_fingerprint" not in encoded
        assert "private-package" not in encoded

        status, detail = asyncio.run(
            get(
                "/library-adapters/central-adapter/versions/central-version"
                "?owner=central-owner&library=default&target_machine_id=remote-machine"
            )
        )
        assert status == 200
        assert detail["adapter_version_id"] == "central-version"
        assert detail["availability"] == item["availability"]
        assert server.calls[-1] == {
            "target_machine_id": "remote-machine",
            "execution_target": "remote",
        }

        status, source = asyncio.run(
            get(
                "/library-adapters/central-adapter/versions/central-version/source"
                "?owner=central-owner&library=default&target_machine_id=remote-machine"
            )
        )
        assert status == 200
        assert set(source) == {
            "schema",
            "schema_version",
            "ref",
            "save_source",
            "load_source",
            "symbols",
            "dependencies",
            "policy",
        }
        assert source["ref"]["owner"] == "central-owner"
        assert source["save_source"] == SAVE_TEXT

        server.record["availability"] = {
            "state": "available",
            "reason_code": "target_environment_verified",
            "environment_fingerprint": "f" * 64,
        }
        status, local_body = asyncio.run(get("/library-adapters?limit=10"))
        assert status == 200
        assert server.calls[-1]["target_machine_id"] == "local-machine"
        assert server.calls[-1]["execution_target"] == "local"
        assert local_body["items"][0]["availability"] == {
            "local": {"state": "available", "reason": None},
            "remote": {
                "state": "requires_target_preflight",
                "reason": "remote_target_not_selected",
            },
        }

        server.fail = True
        status, body = asyncio.run(get("/library-adapters?limit=10"))
        assert status == 503
        assert body["error"]["code"] == "library_adapter_unavailable"
        assert "text-file" not in repr(body)
    finally:
        app.runtime.shutdown()
        store.close()
