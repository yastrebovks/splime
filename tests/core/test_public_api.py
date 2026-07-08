"""Golden snapshot of the public :mod:`spl` surface.

If this test fails, the public API changed. Either the change is intentional —
then update the expected values below in the SAME PR — or it is accidental and
must be reverted.
"""

from __future__ import annotations

from dataclasses import dataclass
import inspect
import importlib
from types import ModuleType
from typing import cast
import warnings

import pytest
import spl
import spl._client
import spl.core._common
import spl.core.entities.node

EXPECTED_ALL = {
    "SPLClient",
    "SPLServerClient",
    "RemoteRun",
    "RemoteResult",
    "PublishedObject",
    "NodeRemote",
    "lift",
    "Deployment",
    "DEFAULT_PORT",
    "InputPort",
    "OutputPort",
    "DDistribution",
    "spl_export_to_file",
    "spl_export_to_dir",
    "spl_import_from_file",
}


@dataclass(frozen=True)
class CanonicalExports:
    module: str
    names: tuple[str, ...]


@dataclass(frozen=True)
class ShimFacade:
    module: str
    canonical_exports: tuple[CanonicalExports, ...]
    owned_exports: tuple[str, ...] = ()


# Add a case here whenever a module is introduced primarily as a compatibility
# shim or facade over implementation modules.  The explicit names are the lock:
# deleting a re-export from the import block or from __all__ must make this test
# fail instead of silently updating the expected surface.
SHIM_FACADES = (
    ShimFacade(
        module="spl",
        canonical_exports=(
            CanonicalExports(
                "spl._client",
                ("PublishedObject", "RemoteResult", "RemoteRun", "SPLClient"),
            ),
            CanonicalExports("spl.server_client", ("SPLServerClient",)),
            CanonicalExports("spl.core.entities.node_remote", ("NodeRemote",)),
            CanonicalExports("spl.core._common", ("Deployment", "lift")),
            CanonicalExports(
                "spl.core.entities.node",
                ("DEFAULT_PORT", "InputPort", "OutputPort"),
            ),
            CanonicalExports("spl.core.entities.distribution", ("DDistribution",)),
            CanonicalExports(
                "spl.core.ir.utils",
                (
                    "spl_export_to_dir",
                    "spl_export_to_file",
                    "spl_import_from_file",
                ),
            ),
        ),
    ),
    ShimFacade(
        module="spl.client",
        canonical_exports=(
            CanonicalExports(
                "spl._client",
                (
                    "ObjectCatalog",
                    "ObjectList",
                    "ObjectScope",
                    "ObjectTable",
                    "OfflinePolicy",
                    "ProgressOption",
                    "PublishedObject",
                    "RemoteResult",
                    "RemoteRun",
                    "RunSource",
                    "SPLClient",
                    "build_runtime_config",
                    "export_object_to_yaml",
                    "export_objects_to_yaml",
                    "prepare_export_object",
                    "read_yaml_input",
                ),
            ),
        ),
    ),
    ShimFacade(
        module="spl.core.common",
        canonical_exports=(
            CanonicalExports(
                "spl.core._common",
                (
                    "Deployment",
                    "PipelineBuilder",
                    "Run",
                    "decode",
                    "encode",
                    "lift",
                ),
            ),
        ),
    ),
    ShimFacade(
        module="spl.core",
        canonical_exports=(
            CanonicalExports("spl.core.entities.node_remote", ("NodeRemote",)),
            CanonicalExports(
                "spl.core.ir.utils",
                (
                    "spl_export_to_dir",
                    "spl_export_to_file",
                    "spl_import_from_file",
                ),
            ),
        ),
    ),
    ShimFacade(
        module="spl.daemon",
        canonical_exports=(CanonicalExports("spl.daemon.client", ("Client",)),),
    ),
    ShimFacade(
        module="spl.daemon.client",
        canonical_exports=(
            CanonicalExports(
                "spl.daemon_client",
                (
                    "DAEMON_API_TOKEN_ENV",
                    "DAEMON_ENDPOINT_FILENAME",
                    "DEFAULT_DAEMON_HOST",
                    "DEFAULT_DAEMON_PORT",
                    "DEFAULT_HEARTBEAT_INTERVAL_SECONDS",
                    "DEFAULT_SERVER_URL",
                    "DEFAULT_URL",
                    "Client",
                    "ClientError",
                    "clear_daemon_endpoint",
                    "daemon_endpoint_file",
                    "daemon_url",
                    "default_daemon_home",
                    "generate_daemon_api_token",
                    "read_daemon_endpoint",
                    "resolve_api_token",
                    "resolve_base_url",
                    "write_daemon_endpoint",
                ),
            ),
        ),
    ),
    ShimFacade(
        module="spl.daemon.environment",
        canonical_exports=(
            CanonicalExports(
                "spl.daemon.environment_base",
                (
                    "ABSENT",
                    "CREATING",
                    "DEFAULT_BUILD_TIMEOUT_SECONDS",
                    "DEFAULT_STALE_LOCK_SECONDS",
                    "EnvironmentBuildError",
                    "FAILED",
                    "READY",
                ),
            ),
        ),
        owned_exports=("EnvironmentManager",),
    ),
    ShimFacade(
        module="spl.daemon.repositories",
        canonical_exports=(
            CanonicalExports("spl.daemon.repositories.env", ("EnvRepository",)),
            CanonicalExports("spl.daemon.repositories.library", ("LibraryRepository",)),
            CanonicalExports("spl.daemon.repositories.object", ("ObjectRepository",)),
            CanonicalExports("spl.daemon.repositories.run", ("RunRepository",)),
            CanonicalExports(
                "spl.daemon.repositories.server_connection",
                ("ServerConnectionRepository",),
            ),
            CanonicalExports(
                "spl.daemon.repositories.sync_event",
                ("SyncEventRepository",),
            ),
        ),
    ),
    ShimFacade(
        module="spl.daemon.runtime_dependencies",
        canonical_exports=(
            CanonicalExports(
                "spl.daemon.environment_base",
                ("EnvironmentManagerProtocol",),
            ),
        ),
        owned_exports=(
            "DockerEnvironmentBuilderProtocol",
            "DockerEnvironmentManagerProtocol",
            "DockerPoolRunnerProtocol",
            "HeartbeatsProtocol",
            "RuntimeBackendProtocol",
            "ServerClientFactoryProtocol",
            "ServerClientProtocol",
            "ServerConnectionsProtocol",
            "SyncVisibilityProtocol",
        ),
    ),
    ShimFacade(
        module="spl.daemon.store",
        canonical_exports=(
            CanonicalExports(
                "spl.daemon.storage_base",
                (
                    "DEFAULT_HEARTBEAT_INTERVAL_SECONDS",
                    "DEFAULT_OBJECT_LIBRARY",
                    "DEFAULT_OBJECT_OWNER_ID",
                    "FUNCTION_REF_SEPARATOR",
                    "NAME_PATTERN",
                    "REDACTED_SECRET_VALUE",
                    "StorageBase",
                    "iso_after_now",
                    "json_dumps",
                    "json_loads",
                    "normalize_heartbeat_interval",
                    "read_json",
                    "split_object_function_ref",
                    "utc_now",
                    "validate_name",
                    "write_json",
                ),
            ),
        ),
        owned_exports=("RegistryStore",),
    ),
)


def test_public_all_matches_snapshot() -> None:
    assert set(spl.__all__) == EXPECTED_ALL


@pytest.mark.parametrize(
    "facade",
    SHIM_FACADES,
    ids=[facade.module for facade in SHIM_FACADES],
)
def test_shim_facade_reexports_are_locked(facade: ShimFacade) -> None:
    module = import_module_suppressing_deprecations(facade.module)
    actual_all = module_all(module)
    expected_all = expected_exports(facade)
    module_globals = vars(module)

    assert set(actual_all) == set(expected_all)
    assert len(actual_all) == len(set(actual_all))

    for name in actual_all:
        assert name in module_globals, f"{facade.module}.__all__ exposes missing global {name!r}"

    for canonical in facade.canonical_exports:
        canonical_module = import_module_suppressing_deprecations(canonical.module)
        for name in canonical.names:
            assert module_globals[name] is getattr(canonical_module, name)

    for name in facade.owned_exports:
        assert name in module_globals, f"{facade.module} is missing owned export {name!r}"


def test_facade_symbols_are_canonical() -> None:
    assert spl.lift is spl.core._common.lift
    assert spl.Deployment is spl.core._common.Deployment
    assert spl.SPLClient is spl._client.SPLClient
    assert spl.DEFAULT_PORT == spl.core.entities.node.DEFAULT_PORT == "default"


def test_call_signature_keeps_expected_parameters() -> None:
    params = inspect.signature(spl.SPLClient.call).parameters
    for name in ("name", "kwargs", "output", "function", "target_machine", "adapters"):
        assert name in params


def test_objects_signature_keeps_expected_parameters() -> None:
    params = inspect.signature(spl.SPLClient.objects).parameters
    for name in ("compact", "scope"):
        assert name in params


def import_module_suppressing_deprecations(module_name: str) -> ModuleType:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return importlib.import_module(module_name)


def expected_exports(facade: ShimFacade) -> tuple[str, ...]:
    names: list[str] = []
    for canonical in facade.canonical_exports:
        names.extend(canonical.names)
    names.extend(facade.owned_exports)
    return tuple(names)


def module_all(module: ModuleType) -> tuple[str, ...]:
    raw_all = getattr(module, "__all__", None)
    if not isinstance(raw_all, list | tuple):
        raise AssertionError(f"{module.__name__} must define __all__")
    if not all(isinstance(name, str) for name in raw_all):
        raise AssertionError(f"{module.__name__}.__all__ must contain only strings")
    return cast(tuple[str, ...], tuple(raw_all))
