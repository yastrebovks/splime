"""Run-scoped authorization for worker callbacks into the local daemon."""

from __future__ import annotations

import hashlib
import secrets
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from spl._timeout import TimeoutDomain, validate_timeout_seconds
from spl.core._graph import canonical_uuid_key
from spl.core.json_contract import validate_json_value


CALLBACK_CAPABILITY_ENV = "SPL_DAEMON_CALLBACK_CAPABILITY"
CALLBACK_CAPABILITY_PREFIX = "splcb_"
CALLBACK_ROUTE = ("POST", "/remote-nodes/run")
DEFAULT_CALLBACK_CAPABILITY_TTL_SECONDS = 24 * 60 * 60
CALLBACK_CAPABILITY_TIMEOUT_GRACE_SECONDS = 30.0
CallbackVersionIdentity = tuple[str, str | int | float | bool | None]
_CALLBACK_NODE_FIELDS = frozenset(
    {
        "uuid",
        "url",
        "name",
        "version",
        "owner_id",
        "library",
        "target_machine",
    }
)
_CALLBACK_NODE_REQUIRED_FIELDS = frozenset({"uuid", "url", "name", "version"})


def callback_capability_ttl_seconds(timeout_seconds: float | None) -> float:
    """Return the finite capability lifetime for one worker run."""

    timeout = validate_timeout_seconds(
        timeout_seconds,
        name="worker run timeout",
        domain=TimeoutDomain.NON_NEGATIVE,
        allow_none=True,
    )
    if timeout is None:
        return float(DEFAULT_CALLBACK_CAPABILITY_TTL_SECONDS)
    return timeout + CALLBACK_CAPABILITY_TIMEOUT_GRACE_SECONDS


@dataclass(frozen=True)
class CallbackNodeIdentity:
    """Immutable identity fields that one callback capability may invoke."""

    node_id: str
    server_url: str | None
    object_name: str
    version: CallbackVersionIdentity
    owner_id: str | None
    library: str | None
    target_machine: str | None


@dataclass(frozen=True)
class CallbackCapabilityPrincipal:
    """Non-secret authorization result attached to one daemon request."""

    run_id: str
    allowed_nodes: frozenset[CallbackNodeIdentity]


@dataclass(frozen=True)
class _CapabilityRecord:
    run_id: str
    allowed_routes: frozenset[tuple[str, str]]
    allowed_nodes: frozenset[CallbackNodeIdentity]
    expires_at: float


class CallbackCapabilityAuthority:
    """Mint, validate, and revoke hash-only worker callback capabilities."""

    def __init__(
        self,
        run_status: Callable[[str], str | None],
        *,
        clock: Callable[[], float] = time.monotonic,
        token_factory: Callable[[int], str] = secrets.token_urlsafe,
    ) -> None:
        self._run_status = run_status
        self._clock = clock
        self._token_factory = token_factory
        self._lock = threading.RLock()
        self._records: dict[bytes, _CapabilityRecord] = {}
        self._digests_by_run: dict[str, set[bytes]] = {}

    def mint(
        self,
        run_id: str,
        remote_nodes: Iterable[Mapping[str, Any]],
        *,
        ttl_seconds: float,
    ) -> str:
        """Mint one capability bound to a run and its registered remote nodes."""

        normalized_ttl = validate_timeout_seconds(
            ttl_seconds,
            name="worker callback capability TTL",
            domain=TimeoutDomain.POSITIVE,
            allow_none=False,
        )
        assert normalized_ttl is not None
        allowed_nodes = frozenset(callback_node_identity_from_record(node) for node in remote_nodes)
        if not allowed_nodes:
            raise ValueError("worker callback capability requires at least one registered remote node")

        with self._lock:
            while True:
                token = CALLBACK_CAPABILITY_PREFIX + self._token_factory(32)
                digest = self._digest(token)
                if digest not in self._records:
                    break
            self._records[digest] = _CapabilityRecord(
                run_id=run_id,
                allowed_routes=frozenset({CALLBACK_ROUTE}),
                allowed_nodes=allowed_nodes,
                expires_at=self._clock() + normalized_ttl,
            )
            self._digests_by_run.setdefault(run_id, set()).add(digest)
        return token

    def authenticate(
        self,
        token: str,
        *,
        method: str,
        path: str,
    ) -> CallbackCapabilityPrincipal | None:
        """Return a principal only for a live run and an allowed route."""

        if not token.startswith(CALLBACK_CAPABILITY_PREFIX):
            return None
        digest = self._digest(token)
        with self._lock:
            record = self._records.get(digest)
            if record is None:
                return None
            if self._clock() >= record.expires_at:
                self._remove_digest(digest, record.run_id)
                return None
            if (method.upper(), path) not in record.allowed_routes:
                return None

        try:
            status = self._run_status(record.run_id)
        except (KeyError, RuntimeError):
            status = None
        if status != "running":
            self.revoke_run(record.run_id)
            return None

        with self._lock:
            if self._records.get(digest) != record:
                return None
        return CallbackCapabilityPrincipal(
            run_id=record.run_id,
            allowed_nodes=record.allowed_nodes,
        )

    @staticmethod
    def authorizes_node(
        principal: CallbackCapabilityPrincipal,
        node: Any,
    ) -> bool:
        """Return whether a request node exactly matches the registered identity."""

        if not isinstance(node, Mapping):
            return False
        try:
            identity = callback_node_identity_from_request(node)
        except (TypeError, ValueError):
            return False
        return identity in principal.allowed_nodes

    def revoke_run(self, run_id: str) -> None:
        """Revoke every callback capability issued for ``run_id``."""

        with self._lock:
            for digest in self._digests_by_run.pop(run_id, set()):
                self._records.pop(digest, None)

    def clear(self) -> None:
        """Revoke all capabilities during daemon shutdown."""

        with self._lock:
            self._records.clear()
            self._digests_by_run.clear()

    @staticmethod
    def _digest(token: str) -> bytes:
        return hashlib.sha256(token.encode("utf-8")).digest()

    def _remove_digest(self, digest: bytes, run_id: str) -> None:
        self._records.pop(digest, None)
        run_digests = self._digests_by_run.get(run_id)
        if run_digests is None:
            return
        run_digests.discard(digest)
        if not run_digests:
            self._digests_by_run.pop(run_id, None)


def callback_node_identity_from_record(node: Mapping[str, Any]) -> CallbackNodeIdentity:
    """Build a callback identity from registered pipeline-node metadata."""

    remote_value = node.get("remote")
    remote = remote_value if isinstance(remote_value, Mapping) else {}
    return CallbackNodeIdentity(
        # Repository projections add a row ``id`` while preserving the graph
        # UUID as ``node_id``.  The graph UUID is the immutable callback
        # identity; use the row id only for the compact registration shape.
        node_id=canonical_uuid_key(node.get("node_id") or node.get("id")),
        server_url=_optional_url(remote.get("url")),
        object_name=_required_text(remote.get("name") or node.get("name"), "registered remote node name"),
        version=_version_value(remote.get("version")),
        owner_id=_optional_text(remote.get("owner_id")),
        library=_optional_text(remote.get("library")),
        target_machine=_optional_text(remote.get("target_machine")),
    )


def callback_node_identity_from_request(node: Mapping[str, Any]) -> CallbackNodeIdentity:
    """Build a callback identity from the worker's request payload."""

    unknown_fields = set(node) - _CALLBACK_NODE_FIELDS
    if unknown_fields:
        raise ValueError("remote callback node contains unsupported identity fields")
    missing_fields = _CALLBACK_NODE_REQUIRED_FIELDS - set(node)
    if missing_fields:
        raise ValueError("remote callback node is missing required identity fields")
    return CallbackNodeIdentity(
        node_id=canonical_uuid_key(node["uuid"]),
        server_url=_optional_url(node["url"]),
        object_name=_required_text(node["name"], "remote callback node name"),
        version=_version_value(node["version"]),
        owner_id=_optional_text(node.get("owner_id")),
        library=_optional_text(node.get("library")),
        target_machine=_optional_text(node.get("target_machine")),
    )


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional remote callback identity fields must be strings or null")
    return value


def _optional_url(value: Any) -> str | None:
    text = _optional_text(value)
    return None if text is None else text.rstrip("/")


def _version_value(value: Any) -> CallbackVersionIdentity:
    if value is not None and type(value) not in {str, int, float, bool}:
        raise ValueError("remote node version must be a JSON scalar value")
    validate_json_value(value, path="$.node.version")
    return (type(value).__name__, value)
