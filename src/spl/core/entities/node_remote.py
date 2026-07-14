import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, cast
from uuid import UUID

import yaml

from spl._deprecate import warn_deprecated
from spl._owner_ref import canonical_owner_from_response, normalize_owner_ref
from spl.core.entities.node import InputPort, Node, OutputPort
from spl.core.ir.common import DBase
from spl.core.ir.parse import _branch, ir_parse
from spl.core.ir.unparse import ir_unparse


@dataclass(frozen=True)
class NodeRemote(Node):
    url: str
    name: str
    version: str
    owner_id: str | None = None
    library: str | None = None
    target_machine: str | None = None

    def __init__(
        self,
        url: str | None = None,
        name: str | None = None,
        version: str | None = "latest",
        inputs: list[InputPort] | None = None,
        outputs: list[OutputPort] | None = None,
        uuid: UUID | None = None,
        *,
        pipeline: str | None = None,
        function: str | None = None,
        owner: str | None = None,
        owner_id: str | None = None,
        library: str | None = None,
        target_machine: str | None = None,
    ) -> None:

        if pipeline is not None:
            if name is not None:
                raise TypeError("pass either name or pipeline/function, not both")
            warn_deprecated("NodeRemote(pipeline=..., function=...)", "NodeRemote.locate(pipeline=..., function=...)")
            name = _remote_name(pipeline, function)
        elif function is not None:
            if name is None:
                raise TypeError("function requires name or pipeline")
            warn_deprecated("NodeRemote(name=..., function=...)", "NodeRemote.locate(name=..., function=...)")
            name = _remote_name(name, function)
        if name is None:
            if url is None:
                raise TypeError("NodeRemote requires object name")
            warn_deprecated("NodeRemote(<object name> passed as the positional url)", "NodeRemote.locate(name=...)")
            name = url
            url = None
        url = "" if url is None else str(url)
        name = str(name)
        version = "latest" if version is None else str(version)
        owner_id = _normalize_owner(owner, owner_id)
        library = None if library is None else str(library)
        target_machine = None if target_machine is None else str(target_machine)

        if inputs is None or outputs is None:
            resolved_inputs, resolved_outputs, resolved_owner_id = _resolve_remote_ports(
                url=url, name=name, version=version, owner_id=owner_id, library=library, target_machine=target_machine
            )
            owner_id = resolved_owner_id
            if inputs is None:
                inputs = resolved_inputs
            if outputs is None:
                outputs = resolved_outputs

        super().__init__(inputs=inputs, outputs=outputs, uuid=uuid)
        object.__setattr__(self, "url", url)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "owner_id", owner_id)
        object.__setattr__(self, "library", library)
        object.__setattr__(self, "target_machine", target_machine)

    @classmethod
    def locate(
        cls,
        *,
        name: str | None = None,
        pipeline: str | None = None,
        function: str | None = None,
        url: str | None = None,
        version: str = "latest",
        owner: str | None = None,
        library: str | None = None,
        target_machine: str | None = None,
    ) -> "NodeRemote":
        """The single documented way to reference a remote object.

        Pass either ``name`` (optionally with ``function``) or
        ``pipeline`` + ``function``.  ``owner`` accepts a canonical user id or
        ``@handle`` and ``library`` selects that user's namespace. When ports
        are resolved through the daemon, the returned canonical owner id
        replaces a handle before serialization. Explicit ``inputs`` and
        ``outputs`` avoid the network round trip and therefore retain the raw
        owner reference until run time. Since 0.2.0 the
        convenience ``__init__`` forms (``pipeline=``/``function=`` keywords,
        object name in the positional ``url`` slot) emit
        ``DeprecationWarning``; ``locate`` is the canonical spelling and the
        plain serialization constructor stays silent.
        """

        if pipeline is not None:
            if name is not None:
                raise TypeError("pass either name or pipeline/function, not both")
            resolved_name = _remote_name(pipeline, function)
        elif function is not None:
            if name is None:
                raise TypeError("function requires name or pipeline")
            resolved_name = _remote_name(name, function)
        elif name is not None:
            resolved_name = str(name)
        else:
            raise TypeError("NodeRemote.locate() requires name or pipeline")

        return cls(
            url=url, name=resolved_name, version=version, owner=owner, library=library, target_machine=target_machine
        )

    def __repr__(self) -> str:
        return "<{}/{}:{}>".format(self.url, self.name, self.version)

    def __hash__(self) -> int:
        return hash(self.uuid)


def _remote_name(object_name: Any, function: Any = None) -> str:
    object_name_text = str(object_name)
    if function is None:
        return object_name_text
    function = str(function)
    if "::" in object_name_text:
        parent, current_function = object_name_text.split("::", 1)
        if not parent or not current_function:
            raise ValueError("remote function reference must look like object::function")
        if current_function != function:
            raise ValueError(
                f"function was provided twice with different values: {current_function!r} and {function!r}"
            )
        return object_name_text
    return f"{object_name_text}::{function}"


def _normalize_owner(owner: Any, owner_id: Any) -> str | None:
    if owner is not None and owner_id is not None and str(owner) != str(owner_id):
        raise ValueError(f"owner and owner_id were both provided with different values: {owner!r} and {owner_id!r}")
    value = owner_id if owner_id is not None else owner
    return normalize_owner_ref(None if value is None else str(value))


def _remote_ref(
    *,
    url: str,
    name: str,
    version: str,
    owner_id: str | None = None,
    library: str | None = None,
    target_machine: str | None = None,
) -> dict[str, Any]:
    ref: dict[str, Any] = {"url": url, "name": name, "version": version}
    if owner_id is not None:
        ref["owner_id"] = owner_id
    if library is not None:
        ref["library"] = library
    if target_machine is not None:
        ref["target_machine"] = target_machine
    return ref


def _resolve_remote_ports(
    url: str,
    name: str,
    version: str,
    *,
    owner_id: str | None = None,
    library: str | None = None,
    target_machine: str | None = None,
) -> tuple[list[InputPort], list[OutputPort], str | None]:
    """Resolve a remote node signature through the local daemon."""

    try:
        from spl.daemon_client import Client

        payload = Client().resolve_remote_signature(
            _remote_ref(
                url=url, name=name, version=version, owner_id=owner_id, library=library, target_machine=target_machine
            )
        )
    except Exception as exc:
        raise RuntimeError(
            "NodeRemote inputs/outputs were omitted, but the local SPL daemon "
            "could not resolve the remote signature. Connect the daemon to "
            "SPLime or pass url, inputs, and outputs explicitly. "
            f"Remote: {name}:{version}; error: {exc}"
        ) from exc

    signature = payload.get("signature") if isinstance(payload, dict) else payload
    if not isinstance(signature, dict):
        raise RuntimeError(f"NodeRemote signature resolver returned an invalid response for {name}:{version}")
    remote = signature.get("remote")
    remote_ref = signature.get("remote_ref")
    response_owner = remote.get("owner_id") if isinstance(remote, dict) else None
    if response_owner is None and isinstance(remote_ref, dict):
        response_owner = remote_ref.get("owner_id")
    if response_owner is None:
        response_owner = signature.get("owner_id")
    try:
        resolved_owner_id = canonical_owner_from_response(owner_id, response_owner)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"NodeRemote signature resolver returned an invalid owner for {name}:{version}: {exc}"
        ) from exc
    return (
        [_signature_input_to_port(item) for item in signature.get("inputs") or []],
        _signature_outputs_to_ports(signature.get("outputs") or []),
        resolved_owner_id,
    )


def _signature_input_to_port(item: dict[str, Any]) -> InputPort:
    return InputPort(name=str(item.get("name") or "default"), typ_=item.get("type"), default=item.get("default"))


def _signature_outputs_to_ports(outputs: list[dict[str, Any]]) -> list[OutputPort]:
    ports = []
    seen = set()
    for item in outputs:
        raw_ports = item.get("ports")
        candidates = raw_ports or [{"name": item.get("name") or "default", "type": item.get("type")}]
        for port in candidates:
            name = str(port.get("name") or "default")
            if name in seen:
                continue
            seen.add(name)
            ports.append(OutputPort(name=name, typ_=port.get("type")))
    return ports or [OutputPort(name="default", typ_=None)]


@dataclass(frozen=True)
class DNodeRemote(DBase):
    uuid: str
    url: str
    name: str
    version: str
    owner_id: str | None = None
    library: str | None = None
    target_machine: str | None = None


def _dnode_remote_mapping(data: DNodeRemote) -> dict[str, str]:
    payload = {"uuid": data.uuid, "url": data.url, "name": data.name, "version": data.version}
    if data.owner_id is not None:
        payload["owner_id"] = data.owner_id
    if data.library is not None:
        payload["library"] = data.library
    if data.target_machine is not None:
        payload["target_machine"] = data.target_machine
    return payload


yaml.add_representer(
    DNodeRemote, lambda dumper, data: dumper.represent_mapping("!DNodeRemote", _dnode_remote_mapping(data))
)

yaml.add_constructor(
    "!DNodeRemote", lambda loader, node: DNodeRemote(**cast(dict[str, Any], loader.construct_mapping(cast(Any, node))))
)


@ir_parse.register(lambda x: isinstance(x, NodeRemote))
def _ir_parse__node_remote(x: NodeRemote, name: str | None = None) -> _branch:

    return _branch(
        x,
        lambda: DNodeRemote(
            uuid=str(x.uuid),
            url=x.url,
            name=x.name,
            version=x.version,
            owner_id=x.owner_id,
            library=x.library,
            target_machine=x.target_machine,
        ),
        lambda _: [],
    )


@ir_unparse.register(lambda x: isinstance(x, DNodeRemote))
def _ir_unparse__node_function(x: DNodeRemote, source: Path) -> Generator[ast.stmt]:
    keywords = [
        ast.keyword(
            arg="uuid", value=ast.Call(func=ast.Name(id="UUID", ctx=ast.Load()), args=[ast.Constant(value=x.uuid)])
        ),
        ast.keyword(arg="url", value=ast.Constant(value=x.url)),
        ast.keyword(arg="name", value=ast.Constant(value=x.name)),
        ast.keyword(arg="version", value=ast.Constant(value=x.version)),
        # TODO: from context
        ast.keyword(arg="inputs", value=ast.List(elts=[], ctx=ast.Load())),
        ast.keyword(arg="outputs", value=ast.List(elts=[], ctx=ast.Load())),
    ]
    for attr in ("owner_id", "library", "target_machine"):
        value = getattr(x, attr)
        if value is not None:
            keywords.append(ast.keyword(arg=attr, value=ast.Constant(value=value)))

    yield ast.Assign(
        targets=[ast.Name(id="_node", ctx=ast.Store())],
        value=ast.Call(func=ast.Name(id="NodeRemote", ctx=ast.Load()), keywords=keywords),
    )
