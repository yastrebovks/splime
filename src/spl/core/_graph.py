from typing import Any
from uuid import UUID

from spl.core.entities.node import InputPort, Node, NodeInputRef


def canonical_uuid_key(value: Any) -> str:
    """Return canonical UUID text without mutating the declared value."""

    if not isinstance(value, (UUID, str)) or not str(value):
        raise ValueError("UUID value must be a non-empty UUID string or UUID")
    return str(UUID(str(value)))


def node_sort_key(node: Node) -> str:
    """Return the canonical, process-stable ordering key for a pipeline node."""

    return canonical_uuid_key(node.uuid)


def input_port_sort_key(port: InputPort) -> tuple[str, bool, str, bool, str]:
    """Return a total stable key for every currently accepted input port."""

    return (
        port.name,
        port.typ_ is not None,
        port.typ_ or "",
        port.default is not None,
        port.default or "",
    )


def node_input_ref_sort_key(ref: NodeInputRef) -> tuple[str, str, bool, str, bool, str]:
    """Return the canonical ordering key for a pipeline input reference."""

    return node_sort_key(ref.node), *input_port_sort_key(ref.port)
