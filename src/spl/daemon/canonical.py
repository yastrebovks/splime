"""Canonical object definition bytes for content-addressed versions."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any

import yaml

from spl.core.entities.adapter import DAdapter, DLoadAdapter, DSaveAdapter
from spl.core.entities.artifact import DArtifactRef, _default_tag_from_key
from spl.core.entities.distribution import DDistribution
from spl.core.entities.function import DFunction
from spl.core.entities.node import InputPort, OutputPort
from spl.core.entities.pipeline import DPipeline
from spl.core.ir.utils import SPLSafeLoader

CANONICAL_OBJECT_FORMAT_VERSION = 1
_UNORDERED_METADATA_LIST_KEYS = {
    "aliases",
    "distributions",
    "imports",
    "internal_objects",
    "links",
    "pipeline_nodes",
}


def canonicalize(object_def: Mapping[str, Any]) -> bytes:
    """Return stable bytes for an object definition."""

    normalized = _normalize_plain(object_def)
    text = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return f"{text}\n".encode("utf-8")


def canonical_object_definition(
    *,
    yaml_text: str,
    entrypoint: str,
    env: str,
    env_python_version: str,
    metadata: Mapping[str, Any],
    runtime_config: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the structured definition used for object version identity."""

    return {
        "format_version": CANONICAL_OBJECT_FORMAT_VERSION,
        "entrypoint": str(entrypoint),
        "env": {
            "name": str(env),
            "python_version": str(env_python_version or "unknown"),
        },
        "runtime_config": dict(runtime_config),
        "spl": _canonical_spl_documents(yaml_text),
        "metadata": metadata,
    }


def _canonical_spl_documents(yaml_text: str) -> list[dict[str, Any]]:
    documents = []
    for index, document in enumerate(
        yaml.load_all(yaml_text, Loader=SPLSafeLoader),
        start=1,
    ):
        if not isinstance(document, list) or not document:
            raise ValueError(f"SPL YAML document #{index} must be a non-empty list")
        root, *dependencies = document
        documents.append(
            {
                "root": _canonical_ir(root),
                "dependencies": _sorted_canonical(_canonical_ir(item) for item in dependencies),
            }
        )
    if not documents:
        raise ValueError("SPL YAML does not contain any documents")
    return _sorted_canonical(documents)


def _canonical_ir(value: Any) -> Any:
    if isinstance(value, DFunction):
        return {
            "tag": "DFunction",
            "name": value.name,
            "body": _normalize_text(value.body),
            "inputs": [_canonical_ir(item) for item in value.inputs],
            "outputs": None if value.outputs is None else [_canonical_ir(item) for item in value.outputs],
        }
    if isinstance(value, DPipeline):
        pipeline_document: dict[str, Any] = {
            "tag": "DPipeline",
            "name": value.name,
            "nodes": _sorted_canonical(_canonical_ir(item) for item in value.nodes),
            "links": _sorted_canonical(_canonical_ir(item) for item in value.links),
            "aliases": _sorted_canonical(_canonical_ir(item) for item in value.aliases),
            "adapters": _sorted_canonical(_canonical_ir(item) for item in value.adapters),
        }
        if value.tags:
            pipeline_document["tags"] = _canonical_ir(value.tags)
        return pipeline_document
    if isinstance(value, DAdapter):
        return {
            "tag": "DAdapter",
            "key": value.key,
            "format": value.key.rpartition("@")[2],
            "save": value.save,
            "load": value.load,
            "distributions": _sorted_canonical(_canonical_ir(item) for item in value.distributions),
        }
    if isinstance(value, DSaveAdapter):
        return {
            "tag": "DSaveAdapter",
            "key": value.key,
            "artifact_tag": value.tag,
            "save": value.save,
            "distributions": _sorted_canonical(_canonical_ir(item) for item in value.distributions),
        }
    if isinstance(value, DLoadAdapter):
        return {
            "tag": "DLoadAdapter",
            "key": value.key,
            "accepted_tags": list(value.accepted_tags),
            "load": value.load,
            "distributions": _sorted_canonical(_canonical_ir(item) for item in value.distributions),
        }
    if isinstance(value, DArtifactRef):
        document: dict[str, Any] = {
            "tag": "DArtifactRef",
            "key": value.key,
            "uri": value.uri,
            "sha256": value.sha256,
            "size": value.size,
        }
        if _default_tag_from_key(value.key) != value.tag:
            document["artifact_tag"] = value.tag
        return document
    if isinstance(value, DDistribution):
        return {
            "tag": "DDistribution",
            "package": value.package,
            "version": value.version,
        }
    if isinstance(value, InputPort):
        return {
            "tag": "InputPort",
            "name": value.name,
            "type": value.typ_,
            "default": _canonical_ir(value.default),
        }
    if isinstance(value, OutputPort):
        return {
            "tag": "OutputPort",
            "name": value.name,
            "type": value.typ_,
        }
    if is_dataclass(value):
        return {
            "tag": type(value).__name__,
            **{field.name: _canonical_ir(getattr(value, field.name)) for field in fields(value)},
        }
    if isinstance(value, Mapping):
        return {str(key): _canonical_ir(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, tuple | list):
        return [_canonical_ir(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _normalize_plain(value: Any, path: tuple[str, ...] = ()) -> Any:
    if is_dataclass(value):
        return _normalize_plain(_canonical_ir(value), path)
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_plain(item, (*path, str(key)))
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, tuple | list):
        items = [_normalize_plain(item, path) for item in value]
        if path and path[-1] in _UNORDERED_METADATA_LIST_KEYS:
            return _sorted_canonical(items)
        return items
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, str):
        return _normalize_text(value)
    return value


def _normalize_text(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _sorted_canonical(values: Any) -> list[Any]:
    return sorted(values, key=_canonical_sort_key)


def _canonical_sort_key(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
