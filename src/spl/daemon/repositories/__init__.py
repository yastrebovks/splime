"""Aggregate repositories for the daemon registry store."""

from spl.daemon.repositories.env import EnvRepository
from spl.daemon.repositories.library import LibraryRepository
from spl.daemon.repositories.object import ObjectRepository
from spl.daemon.repositories.run import RunRepository
from spl.daemon.repositories.server_connection import ServerConnectionRepository
from spl.daemon.repositories.sync_event import SyncEventRepository

__all__ = [
    "EnvRepository",
    "LibraryRepository",
    "ObjectRepository",
    "RunRepository",
    "ServerConnectionRepository",
    "SyncEventRepository",
]
