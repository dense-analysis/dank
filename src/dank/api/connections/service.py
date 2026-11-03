"""Connection use cases, and the read store they need."""

from __future__ import annotations

from typing import Protocol

from dank.api.connections.model import (
    ConnectionKey,
    ConnectionPayload,
    ConnectionScope,
    NeighbourhoodPayload,
)
from dank.errors import NotFound


class ConnectionStore(Protocol):
    async def neighbourhood(
        self, scope: ConnectionScope,
    ) -> NeighbourhoodPayload | None: ...

    async def connection(
        self, key: ConnectionKey,
    ) -> ConnectionPayload | None: ...


class ConnectionService:
    def __init__(self, store: ConnectionStore) -> None:
        self._store = store

    async def neighbourhood(
        self, scope: ConnectionScope,
    ) -> NeighbourhoodPayload:
        found = await self._store.neighbourhood(scope)

        if found is None:
            raise NotFound("Article not found")

        return found

    async def connection(self, key: ConnectionKey) -> ConnectionPayload:
        found = await self._store.connection(key)

        if found is None:
            raise NotFound("Saved evidence not found")

        return found
