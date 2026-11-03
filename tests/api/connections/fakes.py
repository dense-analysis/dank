"""In-memory stand-ins for the connections feature's driven adapter."""

from dank.api.connections.model import (
    ConnectionKey,
    ConnectionPayload,
    ConnectionScope,
    NeighbourhoodPayload,
)
from dank.api.connections.service import ConnectionStore


class FakeConnectionStore:
    """Answers from values a test sets, keyed by the normalized request."""

    def __init__(self) -> None:
        self.neighbourhoods: dict[ConnectionScope, NeighbourhoodPayload] = {}
        self.connections: dict[ConnectionKey, ConnectionPayload] = {}

    async def neighbourhood(
        self, scope: ConnectionScope,
    ) -> NeighbourhoodPayload | None:
        return self.neighbourhoods.get(scope)

    async def connection(self, key: ConnectionKey) -> ConnectionPayload | None:
        return self.connections.get(key)


# Pyright checks this assignment, so the fake can't drift from the port.
_PORT: type[ConnectionStore] = FakeConnectionStore
