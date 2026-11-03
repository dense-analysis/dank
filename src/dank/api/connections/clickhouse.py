"""ConnectionStore read from saved evidence in ClickHouse.

SQL and the projection live in the reader helpers this reuses, so the queries
and payloads are the ones the aiohttp server uses.
"""

from __future__ import annotations

from dank.api.connections.model import (
    ConnectionKey,
    ConnectionPayload,
    ConnectionScope,
    NeighbourhoodPayload,
)
from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_connections import connections, saved_connection


class ClickHouseConnectionStore:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    async def neighbourhood(
        self, scope: ConnectionScope,
    ) -> NeighbourhoodPayload | None:
        return await connections(self._client, _present(scope._asdict()))

    async def connection(self, key: ConnectionKey) -> ConnectionPayload | None:
        return await saved_connection(self._client, _present(key._asdict()))


def _present(params: dict[str, str]) -> dict[str, str]:
    # The helpers read an absent source, community or timestamp as "all".
    return {
        key: value for key, value in params.items()
        if value or key == "reference_id"
    }
