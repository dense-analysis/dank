"""Wires ConnectionService to its adapter. Tests override the adapter here."""

from typing import Annotated

from fastapi import Depends

from dank.api.connections.clickhouse import ClickHouseConnectionStore
from dank.api.connections.service import ConnectionService, ConnectionStore
from dank.api.deps import ResourcesDep


def connection_store(resources: ResourcesDep) -> ConnectionStore:
    return ClickHouseConnectionStore(resources.clickhouse)


def connection_service(
    store: Annotated[ConnectionStore, Depends(connection_store)],
) -> ConnectionService:
    return ConnectionService(store)


ConnectionServiceDep = Annotated[
    ConnectionService, Depends(connection_service),
]
