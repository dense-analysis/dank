"""HTTP routes for connections: parse the request, call the service, shape."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from dank.api.connections.deps import ConnectionServiceDep
from dank.api.connections.schemas import (
    ConnectionOut,
    ConnectionQuery,
    ConnectionsQuery,
    ConnectionsResponse,
)
from dank.api.deps import single_values

router = APIRouter(prefix="/api/reader", tags=["connections"])


@router.get(
    "/connections",
    dependencies=[Depends(single_values(ConnectionsQuery))],
    response_model_exclude_unset=True,
)
async def list_connections(
    query: Annotated[ConnectionsQuery, Query()], service: ConnectionServiceDep,
) -> ConnectionsResponse:
    """A post's incoming, outgoing and related links, from saved evidence."""
    found = await service.neighbourhood(query.scope())

    return ConnectionsResponse.model_validate(found)


@router.get(
    "/connection",
    dependencies=[Depends(single_values(ConnectionQuery))],
)
async def read_connection(
    query: Annotated[ConnectionQuery, Query()], service: ConnectionServiceDep,
) -> ConnectionOut:
    """One saved link, found exactly even beyond a neighbourhood's limit."""
    found = await service.connection(query.key())

    return ConnectionOut.model_validate(found)
