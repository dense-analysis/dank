"""The source icon route: parse the request, call the service, shape."""

from typing import Annotated

from fastapi import APIRouter, Query, Response

from dank.api.source_icons.deps import SourceIconServiceDep
from dank.api.source_icons.schemas import SourceIconQuery

router = APIRouter(prefix="/api/reader", tags=["source icons"])
# Icons rarely change, and every card in the reader's feed shows one.
CACHE_FOR_A_DAY = "public, max-age=86400"


@router.get(
    "/source-icon",
    response_class=Response,
    responses={200: {"content": {"image/png": {}}}},
)
async def read_source_icon(
    query: Annotated[SourceIconQuery, Query()], service: SourceIconServiceDep,
) -> Response:
    """A source's favicon as a small PNG, captured on first use."""
    icon = await service.icon(query.domain)

    return Response(
        icon,
        media_type="image/png",
        headers={"cache-control": CACHE_FOR_A_DAY},
    )
