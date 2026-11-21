"""The source icon route: parse the request, call the service, shape."""

from typing import Annotated

from fastapi import APIRouter, Query, Response

from dank.api.source_icons.deps import CollectedDomainsDep, IconCacheDep
from dank.api.source_icons.schemas import SourceIconQuery
from dank.api.source_icons.service import source_icon
from dank.api.sources.deps import LoadSourcesDep

router = APIRouter(prefix="/api/reader", tags=["source icons"])
# Icons rarely change, and every card in the reader's feed shows one.
CACHE_FOR_A_DAY = "public, max-age=86400"


@router.get(
    "/source-icon",
    response_class=Response,
    responses={200: {"content": {"image/png": {}}}},
)
async def read_source_icon(
    query: Annotated[SourceIconQuery, Query()],
    load_sources: LoadSourcesDep,
    collected: CollectedDomainsDep,
    icons: IconCacheDep,
) -> Response:
    """A source's favicon as a small PNG, captured on first use."""
    icon = await source_icon(query.domain, load_sources, collected, icons)

    return Response(
        icon,
        media_type="image/png",
        headers={"cache-control": CACHE_FOR_A_DAY},
    )
