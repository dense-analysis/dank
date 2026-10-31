"""HTTP routes for sources: parse the request, call the service, shape."""

from fastapi import APIRouter

from dank.api.sources.deps import SourceServiceDep
from dank.api.sources.schemas import SourcesResponse

router = APIRouter(prefix="/api/reader", tags=["sources"])


@router.get("/sources")
async def list_sources(service: SourceServiceDep) -> SourcesResponse:
    """Configured and collected sources, with post counts."""
    overview = await service.overview()

    return SourcesResponse.from_overview(overview)
