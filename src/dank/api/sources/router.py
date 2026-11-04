"""HTTP routes for sources: parse the request, call the service, shape."""

from fastapi import APIRouter

from dank.api.deps import SettingsDep
from dank.api.sources.deps import SourceServiceDep
from dank.api.sources.schemas import CategoriesResponse, SourcesResponse

router = APIRouter(prefix="/api/reader", tags=["sources"])


@router.get("/sources")
async def list_sources(service: SourceServiceDep) -> SourcesResponse:
    """Configured and collected sources, with post counts."""
    overview = await service.overview()

    return SourcesResponse.from_overview(overview)


@router.get("/categories")
async def list_categories(settings: SettingsDep) -> CategoriesResponse:
    """Category names configured for the reader."""
    return CategoriesResponse(categories=[
        category.name for category in settings.categories.definitions
    ])
