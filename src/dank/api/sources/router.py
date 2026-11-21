"""HTTP routes for sources: parse the request, call the service, shape."""

from fastapi import APIRouter

from dank.api.deps import SettingsDep
from dank.api.sources.deps import CollectionStatsDep, LoadSourcesDep
from dank.api.sources.schemas import CategoriesResponse, SourcesResponse
from dank.api.sources.service import source_overview

router = APIRouter(prefix="/api/reader", tags=["sources"])


@router.get("/sources")
async def list_sources(
    load_sources: LoadSourcesDep,
    stats: CollectionStatsDep,
    settings: SettingsDep,
) -> SourcesResponse:
    """Configured and collected sources, with post counts."""
    overview = await source_overview(load_sources, stats)

    return SourcesResponse.from_overview(
        overview, in_database=settings.sources_in_database,
    )


@router.get("/categories")
async def list_categories(settings: SettingsDep) -> CategoriesResponse:
    """Category names configured for the reader."""
    return CategoriesResponse(categories=[
        category.name for category in settings.categories.definitions
    ])
