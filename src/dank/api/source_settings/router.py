"""HTTP routes for managing sources: parse, call the service, shape.

Collection and the reader use these sources once `sources_in_database` is on.
See docs/features/sources.md.
"""

import uuid

from fastapi import APIRouter

from dank.api.deps import SettingsDep
from dank.api.source_settings.deps import SourceServiceDep
from dank.api.source_settings.schemas import (
    SourceBody,
    SourceListResponse,
    SourceOut,
)

router = APIRouter(prefix="/api/reader/source-settings", tags=["sources"])


@router.get("")
async def list_saved_sources(
    service: SourceServiceDep, settings: SettingsDep,
) -> SourceListResponse:
    """Every source, disabled ones too, in the order they were added."""
    return SourceListResponse.model_validate({
        "sources": await service.sources(),
        "in_database": settings.sources_in_database,
    })


@router.get("/{source_id}")
async def read_saved_source(
    source_id: uuid.UUID, service: SourceServiceDep,
) -> SourceOut:
    return SourceOut.model_validate(await service.source(source_id))


@router.post("", status_code=201)
async def add_source(body: SourceBody, service: SourceServiceDep) -> SourceOut:
    """Add a source. The server gives it an ID and normalizes its values."""
    return SourceOut.model_validate(await service.add(body.new_source()))


@router.put("/{source_id}")
async def update_source(
    source_id: uuid.UUID, body: SourceBody, service: SourceServiceDep,
) -> SourceOut:
    """Replace a source, including whether it and each target are enabled.

    Repeating an edit is safe. When two tabs edit one source, the last save
    wins.
    """
    return SourceOut.model_validate(
        await service.update(body.source(source_id)),
    )
