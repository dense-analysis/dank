"""HTTP routes for managing sources: parse, call the service, shape.

Nothing reads these sources for collection yet. See docs/features/sources.md.
"""

import uuid

from fastapi import APIRouter

from dank.api.source_settings.deps import SourceServiceDep
from dank.api.source_settings.schemas import (
    SourceBody,
    SourceListResponse,
    SourceOut,
)

router = APIRouter(prefix="/api/reader/source-settings", tags=["sources"])


@router.get("")
async def list_sources(service: SourceServiceDep) -> SourceListResponse:
    """Every source, disabled ones too, in the order they were added."""
    sources = await service.sources()

    return SourceListResponse.model_validate({"sources": sources})


@router.get("/{source_id}")
async def read_source(
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
