"""HTTP routes for refresh jobs: parse the request, call the service, shape."""

from fastapi import APIRouter

from dank.api.refresh.deps import RefreshServiceDep
from dank.api.refresh.schemas import (
    RefreshJobResponse,
    RefreshJobsResponse,
    RefreshRequest,
)

router = APIRouter(prefix="/api/reader", tags=["refresh"])


@router.get("/refresh", response_model_exclude_unset=True)
async def list_refresh_jobs(service: RefreshServiceDep) -> RefreshJobsResponse:
    """Recent refresh jobs, newest first. Reading never starts a collection."""
    return RefreshJobsResponse.model_validate({"jobs": service.jobs()})


@router.post("/refresh", status_code=202, response_model_exclude_unset=True)
async def start_refresh(
    body: RefreshRequest, service: RefreshServiceDep,
) -> RefreshJobResponse:
    """Queue a collection, or return the active job with the same scope."""
    job = await service.start(body.scope_value())

    return RefreshJobResponse.model_validate({"job": job})
