"""The one file route that isn't a mount: the reader's old address."""

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from dank.api.files.model import reader_location

router = APIRouter(include_in_schema=False)


@router.get("/reader")
@router.get("/reader/{path:path}")
async def old_reader_address(request: Request) -> RedirectResponse:
    # Relative, because behind the HTTPS proxy this server sees plain HTTP.
    location = reader_location(
        request.path_params.get("path", ""), request.url.query,
    )

    return RedirectResponse(location, status_code=302)
