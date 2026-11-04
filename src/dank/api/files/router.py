"""The one file route that isn't a mount."""

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

router = APIRouter(include_in_schema=False)


@router.get("/reader")
async def reader_entry() -> RedirectResponse:
    # Relative, because behind the HTTPS proxy this server sees plain HTTP.
    return RedirectResponse("/reader/", status_code=302)
