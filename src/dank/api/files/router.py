"""The file routes that aren't mounts: both lead to the reader."""

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

router = APIRouter(include_in_schema=False)


# Relative, because behind the HTTPS proxy this server sees plain HTTP.
@router.get("/")
@router.get("/reader")
async def reader_entry() -> RedirectResponse:
    return RedirectResponse("/reader/", status_code=302)
