"""The legacy viewer's pages: parse the request, call the service, render.

They stay out of OpenAPI, which describes the JSON API.
"""

from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from dank.api.viewer.deps import PageSizeDep, ViewerPagesDep, ViewerServiceDep
from dank.api.viewer.schemas import IndexQuery, PostQuery

router = APIRouter(include_in_schema=False)


@router.get("/")
async def index(
    query: Annotated[IndexQuery, Query()],
    service: ViewerServiceDep,
    pages: ViewerPagesDep,
    page_size: PageSizeDep,
) -> HTMLResponse:
    filters = query.filters(page_size)
    page = await service.index(filters)

    return HTMLResponse(pages.index(filters, page))


@router.get("/post")
async def post_detail(
    query: Annotated[PostQuery, Query()],
    service: ViewerServiceDep,
    pages: ViewerPagesDep,
) -> HTMLResponse:
    page = await service.detail(query.domain, query.post_id)

    return HTMLResponse(pages.post(page))
