"""HTTP routes for browsing posts: parse the request, call the service, shape.

Fields a post lacks are left out rather than null, as they always have been,
so each route excludes what its schema builder didn't set.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from dank.api.deps import single_values
from dank.api.posts.deps import PostServiceDep
from dank.api.posts.schemas import (
    PostDetailOut,
    PostQuery,
    PostsQuery,
    PostsResponse,
    XAuthorsQuery,
    XAuthorsResponse,
)

router = APIRouter(prefix="/api/reader", tags=["posts"])


@router.get(
    "/posts",
    dependencies=[Depends(single_values(PostsQuery))],
    response_model_exclude_unset=True,
)
async def list_posts(
    query: Annotated[PostsQuery, Query()], service: PostServiceDep,
) -> PostsResponse:
    """One page of collected posts, filtered, searched and ordered."""
    page = await service.page(query.filters())

    return PostsResponse.from_page(page)


@router.get("/post", response_model_exclude_unset=True)
async def read_post(
    query: Annotated[PostQuery, Query()], service: PostServiceDep,
) -> PostDetailOut:
    """One post, with its archived discussion."""
    payload = await service.detail(query.domain, query.id)

    return PostDetailOut.model_validate(payload)


@router.get(
    "/x/authors",
    dependencies=[Depends(single_values(XAuthorsQuery))],
    response_model_exclude_unset=True,
)
async def list_x_authors(
    query: Annotated[XAuthorsQuery, Query()], service: PostServiceDep,
) -> XAuthorsResponse:
    """Authors in the saved For You feed, most recommended first."""
    page = await service.authors(query.filters(), query.cursor)

    return XAuthorsResponse.from_page(page)
