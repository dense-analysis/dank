"""Filters from query parameters, read as the FastAPI routes read them.

FastAPI hands a query model each list field's values and each other field's
last value, after dank.api.deps.single_values refuses repeats.
"""

from __future__ import annotations

from typing import Any, get_origin

from pydantic import BaseModel
from starlette.datastructures import QueryParams

from dank.api.posts.model import ReaderFilters
from dank.api.posts.schemas import PostsQuery
from dank.api.youtube.model import YouTubeFilters
from dank.api.youtube.schemas import YouTubeQuery


def parse_filters(query: QueryParams) -> ReaderFilters:
    """The filters GET /api/reader/posts reads."""
    return PostsQuery.model_validate(_values(PostsQuery, query)).filters()


def parse_youtube_filters(query: QueryParams) -> YouTubeFilters:
    """The filters GET /api/reader/youtube reads."""
    return YouTubeQuery.model_validate(_values(YouTubeQuery, query)).filters()


def _values(model: type[BaseModel], query: QueryParams) -> dict[str, Any]:
    lists = {
        name for name, field in model.model_fields.items()
        if get_origin(field.annotation) is tuple
    }
    values: dict[str, Any] = {}

    for key in query:
        sent = query.getlist(key)

        if key in lists:
            values[key] = tuple(sent)
        elif len(sent) > 1:
            raise ValueError(f"{key} must appear only once")
        else:
            values[key] = sent[0]

    return values
