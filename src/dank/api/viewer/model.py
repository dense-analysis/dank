"""The legacy viewer's rules, which both servers share.

Nothing here rejects a query: a value it can't read falls back to the default,
as it always has.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from typing import Literal, NamedTuple

from dank.api.posts.model import AssetRow, PostRow

MAX_PAGE_SIZE = 200
DEFAULT_DAYS_BACK = 0
MAX_DAYS_BACK = 365

type CursorDirection = Literal["next", "prev"]


class IndexFilters(NamedTuple):
    limit: int
    search_text: str
    domain: str
    account: str
    days_back: int
    cursor_created_at: dt.datetime | None
    cursor_post_id: str | None
    cursor_direction: CursorDirection


class IndexPage(NamedTuple):
    posts: Sequence[PostRow]
    assets: Mapping[str, Sequence[AssetRow]]
    has_previous_page: bool
    has_next_page: bool


class PostPage(NamedTuple):
    post: PostRow
    assets: Sequence[AssetRow]


def index_filters(
    query: Mapping[str, str | None], *, page_size: int,
) -> IndexFilters:
    return IndexFilters(
        limit=_parse_limit(query.get("limit"), page_size),
        search_text=_stripped(query.get("q")),
        domain=_stripped(query.get("domain")),
        account=_stripped(query.get("account")),
        days_back=_parse_days_back(query.get("days_back")),
        cursor_created_at=_parse_cursor_datetime(
            query.get("cursor_created_at"),
        ),
        cursor_post_id=query.get("cursor_post_id"),
        cursor_direction=_parse_cursor_direction(
            query.get("cursor_direction"),
        ),
    )


def minimum_created_at(days_back: int, now: dt.datetime) -> dt.datetime | None:
    if days_back <= 0:
        return None

    return now - dt.timedelta(days=days_back)


def days_back_label(days_back: int) -> str:
    if days_back <= 0:
        return "∞"

    return str(days_back)


def _parse_limit(value: str | None, default: int) -> int:
    if value is None:
        return default

    try:
        parsed = int(value)
    except ValueError:
        return default

    if parsed <= 0:
        return default

    return min(parsed, MAX_PAGE_SIZE)


def _parse_days_back(value: str | None) -> int:
    if value is None:
        return DEFAULT_DAYS_BACK

    try:
        parsed = int(value)
    except ValueError:
        return DEFAULT_DAYS_BACK

    if parsed < 0:
        return DEFAULT_DAYS_BACK

    return min(parsed, MAX_DAYS_BACK)


def _stripped(value: str | None) -> str:
    return (value or "").strip()


def _parse_cursor_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.UTC)

    return parsed


def _parse_cursor_direction(value: str | None) -> CursorDirection:
    if value == "prev":
        return "prev"

    return "next"
