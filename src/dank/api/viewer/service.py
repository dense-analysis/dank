"""The legacy viewer's use cases, and the port they need."""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from typing import Protocol

from dank.api.posts.model import AssetRow, PostRow
from dank.api.posts.service import Clock
from dank.api.viewer.model import (
    IndexFilters,
    IndexPage,
    PostPage,
    minimum_created_at,
)
from dank.errors import InvalidInput, NotFound


class ViewerStore(Protocol):
    """Read-only access to collected posts and their assets."""

    async def posts(
        self, filters: IndexFilters, since: dt.datetime | None,
    ) -> Sequence[PostRow]:
        """One page around the filters' cursor, newest first."""
        ...

    async def search(
        self, filters: IndexFilters, since: dt.datetime | None,
    ) -> Sequence[PostRow]:
        """Posts closest in meaning to filters.search_text."""
        ...

    async def has_newer(
        self, post: PostRow, filters: IndexFilters, since: dt.datetime | None,
    ) -> bool: ...

    async def has_older(
        self, post: PostRow, filters: IndexFilters, since: dt.datetime | None,
    ) -> bool: ...

    async def post(self, domain: str, post_id: str) -> PostRow | None: ...

    async def assets(
        self, post_ids: Iterable[str],
    ) -> Mapping[str, Sequence[AssetRow]]: ...


class ViewerService:
    def __init__(self, store: ViewerStore, clock: Clock) -> None:
        self._store = store
        self._clock = clock

    async def index(self, filters: IndexFilters) -> IndexPage:
        since = minimum_created_at(filters.days_back, self._clock())
        has_previous_page = has_next_page = False

        if filters.search_text:
            posts = await self._store.search(filters, since)
        else:
            posts = await self._store.posts(filters, since)

            if posts:
                has_previous_page, has_next_page = await asyncio.gather(
                    self._store.has_newer(posts[0], filters, since),
                    self._store.has_older(posts[-1], filters, since),
                )

        assets = await self._store.assets(post.post_id for post in posts)

        return IndexPage(posts, assets, has_previous_page, has_next_page)

    async def detail(
        self, domain: str | None, post_id: str | None,
    ) -> PostPage:
        if not post_id or not domain:
            raise InvalidInput("Missing post_id or domain")

        post = await self._store.post(domain, post_id)

        if post is None:
            raise NotFound("Post not found")

        assets = await self._store.assets([post.post_id])

        return PostPage(post, assets.get(post.post_id, ()))
