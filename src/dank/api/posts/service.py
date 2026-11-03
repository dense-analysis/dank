"""Browsing use cases, and the ports they need from the outside world."""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Callable, Sequence
from typing import Protocol

from dank.api.posts.model import (
    AuthorCursor,
    AuthorListing,
    AuthorsPage,
    Embedding,
    ListedPost,
    PostPayload,
    PostRow,
    PostsPage,
    ReaderFilters,
    author_cursor,
    feed_snapshot,
    next_cursor,
    parse_author_cursor,
    require_author_feed,
)
from dank.api.sources.service import SourceCatalog
from dank.config import SourceConfig
from dank.errors import NotFound

type Clock = Callable[[], dt.datetime]


class SearchEmbeddings(Protocol):
    """A search's vector: None when the model produced nothing."""

    async def embed(self, text: str) -> Embedding | None: ...


class PostStore(Protocol):
    """Read-only access to collected posts."""

    async def listing(
        self,
        filters: ReaderFilters,
        sources: tuple[SourceConfig, ...],
        embedding: Embedding | None,
        snapshot: str,
    ) -> Sequence[ListedPost]:
        """Matching posts in order, up to one more than filters.limit."""
        ...

    async def post(self, domain: str, post_id: str) -> PostRow | None: ...

    async def payloads(
        self,
        posts: Sequence[PostRow],
        search: str = "",
        *,
        include_discussion: bool = False,
    ) -> Sequence[PostPayload]: ...

    async def authors(
        self,
        filters: ReaderFilters,
        sources: tuple[SourceConfig, ...],
        embedding: Embedding | None,
        snapshot: str,
        after: AuthorCursor | None,
    ) -> AuthorListing: ...


class PostService:
    def __init__(
        self,
        catalog: SourceCatalog,
        store: PostStore,
        embeddings: SearchEmbeddings,
        clock: Clock,
    ) -> None:
        self._catalog = catalog
        self._store = store
        self._embeddings = embeddings
        self._clock = clock

    async def page(self, filters: ReaderFilters) -> PostsPage:
        snapshot = feed_snapshot(filters, self._clock())
        sources, embedding = await asyncio.gather(
            self._catalog.sources(), self._embedding(filters),
        )
        listed = await self._store.listing(
            filters, sources, embedding, snapshot,
        )
        shown = listed[:filters.limit]
        has_more = len(listed) > filters.limit
        posts = await self._store.payloads(
            [item.post for item in shown], filters.q,
        )

        return PostsPage(
            posts=tuple(posts),
            next_cursor=(
                next_cursor(filters, shown[-1], snapshot) if has_more else None
            ),
            limited=has_more and filters.sort == "relevance",
        )

    async def detail(self, domain: str, post_id: str) -> PostPayload:
        post = await self._store.post(domain, post_id)

        if post is None:
            raise NotFound("Article not found")

        [payload] = await self._store.payloads([post], include_discussion=True)

        return payload

    async def authors(
        self, filters: ReaderFilters, cursor: str,
    ) -> AuthorsPage:
        # Both checks come before any I/O.
        require_author_feed(filters)
        after = parse_author_cursor(filters, cursor) if cursor else None
        snapshot = after.snapshot if after else self._clock().isoformat()
        sources, embedding = await asyncio.gather(
            self._catalog.sources(), self._embedding(filters),
        )
        listing = await self._store.authors(
            filters, sources, embedding, snapshot, after,
        )
        last = listing.authors[-1] if listing.authors else None

        return AuthorsPage(
            authors=listing.authors,
            next_cursor=(
                author_cursor(filters, snapshot, last.count, last.handle)
                if listing.has_more and last else None
            ),
            total=listing.total,
            unattributed_posts=listing.unattributed_posts,
        )

    async def _embedding(self, filters: ReaderFilters) -> Embedding | None:
        if not filters.q or filters.mode not in {"meaning", "combined"}:
            return None

        embedding = await self._embeddings.embed(filters.q)

        if not embedding:
            # A 503: the search can't run, and its query isn't at fault.
            raise RuntimeError("Search embedding is unavailable")

        return embedding
