"""In-memory stand-ins for the posts feature's driven adapters."""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from typing import Any

from dank.api.posts.model import (
    AuthorCursor,
    AuthorListing,
    Embedding,
    ListedPost,
    PostPayload,
    PostRow,
    ReaderFilters,
)
from dank.api.posts.service import PostStore, SearchEmbeddings
from dank.config import SourceConfig

NOW = dt.datetime(2026, 10, 30, 12, tzinfo=dt.UTC)


def row(domain: str, post_id: str = "same", **fields: Any) -> PostRow:
    values: dict[str, Any] = {
        "url": f"https://{domain}/{post_id}", "author": "Reporter",
        "title": "An article", "html": "<p>Article body</p>",
        "created_at": NOW, "updated_at": NOW, "source": "rss",
    }

    return PostRow(domain, post_id, **values | fields)


def payload(post: PostRow) -> PostPayload:
    """The fields every post has, as the reader helpers build them."""
    return {
        "id": post.post_id, "domain": post.domain, "url": post.url,
        "author": post.author, "title": post.title, "excerpt": "Article body",
        "html": post.html, "collection_incomplete": False,
        "created_at": post.created_at.isoformat(), "source": post.source,
        "thumbnail": None, "media": [],
    }


class FakePostStore:
    """Answers from values a test sets. It records nothing: tests check
    the HTTP response, never which calls were made."""

    def __init__(self) -> None:
        self.listed: list[ListedPost] = []
        self.semantic: dict[Embedding, list[ListedPost]] = {}
        self.posts: dict[tuple[str, str], PostRow] = {}
        self.payloads_by_key: dict[tuple[str, str], PostPayload] = {}
        self.author_listing = AuthorListing(
            authors=(), has_more=False, total=0, unattributed_posts=0,
        )
        self.failing = False

    async def listing(
        self,
        filters: ReaderFilters,
        sources: tuple[SourceConfig, ...],
        embedding: Embedding | None,
        snapshot: str,
    ) -> list[ListedPost]:
        if self.failing:
            raise RuntimeError("ClickHouse is down")

        found = self.semantic[embedding] if embedding else self.listed

        return [
            item for item in found
            if not filters.domains or item.post.domain in filters.domains
        ]

    async def post(self, domain: str, post_id: str) -> PostRow | None:
        return self.posts.get((domain, post_id))

    async def payloads(
        self,
        posts: Sequence[PostRow],
        search: str = "",
        *,
        include_discussion: bool = False,
    ) -> list[PostPayload]:
        return [
            self.payloads_by_key.get((post.domain, post.post_id))
            or payload(post)
            for post in posts
        ]

    async def authors(
        self,
        filters: ReaderFilters,
        sources: tuple[SourceConfig, ...],
        embedding: Embedding | None,
        snapshot: str,
        after: AuthorCursor | None,
    ) -> AuthorListing:
        return self.author_listing


class FakeSearchEmbeddings:
    def __init__(self) -> None:
        self.vectors: dict[str, Embedding | None] = {}

    async def embed(self, text: str) -> Embedding | None:
        # A search nobody set up fails loudly, like the model being absent.
        return self.vectors[text]


# Pyright checks these assignments, so the fakes can't drift from the ports.
_STORE: type[PostStore] = FakePostStore
_EMBEDDINGS: type[SearchEmbeddings] = FakeSearchEmbeddings
