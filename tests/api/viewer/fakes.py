"""An in-memory stand-in for the viewer's driven adapter."""

import datetime as dt
from collections.abc import Iterable, Mapping, Sequence

from dank.api.posts.model import AssetRow, PostRow
from dank.api.viewer.model import IndexFilters
from dank.api.viewer.service import ViewerStore


class FakeViewerStore:
    """Answers from values a test sets. Filtering is the real store's job,
    and test_clickhouse.py checks it."""

    def __init__(self) -> None:
        self.listed: list[PostRow] = []
        self.searched: list[PostRow] = []
        self.newer = False
        self.older = False
        self.asset_rows: dict[str, list[AssetRow]] = {}

    async def posts(
        self, filters: IndexFilters, since: dt.datetime | None,
    ) -> Sequence[PostRow]:
        return self.listed

    async def search(
        self, filters: IndexFilters, since: dt.datetime | None,
    ) -> Sequence[PostRow]:
        return self.searched

    async def has_newer(
        self, post: PostRow, filters: IndexFilters, since: dt.datetime | None,
    ) -> bool:
        return self.newer

    async def has_older(
        self, post: PostRow, filters: IndexFilters, since: dt.datetime | None,
    ) -> bool:
        return self.older

    async def post(self, domain: str, post_id: str) -> PostRow | None:
        return next(
            (
                post for post in self.listed
                if (post.domain, post.post_id) == (domain, post_id)
            ),
            None,
        )

    async def assets(
        self, post_ids: Iterable[str],
    ) -> Mapping[str, Sequence[AssetRow]]:
        return {
            post_id: self.asset_rows[post_id]
            for post_id in post_ids if post_id in self.asset_rows
        }


# Pyright checks this assignment, so the fake can't drift from the port.
_PORT: type[ViewerStore] = FakeViewerStore
