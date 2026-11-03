"""PostStore read from collected posts in ClickHouse.

SQL and the post projection live in the reader helpers this reuses, so the
queries and payloads are the ones the aiohttp server uses.
"""

from __future__ import annotations

import pathlib
from collections.abc import Sequence
from typing import Any

from dank.api.posts.model import (
    AuthorCursor,
    AuthorGroup,
    AuthorListing,
    Embedding,
    ListedPost,
    PostPayload,
    PostRow,
    ReaderFilters,
    Recommendation,
)
from dank.config import SourceConfig
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import (
    _fetch_post,  # pyright: ignore[reportPrivateUsage]
    _parse_post_row,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_accounts import X_ACCOUNT_SQL
from dank.web.reader_posts import post_payloads
from dank.web.reader_query import build_query
from dank.web.reader_x_authors import (
    KNOWN_AUTHOR,
    author_examples,
    author_query,
    author_view,
)


class ClickHousePostStore:
    def __init__(
        self, client: ClickHouseClient, assets_dir: pathlib.Path,
    ) -> None:
        self._client = client
        self._assets_dir = assets_dir

    async def listing(
        self,
        filters: ReaderFilters,
        sources: tuple[SourceConfig, ...],
        embedding: Embedding | None,
        snapshot: str,
    ) -> list[ListedPost]:
        query, params = build_query(
            filters, sources, embedding, snapshot_time=snapshot,
        )
        result = await self._client.fetch_json(query, params)

        return [
            ListedPost(_parse_post_row(row), _recommendation(row))
            for row in result.rows
        ]

    async def post(self, domain: str, post_id: str) -> PostRow | None:
        return await _fetch_post(self._client, domain=domain, post_id=post_id)

    async def payloads(
        self,
        posts: Sequence[PostRow],
        search: str = "",
        *,
        include_discussion: bool = False,
    ) -> list[PostPayload]:
        return await post_payloads(
            self._client, list(posts), self._assets_dir, search,
            include_discussion=include_discussion,
        )

    async def authors(
        self,
        filters: ReaderFilters,
        sources: tuple[SourceConfig, ...],
        embedding: Embedding | None,
        snapshot: str,
        after: AuthorCursor | None,
    ) -> AuthorListing:
        base, params = build_query(
            filters, sources, embedding,
            paginate=False, snapshot_time=snapshot,
        )
        base = f"SELECT *, {X_ACCOUNT_SQL} AS handle FROM ({base})"
        sql, boundary = author_query(
            base, (after.count, after.handle) if after else None,
        )
        params.update(boundary)
        result = await self._client.fetch_json(sql, params)
        rows = result.rows[:filters.limit]
        totals = await self._client.fetch_json(
            f"SELECT uniqExactIf(handle, {KNOWN_AUTHOR}) AS total, "
            f"countIf(NOT ({KNOWN_AUTHOR})) AS unattributed_posts "
            f"FROM ({base})",
            params,
        )
        examples = await author_examples(
            self._client, self._assets_dir, rows, base, params, filters.q,
        )
        total = totals.rows[0] if totals.rows else {}

        return AuthorListing(
            authors=tuple(
                _author_group(row, examples.get(str(row["handle"]), []))
                for row in rows
            ),
            has_more=len(result.rows) > filters.limit,
            total=int(total.get("total", 0)),
            unattributed_posts=int(total.get("unattributed_posts", 0)),
        )


def _recommendation(row: dict[str, Any]) -> Recommendation | None:
    if "recommended_at" not in row:
        return None

    return Recommendation(
        str(row["recommended_at"]), int(row["recommendation_position"]),
    )


def _author_group(
    row: dict[str, Any], posts: list[PostPayload],
) -> AuthorGroup:
    view = author_view(row, posts)

    return AuthorGroup(
        view["handle"], view["name"], view["icon_url"], view["count"],
        tuple(posts),
    )
