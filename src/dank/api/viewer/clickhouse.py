"""ViewerStore read from collected posts and assets in ClickHouse."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from typing import Any

from dank.api.posts.model import AssetRow, PostRow
from dank.api.posts.service import SearchEmbeddings
from dank.api.viewer.model import IndexFilters
from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import (
    _fetch_post,  # pyright: ignore[reportPrivateUsage]
    _parse_asset_row,  # pyright: ignore[reportPrivateUsage]
    _parse_post_row,  # pyright: ignore[reportPrivateUsage]
)

SEARCH_TITLE_WEIGHT = 0.65
"""The percentage of the weighting strength of title content."""
SEARCH_HTML_WEIGHT = 0.35
"""The percentage of the weighting strength of HTML content."""
SEARCH_MAXIMUM_DISTANCE = 1.0
"""Maximum cosine distance before exclusion. (bounded [0.0, 2.0])"""
SEARCH_FRESHNESS_WEIGHT = 0.3
"""
The weighting of freshness. (bounded [0.0, 2.0])

We apply: freshness_weight * exp(-days_ago / half_life_days)

"freshness" pulls content nearer if content matches weaker, but it is newer.
"""
SEARCH_FRESHNESS_HALF_LIFE_DAYS = 21.0
"""
The number of days of content age to measure freshness over.

We apply: freshness_weight * exp(-days_ago / half_life_days)

"freshness" pulls content nearer if content matches weaker, but it is newer.
"""

# Assert configuration of search parameters fit in bounds.
assert 0 <= SEARCH_TITLE_WEIGHT <= 1
assert 0 <= SEARCH_HTML_WEIGHT <= 1
assert 0 <= SEARCH_MAXIMUM_DISTANCE <= 2
assert 0 <= SEARCH_FRESHNESS_WEIGHT <= 2
assert SEARCH_FRESHNESS_HALF_LIFE_DAYS > 0

NEWER_THAN = (
    "(created_at > %(cursor_created_at)s "
    "OR (created_at = %(cursor_created_at)s "
    "AND post_id > %(cursor_post_id)s))"
)
OLDER_THAN = (
    "(created_at < %(cursor_created_at)s "
    "OR (created_at = %(cursor_created_at)s "
    "AND post_id < %(cursor_post_id)s))"
)


class ClickHouseViewerStore:
    def __init__(
        self, client: ClickHouseClient, embeddings: SearchEmbeddings,
    ) -> None:
        self._client = client
        self._embeddings = embeddings

    async def posts(
        self, filters: IndexFilters, since: dt.datetime | None,
    ) -> list[PostRow]:
        query = (
            "SELECT domain, post_id, url, author, title, html, created_at, "
            "updated_at, source FROM posts FINAL"
        )
        params: dict[str, Any] = {"limit": filters.limit}
        conditions = _filter_conditions(params, filters, since)

        if filters.cursor_created_at is not None and filters.cursor_post_id:
            params["cursor_created_at"] = filters.cursor_created_at
            params["cursor_post_id"] = filters.cursor_post_id

            if filters.cursor_direction == "prev":
                conditions.append(NEWER_THAN)
                query += _where_clause(conditions)
                query += " ORDER BY created_at ASC, post_id ASC "
                query += "LIMIT %(limit)s"
                result = await self._client.fetch_json(query, params)
                posts = [_parse_post_row(row) for row in result.rows]
                posts.reverse()

                return posts

            conditions.append(OLDER_THAN)

        query += _where_clause(conditions)
        query += " ORDER BY created_at DESC, post_id DESC "
        query += "LIMIT %(limit)s"
        result = await self._client.fetch_json(query, params)

        return [_parse_post_row(row) for row in result.rows]

    async def has_newer(
        self, post: PostRow, filters: IndexFilters, since: dt.datetime | None,
    ) -> bool:
        return await self._has_neighbor(
            post, filters, since, NEWER_THAN, "ASC",
        )

    async def has_older(
        self, post: PostRow, filters: IndexFilters, since: dt.datetime | None,
    ) -> bool:
        return await self._has_neighbor(
            post, filters, since, OLDER_THAN, "DESC",
        )

    async def _has_neighbor(
        self,
        post: PostRow,
        filters: IndexFilters,
        since: dt.datetime | None,
        boundary: str,
        order: str,
    ) -> bool:
        params: dict[str, Any] = {
            "cursor_created_at": post.created_at,
            "cursor_post_id": post.post_id,
        }
        conditions = _filter_conditions(params, filters, since)
        conditions.append(boundary)
        query = "SELECT post_id FROM posts FINAL"
        query += _where_clause(conditions)
        query += f" ORDER BY created_at {order}, post_id {order} LIMIT 1"
        result = await self._client.fetch_json(query, params)

        return bool(result.rows)

    async def search(
        self, filters: IndexFilters, since: dt.datetime | None,
    ) -> list[PostRow]:
        embedding = await self._embeddings.embed(filters.search_text)

        if embedding is None:
            return []

        params: dict[str, Any] = {
            "embedding": list(embedding),
            "title_weight": SEARCH_TITLE_WEIGHT,
            "html_weight": SEARCH_HTML_WEIGHT,
            "maximum_distance": SEARCH_MAXIMUM_DISTANCE,
            "freshness_weight": SEARCH_FRESHNESS_WEIGHT,
            "freshness_half_life_days": SEARCH_FRESHNESS_HALF_LIFE_DAYS,
            "limit": filters.limit,
        }
        conditions = _filter_conditions(params, filters, since)
        where_clause = _where_clause([
            "length(title_embedding) > 0",
            "length(html_embedding) > 0",
            *conditions,
        ])
        query = rf"""
            SELECT
                *,
                -- The final score for sorting is influenced by 'freshness'
                -- freshness is impact by the number of days that have passed
                -- This way newer posts appear higher up despite being further
                -- in distance by the embeddings match
                (
                    distance - (
                        %(freshness_weight)s
                        * exp(-age_days / %(freshness_half_life_days)s)
                    )
                ) AS score
            FROM (
                SELECT
                    *,
                    (
                        cosineDistance(title_embedding, %(embedding)s)
                            * %(title_weight)s
                        + cosineDistance(html_embedding, %(embedding)s)
                            * %(html_weight)s
                    ) AS distance,
                    (
                        greatest(
                            dateDiff('second', created_at, now64(3)),
                            0
                        ) / 86400.0
                    ) AS age_days
                FROM posts FINAL
                {where_clause}
            )
            WHERE distance <= %(maximum_distance)s
            ORDER BY score ASC
            LIMIT %(limit)s
        """
        result = await self._client.fetch_json(query, params)

        return [_parse_post_row(row) for row in result.rows]

    async def post(self, domain: str, post_id: str) -> PostRow | None:
        return await _fetch_post(self._client, domain=domain, post_id=post_id)

    async def assets(
        self, post_ids: Iterable[str],
    ) -> dict[str, list[AssetRow]]:
        ids = [post_id for post_id in post_ids if post_id]

        if not ids:
            return {}

        query = (
            "SELECT post_id, url, local_path, content_type, size_bytes "
            "FROM assets FINAL WHERE post_id IN %(post_ids)s "
        )
        result = await self._client.fetch_json(query, {"post_ids": ids})
        assets: dict[str, list[AssetRow]] = {}

        for row in result.rows:
            asset = _parse_asset_row(row)
            assets.setdefault(asset.post_id, []).append(asset)

        return assets


def _filter_conditions(
    params: dict[str, Any],
    filters: IndexFilters,
    since: dt.datetime | None,
) -> list[str]:
    conditions: list[str] = []

    if filters.domain:
        conditions.append("domain = %(domain_filter)s")
        params["domain_filter"] = filters.domain

    if filters.account:
        conditions.append(
            "positionCaseInsensitive(author, %(account_filter)s) > 0",
        )
        params["account_filter"] = filters.account

    if since is not None:
        conditions.append("created_at >= %(min_created_at)s")
        params["min_created_at"] = since

    return conditions


def _where_clause(conditions: list[str]) -> str:
    if not conditions:
        return ""

    return " WHERE " + " AND ".join(conditions)
