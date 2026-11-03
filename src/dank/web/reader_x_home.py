"""Stable browsing over saved For You appearances."""

from __future__ import annotations

import datetime as dt
from typing import Any

from dank.api.posts.model import (
    ReaderFilters,
    Recommendation,
    decode_home_cursor,
    encode_home_cursor as home_cursor,
)

HOME_CONDITION = (
    "JSONExtractString(payload, 'dank_x_home', 'feed') = 'for_you'"
)


def encode_home_cursor(
    filters: ReaderFilters, row: dict[str, Any], snapshot: str,
) -> str:
    return home_cursor(
        filters,
        Recommendation(
            str(row["recommended_at"]), int(row["recommendation_position"]),
        ),
        str(row["domain"]),
        str(row["post_id"]),
        snapshot,
    )


def home_listing(
    filters: ReaderFilters, snapshot_time: str | None = None,
) -> tuple[str, dict[str, Any], str]:
    snapshot = (
        decode_home_cursor(filters).snapshot
        if filters.cursor
        else snapshot_time or dt.datetime.now(dt.UTC).isoformat()
    )
    params: dict[str, Any] = {"x_home_snapshot": snapshot}
    table = (
        "(SELECT * FROM posts FINAL) AS p INNER JOIN ("
        "SELECT domain, post_id, max(scraped_at) AS recommended_at, "
        "argMax(JSONExtractUInt(payload, 'dank_x_home', 'position'), "
        "scraped_at) "
        "AS recommendation_position FROM raw_posts "
        f"WHERE domain IN ('x.com', 'twitter.com') AND {HOME_CONDITION} "
        "AND scraped_at <= "
        "parseDateTime64BestEffort(%(x_home_snapshot)s, 3, 'UTC') "
        "GROUP BY domain, post_id) AS h USING (domain, post_id)"
    )
    condition = ""

    if filters.cursor:
        cursor = decode_home_cursor(filters)
        params.update(
            x_home_time=cursor.observed_at,
            x_home_position=-cursor.position,
            cursor_domain=cursor.domain,
            cursor_id=cursor.post_id,
        )
        operator = ">" if filters.sort == "oldest" else "<"
        condition = (
            "(recommended_at, -toInt64(recommendation_position), "
            "domain, post_id) "
            f"{operator} ("
            "parseDateTime64BestEffort(%(x_home_time)s, 3, 'UTC'), "
            "%(x_home_position)s, %(cursor_domain)s, %(cursor_id)s)"
        )

    return table, params, condition
