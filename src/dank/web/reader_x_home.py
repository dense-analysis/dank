"""Stable browsing over saved For You appearances."""

from __future__ import annotations

import base64
import datetime as dt
import json
from typing import TYPE_CHECKING, Any, NamedTuple, cast

if TYPE_CHECKING:
    from dank.web.reader_query import ReaderFilters

HOME_CONDITION = (
    "JSONExtractString(payload, 'dank_x_home', 'feed') = 'for_you'"
)


class HomeCursor(NamedTuple):
    snapshot: str
    observed_at: str
    position: int
    domain: str
    post_id: str


def decode_home_cursor(filters: ReaderFilters) -> HomeCursor:
    try:
        if len(filters.cursor) > 8192 or filters.sort == "relevance":
            raise ValueError

        values = json.loads(
            base64.b64decode(filters.cursor, altchars=b"-_", validate=True),
        )

        if (
            not isinstance(values, list)
            or len(cast(list[Any], values)) != 7
            or cast(list[Any], values)[:2] != [2, filters.signature()]
        ):
            raise ValueError

        snapshot, observed, position, domain, post_id = (
            cast(list[Any], values)[2:]
        )

        if not all(
            isinstance(value, str)
            for value in (snapshot, observed, domain, post_id)
        ):
            raise ValueError

        cutoff, captured = (
            dt.datetime.fromisoformat(snapshot),
            dt.datetime.fromisoformat(observed),
        )

        if (
            cutoff.tzinfo is None
            or captured.tzinfo is None
            or captured > cutoff
            or type(position) is not int
            or not 1 <= position <= 2**31
        ):
            raise ValueError

        return HomeCursor(snapshot, observed, position, domain, post_id)
    except (ValueError, TypeError, UnicodeError):
        raise ValueError("Invalid cursor for these filters") from None


def encode_home_cursor(
    filters: ReaderFilters, row: dict[str, Any], snapshot: str,
) -> str:
    observed = str(row["recommended_at"]).replace(" ", "T")

    if dt.datetime.fromisoformat(observed).tzinfo is None:
        observed += "+00:00"

    return base64.urlsafe_b64encode(
        json.dumps(
            [
                2,
                filters.signature(),
                snapshot,
                observed,
                int(row["recommendation_position"]),
                str(row["domain"]),
                str(row["post_id"]),
            ],
        ).encode(),
    ).decode()


def home_listing(filters: ReaderFilters) -> tuple[str, dict[str, Any], str]:
    snapshot = (
        decode_home_cursor(filters).snapshot
        if filters.cursor
        else dt.datetime.now(dt.UTC).isoformat()
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
