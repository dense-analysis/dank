"""Channel groups derived from the selected YouTube recommendations."""

from __future__ import annotations

import base64
import datetime as dt
import json
import pathlib
from typing import Any, cast

from aiohttp import web
from multidict import MultiDict

from dank.config import Settings
from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_youtube_query import (
    RECOMMENDATIONS_FEED,
    YouTubeFilters,
    build_youtube_query,
    parse_youtube_filters,
)
from dank.web.reader_youtube_sources import configured_channel_keys

CHANNEL_KEY = (
    "if(notEmpty(JSONExtractString(payload, 'channel_id')), "
    "JSONExtractString(payload, 'channel_id'), "
    "JSONExtractString(payload, 'channel_url'))"
)
CHANNEL_PAGE_LIMIT = 50
CHANNEL_EXAMPLES = 3


async def load_configured_channels(
    client: ClickHouseClient, settings: Settings,
) -> tuple[str, ...]:
    result = await client.fetch_json(
        "SELECT channel_id, channel_url, feed_url FROM youtube_channels FINAL "
        "UNION ALL SELECT "
        "argMax(JSONExtractString(payload, 'channel_id'), observed_at) "
        "AS channel_id, "
        "argMax(JSONExtractString(payload, 'channel_url'), observed_at) "
        "AS channel_url, feed_url FROM youtube_discoveries "
        "WHERE feed_kind = 'channel' GROUP BY feed_url",
    )

    return configured_channel_keys(settings, result.rows)


def channel_cursor(
    filters: YouTubeFilters, snapshot: str, count: int, key: str,
) -> str:
    return base64.urlsafe_b64encode(json.dumps([
        filters.signature(), snapshot, count, key,
    ]).encode()).decode()


def parse_channel_cursor(
    filters: YouTubeFilters, cursor: str,
) -> tuple[str, int, str]:
    try:
        if len(cursor) > 8192:
            raise ValueError

        decoded: object = json.loads(base64.b64decode(
            cursor, altchars=b"-_", validate=True,
        ))

        if not isinstance(decoded, list):
            raise ValueError

        raw = cast(list[Any], decoded)

        if (len(raw) != 4
                or raw[0] != filters.signature()
                or not isinstance(raw[1], str)
                or type(raw[2]) is not int or raw[2] < 1
                or not isinstance(raw[3], str) or len(raw[3]) > 2000):
            raise ValueError

        snapshot = dt.datetime.fromisoformat(raw[1])

        if snapshot.tzinfo is None:
            raise ValueError

        return snapshot.astimezone(dt.UTC).isoformat(), raw[2], raw[3]
    except (ValueError, TypeError, UnicodeError):
        raise ValueError("Invalid channel cursor for these filters") from None


def channel_query(
    filters: YouTubeFilters, configured: tuple[str, ...], snapshot: str,
    after: tuple[int, str] | None = None,
) -> tuple[str, dict[str, Any], str]:
    base, params = build_youtube_query(
        filters, configured_channels=configured, paginate=False,
        snapshot_time=snapshot,
    )
    grouped = (
        f"SELECT {CHANNEL_KEY} AS channel_key, count() AS count, "
        "countIf(kind = 'video') AS video_count, "
        "countIf(kind = 'short') AS short_count, "
        "countIf(kind = 'post') AS post_count, "
        "argMaxIf(JSONExtractString(payload, 'channel_name'), observed_at, "
        "notEmpty(JSONExtractString(payload, 'channel_name'))) AS name, "
        "argMaxIf(JSONExtractString(payload, 'channel_url'), observed_at, "
        "notEmpty(JSONExtractString(payload, 'channel_url'))) AS url "
        f"FROM ({base}) WHERE notEmpty({CHANNEL_KEY}) GROUP BY channel_key"
    )
    boundary = ""

    if after:
        boundary = (
            " WHERE count < %(after_count)s OR "
            "(count = %(after_count)s AND channel_key > %(after_key)s)"
        )
        params.update(after_count=after[0], after_key=after[1])

    query = (
        f"SELECT * FROM ({grouped}){boundary} "
        "ORDER BY count DESC, channel_key ASC LIMIT %(limit)s"
    )

    return query, params, base


async def handle_youtube_channels(request: web.Request) -> web.Response:
    try:
        query = MultiDict(request.query)

        if len(query.getall("cursor", [])) > 1:
            raise ValueError("Repeated channel cursor")

        cursor = query.pop("cursor", "")
        query.setdefault("limit", "20")
        filters = parse_youtube_filters(query)

        if filters.limit > CHANNEL_PAGE_LIMIT or filters.item_id:
            raise ValueError("Channel pages allow up to 50 channels")

        snapshot = dt.datetime.now(dt.UTC).isoformat()
        after = None

        if cursor:
            snapshot, count, key = parse_channel_cursor(filters, cursor)
            after = (count, key)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    client: ClickHouseClient = request.app["clickhouse"]
    state = request.app["state"]

    if filters.feed == RECOMMENDATIONS_FEED and not cursor:
        from dank.web.reader_youtube import recommendation_snapshot_time

        snapshot = await recommendation_snapshot_time(client)
    configured = await load_configured_channels(client, state.settings)
    sql, params, base = channel_query(filters, configured, snapshot, after)
    result = await client.fetch_json(sql, params)
    rows = result.rows[:filters.limit]
    totals = await client.fetch_json(
        f"SELECT uniqExactIf({CHANNEL_KEY}, "
        f"notEmpty({CHANNEL_KEY})) AS total, "
        f"countIf(empty({CHANNEL_KEY})) AS unattributed_items FROM ({base})",
        params,
    )
    previews = await channel_previews(
        client, rows, base, params, filters, state.assets_dir,
    )
    channels = [{
        "id": str(row["channel_key"]),
        "name": str(row["name"] or row["channel_key"]),
        "url": str(row["url"] or ""),
        "configured": (str(row["channel_key"]) in configured
                       or str(row["url"]) in configured),
        "count": int(row["count"]),
        "video_count": int(row["video_count"]),
        "short_count": int(row["short_count"]),
        "post_count": int(row["post_count"]),
        "items": previews.get(str(row["channel_key"]), []),
    } for row in rows]
    next_cursor = (
        channel_cursor(filters, snapshot, int(rows[-1]["count"]),
                       str(rows[-1]["channel_key"]))
        if len(result.rows) > filters.limit and rows else None
    )
    total = totals.rows[0] if totals.rows else {}

    return web.json_response({
        "channels": channels, "next_cursor": next_cursor,
        "total": int(total.get("total", 0)),
        "unattributed_items": int(total.get("unattributed_items", 0)),
    })


async def channel_previews(
    client: ClickHouseClient, rows: list[dict[str, Any]], base: str,
    params: dict[str, Any], filters: YouTubeFilters, assets_dir: pathlib.Path,
) -> dict[str, list[dict[str, Any]]]:
    if not rows:
        return {}

    # Reuse the reader's media/safety adapter; no new collection is triggered.
    from dank.web.reader_youtube import youtube_items

    examples = await client.fetch_json(
        f"SELECT *, {CHANNEL_KEY} AS channel_key FROM ({base}) "
        "WHERE channel_key IN %(channel_keys)s "
        "ORDER BY multiIf(kind = 'video', 0, kind = 'short', 1, 2), "
        "observed_at DESC, position ASC, item_id DESC "
        "LIMIT %(examples)s BY channel_key",
        {**params, "channel_keys": tuple(row["channel_key"] for row in rows),
         "examples": CHANNEL_EXAMPLES},
    )
    items = await youtube_items(client, examples.rows, filters, assets_dir)
    group_for = {str(row["item_id"]): str(row["channel_key"])
                 for row in examples.rows}
    grouped: dict[str, list[dict[str, Any]]] = {}

    for item in items:
        key = group_for[str(item["id"])]
        grouped.setdefault(key, []).append(item)

    return grouped
