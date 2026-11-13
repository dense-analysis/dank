"""Channel groups derived from the selected YouTube recommendations."""

from __future__ import annotations

import pathlib
from collections.abc import Mapping, Sequence
from typing import Any

from dank.api.youtube.model import (
    channel_cursor as channel_cursor,
)
from dank.config import Settings
from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_youtube_query import (
    YouTubeFilters,
    build_youtube_query,
)
from dank.web.reader_youtube_sources import configured_channel_keys

CHANNEL_KEY = (
    "if(notEmpty(JSONExtractString(payload, 'channel_id')), "
    "JSONExtractString(payload, 'channel_id'), "
    "JSONExtractString(payload, 'channel_url'))"
)
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


def channel_totals_query(base: str) -> str:
    return (
        f"SELECT uniqExactIf({CHANNEL_KEY}, "
        f"notEmpty({CHANNEL_KEY})) AS total, "
        f"countIf(empty({CHANNEL_KEY})) AS unattributed_items FROM ({base})"
    )


def channel_views(
    rows: Sequence[Mapping[str, Any]],
    configured: tuple[str, ...],
    previews: Mapping[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    return [{
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
