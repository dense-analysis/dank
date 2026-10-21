"""Saved video previews for the mixed feed, without further collection."""

from __future__ import annotations

import datetime as dt
from typing import Any

from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import PostRow
from dank.web.reader_youtube import (
    _item,  # pyright: ignore[reportPrivateUsage]
)


async def add_youtube_post_metadata(
    client: ClickHouseClient, posts: list[dict[str, Any]], rows: list[PostRow],
) -> None:
    youtube = [row for row in rows if row.domain == "youtube.com"]

    if not youtube:
        return

    versions = [
        (row.post_id, int(row.updated_at.replace(
            tzinfo=row.updated_at.tzinfo or dt.UTC,
        ).timestamp() * 1000)) for row in youtube
    ]
    result = await client.fetch_json(
        "SELECT post_id, payload FROM raw_posts "
        "WHERE domain = 'youtube.com' AND "
        "(post_id, toUnixTimestamp64Milli(scraped_at)) "
        "IN %(youtube_feed_versions)s "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id",
        {"youtube_feed_versions": versions},
    )
    snapshots = {str(row["post_id"]): row["payload"] for row in result.rows}

    for post in posts:
        if post["domain"] != "youtube.com":
            continue

        # Reuse the dedicated video view's identity and safe-URL handling.
        item = _item({
            "item_id": post["id"], "payload": snapshots.get(post["id"]),
            "kind": "short" if "/shorts/" in post["url"] else (
                "post" if str(post["id"]).startswith("post:") else "video"
            ),
        })
        item["channel"]["name"] = item["channel"]["name"] or post["author"]
        item["thumbnail_url"] = post["thumbnail"] or next((
            media["url"] for media in post["media"]
            if media["content_type"].startswith("image/")
        ), item["thumbnail_url"])
        item["video_url"] = next((
            media["url"] for media in post["media"]
            if media["content_type"].startswith("video/")
        ), None)
        post["youtube"] = {
            key: item[key] for key in (
                "kind", "video_id", "channel", "duration_text", "views_text",
                "thumbnail_url", "video_url",
            )
        }
