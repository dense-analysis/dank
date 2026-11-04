"""RefreshTargets read from saved content in ClickHouse."""

from __future__ import annotations

from collections.abc import Mapping

from dank.api.refresh.model import ChannelFeed
from dank.storage.clickhouse import ClickHouseClient


class ClickHouseRefreshTargets:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    async def saved_playlists(self, playlist_id: str) -> Mapping[str, str]:
        where = " WHERE playlist_id = %(playlist_id)s" if playlist_id else ""
        result = await self._client.fetch_json(
            "SELECT playlist_id, title FROM youtube_playlists" + where
            + " ORDER BY checked_at DESC LIMIT 1 BY playlist_id",
            {"playlist_id": playlist_id},
        )

        return {
            str(row["playlist_id"]): str(row.get("title") or "")
            for row in result.rows
        }

    async def channel_feeds(self) -> list[ChannelFeed]:
        result = await self._client.fetch_json(
            "SELECT feed_url, channel_id, channel_url "
            "FROM youtube_channels FINAL "
            "UNION ALL SELECT feed_url, "
            "argMax(JSONExtractString(payload, 'channel_id'), observed_at) "
            "AS channel_id, "
            "argMax(JSONExtractString(payload, 'channel_url'), observed_at) "
            "AS channel_url FROM youtube_discoveries "
            "WHERE feed_kind = 'channel' GROUP BY feed_url",
        )

        return [
            ChannelFeed(
                str(row["feed_url"]),
                str(row.get("channel_id")),
                str(row.get("channel_url")),
            )
            for row in result.rows
        ]

    async def has_post(self, domain: str, post_id: str) -> bool:
        result = await self._client.fetch_json(
            "SELECT domain, post_id FROM raw_posts "
            "WHERE domain = %(domain)s AND post_id = %(post_id)s LIMIT 1",
            {"domain": domain, "post_id": post_id},
        )

        return bool(result.rows)

    async def has_video(self, video_id: str) -> bool:
        result = await self._client.fetch_json(
            "SELECT video_id FROM (SELECT video_id "
            "FROM youtube_reference_evidence WHERE video_id = %(id)s "
            "UNION ALL SELECT substring(item_id, 7) AS video_id "
            "FROM youtube_discoveries WHERE item_id = %(post_id)s "
            "UNION ALL SELECT substring(post_id, 7) AS video_id "
            "FROM raw_posts WHERE domain = %(domain)s "
            "AND post_id = %(post_id)s) LIMIT 1",
            {
                "id": video_id,
                "domain": "youtube.com",
                "post_id": "video:" + video_id,
            },
        )

        return bool(result.rows)
