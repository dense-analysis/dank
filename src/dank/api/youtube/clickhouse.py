"""YouTubeStore read from collected videos in ClickHouse.

SQL and the projections live in the reader helpers in dank.web.
"""

from __future__ import annotations

import pathlib
from typing import Any

from dank.api.youtube.model import (
    ChannelBoundary,
    ChannelCursor,
    ChannelListing,
    ItemBoundary,
    ItemListing,
    PlaylistEntries,
    PlaylistListing,
    PlaylistPage,
    SourcesPayload,
    TranscriptPayload,
    YouTubeFilters,
    timestamp_text,
    transcript_view,
)
from dank.config import Settings, SourceConfig
from dank.storage.clickhouse import ClickHouseClient
from dank.web.reader_youtube import (
    recommendation_snapshot_time,
    youtube_items,
    youtube_sources,
)
from dank.web.reader_youtube_channels import (
    channel_previews,
    channel_query,
    channel_totals_query,
    channel_views,
    load_configured_channels,
)
from dank.web.reader_youtube_playlists import (
    latest_playlists_query,
    library_query,
    playlist_entries_query,
    playlist_items,
    playlist_metadata,
    playlist_snapshot_time,
)
from dank.web.reader_youtube_query import build_youtube_query


class ClickHouseYouTubeStore:
    def __init__(
        self,
        client: ClickHouseClient,
        settings: Settings,
        assets_dir: pathlib.Path,
    ) -> None:
        self._client = client
        self._settings = settings
        self._assets_dir = assets_dir

    async def recommendation_snapshot(self) -> str:
        return await recommendation_snapshot_time(self._client)

    async def configured_channels(
        self, sources: tuple[SourceConfig, ...],
    ) -> tuple[str, ...]:
        return await load_configured_channels(
            self._client, self._with(sources),
        )

    async def items(
        self,
        filters: YouTubeFilters,
        configured: tuple[str, ...],
        snapshot: str,
    ) -> ItemListing:
        result = await self._client.fetch_json(*build_youtube_query(
            filters, configured_channels=configured, snapshot_time=snapshot,
        ))
        rows = result.rows[:filters.limit]
        counted = await self._client.fetch_json(*build_youtube_query(
            filters, count=True, configured_channels=configured,
            snapshot_time=snapshot,
        ))
        items = await youtube_items(
            self._client, rows, filters, self._assets_dir,
        )

        return ItemListing(
            items=tuple(items),
            total=int(counted.rows[0]["total"]) if counted.rows else 0,
            after=(
                _item_boundary(items[-1], rows[-1])
                if len(result.rows) > filters.limit and items else None
            ),
        )

    async def channels(
        self,
        filters: YouTubeFilters,
        configured: tuple[str, ...],
        snapshot: str,
        after: ChannelCursor | None,
    ) -> ChannelListing:
        sql, params, base = channel_query(
            filters, configured, snapshot,
            (after.count, after.key) if after else None,
        )
        result = await self._client.fetch_json(sql, params)
        rows = result.rows[:filters.limit]
        totals = await self._client.fetch_json(
            channel_totals_query(base), params,
        )
        previews = await channel_previews(
            self._client, rows, base, params, filters, self._assets_dir,
        )
        total = totals.rows[0] if totals.rows else {}

        return ChannelListing(
            channels=tuple(channel_views(rows, configured, previews)),
            after=(
                ChannelBoundary(
                    int(rows[-1]["count"]), str(rows[-1]["channel_key"]),
                )
                if len(result.rows) > filters.limit and rows else None
            ),
            total=int(total.get("total", 0)),
            unattributed_items=int(total.get("unattributed_items", 0)),
        )

    async def overview(
        self, sources: tuple[SourceConfig, ...],
    ) -> SourcesPayload:
        return await youtube_sources(
            self._client, self._with(sources), self._assets_dir,
        )

    async def playlist_snapshot(self, playlist_id: str = "") -> str:
        return await playlist_snapshot_time(self._client, playlist_id)

    async def playlists(self, page: PlaylistPage) -> PlaylistListing:
        result = await self._client.fetch_json(*library_query(page))
        totals = await self._client.fetch_json(
            *library_query(page, count=True),
        )
        rows = result.rows[:page.limit]

        return PlaylistListing(
            playlists=tuple(playlist_metadata(row) for row in rows),
            total=int(totals.rows[0]["total"]) if totals.rows else 0,
            after=(
                (str(rows[-1]["sort_key"]), str(rows[-1]["playlist_id"]))
                if len(result.rows) > page.limit and rows else None
            ),
        )

    async def playlist(
        self, playlist_id: str, page: PlaylistPage,
    ) -> PlaylistEntries | None:
        found = await self._client.fetch_json(
            latest_playlists_query(individual=True),
            {"playlist_id": playlist_id, "snapshot": page.snapshot},
        )

        if not found.rows:
            return None

        row = found.rows[0]
        metadata = playlist_metadata(row)
        entries = await self._client.fetch_json(*playlist_entries_query(
            playlist_id, metadata["snapshot_id"], page,
        ))
        total = metadata["collected_count"]

        if page.q:
            totals = await self._client.fetch_json(*playlist_entries_query(
                playlist_id, metadata["snapshot_id"], page, count=True,
            ))
            total = int(totals.rows[0]["total"]) if totals.rows else 0

        rows = entries.rows[:page.limit]
        items = await playlist_items(
            self._client, self._assets_dir, rows, row,
        )

        return PlaylistEntries(
            playlist=metadata,
            items=tuple(items),
            total=total,
            after=(
                int(rows[-1]["position"])
                if len(entries.rows) > page.limit and rows else None
            ),
        )

    async def is_known(self, video_id: str) -> bool:
        known = await self._client.fetch_json(
            "SELECT video_id FROM (SELECT video_id "
            "FROM youtube_reference_evidence "
            "WHERE video_id = %(id)s UNION ALL "
            "SELECT substring(item_id, 7) AS video_id "
            "FROM youtube_discoveries WHERE item_id = %(item_id)s) LIMIT 1",
            {"id": video_id, "item_id": f"video:{video_id}"},
        )

        return bool(known.rows)

    async def last_metadata_capture(self, video_id: str) -> str | None:
        success = await self._client.fetch_json(
            "SELECT observed_at FROM youtube_video_metadata "
            "WHERE video_id = %(id)s AND status = 'captured' "
            "ORDER BY observed_at DESC LIMIT 1", {"id": video_id},
        )

        return (
            timestamp_text(success.rows[0]["observed_at"])
            if success.rows else None
        )

    async def transcript(self, video_id: str) -> TranscriptPayload:
        saved = await self._client.fetch_json(
            "SELECT * FROM youtube_transcripts WHERE video_id = %(id)s "
            "ORDER BY observed_at DESC LIMIT 1",
            {"id": video_id},
        )

        return transcript_view(video_id, saved.rows[0] if saved.rows else {})

    def _with(self, sources: tuple[SourceConfig, ...]) -> Settings:
        # The catalog owns sources; the rest of YouTube's settings stay here.
        return self._settings._replace(sources=sources)


def _item_boundary(item: dict[str, Any], row: dict[str, Any]) -> ItemBoundary:
    return ItemBoundary(
        item["observed_at"],
        item["id"],
        int(row.get("position") or 0),
        int(row.get("reference_count") or 0),
    )
