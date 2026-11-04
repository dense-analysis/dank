"""YouTube use cases, and the ports they need from the outside world."""

from __future__ import annotations

import asyncio
from typing import NamedTuple, Protocol

from dank.api.posts.service import Clock
from dank.api.sources.service import SourceCatalog
from dank.api.youtube.model import (
    RECOMMENDATIONS_FEED,
    ChannelCursor,
    ChannelListing,
    ChannelsPage,
    ItemListing,
    ItemsPage,
    MetadataCapture,
    MetadataObservation,
    PlaylistDetail,
    PlaylistEntries,
    PlaylistListing,
    PlaylistPage,
    PlaylistsPage,
    SourcesPayload,
    TranscriptPayload,
    YouTubeFilters,
    channel_cursor,
    encode_youtube_cursor,
    metadata_capture,
    parse_channel_cursor,
    playlist_cursor,
    require_channel_page,
    youtube_cursor_snapshot,
)
from dank.config import SourceConfig
from dank.errors import Busy, NotFound


class YouTubeStore(Protocol):
    """Read-only access to what has been collected from YouTube."""

    async def recommendation_snapshot(self) -> str:
        """When the latest recommendations were saved."""
        ...

    async def configured_channels(
        self, sources: tuple[SourceConfig, ...],
    ) -> tuple[str, ...]: ...

    async def items(
        self,
        filters: YouTubeFilters,
        configured: tuple[str, ...],
        snapshot: str,
    ) -> ItemListing: ...

    async def channels(
        self,
        filters: YouTubeFilters,
        configured: tuple[str, ...],
        snapshot: str,
        after: ChannelCursor | None,
    ) -> ChannelListing: ...

    async def overview(
        self, sources: tuple[SourceConfig, ...],
    ) -> SourcesPayload: ...

    async def playlist_snapshot(self, playlist_id: str = "") -> str:
        """The last persisted check of a playlist, or of the whole library."""
        ...

    async def playlists(self, page: PlaylistPage) -> PlaylistListing: ...

    async def playlist(
        self, playlist_id: str, page: PlaylistPage,
    ) -> PlaylistEntries | None: ...

    async def is_known(self, video_id: str) -> bool:
        """Whether DANK has seen this video, and may ask YouTube about it."""
        ...

    async def last_metadata_capture(self, video_id: str) -> str | None: ...

    async def transcript(self, video_id: str) -> TranscriptPayload: ...


class VideoCollector(Protocol):
    """Asks YouTube about one video and saves what it answers.

    Both reuse a saved answer that isn't due for a refresh.
    """

    async def metadata(self, video_id: str) -> MetadataObservation: ...

    async def transcript(self, video_id: str) -> TranscriptPayload: ...


class CollectionLocks(NamedTuple):
    """One collection of each kind at a time, across every request."""

    metadata: asyncio.Lock
    transcript: asyncio.Lock


class CollectionLimits(NamedTuple):
    """Seconds a request may wait, so requests can't pile up behind YouTube."""

    # Covers waiting for the lock as well as the collection.
    metadata_wait: float
    transcript_lock_wait: float
    transcript_lookup: float


class YouTubeService:
    def __init__(
        self, catalog: SourceCatalog, store: YouTubeStore, clock: Clock,
    ) -> None:
        self._catalog = catalog
        self._store = store
        self._clock = clock

    async def page(self, filters: YouTubeFilters) -> ItemsPage:
        configured, snapshot = await asyncio.gather(
            self._scoped_channels(filters), self._item_snapshot(filters),
        )
        listing = await self._store.items(filters, configured, snapshot)
        after = listing.after

        return ItemsPage(
            items=listing.items,
            next_cursor=(
                encode_youtube_cursor(filters, *after, snapshot_time=snapshot)
                if after else None
            ),
            total=listing.total,
        )

    async def channels(
        self, filters: YouTubeFilters, cursor: str,
    ) -> ChannelsPage:
        # Both checks come before any I/O.
        require_channel_page(filters)
        after = parse_channel_cursor(filters, cursor) if cursor else None
        configured, snapshot = await asyncio.gather(
            self._configured_channels(),
            self._channel_snapshot(filters, after),
        )
        listing = await self._store.channels(
            filters, configured, snapshot, after,
        )
        boundary = listing.after

        return ChannelsPage(
            channels=listing.channels,
            next_cursor=(
                channel_cursor(filters, snapshot, *boundary)
                if boundary else None
            ),
            total=listing.total,
            unattributed_items=listing.unattributed_items,
        )

    async def overview(self) -> SourcesPayload:
        return await self._store.overview(await self._catalog.sources())

    async def playlists(self, page: PlaylistPage) -> PlaylistsPage:
        # A capture can finish before its rows are published. Freeze on a
        # persisted check, so publishing it can't change a later page.
        if not page.snapshot:
            page = page._replace(
                snapshot=await self._store.playlist_snapshot(),
            )

        listing = await self._store.playlists(page)

        return PlaylistsPage(
            playlists=listing.playlists,
            total=listing.total,
            next_cursor=(
                playlist_cursor(
                    "", page.q, page.snapshot, listing.after, page.sort,
                )
                if listing.after else None
            ),
        )

    async def playlist(
        self, playlist_id: str, page: PlaylistPage,
    ) -> PlaylistDetail:
        if not page.snapshot:
            page = page._replace(
                snapshot=await self._store.playlist_snapshot(playlist_id),
            )

        found = await self._store.playlist(playlist_id, page)

        if found is None:
            raise NotFound("Playlist not found")

        return PlaylistDetail(
            playlist=found.playlist,
            items=found.items,
            next_cursor=(
                playlist_cursor(
                    playlist_id, page.q, page.snapshot, found.after,
                )
                if found.after else None
            ),
            total=found.total,
        )

    async def _scoped_channels(
        self, filters: YouTubeFilters,
    ) -> tuple[str, ...]:
        if not filters.channel_scope:
            return ()

        return await self._configured_channels()

    async def _configured_channels(self) -> tuple[str, ...]:
        return await self._store.configured_channels(
            await self._catalog.sources(),
        )

    async def _item_snapshot(self, filters: YouTubeFilters) -> str:
        if filters.feed != RECOMMENDATIONS_FEED:
            return ""

        if filters.cursor:
            return youtube_cursor_snapshot(filters)

        return await self._store.recommendation_snapshot()

    async def _channel_snapshot(
        self, filters: YouTubeFilters, after: ChannelCursor | None,
    ) -> str:
        if after:
            return after.snapshot

        if filters.feed == RECOMMENDATIONS_FEED:
            return await self._store.recommendation_snapshot()

        return self._clock().isoformat()


class VideoCaptureService:
    """Previews and captions for videos DANK already knows about."""

    def __init__(
        self,
        store: YouTubeStore,
        collector: VideoCollector,
        locks: CollectionLocks,
        limits: CollectionLimits,
    ) -> None:
        self._store = store
        self._collector = collector
        self._locks = locks
        self._limits = limits

    async def collect_metadata(self, video_id: str) -> MetadataCapture:
        await self._require_known(video_id)

        try:
            # The lock makes simultaneous readers reuse the first saved result.
            # Limit waiting as well as HTTP time so requests can't accumulate.
            async with (
                asyncio.timeout(self._limits.metadata_wait),
                self._locks.metadata,
            ):
                observed = await self._collector.metadata(video_id)
        except TimeoutError:
            raise Busy(
                "Metadata collection is busy; try again shortly",
            ) from None

        captured_at = (
            observed.observed_at if observed.status == "captured"
            else await self._store.last_metadata_capture(video_id)
        )

        return metadata_capture(video_id, observed, captured_at)

    async def transcript(self, video_id: str) -> TranscriptPayload:
        await self._require_known(video_id)

        return await self._store.transcript(video_id)

    async def collect_transcript(self, video_id: str) -> TranscriptPayload:
        await self._require_known(video_id)
        lock = self._locks.transcript

        try:
            # Don't accumulate requests behind a slow provider.
            async with asyncio.timeout(self._limits.transcript_lock_wait):
                await lock.acquire()
        except TimeoutError:
            raise Busy(
                "Transcript collection is busy; try again shortly",
            ) from None

        try:
            async with asyncio.timeout(self._limits.transcript_lookup):
                return await self._collector.transcript(video_id)
        except TimeoutError:
            raise Busy(
                "Transcript lookup timed out; try again shortly",
            ) from None
        finally:
            lock.release()

    async def _require_known(self, video_id: str) -> None:
        # Never ask YouTube about an ID a stranger made up.
        if not await self._store.is_known(video_id):
            raise NotFound("Video not found")
