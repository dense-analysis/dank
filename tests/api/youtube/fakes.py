"""In-memory stand-ins for the YouTube feature's driven adapters."""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

from dank.api.youtube.model import (
    ChannelCursor,
    ChannelListing,
    ItemListing,
    MetadataObservation,
    PlaylistEntries,
    PlaylistListing,
    PlaylistPage,
    SourcesPayload,
    TranscriptPayload,
    YouTubeFilters,
    not_collected,
)
from dank.api.youtube.service import VideoCollector, YouTubeStore
from dank.config import SourceConfig

NOW = dt.datetime(2026, 10, 30, 12, tzinfo=dt.UTC)
STAMP = "2026-10-25T18:00:00.123456+00:00"
VIDEO = "NnV_cWeoo5Q"
EPOCH = dt.datetime.fromtimestamp(0, dt.UTC).isoformat()


def item(item_id: str = f"video:{VIDEO}", **fields: Any) -> dict[str, Any]:
    """Everything an item has, as the reader helpers build it."""
    return {
        "id": item_id,
        "kind": "video",
        "video_id": item_id.removeprefix("video:"),
        "url": f"https://www.youtube.com/watch?v={item_id[6:]}",
        "title": "A video",
        "text": "",
        "channel": {"id": "UC1", "name": "Channel", "url": ""},
        "thumbnail_url": None,
        "thumbnail_variants": [],
        "video_url": None,
        "duration_text": "",
        "views_text": "",
        "published_text": "",
        "observed_at": STAMP,
        "metadata_capture": {
            "status": "disabled", "provider": "", "observed_at": "",
            "retry_after": "", "available": False, "captured_at": "",
        },
        "detail_capture": {"status": "disabled"},
        "discoveries": [],
        "references": [],
        "reference_count": 0,
        "title_history": [],
        **fields,
    }


def playlist_item(position: int, **fields: Any) -> dict[str, Any]:
    return {
        **item(),
        "playlist_position": position,
        "playlist_entry_id": f"entry-{position}",
        "available": True,
        **fields,
    }


def playlist(playlist_id: str = "PLexample", **fields: Any) -> dict[str, Any]:
    return {
        "id": playlist_id,
        "title": "Saved music",
        "url": f"https://www.youtube.com/playlist?list={playlist_id}",
        "description": "",
        "thumbnail_url": None,
        "channel": {"id": "", "name": "", "url": ""},
        "video_count": None,
        "collected_count": 0,
        "status": "complete",
        "complete": True,
        "observed_at": STAMP,
        "checked_at": STAMP,
        "order": "YouTube order",
        "snapshot_id": "snapshot",
        **fields,
    }


def group(channel_id: str = "UC1", **fields: Any) -> dict[str, Any]:
    return {
        "id": channel_id,
        "name": "Channel One",
        "url": "",
        "configured": True,
        "count": 8,
        "video_count": 6,
        "short_count": 2,
        "post_count": 0,
        "items": [item()],
        **fields,
    }


def observation(**fields: Any) -> MetadataObservation:
    values: dict[str, Any] = {
        "status": "captured", "provider": "youtube_oembed", "cached": False,
        "observed_at": NOW.isoformat(),
        "retry_after": (NOW + dt.timedelta(days=7)).isoformat(),
    }

    return MetadataObservation(**values | fields)


def saved_transcript(video_id: str = VIDEO, **fields: Any) -> dict[str, Any]:
    return {
        "video_id": video_id,
        "status": "captured",
        "observed_at": NOW.isoformat(),
        "retry_after": "",
        "language": "en",
        "language_name": "English",
        "generated": True,
        "segments": [{"start": 1, "duration": 2.5, "text": "Saved words"}],
        **fields,
    }


class FakeYouTubeStore:
    """Answers from values a test sets. It records nothing: tests check
    the HTTP response, never which calls were made.

    Listings are keyed by snapshot, so a request that froze on the wrong
    moment finds nothing there and fails loudly.
    """

    def __init__(self) -> None:
        self.configured: tuple[str, ...] = ()
        self.recommendations_at = EPOCH
        self.listings: dict[str, ItemListing] = {
            "": ItemListing(items=(), total=0, after=None),
        }
        self.channel_listings: dict[str, ChannelListing] = {}
        self.overview_payload: SourcesPayload = {
            "feeds": [], "channels": [], "total": 0, "linked_count": 0,
        }
        self.playlist_times: dict[str, str] = {}
        self.libraries: dict[str, PlaylistListing] = {}
        self.entries: dict[tuple[str, str], PlaylistEntries] = {}
        self.known: set[str] = set()
        self.transcripts: dict[str, TranscriptPayload] = {}
        self.captured_at: dict[str, str] = {}
        self.failing = False

    async def recommendation_snapshot(self) -> str:
        return self.recommendations_at

    async def configured_channels(
        self, sources: tuple[SourceConfig, ...],
    ) -> tuple[str, ...]:
        return self.configured

    async def items(
        self,
        filters: YouTubeFilters,
        configured: tuple[str, ...],
        snapshot: str,
    ) -> ItemListing:
        if self.failing:
            raise RuntimeError("ClickHouse is down")

        return self.listings[snapshot]

    async def channels(
        self,
        filters: YouTubeFilters,
        configured: tuple[str, ...],
        snapshot: str,
        after: ChannelCursor | None,
    ) -> ChannelListing:
        return self.channel_listings[snapshot]

    async def overview(
        self, sources: tuple[SourceConfig, ...],
    ) -> SourcesPayload:
        return self.overview_payload

    async def playlist_snapshot(self, playlist_id: str = "") -> str:
        return self.playlist_times.get(playlist_id, EPOCH)

    async def playlists(self, page: PlaylistPage) -> PlaylistListing:
        return self.libraries[page.snapshot]

    async def playlist(
        self, playlist_id: str, page: PlaylistPage,
    ) -> PlaylistEntries | None:
        return self.entries.get((playlist_id, page.snapshot))

    async def is_known(self, video_id: str) -> bool:
        return video_id in self.known

    async def last_metadata_capture(self, video_id: str) -> str | None:
        return self.captured_at.get(video_id)

    async def transcript(self, video_id: str) -> TranscriptPayload:
        return self.transcripts.get(video_id) or not_collected(video_id)


class FakeVideoCollector:
    """Collects as the scrape runner does: the first result is saved, later
    ones reuse it. A video no test set up fails loudly, so a request that
    must not collect can't pass by collecting.

    With `holding` set, a collection waits for `release`.
    """

    def __init__(self) -> None:
        self.observations: dict[str, MetadataObservation] = {}
        self.transcripts: dict[str, TranscriptPayload] = {}
        self.saved: list[str] = []
        self.holding = False
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def metadata(self, video_id: str) -> MetadataObservation:
        found = self.observations[video_id]
        cached = await self._save(video_id)

        return found._replace(cached=cached)

    async def transcript(self, video_id: str) -> TranscriptPayload:
        found = self.transcripts[video_id]
        await self._save(video_id)

        return found

    async def _save(self, video_id: str) -> bool:
        self.started.set()

        if self.holding:
            await self.release.wait()

        cached = video_id in self.saved
        # Long enough for an overlapping request to read the same answer here
        # and collect again, which is what the lock has to prevent.
        await asyncio.sleep(0.01)

        if not cached:
            self.saved.append(video_id)

        return cached


# Pyright checks these assignments, so the fakes can't drift from the ports.
_STORE: type[YouTubeStore] = FakeYouTubeStore
_COLLECTOR: type[VideoCollector] = FakeVideoCollector
