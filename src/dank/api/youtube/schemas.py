"""Wire contract for the YouTube routes.

Query models check a request's shape; rules that involve more than one field,
or whose wording the reader shows, stay in the domain.

Response models describe the projections in dank.web.reader_youtube*, whose
keys are always present, so a field without a default may still be null.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BeforeValidator, Field

from dank.api.schemas import ApiModel
from dank.api.youtube.model import (
    CHANNEL_PAGE_LIMIT,
    ITEM_PAGE_LIMIT,
    PLAYLIST_PAGE_LIMIT,
    PlaylistPage,
    YouTubeFilters,
    playlist_page,
    youtube_filters,
)

# Pydantic would take limit=5.0 as an integer, which has never been accepted.
WholeNumberText = BeforeValidator(int)


class FilterQuery(ApiModel):
    """What selects YouTube items, on the items and the channels routes."""

    feed: str = ""
    channel: str = ""
    kind: Literal["", "video", "short", "post"] = ""
    q: str = ""
    id: str = ""
    cursor: str = ""
    sort: Literal["newest", "oldest", "most-linked"] = "newest"
    channel_scope: Literal["", "configured"] = ""
    limit: int

    def _sent(self) -> YouTubeFilters:
        return YouTubeFilters(
            feed=self.feed,
            channel=self.channel,
            kind=self.kind,
            q=self.q,
            item_id=self.id,
            limit=self.limit,
            cursor=self.cursor,
            sort=self.sort,
            channel_scope=self.channel_scope,
        )


class YouTubeQuery(FilterQuery):
    limit: Annotated[
        int, WholeNumberText, Field(ge=1, le=ITEM_PAGE_LIMIT),
    ] = 30

    def filters(self) -> YouTubeFilters:
        return youtube_filters(self._sent())


class ChannelsQuery(FilterQuery):
    limit: Annotated[
        int, WholeNumberText, Field(ge=1, le=CHANNEL_PAGE_LIMIT),
    ] = 20

    def filters(self) -> YouTubeFilters:
        # This cursor walks channels, so it isn't a filter's item cursor.
        return youtube_filters(self._sent()._replace(cursor=""))


class PlaylistQuery(ApiModel):
    limit: Annotated[
        int, WholeNumberText, Field(ge=1, le=PLAYLIST_PAGE_LIMIT),
    ] = 30
    q: str = ""
    cursor: str = ""

    def page(self, playlist_id: str) -> PlaylistPage:
        return playlist_page(
            limit=self.limit, q=self.q, cursor=self.cursor,
            playlist_id=playlist_id,
        )


class PlaylistsQuery(PlaylistQuery):
    sort: Literal["recent", "az"] = "recent"

    def library(self) -> PlaylistPage:
        return playlist_page(
            limit=self.limit, q=self.q, cursor=self.cursor, sort=self.sort,
        )


class CollectBody(ApiModel):
    """An empty object: collection has no options, and the reader sends {}."""


class ChannelRefOut(ApiModel):
    id: str
    name: str
    url: str


class ThumbnailTitleOut(ApiModel):
    title: str
    first_seen: str
    last_seen: str
    association: Literal["captured", "matched"]


class ThumbnailVariantOut(ApiModel):
    url: str
    first_seen: str
    last_seen: str
    title_pairs: list[ThumbnailTitleOut]


class MetadataStateOut(ApiModel):
    status: Literal["captured", "failed", "unavailable", "disabled"]
    provider: str
    observed_at: str
    retry_after: str
    available: bool
    captured_at: str


class DetailCaptureOut(ApiModel):
    status: Literal["captured", "failed", "disabled"]


class DiscoveryOut(ApiModel):
    feed_url: str
    feed_name: str
    feed_kind: str
    tab: str
    shelf: str
    region: str
    playlist_id: str
    run_id: str
    observed_at: str
    rank: int | None


class VideoReferenceOut(ApiModel):
    domain: str
    post_id: str
    kind: str
    reference_id: str
    reference_url: str
    author: str
    matched_url: str
    excerpt: str
    observed_at: str
    url: str
    title: str
    source: str
    reader_url: str


class TitleVersionOut(ApiModel):
    title: str
    first_seen: str
    last_seen: str


class YouTubeItemOut(ApiModel):
    id: str
    kind: Literal["video", "short", "post"]
    video_id: str | None
    url: str
    title: str
    text: str
    channel: ChannelRefOut
    thumbnail_url: str | None
    thumbnail_variants: list[ThumbnailVariantOut]
    video_url: str | None
    duration_text: str
    views_text: str
    published_text: str
    observed_at: str
    metadata_capture: MetadataStateOut
    detail_capture: DetailCaptureOut
    discoveries: list[DiscoveryOut]
    references: list[VideoReferenceOut]
    reference_count: int
    title_history: list[TitleVersionOut]


class YouTubeResponse(ApiModel):
    items: list[YouTubeItemOut]
    next_cursor: str | None
    total: int


class YouTubeFeedOut(ApiModel):
    id: str
    url: str
    name: str
    kind: str
    region: str
    tab: str
    count: int


class YouTubeSourceChannelOut(ApiModel):
    id: str
    name: str
    url: str
    count: int
    avatar_url: str | None
    reported_video_count: int | None
    reported_video_count_text: str | None


class YouTubeSourcesResponse(ApiModel):
    feeds: list[YouTubeFeedOut]
    channels: list[YouTubeSourceChannelOut]
    total: int
    linked_count: int


class YouTubeChannelOut(ApiModel):
    id: str
    name: str
    url: str
    configured: bool
    count: int
    video_count: int
    short_count: int
    post_count: int
    items: list[YouTubeItemOut]


class YouTubeChannelsResponse(ApiModel):
    channels: list[YouTubeChannelOut]
    next_cursor: str | None
    total: int
    unattributed_items: int


class PlaylistOut(ApiModel):
    id: str
    title: str
    url: str
    description: str
    thumbnail_url: str | None
    channel: ChannelRefOut
    video_count: int | None
    collected_count: int
    status: str
    complete: bool
    observed_at: str
    checked_at: str
    order: str
    snapshot_id: str


class PlaylistsResponse(ApiModel):
    playlists: list[PlaylistOut]
    total: int
    next_cursor: str | None


class PlaylistItemOut(YouTubeItemOut):
    playlist_position: int
    playlist_entry_id: str
    available: bool


class PlaylistResponse(ApiModel):
    playlist: PlaylistOut
    items: list[PlaylistItemOut]
    next_cursor: str | None
    total: int


class VideoMetadataOut(ApiModel):
    video_id: str
    status: Literal["captured", "failed", "unavailable"]
    provider: str
    cached: bool
    observed_at: str
    retry_after: str
    available: bool
    captured_at: str


class TranscriptSegmentOut(ApiModel):
    # Whatever the captions saved: a saved 1 stays 1 rather than becoming 1.0.
    start: int | float
    duration: int | float
    text: str


class VideoTranscriptOut(ApiModel):
    video_id: str
    status: Literal["not_collected", "captured", "unavailable", "failed"]
    observed_at: str
    retry_after: str
    language: str
    language_name: str
    generated: bool
    segments: list[TranscriptSegmentOut]
