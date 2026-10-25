import datetime
from typing import Any, NamedTuple


class RawPost(NamedTuple):
    """
    A raw post to be stored in the database during scraping for later
    processing.
    """
    domain: str
    post_id: str
    url: str
    post_created_at: datetime.datetime | None
    scraped_at: datetime.datetime
    source: str
    request_url: str
    # A payload such as raw XML data from a scraped post.
    payload: str
    # Collection hint; storage computes this from the unchanged payload.
    is_reference: bool = False


class YouTubeThumbnailCapture(NamedTuple):
    """The listing metadata associated with a thumbnail request."""

    title: str
    observed_at: datetime.datetime


class AssetDiscovery(NamedTuple):
    """
    An asset discovery used in scraping for queuing assets to process
    into RawAsset data and downloaded asset files in the filesystem.
    """
    source: str
    domain: str
    post_id: str
    url: str
    asset_type: str
    estimated_size_bytes: int | None
    # Public document origin for media hosts that reject unreferenced requests.
    referrer_url: str | None = None
    youtube_capture: YouTubeThumbnailCapture | None = None


class YouTubeThumbnailObservation(NamedTuple):
    """One successfully fetched image; raw evidence remains immutable."""

    post_id: str
    observed_at: datetime.datetime
    captured_url: str
    fetched_url: str
    local_path: str
    content_hash: str
    visual_hash: str
    visual_color: str
    width: int
    height: int
    title: str = ""
    title_observed_at: datetime.datetime | None = None


class RawAsset(NamedTuple):
    """
    A raw asset to be stored in the database during scraping for later
    processing.

    Transformed from an ``AssetDiscovery``.
    """
    domain: str
    post_id: str
    url: str
    asset_type: str
    scraped_at: datetime.datetime
    source: str
    local_path: str
    thumbnail: YouTubeThumbnailObservation | None = None


class Asset(NamedTuple):
    """
    A processed asset stored in the database after processing previously
    saved raw asset data and files.

    Transformed from a ``RawAsset``.
    """
    domain: str
    post_id: str
    url: str
    local_path: str
    content_type: str
    size_bytes: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    source: str


class Post(NamedTuple):
    """
    A processed post stored in the database after processing previously
    saved raw post data.

    Transformed from a ``RawPost``.
    """
    domain: str
    post_id: str
    url: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    author: str
    title: str
    title_embedding: tuple[float, ...]
    html: str
    html_embedding: tuple[float, ...]
    source: str


class RedditCommunityDiscovery(NamedTuple):
    """Community identity and its icon observed on a Reddit listing page."""

    community: str
    icon_url: str
    scraped_at: datetime.datetime
    request_url: str
    payload: str


class RedditCommunityTotal(NamedTuple):
    """An archive's post-count estimate, with its original response."""

    community: str
    total_posts: int
    updated_at: datetime.datetime
    scraped_at: datetime.datetime
    request_url: str
    payload: str


class YouTubeDiscovery(NamedTuple):
    """A feed appearance, independent of its canonical video or post."""

    run_id: str
    item_id: str
    kind: str
    observed_at: datetime.datetime
    feed_url: str
    feed_name: str
    feed_kind: str
    tab: str
    shelf: str
    rank: int
    region: str
    playlist_id: str
    payload: str


class YouTubeChannelDiscovery(NamedTuple):
    """Channel identity, including the source's possibly rounded total."""

    channel_id: str
    channel_name: str
    channel_url: str
    feed_url: str
    icon_url: str
    reported_video_count: int | None
    reported_video_count_text: str
    observed_at: datetime.datetime
    request_url: str
    payload: str


class YouTubePlaylistCapture(NamedTuple):
    """A bounded listing check, retaining every ordered video occurrence."""

    playlist_id: str
    metadata: dict[str, Any]
    entries: tuple[dict[str, Any], ...]
    status: str
    error_type: str
    checked_at: datetime.datetime
