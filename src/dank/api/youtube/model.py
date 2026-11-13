"""YouTube rules: filters, cursors, playlist pages and what a capture says.

Rule violations raise InvalidInput with the wording the reader shows.
"""

# `count` is the API's name for a channel's item count, as in posts/model.py.
# pyright: reportIncompatibleMethodOverride=false

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any, NamedTuple, cast

from dank.errors import InvalidInput
from dank.model import parse_datetime
from dank.youtube import payload_object

RECOMMENDATIONS_FEED = "https://www.youtube.com/"
ITEM_PAGE_LIMIT = 500
CHANNEL_PAGE_LIMIT = 50
PLAYLIST_PAGE_LIMIT = 100
MAX_FILTER_LENGTH = 2000
PLAYLIST_ID = re.compile(r"[A-Za-z0-9_-]{1,200}")

# The reader helpers build these JSON-shaped projections.
# schemas.py gives them their contract. See "Projections" in docs/api.md.
type ItemPayload = dict[str, Any]
type ChannelPayload = dict[str, Any]
type PlaylistPayload = dict[str, Any]
type SourcesPayload = dict[str, Any]
type TranscriptPayload = dict[str, Any]


class YouTubeFilters(NamedTuple):
    feed: str = ""
    channel: str = ""
    kind: str = ""
    q: str = ""
    item_id: str = ""
    limit: int = 30
    cursor: str = ""
    sort: str = "newest"
    channel_scope: str = ""

    def signature(self) -> str:
        values = (*self[:5], self.sort)

        if self.feed == RECOMMENDATIONS_FEED:
            values = (*values, "recommendation-history-v1")

        if self.channel_scope:
            values = (*values, self.channel_scope)

        encoded = json.dumps(values).encode()

        return hashlib.sha256(encoded).hexdigest()[:24]


class ItemBoundary(NamedTuple):
    """Where the next page of items starts."""

    observed_at: str
    item_id: str
    position: int = 0
    reference_count: int = 0


class ItemListing(NamedTuple):
    items: tuple[ItemPayload, ...]
    total: int
    after: ItemBoundary | None


class ItemsPage(NamedTuple):
    items: tuple[ItemPayload, ...]
    next_cursor: str | None
    total: int


class ChannelBoundary(NamedTuple):
    """Where the next page of channels starts."""

    count: int
    key: str


class ChannelCursor(NamedTuple):
    snapshot: str
    count: int
    key: str


class ChannelListing(NamedTuple):
    channels: tuple[ChannelPayload, ...]
    after: ChannelBoundary | None
    total: int
    unattributed_items: int


class ChannelsPage(NamedTuple):
    channels: tuple[ChannelPayload, ...]
    next_cursor: str | None
    total: int
    unattributed_items: int


class PlaylistPage(NamedTuple):
    limit: int = 30
    q: str = ""
    snapshot: str = ""
    after: tuple[str, str] | int | None = None
    sort: str = "recent"


class PlaylistListing(NamedTuple):
    playlists: tuple[PlaylistPayload, ...]
    total: int
    after: tuple[str, str] | None


class PlaylistsPage(NamedTuple):
    playlists: tuple[PlaylistPayload, ...]
    total: int
    next_cursor: str | None


class PlaylistEntries(NamedTuple):
    playlist: PlaylistPayload
    items: tuple[ItemPayload, ...]
    total: int
    after: int | None


class PlaylistDetail(NamedTuple):
    playlist: PlaylistPayload
    items: tuple[ItemPayload, ...]
    next_cursor: str | None
    total: int


class MetadataObservation(NamedTuple):
    """One provider response, fresh or reused while it's still valid."""

    status: str
    provider: str
    cached: bool
    observed_at: str
    retry_after: str


class MetadataCapture(NamedTuple):
    """An observation, and whether a preview survives from an earlier one."""

    video_id: str
    status: str
    provider: str
    cached: bool
    observed_at: str
    retry_after: str
    available: bool
    captured_at: str


def youtube_filters(sent: YouTubeFilters) -> YouTubeFilters:
    """Apply the rules to the values a request sent."""
    filters = sent._replace(
        feed=sent.feed.strip(),
        channel=sent.channel.strip(),
        q=sent.q.strip(),
    )

    if not 1 <= filters.limit <= ITEM_PAGE_LIMIT or filters.kind not in {
        "", "video", "short", "post",
    }:
        raise InvalidInput("Invalid limit or content kind")

    if any(len(value) > MAX_FILTER_LENGTH for value in filters[:5]):
        raise InvalidInput("YouTube filters are limited to 2000 characters")

    if filters.sort not in {"newest", "oldest", "most-linked"} or (
        filters.sort != "newest" and filters.feed != "linked"
    ):
        raise InvalidInput("Invalid sort for this YouTube feed")

    if filters.channel_scope not in {"", "configured"}:
        raise InvalidInput("Invalid channel scope")

    if filters.cursor:
        decode_youtube_cursor(filters)

    return filters


def encode_youtube_cursor(
    filters: YouTubeFilters, observed_at: str, item_id: str,
    position: int = 0, reference_count: int = 0, *, snapshot_time: str = "",
) -> str:
    data: list[Any] = [
        filters.signature(), observed_at, item_id, position, reference_count,
    ]

    if filters.feed == RECOMMENDATIONS_FEED:
        data.append(snapshot_time or observed_at)

    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()


def decode_youtube_cursor(
    filters: YouTubeFilters,
) -> tuple[str, str, int, int]:
    data = _cursor_data(filters)

    return data[1], data[2], data[3], data[4]


def youtube_cursor_snapshot(filters: YouTubeFilters) -> str:
    data = _cursor_data(filters)

    return str(data[5] if len(data) == 6 else data[1])


def _cursor_data(filters: YouTubeFilters) -> list[Any]:
    try:
        if len(filters.cursor) > 8192:
            raise ValueError

        raw = json.loads(base64.b64decode(
            filters.cursor, altchars=b"-_", validate=True,
        ))

        if not isinstance(raw, list):
            raise ValueError

        data = cast(list[Any], raw)

        if (
            len(data) != (6 if filters.feed == RECOMMENDATIONS_FEED else 5)
            or not all(isinstance(value, str) for value in data[:3])
            or type(data[3]) is not int or not 0 <= data[3] <= 2**32 - 1
            or type(data[4]) is not int or not 0 <= data[4] <= 2**64 - 1
            or data[0] != filters.signature()
        ):
            raise ValueError

        date = dt.datetime.fromisoformat(data[1])

        if date.tzinfo is None:
            raise ValueError

        data[1] = date.astimezone(dt.UTC).isoformat()

        if len(data) == 6:
            if not isinstance(data[5], str):
                raise ValueError

            snapshot = dt.datetime.fromisoformat(data[5])

            if snapshot.tzinfo is None or snapshot < date:
                raise ValueError

            data[5] = snapshot.astimezone(dt.UTC).isoformat()

        return data
    except (ValueError, TypeError, UnicodeError):
        raise InvalidInput("Invalid cursor for these filters") from None


def require_channel_page(filters: YouTubeFilters) -> None:
    if filters.limit > CHANNEL_PAGE_LIMIT or filters.item_id:
        raise InvalidInput("Channel pages allow up to 50 channels")


def channel_cursor(
    filters: YouTubeFilters, snapshot: str, count: int, key: str,
) -> str:
    return base64.urlsafe_b64encode(json.dumps([
        filters.signature(), snapshot, count, key,
    ]).encode()).decode()


def parse_channel_cursor(
    filters: YouTubeFilters, cursor: str,
) -> ChannelCursor:
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

        return ChannelCursor(
            snapshot.astimezone(dt.UTC).isoformat(), raw[2], raw[3],
        )
    except (ValueError, TypeError, UnicodeError):
        raise InvalidInput(
            "Invalid channel cursor for these filters",
        ) from None


def playlist_page(
    *,
    limit: int,
    q: str = "",
    cursor: str = "",
    sort: str = "recent",
    playlist_id: str = "",
) -> PlaylistPage:
    search = q.strip()

    if sort not in {"recent", "az"}:
        raise InvalidInput("Playlist sort must be recent or az")

    if (
        not 1 <= limit <= PLAYLIST_PAGE_LIMIT
        or len(search) > MAX_FILTER_LENGTH
    ):
        raise InvalidInput("Invalid playlist limit or search")

    if playlist_id and not PLAYLIST_ID.fullmatch(playlist_id):
        raise InvalidInput("Invalid YouTube playlist identifier")

    snapshot = ""
    after = None

    if cursor:
        snapshot, after = parse_cursor(cursor, playlist_id, search, sort)

    return PlaylistPage(limit, search, snapshot, after, sort)


def playlist_cursor(
    identity: str,
    query: str,
    snapshot: str,
    after: tuple[str, str] | int,
    sort: str = "recent",
) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(
            [
                identity,
                query,
                snapshot,
                after,
                sort,
            ],
        ).encode(),
    ).decode()


def parse_cursor(
    cursor: str,
    identity: str,
    query: str,
    sort: str = "recent",
) -> tuple[str, tuple[str, str] | int]:
    try:
        if len(cursor) > 16384:
            raise ValueError

        decoded: object = json.loads(
            base64.b64decode(
                cursor,
                altchars=b"-_",
                validate=True,
            ),
        )

        if not isinstance(decoded, list):
            raise ValueError

        values = cast(list[Any], decoded)

        if len(values) != 5 or values[4] != sort:
            raise ValueError

        if values[:2] != [identity, query] or not isinstance(values[2], str):
            raise ValueError

        snapshot = dt.datetime.fromisoformat(values[2])

        if snapshot.tzinfo is None:
            raise ValueError

        after = _cursor_boundary(values[3], identity)

        return snapshot.astimezone(dt.UTC).isoformat(), after
    except (ValueError, TypeError, UnicodeError):
        raise InvalidInput("Invalid cursor for this playlist view") from None


def _cursor_boundary(value: Any, identity: str) -> tuple[str, str] | int:
    if identity:
        if type(value) is not int or not 1 <= value <= 2**32 - 1:
            raise ValueError

        return value

    if not isinstance(value, list):
        raise ValueError

    parts = cast(list[Any], value)

    if (len(parts) != 2 or any(
        not isinstance(part, str) or len(part) > 4000 for part in parts
    )):
        raise ValueError

    return str(parts[0]), str(parts[1])


def metadata_capture(
    video_id: str, observed: MetadataObservation, captured_at: str | None,
) -> MetadataCapture:
    """A failed refresh never hides the last successful preview."""
    return MetadataCapture(
        video_id=video_id,
        status=observed.status,
        provider=observed.provider,
        cached=observed.cached,
        observed_at=observed.observed_at,
        retry_after=observed.retry_after,
        available=captured_at is not None,
        captured_at=captured_at or "",
    )


def transcript_view(
    video_id: str, row: Mapping[str, Any],
) -> TranscriptPayload:
    payload = payload_object(row.get("payload"))
    captured = row.get("status") == "captured"

    return {
        "video_id": video_id,
        "status": row.get("status", "not_collected"),
        "observed_at": timestamp_text(row.get("observed_at")),
        "retry_after": (
            "" if captured else timestamp_text(row.get("retry_after"))
        ),
        "language": str(payload.get("language") or ""),
        "language_name": str(payload.get("language_name") or ""),
        "generated": payload.get("generated") is True,
        "segments": payload.get("segments", []) if captured else [],
    }


def not_collected(video_id: str) -> TranscriptPayload:
    return transcript_view(video_id, {})


def timestamp_text(value: object) -> str:
    timestamp = parse_datetime(value)

    return timestamp.isoformat() if timestamp else ""
