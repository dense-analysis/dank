"""Rows for the YouTube tables, as collection saves them."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from tests.api.clickhouse import COLLECTED_AT

HYPE = "https://www.youtube.com/feed/hype"
HOME = "https://www.youtube.com/"
CHANNEL_URL = "https://www.youtube.com/@Channel4News"


def video_payload(title: str = "", **fields: Any) -> dict[str, Any]:
    return {
        "title": title, "channel_id": "UC4News",
        "channel_name": "Channel 4 News", "channel_url": CHANNEL_URL,
        "duration_text": "1:02", "views_text": "12K views",
        "published_text": "1 day ago", **fields,
    }


def discovery(
    item_id: str,
    feed_url: str = HYPE,
    *,
    feed_kind: str = "hype",
    feed_name: str = "Hype",
    run_id: str = "run-1",
    rank: int = 1,
    kind: str = "video",
    observed_at: dt.datetime = COLLECTED_AT,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "item_id": item_id,
        "kind": kind,
        "observed_at": observed_at,
        "feed_url": feed_url,
        "feed_name": feed_name,
        "feed_kind": feed_kind,
        "tab": "",
        "shelf": "Top 100",
        "rank": rank,
        "region": "GB",
        "playlist_id": "",
        "payload": json.dumps(video_payload() if payload is None else payload),
    }


def reference(
    video_id: str,
    domain: str = "reddit.com",
    post_id: str = "t3_a",
    *,
    kind: str = "post",
    reference_id: str = "",
    title: str = "A shared video",
    format_hint: str = "video",
    observed_at: dt.datetime = COLLECTED_AT,
) -> dict[str, Any]:
    url = f"https://{domain}/{post_id}"

    return {
        "video_id": video_id,
        "format_hint": format_hint,
        "domain": domain,
        "post_id": post_id,
        "url": url,
        "source": "reddit",
        "title": title,
        "kind": kind,
        "reference_id": reference_id,
        "reference_url": url,
        "author": "Reader",
        "matched_url": f"https://youtu.be/{video_id}",
        "excerpt": "Seen it happen",
        "observed_at": observed_at,
    }


def metadata(
    video_id: str,
    *,
    status: str = "captured",
    observed_at: dt.datetime = COLLECTED_AT,
    retry_after: dt.datetime | None = None,
    title: str = "A provider title",
) -> dict[str, Any]:
    retry = retry_after or observed_at + dt.timedelta(days=7)
    capture: dict[str, Any] = {
        "status": status, "provider": "youtube_oembed",
        "observed_at": observed_at.isoformat(),
        "retry_after": retry.isoformat(),
    }

    if status == "captured":
        capture.update(available=True, captured_at=observed_at.isoformat())

    fields = {"title": title} if status == "captured" else {}

    return {
        "video_id": video_id,
        "observed_at": observed_at,
        "retry_after": retry,
        "status": status,
        "provider": "youtube_oembed",
        "request_url": "https://www.youtube.com/oembed",
        "http_status": 200 if status == "captured" else 503,
        "error": "",
        "provider_payload": "{}",
        "payload": json.dumps({**fields, "metadata_capture": capture}),
    }


def transcript(
    video_id: str,
    *,
    status: str = "captured",
    observed_at: dt.datetime = COLLECTED_AT,
    retry_after: dt.datetime | None = None,
) -> dict[str, Any]:
    payload = {
        "language": "en", "language_name": "English", "generated": True,
        "segments": [
            {"start": 1.5, "duration": 2.0, "text": "Saved words"},
            {"start": 3.5, "duration": 1.25, "text": "More words"},
        ],
    } if status == "captured" else {}

    return {
        "video_id": video_id,
        "observed_at": observed_at,
        "retry_after": retry_after or observed_at + dt.timedelta(hours=1),
        "status": status,
        "error": "private error",
        "track_payload": "private provider URL",
        "provider_payload": "private provider body",
        "payload": json.dumps(payload),
    }


def playlist(
    playlist_id: str = "PLexample",
    *,
    title: str = "Saved music",
    snapshot_hash: str = "old-order",
    status: str = "complete",
    entry_count: int = 3,
    reported_count: int | None = 3,
    checked_at: dt.datetime = COLLECTED_AT,
    thumbnail_url: str = "https://i.ytimg.com/example.jpg",
    channel_url: str = "",
) -> dict[str, Any]:
    return {
        "playlist_id": playlist_id,
        "title": title,
        "url": f"https://www.youtube.com/playlist?list={playlist_id}",
        "description": "Saved playlist",
        "thumbnail_url": thumbnail_url,
        "channel_id": "UCexample",
        "channel_name": "Example",
        "channel_url": channel_url,
        "reported_count": reported_count,
        "entry_count": entry_count,
        "sort_order": "Manual",
        "snapshot_hash": snapshot_hash,
        "observed_at": checked_at,
        "checked_at": checked_at,
        "status": status,
        "error_type": "",
        "payload": "{}",
    }


def entry(
    position: int,
    video_id: str = "aaaaaaaaaaa",
    *,
    playlist_id: str = "PLexample",
    snapshot_hash: str = "old-order",
    title: str = "Saved video",
    available: bool = True,
    duration_seconds: float | None = 3661,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "playlist_id": playlist_id,
        "snapshot_hash": snapshot_hash,
        "position": position,
        "entry_id": f"entry-{position}",
        "video_id": video_id,
        "title": title,
        "channel_id": "UCexample",
        "channel_name": "Example",
        "channel_url": "",
        "thumbnail_url": "",
        "duration_seconds": duration_seconds,
        "available": available,
        "payload": json.dumps(payload or {}),
    }


def channel(
    channel_id: str = "UC4News",
    *,
    channel_url: str = CHANNEL_URL,
    feed_url: str = CHANNEL_URL + "/videos",
    icon_url: str = "https://yt3.ggpht.com/icon",
    local_path: str = "",
    content_type: str = "image/png",
    reported_video_count: int | None = 26000,
    reported_video_count_text: str = "26K videos",
) -> dict[str, Any]:
    return {
        "channel_id": channel_id,
        "channel_name": "Channel 4 News",
        "channel_url": channel_url,
        "feed_url": feed_url,
        "icon_url": icon_url,
        "local_path": local_path,
        "content_type": content_type,
        "reported_video_count": reported_video_count,
        "reported_video_count_text": reported_video_count_text,
        "observed_at": COLLECTED_AT,
        "request_url": feed_url,
        "payload": "{}",
    }


def thumbnail(
    post_id: str,
    local_path: str,
    *,
    content_hash: str = "hash-1",
    title: str = "Captured title",
    observed_at: dt.datetime = COLLECTED_AT,
) -> dict[str, Any]:
    url = "https://i.ytimg.com/vi/thumbnail.jpg"

    return {
        "post_id": post_id,
        "observed_at": observed_at,
        "captured_url": url,
        "fetched_url": url,
        "local_path": local_path,
        "content_hash": content_hash,
        "visual_hash": "0" * 64,
        "visual_color": "100,100,100",
        "width": 1280,
        "height": 720,
        "title": title,
        "title_observed_at": observed_at,
    }
