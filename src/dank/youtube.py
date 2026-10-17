"""Stable YouTube identities and detached collection observations."""

from __future__ import annotations

import html
import json
import re
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit

from dank.model import RawPost, YouTubeDiscovery

VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}")
YOUTUBE_HOSTS = {
    "youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com",
    "youtube-nocookie.com", "www.youtube-nocookie.com",
}


def canonical_video_id(value: str) -> str | None:
    """Recognize links without redirects or changing video ID case."""
    try:
        url = urlsplit(html.unescape(value.strip()))

        if (
            url.scheme not in {"http", "https"} or url.username or url.password
            or url.port not in {None, 80, 443}
        ):
            return None

        parts = url.path.strip("/").split("/")

        if url.hostname in {"youtu.be", "www.youtu.be"} and len(parts) == 1:
            candidate = parts[0]
        elif url.hostname in YOUTUBE_HOSTS:
            if parts == ["watch"]:
                values = parse_qs(url.query).get("v", [])
                candidate = values[0] if len(values) == 1 else ""
            elif len(parts) == 2 and parts[0] in {"shorts", "embed"}:
                candidate = parts[1]
            else:
                return None
        else:
            return None
    except ValueError:
        return None

    return candidate if VIDEO_ID.fullmatch(candidate) else None


def canonical_youtube_url(value: str) -> str | None:
    video_id = canonical_video_id(value)

    return f"https://www.youtube.com/watch?v={video_id}" if video_id else None


def youtube_post_id(value: str) -> str | None:
    try:
        url = urlsplit(value)
        parts = url.path.strip("/").split("/")

        if (
            url.scheme in {"http", "https"} and url.hostname in YOUTUBE_HOSTS
            and not url.username and not url.password
            and url.port in {None, 80, 443} and len(parts) == 2
            and parts[0] == "post" and re.fullmatch(r"[A-Za-z0-9_-]{10,128}",
                                                      parts[1])
        ):
            return parts[1]
    except ValueError:
        pass

    return None


def payload_object(value: object) -> dict[str, Any]:
    """Read an envelope defensively; storage retains the original bytes."""
    try:
        data = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError):
        return {}

    return cast(dict[str, Any], data) if isinstance(data, dict) else {}


def discovery_from_post(post: RawPost, run_id: str) -> YouTubeDiscovery:
    payload = payload_object(post.payload)
    discovery = payload_object(payload.get("discovery"))
    rank = discovery.get("rank")

    return YouTubeDiscovery(
        run_id=run_id, item_id=post.post_id,
        kind=str(payload.get("kind", "video")), observed_at=post.scraped_at,
        feed_url=str(discovery.get("feed_url") or post.request_url),
        feed_name=str(discovery.get("feed_name") or "YouTube"),
        feed_kind=str(discovery.get("feed_kind") or "channel"),
        tab=str(discovery.get("tab") or ""),
        shelf=str(discovery.get("shelf") or ""),
        rank=rank if type(rank) is int and rank > 0 else 0,
        region=str(discovery.get("region") or ""),
        playlist_id=str(discovery.get("playlist_id") or ""),
        payload=post.payload,
    )
