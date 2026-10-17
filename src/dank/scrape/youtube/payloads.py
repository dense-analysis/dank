"""Normalize listing renderers while retaining their discovery context."""

from __future__ import annotations

import datetime
import json
import re
from collections.abc import Iterator
from typing import Any, NamedTuple, cast
from urllib.parse import parse_qs, urljoin, urlsplit

from dank.config import YouTubeSettings
from dank.model import AssetDiscovery, RawPost

ORIGIN = "https://www.youtube.com"
UK_HYPE_PLAYLIST = "OLJbGgOZgwZ9k_XXE2tcr-JsD4tag1iKjng"
VIDEO_RENDERERS = {
    "videoRenderer",
    "gridVideoRenderer",
    "playlistVideoRenderer",
    "compactVideoRenderer",
    "reelItemRenderer",
    "shortsLockupViewModel",
    "lockupViewModel",
}
POST_RENDERERS = {"backstagePostRenderer", "postRenderer"}
SHELF_RENDERERS = {
    "shelfRenderer",
    "richShelfRenderer",
    "reelShelfRenderer",
}


class YouTubeFeed(NamedTuple):
    url: str
    name: str
    kind: str
    tab: str = ""
    region: str = ""


def as_dict(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def text(value: object) -> str:
    if isinstance(value, str):
        return value

    data = as_dict(value)

    if isinstance(data.get("content"), str):
        return data["content"]

    return str(
        data.get("simpleText")
        or "".join(
            str(as_dict(run).get("text", "")) for run in data.get("runs", [])
        ),
    )


def objects(value: object) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        data = cast(dict[str, Any], value)
        yield data

        for child in data.values():
            yield from objects(child)
    elif isinstance(value, list):
        for child in cast(list[object], value):
            yield from objects(child)


def renderers(
    value: object,
    shelf: str = "",
) -> Iterator[tuple[str, dict[str, Any], str]]:
    if isinstance(value, list):
        for child in cast(list[object], value):
            yield from renderers(child, shelf)

        return

    for key, child in as_dict(value).items():
        data = as_dict(child)

        if key in VIDEO_RENDERERS | POST_RENDERERS:
            yield key, data, shelf
        elif key in SHELF_RENDERERS:
            heading = text(data.get("title")) or shelf
            yield from renderers(data, heading)
        else:
            yield from renderers(child, shelf)


def hype_playlist(value: object) -> str | None:
    for node in objects(value):
        shelf = as_dict(node.get("shelfRenderer"))

        if text(shelf.get("title")).casefold() != "top 100":
            continue

        for link in objects(shelf.get("menu")):
            url = str(link.get("url", ""))

            if url.startswith("/playlist?list="):
                return urljoin(ORIGIN, url)

    return None


def playlist_title(value: object) -> str:
    for node in objects(value):
        metadata = as_dict(node.get("playlistMetadataRenderer"))

        if metadata.get("title"):
            return text(metadata["title"])

    return ""


def _live(renderer: dict[str, Any], shelf: str) -> bool:
    if shelf.casefold().startswith(("live now", "live news")):
        return True

    for node in objects(renderer):
        if (
            node.get("isLive") is True
            or node.get("isLiveNow") is True
            or "upcomingEventData" in node
            or "LIVE" in str(node.get("badgeStyle", ""))
        ):
            return True

        if str(node.get("style", "")) in {
            "LIVE",
            "UPCOMING",
            "BADGE_STYLE_TYPE_LIVE_NOW",
        } or str(node.get("label", "")).upper() in {"LIVE", "LIVE NOW"}:
            return True

    return False


def _identity(key: str, renderer: dict[str, Any]) -> tuple[str, str]:
    if key in POST_RENDERERS:
        return "post", str(renderer.get("postId", ""))

    identifier = str(
        renderer.get("videoId") or renderer.get("contentId") or "",
    )
    short = key in {"reelItemRenderer", "shortsLockupViewModel"}

    for node in objects(renderer):
        endpoint = as_dict(node.get("reelWatchEndpoint"))
        url = str(node.get("url", ""))

        if endpoint or url.startswith("/shorts/"):
            short = True
            identifier = identifier or str(endpoint.get("videoId", ""))

        if url.startswith("/shorts/"):
            identifier = identifier or urlsplit(url).path.split("/")[-1]

    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", identifier):
        return "", ""

    return ("short" if short else "video"), identifier


def _thumbnail(value: object) -> str:
    candidates = [
        node
        for node in objects(value)
        if isinstance(node.get("url"), str)
        and str(node["url"]).startswith(("https://", "//"))
    ]

    if not candidates:
        return ""

    largest = max(candidates, key=lambda node: int(node.get("width", 0)))

    return urljoin(ORIGIN, str(largest["url"]))


def _channel(renderer: dict[str, Any]) -> tuple[str, str, str]:
    byline = (
        renderer.get("ownerText")
        or renderer.get("shortBylineText")
        or renderer.get("longBylineText")
        or renderer.get("authorText")
    )
    candidates = (
        [byline]
        if byline
        else [
            node
            for node in objects(renderer.get("metadata", {}))
            if "content" in node and "commandRuns" in node
        ]
    )

    for candidate in candidates:
        for node in objects(candidate):
            endpoint = as_dict(node.get("browseEndpoint"))
            identifier = str(endpoint.get("browseId", ""))

            if identifier.startswith("UC"):
                url = str(
                    endpoint.get("canonicalBaseUrl")
                    or f"/channel/{identifier}",
                )

                return identifier, text(candidate), urljoin(ORIGIN, url)

    for node in objects(renderer.get("metadata", {})):
        rows = as_dict(node.get("contentMetadataViewModel")).get(
            "metadataRows", [],
        )

        if rows:
            parts = as_dict(rows[0]).get("metadataParts", [])
            names = [text(part.get("text")) for part in parts
                     if not part.get("accessibilityLabel")]

            if names:
                return "", " ".join(names), ""

    return "", text(byline), ""


def _duration(renderer: dict[str, Any]) -> str:
    if duration := text(renderer.get("lengthText")):
        return duration

    for node in objects(
        [
            renderer.get("thumbnailOverlays", []),
            renderer.get("contentImage", {}),
        ],
    ):
        overlay = as_dict(node.get("thumbnailOverlayTimeStatusRenderer"))

        if overlay:
            return text(overlay.get("text"))

        badge = as_dict(node.get("thumbnailBadgeViewModel"))

        if re.fullmatch(r"[0-9:]+", text(badge.get("text"))):
            return text(badge.get("text"))

    return ""


def _rank(renderer: dict[str, Any], fallback: int) -> int:
    index = text(renderer.get("index"))

    if index.isdecimal():
        return int(index)

    for node in objects(
        [renderer.get("badges", []), renderer.get("metadata", {})],
    ):
        if match := re.fullmatch(
            r"#(\d+) hyped",
            str(node.get("label") or node.get("badgeText") or ""),
        ):
            return int(match[1])

    return fallback


def _post_text(value: object) -> str:
    runs = as_dict(value).get("runs", [])
    parts: list[str] = []

    for run in runs:
        data = as_dict(run)
        label = text(data.get("text"))
        endpoint = as_dict(data.get("navigationEndpoint"))
        destination = str(as_dict(endpoint.get("urlEndpoint")).get("url", ""))

        if not destination:
            destination = str(as_dict(as_dict(endpoint.get(
                "commandMetadata",
            )).get("webCommandMetadata")).get("url", ""))

        destination = _external_link(destination)

        if destination and label.startswith(("https://", "http://", "www.")):
            label = destination

        parts.append(label)

    return "".join(parts) if parts else text(value)


def _external_link(value: str) -> str:
    if not value:
        return ""

    try:
        parsed = urlsplit(urljoin(ORIGIN, value))

        if (parsed.hostname in {"www.youtube.com", "youtube.com"}
                and parsed.path == "/redirect"):
            value = parse_qs(parsed.query).get("q", [""])[0]
            parsed = urlsplit(value)

        if parsed.scheme in {"http", "https"} and parsed.hostname:
            return parsed.geturl()
    except ValueError:
        pass

    return ""


def normalize_renderer(
    key: str,
    renderer: dict[str, Any],
    *,
    feed: YouTubeFeed,
    shelf: str,
    rank: int,
    settings: YouTubeSettings,
    request_url: str,
    observed_at: datetime.datetime,
    playlist_name: str = "",
    channel: tuple[str, str, str] = ("", "", ""),
    position: int = 0,
) -> RawPost | None:
    kind, identifier = _identity(key, renderer)

    if (
        not kind
        or not identifier
        or _live(renderer, shelf)
        or (kind == "short" and not settings.include_shorts)
        or (kind == "post" and not settings.include_posts)
    ):
        return None

    channel_id, channel_name, channel_url = _channel(renderer)

    if not channel_id and not channel_name:
        channel_id, channel_name, channel_url = channel
    url = (
        f"{ORIGIN}/post/{identifier}"
        if kind == "post"
        else f"{ORIGIN}/watch?v={identifier}"
    )
    metadata = as_dict(
        as_dict(renderer.get("metadata")).get(
            "lockupMetadataViewModel",
        ),
    )
    overlay = as_dict(renderer.get("overlayMetadata"))
    labels = [
        str(node.get("accessibilityLabel", ""))
        for node in objects(metadata.get("metadata", {}))
    ]
    title = text(
        renderer.get("title")
        or metadata.get("title")
        or overlay.get("primaryText"),
    )
    body = _post_text(renderer.get("contentText"))
    thumbnail = _thumbnail(
        renderer.get("thumbnail")
        or renderer.get("contentImage")
        or renderer.get("thumbnailViewModel")
        or renderer.get("content")
        or renderer.get("backstageAttachment")
        or renderer.get("attachment"),
    )
    payload = {
        "schema_version": 1,
        "kind": kind,
        "video_id": identifier if kind != "post" else "",
        "post_id": identifier if kind == "post" else "",
        "title": title or (body[:160] if kind == "post" else ""),
        "text": body,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_url": channel_url,
        "thumbnail_url": thumbnail,
        "duration_text": _duration(renderer),
        "views_text": text(
            renderer.get("viewCountText")
            or renderer.get("shortViewCountText")
            or overlay.get("secondaryText"),
        )
        or next(
            (label for label in labels if "view" in label.casefold()),
            "",
        ),
        "published_text": text(renderer.get("publishedTimeText"))
        or next(
            (label for label in labels if "ago" in label.casefold()),
            "",
        ),
        "likes_text": text(renderer.get("voteCount")),
        "is_live": False,
        "detail_capture": {"status": "disabled"},
        "discovery": {
            "feed_url": feed.url,
            "feed_name": playlist_name or feed.name,
            "feed_kind": feed.kind,
            "tab": feed.tab,
            "shelf": shelf or ("Top 100" if feed.kind == "hype" else ""),
            "rank": _rank(renderer, rank),
            "position": position,
            "region": feed.region or settings.region,
            "playlist_id": parse_qs(urlsplit(request_url).query).get(
                "list",
                [""],
            )[0],
            "observed_at": observed_at.isoformat(),
        },
        "renderer": {key: renderer},
    }

    return RawPost(
        domain="youtube.com",
        post_id=f"{'post' if kind == 'post' else 'video'}:{identifier}",
        url=url,
        post_created_at=None,
        scraped_at=observed_at,
        source="youtube",
        request_url=request_url,
        payload=json.dumps(payload, separators=(",", ":")),
    )


def discover_assets(
    post: RawPost,
    settings: YouTubeSettings,
) -> list[AssetDiscovery]:
    payload = as_dict(json.loads(post.payload))
    assets: list[AssetDiscovery] = []

    if thumbnail := payload.get("thumbnail_url"):
        assets.append(
            AssetDiscovery(
                "youtube",
                "youtube.com",
                post.post_id,
                str(thumbnail),
                "image",
                None,
            ),
        )

    if settings.scrape_videos and payload.get("kind") in {"video", "short"}:
        assets.append(
            AssetDiscovery(
                "youtube",
                "youtube.com",
                post.post_id,
                post.url,
                "youtube",
                None,
            ),
        )

    return assets
