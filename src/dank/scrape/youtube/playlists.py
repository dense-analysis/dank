"""Ordered, flat playlist snapshots; no watch pages or media requests."""

from __future__ import annotations

import datetime
import hashlib
import json
import re
from collections.abc import Iterable
from typing import Any, NamedTuple, cast
from urllib.parse import parse_qs, urljoin, urlsplit

from dank.model import RawPost, YouTubePlaylistCapture
from dank.youtube import VIDEO_ID

from .payloads import ORIGIN, as_dict, objects, playlist_title, text

PLAYLIST_ID = re.compile(r"[A-Za-z0-9_-]{2,128}")
LIBRARY_URL = ORIGIN + "/feed/playlists"


class PlaylistPage(NamedTuple):
    entries: tuple[dict[str, Any], ...]
    continuation: str
    recognized: bool


def playlist_id(url: str) -> str:
    parsed = urlsplit(url)
    identifier = parse_qs(parsed.query).get("list", [""])[0]

    if (
        parsed.hostname not in {"youtube.com", "www.youtube.com"}
        or parsed.path != "/playlist"
        or not PLAYLIST_ID.fullmatch(identifier)
    ):
        raise ValueError("Invalid YouTube playlist URL")

    return identifier


def _thumbnail(value: object) -> str:
    images = [
        node
        for node in objects(value)
        if isinstance(node.get("url"), str)
        and ("width" in node or "height" in node)
    ]

    if not images:
        return ""

    best = max(images, key=lambda item: int(item.get("width", 0)))
    url = str(best["url"])

    return urljoin(ORIGIN, url) if url.startswith(("https://", "//")) else ""


def _channel(value: object) -> tuple[str, str, str]:
    for node in objects(value):
        endpoint = as_dict(
            as_dict(node.get("navigationEndpoint")).get(
                "browseEndpoint",
            ),
        )
        identifier = str(endpoint.get("browseId", ""))

        if identifier.startswith("UC"):
            return (
                identifier,
                text(node.get("text") or node.get("title")),
                ORIGIN + "/channel/" + identifier,
            )

    return "", text(value), ""


def _count(value: object) -> int | None:
    match = re.fullmatch(
        r"([\d,]+)\s+(?:videos?|items?)", text(value).strip(), re.IGNORECASE,
    )

    return int(match[1].replace(",", "")) if match else None


def _sort(value: object) -> str:
    for node in objects(value):
        menu = as_dict(node.get("sortFilterSubMenuRenderer"))
        items = menu.get("subMenuItems", [])

        for index, item in enumerate(items):
            if (
                item.get("selected") is True
                or menu.get("selectedItemIndex") == index
            ):
                return text(item.get("title")) or "YouTube order"

    return "YouTube order"


def playlist_metadata(
    identifier: str,
    value: object,
    *,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "playlist_id": identifier,
        "title": "Playlist",
        "url": ORIGIN + "/playlist?list=" + identifier,
        "description": "",
        "thumbnail_url": "",
        "channel_id": "",
        "channel_name": "",
        "channel_url": "",
        "reported_count": None,
        "sort_order": _sort(value),
    }
    metadata.update({key: value for key, value in (fallback or {}).items()
                     if key in metadata or key == "payload"})
    title = playlist_title(value)

    if title:
        metadata["title"] = title

    for node in objects(value):
        raw = as_dict(node.get("playlistMetadataRenderer"))

        if raw:
            metadata["description"] = text(raw.get("description"))

        for key in (
            "playlistHeaderRenderer",
            "playlistSidebarPrimaryInfoRenderer",
            "playlistSidebarSecondaryInfoRenderer",
        ):
            header = as_dict(node.get(key))

            if header:
                _header_metadata(metadata, header)

    metadata["sort_order"] = _sort(value)

    return metadata


def _header_metadata(metadata: dict[str, Any], header: dict[str, Any]) -> None:
    title = text(header.get("title"))

    if title:
        metadata["title"] = title

    owner = header.get("ownerText") or header.get("videoOwner")
    channel = _channel(owner)

    if channel[0] or channel[1]:
        metadata.update(
            zip(
                ("channel_id", "channel_name", "channel_url"),
                channel,
                strict=True,
            ),
        )

    thumbnail = _thumbnail(
        header.get("playlistHeaderBanner") or header.get("thumbnailRenderer"),
    )

    if thumbnail:
        metadata["thumbnail_url"] = thumbnail

    counts = [header.get("numVideosText"), *header.get("stats", [])]
    reported = next(
        (number for value in counts if (number := _count(value)) is not None),
        None,
    )

    if reported is not None:
        metadata["reported_count"] = reported


def discover_playlists(value: object) -> tuple[dict[str, Any], ...]:
    found: dict[str, dict[str, Any]] = {}

    for node in objects(value):
        for key in (
            "playlistRenderer",
            "gridPlaylistRenderer",
            "richPlaylistRenderer",
            "lockupViewModel",
        ):
            renderer = as_dict(node.get(key))
            identifier = str(renderer.get("playlistId", ""))

            if key == "lockupViewModel":
                if "PLAYLIST" not in str(renderer.get("contentType", "")):
                    continue

                identifier = str(renderer.get("contentId", ""))

            if not PLAYLIST_ID.fullmatch(identifier):
                continue

            lockup = as_dict(
                as_dict(renderer.get("metadata")).get(
                    "lockupMetadataViewModel",
                ),
            )
            channel = _channel(renderer.get("shortBylineText"))
            found[identifier] = {
                "playlist_id": identifier,
                "url": ORIGIN + "/playlist?list=" + identifier,
                "title": text(renderer.get("title") or lockup.get("title"))
                or "Playlist",
                "thumbnail_url": _thumbnail(renderer),
                "channel_id": channel[0],
                "channel_name": channel[1],
                "channel_url": channel[2],
                "reported_count": next(
                    (
                        number
                        for item in objects(renderer)
                        if (number := _count(item.get("text"))) is not None
                    ),
                    None,
                ),
                "payload": json.dumps({key: renderer}),
            }

    return tuple(found.values())


def _page(contents: list[Any]) -> PlaylistPage:
    entries: list[dict[str, Any]] = []
    continuation = ""

    for item in contents:
        renderer = as_dict(as_dict(item).get("playlistVideoRenderer"))

        if renderer:
            entries.append(renderer)

        for node in objects(as_dict(item).get("continuationItemRenderer")):
            command = as_dict(node.get("continuationCommand"))

            if command.get("token"):
                continuation = str(command["token"])

    return PlaylistPage(tuple(entries), continuation, recognized=True)


def initial_playlist_page(value: object) -> PlaylistPage:
    for node in objects(value):
        listing = as_dict(node.get("playlistVideoListRenderer"))

        if "contents" in listing:
            return _page(listing["contents"])

    return PlaylistPage((), "", recognized=False)


def continuation_playlist_page(value: object) -> PlaylistPage:
    for node in objects(value):
        for key in (
            "appendContinuationItemsAction",
            "reloadContinuationItemsCommand",
        ):
            action = as_dict(node.get(key))
            contents = action.get("continuationItems")

            if isinstance(contents, list) and (
                "playlist" in str(action.get("targetId", "")).casefold()
                or any(
                    "playlistVideoRenderer" in as_dict(item)
                    for item in cast(list[object], contents)
                )
            ):
                return _page(cast(list[Any], contents))

        continuation = as_dict(node.get("playlistVideoListContinuation"))

        if "contents" in continuation:
            page = _page(continuation["contents"])
            token = next(
                (
                    str(item["nextContinuationData"]["continuation"])
                    for item in continuation.get("continuations", [])
                    if "nextContinuationData" in item
                ),
                "",
            )

            return page._replace(continuation=token)

    return PlaylistPage((), "", recognized=False)


def playlist_entry(renderer: dict[str, Any], position: int) -> dict[str, Any]:
    identifier = str(renderer.get("videoId", ""))
    title = text(renderer.get("title"))
    channel = _channel(renderer.get("shortBylineText"))
    duration = renderer.get("lengthSeconds")

    if duration is None:
        parts = text(renderer.get("lengthText")).split(":")
        duration = (
            sum(
                int(part) * 60**power
                for power, part in enumerate(reversed(parts))
            )
            if parts and all(part.isdecimal() for part in parts)
            else None
        )

    valid_id = bool(VIDEO_ID.fullmatch(identifier))
    available = (
        valid_id
        and renderer.get("isPlayable") is not False
        and title.casefold()
        not in {
            "[private video]",
            "[deleted video]",
            "private video",
            "deleted video",
            "unavailable video",
            "[unavailable video]",
        }
    )

    return {
        "position": position,
        "entry_id": str(renderer.get("setVideoId") or position),
        "video_id": identifier if valid_id else "",
        "title": title or "Unavailable video",
        "channel_id": channel[0],
        "channel_name": channel[1],
        "channel_url": channel[2],
        "thumbnail_url": _thumbnail(renderer),
        "duration_seconds": float(duration) if duration is not None else None,
        "available": available,
        "payload": json.dumps(renderer),
    }


def snapshot_hash(
    metadata: dict[str, Any], entries: Iterable[dict[str, Any]],
) -> str:
    """Hash source metadata and ordered occurrences, never fetch time."""
    stable_metadata = {
        key: value
        for key, value in metadata.items()
        if key not in {"payload", "thumbnail_url"}
    }
    stable_entries = [
        {
            key: value
            for key, value in entry.items()
            if key not in {"payload", "thumbnail_url"}
        }
        for entry in entries
    ]
    encoded = json.dumps(
        {"metadata": stable_metadata, "entries": stable_entries},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()

    return hashlib.sha256(encoded).hexdigest()


def playlist_capture(
    identifier: str,
    metadata: dict[str, Any],
    renderers: Iterable[dict[str, Any]],
    *,
    status: str = "complete",
    error_type: str = "",
) -> YouTubePlaylistCapture:
    entries = tuple(
        playlist_entry(renderer, position)
        for position, renderer in enumerate(renderers, 1)
    )

    if not metadata.get("thumbnail_url") and entries:
        metadata = {**metadata, "thumbnail_url": entries[0]["thumbnail_url"]}

    return YouTubePlaylistCapture(
        identifier,
        metadata,
        entries,
        status,
        error_type,
        datetime.datetime.now(datetime.UTC),
    )


def playlist_posts(capture: YouTubePlaylistCapture) -> list[RawPost]:
    """Expose flat playlist videos through the existing reader detail route."""
    posts: list[RawPost] = []

    for entry in capture.entries:
        if not entry["video_id"] or not entry["available"]:
            continue

        renderer = json.loads(entry["payload"])
        payload = {
            "schema_version": 1,
            "kind": "video",
            "video_id": entry["video_id"],
            "title": entry["title"],
            "channel_id": entry["channel_id"],
            "channel_name": entry["channel_name"],
            "channel_url": entry["channel_url"],
            "thumbnail_url": entry["thumbnail_url"],
            "duration_text": text(renderer.get("lengthText")),
            "detail_capture": {"status": "disabled"},
            "renderer": {"playlistVideoRenderer": renderer},
            "discovery": {
                "feed_url": capture.metadata["url"],
                "feed_name": capture.metadata["title"],
                "feed_kind": "playlist",
                "tab": "",
                "shelf": "",
                "rank": entry["position"],
                "position": entry["position"],
                "region": "",
                "playlist_id": capture.playlist_id,
                "observed_at": capture.checked_at.isoformat(),
            },
        }
        posts.append(
            RawPost(
                "youtube.com",
                "video:" + entry["video_id"],
                ORIGIN + "/watch?v=" + entry["video_id"],
                None,
                capture.checked_at,
                "youtube",
                capture.metadata["url"],
                json.dumps(payload),
            ),
        )

    return posts
