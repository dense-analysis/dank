"""Read YouTube listing pages through yt-dlp and retain their renderers."""

from __future__ import annotations

import json
import sys
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, cast
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from dank.config import YouTubeSettings, youtube_feed_url

from .payloads import (
    ORIGIN,
    POST_RENDERERS,
    SHELF_RENDERERS,
    UK_HYPE_PLAYLIST,
    VIDEO_RENDERERS,
    YouTubeFeed,
    as_dict,
    hype_playlist,
    objects,
    playlist_title,
    renderers,
    text,
)
from .playlists import LIBRARY_URL, discover_playlists, playlist_id
from .ytdlp_playlists import select_playlist_tab
from .ytdlp_worker import WorkerLogger, cookie_jar, emit, run_worker

# Allow the first request and paginated browse calls, even if a feed stalls.
LISTING_TIMEOUT_SECONDS = 90.0


class ListingFailure(RuntimeError):
    pass


class ListingStopped(Exception):
    """The caller reached its item, overlap, or stall limit."""


async def collect_listing(
    feed: YouTubeFeed, settings: YouTubeSettings,
    consume: Callable[[dict[str, Any]], None], *,
    cookies: Sequence[Mapping[str, object]] = (),
    timeout_seconds: float = LISTING_TIMEOUT_SECONDS,
    max_pages: int | None = None,
) -> str:
    stop: str | None = None

    def event_received(event: dict[str, Any]) -> None:
        nonlocal stop

        match event.get("event"):
            case "done":
                stop = str(event["reason"])
            case "error":
                raise ListingFailure(str(event["error_type"]))
            case "warning":
                if event["category"] in {
                    "expired_session", "feed_redirected_to_home",
                    "extractor_error",
                }:
                    raise ListingFailure(str(event["category"]))
            case _:
                consume(event)

    try:
        returncode = await run_worker(__name__, {
            "feed": feed._asdict(),
            "region": feed.region or settings.region,
            "cookies": list(cookies),
            "max_pages": max_pages if max_pages is not None else (
                settings.scrape.max_scrolls + 1
            ),
            "pause": settings.scrape.scroll_pause_seconds,
        }, timeout_seconds, event_received)
    except ListingStopped as reached:
        return str(reached)

    if returncode or stop is None:
        raise ListingFailure("Listing worker exited before completing")

    return stop


def localized(url: str, region: str) -> str:
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    query.update({"gl": [region], "hl": ["en-GB"]})

    return urlunsplit(parsed._replace(query=urlencode(query, doseq=True)))


def _initial_content(
    extractor: Any, feed: YouTubeFeed, data: dict[str, Any], url: str,
) -> object:
    tabs: list[dict[str, Any]] = extractor._extract_tab_renderers(data)
    selected = next((tab for tab in tabs if tab.get("selected")), None)

    if feed.kind == "hype":
        return _hype_content(data, tabs, as_dict(selected), playlist_id(url))

    if feed.kind == "channel":
        tab, _ = extractor._extract_tab_id_and_name(as_dict(selected))

        if tab != feed.tab and {tab, feed.tab} != {"posts", "community"}:
            raise ListingFailure("Requested channel tab is unavailable")

    if selected:
        return selected.get("content")

    return data.get("contents")


def _hype_content(
    data: dict[str, Any], tabs: list[dict[str, Any]],
    selected: dict[str, Any], identifier: str,
) -> object:
    header = as_dict(as_dict(data.get("header")).get("playlistHeaderRenderer"))

    if header.get("playlistId") == identifier:
        # Hype also serves a ranked linear layout of video lockups.
        layouts = [
            {"linearLayoutViewModel": layout}
            for node in objects(selected.get("content"))
            if (layout := as_dict(node.get("linearLayoutViewModel")))
            and any(renderers(layout))
        ]

        if layouts:
            return layouts

    return select_playlist_tab(data, tabs, identifier)["content"]


def _continuations(
    extractor: Any, value: object, shelf: str,
) -> Iterator[tuple[dict[str, Any], str]]:
    if isinstance(value, list):
        for child in cast(list[object], value):
            yield from _continuations(extractor, child, shelf)

        return

    data = as_dict(value)

    if not data:
        return

    query = as_dict(extractor._extract_continuation(data))

    if query.get("continuation"):
        yield query, shelf

    for key, child in data.items():
        if key in VIDEO_RENDERERS | POST_RENDERERS | {
            "adSlotRenderer", "inFeedAdLayoutRenderer",
        }:
            continue

        heading = (
            text(as_dict(child).get("title")) if key in SHELF_RENDERERS else ""
        )
        yield from _continuations(extractor, child, heading or shelf)


def _continuation_content(data: dict[str, Any]) -> object:
    # Browse continuations can append or replace a list, including Shorts.
    actions = [
        action
        for key in ("onResponseReceivedActions", "onResponseReceivedEndpoints")
        for action in data.get(key, [])
    ]
    if actions:
        return [
            {"contents": value["continuationItems"]}
            for action in actions for value in as_dict(action).values()
            if "continuationItems" in as_dict(value)
        ]

    return data.get("continuationContents")


def _open_listing(
    extractor: Any, feed: YouTubeFeed, region: str,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    url = LIBRARY_URL if feed.kind == "library" else youtube_feed_url(feed.url)
    data, config = extractor._extract_data(localized(url, region), feed.name)

    if feed.kind == "hype" and urlsplit(url).path != "/playlist":
        playlist = hype_playlist(data)

        if playlist is None and region == "GB":
            playlist = f"{ORIGIN}/playlist?list={UK_HYPE_PLAYLIST}"
            emit({"event": "hype_fallback"})

        if playlist is None:
            raise ListingFailure("No regional Hype Top 100 playlist found")

        url = youtube_feed_url(playlist)
        data, config = extractor._extract_data(
            localized(url, region), feed.name,
        )

    return url, as_dict(data), as_dict(config)


def _emit_page(feed: YouTubeFeed, content: object, shelf: str) -> None:
    if feed.kind == "library":
        for card in discover_playlists(content):
            emit({"event": "playlist", "raw": card})
    else:
        for key, raw, heading in renderers(content, shelf):
            emit({"event": "renderer", "key": key, "raw": raw,
                  "shelf": heading})

    emit({"event": "page"})


def _queue_continuations(
    extractor: Any, content: object, shelf: str,
    pending: deque[tuple[dict[str, Any], str]], seen: set[str],
) -> None:
    repeated = False

    for query, heading in _continuations(extractor, content, shelf):
        token = str(query["continuation"])

        if token in seen:
            repeated = True
        else:
            seen.add(token)
            pending.append((query, heading))

    if repeated and not pending:
        raise ListingFailure("Repeated listing continuation")


def _read_pages(
    extractor: Any, feed: YouTubeFeed, region: str, max_pages: int,
) -> str:
    url, data, config = _open_listing(extractor, feed, region)

    if feed.kind == "library" and config.get("LOGGED_IN") is not True:
        raise ListingFailure("Saved YouTube session is not signed in")

    content = _initial_content(extractor, feed, data, url)

    if content is None:
        raise ListingFailure("Missing listing content")

    # Keep channel evidence and individual renderers below the worker line cap.
    emit({"event": "listing", "url": localized(url, region),
          "title": playlist_title(data),
          "raw": {key: data[key] for key in ("metadata", "header")
                  if key in data}})
    session_id = extractor._extract_delegated_session_id(config, data)
    visitor = extractor._extract_visitor_data(data, config)
    pending: deque[tuple[dict[str, Any], str]] = deque()
    seen: set[str] = set()
    shelf = ""

    for page in range(1, max_pages + 1):
        _emit_page(feed, content, shelf)
        _queue_continuations(extractor, content, shelf, pending, seen)

        if not pending:
            return "listing exhausted"

        if page == max_pages:
            break

        query, shelf = pending.popleft()
        headers = extractor.generate_api_headers(
            ytcfg=config, delegated_session_id=session_id,
            visitor_data=visitor,
        )
        response = as_dict(extractor._extract_response(
            item_id=f"{feed.name} page {page + 1}", query=query,
            headers=headers, ytcfg=config, check_get_keys=(
                "continuationContents", "onResponseReceivedActions",
                "onResponseReceivedEndpoints",
            ),
        ))
        visitor = extractor._extract_visitor_data(response) or visitor
        content = _continuation_content(response)

        if content is None:
            raise ListingFailure("Missing browse continuation")

    return "page limit"


def _worker() -> None:
    from yt_dlp import YoutubeDL  # type: ignore[import-untyped]

    try:
        request: dict[str, Any] = json.load(sys.stdin)
        options: dict[str, Any] = {
            "extract_flat": True, "skip_download": True, "cachedir": False,
            "quiet": True, "logger": WorkerLogger(), "socket_timeout": 15,
            "sleep_interval_requests": request["pause"], "retries": 0,
            "extractor_retries": 0,
            "extractor_args": {"youtube": {"lang": ["en-GB"]}},
        }

        # Use the pinned tab extractor's HTTP and browse response handling.
        # https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/extractor/youtube/_tab.py
        with YoutubeDL(cast(Any, options)) as provider:
            downloader: Any = provider
            downloader.cookiejar = cookie_jar(request["cookies"])
            extractor = downloader.get_info_extractor("YoutubeTab")
            extractor.initialize()
            reason = _read_pages(
                extractor, YouTubeFeed(**request["feed"]),
                request["region"], request["max_pages"],
            )
            emit({"event": "done", "reason": reason})
    except ListingFailure as error:
        emit({"event": "error", "error_type": str(error)})
    except Exception as error:
        emit({"event": "error", "error_type": type(error).__name__})


if __name__ == "__main__" and sys.argv[1:] == ["--worker"]:
    _worker()
