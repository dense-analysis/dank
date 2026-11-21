"""Read YouTube listing pages through yt-dlp and retain their renderers."""

from __future__ import annotations

import asyncio
import json
import sys
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AsyncExitStack, aclosing
from typing import Any, cast
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from dank.config import YouTubeSettings, youtube_feed_url
from dank.scrape.ytdlp_runtime import js_runtime_options

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
from .ytdlp_worker import (
    WorkerLogger,
    cookie_jar,
    emit,
    start_worker,
    stop_worker,
)

# Allow the first request and paginated browse calls, even if a feed stalls.
LISTING_TIMEOUT_SECONDS = 90.0


class ListingFailure(RuntimeError):
    pass


class ListingStopped(Exception):
    """The caller reached its item, overlap, or stall limit."""


async def _send(stream: asyncio.StreamWriter, value: object) -> None:
    stream.write((json.dumps(value) + "\n").encode())
    await stream.drain()


class ListingWorker:
    """Serve one listing at a time in a reusable subprocess."""

    def __init__(self) -> None:
        self._process: asyncio.subprocess.Process | None = None

    async def aclose(self) -> None:
        process, self._process = self._process, None

        if process is not None:
            await stop_worker(process)

    async def collect(
        self, request: dict[str, Any], timeout_seconds: float,
        consume: Callable[[dict[str, Any]], None],
    ) -> str:
        try:
            async with asyncio.timeout(timeout_seconds):
                process = self._process

                if process is None or process.returncode is not None:
                    await self.aclose()
                    process = self._process = await start_worker(__name__)

                result = await self._exchange(process, request, consume)
        except BaseException:
            await self.aclose()
            raise

        if result["event"] == "error":
            raise ListingFailure(str(result["error_type"]))

        return str(result["reason"])

    async def _exchange(
        self, process: asyncio.subprocess.Process, request: dict[str, Any],
        consume: Callable[[dict[str, Any]], None],
    ) -> dict[str, Any]:
        assert process.stdin is not None and process.stdout is not None
        await _send(process.stdin, request)
        stop: str | None = None

        while line := await process.stdout.readline():
            event = cast(dict[str, Any], json.loads(line))

            if event["event"] in {"done", "error"}:
                return event

            if stop is None:
                try:
                    consume(event)
                except ListingStopped as reached:
                    # Drain this page, then stop the listing at its boundary.
                    stop = str(reached)

            if event["event"] == "page":
                await _send(process.stdin, stop)

        raise ListingFailure("Listing worker exited before completing")


async def collect_listing(
    feed: YouTubeFeed, settings: YouTubeSettings,
    consume: Callable[[dict[str, Any]], None], *,
    cookies: Sequence[Mapping[str, object]] = (),
    timeout_seconds: float = LISTING_TIMEOUT_SECONDS,
    max_pages: int | None = None,
    worker: ListingWorker | None = None,
) -> str:
    def event_received(event: dict[str, Any]) -> None:
        if event.get("event") == "warning":
            if event["category"] in {
                "expired_session", "feed_redirected_to_home",
                "extractor_error",
            }:
                raise ListingFailure(str(event["category"]))
        else:
            consume(event)

    async with AsyncExitStack() as stack:
        if worker is None:
            worker = await stack.enter_async_context(aclosing(ListingWorker()))

        return await worker.collect({
            "feed": feed._asdict(),
            "region": feed.region or settings.region,
            "cookies": list(cookies),
            "max_pages": max_pages if max_pages is not None else (
                settings.scrape.max_scrolls + 1
            ),
            "pause": settings.scrape.scroll_pause_seconds,
        }, timeout_seconds, event_received)


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
    page_stop: Callable[[], str | None],
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

        if (reason := page_stop()) is not None:
            return reason

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


def _read_page_decision() -> str | None:
    return cast(str | None, json.loads(sys.stdin.readline()))


def _collect_request(request: dict[str, Any]) -> str:
    from yt_dlp import YoutubeDL  # type: ignore[import-untyped]

    options: dict[str, Any] = {
        "extract_flat": True, "skip_download": True, "cachedir": False,
        "quiet": True, "logger": WorkerLogger(), "socket_timeout": 15,
        "sleep_interval_requests": request["pause"], "retries": 0,
        "extractor_retries": 0,
        "js_runtimes": js_runtime_options(),
        "extractor_args": {"youtube": {"lang": ["en-GB"]}},
    }

    # A fresh downloader gives each feed its own cookies and request pacing.
    # https://github.com/yt-dlp/yt-dlp/blob/2026.10.26/yt_dlp/extractor/common.py#L871
    with YoutubeDL(cast(Any, options)) as provider:
        downloader: Any = provider
        downloader.cookiejar = cookie_jar(request["cookies"])
        extractor = downloader.get_info_extractor("YoutubeTab")
        extractor.initialize()

        return _read_pages(
            extractor, YouTubeFeed(**request["feed"]),
            request["region"], request["max_pages"], _read_page_decision,
        )


def _worker() -> None:
    while line := sys.stdin.readline():
        try:
            reason = _collect_request(json.loads(line))
            emit({"event": "done", "reason": reason})
        except ListingFailure as error:
            emit({"event": "error", "error_type": str(error)})
        except Exception as error:
            emit({"event": "error", "error_type": type(error).__name__})


if __name__ == "__main__" and sys.argv[1:] == ["--worker"]:
    _worker()
