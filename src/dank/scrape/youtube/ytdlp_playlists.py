"""Ordered playlist manifests using yt-dlp's HTTP requests and pagination."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections.abc import Callable
from typing import Any, cast

import zendriver

from dank.config import YouTubeSettings
from dank.model import YouTubePlaylistCapture

from .payloads import as_dict, objects
from .playlists import (
    continuation_playlist_page,
    initial_playlist_page,
    playlist_capture,
    playlist_id,
    playlist_metadata,
)
from .ytdlp_renderers import RendererLimitReached
from .ytdlp_worker import (
    WorkerLogger,
    cookie_jar,
    emit,
    has_youtube_session,
    run_worker,
    session_cookies,
)

logger = logging.getLogger(__name__)
PLAYLIST_TIMEOUT_SECONDS = 120.0
PLAYLIST_ENTRY_LIMIT = 10000


class PlaylistResponseCapture:
    """Observe only the requested playlist's selected continuation chain."""

    def __init__(
        self,
        identifier: str,
        output: Callable[[dict[str, Any]], None],
        max_entries: int,
    ) -> None:
        self.identifier = identifier
        self.output = output
        self.max_entries = max_entries
        self.count = 0
        self.started = False
        self.token = ""
        self.tokens: set[str] = set()

    def initial(self, data: object, *, listing: object | None = None) -> None:
        if self.started:
            raise ValueError("Unexpected additional playlist")

        self.started = True
        source = data if listing is None else listing
        metadata = playlist_metadata(self.identifier, source)
        metadata["payload"] = json.dumps(
            {
                "collection_method": "yt_dlp",
                "initial": data,
            },
        )
        self.output({"event": "metadata", "raw": metadata})
        self.page(source, initial=True)

    def page(self, data: object, *, initial: bool = False) -> None:
        result = (
            initial_playlist_page(data, identifier=self.identifier)
            if initial
            else continuation_playlist_page(data, identifier=self.identifier)
        )

        if not result.recognized:
            raise ValueError("Unrecognized playlist listing")

        if result.continuation and result.continuation in self.tokens:
            raise ValueError("Repeated playlist continuation")

        self.token = result.continuation
        self.tokens.add(self.token)

        for raw in result.entries:
            if self.count >= self.max_entries:
                raise RendererLimitReached

            self.output({"event": "entry", "raw": raw})
            self.count += 1


def _selected_playlist_tab(
    data: object,
    tabs: list[dict[str, Any]],
    identifier: str,
) -> dict[str, Any]:
    selected = next((tab for tab in tabs if tab.get("selected")), None)

    if selected is None:
        raise ValueError("Missing selected playlist tab")

    for node in objects(selected.get("content")):
        for key in ("playlistVideoListRenderer", "richGridRenderer"):
            listing = as_dict(node.get(key))

            if "contents" not in listing:
                continue

            if (
                key == "playlistVideoListRenderer"
                and listing.get(
                    "playlistId",
                    identifier,
                )
                != identifier
            ):
                continue

            # Exclude the suggested-video section and its continuation. yt-dlp
            # otherwise traverses it even after the actual playlist ends.
            content = {
                "sectionListRenderer": {
                    "contents": [
                        {
                            "itemSectionRenderer": {
                                "contents": [{key: listing}],
                            },
                        },
                    ],
                },
            }
            candidate = {**selected, "content": content}
            scoped = {
                "header": as_dict(data).get("header"),
                "contents": {
                    "twoColumnBrowseResultsRenderer": {
                        "tabs": [
                            {
                                "tabRenderer": candidate,
                            },
                        ],
                    },
                },
            }

            if initial_playlist_page(scoped, identifier=identifier).recognized:
                return candidate

    raise ValueError("Missing scoped playlist listing")


def install_playlist_capture(
    provider: Any,
    capture: PlaylistResponseCapture,
) -> None:
    # yt-dlp owns HTTP and pagination; retain source order and occurrence IDs
    # before flat normalization drops them. The selected-tab hook is pinned.
    # https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/extractor/youtube/_tab.py
    from yt_dlp.extractor.youtube import YoutubeTabIE  # type: ignore

    class PlaylistTabIE(YoutubeTabIE):
        @classmethod
        def ie_key(cls) -> str:
            return "YoutubeTab"

        def _extract_from_tabs(
            self,
            item_id: str,
            ytcfg: Any,
            data: Any,
            tabs: Any,
        ) -> Any:
            if item_id != capture.identifier:
                raise ValueError("Unexpected playlist identity")

            scoped = _selected_playlist_tab(data, tabs, capture.identifier)
            listing = {
                **as_dict(data),
                "contents": {"twoColumnBrowseResultsRenderer": {"tabs": [
                    {"tabRenderer": scoped},
                ]}},
            }
            capture.initial(data, listing=listing)
            base: Any = super()

            return base._extract_from_tabs(item_id, ytcfg, data, [scoped])

        def _extract_response(self, *args: Any, **kwargs: Any) -> Any:
            query = kwargs.get("query", args[1] if len(args) > 1 else {})
            token = as_dict(query).get("continuation")

            if token:
                if token != capture.token:
                    raise ValueError("Unknown playlist continuation")

                if capture.count >= capture.max_entries:
                    raise RendererLimitReached

            base: Any = super()
            response: Any = base._extract_response(*args, **kwargs)

            if token:
                capture.page(response)

            return response

    provider.add_info_extractor(PlaylistTabIE(provider))


class _PlaylistCapture:
    def __init__(
        self,
        identifier: str,
        fallback: dict[str, Any] | None,
    ) -> None:
        self.identifier = identifier
        self.metadata = playlist_metadata(identifier, {}, fallback=fallback)
        self.entries: list[dict[str, Any]] = []
        self.outcome = "failed"
        self.error_type = "WorkerExit"
        self.warnings: list[str] = []
        self.has_metadata = False

    def consume(self, event: dict[str, Any]) -> None:
        match event.get("event"):
            case "metadata":
                raw = as_dict(event.get("raw"))

                if raw.get("playlist_id") != self.identifier:
                    raise ValueError("Invalid playlist metadata")

                self.metadata = raw
                self.has_metadata = True
            case "entry":
                raw = as_dict(event.get("raw"))

                if not raw or len(self.entries) >= PLAYLIST_ENTRY_LIMIT:
                    raise ValueError("Invalid playlist entry")

                self.entries.append(raw)
            case "done":
                outcome = str(event["outcome"])

                if outcome not in {"completed", "incomplete", "item_limit"}:
                    raise ValueError("Invalid playlist outcome")

                self.outcome = outcome
                self.error_type = "" if outcome == "completed" else outcome
            case "warning":
                self.warnings.append(str(event["category"]))
            case "error":
                self.outcome = "failed"
                self.error_type = str(event["error_type"])
            case _:
                raise ValueError("Invalid playlist event")

    def result(self) -> YouTubePlaylistCapture:
        critical = set(self.warnings).intersection(
            {
                "expired_session",
                "repeated_continuation",
                "extractor_error",
                "feed_redirected_to_home",
            },
        )
        exhausted = (
            self.outcome == "completed" and self.has_metadata and not critical
        )
        status = (
            "complete"
            if exhausted
            else "partial"
            if self.entries
            else "failed"
        )
        error_type = self.error_type
        reported = self.metadata.get("reported_count")

        if exhausted and reported is not None and reported > len(self.entries):
            status = "partial"
            error_type = (
                f"Playlist listing exhausted; {reported - len(self.entries)} "
                "entries not shown by YouTube"
            )
        elif critical:
            error_type = error_type or ", ".join(sorted(critical))
        elif not self.has_metadata:
            error_type = error_type or "MissingPlaylistMetadata"

        return playlist_capture(
            self.identifier,
            self.metadata,
            self.entries,
            status=status,
            error_type=error_type,
            exhausted=exhausted,
        )


async def collect_playlist(
    page: zendriver.Tab,
    url: str,
    settings: YouTubeSettings,
    *,
    fallback: dict[str, Any] | None = None,
    timeout_seconds: float = PLAYLIST_TIMEOUT_SECONDS,
) -> YouTubePlaylistCapture:
    identifier = playlist_id(url)

    if not 0 < timeout_seconds <= PLAYLIST_TIMEOUT_SECONDS:
        raise ValueError("Playlist timeout must be within 120 seconds")

    capture = _PlaylistCapture(identifier, fallback)
    cookies: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    started = loop.time()

    try:
        if settings.authenticated:
            cookies = await session_cookies(page)

            if not has_youtube_session(cookies):
                raise ValueError(
                    "Verified YouTube session cookies unavailable",
                )

        remaining = max(0.001, timeout_seconds - (loop.time() - started))
        returncode = await run_worker(
            __name__,
            {
                "url": url,
                "cookies": cookies,
                "max_entries": PLAYLIST_ENTRY_LIMIT,
                "region": settings.region,
            },
            remaining,
            capture.consume,
        )

        if returncode:
            capture.outcome, capture.error_type = "failed", "WorkerExit"
    except Exception as error:
        capture.outcome, capture.error_type = "failed", type(error).__name__
    finally:
        cookies.clear()

    result = capture.result()
    logger.info(
        "YouTube playlist %s via yt-dlp: %s (%d entries) in %.1fs",
        identifier,
        result.status,
        len(result.entries),
        loop.time() - started,
    )

    return result


def _worker() -> None:
    from yt_dlp import YoutubeDL  # type: ignore[import-untyped]

    try:
        request: dict[str, Any] = json.load(sys.stdin)
        identifier = playlist_id(request["url"])
        capture = PlaylistResponseCapture(
            identifier,
            emit,
            request["max_entries"],
        )
        options: dict[str, Any] = {
            "extract_flat": True,
            "lazy_playlist": True,
            "skip_download": True,
            "cachedir": False,
            "quiet": True,
            "logger": WorkerLogger(),
            "socket_timeout": 15,
            "retries": 0,
            "extractor_retries": 0,
            "extractor_args": {"youtube": {"lang": ["en-GB"]}},
            # Observe the user's selected playlist order. An additional reload
            # to show unavailable videos is a different manifest request.
            "compat_opts": ["no-youtube-unavailable-videos"],
        }

        with YoutubeDL(cast(Any, options)) as provider:
            downloader: Any = provider
            downloader.cookiejar = cookie_jar(request["cookies"])
            install_playlist_capture(downloader, capture)
            url = (
                f"https://www.youtube.com/playlist?list={identifier}"
                f"&gl={request['region']}&hl=en-GB"
            )
            result = downloader.extract_info(
                url,
                download=False,
                process=False,
            )

            if (
                result.get("_type") != "playlist"
                or result.get("id") != identifier
            ):
                raise ValueError("Unexpected playlist result")

            for _ in result.get("entries", ()):
                pass

            emit(
                {
                    "event": "done",
                    "outcome": (
                        "completed"
                        if capture.started and not capture.token
                        else "incomplete"
                    ),
                },
            )
    except RendererLimitReached:
        emit({"event": "done", "outcome": "item_limit"})
    except Exception as error:
        emit({"event": "error", "error_type": type(error).__name__})


if __name__ == "__main__" and sys.argv[1:] == ["--worker"]:
    _worker()
