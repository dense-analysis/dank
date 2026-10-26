"""Isolated metadata-only recommendation capture through yt-dlp."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from collections.abc import Mapping, Sequence
from http.cookiejar import CookieJar
from typing import Any, Literal, NamedTuple, cast
from urllib.parse import parse_qs, urlsplit

from .payloads import POST_RENDERERS, VIDEO_RENDERERS
from .ytdlp_renderers import HomeRenderer, RendererLimitReached
from .ytdlp_worker import (
    WorkerLogger,
    cookie_jar as _cookie_jar,
    cookie_records as _cookie_records,
    emit as _emit,
    has_youtube_session as _youtube_session,
    run_worker,
)

MAX_TIMEOUT_SECONDS = 240.0
MAX_ENTRIES = 10000
LIMITATIONS = (
    "Flat entries omit shelf provenance; news shelves cannot be reliably "
    "excluded from this comparison capture.",
    "Posts may be omitted or represented only by their linked videos.",
    "Shorts classification depends on the flat entry retaining a Shorts URL.",
    "A finished extractor does not prove the browser homepage was exhausted.",
)


class YtDlpHomeEntry(NamedTuple):
    identity: str
    kind: Literal["video", "short", "post", "unknown"]
    position: int
    raw: dict[str, Any]


class YtDlpHomeResult(NamedTuple):
    entries: tuple[YtDlpHomeEntry, ...]
    elapsed_seconds: float
    first_entry_seconds: float | None
    outcome: Literal["completed", "timeout", "item_limit", "failed"]
    error_type: str = ""
    warnings: tuple[str, ...] = ()
    limitations: tuple[str, ...] = LIMITATIONS
    renderers: tuple[HomeRenderer, ...] = ()


def classify_entry(raw: dict[str, Any], position: int) -> YtDlpHomeEntry:
    """Keep each flat entry and classify its supported identity."""
    url = str(raw.get("url") or raw.get("webpage_url") or "")
    parsed = urlsplit(url)
    host = (parsed.hostname or "").removeprefix("www.")
    path = parsed.path
    identifier = str(raw.get("id") or "")

    if host == "youtube.com" and path.startswith("/post/"):
        identifier = path.removeprefix("/post/").split("/")[0]

        return YtDlpHomeEntry(f"post:{identifier}", "post", position, raw)

    if host == "youtube.com" and path.startswith("/shorts/"):
        identifier = path.removeprefix("/shorts/").split("/")[0]
        kind = "short"
    elif host == "youtube.com" and path == "/watch":
        identifier = parse_qs(parsed.query).get("v", [identifier])[0]
        kind = "video"
    elif host == "youtu.be":
        identifier = path.strip("/")
        kind = "video"
    elif raw.get("ie_key") == "Youtube" and not host:
        kind = "video"
    else:
        return YtDlpHomeEntry("", "unknown", position, raw)

    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", identifier):
        return YtDlpHomeEntry("", "unknown", position, raw)

    return YtDlpHomeEntry(
        f"video:{identifier}", kind,
        position, raw,
    )



def _renderer(event: dict[str, Any]) -> HomeRenderer:
    raw, key = event["raw"], event["key"]
    shelf, shelves = event["shelf"], event["shelves"]

    if (key not in VIDEO_RENDERERS | POST_RENDERERS
            or not isinstance(raw, dict)
            or not isinstance(shelf, str)
            or not isinstance(shelves, list)
            or not all(isinstance(value, str)
                       for value in cast(list[object], shelves))):
        raise ValueError("Invalid recommendation renderer")

    return HomeRenderer(
        key, cast(dict[str, Any], raw), shelf,
        tuple(cast(list[str], shelves)),
    )


class _Capture:
    def __init__(self, started: float) -> None:
        self.started = started
        self.entries: list[YtDlpHomeEntry] = []
        self.renderers: list[HomeRenderer] = []
        self.warnings: list[str] = []
        self.first_entry_seconds: float | None = None
        self.outcome: Literal[
            "completed", "timeout", "item_limit", "failed",
        ] = "failed"
        self.error_type = "WorkerExit"

    def consume(self, event: dict[str, Any], now: float) -> None:
        match event.get("event"):
            case "entry":
                raw = event["raw"]

                if not isinstance(raw, dict):
                    raise ValueError("Invalid flat entry")

                self.entries.append(classify_entry(
                    cast(dict[str, Any], raw), len(self.entries) + 1,
                ))
            case "renderer":
                if len(self.renderers) >= MAX_ENTRIES:
                    raise ValueError("Recommendation renderer limit exceeded")

                self.renderers.append(_renderer(event))
            case "warning":
                self.warnings.append(str(event["category"]))
            case "done":
                outcome = event["outcome"]

                if outcome not in {"completed", "item_limit"}:
                    raise ValueError("Invalid worker completion event")

                self.outcome = outcome
                self.error_type = ""
            case "error":
                self.outcome = "failed"
                self.error_type = str(event["error_type"])
            case _:
                raise ValueError("Unknown worker event")

        if (event.get("event") in {"entry", "renderer"}
                and self.first_entry_seconds is None):
            self.first_entry_seconds = now - self.started

    def result(self, now: float) -> YtDlpHomeResult:
        return YtDlpHomeResult(
            tuple(self.entries), now - self.started, self.first_entry_seconds,
            self.outcome, self.error_type, tuple(dict.fromkeys(self.warnings)),
            renderers=tuple(self.renderers),
        )


def _validate_limits(timeout_seconds: float, max_entries: int, region: str):
    if not 0 < timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise ValueError("Recommendation timeout must be within 240 seconds")

    if not 0 < max_entries <= MAX_ENTRIES:
        raise ValueError("Recommendation entry limit must be within 10000")

    if not re.fullmatch(r"[A-Z]{2}", region):
        raise ValueError("Recommendation region must be a two-letter code")


async def collect_ytdlp_home(
    cookies: Sequence[Mapping[str, object]] | CookieJar,
    *,
    timeout_seconds: float = MAX_TIMEOUT_SECONDS,
    max_entries: int = MAX_ENTRIES,
    region: str = "GB",
    include_renderers: bool = False,
) -> YtDlpHomeResult:
    """Use supplied cookies and kill the worker on timeout or cancellation."""
    _validate_limits(timeout_seconds, max_entries, region)
    records = _cookie_records(cookies)

    if not _youtube_session(records):
        raise ValueError("A verified YouTube session snapshot is required")

    loop = asyncio.get_running_loop()
    started = loop.time()
    capture = _Capture(started)
    try:
        returncode = await run_worker(__name__, {
            "cookies": records, "max_entries": max_entries,
            "region": region, "include_renderers": include_renderers,
        }, timeout_seconds, lambda event: capture.consume(event, loop.time()))

        if returncode != 0:
            capture.outcome = "failed"
            capture.error_type = capture.error_type or "WorkerExit"
    except TimeoutError:
        capture.outcome, capture.error_type = "timeout", "TimeoutError"
    except (ValueError, OSError, KeyError, TypeError) as error:
        capture.outcome, capture.error_type = "failed", type(error).__name__
    finally:
        records.clear()

    return capture.result(loop.time())


class _WorkerLogger(WorkerLogger):
    def __init__(self) -> None:
        super().__init__(_emit)


def _run_worker() -> None:
    from yt_dlp import YoutubeDL  # type: ignore[import-untyped]

    request = cast(dict[str, Any], json.load(sys.stdin))
    cookies = _cookie_jar(request["cookies"])
    options: dict[str, Any] = {
        "extract_flat": True, "lazy_playlist": True, "skip_download": True,
        "cachedir": False, "quiet": True, "no_warnings": False,
        "logger": _WorkerLogger(), "socket_timeout": 15,
        "retries": 0, "extractor_retries": 0,
        "extractor_args": {"youtube": {"lang": ["en-GB"]}},
        # Regional query parameters defeat yt-dlp's exact recommended-URL
        # exception. This worker intentionally follows only that home route.
        "compat_opts": ["no-youtube-channel-redirect"],
    }

    try:
        with YoutubeDL(cast(Any, options)) as provider:
            downloader = cast(Any, provider)
            # Inject only the caller's cookie snapshot; never load a profile.
            downloader.cookiejar = cookies

            if request.get("include_renderers"):
                from .ytdlp_renderers import install_home_capture

                install_home_capture(downloader, _emit, request["max_entries"])

            result = downloader.extract_info(":ytrec", download=False,
                                             process=False)

            # :ytrec is a redirect. Resolve that one listing, never entry URLs.
            if result.get("_type") == "url":
                url = str(result["url"])

                if url != "https://www.youtube.com/feed/recommended":
                    raise ValueError("Unexpected recommendation redirect")

                url += f"?gl={request['region']}&hl=en-GB"
                result = downloader.extract_info(url, download=False,
                                                 process=False)

            for position, entry in enumerate(result.get("entries", ()), 1):
                if not request.get("include_renderers"):
                    _emit({"event": "entry", "raw": entry})

                if position >= request["max_entries"]:
                    _emit({"event": "done", "outcome": "item_limit"})
                    return

            _emit({"event": "done", "outcome": "completed"})
    except RendererLimitReached:
        _emit({"event": "done", "outcome": "item_limit"})
    except Exception as error:
        _emit({"event": "error", "error_type": type(error).__name__})


def _worker() -> None:
    try:
        _run_worker()
    except Exception as error:
        # Input/initialization failures must not expose cookie-bearing data.
        _emit({"event": "error", "error_type": type(error).__name__})


if __name__ == "__main__" and sys.argv[1:] == ["--worker"]:
    _worker()
