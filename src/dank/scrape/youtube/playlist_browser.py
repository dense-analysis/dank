"""Read playlist cards and ordered continuations in the saved session."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Mapping
from typing import Any

import zendriver

from dank.config import YouTubeSettings
from dank.model import YouTubePlaylistCapture
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import NetworkCapture

from .browser import HOME_SCROLL_SCRIPT, navigate, snapshot
from .playlists import (
    LIBRARY_URL,
    continuation_playlist_page,
    discover_playlists,
    initial_playlist_page,
    playlist_capture,
    playlist_id,
    playlist_metadata,
)

logger = logging.getLogger(__name__)
# Listing-only budgets keep a large saved library from blocking other sources.
PLAYLIST_TIMEOUT_SECONDS = 120.0
PLAYLIST_ENTRY_LIMIT = 10000
LIBRARY_TIMEOUT_SECONDS = 180.0
LIBRARY_CHECK_LIMIT = 8
LIBRARY_CARD_LIMIT = 1000
CONTINUATION_STALL_SECONDS = 12.0


class PlaylistIncomplete(RuntimeError):
    pass


async def _collect_pages(
    page: zendriver.Tab,
    capture: NetworkCapture,
    entries: list[dict[str, Any]],
    token: str,
) -> None:
    seen: set[str] = {token} if token else set()
    idle_since = asyncio.get_running_loop().time()

    while token:
        if len(entries) >= PLAYLIST_ENTRY_LIMIT:
            raise PlaylistIncomplete("Playlist entry limit")

        await page.evaluate(HOME_SCROLL_SCRIPT)
        await asyncio.sleep(0.25)
        values = await capture.drain(timeout_seconds=0.25)

        for response in values:
            if response.status >= 400:
                raise PlaylistIncomplete("Playlist continuation HTTP failure")

            result = continuation_playlist_page(json.loads(response.body))

            if not result.recognized:
                continue

            if result.continuation and result.continuation in seen:
                raise PlaylistIncomplete("Repeated playlist continuation")

            entries.extend(result.entries)
            token = result.continuation
            seen.add(token)
            idle_since = asyncio.get_running_loop().time()

            if not token:
                break

        if (
            asyncio.get_running_loop().time() - idle_since
            >= CONTINUATION_STALL_SECONDS
        ):
            raise PlaylistIncomplete("Playlist continuation stalled")

    if len(entries) > PLAYLIST_ENTRY_LIMIT:
        del entries[PLAYLIST_ENTRY_LIMIT:]
        raise PlaylistIncomplete("Playlist entry limit")


async def collect_playlist(
    page: zendriver.Tab,
    url: str,
    settings: YouTubeSettings,
    *,
    fallback: dict[str, Any] | None = None,
    timeout_seconds: float = PLAYLIST_TIMEOUT_SECONDS,
) -> YouTubePlaylistCapture:
    identifier = playlist_id(url)
    metadata = playlist_metadata(identifier, {}, fallback=fallback)
    entries: list[dict[str, Any]] = []
    capture = NetworkCapture(page, [r"youtube\.com/youtubei/v1/browse"])
    status, error_type = "complete", ""

    try:
        async with asyncio.timeout(timeout_seconds):
            await capture.start()
            await navigate(page, url)
            state = await snapshot(page)

            if settings.authenticated and not state.get("signed_in"):
                raise PlaylistIncomplete(
                    "Saved YouTube session is not signed in",
                )

            initial = state.get("initial")
            metadata = playlist_metadata(
                identifier, initial, fallback=fallback,
            )
            metadata["payload"] = json.dumps(initial)
            result = initial_playlist_page(initial)

            if not result.recognized:
                raise PlaylistIncomplete("Playlist renderer is unavailable")

            entries.extend(result.entries)
            await _collect_pages(page, capture, entries, result.continuation)
            reported = metadata.get("reported_count")

            if reported is not None and reported > len(entries):
                raise PlaylistIncomplete(
                    "Playlist contains hidden or missing entries",
                )
    except Exception as error:
        status = "partial" if entries else "failed"
        error_type = (
            str(error)
            if isinstance(error, PlaylistIncomplete)
            else type(error).__name__
        )
        logger.warning("YouTube playlist %s: %s", identifier, error_type)
    finally:
        await capture.stop()

    logger.info(
        "YouTube playlist %s: %s (%d entries)",
        identifier,
        status,
        len(entries),
    )

    return playlist_capture(
        identifier, metadata, entries, status=status, error_type=error_type,
    )


async def _library_cards(
    page: zendriver.Tab, found: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], ...]:
    await navigate(page, LIBRARY_URL)
    idle = 0

    for _ in range(100):
        state = await snapshot(page)

        if not state.get("signed_in"):
            raise PlaylistIncomplete("Saved YouTube session is not signed in")

        previous_count = len(found)

        for metadata in discover_playlists(
            [state.get("initial"), state.get("items")],
        ):
            found[metadata["playlist_id"]] = metadata

        settled = (
            len(found) == previous_count
            and state.get("at_bottom")
            and not state.get("loading")
            and not state.get("continuation")
        )
        idle = idle + 1 if settled else 0

        if idle >= 2:
            return tuple(found.values())

        if len(found) >= LIBRARY_CARD_LIMIT:
            raise PlaylistIncomplete("Playlist library card limit")

        await page.evaluate(HOME_SCROLL_SCRIPT)
        await asyncio.sleep(0.5)

    raise PlaylistIncomplete("Playlist library did not reach its end")


def _check_order(
    metadata: dict[str, Any],
    known: Mapping[str, dict[str, Any]],
) -> tuple[bool, str]:
    previous = known.get(metadata["playlist_id"], {})

    return bool(previous.get("snapshot_hash")), str(
        previous.get("checked_at", ""),
    )


async def collect_playlist_library(
    page: zendriver.Tab,
    settings: YouTubeSettings,
    known: Mapping[str, dict[str, Any]],
    *,
    excluded: set[str],
) -> AsyncIterator[ScrapeBatch]:
    deadline = asyncio.get_running_loop().time() + LIBRARY_TIMEOUT_SECONDS

    failure: Exception | None = None

    if settings.authenticated:
        found: dict[str, dict[str, Any]] = {}

        try:
            async with asyncio.timeout(min(60, LIBRARY_TIMEOUT_SECONDS)):
                cards = await _library_cards(page, found)
        except Exception as error:
            cards = tuple(found.values())
            failure = error
    else:
        cards = tuple(known.values())

    # Persist the complete catalog before bounded per-playlist work begins.
    yield ScrapeBatch([], [], youtube_playlists=tuple(
        playlist_capture(card["playlist_id"], playlist_metadata(
            card["playlist_id"], {}, fallback=card,
        ), (), status="not_collected") for card in cards
    ))
    if failure is not None:
        raise failure

    targets = sorted(
        (card for card in cards if card["playlist_id"] not in excluded),
        key=lambda card: _check_order(card, known),
    )

    for card in targets[:LIBRARY_CHECK_LIMIT]:
        remaining = deadline - asyncio.get_running_loop().time()

        if remaining < 3:
            break

        result = await collect_playlist(
            page, card["url"], settings, fallback=card,
            timeout_seconds=min(remaining, PLAYLIST_TIMEOUT_SECONDS),
        )
        yield ScrapeBatch([], [], youtube_playlists=(result,))
