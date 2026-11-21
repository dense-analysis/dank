"""Discover saved playlist cards and revisit bounded sets of playlists."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

from dank.config import YouTubeSettings
from dank.scrape.types import ScrapeBatch

from .payloads import YouTubeFeed
from .playlists import LIBRARY_URL, playlist_capture, playlist_metadata
from .ytdlp_listings import ListingFailure, ListingStopped, collect_listing
from .ytdlp_playlists import PLAYLIST_TIMEOUT_SECONDS, collect_playlist

LIBRARY_TIMEOUT_SECONDS = 180.0
LIBRARY_CHECK_LIMIT = 8
LIBRARY_CARD_LIMIT = 1000
LIBRARY_PAGE_LIMIT = 100
CATALOG_TIMEOUT_SECONDS = 60.0


async def collect_playlist_library(
    settings: YouTubeSettings, known: Mapping[str, dict[str, Any]], *,
    cookies: Sequence[Mapping[str, object]], excluded: set[str],
) -> AsyncIterator[ScrapeBatch]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + LIBRARY_TIMEOUT_SECONDS
    failure: Exception | None = None

    if settings.authenticated:
        found: dict[str, dict[str, Any]] = {}

        def receive(event: dict[str, Any]) -> None:
            if event.get("event") == "playlist":
                card = event["raw"]
                identifier = str(card["playlist_id"])

                if (identifier not in found
                        and len(found) >= LIBRARY_CARD_LIMIT):
                    raise ListingStopped("playlist library card limit")

                found[identifier] = card

        try:
            reason = await collect_listing(
                YouTubeFeed(LIBRARY_URL, "Playlist library", "library"),
                settings, receive, cookies=cookies,
                timeout_seconds=CATALOG_TIMEOUT_SECONDS,
                max_pages=LIBRARY_PAGE_LIMIT,
            )

            if reason != "listing exhausted":
                failure = ListingFailure(reason)
        except Exception as error:
            failure = error

        cards = tuple(found.values())
    else:
        cards = tuple(known.values())

    # Persist captured cards before the slower individual playlist checks.
    yield ScrapeBatch(
        [], [], youtube_playlists=tuple(
            playlist_capture(card["playlist_id"], playlist_metadata(
                card["playlist_id"], {}, fallback=card,
            ), (), status="not_collected") for card in cards
        ),
        youtube_playlist_order=(
            tuple(card["playlist_id"] for card in cards)
            if settings.authenticated and failure is None else None
        ),
    )

    if failure is not None:
        raise failure

    targets = sorted(
        (card for card in cards if card["playlist_id"] not in excluded),
        key=lambda card: str(known.get(card["playlist_id"], {}).get(
            "checked_at", "",
        )),
    )

    for card in targets[:LIBRARY_CHECK_LIMIT]:
        remaining = deadline - loop.time()

        if remaining < 3:
            break

        result = await collect_playlist(
            card["url"], settings, cookies=cookies, fallback=card,
            timeout_seconds=min(remaining, PLAYLIST_TIMEOUT_SECONDS),
        )
        yield ScrapeBatch([], [], youtube_playlists=(result,))
