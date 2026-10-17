from __future__ import annotations

import asyncio
import datetime
import json
import logging
from collections import Counter
from collections.abc import AsyncIterator, Iterable, Mapping
from typing import Any, cast
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import zendriver

from dank.config import Settings, SourceConfig, YouTubeSettings
from dank.model import RawPost
from dank.scrape.metrics import count, current_source
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import BrowserSession, NetworkCapture

from .browser import EXPAND_SCRIPT, SCROLL_SCRIPT, navigate, snapshot
from .feeds import configured_feeds
from .payloads import (
    ORIGIN,
    UK_HYPE_PLAYLIST,
    YouTubeFeed,
    as_dict,
    discover_assets,
    hype_playlist,
    normalize_renderer,
    objects,
    playlist_title,
    renderers,
)

logger = logging.getLogger(__name__)


class YouTubeFeedError(RuntimeError):
    pass


def _localized(url: str, region: str) -> str:
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    query.update({"gl": [region], "hl": ["en-GB"]})

    return urlunsplit(parsed._replace(query=urlencode(query, doseq=True)))


class FeedCapture:
    """Deduplicate page/network echoes without losing distinct shelves."""

    def __init__(
        self, feed: YouTubeFeed, settings: YouTubeSettings,
        known_ids: set[str],
    ) -> None:
        self.feed = feed
        self.settings = settings
        self.known_ids = known_ids
        self.observed_at = datetime.datetime.now(datetime.UTC)
        self.seen: set[tuple[str, str]] = set()
        self.item_ids: set[str] = set()
        self.shelf_ranks: Counter[str] = Counter()
        self.overlap = 0
        self.reached_overlap = False
        self.playlist_name = ""
        self.channel: tuple[str, ...] = ("", "", "")

    def extract(
        self,
        candidates: Iterable[tuple[str, dict[str, Any], str]],
        request_url: str,
    ) -> ScrapeBatch:
        posts: list[RawPost] = []

        for key, renderer, shelf in candidates:
            post = normalize_renderer(
                key,
                renderer,
                feed=self.feed,
                shelf=shelf,
                rank=self.shelf_ranks[shelf] + 1,
                settings=self.settings,
                request_url=request_url,
                observed_at=self.observed_at,
                playlist_name=self.playlist_name,
                position=len(self.seen) + 1,
                channel=cast(tuple[str, str, str], self.channel),
            )

            if post is None or not self._accept(post, shelf):
                continue

            posts.append(post)
            self.shelf_ranks[shelf] += 1
            self._check_overlap(post.post_id)

            if self.reached_overlap:
                break

        return ScrapeBatch(
            posts,
            [
                asset
                for post in posts
                for asset in discover_assets(post, self.settings)
            ],
        )

    def _accept(self, post: RawPost, shelf: str) -> bool:
        identity = (post.post_id, shelf)

        if identity in self.seen or (
            not shelf and post.post_id in self.item_ids
        ):
            return False

        if (
            post.post_id not in self.item_ids
            and len(self.item_ids) >= self.settings.scrape.max_posts
        ):
            return False

        self.seen.add(identity)
        self.item_ids.add(post.post_id)

        return True

    def _check_overlap(self, item_id: str) -> None:
        if (self.feed.kind != "channel" or self.feed.tab != "videos"
                or urlsplit(self.feed.url).query):
            return

        self.overlap = self.overlap + 1 if item_id in self.known_ids else 0
        self.reached_overlap = self.overlap >= self.settings.known_overlap


def _candidates(
    state: dict[str, object], responses: list[object],
) -> Iterable[tuple[str, dict[str, Any], str]]:
    # Initial data includes collapsed shelf items in their original order.
    yield from renderers(state.get("initial"))

    for item in cast(list[object], state.get("items", [])):
        data = as_dict(item)
        yield from renderers(data.get("value"), str(data.get("shelf", "")))

    for response in responses:
        yield from renderers(response)


async def _resolve_feed(
    page: zendriver.Tab, feed: YouTubeFeed, settings: YouTubeSettings,
) -> tuple[str, dict[str, object]]:
    await navigate(page, _localized(feed.url, settings.region))
    state = await snapshot(page)

    if settings.authenticated and not state.get("signed_in"):
        raise YouTubeFeedError(
            "Saved YouTube session is not signed in; run youtube-login",
        )

    if feed.kind != "hype":
        return str(state.get("url") or feed.url), state

    playlist = hype_playlist(state.get("initial"))

    for _ in range(4):
        if playlist is not None:
            break

        await asyncio.sleep(0.5)
        state = await snapshot(page)
        playlist = hype_playlist(state.get("initial"))

    if playlist is None and settings.region == "GB":
        playlist = f"{ORIGIN}/playlist?list={UK_HYPE_PLAYLIST}"
        logger.warning(
            "Hype View all unavailable; trying the UK Top 100 fallback",
        )

    if playlist is None:
        raise YouTubeFeedError("No regional Hype Top 100 playlist found")

    # Follow View all; its playlist carries authoritative rank.
    await navigate(page, _localized(playlist, settings.region))

    return playlist, await snapshot(page)


async def _scrape_feed(
    page: zendriver.Tab,
    feed: YouTubeFeed,
    settings: YouTubeSettings,
    known_ids: set[str],
) -> AsyncIterator[ScrapeBatch]:
    capture = NetworkCapture(page, [r"youtube\.com/youtubei/v1/browse"])
    collected = FeedCapture(feed, settings, known_ids)
    idle = 0
    stop = "scroll limit"
    await capture.start()

    try:
        request_url, state = await _resolve_feed(page, feed, settings)
        collected.playlist_name = playlist_title(state.get("initial"))

        for node in objects(state.get("initial")):
            metadata = as_dict(node.get("channelMetadataRenderer"))

            if metadata:
                collected.channel = tuple(
                    str(metadata.get(key, ""))
                    for key in (
                        "externalId",
                        "title",
                        "channelUrl",
                    )
                )
        # Discard Hype landing responses before reading its playlist.
        await capture.drain(timeout_seconds=0.05)

        for step in range(settings.scrape.max_scrolls + 1):
            values: list[object] = []

            for response in await capture.drain(timeout_seconds=0.05):
                if response.status >= 400:
                    raise YouTubeFeedError(
                        f"YouTube listing returned HTTP {response.status}",
                    )

                try:
                    values.append(json.loads(response.body))
                except json.JSONDecodeError:
                    count("parse_failures")
                    logger.warning(
                        "YouTube browse response was not JSON for %s",
                        feed.url,
                    )

            batch = collected.extract(_candidates(state, values), request_url)

            if batch.posts:
                yield batch

            idle = (
                0 if batch.posts or not state.get("at_bottom", True)
                else idle + 1
            )
            logger.info(
                "YouTube %s scroll=%d items=%d discoveries=%d",
                feed.name,
                step,
                len(collected.item_ids),
                len(collected.seen),
            )
            stop = _stop_reason(collected, idle) or "scroll limit"

            if stop != "scroll limit" or step == settings.scrape.max_scrolls:
                break

            async with asyncio.timeout(15):
                await page.evaluate(EXPAND_SCRIPT)
                await page.evaluate(SCROLL_SCRIPT)
                await asyncio.sleep(settings.scrape.scroll_pause_seconds)

            state = await snapshot(page)

        if not collected.item_ids:
            raise YouTubeFeedError(
                "No usable feed items found; check access or renderer changes",
            )

        logger.info(
            "Finished YouTube %s items=%d reason=%s",
            feed.name,
            len(collected.item_ids),
            stop,
        )
    finally:
        await capture.stop()


def _stop_reason(capture: FeedCapture, idle: int) -> str | None:
    if capture.reached_overlap:
        return "known channel overlap"

    if len(capture.item_ids) >= capture.settings.scrape.max_posts:
        return "item limit"

    if idle >= capture.settings.stall_scrolls:
        return "repeated stall"

    return None


async def scrape_youtube_feeds(
    settings: Settings,
    source: SourceConfig,
    session: BrowserSession,
    *,
    known_ids: Mapping[str, set[str]] | None = None,
) -> AsyncIterator[ScrapeBatch]:
    feeds = configured_feeds(settings.youtube, source)
    stats = current_source.get()

    if stats is not None:
        stats.feed_urls = tuple(feed.url for feed in feeds)

    if not feeds:
        return

    async with asyncio.timeout(30):
        browser = await session.get_browser()
        page = await browser.get("about:blank", new_tab=True)

    try:
        for feed in feeds:
            try:
                async for batch in _scrape_feed(
                    page,
                    feed,
                    settings.youtube,
                    (known_ids or {}).get(feed.url, set()),
                ):
                    yield batch
            except Exception as error:
                count("fetch_failures")

                if stats is not None:
                    stats.error_type = type(error).__name__

                logger.warning(
                    "YouTube %s failed (%s): %s; continuing with next feed",
                    feed.name,
                    type(error).__name__,
                    error,
                )
    finally:
        try:
            async with asyncio.timeout(5):
                await page.close()
        except Exception as error:
            logger.warning(
                "Could not close YouTube tab (%s)", type(error).__name__,
            )
