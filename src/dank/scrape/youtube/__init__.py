from __future__ import annotations

import asyncio
import datetime
import json
import logging
import random
from collections import Counter
from collections.abc import AsyncIterator, Iterable, Mapping
from typing import Any, cast
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import zendriver

from dank.config import Settings, SourceConfig, YouTubeSettings
from dank.model import RawPost
from dank.scrape.metrics import count, current_source
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import (
    BrowserSession,
    NetworkCapture,
    NetworkResponse,
)
from dank.youtube_session import save_youtube_session_from_page

from .browser import (
    EXPAND_SCRIPT,
    HOME_SCROLL_SCRIPT,
    SCROLL_SCRIPT,
    navigate,
    snapshot,
)
from .channels import discover_channel
from .feeds import configured_feeds
from .home_merge import merge_home_posts
from .payloads import (
    ORIGIN,
    UK_HYPE_PLAYLIST,
    YouTubeFeed,
    as_dict,
    discover_assets,
    excluded_home_section,
    excluded_home_shelf,
    hype_playlist,
    normalize_renderer,
    playlist_title,
    renderers,
)
from .playlist_browser import collect_playlist_library
from .playlists import playlist_id
from .ytdlp_home import YtDlpHomeResult, collect_ytdlp_home
from .ytdlp_playlists import collect_playlist
from .ytdlp_worker import session_cookies

logger = logging.getLogger(__name__)
# Consent can trigger a second navigation before the channel header arrives.
CHANNEL_INITIAL_ATTEMPTS = 10
CHANNEL_INITIAL_PAUSE_SECONDS = 0.5
# Home discovery has its own short wall-clock budget, including navigation.
# Generous item/scroll guards prevent runaway work without inheriting the
# smaller limits intended for channel and news listings.
HOME_BUDGET_SECONDS = 30.0
HOME_ITEM_LIMIT = 2000
HOME_SCROLL_LIMIT = 200
HOME_SCROLL_PAUSE_SECONDS = (0.3, 0.65)
HOME_END_SECONDS = 2.0
HOME_STALL_SECONDS = 8.0


class YouTubeFeedError(RuntimeError):
    pass


class YouTubeFeedIncomplete(YouTubeFeedError):
    """A usable snapshot stopped before the end of its recommendation feed."""


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
        self.item_limit = settings.scrape.max_posts

        if feed.kind == "home":
            self.item_limit = HOME_ITEM_LIMIT
        elif feed.kind == "hype":
            self.item_limit = min(self.item_limit, 100)

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
            and len(self.item_ids) >= self.item_limit
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
    state: dict[str, object], responses: list[object], *, home: bool = False,
) -> Iterable[tuple[str, dict[str, Any], str]]:
    # Initial data includes collapsed shelf items in their original order.
    yield from renderers(state.get("initial"), exclude_home_sections=home)

    if home:
        # Network order includes cards not yet rendered in the DOM.
        for response in responses:
            yield from renderers(response, exclude_home_sections=True)

    for item in cast(list[object], state.get("items", [])):
        data = as_dict(item)

        if home and (
            data.get("playable")
            or any(excluded_home_section("shelfRenderer", as_dict(context))
                   for context in data.get("shelf_contexts", []))
            or any(excluded_home_shelf(str(heading))
                   for heading in data.get("shelves", []))
        ):
            continue

        yield from renderers(
            data.get("value"), str(data.get("shelf", "")),
            exclude_home_sections=home,
        )

    if not home:
        for response in responses:
            yield from renderers(response)


async def _resolve_feed(
    page: zendriver.Tab, feed: YouTubeFeed, settings: YouTubeSettings,
) -> tuple[str, dict[str, object]]:
    region = feed.region or settings.region
    await navigate(page, _localized(feed.url, region))
    state = await snapshot(page)

    if settings.authenticated and not state.get("signed_in"):
        raise YouTubeFeedError(
            "Saved YouTube session is not signed in; run youtube-login",
        )

    if feed.kind != "hype" or urlsplit(feed.url).path == "/playlist":
        return str(state.get("url") or feed.url), state

    playlist = hype_playlist(state.get("initial"))

    for _ in range(4):
        if playlist is not None:
            break

        await asyncio.sleep(0.5)
        state = await snapshot(page)
        playlist = hype_playlist(state.get("initial"))

    if playlist is None and region == "GB":
        playlist = f"{ORIGIN}/playlist?list={UK_HYPE_PLAYLIST}"
        logger.warning(
            "Hype View all unavailable; trying the UK Top 100 fallback",
        )

    if playlist is None:
        raise YouTubeFeedError("No regional Hype Top 100 playlist found")

    # Follow View all; its playlist carries authoritative rank.
    await navigate(page, _localized(playlist, region))

    return playlist, await snapshot(page)


async def _channel_state(
    page: zendriver.Tab, feed: YouTubeFeed, state: dict[str, object],
) -> dict[str, object]:
    if feed.kind == "channel":
        for _ in range(CHANNEL_INITIAL_ATTEMPTS):
            if state.get("initial"):
                break

            await asyncio.sleep(CHANNEL_INITIAL_PAUSE_SECONDS)
            state = await snapshot(page)

    return state


def _home_response_values(
    responses: list[NetworkResponse], feed_url: str,
) -> tuple[list[object], YouTubeFeedError | None]:
    values: list[object] = []
    failure: YouTubeFeedError | None = None

    for response in responses:
        if response.status >= 400:
            failure = YouTubeFeedError(
                f"YouTube listing returned HTTP {response.status}",
            )
            continue

        try:
            values.append(json.loads(response.body))
        except json.JSONDecodeError:
            count("parse_failures")
            logger.warning(
                "YouTube browse response was not JSON for %s", feed_url,
            )

    return values, failure


async def _home_initial_state(
    page: zendriver.Tab, feed: YouTubeFeed, settings: YouTubeSettings,
    initial_state: dict[str, object] | None,
) -> tuple[str, dict[str, object]]:
    if initial_state is not None:
        return str(initial_state.get("url") or feed.url), initial_state

    return await _resolve_feed(page, feed, settings)


async def collect_browser_home(
    page: zendriver.Tab,
    feed: YouTubeFeed,
    settings: YouTubeSettings,
    batches: list[ScrapeBatch],
    *, timeout_seconds: float | None = None,
    max_items: int | None = None, max_scrolls: int | None = None,
    initial_state: dict[str, object] | None = None,
) -> None:
    """Buffer discoveries so downstream work cannot consume the time budget."""
    capture = NetworkCapture(page, [r"youtube\.com/youtubei/v1/browse"])
    collected = FeedCapture(feed, settings, set())

    collected.item_limit = (
        collected.item_limit if max_items is None else max_items
    )

    scroll_limit = HOME_SCROLL_LIMIT if max_scrolls is None else max_scrolls
    loop = asyncio.get_running_loop()
    started = loop.time()
    idle_since: float | None = None
    previous_height: object = None
    idle = 0
    stop = "scroll limit"
    budget = asyncio.timeout(
        HOME_BUDGET_SECONDS if timeout_seconds is None else timeout_seconds,
    )

    try:
        async with budget:
            await capture.start()
            request_url, state = await _home_initial_state(
                page, feed, settings, initial_state,
            )

            for step in range(scroll_limit + 1):
                values, failure = _home_response_values(
                    await capture.drain(timeout_seconds=0.01), feed.url,
                )
                batch = collected.extract(
                    _candidates(state, values, home=True), request_url,
                )

                if batch.posts:
                    batches.append(batch)

                if failure is not None:
                    raise failure

                height = state.get("scroll_height")
                settled = (
                    not batch.posts and not values
                    and state.get("at_bottom", False)
                    and not state.get("loading", False)
                    and height == previous_height
                )
                previous_height = height
                idle = idle + 1 if settled else 0
                idle_since = (
                    (idle_since if idle_since is not None else loop.time())
                    if settled else None
                )
                logger.info(
                    "YouTube %s scroll=%d items=%d discoveries=%d "
                    "elapsed=%.1fs",
                    feed.name, step, len(collected.item_ids),
                    len(collected.seen), loop.time() - started,
                )

                if len(collected.item_ids) >= collected.item_limit:
                    stop = "item limit"
                    break

                idle_seconds = (
                    HOME_STALL_SECONDS if state.get("continuation", False)
                    else HOME_END_SECONDS
                )

                if (idle >= settings.stall_scrolls and idle_since is not None
                        and loop.time() - idle_since >= idle_seconds):
                    stop = (
                        "repeated stall" if state.get("continuation", False)
                        else "end of feed"
                    )
                    break

                if step == scroll_limit:
                    break

                await page.evaluate(HOME_SCROLL_SCRIPT)
                await asyncio.sleep(random.uniform(*HOME_SCROLL_PAUSE_SECONDS))
                state = await snapshot(page)
    except TimeoutError:
        if not budget.expired():
            stop = "browser timeout"
            raise

        stop = "time budget"
    except Exception:
        stop = "listing error"
        raise
    finally:
        await capture.stop()
        logger.info(
            "Finished YouTube %s items=%d reason=%s elapsed=%.1fs",
            feed.name, len(collected.item_ids), stop, loop.time() - started,
        )

    _check_home_result(stop, len(collected.item_ids))


def _check_home_result(stop: str, item_count: int) -> None:
    if not item_count:
        raise YouTubeFeedError(
            f"No usable recommendation items found ({stop}); "
            "check access or renderer changes",
        )

    if stop != "end of feed":
        raise YouTubeFeedIncomplete(
            f"Recommendations incomplete: {stop}; retained {item_count} "
            "items, but the end of the feed was not observed",
        )


def _normalize_home_renderers(
    result: YtDlpHomeResult, feed: YouTubeFeed, settings: YouTubeSettings,
    observed_at: datetime.datetime,
) -> tuple[list[RawPost], int]:
    posts_out: list[RawPost] = []
    failures = 0
    capture = FeedCapture(feed, settings, set())
    capture.observed_at = observed_at
    request_url = _localized(ORIGIN + "/feed/recommended",
                             feed.region or settings.region)

    for entry in result.renderers:
        try:
            posts = capture.extract(
                [(entry.key, entry.raw, entry.shelf)], request_url,
            ).posts
        except Exception:
            count("parse_failures")
            failures += 1
            continue

        for post in posts:
            payload = as_dict(json.loads(post.payload))
            payload["discovery"]["shelves"] = list(entry.shelves)
            posts_out.append(post._replace(payload=json.dumps(payload)))

    return posts_out, failures


async def _collect_ytdlp_home(
    page: zendriver.Tab, feed: YouTubeFeed, settings: YouTubeSettings,
    batches: list[ScrapeBatch],
) -> None:
    if not settings.authenticated:
        raise YouTubeFeedError(
            "Recommendations require a saved YouTube session",
        )

    loop = asyncio.get_running_loop()
    started = loop.time()
    observed_at = datetime.datetime.now(datetime.UTC)
    cookies = await session_cookies(page)

    try:
        result = await collect_ytdlp_home(
            cookies,
            timeout_seconds=max(
                0.001, HOME_BUDGET_SECONDS - (loop.time() - started),
            ),
            max_entries=HOME_ITEM_LIMIT,
            region=feed.region or settings.region,
            include_renderers=True,
        )
    finally:
        cookies.clear()

    posts, parse_failures = _normalize_home_renderers(
        result, feed, settings, observed_at,
    )
    captured = merge_home_posts([], posts, settings)

    if captured.posts:
        batches.append(captured)

    logger.info(
        "YouTube Recommendations via yt-dlp: %d items (%s) in %.1fs",
        len(captured.posts), result.outcome, loop.time() - started,
    )

    if not captured.posts:
        raise YouTubeFeedError("yt-dlp returned no usable recommendations")

    if (result.outcome != "completed" or parse_failures
            or set(result.warnings).intersection({
                "repeated_continuation", "expired_session",
                "feed_redirected_to_home",
            })):
        raise YouTubeFeedIncomplete(
            f"Recommendations retained yt-dlp results ({result.outcome}); "
            "the listing check was incomplete",
        )


async def _scrape_feed(
    page: zendriver.Tab,
    feed: YouTubeFeed,
    settings: YouTubeSettings,
    known_ids: set[str],
) -> AsyncIterator[ScrapeBatch]:
    if feed.kind == "playlist":
        result = await collect_playlist(page, feed.url, settings)
        yield ScrapeBatch([], [], youtube_playlists=(result,))
        return

    if feed.kind == "home":
        batches: list[ScrapeBatch] = []
        failure: Exception | None = None

        try:
            await _collect_ytdlp_home(page, feed, settings, batches)
        except Exception as error:
            failure = error

        for batch in batches:
            yield batch

        if failure is not None:
            raise failure

        return

    async for batch in _scrape_listing_feed(page, feed, settings, known_ids):
        yield batch


async def _scrape_listing_feed(
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

        state = await _channel_state(page, feed, state)
        collected.playlist_name = playlist_title(state.get("initial"))

        if feed.kind == "channel":
            channel = discover_channel(
                state.get("initial"), feed_url=feed.url,
                request_url=request_url, observed_at=collected.observed_at,
            )

            if channel is not None:
                collected.channel = (
                    channel.channel_id, channel.channel_name,
                    channel.channel_url,
                )
                yield ScrapeBatch([], [], youtube_channels=(channel,))

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

    if len(capture.item_ids) >= capture.item_limit:
        return "item limit"

    if idle >= capture.settings.stall_scrolls:
        return "repeated stall"

    return None


async def _verify_saved_session(
    settings: Settings, session: BrowserSession, page: zendriver.Tab,
) -> None:
    if not settings.youtube.authenticated:
        return

    await navigate(page, _localized(ORIGIN + "/", settings.youtube.region))
    profile = (settings.youtube.profile_dir
               or settings.data_dir / "youtube-browser-profile")
    guard = await session.get_youtube_session(profile / "youtube-session.json")

    if await guard.check(page) is not True:
        raise YouTubeFeedError(
            "Could not verify the saved YouTube session; "
            "run youtube-login separately",
        )


def _collection_failure(name: str, error: Exception) -> None:
    count("fetch_failures")
    stats = current_source.get()

    if stats is not None:
        stats.error_type = type(error).__name__

    logger.warning("YouTube %s failed (%s)", name, type(error).__name__)


async def scrape_youtube_feeds(
    settings: Settings,
    source: SourceConfig,
    session: BrowserSession,
    *,
    known_ids: Mapping[str, set[str]] | None = None,
    known_playlists: Mapping[str, dict[str, Any]] | None = None,
) -> AsyncIterator[ScrapeBatch]:
    feeds = configured_feeds(settings.youtube, source)
    stats = current_source.get()

    if stats is not None:
        stats.feed_urls = tuple(feed.url for feed in feeds)

    library = settings.youtube.playlists and (
        settings.youtube.authenticated or bool(known_playlists)
    )

    if not feeds and not library:
        return

    async with asyncio.timeout(30):
        browser = await session.get_browser()
        page = await browser.get("about:blank", new_tab=True)

    try:
        try:
            await _verify_saved_session(settings, session, page)
        except Exception as error:
            _collection_failure("session verification", error)
            return

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
                _collection_failure(feed.name, error)

        async for batch in _saved_library_batches(
            page, settings.youtube, feeds, known_playlists or {},
            enabled=library,
        ):
            yield batch
    finally:
        try:
            if settings.youtube.authenticated:
                profile = (
                    settings.youtube.profile_dir
                    or settings.data_dir / "youtube-browser-profile"
                )
                await save_youtube_session_from_page(
                    browser, page, profile / "youtube-session.json",
                )

            async with asyncio.timeout(5):
                await page.close()
        except Exception as error:
            logger.warning(
                "Could not close YouTube tab (%s)", type(error).__name__,
            )


async def _saved_library_batches(
    page: zendriver.Tab, settings: YouTubeSettings,
    feeds: tuple[YouTubeFeed, ...], known: Mapping[str, dict[str, Any]],
    *, enabled: bool,
) -> AsyncIterator[ScrapeBatch]:
    if not enabled:
        return

    try:
        async for batch in collect_playlist_library(
            page, settings, known,
            excluded={playlist_id(feed.url) for feed in feeds
                      if feed.kind == "playlist"},
        ):
            yield batch
    except Exception as error:
        _collection_failure("Playlists", error)
