from __future__ import annotations

import asyncio
import datetime
import json
import logging
import re
from collections.abc import AsyncIterator, Iterable
from typing import cast
from urllib.parse import urlencode

import zendriver
from zendriver import cdp

from dank.config import (
    RedditSettings,
    RedditSort,
    RedditSourceConfig,
    RedditTimeFilter,
)
from dank.model import AssetDiscovery, RawPost
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import (
    BrowserSession,
    NetworkCapture,
    NetworkResponse,
)

from .communities import discover_community
from .html import extract_html_posts
from .payloads import as_dict, extract_json_posts, text, timestamp

logger = logging.getLogger(__name__)
REDDIT_SOURCE = "reddit"
DEFAULT_LISTING = RedditSourceConfig()
REDDIT_PATTERNS = (r"https://(?:[a-z0-9-]+\.)*reddit\.com/",)
SNAPSHOT_SCRIPT = """JSON.stringify({
    url: location.href,
    title: document.title,
    text: (document.body?.innerText || '').slice(0, 4000),
    community_html: Array.from(document.querySelectorAll(
        'shreddit-subreddit-header-buttons, '
        + '#subreddit-icon-img, #subreddit-icon-img-desktop'))
        .filter(node => !node.closest(
            'shreddit-post, shreddit-ad-post, [data-promoted="true"], '
            + '[promoted]:not([promoted="false"]), '
            + '[is-promoted]:not([is-promoted="false"]), '
            + '[is-ad]:not([is-ad="false"])'))
        .map(node => node.outerHTML).join('\\n'),
    html: Array.from(document.querySelectorAll('shreddit-post'))
        .filter(post => !post.closest(
            'shreddit-ad-post, [data-promoted="true"]'))
        .map(post => post.outerHTML).join('\\n')
})"""
SCROLL_SCRIPT = """(() => {
    const posts = document.querySelectorAll('shreddit-post');
    const last = posts[posts.length - 1];
    const root = document.scrollingElement || document.documentElement;
    const viewport = window.innerHeight || 800;
    if (last) last.scrollIntoView({block: 'end'});
    window.scrollTo(0, Math.min(root.scrollTop + viewport,
        Math.max(0, root.scrollHeight - viewport)));
})()"""
BLOCKED_MESSAGES = (
    "you've been blocked by network security",
    "whoa there, pardner",
    "verify you are human",
    "this community is private",
    "this community has been banned",
    "community not found",
    "log in to view this community",
    "this is a private community",
)


class RedditScrapeError(RuntimeError):
    """A Reddit listing could not be collected without user intervention."""


def community_url(
    community: str,
    *,
    sort: RedditSort = DEFAULT_LISTING.sort,
    time_filter: RedditTimeFilter = DEFAULT_LISTING.time_filter,
) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", community):
        raise ValueError("Expected a Reddit community name")

    sort = RedditSort(sort)
    time_filter = RedditTimeFilter(time_filter)

    url = f"https://www.reddit.com/r/{community}/{sort}/"

    return (
        f"{url}?{urlencode({'t': time_filter})}"
        if sort is RedditSort.TOP else url
    )


async def scrape_reddit_communities(
    settings: RedditSettings,
    communities: tuple[str, ...],
    session: BrowserSession,
    *,
    sort: RedditSort = DEFAULT_LISTING.sort,
    time_filter: RedditTimeFilter = DEFAULT_LISTING.time_filter,
) -> AsyncIterator[ScrapeBatch]:
    # Each source owns a tab so X and other Reddit sources cannot navigate it.
    async with asyncio.timeout(30):
        browser = await session.get_browser()
        page = await browser.get("about:blank", new_tab=True)

    visited: set[str] = set()

    try:
        for community in communities:
            if community.casefold() in visited:
                continue

            visited.add(community.casefold())

            async for batch in _scrape_community(
                page,
                community,
                settings,
                sort=sort,
                time_filter=time_filter,
            ):
                yield batch
    finally:
        try:
            async with asyncio.timeout(5):
                await page.close()
        except TimeoutError:
            logger.warning("Timed out closing the Reddit browser tab")


async def _scrape_community(
    page: zendriver.Tab,
    community: str,
    settings: RedditSettings,
    *,
    sort: RedditSort,
    time_filter: RedditTimeFilter,
) -> AsyncIterator[ScrapeBatch]:
    url = community_url(community, sort=sort, time_filter=time_filter)
    limits = settings.scrape
    capture = NetworkCapture(
        page,
        REDDIT_PATTERNS,
        resource_types=(
            cdp.network.ResourceType.DOCUMENT,
            cdp.network.ResourceType.XHR,
            cdp.network.ResourceType.FETCH,
        ),
    )
    seen_posts: set[str] = set()
    seen_assets: set[str] = set()
    community_discovered = False
    idle_scrolls = 0
    stop_reason = "scroll limit"
    step = 0
    logger.info(
        "Starting Reddit scrape community=r/%s sort=%s",
        community,
        sort,
    )

    try:
        async with asyncio.timeout(10):
            await capture.start()

        async with asyncio.timeout(20):
            await page.send(cdp.page.navigate(url))

        for step in range(limits.max_scrolls + 1):
            # Reddit may redirect its initial document before rendering cards.
            pause = (
                max(3, limits.scroll_pause_seconds)
                if step == 0
                else limits.scroll_pause_seconds
            )

            async with asyncio.timeout(max(10, pause + 5)):
                await asyncio.sleep(pause)
                responses = await capture.drain(timeout_seconds=0.05)
                snapshot = as_dict(
                    json.loads(
                        str(await page.evaluate(SNAPSHOT_SCRIPT)),
                    ),
                )
            batch = extract_posts_and_assets(
                responses,
                community,
                seen_posts,
                seen_assets,
                max_posts=limits.max_posts,
                dom_html=text(snapshot.get("html")),
                request_url=text(snapshot.get("url")) or url,
            )
            _check_blocked(snapshot, community)

            if not community_discovered and (
                discovery := discover_community(
                    responses,
                    community,
                    dom_html=text(snapshot.get("community_html")),
                    request_url=text(snapshot.get("url")) or url,
                )
            ) is not None:
                batch = batch._replace(communities=(discovery,))
                community_discovered = True

            logger.info(
                "Reddit r/%s scroll=%d new_posts=%d total_posts=%d",
                community,
                step,
                len(batch.posts),
                len(seen_posts),
            )

            if batch.posts or batch.assets or batch.communities:
                yield batch

            if batch.posts:
                idle_scrolls = 0
            else:
                idle_scrolls += 1

            if len(seen_posts) >= limits.max_posts:
                stop_reason = "post limit"
                break

            if idle_scrolls >= settings.stall_scrolls:
                stop_reason = "repeated stall"
                break

            if step < limits.max_scrolls:
                async with asyncio.timeout(10):
                    await page.evaluate(SCROLL_SCRIPT)

        logger.info(
            "Finished Reddit r/%s posts=%d scrolls=%d reason=%s "
            "post_limit=%d scroll_limit=%d",
            community,
            len(seen_posts),
            step,
            stop_reason,
            limits.max_posts,
            limits.max_scrolls,
        )

        if not seen_posts and not community_discovered:
            raise RedditScrapeError(
                f"No public posts found for r/{community}. Reddit may have "
                "blocked access, the community may be empty, or its listing "
                "markup may have changed. Open the listing in the configured "
                "browser profile to check access.",
            )
    except TimeoutError as exc:
        raise RedditScrapeError(
            f"Reddit browser timed out for r/{community}; check the listing "
            "in the configured browser profile and try again.",
        ) from exc
    finally:
        await capture.stop()


def _check_blocked(snapshot: dict[str, object], community: str) -> None:
    if text(snapshot.get("html")):
        return

    body = text(snapshot.get("text")).casefold()
    location = text(snapshot.get("url"))

    if any(message in body for message in BLOCKED_MESSAGES) or re.search(
        r"reddit\.com/(?:login|account/login|account/register)(?:/|\?|$)",
        location,
    ):
        raise RedditScrapeError(
            f"Reddit access requires attention for r/{community}: "
            "the browser reached an access restriction or login page. "
            "Open Reddit in the configured browser profile to check access.",
        )


def extract_posts_and_assets(
    responses: Iterable[NetworkResponse],
    community: str,
    seen_posts: set[str],
    seen_assets: set[str],
    *,
    max_posts: int,
    dom_html: str = "",
    request_url: str = "",
) -> ScrapeBatch:
    posts: list[RawPost] = []
    assets: list[AssetDiscovery] = []
    candidates: list[tuple[dict[str, object], str]] = []

    for response in responses:
        if not 200 <= response.status < 300:
            continue

        try:
            extracted = extract_json_posts(
                json.loads(response.body),
                community,
            )
        except (json.JSONDecodeError, RecursionError):
            extracted = extract_html_posts(response.body, community)

        candidates.extend((post, response.url) for post in extracted)

    candidates.extend(
        (post, request_url) for post in extract_html_posts(dom_html, community)
    )

    for post, origin in _merge_candidates(candidates):
        post_id = text(post.get("id"))

        if post_id in seen_posts:
            assets.extend(_discover_assets(post, post_id, seen_assets))
            continue

        if len(seen_posts) >= max_posts:
            continue

        seen_posts.add(post_id)
        created = timestamp(post.get("created_utc"))
        posts.append(
            RawPost(
                domain="reddit.com",
                post_id=post_id,
                url=text(post.get("permalink")),
                post_created_at=_created_at(created),
                scraped_at=datetime.datetime.now(datetime.UTC),
                source=REDDIT_SOURCE,
                request_url=origin,
                payload=json.dumps(post, separators=(",", ":")),
            ),
        )
        assets.extend(_discover_assets(post, post_id, seen_assets))

    return ScrapeBatch(posts, assets)


def _created_at(value: float | None) -> datetime.datetime | None:
    if value is None:
        return None

    try:
        return datetime.datetime.fromtimestamp(value, datetime.UTC)
    except (ValueError, OverflowError, OSError):
        return None


def _discover_assets(
    post: dict[str, object],
    post_id: str,
    seen_assets: set[str],
) -> list[AssetDiscovery]:
    assets: list[AssetDiscovery] = []

    for item in cast(list[object], post.get("media", [])):
        media = as_dict(item)
        url = text(media.get("url"))

        asset_key = f"{post_id}:{url}"

        if url and asset_key not in seen_assets:
            seen_assets.add(asset_key)
            assets.append(
                AssetDiscovery(
                    source=REDDIT_SOURCE,
                    domain="reddit.com",
                    post_id=post_id,
                    url=url,
                    asset_type=text(media.get("asset_type")),
                    estimated_size_bytes=None,
                ),
            )

    return assets


def _merge_candidates(
    candidates: list[tuple[dict[str, object], str]],
) -> list[tuple[dict[str, object], str]]:
    merged: dict[str, tuple[dict[str, object], str]] = {}

    for post, origin in candidates:
        post_id = text(post.get("id"))

        if post_id not in merged:
            merged[post_id] = (dict(post), origin)
            continue

        previous, _ = merged[post_id]

        for key in ("selftext", "selftext_html"):
            if len(text(post.get(key))) > len(text(previous.get(key))):
                previous[key] = post[key]

        media = {
            text(as_dict(item).get("url")): item
            for item in cast(list[object], previous.get("media", []))
            + cast(list[object], post.get("media", []))
        }
        previous["media"] = list(media.values())

        if previous.get("raw") != post.get("raw"):
            previous["raw"] = {
                "captures": [previous.get("raw"), post.get("raw")],
            }

        for key in (
            "author",
            "link_flair_text",
            "created_utc",
            "score",
            "num_comments",
        ):
            if not previous.get(key) and post.get(key):
                previous[key] = post[key]

        if previous.get("url") == previous.get("permalink"):
            previous["url"] = post.get("url", previous["url"])

    return list(merged.values())
