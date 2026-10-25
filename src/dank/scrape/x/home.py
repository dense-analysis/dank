"""Capture the signed-in For You timeline, with explicit feed membership."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import zendriver

from dank.config import XSettings
from dank.model import AssetDiscovery, RawPost
from dank.scrape.metrics import count, current_source
from dank.scrape.types import ScrapeBatch
from dank.scrape.x import (
    _asset_discovery_from_extracted,  # pyright: ignore[reportPrivateUsage]
    _raw_post_from_extracted,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.x.navigation import (
    check_login_required,
    scroll_conversation as scroll,
)
from dank.scrape.x.payloads import extract_posts_from_payload
from dank.scrape.x.session import (
    SESSION_CHECK_TIMEOUT_SECONDS,
    X_HOME_URL,
    LoginRequiredError,
    wait_for_x_session,
)
from dank.scrape.zendriver import (
    BrowserProfileBusyError,
    BrowserSession,
    NetworkCapture,
    NetworkResponse,
)
from dank.x_content import object_value

logger = logging.getLogger(__name__)
HOME_PATTERNS = (r"^https://x\.com/i/api/graphql/[^/?]+/HomeTimeline(?:\?|$)",)
ACTION_TIMEOUT_SECONDS = 30
COLLECTION_TIMEOUT_SECONDS = 90
MAX_IDLE_SCROLLS = 4


def home_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Read timeline entries only; never recursively promote quotes or ads."""
    home = object_value(object_value(payload.get("data")).get("home"))
    timeline = object_value(home.get("home_timeline_urt"))
    items: list[dict[str, Any]] = []

    for instruction in timeline.get("instructions", []):
        node = object_value(instruction)
        entries = node.get("entries", [])

        if node.get("type") == "TimelineReplaceEntry":
            entries = [node.get("entry")]

        for entry in entries:
            content = object_value(object_value(entry).get("content"))

            if content.get("entryType") == "TimelineTimelineItem":
                items.append(object_value(content.get("itemContent")))
            elif content.get("entryType") == "TimelineTimelineModule":
                for item in content.get("items", []):
                    wrapper = object_value(object_value(item).get("item"))
                    items.append(object_value(wrapper.get("itemContent")))

        if node.get("type") == "TimelineAddToModule":
            for item in node.get("moduleItems", []):
                wrapper = object_value(object_value(item).get("item"))
                items.append(object_value(wrapper.get("itemContent")))

    return [
        item
        for item in items
        if (
            item.get("itemType") == "TimelineTweet"
            and not item.get("promotedMetadata")
        )
    ]


class HomeCapture:
    def __init__(self, limit: int) -> None:
        self.limit = max(1, limit)
        self.observed_at = dt.datetime.now(dt.UTC)
        self.recommended: set[str] = set()
        self.saved: set[tuple[str, bool]] = set()
        self.assets: set[str] = set()

    def extract(self, response: NetworkResponse) -> ScrapeBatch:
        posts: list[RawPost] = []
        assets: list[AssetDiscovery] = []

        for item in _response_items(response):
            if len(self.recommended) >= self.limit:
                break

            result = object_value(
                object_value(item.get("tweet_results")).get("result"),
            )
            root = object_value(result.get("tweet")) or result
            root_id = str(root.get("rest_id", ""))

            if not root_id or root_id in self.recommended:
                continue

            extracted = extract_posts_from_payload(
                {"tweet_results": {"result": result}},
            )
            primary = next(
                (
                    post
                    for post in extracted
                    if (post.post_id == root_id and not post.is_reference)
                ),
                None,
            )

            if primary is None:
                continue

            self.recommended.add(root_id)

            for post in extracted:
                key = (post.post_id, post.is_reference)

                if key in self.saved and post.post_id != root_id:
                    continue

                raw = _raw_post_from_extracted(post, response.url)

                if post.post_id == root_id:
                    raw = raw._replace(
                        payload=json.dumps(
                            {
                                **post.payload,
                                "dank_x_home": {
                                    "feed": "for_you",
                                    "url": X_HOME_URL,
                                    "position": len(self.recommended),
                                    "observed_at": (
                                        self.observed_at.isoformat()
                                    ),
                                    "item": item,
                                },
                            },
                            separators=(",", ":"),
                        ),
                    )

                posts.append(raw._replace(scraped_at=self.observed_at))
                self.saved.add(key)

                for asset in post.assets:
                    if asset.url not in self.assets:
                        assets.append(
                            _asset_discovery_from_extracted(post, asset),
                        )
                        self.assets.add(asset.url)

        return ScrapeBatch(posts, assets)


async def select_for_you(page: zendriver.Tab) -> None:
    # The first home tab is For You, including in non-English interfaces;
    # only HomeTimeline responses are accepted, never HomeLatestTimeline.
    async with asyncio.timeout(ACTION_TIMEOUT_SECONDS):
        tab = await page.select('[role="tablist"] [role="tab"]', timeout=20)
        await tab.click()


async def scrape_x_for_you(
    settings: XSettings,
    session: BrowserSession,
) -> AsyncIterator[ScrapeBatch]:
    collected = HomeCapture(settings.scrape.max_posts)
    capture: NetworkCapture | None = None
    started = asyncio.get_running_loop().time()

    try:
        async with asyncio.timeout(ACTION_TIMEOUT_SECONDS):
            browser = await session.get_browser()
            page = await browser.get("about:blank")
            capture = NetworkCapture(page, HOME_PATTERNS)
            await capture.start()
            await page.get(X_HOME_URL)
            await wait_for_x_session(
                page, timeout_seconds=SESSION_CHECK_TIMEOUT_SECONDS,
            )
            await select_for_you(page)

        idle = 0

        for step in range(max(0, settings.scrape.max_scrolls) + 1):
            if (
                asyncio.get_running_loop().time() - started
                >= COLLECTION_TIMEOUT_SECONDS
            ):
                count("fetch_failures")
                logger.warning(
                    "X For You stopped at its collection time limit",
                )
                break

            async with asyncio.timeout(ACTION_TIMEOUT_SECONDS):
                await check_login_required(page)
                responses = await capture.drain(
                    timeout_seconds=max(
                        1.5 if step == 0 else 0.35,
                        settings.scrape.scroll_pause_seconds,
                    ),
                )

            previous = len(collected.recommended)

            for response in responses:
                batch = collected.extract(response)

                if batch.posts or batch.assets:
                    yield batch

            idle = idle + 1 if len(collected.recommended) == previous else 0

            if (
                len(collected.recommended) >= collected.limit
                or idle >= MAX_IDLE_SCROLLS
            ):
                break

            if step < settings.scrape.max_scrolls:
                async with asyncio.timeout(ACTION_TIMEOUT_SECONDS):
                    await scroll(page)

        if not collected.recommended:
            raise ValueError("X For You returned no usable recommendations")

        logger.info(
            "X For You saved %d recommendations", len(collected.recommended),
        )
    except Exception as error:
        _handle_failure(error, len(collected.recommended))
    finally:
        if capture is not None:
            await capture.stop()


def _response_items(response: NetworkResponse) -> list[dict[str, Any]]:
    if response.status != 200:
        raise RuntimeError(f"X For You returned HTTP {response.status}")

    payload = object_value(json.loads(response.body))

    if payload.get("errors"):
        raise RuntimeError("X For You returned a timeline error")

    if not object_value(object_value(payload.get("data")).get("home")):
        raise ValueError("X For You response has no home timeline")

    return home_items(payload)


def _handle_failure(error: Exception, retained: int) -> None:
    count("fetch_failures")
    stats = current_source.get()

    if stats is not None:
        stats.error_type = type(error).__name__

    logger.warning(
        "X For You %s; retained=%d: %s",
        "partial" if retained else "failed", retained, error,
    )

    if isinstance(error, LoginRequiredError | BrowserProfileBusyError):
        raise error
