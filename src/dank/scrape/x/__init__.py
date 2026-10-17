from __future__ import annotations

import asyncio
import datetime
import json
import logging
from collections.abc import AsyncIterator, Iterable
from typing import cast
from urllib.parse import quote, urlsplit

import zendriver

from dank.config import XSettings
from dank.model import AssetDiscovery, RawPost
from dank.scrape.metrics import count, current_source
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import (
    BrowserSession,
    NetworkCapture,
    NetworkResponse,
)

from .conversations import ConversationCapture, capture_conversation
from .navigation import (
    check_login_required as _check_login_required,
    ensure_navigation as _ensure_navigation,
    scroll as _scroll,
)
from .payloads import (
    XAsset,
    XExtractedPost,
    extract_posts_from_payload,
)
from .session import (
    SESSION_CHECK_TIMEOUT_SECONDS,
    X_HOME_URL,
    LoginRequiredError,
    wait_for_x_session,
)

logger = logging.getLogger(__name__)

X_SOURCE = "x"
X_GRAPHQL_PATTERNS = (
    r"^https://x\.com/i/api/graphql/[^/?]+/"
    r"(?:UserTweets|UserTweetsAndReplies|UserOriginalsTimeline|"
    r"TweetDetail|UserMedia)(?:\?|$)",
)
# Bound browser steps without charging time spent saving yielded batches.
BROWSER_ACTION_TIMEOUT_SECONDS = 30
FAST_SCROLL_PAUSE_SECONDS = 0.35
INITIAL_DRAIN_TIMEOUT_SECONDS = 0.05
MAX_IDLE_SCROLLS = 4


async def scrape_x_accounts(
    settings: XSettings,
    accounts: tuple[str, ...],
    session: BrowserSession,
) -> AsyncIterator[ScrapeBatch]:
    if not accounts:
        stats = current_source.get()

        if stats is not None:
            stats.skipped = True

        logger.warning("No X accounts configured")
        return

    try:
        async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
            browser = await session.get_browser()
            page = await browser.get(X_HOME_URL)

        await wait_for_x_session(
            page, timeout_seconds=SESSION_CHECK_TIMEOUT_SECONDS,
        )

        for account in accounts:
            async for batch in _scrape_account_with_conversations(
                page, account, settings,
            ):
                yield batch
    except (LoginRequiredError, TimeoutError) as error:
        count("fetch_failures")
        stats = current_source.get()

        if stats is not None:
            stats.error_type = type(error).__name__

        if isinstance(error, TimeoutError):
            logger.warning(
                "X collection stopped: browser did not respond within %ss. "
                "Saved posts are retained; other sources can continue.",
                BROWSER_ACTION_TIMEOUT_SECONDS,
            )
            await session.close()
        else:
            logger.warning("X collection stopped: %s", error)


async def _scrape_account_with_conversations(
    page: zendriver.Tab, account: str, settings: XSettings,
) -> AsyncIterator[ScrapeBatch]:
    candidates: dict[str, RawPost] = {}

    async for batch in _scrape_account(page, account, settings):
        # Yield first so the runner persists timelines before details.
        yield batch

        for post in batch.posts:
            if (urlsplit(post.url).path.split("/")[1].lower()
                    == account.strip("@").strip().lower()):
                candidates[post.post_id] = post

    if settings.conversation.enabled:
        newest = sorted(candidates.values(),
                        key=lambda post: int(post.post_id),
                        reverse=True)

        limit = settings.conversation.max_posts_per_account

        for post in newest[:limit]:
            captured = await capture_conversation(page, post,
                                                  settings.conversation)
            yield ScrapeBatch(posts=[captured], assets=[])


async def scrape_x_posts(
    settings: XSettings, posts: list[RawPost], session: BrowserSession,
) -> AsyncIterator[ScrapeBatch]:
    if not posts:
        return

    reason = None
    page = None

    if settings.conversation.enabled:
        try:
            async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
                browser = await session.get_browser()
                page = await browser.get(X_HOME_URL)

            await wait_for_x_session(
                page, timeout_seconds=SESSION_CHECK_TIMEOUT_SECONDS,
            )
        except LoginRequiredError:
            reason = "login_required"
        except TimeoutError:
            reason = "timeout"
        except Exception:
            logger.warning("X conversation browser unavailable", exc_info=True)
            reason = "unavailable"
    else:
        reason = "disabled"

    for post in posts:
        if page is None or reason:
            captured = ConversationCapture(
                post, settings.conversation.max_replies,
            ).finish(reason)

            if reason != "disabled":
                count("fetch_failures")
        else:
            captured = await capture_conversation(page, post,
                                                  settings.conversation)

        yield ScrapeBatch(posts=[captured], assets=[])


async def _scrape_account(
    page: zendriver.Tab,
    account: str,
    settings: XSettings,
) -> AsyncIterator[ScrapeBatch]:
    limits = settings.scrape
    handle = account.strip("@").strip()
    if not handle:
        return

    logger.info("Starting X scrape for account=%s", handle)

    capture = NetworkCapture(page, X_GRAPHQL_PATTERNS)

    try:
        url = f"https://x.com/{quote(handle)}"

        async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
            await capture.start()
            page = await page.get(url)
            await _ensure_navigation(page, url)
            await _check_login_required(page)

        seen_posts: set[str] = set()
        seen_assets: set[str] = set()
        total_posts = 0
        idle_scrolls = 0

        posts, assets = await _drain_posts_and_assets(
            capture,
            seen_posts,
            seen_assets,
            timeout_seconds=INITIAL_DRAIN_TIMEOUT_SECONDS,
        )
        logger.info(
            "Initial drain for %s produced posts=%d assets=%d",
            handle,
            len(posts),
            len(assets),
        )
        total_posts += len(posts)

        if posts or assets:
            yield ScrapeBatch(posts=posts, assets=assets)

        if total_posts >= limits.max_posts:
            return

        for _ in range(limits.max_scrolls):
            async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
                await _scroll(page)
                await _check_login_required(page)

            posts, assets = await _drain_posts_and_assets(
                capture,
                seen_posts,
                seen_assets,
                timeout_seconds=_scroll_pause_seconds(
                    limits.scroll_pause_seconds,
                    idle_scrolls,
                ),
            )
            logger.info(
                "Scroll drain for %s produced posts=%d assets=%d idle=%d",
                handle,
                len(posts),
                len(assets),
                idle_scrolls,
            )
            total_posts += len(posts)

            if posts or assets:
                yield ScrapeBatch(posts=posts, assets=assets)
                idle_scrolls = 0
            else:
                idle_scrolls += 1

            if total_posts >= limits.max_posts:
                break

            if idle_scrolls >= MAX_IDLE_SCROLLS and total_posts > 0:
                break

        posts, assets = await _drain_posts_and_assets(
            capture,
            seen_posts,
            seen_assets,
            timeout_seconds=min(
                limits.scroll_pause_seconds,
                FAST_SCROLL_PAUSE_SECONDS,
            ),
        )
        logger.info(
            "Trailing drain for %s produced posts=%d assets=%d total_posts=%d",
            handle,
            len(posts),
            len(assets),
            total_posts,
        )

        if posts or assets:
            yield ScrapeBatch(posts=posts, assets=assets)
    finally:
        await capture.stop()
        logger.info("Finished X scrape for account=%s", handle)


async def _drain_posts_and_assets(
    capture: NetworkCapture,
    seen_posts: set[str],
    seen_assets: set[str],
    *,
    timeout_seconds: float,
) -> tuple[list[RawPost], list[AssetDiscovery]]:
    responses = await capture.drain(timeout_seconds=max(0.05, timeout_seconds))
    logger.info("Drained %d X network responses", len(responses))

    return extract_posts_and_assets(
        responses,
        seen_posts,
        seen_assets,
    )


def _scroll_pause_seconds(configured_pause: float, idle_scrolls: int) -> float:
    if configured_pause <= 0:
        return FAST_SCROLL_PAUSE_SECONDS

    if idle_scrolls >= 2:
        return configured_pause

    return min(configured_pause, FAST_SCROLL_PAUSE_SECONDS)


def extract_posts_and_assets(
    responses: Iterable[NetworkResponse],
    seen_posts: set[str],
    seen_assets: set[str],
) -> tuple[list[RawPost], list[AssetDiscovery]]:
    posts: list[RawPost] = []
    assets: list[AssetDiscovery] = []

    for response in responses:
        logger.info(
            (
                "X response request_id=%s status=%s resource=%s "
                "mime=%s bytes=%d url=%s"
            ),
            response.request_id,
            response.status,
            response.resource_type,
            response.mime_type,
            len(response.body),
            response.url,
        )

        try:
            payload = json.loads(response.body)
        except json.JSONDecodeError:
            logger.warning(
                "Skipping non-JSON X response request_id=%s url=%s",
                response.request_id,
                response.url,
            )
            continue

        if not isinstance(payload, dict):
            logger.warning(
                "Skipping non-object X payload request_id=%s url=%s",
                response.request_id,
                response.url,
            )
            continue

        payload = cast(dict[str, object], payload)

        extracted_posts = extract_posts_from_payload(payload)
        logger.info(
            "Parsed %d posts from X response request_id=%s",
            len(extracted_posts),
            response.request_id,
        )

        for extracted in extracted_posts:
            if extracted.post_id not in seen_posts:
                seen_posts.add(extracted.post_id)
                posts.append(
                    _raw_post_from_extracted(extracted, response.url),
                )

                for asset in extracted.assets:
                    if asset.url not in seen_assets:
                        seen_assets.add(asset.url)
                        assets.append(
                            _asset_discovery_from_extracted(extracted, asset),
                        )

    logger.info(
        "Extracted %d new posts and %d new assets from drained responses",
        len(posts),
        len(assets),
    )

    return posts, assets


def _raw_post_from_extracted(
    extracted: XExtractedPost,
    request_url: str,
) -> RawPost:
    return RawPost(
        domain="x.com",
        post_id=extracted.post_id,
        url=extracted.url,
        post_created_at=extracted.created_at,
        scraped_at=datetime.datetime.now(datetime.UTC),
        source=X_SOURCE,
        request_url=request_url,
        payload=json.dumps(extracted.payload, separators=(",", ":")),
    )


def _asset_discovery_from_extracted(
    extracted: XExtractedPost,
    asset: XAsset,
) -> AssetDiscovery:
    return AssetDiscovery(
        source="x",
        domain="x.com",
        post_id=extracted.post_id,
        url=asset.url,
        asset_type=asset.asset_type,
        estimated_size_bytes=asset.estimated_size_bytes,
    )
