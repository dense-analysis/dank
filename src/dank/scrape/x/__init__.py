from __future__ import annotations

import asyncio
import datetime
import json
import logging
from collections.abc import AsyncIterator, Iterable
from typing import cast
from urllib.parse import quote

import zendriver
from zendriver.core.connection import ProtocolException

from dank.config import XSettings
from dank.model import AssetDiscovery, RawPost
from dank.scrape.metrics import count, current_source
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import (
    BrowserProfileBusyError,
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


# Restart only the owned browser, with its existing profile and sign-in state.
BROWSER_RECOVERY_TIMEOUT_SECONDS = 20
RECOVERABLE_BROWSER_ERRORS = (TimeoutError, ConnectionError, ProtocolException)


class XBrowserRecoveryError(RuntimeError):
    pass


class _XBrowser:
    def __init__(self, session: BrowserSession) -> None:
        self.session = session
        self.page: zendriver.Tab | None = None

    async def get_page(self) -> zendriver.Tab:
        if self.page is None:
            async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
                browser = await self.session.get_browser()
                page = await browser.get(X_HOME_URL)

            await wait_for_x_session(
                page, timeout_seconds=SESSION_CHECK_TIMEOUT_SECONDS,
            )
            self.page = page

        return self.page

    async def reset(self) -> None:
        self.page = None
        logger.info("Restarting X browser with the saved session")

        try:
            async with asyncio.timeout(BROWSER_RECOVERY_TIMEOUT_SECONDS):
                await self.session.close()
        except Exception as error:
            raise XBrowserRecoveryError(
                "Could not close the owned X browser safely; stopping X",
            ) from error


def _record_failure(error: Exception) -> None:
    count("fetch_failures")
    stats = current_source.get()

    if stats is not None:
        stats.error_type = type(error).__name__


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

    browser = _XBrowser(session)
    logger.info(
        "Collecting X timelines first; discussions are deferred. "
        "Use --x-details to refresh saved posts separately.",
    )

    index = 0

    for index, account in enumerate(accounts, 1):
        logger.info(
            "X timeline [%d/%d] account=%s", index, len(accounts), account,
        )

        try:
            async for batch in _collect_account(browser, account, settings):
                yield batch
        except (LoginRequiredError, BrowserProfileBusyError,
                XBrowserRecoveryError) as error:
            _record_failure(error)
            logger.warning(
                "X timelines stopped at account=%s: %s; "
                "%d accounts remain unattempted",
                account, error, len(accounts) - index,
            )
            break

    logger.info(
        "X timeline pass finished; attempted=%d/%d", index, len(accounts),
    )


async def _collect_account(
    browser: _XBrowser, account: str, settings: XSettings,
) -> AsyncIterator[ScrapeBatch]:
    seen_posts: set[str] = set()
    seen_assets: set[str] = set()

    for attempt in range(2):
        try:
            page = await browser.get_page()

            async for batch in _scrape_account(page, account, settings):
                posts = [p for p in batch.posts if p.post_id not in seen_posts]
                assets = [a for a in batch.assets if a.url not in seen_assets]
                seen_posts.update(p.post_id for p in posts)
                seen_assets.update(a.url for a in assets)

                if posts or assets:
                    yield batch._replace(posts=posts, assets=assets)
        except (LoginRequiredError, BrowserProfileBusyError):
            raise
        except RECOVERABLE_BROWSER_ERRORS as error:
            logger.warning(
                "X timeline account=%s attempt=%d browser failure=%s; "
                "retained posts=%d", account, attempt + 1,
                type(error).__name__, len(seen_posts),
            )
            await browser.reset()

            if not attempt:
                count("retries")
                continue

            _account_failed(account, seen_posts, error)

            return
        except Exception as error:
            _account_failed(account, seen_posts, error)
            await browser.reset()

            return
        else:
            if not seen_posts:
                _account_failed(
                    account, seen_posts,
                    ValueError("No usable timeline posts returned"),
                )
            else:
                logger.info(
                    "X timeline account=%s outcome=completed "
                    "posts=%d attempts=%d",
                    account, len(seen_posts), attempt + 1,
                )

            return


def _account_failed(
    account: str, seen_posts: set[str], error: Exception,
) -> None:
    _record_failure(error)
    logger.warning(
        "X timeline account=%s outcome=%s posts=%d error=%s: %s; "
        "continuing with remaining accounts",
        account, "partial" if seen_posts else "failed", len(seen_posts),
        type(error).__name__, error,
    )


async def scrape_x_posts(
    settings: XSettings, posts: list[RawPost], session: BrowserSession,
) -> AsyncIterator[ScrapeBatch]:
    browser = _XBrowser(session)

    for index, post in enumerate(posts, 1):
        logger.info(
            "X discussion [%d/%d] post=%s", index, len(posts), post.post_id,
        )

        try:
            captured = await _capture_saved_post(browser, post, settings)
            yield ScrapeBatch(posts=[captured], assets=[])
            archive = json.loads(captured.payload)["conversation_capture"]
            reason = archive["reason"]

            if reason == "login_required":
                logger.warning("X discussions stopped: sign-in required")
                break

            if reason in {"timeout", "unavailable"}:
                await browser.reset()
        except (BrowserProfileBusyError, XBrowserRecoveryError) as error:
            _record_failure(error)
            logger.warning("X discussions stopped: %s", error)
            break


async def _capture_saved_post(
    browser: _XBrowser, post: RawPost, settings: XSettings,
) -> RawPost:
    reason = "disabled"

    if settings.conversation.enabled:
        try:
            page = await browser.get_page()
        except LoginRequiredError:
            reason = "login_required"
        except RECOVERABLE_BROWSER_ERRORS:
            reason = "timeout"
        except BrowserProfileBusyError:
            raise
        except Exception:
            logger.warning("X discussion browser unavailable", exc_info=True)
            reason = "unavailable"
        else:
            return await capture_conversation(
                page, post, settings.conversation,
            )

        count("fetch_failures")

    return ConversationCapture(
        post, settings.conversation.max_replies,
    ).finish(reason)


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


async def _drain_posts_and_assets(
    capture: NetworkCapture,
    seen_posts: set[str],
    seen_assets: set[str],
    *,
    timeout_seconds: float,
) -> tuple[list[RawPost], list[AssetDiscovery]]:
    async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
        responses = await capture.drain(
            timeout_seconds=max(0.05, timeout_seconds),
        )
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
