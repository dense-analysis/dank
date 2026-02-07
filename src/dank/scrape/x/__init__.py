from __future__ import annotations

import datetime
import json
import logging
import random
import time
from collections.abc import AsyncIterator, Iterable
from typing import Any, cast
from urllib.parse import quote

import zendriver
from zendriver import Element, cdp

from dank.config import EmailSettings, XSettings
from dank.model import AssetDiscovery, RawPost
from dank.scrape.imap_email import EmailSearchFilters, wait_for_code
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import (
    BrowserSession,
    NetworkCapture,
    NetworkResponse,
)

from .payloads import (
    XAsset,
    XExtractedPost,
    extract_posts_from_payload,
)

logger = logging.getLogger(__name__)

X_SOURCE = "x"
X_GRAPHQL_PATTERNS = (
    r"https://x\.com/i/api/graphql/.+/UserTweets",
    r"https://x\.com/i/api/graphql/.+/UserTweetsAndReplies",
    r"https://x\.com/i/api/graphql/.+/TweetDetail",
    r"https://x\.com/i/api/graphql/.+/UserMedia",
)
FAST_SCROLL_PAUSE_SECONDS = 0.35
MAX_IDLE_SCROLLS = 4


async def scrape_x_accounts(
    settings: XSettings,
    accounts: tuple[str, ...],
    email_settings: EmailSettings | None,
    session: BrowserSession,
) -> AsyncIterator[ScrapeBatch]:
    browser = await session.get_browser()

    if not accounts:
        logger.warning("No X accounts configured")
        return

    await browser.wait(0.1)

    try:
        page = browser.main_tab
    except Exception:
        page = await browser.get("about:blank")
    for account in accounts:
        try:
            async for batch in _scrape_account(
                page,
                account,
                settings,
                email_settings,
            ):
                yield batch
        except LoginRequiredError:
            logger.warning(
                "X login required; stopping scrape.",
            )
            return


async def _scrape_account(
    page: zendriver.Tab,
    account: str,
    settings: XSettings,
    email_settings: EmailSettings | None,
) -> AsyncIterator[ScrapeBatch]:
    handle = account.strip("@").strip()
    if not handle:
        return

    logger.info("Starting X scrape for account=%s", handle)

    capture = NetworkCapture(page, X_GRAPHQL_PATTERNS)
    await capture.start()

    try:
        url = f"https://x.com/{quote(handle)}"
        page = await page.get(url)
        await _ensure_navigation(page, url)

        if await _is_login_page(page):
            await _login(page, settings, email_settings)
            page = await page.get(url)
            await _ensure_navigation(page, url)

        seen_posts: set[str] = set()
        seen_assets: set[str] = set()
        total_posts = 0
        idle_scrolls = 0

        posts, assets = await _drain_posts_and_assets(
            capture,
            seen_posts,
            seen_assets,
            timeout_seconds=settings.scroll_pause_seconds,
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

        if total_posts >= settings.max_posts:
            return

        for _ in range(settings.max_scrolls):
            await _scroll(page)
            posts, assets = await _drain_posts_and_assets(
                capture,
                seen_posts,
                seen_assets,
                timeout_seconds=_scroll_pause_seconds(
                    settings.scroll_pause_seconds,
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

            if total_posts >= settings.max_posts:
                break

            if idle_scrolls >= MAX_IDLE_SCROLLS and total_posts > 0:
                break

        posts, assets = await _drain_posts_and_assets(
            capture,
            seen_posts,
            seen_assets,
            timeout_seconds=min(
                settings.scroll_pause_seconds,
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


async def _is_login_page(page: zendriver.Tab) -> bool:
    location = await _get_location(page)

    if not location:
        return await _has_login_prompt(page)

    if "/i/flow/login" in location or "/login" in location:
        return True

    return await _has_login_prompt(page)


async def _ensure_navigation(page: zendriver.Tab, url: str) -> None:
    try:
        await page.wait_for_ready_state()
    except TimeoutError:
        pass

    location = await _get_location(page)

    if location and location != "about:blank":
        return

    try:
        await page.send(cdp.page.navigate(url))
    except Exception:
        return

    try:
        await page.wait_for_ready_state()
    except TimeoutError:
        pass


async def _get_location(page: zendriver.Tab) -> str | None:
    try:
        location = await page.evaluate("location.href")
    except Exception:
        return None

    if not isinstance(location, str):
        return None

    return location


async def _has_login_prompt(page: zendriver.Tab) -> bool:
    # Wait for a signup link to appear, which means we aren't logged in.
    try:
        await page.select('a[href="/i/flow/signup"]', timeout=2)
    except TimeoutError:
        return False
    else:
        return True


class LoginRequiredError(RuntimeError):
    pass


async def _submit_input_slowly(
    page: zendriver.Tab,
    input_element: Element,
    string: str,
):
    # Send characters one-by-one in a loop with sleeps.
    # This emulates natural typing better which defeats X bot detection.
    for char in string:
        await page.sleep(0.1)
        await input_element.send_keys(char)

    await page.sleep(0.1)
    await input_element.send_keys("\n")


async def _simulate_human_mouse_move(
    page: zendriver.Tab,
    input_element: Element,
) -> None:
    viewport = cast(
        dict[str, Any],
        await page.evaluate(
            "({width: window.innerWidth, height: window.innerHeight})",
        ),
    )
    width = viewport.get("width", 0)
    height = viewport.get("height", 0)

    if width > 0 and height > 0:
        steps = random.randint(6, 14)
        for _ in range(random.randint(3, 5)):
            x = random.uniform(width * 0.15, width * 0.85)
            y = random.uniform(height * 0.2, height * 0.8)
            await page.mouse_move(x, y, steps=steps)
            await page.sleep(random.uniform(0.05, 0.15))

    await input_element.mouse_move()
    await page.sleep(0.1)
    await input_element.mouse_click()


async def _login(
    page: zendriver.Tab,
    settings: XSettings,
    email_settings: EmailSettings | None,
) -> None:
    otp_start = time.time()
    page = await page.get("https://x.com/login")
    await _ensure_navigation(page, "https://x.com/login")
    try:
        username_input = await page.select('[autocomplete="username"]')
    except TimeoutError as exc:
        raise LoginRequiredError("X login form not available") from exc

    await _simulate_human_mouse_move(page, username_input)
    await _submit_input_slowly(page, username_input, settings.username)

    try:
        # If we trigger a bot detection input then submit the email address
        # into that for confirmation.
        confirmation_input = await page.select(
            '[data-testid="ocfEnterTextTextInput"]',
            timeout=5,
        )
        await _submit_input_slowly(page, confirmation_input, settings.email)
    except TimeoutError:
        pass

    try:
        password_input = await page.select(
            '[autocomplete="current-password"]',
            timeout=10,
        )
    except TimeoutError:
        password_input = await page.select(
            'input[name="password"]',
            timeout=10,
        )

    await _submit_input_slowly(page, password_input, settings.password)
    await _handle_otp(page, email_settings, otp_start)

    try:
        await page.wait_for_ready_state()
    except TimeoutError:
        pass


async def _handle_otp(
    page: zendriver.Tab,
    email_settings: EmailSettings | None,
    otp_start: float,
) -> None:
    selectors = (
        'input[name="challenge_response"]',
        'input[name="verification_code"]',
        'input[autocomplete="one-time-code"]',
        'input[inputmode="numeric"]',
    )

    try:
        otp_input = await page.select(",".join(selectors), timeout=2)
    except TimeoutError:
        return None

    if email_settings is None:
        raise LoginRequiredError("OTP required but email is not configured")

    filters = EmailSearchFilters(domain="x.com", since_epoch=otp_start)
    code = await wait_for_code(email_settings, filters)

    if not code:
        raise LoginRequiredError("OTP code not found in email")

    await _submit_input_slowly(page, otp_input, code)


async def _scroll(page: zendriver.Tab) -> None:
    await page.evaluate(
        "(() => {"
        "const viewport = window.innerHeight || 800;"
        "const step = Math.max(500, Math.floor(viewport * 1.5));"
        "const root = document.scrollingElement || document.documentElement;"
        "const maxTop = Math.max(0, root.scrollHeight - viewport);"
        "const nextTop = Math.min(root.scrollTop + step, maxTop);"
        "window.scrollTo(0, nextTop);"
        "})()",
    )


async def _drain_posts_and_assets(
    capture: NetworkCapture,
    seen_posts: set[str],
    seen_assets: set[str],
    *,
    timeout_seconds: float,
) -> tuple[list[RawPost], list[AssetDiscovery]]:
    responses = await capture.drain(timeout_seconds=max(0.05, timeout_seconds))
    logger.info("Drained %d X network responses", len(responses))

    return extract_posts_and_assets(responses, seen_posts, seen_assets)


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
    )
