from __future__ import annotations

import asyncio
import datetime
import json
import logging
import pathlib
import random
import time
from collections.abc import AsyncIterator, Iterable
from typing import Any, NamedTuple, cast
from urllib.parse import quote, urlparse

import aiohttp
import zendriver
from zendriver import Element, cdp

from dank.config import EmailSettings, XSettings
from dank.model import RawAsset, RawPost
from dank.scrape.imap_email import EmailSearchFilters, wait_for_code
from dank.scrape.x_payloads import (
    XAsset,
    XExtractedPost,
    extract_posts_from_payload,
)
from dank.scrape.zendriver import (
    BrowserSession,
    NetworkCapture,
    NetworkResponse,
)

logger = logging.getLogger(__name__)

X_SOURCE = "x"
X_GRAPHQL_PATTERNS = (
    r"https://x\.com/i/api/graphql/.+/UserTweets",
    r"https://x\.com/i/api/graphql/.+/UserTweetsAndReplies",
    r"https://x\.com/i/api/graphql/.+/TweetDetail",
)


async def scrape_accounts(
    settings: XSettings,
    accounts: tuple[str, ...],
    email_settings: EmailSettings | None,
    assets_dir: pathlib.Path,
    session: BrowserSession,
    *,
    max_asset_bytes: int | None = None,
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
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
    ) as http_client:
        for account in accounts:
            try:
                async for batch in _scrape_account(
                    page,
                    account,
                    settings,
                    email_settings,
                    assets_dir,
                    http_client,
                    max_asset_bytes=max_asset_bytes,
                ):
                    yield batch
            except LoginRequiredError:
                logger.warning(
                    "X login required; stopping scrape.",
                )
                return


class ScrapeBatch(NamedTuple):
    posts: list[RawPost]
    assets: list[RawAsset]


async def _scrape_account(
    page: zendriver.Tab,
    account: str,
    settings: XSettings,
    email_settings: EmailSettings | None,
    assets_dir: pathlib.Path,
    http_client: aiohttp.ClientSession,
    *,
    max_asset_bytes: int | None = None,
) -> AsyncIterator[ScrapeBatch]:
    handle = account.strip("@").strip()
    if not handle:
        return
    url = f"https://x.com/{quote(handle)}"
    page = await page.get(url)
    await _ensure_navigation(page, url)

    if await _is_login_page(page):
        await _login(page, settings, email_settings)
        page = await page.get(url)
        await _ensure_navigation(page, url)

    capture = NetworkCapture(page, X_GRAPHQL_PATTERNS)
    await capture.start()

    try:
        seen_posts: set[str] = set()
        seen_assets: set[str] = set()
        total_posts = 0

        for _ in range(settings.max_scrolls):
            await _scroll(page)
            responses = await capture.drain(
                timeout_seconds=settings.scroll_pause_seconds,
            )
            posts, assets = _extract_posts_and_assets(
                responses,
                seen_posts,
                seen_assets,
            )
            total_posts += len(posts)
            downloaded = await _download_assets(
                assets,
                assets_dir,
                http_client,
                max_asset_bytes=max_asset_bytes,
            )

            if posts or downloaded:
                yield ScrapeBatch(posts=posts, assets=downloaded)

            if total_posts >= settings.max_posts:
                break

        trailing = await capture.drain(
            timeout_seconds=settings.scroll_pause_seconds,
        )
        posts, assets = _extract_posts_and_assets(
            trailing,
            seen_posts,
            seen_assets,
        )
        downloaded = await _download_assets(
            assets,
            assets_dir,
            http_client,
            max_asset_bytes=max_asset_bytes,
        )

        if posts or downloaded:
            yield ScrapeBatch(posts=posts, assets=downloaded)
    finally:
        await capture.stop()


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
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")


def _extract_posts_and_assets(
    responses: Iterable[NetworkResponse],
    seen_posts: set[str],
    seen_assets: set[str],
) -> tuple[list[RawPost], list[RawAsset]]:
    posts: list[RawPost] = []
    assets: list[RawAsset] = []

    for response in responses:
        try:
            payload = json.loads(response.body)
        except json.JSONDecodeError:
            continue

        if not isinstance(payload, dict):
            continue

        payload = cast(dict[str, object], payload)

        for extracted in extract_posts_from_payload(payload):
            if extracted.post_id not in seen_posts:
                seen_posts.add(extracted.post_id)
                posts.append(
                    _raw_post_from_extracted(extracted, response.url),
                )

                for asset in extracted.assets:
                    if asset.url not in seen_assets:
                        seen_assets.add(asset.url)
                        assets.append(
                            _raw_asset_from_extracted(extracted, asset),
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


def _raw_asset_from_extracted(
    extracted: XExtractedPost,
    asset: XAsset,
) -> RawAsset:
    return RawAsset(
        domain="x.com",
        post_id=extracted.post_id,
        url=asset.url,
        asset_type=asset.asset_type,
        scraped_at=datetime.datetime.now(datetime.UTC),
        source=X_SOURCE,
        local_path="",
    )


async def _download_assets(
    assets: list[RawAsset],
    assets_dir: pathlib.Path,
    client: aiohttp.ClientSession,
    *,
    concurrency: int = 4,
    max_asset_bytes: int | None = None,
) -> list[RawAsset]:
    semaphore = asyncio.Semaphore(concurrency)

    async def _download_asset(asset: RawAsset) -> RawAsset | None:
        if asset.asset_type == "link":
            return asset

        parsed = urlparse(asset.url)
        filename = pathlib.Path(parsed.path).name or "asset"
        target_dir = assets_dir / "x.com" / asset.post_id
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / filename

        if target_path.exists():
            return asset._replace(local_path=str(target_path))

        temp_path = target_path.with_suffix(f"{target_path.suffix}.part")
        async with semaphore:
            try:
                async with client.get(asset.url) as response:
                    response.raise_for_status()

                    if max_asset_bytes is not None:
                        content_length = response.content_length

                        if (
                            content_length is not None
                            and content_length > max_asset_bytes
                        ):
                            logger.debug(
                                "Skipping asset larger than limit: %s",
                                asset.url,
                            )
                            temp_path.unlink(missing_ok=True)

                            return asset

                    bytes_read = 0
                    exceeded_limit = False

                    with temp_path.open("wb") as file:
                        async for chunk in response.content.iter_chunked(
                            65536
                        ):
                            if not chunk:
                                continue

                            bytes_read += len(chunk)

                            if (
                                max_asset_bytes is not None
                                and bytes_read > max_asset_bytes
                            ):
                                exceeded_limit = True
                                break

                            file.write(chunk)

                    if exceeded_limit:
                        temp_path.unlink(missing_ok=True)
                        logger.debug(
                            "Skipping asset larger than limit: %s",
                            asset.url,
                        )

                        return asset
            except Exception:
                temp_path.unlink(missing_ok=True)
                logger.debug("Failed to download asset", exc_info=True)

                return None

        temp_path.replace(target_path)

        return asset._replace(local_path=str(target_path))

    results = await asyncio.gather(
        *(_download_asset(asset) for asset in assets),
    )

    return [r for r in results if r is not None]
