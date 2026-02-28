from __future__ import annotations

import datetime
import json
import logging
import random
import time
from collections.abc import AsyncIterator, Iterable
from typing import Any, NamedTuple, cast
from urllib.parse import quote, urlsplit

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
INITIAL_DRAIN_TIMEOUT_SECONDS = 0.05
MAX_IDLE_SCROLLS = 4
READY_STATE_TIMEOUT_SECONDS = 2
LOGIN_PROMPT_TIMEOUT_SECONDS = 0.25
REUSE_READY_STATE_TIMEOUT_SECONDS = 0.12
ACCOUNT_JUMP_OPEN_WAIT_SECONDS = 0.04
ACCOUNT_JUMP_TYPEAHEAD_WAIT_SECONDS = 0.18
ACCOUNT_JUMP_CONFIRM_WAIT_SECONDS = 0.04
ACCOUNT_JUMP_HISTORY_TIMEOUT_SECONDS = 1.25
ACCOUNT_CONTENT_TIMEOUT_SECONDS = 1.5


class HistorySnapshot(NamedTuple):
    path: str
    event_count: int


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
        page = await _open_account_page(page, handle)

        if await _is_login_page(page):
            await _login(page, settings, email_settings)
            page = await _open_account_page(page, handle)

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


async def _open_account_page(
    page: zendriver.Tab,
    handle: str,
) -> zendriver.Tab:
    url = f"https://x.com/{quote(handle)}"

    if await _navigate_using_x_account_jump(page, handle):
        logger.info("Used in-page account jump for account=%s", handle)
        return page

    page = await page.get(url)
    await _ensure_navigation(page, url)

    return page


async def _navigate_using_x_account_jump(
    page: zendriver.Tab,
    handle: str,
) -> bool:
    location = await _get_location(page)

    if not _is_x_location(location) or _is_login_location(location):
        return False

    await page.sleep(REUSE_READY_STATE_TIMEOUT_SECONDS)
    expected_path = _normalize_x_path(f"/{handle}")
    baseline_count = await _install_history_tracker(page)

    if baseline_count is None:
        return False

    keyboard_target = await _select_keyboard_target(page)

    if keyboard_target is None:
        return False

    await keyboard_target.send_keys("/")
    await page.sleep(ACCOUNT_JUMP_OPEN_WAIT_SECONDS)
    await keyboard_target.send_keys(f"@{handle}")
    await page.sleep(ACCOUNT_JUMP_TYPEAHEAD_WAIT_SECONDS)
    await keyboard_target.send_keys(zendriver.SpecialKeys.ARROW_UP)
    await page.sleep(ACCOUNT_JUMP_CONFIRM_WAIT_SECONDS)
    await keyboard_target.send_keys(zendriver.SpecialKeys.ENTER)

    if not await _wait_for_history_path(
        page,
        expected_path=expected_path,
        baseline_count=baseline_count,
    ):
        return False

    await _wait_for_account_content(page, expected_path=expected_path)

    return True


def _is_x_location(location: str | None) -> bool:
    if not location:
        return False

    parsed = urlsplit(location)

    return (parsed.hostname or "").lower() in {"x.com", "www.x.com"}


def _is_login_location(location: str | None) -> bool:
    if not location:
        return False

    path = urlsplit(location).path.lower()

    return "/i/flow/login" in path or "/login" in path


def _normalize_x_path(path: str) -> str:
    normalized = path.strip().lower()

    if not normalized:
        return "/"

    if not normalized.startswith("/"):
        normalized = "/" + normalized

    if normalized != "/":
        normalized = normalized.rstrip("/")

    return normalized


async def _install_history_tracker(page: zendriver.Tab) -> int | None:
    script = (
        "(() => {"
        "if (!Array.isArray(window.__dankHistoryEvents)) {"
        "const events = [];"
        "const trim = () => {"
        "if (events.length > 64) { events.shift(); }"
        "};"
        "const record = (kind) => {"
        "events.push({kind, path: window.location.pathname});"
        "trim();"
        "};"
        "const pushState = history.pushState.bind(history);"
        "history.pushState = (...args) => {"
        "const out = pushState(...args);"
        "record('pushState');"
        "return out;"
        "};"
        "const replaceState = history.replaceState.bind(history);"
        "history.replaceState = (...args) => {"
        "const out = replaceState(...args);"
        "record('replaceState');"
        "return out;"
        "};"
        "window.addEventListener('popstate', () => {"
        "record('popstate');"
        "});"
        "window.__dankHistoryEvents = events;"
        "record('install');"
        "}"
        "return window.__dankHistoryEvents.length;"
        "})()"
    )

    try:
        result = await page.evaluate(script)
    except Exception:
        return None

    if not isinstance(result, int):
        return None

    return result


async def _history_snapshot(page: zendriver.Tab) -> HistorySnapshot | None:
    try:
        value = await page.evaluate(
            "(() => ({"
            "path: window.location.pathname,"
            "event_count: Array.isArray(window.__dankHistoryEvents)"
            " ? window.__dankHistoryEvents.length"
            " : 0"
            "}))()",
        )
    except Exception:
        return None

    if not isinstance(value, dict):
        return None

    value = cast(dict[str, object], value)

    path = value.get("path")
    event_count = value.get("event_count")

    if not isinstance(path, str):
        return None

    if not isinstance(event_count, int):
        return None

    return HistorySnapshot(
        path=_normalize_x_path(path),
        event_count=event_count,
    )


async def _wait_for_history_path(
    page: zendriver.Tab,
    *,
    expected_path: str,
    baseline_count: int,
) -> bool:
    deadline = time.monotonic() + ACCOUNT_JUMP_HISTORY_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        snapshot = await _history_snapshot(page)

        if (
            snapshot is not None
            and snapshot.event_count > baseline_count
            and snapshot.path == expected_path
        ):
            return True

        await page.sleep(0.04)

    return False


async def _select_keyboard_target(page: zendriver.Tab) -> Element | None:
    for selector in ("body", "main", "html"):
        try:
            element = await page.select(selector, timeout=0.1)
        except TimeoutError:
            continue
        else:
            return element

    return None


async def _wait_for_account_content(
    page: zendriver.Tab,
    *,
    expected_path: str,
) -> None:
    await page.sleep(0.5)

    try:
        await page.select(
            '[data-testid="primaryColumn"]',
            timeout=ACCOUNT_CONTENT_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.debug(
            "X account content did not fully load expected_path=%s",
            expected_path,
        )

async def _is_login_page(page: zendriver.Tab) -> bool:
    location = await _get_location(page)

    if not location:
        return await _has_login_prompt(page)

    if _is_login_location(location):
        return True

    return False


async def _ensure_navigation(page: zendriver.Tab, url: str) -> None:
    await _wait_for_ready_state(page)

    location = await _get_location(page)

    if location and location != "about:blank":
        return

    try:
        await page.send(cdp.page.navigate(url))
    except Exception:
        return

    await _wait_for_ready_state(page)


async def _wait_for_ready_state(page: zendriver.Tab) -> None:
    try:
        # Bound ready-state waits because X keeps active requests open.
        await page.wait_for_ready_state(timeout=READY_STATE_TIMEOUT_SECONDS)
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
    # Only wait briefly for a login selector to appear.
    try:
        await page.select(
            'a[href="/i/flow/signup"]',
            timeout=LOGIN_PROMPT_TIMEOUT_SECONDS,
        )
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
    await _wait_for_ready_state(page)


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
