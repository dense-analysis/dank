from __future__ import annotations

import asyncio
import datetime
import json
import logging
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from typing import cast
from urllib.parse import urlsplit

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

from .auto_login import sign_in_automatically
from .conversations import ConversationCapture, capture_conversation
from .navigation import (
    check_login_required as _check_login_required,
    navigate_to_account as _navigate_to_account,
    scroll as _scroll,
)
from .onepassword import load_credentials
from .payloads import (
    XAsset,
    XExtractedPost,
    account_timeline_post_ids,
    extract_posts_from_payload,
)
from .session import (
    LOGIN_NO_IMAP_MESSAGE,
    SESSION_CHECK_TIMEOUT_SECONDS,
    X_HOME_URL,
    LoginRequiredError,
    SignInPromptError,
    wait_for_x_session,
)

logger = logging.getLogger(__name__)

X_SOURCE = "x"
X_GRAPHQL_PATTERNS = (
    r"^https://x\.com/i/api/graphql/[^/?]+/"
    r"(?:UserTweets|UserTweetsAndReplies|UserOriginalsTimeline|"
    r"TweetDetail|UserMedia)(?:\?|$)",
)
ACCOUNT_TIMELINE_OPERATIONS = {"UserTweets", "UserOriginalsTimeline"}
# Bound browser steps without charging time spent saving yielded batches.
BROWSER_ACTION_TIMEOUT_SECONDS = 30
# Project heuristic: drain briefly while progressing, back off on idle scrolls.
FAST_SCROLL_PAUSE_SECONDS = 0.1
INITIAL_DRAIN_TIMEOUT_SECONDS = 0.05
MAX_IDLE_SCROLLS = 4


# Restart only the owned browser, with its existing profile and sign-in state.
BROWSER_RECOVERY_TIMEOUT_SECONDS = 20
RECOVERABLE_BROWSER_ERRORS = (TimeoutError, ConnectionError, ProtocolException)


class XBrowserRecoveryError(RuntimeError):
    pass


class _TimelineOverlap:
    def __init__(
        self, account: str, known_ids: set[str], threshold: int,
    ) -> None:
        self.handle = account.strip().lstrip("@").casefold()
        self.known_ids: set[str] = known_ids if threshold > 0 else set()
        self.threshold = threshold
        self.seen: set[str] = set()
        self.pinned: set[str] = set()
        self.consecutive = 0
        self.found_saved = False
        self.observed_timeline = False
        self.failed_timeline = False

    @property
    def reached(self) -> bool:
        return bool(self.known_ids) and self.consecutive >= self.threshold

    def observe(
        self, response: NetworkResponse, payload: Mapping[str, object] | None,
    ) -> None:
        operation = urlsplit(response.url).path.rsplit("/", 1)[-1]

        if operation not in ACCOUNT_TIMELINE_OPERATIONS:
            return

        if response.status != 200 or payload is None or payload.get("errors"):
            self.failed_timeline = True

            return

        for post_id in account_timeline_post_ids(
            payload, self.handle, pinned=self.pinned,
        ):
            self.observed_timeline = True

            if not self.known_ids:
                break

            if post_id in self.seen:
                continue

            self.seen.add(post_id)
            self.consecutive = (
                self.consecutive + 1 if post_id in self.known_ids else 0
            )

    def already_saved(self, post: XExtractedPost) -> bool:
        if post.is_reference or post.post_id not in self.known_ids:
            return False

        self.found_saved = True

        return True

    def record_check(self, on_checked: Callable[[str], None] | None) -> None:
        if on_checked and self.observed_timeline and not self.failed_timeline:
            on_checked(self.handle)


class XBrowser:
    def __init__(self, session: BrowserSession, settings: XSettings) -> None:
        self.session = session
        self.settings = settings
        self.page: zendriver.Tab | None = None
        self.sign_in_attempted = False

    async def get_page(self) -> zendriver.Tab:
        if self.page is not None:
            return self.page

        async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
            browser = await self.session.get_browser()
            page = await browser.get(X_HOME_URL)

        await self.check_session(page)

        return page

    async def check_session(self, page: zendriver.Tab) -> None:
        self.page = page
        await wait_for_x_session(
            page, timeout_seconds=SESSION_CHECK_TIMEOUT_SECONDS,
            stop_on_prompt=True,
        )

    async def recover_session(
        self, error: LoginRequiredError | BrowserProfileBusyError,
    ) -> None:
        page = self.page

        if not isinstance(error, SignInPromptError):
            raise error

        if error.stage == "email_code" and self.settings.imap is None:
            raise LoginRequiredError(LOGIN_NO_IMAP_MESSAGE) from None

        if (
            page is None or self.sign_in_attempted
            or (self.settings.credentials is None
                and self.settings.onepassword is None)
        ):
            raise error

        self.sign_in_attempted = True
        logger.info("X requested sign-in; checking configured credentials")
        credentials = self.settings.credentials

        if credentials is None:
            credentials = await load_credentials(self.settings.onepassword)

        if credentials is None:
            raise error

        await sign_in_automatically(
            page, credentials,
            email=self.settings.email, imap=self.settings.imap,
        )
        logger.info("X confirmed automatic sign-in; resuming collection")

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
    *,
    browser: XBrowser | None = None,
    known_ids: Mapping[str, set[str]] | None = None,
    on_checked: Callable[[str], None] | None = None,
) -> AsyncIterator[ScrapeBatch]:
    if not accounts:
        stats = current_source.get()

        if stats is not None:
            stats.skipped = True

        logger.warning("No X accounts configured")

        return

    browser = browser or XBrowser(session, settings)
    logger.info(
        "Collecting X timelines first; discussions are deferred. "
        "Use --x-details to refresh saved posts separately.",
    )

    index = 0
    history = known_ids or {}

    for index, account in enumerate(accounts, 1):
        logger.info(
            "X timeline [%d/%d] account=%s", index, len(accounts), account,
        )

        try:
            handle = account.strip().lstrip("@").casefold()

            async for batch in _collect_account(
                browser, account, settings, history.get(handle, set()),
                on_checked=on_checked,
            ):
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
    browser: XBrowser, account: str, settings: XSettings,
    known_ids: set[str], *, on_checked: Callable[[str], None] | None,
) -> AsyncIterator[ScrapeBatch]:
    seen_posts: set[str] = set()
    seen_references: set[str] = set()
    seen_assets: set[str] = set()

    browser_failure = False
    attempt = 0

    while True:
        attempt += 1
        overlap = _TimelineOverlap(account, known_ids, settings.known_overlap)

        try:
            page = await browser.get_page()

            async for batch in _scrape_account(
                page, account, settings, overlap=overlap,
            ):
                posts = [p for p in batch.posts if p.post_id not in (
                    seen_references if p.is_reference else seen_posts
                )]
                assets = [a for a in batch.assets if a.url not in seen_assets]
                seen_posts.update(
                    p.post_id for p in posts if not p.is_reference
                )
                seen_references.update(
                    p.post_id for p in posts if p.is_reference
                )
                seen_assets.update(a.url for a in assets)

                if posts or assets:
                    yield batch._replace(posts=posts, assets=assets)
        except (LoginRequiredError, BrowserProfileBusyError) as error:
            await browser.recover_session(error)
            count("retries")
        except (ConnectionError, ProtocolException) as error:
            logger.warning(
                "X timeline account=%s attempt=%d browser failure=%s; "
                "retained posts=%d", account, attempt,
                type(error).__name__, len(seen_posts),
            )
            await browser.reset()

            if not browser_failure:
                browser_failure = True
                count("retries")
                continue

            _account_failed(account, seen_posts, error, len(seen_references))

            return
        except Exception as error:
            # Timeouts/account errors do not establish that the tab is dead.
            # Preserve the session and in-flight assets for the next account.
            _account_failed(account, seen_posts, error, len(seen_references))

            return
        else:
            if not seen_posts and not overlap.found_saved:
                _account_failed(
                    account, seen_posts,
                    ValueError("No usable timeline posts returned"),
                    len(seen_references),
                )
            else:
                reason = (
                    "known account overlap" if overlap.reached
                    else "listing ended"
                )
                logger.info(
                    "X timeline account=%s outcome=completed "
                    "posts=%d references=%d attempts=%d reason=%s",
                    account, len(seen_posts), len(seen_references),
                    attempt, reason,
                )

                overlap.record_check(on_checked)

            return


def _account_failed(
    account: str, seen_posts: set[str], error: Exception, references: int,
) -> None:
    _record_failure(error)
    logger.warning(
        "X timeline account=%s outcome=%s posts=%d references=%d "
        "error=%s: %s; "
        "continuing with remaining accounts",
        account, "partial" if seen_posts else "failed", len(seen_posts),
        references,
        type(error).__name__, error,
    )


async def scrape_x_posts(
    settings: XSettings, posts: list[RawPost], session: BrowserSession,
) -> AsyncIterator[ScrapeBatch]:
    browser = XBrowser(session, settings)

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
                break

        except (BrowserProfileBusyError, XBrowserRecoveryError) as error:
            _record_failure(error)
            logger.warning("X discussions stopped: %s", error)
            break


async def _capture_saved_post(
    browser: XBrowser, post: RawPost, settings: XSettings,
) -> RawPost:
    reason = "disabled"

    if settings.conversation.enabled:
        try:
            try:
                page = await browser.get_page()
                return await capture_conversation(
                    page, post, settings.conversation, raise_on_sign_in=True,
                )
            except SignInPromptError as error:
                await browser.recover_session(error)
                count("retries")
                page = await browser.get_page()
                return await capture_conversation(
                    page, post, settings.conversation,
                )
        except LoginRequiredError as error:
            logger.warning("X discussion sign-in stopped: %s", error)
            reason = "login_required"
        except RECOVERABLE_BROWSER_ERRORS:
            reason = "timeout"
        except BrowserProfileBusyError:
            raise
        except Exception:
            logger.warning("X discussion browser unavailable", exc_info=True)
            reason = "unavailable"
        count("fetch_failures")

    return ConversationCapture(
        post, settings.conversation.max_replies,
    ).finish(reason)


async def _scrape_account(
    page: zendriver.Tab,
    account: str,
    settings: XSettings,
    *, overlap: _TimelineOverlap | None = None,
) -> AsyncIterator[ScrapeBatch]:
    limits = settings.scrape
    handle = account.strip().lstrip("@")
    if not handle:
        return

    logger.info("Starting X scrape for account=%s", handle)

    capture = NetworkCapture(page, X_GRAPHQL_PATTERNS)
    overlap = overlap or _TimelineOverlap(
        handle, set(), settings.known_overlap,
    )

    try:
        async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
            await capture.start()
            await _open_timeline(page, handle)

        seen_posts: set[str] = set()
        seen_references: set[str] = set()
        seen_assets: set[str] = set()
        idle_scrolls = 0

        posts, assets = await _drain_posts_and_assets(
            capture,
            seen_posts,
            seen_assets,
            seen_references,
            timeout_seconds=INITIAL_DRAIN_TIMEOUT_SECONDS,
            overlap=overlap,
        )
        logger.info(
            "Initial drain for %s produced posts=%d references=%d assets=%d",
            handle,
            sum(not post.is_reference for post in posts),
            sum(post.is_reference for post in posts),
            len(assets),
        )

        if posts or assets:
            yield ScrapeBatch(posts=posts, assets=assets)

        if overlap.reached or len(seen_posts) >= limits.max_posts:
            return

        for _ in range(limits.max_scrolls):
            previous = len(seen_posts)

            async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
                await _scroll(page)
                await _check_login_required(page)

            posts, assets = await _drain_posts_and_assets(
                capture,
                seen_posts,
                seen_assets,
                seen_references,
                timeout_seconds=_scroll_pause_seconds(
                    limits.scroll_pause_seconds,
                    idle_scrolls,
                ),
                overlap=overlap,
            )
            logger.info(
                "Scroll drain for %s produced posts=%d references=%d "
                "assets=%d idle=%d",
                handle,
                sum(not post.is_reference for post in posts),
                sum(post.is_reference for post in posts),
                len(assets),
                idle_scrolls,
            )

            if posts or assets:
                yield ScrapeBatch(posts=posts, assets=assets)

            if overlap.reached:
                return

            if len(seen_posts) > previous:
                idle_scrolls = 0
            else:
                idle_scrolls += 1

            if (len(seen_posts) >= limits.max_posts
                    or (idle_scrolls >= MAX_IDLE_SCROLLS and seen_posts)):
                break

        posts, assets = await _drain_posts_and_assets(
            capture,
            seen_posts,
            seen_assets,
            seen_references,
            timeout_seconds=min(
                limits.scroll_pause_seconds,
                FAST_SCROLL_PAUSE_SECONDS,
            ),
            overlap=overlap,
        )
        logger.info(
            "Trailing drain for %s produced posts=%d references=%d "
            "assets=%d total_posts=%d",
            handle,
            sum(not post.is_reference for post in posts),
            sum(post.is_reference for post in posts),
            len(assets),
            len(seen_posts),
        )

        if posts or assets:
            yield ScrapeBatch(posts=posts, assets=assets)
    finally:
        await capture.stop()


async def _drain_posts_and_assets(
    capture: NetworkCapture,
    seen_posts: set[str],
    seen_assets: set[str],
    seen_references: set[str],
    *,
    timeout_seconds: float,
    overlap: _TimelineOverlap,
) -> tuple[list[RawPost], list[AssetDiscovery]]:
    async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
        responses = await capture.drain(
            timeout_seconds=max(0.05, timeout_seconds),
            wait_for_idle=False,
        )
    logger.info("Drained %d X network responses", len(responses))

    return extract_posts_and_assets(
        responses,
        seen_posts,
        seen_assets,
        seen_references,
        overlap=overlap,
    )


async def _open_timeline(page: zendriver.Tab, handle: str) -> None:
    try:
        await _navigate_to_account(page, handle)
    except TimeoutError:
        await _check_login_required(page)
        raise

    await _check_login_required(page)


def _scroll_pause_seconds(configured_pause: float, idle_scrolls: int) -> float:
    if configured_pause <= 0:
        return FAST_SCROLL_PAUSE_SECONDS

    if idle_scrolls >= 2:
        return configured_pause

    return min(configured_pause, FAST_SCROLL_PAUSE_SECONDS)


def _response_payload(response: NetworkResponse) -> dict[str, object] | None:
    try:
        payload = json.loads(response.body)
    except json.JSONDecodeError:
        logger.warning(
            "Skipping non-JSON X response request_id=%s url=%s",
            response.request_id, response.url,
        )

        return None

    if not isinstance(payload, dict):
        logger.warning(
            "Skipping non-object X payload request_id=%s url=%s",
            response.request_id, response.url,
        )

        return None

    return cast(dict[str, object], payload)


def extract_posts_and_assets(
    responses: Iterable[NetworkResponse],
    seen_posts: set[str],
    seen_assets: set[str],
    seen_references: set[str],
    *, overlap: _TimelineOverlap | None = None,
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

        payload = _response_payload(response)

        if overlap is not None:
            overlap.observe(response, payload)

        if payload is None:
            continue

        extracted_posts = extract_posts_from_payload(payload)
        logger.info(
            "Parsed %d posts and %d ID-only references from X response "
            "request_id=%s",
            sum(not post.is_reference for post in extracted_posts),
            sum(post.is_reference for post in extracted_posts),
            response.request_id,
        )

        for extracted in extracted_posts:
            seen = seen_references if extracted.is_reference else seen_posts

            if extracted.post_id not in seen:
                seen.add(extracted.post_id)

                if overlap is None or not overlap.already_saved(extracted):
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
        "Extracted %d new posts, %d ID-only references and %d new assets "
        "from drained responses",
        sum(not post.is_reference for post in posts),
        sum(post.is_reference for post in posts),
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
        is_reference=extracted.is_reference,
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
