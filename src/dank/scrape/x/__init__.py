from __future__ import annotations

import asyncio
import datetime
import json
import logging
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from itertools import islice
from typing import NamedTuple, cast
from urllib.parse import parse_qs, urlsplit

import zendriver
from zendriver.core.connection import ProtocolException

from dank.config import BrowserScrapeSettings, XSettings
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
    account_profile_id,
    account_timeline_ended,
    account_timeline_post_ids,
    extract_posts_from_payload,
    matches_account_timeline,
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
ACCOUNT_CAPTURE_PATTERNS = (
    *X_GRAPHQL_PATTERNS,
    r"^https://x\.com/i/api/graphql/[^/?]+/UserByScreenName(?:\?|$)",
)
# Bound browser steps without charging time spent saving yielded batches.
BROWSER_ACTION_TIMEOUT_SECONDS = 30
# Project heuristic: drain briefly while progressing, back off on idle scrolls.
FAST_SCROLL_PAUSE_SECONDS = 0.1
FIRST_REQUEST_GRACE_SECONDS = 2.0
MAX_IDLE_SCROLLS = 4
COMPLETE_TIMELINE_REASONS = {
    "known account overlap", "timeline end", "initial coverage window",
}


# Restart only the owned browser, with its existing profile and sign-in state.
BROWSER_RECOVERY_TIMEOUT_SECONDS = 20
RECOVERABLE_BROWSER_ERRORS = (TimeoutError, ConnectionError, ProtocolException)


class XBrowserRecoveryError(RuntimeError):
    pass


class TimelineCoverage(NamedTuple):
    checkpoint: tuple[str, ...] = ()
    since: datetime.datetime | None = None


class _TimelineOverlap:
    def __init__(
        self, account: str, known_ids: set[str], threshold: int,
        coverage: TimelineCoverage | None = None,
    ) -> None:
        self.handle = account.strip().lstrip("@").casefold()
        self.known_ids: set[str] = known_ids if threshold > 0 else set()
        self.threshold = threshold
        self.checkpoint = set(coverage.checkpoint) if coverage else known_ids
        self.since = coverage.since if coverage else None
        self.seen: dict[str, None] = {}
        self.captured_posts: set[str] = set()
        self.captured_assets: set[str] = set()
        self.captured_references: set[str] = set()
        self.pinned: set[str] = set()
        self.consecutive = 0
        self.found_saved = False
        self.observed_timeline = False
        self.failed_timeline = False
        self.ended = False
        self.covered_window = False
        self.user_id: str | None = None
        self.started_at_head = False
        self.pages = 0
        self.initial_scrolls = 0
        self.reason = "unverified timeline"

    @property
    def reached(self) -> bool:
        return bool(self.checkpoint) and 0 < self.threshold <= self.consecutive

    def matches_request(self, url: str) -> bool:
        operation = urlsplit(url).path.rsplit("/", 1)[-1]

        return operation in ACCOUNT_TIMELINE_OPERATIONS and (
            self.user_id is None or _timeline_user_id(url) == self.user_id
        )

    def observe(
        self, response: NetworkResponse, payload: Mapping[str, object] | None,
    ) -> bool:
        operation = urlsplit(response.url).path.rsplit("/", 1)[-1]

        if (operation == "UserByScreenName" and not self.user_id
                and response.status == 200 and payload
                and not payload.get("errors")):
            self.user_id = account_profile_id(payload, self.handle)

        if not self.matches_request(response.url):
            return False

        if response.status != 200 or payload is None:
            if self.user_id is not None or self.observed_timeline:
                self.failed_timeline = True

            return False

        if self.user_id is None:
            if not matches_account_timeline(payload, self.handle):
                return False

            self.user_id = _timeline_user_id(response.url)

        self.failed_timeline |= bool(payload.get("errors"))
        self.pages += 1
        ended = account_timeline_ended(payload)
        post_ids = tuple(account_timeline_post_ids(
            payload, self.handle, pinned=self.pinned,
        ))
        self.ended |= ended
        self.observed_timeline |= ended or bool(post_ids)
        self.started_at_head |= (
            (ended or bool(post_ids))
            and not _timeline_variables(response.url).get("cursor")
        )

        for post_id in post_ids:
            if post_id in self.seen:
                continue

            self.seen[post_id] = None
            self.consecutive = (
                self.consecutive + 1 if post_id in self.checkpoint else 0
            )

        return True

    def already_saved(self, post: XExtractedPost) -> bool:
        if not post.is_reference and post.author.casefold() == self.handle:
            self.observed_timeline = True

        if (self.since is not None and post.post_id in self.seen
                and post.created_at is not None
                and post.created_at <= self.since):
            self.covered_window = True

        if post.is_reference or post.post_id not in self.known_ids:
            return False

        self.found_saved = True

        return True

    def record_check(
        self, on_checked: Callable[[str, tuple[str, ...]], None] | None,
    ) -> None:
        if on_checked and self.reason in COMPLETE_TIMELINE_REASONS:
            head = tuple(islice(self.seen, max(20, self.threshold)))
            on_checked(self.handle, head)

    def coverage_reason(self) -> str | None:
        if self.failed_timeline:
            return "timeline response error"

        if not self.started_at_head:
            return None

        if self.ended:
            return "timeline end"

        if self.reached:
            return "known account overlap"

        if self.covered_window:
            return "initial coverage window"

        return None


def _timeline_variables(url: str) -> Mapping[str, object]:
    query = parse_qs(urlsplit(url).query)

    try:
        variables = json.loads(query["variables"][0])
    except (KeyError, json.JSONDecodeError):
        return {}

    return (cast(dict[str, object], variables)
            if isinstance(variables, dict) else {})


def _timeline_user_id(url: str) -> str | None:
    user_id = _timeline_variables(url).get("userId")

    return user_id if isinstance(user_id, str) else None


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
    coverage: Mapping[str, TimelineCoverage] | None = None,
    on_checked: Callable[[str, tuple[str, ...]], None] | None = None,
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
                coverage=coverage.get(handle) if coverage else None,
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
    known_ids: set[str], *, coverage: TimelineCoverage | None,
    on_checked: Callable[[str, tuple[str, ...]], None] | None,
) -> AsyncIterator[ScrapeBatch]:
    seen_posts: set[str] = set()
    seen_references: set[str] = set()
    seen_assets: set[str] = set()

    browser_failure = False
    attempt = 0

    while True:
        attempt += 1
        overlap = _TimelineOverlap(
            account, known_ids, settings.known_overlap, coverage,
        )

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
            _finish_account(
                account, overlap, seen_posts, len(seen_references),
                attempt, on_checked,
            )
            return


def _finish_account(
    account: str, overlap: _TimelineOverlap, seen_posts: set[str],
    references: int, attempt: int,
    on_checked: Callable[[str, tuple[str, ...]], None] | None,
) -> None:
    complete = overlap.reason in COMPLETE_TIMELINE_REASONS

    if not complete and not seen_posts and not overlap.found_saved:
        _account_failed(
            account, seen_posts,
            ValueError(f"No usable timeline posts: {overlap.reason}"),
            references,
        )

        return

    if not complete:
        count("fetch_failures")

    logger.info(
        "X timeline account=%s outcome=%s posts=%d references=%d "
        "attempts=%d reason=%s pages=%d captured_head=%s",
        account, "completed" if complete else "partial",
        len(seen_posts), references, attempt, overlap.reason,
        overlap.pages, overlap.started_at_head,
    )
    overlap.record_check(on_checked)


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

    capture = NetworkCapture(
        page, ACCOUNT_CAPTURE_PATTERNS, track_requests=True,
    )
    overlap = overlap or _TimelineOverlap(
        handle, set(), settings.known_overlap,
    )

    try:
        async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
            await capture.start()
            await _open_timeline(page, handle)

        batch = await _first_timeline_batch(page, capture, overlap, limits)
        logger.info(
            "Initial drain for %s produced posts=%d references=%d assets=%d",
            handle, batch.post_count, batch.reference_count, len(batch.assets),
        )

        if batch.posts or batch.assets:
            yield batch

        async for batch in _scroll_account(page, capture, overlap, limits):
            yield batch
    finally:
        await capture.stop()


async def _first_timeline_batch(
    page: zendriver.Tab, capture: NetworkCapture, overlap: _TimelineOverlap,
    limits: BrowserScrapeSettings,
) -> ScrapeBatch:
    loop = asyncio.get_running_loop()
    grace_deadline = loop.time() + FIRST_REQUEST_GRACE_SECONDS
    can_scroll = limits.max_scrolls > 0

    try:
        async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
            while True:
                wait = (max(0, grace_deadline - loop.time()) if can_scroll
                        else BROWSER_ACTION_TIMEOUT_SECONDS)
                batch = await _drain_posts_and_assets(
                    capture, timeout_seconds=wait,
                    overlap=overlap,
                )

                if batch.posts or batch.assets or overlap.observed_timeline:
                    return batch

                if overlap.failed_timeline:
                    raise ValueError("X returned an initial timeline error")

                if can_scroll and loop.time() >= grace_deadline:
                    can_scroll = False

                    if not capture.has_pending(overlap.matches_request):
                        # A profile revisit can show cached posts without a
                        # request. Ask the page for pagination once.
                        await _scroll(page)
                        overlap.initial_scrolls = 1

                await _check_login_required(page)
                await asyncio.sleep(0)
    except TimeoutError as error:
        await _check_login_required(page)
        raise TimeoutError(
            f"Waiting for the first X timeline response for @{overlap.handle}",
        ) from error


async def _scroll_account(
    page: zendriver.Tab, capture: NetworkCapture, overlap: _TimelineOverlap,
    limits: BrowserScrapeSettings,
) -> AsyncIterator[ScrapeBatch]:
    idle_scrolls = 0

    for step in range(overlap.initial_scrolls, limits.max_scrolls + 1):
        settled = True
        previous = len(overlap.captured_posts)

        while capture.has_ready or capture.has_pending(
            overlap.matches_request,
        ):
            settled = await capture.wait_for_pending(
                overlap.matches_request,
                timeout_seconds=BROWSER_ACTION_TIMEOUT_SECONDS,
            )
            batch = await _drain_posts_and_assets(
                capture, timeout_seconds=0, overlap=overlap,
            )

            if batch.posts or batch.assets:
                yield batch

            if not settled:
                break

        if len(overlap.captured_posts) > previous:
            idle_scrolls = 0

        reason = _timeline_stop_reason(overlap, limits, step, idle_scrolls)

        if not settled or reason:
            overlap.reason = reason if settled else "timeline response timeout"

            return

        previous = len(overlap.captured_posts)

        async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
            await _scroll(page)
            await _check_login_required(page)

        batch = await _drain_posts_and_assets(
            capture, timeout_seconds=_scroll_pause_seconds(
                limits.scroll_pause_seconds, idle_scrolls,
            ), overlap=overlap,
        )
        logger.info(
            "Scroll drain for %s produced posts=%d references=%d "
            "assets=%d idle=%d",
            overlap.handle, batch.post_count, batch.reference_count,
            len(batch.assets), idle_scrolls,
        )

        if batch.posts or batch.assets:
            yield batch

        idle_scrolls = (
            0 if len(overlap.captured_posts) > previous else idle_scrolls + 1
        )


def _timeline_stop_reason(
    overlap: _TimelineOverlap, limits: BrowserScrapeSettings,
    step: int, idle_scrolls: int,
) -> str:
    reason = overlap.coverage_reason()

    if reason:
        return reason

    if len(overlap.captured_posts) >= limits.max_posts:
        return "post limit"

    if idle_scrolls >= MAX_IDLE_SCROLLS:
        return "idle scroll limit"

    if step >= limits.max_scrolls:
        return "scroll limit"

    return ""


async def _drain_posts_and_assets(
    capture: NetworkCapture,
    *,
    timeout_seconds: float,
    overlap: _TimelineOverlap,
) -> ScrapeBatch:
    async with asyncio.timeout(BROWSER_ACTION_TIMEOUT_SECONDS):
        responses = await capture.drain(
            timeout_seconds=timeout_seconds,
            wait_for_idle=False,
        )

    logger.info("Drained %d X network responses", len(responses))
    posts, assets = extract_posts_and_assets(
        responses, overlap.captured_posts, overlap.captured_assets,
        overlap.captured_references, overlap=overlap,
    )

    return ScrapeBatch(posts=posts, assets=assets)


async def _open_timeline(page: zendriver.Tab, handle: str) -> None:
    try:
        await _navigate_to_account(page, handle)
    except TimeoutError:
        await _check_login_required(page)
        raise


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

        if overlap is not None and not overlap.observe(response, payload):
            continue

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
