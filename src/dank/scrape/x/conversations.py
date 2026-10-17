from __future__ import annotations

import asyncio
import datetime
import json
import logging
import re
from collections.abc import Iterable
from typing import NamedTuple, cast
from urllib.parse import parse_qs, urlsplit

import zendriver

from dank.config import XConversationSettings
from dank.model import RawPost
from dank.scrape.metrics import count
from dank.scrape.zendriver import NetworkCapture, NetworkResponse

from .navigation import (
    check_login_required,
    ensure_navigation,
    scroll_conversation,
)
from .session import LoginRequiredError

logger = logging.getLogger(__name__)
DETAIL_PATTERNS = (
    r"^https://x\.com/i/api/graphql/[^/?]+/TweetDetail(?:\?|$)",
)
# A short quiet period lets each scroll's response arrive without busy polling.
RESPONSE_WAIT_SECONDS = 1.5
CLEANUP_TIMEOUT_SECONDS = 2


def canonical_post_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise ValueError("Expected an X post URL") from None

    match = re.fullmatch(
        r"/([A-Za-z0-9_]{1,15}|i)/status/([1-9][0-9]*)/?", parsed.path,
    )

    if (
        parsed.scheme not in {"https", "http"}
        or parsed.netloc.lower()
        not in {
            "x.com",
            "www.x.com",
            "twitter.com",
            "www.twitter.com",
        }
        or match is None
    ):
        raise ValueError("Expected an x.com or twitter.com post URL")

    return f"https://x.com/{match[1]}/status/{match[2]}"


def _object(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _identifier(tweet: dict[str, object]) -> str:
    value = tweet.get("rest_id") or _object(tweet.get("legacy")).get("id_str")

    return value if isinstance(value, str) and value.isdecimal() else ""


def _parent(tweet: dict[str, object]) -> str:
    value = _object(tweet.get("legacy")).get("in_reply_to_status_id_str")

    return value if isinstance(value, str) else ""


def _conversation(tweet: dict[str, object]) -> str:
    value = _object(tweet.get("legacy")).get("conversation_id_str")

    return (
        value if isinstance(value, str) and value.isascii()
        and value.isdecimal() else ""
    )


def _tweet(item: dict[str, object]) -> dict[str, object]:
    # Only timeline slots count. Never traverse quoted/retweeted tweet results.
    if "promotedMetadata" in item or item.get("itemType") != "TimelineTweet":
        return {}

    result = _object(_object(item.get("tweet_results")).get("result"))

    for _ in range(3):
        if _identifier(result) and isinstance(result.get("legacy"), dict):
            return result

        result = _object(result.get("tweet") or result.get("result"))

    return {}


class DetailPage(NamedTuple):
    posts: tuple[dict[str, object], ...]
    has_more: bool
    unexpanded: bool
    terminated: bool


def parse_detail_page(payload: object) -> DetailPage | None:
    data = _object(_object(payload).get("data"))
    timeline = _object(data.get("threaded_conversation_with_injections_v2"))
    instructions = timeline.get("instructions")

    if not isinstance(instructions, list):
        return None

    posts: list[dict[str, object]] = []
    items, has_more = _detail_items(cast(list[object], instructions))
    unexpanded = False

    for value in items:
        item = _object(value)
        item = _object(item.get("item")) or item
        content = _object(item.get("itemContent"))
        result = _tweet(content)

        if result:
            posts.append(result)
        elif content.get("itemType") in {
            "TimelineTombstone",
            "TimelineTimelineCursor",
            "TimelinePrompt",
        } or content.get("cursorType") in {"ShowMore", "ShowMoreThreads"}:
            unexpanded = True

    terminated = any(
        _object(instruction).get("type") == "TimelineTerminateTimeline"
        and _object(instruction).get("direction") == "Bottom"
        for instruction in cast(list[object], instructions)
    )

    return DetailPage(tuple(posts), has_more, unexpanded, terminated)


def _detail_items(instructions: list[object]) -> tuple[list[object], bool]:
    has_more = False
    items: list[object] = []

    for instruction in instructions:
        node = _object(instruction)

        if node.get("type") in {"TimelineAddEntries", "TimelineReplaceEntry"}:
            entries = node.get("entries", [node.get("entry")])

            for entry in (
                cast(list[object], entries)
                if isinstance(entries, list)
                else []
            ):
                content = _object(_object(entry).get("content"))
                cursor_type = str(content.get("cursorType", "")).lower()

                if cursor_type in {
                    "bottom",
                    "showmore",
                    "showmorethreads",
                } and content.get("value"):
                    has_more = True

                if content.get("entryType") == "TimelineTimelineModule":
                    module_items = content.get("items")

                    if isinstance(module_items, list):
                        items.extend(cast(list[object], module_items))

                elif content.get("entryType") == "TimelineTimelineItem":
                    items.append(content)

        elif node.get("type") == "TimelineAddToModule":
            module_items = node.get("moduleItems")

            if isinstance(module_items, list):
                items.extend(cast(list[object], module_items))

    return items, has_more


class ConversationCapture:
    """Validated capture state retained even if browser I/O times out."""

    def __init__(self, seed: RawPost, limit: int) -> None:
        self.seed = seed
        self.limit = limit
        self.target: dict[str, object] | None = None
        self.posts: dict[str, dict[str, object]] = {}
        self.has_more = False
        self.unexpanded = False
        self.limited = False
        self.error: str | None = None
        self.request_url = seed.request_url

    def accept(self, responses: Iterable[NetworkResponse]) -> None:
        for response in responses:
            if not re.match(DETAIL_PATTERNS[0], response.url):
                continue

            try:
                variables = json.loads(
                    parse_qs(urlsplit(response.url).query).get(
                        "variables", ["{}"],
                    )[0],
                )
            except (ValueError, TypeError):
                continue

            if _object(variables).get("focalTweetId") != self.seed.post_id:
                continue

            if response.status >= 400:
                self.error = (
                    "rate_limited" if response.status == 429 else "unavailable"
                )
                continue

            try:
                detail = parse_detail_page(json.loads(response.body))
            except ValueError:
                continue

            if detail is None:
                continue

            found = next(
                (
                    post
                    for post in detail.posts
                    if _identifier(post) == self.seed.post_id
                ),
                None,
            )

            if found is not None and _conversation(found):
                self.target = found
                self.request_url = response.url

            if self.target is None:
                continue

            self._merge_detail(detail)

    def _merge_detail(self, detail: DetailPage) -> None:
        assert self.target is not None
        # A module-only response cannot prove an earlier cursor was exhausted.
        self.has_more = (
            False if detail.terminated else self.has_more or detail.has_more
        )
        self.unexpanded = self.unexpanded or detail.unexpanded
        conversation = _conversation(self.target)
        # Preserve ancestor chains even if a historical payload lacks root ID.
        candidates = {
            _identifier(post): post
            for post in detail.posts
            if _identifier(post)
        }
        parent = _parent(self.target)
        ancestor_ids: set[str] = set()

        while parent in candidates and parent not in ancestor_ids:
            ancestor_ids.add(parent)
            parent = _parent(candidates[parent])

        for identifier, post in candidates.items():
            if identifier == self.seed.post_id:
                continue

            if (
                _conversation(post) != conversation
                and identifier not in ancestor_ids
            ):
                continue

            if identifier not in self.posts and len(self.posts) >= self.limit:
                self.limited = True
                continue

            self.posts[identifier] = post

    def finish(self, reason: str | None = None) -> RawPost:
        now = datetime.datetime.now(datetime.UTC)
        payload = _object(json.loads(self.seed.payload))
        status = "failed"
        reason = reason or self.error

        if self.target is not None:
            payload.update(self.target)
            known = {self.seed.post_id: self.target, **self.posts}
            missing_parent = any(
                _parent(post) and _parent(post) not in known
                for post in known.values()
            )
            direct_count = sum(
                _parent(post) == self.seed.post_id
                for post in self.posts.values()
            )
            reported = _object(self.target.get("legacy")).get("reply_count")
            shortfall = isinstance(reported, int) and reported > direct_count
            missing_count = not isinstance(reported, int) or isinstance(
                reported, bool,
            )
            reason = reason or (
                "limit"
                if self.limited
                else "cursor"
                if self.has_more
                else "unexpanded"
                if self.unexpanded
                else "missing_context"
                if missing_parent
                else "reported_replies"
                if shortfall
                else "missing_source"
                if missing_count
                else None
            )
            status = "partial" if reason else "captured"
        elif reason == "disabled":
            status = "disabled"
        else:
            reason = reason or "no_matching_response"

        payload["conversation_capture"] = {
            "status": status,
            "target_id": self.seed.post_id,
            "conversation_id": _conversation(self.target or payload) or None,
            "captured_at": now.isoformat(),
            "limit": self.limit,
            "has_more": status == "partial",
            "reason": reason,
            "posts": list(self.posts.values()),
        }

        return self.seed._replace(
            payload=json.dumps(payload, separators=(",", ":")),
            scraped_at=now,
            request_url=self.request_url,
        )


async def _collect_detail_responses(
    page: zendriver.Tab,
    capture: NetworkCapture,
    state: ConversationCapture,
    settings: XConversationSettings,
) -> None:
    responses = capture.stream()
    scrolls = 0

    while True:
        try:
            async with asyncio.timeout(RESPONSE_WAIT_SECONDS):
                response = await anext(responses)
        except (TimeoutError, StopAsyncIteration):
            # Recreate a cancelled generator; queue state is preserved.
            responses = capture.stream()

            if state.target is None:
                if state.error:
                    break

                continue

            if scrolls >= settings.max_scrolls or state.limited:
                break

            await scroll_conversation(page)
            await check_login_required(page)
            scrolls += 1
        else:
            state.accept([response])

            if state.limited or state.error:
                break


async def capture_conversation(
    page: zendriver.Tab,
    seed: RawPost,
    settings: XConversationSettings,
) -> RawPost:
    state = ConversationCapture(seed, settings.max_replies)

    if not settings.enabled:
        return state.finish("disabled")

    capture = NetworkCapture(page, DETAIL_PATTERNS)
    reason = None

    try:
        async with asyncio.timeout(settings.timeout_seconds):
            await capture.start()
            url = canonical_post_url(seed.url)
            await page.get(url)
            await ensure_navigation(page, url)
            await check_login_required(page)
            await _collect_detail_responses(page, capture, state, settings)
    except TimeoutError:
        reason = "timeout"
    except LoginRequiredError:
        reason = "login_required"
    except Exception:
        logger.warning(
            "X conversation capture failed for post=%s",
            seed.post_id,
            exc_info=True,
        )
        reason = "unavailable"
    finally:
        try:
            async with asyncio.timeout(CLEANUP_TIMEOUT_SECONDS):
                await capture.stop()
        except Exception:
            logger.warning("X conversation capture cleanup did not finish")

    result = state.finish(reason)
    archive = _object(
        _object(json.loads(result.payload)).get("conversation_capture"),
    )

    if archive.get("status") == "failed":
        count("fetch_failures")

    logger.info(
        "X conversation post=%s status=%s replies/context=%d reason=%s",
        seed.post_id,
        archive.get("status"),
        len(state.posts),
        archive.get("reason"),
    )

    return result
