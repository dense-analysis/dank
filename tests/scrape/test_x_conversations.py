from __future__ import annotations

import asyncio
import datetime
import json
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import quote

import pytest

from dank.config import XConversationSettings, XSettings
from dank.model import RawPost
from dank.scrape.types import ScrapeBatch
from dank.scrape.x import (
    scrape_x_accounts,
    scrape_x_posts,
)
from dank.scrape.x.conversations import (
    ConversationCapture,
    canonical_post_url,
    capture_conversation,
    parse_detail_page,
)
from dank.scrape.x.session import LoginRequiredError
from dank.scrape.zendriver import NetworkResponse


def tweet(
    identifier: str, parent: str = "", *, root: str = "100", replies: int = 0,
) -> dict[str, Any]:
    return {
        "__typename": "Tweet",
        "rest_id": identifier,
        "legacy": {
            "id_str": identifier,
            "conversation_id_str": root,
            "in_reply_to_status_id_str": parent or None,
            "full_text": f"Post {identifier}",
            "reply_count": replies,
        },
        "core": {
            "user_results": {
                "result": {
                    "legacy": {"screen_name": "writer", "name": "Writer"},
                },
            },
        },
    }


def slot(post: dict[str, Any]) -> dict[str, Any]:
    return {
        "itemContent": {
            "itemType": "TimelineTweet",
            "tweet_results": {"result": post},
        },
    }


def payload(
    *posts: dict[str, Any], cursor: bool = False, unexpanded: bool = False,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = [
        {
            "entryId": "tweet-100",
            "content": {
                "entryType": "TimelineTimelineItem",
                **slot(posts[0]),
            },
        },
    ]

    if len(posts) > 1:
        entries.append(
            {
                "entryId": "conversationthread-1",
                "content": {
                    "entryType": "TimelineTimelineModule",
                    "items": [{"item": slot(post)} for post in posts[1:]],
                },
            },
        )

    if cursor:
        entries.append(
            {
                "entryId": "cursor-bottom-0",
                "content": {
                    "entryType": "TimelineTimelineCursor",
                    "cursorType": "Bottom",
                    "value": "continuation-token",
                },
            },
        )

    if unexpanded:
        entries.append(
            {
                "content": {
                    "entryType": "TimelineTimelineItem",
                    "itemContent": {
                        "itemType": "TimelineTombstone",
                    },
                },
            },
        )

    return {
        "data": {
            "threaded_conversation_with_injections_v2": {
                "instructions": [
                    {"type": "TimelineAddEntries", "entries": entries},
                ],
            },
        },
    }


def response(
    body: object,
    focal: str = "100",
    status: int = 200,
    endpoint: str = "TweetDetail",
) -> NetworkResponse:
    variables = quote(json.dumps({"focalTweetId": focal}))

    return NetworkResponse(
        f"https://x.com/i/api/graphql/id/{endpoint}?variables={variables}",
        status,
        "application/json",
        json.dumps(body),
        "request",
        "Fetch",
    )


def seed(identifier: str = "100", *, author: str = "writer") -> RawPost:
    return RawPost(
        "x.com",
        identifier,
        f"https://x.com/{author}/status/{identifier}",
        datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC),
        "x",
        "old-request",
        json.dumps(tweet(identifier)),
    )


def archive(post: RawPost) -> dict[str, Any]:
    return json.loads(post.payload)["conversation_capture"]


def capture(
    body: object, *, focal: str = "100", limit: int = 50,
) -> dict[str, Any]:
    state = ConversationCapture(seed(focal), limit)
    state.accept([response(body, focal)])

    return archive(state.finish())


@pytest.mark.parametrize(
    "value",
    [
        "https://x.com/writer/status/100",
        "http://twitter.com/writer/status/100/",
        "https://www.x.com/writer/status/100?s=20#reply",
    ],
)
def test_canonical_post_urls(value: str) -> None:
    assert canonical_post_url(value) == "https://x.com/writer/status/100"


@pytest.mark.parametrize(
    "value",
    [
        "https://x.com/writer",
        "https://x.com/writer/status/nope",
        "https://x.com/writer/status/0",
        "https://x.com/writer/status/100/photo/1",
        "https://x.com@evil.test/writer/status/100",
        "https://user:password@x.com/writer/status/100",
        "https://x.com:443/writer/status/100",
        "https://evil.test/writer/status/100",
        "javascript:alert(1)",
        "https://x.com/way_too_long_handle/status/100",
    ],
)
def test_invalid_post_urls(value: str) -> None:
    with pytest.raises(ValueError, match="post URL"):
        canonical_post_url(value)


def test_capture_preserves_context_replies_and_raw_media_without_quotes() -> (
    None
):
    focal = tweet("102", "101", replies=1)
    focal["quoted_status_result"] = {"result": tweet("999", root="999")}
    reply = tweet("103", "102")
    reply["legacy"]["extended_entities"] = {
        "media": [
            {
                "type": "photo",
                "media_url_https": "https://pbs.twimg.com/media/p.jpg",
            },
        ],
    }
    result = capture(
        payload(
            focal,
            tweet("100"),
            tweet("101", "100"),
            reply,
            tweet("900", root="900"),
        ),
        focal="102",
    )

    assert result["status"] == "captured"
    assert result["conversation_id"] == "100"
    assert [post["rest_id"] for post in result["posts"]] == [
        "100",
        "101",
        "103",
    ]
    assert result["posts"][-1]["legacy"]["extended_entities"]
    assert result["has_more"] is False


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (
            payload(tweet("100", replies=2), tweet("101", "100")),
            "reported_replies",
        ),
        (payload(tweet("100"), cursor=True), "cursor"),
        (payload(tweet("100"), unexpanded=True), "unexpanded"),
        (payload(tweet("100", "90")), "missing_context"),
    ],
)
def test_honest_partial_status(body: object, reason: str) -> None:
    result = capture(body)

    assert result["status"] == "partial"
    assert result["reason"] == reason
    assert result["has_more"] is True


def test_post_limit_deduplicates_and_bounds_archive() -> None:
    state = ConversationCapture(seed(), 2)
    body = payload(
        tweet("100", replies=10),
        tweet("101", "100"),
        tweet("102", "101"),
        tweet("103", "100"),
    )
    state.accept([response(body), response(body)])
    result = archive(state.finish())

    assert len(result["posts"]) == 2
    assert result["reason"] == "limit"
    assert result["status"] == "partial"


def test_pagination_can_extend_validated_focal_without_repeating_it() -> None:
    state = ConversationCapture(seed(), 50)
    state.accept([response(payload(tweet("100", replies=2), cursor=True))])
    final_page = payload(tweet("101", "100"), tweet("102", "100"))
    timeline = final_page["data"]["threaded_conversation_with_injections_v2"]
    timeline["instructions"].append({
        "type": "TimelineTerminateTimeline", "direction": "Bottom",
    })
    state.accept([response(final_page)])
    result = archive(state.finish())

    assert result["status"] == "captured"
    assert len(result["posts"]) == 2


def test_add_to_module_and_wrapped_tweet_payloads() -> None:
    body = payload(tweet("100", replies=1))
    instructions = body["data"]["threaded_conversation_with_injections_v2"][
        "instructions"
    ]
    instructions.append(
        {
            "type": "TimelineAddToModule",
            "moduleItems": [
                {
                    "item": slot(
                        {
                            "__typename": "TweetWithVisibilityResults",
                            "tweet": tweet("101", "100"),
                        },
                    ),
                },
            ],
        },
    )
    result = capture(body)

    assert result["status"] == "captured"
    assert result["posts"][0]["rest_id"] == "101"


def test_ads_are_not_replies() -> None:
    body = payload(tweet("100"), tweet("101", "100"))
    module = body["data"]["threaded_conversation_with_injections_v2"][
        "instructions"
    ][0]["entries"][1]
    module["content"]["items"][0]["item"]["itemContent"][
        "promotedMetadata"
    ] = {"id": "ad"}
    result = capture(body)

    assert result["posts"] == []


@pytest.mark.parametrize(
    ("focal", "endpoint", "body"),
    [
        ("999", "TweetDetail", payload(tweet("100"))),
        ("100", "UserTweets", payload(tweet("100"))),
        ("100", "TweetDetail", payload(tweet("999", root="999"))),
        ("100", "TweetDetail", {"data": {"user": {"result": tweet("100")}}}),
        ("100", "TweetDetail", {"errors": [{"message": "Unavailable"}]}),
    ],
)
def test_unrelated_or_missing_detail_never_claims_empty_capture(
    focal: str,
    endpoint: str,
    body: object,
) -> None:
    original = seed()
    state = ConversationCapture(original, 50)
    state.accept([response(body, focal, endpoint=endpoint)])
    result = state.finish()

    assert archive(result)["status"] == "failed"
    assert archive(result)["reason"] == "no_matching_response"
    assert (
        json.loads(result.payload)["legacy"]
        == json.loads(original.payload)["legacy"]
    )
    assert result.scraped_at > original.scraped_at


@pytest.mark.parametrize(
    "status, reason", [(429, "rate_limited"), (403, "unavailable")],
)
def test_http_failure_is_not_an_empty_conversation(
    status: int, reason: str,
) -> None:
    state = ConversationCapture(seed(), 50)
    state.accept([response({}, status=status)])

    assert archive(state.finish())["reason"] == reason


def test_unknown_detail_shape_is_rejected() -> None:
    assert parse_detail_page({"data": {}}) is None


async def _never(*args: Any) -> None:
    await asyncio.Event().wait()


@pytest.fixture
def browser_capture(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Any]:
    queue: asyncio.Queue[NetworkResponse] = asyncio.Queue()

    async def stream() -> AsyncIterator[NetworkResponse]:
        while True:
            yield await queue.get()

    network = MagicMock(start=AsyncMock(), stop=AsyncMock(), stream=stream)
    network.queue = queue
    page = MagicMock(get=AsyncMock())
    monkeypatch.setattr(
        "dank.scrape.x.conversations.NetworkCapture",
        MagicMock(return_value=network),
    )
    monkeypatch.setattr(
        "dank.scrape.x.conversations.ensure_navigation", AsyncMock(),
    )
    monkeypatch.setattr(
        "dank.scrape.x.conversations.check_login_required", AsyncMock(),
    )
    monkeypatch.setattr(
        "dank.scrape.x.conversations.scroll_conversation", AsyncMock(),
    )
    monkeypatch.setattr(
        "dank.scrape.x.conversations.RESPONSE_WAIT_SECONDS", 0.01,
    )
    monkeypatch.setattr(
        "dank.scrape.x.conversations.CLEANUP_TIMEOUT_SECONDS", 0.01,
    )

    return page, network


async def test_browser_capture_saves_valid_empty_conversation_and_cleans_up(
    browser_capture: tuple[Any, Any],
) -> None:
    page, network = browser_capture
    network.queue.put_nowait(response(payload(tweet("100"))))
    result = await capture_conversation(page, seed(), XConversationSettings())

    assert archive(result)["status"] == "captured"
    network.stop.assert_awaited_once()
    page.get.assert_awaited_once_with("https://x.com/writer/status/100")


async def test_timeout_after_valid_data_retains_partial_evidence(
    monkeypatch: pytest.MonkeyPatch,
    browser_capture: tuple[Any, Any],
) -> None:
    page, network = browser_capture
    network.queue.put_nowait(
        response(payload(tweet("100"), tweet("101", "100"))),
    )
    monkeypatch.setattr(
        "dank.scrape.x.conversations.scroll_conversation", _never,
    )
    settings = XConversationSettings(timeout_seconds=0.04)
    result = await capture_conversation(page, seed(), settings)

    assert archive(result)["status"] == "partial"
    assert archive(result)["reason"] == "timeout"
    assert archive(result)["posts"][0]["rest_id"] == "101"


async def test_timeout_without_response_is_failed_and_cleanup_is_bounded(
    browser_capture: tuple[Any, Any],
) -> None:
    page, network = browser_capture
    network.stop.side_effect = _never

    async with asyncio.timeout(0.5):
        result = await capture_conversation(
            page,
            seed(),
            XConversationSettings(timeout_seconds=0.02),
        )

    assert archive(result)["status"] == "failed"
    assert archive(result)["reason"] == "timeout"


async def test_login_required_retains_seed_and_reports_failure(
    monkeypatch: pytest.MonkeyPatch,
    browser_capture: tuple[Any, Any],
) -> None:
    page, _ = browser_capture
    monkeypatch.setattr(
        "dank.scrape.x.conversations.check_login_required",
        AsyncMock(side_effect=LoginRequiredError("Sign in")),
    )
    result = await capture_conversation(page, seed(), XConversationSettings())

    assert archive(result)["status"] == "failed"
    assert archive(result)["reason"] == "login_required"


async def test_normal_collection_saves_timelines_without_visiting_discussions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    async def account(*args: Any) -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([seed("100"), seed("300", author="other")], [])
        yield ScrapeBatch([seed("200")], [])

    async def collect(page: Any, post: RawPost, settings: Any) -> RawPost:
        events.append("capture:" + post.post_id)

        return post

    monkeypatch.setattr("dank.scrape.x._scrape_account", account)
    monkeypatch.setattr("dank.scrape.x.capture_conversation", collect)
    settings = XSettings(
        conversation=XConversationSettings(max_posts_per_account=1),
    )

    page = MagicMock()
    browser = MagicMock(get=AsyncMock(return_value=page))
    session = MagicMock(get_browser=AsyncMock(return_value=browser))
    monkeypatch.setattr("dank.scrape.x.wait_for_x_session", AsyncMock())

    async for batch in scrape_x_accounts(
        settings, ("writer", "second"), session,
    ):
        events.append("save:" + ",".join(post.post_id for post in batch.posts))

    assert events == ["save:100,300", "save:200"] * 2


async def test_disabled_targeted_capture_does_not_open_browser() -> None:
    session = MagicMock(get_browser=AsyncMock())
    settings = XSettings(conversation=XConversationSettings(enabled=False))
    batches = [
        batch async for batch in scrape_x_posts(settings, [seed()], session)
    ]

    session.get_browser.assert_not_called()
    assert archive(batches[0].posts[0])["status"] == "disabled"


def test_module_response_does_not_erase_unconsumed_cursor() -> None:
    state = ConversationCapture(seed(), 50)
    state.accept([response(payload(tweet("100", replies=1), cursor=True))])
    state.accept([response(payload(tweet("101", "100")))])

    assert archive(state.finish())["reason"] == "cursor"


def test_missing_reply_count_is_not_claimed_complete() -> None:
    focal = tweet("100")
    del focal["legacy"]["reply_count"]
    result = capture(payload(focal))

    assert result["status"] == "partial"
    assert result["reason"] == "missing_source"


async def test_rate_limit_after_data_preserves_posts_and_stops_scrolling(
    monkeypatch: pytest.MonkeyPatch, browser_capture: tuple[Any, Any],
) -> None:
    page, network = browser_capture
    network.queue.put_nowait(response(payload(tweet("100"),
                                             tweet("101", "100"))))
    network.queue.put_nowait(response({}, status=429))
    scroll = AsyncMock()
    monkeypatch.setattr(
        "dank.scrape.x.conversations.scroll_conversation", scroll,
    )
    result = await capture_conversation(page, seed(), XConversationSettings())

    assert archive(result)["status"] == "partial"
    assert archive(result)["reason"] == "rate_limited"
    assert archive(result)["posts"][0]["rest_id"] == "101"
    scroll.assert_not_called()
