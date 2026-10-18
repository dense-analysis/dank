import datetime as dt
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dank.config import BrowserScrapeSettings, XSettings
from dank.model import AssetDiscovery, RawPost
from dank.scrape.types import ScrapeBatch
from dank.scrape.x import (
    _scrape_account,  # pyright: ignore[reportPrivateUsage]
    extract_posts_and_assets,
    scrape_x_accounts,
)
from dank.scrape.x.payloads import extract_posts_from_payload
from dank.scrape.zendriver import NetworkResponse
from dank.x_content import is_x_reference

REFERENCE = {"__typename": "Tweet", "rest_id": "123"}
FULL = {**REFERENCE, "legacy": {"full_text": "Real post"}}


def response(*items: dict[str, Any]) -> NetworkResponse:
    return NetworkResponse(
        "https://x.com/i/api/graphql/query/UserTweets",
        200,
        "application/json",
        json.dumps({"items": items}),
        "test",
        "XHR",
    )


def raw(*, reference: bool) -> RawPost:
    return RawPost(
        "x.com",
        "123",
        "https://x.com/writer/status/123",
        None,
        dt.datetime.now(dt.UTC),
        "x",
        "timeline",
        json.dumps(REFERENCE if reference else FULL),
        reference,
    )


@pytest.mark.parametrize(
    "extra",
    [
        {"legacy": {"full_text": "text"}},
        {
            "core": {
                "user_results": {
                    "result": {"legacy": {"screen_name": "writer"}},
                },
            },
        },
        {"legacy": {"extended_entities": {"media": [{"type": "photo"}]}}},
        {"note_tweet": {"note_tweet_results": {"result": {"text": "note"}}}},
        {"unknown_future_field": None},
    ],
)
def test_only_exact_id_reference_shape_is_classified(extra: dict[str, Any]):
    assert is_x_reference(REFERENCE)
    assert not is_x_reference({**REFERENCE, **extra})
    assert not is_x_reference({"__typename": "User", "rest_id": "123"})


@pytest.mark.parametrize("items", [(REFERENCE, FULL), (FULL, REFERENCE)])
def test_reference_and_content_with_same_id_are_both_preserved(
    items: tuple[dict[str, Any], ...],
):
    extracted = extract_posts_from_payload({"items": list(items)})
    assert len(extracted) == 2
    assert {item.is_reference for item in extracted} == {False, True}
    assert (
        next(item for item in extracted if item.is_reference).payload
        == REFERENCE
    )
    assert (
        next(item for item in extracted if not item.is_reference).payload
        == FULL
    )


@pytest.mark.parametrize("first_reference", [False, True])
def test_reference_dedup_does_not_suppress_later_content_or_raw_reference(
    *, first_reference: bool,
):
    seen_posts: set[str] = set()
    seen_refs: set[str] = set()
    seen_assets: set[str] = set()
    items = [REFERENCE, FULL] if first_reference else [FULL, REFERENCE]
    results: list[RawPost] = []
    for item in [*items, *items]:
        posts, _ = extract_posts_and_assets(
            [response(item)], seen_posts, seen_assets, seen_refs,
        )
        results.extend(posts)
    assert len(results) == 2
    assert sum(not p.is_reference for p in results) == 1
    assert sum(p.is_reference for p in results) == 1
    assert {p.payload for p in results} == {
        json.dumps(REFERENCE, separators=(",", ":")),
        json.dumps(FULL, separators=(",", ":")),
    }


@pytest.mark.parametrize("scrolls", [0, 1])
async def test_references_do_not_use_post_limit(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    scrolls: int,
):
    page = MagicMock(get=AsyncMock())
    page.get.return_value = page
    capture = MagicMock(start=AsyncMock(), stop=AsyncMock())
    caplog.set_level("INFO", logger="dank.scrape.x")
    replies: list[tuple[list[RawPost], list[AssetDiscovery]]] = [
        ([raw(reference=True)], []), ([raw(reference=False)], []),
    ]
    drain = AsyncMock(side_effect=replies + ([([], [])] if scrolls else []))
    monkeypatch.setattr(
        "dank.scrape.x.NetworkCapture", MagicMock(return_value=capture),
    )
    monkeypatch.setattr("dank.scrape.x._ensure_navigation", AsyncMock())
    monkeypatch.setattr("dank.scrape.x._check_login_required", AsyncMock())
    monkeypatch.setattr("dank.scrape.x._scroll", AsyncMock())
    monkeypatch.setattr("dank.scrape.x._drain_posts_and_assets", drain)
    saved = [
        b
        async for b in _scrape_account(
            page,
            "writer",
            XSettings(
                scrape=BrowserScrapeSettings(max_posts=1, max_scrolls=scrolls),
            ),
        )
    ]
    assert sum(b.post_count for b in saved) == 1
    assert sum(b.reference_count for b in saved) == 1
    assert drain.await_count == 2 + scrolls
    assert "total_posts=1" in caplog.text


async def test_retry_keeps_reference_and_later_content_once(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    attempts = 0

    async def collect(*args: Any):
        nonlocal attempts
        attempts += 1
        yield ScrapeBatch([raw(reference=True)], [])
        if attempts == 1:
            raise TimeoutError()
        yield ScrapeBatch([raw(reference=False)], [])

    browser = MagicMock(get=AsyncMock())
    session = MagicMock(
        get_browser=AsyncMock(return_value=browser), close=AsyncMock(),
    )
    monkeypatch.setattr("dank.scrape.x.wait_for_x_session", AsyncMock())
    monkeypatch.setattr("dank.scrape.x._scrape_account", collect)
    caplog.set_level("INFO", logger="dank.scrape.x")
    saved = [
        b async for b in scrape_x_accounts(XSettings(), ("writer",), session)
    ]
    assert sum(b.post_count for b in saved) == 1
    assert sum(b.reference_count for b in saved) == 1
    assert "posts=1 references=1 attempts=2" in caplog.text


def test_quoted_posts_and_media_are_saved_under_their_own_identity():
    child = {
        "__typename": "Tweet", "rest_id": "456",
        "core": {"user_results": {"result": {
            "core": {"screen_name": "quoted"},
        }}},
        "legacy": {"full_text": "Quoted post", "extended_entities": {
            "media": [{
                "type": "video",
                "media_url_https": "https://pbs.twimg.com/media/quote.jpg",
                "video_info": {"variants": [{
                    "url": "https://video.twimg.com/vid/720x720/quote.mp4",
                    "content_type": "video/mp4", "bitrate": 500,
                }]},
            }],
        }},
    }
    parent = {
        **FULL, "quoted_status_result": {"result": {
            "__typename": "TweetWithVisibilityResults", "tweet": child,
        }},
    }
    posts, assets = extract_posts_and_assets(
        [response(parent)], set(), set(), set(),
    )
    assert {post.post_id for post in posts} == {"123", "456"}
    quote = next(post for post in posts if post.post_id == "456")
    assert quote.url == "https://x.com/quoted/status/456"
    assert json.loads(quote.payload) == child
    assert {asset.post_id for asset in assets} == {"456"}
    assert {asset.asset_type for asset in assets} == {"photo", "video"}
