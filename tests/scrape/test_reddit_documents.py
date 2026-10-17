import datetime as dt
import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from dank.config import RedditSettings
from dank.model import RawPost
from dank.scrape.reddit.access import (
    RedditRateLimitError,
    RedditRequests,
    current_requests,
)
from dank.scrape.reddit.details import capture_post_document
from dank.scrape.reddit.documents import fetch_document, next_comments_url

URL = "https://www.reddit.com/r/python/comments/abc/title/"
MORE = "/svc/shreddit/more-comments/python/t3_abc?top-level=1&sort=confidence"
CARD = (
    '<shreddit-post id="t3_abc" post-title="Post" author="person" '
    'permalink="/r/python/comments/abc/title/" comment-count="90">'
    '<div slot="text-body"><p>Full body</p></div></shreddit-post>'
)


def raw() -> RawPost:
    return RawPost(
        "reddit.com",
        "t3_abc",
        URL,
        None,
        dt.datetime.now(dt.UTC),
        "reddit",
        URL,
        json.dumps(
            {
                "id": "t3_abc",
                "subreddit": "python",
                "num_comments": 90,
                "title": "Post",
                "selftext": "Preview",
            },
        ),
    )


def comments(start: int, end: int) -> list[dict[str, Any]]:
    return [
        {
            "id": f"t1_c{i}",
            "parent_id": "t3_abc",
            "body": f"Reply {i}",
            "body_html": f"<p>Reply {i}</p>",
            "author": "person",
        }
        for i in range(start, end)
    ]


async def test_documents_keep_body_and_fifty_comments_without_navigation(
    monkeypatch: pytest.MonkeyPatch,
):
    first = {
        "url": URL,
        "html": CARD,
        "comments": comments(0, 25),
        "comments_html": "original initial HTML",
        "has_more": True,
        "next_url": MORE, "next_cursor": "next-page",
    }
    more = {
        "comments": comments(23, 100),
        "comments_html": "original more HTML",
    }
    fetch = AsyncMock(side_effect=[first, more])
    monkeypatch.setattr("dank.scrape.reddit.details.fetch_document", fetch)
    page = AsyncMock()
    result = await capture_post_document(page, raw(), RedditSettings())
    assert result is not None
    post = json.loads(result.payload)
    assert post["detail_capture"]["method"] == "document"
    assert post["selftext_html"] == "<p>Full body</p>"
    assert len(post["comment_capture"]["comments"]) == 50
    assert post["comment_capture"]["has_more"] is True
    assert "original more HTML" in post["raw"]["detail"]["comments_html"]
    assert fetch.await_count == 2
    assert fetch.await_args is not None
    assert fetch.await_args.kwargs["cursor"] == "next-page"
    page.send.assert_not_awaited()
    page.activate.assert_not_awaited()


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.test" + MORE,
        "http://www.reddit.com" + MORE,
        MORE.replace("python", "other"),
        MORE.replace("t3_abc", "t3_other"),
        MORE.replace("top-level=1", "top-level=0"),
        "//evil.test" + MORE,
    ],
)
def test_partial_urls_cannot_request_other_hosts_posts_or_nested_threads(
    url: str,
):
    assert next_comments_url({"next_url": url}, "python", "t3_abc") is None


async def test_document_429_keeps_listing_and_slows_subsequent_requests():
    page = AsyncMock()
    page.evaluate.return_value = json.dumps(
        {"status": 429, "retry_after": "30"},
    )
    requests = RedditRequests(2)
    token = current_requests.set(requests)

    try:
        result = await capture_post_document(page, raw(), RedditSettings())
    finally:
        current_requests.reset(token)

    assert result is not None
    post = json.loads(result.payload)
    assert post["selftext"] == "Preview"
    assert post["detail_capture"]["reason"] == "rate_limited"
    assert requests.interval == 4
    page.send.assert_not_awaited()


async def test_document_denial_falls_back_once_per_run():
    page = AsyncMock()
    page.evaluate.return_value = json.dumps({"status": 403})
    requests = RedditRequests(0)
    token = current_requests.set(requests)

    try:
        assert (
            await capture_post_document(page, raw(), RedditSettings()) is None
        )
        assert (
            await capture_post_document(page, raw(), RedditSettings()) is None
        )
    finally:
        current_requests.reset(token)

    page.evaluate.assert_awaited_once()


async def test_incomplete_document_uses_browser_fallback(
    monkeypatch: pytest.MonkeyPatch,
):
    fetch = AsyncMock(
        return_value={
            "url": URL,
            "html": CARD,
            "comments": [],
            "next_url": "",
        },
    )
    monkeypatch.setattr("dank.scrape.reddit.details.fetch_document", fetch)
    assert (
        await capture_post_document(AsyncMock(), raw(), RedditSettings())
        is None
    )
    fetch.return_value["url"] = URL.replace("/abc/", "/other/")
    assert (
        await capture_post_document(
            AsyncMock(),
            raw(),
            RedditSettings(capture_comments=False),
        )
        is None
    )


async def test_rate_limited_comment_pages_are_not_treated_as_empty():
    page = AsyncMock()
    page.evaluate.return_value = json.dumps(
        {"status": 429, "retry_after": "120"},
    )

    with pytest.raises(RedditRateLimitError):
        await fetch_document(page, "https://www.reddit.com" + MORE, "t3_abc")
