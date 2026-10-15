import datetime
import json
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dank.html_utils import html_text
from dank.model import RawPost
from dank.process.rss import convert_raw_post
from dank.scrape.rss import FetchedText, retry_feed_pages

NOW = datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC)
FEED = (
    '<entry xmlns="http://www.w3.org/2005/Atom">'
    '<title>A saved article</title><author><name>Writer</name></author>'
    '<content type="text">First paragraph. Second paragraph.</content>'
    '</entry>'
)
PAGE = (
    '<article><p>First <a href="details">paragraph.</a></p>'
    '<img src="image.jpg"><p>Second paragraph.</p></article>'
)


def _failed_post(post_id: str = "saved-id", fragment: str = "") -> RawPost:
    return RawPost(
        "example.test", post_id, f"https://example.test/story{fragment}",
        NOW, NOW, "rss", "https://example.test/feed",
        json.dumps({
            "feed_xml": FEED, "page_html": "", "page_fetch_status": "failed",
            "feed_final_url": "https://feeds.example.test/news.xml",
            "extra_provenance": "preserve this too",
        }),
    )


async def test_retry_restores_paragraphs_without_refetching_the_feed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = _failed_post()
    original_payload = failed.payload
    before = convert_raw_post(failed)
    assert before is not None
    assert before.html == "First paragraph. Second paragraph."
    fetch = AsyncMock(return_value=FetchedText(
        PAGE, "https://publisher.test/articles/story/",
    ))
    monkeypatch.setattr("dank.scrape.rss._fetch_text", fetch)
    batches = [
        batch async for batch in retry_feed_pages(cast(Any, None), [failed])
    ]

    fetch.assert_awaited_once()
    assert fetch.call_args.args[1] == failed.url
    assert len(batches) == 1
    saved, = batches[0].posts
    payload = json.loads(saved.payload)
    assert saved == failed._replace(payload=saved.payload,
                                   scraped_at=saved.scraped_at)
    assert saved.scraped_at > failed.scraped_at
    assert payload == {
        "feed_xml": FEED, "page_html": PAGE,
        "page_final_url": "https://publisher.test/articles/story/",
        "feed_final_url": "https://feeds.example.test/news.xml",
        "extra_provenance": "preserve this too",
    }
    assert failed.payload == original_payload
    assert [(asset.post_id, asset.url) for asset in batches[0].assets] == [
        (failed.post_id, "https://publisher.test/articles/story/image.jpg"),
    ]
    after = convert_raw_post(saved)
    assert after is not None
    assert after.html.count("<p>") == 2
    assert 'href="https://publisher.test/articles/story/details"' in after.html
    assert " ".join(html_text(after.html).split()) == before.html
    assert (after.post_id, after.title, after.author, after.created_at) == (
        before.post_id, before.title, before.author, before.created_at,
    )


@pytest.mark.parametrize("page", [None, FetchedText("", "https://example.test")])
async def test_failed_retry_does_not_replace_saved_content(
    monkeypatch: pytest.MonkeyPatch, page: FetchedText | None,
) -> None:
    failed = _failed_post()
    monkeypatch.setattr("dank.scrape.rss._fetch_text",
                        AsyncMock(return_value=page))

    assert [
        batch async for batch in retry_feed_pages(cast(Any, None), [failed])
    ] == []
    assert json.loads(failed.payload)["page_fetch_status"] == "failed"


async def test_retry_shares_page_requests_and_keeps_comment_identities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posts = [_failed_post("first", "#comment-1"),
             _failed_post("second", "#comment-2")]
    fetch = AsyncMock(return_value=FetchedText(PAGE, "https://example.test/"))
    monkeypatch.setattr("dank.scrape.rss._fetch_text", fetch)
    batches = [
        batch async for batch in retry_feed_pages(cast(Any, None), posts)
    ]

    fetch.assert_awaited_once()
    assert fetch.call_args.args[1] == "https://example.test/story"
    assert [(post.post_id, post.url) for post in batches[0].posts] == [
        (post.post_id, post.url) for post in posts
    ]
    assert [asset.post_id for asset in batches[0].assets] == [
        "first", "second",
    ]


async def test_retry_batches_keep_successes_when_another_page_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posts = [
        _failed_post(str(i))._replace(url=f"https://example.test/{i}")
        for i in range(3)
    ]
    fetch = AsyncMock(side_effect=[
        FetchedText(PAGE, posts[0].url), None, FetchedText(PAGE, posts[2].url),
    ])
    monkeypatch.setattr("dank.scrape.rss._fetch_text", fetch)
    batches = [
        batch async for batch in retry_feed_pages(
            cast(Any, None), posts, batch_size=2,
        )
    ]

    assert [[post.post_id for post in batch.posts] for batch in batches] == [
        ["0"], ["2"],
    ]
    assert fetch.await_count == 3
