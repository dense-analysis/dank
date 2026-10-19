from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, cast
from unittest.mock import AsyncMock

import aiohttp
import pytest
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from dank.scrape.linked_articles import LinkedArticle, scrape_linked_articles
from dank.scrape.rss import FetchedText


class _Response:
    def __init__(
        self, url: str, body: str, *,
        content_type: str = "text/html", status: int = 200,
    ) -> None:
        self.url = URL(url)
        self.body = body
        self.content_type = content_type
        self.status = status
        self.text_reads = 0
        self.headers = CIMultiDictProxy(CIMultiDict[str]())

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def text(self) -> str:
        self.text_reads += 1

        return self.body

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                aiohttp.RequestInfo(self.url, "GET", self.headers, self.url),
                (), status=self.status,
            )


class _Client:
    def __init__(self, response: _Response) -> None:
        self.response = response
        self.requests: list[str] = []

    def get(self, url: str, **kwargs: object) -> _Response:
        self.requests.append(url)

        return self.response


async def test_capture_preserves_page_and_redirects_with_publisher_assets(
) -> None:
    requested = "https://short.test/go#comments"
    final = "https://publisher.test/news/article"
    page = (
        '<head><base href="https://cdn.test/story/">'
        '<link rel="canonical" href="https://unrelated.test/forged">'
        '</head><article><p>The article.</p><img src="cover.jpg">'
        '<video src="clip.mp4"></video></article>'
    )
    client = _Client(_Response(final, page))
    batches = [batch async for batch in scrape_linked_articles(
        cast(Any, client), [LinkedArticle(requested, "Title & text")],
    )]
    post, = batches[0].posts
    payload = json.loads(post.payload)
    expected_id = hashlib.sha256(final.encode()).hexdigest()

    assert post.domain == "publisher.test"
    assert post.post_id == expected_id
    assert post.url == final
    assert post.source == "linked_article"
    assert post.post_created_at is None
    assert post.request_url == requested
    assert payload["page_html"] == page
    assert payload["page_final_url"] == final
    assert "Title &amp; text" in payload["feed_xml"]
    assert payload["linked_article"] == {
        "requested_url": requested, "final_url": final, "status": "captured",
    }
    assert "page_fetch_status" not in payload
    assert client.requests == ["https://short.test/go"]
    assert {asset.url for asset in batches[0].assets} == {
        "https://cdn.test/story/cover.jpg", "https://cdn.test/story/clip.mp4",
    }
    assert all(
        (asset.source, asset.domain, asset.post_id, asset.referrer_url)
        == ("linked_article", "publisher.test", expected_id, final)
        for asset in batches[0].assets
    )


async def test_existing_identity_survives_redirect_and_duplicate_links(
) -> None:
    original = "https://publisher.test/article"
    final = "https://publisher.test/new/article"
    client = _Client(_Response(final, "<article>Readable text</article>"))
    article = LinkedArticle(original, "Title", ("saved.test", "saved-id"))
    batches = [batch async for batch in scrape_linked_articles(
        cast(Any, client), [article, article._replace(url=original + "#part")],
    )]
    post, = batches[0].posts

    assert (post.domain, post.post_id, post.url) == (
        "saved.test", "saved-id", original,
    )
    assert client.requests == [original]


@pytest.mark.parametrize("status", [403, 404, 500])
async def test_failures_keep_empty_article_capture(status: int) -> None:
    url = "https://publisher.test/restricted"
    client = _Client(_Response(url, "Access denied", status=status))
    batches = [batch async for batch in scrape_linked_articles(
        cast(Any, client), [LinkedArticle(url, "Known title")],
    )]
    post, = batches[0].posts
    payload = json.loads(post.payload)

    assert payload["page_fetch_status"] == "failed"
    assert payload["page_html"] == ""
    assert payload["linked_article"]["status"] == "failed"
    assert "Known title" in payload["feed_xml"]
    assert post.post_created_at is None
    assert not batches[0].assets


@pytest.mark.parametrize("content_type", [
    "application/pdf", "application/json", "image/png", "text/plain",
])
async def test_unsupported_document_does_not_decode_or_render_binary(
    content_type: str,
) -> None:
    url = "https://publisher.test/document"
    response = _Response(url, "not article HTML", content_type=content_type)
    batches = [batch async for batch in scrape_linked_articles(
        cast(Any, _Client(response)), [LinkedArticle(url, "A document")],
    )]
    payload = json.loads(batches[0].posts[0].payload)

    assert payload["linked_article"]["status"] == "unsupported"
    assert payload["page_content_type"] == content_type
    assert payload["page_html"] == ""
    assert payload["page_fetch_status"] == "failed"
    assert response.text_reads == 0
    assert not batches[0].assets


async def test_challenge_is_unavailable_instead_of_article_body() -> None:
    url = "https://publisher.test/article"
    page = ('<html><title>Are we human?</title>'
            '<script src="/_labrador/pow/slow.js"></script>'
            '<div class="wicketkeeper"></div></html>')
    batches = [batch async for batch in scrape_linked_articles(
        cast(Any, _Client(_Response(url, page))), [LinkedArticle(url)],
    )]
    payload = json.loads(batches[0].posts[0].payload)

    assert payload["page_fetch_status"] == "failed"
    assert payload["page_html"] == ""
    assert not batches[0].assets


async def test_capture_bounds_requests_and_preserves_browser_method(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    in_flight = 0
    peak = 0

    async def fetch(
        client: object, url: str, **kwargs: object,
    ) -> FetchedText:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0)
        in_flight -= 1

        return FetchedText("<article>Article</article>", url, "browser")

    monkeypatch.setattr("dank.scrape.linked_articles._fetch_text", fetch)
    batches = [batch async for batch in scrape_linked_articles(
        cast(Any, object()),
        [LinkedArticle(f"https://publisher.test/{index}")
         for index in range(7)],
        concurrency=2, batch_size=3,
    )]

    assert [len(batch.posts) for batch in batches] == [3, 3, 1]
    assert peak == 2
    assert all(
        json.loads(post.payload)["page_fetch_method"] == "browser"
        for batch in batches for post in batch.posts
    )


async def test_non_web_and_credential_links_are_not_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = AsyncMock()
    monkeypatch.setattr("dank.scrape.linked_articles._fetch_text", fetch)
    batches = [batch async for batch in scrape_linked_articles(
        cast(Any, object()), [
            LinkedArticle("https://[malformed/article"),
            LinkedArticle("file:///private/file"),
            LinkedArticle("javascript:alert(1)"),
            LinkedArticle("https://user:password@publisher.test/article"),
        ],
    )]

    assert not batches
    fetch.assert_not_called()


async def test_capture_keeps_collection_origin_separate_from_fetch_time(
) -> None:
    url = "https://publisher.test/article"
    origin = {
        "domain": "reddit.com", "post_id": "t3_abc", "source": "reddit",
        "community": "python", "url": "https://reddit.com/comments/abc",
        "observed_at": "2026-01-01T00:00:00+00:00",
    }
    client = _Client(_Response(url, "<article>Publisher body</article>"))
    batches = [batch async for batch in scrape_linked_articles(
        cast(Any, client), [LinkedArticle(url, "Story", origin=origin)],
    )]
    post, = batches[0].posts
    payload = json.loads(post.payload)
    assert payload["collection_origin"] == origin
    assert post.source == "linked_article"
    assert post.domain == "publisher.test"
    assert post.scraped_at.isoformat() != origin["observed_at"]
    assert post.post_created_at is None
