from __future__ import annotations

import hashlib
import json
from collections.abc import Generator
from typing import Any, cast
from unittest.mock import Mock

import aiohttp
import pytest
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from dank.config import ScrapeSettings
from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.rss import fetch_feed_links, scrape_feed_batches


class _Response:
    def __init__(
        self, url: str, body: str = "", status: int = 200,
        location: str | None = None,
    ) -> None:
        self.url = URL(url)
        self.body = body
        self.status = status
        self.headers = CIMultiDictProxy(CIMultiDict(
            {"Location": location} if location else {},
        ))
        self.request_info = aiohttp.RequestInfo(
            self.url, "GET", self.headers, self.url,
        )

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def text(self) -> str:
        return self.body

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                self.request_info, (), status=self.status,
            )


class _Client:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = {
            str(response.url): response for response in responses
        }
        self.requests: list[str] = []

    async def __aenter__(self) -> _Client:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    def get(self, url: str, **kwargs: object) -> _Response:
        assert kwargs.get("allow_redirects") is False
        self.requests.append(url)

        return self.responses[url]


@pytest.fixture(autouse=True)
def shared_limiter() -> Generator[None]:
    token = current_limiter.set(RequestLimiter(ScrapeSettings()))

    try:
        yield
    finally:
        current_limiter.reset(token)


@pytest.mark.parametrize("base, expected", [
    ("", "https://blog.test/news/rss.xml"),
    ('<base href="../feeds/">', "https://blog.test/feeds/rss.xml"),
])
async def test_homepage_redirect_preserves_final_base_for_feed_discovery(
    monkeypatch: pytest.MonkeyPatch, base: str, expected: str,
) -> None:
    client = _Client([
        _Response("https://short.test", status=301,
                  location="https://blog.test/news/"),
        _Response("https://blog.test/news/", f'<head>{base}'
                  '<link rel="alternate" type="application/rss+xml" '
                  'href="rss.xml"></head>'),
    ])
    monkeypatch.setattr(
        "dank.scrape.rss.aiohttp.ClientSession", Mock(return_value=client),
    )
    links = await fetch_feed_links("short.test")
    assert [link.url for link in links] == [expected]
    assert client.requests == ["https://short.test", "https://blog.test/news/"]


@pytest.mark.parametrize("final_url, image", [
    ("https://blog.test/posts/article/", "https://blog.test/posts/cover.png"),
    ("https://provider.test/articles/post", "https://provider.test/cover.png"),
])
async def test_feed_and_article_redirects_keep_original_urls_and_raw_content(
    final_url: str, image: str,
) -> None:
    requested_feed = "https://short.test/feed"
    final_feed = "https://provider.test/feeds/news/rss.xml"
    article = "https://provider.test/go"
    feed = ('<rss><channel><item><title>One</title><link>../../go#comment-1'
            '</link></item><item><title>Two</title><link>../../go#comment-2'
            '</link></item></channel></rss>')
    page = ('<article><img src="../cover.png"><audio src="audio.mp3">'
            '</audio><video src="video.mp4" poster="poster.jpg"></video>'
            '<img data-src="lazy.png">'
            '<img srcset="small.png 1x, large.png 2x">'
            '</article>')
    client = _Client([
        _Response(requested_feed, status=302, location=final_feed),
        _Response(final_feed, feed),
        _Response(article, status=302, location=final_url),
        _Response(final_url, page),
    ])
    batches = [batch async for batch in scrape_feed_batches(
        cast(Any, client), domain="configured.test",
        feed_urls=[requested_feed],
    )]
    assert len(batches) == 1
    posts = batches[0].posts
    assert [post.url for post in posts] == [
        article + "#comment-1", article + "#comment-2",
    ]
    assert all(post.domain == "configured.test" for post in posts)
    assert all(post.request_url == requested_feed for post in posts)
    assert all(post.post_id == hashlib.sha256(post.url.encode()).hexdigest()
               for post in posts)
    payload = json.loads(posts[0].payload)
    assert payload["page_html"] == page
    assert "../../go#comment-1" in payload["feed_xml"]
    assert payload["feed_final_url"] == final_feed
    assert payload["page_final_url"] == final_url
    assert "page_fetch_status" not in payload
    assert client.requests == [requested_feed, final_feed, article, final_url]
    assert all(asset.url.startswith(final_url.rsplit('/', 1)[0] + '/')
               or asset.url == image for asset in batches[0].assets)
    assert image in {asset.url for asset in batches[0].assets}
    assert len(batches[0].assets) == 12


async def test_article_base_overrides_redirect_url_for_all_media() -> None:
    feed_url = "https://feeds.test/rss"
    article = "https://short.test/abc"
    final = "https://blog.test/posts/entry"
    page = ('<head><base href="https://cdn.test/assets/"></head>'
            '<article><img src="cover.png"><video src="clip.mp4"></video>'
            '<audio src="sound.mp3"></audio></article>')
    client = _Client([
        _Response(feed_url, '<rss><channel><item><link>' + article +
                  '</link></item></channel></rss>'),
        _Response(article, status=301, location=final),
        _Response(final, page),
    ])
    batches = [batch async for batch in scrape_feed_batches(
        cast(Any, client), domain="feeds.test", feed_urls=[feed_url],
    )]
    assert {asset.url for asset in batches[0].assets} == {
        "https://cdn.test/assets/cover.png", "https://cdn.test/assets/clip.mp4",
        "https://cdn.test/assets/sound.mp3",
    }
    assert json.loads(batches[0].posts[0].payload)["page_final_url"] == final


async def test_failed_redirect_target_retains_feed_provenance() -> None:
    feed_url = "https://feeds.test/rss"
    article = "https://short.test/abc"
    client = _Client([
        _Response(feed_url, '<rss><channel><item><link>' + article +
                  '</link></item></channel></rss>'),
        _Response(article, status=302, location="https://blog.test/missing"),
        _Response("https://blog.test/missing", status=404),
    ])
    batches = [batch async for batch in scrape_feed_batches(
        cast(Any, client), domain="feeds.test", feed_urls=[feed_url],
        keep_feed_on_fetch_failure=True,
    )]
    post = batches[0].posts[0]
    assert post.url == article and post.request_url == feed_url
    payload = json.loads(post.payload)
    assert payload["page_fetch_status"] == "failed"
    assert payload["page_html"] == "" and "page_final_url" not in payload
    assert payload["feed_final_url"] == feed_url
    assert batches[0].assets == []
