import asyncio
from typing import Any, cast

from dank.scrape.rss import scrape_feed_batches
from dank.scrape.types import ScrapeBatch

RSS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example Feed</title>
    <link>https://example.test/</link>
    <description>Example</description>
    <item>
      <title>First</title>
      <link>https://example.test/post-one</link>
      <guid>post-one</guid>
      <pubDate>Sun, 01 Feb 2026 01:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Second</title>
      <link>https://example.test/post-two</link>
      <guid>post-two</guid>
      <pubDate>Sun, 01 Feb 2026 02:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

RSS_XML_TWO = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example Feed Two</title>
    <link>https://example.test/</link>
    <description>Example</description>
    <item>
      <title>Third</title>
      <link>https://example.test/post-three</link>
      <guid>post-three</guid>
      <pubDate>Sun, 01 Feb 2026 03:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

PAGE_ONE_HTML = """
<html>
  <body>
    <article>
      <p>First post.</p>
      <img src="/img-one.jpg" />
    </article>
  </body>
</html>
"""

PAGE_TWO_HTML = """
<html>
  <body>
    <article>
      <p>Second post.</p>
      <video src="/video-two.mp4"></video>
    </article>
  </body>
</html>
"""

PAGE_THREE_HTML = """
<html>
  <body>
    <main>
      <article>
        <p>Third post.</p>
      </article>
    </main>
  </body>
</html>
"""


class _FakeResponse:
    def __init__(self, body: str, status: int) -> None:
        self._body = body
        self._status = status

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> None:
        return None

    async def text(self) -> str:
        return self._body

    def raise_for_status(self) -> None:
        if self._status >= 400:
            raise RuntimeError(f"HTTP {self._status}")


class _FakeClient:
    def __init__(self, responses: dict[str, str]) -> None:
        self._responses = responses

    def get(
        self,
        url: str,
        headers: dict[str, str],
    ) -> _FakeResponse:
        del headers
        body = self._responses.get(url)

        if body is None:
            return _FakeResponse("", 404)

        return _FakeResponse(body, 200)


def test_scrape_feed_batches_yields_posts_and_assets() -> None:
    client = _FakeClient(
        {
            "https://example.test/feed.xml": RSS_XML,
            "https://example.test/post-one": PAGE_ONE_HTML,
            "https://example.test/post-two": PAGE_TWO_HTML,
        },
    )

    async def _collect_batches() -> list[ScrapeBatch]:
        batches: list[ScrapeBatch] = []

        async for batch in scrape_feed_batches(
            cast(Any, client),
            domain="example.test",
            feed_urls=["https://example.test/feed.xml"],
            batch_size=2,
        ):
            batches.append(batch)

        return batches

    batches = asyncio.run(_collect_batches())

    assert len(batches) == 1
    batch = batches[0]
    posts = batch.posts
    assets = batch.assets

    assert [post.url for post in posts] == [
        "https://example.test/post-one",
        "https://example.test/post-two",
    ]
    assert all(post.source == "rss" for post in posts)
    assert {asset.url for asset in assets} == {
        "https://example.test/img-one.jpg",
        "https://example.test/video-two.mp4",
    }


def test_scrape_feed_batches_loads_multiple_feeds() -> None:
    client = _FakeClient(
        {
            "https://example.test/feed-one.xml": RSS_XML,
            "https://example.test/feed-two.xml": RSS_XML_TWO,
            "https://example.test/post-one": PAGE_ONE_HTML,
            "https://example.test/post-two": PAGE_TWO_HTML,
            "https://example.test/post-three": PAGE_THREE_HTML,
        },
    )

    async def _collect_batches() -> list[ScrapeBatch]:
        batches: list[ScrapeBatch] = []

        async for batch in scrape_feed_batches(
            cast(Any, client),
            domain="example.test",
            feed_urls=[
                "https://example.test/feed-one.xml",
                "https://example.test/feed-two.xml",
            ],
            batch_size=10,
        ):
            batches.append(batch)

        return batches

    batches = asyncio.run(_collect_batches())

    assert len(batches) == 1
    assert [post.url for post in batches[0].posts] == [
        "https://example.test/post-one",
        "https://example.test/post-two",
        "https://example.test/post-three",
    ]
    assert [post.request_url for post in batches[0].posts] == [
        "https://example.test/feed-one.xml",
        "https://example.test/feed-one.xml",
        "https://example.test/feed-two.xml",
    ]
