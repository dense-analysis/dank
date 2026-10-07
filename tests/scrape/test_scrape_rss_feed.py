import datetime
import json
import pathlib
import xml.etree.ElementTree as ElementTree
from typing import Any, cast

import pytest

from dank.config import load_settings
from dank.scrape.metrics import SourceMetrics, current_source
from dank.scrape.rss import scrape_feed_batches
from dank.scrape.runner import (
    _discover_source_batches,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch
from dank.storage.clickhouse import QueryResult

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
    def __init__(self, body: str, status: int, url: str) -> None:
        self.url = url
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
        self.requests: list[str] = []

    def get(
        self,
        url: str,
        headers: dict[str, str],
        **kwargs: object,
    ) -> _FakeResponse:
        del headers
        self.requests.append(url)
        body = self._responses.get(url)

        if body is None:
            return _FakeResponse("", 404, url)

        return _FakeResponse(body, 200, url)


async def test_scrape_feed_batches_yields_posts_and_assets() -> None:
    client = _FakeClient(
        {
            "https://example.test/feed.xml": RSS_XML,
            "https://example.test/post-one": PAGE_ONE_HTML,
            "https://example.test/post-two": PAGE_TWO_HTML,
        },
    )

    batches: list[ScrapeBatch] = []

    async for batch in scrape_feed_batches(
        cast(Any, client),
        domain="example.test",
        feed_urls=["https://example.test/feed.xml"],
        batch_size=2,
    ):
        batches.append(batch)

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


async def test_scrape_feed_batches_loads_multiple_feeds() -> None:
    client = _FakeClient(
        {
            "https://example.test/feed-one.xml": RSS_XML,
            "https://example.test/feed-two.xml": RSS_XML_TWO,
            "https://example.test/post-one": PAGE_ONE_HTML,
            "https://example.test/post-two": PAGE_TWO_HTML,
            "https://example.test/post-three": PAGE_THREE_HTML,
        },
    )

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


@pytest.mark.parametrize("keep", [None, False, True])
@pytest.mark.parametrize("failed_body", [None, ""])
async def test_scrape_feed_batches_handles_article_fetch_failure(
    *,
    keep: bool | None,
    failed_body: str | None,
) -> None:
    responses = {
        "https://example.test/feed.xml": RSS_XML,
        "https://example.test/post-one": PAGE_ONE_HTML,
    }

    if failed_body is not None:
        responses["https://example.test/post-two"] = failed_body

    client = _FakeClient(responses)
    options = {} if keep is None else {"keep_feed_on_fetch_failure": keep}
    batches = [
        batch
        async for batch in scrape_feed_batches(
            cast(Any, client),
            domain="example.test",
            feed_urls=["https://example.test/feed.xml"],
            **options,
        )
    ]

    assert len(batches) == 1
    batch = batches[0]
    assert len(batch.posts) == (2 if keep else 1)
    assert {asset.url for asset in batch.assets} == {
        "https://example.test/img-one.jpg",
    }
    assert json.loads(batch.posts[0].payload)["page_html"] == PAGE_ONE_HTML
    assert "page_fetch_status" not in json.loads(batch.posts[0].payload)

    if keep:
        retained = batch.posts[1]
        payload = json.loads(retained.payload)
        assert retained.url == "https://example.test/post-two"
        assert retained.request_url == "https://example.test/feed.xml"
        assert retained.source == "rss"
        assert retained.post_created_at == datetime.datetime(
            2026, 2, 1, 2, tzinfo=datetime.UTC,
        )
        assert payload["page_html"] == ""
        assert payload["page_fetch_status"] == "failed"
        assert ElementTree.fromstring(payload["feed_xml"]).findtext(
            "title",
        ) == "Second"


@pytest.mark.parametrize("keep", [False, True])
async def test_scrape_feed_batches_when_all_articles_fail(
    *,
    keep: bool,
) -> None:
    client = _FakeClient({"https://example.test/feed.xml": RSS_XML})
    batches = [
        batch
        async for batch in scrape_feed_batches(
            cast(Any, client),
            domain="example.test",
            feed_urls=["https://example.test/feed.xml"],
            keep_feed_on_fetch_failure=keep,
        )
    ]

    if keep:
        assert len(batches) == 1
        assert len(batches[0].posts) == 2
        assert batches[0].assets == []
    else:
        assert batches == []


class _FakeFeedCache:
    async def fetch_json(
        self,
        query: str,
        params: dict[str, Any],
    ) -> QueryResult:
        del query, params

        return QueryResult(rows=[{
            "feed_url": "https://example.test/feed.xml",
            "feed_type": "rss2",
        }])


@pytest.mark.parametrize("keep", [False, True])
async def test_runner_uses_configured_feed_failure_retention(
    *,
    tmp_path: pathlib.Path,
    keep: bool,
) -> None:
    config_path = tmp_path / "test-settings.toml"
    config_path.write_text(
        'sources = ["example.test"]\n'
        '[rss]\n'
        f'keep_feed_on_fetch_failure = {str(keep).lower()}\n',
    )
    settings = load_settings(config_path)
    client = _FakeClient({"https://example.test/feed.xml": RSS_XML})
    batches = [
        batch
        async for batch in _discover_source_batches(
            settings,
            settings.sources[0],
            cast(Any, _FakeFeedCache()),
            cast(Any, client),
            cast(Any, None),
            feed_staleness=datetime.timedelta(days=14),
            batch_size=50,
        )
    ]

    if keep:
        assert len(batches) == 1
        assert len(batches[0].posts) == 2
    else:
        assert batches == []


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("keep", [False, True])
async def test_comment_entries_share_page_fetch_but_keep_their_identity(
    *,
    failed: bool,
    keep: bool,
) -> None:
    page_url = "https://example.test/article"
    comment_urls = [f"{page_url}#comment-1", f"{page_url}#comment-2"]
    feed_url = "https://example.test/comments/feed/"
    feed = RSS_XML.replace(
        "https://example.test/post-one", comment_urls[0],
    ).replace("https://example.test/post-two", comment_urls[1])
    # A different query string is a separate HTTP resource.
    other_feed = RSS_XML_TWO.replace(
        "https://example.test/post-three", page_url + "?page=2#comment-3",
    )
    responses = {
        feed_url: feed,
        "https://example.test/other-feed/": other_feed,
        page_url + "?page=2": PAGE_THREE_HTML,
    }

    if not failed:
        responses[page_url] = PAGE_ONE_HTML

    class _CountingClient(_FakeClient):
        def __init__(self) -> None:
            super().__init__(responses)
            self.urls: list[str] = []

        def get(
            self, url: str, headers: dict[str, str], **kwargs: object,
        ) -> _FakeResponse:
            self.urls.append(url)

            return super().get(url, headers)

    client = _CountingClient()
    batches = [
        batch async for batch in scrape_feed_batches(
            cast(Any, client), domain="example.test",
            feed_urls=[feed_url, "https://example.test/other-feed/"],
            keep_feed_on_fetch_failure=keep,
        )
    ]
    posts = batches[0].posts
    assert client.urls == [
        feed_url, "https://example.test/other-feed/", page_url,
        page_url + "?page=2",
    ]
    expected_urls = [] if failed and not keep else comment_urls
    assert [post.url for post in posts] == [
        *expected_urls, page_url + "?page=2#comment-3",
    ]
    assert len({post.post_id for post in posts}) == len(posts)

    for index, post in enumerate(posts[:-1]):
        payload = json.loads(post.payload)
        assert post.request_url == feed_url
        entry = ElementTree.fromstring(payload["feed_xml"])
        assert entry.findtext("title") == ["First", "Second"][index]
        assert entry.findtext("link") == comment_urls[index]
        assert payload["page_html"] == ("" if failed else PAGE_ONE_HTML)

        if failed:
            assert payload["page_fetch_status"] == "failed"


async def test_explicit_feeds_bypass_discovery_and_cached_comments(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text(
        'sources = [{ domain = "example.test", '
        'feed_urls = ["https://provider.test/feed"], '
        'tags = ["engineering"] }]\n[rss]\nmax_entries_per_feed = 1\n',
    )
    settings = load_settings(path)
    client = _FakeClient({
        "https://provider.test/feed": RSS_XML,
        "https://example.test/post-two": PAGE_TWO_HTML,
    })

    class _ForbiddenCache:
        async def fetch_json(self, *args: Any) -> QueryResult:
            raise AssertionError("Explicit feeds must bypass cache reads")

        async def insert_rows(self, *args: Any) -> None:
            raise AssertionError("Explicit feeds must bypass cache writes")

    stats = SourceMetrics(1, settings.sources[0])
    token = current_source.set(stats)

    try:
        batches = [
            batch async for batch in _discover_source_batches(
                settings, settings.sources[0], cast(Any, _ForbiddenCache()),
                cast(Any, client), cast(Any, None),
                feed_staleness=datetime.timedelta(days=14), batch_size=50,
            )
        ]
    finally:
        current_source.reset(token)

    assert client.requests == [
        "https://provider.test/feed", "https://example.test/post-two",
    ]
    assert [post.url for post in batches[0].posts] == [
        "https://example.test/post-two",
    ]
    assert stats.feed_urls == ("https://provider.test/feed",)
    assert stats.source.tags == ("engineering",)


async def test_tag_only_source_keeps_cached_automatic_discovery(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "settings.fixture.toml"
    path.write_text('sources = [{ domain = "example.test", tags = ["ddd"] }]')
    settings = load_settings(path)
    client = _FakeClient({
        "https://example.test/feed.xml": RSS_XML,
        "https://example.test/post-one": PAGE_ONE_HTML,
        "https://example.test/post-two": PAGE_TWO_HTML,
    })
    stats = SourceMetrics(1, settings.sources[0])
    token = current_source.set(stats)

    try:
        batches = [
            batch async for batch in _discover_source_batches(
                settings, settings.sources[0], cast(Any, _FakeFeedCache()),
                cast(Any, client), cast(Any, None),
                feed_staleness=datetime.timedelta(days=14), batch_size=50,
            )
        ]
    finally:
        current_source.reset(token)

    assert len(batches[0].posts) == 2
    assert stats.feed_urls == ("https://example.test/feed.xml",)
    assert client.requests == [
        "https://example.test/feed.xml", "https://example.test/post-one",
        "https://example.test/post-two",
    ]


def _dated_feed(items: list[tuple[str, str | None]]) -> str:
    root = ElementTree.Element("rss", version="2.0")
    channel = ElementTree.SubElement(root, "channel")

    for url, date in items:
        item = ElementTree.SubElement(channel, "item")
        ElementTree.SubElement(item, "link").text = url

        if date is not None:
            ElementTree.SubElement(item, "pubDate").text = date

    return ElementTree.tostring(root, encoding="unicode")


@pytest.mark.parametrize("limit, expected", [
    (0, ["old", "undated", "offset", "new", "tied", "invalid"]),
    (2, ["new", "tied"]),
    (4, ["new", "tied", "offset", "old"]),
    (6, ["new", "tied", "offset", "old", "undated", "invalid"]),
])
async def test_entry_selection_handles_dates_timezones_and_stable_order(
    limit: int, expected: list[str],
) -> None:
    entries = [
        ("old", "2026-01-01T00:00:00Z"), ("undated", None),
        ("offset", "2026-03-01T00:00:00+02:00"),
        ("new", "2026-03-01T00:00:00Z"),
        ("tied", "2026-03-01T00:00:00"), ("invalid", "not a date"),
    ]
    prefix = "https://example.test/"
    feed_url = prefix + "feed.xml"
    responses = {feed_url: _dated_feed([
        (prefix + name, date) for name, date in entries
    ])}
    responses.update({prefix + name: PAGE_ONE_HTML for name, _ in entries})
    client = _FakeClient(responses)
    batches = [
        batch async for batch in scrape_feed_batches(
            cast(Any, client), domain="example.test", feed_urls=[feed_url],
            max_entries_per_feed=limit,
        )
    ]
    assert client.requests == [feed_url, *(prefix + name for name in expected)]
    assert [post.url for post in batches[0].posts] == [
        prefix + name for name in expected
    ]


async def test_limit_is_per_feed_before_cross_feed_deduplication() -> None:
    prefix = "https://example.test/"
    first, second = prefix + "first-feed", prefix + "second-feed"
    client = _FakeClient({
        first: _dated_feed([(prefix + name, None) for name in (
            "shared", "first", "excluded-a",
        )]),
        second: _dated_feed([(prefix + name, None) for name in (
            "shared", "second", "excluded-b",
        )]),
        prefix + "shared": PAGE_ONE_HTML,
        prefix + "first": PAGE_ONE_HTML,
        prefix + "second": PAGE_TWO_HTML,
    })
    batches = [
        batch async for batch in scrape_feed_batches(
            cast(Any, client), domain="example.test",
            feed_urls=[first, second],
            max_entries_per_feed=2,
        )
    ]
    assert client.requests == [
        first, second, prefix + "shared", prefix + "first", prefix + "second",
    ]
    assert len(batches[0].posts) == 3
    assert batches[0].posts[0].request_url == first


@pytest.mark.parametrize("keep", [False, True])
async def test_failed_selected_entry_does_not_fetch_an_older_replacement(
    *, keep: bool,
) -> None:
    client = _FakeClient({
        "https://example.test/feed": RSS_XML,
        "https://example.test/post-one": PAGE_ONE_HTML,
    })
    batches = [
        batch async for batch in scrape_feed_batches(
            cast(Any, client), domain="example.test",
            feed_urls=["https://example.test/feed"], max_entries_per_feed=1,
            keep_feed_on_fetch_failure=keep,
        )
    ]
    assert client.requests == [
        "https://example.test/feed", "https://example.test/post-two",
    ]
    assert bool(batches) is keep

    if keep:
        payload = json.loads(batches[0].posts[0].payload)
        assert payload["page_fetch_status"] == "failed"
