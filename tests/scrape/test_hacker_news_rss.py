from __future__ import annotations

import json
from typing import Any, cast

import pytest

from dank.scrape import rss
from dank.scrape.hacker_news_rss import scrape_hacker_news_rss
from dank.scrape.rss import FetchedText

FEED_URL = "https://news.ycombinator.com/rss"
FEED = '''<rss version="2.0"><channel>
<item><title>First &amp; latest</title><link>https://publisher.test/story</link>
<pubDate>Sun, 18 Oct 2026 10:00:00 GMT</pubDate>
<comments>https://news.ycombinator.com/item?id=123</comments>
<description><![CDATA[<a href="https://news.ycombinator.com/item?id=123">Comments</a>]]></description>
</item>
<item><title>A separate discussion</title><link>https://publisher.test/story</link>
<pubDate>Sun, 18 Oct 2026 09:00:00 GMT</pubDate>
<comments>https://news.ycombinator.com/item?id=456</comments></item>
<item><title>Ask HN: A question</title>
<link>https://news.ycombinator.com/item?id=789</link></item>
<item><title>No discussion ID</title><link>https://publisher.test/other</link>
<comments>https://impostor.test/item?id=12</comments></item>
</channel></rss>'''


async def test_rss_keeps_discussion_identity_raw_xml_and_self_posts(
    monkeypatch: pytest.MonkeyPatch,
):
    requested: list[str] = []

    async def fetch(client: object, url: str, **kwargs: Any):
        requested.append(url)

        return FetchedText(FEED, url)

    monkeypatch.setattr(rss, "_fetch_text", fetch)
    posts = [post async for batch in scrape_hacker_news_rss(
        cast(Any, None), [FEED_URL, FEED_URL + "?copy=1"], batch_size=2,
    ) for post in batch.posts]
    assert [post.post_id for post in posts] == ["123", "456", "789"]
    assert all(post.source == "hacker_news" for post in posts)
    assert all(post.domain == "news.ycombinator.com" for post in posts)
    assert posts[0].url == "https://news.ycombinator.com/item?id=123"
    assert posts[0].request_url == FEED_URL
    payload = json.loads(posts[0].payload)
    assert payload["rss"]["title"] == "First & latest"
    assert payload["rss"]["url"] == "https://publisher.test/story"
    assert "Comments" in payload["rss"]["feed_xml"]
    assert "hacker_news" not in payload  # No invented archive rankings.
    assert "story" not in payload  # No dependency on the official API.
    assert requested == [FEED_URL, FEED_URL + "?copy=1"]


async def test_rss_respects_feed_entry_limit(monkeypatch: pytest.MonkeyPatch):
    async def fetch(*args: Any, **kwargs: Any):
        return FetchedText(FEED, FEED_URL)

    monkeypatch.setattr(rss, "_fetch_text", fetch)
    batches = [batch async for batch in scrape_hacker_news_rss(
        cast(Any, None), [FEED_URL], max_entries_per_feed=1,
    )]
    assert [post.post_id for post in batches[0].posts] == ["123"]
