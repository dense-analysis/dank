"""Read HN RSS with native story identities, without requiring its API."""

from __future__ import annotations

import datetime as dt
import itertools
import json
import logging
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator
from urllib.parse import parse_qs, urlsplit

import aiohttp

from dank.links import normalized_url
from dank.model import RawPost
from dank.scrape.metrics import count
from dank.scrape.rss import FeedPageDiscovery, fetch_feed_page_discoveries
from dank.scrape.types import ScrapeBatch

logger = logging.getLogger(__name__)


def _story_id(value: str) -> str | None:
    url = normalized_url(value)

    if not url:
        return None

    parts = urlsplit(url)
    ids = parse_qs(parts.query).get("id", [])

    if (parts.hostname == "news.ycombinator.com" and parts.path == "/item"
            and len(ids) == 1 and ids[0].isascii() and ids[0].isdecimal()
            and int(ids[0]) > 0):
        return str(int(ids[0]))

    return None


def _post(
    discovery: FeedPageDiscovery, captured: dt.datetime,
) -> RawPost | None:
    page = discovery.page
    entry = ET.fromstring(page.payload)
    post_id = (
        _story_id(entry.findtext("comments") or "") or _story_id(page.url)
    )
    title = (entry.findtext("title") or "").strip()

    if not post_id or not title:
        count("parse_failures")
        logger.warning("HN RSS entry has no story ID or title: %s", page.url)

        return None

    rss = {
        "title": title, "url": page.url,
        "feed_xml": page.payload, "feed_url": discovery.feed_url,
        "feed_final_url": discovery.feed_final_url,
        "captured_at": captured.isoformat(),
        "time": int(page.created_at.timestamp()) if page.created_at else None,
    }

    return RawPost(
        domain="news.ycombinator.com", post_id=post_id,
        url=f"https://news.ycombinator.com/item?id={post_id}",
        post_created_at=page.created_at, scraped_at=captured,
        source="hacker_news", request_url=discovery.feed_url,
        payload=json.dumps({"rss": rss}, ensure_ascii=False),
    )


async def scrape_hacker_news_rss(
    client: aiohttp.ClientSession, feed_urls: list[str], *,
    max_entries_per_feed: int = 0, batch_size: int = 50,
) -> AsyncIterator[ScrapeBatch]:
    discoveries = await fetch_feed_page_discoveries(
        client, domain="news.ycombinator.com", feed_urls=feed_urls,
        max_entries_per_feed=max_entries_per_feed, deduplicate_urls=False,
    )
    captured = dt.datetime.now(dt.UTC)
    posts: dict[str, RawPost] = {}

    for discovery in discoveries:
        post = _post(discovery, captured)

        if post is not None:
            # Two HN discussions can link to the same publisher article.
            posts.setdefault(post.post_id, post)

    for batch in itertools.batched(posts.values(), batch_size):
        yield ScrapeBatch(list(batch), [])
