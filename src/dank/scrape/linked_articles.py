"""Collect linked pages using the existing article and media pipeline."""

from __future__ import annotations

import asyncio
import datetime
import hashlib
import itertools
import json
import logging
import xml.etree.ElementTree as ElementTree
from collections.abc import AsyncIterator
from typing import NamedTuple
from urllib.parse import urldefrag, urlsplit

import aiohttp

from dank.model import AssetDiscovery, RawPost
from dank.scrape.metrics import RSS_CONCURRENCY
from dank.scrape.rss import (
    HTML_ACCEPT,
    FetchedText,
    _extract_page_assets,  # pyright: ignore[reportPrivateUsage]
    _fetch_text,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.tasks import gather_tasks
from dank.scrape.types import ScrapeBatch

logger = logging.getLogger(__name__)


class LinkedArticle(NamedTuple):
    """A destination selected by the runner, with optional saved identity."""

    url: str
    title: str = ""
    identity: tuple[str, str] | None = None


async def scrape_linked_articles(
    http_client: aiohttp.ClientSession,
    articles: list[LinkedArticle],
    *,
    concurrency: int = RSS_CONCURRENCY,
    batch_size: int = 50,
) -> AsyncIterator[ScrapeBatch]:
    """Retain failures; caller resolves known and social destinations."""
    semaphore = asyncio.Semaphore(concurrency)
    unique = {urldefrag(article.url).url: article for article in articles}

    async def collect(
        url: str, article: LinkedArticle,
    ) -> tuple[RawPost, list[AssetDiscovery]] | None:
        try:
            parsed = urlsplit(url)
            domain = parsed.hostname
        except ValueError:
            return None

        if (parsed.scheme not in {"http", "https"} or not domain
                or parsed.username or parsed.password):
            return None

        async with semaphore:
            page = await _fetch_text(
                http_client, url, accept=HTML_ACCEPT, html_only=True,
            )

        return _build_capture(article, url, domain, page)

    for chunk in itertools.batched(unique.items(), batch_size):
        results = await gather_tasks(
            asyncio.create_task(collect(url, article))
            for url, article in chunk
        )
        posts: list[RawPost] = []
        assets: list[AssetDiscovery] = []

        for result in results:
            if result is not None:
                post, discoveries = result
                posts.append(post)
                assets.extend(discoveries)

        fetched = sum(
            bool(json.loads(post.payload)["page_html"]) for post in posts
        )
        logger.info(
            "Linked articles: fetched=%d, unavailable=%d",
            fetched, len(posts) - fetched,
        )

        if posts:
            yield ScrapeBatch(posts, assets)


def _build_capture(
    article: LinkedArticle, url: str, domain: str,
    page: FetchedText | None,
) -> tuple[RawPost, list[AssetDiscovery]]:
    page_html = page.text if page else ""
    final_url = page.url if page else url

    # Only an observed HTTP redirect can change a new destination identity.
    if article.identity is None:
        url = urldefrag(final_url).url
        domain = urlsplit(url).hostname or domain

    domain, post_id = article.identity or (
        domain, hashlib.sha256(url.encode()).hexdigest(),
    )
    status = "captured" if page_html else "failed"

    if page and page.content_type not in {
        "text/html", "application/xhtml+xml",
    }:
        status = "unsupported"
        page_html = ""

    entry = ElementTree.Element("item")
    ElementTree.SubElement(entry, "title").text = article.title
    ElementTree.SubElement(entry, "link").text = url
    payload: dict[str, object] = {
        "feed_xml": ElementTree.tostring(entry, encoding="unicode"),
        "page_html": page_html,
        "page_final_url": final_url,
        "linked_article": {
            "requested_url": article.url,
            "final_url": final_url,
            "status": status,
        },
    }

    if page:
        payload["page_content_type"] = page.content_type
        payload["page_fetch_method"] = page.method

    if not page_html:
        payload["page_fetch_status"] = "failed"

    post = RawPost(
        domain=domain, post_id=post_id, url=url, post_created_at=None,
        scraped_at=datetime.datetime.now(datetime.UTC),
        source="linked_article", request_url=article.url,
        payload=json.dumps(payload, separators=(",", ":")),
    )
    assets = [
        asset._replace(source="linked_article")
        for asset in _extract_page_assets(
            page_html, domain=domain, post_id=post_id, base_url=final_url,
        )
    ]

    return post, assets
