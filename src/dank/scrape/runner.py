from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
from collections.abc import AsyncIterator

import aiohttp

from dank.config import Settings, SourceConfig, load_settings
from dank.logging_setup import configure_logging
from dank.model import AssetDiscovery, RawPost
from dank.scrape.assets import download_assets
from dank.scrape.rss import (
    FeedLink,
    fetch_feed_links,
    scrape_feed_batches,
)
from dank.scrape.types import ScrapeBatch
from dank.scrape.x import scrape_x_accounts
from dank.scrape.zendriver import BrowserConfig, BrowserSession
from dank.storage.clickhouse import ClickHouseClient

logger = logging.getLogger(__name__)


async def run_scrape(
    settings: Settings,
    *,
    headless: bool = False,
    batch_size: int = 50,
) -> None:
    logger.info(
        "Starting scrape run headless=%s batch_size=%d sources=%d",
        headless,
        batch_size,
        len(settings.sources),
    )

    if batch_size <= 0:
        batch_size = 1

    assets_dir = pathlib.Path(settings.assets_dir)
    assets_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = assets_dir.parent / "browser-profile"
    feed_staleness = datetime.timedelta(days=settings.feed_staleness_days)
    http_timeout = aiohttp.ClientTimeout(total=30)
    browser_config = BrowserConfig(
        headless=headless,
        browser_executable_path=(
            str(settings.browser.executable_path)
            if settings.browser.executable_path
            else None
        ),
        connection_timeout=settings.browser.connection_timeout,
        connection_max_tries=settings.browser.connection_max_tries,
        keep_open=not headless,
        profile_dir=profile_dir,
    )

    async with (
        ClickHouseClient(settings.clickhouse) as clickhouse_client,
        aiohttp.ClientSession(timeout=http_timeout) as http_client,
        BrowserSession(browser_config) as session,
    ):
        # Run a query early to check if the ClickHouse connection lives.
        await clickhouse_client.execute("SELECT 1")
        queue: asyncio.Queue[ScrapeBatch | None] = asyncio.Queue()
        processor_task = asyncio.create_task(
            _process_batches(
                queue,
                clickhouse_client,
                http_client,
                assets_dir=assets_dir,
                max_asset_bytes=settings.max_asset_bytes,
                batch_size=batch_size,
            ),
        )

        try:
            for source in settings.sources:
                async for batch in _discover_source_batches(
                    settings,
                    source,
                    clickhouse_client,
                    http_client,
                    session,
                    feed_staleness=feed_staleness,
                    batch_size=batch_size,
                ):
                    await queue.put(batch)
        finally:
            await queue.put(None)
            await processor_task

        logger.info("Scrape run complete")


async def _discover_source_batches(
    settings: Settings,
    source: SourceConfig,
    clickhouse_client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    session: BrowserSession,
    *,
    feed_staleness: datetime.timedelta,
    batch_size: int,
) -> AsyncIterator[ScrapeBatch]:
    match source.domain:
        case "x.com":
            logger.info(
                "Scraping X source accounts=%d",
                len(source.accounts),
            )

            batches_iter = scrape_x_accounts(
                settings.x,
                source.accounts,
                settings.email,
                session,
            )
        case _:
            logger.info("Scraping RSS feeds for domain=%s", source.domain)
            await _refresh_site_feeds(
                clickhouse_client,
                source.domain,
                feed_staleness,
            )
            feed_urls = await _load_site_feed_urls(
                clickhouse_client,
                source.domain,
            )

            batches_iter = scrape_feed_batches(
                http_client,
                domain=source.domain,
                feed_urls=feed_urls,
                batch_size=batch_size,
            )

    async for batch in batches_iter:
        logger.info(
            "Discovered batch domain=%s posts=%d assets=%d",
            source.domain,
            len(batch.posts),
            len(batch.assets),
        )
        yield batch


async def _process_batches(
    queue: asyncio.Queue[ScrapeBatch | None],
    clickhouse_client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    *,
    assets_dir: pathlib.Path,
    max_asset_bytes: int | None,
    batch_size: int,
) -> None:
    pending_posts: list[RawPost] = []
    pending_discoveries: list[AssetDiscovery] = []

    while True:
        # A None value signals we've stopped sending content to process.
        batch = await queue.get()

        if batch is not None:
            if batch.posts:
                pending_posts.extend(batch.posts)

            if batch.assets:
                pending_discoveries.extend(batch.assets)

        if batch is None or len(pending_posts) >= batch_size:
            await _flush_posts(clickhouse_client, pending_posts)

        if batch is None or len(pending_discoveries) >= batch_size:
            await _flush_assets(
                clickhouse_client,
                http_client,
                pending_discoveries,
                assets_dir=assets_dir,
                max_asset_bytes=max_asset_bytes,
            )

        if batch is None:
            # Break when there's nothing left.
            break


async def _flush_posts(
    clickhouse_client: ClickHouseClient,
    pending_posts: list[RawPost],
) -> None:
    if not pending_posts:
        return

    await clickhouse_client.insert_rows(
        "raw_posts",
        [post._asdict() for post in pending_posts],
    )
    logger.info("Flushed %d raw posts", len(pending_posts))
    pending_posts.clear()


async def _flush_assets(
    clickhouse_client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    discoveries: list[AssetDiscovery],
    *,
    assets_dir: pathlib.Path,
    max_asset_bytes: int | None,
) -> None:
    if not discoveries:
        return

    downloaded = await download_assets(
        discoveries,
        assets_dir=assets_dir,
        http_client=http_client,
        max_asset_bytes=max_asset_bytes,
    )
    discoveries.clear()

    if not downloaded:
        return

    await clickhouse_client.insert_rows(
        "raw_assets",
        [asset._asdict() for asset in downloaded],
    )
    logger.info("Flushed %d raw assets", len(downloaded))


async def _refresh_site_feeds(
    clickhouse_client: ClickHouseClient,
    domain: str,
    staleness: datetime.timedelta,
) -> None:
    now = datetime.datetime.now(datetime.UTC)
    cutoff = now - staleness
    recent = await _load_recent_site_feeds(clickhouse_client, domain, cutoff)

    if recent:
        return

    discovered = await fetch_feed_links(domain)

    if not discovered:
        return

    rows = [
        {
            "domain": domain,
            "feed_url": link.url,
            "feed_type": link.feed_type,
            "scraped_at": now,
        }
        for link in discovered
    ]
    await clickhouse_client.insert_rows("site_feeds", rows)


async def _load_recent_site_feeds(
    clickhouse_client: ClickHouseClient,
    domain: str,
    cutoff: datetime.datetime,
) -> list[FeedLink]:
    query = (
        "SELECT feed_url, feed_type, scraped_at FROM site_feeds FINAL "
        "WHERE domain = %(domain)s AND scraped_at >= %(cutoff)s "
        "ORDER BY scraped_at DESC"
    )
    result = await clickhouse_client.fetch_json(
        query,
        {"domain": domain, "cutoff": cutoff},
    )

    return [_parse_feed_row(row) for row in result.rows]


async def _load_site_feed_urls(
    clickhouse_client: ClickHouseClient,
    domain: str,
) -> list[str]:
    query = (
        "SELECT feed_url FROM site_feeds FINAL "
        "WHERE domain = %(domain)s "
        "ORDER BY scraped_at DESC"
    )
    result = await clickhouse_client.fetch_json(query, {"domain": domain})
    urls: list[str] = []
    seen: set[str] = set()

    for row in result.rows:
        url = str(row.get("feed_url", "")).strip()

        if url and url not in seen:
            seen.add(url)
            urls.append(url)

    return urls


def _parse_feed_row(row: dict[str, object]) -> FeedLink:
    match row.get("feed_type"):
        case "atom" | "rss1" | "rss2" as feed_type:
            pass
        case _:
            feed_type = "rss2"

    return FeedLink(
        url=str(row.get("feed_url", "")),
        feed_type=feed_type,
        mime_type=None,
    )


def run_scrape_from_config(
    path: str = "config.toml",
    *,
    headless: bool = False,
) -> None:
    settings = load_settings(path)
    configure_logging(settings.logging, component="scrape")

    asyncio.run(run_scrape(settings, headless=headless))
