from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
import re
import time
from collections.abc import AsyncIterator
from typing import cast

import aiohttp

from dank.config import Settings, SourceConfig, load_settings
from dank.logging_setup import configure_logging
from dank.model import AssetDiscovery, RawPost
from dank.progress import Progress
from dank.scrape.assets import download_assets
from dank.scrape.rss import (
    FeedLink,
    FeedType,
    fetch_feed_links,
    scrape_feed_batches,
)
from dank.scrape.types import ScrapeBatch, ScrapeTotals
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
    started = time.monotonic()
    logger.info(
        "Starting scrape: %d sources (headless=%s, batch size=%d)",
        len(settings.sources),
        headless,
        batch_size,
    )

    if not settings.sources:
        logger.warning("No sources selected; check sources and --domains.")

        return

    if batch_size <= 0:
        batch_size = 1

    data_dir = pathlib.Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)  # noqa
    assets_dir = data_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = data_dir / "browser-profile"
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
                browser_profile_dir=profile_dir,
                max_asset_bytes=settings.max_asset_bytes,
                batch_size=batch_size,
            ),
        )

        try:
            for index, source in enumerate(settings.sources, 1):
                logger.info(
                    "[%d/%d] %s: starting collection",
                    index, len(settings.sources), source.domain,
                )
                source_posts = 0
                source_assets = 0

                async for batch in _discover_source_batches(
                    settings,
                    source,
                    clickhouse_client,
                    http_client,
                    session,
                    feed_staleness=feed_staleness,
                    batch_size=batch_size,
                ):
                    source_posts += len(batch.posts)
                    source_assets += len(batch.assets)
                    await queue.put(batch)

                logger.info(
                    "[%d/%d] %s: queued %d posts and %d media references",
                    index, len(settings.sources), source.domain,
                    source_posts, source_assets,
                )
        finally:
            logger.info("Finishing queued downloads and database writes")
            await queue.put(None)
            totals = await processor_task

        logger.info(
            "Scrape complete in %.1fs: %d posts saved; "
            "%d assets available; %d not downloaded",
            time.monotonic() - started, totals.posts,
            totals.assets_available, totals.assets_unavailable,
        )


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

            if not feed_urls:
                logger.warning(
                    "%s: no usable feeds; source skipped", source.domain,
                )

                return

            batches_iter = scrape_feed_batches(
                http_client,
                domain=source.domain,
                feed_urls=feed_urls,
                batch_size=batch_size,
                keep_feed_on_fetch_failure=(
                    settings.keep_feed_on_fetch_failure
                ),
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
    browser_profile_dir: pathlib.Path,
    max_asset_bytes: int | None,
    batch_size: int,
) -> ScrapeTotals:
    saved_posts = 0
    available_assets = 0
    unavailable_assets = 0
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
            saved_posts += await _flush_posts(clickhouse_client, pending_posts)

        if batch is None or len(pending_discoveries) >= batch_size:
            assets = await _flush_assets(
                clickhouse_client,
                http_client,
                pending_discoveries,
                assets_dir=assets_dir,
                browser_profile_dir=browser_profile_dir,
                max_asset_bytes=max_asset_bytes,
            )
            available_assets += assets.assets_available
            unavailable_assets += assets.assets_unavailable

        if batch is None:
            # Break when there's nothing left.
            break

    return ScrapeTotals(saved_posts, available_assets, unavailable_assets)


async def _flush_posts(
    clickhouse_client: ClickHouseClient,
    pending_posts: list[RawPost],
) -> int:
    if not pending_posts:
        return 0

    count = len(pending_posts)
    await clickhouse_client.insert_rows(
        "raw_posts",
        [post._asdict() for post in pending_posts],
    )
    logger.info("Saved %d raw posts", count)
    pending_posts.clear()

    return count


async def _flush_assets(
    clickhouse_client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    discoveries: list[AssetDiscovery],
    *,
    assets_dir: pathlib.Path,
    browser_profile_dir: pathlib.Path,
    max_asset_bytes: int | None,
) -> ScrapeTotals:
    if not discoveries:
        return ScrapeTotals()

    downloaded = await download_assets(
        discoveries,
        assets_dir=assets_dir,
        browser_profile_dir=browser_profile_dir,
        http_client=http_client,
        max_asset_bytes=max_asset_bytes,
    )
    discoveries.clear()

    if not downloaded:
        return ScrapeTotals()

    await clickhouse_client.insert_rows(
        "raw_assets",
        [asset._asdict() for asset in downloaded],
    )
    available = sum(bool(asset.local_path) for asset in downloaded)
    unavailable = len(downloaded) - available
    logger.info(
        "Saved %d media references: %d files available; %d not downloaded",
        len(downloaded), available, unavailable,
    )

    return ScrapeTotals(0, available, unavailable)


async def _refresh_site_feeds(
    clickhouse_client: ClickHouseClient,
    domain: str,
    staleness: datetime.timedelta,
) -> None:
    now = datetime.datetime.now(datetime.UTC)
    cutoff = now - staleness
    recent = await _load_recent_site_feeds(clickhouse_client, domain, cutoff)

    if recent:
        logger.info("%s: using %d cached feed URLs", domain, len(recent))

        return

    async with Progress(f"Discovering feeds for {domain}"):
        discovered = await fetch_feed_links(domain)

    logger.info("%s: discovered %d feed URLs", domain, len(discovered))

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
            feed_type = cast(FeedType, feed_type)
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
    domain_regex: re.Pattern[str] | None = None,
) -> None:
    settings = load_settings(path)
    configure_logging(settings.logging, component="scrape")

    if domain_regex:
        settings = filter_settings_sources(settings, domain_regex)

    asyncio.run(run_scrape(settings, headless=headless))


def filter_settings_sources(
    settings: Settings,
    domain_regex: re.Pattern[str],
) -> Settings:
    filtered_sources = tuple(
        source
        for source in settings.sources
        if domain_regex.search(source.domain)
    )

    logger.info(
        "Filtered sources with pattern=%r retained=%d total=%d",
        domain_regex.pattern,
        len(filtered_sources),
        len(settings.sources),
    )

    return settings._replace(sources=filtered_sources)
