from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
import re
from collections.abc import AsyncIterator
from typing import cast

import aiohttp

from dank.config import Settings, SourceConfig, load_settings
from dank.logging_setup import configure_logging
from dank.model import AssetDiscovery, RawPost
from dank.progress import Progress
from dank.runtime import runtime_info
from dank.scrape.assets import download_assets
from dank.scrape.metrics import (
    MEDIA_CONCURRENCY,
    RSS_CONCURRENCY,
    RunMetrics,
    ScrapeOptions,
    SourceMetrics,
    current_source,
)
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
    if not settings.sources:
        logger.warning("No sources selected; check sources and --domains.")

        return

    batch_size = max(1, batch_size)
    history = RunMetrics(
        runtime_info(), len(settings.sources), batch_size, headless=headless,
        options=ScrapeOptions(
            int(settings.keep_feed_on_fetch_failure), settings.max_asset_bytes,
            settings.feed_staleness_days, settings.x.max_posts,
            settings.x.max_scrolls, settings.x.scroll_pause_seconds,
            settings.max_entries_per_feed,
        ),
    )
    memory = history.runtime.memory_limit_bytes
    logger.info(
        "Starting scrape run %s: %d sources; code=%s",
        history.run_id, len(settings.sources), history.runtime.code_version,
    )
    logger.info(
        "Runtime: %s; %s; CPU allowance=%.2f; memory capacity=%s",
        history.runtime.runtime, history.runtime.platform,
        history.runtime.cpu_limit,
        f"{memory / 1024 ** 3:.1f} GiB" if memory else "unknown",
    )
    logger.info(
        "Limits: sources=1; RSS requests=%d; media jobs=%d; "
        "batch size=%d; queue=unbounded; headless=%s",
        RSS_CONCURRENCY, MEDIA_CONCURRENCY, batch_size, headless,
    )
    data_dir = pathlib.Path(settings.data_dir)
    assets_dir = data_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = data_dir / "browser-profile"
    browser_config = BrowserConfig(
        headless=headless,
        browser_executable_path=(
            str(settings.browser.executable_path)
            if settings.browser.executable_path else None
        ),
        connection_timeout=settings.browser.connection_timeout,
        connection_max_tries=settings.browser.connection_max_tries,
        keep_open=not headless, profile_dir=profile_dir,
    )

    async with ClickHouseClient(settings.clickhouse) as clickhouse_client:
        await _ensure_history_tables(clickhouse_client)
        await clickhouse_client.insert_rows("scrape_runs", [history.row(1)])

        try:
            async with (
                aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as http_client,
                BrowserSession(browser_config) as session,
            ):
                await _collect_with_history(
                    settings, clickhouse_client, http_client, session,
                    history, assets_dir, profile_dir,
                )
        except BaseException as error:
            history.finish(error)

            try:
                await _save_history(clickhouse_client, history)
            except Exception:
                logger.exception("Could not save final scrape history")

            raise
        else:
            history.finish()
            await _save_history(clickhouse_client, history)

    counts = history.counts()
    logger.info(
        "Scrape %s in %.1fs: %d posts saved; %d files new/%d cached/"
        "%d failed; run=%s",
        history.status, (history.elapsed_ms or 0) / 1000,
        counts["posts_saved"], counts["files_downloaded"],
        counts["files_cached"], counts["files_failed"], history.run_id,
    )


async def _ensure_history_tables(client: ClickHouseClient) -> None:
    schema = pathlib.Path(__file__).with_name("history.sql").read_text()

    for statement in schema.split(";"):
        if statement.strip():
            await client.execute(statement)


async def _save_history(
    client: ClickHouseClient, history: RunMetrics,
) -> None:
    rows = [
        source.row(history.run_id, 2) for source in history.sources.values()
    ]

    if rows:
        await client.insert_rows("scrape_source_runs", rows)

    await client.insert_rows("scrape_runs", [history.row(2)])


async def _collect_with_history(
    settings: Settings,
    client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    session: BrowserSession,
    history: RunMetrics,
    assets_dir: pathlib.Path,
    profile_dir: pathlib.Path,
) -> None:
    queue: asyncio.Queue[ScrapeBatch | None] = asyncio.Queue()
    processor = asyncio.create_task(_process_batches(
        queue, client, http_client, assets_dir=assets_dir,
        browser_profile_dir=profile_dir,
        max_asset_bytes=settings.max_asset_bytes,
        batch_size=history.batch_size, history=history,
    ))
    progress = asyncio.create_task(history.report_progress())

    try:
        for index, source in enumerate(settings.sources, 1):
            if processor.done():
                await processor

            stats = SourceMetrics(index, source)
            history.sources[index] = stats
            await client.insert_rows(
                "scrape_source_runs", [stats.row(history.run_id, 1)],
            )
            logger.info(
                "[%d/%d] %s: starting collection",
                index, len(settings.sources), source.domain,
            )
            token = current_source.set(stats)

            try:
                async for batch in _discover_source_batches(
                    settings, source, client, http_client, session,
                    feed_staleness=datetime.timedelta(
                        days=settings.feed_staleness_days,
                    ),
                    batch_size=history.batch_size,
                ):
                    if processor.done():
                        await processor

                    stats.discovered(len(batch.posts), len(batch.assets))
                    await queue.put(batch._replace(source_index=index))
                    history.queue_size = queue.qsize()
            finally:
                current_source.reset(token)

            stats.collected()
            logger.info(
                "%s: collection finished; posts=%d; media references=%d; "
                "pending records=%d",
                source.domain, stats.counts["posts_queued"],
                stats.counts["media_references"], stats.pending,
            )

        logger.info("Finishing queued downloads and database writes")
        await queue.put(None)
        await processor
    finally:
        progress.cancel()

        if not processor.done():
            processor.cancel()

        await asyncio.gather(progress, processor, return_exceptions=True)


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
            feed_urls = await _source_feed_urls(
                clickhouse_client, source, feed_staleness,
            )
            stats = current_source.get()

            if stats is not None:
                stats.feed_urls = tuple(feed_urls)

            if not feed_urls:
                stats = current_source.get()

                if stats is not None:
                    stats.skipped = True

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
                max_entries_per_feed=settings.max_entries_per_feed,
            )

    async for batch in batches_iter:
        logger.info(
            "Discovered batch domain=%s posts=%d assets=%d",
            source.domain,
            len(batch.posts),
            len(batch.assets),
        )
        yield batch


async def _source_feed_urls(
    client: ClickHouseClient,
    source: SourceConfig,
    staleness: datetime.timedelta,
) -> list[str]:
    if source.feed_urls:
        logger.info(
            "%s: using %d explicit feed URLs",
            source.domain, len(source.feed_urls),
        )

        return list(source.feed_urls)

    await _refresh_site_feeds(client, source.domain, staleness)

    return await _load_site_feed_urls(client, source.domain)


async def _process_batches(
    queue: asyncio.Queue[ScrapeBatch | None],
    clickhouse_client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    *,
    assets_dir: pathlib.Path,
    browser_profile_dir: pathlib.Path,
    max_asset_bytes: int | None,
    batch_size: int,
    history: RunMetrics | None = None,
) -> ScrapeTotals:
    saved_posts = 0
    available_assets = 0
    unavailable_assets = 0
    pending_posts: list[RawPost] = []
    pending_discoveries: list[AssetDiscovery] = []
    post_owners: list[SourceMetrics | None] = []
    asset_owners: list[SourceMetrics | None] = []

    while True:
        # A None value signals we've stopped sending content to process.
        batch = await queue.get()

        if history:
            history.queue_size = queue.qsize()

        if batch is not None:
            owner = (
                history.sources.get(batch.source_index) if history else None
            )
            post_owners.extend([owner] * len(batch.posts))
            asset_owners.extend([owner] * len(batch.assets))
            if batch.posts:
                pending_posts.extend(batch.posts)

            if batch.assets:
                pending_discoveries.extend(batch.assets)

        if batch is None or len(pending_posts) >= batch_size:
            saved_posts += await _flush_posts(clickhouse_client, pending_posts)
            _records_persisted(post_owners, "posts_saved")

        if batch is None or len(pending_discoveries) >= batch_size:
            assets = await _flush_assets(
                clickhouse_client,
                http_client,
                pending_discoveries,
                assets_dir=assets_dir,
                browser_profile_dir=browser_profile_dir,
                max_asset_bytes=max_asset_bytes,
                owners=_asset_owner_map(pending_discoveries, asset_owners),
            )
            _records_persisted(asset_owners)
            available_assets += assets.assets_available
            unavailable_assets += assets.assets_unavailable

        if batch is None:
            # Break when there's nothing left.
            break

    return ScrapeTotals(saved_posts, available_assets, unavailable_assets)


def _records_persisted(
    owners: list[SourceMetrics | None], counter: str | None = None,
) -> None:
    for owner in owners:
        if owner is not None:
            if counter:
                owner.counts[counter] += 1

            owner.persisted(1)

    owners.clear()


def _asset_owner_map(
    discoveries: list[AssetDiscovery], owners: list[SourceMetrics | None],
) -> dict[AssetDiscovery, SourceMetrics]:
    result: dict[AssetDiscovery, SourceMetrics] = {}

    for discovery, owner in zip(discoveries, owners, strict=True):
        if owner is not None:
            result.setdefault(discovery, owner)

    return result


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
    owners: dict[AssetDiscovery, SourceMetrics] | None = None,
) -> ScrapeTotals:
    if not discoveries:
        return ScrapeTotals()

    downloaded = await download_assets(
        discoveries,
        assets_dir=assets_dir,
        browser_profile_dir=browser_profile_dir,
        http_client=http_client,
        max_asset_bytes=max_asset_bytes,
        owners=owners,
    )
    discoveries.clear()

    if not downloaded:
        return ScrapeTotals()

    await clickhouse_client.insert_rows(
        "raw_assets",
        [asset._asdict() for asset in downloaded],
    )
    by_identity = {
        (asset.domain, asset.post_id, asset.url): owner
        for asset, owner in (owners or {}).items()
    }

    for asset in downloaded:
        owner = by_identity.get((asset.domain, asset.post_id, asset.url))

        if owner is not None:
            owner.counts["asset_records_saved"] += 1

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
