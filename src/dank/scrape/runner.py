from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
import re
from collections.abc import AsyncIterator
from contextlib import nullcontext
from typing import cast
from urllib.parse import urlsplit

import aiohttp

from dank.config import (
    MEDIA_DOWNLOAD_TYPES,
    RedditSourceConfig,
    Settings,
    SourceConfig,
    load_settings,
)
from dank.html_utils import PAGE_CHALLENGE_PATTERN
from dank.logging_setup import configure_logging
from dank.model import AssetDiscovery, RawPost, RedditCommunityDiscovery
from dank.progress import Progress
from dank.runtime import runtime_info
from dank.scrape.assets import download_assets
from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.metrics import (
    RSS_CONCURRENCY,
    RunMetrics,
    ScrapeOptions,
    SourceMetrics,
    current_source,
)
from dank.scrape.reddit import scrape_reddit_communities
from dank.scrape.rss import (
    FeedLink,
    FeedType,
    fetch_feed_links,
    retry_feed_pages,
    scrape_feed_batches,
)
from dank.scrape.tasks import gather_tasks
from dank.scrape.types import ScrapeBatch, ScrapeTotals
from dank.scrape.x import scrape_x_accounts
from dank.scrape.zendriver import BrowserConfig, BrowserSession
from dank.storage.clickhouse import ClickHouseClient, parse_datetime

logger = logging.getLogger(__name__)
# Community avatars should remain small even when post media is unlimited.
MAX_COMMUNITY_ICON_BYTES = 1024 * 1024


async def run_scrape(
    settings: Settings,
    *,
    headless: bool = False,
    batch_size: int = 50,
    retry_failed_pages: bool = False,
) -> None:
    if not settings.sources:
        logger.warning("No sources selected; check sources and --domains.")

        return

    batch_size = max(1, batch_size)
    history = RunMetrics(
        runtime_info(), len(settings.sources), batch_size, headless=headless,
        limits=settings.scrape,
        options=ScrapeOptions(
            int(settings.keep_feed_on_fetch_failure), settings.max_asset_bytes,
            settings.feed_staleness_days, settings.x.scrape.max_posts,
            settings.x.scrape.max_scrolls,
            settings.x.scrape.scroll_pause_seconds,
            settings.max_entries_per_feed, settings.media_download_types,
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
        "Limits: sources=%d; HTTP=%d total/%d per host; "
        "RSS requests=%d per source; media jobs=%d total; "
        "batch size=%d; queue=%d batches per source; headless=%s",
        history.source_concurrency, settings.scrape.http_concurrency,
        settings.scrape.http_per_host, RSS_CONCURRENCY,
        settings.scrape.media_concurrency, batch_size,
        settings.scrape.queue_batches, headless,
    )
    logger.info(
        "Media downloads: %s",
        ", ".join(settings.media_download_types) or "disabled",
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
        schema = pathlib.Path(__file__).parents[1] / "storage/reddit.sql"
        await clickhouse_client.execute(schema.read_text())
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
                    retry_failed_pages=retry_failed_pages,
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
        "%d failed; media skipped=%d; run=%s",
        history.status, (history.elapsed_ms or 0) / 1000,
        counts["posts_saved"], counts["files_downloaded"],
        counts["files_cached"], counts["files_failed"],
        counts["media_skipped"], history.run_id,
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
        if source.index not in history.saved_sources
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
    *,
    retry_failed_pages: bool = False,
) -> None:
    sources = iter(enumerate(settings.sources, 1))
    browser_lock = asyncio.Lock()
    token = current_limiter.set(RequestLimiter(settings.scrape))
    progress = asyncio.create_task(history.report_progress())

    async def worker() -> None:
        # No await while claiming the next source from the shared iterator.
        for index, source in sources:
            await _collect_source(
                settings, source, index, client, http_client, session,
                history, assets_dir, profile_dir, browser_lock,
                retry_failed_pages=retry_failed_pages,
            )

    try:
        await gather_tasks(
            asyncio.create_task(worker())
            for _ in range(history.source_concurrency)
        )
    finally:
        progress.cancel()
        await asyncio.gather(progress, return_exceptions=True)
        current_limiter.reset(token)


async def _collect_source(
    settings: Settings,
    source: SourceConfig,
    index: int,
    client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    session: BrowserSession,
    history: RunMetrics,
    assets_dir: pathlib.Path,
    profile_dir: pathlib.Path,
    browser_lock: asyncio.Lock,
    *,
    retry_failed_pages: bool = False,
) -> None:
    stats = SourceMetrics(index, source)
    history.sources[index] = stats
    await client.insert_rows(
        "scrape_source_runs", [stats.row(history.run_id, 1)],
    )
    logger.info(
        "[%d/%d] %s: starting collection",
        index, len(settings.sources), source.domain,
    )
    queue: asyncio.Queue[ScrapeBatch | None] = asyncio.Queue(
        maxsize=settings.scrape.queue_batches,
    )

    async def produce() -> None:
        token = current_source.set(stats)

        try:
            # Browser collectors share one browser/profile and login state.
            async with (
                browser_lock
                if source.domain in {"x.com", "reddit.com"}
                else nullcontext()
            ):
                async for batch in _discover_source_batches(
                    settings, source, client, http_client, session,
                    feed_staleness=datetime.timedelta(
                        days=settings.feed_staleness_days,
                    ),
                    batch_size=history.batch_size,
                    retry_failed_pages=retry_failed_pages,
                ):
                    stats.discovered(len(batch.posts), len(batch.assets))
                    stats.pending += len(batch.communities)
                    await queue.put(batch._replace(source_index=index))
                    history.queue_size += 1

            stats.collected()
            logger.info(
                "%s: collection finished; posts=%d; media references=%d; "
                "pending records=%d",
                source.domain, stats.counts["posts_queued"],
                stats.counts["media_references"], stats.pending,
            )
            await queue.put(None)
        finally:
            current_source.reset(token)

    async def consume() -> None:
        await _process_batches(
            queue, client, http_client, assets_dir=assets_dir,
            browser_profile_dir=profile_dir,
            max_asset_bytes=settings.max_asset_bytes,
            batch_size=history.batch_size, history=history,
            download_types=settings.media_download_types,
        )

    await gather_tasks([
        asyncio.create_task(produce()), asyncio.create_task(consume()),
    ])
    # A completed source is visible in history while other sources still run.
    await client.insert_rows(
        "scrape_source_runs", [stats.row(history.run_id, 2)],
    )
    history.saved_sources.add(index)


async def _discover_source_batches(
    settings: Settings,
    source: SourceConfig,
    clickhouse_client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    session: BrowserSession,
    *,
    feed_staleness: datetime.timedelta,
    batch_size: int,
    retry_failed_pages: bool = False,
) -> AsyncIterator[ScrapeBatch]:
    if retry_failed_pages:
        if source.domain in {"x.com", "reddit.com"}:
            return

        posts = await _load_failed_rss_pages(clickhouse_client, source)
        logger.info("%s: retrying %d saved article pages", source.domain,
                    len(posts))

        async for batch in retry_feed_pages(
            http_client, posts, batch_size=batch_size,
        ):
            yield batch

        return

    match source.domain:
        case "reddit.com":
            options = source.reddit or RedditSourceConfig()
            communities = tuple(
                urlsplit(url).path.split("/")[2] for url in source.feed_urls
            )
            batches_iter = scrape_reddit_communities(
                settings.reddit, communities, session,
                sort=options.sort, time_filter=options.time_filter,
            )
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


async def _load_failed_rss_pages(
    client: ClickHouseClient, source: SourceConfig,
) -> list[RawPost]:
    # Filter after selecting the latest capture, so recovered pages stay out.
    result = await client.fetch_json(
        "SELECT * FROM ("
        "SELECT domain, post_id, url, post_created_at, scraped_at, source, "
        "request_url, payload FROM raw_posts "
        "WHERE domain = %(domain)s AND source = 'rss' "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id"
        ") WHERE (JSONExtractString(payload, 'page_fetch_status') = 'failed' "
        "OR match(JSONExtractString(payload, 'page_html'), "
        "%(challenge_pattern)s)) "
        "AND JSONExtractString(payload, 'feed_xml') != '' "
        "AND (empty(%(feed_urls)s) OR request_url IN %(feed_urls)s) "
        "ORDER BY scraped_at, post_id",
        {"domain": source.domain, "feed_urls": list(source.feed_urls),
         "challenge_pattern": PAGE_CHALLENGE_PATTERN},
    )

    return [
        RawPost(
            domain=str(row["domain"]), post_id=str(row["post_id"]),
            url=str(row["url"]),
            post_created_at=parse_datetime(row["post_created_at"]),
            scraped_at=datetime.datetime.fromisoformat(str(row["scraped_at"])),
            source=str(row["source"]), request_url=str(row["request_url"]),
            payload=str(row["payload"]),
        )
        for row in result.rows
    ]


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
    download_types: tuple[str, ...] = MEDIA_DOWNLOAD_TYPES,
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

        if history and batch is not None:
            history.queue_size -= 1

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

        # Persist articles before waiting on a potentially slow media batch.
        flush_media = batch is None or len(pending_discoveries) >= batch_size

        if batch is None or len(pending_posts) >= batch_size or flush_media:
            saved_posts += await _flush_posts(clickhouse_client, pending_posts)
            _records_persisted(post_owners, "posts_saved")

        if flush_media:
            assets = await _flush_assets(
                clickhouse_client,
                http_client,
                pending_discoveries,
                assets_dir=assets_dir,
                browser_profile_dir=browser_profile_dir,
                max_asset_bytes=max_asset_bytes,
                owners=_asset_owner_map(pending_discoveries, asset_owners),
                download_types=download_types,
            )
            _records_persisted(asset_owners)
            available_assets += assets.assets_available
            unavailable_assets += assets.assets_unavailable

        if batch is not None and batch.communities:
            owner = (
                history.sources.get(batch.source_index) if history else None
            )
            await _persist_community_icons(
                clickhouse_client, http_client, batch.communities,
                assets_dir=assets_dir, max_asset_bytes=max_asset_bytes,
                download_types=download_types, owner=owner,
            )

            _records_persisted([owner] * len(batch.communities))

        if batch is None:
            # Break when there's nothing left.
            break

    return ScrapeTotals(saved_posts, available_assets, unavailable_assets)


async def _persist_community_icons(
    client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    communities: tuple[RedditCommunityDiscovery, ...],
    *,
    assets_dir: pathlib.Path,
    max_asset_bytes: int | None,
    download_types: tuple[str, ...],
    owner: SourceMetrics | None,
) -> None:
    if "image" not in download_types:
        return

    limit = min(max_asset_bytes or MAX_COMMUNITY_ICON_BYTES,
                MAX_COMMUNITY_ICON_BYTES)

    for community in communities:
        if not community.icon_url:
            continue

        name = community.community.lower()
        if (
            not re.fullmatch(r"[a-z0-9_]{2,21}", name)
            or not _community_icon_url(community.icon_url)
        ):
            logger.warning("Ignoring invalid Reddit community icon metadata")
            continue

        # Reuse file caching without creating fictitious post/media rows.
        discovery = AssetDiscovery(
            "reddit-community", "reddit.com", f"communities/{name}",
            community.icon_url, "image", None,
        )

        if owner is not None:
            owner.counts["media_references"] += 1

        downloaded = await download_assets(
            [discovery], assets_dir=assets_dir, http_client=http_client,
            max_asset_bytes=limit, download_types=download_types,
            owners={discovery: owner} if owner is not None else None,
        )

        if not downloaded or not downloaded[0].local_path:
            continue

        local_path = pathlib.Path(downloaded[0].local_path)
        content_type = await asyncio.to_thread(_icon_content_type, local_path)

        if not content_type:
            # A transient HTML response must not poison this URL's file cache.
            await asyncio.to_thread(local_path.unlink, missing_ok=True)
            logger.warning("Reddit r/%s icon is not a supported image", name)

            if owner is not None:
                owner.counts["parse_failures"] += 1

            continue

        # Failed refreshes never overwrite a previously cached community icon.
        await client.insert_rows("reddit_community_icons", [{
            "community": name, "icon_url": community.icon_url,
            "local_path": str(local_path), "content_type": content_type,
            "scraped_at": community.scraped_at,
            "request_url": community.request_url,
            "payload": community.payload,
        }])

        if owner is not None:
            owner.counts["asset_records_saved"] += 1

        logger.info("Cached community icon for r/%s", community.community)


def _community_icon_url(value: str) -> bool:
    try:
        url = urlsplit(value)

        return (
            url.scheme == "https" and url.hostname == "styles.redditmedia.com"
            and url.username is None and url.password is None
            and url.port in {None, 443} and not url.fragment
        )
    except ValueError:
        return False


def _icon_content_type(path: pathlib.Path) -> str:
    with path.open("rb") as file:
        header = file.read(12)

    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"

    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"

    if header.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"

    if header[:4] == b"RIFF" and header[8:12] == b"WEBP":
        return "image/webp"

    return ""


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
    download_types: tuple[str, ...] = MEDIA_DOWNLOAD_TYPES,
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
        download_types=download_types,
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
    retry_failed_pages: bool = False,
) -> None:
    settings = load_settings(path)
    configure_logging(settings.logging, component="scrape")

    if domain_regex:
        settings = filter_settings_sources(settings, domain_regex)

    asyncio.run(run_scrape(
        settings, headless=headless, retry_failed_pages=retry_failed_pages,
    ))


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
