from __future__ import annotations

import asyncio
import datetime
import json
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
    RedditSettings,
    RedditSourceConfig,
    Settings,
    SourceConfig,
    XSettings,
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
from dank.scrape.reddit import scrape_reddit_communities, scrape_reddit_details
from dank.scrape.reddit.access import RedditRequests, current_requests
from dank.scrape.reddit.cache import DetailSnapshot
from dank.scrape.rss import (
    FeedLink,
    FeedType,
    fetch_feed_links,
    retry_feed_pages,
    scrape_feed_batches,
)
from dank.scrape.tasks import gather_tasks
from dank.scrape.types import ScrapeBatch, ScrapeTotals
from dank.scrape.x import scrape_x_accounts, scrape_x_posts
from dank.scrape.x.conversations import canonical_post_url
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
    x_posts: tuple[str, ...] = (),
    reddit_details: bool = False,
    x_details: bool = False,
) -> None:
    if reddit_details or x_details:
        pattern = r"^reddit\.com$" if reddit_details else r"^x\.com$"
        settings = filter_settings_sources(settings, re.compile(pattern))

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
    browser_config = BrowserConfig.from_settings(
        settings, headless=headless, keep_open=not headless,
    )
    reddit_config = browser_config._replace(
        profile_dir=data_dir / "reddit-browser-profile",
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
                BrowserSession(browser_config) as x_session,
                BrowserSession(reddit_config) as reddit_session,
            ):
                await _collect_with_history(
                    settings, clickhouse_client, http_client,
                    x_session, reddit_session, history, assets_dir,
                    retry_failed_pages=retry_failed_pages, x_posts=x_posts,
                    reddit_details=reddit_details, x_details=x_details,
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
    x_session: BrowserSession,
    reddit_session: BrowserSession,
    history: RunMetrics,
    assets_dir: pathlib.Path,
    *,
    retry_failed_pages: bool = False,
    x_posts: tuple[str, ...] = (),
    reddit_details: bool = False,
    x_details: bool = False,
) -> None:
    sources = iter(enumerate(settings.sources, 1))
    browser_locks = {
        domain: asyncio.Lock() for domain in ("x.com", "reddit.com")
    }
    token = current_limiter.set(RequestLimiter(settings.scrape))
    reddit_requests = RedditRequests(settings.reddit.request_interval_seconds)
    reddit_token = current_requests.set(reddit_requests)
    progress = asyncio.create_task(history.report_progress())

    async def worker() -> None:
        # No await while claiming the next source from the shared iterator.
        for index, source in sources:
            is_reddit = source.domain == "reddit.com"
            session = reddit_session if is_reddit else x_session
            profile_dir = settings.data_dir / (
                "reddit-browser-profile" if is_reddit else "browser-profile"
            )
            await _collect_source(
                settings, source, index, client, http_client, session,
                history, assets_dir, profile_dir,
                browser_locks.get(source.domain),
                retry_failed_pages=retry_failed_pages, x_posts=x_posts,
                reddit_details=reddit_details, x_details=x_details,
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
        current_requests.reset(reddit_token)

        if reddit_requests.requests:
            logger.info(
                "Reddit pacing: %d browser actions; minimum interval=%.1fs",
                reddit_requests.requests, reddit_requests.interval,
            )


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
    browser_lock: asyncio.Lock | None,
    *,
    retry_failed_pages: bool = False,
    x_posts: tuple[str, ...] = (),
    reddit_details: bool = False,
    x_details: bool = False,
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
            # Same-site sources share a tab, independently of the other site.
            async with (
                browser_lock if browser_lock is not None else nullcontext()
            ):
                async for batch in _discover_source_batches(
                    settings, source, client, http_client, session,
                    feed_staleness=datetime.timedelta(
                        days=settings.feed_staleness_days,
                    ),
                    batch_size=history.batch_size,
                    retry_failed_pages=retry_failed_pages, x_posts=x_posts,
                    reddit_details=reddit_details, x_details=x_details,
                ):
                    stats.discovered(len(batch.posts), len(batch.assets))
                    stats.pending += len(batch.communities)
                    # Persist this batch before moving on to the next one.
                    saved = await _flush_posts(client, list(batch.posts))
                    stats.counts["posts_saved"] += saved
                    stats.persisted(saved)
                    await queue.put(batch._replace(
                        posts=[], source_index=index,
                    ))
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
            download_types=tuple(
                kind for kind in settings.media_download_types
                if kind != "video" or source.domain != "reddit.com"
                or settings.reddit.download_videos
            ),
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
    x_posts: tuple[str, ...] = (),
    reddit_details: bool = False,
    x_details: bool = False,
) -> AsyncIterator[ScrapeBatch]:
    if x_posts or retry_failed_pages or reddit_details or x_details:
        async for batch in _retry_saved_posts(
            settings, source, clickhouse_client, http_client, session,
            x_posts=x_posts, batch_size=batch_size,
            reddit_details=reddit_details, x_details=x_details,
        ):
            yield batch

        return

    match source.domain:
        case "reddit.com":
            logger.info(
                "Collecting Reddit posts first; details are deferred. "
                "Use --reddit-details to refresh saved posts separately.",
            )
            options = source.reddit or RedditSourceConfig()
            communities = tuple(
                urlsplit(url).path.split("/")[2] for url in source.feed_urls
            )
            cached_details = await _load_reddit_details(
                clickhouse_client, communities, settings.reddit,
            )
            batches_iter = scrape_reddit_communities(
                settings.reddit, communities, session,
                sort=options.sort, time_filter=options.time_filter,
                cached_details=cached_details,
            )
        case "x.com":
            logger.info(
                "Scraping X source accounts=%d",
                len(source.accounts),
            )

            batches_iter = scrape_x_accounts(
                settings.x,
                source.accounts,
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


async def _retry_saved_posts(
    settings: Settings, source: SourceConfig, client: ClickHouseClient,
    http_client: aiohttp.ClientSession, session: BrowserSession,
    *, x_posts: tuple[str, ...], batch_size: int,
    reddit_details: bool = False,
    x_details: bool = False,
) -> AsyncIterator[ScrapeBatch]:
    if reddit_details:
        posts = await _load_pending_reddit_posts(
            client, source, settings.reddit,
        )
        logger.info("Refreshing details for %d saved Reddit posts", len(posts))
        batches = scrape_reddit_details(settings.reddit, posts, session)
    elif x_details:
        if not settings.x.conversation.enabled:
            logger.info("X discussion refresh is disabled in configuration")

            return

        posts = await _load_pending_x_posts(client, source, settings.x)
        logger.info("Refreshing discussions for %d saved X posts", len(posts))
        batches = scrape_x_posts(settings.x, posts, session)
    elif x_posts:
        posts = await _load_x_posts(client, x_posts)
        batches = scrape_x_posts(settings.x, posts, session)
    elif source.domain in {"x.com", "reddit.com"}:
        return
    else:
        posts = await _load_failed_rss_pages(client, source)
        logger.info("%s: retrying %d saved article pages", source.domain,
                    len(posts))
        batches = retry_feed_pages(http_client, posts, batch_size=batch_size)

    async for batch in batches:
        yield batch


async def _load_x_posts(
    client: ClickHouseClient, urls: tuple[str, ...],
) -> list[RawPost]:
    by_id = {canonical_post_url(url).rsplit("/", 1)[1]: canonical_post_url(url)
             for url in urls}
    result = await client.fetch_json(
        "SELECT domain, post_id, url, post_created_at, scraped_at, source, "
        "request_url, payload FROM raw_posts "
        "WHERE domain = 'x.com' AND post_id IN %(post_ids)s "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id",
        {"post_ids": list(by_id)},
    )
    posts = [
        RawPost(
            domain="x.com", post_id=str(row["post_id"]),
            url=by_id[str(row["post_id"])],
            post_created_at=parse_datetime(row["post_created_at"]),
            scraped_at=datetime.datetime.fromisoformat(str(row["scraped_at"])),
            source=str(row["source"]), request_url=str(row["request_url"]),
            payload=str(row["payload"]),
        )
        for row in result.rows
    ]
    missing = set(by_id) - {post.post_id for post in posts}

    if missing:
        raise ValueError(
            "X posts must already be saved: " + ", ".join(sorted(missing)),
        )

    return posts


async def _load_pending_x_posts(
    client: ClickHouseClient, source: SourceConfig, settings: XSettings,
) -> list[RawPost]:
    result = await client.fetch_json(
        "SELECT * FROM (SELECT domain, post_id, url, post_created_at, "
        "scraped_at, source, request_url, payload, "
        "lowerUTF8(arrayElement(splitByChar('/', url), 4)) AS account "
        "FROM raw_posts WHERE source = 'x' AND domain = 'x.com' "
        "AND scraped_at >= %(cutoff)s AND account IN %(accounts)s "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id) "
        "WHERE JSONExtractString(payload, 'conversation_capture', 'status') "
        "NOT IN ('captured', 'partial') "
        "ORDER BY toUInt64OrZero(post_id) DESC LIMIT %(limit)s BY account",
        {"accounts": [
             name.strip().lstrip('@').lower() for name in source.accounts
         ],
         "limit": settings.conversation.max_posts_per_account,
         "cutoff": datetime.datetime.now(datetime.UTC)
         - datetime.timedelta(days=1)},
    )

    return [RawPost(
        domain=str(row["domain"]), post_id=str(row["post_id"]),
        url=str(row["url"]),
        post_created_at=parse_datetime(row["post_created_at"]),
        scraped_at=datetime.datetime.fromisoformat(str(row["scraped_at"])),
        source=str(row["source"]), request_url=str(row["request_url"]),
        payload=str(row["payload"]),
    ) for row in result.rows]


async def _load_pending_reddit_posts(
    client: ClickHouseClient, source: SourceConfig, settings: RedditSettings,
) -> list[RawPost]:
    communities = [
        urlsplit(url).path.split("/")[2].lower() for url in source.feed_urls
    ]
    result = await client.fetch_json(
        "SELECT * FROM (SELECT domain, post_id, url, post_created_at, "
        "scraped_at, "
        "source, request_url, payload, lowerUTF8(JSONExtractString(payload, "
        "'subreddit')) AS community FROM raw_posts WHERE source = 'reddit' "
        "AND domain = 'reddit.com' AND scraped_at >= %(cutoff)s "
        "AND community IN %(communities)s "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id) "
        "WHERE JSONExtractString(payload, 'detail_capture', 'status') "
        "!= 'captured' "
        "OR (%(comments)s AND JSONExtractString(payload, "
        "'comment_capture', 'status') "
        "!= 'captured') ORDER BY scraped_at DESC LIMIT %(limit)s BY community",
        {"communities": communities, "comments": settings.capture_comments,
         "limit": settings.scrape.max_posts,
         "cutoff": datetime.datetime.now(datetime.UTC)
         - datetime.timedelta(days=1)},
    )

    return [RawPost(
        domain=str(row["domain"]), post_id=str(row["post_id"]),
        url=str(row["url"]),
        post_created_at=parse_datetime(row["post_created_at"]),
        scraped_at=datetime.datetime.fromisoformat(str(row["scraped_at"])),
        source=str(row["source"]), request_url=str(row["request_url"]),
        payload=str(row["payload"]),
    ) for row in result.rows]


async def _load_reddit_details(
    client: ClickHouseClient, communities: tuple[str, ...],
    settings: RedditSettings,
) -> dict[str, DetailSnapshot]:
    if not settings.detail_cache_hours:
        return {}

    result = await client.fetch_json(
        "SELECT post_id, payload, scraped_at FROM raw_posts "
        "WHERE source = 'reddit' AND domain = 'reddit.com' "
        "AND scraped_at >= %(cutoff)s "
        "AND lowerUTF8(JSONExtractString(payload, 'subreddit')) "
        "IN %(communities)s "
        "AND JSONExtractString(payload, 'detail_capture', 'status') "
        "= 'captured' ORDER BY scraped_at DESC LIMIT 1 BY post_id",
        {
            "cutoff": datetime.datetime.now(datetime.UTC)
            - datetime.timedelta(hours=settings.detail_cache_hours),
            "communities": [name.lower() for name in communities],
        },
    )
    cached: dict[str, DetailSnapshot] = {}

    for row in result.rows:
        captured_at = parse_datetime(row.get("scraped_at"))

        try:
            payload = json.loads(row["payload"])
        except (TypeError, ValueError):
            continue

        if captured_at is not None and isinstance(payload, dict):
            cached[str(row["post_id"])] = DetailSnapshot(
                cast(dict[str, object], payload), captured_at.replace(
                    tzinfo=captured_at.tzinfo or datetime.UTC,
                ),
            )

    logger.info(
        "Reddit detail cache: %d successful snapshots loaded", len(cached),
    )

    return cached


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
    available_assets = 0
    unavailable_assets = 0
    pending_discoveries: list[AssetDiscovery] = []
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
            asset_owners.extend([owner] * len(batch.assets))

            if batch.assets:
                pending_discoveries.extend(batch.assets)

        # Posts are already durable; only media waits for a larger batch.
        flush_media = batch is None or len(pending_discoveries) >= batch_size

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

    return ScrapeTotals(0, available_assets, unavailable_assets)


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
    x_posts: tuple[str, ...] = (),
    reddit_details: bool = False,
    x_details: bool = False,
) -> None:
    settings = load_settings(path)
    configure_logging(settings.logging, component="scrape")

    if x_posts:
        x_posts = tuple(dict.fromkeys(
            canonical_post_url(url) for url in x_posts
        ))
        settings = settings._replace(sources=(SourceConfig("x.com", ()),))
    elif domain_regex:
        settings = filter_settings_sources(settings, domain_regex)

    asyncio.run(run_scrape(
        settings, headless=headless, retry_failed_pages=retry_failed_pages,
        x_posts=x_posts, reddit_details=reddit_details, x_details=x_details,
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
