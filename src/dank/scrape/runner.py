from __future__ import annotations

import asyncio
import datetime
import json
import logging
import pathlib
import re
import shutil
import tempfile
from collections.abc import AsyncIterator
from contextlib import nullcontext
from typing import Any, cast
from urllib.parse import urlsplit

import aiohttp

from dank.config import (
    MEDIA_DOWNLOAD_TYPES,
    HackerNewsSettings,
    RedditSettings,
    RedditSourceConfig,
    Settings,
    SourceConfig,
    XSettings,
    load_settings,
)
from dank.html_utils import PAGE_CHALLENGE_PATTERN
from dank.links import is_platform_url, normalized_url
from dank.logging_setup import configure_logging
from dank.model import (
    AssetDiscovery,
    RawPost,
    RedditCommunityDiscovery,
    YouTubeChannelDiscovery,
)
from dank.progress import Progress
from dank.runtime import runtime_info
from dank.scrape.assets import download_assets
from dank.scrape.assets.http import (
    thumbnail_observation,
    youtube_thumbnail_urls,
)
from dank.scrape.hacker_news import (
    scrape_hacker_news,
    scrape_hacker_news_details,
)
from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.linked_articles import LinkedArticle, scrape_linked_articles
from dank.scrape.metrics import (
    RSS_CONCURRENCY,
    RunMetrics,
    ScrapeOptions,
    SourceMetrics,
    current_source,
)
from dank.scrape.page_recovery import PageRecovery, current_page_recovery
from dank.scrape.reddit import scrape_reddit_communities, scrape_reddit_details
from dank.scrape.reddit.access import RedditRequests, current_requests
from dank.scrape.reddit.cache import DetailSnapshot
from dank.scrape.reddit.totals import scrape_community_totals
from dank.scrape.rss import (
    FeedLink,
    FeedType,
    fetch_feed_links,
    is_comment_feed,
    retry_feed_pages,
    scrape_feed_batches,
)
from dank.scrape.tasks import gather_tasks
from dank.scrape.types import ScrapeBatch, ScrapeTotals
from dank.scrape.x import scrape_x_accounts, scrape_x_posts
from dank.scrape.x.conversations import canonical_post_url
from dank.scrape.youtube import scrape_youtube_feeds
from dank.scrape.youtube.channels import CHANNEL_ID, channel_icon_url
from dank.scrape.zendriver import BrowserConfig, BrowserSession
from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.thumbnails import thumbnail_suffix
from dank.youtube import discovery_from_post, payload_object

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
    hacker_news_details: bool = False,
) -> None:
    settings = _detail_source_settings(
        settings, reddit_details=reddit_details, x_details=x_details,
        hacker_news_details=hacker_news_details,
    )

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
        for statement in schema.read_text().split(";"):
            if statement.strip():
                await clickhouse_client.execute(statement)
        youtube_schema = (
            pathlib.Path(__file__).parents[1] / "storage/youtube.sql"
        )
        for statement in youtube_schema.read_text().split(";"):
            if statement.strip():
                await clickhouse_client.execute(statement)
        await clickhouse_client.insert_rows("scrape_runs", [history.row(1)])

        try:
            # Signed-out collection must never reuse an authenticated profile.
            with tempfile.TemporaryDirectory(
                prefix="dank-youtube-",
            ) as temporary:
                youtube_profile = (
                    settings.youtube.profile_dir
                    or data_dir / "youtube-browser-profile"
                    if settings.youtube.authenticated
                    else pathlib.Path(temporary)
                )
                youtube_config = browser_config._replace(
                    profile_dir=youtube_profile,
                )
                await _run_source_sessions(
                    settings, clickhouse_client, history, assets_dir,
                    browser_config, reddit_config, youtube_config,
                    retry_failed_pages=retry_failed_pages, x_posts=x_posts,
                    reddit_details=reddit_details, x_details=x_details,
                    hacker_news_details=hacker_news_details,
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
        "Scrape %s in %.1fs: %d posts saved; %d references saved; "
        "%d files new/%d cached/"
        "%d failed; media skipped=%d; run=%s",
        history.status, (history.elapsed_ms or 0) / 1000,
        counts["posts_saved"], counts["post_references_saved"],
        counts["files_downloaded"],
        counts["files_cached"], counts["files_failed"],
        counts["media_skipped"], history.run_id,
    )


def _detail_source_settings(
    settings: Settings, *, reddit_details: bool, x_details: bool,
    hacker_news_details: bool,
) -> Settings:
    for enabled, domain in (
        (hacker_news_details, "news.ycombinator.com"),
        (reddit_details, "reddit.com"), (x_details, "x.com"),
    ):
        if enabled:
            selected = filter_settings_sources(
                settings, re.compile("^" + re.escape(domain) + "$"),
            )

            if hacker_news_details:
                # HN listings share one saved-story queue and detail limit.
                selected = selected._replace(sources=selected.sources[:1])

            return selected

    return settings


async def _run_source_sessions(
    settings: Settings, client: ClickHouseClient, history: RunMetrics,
    assets_dir: pathlib.Path, x_config: BrowserConfig,
    reddit_config: BrowserConfig, youtube_config: BrowserConfig,
    *, retry_failed_pages: bool, x_posts: tuple[str, ...],
    reddit_details: bool, x_details: bool, hacker_news_details: bool = False,
) -> None:
    async with (
        aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        ) as http_client,
        BrowserSession(x_config) as x_session,
        BrowserSession(reddit_config) as reddit_session,
        BrowserSession(youtube_config) as youtube_session,
        BrowserSession(x_config._replace(
            profile_dir=settings.data_dir / "rss-browser-profile",
            keep_open=False,
        )) as rss_session,
    ):
        token = current_page_recovery.set(PageRecovery(rss_session))

        try:
            await _collect_with_history(
                settings, client, http_client, x_session, reddit_session,
                youtube_session, history, assets_dir,
                youtube_profile=youtube_config.profile_dir or assets_dir,
                retry_failed_pages=retry_failed_pages, x_posts=x_posts,
                reddit_details=reddit_details, x_details=x_details,
                hacker_news_details=hacker_news_details,
            )
        finally:
            current_page_recovery.reset(token)


async def _ensure_history_tables(client: ClickHouseClient) -> None:
    links_schema = pathlib.Path(__file__).parents[1] / "storage/links.sql"

    for statement in links_schema.read_text().split(";"):
        if statement.strip():
            await client.execute(statement)

    raw_schema = pathlib.Path(__file__).parents[1] / "storage/raw_posts.sql"
    await client.execute(raw_schema.read_text())
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
    youtube_session: BrowserSession,
    history: RunMetrics,
    assets_dir: pathlib.Path,
    *,
    youtube_profile: pathlib.Path,
    retry_failed_pages: bool = False,
    x_posts: tuple[str, ...] = (),
    reddit_details: bool = False,
    x_details: bool = False,
    hacker_news_details: bool = False,
) -> None:
    sources = iter(enumerate(settings.sources, 1))
    totals_seen: set[str] = set()
    browser_locks = {
        domain: asyncio.Lock()
        for domain in ("x.com", "reddit.com", "youtube.com")
    }
    token = current_limiter.set(RequestLimiter(settings.scrape))
    reddit_requests = RedditRequests(settings.reddit.request_interval_seconds)
    reddit_token = current_requests.set(reddit_requests)
    progress = asyncio.create_task(history.report_progress())

    async def worker() -> None:
        # No await while claiming the next source from the shared iterator.
        for index, source in sources:
            session, profile_dir = {
                "reddit.com": (
                    reddit_session,
                    settings.data_dir / "reddit-browser-profile",
                ),
                "youtube.com": (youtube_session, youtube_profile),
            }.get(source.domain, (
                x_session, settings.data_dir / "browser-profile",
            ))
            await _collect_source(
                settings, source, index, client, http_client, session,
                history, assets_dir, profile_dir,
                browser_locks.get(source.domain),
                retry_failed_pages=retry_failed_pages, x_posts=x_posts,
                reddit_details=reddit_details, x_details=x_details,
                hacker_news_details=hacker_news_details,
                totals_seen=totals_seen,
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
    hacker_news_details: bool = False,
    totals_seen: set[str] | None = None,
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
                    hacker_news_details=hacker_news_details,
                    totals_seen=totals_seen,
                ):
                    stats.discovered(
                        batch.post_count, len(batch.assets),
                        references=batch.reference_count,
                    )
                    stats.pending += (
                        len(batch.communities) + len(batch.youtube_channels)
                    )
                    # Persist this batch before moving on to the next one.
                    saved = await _flush_posts(client, list(batch.posts))
                    await _persist_youtube_discoveries(
                        client, batch.posts, str(history.run_id),
                    )
                    stats.counts["posts_saved"] += batch.post_count
                    stats.counts["post_references_saved"] += (
                        batch.reference_count
                    )
                    stats.persisted(saved)
                    await queue.put(batch._replace(
                        posts=[], source_index=index,
                    ))
                    history.queue_size += 1

            stats.collected()
            logger.info(
                "%s: collection finished; posts=%d; references=%d; "
                "media references=%d; "
                "pending records=%d",
                source.domain, stats.counts["posts_queued"],
                stats.counts["post_references_queued"],
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
                if kind != "video" or (
                    (source.domain != "reddit.com"
                     or settings.reddit.download_videos)
                    and (source.domain != "youtube.com"
                         or settings.youtube.scrape_videos)
                )
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
    hacker_news_details: bool = False,
    totals_seen: set[str] | None = None,
) -> AsyncIterator[ScrapeBatch]:
    if (x_posts or retry_failed_pages or reddit_details or x_details
            or hacker_news_details):
        async for batch in _retry_saved_posts(
            settings, source, clickhouse_client, http_client, session,
            x_posts=x_posts, batch_size=batch_size,
            reddit_details=reddit_details, x_details=x_details,
            hacker_news_details=hacker_news_details,
        ):
            yield batch

        return

    match source.domain:
        case "news.ycombinator.com":
            batches_iter = _hacker_news_content_batches(
                settings, source, clickhouse_client, http_client,
            )
        case "youtube.com":
            known_ids = await _known_youtube_channel_ids(clickhouse_client)
            batches_iter = scrape_youtube_feeds(
                settings, source, session, known_ids=known_ids,
            )
        case "reddit.com":
            batches_iter = await _reddit_listing_batches(
                settings, source, clickhouse_client, http_client, session,
                totals_seen,
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
                stats.skipped = not feed_urls

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
                max_entries_per_feed=settings.max_entries_per_feed,
            )

    async for batch in batches_iter:
        logger.info(
            "Discovered batch domain=%s posts=%d references=%d assets=%d",
            source.domain,
            batch.post_count,
            batch.reference_count,
            len(batch.assets),
        )
        yield batch


async def _hacker_news_content_batches(
    settings: Settings, source: SourceConfig,
    client: ClickHouseClient, http_client: aiohttp.ClientSession,
) -> AsyncIterator[ScrapeBatch]:
    seen: set[str] = set()

    async for batch in scrape_hacker_news(
        settings.hacker_news, source, http_client,
    ):
        # Save listing/metadata first; a slow publisher cannot erase discovery.
        yield batch
        articles = _hacker_news_article_links(batch.posts, seen)

        if not articles:
            continue

        known = await _known_link_destinations(client, list(articles))
        pending = [
            LinkedArticle(url, title, (
                str(known[url]["domain"]), str(known[url]["post_id"]),
            ) if url in known else None)
            for url, title in articles.items()
            if not is_platform_url(url) and not (
                url in known and str(known[url].get("html") or "").strip()
            )
        ]

        async for linked_batch in scrape_linked_articles(
            http_client, pending, batch_size=10,
        ):
            seen.update(
                url for post in linked_batch.posts
                if (url := normalized_url(post.url))
            )
            yield linked_batch


def _hacker_news_article_links(
    posts: list[RawPost], seen: set[str],
) -> dict[str, str]:
    links: dict[str, str] = {}

    for post in posts:
        payload = payload_object(post.payload)
        listing = payload_object(payload.get("hckrnews"))
        story = payload_object(payload.get("story"))
        url = normalized_url(str(
            listing.get("link") or story.get("url") or "",
        ))

        if url and url not in seen and url != normalized_url(post.url):
            seen.add(url)
            links[url] = str(
                listing.get("link_text") or story.get("title") or "",
            )

    return links


async def _known_link_destinations(
    client: ClickHouseClient, urls: list[str],
) -> dict[str, dict[str, Any]]:
    result = await client.fetch_json(
        "SELECT u.url AS lookup_url, p.domain, p.post_id, p.url, "
        "p.html, p.source FROM post_urls AS u FINAL "
        "INNER JOIN (SELECT domain, post_id, url, html, source, updated_at "
        "FROM posts "
        "FINAL) AS p ON p.domain = u.domain AND p.post_id = u.post_id "
        "AND u.observed_at <= p.updated_at "
        "WHERE u.url IN %(urls)s "
        "ORDER BY (p.domain = domainWithoutWWW(p.url)) DESC, "
        "(p.source != 'linked_article') DESC, p.domain, p.post_id "
        "LIMIT 1 BY lookup_url", {"urls": urls},
    )
    known = {str(row["lookup_url"]): row for row in result.rows}
    missing = [url for url in urls if url not in known]

    if missing:
        # Fall back before the web app indexes older URL aliases.
        result = await client.fetch_json(
            "SELECT domain, post_id, url, html, source FROM posts FINAL "
            "WHERE url IN %(urls)s ORDER BY "
            "(domain = domainWithoutWWW(url)) DESC, "
            "(source != 'linked_article') DESC, domain, post_id "
            "LIMIT 1 BY url",
            {"urls": missing},
        )

        for row in result.rows:
            if url := normalized_url(str(row["url"])):
                known[url] = row

    return known


async def _reddit_listing_batches(
    settings: Settings, source: SourceConfig,
    clickhouse_client: ClickHouseClient,
    http_client: aiohttp.ClientSession, session: BrowserSession,
    totals_seen: set[str] | None,
) -> AsyncIterator[ScrapeBatch]:
    logger.info(
        "Collecting Reddit posts first; details are deferred. "
        "Use --reddit-details to refresh saved posts separately.",
    )
    options = source.reddit or RedditSourceConfig()
    communities = tuple(
        urlsplit(url).path.split("/")[2] for url in source.feed_urls
    )
    new_communities = tuple(dict.fromkeys(
        name.lower() for name in communities
        if totals_seen is None or name.lower() not in totals_seen
    ))

    if totals_seen is not None:
        totals_seen.update(new_communities)

    await _refresh_reddit_totals(
        clickhouse_client, http_client, new_communities,
    )
    cached_details = await _load_reddit_details(
        clickhouse_client, communities, settings.reddit,
    )

    return scrape_reddit_communities(
        settings.reddit, communities, session,
        sort=options.sort, time_filter=options.time_filter,
        cached_details=cached_details,
    )


async def _persist_youtube_discoveries(
    client: ClickHouseClient, posts: list[RawPost], run_id: str,
) -> None:
    discoveries = [
        discovery_from_post(post, run_id)._asdict()
        for post in posts if post.source == "youtube"
    ]

    if discoveries:
        await client.insert_rows("youtube_discoveries", discoveries)


async def _known_youtube_channel_ids(
    client: ClickHouseClient,
) -> dict[str, set[str]]:
    # Only recent channel appearances are needed to detect the saved frontier.
    # Recommendation feeds must refresh even when all their videos are known.
    result = await client.fetch_json(
        "SELECT feed_url, item_id FROM youtube_discoveries "
        "WHERE feed_kind = 'channel' ORDER BY observed_at DESC "
        "LIMIT 1000 BY feed_url",
    )
    known: dict[str, set[str]] = {}

    for row in result.rows:
        known.setdefault(str(row["feed_url"]), set()).add(str(row["item_id"]))

    return known


async def _refresh_reddit_totals(
    client: ClickHouseClient, http_client: aiohttp.ClientSession,
    communities: tuple[str, ...],
) -> None:
    async for total in scrape_community_totals(http_client, communities):
        # Persist before listing discovery can fail or be cancelled.
        await client.insert_rows("reddit_community_totals", [{
            **total._asdict(), "source": "arctic_shift",
        }])


async def _retry_saved_posts(
    settings: Settings, source: SourceConfig, client: ClickHouseClient,
    http_client: aiohttp.ClientSession, session: BrowserSession,
    *, x_posts: tuple[str, ...], batch_size: int,
    reddit_details: bool = False,
    x_details: bool = False,
    hacker_news_details: bool = False,
) -> AsyncIterator[ScrapeBatch]:
    if hacker_news_details:
        posts = await _load_pending_hacker_news_posts(
            client, settings.hacker_news,
        )
        logger.info("Refreshing details for %d saved HN posts", len(posts))
        batches = scrape_hacker_news_details(
            settings.hacker_news, posts, http_client,
        )
    elif reddit_details:
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
    elif source.domain in {
        "x.com", "reddit.com", "youtube.com", "news.ycombinator.com",
    }:
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
        "WHERE domain = 'x.com' AND NOT is_reference "
        "AND post_id IN %(post_ids)s "
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
        "AND NOT is_reference "
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


async def _load_pending_hacker_news_posts(
    client: ClickHouseClient, settings: HackerNewsSettings,
) -> list[RawPost]:
    result = await client.fetch_json(
        "WITH latest AS (SELECT domain, post_id, url, post_created_at, "
        "scraped_at, source, request_url, payload FROM raw_posts "
        "WHERE source = 'hacker_news' AND domain = 'news.ycombinator.com' "
        "AND scraped_at >= %(cutoff)s "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id) "
        "SELECT latest.*, history.detail_payload FROM latest "
        "LEFT JOIN (SELECT post_id, "
        "argMaxIf(payload, scraped_at, "
        "JSONHas(payload, 'comment_capture')) AS detail_payload, "
        "countIf(JSONExtractString(payload, 'comment_capture', 'status') "
        "= 'captured') AS complete FROM raw_posts "
        "WHERE domain = 'news.ycombinator.com' AND source = 'hacker_news' "
        "AND post_id IN (SELECT post_id FROM latest) GROUP BY post_id) "
        "AS history ON latest.post_id = history.post_id "
        "WHERE history.complete = 0 AND (%(all_comments)s "
        "OR JSONExtractString(detail_payload, 'comment_capture', 'status') "
        "!= 'partial' OR JSONExtractString(detail_payload, "
        "'comment_capture', 'reason') != 'comment_limit' "
        "OR JSONExtractUInt(detail_payload, 'comment_capture', 'captured') "
        "< %(comment_limit)s) "
        "ORDER BY JSONExtractString(detail_payload, "
        "'comment_capture', 'status') = 'partial' DESC, "
        "latest.post_created_at DESC, latest.post_id DESC LIMIT %(limit)s",
        {"limit": settings.detail_max_posts,
         "all_comments": settings.comment_limit is None,
         "comment_limit": settings.comment_limit or 0,
         "cutoff": datetime.datetime.now(datetime.UTC)
         - datetime.timedelta(days=settings.days)},
    )
    posts: list[RawPost] = []

    for row in result.rows:
        payload = json.loads(str(row["payload"]))
        detail_payload = str(row.get("detail_payload") or "")

        if detail_payload:
            previous = json.loads(detail_payload)

            for key in ("comments", "comment_capture"):
                if key in previous:
                    payload[key] = previous[key]

        posts.append(RawPost(
            domain=str(row["domain"]), post_id=str(row["post_id"]),
            url=str(row["url"]),
            post_created_at=parse_datetime(row["post_created_at"]),
            scraped_at=datetime.datetime.fromisoformat(str(row["scraped_at"])),
            source=str(row["source"]), request_url=str(row["request_url"]),
            payload=json.dumps(payload, ensure_ascii=False),
        ))

    return posts


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

    urls = await _load_site_feed_urls(client, source.domain)

    return [url for url in urls if not is_comment_feed(url)]


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

        if batch is not None and batch.youtube_channels:
            owner = (
                history.sources.get(batch.source_index) if history else None
            )
            await _persist_youtube_channels(
                clickhouse_client, http_client, batch.youtube_channels,
                assets_dir=assets_dir, max_asset_bytes=max_asset_bytes,
                download_types=download_types, owner=owner,
            )
            _records_persisted([owner] * len(batch.youtube_channels))

        if batch is None:
            # Break when there's nothing left.
            break

    return ScrapeTotals(0, available_assets, unavailable_assets)


async def _persist_youtube_channels(
    client: ClickHouseClient,
    http_client: aiohttp.ClientSession,
    channels: tuple[YouTubeChannelDiscovery, ...],
    *,
    assets_dir: pathlib.Path,
    max_asset_bytes: int | None,
    download_types: tuple[str, ...],
    owner: SourceMetrics | None,
) -> None:
    for channel in channels:
        if not CHANNEL_ID.fullmatch(channel.channel_id):
            continue

        icon = await _youtube_channel_icon(
            channel, http_client, assets_dir=assets_dir,
            max_asset_bytes=max_asset_bytes, download_types=download_types,
            owner=owner,
        )

        if icon is None:
            # Metadata can refresh even when an icon download fails or is off.
            previous = await client.fetch_json(
                "SELECT icon_url, local_path, content_type "
                "FROM youtube_channels FINAL WHERE channel_id = %(id)s "
                "AND local_path != '' LIMIT 1",
                {"id": channel.channel_id},
            )
            icon = (
                tuple(str(previous.rows[0][key]) for key in (
                    "icon_url", "local_path", "content_type",
                )) if previous.rows else ("", "", "")
            )

        await client.insert_rows("youtube_channels", [{
            **channel._asdict(), "icon_url": icon[0],
            "local_path": icon[1], "content_type": icon[2],
        }])


async def _youtube_channel_icon(
    channel: YouTubeChannelDiscovery,
    http_client: aiohttp.ClientSession,
    *,
    assets_dir: pathlib.Path,
    max_asset_bytes: int | None,
    download_types: tuple[str, ...],
    owner: SourceMetrics | None,
) -> tuple[str, str, str] | None:
    if "image" not in download_types or not channel_icon_url(channel.icon_url):
        return None

    discovery = AssetDiscovery(
        "youtube-channel", "youtube.com", f"channels/{channel.channel_id}",
        channel.icon_url, "image", None,
    )

    if owner is not None:
        owner.counts["media_references"] += 1

    downloaded = await download_assets(
        [discovery], assets_dir=assets_dir, http_client=http_client,
        max_asset_bytes=min(max_asset_bytes or MAX_COMMUNITY_ICON_BYTES,
                            MAX_COMMUNITY_ICON_BYTES),
        download_types=download_types,
        owners={discovery: owner} if owner is not None else None,
    )

    if not downloaded or not downloaded[0].local_path:
        return None

    path = pathlib.Path(downloaded[0].local_path)
    content_type = await asyncio.to_thread(_icon_content_type, path)

    if not content_type:
        await asyncio.to_thread(path.unlink, missing_ok=True)
        logger.warning("YouTube channel icon is not a supported image")

        if owner is not None:
            owner.counts["parse_failures"] += 1

        return None

    if owner is not None:
        owner.counts["asset_records_saved"] += 1

    return channel.icon_url, str(path), content_type


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
    # is_reference is a computed storage column, not part of the raw payload.
    await clickhouse_client.insert_rows(
        "raw_posts",
        [{key: value for key, value in post._asdict().items()
          if key != "is_reference"} for post in pending_posts],
    )
    references = sum(post.is_reference for post in pending_posts)
    logger.info(
        "Saved %d raw posts and %d ID-only references", count - references,
        references,
    )
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

    await _preserve_legacy_youtube_thumbnails(clickhouse_client, sorted({
        item.post_id for item in discoveries if youtube_thumbnail_urls(item)
    }))

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
        [{key: value for key, value in asset._asdict().items()
          if key != "thumbnail"} for asset in downloaded],
    )
    observations = [
        asset.thumbnail._asdict() for asset in downloaded
        if asset.thumbnail is not None
    ]

    if observations:
        await clickhouse_client.insert_rows(
            "youtube_thumbnail_observations", observations,
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


async def _preserve_legacy_youtube_thumbnails(
    client: ClickHouseClient, identifiers: list[str],
) -> None:
    if not identifiers:
        return

    # The old URL cache never refreshed bytes. Its first saved timestamp is
    # therefore the only observation time we can honestly attribute to it.
    result = await client.fetch_json(
        "SELECT post_id, url, local_path, min(scraped_at) AS observed_at "
        "FROM raw_assets WHERE domain = 'youtube.com' AND source = 'youtube' "
        "AND asset_type = 'image' AND post_id IN %(ids)s "
        "AND local_path != '' AND position(local_path, '/thumbnails/') = 0 "
        "AND (post_id, url) NOT IN (SELECT post_id, captured_url "
        "FROM youtube_thumbnail_observations WHERE post_id IN %(ids)s) "
        "GROUP BY post_id, url, local_path",
        {"ids": identifiers},
    )
    observations: list[dict[str, object]] = []

    for row in result.rows:
        path = pathlib.Path(str(row["local_path"]))

        try:
            observed_at = parse_datetime(row["observed_at"])

            if observed_at is None:
                continue

            capture = await asyncio.to_thread(
                thumbnail_observation, path, str(row["post_id"]), observed_at,
                str(row["url"]), str(row["url"]),
            )
            suffix = await asyncio.to_thread(thumbnail_suffix, path)
            snapshot = path.parent / "thumbnails" / (
                f"{capture.content_hash}{suffix}"
            )
            await asyncio.to_thread(_copy_thumbnail_snapshot, path, snapshot)
            observations.append(capture._replace(
                local_path=str(snapshot),
            )._asdict())
        except (OSError, ValueError):
            # Invalid/missing old files must not block a fresh capture.
            logger.info("Could not preserve old YouTube thumbnail: %s", path)

    if observations:
        await client.insert_rows(
            "youtube_thumbnail_observations", observations,
        )


def _copy_thumbnail_snapshot(
    original: pathlib.Path, snapshot: pathlib.Path,
) -> None:
    if snapshot.exists():
        return

    snapshot.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        dir=snapshot.parent, delete=False,
    ) as temporary:
        pending = pathlib.Path(temporary.name)

    try:
        shutil.copyfile(original, pending)
        pending.replace(snapshot)
    finally:
        pending.unlink(missing_ok=True)


async def retry_youtube_thumbnails(
    settings: Settings, *, post_ids: tuple[str, ...] = (),
) -> ScrapeTotals:
    """Adopt cached history and retry missing files without crawling feeds."""
    if "image" not in settings.media_download_types:
        logger.warning("YouTube thumbnail retry requires image downloads")

        return ScrapeTotals()

    if any(not re.fullmatch(r"video:[A-Za-z0-9_-]{11}", item)
           for item in post_ids):
        raise ValueError(
            "YouTube thumbnail retry requires canonical video IDs",
        )

    limiter_token = current_limiter.set(RequestLimiter(settings.scrape))

    try:
        async with ClickHouseClient(settings.clickhouse) as client:
            schema = pathlib.Path(__file__).parents[1] / "storage/youtube.sql"

            for statement in schema.read_text().split(";"):
                if statement.strip():
                    await client.execute(statement)

            if post_ids:
                await _preserve_legacy_youtube_thumbnails(
                    client, list(post_ids),
                )

            result = await client.fetch_json(
                "SELECT post_id, url FROM (SELECT post_id, url, local_path "
                "FROM raw_assets WHERE domain = 'youtube.com' "
                "AND source = 'youtube' AND asset_type = 'image' "
                "AND startsWith(post_id, 'video:') "
                + ("AND post_id IN %(ids)s " if post_ids else "")
                + "ORDER BY scraped_at DESC LIMIT 1 BY post_id, url) "
                "WHERE local_path = ''",
                {"ids": list(post_ids)} if post_ids else {},
            )
            discoveries = [AssetDiscovery(
                "youtube", "youtube.com", str(row["post_id"]),
                str(row["url"]), "image", None,
            ) for row in result.rows]
            discoveries = [item for item in discoveries
                           if youtube_thumbnail_urls(item)]
            logger.info(
                "Retrying %d saved YouTube thumbnails", len(discoveries),
            )

            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60),
            ) as http_client:
                return await _flush_assets(
                    client, http_client, discoveries,
                    assets_dir=pathlib.Path(settings.data_dir) / "assets",
                    browser_profile_dir=(pathlib.Path(settings.data_dir)
                                         / "youtube-browser-profile"),
                    max_asset_bytes=settings.max_asset_bytes,
                    download_types=("image",),
                )
    finally:
        current_limiter.reset(limiter_token)


async def _refresh_site_feeds(
    clickhouse_client: ClickHouseClient,
    domain: str,
    staleness: datetime.timedelta,
) -> None:
    now = datetime.datetime.now(datetime.UTC)
    cutoff = now - staleness
    recent = await _load_recent_site_feeds(clickhouse_client, domain, cutoff)
    recent = [link for link in recent if not is_comment_feed(link.url)]

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
    hacker_news_details: bool = False,
    retry_youtube_thumbnails_only: bool = False,
    youtube_thumbnail_ids: tuple[str, ...] = (),
) -> None:
    settings = load_settings(path)
    configure_logging(settings.logging, component="scrape")

    if retry_youtube_thumbnails_only:
        asyncio.run(retry_youtube_thumbnails(
            settings, post_ids=youtube_thumbnail_ids,
        ))

        return

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
        hacker_news_details=hacker_news_details,
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
